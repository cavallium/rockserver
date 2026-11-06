package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

/**
 * A client attached to a follower while the leader is killed (SIGKILL): the follower forwards its reads (point
 * reads and range streams) and proposes its writes to whichever node leads. The client must only ever see retryable
 * errors, never a transport or internal error from the forwarding, and reads must come back soon after the election.
 * A range read that succeeds returns every key exactly once: a stream cut by the kill fails, it is not truncated.
 */
@Timeout(180)
class ForwardingFailoverTest {

	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.latency(Duration.ofSeconds(8));
	private static final RequestContext SETUP = RequestContext.batch(Duration.ofSeconds(30));
	/** Range streams are not latency work. */
	private static final RequestContext STREAM = RequestContext.analytical(Duration.ofSeconds(8));
	private static final int KEYS = 64;
	/**
	 * A range far larger than what fits between a client that stopped asking for elements and the leader, whatever
	 * flow-control windows gRPC tunes on the way: the leader is still sending when the stream is cut.
	 */
	private static final int RANGE_KEYS = 30_000;
	private static final int RANGE_VALUE_BYTES = 1024;
	private static final int RANGE_BATCH_KEYS = 1000;
	private static final int FIRST_ELEMENTS = 16;
	/** Long enough for a range read that the test holds back, and for its failure to arrive. */
	private static final Duration RANGE_TIMEOUT = Duration.ofSeconds(60);
	private static final RequestContext RANGE_STREAM = RequestContext.analytical(RANGE_TIMEOUT);

	@TempDir
	Path root;
	private ProcessCluster cluster;

	@AfterEach
	void tearDown() {
		if (cluster != null) {
			cluster.close();
		}
	}

	@Test
	void aFollowersClientSeesOnlyRetryableErrorsWhileTheLeaderIsKilled() throws Exception {
		cluster = ProcessCluster.start(root.resolve("cluster"), 3, List.of(
				"--election-timeout-ms", "400:800",
				"--request-timeout-ms", "5000"));
		cluster.waitReady(READY_TIMEOUT);
		var members = List.of("1", "2", "3");
		var leader = awaitLeader(members);
		var follower = members.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();

		try (var connection = GrpcConnection.forHostAndPort("follower-client", cluster.apiAddress(follower))) {
			var api = connection.getAsyncApi(CONTEXT);
			var streamApi = connection.getAsyncApi(STREAM);
			long column = connection.getAsyncApi(SETUP).createColumnAsync("data", SCHEMA).join();
			for (int key = 0; key < KEYS; key++) {
				api.putAsync(0, column, key(key), Buf.wrap(new byte[] {1}), RequestType.none()).join();
			}

			var stop = new AtomicBoolean();
			var unexpected = new ConcurrentHashMap<String, String>();
			var retryable = new AtomicLong();
			var lastReadNanos = new AtomicLong();
			var lastRangeNanos = new AtomicLong();
			var lastWriteNanos = new AtomicLong();
			var workers = new ArrayList<Thread>();
			for (int i = 0; i < 8; i++) {
				boolean reader = i % 4 == 0;
				boolean rangeReader = i % 4 == 2;
				workers.add(Thread.ofVirtual().start(() -> {
					var random = ThreadLocalRandom.current();
					while (!stop.get()) {
						try {
							if (rangeReader) {
								var range = streamApi.getRangeAsync(0, column, null, null, false,
										RequestType.<KV>allInRange());
								var keys = Flux.from(range)
										.map(ForwardingFailoverTest::keyOf)
										.collectList()
										.block();
								// Writes only overwrite the keys written before: a complete read returns each once.
								long distinct = keys.stream().distinct().count();
								if (keys.size() != KEYS || distinct != KEYS) {
									unexpected.putIfAbsent("range", "range returned " + keys.size() + " keys, " + distinct
											+ " distinct, of " + KEYS);
								}
								lastRangeNanos.set(System.nanoTime());
							} else if (reader) {
								api.getAsync(0, column, key(random.nextInt(KEYS)), RequestType.current()).join();
								lastReadNanos.set(System.nanoTime());
							} else {
								api.putAsync(0, column, key(random.nextInt(KEYS)), Buf.wrap(new byte[] {2}),
										RequestType.none()).join();
								lastWriteNanos.set(System.nanoTime());
							}
						} catch (RuntimeException e) {
							var cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
							if (isRetryable(cause)) {
								retryable.incrementAndGet();
							} else {
								unexpected.putIfAbsent(String.valueOf(cause), String.valueOf(cause.getMessage()));
							}
						}
					}
				}));
			}

			Thread.sleep(1000);
			long killedAt = System.nanoTime();
			cluster.kill(leader);
			Thread.sleep(6000);
			stop.set(true);
			for (var worker : workers) {
				worker.join();
			}

			System.out.println("ForwardingFailoverTest: retryable errors " + retryable.get() + ", reads resumed "
					+ Duration.ofNanos(lastReadNanos.get() - killedAt).toMillis() + " ms, range reads "
					+ Duration.ofNanos(lastRangeNanos.get() - killedAt).toMillis() + " ms and writes "
					+ Duration.ofNanos(lastWriteNanos.get() - killedAt).toMillis() + " ms after the kill (last success)");
			assertTrue(unexpected.isEmpty(), () -> "non-retryable errors through the follower: " + unexpected);
			// Requests kept succeeding until the end, well after the election.
			assertTrue(lastReadNanos.get() - killedAt > Duration.ofSeconds(4).toNanos(), "reads did not resume");
			assertTrue(lastRangeNanos.get() - killedAt > Duration.ofSeconds(4).toNanos(),
					"range reads did not resume");
			assertTrue(lastWriteNanos.get() - killedAt > Duration.ofSeconds(4).toNanos(), "writes did not resume");
		}
	}

	/**
	 * A commit that a follower forwarded to a leader that is then killed fails with an unknown outcome: it is not sent
	 * again to the next leader, which does not know the transaction and would answer "retry", although the commit may
	 * have been applied. The leader is frozen first, so that it cannot answer before it dies.
	 */
	@Test
	void aForwardedCommitLostWithTheLeaderHasAnUnknownOutcome() throws Exception {
		cluster = startWithMergeOperator();
		var members = List.of("1", "2", "3");
		var leader = awaitLeader(members);
		var follower = members.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();

		try (var connection = GrpcConnection.forHostAndPort("follower-client", cluster.apiAddress(follower))) {
			var api = connection.getAsyncApi(SETUP);
			long column = api.createColumnAsync("data", SCHEMA).join();
			long tx = api.openTransactionAsync(Duration.ofSeconds(60)).join();
			api.mergeAsync(tx, column, key(1), text("tx"), RequestType.none()).join();
			connectEverySlot(api, column);

			cluster.pause(leader);
			var commit = api.closeTransactionAsync(tx, true);
			// Time for the follower to forward it to the frozen leader.
			Thread.sleep(300);
			assertFalse(commit.isDone(), "precondition: the commit waits for the frozen leader");
			cluster.kill(leader);
			var error = assertThrows(ExecutionException.class, () -> commit.get(60, TimeUnit.SECONDS));
			var cause = assertInstanceOf(RocksDBException.class, error.getCause(), () -> String.valueOf(error.getCause()));
			// The client resends the numbered commit until its deadline: the next leader finds no record of it and
			// answers "run it again", which is now safe; if that leader is not ready in time, the outcome is unknown.
			assertTrue(cause instanceof RocksDBRetryException
					|| cause.getErrorUniqueId() == RocksDBErrorType.OUTCOME_UNKNOWN, cause.getMessage());

			cluster.start(leader);
			cluster.waitReady(leader, READY_TIMEOUT);
			// Applied at most once, whichever node now leads.
			var value = readEventually(api, column, 1);
			// The leader was frozen before the commit reached it: no node ever had it to apply.
			assertNull(value, "a commit no leader processed was applied: " + value);
		}
	}

	/**
	 * An update that a follower forwarded to the leader, and whose connection to the leader breaks while it waits
	 * there for its key, fails with a retryable error: it is not opened again on another attempt, since the leader is
	 * alive and may have opened it. The leader does not keep it either: once the key is released, a new update
	 * through the follower takes it at once.
	 */
	@Test
	void aForwardedUpdateLostInTransportIsNotOpenedAgain() throws Exception {
		cluster = startWithMergeOperator();
		var members = List.of("1", "2", "3");
		var leader = awaitLeader(members);
		var follower = members.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();

		try (var throughFollower = GrpcConnection.forHostAndPort("follower-client", cluster.apiAddress(follower));
				var direct = GrpcConnection.forHostAndPort("leader-client", cluster.apiAddress(leader))) {
			long column = throughFollower.getAsyncApi(SETUP).createColumnAsync("data", SCHEMA).join();
			connectEverySlot(throughFollower.getAsyncApi(CONTEXT), column);
			var holder = direct.getAsyncApi(CONTEXT).getAsync(0, column, key(1), RequestType.forUpdate()).join();

			var waiting = throughFollower.getAsyncApi(CONTEXT).getAsync(0, column, key(1), RequestType.forUpdate());
			// Time for it to reach the leader and wait there behind the holder.
			Thread.sleep(300);
			assertFalse(waiting.isDone(), "precondition: the update waits for the key");
			cluster.network().blockLink(follower, leader);
			var error = assertThrows(ExecutionException.class, () -> waiting.get(60, TimeUnit.SECONDS));
			var cause = assertInstanceOf(RocksDBException.class, error.getCause(), () -> String.valueOf(error.getCause()));
			assertEquals(RocksDBErrorType.UPDATE_RETRY, cause.getErrorUniqueId(), cause.getMessage());

			cluster.network().heal();
			direct.getAsyncApi(CONTEXT).closeFailedUpdateAsync(holder.updateId()).join();
			var next = throughFollower.getAsyncApi(RequestContext.latency(Duration.ofSeconds(3)))
					.getAsync(0, column, key(1), RequestType.forUpdate())
					.get(10, TimeUnit.SECONDS);
			throughFollower.getAsyncApi(CONTEXT).closeFailedUpdateAsync(next.updateId()).join();
		}
	}

	/**
	 * A range stream whose link to a leader that stays alive breaks after the client received its first elements
	 * fails with a retryable error. The follower does not send it again, which would repeat those elements, and does
	 * not end it normally, which would hide the keys never received.
	 */
	@Test
	void aForwardedStreamCutByALostLinkFailsWithoutRepeatingOrLosingKeys() throws Exception {
		cluster = startForRanges();
		var members = List.of("1", "2", "3");
		var leader = awaitLeader(members);
		var follower = members.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();

		try (var connection = GrpcConnection.forHostAndPort("follower-client", cluster.apiAddress(follower))) {
			long column = createFilledRange(connection);

			var reader = new RangeReader();
			reader.read(connection.getAsyncApi(RANGE_STREAM), column);
			reader.awaitFirstElements();
			cluster.network().blockLink(follower, leader);
			// The connection is gone, but the leader can be reached again: a stream sent again would repeat keys.
			cluster.network().heal();
			reader.requestUnbounded();
			reader.assertFailedPartWay();

			assertRangeIsComplete(connection.getAsyncApi(RANGE_STREAM), column);
		}
	}

	/** Like {@link #aForwardedStreamCutByALostLinkFailsWithoutRepeatingOrLosingKeys}, but the leader is killed. */
	@Test
	void aForwardedStreamCutByTheLeadersDeathFailsWithoutRepeatingOrLosingKeys() throws Exception {
		cluster = startForRanges();
		var members = List.of("1", "2", "3");
		var leader = awaitLeader(members);
		var follower = members.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();

		try (var connection = GrpcConnection.forHostAndPort("follower-client", cluster.apiAddress(follower))) {
			long column = createFilledRange(connection);

			var reader = new RangeReader();
			reader.read(connection.getAsyncApi(RANGE_STREAM), column);
			reader.awaitFirstElements();
			cluster.kill(leader);
			reader.requestUnbounded();
			reader.assertFailedPartWay();

			assertRangeIsComplete(connection.getAsyncApi(RANGE_STREAM), column);
		}
	}

	/**
	 * A range stream lost before its first element is sent again to the next leader, and the client gets every key
	 * once. The leader is frozen first, so that it cannot send anything before it dies.
	 */
	@Test
	void aForwardedStreamCutBeforeItsFirstElementIsSentAgain() throws Exception {
		cluster = startForRanges();
		var members = List.of("1", "2", "3");
		var leader = awaitLeader(members);
		var follower = members.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();

		try (var connection = GrpcConnection.forHostAndPort("follower-client", cluster.apiAddress(follower))) {
			long column = createFilledRange(connection);
			connectEverySlot(connection.getAsyncApi(CONTEXT), column);

			cluster.pause(leader);
			var reader = new RangeReader();
			reader.read(connection.getAsyncApi(RANGE_STREAM), column);
			// Time for the follower to forward it to the frozen leader.
			Thread.sleep(300);
			assertEquals(0, reader.keys().size(), "precondition: no element came from the frozen leader");
			assertFalse(reader.isFinished(), "precondition: the stream waits for the frozen leader");
			cluster.kill(leader);
			reader.requestUnbounded();
			reader.awaitEnd();

			assertEveryKeyOnce(reader.keys());
		}
	}

	private ProcessCluster startForRanges() throws Exception {
		var started = ProcessCluster.start(root.resolve("cluster"), 3, List.of(
				"--election-timeout-ms", "400:800",
				// A resent stream has this long to find the next leader, even on a loaded machine.
				"--request-timeout-ms", "30000"));
		started.waitReady(READY_TIMEOUT);
		return started;
	}

	/** Creates a column holding the keys from 0 to {@link #RANGE_KEYS} (excluded). */
	private static long createFilledRange(GrpcConnection connection) {
		var api = connection.getAsyncApi(SETUP);
		long column = api.createColumnAsync("data", SCHEMA).join();
		var value = Buf.wrap(new byte[RANGE_VALUE_BYTES]);
		var batches = new ArrayList<KVBatch>();
		for (int start = 0; start < RANGE_KEYS; start += RANGE_BATCH_KEYS) {
			var keys = new ArrayList<Keys>(RANGE_BATCH_KEYS);
			var values = new ArrayList<Buf>(RANGE_BATCH_KEYS);
			for (int key = start; key < start + RANGE_BATCH_KEYS; key++) {
				keys.add(key(key));
				values.add(value);
			}
			batches.add(new KVBatch.KVBatchRef(keys, values));
		}
		api.putBatchAsync(column, Flux.fromIterable(batches), PutBatchMode.WRITE_BATCH).join();
		return column;
	}

	/** Reads the whole range through {@code api}, retrying while the cluster recovers. */
	private static void assertRangeIsComplete(RocksDBAsyncAPI api, long column) throws Exception {
		long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
		while (true) {
			try {
				assertEveryKeyOnce(Flux.from(api.getRangeAsync(0, column, null, null, false, RequestType.<KV>allInRange()))
						.map(ForwardingFailoverTest::keyOf)
						.collectList()
						.block());
				return;
			} catch (RocksDBException e) {
				if (!isRetryable(e) || System.nanoTime() - deadline > 0) {
					throw e;
				}
				Thread.sleep(100);
			}
		}
	}

	private static void assertEveryKeyOnce(List<Long> keys) {
		assertEquals(RANGE_KEYS, keys.size(), "keys read");
		assertKeysFromZero(keys);
	}

	/** Any repeated key, as a stream sent again would give, or any gap, shows here. */
	private static void assertKeysFromZero(List<Long> keys) {
		for (int i = 0; i < keys.size(); i++) {
			assertEquals(i, keys.get(i), "key at position " + i + " of the " + keys.size() + " read");
		}
	}

	/**
	 * Reads a range one demand at a time, so that the test decides how far the stream goes: {@link #FIRST_ELEMENTS}
	 * at first, then whatever is left once {@link #requestUnbounded()} is called. A cut connection is reported only
	 * after what it had delivered was consumed, so the read must go on after a cut for its failure to arrive.
	 */
	private static final class RangeReader extends BaseSubscriber<KV> {

		private final List<Long> keys = Collections.synchronizedList(new ArrayList<>());
		private final CountDownLatch firstElements = new CountDownLatch(FIRST_ELEMENTS);
		private final CompletableFuture<Void> end = new CompletableFuture<>();

		void read(RocksDBAsyncAPI api, long column) {
			Flux.from(api.getRangeAsync(0, column, null, null, false, RequestType.<KV>allInRange())).subscribe(this);
		}

		@Override
		protected void hookOnSubscribe(Subscription subscription) {
			subscription.request(FIRST_ELEMENTS);
		}

		@Override
		protected void hookOnNext(KV kv) {
			keys.add(keyOf(kv));
			firstElements.countDown();
		}

		@Override
		protected void hookOnComplete() {
			end.complete(null);
		}

		@Override
		protected void hookOnError(Throwable error) {
			end.completeExceptionally(error);
		}

		/** Returns when the first elements arrived, while the rest of the stream is held back by the missing demand. */
		void awaitFirstElements() throws InterruptedException {
			assertTrue(firstElements.await(RANGE_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS), "no first elements");
			// Time to fill every buffer up to the leader, which then waits for flow control, most of the range unsent.
			Thread.sleep(500);
			assertEquals(FIRST_ELEMENTS, keys().size(), "precondition: the client received only what it asked for");
			assertFalse(isFinished(), "precondition: the stream is still running");
		}

		void awaitEnd() throws Exception {
			end.get(RANGE_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS);
		}

		/** The read ended with a retryable error, and what came before it is the start of the range, with no repeats. */
		void assertFailedPartWay() {
			var error = assertThrows(ExecutionException.class, this::awaitEnd,
					() -> "a cut stream ended normally with " + keys().size() + " keys of " + RANGE_KEYS);
			var cause = assertInstanceOf(RocksDBException.class, error.getCause(), () -> String.valueOf(error.getCause()));
			assertTrue(isRetryable(cause), () -> "not retryable: " + cause);
			var read = keys();
			System.out.println("ForwardingFailoverTest: stream cut after " + read.size() + " of " + RANGE_KEYS + " keys: "
					+ cause);
			assertTrue(read.size() >= FIRST_ELEMENTS && read.size() < RANGE_KEYS, () -> read.size() + " keys read");
			assertKeysFromZero(read);
		}

		boolean isFinished() {
			return end.isDone();
		}

		List<Long> keys() {
			synchronized (keys) {
				return List.copyOf(keys);
			}
		}
	}

	private ProcessCluster startWithMergeOperator() throws Exception {
		var rockserverConfig = root.resolve("rockserver.conf");
		Files.writeString(rockserverConfig, InProcessCluster.MERGE_CONFIG);
		var started = ProcessCluster.start(root.resolve("cluster"), 3, List.of(
				"--election-timeout-ms", "400:800",
				"--request-timeout-ms", "5000",
				"--rockserver-config", rockserverConfig.toString()));
		started.waitReady(READY_TIMEOUT);
		return started;
	}

	/**
	 * Forwards reads through the follower until it has connected each of its connections to the leader (four, used
	 * in turn): a later call is then sent at once. A call that finds its connection still connecting to a frozen
	 * leader is never sent, and fails with a retryable error instead.
	 */
	private static void connectEverySlot(RocksDBAsyncAPI throughFollower, long column) {
		for (int i = 0; i < 8; i++) {
			throughFollower.getAsync(0, column, key(i), RequestType.current()).join();
		}
	}

	/** Reads a key through {@code api}, retrying while the cluster elects a leader. */
	private static String readEventually(RocksDBAsyncAPI api, long column, long key) throws Exception {
		long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
		while (true) {
			try {
				var value = api.getAsync(0, column, key(key), RequestType.current()).join();
				return value == null ? null : new String(value.toByteArray(), StandardCharsets.UTF_8);
			} catch (CompletionException e) {
				if (System.nanoTime() - deadline > 0) {
					throw e;
				}
				Thread.sleep(100);
			}
		}
	}

	private static boolean isRetryable(Throwable error) {
		return error instanceof RocksDBException rocks
				&& (rocks.getErrorUniqueId() == RocksDBErrorType.SERVER_OVERLOADED
				|| rocks.getErrorUniqueId() == RocksDBErrorType.OUTCOME_UNKNOWN
				|| rocks.getErrorUniqueId() == RocksDBErrorType.UPDATE_RETRY);
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	/** Waits for a leader: every node printed READY, but the first election may still be running. */
	private String awaitLeader(List<String> members) throws Exception {
		long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
		while (true) {
			var status = cluster.admin(members, "status");
			var leader = status.lines()
					.filter(line -> line.contains(": LEADER"))
					.map(line -> line.substring(0, line.indexOf(':')))
					.findFirst();
			if (leader.isPresent()) {
				return leader.get();
			}
			if (System.nanoTime() - deadline > 0) {
				throw new AssertionError("No leader in status:\n" + status);
			}
			Thread.sleep(100);
		}
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static long keyOf(KV kv) {
		return ByteBuffer.wrap(kv.keys().keys()[0].toByteArray()).getLong();
	}
}
