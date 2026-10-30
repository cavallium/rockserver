package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
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
										.map(kv -> ByteBuffer.wrap(kv.keys().keys()[0].toByteArray()).getLong())
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
							if (cause instanceof RocksDBException rocks
									&& (rocks.getErrorUniqueId() == RocksDBErrorType.SERVER_OVERLOADED
									|| rocks.getErrorUniqueId() == RocksDBErrorType.UPDATE_RETRY)) {
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
			assertEquals(RocksDBErrorType.SERVER_OVERLOADED, cause.getErrorUniqueId(), cause.getMessage());
			assertTrue(cause.getMessage().contains("outcome of the commit is unknown"), cause.getMessage());

			cluster.start(leader);
			cluster.waitReady(leader, READY_TIMEOUT);
			// Applied at most once, whichever node now leads.
			var value = readEventually(api, column, 1);
			assertTrue(value == null || value.equals("tx"), "committed value: " + value);
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
}
