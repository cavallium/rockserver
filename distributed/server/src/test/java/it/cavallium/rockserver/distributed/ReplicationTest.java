package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

/** Replication through real Ratis servers in one JVM: writes, reads, transactions, restarts, catch-up. */
class ReplicationTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));
	/** Lowered limit of the apply queue, in KiB: {@link #LARGE_VALUE}s fill it with 16 log entries. */
	private static final int QUEUE_LIMIT_KIB = 256;
	/** 16 KiB: a log entry holding one such put takes 16 KiB of the apply queue. */
	private static final Buf LARGE_VALUE = Buf.wrap(new byte[16 * 1024]);
	/** Setting that bounds how long a stopping API server waits for the calls in progress, in milliseconds. */
	private static final String GRPC_GRACEFUL_SHUTDOWN =
			"it.cavallium.rockserver.grpc.server.shutdown-graceful-timeout-ms";

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private static String text(Buf value) {
		return value == null ? null : new String(value.toByteArray(), StandardCharsets.UTF_8);
	}

	private static RocksDBSyncAPI api(ReplicatedNode node) {
		return node.connection().getSyncApi(CONTEXT);
	}

	/** Reads a key from a node's own database, bypassing routing, to compare replicas. */
	private static String localValue(ReplicatedNode node, long columnId, long key) {
		return text(node.stateMachine().store().db().get(0, columnId, key(key), RequestType.current()));
	}

	private static long commitIndex(ReplicatedNode node) {
		return node.stateMachine().division().getRaftLog().getLastCommittedIndex();
	}

	/** The other running node, neither {@code a} nor {@code b}. */
	private ReplicatedNode third(ReplicatedNode a, ReplicatedNode b) {
		return cluster.runningNodes().stream().filter(node -> node != a && node != b).findFirst().orElseThrow();
	}

	/**
	 * Halts {@code id} on a thread of its own and returns that thread: a node whose apply thread waits for
	 * {@code testHooks().applyGate} stops only once the gate opens.
	 */
	private Thread haltInBackground(String id) {
		return Thread.ofPlatform().name("halt-" + id).start(() -> {
			try {
				cluster.halt(id);
			} catch (IOException ex) {
				throw new UncheckedIOException(ex);
			}
		});
	}

	/** A write that may have taken effect: OUTCOME_UNKNOWN, never "not applied". */
	private static void assertOutcomeUnknown(Throwable error) {
		var rocks = assertInstanceOf(RocksDBException.class, error);
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, rocks.getErrorUniqueId(), rocks.getMessage());
		assertTrue(rocks.getMessage().contains("outcome is unknown"), rocks.getMessage());
		assertFalse(rocks.getMessage().contains("not applied"), rocks.getMessage());
	}

	/** Puts {@link #LARGE_VALUE} at keys {@code from} to {@code from + count - 1}, one log entry each. */
	private static void writeLarge(ReplicatedNode through, long column, int from, int count) {
		for (int i = from; i < from + count; i++) {
			api(through).put(0, column, key(i), LARGE_VALUE, RequestType.none());
		}
	}

	@Test
	void writesThroughAnyNodeReachEveryReplica() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		long column = api(follower).createColumn("users", SCHEMA);
		for (ReplicatedNode node : cluster.runningNodes()) {
			api(node).put(0, column, key(node.config().nodeId().hashCode()), text("from " + node.config().nodeId()),
					RequestType.none());
		}
		assertEquals("from n1", text(api(follower).get(0, column, key("n1".hashCode()), RequestType.current())));
		cluster.awaitConverged(TIMEOUT);
		for (ReplicatedNode node : cluster.runningNodes()) {
			assertEquals(column, node.stateMachine().store().db().getColumnId("users"), "column id differs");
			for (String id : cluster.ids()) {
				assertEquals("from " + id, localValue(node, column, id.hashCode()));
			}
		}
		// The internal metadata column stays hidden.
		assertEquals(List.of("users"), List.copyOf(api(follower).getAllColumnDefinitions().keySet()));
	}

	/**
	 * A new leader may still be applying the entries of earlier leaders when Ratis counts it ready: its reads wait
	 * for them, and never miss a write the previous leader acknowledged.
	 */
	@Test
	void aNewLeaderDoesNotServeReadsBeforeApplyingWhatTheOldOneAcknowledged() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var oldLeader = cluster.awaitLeader(TIMEOUT);
		long column = api(oldLeader).createColumn("c", SCHEMA);
		api(oldLeader).put(0, column, key(1), text("old"), RequestType.none());
		cluster.awaitConverged(TIMEOUT);
		var gate = new java.util.concurrent.CountDownLatch(1);
		var followers = cluster.runningNodes().stream().filter(node -> node != oldLeader).toList();
		try {
			for (var follower : followers) {
				follower.stateMachine().testHooks().applyGate = gate;
			}
			// Acknowledged by the leader once a follower logged it; neither follower can apply it.
			api(oldLeader).put(0, column, key(1), text("new"), RequestType.none());
			cluster.stop(oldLeader.config().nodeId());
			var newLeader = cluster.awaitLeader(TIMEOUT);
			var read = newLeader.connection().getAsyncApi(RequestContext.batch(Duration.ofSeconds(20)))
					.getAsync(0, column, key(1), RequestType.current());
			try {
				var value = read.get(1, java.util.concurrent.TimeUnit.SECONDS);
				assertEquals("new", text(value), "read served before the acknowledged write was applied");
			} catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException expected) {
				// Still waiting for the apply, or failed in time: either way not stale.
			}
		} finally {
			gate.countDown();
			for (var follower : followers) {
				follower.stateMachine().testHooks().applyGate = null;
			}
		}
		var newLeader = cluster.awaitLeader(TIMEOUT);
		assertEquals("new", text(api(newLeader).get(0, column, key(1), RequestType.current())));
	}

	@Test
	void writeResultsAreComputedByApply() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var api = api(cluster.anyFollower());
		long column = api.createColumn("c", SCHEMA);
		assertNull(api.put(0, column, key(1), text("a"), RequestType.previous()));
		assertEquals("a", text(api.put(0, column, key(1), text("b"), RequestType.previous())));
		assertTrue(api.put(0, column, key(1), text("c"), RequestType.changed()));
		assertFalse(api.put(0, column, key(1), text("c"), RequestType.changed()));
		assertEquals("c", text(api.delete(0, column, key(1), RequestType.previous())));
		assertEquals("x", text(api.merge(0, column, key(2), text("x"), RequestType.merged())));
		assertEquals("x,y", text(api.merge(0, column, key(2), text("y"), RequestType.merged())));
	}

	/**
	 * The results of a batch come back in one reply, which a follower receives through gRPC with a size limit: many
	 * concurrent writes through a follower, each returning a large previous value, all get their results. Twelve of
	 * these results already exceed a reply, so those that do not fit are sent again alone (data writes) or read from
	 * the leader's own record (update commits).
	 */
	@Test
	void manyLargeResultsThroughAFollowerAreAllReceived() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		var api = api(cluster.anyFollower());
		long column = api.createColumn("c", SCHEMA);
		int writes = 64;
		var large = Buf.wrap(new byte[3 * 512 * 1024]);
		try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
			// Keys below `writes` for data writes, the others for updates.
			var puts = new ArrayList<java.util.concurrent.Future<?>>();
			for (int i = 0; i < 2 * writes; i++) {
				long key = i;
				puts.add(executor.submit(() -> api.put(0, column, key(key), large, RequestType.none())));
			}
			for (var put : puts) {
				put.get();
			}
			var previous = new ArrayList<java.util.concurrent.Future<Buf>>();
			for (int i = 0; i < writes; i++) {
				long key = i;
				previous.add(executor.submit(() -> api.put(0, column, key(key), text("small"), RequestType.previous())));
			}
			for (var result : previous) {
				assertEquals(large.size(), result.get().size());
			}
			// Updates opened first, then committed together, so that their commits share log entries.
			var updates = new long[writes];
			for (int i = 0; i < writes; i++) {
				updates[i] = api.get(0, column, key(writes + i), RequestType.<Buf>forUpdate()).updateId();
			}
			var updated = new ArrayList<java.util.concurrent.Future<Buf>>();
			for (int i = 0; i < writes; i++) {
				long update = updates[i];
				long key = writes + i;
				updated.add(executor.submit(() -> api.put(update, column, key(key), text("small"),
						RequestType.<Buf>previous())));
			}
			for (var result : updated) {
				assertEquals(large.size(), result.get().size());
			}
		}
	}

	/**
	 * Two results so close to the reply limit that the reply of their batch has room for neither: both are marked
	 * "resend alone", and their copies must then go one per batch instead of together again, where they would get
	 * the same reply until their deadline.
	 */
	@Test
	void resentCopiesWhoseReplyCarriedNoResultGoOnePerBatch() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withRequestTimeout(Duration.ofSeconds(30))).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var through = cluster.anyFollower();
		var api = api(through);
		long column = api.createColumn("c", SCHEMA);
		// A delta result is a tag, a value tag and both sides with their lengths.
		var previous = Buf.wrap(new byte[15 * 1024 * 1024]);
		var current = Buf.wrap(new byte[ResultCodec.MAX_RESULT_BYTES - 2 - 10 - previous.size()]);
		var result = new ApplyResult.Ok(new it.cavallium.rockserver.core.common.Delta<>(previous, current));
		assertEquals(List.of(ApplyResult.RESEND_ALONE, ApplyResult.RESEND_ALONE),
				ResultCodec.decodeAll(ResultCodec.encodeAll(List.of(result, result))), "precondition");
		api.put(0, column, key(1), previous, RequestType.none());
		api.put(0, column, key(2), previous, RequestType.none());

		// Without a majority, every write stays in flight: once MAX_BATCHES_IN_FLIGHT are, the two deltas queue and
		// then go together in one batch.
		var halted = cluster.ids().stream().filter(id -> !id.equals(through.config().nodeId())).toList();
		assertTrue(halted.contains(leader.config().nodeId()));
		for (String id : halted) {
			cluster.halt(id);
		}
		var async = through.connection().getAsyncApi(CONTEXT);
		var fillers = new ArrayList<java.util.concurrent.CompletableFuture<Void>>();
		for (int i = 0; i < Proposer.MAX_BATCHES_IN_FLIGHT; i++) {
			fillers.add(async.putAsync(0, column, key(100 + i), text("filler"), RequestType.none()));
		}
		var deltas = List.of(async.putAsync(0, column, key(1), current, RequestType.<Buf>delta()),
				async.putAsync(0, column, key(2), current, RequestType.<Buf>delta()));
		for (String id : halted) {
			cluster.start(id);
		}
		for (var filler : fillers) {
			filler.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		}
		for (var delta : deltas) {
			var received = delta.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
			assertEquals(previous.size(), received.previous().size());
			assertEquals(current.size(), received.current().size());
		}
	}

	/** Without a majority, a write returns at its caller's deadline, not at the node's request timeout. */
	@Test
	void aWriteWithoutMajorityReturnsAtItsCallersDeadline() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withRequestTimeout(Duration.ofSeconds(30))).startAll();
		cluster.awaitLeader(TIMEOUT);
		var through = cluster.anyFollower();
		long column = api(through).createColumn("c", SCHEMA);
		for (String id : cluster.ids()) {
			if (!id.equals(through.config().nodeId())) {
				cluster.halt(id);
			}
		}
		var shortDeadline = through.connection().getSyncApi(RequestContext.latency(Duration.ofSeconds(1)));
		long start = System.nanoTime();
		assertThrows(RocksDBException.class, () -> shortDeadline.put(0, column, key(1), text("sent"),
				RequestType.none()));
		assertTrue(System.nanoTime() - start < Duration.ofSeconds(3).toNanos(),
				"the write waited past its caller's deadline");
	}

	/**
	 * A write still queued at its caller's deadline (every batch slot taken by writes no majority answers) fails
	 * then as not applied, and so does one whose caller cancelled it: neither is ever applied, also once the writes
	 * ahead of them go through.
	 */
	@Test
	void aWriteQueuedPastItsCallersDeadlineIsNeverApplied() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withRequestTimeout(Duration.ofSeconds(30))).startAll();
		cluster.awaitLeader(TIMEOUT);
		var through = cluster.anyFollower();
		var api = api(through);
		long column = api.createColumn("c", SCHEMA);
		var halted = cluster.ids().stream().filter(id -> !id.equals(through.config().nodeId())).toList();
		for (String id : halted) {
			cluster.halt(id);
		}
		var async = through.connection().getAsyncApi(CONTEXT);
		var fillers = new ArrayList<java.util.concurrent.CompletableFuture<Void>>();
		for (int i = 0; i < Proposer.MAX_BATCHES_IN_FLIGHT; i++) {
			fillers.add(async.putAsync(0, column, key(100 + i), text("filler"), RequestType.none()));
		}
		var shortDeadline = through.connection().getSyncApi(RequestContext.latency(Duration.ofSeconds(1)));
		long start = System.nanoTime();
		var error = assertThrows(RocksDBException.class, () -> shortDeadline.put(0, column, key(2), text("queued"),
				RequestType.none()));
		assertTrue(System.nanoTime() - start < Duration.ofSeconds(3).toNanos(),
				"the queued write waited past its caller's deadline");
		assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, error.getErrorUniqueId());
		assertTrue(error.getMessage().contains("not applied"), error.getMessage());
		var cancelled = async.putAsync(0, column, key(3), text("cancelled"), RequestType.none());
		assertTrue(cancelled.cancel(true));

		for (String id : halted) {
			cluster.start(id);
		}
		for (var filler : fillers) {
			filler.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		}
		api.put(0, column, key(4), text("after"), RequestType.none());
		cluster.awaitConverged(TIMEOUT);
		for (ReplicatedNode node : cluster.runningNodes()) {
			assertEquals("filler", localValue(node, column, 100));
			assertNull(localValue(node, column, 2), "a write that failed as not applied was applied");
			assertNull(localValue(node, column, 3), "a write cancelled before it was sent was applied");
			assertEquals("after", localValue(node, column, 4));
		}
	}

	/**
	 * A write with a short caller deadline and one without a deadline share a batch while no leader can be elected.
	 * The batch is resent only until the short deadline; then only that write fails, and the other one goes back to
	 * the queue and is applied once a leader is elected.
	 */
	@Test
	void aShortDeadlineInABatchDoesNotEndTheOtherWritesOfTheBatch() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withRequestTimeout(Duration.ofSeconds(30))).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var through = cluster.anyFollower();
		var api = api(through);
		long column = api.createColumn("c", SCHEMA);
		var halted = cluster.ids().stream().filter(id -> !id.equals(through.config().nodeId())).toList();
		assertTrue(halted.contains(leader.config().nodeId()));
		for (String id : halted) {
			cluster.halt(id);
		}
		// Fillers take every batch slot until their own deadline: the two writes queue meanwhile, and then go
		// together in one batch.
		var fillerApi = through.connection().getAsyncApi(RequestContext.latency(Duration.ofMillis(500)));
		var fillers = new ArrayList<java.util.concurrent.CompletableFuture<Void>>();
		for (int i = 0; i < Proposer.MAX_BATCHES_IN_FLIGHT; i++) {
			fillers.add(fillerApi.putAsync(0, column, key(100 + i), text("filler"), RequestType.none()));
		}
		var shortWrite = through.connection().getAsyncApi(RequestContext.latency(Duration.ofSeconds(2)))
				.putAsync(0, column, key(1), text("short"), RequestType.none());
		var longWrite = through.connection().getAsyncApi(RequestContext.batch())
				.putAsync(0, column, key(2), text("long"), RequestType.none());
		var shortError = assertThrows(ExecutionException.class,
				() -> shortWrite.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
		assertInstanceOf(RocksDBException.class, shortError.getCause());
		// With no leader electable, the other write must still be pending a while later.
		assertThrows(java.util.concurrent.TimeoutException.class, () -> longWrite.get(1, TimeUnit.SECONDS),
				"the write without a deadline ended with the short one");

		for (String id : halted) {
			cluster.start(id);
		}
		longWrite.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		cluster.awaitConverged(TIMEOUT);
		for (ReplicatedNode node : cluster.runningNodes()) {
			assertEquals("long", localValue(node, column, 2));
		}
	}

	/**
	 * A write whose first attempt timed out after the leader appended it may have been applied: when every later
	 * attempt is refused (no leader is left), it fails with an unknown outcome at its caller's deadline, never as
	 * "not applied", which a client would take as safe to send again. Here the write was committed, and every
	 * replica holds it once the cluster is back.
	 */
	@Test
	void aWriteWhoseFirstAttemptMayHaveBeenAppendedIsNeverReportedNotApplied() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withRequestTimeout(Duration.ofSeconds(30))).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var through = cluster.anyFollower();
		var other = third(leader, through);
		var leaderId = leader.config().nodeId();
		var otherId = other.config().nodeId();
		long column = api(through).createColumn("c", SCHEMA);
		cluster.awaitConverged(TIMEOUT);
		long committedBefore = commitIndex(other);
		var gate = new CountDownLatch(1);
		leader.stateMachine().testHooks().applyGate = gate;
		CompletableFuture<Void> write;
		Thread halting = null;
		try {
			write = through.connection().getAsyncApi(RequestContext.latency(Duration.ofSeconds(8)))
					.putAsync(0, column, key(1), text("maybe"), RequestType.none());
			// Committed, but the leader cannot apply it, so it cannot answer: the first attempt times out after 3 s.
			InProcessCluster.await(TIMEOUT, () -> commitIndex(other) > committedBefore);
			Thread.sleep(3500);
			cluster.halt(otherId);
			// The leader opens its gate only once its leadership ended, so that no reply reaches the follower. Then
			// the follower knows no leader: every later attempt is refused before it is appended.
			halting = haltInBackground(leaderId);
			InProcessCluster.await(TIMEOUT, () -> through.leaderId() == null);
		} finally {
			gate.countDown();
			leader.stateMachine().testHooks().applyGate = null;
			if (halting != null) {
				halting.join();
			}
		}
		var error = assertThrows(ExecutionException.class, () -> write.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
		assertOutcomeUnknown(error.getCause());

		cluster.start(leaderId);
		cluster.start(otherId);
		cluster.awaitConverged(TIMEOUT);
		for (ReplicatedNode node : cluster.runningNodes()) {
			assertEquals("maybe", localValue(node, column, 1), "the write was not applied on " + node.config().nodeId());
		}
	}

	/** A write that no leader ever accepted fails at its caller's deadline as not applied, and is never applied. */
	@Test
	void aWriteThatNoLeaderAcceptedIsReportedNotApplied() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withRequestTimeout(Duration.ofSeconds(30))).startAll();
		cluster.awaitLeader(TIMEOUT);
		var through = cluster.anyFollower();
		long column = api(through).createColumn("c", SCHEMA);
		var halted = cluster.ids().stream().filter(id -> !id.equals(through.config().nodeId())).toList();
		for (String id : halted) {
			cluster.halt(id);
		}
		InProcessCluster.await(TIMEOUT, () -> through.leaderId() == null);
		var write = through.connection().getAsyncApi(RequestContext.latency(Duration.ofSeconds(1)))
				.putAsync(0, column, key(1), text("refused"), RequestType.none());
		var error = assertThrows(ExecutionException.class, () -> write.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
		var cause = assertInstanceOf(RocksDBException.class, error.getCause());
		assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, cause.getErrorUniqueId(), cause.getMessage());
		assertTrue(cause.getMessage().contains("not applied"), cause.getMessage());

		for (String id : halted) {
			cluster.start(id);
		}
		api(through).put(0, column, key(2), text("after"), RequestType.none());
		cluster.awaitConverged(TIMEOUT);
		for (ReplicatedNode node : cluster.runningNodes()) {
			assertNull(localValue(node, column, 1), "a write that failed as not applied was applied");
			assertEquals("after", localValue(node, column, 2));
		}
	}

	/**
	 * A follower's attempt that times out while the leader still has the batch pending is resent as the same Ratis
	 * invocation: the leader answers it from its retry cache when the first copy is applied, instead of appending
	 * the whole batch again as a second log entry.
	 */
	@Test
	void aFollowersResendWhileTheLeaderIsSlowIsNotAppendedAgain() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		long column = api(follower).createColumn("c", SCHEMA);
		api(follower).put(0, column, key(1), text("before"), RequestType.none());
		cluster.awaitConverged(TIMEOUT);
		long before = leader.appliedIndex();
		var gate = new java.util.concurrent.CountDownLatch(1);
		leader.stateMachine().testHooks().applyGate = gate;
		java.util.concurrent.CompletableFuture<Void> write;
		try {
			write = follower.connection().getAsyncApi(CONTEXT).putAsync(0, column, key(1), text("slow"),
					RequestType.none());
			// Longer than one attempt (3 s with these election timeouts) and shorter than the retry cache expiry plus
			// it: the follower resent the batch once, while the leader could not apply it.
			Thread.sleep(4500);
		} finally {
			gate.countDown();
			leader.stateMachine().testHooks().applyGate = null;
		}
		write.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		cluster.awaitConverged(TIMEOUT);
		assertEquals("slow", localValue(leader, column, 1));
		assertEquals(before + 1, leader.appliedIndex(), "the resent batch was appended again");
	}

	@Test
	void writesContinueAfterLeaderLossAndTheOldLeaderCatchesUp() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		long column = api(leader).createColumn("c", SCHEMA);
		for (int i = 0; i < 50; i++) {
			api(leader).merge(0, column, key(1), text(Integer.toString(i)), RequestType.none());
		}
		String oldLeader = leader.config().nodeId();
		cluster.halt(oldLeader);
		var survivor = cluster.runningNodes().getFirst();
		for (int i = 50; i < 100; i++) {
			api(survivor).merge(0, column, key(1), text(Integer.toString(i)), RequestType.none());
		}
		var expected = String.join(",", java.util.stream.IntStream.range(0, 100).mapToObj(Integer::toString).toList());
		assertEquals(expected, text(api(survivor).get(0, column, key(1), RequestType.current())));

		cluster.start(oldLeader);
		cluster.awaitConverged(TIMEOUT);
		// Each merge was applied exactly once on every replica, including across the restart.
		for (ReplicatedNode node : cluster.runningNodes()) {
			assertEquals(expected, localValue(node, column, 1));
		}
	}

	/**
	 * A write that gives up because no majority answered for its whole request timeout must not break later
	 * writes through the same node (found by {@link RandomFaultTest}: after an isolation, every write proposed by
	 * the formerly isolated node kept failing until it was restarted).
	 */
	@Test
	void writesThroughANodeRecoverAfterAProposalTimedOut() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withRequestTimeout(Duration.ofSeconds(1))).startAll();
		cluster.awaitLeader(TIMEOUT);
		var node = cluster.anyFollower();
		var api = api(node);
		long column = api.createColumn("c", SCHEMA);
		var others = cluster.ids().stream().filter(id -> !id.equals(node.config().nodeId())).toList();
		for (String id : others) {
			cluster.halt(id);
		}
		assertThrows(RocksDBException.class, () -> api.put(0, column, key(1), text("lost"), RequestType.none()));

		for (String id : others) {
			cluster.start(id);
		}
		cluster.awaitLeader(TIMEOUT);
		api.put(0, column, key(2), text("after"), RequestType.none());
		assertEquals("after", text(api.get(0, column, key(2), RequestType.current())));
	}

	@Test
	void laggingFollowerCatchesUpFromASnapshot() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withSnapshotEveryEntries(64)).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		long column = api(leader).createColumn("c", SCHEMA);
		var lagging = cluster.anyFollower().config().nodeId();
		cluster.halt(lagging);
		var api = api(cluster.awaitLeader(TIMEOUT));
		for (int i = 0; i < 3000; i++) {
			api.put(0, column, key(i), text("v" + i), RequestType.none());
		}
		cluster.start(lagging);
		cluster.awaitConverged(Duration.ofSeconds(60));
		var node = cluster.node(lagging);
		for (int i = 0; i < 3000; i += 97) {
			assertEquals("v" + i, localValue(node, column, i));
		}
	}

	/**
	 * A follower that is down while many snapshots are taken still catches up from the log: the leader purges only
	 * entries every member committed, so a restarted member never needs a full database download. (Snapshot
	 * installs for new members are covered by {@code MembershipTest}.)
	 */
	@Test
	void followerDownAcrossManySnapshotsCatchesUpFromTheRetainedLog() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withSnapshotEveryEntries(64)).startAll();
		long column = api(cluster.awaitLeader(TIMEOUT)).createColumn("c", SCHEMA);
		var lagging = cluster.anyFollower();
		var laggingId = lagging.config().nodeId();
		long laggingApplied = lagging.appliedIndex();
		cluster.halt(laggingId);
		int written = 0;
		// A new term starts a new log segment, and only closed segments are purged: change leaders a few times.
		for (int term = 0; term < 3; term++) {
			var leader = cluster.awaitLeader(TIMEOUT);
			for (int i = 0; i < 600; i++, written++) {
				putUntilAccepted(leader, column, written);
			}
			var leaderId = leader.config().nodeId();
			cluster.stop(leaderId);
			cluster.start(leaderId);
		}
		var leader = cluster.awaitLeader(TIMEOUT);
		putUntilAccepted(leader, column, written);
		written++;
		long logStart = leader.stateMachine().division().getRaftLog().getStartIndex();
		assertTrue(logStart <= laggingApplied + 1, "log purged past the lagging follower: starts at " + logStart
				+ ", follower applied " + laggingApplied);

		cluster.start(laggingId);
		cluster.awaitConverged(Duration.ofSeconds(60));
		var node = cluster.node(laggingId);
		assertFalse(node.stateMachine().isFailed());
		for (int i = 0; i < written; i += 97) {
			assertEquals("v" + i, localValue(node, column, i));
		}
		assertEquals("v" + (written - 1), localValue(node, column, written - 1));
		// The installed replica keeps applying new entries.
		api(cluster.awaitLeader(TIMEOUT)).put(0, column, key(written), text("after"), RequestType.none());
		cluster.awaitConverged(TIMEOUT);
		assertEquals("after", localValue(node, column, written));
	}

	/**
	 * With only two members left, the old leader can stop before the reply to its successor's first entry goes out: that
	 * leader then takes the bound of {@link LeaderStall} to start over, longer than one request waits for a leader.
	 */
	private void putUntilAccepted(ReplicatedNode through, long column, int index) {
		long deadline = System.nanoTime() + TIMEOUT.toNanos();
		while (true) {
			try {
				api(through).put(0, column, key(index), text("v" + index), RequestType.none());
				return;
			} catch (RuntimeException e) {
				if (!ClusterClient.isRetryable(e) || System.nanoTime() - deadline > 0) {
					throw e;
				}
			}
		}
	}

	/**
	 * Ratis closes its server by itself after a long JVM pause. The node stops as after a failed apply, and what the
	 * closed division remembers about the leader does not count until then.
	 */
	@Test
	void aNodeWhoseRaftClosedStops() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		assertTrue(follower.knowsLeader());

		follower.raftServerForTests().close();

		assertFalse(follower.knowsLeader());
		var cause = follower.stopped().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		assertNotNull(cause);
		assertTrue(cause.getMessage().contains("Raft closed"), cause::toString);
		cluster.halt(follower.config().nodeId());
		cluster.awaitLeader(TIMEOUT);
	}

	/**
	 * A leader whose apply fails stops, consensus first, and the other two elect a new leader and go on. The write
	 * whose apply failed was committed, so the others apply it: its caller gets its result, or an unknown outcome,
	 * never "not applied".
	 */
	@Test
	void aLeaderWhoseApplyFailsStopsAndTheOthersTakeOver() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var through = cluster.anyFollower();
		long column = api(through).createColumn("c", SCHEMA);
		cluster.awaitConverged(TIMEOUT);
		var failure = new IllegalStateException("Injected apply failure");
		leader.stateMachine().testHooks().applyFailure = failure;

		var write = through.connection().getAsyncApi(CONTEXT).putAsync(0, column, key(1), text("written"),
				RequestType.none());
		assertSame(failure, leader.stopped().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
		assertTrue(leader.isFailed());
		try {
			write.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		} catch (ExecutionException ex) {
			assertOutcomeUnknown(ex.getCause());
		}
		// Already stopped: the cluster only forgets it.
		cluster.halt(leader.config().nodeId());
		var newLeader = cluster.awaitLeader(TIMEOUT);
		// A new leader's applied index can still be behind the write: wait for the write itself.
		for (ReplicatedNode node : cluster.runningNodes()) {
			InProcessCluster.await(TIMEOUT, () -> localValue(node, column, 1) != null);
			assertEquals("written", localValue(node, column, 1));
		}
		api(through).put(0, column, key(2), text("after"), RequestType.none());
		assertEquals("after", text(api(through).get(0, column, key(2), RequestType.current())));
		assertEquals("after", text(api(newLeader).get(0, column, key(2), RequestType.current())));

		long start = System.nanoTime();
		cluster.close();
		assertTrue(System.nanoTime() - start < TIMEOUT.toNanos(), "the survivors took too long to stop");
	}

	/**
	 * A follower whose apply fails while Ratis waits for room in its apply queue stops (the failure frees the queue),
	 * and the leader goes on with the other follower.
	 */
	@Test
	void aFollowerWhoseApplyFailsWithAFullApplyQueueStops() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var failing = cluster.anyFollower();
		var stateMachine = failing.stateMachine();
		long column = api(leader).createColumn("c", SCHEMA);
		cluster.awaitConverged(TIMEOUT);
		stateMachine.testHooks().limitApplyQueue(QUEUE_LIMIT_KIB);
		var gate = new CountDownLatch(1);
		stateMachine.testHooks().applyGate = gate;
		var failure = new IllegalStateException("Injected apply failure");
		try {
			writeLarge(leader, column, 0, 64);
			long committed = commitIndex(leader);
			InProcessCluster.await(TIMEOUT, () -> commitIndex(failing) >= committed);
			assertTrue(stateMachine.handedOverForTesting() < committed - 32,
					"precondition: Ratis waits for room in the apply queue, handed over "
							+ stateMachine.handedOverForTesting() + " of " + committed);
			stateMachine.testHooks().applyFailure = failure;
		} finally {
			gate.countDown();
			stateMachine.testHooks().applyGate = null;
		}
		assertSame(failure, failing.stopped().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
		assertTrue(failing.isFailed());
		cluster.halt(failing.config().nodeId());

		api(leader).put(0, column, key(1000), text("after"), RequestType.none());
		cluster.awaitConverged(TIMEOUT);
		for (ReplicatedNode node : cluster.runningNodes()) {
			assertEquals("after", localValue(node, column, 1000));
		}
		long start = System.nanoTime();
		cluster.close();
		assertTrue(System.nanoTime() - start < TIMEOUT.toNanos(), "the others took too long to stop");
	}

	/**
	 * A follower that cannot apply keeps its backlog in the Raft log: Ratis hands it entries only while its apply
	 * queue has room, so at most the queue limit of entries waits decoded on the heap. Once it applies again it
	 * catches up, and every KiB of the queue is free again.
	 */
	@Test
	void aFollowerThatCannotApplyHoldsItsBacklogInTheLogAndCatchesUp() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var lagging = cluster.anyFollower();
		var stateMachine = lagging.stateMachine();
		long column = api(leader).createColumn("c", SCHEMA);
		cluster.awaitConverged(TIMEOUT);
		stateMachine.testHooks().limitApplyQueue(QUEUE_LIMIT_KIB);
		int largeWrites = 256;
		var gate = new CountDownLatch(1);
		stateMachine.testHooks().applyGate = gate;
		try {
			long applied = lagging.appliedIndex();
			// 4 MiB, written through the leader and the other follower.
			writeLarge(leader, column, 0, largeWrites);
			long committed = commitIndex(leader);
			InProcessCluster.await(TIMEOUT, () -> commitIndex(lagging) >= committed);
			long handedOver = stateMachine.handedOverForTesting();
			assertTrue(handedOver - applied <= QUEUE_LIMIT_KIB / 16,
					"handed over " + handedOver + " with " + applied + " applied, beyond the queue limit");
			assertEquals(applied, lagging.appliedIndex());
		} finally {
			gate.countDown();
			stateMachine.testHooks().applyGate = null;
		}
		cluster.awaitConverged(TIMEOUT);
		for (int i = 0; i < largeWrites; i += 17) {
			assertEquals(LARGE_VALUE.size(),
					lagging.stateMachine().store().db().get(0, column, key(i), RequestType.current()).size());
		}
		InProcessCluster.await(TIMEOUT, () -> stateMachine.freeQueuedKiBForTesting() == QUEUE_LIMIT_KIB);

		// With a permit lost per entry, these would fill the queue for good.
		for (int i = 0; i < 1000; i++) {
			api(leader).put(0, column, key(i), text("small" + i), RequestType.none());
		}
		cluster.awaitConverged(TIMEOUT);
		assertEquals("small999", localValue(lagging, column, 999));
		InProcessCluster.await(TIMEOUT, () -> stateMachine.freeQueuedKiBForTesting() == QUEUE_LIMIT_KIB);
	}

	@Test
	void batchWritesAreReplicated() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var api = api(cluster.anyFollower());
		long column = api.createColumn("c", SCHEMA);
		var batches = new ArrayList<KVBatch>();
		for (int b = 0; b < 10; b++) {
			var keys = new ArrayList<Keys>();
			var values = new ArrayList<Buf>();
			for (int i = 0; i < 100; i++) {
				keys.add(key(b * 100L + i));
				values.add(text("b" + b + "i" + i));
			}
			batches.add(new KVBatch.KVBatchRef(keys, values));
		}
		api.putBatch(column, Flux.fromIterable(batches), PutBatchMode.WRITE_BATCH);
		cluster.awaitConverged(TIMEOUT);
		for (ReplicatedNode node : cluster.runningNodes()) {
			assertEquals("b9i99", localValue(node, column, 999));
		}
	}

	/**
	 * A batch whose later chunk fails after an earlier chunk was applied reports a non-retryable error that says so,
	 * not the later chunk's own retryable "not applied" error: a client resending the whole batch would merge the
	 * first chunk twice. Here the second batch arrives after the caller's deadline, so it is never sent.
	 */
	@Test
	void aBatchWhoseLaterChunkFailsReportsThatItWasPartlyApplied() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var through = cluster.anyFollower();
		long column = api(through).createColumn("c", SCHEMA);
		KVBatch first = new KVBatch.KVBatchRef(List.of(key(1)), List.of(text("first")));
		KVBatch second = new KVBatch.KVBatchRef(List.of(key(2)), List.of(text("second")));
		var batches = Flux.concat(Flux.just(first),
				reactor.core.publisher.Mono.delay(Duration.ofSeconds(2)).thenMany(Flux.just(second)));
		var call = through.connection().getAsyncApi(RequestContext.batch(Duration.ofSeconds(1)))
				.mergeBatchAsync(column, batches, it.cavallium.rockserver.core.common.MergeBatchMode.MERGE_WRITE_BATCH);
		var error = assertThrows(ExecutionException.class, () -> call.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
		var cause = assertInstanceOf(RocksDBException.class, error.getCause());
		assertEquals(RocksDBErrorType.INVALID_REQUEST, cause.getErrorUniqueId());
		assertTrue(cause.getMessage().contains("partly applied"), cause.getMessage());
		assertEquals("first", text(api(through).get(0, column, key(1), RequestType.current())));
		assertNull(api(through).get(0, column, key(2), RequestType.current()));
	}

	@Test
	void transactionsCommitAtomicallyAndDetectConflicts() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var api = api(cluster.anyFollower());
		long column = api.createColumn("c", SCHEMA);
		api.put(0, column, key(1), text("initial"), RequestType.none());

		long tx = api.openTransaction(Duration.ofSeconds(30));
		var update = api.get(tx, column, key(1), RequestType.forUpdate());
		assertEquals("initial", text(update.previous()));
		api.put(tx, column, key(1), text("tx"), RequestType.none());
		api.put(tx, column, key(2), text("tx2"), RequestType.none());
		// Not visible outside the transaction before commit.
		assertNull(api.get(0, column, key(2), RequestType.current()));
		assertTrue(api.closeTransaction(tx, true));
		assertEquals("tx", text(api.get(0, column, key(1), RequestType.current())));
		assertEquals("tx2", text(api.get(0, column, key(2), RequestType.current())));

		long conflicting = api.openTransaction(Duration.ofSeconds(30));
		api.get(conflicting, column, key(1), RequestType.forUpdate());
		api.put(0, column, key(1), text("outside"), RequestType.none());
		api.put(conflicting, column, key(1), text("lost update"), RequestType.none());
		assertFalse(api.closeTransaction(conflicting, true), "a concurrent write must make the commit fail");
		assertTrue(api.closeTransaction(conflicting, false));
		assertEquals("outside", text(api.get(0, column, key(1), RequestType.current())));
	}

	@Test
	void aTransactionIsRefusedTheWriteThatWouldMakeItsCommitTooLarge() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var api = api(cluster.anyFollower());
		long column = api.createColumn("c", SCHEMA);
		var value = Buf.wrap(new byte[4 * 1024 * 1024]);
		long tx = api.openTransaction(Duration.ofSeconds(30));
		for (int i = 0; i < 3; i++) {
			api.put(tx, column, key(i), value, RequestType.none());
		}
		// A fourth value would take the commit past 16 MiB: refused now, not at commit.
		var error = assertThrows(RocksDBException.class, () -> api.put(tx, column, key(3), value, RequestType.none()));
		assertEquals(RocksDBErrorType.INVALID_REQUEST, error.getErrorUniqueId());
		// The transaction stays open without that write, as after a failed write on a single node.
		api.put(tx, column, key(4), text("small"), RequestType.none());
		assertTrue(api.closeTransaction(tx, true));
		assertEquals(value.size(), api.get(0, column, key(2), RequestType.current()).size());
		assertNull(api.get(0, column, key(3), RequestType.current()));
		assertEquals("small", text(api.get(0, column, key(4), RequestType.current())));
	}

	@Test
	void theLeaseOfATransactionIsCapped() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withMaxTransactionLease(Duration.ofSeconds(1))).startAll();
		var api = api(cluster.anyFollower());
		long column = api.createColumn("c", SCHEMA);
		long tx = api.openTransaction(Duration.ofHours(1));
		api.put(tx, column, key(1), text("tx"), RequestType.none());
		Thread.sleep(2_000);
		// Expired after the capped lease, not the requested hour: the client must run it again.
		assertThrows(RocksDBRetryException.class, () -> api.closeTransaction(tx, true));
		assertNull(api.get(0, column, key(1), RequestType.current()));
	}

	@Test
	void updatesDetectConflicts() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var api = api(cluster.anyFollower());
		long column = api.createColumn("c", SCHEMA);
		var first = api.get(0, column, key(1), RequestType.forUpdate());
		api.put(first.updateId(), column, key(1), text("one"), RequestType.none());
		assertEquals("one", text(api.get(0, column, key(1), RequestType.current())));

		var stale = api.get(0, column, key(1), RequestType.forUpdate());
		api.put(0, column, key(1), text("two"), RequestType.none());
		assertThrows(RocksDBRetryException.class,
				() -> api.put(stale.updateId(), column, key(1), text("lost"), RequestType.none()));
		assertEquals("two", text(api.get(0, column, key(1), RequestType.current())));
	}

	@Test
	void anUpdateWhoseCallerIsGoneDoesNotTakeTheKey() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var api = api(leader);
		long column = api.createColumn("c", SCHEMA);
		var holder = api.get(0, column, key(1), RequestType.forUpdate());

		// Queued behind the holder: one caller whose deadline passes, and one that cancels.
		var timedOut = leader.connection().getAsyncApi(RequestContext.latency(Duration.ofMillis(300)))
				.getAsync(0, column, key(1), RequestType.forUpdate());
		var cancelled = leader.connection().getAsyncApi(CONTEXT).getAsync(0, column, key(1), RequestType.forUpdate());
		var error = assertThrows(ExecutionException.class, () -> timedOut.get(5, TimeUnit.SECONDS));
		assertInstanceOf(RocksDBRetryException.class, error.getCause());
		cancelled.cancel(true);

		api.closeFailedUpdate(holder.updateId());
		// The key is free at once: neither gone caller opened an update that nobody can close.
		var next = leader.connection().getAsyncApi(RequestContext.latency(Duration.ofSeconds(3)))
				.getAsync(0, column, key(1), RequestType.forUpdate())
				.get(5, TimeUnit.SECONDS);
		api.closeFailedUpdate(next.updateId());
	}

	@Test
	void anUpdateCancelledWhileItOpensDoesNotKeepTheKey() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var api = api(leader);
		long column = api.createColumn("c", SCHEMA);
		var async = leader.connection().getAsyncApi(CONTEXT);
		// Cancelled at varying points of the open: some land after the update registered, before its id is returned.
		for (int i = 0; i < 300; i++) {
			var update = async.getAsync(0, column, key(1), RequestType.forUpdate());
			LockSupport.parkNanos((i % 100) * 100_000L);
			if (!update.cancel(true)) {
				api.closeFailedUpdate(update.get(5, TimeUnit.SECONDS).updateId());
			}
		}
		// The key is free well before the update lease ends: every cancelled update was closed.
		var next = leader.connection().getAsyncApi(RequestContext.latency(Duration.ofSeconds(3)))
				.getAsync(0, column, key(1), RequestType.forUpdate())
				.get(5, TimeUnit.SECONDS);
		api.closeFailedUpdate(next.updateId());
	}

	@Test
	void transactionsDoNotSurviveALeaderChange() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		var api = api(follower);
		long column = api.createColumn("c", SCHEMA);
		long tx = api.openTransaction(Duration.ofSeconds(30));
		api.put(tx, column, key(1), text("tx"), RequestType.none());
		cluster.stop(leader.config().nodeId());
		cluster.awaitLeader(TIMEOUT);
		assertThrows(RocksDBRetryException.class, () -> api.closeTransaction(tx, true));
		assertNull(api.get(0, column, key(1), RequestType.current()));
	}

	/**
	 * A commit forwarded by a follower, whose call the stopping leader drops after it appended the commit, fails
	 * with an unknown outcome. It is not sent again to the next leader, which does not know the transaction and
	 * would answer "retry" although the commit was applied: a client running the transaction again would apply it
	 * twice. The commit is applied once.
	 */
	@Test
	void aForwardedCommitLostInTransportHasAnUnknownOutcomeAndIsAppliedOnce() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var through = cluster.anyFollower();
		var other = third(leader, through);
		var leaderId = leader.config().nodeId();
		var api = api(through);
		long column = api.createColumn("c", SCHEMA);
		long tx = api.openTransaction(Duration.ofSeconds(60));
		api.merge(tx, column, key(1), text("tx"), RequestType.none());
		cluster.awaitConverged(TIMEOUT);
		long committedBefore = commitIndex(other);
		var gate = new CountDownLatch(1);
		leader.stateMachine().testHooks().applyGate = gate;
		var graceful = System.getProperty(GRPC_GRACEFUL_SHUTDOWN);
		Thread halting = null;
		CompletableFuture<Boolean> commit;
		try {
			commit = through.connection().getAsyncApi(RequestContext.batch(Duration.ofSeconds(10)))
					.closeTransactionAsync(tx, true);
			// Committed, but the leader cannot apply it, so it cannot answer.
			InProcessCluster.await(TIMEOUT, () -> commitIndex(other) > committedBefore);
			// The leader's API server drops the call in progress instead of waiting for it.
			System.setProperty(GRPC_GRACEFUL_SHUTDOWN, "0");
			halting = haltInBackground(leaderId);
			var error = assertThrows(ExecutionException.class, () -> commit.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
			assertInstanceOf(RocksDBException.class, error.getCause(), () -> String.valueOf(error.getCause()));
			assertFalse(error.getCause() instanceof RocksDBRetryException, "a commit that may have been applied"
					+ " reported as safe to run again: " + error.getCause());
			var cause = (RocksDBException) error.getCause();
			assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, cause.getErrorUniqueId(), cause.getMessage());
			assertTrue(cause.getMessage().contains("outcome of the commit is unknown"), cause.getMessage());
		} finally {
			gate.countDown();
			leader.stateMachine().testHooks().applyGate = null;
			if (halting != null) {
				halting.join();
			}
			if (graceful == null) {
				System.clearProperty(GRPC_GRACEFUL_SHUTDOWN);
			} else {
				System.setProperty(GRPC_GRACEFUL_SHUTDOWN, graceful);
			}
		}

		cluster.awaitConverged(TIMEOUT);
		for (ReplicatedNode node : cluster.runningNodes()) {
			assertEquals("tx", localValue(node, column, 1));
		}
		cluster.start(leaderId);
		cluster.awaitConverged(TIMEOUT);
		for (ReplicatedNode node : cluster.runningNodes()) {
			assertEquals("tx", localValue(node, column, 1), "on " + node.config().nodeId());
		}
	}

	/**
	 * An update forwarded by a follower, whose call the stopping leader drops while it waits there for its key,
	 * fails with a retryable error: it is not opened again on the next leader, since the old leader may still be
	 * alive and have opened it.
	 */
	@Test
	void aForwardedUpdateLostInTransportIsNotOpenedAgain() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var through = cluster.anyFollower();
		long column = api(through).createColumn("c", SCHEMA);
		// Connects the follower to the leader's API.
		assertNull(api(through).get(0, column, key(1), RequestType.current()));
		// Held until the leader stops.
		api(leader).get(0, column, key(1), RequestType.forUpdate());
		var graceful = System.getProperty(GRPC_GRACEFUL_SHUTDOWN);
		try {
			var waiting = through.connection().getAsyncApi(CONTEXT).getAsync(0, column, key(1), RequestType.forUpdate());
			// Time for it to reach the leader and wait there behind the holder.
			Thread.sleep(500);
			assertFalse(waiting.isDone(), "precondition: the update waits for the key");
			System.setProperty(GRPC_GRACEFUL_SHUTDOWN, "0");
			cluster.halt(leader.config().nodeId());
			var error = assertThrows(ExecutionException.class, () -> waiting.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
			assertInstanceOf(RocksDBRetryException.class, error.getCause(), () -> String.valueOf(error.getCause()));
		} finally {
			if (graceful == null) {
				System.clearProperty(GRPC_GRACEFUL_SHUTDOWN);
			} else {
				System.setProperty(GRPC_GRACEFUL_SHUTDOWN, graceful);
			}
		}
		cluster.awaitLeader(TIMEOUT);
		// The next leader holds no update of the key.
		var next = through.connection().getAsyncApi(RequestContext.latency(Duration.ofSeconds(3)))
				.getAsync(0, column, key(1), RequestType.forUpdate())
				.get(5, TimeUnit.SECONDS);
		api(through).closeFailedUpdate(next.updateId());
	}

	/**
	 * Commits that queue while every commit slot is taken go together in one log entry. When one of them conflicts
	 * with an earlier one of the same entry, the leader refuses the entry: only that commit fails, and its
	 * transaction stays open, while the others go again together in the next entry.
	 */
	@Test
	void onlyTheConflictingCommitsOfABatchFail() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var api = api(leader);
		long column = api.createColumn("c", SCHEMA);
		var fillers = new long[Proposer.MAX_BATCHES_IN_FLIGHT];
		for (int i = 0; i < fillers.length; i++) {
			fillers[i] = api.openTransaction(Duration.ofSeconds(60));
			api.put(fillers[i], column, key(100 + i), text("filler"), RequestType.none());
		}
		long first = api.openTransaction(Duration.ofSeconds(60));
		api.put(first, column, key(1), text("first"), RequestType.none());
		long second = api.openTransaction(Duration.ofSeconds(60));
		api.put(second, column, key(1), text("second"), RequestType.none());
		long third = api.openTransaction(Duration.ofSeconds(60));
		api.put(third, column, key(2), text("third"), RequestType.none());
		cluster.awaitConverged(TIMEOUT);
		long before = leader.appliedIndex();

		var async = leader.connection().getAsyncApi(CONTEXT);
		var fillerCommits = new ArrayList<CompletableFuture<Boolean>>();
		var gate = new CountDownLatch(1);
		leader.stateMachine().testHooks().applyGate = gate;
		CompletableFuture<Boolean> firstCommit;
		CompletableFuture<Boolean> secondCommit;
		CompletableFuture<Boolean> thirdCommit;
		try {
			for (long filler : fillers) {
				fillerCommits.add(async.closeTransactionAsync(filler, true));
			}
			// Every commit slot is taken by a filler that cannot be applied yet: these three queue, then go together.
			firstCommit = async.closeTransactionAsync(first, true);
			secondCommit = async.closeTransactionAsync(second, true);
			thirdCommit = async.closeTransactionAsync(third, true);
		} finally {
			gate.countDown();
			leader.stateMachine().testHooks().applyGate = null;
		}
		for (var commit : fillerCommits) {
			assertTrue(commit.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
		}
		assertTrue(firstCommit.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
		assertFalse(secondCommit.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS), "the conflicting commit succeeded");
		assertTrue(thirdCommit.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
		// Still open, so its commit conflicts again, instead of failing because the transaction is gone.
		assertFalse(api.closeTransaction(second, true));
		assertTrue(api.closeTransaction(second, false));

		cluster.awaitConverged(TIMEOUT);
		// One entry per filler, and one for the first and third commits together: the refused entry was not appended.
		assertEquals(before + Proposer.MAX_BATCHES_IN_FLIGHT + 1, leader.appliedIndex());
		for (ReplicatedNode node : cluster.runningNodes()) {
			assertEquals("first", localValue(node, column, 1));
			assertEquals("third", localValue(node, column, 2));
			for (int i = 0; i < fillers.length; i++) {
				assertEquals("filler", localValue(node, column, 100 + i));
			}
		}
	}

	@Test
	void restartOfTheWholeClusterKeepsEveryAcknowledgedWrite() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var api = api(cluster.anyFollower());
		long column = api.createColumn("c", SCHEMA);
		for (int i = 0; i < 200; i++) {
			api.merge(0, column, key(i % 10), text("m" + i), RequestType.none());
		}
		for (String id : cluster.ids()) {
			cluster.halt(id);
		}
		cluster.startAll();
		cluster.awaitLeader(TIMEOUT);
		var after = api(cluster.anyFollower());
		for (int k = 0; k < 10; k++) {
			var expected = new ArrayList<String>();
			for (int i = k; i < 200; i += 10) {
				expected.add("m" + i);
			}
			assertEquals(String.join(",", expected), text(after.get(0, column, key(k), RequestType.current())));
		}
	}
}
