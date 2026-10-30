package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
				follower.stateMachine().applyGateForTesting = gate;
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
				follower.stateMachine().applyGateForTesting = null;
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
				api(leader).put(0, column, key(written), text("v" + written), RequestType.none());
			}
			var leaderId = leader.config().nodeId();
			cluster.stop(leaderId);
			cluster.start(leaderId);
		}
		var leader = cluster.awaitLeader(TIMEOUT);
		api(leader).put(0, column, key(written), text("v" + written), RequestType.none());
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
