package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.UpdateContext;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Updates of one key with {@link NodeConfig#pipelinedUpdates()} on, on real Ratis servers in one JVM. Most tests stop
 * the leader's apply with {@code applyGateForTesting}, which keeps chain links unsettled for as long as a test needs.
 */
class PipelinedUpdatesTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));

	@TempDir
	Path temp;
	private InProcessCluster cluster;
	private CountDownLatch gate;
	private ReplicatedNode gated;
	private ReplicatedNode leader;
	private long column;

	@AfterEach
	void tearDown() throws Exception {
		openGate();
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

	private static RocksDBAsyncAPI async(ReplicatedNode node) {
		return node.connection().getAsyncApi(CONTEXT);
	}

	private void start() throws Exception {
		start(c -> c);
	}

	private void start(java.util.function.UnaryOperator<NodeConfig> customize) throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> customize.apply(c.withPipelinedUpdates(true))).startAll();
		leader = cluster.awaitLeader(TIMEOUT);
		column = api(leader).createColumn("c", SCHEMA);
		api(leader).put(0, column, key(1), text("v0"), RequestType.none());
	}

	/** Stops the apply of {@code node}, whose admitted commits then stay unsettled. */
	private void closeGate(ReplicatedNode node) {
		gate = new CountDownLatch(1);
		gated = node;
		node.stateMachine().applyGateForTesting = gate;
	}

	private void openGate() {
		if (gate != null) {
			gate.countDown();
			gated.stateMachine().applyGateForTesting = null;
			gate = null;
		}
	}

	private static <T> T await(CompletableFuture<T> future) throws Exception {
		return future.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
	}

	private UpdateContext<Buf> forUpdate(long key) throws Exception {
		return await(async(leader).getAsync(0, column, key(key), RequestType.<Buf>forUpdate()));
	}

	private CompletableFuture<UpdateContext<Buf>> forUpdateLater(long key) {
		return async(leader).getAsync(0, column, key(key), RequestType.<Buf>forUpdate());
	}

	private CompletableFuture<?> put(UpdateContext<Buf> update, long key, String value) {
		return async(leader).putAsync(update.updateId(), column, key(key), text(value), RequestType.none());
	}

	private static boolean chained(UpdateContext<Buf> update) {
		return update.updateId() > 0;
	}

	private static long count(ReplicatedNode node, String metric) {
		return node.metrics().amount(metric).count();
	}

	/** {@code id} goes down with its apply stopped, as a crash would leave it, and a new leader takes over. */
	private void haltAndAwaitLeader(String id) throws Exception {
		var halt = Thread.ofPlatform().start(() -> {
			try {
				cluster.halt(id);
			} catch (IOException ex) {
				throw new UncheckedIOException(ex);
			}
		});
		openGate();
		halt.join();
		leader = cluster.awaitLeader(TIMEOUT);
	}

	private static long lastLogIndex(ReplicatedNode node) {
		return node.stateMachine().division().getRaftLog().getLastEntryTermIndex().getIndex();
	}

	/** Waits for {@code action}'s write to be in the leader's log, past the index it had before. */
	private <T> CompletableFuture<T> appended(java.util.function.Supplier<CompletableFuture<T>> action) {
		long before = lastLogIndex(leader);
		var result = action.get();
		InProcessCluster.await(TIMEOUT, () -> lastLogIndex(leader) > before || result.isDone());
		return result;
	}

	private static void assertRetry(CompletableFuture<?> future) {
		var error = assertThrows(ExecutionException.class, () -> future.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
		assertInstanceOf(RocksDBRetryException.class, error.getCause(), String.valueOf(error.getCause()));
	}

	private static void assertStaysPending(CompletableFuture<?> future) throws Exception {
		assertThrows(TimeoutException.class, () -> future.get(300, TimeUnit.MILLISECONDS));
	}

	private void assertReplicasHold(long key, String expected) {
		cluster.awaitConverged(TIMEOUT);
		for (var node : cluster.runningNodes()) {
			assertEquals(expected, text(node.stateMachine().store().db().get(0, column, key(key), RequestType.current())),
					"on " + node.config().nodeId());
		}
	}

	/** A commit of the chained {@code update} whose guard says it read {@code read}, which it did not. */
	private CompletableFuture<Object> commitWithWrongGuard(UpdateContext<Buf> update, String read, String value) {
		var term = leader.stateMachine().division().getInfo().getCurrentTerm();
		var transaction = leader.stateMachine().leaderSession(term).get(update.updateId());
		long hash = ConflictTracker.keyHash(column, key(1));
		var entry = new LogEntry.Guarded(term, transaction.snapshotIndex, new long[] {column}, new long[] {hash},
				List.of(new LogEntry.Put(column, key(1), text(value), RequestTypeId.NOTHING)), true, update.updateId(),
				LogEntry.Guard.of(column, key(1), text(read)));
		return leader.proposer().proposeOnce(entry, Proposer.NO_CALLER_DEADLINE);
	}

	/** One client, every update after the first seeing the last value before any commit was applied. */
	@Test
	void updatesOfOneKeyRunWithoutWaitingForTheApply() throws Exception {
		start();
		closeGate(leader);
		int count = 30;
		var puts = new ArrayList<CompletableFuture<?>>();
		for (int i = 1; i <= count; i++) {
			var update = forUpdate(1);
			assertEquals("v" + (i - 1), text(update.previous()));
			assertEquals(i > 1, chained(update), "only the first one reads the database");
			puts.add(put(update, 1, "v" + i));
		}
		assertTrue(puts.stream().noneMatch(CompletableFuture::isDone), "nothing is acknowledged before the apply");
		assertEquals("v0", text(leader.stateMachine().store().db().get(0, column, key(1), RequestType.current())));
		openGate();
		for (var put : puts) {
			await(put);
		}
		assertReplicasHold(1, "v" + count);
	}

	@Test
	void anAbsentValueAndADeleteChainToo() throws Exception {
		start();
		closeGate(leader);
		var first = forUpdate(2);
		assertNull(first.previous());
		var created = put(first, 2, "created");
		var second = forUpdate(2);
		assertEquals("created", text(second.previous()));
		var deleted = async(leader).deleteAsync(second.updateId(), column, key(2), RequestType.none());
		var third = forUpdate(2);
		assertTrue(chained(third));
		assertNull(third.previous());
		var again = put(third, 2, "again");
		openGate();
		await(created);
		await(deleted);
		await(again);
		assertReplicasHold(2, "again");
	}

	@Test
	void aFollowerClientRunsAChainToo() throws Exception {
		start();
		var client = async(cluster.anyFollower());
		closeGate(leader);
		var puts = new ArrayList<CompletableFuture<?>>();
		for (int i = 1; i <= 20; i++) {
			var update = await(client.getAsync(0, column, key(1), RequestType.<Buf>forUpdate()));
			assertEquals("v" + (i - 1), text(update.previous()));
			assertEquals(i > 1, chained(update), "only the first one reads the database");
			puts.add(client.putAsync(update.updateId(), column, key(1), text("v" + i), RequestType.none()));
		}
		openGate();
		for (var put : puts) {
			await(put);
		}
		assertReplicasHold(1, "v20");
	}

	@Test
	void aWrongGuardIsRejectedAndTheKeyRecoversOnTheRealValue() throws Exception {
		start();
		closeGate(leader);
		var head = forUpdate(1);
		var headPut = put(head, 1, "v1");
		var chained = forUpdate(1);
		assertEquals("v1", text(chained.previous()));
		var rejected = commitWithWrongGuard(chained, "wrong", "crafted");
		// Its successor reads a value that will never exist, and is rejected in turn.
		var successor = forUpdate(1);
		assertEquals("crafted", text(successor.previous()));
		var successorPut = put(successor, 1, "after crafted");
		openGate();
		await(headPut);
		assertRetry(rejected);
		assertRetry(successorPut);

		var recovered = forUpdate(1);
		assertFalse(chained(recovered), "a head after the chain drained");
		assertEquals("v1", text(recovered.previous()));
		await(put(recovered, 1, "v2"));
		assertReplicasHold(1, "v2");
	}

	/** A write that gets between two updates of a chain: the second one fails, and the key drains its links. */
	private void aWriteStopsTheChain(Function<RocksDBAsyncAPI, CompletableFuture<?>> writer, String expected)
			throws Exception {
		closeGate(leader);
		var head = forUpdate(1);
		var headPut = put(head, 1, "v1");
		var chained = forUpdate(1);
		assertTrue(chained(chained));
		CompletableFuture<?> write = appended(() -> writer.apply(async(leader)));
		var chainedPut = put(chained, 1, "lost update");
		assertRetry(chainedPut);
		var next = forUpdateLater(1);
		assertStaysPending(next);
		openGate();
		await(headPut);
		await(write);
		var update = await(next);
		assertFalse(chained(update), "the next update is a head, which reads the database");
		// It may run before the write is applied: the two were concurrent, and its commit would conflict.
		assertTrue(java.util.Objects.equals(expected, text(update.previous())) || "v1".equals(text(update.previous())),
				text(update.previous()));
		await(async(leader).closeFailedUpdateAsync(update.updateId()));
		assertReplicasHold(1, expected);
	}

	@Test
	void aPlainWriteStopsAChain() throws Exception {
		start();
		aWriteStopsTheChain(api -> api.putAsync(0, column, key(1), text("plain"), RequestType.none()), "plain");
	}

	/**
	 * No pending write leaves a transaction's snapshot valid: this one conflicts. The update that holds the key goes on,
	 * but the chain ends there, so that the next try of the transaction finds a key with no unsettled links.
	 */
	@Test
	void aTransactionOnAKeyInAChainConflicts() throws Exception {
		start();
		long transaction = api(leader).openTransaction(Duration.ofSeconds(30));
		api(leader).put(transaction, column, key(1), text("transaction"), RequestType.none());
		closeGate(leader);
		var headPut = put(forUpdate(1), 1, "v1");
		var chained = forUpdate(1);
		assertFalse(await(async(leader).closeTransactionAsync(transaction, true)));
		assertEquals(1, count(leader, "transaction.chainConflict"));
		var chainedPut = put(chained, 1, "v2");
		var next = forUpdateLater(1);
		assertStaysPending(next);
		openGate();
		await(headPut);
		await(chainedPut);
		assertFalse(chained(await(next)));
		await(async(leader).closeFailedUpdateAsync(next.get().updateId()));
		assertTrue(api(leader).closeTransaction(transaction, false));
		assertReplicasHold(1, "v2");
	}

	@Test
	void aDeleteRangeStopsAChain() throws Exception {
		start();
		aWriteStopsTheChain(api -> api.deleteRangeAsync(column, key(0), key(10)), null);
	}

	/** A write already in the log before the update arrives: the update must see it, not the link before it. */
	@Test
	void anUpdateAfterAWriteNeverChainsOnWhatTheWriteHides() throws Exception {
		start();
		closeGate(leader);
		var headPut = put(forUpdate(1), 1, "v1");
		var plain = appended(() -> async(leader).putAsync(0, column, key(1), text("plain"), RequestType.none()));
		var next = forUpdateLater(1);
		assertStaysPending(next);
		openGate();
		await(headPut);
		await(plain);
		var update = await(next);
		assertFalse(chained(update));
		await(async(leader).closeFailedUpdateAsync(update.updateId()));
		assertReplicasHold(1, "plain");
	}

	@Test
	void aCloseInMidChainPassesTheKeyAtOnceAndWaitsForItsBase() throws Exception {
		start();
		closeGate(leader);
		var headPut = put(forUpdate(1), 1, "v1");
		var closing = forUpdate(1);
		assertEquals("v1", text(closing.previous()));
		var close = async(leader).closeFailedUpdateAsync(closing.updateId());
		var next = forUpdate(1);
		assertEquals("v1", text(next.previous()), "it wrote nothing, so the next update reads the same link");
		assertTrue(chained(next));
		var nextPut = put(next, 1, "v2");
		assertStaysPending(close);
		openGate();
		await(close);
		await(headPut);
		await(nextPut);
		assertReplicasHold(1, "v2");
	}

	@Test
	void aCloseOverADeadBaseIsARetry() throws Exception {
		start();
		closeGate(leader);
		var headPut = put(forUpdate(1), 1, "v1");
		var rejected = commitWithWrongGuard(forUpdate(1), "wrong", "crafted");
		var closing = forUpdate(1);
		assertEquals("crafted", text(closing.previous()));
		var close = async(leader).closeFailedUpdateAsync(closing.updateId());
		openGate();
		await(headPut);
		assertRetry(rejected);
		assertRetry(close);
		assertReplicasHold(1, "v1");
	}

	/** The close is bounded inside its future: a synchronous call has no timeout of its own. */
	@Test
	void aCloseStopsWaitingForItsBase() throws Exception {
		start();
		closeGate(leader);
		var headPut = put(forUpdate(1), 1, "v1");
		var closing = forUpdate(1);
		long start = System.nanoTime();
		assertThrows(RocksDBRetryException.class, () -> api(leader).closeFailedUpdate(closing.updateId()));
		assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5));
		// Timing out did not touch the outcome other waiters share.
		var another = forUpdate(1);
		var close = async(leader).closeFailedUpdateAsync(another.updateId());
		openGate();
		await(headPut);
		await(close);
	}

	@Test
	void aCloseOfAnUnknownChainedUpdateIsARetryAlsoOnANewLeader() throws Exception {
		start();
		assertThrows(RocksDBRetryException.class, () -> api(leader).closeFailedUpdate(123456789L));
		assertThrows(RocksDBRetryException.class, () -> api(leader).closeTransaction(123456789L, false));
		assertTrue(api(leader).closeTransaction(-123456789L, false), "a native rollback has nothing to confirm");
		closeGate(leader);
		var headPut = put(forUpdate(1), 1, "v1");
		var chained = forUpdate(1);
		assertTrue(chained(chained));
		openGate();
		await(headPut);
		cluster.stop(leader.config().nodeId());
		leader = cluster.awaitLeader(TIMEOUT);
		for (var api : List.of(api(leader), api(cluster.anyFollower()))) {
			assertThrows(RocksDBRetryException.class, () -> api.closeFailedUpdate(chained.updateId()));
			assertThrows(RocksDBRetryException.class, () -> api.closeTransaction(chained.updateId(), false));
		}
	}

	@Test
	void anOperationThatNeedsASnapshotIsRefusedOnAChainedUpdate() throws Exception {
		start();
		for (Consumer<UpdateContext<Buf>> operation : List.<Consumer<UpdateContext<Buf>>>of(
				update -> api(leader).get(update.updateId(), column, key(1), RequestType.current()),
				update -> api(leader).get(update.updateId(), column, key(2), RequestType.forUpdate()),
				update -> api(leader).closeTransaction(update.updateId(), true),
				update -> api(leader).closeTransaction(update.updateId(), false))) {
			closeGate(leader);
			var head = forUpdate(1);
			var headPut = put(head, 1, "same");
			var chained = forUpdate(1);
			assertTrue(chained(chained));
			assertThrows(RocksDBRetryException.class, () -> operation.accept(chained));
			var next = forUpdateLater(1);
			assertStaysPending(next);
			// The update ended, and its id with it: a repeated rollback must not confirm what it read.
			assertRetry(put(chained, 1, "too late"));
			assertThrows(RocksDBRetryException.class, () -> api(leader).closeTransaction(chained.updateId(), false));
			openGate();
			await(headPut);
			var update = await(next);
			assertFalse(chained(update), "the next update is a head");
			assertEquals("same", text(update.previous()));
			await(async(leader).closeFailedUpdateAsync(update.updateId()));
		}
	}

	@Test
	void aMergeByAChainedUpdateHoldsTheKeyUntilItIsApplied() throws Exception {
		start();
		closeGate(leader);
		var headPut = put(forUpdate(1), 1, "v1");
		var chained = forUpdate(1);
		var merge = appended(() -> async(leader).mergeAsync(chained.updateId(), column, key(1), text("m"),
				RequestType.none()));
		var next = forUpdateLater(1);
		assertStaysPending(next);
		openGate();
		await(headPut);
		await(merge);
		var update = await(next);
		assertEquals("v1,m", text(update.previous()));
		await(async(leader).closeFailedUpdateAsync(update.updateId()));
		assertReplicasHold(1, "v1,m");
	}

	@Test
	void aMultiKeyCommitByAChainedUpdateHoldsTheKeyUntilItIsApplied() throws Exception {
		start();
		closeGate(leader);
		var headPut = put(forUpdate(1), 1, "v1");
		var chained = forUpdate(1);
		var commit = appended(() -> async(leader).putMultiAsync(chained.updateId(), column, List.of(key(1), key(2)),
				List.of(text("a"), text("b")), RequestType.none()));
		var next = forUpdateLater(1);
		assertStaysPending(next);
		openGate();
		await(headPut);
		await(commit);
		var update = await(next);
		assertEquals("a", text(update.previous()));
		await(async(leader).closeFailedUpdateAsync(update.updateId()));
		assertReplicasHold(1, "a");
		assertReplicasHold(2, "b");
	}

	@Test
	void anAbandonedChainedUpdateGivesTheKeyAwayAtTheEndOfItsLease() throws Exception {
		start();
		closeGate(leader);
		var headPut = put(forUpdate(1), 1, "v1");
		forUpdate(1);
		var next = leader.connection().getAsyncApi(RequestContext.batch(Duration.ofSeconds(20)))
				.getAsync(0, column, key(1), RequestType.<Buf>forUpdate());
		assertStaysPending(next);
		openGate();
		await(headPut);
		var update = await(next);
		assertEquals("v1", text(update.previous()));
		await(async(leader).closeFailedUpdateAsync(update.updateId()));
	}

	@Test
	void aChainedUpdateWhoseCallerIsGoneDoesNotTakeTheKey() throws Exception {
		start();
		closeGate(leader);
		var headPut = put(forUpdate(1), 1, "v1");
		var holder = forUpdate(1);
		var timedOut = leader.connection().getAsyncApi(RequestContext.latency(Duration.ofMillis(300)))
				.getAsync(0, column, key(1), RequestType.<Buf>forUpdate());
		var cancelled = async(leader).getAsync(0, column, key(1), RequestType.<Buf>forUpdate());
		assertRetry(timedOut);
		cancelled.cancel(true);
		var closed = async(leader).closeFailedUpdateAsync(holder.updateId());
		// The key is free at once, well before a lease ends: neither gone caller opened an update that nobody can close.
		var next = await(soonAfterTheKeyIsFree());
		assertEquals("v1", text(next.previous()));
		var close = async(leader).closeFailedUpdateAsync(next.updateId());
		openGate();
		await(headPut);
		await(closed);
		await(close);
	}

	@Test
	void updatesCancelledWhileTheyOpenDoNotKeepTheKey() throws Exception {
		start();
		// Cancelled at varying points of the open: some land after the update registered, before its id is returned.
		for (int i = 0; i < 300; i++) {
			var update = forUpdateLater(1);
			LockSupport.parkNanos((i % 100) * 100_000L);
			if (!update.cancel(true)) {
				await(async(leader).closeFailedUpdateAsync(update.get(5, TimeUnit.SECONDS).updateId()));
			}
		}
		// The key is free well before the update lease ends: every cancelled update was closed.
		await(async(leader).closeFailedUpdateAsync(await(soonAfterTheKeyIsFree()).updateId()));
	}

	/** An update of key 1 that gives up long before an update nobody closes would let the key go. */
	private CompletableFuture<UpdateContext<Buf>> soonAfterTheKeyIsFree() {
		return soonAfterTheKeyIsFree(1);
	}

	private CompletableFuture<UpdateContext<Buf>> soonAfterTheKeyIsFree(long key) {
		return leader.connection().getAsyncApi(RequestContext.latency(Duration.ofSeconds(3)))
				.getAsync(0, column, key(key), RequestType.<Buf>forUpdate());
	}

	/** Bounded by the applied index, not by the chain: a snapshot may fall anywhere in one. */
	@Test
	void aSnapshotAndARestartInsideAChainKeepEveryVerdict() throws Exception {
		start(c -> c.withSnapshotEveryEntries(8));
		var follower = cluster.anyFollower();
		var id = follower.config().nodeId();
		closeGate(follower);
		var puts = new ArrayList<CompletableFuture<?>>();
		for (int i = 1; i <= 40; i++) {
			if (i == 20) {
				puts.add(aRejectedEntry());
				continue;
			}
			puts.add(put(forUpdate(1), 1, "v" + i));
			if (i % 3 == 0) {
				await(puts.getLast());
			}
		}
		for (var put : puts) {
			await(put);
		}
		haltAndAwaitLeader(id);
		cluster.start(id);
		leader = cluster.awaitLeader(TIMEOUT);
		assertReplicasHold(1, "v40");
		for (var node : List.copyOf(cluster.runningNodes())) {
			cluster.stop(node.config().nodeId());
		}
		cluster.startAll();
		leader = cluster.awaitLeader(TIMEOUT);
		assertReplicasHold(1, "v40");
	}

	/**
	 * Puts a rejected entry in the log, by stopping the leader's apply for a moment: the commit before it stays unsettled,
	 * so the update after that one is chained and its commit gets past the leader's own checks.
	 *
	 * @return the commit before the rejected entry, which writes v20
	 */
	private CompletableFuture<?> aRejectedEntry() throws Exception {
		var stopped = new CountDownLatch(1);
		leader.stateMachine().applyGateForTesting = stopped;
		var commit = put(forUpdate(1), 1, "v20");
		var chained = forUpdate(1);
		assertTrue(chained(chained));
		long rejections = count(leader, "update.guardRejected");
		var rejected = appended(() -> commitWithWrongGuard(chained, "wrong", "crafted"));
		stopped.countDown();
		leader.stateMachine().applyGateForTesting = null;
		assertRetry(rejected);
		assertEquals(rejections + 1, count(leader, "update.guardRejected"), "the log holds a rejection");
		await(async(leader).closeFailedUpdateAsync(chained.updateId()));
		return commit;
	}

	/** The leader dies with a chain in flight: what survives is a prefix, in order, and updates go on from there. */
	@Test
	void aLeaderChangeKeepsAPrefixOfTheChainAndTheNextUpdateIsAHead() throws Exception {
		start();
		closeGate(leader);
		int count = 20;
		for (int i = 1; i <= count; i++) {
			var update = forUpdate(1);
			assertEquals("v" + (i - 1), text(update.previous()));
			put(update, 1, "v" + i);
		}
		var oldLeader = leader.config().nodeId();
		haltAndAwaitLeader(oldLeader);
		var update = api(leader).get(0, column, key(1), RequestType.<Buf>forUpdate());
		assertFalse(chained(update), "a new session has no chains");
		var survived = text(update.previous());
		assertTrue(survived.matches("v([0-9]|1[0-9]|20)"), survived);
		api(leader).put(update.updateId(), column, key(1), text("after"), RequestType.none());
		cluster.start(oldLeader);
		assertReplicasHold(1, "after");
	}

	/**
	 * With the apply stopped, the second update of a key chains if the leader has the flag, and otherwise waits for the
	 * apply and is a head.
	 *
	 * @return how many values key 1 has gone through
	 */
	private long aChainOnlyWithTheFlag(boolean pipelined, long written) throws Exception {
		// A new leader's first entry must be applied, or no read barrier passes with the apply stopped.
		InProcessCluster.await(TIMEOUT, () -> leader.appliedIndex() >= lastLogIndex(leader));
		closeGate(leader);
		var head = forUpdate(1);
		assertFalse(chained(head));
		assertEquals("v" + written, text(head.previous()));
		var headPut = put(head, 1, "v" + (written + 1));
		var next = forUpdateLater(1);
		if (pipelined) {
			assertTrue(chained(await(next)), "chained with the flag on");
		} else {
			assertStaysPending(next);
		}
		openGate();
		await(headPut);
		var update = await(next);
		assertEquals(pipelined, chained(update));
		assertEquals("v" + (written + 1), text(update.previous()));
		await(put(update, 1, "v" + (written + 2)));
		assertReplicasHold(1, "v" + (written + 2));
		return written + 2;
	}

	/**
	 * The flag only changes how a leader behaves: entries a pipelined leader wrote, guards included, apply the same with
	 * the flag off, and the cluster goes on through a mix of the two.
	 */
	@Test
	void thePipelineCanBeSwitchedOnAndOffAtARestart() throws Exception {
		start();
		// A chain in the log, for what comes next to replay.
		closeGate(leader);
		var headPut = put(forUpdate(1), 1, "v1");
		var chainedPut = put(forUpdate(1), 1, "v2");
		var other = forUpdate(1);
		assertTrue(chained(other));
		var rejected = commitWithWrongGuard(other, "wrong", "crafted");
		openGate();
		await(headPut);
		await(chainedPut);
		assertRetry(rejected);
		await(async(leader).closeFailedUpdateAsync(other.updateId()));
		long written = 2;

		boolean pipelined = true;
		for (int phase = 0; phase < 3; phase++) {
			written = aChainOnlyWithTheFlag(pipelined, written);
			pipelined = !pipelined;
			for (var id : cluster.ids()) {
				cluster.stop(id);
				boolean flag = pipelined;
				cluster.reconfigure(id, c -> c.withPipelinedUpdates(flag));
			}
			cluster.startAll();
			leader = cluster.awaitLeader(TIMEOUT);
			assertReplicasHold(1, "v" + written);
		}

		// One leader with it on, two followers with it off.
		var on = cluster.ids().getFirst();
		leaderWithTheFlag(on);
		assertReplicasHold(1, "v" + written);
		written = aChainOnlyWithTheFlag(true, written);

		// A hand-over with a chain in flight: the new leader knows nothing of the chained id.
		closeGate(leader);
		put(forUpdate(1), 1, "lost or not");
		var chained = forUpdate(1);
		assertTrue(chained(chained));
		haltAndAwaitLeader(on);
		assertFalse(leader.config().pipelinedUpdates());
		assertThrows(RocksDBRetryException.class, () -> api(leader).closeFailedUpdate(chained.updateId()));
		assertThrows(RocksDBRetryException.class, () -> api(leader).closeTransaction(chained.updateId(), false));
		var update = forUpdate(1);
		assertFalse(chained(update));
		await(put(update, 1, "after"));
		cluster.start(on);
		assertReplicasHold(1, "after");
	}

	/** Turns the flag on on the node {@code on} alone, and makes it the leader. */
	private void leaderWithTheFlag(String on) throws Exception {
		cluster.stop(on);
		cluster.reconfigure(on, c -> c.withPipelinedUpdates(true));
		cluster.start(on);
		leader = cluster.awaitLeader(TIMEOUT);
		while (!leader.config().nodeId().equals(on)) {
			var id = leader.config().nodeId();
			cluster.stop(id);
			leader = cluster.awaitLeader(TIMEOUT);
			cluster.start(id);
			leader = cluster.awaitLeader(TIMEOUT);
		}
	}

	/** A second write on an update, such as a resend: it fails alone, and the commit and its chain go on. */
	@Test
	void aSecondWriteOnAnUpdateLeavesItsCommitAlone() throws Exception {
		start();
		closeGate(leader);
		var head = forUpdate(1);
		var headPut = put(head, 1, "v1");
		var chained = forUpdate(1);
		assertTrue(chained(chained));
		assertRetry(put(head, 1, "resent"));
		var chainedPut = put(chained, 1, "v2");
		var closing = forUpdate(1);
		assertTrue(chained(closing), "the chain is not broken");
		assertEquals("v2", text(closing.previous()));
		var close = async(leader).closeFailedUpdateAsync(closing.updateId());
		openGate();
		await(headPut);
		await(chainedPut);
		await(close);
		assertReplicasHold(1, "v2");
	}

	/**
	 * With the apply stopped, more closes wait for their base at once than a server's CONTROL workers (256) and queue
	 * (1024) hold. Nothing refuses them, not even the other requests of that scheduler, and no key stays held. The closes
	 * wait longer here than a real one does: on a busy machine, the first ones would give up before the last ones start.
	 */
	@Test
	void closesThatWaitForTheirBaseHoldNoWorkerOfTheServer() throws Exception {
		start();
		connection().chainedCloseWaitForTesting = TIMEOUT.multipliedBy(2);
		var self = leader.config().self();
		try (var client = GrpcConnection.forHostAndPort("closes", new HostAndPort(self.apiHost(), self.apiPort()))) {
			var grpc = client.getAsyncApi(CONTEXT);
			closeGate(leader);
			int keys = 32;
			var closes = new ConcurrentLinkedQueue<CompletableFuture<Void>>();
			try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
				var workers = new ArrayList<Future<?>>();
				for (int i = 0; i < keys; i++) {
					long key = 100 + i;
					workers.add(executor.submit(() -> {
						put(forUpdate(key), key, "v");
						for (int reader = 0; reader < LeaderSession.MAX_LINK_READERS; reader++) {
							var update = forUpdate(key);
							assertTrue(chained(update));
							closes.add(grpc.closeFailedUpdateAsync(update.updateId()));
						}
						return null;
					}));
				}
				for (var worker : workers) {
					worker.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
				}
			}
			assertTrue(closes.size() > 1280);
			assertTrue(closes.stream().noneMatch(CompletableFuture::isDone), "every close waits for its base");
			long transaction = api(leader).openTransaction(Duration.ofSeconds(30));
			assertTrue(await(grpc.closeTransactionAsync(transaction, false)), "a rollback still runs");
			assertTrue(closes.stream().noneMatch(CompletableFuture::isDone), "every close waits for its base");
			openGate();
			for (var close : closes) {
				await(close);
			}
			for (int i = 0; i < keys; i++) {
				await(async(leader).closeFailedUpdateAsync(await(soonAfterTheKeyIsFree(100 + i)).updateId()));
			}
		}
	}

	private ReplicatedConnection connection() {
		return (ReplicatedConnection) leader.connection();
	}

	/**
	 * A commit that may be applying, and whose caller's deadline ended it: its outcome is unknown, which must count as a
	 * failure at once, not when the apply comes back. It is admitted, so an update chains on it first.
	 */
	@Test
	void aCommitThatEndsWithoutAnOutcomeFailsItsLink() throws Exception {
		start();
		connection().chainedCloseWaitForTesting = TIMEOUT.multipliedBy(2);
		closeGate(leader);
		var head = forUpdate(1);
		var shortCaller = leader.connection().getAsyncApi(RequestContext.latency(Duration.ofMillis(1500)));
		var headPut = shortCaller.putAsync(head.updateId(), column, key(1), text("v1"), RequestType.none());
		var chained = forUpdate(1);
		assertTrue(chained(chained));
		assertEquals("v1", text(chained.previous()));
		var error = assertThrows(ExecutionException.class, () -> headPut.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
		assertFalse(error.getCause() instanceof RuntimeException retry && HotCounters.isRetry(retry),
				"it was sent, so this is no clean refusal: " + error.getCause());
		assertRetry(async(leader).closeFailedUpdateAsync(chained.updateId()));
		// With no link left, the next update is a head while the commit is still in the log, and may read what came before
		// it. Its own commit conflicts with that one, so no update is lost.
		var update = await(forUpdateLater(1));
		assertFalse(chained(update));
		assertEquals("v0", text(update.previous()));
		assertRetry(put(update, 1, "v0 again"));
		openGate();
		var after = forUpdate(1);
		assertFalse(chained(after));
		assertEquals("v1", text(after.previous()), "the commit did apply");
		await(async(leader).closeFailedUpdateAsync(after.updateId()));
		assertReplicasHold(1, "v1");
	}

	/** The data of the log entries after {@code index} that hold a commit. */
	private List<byte[]> commitsAfter(long index) throws Exception {
		var log = leader.stateMachine().division().getRaftLog();
		var commits = new ArrayList<byte[]>();
		for (long i = index + 1; i <= lastLogIndex(leader); i++) {
			var entry = log.get(i);
			if (entry.hasStateMachineLogEntry()) {
				var data = entry.getStateMachineLogEntry().getLogData().toByteArray();
				if (LogEntryCodec.decodeBatch(data).stream().anyMatch(r -> r.entry() instanceof LogEntry.Guarded)) {
					commits.add(data);
				}
			}
		}
		return commits;
	}

	@Test
	void theFlagIsOffByDefault() {
		var peers = List.of(new PeerConfig("n", "127.0.0.1", 1, "127.0.0.1", 2));
		assertFalse(NodeConfig.defaults("n", peers, temp).pipelinedUpdates());
	}

	/**
	 * What a leader with the flag off writes for an update, read back from its Raft log: the bytes of a log from before
	 * pipelined updates, which is why they need no new log version and a flag-off rolling upgrade works.
	 */
	@Test
	void anUpdateOfALeaderWithTheFlagOffWritesTheOldBytes() throws Exception {
		start(c -> c.withPipelinedUpdates(false));
		long before = lastLogIndex(leader);
		var update = forUpdate(1);
		await(put(update, 1, "v1"));
		var commits = commitsAfter(before);
		assertEquals(1, commits.size());
		var data = commits.getFirst();
		var requests = LogEntryCodec.decodeBatch(data);
		assertEquals(1, requests.size());
		var commit = assertInstanceOf(LogEntry.Guarded.class, requests.getFirst().entry());
		assertEquals(0, commit.updateId());
		assertNull(commit.guard());
		assertEquals(1, data[data.length - 1], "the entry ends with the boolean it always ended with");
		byte[] body = LogEntryCodec.encodeEntryBody(commit);
		assertArrayEquals(body, java.util.Arrays.copyOfRange(data, data.length - body.length, data.length));
		assertEquals(LogEntryCodec.GUARDED_BASE_BYTES + LogEntryCodec.writeSize(commit.writes().getFirst())
				+ commit.conflictHashes().length * LogEntryCodec.CONFLICT_KEY_BYTES, body.length);
	}

	/** Without the flag an update has no single commit to protect: its writes go to the log as they always did. */
	@Test
	void aSecondWriteOnAnUpdateWithTheFlagOffGoesToTheLog() throws Exception {
		start(c -> c.withPipelinedUpdates(false));
		closeGate(leader);
		var update = forUpdate(1);
		var first = appended(() -> put(update, 2, "a"));
		var second = put(update, 3, "b");
		openGate();
		await(first);
		await(second);
		assertReplicasHold(2, "a");
		assertReplicasHold(3, "b");
	}

	/** The write conflicts with the first one in the log, and the conflict ends the update. */
	@Test
	void aConflictingSecondWriteEndsAnUpdateWithTheFlagOff() throws Exception {
		start(c -> c.withPipelinedUpdates(false));
		closeGate(leader);
		var update = forUpdate(1);
		var first = appended(() -> put(update, 1, "v1"));
		assertRetry(put(update, 1, "resent"));
		var session = leader.stateMachine().leaderSession(leader.stateMachine().division().getInfo().getCurrentTerm());
		assertNull(session.get(update.updateId()));
		assertEquals(0, session.lockedKeys());
		openGate();
		await(first);
		assertReplicasHold(1, "v1");
	}

	/** Many clients, one key, retrying as real clients do: nothing is lost, and the replicas agree. */
	@Test
	void concurrentClientsLoseNoUpdate() throws Exception {
		start();
		api(leader).put(0, column, key(7), text("0"), RequestType.none());
		int clients = 16;
		int perClient = 40;
		var retries = new AtomicInteger();
		var apis = new ArrayList<RocksDBSyncAPI>();
		for (int i = 0; i < clients; i++) {
			apis.add(api(cluster.runningNodes().get(i % cluster.runningNodes().size())));
		}
		try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
			var workers = new ArrayList<Future<?>>();
			for (int i = 0; i < clients; i++) {
				var api = apis.get(i);
				workers.add(executor.submit(() -> {
					for (int done = 0; done < perClient; ) {
						try {
							var update = api.get(0, column, key(7), RequestType.<Buf>forUpdate());
							long value = Long.parseLong(text(update.previous()));
							api.put(update.updateId(), column, key(7), text(String.valueOf(value + 1)), RequestType.none());
							done++;
						} catch (RocksDBRetryException retry) {
							retries.incrementAndGet();
						}
					}
				}));
			}
			for (var worker : workers) {
				worker.get(120, TimeUnit.SECONDS);
			}
		}
		assertReplicasHold(7, String.valueOf(clients * perClient));
	}
}
