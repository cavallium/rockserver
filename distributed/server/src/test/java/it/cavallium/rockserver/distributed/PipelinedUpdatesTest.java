package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.UpdateContext;
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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
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
		var api = api(cluster.anyFollower());
		for (int i = 1; i <= 20; i++) {
			var update = api.get(0, column, key(1), RequestType.<Buf>forUpdate());
			assertEquals("v" + (i - 1), text(update.previous()));
			api.put(update.updateId(), column, key(1), text("v" + i), RequestType.none());
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

	/** No pending write leaves a transaction's snapshot valid: this one conflicts, and the chain goes on. */
	@Test
	void aTransactionOnAKeyInAChainConflicts() throws Exception {
		start();
		long transaction = api(leader).openTransaction(Duration.ofSeconds(30));
		api(leader).put(transaction, column, key(1), text("transaction"), RequestType.none());
		closeGate(leader);
		var headPut = put(forUpdate(1), 1, "v1");
		var chained = forUpdate(1);
		assertFalse(await(async(leader).closeTransactionAsync(transaction, true)));
		var chainedPut = put(chained, 1, "v2");
		openGate();
		await(headPut);
		await(chainedPut);
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
		closeGate(leader);
		var headPut = put(forUpdate(1), 1, "v1");
		var chained = forUpdate(1);
		assertTrue(chained(chained));
		openGate();
		await(headPut);
		cluster.stop(leader.config().nodeId());
		leader = cluster.awaitLeader(TIMEOUT);
		assertThrows(RocksDBRetryException.class, () -> api(leader).closeFailedUpdate(chained.updateId()));
		assertThrows(RocksDBRetryException.class, () -> api(cluster.anyFollower()).closeFailedUpdate(chained.updateId()));
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
			// The update ended, and its id with it.
			assertRetry(put(chained, 1, "too late"));
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
		var next = forUpdate(1);
		assertEquals("v1", text(next.previous()), "neither gone caller opened an update that nobody can close");
		var close = async(leader).closeFailedUpdateAsync(next.updateId());
		openGate();
		await(headPut);
		await(closed);
		await(close);
	}

	@Test
	void updatesCancelledWhileTheyOpenDoNotKeepTheKey() throws Exception {
		start();
		for (int i = 0; i < 200; i++) {
			var update = forUpdateLater(1);
			if (!update.cancel(true)) {
				await(async(leader).closeFailedUpdateAsync(update.get(5, TimeUnit.SECONDS).updateId()));
			}
		}
		var next = forUpdate(1);
		await(async(leader).closeFailedUpdateAsync(next.updateId()));
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
			var update = forUpdate(1);
			if (i == 20) {
				var rejected = commitWithWrongGuard(update, "wrong", "crafted");
				assertRetry(rejected);
				update = forUpdate(1);
			}
			puts.add(put(update, 1, "v" + i));
			if (i % 3 == 0) {
				await(puts.getLast());
			}
		}
		for (var put : puts) {
			await(put);
		}
		var halt = Thread.ofPlatform().start(() -> {
			try {
				cluster.halt(id);
			} catch (IOException ex) {
				throw new UncheckedIOException(ex);
			}
		});
		openGate();
		halt.join();
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
		var halt = Thread.ofPlatform().start(() -> {
			try {
				cluster.halt(oldLeader);
			} catch (IOException ex) {
				throw new UncheckedIOException(ex);
			}
		});
		openGate();
		halt.join();
		leader = cluster.awaitLeader(TIMEOUT);
		var update = api(leader).get(0, column, key(1), RequestType.<Buf>forUpdate());
		assertFalse(chained(update), "a new session has no chains");
		var survived = text(update.previous());
		assertTrue(survived.matches("v([0-9]|1[0-9]|20)"), survived);
		api(leader).put(update.updateId(), column, key(1), text("after"), RequestType.none());
		cluster.start(oldLeader);
		assertReplicasHold(1, "after");
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
