package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * Ratis calls {@code notifyLeaderChanged} and {@code notifyServerShutdown} holding its server's monitor (R23-B). The
 * rollbacks of the ended term's native transactions take each transaction's monitor, which a read or a write in that
 * transaction holds for a whole native call, for as long as a stalled disk makes it last. A test thread stands in
 * for such a call by holding the monitor of one open transaction.
 */
@Timeout(120)
class StepDownRollbackOffRaftTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	/** Well above an election (400-800 ms) and a hand-off; without the fix the node waits until the monitor is freed. */
	private static final Duration STEP_DOWN_BOUND = Duration.ofSeconds(10);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));

	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path temp;
	private InProcessCluster cluster;
	private final CountDownLatch release = new CountDownLatch(1);

	@AfterEach
	void tearDown() throws Exception {
		release.countDown();
		if (cluster != null) {
			cluster.close();
		}
	}

	/**
	 * The old leader becomes a follower of the new one and applies its entries while the monitor is held; an update
	 * waiting for a key of the ended term is answered meanwhile; the transaction is rolled back once the monitor is
	 * free, and its id is unknown to the new leader.
	 */
	@Test
	void aStuckTransactionDoesNotHoldTheStepDown() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var oldLeader = cluster.awaitLeader(TIMEOUT);
		var api = api(oldLeader);
		long column = api.createColumn("c", SCHEMA);
		long tx = api.openTransaction(Duration.ofSeconds(60));
		api.put(tx, column, key(1), text("tx"), RequestType.none());
		var holder = api.get(0, column, key(2), RequestType.forUpdate());
		var waiter = oldLeader.connection().getAsyncApi(CONTEXT)
				.getAsync(0, column, key(2), RequestType.forUpdate());
		Thread.sleep(200);
		assertFalse(waiter.isDone(), "the second update did not wait for the key");

		var db = oldLeader.stateMachine().store().db();
		var monitor = nativeTransaction(db, tx);
		assertNotNull(monitor, "the transaction is not open on the leader");
		holdMonitor(monitor);

		var target = cluster.runningNodes().stream().filter(node -> node != oldLeader).findFirst().orElseThrow();
		String targetId = target.config().nodeId();
		oldLeader.raftServerForTests().transferLeadershipAsync(transferRequest(oldLeader, targetId));

		within(STEP_DOWN_BOUND, "the old leader did not follow the new one", () -> target.isLeader()
				&& oldLeader.stateMachine().division().getInfo().isFollower() && targetId.equals(oldLeader.leaderId()));
		api(target).put(0, column, key(3), text("after"), RequestType.none());
		within(STEP_DOWN_BOUND, "the old leader did not apply the new leader's entry",
				() -> "after".equals(localValue(oldLeader, column, 3)));
		within(STEP_DOWN_BOUND, "the update waiting for the key was not answered", waiter::isDone);
		assertNotNull(nativeTransaction(db, tx), "rolled back while its monitor was held");
		assertThrows(RocksDBRetryException.class, () -> api(target).closeTransaction(tx, true));

		release.countDown();
		within(TIMEOUT, "the transaction of the ended term was not rolled back", () -> nativeTransaction(db, tx) == null);
		within(TIMEOUT, "the update of the ended term was not rolled back",
				() -> nativeTransaction(db, holder.updateId()) == null);
		assertNull(localValue(target, column, 1));
	}

	/**
	 * A node that stops while a rollback of its ended term is stuck closes its database only once the rollback can go
	 * on. A regression check: the database's own close takes the same monitor, so it cannot fail without the wait.
	 */
	@Test
	void aStopWaitsForTheStuckRollbackThenCloses() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var api = api(leader);
		long column = api.createColumn("c", SCHEMA);
		long tx = api.openTransaction(Duration.ofSeconds(60));
		api.put(tx, column, key(1), text("tx"), RequestType.none());
		var db = leader.stateMachine().store().db();
		holdMonitor(nativeTransaction(db, tx));

		var stateMachine = leader.stateMachine();
		var halted = CompletableFuture.runAsync(() -> {
			try {
				cluster.halt(leader.config().nodeId());
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}, command -> Thread.ofPlatform().name("halt").start(command));
		Thread.sleep(1_000);
		assertFalse(stateMachine.awaitClosed(Duration.ofMillis(100)), "the database closed under a stuck rollback");

		release.countDown();
		halted.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		assertTrue(stateMachine.awaitClosed(TIMEOUT));
		assertNull(nativeTransaction(db, tx));
	}

	/**
	 * A step-down whose drain thread cannot start (an {@link OutOfMemoryError} "unable to create native thread" on a
	 * host short of memory) throws nothing into Ratis: the old leader follows the new one and applies its entries, the
	 * update waiting for a key of the ended term is answered, the ended term's transactions are rolled back, and a
	 * stop that waits for the session's drain returns.
	 */
	@Test
	void aDrainThreadThatCannotStartDoesNotBreakTheStepDown() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var oldLeader = cluster.awaitLeader(TIMEOUT);
		var api = api(oldLeader);
		long column = api.createColumn("c", SCHEMA);
		long tx = api.openTransaction(Duration.ofSeconds(60));
		api.put(tx, column, key(1), text("tx"), RequestType.none());
		var holder = api.get(0, column, key(2), RequestType.forUpdate());
		var waiter = oldLeader.connection().getAsyncApi(CONTEXT)
				.getAsync(0, column, key(2), RequestType.forUpdate());
		Thread.sleep(200);
		assertFalse(waiter.isDone(), "the second update did not wait for the key");
		var db = oldLeader.stateMachine().store().db();
		assertNotNull(nativeTransaction(db, tx), "the transaction is not open on the leader");
		oldLeader.stateMachine().sessionCloseStarter = (name, task) -> {
			throw new OutOfMemoryError("unable to create native thread: possibly out of memory (test)");
		};

		var target = cluster.runningNodes().stream().filter(node -> node != oldLeader).findFirst().orElseThrow();
		String targetId = target.config().nodeId();
		oldLeader.raftServerForTests().transferLeadershipAsync(transferRequest(oldLeader, targetId));

		within(STEP_DOWN_BOUND, "the old leader did not follow the new one", () -> target.isLeader()
				&& oldLeader.stateMachine().division().getInfo().isFollower() && targetId.equals(oldLeader.leaderId()));
		api(target).put(0, column, key(3), text("after"), RequestType.none());
		within(STEP_DOWN_BOUND, "the old leader did not apply the new leader's entry",
				() -> "after".equals(localValue(oldLeader, column, 3)));
		within(STEP_DOWN_BOUND, "the update waiting for the key was not answered", waiter::isDone);
		within(TIMEOUT, "the transaction of the ended term was not rolled back", () -> nativeTransaction(db, tx) == null);
		within(TIMEOUT, "the update of the ended term was not rolled back",
				() -> nativeTransaction(db, holder.updateId()) == null);

		var stateMachine = oldLeader.stateMachine();
		var halted = CompletableFuture.runAsync(() -> {
			try {
				cluster.halt(oldLeader.config().nodeId());
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}, command -> Thread.ofPlatform().daemon().name("halt").start(command));
		halted.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		assertTrue(stateMachine.awaitClosed(TIMEOUT), "the stop did not close the database");
	}

	private void holdMonitor(Object monitor) throws InterruptedException {
		var held = new CountDownLatch(1);
		Thread.ofPlatform().daemon().name("stuck-native-call").start(() -> {
			synchronized (monitor) {
				held.countDown();
				try {
					release.await();
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
		});
		assertTrue(held.await(10, TimeUnit.SECONDS));
	}

	/** The native transaction {@code id} of {@code db}, whose monitor its reads, writes and rollback take, or null. */
	private static Object nativeTransaction(EmbeddedDB db, long id) {
		try {
			Field field = EmbeddedDB.class.getDeclaredField("txs");
			field.setAccessible(true);
			Object map = field.get(db);
			return map.getClass().getMethod("get", long.class).invoke(map, id);
		} catch (ReflectiveOperationException ex) {
			throw new AssertionError(ex);
		}
	}

	private static void within(Duration bound, String message, BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + bound.toNanos();
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() - deadline > 0) {
				fail(message + " within " + bound);
			}
			Thread.sleep(20);
		}
	}

	private static TransferLeadershipRequest transferRequest(ReplicatedNode from, String to) {
		var groupId = RaftGroupId.valueOf(UUID.fromString(from.config().groupId()));
		return new TransferLeadershipRequest(ClientId.randomId(), from.raftServerForTests().getId(), groupId, 0,
				RaftPeerId.valueOf(to), TIMEOUT.toMillis());
	}

	private static RocksDBSyncAPI api(ReplicatedNode node) {
		return node.connection().getSyncApi(CONTEXT);
	}

	private static String localValue(ReplicatedNode node, long columnId, long key) {
		var value = node.stateMachine().store().db().get(0, columnId, key(key), RequestType.current());
		return value == null ? null : new String(value.toByteArray(), StandardCharsets.UTF_8);
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}
}
