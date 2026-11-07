package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ClientRequestId;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
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
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The error paths of {@link UpdateProtocol} on a one-node cluster: what a staged write, a commit, a rollback and the
 * write that ends an update answer when their transaction or update is not (or no longer) on the leader, a staged write
 * that carries a client request id, and a commit with nothing to write.
 */
class UpdateProtocolPathsTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));
	/** A client session that was never opened. */
	private static final long UNKNOWN_SESSION = 987_654_321L;

	@TempDir
	Path temp;
	private InProcessCluster cluster;
	private ReplicatedNode node;
	private RocksDBSyncAPI api;
	private long column;

	@BeforeEach
	void setUp() throws Exception {
		cluster = new InProcessCluster(temp, 1, c -> c).startAll();
		node = cluster.awaitLeader(Duration.ofSeconds(30));
		api = node.connection().getSyncApi(CONTEXT);
		column = api.createColumn("c", SCHEMA);
	}

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	private RocksDBSyncAPI numbered(long session, long sequence) {
		return node.connection().getSyncApi(CONTEXT.withRequestId(new ClientRequestId(session, sequence, 0)));
	}

	private ReplicatedConnection connection() {
		return (ReplicatedConnection) node.connection();
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private long lastLogIndex() {
		return node.stateMachine().division().getRaftLog().getLastEntryTermIndex().getIndex();
	}

	@Test
	void aWriteStagedInATransactionCannotCarryARequestId() {
		long tx = api.openTransaction(Duration.ofSeconds(30));
		var error = assertThrows(RocksDBException.class,
				() -> numbered(UNKNOWN_SESSION, 1).put(tx, column, key(1), text("x"), RequestType.none()));
		assertEquals(RocksDBErrorType.INVALID_REQUEST, error.getErrorUniqueId(), error.getMessage());
		// The transaction is still usable without the refused write.
		api.put(tx, column, key(2), text("y"), RequestType.none());
		assertTrue(api.closeTransaction(tx, true));
		assertNull(api.get(0, column, key(1), RequestType.current()));
	}

	@Test
	void aTransactionThatIsNotOnTheLeaderMustBeRunAgainButItsRollbackSucceeds() {
		long unknown = -424242L;
		assertThrows(RocksDBRetryException.class, () -> api.closeTransaction(unknown, true));
		assertTrue(api.closeTransaction(unknown, false), "rolling back a transaction that is gone is not an error");
		assertThrows(RocksDBRetryException.class,
				() -> api.put(unknown, column, key(1), text("x"), RequestType.none()));
		// A positive id is a chained update: even its clean end would confirm a value that may never be written.
		assertThrows(RocksDBRetryException.class, () -> api.closeTransaction(424242L, false));
	}

	/** A copy of a numbered commit whose transaction is gone, of a session that does not exist: no record can say. */
	@Test
	void aCopyOfANumberedCommitOfAnUnknownSessionAnswersSessionExpired() {
		var error = assertThrows(RocksDBException.class,
				() -> numbered(UNKNOWN_SESSION, 3).closeTransaction(-55L, true));
		assertEquals(RocksDBErrorType.SESSION_EXPIRED, error.getErrorUniqueId(), error.getMessage());
	}

	/** The same for a copy of the write that ends an update. */
	@Test
	void aCopyOfTheWriteEndingAGoneUpdateOfAnUnknownSessionAnswersSessionExpired() {
		var error = assertThrows(RocksDBException.class,
				() -> numbered(UNKNOWN_SESSION, 4).put(-77L, column, key(1), text("x"), RequestType.none()));
		assertEquals(RocksDBErrorType.SESSION_EXPIRED, error.getErrorUniqueId(), error.getMessage());
	}

	/**
	 * R25-O, through the router and the update protocol: a copy of a numbered commit whose transaction is gone, on a
	 * leader whose barrier then fails because it lost leadership. An earlier copy may be in its log and commit under the
	 * next leader, so the answer is "unknown", never {@code UPDATE_RETRY} (the client would run the transaction again).
	 */
	@Test
	void aCopyOfANumberedCommitWhoseBarrierLosesLeadershipAnswersUnknown() {
		long session = connection().openClientSessionAsync(CONTEXT).join();
		connection().outcomes.leadershipForTesting(
				() -> CompletableFuture.failedFuture(new ReadBarrier.NotLeaderException("deposed")));
		var error = assertThrows(RocksDBException.class, () -> numbered(session, 1).closeTransaction(-55L, true));
		assertUnknown(error);
	}

	/** The same for a copy of the write that ends an update. */
	@Test
	void aCopyOfTheWriteEndingAGoneUpdateWhoseBarrierLosesLeadershipAnswersUnknown() {
		long session = connection().openClientSessionAsync(CONTEXT).join();
		connection().outcomes.leadershipForTesting(
				() -> CompletableFuture.failedFuture(new ReadBarrier.NotLeaderException("deposed")));
		var error = assertThrows(RocksDBException.class,
				() -> numbered(session, 2).put(-77L, column, key(1), text("x"), RequestType.none()));
		assertUnknown(error);
	}

	private static void assertUnknown(RocksDBException error) {
		assertFalse(error instanceof RocksDBRetryException, () -> "\"not applied\": " + error);
		assertFalse(error.isNotApplied(), () -> "flagged not applied: " + error);
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, error.getErrorUniqueId(), error.getMessage());
	}

	@Test
	void aCommitWithNothingToWriteSucceedsWithoutALogEntry() {
		long tx = api.openTransaction(Duration.ofSeconds(30));
		long before = lastLogIndex();
		assertTrue(api.closeTransaction(tx, true));
		assertEquals(before, lastLogIndex(), "an empty commit went to the log");
	}

	@Test
	void anUpdateEndsWithItsWriteAndASecondEndIsRefused() {
		api.put(0, column, key(9), text("a"), RequestType.none());
		var update = api.get(0, column, key(9), RequestType.<Buf>forUpdate());
		assertEquals("a", update.previous().toString(StandardCharsets.UTF_8));
		api.put(update.updateId(), column, key(9), text("b"), RequestType.none());
		assertEquals("b", api.get(0, column, key(9), RequestType.current()).toString(StandardCharsets.UTF_8));
		// The update is over: a second write on its id must not be committed.
		assertInstanceOf(RocksDBRetryException.class, assertThrows(RocksDBException.class,
				() -> api.put(update.updateId(), column, key(9), text("c"), RequestType.none())));
		assertEquals("b", api.get(0, column, key(9), RequestType.current()).toString(StandardCharsets.UTF_8));
	}

	@Test
	void aFailedUpdateIsClosedAndReleasesItsKey() {
		var update = api.get(0, column, key(10), RequestType.<Buf>forUpdate());
		api.closeFailedUpdate(update.updateId());
		// The key is free at once: the next update gets it without waiting for a lease.
		long start = System.nanoTime();
		var next = api.get(0, column, key(10), RequestType.<Buf>forUpdate());
		assertTrue(System.nanoTime() - start < Duration.ofSeconds(2).toNanos(), "the key stayed locked");
		api.put(next.updateId(), column, key(10), text("v"), RequestType.none());
	}
}
