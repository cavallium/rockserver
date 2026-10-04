package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.rocksdb.TransactionalDB;
import it.cavallium.rockserver.core.impl.rocksdb.Tx;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.util.List;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.rocksdb.Transaction;

@Timeout(10)
class TransactionCloseConcurrencyTest {

	@TempDir
	Path tempDir;

	@Test
	void duplicateCommitsSerializeAndBalanceTheTransactionOnce() throws Exception {
		try (var db = new EmbeddedConnection(tempDir.resolve("duplicate-close"), "duplicate-close", null)) {
			EmbeddedDB internal = db.getInternalDB();
			long transactionId = db.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).openTransaction( java.time.Duration.ofMillis(10_000));
			Object transactionMonitor = getTransactionMonitor(internal, transactionId);
			assertEquals(1, internal.getOpenTransactionsCount());
			assertEquals(1, internal.getPendingOpsCount());

			var firstThread = new AtomicReference<Thread>();
			var secondThread = new AtomicReference<Thread>();
			var ready = new CountDownLatch(2);
			var start = new CountDownLatch(1);
			try (var executor = Executors.newFixedThreadPool(2)) {
				Future<CloseResult> first;
				Future<CloseResult> second;
				synchronized (transactionMonitor) {
					first = executor.submit(() -> closeTransaction(
							db, transactionId, firstThread, ready, start));
					second = executor.submit(() -> closeTransaction(
							db, transactionId, secondThread, ready, start));
					assertTrue(ready.await(1, TimeUnit.SECONDS));
					start.countDown();
					awaitBlockedOnTransaction(firstThread);
					awaitBlockedOnTransaction(secondThread);
				}

				assertOneSuccessfulCommit(first.get(1, TimeUnit.SECONDS), second.get(1, TimeUnit.SECONDS));
			}

			assertEquals(0, internal.getOpenTransactionsCount());
			assertEquals(0, internal.getPendingOpsCount());
		}
	}

	@ParameterizedTest
	@MethodSource("operationAndCleaner")
	void transactionUseExcludesRollbackAndExpiryCleaner(Operation operation, boolean cleaner, boolean owned) throws Exception {
		try (var connection = ownedUpdateConnection()) {
			var db = connection.getInternalDB();
			long column = createColumn(connection);
			long id = openTransaction(connection, column, owned);
			var probe = replaceNativeTransaction(db, id, cleaner ? 0L : Long.MAX_VALUE);
			probe.blockMethod = operation.nativeMethod(owned);
			var closerThread = new AtomicReference<Thread>();
			try (var executor = Executors.newFixedThreadPool(2)) {
				var operationFuture = executor.submit(() -> operation.run(db, id, column));
				try {
					assertTrue(probe.entered.await(2, TimeUnit.SECONDS));
					assertTrue(probe.original.isOwningHandle());
					var closed = executor.submit(() -> {
						closerThread.set(Thread.currentThread());
						if (cleaner) cleanupExpiredTransactions(db);
						else db.closeFailedUpdate(id);
						return null;
					});
					awaitBlockedOnTransaction(closerThread);
					assertFalse(closed.isDone());
					assertEquals(0, probe.closes.get());
					long otherId = db.get(0, column, key(2), RequestType.forUpdate(), WorkloadProfile.INGEST).updateId();
					db.put(otherId, column, key(2), bytes("independent"), RequestType.none());
					assertEquals(bytes("independent"), db.get(0, column, key(2), RequestType.current()));
					probe.release.countDown();
					operationFuture.get(2, TimeUnit.SECONDS);
					closed.get(2, TimeUnit.SECONDS);
				} finally {
					probe.release.countDown();
				}
			}
			assertEquals(1, probe.closes.get());
			assertEquals(0, probe.closedNativeCalls.get());
			assertEquals(0, db.getOpenTransactionsCount());
			assertEquals(0, db.getPendingOpsCount());
			db.closeFailedUpdate(id);
			assertEquals(1, probe.closes.get());
			assertEquals(owned ? operation.expectedValue() : bytes("a"), db.get(0, column, key(1), RequestType.current()));
		}
	}

	@ParameterizedTest
	@MethodSource("operationAndOwnership")
	void removedTransactionIsRejectedBeforeNativeUse(Operation operation, boolean owned) throws Exception {
		try (var connection = ownedUpdateConnection()) {
			var db = connection.getInternalDB();
			long column = createColumn(connection);
			long id = openTransaction(connection, column, owned);
			var probe = replaceNativeTransaction(db, id, Long.MAX_VALUE);
			var monitor = transactionMap(db).get(id);
			var callerThread = new AtomicReference<Thread>();
			try (var executor = Executors.newSingleThreadExecutor()) {
				Future<Throwable> request;
				synchronized (monitor) {
					request = executor.submit(() -> {
						callerThread.set(Thread.currentThread());
						try {
							operation.run(db, id, column);
							return null;
						} catch (Throwable failure) {
							return failure;
						}
					});
					awaitBlockedOnTransaction(callerThread);
					db.closeFailedUpdate(id);
				}
				var failure = assertInstanceOf(RocksDBException.class, request.get(2, TimeUnit.SECONDS));
				assertEquals(RocksDBErrorType.TRANSACTION_NOT_FOUND, failure.getErrorUniqueId());
			}
			assertEquals(0, probe.closedNativeCalls.get());
			assertEquals(1, probe.closes.get());
			assertEquals(0, db.getOpenTransactionsCount());
			assertEquals(0, db.getPendingOpsCount());
		}
	}

	@ParameterizedTest
	@MethodSource("mutationAndFailure")
	void failedOwnedCommitNeverUsesClosedHandle(Operation operation, boolean hardFailure) throws Exception {
		try (var connection = ownedUpdateConnection()) {
			var db = connection.getInternalDB();
			long column = createColumn(connection);
			long id = db.get(0, column, key(1), RequestType.forUpdate(), WorkloadProfile.INGEST).updateId();
			var probe = replaceNativeTransaction(db, id, Long.MAX_VALUE);
			probe.commitFailure = new org.rocksdb.RocksDBException(new org.rocksdb.Status(
					hardFailure ? org.rocksdb.Status.Code.IOError : org.rocksdb.Status.Code.Busy,
					org.rocksdb.Status.SubCode.None, "forced commit failure"));
			assertThrows(RocksDBException.class, () -> operation.run(db, id, column));
			assertEquals(0, probe.closedNativeCalls.get());
			if (!hardFailure && !operation.multi) {
				assertEquals(1, db.getOpenTransactionsCount());
				assertEquals(1, db.getPendingOpsCount());
				assertTrue(probe.original.isOwningHandle());
			}
			db.closeFailedUpdate(id);
			assertEquals(1, probe.closes.get());
			assertEquals(0, db.getOpenTransactionsCount());
			assertEquals(0, db.getPendingOpsCount());
			assertEquals(bytes("a"), db.get(0, column, key(1), RequestType.current()));
		}
	}

	@Test
	void initialForUpdateAllocationIsProtectedDuringNativeRead() throws Exception {
		try (var connection = ownedUpdateConnection()) {
			var db = connection.getInternalDB();
			long column = createColumn(connection);
			var originalDb = db.getDb();
			var nativeProbe = new AtomicReference<NativeProbe>();
			var dbField = EmbeddedDB.class.getDeclaredField("db");
			dbField.setAccessible(true);
			var wrappedDb = mock(TransactionalDB.OptimisticTransactionalDB.class, call -> {
				var result = invoke(call.getMethod(), originalDb, call.getArguments());
				if (call.getMethod().getName().equals("beginTransaction")) {
					var probe = new NativeProbe((Transaction) result);
					probe.blockMethod = "getForUpdate";
					nativeProbe.set(probe);
					return probe.wrapped;
				}
				return result;
			});
			dbField.set(db, wrappedDb);
			var closerThread = new AtomicReference<Thread>();
			try (var executor = Executors.newFixedThreadPool(2)) {
				var request = executor.submit(() -> db.get(0, column, key(1), RequestType.forUpdate(), WorkloadProfile.INGEST));
				try {
					long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
					while (nativeProbe.get() == null && System.nanoTime() < deadline) Thread.onSpinWait();
					var probe = nativeProbe.get();
					assertNotNull(probe);
					assertTrue(probe.entered.await(2, TimeUnit.SECONDS));
					long id = transactionMap(db).entrySet().stream()
							.filter(entry -> entry.getValue().val() == probe.wrapped).findFirst().orElseThrow().getKey();
					var close = executor.submit(() -> {
						closerThread.set(Thread.currentThread());
						db.closeFailedUpdate(id);
					});
					awaitBlockedOnTransaction(closerThread);
					assertEquals(0, probe.closes.get());
					probe.release.countDown();
					assertEquals(bytes("a"), request.get(2, TimeUnit.SECONDS).previous());
					close.get(2, TimeUnit.SECONDS);
					assertEquals(1, probe.closes.get());
					assertEquals(0, probe.closedNativeCalls.get());
				} finally {
					if (nativeProbe.get() != null) nativeProbe.get().release.countDown();
				}
			} finally {
				dbField.set(db, originalDb);
			}
			assertEquals(0, db.getOpenTransactionsCount());
			assertEquals(0, db.getPendingOpsCount());
		}
	}

	@ParameterizedTest
	@MethodSource("explicitMutationAndCommit")
	void explicitMutationsStayInvisibleUntilCommitAndAreDiscardedByRollback(Operation operation, boolean commit) throws Exception {
		try (var connection = ownedUpdateConnection()) {
			var db = connection.getInternalDB();
			long column = createColumn(connection);
			long id = openTransaction(connection, column, false);
			var probe = replaceNativeTransaction(db, id, Long.MAX_VALUE);
			operation.run(db, id, column);
			assertEquals(bytes("a"), db.get(0, column, key(1), RequestType.current()));
			assertEquals(operation.expectedValue(), db.get(id, column, key(1), RequestType.current(), WorkloadProfile.INGEST));
			assertEquals(1, db.getOpenTransactionsCount());
			assertEquals(1, db.getPendingOpsCount());
			assertEquals(0, probe.closes.get());
			assertTrue(db.closeTransaction(id, commit));
			assertEquals(commit ? operation.expectedValue() : bytes("a"), db.get(0, column, key(1), RequestType.current()));
			assertEquals(1, probe.closes.get());
			assertEquals(0, probe.closedNativeCalls.get());
			assertEquals(0, db.getOpenTransactionsCount());
			assertEquals(0, db.getPendingOpsCount());
		}
	}

	@Test
	void sameExplicitTransactionOperationsSerialize() throws Exception {
		try (var connection = ownedUpdateConnection()) {
			var db = connection.getInternalDB();
			long column = createColumn(connection);
			long id = openTransaction(connection, column, false);
			var probe = replaceNativeTransaction(db, id, Long.MAX_VALUE);
			probe.blockMethod = "put";
			var secondThread = new AtomicReference<Thread>();
			try (var executor = Executors.newFixedThreadPool(2)) {
				var mutation = executor.submit(() -> Operation.PUT.run(db, id, column));
				try {
					assertTrue(probe.entered.await(2, TimeUnit.SECONDS));
					var read = executor.submit(() -> {
						secondThread.set(Thread.currentThread());
						return Operation.CURRENT.run(db, id, column);
					});
					awaitBlockedOnTransaction(secondThread);
					assertFalse(read.isDone());
					probe.release.countDown();
					mutation.get(2, TimeUnit.SECONDS);
					assertEquals(bytes("b"), read.get(2, TimeUnit.SECONDS));
				} finally {
					probe.release.countDown();
				}
			}
			assertEquals(0, probe.closedNativeCalls.get());
			assertEquals(0, probe.closes.get());
			assertTrue(db.closeTransaction(id, false));
			assertEquals(bytes("a"), db.get(0, column, key(1), RequestType.current()));
			assertEquals(0, db.getOpenTransactionsCount());
			assertEquals(0, db.getPendingOpsCount());
		}
	}

	@Test
	void conflictingExplicitCommitKeepsItsHandleUntilRollbackAndFreshTransactionReadsNewValue() throws Exception {
		try (var connection = ownedUpdateConnection()) {
			var db = connection.getInternalDB();
			long column = createColumn(connection);
			long id = openTransaction(connection, column, false);
			var probe = replaceNativeTransaction(db, id, Long.MAX_VALUE);
			db.get(id, column, key(1), RequestType.forUpdate(), WorkloadProfile.INGEST);
			db.put(0, column, key(1), bytes("new"), RequestType.none());
			db.put(id, column, key(2), bytes("pending"), RequestType.none());
			assertFalse(db.closeTransaction(id, true));
			assertEquals(0, probe.closes.get());
			assertEquals(1, db.getOpenTransactionsCount());
			assertEquals(1, db.getPendingOpsCount());
			assertTrue(db.closeTransaction(id, false));
			long freshId = openTransaction(connection, column, false);
			assertEquals(bytes("new"), db.get(freshId, column, key(1), RequestType.current(), WorkloadProfile.INGEST));
			assertEquals(null, db.get(freshId, column, key(2), RequestType.current(), WorkloadProfile.INGEST));
			assertTrue(db.closeTransaction(freshId, false));
			assertEquals(0, probe.closedNativeCalls.get());
			assertEquals(0, db.getOpenTransactionsCount());
			assertEquals(0, db.getPendingOpsCount());
		}
	}

	private EmbeddedConnection ownedUpdateConnection() throws Exception {
		var config = tempDir.resolve("owned-update.conf");
		Files.writeString(config, """
				database: {
				  global: { ingest-behind: false, optimistic: true,
				    fallback-column-options: { merge-operator-class: "it.cavallium.rockserver.core.impl.MyStringAppendOperator" }
				  }
				}
				""");
		return new EmbeddedConnection(tempDir.resolve("owned-update"), "owned-update", config);
	}

	private static long createColumn(EmbeddedConnection db) {
		var api = db.getSyncApi(RequestContext.batch());
		long column = api.createColumn("entries", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
		api.put(0, column, key(1), bytes("a"), RequestType.none());
		return column;
	}

	private static Keys key(int key) {
		return new Keys(Buf.wrap(new byte[] {(byte) key}));
	}

	private static Buf bytes(String value) {
		return Buf.wrap(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
	}

	private static Stream<Arguments> operationAndCleaner() {
		return Stream.of(Operation.values()).flatMap(operation -> Stream.of(false, true)
				.flatMap(cleaner -> Stream.of(false, true).map(owned -> Arguments.of(operation, cleaner, owned))));
	}

	private static Stream<Arguments> operationAndOwnership() {
		return Stream.of(Operation.values()).flatMap(operation -> Stream.of(false, true)
				.map(owned -> Arguments.of(operation, owned)));
	}

	private static Stream<Arguments> explicitMutationAndCommit() {
		return Stream.of(Operation.values()).filter(operation -> operation.nativeMethod.matches("put|delete|merge"))
				.flatMap(operation -> Stream.of(false, true).map(commit -> Arguments.of(operation, commit)));
	}

	private static long openTransaction(EmbeddedConnection connection, long column, boolean owned) {
		return owned ? connection.getInternalDB().get(0, column, key(1), RequestType.forUpdate(), WorkloadProfile.INGEST).updateId()
				: connection.getSyncApi(RequestContext.ingest()).openTransaction(java.time.Duration.ofMinutes(1));
	}

	private static Stream<Arguments> mutationAndFailure() {
		return Stream.of(Operation.values()).filter(operation -> operation.nativeMethod.matches("put|delete|merge"))
				.flatMap(operation -> Stream.of(false, true).map(failure -> Arguments.of(operation, failure)));
	}

	static void awaitBlockedOnTransaction(AtomicReference<Thread> reference) {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
		while ((reference.get() == null || reference.get().getState() != Thread.State.BLOCKED)
				&& System.nanoTime() < deadline) Thread.onSpinWait();
		assertNotNull(reference.get());
		assertEquals(Thread.State.BLOCKED, reference.get().getState(), "closer or caller must wait on the transaction monitor");
	}

	private static void cleanupExpiredTransactions(EmbeddedDB db) throws Exception {
		var cleanup = EmbeddedDB.class.getDeclaredMethod("cleanupExpiredTransactionsNow");
		cleanup.setAccessible(true);
		invoke(cleanup, db, new Object[0]);
	}

	@SuppressWarnings("unchecked")
	private static Map<Long, Tx> transactionMap(EmbeddedDB db) throws Exception {
		var field = EmbeddedDB.class.getDeclaredField("txs");
		field.setAccessible(true);
		return (Map<Long, Tx>) field.get(db);
	}

	static NativeProbe replaceNativeTransaction(EmbeddedDB db, long id, long expiry) throws Exception {
		var transactions = transactionMap(db);
		var old = transactions.get(id);
		var probe = new NativeProbe(old.val());
		transactions.put(id, new Tx(probe.wrapped, old.isFromGetForUpdate(), expiry, old.objs(), old.workloadProfile()));
		return probe;
	}

	private static Object invoke(java.lang.reflect.Method method, Object target, Object[] arguments) throws Exception {
		try {
			return method.invoke(target, arguments);
		} catch (InvocationTargetException failure) {
			if (failure.getCause() instanceof Exception exception) throw exception;
			if (failure.getCause() instanceof Error error) throw error;
			throw failure;
		}
	}

	static final class NativeProbe {
		private final Transaction original;
		private final Transaction wrapped;
		final CountDownLatch entered = new CountDownLatch(1);
		final CountDownLatch release = new CountDownLatch(1);
		final AtomicInteger closes = new AtomicInteger();
		final AtomicInteger closedNativeCalls = new AtomicInteger();
		volatile String blockMethod;
		private volatile org.rocksdb.RocksDBException commitFailure;

		private NativeProbe(Transaction original) {
			this.original = original;
			wrapped = mock(Transaction.class, call -> {
				String method = call.getMethod().getName();
				if (method.equals(blockMethod)) {
					entered.countDown();
					assertTrue(release.await(5, TimeUnit.SECONDS), "native boundary gate was not released");
				}
				if (!method.equals("isOwningHandle") && !method.equals("close") && !original.isOwningHandle()) {
					closedNativeCalls.incrementAndGet();
					throw new AssertionError("prevented use of closed native transaction: " + method);
				}
				if (method.equals("close")) closes.incrementAndGet();
				if (method.equals("commit") && commitFailure != null) throw commitFailure;
				return invoke(call.getMethod(), original, call.getArguments());
			});
		}
	}

	private enum Operation {
		PUT("put", false), DELETE("delete", false), MERGE("merge", false),
		PUT_MULTI("put", true), DELETE_MULTI("delete", true), MERGE_MULTI("merge", true),
		CURRENT("getForUpdate", false), FOR_UPDATE("getForUpdate", false);

		private final String nativeMethod;
		private final boolean multi;

		Operation(String nativeMethod, boolean multi) {
			this.nativeMethod = nativeMethod;
			this.multi = multi;
		}

		private String nativeMethod(boolean owned) {
			return this == CURRENT && !owned ? "get" : nativeMethod;
		}

		private Object run(EmbeddedDB db, long id, long column) {
			return switch (this) {
				case PUT -> db.put(id, column, key(1), bytes("b"), RequestType.none());
				case DELETE -> db.delete(id, column, key(1), RequestType.none());
				case MERGE -> db.merge(id, column, key(1), bytes("b"), RequestType.none());
				case PUT_MULTI -> db.putMulti(id, column, List.of(key(1)), List.of(bytes("b")), RequestType.none());
				case DELETE_MULTI -> db.deleteMulti(id, column, List.of(key(1)), RequestType.none());
				case MERGE_MULTI -> db.mergeMulti(id, column, List.of(key(1)), List.of(bytes("b")), RequestType.none());
				case CURRENT -> db.get(id, column, key(1), RequestType.current(), WorkloadProfile.INGEST);
				case FOR_UPDATE -> db.get(id, column, key(1), RequestType.forUpdate(), WorkloadProfile.INGEST);
			};
		}

		private Buf expectedValue() {
			return switch (this) {
				case PUT, PUT_MULTI -> bytes("b");
				case DELETE, DELETE_MULTI -> null;
				case MERGE, MERGE_MULTI -> bytes("a,b");
				case CURRENT, FOR_UPDATE -> bytes("a");
			};
		}
	}

	private static CloseResult closeTransaction(EmbeddedConnection db,
			long transactionId,
			AtomicReference<Thread> thread,
			CountDownLatch ready,
			CountDownLatch start) throws InterruptedException {
		thread.set(Thread.currentThread());
		ready.countDown();
		start.await();
		try {
			return new CloseResult(db.getInternalDB().closeTransaction(transactionId, true), null);
		} catch (Throwable error) {
			return new CloseResult(false, error);
		}
	}

	private static void assertOneSuccessfulCommit(CloseResult first, CloseResult second) {
		CloseResult successful = first.error() == null ? first : second;
		CloseResult rejected = first.error() != null ? first : second;
		assertTrue(successful.committed());
		RocksDBException error = assertInstanceOf(RocksDBException.class, rejected.error());
		assertEquals(RocksDBErrorType.TX_NOT_FOUND, error.getErrorUniqueId());
	}

	@SuppressWarnings("unchecked")
	private static Object getTransactionMonitor(EmbeddedDB db, long transactionId) throws Exception {
		Field transactionsField = EmbeddedDB.class.getDeclaredField("txs");
		transactionsField.setAccessible(true);
		Map<Long, ?> transactions = (Map<Long, ?>) transactionsField.get(db);
		Object transaction = transactions.get(transactionId);
		assertNotNull(transaction);
		return transaction;
	}

	private record CloseResult(boolean committed, Throwable error) {
	}
}
