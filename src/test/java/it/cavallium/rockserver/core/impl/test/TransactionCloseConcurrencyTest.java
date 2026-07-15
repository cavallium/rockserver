package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(10)
class TransactionCloseConcurrencyTest {

	@TempDir
	Path tempDir;

	@Test
	void duplicateCommitsSerializeAndBalanceTheTransactionOnce() throws Exception {
		try (var db = new EmbeddedConnection(tempDir.resolve("duplicate-close"), "duplicate-close", null)) {
			EmbeddedDB internal = db.getInternalDB();
			long transactionId = db.openTransaction(10_000);
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
					awaitBlocked(firstThread);
					awaitBlocked(secondThread);
				}

				assertOneSuccessfulCommit(first.get(1, TimeUnit.SECONDS), second.get(1, TimeUnit.SECONDS));
			}

			assertEquals(0, internal.getOpenTransactionsCount());
			assertEquals(0, internal.getPendingOpsCount());
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
			return new CloseResult(db.closeTransaction(transactionId, true), null);
		} catch (Throwable error) {
			return new CloseResult(false, error);
		}
	}

	private static void awaitBlocked(AtomicReference<Thread> threadReference) {
		Thread thread = threadReference.get();
		assertNotNull(thread);
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
		while (thread.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
			Thread.onSpinWait();
		}
		assertEquals(Thread.State.BLOCKED, thread.getState(),
				"transaction closer did not wait for exclusive ownership");
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
