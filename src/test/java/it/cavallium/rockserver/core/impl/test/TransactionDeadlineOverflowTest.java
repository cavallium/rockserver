package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import java.lang.reflect.Method;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
class TransactionDeadlineOverflowTest {

	@Test
	void maximumTimeoutSurvivesLeakCleanupAndReleasesItsLeaseExactlyOnce(@TempDir Path tempDir)
			throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"),
				"transaction-deadline-overflow", null)) {
			EmbeddedDB internal = connection.getInternalDB();
			var api = connection.getSyncApi(RequestContext.batch());
			long transactionId = api.openTransaction( java.time.Duration.ofMillis(Long.MAX_VALUE));

			assertEquals(1, internal.getOpenTransactionsCount());
			assertEquals(1L, internal.getPendingOpsCount(),
					"the open transaction must own exactly one native resource lease");

			invokeTransactionCleanup(internal);
			assertEquals(1, internal.getOpenTransactionsCount(),
					"a saturating timeout must not wrap into an already-expired timestamp");
			assertEquals(1L, internal.getPendingOpsCount());

			assertTrue(api.closeTransaction(transactionId, false));
			assertEquals(0, internal.getOpenTransactionsCount());
			assertEquals(0L, internal.getPendingOpsCount(),
					"explicit rollback must release the transaction lease exactly once");
		}
	}

	@Test
	void negativeTimeoutIsRejectedBeforeAllocatingANativeTransaction(@TempDir Path tempDir)
			throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("db-negative"),
				"transaction-negative-timeout", null)) {
			EmbeddedDB internal = connection.getInternalDB();
			var api = connection.getSyncApi(RequestContext.batch());

			var failure = assertThrows(RocksDBException.class, () -> api.openTransaction( java.time.Duration.ofMillis(-1L)));
			assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, failure.getErrorUniqueId());
			assertEquals(0, internal.getOpenTransactionsCount());
			assertEquals(0L, internal.getPendingOpsCount(),
					"invalid timeout admission must not allocate or retain a native resource");
		}
	}

	private static void invokeTransactionCleanup(EmbeddedDB database) throws Exception {
		Method cleanup = EmbeddedDB.class.getDeclaredMethod("cleanupExpiredTransactionsNow");
		cleanup.setAccessible(true);
		cleanup.invoke(database);
	}
}
