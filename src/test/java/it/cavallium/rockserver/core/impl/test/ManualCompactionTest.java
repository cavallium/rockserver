package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
class ManualCompactionTest {

	@Test
	void compactIsIndependentFromTheProcessLocale(@TempDir Path tempDir) throws Exception {
		Locale previousLocale = Locale.getDefault();
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "compact-locale", null)) {
			var api = connection.getSyncApi();
			api.createColumn("entries", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));

			Locale.setDefault(Locale.ITALY);
			assertDoesNotThrow(api::compact);
		} finally {
			Locale.setDefault(previousLocale);
		}
	}

	@Test
	void deletingAColumnWaitsForCompactionToReleaseItsHandle(@TempDir Path tempDir) throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "compact-delete", null)) {
			var api = connection.getSyncApi();
			var internal = connection.getInternalDB();
			long columnId = api.createColumn("entries", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
			var compactionEntered = new CountDownLatch(1);
			var releaseCompaction = new CountDownLatch(1);
			var deleteStarted = new CountDownLatch(1);
			internal.setColumnMaintenanceObserverForTesting(() -> {
				compactionEntered.countDown();
				await(releaseCompaction);
			});

			CompletableFuture<Void> compaction = CompletableFuture.runAsync(api::compact);
			CompletableFuture<Void> deletion;
			try {
				assertTrue(compactionEntered.await(10, TimeUnit.SECONDS), "Compaction did not enter its handle lease");
				deletion = CompletableFuture.runAsync(() -> {
					deleteStarted.countDown();
					api.deleteColumn(columnId);
				});
				assertTrue(deleteStarted.await(10, TimeUnit.SECONDS), "Column deletion did not start");
				CompletableFuture<Void> pendingDeletion = deletion;
				assertThrows(TimeoutException.class,
						() -> pendingDeletion.get(200, TimeUnit.MILLISECONDS),
						"Column deletion must not close a handle used by compaction");
			} finally {
				releaseCompaction.countDown();
				internal.setColumnMaintenanceObserverForTesting(null);
			}

			compaction.get(10, TimeUnit.SECONDS);
			deletion.get(10, TimeUnit.SECONDS);
		}
	}

	private static void await(CountDownLatch latch) {
		try {
			if (!latch.await(10, TimeUnit.SECONDS)) {
				throw new AssertionError("Timed out waiting for test coordination");
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError(e);
		}
	}
}
