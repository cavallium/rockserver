package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.SafeShutdown;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDB;

@Timeout(90)
class ScanRawCompactionSafetyTest {

	private static final String COLUMN_NAME = "events";
	private static final int GENERATIONS = 4;
	private static final int KEYS_PER_GENERATION = 32;

	@Test
	void compactionCannotDeleteCapturedSstsUntilScanCompletes(@TempDir Path tempDir) throws Exception {
		Path configFile = tempDir.resolve("scan-raw.conf");
		Files.writeString(configFile, """
				database: { global: {
				  ingest-behind: false
				  optimistic: false
				  disable-auto-compactions: true
				  disable-write-slowdown: true
				} }
				""");
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "scan-raw-compaction", configFile)) {
			RocksDBSyncAPI api = connection.getSyncApi();
			EmbeddedDB internal = connection.getInternalDB();
			long columnId = api.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));

			writeOverlappingSsts(api, columnId);
			Set<Path> capturedFiles = liveColumnSsts(internal);
			assertEquals(GENERATIONS, capturedFiles.size(),
					"The test needs one overlapping input SST per flush generation");

			var filesCaptured = new CountDownLatch(1);
			var allowReadersToOpen = new CountDownLatch(1);
			internal.setRawScanFilesCapturedObserverForTesting(() -> {
				filesCaptured.countDown();
				awaitLatch(allowReadersToOpen);
			});

			CompletableFuture<? extends java.util.List<?>> scan = internal
					.scanRawAsyncInternal(columnId, 0, 1)
					.collectList()
					.toFuture();
			try {
				assertTrue(filesCaptured.await(10, TimeUnit.SECONDS), "Raw scan did not capture its SST list");
				api.compact();

				Set<Path> currentFiles = liveColumnSsts(internal);
				Set<Path> obsoleteCapturedFiles = new LinkedHashSet<>(capturedFiles);
				obsoleteCapturedFiles.removeAll(currentFiles);
				assertFalse(obsoleteCapturedFiles.isEmpty(),
						"Manual compaction must replace at least one captured input SST");
				assertTrue(obsoleteCapturedFiles.stream().allMatch(Files::exists),
						"Captured input SSTs must stay on disk while their raw scan is active");

				allowReadersToOpen.countDown();
				var batches = scan.get(30, TimeUnit.SECONDS);
				long entries = batches.stream()
						.map(batch -> (it.cavallium.rockserver.core.common.SerializedKVBatch) batch)
						.mapToLong(batch -> batch.decode().count())
						.sum();
				assertEquals((long) GENERATIONS * KEYS_PER_GENERATION, entries,
						"The scan must read every captured physical generation, even after compaction");

				awaitFilesDeleted(obsoleteCapturedFiles, Duration.ofSeconds(10));
			} finally {
				internal.setRawScanFilesCapturedObserverForTesting(null);
				allowReadersToOpen.countDown();
				if (!scan.isDone()) {
					scan.cancel(true);
				}
			}
		}
	}

	@Test
	void cancellationReleasesTheFileDeletionLease(@TempDir Path tempDir) throws Exception {
		Path configFile = tempDir.resolve("scan-raw-cancel.conf");
		Files.writeString(configFile, """
				database: { global: {
				  ingest-behind: false
				  optimistic: false
				  disable-auto-compactions: true
				  disable-write-slowdown: true
				} }
				""");
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "scan-raw-cancel", configFile)) {
			RocksDBSyncAPI api = connection.getSyncApi();
			EmbeddedDB internal = connection.getInternalDB();
			long columnId = api.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			writeOverlappingSsts(api, columnId);
			Set<Path> capturedFiles = liveColumnSsts(internal);

			var filesCaptured = new CountDownLatch(1);
			var allowReadersToOpen = new CountDownLatch(1);
			internal.setRawScanFilesCapturedObserverForTesting(() -> {
				filesCaptured.countDown();
				awaitLatch(allowReadersToOpen);
			});
			CompletableFuture<? extends java.util.List<?>> scan = internal
					.scanRawAsyncInternal(columnId, 0, 1)
					.collectList()
					.toFuture();
			try {
				assertTrue(filesCaptured.await(10, TimeUnit.SECONDS));
				assertTrue(scan.cancel(true));
				allowReadersToOpen.countDown();
				api.compact();

				Set<Path> obsoleteCapturedFiles = new LinkedHashSet<>(capturedFiles);
				obsoleteCapturedFiles.removeAll(liveColumnSsts(internal));
				assertFalse(obsoleteCapturedFiles.isEmpty());
				awaitFilesDeleted(obsoleteCapturedFiles, Duration.ofSeconds(10));
			} finally {
				internal.setRawScanFilesCapturedObserverForTesting(null);
				allowReadersToOpen.countDown();
			}
		}
	}

	@Test
	void rejectedAdmissionDoesNotEndAnUnrelatedShutdownOperation(@TempDir Path tempDir) throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "scan-raw-shutdown", null)) {
			long columnId = connection.getSyncApi().createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			EmbeddedDB internal = connection.getInternalDB();
			SafeShutdown shutdown = shutdownOf(internal);
			shutdown.beginOp();
			try {
				assertThrows(TimeoutException.class, () -> shutdown.closeAndWait(0L));
				assertThrows(IllegalStateException.class,
						() -> internal.scanRawAsyncInternal(columnId, 0, 1).blockLast());
				assertEquals(1L, internal.getPendingOpsCount(),
						"Rejected scan admission must not decrement an unrelated operation");
			} finally {
				shutdown.endOp();
			}
		}
	}

	@Test
	void failedFileDeletionEnableIsRetried(@TempDir Path tempDir) throws Exception {
		try (var db = new FailOnceEnableEmbeddedDB(tempDir.resolve("db"), "scan-raw-enable-retry")) {
			long columnId = db.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			db.put(0, columnId, key(1), value(1, 1), RequestType.none());
			db.flush();

			db.scanRawAsyncInternal(columnId, 0, 1).collectList().block();

			assertTrue(db.recovered.await(5, TimeUnit.SECONDS),
					"Raw scan did not retry the failed enableFileDeletions call");
			assertEquals(2, db.enableAttempts.get());
		}
	}

	private static void writeOverlappingSsts(RocksDBSyncAPI api, long columnId) {
		for (int generation = 0; generation < GENERATIONS; generation++) {
			for (int key = 0; key < KEYS_PER_GENERATION; key++) {
				api.put(0,
						columnId,
						key(key),
						value(generation, key),
						RequestType.none());
			}
			api.flush();
		}
	}

	private static Set<Path> liveColumnSsts(EmbeddedDB db) {
		var result = new LinkedHashSet<Path>();
		for (LiveFileMetaData file : db.getDb().get().getLiveFilesMetaData()) {
			if (COLUMN_NAME.equals(new String(file.columnFamilyName(), StandardCharsets.UTF_8))
					&& file.fileName().endsWith(".sst")) {
				result.add(filePath(file));
			}
		}
		return result;
	}

	private static Path filePath(LiveFileMetaData file) {
		String filePath = file.path();
		if (!filePath.endsWith(".sst")) {
			filePath += file.fileName();
		}
		return Path.of(filePath);
	}

	private static void awaitFilesDeleted(Set<Path> files, Duration timeout) throws InterruptedException {
		long deadline = System.nanoTime() + timeout.toNanos();
		do {
			if (files.stream().noneMatch(Files::exists)) {
				return;
			}
			Thread.sleep(20);
		} while (System.nanoTime() < deadline);
		assertTrue(files.stream().noneMatch(Files::exists),
				"Obsolete SSTs must be deleted after the raw-scan lease is released: " + files);
	}

	private static void awaitLatch(CountDownLatch latch) {
		try {
			if (!latch.await(30, TimeUnit.SECONDS)) {
				throw new AssertionError("Timed out waiting to continue raw scan");
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError("Interrupted while waiting to continue raw scan", e);
		}
	}

	private static Keys key(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Buf value(int generation, int key) {
		return Buf.wrap(ByteBuffer.allocate(2 * Integer.BYTES).putInt(generation).putInt(key).array());
	}

	private static SafeShutdown shutdownOf(EmbeddedDB db) throws ReflectiveOperationException {
		var field = EmbeddedDB.class.getDeclaredField("ops");
		field.setAccessible(true);
		return (SafeShutdown) field.get(db);
	}

	private static final class FailOnceEnableEmbeddedDB extends EmbeddedDB {
		private final AtomicInteger enableAttempts = new AtomicInteger();
		private final CountDownLatch recovered = new CountDownLatch(1);

		private FailOnceEnableEmbeddedDB(Path path, String name) throws IOException {
			super(path, name, null);
		}

		@Override
		protected void enableRawScanFileDeletions(RocksDB rocksDB) throws org.rocksdb.RocksDBException {
			if (enableAttempts.incrementAndGet() == 1) {
				throw new org.rocksdb.RocksDBException("synthetic enable failure");
			}
			super.enableRawScanFileDeletions(rocksDB);
			recovered.countDown();
		}
	}
}
