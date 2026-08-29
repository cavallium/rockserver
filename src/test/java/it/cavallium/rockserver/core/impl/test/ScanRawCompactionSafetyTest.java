package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.RWScheduler;
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
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
	private static final int COOPERATIVE_SCAN_KEYS = 5_000;

	@Test
	void blockedConcurrentPinCapturesDoNotStarveLatencyLookups(@TempDir Path tempDir) throws Exception {
		final int readParallelism = 3;
		Path configFile = tempDir.resolve("scan-raw-capture-isolation.conf");
		Files.writeString(configFile, """
				database.parallelism.read = %d
				database.parallelism.write = 3
				database.parallelism.workload.competing-batch-read-maximum-active = %d
				""".formatted(readParallelism, readParallelism));
		String databaseName = "scan-raw-capture-isolation";
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), databaseName, configFile)) {
			RocksDBSyncAPI batch = connection.getSyncApi(RequestContext.batch());
			EmbeddedDB internal = connection.getInternalDB();
			long columnId = batch.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			Buf expected = value(0, 1);
			batch.put(0, columnId, key(1), expected, RequestType.none());
			batch.flush();

			var firstHardLinkEntered = new CountDownLatch(1);
			var releaseHardLink = new CountDownLatch(1);
			var allScansAdmitted = new CountDownLatch(readParallelism);
			var blockFirstHardLink = new AtomicBoolean(true);
			internal.setColumnUseAcquiredObserverForTesting(_ -> allScansAdmitted.countDown());
			internal.setRawScanHardLinkObserverForTesting(() -> {
				if (blockFirstHardLink.compareAndSet(true, false)) {
					firstHardLinkEntered.countDown();
					awaitLatch(releaseHardLink);
				}
			});

			List<CompletableFuture<List<SerializedKVBatch>>> scans = java.util.stream.IntStream
					.range(0, readParallelism)
					.mapToObj(_ -> internal.scanRawAsyncInternal(columnId, 0, 1).collectList().toFuture())
					.toList();
			try {
				assertTrue(firstHardLinkEntered.await(10, TimeUnit.SECONDS),
						"the first raw scan never entered hard-link capture");
				assertTrue(allScansAdmitted.await(10, TimeUnit.SECONDS),
						"the regression needs at least readParallelism scans waiting to capture pins");

				Buf actual = connection.getAsyncApi(RequestContext.latency(Duration.ofSeconds(10)))
						.getAsync(0, columnId, key(1), RequestType.current())
						.get(2, TimeUnit.SECONDS);
				assertEquals(expected, actual,
						"serialized raw-scan capture must not prevent admitted LATENCY reads from dispatching");
			} finally {
				releaseHardLink.countDown();
				internal.setColumnUseAcquiredObserverForTesting(null);
				internal.setRawScanHardLinkObserverForTesting(null);
			}

			for (CompletableFuture<List<SerializedKVBatch>> scan : scans) {
				List<SerializedKVBatch> batches = scan.get(30, TimeUnit.SECONDS);
				assertEquals(1, batches.size());
				assertEquals(1L, batches.getFirst().decode().count());
			}
			assertTrue(awaitCondition(() -> {
				var snapshot = internal.getScheduler().poolSnapshot(RWScheduler.Pool.READ);
				return snapshot.activeTasks() == 0 && snapshot.queuedTasks() == 0;
			}, Duration.ofSeconds(10)), "all raw-scan and LATENCY scheduler tasks must drain");
			assertEquals(0L, internal.getPendingOpsCount());
			assertTrue(internal.getRawScanPinnedFilesForTesting().isEmpty());
			assertFileDeletionsEnabled(internal);
		}
	}

	@Test
	void cancellingQueuedPinCaptureDoesNotRunOrLeakItsResources(@TempDir Path tempDir) throws Exception {
		Path configFile = tempDir.resolve("scan-raw-capture-cancel.conf");
		Files.writeString(configFile, """
				database.parallelism.read = 3
				database.parallelism.write = 3
				database.parallelism.workload.batch-queue-capacity = 1
				database.parallelism.workload.competing-batch-read-maximum-active = 3
				""");
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "scan-raw-capture-cancel", configFile)) {
			RocksDBSyncAPI batch = connection.getSyncApi(RequestContext.batch());
			EmbeddedDB internal = connection.getInternalDB();
			long columnId = batch.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			batch.put(0, columnId, key(1), value(0, 1), RequestType.none());
			batch.flush();

			var firstHardLinkEntered = new CountDownLatch(1);
			var releaseHardLink = new CountDownLatch(1);
			var hardLinkAttempts = new AtomicInteger();
			internal.setRawScanHardLinkObserverForTesting(() -> {
				if (hardLinkAttempts.incrementAndGet() == 1) {
					firstHardLinkEntered.countDown();
					awaitLatch(releaseHardLink);
				}
			});

			CompletableFuture<List<SerializedKVBatch>> active = internal
					.scanRawAsyncInternal(columnId, 0, 1).collectList().toFuture();
			CompletableFuture<List<SerializedKVBatch>> queued;
			CompletableFuture<List<SerializedKVBatch>> replacement;
			try {
				assertTrue(firstHardLinkEntered.await(10, TimeUnit.SECONDS));
				queued = internal.scanRawAsyncInternal(columnId, 0, 1).collectList().toFuture();
				assertTrue(awaitCondition(
						() -> internal.getRawScanPinCaptureQueueSizeForTesting() == 1,
						Duration.ofSeconds(2)), "the second capture did not enter the bounded queue");
				assertEquals(1L, internal.getRawScanPinCaptureQueueCapacityForTesting());
				assertTrue(queued.cancel(true), "the queued scan should still be cancellable before pin capture");
				assertTrue(queued.isCancelled());
				assertTrue(awaitCondition(
						() -> internal.getRawScanPinCaptureQueueSizeForTesting() == 0,
						Duration.ofSeconds(2)), "cancellation must physically remove the queued capture");
				replacement = internal.scanRawAsyncInternal(columnId, 0, 1).collectList().toFuture();
				assertTrue(awaitCondition(
						() -> internal.getRawScanPinCaptureQueueSizeForTesting() == 1,
						Duration.ofSeconds(2)), "the canceled slot must accept a replacement without waiting for capture");
			} finally {
				releaseHardLink.countDown();
			}

			assertEquals(1, active.get(30, TimeUnit.SECONDS).size());
			assertEquals(1, replacement.get(30, TimeUnit.SECONDS).size());
			internal.setRawScanHardLinkObserverForTesting(null);
			assertTrue(awaitCondition(() -> internal.getPendingOpsCount() == 0L, Duration.ofSeconds(10)),
					"cancelling a queued capture must release its operation and column lease");
			assertEquals(2, hardLinkAttempts.get(),
					"only the active capture and its admitted replacement may enter the critical section");
			assertTrue(internal.getRawScanPinnedFilesForTesting().isEmpty());
			assertFileDeletionsEnabled(internal);
			batch.deleteColumn(columnId);
		}
	}

	@Test
	void pinCaptureQueueIsBoundedAndRejectsOverflowWithoutBlockingReadWorkers(@TempDir Path tempDir)
			throws Exception {
		final int queueCapacity = 2;
		Path configFile = tempDir.resolve("scan-raw-capture-saturation.conf");
		Files.writeString(configFile, """
				database.parallelism.read = 3
				database.parallelism.write = 3
				database.parallelism.workload.batch-queue-capacity = %d
				database.parallelism.workload.competing-batch-read-maximum-active = 3
				""".formatted(queueCapacity));
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"),
				"scan-raw-capture-saturation",
				configFile)) {
			RocksDBSyncAPI batch = connection.getSyncApi(RequestContext.batch());
			EmbeddedDB internal = connection.getInternalDB();
			long columnId = batch.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			Buf expected = value(0, 1);
			batch.put(0, columnId, key(1), expected, RequestType.none());
			batch.flush();

			var firstHardLinkEntered = new CountDownLatch(1);
			var releaseHardLink = new CountDownLatch(1);
			var initialScansAdmitted = new CountDownLatch(queueCapacity + 1);
			var hardLinkAttempts = new AtomicInteger();
			internal.setColumnUseAcquiredObserverForTesting(_ -> initialScansAdmitted.countDown());
			internal.setRawScanHardLinkObserverForTesting(() -> {
				if (hardLinkAttempts.incrementAndGet() == 1) {
					firstHardLinkEntered.countDown();
					awaitLatch(releaseHardLink);
				}
			});

			List<CompletableFuture<List<SerializedKVBatch>>> accepted = java.util.stream.IntStream
					.range(0, queueCapacity + 1)
					.mapToObj(_ -> internal.scanRawAsyncInternal(columnId, 0, 1).collectList().toFuture())
					.toList();
			CompletableFuture<List<SerializedKVBatch>> overflow = null;
			try {
				assertTrue(firstHardLinkEntered.await(10, TimeUnit.SECONDS));
				assertTrue(initialScansAdmitted.await(10, TimeUnit.SECONDS));
				assertTrue(awaitCondition(
						() -> internal.getRawScanPinCaptureQueueSizeForTesting() == queueCapacity,
						Duration.ofSeconds(10)), "the capture lane did not reach its configured queue bound");
				assertEquals(queueCapacity, internal.getRawScanPinCaptureQueueCapacityForTesting(),
						"capture waiting must inherit the bounded BATCH admission capacity");

				Buf actual = connection.getAsyncApi(RequestContext.latency(Duration.ofSeconds(10)))
						.getAsync(0, columnId, key(1), RequestType.current())
						.get(2, TimeUnit.SECONDS);
				assertEquals(expected, actual, "a saturated capture lane must not consume read workers");

				var rejectedCapture = internal.scanRawAsyncInternal(columnId, 0, 1).collectList().toFuture();
				overflow = rejectedCapture;
				ExecutionException rejected = assertThrows(ExecutionException.class,
						() -> rejectedCapture.get(2, TimeUnit.SECONDS));
				assertTrue(rejected.getCause() instanceof RocksDBException rocksFailure
						&& rocksFailure.getErrorUniqueId() == RocksDBException.RocksDBErrorType.SERVER_OVERLOADED
						&& rocksFailure.getMessage().contains("Raw-scan pin capture queue is full"),
						"capture saturation must fail promptly through the typed overload contract: " + rejected);
				assertEquals(queueCapacity, internal.getRawScanPinCaptureQueueSizeForTesting(),
						"rejected capture must not exceed the configured queue bound");
				assertEquals(1, hardLinkAttempts.get(),
						"rejected capture must not enter the file-deletion critical section");
			} finally {
				if (overflow != null && !overflow.isDone()) {
					overflow.cancel(true);
				}
				releaseHardLink.countDown();
				internal.setColumnUseAcquiredObserverForTesting(null);
				internal.setRawScanHardLinkObserverForTesting(null);
			}

			for (CompletableFuture<List<SerializedKVBatch>> scan : accepted) {
				assertEquals(1, scan.get(30, TimeUnit.SECONDS).size());
			}
			assertTrue(awaitCondition(() -> internal.getPendingOpsCount() == 0L, Duration.ofSeconds(10)));
			assertEquals(0, internal.getRawScanPinCaptureQueueSizeForTesting());
			assertTrue(internal.getRawScanPinnedFilesForTesting().isEmpty());
			assertFileDeletionsEnabled(internal);
			batch.deleteColumn(columnId);
		}
	}

	@Test
	void competingReadWorkYieldsWithoutShorteningWireBatches(@TempDir Path tempDir) throws Exception {
		Path configFile = tempDir.resolve("scan-raw-cooperative.conf");
		Files.writeString(configFile, """
				database.parallelism.workload.range-quantum-max-duration = PT0.000001S
				""");
		String databaseName = "scan-raw-cooperative";
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), databaseName, configFile)) {
			RocksDBSyncAPI api = connection.getSyncApi(RequestContext.batch());
			EmbeddedDB internal = connection.getInternalDB();
			long columnId = api.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			for (int i = 0; i < COOPERATIVE_SCAN_KEYS; i++) {
				api.put(0, columnId, key(i), value(0, i), RequestType.none());
			}
			api.flush();

			var foregroundStarted = new CountDownLatch(1);
			var releaseForeground = new CountDownLatch(1);
			internal.getScheduler().executor(
					WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30)).execute(() -> {
				foregroundStarted.countDown();
				awaitLatch(releaseForeground);
			});
			assertTrue(foregroundStarted.await(10, TimeUnit.SECONDS));

			var quantumCounter = internal.getMetricsRegistry()
					.get("rockserver.workload.quantums")
					.tags("database", databaseName,
							"resource", "read",
							"profile", "batch",
							"operation", "range_page")
					.counter();
			double quantumsBefore = quantumCounter.count();
			try {
				var batches = internal.scanRawAsyncInternal(columnId, 0, 1)
						.collectList()
						.block(Duration.ofSeconds(30));
				assertEquals(1, batches.size(),
						"cooperative yields must retain the partial buffer instead of shortening wire batches");
				assertEquals(COOPERATIVE_SCAN_KEYS, batches.getFirst().decode().count());
				long metricDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
				while (quantumCounter.count() - quantumsBefore <= 10.0
						&& System.nanoTime() < metricDeadline) {
					Thread.onSpinWait();
				}
				assertTrue(quantumCounter.count() - quantumsBefore > 10.0,
						"the competing LATENCY task must force repeated raw-scan scheduler quanta");
			} finally {
				releaseForeground.countDown();
			}
		}
	}

	@Test
	void nativeCleanupFailureIsASchedulerFailureAndDrainsTheScan(@TempDir Path tempDir) throws Exception {
		String databaseName = "scan-raw-cleanup-failure";
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), databaseName, null)) {
			RocksDBSyncAPI api = connection.getSyncApi(RequestContext.batch());
			EmbeddedDB internal = connection.getInternalDB();
			long columnId = api.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			api.put(0, columnId, key(1), value(0, 1), RequestType.none());
			api.flush();

			internal.setRawScanCleanupObserverForTesting(() -> {
				throw new IllegalStateException("synthetic raw scan cleanup failure");
			});
			var scheduler = internal.getScheduler();
			var before = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			try {
				var failure = assertThrows(IllegalStateException.class,
						() -> internal.scanRawAsyncInternal(
								columnId,
								0,
								1,
								reactor.core.scheduler.Schedulers.immediate(),
								scheduler.readExecutor())
								.collectList()
								.block(Duration.ofSeconds(10)));
				assertEquals("synthetic raw scan cleanup failure", failure.getMessage());
				assertEquals(0L, internal.getPendingOpsCount());

				var after = scheduler.poolSnapshot(RWScheduler.Pool.READ);
				assertEquals(1L, after.acceptedTasks() - before.acceptedTasks());
				assertEquals(1L, after.outcomes().get(RWScheduler.TerminalOutcome.FAILURE)
						- before.outcomes().get(RWScheduler.TerminalOutcome.FAILURE));
				assertEquals(0L, after.outcomes().get(RWScheduler.TerminalOutcome.RUN)
						- before.outcomes().get(RWScheduler.TerminalOutcome.RUN));
				assertEquals(0, after.activeTasks());
				assertEquals(0, after.queuedTasks());
			} finally {
				internal.setRawScanCleanupObserverForTesting(null);
			}
		}
	}

	@Test
	void firstDispatchParksUntilTheSchedulerHandleIsAttached(@TempDir Path tempDir) throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "scan-raw-attach", null)) {
			RocksDBSyncAPI api = connection.getSyncApi(RequestContext.batch());
			EmbeddedDB internal = connection.getInternalDB();
			long columnId = api.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			api.put(0, columnId, key(1), value(0, 1), RequestType.none());
			api.flush();

			var firstDispatch = new AtomicReference<RWScheduler.CooperativeResult>();
			RWScheduler.WorkloadExecutor immediateExecutor = new RWScheduler.WorkloadExecutor() {
				@Override
				public void execute(Runnable command) {
					command.run();
				}

				@Override
				public void execute(Runnable command, long estimatedBytes) {
					command.run();
				}

				@Override
				public RWScheduler.CooperativeHandle executeCooperatively(
						RWScheduler.CooperativeTask command,
						long estimatedBytes) {
					firstDispatch.set(command.runCooperatively(new RWScheduler.CooperativeContext() {
						@Override
						public boolean preemptionRequested() {
							return false;
						}

						@Override
						public boolean terminationRequested() {
							return false;
						}

						@Override
						public RuntimeException terminationFailure() {
							return null;
						}

						@Override
						public boolean fail(RuntimeException failure) {
							return true;
						}
					}));
					return new RWScheduler.CooperativeHandle() {
						private boolean disposed;

						@Override
						public void resume() {
							command.reject(new RejectedExecutionException("stop after attach proof"));
						}

						@Override
						public void dispose() {
							disposed = true;
						}

						@Override
						public boolean isDisposed() {
							return disposed;
						}
					};
				}
			};

			assertThrows(RejectedExecutionException.class,
					() -> internal.scanRawAsyncInternal(
							columnId,
							0,
							1,
							reactor.core.scheduler.Schedulers.immediate(),
							immediateExecutor)
							.collectList()
							.block(Duration.ofSeconds(10)));
			assertEquals(RWScheduler.CooperativeResult.PARK, firstDispatch.get(),
					"the first dispatch must not open native state before attach returns the handle");
			assertEquals(0L, internal.getPendingOpsCount());
		}
	}

	@Test
	void compactionCanDeleteOriginalSstsWhilePinnedScanCompletes(@TempDir Path tempDir) throws Exception {
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
			RocksDBSyncAPI api = connection.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
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
				Set<Path> pinnedFiles = internal.getRawScanPinnedFilesForTesting();
				assertEquals(capturedFiles.size(), pinnedFiles.size());
				assertHardLinks(capturedFiles, pinnedFiles);
				assertFileDeletionsEnabled(internal);

				api.compact();

				Set<Path> currentFiles = liveColumnSsts(internal);
				Set<Path> obsoleteCapturedFiles = new LinkedHashSet<>(capturedFiles);
				obsoleteCapturedFiles.removeAll(currentFiles);
				assertFalse(obsoleteCapturedFiles.isEmpty(),
						"Manual compaction must replace at least one captured input SST");
				awaitFilesDeleted(obsoleteCapturedFiles, Duration.ofSeconds(10));
				assertTrue(pinnedFiles.stream().allMatch(Files::exists),
						"Raw-scan hard links must retain captured SST contents after RocksDB deletes originals");

				allowReadersToOpen.countDown();
				var batches = scan.get(30, TimeUnit.SECONDS);
				long entries = batches.stream()
						.map(batch -> (it.cavallium.rockserver.core.common.SerializedKVBatch) batch)
						.mapToLong(batch -> batch.decode().count())
						.sum();
				assertEquals((long) GENERATIONS * KEYS_PER_GENERATION, entries,
						"The scan must read every captured physical generation, even after compaction");

				awaitFilesDeleted(pinnedFiles, Duration.ofSeconds(10));
				assertFileDeletionsEnabled(internal);
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
	void cancellationReleasesPinnedSstsWithoutChangingFileDeletionState(@TempDir Path tempDir) throws Exception {
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
			RocksDBSyncAPI api = connection.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
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
				Set<Path> pinnedFiles = internal.getRawScanPinnedFilesForTesting();
				assertEquals(capturedFiles.size(), pinnedFiles.size());
				assertFileDeletionsEnabled(internal);
				assertTrue(scan.cancel(true));
				allowReadersToOpen.countDown();
				awaitFilesDeleted(pinnedFiles, Duration.ofSeconds(10));
				assertFileDeletionsEnabled(internal);
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
	void activeCancellationKeepsPinsAndDatabaseLeasesUntilNativeReaderCleanup(@TempDir Path tempDir)
			throws Exception {
		Path configFile = tempDir.resolve("scan-raw-active-cancel.conf");
		Files.writeString(configFile, """
				database: { global: {
				  ingest-behind: false
				  optimistic: false
				  disable-auto-compactions: true
				  disable-write-slowdown: true
				} }
				""");
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "scan-raw-active-cancel", configFile)) {
			RocksDBSyncAPI api = connection.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
			EmbeddedDB internal = connection.getInternalDB();
			long columnId = api.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			writeOverlappingSsts(api, columnId);

			for (int attempt = 0; attempt < 8; attempt++) {
				var readerOpened = new CountDownLatch(1);
				var allowNativeReaderToReturn = new CountDownLatch(1);
				internal.setRawScanReaderOpenedObserverForTesting(() -> {
					readerOpened.countDown();
					awaitLatch(allowNativeReaderToReturn);
				});
				CompletableFuture<? extends java.util.List<?>> scan = internal
						.scanRawAsyncInternal(columnId, 0, 1)
						.collectList()
						.toFuture();
				try {
					assertTrue(readerOpened.await(10, TimeUnit.SECONDS),
							"raw-scan reader did not open on cancellation attempt " + attempt);
					Set<Path> allPinnedFiles = internal.getRawScanPinnedFilesForTesting();
					assertEquals(GENERATIONS, allPinnedFiles.size(),
							"the real multi-SST fixture must pin every captured generation");
					Set<Path> scanDirectories = scanDirectories(allPinnedFiles);
					assertEquals(1, scanDirectories.size());
					assertEquals(GENERATIONS, rawScanPinnedFilesGauge(internal));
					assertTrue(rawScanPinnedBytesGauge(internal) > 0.0d);
					assertTrue(scan.cancel(true));

					Set<Path> claimedPins = awaitExistingRawScanPins(internal, Duration.ofSeconds(5));
					assertFalse(claimedPins.isEmpty(),
							"active native readers must retain their pin after outer stream cancellation");
					assertTrue(claimedPins.stream().allMatch(Files::exists));
					assertTrue(scanDirectories.stream().allMatch(Files::isDirectory),
							"a scan directory must outlive its active claimed pins");
					assertTrue(internal.getDb().get().isOwningHandle(),
							"database handle must remain owned while cancelled native readers drain");

					allowNativeReaderToReturn.countDown();
					awaitRawScanDrained(internal, allPinnedFiles, scanDirectories, Duration.ofSeconds(10));
					assertTrue(api.estimateNumKeys(columnId) > 0L,
							"database must remain usable after active raw-scan cancellation");
				} finally {
					internal.setRawScanReaderOpenedObserverForTesting(null);
					allowNativeReaderToReturn.countDown();
					if (!scan.isDone()) {
						scan.cancel(true);
					}
				}
			}
		}
	}

	@Test
	void oneInnerFailurePreservesCauseAndDeletesDirectoriesAfterActiveSiblingsDrain(@TempDir Path tempDir)
			throws Exception {
		Path configFile = tempDir.resolve("scan-raw-sibling-failure.conf");
		Files.writeString(configFile, """
				database: {
				  parallelism: { workload: { raw-scan-file-concurrency: 4 } }
				  global: {
				    ingest-behind: false
				    optimistic: false
				    disable-auto-compactions: true
				    disable-write-slowdown: true
				  }
				}
				""");
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"),
				"scan-raw-sibling-failure",
				configFile)) {
			RocksDBSyncAPI api = connection.getSyncApi(RequestContext.batch());
			EmbeddedDB internal = connection.getInternalDB();
			long columnId = api.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			writeOverlappingSsts(api, columnId);
			assertEquals(GENERATIONS, liveColumnSsts(internal).size());

			var readersOpened = new AtomicInteger();
			var injectedFailure = new CountDownLatch(1);
			var allowActiveSiblingsToReturn = new CountDownLatch(1);
			internal.setRawScanReaderOpenedObserverForTesting(() -> {
				int readerNumber = readersOpened.incrementAndGet();
				if (readerNumber == GENERATIONS) {
					injectedFailure.countDown();
					throw new IllegalStateException("synthetic inner raw-scan failure");
				}
				awaitLatch(allowActiveSiblingsToReturn);
			});
			CompletableFuture<? extends java.util.List<?>> scan = internal
					.scanRawAsyncInternal(columnId, 0, 1)
					.collectList()
					.toFuture();
			Set<Path> pinnedFiles = Set.of();
			Set<Path> scanDirectories = Set.of();
			try {
				assertTrue(injectedFailure.await(10, TimeUnit.SECONDS),
						"four real SST readers did not enter the failure barrier");
				pinnedFiles = internal.getRawScanPinnedFilesForTesting();
				scanDirectories = scanDirectories(pinnedFiles);
				assertEquals(1, scanDirectories.size());

				ExecutionException terminal = assertThrows(ExecutionException.class,
						() -> scan.get(10, TimeUnit.SECONDS));
				assertEquals("synthetic inner raw-scan failure", terminal.getCause().getMessage(),
						"eager outer cleanup must not replace the first inner terminal cause");
				assertTrue(scanDirectories.stream().allMatch(Files::isDirectory),
						"the directory must remain while cancelled sibling readers still own pins");
			} finally {
				internal.setRawScanReaderOpenedObserverForTesting(null);
				allowActiveSiblingsToReturn.countDown();
				if (!scan.isDone()) {
					scan.cancel(true);
				}
			}
			awaitRawScanDrained(internal, pinnedFiles, scanDirectories, Duration.ofSeconds(10));
			assertTrue(api.estimateNumKeys(columnId) > 0L);
		}
	}

	@Test
	void hardLinkFailureDoesNotCopyOrLeaveFileDeletionDisabled(@TempDir Path tempDir) throws Exception {
		Path configFile = tempDir.resolve("scan-raw-link-failure.conf");
		Files.writeString(configFile, """
				database: { global: {
				  ingest-behind: false
				  optimistic: false
				  disable-auto-compactions: true
				  disable-write-slowdown: true
				} }
				""");
		try (var db = new FailSecondHardLinkEmbeddedDB(tempDir.resolve("db"),
				"scan-raw-link-failure",
				configFile)) {
			long columnId = db.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			writeOverlappingSsts(db, columnId);

			RuntimeException failure = assertThrows(RuntimeException.class,
					() -> db.scanRawAsyncInternal(columnId, 0, 1).collectList().block());
			assertTrue(hasCauseMessage(failure, "synthetic hard-link failure"),
					"The raw scan must expose the hard-link failure instead of copying the SST: " + failure);
			assertTrue(db.getRawScanPinnedFilesForTesting().isEmpty(),
					"Partial pin acquisition must release every hard link");
			assertFileDeletionsEnabled(db);
		}
	}

	@Test
	void pinAcquisitionDeadlineFailsClosed(@TempDir Path tempDir) throws Exception {
		try (var db = new ExpiredPinDeadlineEmbeddedDB(tempDir.resolve("db"), "scan-raw-pin-timeout")) {
			long columnId = db.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			db.put(0, columnId, key(1), value(0, 1), RequestType.none());
			db.flush();

			RuntimeException failure = assertThrows(RuntimeException.class,
					() -> db.scanRawAsyncInternal(columnId, 0, 1).collectList().block());
			assertTrue(hasCauseMessage(failure, "exceeded the configured maximum duration"));
			assertTrue(db.getRawScanPinnedFilesForTesting().isEmpty());
			assertFileDeletionsEnabled(db);
		}
	}

	@Test
	void nextProcessRemovesOrphanedPinsDuringStartup(@TempDir Path tempDir) throws Exception {
		Path databasePath = tempDir.resolve("db");
		String databaseName = "scan-raw-orphan-cleanup";
		Path firstVolume = tempDir.resolve("sst-volume-a");
		Path secondVolume = tempDir.resolve("sst-volume-b");
		Path configFile = tempDir.resolve("scan-raw-orphan-cleanup.conf");
		Files.writeString(configFile, """
				database: { global: {
				  ingest-behind: false
				  optimistic: false
				  fallback-column-options: { volumes: [
				    { volume-path: "%s", target-size: "1TiB" }
				    { volume-path: "%s", target-size: "1TiB" }
				  ] }
				} }
				""".formatted(firstVolume, secondVolume));
		Path databasePinRoot;
		Path sourceSst;
		try (var connection = new EmbeddedConnection(databasePath, databaseName, configFile)) {
			RocksDBSyncAPI api = connection.getSyncApi(RequestContext.batch());
			long columnId = api.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			api.put(0, columnId, key(1), value(0, 1), RequestType.none());
			api.flush();
			connection.getInternalDB().scanRawAsyncInternal(columnId, 0, 1).blockLast();

			sourceSst = liveColumnSsts(connection.getInternalDB()).iterator().next();
			Path pinRoot = sourceSst.getParent().resolve(".rockserver-raw-scan-pins");
			try (var roots = Files.list(pinRoot)) {
				databasePinRoot = roots.findFirst().orElseThrow();
			}
		}

		Path orphanDirectory = databasePinRoot.resolve("orphaned-process");
		Files.createDirectories(orphanDirectory);
		Path orphanPin = orphanDirectory.resolve(sourceSst.getFileName());
		Files.createLink(orphanPin, sourceSst);
		assertTrue(Files.exists(orphanPin));

		Path unusedConfiguredVolume = sourceSst.getParent().equals(firstVolume)
				? secondVolume
				: firstVolume;
		Path unusedVolumeDatabasePinRoot = unusedConfiguredVolume
				.resolve(".rockserver-raw-scan-pins")
				.resolve(databasePinRoot.getFileName());
		Path unusedVolumeOrphanDirectory = unusedVolumeDatabasePinRoot.resolve("orphaned-process");
		Files.createDirectories(unusedVolumeOrphanDirectory);
		Path unusedVolumeOrphanPin = unusedVolumeOrphanDirectory.resolve(sourceSst.getFileName());
		Files.createLink(unusedVolumeOrphanPin, sourceSst);

		Path otherDatabasePin = sourceSst.getParent()
				.resolve(".rockserver-raw-scan-pins")
				.resolve("other-database")
				.resolve(sourceSst.getFileName());
		Files.createDirectories(otherDatabasePin.getParent());
		Files.createLink(otherDatabasePin, sourceSst);

		try (var connection = new EmbeddedConnection(databasePath, databaseName, configFile)) {
			EmbeddedDB internal = connection.getInternalDB();
			assertFalse(Files.exists(orphanPin),
					"Restart must remove hard links left by an interrupted raw scan before another scan");
			assertFalse(Files.exists(databasePinRoot),
					"Restart recovery must remove the interrupted process's complete pin namespace");
			assertFalse(Files.exists(unusedVolumeOrphanPin),
					"Restart must inspect configured SST volumes even when they contain no live SST");
			assertFalse(Files.exists(unusedVolumeDatabasePinRoot));
			assertTrue(Files.exists(otherDatabasePin),
					"Recovery for one database must not delete another database's pin namespace");
			assertTrue(internal.getRawScanPinnedFilesForTesting().isEmpty());
			assertFileDeletionsEnabled(internal);
		}
	}

	@Test
	void rejectedAdmissionDoesNotEndAnUnrelatedShutdownOperation(@TempDir Path tempDir) throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "scan-raw-shutdown", null)) {
			long columnId = connection.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).createColumn(COLUMN_NAME,
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

	private static void assertHardLinks(Set<Path> originals, Set<Path> pins) throws IOException {
		for (Path original : originals) {
			Path pin = pins.stream()
					.filter(candidate -> candidate.getFileName().equals(original.getFileName()))
					.findFirst()
					.orElseThrow(() -> new AssertionError("Missing pin for " + original));
			assertTrue(Files.isSameFile(original, pin),
					() -> "Raw scan must hard-link rather than copy " + original);
		}
	}

	private static void assertFileDeletionsEnabled(EmbeddedDB db) throws org.rocksdb.RocksDBException {
		assertEquals(1L,
				db.getDb().get().getLongProperty("rocksdb.is-file-deletions-enabled"),
				"A raw scan must not keep RocksDB file and WAL deletion disabled");
	}

	private static boolean hasCauseMessage(Throwable failure, String expected) {
		for (Throwable current = failure; current != null; current = current.getCause()) {
			if (current.getMessage() != null && current.getMessage().contains(expected)) {
				return true;
			}
		}
		return false;
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

	private static Set<Path> scanDirectories(Set<Path> pinnedFiles) {
		return pinnedFiles.stream()
				.map(Path::getParent)
				.collect(java.util.stream.Collectors.toUnmodifiableSet());
	}

	private static double rawScanPinnedFilesGauge(EmbeddedDB db) {
		return db.getMetricsRegistry().get("rockserver.raw.scan.pinned.files").gauge().value();
	}

	private static double rawScanPinnedBytesGauge(EmbeddedDB db) {
		return db.getMetricsRegistry().get("rockserver.raw.scan.pinned.bytes").gauge().value();
	}

	private static void awaitRawScanDrained(EmbeddedDB db,
			Set<Path> pinnedFiles,
			Set<Path> scanDirectories,
			Duration timeout) throws InterruptedException {
		long deadline = System.nanoTime() + timeout.toNanos();
		do {
			if (pinnedFiles.stream().noneMatch(Files::exists)
					&& scanDirectories.stream().noneMatch(Files::exists)
					&& db.getRawScanPinnedFilesForTesting().isEmpty()
					&& rawScanPinnedFilesGauge(db) == 0.0d
					&& rawScanPinnedBytesGauge(db) == 0.0d
					&& db.getPendingOpsCount() == 0L) {
				return;
			}
			Thread.sleep(20L);
		} while (System.nanoTime() < deadline);
		assertTrue(pinnedFiles.stream().noneMatch(Files::exists),
				"raw-scan hard links did not drain: " + pinnedFiles);
		assertTrue(scanDirectories.stream().noneMatch(Files::exists),
				"raw-scan UUID directories leaked after their last pin drained: " + scanDirectories);
		assertTrue(db.getRawScanPinnedFilesForTesting().isEmpty());
		assertEquals(0.0d, rawScanPinnedFilesGauge(db));
		assertEquals(0.0d, rawScanPinnedBytesGauge(db));
		assertEquals(0L, db.getPendingOpsCount());
	}

	private static Set<Path> awaitExistingRawScanPins(EmbeddedDB db, Duration timeout)
			throws InterruptedException {
		long deadline = System.nanoTime() + timeout.toNanos();
		do {
			Set<Path> pins = db.getRawScanPinnedFilesForTesting();
			if (!pins.isEmpty() && pins.stream().allMatch(Files::exists)) {
				return pins;
			}
			Thread.sleep(10L);
		} while (System.nanoTime() < deadline);
		Set<Path> pins = db.getRawScanPinnedFilesForTesting();
		assertFalse(pins.isEmpty(), "active native readers did not retain any raw-scan pin");
		assertTrue(pins.stream().allMatch(Files::exists),
				"active pin accounting did not converge to files still owned by readers: " + pins);
		return pins;
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

	private static boolean awaitCondition(BooleanSupplier condition, Duration timeout) throws InterruptedException {
		long deadline = System.nanoTime() + timeout.toNanos();
		do {
			if (condition.getAsBoolean()) {
				return true;
			}
			Thread.sleep(10L);
		} while (System.nanoTime() < deadline);
		return condition.getAsBoolean();
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

	private static final class FailSecondHardLinkEmbeddedDB extends EmbeddedDB {
		private final AtomicInteger hardLinkAttempts = new AtomicInteger();

		private FailSecondHardLinkEmbeddedDB(Path path, String name, Path configPath) throws IOException {
			super(path, name, configPath);
		}

		@Override
		protected void createRawScanHardLink(Path source, Path target) throws IOException {
			if (hardLinkAttempts.incrementAndGet() == 2) {
				throw new IOException("synthetic hard-link failure");
			}
			super.createRawScanHardLink(source, target);
		}
	}

	private static final class ExpiredPinDeadlineEmbeddedDB extends EmbeddedDB {

		private ExpiredPinDeadlineEmbeddedDB(Path path, String name) throws IOException {
			super(path, name, null);
		}

		@Override
		protected long rawScanPinMaxDurationNanos() {
			return 0L;
		}
	}
}
