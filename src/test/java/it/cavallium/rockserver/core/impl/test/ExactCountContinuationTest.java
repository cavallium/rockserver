package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(60)
class ExactCountContinuationTest {

	private static final int INITIAL_ENTRIES = 4_200;

	@TempDir
	Path tempDir;

	@Test
	void countUsesOneRetainedSnapshotAcrossYieldedQuantums() throws Exception {
		try (var connection = populatedConnection("snapshot-consistency")) {
			long columnId = connection.getSyncApi(RequestContext.analytical()).getColumnId("entries");
			var firstQuantum = new CountDownLatch(1);
			var release = new CountDownLatch(1);
			var first = new AtomicBoolean(true);
			connection.getInternalDB().setRangeCountChunkObserverForTesting(() -> {
				if (first.compareAndSet(true, false)) {
					firstQuantum.countDown();
					await(release);
				}
			});
			try {
				var count = connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount());
				assertTrue(firstQuantum.await(10, TimeUnit.SECONDS));
				assertEquals(1, connection.getInternalDB().getRetainedRangeSnapshotCount());
				var ingest = connection.getSyncApi(RequestContext.ingest());
				for (int i = INITIAL_ENTRIES; i < INITIAL_ENTRIES + 32; i++) {
					ingest.put(0, columnId, key(i), value(i), RequestType.none());
				}
				release.countDown();
				assertEquals(INITIAL_ENTRIES, count.get(10, TimeUnit.SECONDS));
				assertTrue(awaitCondition(
						() -> connection.getInternalDB().getRetainedRangeSnapshotCount() == 0, 5_000));
			} finally {
				release.countDown();
				connection.getInternalDB().setRangeCountChunkObserverForTesting(null);
			}
		}
	}

	@Test
	void cancellationClosesRetainedIteratorAndSnapshotAfterRunningQuantumFinishes() throws Exception {
		try (var connection = populatedConnection("snapshot-cancellation")) {
			long columnId = connection.getSyncApi(RequestContext.analytical()).getColumnId("entries");
			var firstQuantum = new CountDownLatch(1);
			var release = new CountDownLatch(1);
			var first = new AtomicBoolean(true);
			connection.getInternalDB().setRangeCountChunkObserverForTesting(() -> {
				if (first.compareAndSet(true, false)) {
					firstQuantum.countDown();
					await(release);
				}
			});
			try {
				var count = connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount());
				assertTrue(firstQuantum.await(10, TimeUnit.SECONDS));
				assertEquals(1, connection.getInternalDB().getRetainedRangeSnapshotCount());
				assertTrue(count.cancel(false));
				release.countDown();
				assertTrue(awaitCondition(
						() -> connection.getInternalDB().getRetainedRangeSnapshotCount() == 0, 2_000));
				assertEquals(0, connection.getInternalDB().getActiveRangeCursorCount());
				assertEquals(0, connection.getInternalDB().getRetainedRangePermitCount());
			} finally {
				release.countDown();
				connection.getInternalDB().setRangeCountChunkObserverForTesting(null);
			}
		}
	}

	@Test
	void configuredMaximumSnapshotAgeClosesAStalledCount() throws Exception {
		try (var connection = populatedConnection("snapshot-max-age", """
				database.parallelism.workload.retained-snapshot-maximum-age = PT0.5S
				database.parallelism.workload.range-quantum-max-items = 64
				""")) {
			long columnId = connection.getSyncApi(RequestContext.analytical()).getColumnId("entries");
			var firstContinuation = new CountDownLatch(1);
			var release = new CountDownLatch(1);
			var first = new AtomicBoolean(true);
			connection.getInternalDB().setRangeContinuationObserverForTesting(() -> {
				if (first.compareAndSet(true, false)) {
					firstContinuation.countDown();
					await(release);
				}
			});
			try {
				var count = connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount());
				assertTrue(firstContinuation.await(10, TimeUnit.SECONDS));
				assertEquals(1, connection.getInternalDB().getRetainedRangeSnapshotCount());
				assertTrue(awaitCondition(
						() -> connection.getInternalDB().getRetainedRangeSnapshotCount() == 0, 2_000));
				release.countDown();
				assertReadDeadline(count);
				assertRetainedResourcesClosed(connection);
			} finally {
				release.countDown();
				connection.getInternalDB().setRangeContinuationObserverForTesting(null);
			}
		}
	}

	@Test
	void contextDeadlineExpiresAStalledUnlimitedCount() throws Exception {
		try (var connection = populatedConnection("snapshot-context-deadline", """
				database.parallelism.workload.retained-snapshot-maximum-age = PT10S
				database.parallelism.workload.range-quantum-max-items = 64
				""")) {
			long columnId = connection.getSyncApi(RequestContext.analytical()).getColumnId("entries");
			var firstContinuation = new CountDownLatch(1);
			var release = new CountDownLatch(1);
			var first = new AtomicBoolean(true);
			connection.getInternalDB().setRangeContinuationObserverForTesting(() -> {
				if (first.compareAndSet(true, false)) {
					firstContinuation.countDown();
					await(release);
				}
			});
			try {
				var deadline = RequestContext.analytical(java.time.Duration.ofMillis(500));
				var count = connection.getAsyncApi(deadline).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount());
				assertTrue(firstContinuation.await(10, TimeUnit.SECONDS));
				assertTrue(awaitCondition(
						() -> connection.getInternalDB().getRetainedRangeSnapshotCount() == 0, 2_000));
				release.countDown();
				assertReadDeadline(count);
				assertRetainedResourcesClosed(connection);
			} finally {
				release.countDown();
				connection.getInternalDB().setRangeContinuationObserverForTesting(null);
			}
		}
	}

	@Test
	void negativeExactCountTimeoutIsRejectedAndZeroIsAlreadyExpired() throws Exception {
		try (var connection = populatedConnection("snapshot-invalid-timeout")) {
			long columnId = connection.getSyncApi(RequestContext.analytical()).getColumnId("entries");
			var negative = assertThrows(RocksDBException.class,
					() -> connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
							0, columnId, null, null, false, RequestType.entriesCount()));
			assertEquals(RocksDBErrorType.PUT_INVALID_REQUEST, negative.getErrorUniqueId());

			var zero = connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
					0, columnId, null, null, false, RequestType.entriesCount());
			assertReadDeadline(zero);
			assertRetainedResourcesClosed(connection);
		}
	}

	@Test
	void fifoLimiterKeepsHundredsOfWaitersOutOfTheSchedulerAndSkipsCancelledWaiters() throws Exception {
		final int waiterCount = 240;
		try (var connection = populatedConnection("snapshot-fifo", """
				database.parallelism.workload.retained-analytical-snapshots = 1
				database.parallelism.workload.retained-snapshot-maximum-age = PT30S
				database.parallelism.workload.range-quantum-max-items = 1
				database.parallelism.workload.range-quantum-max-bytes = 1MiB
				database.parallelism.workload.range-quantum-max-duration = PT1S
				""", 2)) {
			long columnId = connection.getSyncApi(RequestContext.analytical()).getColumnId("entries");
			var firstContinuation = new CountDownLatch(1);
			var release = new CountDownLatch(1);
			var first = new AtomicBoolean(true);
			var grantedTickets = new ArrayList<Long>();
			connection.getInternalDB().setRetainedQueryPermitGrantedObserverForTesting(ticket -> {
				synchronized (grantedTickets) {
					grantedTickets.add(ticket);
				}
			});
			connection.getInternalDB().setRangeContinuationObserverForTesting(() -> {
				if (first.compareAndSet(true, false)) {
					firstContinuation.countDown();
					await(release);
				}
			});
			var firstCount = connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
					0, columnId, null, null, false, RequestType.entriesCount());
			assertTrue(firstContinuation.await(10, TimeUnit.SECONDS));

			var waiters = new ArrayList<CompletableFuture<Long>>(waiterCount);
			for (int i = 0; i < waiterCount; i++) {
				waiters.add(connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount()));
			}
			assertTrue(awaitCondition(
					() -> connection.getInternalDB().getRetainedRangeWaiterCount() == waiterCount, 5_000));
			assertTrue(awaitCondition(() -> {
				var snapshot = connection.getScheduler().poolSnapshot(RWScheduler.Pool.READ);
				return snapshot.queuedTasks() == 0 && snapshot.activeTasks() == 0;
			}, 5_000));
			var readPool = connection.getScheduler().poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(0, readPool.queuedTasks(), "permit waiters must not occupy the workload queue");
			assertEquals(0, readPool.activeTasks(), "continuation handoff must not retain a read worker");

			var expectedTickets = new ArrayList<Long>();
			expectedTickets.add(0L);
			for (int i = 0; i < waiters.size(); i++) {
				if (i % 4 == 0) {
					assertTrue(waiters.get(i).cancel(false));
				} else {
					expectedTickets.add((long) i + 1L);
				}
			}
			release.countDown();
			assertEquals(2L, firstCount.get(10, TimeUnit.SECONDS));
			for (var waiter : waiters) {
				if (!waiter.isCancelled()) {
					assertEquals(2L, waiter.get(20, TimeUnit.SECONDS));
				}
			}
			synchronized (grantedTickets) {
				assertEquals(expectedTickets, List.copyOf(grantedTickets));
			}
			assertRetainedResourcesClosed(connection);
		}
	}

	@Test
	void permitWaiterExpiresWithoutEnteringTheScheduler() throws Exception {
		try (var connection = populatedConnection("snapshot-waiter-deadline", """
				database.parallelism.workload.retained-analytical-snapshots = 1
				database.parallelism.workload.retained-snapshot-maximum-age = PT30S
				database.parallelism.workload.range-quantum-max-items = 1
				""", 2)) {
			long columnId = connection.getSyncApi(RequestContext.analytical()).getColumnId("entries");
			var firstContinuation = new CountDownLatch(1);
			var release = new CountDownLatch(1);
			var first = new AtomicBoolean(true);
			connection.getInternalDB().setRangeContinuationObserverForTesting(() -> {
				if (first.compareAndSet(true, false)) {
					firstContinuation.countDown();
					await(release);
				}
			});
			try {
				var active = connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount());
				assertTrue(firstContinuation.await(10, TimeUnit.SECONDS));
				long acceptedBefore = connection.getScheduler()
						.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks();
				var waiting = connection.getAsyncApi(
						RequestContext.analytical(java.time.Duration.ofMillis(500))).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount());
				assertTrue(awaitCondition(
						() -> connection.getInternalDB().getRetainedRangeWaiterCount() == 1, 2_000));
				assertReadDeadline(waiting);
				assertEquals(0, connection.getInternalDB().getRetainedRangeWaiterCount());
				assertEquals(acceptedBefore, connection.getScheduler()
						.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks());

				release.countDown();
				assertEquals(2L, active.get(10, TimeUnit.SECONDS));
				assertRetainedResourcesClosed(connection);
			} finally {
				release.countDown();
				connection.getInternalDB().setRangeContinuationObserverForTesting(null);
			}
		}
	}

	@Test
	void cancellationAfterPermitGrantRemovesQueuedCursorConstruction() throws Exception {
		try (var connection = populatedConnection("snapshot-granted-cancellation", """
				database.parallelism.workload.retained-analytical-snapshots = 1
				database.parallelism.workload.retained-snapshot-maximum-age = PT30S
				""", 2)) {
			long columnId = connection.getSyncApi(RequestContext.analytical()).getColumnId("entries");
			var analyticalWorkerEntered = new CountDownLatch(1);
			var releaseAnalyticalWorker = new CountDownLatch(1);
			connection.getScheduler().executor(WorkloadProfile.ANALYTICAL,
					OperationFamily.FULL_SCAN_AGGREGATE,
					Long.MAX_VALUE).execute(() -> {
				analyticalWorkerEntered.countDown();
				await(releaseAnalyticalWorker);
			});
			assertTrue(analyticalWorkerEntered.await(5, TimeUnit.SECONDS));
			try {
				var count = connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount());
				assertTrue(awaitCondition(() -> connection.getInternalDB().getRetainedRangePermitCount() == 1
						&& connection.getScheduler().poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 1,
						5_000));

				assertTrue(count.cancel(false));
				assertTrue(awaitCondition(() -> connection.getInternalDB().getRetainedRangePermitCount() == 0
						&& connection.getInternalDB().getRetainedRangeWaiterCount() == 0
						&& connection.getInternalDB().getActiveRangeCursorCount() == 0
						&& connection.getInternalDB().getRetainedRangeSnapshotCount() == 0
						&& connection.getScheduler().poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 0,
						5_000));
			} finally {
				releaseAnalyticalWorker.countDown();
			}
			assertRetainedResourcesClosed(connection);
		}
	}

	@Test
	void configuredCountQuantumMaximumIsAppliedAndConstructionFailureReleasesPermit() throws Exception {
		try (var connection = populatedConnection("snapshot-configured-quantum", """
				database.parallelism.workload.range-quantum-max-items = 7
				database.parallelism.workload.range-quantum-max-bytes = 1MiB
				database.parallelism.workload.range-quantum-max-duration = PT0.000001S
				""", 29)) {
			long columnId = connection.getSyncApi(RequestContext.analytical()).getColumnId("entries");
			var quantumSizes = new ArrayList<Long>();
			connection.getInternalDB().setRangeCountQuantumItemsObserverForTesting(quantumSizes::add);
			assertEquals(29L, connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
					0, columnId, null, null, false, RequestType.entriesCount())
					.get(10, TimeUnit.SECONDS));
			assertTrue(quantumSizes.size() > 5,
					"the configured duration must yield before the seven-item ceiling at least once");
			assertTrue(quantumSizes.stream().allMatch(size -> size > 0L && size <= 7L));

			var missingColumn = connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
					0, Long.MAX_VALUE, null, null, false, RequestType.entriesCount());
			assertThrows(ExecutionException.class, () -> missingColumn.get(10, TimeUnit.SECONDS));
			assertRetainedResourcesClosed(connection);
		}
	}

	@Test
	void latencyAndCdcProgressBetweenAnalyticalCountQuanta() throws Exception {
		try (var connection = populatedConnection("snapshot-inter-quantum-progress", """
				database.parallelism.workload.range-quantum-max-items = 1
				database.parallelism.workload.range-quantum-max-duration = PT1S
				""", 2)) {
			long columnId = connection.getSyncApi(RequestContext.analytical()).getColumnId("entries");
			var firstContinuation = new CountDownLatch(1);
			var release = new CountDownLatch(1);
			var first = new AtomicBoolean(true);
			connection.getInternalDB().setRangeContinuationObserverForTesting(() -> {
				if (first.compareAndSet(true, false)) {
					firstContinuation.countDown();
					await(release);
				}
			});
			try {
				var count = connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount());
				assertTrue(firstContinuation.await(10, TimeUnit.SECONDS));
				assertTrue(awaitCondition(() -> connection.getScheduler()
						.poolSnapshot(RWScheduler.Pool.READ).activeTasks() == 0, 5_000));
				assertEquals(0, connection.getScheduler().poolSnapshot(RWScheduler.Pool.READ).activeTasks());

				var latencyProgress = new CountDownLatch(1);
				var cdcProgress = new CountDownLatch(1);
				connection.getScheduler().executor(WorkloadProfile.LATENCY,
						OperationFamily.POINT_LOOKUP,
						connection.getScheduler().bindTimeoutNanos(TimeUnit.MILLISECONDS.toNanos(5_000L)))
						.execute(latencyProgress::countDown);
				connection.getScheduler().executor(WorkloadProfile.CDC,
						OperationFamily.WAL_PAGE,
						Long.MAX_VALUE).execute(cdcProgress::countDown);
				assertTrue(latencyProgress.await(5, TimeUnit.SECONDS));
				assertTrue(cdcProgress.await(5, TimeUnit.SECONDS));

				release.countDown();
				assertEquals(2L, count.get(10, TimeUnit.SECONDS));
				assertRetainedResourcesClosed(connection);
			} finally {
				release.countDown();
				connection.getInternalDB().setRangeContinuationObserverForTesting(null);
			}
		}
	}

	@Test
	void shutdownDrainsActiveRetainedQueryAndPermitWaiters() throws Exception {
		var connection = populatedConnection("snapshot-shutdown", """
				database.parallelism.workload.retained-analytical-snapshots = 1
				database.parallelism.workload.retained-snapshot-maximum-age = PT30S
				database.parallelism.workload.range-quantum-max-items = 1
				""", 2);
		var release = new CountDownLatch(1);
		boolean closed = false;
		try {
			long columnId = connection.getSyncApi(RequestContext.analytical()).getColumnId("entries");
			var firstContinuation = new CountDownLatch(1);
			var first = new AtomicBoolean(true);
			connection.getInternalDB().setRangeContinuationObserverForTesting(() -> {
				if (first.compareAndSet(true, false)) {
					firstContinuation.countDown();
					await(release);
				}
			});
			connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
					0, columnId, null, null, false, RequestType.entriesCount());
			assertTrue(firstContinuation.await(10, TimeUnit.SECONDS));
			for (int i = 0; i < 8; i++) {
				connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount());
			}
			assertTrue(awaitCondition(
					() -> connection.getInternalDB().getRetainedRangeWaiterCount() == 8, 5_000));

			connection.closeTesting();
			closed = true;
			assertRetainedResourcesClosed(connection);
		} finally {
			release.countDown();
			if (!closed) {
				connection.closeTesting();
			}
		}
	}

	@Test
	void forcedShutdownDrainsActiveRetainedQueryAndPermitWaiter() throws Exception {
		String timeoutProperty = "it.cavallium.rockserver.db.shutdown-pending-ops-timeout-ms";
		String previousTimeout = System.getProperty(timeoutProperty);
		System.setProperty(timeoutProperty, "1");
		var releaseContinuation = new CountDownLatch(1);
		var releaseOperation = new CountDownLatch(1);
		var connection = populatedConnection("snapshot-forced-shutdown", """
				database.parallelism.workload.retained-analytical-snapshots = 1
				database.parallelism.workload.retained-snapshot-maximum-age = PT30S
				database.parallelism.workload.range-quantum-max-items = 1
				""", 2);
		CompletableFuture<Void> close = null;
		boolean closed = false;
		try {
			long columnId = connection.getSyncApi(RequestContext.analytical()).getColumnId("entries");
			var firstContinuation = new CountDownLatch(1);
			var first = new AtomicBoolean(true);
			connection.getInternalDB().setRangeContinuationObserverForTesting(() -> {
				if (first.compareAndSet(true, false)) {
					firstContinuation.countDown();
					await(releaseContinuation);
				}
			});
			connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
					0, columnId, null, null, false, RequestType.entriesCount());
			assertTrue(firstContinuation.await(10, TimeUnit.SECONDS));
			connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
					0, columnId, null, null, false, RequestType.entriesCount());
			assertTrue(awaitCondition(
					() -> connection.getInternalDB().getRetainedRangeWaiterCount() == 1, 5_000));

			var operationEntered = new CountDownLatch(1);
			var firstColumnUse = new AtomicBoolean(true);
			connection.getInternalDB().setColumnUseAcquiredObserverForTesting(ignored -> {
				if (firstColumnUse.compareAndSet(true, false)) {
					operationEntered.countDown();
					await(releaseOperation);
				}
			});
			var blockingOperation = CompletableFuture.supplyAsync(() ->
					connection.getSyncApi(RequestContext.batch()).estimateNumKeys(columnId));
			assertTrue(operationEntered.await(5, TimeUnit.SECONDS));

			var forced = new CountDownLatch(1);
			connection.getInternalDB().setForcedShutdownObserverForTesting(forced::countDown);
			close = CompletableFuture.runAsync(() -> {
				try {
					connection.closeTesting();
				} catch (Exception error) {
					throw new CompletionException(error);
				}
			});
			assertTrue(forced.await(5, TimeUnit.SECONDS),
					"shutdown must enter forced cleanup while the admitted operation is held");
			assertRetainedResourcesClosed(connection);

			releaseOperation.countDown();
			blockingOperation.get(5, TimeUnit.SECONDS);
			close.get(5, TimeUnit.SECONDS);
			closed = true;
			assertRetainedResourcesClosed(connection);
		} finally {
			releaseOperation.countDown();
			releaseContinuation.countDown();
			connection.getInternalDB().setRangeContinuationObserverForTesting(null);
			connection.getInternalDB().setColumnUseAcquiredObserverForTesting(null);
			connection.getInternalDB().setForcedShutdownObserverForTesting(null);
			if (!closed && close == null) {
				connection.closeTesting();
			} else if (!closed && !close.isDone()) {
				close.get(5, TimeUnit.SECONDS);
			}
			if (previousTimeout == null) {
				System.clearProperty(timeoutProperty);
			} else {
				System.setProperty(timeoutProperty, previousTimeout);
			}
		}
	}

	private EmbeddedConnection populatedConnection(String name) throws Exception {
		return populatedConnection(name, "", INITIAL_ENTRIES);
	}

	private EmbeddedConnection populatedConnection(String name, String configuration) throws Exception {
		return populatedConnection(name, configuration, INITIAL_ENTRIES);
	}

	private EmbeddedConnection populatedConnection(String name, String configuration, int entries) throws Exception {
		Path config = null;
		if (!configuration.isBlank()) {
			config = Files.writeString(tempDir.resolve(name + ".conf"), configuration);
		}
		var connection = new EmbeddedConnection(tempDir.resolve(name), name, config);
		var batch = connection.getSyncApi(RequestContext.batch());
		var ingest = connection.getSyncApi(RequestContext.ingest());
		long columnId = batch.createColumn("entries",
				ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
		for (int i = 0; i < entries; i++) {
			ingest.put(0, columnId, key(i), value(i), RequestType.none());
		}
		return connection;
	}

	private static Keys key(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Buf value(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}

	private static void await(CountDownLatch latch) {
		try {
			latch.await();
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private static boolean awaitCondition(java.util.function.BooleanSupplier condition,
			long timeoutMillis) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(10L);
		}
		return condition.getAsBoolean();
	}

	private static void assertReadDeadline(CompletableFuture<?> future) throws Exception {
		var failure = assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
		var rocks = assertInstanceOf(RocksDBException.class, failure.getCause());
		assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, rocks.getErrorUniqueId());
	}

	private static void assertRetainedResourcesClosed(EmbeddedConnection connection) throws InterruptedException {
		assertTrue(awaitCondition(() -> connection.getInternalDB().getActiveRangeCursorCount() == 0
				&& connection.getInternalDB().getRetainedRangeSnapshotCount() == 0
				&& connection.getInternalDB().getRetainedRangePermitCount() == 0
				&& connection.getInternalDB().getRetainedRangeWaiterCount() == 0, 5_000));
	}
}
