package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@Timeout(60)
class RetainedRangeCooperativeTest {

	@TempDir
	Path tempDir;

	@Test
	void batchCountUsesOneLogicalTaskAndYieldsAfterOneNativeQuantum() throws Exception {
		String databaseName = "retained-count-batch";
		try (var connection = populatedConnection(databaseName, 128, """
				database.parallelism.read = 3
				database.parallelism.write = 3
				database.parallelism.workload.competing-batch-read-maximum-active = 3
				database.parallelism.workload.range-quantum-max-items = 8
				database.parallelism.workload.range-quantum-max-bytes = 1MiB
				database.parallelism.workload.range-quantum-max-duration = PT1S
				""")) {
			long columnId = connection.getSyncApi(RequestContext.batch()).getColumnId("entries");
			var scheduler = connection.getScheduler();
			var releaseBlockers = new CountDownLatch(1);
			var blockersStarted = new CountDownLatch(2);
			occupyReadWorkers(scheduler.executor(it.cavallium.rockserver.core.common.WorkloadProfile.BATCH, it.cavallium.rockserver.core.common.OperationFamily.RANGE_PAGE, Long.MAX_VALUE), 2, blockersStarted, releaseBlockers);
			assertTrue(blockersStarted.await(5, TimeUnit.SECONDS));

			var firstChunk = new CountDownLatch(1);
			var releaseFirstChunk = new CountDownLatch(1);
			var foregroundRan = new CountDownLatch(1);
			var chunks = new AtomicInteger();
			var foregroundBeforeSecondChunk = new AtomicBoolean();
			var iteratorOpens = new AtomicInteger();
			connection.getInternalDB().setRangeIteratorOpenObserverForTesting(iteratorOpens::incrementAndGet);
			connection.getInternalDB().setRangeCountChunkObserverForTesting(() -> {
				int chunk = chunks.incrementAndGet();
				if (chunk == 1) {
					firstChunk.countDown();
					await(releaseFirstChunk);
				} else if (chunk == 2) {
					foregroundBeforeSecondChunk.set(foregroundRan.getCount() == 0L);
				}
			});

			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.FULL_SCAN_AGGREGATE,
					Long.MAX_VALUE);
			var quantumCounter = connection.getInternalDB().getMetricsRegistry()
					.get("rockserver.workload.quantums")
					.tags("database", databaseName,
							"resource", "read",
							"profile", "batch",
							"operation", "full_scan_aggregate")
					.counter();
			double quantumsBefore = quantumCounter.count();
			long acceptedBefore = scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks();
			try {
				var count = connection.getAsyncApi(RequestContext.batch()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount());
				assertTrue(firstChunk.await(5, TimeUnit.SECONDS));
				assertEquals(1, connection.getInternalDB().getActiveRangeCursorCount());
				assertEquals(1, connection.getInternalDB().getRetainedRangeSnapshotCount());
				assertEquals(1, connection.getInternalDB().getRetainedRangePermitCount());

				scheduler.executor(WorkloadProfile.LATENCY,
						OperationFamily.POINT_LOOKUP,
						System.currentTimeMillis() + 5_000L).execute(foregroundRan::countDown);
				releaseFirstChunk.countDown();
				assertTrue(foregroundRan.await(5, TimeUnit.SECONDS));
				assertEquals(128L, count.get(10, TimeUnit.SECONDS));

				assertTrue(foregroundBeforeSecondChunk.get(),
						"queued LATENCY work must run before a second count quantum");
				assertEquals(1, iteratorOpens.get());
				assertEquals(2L,
						scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks() - acceptedBefore,
						"one retained count plus one foreground task are two logical submissions");
				assertTrue(awaitCondition(() -> quantumCounter.count() - quantumsBefore >= 2.0d, 5_000),
						"contention must redispatch the same count task for a later scheduler quantum");
				assertRetainedResourcesDrained(connection);
			} finally {
				releaseFirstChunk.countDown();
				releaseBlockers.countDown();
				connection.getInternalDB().setRangeCountChunkObserverForTesting(null);
				connection.getInternalDB().setRangeIteratorOpenObserverForTesting(null);
			}
		}
	}

	@Test
	void batchStreamParksAndResumesOneLogicalSchedulerNode() throws Exception {
		String databaseName = "retained-stream-batch";
		try (var connection = populatedConnection(databaseName, 65, """
				database.parallelism.workload.range-quantum-max-items = 16
				database.parallelism.workload.range-quantum-max-bytes = 1MiB
				database.parallelism.workload.range-quantum-max-duration = PT1S
				""")) {
			long columnId = connection.getSyncApi(RequestContext.batch()).getColumnId("entries");
			var scheduler = connection.getScheduler();
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					Long.MAX_VALUE);
			var quantumCounter = connection.getInternalDB().getMetricsRegistry()
					.get("rockserver.workload.quantums")
					.tags("database", databaseName,
							"resource", "read",
							"profile", "batch",
							"operation", "range_page")
					.counter();
			double quantumsBefore = quantumCounter.count();
			long acceptedBefore = scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks();
			var iteratorOpens = new AtomicInteger();
			var chunkSizes = Collections.synchronizedList(new ArrayList<Integer>());
			connection.getInternalDB().setRangeIteratorOpenObserverForTesting(iteratorOpens::incrementAndGet);
			connection.getInternalDB().setRangeReadChunkSizeObserverForTesting(chunkSizes::add);

			var range = Flux.from(connection.getAsyncApi(RequestContext.batch()).getRangeAsync(
					0, columnId, null, null, false, RequestType.allInRange()));
			StepVerifier.create(range, 1)
					.assertNext(first -> assertEquals(key(0), first.keys()))
					.thenAwait(Duration.ofMillis(200))
					.then(() -> {
						assertEquals(1L,
								scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks() - acceptedBefore);
						assertEquals(0, scheduler.poolSnapshot(RWScheduler.Pool.READ).activeTasks());
						assertEquals(1, connection.getInternalDB().getActiveRangeCursorCount());
						assertEquals(1, connection.getInternalDB().getRetainedRangePermitCount());
						assertTrue(chunkSizes.size() <= 2,
								"one delivery-side prefetch may decode at most one extra chunk");
					})
					.thenRequest(Long.MAX_VALUE)
					.expectNextCount(64)
					.verifyComplete();

			assertEquals(1, iteratorOpens.get());
			assertTrue(chunkSizes.size() >= 5);
			assertTrue(chunkSizes.stream().allMatch(size -> size > 0 && size <= 16));
			assertTrue(awaitCondition(() -> quantumCounter.count() - quantumsBefore > 1.0d, 5_000));
			assertRetainedResourcesDrained(connection);
		}
	}

	@Test
	void shutdownWinsWhileSuccessfulRetainedRangeCleanupIsStillFallible() throws Exception {
		var cleanupEntered = new CountDownLatch(1);
		var releaseCleanup = new CountDownLatch(1);
		EmbeddedConnection connection = null;
		java.util.concurrent.CompletableFuture<Void> close = null;
		try {
			connection = populatedConnection("retained-count-shutdown-cleanup", 33, ""
			);
			var db = connection.getInternalDB();
			db.setRetainedRangeCleanupObserverForTesting(() -> {
				cleanupEntered.countDown();
				await(releaseCleanup);
			});
			long columnId = connection.getSyncApi(RequestContext.batch()).getColumnId("entries");
			var scheduler = connection.getScheduler();
			awaitReadPoolDrained(scheduler);
			var before = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			var count = connection.getAsyncApi(RequestContext.batch()).reduceRangeAsync(
					0, columnId, null, null, false, RequestType.entriesCount());
			assertTrue(cleanupEntered.await(5, TimeUnit.SECONDS));
			assertEquals(1, db.getActiveRangeCursorCount());
			assertEquals(1, db.getRetainedRangeSnapshotCount());
			assertEquals(1, db.getRetainedRangePermitCount());

			var connectionToClose = connection;
			close = java.util.concurrent.CompletableFuture.runAsync(() -> {
				try {
					connectionToClose.closeTesting();
				} catch (Exception failure) {
					throw new java.util.concurrent.CompletionException(failure);
				}
			});
			Thread.sleep(100L);
			assertFalse(close.isDone(),
					"shutdown advanced while the winning task still owned native range cleanup");

			releaseCleanup.countDown();
			var failure = assertThrows(ExecutionException.class, () -> count.get(5, TimeUnit.SECONDS));
			assertTrue(failure.getCause() instanceof java.util.concurrent.RejectedExecutionException);
			close.get(10, TimeUnit.SECONDS);
			assertRetainedResourcesDrained(connection);
			var after = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(1L, after.acceptedTasks() - before.acceptedTasks());
			assertEquals(0L, after.outcomes().get(RWScheduler.TerminalOutcome.RUN)
					- before.outcomes().get(RWScheduler.TerminalOutcome.RUN),
					"RUN cannot be selected until fallible native cleanup succeeds");
			assertEquals(1L, after.outcomes().get(RWScheduler.TerminalOutcome.SHUTDOWN)
					- before.outcomes().get(RWScheduler.TerminalOutcome.SHUTDOWN),
					"shutdown must win while successful completion is not yet prepared");
			assertEquals(0L, after.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION)
					- before.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION),
					"database shutdown must not be recorded as cooperative cancellation");
		} finally {
			releaseCleanup.countDown();
			if (connection != null) {
				connection.getInternalDB().setRetainedRangeCleanupObserverForTesting(null);
				if (close == null) {
					connection.closeTesting();
				} else {
					try {
						close.get(10, TimeUnit.SECONDS);
					} catch (Exception ignored) {
						// Preserve the primary assertion failure; the close future carries shutdown diagnostics.
					}
				}
			}
		}
	}

	@Test
	void retainedCleanupFailureIsASchedulerFailureAndDrainsResources() throws Exception {
		String databaseName = "retained-count-cleanup-failure";
		try (var connection = populatedConnection(databaseName, 33, "")) {
			var db = connection.getInternalDB();
			db.setRetainedRangeCleanupObserverForTesting(() -> {
				throw new IllegalStateException("synthetic retained cleanup failure");
			});
			long columnId = connection.getSyncApi(RequestContext.batch()).getColumnId("entries");
			var scheduler = connection.getScheduler();
			awaitReadPoolDrained(scheduler);
			var before = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			try {
				var count = connection.getAsyncApi(RequestContext.batch()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount());
				var failure = assertThrows(ExecutionException.class, () -> count.get(5, TimeUnit.SECONDS));
				assertTrue(failure.getCause() instanceof IllegalStateException);
				assertEquals("synthetic retained cleanup failure", failure.getCause().getMessage());
				assertRetainedResourcesDrained(connection);

				var after = scheduler.poolSnapshot(RWScheduler.Pool.READ);
				assertEquals(1L, after.acceptedTasks() - before.acceptedTasks());
				assertEquals(1L, after.outcomes().get(RWScheduler.TerminalOutcome.FAILURE)
						- before.outcomes().get(RWScheduler.TerminalOutcome.FAILURE));
				assertEquals(0L, after.outcomes().get(RWScheduler.TerminalOutcome.RUN)
						- before.outcomes().get(RWScheduler.TerminalOutcome.RUN));
			} finally {
				db.setRetainedRangeCleanupObserverForTesting(null);
			}
		}
	}

	@Test
	void retainedStreamCleanupFailureIsASchedulerFailureAndDrainsResources() throws Exception {
		String databaseName = "retained-stream-cleanup-failure";
		try (var connection = populatedConnection(databaseName, 33, "")) {
			var db = connection.getInternalDB();
			db.setRetainedRangeCleanupObserverForTesting(() -> {
				throw new IllegalStateException("synthetic retained stream cleanup failure");
			});
			long columnId = connection.getSyncApi(RequestContext.batch()).getColumnId("entries");
			var scheduler = connection.getScheduler();
			awaitReadPoolDrained(scheduler);
			var before = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			try {
				var range = Flux.from(connection.getAsyncApi(RequestContext.batch()).getRangeAsync(
						0, columnId, null, null, false, RequestType.allInRange()))
						.collectList()
						.toFuture();
				var failure = assertThrows(ExecutionException.class, () -> range.get(5, TimeUnit.SECONDS));
				assertTrue(failure.getCause() instanceof IllegalStateException);
				assertEquals("synthetic retained stream cleanup failure", failure.getCause().getMessage());
				assertRetainedResourcesDrained(connection);

				var after = scheduler.poolSnapshot(RWScheduler.Pool.READ);
				assertEquals(1L, after.acceptedTasks() - before.acceptedTasks());
				assertEquals(1L, after.outcomes().get(RWScheduler.TerminalOutcome.FAILURE)
						- before.outcomes().get(RWScheduler.TerminalOutcome.FAILURE));
				assertEquals(0L, after.outcomes().get(RWScheduler.TerminalOutcome.RUN)
						- before.outcomes().get(RWScheduler.TerminalOutcome.RUN));
			} finally {
				db.setRetainedRangeCleanupObserverForTesting(null);
			}
		}
	}

	@Test
	void retainedNativeOpenFailureIsASchedulerFailureAndDrainsResources() throws Exception {
		String databaseName = "retained-count-open-failure";
		try (var connection = populatedConnection(databaseName, 33, "")) {
			var db = connection.getInternalDB();
			db.setRangeIteratorOpenObserverForTesting(() -> {
				throw new IllegalStateException("synthetic retained iterator-open failure");
			});
			long columnId = connection.getSyncApi(RequestContext.batch()).getColumnId("entries");
			var scheduler = connection.getScheduler();
			awaitReadPoolDrained(scheduler);
			var before = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			try {
				var count = connection.getAsyncApi(RequestContext.batch()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount());
				var failure = assertThrows(ExecutionException.class, () -> count.get(5, TimeUnit.SECONDS));
				assertTrue(failure.getCause() instanceof IllegalStateException);
				assertEquals("synthetic retained iterator-open failure", failure.getCause().getMessage());
				assertRetainedResourcesDrained(connection);

				var after = scheduler.poolSnapshot(RWScheduler.Pool.READ);
				assertEquals(1L, after.acceptedTasks() - before.acceptedTasks());
				assertEquals(1L, after.outcomes().get(RWScheduler.TerminalOutcome.FAILURE)
						- before.outcomes().get(RWScheduler.TerminalOutcome.FAILURE));
				assertEquals(0L, after.outcomes().get(RWScheduler.TerminalOutcome.RUN)
						- before.outcomes().get(RWScheduler.TerminalOutcome.RUN));
			} finally {
				db.setRangeIteratorOpenObserverForTesting(null);
			}
		}
	}

	@Test
	void shutdownDefersRetainedRangeCleanupUntilTheActiveQuantumReturns() throws Exception {
		var quantumEntered = new CountDownLatch(1);
		var releaseQuantum = new CountDownLatch(1);
		EmbeddedConnection connection = null;
		java.util.concurrent.CompletableFuture<Void> close = null;
		try {
			connection = populatedConnection("retained-count-active-shutdown", 64, """
					database.parallelism.workload.range-quantum-max-items = 8
					database.parallelism.workload.range-quantum-max-bytes = 1MiB
					database.parallelism.workload.range-quantum-max-duration = PT1S
					""");
			var db = connection.getInternalDB();
			db.setRangeCountChunkObserverForTesting(() -> {
				quantumEntered.countDown();
				await(releaseQuantum);
			});
			long columnId = connection.getSyncApi(RequestContext.batch()).getColumnId("entries");
			var scheduler = connection.getScheduler();
			var outcomesBefore = scheduler.poolSnapshot(RWScheduler.Pool.READ).outcomes();
			var count = connection.getAsyncApi(RequestContext.batch()).reduceRangeAsync(
					0, columnId, null, null, false, RequestType.entriesCount());
			assertTrue(quantumEntered.await(5, TimeUnit.SECONDS));

			var connectionToClose = connection;
			close = java.util.concurrent.CompletableFuture.runAsync(() -> {
				try {
					connectionToClose.closeTesting();
				} catch (Exception failure) {
					throw new java.util.concurrent.CompletionException(failure);
				}
			});
			Thread.sleep(100L);
			assertFalse(close.isDone(), "shutdown completed while a retained range quantum was active");
			assertEquals(1, db.getActiveRangeCursorCount(),
					"shutdown must not close a cursor owned by an active scheduler quantum");
			assertEquals(1, db.getRetainedRangeSnapshotCount());

			releaseQuantum.countDown();
			var failure = assertThrows(ExecutionException.class, () -> count.get(5, TimeUnit.SECONDS));
			assertTrue(failure.getCause() instanceof java.util.concurrent.RejectedExecutionException);
			close.get(10, TimeUnit.SECONDS);
			assertRetainedResourcesDrained(connection);
			var outcomesAfter = scheduler.poolSnapshot(RWScheduler.Pool.READ).outcomes();
			assertEquals(1L, outcomesAfter.get(RWScheduler.TerminalOutcome.SHUTDOWN)
					- outcomesBefore.get(RWScheduler.TerminalOutcome.SHUTDOWN));
			assertEquals(0L, outcomesAfter.get(RWScheduler.TerminalOutcome.CANCELLATION)
					- outcomesBefore.get(RWScheduler.TerminalOutcome.CANCELLATION));
		} finally {
			releaseQuantum.countDown();
			if (connection != null) {
				connection.getInternalDB().setRangeCountChunkObserverForTesting(null);
				if (close == null) {
					connection.closeTesting();
				} else {
					try {
						close.get(10, TimeUnit.SECONDS);
					} catch (Exception ignored) {
						// Preserve the primary assertion failure; the close future carries shutdown diagnostics.
					}
				}
			}
		}
	}

	@Test
	void deadlineRemainsFirstCauseWhenShutdownRacesAnActiveQuantum() throws Exception {
		var quantumEntered = new CountDownLatch(1);
		var releaseQuantum = new CountDownLatch(1);
		EmbeddedConnection connection = null;
		java.util.concurrent.CompletableFuture<Void> close = null;
		try {
			connection = populatedConnection("retained-count-deadline-shutdown-race", 64, """
					database.parallelism.workload.range-quantum-max-items = 8
					database.parallelism.workload.range-quantum-max-bytes = 1MiB
					database.parallelism.workload.range-quantum-max-duration = PT1S
					""");
			var db = connection.getInternalDB();
			db.setRangeCountChunkObserverForTesting(() -> {
				quantumEntered.countDown();
				await(releaseQuantum);
				throw new IllegalStateException("late range observer failure");
			});
			long columnId = connection.getSyncApi(RequestContext.batch()).getColumnId("entries");
			var scheduler = connection.getScheduler();
			var outcomesBefore = scheduler.poolSnapshot(RWScheduler.Pool.READ).outcomes();
			var count = connection.getAsyncApi(RequestContext.batch(java.time.Duration.ofMillis(250)))
					.reduceRangeAsync(0, columnId, null, null, false, RequestType.entriesCount());
			assertTrue(quantumEntered.await(5, TimeUnit.SECONDS));
			Thread.sleep(300L);
			db.cleanupExpiredRangesNow();

			var connectionToClose = connection;
			close = java.util.concurrent.CompletableFuture.runAsync(() -> {
				try {
					connectionToClose.closeTesting();
				} catch (Exception failure) {
					throw new java.util.concurrent.CompletionException(failure);
				}
			});
			Thread.sleep(100L);
			assertFalse(close.isDone());
			assertEquals(1, db.getActiveRangeCursorCount());

			releaseQuantum.countDown();
			var failure = assertThrows(ExecutionException.class, () -> count.get(5, TimeUnit.SECONDS));
			assertTrue(failure.getCause() instanceof RocksDBException rocksFailure
					&& rocksFailure.getErrorUniqueId() == RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,
					"shutdown replaced the scheduler's winning deadline failure: " + failure.getCause());
			close.get(10, TimeUnit.SECONDS);
			assertRetainedResourcesDrained(connection);
			var outcomesAfter = scheduler.poolSnapshot(RWScheduler.Pool.READ).outcomes();
			assertEquals(1L, outcomesAfter.get(RWScheduler.TerminalOutcome.DEADLINE)
					- outcomesBefore.get(RWScheduler.TerminalOutcome.DEADLINE));
			assertEquals(0L, outcomesAfter.get(RWScheduler.TerminalOutcome.SHUTDOWN)
					- outcomesBefore.get(RWScheduler.TerminalOutcome.SHUTDOWN));
			assertEquals(0L, outcomesAfter.get(RWScheduler.TerminalOutcome.CANCELLATION)
					- outcomesBefore.get(RWScheduler.TerminalOutcome.CANCELLATION));
		} finally {
			releaseQuantum.countDown();
			if (connection != null) {
				connection.getInternalDB().setRangeCountChunkObserverForTesting(null);
				if (close == null) {
					connection.closeTesting();
				} else {
					try {
						close.get(10, TimeUnit.SECONDS);
					} catch (Exception ignored) {
						// Preserve the primary assertion failure; the close future carries shutdown diagnostics.
					}
				}
			}
		}
	}

	@Test
	void backpressuredStreamParksWithoutACompetitiveZeroDemandRedispatch() throws Exception {
		String databaseName = "retained-stream-competitive-park";
		try (var connection = populatedConnection(databaseName, 17, """
				database.parallelism.read = 3
				database.parallelism.write = 3
				database.parallelism.workload.competing-batch-read-maximum-active = 3
				database.parallelism.workload.range-quantum-max-items = 16
				database.parallelism.workload.range-quantum-max-bytes = 1MiB
				database.parallelism.workload.range-quantum-max-duration = PT1S
				""")) {
			long columnId = connection.getSyncApi(RequestContext.batch()).getColumnId("entries");
			var scheduler = connection.getScheduler();
			var releaseBlockers = new CountDownLatch(1);
			var blockersStarted = new CountDownLatch(2);
			occupyReadWorkers(scheduler.executor(it.cavallium.rockserver.core.common.WorkloadProfile.BATCH, it.cavallium.rockserver.core.common.OperationFamily.RANGE_PAGE, Long.MAX_VALUE), 2, blockersStarted, releaseBlockers);
			assertTrue(blockersStarted.await(5, TimeUnit.SECONDS));

			var cursorOpening = new CountDownLatch(1);
			var releaseCursorOpen = new CountDownLatch(1);
			var foregroundRan = new CountDownLatch(1);
			connection.getInternalDB().setRangeIteratorOpenObserverForTesting(() -> {
				cursorOpening.countDown();
				await(releaseCursorOpen);
			});

			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					Long.MAX_VALUE);
			var quantumCounter = connection.getInternalDB().getMetricsRegistry()
					.get("rockserver.workload.quantums")
					.tags("database", databaseName,
							"resource", "read",
							"profile", "batch",
							"operation", "range_page")
					.counter();
			double quantumsBefore = quantumCounter.count();
			long acceptedBefore = scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks();
			try {
				var range = Flux.from(connection.getAsyncApi(RequestContext.batch()).getRangeAsync(
						0, columnId, null, null, false, RequestType.allInRange()));
				StepVerifier.create(range, 1)
						.then(() -> {
							assertTrue(await(cursorOpening, 5, TimeUnit.SECONDS));
							scheduler.executor(WorkloadProfile.LATENCY,
									OperationFamily.POINT_LOOKUP,
									System.currentTimeMillis() + 5_000L).execute(foregroundRan::countDown);
							releaseCursorOpen.countDown();
						})
						.assertNext(first -> assertEquals(key(0), first.keys()))
						.thenAwait(Duration.ofMillis(200))
						.then(() -> assertTrue(await(foregroundRan, 5, TimeUnit.SECONDS)))
						.thenRequest(Long.MAX_VALUE)
						.expectNextCount(16)
						.verifyComplete();

				assertEquals(2L,
						scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks() - acceptedBefore,
						"one retained stream plus one foreground task are two logical submissions");
				assertTrue(awaitCondition(() -> quantumCounter.count() - quantumsBefore == 2.0d, 5_000),
						"two native chunks must use two quantums without a zero-demand redispatch");
				assertRetainedResourcesDrained(connection);
			} finally {
				releaseCursorOpen.countDown();
				releaseBlockers.countDown();
				connection.getInternalDB().setRangeIteratorOpenObserverForTesting(null);
			}
		}
	}

	@Test
	void analyticalCountUsesTheCooperativeRuntimeAfterSchedulerIntegration() throws Exception {
		assertUncontendedCountProfile(WorkloadProfile.ANALYTICAL, RequestContext.analytical());
	}

	@Test
	void ingestCountRemainsRejectedByTheWorkloadContract() throws Exception {
		try (var connection = populatedConnection("retained-count-ingest", 33, "")) {
			long columnId = connection.getSyncApi(RequestContext.batch()).getColumnId("entries");
			var scheduler = connection.getScheduler();
			long acceptedBefore = scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks();

			var failure = assertThrows(RocksDBException.class,
					() -> connection.getAsyncApi(RequestContext.ingest()).reduceRangeAsync(
							0, columnId, null, null, false, RequestType.entriesCount()));
			assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, failure.getErrorUniqueId());
			assertEquals(acceptedBefore, scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks(),
					"rejected INGEST aggregates must not reach scheduler admission");
			assertRetainedResourcesDrained(connection);
		}
	}

	private void assertUncontendedCountProfile(WorkloadProfile profile, RequestContext context) throws Exception {
		String databaseName = "retained-count-" + profile.name().toLowerCase(java.util.Locale.ROOT);
		try (var connection = populatedConnection(databaseName, 33, """
				database.parallelism.workload.range-quantum-max-items = 4
				database.parallelism.workload.range-quantum-max-duration = PT1S
				""")) {
			long columnId = connection.getSyncApi(context).getColumnId("entries");
			var scheduler = connection.getScheduler();
			long acceptedBefore = scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks();
			var iteratorOpens = new AtomicInteger();
			connection.getInternalDB().setRangeIteratorOpenObserverForTesting(iteratorOpens::incrementAndGet);

			assertEquals(33L, connection.getAsyncApi(context).reduceRangeAsync(
					0, columnId, null, null, false, RequestType.entriesCount())
					.get(10, TimeUnit.SECONDS));
			assertEquals(1L,
					scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks() - acceptedBefore);
			assertEquals(1, iteratorOpens.get());
			assertRetainedResourcesDrained(connection);
		}
	}

	private EmbeddedConnection populatedConnection(String databaseName,
			int entries,
			String configuration) throws Exception {
		Path config = configuration.isBlank()
				? null
				: Files.writeString(tempDir.resolve(databaseName + ".conf"), configuration);
		var connection = new EmbeddedConnection(tempDir.resolve(databaseName), databaseName, config);
		var api = connection.getSyncApi(RequestContext.batch());
		long columnId = api.createColumn("entries",
				ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
		for (int i = 0; i < entries; i++) {
			api.put(0, columnId, key(i), value(i), RequestType.none());
		}
		return connection;
	}

	private static void occupyReadWorkers(java.util.concurrent.Executor executor,
			int workers,
			CountDownLatch started,
			CountDownLatch release) {
		for (int i = 0; i < workers; i++) {
			executor.execute(() -> {
				started.countDown();
				await(release);
			});
		}
	}

	private static void assertRetainedResourcesDrained(EmbeddedConnection connection) throws Exception {
		assertTrue(awaitCondition(() -> connection.getInternalDB().getActiveRangeCursorCount() == 0
				&& connection.getInternalDB().getRetainedRangeSnapshotCount() == 0
				&& connection.getInternalDB().getRetainedRangePermitCount() == 0
				&& connection.getInternalDB().getRetainedRangeWaiterCount() == 0, 5_000));
	}

	private static void awaitReadPoolDrained(RWScheduler scheduler) throws InterruptedException {
		assertTrue(awaitCondition(() -> {
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			return snapshot.activeTasks() == 0 && snapshot.queuedTasks() == 0;
		}, 5_000));
	}

	private static boolean awaitCondition(java.util.function.BooleanSupplier condition,
			long timeoutMillis) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(10L);
		}
		return condition.getAsBoolean();
	}

	private static void await(CountDownLatch latch) {
		boolean interrupted = false;
		while (true) {
			try {
				latch.await();
				break;
			} catch (InterruptedException ignored) {
				interrupted = true;
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private static boolean await(CountDownLatch latch, long timeout, TimeUnit unit) {
		try {
			return latch.await(timeout, unit);
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			return false;
		}
	}

	private static Keys key(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Buf value(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}
}
