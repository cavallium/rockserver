package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch.KVBatchRef;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
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
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

@Timeout(60)
class CooperativeIteratorContinuationTest {

	private static final int ITERATOR_STEP = 4_096;
	private static final int READ_WORKERS = 3;

	@TempDir
	Path tempDir;

	@Test
	void batchLongContinuationsPreserveAllModesAndUseOneLogicalTask() throws Exception {
		final int entries = ITERATOR_STEP * 2 + 37;
		try (var connection = connection("batch-modes")) {
			var sync = connection.getSyncApi(RequestContext.batch());
			var async = connection.getAsyncApi(RequestContext.batch());
			long columnId = populateFixedColumn(sync, "entries", entries);
			var scheduler = connection.getScheduler();

			long multiIterator = sync.openIterator(0L, columnId, new Keys(), null, false, 30_000L);
			try {
				long acceptedBefore = readSnapshot(scheduler).acceptedTasks();
				var values = async.subsequentAsync(
						multiIterator, ITERATOR_STEP + 1L, ITERATOR_STEP + 1L, RequestType.<Buf>multi())
						.get(10, SECONDS);
				assertEquals(ITERATOR_STEP + 1, values.size());
				assertEquals(value(ITERATOR_STEP + 1), values.getFirst());
				assertEquals(value(ITERATOR_STEP * 2 + 1), values.getLast());
				assertEquals(1L, readSnapshot(scheduler).acceptedTasks() - acceptedBefore,
						"all MULTI steps must share one scheduler admission");
			} finally {
				sync.closeIterator(multiIterator);
			}

			long existsIterator = sync.openIterator(0L, columnId, new Keys(), null, false, 30_000L);
			try {
				long acceptedBefore = readSnapshot(scheduler).acceptedTasks();
				assertTrue(async.subsequentAsync(
						existsIterator, entries - 1L, Long.MAX_VALUE, RequestType.exists()).get(10, SECONDS));
				assertEquals(1L, readSnapshot(scheduler).acceptedTasks() - acceptedBefore,
						"SKIP and EXISTS must share one scheduler admission");
			} finally {
				sync.closeIterator(existsIterator);
			}

			long noneIterator = sync.openIterator(0L, columnId, new Keys(), null, false, 30_000L);
			try {
				long acceptedBefore = readSnapshot(scheduler).acceptedTasks();
				async.subsequentAsync(noneIterator, 0L, Long.MAX_VALUE, RequestType.none()).get(10, SECONDS);
				assertEquals(1L, readSnapshot(scheduler).acceptedTasks() - acceptedBefore,
						"exhaustion must stop Long.MAX_VALUE without recursive submissions");
				assertFalse(async.subsequentAsync(noneIterator, 0L, 1L, RequestType.exists()).get(5, SECONDS));
			} finally {
				sync.closeIterator(noneIterator);
			}
		}
	}

	@Test
	void batchContinuationPreservesReverseBucketedAndTransactionalIteration() throws Exception {
		final int entries = ITERATOR_STEP + 8;
		try (var connection = connection("iterator-forms")) {
			var sync = connection.getSyncApi(RequestContext.batch());
			var async = connection.getAsyncApi(RequestContext.batch());
			long fixedColumn = populateFixedColumn(sync, "fixed", entries);
			long bucketedColumn = populateBucketedColumn(sync, "bucketed", entries);

			long reverseIterator = sync.openIterator(0L, fixedColumn, new Keys(), null, true, 30_000L);
			try {
				var values = async.subsequentAsync(
						reverseIterator, ITERATOR_STEP, 3L, RequestType.<Buf>multi()).get(10, SECONDS);
				assertEquals(List.of(value(7), value(6), value(5)), values);
			} finally {
				sync.closeIterator(reverseIterator);
			}

			long bucketedIterator = sync.openIterator(0L, bucketedColumn, new Keys(), null, false, 30_000L);
			try {
				var values = async.subsequentAsync(
						bucketedIterator, ITERATOR_STEP, 3L, RequestType.<Buf>multi()).get(10, SECONDS);
				assertEquals(List.of(value(ITERATOR_STEP), value(ITERATOR_STEP + 1), value(ITERATOR_STEP + 2)),
						values);
			} finally {
				sync.closeIterator(bucketedIterator);
			}

			long transactionId = sync.openTransaction(30_000L);
			try {
				long transactionIterator = sync.openIterator(
						transactionId, fixedColumn, new Keys(), null, false, 30_000L);
				try {
					var values = async.subsequentAsync(
							transactionIterator, ITERATOR_STEP, 3L, RequestType.<Buf>multi()).get(10, SECONDS);
					assertEquals(List.of(value(ITERATOR_STEP), value(ITERATOR_STEP + 1), value(ITERATOR_STEP + 2)),
							values);
				} finally {
					sync.closeIterator(transactionIterator);
				}
			} finally {
				assertTrue(sync.closeTransaction(transactionId, false));
			}
		}
	}

	@Test
	void batchCompetitionYieldsOneLogicalTaskAndForegroundRunsFirst() throws Exception {
		final int entries = ITERATOR_STEP * 16 + 1;
		String database = "iterator-yield";
		try (var connection = connection(database)) {
			var sync = connection.getSyncApi(RequestContext.batch());
			var async = connection.getAsyncApi(RequestContext.batch());
			long columnId = populateFixedColumn(sync, "entries", entries);
			long iteratorId = sync.openIterator(0L, columnId, new Keys(), null, false, 30_000L);
			var scheduler = connection.getScheduler();
			var blockersEntered = new CountDownLatch(READ_WORKERS - 1);
			var releaseBlockers = new CountDownLatch(1);
			try {
				occupyLatencyWorkers(scheduler, READ_WORKERS - 1, blockersEntered, releaseBlockers);
				assertTrue(blockersEntered.await(5, SECONDS));
				var before = readSnapshot(scheduler);
				double quantumsBefore = rangeQuantums(connection, database);
				var firstCompletion = new AtomicInteger();

				var continuation = async.subsequentAsync(
						iteratorId, 0L, entries, RequestType.<Buf>multi());
				continuation.whenComplete((_, _) -> firstCompletion.compareAndSet(0, 2));
				assertEventually(() -> readSnapshot(scheduler).activeByProfile().get(WorkloadProfile.BATCH) == 1);
				assertFalse(continuation.isDone(), "the foreground probe needs a live multi-step continuation");

				var foregroundRan = new CountDownLatch(1);
				scheduler.executor(WorkloadProfile.LATENCY,
						OperationFamily.POINT_LOOKUP,
						System.currentTimeMillis() + SECONDS.toMillis(30)).execute(() -> {
					firstCompletion.compareAndSet(0, 1);
					foregroundRan.countDown();
				});

				assertTrue(foregroundRan.await(5, SECONDS),
						"foreground work did not run after the next bounded iterator step");
				assertEquals(entries, continuation.get(15, SECONDS).size());
				assertEquals(1, firstCompletion.get(), "the continuation monopolized the last read worker");
				assertEventually(() -> readSnapshot(scheduler).completedTasks() >= before.completedTasks() + 2L);
				var after = readSnapshot(scheduler);
				assertEquals(2L, after.acceptedTasks() - before.acceptedTasks(),
						"one iterator task plus one foreground task should be admitted");
				assertEquals(2L, after.startedTasks() - before.startedTasks());
				assertEquals(2L, after.completedTasks() - before.completedTasks());
				assertEventually(() -> rangeQuantums(connection, database) - quantumsBefore >= 2.0);
			} finally {
				releaseBlockers.countDown();
				sync.closeIterator(iteratorId);
			}
		}
	}

	@Test
	void configuredItemAndByteQuantumBoundsSplitLargeMultiUnderCompetition() throws Exception {
		final int entries = 20;
		final int valueBytes = 8 * 1_024;
		String database = "iterator-configured-quantum";
		try (var connection = connection(database, """
				range-quantum-max-items: 16
				range-quantum-max-bytes: 16KiB
				range-quantum-max-duration: PT0.008S
				""")) {
			var sync = connection.getSyncApi(RequestContext.batch());
			var async = connection.getAsyncApi(RequestContext.batch());
			long columnId = populateFixedColumn(sync, "entries", entries, valueBytes);
			long iteratorId = sync.openIterator(0L, columnId, new Keys(), null, false, 30_000L);
			var scheduler = connection.getScheduler();
			var blockersEntered = new CountDownLatch(READ_WORKERS - 1);
			var releaseBlockers = new CountDownLatch(1);
			try {
				occupyLatencyWorkers(scheduler, READ_WORKERS - 1, blockersEntered, releaseBlockers);
				assertTrue(blockersEntered.await(5, SECONDS));
				double quantumsBefore = rangeQuantums(connection, database);

				var values = async.subsequentAsync(iteratorId, 0L, entries, RequestType.<Buf>multi())
						.get(10, SECONDS);

				assertEquals(entries, values.size());
				assertTrue(rangeQuantums(connection, database) - quantumsBefore >= 10.0,
						"16KiB of service per competitive quantum permits at most two 8KiB values");
			} finally {
				releaseBlockers.countDown();
				sync.closeIterator(iteratorId);
			}
		}
	}

	@Test
	void iteratorQuantumStopsAtByteOrTimeBudgetButRemainsWorkConservingWhenIdle() throws Exception {
		final int entries = 20;
		final int valueBytes = 8 * 1_024;
		try (var connection = connection("iterator-quantum-control")) {
			var sync = connection.getSyncApi(RequestContext.batch());
			long columnId = populateFixedColumn(sync, "entries", entries, valueBytes);
			long iteratorId = sync.openIterator(0L, columnId, new Keys(), null, false, 30_000L);
			try {
				var competing = new FixedCooperativeContext(true, false);
				var byteBounded = connection.getInternalDB().readIteratorQuantumInternal(
						iteratorId, entries, 16L * 1_024L, Long.MAX_VALUE, competing);
				assertEquals(2, byteBounded.values().size());
				assertEquals(16L * 1_024L, byteBounded.decodedBytes());
				assertTrue(byteBounded.checkpointRequested());
				assertFalse(byteBounded.exhausted());

				sync.seekTo(iteratorId, key(0));
				var timeBounded = connection.getInternalDB().readIteratorQuantumInternal(
						iteratorId, entries, Long.MAX_VALUE, 1L, competing);
				assertEquals(1, timeBounded.values().size(),
						"the duration bound may overshoot by only the current logical iterator step");
				assertTrue(timeBounded.checkpointRequested());

				sync.seekTo(iteratorId, key(0));
				var idle = connection.getInternalDB().readIteratorQuantumInternal(
						iteratorId, entries, 1L, 1L, new FixedCooperativeContext(false, false));
				assertEquals(entries, idle.values().size(),
						"byte/time slicing must not pace a worker when no peer needs it");
				assertFalse(idle.checkpointRequested());

				sync.seekTo(iteratorId, key(0));
				var terminated = connection.getInternalDB().readIteratorQuantumInternal(
						iteratorId, entries, Long.MAX_VALUE, Long.MAX_VALUE,
						new FixedCooperativeContext(true, true));
				assertTrue(terminated.values().isEmpty());
				assertFalse(terminated.checkpointRequested());
			} finally {
				sync.closeIterator(iteratorId);
			}
		}
	}

	@Test
	void batchCancellationReleasesTheGateAndLeavesIteratorReusable() throws Exception {
		final int entries = ITERATOR_STEP * 16 + 1;
		try (var connection = connection("iterator-cancel")) {
			var sync = connection.getSyncApi(RequestContext.batch());
			var async = connection.getAsyncApi(RequestContext.batch());
			long columnId = populateFixedColumn(sync, "entries", entries);
			long iteratorId = sync.openIterator(0L, columnId, new Keys(), null, false, 30_000L);
			var scheduler = connection.getScheduler();
			var blockersEntered = new CountDownLatch(READ_WORKERS - 1);
			var releaseBlockers = new CountDownLatch(1);
			try {
				occupyLatencyWorkers(scheduler, READ_WORKERS - 1, blockersEntered, releaseBlockers);
				assertTrue(blockersEntered.await(5, SECONDS));
				var continuation = async.subsequentAsync(
						iteratorId, 0L, entries, RequestType.<Buf>multi());
				assertEventually(() -> readSnapshot(scheduler).activeByProfile().get(WorkloadProfile.BATCH) == 1);
				assertFalse(continuation.isDone());
				assertConcurrentIteratorOperation(async.seekToAsync(iteratorId, key(0)));

				assertTrue(continuation.cancel(true));
				assertThrows(CancellationException.class, () -> continuation.get(5, SECONDS));
				awaitSuccessfulSeek(async, iteratorId, key(0));
				assertTrue(async.subsequentAsync(iteratorId, 0L, 1L, RequestType.exists()).get(5, SECONDS));
				assertEquals(1, connection.getInternalDB().getOpenIteratorsCount(),
						"cancellation must not close an otherwise reusable iterator");
			} finally {
				releaseBlockers.countDown();
				sync.closeIterator(iteratorId);
			}
		}
	}

	@Test
	void cancellationCompletesOnlyAfterTheCurrentNativeStepAndGateRelease() throws Exception {
		final int entries = ITERATOR_STEP * 2 + 1;
		try (var connection = connection("iterator-cancel-active-step")) {
			var sync = connection.getSyncApi(RequestContext.batch());
			var async = connection.getAsyncApi(RequestContext.batch());
			long columnId = populateFixedColumn(sync, "entries", entries);
			long iteratorId = sync.openIterator(0L, columnId, new Keys(), null, false, 30_000L);
			var stepCompleted = new CountDownLatch(1);
			var releaseStep = new CountDownLatch(1);
			var scheduler = connection.getScheduler();
			connection.getInternalDB().setIteratorAdvanceStepCompletedObserverForTesting(() -> {
				stepCompleted.countDown();
				awaitUninterruptibly(releaseStep);
			});
			try {
				var before = readSnapshot(scheduler);
				var continuation = async.subsequentAsync(
						iteratorId, 0L, entries, RequestType.none());
				assertTrue(stepCompleted.await(5, SECONDS));
				assertConcurrentIteratorOperation(async.seekToAsync(iteratorId, key(0)));

				assertTrue(continuation.cancel(true));
				assertFalse(continuation.isDone(),
						"cancellation became observable before the bounded iterator step returned");
				assertFalse(continuation.isCancelled());
				assertConcurrentIteratorOperation(async.seekToAsync(iteratorId, key(0)));

				releaseStep.countDown();
				assertThrows(CancellationException.class, () -> continuation.get(5, SECONDS));
				assertTrue(continuation.isCancelled());
				var after = readSnapshot(scheduler);
				assertEquals(1L, after.acceptedTasks() - before.acceptedTasks());
				assertEquals(1L, after.startedTasks() - before.startedTasks());
				assertEquals(1L, after.completedTasks() - before.completedTasks());
				assertEquals(1L,
						after.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION)
								- before.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION));
				assertEquals(0L,
						after.outcomes().get(RWScheduler.TerminalOutcome.RUN)
								- before.outcomes().get(RWScheduler.TerminalOutcome.RUN));
				awaitSuccessfulSeek(async, iteratorId, key(0));
				assertTrue(async.subsequentAsync(iteratorId, 0L, 1L, RequestType.exists()).get(5, SECONDS));
			} finally {
				releaseStep.countDown();
				connection.getInternalDB().setIteratorAdvanceStepCompletedObserverForTesting(null);
				sync.closeIterator(iteratorId);
			}
		}
	}

	@Test
	void nativeFailureIsSchedulerAuthoritativeAndGateReleasePrecedesPublication() throws Exception {
		final int entries = ITERATOR_STEP + 1;
		try (var connection = connection("iterator-native-failure")) {
			var sync = connection.getSyncApi(RequestContext.batch());
			var async = connection.getAsyncApi(RequestContext.batch());
			long columnId = populateFixedColumn(sync, "entries", entries);
			long iteratorId = sync.openIterator(0L, columnId, new Keys(), null, false, 30_000L);
			var scheduler = connection.getScheduler();
			var nativeFailure = RocksDBException.of(RocksDBErrorType.GET_1,
					"forced iterator continuation failure");
			var snapshotAtPublication = new AtomicReference<RWScheduler.PoolSnapshot>();
			connection.getInternalDB().setIteratorAdvanceStepCompletedObserverForTesting(() -> {
				throw nativeFailure;
			});
			try {
				var before = readSnapshot(scheduler);
				var continuation = async.subsequentAsync(
						iteratorId, 0L, entries, RequestType.none());
				var gateProbe = continuation.handle((_, _) -> {
					snapshotAtPublication.set(readSnapshot(scheduler));
					return async.seekToAsync(iteratorId, key(0));
				}).thenCompose(future -> future);

				var completion = assertThrows(ExecutionException.class,
						() -> continuation.get(5, SECONDS));
				assertSame(nativeFailure, completion.getCause());
				gateProbe.get(5, SECONDS);

				var published = snapshotAtPublication.get();
				assertNotNull(published);
				assertEquals(1L,
						published.outcomes().get(RWScheduler.TerminalOutcome.FAILURE)
								- before.outcomes().get(RWScheduler.TerminalOutcome.FAILURE));
				assertEquals(0L,
						published.outcomes().get(RWScheduler.TerminalOutcome.RUN)
								- before.outcomes().get(RWScheduler.TerminalOutcome.RUN));
			} finally {
				connection.getInternalDB().setIteratorAdvanceStepCompletedObserverForTesting(null);
				sync.closeIterator(iteratorId);
			}
		}
	}

	@Test
	void batchDeadlineWhileQueuedReleasesTheGateWithoutAdvancing() throws Exception {
		try (var connection = connection("iterator-deadline")) {
			var sync = connection.getSyncApi(RequestContext.batch());
			long columnId = populateFixedColumn(sync, "entries", ITERATOR_STEP + 8);
			long iteratorId = sync.openIterator(0L, columnId, new Keys(), null, false, 30_000L);
			var scheduler = connection.getScheduler();
			var blockersEntered = new CountDownLatch(READ_WORKERS);
			var releaseBlockers = new CountDownLatch(1);
			try {
				occupyLatencyWorkers(scheduler, READ_WORKERS, blockersEntered, releaseBlockers);
				assertTrue(blockersEntered.await(5, SECONDS));
				var deadlineApi = connection.getAsyncApi(RequestContext.batch(Instant.now().plusMillis(50L)));
				var continuation = deadlineApi.subsequentAsync(
						iteratorId, 0L, Long.MAX_VALUE, RequestType.none());
				assertEventually(() -> readSnapshot(scheduler).queuedByProfile().get(WorkloadProfile.BATCH) == 1);
				Thread.sleep(100L);
				releaseBlockers.countDown();

				var completion = assertThrows(ExecutionException.class, () -> continuation.get(5, SECONDS));
				var rocksFailure = assertInstanceOf(RocksDBException.class, completion.getCause());
				assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, rocksFailure.getErrorUniqueId());

				awaitSuccessfulSeek(connection.getAsyncApi(RequestContext.batch()), iteratorId, key(0));
				assertTrue(connection.getAsyncApi(RequestContext.batch())
						.subsequentAsync(iteratorId, 0L, 1L, RequestType.exists()).get(5, SECONDS),
						"an expired queued continuation advanced the iterator");
			} finally {
				releaseBlockers.countDown();
				sync.closeIterator(iteratorId);
			}
		}
	}

	@Test
	void batchCloseWinsTheRaceAfterCancellingAnActiveContinuation() throws Exception {
		final int entries = ITERATOR_STEP * 16 + 1;
		try (var connection = connection("iterator-close-race")) {
			var sync = connection.getSyncApi(RequestContext.batch());
			var async = connection.getAsyncApi(RequestContext.batch());
			long columnId = populateFixedColumn(sync, "entries", entries);
			long iteratorId = sync.openIterator(0L, columnId, new Keys(), null, false, 30_000L);
			var scheduler = connection.getScheduler();
			var blockersEntered = new CountDownLatch(READ_WORKERS - 1);
			var releaseBlockers = new CountDownLatch(1);
			try {
				occupyLatencyWorkers(scheduler, READ_WORKERS - 1, blockersEntered, releaseBlockers);
				assertTrue(blockersEntered.await(5, SECONDS));
				var continuation = async.subsequentAsync(
						iteratorId, 0L, entries, RequestType.<Buf>multi());
				assertEventually(() -> readSnapshot(scheduler).activeByProfile().get(WorkloadProfile.BATCH) == 1);
				assertFalse(continuation.isDone());

				var close = async.closeIteratorAsync(iteratorId);
				assertFalse(close.isDone(), "close must wait for the active logical continuation");
				assertTrue(continuation.cancel(true));
				assertThrows(CancellationException.class, () -> continuation.get(5, SECONDS));
				close.get(5, SECONDS);
				assertEquals(0, connection.getInternalDB().getOpenIteratorsCount());
				var closed = assertThrows(RocksDBException.class, () -> sync.seekTo(iteratorId, key(0)));
				assertEquals(RocksDBErrorType.PUT_INVALID_REQUEST, closed.getErrorUniqueId());
			} finally {
				releaseBlockers.countDown();
				if (connection.getInternalDB().getOpenIteratorsCount() != 0) {
					sync.closeIterator(iteratorId);
				}
			}
		}
	}

	@Test
	void batchShutdownCancelsAQueuedContinuationAndDrainsTheIterator() throws Exception {
		String timeoutProperty = "it.cavallium.rockserver.db.shutdown-pending-ops-timeout-ms";
		String previousTimeout = System.getProperty(timeoutProperty);
		System.setProperty(timeoutProperty, "1");
		var releaseBlockers = new CountDownLatch(1);
		EmbeddedConnection connection = null;
		CompletableFuture<Void> close = null;
		try {
			connection = connection("iterator-shutdown");
			var sync = connection.getSyncApi(RequestContext.batch());
			long columnId = populateFixedColumn(sync, "entries", ITERATOR_STEP + 8);
			long iteratorId = sync.openIterator(0L, columnId, new Keys(), null, false, 30_000L);
			var scheduler = connection.getScheduler();
			var blockersEntered = new CountDownLatch(READ_WORKERS);
			occupyLatencyWorkers(scheduler, READ_WORKERS, blockersEntered, releaseBlockers);
			assertTrue(blockersEntered.await(5, SECONDS));

			var continuation = connection.getAsyncApi(RequestContext.batch())
					.subsequentAsync(iteratorId, 0L, Long.MAX_VALUE, RequestType.none());
			assertEventually(() -> readSnapshot(scheduler).queuedByProfile().get(WorkloadProfile.BATCH) == 1);
			var connectionToClose = connection;
			close = CompletableFuture.runAsync(() -> {
				try {
					connectionToClose.closeTesting();
				} catch (Exception failure) {
					throw new java.util.concurrent.CompletionException(failure);
				}
			});

			assertThrows(CancellationException.class, () -> continuation.get(5, SECONDS));
			assertTrue(continuation.isCancelled());
			releaseBlockers.countDown();
			close.get(10, SECONDS);
			assertEquals(0, connection.getInternalDB().getPendingOpsCount());
			assertEquals(0, connection.getInternalDB().getOpenIteratorsCount());
		} finally {
			releaseBlockers.countDown();
			if (connection != null && (close == null || !close.isDone())) {
				connection.closeTesting();
			}
			if (previousTimeout == null) {
				System.clearProperty(timeoutProperty);
			} else {
				System.setProperty(timeoutProperty, previousTimeout);
			}
		}
	}

	@Test
	void boundedLatencyContinuationKeepsTheOrdinaryTaskPath() throws Exception {
		try (var connection = connection("iterator-latency")) {
			var batch = connection.getSyncApi(RequestContext.batch());
			long columnId = populateFixedColumn(batch, "entries", ITERATOR_STEP + 1);
			var latencyContext = RequestContext.latency(Duration.ofSeconds(10));
			var sync = connection.getSyncApi(latencyContext);
			var async = connection.getAsyncApi(latencyContext);
			long iteratorId = sync.openIterator(0L, columnId, new Keys(), null, false, 30_000L);
			try {
				long acceptedBefore = readSnapshot(connection.getScheduler()).acceptedTasks();
				var values = async.subsequentAsync(
						iteratorId, 1L, ITERATOR_STEP - 1L, RequestType.<Buf>multi()).get(10, SECONDS);
				assertEquals(ITERATOR_STEP - 1, values.size());
				assertEquals(2L, readSnapshot(connection.getScheduler()).acceptedTasks() - acceptedBefore,
						"the bounded LATENCY skip/take path must retain its existing submissions");
			} finally {
				sync.closeIterator(iteratorId);
			}
		}
	}

	private EmbeddedConnection connection(String name) throws Exception {
		return connection(name, "");
	}

	private EmbeddedConnection connection(String name, String workloadOverrides) throws Exception {
		var config = tempDir.resolve(name + ".conf");
		Files.writeString(config, """
				database: {
				  parallelism: {
				    read: 3
				    write: 3
				    workload: {
				      competing-batch-read-maximum-active: 3
				%s
				    }
				  }
				  global: { enable-fast-get: false, ingest-behind: false, optimistic: false }
				}
				""".formatted(workloadOverrides.indent(6)));
		return new EmbeddedConnection(tempDir.resolve(name + "-db"), name, config);
	}

	private static long populateFixedColumn(RocksDBSyncAPI sync, String name, int count) {
		return populateFixedColumn(sync, name, count, Integer.BYTES);
	}

	private static long populateFixedColumn(RocksDBSyncAPI sync, String name, int count, int valueBytes) {
		long columnId = sync.createColumn(name,
				ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
		var keys = new ArrayList<Keys>(count);
		var values = new ArrayList<Buf>(count);
		for (int i = 0; i < count; i++) {
			keys.add(key(i));
			var value = new byte[valueBytes];
			ByteBuffer.wrap(value).putInt(i);
			values.add(Buf.wrap(value));
		}
		sync.putBatch(columnId, Flux.just(new KVBatchRef(keys, values)), PutBatchMode.WRITE_BATCH_NO_WAL);
		return columnId;
	}

	private static long populateBucketedColumn(RocksDBSyncAPI sync, String name, int count) {
		long columnId = sync.createColumn(name,
				ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(ColumnHashType.XXHASH32), true));
		var variableKey = Buf.wrap(new byte[] {1});
		for (int i = 0; i < count; i++) {
			sync.put(0L,
					columnId,
					new Keys(new Buf[] {intBuf(i), variableKey}),
					value(i),
					RequestType.none());
		}
		return columnId;
	}

	private static void occupyLatencyWorkers(RWScheduler scheduler,
			int workers,
			CountDownLatch entered,
			CountDownLatch release) {
		var executor = scheduler.executor(WorkloadProfile.LATENCY,
				OperationFamily.POINT_LOOKUP,
				System.currentTimeMillis() + SECONDS.toMillis(30));
		for (int i = 0; i < workers; i++) {
			executor.execute(() -> {
				entered.countDown();
				awaitUninterruptibly(release);
			});
		}
	}

	private static void assertConcurrentIteratorOperation(CompletableFuture<?> rejected) {
		var completion = assertThrows(ExecutionException.class, () -> rejected.get(5, SECONDS));
		var failure = assertInstanceOf(RocksDBException.class, completion.getCause());
		assertEquals(RocksDBErrorType.PUT_INVALID_REQUEST, failure.getErrorUniqueId());
		assertTrue(failure.getMessage().contains("Concurrent operation on iterator"));
	}

	private static void awaitSuccessfulSeek(RocksDBAsyncAPI async, long iteratorId, Keys keys) throws Exception {
		long deadline = System.nanoTime() + SECONDS.toNanos(5);
		do {
			try {
				async.seekToAsync(iteratorId, keys).get(5, SECONDS);
				return;
			} catch (ExecutionException failure) {
				var rocksFailure = assertInstanceOf(RocksDBException.class, failure.getCause());
				if (rocksFailure.getErrorUniqueId() != RocksDBErrorType.PUT_INVALID_REQUEST
						|| !rocksFailure.getMessage().contains("Concurrent operation on iterator")) {
					throw failure;
				}
				Thread.sleep(1L);
			}
		} while (System.nanoTime() < deadline);
		throw new AssertionError("iterator operation gate was not released");
	}

	private static RWScheduler.PoolSnapshot readSnapshot(RWScheduler scheduler) {
		return scheduler.poolSnapshot(RWScheduler.Pool.READ);
	}

	private static double rangeQuantums(EmbeddedConnection connection, String database) {
		return connection.getInternalDB().getMetricsRegistry()
				.get("rockserver.workload.quantums")
				.tags("database", database,
						"resource", "read",
						"profile", "batch",
						"operation", "range_page")
				.counter()
				.count();
	}

	private static void assertEventually(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + SECONDS.toNanos(5);
		do {
			if (condition.getAsBoolean()) {
				return;
			}
			Thread.sleep(1L);
		} while (System.nanoTime() < deadline);
		assertTrue(condition.getAsBoolean(), "condition did not become true before timeout");
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		while (true) {
			try {
				latch.await();
				break;
			} catch (InterruptedException _) {
				interrupted = true;
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private record FixedCooperativeContext(boolean preemptionRequested,
			boolean terminationRequested) implements RWScheduler.CooperativeContext {

		@Override
		public RuntimeException terminationFailure() {
			return terminationRequested ? new CancellationException("test termination") : null;
		}

		@Override
		public boolean fail(RuntimeException failure) {
			throw new AssertionError("test context must not fail", failure);
		}
	}

	private static Keys key(int value) {
		return new Keys(intBuf(value));
	}

	private static Buf value(int value) {
		return intBuf(value);
	}

	private static Buf intBuf(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}
}
