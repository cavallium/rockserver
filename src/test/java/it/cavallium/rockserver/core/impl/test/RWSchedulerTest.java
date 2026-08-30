package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.IntConsumer;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

class RWSchedulerTest {

	@Test
	void primitivePoolTelemetryMatchesImmutableSnapshot() throws Exception {
		var scheduler = scheduler(1, "primitive-pool-telemetry");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var queuedCompleted = new CountDownLatch(1);
		try {
			scheduler.executor(RequestContext.latency(Duration.ofSeconds(30)), OperationFamily.POINT_LOOKUP)
					.execute(() -> {
						blockerStarted.countDown();
						awaitUninterruptibly(release);
					});
			assertTrue(blockerStarted.await(5, SECONDS));
			scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE)
					.execute(queuedCompleted::countDown);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 1);

			assertPoolTelemetryMatchesSnapshot(scheduler, RWScheduler.Pool.READ);
			assertThrows(IllegalArgumentException.class,
					() -> scheduler.copyPoolTelemetry(RWScheduler.Pool.READ,
							new long[RWScheduler.POOL_TELEMETRY_LENGTH - 1]));

			release.countDown();
			assertTrue(queuedCompleted.await(5, SECONDS));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).drainedAndConserved());
			for (var pool : RWScheduler.Pool.values()) {
				assertPoolTelemetryMatchesSnapshot(scheduler, pool);
			}
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void primitiveTelemetrySchemaIsBoundedAndWarmedCopiesAllocateNothing() {
		assertEquals(RWScheduler.POOL_TELEMETRY_SCALARS,
				RWScheduler.POOL_TELEMETRY_QUEUED_BY_PROFILE);
		assertEquals(RWScheduler.POOL_TELEMETRY_QUEUED_BY_PROFILE + WorkloadProfile.values().length,
				RWScheduler.POOL_TELEMETRY_ACTIVE_BY_PROFILE);
		assertEquals(RWScheduler.POOL_TELEMETRY_ACTIVE_BY_PROFILE + WorkloadProfile.values().length,
				RWScheduler.POOL_TELEMETRY_LENGTH);
		var scheduler = scheduler(1, "primitive-telemetry-allocation");
		var telemetry = new long[RWScheduler.POOL_TELEMETRY_LENGTH + 1];
		try {
			for (int iteration = 0; iteration < 100_000; iteration++) {
				scheduler.copyPoolTelemetry(RWScheduler.Pool.READ, telemetry);
			}
			java.util.Arrays.fill(telemetry, -1L);
			var threads = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory
					.getThreadMXBean();
			if (!threads.isThreadAllocatedMemoryEnabled()) threads.setThreadAllocatedMemoryEnabled(true);
			long threadId = Thread.currentThread().threadId();
			for (int iteration = 0; iteration < 256; iteration++) {
				threads.getThreadAllocatedBytes(threadId);
			}
			long minimumAllocated = Long.MAX_VALUE;
			for (int window = 0; window < 8; window++) {
				long before = threads.getThreadAllocatedBytes(threadId);
				for (int iteration = 0; iteration < 250_000; iteration++) {
					scheduler.copyPoolTelemetry(RWScheduler.Pool.READ, telemetry);
				}
				minimumAllocated = Math.min(minimumAllocated,
						threads.getThreadAllocatedBytes(threadId) - before);
			}
			assertEquals(0L, minimumAllocated,
					"a fully warmed caller-owned telemetry window must remain allocation-free");
			assertEquals(-1L, telemetry[RWScheduler.POOL_TELEMETRY_LENGTH],
					"copy must not write beyond the public schema");
		} finally {
			scheduler.dispose();
		}
	}

	@Test
	void immutableTelemetryAggregatesFailLoudlyInsteadOfWrapping() {
		var admission = new RWScheduler.ProfileAdmissionSnapshot(
				Map.of(WorkloadProfile.LATENCY, Integer.MAX_VALUE,
						WorkloadProfile.BATCH, 1),
				Map.of(WorkloadProfile.LATENCY, Integer.MAX_VALUE,
						WorkloadProfile.BATCH, 1),
				false);
		assertThrows(ArithmeticException.class, admission::totalQueued);
		assertThrows(ArithmeticException.class, admission::totalActive);

		var pool = new RWScheduler.PoolSnapshot(1, 0, 0, 0, 0, 0,
				0L, 0L, 0L, 0L, 0L,
				Map.of(), Map.of(), Map.of(), Map.of(), Map.of(),
				Map.of(RWScheduler.TerminalOutcome.RUN, Long.MAX_VALUE,
						RWScheduler.TerminalOutcome.FAILURE, 1L),
				false, 1, List.of(), false, false);
		assertThrows(ArithmeticException.class, pool::terminalOutcomes);
	}

	private static void assertPoolTelemetryMatchesSnapshot(RWScheduler scheduler, RWScheduler.Pool pool) {
		var telemetry = new long[RWScheduler.POOL_TELEMETRY_LENGTH];
		scheduler.copyPoolTelemetry(pool, telemetry);
		var snapshot = scheduler.poolSnapshot(pool);
		assertEquals(snapshot.workerCount(), telemetry[RWScheduler.POOL_TELEMETRY_WORKER_COUNT]);
		assertEquals(snapshot.waitingWorkers(), telemetry[RWScheduler.POOL_TELEMETRY_WAITING_WORKERS]);
		assertEquals(snapshot.queuedTasks(), telemetry[RWScheduler.POOL_TELEMETRY_QUEUED_TASKS]);
		assertEquals(snapshot.activeTasks(), telemetry[RWScheduler.POOL_TELEMETRY_ACTIVE_TASKS]);
		assertEquals(snapshot.parkedTasks(), telemetry[RWScheduler.POOL_TELEMETRY_PARKED_TASKS]);
		assertEquals(snapshot.outstandingTasks(), telemetry[RWScheduler.POOL_TELEMETRY_OUTSTANDING_TASKS]);
		assertEquals(snapshot.submissionAttempts(), telemetry[RWScheduler.POOL_TELEMETRY_SUBMISSION_ATTEMPTS]);
		assertEquals(snapshot.acceptedTasks(), telemetry[RWScheduler.POOL_TELEMETRY_ACCEPTED_TASKS]);
		assertEquals(snapshot.startedTasks(), telemetry[RWScheduler.POOL_TELEMETRY_STARTED_TASKS]);
		assertEquals(snapshot.completedTasks(), telemetry[RWScheduler.POOL_TELEMETRY_COMPLETED_TASKS]);
		assertEquals(snapshot.failedTasks(), telemetry[RWScheduler.POOL_TELEMETRY_FAILED_TASKS]);
		assertEquals(snapshot.terminalOutcomes(), telemetry[RWScheduler.POOL_TELEMETRY_TERMINAL_OUTCOMES]);
		assertEquals(snapshot.batchDispatchLimited() ? 1L : 0L,
				telemetry[RWScheduler.POOL_TELEMETRY_BATCH_LIMITED]);
		assertEquals(snapshot.batchStartAllowance(), telemetry[RWScheduler.POOL_TELEMETRY_BATCH_ALLOWANCE]);
		for (var profile : WorkloadProfile.values()) {
			assertEquals(snapshot.queuedByProfile().get(profile).longValue(),
					telemetry[RWScheduler.POOL_TELEMETRY_QUEUED_BY_PROFILE + profile.ordinal()]);
			assertEquals(snapshot.activeByProfile().get(profile).longValue(),
					telemetry[RWScheduler.POOL_TELEMETRY_ACTIVE_BY_PROFILE + profile.ordinal()]);
		}
	}

	@Test
	void everyAllowedProfileFamilyHasOneExactPhysicalPoolRoute() {
		var seenFamilies = java.util.EnumSet.noneOf(OperationFamily.class);
		var seenPools = java.util.EnumSet.noneOf(RWScheduler.Pool.class);
		for (var profile : WorkloadProfile.values()) {
			for (var family : OperationFamily.values()) {
				if (it.cavallium.rockserver.core.impl.WorkloadAdmission.isAllowed(profile, family)) {
					var expected = expectedPool(profile, family);
					assertEquals(expected, RWScheduler.resourcePool(profile, family),
							profile + "/" + family);
					seenFamilies.add(family);
					seenPools.add(expected);
				} else {
					assertThrows(RocksDBException.class, () -> RWScheduler.resourcePool(profile, family));
				}
			}
		}
		assertEquals(java.util.EnumSet.allOf(OperationFamily.class), seenFamilies);
		assertEquals(java.util.EnumSet.allOf(RWScheduler.Pool.class), seenPools);
	}

	private static RWScheduler.Pool expectedPool(WorkloadProfile profile, OperationFamily family) {
		if (profile == WorkloadProfile.CONTROL) return RWScheduler.Pool.CONTROL;
		if (profile == WorkloadProfile.PHYSICAL_MAINTENANCE) return RWScheduler.Pool.PHYSICAL;
		return switch (family) {
			case MUTATION, FLUSH -> RWScheduler.Pool.WRITE;
			case CONTROL -> RWScheduler.Pool.CONTROL;
			case COMPACTION -> RWScheduler.Pool.PHYSICAL;
			case METADATA, POINT_LOOKUP, BOUNDARY_SEEK, BOUNDED_FAN_OUT,
					RANGE_PAGE, FULL_SCAN_AGGREGATE, WAL_PAGE -> RWScheduler.Pool.READ;
		};
	}

	@Test
	void latencyUsesEarliestDeadlineFirst() throws Exception {
		var scheduler = scheduler(1, "edf-test");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var completed = new CountDownLatch(2);
		var order = Collections.synchronizedList(new ArrayList<String>());
		try {
			scheduler.executor(RequestContext.latency(Duration.ofSeconds(30)), OperationFamily.POINT_LOOKUP)
					.execute(() -> {
						blockerStarted.countDown();
						awaitUninterruptibly(release);
					});
			assertTrue(blockerStarted.await(5, SECONDS));
			scheduler.executor(RequestContext.latency(Duration.ofSeconds(20)), OperationFamily.POINT_LOOKUP)
					.execute(() -> record("later", order, completed));
			scheduler.executor(RequestContext.latency(Duration.ofSeconds(10)), OperationFamily.POINT_LOOKUP)
					.execute(() -> record("earlier", order, completed));
			release.countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEquals(List.of("earlier", "later"), order);
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void queuedDeadlineCompletesExceptionallyWithoutRunning() throws Exception {
		var scheduler = scheduler(1, "queued-deadline");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var expired = new ObservableTask(() -> {});
		try {
			scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE).execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(release);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			var deadline = new RequestContext(WorkloadProfile.LATENCY, System.currentTimeMillis() + 100L);
			scheduler.executor(deadline, OperationFamily.POINT_LOOKUP).execute(expired);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 1);
			Thread.sleep(150L);
			release.countDown();

			var completion = assertThrows(ExecutionException.class, () -> expired.get(5, SECONDS));
			var failure = assertInstanceOf(RocksDBException.class, completion.getCause());
			assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,
					failure.getErrorUniqueId());
			assertFalse(expired.ran());
			assertEquals(1, expired.disposeCount());
			assertEquals(1L,
					scheduler.poolSnapshot(RWScheduler.Pool.READ)
							.outcomes()
							.get(RWScheduler.TerminalOutcome.DEADLINE));
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void latencyBurstIsBoundedBeforeGuaranteedWorkRuns() throws Exception {
		var scheduler = scheduler(1, "latency-burst");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var order = Collections.synchronizedList(new ArrayList<String>());
		var completed = new CountDownLatch(10);
		try {
			var latency = scheduler.executor(RequestContext.latency(Duration.ofSeconds(30)),
					OperationFamily.POINT_LOOKUP);
			latency.execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(release);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			for (int i = 0; i < 9; i++) {
				latency.execute(() -> record("latency", order, completed));
			}
			scheduler.executor(RequestContext.ingest(), OperationFamily.POINT_LOOKUP)
					.execute(() -> record("ingest", order, completed));
			release.countDown();
			assertTrue(completed.await(5, SECONDS));
			int ingestIndex = order.indexOf("ingest");
			assertTrue(ingestIndex >= 0 && ingestIndex <= 8,
					"guaranteed work must be reconsidered after at most eight LATENCY tasks");
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void reservationsWinTheNextSlotFromBlockingBorrowers() throws Exception {
		var scheduler = scheduler(3, "reservation-test");
		var batchStarted = new CountDownLatch(2);
		var releaseBatch = new CountDownLatch(1);
		var analyticalStarted = new CountDownLatch(1);
		var releaseAnalytical = new CountDownLatch(1);
		var reservedDone = new CountDownLatch(3);
		var order = Collections.synchronizedList(new ArrayList<String>());
		try {
			var batch = scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE);
			for (int i = 0; i < 2; i++) {
				batch.execute(() -> {
					batchStarted.countDown();
					awaitUninterruptibly(releaseBatch);
				});
			}
			assertTrue(batchStarted.await(5, SECONDS));
			scheduler.executor(RequestContext.analytical(), OperationFamily.FULL_SCAN_AGGREGATE)
					.execute(() -> {
						analyticalStarted.countDown();
						awaitUninterruptibly(releaseAnalytical);
					});
			assertTrue(analyticalStarted.await(5, SECONDS));

			batch.execute(() -> record("borrowed-batch", order, new CountDownLatch(0)));
			scheduler.executor(RequestContext.latency(Duration.ofSeconds(30)), OperationFamily.POINT_LOOKUP)
					.execute(() -> record("latency", order, reservedDone));
			scheduler.executor(RequestContext.ingest(), OperationFamily.POINT_LOOKUP)
					.execute(() -> record("ingest", order, reservedDone));
			scheduler.executor(WorkloadProfile.CDC, OperationFamily.WAL_PAGE, RequestContext.NO_DEADLINE)
					.execute(() -> record("cdc", order, reservedDone));

			releaseAnalytical.countDown();
			assertTrue(reservedDone.await(5, SECONDS));
			List<String> firstThree;
			synchronized (order) {
				assertTrue(order.size() >= 3);
				firstThree = List.copyOf(order.subList(0, 3));
			}
			assertEquals(new HashSet<>(List.of("latency", "ingest", "cdc")),
					new HashSet<>(firstThree));
		} finally {
			releaseAnalytical.countDown();
			releaseBatch.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void idleReservationsAreBorrowedWithoutExceedingTheHardCapacity() throws Exception {
		var scheduler = scheduler(3, "borrowing-test");
		var started = new CountDownLatch(3);
		var release = new CountDownLatch(1);
		try {
			var batch = scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE);
			for (int i = 0; i < 3; i++) {
				batch.execute(() -> {
					started.countDown();
					awaitUninterruptibly(release);
				});
			}
			assertTrue(started.await(5, SECONDS));
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(3, snapshot.workerCount());
			assertEquals(3, snapshot.activeTasks());
			assertEquals(0, snapshot.waitingWorkers());
			assertEquals(3, snapshot.activeByProfile().get(WorkloadProfile.BATCH));
			release.countDown();
			assertEventually(() -> {
				var idle = scheduler.poolSnapshot(RWScheduler.Pool.READ);
				return idle.activeTasks() == 0 && idle.queuedTasks() == 0 && idle.waitingWorkers() == 3;
			});
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void drrDeliversConfiguredSharesForUnitCostWork() throws Exception {
		var scheduler = scheduler(1, "drr-shares");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var completed = new CountDownLatch(33);
		var order = Collections.synchronizedList(new ArrayList<WorkloadProfile>());
		try {
			scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE).execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(release);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			enqueue(scheduler, WorkloadProfile.INGEST, OperationFamily.POINT_LOOKUP, 12, 0L, order, completed);
			enqueue(scheduler, WorkloadProfile.CDC, OperationFamily.WAL_PAGE, 12, 0L, order, completed);
			enqueue(scheduler, WorkloadProfile.ANALYTICAL, OperationFamily.FULL_SCAN_AGGREGATE,
					6, 0L, order, completed);
			enqueue(scheduler, WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, 3, 0L, order, completed);
			release.countDown();
			assertTrue(completed.await(5, SECONDS));

			var firstRound = frequencies(order.subList(0, 11));
			assertEquals(4, firstRound.get(WorkloadProfile.INGEST));
			assertEquals(4, firstRound.get(WorkloadProfile.CDC));
			assertEquals(2, firstRound.get(WorkloadProfile.ANALYTICAL));
			assertEquals(1, firstRound.get(WorkloadProfile.BATCH));
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void drrChargesDeclaredByteCost() throws Exception {
		var scheduler = scheduler(1, "drr-cost");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var completed = new CountDownLatch(8);
		var order = Collections.synchronizedList(new ArrayList<WorkloadProfile>());
		try {
			scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE).execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(release);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			enqueue(scheduler,
					WorkloadProfile.INGEST,
					OperationFamily.POINT_LOOKUP,
					4,
					8L * 1024L * 1024L,
					order,
					completed);
			enqueue(scheduler, WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, 4, 1L, order, completed);
			release.countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEquals(List.of(
					WorkloadProfile.INGEST,
					WorkloadProfile.BATCH,
					WorkloadProfile.INGEST,
					WorkloadProfile.BATCH), order.subList(0, 4));
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void taskCostUsesTwoMiBQuantaAndCapsAtSixteen() {
		long twoMiB = 2L * 1024L * 1024L;
		assertEquals(1, RWScheduler.taskCost(0L));
		assertEquals(1, RWScheduler.taskCost(1L));
		assertEquals(1, RWScheduler.taskCost(twoMiB));
		assertEquals(2, RWScheduler.taskCost(twoMiB + 1L));
		assertEquals(16, RWScheduler.taskCost(32L * 1024L * 1024L));
		assertEquals(16, RWScheduler.taskCost(Long.MAX_VALUE));
		assertThrows(IllegalArgumentException.class, () -> RWScheduler.taskCost(-1L));
	}

	@Test
	void storagePressureParksPhysicalAndGloballySpacesBatchAfterCompletion() throws Exception {
		var scheduler = scheduler(3, "pressure-test");
		var physical = new ObservableTask(() -> {});
		var firstStarted = new CountDownLatch(1);
		var releaseFirst = new CountDownLatch(1);
		var firstFinishedNanos = new AtomicLong();
		var secondStartedNanos = new AtomicLong();
		var secondStarted = new CountDownLatch(1);
		try {
			scheduler.setStoragePressure(true);
			scheduler.maintenanceExecutor().execute(physical);
			scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE).execute(() -> {
				firstStarted.countDown();
				awaitUninterruptibly(releaseFirst);
				firstFinishedNanos.set(System.nanoTime());
			});
			assertTrue(firstStarted.await(5, SECONDS));
			scheduler.executor(RequestContext.batch(), OperationFamily.MUTATION).execute(() -> {
				secondStartedNanos.set(System.nanoTime());
				secondStarted.countDown();
			});
			assertEquals(1, scheduler.activeTasks(WorkloadProfile.BATCH));
			assertFalse(physical.ran());

			releaseFirst.countDown();
			assertTrue(secondStarted.await(3, SECONDS));
			assertTrue(secondStartedNanos.get() - firstFinishedNanos.get()
					>= TimeUnit.MILLISECONDS.toNanos(900),
					"pressured BATCH dispatches must be separated by one second after completion");
			assertFalse(physical.ran());

			scheduler.setStoragePressure(false);
			physical.get(5, SECONDS);
			assertTrue(physical.ran());
			assertFalse(scheduler.admissionSnapshot().storagePressure());
		} finally {
			releaseFirst.countDown();
			scheduler.setStoragePressure(false);
			scheduler.dispose();
		}
	}

	@Test
	void pressuredFairTurnSkipsQueuedPeerUntilThatPoolHasAFreeWorker() throws Exception {
		var scheduler = scheduler(1, "pressure-dispatchable-peer");
		var writeForegroundStarted = new CountDownLatch(1);
		var releaseWriteForeground = new CountDownLatch(1);
		var readBatchStarts = new CountDownLatch(2);
		var writeBatchStarted = new CountDownLatch(1);
		try {
			scheduler.setStoragePressure(true);
			scheduler.executor(WorkloadProfile.INGEST,
					OperationFamily.MUTATION,
					RequestContext.NO_DEADLINE).execute(() -> {
				writeForegroundStarted.countDown();
				awaitUninterruptibly(releaseWriteForeground);
			});
			assertTrue(writeForegroundStarted.await(5, SECONDS));
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.MUTATION,
					RequestContext.NO_DEADLINE).execute(writeBatchStarted::countDown);

			var readBatch = scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE);
			readBatch.execute(readBatchStarts::countDown);
			readBatch.execute(readBatchStarts::countDown);

			assertTrue(readBatchStarts.await(2_500L, TimeUnit.MILLISECONDS),
					"READ must keep its pressure turn while queued WRITE has no available worker");
			assertEquals(1L, writeBatchStarted.getCount());

			releaseWriteForeground.countDown();
			assertTrue(writeBatchStarted.await(2_500L, TimeUnit.MILLISECONDS),
					"WRITE must receive a bounded turn as soon as it becomes dispatchable");
		} finally {
			releaseWriteForeground.countDown();
			scheduler.setStoragePressure(false);
			scheduler.disposeNow();
		}
	}

	@Test
	void losingPeerDispatchabilityWakesPoolWaitingForFairPressureTurn() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 2, 1, 16, 16,
				"pressure-dispatchability-transition");
		var firstReadStarted = new CountDownLatch(1);
		var releaseFirstRead = new CountDownLatch(1);
		var secondReadStarted = new CountDownLatch(1);
		var firstWriteForegroundStarted = new CountDownLatch(1);
		var secondWriteForegroundStarted = new CountDownLatch(1);
		var releaseWriteForeground = new CountDownLatch(1);
		var writeBatchStarted = new CountDownLatch(1);
		try {
			scheduler.setStoragePressure(true);
			scheduler.executor(WorkloadProfile.INGEST,
					OperationFamily.MUTATION,
					RequestContext.NO_DEADLINE).execute(() -> {
				firstWriteForegroundStarted.countDown();
				awaitUninterruptibly(releaseWriteForeground);
			});
			assertTrue(firstWriteForegroundStarted.await(5, SECONDS));
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE).execute(() -> {
				firstReadStarted.countDown();
				awaitUninterruptibly(releaseFirstRead);
			});
			assertTrue(firstReadStarted.await(5, SECONDS));

			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.MUTATION,
					RequestContext.NO_DEADLINE).execute(writeBatchStarted::countDown);
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE).execute(secondReadStarted::countDown);
			releaseFirstRead.countDown();
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.outcomes().get(RWScheduler.TerminalOutcome.RUN) == 1L);
			assertEquals(1, scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks());
			assertEquals(1, scheduler.poolSnapshot(RWScheduler.Pool.WRITE).activeTasks());
			assertEquals(1, scheduler.poolSnapshot(RWScheduler.Pool.WRITE)
					.queuedByProfile().get(WorkloadProfile.BATCH));
			assertTrue(scheduler.isBatchDispatchableForTesting(RWScheduler.Pool.WRITE));
			assertFalse(scheduler.hasFairPressureTurnForTesting(RWScheduler.Pool.READ),
					"READ must be waiting specifically for WRITE's published fair turn");
			assertEquals(1L, secondReadStarted.getCount());
			assertEquals(1L, writeBatchStarted.getCount());

			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.MUTATION,
					RequestContext.NO_DEADLINE).execute(() -> {
				secondWriteForegroundStarted.countDown();
				awaitUninterruptibly(releaseWriteForeground);
			});
			assertTrue(secondWriteForegroundStarted.await(5, SECONDS));
			assertEquals(2, scheduler.poolSnapshot(RWScheduler.Pool.WRITE).activeTasks());
			assertFalse(scheduler.isBatchDispatchableForTesting(RWScheduler.Pool.WRITE));
			assertTrue(scheduler.hasFairPressureTurnForTesting(RWScheduler.Pool.READ));
			assertTrue(secondReadStarted.await(2_500L, TimeUnit.MILLISECONDS),
					"READ's indefinite fair-turn wait must be signaled when WRITE becomes nondispatchable");
			assertEquals(1L, writeBatchStarted.getCount());
		} finally {
			releaseFirstRead.countDown();
			releaseWriteForeground.countDown();
			scheduler.setStoragePressure(false);
			scheduler.disposeNow();
		}
	}

	@Test
	void cancellationImmediatelyBeforeRunDoesNotConsumeAPressureInterval() throws Exception {
		var scheduler = scheduler(1, "pressure-cancel-test");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var nextStarted = new CountDownLatch(1);
		var batchView = scheduler.scheduler(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		try {
			scheduler.setStoragePressure(true);
			scheduler.executor(RequestContext.latency(Duration.ofSeconds(5)), OperationFamily.POINT_LOOKUP)
					.execute(() -> {
						blockerStarted.countDown();
						awaitUninterruptibly(releaseBlocker);
					});
			assertTrue(blockerStarted.await(5, SECONDS));
			var cancelled = batchView.schedule(() -> {
			});
			cancelled.dispose();
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION) == 1L);

			releaseBlocker.countDown();
			scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE)
					.execute(nextStarted::countDown);
			assertTrue(nextStarted.await(800L, TimeUnit.MILLISECONDS),
					"a BATCH task that never ran must not start the one-second pressure interval");
		} finally {
			releaseBlocker.countDown();
			batchView.dispose();
			scheduler.setStoragePressure(false);
			scheduler.dispose();
		}
	}

	@Test
	void queueOverloadAndCancellationHaveOneTerminalOutcomeAndMetric() throws Exception {
		var registry = new SimpleMeterRegistry();
		var scheduler = RWScheduler.forTesting(1, 1, 1, 1, 1, "terminal-test", registry, "terminal-db");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var queued = new ObservableTask(() -> {});
		var overloaded = new ObservableTask(() -> {});
		var view = scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE);
		try {
			view.execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(release);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			view.execute(queued);
			var failure = assertThrows(RocksDBException.class, () -> view.execute(overloaded));
			assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED, failure.getErrorUniqueId());
			assertTrue(overloaded.isCompletedExceptionally());
			assertEquals(1, overloaded.disposeCount());

			assertTrue(queued.cancel(false));
			assertTrue(scheduler.removeQueuedTask(view, queued));
			assertFalse(scheduler.removeQueuedTask(view, queued));
			assertEquals(0, scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks());
			assertEquals(1L,
					scheduler.poolSnapshot(RWScheduler.Pool.READ)
							.outcomes()
							.get(RWScheduler.TerminalOutcome.CANCELLATION));
			assertEquals(1.0,
					registry.get("rockserver.workload.cancellations")
							.tags("database", "terminal-db",
									"resource", "read",
									"profile", "batch",
									"operation", "range_page")
							.counter()
							.count());
		} finally {
			release.countDown();
			scheduler.dispose();
			registry.close();
		}
	}

	@Test
	void directQueuedCancellationRacingShutdownPublishesOneCallbackOutsideThePoolLock() throws Exception {
		for (int repetition = 0; repetition < 32; repetition++) {
			var scheduler = scheduler(1, "direct-terminal-race-" + repetition);
			var blockerStarted = new CountDownLatch(1);
			var releaseBlocker = new CountDownLatch(1);
			var task = new LockCheckingLifecycleTask(scheduler);
			var view = scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE);
			try {
				view.execute(() -> {
					blockerStarted.countDown();
					try {
						releaseBlocker.await();
					} catch (InterruptedException expectedDuringForcedShutdown) {
						Thread.currentThread().interrupt();
					}
				});
				assertTrue(blockerStarted.await(5, SECONDS));
				view.execute(task);
				assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 1);

				var raceStart = new CountDownLatch(1);
				var cancellation = CompletableFuture.runAsync(() -> {
					awaitUninterruptibly(raceStart);
					scheduler.removeQueuedTask(view, task);
				});
				var shutdown = CompletableFuture.runAsync(() -> {
					awaitUninterruptibly(raceStart);
					scheduler.disposeNow();
				});
				raceStart.countDown();
				cancellation.get(15, SECONDS);
				shutdown.get(15, SECONDS);

				assertEquals(1, task.rejectionCount());
				assertEquals(1, task.disposeCount());
				assertFalse(task.ran());
				assertTrue(task.callbackOutsideLock());
				assertNull(task.callbackFailure());
				var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
				assertEquals(1L,
						snapshot.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION)
								+ snapshot.outcomes().get(RWScheduler.TerminalOutcome.SHUTDOWN));
				assertTrue(snapshot.drainedAndConserved());
			} finally {
				releaseBlocker.countDown();
				scheduler.disposeNow();
			}
		}
	}

	@Test
	void taskFailureIsLoggedAndMetricizedOnce() throws Exception {
		var registry = new SimpleMeterRegistry();
		var scheduler = RWScheduler.forTesting(1, 1, 1, 8, 8, "failure-test", registry, "failure-db");
		try {
			scheduler.readExecutor().execute(() -> {
				throw new IllegalStateException("expected test failure");
			});
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).completedTasks() == 1L);
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(1L, snapshot.failedTasks());
			assertEquals(1L, snapshot.outcomes().get(RWScheduler.TerminalOutcome.FAILURE));
			assertEquals(0L, snapshot.outcomes().get(RWScheduler.TerminalOutcome.RUN));
			assertEquals(1.0,
					registry.get("rockserver.workload.failures")
							.tags("database", "failure-db",
									"resource", "read",
									"profile", "batch",
									"operation", "range_page")
							.counter()
							.count());
		} finally {
			scheduler.dispose();
			registry.close();
		}
	}

	@Test
	void queuedRemovalUsesIdentityEvenWhenTasksCompareEqual() throws Exception {
		var scheduler = scheduler(1, "identity-removal-test");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var first = new EqualTask();
		var second = new EqualTask();
		var view = scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE);
		try {
			view.execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(release);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			view.execute(first);
			view.execute(second);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 2);

			assertTrue(scheduler.removeQueuedTask(view, second));
			release.countDown();
			first.get(5, SECONDS);
			assertTrue(first.ran());
			assertFalse(second.ran());
			assertTrue(second.isCompletedExceptionally());
			assertEquals(1, second.disposeCount());
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void queuedRemovalPreservesDuplicateIdentitySubmissionOrder() throws Exception {
		var scheduler = scheduler(1, "duplicate-identity-removal-test");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var runs = new AtomicInteger();
		Runnable duplicate = runs::incrementAndGet;
		var view = scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE);
		try {
			scheduler.executor(RequestContext.latency(Duration.ofSeconds(5)), OperationFamily.POINT_LOOKUP)
					.execute(() -> {
						blockerStarted.countDown();
						awaitUninterruptibly(release);
					});
			assertTrue(blockerStarted.await(5, SECONDS));
			view.execute(duplicate);
			view.execute(duplicate);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 2);

			assertTrue(scheduler.removeQueuedTask(view, duplicate));
			assertTrue(scheduler.removeQueuedTask(view, duplicate));
			assertFalse(scheduler.removeQueuedTask(view, duplicate));
			assertEquals(0, scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks());
			assertEquals(2L, scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION));
			assertEquals(0, runs.get());
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void cancellationIndexRehashesAndReusesEntriesWithoutLosingIdentity() throws Exception {
		var scheduler = scheduler(1, "cancellation-index-reuse-test");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var view = scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE);
		try {
			scheduler.executor(RequestContext.latency(Duration.ofSeconds(5)), OperationFamily.POINT_LOOKUP)
					.execute(() -> {
						blockerStarted.countDown();
						awaitUninterruptibly(release);
					});
			assertTrue(blockerStarted.await(5, SECONDS));
			for (int wave = 0; wave < 3; wave++) {
				var commands = new ArrayList<Runnable>(64);
				for (int index = 0; index < 64; index++) {
					int identity = wave * 64 + index;
					Runnable command = () -> {
						if (identity < 0) throw new AssertionError("unreachable");
					};
					commands.add(command);
					view.execute(command);
				}
				assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 64);
				for (var command : commands) {
					assertTrue(scheduler.removeQueuedTask(view, command));
					assertFalse(scheduler.removeQueuedTask(view, command));
				}
				assertEquals(0, scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks());
			}
			assertEquals(192L, scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION));
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void cancellationIdentityIsScopedByProfileAndOperationFamily() throws Exception {
		var scheduler = scheduler(1, "cancellation-index-scope-test");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var ran = new CountDownLatch(1);
		Runnable shared = ran::countDown;
		var batch = scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE);
		var analytical = scheduler.executor(RequestContext.analytical(),
				OperationFamily.FULL_SCAN_AGGREGATE);
		try {
			scheduler.executor(RequestContext.latency(Duration.ofSeconds(5)), OperationFamily.POINT_LOOKUP)
					.execute(() -> {
						blockerStarted.countDown();
						awaitUninterruptibly(release);
					});
			assertTrue(blockerStarted.await(5, SECONDS));
			batch.execute(shared);
			analytical.execute(shared);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 2);

			assertTrue(scheduler.removeQueuedTask(batch, shared));
			assertFalse(scheduler.removeQueuedTask(batch, shared));
			release.countDown();
			assertTrue(ran.await(5, SECONDS));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).outstandingTasks() == 0);
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void forcedShutdownCompletesQueuedWorkDisposesItAndInterruptsRunningWork() throws Exception {
		var scheduler = scheduler(1, "forced-shutdown");
		var started = new CountDownLatch(1);
		var interrupted = new AtomicBoolean();
		var queued = new LifecycleTask(true);
		var reactorRan = new AtomicBoolean();
		scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE).execute(() -> {
			started.countDown();
			try {
				new CountDownLatch(1).await();
			} catch (InterruptedException expected) {
				interrupted.set(true);
				Thread.currentThread().interrupt();
			}
		});
		assertTrue(started.await(5, SECONDS));
		scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE).execute(queued);
		var reactorTask = scheduler.scheduler(RequestContext.batch(), OperationFamily.RANGE_PAGE)
				.schedule(() -> reactorRan.set(true));
		assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 2);

		scheduler.disposeNow();

		var completion = assertThrows(ExecutionException.class, () -> queued.get(5, SECONDS));
		assertInstanceOf(RejectedExecutionException.class, completion.getCause());
		assertFalse(queued.ran());
		assertEquals(1, queued.rejectionCount());
		assertEquals(1, queued.disposeCount());
		assertTrue(reactorTask.isDisposed());
		assertFalse(reactorRan.get());
		assertTrue(interrupted.get());
		var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
		assertEquals(2L, snapshot.outcomes().get(RWScheduler.TerminalOutcome.SHUTDOWN));
		assertEquals(1L, snapshot.failedTasks());
		assertTrue(snapshot.terminated());
		assertThrows(RejectedExecutionException.class,
				() -> scheduler.readExecutor().execute(() -> {}));
	}

	@Test
	void reentrancyBelongsToTheOwningSchedulerOnly() throws Exception {
		var first = scheduler(1, "reentrant-first");
		var second = scheduler(1, "reentrant-second");
		var result = new CompletableFuture<List<Boolean>>();
		try {
			first.readExecutor().execute(() -> result.complete(List.of(
					first.isExecutingWorkloadTask(),
					second.isExecutingWorkloadTask())));
			assertEquals(List.of(true, false), result.get(5, SECONDS));
			assertFalse(first.isExecutingWorkloadTask());
			assertFalse(second.isExecutingWorkloadTask());
		} finally {
			first.dispose();
			second.dispose();
		}
	}

	@Test
	void oneThreadFactoryProducesUniqueNamesAndNoExtraCdcWorker() throws Exception {
		var scheduler = scheduler(3, "thread-name-test");
		var started = new CountDownLatch(3);
		var release = new CountDownLatch(1);
		try {
			var cdc = scheduler.executor(WorkloadProfile.CDC,
					OperationFamily.WAL_PAGE,
					RequestContext.NO_DEADLINE);
			for (int i = 0; i < 3; i++) {
				cdc.execute(() -> {
					started.countDown();
					awaitUninterruptibly(release);
				});
			}
			assertTrue(started.await(5, SECONDS));
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(3, snapshot.workerCount());
			assertEquals(3, snapshot.workerThreadNames().size());
			assertEquals(3, new HashSet<>(snapshot.workerThreadNames()).size());
			assertTrue(snapshot.workerThreadNames().stream()
					.allMatch(name -> name.startsWith("thread-name-test-read-")));
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void controlIsIsolatedAndGracefulShutdownDrainsAcceptedControl() throws Exception {
		var scheduler = scheduler(3, "control-test");
		var dataStarted = new CountDownLatch(3);
		var release = new CountDownLatch(1);
		var controlDone = new CountDownLatch(1);
		try {
			for (int i = 0; i < 3; i++) {
				scheduler.readExecutor().execute(() -> {
					dataStarted.countDown();
					awaitUninterruptibly(release);
				});
			}
			assertTrue(dataStarted.await(5, SECONDS));
			scheduler.controlExecutor().execute(controlDone::countDown);
			assertTrue(controlDone.await(2, SECONDS));
		} finally {
			release.countDown();
			scheduler.dispose();
		}
		assertTrue(scheduler.poolSnapshot(RWScheduler.Pool.CONTROL).terminated());
	}

	@Test
	void competingForegroundCapsGlobalBatchThenImmediatelyReclaimsIdleWorkers() throws Exception {
		var scheduler = RWScheduler.forTesting(6, 6, 1, 128, 128, "global-batch-contention");
		var foregroundStarted = new CountDownLatch(1);
		var releaseForeground = new CountDownLatch(1);
		var firstBatchStarted = new CountDownLatch(1);
		var allBatchStarted = new CountDownLatch(6);
		var releaseBatch = new CountDownLatch(1);
		var batchStarts = new AtomicInteger();
		try {
			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					System.currentTimeMillis() + SECONDS.toMillis(30)).execute(() -> {
				foregroundStarted.countDown();
				awaitUninterruptibly(releaseForeground);
			});
			assertTrue(foregroundStarted.await(5, SECONDS));

			var batch = scheduler.executor(
					WorkloadProfile.BATCH, OperationFamily.MUTATION, RequestContext.NO_DEADLINE);
			for (int i = 0; i < 6; i++) {
				batch.execute(() -> {
					batchStarts.incrementAndGet();
					firstBatchStarted.countDown();
					allBatchStarted.countDown();
					awaitUninterruptibly(releaseBatch);
				});
			}
			assertTrue(firstBatchStarted.await(5, SECONDS));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.WRITE).batchDispatchLimited());
			assertEquals(1, batchStarts.get(),
					"foreground competition must limit write-side BATCH without reducing read-side SST parallelism");
			assertEquals(1,
					scheduler.poolSnapshot(RWScheduler.Pool.WRITE)
							.activeByProfile().get(WorkloadProfile.BATCH));

			releaseForeground.countDown();
			assertTrue(allBatchStarted.await(5, SECONDS),
					"every idle write worker must be reclaimed as soon as global competition drains");
			assertEquals(6, batchStarts.get());
			assertFalse(scheduler.poolSnapshot(RWScheduler.Pool.WRITE).batchDispatchLimited());
		} finally {
			releaseForeground.countDown();
			releaseBatch.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void competingBatchWritesArePacedWithoutDelayingBatchOnlyWork() throws Exception {
		var scheduler = RWScheduler.forTesting(6, 6, 1, 128, 128, "competing-batch-pacing");
		var foregroundStarted = new CountDownLatch(1);
		var releaseForeground = new CountDownLatch(1);
		var pacedDone = new CountDownLatch(3);
		var pacedStarts = Collections.synchronizedList(new ArrayList<Long>());
		try {
			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					System.currentTimeMillis() + SECONDS.toMillis(30)).execute(() -> {
				foregroundStarted.countDown();
				awaitUninterruptibly(releaseForeground);
			});
			assertTrue(foregroundStarted.await(5, SECONDS));
			var batch = scheduler.executor(
					WorkloadProfile.BATCH, OperationFamily.MUTATION, RequestContext.NO_DEADLINE);
			for (int i = 0; i < 3; i++) {
				batch.execute(() -> {
					pacedStarts.add(System.nanoTime());
					pacedDone.countDown();
				});
			}
			assertTrue(pacedDone.await(5, SECONDS));
			for (int i = 1; i < pacedStarts.size(); i++) {
				assertTrue(pacedStarts.get(i) - pacedStarts.get(i - 1)
						>= TimeUnit.MICROSECONDS.toNanos(800),
						"competing write-side BATCH starts must observe the one-millisecond interval");
			}

			releaseForeground.countDown();
			var unpacedStarted = new CountDownLatch(6);
			var releaseUnpaced = new CountDownLatch(1);
			for (int i = 0; i < 6; i++) {
				batch.execute(() -> {
					unpacedStarted.countDown();
					awaitUninterruptibly(releaseUnpaced);
				});
			}
			try {
				assertTrue(unpacedStarted.await(5, SECONDS),
						"BATCH-only work must immediately recover unpaced write-pool parallelism");
			} finally {
				releaseUnpaced.countDown();
			}
		} finally {
			releaseForeground.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void abortedBatchPermitReopensCapacityWithoutAdvancingPacing() throws Exception {
		var controllerType = Class.forName(
				"it.cavallium.rockserver.core.impl.WorkloadPressureController");
		var constructor = controllerType.getDeclaredConstructor(
				int.class,
				int.class,
				Duration.class,
				int.class,
				Duration.class,
				Duration.class);
		constructor.setAccessible(true);
		var controller = constructor.newInstance(
				1,
				1,
				Duration.ofSeconds(10),
				1,
				Duration.ofSeconds(10),
				Duration.ofSeconds(10));

		var setPressured = controllerType.getDeclaredMethod("setPressured", boolean.class);
		var setNotifier = controllerType.getDeclaredMethod("setNotifier", Runnable.class);
		var setBatchNotifier = controllerType.getDeclaredMethod("setBatchNotifier", IntConsumer.class);
		var setBatchQueued = controllerType.getDeclaredMethod(
				"setBatchQueued", RWScheduler.Pool.class, boolean.class);
		var signalPendingAvailability = controllerType.getDeclaredMethod("signalPendingAvailability");
		var tryStartBatch = controllerType.getDeclaredMethod(
				"tryStartBatch", boolean.class, RWScheduler.Pool.class, long.class);
		var batchStartAllowance = controllerType.getDeclaredMethod(
				"batchStartAllowance", boolean.class, RWScheduler.Pool.class, long.class);
		var abortBatch = controllerType.getDeclaredMethod(
				"abortBatch", long.class, RWScheduler.Pool.class);
		for (var method : List.of(
				setPressured,
				setNotifier,
				setBatchNotifier,
				setBatchQueued,
				signalPendingAvailability,
				tryStartBatch,
				batchStartAllowance,
				abortBatch)) {
			method.setAccessible(true);
		}

		setPressured.invoke(controller, true);
		var deferredNotifications = new AtomicInteger();
		var directBatchNotifications = new AtomicInteger();
		setNotifier.invoke(controller, (Runnable) deferredNotifications::incrementAndGet);
		setBatchNotifier.invoke(controller, (IntConsumer) _ -> directBatchNotifications.incrementAndGet());
		setBatchQueued.invoke(controller, RWScheduler.Pool.WRITE, true);
		long fixedNow = System.nanoTime();
		long firstPermit = (long) tryStartBatch.invoke(controller, false, RWScheduler.Pool.READ, fixedNow);
		assertTrue(firstPermit != 0L);
		assertEquals(0,
				batchStartAllowance.invoke(controller, false, RWScheduler.Pool.READ, fixedNow));

		abortBatch.invoke(controller, firstPermit, RWScheduler.Pool.READ);
		assertEquals(0, directBatchNotifications.get(),
				"permit abort must not invoke another pool while the caller holds its scheduler lock");
		assertEquals(0, deferredNotifications.get());
		assertEquals(1,
				batchStartAllowance.invoke(controller, false, RWScheduler.Pool.READ, fixedNow),
				"a cancelled pre-dispatch permit must not consume the pressure interval");
		signalPendingAvailability.invoke(controller);
		assertEquals(1, deferredNotifications.get(),
				"the other queued pool must be woken only through the post-unlock notifier");

		long replacementPermit = (long) tryStartBatch.invoke(controller, false, RWScheduler.Pool.READ, fixedNow);
		assertTrue(replacementPermit != 0L,
				"capacity must reopen immediately at the same scheduler timestamp");
		abortBatch.invoke(controller, replacementPermit, RWScheduler.Pool.READ);
		assertEquals(0, directBatchNotifications.get());
		assertEquals(1, deferredNotifications.get());
		signalPendingAvailability.invoke(controller);
		assertEquals(2, deferredNotifications.get());
	}

	@Test
	void productionConstructorsRequireAllThreeReservationsAndExposeNoUnmanagedVariant() {
		assertThrows(IllegalArgumentException.class, () -> new RWScheduler(2, 3, "too-small-read"));
		assertThrows(IllegalArgumentException.class, () -> new RWScheduler(3, 2, "too-small-write"));
		assertTrue(java.util.Arrays.stream(RWScheduler.class.getConstructors())
				.noneMatch(constructor -> java.util.Arrays.stream(constructor.getParameterTypes())
						.anyMatch(type -> type.getName().equals("reactor.core.scheduler.Scheduler"))));
	}

	@Test
	void instrumentationReportsZeroBatchAllowanceForIsolatedPools() {
		var scheduler = scheduler(3, "isolated-instrumentation");
		try {
			var snapshot = scheduler.instrumentationSnapshot();
			assertEquals(0, snapshot.pools().get(RWScheduler.Pool.CONTROL).batchStartAllowance());
			assertEquals(0, snapshot.pools().get(RWScheduler.Pool.PHYSICAL).batchStartAllowance());
			assertFalse(snapshot.pools().get(RWScheduler.Pool.CONTROL).batchDispatchLimited());
			assertFalse(snapshot.pools().get(RWScheduler.Pool.PHYSICAL).batchDispatchLimited());
		} finally {
			scheduler.disposeNow();
		}
	}

	private static RWScheduler scheduler(int threads, String name) {
		return RWScheduler.forTesting(threads, threads, 1, 128, 128, name);
	}

	private static void enqueue(RWScheduler scheduler,
			WorkloadProfile profile,
			OperationFamily family,
			int count,
			long estimatedBytes,
			List<WorkloadProfile> order,
			CountDownLatch completed) {
		var executor = scheduler.executor(profile, family, RequestContext.NO_DEADLINE);
		for (int i = 0; i < count; i++) {
			executor.execute(() -> {
				order.add(profile);
				completed.countDown();
			}, estimatedBytes);
		}
	}

	private static Map<WorkloadProfile, Integer> frequencies(List<WorkloadProfile> profiles) {
		var result = new HashMap<WorkloadProfile, Integer>();
		for (var profile : profiles) {
			result.merge(profile, 1, Integer::sum);
		}
		return result;
	}

	private static void record(String value, List<String> order, CountDownLatch completed) {
		order.add(value);
		completed.countDown();
	}

	private static void assertEventually(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + SECONDS.toNanos(5);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(5L);
		}
		assertTrue(condition.getAsBoolean());
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
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

	private static final class ObservableTask extends CompletableFuture<Void> implements Runnable, Disposable {

		private final Runnable action;
		private final AtomicBoolean ran = new AtomicBoolean();
		private final AtomicInteger disposeCount = new AtomicInteger();

		private ObservableTask(Runnable action) {
			this.action = action;
		}

		@Override
		public void run() {
			ran.set(true);
			try {
				action.run();
				complete(null);
			} catch (Throwable failure) {
				completeExceptionally(failure);
			}
		}

		@Override
		public void dispose() {
			disposeCount.incrementAndGet();
		}

		@Override
		public boolean isDisposed() {
			return disposeCount.get() > 0;
		}

		private boolean ran() {
			return ran.get();
		}

		private int disposeCount() {
			return disposeCount.get();
		}
	}

	private static final class EqualTask extends CompletableFuture<Void> implements Runnable, Disposable {

		private final AtomicBoolean ran = new AtomicBoolean();
		private final AtomicInteger disposeCount = new AtomicInteger();

		@Override
		public void run() {
			ran.set(true);
			complete(null);
		}

		@Override
		public void dispose() {
			disposeCount.incrementAndGet();
		}

		@Override
		public boolean isDisposed() {
			return disposeCount.get() > 0;
		}

		@Override
		public boolean equals(Object ignored) {
			return ignored instanceof EqualTask;
		}

		@Override
		public int hashCode() {
			return 1;
		}

		private boolean ran() {
			return ran.get();
		}

		private int disposeCount() {
			return disposeCount.get();
		}
	}

	private static final class LifecycleTask extends CompletableFuture<Void>
			implements Runnable, Disposable, RWScheduler.RejectionAwareTask {

		private final boolean failAfterReject;
		private final AtomicBoolean ran = new AtomicBoolean();
		private final AtomicInteger rejectionCount = new AtomicInteger();
		private final AtomicInteger disposeCount = new AtomicInteger();

		private LifecycleTask(boolean failAfterReject) {
			this.failAfterReject = failAfterReject;
		}

		@Override
		public void run() {
			ran.set(true);
			complete(null);
		}

		@Override
		public void reject(RuntimeException failure) {
			rejectionCount.incrementAndGet();
			completeExceptionally(failure);
			if (failAfterReject) {
				throw new IllegalStateException("expected rejection callback failure");
			}
		}

		@Override
		public void dispose() {
			disposeCount.incrementAndGet();
		}

		@Override
		public boolean isDisposed() {
			return disposeCount.get() > 0;
		}

		private boolean ran() {
			return ran.get();
		}

		private int rejectionCount() {
			return rejectionCount.get();
		}

		private int disposeCount() {
			return disposeCount.get();
		}
	}

	private static final class LockCheckingLifecycleTask extends CompletableFuture<Void>
			implements Runnable, Disposable, RWScheduler.RejectionAwareTask {

		private final RWScheduler scheduler;
		private final AtomicBoolean ran = new AtomicBoolean();
		private final AtomicInteger rejectionCount = new AtomicInteger();
		private final AtomicInteger disposeCount = new AtomicInteger();
		private final AtomicBoolean callbackOutsideLock = new AtomicBoolean();
		private final AtomicReference<Throwable> callbackFailure = new AtomicReference<>();

		private LockCheckingLifecycleTask(RWScheduler scheduler) {
			this.scheduler = scheduler;
		}

		@Override
		public void run() {
			ran.set(true);
			complete(null);
		}

		@Override
		public void reject(RuntimeException failure) {
			try {
				CompletableFuture.runAsync(
						() -> scheduler.poolSnapshot(RWScheduler.Pool.READ)).get(2, SECONDS);
				callbackOutsideLock.set(true);
			} catch (Throwable callbackError) {
				callbackFailure.set(callbackError);
			}
			rejectionCount.incrementAndGet();
			completeExceptionally(failure);
		}

		@Override
		public void dispose() {
			disposeCount.incrementAndGet();
		}

		@Override
		public boolean isDisposed() {
			return disposeCount.get() > 0;
		}

		private boolean ran() {
			return ran.get();
		}

		private int rejectionCount() {
			return rejectionCount.get();
		}

		private int disposeCount() {
			return disposeCount.get();
		}

		private boolean callbackOutsideLock() {
			return callbackOutsideLock.get();
		}

		private Throwable callbackFailure() {
			return callbackFailure.get();
		}
	}

}
