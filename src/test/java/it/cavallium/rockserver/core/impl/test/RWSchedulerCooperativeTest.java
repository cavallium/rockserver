package it.cavallium.rockserver.core.impl.test;

import com.sun.management.ThreadMXBean;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;

class RWSchedulerCooperativeTest {

	private static final long RANGE_QUANTUM_NANOS = Duration.ofMillis(8).toNanos();

	@Test
	void cooperativeExecutionAcceptsOnlyAnalyticalIngestAndBatchProfiles() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 8, 8, "cooperative-profile-matrix");
		try {
			for (var profile : new WorkloadProfile[] {
					WorkloadProfile.ANALYTICAL,
					WorkloadProfile.INGEST,
					WorkloadProfile.BATCH
			}) {
				var task = new FixedYieldTask(0);
				var handle = scheduler.executor(profile,
						OperationFamily.RANGE_PAGE,
						RequestContext.NO_DEADLINE).executeCooperatively(task, 1L);
				assertTrue(task.completed.await(5, SECONDS), profile + " task did not complete");
				assertEventually(handle::isDisposed);
				assertNull(task.failure.get());
			}

			var rejectedProfiles = new WorkloadProfile[] {
					WorkloadProfile.LATENCY,
					WorkloadProfile.CONTROL,
					WorkloadProfile.CDC,
					WorkloadProfile.PHYSICAL_MAINTENANCE
			};
			var rejectedFamilies = new OperationFamily[] {
					OperationFamily.POINT_LOOKUP,
					OperationFamily.CONTROL,
					OperationFamily.WAL_PAGE,
					OperationFamily.FLUSH
			};
			for (int i = 0; i < rejectedProfiles.length; i++) {
				var executor = scheduler.executor(rejectedProfiles[i],
						rejectedFamilies[i],
						RequestContext.NO_DEADLINE);
				var failure = assertThrows(IllegalArgumentException.class,
						() -> executor.executeCooperatively(new FixedYieldTask(0), 1L));
				assertTrue(failure.getMessage().contains("ANALYTICAL, INGEST, or BATCH"));
			}
		} finally {
			scheduler.disposeNow();
		}
	}

	@Test
	void uncontendedCooperativeProfilesFinishAllChunksInOneDispatch() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 8, 8, "cooperative-uncontended");
		try {
			for (var profile : new WorkloadProfile[] {
					WorkloadProfile.ANALYTICAL,
					WorkloadProfile.INGEST,
					WorkloadProfile.BATCH
			}) {
				var task = new UncontendedChunkTask(32);
				scheduler.executor(profile,
						OperationFamily.RANGE_PAGE,
						RequestContext.NO_DEADLINE).executeCooperatively(task, 1L);
				assertTrue(task.completed.await(5, SECONDS), profile + " task did not complete");
				assertEquals(32, task.completedChunks);
				assertEquals(1, task.invocations,
						profile + " must continue immediately while its pool has no queued competition");
				assertEquals(0, task.observedPreemptions);
				assertNull(task.failure.get());
			}
		} finally {
			scheduler.disposeNow();
		}
	}

	@Test
	void analyticalCooperativeWorkDoesNotTreatItselfAsPreemptionPressure() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 8, 8, "cooperative-analytical-self");
		var task = new SelfPreemptionProbeTask(100_000);
		try {
			scheduler.executor(WorkloadProfile.ANALYTICAL,
					OperationFamily.FULL_SCAN_AGGREGATE,
					RequestContext.NO_DEADLINE).executeCooperatively(task, 1L);
			assertTrue(task.completed.await(5, SECONDS));
			assertFalse(task.preemptionObserved,
					"the active ANALYTICAL task itself must not publish local queued competition");
			assertNull(task.failure.get());
		} finally {
			scheduler.disposeNow();
		}
	}

	@Test
	void cooperativeAnalyticalWorkYieldsForSameProfileQueuedWork() throws Exception {
		assertYieldsForQueuedCompetition(WorkloadProfile.ANALYTICAL,
				WorkloadProfile.ANALYTICAL,
				OperationFamily.RANGE_PAGE,
				"cooperative-same-profile");
	}

	@Test
	void cooperativeIngestWorkYieldsForHigherPriorityQueuedWork() throws Exception {
		assertYieldsForQueuedCompetition(WorkloadProfile.INGEST,
				WorkloadProfile.LATENCY,
				OperationFamily.POINT_LOOKUP,
				"cooperative-higher-priority");
	}

	@Test
	void saturatedBatchUsesEveryReadWorkerAndYieldsToEveryCompetingProfile() throws Exception {
		var scheduler = RWScheduler.forTesting(4, 4, 4, 64, 64, "cooperative-saturation");
		var allScansStarted = new CountDownLatch(4);
		var scansCompleted = new CountDownLatch(4);
		var scansReclaimed = new CountDownLatch(4);
		var stop = new AtomicBoolean();
		var scans = new ArrayList<SaturatingScanTask>();
		var handles = new ArrayList<RWScheduler.CooperativeHandle>();
		try {
			var batch = scheduler.executor(
					WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
			for (int i = 0; i < 4; i++) {
				var scan = new SaturatingScanTask(allScansStarted, scansCompleted, scansReclaimed, stop);
				scans.add(scan);
				handles.add(batch.executeCooperatively(scan, 2L * 1024L * 1024L));
			}

			assertTrue(allScansStarted.await(5, SECONDS));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).activeTasks() == 4);
			assertEquals(0, scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks());

			var foregroundStarted = new CountDownLatch(5);
			long queuedAtNanos = System.nanoTime();
			var latestStartNanos = new java.util.concurrent.atomic.AtomicLong();
			Runnable foreground = () -> {
				latestStartNanos.accumulateAndGet(System.nanoTime(), Math::max);
				foregroundStarted.countDown();
			};
			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					System.currentTimeMillis() + SECONDS.toMillis(5)).execute(foreground);
			scheduler.executor(WorkloadProfile.INGEST,
					OperationFamily.POINT_LOOKUP,
					RequestContext.NO_DEADLINE).execute(foreground);
			scheduler.executor(WorkloadProfile.CDC,
					OperationFamily.WAL_PAGE,
					RequestContext.NO_DEADLINE).execute(foreground);
			scheduler.executor(WorkloadProfile.ANALYTICAL,
					OperationFamily.FULL_SCAN_AGGREGATE,
					RequestContext.NO_DEADLINE).execute(foreground);
			batch.execute(foreground);

			assertTrue(foregroundStarted.await(2, SECONDS));
			assertTrue(latestStartNanos.get() - queuedAtNanos < MILLISECONDS.toNanos(250),
					"eligible work should start after one cooperative quantum plus scheduling jitter");
			assertTrue(scans.stream().allMatch(scan -> scan.yields() > 0),
					"every saturated scan worker must observe contention and yield");

			assertTrue(scansReclaimed.await(2, SECONDS));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).activeTasks() == 4);
			assertEquals(0, scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks(),
					"BATCH must immediately reclaim every idle worker after contention ends");
		} finally {
			stop.set(true);
			assertTrue(scansCompleted.await(5, SECONDS));
			assertEventually(() -> handles.stream().allMatch(RWScheduler.CooperativeHandle::isDisposed));
			scheduler.disposeNow();
		}
		for (var scan : scans) {
			assertNull(scan.failure());
		}
	}

	@Test
	void cooperativeReadTasksObserveCompetitionFromTheWritePool() throws Exception {
		var scheduler = RWScheduler.forTesting(4, 4, 4, 64, 64, "cooperative-cross-pool");
		var allScansStarted = new CountDownLatch(4);
		var scansCompleted = new CountDownLatch(4);
		var stop = new AtomicBoolean();
		var scans = new ArrayList<SaturatingScanTask>();
		var handles = new ArrayList<RWScheduler.CooperativeHandle>();
		var writeStarted = new CountDownLatch(1);
		var releaseWrite = new CountDownLatch(1);
		try {
			var batch = scheduler.executor(
					WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
			for (int i = 0; i < 4; i++) {
				var scan = new SaturatingScanTask(
						allScansStarted, scansCompleted, new CountDownLatch(0), stop);
				scans.add(scan);
				handles.add(batch.executeCooperatively(scan, 2L * 1024L * 1024L));
			}
			assertTrue(allScansStarted.await(5, SECONDS));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).activeTasks() == 4
					&& scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 0);
			var yieldsBefore = scans.stream().map(SaturatingScanTask::yields).toList();

			scheduler.executor(WorkloadProfile.INGEST,
					OperationFamily.MUTATION,
					RequestContext.NO_DEADLINE).execute(() -> {
				writeStarted.countDown();
				awaitUninterruptibly(releaseWrite);
			});
			assertTrue(writeStarted.await(5, SECONDS));
			assertEventually(() -> {
				for (int i = 0; i < scans.size(); i++) {
					if (scans.get(i).yields() <= yieldsBefore.get(i)) {
						return false;
					}
				}
				return true;
			});
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.activeByProfile().get(WorkloadProfile.BATCH) == 4);
		} finally {
			releaseWrite.countDown();
			stop.set(true);
			assertTrue(scansCompleted.await(5, SECONDS));
			assertEventually(() -> handles.stream().allMatch(RWScheduler.CooperativeHandle::isDisposed));
			scheduler.disposeNow();
		}
		for (var scan : scans) {
			assertNull(scan.failure());
		}
	}

	@Test
	void cooperativeForegroundCompetitionLimitsCrossPoolBatchUntilTerminal() throws Exception {
		var scheduler = RWScheduler.forTesting(6, 6, 1, 128, 128, "cooperative-cross-pool-pressure");
		var releaseForeground = new CountDownLatch(1);
		var foreground = new BlockingCompleteTask(releaseForeground);
		var firstBatchStarted = new CountDownLatch(1);
		var allBatchStarted = new CountDownLatch(6);
		var releaseBatch = new CountDownLatch(1);
		var batchStarts = new AtomicInteger();
		try {
			var foregroundHandle = scheduler.executor(WorkloadProfile.ANALYTICAL,
					OperationFamily.FULL_SCAN_AGGREGATE,
					RequestContext.NO_DEADLINE).executeCooperatively(foreground, 1L);
			assertTrue(foreground.started.await(5, SECONDS));

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
					"cooperative foreground work must retain the existing global BATCH cap");

			releaseForeground.countDown();
			assertTrue(foreground.completed.await(5, SECONDS));
			assertEventually(foregroundHandle::isDisposed);
			assertTrue(allBatchStarted.await(5, SECONDS),
					"terminal cooperative foreground work must wake cross-pool BATCH waiters");
			assertEquals(6, batchStarts.get());
			assertNull(foreground.failure.get());
		} finally {
			releaseForeground.countDown();
			releaseBatch.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void parkedTaskResumesTheSameLogicalSubmissionAndCachesNoDeadlineViews() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 8, 8, "cooperative-park");
		var executor = scheduler.executor(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		var task = new ParkOnceTask();
		try {
			assertSame(executor,
					scheduler.executor(WorkloadProfile.BATCH,
							OperationFamily.RANGE_PAGE,
							RequestContext.NO_DEADLINE));
			assertSame(scheduler.readExecutor(), scheduler.readExecutor());

			var handle = executor.executeCooperatively(task, 2L * 1024L * 1024L);
			assertTrue(task.parked.await(5, SECONDS));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).activeTasks() == 0);
			assertEquals(0, scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks());

			handle.resume();
			assertTrue(task.completed.await(5, SECONDS));
			assertEventually(handle::isDisposed);
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(1L, snapshot.acceptedTasks());
			assertEquals(1L, snapshot.startedTasks());
			assertEquals(1L, snapshot.completedTasks());
			assertEquals(1L, snapshot.outcomes().get(RWScheduler.TerminalOutcome.RUN));
			assertNull(task.failure.get());
		} finally {
			scheduler.disposeNow();
		}
	}

	@Test
	void cancellingAParkedTaskRecordsOneStartedCompletionAndOneTerminalOutcome() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 8, 8, "cooperative-park-cancel");
		var task = new ParkOnceTask();
		try {
			var handle = scheduler.executor(
					WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE).executeCooperatively(task, 2L * 1024L * 1024L);
			assertTrue(task.parked.await(5, SECONDS));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).activeTasks() == 0);

			handle.dispose();
			assertTrue(task.completed.await(5, SECONDS));
			assertEventually(handle::isDisposed);
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(1L, snapshot.acceptedTasks());
			assertEquals(1L, snapshot.startedTasks());
			assertEquals(1L, snapshot.completedTasks());
			assertEquals(1L, snapshot.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION));
			assertEquals(0L, snapshot.outcomes().get(RWScheduler.TerminalOutcome.RUN));
			assertTrue(task.failure.get() instanceof java.util.concurrent.CancellationException);
		} finally {
			scheduler.disposeNow();
		}
	}

	@Test
	void duplicateCooperativeIdentityRemainsMappedAcrossParkCancelAndResume() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 8, 8, "cooperative-duplicate-identity");
		var executor = scheduler.executor(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		var task = new AlwaysParkTask();
		try {
			var first = executor.executeCooperatively(task, 1L);
			var second = executor.executeCooperatively(task, 1L);
			assertEventually(() -> task.parks.get() == 2);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).parkedTasks() == 2);

			assertTrue(first.cancel());
			assertEventually(() -> task.rejections.get() == 1);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).parkedTasks() == 1);

			second.resume();
			assertEventually(() -> task.parks.get() == 3);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).parkedTasks() == 1);
			assertTrue(second.cancel());
			assertEventually(() -> task.rejections.get() == 2);

			var drained = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(2L, drained.submissionAttempts());
			assertEquals(2L, drained.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION));
			assertTrue(drained.drainedAndConserved());
		} finally {
			scheduler.disposeNow();
		}
	}

	@Test
	void parkedBacklogHasAFixedAdmissionBoundAndExactConservation() throws Exception {
		var registry = new SimpleMeterRegistry();
		var scheduler = RWScheduler.forTesting(
				1, 1, 1, 1, 1, "cooperative-parked-bound", registry, "cooperative-parked-bound-db");
		var tasks = new ArrayList<ParkOnceTask>();
		var handles = new ArrayList<RWScheduler.CooperativeHandle>();
		var executor = scheduler.executor(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		try {
			for (int i = 0; i < 2; i++) {
				var task = new ParkOnceTask();
				tasks.add(task);
				handles.add(executor.executeCooperatively(task, 1L));
				assertTrue(task.parked.await(5, SECONDS));
				int expectedParked = i + 1;
				assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).parkedTasks()
						== expectedParked);
			}

			var saturated = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(0, saturated.queuedTasks());
			assertEquals(0, saturated.activeTasks());
			assertEquals(2, saturated.parkedTasks());
			assertEquals(2, saturated.outstandingTasks());
			assertEquals(2, saturated.outstandingByProfile().get(WorkloadProfile.BATCH));

			var rejected = new ParkOnceTask();
			var overload = assertThrows(RocksDBException.class,
					() -> executor.executeCooperatively(rejected, 1L));
			assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
					overload.getErrorUniqueId());
			assertTrue(rejected.completed.await(5, SECONDS));
			assertSame(overload, rejected.failure.get());
			assertEquals(3.0, parkedGauge(registry, "rockserver.workload.submission.attempts"));
			assertEquals(2.0, parkedGauge(registry, "rockserver.workload.parked"));
			assertEquals(2.0, parkedGauge(registry, "rockserver.workload.outstanding"));

			assertTrue(handles.getFirst().cancel());
			assertTrue(tasks.getFirst().completed.await(5, SECONDS));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).outstandingTasks() == 1);

			var replacement = new ParkOnceTask();
			tasks.add(replacement);
			var replacementHandle = executor.executeCooperatively(replacement, 1L);
			handles.add(replacementHandle);
			assertTrue(replacement.parked.await(5, SECONDS));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).parkedTasks() == 2);

			for (int i = 1; i < handles.size(); i++) {
				assertTrue(handles.get(i).cancel());
				assertTrue(tasks.get(i).completed.await(5, SECONDS));
			}
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).outstandingTasks() == 0);

			var drained = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(4L, drained.submissionAttempts());
			assertEquals(4L, drained.submissionAttemptsByProfile().get(WorkloadProfile.BATCH));
			assertEquals(3L, drained.acceptedTasks());
			assertEquals(3L, drained.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION));
			assertEquals(1L, drained.outcomes().get(RWScheduler.TerminalOutcome.OVERLOAD));
			assertEquals(4L, drained.terminalOutcomes());
			assertTrue(drained.drainedAndConserved());
			assertEquals(4.0, parkedGauge(registry, "rockserver.workload.submission.attempts"));
			assertEquals(0.0, parkedGauge(registry, "rockserver.workload.parked"));
			assertEquals(0.0, parkedGauge(registry, "rockserver.workload.outstanding"));
		} finally {
			for (var handle : handles) {
				handle.cancel();
			}
			scheduler.disposeNow();
			registry.close();
		}
	}

	private static double parkedGauge(SimpleMeterRegistry registry, String name) {
		return registry.get(name)
				.tags("database", "cooperative-parked-bound-db",
						"resource", "read",
						"profile", "batch")
				.gauge()
				.value();
	}

	@Test
	void forcedShutdownDrainsActiveAndParkedIntrusiveLifetimeEntries() throws Exception {
		var scheduler = RWScheduler.forTesting(2, 2, 2, 8, 8, "cooperative-intrusive-shutdown");
		var active = new DeadlineTask();
		var parked = new ParkOnceTask();
		try {
			var executor = scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE);
			executor.executeCooperatively(active, 1L);
			assertTrue(active.started.await(5, SECONDS));
			executor.executeCooperatively(parked, 1L);
			assertTrue(parked.parked.await(5, SECONDS));

			scheduler.disposeNow();

			assertTrue(active.completed.await(5, SECONDS));
			assertTrue(parked.completed.await(5, SECONDS));
			assertTrue(active.failure.get() instanceof java.util.concurrent.RejectedExecutionException);
			assertTrue(parked.failure.get() instanceof java.util.concurrent.RejectedExecutionException);
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(2L, snapshot.outcomes().get(RWScheduler.TerminalOutcome.SHUTDOWN));
			assertTrue(snapshot.drainedAndConserved());
		} finally {
			scheduler.disposeNow();
		}
	}

	@Test
	void cooperativeHandleDefaultCancelPreservesLegacyImplementors() {
		var disposed = new AtomicBoolean();
		RWScheduler.CooperativeHandle legacyHandle = new RWScheduler.CooperativeHandle() {
			@Override
			public void resume() {
			}

			@Override
			public void dispose() {
				disposed.set(true);
			}

			@Override
			public boolean isDisposed() {
				return disposed.get();
			}
		};

		assertTrue(legacyHandle.cancel());
		assertTrue(disposed.get());
		assertFalse(legacyHandle.cancel());
	}

	@Test
	void completionAwareTaskPublishesOnlyTheSchedulerWinningTerminalCause() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 8, 8, "cooperative-terminal-authority");
		try {
			var cancelled = new TerminalAuthorityTask(false);
			var cancelledHandle = scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE).executeCooperatively(cancelled, 1L);
			assertTrue(cancelled.entered.await(5, SECONDS));
			assertTrue(cancelledHandle.cancel());
			cancelled.release.countDown();
			assertTrue(cancelled.terminal.await(5, SECONDS));
			assertEquals(0, cancelled.successes.get());
			assertEquals(1, cancelled.rejections.get());
			assertTrue(cancelled.failure.get() instanceof java.util.concurrent.CancellationException);

			var completed = new TerminalAuthorityTask(true);
			var completedHandle = scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE).executeCooperatively(completed, 1L);
			assertTrue(completed.entered.await(5, SECONDS));
			completed.release.countDown();
			assertTrue(completed.completionEntered.await(5, SECONDS));
			try {
				assertFalse(completedHandle.cancel(),
						"cancellation must report that the scheduler already selected RUN");
			} finally {
				completed.releaseCompletion.countDown();
			}
			assertTrue(completed.terminal.await(5, SECONDS));
			assertEquals(1, completed.successes.get());
			assertEquals(0, completed.rejections.get());
			assertNull(completed.failure.get());

			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).completedTasks() == 2L);
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(1L, snapshot.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION));
			assertEquals(1L, snapshot.outcomes().get(RWScheduler.TerminalOutcome.RUN));
		} finally {
			scheduler.disposeNow();
		}
	}

	@Test
	void cooperativeFailureUsesDeterministicFirstCauseArbitration() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 8, 8, "cooperative-failure-authority");
		var firstFailure = new IllegalStateException("first cooperative failure");
		var task = new FirstCauseFailureTask(firstFailure);
		try {
			var handle = scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE).executeCooperatively(task, 1L);
			assertTrue(task.failureSelected.await(5, SECONDS));
			assertTrue(task.firstFailureSelected);
			assertFalse(task.laterFailureSelected);
			assertFalse(handle.cancel(), "the first command failure must beat later cancellation");

			task.release.countDown();
			assertTrue(task.terminal.await(5, SECONDS));
			assertSame(firstFailure, task.rejection.get());
			assertEventually(handle::isDisposed);

			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(1L, snapshot.outcomes().get(RWScheduler.TerminalOutcome.FAILURE));
			assertEquals(0L, snapshot.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION));
			assertEquals(0L, snapshot.outcomes().get(RWScheduler.TerminalOutcome.RUN));
		} finally {
			task.release.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void cooperativeYieldAllocatesNoSchedulerHeapAfterWarmup() throws Exception {
		var threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
		Assumptions.assumeTrue(threads.isThreadAllocatedMemorySupported());
		if (!threads.isThreadAllocatedMemoryEnabled()) {
			threads.setThreadAllocatedMemoryEnabled(true);
		}

		var registry = new SimpleMeterRegistry();
		var scheduler = RWScheduler.forTesting(
				2, 2, 2, 8, 8, "cooperative-allocation", registry, "cooperative-allocation-db");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var probe = new AllocationProbeTask(50_000, 10_000);
		try {
			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					System.currentTimeMillis() + SECONDS.toMillis(30)).execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE).executeCooperatively(probe, 2L * 1024L * 1024L);

			probe.measureSchedulerIntervals(threads);
			assertTrue(probe.completed.await(20, SECONDS));
			assertEquals(0, probe.nonZeroIntervals(),
					"requeue, permit release, sequence refresh, and redispatch must allocate zero bytes per yield"
							+ "; maximum scheduler bytes=" + probe.maximumSchedulerBytes()
							+ "; total scheduler bytes=" + probe.totalSchedulerBytes()
							+ "; maximum sampling bytes=" + probe.maximumSamplingBytes());
			assertEquals(0L, probe.maximumSchedulerBytes());
			assertEquals(0L, probe.totalSchedulerBytes());
			assertNull(probe.failure.get());
		} finally {
			probe.abort();
			releaseBlocker.countDown();
			scheduler.disposeNow();
			registry.close();
		}
	}

	@Test
	void cooperativeMetricsPublishOneLogicalSampleAndEveryQuantumAtTerminal() throws Exception {
		var registry = new SimpleMeterRegistry();
		var scheduler = RWScheduler.forTesting(
				1, 1, 1, 8, 8, "cooperative-metrics", registry, "cooperative-metrics-db");
		try {
			for (var profile : new WorkloadProfile[] {
					WorkloadProfile.ANALYTICAL,
					WorkloadProfile.INGEST,
					WorkloadProfile.BATCH
			}) {
				var task = new FixedYieldTask(7);
				scheduler.executor(profile,
						OperationFamily.RANGE_PAGE,
						RequestContext.NO_DEADLINE).executeCooperatively(task, 2L * 1024L * 1024L);
				assertTrue(task.completed.await(5, SECONDS));
				var profileTag = profile.name().toLowerCase(java.util.Locale.ROOT);
				assertEventually(() -> registry.get("rockserver.workload.quantums")
						.tags("database", "cooperative-metrics-db",
								"resource", "read",
								"profile", profileTag,
								"operation", "range_page")
						.counter()
						.count() == 8.0);
				assertEquals(1L, registry.get("rockserver.workload.queue.wait")
						.tags("database", "cooperative-metrics-db",
								"resource", "read",
								"profile", profileTag,
								"operation", "range_page")
						.timer()
						.count());
				assertEquals(1L, registry.get("rockserver.workload.execution")
						.tags("database", "cooperative-metrics-db",
								"resource", "read",
								"profile", profileTag,
								"operation", "range_page")
						.timer()
						.count());
				assertNull(task.failure.get());
			}
		} finally {
			scheduler.disposeNow();
			registry.close();
		}
	}

	@Test
	void cooperativeQueueWaitMeasuresAdmissionNotParkedRedispatchDelay() throws Exception {
		var registry = new SimpleMeterRegistry();
		var scheduler = RWScheduler.forTesting(
				1, 1, 1, 8, 8, "cooperative-queue-semantics", registry, "cooperative-queue-db");
		var task = new ParkOnceTask();
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		try {
			var handle = scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE).executeCooperatively(task, 2L * 1024L * 1024L);
			assertTrue(task.parked.await(5, SECONDS));

			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					System.currentTimeMillis() + SECONDS.toMillis(5)).execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			handle.resume();
			Thread.sleep(300L);
			releaseBlocker.countDown();
			assertTrue(task.completed.await(5, SECONDS));

			var queue = registry.get("rockserver.workload.queue.wait")
					.tags("database", "cooperative-queue-db",
							"resource", "read",
							"profile", "batch",
							"operation", "range_page")
					.timer();
			assertEquals(1L, queue.count());
			assertTrue(queue.totalTime(MILLISECONDS) < 200.0d,
					"the 300 ms downstream park/redispatch delay must not inflate initial queue wait: "
							+ queue.totalTime(MILLISECONDS) + " ms");
			assertNull(task.failure.get());
		} finally {
			releaseBlocker.countDown();
			scheduler.disposeNow();
			registry.close();
		}
	}

	@Test
	void activeCooperativeTaskObservesItsDeadlineWithoutAnIdleWorker() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 8, 8, "cooperative-deadline");
		var task = new DeadlineTask();
		try {
			scheduler.executor(WorkloadProfile.ANALYTICAL,
					OperationFamily.FULL_SCAN_AGGREGATE,
					System.currentTimeMillis() + 1_000L).executeCooperatively(task, 2L * 1024L * 1024L);
			assertTrue(task.started.await(5, SECONDS));
			assertTrue(task.completed.await(5, SECONDS));
			assertTrue(task.failure.get() instanceof it.cavallium.rockserver.core.common.RocksDBException);
			assertEquals(1L,
					scheduler.poolSnapshot(RWScheduler.Pool.READ)
							.outcomes()
							.get(RWScheduler.TerminalOutcome.DEADLINE));
			assertEquals(0L,
					scheduler.poolSnapshot(RWScheduler.Pool.READ)
							.outcomes()
							.get(RWScheduler.TerminalOutcome.RUN));
		} finally {
			scheduler.disposeNow();
		}
	}

	private static void assertEventually(BooleanSupplier condition) {
		long deadline = System.nanoTime() + SECONDS.toNanos(5);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.onSpinWait();
		}
		assertTrue(condition.getAsBoolean());
	}

	private static void assertYieldsForQueuedCompetition(WorkloadProfile cooperativeProfile,
	                                                     WorkloadProfile contenderProfile,
	                                                     OperationFamily contenderFamily,
	                                                     String schedulerName) throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 8, 8, schedulerName);
		var contenderRan = new AtomicBoolean();
		var contenderCompleted = new CountDownLatch(1);
		var task = new YieldForCompetitionTask(contenderRan);
		try {
			scheduler.executor(cooperativeProfile,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE).executeCooperatively(task, 1L);
			assertTrue(task.started.await(5, SECONDS));

			scheduler.executor(contenderProfile,
					contenderFamily,
					RequestContext.NO_DEADLINE).execute(() -> {
				contenderRan.set(true);
				contenderCompleted.countDown();
			});

			assertTrue(task.completed.await(5, SECONDS));
			assertTrue(contenderCompleted.await(5, SECONDS));
			assertTrue(task.preemptionObserved,
					"the active cooperative task must observe queued pool-local competition");
			assertTrue(task.contenderRanBeforeRedispatch,
					"the queued contender must run before the yielded task is redispatched");
			assertNull(task.failure.get());
		} finally {
			scheduler.disposeNow();
		}
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

	private static final class UncontendedChunkTask implements RWScheduler.CooperativeTask {

		private final CountDownLatch completed = new CountDownLatch(1);
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
		private int remainingChunks;
		private int completedChunks;
		private int invocations;
		private int observedPreemptions;

		private UncontendedChunkTask(int remainingChunks) {
			this.remainingChunks = remainingChunks;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			invocations++;
			while (remainingChunks > 0) {
				remainingChunks--;
				completedChunks++;
				if (context.preemptionRequested()) {
					observedPreemptions++;
					return RWScheduler.CooperativeResult.YIELD;
				}
			}
			completed.countDown();
			return RWScheduler.CooperativeResult.COMPLETE;
		}

		@Override
		public void reject(RuntimeException failure) {
			this.failure.compareAndSet(null, failure);
			completed.countDown();
		}
	}

	private static final class BlockingCompleteTask implements RWScheduler.CooperativeTask {

		private final CountDownLatch release;
		private final CountDownLatch started = new CountDownLatch(1);
		private final CountDownLatch completed = new CountDownLatch(1);
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();

		private BlockingCompleteTask(CountDownLatch release) {
			this.release = release;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			started.countDown();
			awaitUninterruptibly(release);
			completed.countDown();
			return RWScheduler.CooperativeResult.COMPLETE;
		}

		@Override
		public void reject(RuntimeException failure) {
			this.failure.compareAndSet(null, failure);
			completed.countDown();
		}
	}

	private static final class TerminalAuthorityTask implements RWScheduler.CooperativeCompletionTask {

		private final boolean blockCompletion;
		private final CountDownLatch entered = new CountDownLatch(1);
		private final CountDownLatch release = new CountDownLatch(1);
		private final CountDownLatch completionEntered = new CountDownLatch(1);
		private final CountDownLatch releaseCompletion = new CountDownLatch(1);
		private final CountDownLatch terminal = new CountDownLatch(1);
		private final AtomicInteger successes = new AtomicInteger();
		private final AtomicInteger rejections = new AtomicInteger();
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();

		private TerminalAuthorityTask(boolean blockCompletion) {
			this.blockCompletion = blockCompletion;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			entered.countDown();
			awaitUninterruptibly(release);
			return RWScheduler.CooperativeResult.COMPLETE;
		}

		@Override
		public void completeCooperatively() {
			completionEntered.countDown();
			if (blockCompletion) {
				awaitUninterruptibly(releaseCompletion);
			}
			successes.incrementAndGet();
			terminal.countDown();
		}

		@Override
		public void reject(RuntimeException failure) {
			this.failure.set(failure);
			rejections.incrementAndGet();
			terminal.countDown();
		}
	}

	private static final class SelfPreemptionProbeTask implements RWScheduler.CooperativeTask {

		private final CountDownLatch completed = new CountDownLatch(1);
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
		private final int samples;
		private boolean preemptionObserved;

		private SelfPreemptionProbeTask(int samples) {
			this.samples = samples;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			for (int i = 0; i < samples; i++) {
				preemptionObserved |= context.preemptionRequested();
				Thread.onSpinWait();
			}
			completed.countDown();
			return RWScheduler.CooperativeResult.COMPLETE;
		}

		@Override
		public void reject(RuntimeException failure) {
			this.failure.compareAndSet(null, failure);
			completed.countDown();
		}
	}

	private static final class FirstCauseFailureTask implements RWScheduler.CooperativeTask {

		private final RuntimeException firstFailure;
		private final RuntimeException laterFailure = new IllegalArgumentException("later cooperative failure");
		private final RuntimeException thrownFailure = new IllegalStateException("thrown cooperative failure");
		private final CountDownLatch failureSelected = new CountDownLatch(1);
		private final CountDownLatch release = new CountDownLatch(1);
		private final CountDownLatch terminal = new CountDownLatch(1);
		private final AtomicReference<RuntimeException> rejection = new AtomicReference<>();
		private boolean firstFailureSelected;
		private boolean laterFailureSelected;

		private FirstCauseFailureTask(RuntimeException firstFailure) {
			this.firstFailure = firstFailure;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			firstFailureSelected = context.fail(firstFailure);
			laterFailureSelected = context.fail(laterFailure);
			failureSelected.countDown();
			awaitUninterruptibly(release);
			throw thrownFailure;
		}

		@Override
		public void reject(RuntimeException failure) {
			rejection.set(failure);
			terminal.countDown();
		}
	}

	private static final class YieldForCompetitionTask implements RWScheduler.CooperativeTask {

		private final AtomicBoolean contenderRan;
		private final CountDownLatch started = new CountDownLatch(1);
		private final CountDownLatch completed = new CountDownLatch(1);
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
		private boolean firstInvocation = true;
		private boolean preemptionObserved;
		private boolean contenderRanBeforeRedispatch;

		private YieldForCompetitionTask(AtomicBoolean contenderRan) {
			this.contenderRan = contenderRan;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			if (firstInvocation) {
				firstInvocation = false;
				started.countDown();
				while (!context.preemptionRequested()) {
					if (context.terminationRequested()) {
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					Thread.onSpinWait();
				}
				preemptionObserved = true;
				return RWScheduler.CooperativeResult.YIELD;
			}
			contenderRanBeforeRedispatch = contenderRan.get();
			completed.countDown();
			return RWScheduler.CooperativeResult.COMPLETE;
		}

		@Override
		public void reject(RuntimeException failure) {
			this.failure.compareAndSet(null, failure);
			completed.countDown();
		}
	}

	private static final class SaturatingScanTask implements RWScheduler.CooperativeTask {

		private final CountDownLatch allStarted;
		private final CountDownLatch completed;
		private final CountDownLatch reclaimed;
		private final AtomicBoolean stop;
		private final AtomicBoolean firstRun = new AtomicBoolean(true);
		private final AtomicBoolean reclaimedOnce = new AtomicBoolean();
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
		private volatile int yields;

		private SaturatingScanTask(CountDownLatch allStarted,
		                           CountDownLatch completed,
		                           CountDownLatch reclaimed,
		                           AtomicBoolean stop) {
			this.allStarted = allStarted;
			this.completed = completed;
			this.reclaimed = reclaimed;
			this.stop = stop;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			if (firstRun.compareAndSet(true, false)) {
				allStarted.countDown();
			}
			while (allStarted.getCount() > 0 && !stop.get()) {
				Thread.onSpinWait();
			}
			long preemptionStarted = 0L;
			while (!stop.get()) {
				if (context.terminationRequested()) {
					return RWScheduler.CooperativeResult.COMPLETE;
				}
				if (!context.preemptionRequested()) {
					preemptionStarted = 0L;
					if (yields > 0 && reclaimedOnce.compareAndSet(false, true)) {
						reclaimed.countDown();
					}
					Thread.onSpinWait();
					continue;
				}
				long now = System.nanoTime();
				if (preemptionStarted == 0L) {
					preemptionStarted = now;
				} else if (now - preemptionStarted >= RANGE_QUANTUM_NANOS) {
					yields++;
					return RWScheduler.CooperativeResult.YIELD;
				}
				Thread.onSpinWait();
			}
			completed.countDown();
			return RWScheduler.CooperativeResult.COMPLETE;
		}

		@Override
		public void reject(RuntimeException failure) {
			this.failure.compareAndSet(null, failure);
			completed.countDown();
		}

		private int yields() {
			return yields;
		}

		private RuntimeException failure() {
			return failure.get();
		}
	}

	private static final class ParkOnceTask implements RWScheduler.CooperativeTask {

		private final CountDownLatch parked = new CountDownLatch(1);
		private final CountDownLatch completed = new CountDownLatch(1);
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
		private boolean first = true;

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			if (first) {
				first = false;
				parked.countDown();
				return RWScheduler.CooperativeResult.PARK;
			}
			completed.countDown();
			return RWScheduler.CooperativeResult.COMPLETE;
		}

		@Override
		public void reject(RuntimeException failure) {
			this.failure.compareAndSet(null, failure);
			completed.countDown();
		}
	}

	private static final class AlwaysParkTask implements RWScheduler.CooperativeTask {

		private final AtomicInteger parks = new AtomicInteger();
		private final AtomicInteger rejections = new AtomicInteger();

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			parks.incrementAndGet();
			return RWScheduler.CooperativeResult.PARK;
		}

		@Override
		public void reject(RuntimeException failure) {
			rejections.incrementAndGet();
		}
	}

	private static final class AllocationProbeTask implements RWScheduler.CooperativeTask {

		private final int warmupYields;
		private final int measuredIntervals;
		private final CountDownLatch completed = new CountDownLatch(1);
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
		private int invocations;
		private volatile long workerThreadId;
		private volatile int checkpoint;
		private volatile int acknowledgedCheckpoint;
		private volatile boolean aborted;
		private volatile long maximumSchedulerBytes;
		private volatile long totalSchedulerBytes;
		private volatile long maximumSamplingBytes;
		private volatile int nonZeroIntervals;

		private AllocationProbeTask(int warmupYields, int measuredIntervals) {
			this.warmupYields = warmupYields;
			this.measuredIntervals = measuredIntervals;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			int invocation = invocations++;
			if (invocation < warmupYields) {
				return RWScheduler.CooperativeResult.YIELD;
			}

			int measuredInvocation = invocation - warmupYields;
			if (workerThreadId == 0L) {
				workerThreadId = Thread.currentThread().threadId();
			}
			if (!awaitObserver(2 * measuredInvocation + 1)
					|| !awaitObserver(2 * measuredInvocation + 2)) {
				completed.countDown();
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			if (measuredInvocation >= measuredIntervals) {
				completed.countDown();
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			return RWScheduler.CooperativeResult.YIELD;
		}

		private boolean awaitObserver(int expectedCheckpoint) {
			checkpoint = expectedCheckpoint;
			while (acknowledgedCheckpoint < expectedCheckpoint && !aborted) {
				Thread.onSpinWait();
			}
			return !aborted;
		}

		private void measureSchedulerIntervals(ThreadMXBean threads) {
			long deadlineNanos = System.nanoTime() + SECONDS.toNanos(20);
			long previousEndBytes = -1L;
			for (int measuredInvocation = 0; measuredInvocation <= measuredIntervals; measuredInvocation++) {
				int startCheckpoint = 2 * measuredInvocation + 1;
				awaitCheckpoint(startCheckpoint, deadlineNanos);
				long currentBytes = threads.getThreadAllocatedBytes(workerThreadId);
				long afterCurrentSampleBytes = threads.getThreadAllocatedBytes(workerThreadId);
				long samplingBytes = afterCurrentSampleBytes - currentBytes;
				maximumSamplingBytes = Math.max(maximumSamplingBytes, samplingBytes);
				if (previousEndBytes >= 0L) {
					long allocated = currentBytes - previousEndBytes - samplingBytes;
					maximumSchedulerBytes = Math.max(maximumSchedulerBytes, allocated);
					totalSchedulerBytes += allocated;
					if (allocated != 0L) {
						nonZeroIntervals++;
					}
				}
				acknowledgedCheckpoint = startCheckpoint;

				int endCheckpoint = startCheckpoint + 1;
				awaitCheckpoint(endCheckpoint, deadlineNanos);
				long endBytes = threads.getThreadAllocatedBytes(workerThreadId);
				previousEndBytes = threads.getThreadAllocatedBytes(workerThreadId);
				maximumSamplingBytes = Math.max(maximumSamplingBytes, previousEndBytes - endBytes);
				acknowledgedCheckpoint = endCheckpoint;
			}
		}

		private void awaitCheckpoint(int expectedCheckpoint, long deadlineNanos) {
			while (checkpoint < expectedCheckpoint) {
				RuntimeException rejection = failure.get();
				if (rejection != null) {
					throw new AssertionError("allocation probe was rejected", rejection);
				}
				if (System.nanoTime() >= deadlineNanos) {
					aborted = true;
					throw new AssertionError("timed out waiting for allocation checkpoint " + expectedCheckpoint);
				}
				Thread.onSpinWait();
			}
		}

		private void abort() {
			aborted = true;
		}

		@Override
		public void reject(RuntimeException failure) {
			this.failure.compareAndSet(null, failure);
			completed.countDown();
		}

		private long maximumSchedulerBytes() {
			return maximumSchedulerBytes;
		}

		private long totalSchedulerBytes() {
			return totalSchedulerBytes;
		}

		private int nonZeroIntervals() {
			return nonZeroIntervals;
		}

		private long maximumSamplingBytes() {
			return maximumSamplingBytes;
		}
	}

	private static final class DeadlineTask implements RWScheduler.CooperativeTask {

		private final CountDownLatch started = new CountDownLatch(1);
		private final CountDownLatch completed = new CountDownLatch(1);
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			started.countDown();
			while (!context.terminationRequested()) {
				Thread.onSpinWait();
			}
			return RWScheduler.CooperativeResult.COMPLETE;
		}

		@Override
		public void reject(RuntimeException failure) {
			this.failure.compareAndSet(null, failure);
			completed.countDown();
		}
	}

	private static final class FixedYieldTask implements RWScheduler.CooperativeTask {

		private final CountDownLatch completed = new CountDownLatch(1);
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
		private int remainingYields;

		private FixedYieldTask(int remainingYields) {
			this.remainingYields = remainingYields;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			if (remainingYields-- > 0) {
				return RWScheduler.CooperativeResult.YIELD;
			}
			completed.countDown();
			return RWScheduler.CooperativeResult.COMPLETE;
		}

		@Override
		public void reject(RuntimeException failure) {
			this.failure.compareAndSet(null, failure);
			completed.countDown();
		}
	}
}
