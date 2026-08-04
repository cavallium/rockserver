package it.cavallium.rockserver.core.impl.benchmark;

import com.sun.management.ThreadMXBean;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Fresh-process scheduler microbenchmark for admission, dispatch, cooperative transitions, and cancellation.
 *
 * <p>This deliberately reuses commands and keeps coordination outside the measured scheduler
 * allocation. Select {@code normal}, {@code indexed}, {@code cooperative},
 * {@code cooperative-yield}, {@code cooperative-park-resume}, or
 * {@code cooperative-cancel}, or {@code saturated-rejection} as the first argument. Repeated
 * YIELD and PARK/resume measure a
 * single admitted scheduler node after warm-up and fail unless both the submitting and worker
 * threads allocate exactly zero bytes. Run multiple counterbalanced baseline/candidate subprocess
 * pairs; one invocation is not a release-performance claim.</p>
 */
public final class SchedulerHotPathBenchmark {

	private static final int DEFAULT_WARMUP_OPERATIONS = 100_000;
	private static final int DEFAULT_MEASURED_OPERATIONS = 500_000;
	private static final int SUBMISSION_BATCH = 8_192;

	private SchedulerHotPathBenchmark() {
	}

	public static void main(String[] args) {
		String scenario = args.length > 0 ? args[0] : "cooperative";
		int warmupOperations = integerArgument(args, 1, DEFAULT_WARMUP_OPERATIONS);
		int measuredOperations = integerArgument(args, 2, DEFAULT_MEASURED_OPERATIONS);
		if (warmupOperations < 1 || measuredOperations < 1) {
			throw new IllegalArgumentException("operation counts must be positive");
		}

		var threadMetrics = (ThreadMXBean) ManagementFactory.getThreadMXBean();
		if (!threadMetrics.isThreadAllocatedMemorySupported()) {
			throw new IllegalStateException("thread allocation accounting is unavailable");
		}
		if (!threadMetrics.isThreadAllocatedMemoryEnabled()) {
			threadMetrics.setThreadAllocatedMemoryEnabled(true);
		}

		int queueCapacity = scenario.equals("saturated-rejection") ? 1 : SUBMISSION_BATCH * 2;
		var scheduler = RWScheduler.forTesting(
				1, 1, 1,
				queueCapacity,
				queueCapacity,
				"scheduler-hot-path");
		var cooperativeTask = new ImmediateCompletionTask();
		var normalTask = new ImmediateRunnable();
		try {
			if (scenario.equals("cooperative-yield") || scenario.equals("cooperative-park-resume")) {
				runRepeatedCooperativeTransitions(scheduler,
						threadMetrics,
						scenario,
						warmupOperations,
						measuredOperations);
				return;
			}
			if (scenario.equals("cooperative-cancel")) {
				runCooperativeCancellation(scheduler,
						threadMetrics,
						warmupOperations,
						measuredOperations);
				return;
			}
			if (scenario.equals("saturated-rejection")) {
				runSaturatedRejection(scheduler,
						threadMetrics,
						warmupOperations,
						measuredOperations);
				return;
			}
			OperationSubmitter submitter;
			AtomicInteger completed;
			AtomicReference<RuntimeException> failure;
			WorkloadProfile profile;
			switch (scenario) {
				case "cooperative" -> {
					profile = WorkloadProfile.BATCH;
					var executor = scheduler.executor(WorkloadProfile.BATCH,
							OperationFamily.RANGE_PAGE,
							RequestContext.NO_DEADLINE);
					submitter = () -> executor.executeCooperatively(cooperativeTask, 1L);
					completed = cooperativeTask.completed;
					failure = cooperativeTask.failure;
				}
				case "normal" -> {
					profile = WorkloadProfile.INGEST;
					var executor = scheduler.executor(WorkloadProfile.INGEST,
							OperationFamily.POINT_LOOKUP,
							RequestContext.NO_DEADLINE);
					submitter = () -> executor.execute(normalTask);
					completed = normalTask.completed;
					failure = normalTask.failure;
				}
				case "indexed" -> {
					profile = WorkloadProfile.INGEST;
					var indexedScheduler = scheduler.scheduler(WorkloadProfile.INGEST,
							OperationFamily.POINT_LOOKUP,
							RequestContext.NO_DEADLINE);
					submitter = () -> indexedScheduler.schedule(normalTask);
					completed = normalTask.completed;
					failure = normalTask.failure;
				}
				default -> throw new IllegalArgumentException("unknown scenario: " + scenario);
			}
			runOperations(scheduler, profile, submitter, completed, failure, 0, warmupOperations);
			long workerThreadId = workerThreadId(scheduler, RWScheduler.Pool.READ, threadMetrics);
			long submitterThreadId = Thread.currentThread().threadId();
			long workerAllocatedBefore = threadMetrics.getThreadAllocatedBytes(workerThreadId);
			long submitterAllocatedBefore = threadMetrics.getThreadAllocatedBytes(submitterThreadId);
			long startedNanos = System.nanoTime();
			runOperations(scheduler,
					profile,
					submitter,
					completed,
					failure,
					warmupOperations,
					measuredOperations);
			long elapsedNanos = System.nanoTime() - startedNanos;
			long submitterAllocated = threadMetrics.getThreadAllocatedBytes(submitterThreadId)
					- submitterAllocatedBefore;
			long workerAllocated = threadMetrics.getThreadAllocatedBytes(workerThreadId) - workerAllocatedBefore;
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			if (!drainedAndConserved(snapshot) || failure.get() != null) {
				throw new IllegalStateException("scheduler did not drain cleanly", failure.get());
			}
			double operationsPerSecond = measuredOperations * 1_000_000_000.0 / elapsedNanos;
			double allocatedBytesPerOperation = (submitterAllocated + workerAllocated)
					/ (double) measuredOperations;
			System.out.printf(java.util.Locale.ROOT,
					"scenario=%s operations=%d elapsed_nanos=%d operations_per_second=%.3f "
							+ "submitter_allocated_bytes=%d worker_allocated_bytes=%d "
							+ "allocated_bytes_per_operation=%.3f%n",
					scenario,
					measuredOperations,
					elapsedNanos,
					operationsPerSecond,
					submitterAllocated,
					workerAllocated,
					allocatedBytesPerOperation);
		} finally {
			scheduler.disposeNow();
		}
	}

	private static void runRepeatedCooperativeTransitions(RWScheduler scheduler,
	                                                       ThreadMXBean threadMetrics,
	                                                       String scenario,
	                                                       int warmupOperations,
	                                                       int measuredOperations) {
		var executor = scheduler.executor(WorkloadProfile.BATCH,
				OperationFamily.RANGE_PAGE,
				RequestContext.NO_DEADLINE);
		TransitionMeasurement measurement;
		if (scenario.equals("cooperative-yield")) {
			runYieldWindow(scheduler, executor, threadMetrics, warmupOperations, false);
			measurement = runYieldWindow(scheduler, executor, threadMetrics, measuredOperations, true);
		} else {
			var target = new ParkTargetTask();
			var targetHandle = executor.executeCooperatively(target, 1L);
			awaitParked(scheduler,
					WorkloadProfile.BATCH,
					target.parked,
					1,
					target.rejection);
			runParkResumeWindow(scheduler,
					executor,
					targetHandle,
					target,
					threadMetrics,
					warmupOperations,
					false);
			measurement = runParkResumeWindow(scheduler,
					executor,
					targetHandle,
					target,
					threadMetrics,
					measuredOperations,
					true);
			if (!targetHandle.cancel()) {
				throw new IllegalStateException("park/resume target could not be cancelled after measurement");
			}
			awaitRejectionAndDrain(scheduler, WorkloadProfile.BATCH, target.rejection);
		}
		long elapsedNanos = measurement.elapsedNanos();
		long submitterAllocated = measurement.submitterAllocatedBytes();
		long workerAllocated = measurement.workerAllocatedBytes();
		boolean zeroAllocation = workerAllocated == 0L && submitterAllocated == 0L;
		double operationsPerSecond = measuredOperations * 1_000_000_000.0 / elapsedNanos;
		double allocatedBytesPerOperation = (submitterAllocated + workerAllocated)
				/ (double) measuredOperations;
		System.out.printf(java.util.Locale.ROOT,
				"scenario=%s operations=%d elapsed_nanos=%d operations_per_second=%.3f "
						+ "submitter_allocated_bytes=%d worker_allocated_bytes=%d "
						+ "allocated_bytes_per_operation=%.3f zero_scheduler_allocation=%s%n",
				scenario,
				measuredOperations,
				elapsedNanos,
				operationsPerSecond,
				submitterAllocated,
				workerAllocated,
				allocatedBytesPerOperation,
				zeroAllocation);
		if (!drainedAndConserved(scheduler.poolSnapshot(RWScheduler.Pool.READ))) {
			throw new IllegalStateException("scheduler did not drain after cooperative transition measurement");
		}
		if (!zeroAllocation) {
			throw new IllegalStateException("warmed cooperative transitions allocated scheduler memory: "
					+ "submitter=" + submitterAllocated + " worker=" + workerAllocated);
		}
	}

	private static TransitionMeasurement runYieldWindow(RWScheduler scheduler,
	                                                    RWScheduler.WorkloadExecutor executor,
	                                                    ThreadMXBean threadMetrics,
	                                                    int operations,
	                                                    boolean measured) {
		var task = new ControlledYieldTask(operations);
		var handle = executor.executeCooperatively(task, 1L);
		while (!task.entered) {
			Thread.onSpinWait();
		}
		long workerThreadId = measured
				? workerThreadId(scheduler, RWScheduler.Pool.READ, threadMetrics)
				: -1L;
		long submitterThreadId = Thread.currentThread().threadId();
		long workerAllocatedBefore = measured
				? threadMetrics.getThreadAllocatedBytes(workerThreadId)
				: 0L;
		long submitterAllocatedBefore = measured
				? threadMetrics.getThreadAllocatedBytes(submitterThreadId)
				: 0L;
		long startedNanos = measured ? System.nanoTime() : 0L;
		task.start = true;
		while (!task.done) {
			Thread.onSpinWait();
		}
		long elapsedNanos = measured ? System.nanoTime() - startedNanos : 0L;
		long submitterAllocated = measured
				? threadMetrics.getThreadAllocatedBytes(submitterThreadId) - submitterAllocatedBefore
				: 0L;
		long workerAllocated = measured
				? threadMetrics.getThreadAllocatedBytes(workerThreadId) - workerAllocatedBefore
				: 0L;
		task.release = true;
		while (!handle.isDisposed()) {
			Thread.onSpinWait();
		}
		awaitIdle(scheduler, WorkloadProfile.BATCH);
		if (task.rejection.get() != null) {
			throw new IllegalStateException("controlled YIELD task failed", task.rejection.get());
		}
		return new TransitionMeasurement(elapsedNanos, submitterAllocated, workerAllocated);
	}

	private static TransitionMeasurement runParkResumeWindow(RWScheduler scheduler,
	                                                         RWScheduler.WorkloadExecutor executor,
	                                                         RWScheduler.CooperativeHandle targetHandle,
	                                                         ParkTargetTask target,
	                                                         ThreadMXBean threadMetrics,
	                                                         int operations,
	                                                         boolean measured) {
		int parkedBefore = target.parked.get();
		var driver = new ControlledParkResumeDriver(targetHandle, operations);
		var driverHandle = executor.executeCooperatively(driver, 1L);
		while (!driver.entered) {
			Thread.onSpinWait();
		}
		long workerThreadId = measured
				? workerThreadId(scheduler, RWScheduler.Pool.READ, threadMetrics)
				: -1L;
		long submitterThreadId = Thread.currentThread().threadId();
		long workerAllocatedBefore = measured
				? threadMetrics.getThreadAllocatedBytes(workerThreadId)
				: 0L;
		long submitterAllocatedBefore = measured
				? threadMetrics.getThreadAllocatedBytes(submitterThreadId)
				: 0L;
		long startedNanos = measured ? System.nanoTime() : 0L;
		driver.start = true;
		while (!driver.done) {
			Thread.onSpinWait();
		}
		long elapsedNanos = measured ? System.nanoTime() - startedNanos : 0L;
		long submitterAllocated = measured
				? threadMetrics.getThreadAllocatedBytes(submitterThreadId) - submitterAllocatedBefore
				: 0L;
		long workerAllocated = measured
				? threadMetrics.getThreadAllocatedBytes(workerThreadId) - workerAllocatedBefore
				: 0L;
		if (target.parked.get() != parkedBefore + operations) {
			throw new IllegalStateException("park/resume transition count mismatch");
		}
		driver.release = true;
		while (!driverHandle.isDisposed()) {
			Thread.onSpinWait();
		}
		awaitIdle(scheduler, WorkloadProfile.BATCH);
		if (driver.rejection.get() != null) {
			throw new IllegalStateException("park/resume driver failed", driver.rejection.get());
		}
		return new TransitionMeasurement(elapsedNanos, submitterAllocated, workerAllocated);
	}

	private static void runCooperativeCancellation(RWScheduler scheduler,
	                                               ThreadMXBean threadMetrics,
	                                               int warmupOperations,
	                                               int measuredOperations) {
		var executor = scheduler.executor(WorkloadProfile.BATCH,
				OperationFamily.RANGE_PAGE,
				RequestContext.NO_DEADLINE);
		var task = new ParkUntilCancelledTask();
		runCancellationBatch(scheduler, executor, task, 0, warmupOperations);
		long workerThreadId = workerThreadId(scheduler, RWScheduler.Pool.READ, threadMetrics);
		long submitterThreadId = Thread.currentThread().threadId();
		long workerAllocatedBefore = threadMetrics.getThreadAllocatedBytes(workerThreadId);
		long submitterAllocatedBefore = threadMetrics.getThreadAllocatedBytes(submitterThreadId);
		long startedNanos = System.nanoTime();
		runCancellationBatch(scheduler, executor, task, warmupOperations, measuredOperations);
		long elapsedNanos = System.nanoTime() - startedNanos;
		long submitterAllocated = threadMetrics.getThreadAllocatedBytes(submitterThreadId)
				- submitterAllocatedBefore;
		long workerAllocated = threadMetrics.getThreadAllocatedBytes(workerThreadId) - workerAllocatedBefore;
		var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
		if (!drainedAndConserved(snapshot) || task.unexpectedFailure.get() != null) {
			throw new IllegalStateException("scheduler did not drain after cooperative cancellation",
					task.unexpectedFailure.get());
		}
		double operationsPerSecond = measuredOperations * 1_000_000_000.0 / elapsedNanos;
		double allocatedBytesPerOperation = (submitterAllocated + workerAllocated)
				/ (double) measuredOperations;
		System.out.printf(java.util.Locale.ROOT,
				"scenario=cooperative-cancel operations=%d elapsed_nanos=%d operations_per_second=%.3f "
						+ "submitter_allocated_bytes=%d worker_allocated_bytes=%d "
						+ "allocated_bytes_per_operation=%.3f%n",
				measuredOperations,
				elapsedNanos,
				operationsPerSecond,
				submitterAllocated,
				workerAllocated,
				allocatedBytesPerOperation);
	}

	private static void runSaturatedRejection(RWScheduler scheduler,
	                                           ThreadMXBean threadMetrics,
	                                           int warmupOperations,
	                                           int measuredOperations) {
		var executor = scheduler.executor(WorkloadProfile.INGEST,
				OperationFamily.POINT_LOOKUP,
				RequestContext.NO_DEADLINE);
		var blocker = new BlockingRunnable();
		var queued = new ImmediateRunnable();
		var rejected = new RejectedRunnable();
		executor.execute(blocker);
		while (!blocker.started) {
			Thread.onSpinWait();
		}
		executor.execute(queued);
		while (scheduler.activeTasks(WorkloadProfile.INGEST) != 1
				|| scheduler.queuedTasks(WorkloadProfile.INGEST) != 1) {
			Thread.onSpinWait();
		}
		runRejectionBatch(executor, rejected, 0, warmupOperations);
		long workerThreadId = workerThreadId(scheduler, RWScheduler.Pool.READ, threadMetrics);
		long submitterThreadId = Thread.currentThread().threadId();
		long workerAllocatedBefore = threadMetrics.getThreadAllocatedBytes(workerThreadId);
		long submitterAllocatedBefore = threadMetrics.getThreadAllocatedBytes(submitterThreadId);
		long startedNanos = System.nanoTime();
		runRejectionBatch(executor, rejected, warmupOperations, measuredOperations);
		long elapsedNanos = System.nanoTime() - startedNanos;
		long submitterAllocated = threadMetrics.getThreadAllocatedBytes(submitterThreadId)
				- submitterAllocatedBefore;
		long workerAllocated = threadMetrics.getThreadAllocatedBytes(workerThreadId) - workerAllocatedBefore;
		blocker.release = true;
		while (queued.completed.get() != 1) {
			Thread.onSpinWait();
		}
		awaitIdle(scheduler, WorkloadProfile.INGEST);
		var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
		if (!drainedAndConserved(snapshot) || rejected.ran) {
			throw new IllegalStateException("saturated rejection benchmark did not drain cleanly");
		}
		double operationsPerSecond = measuredOperations * 1_000_000_000.0 / elapsedNanos;
		double allocatedBytesPerOperation = (submitterAllocated + workerAllocated)
				/ (double) measuredOperations;
		System.out.printf(java.util.Locale.ROOT,
				"scenario=saturated-rejection operations=%d elapsed_nanos=%d operations_per_second=%.3f "
						+ "submitter_allocated_bytes=%d worker_allocated_bytes=%d "
						+ "allocated_bytes_per_operation=%.3f%n",
				measuredOperations,
				elapsedNanos,
				operationsPerSecond,
				submitterAllocated,
				workerAllocated,
				allocatedBytesPerOperation);
	}

	private static void runRejectionBatch(RWScheduler.WorkloadExecutor executor,
	                                      RejectedRunnable rejected,
	                                      int rejectedBefore,
	                                      int operations) {
		for (int operation = 1; operation <= operations; operation++) {
			try {
				executor.execute(rejected);
				throw new IllegalStateException("saturated submission was unexpectedly accepted");
			} catch (RocksDBException expectedRejection) {
				if (expectedRejection.getErrorUniqueId()
						!= RocksDBException.RocksDBErrorType.SERVER_OVERLOADED) {
					throw expectedRejection;
				}
				// The exception is part of the public rejection contract and therefore part of allocation.
			}
			if (rejected.rejections.get() != rejectedBefore + operation) {
				throw new IllegalStateException("rejection callback count mismatch");
			}
		}
	}

	private static void runCancellationBatch(RWScheduler scheduler,
	                                         RWScheduler.WorkloadExecutor executor,
	                                         ParkUntilCancelledTask task,
	                                         int completedBefore,
	                                         int operations) {
		for (int operation = 1; operation <= operations; operation++) {
			int expected = completedBefore + operation;
			var handle = executor.executeCooperatively(task, 1L);
			awaitParked(scheduler, WorkloadProfile.BATCH, task.parked, expected, task.unexpectedFailure);
			if (!handle.cancel()) {
				throw new IllegalStateException("cooperative cancellation lost before terminal arbitration");
			}
			while (task.cancelled.get() < expected) {
				if (task.unexpectedFailure.get() != null) {
					throw new IllegalStateException("cooperative cancellation failed",
							task.unexpectedFailure.get());
				}
				Thread.onSpinWait();
			}
			awaitIdle(scheduler, WorkloadProfile.BATCH);
		}
	}

	private static void awaitParked(RWScheduler scheduler,
	                                WorkloadProfile profile,
	                                AtomicInteger parked,
	                                int expectedParks,
	                                AtomicReference<RuntimeException> failure) {
		while (parked.get() < expectedParks
				|| scheduler.queuedTasks(profile) != 0
				|| scheduler.activeTasks(profile) != 0) {
			if (failure.get() != null) {
				throw new IllegalStateException("cooperative task failed before parking", failure.get());
			}
			Thread.onSpinWait();
		}
	}

	private static void awaitRejectionAndDrain(RWScheduler scheduler,
	                                           WorkloadProfile profile,
	                                           AtomicReference<RuntimeException> rejection) {
		while (rejection.get() == null) {
			Thread.onSpinWait();
		}
		awaitIdle(scheduler, profile);
	}

	private static void awaitIdle(RWScheduler scheduler, WorkloadProfile profile) {
		while (scheduler.queuedTasks(profile) != 0 || scheduler.activeTasks(profile) != 0) {
			Thread.onSpinWait();
		}
	}

	private static void runOperations(RWScheduler scheduler,
	                                  WorkloadProfile profile,
	                                  OperationSubmitter submitter,
	                                  AtomicInteger completed,
	                                  AtomicReference<RuntimeException> failure,
	                                  int completedBefore,
	                                  int operations) {
		int submitted = 0;
		while (submitted < operations) {
			int batch = Math.min(SUBMISSION_BATCH, operations - submitted);
			for (int i = 0; i < batch; i++) {
				submitter.submit();
			}
			submitted += batch;
			int expectedCompleted = completedBefore + submitted;
			while (completed.get() < expectedCompleted) {
				if (failure.get() != null) {
					throw new IllegalStateException("scheduler benchmark task failed", failure.get());
				}
				Thread.onSpinWait();
			}
			while (scheduler.queuedTasks(profile) != 0 || scheduler.activeTasks(profile) != 0) {
				Thread.onSpinWait();
			}
		}
	}

	private static long workerThreadId(RWScheduler scheduler,
	                                   RWScheduler.Pool pool,
	                                   ThreadMXBean threadMetrics) {
		String workerName = scheduler.poolSnapshot(pool).workerThreadNames().getFirst();
		for (long threadId : threadMetrics.getAllThreadIds()) {
			var threadInfo = threadMetrics.getThreadInfo(threadId);
			if (threadInfo != null && workerName.equals(threadInfo.getThreadName())) {
				return threadId;
			}
		}
		throw new IllegalStateException("scheduler worker thread is unavailable: " + workerName);
	}

	private static boolean drainedAndConserved(RWScheduler.PoolSnapshot snapshot) {
		int outstanding = BenchmarkSchedulerTelemetry.outstandingTasks(snapshot);
		int parked = BenchmarkSchedulerTelemetry.parkedTasks(snapshot, outstanding);
		return snapshot.queuedTasks() == 0
				&& snapshot.activeTasks() == 0
				&& parked == 0
				&& outstanding == 0
				&& BenchmarkSchedulerTelemetry.terminalOutcomes(snapshot)
				== BenchmarkSchedulerTelemetry.submissionAttempts(snapshot);
	}

	private static int integerArgument(String[] args, int index, int defaultValue) {
		return args.length > index ? Integer.parseInt(args[index]) : defaultValue;
	}

	@FunctionalInterface
	private interface OperationSubmitter {

		void submit();
	}

	private static final class ImmediateRunnable implements Runnable {

		private final AtomicInteger completed = new AtomicInteger();
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();

		@Override
		public void run() {
			completed.incrementAndGet();
		}
	}

	private static final class BlockingRunnable implements Runnable {

		private volatile boolean started;
		private volatile boolean release;

		@Override
		public void run() {
			started = true;
			while (!release) {
				Thread.onSpinWait();
			}
		}
	}

	private static final class RejectedRunnable implements Runnable, RWScheduler.RejectionAwareTask {

		private final AtomicInteger rejections = new AtomicInteger();
		private volatile boolean ran;

		@Override
		public void run() {
			ran = true;
		}

		@Override
		public void reject(RuntimeException failure) {
			rejections.incrementAndGet();
		}
	}

	private static final class ImmediateCompletionTask implements RWScheduler.CooperativeCompletionTask {

		private final AtomicInteger completed = new AtomicInteger();
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			return RWScheduler.CooperativeResult.COMPLETE;
		}

		@Override
		public void completeCooperatively() {
			completed.incrementAndGet();
		}

		@Override
		public void reject(RuntimeException failure) {
			this.failure.compareAndSet(null, failure);
		}
	}

	private record TransitionMeasurement(long elapsedNanos,
	                                     long submitterAllocatedBytes,
	                                     long workerAllocatedBytes) {
	}

	private static final class ControlledYieldTask implements RWScheduler.CooperativeTask {

		private final AtomicReference<RuntimeException> rejection = new AtomicReference<>();
		private int remainingYields;
		private volatile boolean entered;
		private volatile boolean start;
		private volatile boolean done;
		private volatile boolean release;

		private ControlledYieldTask(int remainingYields) {
			this.remainingYields = remainingYields;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			if (!entered) {
				entered = true;
				while (!start && !context.terminationRequested()) {
					Thread.onSpinWait();
				}
			}
			if (remainingYields > 0) {
				remainingYields--;
				return RWScheduler.CooperativeResult.YIELD;
			}
			done = true;
			while (!release && !context.terminationRequested()) {
				Thread.onSpinWait();
			}
			return RWScheduler.CooperativeResult.COMPLETE;
		}

		@Override
		public void reject(RuntimeException failure) {
			rejection.compareAndSet(null, failure);
		}
	}

	private static final class ParkTargetTask implements RWScheduler.CooperativeTask {

		private final AtomicInteger parked = new AtomicInteger();
		private final AtomicReference<RuntimeException> rejection = new AtomicReference<>();

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			parked.incrementAndGet();
			return RWScheduler.CooperativeResult.PARK;
		}

		@Override
		public void reject(RuntimeException failure) {
			rejection.compareAndSet(null, failure);
		}
	}

	private static final class ControlledParkResumeDriver implements RWScheduler.CooperativeTask {

		private final RWScheduler.CooperativeHandle target;
		private final AtomicReference<RuntimeException> rejection = new AtomicReference<>();
		private int remainingResumes;
		private volatile boolean entered;
		private volatile boolean start;
		private volatile boolean done;
		private volatile boolean release;

		private ControlledParkResumeDriver(RWScheduler.CooperativeHandle target, int remainingResumes) {
			this.target = target;
			this.remainingResumes = remainingResumes;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			if (!entered) {
				entered = true;
				while (!start && !context.terminationRequested()) {
					Thread.onSpinWait();
				}
			}
			if (remainingResumes > 0) {
				target.resume();
				remainingResumes--;
				return RWScheduler.CooperativeResult.YIELD;
			}
			done = true;
			while (!release && !context.terminationRequested()) {
				Thread.onSpinWait();
			}
			return RWScheduler.CooperativeResult.COMPLETE;
		}

		@Override
		public void reject(RuntimeException failure) {
			rejection.compareAndSet(null, failure);
		}
	}

	private static final class ParkUntilCancelledTask implements RWScheduler.CooperativeTask {

		private final AtomicInteger parked = new AtomicInteger();
		private final AtomicInteger cancelled = new AtomicInteger();
		private final AtomicReference<RuntimeException> unexpectedFailure = new AtomicReference<>();

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			parked.incrementAndGet();
			return RWScheduler.CooperativeResult.PARK;
		}

		@Override
		public void reject(RuntimeException failure) {
			if (!(failure instanceof CancellationException)) {
				unexpectedFailure.compareAndSet(null, failure);
			}
			cancelled.incrementAndGet();
		}
	}

}
