package it.cavallium.rockserver.core.impl.benchmark;

import com.sun.management.ThreadMXBean;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Fresh-process scheduler microbenchmark for direct, indexed, and cooperative admission/dispatch.
 *
 * <p>This deliberately reuses the command and keeps its own coordination outside the measured
 * scheduler allocation. Select {@code normal}, {@code indexed}, or {@code cooperative} as the
 * first argument. Run multiple counterbalanced baseline/candidate subprocess pairs; one invocation
 * is not a release-performance claim.</p>
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

		var scheduler = RWScheduler.forTesting(
				1, 1, 1, SUBMISSION_BATCH * 2, SUBMISSION_BATCH * 2, "scheduler-hot-path");
		var cooperativeTask = new ImmediateCompletionTask();
		var normalTask = new ImmediateRunnable();
		try {
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
			long workerThreadId = workerThreadId(scheduler, threadMetrics);
			long submitterThreadId = Thread.currentThread().threadId();
			long submitterAllocatedBefore = threadMetrics.getThreadAllocatedBytes(submitterThreadId);
			long workerAllocatedBefore = threadMetrics.getThreadAllocatedBytes(workerThreadId);
			long startedNanos = System.nanoTime();
			runOperations(scheduler,
					profile,
					submitter,
					completed,
					failure,
					warmupOperations,
					measuredOperations);
			long elapsedNanos = System.nanoTime() - startedNanos;
			long workerAllocated = threadMetrics.getThreadAllocatedBytes(workerThreadId) - workerAllocatedBefore;
			long submitterAllocated = threadMetrics.getThreadAllocatedBytes(submitterThreadId)
					- submitterAllocatedBefore;
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			if (!snapshot.drainedAndConserved() || failure.get() != null) {
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

	private static long workerThreadId(RWScheduler scheduler, ThreadMXBean threadMetrics) {
		String workerName = scheduler.poolSnapshot(RWScheduler.Pool.READ).workerThreadNames().getFirst();
		for (long threadId : threadMetrics.getAllThreadIds()) {
			var threadInfo = threadMetrics.getThreadInfo(threadId);
			if (threadInfo != null && workerName.equals(threadInfo.getThreadName())) {
				return threadId;
			}
		}
		throw new IllegalStateException("scheduler worker thread is unavailable: " + workerName);
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
}
