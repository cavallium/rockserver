package it.cavallium.rockserver.core.impl.benchmark;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.config.WorkloadSettings;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import reactor.core.Disposable;

/**
 * Reproduces the pressured cross-pool liveness topology behind prolonged backfill zero-progress
 * windows. Correctness transitions use barriers; wall-clock time is used only for the measured
 * nondispatchable interval and the bounded fair-turn latency.
 */
public final class AdversarialBatchLivenessBenchmark {

	private AdversarialBatchLivenessBenchmark() {
	}

	public static Result run(Config config) throws InterruptedException {
		var scheduler = new RWScheduler(settings(config), "adversarial-batch-liveness", null,
				"adversarial-batch-liveness");
		var foregroundStarted = new CountDownLatch(config.writeWorkers());
		var foregroundReleases = new CountDownLatch[config.writeWorkers()];
		var firstReadStarted = new CountDownLatch(1);
		var releaseFirstRead = new CountDownLatch(1);
		var writeBatchStarted = new CountDownLatch(1);
		var writeBatchCompleted = new CountDownLatch(1);
		var writeBatchStartNanos = new AtomicLong(Long.MIN_VALUE);
		var progress = new ProgressRecorder();
		var queuedReadHandles = new ArrayList<Disposable>();
		boolean topologyProven = false;
		long phaseStartNanos = Long.MIN_VALUE;
		long phaseEndNanos = Long.MIN_VALUE;
		long writeWorkerReleaseNanos = Long.MIN_VALUE;
		try {
			var foreground = scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.MUTATION,
					RequestContext.NO_DEADLINE);
			for (int index = 0; index < foregroundReleases.length; index++) {
				var release = new CountDownLatch(1);
				foregroundReleases[index] = release;
				foreground.execute(() -> {
					foregroundStarted.countDown();
					awaitUninterruptibly(release);
				});
			}
			require(foregroundStarted.await(config.setupTimeout().toNanos(), TimeUnit.NANOSECONDS),
					"not every WRITE worker reached the foreground barrier");
			var blockedWrite = scheduler.poolSnapshot(RWScheduler.Pool.WRITE);
			require(blockedWrite.activeTasks() == config.writeWorkers(),
					"WRITE active count does not prove that every worker is occupied");

			scheduler.setStoragePressure(true);
			var firstRead = scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE);
			firstRead.execute(() -> {
				firstReadStarted.countDown();
				awaitUninterruptibly(releaseFirstRead);
				progress.record();
			});
			require(firstReadStarted.await(config.setupTimeout().toNanos(), TimeUnit.NANOSECONDS),
					"the priming READ BATCH did not acquire the pressured permit");

			var writeBatch = scheduler.scheduler(WorkloadProfile.BATCH,
					OperationFamily.MUTATION,
					RequestContext.NO_DEADLINE);
			var writeHandle = writeBatch.schedule(() -> {
				writeBatchStartNanos.compareAndSet(Long.MIN_VALUE, System.nanoTime());
				writeBatchStarted.countDown();
				writeBatchCompleted.countDown();
			});
			var queuedWrite = scheduler.poolSnapshot(RWScheduler.Pool.WRITE);
			require(queuedWrite.activeTasks() == config.writeWorkers(),
					"WRITE foreground occupancy changed before the measured phase");
			require(queuedWrite.queuedByProfile().get(WorkloadProfile.BATCH) == 1,
					"WRITE BATCH was not queued behind the fully occupied WRITE pool");

			var readBatch = scheduler.scheduler(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE);
			for (int index = 0; index < config.queuedReadTasks(); index++) {
				queuedReadHandles.add(readBatch.schedule(progress::record));
			}
			var queuedRead = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			require(queuedRead.queuedByProfile().get(WorkloadProfile.BATCH)
						== config.queuedReadTasks(),
					"READ BATCH queue was not continuously runnable before release");
			topologyProven = true;

			releaseFirstRead.countDown();
			phaseStartNanos = progress.awaitCount(1, config.setupTimeout());
			phaseEndNanos = Math.addExact(phaseStartNanos, config.nondispatchablePhase().toNanos());
			progress.awaitDeadline(phaseEndNanos);
			var phase = progress.phase(phaseStartNanos, phaseEndNanos);
			require(writeBatchStarted.getCount() == 1L,
					"WRITE BATCH ran even though every WRITE worker remained barrier-blocked");

			writeWorkerReleaseNanos = System.nanoTime();
			foregroundReleases[0].countDown();
			require(writeBatchStarted.await(config.fairTurnBound().toNanos(), TimeUnit.NANOSECONDS),
					"WRITE did not receive its fair pressured turn after becoming dispatchable");
			require(writeBatchCompleted.await(config.setupTimeout().toNanos(), TimeUnit.NANOSECONDS),
					"WRITE BATCH did not complete after receiving its fair turn");
			long fairTurnDelayNanos = writeBatchStartNanos.get() - writeWorkerReleaseNanos;
			require(fairTurnDelayNanos >= 0L && fairTurnDelayNanos <= config.fairTurnBound().toNanos(),
					"WRITE fair-turn delay was outside the configured bound");

			writeHandle.dispose();
			return new Result(topologyProven,
					phase.usefulCompletions(),
					phase.maximumZeroProgressGapNanos(),
					phase.usefulThroughputPerSecond(),
					fairTurnDelayNanos,
					config.nondispatchablePhase().toNanos(),
					config.pressureInterval().toNanos());
		} finally {
			releaseFirstRead.countDown();
			for (var release : foregroundReleases) {
				if (release != null) release.countDown();
			}
			for (var handle : queuedReadHandles) handle.dispose();
			scheduler.setStoragePressure(false);
			scheduler.dispose();
			if (topologyProven) {
				require(scheduler.poolSnapshot(RWScheduler.Pool.READ).drainedAndConserved(),
						"READ pool did not drain and conserve ownership");
				require(scheduler.poolSnapshot(RWScheduler.Pool.WRITE).drainedAndConserved(),
						"WRITE pool did not drain and conserve ownership");
			}
		}
	}

	private static WorkloadSettings settings(Config config) {
		var base = WorkloadSettings.testingDefaults(config.readWorkers(),
				config.writeWorkers(), 1, 64, Math.max(64, config.queuedReadTasks() + 8));
		return new WorkloadSettings(base.readParallelism(),
				base.writeParallelism(),
				base.latencyQueueCapacity(),
				base.ingestQueueCapacity(),
				base.cdcQueueCapacity(),
				base.analyticalQueueCapacity(),
				base.batchQueueCapacity(),
				base.controlQueueCapacity(),
				base.physicalMaintenanceQueueCapacity(),
				base.readLatencyReservation(),
				base.readIngestReservation(),
				base.readCdcReservation(),
				base.writeLatencyReservation(),
				base.writeIngestReservation(),
				base.writeCdcReservation(),
				base.controlThreads(),
				base.physicalConcurrency(),
				base.analyticalActiveLimit(),
				base.retainedAnalyticalSnapshots(),
				base.retainedSnapshotMaximumAge(),
				base.latencyBurst(),
				base.ingestDrrWeight(),
				base.cdcDrrWeight(),
				base.analyticalDrrWeight(),
				base.batchDrrWeight(),
				base.competingBatchReadMaximumActive(),
				base.competingBatchWriteMaximumActive(),
				base.competingBatchWriteInterval(),
				1,
				config.pressureInterval(),
				base.rangeQuantumMaxItems(),
				base.rangeQuantumMaxBytes(),
				base.rangeQuantumMaxDuration(),
				base.rawScanFileConcurrency(),
				base.rawScanReadaheadBytes(),
				base.cdcQuantumMaxMutations(),
				base.cdcQuantumMaxBytes(),
				base.cdcQuantumMaxDuration(),
				base.latencyRangeMaxItems(),
				base.latencyRangeMaxBytes(),
				base.latencyFanOutMaxItems(),
				base.latencyFanOutMaxBytes());
	}

	private static void require(boolean condition, String message) {
		if (!condition) throw new IllegalStateException(message);
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		for (;;) {
			try {
				latch.await();
				break;
			} catch (InterruptedException _) {
				interrupted = true;
			}
		}
		if (interrupted) Thread.currentThread().interrupt();
	}

	public record Config(int readWorkers,
			int writeWorkers,
			int queuedReadTasks,
			Duration pressureInterval,
			Duration nondispatchablePhase,
			Duration setupTimeout,
			Duration fairTurnBound) {

		public Config {
			if (readWorkers < WorkloadSettings.MIN_PRODUCTION_DATA_THREADS
					|| writeWorkers < WorkloadSettings.MIN_PRODUCTION_DATA_THREADS) {
				throw new IllegalArgumentException("data pools require production-sized worker counts");
			}
			if (queuedReadTasks < 8) throw new IllegalArgumentException("queuedReadTasks must be at least 8");
			positive(pressureInterval, "pressureInterval");
			positive(nondispatchablePhase, "nondispatchablePhase");
			positive(setupTimeout, "setupTimeout");
			positive(fairTurnBound, "fairTurnBound");
			if (nondispatchablePhase.toNanos() < Math.multiplyExact(pressureInterval.toNanos(), 6L)) {
				throw new IllegalArgumentException("nondispatchablePhase must span at least six pressure intervals");
			}
		}

		private static void positive(Duration value, String name) {
			if (value == null || value.isZero() || value.isNegative()) {
				throw new IllegalArgumentException(name + " must be positive");
			}
		}
	}

	public record Result(boolean topologyProven,
			long usefulReadCompletions,
			long maximumReadZeroProgressGapNanos,
			double usefulReadThroughputPerSecond,
			long writeFairTurnDelayNanos,
			long nondispatchablePhaseNanos,
			long pressureIntervalNanos) {

		public void assertBaselineStall() {
			require(topologyProven, "adversarial topology was not proven");
			require(usefulReadCompletions == 0L,
					"historical baseline did not reproduce the nondispatchable-peer stall");
			require(maximumReadZeroProgressGapNanos >= nondispatchablePhaseNanos * 9L / 10L,
					"historical baseline zero-progress gap was shorter than the measured phase");
		}

		public void assertCandidateWorkConserving(Duration maximumGap, double minimumThroughput) {
			require(topologyProven, "adversarial topology was not proven");
			require(usefulReadCompletions >= 2L,
					"candidate did not make repeated READ progress while WRITE was nondispatchable");
			require(maximumReadZeroProgressGapNanos <= maximumGap.toNanos(),
					"candidate READ zero-progress gap exceeded the work-conserving bound");
			require(usefulReadThroughputPerSecond >= minimumThroughput,
					"candidate useful READ throughput fell below the configured bound");
		}
	}

	private static final class ProgressRecorder {

		private final ReentrantLock lock = new ReentrantLock();
		private final Condition changed = lock.newCondition();
		private final List<Long> completions = new ArrayList<>();

		void record() {
			lock.lock();
			try {
				completions.add(System.nanoTime());
				changed.signalAll();
			} finally {
				lock.unlock();
			}
		}

		long awaitCount(int count, Duration timeout) throws InterruptedException {
			long deadline = System.nanoTime() + timeout.toNanos();
			lock.lockInterruptibly();
			try {
				while (completions.size() < count) {
					long remaining = deadline - System.nanoTime();
					if (remaining <= 0L) throw new IllegalStateException("READ progress barrier timed out");
					changed.awaitNanos(remaining);
				}
				return completions.get(count - 1);
			} finally {
				lock.unlock();
			}
		}

		void awaitDeadline(long deadlineNanos) throws InterruptedException {
			lock.lockInterruptibly();
			try {
				long remaining;
				while ((remaining = deadlineNanos - System.nanoTime()) > 0L) {
					changed.awaitNanos(remaining);
				}
			} finally {
				lock.unlock();
			}
		}

		Phase phase(long startNanos, long endNanos) {
			lock.lock();
			try {
				long previous = startNanos;
				long maximumGap = 0L;
				long useful = 0L;
				for (long completion : completions) {
					if (completion <= startNanos || completion > endNanos) continue;
					maximumGap = Math.max(maximumGap, completion - previous);
					previous = completion;
					useful++;
				}
				maximumGap = Math.max(maximumGap, endNanos - previous);
				double throughput = useful * 1_000_000_000.0d / (endNanos - startNanos);
				return new Phase(useful, maximumGap, throughput);
			} finally {
				lock.unlock();
			}
		}
	}

	private record Phase(long usefulCompletions,
			long maximumZeroProgressGapNanos,
			double usefulThroughputPerSecond) {
	}
}
