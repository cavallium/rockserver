package it.cavallium.rockserver.core.impl.benchmark;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.WorkloadAdmission;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import reactor.core.Disposable;

/**
 * Fresh-process, direct scheduler benchmark for heavily contended mixed work.
 *
 * <p>The driver starts many concurrent producers against every workload profile and every
 * scheduler pool. The stream mixes normal, expired-deadline, immediately cancelled, cooperative
 * YIELD, and true PARK/resume work while storage pressure alternates. Correctness is a hard gate:
 * every submission must have exactly one terminal outcome, every pool must drain and conserve,
 * queues must stay inside their configured bounds, cancellation arbitration must balance, no task
 * may execute twice, and every profile must make useful progress. Throughput and latency are
 * measurements, not host-dependent pass/fail thresholds.</p>
 *
 * <p>Run this class in counterbalanced fresh JVMs when comparing revisions. Positional arguments
 * are operations, submitters, read workers, write workers, queue capacity, and seed. Defaults are
 * intentionally much larger than the ordinary Maven acceptance test.</p>
 */
public final class SchedulerHighContentionBenchmark {

	private static final Lane[] LANES = allowedLanes();
	private static final RWScheduler.Pool[] POOLS = RWScheduler.Pool.values();
	private static final WorkloadProfile[] PROFILES = WorkloadProfile.values();
	private static final OperationFamily[] FAMILIES = OperationFamily.values();
	private static final RWScheduler.TerminalOutcome[] OUTCOMES = RWScheduler.TerminalOutcome.values();
	private static final int TASK_NEW = 0;
	private static final int TASK_RUNNING = 1;
	private static final int TASK_RAN = 2;
	private static final int TASK_REJECTED = 3;
	private static volatile long blackhole;

	private SchedulerHighContentionBenchmark() {
	}

	public static void main(String[] args) throws Exception {
		int processors = Runtime.getRuntime().availableProcessors();
		int operations = integerArgument(args, 0, 1_000_000);
		int submitters = integerArgument(args, 1, Math.max(64, processors * 4));
		int readWorkers = integerArgument(args, 2, Math.max(8, processors));
		int writeWorkers = integerArgument(args, 3, Math.max(8, processors));
		int queueCapacity = integerArgument(args, 4, 65_536);
		long seed = longArgument(args, 5, 0x5EED_C0FFEE_2026L);
		int failurePercent = integerArgument(args, 6, 5);
		var config = new Config(operations,
				submitters,
				readWorkers,
				writeWorkers,
				Math.max(2, readWorkers / 4),
				queueCapacity,
				queueCapacity,
				256,
				4,
				2,
				5,
				10,
				failurePercent,
				30,
				true,
				seed,
				Duration.ofMinutes(3));
		int warmupOperations = Math.max(LANES.length * 16, Math.min(200_000, operations / 10));
		run(warmupConfig(config, warmupOperations));
		System.out.print(run(config).toReport());
	}

	private static Config warmupConfig(Config measured, int operations) {
		return new Config(operations,
				Math.min(measured.submitters(), 32),
				measured.readWorkers(),
				measured.writeWorkers(),
				measured.analyticalLimit(),
				Math.min(measured.foregroundQueueCapacity(), 8_192),
				Math.min(measured.batchQueueCapacity(), 8_192),
				measured.workTokens(),
				measured.cooperativeYields(),
				measured.cooperativeParks(),
				measured.expiredDeadlinePercent(),
				measured.cancellationPercent(),
				measured.failurePercent(),
				measured.cooperativePercent(),
				measured.alternateStoragePressure(),
				measured.seed() ^ 0xD1B54A32D192ED03L,
				measured.timeout());
	}

	public static Result run(Config config) throws Exception {
		Objects.requireNonNull(config, "config").validate();
		BenchmarkProcessTelemetry.enableAllocationMeasurement();
		var scheduler = RWScheduler.forTesting(config.readWorkers(),
				config.writeWorkers(),
				config.analyticalLimit(),
				config.foregroundQueueCapacity(),
				config.batchQueueCapacity(),
				"scheduler-high-contention");
		var state = new RunState(config, scheduler);
		var peaks = new BenchmarkProcessTelemetry.PeakSampler();
		peaks.reset();
		state.peaks = peaks;
		ExecutorService producers = Executors.newFixedThreadPool(config.submitters(),
				Thread.ofPlatform().name("scheduler-contention-submitter-", 0).factory());
		Thread monitor = Thread.ofPlatform()
				.name("scheduler-contention-monitor")
				.start(state::monitor);
		Thread resumer = Thread.ofPlatform()
				.name("scheduler-contention-resumer")
				.start(state::resumeParkedWork);
		var producerFailure = new ConcurrentLinkedQueue<Throwable>();
		var start = new CountDownLatch(1);
		var producersDone = new CountDownLatch(config.submitters());
		try (peaks) {
			if (config.alternateStoragePressure()) {
				state.setStoragePressure(true);
			}
			for (int producer = 0; producer < config.submitters(); producer++) {
				producers.execute(() -> {
					try {
						await(start);
						while (true) {
							int index = state.nextOperation.getAndIncrement();
							if (index >= config.operations()) {
								return;
							}
							state.submit(index);
						}
					} catch (Throwable failure) {
						producerFailure.add(failure);
					} finally {
						producersDone.countDown();
					}
				});
			}
			var processBefore = BenchmarkProcessTelemetry.processSnapshot();
			long startedNanos = System.nanoTime();
			state.startedNanos = startedNanos;
			start.countDown();
			if (!producersDone.await(config.timeout().toNanos(), TimeUnit.NANOSECONDS)) {
				throw new IllegalStateException("mixed-work producers did not finish within " + config.timeout());
			}
			if (!producerFailure.isEmpty()) {
				throw rethrow(producerFailure.peek());
			}
			long deadlineNanos = startedNanos + config.timeout().toNanos();
			while (state.terminalOutcomes() < config.operations() && System.nanoTime() < deadlineNanos) {
				LockSupport.parkNanos(100_000L);
			}
			state.elapsedNanos = System.nanoTime() - startedNanos;
			if (state.terminalOutcomes() != config.operations()) {
				throw new IllegalStateException("scheduler did not terminally account all work: expected="
						+ config.operations() + " actual=" + state.terminalOutcomes());
			}
			state.stop.set(true);
			monitor.join(TimeUnit.SECONDS.toMillis(5));
			resumer.join(TimeUnit.SECONDS.toMillis(5));
			if (monitor.isAlive() || resumer.isAlive()) {
				throw new IllegalStateException("benchmark coordination threads did not stop");
			}
			var process = BenchmarkProcessTelemetry.processSnapshot().minus(processBefore);
			var result = state.finish(process, peaks.peaks());
			result.assertCorrect();
			return result;
		} finally {
			start.countDown();
			state.stop.set(true);
			scheduler.setStoragePressure(false);
			producers.shutdownNow();
			producers.awaitTermination(5, TimeUnit.SECONDS);
			monitor.join(TimeUnit.SECONDS.toMillis(5));
			resumer.join(TimeUnit.SECONDS.toMillis(5));
			scheduler.disposeNow();
		}
	}

	public record Config(int operations,
			int submitters,
			int readWorkers,
			int writeWorkers,
			int analyticalLimit,
			int foregroundQueueCapacity,
			int batchQueueCapacity,
			int workTokens,
			int cooperativeYields,
			int cooperativeParks,
			int expiredDeadlinePercent,
			int cancellationPercent,
			int failurePercent,
			int cooperativePercent,
			boolean alternateStoragePressure,
			long seed,
			Duration timeout) {

		public void validate() {
			if (operations < LANES.length * 16) {
				throw new IllegalArgumentException("operations must exercise every lane repeatedly");
			}
			if (submitters < 2 || readWorkers < 1 || writeWorkers < 1 || analyticalLimit < 1) {
				throw new IllegalArgumentException("submitters and worker counts must be positive and contended");
			}
			if (foregroundQueueCapacity < 1 || batchQueueCapacity < 1 || workTokens < 1) {
				throw new IllegalArgumentException("queue capacities and work tokens must be positive");
			}
			if (cooperativeYields < 1 || cooperativeParks < 1) {
				throw new IllegalArgumentException("cooperative transition counts must be positive");
			}
			for (int percentage : new int[] {
				expiredDeadlinePercent, cancellationPercent, failurePercent, cooperativePercent
			}) {
				if (percentage < 0 || percentage > 100) {
					throw new IllegalArgumentException("percentages must be in [0,100]");
				}
			}
			if (timeout == null || timeout.isZero() || timeout.isNegative()) {
				throw new IllegalArgumentException("timeout must be positive");
			}
		}
	}

	public record ProfileResult(long attempts,
			long runs,
			long rejectedCallbacks,
			long queueP50Nanos,
			long queueP95Nanos,
			long queueP99Nanos,
			long executionP99Nanos,
			long endToEndP99Nanos,
			long maximumProgressGapNanos) {
	}

	public record FamilyResult(long attempts,
			long runs,
			long queueP99Nanos,
			long executionP99Nanos,
			long endToEndP99Nanos,
			long maximumProgressGapNanos) {
	}

	public record PoolResult(int workers,
			int peakActive,
			int peakQueued,
			int peakOutstanding,
			int queueBound,
			int outstandingBound,
			boolean batchLimitedObserved,
			RWScheduler.PoolSnapshot finalSnapshot) {
	}

	public record Result(Config config,
			long elapsedNanos,
			long attempts,
			long accepted,
			long runs,
			long expectedDeadlines,
			long cancellationRequests,
			long cancelledJobsThatRan,
			long injectedFailures,
			long yieldTransitions,
			long parkTransitions,
			long pressureTransitions,
			long duplicateExecutions,
			Map<RWScheduler.TerminalOutcome, Long> outcomes,
			Map<WorkloadProfile, ProfileResult> profiles,
			Map<OperationFamily, FamilyResult> families,
			Map<RWScheduler.Pool, PoolResult> pools,
			BenchmarkProcessTelemetry.ProcessDelta process,
			BenchmarkProcessTelemetry.Peaks peaks) {

		public double attemptsPerSecond() {
			return attempts * 1_000_000_000.0d / elapsedNanos;
		}

		public double usefulRunsPerSecond() {
			return runs * 1_000_000_000.0d / elapsedNanos;
		}

		public double cpuNanosPerAttempt() {
			return process.cpuNanos() / (double) attempts;
		}

		public double allocatedBytesPerAttempt() {
			return process.allocatedBytes() / (double) attempts;
		}

		public void assertCorrect() {
			if (attempts != config.operations()) {
				throw new IllegalStateException("submission attempt mismatch: " + attempts);
			}
			long terminal = outcomes.values().stream().mapToLong(Long::longValue).sum();
			if (terminal != attempts) {
				throw new IllegalStateException("terminal conservation mismatch: " + terminal + " != " + attempts);
			}
			if (outcomes.get(RWScheduler.TerminalOutcome.RUN) != runs) {
				throw new IllegalStateException("RUN outcome mismatch");
			}
			if (outcomes.get(RWScheduler.TerminalOutcome.DEADLINE) != expectedDeadlines) {
				throw new IllegalStateException("expired-deadline mismatch");
			}
			long expectedCancellations = cancellationRequests - cancelledJobsThatRan;
			if (outcomes.get(RWScheduler.TerminalOutcome.CANCELLATION) != expectedCancellations) {
				throw new IllegalStateException("cancellation arbitration mismatch: expected="
						+ expectedCancellations + " actual="
						+ outcomes.get(RWScheduler.TerminalOutcome.CANCELLATION));
			}
			if (outcomes.get(RWScheduler.TerminalOutcome.FAILURE) != injectedFailures
					|| outcomes.get(RWScheduler.TerminalOutcome.SHUTDOWN) != 0L) {
				throw new IllegalStateException("failure/shutdown outcome mismatch: " + outcomes);
			}
			long failedTasks = pools.values().stream()
					.mapToLong(pool -> pool.finalSnapshot().failedTasks())
					.sum();
			if (failedTasks != injectedFailures) {
				throw new IllegalStateException("failure telemetry mismatch: expected="
						+ injectedFailures + " actual=" + failedTasks);
			}
			if (accepted != runs + injectedFailures
					+ outcomes.get(RWScheduler.TerminalOutcome.CANCELLATION)) {
				throw new IllegalStateException("accepted task conservation mismatch");
			}
			long expectedOverload = attempts - accepted - expectedDeadlines;
			if (outcomes.get(RWScheduler.TerminalOutcome.OVERLOAD) != expectedOverload) {
				throw new IllegalStateException("overload accounting mismatch");
			}
			if (duplicateExecutions != 0L) {
				throw new IllegalStateException("tasks executed or terminated more than once: " + duplicateExecutions);
			}
			for (var profile : PROFILES) {
				var result = Objects.requireNonNull(profiles.get(profile), "missing profile " + profile);
				if (result.attempts() == 0L || result.runs() == 0L) {
					throw new IllegalStateException("profile made no useful progress: " + profile + " " + result);
				}
			}
			for (var family : FAMILIES) {
				var result = Objects.requireNonNull(families.get(family), "missing family " + family);
				if (result.attempts() == 0L || result.runs() == 0L) {
					throw new IllegalStateException("operation family made no useful progress: " + family + " " + result);
				}
			}
			if (yieldTransitions == 0L || parkTransitions == 0L) {
				throw new IllegalStateException("cooperative transitions were not exercised");
			}
			for (var entry : pools.entrySet()) {
				var pool = entry.getKey();
				var result = entry.getValue();
				if (!result.finalSnapshot().drainedAndConserved()) {
					throw new IllegalStateException("pool did not drain and conserve: " + pool);
				}
				if (result.finalSnapshot().startedTasks() != result.finalSnapshot().completedTasks()) {
					throw new IllegalStateException("started/completed imbalance: " + pool + " " + result);
				}
				if (result.peakActive() > result.workers()
						|| result.peakQueued() > result.queueBound()
						|| result.peakOutstanding() > result.outstandingBound()) {
					throw new IllegalStateException("pool exceeded configured bounds: " + pool + " " + result);
				}
			}
		}

		public String toReport() {
			var report = new StringBuilder();
			report.append("schema=rockserver-scheduler-high-contention-v2\n");
			report.append("operations=").append(attempts).append('\n');
			report.append("seed=").append(config.seed()).append('\n');
			report.append("submitters=").append(config.submitters()).append('\n');
			report.append("elapsed_nanos=").append(elapsedNanos).append('\n');
			report.append("attempts_per_second=")
					.append(String.format(Locale.ROOT, "%.3f", attemptsPerSecond())).append('\n');
			report.append("useful_runs_per_second=")
					.append(String.format(Locale.ROOT, "%.3f", usefulRunsPerSecond())).append('\n');
			report.append("accepted=").append(accepted).append('\n');
			report.append("runs=").append(runs).append('\n');
			report.append("yield_transitions=").append(yieldTransitions).append('\n');
			report.append("park_transitions=").append(parkTransitions).append('\n');
			report.append("pressure_transitions=").append(pressureTransitions).append('\n');
			report.append("injected_failures=").append(injectedFailures).append('\n');
			report.append("process.cpu_nanos=").append(process.cpuNanos()).append('\n');
			report.append("process.cpu_nanos_per_attempt=")
					.append(String.format(Locale.ROOT, "%.3f", cpuNanosPerAttempt())).append('\n');
			report.append("process.allocated_bytes=").append(process.allocatedBytes()).append('\n');
			report.append("process.allocated_bytes_per_attempt=")
					.append(String.format(Locale.ROOT, "%.3f", allocatedBytesPerAttempt())).append('\n');
			report.append("process.gc_collections=").append(process.gcCollections()).append('\n');
			report.append("process.gc_millis=").append(process.gcMillis()).append('\n');
			report.append("process.peak_live_heap_bytes=").append(peaks.liveHeapBytes()).append('\n');
			report.append("process.peak_direct_memory_bytes=").append(peaks.directMemoryBytes()).append('\n');
			report.append("process.peak_resident_set_bytes=").append(peaks.residentSetBytes()).append('\n');
			report.append("process.peak_threads=").append(peaks.threadCount()).append('\n');
			report.append("process.peak_native_handles=").append(peaks.nativeHandles()).append('\n');
			for (var outcome : OUTCOMES) {
				report.append("outcome.").append(outcome.name().toLowerCase(Locale.ROOT)).append('=')
						.append(outcomes.get(outcome)).append('\n');
			}
			for (var profile : PROFILES) {
				var result = profiles.get(profile);
				String prefix = "profile." + profile.name().toLowerCase(Locale.ROOT) + ".";
				report.append(prefix).append("attempts=").append(result.attempts()).append('\n');
				report.append(prefix).append("runs=").append(result.runs()).append('\n');
				report.append(prefix).append("queue_p99_nanos=").append(result.queueP99Nanos()).append('\n');
				report.append(prefix).append("execution_p99_nanos=").append(result.executionP99Nanos()).append('\n');
				report.append(prefix).append("end_to_end_p99_nanos=").append(result.endToEndP99Nanos()).append('\n');
				report.append(prefix).append("maximum_progress_gap_nanos=")
						.append(result.maximumProgressGapNanos()).append('\n');
			}
			for (var family : FAMILIES) {
				var result = families.get(family);
				String prefix = "family." + family.name().toLowerCase(Locale.ROOT) + ".";
				report.append(prefix).append("attempts=").append(result.attempts()).append('\n');
				report.append(prefix).append("runs=").append(result.runs()).append('\n');
				report.append(prefix).append("queue_p99_nanos=").append(result.queueP99Nanos()).append('\n');
				report.append(prefix).append("execution_p99_nanos=").append(result.executionP99Nanos()).append('\n');
				report.append(prefix).append("end_to_end_p99_nanos=").append(result.endToEndP99Nanos()).append('\n');
				report.append(prefix).append("maximum_progress_gap_nanos=")
						.append(result.maximumProgressGapNanos()).append('\n');
			}
			for (var pool : POOLS) {
				var result = pools.get(pool);
				String prefix = "pool." + pool.name().toLowerCase(Locale.ROOT) + ".";
				report.append(prefix).append("peak_active=").append(result.peakActive()).append('\n');
				report.append(prefix).append("peak_queued=").append(result.peakQueued()).append('\n');
				report.append(prefix).append("peak_outstanding=").append(result.peakOutstanding()).append('\n');
				report.append(prefix).append("queue_bound=").append(result.queueBound()).append('\n');
				report.append(prefix).append("outstanding_bound=").append(result.outstandingBound()).append('\n');
				report.append(prefix).append("batch_limited_observed=")
						.append(result.batchLimitedObserved()).append('\n');
			}
			return report.toString();
		}
	}

	private record Lane(WorkloadProfile profile, OperationFamily family) {

		private boolean cooperative() {
			return profile == WorkloadProfile.ANALYTICAL
					|| profile == WorkloadProfile.INGEST
					|| profile == WorkloadProfile.BATCH;
		}
	}

	private static final class RunState {

		private final Config config;
		private final RWScheduler scheduler;
		private final AtomicInteger nextOperation = new AtomicInteger();
		private final AtomicBoolean stop = new AtomicBoolean();
		private final ConcurrentLinkedQueue<ParkRegistration> parked = new ConcurrentLinkedQueue<>();
		private final LongAdder[] attempts = adders(PROFILES.length);
		private final LongAdder[] runs = adders(PROFILES.length);
		private final LongAdder[] rejectedCallbacks = adders(PROFILES.length);
		private final LongAdder[] familyAttempts = adders(FAMILIES.length);
		private final LongAdder[] familyRuns = adders(FAMILIES.length);
		private final LongAdder expectedDeadlines = new LongAdder();
		private final LongAdder cancellationRequests = new LongAdder();
		private final LongAdder cancelledJobsThatRan = new LongAdder();
		private final LongAdder injectedFailures = new LongAdder();
		private final LongAdder yieldTransitions = new LongAdder();
		private final LongAdder parkTransitions = new LongAdder();
		private final LongAdder pressureTransitions = new LongAdder();
		private final LongAdder duplicateExecutions = new LongAdder();
		private final long[] queueLatencyNanos;
		private final long[] executionNanos;
		private final long[] endToEndNanos;
		private final long[] completionNanos;
		private final boolean[] successfulRuns;
		private final byte[] profileOrdinals;
		private final byte[] familyOrdinals;
		private final PoolObservation[] poolObservations = new PoolObservation[POOLS.length];
		private final AtomicLong monitorSamples = new AtomicLong();
		private BenchmarkProcessTelemetry.PeakSampler peaks;
		private volatile long startedNanos;
		private volatile long elapsedNanos;

		private RunState(Config config, RWScheduler scheduler) {
			this.config = config;
			this.scheduler = scheduler;
			this.queueLatencyNanos = new long[config.operations()];
			this.executionNanos = new long[config.operations()];
			this.endToEndNanos = new long[config.operations()];
			this.completionNanos = new long[config.operations()];
			this.successfulRuns = new boolean[config.operations()];
			this.profileOrdinals = new byte[config.operations()];
			this.familyOrdinals = new byte[config.operations()];
			for (var pool : POOLS) {
				poolObservations[pool.ordinal()] = new PoolObservation();
			}
		}

		private void submit(int index) {
			long hash = mix64(config.seed() + index * 0x9E3779B97F4A7C15L);
			Lane lane = LANES[Math.floorMod((int) hash, LANES.length)];
			profileOrdinals[index] = (byte) lane.profile().ordinal();
			familyOrdinals[index] = (byte) lane.family().ordinal();
			attempts[lane.profile().ordinal()].increment();
			familyAttempts[lane.family().ordinal()].increment();
			boolean expired = lane.profile() == WorkloadProfile.LATENCY
					&& percent(hash) < config.expiredDeadlinePercent();
			boolean fail = !expired && percent(hash >>> 11) < config.failurePercent();
			boolean cancel = !expired && !fail && percent(hash >>> 17) < config.cancellationPercent();
			boolean cooperative = !expired && !fail && !cancel && lane.cooperative()
					&& percent(hash >>> 37) < config.cooperativePercent();
			boolean park = cooperative && (hash & 1L) == 0L;
			long submittedNanos = System.nanoTime();
			long estimatedBytes = 1L << (10 + Math.floorMod((int) (hash >>> 8), 16));
			int tokens = config.workTokens() * (1 + Math.floorMod((int) (hash >>> 29), 4));
			long deadline = expired ? System.currentTimeMillis() - 1L : RequestContext.NO_DEADLINE;
			try {
				if (fail) {
					var task = new FailingTask(this, index, lane, submittedNanos, tokens);
					if (lane.cooperative()) {
						scheduler.executor(lane.profile(), lane.family(), deadline)
								.executeCooperatively(task, estimatedBytes);
					} else {
						scheduler.executor(lane.profile(), lane.family(), deadline).execute(task, estimatedBytes);
					}
				} else if (cancel) {
					var task = new NormalTask(this, index, lane, submittedNanos, tokens, true);
					Disposable disposable = scheduler.scheduler(lane.profile(), lane.family(), deadline).schedule(task);
					cancellationRequests.increment();
					disposable.dispose();
				} else if (cooperative) {
					if (park) {
						var task = new ParkTask(this, index, lane, submittedNanos, tokens,
								config.cooperativeParks());
						var handle = scheduler.executor(lane.profile(), lane.family(), deadline)
								.executeCooperatively(task, estimatedBytes);
						parked.add(new ParkRegistration(task, handle));
					} else {
						var task = new YieldTask(this, index, lane, submittedNanos, tokens,
								config.cooperativeYields());
						scheduler.executor(lane.profile(), lane.family(), deadline)
								.executeCooperatively(task, estimatedBytes);
					}
				} else {
					var task = new NormalTask(this, index, lane, submittedNanos, tokens, false);
					scheduler.executor(lane.profile(), lane.family(), deadline).execute(task, estimatedBytes);
				}
			} catch (Throwable failure) {
				if (!expectedAdmissionFailure(failure)) {
					throw rethrow(failure);
				}
				if (expired) {
					expectedDeadlines.increment();
				}
			}
		}

		private void monitor() {
			long[][] telemetry = new long[POOLS.length][RWScheduler.POOL_TELEMETRY_LENGTH];
			long sample = 0L;
			while (!stop.get()) {
				if (config.alternateStoragePressure() && (sample & 31L) == 0L) {
					setStoragePressure((sample & 32L) == 0L);
				}
				for (var pool : POOLS) {
					scheduler.copyPoolTelemetry(pool, telemetry[pool.ordinal()]);
					poolObservations[pool.ordinal()].observe(telemetry[pool.ordinal()]);
				}
				monitorSamples.incrementAndGet();
				peaks.sample();
				sample++;
				LockSupport.parkNanos(50_000L);
			}
		}

		private void setStoragePressure(boolean pressured) {
			if (scheduler.isStoragePressure() != pressured) {
				scheduler.setStoragePressure(pressured);
				pressureTransitions.increment();
			}
		}

		private void resumeParkedWork() {
			while (!stop.get() || !parked.isEmpty()) {
				var registration = parked.poll();
				if (registration == null) {
					LockSupport.parkNanos(50_000L);
					continue;
				}
				if (registration.handle().isDisposed()) {
					continue;
				}
				int observed = registration.task().parks.get();
				if (registration.resumesSent < observed) {
					registration.handle().resume();
					registration.resumesSent++;
				}
				parked.add(registration);
			}
		}

		private long terminalOutcomes() {
			long terminal = 0L;
			for (var pool : POOLS) {
				terminal += scheduler.poolSnapshot(pool).terminalOutcomes();
			}
			return terminal;
		}

		private Result finish(BenchmarkProcessTelemetry.ProcessDelta process,
				BenchmarkProcessTelemetry.Peaks peaks) {
			var outcomeCounts = new EnumMap<RWScheduler.TerminalOutcome, Long>(RWScheduler.TerminalOutcome.class);
			for (var outcome : OUTCOMES) {
				outcomeCounts.put(outcome, 0L);
			}
			var poolResults = new EnumMap<RWScheduler.Pool, PoolResult>(RWScheduler.Pool.class);
			long accepted = 0L;
			long submissionAttempts = 0L;
			long[] snapshotProfileAttempts = new long[PROFILES.length];
			for (var pool : POOLS) {
				var snapshot = scheduler.poolSnapshot(pool);
				accepted += snapshot.acceptedTasks();
				submissionAttempts += snapshot.submissionAttempts();
				for (var profile : PROFILES) {
					snapshotProfileAttempts[profile.ordinal()] += snapshot.submissionAttemptsByProfile()
							.getOrDefault(profile, 0L);
				}
				for (var outcome : OUTCOMES) {
					outcomeCounts.merge(outcome, snapshot.outcomes().getOrDefault(outcome, 0L), Long::sum);
				}
				var observed = poolObservations[pool.ordinal()];
				poolResults.put(pool, new PoolResult(snapshot.workerCount(),
						observed.peakActive,
						observed.peakQueued,
						observed.peakOutstanding,
						queueBound(config, pool, snapshot.workerCount()),
						outstandingBound(config, pool, snapshot.workerCount()),
						observed.batchLimited,
						snapshot));
			}
			var profileResults = new EnumMap<WorkloadProfile, ProfileResult>(WorkloadProfile.class);
			for (var profile : PROFILES) {
				long expectedAttempts = attempts[profile.ordinal()].sum();
				if (snapshotProfileAttempts[profile.ordinal()] != expectedAttempts) {
					throw new IllegalStateException("profile attempt mismatch for " + profile + ": expected="
							+ expectedAttempts + " actual=" + snapshotProfileAttempts[profile.ordinal()]);
				}
				long[] queueSamples = samples(profile, queueLatencyNanos);
				long[] executionSamples = samples(profile, executionNanos);
				long[] endToEndSamples = samples(profile, endToEndNanos);
				profileResults.put(profile, new ProfileResult(expectedAttempts,
						runs[profile.ordinal()].sum(),
						rejectedCallbacks[profile.ordinal()].sum(),
						quantile(queueSamples, 0.50d),
						quantile(queueSamples, 0.95d),
						quantile(queueSamples, 0.99d),
						quantile(executionSamples, 0.99d),
						quantile(endToEndSamples, 0.99d),
						maximumProgressGap(profile, null)));
			}
			var familyResults = new EnumMap<OperationFamily, FamilyResult>(OperationFamily.class);
			for (var family : FAMILIES) {
				long[] queueSamples = samples(null, family, queueLatencyNanos);
				long[] executionSamples = samples(null, family, executionNanos);
				long[] endToEndSamples = samples(null, family, endToEndNanos);
				familyResults.put(family, new FamilyResult(
						familyAttempts[family.ordinal()].sum(),
						familyRuns[family.ordinal()].sum(),
						quantile(queueSamples, 0.99d),
						quantile(executionSamples, 0.99d),
						quantile(endToEndSamples, 0.99d),
						maximumProgressGap(null, family)));
			}
			return new Result(config,
					elapsedNanos,
					submissionAttempts,
					accepted,
					Arrays.stream(runs).mapToLong(LongAdder::sum).sum(),
					expectedDeadlines.sum(),
					cancellationRequests.sum(),
					cancelledJobsThatRan.sum(),
					injectedFailures.sum(),
					yieldTransitions.sum(),
					parkTransitions.sum(),
					pressureTransitions.sum(),
					duplicateExecutions.sum(),
					Map.copyOf(outcomeCounts),
					Map.copyOf(profileResults),
					Map.copyOf(familyResults),
					Map.copyOf(poolResults),
					process,
					peaks);
		}

		private long[] samples(WorkloadProfile profile, long[] values) {
			return samples(profile, null, values);
		}

		private long[] samples(WorkloadProfile profile, OperationFamily family, long[] values) {
			long sampleCount = profile != null
					? runs[profile.ordinal()].sum()
					: familyRuns[Objects.requireNonNull(family, "family").ordinal()].sum();
			long[] selected = new long[Math.toIntExact(sampleCount)];
			int target = 0;
			for (int index = 0; index < values.length; index++) {
				boolean matches = profile != null
						? profileOrdinals[index] == profile.ordinal()
						: familyOrdinals[index] == family.ordinal();
				if (matches
						&& successfulRuns[index]
						&& values[index] > 0L) {
					selected[target++] = values[index];
				}
			}
			if (target != selected.length) {
				throw new IllegalStateException("latency sample mismatch for "
						+ (profile != null ? profile : family) + ": expected="
						+ selected.length + " actual=" + target);
			}
			Arrays.sort(selected);
			return selected;
		}

		private long maximumProgressGap(WorkloadProfile profile, OperationFamily family) {
			long[] completions = samples(profile, family, completionNanos);
			if (completions.length == 0) {
				return elapsedNanos;
			}
			long previous = startedNanos;
			long maximum = 0L;
			for (long completion : completions) {
				maximum = Math.max(maximum, completion - previous);
				previous = completion;
			}
			return Math.max(maximum, startedNanos + elapsedNanos - previous);
		}
	}

	private abstract static class TaskBase implements RWScheduler.RejectionAwareTask {

		final RunState owner;
		final int index;
		final Lane lane;
		final long submittedNanos;
		final int tokens;
		final AtomicInteger state = new AtomicInteger(TASK_NEW);
		final boolean failureExpectedWhileRunning;
		long started;
		long executionTotalNanos;

		TaskBase(RunState owner,
				int index,
				Lane lane,
				long submittedNanos,
				int tokens,
				boolean failureExpectedWhileRunning) {
			this.owner = owner;
			this.index = index;
			this.lane = lane;
			this.submittedNanos = submittedNanos;
			this.tokens = tokens;
			this.failureExpectedWhileRunning = failureExpectedWhileRunning;
		}

		final boolean start() {
			if (!state.compareAndSet(TASK_NEW, TASK_RUNNING)) {
				owner.duplicateExecutions.increment();
				return false;
			}
			started = System.nanoTime();
			owner.queueLatencyNanos[index] = Math.max(1L, started - submittedNanos);
			return true;
		}

		final void completeRun() {
			long completedNanos = System.nanoTime();
			owner.executionNanos[index] = Math.max(1L, executionTotalNanos);
			owner.endToEndNanos[index] = Math.max(1L, completedNanos - submittedNanos);
			if (!state.compareAndSet(TASK_RUNNING, TASK_RAN)) {
				owner.duplicateExecutions.increment();
				return;
			}
			owner.runs[lane.profile().ordinal()].increment();
			owner.familyRuns[lane.family().ordinal()].increment();
			owner.completionNanos[index] = completedNanos;
			owner.successfulRuns[index] = true;
		}

		final void executeTokens() {
			long before = System.nanoTime();
			consumeCpu(tokens);
			executionTotalNanos += Math.max(1L, System.nanoTime() - before);
		}

		@Override
		public final void reject(RuntimeException failure) {
			int expected = failureExpectedWhileRunning ? TASK_RUNNING : TASK_NEW;
			if (!state.compareAndSet(expected, TASK_REJECTED)
					&& !(failureExpectedWhileRunning && state.compareAndSet(TASK_NEW, TASK_REJECTED))) {
				owner.duplicateExecutions.increment();
				return;
			}
			owner.rejectedCallbacks[lane.profile().ordinal()].increment();
		}
	}

	private static final class NormalTask extends TaskBase implements Runnable {

		private final boolean cancellationCandidate;

		private NormalTask(RunState owner,
				int index,
				Lane lane,
				long submittedNanos,
				int tokens,
				boolean cancellationCandidate) {
			super(owner, index, lane, submittedNanos, tokens, false);
			this.cancellationCandidate = cancellationCandidate;
		}

		@Override
		public void run() {
			if (!start()) {
				return;
			}
			executeTokens();
			completeRun();
			if (cancellationCandidate) {
				owner.cancelledJobsThatRan.increment();
			}
		}
	}

	private static final class FailingTask extends TaskBase
			implements Runnable, RWScheduler.CooperativeTask {

		private FailingTask(RunState owner,
				int index,
				Lane lane,
				long submittedNanos,
				int tokens) {
			super(owner, index, lane, submittedNanos, tokens, true);
		}

		@Override
		public void run() {
			if (!start()) {
				return;
			}
			executeTokens();
			owner.executionNanos[index] = Math.max(1L, executionTotalNanos);
			owner.endToEndNanos[index] = Math.max(1L, System.nanoTime() - submittedNanos);
			owner.injectedFailures.increment();
			throw new InjectedWorkloadFailure(index);
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			if (!start()) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			executeTokens();
			owner.executionNanos[index] = Math.max(1L, executionTotalNanos);
			owner.endToEndNanos[index] = Math.max(1L, System.nanoTime() - submittedNanos);
			owner.injectedFailures.increment();
			context.fail(new InjectedWorkloadFailure(index));
			return RWScheduler.CooperativeResult.COMPLETE;
		}
	}

	private static final class InjectedWorkloadFailure extends RuntimeException {

		private InjectedWorkloadFailure(int index) {
			super("synthetic mixed-work failure " + index, null, false, false);
		}
	}

	private static final class YieldTask extends TaskBase implements RWScheduler.CooperativeTask {

		private int remainingYields;

		private YieldTask(RunState owner,
				int index,
				Lane lane,
				long submittedNanos,
				int tokens,
				int remainingYields) {
			super(owner, index, lane, submittedNanos, tokens, false);
			this.remainingYields = remainingYields;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			if (state.get() == TASK_NEW && !start()) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			executeTokens();
			if (remainingYields-- > 0) {
				owner.yieldTransitions.increment();
				return RWScheduler.CooperativeResult.YIELD;
			}
			completeRun();
			return RWScheduler.CooperativeResult.COMPLETE;
		}
	}

	private static final class ParkTask extends TaskBase implements RWScheduler.CooperativeTask {

		private final AtomicInteger parks = new AtomicInteger();
		private int remainingParks;

		private ParkTask(RunState owner,
				int index,
				Lane lane,
				long submittedNanos,
				int tokens,
				int remainingParks) {
			super(owner, index, lane, submittedNanos, tokens, false);
			this.remainingParks = remainingParks;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			if (state.get() == TASK_NEW && !start()) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			executeTokens();
			if (remainingParks-- > 0) {
				parks.incrementAndGet();
				owner.parkTransitions.increment();
				return RWScheduler.CooperativeResult.PARK;
			}
			completeRun();
			return RWScheduler.CooperativeResult.COMPLETE;
		}
	}

	private static final class ParkRegistration {

		private final ParkTask task;
		private final RWScheduler.CooperativeHandle handle;
		private int resumesSent;

		private ParkRegistration(ParkTask task, RWScheduler.CooperativeHandle handle) {
			this.task = task;
			this.handle = handle;
		}

		private ParkTask task() {
			return task;
		}

		private RWScheduler.CooperativeHandle handle() {
			return handle;
		}
	}

	private static final class PoolObservation {

		private int peakActive;
		private int peakQueued;
		private int peakOutstanding;
		private boolean batchLimited;

		private void observe(long[] telemetry) {
			peakActive = Math.max(peakActive,
					Math.toIntExact(telemetry[RWScheduler.POOL_TELEMETRY_ACTIVE_TASKS]));
			peakQueued = Math.max(peakQueued,
					Math.toIntExact(telemetry[RWScheduler.POOL_TELEMETRY_QUEUED_TASKS]));
			peakOutstanding = Math.max(peakOutstanding,
					Math.toIntExact(telemetry[RWScheduler.POOL_TELEMETRY_OUTSTANDING_TASKS]));
			batchLimited |= telemetry[RWScheduler.POOL_TELEMETRY_BATCH_LIMITED] != 0L;
		}
	}

	private static int queueBound(Config config, RWScheduler.Pool pool, int workers) {
		int cdc = Math.max(64, Math.min(config.foregroundQueueCapacity(), 1_024));
		int analytical = Math.max(1, Math.min(config.batchQueueCapacity(), 512));
		return switch (pool) {
			// A running cooperative task may YIELD after a producer fills the queue
			// slot it vacated. The scheduler therefore permits at most one worker-set
			// of transient requeues above the sum of static profile capacities.
			case READ -> config.foregroundQueueCapacity() * 2 + cdc + analytical
					+ config.batchQueueCapacity() + workers;
			case WRITE -> config.foregroundQueueCapacity() * 2 + cdc
					+ config.batchQueueCapacity() + workers;
			case CONTROL -> 256;
			case PHYSICAL -> 16;
		};
	}

	private static int outstandingBound(Config config, RWScheduler.Pool pool, int workers) {
		int cdc = Math.max(64, Math.min(config.foregroundQueueCapacity(), 1_024));
		int analytical = Math.max(1, Math.min(config.batchQueueCapacity(), 512));
		return switch (pool) {
			// Each profile has an independently checked capacity + workerCount
			// outstanding ceiling. Summing those explicit scheduler bounds remains
			// valid even when multiple profiles are PARKED simultaneously.
			case READ -> config.foregroundQueueCapacity() * 2 + cdc + analytical
					+ config.batchQueueCapacity() + workers * 5;
			case WRITE -> config.foregroundQueueCapacity() * 2 + cdc
					+ config.batchQueueCapacity() + workers * 4;
			case CONTROL -> 256 + workers;
			case PHYSICAL -> 16 + workers;
		};
	}

	private static long quantile(long[] sorted, double quantile) {
		if (sorted.length == 0) {
			return 0L;
		}
		int index = (int) Math.ceil(quantile * sorted.length) - 1;
		return sorted[Math.max(0, Math.min(sorted.length - 1, index))];
	}

	private static LongAdder[] adders(int size) {
		var result = new LongAdder[size];
		for (int i = 0; i < size; i++) {
			result[i] = new LongAdder();
		}
		return result;
	}

	private static Lane[] allowedLanes() {
		var lanes = new ArrayList<Lane>();
		for (var profile : WorkloadProfile.values()) {
			for (var family : OperationFamily.values()) {
				if (WorkloadAdmission.isAllowed(profile, family)) {
					lanes.add(new Lane(profile, family));
				}
			}
		}
		return lanes.toArray(Lane[]::new);
	}

	private static int percent(long value) {
		return Math.floorMod((int) value, 100);
	}

	private static long mix64(long value) {
		value = (value ^ (value >>> 30)) * 0xBF58476D1CE4E5B9L;
		value = (value ^ (value >>> 27)) * 0x94D049BB133111EBL;
		return value ^ (value >>> 31);
	}

	private static void consumeCpu(int tokens) {
		long value = blackhole ^ tokens;
		for (int i = 0; i < tokens; i++) {
			value = value * 0x9E3779B97F4A7C15L + i;
			value ^= value >>> 29;
		}
		blackhole = value;
	}

	private static boolean expectedAdmissionFailure(Throwable failure) {
		for (Throwable current = failure; current != null; current = current.getCause()) {
			if (current instanceof RocksDBException rocks) {
				return rocks.getErrorUniqueId() == RocksDBException.RocksDBErrorType.SERVER_OVERLOADED
						|| rocks.getErrorUniqueId() == RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED;
			}
			if (current instanceof RejectedExecutionException && current.getCause() == null) {
				return true;
			}
		}
		return false;
	}

	private static RuntimeException rethrow(Throwable failure) {
		if (failure instanceof RuntimeException runtime) {
			return runtime;
		}
		if (failure instanceof Error error) {
			throw error;
		}
		return new IllegalStateException(failure);
	}

	private static void await(CountDownLatch latch) {
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

	private static int integerArgument(String[] args, int index, int fallback) {
		return args.length > index ? Integer.parseInt(args[index]) : fallback;
	}

	private static long longArgument(String[] args, int index, long fallback) {
		return args.length > index ? Long.parseLong(args[index]) : fallback;
	}
}
