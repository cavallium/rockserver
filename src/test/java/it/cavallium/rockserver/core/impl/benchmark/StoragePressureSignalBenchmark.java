package it.cavallium.rockserver.core.impl.benchmark;

import com.sun.management.ThreadMXBean;
import it.cavallium.rockserver.core.impl.StoragePressureSignal;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Allocation and scaling benchmark for the exact production storage-pressure evaluator.
 *
 * <p>The matrix contains the production-sized 1/64/256/1024/4096-column shapes and every
 * independent signal path. Input arrays and the evaluator are reused, so the measured region is
 * the production primitive hot path rather than benchmark setup. Every case is checked against an
 * independent oracle before timing, and the process fails if a warmed evaluation allocates.</p>
 *
 * <p>Run immutable baseline/candidate builds as fresh, serial, counterbalanced JVMs. The report is
 * structural development-host evidence until repeated on representative production hardware.</p>
 */
public final class StoragePressureSignalBenchmark {

	private static final long DEFAULT_SOFT_LIMIT = 64L * 1024L * 1024L * 1024L;
	private static final int NO_REASON = 0;
	private static volatile long blackhole;

	private StoragePressureSignalBenchmark() {
	}

	public static void main(String[] args) {
		Config config = Config.parse(args);
		Result result = run(config);
		result.assertCorrectAndAllocationFree();
		System.out.print(result.toReport());
	}

	public static Result run(Config config) {
		Objects.requireNonNull(config, "config").validate();
		ThreadMXBean threads = threadBean();
		long threadId = Thread.currentThread().threadId();
		// Exercise the exact timed loop once outside the reported matrix. A fresh JVM can
		// deoptimize/OSR-compile this loop after its ordinary warmup loop has completed;
		// those one-time runtime allocations belong to the harness, not the evaluator.
		var calibration = PreparedCase.create(Scenario.NO_PRESSURE, 1);
		int calibrationEvaluations = Math.max(config.minimumEvaluations(),
				Math.min(config.maximumEvaluations(), 100_000));
		measure(calibration, threads, threadId, calibrationEvaluations, config.latencySampleStride());
		var results = new ArrayList<CaseResult>();
		for (int columnFamilies : config.columnFamilyCounts()) {
			for (Scenario scenario : Scenario.values()) {
				var prepared = PreparedCase.create(scenario, columnFamilies);
				prepared.assertOracle(0L);
				prepared.assertOracle(1L);
				int warmupEvaluations = evaluationsFor(config.warmupColumnObservations(),
						columnFamilies,
						config.minimumEvaluations(),
						config.maximumEvaluations());
				int measuredEvaluations = evaluationsFor(config.measuredColumnObservations(),
						columnFamilies,
						config.minimumEvaluations(),
						config.maximumEvaluations());
				for (int iteration = 0; iteration < warmupEvaluations; iteration++) {
					blackhole ^= prepared.evaluate(iteration);
					blackhole ^= prepared.signal.pressured() ? 1L : 0L;
				}
				results.add(measure(prepared,
						threads,
						threadId,
						measuredEvaluations,
						config.latencySampleStride()));
			}
		}
		return new Result(config, List.copyOf(results));
	}

	private static CaseResult measure(PreparedCase prepared,
			ThreadMXBean threads,
			long threadId,
			int evaluations,
			int sampleStride) {
		int sampleCount = Math.toIntExact((evaluations + (long) sampleStride - 1L) / sampleStride);
		long[] latencySamples = new long[sampleCount];
		long pressuredEvaluations = 0L;
		long localBlackhole = 0L;
		int sample = 0;
		long cpuBefore = threads.getCurrentThreadCpuTime();
		long allocatedBefore = threads.getThreadAllocatedBytes(threadId);
		long startedNanos = System.nanoTime();
		for (int iteration = 0; iteration < evaluations; iteration++) {
			long value;
			if (iteration % sampleStride == 0) {
				long sampleStarted = System.nanoTime();
				value = prepared.evaluate(iteration);
				latencySamples[sample++] = Math.max(1L, System.nanoTime() - sampleStarted);
			} else {
				value = prepared.evaluate(iteration);
			}
			localBlackhole ^= value;
			pressuredEvaluations += prepared.signal.pressured() ? 1L : 0L;
		}
		long elapsedNanos = System.nanoTime() - startedNanos;
		long allocatedBytes = threads.getThreadAllocatedBytes(threadId) - allocatedBefore;
		long cpuNanos = threads.getCurrentThreadCpuTime() - cpuBefore;
		blackhole ^= localBlackhole;
		Arrays.sort(latencySamples);
		return new CaseResult(prepared.scenario,
				prepared.columnFamilies,
				evaluations,
				Math.multiplyExact((long) evaluations, prepared.columnFamilies),
				elapsedNanos,
				cpuNanos,
				allocatedBytes,
				pressuredEvaluations,
				prepared.expectedPressuredEvaluations(evaluations),
				quantile(latencySamples, 0.50d),
				quantile(latencySamples, 0.95d),
				quantile(latencySamples, 0.99d));
	}

	private static int evaluationsFor(long targetColumnObservations,
			int columnFamilies,
			int minimum,
			int maximum) {
		long target = Math.max(1L, targetColumnObservations / columnFamilies);
		return Math.toIntExact(Math.max(minimum, Math.min(maximum, target)));
	}

	private static ThreadMXBean threadBean() {
		if (!(ManagementFactory.getThreadMXBean() instanceof ThreadMXBean threads)
				|| !threads.isThreadAllocatedMemorySupported()
				|| !threads.isCurrentThreadCpuTimeSupported()) {
			throw new IllegalStateException("HotSpot thread CPU/allocation telemetry is unavailable");
		}
		if (!threads.isThreadAllocatedMemoryEnabled()) {
			threads.setThreadAllocatedMemoryEnabled(true);
		}
		if (!threads.isThreadCpuTimeEnabled()) {
			threads.setThreadCpuTimeEnabled(true);
		}
		long threadId = Thread.currentThread().threadId();
		for (int warmup = 0; warmup < 256; warmup++) {
			threads.getThreadAllocatedBytes(threadId);
			threads.getCurrentThreadCpuTime();
		}
		return threads;
	}

	private static long quantile(long[] sorted, double quantile) {
		if (sorted.length == 0) return 0L;
		int index = (int) Math.ceil(quantile * sorted.length) - 1;
		return sorted[Math.max(0, Math.min(sorted.length - 1, index))];
	}

	public enum Scenario {
		NO_PRESSURE,
		FIRST_COLUMN_TRIGGER,
		LAST_COLUMN_TRIGGER,
		ACTUAL_DELAYED_WRITE,
		WRITE_STOPPED,
		DISABLED_LIMITS,
		OVERRIDE_LAST_COLUMN_TRIGGER,
		UNSIGNED_LAST_COLUMN_TRIGGER,
		INVALID_LIMIT,
		SIGNAL_FAILURE,
		FREQUENT_TRANSITIONS
	}

	public record Config(int[] columnFamilyCounts,
			long warmupColumnObservations,
			long measuredColumnObservations,
			int minimumEvaluations,
			int maximumEvaluations,
			int latencySampleStride,
			String buildId) {

		public Config {
			columnFamilyCounts = columnFamilyCounts.clone();
		}

		@Override
		public int[] columnFamilyCounts() {
			return columnFamilyCounts.clone();
		}

		public void validate() {
			if (columnFamilyCounts.length == 0) {
				throw new IllegalArgumentException("at least one column-family count is required");
			}
			for (int count : columnFamilyCounts) {
				if (count < 1) throw new IllegalArgumentException("column-family counts must be positive");
			}
			if (warmupColumnObservations < 1L || measuredColumnObservations < 1L
					|| minimumEvaluations < 1 || maximumEvaluations < minimumEvaluations
					|| latencySampleStride < 1) {
				throw new IllegalArgumentException("observation, evaluation, and sampling values must be positive");
			}
			if (buildId == null || buildId.isBlank()) {
				throw new IllegalArgumentException("buildId is required");
			}
		}

		private static Config parse(String[] args) {
			long warmup = longArgument(args, "warmup-columns", 5_000_000L);
			long measured = longArgument(args, "measured-columns", 20_000_000L);
			int minimum = integerArgument(args, "minimum-evaluations", 20_000);
			int maximum = integerArgument(args, "maximum-evaluations", 2_000_000);
			int stride = integerArgument(args, "latency-sample-stride", 1_024);
			String build = argument(args, "build-id", "development-tree");
			return new Config(new int[] {1, 64, 256, 1_024, 4_096},
					warmup,
					measured,
					minimum,
					maximum,
					stride,
					build);
		}
	}

	public record CaseResult(Scenario scenario,
			int columnFamilies,
			long evaluations,
			long columnObservations,
			long elapsedNanos,
			long cpuNanos,
			long allocatedBytes,
			long pressuredEvaluations,
			long expectedPressuredEvaluations,
			long latencyP50Nanos,
			long latencyP95Nanos,
			long latencyP99Nanos) {

		public double evaluationsPerSecond() {
			return evaluations * 1_000_000_000.0d / elapsedNanos;
		}

		public double columnObservationsPerSecond() {
			return columnObservations * 1_000_000_000.0d / elapsedNanos;
		}

		public double cpuNanosPerEvaluation() {
			return cpuNanos / (double) evaluations;
		}

		public double cpuNanosPerColumnObservation() {
			return cpuNanos / (double) columnObservations;
		}

		public double allocatedBytesPerEvaluation() {
			return allocatedBytes / (double) evaluations;
		}
	}

	public record Result(Config config, List<CaseResult> cases) {

		public void assertCorrectAndAllocationFree() {
			int expectedCases = config.columnFamilyCounts().length * Scenario.values().length;
			if (cases.size() != expectedCases) {
				throw new IllegalStateException("case matrix incomplete: " + cases.size() + " != " + expectedCases);
			}
			for (CaseResult result : cases) {
				if (result.pressuredEvaluations() != result.expectedPressuredEvaluations()) {
					throw new IllegalStateException("pressure result mismatch for " + result);
				}
				if (result.allocatedBytes() != 0L) {
					throw new IllegalStateException("warmed pressure evaluation allocated for " + result.scenario()
							+ '/' + result.columnFamilies() + ": " + result.allocatedBytes());
				}
				if (result.elapsedNanos() <= 0L || result.cpuNanos() <= 0L
						|| result.latencyP50Nanos() <= 0L || result.latencyP99Nanos() < result.latencyP50Nanos()) {
					throw new IllegalStateException("timing telemetry incomplete for " + result);
				}
			}
		}

		public String toReport() {
			var text = new StringBuilder();
			text.append("schema=rockserver-storage-pressure-signal-v1\n");
			text.append("build_id=").append(config.buildId()).append('\n');
			text.append("java_version=").append(System.getProperty("java.version")).append('\n');
			for (CaseResult result : cases) {
				String prefix = "case." + result.scenario().name().toLowerCase(Locale.ROOT)
						+ ".cf_" + result.columnFamilies() + '.';
				text.append(prefix).append("evaluations=").append(result.evaluations()).append('\n');
				text.append(prefix).append("column_observations=").append(result.columnObservations()).append('\n');
				text.append(prefix).append("evaluations_per_second=")
						.append(String.format(Locale.ROOT, "%.3f", result.evaluationsPerSecond())).append('\n');
				text.append(prefix).append("column_observations_per_second=")
						.append(String.format(Locale.ROOT, "%.3f", result.columnObservationsPerSecond())).append('\n');
				text.append(prefix).append("cpu_nanos_per_evaluation=")
						.append(String.format(Locale.ROOT, "%.3f", result.cpuNanosPerEvaluation())).append('\n');
				text.append(prefix).append("cpu_nanos_per_column_observation=")
						.append(String.format(Locale.ROOT, "%.3f", result.cpuNanosPerColumnObservation())).append('\n');
				text.append(prefix).append("allocated_bytes_per_evaluation=")
						.append(String.format(Locale.ROOT, "%.3f", result.allocatedBytesPerEvaluation())).append('\n');
				text.append(prefix).append("latency_p50_nanos=").append(result.latencyP50Nanos()).append('\n');
				text.append(prefix).append("latency_p95_nanos=").append(result.latencyP95Nanos()).append('\n');
				text.append(prefix).append("latency_p99_nanos=").append(result.latencyP99Nanos()).append('\n');
				text.append(prefix).append("pressure_count=").append(result.pressuredEvaluations()).append('\n');
			}
			return text.toString();
		}
	}

	private static final class PreparedCase {

		private final Scenario scenario;
		private final int columnFamilies;
		private final StoragePressureSignal signal;
		private final long[] pendingBytes;
		private final long[] softLimits;

		private PreparedCase(Scenario scenario,
				int columnFamilies,
				StoragePressureSignal signal,
				long[] pendingBytes,
				long[] softLimits) {
			this.scenario = scenario;
			this.columnFamilies = columnFamilies;
			this.signal = signal;
			this.pendingBytes = pendingBytes;
			this.softLimits = softLimits;
		}

		private static PreparedCase create(Scenario scenario, int columnFamilies) {
			long[] pending = new long[columnFamilies];
			long[] limits = new long[columnFamilies];
			Arrays.fill(pending, DEFAULT_SOFT_LIMIT - 1L);
			Arrays.fill(limits, DEFAULT_SOFT_LIMIT);
			StoragePressureSignal signal = scenario == Scenario.OVERRIDE_LAST_COLUMN_TRIGGER
					? new StoragePressureSignal(DEFAULT_SOFT_LIMIT)
					: new StoragePressureSignal();
			switch (scenario) {
				case FIRST_COLUMN_TRIGGER -> pending[0] = DEFAULT_SOFT_LIMIT;
				case LAST_COLUMN_TRIGGER, OVERRIDE_LAST_COLUMN_TRIGGER ->
						pending[columnFamilies - 1] = DEFAULT_SOFT_LIMIT;
				case DISABLED_LIMITS -> {
					Arrays.fill(pending, Long.MAX_VALUE);
					Arrays.fill(limits, Long.MAX_VALUE);
				}
				case UNSIGNED_LAST_COLUMN_TRIGGER -> pending[columnFamilies - 1] = -1L;
				case INVALID_LIMIT -> limits[columnFamilies - 1] = -1L;
				case NO_PRESSURE, ACTUAL_DELAYED_WRITE, WRITE_STOPPED, SIGNAL_FAILURE,
						FREQUENT_TRANSITIONS -> {
				}
			}
			if (scenario == Scenario.OVERRIDE_LAST_COLUMN_TRIGGER) {
				Arrays.fill(limits, Long.MAX_VALUE);
			}
			return new PreparedCase(scenario, columnFamilies, signal, pending, limits);
		}

		private long evaluate(long iteration) {
			signal.reset(writeStopped(iteration), actualDelayedWriteRate(iteration));
			for (int column = 0; column < columnFamilies; column++) {
				signal.observeColumn(column, pendingBytes[column], softLimits[column]);
			}
			if (scenario == Scenario.SIGNAL_FAILURE) {
				signal.markSignalFailure();
			}
			return ((long) signal.reasonMask() << 56)
					^ signal.maximumPendingCompactionBytes()
					^ signal.triggeringColumnId()
					^ signal.triggeringPendingCompactionBytes()
					^ signal.triggeringPendingCompactionLimit();
		}

		private void assertOracle(long iteration) {
			evaluate(iteration);
			int expectedReason = expectedReason(iteration);
			if (signal.reasonMask() != expectedReason || signal.pressured() != (expectedReason != NO_REASON)) {
				throw new IllegalStateException("oracle mismatch for " + scenario + '/' + columnFamilies
						+ " iteration=" + iteration + " expected=" + expectedReason
						+ " actual=" + signal.reasonMask());
			}
			long expectedMaximum = pendingBytes[0];
			for (int column = 1; column < pendingBytes.length; column++) {
				if (Long.compareUnsigned(pendingBytes[column], expectedMaximum) > 0) {
					expectedMaximum = pendingBytes[column];
				}
			}
			if (signal.maximumPendingCompactionBytes() != expectedMaximum) {
				throw new IllegalStateException("maximum pending bytes mismatch for " + scenario);
			}
			long expectedTrigger = switch (scenario) {
				case FIRST_COLUMN_TRIGGER -> 0L;
				case LAST_COLUMN_TRIGGER, OVERRIDE_LAST_COLUMN_TRIGGER,
						UNSIGNED_LAST_COLUMN_TRIGGER -> columnFamilies - 1L;
				default -> -1L;
			};
			if (signal.triggeringColumnId() != expectedTrigger) {
				throw new IllegalStateException("triggering column mismatch for " + scenario
						+ ": expected=" + expectedTrigger + " actual=" + signal.triggeringColumnId());
			}
			if (expectedTrigger >= 0L) {
				int index = Math.toIntExact(expectedTrigger);
				long expectedLimit = scenario == Scenario.OVERRIDE_LAST_COLUMN_TRIGGER
						? DEFAULT_SOFT_LIMIT : softLimits[index];
				if (signal.triggeringPendingCompactionBytes() != pendingBytes[index]
						|| signal.triggeringPendingCompactionLimit() != expectedLimit) {
					throw new IllegalStateException("trigger details mismatch for " + scenario);
				}
			}
		}

		private long expectedPressuredEvaluations(long evaluations) {
			return scenario == Scenario.NO_PRESSURE || scenario == Scenario.DISABLED_LIMITS
					? 0L
					: scenario == Scenario.FREQUENT_TRANSITIONS ? evaluations / 2L : evaluations;
		}

		private int expectedReason(long iteration) {
			return switch (scenario) {
				case NO_PRESSURE, DISABLED_LIMITS -> NO_REASON;
				case FIRST_COLUMN_TRIGGER, LAST_COLUMN_TRIGGER, OVERRIDE_LAST_COLUMN_TRIGGER,
						UNSIGNED_LAST_COLUMN_TRIGGER -> StoragePressureSignal.REASON_PENDING_COMPACTION;
				case ACTUAL_DELAYED_WRITE -> StoragePressureSignal.REASON_DELAYED_WRITE;
				case WRITE_STOPPED -> StoragePressureSignal.REASON_WRITE_STOPPED;
				case INVALID_LIMIT, SIGNAL_FAILURE -> StoragePressureSignal.REASON_SIGNAL_FAILURE;
				case FREQUENT_TRANSITIONS -> (iteration & 1L) == 0L
						? NO_REASON : StoragePressureSignal.REASON_DELAYED_WRITE;
			};
		}

		private long writeStopped(long iteration) {
			return scenario == Scenario.WRITE_STOPPED ? 1L : 0L;
		}

		private long actualDelayedWriteRate(long iteration) {
			return scenario == Scenario.ACTUAL_DELAYED_WRITE
					|| (scenario == Scenario.FREQUENT_TRANSITIONS && (iteration & 1L) != 0L) ? 1L : 0L;
		}
	}

	private static String argument(String[] args, String key, String fallback) {
		String prefix = "--" + key + '=';
		for (String argument : args) {
			if (argument.startsWith(prefix)) return argument.substring(prefix.length());
		}
		return fallback;
	}

	private static int integerArgument(String[] args, String key, int fallback) {
		return Integer.parseInt(argument(args, key, Integer.toString(fallback)));
	}

	private static long longArgument(String[] args, String key, long fallback) {
		return Long.parseLong(argument(args, key, Long.toString(fallback)));
	}
}
