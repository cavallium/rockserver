package it.cavallium.rockserver.core.impl.benchmark;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Strict paired contracts for pressure-signal and pressure-transition benchmark results. */
public final class PressurePerformanceContract {

	private PressurePerformanceContract() {
	}

	public static Map<String, Double> schedulerMetrics(SchedulerHighContentionBenchmark.Result result) {
		result.assertCorrect();
		var values = new LinkedHashMap<String, Double>();
		values.put("scheduler.attempts_per_second", result.attemptsPerSecond());
		values.put("scheduler.useful_runs_per_second", result.usefulRunsPerSecond());
		values.put("scheduler.cpu_nanos_per_attempt", result.cpuNanosPerAttempt());
		values.put("scheduler.allocated_bytes_per_attempt", result.allocatedBytesPerAttempt());
		values.put("scheduler.gc_collections", (double) result.process().gcCollections());
		values.put("scheduler.gc_millis", (double) result.process().gcMillis());
		values.put("scheduler.peak_live_heap_bytes", (double) result.peaks().liveHeapBytes());
		values.put("scheduler.peak_direct_memory_bytes", (double) result.peaks().directMemoryBytes());
		values.put("scheduler.peak_resident_set_bytes", (double) result.peaks().residentSetBytes());
		values.put("scheduler.peak_threads", (double) result.peaks().threadCount());
		values.put("scheduler.peak_native_handles", (double) result.peaks().nativeHandles());
		for (var profile : WorkloadProfile.values()) {
			var profileResult = result.profiles().get(profile);
			String prefix = "profile." + profile.name().toLowerCase(Locale.ROOT) + '.';
			values.put(prefix + "queue_p99_nanos", (double) profileResult.queueP99Nanos());
			values.put(prefix + "end_to_end_p99_nanos", (double) profileResult.endToEndP99Nanos());
			values.put(prefix + "maximum_progress_gap_nanos", (double) profileResult.maximumProgressGapNanos());
		}
		for (var family : OperationFamily.values()) {
			var familyResult = result.families().get(family);
			String prefix = "family." + family.name().toLowerCase(Locale.ROOT) + '.';
			values.put(prefix + "queue_p99_nanos", (double) familyResult.queueP99Nanos());
			values.put(prefix + "end_to_end_p99_nanos", (double) familyResult.endToEndP99Nanos());
			values.put(prefix + "maximum_progress_gap_nanos", (double) familyResult.maximumProgressGapNanos());
		}
		for (var pool : RWScheduler.Pool.values()) {
			var poolResult = result.pools().get(pool);
			String prefix = "pool." + pool.name().toLowerCase(Locale.ROOT) + '.';
			values.put(prefix + "peak_active", (double) poolResult.peakActive());
			values.put(prefix + "peak_queued", (double) poolResult.peakQueued());
			values.put(prefix + "peak_outstanding", (double) poolResult.peakOutstanding());
		}
		return Map.copyOf(values);
	}

	public static Map<String, Double> signalMetrics(StoragePressureSignalBenchmark.Result result) {
		result.assertCorrectAndAllocationFree();
		var values = new LinkedHashMap<String, Double>();
		for (var measurement : result.cases()) {
			String prefix = signalPrefix(measurement.scenario(), measurement.columnFamilies());
			values.put(prefix + "column_observations_per_second",
					measurement.columnObservationsPerSecond());
			values.put(prefix + "cpu_nanos_per_column_observation",
					measurement.cpuNanosPerColumnObservation());
			values.put(prefix + "latency_p99_nanos", (double) measurement.latencyP99Nanos());
			values.put(prefix + "allocated_bytes_per_evaluation",
					measurement.allocatedBytesPerEvaluation());
		}
		return Map.copyOf(values);
	}

	public static PairedPerformanceContract.Evaluation evaluateScheduler(
			List<Map<String, Double>> baseline,
			List<Map<String, Double>> candidate,
			List<String> structuralFailures) {
		var specifications = new ArrayList<PairedPerformanceContract.MetricSpec>();
		specifications.add(PairedPerformanceContract.MetricSpec.throughput(
				"scheduler.attempts_per_second", false));
		specifications.add(PairedPerformanceContract.MetricSpec.throughput(
				"scheduler.useful_runs_per_second", true));
		specifications.add(PairedPerformanceContract.MetricSpec.cost(
				"scheduler.cpu_nanos_per_attempt", true));
		specifications.add(PairedPerformanceContract.MetricSpec.allocation(
				"scheduler.allocated_bytes_per_attempt", true));
		specifications.add(PairedPerformanceContract.MetricSpec.noIncrease("scheduler.gc_collections"));
		specifications.add(PairedPerformanceContract.MetricSpec.noIncrease("scheduler.gc_millis"));
		specifications.add(PairedPerformanceContract.MetricSpec.cost("scheduler.peak_live_heap_bytes", false));
		specifications.add(PairedPerformanceContract.MetricSpec.noIncrease(
				"scheduler.peak_direct_memory_bytes"));
		specifications.add(PairedPerformanceContract.MetricSpec.cost(
				"scheduler.peak_resident_set_bytes", false));
		specifications.add(PairedPerformanceContract.MetricSpec.noIncrease("scheduler.peak_threads"));
		specifications.add(PairedPerformanceContract.MetricSpec.noIncrease("scheduler.peak_native_handles"));
		for (var profile : WorkloadProfile.values()) {
			String prefix = "profile." + profile.name().toLowerCase(Locale.ROOT) + '.';
			addLatencyAndStarvationMetrics(specifications, prefix);
		}
		for (var family : OperationFamily.values()) {
			String prefix = "family." + family.name().toLowerCase(Locale.ROOT) + '.';
			addLatencyAndStarvationMetrics(specifications, prefix);
		}
		for (var pool : RWScheduler.Pool.values()) {
			String prefix = "pool." + pool.name().toLowerCase(Locale.ROOT) + '.';
			specifications.add(PairedPerformanceContract.MetricSpec.cost(prefix + "peak_active", false));
			specifications.add(PairedPerformanceContract.MetricSpec.cost(prefix + "peak_queued", false));
			specifications.add(PairedPerformanceContract.MetricSpec.cost(prefix + "peak_outstanding", false));
		}
		return evaluate(specifications, baseline, candidate, structuralFailures, true);
	}

	public static PairedPerformanceContract.Evaluation evaluateSignal(
			int[] columnFamilyCounts,
			List<Map<String, Double>> baseline,
			List<Map<String, Double>> candidate,
			List<String> structuralFailures) {
		var specifications = new ArrayList<PairedPerformanceContract.MetricSpec>();
		for (int columnFamilies : columnFamilyCounts) {
			for (var scenario : StoragePressureSignalBenchmark.Scenario.values()) {
				String prefix = signalPrefix(scenario, columnFamilies);
				specifications.add(PairedPerformanceContract.MetricSpec.throughput(
						prefix + "column_observations_per_second", true));
				specifications.add(PairedPerformanceContract.MetricSpec.cost(
						prefix + "cpu_nanos_per_column_observation", true));
				specifications.add(PairedPerformanceContract.MetricSpec.cost(
						prefix + "latency_p99_nanos", true));
				specifications.add(PairedPerformanceContract.MetricSpec.noIncrease(
						prefix + "allocated_bytes_per_evaluation"));
			}
		}
		return evaluate(specifications, baseline, candidate, structuralFailures, false);
	}

	private static void addLatencyAndStarvationMetrics(
			List<PairedPerformanceContract.MetricSpec> specifications,
			String prefix) {
		specifications.add(PairedPerformanceContract.MetricSpec.cost(prefix + "queue_p99_nanos", false));
		specifications.add(PairedPerformanceContract.MetricSpec.cost(prefix + "end_to_end_p99_nanos", false));
		specifications.add(PairedPerformanceContract.MetricSpec.cost(
				prefix + "maximum_progress_gap_nanos", true));
	}

	private static PairedPerformanceContract.Evaluation evaluate(
			List<PairedPerformanceContract.MetricSpec> specifications,
			List<Map<String, Double>> baseline,
			List<Map<String, Double>> candidate,
			List<String> structuralFailures,
			boolean requireMaterialImprovement) {
		var failures = new ArrayList<>(structuralFailures);
		boolean validPairCount = baseline.size() == PairedPerformanceContract.REQUIRED_PAIRS
				&& candidate.size() == PairedPerformanceContract.REQUIRED_PAIRS;
		if (!validPairCount) {
			failures.add("exactly " + PairedPerformanceContract.REQUIRED_PAIRS
					+ " counterbalanced pairs are required");
		}
		var samples = new LinkedHashMap<String, PairedPerformanceContract.MetricSamples>();
		if (validPairCount) {
			for (var specification : specifications) {
				double[] baselineValues = values(baseline, specification.name());
				double[] candidateValues = values(candidate, specification.name());
				if (baselineValues == null) {
					failures.add("missing baseline metric " + specification.name());
				}
				if (candidateValues == null) {
					failures.add("missing candidate metric " + specification.name());
				}
				if (baselineValues != null && candidateValues != null) {
					samples.put(specification.name(),
							new PairedPerformanceContract.MetricSamples(baselineValues, candidateValues));
				}
			}
		}
		return PairedPerformanceContract.evaluate(specifications,
				samples,
				failures,
				requireMaterialImprovement);
	}

	private static double[] values(List<Map<String, Double>> runs, String metric) {
		double[] values = new double[runs.size()];
		for (int pair = 0; pair < runs.size(); pair++) {
			Double value = runs.get(pair).get(metric);
			if (value == null) return null;
			values[pair] = value;
		}
		return values;
	}

	private static String signalPrefix(StoragePressureSignalBenchmark.Scenario scenario,
			int columnFamilies) {
		return "case." + scenario.name().toLowerCase(Locale.ROOT) + ".cf_" + columnFamilies + '.';
	}
}
