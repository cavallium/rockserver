package it.cavallium.rockserver.core.impl.benchmark;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Versioned pressure-performance metric set evaluated by {@link PairedPerformanceContractV2}. */
public final class PressurePerformanceContractV2 {

	private PressurePerformanceContractV2() {
	}

	public static PairedPerformanceContractV2.Evaluation evaluateScheduler(
			List<Map<String, Double>> baseline,
			List<Map<String, Double>> candidate,
			List<String> structuralFailures) {
		return evaluate(schedulerSpecifications(), baseline, candidate, structuralFailures);
	}

	static List<PairedPerformanceContractV2.MetricSpec> schedulerSpecifications() {
		var specifications = new ArrayList<PairedPerformanceContractV2.MetricSpec>();
		specifications.add(PairedPerformanceContractV2.MetricSpec.throughput(
				"scheduler.attempts_per_second", false));
		specifications.add(PairedPerformanceContractV2.MetricSpec.throughput(
				"scheduler.useful_runs_per_second", true));
		specifications.add(PairedPerformanceContractV2.MetricSpec.cost(
				"scheduler.cpu_nanos_per_attempt", true));
		specifications.add(PairedPerformanceContractV2.MetricSpec.allocation(
				"scheduler.allocated_bytes_per_attempt", true));
		specifications.add(PairedPerformanceContractV2.MetricSpec.noIncrease("scheduler.gc_collections"));
		specifications.add(PairedPerformanceContractV2.MetricSpec.noIncrease("scheduler.gc_millis"));
		specifications.add(PairedPerformanceContractV2.MetricSpec.cost("scheduler.peak_live_heap_bytes", false));
		specifications.add(PairedPerformanceContractV2.MetricSpec.noIncrease(
				"scheduler.peak_direct_memory_bytes"));
		specifications.add(PairedPerformanceContractV2.MetricSpec.cost(
				"scheduler.peak_resident_set_bytes", false));
		specifications.add(PairedPerformanceContractV2.MetricSpec.noIncrease("scheduler.peak_threads"));
		specifications.add(PairedPerformanceContractV2.MetricSpec.noIncrease("scheduler.peak_native_handles"));
		for (var profile : WorkloadProfile.values()) {
			addLatencyAndStarvationMetrics(specifications,
					"profile." + profile.name().toLowerCase(Locale.ROOT) + '.');
		}
		for (var family : OperationFamily.values()) {
			addLatencyAndStarvationMetrics(specifications,
					"family." + family.name().toLowerCase(Locale.ROOT) + '.');
		}
		for (var pool : RWScheduler.Pool.values()) {
			String prefix = "pool." + pool.name().toLowerCase(Locale.ROOT) + '.';
			specifications.add(PairedPerformanceContractV2.MetricSpec.cost(prefix + "peak_active", false));
			specifications.add(PairedPerformanceContractV2.MetricSpec.cost(prefix + "peak_queued", false));
			specifications.add(PairedPerformanceContractV2.MetricSpec.cost(prefix + "peak_outstanding", false));
		}
		return List.copyOf(specifications);
	}

	public static PairedPerformanceContractV2.Evaluation evaluateSignal(
			int[] columnFamilyCounts,
			List<Map<String, Double>> baseline,
			List<Map<String, Double>> candidate,
			List<String> structuralFailures) {
		return evaluate(signalSpecifications(columnFamilyCounts), baseline, candidate, structuralFailures);
	}

	static List<PairedPerformanceContractV2.MetricSpec> signalSpecifications(int[] columnFamilyCounts) {
		var specifications = new ArrayList<PairedPerformanceContractV2.MetricSpec>();
		for (int columnFamilies : columnFamilyCounts) {
			for (var scenario : StoragePressureSignalBenchmark.Scenario.values()) {
				String prefix = signalPrefix(scenario, columnFamilies);
				specifications.add(PairedPerformanceContractV2.MetricSpec.throughput(
						prefix + "column_observations_per_second", true));
				specifications.add(PairedPerformanceContractV2.MetricSpec.cost(
						prefix + "cpu_nanos_per_column_observation", true));
				specifications.add(PairedPerformanceContractV2.MetricSpec.cost(
						prefix + "latency_p99_nanos", true));
				specifications.add(PairedPerformanceContractV2.MetricSpec.noIncrease(
						prefix + "allocated_bytes_per_evaluation"));
			}
		}
		return List.copyOf(specifications);
	}

	private static void addLatencyAndStarvationMetrics(
			List<PairedPerformanceContractV2.MetricSpec> specifications,
			String prefix) {
		specifications.add(PairedPerformanceContractV2.MetricSpec.cost(prefix + "queue_p99_nanos", false));
		specifications.add(PairedPerformanceContractV2.MetricSpec.cost(prefix + "end_to_end_p99_nanos", false));
		specifications.add(PairedPerformanceContractV2.MetricSpec.cost(
				prefix + "maximum_progress_gap_nanos", true));
	}

	private static PairedPerformanceContractV2.Evaluation evaluate(
			List<PairedPerformanceContractV2.MetricSpec> specifications,
			List<Map<String, Double>> baseline,
			List<Map<String, Double>> candidate,
			List<String> structuralFailures) {
		var failures = new ArrayList<>(structuralFailures);
		boolean validPairCount = baseline.size() == PairedPerformanceContractV2.REQUIRED_PAIRS
				&& candidate.size() == PairedPerformanceContractV2.REQUIRED_PAIRS;
		if (!validPairCount) {
			failures.add("exactly " + PairedPerformanceContractV2.REQUIRED_PAIRS
					+ " fresh counterbalanced v2 pairs are required");
		}
		var samples = new LinkedHashMap<String, PairedPerformanceContractV2.MetricSamples>();
		if (validPairCount) {
			for (var specification : specifications) {
				double[] baselineValues = values(baseline, specification.name());
				double[] candidateValues = values(candidate, specification.name());
				if (baselineValues == null) failures.add("missing baseline metric " + specification.name());
				if (candidateValues == null) failures.add("missing candidate metric " + specification.name());
				if (baselineValues != null && candidateValues != null) {
					samples.put(specification.name(),
							new PairedPerformanceContractV2.MetricSamples(baselineValues, candidateValues));
				}
			}
		}
		return PairedPerformanceContractV2.evaluate(specifications, samples, failures);
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
