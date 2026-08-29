package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PressurePerformanceContractTest {

	@Test
	void schedulerContractRequiresMaterialGainWithoutAnyLatencyCostOrStarvationRegression() {
		Map<String, Double> baseline = schedulerMetrics(100.0d);
		var candidate = new LinkedHashMap<>(baseline);
		candidate.put("scheduler.useful_runs_per_second", 103.0d);

		var result = PressurePerformanceContract.evaluateScheduler(
				repeated(baseline), repeated(candidate), List.of());

		assertTrue(result.automaticAcceptancePassed(), result.failures().toString());
		assertTrue(result.materialImprovements().contains("scheduler.useful_runs_per_second"));
	}

	@Test
	void oneFamilyStarvationGapRegressionRejectsAnOtherwiseFasterScheduler() {
		Map<String, Double> baseline = schedulerMetrics(100.0d);
		var candidate = new LinkedHashMap<>(baseline);
		candidate.put("scheduler.useful_runs_per_second", 103.0d);
		candidate.put("family.range_page.maximum_progress_gap_nanos", 101.0d);

		var result = PressurePerformanceContract.evaluateScheduler(
				repeated(baseline), repeated(candidate), List.of());

		assertFalse(result.automaticAcceptancePassed());
		assertTrue(result.failures().stream().anyMatch(failure ->
				failure.contains("family.range_page.maximum_progress_gap_nanos")));
	}

	@Test
	void signalContractCoversEveryScenarioAndRejectsCpuRegression() {
		int[] counts = {1, 1_024};
		Map<String, Double> baseline = signalMetrics(counts, 100.0d);
		var candidate = new LinkedHashMap<>(baseline);
		candidate.put("case.frequent_transitions.cf_1024.cpu_nanos_per_column_observation", 101.0d);

		var result = PressurePerformanceContract.evaluateSignal(
				counts, repeated(baseline), repeated(candidate), List.of());

		assertFalse(result.automaticAcceptancePassed());
		assertTrue(result.failures().stream().anyMatch(failure ->
				failure.contains("case.frequent_transitions.cf_1024.cpu_nanos_per_column_observation")));
	}

	@Test
	void signalContractAllowsEqualityIncludingExactZeroAllocation() {
		int[] counts = {1, 64, 256, 1_024, 4_096};
		Map<String, Double> values = signalMetrics(counts, 100.0d);

		var result = PressurePerformanceContract.evaluateSignal(
				counts, repeated(values), repeated(values), List.of());

		assertTrue(result.automaticAcceptancePassed(), result.failures().toString());
	}

	@Test
	void missingMetricsWrongPairCountAndStructuralFailuresCannotPass() {
		Map<String, Double> baseline = schedulerMetrics(100.0d);
		var missing = new LinkedHashMap<>(baseline);
		missing.remove("profile.batch.maximum_progress_gap_nanos");
		var missingResult = PressurePerformanceContract.evaluateScheduler(
				repeated(baseline), repeated(missing), List.of());
		assertFalse(missingResult.automaticAcceptancePassed());

		var shortRuns = new ArrayList<>(repeated(baseline));
		shortRuns.removeLast();
		var pairResult = PressurePerformanceContract.evaluateScheduler(shortRuns, shortRuns, List.of());
		assertFalse(pairResult.automaticAcceptancePassed());
		assertTrue(pairResult.failures().stream().anyMatch(failure -> failure.contains("10 counterbalanced pairs")));

		var structural = PressurePerformanceContract.evaluateScheduler(
				repeated(baseline), repeated(baseline), List.of("conservation failed"));
		assertFalse(structural.automaticAcceptancePassed());
	}

	private static Map<String, Double> schedulerMetrics(double value) {
		var metrics = new LinkedHashMap<String, Double>();
		metrics.put("scheduler.attempts_per_second", value);
		metrics.put("scheduler.useful_runs_per_second", value);
		metrics.put("scheduler.cpu_nanos_per_attempt", value);
		metrics.put("scheduler.allocated_bytes_per_attempt", value);
		metrics.put("scheduler.gc_collections", 0.0d);
		metrics.put("scheduler.gc_millis", 0.0d);
		metrics.put("scheduler.peak_live_heap_bytes", value);
		metrics.put("scheduler.peak_direct_memory_bytes", 0.0d);
		metrics.put("scheduler.peak_resident_set_bytes", value);
		metrics.put("scheduler.peak_threads", value);
		metrics.put("scheduler.peak_native_handles", value);
		for (var profile : WorkloadProfile.values()) {
			addLatencyMetrics(metrics, "profile." + profile.name().toLowerCase(Locale.ROOT) + '.', value);
		}
		for (var family : OperationFamily.values()) {
			addLatencyMetrics(metrics, "family." + family.name().toLowerCase(Locale.ROOT) + '.', value);
		}
		for (var pool : RWScheduler.Pool.values()) {
			String prefix = "pool." + pool.name().toLowerCase(Locale.ROOT) + '.';
			metrics.put(prefix + "peak_active", value);
			metrics.put(prefix + "peak_queued", value);
			metrics.put(prefix + "peak_outstanding", value);
		}
		return Map.copyOf(metrics);
	}

	private static Map<String, Double> signalMetrics(int[] columnFamilyCounts, double value) {
		var metrics = new LinkedHashMap<String, Double>();
		for (int count : columnFamilyCounts) {
			for (var scenario : StoragePressureSignalBenchmark.Scenario.values()) {
				String prefix = "case." + scenario.name().toLowerCase(Locale.ROOT) + ".cf_" + count + '.';
				metrics.put(prefix + "column_observations_per_second", value);
				metrics.put(prefix + "cpu_nanos_per_column_observation", value);
				metrics.put(prefix + "latency_p99_nanos", value);
				metrics.put(prefix + "allocated_bytes_per_evaluation", 0.0d);
			}
		}
		return Map.copyOf(metrics);
	}

	private static void addLatencyMetrics(Map<String, Double> metrics, String prefix, double value) {
		metrics.put(prefix + "queue_p99_nanos", value);
		metrics.put(prefix + "end_to_end_p99_nanos", value);
		metrics.put(prefix + "maximum_progress_gap_nanos", value);
	}

	private static List<Map<String, Double>> repeated(Map<String, Double> values) {
		return java.util.Collections.nCopies(PairedPerformanceContract.REQUIRED_PAIRS, values);
	}
}
