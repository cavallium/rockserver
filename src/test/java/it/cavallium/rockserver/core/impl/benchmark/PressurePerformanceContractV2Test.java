package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

class PressurePerformanceContractV2Test {

	private static final int[] DEFAULT_SIGNAL_COUNTS = {1, 64, 256, 1_024, 4_096};

	@Test
	void defaultPressureMetricSetHasTwoHundredThirtySevenMultiplicityControlledRatios() {
		var scheduler = PressurePerformanceContractV2.schedulerSpecifications();
		var signal = PressurePerformanceContractV2.signalSpecifications(DEFAULT_SIGNAL_COUNTS);
		long stochastic = java.util.stream.Stream.concat(scheduler.stream(), signal.stream())
				.filter(specification -> specification.direction()
						!= PairedPerformanceContractV2.Direction.DETERMINISTIC_NO_INCREASE)
				.count();
		long deterministic = scheduler.size() + signal.size() - stochastic;

		assertEquals(237L, stochastic);
		assertEquals(60L, deterministic);
		assertEquals(297, scheduler.size() + signal.size());
	}

	@Test
	void equalityAcrossEveryDefaultMetricPassesWithoutMaterialGain() {
		Map<String, Double> scheduler = schedulerMetrics(100.0d);
		Map<String, Double> signal = signalMetrics(DEFAULT_SIGNAL_COUNTS, 100.0d);

		var schedulerResult = PressurePerformanceContractV2.evaluateScheduler(
				repeated(scheduler), repeated(scheduler), List.of());
		var signalResult = PressurePerformanceContractV2.evaluateSignal(
				DEFAULT_SIGNAL_COUNTS, repeated(signal), repeated(signal), List.of());

		assertEquals(PairedPerformanceContractV2.Decision.PASS, schedulerResult.decision());
		assertEquals(PairedPerformanceContractV2.Decision.PASS, signalResult.decision());
		assertTrue(schedulerResult.materialImprovements().isEmpty());
		assertTrue(signalResult.materialImprovements().isEmpty());
	}

	@Test
	void v2ToleranceDoesNotReinterpretTheStricterV1Rule() {
		Map<String, Double> baseline = schedulerMetrics(100.0d);
		var candidate = new LinkedHashMap<>(baseline);
		candidate.put("scheduler.attempts_per_second", 99.5d);

		var v1 = PressurePerformanceContract.evaluateScheduler(
				repeated(baseline), repeated(candidate), List.of());
		var v2 = PressurePerformanceContractV2.evaluateScheduler(
				repeated(baseline), repeated(candidate), List.of());

		assertFalse(v1.automaticAcceptancePassed(), "v1 remains strict and unchanged");
		assertEquals(PairedPerformanceContractV2.Decision.PASS, v2.decision());
	}

	@Test
	void clearSingleStarvationRegressionFailsAfterFullMetricHolmCorrection() {
		Map<String, Double> baseline = schedulerMetrics(100.0d);
		var candidate = new LinkedHashMap<>(baseline);
		candidate.put("family.range_page.maximum_progress_gap_nanos", 103.0d);

		var result = PressurePerformanceContractV2.evaluateScheduler(
				repeated(baseline), repeated(candidate), List.of());

		assertEquals(PairedPerformanceContractV2.Decision.FAIL, result.decision());
		var metric = result.metrics().get("family.range_page.maximum_progress_gap_nanos");
		assertTrue(metric.regressionDemonstrated());
		assertEquals(0.0d, metric.regressionHolmAdjustedPValue());
	}

	@Test
	void oneImpreciseMetricMakesTheSuiteInconclusiveNotFailed() {
		Map<String, Double> baseline = schedulerMetrics(100.0d);
		var candidateRuns = new ArrayList<Map<String, Double>>();
		for (int pair = 0; pair < PairedPerformanceContractV2.REQUIRED_PAIRS; pair++) {
			var candidate = new LinkedHashMap<>(baseline);
			candidate.put("scheduler.cpu_nanos_per_attempt",
					(pair & 1) == 0 ? 95.0d : 100.0d / 0.95d);
			candidateRuns.add(Map.copyOf(candidate));
		}

		var result = PressurePerformanceContractV2.evaluateScheduler(
				repeated(baseline), candidateRuns, List.of());

		assertEquals(PairedPerformanceContractV2.Decision.INCONCLUSIVE, result.decision());
		assertTrue(result.failures().isEmpty());
		assertTrue(result.inconclusiveMetrics().contains("scheduler.cpu_nanos_per_attempt"));
	}

	@Test
	void structuralAndDeterministicResourceFailuresCannotBecomeStatisticalInconclusive() {
		Map<String, Double> baseline = schedulerMetrics(100.0d);
		var handles = new LinkedHashMap<>(baseline);
		handles.put("scheduler.peak_native_handles", 1.0d);

		var resource = PressurePerformanceContractV2.evaluateScheduler(
				repeated(baseline), repeated(handles), List.of());
		var structural = PressurePerformanceContractV2.evaluateScheduler(
				repeated(baseline), repeated(baseline), List.of("terminal conservation failed"));

		assertEquals(PairedPerformanceContractV2.Decision.FAIL, resource.decision());
		assertEquals(PairedPerformanceContractV2.Decision.FAIL, structural.decision());
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
		metrics.put("scheduler.peak_native_handles", 0.0d);
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
		return java.util.Collections.nCopies(PairedPerformanceContractV2.REQUIRED_PAIRS, values);
	}
}
