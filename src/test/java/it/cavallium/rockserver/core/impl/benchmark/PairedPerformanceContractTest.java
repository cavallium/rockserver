package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PairedPerformanceContractTest {

	@Test
	void automaticAcceptanceRequiresNoWorsePointEstimatesAndAMaterialPrimaryGain() {
		var result = evaluate(Map.of(
				"throughput", samples(100.0d, 103.0d),
				"p99", samples(100.0d, 100.0d),
				"allocation", samples(100.0d, 100.0d),
				"gc", samples(2.0d, 2.0d)), true);

		assertTrue(result.automaticAcceptancePassed());
		assertTrue(result.materialImprovements().contains("throughput"));
	}

	@Test
	void anySmallPointEstimateRegressionFailsEvenWhenInsideTheExceptionCeiling() {
		var result = evaluate(Map.of(
				"throughput", samples(100.0d, 99.5d),
				"p99", samples(100.0d, 97.0d),
				"allocation", samples(100.0d, 100.0d),
				"gc", samples(2.0d, 2.0d)), true);

		assertFalse(result.automaticAcceptancePassed());
		assertTrue(result.exceptionCandidates().contains("throughput"));
		assertTrue(result.materialImprovements().contains("p99"));
	}

	@Test
	void allocationHasNoExceptionPath() {
		var result = evaluate(Map.of(
				"throughput", samples(100.0d, 103.0d),
				"p99", samples(100.0d, 100.0d),
				"allocation", samples(100.0d, 100.1d),
				"gc", samples(2.0d, 2.0d)), true);

		assertFalse(result.automaticAcceptancePassed());
		assertFalse(result.exceptionCandidates().contains("allocation"));
	}

	@Test
	void exactNoIncreaseMetricsRejectOneWorsePair() {
		double[] baseline = constant(2.0d);
		double[] candidate = constant(2.0d);
		candidate[7] = 3.0d;
		var samples = new LinkedHashMap<String, PairedPerformanceContract.MetricSamples>();
		samples.put("throughput", samples(100.0d, 103.0d));
		samples.put("p99", samples(100.0d, 100.0d));
		samples.put("allocation", samples(100.0d, 100.0d));
		samples.put("gc", new PairedPerformanceContract.MetricSamples(baseline, candidate));

		var result = evaluate(samples, true);

		assertFalse(result.automaticAcceptancePassed());
		assertFalse(result.exceptionCandidates().contains("gc"));
	}

	@Test
	void materialGainCanBeDeferredToTheFinalCrossSuiteGate() {
		var result = evaluate(Map.of(
				"throughput", samples(100.0d, 100.0d),
				"p99", samples(100.0d, 100.0d),
				"allocation", samples(100.0d, 100.0d),
				"gc", samples(2.0d, 2.0d)), false);

		assertTrue(result.automaticAcceptancePassed());
		assertFalse(result.materialImprovementProven());
	}

	private static PairedPerformanceContract.Evaluation evaluate(
			Map<String, PairedPerformanceContract.MetricSamples> samples,
			boolean requireMaterial) {
		return PairedPerformanceContract.evaluate(List.of(
				PairedPerformanceContract.MetricSpec.throughput("throughput", true),
				PairedPerformanceContract.MetricSpec.cost("p99", true),
				PairedPerformanceContract.MetricSpec.allocation("allocation", true),
				PairedPerformanceContract.MetricSpec.noIncrease("gc")),
				samples, List.of(), requireMaterial);
	}

	private static PairedPerformanceContract.MetricSamples samples(double baseline, double candidate) {
		return new PairedPerformanceContract.MetricSamples(constant(baseline), constant(candidate));
	}

	private static double[] constant(double value) {
		double[] result = new double[PairedPerformanceContract.REQUIRED_PAIRS];
		java.util.Arrays.fill(result, value);
		return result;
	}
}
