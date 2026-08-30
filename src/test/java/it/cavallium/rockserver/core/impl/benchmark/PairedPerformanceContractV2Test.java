package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PairedPerformanceContractV2Test {

	@Test
	void studentTCdfMatchesPredeclaredReferenceCriticalValues() {
		assertEquals(0.5d, PairedPerformanceContractV2.studentTCdf(0.0d, 9), 1.0e-15d);
		assertEquals(0.975d, PairedPerformanceContractV2.studentTCdf(2.262d, 9), 2.0e-5d);
		assertEquals(0.025d, PairedPerformanceContractV2.studentTCdf(-2.262d, 9), 2.0e-5d);
		assertEquals(0.95d, PairedPerformanceContractV2.studentTCdf(1.833d, 9), 5.0e-5d);
	}

	@Test
	void exactEqualityPassesNonInferiorityAndEquivalenceWithoutInventingImprovement() {
		var result = evaluate(List.of(
				PairedPerformanceContractV2.MetricSpec.throughput("throughput", true),
				PairedPerformanceContractV2.MetricSpec.cost("latency", true),
				PairedPerformanceContractV2.MetricSpec.allocation("allocation", true),
				PairedPerformanceContractV2.MetricSpec.noIncrease("handles")),
				Map.of(
						"throughput", samples(100.0d, 100.0d),
						"latency", samples(100.0d, 100.0d),
						"allocation", samples(100.0d, 100.0d),
						"handles", samples(3.0d, 3.0d)));

		assertEquals(PairedPerformanceContractV2.Decision.PASS, result.decision());
		assertTrue(result.metrics().get("throughput").nonInferiorityProven());
		assertTrue(result.metrics().get("throughput").equivalenceProven());
		assertTrue(result.materialImprovements().isEmpty());
	}

	@Test
	void smallSymmetricNoiseAroundEqualityPassesInsidePredeclaredMargins() {
		double[] baseline = constant(100.0d);
		double[] throughput = alternating(99.8d, 100.2d);
		double[] cost = alternating(99.7d, 100.3d);
		var result = evaluate(List.of(
				PairedPerformanceContractV2.MetricSpec.throughput("throughput", false),
				PairedPerformanceContractV2.MetricSpec.cost("cost", false)), Map.of(
						"throughput", new PairedPerformanceContractV2.MetricSamples(baseline, throughput),
						"cost", new PairedPerformanceContractV2.MetricSamples(baseline, cost)));

		assertEquals(PairedPerformanceContractV2.Decision.PASS, result.decision());
		assertTrue(result.failures().isEmpty());
	}

	@Test
	void exactOperationalBoundaryIsInconclusiveRatherThanPassOrFalseFail() {
		var result = evaluate(List.of(
				PairedPerformanceContractV2.MetricSpec.cost("cost", false)),
				Map.of("cost", samples(100.0d, 102.0d)));

		assertEquals(PairedPerformanceContractV2.Decision.INCONCLUSIVE, result.decision(), result.failures().toString());
		assertTrue(result.failures().isEmpty());
		assertEquals(List.of("cost"), result.inconclusiveMetrics());
	}

	@Test
	void zeroVarianceRegressionBeyondMarginFailsAfterHolmCorrection() {
		var result = evaluate(List.of(
				PairedPerformanceContractV2.MetricSpec.throughput("throughput", false),
				PairedPerformanceContractV2.MetricSpec.cost("cost", false)), Map.of(
						"throughput", samples(100.0d, 98.0d),
						"cost", samples(100.0d, 103.0d)));

		assertEquals(PairedPerformanceContractV2.Decision.FAIL, result.decision());
		assertTrue(result.metrics().get("throughput").regressionDemonstrated());
		assertTrue(result.metrics().get("cost").regressionDemonstrated());
		assertEquals(0.0d, result.metrics().get("throughput").regressionHolmAdjustedPValue());
	}

	@Test
	void holmControlsOneRegressionAcrossOneHundredCorrelatedMetrics() {
		var specifications = new ArrayList<PairedPerformanceContractV2.MetricSpec>();
		var samples = new LinkedHashMap<String, PairedPerformanceContractV2.MetricSamples>();
		for (int metric = 0; metric < 100; metric++) {
			String name = "metric-" + metric;
			specifications.add(PairedPerformanceContractV2.MetricSpec.cost(name, false));
			samples.put(name, samples(100.0d, metric == 73 ? 103.0d : 100.0d));
		}

		var result = evaluate(specifications, samples);

		assertEquals(100, result.stochasticHypotheses());
		assertEquals(PairedPerformanceContractV2.Decision.FAIL, result.decision());
		assertTrue(result.metrics().get("metric-73").regressionDemonstrated());
		assertTrue(result.metrics().entrySet().stream()
				.filter(entry -> !entry.getKey().equals("metric-73"))
				.noneMatch(entry -> entry.getValue().regressionDemonstrated()));
	}

	@Test
	void insufficientPrecisionIsReportedWithoutInflatingFalseFailure() {
		double[] noisyEquality = alternating(95.0d, 100.0d / 0.95d);
		var result = evaluate(List.of(
				PairedPerformanceContractV2.MetricSpec.cost("noisy", false)),
				Map.of("noisy", new PairedPerformanceContractV2.MetricSamples(
						constant(100.0d), noisyEquality)));

		assertEquals(PairedPerformanceContractV2.Decision.INCONCLUSIVE, result.decision());
		assertFalse(result.metrics().get("noisy").regressionDemonstrated());
		assertFalse(result.metrics().get("noisy").nonInferiorityProven());
	}

	@Test
	void deterministicResourceCeilingRemainsPairwiseAndNonStatistical() {
		double[] candidate = constant(3.0d);
		candidate[7] = 4.0d;
		var result = evaluate(List.of(PairedPerformanceContractV2.MetricSpec.noIncrease("handles")),
				Map.of("handles", new PairedPerformanceContractV2.MetricSamples(
						constant(3.0d), candidate)));

		assertEquals(PairedPerformanceContractV2.Decision.FAIL, result.decision());
		assertFalse(result.metrics().get("handles").deterministicCeilingPassed());
	}

	@Test
	void materialImprovementEvidenceIsHolmAdjustedButNotRequiredForPass() {
		var specifications = new ArrayList<PairedPerformanceContractV2.MetricSpec>();
		var samples = new LinkedHashMap<String, PairedPerformanceContractV2.MetricSamples>();
		for (int metric = 0; metric < 32; metric++) {
			String name = "primary-" + metric;
			specifications.add(PairedPerformanceContractV2.MetricSpec.throughput(name, true));
			samples.put(name, samples(100.0d, metric == 9 ? 103.0d : 100.0d));
		}

		var result = evaluate(specifications, samples);

		assertEquals(PairedPerformanceContractV2.Decision.PASS, result.decision());
		assertEquals(List.of("primary-9"), result.materialImprovements());
		assertEquals(0.0d, result.metrics().get("primary-9").materialHolmAdjustedPValue());
	}

	@Test
	void malformedMissingShortAndStructuralInputsFailClosed() {
		var specification = PairedPerformanceContractV2.MetricSpec.cost("cost", false);
		assertEquals(PairedPerformanceContractV2.Decision.FAIL,
				PairedPerformanceContractV2.evaluate(List.of(specification), Map.of(), List.of()).decision());
		assertEquals(PairedPerformanceContractV2.Decision.FAIL,
				PairedPerformanceContractV2.evaluate(List.of(specification), Map.of(
						"cost", new PairedPerformanceContractV2.MetricSamples(
								new double[9], new double[9])), List.of()).decision());
		assertEquals(PairedPerformanceContractV2.Decision.FAIL,
				PairedPerformanceContractV2.evaluate(List.of(specification), Map.of(
						"cost", samples(100.0d, 100.0d)), List.of("conservation failed")).decision());
	}

	private static PairedPerformanceContractV2.Evaluation evaluate(
			List<PairedPerformanceContractV2.MetricSpec> specifications,
			Map<String, PairedPerformanceContractV2.MetricSamples> samples) {
		return PairedPerformanceContractV2.evaluate(specifications, samples, List.of());
	}

	private static PairedPerformanceContractV2.MetricSamples samples(double baseline, double candidate) {
		return new PairedPerformanceContractV2.MetricSamples(constant(baseline), constant(candidate));
	}

	private static double[] constant(double value) {
		double[] result = new double[PairedPerformanceContractV2.REQUIRED_PAIRS];
		java.util.Arrays.fill(result, value);
		return result;
	}

	private static double[] alternating(double first, double second) {
		double[] result = new double[PairedPerformanceContractV2.REQUIRED_PAIRS];
		for (int index = 0; index < result.length; index++) result[index] = (index & 1) == 0 ? first : second;
		return result;
	}
}
