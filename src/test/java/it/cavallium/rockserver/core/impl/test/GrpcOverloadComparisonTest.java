package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.benchmark.GrpcOverloadComparison;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GrpcOverloadComparisonTest {

	private static final String PRIMARY = "mixed.useful-throughput";

	@TempDir
	Path tempDir;

	@Test
	void exactNeutralMetricsPassWhenThePredeclaredPrimaryMateriallyImproves() {
		var result = GrpcOverloadComparison.evaluateForTesting(
				samples(PRIMARY, 1.03d, null, 1.0d), Set.of(PRIMARY), List.of());

		assertTrue(result.passed(), result.failedSummary());
		assertTrue(metric(result, PRIMARY).materialImprovement());
	}

	@Test
	void anySmallThroughputRegressionFailsEvenInsideTheExceptionCeiling() {
		String regressed = "mixed.foreground-throughput";
		var result = GrpcOverloadComparison.evaluateForTesting(
				samples(PRIMARY, 1.03d, regressed, 0.999d), Set.of(PRIMARY), List.of());

		assertFalse(result.passed());
		assertFalse(metric(result, regressed).automaticPass());
		assertTrue(metric(result, regressed).withinExceptionCeiling(),
				"the ceiling is diagnostic and must not convert the failure to a pass");
	}

	@Test
	void allocationRegressionHasNoExceptionPath() {
		String regressed = "mixed.allocated-bytes-per-operation";
		var result = GrpcOverloadComparison.evaluateForTesting(
				samples(PRIMARY, 1.03d, regressed, 1.001d), Set.of(PRIMARY), List.of());

		assertFalse(result.passed());
		assertFalse(metric(result, regressed).automaticPass());
		assertFalse(metric(result, regressed).withinExceptionCeiling());
	}

	@Test
	void gcThreadsAndHandlesRequirePairwiseNoIncrease() {
		String regressed = "mixed.peak-thread-count";
		var result = GrpcOverloadComparison.evaluateForTesting(
				samples(PRIMARY, 1.03d, regressed, 1.01d), Set.of(PRIMARY), List.of());

		assertFalse(result.passed());
		assertFalse(metric(result, regressed).automaticPass());
		assertFalse(metric(result, regressed).withinExceptionCeiling());
	}

	@Test
	void statisticalNeutralityWithoutMaterialImprovementDoesNotCompleteTheContract() {
		var result = GrpcOverloadComparison.evaluateForTesting(
				samples(null, 1.0d, null, 1.0d), Set.of(PRIMARY), List.of());

		assertFalse(result.passed());
		assertTrue(result.failedSummary().contains("material 95% confidence bound"));
	}

	@Test
	void strictManifestConsumesTenCounterbalancedFreshProcessPairs() throws Exception {
		Path manifest = writeFixture(false);
		Path output = tempDir.resolve("accepted-report");

		GrpcOverloadComparison.main(new String[] {
				"--manifest=" + manifest,
				"--output=" + output,
				"--enforce=true"
		});

		assertTrue(Files.readString(output.resolve("comparison.json")).contains("\"passed\": true"));
		assertTrue(Files.readString(output.resolve("comparison.md")).contains("Overall: **PASS**"));
	}

	@Test
	void declaredPairOrderMustMatchNonOverlappingRunIntervals() throws Exception {
		Path manifest = writeFixture(true);
		Path output = tempDir.resolve("rejected-report");

		IllegalStateException failure = assertThrows(IllegalStateException.class,
				() -> GrpcOverloadComparison.main(new String[] {
						"--manifest=" + manifest,
						"--output=" + output,
						"--enforce=true"
				}));

		assertTrue(failure.getMessage().contains("declared serial order"));
	}

	private Path writeFixture(boolean lieAboutFirstPair) throws Exception {
		String candidateBuild = "c".repeat(40);
		var manifest = new StringBuilder()
				.append("schema=").append(GrpcOverloadComparison.MANIFEST_SCHEMA).append('\n')
				.append("declared-at=2026-08-03T00:00:00Z\n")
				.append("baseline-build=").append(GrpcOverloadComparison.REQUIRED_BASELINE_SHA).append('\n')
				.append("candidate-build=").append(candidateBuild).append('\n')
				.append("primary-metrics=").append(PRIMARY).append('\n')
				.append("pairs=10\n");
		Instant cursor = Instant.parse("2026-08-03T01:00:00Z");
		long processId = 1_000L;
		for (int pair = 1; pair <= 10; pair++) {
			boolean baselineFirst = (pair & 1) == 1;
			Instant firstStarted = cursor;
			Instant firstFinished = firstStarted.plusSeconds(10L);
			Instant secondStarted = firstFinished.plusSeconds(1L);
			Instant secondFinished = secondStarted.plusSeconds(10L);
			Instant baselineStarted = baselineFirst ? firstStarted : secondStarted;
			Instant baselineFinished = baselineFirst ? firstFinished : secondFinished;
			Instant candidateStarted = baselineFirst ? secondStarted : firstStarted;
			Instant candidateFinished = baselineFirst ? secondFinished : firstFinished;
			Path baseline = tempDir.resolve("pair-" + pair + "-baseline.properties");
			Path candidate = tempDir.resolve("pair-" + pair + "-candidate.properties");
			GrpcOverloadComparison.writeRunInput(baseline,
					runInput(GrpcOverloadComparison.REQUIRED_BASELINE_SHA,
							processId++, baselineStarted, baselineFinished, false));
			GrpcOverloadComparison.writeRunInput(candidate,
					runInput(candidateBuild, processId++, candidateStarted, candidateFinished, true));
			String order = baselineFirst ? "baseline-first" : "candidate-first";
			if (lieAboutFirstPair && pair == 1) order = "candidate-first";
			manifest.append("pair.").append(pair).append(".order=").append(order).append('\n')
					.append("pair.").append(pair).append(".baseline=").append(baseline).append('\n')
					.append("pair.").append(pair).append(".candidate=").append(candidate).append('\n');
			cursor = secondFinished.plusSeconds(1L);
		}
		Path output = tempDir.resolve(lieAboutFirstPair ? "lying-manifest.properties" : "manifest.properties");
		Files.writeString(output, manifest);
		return output;
	}

	private static GrpcOverloadComparison.RunInput runInput(String build,
			long processId,
			Instant started,
			Instant finished,
			boolean candidate) {
		var metrics = new LinkedHashMap<String, Double>();
		for (String metric : GrpcOverloadComparison.metricNamesForTesting()) {
			metrics.put(metric, candidate && metric.equals(PRIMARY) ? 103.0d : 100.0d);
		}
		return new GrpcOverloadComparison.RunInput(
				build,
				"clean",
				"nvme",
				"cold",
				"dedicated",
				"1".repeat(64),
				"2".repeat(64),
				"3".repeat(64),
				"synthetic identical hardware",
				processId,
				started.minusSeconds(1L).toString(),
				started.toString(),
				finished.toString(),
				5,
				true,
				true,
				true,
				true,
				true,
				true,
				true,
				0L,
				1L,
				metrics);
	}

	private static Map<String, GrpcOverloadComparison.MetricSamples> samples(
			String improved,
			double improvementRatio,
			String regressed,
			double regressionRatio) {
		var result = new LinkedHashMap<String, GrpcOverloadComparison.MetricSamples>();
		for (String metric : GrpcOverloadComparison.metricNamesForTesting()) {
			double[] baseline = repeated(100.0d);
			double ratio = metric.equals(improved) ? improvementRatio : 1.0d;
			if (metric.equals(regressed)) {
				ratio = regressionRatio;
			}
			double[] candidate = repeated(100.0d * ratio);
			result.put(metric, new GrpcOverloadComparison.MetricSamples(baseline, candidate));
		}
		return Map.copyOf(result);
	}

	private static double[] repeated(double value) {
		double[] values = new double[10];
		Arrays.fill(values, value);
		return values;
	}

	private static GrpcOverloadComparison.MetricComparison metric(
			GrpcOverloadComparison.Comparison comparison,
			String name) {
		return comparison.metrics().stream()
				.filter(metric -> metric.name().equals(name))
				.findFirst()
				.orElseThrow();
	}
}
