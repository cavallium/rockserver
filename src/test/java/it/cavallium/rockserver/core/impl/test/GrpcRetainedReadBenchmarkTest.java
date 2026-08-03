package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.benchmark.GrpcRetainedReadBenchmark;
import it.cavallium.rockserver.core.impl.benchmark.GrpcRetainedReadBenchmark.MetricSamples;
import it.cavallium.rockserver.core.impl.benchmark.PairedBenchmarkStatistics;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GrpcRetainedReadBenchmarkTest {

	@TempDir
	java.nio.file.Path tempDir;

	private static final String SCENARIO = "stream-range-with-latency-gets";
	private static final String BUILD_SHA = "0123456789abcdef0123456789abcdef01234567";
	private static final String PERFORMANCE_BASELINE_SHA = "bb4f1a7e90db1fdfd785936594d080e8c4a0ba4e";
	private static final String DIGEST =
			"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

	@Test
	void completeStrictFixturePasses() {
		var result = GrpcRetainedReadBenchmark.evaluateForTesting(
				SCENARIO,
				GrpcRetainedReadBenchmark.passingMetricSamplesForTesting(SCENARIO),
				List.of());

		assertTrue(result.passed());
		assertTrue(result.intervals().values().stream().allMatch(interval -> interval.samples() == 10));
	}

	@Test
	void throughputLowerBoundRegressionIsRejected() {
		var samples = passingSamples();
		replaceCandidate(samples, "entries-per-second", 98.0d);

		var result = GrpcRetainedReadBenchmark.evaluateForTesting(SCENARIO, samples, List.of());

		assertFalse(result.passed());
		assertTrue(hasFailure(result.failures(), "entries-per-second geometric-mean ratio"));
	}

	@Test
	void completionFirstItemForegroundAndColdLatencyRegressionsAreRejected() {
		for (String metric : List.of("completion-p99", "first-item-p99", "foreground-p99",
				"cold-completion", "cold-first-item")) {
			var samples = passingSamples();
			replaceCandidate(samples, metric, 103.0d);

			var result = GrpcRetainedReadBenchmark.evaluateForTesting(SCENARIO, samples, List.of());

			assertFalse(result.passed(), metric);
			assertTrue(hasFailure(result.failures(), metric + " geometric-mean ratio"), metric);
		}
	}

	@Test
	void cpuAllocationAndMemoryRegressionsAreRejected() {
		Map<String, Double> regressions = Map.of(
				"cpu-nanos-per-item", 103.0d,
				"allocated-bytes-per-item", 101.0d,
				"peak-live-heap", 103.0d,
				"peak-direct-memory", 103.0d,
				"peak-resident-set", 103.0d);
		for (Map.Entry<String, Double> regression : regressions.entrySet()) {
			var samples = passingSamples();
			replaceCandidate(samples, regression.getKey(), regression.getValue());

			var result = GrpcRetainedReadBenchmark.evaluateForTesting(SCENARIO, samples, List.of());

			assertFalse(result.passed(), regression.getKey());
			assertTrue(hasFailure(result.failures(), regression.getKey() + " geometric-mean ratio"),
					regression.getKey());
		}
	}

	@Test
	void gcThreadsAndNativeHandlesAllowNoIncreaseInAnyPair() {
		for (String metric : List.of("gc-collections", "gc-millis", "peak-thread-count",
				"peak-native-handles", "peak-parked", "peak-outstanding",
				"peak-retained-snapshots", "peak-exists-multi-arenas")) {
			var samples = passingSamples();
			replaceCandidate(samples, metric, 101.0d);

			var result = GrpcRetainedReadBenchmark.evaluateForTesting(SCENARIO, samples, List.of());

			assertFalse(result.passed(), metric);
			assertTrue(hasFailure(result.failures(), metric + " geometric-mean ratio"), metric);
			assertFalse(result.exceptionCandidates().contains(metric), metric);
		}
	}

	@Test
	void exceptionCeilingsAreReportedButNeverAutomaticallyAccepted() {
		var samples = passingSamples();
		replaceCandidate(samples, "completion-p99", 101.0d);

		var result = GrpcRetainedReadBenchmark.evaluateForTesting(SCENARIO, samples, List.of());

		assertFalse(result.passed());
		assertTrue(result.exceptionCandidates().contains("completion-p99"));
	}

	@Test
	void confidenceBoundCrossingThresholdIsInconclusiveAndRejected() {
		var samples = passingSamples();
		double[] candidate = {96, 104, 97, 103, 98, 102, 99, 101, 96, 104};
		samples.put("entries-per-second", new MetricSamples(constant(100.0d), candidate));

		var result = GrpcRetainedReadBenchmark.evaluateForTesting(SCENARIO, samples, List.of());
		var interval = result.intervals().get("entries-per-second");

		assertFalse(result.passed());
		assertTrue(interval.lower95() < 1.0d && interval.upper95() > 1.0d);
	}

	@Test
	void missingAndIncompleteMetricsAreRejected() {
		var missing = passingSamples();
		missing.remove("mib-per-second");
		assertFalse(GrpcRetainedReadBenchmark.evaluateForTesting(SCENARIO, missing, List.of()).passed());

		var incomplete = passingSamples();
		incomplete.put("completion-p99", new MetricSamples(new double[]{100, 100},
				new double[]{100, 100}));
		assertFalse(GrpcRetainedReadBenchmark.evaluateForTesting(SCENARIO, incomplete, List.of()).passed());
	}

	@Test
	void correctnessAccountingAndResourceFailuresAreTerminal() {
		var result = GrpcRetainedReadBenchmark.evaluateForTesting(SCENARIO, passingSamples(),
				List.of("correctness validation failed", "logical scheduler accounting is invalid",
						"resources did not drain", "native leak count=1"));

		assertFalse(result.passed());
		assertTrue(hasFailure(result.failures(), "correctness"));
		assertTrue(hasFailure(result.failures(), "scheduler accounting"));
		assertTrue(hasFailure(result.failures(), "resources did not drain"));
		assertTrue(hasFailure(result.failures(), "native leak"));
	}

	@Test
	void workerArtifactRejectsCorrectnessResourceAndLeakFailures() {
		String valid = GrpcRetainedReadBenchmark.validWorkerArtifactForTesting();

		var incorrect = parse(valid.replace("correctness=true", "correctness=false"));
		var undrained = parse(valid.replace("resources-drained=true", "resources-drained=false")
				.replace("final-pending=0", "final-pending=1"));
		var existsMultiUndrained = parse(valid.replace(
				"final-exists-multi-requests=0", "final-exists-multi-requests=1"));
		var existsMultiArenaUndrained = parse(valid.replace(
				"final-exists-multi-arenas=0", "final-exists-multi-arenas=1"));
		var impossibleExistsMultiPeak = parse(valid.replace(
				"peak-exists-multi-snapshots=0", "peak-exists-multi-snapshots=1"));
		var leaked = parse(valid.replace("native-leaks=0", "native-leaks=1"));
		var badAccounting = parse(valid.replace("accounting-valid=true", "accounting-valid=false"));
		var parked = parse(valid.replace("final-parked=0", "final-parked=1"));
		var unconserved = parse(valid.replace("terminal-outcomes=2", "terminal-outcomes=1"));
		var inexactCandidate = parse(valid.replace("scheduler-accounting-exact=true",
				"scheduler-accounting-exact=false"));

		assertFalse(incorrect.passed());
		assertFalse(undrained.passed());
		assertFalse(existsMultiUndrained.passed());
		assertFalse(existsMultiArenaUndrained.passed());
		assertFalse(impossibleExistsMultiPeak.passed());
		assertFalse(leaked.passed());
		assertFalse(badAccounting.passed());
		assertFalse(parked.passed());
		assertFalse(unconserved.passed());
		assertFalse(inexactCandidate.passed());
	}

	@Test
	void malformedWorkerArtifactsAreRejected() {
		String valid = GrpcRetainedReadBenchmark.validWorkerArtifactForTesting();

		assertThrows(IllegalArgumentException.class,
				() -> parse(valid.replace("entries-per-second=2.0E8", "entries-per-second=NaN")));
		assertThrows(IllegalArgumentException.class, () -> parse(valid + "unknown=value\n"));
		assertThrows(IllegalArgumentException.class,
				() -> parse(valid.replaceFirst("(?m)^mib-per-second=.*\\R", "")));
		assertThrows(IllegalArgumentException.class,
				() -> parse(valid.replace("arena-instrumentation-sha256=" + DIGEST,
						"arena-instrumentation-sha256=unverified")));
		assertThrows(IllegalArgumentException.class,
				() -> parse(valid + "round=1\n"));
		assertThrows(IllegalArgumentException.class,
				() -> parse(valid.replaceFirst("\\R", "\n\n")));
	}

	@Test
	void workerArtifactRejectsMismatchedProvenance() {
		String valid = GrpcRetainedReadBenchmark.validWorkerArtifactForTesting();
		String anotherSha = "fedcba9876543210fedcba9876543210fedcba98";

		assertThrows(IllegalArgumentException.class, () ->
				GrpcRetainedReadBenchmark.parseWorkerForTesting(valid, anotherSha, DIGEST, DIGEST));
		assertThrows(IllegalArgumentException.class, () ->
				GrpcRetainedReadBenchmark.parseWorkerForTesting(valid, BUILD_SHA,
						"f".repeat(64), DIGEST));
		assertThrows(IllegalArgumentException.class, () ->
				GrpcRetainedReadBenchmark.parseWorkerForTesting(valid, BUILD_SHA, DIGEST,
						"e".repeat(64)));
	}

	@Test
	void classpathFingerprintIsBoundToExecutableContents() throws Exception {
		var classes = java.nio.file.Files.createDirectories(tempDir.resolve("classes"));
		var classFile = classes.resolve("Example.class");
		java.nio.file.Files.write(classFile, new byte[] {1, 2, 3, 4});
		String before = GrpcRetainedReadBenchmark.classPathContentSha256ForTesting(classes.toString());

		java.nio.file.Files.write(classFile, new byte[] {4, 3, 2, 1});
		String after = GrpcRetainedReadBenchmark.classPathContentSha256ForTesting(classes.toString());

		assertFalse(before.equals(after), "modified class bytes retained the same provenance fingerprint");
	}

	@Test
	void exactArenaInstrumentationObservesTheActualSuccessfulClose() throws Exception {
		String digest = GrpcRetainedReadBenchmark.exactArenaInstrumentationForTesting();
		assertTrue(digest.matches("[0-9a-f]{64}"));
	}

	@Test
	void retainedComparisonRejectsAnyBaselineOtherThanTheFrozenCheckpoint() {
		GrpcRetainedReadBenchmark.validateRawBaselineForTesting(PERFORMANCE_BASELINE_SHA);
		assertThrows(IllegalArgumentException.class,
				() -> GrpcRetainedReadBenchmark.validateRawBaselineForTesting(BUILD_SHA));
	}

	@Test
	void confidenceIntervalUsesPairedLogRatios() {
		var interval = PairedBenchmarkStatistics.pairedLogRatio(
				new double[]{100, 100, 100, 100},
				new double[]{50, 200, 50, 200});

		assertTrue(Math.abs(interval.mean() - 1.0d) < 1.0e-12,
				"geometric mean must be exp(mean(log(candidate/baseline)))");
	}

	private static Map<String, MetricSamples> passingSamples() {
		return new LinkedHashMap<>(GrpcRetainedReadBenchmark.passingMetricSamplesForTesting(SCENARIO));
	}

	private static void replaceCandidate(Map<String, MetricSamples> samples, String metric, double value) {
		samples.put(metric, new MetricSamples(constant(100.0d), constant(value)));
	}

	private static double[] constant(double value) {
		double[] values = new double[10];
		Arrays.fill(values, value);
		return values;
	}

	private static boolean hasFailure(List<String> failures, String text) {
		return failures.stream().anyMatch(failure -> failure.contains(text));
	}

	private static GrpcRetainedReadBenchmark.WorkerArtifactSummary parse(String artifact) {
		return GrpcRetainedReadBenchmark.parseWorkerForTesting(artifact, BUILD_SHA, DIGEST, DIGEST);
	}
}
