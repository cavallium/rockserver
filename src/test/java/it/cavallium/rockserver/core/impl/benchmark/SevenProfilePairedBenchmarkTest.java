package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SevenProfilePairedBenchmarkTest {

	@TempDir
	Path temporaryDirectory;

	@Test
	void scheduleIsPredeterminedAndCounterbalanced() {
		assertEquals(List.of("baseline", "candidate"),
				SevenProfilePairedBenchmark.scheduledOrderForTesting(1));
		assertEquals(List.of("candidate", "baseline"),
				SevenProfilePairedBenchmark.scheduledOrderForTesting(2));
		assertEquals(List.of("baseline", "candidate"),
				SevenProfilePairedBenchmark.scheduledOrderForTesting(9));
		assertEquals(List.of("candidate", "baseline"),
				SevenProfilePairedBenchmark.scheduledOrderForTesting(10));
	}

	@Test
	void prepareWritesOneShotScheduleWithFrozenEnvironmentLabels() throws Exception {
		Path root = temporaryDirectory.resolve("paired");
		String candidate = "0123456789abcdef0123456789abcdef01234567";
		String[] arguments = {
				"--mode=prepare", "--root=" + root, "--candidate-sha=" + candidate,
				"--host-state=dedicated", "--storage-label=hdd-btrfs", "--cache-state=cold",
				"--enforce=false"
		};

		SevenProfilePairedBenchmark.main(arguments);
		String schedule = Files.readString(root.resolve("schedule.tsv"));

		assertTrue(schedule.contains("baseline-sha\tbb4f1a7e90db1fdfd785936594d080e8c4a0ba4e\n"));
		assertTrue(schedule.contains("host-state\tdedicated\n"));
		assertTrue(schedule.contains("storage-label\thdd-btrfs\n"));
		assertTrue(schedule.contains("cache-state\tcold\n"));
		assertEquals(20L, schedule.lines().filter(line -> line.matches("\\d+\\t\\d+\\t.*")).count());
		assertThrows(IllegalArgumentException.class, () -> SevenProfilePairedBenchmark.main(arguments));
	}

	@Test
	void sevenProfileGateRequiresEveryMetricAndOneMaterialPrimaryImprovement() {
		Map<String, PairedPerformanceContract.MetricSamples> samples = completeSamples();
		String throughput = samples.keySet().stream().filter(name -> name.endsWith(".throughput"))
				.findFirst().orElseThrow();
		samples.put(throughput, paired(100.0d, 103.0d));

		var result = SevenProfilePairedBenchmark.evaluateForTesting(samples, List.of());

		assertTrue(result.automaticAcceptancePassed());
		assertTrue(result.materialImprovements().contains(throughput));
	}

	@Test
	void allocationAndOutstandingRegressionsHaveNoAutomaticOrExceptionPass() {
		Map<String, PairedPerformanceContract.MetricSamples> samples = completeSamples();
		String throughput = samples.keySet().stream().filter(name -> name.endsWith(".throughput"))
				.findFirst().orElseThrow();
		String outstanding = samples.keySet().stream().filter(name -> name.equals("maximum-outstanding"))
				.findFirst().orElseThrow();
		samples.put(throughput, paired(100.0d, 103.0d));
		samples.put("allocated-bytes-per-operation", paired(100.0d, 101.0d));
		samples.put(outstanding, paired(2.0d, 3.0d));

		var result = SevenProfilePairedBenchmark.evaluateForTesting(samples, List.of());

		assertFalse(result.automaticAcceptancePassed());
		assertFalse(result.exceptionCandidates().contains("allocated-bytes-per-operation"));
		assertFalse(result.exceptionCandidates().contains(outstanding));
	}

	private static Map<String, PairedPerformanceContract.MetricSamples> completeSamples() {
		Map<String, PairedPerformanceContract.MetricSamples> samples = new LinkedHashMap<>();
		// Let the gate enumerate its contract through deterministic missing-metric failures, then fill them.
		var missing = SevenProfilePairedBenchmark.evaluateForTesting(Map.of(), List.of());
		for (String failure : missing.failures()) {
			if (failure.startsWith("missing metric ")) {
				samples.put(failure.substring("missing metric ".length()), paired(100.0d, 100.0d));
			}
		}
		return samples;
	}

	private static PairedPerformanceContract.MetricSamples paired(double baseline, double candidate) {
		double[] base = new double[PairedPerformanceContract.REQUIRED_PAIRS];
		double[] next = new double[PairedPerformanceContract.REQUIRED_PAIRS];
		java.util.Arrays.fill(base, baseline);
		java.util.Arrays.fill(next, candidate);
		return new PairedPerformanceContract.MetricSamples(base, next);
	}
}
