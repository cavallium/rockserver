package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AdversarialBatchLivenessPairedBenchmarkTest {

	@TempDir
	Path temporary;

	@Test
	void scheduleIsTenCounterbalancedPairsAndCannotBeReprepared() throws Exception {
		Path root = prepare("schedule", true);
		String schedule = Files.readString(root.resolve("schedule.tsv"));
		assertEquals(20L, schedule.lines().filter(line -> line.matches("\\d+\\t\\d+\\t.*")).count());
		assertTrue(schedule.contains("1\t1\tbaseline"));
		assertTrue(schedule.contains("2\t1\tcandidate"));
		assertTrue(schedule.contains("3\t2\tcandidate"));
		assertTrue(schedule.contains("4\t2\tbaseline"));
		assertThrows(IllegalArgumentException.class, () -> prepareAt(root, true));
	}

	@Test
	void strictSyntheticArtifactsProveBaselineStallCandidateProgressAndBoundedFairness() throws Exception {
		Path root = prepare("passing", true);
		writeArtifacts(root, Mutation.NONE);

		var result = AdversarialBatchLivenessPairedBenchmark.evaluate(root);

		assertTrue(result.passed());
		assertTrue(result.failures().isEmpty());
		assertTrue(result.maximumGapRatio().upper95() <= 0.5d);
		String json = Files.readString(root.resolve("results.json"));
		assertTrue(json.contains("\"fixed_pairs\": 10"));
		assertTrue(json.contains("\"fresh_processes\": 20"));
		assertTrue(json.contains("\"passed\": true"));
	}

	@Test
	void evaluatorRejectsEverySemanticAndProvenanceEscape() throws Exception {
		for (var mutation : Mutation.values()) {
			if (mutation == Mutation.NONE) continue;
			Path root = prepare("reject-" + mutation.name().toLowerCase(), false);
			writeArtifacts(root, mutation);
			var result = AdversarialBatchLivenessPairedBenchmark.evaluate(root);
			assertFalse(result.passed(), mutation.name());
			assertFalse(result.failures().isEmpty(), mutation.name());
		}
	}

	@Test
	void productionClasspathReplacementIsExactAndRejectsWrongOrigin() {
		String separator = java.io.File.pathSeparator;
		Path candidate = temporary.resolve("candidate/classes").toAbsolutePath();
		Path baseline = temporary.resolve("baseline/classes").toAbsolutePath();
		String classPath = temporary.resolve("test-classes").toAbsolutePath() + separator + candidate
				+ separator + temporary.resolve("dependency.jar").toAbsolutePath();
		String replaced = AdversarialBatchLivenessPairedBenchmark.replaceProductionClasses(
				classPath, candidate, baseline);
		assertTrue(replaced.contains(baseline.toString()));
		assertFalse(replaced.contains(candidate.toString()));
		assertThrows(IllegalArgumentException.class, () ->
				AdversarialBatchLivenessPairedBenchmark.replaceProductionClasses(
						classPath, temporary.resolve("absent"), baseline));
	}

	@Test
	void productionIdentityAllowsHarnessOnlyDescendantsButRejectsSourceDrift() throws Exception {
		Path worktree = temporary.resolve("production-identity");
		Path classes = worktree.resolve("target/classes");
		createCheckoutFixture(worktree, "production", classes);
		String productionSha = checkoutSha(worktree);
		Path harness = worktree.resolve("src/test/java/Harness.java");
		Files.createDirectories(harness.getParent());
		Files.writeString(harness, "final class Harness {}\n");
		new ProcessBuilder("git", "add", "src/test/java/Harness.java")
				.directory(worktree.toFile()).start().waitFor();
		new ProcessBuilder("git", "commit", "-q", "-m", "harness")
				.directory(worktree.toFile()).start().waitFor();

		AdversarialBatchLivenessPairedBenchmark.verifyProductionCheckout(worktree, productionSha);

		Path drift = worktree.resolve("src/main/java/ProductionDrift.java");
		Files.createDirectories(drift.getParent());
		Files.writeString(drift, "final class ProductionDrift {}\n");
		assertThrows(IllegalArgumentException.class, () ->
				AdversarialBatchLivenessPairedBenchmark.verifyProductionCheckout(worktree, productionSha));
	}

	private Path prepare(String name, boolean enforce) throws Exception {
		Path root = temporary.resolve(name);
		prepareAt(root, enforce);
		return root;
	}

	private void prepareAt(Path root, boolean enforce) throws Exception {
		Path baselineWorktree = temporary.resolve("baseline-worktree");
		Path candidateWorktree = temporary.resolve("candidate-worktree");
		Path baselineClasses = baselineWorktree.resolve("target/classes");
		Path candidateClasses = candidateWorktree.resolve("target/classes");
		if (!Files.exists(baselineWorktree)) {
			createCheckoutFixture(baselineWorktree, "baseline", baselineClasses);
			createCheckoutFixture(candidateWorktree, "candidate", candidateClasses);
		}
		String baselineSha = checkoutSha(baselineWorktree);
		String candidateSha = checkoutSha(candidateWorktree);
		AdversarialBatchLivenessPairedBenchmark.main(new String[] {
				"--mode=prepare", "--root=" + root,
				"--baseline-sha=" + baselineSha, "--candidate-sha=" + candidateSha,
				"--baseline-worktree=" + baselineWorktree, "--candidate-worktree=" + candidateWorktree,
				"--baseline-classes=" + baselineClasses, "--candidate-classes=" + candidateClasses,
				"--hardware-description=test-host", "--enforce=" + enforce,
				"--pressure-interval-ms=10", "--nondispatchable-phase-ms=240",
				"--candidate-maximum-gap-ms=100", "--candidate-minimum-throughput=20"
		});
	}

	private static void createCheckoutFixture(Path worktree, String identity, Path classes) throws Exception {
		Files.createDirectories(classes);
		Files.writeString(classes.resolve("identity.txt"), identity);
		new ProcessBuilder("git", "init", "-q").directory(worktree.toFile()).start().waitFor();
		new ProcessBuilder("git", "config", "user.email", "benchmark@example.invalid")
				.directory(worktree.toFile()).start().waitFor();
		new ProcessBuilder("git", "config", "user.name", "Benchmark Test")
				.directory(worktree.toFile()).start().waitFor();
		Files.writeString(worktree.resolve("source.txt"), identity);
		new ProcessBuilder("git", "add", "source.txt").directory(worktree.toFile()).start().waitFor();
		new ProcessBuilder("git", "commit", "-q", "-m", "fixture")
				.directory(worktree.toFile()).start().waitFor();
	}

	private static String checkoutSha(Path worktree) throws Exception {
		var process = new ProcessBuilder("git", "rev-parse", "HEAD")
				.directory(worktree.toFile()).redirectErrorStream(true).start();
		String value = new String(process.getInputStream().readAllBytes()).trim();
		if (process.waitFor() != 0) throw new IllegalStateException(value);
		return value;
	}

	private void writeArtifacts(Path root, Mutation mutation) throws Exception {
		var prepared = AdversarialBatchLivenessPairedBenchmark.Prepared.read(root);
		long previousFinish = 1_000L;
		boolean mutationApplied = false;
		String hostSha = AdversarialBatchLivenessPairedBenchmark.hostSha("test-host");
		String runtimeSha = AdversarialBatchLivenessPairedBenchmark.runtimeSha();
		String harnessSha = AdversarialBatchLivenessPairedBenchmark.harnessSha();
		for (var run : AdversarialBatchLivenessPairedBenchmark.schedule(prepared)) {
			boolean baseline = run.implementation()
					== AdversarialBatchLivenessPairedBenchmark.Implementation.BASELINE;
			long completions = baseline ? 0L : 20L;
			long gap = baseline ? 240_000_000L : 20_000_000L;
			double throughput = completions * 1_000_000_000.0d / 240_000_000L;
			long fairDelay = 20_000_000L;
			boolean topology = true;
			String production = baseline ? prepared.baselineProductionSha256()
					: prepared.candidateProductionSha256();
			long processId = 10_000L + run.ordinal();
			boolean matchingImplementation = switch (mutation) {
				case BASELINE_PROGRESS -> baseline;
				case CANDIDATE_STALL -> !baseline;
				default -> run.ordinal() == 7;
			};
			if (!mutationApplied && mutation != Mutation.NONE && matchingImplementation) {
				mutationApplied = true;
				switch (mutation) {
					case BASELINE_PROGRESS -> {
						completions = 1L;
					}
					case CANDIDATE_STALL -> {
						completions = 0L;
						gap = 240_000_000L;
						throughput = 0.0d;
					}
					case FAIRNESS -> fairDelay = 300_000_000L;
					case TOPOLOGY -> topology = false;
					case PRODUCTION -> production = "f".repeat(64);
					case PROCESS -> processId = 10_001L;
					case NONE -> {
					}
				}
			}
			throughput = completions * 1_000_000_000.0d / 240_000_000L;
			var result = new AdversarialBatchLivenessBenchmark.Result(topology,
					completions, gap, throughput, fairDelay, 240_000_000L, 10_000_000L);
			String build = baseline ? prepared.baselineSha() : prepared.candidateSha();
			var artifact = new AdversarialBatchLivenessPairedBenchmark.Artifact(run.round(),
					run.ordinal(), run.implementation(), build, prepared.configurationSha256(),
					hostSha, "test-host", runtimeSha, harnessSha,
					(baseline ? "4" : "5").repeat(64), production, processId,
					previousFinish + 1L, previousFinish + 2L, true, result);
			artifact.write(run.artifact());
			previousFinish += 3L;
		}
	}

	private enum Mutation {
		NONE,
		BASELINE_PROGRESS,
		CANDIDATE_STALL,
		FAIRNESS,
		TOPOLOGY,
		PRODUCTION,
		PROCESS
	}
}
