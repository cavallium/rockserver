package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FiniteDeadlineContentionBenchmarkTest {

	private static final int OPERATIONS = 16_384;
	private static final long SEED = 104_372_305_701_837L;

	@TempDir
	static Path temporary;

	private static Path baselineWorktree;
	private static Path candidateWorktree;
	private static Path baselineClasses;
	private static Path candidateClasses;
	private static String baselineSha;
	private static String candidateSha;
	private static SchedulerHighContentionBenchmark.Result schedulerResult;

	@BeforeAll
	static void prepareFixtures() throws Exception {
		baselineWorktree = temporary.resolve("baseline-worktree");
		candidateWorktree = temporary.resolve("candidate-worktree");
		baselineClasses = baselineWorktree.resolve("target/classes");
		candidateClasses = candidateWorktree.resolve("target/classes");
		createCheckoutFixture(baselineWorktree, "baseline", baselineClasses);
		createCheckoutFixture(candidateWorktree, "candidate", candidateClasses);
		baselineSha = checkoutSha(baselineWorktree);
		candidateSha = checkoutSha(candidateWorktree);
		schedulerResult = SchedulerHighContentionBenchmark.run(config());
		schedulerResult.assertCorrect();
	}

	@Test
	void preparationIsImmutableParameterizedAndCounterbalanced() throws Exception {
		Path root = prepare("schedule", 12);
		String schedule = Files.readString(root.resolve(FiniteDeadlineContentionBenchmark.SCHEDULE_FILE));
		assertEquals(24L, schedule.lines().filter(line -> line.matches("\\d+\\t\\d+\\t.*")).count());
		assertTrue(schedule.contains("1\t1\tbaseline"));
		assertTrue(schedule.contains("2\t1\tcandidate"));
		assertTrue(schedule.contains("3\t2\tcandidate"));
		assertTrue(schedule.contains("4\t2\tbaseline"));
		assertTrue(schedule.contains("adaptive-stopping\tfalse"));
		assertThrows(IllegalArgumentException.class, () -> prepareAt(root, 12));
		assertThrows(IllegalArgumentException.class, () -> prepare("too-few-pairs", 9));
	}

	@Test
	void completeIdentityAndHardGateEvidencePassesAndResultsCannotBeOverwritten() throws Exception {
		Path root = prepare("passing", 10);
		writeArtifacts(root, Mutation.NONE);

		var result = FiniteDeadlineContentionBenchmark.evaluate(root);

		assertEquals(PairedPerformanceContractV2.Decision.PASS, result.evaluation().decision());
		assertEquals(8, result.evaluation().metrics().size());
		assertTrue(Files.readString(root.resolve(FiniteDeadlineContentionBenchmark.RESULTS_JSON))
				.contains("\"decision\": \"pass\""));
		assertThrows(java.nio.file.FileAlreadyExistsException.class,
				() -> FiniteDeadlineContentionBenchmark.evaluate(root));
	}

	@Test
	void evaluatorRejectsTamperDuplicateMissingReorderAndOverlap() throws Exception {
		for (Mutation mutation : Mutation.values()) {
			if (mutation == Mutation.NONE || mutation == Mutation.REGRESSION
					|| mutation == Mutation.INCONCLUSIVE) continue;
			Path root = prepare("reject-" + mutation.name().toLowerCase(), 10);
			writeArtifacts(root, mutation);
			if (mutation == Mutation.SCHEDULE_REORDER) reorderSchedule(root);
			var result = FiniteDeadlineContentionBenchmark.evaluate(root);
			assertEquals(PairedPerformanceContractV2.Decision.FAIL,
					result.evaluation().decision(), mutation.name());
			assertFalse(result.evaluation().failures().isEmpty(), mutation.name());
		}
	}

	@Test
	void duplicateOrRehashedMetadataCannotChangeThePreparedContract() throws Exception {
		Path duplicate = prepare("duplicate-metadata", 10);
		Files.writeString(duplicate.resolve(FiniteDeadlineContentionBenchmark.METADATA_FILE),
				"fixed-pairs=10\n", StandardOpenOption.APPEND);
		assertThrows(IllegalArgumentException.class,
				() -> FiniteDeadlineContentionBenchmark.Prepared.read(duplicate));

		Path changed = prepare("changed-metadata", 10);
		Path metadata = changed.resolve(FiniteDeadlineContentionBenchmark.METADATA_FILE);
		Files.writeString(metadata, Files.readString(metadata).replace(
				"scheduler-operations=" + OPERATIONS, "scheduler-operations=" + (OPERATIONS + 1)));
		assertThrows(IllegalArgumentException.class,
				() -> FiniteDeadlineContentionBenchmark.Prepared.read(changed));
	}

	@Test
	void allPredeclaredMetricsUseTolerancedHolmContract() throws Exception {
		Path root = prepare("regression", 10);
		writeArtifacts(root, Mutation.REGRESSION);

		var evaluation = FiniteDeadlineContentionBenchmark.evaluate(root).evaluation();

		assertEquals(PairedPerformanceContractV2.Decision.FAIL, evaluation.decision());
		assertTrue(evaluation.metrics().get("attempts-throughput").regressionDemonstrated());
		assertTrue(evaluation.metrics().get("allocation-per-attempt").regressionDemonstrated());
		assertEquals(8, evaluation.stochasticHypotheses());
	}

	@Test
	void equalityPlannerUsesAllEightMetricsAndProducesAConsumableFixedPlan() throws Exception {
		Path root = prepare("precision", 10);
		writeArtifacts(root, Mutation.INCONCLUSIVE);
		assertEquals(PairedPerformanceContractV2.Decision.INCONCLUSIVE,
				FiniteDeadlineContentionBenchmark.evaluate(root).evaluation().decision());

		var plan = FiniteDeadlineContentionPrecisionPlanner.plan(root);

		assertEquals(8, plan.metrics().size());
		assertEquals(40, plan.recommendedFixedPairs());
		String next = Files.readString(root.resolve(FiniteDeadlineContentionPrecisionPlanner.NEXT_FILE));
		assertTrue(next.contains("adaptive-stopping=false"));
		assertTrue(next.contains("throughput-minimum-ratio=0.99"));
		assertTrue(next.contains("cost-maximum-ratio=1.02"));
		Path nextRoot = temporary.resolve("precision-next-run");
		FiniteDeadlineContentionBenchmark.main(new String[] {
				"--mode=prepare", "--root=" + nextRoot,
				"--next-run-properties=" + root.resolve(FiniteDeadlineContentionPrecisionPlanner.NEXT_FILE)
		});
		assertEquals(plan.recommendedFixedPairs(),
				FiniteDeadlineContentionBenchmark.Prepared.read(nextRoot).fixedPairs());
		assertThrows(IllegalArgumentException.class,
				() -> FiniteDeadlineContentionPrecisionPlanner.plan(root));
		var sourcePrepared = FiniteDeadlineContentionBenchmark.Prepared.read(root);
		Path firstWorker = FiniteDeadlineContentionBenchmark.schedule(sourcePrepared).getFirst().artifact();
		Files.writeString(firstWorker, "tampered=value\n", StandardOpenOption.APPEND);
		assertThrows(IllegalArgumentException.class, () -> FiniteDeadlineContentionBenchmark.main(new String[] {
				"--mode=prepare", "--root=" + temporary.resolve("precision-tampered-next-run"),
				"--next-run-properties=" + root.resolve(FiniteDeadlineContentionPrecisionPlanner.NEXT_FILE)
		}));
	}

	@Test
	void plannerRejectsPassFailAndCanonicalResultTamper() throws Exception {
		Path pass = prepare("precision-reject-pass", 10);
		writeArtifacts(pass, Mutation.NONE);
		FiniteDeadlineContentionBenchmark.evaluate(pass);
		assertThrows(IllegalArgumentException.class,
				() -> FiniteDeadlineContentionPrecisionPlanner.plan(pass));

		Path fail = prepare("precision-reject-fail", 10);
		writeArtifacts(fail, Mutation.REGRESSION);
		FiniteDeadlineContentionBenchmark.evaluate(fail);
		assertThrows(IllegalArgumentException.class,
				() -> FiniteDeadlineContentionPrecisionPlanner.plan(fail));

		Path tamper = prepare("precision-reject-tamper", 10);
		writeArtifacts(tamper, Mutation.INCONCLUSIVE);
		FiniteDeadlineContentionBenchmark.evaluate(tamper);
		Path results = tamper.resolve(FiniteDeadlineContentionBenchmark.RESULTS_JSON);
		Files.writeString(results, Files.readString(results).replaceFirst(
				"\\\"ratio_mean\\\":", "\\\"ratio_mean\\\": 1.000000, \\\"tampered\\\":"));
		assertThrows(IllegalArgumentException.class,
				() -> FiniteDeadlineContentionPrecisionPlanner.plan(tamper));
	}

	private static SchedulerHighContentionBenchmark.Config config() {
		return new SchedulerHighContentionBenchmark.Config(OPERATIONS, 8, 2, 2, 1,
				4_096, 4_096, 64, 2, 1, 5, 10, 5, 30, true, SEED, Duration.ofSeconds(30));
	}

	private static Path prepare(String name, int pairs) throws Exception {
		Path root = temporary.resolve(name);
		prepareAt(root, pairs);
		return root;
	}

	private static void prepareAt(Path root, int pairs) throws Exception {
		FiniteDeadlineContentionBenchmark.main(new String[] {
				"--mode=prepare", "--root=" + root,
				"--baseline-sha=" + baselineSha, "--candidate-sha=" + candidateSha,
				"--baseline-worktree=" + baselineWorktree, "--candidate-worktree=" + candidateWorktree,
				"--baseline-classes=" + baselineClasses, "--candidate-classes=" + candidateClasses,
				"--hardware-description=deadline-test-host", "--enforce=false", "--pairs=" + pairs,
				"--operations=" + OPERATIONS, "--warmup-operations=8192", "--submitters=8",
				"--read-workers=2", "--write-workers=2", "--analytical-limit=1",
				"--foreground-capacity=4096", "--batch-capacity=4096", "--work-tokens=64",
				"--cooperative-yields=2", "--cooperative-parks=1", "--expired-deadline-percent=5",
				"--cancellation-percent=10", "--failure-percent=5", "--cooperative-percent=30",
				"--alternate-storage-pressure=true", "--seed=" + SEED, "--timeout-seconds=30"
		});
	}

	private static void writeArtifacts(Path root, Mutation mutation) throws Exception {
		var prepared = FiniteDeadlineContentionBenchmark.Prepared.read(root);
		var runs = FiniteDeadlineContentionBenchmark.schedule(prepared);
		var firstBaseline = runs.stream().filter(run -> run.implementation()
				== FiniteDeadlineContentionBenchmark.Implementation.BASELINE).findFirst().orElseThrow();
		var firstCandidate = runs.stream().filter(run -> run.implementation()
				== FiniteDeadlineContentionBenchmark.Implementation.CANDIDATE).findFirst().orElseThrow();
		var baselineTemplate = FiniteDeadlineContentionBenchmark.Artifact.fromResult(prepared, firstBaseline,
				prepared.baselineSha(), prepared.baselineProductionSha256(), 1L, 2L, schedulerResult);
		var candidateTemplate = FiniteDeadlineContentionBenchmark.Artifact.fromResult(prepared, firstCandidate,
				prepared.candidateSha(), prepared.candidateProductionSha256(), 1L, 2L, schedulerResult);
		long priorFinish = 1_000L;
		for (var run : runs) {
			if (mutation == Mutation.MISSING && run.ordinal() == 7) continue;
			boolean baseline = run.implementation() == FiniteDeadlineContentionBenchmark.Implementation.BASELINE;
			var artifact = (baseline ? baselineTemplate : candidateTemplate)
					.with("round", Integer.toString(run.round()))
					.with("ordinal", Integer.toString(run.ordinal()))
					.with("started-epoch-millis", Long.toString(priorFinish + 1L))
					.with("finished-epoch-millis", Long.toString(priorFinish + 2L))
					.with("process-id", Long.toString(10_000L + run.ordinal()));
			if (run.ordinal() == 7) artifact = mutate(artifact, mutation, priorFinish);
			if (mutation == Mutation.REGRESSION && !baseline) {
				artifact = artifact.with("attempts_per_second",
						Double.toString(schedulerResult.attemptsPerSecond() * 0.90d))
						.with("process.allocated_bytes_per_attempt",
								Double.toString(schedulerResult.allocatedBytesPerAttempt() * 1.10d));
			}
			if (mutation == Mutation.INCONCLUSIVE && !baseline) {
				double[] standardized = {-1.5d, -1.2d, -0.9d, -0.6d, -0.3d,
						0.3d, 0.6d, 0.9d, 1.2d, 1.5d};
				double logRatio = -0.004d + standardized[run.round() - 1] * 0.015255d;
				artifact = artifact.with("attempts_per_second",
						Double.toString(schedulerResult.attemptsPerSecond() * Math.exp(logRatio)));
			}
			artifact.write(run.artifact());
			if (run.ordinal() == 7 && mutation == Mutation.DUPLICATE_KEY) {
				Files.writeString(run.artifact(), "round=7\n", StandardOpenOption.APPEND);
			} else if (run.ordinal() == 7 && mutation == Mutation.UNKNOWN_KEY) {
				Files.writeString(run.artifact(), "unexpected=value\n", StandardOpenOption.APPEND);
			}
			priorFinish += 3L;
		}
	}

	private static FiniteDeadlineContentionBenchmark.Artifact mutate(
			FiniteDeadlineContentionBenchmark.Artifact artifact, Mutation mutation, long priorFinish) {
		return switch (mutation) {
			case CONFIG -> artifact.with("configuration-sha256", "0".repeat(64));
			case BUILD -> artifact.with("build-sha", "f".repeat(40));
			case PRODUCTION -> artifact.with("production-sha256", "f".repeat(64));
			case METRIC_SET -> artifact.with("metric-set-sha256", "f".repeat(64));
			case GATE -> artifact.with("gate-drain", "false");
			case DUPLICATE_PID -> artifact.with("process-id", "10001");
			case OVERLAP -> artifact.with("started-epoch-millis", Long.toString(priorFinish - 2L));
			default -> artifact;
		};
	}

	private static void reorderSchedule(Path root) throws Exception {
		Path schedule = root.resolve(FiniteDeadlineContentionBenchmark.SCHEDULE_FILE);
		var lines = new java.util.ArrayList<>(Files.readAllLines(schedule));
		java.util.Collections.swap(lines, 7, 8);
		Files.writeString(schedule, String.join("\n", lines) + '\n');
	}

	private static void createCheckoutFixture(Path worktree, String identity, Path classes) throws Exception {
		Files.createDirectories(classes);
		Files.writeString(classes.resolve("identity.txt"), identity);
		command(worktree, "git", "init", "-q");
		command(worktree, "git", "config", "user.email", "benchmark@example.invalid");
		command(worktree, "git", "config", "user.name", "Benchmark Test");
		Files.writeString(worktree.resolve("source.txt"), identity);
		command(worktree, "git", "add", "source.txt");
		command(worktree, "git", "commit", "-q", "-m", "fixture");
	}

	private static String checkoutSha(Path worktree) throws Exception {
		return command(worktree, "git", "rev-parse", "HEAD").trim();
	}

	private static String command(Path directory, String... command) throws Exception {
		var process = new ProcessBuilder(command).directory(directory.toFile()).redirectErrorStream(true).start();
		String output = new String(process.getInputStream().readAllBytes());
		if (process.waitFor() != 0) throw new IllegalStateException(output);
		return output;
	}

	private enum Mutation {
		NONE, CONFIG, BUILD, PRODUCTION, METRIC_SET, GATE, DUPLICATE_PID, OVERLAP,
		DUPLICATE_KEY, UNKNOWN_KEY, MISSING, SCHEDULE_REORDER, REGRESSION, INCONCLUSIVE
	}
}
