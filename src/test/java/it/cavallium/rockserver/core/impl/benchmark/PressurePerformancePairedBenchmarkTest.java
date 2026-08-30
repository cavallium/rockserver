package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PressurePerformancePairedBenchmarkTest {

	@TempDir
	Path temporary;

	@Test
	void prepareWritesFortyFreshJvmRunsWithCounterbalancedPairs() throws Exception {
		Path root = temporary.resolve("prepare");
		prepare(root, false);

		String schedule = Files.readString(root.resolve("schedule.tsv"));
		assertEquals(40L, schedule.lines().filter(line -> line.matches("\\d+\\t\\d+\\t.*")).count());
		assertTrue(schedule.contains("1\t1\tscheduler\tbaseline"));
		assertTrue(schedule.contains("2\t1\tscheduler\tcandidate"));
		assertTrue(schedule.contains("5\t2\tscheduler\tcandidate"));
		assertTrue(schedule.contains("6\t2\tscheduler\tbaseline"));
		assertThrows(IllegalArgumentException.class, () -> prepare(root, false));
	}

	@Test
	void workerRunsRealSchedulerSmokeAndWritesStrictFirstArtifact() throws Exception {
		Path root = temporary.resolve("worker");
		prepare(root, false);
		assertThrows(IllegalStateException.class, () -> PressurePerformancePairedBenchmark.main(new String[] {
				"--mode=worker", "--root=" + root, "--suite=scheduler", "--round=1",
				"--implementation=candidate", "--build-sha=" + "b".repeat(40)
		}));

		PressurePerformancePairedBenchmark.main(new String[] {
				"--mode=worker", "--root=" + root, "--suite=scheduler", "--round=1",
				"--implementation=baseline", "--build-sha=" + "a".repeat(40)
		});

		var prepared = PressurePerformancePairedBenchmark.Prepared.read(root);
		var first = PressurePerformancePairedBenchmark.schedule(prepared).getFirst();
		var artifact = PressureBenchmarkArtifact.read(first.artifact(), first.suite(),
				PressurePerformancePairedBenchmark.metricNames(first.suite(), prepared.signalColumnFamilyCounts()));
		assertTrue(artifact.correctnessPassed());
		assertEquals(1, artifact.ordinal());
		assertThrows(IllegalStateException.class, () -> PressurePerformancePairedBenchmark.main(new String[] {
				"--mode=worker", "--root=" + root, "--suite=scheduler", "--round=1",
				"--implementation=baseline", "--build-sha=" + "a".repeat(40)
		}));
	}

	@Test
	void completeSyntheticArtifactsProduceMachineAndHumanReports() throws Exception {
		Path root = temporary.resolve("evaluate-pass");
		prepare(root, true);
		writeSyntheticArtifacts(root, Mutation.NONE);

		PressurePerformancePairedBenchmark.main(new String[] {"--mode=evaluate", "--root=" + root});

		String json = Files.readString(root.resolve("results.json"));
		String markdown = Files.readString(root.resolve("results.md"));
		assertTrue(json.contains("\"schema\": \"rockserver-pressure-performance-comparison-v1\""));
		assertTrue(json.contains("\"fixed_pairs\": 10"));
		assertTrue(json.contains("\"fresh_processes\": 40"));
		assertTrue(json.contains("\"passed\": true"));
		assertTrue(json.contains("\"host_sha256\": \"" + "1".repeat(64) + "\""));
		assertTrue(json.contains("\"baseline\":[100.000000000,100.000000000"));
		assertTrue(json.contains("\"candidate\":[103.000000000,103.000000000"));
		assertTrue(markdown.contains("Overall: **PASS**"));
	}

	@Test
	void evaluatorRejectsBuildConfigHostProcessAndOrderDrift() throws Exception {
		for (Mutation mutation : new Mutation[] {
				Mutation.BUILD, Mutation.CONFIGURATION, Mutation.HOST, Mutation.PROCESS, Mutation.ORDER
		}) {
			Path root = temporary.resolve("reject-" + mutation.name().toLowerCase());
			prepare(root, true);
			writeSyntheticArtifacts(root, mutation);
			assertThrows(IllegalStateException.class, () -> PressurePerformancePairedBenchmark.main(
					new String[] {"--mode=evaluate", "--root=" + root}), mutation.name());
		}
	}

	private static void prepare(Path root, boolean enforce) throws Exception {
		PressurePerformancePairedBenchmark.main(new String[] {
				"--mode=prepare", "--root=" + root,
				"--baseline-sha=" + "a".repeat(40), "--candidate-sha=" + "b".repeat(40),
				"--host-state=dedicated", "--hardware-description=test-host", "--enforce=" + enforce,
				"--scheduler-operations=1024", "--scheduler-warmup-operations=512",
				"--scheduler-submitters=4", "--scheduler-read-workers=2", "--scheduler-write-workers=2",
				"--scheduler-analytical-limit=1", "--scheduler-foreground-capacity=128",
				"--scheduler-batch-capacity=128", "--scheduler-work-tokens=8",
				"--signal-cf-counts=1,64", "--signal-warmup-columns=1000",
				"--signal-measured-columns=2000", "--signal-minimum-evaluations=100",
				"--signal-maximum-evaluations=1000", "--signal-latency-sample-stride=16"
		});
	}

	private static void writeSyntheticArtifacts(Path root, Mutation mutation) throws Exception {
		var prepared = PressurePerformancePairedBenchmark.Prepared.read(root);
		long previousFinish = 1_000L;
		for (var run : PressurePerformancePairedBenchmark.schedule(prepared)) {
			var names = PressurePerformancePairedBenchmark.metricNames(
					run.suite(), prepared.signalColumnFamilyCounts());
			var metrics = new LinkedHashMap<String, Double>();
			for (String name : names) metrics.put(name, 100.0d);
			if (run.suite() == PressureBenchmarkArtifact.Suite.SCHEDULER
					&& run.implementation() == PressureBenchmarkArtifact.Implementation.CANDIDATE) {
				metrics.put("scheduler.useful_runs_per_second", 103.0d);
			}
			boolean mutate = run.ordinal() == 7;
			String build = run.implementation() == PressureBenchmarkArtifact.Implementation.BASELINE
					? prepared.baselineSha() : prepared.candidateSha();
			long started = previousFinish + 1L;
			long finished = started + 1L;
			long processId = 10_000L + run.ordinal();
			if (mutate && mutation == Mutation.BUILD) build = "c".repeat(40);
			String configuration = mutate && mutation == Mutation.CONFIGURATION
					? "d".repeat(64) : prepared.configurationSha256();
			String host = mutate && mutation == Mutation.HOST ? "e".repeat(64) : "1".repeat(64);
			if (mutate && mutation == Mutation.PROCESS) processId = 10_001L;
			if (mutate && mutation == Mutation.ORDER) started = previousFinish - 1L;
			var artifact = new PressureBenchmarkArtifact.Artifact(run.suite(), run.round(), run.ordinal(),
					run.implementation(), build, configuration, host, "test-host", "2".repeat(64),
					(run.implementation() == PressureBenchmarkArtifact.Implementation.BASELINE ? "3" : "4").repeat(64),
					(run.implementation() == PressureBenchmarkArtifact.Implementation.BASELINE ? "5" : "6").repeat(64),
					processId, started, finished, true, true, Map.copyOf(metrics));
			PressureBenchmarkArtifact.write(run.artifact(), artifact, names);
			previousFinish = finished;
		}
	}

	private enum Mutation { NONE, BUILD, CONFIGURATION, HOST, PROCESS, ORDER }
}
