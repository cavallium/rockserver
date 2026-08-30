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

class PressurePerformancePrecisionPlannerTest {

	@TempDir
	Path temporary;

	@Test
	void quantilesPowerAndRequiredPairsMatchReferenceBoundaries() {
		assertEquals(1.833d,
				PressurePerformancePrecisionPlanner.studentTQuantile(0.95d, 9), 5.0e-4d);
		assertEquals(1.2815516d,
				PressurePerformancePrecisionPlanner.normalQuantile(0.90d), 2.0e-5d);
		assertEquals(0.90d,
				PressurePerformancePrecisionPlanner.normalCdf(
						PressurePerformancePrecisionPlanner.normalQuantile(0.90d)), 2.0e-7d);

		double margin = Math.log(1.02d);
		double lowPower = PressurePerformancePrecisionPlanner.approximateNonInferiorityPower(
				10, 0.03d, margin);
		double highPower = PressurePerformancePrecisionPlanner.approximateNonInferiorityPower(
				40, 0.03d, margin);
		assertTrue(highPower > lowPower);
		int required = PressurePerformancePrecisionPlanner.requiredPairs(0.03d, margin, 0.90d);
		assertTrue(required >= 10);
		assertTrue(PressurePerformancePrecisionPlanner.approximateNonInferiorityPower(
				required, 0.03d, margin) >= 0.90d);
		if (required > 10) {
			assertTrue(PressurePerformancePrecisionPlanner.approximateNonInferiorityPower(
					required - 1, 0.03d, margin) < 0.90d);
		}
		assertEquals(1.0d,
				PressurePerformancePrecisionPlanner.durationScale(10, 0.0d, margin, 0.999d));
	}

	@Test
	void inconclusiveV2ArtifactsProduceFixedAllMetricPlanAndEqualComponentProvenance() throws Exception {
		Path root = temporary.resolve("inconclusive");
		prepare(root);
		writeSyntheticArtifacts(root, true);
		PressurePerformancePairedBenchmark.main(new String[] {"--mode=evaluate", "--root=" + root});
		var classes = componentRoots("equal", true);

		PressurePerformancePrecisionPlanner.main(new String[] {
				"--root=" + root,
				"--baseline-classes=" + classes.baseline(),
				"--candidate-classes=" + classes.candidate()
		});

		String json = Files.readString(root.resolve("precision-plan.json"));
		String markdown = Files.readString(root.resolve("precision-plan.md"));
		String next = Files.readString(root.resolve("next-run-v2.properties"));
		assertTrue(json.contains("\"schema\": \"rockserver-pressure-performance-precision-plan-v1\""));
		assertTrue(json.contains("\"source_decision\": \"inconclusive\""));
		assertTrue(json.contains("\"planning_effect\": \"exact-equality\""));
		assertTrue(json.contains("\"stochastic_metrics\": 143"));
		assertTrue(json.contains("\"critical_scheduling_metrics\": 54"));
		assertTrue(json.contains("\"critical_beta_budget\": 0.03"));
		assertTrue(json.contains("\"used_for_decision\":false"));
		assertTrue(json.contains("\"equal\":true"));
		assertTrue(json.contains("scheduler.cpu_nanos_per_attempt"));
		assertTrue(json.contains("\"critical\":true"));
		assertEquals(143L, json.lines().filter(line -> line.contains("\"suite\":")).count());
		assertTrue(markdown.contains("Source decision: `INCONCLUSIVE` (unchanged)"));
		assertTrue(markdown.contains("provenance only; never used for PASS"));
		assertTrue(next.contains("schema=rockserver-pressure-performance-next-run-v1"));
		assertTrue(next.contains("fixed-pairs=10\nadaptive-stopping=false"));
		assertTrue(next.contains("planning-effect=exact-equality"));
		assertTrue(next.contains("planning-critical-beta-budget=0.03"));
		assertTrue(nextConfigurationValue(next, "scheduler-operations") > 1_024L);
		assertEquals(2_000L, nextConfigurationValue(next, "signal-measured-columns"));
		assertThrows(IllegalArgumentException.class,
				() -> PressurePerformancePrecisionPlanner.main(new String[] {
						"--root=" + root,
						"--baseline-classes=" + classes.baseline(),
						"--candidate-classes=" + classes.candidate()
				}));
	}

	@Test
	void differingSignalComponentBytesRemainNonDecisionProvenance() throws Exception {
		Path root = temporary.resolve("different-components");
		prepare(root);
		writeSyntheticArtifacts(root, true);
		PressurePerformancePairedBenchmark.main(new String[] {"--mode=evaluate", "--root=" + root});
		var classes = componentRoots("different", false);

		PressurePerformancePrecisionPlanner.main(new String[] {
				"--root=" + root,
				"--baseline-classes=" + classes.baseline(),
				"--candidate-classes=" + classes.candidate()
		});

		String json = Files.readString(root.resolve("precision-plan.json"));
		assertTrue(json.contains("\"equal\":false"));
		assertTrue(json.contains("\"used_for_decision\":false"));
		assertTrue(Files.readString(root.resolve("next-run-v2.properties"))
				.contains("contract-version=v2"));
	}

	@Test
	void plannerRejectsPassResultsAndNonV2OrTamperedEvidence() throws Exception {
		Path pass = temporary.resolve("pass");
		prepare(pass);
		writeSyntheticArtifacts(pass, false);
		PressurePerformancePairedBenchmark.main(new String[] {"--mode=evaluate", "--root=" + pass});
		var classes = componentRoots("pass", true);
		assertThrows(IllegalArgumentException.class,
				() -> PressurePerformancePrecisionPlanner.main(new String[] {
						"--root=" + pass,
						"--baseline-classes=" + classes.baseline(),
						"--candidate-classes=" + classes.candidate()
				}));

		Path tampered = temporary.resolve("tampered");
		prepare(tampered);
		writeSyntheticArtifacts(tampered, true);
		PressurePerformancePairedBenchmark.main(new String[] {"--mode=evaluate", "--root=" + tampered});
		var prepared = PressurePerformancePairedBenchmark.Prepared.read(tampered);
		var first = PressurePerformancePairedBenchmark.schedule(prepared).getFirst();
		Files.writeString(first.artifact(), Files.readString(first.artifact())
				.replace("configuration-sha256=" + prepared.configurationSha256(),
						"configuration-sha256=" + "f".repeat(64)));
		assertThrows(IllegalArgumentException.class,
				() -> PressurePerformancePrecisionPlanner.main(new String[] {
						"--root=" + tampered,
						"--baseline-classes=" + classes.baseline(),
						"--candidate-classes=" + classes.candidate()
				}));
	}

	private static void prepare(Path root) throws Exception {
		PressurePerformancePairedBenchmark.main(new String[] {
				"--mode=prepare", "--root=" + root, "--contract-version=v2",
				"--baseline-sha=" + "a".repeat(40), "--candidate-sha=" + "b".repeat(40),
				"--host-state=dedicated", "--hardware-description=test-host", "--enforce=false",
				"--scheduler-operations=1024", "--scheduler-warmup-operations=512",
				"--scheduler-submitters=4", "--scheduler-read-workers=2", "--scheduler-write-workers=2",
				"--scheduler-analytical-limit=1", "--scheduler-foreground-capacity=128",
				"--scheduler-batch-capacity=128", "--scheduler-work-tokens=8",
				"--signal-cf-counts=1,64", "--signal-warmup-columns=1000",
				"--signal-measured-columns=2000", "--signal-minimum-evaluations=100",
				"--signal-maximum-evaluations=1000", "--signal-latency-sample-stride=16"
		});
	}

	private static void writeSyntheticArtifacts(Path root, boolean imprecise) throws Exception {
		var prepared = PressurePerformancePairedBenchmark.Prepared.read(root);
		long priorFinish = 1_000L;
		for (var run : PressurePerformancePairedBenchmark.schedule(prepared)) {
			var names = PressurePerformancePairedBenchmark.metricNames(
					run.suite(), prepared.signalColumnFamilyCounts());
			var metrics = new LinkedHashMap<String, Double>();
			for (String name : names) metrics.put(name, 100.0d);
			if (imprecise && run.suite() == PressureBenchmarkArtifact.Suite.SCHEDULER
					&& run.implementation() == PressureBenchmarkArtifact.Implementation.CANDIDATE) {
				metrics.put("scheduler.cpu_nanos_per_attempt",
						(run.round() & 1) == 1 ? 90.0d : 100.0d / 0.90d);
			}
			String build = run.implementation() == PressureBenchmarkArtifact.Implementation.BASELINE
					? prepared.baselineSha() : prepared.candidateSha();
			long started = priorFinish + 1L;
			long finished = started + 1L;
			var artifact = new PressureBenchmarkArtifact.Artifact(
					run.suite(), run.round(), run.ordinal(), run.implementation(), build,
					prepared.configurationSha256(), "1".repeat(64), "test-host", "2".repeat(64),
					(run.implementation() == PressureBenchmarkArtifact.Implementation.BASELINE
							? "3" : "4").repeat(64),
					(run.implementation() == PressureBenchmarkArtifact.Implementation.BASELINE
							? "5" : "6").repeat(64),
					10_000L + run.ordinal(), started, finished, true, true, Map.copyOf(metrics));
			PressureBenchmarkArtifact.write(run.artifact(), artifact, names);
			priorFinish = finished;
		}
	}

	private ComponentRoots componentRoots(String name, boolean equal) throws Exception {
		Path baseline = temporary.resolve(name + "-baseline");
		Path candidate = temporary.resolve(name + "-candidate");
		Path relative = Path.of("it/cavallium/rockserver/core/impl/StoragePressureSignal.class");
		Files.createDirectories(baseline.resolve(relative).getParent());
		Files.createDirectories(candidate.resolve(relative).getParent());
		Files.write(baseline.resolve(relative), new byte[] {1, 2, 3, 4});
		Files.write(candidate.resolve(relative), equal
				? new byte[] {1, 2, 3, 4} : new byte[] {1, 2, 3, 5});
		return new ComponentRoots(baseline, candidate);
	}

	private static long nextConfigurationValue(String properties, String key) {
		return properties.lines().filter(line -> line.startsWith(key + '='))
				.map(line -> line.substring(key.length() + 1))
				.mapToLong(Long::parseLong).findFirst().orElseThrow();
	}

	private record ComponentRoots(Path baseline, Path candidate) {
	}
}
