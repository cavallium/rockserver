package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PressurePerformancePairedBenchmarkPlannedTest {

	@TempDir
	Path temporary;

	@Test
	void existingV2DefaultsToTenWhilePlannedV21BindsFixedPairCountAndDurationCap() throws Exception {
		Path v2Root = temporary.resolve("v2");
		Path plannedRoot = temporary.resolve("v2-planned");
		prepare(v2Root, "v2", 10, 1);
		prepare(plannedRoot, "v2.1", 12, 64);

		var v2 = PressurePerformancePairedBenchmark.Prepared.read(v2Root);
		var planned = PressurePerformancePairedBenchmark.Prepared.read(plannedRoot);
		assertEquals(10, v2.fixedPairs());
		assertEquals(1, v2.planningDurationScaleCap());
		assertEquals(PressurePerformancePairedBenchmark.ContractVersion.V2_PLANNED,
				planned.contractVersion());
		assertEquals(12, planned.fixedPairs());
		assertEquals(64, planned.planningDurationScaleCap());
		assertEquals("c".repeat(64), planned.planningSourceConfigurationSha256());
		assertEquals("d".repeat(64), planned.planningSourceResultsSha256());
		assertNotEquals(v2.configurationSha256(), planned.configurationSha256());
		assertTrue(Files.readString(plannedRoot.resolve("schedule.tsv"))
				.startsWith("schema\trockserver-pressure-performance-schedule-v2.1\n"));
		assertEquals(48L, Files.readString(plannedRoot.resolve("schedule.tsv")).lines()
				.filter(line -> line.matches("\\d+\\t\\d+\\t.*")).count());
		assertTrue(Files.readString(plannedRoot.resolve("schedule.tsv"))
				.contains("45\t12\tscheduler\tcandidate"));
	}

	@Test
	void plannerSourceHashTamperingChangesConfigurationAndInvalidatesPreparedSchedule() throws Exception {
		Path root = temporary.resolve("source-hash-tamper");
		prepare(root, "v2.1", 12, 64);
		var original = PressurePerformancePairedBenchmark.Prepared.read(root);
		Files.writeString(root.resolve("metadata.properties"),
				Files.readString(root.resolve("metadata.properties"))
						.replace("planning-source-results-sha256=" + "d".repeat(64),
								"planning-source-results-sha256=" + "e".repeat(64)));
		var tampered = PressurePerformancePairedBenchmark.Prepared.read(root);
		assertNotEquals(original.configurationSha256(), tampered.configurationSha256());
		assertThrows(IllegalArgumentException.class,
				() -> PressurePerformancePairedBenchmark.main(new String[] {"--mode=evaluate", "--root=" + root}));
	}

	@Test
	void plannedEvidenceEvaluatesEveryPairAndRecordsVariableFreshProcessCount() throws Exception {
		Path root = temporary.resolve("evaluate-planned");
		prepare(root, "v2.1", 12, 64);
		writeSyntheticArtifacts(root);

		PressurePerformancePairedBenchmark.main(new String[] {"--mode=evaluate", "--root=" + root});

		String result = Files.readString(root.resolve("results.json"));
		assertTrue(result.contains("\"schema\": \"rockserver-pressure-performance-comparison-v2.1\""));
		assertTrue(result.contains("\"contract_version\": \"v2.1\""));
		assertTrue(result.contains("\"fixed_pairs\": 12"));
		assertTrue(result.contains("\"fresh_processes\": 48"));
		assertTrue(result.contains("\"decision\": \"pass\""));
	}

	@Test
	void plannedEvidenceRejectsMissingRoundTamperedOrdinalAndWrongWorkerSchema() throws Exception {
		Path missing = temporary.resolve("missing");
		prepare(missing, "v2.1", 12, 64);
		writeSyntheticArtifacts(missing);
		var prepared = PressurePerformancePairedBenchmark.Prepared.read(missing);
		var finalRun = PressurePerformancePairedBenchmark.schedule(prepared).getLast();
		Files.delete(finalRun.artifact());
		assertThrows(IllegalArgumentException.class,
				() -> PressurePerformancePairedBenchmark.main(new String[] {"--mode=evaluate", "--root=" + missing}));

		Path ordinal = temporary.resolve("ordinal");
		prepare(ordinal, "v2.1", 12, 64);
		writeSyntheticArtifacts(ordinal);
		var ordinalPrepared = PressurePerformancePairedBenchmark.Prepared.read(ordinal);
		var run = PressurePerformancePairedBenchmark.schedule(ordinalPrepared).getLast();
		Files.writeString(run.artifact(), Files.readString(run.artifact())
				.replace("ordinal=" + run.ordinal(), "ordinal=" + (run.ordinal() - 1)));
		assertThrows(IllegalStateException.class,
				() -> PressurePerformancePairedBenchmark.main(new String[] {"--mode=evaluate", "--root=" + ordinal}));

		Path schema = temporary.resolve("schema");
		prepare(schema, "v2.1", 12, 64);
		writeSyntheticArtifacts(schema);
		var schemaPrepared = PressurePerformancePairedBenchmark.Prepared.read(schema);
		var first = PressurePerformancePairedBenchmark.schedule(schemaPrepared).getFirst();
		Files.writeString(first.artifact(), Files.readString(first.artifact())
				.replace("schema=rockserver-pressure-performance-worker-v2",
						"schema=rockserver-pressure-performance-worker-v1"));
		assertThrows(IllegalArgumentException.class,
				() -> PressurePerformancePairedBenchmark.main(new String[] {"--mode=evaluate", "--root=" + schema}));
	}

	private static void prepare(Path root, String version, int pairs, int durationCap) throws Exception {
		var options = new java.util.ArrayList<String>();
		options.add("--mode=prepare");
		options.add("--root=" + root);
		options.add("--contract-version=" + version);
		if (version.equals("v2.1")) {
			options.add("--fixed-pairs=" + pairs);
			options.add("--planning-duration-scale-cap=" + durationCap);
			options.add("--planning-source-configuration-sha256=" + "c".repeat(64));
			options.add("--planning-source-results-sha256=" + "d".repeat(64));
		}
		options.addAll(java.util.List.of(
				"--baseline-sha=" + "a".repeat(40), "--candidate-sha=" + "b".repeat(40),
				"--host-state=dedicated", "--hardware-description=test-host", "--enforce=true",
				"--scheduler-operations=1024", "--scheduler-warmup-operations=512",
				"--scheduler-submitters=4", "--scheduler-read-workers=2", "--scheduler-write-workers=2",
				"--scheduler-analytical-limit=1", "--scheduler-foreground-capacity=128",
				"--scheduler-batch-capacity=128", "--scheduler-work-tokens=8",
				"--signal-cf-counts=1,64", "--signal-warmup-columns=1000",
				"--signal-measured-columns=2000", "--signal-minimum-evaluations=100",
				"--signal-maximum-evaluations=1000", "--signal-latency-sample-stride=16"));
		PressurePerformancePairedBenchmark.main(options.toArray(String[]::new));
	}

	private static void writeSyntheticArtifacts(Path root) throws Exception {
		var prepared = PressurePerformancePairedBenchmark.Prepared.read(root);
		long priorFinish = 1_000L;
		for (var run : PressurePerformancePairedBenchmark.schedule(prepared)) {
			var names = PressurePerformancePairedBenchmark.metricNames(
					run.suite(), prepared.signalColumnFamilyCounts());
			var metrics = new LinkedHashMap<String, Double>();
			for (String name : names) metrics.put(name, 100.0d);
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
			PressureBenchmarkArtifact.write(run.artifact(), artifact, names,
					PressureBenchmarkArtifact.SCHEMA_V2);
			priorFinish = finished;
		}
	}
}
