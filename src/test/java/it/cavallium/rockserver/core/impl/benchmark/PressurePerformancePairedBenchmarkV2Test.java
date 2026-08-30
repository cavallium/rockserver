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

class PressurePerformancePairedBenchmarkV2Test {

	@TempDir
	Path temporary;

	@Test
	void v1AndV2PrepareDistinctImmutableSchedulesAndConfigurationFingerprints() throws Exception {
		Path v1Root = temporary.resolve("v1");
		Path v2Root = temporary.resolve("v2");
		prepare(v1Root, "v1", false);
		prepare(v2Root, "v2", false);

		var v1 = PressurePerformancePairedBenchmark.Prepared.read(v1Root);
		var v2 = PressurePerformancePairedBenchmark.Prepared.read(v2Root);
		assertEquals(PressurePerformancePairedBenchmark.ContractVersion.V1, v1.contractVersion());
		assertEquals(PressurePerformancePairedBenchmark.ContractVersion.V2, v2.contractVersion());
		assertNotEquals(v1.configurationSha256(), v2.configurationSha256(),
				"v1 worker artifacts must not be reusable in a v2 decision");
		assertTrue(Files.readString(v1Root.resolve("schedule.tsv"))
				.startsWith("schema\trockserver-pressure-performance-schedule-v1\n"));
		assertTrue(Files.readString(v2Root.resolve("schedule.tsv"))
				.startsWith("schema\trockserver-pressure-performance-schedule-v2\n"));
		assertTrue(Files.readString(v2Root.resolve("metadata.properties"))
				.startsWith("schema=rockserver-pressure-performance-metadata-v2\ncontract-version=v2\n"));
	}

	@Test
	void freshV2EqualityArtifactsPassWithoutAMaterialImprovement() throws Exception {
		Path root = temporary.resolve("v2-pass");
		prepare(root, "v2", true);
		writeSyntheticArtifacts(root, false);

		PressurePerformancePairedBenchmark.main(new String[] {"--mode=evaluate", "--root=" + root});

		String json = Files.readString(root.resolve("results.json"));
		String markdown = Files.readString(root.resolve("results.md"));
		assertTrue(json.contains("\"schema\": \"rockserver-pressure-performance-comparison-v2\""));
		assertTrue(json.contains("\"decision\": \"pass\""));
		assertTrue(json.contains("\"material_improvements\":[]"));
		assertTrue(json.contains("\"family_wise_alpha\": 0.05"));
		assertTrue(markdown.contains("Overall decision: **PASS**"));
	}

	@Test
	void impreciseV2EvidenceIsPersistedAsInconclusiveAndCannotAutomaticallyPass() throws Exception {
		Path root = temporary.resolve("v2-inconclusive");
		prepare(root, "v2", true);
		writeSyntheticArtifacts(root, true);

		var failure = assertThrows(IllegalStateException.class,
				() -> PressurePerformancePairedBenchmark.main(
						new String[] {"--mode=evaluate", "--root=" + root}));
		assertTrue(failure.getMessage().contains("inconclusive"));
		String json = Files.readString(root.resolve("results.json"));
		assertTrue(json.contains("\"decision\": \"inconclusive\""));
		assertTrue(json.contains("scheduler.cpu_nanos_per_attempt"));
		assertTrue(json.contains("\"passed\": false"));
	}

	@Test
	void v2MetadataCannotBeDowngradedOrPreparedOverAnExistingV1Root() throws Exception {
		Path root = temporary.resolve("immutable-root");
		prepare(root, "v1", false);
		assertThrows(IllegalArgumentException.class, () -> prepare(root, "v2", false));

		Path malformed = temporary.resolve("malformed-v2");
		prepare(malformed, "v2", false);
		String metadata = Files.readString(malformed.resolve("metadata.properties"))
				.replace("contract-version=v2\n", "");
		Files.writeString(malformed.resolve("metadata.properties"), metadata);
		assertThrows(IllegalArgumentException.class,
				() -> PressurePerformancePairedBenchmark.Prepared.read(malformed));
	}

	private static void prepare(Path root, String version, boolean enforce) throws Exception {
		PressurePerformancePairedBenchmark.main(new String[] {
				"--mode=prepare", "--root=" + root, "--contract-version=" + version,
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

	private static void writeSyntheticArtifacts(Path root, boolean imprecise) throws Exception {
		var prepared = PressurePerformancePairedBenchmark.Prepared.read(root);
		long previousFinish = 1_000L;
		for (var run : PressurePerformancePairedBenchmark.schedule(prepared)) {
			var names = PressurePerformancePairedBenchmark.metricNames(
					run.suite(), prepared.signalColumnFamilyCounts());
			var metrics = new LinkedHashMap<String, Double>();
			for (String name : names) metrics.put(name, 100.0d);
			if (imprecise
					&& run.suite() == PressureBenchmarkArtifact.Suite.SCHEDULER
					&& run.implementation() == PressureBenchmarkArtifact.Implementation.CANDIDATE) {
				metrics.put("scheduler.cpu_nanos_per_attempt",
						(run.round() & 1) == 1 ? 95.0d : 100.0d / 0.95d);
			}
			String build = run.implementation() == PressureBenchmarkArtifact.Implementation.BASELINE
					? prepared.baselineSha() : prepared.candidateSha();
			long started = previousFinish + 1L;
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
			previousFinish = finished;
		}
	}
}
