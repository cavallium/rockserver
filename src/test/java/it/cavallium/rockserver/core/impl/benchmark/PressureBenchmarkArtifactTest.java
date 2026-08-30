package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PressureBenchmarkArtifactTest {

	@TempDir
	Path temporary;

	@Test
	void strictArtifactRoundTripsEveryMetricAndProvenanceField() throws Exception {
		Set<String> metrics = Set.of("throughput", "p99");
		var expected = artifact(Map.of("throughput", 123.5d, "p99", 456.0d));
		Path output = temporary.resolve("worker.properties");

		PressureBenchmarkArtifact.write(output, expected, metrics);

		assertEquals(expected, PressureBenchmarkArtifact.read(
				output, PressureBenchmarkArtifact.Suite.SCHEDULER, metrics));
		assertThrows(java.nio.file.FileAlreadyExistsException.class,
				() -> PressureBenchmarkArtifact.write(output, expected, metrics));
	}

	@Test
	void parserRejectsMissingDuplicateUnknownAndNonFiniteMetrics() throws Exception {
		Set<String> metrics = Set.of("throughput", "p99");
		String valid = PressureBenchmarkArtifact.encode(
				artifact(Map.of("throughput", 123.5d, "p99", 456.0d)), metrics);

		assertRejected(valid.replaceFirst("metric.p99=.*\\n", ""), metrics);
		assertRejected(valid + "metric.p99=1\n", metrics);
		assertRejected(valid + "unknown=1\n", metrics);
		assertRejected(valid.replace("metric.p99=456.0", "metric.p99=NaN"), metrics);
	}

	@Test
	void writerRejectsIncompleteMetricMapAndUnsafeProvenance() {
		assertThrows(IllegalArgumentException.class, () -> PressureBenchmarkArtifact.encode(
				artifact(Map.of("throughput", 1.0d)), Set.of("throughput", "p99")));
		var values = new LinkedHashMap<>(artifact(Map.of("throughput", 1.0d)).metrics());
		values.put("bad", -1.0d);
		assertThrows(IllegalArgumentException.class, () -> artifact(values));
	}

	private void assertRejected(String text, Set<String> metrics) throws Exception {
		Path input = temporary.resolve("invalid-" + Math.abs(text.hashCode()) + ".properties");
		Files.writeString(input, text);
		assertThrows(IllegalArgumentException.class, () -> PressureBenchmarkArtifact.read(
				input, PressureBenchmarkArtifact.Suite.SCHEDULER, metrics));
	}

	private static PressureBenchmarkArtifact.Artifact artifact(Map<String, Double> metrics) {
		return new PressureBenchmarkArtifact.Artifact(
				PressureBenchmarkArtifact.Suite.SCHEDULER,
				1,
				1,
				PressureBenchmarkArtifact.Implementation.BASELINE,
				"a".repeat(40),
				"b".repeat(64),
				"c".repeat(64),
				"test-host",
				"d".repeat(64),
				"e".repeat(64),
				"f".repeat(64),
				123L,
				1_000L,
				2_000L,
				true,
				true,
				metrics);
	}
}
