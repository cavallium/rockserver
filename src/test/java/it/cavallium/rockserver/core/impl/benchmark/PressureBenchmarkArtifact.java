package it.cavallium.rockserver.core.impl.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Strict immutable worker artifact for one fresh-JVM pressure benchmark measurement. */
final class PressureBenchmarkArtifact {

	static final String SCHEMA = "rockserver-pressure-performance-worker-v1";
	private static final String METRIC_PREFIX = "metric.";
	private static final Set<String> FIXED_KEYS = Set.of(
			"schema", "suite", "round", "ordinal", "implementation", "build-sha",
			"configuration-sha256", "host-sha256", "hardware-description",
			"runtime-sha256", "classpath-sha256", "production-classes-sha256",
			"process-id", "started-epoch-millis", "finished-epoch-millis",
			"enforced-hardware-run", "correctness-passed");

	private PressureBenchmarkArtifact() {
	}

	enum Suite {
		SCHEDULER("scheduler"), SIGNAL("signal");

		final String value;

		Suite(String value) {
			this.value = value;
		}

		static Suite parse(String value) {
			for (var suite : values()) if (suite.value.equals(value)) return suite;
			throw new IllegalArgumentException("Unknown pressure benchmark suite " + value);
		}
	}

	enum Implementation {
		BASELINE("baseline"), CANDIDATE("candidate");

		final String value;

		Implementation(String value) {
			this.value = value;
		}

		static Implementation parse(String value) {
			for (var implementation : values()) {
				if (implementation.value.equals(value)) return implementation;
			}
			throw new IllegalArgumentException("Unknown pressure benchmark implementation " + value);
		}
	}

	record Artifact(Suite suite,
			int round,
			int ordinal,
			Implementation implementation,
			String buildSha,
			String configurationSha256,
			String hostSha256,
			String hardwareDescription,
			String runtimeSha256,
			String classpathSha256,
			String productionClassesSha256,
			long processId,
			long startedEpochMillis,
			long finishedEpochMillis,
			boolean enforcedHardwareRun,
			boolean correctnessPassed,
			Map<String, Double> metrics) {

		Artifact {
			if (round < 1 || round > PairedPerformanceContract.REQUIRED_PAIRS || ordinal < 1) {
				throw new IllegalArgumentException("round and ordinal must be in the prepared schedule");
			}
			if (suite == null || implementation == null) {
				throw new IllegalArgumentException("suite and implementation are required");
			}
			sha("build-sha", buildSha, 40);
			sha("configuration-sha256", configurationSha256, 64);
			sha("host-sha256", hostSha256, 64);
			sha("runtime-sha256", runtimeSha256, 64);
			sha("classpath-sha256", classpathSha256, 64);
			sha("production-classes-sha256", productionClassesSha256, 64);
			canonical("hardware-description", hardwareDescription);
			if (processId <= 0L || startedEpochMillis <= 0L || finishedEpochMillis < startedEpochMillis) {
				throw new IllegalArgumentException("process identity and timestamps must be positive and ordered");
			}
			var copy = new LinkedHashMap<String, Double>();
			for (var entry : metrics.entrySet()) {
				canonical("metric name", entry.getKey());
				double value = entry.getValue();
				if (!Double.isFinite(value) || value < 0.0d) {
					throw new IllegalArgumentException("metric must be finite and non-negative: " + entry.getKey());
				}
				copy.put(entry.getKey(), value);
			}
			metrics = Map.copyOf(copy);
		}
	}

	static void write(Path output, Artifact artifact, Set<String> expectedMetrics) throws IOException {
		Files.createDirectories(output.toAbsolutePath().normalize().getParent());
		Files.writeString(output, encode(artifact, expectedMetrics), StandardOpenOption.CREATE_NEW);
	}

	static String encode(Artifact artifact, Set<String> expectedMetrics) {
		validateMetricSet(artifact.metrics(), expectedMetrics);
		var lines = new StringBuilder();
		property(lines, "schema", SCHEMA);
		property(lines, "suite", artifact.suite().value);
		property(lines, "round", artifact.round());
		property(lines, "ordinal", artifact.ordinal());
		property(lines, "implementation", artifact.implementation().value);
		property(lines, "build-sha", artifact.buildSha());
		property(lines, "configuration-sha256", artifact.configurationSha256());
		property(lines, "host-sha256", artifact.hostSha256());
		property(lines, "hardware-description", artifact.hardwareDescription());
		property(lines, "runtime-sha256", artifact.runtimeSha256());
		property(lines, "classpath-sha256", artifact.classpathSha256());
		property(lines, "production-classes-sha256", artifact.productionClassesSha256());
		property(lines, "process-id", artifact.processId());
		property(lines, "started-epoch-millis", artifact.startedEpochMillis());
		property(lines, "finished-epoch-millis", artifact.finishedEpochMillis());
		property(lines, "enforced-hardware-run", artifact.enforcedHardwareRun());
		property(lines, "correctness-passed", artifact.correctnessPassed());
		expectedMetrics.stream().sorted().forEach(metric ->
				property(lines, METRIC_PREFIX + metric, artifact.metrics().get(metric)));
		return lines.toString();
	}

	static Artifact read(Path input, Suite expectedSuite, Set<String> expectedMetrics) throws IOException {
		if (!Files.isRegularFile(input)) {
			throw new IllegalArgumentException("Missing pressure benchmark artifact: " + input);
		}
		Map<String, String> values = strictProperties(Files.readString(input), expectedMetrics);
		require(values, "schema", SCHEMA);
		require(values, "suite", expectedSuite.value);
		var metrics = new LinkedHashMap<String, Double>();
		for (String metric : expectedMetrics) {
			metrics.put(metric, decimal(values, METRIC_PREFIX + metric));
		}
		return new Artifact(expectedSuite,
				integer(values, "round"),
				integer(values, "ordinal"),
				Implementation.parse(values.get("implementation")),
				values.get("build-sha"),
				values.get("configuration-sha256"),
				values.get("host-sha256"),
				values.get("hardware-description"),
				values.get("runtime-sha256"),
				values.get("classpath-sha256"),
				values.get("production-classes-sha256"),
				number(values, "process-id"),
				number(values, "started-epoch-millis"),
				number(values, "finished-epoch-millis"),
				bool(values, "enforced-hardware-run"),
				bool(values, "correctness-passed"),
				metrics);
	}

	private static Map<String, String> strictProperties(String text, Set<String> expectedMetrics) {
		var expected = new LinkedHashSet<>(FIXED_KEYS);
		for (String metric : expectedMetrics) expected.add(METRIC_PREFIX + metric);
		var values = new LinkedHashMap<String, String>();
		String[] lines = text.split("\\R", -1);
		for (int index = 0; index < lines.length; index++) {
			String line = lines[index];
			if (line.isEmpty() && index == lines.length - 1) continue;
			if (line.isBlank() || line.startsWith("#") || line.startsWith("!")) {
				throw new IllegalArgumentException("Blank lines and comments are forbidden at line " + (index + 1));
			}
			int separator = line.indexOf('=');
			if (separator <= 0 || separator != line.lastIndexOf('=')) {
				throw new IllegalArgumentException("Malformed pressure worker property at line " + (index + 1));
			}
			String key = line.substring(0, separator);
			String value = line.substring(separator + 1);
			if (!expected.contains(key)) throw new IllegalArgumentException("Unknown pressure worker property " + key);
			if (value.isEmpty()) throw new IllegalArgumentException("Empty pressure worker property " + key);
			if (values.put(key, value) != null) throw new IllegalArgumentException("Duplicate pressure worker property " + key);
		}
		var missing = new LinkedHashSet<>(expected);
		missing.removeAll(values.keySet());
		if (!missing.isEmpty()) throw new IllegalArgumentException("Missing pressure worker properties " + missing);
		return Map.copyOf(values);
	}

	private static void validateMetricSet(Map<String, Double> metrics, Set<String> expected) {
		if (!metrics.keySet().equals(expected)) {
			throw new IllegalArgumentException("Pressure metric set mismatch: expected="
					+ expected + " actual=" + metrics.keySet());
		}
	}

	private static void property(StringBuilder target, String key, Object value) {
		target.append(key).append('=').append(value).append('\n');
	}

	private static void require(Map<String, String> values, String key, String expected) {
		if (!expected.equals(values.get(key))) {
			throw new IllegalArgumentException("Pressure worker mismatch for " + key);
		}
	}

	private static int integer(Map<String, String> values, String key) {
		try {
			return Integer.parseInt(values.get(key));
		} catch (RuntimeException failure) {
			throw new IllegalArgumentException("Invalid integer pressure worker property " + key, failure);
		}
	}

	private static long number(Map<String, String> values, String key) {
		try {
			return Long.parseLong(values.get(key));
		} catch (RuntimeException failure) {
			throw new IllegalArgumentException("Invalid long pressure worker property " + key, failure);
		}
	}

	private static double decimal(Map<String, String> values, String key) {
		try {
			double value = Double.parseDouble(values.get(key));
			if (!Double.isFinite(value) || value < 0.0d) throw new NumberFormatException("invalid metric");
			return value;
		} catch (RuntimeException failure) {
			throw new IllegalArgumentException("Invalid decimal pressure worker property " + key, failure);
		}
	}

	private static boolean bool(Map<String, String> values, String key) {
		String value = values.get(key);
		if (!value.equals("true") && !value.equals("false")) {
			throw new IllegalArgumentException("Invalid boolean pressure worker property " + key);
		}
		return Boolean.parseBoolean(value);
	}

	private static void sha(String name, String value, int length) {
		if (value == null || !value.matches("[0-9a-f]{" + length + "}")) {
			throw new IllegalArgumentException(name + " must be a lowercase hexadecimal fingerprint");
		}
	}

	private static void canonical(String name, String value) {
		if (value == null || value.isBlank() || !value.equals(value.strip())
				|| value.indexOf('=') >= 0 || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0) {
			throw new IllegalArgumentException(name + " must be a canonical single-line value");
		}
	}
}
