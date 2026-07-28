package it.cavallium.rockserver.core.impl.benchmark;

import it.cavallium.rockserver.core.common.WorkloadProfile;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/** Command-line, machine-readable selection over completed seven-profile candidate runs. */
public final class WorkloadBenchmarkSelection {

	public static final String INPUT_SCHEMA = "rockserver-seven-profile-selection-input-v3";
	public static final String OUTPUT_SCHEMA = "rockserver-seven-profile-selection-v3";

	private WorkloadBenchmarkSelection() {
	}

	public static void main(String[] args) throws Exception {
		var inputs = new ArrayList<Path>();
		Path output = Path.of("workload-selection.json");
		for (String argument : args) {
			if (argument.startsWith("--output=")) {
				output = Path.of(argument.substring("--output=".length()));
			} else if (argument.equals("--help")) {
				printUsage();
				return;
			} else {
				inputs.add(Path.of(argument));
			}
		}
		if (inputs.isEmpty()) {
			throw new IllegalArgumentException("Pass one selection-input.properties file per candidate");
		}
		var measurements = new ArrayList<WorkloadBenchmarkSelector.CandidateMeasurement>();
		for (Path input : inputs) {
			measurements.add(readSelectionInput(input));
		}
		var selection = WorkloadBenchmarkSelector.select(measurements);
		String json = toJson(selection);
		Files.writeString(output, json, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
		System.out.print(json);
		System.out.println("Selection written to " + output.toAbsolutePath().normalize());
	}

	public static void writeSelectionInput(Path output,
			WorkloadBenchmarkSelector.CandidateMeasurement measurement) throws IOException {
		var lines = new ArrayList<String>();
		lines.add("schema=" + INPUT_SCHEMA);
		lines.add("candidate=" + measurement.candidate());
		lines.add("dataset-fingerprint=" + measurement.datasetFingerprint());
		lines.add("comparison-fingerprint=" + measurement.comparisonFingerprint());
		lines.add("build-id=" + measurement.buildId());
		lines.add("storage-label=" + measurement.storageLabel());
		lines.add("seed=" + measurement.seed());
		lines.add("enforced-hardware-run=" + measurement.enforcedHardwareRun());
		lines.add("run-checks-passed=" + measurement.runChecksPassed());
		lines.add("maximum-cdc-lag=" + measurement.maximumCdcLag());
		lines.add("maximum-retained-snapshots=" + measurement.maximumRetainedSnapshots());
		lines.add("maximum-storage-pressure=" + measurement.maximumStoragePressure());
		lines.add("leaked-resources=" + measurement.leakedResources());
		for (var profile : WorkloadBenchmarkSelector.ALL_PROFILES) {
			String prefix = "profile." + metricName(profile) + ".";
			var value = measurement.profiles().get(profile);
			lines.add(prefix + "throughput=" + Double.toString(value.throughput()));
			lines.add(prefix + "queue-p99-nanos=" + value.queueP99Nanos());
			lines.add(prefix + "execution-p99-nanos=" + value.executionP99Nanos());
			lines.add(prefix + "end-to-end-p99-nanos=" + value.endToEndP99Nanos());
			lines.add(prefix + "rejections=" + value.rejections());
			lines.add(prefix + "cancellations=" + value.cancellations());
			lines.add(prefix + "quantum-count=" + value.quantumCount());
			lines.add(prefix + "relevant-p99=" + value.relevantP99());
			lines.add(prefix + "slo-passed=" + value.sloPassed());
		}
		Files.write(output, lines, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
	}

	public static WorkloadBenchmarkSelector.CandidateMeasurement readSelectionInput(Path input) throws IOException {
		var properties = new Properties();
		try (Reader reader = Files.newBufferedReader(input)) {
			properties.load(reader);
		}
		if (!INPUT_SCHEMA.equals(required(properties, "schema"))) {
			throw new IllegalArgumentException("Unsupported selection input schema in " + input);
		}
		var profiles = new EnumMap<WorkloadProfile, WorkloadBenchmarkSelector.ProfileMeasurement>(WorkloadProfile.class);
		for (var profile : WorkloadBenchmarkSelector.ALL_PROFILES) {
			String prefix = "profile." + metricName(profile) + ".";
			profiles.put(profile, new WorkloadBenchmarkSelector.ProfileMeasurement(
					Double.parseDouble(required(properties, prefix + "throughput")),
					Long.parseLong(required(properties, prefix + "queue-p99-nanos")),
					Long.parseLong(required(properties, prefix + "execution-p99-nanos")),
					Long.parseLong(required(properties, prefix + "end-to-end-p99-nanos")),
					Long.parseLong(required(properties, prefix + "rejections")),
					Long.parseLong(required(properties, prefix + "cancellations")),
					Long.parseLong(required(properties, prefix + "quantum-count")),
					requiredBoolean(properties, prefix + "relevant-p99"),
					requiredBoolean(properties, prefix + "slo-passed")));
		}
		return new WorkloadBenchmarkSelector.CandidateMeasurement(
				Integer.parseInt(required(properties, "candidate")),
				required(properties, "dataset-fingerprint"),
				required(properties, "comparison-fingerprint"),
				required(properties, "build-id"),
				required(properties, "storage-label"),
				Long.parseLong(required(properties, "seed")),
				requiredBoolean(properties, "enforced-hardware-run"),
				requiredBoolean(properties, "run-checks-passed"),
				profiles,
				Long.parseLong(required(properties, "maximum-cdc-lag")),
				Long.parseLong(required(properties, "maximum-retained-snapshots")),
				Long.parseLong(required(properties, "maximum-storage-pressure")),
				Long.parseLong(required(properties, "leaked-resources")));
	}

	public static String toJson(WorkloadBenchmarkSelector.Selection selection) {
		var json = new StringBuilder(2_048);
		json.append("{\n  \"schema\": ");
		appendJsonString(json, OUTPUT_SCHEMA);
		json.append(",\n  \"dataset_fingerprint\": ");
		appendJsonString(json, selection.datasetFingerprint());
		json.append(",\n  \"comparison_fingerprint\": ");
		appendJsonString(json, selection.comparisonFingerprint());
		json.append(",\n  \"build_id\": ");
		appendJsonString(json, selection.buildId());
		json.append(",\n  \"storage_label\": ");
		appendJsonString(json, selection.storageLabel());
		json.append(",\n  \"seed\": ").append(selection.seed()).append(',')
				.append("\n  \"winner\": ").append(selection.winner()).append(',')
				.append("\n  \"maximum_throughput\": ").append(format(selection.maximumThroughput())).append(',')
				.append("\n  \"minimum_relevant_p99_nanos\": ")
				.append(selection.minimumRelevantP99Nanos()).append(',')
				.append("\n  \"adjacent_verification_candidates\": ").append(selection.adjacentCandidates()).append(',')
				.append("\n  \"evaluations\": [");
		for (int index = 0; index < selection.evaluations().size(); index++) {
			var evaluation = selection.evaluations().get(index);
			if (index > 0) {
				json.append(',');
			}
			json.append("\n    {\"candidate\": ").append(evaluation.candidate())
					.append(", \"throughput\": ").append(format(evaluation.throughput()))
					.append(", \"relevant_p99_nanos\": ").append(evaluation.relevantP99Nanos())
					.append(", \"throughput_passed\": ").append(evaluation.throughputPassed())
					.append(", \"p99_passed\": ").append(evaluation.p99Passed())
					.append(", \"slos_passed\": ").append(evaluation.slosPassed())
					.append(", \"leaks_passed\": ").append(evaluation.leaksPassed())
					.append(", \"run_checks_passed\": ").append(evaluation.runChecksPassed())
					.append(", \"eligible\": ").append(evaluation.eligible()).append('}');
		}
		json.append("\n  ]\n}\n");
		return json.toString();
	}

	private static String required(Properties properties, String key) {
		String value = properties.getProperty(key);
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Missing property: " + key);
		}
		return value;
	}

	private static boolean requiredBoolean(Properties properties, String key) {
		String value = required(properties, key);
		if (value.equalsIgnoreCase("true")) {
			return true;
		}
		if (value.equalsIgnoreCase("false")) {
			return false;
		}
		throw new IllegalArgumentException("Property " + key + " must be true or false");
	}

	private static String metricName(WorkloadProfile profile) {
		return profile.name().toLowerCase(Locale.ROOT);
	}

	private static String format(double value) {
		return String.format(Locale.ROOT, "%.6f", value);
	}

	private static void appendJsonString(StringBuilder json, String value) {
		json.append('"');
		for (int index = 0; index < value.length(); index++) {
			char character = value.charAt(index);
			switch (character) {
				case '"' -> json.append("\\\"");
				case '\\' -> json.append("\\\\");
				case '\n' -> json.append("\\n");
				case '\r' -> json.append("\\r");
				case '\t' -> json.append("\\t");
				default -> json.append(character);
			}
		}
		json.append('"');
	}

	private static void printUsage() {
		System.out.println("Usage: WorkloadBenchmarkSelection [--output=selection.json] "
				+ "candidate-*/selection-input.properties...");
	}
}
