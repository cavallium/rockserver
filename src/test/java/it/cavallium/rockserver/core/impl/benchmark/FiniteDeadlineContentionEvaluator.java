package it.cavallium.rockserver.core.impl.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Fixed, multiplicity-controlled evaluator for the finite-LATENCY contention schedule. */
public final class FiniteDeadlineContentionEvaluator {

	private static final int PAIRS = PairedPerformanceContractV2.REQUIRED_PAIRS;
	private static final List<Metric> METRICS = List.of(
			new Metric("attempts-throughput", "attempts_per_second", true),
			new Metric("useful-throughput", "useful_runs_per_second", true),
			new Metric("latency-throughput", null, true),
			new Metric("latency-queue-p99", "profile.latency.queue_p99_nanos", false),
			new Metric("latency-end-to-end-p99", "profile.latency.end_to_end_p99_nanos", false),
			new Metric("latency-maximum-progress-gap",
					"profile.latency.maximum_progress_gap_nanos", false),
			new Metric("cpu-per-attempt", "process.cpu_nanos_per_attempt", false),
			new Metric("allocation-per-attempt", "process.allocated_bytes_per_attempt", false));

	private FiniteDeadlineContentionEvaluator() {
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			throw new IllegalArgumentException("Expected one finite-deadline benchmark root");
		}
		Path root = Path.of(args[0]).toAbsolutePath().normalize();
		var result = evaluate(root);
		write(root.resolve("deadline-results-v2.json"), json(result));
		write(root.resolve("deadline-results-v2.md"), markdown(result));
		System.out.println("finite-deadline-contention-decision="
				+ result.evaluation().decision().name().toLowerCase(Locale.ROOT));
	}

	static Result evaluate(Path root) throws IOException {
		Objects.requireNonNull(root, "root");
		var metadata = strictProperties(root.resolve("metadata.properties"));
		require(metadata, "contract-version", "v2");
		require(metadata, "scheduler-operations", "1000000");
		require(metadata, "scheduler-submitters", "64");
		require(metadata, "scheduler-read-workers", "8");
		require(metadata, "scheduler-write-workers", "8");
		require(metadata, "scheduler-foreground-capacity", "65536");
		require(metadata, "scheduler-batch-capacity", "65536");
		require(metadata, "scheduler-seed", "104372305701837");
		var samples = new LinkedHashMap<String, PairedPerformanceContractV2.MetricSamples>();
		var baseline = new LinkedHashMap<String, double[]>();
		var candidate = new LinkedHashMap<String, double[]>();
		for (var metric : METRICS) {
			baseline.put(metric.name(), new double[PAIRS]);
			candidate.put(metric.name(), new double[PAIRS]);
		}
		var structuralFailures = new ArrayList<String>();
		for (int round = 1; round <= PAIRS; round++) {
			for (String implementation : List.of("baseline", "candidate")) {
				Path worker = root.resolve("raw-scheduler/round-%02d-%s.properties"
						.formatted(round, implementation));
				Map<String, String> values;
				try {
					values = strictProperties(worker);
					validateWorker(values);
				} catch (RuntimeException | IOException invalid) {
					structuralFailures.add(worker.getFileName() + ": " + invalid.getMessage());
					continue;
				}
				var target = implementation.equals("baseline") ? baseline : candidate;
				for (var metric : METRICS) {
					double value = metric.property() == null
							? number(values, "profile.latency.runs") * 1_000_000_000.0d
							/ number(values, "elapsed_nanos")
							: number(values, metric.property());
					target.get(metric.name())[round - 1] = value;
				}
			}
		}
		var specifications = new ArrayList<PairedPerformanceContractV2.MetricSpec>();
		for (var metric : METRICS) {
			specifications.add(metric.higherIsBetter()
					? PairedPerformanceContractV2.MetricSpec.throughput(metric.name(), false)
					: PairedPerformanceContractV2.MetricSpec.cost(metric.name(), false));
			samples.put(metric.name(), new PairedPerformanceContractV2.MetricSamples(
					baseline.get(metric.name()), candidate.get(metric.name())));
		}
		return new Result(metadata.get("baseline-sha"),
				metadata.get("candidate-sha"),
				PairedPerformanceContractV2.evaluate(specifications, samples, structuralFailures));
	}

	private static void validateWorker(Map<String, String> values) {
		require(values, "schema", "rockserver-scheduler-high-contention-v2");
		require(values, "operations", "1000000");
		require(values, "seed", "104372305701837");
		require(values, "submitters", "64");
		require(values, "latency_finite_deadlines", "true");
		long operations = integer(values, "operations");
		long terminal = integer(values, "outcome.run")
				+ integer(values, "outcome.failure")
				+ integer(values, "outcome.deadline")
				+ integer(values, "outcome.cancellation")
				+ integer(values, "outcome.overload")
				+ integer(values, "outcome.shutdown");
		if (terminal != operations || integer(values, "outcome.shutdown") != 0L
				|| integer(values, "profile.latency.runs") <= 0L) {
			throw new IllegalArgumentException("worker conservation/progress gate failed");
		}
	}

	private static Map<String, String> strictProperties(Path path) throws IOException {
		if (!Files.isRegularFile(path)) throw new IllegalArgumentException("missing file " + path);
		var values = new LinkedHashMap<String, String>();
		String[] lines = Files.readString(path).split("\\R", -1);
		for (int index = 0; index < lines.length; index++) {
			String line = lines[index];
			if (line.isEmpty() && index == lines.length - 1) continue;
			int separator = line.indexOf('=');
			if (separator <= 0 || separator != line.lastIndexOf('=')) {
				throw new IllegalArgumentException("malformed property at line " + (index + 1));
			}
			String key = line.substring(0, separator);
			String value = line.substring(separator + 1);
			if (value.isEmpty() || values.put(key, value) != null) {
				throw new IllegalArgumentException("empty or duplicate property " + key);
			}
		}
		return Map.copyOf(values);
	}

	private static void require(Map<String, String> values, String key, String expected) {
		if (!expected.equals(values.get(key))) {
			throw new IllegalArgumentException("expected " + key + '=' + expected);
		}
	}

	private static double number(Map<String, String> values, String key) {
		try {
			double value = Double.parseDouble(Objects.requireNonNull(values.get(key), "missing " + key));
			if (!Double.isFinite(value) || value <= 0.0d) throw new NumberFormatException();
			return value;
		} catch (NumberFormatException invalid) {
			throw new IllegalArgumentException("invalid positive number " + key, invalid);
		}
	}

	private static long integer(Map<String, String> values, String key) {
		try {
			return Long.parseLong(Objects.requireNonNull(values.get(key), "missing " + key));
		} catch (NumberFormatException invalid) {
			throw new IllegalArgumentException("invalid integer " + key, invalid);
		}
	}

	private static String markdown(Result result) {
		var out = new StringBuilder("# Finite-deadline contention v2 evaluation\n\n");
		out.append("- Decision: **").append(result.evaluation().decision()).append("**\n")
				.append("- Baseline: `").append(result.baselineSha()).append("`\n")
				.append("- Candidate: `").append(result.candidateSha()).append("`\n")
				.append("- Fixed counterbalanced pairs: `10` (`20` fresh JVM workers)\n")
				.append("- Contract: throughput minimum ratio `0.99`, cost maximum ratio `1.02`, ")
				.append("Holm family-wise alpha `0.05`, no adaptive stopping\n\n")
				.append("| Metric | Ratio | 95% CI | Regression Holm p | NI proven |\n")
				.append("|---|---:|---:|---:|---:|\n");
		for (var metric : METRICS) {
			var value = result.evaluation().metrics().get(metric.name());
			var interval = value.interval();
			out.append("| ").append(metric.name()).append(" | ")
					.append(format(interval.mean())).append(" | [")
					.append(format(interval.lower95())).append(", ")
					.append(format(interval.upper95())).append("] | ")
					.append(format(value.regressionHolmAdjustedPValue())).append(" | ")
					.append(value.nonInferiorityProven()).append(" |\n");
		}
		if (!result.evaluation().failures().isEmpty()) {
			out.append("\nFailures:\n");
			for (String failure : result.evaluation().failures()) out.append("- ").append(failure).append('\n');
		}
		return out.toString();
	}

	private static String json(Result result) {
		var out = new StringBuilder("{\n  \"schema\": \"rockserver-finite-deadline-contention-v2\",");
		out.append("\n  \"generated_at\": \"").append(Instant.now()).append("\",")
				.append("\n  \"baseline_sha\": \"").append(result.baselineSha()).append("\",")
				.append("\n  \"candidate_sha\": \"").append(result.candidateSha()).append("\",")
				.append("\n  \"decision\": \"").append(result.evaluation().decision()).append("\",")
				.append("\n  \"metrics\": {\n");
		for (int index = 0; index < METRICS.size(); index++) {
			var metric = METRICS.get(index);
			var value = result.evaluation().metrics().get(metric.name());
			var interval = value.interval();
			out.append("    \"").append(metric.name()).append("\": {\"ratio\": ")
					.append(interval.mean()).append(", \"lower95\": ").append(interval.lower95())
					.append(", \"upper95\": ").append(interval.upper95())
					.append(", \"regression_holm_p\": ").append(value.regressionHolmAdjustedPValue())
					.append(", \"non_inferiority_proven\": ").append(value.nonInferiorityProven()).append('}')
					.append(index + 1 == METRICS.size() ? '\n' : ",\n");
		}
		return out.append("  }\n}\n").toString();
	}

	private static void write(Path path, String value) throws IOException {
		Files.writeString(path, value, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
	}

	private static String format(double value) {
		return String.format(Locale.ROOT, "%.6f", value);
	}

	private record Metric(String name, String property, boolean higherIsBetter) {
	}

	record Result(String baselineSha,
			String candidateSha,
			PairedPerformanceContractV2.Evaluation evaluation) {
	}
}
