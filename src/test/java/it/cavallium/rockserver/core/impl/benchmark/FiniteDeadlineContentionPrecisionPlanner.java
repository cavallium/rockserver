package it.cavallium.rockserver.core.impl.benchmark;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Fixed-design precision planner using paired variance at exact equality. */
public final class FiniteDeadlineContentionPrecisionPlanner {

	static final String SCHEMA = "rockserver-finite-deadline-contention-precision-plan-v1";
	static final String NEXT_SCHEMA = "rockserver-finite-deadline-contention-next-run-v2";
	static final String JSON_FILE = "deadline-precision-plan-v1.json";
	static final String MARKDOWN_FILE = "deadline-precision-plan-v1.md";
	static final String NEXT_FILE = "deadline-next-run-v2.properties";
	static final double GLOBAL_PASS_POWER = 0.90d;
	static final double PER_METRIC_POWER = 1.0d - (1.0d - GLOBAL_PASS_POWER)
			/ FiniteDeadlineContentionBenchmark.METRICS.size();
	private static final Pattern EVALUATED_AT = Pattern.compile("\\\"evaluated_at\\\": \\\"([^\\\"]+)\\\"");
	private static final Set<String> NEXT_KEYS = Set.of(
			"schema", "source-root", "source-configuration-sha256", "source-results-sha256",
			"source-schedule-sha256", "source-metadata-sha256", "source-workers-sha256", "planning-effect",
			"adaptive-stopping", "family-wise-alpha", "throughput-minimum-ratio",
			"cost-maximum-ratio", "multiplicity", "baseline-sha", "candidate-sha",
			"baseline-worktree", "candidate-worktree", "baseline-classes", "candidate-classes",
			"hardware-description", "enforce", "pairs", "operations", "warmup-operations",
			"submitters", "read-workers", "write-workers", "analytical-limit",
			"foreground-capacity", "batch-capacity", "work-tokens", "cooperative-yields",
			"cooperative-parks", "expired-deadline-percent", "cancellation-percent",
			"failure-percent", "cooperative-percent", "alternate-storage-pressure", "seed",
			"timeout-seconds");

	private FiniteDeadlineContentionPrecisionPlanner() {}

	public static void main(String[] args) throws Exception {
		Map<String, String> arguments = arguments(args);
		if (!arguments.keySet().equals(Set.of("root"))) {
			throw new IllegalArgumentException("Expected only --root=<completed benchmark>");
		}
		plan(Path.of(arguments.get("root")).toAbsolutePath().normalize());
	}

	static Plan plan(Path root) throws IOException {
		for (String output : List.of(JSON_FILE, MARKDOWN_FILE, NEXT_FILE)) {
			if (Files.exists(root.resolve(output))) {
				throw new IllegalArgumentException("Precision output already exists: " + root.resolve(output));
			}
		}
		var prepared = FiniteDeadlineContentionBenchmark.Prepared.read(root);
		Path resultsPath = root.resolve(FiniteDeadlineContentionBenchmark.RESULTS_JSON);
		Path markdownPath = root.resolve(FiniteDeadlineContentionBenchmark.RESULTS_MARKDOWN);
		if (!Files.isRegularFile(resultsPath) || !Files.isRegularFile(markdownPath)) {
			throw new IllegalArgumentException("Planner requires a completed controller result");
		}
		String resultsText = Files.readString(resultsPath);
		var matcher = EVALUATED_AT.matcher(resultsText);
		if (!matcher.find()) throw new IllegalArgumentException("Result has no canonical evaluation timestamp");
		Instant evaluatedAt;
		try { evaluatedAt = Instant.parse(matcher.group(1)); }
		catch (RuntimeException invalid) { throw new IllegalArgumentException("Invalid result timestamp", invalid); }
		var inspected = FiniteDeadlineContentionBenchmark.inspect(prepared);
		if (inspected.evaluation().decision() != PairedPerformanceContractV2.Decision.INCONCLUSIVE
				|| !inspected.evaluation().failures().isEmpty()) {
			throw new IllegalArgumentException("Precision planning requires a complete INCONCLUSIVE result");
		}
		if (!resultsText.equals(FiniteDeadlineContentionBenchmark.resultJson(prepared, inspected, evaluatedAt))
				|| !Files.readString(markdownPath).equals(
						FiniteDeadlineContentionBenchmark.resultMarkdown(prepared, inspected))) {
			throw new IllegalArgumentException("Completed result was modified after evaluation");
		}

		var metricPlans = new ArrayList<MetricPlan>();
		int recommended = prepared.fixedPairs();
		for (var metric : FiniteDeadlineContentionBenchmark.METRICS) {
			var samples = inspected.samples().get(metric.name());
			double standardDeviation = pairedLogStandardDeviation(samples.baseline(), samples.candidate());
			double marginLog = metric.higherIsBetter()
					? -Math.log(PairedPerformanceContractV2.THROUGHPUT_MINIMUM_RATIO)
					: Math.log(PairedPerformanceContractV2.COST_MAXIMUM_RATIO);
			int required = PressurePerformancePrecisionPlanner.requiredPairs(
					standardDeviation, marginLog, PER_METRIC_POWER);
			recommended = Math.max(recommended, required);
			metricPlans.add(new MetricPlan(metric.name(), metric.higherIsBetter() ? "higher" : "lower",
					standardDeviation, marginLog, PER_METRIC_POWER, required));
		}
		metricPlans.sort(Comparator.comparingInt(MetricPlan::requiredFixedPairs).reversed()
				.thenComparing(MetricPlan::name));
		String resultsSha = sha256(resultsPath);
		String scheduleSha = sha256(root.resolve(FiniteDeadlineContentionBenchmark.SCHEDULE_FILE));
		String metadataSha = sha256(root.resolve(FiniteDeadlineContentionBenchmark.METADATA_FILE));
		String workersSha = workersSha256(prepared);
		var plan = new Plan(prepared.configurationSha256(), resultsSha, scheduleSha, metadataSha, workersSha,
				prepared.fixedPairs(), recommended, List.copyOf(metricPlans));
		Instant generatedAt = Instant.now();
		Files.writeString(root.resolve(JSON_FILE), json(plan, generatedAt), StandardOpenOption.CREATE_NEW);
		Files.writeString(root.resolve(MARKDOWN_FILE), markdown(plan), StandardOpenOption.CREATE_NEW);
		Files.writeString(root.resolve(NEXT_FILE), nextProperties(root, prepared, plan),
				StandardOpenOption.CREATE_NEW);
		return plan;
	}

	/** Expand one planner output into the exact ordinary prepare arguments. */
	static Map<String, String> expandPlannedPrepare(Map<String, String> arguments) throws IOException {
		if (!arguments.containsKey("next-run-properties")) return arguments;
		if (!arguments.keySet().equals(Set.of("mode", "root", "next-run-properties"))) {
			throw new IllegalArgumentException("Planned prepare accepts only mode, root, and next-run-properties");
		}
		Path planPath = Path.of(arguments.get("next-run-properties")).toAbsolutePath().normalize();
		Map<String, String> values = exactProperties(planPath, NEXT_KEYS);
		if (!values.get("schema").equals(NEXT_SCHEMA)
				|| !values.get("planning-effect").equals("exact-equality")
				|| !values.get("adaptive-stopping").equals("false")
				|| !values.get("family-wise-alpha").equals(Double.toString(PairedPerformanceContractV2.FAMILY_WISE_ALPHA))
				|| !values.get("throughput-minimum-ratio").equals(Double.toString(PairedPerformanceContractV2.THROUGHPUT_MINIMUM_RATIO))
				|| !values.get("cost-maximum-ratio").equals(Double.toString(PairedPerformanceContractV2.COST_MAXIMUM_RATIO))
				|| !values.get("multiplicity").equals("holm-bonferroni")) {
			throw new IllegalArgumentException("Planned prepare contract changed");
		}
		Path source = Path.of(values.get("source-root")).toAbsolutePath().normalize();
		if (!values.get("source-results-sha256").equals(
				sha256(source.resolve(FiniteDeadlineContentionBenchmark.RESULTS_JSON)))
				|| !values.get("source-schedule-sha256").equals(
						sha256(source.resolve(FiniteDeadlineContentionBenchmark.SCHEDULE_FILE)))
				|| !values.get("source-metadata-sha256").equals(
						sha256(source.resolve(FiniteDeadlineContentionBenchmark.METADATA_FILE)))
				|| !values.get("source-workers-sha256").equals(
						workersSha256(FiniteDeadlineContentionBenchmark.Prepared.read(source)))
				|| !values.get("source-configuration-sha256").equals(
						FiniteDeadlineContentionBenchmark.Prepared.read(source).configurationSha256())) {
			throw new IllegalArgumentException("Precision source changed after planning");
		}
		var expanded = new LinkedHashMap<String, String>();
		expanded.put("mode", "prepare");
		expanded.put("root", arguments.get("root"));
		for (String key : List.of("baseline-sha", "candidate-sha", "baseline-worktree",
				"candidate-worktree", "baseline-classes", "candidate-classes", "hardware-description",
				"enforce", "pairs", "operations", "warmup-operations", "submitters", "read-workers",
				"write-workers", "analytical-limit", "foreground-capacity", "batch-capacity", "work-tokens",
				"cooperative-yields", "cooperative-parks", "expired-deadline-percent",
				"cancellation-percent", "failure-percent", "cooperative-percent",
				"alternate-storage-pressure", "seed", "timeout-seconds")) expanded.put(key, values.get(key));
		return Map.copyOf(expanded);
	}

	private static double pairedLogStandardDeviation(double[] baseline, double[] candidate) {
		if (baseline.length != candidate.length || baseline.length < PairedPerformanceContractV2.REQUIRED_PAIRS) {
			throw new IllegalArgumentException("Complete paired samples are required");
		}
		double[] logs = new double[baseline.length];
		double sum = 0.0d;
		for (int index = 0; index < logs.length; index++) {
			if (!Double.isFinite(baseline[index]) || baseline[index] <= 0.0d
					|| !Double.isFinite(candidate[index]) || candidate[index] <= 0.0d) {
				throw new IllegalArgumentException("Planner samples must be finite and positive");
			}
			logs[index] = Math.log(candidate[index] / baseline[index]);
			sum += logs[index];
		}
		double mean = sum / logs.length;
		double squared = 0.0d;
		for (double value : logs) squared += (value - mean) * (value - mean);
		return Math.sqrt(squared / (logs.length - 1.0d));
	}

	private static String json(Plan plan, Instant generatedAt) {
		var out = new StringBuilder("{\n")
				.append("  \"schema\": \"").append(SCHEMA).append("\",\n")
				.append("  \"generated_at\": \"").append(generatedAt).append("\",\n")
				.append("  \"source_decision\": \"inconclusive\",\n")
				.append("  \"source_configuration_sha256\": \"").append(plan.sourceConfigurationSha256()).append("\",\n")
				.append("  \"source_results_sha256\": \"").append(plan.sourceResultsSha256()).append("\",\n")
				.append("  \"source_schedule_sha256\": \"").append(plan.sourceScheduleSha256()).append("\",\n")
				.append("  \"source_metadata_sha256\": \"").append(plan.sourceMetadataSha256()).append("\",\n")
				.append("  \"source_workers_sha256\": \"").append(plan.sourceWorkersSha256()).append("\",\n")
				.append("  \"planning_effect\": \"exact-equality\",\n")
				.append("  \"power_method\": \"student-critical-normal-approximation-v1\",\n")
				.append("  \"global_pass_power_lower_bound\": ").append(GLOBAL_PASS_POWER).append(",\n")
				.append("  \"per_metric_target_power\": ").append(PER_METRIC_POWER).append(",\n")
				.append("  \"metrics_planned\": ").append(FiniteDeadlineContentionBenchmark.METRICS.size()).append(",\n")
				.append("  \"source_fixed_pairs\": ").append(plan.sourceFixedPairs()).append(",\n")
				.append("  \"recommended_fixed_pairs\": ").append(plan.recommendedFixedPairs()).append(",\n")
				.append("  \"recommended_fresh_processes\": ").append(plan.recommendedFixedPairs() * 2).append(",\n")
				.append("  \"adaptive_stopping\": false,\n")
				.append("  \"family_wise_alpha\": ").append(PairedPerformanceContractV2.FAMILY_WISE_ALPHA).append(",\n")
				.append("  \"throughput_minimum_ratio\": ").append(PairedPerformanceContractV2.THROUGHPUT_MINIMUM_RATIO).append(",\n")
				.append("  \"cost_maximum_ratio\": ").append(PairedPerformanceContractV2.COST_MAXIMUM_RATIO).append(",\n")
				.append("  \"multiplicity\": \"holm-bonferroni\",\n")
				.append("  \"metrics\": [\n");
		for (int index = 0; index < plan.metrics().size(); index++) {
			MetricPlan metric = plan.metrics().get(index);
			out.append("    {\"name\": \"").append(metric.name()).append("\", \"direction\": \"")
					.append(metric.direction()).append("\", \"paired_log_sd\": ")
					.append(metric.pairedLogStandardDeviation()).append(", \"margin_log\": ")
					.append(metric.marginLog()).append(", \"target_power\": ").append(metric.targetPower())
					.append(", \"required_fixed_pairs\": ").append(metric.requiredFixedPairs()).append('}')
					.append(index + 1 == plan.metrics().size() ? '\n' : ',').append(index + 1 == plan.metrics().size() ? "" : "\n");
		}
		return out.append("  ]\n}\n").toString();
	}

	private static String markdown(Plan plan) {
		var out = new StringBuilder("# Finite-deadline precision plan\n\n")
				.append("- Source decision: `INCONCLUSIVE`\n")
				.append("- Planning effect: exact equality; observed direction is not used\n")
				.append("- Source fixed pairs: `").append(plan.sourceFixedPairs()).append("`\n")
				.append("- Recommended fixed pairs: `").append(plan.recommendedFixedPairs()).append("` (`")
				.append(plan.recommendedFixedPairs() * 2).append("` fresh serial JVMs)\n")
				.append("- Margins remain `0.99` throughput and `1.02` cost; no adaptive stopping\n")
				.append("- Global pass-power lower bound: `0.90` via eight predeclared per-metric targets\n\n")
				.append("| Metric | Direction | Paired log SD | Required pairs |\n")
				.append("|---|---|---:|---:|\n");
		for (MetricPlan metric : plan.metrics()) {
			out.append("| ").append(metric.name()).append(" | ").append(metric.direction()).append(" | ")
					.append(String.format(Locale.ROOT, "%.9f", metric.pairedLogStandardDeviation())).append(" | ")
					.append(metric.requiredFixedPairs()).append(" |\n");
		}
		return out.toString();
	}

	private static String nextProperties(Path root,
			FiniteDeadlineContentionBenchmark.Prepared prepared, Plan plan) {
		return "schema=" + NEXT_SCHEMA + '\n' + "source-root=" + root.toAbsolutePath().normalize() + '\n'
				+ "source-configuration-sha256=" + plan.sourceConfigurationSha256() + '\n'
				+ "source-results-sha256=" + plan.sourceResultsSha256() + '\n'
				+ "source-schedule-sha256=" + plan.sourceScheduleSha256() + '\n'
				+ "source-metadata-sha256=" + plan.sourceMetadataSha256() + '\n'
				+ "source-workers-sha256=" + plan.sourceWorkersSha256() + '\n'
				+ "planning-effect=exact-equality\n" + "adaptive-stopping=false\n"
				+ "family-wise-alpha=" + PairedPerformanceContractV2.FAMILY_WISE_ALPHA + '\n'
				+ "throughput-minimum-ratio=" + PairedPerformanceContractV2.THROUGHPUT_MINIMUM_RATIO + '\n'
				+ "cost-maximum-ratio=" + PairedPerformanceContractV2.COST_MAXIMUM_RATIO + '\n'
				+ "multiplicity=holm-bonferroni\n" + "baseline-sha=" + prepared.baselineSha() + '\n'
				+ "candidate-sha=" + prepared.candidateSha() + '\n'
				+ "baseline-worktree=" + prepared.baselineWorktree() + '\n'
				+ "candidate-worktree=" + prepared.candidateWorktree() + '\n'
				+ "baseline-classes=" + prepared.baselineClasses() + '\n'
				+ "candidate-classes=" + prepared.candidateClasses() + '\n'
				+ "hardware-description=" + prepared.hardwareDescription() + '\n' + "enforce=" + prepared.enforce() + '\n'
				+ "pairs=" + plan.recommendedFixedPairs() + '\n' + "operations=" + prepared.operations() + '\n'
				+ "warmup-operations=" + prepared.warmupOperations() + '\n' + "submitters=" + prepared.submitters() + '\n'
				+ "read-workers=" + prepared.readWorkers() + '\n' + "write-workers=" + prepared.writeWorkers() + '\n'
				+ "analytical-limit=" + prepared.analyticalLimit() + '\n'
				+ "foreground-capacity=" + prepared.foregroundCapacity() + '\n'
				+ "batch-capacity=" + prepared.batchCapacity() + '\n' + "work-tokens=" + prepared.workTokens() + '\n'
				+ "cooperative-yields=" + prepared.cooperativeYields() + '\n'
				+ "cooperative-parks=" + prepared.cooperativeParks() + '\n'
				+ "expired-deadline-percent=" + prepared.expiredDeadlinePercent() + '\n'
				+ "cancellation-percent=" + prepared.cancellationPercent() + '\n'
				+ "failure-percent=" + prepared.failurePercent() + '\n'
				+ "cooperative-percent=" + prepared.cooperativePercent() + '\n'
				+ "alternate-storage-pressure=" + prepared.alternateStoragePressure() + '\n'
				+ "seed=" + prepared.seed() + '\n' + "timeout-seconds=" + prepared.timeout().toSeconds() + '\n';
	}

	private static Map<String, String> arguments(String[] args) {
		var values = new LinkedHashMap<String, String>();
		for (String argument : args) {
			int separator = argument.indexOf('=');
			if (!argument.startsWith("--") || separator <= 2
					|| values.putIfAbsent(argument.substring(2, separator), argument.substring(separator + 1)) != null) {
				throw new IllegalArgumentException("Expected unique --key=value arguments");
			}
		}
		return Map.copyOf(values);
	}

	private static Map<String, String> exactProperties(Path path, Set<String> expected) throws IOException {
		if (!Files.isRegularFile(path)) throw new IllegalArgumentException("Missing plan file " + path);
		var values = new LinkedHashMap<String, String>();
		String[] lines = Files.readString(path).split("\\R", -1);
		for (int index = 0; index < lines.length; index++) {
			String line = lines[index];
			if (line.isEmpty() && index == lines.length - 1) continue;
			int separator = line.indexOf('=');
			if (separator <= 0 || separator != line.lastIndexOf('=')
					|| values.putIfAbsent(line.substring(0, separator), line.substring(separator + 1)) != null) {
				throw new IllegalArgumentException("Malformed or duplicate plan property at line " + (index + 1));
			}
		}
		if (!values.keySet().equals(expected)) throw new IllegalArgumentException("Next-run plan keys differ");
		return Map.copyOf(values);
	}

	private static String sha256(Path path) throws IOException {
		if (!Files.isRegularFile(path)) throw new IllegalArgumentException("Missing precision source " + path);
		try {
			return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
					.digest(Files.readAllBytes(path)));
		} catch (java.security.NoSuchAlgorithmException impossible) {
			throw new AssertionError(impossible);
		}
	}

	private static String workersSha256(FiniteDeadlineContentionBenchmark.Prepared prepared) throws IOException {
		var manifest = new StringBuilder();
		for (var run : FiniteDeadlineContentionBenchmark.schedule(prepared)) {
			manifest.append(prepared.root().relativize(run.artifact())).append('\t')
					.append(sha256(run.artifact())).append('\n');
		}
		try {
			return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
					.digest(manifest.toString().getBytes(StandardCharsets.UTF_8)));
		} catch (java.security.NoSuchAlgorithmException impossible) {
			throw new AssertionError(impossible);
		}
	}

	record MetricPlan(String name, String direction, double pairedLogStandardDeviation,
			double marginLog, double targetPower, int requiredFixedPairs) {}

	record Plan(String sourceConfigurationSha256, String sourceResultsSha256,
			String sourceScheduleSha256, String sourceMetadataSha256, String sourceWorkersSha256, int sourceFixedPairs,
			int recommendedFixedPairs, List<MetricPlan> metrics) {}
}
