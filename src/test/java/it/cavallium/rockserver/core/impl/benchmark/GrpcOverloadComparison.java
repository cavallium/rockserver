package it.cavallium.rockserver.core.impl.benchmark;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * Strict paired comparator for {@link GrpcOverloadBenchmark} subprocess artifacts.
 *
 * <p>The schedule is declared in a manifest before measurement. Each pair names one baseline and
 * one candidate artifact and records which ran first. Enforced comparison requires at least ten
 * fresh-process pairs, alternating order, exact workload/dataset/environment identity, the immutable
 * v1.3.11 baseline, all structural gates, no mean regression, no statistically demonstrated
 * regression, and a material improvement in a predeclared primary metric. Exception ceilings are
 * reported but never turn a failure into a pass.</p>
 */
public final class GrpcOverloadComparison {

	public static final String RUN_INPUT_FILE = "comparison-input.properties";
	public static final String RUN_INPUT_SCHEMA = "rockserver-grpc-overload-comparison-input-v1";
	public static final String MANIFEST_SCHEMA = "rockserver-grpc-overload-comparison-manifest-v1";
	public static final String REPORT_SCHEMA = "rockserver-grpc-overload-comparison-v1";
	public static final String REQUIRED_BASELINE_SHA = "bb4f1a7e90db1fdfd785936594d080e8c4a0ba4e";
	private static final int MIN_ENFORCED_PAIRS = 10;

	private static final List<MetricSpec> METRICS = List.of(
			MetricSpec.throughput("foreground-only.foreground-throughput"),
			MetricSpec.throughput("mixed.foreground-throughput"),
			MetricSpec.throughput("mixed.useful-throughput"),
			MetricSpec.latency("foreground-only.foreground-p99-nanos"),
			MetricSpec.latency("mixed.foreground-p99-nanos"),
			MetricSpec.latency("foreground-only.latency-queue-p99-nanos"),
			MetricSpec.latency("mixed.latency-queue-p99-nanos"),
			MetricSpec.latency("foreground-only.latency-execution-p99-nanos"),
			MetricSpec.latency("mixed.latency-execution-p99-nanos"),
			MetricSpec.cpu("foreground-only.cpu-nanos-per-operation"),
			MetricSpec.cpu("mixed.cpu-nanos-per-operation"),
			MetricSpec.allocation("foreground-only.allocated-bytes-per-operation"),
			MetricSpec.allocation("mixed.allocated-bytes-per-operation"),
			MetricSpec.memory("foreground-only.peak-live-heap-bytes"),
			MetricSpec.memory("mixed.peak-live-heap-bytes"),
			MetricSpec.memory("foreground-only.peak-direct-memory-bytes"),
			MetricSpec.memory("mixed.peak-direct-memory-bytes"),
			MetricSpec.memory("foreground-only.peak-rss-bytes"),
			MetricSpec.memory("mixed.peak-rss-bytes"),
			MetricSpec.throughput("degradation.foreground-throughput-ratio"),
			MetricSpec.latency("degradation.foreground-p99-ratio"),
			MetricSpec.noIncrease("foreground-only.gc-collections"),
			MetricSpec.noIncrease("mixed.gc-collections"),
			MetricSpec.noIncrease("foreground-only.gc-millis"),
			MetricSpec.noIncrease("mixed.gc-millis"),
			MetricSpec.noIncrease("foreground-only.peak-thread-count"),
			MetricSpec.noIncrease("mixed.peak-thread-count"),
			MetricSpec.noIncrease("foreground-only.peak-native-handles"),
			MetricSpec.noIncrease("mixed.peak-native-handles")
	);
	private static final Map<String, MetricSpec> METRIC_BY_NAME = metricIndex();

	private GrpcOverloadComparison() {
	}

	public static void main(String[] args) throws Exception {
		if (Arrays.asList(args).contains("--help")) {
			printUsage();
			return;
		}
		Map<String, String> options = parseOptions(args);
		Path manifestPath = Path.of(required(options, "manifest")).toAbsolutePath().normalize();
		Path output = Path.of(required(options, "output")).toAbsolutePath().normalize();
		boolean enforce = bool(options.getOrDefault("enforce", "true"), "enforce");
		if (!options.keySet().equals(Set.of("manifest", "output", "enforce"))
				&& !options.keySet().equals(Set.of("manifest", "output"))) {
			throw new IllegalArgumentException("Known options are --manifest, --output, and --enforce");
		}
		Manifest manifest = readManifest(manifestPath);
		Comparison comparison = compare(manifest, enforce);
		Files.createDirectories(output);
		Path json = output.resolve("comparison.json");
		Path markdown = output.resolve("comparison.md");
		Files.writeString(json, toJson(comparison), StandardOpenOption.CREATE_NEW);
		Files.writeString(markdown, toMarkdown(comparison), StandardOpenOption.CREATE_NEW);
		System.out.println(toMarkdown(comparison));
		System.out.println("Machine-readable comparison: " + json);
		System.out.println("Human-readable comparison: " + markdown);
		if (enforce && !comparison.passed()) {
			throw new IllegalStateException("Overload Pareto comparison failed: "
					+ comparison.failedSummary());
		}
	}

	/** Writes the strict, deterministic per-process artifact consumed by the paired comparator. */
	public static void writeRunInput(Path output, RunInput input) throws IOException {
		Objects.requireNonNull(input, "input");
		var text = new StringBuilder(4_096);
		property(text, "schema", RUN_INPUT_SCHEMA);
		property(text, "build-id", input.buildId());
		property(text, "build-state", input.buildState());
		property(text, "storage-label", input.storageLabel());
		property(text, "cache-state", input.cacheState());
		property(text, "host-state", input.hostState());
		property(text, "dataset-fingerprint", input.datasetFingerprint());
		property(text, "comparison-fingerprint", input.comparisonFingerprint());
		property(text, "environment-fingerprint", input.environmentFingerprint());
		property(text, "environment-summary", input.environmentSummary());
		property(text, "process-id", Long.toString(input.processId()));
		property(text, "process-start", input.processStart());
		property(text, "run-started", input.runStarted());
		property(text, "run-finished", input.runFinished());
		property(text, "rounds", Integer.toString(input.rounds()));
		property(text, "enforced", Boolean.toString(input.enforced()));
		property(text, "acceptance-passed", Boolean.toString(input.acceptancePassed()));
		property(text, "integrity-passed", Boolean.toString(input.integrityPassed()));
		property(text, "requests-conserved", Boolean.toString(input.requestsConserved()));
		property(text, "resources-drained", Boolean.toString(input.resourcesDrained()));
		property(text, "shutdown-clean", Boolean.toString(input.shutdownClean()));
		property(text, "telemetry-available", Boolean.toString(input.telemetryAvailable()));
		property(text, "native-leaks", Long.toString(input.nativeLeaks()));
		property(text, "cancellations", Long.toString(input.cancellations()));
		for (MetricSpec metric : METRICS) {
			Double value = input.metrics().get(metric.name());
			if (value == null) {
				throw new IllegalArgumentException("Missing overload comparison metric " + metric.name());
			}
			property(text, "metric." + metric.name(), format(value));
		}
		if (!input.metrics().keySet().equals(METRIC_BY_NAME.keySet())) {
			throw new IllegalArgumentException("Unexpected overload comparison metrics: "
					+ difference(input.metrics().keySet(), METRIC_BY_NAME.keySet()));
		}
		Files.writeString(output, text, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
	}

	private static RunInput readRunInput(Path input) throws IOException {
		Properties properties = loadStrict(input, runInputKeys());
		if (!RUN_INPUT_SCHEMA.equals(required(properties, "schema"))) {
			throw new IllegalArgumentException("Unsupported overload run-input schema: " + input);
		}
		var metrics = new LinkedHashMap<String, Double>();
		for (MetricSpec metric : METRICS) {
			metrics.put(metric.name(), decimal(properties, "metric." + metric.name()));
		}
		return new RunInput(
				required(properties, "build-id"),
				required(properties, "build-state"),
				required(properties, "storage-label"),
				required(properties, "cache-state"),
				required(properties, "host-state"),
				required(properties, "dataset-fingerprint"),
				required(properties, "comparison-fingerprint"),
				required(properties, "environment-fingerprint"),
				required(properties, "environment-summary"),
				number(properties, "process-id"),
				required(properties, "process-start"),
				required(properties, "run-started"),
				required(properties, "run-finished"),
				integer(properties, "rounds"),
				bool(properties, "enforced"),
				bool(properties, "acceptance-passed"),
				bool(properties, "integrity-passed"),
				bool(properties, "requests-conserved"),
				bool(properties, "resources-drained"),
				bool(properties, "shutdown-clean"),
				bool(properties, "telemetry-available"),
				number(properties, "native-leaks"),
				number(properties, "cancellations"),
				metrics);
	}

	private static Manifest readManifest(Path path) throws IOException {
		Properties first = new Properties();
		try (var reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
			first.load(reader);
		}
		if (!MANIFEST_SCHEMA.equals(required(first, "schema"))) {
			throw new IllegalArgumentException("Unsupported overload comparison manifest schema");
		}
		int pairs = integer(first, "pairs");
		if (pairs < 1) {
			throw new IllegalArgumentException("Manifest pairs must be positive");
		}
		var expected = new LinkedHashSet<>(Set.of(
				"schema", "declared-at", "baseline-build", "candidate-build", "primary-metrics", "pairs"));
		for (int index = 1; index <= pairs; index++) {
			expected.add("pair." + index + ".order");
			expected.add("pair." + index + ".baseline");
			expected.add("pair." + index + ".candidate");
		}
		Properties properties = loadStrict(path, expected);
		Path parent = path.getParent() == null ? Path.of(".").toAbsolutePath().normalize() : path.getParent();
		var schedule = new ArrayList<ScheduledPair>(pairs);
		for (int index = 1; index <= pairs; index++) {
			String prefix = "pair." + index + '.';
			schedule.add(new ScheduledPair(index,
					Order.parse(required(properties, prefix + "order")),
					resolve(parent, required(properties, prefix + "baseline")),
					resolve(parent, required(properties, prefix + "candidate"))));
		}
		Set<String> primaryMetrics = parsePrimaryMetrics(required(properties, "primary-metrics"));
		return new Manifest(path,
				parseInstant(required(properties, "declared-at"), "declared-at"),
				required(properties, "baseline-build"),
				required(properties, "candidate-build"),
				primaryMetrics,
				List.copyOf(schedule));
	}

	private static Comparison compare(Manifest manifest, boolean enforce) throws IOException {
		var failures = new ArrayList<String>();
		if (enforce && manifest.schedule().size() < MIN_ENFORCED_PAIRS) {
			failures.add("enforced comparison requires at least " + MIN_ENFORCED_PAIRS + " pairs");
		}
		if (enforce && !manifest.baselineBuild().equals(REQUIRED_BASELINE_SHA)) {
			failures.add("baseline build must be immutable v1.3.11 " + REQUIRED_BASELINE_SHA);
		}
		if (!fullSha(manifest.baselineBuild()) || !fullSha(manifest.candidateBuild())
				|| manifest.baselineBuild().equals(manifest.candidateBuild())) {
			failures.add("baseline and candidate must be distinct full lowercase Git SHAs");
		}
		validatePrimaryMetrics(manifest.primaryMetrics(), failures);
		validateSchedule(manifest.schedule(), failures);

		var baselineRuns = new ArrayList<RunInput>(manifest.schedule().size());
		var candidateRuns = new ArrayList<RunInput>(manifest.schedule().size());
		var processIdentities = new LinkedHashSet<String>();
		var artifactPaths = new LinkedHashSet<Path>();
		Instant previousPairFinished = null;
		for (ScheduledPair pair : manifest.schedule()) {
			Path baselineArtifact = pair.baseline().toRealPath();
			Path candidateArtifact = pair.candidate().toRealPath();
			if (!artifactPaths.add(baselineArtifact) || !artifactPaths.add(candidateArtifact)) {
				failures.add("pair " + pair.index() + " reuses a run artifact");
			}
			RunInput baseline = readRunInput(baselineArtifact);
			RunInput candidate = readRunInput(candidateArtifact);
			baselineRuns.add(baseline);
			candidateRuns.add(candidate);
			validateRun(pair.index(), "baseline", baseline, manifest.baselineBuild(), enforce, failures);
			validateRun(pair.index(), "candidate", candidate, manifest.candidateBuild(), enforce, failures);
			previousPairFinished = validateExecutionOrder(pair,
					baseline,
					candidate,
					previousPairFinished,
					failures);
			for (RunInput run : List.of(baseline, candidate)) {
				String identity = run.processId() + "@" + run.processStart();
				if (!processIdentities.add(identity)) {
					failures.add("fresh-subprocess identity reused: " + identity);
				}
			}
		}
		String dataset = baselineRuns.isEmpty() ? "unavailable" : baselineRuns.getFirst().datasetFingerprint();
		String workload = baselineRuns.isEmpty() ? "unavailable" : baselineRuns.getFirst().comparisonFingerprint();
		String environment = baselineRuns.isEmpty() ? "unavailable" : baselineRuns.getFirst().environmentFingerprint();
		String environmentSummary = baselineRuns.isEmpty() ? "unavailable" : baselineRuns.getFirst().environmentSummary();
		for (RunInput run : combined(baselineRuns, candidateRuns)) {
			if (!run.datasetFingerprint().equals(dataset)) {
				failures.add("fixed dataset fingerprint mismatch for build " + run.buildId());
			}
			if (!run.comparisonFingerprint().equals(workload)) {
				failures.add("workload/config/cache fingerprint mismatch for build " + run.buildId());
			}
			if (!run.environmentFingerprint().equals(environment)
					|| !run.environmentSummary().equals(environmentSummary)) {
				failures.add("JDK/native/hardware environment mismatch for build " + run.buildId());
			}
		}
		Instant earliestRun = combined(baselineRuns, candidateRuns).stream()
				.map(run -> parseInstantOrNull(run.runStarted()))
				.filter(Objects::nonNull)
				.min(Instant::compareTo)
				.orElse(null);
		if (earliestRun != null && manifest.declaredAt().isAfter(earliestRun)) {
			failures.add("manifest primary metrics and schedule were not declared before measurement");
		}
		Map<String, MetricSamples> samples = samples(baselineRuns, candidateRuns);
		List<MetricComparison> metricComparisons = evaluateMetrics(samples, manifest.primaryMetrics(), failures);
		boolean material = metricComparisons.stream().anyMatch(MetricComparison::materialImprovement);
		if (!material) {
			failures.add("no predeclared primary metric demonstrated a material 95% confidence bound");
		}
		return new Comparison(REPORT_SCHEMA,
				Instant.now(),
				manifest.source(),
				manifest.declaredAt(),
				manifest.baselineBuild(),
				manifest.candidateBuild(),
				manifest.schedule().size(),
				manifest.primaryMetrics(),
				dataset,
				workload,
				environment,
				environmentSummary,
				List.copyOf(metricComparisons),
				List.copyOf(failures));
	}

	private static Instant validateExecutionOrder(ScheduledPair pair,
			RunInput baseline,
			RunInput candidate,
			Instant previousPairFinished,
			List<String> failures) {
		Instant baselineStarted = parseInstantOrNull(baseline.runStarted());
		Instant baselineFinished = parseInstantOrNull(baseline.runFinished());
		Instant candidateStarted = parseInstantOrNull(candidate.runStarted());
		Instant candidateFinished = parseInstantOrNull(candidate.runFinished());
		if (baselineStarted == null || baselineFinished == null
				|| candidateStarted == null || candidateFinished == null) {
			return previousPairFinished;
		}
		Instant firstStarted;
		Instant firstFinished;
		Instant secondStarted;
		Instant secondFinished;
		if (pair.order() == Order.BASELINE_FIRST) {
			firstStarted = baselineStarted;
			firstFinished = baselineFinished;
			secondStarted = candidateStarted;
			secondFinished = candidateFinished;
		} else {
			firstStarted = candidateStarted;
			firstFinished = candidateFinished;
			secondStarted = baselineStarted;
			secondFinished = baselineFinished;
		}
		if (firstFinished.isAfter(secondStarted)) {
			failures.add("pair " + pair.index()
					+ " did not execute in its declared serial order or its runs overlapped");
		}
		if (previousPairFinished != null && previousPairFinished.isAfter(firstStarted)) {
			failures.add("pair " + pair.index() + " started before the previous pair finished");
		}
		return secondFinished.isAfter(firstFinished) ? secondFinished : firstFinished;
	}

	private static void validateRun(int pair,
			String implementation,
			RunInput run,
			String expectedBuild,
			boolean enforce,
			List<String> failures) {
		if (!run.buildId().equals(expectedBuild)) {
			failures.add("pair " + pair + ' ' + implementation + " build mismatch: " + run.buildId());
		}
		for (String failure : run.structuralFailures(enforce)) {
			failures.add("pair " + pair + ' ' + implementation + ": " + failure);
		}
	}

	private static void validateSchedule(List<ScheduledPair> schedule, List<String> failures) {
		if (schedule.isEmpty()) {
			failures.add("comparison schedule is empty");
			return;
		}
		Order first = schedule.getFirst().order();
		int baselineFirst = 0;
		int candidateFirst = 0;
		for (int index = 0; index < schedule.size(); index++) {
			ScheduledPair pair = schedule.get(index);
			Order expected = (index & 1) == 0 ? first : first.opposite();
			if (pair.index() != index + 1 || pair.order() != expected) {
				failures.add("pair order must alternate from the predeclared first pair");
				break;
			}
			if (pair.order() == Order.BASELINE_FIRST) baselineFirst++;
			else candidateFirst++;
		}
		if (Math.abs(baselineFirst - candidateFirst) > 1) {
			failures.add("counterbalanced order counts differ by more than one");
		}
	}

	private static void validatePrimaryMetrics(Set<String> primaryMetrics, List<String> failures) {
		if (primaryMetrics.isEmpty()) {
			failures.add("at least one primary metric must be predeclared");
		}
		for (String name : primaryMetrics) {
			MetricSpec spec = METRIC_BY_NAME.get(name);
			if (spec == null || !spec.materialEligible()) {
				failures.add("primary metric is unknown or cannot demonstrate material improvement: " + name);
			}
		}
	}

	private static Map<String, MetricSamples> samples(List<RunInput> baseline, List<RunInput> candidate) {
		var result = new LinkedHashMap<String, MetricSamples>();
		for (MetricSpec metric : METRICS) {
			double[] baselineValues = new double[baseline.size()];
			double[] candidateValues = new double[candidate.size()];
			for (int index = 0; index < baseline.size(); index++) {
				baselineValues[index] = baseline.get(index).metrics().get(metric.name());
				candidateValues[index] = candidate.get(index).metrics().get(metric.name());
			}
			result.put(metric.name(), new MetricSamples(baselineValues, candidateValues));
		}
		return Map.copyOf(result);
	}

	private static List<MetricComparison> evaluateMetrics(Map<String, MetricSamples> samples,
			Set<String> primaryMetrics,
			List<String> failures) {
		var comparisons = new ArrayList<MetricComparison>(METRICS.size());
		for (MetricSpec metric : METRICS) {
			MetricSamples values = Objects.requireNonNull(samples.get(metric.name()), metric.name());
			if (values.baseline().length != values.candidate().length || values.baseline().length == 0) {
				throw new IllegalArgumentException("Metric samples must contain non-empty paired arrays: "
						+ metric.name());
			}
			PairedBenchmarkStatistics.RatioConfidenceInterval interval = interval(metric, values);
			boolean automatic;
			boolean confidence;
			if (metric.direction() == Direction.NO_INCREASE) {
				automatic = true;
				for (int index = 0; index < values.baseline().length; index++) {
					automatic &= values.candidate()[index] <= values.baseline()[index];
				}
				confidence = automatic;
			} else if (metric.direction() == Direction.HIGHER) {
				automatic = interval.available() && interval.mean() >= 1.0d;
				confidence = interval.available() && interval.upper95() >= 1.0d;
			} else {
				automatic = interval.available() && interval.mean() <= 1.0d;
				confidence = interval.available() && interval.lower95() <= 1.0d;
			}
			boolean passed = automatic && confidence;
			boolean withinException = !passed && Double.isFinite(metric.exceptionCeiling())
					&& interval.available()
					&& (metric.direction() == Direction.HIGHER
					? interval.mean() >= metric.exceptionCeiling()
					: interval.mean() <= metric.exceptionCeiling());
			boolean material = primaryMetrics.contains(metric.name()) && metric.materialEligible()
					&& interval.available()
					&& (metric.direction() == Direction.HIGHER
					? interval.lower95() >= 1.02d
					: interval.upper95() <= 0.98d);
			var comparison = new MetricComparison(metric.name(),
					metric.direction().value,
					values.baseline(),
					values.candidate(),
					interval,
					passed,
					withinException,
					material);
			comparisons.add(comparison);
			if (!passed) {
				failures.add(metric.name() + " automatic no-regression gate failed"
						+ (withinException ? " (inside exception ceiling, still requires ablation and approval)" : ""));
			}
		}
		return List.copyOf(comparisons);
	}

	private static PairedBenchmarkStatistics.RatioConfidenceInterval interval(MetricSpec metric,
			MetricSamples values) {
		if (metric.direction() == Direction.NO_INCREASE) {
			for (int index = 0; index < values.baseline().length; index++) {
				if (values.baseline()[index] <= 0.0d || values.candidate()[index] <= 0.0d) {
					return new PairedBenchmarkStatistics.RatioConfidenceInterval(
							0, Double.NaN, Double.NaN, Double.NaN);
				}
			}
		}
		return PairedBenchmarkStatistics.pairedLogRatio(values.baseline(), values.candidate());
	}

	/** Pure Pareto evaluation for deterministic unit tests. */
	public static Comparison evaluateForTesting(Map<String, MetricSamples> samples,
			Set<String> primaryMetrics,
			List<String> structuralFailures) {
		var failures = new ArrayList<>(structuralFailures);
		validatePrimaryMetrics(primaryMetrics, failures);
		List<MetricComparison> metrics = evaluateMetrics(samples, primaryMetrics, failures);
		if (metrics.stream().noneMatch(MetricComparison::materialImprovement)) {
			failures.add("no predeclared primary metric demonstrated a material 95% confidence bound");
		}
		return new Comparison(REPORT_SCHEMA,
				Instant.EPOCH,
				Path.of("synthetic-manifest"),
				Instant.EPOCH,
				REQUIRED_BASELINE_SHA,
				"c".repeat(40),
				samples.values().stream().findFirst().map(value -> value.baseline().length).orElse(0),
				primaryMetrics,
				"synthetic-dataset",
				"synthetic-workload",
				"synthetic-environment",
				"synthetic environment",
				metrics,
				failures);
	}

	public static Set<String> metricNamesForTesting() {
		return METRIC_BY_NAME.keySet();
	}

	public static boolean higherIsBetterForTesting(String name) {
		return Objects.requireNonNull(METRIC_BY_NAME.get(name), name).direction() == Direction.HIGHER;
	}

	public static boolean noIncreaseForTesting(String name) {
		return Objects.requireNonNull(METRIC_BY_NAME.get(name), name).direction() == Direction.NO_INCREASE;
	}

	private static String toJson(Comparison comparison) {
		var json = new StringBuilder(16_384);
		json.append("{\n  \"schema\": ");
		json(json, comparison.schema());
		json.append(",\n  \"generated\": ");
		json(json, comparison.generated().toString());
		json.append(",\n  \"manifest\": ");
		json(json, comparison.manifest().toString());
		json.append(",\n  \"declared_at\": ");
		json(json, comparison.declaredAt().toString());
		json.append(",\n  \"baseline_build\": ");
		json(json, comparison.baselineBuild());
		json.append(",\n  \"candidate_build\": ");
		json(json, comparison.candidateBuild());
		json.append(",\n  \"pairs\": ").append(comparison.pairs());
		json.append(",\n  \"dataset_fingerprint\": ");
		json(json, comparison.datasetFingerprint());
		json.append(",\n  \"comparison_fingerprint\": ");
		json(json, comparison.comparisonFingerprint());
		json.append(",\n  \"environment_fingerprint\": ");
		json(json, comparison.environmentFingerprint());
		json.append(",\n  \"environment_summary\": ");
		json(json, comparison.environmentSummary());
		json.append(",\n  \"primary_metrics\": [");
		int primaryIndex = 0;
		for (String primary : comparison.primaryMetrics()) {
			if (primaryIndex++ > 0) json.append(',');
			json(json, primary);
		}
		json.append("],\n  \"metrics\": [");
		for (int index = 0; index < comparison.metrics().size(); index++) {
			MetricComparison metric = comparison.metrics().get(index);
			if (index > 0) json.append(',');
			json.append("\n    {\"name\": ");
			json(json, metric.name());
			json.append(", \"direction\": ");
			json(json, metric.direction());
			json.append(", \"baseline\": ").append(array(metric.baseline()))
					.append(", \"candidate\": ").append(array(metric.candidate()))
					.append(", \"ratio\": {\"samples\": ").append(metric.ratio().samples())
					.append(", \"geometric_mean\": ").append(formatJson(metric.ratio().mean()))
					.append(", \"lower_95\": ").append(formatJson(metric.ratio().lower95()))
					.append(", \"upper_95\": ").append(formatJson(metric.ratio().upper95()))
					.append("}, \"automatic_pass\": ").append(metric.automaticPass())
					.append(", \"within_exception_ceiling\": ").append(metric.withinExceptionCeiling())
					.append(", \"material_improvement\": ").append(metric.materialImprovement())
					.append('}');
		}
		json.append("\n  ],\n  \"failures\": [");
		for (int index = 0; index < comparison.failures().size(); index++) {
			if (index > 0) json.append(',');
			json(json, comparison.failures().get(index));
		}
		json.append("],\n  \"passed\": ").append(comparison.passed()).append("\n}\n");
		return json.toString();
	}

	private static String toMarkdown(Comparison comparison) {
		var markdown = new StringBuilder(12_000);
		markdown.append("# Rockserver gRPC overload paired comparison\n\n")
				.append("- Manifest declared: `").append(comparison.declaredAt()).append("`\n")
				.append("- Baseline: `").append(comparison.baselineBuild()).append("`\n")
				.append("- Candidate: `").append(comparison.candidateBuild()).append("`\n")
				.append("- Counterbalanced fresh-process pairs: `").append(comparison.pairs()).append("`\n")
				.append("- Primary metrics: `").append(comparison.primaryMetrics()).append("`\n")
				.append("- Dataset/workload/environment: `").append(comparison.datasetFingerprint()).append("` / `")
				.append(comparison.comparisonFingerprint()).append("` / `")
				.append(comparison.environmentFingerprint()).append("`\n")
				.append("- Hardware/runtime: `").append(comparison.environmentSummary()).append("`\n\n")
				.append("| Metric | Direction | Baseline absolute pairs | Candidate absolute pairs | Geomean ratio | 95% CI | Automatic | Exception ceiling only | Material primary |\n")
				.append("|---|---|---|---|---:|---:|---|---|---|\n");
		for (MetricComparison metric : comparison.metrics()) {
			markdown.append('|').append(metric.name())
					.append('|').append(metric.direction())
					.append("|`").append(Arrays.toString(metric.baseline())).append("`")
					.append("|`").append(Arrays.toString(metric.candidate())).append("`")
					.append('|').append(format(metric.ratio().mean()))
					.append("|[").append(format(metric.ratio().lower95())).append(", ")
					.append(format(metric.ratio().upper95())).append(']')
					.append('|').append(metric.automaticPass() ? "PASS" : "FAIL")
					.append('|').append(metric.withinExceptionCeiling())
					.append('|').append(metric.materialImprovement()).append("|\n");
		}
		markdown.append("\nException ceilings are diagnostic only; they never produce automatic acceptance.\n\n")
				.append("## Failures\n\n");
		if (comparison.failures().isEmpty()) {
			markdown.append("- None.\n");
		} else {
			for (String failure : comparison.failures()) markdown.append("- ").append(failure).append('\n');
		}
		markdown.append("\nOverall: **").append(comparison.passed() ? "PASS" : "FAIL").append("**.\n");
		return markdown.toString();
	}

	private static Map<String, MetricSpec> metricIndex() {
		var result = new LinkedHashMap<String, MetricSpec>();
		for (MetricSpec metric : METRICS) {
			if (result.put(metric.name(), metric) != null) {
				throw new IllegalStateException("Duplicate overload metric " + metric.name());
			}
		}
		return Map.copyOf(result);
	}

	private static Set<String> runInputKeys() {
		var result = new LinkedHashSet<>(Set.of(
				"schema", "build-id", "build-state", "storage-label", "cache-state", "host-state",
				"dataset-fingerprint", "comparison-fingerprint", "environment-fingerprint",
				"environment-summary", "process-id", "process-start", "run-started", "run-finished",
				"rounds", "enforced",
				"acceptance-passed", "integrity-passed", "requests-conserved", "resources-drained",
				"shutdown-clean", "telemetry-available", "native-leaks", "cancellations"));
		for (MetricSpec metric : METRICS) result.add("metric." + metric.name());
		return Set.copyOf(result);
	}

	private static Properties loadStrict(Path path, Set<String> expected) throws IOException {
		Properties properties = new Properties();
		try (var reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
			properties.load(reader);
		}
		Set<String> actual = properties.stringPropertyNames();
		if (!actual.equals(expected)) {
			throw new IllegalArgumentException("Unexpected properties in " + path + ": missing="
					+ difference(expected, actual) + ", extra=" + difference(actual, expected));
		}
		return properties;
	}

	private static Set<String> difference(Set<String> left, Set<String> right) {
		var result = new LinkedHashSet<>(left);
		result.removeAll(right);
		return result;
	}

	private static List<RunInput> combined(List<RunInput> first, List<RunInput> second) {
		var result = new ArrayList<RunInput>(first.size() + second.size());
		result.addAll(first);
		result.addAll(second);
		return result;
	}

	private static Path resolve(Path parent, String value) {
		Path path = Path.of(value);
		return (path.isAbsolute() ? path : parent.resolve(path)).toAbsolutePath().normalize();
	}

	private static Set<String> parsePrimaryMetrics(String value) {
		var result = new LinkedHashSet<String>();
		for (String metric : value.split(",")) {
			String trimmed = metric.trim();
			if (!trimmed.isEmpty()) result.add(trimmed);
		}
		return Set.copyOf(result);
	}

	private static Map<String, String> parseOptions(String[] args) {
		var result = new LinkedHashMap<String, String>();
		for (String argument : args) {
			if (!argument.startsWith("--") || !argument.contains("=")) {
				throw new IllegalArgumentException("Options must use --name=value: " + argument);
			}
			int equals = argument.indexOf('=');
			String key = argument.substring(2, equals);
			if (result.put(key, argument.substring(equals + 1)) != null) {
				throw new IllegalArgumentException("Duplicate option --" + key);
			}
		}
		return result;
	}

	private static String required(Map<String, String> values, String key) {
		String value = values.get(key);
		if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing --" + key);
		return value;
	}

	private static String required(Properties properties, String key) {
		String value = properties.getProperty(key);
		if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing property " + key);
		return value;
	}

	private static int integer(Properties properties, String key) {
		return Integer.parseInt(required(properties, key));
	}

	private static long number(Properties properties, String key) {
		return Long.parseLong(required(properties, key));
	}

	private static double decimal(Properties properties, String key) {
		double value = Double.parseDouble(required(properties, key));
		if (!Double.isFinite(value)) throw new IllegalArgumentException("Non-finite property " + key);
		return value;
	}

	private static boolean bool(Properties properties, String key) {
		return bool(required(properties, key), key);
	}

	private static boolean bool(String value, String key) {
		return switch (value) {
			case "true" -> true;
			case "false" -> false;
			default -> throw new IllegalArgumentException("Boolean " + key + " must be true or false");
		};
	}

	private static boolean fullSha(String value) {
		return value.matches("[0-9a-f]{40}");
	}

	private static Instant parseInstantOrNull(String value) {
		try {
			return Instant.parse(value);
		} catch (DateTimeParseException failure) {
			return null;
		}
	}

	private static Instant parseInstant(String value, String key) {
		Instant result = parseInstantOrNull(value);
		if (result == null) {
			throw new IllegalArgumentException("Timestamp " + key + " must use ISO-8601 instant syntax");
		}
		return result;
	}

	private static void property(StringBuilder target, String key, String value) {
		target.append(key).append('=').append(escapeProperty(value)).append('\n');
	}

	private static String escapeProperty(String value) {
		return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r");
	}

	private static String format(double value) {
		return Double.isFinite(value) ? String.format(Locale.ROOT, "%.9f", value) : "unavailable";
	}

	private static String formatJson(double value) {
		return Double.isFinite(value) ? Double.toString(value) : "null";
	}

	private static String array(double[] values) {
		var text = new StringBuilder("[");
		for (int index = 0; index < values.length; index++) {
			if (index > 0) text.append(',');
			text.append(formatJson(values[index]));
		}
		return text.append(']').toString();
	}

	private static void json(StringBuilder target, String value) {
		target.append('"');
		for (int index = 0; index < value.length(); index++) {
			char character = value.charAt(index);
			switch (character) {
				case '"' -> target.append("\\\"");
				case '\\' -> target.append("\\\\");
				case '\n' -> target.append("\\n");
				case '\r' -> target.append("\\r");
				case '\t' -> target.append("\\t");
				default -> target.append(character);
			}
		}
		target.append('"');
	}

	private static void printUsage() {
		System.out.println("""
				java ... GrpcOverloadComparison \\
				  --manifest=/path/to/predeclared-manifest.properties \\
				  --output=/path/to/new-comparison-directory --enforce=true

				The manifest must declare its pre-measurement timestamp, baseline/candidate SHAs,
				primary metrics, at least ten pairs, and alternating
				pair.<n>.order/baseline/candidate entries. Each artifact path names a
				comparison-input.properties file written by GrpcOverloadBenchmark.
				""");
	}

	private enum Direction {
		HIGHER("higher"),
		LOWER("lower"),
		NO_INCREASE("no-increase");

		private final String value;

		Direction(String value) {
			this.value = value;
		}
	}

	private record MetricSpec(String name,
			Direction direction,
			double exceptionCeiling,
			boolean materialEligible) {

		private static MetricSpec throughput(String name) {
			return new MetricSpec(name, Direction.HIGHER, 0.990d, true);
		}

		private static MetricSpec latency(String name) {
			return new MetricSpec(name, Direction.LOWER, 1.020d, true);
		}

		private static MetricSpec cpu(String name) {
			return new MetricSpec(name, Direction.LOWER, 1.020d, true);
		}

		private static MetricSpec allocation(String name) {
			return new MetricSpec(name, Direction.LOWER, Double.NaN, true);
		}

		private static MetricSpec memory(String name) {
			return new MetricSpec(name, Direction.LOWER, 1.020d, true);
		}

		private static MetricSpec noIncrease(String name) {
			return new MetricSpec(name, Direction.NO_INCREASE, Double.NaN, false);
		}
	}

	private enum Order {
		BASELINE_FIRST,
		CANDIDATE_FIRST;

		private static Order parse(String value) {
			return switch (value) {
				case "baseline-first" -> BASELINE_FIRST;
				case "candidate-first" -> CANDIDATE_FIRST;
				default -> throw new IllegalArgumentException("Pair order must be baseline-first or candidate-first");
			};
		}

		private Order opposite() {
			return this == BASELINE_FIRST ? CANDIDATE_FIRST : BASELINE_FIRST;
		}
	}

	private record ScheduledPair(int index, Order order, Path baseline, Path candidate) {
	}

	private record Manifest(Path source,
			Instant declaredAt,
			String baselineBuild,
			String candidateBuild,
			Set<String> primaryMetrics,
			List<ScheduledPair> schedule) {
	}

	public record RunInput(String buildId,
			String buildState,
			String storageLabel,
			String cacheState,
			String hostState,
			String datasetFingerprint,
			String comparisonFingerprint,
			String environmentFingerprint,
			String environmentSummary,
			long processId,
			String processStart,
			String runStarted,
			String runFinished,
			int rounds,
			boolean enforced,
			boolean acceptancePassed,
			boolean integrityPassed,
			boolean requestsConserved,
			boolean resourcesDrained,
			boolean shutdownClean,
			boolean telemetryAvailable,
			long nativeLeaks,
			long cancellations,
			Map<String, Double> metrics) {

		public RunInput {
			metrics = Map.copyOf(metrics);
		}

		private List<String> structuralFailures(boolean requireEnforced) {
			var failures = new ArrayList<String>();
			if (requireEnforced && (!enforced || !buildState.equals("clean")
					|| !hostState.equals("dedicated") || !cacheState.equals("cold")
					|| !Set.of("hdd-zfs", "hdd-btrfs", "nvme").contains(storageLabel)
					|| rounds < 5)) {
				failures.add("release provenance is not enforced clean/dedicated/cold hardware");
			}
			if (!acceptancePassed) failures.add("correctness acceptance failed");
			if (!integrityPassed) failures.add("round-trip integrity failed");
			if (!requestsConserved) failures.add("request conservation failed");
			if (!resourcesDrained) failures.add("queues/resources did not drain");
			if (!shutdownClean) failures.add("shutdown was not clean");
			if (!telemetryAvailable) failures.add("runtime telemetry is unavailable");
			if (nativeLeaks != 0L) failures.add("native leaks=" + nativeLeaks);
			if (cancellations <= 0L) failures.add("no queued cancellation was observed");
			if (rounds < 1) failures.add("no alternating phase rounds were recorded");
			if (!fullSha(buildId)) failures.add("build ID is not a full lowercase SHA");
			if (!datasetFingerprint.matches("[0-9a-f]{64}")
					|| !comparisonFingerprint.matches("[0-9a-f]{64}")
					|| !environmentFingerprint.matches("[0-9a-f]{64}")) {
				failures.add("dataset/workload/environment fingerprints are malformed");
			}
			if (processId <= 0L) failures.add("process ID is unavailable");
			Instant parsedProcessStart = parseInstantOrNull(processStart);
			Instant parsedRunStarted = parseInstantOrNull(runStarted);
			Instant parsedRunFinished = parseInstantOrNull(runFinished);
			if (parsedProcessStart == null) failures.add("process start is unavailable");
			if (parsedRunStarted == null || parsedRunFinished == null) {
				failures.add("run interval is unavailable");
			} else {
				if (!parsedRunStarted.isBefore(parsedRunFinished)) {
					failures.add("run interval is empty or reversed");
				}
				if (parsedProcessStart != null && parsedProcessStart.isAfter(parsedRunStarted)) {
					failures.add("run started before its fresh subprocess");
				}
			}
			if (!metrics.keySet().equals(METRIC_BY_NAME.keySet())) {
				failures.add("metric schema mismatch");
			} else {
				for (MetricSpec metric : METRICS) {
					double value = metrics.get(metric.name());
					if (!Double.isFinite(value)
							|| (metric.direction() == Direction.NO_INCREASE
							? value < 0.0d || value != Math.rint(value) : value <= 0.0d)) {
						failures.add("invalid metric " + metric.name() + '=' + value);
					}
				}
			}
			return List.copyOf(failures);
		}
	}

	public record MetricSamples(double[] baseline, double[] candidate) {

		public MetricSamples {
			baseline = baseline.clone();
			candidate = candidate.clone();
		}

		@Override
		public double[] baseline() {
			return baseline.clone();
		}

		@Override
		public double[] candidate() {
			return candidate.clone();
		}
	}

	public record MetricComparison(String name,
			String direction,
			double[] baseline,
			double[] candidate,
			PairedBenchmarkStatistics.RatioConfidenceInterval ratio,
			boolean automaticPass,
			boolean withinExceptionCeiling,
			boolean materialImprovement) {

		public MetricComparison {
			baseline = baseline.clone();
			candidate = candidate.clone();
		}

		@Override
		public double[] baseline() {
			return baseline.clone();
		}

		@Override
		public double[] candidate() {
			return candidate.clone();
		}
	}

	public record Comparison(String schema,
			Instant generated,
			Path manifest,
			Instant declaredAt,
			String baselineBuild,
			String candidateBuild,
			int pairs,
			Set<String> primaryMetrics,
			String datasetFingerprint,
			String comparisonFingerprint,
			String environmentFingerprint,
			String environmentSummary,
			List<MetricComparison> metrics,
			List<String> failures) {

		public Comparison {
			primaryMetrics = Set.copyOf(primaryMetrics);
			metrics = List.copyOf(metrics);
			failures = List.copyOf(failures);
		}

		public boolean passed() {
			return failures.isEmpty()
					&& metrics.stream().allMatch(MetricComparison::automaticPass)
					&& metrics.stream().anyMatch(MetricComparison::materialImprovement);
		}

		public String failedSummary() {
			return failures.isEmpty() ? "none" : String.join("; ", failures);
		}
	}
}
