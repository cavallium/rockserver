package it.cavallium.rockserver.core.impl.benchmark;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Read-only precision planner for an immutable, completed v2 pressure comparison.
 *
 * <p>The planner uses every predeclared stochastic metric and only its observed paired-log
 * variance. Power is planned at exact equality, never at the observed favorable point estimate.
 * It writes a fixed next-run design and does not alter or reinterpret the completed decision.</p>
 */
public final class PressurePerformancePrecisionPlanner {

	static final String SCHEMA = "rockserver-pressure-performance-precision-plan-v2";
	static final double GLOBAL_PASS_POWER = 0.90d;
	static final double CRITICAL_BETA_BUDGET = 0.03d;
	static final double STANDARD_BETA_BUDGET = 0.07d;
	static final int MAXIMUM_PLANNED_PAIRS = 1_000_000;
	static final long MAXIMUM_PER_WORKER_DURATION_SCALE = 64L;
	private static final String JSON_FILE = "precision-plan.json";
	private static final String MARKDOWN_FILE = "precision-plan.md";
	private static final String NEXT_RUN_FILE = "next-run-v2.properties";
	private static final String SIGNAL_CLASS =
			"it/cavallium/rockserver/core/impl/StoragePressureSignal.class";
	private static final Pattern TOP_LEVEL_DECISION = Pattern.compile(
			"\\\"decision\\\"\\s*:\\s*\\\"(pass|fail|inconclusive)\\\"");

	private PressurePerformancePrecisionPlanner() {
	}

	public static void main(String[] args) throws Exception {
		Map<String, String> arguments = arguments(args);
		Path root = requiredPath(arguments, "root");
		Path baselineClasses = requiredPath(arguments, "baseline-classes");
		Path candidateClasses = requiredPath(arguments, "candidate-classes");
		if (!arguments.keySet().equals(Set.of("root", "baseline-classes", "candidate-classes"))) {
			throw new IllegalArgumentException("Planner accepts only --root, --baseline-classes, and --candidate-classes");
		}
		plan(root, baselineClasses, candidateClasses);
	}

	private static void plan(Path root, Path baselineClasses, Path candidateClasses) throws Exception {
		for (String output : List.of(JSON_FILE, MARKDOWN_FILE, NEXT_RUN_FILE)) {
			if (Files.exists(root.resolve(output))) {
				throw new IllegalArgumentException("Precision output already exists: " + root.resolve(output));
			}
		}
		var prepared = PressurePerformancePairedBenchmark.Prepared.read(root);
		if (prepared.contractVersion() != PressurePerformancePairedBenchmark.ContractVersion.V2) {
			throw new IllegalArgumentException("Precision planning requires an immutable v2 schedule");
		}
		String resultsText = Files.readString(root.resolve("results.json"));
		if (!resultsText.contains("\"schema\": \"rockserver-pressure-performance-comparison-v2\"")) {
			throw new IllegalArgumentException("Precision planning requires a v2 comparison result");
		}
		var decisionMatcher = TOP_LEVEL_DECISION.matcher(resultsText);
		if (!decisionMatcher.find() || !decisionMatcher.group(1).equals("inconclusive")) {
			throw new IllegalArgumentException("Precision planning is reserved for a completed v2 INCONCLUSIVE result");
		}

		var measurements = readMeasurements(prepared);
		var schedulerEvaluation = PressurePerformanceContractV2.evaluateScheduler(
				measurements.baseline().get(PressureBenchmarkArtifact.Suite.SCHEDULER),
				measurements.candidate().get(PressureBenchmarkArtifact.Suite.SCHEDULER), List.of());
		var signalEvaluation = PressurePerformanceContractV2.evaluateSignal(
				prepared.signalColumnFamilyCounts(),
				measurements.baseline().get(PressureBenchmarkArtifact.Suite.SIGNAL),
				measurements.candidate().get(PressureBenchmarkArtifact.Suite.SIGNAL), List.of());
		if (combinedDecision(schedulerEvaluation.decision(), signalEvaluation.decision())
				!= PairedPerformanceContractV2.Decision.INCONCLUSIVE) {
			throw new IllegalArgumentException("Current worker artifacts no longer reproduce the recorded INCONCLUSIVE decision");
		}

		var specifications = specifications(prepared.signalColumnFamilyCounts());
		int stochasticMetrics = Math.toIntExact(specifications.stream()
				.filter(item -> item.specification().direction()
						!= PairedPerformanceContractV2.Direction.DETERMINISTIC_NO_INCREASE)
				.count());
		int criticalMetrics = Math.toIntExact(specifications.stream()
				.filter(item -> item.specification().direction()
						!= PairedPerformanceContractV2.Direction.DETERMINISTIC_NO_INCREASE)
				.filter(item -> isSchedulingQualityMetric(item.specification().name()))
				.count());
		int standardMetrics = stochasticMetrics - criticalMetrics;
		double criticalTargetPower = 1.0d - CRITICAL_BETA_BUDGET / criticalMetrics;
		double standardTargetPower = 1.0d - STANDARD_BETA_BUDGET / standardMetrics;
		var metrics = new ArrayList<MetricPlan>();
		for (var item : specifications) {
			if (item.specification().direction()
					== PairedPerformanceContractV2.Direction.DETERMINISTIC_NO_INCREASE) continue;
			var baseline = values(measurements.baseline().get(item.suite()), item.specification().name());
			var candidate = values(measurements.candidate().get(item.suite()), item.specification().name());
			boolean critical = isSchedulingQualityMetric(item.specification().name());
			metrics.add(metricPlan(item.suite(), item.specification(), baseline, candidate,
					critical, critical ? criticalTargetPower : standardTargetPower));
		}
		double schedulerRequestedScale = maximumScale(metrics, PressureBenchmarkArtifact.Suite.SCHEDULER);
		double signalRequestedScale = maximumScale(metrics, PressureBenchmarkArtifact.Suite.SIGNAL);
		long schedulerScaleInteger = boundedDurationScale(prepared,
				PressureBenchmarkArtifact.Suite.SCHEDULER, schedulerRequestedScale);
		long signalScaleInteger = boundedDurationScale(prepared,
				PressureBenchmarkArtifact.Suite.SIGNAL, signalRequestedScale);
		metrics = metrics.stream().map(metric -> metric.withRecommendedDuration(
				metric.suite().equals(PressureBenchmarkArtifact.Suite.SCHEDULER.value)
						? schedulerScaleInteger : signalScaleInteger)).collect(java.util.stream.Collectors.toCollection(ArrayList::new));
		metrics.sort(Comparator.comparingInt(MetricPlan::requiredPairsAtRecommendedDuration).reversed()
				.thenComparing(Comparator.comparingInt(MetricPlan::requiredFixedPairs).reversed())
				.thenComparing(Comparator.comparing(MetricPlan::critical).reversed())
				.thenComparing(MetricPlan::name));

		int requiredFixedPairs = metrics.stream().mapToInt(MetricPlan::requiredFixedPairs).max()
				.orElse(PairedPerformanceContractV2.REQUIRED_PAIRS);
		int recommendedFixedPairs = metrics.stream().mapToInt(MetricPlan::requiredPairsAtRecommendedDuration)
				.max().orElse(PairedPerformanceContractV2.REQUIRED_PAIRS);
		var next = NextRun.from(prepared, recommendedFixedPairs,
				schedulerScaleInteger, signalScaleInteger);
		var component = componentEvidence(baselineClasses, candidateClasses);
		String resultsSha = sha256(Files.readAllBytes(root.resolve("results.json")));
		String scheduleSha = sha256(Files.readAllBytes(root.resolve("schedule.tsv")));
		String metadataSha = sha256(Files.readAllBytes(root.resolve("metadata.properties")));

		Files.writeString(root.resolve(JSON_FILE), json(prepared, metrics, stochasticMetrics,
				criticalMetrics, criticalTargetPower, standardTargetPower,
				requiredFixedPairs, schedulerRequestedScale, signalRequestedScale, next, component,
				resultsSha, scheduleSha, metadataSha), StandardOpenOption.CREATE_NEW);
		Files.writeString(root.resolve(MARKDOWN_FILE), markdown(metrics, stochasticMetrics,
				criticalMetrics, criticalTargetPower, standardTargetPower,
				requiredFixedPairs, schedulerRequestedScale, signalRequestedScale, next, component),
				StandardOpenOption.CREATE_NEW);
		Files.writeString(root.resolve(NEXT_RUN_FILE), next.properties(), StandardOpenOption.CREATE_NEW);
	}

	private static Measurements readMeasurements(PressurePerformancePairedBenchmark.Prepared prepared)
			throws IOException {
		var baseline = new EnumMap<PressureBenchmarkArtifact.Suite,
				List<Map<String, Double>>>(PressureBenchmarkArtifact.Suite.class);
		var candidate = new EnumMap<PressureBenchmarkArtifact.Suite,
				List<Map<String, Double>>>(PressureBenchmarkArtifact.Suite.class);
		for (var suite : PressureBenchmarkArtifact.Suite.values()) {
			baseline.put(suite, new ArrayList<>());
			candidate.put(suite, new ArrayList<>());
		}
		var processIds = new LinkedHashSet<Long>();
		long priorFinish = Long.MIN_VALUE;
		String hostSha = null;
		String runtimeSha = null;
		for (var run : PressurePerformancePairedBenchmark.schedule(prepared)) {
			var names = PressurePerformancePairedBenchmark.metricNames(
					run.suite(), prepared.signalColumnFamilyCounts());
			var artifact = PressureBenchmarkArtifact.read(run.artifact(), run.suite(), names);
			String build = run.implementation() == PressureBenchmarkArtifact.Implementation.BASELINE
					? prepared.baselineSha() : prepared.candidateSha();
			if (artifact.round() != run.round() || artifact.ordinal() != run.ordinal()
					|| artifact.implementation() != run.implementation()
					|| !artifact.buildSha().equals(build)
					|| !artifact.configurationSha256().equals(prepared.configurationSha256())
					|| !artifact.correctnessPassed()) {
				throw new IllegalArgumentException("Worker identity/correctness mismatch at ordinal " + run.ordinal());
			}
			if (!processIds.add(artifact.processId()) || artifact.startedEpochMillis() < priorFinish) {
				throw new IllegalArgumentException("Worker process/order mismatch at ordinal " + run.ordinal());
			}
			priorFinish = artifact.finishedEpochMillis();
			if (hostSha == null) hostSha = artifact.hostSha256();
			if (runtimeSha == null) runtimeSha = artifact.runtimeSha256();
			if (!hostSha.equals(artifact.hostSha256()) || !runtimeSha.equals(artifact.runtimeSha256())) {
				throw new IllegalArgumentException("Worker host/runtime drift at ordinal " + run.ordinal());
			}
			var target = run.implementation() == PressureBenchmarkArtifact.Implementation.BASELINE
					? baseline : candidate;
			target.get(run.suite()).add(artifact.metrics());
		}
		return new Measurements(copy(baseline), copy(candidate));
	}

	private static Map<PressureBenchmarkArtifact.Suite, List<Map<String, Double>>> copy(
			Map<PressureBenchmarkArtifact.Suite, List<Map<String, Double>>> source) {
		var result = new EnumMap<PressureBenchmarkArtifact.Suite,
				List<Map<String, Double>>>(PressureBenchmarkArtifact.Suite.class);
		for (var entry : source.entrySet()) result.put(entry.getKey(), List.copyOf(entry.getValue()));
		return Map.copyOf(result);
	}

	private static List<SuiteMetric> specifications(int[] counts) {
		var result = new ArrayList<SuiteMetric>();
		for (var specification : PressurePerformanceContractV2.schedulerSpecifications()) {
			result.add(new SuiteMetric(PressureBenchmarkArtifact.Suite.SCHEDULER, specification));
		}
		for (var specification : PressurePerformanceContractV2.signalSpecifications(counts)) {
			result.add(new SuiteMetric(PressureBenchmarkArtifact.Suite.SIGNAL, specification));
		}
		return List.copyOf(result);
	}

	private static MetricPlan metricPlan(PressureBenchmarkArtifact.Suite suite,
			PairedPerformanceContractV2.MetricSpec specification,
			double[] baseline,
			double[] candidate,
			boolean critical,
			double targetPower) {
		if (baseline.length != PairedPerformanceContractV2.REQUIRED_PAIRS
				|| candidate.length != PairedPerformanceContractV2.REQUIRED_PAIRS) {
			throw new IllegalArgumentException("Planner requires the complete fixed ten-pair v2 vectors");
		}
		double[] logs = new double[baseline.length];
		double sum = 0.0d;
		for (int pair = 0; pair < logs.length; pair++) {
			double base = baseline[pair] + specification.ratioOffset();
			double next = candidate[pair] + specification.ratioOffset();
			if (!Double.isFinite(base) || base <= 0.0d || !Double.isFinite(next) || next <= 0.0d) {
				throw new IllegalArgumentException("Invalid planner sample for " + specification.name());
			}
			logs[pair] = Math.log(next / base);
			sum += logs[pair];
		}
		double mean = sum / logs.length;
		double squared = 0.0d;
		for (double value : logs) {
			double deviation = value - mean;
			squared += deviation * deviation;
		}
		double standardDeviation = Math.sqrt(squared / (logs.length - 1.0d));
		double marginLog = specification.direction() == PairedPerformanceContractV2.Direction.HIGHER_IS_BETTER
				? -Math.log(PairedPerformanceContractV2.THROUGHPUT_MINIMUM_RATIO)
				: Math.log(PairedPerformanceContractV2.COST_MAXIMUM_RATIO);
		double currentPower = approximateNonInferiorityPower(logs.length, standardDeviation, marginLog);
		int requiredPairs = requiredPairs(standardDeviation, marginLog, targetPower);
		double durationScale = durationScale(logs.length, standardDeviation, marginLog, targetPower);
		return new MetricPlan(suite.value,
				specification.name(),
				critical,
				specification.direction().name().toLowerCase(Locale.ROOT),
				specification.ratioOffset(),
				standardDeviation,
				marginLog,
				currentPower,
				targetPower,
				requiredPairs,
				durationScale,
				1L,
				requiredPairs);
	}

	private static boolean isSchedulingQualityMetric(String name) {
		return name.endsWith("queue_p99_nanos")
				|| name.endsWith("end_to_end_p99_nanos")
				|| name.endsWith("maximum_progress_gap_nanos");
	}

	static double approximateNonInferiorityPower(int pairs, double pairedLogStandardDeviation,
			double marginLog) {
		if (pairs < 2 || !Double.isFinite(pairedLogStandardDeviation)
				|| pairedLogStandardDeviation < 0.0d || !Double.isFinite(marginLog) || marginLog <= 0.0d) {
			throw new IllegalArgumentException("Invalid precision-planning inputs");
		}
		if (pairedLogStandardDeviation == 0.0d) return 1.0d;
		double critical = studentTQuantile(1.0d - PairedPerformanceContractV2.FAMILY_WISE_ALPHA, pairs - 1);
		double signal = marginLog * Math.sqrt(pairs) / pairedLogStandardDeviation;
		return normalCdf(signal - critical);
	}

	static int requiredPairs(double pairedLogStandardDeviation, double marginLog, double targetPower) {
		if (!Double.isFinite(targetPower) || targetPower <= 0.5d || targetPower >= 1.0d) {
			throw new IllegalArgumentException("Target power must be between 0.5 and 1");
		}
		if (pairedLogStandardDeviation == 0.0d) return PairedPerformanceContractV2.REQUIRED_PAIRS;
		int low = PairedPerformanceContractV2.REQUIRED_PAIRS;
		int high = PairedPerformanceContractV2.REQUIRED_PAIRS;
		while (approximateNonInferiorityPower(high, pairedLogStandardDeviation, marginLog) < targetPower
				&& high < MAXIMUM_PLANNED_PAIRS) {
			low = high + 1;
			high = Math.min(MAXIMUM_PLANNED_PAIRS, Math.multiplyExact(high, 2));
		}
		if (approximateNonInferiorityPower(high, pairedLogStandardDeviation, marginLog) < targetPower) {
			return MAXIMUM_PLANNED_PAIRS;
		}
		while (low < high) {
			int middle = low + (high - low) / 2;
			if (approximateNonInferiorityPower(middle, pairedLogStandardDeviation, marginLog) >= targetPower) {
				high = middle;
			} else {
				low = middle + 1;
			}
		}
		return low;
	}

	static double durationScale(int fixedPairs, double pairedLogStandardDeviation,
			double marginLog, double targetPower) {
		if (pairedLogStandardDeviation == 0.0d) return 1.0d;
		double critical = studentTQuantile(1.0d - PairedPerformanceContractV2.FAMILY_WISE_ALPHA,
				fixedPairs - 1);
		double requiredSignal = critical + normalQuantile(targetPower);
		double currentSignal = marginLog * Math.sqrt(fixedPairs) / pairedLogStandardDeviation;
		double scale = Math.pow(requiredSignal / currentSignal, 2.0d);
		return Math.max(1.0d, scale);
	}

	static double studentTQuantile(double probability, int degreesOfFreedom) {
		if (!Double.isFinite(probability) || probability <= 0.0d || probability >= 1.0d
				|| degreesOfFreedom < 1) {
			throw new IllegalArgumentException("Invalid Student-t quantile inputs");
		}
		double low = -64.0d;
		double high = 64.0d;
		for (int iteration = 0; iteration < 160; iteration++) {
			double middle = (low + high) / 2.0d;
			if (PairedPerformanceContractV2.studentTCdf(middle, degreesOfFreedom) < probability) low = middle;
			else high = middle;
		}
		return (low + high) / 2.0d;
	}

	static double normalQuantile(double probability) {
		if (!Double.isFinite(probability) || probability <= 0.0d || probability >= 1.0d) {
			throw new IllegalArgumentException("Invalid normal quantile probability");
		}
		double low = -10.0d;
		double high = 10.0d;
		for (int iteration = 0; iteration < 160; iteration++) {
			double middle = (low + high) / 2.0d;
			if (normalCdf(middle) < probability) low = middle;
			else high = middle;
		}
		return (low + high) / 2.0d;
	}

	static double normalCdf(double value) {
		double absolute = Math.abs(value);
		double t = 1.0d / (1.0d + 0.2316419d * absolute);
		double density = 0.3989422804014327d * Math.exp(-0.5d * absolute * absolute);
		double tail = density * t * (0.319381530d + t * (-0.356563782d
				+ t * (1.781477937d + t * (-1.821255978d + t * 1.330274429d))));
		double cdf = 1.0d - tail;
		return value >= 0.0d ? cdf : 1.0d - cdf;
	}

	private static double maximumScale(List<MetricPlan> metrics, PressureBenchmarkArtifact.Suite suite) {
		return metrics.stream().filter(metric -> metric.suite().equals(suite.value))
				.mapToDouble(MetricPlan::fixedPairDurationScale).max().orElse(1.0d);
	}

	private static long boundedDurationScale(PressurePerformancePairedBenchmark.Prepared prepared,
			PressureBenchmarkArtifact.Suite suite,
			double requestedScale) {
		if (!Double.isFinite(requestedScale) || requestedScale > Long.MAX_VALUE) {
			throw new IllegalArgumentException("Required duration scaling is not executable");
		}
		long fieldLimit = suite == PressureBenchmarkArtifact.Suite.SCHEDULER
				? Integer.MAX_VALUE / (long) prepared.schedulerOperations()
				: Math.min(Long.MAX_VALUE / prepared.signalMeasuredColumns(),
						Math.min(Integer.MAX_VALUE / (long) prepared.signalMinimumEvaluations(),
								Integer.MAX_VALUE / (long) prepared.signalMaximumEvaluations()));
		long maximum = Math.max(1L, Math.min(MAXIMUM_PER_WORKER_DURATION_SCALE, fieldLimit));
		return Math.max(1L, Math.min(maximum, (long) Math.ceil(requestedScale)));
	}

	private static double[] values(List<Map<String, Double>> runs, String metric) {
		double[] values = new double[runs.size()];
		for (int pair = 0; pair < runs.size(); pair++) values[pair] = runs.get(pair).get(metric);
		return values;
	}

	private static PairedPerformanceContractV2.Decision combinedDecision(
			PairedPerformanceContractV2.Decision first,
			PairedPerformanceContractV2.Decision second) {
		if (first == PairedPerformanceContractV2.Decision.FAIL
				|| second == PairedPerformanceContractV2.Decision.FAIL) return PairedPerformanceContractV2.Decision.FAIL;
		if (first == PairedPerformanceContractV2.Decision.INCONCLUSIVE
				|| second == PairedPerformanceContractV2.Decision.INCONCLUSIVE) {
			return PairedPerformanceContractV2.Decision.INCONCLUSIVE;
		}
		return PairedPerformanceContractV2.Decision.PASS;
	}

	private static ComponentEvidence componentEvidence(Path baselineRoot, Path candidateRoot) throws Exception {
		Path baseline = baselineRoot.resolve(SIGNAL_CLASS);
		Path candidate = candidateRoot.resolve(SIGNAL_CLASS);
		if (!Files.isRegularFile(baseline) || !Files.isRegularFile(candidate)) {
			throw new IllegalArgumentException("Both class roots must contain " + SIGNAL_CLASS);
		}
		String baselineSha = sha256(Files.readAllBytes(baseline));
		String candidateSha = sha256(Files.readAllBytes(candidate));
		return new ComponentEvidence(SIGNAL_CLASS, baselineSha, candidateSha,
				baselineSha.equals(candidateSha), false);
	}

	private static String json(PressurePerformancePairedBenchmark.Prepared prepared,
			List<MetricPlan> metrics,
			int stochasticMetrics,
			int criticalMetrics,
			double criticalTargetPower,
			double standardTargetPower,
			int requiredFixedPairs,
			double schedulerScale,
			double signalScale,
			NextRun next,
			ComponentEvidence component,
			String resultsSha,
			String scheduleSha,
			String metadataSha) {
		var text = new StringBuilder("{\n  \"schema\": \"").append(SCHEMA).append("\",\n")
				.append("  \"source_contract\": \"v2\",\n")
				.append("  \"source_decision\": \"inconclusive\",\n")
				.append("  \"source_configuration_sha256\": \"").append(prepared.configurationSha256()).append("\",\n")
				.append("  \"source_results_sha256\": \"").append(resultsSha).append("\",\n")
				.append("  \"source_schedule_sha256\": \"").append(scheduleSha).append("\",\n")
				.append("  \"source_metadata_sha256\": \"").append(metadataSha).append("\",\n")
				.append("  \"planning_effect\": \"exact-equality\",\n")
				.append("  \"power_method\": \"student-critical-normal-approximation-v1\",\n")
				.append("  \"global_pass_power\": ").append(GLOBAL_PASS_POWER).append(",\n")
				.append("  \"stochastic_metrics\": ").append(stochasticMetrics).append(",\n")
				.append("  \"critical_scheduling_metrics\": ").append(criticalMetrics).append(",\n")
				.append("  \"critical_beta_budget\": ").append(CRITICAL_BETA_BUDGET).append(",\n")
				.append("  \"standard_beta_budget\": ").append(STANDARD_BETA_BUDGET).append(",\n")
				.append("  \"critical_target_per_metric_power\": ").append(format(criticalTargetPower)).append(",\n")
				.append("  \"standard_target_per_metric_power\": ").append(format(standardTargetPower)).append(",\n")
				.append("  \"required_fixed_pairs_same_duration\": ").append(requiredFixedPairs).append(",\n")
				.append("  \"maximum_per_worker_duration_scale\": ")
				.append(MAXIMUM_PER_WORKER_DURATION_SCALE).append(",\n")
				.append("  \"source_fixed_pairs\": ").append(prepared.fixedPairs())
				.append(",\n  \"adaptive_stopping\": false,\n")
				.append("  \"throughput_minimum_ratio\": 0.99,\n  \"cost_maximum_ratio\": 1.02,\n")
				.append("  \"scheduler_duration_scale\": ").append(format(schedulerScale)).append(",\n")
				.append("  \"signal_duration_scale\": ").append(format(signalScale)).append(",\n")
				.append("  \"signal_component\": {\"path\":\"").append(component.path())
				.append("\",\"baseline_sha256\":\"").append(component.baselineSha256())
				.append("\",\"candidate_sha256\":\"").append(component.candidateSha256())
				.append("\",\"equal\":").append(component.equal())
				.append(",\"used_for_decision\":false},\n")
				.append("  \"next_run\": ").append(next.json()).append(",\n")
				.append("  \"limiting_metrics\": [");
		for (int index = 0; index < Math.min(10, metrics.size()); index++) {
			if (index > 0) text.append(',');
			text.append('"').append(metrics.get(index).name()).append('"');
		}
		text.append("],\n  \"metrics\": [\n");
		for (int index = 0; index < metrics.size(); index++) {
			if (index > 0) text.append(",\n");
			text.append("    ").append(metrics.get(index).json());
		}
		return text.append("\n  ]\n}\n").toString();
	}

	private static String markdown(List<MetricPlan> metrics,
			int stochasticMetrics,
			int criticalMetrics,
			double criticalTargetPower,
			double standardTargetPower,
			int requiredFixedPairs,
			double schedulerScale,
			double signalScale,
			NextRun next,
			ComponentEvidence component) {
		var text = new StringBuilder("# Pressure-performance precision plan\n\n")
				.append("- Source decision: `INCONCLUSIVE` (unchanged)\n")
				.append("- Planning effect: exact equality\n")
				.append("- Global all-metric power target: `0.90`\n")
				.append("- Stochastic metrics: `").append(stochasticMetrics).append("`\n")
				.append("- Critical scheduling-quality metrics: `").append(criticalMetrics).append("`\n")
				.append("- Critical / standard beta budgets: `0.03` / `0.07`\n")
				.append("- Critical / standard per-metric power: `").append(format(criticalTargetPower))
				.append("` / `").append(format(standardTargetPower)).append("`\n")
				.append("- Source fixed pairs / adaptive stopping: `10` / `false`\n")
				.append("- Maximum per-worker duration scale: `")
				.append(MAXIMUM_PER_WORKER_DURATION_SCALE).append("`\n")
				.append("- Required fixed pairs at unchanged duration: `").append(requiredFixedPairs).append("`\n")
				.append("- Requested scheduler / signal duration scale: `").append(format(schedulerScale))
				.append("` / `").append(format(signalScale)).append("`\n")
				.append("- Recommended fixed pairs: `").append(next.fixedPairs()).append("`\n")
				.append("- Bounded scheduler / signal duration scale: `")
				.append(next.schedulerDurationScale()).append("` / `")
				.append(next.signalDurationScale()).append("`\n")
				.append("- Signal evaluator component bytes equal: `").append(component.equal())
				.append("` (provenance only; never used for PASS)\n\n")
				.append("## Limiting metrics\n\n")
				.append("| Metric | Suite | Critical | Paired-log SD | Current NI power | Same-duration pairs | Bounded duration | Final pairs |\n")
				.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |\n");
		for (int index = 0; index < Math.min(20, metrics.size()); index++) {
			var metric = metrics.get(index);
			text.append("| `").append(metric.name()).append("` | ").append(metric.suite()).append(" | ")
					.append(metric.critical()).append(" | ")
					.append(format(metric.pairedLogStandardDeviation())).append(" | ")
					.append(format(metric.currentNonInferiorityPower())).append(" | ")
					.append(metric.requiredFixedPairs()).append(" | ")
					.append(metric.recommendedDurationScale()).append(" | ")
					.append(metric.requiredPairsAtRecommendedDuration()).append(" |\n");
		}
		return text.append("\n## Fixed next run\n\n```properties\n")
				.append(next.properties())
				.append("```\n").toString();
	}

	private static String format(double value) {
		return String.format(Locale.ROOT, "%.12f", value);
	}

	private static String sha256(byte[] bytes) {
		try {
			return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
		} catch (java.security.NoSuchAlgorithmException impossible) {
			throw new ExceptionInInitializerError(impossible);
		}
	}

	private static Map<String, String> arguments(String[] args) {
		var values = new LinkedHashMap<String, String>();
		for (String argument : args) {
			if (!argument.startsWith("--") || !argument.contains("=")) {
				throw new IllegalArgumentException("Options must use --name=value: " + argument);
			}
			int separator = argument.indexOf('=');
			String key = argument.substring(2, separator);
			if (values.put(key, argument.substring(separator + 1)) != null) {
				throw new IllegalArgumentException("Duplicate option --" + key);
			}
		}
		return Map.copyOf(values);
	}

	private static Path requiredPath(Map<String, String> values, String key) {
		String value = values.get(key);
		if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing --" + key);
		return Path.of(value).toAbsolutePath().normalize();
	}

	private record Measurements(
			Map<PressureBenchmarkArtifact.Suite, List<Map<String, Double>>> baseline,
			Map<PressureBenchmarkArtifact.Suite, List<Map<String, Double>>> candidate) {
	}

	private record SuiteMetric(PressureBenchmarkArtifact.Suite suite,
			PairedPerformanceContractV2.MetricSpec specification) {
	}

	record MetricPlan(String suite,
	                  String name,
	                  boolean critical,
	                  String direction,
	                  double ratioOffset,
	                  double pairedLogStandardDeviation,
	                  double marginLog,
	                  double currentNonInferiorityPower,
	                  double targetPower,
	                  int requiredFixedPairs,
	                  double fixedPairDurationScale,
	                  long recommendedDurationScale,
	                  int requiredPairsAtRecommendedDuration) {

		private MetricPlan withRecommendedDuration(long durationScale) {
			int pairs = requiredPairs(pairedLogStandardDeviation / Math.sqrt(durationScale),
					marginLog, targetPower);
			return new MetricPlan(suite, name, critical, direction, ratioOffset,
					pairedLogStandardDeviation, marginLog, currentNonInferiorityPower, targetPower,
					requiredFixedPairs, fixedPairDurationScale, durationScale, pairs);
		}

		private String json() {
			return "{\"suite\":\"" + suite + "\",\"name\":\"" + name
					+ "\",\"critical\":" + critical
					+ ",\"direction\":\"" + direction + "\",\"ratio_offset\":" + format(ratioOffset)
					+ ",\"paired_log_sd\":" + format(pairedLogStandardDeviation)
					+ ",\"margin_log\":" + format(marginLog)
					+ ",\"current_ni_power\":" + format(currentNonInferiorityPower)
					+ ",\"target_power\":" + format(targetPower)
					+ ",\"required_fixed_pairs\":" + requiredFixedPairs
					+ ",\"fixed_pair_duration_scale\":" + format(fixedPairDurationScale)
					+ ",\"recommended_duration_scale\":" + recommendedDurationScale
					+ ",\"required_pairs_at_recommended_duration\":"
					+ requiredPairsAtRecommendedDuration + '}';
		}
	}

	private record ComponentEvidence(String path,
			String baselineSha256,
			String candidateSha256,
			boolean equal,
			boolean usedForDecision) {
	}

	private record NextRun(int fixedPairs,
			long schedulerOperations,
			long signalMeasuredColumns,
			long signalMinimumEvaluations,
			long signalMaximumEvaluations,
			long schedulerDurationScale,
			long signalDurationScale,
			PressurePerformancePairedBenchmark.Prepared source) {

		private static NextRun from(PressurePerformancePairedBenchmark.Prepared source,
				int fixedPairs,
				long schedulerScale,
				long signalScale) {
			long schedulerOperations = Math.multiplyExact((long) source.schedulerOperations(), schedulerScale);
			long signalMinimum = Math.multiplyExact((long) source.signalMinimumEvaluations(), signalScale);
			long signalMaximum = Math.multiplyExact((long) source.signalMaximumEvaluations(), signalScale);
			Math.toIntExact(schedulerOperations);
			Math.toIntExact(signalMinimum);
			Math.toIntExact(signalMaximum);
			return new NextRun(fixedPairs,
					schedulerOperations,
					Math.multiplyExact(source.signalMeasuredColumns(), signalScale),
					signalMinimum,
					signalMaximum,
					schedulerScale,
					signalScale,
					source);
		}

		private String json() {
			return "{\"contract_version\":\"v2.1\",\"fixed_pairs\":" + fixedPairs
					+ ",\"fresh_processes\":" + Math.multiplyExact(fixedPairs, 4)
					+ ",\"adaptive_stopping\":false"
					+ ",\"scheduler_duration_scale\":" + schedulerDurationScale
					+ ",\"signal_duration_scale\":" + signalDurationScale
					+ ",\"scheduler_operations\":" + schedulerOperations
					+ ",\"signal_measured_columns\":" + signalMeasuredColumns
					+ ",\"signal_minimum_evaluations\":" + signalMinimumEvaluations
					+ ",\"signal_maximum_evaluations\":" + signalMaximumEvaluations + '}';
		}

		private String properties() {
			return "contract-version=v2.1\nfixed-pairs=" + fixedPairs + '\n'
					+ "planning-duration-scale-cap=" + MAXIMUM_PER_WORKER_DURATION_SCALE + '\n'
					+ "baseline-sha=" + source.baselineSha() + '\n'
					+ "candidate-sha=" + source.candidateSha() + '\n'
					+ "host-state=" + source.hostState() + '\n'
					+ "hardware-description=" + source.hardwareDescription() + '\n'
					+ "enforce=" + source.enforce() + '\n'
					+ "scheduler-operations=" + schedulerOperations + '\n'
					+ "scheduler-submitters=" + source.schedulerSubmitters() + '\n'
					+ "scheduler-read-workers=" + source.schedulerReadWorkers() + '\n'
					+ "scheduler-write-workers=" + source.schedulerWriteWorkers() + '\n'
					+ "scheduler-analytical-limit=" + source.schedulerAnalyticalLimit() + '\n'
					+ "scheduler-foreground-capacity=" + source.schedulerForegroundCapacity() + '\n'
					+ "scheduler-batch-capacity=" + source.schedulerBatchCapacity() + '\n'
					+ "scheduler-work-tokens=" + source.schedulerWorkTokens() + '\n'
					+ "scheduler-warmup-operations=" + source.schedulerWarmupOperations() + '\n'
					+ "scheduler-seed=" + source.schedulerSeed() + '\n'
					+ "signal-cf-counts=" + java.util.Arrays.stream(source.signalColumnFamilyCounts())
							.mapToObj(Integer::toString).collect(java.util.stream.Collectors.joining(",")) + '\n'
					+ "signal-warmup-columns=" + source.signalWarmupColumns() + '\n'
					+ "signal-measured-columns=" + signalMeasuredColumns + '\n'
					+ "signal-minimum-evaluations=" + signalMinimumEvaluations + '\n'
					+ "signal-maximum-evaluations=" + signalMaximumEvaluations + '\n'
					+ "signal-latency-sample-stride=" + source.signalLatencySampleStride() + '\n';
		}
	}
}
