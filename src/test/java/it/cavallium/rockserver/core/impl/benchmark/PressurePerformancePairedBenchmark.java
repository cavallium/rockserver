package it.cavallium.rockserver.core.impl.benchmark;

import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Executable immutable 10-pair workflow for scheduler and storage-pressure microbenchmarks. */
public final class PressurePerformancePairedBenchmark {

	static final String SCHEDULE_SCHEMA = "rockserver-pressure-performance-schedule-v1";
	static final String RESULT_SCHEMA = "rockserver-pressure-performance-comparison-v1";
	static final String METADATA_SCHEMA = "rockserver-pressure-performance-metadata-v1";
	private static final String SCHEDULE_FILE = "schedule.tsv";
	private static final String METADATA_FILE = "metadata.properties";
	private static final Set<String> METADATA_KEYS = Set.of(
			"schema", "baseline-sha", "candidate-sha", "host-state", "hardware-description", "enforce",
			"scheduler-operations", "scheduler-submitters", "scheduler-read-workers",
			"scheduler-write-workers", "scheduler-analytical-limit", "scheduler-foreground-capacity",
			"scheduler-batch-capacity", "scheduler-work-tokens", "scheduler-warmup-operations",
			"scheduler-seed", "signal-cf-counts", "signal-warmup-columns", "signal-measured-columns",
			"signal-minimum-evaluations", "signal-maximum-evaluations", "signal-latency-sample-stride");

	private PressurePerformancePairedBenchmark() {
	}

	public static void main(String[] args) throws Exception {
		Map<String, String> values = arguments(args);
		Mode mode = Mode.parse(values.getOrDefault("mode", "evaluate"));
		switch (mode) {
			case PREPARE -> prepare(Prepared.fromArguments(values));
			case WORKER -> worker(values);
			case EVALUATE -> evaluate(requiredRoot(values, Set.of("mode", "root")));
		}
	}

	private static void prepare(Prepared prepared) throws IOException {
		if (Files.exists(prepared.root())) {
			throw new IllegalArgumentException("Pressure benchmark root already exists: " + prepared.root());
		}
		Files.createDirectories(prepared.root());
		Files.writeString(prepared.root().resolve(METADATA_FILE), prepared.metadataText(),
				StandardOpenOption.CREATE_NEW);
		Files.writeString(prepared.root().resolve(SCHEDULE_FILE), scheduleText(prepared),
				StandardOpenOption.CREATE_NEW);
		System.out.println("Prepared immutable pressure benchmark schedule: "
				+ prepared.root().resolve(SCHEDULE_FILE));
	}

	private static void worker(Map<String, String> arguments) throws Exception {
		Path root = requiredRoot(arguments,
				Set.of("mode", "root", "suite", "round", "implementation", "build-sha"));
		Prepared prepared = Prepared.read(root);
		if (!Files.readString(root.resolve(SCHEDULE_FILE)).equals(scheduleText(prepared))) {
			throw new IllegalArgumentException("Pressure benchmark schedule changed before worker execution");
		}
		var suite = PressureBenchmarkArtifact.Suite.parse(require(arguments, "suite"));
		int round = integer(arguments, "round");
		var implementation = PressureBenchmarkArtifact.Implementation.parse(
				require(arguments, "implementation"));
		String buildSha = require(arguments, "build-sha");
		String expectedBuild = implementation == PressureBenchmarkArtifact.Implementation.BASELINE
				? prepared.baselineSha() : prepared.candidateSha();
		if (!expectedBuild.equals(buildSha)) throw new IllegalArgumentException("Worker build SHA mismatch");
		if (prepared.enforce()) verifyCheckout(buildSha);
		ScheduledRun run = scheduledRun(prepared, round, suite, implementation);
		assertWorkerOrder(prepared, run);
		Set<String> metricNames = metricNames(suite, prepared.signalColumnFamilyCounts());
		long started = System.currentTimeMillis();
		Map<String, Double> metrics;
		if (suite == PressureBenchmarkArtifact.Suite.SCHEDULER) {
			SchedulerHighContentionBenchmark.run(prepared.schedulerWarmupConfig());
			metrics = PressurePerformanceContract.schedulerMetrics(
					SchedulerHighContentionBenchmark.run(prepared.schedulerConfig()));
		} else {
			metrics = PressurePerformanceContract.signalMetrics(StoragePressureSignalBenchmark.run(
					prepared.signalConfig(buildSha)));
		}
		long finished = System.currentTimeMillis();
		String classPath = System.getProperty("java.class.path");
		String classPathSha = GrpcRetainedReadBenchmark.classPathContentSha256ForTesting(classPath);
		URI productionLocation = RWScheduler.class.getProtectionDomain().getCodeSource().getLocation().toURI();
		String productionSha = GrpcRetainedReadBenchmark.classPathContentSha256ForTesting(
				Path.of(productionLocation).toString());
		var artifact = new PressureBenchmarkArtifact.Artifact(suite,
				round,
				run.ordinal(),
				implementation,
				buildSha,
				prepared.configurationSha256(),
				hostSha(prepared.hardwareDescription()),
				prepared.hardwareDescription(),
				runtimeSha(),
				classPathSha,
				productionSha,
				ProcessHandle.current().pid(),
				started,
				finished,
				prepared.enforce(),
				true,
				metrics);
		PressureBenchmarkArtifact.write(run.artifact(), artifact, metricNames);
		System.out.println("Wrote pressure worker artifact " + run.artifact());
	}

	private static void evaluate(Path root) throws IOException {
		Prepared prepared = Prepared.read(root);
		if (!Files.readString(root.resolve(SCHEDULE_FILE)).equals(scheduleText(prepared))) {
			throw new IllegalArgumentException("Pressure benchmark schedule changed after preparation");
		}
		var baselineBySuite = new java.util.EnumMap<PressureBenchmarkArtifact.Suite,
				List<Map<String, Double>>>(PressureBenchmarkArtifact.Suite.class);
		var candidateBySuite = new java.util.EnumMap<PressureBenchmarkArtifact.Suite,
				List<Map<String, Double>>>(PressureBenchmarkArtifact.Suite.class);
		for (var suite : PressureBenchmarkArtifact.Suite.values()) {
			baselineBySuite.put(suite, new ArrayList<>());
			candidateBySuite.put(suite, new ArrayList<>());
		}
		var failures = new ArrayList<String>();
		var processIds = new LinkedHashSet<Long>();
		long previousFinished = Long.MIN_VALUE;
		String runtimeSha = null;
		String hostSha = null;
		String hardware = null;
		var baselineClasspath = new java.util.EnumMap<PressureBenchmarkArtifact.Suite, String>(
				PressureBenchmarkArtifact.Suite.class);
		var candidateClasspath = new java.util.EnumMap<PressureBenchmarkArtifact.Suite, String>(
				PressureBenchmarkArtifact.Suite.class);
		String baselineProduction = null;
		String candidateProduction = null;
		for (ScheduledRun run : schedule(prepared)) {
			Set<String> expectedMetrics = metricNames(run.suite(), prepared.signalColumnFamilyCounts());
			var artifact = PressureBenchmarkArtifact.read(run.artifact(), run.suite(), expectedMetrics);
			String expectedBuild = run.implementation() == PressureBenchmarkArtifact.Implementation.BASELINE
					? prepared.baselineSha() : prepared.candidateSha();
			if (artifact.round() != run.round() || artifact.ordinal() != run.ordinal()
					|| artifact.implementation() != run.implementation()) {
				failures.add("artifact schedule identity mismatch at ordinal " + run.ordinal());
			}
			if (!artifact.buildSha().equals(expectedBuild)) failures.add("build mismatch at ordinal " + run.ordinal());
			if (!artifact.configurationSha256().equals(prepared.configurationSha256())) {
				failures.add("configuration mismatch at ordinal " + run.ordinal());
			}
			if (prepared.enforce() && (!artifact.enforcedHardwareRun() || !artifact.correctnessPassed())) {
				failures.add("unenforced or incorrect worker at ordinal " + run.ordinal());
			}
			if (!processIds.add(artifact.processId())) failures.add("process id reused at ordinal " + run.ordinal());
			if (artifact.startedEpochMillis() < previousFinished) failures.add("worker order overlapped at ordinal " + run.ordinal());
			previousFinished = artifact.finishedEpochMillis();
			if (runtimeSha == null) runtimeSha = artifact.runtimeSha256();
			if (hostSha == null) hostSha = artifact.hostSha256();
			if (hardware == null) hardware = artifact.hardwareDescription();
			if (!runtimeSha.equals(artifact.runtimeSha256())) failures.add("runtime changed at ordinal " + run.ordinal());
			if (!hostSha.equals(artifact.hostSha256()) || !hardware.equals(artifact.hardwareDescription())) {
				failures.add("host changed at ordinal " + run.ordinal());
			}
			var classpaths = run.implementation() == PressureBenchmarkArtifact.Implementation.BASELINE
					? baselineClasspath : candidateClasspath;
			String priorClasspath = classpaths.putIfAbsent(run.suite(), artifact.classpathSha256());
			if (priorClasspath != null && !priorClasspath.equals(artifact.classpathSha256())) {
				failures.add(run.implementation().value + " classpath changed for " + run.suite().value);
			}
			if (run.implementation() == PressureBenchmarkArtifact.Implementation.BASELINE) {
				if (baselineProduction == null) baselineProduction = artifact.productionClassesSha256();
				else if (!baselineProduction.equals(artifact.productionClassesSha256())) failures.add("baseline production bytes changed");
				baselineBySuite.get(run.suite()).add(artifact.metrics());
			} else {
				if (candidateProduction == null) candidateProduction = artifact.productionClassesSha256();
				else if (!candidateProduction.equals(artifact.productionClassesSha256())) failures.add("candidate production bytes changed");
				candidateBySuite.get(run.suite()).add(artifact.metrics());
			}
		}
		var scheduler = PressurePerformanceContract.evaluateScheduler(
				baselineBySuite.get(PressureBenchmarkArtifact.Suite.SCHEDULER),
				candidateBySuite.get(PressureBenchmarkArtifact.Suite.SCHEDULER), failures);
		var signal = PressurePerformanceContract.evaluateSignal(prepared.signalColumnFamilyCounts(),
				baselineBySuite.get(PressureBenchmarkArtifact.Suite.SIGNAL),
				candidateBySuite.get(PressureBenchmarkArtifact.Suite.SIGNAL), failures);
		boolean passed = scheduler.automaticAcceptancePassed() && signal.automaticAcceptancePassed();
		Instant finished = Instant.now();
		Files.writeString(root.resolve("results.json"), resultJson(prepared, finished, scheduler, signal, passed),
				StandardOpenOption.CREATE_NEW);
		Files.writeString(root.resolve("results.md"), resultMarkdown(prepared, finished, scheduler, signal, passed),
				StandardOpenOption.CREATE_NEW);
		if (prepared.enforce() && !passed) {
			throw new IllegalStateException("Pressure performance comparison failed");
		}
	}

	private static void assertWorkerOrder(Prepared prepared, ScheduledRun target) {
		for (ScheduledRun run : schedule(prepared)) {
			if (run.ordinal() < target.ordinal() && !Files.isRegularFile(run.artifact())) {
				throw new IllegalStateException("Earlier scheduled artifact is missing: " + run.artifact());
			}
			if (run.ordinal() >= target.ordinal() && Files.exists(run.artifact())) {
				throw new IllegalStateException("Scheduled artifact already or prematurely exists: " + run.artifact());
			}
		}
	}

	static List<ScheduledRun> schedule(Prepared prepared) {
		var result = new ArrayList<ScheduledRun>(PairedPerformanceContract.REQUIRED_PAIRS * 4);
		int ordinal = 0;
		for (int round = 1; round <= PairedPerformanceContract.REQUIRED_PAIRS; round++) {
			for (var suite : PressureBenchmarkArtifact.Suite.values()) {
				var order = (round & 1) == 1
						? List.of(PressureBenchmarkArtifact.Implementation.BASELINE,
								PressureBenchmarkArtifact.Implementation.CANDIDATE)
						: List.of(PressureBenchmarkArtifact.Implementation.CANDIDATE,
								PressureBenchmarkArtifact.Implementation.BASELINE);
				for (var implementation : order) {
					result.add(new ScheduledRun(++ordinal, round, suite, implementation,
							artifactPath(prepared.root(), round, suite, implementation)));
				}
			}
		}
		return List.copyOf(result);
	}

	private static ScheduledRun scheduledRun(Prepared prepared,
			int round,
			PressureBenchmarkArtifact.Suite suite,
			PressureBenchmarkArtifact.Implementation implementation) {
		return schedule(prepared).stream()
				.filter(run -> run.round() == round && run.suite() == suite && run.implementation() == implementation)
				.findFirst().orElseThrow(() -> new IllegalArgumentException("Run is outside prepared schedule"));
	}

	private static Path artifactPath(Path root,
			int round,
			PressureBenchmarkArtifact.Suite suite,
			PressureBenchmarkArtifact.Implementation implementation) {
		return root.resolve("round-%02d".formatted(round))
				.resolve(suite.value + '-' + implementation.value + ".properties");
	}

	private static String scheduleText(Prepared prepared) {
		var text = new StringBuilder("schema\t").append(SCHEDULE_SCHEMA).append('\n')
				.append("configuration-sha256\t").append(prepared.configurationSha256()).append('\n')
				.append("pairs\t").append(PairedPerformanceContract.REQUIRED_PAIRS).append('\n')
				.append("adaptive-stopping\tfalse\n")
				.append("ordinal\tround\tsuite\timplementation\tartifact\n");
		for (ScheduledRun run : schedule(prepared)) {
			text.append(run.ordinal()).append('\t').append(run.round()).append('\t')
					.append(run.suite().value).append('\t').append(run.implementation().value).append('\t')
					.append(prepared.root().relativize(run.artifact())).append('\n');
		}
		return text.toString();
	}

	static Set<String> metricNames(PressureBenchmarkArtifact.Suite suite, int[] counts) {
		var specifications = suite == PressureBenchmarkArtifact.Suite.SCHEDULER
				? PressurePerformanceContract.schedulerSpecifications()
				: PressurePerformanceContract.signalSpecifications(counts);
		var names = new LinkedHashSet<String>();
		for (var specification : specifications) names.add(specification.name());
		return Set.copyOf(names);
	}

	private static String resultJson(Prepared prepared,
			Instant finished,
			PairedPerformanceContract.Evaluation scheduler,
			PairedPerformanceContract.Evaluation signal,
			boolean passed) {
		return "{\n  \"schema\": \"" + RESULT_SCHEMA + "\",\n"
				+ "  \"finished\": \"" + finished + "\",\n"
				+ "  \"baseline_sha\": \"" + prepared.baselineSha() + "\",\n"
				+ "  \"candidate_sha\": \"" + prepared.candidateSha() + "\",\n"
				+ "  \"configuration_sha256\": \"" + prepared.configurationSha256() + "\",\n"
				+ "  \"fixed_pairs\": 10,\n  \"fresh_processes\": 40,\n"
				+ "  \"adaptive_stopping\": false,\n  \"passed\": " + passed + ",\n"
				+ "  \"scheduler\": " + evaluationJson(scheduler) + ",\n"
				+ "  \"signal\": " + evaluationJson(signal) + "\n}\n";
	}

	private static String evaluationJson(PairedPerformanceContract.Evaluation evaluation) {
		var text = new StringBuilder("{\"passed\":").append(evaluation.automaticAcceptancePassed())
				.append(",\"failures\":").append(stringArrayJson(evaluation.failures()))
				.append(",\"metrics\":{");
		int index = 0;
		for (var entry : evaluation.metrics().entrySet()) {
			if (index++ > 0) text.append(',');
			var metric = entry.getValue();
			text.append('"').append(json(entry.getKey())).append("\":{")
					.append("\"ratio_mean\":").append(formatOrNull(metric.interval().mean()))
					.append(",\"lower_95\":").append(formatOrNull(metric.interval().lower95()))
					.append(",\"upper_95\":").append(formatOrNull(metric.interval().upper95()))
					.append(",\"passed\":").append(metric.automaticNonRegressionPassed())
					.append(",\"material\":").append(metric.materialImprovement()).append('}');
		}
		return text.append("}}").toString();
	}

	private static String resultMarkdown(Prepared prepared,
			Instant finished,
			PairedPerformanceContract.Evaluation scheduler,
			PairedPerformanceContract.Evaluation signal,
			boolean passed) {
		var text = new StringBuilder("# Pressure performance comparison\n\n")
				.append("- Finished: `").append(finished).append("`\n")
				.append("- Baseline / candidate: `").append(prepared.baselineSha()).append("` / `")
				.append(prepared.candidateSha()).append("`\n")
				.append("- Fixed counterbalanced pairs: `10` per suite (`40` fresh JVMs)\n")
				.append("- Overall: **").append(passed ? "PASS" : "FAIL").append("**\n\n");
		appendEvaluationMarkdown(text, "Scheduler high contention", scheduler);
		appendEvaluationMarkdown(text, "Storage-pressure signal", signal);
		return text.toString();
	}

	private static void appendEvaluationMarkdown(StringBuilder text,
			String title,
			PairedPerformanceContract.Evaluation evaluation) {
		text.append("## ").append(title).append("\n\n")
				.append("Passed: `").append(evaluation.automaticAcceptancePassed()).append("`\n\n")
				.append("| Metric | Ratio | 95% CI | Pass | Material |\n| --- | ---: | ---: | --- | --- |\n");
		for (var entry : evaluation.metrics().entrySet()) {
			var metric = entry.getValue();
			text.append("| `").append(entry.getKey()).append("` | ")
					.append(formatOrNull(metric.interval().mean())).append(" | ")
					.append(formatOrNull(metric.interval().lower95())).append("–")
					.append(formatOrNull(metric.interval().upper95())).append(" | ")
					.append(metric.automaticNonRegressionPassed()).append(" | ")
					.append(metric.materialImprovement()).append(" |\n");
		}
		if (!evaluation.failures().isEmpty()) {
			text.append("\nFailures:\n");
			for (String failure : evaluation.failures()) text.append("- ").append(failure).append('\n');
		}
		text.append('\n');
	}

	private static String formatOrNull(double value) {
		return Double.isFinite(value) ? String.format(Locale.ROOT, "%.6f", value) : "null";
	}

	private static String stringArrayJson(List<String> values) {
		var text = new StringBuilder("[");
		for (int i = 0; i < values.size(); i++) {
			if (i > 0) text.append(',');
			text.append('"').append(json(values.get(i))).append('"');
		}
		return text.append(']').toString();
	}

	private static String json(String value) {
		return value.replace("\\", "\\\\").replace("\"", "\\\"")
				.replace("\n", "\\n").replace("\r", "\\r");
	}

	private static String hostSha(String hardwareDescription) {
		return sha256(hardwareDescription + '\n' + System.getProperty("os.name") + '\n'
				+ System.getProperty("os.arch") + '\n' + Runtime.getRuntime().availableProcessors());
	}

	private static String runtimeSha() {
		return sha256(System.getProperty("java.runtime.version") + '\n'
				+ System.getProperty("java.vm.name") + '\n' + System.getProperty("java.home"));
	}

	private static void verifyCheckout(String expectedSha) throws IOException, InterruptedException {
		var head = new ProcessBuilder("git", "rev-parse", "HEAD").redirectErrorStream(true).start();
		String actual = new String(head.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
		if (head.waitFor() != 0 || !actual.equals(expectedSha)) {
			throw new IllegalStateException("Worker checkout does not match scheduled build " + expectedSha);
		}
		for (String[] command : List.of(
				new String[] {"git", "diff", "--quiet"},
				new String[] {"git", "diff", "--cached", "--quiet"})) {
			var process = new ProcessBuilder(command).inheritIO().start();
			if (process.waitFor() != 0) throw new IllegalStateException("Enforced worker checkout is dirty");
		}
	}

	static String sha256(String value) {
		try {
			byte[] hash = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
			return java.util.HexFormat.of().formatHex(hash);
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

	private static Path requiredRoot(Map<String, String> values, Set<String> allowedKeys) {
		for (String key : values.keySet()) {
			if (!allowedKeys.contains(key)) throw new IllegalArgumentException("Unknown option --" + key);
		}
		String root = require(values, "root");
		return Path.of(root).toAbsolutePath().normalize();
	}

	private static String require(Map<String, String> values, String key) {
		String value = values.get(key);
		if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing --" + key);
		return value;
	}

	private static int integer(Map<String, String> values, String key) {
		try {
			return Integer.parseInt(require(values, key));
		} catch (NumberFormatException failure) {
			throw new IllegalArgumentException("Invalid integer --" + key, failure);
		}
	}

	enum Mode { PREPARE, WORKER, EVALUATE;
		static Mode parse(String value) {
			return switch (value) {
				case "prepare" -> PREPARE;
				case "worker" -> WORKER;
				case "evaluate" -> EVALUATE;
				default -> throw new IllegalArgumentException("mode must be prepare, worker, or evaluate");
			};
		}
	}

	record ScheduledRun(int ordinal,
			int round,
			PressureBenchmarkArtifact.Suite suite,
			PressureBenchmarkArtifact.Implementation implementation,
			Path artifact) {
	}

	record Prepared(Path root,
			String baselineSha,
			String candidateSha,
			String hostState,
			String hardwareDescription,
			boolean enforce,
			int schedulerOperations,
			int schedulerSubmitters,
			int schedulerReadWorkers,
			int schedulerWriteWorkers,
			int schedulerAnalyticalLimit,
			int schedulerForegroundCapacity,
			int schedulerBatchCapacity,
			int schedulerWorkTokens,
			int schedulerWarmupOperations,
			long schedulerSeed,
			int[] signalColumnFamilyCounts,
			long signalWarmupColumns,
			long signalMeasuredColumns,
			int signalMinimumEvaluations,
			int signalMaximumEvaluations,
			int signalLatencySampleStride) {

		Prepared {
			root = root.toAbsolutePath().normalize();
			signalColumnFamilyCounts = signalColumnFamilyCounts.clone();
			if (!baselineSha.matches("[0-9a-f]{40}") || !candidateSha.matches("[0-9a-f]{40}")
					|| baselineSha.equals(candidateSha)) throw new IllegalArgumentException("distinct full Git SHAs required");
			if (hostState.isBlank() || hardwareDescription.isBlank()
					|| hostState.indexOf('=') >= 0 || hardwareDescription.indexOf('=') >= 0
					|| hostState.indexOf('\n') >= 0 || hardwareDescription.indexOf('\n') >= 0) {
				throw new IllegalArgumentException("canonical single-line host metadata required");
			}
			if (enforce && !hostState.equals("dedicated")) throw new IllegalArgumentException("enforced run requires dedicated host");
			new SchedulerHighContentionBenchmark.Config(schedulerOperations, schedulerSubmitters,
					schedulerReadWorkers, schedulerWriteWorkers, schedulerAnalyticalLimit,
					schedulerForegroundCapacity, schedulerBatchCapacity, schedulerWorkTokens,
					4, 2, 5, 10, 5, 30, true, schedulerSeed, Duration.ofMinutes(5)).validate();
			if (schedulerWarmupOperations < 512 || schedulerWarmupOperations > schedulerOperations) {
				throw new IllegalArgumentException("scheduler warmup operations are outside bounds");
			}
			new StoragePressureSignalBenchmark.Config(signalColumnFamilyCounts, signalWarmupColumns,
					signalMeasuredColumns, signalMinimumEvaluations, signalMaximumEvaluations,
					signalLatencySampleStride, candidateSha).validate();
		}

		@Override public int[] signalColumnFamilyCounts() { return signalColumnFamilyCounts.clone(); }

		SchedulerHighContentionBenchmark.Config schedulerConfig() {
			return new SchedulerHighContentionBenchmark.Config(schedulerOperations, schedulerSubmitters,
					schedulerReadWorkers, schedulerWriteWorkers, schedulerAnalyticalLimit,
					schedulerForegroundCapacity, schedulerBatchCapacity, schedulerWorkTokens,
					4, 2, 5, 10, 5, 30, true, schedulerSeed, Duration.ofMinutes(5));
		}

		SchedulerHighContentionBenchmark.Config schedulerWarmupConfig() {
			var measured = schedulerConfig();
			return new SchedulerHighContentionBenchmark.Config(schedulerWarmupOperations,
					Math.min(schedulerSubmitters, 32), schedulerReadWorkers, schedulerWriteWorkers,
					schedulerAnalyticalLimit, Math.min(schedulerForegroundCapacity, 8_192),
					Math.min(schedulerBatchCapacity, 8_192), schedulerWorkTokens,
					4, 2, 5, 10, 5, 30, true, schedulerSeed ^ 0xD1B54A32D192ED03L,
					measured.timeout());
		}

		StoragePressureSignalBenchmark.Config signalConfig(String buildId) {
			return new StoragePressureSignalBenchmark.Config(signalColumnFamilyCounts, signalWarmupColumns,
					signalMeasuredColumns, signalMinimumEvaluations, signalMaximumEvaluations,
					signalLatencySampleStride, buildId);
		}

		String configurationText() {
			return "scheduler-operations=" + schedulerOperations + '\n'
					+ "scheduler-submitters=" + schedulerSubmitters + '\n'
					+ "scheduler-read-workers=" + schedulerReadWorkers + '\n'
					+ "scheduler-write-workers=" + schedulerWriteWorkers + '\n'
					+ "scheduler-analytical-limit=" + schedulerAnalyticalLimit + '\n'
					+ "scheduler-foreground-capacity=" + schedulerForegroundCapacity + '\n'
					+ "scheduler-batch-capacity=" + schedulerBatchCapacity + '\n'
					+ "scheduler-work-tokens=" + schedulerWorkTokens + '\n'
					+ "scheduler-warmup-operations=" + schedulerWarmupOperations + '\n'
					+ "scheduler-seed=" + schedulerSeed + '\n'
					+ "signal-cf-counts=" + csv(signalColumnFamilyCounts) + '\n'
					+ "signal-warmup-columns=" + signalWarmupColumns + '\n'
					+ "signal-measured-columns=" + signalMeasuredColumns + '\n'
					+ "signal-minimum-evaluations=" + signalMinimumEvaluations + '\n'
					+ "signal-maximum-evaluations=" + signalMaximumEvaluations + '\n'
					+ "signal-latency-sample-stride=" + signalLatencySampleStride + '\n';
		}

		String configurationSha256() { return sha256(configurationText()); }

		String metadataText() {
			return "schema=" + METADATA_SCHEMA + '\n'
					+ "baseline-sha=" + baselineSha + '\n'
					+ "candidate-sha=" + candidateSha + '\n'
					+ "host-state=" + hostState + '\n'
					+ "hardware-description=" + hardwareDescription + '\n'
					+ "enforce=" + enforce + '\n' + configurationText();
		}

		static Prepared fromArguments(Map<String, String> arguments) {
			var allowed = new LinkedHashSet<>(METADATA_KEYS);
			allowed.remove("schema");
			allowed.add("mode"); allowed.add("root");
			for (String key : arguments.keySet()) if (!allowed.contains(key)) throw new IllegalArgumentException("Unknown option --" + key);
			Path root = Path.of(require(arguments, "root"));
			return fromValues(root, arguments, true);
		}

		static Prepared read(Path root) throws IOException {
			Path metadata = root.resolve(METADATA_FILE);
			Map<String, String> values = strictMetadata(Files.readString(metadata));
			return fromValues(root, values, false);
		}

		private static Prepared fromValues(Path root, Map<String, String> values, boolean options) {
			return new Prepared(root,
					value(values, "baseline-sha", null), value(values, "candidate-sha", null),
					value(values, "host-state", "dedicated"), value(values, "hardware-description", "unspecified"),
					bool(values, "enforce", false), intValue(values, "scheduler-operations", 1_000_000),
					intValue(values, "scheduler-submitters", 64), intValue(values, "scheduler-read-workers", 16),
					intValue(values, "scheduler-write-workers", 16), intValue(values, "scheduler-analytical-limit", 4),
					intValue(values, "scheduler-foreground-capacity", 65_536), intValue(values, "scheduler-batch-capacity", 65_536),
					intValue(values, "scheduler-work-tokens", 256), intValue(values, "scheduler-warmup-operations", 100_000),
					longValue(values, "scheduler-seed", 0x5EED_C0FFEE_2026L),
					counts(value(values, "signal-cf-counts", "1,64,256,1024,4096")),
					longValue(values, "signal-warmup-columns", 5_000_000L),
					longValue(values, "signal-measured-columns", 20_000_000L),
					intValue(values, "signal-minimum-evaluations", 20_000),
					intValue(values, "signal-maximum-evaluations", 2_000_000),
					intValue(values, "signal-latency-sample-stride", 1_024));
		}

		private static Map<String, String> strictMetadata(String text) {
			var values = new LinkedHashMap<String, String>();
			for (String line : text.split("\\R")) {
				int separator = line.indexOf('=');
				if (separator <= 0 || separator != line.lastIndexOf('=')) throw new IllegalArgumentException("Malformed pressure metadata");
				String key = line.substring(0, separator);
				if (!METADATA_KEYS.contains(key) || values.put(key, line.substring(separator + 1)) != null) {
					throw new IllegalArgumentException("Unknown or duplicate pressure metadata " + key);
				}
			}
			if (!values.keySet().equals(METADATA_KEYS)) throw new IllegalArgumentException("Missing pressure metadata keys");
			if (!METADATA_SCHEMA.equals(values.get("schema"))) throw new IllegalArgumentException("Unsupported pressure metadata schema");
			return Map.copyOf(values);
		}

		private static String value(Map<String, String> values, String key, String fallback) {
			String value = values.get(key);
			if (value == null) value = fallback;
			if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing " + key);
			return value;
		}
		private static int intValue(Map<String, String> values, String key, int fallback) { return Integer.parseInt(value(values, key, Integer.toString(fallback))); }
		private static long longValue(Map<String, String> values, String key, long fallback) { return Long.parseLong(value(values, key, Long.toString(fallback))); }
		private static boolean bool(Map<String, String> values, String key, boolean fallback) {
			String value = value(values, key, Boolean.toString(fallback));
			if (!value.equals("true") && !value.equals("false")) throw new IllegalArgumentException("Invalid boolean " + key);
			return Boolean.parseBoolean(value);
		}
		private static int[] counts(String value) {
			int[] counts = Arrays.stream(value.split(",")).mapToInt(Integer::parseInt).toArray();
			if (counts.length == 0) throw new IllegalArgumentException("signal counts required");
			return counts;
		}
		private static String csv(int[] values) {
			return Arrays.stream(values).mapToObj(Integer::toString).collect(java.util.stream.Collectors.joining(","));
		}
	}
}
