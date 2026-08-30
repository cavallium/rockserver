package it.cavallium.rockserver.core.impl.benchmark;

import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.File;
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
import java.util.Properties;
import java.util.Set;

/** Immutable fresh-process controller for the adversarial cross-pool liveness proof. */
public final class AdversarialBatchLivenessPairedBenchmark {

	static final int REQUIRED_PAIRS = 10;
	static final String METADATA_SCHEMA = "rockserver-adversarial-batch-liveness-metadata-v1";
	static final String ARTIFACT_SCHEMA = "rockserver-adversarial-batch-liveness-worker-v1";
	static final String RESULT_SCHEMA = "rockserver-adversarial-batch-liveness-comparison-v1";
	private static final String METADATA_FILE = "metadata.properties";
	private static final String SCHEDULE_FILE = "schedule.tsv";
	private static final Set<String> METADATA_KEYS = Set.of(
			"schema", "configuration-sha256", "baseline-sha", "candidate-sha",
			"baseline-worktree", "candidate-worktree", "baseline-classes", "candidate-classes",
			"baseline-production-sha256", "candidate-production-sha256", "hardware-description",
			"enforce", "read-workers", "write-workers", "queued-read-tasks", "pressure-interval-nanos",
			"nondispatchable-phase-nanos", "setup-timeout-nanos", "fair-turn-bound-nanos",
			"candidate-maximum-gap-nanos", "candidate-minimum-throughput");
	private static final Set<String> ARTIFACT_KEYS = Set.of(
			"schema", "round", "ordinal", "implementation", "build-sha", "configuration-sha256",
			"host-sha256", "hardware-description", "runtime-sha256", "harness-sha256",
			"classpath-sha256", "production-sha256", "process-id", "started-epoch-millis",
			"finished-epoch-millis", "correctness-passed", "topology-proven",
			"useful-read-completions", "maximum-read-zero-progress-gap-nanos",
			"useful-read-throughput-per-second", "write-fair-turn-delay-nanos",
			"nondispatchable-phase-nanos", "pressure-interval-nanos");

	private AdversarialBatchLivenessPairedBenchmark() {
	}

	public static void main(String[] args) throws Exception {
		var arguments = arguments(args);
		switch (Mode.parse(arguments.getOrDefault("mode", "evaluate"))) {
			case PREPARE -> prepare(Prepared.fromArguments(arguments));
			case EXECUTE -> execute(requiredRoot(arguments, Set.of("mode", "root")));
			case WORKER -> worker(arguments);
			case EVALUATE -> evaluate(requiredRoot(arguments, Set.of("mode", "root")));
		}
	}

	private static void prepare(Prepared prepared) throws IOException {
		if (Files.exists(prepared.root())) {
			throw new IllegalArgumentException("Benchmark root already exists: " + prepared.root());
		}
		verifyProductionCheckout(prepared.baselineWorktree(), prepared.baselineSha());
		verifyProductionCheckout(prepared.candidateWorktree(), prepared.candidateSha());
		Files.createDirectories(prepared.root());
		Files.writeString(prepared.root().resolve(METADATA_FILE), prepared.metadataText(),
				StandardOpenOption.CREATE_NEW);
		Files.writeString(prepared.root().resolve(SCHEDULE_FILE), scheduleText(prepared),
				StandardOpenOption.CREATE_NEW);
		System.out.println("Prepared immutable adversarial liveness schedule: "
				+ prepared.root().resolve(SCHEDULE_FILE));
	}

	private static void execute(Path root) throws Exception {
		var prepared = Prepared.read(root);
		String originalClassPath = System.getProperty("java.class.path");
		Path java = Path.of(System.getProperty("java.home"), "bin", "java");
		for (var run : schedule(prepared)) {
			Path selectedClasses = run.implementation() == Implementation.BASELINE
					? prepared.baselineClasses() : prepared.candidateClasses();
			Path selectedWorktree = run.implementation() == Implementation.BASELINE
					? prepared.baselineWorktree() : prepared.candidateWorktree();
			String selectedSha = run.implementation() == Implementation.BASELINE
					? prepared.baselineSha() : prepared.candidateSha();
			String selectedClassPath = replaceProductionClasses(originalClassPath,
					prepared.candidateClasses(), selectedClasses);
			var process = new ProcessBuilder(java.toString(),
					"-XX:+EnableDynamicAgentLoading",
					"--enable-native-access=ALL-UNNAMED",
					"-cp", selectedClassPath,
					AdversarialBatchLivenessPairedBenchmark.class.getName(),
					"--mode=worker",
					"--root=" + root,
					"--round=" + run.round(),
					"--implementation=" + run.implementation().value,
					"--build-sha=" + selectedSha)
					.directory(selectedWorktree.toFile())
					.inheritIO()
					.start();
			int exit = process.waitFor();
			if (exit != 0) {
				throw new IllegalStateException("Fresh worker failed at ordinal " + run.ordinal()
						+ " with exit " + exit);
			}
		}
		evaluate(root);
	}

	private static void worker(Map<String, String> arguments) throws Exception {
		Path root = requiredRoot(arguments,
				Set.of("mode", "root", "round", "implementation", "build-sha"));
		var prepared = Prepared.read(root);
		int round = integer(arguments, "round");
		var implementation = Implementation.parse(require(arguments, "implementation"));
		String buildSha = requireSha(arguments, "build-sha");
		var scheduled = scheduledRun(prepared, round, implementation);
		assertWorkerOrder(prepared, scheduled);
		String expectedBuild = implementation == Implementation.BASELINE
				? prepared.baselineSha() : prepared.candidateSha();
		if (!expectedBuild.equals(buildSha)) throw new IllegalArgumentException("Worker build SHA mismatch");
		if (prepared.enforce()) verifyProductionCheckout(Path.of("."), buildSha);

		URI productionLocation = RWScheduler.class.getProtectionDomain().getCodeSource().getLocation().toURI();
		String productionSha = contentSha(Path.of(productionLocation));
		String expectedProduction = implementation == Implementation.BASELINE
				? prepared.baselineProductionSha256() : prepared.candidateProductionSha256();
		if (!expectedProduction.equals(productionSha)) {
			throw new IllegalStateException("Selected RWScheduler production bytes do not match prepared provenance");
		}
		long started = System.currentTimeMillis();
		var result = AdversarialBatchLivenessBenchmark.run(prepared.config());
		if (implementation == Implementation.BASELINE) {
			result.assertBaselineStall();
		} else {
			result.assertCandidateWorkConserving(prepared.candidateMaximumGap(),
					prepared.candidateMinimumThroughput());
		}
		long finished = System.currentTimeMillis();
		String classPathSha = GrpcRetainedReadBenchmark.classPathContentSha256ForTesting(
				System.getProperty("java.class.path"));
		URI harnessLocation = AdversarialBatchLivenessPairedBenchmark.class.getProtectionDomain()
				.getCodeSource().getLocation().toURI();
		var artifact = new Artifact(scheduled.round(),
				scheduled.ordinal(),
				implementation,
				buildSha,
				prepared.configurationSha256(),
				hostSha(prepared.hardwareDescription()),
				prepared.hardwareDescription(),
				runtimeSha(),
				contentSha(Path.of(harnessLocation)),
				classPathSha,
				productionSha,
				ProcessHandle.current().pid(),
				started,
				finished,
				true,
				result);
		artifact.write(scheduled.artifact());
		System.out.println("Wrote adversarial liveness artifact " + scheduled.artifact());
	}

	static Evaluation evaluate(Path root) throws IOException {
		var prepared = Prepared.read(root);
		if (!Files.readString(root.resolve(SCHEDULE_FILE)).equals(scheduleText(prepared))) {
			throw new IllegalArgumentException("Benchmark schedule changed after preparation");
		}
		var baseline = new ArrayList<Artifact>();
		var candidate = new ArrayList<Artifact>();
		var failures = new ArrayList<String>();
		var processIds = new LinkedHashSet<Long>();
		long previousFinished = Long.MIN_VALUE;
		String runtimeSha = null;
		String hostSha = null;
		String harnessSha = null;
		String baselineClassPath = null;
		String candidateClassPath = null;
		String expectedHostSha = hostSha(prepared.hardwareDescription());
		String expectedRuntimeSha = runtimeSha();
		URI harnessLocation;
		try {
			harnessLocation = AdversarialBatchLivenessPairedBenchmark.class.getProtectionDomain()
					.getCodeSource().getLocation().toURI();
		} catch (java.net.URISyntaxException malformed) {
			throw new IOException("Invalid harness code-source URI", malformed);
		}
		String expectedHarnessSha = contentSha(Path.of(harnessLocation));
		for (var run : schedule(prepared)) {
			var artifact = Artifact.read(run.artifact());
			String prefix = "ordinal " + run.ordinal() + ": ";
			if (artifact.round() != run.round() || artifact.ordinal() != run.ordinal()
					|| artifact.implementation() != run.implementation()) {
				failures.add(prefix + "schedule identity mismatch");
			}
			String expectedBuild = run.implementation() == Implementation.BASELINE
					? prepared.baselineSha() : prepared.candidateSha();
			String expectedProduction = run.implementation() == Implementation.BASELINE
					? prepared.baselineProductionSha256() : prepared.candidateProductionSha256();
			if (!artifact.buildSha().equals(expectedBuild)) failures.add(prefix + "build SHA mismatch");
			if (!artifact.productionSha256().equals(expectedProduction)) failures.add(prefix + "production bytes mismatch");
			if (!artifact.configurationSha256().equals(prepared.configurationSha256())) {
				failures.add(prefix + "configuration mismatch");
			}
			if (!artifact.correctnessPassed()) failures.add(prefix + "worker correctness failed");
			if (!artifact.result().topologyProven()) failures.add(prefix + "adversarial topology was not proven");
			if (!artifact.hostSha256().equals(expectedHostSha)) failures.add(prefix + "host identity mismatch");
			if (!artifact.runtimeSha256().equals(expectedRuntimeSha)) failures.add(prefix + "runtime identity mismatch");
			if (!artifact.harnessSha256().equals(expectedHarnessSha)) failures.add(prefix + "harness identity mismatch");
			if (!processIds.add(artifact.processId())) failures.add(prefix + "process ID was reused");
			if (artifact.startedEpochMillis() < previousFinished) failures.add(prefix + "fresh workers overlapped");
			if (artifact.finishedEpochMillis() < artifact.startedEpochMillis()) {
				failures.add(prefix + "worker finish preceded its start");
			}
			previousFinished = artifact.finishedEpochMillis();
			if (runtimeSha == null) runtimeSha = artifact.runtimeSha256();
			else if (!runtimeSha.equals(artifact.runtimeSha256())) failures.add(prefix + "runtime changed");
			if (hostSha == null) hostSha = artifact.hostSha256();
			else if (!hostSha.equals(artifact.hostSha256())) failures.add(prefix + "host changed");
			if (harnessSha == null) harnessSha = artifact.harnessSha256();
			else if (!harnessSha.equals(artifact.harnessSha256())) failures.add(prefix + "harness changed");
			if (!artifact.hardwareDescription().equals(prepared.hardwareDescription())) {
				failures.add(prefix + "hardware description changed");
			}
			if (artifact.result().nondispatchablePhaseNanos()
					!= prepared.config().nondispatchablePhase().toNanos()
					|| artifact.result().pressureIntervalNanos()
					!= prepared.config().pressureInterval().toNanos()) {
				failures.add(prefix + "measured timing configuration changed");
			}
			try {
				if (run.implementation() == Implementation.BASELINE) artifact.result().assertBaselineStall();
				else artifact.result().assertCandidateWorkConserving(prepared.candidateMaximumGap(),
						prepared.candidateMinimumThroughput());
				if (artifact.result().writeFairTurnDelayNanos() > prepared.config().fairTurnBound().toNanos()) {
					throw new IllegalStateException("WRITE fair turn exceeded the bound");
				}
			} catch (IllegalStateException semanticFailure) {
				failures.add(prefix + semanticFailure.getMessage());
			}
			if (run.implementation() == Implementation.BASELINE) {
				baselineClassPath = stableIdentity(baselineClassPath, artifact.classPathSha256(),
						prefix + "baseline classpath changed", failures);
				baseline.add(artifact);
			} else {
				candidateClassPath = stableIdentity(candidateClassPath, artifact.classPathSha256(),
						prefix + "candidate classpath changed", failures);
				candidate.add(artifact);
			}
		}
		if (baseline.size() != REQUIRED_PAIRS || candidate.size() != REQUIRED_PAIRS) {
			failures.add("exactly " + REQUIRED_PAIRS + " baseline/candidate pairs are required");
		}
		PairedBenchmarkStatistics.RatioConfidenceInterval gapRatio;
		try {
			gapRatio = PairedBenchmarkStatistics.pairedLogRatio(
					values(baseline, Metric.MAXIMUM_GAP), values(candidate, Metric.MAXIMUM_GAP));
			if (!gapRatio.available() || gapRatio.upper95() > 0.5d) {
				failures.add("candidate maximum-gap upper 95% ratio must be <= 0.5");
			}
		} catch (IllegalArgumentException malformed) {
			gapRatio = new PairedBenchmarkStatistics.RatioConfidenceInterval(0,
					Double.NaN, Double.NaN, Double.NaN);
			failures.add("maximum-gap paired samples are invalid: " + malformed.getMessage());
		}
		var evaluation = new Evaluation(failures.isEmpty(),
				List.copyOf(failures),
				gapRatio,
				mean(baseline, Metric.THROUGHPUT),
				mean(candidate, Metric.THROUGHPUT),
				mean(baseline, Metric.FAIR_DELAY),
				mean(candidate, Metric.FAIR_DELAY),
				baselineClassPath,
				candidateClassPath,
				runtimeSha,
				hostSha,
				harnessSha);
		Files.writeString(root.resolve("results.json"), resultJson(prepared, evaluation),
				StandardOpenOption.CREATE_NEW);
		Files.writeString(root.resolve("results.md"), resultMarkdown(prepared, evaluation),
				StandardOpenOption.CREATE_NEW);
		if (prepared.enforce() && !evaluation.passed()) {
			throw new IllegalStateException("Adversarial liveness comparison failed: "
					+ String.join("; ", evaluation.failures()));
		}
		return evaluation;
	}

	private static String stableIdentity(String existing,
			String value,
			String failure,
			List<String> failures) {
		if (existing != null && !existing.equals(value)) failures.add(failure);
		return existing == null ? value : existing;
	}

	private static double[] values(List<Artifact> artifacts, Metric metric) {
		double[] values = new double[artifacts.size()];
		for (int index = 0; index < values.length; index++) {
			values[index] = switch (metric) {
				case MAXIMUM_GAP -> artifacts.get(index).result().maximumReadZeroProgressGapNanos();
				case THROUGHPUT -> artifacts.get(index).result().usefulReadThroughputPerSecond();
				case FAIR_DELAY -> artifacts.get(index).result().writeFairTurnDelayNanos();
			};
		}
		return values;
	}

	private static double mean(List<Artifact> artifacts, Metric metric) {
		return Arrays.stream(values(artifacts, metric)).average().orElse(Double.NaN);
	}

	private static String resultJson(Prepared prepared, Evaluation result) {
		return """
				{
				  "schema": "%s",
				  "evaluated_at": "%s",
				  "baseline_sha": "%s",
				  "candidate_sha": "%s",
				  "configuration_sha256": "%s",
				  "fixed_pairs": %d,
				  "fresh_processes": %d,
				  "runtime_sha256": "%s",
				  "host_sha256": "%s",
				  "harness_sha256": "%s",
				  "baseline_classpath_sha256": "%s",
				  "candidate_classpath_sha256": "%s",
				  "baseline_mean_read_ops_per_second": %.9f,
				  "candidate_mean_read_ops_per_second": %.9f,
				  "baseline_mean_write_fair_turn_nanos": %.9f,
				  "candidate_mean_write_fair_turn_nanos": %.9f,
				  "maximum_gap_ratio": {"mean": %.9f, "lower95": %.9f, "upper95": %.9f},
				  "failures": %s,
				  "passed": %s
				}
				""".formatted(RESULT_SCHEMA,
				Instant.now(),
				prepared.baselineSha(),
				prepared.candidateSha(),
				prepared.configurationSha256(),
				REQUIRED_PAIRS,
				REQUIRED_PAIRS * 2,
				nullToEmpty(result.runtimeSha256()),
				nullToEmpty(result.hostSha256()),
				nullToEmpty(result.harnessSha256()),
				nullToEmpty(result.baselineClassPathSha256()),
				nullToEmpty(result.candidateClassPathSha256()),
				result.baselineMeanThroughput(),
				result.candidateMeanThroughput(),
				result.baselineMeanFairTurnNanos(),
				result.candidateMeanFairTurnNanos(),
				result.maximumGapRatio().mean(),
				result.maximumGapRatio().lower95(),
				result.maximumGapRatio().upper95(),
				jsonArray(result.failures()),
				result.passed());
	}

	private static String resultMarkdown(Prepared prepared, Evaluation result) {
		return """
				# Adversarial BATCH liveness proof

				Overall: **%s**

				- Baseline: `%s`
				- Candidate: `%s`
				- Fixed counterbalanced pairs: %d (%d fresh JVMs)
				- Baseline useful READ throughput: %.3f ops/s
				- Candidate useful READ throughput: %.3f ops/s
				- Maximum zero-progress-gap candidate/baseline ratio: %.6f (95%% CI %.6f..%.6f)
				- Baseline WRITE fair-turn delay: %.3f ms
				- Candidate WRITE fair-turn delay: %.3f ms
				- Failures: %s
				""".formatted(result.passed() ? "PASS" : "FAIL",
				prepared.baselineSha(),
				prepared.candidateSha(),
				REQUIRED_PAIRS,
				REQUIRED_PAIRS * 2,
				result.baselineMeanThroughput(),
				result.candidateMeanThroughput(),
				result.maximumGapRatio().mean(),
				result.maximumGapRatio().lower95(),
				result.maximumGapRatio().upper95(),
				result.baselineMeanFairTurnNanos() / 1_000_000.0d,
				result.candidateMeanFairTurnNanos() / 1_000_000.0d,
				result.failures().isEmpty() ? "none" : String.join("; ", result.failures()));
	}

	private static String scheduleText(Prepared prepared) {
		var text = new StringBuilder("schema\trockserver-adversarial-batch-liveness-schedule-v1\n")
				.append("configuration-sha256\t").append(prepared.configurationSha256()).append('\n')
				.append("pairs\t").append(REQUIRED_PAIRS).append('\n')
				.append("adaptive-stopping\tfalse\n")
				.append("ordinal\tround\timplementation\tartifact\n");
		for (var run : schedule(prepared)) {
			text.append(run.ordinal()).append('\t').append(run.round()).append('\t')
					.append(run.implementation().value).append('\t')
					.append(prepared.root().relativize(run.artifact())).append('\n');
		}
		return text.toString();
	}

	static List<ScheduledRun> schedule(Prepared prepared) {
		var runs = new ArrayList<ScheduledRun>(REQUIRED_PAIRS * 2);
		int ordinal = 0;
		for (int round = 1; round <= REQUIRED_PAIRS; round++) {
			var order = (round & 1) == 1
					? List.of(Implementation.BASELINE, Implementation.CANDIDATE)
					: List.of(Implementation.CANDIDATE, Implementation.BASELINE);
			for (var implementation : order) {
				Path artifact = prepared.root().resolve("round-%02d".formatted(round))
						.resolve(implementation.value + ".properties");
				runs.add(new ScheduledRun(++ordinal, round, implementation, artifact));
			}
		}
		return List.copyOf(runs);
	}

	private static ScheduledRun scheduledRun(Prepared prepared, int round, Implementation implementation) {
		return schedule(prepared).stream()
				.filter(run -> run.round() == round && run.implementation() == implementation)
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Run is outside the prepared schedule"));
	}

	private static void assertWorkerOrder(Prepared prepared, ScheduledRun target) {
		for (var run : schedule(prepared)) {
			if (run.ordinal() < target.ordinal() && !Files.isRegularFile(run.artifact())) {
				throw new IllegalStateException("Earlier scheduled artifact is missing: " + run.artifact());
			}
			if (run.ordinal() >= target.ordinal() && Files.exists(run.artifact())) {
				throw new IllegalStateException("Artifact already or prematurely exists: " + run.artifact());
			}
		}
	}

	static String replaceProductionClasses(String classPath, Path candidateClasses, Path selectedClasses) {
		Path candidate = candidateClasses.toAbsolutePath().normalize();
		Path selected = selectedClasses.toAbsolutePath().normalize();
		var entries = new ArrayList<String>();
		boolean replaced = false;
		for (String entry : classPath.split(java.util.regex.Pattern.quote(File.pathSeparator))) {
			Path normalized = Path.of(entry).toAbsolutePath().normalize();
			if (normalized.equals(candidate)) {
				entries.add(selected.toString());
				replaced = true;
			} else {
				entries.add(normalized.toString());
			}
		}
		if (!replaced) throw new IllegalArgumentException("Candidate classes are absent from the classpath");
		return String.join(File.pathSeparator, entries);
	}

	static void verifyProductionCheckout(Path worktree, String expectedSha) throws IOException {
		try {
			Path directory = worktree.toAbsolutePath().normalize();
			var revision = new ProcessBuilder("git", "rev-parse", expectedSha + "^{commit}")
					.directory(directory.toFile())
					.redirectErrorStream(true)
					.start();
			String resolved = new String(revision.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
			if (revision.waitFor() != 0 || !resolved.equals(expectedSha)) {
				throw new IllegalArgumentException("Expected production SHA is absent: " + expectedSha);
			}
			var difference = new ProcessBuilder("git", "diff", "--quiet", expectedSha, "--",
					"pom.xml", "src/main", "src/library")
					.directory(directory.toFile()).start();
			if (difference.waitFor() != 0) {
				throw new IllegalArgumentException("Production sources differ from expected SHA: " + worktree);
			}
			var dirty = new ProcessBuilder("git", "status", "--porcelain", "--untracked-files=all", "--",
					"pom.xml", "src/main", "src/library")
					.directory(directory.toFile()).redirectErrorStream(true).start();
			String dirtyProduction = new String(dirty.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
			if (dirty.waitFor() != 0 || !dirtyProduction.isEmpty()) {
				throw new IllegalArgumentException("Production source tree is dirty: " + worktree);
			}
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throw new IOException("Interrupted while verifying checkout", interrupted);
		}
	}

	private static String contentSha(Path path) throws IOException {
		return GrpcRetainedReadBenchmark.classPathContentSha256ForTesting(path.toString());
	}

	static String hostSha(String hardwareDescription) {
		return sha256(hardwareDescription + '\n' + System.getProperty("os.name") + '\n'
				+ System.getProperty("os.version") + '\n' + System.getProperty("os.arch") + '\n'
				+ Runtime.getRuntime().availableProcessors());
	}

	static String runtimeSha() {
		return sha256(System.getProperty("java.runtime.version") + '\n'
				+ System.getProperty("java.vm.name") + '\n' + System.getProperty("java.vm.vendor"));
	}

	static String harnessSha() throws Exception {
		URI location = AdversarialBatchLivenessPairedBenchmark.class.getProtectionDomain()
				.getCodeSource().getLocation().toURI();
		return contentSha(Path.of(location));
	}

	static String sha256(String value) {
		try {
			var digest = MessageDigest.getInstance("SHA-256");
			return java.util.HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
		} catch (java.security.NoSuchAlgorithmException impossible) {
			throw new AssertionError(impossible);
		}
	}

	private static Map<String, String> arguments(String[] args) {
		var values = new LinkedHashMap<String, String>();
		for (String argument : args) {
			if (!argument.startsWith("--") || !argument.contains("=")) {
				throw new IllegalArgumentException("Expected --key=value, got " + argument);
			}
			int separator = argument.indexOf('=');
			String key = argument.substring(2, separator);
			String value = argument.substring(separator + 1);
			if (key.isBlank() || value.isBlank() || values.putIfAbsent(key, value) != null) {
				throw new IllegalArgumentException("Invalid or duplicate argument " + key);
			}
		}
		return Map.copyOf(values);
	}

	private static Path requiredRoot(Map<String, String> arguments, Set<String> allowed) {
		rejectUnknown(arguments, allowed);
		return Path.of(require(arguments, "root")).toAbsolutePath().normalize();
	}

	private static void rejectUnknown(Map<String, String> arguments, Set<String> allowed) {
		for (String key : arguments.keySet()) {
			if (!allowed.contains(key)) throw new IllegalArgumentException("Unknown argument --" + key);
		}
	}

	private static String require(Map<String, String> values, String key) {
		String value = values.get(key);
		if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing --" + key);
		return value;
	}

	private static String requireSha(Map<String, String> values, String key) {
		String sha = require(values, key);
		if (!sha.matches("[0-9a-f]{40}")) throw new IllegalArgumentException(key + " must be a full SHA-1");
		return sha;
	}

	private static int integer(Map<String, String> values, String key) {
		try {
			return Integer.parseInt(require(values, key));
		} catch (NumberFormatException malformed) {
			throw new IllegalArgumentException(key + " must be an integer", malformed);
		}
	}

	private static long longValue(Properties values, String key) {
		try {
			return Long.parseLong(require(values, key));
		} catch (NumberFormatException malformed) {
			throw new IllegalArgumentException(key + " must be a long", malformed);
		}
	}

	private static double doubleValue(Properties values, String key) {
		try {
			double value = Double.parseDouble(require(values, key));
			if (!Double.isFinite(value)) throw new NumberFormatException("not finite");
			return value;
		} catch (NumberFormatException malformed) {
			throw new IllegalArgumentException(key + " must be finite", malformed);
		}
	}

	private static String require(Properties values, String key) {
		String value = values.getProperty(key);
		if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing property " + key);
		return value;
	}

	private static Properties exactProperties(Path path, Set<String> expected) throws IOException {
		var values = new Properties();
		try (var input = Files.newInputStream(path)) {
			values.load(input);
		}
		if (!values.stringPropertyNames().equals(expected)) {
			throw new IllegalArgumentException("Property keys differ in " + path + ": "
					+ values.stringPropertyNames());
		}
		return values;
	}

	private static String jsonArray(List<String> values) {
		return values.stream().map(value -> "\"" + json(value) + "\"")
				.collect(java.util.stream.Collectors.joining(",", "[", "]"));
	}

	private static String json(String value) {
		return value.replace("\\", "\\\\").replace("\"", "\\\"")
				.replace("\n", "\\n").replace("\r", "\\r");
	}

	private static String nullToEmpty(String value) {
		return value == null ? "" : value;
	}

	enum Mode {
		PREPARE,
		EXECUTE,
		WORKER,
		EVALUATE;

		static Mode parse(String value) {
			try {
				return valueOf(value.toUpperCase(Locale.ROOT));
			} catch (IllegalArgumentException malformed) {
				throw new IllegalArgumentException("Unknown mode " + value, malformed);
			}
		}
	}

	enum Implementation {
		BASELINE("baseline"),
		CANDIDATE("candidate");

		private final String value;

		Implementation(String value) {
			this.value = value;
		}

		static Implementation parse(String value) {
			return Arrays.stream(values()).filter(candidate -> candidate.value.equals(value))
					.findFirst().orElseThrow(() -> new IllegalArgumentException("Unknown implementation " + value));
		}
	}

	private enum Metric {
		MAXIMUM_GAP,
		THROUGHPUT,
		FAIR_DELAY
	}

	record ScheduledRun(int ordinal, int round, Implementation implementation, Path artifact) {
	}

	record Evaluation(boolean passed,
			List<String> failures,
			PairedBenchmarkStatistics.RatioConfidenceInterval maximumGapRatio,
			double baselineMeanThroughput,
			double candidateMeanThroughput,
			double baselineMeanFairTurnNanos,
			double candidateMeanFairTurnNanos,
			String baselineClassPathSha256,
			String candidateClassPathSha256,
			String runtimeSha256,
			String hostSha256,
			String harnessSha256) {
	}

	record Prepared(Path root,
			String baselineSha,
			String candidateSha,
			Path baselineWorktree,
			Path candidateWorktree,
			Path baselineClasses,
			Path candidateClasses,
			String baselineProductionSha256,
			String candidateProductionSha256,
			String hardwareDescription,
			boolean enforce,
			AdversarialBatchLivenessBenchmark.Config config,
			Duration candidateMaximumGap,
			double candidateMinimumThroughput) {

		Prepared {
			root = root.toAbsolutePath().normalize();
			baselineWorktree = baselineWorktree.toAbsolutePath().normalize();
			candidateWorktree = candidateWorktree.toAbsolutePath().normalize();
			baselineClasses = baselineClasses.toAbsolutePath().normalize();
			candidateClasses = candidateClasses.toAbsolutePath().normalize();
			if (!baselineSha.matches("[0-9a-f]{40}") || !candidateSha.matches("[0-9a-f]{40}")) {
				throw new IllegalArgumentException("full baseline and candidate SHA-1 values are required");
			}
			if (!baselineProductionSha256.matches("[0-9a-f]{64}")
					|| !candidateProductionSha256.matches("[0-9a-f]{64}")) {
				throw new IllegalArgumentException("production SHA-256 values are required");
			}
			if (hardwareDescription.isBlank() || hardwareDescription.indexOf('\n') >= 0
					|| hardwareDescription.indexOf('\r') >= 0) {
				throw new IllegalArgumentException("single-line hardware description is required");
			}
			if (candidateMaximumGap.isZero() || candidateMaximumGap.isNegative()
					|| !Double.isFinite(candidateMinimumThroughput) || candidateMinimumThroughput <= 0.0d) {
				throw new IllegalArgumentException("positive candidate liveness bounds are required");
			}
		}

		static Prepared fromArguments(Map<String, String> arguments) throws IOException {
			Set<String> allowed = Set.of("mode", "root", "baseline-sha", "candidate-sha",
					"baseline-worktree", "candidate-worktree", "baseline-classes", "candidate-classes",
					"hardware-description", "enforce", "read-workers", "write-workers", "queued-read-tasks",
					"pressure-interval-ms", "nondispatchable-phase-ms", "setup-timeout-ms",
					"fair-turn-bound-ms", "candidate-maximum-gap-ms", "candidate-minimum-throughput");
			rejectUnknown(arguments, allowed);
			Path root = Path.of(require(arguments, "root"));
			Path baselineWorktree = Path.of(require(arguments, "baseline-worktree"));
			Path candidateWorktree = Path.of(require(arguments, "candidate-worktree"));
			Path baselineClasses = Path.of(require(arguments, "baseline-classes"));
			Path candidateClasses = Path.of(require(arguments, "candidate-classes"));
			var config = new AdversarialBatchLivenessBenchmark.Config(
					Integer.parseInt(arguments.getOrDefault("read-workers", "3")),
					Integer.parseInt(arguments.getOrDefault("write-workers", "3")),
					Integer.parseInt(arguments.getOrDefault("queued-read-tasks", "96")),
					Duration.ofMillis(Long.parseLong(arguments.getOrDefault("pressure-interval-ms", "10"))),
					Duration.ofMillis(Long.parseLong(arguments.getOrDefault("nondispatchable-phase-ms", "240"))),
					Duration.ofMillis(Long.parseLong(arguments.getOrDefault("setup-timeout-ms", "5000"))),
					Duration.ofMillis(Long.parseLong(arguments.getOrDefault("fair-turn-bound-ms", "250"))));
			return new Prepared(root,
					requireSha(arguments, "baseline-sha"),
					requireSha(arguments, "candidate-sha"),
					baselineWorktree,
					candidateWorktree,
					baselineClasses,
					candidateClasses,
					contentSha(baselineClasses.toAbsolutePath().normalize()),
					contentSha(candidateClasses.toAbsolutePath().normalize()),
					arguments.getOrDefault("hardware-description", "unspecified"),
					Boolean.parseBoolean(arguments.getOrDefault("enforce", "true")),
					config,
					Duration.ofMillis(Long.parseLong(arguments.getOrDefault("candidate-maximum-gap-ms", "100"))),
					Double.parseDouble(arguments.getOrDefault("candidate-minimum-throughput", "20")));
		}

		static Prepared read(Path root) throws IOException {
			root = root.toAbsolutePath().normalize();
			var values = exactProperties(root.resolve(METADATA_FILE), METADATA_KEYS);
			if (!METADATA_SCHEMA.equals(require(values, "schema"))) {
				throw new IllegalArgumentException("Unknown metadata schema");
			}
			var config = new AdversarialBatchLivenessBenchmark.Config(
					Math.toIntExact(longValue(values, "read-workers")),
					Math.toIntExact(longValue(values, "write-workers")),
					Math.toIntExact(longValue(values, "queued-read-tasks")),
					Duration.ofNanos(longValue(values, "pressure-interval-nanos")),
					Duration.ofNanos(longValue(values, "nondispatchable-phase-nanos")),
					Duration.ofNanos(longValue(values, "setup-timeout-nanos")),
					Duration.ofNanos(longValue(values, "fair-turn-bound-nanos")));
			var prepared = new Prepared(root,
					require(values, "baseline-sha"),
					require(values, "candidate-sha"),
					Path.of(require(values, "baseline-worktree")),
					Path.of(require(values, "candidate-worktree")),
					Path.of(require(values, "baseline-classes")),
					Path.of(require(values, "candidate-classes")),
					require(values, "baseline-production-sha256"),
					require(values, "candidate-production-sha256"),
					require(values, "hardware-description"),
					Boolean.parseBoolean(require(values, "enforce")),
					config,
					Duration.ofNanos(longValue(values, "candidate-maximum-gap-nanos")),
					doubleValue(values, "candidate-minimum-throughput"));
			if (!prepared.configurationSha256().equals(require(values, "configuration-sha256"))) {
				throw new IllegalArgumentException("Metadata configuration hash mismatch");
			}
			return prepared;
		}

		String metadataText() {
			return "schema=" + METADATA_SCHEMA + '\n'
					+ "configuration-sha256=" + configurationSha256() + '\n'
					+ configurationText();
		}

		String configurationText() {
			return "baseline-sha=" + baselineSha + '\n'
					+ "candidate-sha=" + candidateSha + '\n'
					+ "baseline-worktree=" + baselineWorktree + '\n'
					+ "candidate-worktree=" + candidateWorktree + '\n'
					+ "baseline-classes=" + baselineClasses + '\n'
					+ "candidate-classes=" + candidateClasses + '\n'
					+ "baseline-production-sha256=" + baselineProductionSha256 + '\n'
					+ "candidate-production-sha256=" + candidateProductionSha256 + '\n'
					+ "hardware-description=" + hardwareDescription + '\n'
					+ "enforce=" + enforce + '\n'
					+ "read-workers=" + config.readWorkers() + '\n'
					+ "write-workers=" + config.writeWorkers() + '\n'
					+ "queued-read-tasks=" + config.queuedReadTasks() + '\n'
					+ "pressure-interval-nanos=" + config.pressureInterval().toNanos() + '\n'
					+ "nondispatchable-phase-nanos=" + config.nondispatchablePhase().toNanos() + '\n'
					+ "setup-timeout-nanos=" + config.setupTimeout().toNanos() + '\n'
					+ "fair-turn-bound-nanos=" + config.fairTurnBound().toNanos() + '\n'
					+ "candidate-maximum-gap-nanos=" + candidateMaximumGap.toNanos() + '\n'
					+ "candidate-minimum-throughput=" + candidateMinimumThroughput + '\n';
		}

		String configurationSha256() {
			return sha256(configurationText());
		}
	}

	record Artifact(int round,
			int ordinal,
			Implementation implementation,
			String buildSha,
			String configurationSha256,
			String hostSha256,
			String hardwareDescription,
			String runtimeSha256,
			String harnessSha256,
			String classPathSha256,
			String productionSha256,
			long processId,
			long startedEpochMillis,
			long finishedEpochMillis,
			boolean correctnessPassed,
			AdversarialBatchLivenessBenchmark.Result result) {

		void write(Path path) throws IOException {
			Files.createDirectories(path.getParent());
			Files.writeString(path, text(), StandardOpenOption.CREATE_NEW);
		}

		String text() {
			return "schema=" + ARTIFACT_SCHEMA + '\n'
					+ "round=" + round + '\n'
					+ "ordinal=" + ordinal + '\n'
					+ "implementation=" + implementation.value + '\n'
					+ "build-sha=" + buildSha + '\n'
					+ "configuration-sha256=" + configurationSha256 + '\n'
					+ "host-sha256=" + hostSha256 + '\n'
					+ "hardware-description=" + hardwareDescription + '\n'
					+ "runtime-sha256=" + runtimeSha256 + '\n'
					+ "harness-sha256=" + harnessSha256 + '\n'
					+ "classpath-sha256=" + classPathSha256 + '\n'
					+ "production-sha256=" + productionSha256 + '\n'
					+ "process-id=" + processId + '\n'
					+ "started-epoch-millis=" + startedEpochMillis + '\n'
					+ "finished-epoch-millis=" + finishedEpochMillis + '\n'
					+ "correctness-passed=" + correctnessPassed + '\n'
					+ "topology-proven=" + result.topologyProven() + '\n'
					+ "useful-read-completions=" + result.usefulReadCompletions() + '\n'
					+ "maximum-read-zero-progress-gap-nanos=" + result.maximumReadZeroProgressGapNanos() + '\n'
					+ "useful-read-throughput-per-second=" + result.usefulReadThroughputPerSecond() + '\n'
					+ "write-fair-turn-delay-nanos=" + result.writeFairTurnDelayNanos() + '\n'
					+ "nondispatchable-phase-nanos=" + result.nondispatchablePhaseNanos() + '\n'
					+ "pressure-interval-nanos=" + result.pressureIntervalNanos() + '\n';
		}

		static Artifact read(Path path) throws IOException {
			var values = exactProperties(path, ARTIFACT_KEYS);
			if (!ARTIFACT_SCHEMA.equals(require(values, "schema"))) {
				throw new IllegalArgumentException("Unknown worker artifact schema");
			}
			var result = new AdversarialBatchLivenessBenchmark.Result(
					Boolean.parseBoolean(require(values, "topology-proven")),
					longValue(values, "useful-read-completions"),
					longValue(values, "maximum-read-zero-progress-gap-nanos"),
					doubleValue(values, "useful-read-throughput-per-second"),
					longValue(values, "write-fair-turn-delay-nanos"),
					longValue(values, "nondispatchable-phase-nanos"),
					longValue(values, "pressure-interval-nanos"));
			return new Artifact(Math.toIntExact(longValue(values, "round")),
					Math.toIntExact(longValue(values, "ordinal")),
					Implementation.parse(require(values, "implementation")),
					require(values, "build-sha"),
					require(values, "configuration-sha256"),
					require(values, "host-sha256"),
					require(values, "hardware-description"),
					require(values, "runtime-sha256"),
					require(values, "harness-sha256"),
					require(values, "classpath-sha256"),
					require(values, "production-sha256"),
					longValue(values, "process-id"),
					longValue(values, "started-epoch-millis"),
					longValue(values, "finished-epoch-millis"),
					Boolean.parseBoolean(require(values, "correctness-passed")),
					result);
		}
	}
}
