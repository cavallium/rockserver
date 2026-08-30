package it.cavallium.rockserver.core.impl.benchmark;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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

/** Immutable fresh-process controller for finite-deadline scheduler contention evidence. */
public final class FiniteDeadlineContentionBenchmark {

	static final int DEFAULT_PAIRS = 10;
	static final String METADATA_SCHEMA = "rockserver-finite-deadline-contention-metadata-v2";
	static final String SCHEDULE_SCHEMA = "rockserver-finite-deadline-contention-schedule-v2";
	static final String WORKER_SCHEMA = "rockserver-finite-deadline-contention-worker-v2";
	static final String RESULT_SCHEMA = "rockserver-finite-deadline-contention-comparison-v2";
	static final String METADATA_FILE = "metadata.properties";
	static final String SCHEDULE_FILE = "schedule.tsv";
	static final String RESULTS_JSON = "deadline-results-v2.json";
	static final String RESULTS_MARKDOWN = "deadline-results-v2.md";
	static final List<Metric> METRICS = List.of(
			new Metric("attempts-throughput", "attempts_per_second", true),
			new Metric("useful-throughput", "useful_runs_per_second", true),
			new Metric("latency-throughput", null, true),
			new Metric("latency-queue-p99", "profile.latency.queue_p99_nanos", false),
			new Metric("latency-end-to-end-p99", "profile.latency.end_to_end_p99_nanos", false),
			new Metric("latency-maximum-progress-gap",
					"profile.latency.maximum_progress_gap_nanos", false),
			new Metric("cpu-per-attempt", "process.cpu_nanos_per_attempt", false),
			new Metric("allocation-per-attempt", "process.allocated_bytes_per_attempt", false));
	private static final Set<String> METADATA_KEYS = Set.of(
			"schema", "configuration-sha256", "metric-set-sha256", "baseline-sha", "candidate-sha",
			"baseline-worktree", "candidate-worktree", "baseline-classes", "candidate-classes",
			"baseline-production-sha256", "candidate-production-sha256", "hardware-description",
			"enforce", "fixed-pairs", "adaptive-stopping", "family-wise-alpha",
			"throughput-minimum-ratio", "cost-maximum-ratio", "multiplicity",
			"scheduler-operations", "scheduler-warmup-operations", "scheduler-submitters",
			"scheduler-read-workers", "scheduler-write-workers", "scheduler-analytical-limit",
			"scheduler-foreground-capacity", "scheduler-batch-capacity", "scheduler-work-tokens",
			"scheduler-cooperative-yields", "scheduler-cooperative-parks",
			"scheduler-expired-deadline-percent", "scheduler-cancellation-percent",
			"scheduler-failure-percent", "scheduler-cooperative-percent",
			"scheduler-alternate-storage-pressure", "scheduler-seed", "scheduler-timeout-nanos");
	private static final Set<String> IDENTITY_KEYS = Set.of(
			"artifact-schema", "round", "ordinal", "implementation", "build-sha",
			"configuration-sha256", "metric-set-sha256", "host-sha256", "hardware-description",
			"runtime-sha256", "harness-sha256", "classpath-sha256", "production-sha256",
			"process-id", "started-epoch-millis", "finished-epoch-millis",
			"gate-correctness", "gate-terminal-conservation", "gate-progress",
			"gate-drain", "gate-pressure", "gate-bounds");
	private static final Set<String> WORKER_KEYS = workerKeys();

	private FiniteDeadlineContentionBenchmark() {}

	public static void main(String[] args) throws Exception {
		Map<String, String> arguments = arguments(args);
		switch (Mode.parse(arguments.getOrDefault("mode", "evaluate"))) {
			case PREPARE -> prepare(Prepared.fromArguments(
					FiniteDeadlineContentionPrecisionPlanner.expandPlannedPrepare(arguments)));
			case EXECUTE -> execute(requiredRoot(arguments, Set.of("mode", "root")));
			case WORKER -> worker(arguments);
			case EVALUATE -> evaluate(requiredRoot(arguments, Set.of("mode", "root")));
		}
	}

	static void prepare(Prepared prepared) throws IOException {
		if (Files.exists(prepared.root())) {
			throw new IllegalArgumentException("Benchmark root already exists: " + prepared.root());
		}
		AdversarialBatchLivenessPairedBenchmark.verifyProductionCheckout(
				prepared.baselineWorktree(), prepared.baselineSha());
		AdversarialBatchLivenessPairedBenchmark.verifyProductionCheckout(
				prepared.candidateWorktree(), prepared.candidateSha());
		Path parent = prepared.root().getParent();
		if (parent != null) Files.createDirectories(parent);
		Files.createDirectory(prepared.root());
		Files.writeString(prepared.root().resolve(METADATA_FILE), prepared.metadataText(),
				StandardOpenOption.CREATE_NEW);
		Files.writeString(prepared.root().resolve(SCHEDULE_FILE), scheduleText(prepared),
				StandardOpenOption.CREATE_NEW);
	}

	static void execute(Path root) throws Exception {
		Prepared prepared = Prepared.read(root);
		validateSchedule(prepared);
		String controllerClassPath = System.getProperty("java.class.path");
		Path java = Path.of(System.getProperty("java.home"), "bin", "java");
		for (ScheduledRun run : schedule(prepared)) {
			Path selectedClasses = run.implementation() == Implementation.BASELINE
					? prepared.baselineClasses() : prepared.candidateClasses();
			Path selectedWorktree = run.implementation() == Implementation.BASELINE
					? prepared.baselineWorktree() : prepared.candidateWorktree();
			String selectedSha = run.implementation() == Implementation.BASELINE
					? prepared.baselineSha() : prepared.candidateSha();
			String selectedClassPath = replaceProductionClasses(controllerClassPath,
					prepared.candidateClasses(), selectedClasses);
			Process process = new ProcessBuilder(java.toString(),
					"-XX:+EnableDynamicAgentLoading", "--enable-native-access=ALL-UNNAMED",
					"-cp", selectedClassPath, FiniteDeadlineContentionBenchmark.class.getName(),
					"--mode=worker", "--root=" + prepared.root(), "--round=" + run.round(),
					"--ordinal=" + run.ordinal(), "--implementation=" + run.implementation().value,
					"--build-sha=" + selectedSha)
					.directory(selectedWorktree.toFile()).inheritIO().start();
			int exit = process.waitFor();
			if (exit != 0) {
				throw new IllegalStateException("Fresh deadline worker failed at ordinal "
						+ run.ordinal() + " with exit " + exit);
			}
		}
		evaluate(prepared.root());
	}

	private static void worker(Map<String, String> arguments) throws Exception {
		Path root = requiredRoot(arguments,
				Set.of("mode", "root", "round", "ordinal", "implementation", "build-sha"));
		Prepared prepared = Prepared.read(root);
		validateSchedule(prepared);
		int round = integer(arguments, "round");
		int ordinal = integer(arguments, "ordinal");
		Implementation implementation = Implementation.parse(require(arguments, "implementation"));
		ScheduledRun run = scheduledRun(prepared, ordinal);
		if (run.round() != round || run.implementation() != implementation) {
			throw new IllegalArgumentException("Worker does not match the immutable schedule");
		}
		assertWorkerOrder(prepared, run);
		String buildSha = requireSha(arguments, "build-sha");
		String expectedBuild = implementation == Implementation.BASELINE
				? prepared.baselineSha() : prepared.candidateSha();
		if (!buildSha.equals(expectedBuild)) throw new IllegalArgumentException("Worker build SHA mismatch");
		if (prepared.enforce()) {
			AdversarialBatchLivenessPairedBenchmark.verifyProductionCheckout(Path.of("."), buildSha);
		}
		URI productionLocation = RWScheduler.class.getProtectionDomain().getCodeSource().getLocation().toURI();
		String productionSha = contentSha(Path.of(productionLocation));
		String expectedProduction = implementation == Implementation.BASELINE
				? prepared.baselineProductionSha256() : prepared.candidateProductionSha256();
		if (!productionSha.equals(expectedProduction)) {
			throw new IllegalStateException("Loaded scheduler production directory differs from preparation");
		}
		long started = System.currentTimeMillis();
		SchedulerHighContentionBenchmark.run(prepared.warmupConfig());
		SchedulerHighContentionBenchmark.Result result =
				SchedulerHighContentionBenchmark.run(prepared.measuredConfig());
		result.assertCorrect();
		long finished = System.currentTimeMillis();
		Artifact.fromResult(prepared, run, buildSha, productionSha, started, finished, result)
				.write(run.artifact());
	}

	static Result evaluate(Path root) throws IOException {
		Prepared prepared = Prepared.read(root);
		Result result = inspect(prepared);
		Instant evaluatedAt = Instant.now();
		Files.writeString(root.resolve(RESULTS_JSON), resultJson(prepared, result, evaluatedAt),
				StandardOpenOption.CREATE_NEW);
		Files.writeString(root.resolve(RESULTS_MARKDOWN), resultMarkdown(prepared, result),
				StandardOpenOption.CREATE_NEW);
		if (prepared.enforce() && result.evaluation().decision() != PairedPerformanceContractV2.Decision.PASS) {
			throw new IllegalStateException("Finite-deadline comparison "
					+ result.evaluation().decision().name().toLowerCase(Locale.ROOT));
		}
		return result;
	}

	static Result inspect(Prepared prepared) throws IOException {
		var failures = new ArrayList<String>();
		try {
			validateSchedule(prepared);
		} catch (RuntimeException invalid) {
			failures.add("schedule: " + invalid.getMessage());
		}
		var baseline = metricVectors(prepared.fixedPairs());
		var candidate = metricVectors(prepared.fixedPairs());
		var processIds = new LinkedHashSet<Long>();
		long previousFinished = Long.MIN_VALUE;
		String runtimeSha = null;
		String hostSha = null;
		String harnessSha = null;
		String baselineClassPath = null;
		String candidateClassPath = null;
		String expectedRuntime = AdversarialBatchLivenessPairedBenchmark.runtimeSha();
		String expectedHost = AdversarialBatchLivenessPairedBenchmark.hostSha(prepared.hardwareDescription());
		String expectedHarness;
		try {
			expectedHarness = harnessSha();
		} catch (Exception malformed) {
			throw new IOException("Cannot identify benchmark harness", malformed);
		}
		for (ScheduledRun run : schedule(prepared)) {
			Artifact artifact;
			try {
				artifact = Artifact.read(run.artifact());
			} catch (RuntimeException | IOException invalid) {
				failures.add("ordinal " + run.ordinal() + ": " + invalid.getMessage());
				continue;
			}
			String prefix = "ordinal " + run.ordinal() + ": ";
			if (artifact.integer("round") != run.round()
					|| artifact.integer("ordinal") != run.ordinal()
					|| artifact.implementation() != run.implementation()) {
				failures.add(prefix + "schedule identity mismatch");
			}
			String expectedBuild = run.implementation() == Implementation.BASELINE
					? prepared.baselineSha() : prepared.candidateSha();
			String expectedProduction = run.implementation() == Implementation.BASELINE
					? prepared.baselineProductionSha256() : prepared.candidateProductionSha256();
			if (!artifact.value("build-sha").equals(expectedBuild)) failures.add(prefix + "build SHA mismatch");
			if (!artifact.value("production-sha256").equals(expectedProduction)) {
				failures.add(prefix + "production directory mismatch");
			}
			if (!artifact.value("configuration-sha256").equals(prepared.configurationSha256())) {
				failures.add(prefix + "configuration mismatch");
			}
			if (!artifact.value("metric-set-sha256").equals(metricSetSha256())) {
				failures.add(prefix + "metric set mismatch");
			}
			if (!artifact.value("hardware-description").equals(prepared.hardwareDescription())
					|| !artifact.value("host-sha256").equals(expectedHost)) {
				failures.add(prefix + "hardware identity mismatch");
			}
			if (!artifact.value("runtime-sha256").equals(expectedRuntime)) failures.add(prefix + "runtime mismatch");
			if (!artifact.value("harness-sha256").equals(expectedHarness)) failures.add(prefix + "harness mismatch");
			for (String gate : List.of("gate-correctness", "gate-terminal-conservation", "gate-progress",
					"gate-drain", "gate-pressure", "gate-bounds")) {
				if (!artifact.bool(gate)) failures.add(prefix + gate + " failed");
			}
			long processId = artifact.longValue("process-id");
			long started = artifact.longValue("started-epoch-millis");
			long finished = artifact.longValue("finished-epoch-millis");
			if (processId <= 0L || !processIds.add(processId)) failures.add(prefix + "process ID reused or invalid");
			if (started <= 0L || finished < started) failures.add(prefix + "timestamps are invalid");
			if (started < previousFinished) failures.add(prefix + "fresh serial workers overlapped or reordered");
			previousFinished = Math.max(previousFinished, finished);
			runtimeSha = stable(runtimeSha, artifact.value("runtime-sha256"), prefix + "runtime changed", failures);
			hostSha = stable(hostSha, artifact.value("host-sha256"), prefix + "host changed", failures);
			harnessSha = stable(harnessSha, artifact.value("harness-sha256"), prefix + "harness changed", failures);
			if (run.implementation() == Implementation.BASELINE) {
				baselineClassPath = stable(baselineClassPath, artifact.value("classpath-sha256"),
						prefix + "baseline classpath changed", failures);
			} else {
				candidateClassPath = stable(candidateClassPath, artifact.value("classpath-sha256"),
						prefix + "candidate classpath changed", failures);
			}
			validateHardGates(prepared, artifact, prefix, failures);
			Map<String, double[]> target = run.implementation() == Implementation.BASELINE ? baseline : candidate;
			int pair = run.round() - 1;
			for (Metric metric : METRICS) target.get(metric.name())[pair] = artifact.metric(metric);
		}
		var specifications = new ArrayList<PairedPerformanceContractV2.MetricSpec>();
		var samples = new LinkedHashMap<String, PairedPerformanceContractV2.MetricSamples>();
		for (Metric metric : METRICS) {
			specifications.add(metric.higherIsBetter()
					? PairedPerformanceContractV2.MetricSpec.throughput(metric.name(), false)
					: PairedPerformanceContractV2.MetricSpec.cost(metric.name(), false));
			samples.put(metric.name(), new PairedPerformanceContractV2.MetricSamples(
					baseline.get(metric.name()), candidate.get(metric.name())));
		}
		var evaluation = PairedPerformanceContractV2.evaluate(
				specifications, samples, failures, prepared.fixedPairs());
		return new Result(evaluation, Map.copyOf(samples), runtimeSha, hostSha, harnessSha,
				baselineClassPath, candidateClassPath);
	}

	private static void validateHardGates(Prepared prepared, Artifact artifact,
			String prefix, List<String> failures) {
		try {
			if (!artifact.value("schema").equals("rockserver-scheduler-high-contention-v2")
					|| artifact.longValue("operations") != prepared.operations()
					|| artifact.longValue("seed") != prepared.seed()
					|| artifact.longValue("submitters") != prepared.submitters()
					|| !artifact.bool("latency_finite_deadlines")) {
				failures.add(prefix + "workload identity mismatch");
			}
			long operations = artifact.longValue("operations");
			long terminal = 0L;
			for (RWScheduler.TerminalOutcome outcome : RWScheduler.TerminalOutcome.values()) {
				terminal = Math.addExact(terminal,
						artifact.nonNegative("outcome." + outcome.name().toLowerCase(Locale.ROOT)));
			}
			if (terminal != operations || artifact.longValue("outcome.shutdown") != 0L
					|| artifact.longValue("outcome.run") != artifact.longValue("runs")) {
				failures.add(prefix + "terminal conservation gate failed");
			}
			if (artifact.longValue("yield_transitions") <= 0L
					|| artifact.longValue("park_transitions") <= 0L
					|| artifact.longValue("pressure_transitions") <= 0L) {
				failures.add(prefix + "cooperative/pressure transition gate failed");
			}
			for (WorkloadProfile profile : WorkloadProfile.values()) {
				String base = "profile." + profile.name().toLowerCase(Locale.ROOT) + ".";
				if (artifact.longValue(base + "attempts") <= 0L || artifact.longValue(base + "runs") <= 0L) {
					failures.add(prefix + "profile progress failed for " + profile);
				}
			}
			for (OperationFamily family : OperationFamily.values()) {
				String base = "family." + family.name().toLowerCase(Locale.ROOT) + ".";
				if (artifact.longValue(base + "attempts") <= 0L || artifact.longValue(base + "runs") <= 0L) {
					failures.add(prefix + "family progress failed for " + family);
				}
			}
			for (RWScheduler.Pool pool : RWScheduler.Pool.values()) {
				String base = "pool." + pool.name().toLowerCase(Locale.ROOT) + ".";
				if (artifact.longValue(base + "peak_queued") > artifact.longValue(base + "queue_bound")
						|| artifact.longValue(base + "peak_outstanding") > artifact.longValue(base + "outstanding_bound")) {
					failures.add(prefix + "pool bound failed for " + pool);
				}
			}
			long batchRead = artifact.longValue("pressure.batch_read.pressured_runs")
					+ artifact.longValue("pressure.batch_read.unpressured_runs");
			long batchWrite = artifact.longValue("pressure.batch_write.pressured_runs")
					+ artifact.longValue("pressure.batch_write.unpressured_runs");
			if (batchRead <= 0L || batchWrite <= 0L
					|| artifact.longValue("pressure.foreground_read.pressured_runs") <= 0L
					|| artifact.longValue("pressure.foreground_write.pressured_runs") <= 0L) {
				failures.add(prefix + "pressure coverage gate failed");
			}
			for (Metric metric : METRICS) artifact.metric(metric);
		} catch (RuntimeException malformed) {
			failures.add(prefix + "invalid hard-gate value: " + malformed.getMessage());
		}
	}

	static String resultJson(Prepared prepared, Result result, Instant evaluatedAt) {
		var out = new StringBuilder("{\n")
				.append("  \"schema\": \"").append(RESULT_SCHEMA).append("\",\n")
				.append("  \"evaluated_at\": \"").append(evaluatedAt).append("\",\n")
				.append("  \"baseline_sha\": \"").append(prepared.baselineSha()).append("\",\n")
				.append("  \"candidate_sha\": \"").append(prepared.candidateSha()).append("\",\n")
				.append("  \"configuration_sha256\": \"").append(prepared.configurationSha256()).append("\",\n")
				.append("  \"metric_set_sha256\": \"").append(metricSetSha256()).append("\",\n")
				.append("  \"fixed_pairs\": ").append(prepared.fixedPairs()).append(",\n")
				.append("  \"fresh_processes\": ").append(prepared.fixedPairs() * 2).append(",\n")
				.append("  \"adaptive_stopping\": false,\n")
				.append("  \"family_wise_alpha\": ").append(PairedPerformanceContractV2.FAMILY_WISE_ALPHA).append(",\n")
				.append("  \"throughput_minimum_ratio\": ").append(PairedPerformanceContractV2.THROUGHPUT_MINIMUM_RATIO).append(",\n")
				.append("  \"cost_maximum_ratio\": ").append(PairedPerformanceContractV2.COST_MAXIMUM_RATIO).append(",\n")
				.append("  \"multiplicity\": \"holm-bonferroni\",\n")
				.append("  \"runtime_sha256\": ").append(jsonStringOrNull(result.runtimeSha256())).append(",\n")
				.append("  \"host_sha256\": ").append(jsonStringOrNull(result.hostSha256())).append(",\n")
				.append("  \"harness_sha256\": ").append(jsonStringOrNull(result.harnessSha256())).append(",\n")
				.append("  \"baseline_production_sha256\": \"").append(prepared.baselineProductionSha256()).append("\",\n")
				.append("  \"candidate_production_sha256\": \"").append(prepared.candidateProductionSha256()).append("\",\n")
				.append("  \"baseline_classpath_sha256\": ").append(jsonStringOrNull(result.baselineClassPathSha256())).append(",\n")
				.append("  \"candidate_classpath_sha256\": ").append(jsonStringOrNull(result.candidateClassPathSha256())).append(",\n")
				.append("  \"decision\": \"").append(result.evaluation().decision().name().toLowerCase(Locale.ROOT)).append("\",\n")
				.append("  \"failures\": ").append(jsonArray(result.evaluation().failures())).append(",\n")
				.append("  \"inconclusive_metrics\": ").append(jsonArray(result.evaluation().inconclusiveMetrics())).append(",\n")
				.append("  \"metrics\": {\n");
		for (int index = 0; index < METRICS.size(); index++) {
			Metric metric = METRICS.get(index);
			var samples = result.samples().get(metric.name());
			var value = result.evaluation().metrics().get(metric.name());
			out.append("    \"").append(metric.name()).append("\": {")
					.append("\"direction\": \"").append(metric.higherIsBetter() ? "higher" : "lower").append("\",")
					.append(" \"baseline\": ").append(jsonNumbers(samples.baseline())).append(',')
					.append(" \"candidate\": ").append(jsonNumbers(samples.candidate())).append(',');
			if (value == null) {
				out.append(" \"ratio_mean\": null, \"lower_95\": null, \"upper_95\": null,")
						.append(" \"regression_holm_p\": null, \"regression_demonstrated\": false,")
						.append(" \"noninferiority_p\": null, \"noninferiority_proven\": false}");
			} else {
				out.append(" \"ratio_mean\": ").append(jsonNumber(value.interval().mean())).append(',')
						.append(" \"lower_95\": ").append(jsonNumber(value.interval().lower95())).append(',')
						.append(" \"upper_95\": ").append(jsonNumber(value.interval().upper95())).append(',')
						.append(" \"regression_holm_p\": ").append(jsonNumber(value.regressionHolmAdjustedPValue())).append(',')
						.append(" \"regression_demonstrated\": ").append(value.regressionDemonstrated()).append(',')
						.append(" \"noninferiority_p\": ").append(jsonNumber(value.nonInferiorityPValue())).append(',')
						.append(" \"noninferiority_proven\": ").append(value.nonInferiorityProven()).append('}');
			}
			out.append(index + 1 == METRICS.size() ? '\n' : ',').append(index + 1 == METRICS.size() ? "" : "\n");
		}
		return out.append("  }\n}\n").toString();
	}

	static String resultMarkdown(Prepared prepared, Result result) {
		var out = new StringBuilder("# Finite-deadline contention comparison\n\n")
				.append("- Decision: **").append(result.evaluation().decision()).append("**\n")
				.append("- Baseline: `").append(prepared.baselineSha()).append("`\n")
				.append("- Candidate: `").append(prepared.candidateSha()).append("`\n")
				.append("- Fixed counterbalanced pairs: `").append(prepared.fixedPairs()).append("` (`")
				.append(prepared.fixedPairs() * 2).append("` fresh serial JVMs)\n")
				.append("- Contract: `0.99` throughput, `1.02` cost, Holm FWER `0.05`, no adaptive stopping\n\n")
				.append("| Metric | Ratio | 95% CI | Holm regression p | NI |\n")
				.append("|---|---:|---:|---:|---:|\n");
		for (Metric metric : METRICS) {
			var value = result.evaluation().metrics().get(metric.name());
			out.append("| ").append(metric.name()).append(" | ");
			if (value == null) out.append("n/a | n/a | n/a | false |\n");
			else out.append(format(value.interval().mean())).append(" | [")
					.append(format(value.interval().lower95())).append(", ")
					.append(format(value.interval().upper95())).append("] | ")
					.append(format(value.regressionHolmAdjustedPValue())).append(" | ")
					.append(value.nonInferiorityProven()).append(" |\n");
		}
		if (!result.evaluation().failures().isEmpty()) {
			out.append("\nFailures:\n");
			for (String failure : result.evaluation().failures()) out.append("- ").append(failure).append('\n');
		}
		return out.toString();
	}

	static List<ScheduledRun> schedule(Prepared prepared) {
		var result = new ArrayList<ScheduledRun>(prepared.fixedPairs() * 2);
		int ordinal = 0;
		for (int round = 1; round <= prepared.fixedPairs(); round++) {
			List<Implementation> order = (round & 1) == 1
					? List.of(Implementation.BASELINE, Implementation.CANDIDATE)
					: List.of(Implementation.CANDIDATE, Implementation.BASELINE);
			for (Implementation implementation : order) {
				Path artifact = prepared.root().resolve("raw-scheduler")
						.resolve("round-%03d-%s.properties".formatted(round, implementation.value));
				result.add(new ScheduledRun(++ordinal, round, implementation, artifact));
			}
		}
		return List.copyOf(result);
	}

	private static String scheduleText(Prepared prepared) {
		var out = new StringBuilder("schema\t").append(SCHEDULE_SCHEMA).append('\n')
				.append("configuration-sha256\t").append(prepared.configurationSha256()).append('\n')
				.append("metric-set-sha256\t").append(metricSetSha256()).append('\n')
				.append("pairs\t").append(prepared.fixedPairs()).append('\n')
				.append("fresh-processes\t").append(prepared.fixedPairs() * 2).append('\n')
				.append("adaptive-stopping\tfalse\n")
				.append("ordinal\tround\timplementation\tartifact\n");
		for (ScheduledRun run : schedule(prepared)) {
			out.append(run.ordinal()).append('\t').append(run.round()).append('\t')
					.append(run.implementation().value).append('\t')
					.append(prepared.root().relativize(run.artifact())).append('\n');
		}
		return out.toString();
	}

	private static void validateSchedule(Prepared prepared) throws IOException {
		Path schedule = prepared.root().resolve(SCHEDULE_FILE);
		if (!Files.isRegularFile(schedule) || !Files.readString(schedule).equals(scheduleText(prepared))) {
			throw new IllegalArgumentException("Schedule differs from canonical prepared bytes");
		}
	}

	private static ScheduledRun scheduledRun(Prepared prepared, int ordinal) {
		return schedule(prepared).stream().filter(run -> run.ordinal() == ordinal).findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Ordinal is outside the fixed schedule"));
	}

	private static void assertWorkerOrder(Prepared prepared, ScheduledRun target) {
		for (ScheduledRun run : schedule(prepared)) {
			if (run.ordinal() < target.ordinal() && !Files.isRegularFile(run.artifact())) {
				throw new IllegalStateException("Earlier artifact is missing: " + run.artifact());
			}
			if (run.ordinal() >= target.ordinal() && Files.exists(run.artifact())) {
				throw new IllegalStateException("Artifact already or prematurely exists: " + run.artifact());
			}
		}
	}

	private static Map<String, double[]> metricVectors(int pairs) {
		var values = new LinkedHashMap<String, double[]>();
		for (Metric metric : METRICS) values.put(metric.name(), new double[pairs]);
		return values;
	}

	private static Set<String> workerKeys() {
		var keys = new LinkedHashSet<>(IDENTITY_KEYS);
		keys.addAll(Set.of("schema", "operations", "seed", "submitters", "latency_finite_deadlines",
				"elapsed_nanos", "attempts_per_second", "useful_runs_per_second", "accepted", "runs",
				"yield_transitions", "park_transitions", "pressure_transitions", "injected_failures",
				"pressure.batch_read.pressured_runs", "pressure.batch_read.unpressured_runs",
				"pressure.batch_write.pressured_runs", "pressure.batch_write.unpressured_runs",
				"pressure.foreground_read.pressured_runs", "pressure.foreground_read.unpressured_runs",
				"pressure.foreground_write.pressured_runs", "pressure.foreground_write.unpressured_runs",
				"process.cpu_nanos", "process.cpu_nanos_per_attempt", "process.allocated_bytes",
				"process.allocated_bytes_per_attempt", "process.gc_collections", "process.gc_millis",
				"process.peak_live_heap_bytes", "process.peak_direct_memory_bytes",
				"process.peak_resident_set_bytes", "process.peak_threads", "process.peak_native_handles"));
		for (RWScheduler.TerminalOutcome outcome : RWScheduler.TerminalOutcome.values()) {
			keys.add("outcome." + outcome.name().toLowerCase(Locale.ROOT));
		}
		for (WorkloadProfile profile : WorkloadProfile.values()) {
			String base = "profile." + profile.name().toLowerCase(Locale.ROOT) + ".";
			for (String suffix : List.of("attempts", "runs", "queue_p99_nanos", "execution_p99_nanos",
					"end_to_end_p99_nanos", "maximum_progress_gap_nanos")) keys.add(base + suffix);
		}
		for (OperationFamily family : OperationFamily.values()) {
			String base = "family." + family.name().toLowerCase(Locale.ROOT) + ".";
			for (String suffix : List.of("attempts", "runs", "queue_p99_nanos", "execution_p99_nanos",
					"end_to_end_p99_nanos", "maximum_progress_gap_nanos")) keys.add(base + suffix);
		}
		for (RWScheduler.Pool pool : RWScheduler.Pool.values()) {
			String base = "pool." + pool.name().toLowerCase(Locale.ROOT) + ".";
			for (String suffix : List.of("peak_active", "peak_queued", "peak_outstanding", "queue_bound",
					"outstanding_bound", "batch_limited_observed")) keys.add(base + suffix);
		}
		return Set.copyOf(keys);
	}

	private static String replaceProductionClasses(String classPath, Path candidateClasses, Path selectedClasses) {
		Path candidate = candidateClasses.toAbsolutePath().normalize();
		Path selected = selectedClasses.toAbsolutePath().normalize();
		var entries = new ArrayList<String>();
		boolean replaced = false;
		for (String entry : classPath.split(java.util.regex.Pattern.quote(File.pathSeparator))) {
			Path normalized = Path.of(entry).toAbsolutePath().normalize();
			if (normalized.equals(candidate)) {
				entries.add(selected.toString());
				replaced = true;
			} else entries.add(normalized.toString());
		}
		if (!replaced) throw new IllegalArgumentException("Candidate production classes are absent from classpath");
		return String.join(File.pathSeparator, entries);
	}

	private static String contentSha(Path path) throws IOException {
		if (!Files.isDirectory(path)) throw new IllegalArgumentException("Missing class directory " + path);
		return GrpcRetainedReadBenchmark.classPathContentSha256ForTesting(path.toString());
	}

	static String harnessSha() throws Exception {
		URI location = FiniteDeadlineContentionBenchmark.class.getProtectionDomain().getCodeSource().getLocation().toURI();
		return contentSha(Path.of(location));
	}

	static String metricSetSha256() {
		var text = new StringBuilder();
		for (Metric metric : METRICS) {
			text.append(metric.name()).append('\t').append(metric.higherIsBetter() ? "higher" : "lower")
					.append('\t').append(metric.property()).append('\n');
		}
		text.append("throughput-minimum-ratio=").append(PairedPerformanceContractV2.THROUGHPUT_MINIMUM_RATIO).append('\n')
				.append("cost-maximum-ratio=").append(PairedPerformanceContractV2.COST_MAXIMUM_RATIO).append('\n')
				.append("family-wise-alpha=").append(PairedPerformanceContractV2.FAMILY_WISE_ALPHA).append('\n')
				.append("multiplicity=holm-bonferroni\n");
		return AdversarialBatchLivenessPairedBenchmark.sha256(text.toString());
	}

	private static Map<String, String> arguments(String[] args) {
		var values = new LinkedHashMap<String, String>();
		for (String argument : args) {
			if (!argument.startsWith("--") || argument.indexOf('=') <= 2) {
				throw new IllegalArgumentException("Expected --key=value, got " + argument);
			}
			int separator = argument.indexOf('=');
			String key = argument.substring(2, separator);
			String value = argument.substring(separator + 1);
			if (value.isBlank() || values.putIfAbsent(key, value) != null) {
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
		if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing " + key);
		return value;
	}

	private static String requireSha(Map<String, String> values, String key) {
		String value = require(values, key);
		if (!value.matches("[0-9a-f]{40}")) throw new IllegalArgumentException(key + " must be a full SHA-1");
		return value;
	}

	private static int integer(Map<String, String> values, String key) {
		try { return Integer.parseInt(require(values, key)); }
		catch (NumberFormatException invalid) { throw new IllegalArgumentException(key + " must be an integer", invalid); }
	}

	private static long longValue(Map<String, String> values, String key) {
		try { return Long.parseLong(require(values, key)); }
		catch (NumberFormatException invalid) { throw new IllegalArgumentException(key + " must be a long", invalid); }
	}

	private static boolean booleanValue(Map<String, String> values, String key) {
		String value = require(values, key);
		if (!value.equals("true") && !value.equals("false")) {
			throw new IllegalArgumentException(key + " must be true or false");
		}
		return Boolean.parseBoolean(value);
	}

	private static boolean argumentBoolean(Map<String, String> values, String key, boolean fallback) {
		if (!values.containsKey(key)) return fallback;
		return booleanValue(values, key);
	}

	private static Map<String, String> exactProperties(Path path, Set<String> expected) throws IOException {
		if (!Files.isRegularFile(path)) throw new IllegalArgumentException("Missing file " + path);
		var values = new LinkedHashMap<String, String>();
		String[] lines = Files.readString(path).split("\\R", -1);
		for (int index = 0; index < lines.length; index++) {
			String line = lines[index];
			if (line.isEmpty() && index == lines.length - 1) continue;
			int separator = line.indexOf('=');
			if (separator <= 0 || separator != line.lastIndexOf('=')) {
				throw new IllegalArgumentException("Malformed property at line " + (index + 1));
			}
			String key = line.substring(0, separator);
			String value = line.substring(separator + 1);
			if (value.isEmpty() || values.putIfAbsent(key, value) != null) {
				throw new IllegalArgumentException("Empty or duplicate property " + key);
			}
		}
		if (!values.keySet().equals(expected)) {
			throw new IllegalArgumentException("Property keys differ in " + path);
		}
		return Map.copyOf(values);
	}

	private static String stable(String previous, String current, String failure, List<String> failures) {
		if (previous != null && !previous.equals(current)) failures.add(failure);
		return previous == null ? current : previous;
	}

	private static String jsonArray(List<String> values) {
		return values.stream().map(value -> "\"" + json(value) + "\"")
				.collect(java.util.stream.Collectors.joining(",", "[", "]"));
	}

	private static String jsonNumbers(double[] values) {
		return Arrays.stream(values).mapToObj(FiniteDeadlineContentionBenchmark::jsonNumber)
				.collect(java.util.stream.Collectors.joining(",", "[", "]"));
	}

	private static String jsonNumber(double value) { return Double.isFinite(value) ? Double.toString(value) : "null"; }
	private static String json(String value) { return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n"); }
	private static String jsonStringOrNull(String value) { return value == null ? "null" : "\"" + json(value) + "\""; }
	private static String format(double value) { return Double.isFinite(value) ? String.format(Locale.ROOT, "%.6f", value) : "n/a"; }

	enum Mode { PREPARE, EXECUTE, WORKER, EVALUATE;
		static Mode parse(String value) {
			try { return valueOf(value.toUpperCase(Locale.ROOT)); }
			catch (IllegalArgumentException invalid) { throw new IllegalArgumentException("Unknown mode " + value, invalid); }
		}
	}

	enum Implementation { BASELINE("baseline"), CANDIDATE("candidate");
		final String value;
		Implementation(String value) { this.value = value; }
		static Implementation parse(String value) {
			return Arrays.stream(values()).filter(item -> item.value.equals(value)).findFirst()
					.orElseThrow(() -> new IllegalArgumentException("Unknown implementation " + value));
		}
	}

	record Metric(String name, String property, boolean higherIsBetter) {}
	record ScheduledRun(int ordinal, int round, Implementation implementation, Path artifact) {}
	record Result(PairedPerformanceContractV2.Evaluation evaluation,
			Map<String, PairedPerformanceContractV2.MetricSamples> samples,
			String runtimeSha256, String hostSha256, String harnessSha256,
			String baselineClassPathSha256, String candidateClassPathSha256) {}

	record Prepared(Path root, String baselineSha, String candidateSha,
			Path baselineWorktree, Path candidateWorktree, Path baselineClasses, Path candidateClasses,
			String baselineProductionSha256, String candidateProductionSha256,
			String hardwareDescription, boolean enforce, int fixedPairs,
			int operations, int warmupOperations, int submitters, int readWorkers, int writeWorkers,
			int analyticalLimit, int foregroundCapacity, int batchCapacity, int workTokens,
			int cooperativeYields, int cooperativeParks, int expiredDeadlinePercent,
			int cancellationPercent, int failurePercent, int cooperativePercent,
			boolean alternateStoragePressure, long seed, Duration timeout) {

		Prepared {
			root = root.toAbsolutePath().normalize();
			baselineWorktree = baselineWorktree.toAbsolutePath().normalize();
			candidateWorktree = candidateWorktree.toAbsolutePath().normalize();
			baselineClasses = baselineClasses.toAbsolutePath().normalize();
			candidateClasses = candidateClasses.toAbsolutePath().normalize();
			if (!baselineSha.matches("[0-9a-f]{40}") || !candidateSha.matches("[0-9a-f]{40}")) {
				throw new IllegalArgumentException("Full baseline/candidate SHA-1 values are required");
			}
			if (!baselineProductionSha256.matches("[0-9a-f]{64}")
					|| !candidateProductionSha256.matches("[0-9a-f]{64}")) {
				throw new IllegalArgumentException("Production directory SHA-256 values are required");
			}
			if (fixedPairs < PairedPerformanceContractV2.REQUIRED_PAIRS) {
				throw new IllegalArgumentException("At least ten fixed pairs are required");
			}
			if (hardwareDescription.isBlank() || hardwareDescription.contains("\n")
					|| hardwareDescription.contains("\r") || hardwareDescription.contains("=")) {
				throw new IllegalArgumentException("Hardware description must be a non-empty property-safe line");
			}
			if (warmupOperations <= 0 || !alternateStoragePressure) {
				throw new IllegalArgumentException("Positive warmup and alternating storage pressure are required");
			}
			new SchedulerHighContentionBenchmark.Config(operations, submitters, readWorkers, writeWorkers,
					analyticalLimit, foregroundCapacity, batchCapacity, workTokens, cooperativeYields,
					cooperativeParks, expiredDeadlinePercent, cancellationPercent, failurePercent,
					cooperativePercent, alternateStoragePressure, seed, timeout).validate();
			new SchedulerHighContentionBenchmark.Config(warmupOperations, Math.min(submitters, 32),
					readWorkers, writeWorkers, analyticalLimit, Math.min(foregroundCapacity, 8_192),
					Math.min(batchCapacity, 8_192), workTokens, cooperativeYields, cooperativeParks,
					expiredDeadlinePercent, cancellationPercent, failurePercent, cooperativePercent,
					alternateStoragePressure, seed ^ 0xD1B54A32D192ED03L, timeout).validate();
		}

		static Prepared fromArguments(Map<String, String> arguments) throws IOException {
			Set<String> allowed = Set.of("mode", "root", "baseline-sha", "candidate-sha",
					"baseline-worktree", "candidate-worktree", "baseline-classes", "candidate-classes",
					"hardware-description", "enforce", "pairs", "operations", "warmup-operations",
					"submitters", "read-workers", "write-workers", "analytical-limit",
					"foreground-capacity", "batch-capacity", "work-tokens", "cooperative-yields",
					"cooperative-parks", "expired-deadline-percent", "cancellation-percent",
					"failure-percent", "cooperative-percent", "alternate-storage-pressure",
					"seed", "timeout-seconds");
			rejectUnknown(arguments, allowed);
			Path baselineClasses = Path.of(require(arguments, "baseline-classes")).toAbsolutePath().normalize();
			Path candidateClasses = Path.of(require(arguments, "candidate-classes")).toAbsolutePath().normalize();
			return new Prepared(Path.of(require(arguments, "root")),
					requireSha(arguments, "baseline-sha"), requireSha(arguments, "candidate-sha"),
					Path.of(require(arguments, "baseline-worktree")),
					Path.of(require(arguments, "candidate-worktree")),
					baselineClasses, candidateClasses, contentSha(baselineClasses), contentSha(candidateClasses),
					arguments.getOrDefault("hardware-description", "unspecified"),
					argumentBoolean(arguments, "enforce", true),
					Integer.parseInt(arguments.getOrDefault("pairs", Integer.toString(DEFAULT_PAIRS))),
					Integer.parseInt(arguments.getOrDefault("operations", "1000000")),
					Integer.parseInt(arguments.getOrDefault("warmup-operations", "100000")),
					Integer.parseInt(arguments.getOrDefault("submitters", "64")),
					Integer.parseInt(arguments.getOrDefault("read-workers", "8")),
					Integer.parseInt(arguments.getOrDefault("write-workers", "8")),
					Integer.parseInt(arguments.getOrDefault("analytical-limit", "2")),
					Integer.parseInt(arguments.getOrDefault("foreground-capacity", "65536")),
					Integer.parseInt(arguments.getOrDefault("batch-capacity", "65536")),
					Integer.parseInt(arguments.getOrDefault("work-tokens", "256")),
					Integer.parseInt(arguments.getOrDefault("cooperative-yields", "4")),
					Integer.parseInt(arguments.getOrDefault("cooperative-parks", "2")),
					Integer.parseInt(arguments.getOrDefault("expired-deadline-percent", "5")),
					Integer.parseInt(arguments.getOrDefault("cancellation-percent", "10")),
					Integer.parseInt(arguments.getOrDefault("failure-percent", "5")),
					Integer.parseInt(arguments.getOrDefault("cooperative-percent", "30")),
					argumentBoolean(arguments, "alternate-storage-pressure", true),
					Long.parseLong(arguments.getOrDefault("seed", "104372305701837")),
					Duration.ofSeconds(Long.parseLong(arguments.getOrDefault("timeout-seconds", "180"))));
		}

		static Prepared read(Path root) throws IOException {
			root = root.toAbsolutePath().normalize();
			Map<String, String> values = exactProperties(root.resolve(METADATA_FILE), METADATA_KEYS);
			if (!require(values, "schema").equals(METADATA_SCHEMA)) {
				throw new IllegalArgumentException("Unknown deadline metadata schema");
			}
			Prepared prepared = new Prepared(root,
					require(values, "baseline-sha"), require(values, "candidate-sha"),
					Path.of(require(values, "baseline-worktree")), Path.of(require(values, "candidate-worktree")),
					Path.of(require(values, "baseline-classes")), Path.of(require(values, "candidate-classes")),
					require(values, "baseline-production-sha256"), require(values, "candidate-production-sha256"),
					require(values, "hardware-description"), booleanValue(values, "enforce"),
					Math.toIntExact(longValue(values, "fixed-pairs")),
					Math.toIntExact(longValue(values, "scheduler-operations")),
					Math.toIntExact(longValue(values, "scheduler-warmup-operations")),
					Math.toIntExact(longValue(values, "scheduler-submitters")),
					Math.toIntExact(longValue(values, "scheduler-read-workers")),
					Math.toIntExact(longValue(values, "scheduler-write-workers")),
					Math.toIntExact(longValue(values, "scheduler-analytical-limit")),
					Math.toIntExact(longValue(values, "scheduler-foreground-capacity")),
					Math.toIntExact(longValue(values, "scheduler-batch-capacity")),
					Math.toIntExact(longValue(values, "scheduler-work-tokens")),
					Math.toIntExact(longValue(values, "scheduler-cooperative-yields")),
					Math.toIntExact(longValue(values, "scheduler-cooperative-parks")),
					Math.toIntExact(longValue(values, "scheduler-expired-deadline-percent")),
					Math.toIntExact(longValue(values, "scheduler-cancellation-percent")),
					Math.toIntExact(longValue(values, "scheduler-failure-percent")),
					Math.toIntExact(longValue(values, "scheduler-cooperative-percent")),
					booleanValue(values, "scheduler-alternate-storage-pressure"),
					longValue(values, "scheduler-seed"), Duration.ofNanos(longValue(values, "scheduler-timeout-nanos")));
			if (!booleanValue(values, "adaptive-stopping")
					&& require(values, "multiplicity").equals("holm-bonferroni")
					&& require(values, "metric-set-sha256").equals(metricSetSha256())
					&& require(values, "family-wise-alpha").equals(Double.toString(PairedPerformanceContractV2.FAMILY_WISE_ALPHA))
					&& require(values, "throughput-minimum-ratio").equals(Double.toString(PairedPerformanceContractV2.THROUGHPUT_MINIMUM_RATIO))
					&& require(values, "cost-maximum-ratio").equals(Double.toString(PairedPerformanceContractV2.COST_MAXIMUM_RATIO))) {
				// exact immutable contract
			} else throw new IllegalArgumentException("Statistical contract metadata mismatch");
			if (!prepared.configurationSha256().equals(require(values, "configuration-sha256"))) {
				throw new IllegalArgumentException("Metadata configuration hash mismatch");
			}
			return prepared;
		}

		SchedulerHighContentionBenchmark.Config measuredConfig() {
			return new SchedulerHighContentionBenchmark.Config(operations, submitters, readWorkers, writeWorkers,
					analyticalLimit, foregroundCapacity, batchCapacity, workTokens, cooperativeYields,
					cooperativeParks, expiredDeadlinePercent, cancellationPercent, failurePercent,
					cooperativePercent, alternateStoragePressure, seed, timeout);
		}

		SchedulerHighContentionBenchmark.Config warmupConfig() {
			return new SchedulerHighContentionBenchmark.Config(warmupOperations, Math.min(submitters, 32),
					readWorkers, writeWorkers, analyticalLimit, Math.min(foregroundCapacity, 8_192),
					Math.min(batchCapacity, 8_192), workTokens, cooperativeYields, cooperativeParks,
					expiredDeadlinePercent, cancellationPercent, failurePercent, cooperativePercent,
					alternateStoragePressure, seed ^ 0xD1B54A32D192ED03L, timeout);
		}

		String metadataText() {
			return "schema=" + METADATA_SCHEMA + '\n' + "configuration-sha256=" + configurationSha256() + '\n'
					+ "metric-set-sha256=" + metricSetSha256() + '\n' + configurationText();
		}

		String configurationSha256() {
			return AdversarialBatchLivenessPairedBenchmark.sha256(configurationText());
		}

		private String configurationText() {
			return "baseline-sha=" + baselineSha + '\n' + "candidate-sha=" + candidateSha + '\n'
					+ "baseline-worktree=" + baselineWorktree + '\n' + "candidate-worktree=" + candidateWorktree + '\n'
					+ "baseline-classes=" + baselineClasses + '\n' + "candidate-classes=" + candidateClasses + '\n'
					+ "baseline-production-sha256=" + baselineProductionSha256 + '\n'
					+ "candidate-production-sha256=" + candidateProductionSha256 + '\n'
					+ "hardware-description=" + hardwareDescription + '\n' + "enforce=" + enforce + '\n'
					+ "fixed-pairs=" + fixedPairs + '\n' + "adaptive-stopping=false\n"
					+ "family-wise-alpha=" + PairedPerformanceContractV2.FAMILY_WISE_ALPHA + '\n'
					+ "throughput-minimum-ratio=" + PairedPerformanceContractV2.THROUGHPUT_MINIMUM_RATIO + '\n'
					+ "cost-maximum-ratio=" + PairedPerformanceContractV2.COST_MAXIMUM_RATIO + '\n'
					+ "multiplicity=holm-bonferroni\n" + "scheduler-operations=" + operations + '\n'
					+ "scheduler-warmup-operations=" + warmupOperations + '\n' + "scheduler-submitters=" + submitters + '\n'
					+ "scheduler-read-workers=" + readWorkers + '\n' + "scheduler-write-workers=" + writeWorkers + '\n'
					+ "scheduler-analytical-limit=" + analyticalLimit + '\n'
					+ "scheduler-foreground-capacity=" + foregroundCapacity + '\n'
					+ "scheduler-batch-capacity=" + batchCapacity + '\n' + "scheduler-work-tokens=" + workTokens + '\n'
					+ "scheduler-cooperative-yields=" + cooperativeYields + '\n'
					+ "scheduler-cooperative-parks=" + cooperativeParks + '\n'
					+ "scheduler-expired-deadline-percent=" + expiredDeadlinePercent + '\n'
					+ "scheduler-cancellation-percent=" + cancellationPercent + '\n'
					+ "scheduler-failure-percent=" + failurePercent + '\n'
					+ "scheduler-cooperative-percent=" + cooperativePercent + '\n'
					+ "scheduler-alternate-storage-pressure=" + alternateStoragePressure + '\n'
					+ "scheduler-seed=" + seed + '\n' + "scheduler-timeout-nanos=" + timeout.toNanos() + '\n';
		}
	}

	record Artifact(Map<String, String> values) {

		Artifact { values = Map.copyOf(values); }

		static Artifact fromResult(Prepared prepared, ScheduledRun run, String buildSha,
				String productionSha, long started, long finished,
				SchedulerHighContentionBenchmark.Result result) throws Exception {
			var values = new LinkedHashMap<String, String>();
			put(values, "artifact-schema", WORKER_SCHEMA);
			put(values, "round", run.round());
			put(values, "ordinal", run.ordinal());
			put(values, "implementation", run.implementation().value);
			put(values, "build-sha", buildSha);
			put(values, "configuration-sha256", prepared.configurationSha256());
			put(values, "metric-set-sha256", metricSetSha256());
			put(values, "host-sha256", AdversarialBatchLivenessPairedBenchmark.hostSha(prepared.hardwareDescription()));
			put(values, "hardware-description", prepared.hardwareDescription());
			put(values, "runtime-sha256", AdversarialBatchLivenessPairedBenchmark.runtimeSha());
			put(values, "harness-sha256", harnessSha());
			put(values, "classpath-sha256", GrpcRetainedReadBenchmark.classPathContentSha256ForTesting(
					System.getProperty("java.class.path")));
			put(values, "production-sha256", productionSha);
			put(values, "process-id", ProcessHandle.current().pid());
			put(values, "started-epoch-millis", started);
			put(values, "finished-epoch-millis", finished);
			put(values, "gate-correctness", true);
			put(values, "gate-terminal-conservation", terminalConserved(result));
			put(values, "gate-progress", progressProven(result));
			put(values, "gate-drain", drainProven(result));
			put(values, "gate-pressure", pressureProven(result));
			put(values, "gate-bounds", boundsProven(result));
			Map<String, String> report = parseReport(result.toReport());
			for (var entry : report.entrySet()) put(values, entry.getKey(), entry.getValue());
			if (!values.keySet().equals(WORKER_KEYS)) throw new IllegalStateException("Worker report key drift");
			return new Artifact(values);
		}

		static Artifact read(Path path) throws IOException {
			Map<String, String> values = exactProperties(path, WORKER_KEYS);
			if (!require(values, "artifact-schema").equals(WORKER_SCHEMA)) {
				throw new IllegalArgumentException("Unknown deadline worker schema");
			}
			return new Artifact(values);
		}

		void write(Path path) throws IOException {
			Files.createDirectories(path.getParent());
			var text = new StringBuilder();
			for (var entry : values.entrySet()) text.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
			Files.writeString(path, text, StandardOpenOption.CREATE_NEW);
		}

		String value(String key) { return require(values, key); }
		int integer(String key) { return Math.toIntExact(longValue(key)); }
		long longValue(String key) { return FiniteDeadlineContentionBenchmark.longValue(values, key); }
		long nonNegative(String key) {
			long value = longValue(key);
			if (value < 0L) throw new IllegalArgumentException(key + " must be non-negative");
			return value;
		}
		boolean bool(String key) { return booleanValue(values, key); }
		Implementation implementation() { return Implementation.parse(value("implementation")); }
		double metric(Metric metric) {
			try {
				double value = metric.property() == null
						? Double.parseDouble(value("profile.latency.runs")) * 1_000_000_000.0d
						/ Double.parseDouble(value("elapsed_nanos"))
						: Double.parseDouble(value(metric.property()));
				if (!Double.isFinite(value) || value <= 0.0d) throw new NumberFormatException();
				return value;
			} catch (NumberFormatException invalid) {
				throw new IllegalArgumentException("Metric " + metric.name() + " must be finite and positive", invalid);
			}
		}

		Artifact with(String key, String value) {
			var changed = new LinkedHashMap<>(values);
			if (changed.replace(key, value) == null) throw new IllegalArgumentException("Unknown key " + key);
			return new Artifact(changed);
		}

		private static Map<String, String> parseReport(String report) {
			var values = new LinkedHashMap<String, String>();
			for (String line : report.split("\\R")) {
				int separator = line.indexOf('=');
				if (separator <= 0 || separator != line.lastIndexOf('=')) {
					throw new IllegalArgumentException("Malformed scheduler report");
				}
				put(values, line.substring(0, separator), line.substring(separator + 1));
			}
			return values;
		}

		private static boolean terminalConserved(SchedulerHighContentionBenchmark.Result result) {
			return result.attempts() == result.config().operations()
					&& result.outcomes().values().stream().mapToLong(Long::longValue).sum() == result.attempts()
					&& result.outcomes().get(RWScheduler.TerminalOutcome.RUN) == result.runs()
					&& result.outcomes().get(RWScheduler.TerminalOutcome.SHUTDOWN) == 0L;
		}

		private static boolean progressProven(SchedulerHighContentionBenchmark.Result result) {
			return result.profiles().values().stream().allMatch(item -> item.attempts() > 0L && item.runs() > 0L)
					&& result.families().values().stream().allMatch(item -> item.attempts() > 0L && item.runs() > 0L)
					&& result.yieldTransitions() > 0L && result.parkTransitions() > 0L;
		}

		private static boolean drainProven(SchedulerHighContentionBenchmark.Result result) {
			return result.pools().values().stream().allMatch(item ->
					item.finalSnapshot().drainedAndConserved()
							&& item.finalSnapshot().startedTasks() == item.finalSnapshot().completedTasks());
		}

		private static boolean pressureProven(SchedulerHighContentionBenchmark.Result result) {
			var value = result.pressureProgress();
			return result.pressureTransitions() > 0L
					&& value.batchReadPressured() + value.batchReadUnpressured() > 0L
					&& value.batchWritePressured() + value.batchWriteUnpressured() > 0L
					&& value.foregroundReadPressured() > 0L && value.foregroundWritePressured() > 0L;
		}

		private static boolean boundsProven(SchedulerHighContentionBenchmark.Result result) {
			return result.pools().values().stream().allMatch(item -> item.peakActive() <= item.workers()
					&& item.peakQueued() <= item.outstandingBound()
					&& item.peakOutstanding() <= item.outstandingBound());
		}
	}

	private static void put(Map<String, String> values, String key, Object value) {
		String text = String.valueOf(value);
		if (key.isEmpty() || text.isEmpty() || key.contains("=") || text.contains("=")
				|| text.contains("\n") || text.contains("\r") || values.putIfAbsent(key, text) != null) {
			throw new IllegalArgumentException("Invalid or duplicate property " + key);
		}
	}
}
