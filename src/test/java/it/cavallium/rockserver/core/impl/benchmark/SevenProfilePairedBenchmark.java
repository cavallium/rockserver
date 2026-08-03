package it.cavallium.rockserver.core.impl.benchmark;

import it.cavallium.rockserver.core.common.WorkloadProfile;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Counterbalanced Pareto gate for ten baseline/candidate seven-profile subprocess pairs.
 *
 * <p>{@code prepare} writes the immutable schedule before any measurement. The operator then runs
 * {@link SevenProfileWorkloadBenchmark} in the listed order, one subprocess at a time, placing each
 * resulting {@code pareto-worker.properties} under the scheduled directory. {@code evaluate}
 * strictly parses those artifacts, validates provenance and conservation, and writes JSON/Markdown
 * reports. Keeping measurement execution external lets the dedicated host enforce cache control and
 * ensures no build, validator, or other benchmark overlaps a measured process.</p>
 */
public final class SevenProfilePairedBenchmark {

	private static final String RESULT_SCHEMA = "rockserver-seven-profile-paired-comparison-v1";
	private static final String SCHEDULE_SCHEMA = "rockserver-seven-profile-paired-schedule-v1";
	private static final String PERFORMANCE_BASELINE_SHA =
			"bb4f1a7e90db1fdfd785936594d080e8c4a0ba4e";
	private static final String WORKER_FILE = "pareto-worker.properties";

	private SevenProfilePairedBenchmark() {
	}

	public static void main(String[] args) throws Exception {
		if (Arrays.asList(args).contains("--help")) {
			printUsage();
			return;
		}
		Options options = Options.parse(args);
		if (options.mode() == Mode.PREPARE) {
			prepare(options);
		} else {
			evaluate(options);
		}
	}

	private static void prepare(Options options) throws IOException {
		if (Files.exists(options.root())) {
			throw new IllegalArgumentException("Paired benchmark root already exists: " + options.root());
		}
		Files.createDirectories(options.root());
		Files.writeString(options.root().resolve("schedule.tsv"), scheduleText(options),
				StandardOpenOption.CREATE_NEW);
		Files.writeString(options.root().resolve("metadata.properties"), metadataText(options),
				StandardOpenOption.CREATE_NEW);
		System.out.println("Prepared immutable seven-profile schedule: "
				+ options.root().resolve("schedule.tsv").toAbsolutePath());
	}

	private static void evaluate(Options options) throws IOException {
		if (!Files.isDirectory(options.root())) {
			throw new IllegalArgumentException("Prepared paired benchmark root does not exist: " + options.root());
		}
		String expectedSchedule = scheduleText(options);
		String actualSchedule = Files.readString(options.root().resolve("schedule.tsv"));
		if (!actualSchedule.equals(expectedSchedule)) {
			throw new IllegalArgumentException("Seven-profile schedule or provenance changed after preparation");
		}
		List<WorkerArtifact> baseline = new ArrayList<>(PairedPerformanceContract.REQUIRED_PAIRS);
		List<WorkerArtifact> candidate = new ArrayList<>(PairedPerformanceContract.REQUIRED_PAIRS);
		List<String> structuralFailures = new ArrayList<>();
		String datasetFingerprint = null;
		String configurationFingerprint = null;
		Integer schedulerCandidate = null;
		String runtimeFingerprint = null;
		String hardwareDescription = null;
		String baselineClassPathSha256 = null;
		String candidateClassPathSha256 = null;
		String baselineProductionSha256 = null;
		String candidateProductionSha256 = null;
		boolean workersEnforced = true;
		Set<Long> processIds = new LinkedHashSet<>();
		long previousScheduledFinished = Long.MIN_VALUE;
		for (int round = 1; round <= PairedPerformanceContract.REQUIRED_PAIRS; round++) {
			WorkerArtifact base = readWorker(options, round, Implementation.BASELINE);
			WorkerArtifact next = readWorker(options, round, Implementation.CANDIDATE);
			baseline.add(base);
			candidate.add(next);
			workersEnforced &= base.enforcedHardwareRun() && next.enforcedHardwareRun();
			for (String failure : base.structuralFailures()) {
				structuralFailures.add("baseline round " + round + ": " + failure);
			}
			for (String failure : next.structuralFailures()) {
				structuralFailures.add("candidate round " + round + ": " + failure);
			}
			if (datasetFingerprint == null) {
				datasetFingerprint = base.datasetFingerprint();
				configurationFingerprint = base.configurationFingerprint();
				schedulerCandidate = base.schedulerCandidate();
			}
			if (!datasetFingerprint.equals(base.datasetFingerprint())
					|| !datasetFingerprint.equals(next.datasetFingerprint())) {
				structuralFailures.add("dataset fingerprint changed in round " + round);
			}
			if (!configurationFingerprint.equals(base.configurationFingerprint())
					|| !configurationFingerprint.equals(next.configurationFingerprint())) {
				structuralFailures.add("configuration fingerprint changed in round " + round);
			}
			if (schedulerCandidate != base.schedulerCandidate()
					|| schedulerCandidate != next.schedulerCandidate()) {
				structuralFailures.add("scheduler candidate changed in round " + round);
			}
			if (runtimeFingerprint == null) runtimeFingerprint = base.runtimeFingerprint();
			if (!runtimeFingerprint.equals(base.runtimeFingerprint())
					|| !runtimeFingerprint.equals(next.runtimeFingerprint())) {
				structuralFailures.add("JDK/native runtime provenance changed in round " + round);
			}
			if (hardwareDescription == null) hardwareDescription = base.hardwareDescription();
			if (!hardwareDescription.equals(base.hardwareDescription())
					|| !hardwareDescription.equals(next.hardwareDescription())) {
				structuralFailures.add("measurement hardware changed in round " + round);
			}
			if (baselineClassPathSha256 == null) {
				baselineClassPathSha256 = base.classPathSha256();
				candidateClassPathSha256 = next.classPathSha256();
				baselineProductionSha256 = base.productionClassesSha256();
				candidateProductionSha256 = next.productionClassesSha256();
			}
			if (!baselineClassPathSha256.equals(base.classPathSha256())
					|| !baselineProductionSha256.equals(base.productionClassesSha256())) {
				structuralFailures.add("baseline bytecode provenance changed in round " + round);
			}
			if (!candidateClassPathSha256.equals(next.classPathSha256())
					|| !candidateProductionSha256.equals(next.productionClassesSha256())) {
				structuralFailures.add("candidate bytecode provenance changed in round " + round);
			}
			if (!processIds.add(base.processId()) || !processIds.add(next.processId())) {
				structuralFailures.add("fresh subprocess identity was reused in round " + round);
			}
			WorkerArtifact first = (round & 1) == 1 ? base : next;
			WorkerArtifact second = (round & 1) == 1 ? next : base;
			if (first.startedEpochMillis() < previousScheduledFinished) {
				structuralFailures.add("scheduled subprocess order overlapped or reversed before round " + round);
			}
			if (first.finishedEpochMillis() > second.startedEpochMillis()) {
				structuralFailures.add("scheduled subprocess order overlapped or reversed in round " + round);
			}
			previousScheduledFinished = second.finishedEpochMillis();
		}
		Map<String, PairedPerformanceContract.MetricSamples> samples = samples(baseline, candidate);
		PairedPerformanceContract.Evaluation evaluation = evaluateForTesting(samples, structuralFailures);
		Instant finished = Instant.now();
		Files.writeString(options.root().resolve("results.json"), toJson(options, finished, samples, evaluation,
				datasetFingerprint, configurationFingerprint, schedulerCandidate, runtimeFingerprint,
				hardwareDescription, baselineClassPathSha256, candidateClassPathSha256,
				baselineProductionSha256, candidateProductionSha256, workersEnforced), StandardOpenOption.CREATE_NEW);
		Files.writeString(options.root().resolve("results.md"), toMarkdown(options, finished, samples, evaluation,
				datasetFingerprint, configurationFingerprint, schedulerCandidate, runtimeFingerprint,
				hardwareDescription, baselineClassPathSha256, candidateClassPathSha256,
				baselineProductionSha256, candidateProductionSha256, workersEnforced), StandardOpenOption.CREATE_NEW);
		if (options.enforce() && !evaluation.automaticAcceptancePassed()) {
			throw new IllegalStateException("Seven-profile Pareto comparison failed: "
					+ String.join("; ", evaluation.failures()));
		}
	}

	private static WorkerArtifact readWorker(Options options,
	                                         int round,
	                                         Implementation implementation) throws IOException {
		Path input = artifactPath(options.root(), round, implementation);
		if (!Files.isRegularFile(input)) {
			throw new IllegalArgumentException("Missing scheduled worker artifact: " + input);
		}
		Map<String, String> values = strictProperties(Files.readString(input), expectedWorkerKeys());
		require(values, "schema", SevenProfileWorkloadBenchmark.PARETO_WORKER_SCHEMA);
		String expectedBuild = implementation == Implementation.BASELINE
				? options.baselineSha() : options.candidateSha();
		require(values, "build-sha", expectedBuild);
		require(values, "storage-label", options.storageLabel());
		require(values, "cache-state", options.cacheState());
		for (String key : List.of("dataset-fingerprint", "configuration-fingerprint")) {
			if (!values.get(key).matches("[0-9a-f]{64}")) {
				throw new IllegalArgumentException("Invalid worker fingerprint " + key);
			}
		}
		Map<String, Double> metrics = new LinkedHashMap<>();
		for (PairedPerformanceContract.MetricSpec specification : metricSpecifications()) {
			double value = decimal(values, specification.name());
			if (specification.direction() == PairedPerformanceContract.Direction.NO_INCREASE) {
				if (value < 0.0d) throw new IllegalArgumentException("Negative metric " + specification.name());
			} else if (value <= 0.0d) {
				throw new IllegalArgumentException("Non-positive metric " + specification.name());
			}
			metrics.put(specification.name(), value);
		}
		List<String> failures = new ArrayList<>();
		if (!bool(values, "workload-checks-passed")) failures.add("workload checks failed");
		if (!bool(values, "resources-drained")) failures.add("resources did not drain");
		if (number(values, "native-leaks") != 0L) failures.add("native leak count is nonzero");
		for (String key : List.of("final-queued", "final-active", "final-parked", "final-outstanding",
				"final-pending", "final-transactions", "final-iterators", "final-range-cursors",
				"final-retained-snapshots")) {
			if (number(values, key) != 0L) failures.add(key + " is not zero");
		}
		if (number(values, "submission-attempts") != number(values, "terminal-outcomes")) {
			failures.add("submission attempts do not equal terminal outcomes after drain");
		}
		boolean exactSchedulerAccounting = bool(values, "scheduler-accounting-exact");
		if (implementation == Implementation.CANDIDATE && !exactSchedulerAccounting) {
			failures.add("candidate did not expose exact scheduler accounting");
		}
		boolean enforcedHardwareRun = bool(values, "enforced-hardware-run");
		if (!enforcedHardwareRun) {
			failures.add("worker was not run with enforced hardware validation");
		}
		if (number(values, "duration-nanos") <= 0L) failures.add("measurement duration is not positive");
		long startedEpochMillis = number(values, "started-epoch-millis");
		long finishedEpochMillis = number(values, "finished-epoch-millis");
		if (startedEpochMillis <= 0L || finishedEpochMillis < startedEpochMillis) {
			failures.add("worker timestamps are invalid");
		}
		long processId = number(values, "process-id");
		if (processId <= 0L) failures.add("process identity is invalid");
		String runtimeFingerprint = values.get("java-runtime") + '\n' + values.get("java-home")
				+ '\n' + values.get("rocksdb-version") + '\n' + values.get("rocksdb-artifact-sha256");
		for (String key : List.of("production-classes-sha256", "classpath-sha256",
				"rocksdb-artifact-sha256")) {
			if (!values.get(key).matches("[0-9a-f]{64}")) {
				throw new IllegalArgumentException("Invalid worker fingerprint " + key);
			}
		}
		int schedulerCandidate = integer(values, "candidate");
		if (schedulerCandidate < 4 || (schedulerCandidate & (schedulerCandidate - 1)) != 0) {
			throw new IllegalArgumentException("Worker scheduler candidate is not a valid power of two");
		}
		return new WorkerArtifact(values.get("dataset-fingerprint"),
				values.get("configuration-fingerprint"), schedulerCandidate,
				startedEpochMillis, finishedEpochMillis, processId, runtimeFingerprint,
				values.get("hardware-description"), values.get("classpath-sha256"),
				values.get("production-classes-sha256"), enforcedHardwareRun,
				Map.copyOf(metrics), List.copyOf(failures));
	}

	private static Map<String, PairedPerformanceContract.MetricSamples> samples(
			List<WorkerArtifact> baseline,
			List<WorkerArtifact> candidate) {
		Map<String, PairedPerformanceContract.MetricSamples> result = new LinkedHashMap<>();
		for (PairedPerformanceContract.MetricSpec specification : metricSpecifications()) {
			double[] base = baseline.stream().mapToDouble(value -> value.metrics().get(specification.name())).toArray();
			double[] next = candidate.stream().mapToDouble(value -> value.metrics().get(specification.name())).toArray();
			result.put(specification.name(), new PairedPerformanceContract.MetricSamples(base, next));
		}
		return Map.copyOf(result);
	}

	public static PairedPerformanceContract.Evaluation evaluateForTesting(
			Map<String, PairedPerformanceContract.MetricSamples> samples,
			List<String> structuralFailures) {
		return PairedPerformanceContract.evaluate(metricSpecifications(), samples,
				structuralFailures, true);
	}

	static List<String> scheduledOrderForTesting(int round) {
		if (round < 1 || round > PairedPerformanceContract.REQUIRED_PAIRS) {
			throw new IllegalArgumentException("round is outside the fixed schedule");
		}
		return (round & 1) == 1 ? List.of("baseline", "candidate") : List.of("candidate", "baseline");
	}

	private static List<PairedPerformanceContract.MetricSpec> metricSpecifications() {
		List<PairedPerformanceContract.MetricSpec> metrics = new ArrayList<>(List.of(
				PairedPerformanceContract.MetricSpec.cost("cpu-nanos-per-operation", true),
				PairedPerformanceContract.MetricSpec.allocation("allocated-bytes-per-operation", true),
				PairedPerformanceContract.MetricSpec.noIncrease("gc-collections"),
				PairedPerformanceContract.MetricSpec.noIncrease("gc-millis"),
				PairedPerformanceContract.MetricSpec.cost("peak-live-heap-bytes", true),
				PairedPerformanceContract.MetricSpec.cost("peak-direct-memory-bytes", true),
				PairedPerformanceContract.MetricSpec.cost("peak-resident-set-bytes", true),
				PairedPerformanceContract.MetricSpec.noIncrease("peak-thread-count"),
				PairedPerformanceContract.MetricSpec.noIncrease("peak-native-handles"),
				PairedPerformanceContract.MetricSpec.noIncrease("maximum-retained-snapshots"),
				PairedPerformanceContract.MetricSpec.noIncrease("maximum-parked"),
				PairedPerformanceContract.MetricSpec.noIncrease("maximum-outstanding")));
		for (WorkloadProfile profile : WorkloadBenchmarkSelector.ALL_PROFILES) {
			String prefix = "profile." + profile.name().toLowerCase(Locale.ROOT) + '.';
			metrics.add(PairedPerformanceContract.MetricSpec.throughput(prefix + "throughput", true));
			metrics.add(PairedPerformanceContract.MetricSpec.cost(prefix + "queue-p99-nanos", false));
			metrics.add(PairedPerformanceContract.MetricSpec.cost(prefix + "execution-p99-nanos", false));
			metrics.add(PairedPerformanceContract.MetricSpec.cost(prefix + "end-to-end-p99-nanos", true));
			metrics.add(PairedPerformanceContract.MetricSpec.noIncrease(prefix + "maximum-queued"));
			metrics.add(PairedPerformanceContract.MetricSpec.noIncrease(prefix + "maximum-active"));
		}
		return List.copyOf(metrics);
	}

	private static Set<String> expectedWorkerKeys() {
		Set<String> keys = new LinkedHashSet<>(List.of(
				"schema", "build-sha", "started-epoch-millis", "finished-epoch-millis", "process-id",
				"java-runtime", "java-home", "rocksdb-version", "production-classes-sha256",
				"classpath-sha256", "rocksdb-artifact-sha256", "hardware-description",
				"dataset-fingerprint", "configuration-fingerprint",
				"storage-label", "cache-state", "enforced-hardware-run", "candidate", "duration-nanos",
				"workload-checks-passed", "resources-drained", "native-leaks",
				"cpu-nanos-per-operation", "allocated-bytes-per-operation", "gc-collections", "gc-millis",
				"peak-live-heap-bytes", "peak-direct-memory-bytes", "peak-resident-set-bytes",
				"peak-thread-count", "peak-native-handles", "maximum-retained-snapshots",
				"final-queued", "final-active", "final-parked", "final-outstanding",
				"submission-attempts", "terminal-outcomes", "scheduler-accounting-exact",
				"final-pending", "final-transactions", "final-iterators", "final-range-cursors",
				"final-retained-snapshots", "maximum-parked", "maximum-outstanding"));
		for (PairedPerformanceContract.MetricSpec specification : metricSpecifications()) {
			keys.add(specification.name());
		}
		return Set.copyOf(keys);
	}

	private static String scheduleText(Options options) {
		StringBuilder out = new StringBuilder("schema\t").append(SCHEDULE_SCHEMA).append('\n')
				.append("baseline-sha\t").append(options.baselineSha()).append('\n')
				.append("candidate-sha\t").append(options.candidateSha()).append('\n')
				.append("host-state\t").append(options.hostState()).append('\n')
				.append("storage-label\t").append(options.storageLabel()).append('\n')
				.append("cache-state\t").append(options.cacheState()).append('\n')
				.append("pairs\t").append(PairedPerformanceContract.REQUIRED_PAIRS).append('\n')
				.append("adaptive-stopping\tfalse\n")
				.append("ordinal\tround\timplementation\tartifact\n");
		int ordinal = 0;
		for (int round = 1; round <= PairedPerformanceContract.REQUIRED_PAIRS; round++) {
			for (String value : scheduledOrderForTesting(round)) {
				Implementation implementation = Implementation.parse(value);
				out.append(++ordinal).append('\t').append(round).append('\t').append(value).append('\t')
						.append(options.root().relativize(artifactPath(options.root(), round, implementation)))
						.append('\n');
			}
		}
		return out.toString();
	}

	private static String metadataText(Options options) {
		return "schema=" + RESULT_SCHEMA + '\n'
				+ "prepared=" + Instant.now() + '\n'
				+ "baseline-sha=" + options.baselineSha() + '\n'
				+ "candidate-sha=" + options.candidateSha() + '\n'
				+ "host-state=" + options.hostState() + '\n'
				+ "storage-label=" + options.storageLabel() + '\n'
				+ "cache-state=" + options.cacheState() + '\n'
				+ "pairs=" + PairedPerformanceContract.REQUIRED_PAIRS + '\n'
				+ "adaptive-stopping=false\n";
	}

	private static Path artifactPath(Path root, int round, Implementation implementation) {
		return root.resolve("round-%02d-%s".formatted(round, implementation.value)).resolve(WORKER_FILE);
	}

	private static String toMarkdown(Options options,
	                                 Instant finished,
	                                 Map<String, PairedPerformanceContract.MetricSamples> samples,
	                                 PairedPerformanceContract.Evaluation evaluation,
	                                 String datasetFingerprint,
	                                 String configurationFingerprint,
	                                 int schedulerCandidate,
	                                 String runtimeFingerprint,
	                                 String hardwareDescription,
	                                 String baselineClassPathSha256,
	                                 String candidateClassPathSha256,
	                                 String baselineProductionSha256,
	                                 String candidateProductionSha256,
	                                 boolean workersEnforced) {
		StringBuilder out = new StringBuilder("# Paired seven-profile Pareto comparison\n\n")
				.append("- Result: **").append(evaluation.automaticAcceptancePassed() ? "PASS" : "FAIL")
				.append("**\n- Baseline / candidate: `").append(options.baselineSha()).append("` / `")
				.append(options.candidateSha()).append("`\n- Finished: `").append(finished)
				.append("`\n- Dataset / configuration: `").append(datasetFingerprint).append("` / `")
				.append(configurationFingerprint).append("`\n- Scheduler candidate: `")
				.append(schedulerCandidate).append("`; host/storage/cache: `").append(options.hostState())
				.append("` / `").append(options.storageLabel()).append("` / `").append(options.cacheState())
				.append("`\n- Runtime: `").append(runtimeFingerprint.replace('\n', '|'))
				.append("`\n- Hardware: `").append(hardwareDescription)
				.append("`\n- Baseline classpath / production: `").append(baselineClassPathSha256)
				.append("` / `").append(baselineProductionSha256)
				.append("`\n- Candidate classpath / production: `").append(candidateClassPathSha256)
				.append("` / `").append(candidateProductionSha256)
				.append("`\n- Worker hardware enforcement: `").append(workersEnforced)
				.append("`\n- Fixed schedule: ten pairs, alternating order, no adaptive stopping\n\n")
				.append("|Metric|Baseline absolute|Candidate absolute|Ratio|95% CI|Gate|Material|\n")
				.append("|---|---:|---:|---:|---:|---|---|\n");
		for (PairedPerformanceContract.MetricSpec specification : metricSpecifications()) {
			var metric = evaluation.metrics().get(specification.name());
			var absolute = samples.get(specification.name());
			out.append('|').append(specification.name()).append('|')
					.append(format(geometricMean(absolute.baseline()))).append('|')
					.append(format(geometricMean(absolute.candidate()))).append('|')
					.append(metric.interval().available() ? format(metric.interval().mean()) : "n/a").append('|')
					.append(metric.interval().available() ? "[" + format(metric.interval().lower95()) + ", "
							+ format(metric.interval().upper95()) + "]" : "exact no-increase")
					.append('|').append(metric.automaticNonRegressionPassed() ? "PASS" : "FAIL")
					.append('|').append(metric.materialImprovement() ? "YES" : "NO").append("|\n");
		}
		out.append("\nMaterial improvements: `").append(String.join(", ", evaluation.materialImprovements()))
				.append("`.\n\nException candidates remain failures until commit ablation, profiling, a compensating ")
				.append("gain, and explicit user approval: `")
				.append(String.join(", ", evaluation.exceptionCandidates())).append("`.\n");
		if (!evaluation.failures().isEmpty()) {
			out.append("\n## Failures\n\n");
			for (String failure : evaluation.failures()) out.append("- ").append(failure).append('\n');
		}
		return out.toString();
	}

	private static String toJson(Options options,
	                             Instant finished,
	                             Map<String, PairedPerformanceContract.MetricSamples> samples,
	                             PairedPerformanceContract.Evaluation evaluation,
	                             String datasetFingerprint,
	                             String configurationFingerprint,
	                             int schedulerCandidate,
	                             String runtimeFingerprint,
	                             String hardwareDescription,
	                             String baselineClassPathSha256,
	                             String candidateClassPathSha256,
	                             String baselineProductionSha256,
	                             String candidateProductionSha256,
	                             boolean workersEnforced) {
		StringBuilder out = new StringBuilder("{\n  \"schema\": \"").append(RESULT_SCHEMA)
				.append("\",\n  \"finished\": \"").append(finished)
				.append("\",\n  \"baseline_sha\": \"").append(options.baselineSha())
				.append("\",\n  \"candidate_sha\": \"").append(options.candidateSha())
				.append("\",\n  \"dataset_fingerprint\": \"").append(datasetFingerprint)
				.append("\",\n  \"configuration_fingerprint\": \"").append(configurationFingerprint)
				.append("\",\n  \"scheduler_candidate\": ").append(schedulerCandidate)
				.append(",\n  \"host_state\": \"").append(json(options.hostState()))
				.append("\",\n  \"storage_label\": \"").append(json(options.storageLabel()))
				.append("\",\n  \"cache_state\": \"").append(json(options.cacheState()))
				.append("\",\n  \"runtime_fingerprint\": \"").append(json(runtimeFingerprint))
				.append("\",\n  \"hardware_description\": \"").append(json(hardwareDescription))
				.append("\",\n  \"baseline_classpath_sha256\": \"").append(baselineClassPathSha256)
				.append("\",\n  \"candidate_classpath_sha256\": \"").append(candidateClassPathSha256)
				.append("\",\n  \"baseline_production_sha256\": \"").append(baselineProductionSha256)
				.append("\",\n  \"candidate_production_sha256\": \"").append(candidateProductionSha256)
				.append("\",\n  \"workers_enforced\": ").append(workersEnforced)
				.append(",\n  \"fixed_pairs\": ").append(PairedPerformanceContract.REQUIRED_PAIRS)
				.append(",\n  \"adaptive_stopping\": false,\n  \"passed\": ")
				.append(evaluation.automaticAcceptancePassed()).append(",\n  \"metrics\": {");
		int metricIndex = 0;
		for (PairedPerformanceContract.MetricSpec specification : metricSpecifications()) {
			if (metricIndex++ > 0) out.append(',');
			var metric = evaluation.metrics().get(specification.name());
			var absolute = samples.get(specification.name());
			out.append("\n    \"").append(json(specification.name())).append("\": {")
					.append("\"baseline\": ").append(arrayJson(absolute.baseline()))
					.append(", \"candidate\": ").append(arrayJson(absolute.candidate()))
					.append(", \"ratio_mean\": ")
					.append(metric.interval().available() ? format(metric.interval().mean()) : "null")
					.append(", \"lower_95\": ")
					.append(metric.interval().available() ? format(metric.interval().lower95()) : "null")
					.append(", \"upper_95\": ")
					.append(metric.interval().available() ? format(metric.interval().upper95()) : "null")
					.append(", \"automatic_passed\": ").append(metric.automaticNonRegressionPassed())
					.append(", \"material_improvement\": ").append(metric.materialImprovement())
					.append(", \"exception_candidate\": ").append(metric.exceptionCandidate()).append('}');
		}
		out.append("\n  },\n  \"material_improvements\": ")
				.append(stringArrayJson(evaluation.materialImprovements()))
				.append(",\n  \"exception_candidates\": ")
				.append(stringArrayJson(evaluation.exceptionCandidates()))
				.append(",\n  \"failures\": ").append(stringArrayJson(evaluation.failures()))
				.append("\n}\n");
		return out.toString();
	}

	private static String arrayJson(double[] values) {
		StringBuilder out = new StringBuilder("[");
		for (int index = 0; index < values.length; index++) {
			if (index > 0) out.append(',');
			out.append(format(values[index]));
		}
		return out.append(']').toString();
	}

	private static String stringArrayJson(List<String> values) {
		StringBuilder out = new StringBuilder("[");
		for (int index = 0; index < values.size(); index++) {
			if (index > 0) out.append(',');
			out.append('"').append(json(values.get(index))).append('"');
		}
		return out.append(']').toString();
	}

	private static double geometricMean(double[] values) {
		double sum = 0.0d;
		boolean zero = false;
		for (double value : values) {
			if (!Double.isFinite(value) || value < 0.0d) {
				throw new IllegalArgumentException("Absolute samples must be finite and non-negative");
			}
			if (value == 0.0d) zero = true;
			else sum += Math.log(value);
		}
		return zero ? 0.0d : Math.exp(sum / values.length);
	}

	private static Map<String, String> strictProperties(String artifact, Set<String> expectedKeys) {
		Map<String, String> values = new LinkedHashMap<>();
		String[] lines = artifact.split("\\R", -1);
		for (int index = 0; index < lines.length; index++) {
			String line = lines[index];
			if (line.isEmpty() && index == lines.length - 1) continue;
			if (line.isBlank() || line.startsWith("#") || line.startsWith("!")) {
				throw new IllegalArgumentException("Blank lines and comments are forbidden at line " + (index + 1));
			}
			int separator = line.indexOf('=');
			if (separator <= 0 || separator != line.lastIndexOf('=')) {
				throw new IllegalArgumentException("Malformed worker property at line " + (index + 1));
			}
			String key = line.substring(0, separator);
			String value = line.substring(separator + 1);
			if (!expectedKeys.contains(key)) throw new IllegalArgumentException("Unknown worker property " + key);
			if (value.isEmpty()) throw new IllegalArgumentException("Empty worker property " + key);
			if (values.put(key, value) != null) throw new IllegalArgumentException("Duplicate worker property " + key);
		}
		Set<String> missing = new LinkedHashSet<>(expectedKeys);
		missing.removeAll(values.keySet());
		if (!missing.isEmpty()) throw new IllegalArgumentException("Missing worker properties " + missing);
		return Map.copyOf(values);
	}

	private static void require(Map<String, String> values, String key, String expected) {
		if (!expected.equals(values.get(key))) {
			throw new IllegalArgumentException("Worker provenance mismatch for " + key + ": expected="
					+ expected + " actual=" + values.get(key));
		}
	}

	private static long number(Map<String, String> values, String key) {
		try {
			return Long.parseLong(values.get(key));
		} catch (RuntimeException invalid) {
			throw new IllegalArgumentException("Invalid long worker property " + key, invalid);
		}
	}

	private static int integer(Map<String, String> values, String key) {
		try {
			return Integer.parseInt(values.get(key));
		} catch (RuntimeException invalid) {
			throw new IllegalArgumentException("Invalid integer worker property " + key, invalid);
		}
	}

	private static double decimal(Map<String, String> values, String key) {
		try {
			double value = Double.parseDouble(values.get(key));
			if (!Double.isFinite(value)) throw new NumberFormatException("non-finite");
			return value;
		} catch (RuntimeException invalid) {
			throw new IllegalArgumentException("Invalid decimal worker property " + key, invalid);
		}
	}

	private static boolean bool(Map<String, String> values, String key) {
		String value = values.get(key);
		if (!value.equals("true") && !value.equals("false")) {
			throw new IllegalArgumentException("Invalid boolean worker property " + key);
		}
		return Boolean.parseBoolean(value);
	}

	private static String format(double value) {
		return String.format(Locale.ROOT, "%.6f", value);
	}

	private static String json(String value) {
		return value.replace("\\", "\\\\").replace("\"", "\\\"")
				.replace("\n", "\\n").replace("\r", "\\r");
	}

	private static void printUsage() {
		System.out.println("""
				Seven-profile paired Pareto gate:

				  # Write the immutable ten-pair counterbalanced schedule.
				  java ... SevenProfilePairedBenchmark --mode=prepare --root=/mnt/run \\
				    --candidate-sha=<full-sha> --host-state=dedicated --storage-label=nvme \\
				    --cache-state=cold --enforce=true

				  # Run SevenProfileWorkloadBenchmark in schedule.tsv order, one fresh JVM at a time,
				  # and place each pareto-worker.properties at the scheduled path. Then evaluate:
				  java ... SevenProfilePairedBenchmark --mode=evaluate --root=/mnt/run \\
				    --candidate-sha=<full-sha> --host-state=dedicated --storage-label=nvme \\
				    --cache-state=cold --enforce=true
				""");
	}

	private enum Mode {
		PREPARE, EVALUATE;

		private static Mode parse(String value) {
			return switch (value) {
				case "prepare" -> PREPARE;
				case "evaluate" -> EVALUATE;
				default -> throw new IllegalArgumentException("mode must be prepare or evaluate");
			};
		}
	}

	private enum Implementation {
		BASELINE("baseline"), CANDIDATE("candidate");

		private final String value;

		Implementation(String value) {
			this.value = value;
		}

		private static Implementation parse(String value) {
			return switch (value) {
				case "baseline" -> BASELINE;
				case "candidate" -> CANDIDATE;
				default -> throw new IllegalArgumentException("Unknown implementation " + value);
			};
		}
	}

	private record WorkerArtifact(String datasetFingerprint,
	                              String configurationFingerprint,
	                              int schedulerCandidate,
	                              long startedEpochMillis,
	                              long finishedEpochMillis,
	                              long processId,
	                              String runtimeFingerprint,
	                              String hardwareDescription,
	                              String classPathSha256,
	                              String productionClassesSha256,
	                              boolean enforcedHardwareRun,
	                              Map<String, Double> metrics,
	                              List<String> structuralFailures) {
	}

	private record Options(Mode mode,
	                       Path root,
	                       String baselineSha,
	                       String candidateSha,
	                       String hostState,
	                       String storageLabel,
	                       String cacheState,
	                       boolean enforce) {

		private static final Set<String> KEYS = Set.of("mode", "root", "baseline-sha", "candidate-sha",
				"host-state", "storage-label", "cache-state", "enforce");

		private static Options parse(String[] args) {
			Map<String, String> values = new LinkedHashMap<>();
			for (String argument : args) {
				if (!argument.startsWith("--") || !argument.contains("=")) {
					throw new IllegalArgumentException("Options must use --name=value: " + argument);
				}
				int equals = argument.indexOf('=');
				String key = argument.substring(2, equals);
				if (!KEYS.contains(key)) throw new IllegalArgumentException("Unknown option --" + key);
				if (values.put(key, argument.substring(equals + 1)) != null) {
					throw new IllegalArgumentException("Duplicate option --" + key);
				}
			}
			String root = values.get("root");
			String candidate = values.get("candidate-sha");
			if (root == null || root.isBlank() || candidate == null) {
				throw new IllegalArgumentException("--root and --candidate-sha are required");
			}
			String enforceValue = values.getOrDefault("enforce", "true");
			if (!enforceValue.equals("true") && !enforceValue.equals("false")) {
				throw new IllegalArgumentException("--enforce must be true or false");
			}
			Options options = new Options(Mode.parse(values.getOrDefault("mode", "evaluate")),
					Path.of(root).toAbsolutePath().normalize(),
					values.getOrDefault("baseline-sha", PERFORMANCE_BASELINE_SHA), candidate,
					values.getOrDefault("host-state", "dedicated"),
					values.getOrDefault("storage-label", "ci-structural"),
					values.getOrDefault("cache-state", "unknown"), Boolean.parseBoolean(enforceValue));
			options.validate();
			return options;
		}

		private void validate() {
			if (!baselineSha.equals(PERFORMANCE_BASELINE_SHA)) {
				throw new IllegalArgumentException("baseline-sha must be immutable v1.3.11 "
						+ PERFORMANCE_BASELINE_SHA);
			}
			if (!candidateSha.matches("[0-9a-f]{40}")) {
				throw new IllegalArgumentException("candidate-sha must be a full lowercase Git SHA");
			}
			if (enforce && (!hostState.equals("dedicated") || storageLabel.equals("ci-structural")
					|| cacheState.equals("unknown"))) {
				throw new IllegalArgumentException("enforced evaluation requires dedicated host and explicit "
						+ "hardware/cache labels");
			}
		}
	}
}
