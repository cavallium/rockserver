package it.cavallium.rockserver.core.impl.benchmark;

import com.sun.management.OperatingSystemMXBean;
import com.sun.management.ThreadMXBean;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.RocksLeakDetector;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.asm.MemberSubstitution;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.jar.asm.ClassReader;
import net.bytebuddy.jar.asm.ClassVisitor;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.matcher.ElementMatchers;
import org.rocksdb.RocksDB;
import reactor.core.publisher.Flux;

/**
 * Opt-in paired whole-gRPC Pareto gate for retained range, count, fan-out and
 * explicit-iterator reads.
 *
 * <p>The controller writes a fixed ten-pair schedule before execution. Every scheduled scenario
 * runs in a fresh subprocess, performs a process-first cold probe, warms that operation, and only
 * then enters the steady-state window. Odd pairs run the baseline first and even pairs run the
 * candidate first. There is no adaptive stopping.</p>
 */
public final class GrpcRetainedReadBenchmark {

	private static final String RESULT_SCHEMA = "rockserver-grpc-retained-read-comparison-v2";
	private static final String WORKER_SCHEMA = "rockserver-grpc-retained-read-worker-v2";
	private static final String DATASET_SCHEMA = "rockserver-grpc-retained-read-dataset-v1";
	private static final String SCHEDULE_SCHEMA = "rockserver-grpc-retained-read-schedule-v1";
	private static final String COLUMN_NAME = "grpc-retained-read-benchmark";
	private static final long VALUE_MAGIC = 0x525441494e454431L;
	private static final String PERFORMANCE_BASELINE_SHA = "bb4f1a7e90db1fdfd785936594d080e8c4a0ba4e";
	private static final int FIXED_PAIRED_ROUNDS = 10;
	private static final long READ_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(10L);
	private static final Set<String> WORKER_KEYS = Set.of(
			"schema", "implementation", "scenario", "round", "build-sha", "classpath-sha256",
			"arena-instrumentation-sha256",
			"dataset-id", "cold-completion-nanos", "cold-first-item-nanos", "elapsed-nanos",
			"operations", "items", "logical-bytes", "checksum", "entries-per-second",
			"mib-per-second", "completion-p99-nanos", "first-item-p99-nanos",
			"foreground-p99-nanos", "foreground-operations", "cpu-nanos-per-item",
			"allocated-bytes-per-item", "gc-collections", "gc-millis", "peak-live-heap-bytes",
			"peak-direct-memory-bytes", "peak-resident-set-bytes", "peak-thread-count",
			"peak-native-handles", "scheduler-accepted", "scheduler-started",
			"scheduler-terminal", "scheduler-execution-terminal", "scheduler-quantums",
			"scheduler-queue-p99-nanos", "scheduler-execution-p99-nanos",
			"scheduler-failures", "scheduler-rejections", "scheduler-cancellations",
			"peak-queued", "peak-active", "peak-parked", "peak-outstanding", "peak-pending", "peak-iterators",
			"peak-range-cursors", "peak-retained-snapshots", "peak-retained-permits",
			"peak-retained-waiters", "peak-iterator-leases", "peak-exists-multi-requests",
			"peak-exists-multi-snapshots", "peak-exists-multi-read-options", "peak-exists-multi-arenas",
			"final-queued", "final-active", "final-parked", "final-outstanding",
			"submission-attempts", "terminal-outcomes", "scheduler-accounting-exact",
			"final-pending", "final-transactions", "final-iterators", "final-range-cursors",
			"final-retained-snapshots", "final-retained-permits", "final-retained-waiters",
			"final-iterator-leases", "final-exists-multi-requests", "final-exists-multi-snapshots",
			"final-exists-multi-read-options", "final-exists-multi-arenas",
			"native-leaks", "correctness", "resources-drained",
			"accounting-valid", "configured-retained-limit");

	private GrpcRetainedReadBenchmark() {
	}

	public static void main(String[] args) throws Exception {
		if (Arrays.asList(args).contains("--help")) {
			printUsage();
			return;
		}
		Options options = Options.parse(args);
		System.setProperty("rockserver.core.print-config", "false");
		System.setProperty("it.cavallium.rockserver.leakdetection", "true");
		if (options.worker()) {
			runWorker(options);
		} else {
			runController(options);
		}
	}

	private static void runController(Options options) throws Exception {
		Path root = options.root().toAbsolutePath().normalize();
		if (Files.exists(root)) {
			throw new IllegalArgumentException("Benchmark root already exists; refusing to reuse state: " + root);
		}
		Files.createDirectories(root);
		Path shared = root.resolve("shared-dataset");
		Files.createDirectories(shared);
		Path config = shared.resolve("rockserver.conf");
		Files.writeString(config, configText(options), StandardOpenOption.CREATE_NEW);
		String datasetId = datasetId(options, Files.readString(config));
		writeDatasetMetadata(shared.resolve("dataset.properties"), options, datasetId);

		String currentClassPath = normalizedClassPath(System.getProperty("java.class.path"));
		verifyBuildCheckout(options.baselineClasses(), options.buildBaseline(), options.buildStateBaseline());
		verifyBuildCheckout(options.candidateClasses(), options.buildCandidate(), options.buildStateCandidate());
		String baselineClassPath = replaceProductionClasses(currentClassPath,
				options.candidateClasses(), options.baselineClasses());
		String candidateClassPath = replaceProductionClasses(currentClassPath,
				options.candidateClasses(), options.candidateClasses());
		String baselineClassPathHash = classPathContentSha256(baselineClassPath);
		String candidateClassPathHash = classPathContentSha256(candidateClassPath);
		List<ScheduledRun> schedule = fixedSchedule();
		writeSchedule(root.resolve("schedule.tsv"), schedule, options,
				baselineClassPathHash, candidateClassPathHash, datasetId);
		FileStore store = Files.getFileStore(root);
		Instant started = Instant.now();
		writeControllerMetadata(root, options, store, started, baselineClassPath,
				candidateClassPath, baselineClassPathHash, candidateClassPathHash, datasetId);
		prepareDataset(shared.resolve("db"), config, options);

		List<WorkerResult> results = new ArrayList<>(schedule.size());
		for (ScheduledRun run : schedule) {
			String classPath = run.implementation() == Implementation.BASELINE
					? baselineClassPath : candidateClassPath;
			String classPathHash = run.implementation() == Implementation.BASELINE
					? baselineClassPathHash : candidateClassPathHash;
			String buildSha = run.implementation() == Implementation.BASELINE
					? options.buildBaseline() : options.buildCandidate();
			Path output = root.resolve("round-%02d-%s-%s.properties".formatted(
					run.round(), run.implementation().value, run.scenario().value));
			runChild(options, shared, output, run, classPath, classPathHash, datasetId);
			results.add(WorkerResult.read(output,
					new ExpectedProvenance(run.implementation(), run.scenario(), run.round(), buildSha,
							classPathHash, datasetId)));
		}

		Comparison comparison = compare(results);
		Instant finished = Instant.now();
		writeReports(root, options, store, started, finished, results, comparison,
				baselineClassPathHash, candidateClassPathHash, datasetId);
		System.out.println(toMarkdown(options, store, started, finished, results, comparison,
				baselineClassPathHash, candidateClassPathHash, datasetId));
		if (options.enforce() && !comparison.passed()) {
			throw new IllegalStateException("Retained-read comparison failed: " + comparison.failedSummary());
		}
	}

	private static List<ScheduledRun> fixedSchedule() {
		List<ScheduledRun> schedule = new ArrayList<>(
				FIXED_PAIRED_ROUNDS * Scenario.values().length * Implementation.values().length);
		for (int round = 1; round <= FIXED_PAIRED_ROUNDS; round++) {
			Implementation first = (round & 1) == 1 ? Implementation.BASELINE : Implementation.CANDIDATE;
			Implementation second = first == Implementation.BASELINE
					? Implementation.CANDIDATE : Implementation.BASELINE;
			for (Scenario scenario : Scenario.values()) {
				schedule.add(new ScheduledRun(round, scenario, first));
				schedule.add(new ScheduledRun(round, scenario, second));
			}
		}
		return List.copyOf(schedule);
	}

	private static void runChild(Options options,
	                             Path shared,
	                             Path output,
	                             ScheduledRun run,
	                             String classPath,
	                             String classPathHash,
	                             String datasetId) throws Exception {
		String byteBuddyAgent = Path.of(ByteBuddyAgent.class.getProtectionDomain()
				.getCodeSource().getLocation().toURI()).toString();
		List<String> command = new ArrayList<>(List.of(
				Path.of(System.getProperty("java.home"), "bin", "java").toString(),
				"--enable-native-access=ALL-UNNAMED",
				"-javaagent:" + byteBuddyAgent,
				"-Xms" + options.childHeap(),
				"-Xmx" + options.childHeap(),
				"-cp", classPath,
				GrpcRetainedReadBenchmark.class.getName(),
				"--worker=true",
				"--root=" + options.root(),
				"--dataset-root=" + shared,
				"--output=" + output,
				"--baseline-classes=" + options.baselineClasses(),
				"--candidate-classes=" + options.candidateClasses(),
				"--implementation=" + run.implementation().value,
				"--scenario=" + run.scenario().value,
				"--round=" + run.round(),
				"--build-baseline=" + options.buildBaseline(),
				"--build-candidate=" + options.buildCandidate(),
				"--build-state-baseline=" + options.buildStateBaseline(),
				"--build-state-candidate=" + options.buildStateCandidate(),
				"--storage-label=" + options.storageLabel(),
				"--host-state=" + options.hostState(),
				"--cache-state=" + options.cacheState(),
				"--preload-keys=" + options.preloadKeys(),
				"--flush-keys=" + options.flushKeys(),
				"--batch-entries=" + options.batchEntries(),
				"--value-bytes=" + options.valueBytes(),
				"--exists-items=" + options.existsItems(),
				"--iterator-skip=" + options.iteratorSkip(),
				"--iterator-take=" + options.iteratorTake(),
				"--read-parallelism=" + options.readParallelism(),
				"--write-parallelism=" + options.writeParallelism(),
				"--foreground-workers=" + options.foregroundWorkers(),
				"--warmup-operations=" + options.warmupOperations(),
				"--measure-seconds=" + options.measureSeconds(),
				"--sample-micros=" + options.sampleMicros(),
				"--max-latency-samples=" + options.maxLatencySamples(),
				"--configured-retained-limit=" + options.configuredRetainedLimit(),
				"--child-heap=" + options.childHeap(),
				"--expected-classpath-sha256=" + classPathHash,
				"--dataset-id=" + datasetId,
				"--enforce=false",
				"--smoke=" + options.smoke()));
		System.out.printf(Locale.ROOT, "Starting retained-read round %d %s %s%n",
				run.round(), run.scenario().value, run.implementation().value);
		Process process = new ProcessBuilder(command).inheritIO().start();
		if (!process.waitFor(45, TimeUnit.MINUTES)) {
			process.destroyForcibly();
			throw new IllegalStateException("Retained-read child timed out: " + run);
		}
		if (process.exitValue() != 0) {
			throw new IllegalStateException("Retained-read child failed with exit "
					+ process.exitValue() + ": " + run);
		}
	}

	private static void prepareDataset(Path database, Path config, Options options) throws Exception {
		long leaksBefore = RocksLeakDetector.detectedLeakCount();
		EmbeddedConnection connection = new EmbeddedConnection(database, COLUMN_NAME, config);
		try {
			RocksDBSyncAPI api = connection.getSyncApi(RequestContext.batch());
			long columnId = api.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			int loaded = 0;
			while (loaded < options.preloadKeys()) {
				int count = Math.min(options.batchEntries(), options.preloadKeys() - loaded);
				List<Keys> keys = new ArrayList<>(count);
				List<Buf> values = new ArrayList<>(count);
				for (int offset = 0; offset < count; offset++) {
					long value = loaded + offset;
					keys.add(key(value));
					values.add(value(value, options.valueBytes()));
				}
				api.putBatch(columnId, Flux.just(new KVBatch.KVBatchRef(keys, values)), PutBatchMode.WRITE_BATCH);
				loaded += count;
				if (loaded % options.flushKeys() == 0 || loaded == options.preloadKeys()) {
					api.flush();
					System.out.printf(Locale.ROOT, "Prepared %,d / %,d retained-read keys%n",
							loaded, options.preloadKeys());
				}
			}
			api.flush();
		} finally {
			connection.closeTesting();
		}
		long leaks = awaitNativeLeakDetection(leaksBefore);
		if (leaks != 0L) {
			throw new IllegalStateException("Dataset preparation leaked " + leaks + " native handles");
		}
	}

	private static String configText(Options options) {
		return """
				database: {
				  metrics: {
				    database-name: "%s"
				    influx: { enabled: false }
				    jmx: { enabled: false }
				  }
				  parallelism: {
				    read: %d
				    write: %d
				    workload: {
				      latency-queue-capacity: 4096
				      analytical-queue-capacity: 4096
				      ingest-queue-capacity: 4096
				      batch-queue-capacity: 4096
				      retained-analytical-snapshots: %d
				      range-quantum-max-items: 4096
				      range-quantum-max-bytes: 2MiB
				      range-quantum-max-duration: PT0.008S
				    }
				  }
				  global: {
				    ingest-behind: false
				    optimistic: false
				    disable-auto-compactions: true
				    disable-write-slowdown: true
				    maximum-open-files: -1
				    fallback-column-options: { write-buffer-size: "64MiB" }
				  }
				}
				""".formatted(COLUMN_NAME, options.readParallelism(), options.writeParallelism(),
				options.configuredRetainedLimit());
	}

	private static String datasetId(Options options, String config) {
		return sha256(DATASET_SCHEMA + '\n' + options.preloadKeys() + '\n' + options.flushKeys()
				+ '\n' + options.valueBytes() + '\n' + VALUE_MAGIC + '\n' + config);
	}

	private static void writeDatasetMetadata(Path output, Options options, String datasetId) throws IOException {
		Files.writeString(output, "schema=" + DATASET_SCHEMA + '\n'
						+ "dataset-id=" + datasetId + '\n'
						+ "preload-keys=" + options.preloadKeys() + '\n'
						+ "flush-keys=" + options.flushKeys() + '\n'
						+ "value-bytes=" + options.valueBytes() + '\n'
						+ "value-magic=" + Long.toUnsignedString(VALUE_MAGIC) + '\n',
				StandardOpenOption.CREATE_NEW);
	}

	private static Keys key(long value) {
		byte[] bytes = new byte[Long.BYTES];
		ByteBuffer.wrap(bytes).putLong(value);
		return new Keys(Buf.wrap(bytes));
	}

	private static Buf value(long key, int size) {
		byte[] bytes = new byte[size];
		for (int index = 0; index < bytes.length; index++) {
			bytes[index] = (byte) mix64(key + index);
		}
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		buffer.putLong(0, VALUE_MAGIC);
		buffer.putLong(Long.BYTES, key);
		buffer.putLong(size - Long.BYTES, VALUE_MAGIC ^ key);
		return Buf.wrap(bytes);
	}

	private static long mix64(long value) {
		value ^= value >>> 30;
		value *= 0xbf58476d1ce4e5b9L;
		value ^= value >>> 27;
		value *= 0x94d049bb133111ebL;
		return value ^ (value >>> 31);
	}

	private static String replaceProductionClasses(String classPath,
	                                               Path candidateClasses,
	                                               Path selectedClasses) {
		Path candidate = candidateClasses.toAbsolutePath().normalize();
		Path selected = selectedClasses.toAbsolutePath().normalize();
		List<String> entries = new ArrayList<>();
		boolean replaced = false;
		for (String entry : classPath.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
			Path normalized = Path.of(entry).toAbsolutePath().normalize();
			if (normalized.equals(candidate)) {
				entries.add(selected.toString());
				replaced = true;
			} else {
				entries.add(normalized.toString());
			}
		}
		if (!replaced) {
			throw new IllegalArgumentException("Current classpath does not contain candidate classes: " + candidate);
		}
		return String.join(java.io.File.pathSeparator, entries);
	}

	private static String normalizedClassPath(String classPath) {
		return Arrays.stream(classPath.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator)))
				.map(Path::of)
				.map(Path::toAbsolutePath)
				.map(Path::normalize)
				.map(Path::toString)
				.collect(Collectors.joining(java.io.File.pathSeparator));
	}

	private static String sha256(String value) {
		try {
			byte[] digest = MessageDigest.getInstance("SHA-256")
					.digest(value.getBytes(StandardCharsets.UTF_8));
			return java.util.HexFormat.of().formatHex(digest);
		} catch (java.security.NoSuchAlgorithmException impossible) {
			throw new IllegalStateException(impossible);
		}
	}

	private static String sha256(byte[]... values) {
		try {
			var digest = MessageDigest.getInstance("SHA-256");
			for (byte[] value : values) {
				updateDigest(digest, value.length);
				digest.update(value);
			}
			return java.util.HexFormat.of().formatHex(digest.digest());
		} catch (java.security.NoSuchAlgorithmException impossible) {
			throw new IllegalStateException(impossible);
		}
	}

	private static String classPathContentSha256(String classPath) throws IOException {
		final MessageDigest digest;
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (java.security.NoSuchAlgorithmException impossible) {
			throw new IllegalStateException(impossible);
		}
		for (String value : classPath.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
			Path entry = Path.of(value).toAbsolutePath().normalize();
			if (!Files.exists(entry)) {
				throw new IOException("Classpath entry does not exist: " + entry);
			}
			updateDigest(digest, "entry");
			updateDigest(digest, entry.toString());
			if (Files.isSymbolicLink(entry)) {
				throw new IOException("Symbolic classpath entries are not accepted: " + entry);
			}
			if (Files.isRegularFile(entry)) {
				updateDigest(digest, "file");
				hashFile(digest, entry, entry.getFileName().toString());
			} else if (Files.isDirectory(entry)) {
				updateDigest(digest, "directory");
				final List<Path> files;
				try (var paths = Files.walk(entry)) {
					files = paths.sorted(Comparator.comparing(path -> normalizedRelativePath(entry, path))).toList();
				}
				for (Path file : files) {
					if (file.equals(entry)) {
						continue;
					}
					if (Files.isSymbolicLink(file)) {
						throw new IOException("Symbolic classpath content is not accepted: " + file);
					}
					if (Files.isRegularFile(file)) {
						hashFile(digest, file, normalizedRelativePath(entry, file));
					}
				}
			} else {
				throw new IOException("Unsupported classpath entry: " + entry);
			}
		}
		return java.util.HexFormat.of().formatHex(digest.digest());
	}

	private static String normalizedRelativePath(Path root, Path path) {
		return root.relativize(path).toString().replace(java.io.File.separatorChar, '/');
	}

	private static void hashFile(MessageDigest digest, Path file, String relativePath) throws IOException {
		updateDigest(digest, relativePath);
		updateDigest(digest, Files.size(file));
		byte[] buffer = new byte[64 * 1024];
		try (InputStream input = Files.newInputStream(file)) {
			int read;
			while ((read = input.read(buffer)) >= 0) {
				if (read > 0) {
					digest.update(buffer, 0, read);
				}
			}
		}
	}

	private static void updateDigest(MessageDigest digest, String value) {
		byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
		updateDigest(digest, bytes.length);
		digest.update(bytes);
	}

	private static void updateDigest(MessageDigest digest, long value) {
		for (int shift = Long.SIZE - Byte.SIZE; shift >= 0; shift -= Byte.SIZE) {
			digest.update((byte) (value >>> shift));
		}
	}

	static String verifyBuildCheckout(Path classes,
	                                          String expectedSha,
	                                          String declaredState) throws Exception {
		Path location = classes.toAbsolutePath().normalize();
		if (!Files.isDirectory(location)) {
			throw new IllegalArgumentException("Classes directory does not exist: " + location);
		}
		String actualSha = gitOutput(location, "rev-parse", "--verify", "HEAD").trim();
		if (!actualSha.equals(expectedSha)) {
			throw new IllegalArgumentException("Classes checkout SHA mismatch for " + location
					+ ": expected=" + expectedSha + " actual=" + actualSha);
		}
		if (declaredState.equals("clean")) {
			String status = gitOutput(location, "status", "--porcelain=v1", "--untracked-files=normal");
			if (!status.isBlank()) {
				throw new IllegalArgumentException("Classes checkout was declared clean but is dirty: " + location);
			}
		}
		return actualSha;
	}

	private static String gitOutput(Path location, String... arguments) throws Exception {
		List<String> command = new ArrayList<>(arguments.length + 3);
		command.add("git");
		command.add("-C");
		command.add(location.toString());
		command.addAll(List.of(arguments));
		Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
		var outputTask = new FutureTask<byte[]>(() -> process.getInputStream().readAllBytes());
		Thread.ofVirtual().name("retained-provenance-output").start(outputTask);
		final byte[] output;
		try {
			if (!process.waitFor(30, TimeUnit.SECONDS)) {
				process.destroyForcibly();
				outputTask.cancel(true);
				throw new IllegalStateException("Git provenance command timed out for " + location);
			}
			output = outputTask.get(30, TimeUnit.SECONDS);
		} catch (InterruptedException interrupted) {
			process.destroyForcibly();
			outputTask.cancel(true);
			Thread.currentThread().interrupt();
			throw interrupted;
		} catch (java.util.concurrent.TimeoutException outputTimeout) {
			process.destroyForcibly();
			outputTask.cancel(true);
			throw new IllegalStateException("Git provenance output drain timed out for " + location,
					outputTimeout);
		}
		String text = new String(output, StandardCharsets.UTF_8);
		if (process.exitValue() != 0) {
			throw new IllegalArgumentException("Git provenance command failed for " + location + ": " + text.trim());
		}
		return text;
	}

	private enum Implementation {
		BASELINE("baseline"), CANDIDATE("candidate");

		private final String value;

		Implementation(String value) {
			this.value = value;
		}

		private static Implementation parse(String value) {
			return Arrays.stream(values()).filter(candidate -> candidate.value.equals(value)).findFirst()
					.orElseThrow(() -> new IllegalArgumentException("Unknown implementation: " + value));
		}
	}

	private enum Operation {
		EXACT_COUNT("exact-count", OperationFamily.FULL_SCAN_AGGREGATE),
		STREAM_RANGE("stream-range", OperationFamily.RANGE_PAGE),
		EXISTS_MULTI("exists-multi", OperationFamily.BOUNDED_FAN_OUT),
		ITERATOR("iterator-skip-take", OperationFamily.RANGE_PAGE);

		private final String value;
		private final OperationFamily family;

		Operation(String value, OperationFamily family) {
			this.value = value;
			this.family = family;
		}
	}

	private enum Scenario {
		EXACT_COUNT(Operation.EXACT_COUNT, false),
		EXACT_COUNT_MIXED(Operation.EXACT_COUNT, true),
		STREAM_RANGE(Operation.STREAM_RANGE, false),
		STREAM_RANGE_MIXED(Operation.STREAM_RANGE, true),
		EXISTS_MULTI(Operation.EXISTS_MULTI, false),
		EXISTS_MULTI_MIXED(Operation.EXISTS_MULTI, true),
		ITERATOR(Operation.ITERATOR, false),
		ITERATOR_MIXED(Operation.ITERATOR, true);

		private final Operation operation;
		private final boolean mixed;
		private final String value;

		Scenario(Operation operation, boolean mixed) {
			this.operation = operation;
			this.mixed = mixed;
			this.value = operation.value + (mixed ? "-with-latency-gets" : "-isolated");
		}

		private boolean hasFirstItem() {
			return operation == Operation.STREAM_RANGE;
		}

		private static Scenario parse(String value) {
			return Arrays.stream(values()).filter(candidate -> candidate.value.equals(value)).findFirst()
					.orElseThrow(() -> new IllegalArgumentException("Unknown scenario: " + value));
		}
	}

	private record ScheduledRun(int round, Scenario scenario, Implementation implementation) {
	}

	private record ExpectedProvenance(Implementation implementation,
	                                  Scenario scenario,
	                                  int round,
	                                  String buildSha,
	                                  String classPathSha256,
	                                  String datasetId) {
	}

	private static void runWorker(Options options) throws Exception {
		Path selectedClasses = options.implementation() == Implementation.BASELINE
				? options.baselineClasses() : options.candidateClasses();
		String expectedBuildSha = options.implementation() == Implementation.BASELINE
				? options.buildBaseline() : options.buildCandidate();
		String declaredBuildState = options.implementation() == Implementation.BASELINE
				? options.buildStateBaseline() : options.buildStateCandidate();
		String actualBuildSha = verifyBuildCheckout(selectedClasses, expectedBuildSha, declaredBuildState);
		String actualClassPathHash = classPathContentSha256(
				normalizedClassPath(System.getProperty("java.class.path")));
		if (!actualClassPathHash.equals(options.expectedClassPathSha256())) {
			throw new IllegalArgumentException("Worker classpath fingerprint mismatch: expected="
					+ options.expectedClassPathSha256() + " actual=" + actualClassPathHash);
		}
		validateDatasetMetadata(options);
		String arenaInstrumentationSha256 = ExactArenaTracking.install(EmbeddedDB.class,
				"existsMultiStatusOnly");
		ProcessSnapshot.enableAllocationMeasurement();
		long leaksBefore = RocksLeakDetector.detectedLeakCount();
		EmbeddedConnection embedded = null;
		GrpcServer server = null;
		GrpcConnection client = null;
		BenchmarkMeterRegistry meterRegistry = null;
		WorkerMeasurement measurement;
		Throwable closeFailure = null;
		try {
			Path shared = options.datasetRoot().toAbsolutePath().normalize();
			embedded = new EmbeddedConnection(shared.resolve("db"), COLUMN_NAME,
					shared.resolve("rockserver.conf"));
			meterRegistry = new BenchmarkMeterRegistry();
			server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0));
			server.start();
			client = GrpcConnection.forHostAndPort("grpc-retained-read-client",
					new HostAndPort("127.0.0.1", server.getPort()));
			RocksDBSyncAPI metadata = client.getSyncApi(RequestContext.batch());
			long columnId = metadata.getColumnId(COLUMN_NAME);
			var context = WorkerContext.create(client, columnId, options);
			measurement = measureScenario(context, embedded, server, meterRegistry, options);
		} finally {
			if (client != null) {
				try {
					client.close();
				} catch (Throwable failure) {
					closeFailure = appendFailure(closeFailure, failure);
				}
			}
			if (server != null) {
				try {
					server.close();
				} catch (Throwable failure) {
					closeFailure = appendFailure(closeFailure, failure);
				}
			}
			if (embedded != null && meterRegistry != null) {
				try {
					((CompositeMeterRegistry) embedded.getEmbeddedDB().getMetricsRegistry()).remove(meterRegistry);
					meterRegistry.close();
				} catch (Throwable failure) {
					closeFailure = appendFailure(closeFailure, failure);
				}
			}
			if (embedded != null) {
				try {
					embedded.closeTesting();
				} catch (Throwable failure) {
					closeFailure = appendFailure(closeFailure, failure);
				}
			}
		}
		if (closeFailure != null) {
			throw rethrow(closeFailure);
		}
		long nativeLeaks = awaitNativeLeakDetection(leaksBefore);
		WorkerResult result = WorkerResult.from(options, actualBuildSha, actualClassPathHash,
				arenaInstrumentationSha256, measurement, nativeLeaks);
		result.write(options.output());
		System.out.printf(Locale.ROOT,
				"RETAINED_RESULT scenario=%s implementation=%s round=%d entries=%.3f/s "
						+ "completion_p99=%.3fms cpu=%.3fns/item allocation=%.3fB/item "
						+ "accepted=%d quantums=%d passed=%s%n",
				result.scenario().value, result.implementation().value, result.round(),
				result.entriesPerSecond(), result.completionP99Nanos() / 1_000_000.0d,
				result.cpuNanosPerItem(), result.allocatedBytesPerItem(), result.scheduler().accepted(),
				result.scheduler().quantums(), result.structurallyPassed());
	}

	private static void validateDatasetMetadata(Options options) throws IOException {
		Path metadata = options.datasetRoot().resolve("dataset.properties");
		Map<String, String> values = strictProperties(Files.readString(metadata), Set.of(
				"schema", "dataset-id", "preload-keys", "flush-keys", "value-bytes", "value-magic"));
		requireEqual(values, "schema", DATASET_SCHEMA);
		requireEqual(values, "dataset-id", options.datasetId());
		requireEqual(values, "preload-keys", Integer.toString(options.preloadKeys()));
		requireEqual(values, "flush-keys", Integer.toString(options.flushKeys()));
		requireEqual(values, "value-bytes", Integer.toString(options.valueBytes()));
		requireEqual(values, "value-magic", Long.toUnsignedString(VALUE_MAGIC));
	}

	private static WorkerMeasurement measureScenario(WorkerContext context,
	                                                 EmbeddedConnection embedded,
	                                                 GrpcServer server,
	                                                 BenchmarkMeterRegistry meterRegistry,
	                                                 Options options) throws Exception {
		Scenario scenario = options.scenario();
		long coldStarted = System.nanoTime();
		OperationResult cold = executeOperation(context, scenario.operation, options);
		long coldCompletionNanos = System.nanoTime() - coldStarted;
		for (int index = 0; index < options.warmupOperations(); index++) {
			executeOperation(context, scenario.operation, options);
		}
		awaitDrain(embedded, server);
		System.gc();
		Thread.sleep(options.smoke() ? 10L : 100L);
		((CompositeMeterRegistry) embedded.getEmbeddedDB().getMetricsRegistry()).add(meterRegistry);

		LatencyRecorder completions = new LatencyRecorder(options.maxLatencySamples());
		LatencyRecorder firstItems = new LatencyRecorder(options.maxLatencySamples());
		LatencyRecorder foregroundLatencies = new LatencyRecorder(options.maxLatencySamples());
		AtomicBoolean done = new AtomicBoolean();
		LongAdder foregroundOperations = new LongAdder();
		LongAdder foregroundErrors = new LongAdder();
		ResourceSampler sampler = new ResourceSampler(embedded, server, options.sampleNanos());
		int backgroundWorkers = (scenario.mixed ? options.foregroundWorkers() : 0) + 1;
		ExecutorService executor = Executors.newFixedThreadPool(backgroundWorkers,
				Thread.ofPlatform().name("retained-read-measure-", 0).factory());
		CountDownLatch ready = new CountDownLatch(backgroundWorkers);
		CountDownLatch start = new CountDownLatch(1);
		List<Future<?>> futures = new ArrayList<>();
		try {
			futures.add(executor.submit(() -> {
				ready.countDown();
				start.await();
				sampler.run(done);
				return null;
			}));
			if (scenario.mixed) {
				for (int worker = 0; worker < options.foregroundWorkers(); worker++) {
					int workerIndex = worker;
					futures.add(executor.submit(() -> {
						ready.countDown();
						start.await();
						runForeground(context, options, workerIndex, done, foregroundOperations,
								foregroundErrors, foregroundLatencies);
						return null;
					}));
				}
			}
			if (!ready.await(30, TimeUnit.SECONDS)) {
				throw new IllegalStateException("Measurement workers did not become ready");
			}
			sampler.resetPeaks();
			MetricSnapshot schedulerBefore = MetricSnapshot.capture(meterRegistry,
					WorkloadProfile.BATCH, scenario.operation.family);
			ProcessSnapshot processBefore = ProcessSnapshot.capture();
			long started = System.nanoTime();
			long deadline = started + TimeUnit.SECONDS.toNanos(options.measureSeconds());
			start.countDown();
			long operations = 0L;
			long items = 0L;
			long logicalBytes = 0L;
			long checksum = 0L;
			boolean first = true;
			while (first || System.nanoTime() < deadline) {
				first = false;
				long operationStarted = System.nanoTime();
				OperationResult operation = executeOperation(context, scenario.operation, options);
				long completed = System.nanoTime();
				completions.record(completed - operationStarted);
				if (scenario.hasFirstItem()) {
					firstItems.record(operation.firstItemNanos());
				}
				operations++;
				items = Math.addExact(items, operation.items());
				logicalBytes = Math.addExact(logicalBytes, operation.logicalBytes());
				checksum = mix64(checksum ^ operation.checksum() ^ operations);
			}
			long finished = System.nanoTime();
			done.set(true);
			for (Future<?> future : futures) {
				future.get();
			}
			ProcessSnapshot processAfter = ProcessSnapshot.capture();
			ResourceSnapshot resources = awaitDrain(embedded, server, sampler);
			MetricSnapshot schedulerAfter = MetricSnapshot.capture(meterRegistry,
					WorkloadProfile.BATCH, scenario.operation.family);
			MetricSnapshot scheduler = schedulerAfter.minus(schedulerBefore);
			long normalizedItems = Math.addExact(items, foregroundOperations.sum());
			if (operations <= 0L || items <= 0L || normalizedItems <= 0L) {
				throw new IllegalStateException("Scenario completed no measurable work");
			}
			boolean accountingValid = accountingValid(options.implementation(), scenario, operations, scheduler);
			boolean correctness = foregroundErrors.sum() == 0L
					&& (!scenario.mixed || foregroundOperations.sum() > 0L)
					&& completions.samples() > 0L
					&& (!scenario.hasFirstItem() || firstItems.samples() == completions.samples());
			long elapsedNanos = finished - started;
			BenchmarkProcessTelemetry.Peaks processPeaks = sampler.processPeaks();
			return new WorkerMeasurement(coldCompletionNanos, cold.firstItemNanos(), elapsedNanos,
					operations, items, logicalBytes, checksum,
					items * 1_000_000_000.0d / elapsedNanos,
					logicalBytes * 1_000_000_000.0d / elapsedNanos / (1024.0d * 1024.0d),
					completions.p99(), scenario.hasFirstItem() ? firstItems.p99() : 0L,
					scenario.mixed ? foregroundLatencies.p99() : 0L, foregroundOperations.sum(),
					(processAfter.cpuNanos() - processBefore.cpuNanos()) / (double) normalizedItems,
					(processAfter.allocatedBytes() - processBefore.allocatedBytes()) / (double) normalizedItems,
					processAfter.gcCollections() - processBefore.gcCollections(),
					processAfter.gcMillis() - processBefore.gcMillis(), processPeaks.liveHeapBytes(),
					processPeaks.directMemoryBytes(), processPeaks.residentSetBytes(),
					processPeaks.threadCount(), processPeaks.nativeHandles(), scheduler, sampler.peaks(), resources,
					correctness, accountingValid, options.configuredRetainedLimit());
		} finally {
			done.set(true);
			start.countDown();
			executor.shutdownNow();
			try {
				if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
					throw new IllegalStateException("Measurement workers did not terminate");
				}
			} finally {
				sampler.close();
			}
		}
	}

	private static boolean accountingValid(Implementation implementation,
	                                       Scenario scenario,
	                                       long operations,
	                                       MetricSnapshot scheduler) {
		boolean conserved = scheduler.accepted() > 0L
				&& scheduler.accepted() == scheduler.started()
				&& scheduler.accepted() == scheduler.terminal()
				&& scheduler.accepted() == scheduler.executionTerminal()
				&& scheduler.quantums() >= scheduler.accepted()
				&& scheduler.failures() == 0L
				&& scheduler.rejections() == 0L
				&& scheduler.cancellations() == 0L;
		if (!conserved || implementation == Implementation.BASELINE) {
			return conserved;
		}
		boolean oneLogicalTask = scheduler.accepted() == operations;
		boolean forcedYield = !scenario.mixed
				|| scheduler.quantums() >= Math.multiplyExact(operations, 2L);
		return oneLogicalTask && forcedYield;
	}

	private static void runForeground(WorkerContext context,
	                                  Options options,
	                                  int worker,
	                                  AtomicBoolean done,
	                                  LongAdder operations,
	                                  LongAdder errors,
	                                  LatencyRecorder latencies) {
		long sequence = worker;
		while (!done.get()) {
			long expectedKey = Math.floorMod(mix64(sequence += options.foregroundWorkers()),
					options.preloadKeys());
			long started = System.nanoTime();
			try {
				Buf value = context.latencyApi().get(0L, context.columnId(), key(expectedKey),
						RequestType.current());
				validateValue(value, expectedKey, options.valueBytes());
				latencies.record(System.nanoTime() - started);
				operations.increment();
			} catch (Throwable failure) {
				errors.increment();
				done.set(true);
				return;
			}
		}
	}

	private static OperationResult executeOperation(WorkerContext context,
	                                                Operation operation,
	                                                Options options) {
		return switch (operation) {
			case EXACT_COUNT -> executeCount(context, options);
			case STREAM_RANGE -> executeRange(context, options);
			case EXISTS_MULTI -> executeExistsMulti(context, options);
			case ITERATOR -> executeIterator(context, options);
		};
	}

	private static OperationResult executeCount(WorkerContext context, Options options) {
		long count = context.retainedApi().reduceRange(0L, context.columnId(), null, null, false,
				RequestType.entriesCount(), READ_TIMEOUT_MILLIS);
		if (count != options.preloadKeys()) {
			throw new IllegalStateException("Exact count mismatch: expected=" + options.preloadKeys()
					+ " actual=" + count);
		}
		return new OperationResult(count, Math.multiplyExact(count, Long.BYTES + options.valueBytes()),
				context.datasetChecksum(), 0L);
	}

	private static OperationResult executeRange(WorkerContext context, Options options) {
		long started = System.nanoTime();
		long firstItemNanos = 0L;
		long count = 0L;
		long checksum = 0L;
		try (var stream = context.retainedApi().getRange(0L, context.columnId(), null, null, false,
				RequestType.<KV>allInRange(), READ_TIMEOUT_MILLIS)) {
			var iterator = stream.iterator();
			while (iterator.hasNext()) {
				KV entry = iterator.next();
				if (firstItemNanos == 0L) {
					firstItemNanos = System.nanoTime() - started;
				}
				long key = validateEntry(entry, count, options.valueBytes());
				checksum = checksumStep(checksum, key);
				count++;
			}
		}
		if (count != options.preloadKeys() || checksum != context.datasetChecksum()) {
			throw new IllegalStateException("Range mismatch: entries=" + count + " checksum="
					+ Long.toUnsignedString(checksum));
		}
		return new OperationResult(count, Math.multiplyExact(count, Long.BYTES + options.valueBytes()),
				checksum, firstItemNanos);
	}

	private static OperationResult executeExistsMulti(WorkerContext context, Options options) {
		List<Boolean> result = context.retainedApi().existsMulti(0L, context.columnId(),
				context.existsKeys(), READ_TIMEOUT_MILLIS);
		if (result.size() != context.existsExpected().size()) {
			throw new IllegalStateException("existsMulti result size mismatch");
		}
		long checksum = 0L;
		for (int index = 0; index < result.size(); index++) {
			boolean actual = result.get(index);
			boolean expected = context.existsExpected().get(index);
			if (actual != expected) {
				throw new IllegalStateException("existsMulti mismatch at " + index + ": expected="
						+ expected + " actual=" + actual);
			}
			checksum = mix64(checksum ^ (actual ? index + 1L : ~index));
		}
		return new OperationResult(result.size(), Math.multiplyExact((long) result.size(), Long.BYTES),
				checksum, 0L);
	}

	private static OperationResult executeIterator(WorkerContext context, Options options) {
		long iteratorId = context.retainedApi().openIterator(0L, context.columnId(), new Keys(), null,
				false, READ_TIMEOUT_MILLIS);
		try {
			List<Buf> values = context.retainedApi().subsequent(iteratorId, options.iteratorSkip(),
					options.iteratorTake(), RequestType.multi());
			if (values.size() != options.iteratorTake()) {
				throw new IllegalStateException("Iterator result size mismatch: expected="
						+ options.iteratorTake() + " actual=" + values.size());
			}
			long checksum = 0L;
			for (int index = 0; index < values.size(); index++) {
				long expectedKey = options.iteratorSkip() + index;
				validateValue(values.get(index), expectedKey, options.valueBytes());
				checksum = checksumStep(checksum, expectedKey);
			}
			long logicalBytes = Math.multiplyExact((long) values.size(), options.valueBytes());
			return new OperationResult(values.size(), logicalBytes, checksum, 0L);
		} finally {
			context.retainedApi().closeIterator(iteratorId);
		}
	}

	private static long validateEntry(KV entry, long expectedKey, int valueBytes) {
		if (entry.keys().keys().length != 1 || entry.keys().keys()[0].size() != Long.BYTES) {
			throw new IllegalStateException("Malformed range key at " + expectedKey);
		}
		long actualKey = entry.keys().keys()[0].getLong(0);
		if (actualKey != expectedKey) {
			throw new IllegalStateException("Range order mismatch: expected=" + expectedKey
					+ " actual=" + actualKey);
		}
		validateValue(entry.value(), actualKey, valueBytes);
		return actualKey;
	}

	private static void validateValue(Buf value, long expectedKey, int expectedSize) {
		if (value == null || value.size() != expectedSize
				|| value.getLong(0) != VALUE_MAGIC
				|| value.getLong(Long.BYTES) != expectedKey
				|| value.getLong(expectedSize - Long.BYTES) != (VALUE_MAGIC ^ expectedKey)) {
			throw new IllegalStateException("Value mismatch for key " + expectedKey);
		}
	}

	private static long checksumStep(long checksum, long key) {
		return mix64(checksum ^ key ^ VALUE_MAGIC);
	}

	private static long datasetChecksum(int keys) {
		long checksum = 0L;
		for (long key = 0L; key < keys; key++) {
			checksum = checksumStep(checksum, key);
		}
		return checksum;
	}

	private static ResourceSnapshot awaitDrain(EmbeddedConnection embedded, GrpcServer server)
			throws InterruptedException {
		return awaitDrain(embedded, server, null);
	}

	private static ResourceSnapshot awaitDrain(EmbeddedConnection embedded,
	                                           GrpcServer server,
	                                           ResourceSampler sampler) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20L);
		ResourceSnapshot snapshot;
		do {
			snapshot = ResourceSnapshot.capture(embedded, server, sampler);
			if (snapshot.drained()) {
				return snapshot;
			}
			Thread.sleep(10L);
		} while (System.nanoTime() < deadline);
		return snapshot;
	}

	private static long awaitNativeLeakDetection(long before) throws InterruptedException {
		for (int attempt = 0; attempt < 3; attempt++) {
			System.gc();
			Thread.sleep(100L);
		}
		return Math.max(0L, RocksLeakDetector.detectedLeakCount() - before);
	}

	private static Throwable appendFailure(Throwable existing, Throwable added) {
		if (existing == null) return added;
		existing.addSuppressed(added);
		return existing;
	}

	private static Exception rethrow(Throwable failure) {
		return failure instanceof Exception exception ? exception : new RuntimeException(failure);
	}

	private record OperationResult(long items, long logicalBytes, long checksum, long firstItemNanos) {
	}

	private record WorkerContext(RocksDBSyncAPI retainedApi,
	                             RocksDBSyncAPI latencyApi,
	                             long columnId,
	                             List<Keys> existsKeys,
	                             List<Boolean> existsExpected,
	                             long datasetChecksum) {

		private static WorkerContext create(GrpcConnection client, long columnId, Options options) {
			RocksDBSyncAPI retained = client.getSyncApi(RequestContext.batch());
			RocksDBSyncAPI latency = client.getSyncApi(new RequestContext(WorkloadProfile.LATENCY,
					System.currentTimeMillis() + TimeUnit.HOURS.toMillis(2L)));
			List<Keys> keys = new ArrayList<>(options.existsItems());
			List<Boolean> expected = new ArrayList<>(options.existsItems());
			for (int index = 0; index < options.existsItems(); index++) {
				boolean hit = (index & 1) == 0;
				long key = hit ? (index / 2L) % options.preloadKeys()
						: options.preloadKeys() + index + 1L;
				keys.add(key(key));
				expected.add(hit);
			}
			return new WorkerContext(retained, latency, columnId, List.copyOf(keys),
					List.copyOf(expected), GrpcRetainedReadBenchmark.datasetChecksum(options.preloadKeys()));
		}
	}

	private static final class LatencyRecorder {

		private final long[] samples;
		private final AtomicLong sequence = new AtomicLong();

		private LatencyRecorder(int capacity) {
			this.samples = new long[capacity];
		}

		private void record(long nanos) {
			long index = sequence.getAndIncrement();
			samples[(int) Math.floorMod(index, samples.length)] = Math.max(1L, nanos);
		}

		private long samples() {
			return Math.min(sequence.get(), samples.length);
		}

		private long p99() {
			int count = Math.toIntExact(samples());
			if (count == 0) return 0L;
			long[] sorted = Arrays.copyOf(samples, count);
			Arrays.sort(sorted);
			return GrpcOverloadBenchmark.percentile(sorted, 0.99d);
		}
	}

	private record ProcessSnapshot(long cpuNanos,
	                               long allocatedBytes,
	                               long gcCollections,
	                               long gcMillis) {

		private static void enableAllocationMeasurement() {
			ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
			if (!threads.isThreadAllocatedMemorySupported()) {
				throw new IllegalStateException("Thread allocation measurement is unavailable");
			}
			if (!threads.isThreadAllocatedMemoryEnabled()) {
				threads.setThreadAllocatedMemoryEnabled(true);
			}
		}

		private static ProcessSnapshot capture() {
			OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
			ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
			long collections = 0L;
			long millis = 0L;
			for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
				collections += Math.max(0L, gc.getCollectionCount());
				millis += Math.max(0L, gc.getCollectionTime());
			}
			return new ProcessSnapshot(os.getProcessCpuTime(), threads.getTotalThreadAllocatedBytes(),
					collections, millis);
		}
	}

	private static final class ResourceSampler {

		private final EmbeddedConnection embedded;
		private final GrpcServer server;
		private final long sampleNanos;
		private final ExistsMultiResourceProbe existsMultiProbe;
		private final BenchmarkProcessTelemetry.PeakSampler processPeaks =
				new BenchmarkProcessTelemetry.PeakSampler();
		private ResourcePeaks peaks = ResourcePeaks.empty();

		private ResourceSampler(EmbeddedConnection embedded,
		                        GrpcServer server,
		                        long sampleNanos) {
			this.embedded = embedded;
			this.server = server;
			this.sampleNanos = sampleNanos;
			this.existsMultiProbe = new ExistsMultiResourceProbe(embedded.getInternalDB());
		}

		private void resetPeaks() {
			processPeaks.reset();
			peaks = ResourcePeaks.empty();
			existsMultiProbe.resetObservedPeaks();
		}

		private void run(AtomicBoolean done) throws InterruptedException {
			do {
				sample();
				LockSupport.parkNanos(sampleNanos);
				if (Thread.interrupted()) throw new InterruptedException();
			} while (!done.get());
			sample();
		}

		private void sample() {
			processPeaks.sample();
			peaks = peaks.max(ResourceSnapshot.capture(embedded, server, this));
		}

		private BenchmarkProcessTelemetry.Peaks processPeaks() {
			return processPeaks.peaks();
		}

		private ResourcePeaks peaks() {
			return peaks.withExistsMultiPeaks(existsMultiProbe.observedPeaks());
		}

		private ExistsMultiResources existsMultiResources() {
			return existsMultiProbe.capture();
		}

		private void close() {
			try {
				existsMultiProbe.close();
			} finally {
				processPeaks.close();
			}
		}
	}

	private static final class ExistsMultiResourceProbe {

		private final EmbeddedDB database;
		private final Field activeRequestsField;
		private final Field cursorField;
		private final Field snapshotField;
		private final Field readOptionsField;
		private final AtomicInteger snapshotObserved = new AtomicInteger();
		private final AtomicInteger chunkObserved = new AtomicInteger();

		private ExistsMultiResourceProbe(EmbeddedDB database) {
			this.database = database;
			try {
				Class<?> databaseType = database.getClass();
				activeRequestsField = accessible(databaseType.getDeclaredField("activeExistsMultiRequests"));
				Class<?> requestType = nestedClass(databaseType, "AsyncExistsMultiRequest");
				Class<?> cursorType = nestedClass(databaseType, "ExistsMultiCursor");
				cursorField = accessible(requestType.getDeclaredField("cursor"));
				snapshotField = accessible(cursorType.getDeclaredField("snapshot"));
				readOptionsField = accessible(cursorType.getDeclaredField("readOptions"));
			} catch (ReflectiveOperationException failure) {
				throw new IllegalStateException("Retained benchmark cannot inspect existsMulti resources", failure);
			}
			database.setExistsMultiSnapshotObserverForTesting(() -> snapshotObserved.set(1));
			database.setExistsMultiChunkObserverForTesting(() -> chunkObserved.set(1));
		}

		private static Class<?> nestedClass(Class<?> owner, String simpleName) {
			return Arrays.stream(owner.getDeclaredClasses())
					.filter(candidate -> candidate.getSimpleName().equals(simpleName))
					.findFirst()
					.orElseThrow(() -> new IllegalStateException("Missing nested resource type " + simpleName));
		}

		private static Field accessible(Field field) {
			field.setAccessible(true);
			return field;
		}

		private ExistsMultiResources capture() {
			try {
				int requests = 0;
				int snapshots = 0;
				int readOptions = 0;
				for (Object request : (Set<?>) activeRequestsField.get(database)) {
					requests++;
					Object cursor = cursorField.get(request);
					if (cursor != null) {
						if (snapshotField.get(cursor) != null) snapshots++;
						if (readOptionsField.get(cursor) != null) readOptions++;
					}
				}
				ExactArenaTracking.assertValid();
				return new ExistsMultiResources(requests, snapshots, readOptions,
						ExactArenaTracking.activeArenas());
			} catch (IllegalAccessException impossible) {
				throw new IllegalStateException("Cannot inspect existsMulti retained resources", impossible);
			}
		}

		private void resetObservedPeaks() {
			ExactArenaTracking.resetPeakAfterDrain();
			snapshotObserved.set(0);
			chunkObserved.set(0);
		}

		private ExistsMultiResources observedPeaks() {
			ExactArenaTracking.assertValid();
			int snapshot = snapshotObserved.get();
			int chunk = chunkObserved.get();
			return new ExistsMultiResources(Math.max(snapshot, chunk), snapshot, chunk,
					ExactArenaTracking.peakArenas());
		}

		private void close() {
			try {
				database.setExistsMultiSnapshotObserverForTesting(null);
				database.setExistsMultiChunkObserverForTesting(null);
			} finally {
				ExactArenaTracking.assertDrained();
			}
		}
	}

	/**
	 * Exact per-call Arena tracking injected into the selected production EmbeddedDB bytecode.
	 * Both baseline and candidate receive the same substitution, so measurement overhead and
	 * lifecycle evidence are symmetric. Installation fails unless the target contains exactly
	 * one expected Arena.ofConfined call and the transformed bytes contain exactly one tracker call.
	 */
	private static final class ExactArenaTracking {

		private static final String ARENA_OWNER = "java/lang/foreign/Arena";
		private static final String TRACKER_OWNER = GrpcRetainedReadBenchmark.class.getName().replace('.', '/');
		private static final AtomicInteger ACTIVE = new AtomicInteger();
		private static final AtomicInteger PEAK = new AtomicInteger();
		private static volatile RuntimeException lifecycleFailure;

		private static String install(Class<?> target, String targetMethod) throws Exception {
			var transformation = transform(target, targetMethod);
			var instrumentation = ByteBuddyAgent.getInstrumentation();
			transformation.unloaded().load(target.getClassLoader(), ClassReloadingStrategy.of(instrumentation));
			resetPeakAfterDrain();
			return fingerprint(transformation.bytes());
		}

		private static ArenaTransformation transform(Class<?> target, String targetMethod) throws Exception {
			byte[] original = classBytes(target);
			if (countCalls(original, targetMethod, ARENA_OWNER, "ofConfined") != 1) {
				throw new IllegalStateException("Expected exactly one Arena.ofConfined call in "
						+ target.getName() + '.' + targetMethod);
			}
			Method replacement = GrpcRetainedReadBenchmark.class.getMethod("openTrackedArena");
			var transformed = new ByteBuddy()
					.redefine(target)
					.visit(MemberSubstitution.strict()
							.method(ElementMatchers.isDeclaredBy(Arena.class)
									.and(ElementMatchers.named("ofConfined")))
							.replaceWith(replacement)
							.on(ElementMatchers.named(targetMethod)))
					.make();
			byte[] transformedBytes = transformed.getBytes();
			if (countCalls(transformedBytes, targetMethod, ARENA_OWNER, "ofConfined") != 0
					|| countCalls(transformedBytes, targetMethod, TRACKER_OWNER, "openTrackedArena") != 1) {
				throw new IllegalStateException("Exact Arena substitution did not match the expected call site");
			}
			return new ArenaTransformation(transformed, transformedBytes);
		}

		private static String fingerprint(byte[] transformedBytes) throws Exception {
			String agentHash = classPathContentSha256(Path.of(ByteBuddyAgent.class.getProtectionDomain()
					.getCodeSource().getLocation().toURI()).toString());
			return sha256(transformedBytes, classBytes(GrpcRetainedReadBenchmark.class),
					agentHash.getBytes(StandardCharsets.UTF_8));
		}

		private record ArenaTransformation(net.bytebuddy.dynamic.DynamicType.Unloaded<?> unloaded,
		                                   byte[] bytes) {
		}

		private static int countCalls(byte[] classBytes,
		                              String targetMethod,
		                              String owner,
		                              String method) {
			var count = new AtomicInteger();
			new ClassReader(classBytes).accept(new ClassVisitor(Opcodes.ASM9) {
				@Override
				public MethodVisitor visitMethod(int access,
				                                 String name,
				                                 String descriptor,
				                                 String signature,
				                                 String[] exceptions) {
					MethodVisitor delegate = super.visitMethod(access, name, descriptor, signature, exceptions);
					if (!name.equals(targetMethod)) {
						return delegate;
					}
					return new MethodVisitor(Opcodes.ASM9, delegate) {
						@Override
						public void visitMethodInsn(int opcode,
						                            String actualOwner,
						                            String actualName,
						                            String actualDescriptor,
						                            boolean isInterface) {
							if (actualOwner.equals(owner) && actualName.equals(method)) {
								count.incrementAndGet();
							}
							super.visitMethodInsn(opcode, actualOwner, actualName, actualDescriptor, isInterface);
						}
					};
				}
			}, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
			return count.get();
		}

		private static byte[] classBytes(Class<?> type) throws IOException {
			String resource = '/' + type.getName().replace('.', '/') + ".class";
			try (InputStream input = type.getResourceAsStream(resource)) {
				if (input == null) {
					throw new IOException("Cannot read class bytes for " + type.getName());
				}
				return input.readAllBytes();
			}
		}

		private static Arena open() {
			assertValid();
			Arena delegate = Arena.ofConfined();
			int active = ACTIVE.incrementAndGet();
			PEAK.accumulateAndGet(active, Math::max);
			return new TrackedArena(delegate);
		}

		private static int activeArenas() {
			assertValid();
			return ACTIVE.get();
		}

		private static int peakArenas() {
			assertValid();
			return PEAK.get();
		}

		private static void resetPeakAfterDrain() {
			assertDrained();
			PEAK.set(0);
		}

		private static void assertDrained() {
			assertValid();
			int active = ACTIVE.get();
			if (active != 0) {
				throw new IllegalStateException("existsMulti Arena lifetime did not drain: active=" + active);
			}
		}

		private static void assertValid() {
			var failure = lifecycleFailure;
			if (failure != null) {
				throw failure;
			}
		}

		private static final class TrackedArena implements Arena {

			private final Arena delegate;
			private final AtomicBoolean closed = new AtomicBoolean();

			private TrackedArena(Arena delegate) {
				this.delegate = delegate;
			}

			@Override
			public MemorySegment allocate(long byteSize, long byteAlignment) {
				return delegate.allocate(byteSize, byteAlignment);
			}

			@Override
			public MemorySegment.Scope scope() {
				return delegate.scope();
			}

			@Override
			public void close() {
				if (!closed.compareAndSet(false, true)) {
					var failure = new IllegalStateException("Tracked existsMulti Arena closed more than once");
					lifecycleFailure = failure;
					throw failure;
				}
				try {
					delegate.close();
				} catch (RuntimeException | Error failure) {
					lifecycleFailure = new IllegalStateException("Tracked existsMulti Arena close failed", failure);
					throw failure;
				}
				int active = ACTIVE.decrementAndGet();
				if (active < 0) {
					lifecycleFailure = new IllegalStateException(
							"Tracked existsMulti Arena close had no matching open");
					throw lifecycleFailure;
				}
			}
		}
	}

	/** Replacement target used by exact baseline/candidate bytecode instrumentation. */
	public static Arena openTrackedArena() {
		return ExactArenaTracking.open();
	}

	private record ExistsMultiResources(int requests,
	                                    int snapshots,
	                                    int readOptions,
	                                    int arenas) {

		private static ExistsMultiResources empty() {
			return new ExistsMultiResources(0, 0, 0, 0);
		}
	}

	private record ResourcePeaks(int queued,
	                             int active,
	                             int parked,
	                             int outstanding,
	                             long pending,
	                             int iterators,
	                             int rangeCursors,
	                             int retainedSnapshots,
	                             int retainedPermits,
	                             int retainedWaiters,
	                             int iteratorLeases,
	                             int existsMultiRequests,
	                             int existsMultiSnapshots,
	                             int existsMultiReadOptions,
	                             int existsMultiArenas) {

		private static ResourcePeaks empty() {
			return new ResourcePeaks(0, 0, 0, 0, 0L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
		}

		private ResourcePeaks max(ResourceSnapshot snapshot) {
			return new ResourcePeaks(Math.max(queued, snapshot.queued()),
					Math.max(active, snapshot.active()), Math.max(parked, snapshot.parked()),
					Math.max(outstanding, snapshot.outstanding()), Math.max(pending, snapshot.pending()),
					Math.max(iterators, snapshot.iterators()),
					Math.max(rangeCursors, snapshot.rangeCursors()),
					Math.max(retainedSnapshots, snapshot.retainedSnapshots()),
					Math.max(retainedPermits, snapshot.retainedPermits()),
					Math.max(retainedWaiters, snapshot.retainedWaiters()),
					Math.max(iteratorLeases, snapshot.iteratorLeases()),
					Math.max(existsMultiRequests, snapshot.existsMultiRequests()),
					Math.max(existsMultiSnapshots, snapshot.existsMultiSnapshots()),
					Math.max(existsMultiReadOptions, snapshot.existsMultiReadOptions()),
					Math.max(existsMultiArenas, snapshot.existsMultiArenas()));
		}

		private ResourcePeaks withExistsMultiPeaks(ExistsMultiResources observed) {
			return new ResourcePeaks(queued, active, parked, outstanding, pending, iterators, rangeCursors,
					retainedSnapshots, retainedPermits, retainedWaiters, iteratorLeases,
					Math.max(existsMultiRequests, observed.requests()),
					Math.max(existsMultiSnapshots, observed.snapshots()),
					Math.max(existsMultiReadOptions, observed.readOptions()),
					Math.max(existsMultiArenas, observed.arenas()));
		}
	}

	private record ResourceSnapshot(int queued,
	                                int active,
	                                int parked,
	                                int outstanding,
	                                long submissionAttempts,
	                                long terminalOutcomes,
	                                boolean exactSchedulerAccounting,
	                                long pending,
	                                int transactions,
	                                int iterators,
	                                int rangeCursors,
	                                int retainedSnapshots,
	                                int retainedPermits,
	                                int retainedWaiters,
	                                int iteratorLeases,
	                                int existsMultiRequests,
	                                int existsMultiSnapshots,
	                                int existsMultiReadOptions,
	                                int existsMultiArenas) {

		private static ResourceSnapshot capture(EmbeddedConnection embedded,
		                                        GrpcServer server,
		                                        ResourceSampler sampler) {
			RWScheduler scheduler = embedded.getScheduler();
			int queued = 0;
			int active = 0;
			int parked = 0;
			int outstanding = 0;
			long submissionAttempts = 0L;
			long terminalOutcomes = 0L;
			for (RWScheduler.Pool pool : RWScheduler.Pool.values()) {
				var snapshot = scheduler.poolSnapshot(pool);
				queued += snapshot.queuedTasks();
				active += snapshot.activeTasks();
				int poolOutstanding = BenchmarkSchedulerTelemetry.outstandingTasks(snapshot);
				parked += BenchmarkSchedulerTelemetry.parkedTasks(snapshot, poolOutstanding);
				outstanding += poolOutstanding;
				submissionAttempts += BenchmarkSchedulerTelemetry.submissionAttempts(snapshot);
				terminalOutcomes += BenchmarkSchedulerTelemetry.terminalOutcomes(snapshot);
			}
			var database = embedded.getInternalDB();
			var existsMulti = sampler == null ? ExistsMultiResources.empty() : sampler.existsMultiResources();
			return new ResourceSnapshot(queued, active, parked, outstanding, submissionAttempts,
					terminalOutcomes, BenchmarkSchedulerTelemetry.exactAccounting(), database.getPendingOpsCount(),
					database.getOpenTransactionsCount(), database.getOpenIteratorsCount(),
					database.getActiveRangeCursorCount(), database.getRetainedRangeSnapshotCount(),
					database.getRetainedRangePermitCount(), database.getRetainedRangeWaiterCount(),
					server.getActiveIteratorOperationLeaseCountForTesting(), existsMulti.requests(),
					existsMulti.snapshots(), existsMulti.readOptions(), existsMulti.arenas());
		}

		private boolean drained() {
			return queued == 0 && active == 0 && parked == 0 && outstanding == 0
					&& submissionAttempts == terminalOutcomes && pending == 0L && transactions == 0
					&& iterators == 0 && rangeCursors == 0 && retainedSnapshots == 0
					&& retainedPermits == 0 && retainedWaiters == 0 && iteratorLeases == 0
					&& existsMultiRequests == 0 && existsMultiSnapshots == 0
					&& existsMultiReadOptions == 0 && existsMultiArenas == 0;
		}
	}

	private record MetricSnapshot(long accepted,
	                              long started,
	                              long terminal,
	                              long executionTerminal,
	                              long quantums,
	                              long failures,
	                              long rejections,
	                              long cancellations,
	                              long queueP99Nanos,
	                              long executionP99Nanos) {

		private static MetricSnapshot capture(BenchmarkMeterRegistry registry,
		                                      WorkloadProfile profile,
		                                      OperationFamily family) {
			long accepted = 0L;
			long started = 0L;
			long terminal = 0L;
			long executionTerminal = 0L;
			long quantums = 0L;
			long failures = 0L;
			long rejections = 0L;
			long cancellations = 0L;
			long queueP99Nanos = 0L;
			long executionP99Nanos = 0L;
			String profileTag = profile.name().toLowerCase(Locale.ROOT);
			String operationTag = family.name().toLowerCase(Locale.ROOT);
			for (Meter meter : registry.getMeters()) {
				if (!profileTag.equals(meter.getId().getTag("profile"))
						|| !operationTag.equals(meter.getId().getTag("operation"))) {
					continue;
				}
				switch (meter.getId().getName()) {
					case "rockserver.workload.admission" -> {
						if (meter instanceof Counter counter
								&& "accepted".equals(meter.getId().getTag("result"))) {
							accepted += Math.round(counter.count());
						}
					}
					case "rockserver.workload.queue.wait" -> {
						if (meter instanceof Timer timer) {
							started += timer.count();
							queueP99Nanos = Math.max(queueP99Nanos, timerP99(timer));
						}
					}
					case "rockserver.workload.execution" -> {
						if (meter instanceof Timer timer) {
							executionTerminal += timer.count();
							executionP99Nanos = Math.max(executionP99Nanos, timerP99(timer));
						}
					}
					case "rockserver.workload.outcomes" -> {
						if (meter instanceof Counter counter) terminal += Math.round(counter.count());
					}
					case "rockserver.workload.quantums" -> {
						if (meter instanceof Counter counter) quantums += Math.round(counter.count());
					}
					case "rockserver.workload.failures" -> {
						if (meter instanceof Counter counter) failures += Math.round(counter.count());
					}
					case "rockserver.workload.rejections" -> {
						if (meter instanceof Counter counter) rejections += Math.round(counter.count());
					}
					case "rockserver.workload.cancellations" -> {
						if (meter instanceof Counter counter) cancellations += Math.round(counter.count());
					}
					default -> {
					}
				}
			}
			return new MetricSnapshot(accepted, started, terminal, executionTerminal, quantums,
					failures, rejections, cancellations, queueP99Nanos, executionP99Nanos);
		}

		private MetricSnapshot minus(MetricSnapshot before) {
			return new MetricSnapshot(accepted - before.accepted, started - before.started,
					terminal - before.terminal, executionTerminal - before.executionTerminal,
					quantums - before.quantums, failures - before.failures,
					rejections - before.rejections, cancellations - before.cancellations,
					queueP99Nanos, executionP99Nanos);
		}
	}

	private static long timerP99(Timer timer) {
		for (var percentile : timer.takeSnapshot().percentileValues()) {
			if (Math.abs(percentile.percentile() - 0.99d) < 0.000_001d) {
				return Math.max(1L, (long) percentile.value(TimeUnit.NANOSECONDS));
			}
		}
		return 0L;
	}

	private static final class BenchmarkMeterRegistry extends SimpleMeterRegistry {

		@Override
		protected Timer newTimer(Meter.Id id,
		                         DistributionStatisticConfig distributionStatisticConfig,
		                         PauseDetector pauseDetector) {
			return super.newTimer(id, DistributionStatisticConfig.builder()
					.percentiles(0.99d)
					.percentilePrecision(3)
					.build()
					.merge(distributionStatisticConfig), pauseDetector);
		}
	}

	private record WorkerMeasurement(long coldCompletionNanos,
	                                 long coldFirstItemNanos,
	                                 long elapsedNanos,
	                                 long operations,
	                                 long items,
	                                 long logicalBytes,
	                                 long checksum,
	                                 double entriesPerSecond,
	                                 double mibPerSecond,
	                                 long completionP99Nanos,
	                                 long firstItemP99Nanos,
	                                 long foregroundP99Nanos,
	                                 long foregroundOperations,
	                                 double cpuNanosPerItem,
	                                 double allocatedBytesPerItem,
	                                 long gcCollections,
	                                 long gcMillis,
	                                 long peakLiveHeapBytes,
	                                 long peakDirectMemoryBytes,
	                                 long peakResidentSetBytes,
	                                 int peakThreadCount,
	                                 long peakNativeHandles,
	                                 MetricSnapshot scheduler,
	                                 ResourcePeaks resourcePeaks,
	                                 ResourceSnapshot finalResources,
	                                 boolean correctness,
	                                 boolean accountingValid,
	                                 int configuredRetainedLimit) {
	}

	private record WorkerResult(Implementation implementation,
	                            Scenario scenario,
	                            int round,
	                            String buildSha,
	                            String classPathSha256,
	                            String arenaInstrumentationSha256,
	                            String datasetId,
	                            long coldCompletionNanos,
	                            long coldFirstItemNanos,
	                            long elapsedNanos,
	                            long operations,
	                            long items,
	                            long logicalBytes,
	                            long checksum,
	                            double entriesPerSecond,
	                            double mibPerSecond,
	                            long completionP99Nanos,
	                            long firstItemP99Nanos,
	                            long foregroundP99Nanos,
	                            long foregroundOperations,
	                            double cpuNanosPerItem,
	                            double allocatedBytesPerItem,
	                            long gcCollections,
	                            long gcMillis,
	                            long peakLiveHeapBytes,
	                            long peakDirectMemoryBytes,
	                            long peakResidentSetBytes,
	                            int peakThreadCount,
	                            long peakNativeHandles,
	                            MetricSnapshot scheduler,
	                            ResourcePeaks resourcePeaks,
	                            ResourceSnapshot finalResources,
	                            long nativeLeaks,
	                            boolean correctness,
	                            boolean resourcesDrained,
	                            boolean accountingValid,
	                            int configuredRetainedLimit) {

		private static WorkerResult from(Options options,
		                                 String buildSha,
		                                 String classPathSha256,
		                                 String arenaInstrumentationSha256,
		                                 WorkerMeasurement measurement,
		                                 long nativeLeaks) {
			return new WorkerResult(options.implementation(), options.scenario(), options.round(), buildSha,
					classPathSha256, arenaInstrumentationSha256, options.datasetId(),
					measurement.coldCompletionNanos(),
					measurement.coldFirstItemNanos(), measurement.elapsedNanos(), measurement.operations(),
					measurement.items(), measurement.logicalBytes(), measurement.checksum(),
					measurement.entriesPerSecond(), measurement.mibPerSecond(),
					measurement.completionP99Nanos(), measurement.firstItemP99Nanos(),
					measurement.foregroundP99Nanos(), measurement.foregroundOperations(),
					measurement.cpuNanosPerItem(), measurement.allocatedBytesPerItem(),
					measurement.gcCollections(), measurement.gcMillis(), measurement.peakLiveHeapBytes(),
					measurement.peakDirectMemoryBytes(), measurement.peakResidentSetBytes(),
					measurement.peakThreadCount(), measurement.peakNativeHandles(), measurement.scheduler(),
					measurement.resourcePeaks(), measurement.finalResources(), nativeLeaks,
					measurement.correctness(), measurement.finalResources().drained(),
					measurement.accountingValid(), measurement.configuredRetainedLimit());
		}

		private void write(Path output) throws IOException {
			Map<String, String> values = toProperties();
			StringBuilder text = new StringBuilder();
			for (Map.Entry<String, String> entry : values.entrySet()) {
				text.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
			}
			Files.writeString(output, text, StandardOpenOption.CREATE_NEW);
		}

		private Map<String, String> toProperties() {
			Map<String, String> values = new LinkedHashMap<>();
			values.put("schema", WORKER_SCHEMA);
			values.put("implementation", implementation.value);
			values.put("scenario", scenario.value);
			values.put("round", Integer.toString(round));
			values.put("build-sha", buildSha);
			values.put("classpath-sha256", classPathSha256);
			values.put("arena-instrumentation-sha256", arenaInstrumentationSha256);
			values.put("dataset-id", datasetId);
			values.put("cold-completion-nanos", Long.toString(coldCompletionNanos));
			values.put("cold-first-item-nanos", Long.toString(coldFirstItemNanos));
			values.put("elapsed-nanos", Long.toString(elapsedNanos));
			values.put("operations", Long.toString(operations));
			values.put("items", Long.toString(items));
			values.put("logical-bytes", Long.toString(logicalBytes));
			values.put("checksum", Long.toUnsignedString(checksum));
			values.put("entries-per-second", Double.toString(entriesPerSecond));
			values.put("mib-per-second", Double.toString(mibPerSecond));
			values.put("completion-p99-nanos", Long.toString(completionP99Nanos));
			values.put("first-item-p99-nanos", Long.toString(firstItemP99Nanos));
			values.put("foreground-p99-nanos", Long.toString(foregroundP99Nanos));
			values.put("foreground-operations", Long.toString(foregroundOperations));
			values.put("cpu-nanos-per-item", Double.toString(cpuNanosPerItem));
			values.put("allocated-bytes-per-item", Double.toString(allocatedBytesPerItem));
			values.put("gc-collections", Long.toString(gcCollections));
			values.put("gc-millis", Long.toString(gcMillis));
			values.put("peak-live-heap-bytes", Long.toString(peakLiveHeapBytes));
			values.put("peak-direct-memory-bytes", Long.toString(peakDirectMemoryBytes));
			values.put("peak-resident-set-bytes", Long.toString(peakResidentSetBytes));
			values.put("peak-thread-count", Integer.toString(peakThreadCount));
			values.put("peak-native-handles", Long.toString(peakNativeHandles));
			values.put("scheduler-accepted", Long.toString(scheduler.accepted()));
			values.put("scheduler-started", Long.toString(scheduler.started()));
			values.put("scheduler-terminal", Long.toString(scheduler.terminal()));
			values.put("scheduler-execution-terminal", Long.toString(scheduler.executionTerminal()));
			values.put("scheduler-quantums", Long.toString(scheduler.quantums()));
			values.put("scheduler-failures", Long.toString(scheduler.failures()));
			values.put("scheduler-rejections", Long.toString(scheduler.rejections()));
			values.put("scheduler-cancellations", Long.toString(scheduler.cancellations()));
			values.put("scheduler-queue-p99-nanos", Long.toString(scheduler.queueP99Nanos()));
			values.put("scheduler-execution-p99-nanos", Long.toString(scheduler.executionP99Nanos()));
			values.put("peak-queued", Integer.toString(resourcePeaks.queued()));
			values.put("peak-active", Integer.toString(resourcePeaks.active()));
			values.put("peak-parked", Integer.toString(resourcePeaks.parked()));
			values.put("peak-outstanding", Integer.toString(resourcePeaks.outstanding()));
			values.put("peak-pending", Long.toString(resourcePeaks.pending()));
			values.put("peak-iterators", Integer.toString(resourcePeaks.iterators()));
			values.put("peak-range-cursors", Integer.toString(resourcePeaks.rangeCursors()));
			values.put("peak-retained-snapshots", Integer.toString(resourcePeaks.retainedSnapshots()));
			values.put("peak-retained-permits", Integer.toString(resourcePeaks.retainedPermits()));
			values.put("peak-retained-waiters", Integer.toString(resourcePeaks.retainedWaiters()));
			values.put("peak-iterator-leases", Integer.toString(resourcePeaks.iteratorLeases()));
			values.put("peak-exists-multi-requests", Integer.toString(resourcePeaks.existsMultiRequests()));
			values.put("peak-exists-multi-snapshots", Integer.toString(resourcePeaks.existsMultiSnapshots()));
			values.put("peak-exists-multi-read-options", Integer.toString(resourcePeaks.existsMultiReadOptions()));
			values.put("peak-exists-multi-arenas", Integer.toString(resourcePeaks.existsMultiArenas()));
			values.put("final-queued", Integer.toString(finalResources.queued()));
			values.put("final-active", Integer.toString(finalResources.active()));
			values.put("final-parked", Integer.toString(finalResources.parked()));
			values.put("final-outstanding", Integer.toString(finalResources.outstanding()));
			values.put("submission-attempts", Long.toString(finalResources.submissionAttempts()));
			values.put("terminal-outcomes", Long.toString(finalResources.terminalOutcomes()));
			values.put("scheduler-accounting-exact",
					Boolean.toString(finalResources.exactSchedulerAccounting()));
			values.put("final-pending", Long.toString(finalResources.pending()));
			values.put("final-transactions", Integer.toString(finalResources.transactions()));
			values.put("final-iterators", Integer.toString(finalResources.iterators()));
			values.put("final-range-cursors", Integer.toString(finalResources.rangeCursors()));
			values.put("final-retained-snapshots", Integer.toString(finalResources.retainedSnapshots()));
			values.put("final-retained-permits", Integer.toString(finalResources.retainedPermits()));
			values.put("final-retained-waiters", Integer.toString(finalResources.retainedWaiters()));
			values.put("final-iterator-leases", Integer.toString(finalResources.iteratorLeases()));
			values.put("final-exists-multi-requests", Integer.toString(finalResources.existsMultiRequests()));
			values.put("final-exists-multi-snapshots", Integer.toString(finalResources.existsMultiSnapshots()));
			values.put("final-exists-multi-read-options", Integer.toString(finalResources.existsMultiReadOptions()));
			values.put("final-exists-multi-arenas", Integer.toString(finalResources.existsMultiArenas()));
			values.put("native-leaks", Long.toString(nativeLeaks));
			values.put("correctness", Boolean.toString(correctness));
			values.put("resources-drained", Boolean.toString(resourcesDrained));
			values.put("accounting-valid", Boolean.toString(accountingValid));
			values.put("configured-retained-limit", Integer.toString(configuredRetainedLimit));
			return Map.copyOf(values);
		}

		private static WorkerResult read(Path input, ExpectedProvenance expected) throws IOException {
			return read(Files.readString(input), expected);
		}

		private static WorkerResult read(String artifact, ExpectedProvenance expected) {
			Map<String, String> values = strictProperties(artifact, WORKER_KEYS);
			requireEqual(values, "schema", WORKER_SCHEMA);
			requireEqual(values, "implementation", expected.implementation().value);
			requireEqual(values, "scenario", expected.scenario().value);
			requireEqual(values, "round", Integer.toString(expected.round()));
			requireEqual(values, "build-sha", expected.buildSha());
			requireEqual(values, "classpath-sha256", expected.classPathSha256());
			requireEqual(values, "dataset-id", expected.datasetId());
			WorkerResult result = new WorkerResult(Implementation.parse(values.get("implementation")),
					Scenario.parse(values.get("scenario")), integer(values, "round"), values.get("build-sha"),
					values.get("classpath-sha256"), values.get("arena-instrumentation-sha256"),
					values.get("dataset-id"),
					number(values, "cold-completion-nanos"), number(values, "cold-first-item-nanos"),
					number(values, "elapsed-nanos"), number(values, "operations"), number(values, "items"),
					number(values, "logical-bytes"), unsignedNumber(values, "checksum"),
					decimal(values, "entries-per-second"), decimal(values, "mib-per-second"),
					number(values, "completion-p99-nanos"), number(values, "first-item-p99-nanos"),
					number(values, "foreground-p99-nanos"), number(values, "foreground-operations"),
					decimal(values, "cpu-nanos-per-item"), decimal(values, "allocated-bytes-per-item"),
					number(values, "gc-collections"), number(values, "gc-millis"),
					number(values, "peak-live-heap-bytes"), number(values, "peak-direct-memory-bytes"),
					number(values, "peak-resident-set-bytes"), integer(values, "peak-thread-count"),
					number(values, "peak-native-handles"),
					new MetricSnapshot(number(values, "scheduler-accepted"),
							number(values, "scheduler-started"), number(values, "scheduler-terminal"),
							number(values, "scheduler-execution-terminal"),
							number(values, "scheduler-quantums"), number(values, "scheduler-failures"),
							number(values, "scheduler-rejections"), number(values, "scheduler-cancellations"),
							number(values, "scheduler-queue-p99-nanos"),
							number(values, "scheduler-execution-p99-nanos")),
					new ResourcePeaks(integer(values, "peak-queued"), integer(values, "peak-active"),
							integer(values, "peak-parked"), integer(values, "peak-outstanding"),
							number(values, "peak-pending"), integer(values, "peak-iterators"),
							integer(values, "peak-range-cursors"), integer(values, "peak-retained-snapshots"),
							integer(values, "peak-retained-permits"), integer(values, "peak-retained-waiters"),
							integer(values, "peak-iterator-leases"), integer(values, "peak-exists-multi-requests"),
							integer(values, "peak-exists-multi-snapshots"),
							integer(values, "peak-exists-multi-read-options"),
							integer(values, "peak-exists-multi-arenas")),
					new ResourceSnapshot(integer(values, "final-queued"), integer(values, "final-active"),
							integer(values, "final-parked"), integer(values, "final-outstanding"),
							number(values, "submission-attempts"), number(values, "terminal-outcomes"),
							bool(values, "scheduler-accounting-exact"), number(values, "final-pending"),
							integer(values, "final-transactions"),
							integer(values, "final-iterators"), integer(values, "final-range-cursors"),
							integer(values, "final-retained-snapshots"),
							integer(values, "final-retained-permits"), integer(values, "final-retained-waiters"),
							integer(values, "final-iterator-leases"), integer(values, "final-exists-multi-requests"),
							integer(values, "final-exists-multi-snapshots"),
							integer(values, "final-exists-multi-read-options"),
							integer(values, "final-exists-multi-arenas")), number(values, "native-leaks"),
					bool(values, "correctness"), bool(values, "resources-drained"),
					bool(values, "accounting-valid"), integer(values, "configured-retained-limit"));
			result.validateMetrics();
			return result;
		}

		private void validateMetrics() {
			if (round < 1 || coldCompletionNanos <= 0L || elapsedNanos <= 0L || operations <= 0L
					|| items <= 0L || logicalBytes <= 0L || completionP99Nanos <= 0L
					|| !positiveFinite(entriesPerSecond) || !positiveFinite(mibPerSecond)
					|| !positiveFinite(cpuNanosPerItem) || !positiveFinite(allocatedBytesPerItem)
					|| peakLiveHeapBytes <= 0L || peakDirectMemoryBytes < 0L
					|| peakResidentSetBytes <= 0L || peakThreadCount <= 0 || peakNativeHandles <= 0L
					|| gcCollections < 0L || gcMillis < 0L || nativeLeaks < 0L
					|| configuredRetainedLimit <= 0
					|| !arenaInstrumentationSha256.matches("[0-9a-f]{64}")) {
				throw new IllegalArgumentException("Worker artifact contains missing, non-positive, or invalid metrics");
			}
			if (scenario.hasFirstItem() != (coldFirstItemNanos > 0L && firstItemP99Nanos > 0L)) {
				throw new IllegalArgumentException("First-item metrics do not match scenario applicability");
			}
			if (scenario.mixed != (foregroundP99Nanos > 0L && foregroundOperations > 0L)) {
				throw new IllegalArgumentException("Foreground metrics do not match scenario applicability");
			}
			if (scheduler.accepted() < 0L || scheduler.started() < 0L || scheduler.terminal() < 0L
					|| scheduler.executionTerminal() < 0L || scheduler.quantums() < 0L
					|| scheduler.failures() < 0L || scheduler.rejections() < 0L
					|| scheduler.cancellations() < 0L || scheduler.queueP99Nanos() <= 0L
					|| scheduler.executionP99Nanos() <= 0L || !nonNegative(resourcePeaks)
					|| !nonNegative(finalResources)) {
				throw new IllegalArgumentException("Worker artifact contains negative counters");
			}
		}

		private List<String> structuralFailures() {
			List<String> failures = new ArrayList<>();
			if (!correctness) failures.add("correctness validation failed");
			if (!resourcesDrained || !finalResources.drained()) failures.add("resources did not drain");
			if (nativeLeaks != 0L) failures.add("native leak count=" + nativeLeaks);
			if (!accountingValid) failures.add("logical scheduler accounting is invalid");
			if (implementation == Implementation.CANDIDATE && !finalResources.exactSchedulerAccounting()) {
				failures.add("candidate did not expose exact scheduler accounting");
			}
			if (resourcePeaks.retainedPermits() > configuredRetainedLimit
					|| resourcePeaks.retainedSnapshots() > configuredRetainedLimit) {
				failures.add("configured retained-resource limit was exceeded");
			}
			if (resourcePeaks.existsMultiSnapshots() > resourcePeaks.existsMultiRequests()
					|| resourcePeaks.existsMultiReadOptions() > resourcePeaks.existsMultiRequests()
					|| resourcePeaks.existsMultiArenas() > resourcePeaks.existsMultiRequests()) {
				failures.add("existsMulti native-resource peak exceeded its logical-request peak");
			}
			if (scenario.operation == Operation.EXISTS_MULTI
					&& (resourcePeaks.existsMultiRequests() <= 0
							|| resourcePeaks.existsMultiSnapshots() <= 0
							|| resourcePeaks.existsMultiReadOptions() <= 0
							|| resourcePeaks.existsMultiArenas() <= 0)) {
				failures.add("existsMulti exact native-resource peaks were not observed");
			}
			return List.copyOf(failures);
		}

		private boolean structurallyPassed() {
			return structuralFailures().isEmpty();
		}
	}

	private static boolean positiveFinite(double value) {
		return Double.isFinite(value) && value > 0.0d;
	}

	private static boolean nonNegative(ResourcePeaks value) {
		return value.queued() >= 0 && value.active() >= 0 && value.parked() >= 0
				&& value.outstanding() >= 0 && value.pending() >= 0L
				&& value.iterators() >= 0 && value.rangeCursors() >= 0 && value.retainedSnapshots() >= 0
				&& value.retainedPermits() >= 0 && value.retainedWaiters() >= 0 && value.iteratorLeases() >= 0
				&& value.existsMultiRequests() >= 0 && value.existsMultiSnapshots() >= 0
				&& value.existsMultiReadOptions() >= 0 && value.existsMultiArenas() >= 0;
	}

	private static boolean nonNegative(ResourceSnapshot value) {
		return value.queued() >= 0 && value.active() >= 0 && value.parked() >= 0
				&& value.outstanding() >= 0 && value.submissionAttempts() >= 0L
				&& value.terminalOutcomes() >= 0L && value.pending() >= 0L
				&& value.transactions() >= 0 && value.iterators() >= 0 && value.rangeCursors() >= 0
				&& value.retainedSnapshots() >= 0 && value.retainedPermits() >= 0
				&& value.retainedWaiters() >= 0 && value.iteratorLeases() >= 0
				&& value.existsMultiRequests() >= 0 && value.existsMultiSnapshots() >= 0
				&& value.existsMultiReadOptions() >= 0 && value.existsMultiArenas() >= 0;
	}

	private static Comparison compare(List<WorkerResult> results) {
		Map<Scenario, ScenarioComparison> scenarios = new EnumMap<>(Scenario.class);
		List<String> failures = new ArrayList<>();
		for (Implementation implementation : Implementation.values()) {
			var instrumentationHashes = results.stream()
					.filter(result -> result.implementation() == implementation)
					.map(WorkerResult::arenaInstrumentationSha256)
					.collect(Collectors.toSet());
			if (instrumentationHashes.size() != 1) {
				failures.add(implementation.value
						+ ": arena instrumentation provenance changed across fixed runs");
			}
		}
		for (Scenario scenario : Scenario.values()) {
			List<WorkerResult> baseline = resultsFor(results, scenario, Implementation.BASELINE);
			List<WorkerResult> candidate = resultsFor(results, scenario, Implementation.CANDIDATE);
			if (baseline.size() != FIXED_PAIRED_ROUNDS || candidate.size() != FIXED_PAIRED_ROUNDS) {
				failures.add(scenario.value + ": expected exactly " + FIXED_PAIRED_ROUNDS
						+ " baseline/candidate pairs");
				continue;
			}
			List<String> structural = new ArrayList<>();
			for (int index = 0; index < FIXED_PAIRED_ROUNDS; index++) {
				WorkerResult base = baseline.get(index);
				WorkerResult next = candidate.get(index);
				int expectedRound = index + 1;
				if (base.round() != expectedRound || next.round() != expectedRound) {
					structural.add("round pairing mismatch at " + expectedRound);
				}
				for (String failure : base.structuralFailures()) {
					structural.add("baseline round " + expectedRound + ": " + failure);
				}
				for (String failure : next.structuralFailures()) {
					structural.add("candidate round " + expectedRound + ": " + failure);
				}
				if (next.configuredRetainedLimit() != base.configuredRetainedLimit()) {
					structural.add("configured retained-resource limit changed in round " + expectedRound);
				}
			}
			Map<String, MetricSamples> samples = metricSamples(scenario, baseline, candidate);
			GateResult gate = evaluateGate(scenario, samples, structural, false);
			scenarios.put(scenario, new ScenarioComparison(gate, gcSummary(baseline, candidate)));
			for (String failure : gate.failures()) failures.add(scenario.value + ": " + failure);
		}
		if (scenarios.values().stream()
				.flatMap(result -> result.gate().materialImprovements().stream())
				.findAny().isEmpty()) {
			failures.add("no predeclared retained-read primary metric demonstrates a material improvement");
		}
		return new Comparison(Map.copyOf(scenarios), List.copyOf(failures));
	}

	private static List<WorkerResult> resultsFor(List<WorkerResult> results,
	                                             Scenario scenario,
	                                             Implementation implementation) {
		return results.stream()
				.filter(result -> result.scenario() == scenario && result.implementation() == implementation)
				.sorted(Comparator.comparingInt(WorkerResult::round))
				.toList();
	}

	private static String gcSummary(List<WorkerResult> baseline, List<WorkerResult> candidate) {
		long baselineCollections = baseline.stream().mapToLong(WorkerResult::gcCollections).sum();
		long candidateCollections = candidate.stream().mapToLong(WorkerResult::gcCollections).sum();
		long baselineMillis = baseline.stream().mapToLong(WorkerResult::gcMillis).sum();
		long candidateMillis = candidate.stream().mapToLong(WorkerResult::gcMillis).sum();
		return "collections baseline=" + baselineCollections + " candidate=" + candidateCollections
				+ ", millis baseline=" + baselineMillis + " candidate=" + candidateMillis;
	}

	private static Map<String, MetricSamples> metricSamples(Scenario scenario,
	                                                        List<WorkerResult> baseline,
	                                                        List<WorkerResult> candidate) {
		Map<String, MetricSamples> metrics = new LinkedHashMap<>();
		putMetric(metrics, "entries-per-second", baseline, candidate, WorkerResult::entriesPerSecond);
		putMetric(metrics, "mib-per-second", baseline, candidate, WorkerResult::mibPerSecond);
		putMetric(metrics, "completion-p99", baseline, candidate, WorkerResult::completionP99Nanos);
		putMetric(metrics, "cold-completion", baseline, candidate, WorkerResult::coldCompletionNanos);
		putMetric(metrics, "cpu-nanos-per-item", baseline, candidate, WorkerResult::cpuNanosPerItem);
		putMetric(metrics, "allocated-bytes-per-item", baseline, candidate,
				WorkerResult::allocatedBytesPerItem);
		putMetric(metrics, "peak-live-heap", baseline, candidate, WorkerResult::peakLiveHeapBytes);
		putMetric(metrics, "peak-direct-memory", baseline, candidate,
				WorkerResult::peakDirectMemoryBytes);
		putMetric(metrics, "peak-resident-set", baseline, candidate,
				WorkerResult::peakResidentSetBytes);
		putMetric(metrics, "peak-thread-count", baseline, candidate, WorkerResult::peakThreadCount);
		putMetric(metrics, "peak-native-handles", baseline, candidate, WorkerResult::peakNativeHandles);
		putMetric(metrics, "gc-collections", baseline, candidate, WorkerResult::gcCollections);
		putMetric(metrics, "gc-millis", baseline, candidate, WorkerResult::gcMillis);
		putMetric(metrics, "peak-parked", baseline, candidate,
				result -> result.resourcePeaks().parked());
		putMetric(metrics, "peak-outstanding", baseline, candidate,
				result -> result.resourcePeaks().outstanding());
		putMetric(metrics, "peak-retained-snapshots", baseline, candidate,
				result -> result.resourcePeaks().retainedSnapshots());
		putMetric(metrics, "peak-retained-permits", baseline, candidate,
				result -> result.resourcePeaks().retainedPermits());
		putMetric(metrics, "peak-retained-waiters", baseline, candidate,
				result -> result.resourcePeaks().retainedWaiters());
		putMetric(metrics, "peak-range-cursors", baseline, candidate,
				result -> result.resourcePeaks().rangeCursors());
		putMetric(metrics, "peak-iterators", baseline, candidate,
				result -> result.resourcePeaks().iterators());
		putMetric(metrics, "peak-iterator-leases", baseline, candidate,
				result -> result.resourcePeaks().iteratorLeases());
		putMetric(metrics, "peak-exists-multi-requests", baseline, candidate,
				result -> result.resourcePeaks().existsMultiRequests());
		putMetric(metrics, "peak-exists-multi-snapshots", baseline, candidate,
				result -> result.resourcePeaks().existsMultiSnapshots());
		putMetric(metrics, "peak-exists-multi-read-options", baseline, candidate,
				result -> result.resourcePeaks().existsMultiReadOptions());
		putMetric(metrics, "peak-exists-multi-arenas", baseline, candidate,
				result -> result.resourcePeaks().existsMultiArenas());
		putMetric(metrics, "queue-p99", baseline, candidate,
				result -> result.scheduler().queueP99Nanos());
		putMetric(metrics, "execution-p99", baseline, candidate,
				result -> result.scheduler().executionP99Nanos());
		if (scenario.hasFirstItem()) {
			putMetric(metrics, "first-item-p99", baseline, candidate, WorkerResult::firstItemP99Nanos);
			putMetric(metrics, "cold-first-item", baseline, candidate, WorkerResult::coldFirstItemNanos);
		}
		if (scenario.mixed) {
			putMetric(metrics, "foreground-p99", baseline, candidate, WorkerResult::foregroundP99Nanos);
		}
		return Map.copyOf(metrics);
	}

	private static void putMetric(Map<String, MetricSamples> metrics,
	                              String name,
	                              List<WorkerResult> baseline,
	                              List<WorkerResult> candidate,
	                              ToDoubleFunction<WorkerResult> extractor) {
		metrics.put(name, new MetricSamples(
				baseline.stream().mapToDouble(extractor).toArray(),
				candidate.stream().mapToDouble(extractor).toArray()));
	}

	private static GateResult evaluateGate(Scenario scenario,
	                                       Map<String, MetricSamples> samples,
	                                       List<String> structuralFailures,
	                                       boolean requireMaterialImprovement) {
		Map<String, PairedPerformanceContract.MetricSamples> contractSamples = new LinkedHashMap<>();
		for (Map.Entry<String, MetricSamples> entry : samples.entrySet()) {
			contractSamples.put(entry.getKey(), new PairedPerformanceContract.MetricSamples(
					entry.getValue().baseline(), entry.getValue().candidate()));
		}
		List<MetricSpec> specifications = MetricSpec.forScenario(scenario);
		PairedPerformanceContract.Evaluation evaluation = PairedPerformanceContract.evaluate(
				specifications.stream().map(MetricSpec::contract).toList(), contractSamples,
				structuralFailures, requireMaterialImprovement);
		Map<String, PairedBenchmarkStatistics.RatioConfidenceInterval> intervals = new LinkedHashMap<>();
		evaluation.metrics().forEach((name, result) -> intervals.put(name, result.interval()));
		return new GateResult(intervals, evaluation.failures(), evaluation.materialImprovements(),
				evaluation.exceptionCandidates());
	}

	/**
	 * Pure strict-gate helper used by deterministic parser and rejection tests.
	 */
	public static GateResult evaluateForTesting(String scenario,
	                                            Map<String, MetricSamples> samples,
	                                            List<String> structuralFailures) {
		return evaluateGate(Scenario.parse(scenario), Map.copyOf(samples),
				List.copyOf(structuralFailures), true);
	}

	/**
	 * Returns a complete ten-pair equality fixture for the named scenario.
	 */
	public static Map<String, MetricSamples> passingMetricSamplesForTesting(String scenario) {
		Scenario parsed = Scenario.parse(scenario);
		double[] baseline = new double[FIXED_PAIRED_ROUNDS];
		double[] candidate = new double[FIXED_PAIRED_ROUNDS];
		Arrays.fill(baseline, 100.0d);
		Arrays.fill(candidate, 100.0d);
		Map<String, MetricSamples> values = new LinkedHashMap<>();
		for (MetricSpec spec : MetricSpec.forScenario(parsed)) {
			values.put(spec.name(), new MetricSamples(baseline, candidate));
		}
		MetricSpec primary = MetricSpec.forScenario(parsed).stream()
				.filter(spec -> spec.contract().primary())
				.findFirst().orElseThrow();
		double[] improved = candidate.clone();
		Arrays.fill(improved, primary.contract().direction()
				== PairedPerformanceContract.Direction.HIGHER_IS_BETTER ? 103.0d : 97.0d);
		values.put(primary.name(), new MetricSamples(baseline, improved));
		return Map.copyOf(values);
	}

	/**
	 * Valid worker artifact fixture for strict parser tests.
	 */
	public static String validWorkerArtifactForTesting() {
		String sha = "0123456789abcdef0123456789abcdef01234567";
		String digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
		WorkerResult result = new WorkerResult(Implementation.CANDIDATE, Scenario.EXACT_COUNT, 1,
				sha, digest, digest, digest, 100L, 0L, 1_000L, 2L, 200L, 20_000L, 1L,
				200_000_000.0d, 20_000.0d, 100L, 0L, 0L, 0L, 5.0d, 10.0d, 0L, 0L,
				1_000_000L, 1_000L, 2_000_000L, 20, 50L,
				new MetricSnapshot(2L, 2L, 2L, 2L, 2L, 0L, 0L, 0L, 10L, 20L),
				new ResourcePeaks(1, 1, 0, 1, 1L, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0),
				new ResourceSnapshot(0, 0, 0, 0, 2L, 2L, true,
						0L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
				0L, true, true, true, 1);
		StringBuilder artifact = new StringBuilder();
		result.toProperties().forEach((key, value) -> artifact.append(key).append('=').append(value).append('\n'));
		return artifact.toString();
	}

	/** Content-bound fingerprint helper for deterministic provenance tests. */
	public static String classPathContentSha256ForTesting(String classPath) throws IOException {
		return classPathContentSha256(normalizedClassPath(classPath));
	}

	/** Exact bytecode-substitution smoke proof used by the parser/gate test suite. */
	public static String exactArenaInstrumentationForTesting() throws Exception {
		var transformation = ExactArenaTracking.transform(ArenaTrackingFixture.class, "exercise");
		ExactArenaTracking.resetPeakAfterDrain();
		try (var arena = openTrackedArena()) {
			arena.allocate(1L);
		}
		ExactArenaTracking.assertDrained();
		if (ExactArenaTracking.peakArenas() != 1) {
			throw new IllegalStateException("Exact Arena instrumentation did not observe one live Arena");
		}
		return ExactArenaTracking.fingerprint(transformation.bytes());
	}

	/** Frozen-baseline provenance check used by deterministic gate tests. */
	public static void validateRawBaselineForTesting(String buildBaseline) {
		requireRawBaseline(buildBaseline);
	}

	private static final class ArenaTrackingFixture {

		private static void exercise() {
			try (var arena = Arena.ofConfined()) {
				arena.allocate(1L);
			}
		}
	}

	/**
	 * Strict parser/provenance helper used by deterministic tests.
	 */
	public static WorkerArtifactSummary parseWorkerForTesting(String artifact,
	                                                          String expectedBuildSha,
	                                                          String expectedClassPathSha256,
	                                                          String expectedDatasetId) {
		WorkerResult result = WorkerResult.read(artifact, new ExpectedProvenance(
				Implementation.CANDIDATE, Scenario.EXACT_COUNT, 1, expectedBuildSha,
				expectedClassPathSha256, expectedDatasetId));
		return new WorkerArtifactSummary(result.structurallyPassed(), result.structuralFailures());
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

	public record GateResult(
			Map<String, PairedBenchmarkStatistics.RatioConfidenceInterval> intervals,
			List<String> failures,
			List<String> materialImprovements,
			List<String> exceptionCandidates) {

		public GateResult {
			intervals = Map.copyOf(intervals);
			failures = List.copyOf(failures);
			materialImprovements = List.copyOf(materialImprovements);
			exceptionCandidates = List.copyOf(exceptionCandidates);
		}

		public boolean passed() {
			return failures.isEmpty();
		}
	}

	public record WorkerArtifactSummary(boolean passed, List<String> failures) {

		public WorkerArtifactSummary {
			failures = List.copyOf(failures);
		}
	}

	private record MetricSpec(PairedPerformanceContract.MetricSpec contract) {

		private String name() {
			return contract.name();
		}

		private static List<MetricSpec> forScenario(Scenario scenario) {
			List<MetricSpec> specs = new ArrayList<>(List.of(
					new MetricSpec(PairedPerformanceContract.MetricSpec.throughput("entries-per-second", true)),
					new MetricSpec(PairedPerformanceContract.MetricSpec.throughput("mib-per-second", true)),
					new MetricSpec(PairedPerformanceContract.MetricSpec.cost("queue-p99", false)),
					new MetricSpec(PairedPerformanceContract.MetricSpec.cost("execution-p99", false)),
					new MetricSpec(PairedPerformanceContract.MetricSpec.cost("completion-p99", true)),
					new MetricSpec(PairedPerformanceContract.MetricSpec.cost("cold-completion", true)),
					new MetricSpec(PairedPerformanceContract.MetricSpec.cost("cpu-nanos-per-item", true)),
					new MetricSpec(PairedPerformanceContract.MetricSpec.allocation(
							"allocated-bytes-per-item", true)),
					new MetricSpec(PairedPerformanceContract.MetricSpec.cost("peak-live-heap", true)),
					new MetricSpec(PairedPerformanceContract.MetricSpec.cost("peak-direct-memory", true)),
					new MetricSpec(PairedPerformanceContract.MetricSpec.cost("peak-resident-set", true)),
					new MetricSpec(PairedPerformanceContract.MetricSpec.noIncrease("peak-thread-count")),
					new MetricSpec(PairedPerformanceContract.MetricSpec.noIncrease("peak-native-handles")),
					new MetricSpec(PairedPerformanceContract.MetricSpec.noIncrease("gc-collections")),
					new MetricSpec(PairedPerformanceContract.MetricSpec.noIncrease("gc-millis")),
					new MetricSpec(PairedPerformanceContract.MetricSpec.noIncrease("peak-parked")),
					new MetricSpec(PairedPerformanceContract.MetricSpec.noIncrease("peak-outstanding")),
					new MetricSpec(PairedPerformanceContract.MetricSpec.noIncrease("peak-retained-snapshots")),
					new MetricSpec(PairedPerformanceContract.MetricSpec.noIncrease("peak-retained-permits")),
					new MetricSpec(PairedPerformanceContract.MetricSpec.noIncrease("peak-retained-waiters")),
					new MetricSpec(PairedPerformanceContract.MetricSpec.noIncrease("peak-range-cursors")),
					new MetricSpec(PairedPerformanceContract.MetricSpec.noIncrease("peak-iterators")),
					new MetricSpec(PairedPerformanceContract.MetricSpec.noIncrease("peak-iterator-leases")),
					new MetricSpec(PairedPerformanceContract.MetricSpec.noIncrease("peak-exists-multi-requests")),
					new MetricSpec(PairedPerformanceContract.MetricSpec.noIncrease("peak-exists-multi-snapshots")),
					new MetricSpec(PairedPerformanceContract.MetricSpec.noIncrease("peak-exists-multi-read-options")),
					new MetricSpec(PairedPerformanceContract.MetricSpec.noIncrease("peak-exists-multi-arenas"))));
			if (scenario.hasFirstItem()) {
				specs.add(new MetricSpec(PairedPerformanceContract.MetricSpec.cost("first-item-p99", true)));
				specs.add(new MetricSpec(PairedPerformanceContract.MetricSpec.cost("cold-first-item", true)));
			}
			if (scenario.mixed) {
				specs.add(new MetricSpec(PairedPerformanceContract.MetricSpec.cost("foreground-p99", true)));
			}
			return List.copyOf(specs);
		}
	}

	private record ScenarioComparison(GateResult gate, String gcSummary) {
	}

	private record Comparison(Map<Scenario, ScenarioComparison> scenarios, List<String> failures) {

		private Comparison {
			scenarios = Map.copyOf(scenarios);
			failures = List.copyOf(failures);
		}

		private boolean passed() {
			return failures.isEmpty();
		}

		private String failedSummary() {
			return failures.isEmpty() ? "none" : String.join("; ", failures);
		}
	}

	private static void writeSchedule(Path output,
	                                  List<ScheduledRun> schedule,
	                                  Options options,
	                                  String baselineClassPathHash,
	                                  String candidateClassPathHash,
	                                  String datasetId) throws IOException {
		StringBuilder text = new StringBuilder("schema\t").append(SCHEDULE_SCHEMA).append('\n')
				.append("build-baseline\t").append(options.buildBaseline()).append('\n')
				.append("build-candidate\t").append(options.buildCandidate()).append('\n')
				.append("classpath-baseline-sha256\t").append(baselineClassPathHash).append('\n')
				.append("classpath-candidate-sha256\t").append(candidateClassPathHash).append('\n')
				.append("dataset-id\t").append(datasetId).append('\n')
				.append("fixed-paired-rounds\t").append(FIXED_PAIRED_ROUNDS).append('\n')
				.append("ordinal\tround\tscenario\timplementation\n");
		for (int index = 0; index < schedule.size(); index++) {
			ScheduledRun run = schedule.get(index);
			text.append(index + 1).append('\t').append(run.round()).append('\t')
					.append(run.scenario().value).append('\t').append(run.implementation().value).append('\n');
		}
		Files.writeString(output, text, StandardOpenOption.CREATE_NEW);
	}

	private static void writeControllerMetadata(Path root,
	                                            Options options,
	                                            FileStore store,
	                                            Instant started,
	                                            String baselineClassPath,
	                                            String candidateClassPath,
	                                            String baselineClassPathHash,
	                                            String candidateClassPathHash,
	                                            String datasetId) throws IOException {
		var os = ManagementFactory.getOperatingSystemMXBean();
		long physicalMemory = os instanceof OperatingSystemMXBean extended
				? extended.getTotalMemorySize() : -1L;
		String metadata = "schema=" + RESULT_SCHEMA + '\n'
				+ "started=" + started + '\n'
				+ "build-baseline=" + options.buildBaseline() + '\n'
				+ "build-candidate=" + options.buildCandidate() + '\n'
				+ "build-state-baseline=" + options.buildStateBaseline() + '\n'
				+ "build-state-candidate=" + options.buildStateCandidate() + '\n'
				+ "baseline-classpath-sha256=" + baselineClassPathHash + '\n'
				+ "candidate-classpath-sha256=" + candidateClassPathHash + '\n'
				+ "baseline-classpath=" + escapeProperty(baselineClassPath) + '\n'
				+ "candidate-classpath=" + escapeProperty(candidateClassPath) + '\n'
				+ "dataset-id=" + datasetId + '\n'
				+ "storage-label=" + options.storageLabel() + '\n'
				+ "storage-name=" + escapeProperty(store.name()) + '\n'
				+ "storage-type=" + escapeProperty(store.type()) + '\n'
				+ "host-state=" + options.hostState() + '\n'
				+ "cache-state=" + options.cacheState() + '\n'
				+ "os-name=" + escapeProperty(os.getName()) + '\n'
				+ "os-version=" + escapeProperty(os.getVersion()) + '\n'
				+ "os-arch=" + escapeProperty(os.getArch()) + '\n'
				+ "processors=" + os.getAvailableProcessors() + '\n'
				+ "cpu-model=" + escapeProperty(cpuModel()) + '\n'
				+ "physical-memory-bytes=" + physicalMemory + '\n'
				+ "jvm-vendor=" + escapeProperty(System.getProperty("java.vendor")) + '\n'
				+ "jvm-version=" + escapeProperty(System.getProperty("java.runtime.version")) + '\n'
				+ "java-home=" + escapeProperty(System.getProperty("java.home")) + '\n'
				+ "jvm-arguments=" + escapeProperty(ManagementFactory.getRuntimeMXBean()
				.getInputArguments().stream().collect(Collectors.joining(" "))) + '\n'
				+ "rocksdb-version=" + escapeProperty(String.valueOf(RocksDB.rocksdbVersion())) + '\n'
				+ "command=" + escapeProperty(System.getProperty("sun.java.command", "unknown")) + '\n'
				+ "preload-keys=" + options.preloadKeys() + '\n'
				+ "flush-keys=" + options.flushKeys() + '\n'
				+ "batch-entries=" + options.batchEntries() + '\n'
				+ "value-bytes=" + options.valueBytes() + '\n'
				+ "exists-items=" + options.existsItems() + '\n'
				+ "iterator-skip=" + options.iteratorSkip() + '\n'
				+ "iterator-take=" + options.iteratorTake() + '\n'
				+ "read-parallelism=" + options.readParallelism() + '\n'
				+ "write-parallelism=" + options.writeParallelism() + '\n'
				+ "foreground-workers=" + options.foregroundWorkers() + '\n'
				+ "warmup-operations=" + options.warmupOperations() + '\n'
				+ "measure-seconds=" + options.measureSeconds() + '\n'
				+ "sample-micros=" + options.sampleMicros() + '\n'
				+ "configured-retained-limit=" + options.configuredRetainedLimit() + '\n'
				+ "fixed-paired-rounds=" + FIXED_PAIRED_ROUNDS + '\n'
				+ "adaptive-stopping=false\n";
		Files.writeString(root.resolve("metadata.properties"), metadata, StandardOpenOption.CREATE_NEW);
	}

	private static void writeReports(Path root,
	                                 Options options,
	                                 FileStore store,
	                                 Instant started,
	                                 Instant finished,
	                                 List<WorkerResult> results,
	                                 Comparison comparison,
	                                 String baselineClassPathHash,
	                                 String candidateClassPathHash,
	                                 String datasetId) throws IOException {
		Files.writeString(root.resolve("results.md"), toMarkdown(options, store, started, finished,
						results, comparison, baselineClassPathHash, candidateClassPathHash, datasetId),
				StandardOpenOption.CREATE_NEW);
		Files.writeString(root.resolve("results.json"), toJson(options, store, started, finished,
						results, comparison, baselineClassPathHash, candidateClassPathHash, datasetId),
				StandardOpenOption.CREATE_NEW);
	}

	private static String toMarkdown(Options options,
	                                 FileStore store,
	                                 Instant started,
	                                 Instant finished,
	                                 List<WorkerResult> results,
	                                 Comparison comparison,
	                                 String baselineClassPathHash,
	                                 String candidateClassPathHash,
	                                 String datasetId) {
		StringBuilder out = new StringBuilder("# Paired gRPC retained-read comparison\n\n")
				.append("- Result: **").append(comparison.passed() ? "PASS" : "FAIL").append("**\n")
				.append("- Started / finished: `").append(started).append("` / `").append(finished).append("`\n")
				.append("- Baseline / candidate: `").append(options.buildBaseline()).append("` / `")
				.append(options.buildCandidate()).append("`\n")
				.append("- Classpath SHA-256: `").append(baselineClassPathHash).append("` / `")
				.append(candidateClassPathHash).append("`\n")
				.append("- Dataset: `").append(datasetId).append("`; storage `")
				.append(options.storageLabel()).append("` (`").append(store.name()).append("`, `")
				.append(store.type()).append("`)\n")
				.append("- Host/cache state: `").append(options.hostState()).append("` / `")
				.append(options.cacheState()).append("`\n")
				.append("- Schedule: exactly ").append(FIXED_PAIRED_ROUNDS)
				.append(" predetermined pairs, alternating order, no adaptive stopping\n\n")
				.append("|Scenario|Metric|Geometric ratio|95% confidence interval|Gate|\n")
				.append("|---|---|---:|---:|---|\n");
		for (Scenario scenario : Scenario.values()) {
			ScenarioComparison scenarioResult = comparison.scenarios().get(scenario);
			if (scenarioResult == null) continue;
			for (MetricSpec spec : MetricSpec.forScenario(scenario)) {
				var interval = scenarioResult.gate().intervals().get(spec.name());
				if (interval == null) {
					out.append('|').append(scenario.value).append('|').append(spec.name())
							.append("|n/a|n/a|FAIL|\n");
					continue;
				}
				boolean passed = scenarioResult.gate().failures().stream()
						.noneMatch(failure -> failure.startsWith(spec.name()));
				out.append('|').append(scenario.value).append('|').append(spec.name()).append('|')
						.append(interval.available() ? format(interval.mean()) : "n/a").append('|')
						.append(interval.available() ? "[" + format(interval.lower95()) + ", "
								+ format(interval.upper95()) + "]" : "exact no-increase").append('|')
						.append(passed ? "PASS" : "FAIL").append("|\n");
			}
			out.append("\nMaterial improvements: `")
					.append(String.join(", ", scenarioResult.gate().materialImprovements()))
					.append("`; exception candidates (still FAIL, approval required): `")
					.append(String.join(", ", scenarioResult.gate().exceptionCandidates())).append("`.\n");
			out.append("\nGC: ").append(scenarioResult.gcSummary()).append(".\n\n");
		}
		out.append("Worker artifacts: ").append(results.size()).append(". All workers must validate exact ")
				.append("counts/order/hit-miss/checksums, logical scheduler accounting, retained limits, ")
				.append("parked/outstanding bounds, attempt conservation, terminal drains, and native leak count.\n");
		out.append("\n## Worker accounting and resources\n\n")
				.append("|Round|Scenario|Build|accepted/started/terminal/execution|quantums|")
				.append("parked/outstanding|attempts/outcomes/exact|")
				.append("range snap/permits/waiters/cursors/iterators/leases|")
				.append("exists req/snap/read-options/arenas|final drain|native leaks|gate|\n")
				.append("|---:|---|---|---:|---:|---:|---:|---:|---:|---|---:|---|\n");
		for (WorkerResult result : results) {
			MetricSnapshot scheduler = result.scheduler();
			ResourcePeaks peaks = result.resourcePeaks();
			out.append('|').append(result.round()).append('|').append(result.scenario().value)
					.append('|').append(result.implementation().value).append('|')
					.append(scheduler.accepted()).append('/').append(scheduler.started()).append('/')
					.append(scheduler.terminal()).append('/').append(scheduler.executionTerminal()).append('|')
					.append(scheduler.quantums()).append('|').append(peaks.parked()).append('/')
					.append(peaks.outstanding()).append('|')
					.append(result.finalResources().submissionAttempts()).append('/')
					.append(result.finalResources().terminalOutcomes()).append('/')
					.append(result.finalResources().exactSchedulerAccounting()).append('|')
					.append(peaks.retainedSnapshots()).append('/')
					.append(peaks.retainedPermits()).append('/').append(peaks.retainedWaiters()).append('/')
					.append(peaks.rangeCursors()).append('/').append(peaks.iterators()).append('/')
					.append(peaks.iteratorLeases()).append('|').append(peaks.existsMultiRequests()).append('/')
					.append(peaks.existsMultiSnapshots()).append('/').append(peaks.existsMultiReadOptions()).append('/')
					.append(peaks.existsMultiArenas()).append('|').append(result.finalResources().drained())
					.append('|').append(result.nativeLeaks()).append('|')
					.append(result.structurallyPassed() ? "PASS" : "FAIL").append("|\n");
		}
		if (!comparison.failures().isEmpty()) {
			out.append("\n## Failures\n\n");
			for (String failure : comparison.failures()) out.append("- ").append(failure).append('\n');
		}
		return out.toString();
	}

	private static String toJson(Options options,
	                             FileStore store,
	                             Instant started,
	                             Instant finished,
	                             List<WorkerResult> results,
	                             Comparison comparison,
	                             String baselineClassPathHash,
	                             String candidateClassPathHash,
	                             String datasetId) {
		StringBuilder out = new StringBuilder("{\n")
				.append("  \"schema\": \"").append(RESULT_SCHEMA).append("\",\n")
				.append("  \"started\": \"").append(started).append("\",\n")
				.append("  \"finished\": \"").append(finished).append("\",\n")
				.append("  \"build_baseline\": \"").append(json(options.buildBaseline())).append("\",\n")
				.append("  \"build_candidate\": \"").append(json(options.buildCandidate())).append("\",\n")
				.append("  \"classpath_baseline_sha256\": \"").append(baselineClassPathHash).append("\",\n")
				.append("  \"classpath_candidate_sha256\": \"").append(candidateClassPathHash).append("\",\n")
				.append("  \"dataset_id\": \"").append(datasetId).append("\",\n")
				.append("  \"host_state\": \"").append(json(options.hostState())).append("\",\n")
				.append("  \"cache_state\": \"").append(json(options.cacheState())).append("\",\n")
				.append("  \"storage_label\": \"").append(json(options.storageLabel())).append("\",\n")
				.append("  \"storage_name\": \"").append(json(store.name())).append("\",\n")
				.append("  \"storage_type\": \"").append(json(store.type())).append("\",\n")
				.append("  \"fixed_paired_rounds\": ").append(FIXED_PAIRED_ROUNDS).append(",\n")
				.append("  \"adaptive_stopping\": false,\n")
				.append("  \"passed\": ").append(comparison.passed()).append(",\n")
				.append("  \"scenarios\": {\n");
		int scenarioIndex = 0;
		for (Scenario scenario : Scenario.values()) {
			ScenarioComparison scenarioResult = comparison.scenarios().get(scenario);
			if (scenarioResult == null) continue;
			if (scenarioIndex++ > 0) out.append(",\n");
			out.append("    \"").append(scenario.value).append("\": {\"passed\": ")
					.append(scenarioResult.gate().passed()).append(", \"metrics\": {");
			int metricIndex = 0;
			for (Map.Entry<String, PairedBenchmarkStatistics.RatioConfidenceInterval> entry
					: scenarioResult.gate().intervals().entrySet()) {
				if (metricIndex++ > 0) out.append(',');
				var interval = entry.getValue();
				out.append("\"").append(json(entry.getKey())).append("\":{\"samples\":")
						.append(interval.samples()).append(",\"mean\":")
						.append(interval.available() ? format(interval.mean()) : "null")
						.append(",\"lower_95\":")
						.append(interval.available() ? format(interval.lower95()) : "null")
						.append(",\"upper_95\":")
						.append(interval.available() ? format(interval.upper95()) : "null").append('}');
			}
			out.append("}, \"material_improvements\": [");
			for (int index = 0; index < scenarioResult.gate().materialImprovements().size(); index++) {
				if (index > 0) out.append(',');
				out.append('"').append(json(scenarioResult.gate().materialImprovements().get(index))).append('"');
			}
			out.append("], \"exception_candidates\": [");
			for (int index = 0; index < scenarioResult.gate().exceptionCandidates().size(); index++) {
				if (index > 0) out.append(',');
				out.append('"').append(json(scenarioResult.gate().exceptionCandidates().get(index))).append('"');
			}
			out.append("], \"gc\": \"").append(json(scenarioResult.gcSummary())).append("\"}");
		}
		out.append("\n  },\n  \"workers\": [\n");
		for (int index = 0; index < results.size(); index++) {
			WorkerResult result = results.get(index);
			out.append("    {\"round\":").append(result.round()).append(",\"scenario\":\"")
					.append(result.scenario().value).append("\",\"implementation\":\"")
					.append(result.implementation().value).append("\",\"build_sha\":\"")
					.append(result.buildSha()).append("\",\"classpath_sha256\":\"")
					.append(result.classPathSha256()).append("\",\"arena_instrumentation_sha256\":\"")
					.append(result.arenaInstrumentationSha256()).append("\",\"dataset_id\":\"")
					.append(result.datasetId()).append("\",\"operations\":")
					.append(result.operations()).append(",\"items\":").append(result.items())
					.append(",\"logical_bytes\":").append(result.logicalBytes())
					.append(",\"checksum\":\"").append(Long.toUnsignedString(result.checksum())).append("\"")
					.append(",\"elapsed_nanos\":").append(result.elapsedNanos())
					.append(",\"entries_per_second\":")
					.append(format(result.entriesPerSecond())).append(",\"mib_per_second\":")
					.append(format(result.mibPerSecond())).append(",\"completion_p99_nanos\":")
					.append(result.completionP99Nanos()).append(",\"cold_completion_nanos\":")
					.append(result.coldCompletionNanos()).append(",\"first_item_p99_nanos\":")
					.append(result.firstItemP99Nanos()).append(",\"cold_first_item_nanos\":")
					.append(result.coldFirstItemNanos()).append(",\"foreground_p99_nanos\":")
					.append(result.foregroundP99Nanos()).append(",\"foreground_operations\":")
					.append(result.foregroundOperations()).append(",\"cpu_nanos_per_item\":")
					.append(format(result.cpuNanosPerItem())).append(",\"allocated_bytes_per_item\":")
					.append(format(result.allocatedBytesPerItem())).append(",\"peak_live_heap_bytes\":")
					.append(result.peakLiveHeapBytes()).append(",\"peak_direct_memory_bytes\":")
					.append(result.peakDirectMemoryBytes()).append(",\"peak_resident_set_bytes\":")
					.append(result.peakResidentSetBytes()).append(",\"peak_thread_count\":")
					.append(result.peakThreadCount()).append(",\"peak_native_handles\":")
					.append(result.peakNativeHandles()).append(",\"gc_collections\":")
					.append(result.gcCollections()).append(",\"gc_millis\":").append(result.gcMillis())
					.append(",\"scheduler_accepted\":").append(result.scheduler().accepted())
					.append(",\"scheduler_started\":").append(result.scheduler().started())
					.append(",\"scheduler_terminal\":").append(result.scheduler().terminal())
					.append(",\"scheduler_execution_terminal\":")
					.append(result.scheduler().executionTerminal()).append(",\"scheduler_quantums\":")
					.append(result.scheduler().quantums()).append(",\"scheduler_failures\":")
					.append(result.scheduler().failures()).append(",\"scheduler_rejections\":")
					.append(result.scheduler().rejections()).append(",\"scheduler_cancellations\":")
					.append(result.scheduler().cancellations()).append(",\"scheduler_queue_p99_nanos\":")
					.append(result.scheduler().queueP99Nanos())
					.append(",\"scheduler_execution_p99_nanos\":")
					.append(result.scheduler().executionP99Nanos()).append(",\"resource_peaks\":")
					.append(resourcePeaksJson(result.resourcePeaks())).append(",\"final_resources\":")
					.append(resourceSnapshotJson(result.finalResources())).append(",\"native_leaks\":")
					.append(result.nativeLeaks()).append(",\"correctness\":").append(result.correctness())
					.append(",\"resources_drained\":").append(result.resourcesDrained())
					.append(",\"accounting_valid\":").append(result.accountingValid())
					.append(",\"configured_retained_limit\":").append(result.configuredRetainedLimit())
					.append(",\"passed\":")
					.append(result.structurallyPassed()).append('}');
			out.append(index + 1 == results.size() ? '\n' : ",\n");
		}
		out.append("  ],\n  \"failures\": [");
		for (int index = 0; index < comparison.failures().size(); index++) {
			if (index > 0) out.append(',');
			out.append('"').append(json(comparison.failures().get(index))).append('"');
		}
		return out.append("]\n}\n").toString();
	}

	private static String resourcePeaksJson(ResourcePeaks peaks) {
		return "{\"queued\":" + peaks.queued() + ",\"active\":" + peaks.active()
				+ ",\"parked\":" + peaks.parked() + ",\"outstanding\":" + peaks.outstanding()
				+ ",\"pending\":" + peaks.pending() + ",\"iterators\":" + peaks.iterators()
				+ ",\"range_cursors\":" + peaks.rangeCursors() + ",\"retained_snapshots\":"
				+ peaks.retainedSnapshots() + ",\"retained_permits\":" + peaks.retainedPermits()
				+ ",\"retained_waiters\":" + peaks.retainedWaiters() + ",\"iterator_leases\":"
				+ peaks.iteratorLeases() + ",\"exists_multi_requests\":" + peaks.existsMultiRequests()
				+ ",\"exists_multi_snapshots\":" + peaks.existsMultiSnapshots()
				+ ",\"exists_multi_read_options\":" + peaks.existsMultiReadOptions()
				+ ",\"exists_multi_arenas\":" + peaks.existsMultiArenas() + '}';
	}

	private static String resourceSnapshotJson(ResourceSnapshot resources) {
		return "{\"queued\":" + resources.queued() + ",\"active\":" + resources.active()
				+ ",\"parked\":" + resources.parked() + ",\"outstanding\":" + resources.outstanding()
				+ ",\"submission_attempts\":" + resources.submissionAttempts()
				+ ",\"terminal_outcomes\":" + resources.terminalOutcomes()
				+ ",\"scheduler_accounting_exact\":" + resources.exactSchedulerAccounting()
				+ ",\"pending\":" + resources.pending() + ",\"transactions\":" + resources.transactions()
				+ ",\"iterators\":" + resources.iterators() + ",\"range_cursors\":"
				+ resources.rangeCursors() + ",\"retained_snapshots\":" + resources.retainedSnapshots()
				+ ",\"retained_permits\":" + resources.retainedPermits() + ",\"retained_waiters\":"
				+ resources.retainedWaiters() + ",\"iterator_leases\":" + resources.iteratorLeases()
				+ ",\"exists_multi_requests\":" + resources.existsMultiRequests()
				+ ",\"exists_multi_snapshots\":" + resources.existsMultiSnapshots()
				+ ",\"exists_multi_read_options\":" + resources.existsMultiReadOptions()
				+ ",\"exists_multi_arenas\":" + resources.existsMultiArenas() + '}';
	}

	private static String format(double value) {
		return String.format(Locale.ROOT, "%.6f", value);
	}

	private static String json(String value) {
		return value.replace("\\", "\\\\").replace("\"", "\\\"")
				.replace("\n", "\\n").replace("\r", "\\r");
	}

	private static String escapeProperty(String value) {
		return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r");
	}

	private static String cpuModel() {
		Path cpuInfo = Path.of("/proc/cpuinfo");
		if (Files.isReadable(cpuInfo)) {
			try {
				for (String line : Files.readAllLines(cpuInfo)) {
					int separator = line.indexOf(':');
					if (separator > 0 && line.substring(0, separator).trim().equalsIgnoreCase("model name")) {
						return line.substring(separator + 1).trim();
					}
				}
			} catch (IOException ignored) {
				// The portable architecture fallback below is sufficient when procfs is unavailable.
			}
		}
		return System.getProperty("os.arch", "unknown");
	}

	private record Options(boolean worker,
	                       Path root,
	                       Path datasetRoot,
	                       Path output,
	                       Path baselineClasses,
	                       Path candidateClasses,
	                       Implementation implementation,
	                       Scenario scenario,
	                       int round,
	                       String buildBaseline,
	                       String buildCandidate,
	                       String buildStateBaseline,
	                       String buildStateCandidate,
	                       String storageLabel,
	                       String hostState,
	                       String cacheState,
	                       int preloadKeys,
	                       int flushKeys,
	                       int batchEntries,
	                       int valueBytes,
	                       int existsItems,
	                       int iteratorSkip,
	                       int iteratorTake,
	                       int readParallelism,
	                       int writeParallelism,
	                       int foregroundWorkers,
	                       int warmupOperations,
	                       int measureSeconds,
	                       int sampleMicros,
	                       int maxLatencySamples,
	                       int configuredRetainedLimit,
	                       String childHeap,
	                       String expectedClassPathSha256,
	                       String datasetId,
	                       boolean enforce,
	                       boolean smoke) {

		private static final Set<String> KNOWN_OPTIONS = Set.of(
				"worker", "root", "dataset-root", "output", "baseline-classes", "candidate-classes",
				"implementation", "scenario", "round", "build-baseline", "build-candidate",
				"build-state-baseline", "build-state-candidate", "storage-label", "host-state",
				"cache-state", "preload-keys", "flush-keys", "batch-entries", "value-bytes",
				"exists-items", "iterator-skip", "iterator-take", "read-parallelism",
				"write-parallelism", "foreground-workers", "warmup-operations", "measure-seconds",
				"sample-micros", "max-latency-samples", "configured-retained-limit", "child-heap",
				"expected-classpath-sha256", "dataset-id", "enforce", "smoke");

		private static Options parse(String[] args) {
			Map<String, String> values = new LinkedHashMap<>();
			for (String argument : args) {
				if (!argument.startsWith("--") || !argument.contains("=")) {
					throw new IllegalArgumentException("Options must use --name=value: " + argument);
				}
				int equals = argument.indexOf('=');
				String key = argument.substring(2, equals);
				String previous = values.put(key, argument.substring(equals + 1));
				if (previous != null) throw new IllegalArgumentException("Duplicate option: --" + key);
			}
			for (String key : values.keySet()) {
				if (!KNOWN_OPTIONS.contains(key)) throw new IllegalArgumentException("Unknown option: --" + key);
			}
			boolean worker = bool(values, "worker", false);
			boolean smoke = bool(values, "smoke", false);
			Path root = Path.of(values.getOrDefault("root", Path.of(System.getProperty("java.io.tmpdir"),
					"rockserver-retained-read-" + System.currentTimeMillis()).toString()));
			Options options = new Options(worker, root,
					Path.of(values.getOrDefault("dataset-root", root.resolve("shared-dataset").toString())),
					Path.of(values.getOrDefault("output", root.resolve("worker.properties").toString())),
					Path.of(values.getOrDefault("baseline-classes", "baseline-target/classes")),
					Path.of(values.getOrDefault("candidate-classes", "target/classes")),
					Implementation.parse(values.getOrDefault("implementation", "candidate")),
					Scenario.parse(values.getOrDefault("scenario", "exact-count-isolated")),
					integer(values, "round", 1), values.getOrDefault("build-baseline", "unverified"),
					values.getOrDefault("build-candidate", "unverified"),
					values.getOrDefault("build-state-baseline", "unknown"),
					values.getOrDefault("build-state-candidate", "unknown"),
					values.getOrDefault("storage-label", "ci-structural"),
					values.getOrDefault("host-state", "shared"),
					values.getOrDefault("cache-state", "unspecified"),
					integer(values, "preload-keys", smoke ? 16_384 : 262_144),
					integer(values, "flush-keys", smoke ? 4_096 : 32_768),
					integer(values, "batch-entries", smoke ? 1_024 : 4_096),
					integer(values, "value-bytes", 256),
					integer(values, "exists-items", smoke ? 8_192 : 16_384),
					integer(values, "iterator-skip", smoke ? 4_097 : 8_192),
					integer(values, "iterator-take", smoke ? 4_097 : 8_192),
					integer(values, "read-parallelism", smoke ? 2 : 4),
					integer(values, "write-parallelism", smoke ? 2 : 4),
					integer(values, "foreground-workers", smoke ? 2 : 4),
					integer(values, "warmup-operations", smoke ? 1 : 2),
					integer(values, "measure-seconds", smoke ? 1 : 10),
					integer(values, "sample-micros", 250),
					integer(values, "max-latency-samples", smoke ? 10_000 : 200_000),
					integer(values, "configured-retained-limit", smoke ? 2 : 4),
					values.getOrDefault("child-heap", smoke ? "1g" : "4g"),
					values.getOrDefault("expected-classpath-sha256", ""),
					values.getOrDefault("dataset-id", ""), bool(values, "enforce", !smoke), smoke);
			options.validate();
			return options;
		}

		private void validate() {
			if (preloadKeys < 8_194 || flushKeys < 1 || batchEntries < 1 || valueBytes < 24
					|| preloadKeys % flushKeys != 0 || flushKeys % batchEntries != 0
					|| existsItems <= 4_096 || iteratorSkip <= 4_096 || iteratorTake <= 4_096
					|| (long) iteratorSkip + iteratorTake > preloadKeys) {
				throw new IllegalArgumentException("Dataset and retained-read dimensions must force multiple "
						+ "native steps and divide into exact flush/batch groups");
			}
			if (readParallelism < 1 || writeParallelism < 1 || foregroundWorkers < readParallelism
					|| warmupOperations < 1 || measureSeconds < 1 || sampleMicros < 1
					|| maxLatencySamples < 100 || configuredRetainedLimit < 1 || round < 1
					|| round > FIXED_PAIRED_ROUNDS || childHeap.isBlank()) {
				throw new IllegalArgumentException("Parallelism, timing, sampling, round, and limit values are invalid");
			}
			if (!buildBaseline.matches("[0-9a-f]{40}") || !buildCandidate.matches("[0-9a-f]{40}")) {
				throw new IllegalArgumentException("Baseline and candidate must be exact 40-character Git SHAs");
			}
			requireRawBaseline(buildBaseline);
			if (worker) {
				if (!expectedClassPathSha256.matches("[0-9a-f]{64}") || !datasetId.matches("[0-9a-f]{64}")) {
					throw new IllegalArgumentException("Workers require exact classpath and dataset SHA-256 values");
				}
				if (!Files.isDirectory(datasetRoot)) {
					throw new IllegalArgumentException("Worker dataset root does not exist: " + datasetRoot);
				}
			} else {
				if (!Files.isDirectory(baselineClasses) || !Files.isDirectory(candidateClasses)) {
					throw new IllegalArgumentException("baseline-classes and candidate-classes must exist");
				}
			}
			if (enforce && (worker || smoke || preloadKeys < 262_144 || existsItems < 16_384
					|| iteratorSkip < 8_192 || iteratorTake < 8_192 || measureSeconds < 10
					|| !buildStateBaseline.equals("clean") || !buildStateCandidate.equals("clean")
					|| !hostState.equals("dedicated") || cacheState.equals("unspecified")
					|| storageLabel.equals("ci-structural"))) {
				throw new IllegalArgumentException("Enforced retained-read comparison requires clean full-SHA "
						+ "builds, dedicated hardware, explicit cache/storage labels, full dimensions, "
						+ "and the fixed ten-pair controller schedule");
			}
		}

		private long sampleNanos() {
			return TimeUnit.MICROSECONDS.toNanos(sampleMicros);
		}
	}

	private static void requireRawBaseline(String buildBaseline) {
		if (!buildBaseline.equals(PERFORMANCE_BASELINE_SHA)) {
			throw new IllegalArgumentException("Retained-read baseline must be the immutable v1.3.11 checkpoint "
					+ PERFORMANCE_BASELINE_SHA + ", not " + buildBaseline);
		}
	}

	private static Map<String, String> strictProperties(String artifact, Set<String> expectedKeys) {
		Map<String, String> values = new LinkedHashMap<>();
		int lineNumber = 0;
		String[] lines = artifact.split("\\R", -1);
		for (int index = 0; index < lines.length; index++) {
			String rawLine = lines[index];
			lineNumber++;
			if (rawLine.isEmpty() && index == lines.length - 1) continue;
			if (rawLine.startsWith("#") || rawLine.startsWith("!") || rawLine.isBlank()) {
				throw new IllegalArgumentException("Comments and blank padded lines are not allowed at line "
						+ lineNumber);
			}
			int equals = rawLine.indexOf('=');
			if (equals <= 0 || equals != rawLine.lastIndexOf('=')) {
				throw new IllegalArgumentException("Malformed property at line " + lineNumber);
			}
			String key = rawLine.substring(0, equals);
			String value = rawLine.substring(equals + 1);
			if (!expectedKeys.contains(key)) {
				throw new IllegalArgumentException("Unknown property: " + key);
			}
			if (value.isEmpty()) {
				throw new IllegalArgumentException("Empty property: " + key);
			}
			if (values.put(key, value) != null) {
				throw new IllegalArgumentException("Duplicate property: " + key);
			}
		}
		Set<String> missing = new LinkedHashSet<>(expectedKeys);
		missing.removeAll(values.keySet());
		if (!missing.isEmpty()) throw new IllegalArgumentException("Missing properties: " + missing);
		return Map.copyOf(values);
	}

	private static void requireEqual(Map<String, String> values, String key, String expected) {
		String actual = values.get(key);
		if (!Objects.equals(expected, actual)) {
			throw new IllegalArgumentException("Provenance mismatch for " + key + ": expected="
					+ expected + " actual=" + actual);
		}
	}

	private static int integer(Map<String, String> values, String key) {
		try {
			return Integer.parseInt(required(values, key));
		} catch (NumberFormatException invalid) {
			throw new IllegalArgumentException("Invalid integer property: " + key, invalid);
		}
	}

	private static long number(Map<String, String> values, String key) {
		try {
			return Long.parseLong(required(values, key));
		} catch (NumberFormatException invalid) {
			throw new IllegalArgumentException("Invalid long property: " + key, invalid);
		}
	}

	private static long unsignedNumber(Map<String, String> values, String key) {
		try {
			return Long.parseUnsignedLong(required(values, key));
		} catch (NumberFormatException invalid) {
			throw new IllegalArgumentException("Invalid unsigned long property: " + key, invalid);
		}
	}

	private static double decimal(Map<String, String> values, String key) {
		final double value;
		try {
			value = Double.parseDouble(required(values, key));
		} catch (NumberFormatException invalid) {
			throw new IllegalArgumentException("Invalid decimal property: " + key, invalid);
		}
		if (!Double.isFinite(value)) throw new IllegalArgumentException("Non-finite decimal property: " + key);
		return value;
	}

	private static boolean bool(Map<String, String> values, String key) {
		String value = required(values, key);
		if (!value.equals("true") && !value.equals("false")) {
			throw new IllegalArgumentException("Invalid boolean property: " + key);
		}
		return Boolean.parseBoolean(value);
	}

	private static String required(Map<String, String> values, String key) {
		String value = values.get(key);
		if (value == null) throw new IllegalArgumentException("Missing property: " + key);
		return value;
	}

	private static int integer(Map<String, String> values, String key, int fallback) {
		try {
			return Integer.parseInt(values.getOrDefault(key, Integer.toString(fallback)));
		} catch (NumberFormatException invalid) {
			throw new IllegalArgumentException("Invalid integer option: --" + key, invalid);
		}
	}

	private static boolean bool(Map<String, String> values, String key, boolean fallback) {
		String value = values.getOrDefault(key, Boolean.toString(fallback));
		if (!value.equals("true") && !value.equals("false")) {
			throw new IllegalArgumentException("Invalid boolean option: --" + key);
		}
		return Boolean.parseBoolean(value);
	}

	private static void printUsage() {
		System.out.println("""
				Paired whole-gRPC retained-read Pareto gate:

				  java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \\
				    -cp target/test-classes:target/classes:<test-dependencies> \\
				    it.cavallium.rockserver.core.impl.benchmark.GrpcRetainedReadBenchmark \\
				    --root=/mnt/rockserver-hdd/retained-read-RC-SHA \\
				    --baseline-classes=/tmp/rockserver-baseline/target/classes \\
				    --candidate-classes=target/classes \\
				    --build-baseline=<full-baseline-sha> --build-candidate=<full-candidate-sha> \\
				    --build-state-baseline=clean --build-state-candidate=clean \\
				    --storage-label=nvme-ext4 --host-state=dedicated --cache-state=controlled \\
				    --enforce=true

				The controller predetermines ten paired rounds and alternates build order. Each of
				eight isolated or foreground-mixed scenarios records a process-first cold probe followed by
				a warmed steady window. Strict gates use exponentiated paired log-ratio Student-t
				95% confidence intervals, automatic 1.0 gates, and one material primary gain;
				missing or malformed evidence fails. Use --smoke=true
				--enforce=false only for structural validation; the ten-pair schedule is unchanged.
				""");
	}
}
