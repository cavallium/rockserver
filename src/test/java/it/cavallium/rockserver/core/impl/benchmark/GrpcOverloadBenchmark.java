package it.cavallium.rockserver.core.impl.benchmark;

import com.sun.management.OperatingSystemMXBean;
import com.sun.management.ThreadMXBean;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.proto.FirstAndLast;
import it.cavallium.rockserver.core.common.api.proto.CdcCommitRequest;
import it.cavallium.rockserver.core.common.api.proto.CdcCreateRequest;
import it.cavallium.rockserver.core.common.api.proto.CdcPollRequest;
import it.cavallium.rockserver.core.common.api.proto.CloseTransactionRequest;
import it.cavallium.rockserver.core.common.api.proto.FlushRequest;
import it.cavallium.rockserver.core.common.api.proto.GetRangeRequest;
import it.cavallium.rockserver.core.common.api.proto.GetRequest;
import it.cavallium.rockserver.core.common.api.proto.GetResponse;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.api.proto.OpenTransactionRequest;
import it.cavallium.rockserver.core.common.api.proto.PutRequest;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.RocksLeakDetector;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.ToDoubleFunction;
import org.rocksdb.RocksDB;

/**
 * Opt-in, disk-backed gRPC overload regression benchmark.
 *
 * <p>The runner opens one real RocksDB database, preloads and flushes it, then runs a
 * foreground-only phase followed by a seven-profile mixed-flood phase against the same state. The
 * five-second client and range budgets are fixed deliberately: changing the timeout is not a
 * benchmark knob. Every transport call is independently counted at start and terminal close, and
 * the mixed phase samples scheduler work conservation in every pool. This class lives in test
 * sources and is not executed by ordinary CI.</p>
 */
public final class GrpcOverloadBenchmark {

	private static final long DEADLINE_MILLIS = 5_000;
	private static final long WORKER_SHUTDOWN_GRACE_SECONDS = 30;
	private static final long RESOURCE_DRAIN_TIMEOUT_SECONDS = 30;
	private static final int WRITE_REQUEST_VARIANTS = 64;
	private static final int MAX_RECORDED_ERRORS = 20;
	private static final long COOPERATIVE_QUANTUM_NANOS = TimeUnit.MILLISECONDS.toNanos(8L);
	private static final long RUNTIME_RESOURCE_SAMPLE_NANOS = TimeUnit.MILLISECONDS.toNanos(100L);
	private static final String CDC_SUBSCRIPTION_PREFIX = "grpc-overload-cdc-";
	private static final String RESULT_SCHEMA = "rockserver-grpc-overload-v6";
	private static final String PERFORMANCE_BASELINE_SHA = "bb4f1a7e90db1fdfd785936594d080e8c4a0ba4e";
	private static final String DATASET_SCHEMA = "rockserver-grpc-overload-dataset-v4";
	private static final String RUN_ATTEMPT_SCHEMA = "rockserver-grpc-overload-run-attempt-v2";
	private static final String DATASET_MARKER_FILE = ".rockserver-overload-benchmark";
	private static final String RUN_ATTEMPT_FILE = "run-attempt.properties";
	private static final long GIBIBYTE = 1L << 30;
	private static final int MIN_RELEASE_HOST_AVAILABLE_GIB = 8;
	private static final RWScheduler.Pool[] POOL_VALUES = RWScheduler.Pool.values();
	private static final WorkloadProfile[] WORKLOAD_PROFILE_VALUES = WorkloadProfile.values();

	private GrpcOverloadBenchmark() {
	}

	public static void main(String[] args) throws Exception {
		if (Arrays.asList(args).contains("--help")) {
			printUsage();
			return;
		}

		Options options = Options.parse(args);
		if (!options.prepareOnly()
				&& options.instrumentationMode().equals("strict")
				&& !options.buildId().equals(PERFORMANCE_BASELINE_SHA)
				&& (!BenchmarkSchedulerTelemetry.exactAccounting()
				|| !BenchmarkSchedulerTelemetry.allocationFreePoolTelemetry())) {
			throw new IllegalStateException("Strict candidate measurement requires exact accounting "
					+ "and allocation-free pool telemetry");
		}
		System.setProperty("rockserver.core.print-config", "false");
		System.setProperty("it.cavallium.rockserver.leakdetection", "true");
		Instant started = Instant.now();
		RunEnvironment environment = RunEnvironment.capture(options.root());
		verifyEnvironment(options, environment);
		verifyHostMemory(options, environment.hostMemory());
		verifyStorage(options, environment.storage());
		verifyCompetingBenchmarks(options, environment.competingBenchmarkProcesses());
		long nativeLeaksBefore = RocksLeakDetector.detectedLeakCount();
		Path root;
		Path config;
		if (options.reusePreloaded()) {
			root = openPreparedRoot(options, environment);
			config = root.resolve("rockserver.conf");
			writeMetadata(root.resolve("run-metadata.txt"), options, environment, started);
		} else {
			root = createFreshRoot(options);
			config = writeConfig(root, options);
			writeMetadata(root.resolve("metadata.txt"), options, environment, started);
		}

		List<PhaseResult> phases = new ArrayList<>(options.rounds() * 2);
		IntegrityResult integrity = IntegrityResult.notRun();
		Throwable runFailure = null;
		Throwable closeFailure = null;
		EmbeddedConnection embedded = null;
		GrpcServer server = null;
		Client client = null;
		try {
			embedded = new EmbeddedConnection(root.resolve("db"), options.databaseName(), config);
			long columnId = options.reusePreloaded()
					? embedded.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).getColumnId("overload-benchmark")
					: preload(embedded.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()), options);
			if (!options.prepareOnly()) {
				Requests requests = Requests.create(columnId, options);
				server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0));
				server.start();
				client = Client.open(server.getPort());
				preflight(client, requests, columnId, options);

				for (int round = 1; round <= options.rounds(); round++) {
					Phase first = (round & 1) == 1 ? Phase.FOREGROUND_ONLY : Phase.MAINTENANCE_FLOOD;
					Phase second = first == Phase.FOREGROUND_ONLY
							? Phase.MAINTENANCE_FLOOD : Phase.FOREGROUND_ONLY;
					phases.add(runPhase(round, first, client, embedded, server, requests, options));
					phases.add(runPhase(round, second, client, embedded, server, requests, options));
				}
				integrity = runIntegrityProbe(client, columnId, options);
			}
		} catch (Throwable failure) {
			runFailure = failure;
		} finally {
			closeFailure = closeAll(client, server, embedded);
		}
		client = null;
		server = null;
		embedded = null;
		long nativeLeaksDetected = awaitNativeLeakDetection(nativeLeaksBefore);
		if (options.prepareOnly()) {
			if (runFailure != null) {
				if (closeFailure != null) {
					runFailure.addSuppressed(closeFailure);
				}
				throw rethrow(runFailure);
			}
			if (closeFailure != null) {
				throw rethrow(closeFailure);
			}
			if (nativeLeaksDetected != 0) {
				throw new IllegalStateException("Preparation detected native-handle leaks: " + nativeLeaksDetected);
			}
			System.out.println("Prepared and closed benchmark database: " + root);
			System.out.println("Drop the host page cache, then rerun once with identical build/workload options, "
					+ "--cache-state=cold, and --reuse-preloaded=true.");
			return;
		}

		boolean shutdownClean = closeFailure == null;
		Acceptance acceptance = phases.size() == options.rounds() * 2
				? evaluateAcceptance(gateInput(phases, integrity, shutdownClean, nativeLeaksDetected))
				: Acceptance.failed("Every alternating benchmark round must complete both phases");
		BenchmarkResult result = new BenchmarkResult(
				RESULT_SCHEMA,
				started,
				Instant.now(),
				options,
				environment,
				List.copyOf(phases),
				integrity,
				shutdownClean,
				nativeLeaksDetected,
				acceptance);
		writeReports(root, result);
		System.out.println(toMarkdown(result));
		System.out.println("Machine-readable results: " + root.resolve("results.json").toAbsolutePath());
		System.out.println("Human-readable results: " + root.resolve("results.md").toAbsolutePath());
		if (phases.size() == options.rounds() * 2) {
			System.out.println("Paired comparison input: "
					+ root.resolve(GrpcOverloadComparison.RUN_INPUT_FILE).toAbsolutePath());
		}

		if (runFailure != null) {
			if (closeFailure != null) {
				runFailure.addSuppressed(closeFailure);
			}
			throw rethrow(runFailure);
		}
		if (closeFailure != null) {
			throw rethrow(closeFailure);
		}
		if (options.enforce() && !acceptance.passed()) {
			throw new IllegalStateException("Overload benchmark acceptance failed: "
					+ acceptance.failedSummary());
		}
	}

	private static Path createFreshRoot(Options options) throws IOException {
		Path root = options.root().toAbsolutePath().normalize();
		if (Files.exists(root)) {
			throw new IllegalArgumentException("Benchmark root already exists; refusing to reuse state: " + root);
		}
		Files.createDirectories(root);
		Files.writeString(root.resolve(DATASET_MARKER_FILE), datasetMarker(options),
				StandardOpenOption.CREATE_NEW);
		return root;
	}

	private static Path openPreparedRoot(Options options, RunEnvironment environment) throws IOException {
		Path root = options.root().toAbsolutePath().normalize();
		Path marker = root.resolve(DATASET_MARKER_FILE);
		Path config = root.resolve("rockserver.conf");
		Path database = root.resolve("db");
		if (!Files.isRegularFile(marker) || !Files.isRegularFile(config) || !Files.isDirectory(database)) {
			throw new IllegalArgumentException("Prepared benchmark root is incomplete: " + root);
		}
		Map<String, String> markerValues = new LinkedHashMap<>();
		for (String line : Files.readAllLines(marker)) {
			int equals = line.indexOf('=');
			if (equals > 0) {
				markerValues.put(line.substring(0, equals), line.substring(equals + 1));
			}
		}
		if (!DATASET_SCHEMA.equals(markerValues.get("schema"))
				|| !datasetFingerprint(options).equals(markerValues.get("dataset-fingerprint"))
				|| !workloadFingerprint(options).equals(markerValues.get("workload-fingerprint"))
				|| !sha256(configText(options)).equals(markerValues.get("config-sha256"))
				|| !options.buildId().equals(markerValues.get("build-id"))
				|| !options.buildState().equals(markerValues.get("build-state"))
				|| !options.storageLabel().equals(markerValues.get("storage-label"))
				|| !options.hostState().equals(markerValues.get("host-state"))) {
			throw new IllegalArgumentException(
					"Prepared dataset, build provenance, workload, or configuration does not match the requested run");
		}
		if (!Files.readString(config).equals(configText(options))) {
			throw new IllegalArgumentException("Prepared Rockserver configuration does not match the requested run");
		}
		if (Files.exists(root.resolve("results.json")) || Files.exists(root.resolve("results.md"))
				|| Files.exists(root.resolve(RUN_ATTEMPT_FILE))) {
			throw new IllegalArgumentException("Prepared root was already consumed by a benchmark attempt: " + root);
		}
		writeRunAttempt(root, options, environment);
		return root;
	}

	private static String datasetMarker(Options options) {
		String config = configText(options);
		return """
				schema=%s
				dataset-fingerprint=%s
				workload-fingerprint=%s
				config-sha256=%s
				build-id=%s
				build-state=%s
				storage-label=%s
				host-state=%s
				preload-keys=%d
				value-bytes=%d
				seed=%d
				""".formatted(DATASET_SCHEMA,
				datasetFingerprint(options),
				workloadFingerprint(options),
				sha256(config),
				options.buildId(),
				options.buildState(),
				options.storageLabel(),
				options.hostState(),
				options.preloadKeys(),
				options.valueBytes(),
				options.seed());
	}

	private static String datasetFingerprint(Options options) {
		return sha256(DATASET_SCHEMA
				+ "\nseed=" + options.seed()
				+ "\npreload-keys=" + options.preloadKeys()
				+ "\npreload-flush-keys=" + options.preloadFlushKeys()
				+ "\nvalue-bytes=" + options.valueBytes());
	}

	private static String workloadFingerprint(Options options) {
		return sha256("rockserver-grpc-overload-workload-v4"
				+ "\ndataset=" + datasetFingerprint(options)
				+ "\ndatabase-name=" + options.databaseName()
				+ "\nstorage-label=" + options.storageLabel()
				+ "\nhost-state=" + options.hostState()
				+ "\ndeadline-ms=" + DEADLINE_MILLIS
				+ "\nwarmup-seconds=" + options.warmupSeconds()
				+ "\nmeasure-seconds=" + options.measureSeconds()
				+ "\nrounds=" + options.rounds()
				+ "\nworkers=" + options.pointReaders() + ',' + options.foregroundWriters() + ','
				+ options.maintenanceWriters() + ',' + options.firstLastReaders() + ','
				+ options.cancellationWorkers() + ',' + options.analyticalReaders() + ','
				+ options.controlWorkers() + ',' + options.cdcWorkers() + ',' + options.physicalWorkers()
				+ "\nrates=" + options.foregroundWriteRate() + ',' + options.maintenanceWriteRate() + ','
				+ options.firstLastRate() + ',' + options.cancellationRate() + ',' + options.analyticalRate()
				+ ',' + options.controlRate() + ',' + options.cdcRate() + ',' + options.physicalRate()
				+ "\ncancellation=" + options.cancellationDelayMillis() + ',' + options.cancellationBurst()
				+ "\nintegrity-requests=" + options.integrityRequests()
				+ "\nrequest-counts=" + options.pointRequestCount() + ',' + options.rangeRequestCount()
				+ "\nrange-width=" + options.rangeWidth()
				+ "\nparallelism=" + options.readParallelism() + ',' + options.writeParallelism()
				+ "\nqueue-capacities=" + options.foregroundQueueCapacity() + ','
				+ options.maintenanceQueueCapacity()
				+ "\nadmission-sample-micros=" + options.admissionSampleMicros()
				+ "\ninstrumentation-mode=" + options.instrumentationMode()
				+ "\nmax-latency-samples=" + options.maxLatencySamples()
				+ "\nwrite-buffer-size=" + options.writeBufferSize()
				+ "\ndirect-io=" + options.directIo()
				+ "\nspinning=" + options.spinning());
	}

	private static String comparisonFingerprint(Options options) {
		return sha256(workloadFingerprint(options) + "\ncache-state=" + options.cacheState());
	}

	private static String sha256(String value) {
		try {
			return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
					.digest(value.getBytes(StandardCharsets.UTF_8)));
		} catch (NoSuchAlgorithmException impossible) {
			throw new AssertionError(impossible);
		}
	}

	private static String dependencyClasspathSha256() {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			int files = 0;
			for (String value : System.getProperty("java.class.path", "")
					.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
				Path entry = Path.of(value).toAbsolutePath().normalize();
				if (!Files.isRegularFile(entry)) {
					continue;
				}
				files++;
				digest.update(entry.getFileName().toString().getBytes(StandardCharsets.UTF_8));
				digest.update((byte) 0);
				try (InputStream input = Files.newInputStream(entry)) {
					byte[] buffer = new byte[64 * 1_024];
					int read;
					while ((read = input.read(buffer)) >= 0) {
						if (read > 0) {
							digest.update(buffer, 0, read);
						}
					}
				}
				digest.update((byte) 0xff);
			}
			return files == 0 ? "unavailable" : HexFormat.of().formatHex(digest.digest());
		} catch (IOException | NoSuchAlgorithmException | RuntimeException failure) {
			return "unavailable";
		}
	}

	private static String codeSourceSha256(Class<?> type) {
		try {
			var codeSource = type.getProtectionDomain().getCodeSource();
			if (codeSource == null) return "unavailable";
			Path location = Path.of(codeSource.getLocation().toURI()).toAbsolutePath().normalize();
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] buffer = new byte[64 * 1_024];
			List<Path> files;
			if (Files.isRegularFile(location)) {
				files = List.of(location);
			} else if (Files.isDirectory(location)) {
				try (var walked = Files.walk(location)) {
					files = walked.filter(Files::isRegularFile)
							.sorted(java.util.Comparator.comparing(path -> location.relativize(path).toString()))
							.toList();
				}
			} else {
				return "unavailable";
			}
			for (Path file : files) {
				String name = Files.isDirectory(location)
						? location.relativize(file).toString() : file.getFileName().toString();
				digest.update(name.getBytes(StandardCharsets.UTF_8));
				digest.update((byte) 0);
				try (InputStream input = Files.newInputStream(file)) {
					int read;
					while ((read = input.read(buffer)) >= 0) {
						if (read > 0) digest.update(buffer, 0, read);
					}
				}
				digest.update((byte) 0xff);
			}
			return files.isEmpty() ? "unavailable" : HexFormat.of().formatHex(digest.digest());
		} catch (Exception failure) {
			return "unavailable";
		}
	}

	private static String cpuModel() {
		try {
			for (String line : Files.readAllLines(Path.of("/proc/cpuinfo"))) {
				int colon = line.indexOf(':');
				if (colon > 0) {
					String key = line.substring(0, colon).trim();
					if (key.equalsIgnoreCase("model name") || key.equalsIgnoreCase("hardware")) {
						return line.substring(colon + 1).trim();
					}
				}
			}
		} catch (IOException ignored) {
			// Report the missing evidence through the enforced environment gate.
		}
		return "unavailable";
	}

	private static String environmentFingerprint(RunEnvironment environment) {
		return sha256("rockserver-grpc-overload-environment-v1"
				+ "\njava-version=" + environment.javaVersion()
				+ "\njava-vm=" + environment.javaVm()
				+ "\njava-vendor=" + environment.javaVendor()
				+ "\njava-home=" + environment.javaHome()
				+ "\njava-library-path=" + environment.javaLibraryPath()
				+ "\njvm-arguments=" + environment.jvmArguments()
				+ "\njvm-max-memory=" + environment.jvmMaxMemoryBytes()
				+ "\nos=" + environment.os()
				+ "\nprocessors=" + environment.availableProcessors()
				+ "\ncpu-model=" + environment.cpuModel()
				+ "\nphysical-memory=" + environment.hostMemory().totalBytes()
				+ "\nstorage=" + environment.storage()
				+ "\nrocksdb-version=" + environment.rocksdbVersion()
				+ "\ndependency-classpath-sha256=" + environment.dependencyClasspathSha256()
				+ "\nharness-classpath-sha256=" + environment.harnessClasspathSha256()
				+ "\nresource-sample-nanos=" + RUNTIME_RESOURCE_SAMPLE_NANOS);
	}

	private static void writeRunAttempt(Path root, Options options, RunEnvironment environment) throws IOException {
		writeRunAttempt(root, "schema=" + RUN_ATTEMPT_SCHEMA + "\n"
				+ "started=" + Instant.now() + "\n"
				+ "build-id=" + options.buildId() + "\n"
				+ "build-state=" + options.buildState() + "\n"
				+ "storage-label=" + options.storageLabel() + "\n"
				+ "host-state=" + options.hostState() + "\n"
				+ "cache-state=" + options.cacheState() + "\n"
				+ "dataset-fingerprint=" + datasetFingerprint(options) + "\n"
				+ "comparison-fingerprint=" + comparisonFingerprint(options) + "\n"
				+ "environment-fingerprint=" + environmentFingerprint(environment) + "\n"
				+ "process-id=" + environment.processId() + "\n"
				+ "process-start=" + environment.processStart() + "\n"
				+ "host-memory-total-bytes=" + environment.hostMemory().totalBytes() + "\n"
				+ "host-memory-available-bytes=" + environment.hostMemory().availableBytes() + "\n"
				+ "host-swap-free-bytes=" + environment.hostMemory().swapFreeBytes() + "\n"
				+ "storage-mount-point=" + environment.storage().mountPoint() + "\n"
				+ "storage-source=" + environment.storage().source() + "\n"
				+ "storage-filesystem=" + environment.storage().filesystem() + "\n"
				+ "storage-rotational=" + environment.storage().rotational() + "\n"
				+ "storage-model=" + environment.storage().model() + "\n"
				+ "competing-benchmark-processes=" + environment.competingBenchmarkProcesses() + "\n");
	}

	private static void writeRunAttempt(Path root, String content) throws IOException {
		Files.writeString(root.resolve(RUN_ATTEMPT_FILE), content, StandardOpenOption.CREATE_NEW);
	}

	/** Exercises the exact atomic one-shot primitive without opening RocksDB. */
	public static void claimRunAttemptForTesting(Path root) throws IOException {
		Files.createDirectories(root);
		writeRunAttempt(root, "schema=" + RUN_ATTEMPT_SCHEMA + "\n");
	}

	/** Parses and validates options without creating a benchmark root. */
	public static void validateOptionsForTesting(String... args) {
		Options.parse(args);
	}

	/** Stable comparison identity exposed to deterministic contract tests. */
	public static String comparisonFingerprintForTesting(String... args) {
		return comparisonFingerprint(Options.parse(args));
	}

	public static long subtractObserverForTesting(long processDelta, long observerDelta) {
		return RuntimeTelemetryTracker.subtractObserver(processDelta, observerDelta);
	}

	private static long preload(RocksDBSyncAPI api, Options options) {
		System.out.printf(Locale.ROOT,
				"Preloading %,d keys (%d bytes/value), flushing every %,d keys...%n",
				options.preloadKeys(), options.valueBytes(), options.preloadFlushKeys());
		long columnId = api.createColumn("overload-benchmark",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		byte[] value = valueBytes(options.valueBytes(), 0x4f5645524c4f4144L);
		int nextProgress = 10;
		for (long key = 0; key < options.preloadKeys(); key++) {
			api.put(0, columnId, keys(key), Buf.wrap(value), RequestType.none());
			long loaded = key + 1;
			if (loaded % options.preloadFlushKeys() == 0) {
				api.flush();
			}
			int progress = (int) (loaded * 100 / options.preloadKeys());
			if (progress >= nextProgress) {
				System.out.printf(Locale.ROOT, "Preload %d%%%n", progress);
				nextProgress += 10;
			}
		}
		api.flush();
		return columnId;
	}

	private static void preflight(Client client, Requests requests, long columnId, Options options) {
		GetResponse point = client.blockingWithDeadline().get(requests.pointReads()[0]);
		if (!point.hasValue()) {
			throw new IllegalStateException("Point-read preflight returned an absent value");
		}
		RangeCase range = requests.latencyRanges()[0];
		validateRange(client.blockingWithDeadline().reduceRangeFirstAndLast(range.request()), range);
		RangeCase analytical = requests.analyticalRanges()[0];
		validateRange(client.blockingWithDeadline().reduceRangeFirstAndLast(analytical.request()), analytical);
		client.blockingWithDeadline().put(requests.foregroundWrites()[0][0]);
		client.blockingWithDeadline().put(requests.maintenanceWrites()[0][0]);
		long transactionId = client.blockingWithDeadline().openTransaction(OpenTransactionRequest.newBuilder()
				.setTimeoutMs(DEADLINE_MILLIS)
				.setContext(wireContext(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH))
				.build()).getTransactionId();
		var rollback = client.blockingWithDeadline().closeTransaction(CloseTransactionRequest.newBuilder()
				.setTransactionId(transactionId)
				.setTimeoutMs(DEADLINE_MILLIS)
				.setCommit(false)
				.setContext(wireContext(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH))
				.build());
		if (!rollback.getSuccessful()) {
			throw new IllegalStateException("CONTROL rollback preflight was not successful");
		}
		for (int worker = 0; worker < options.cdcWorkers(); worker++) {
			client.blockingWithDeadline().cdcCreate(CdcCreateRequest.newBuilder()
					.setId(cdcSubscription(worker))
					.addColumnIds(columnId)
					.build());
		}
		client.blockingWithDeadline().flush(FlushRequest.getDefaultInstance());
	}

	private static it.cavallium.rockserver.core.common.api.proto.RequestContext wireContext(
			it.cavallium.rockserver.core.common.api.proto.WorkloadProfile profile) {
		return it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
				.setProfile(profile)
				.setDeadlineEpochMillis(Long.MAX_VALUE - 1L)
				.build();
	}

	private static String cdcSubscription(int worker) {
		return CDC_SUBSCRIPTION_PREFIX + worker;
	}

	private static PhaseResult runPhase(int round,
			Phase phase,
			Client client,
			EmbeddedConnection embedded,
			GrpcServer server,
			Requests requests,
			Options options) throws Exception {
		boolean maintenanceFlood = phase == Phase.MAINTENANCE_FLOOD;
		int maintenanceWorkers = maintenanceFlood ? options.maintenanceWriters() : 0;
		int cancellationWorkers = maintenanceFlood ? options.cancellationWorkers() : 0;
		int analyticalWorkers = maintenanceFlood ? options.analyticalReaders() : 0;
		int controlWorkers = maintenanceFlood ? options.controlWorkers() : 0;
		int cdcWorkers = maintenanceFlood ? options.cdcWorkers() : 0;
		int physicalWorkers = maintenanceFlood ? options.physicalWorkers() : 0;
		int workerCount = options.pointReaders()
				+ options.foregroundWriters()
				+ options.firstLastReaders()
				+ maintenanceWorkers
				+ cancellationWorkers
				+ analyticalWorkers
				+ controlWorkers
				+ cdcWorkers
				+ physicalWorkers
				+ 1;
		var runtimeTelemetry = new RuntimeTelemetryTracker(
				options.instrumentationMode().equals("strict"));
		PhaseControl control = new PhaseControl(workerCount,
				options.maxLatencySamples(),
				options.instrumentationMode().equals("strict"),
				TimeUnit.MICROSECONDS.toNanos(options.admissionSampleMicros()),
				runtimeTelemetry);
		var meterRegistry = new BenchmarkMeterRegistry();
		var composite = (CompositeMeterRegistry) embedded.getEmbeddedDB().getMetricsRegistry();
		boolean registryAdded = false;
		ExecutorService executor = Executors.newFixedThreadPool(workerCount,
				Thread.ofPlatform().name("grpc-overload-r" + round + '-' + phase.value + "-", 0).factory());
		List<Future<?>> futures = new ArrayList<>(workerCount);
		System.out.printf(Locale.ROOT,
				"Starting round %d %s: warmup=%ds measure=%ds point-readers=%d foreground-writers=%d "
						+ "first-last-readers=%d maintenance-writers=%d cancellation-workers=%d "
						+ "analytical=%d control=%d cdc=%d physical=%d%n",
				round, phase.value, options.warmupSeconds(), options.measureSeconds(), options.pointReaders(),
				options.foregroundWriters(), options.firstLastReaders(), maintenanceWorkers,
				cancellationWorkers, analyticalWorkers, controlWorkers, cdcWorkers, physicalWorkers);
		try {
			for (int index = 0; index < options.pointReaders(); index++) {
				int worker = index;
				submit(executor, futures, control,
						() -> runPointReader(client, requests, options, worker, control));
			}
			for (int index = 0; index < options.foregroundWriters(); index++) {
				int worker = index;
				submit(executor, futures, control,
						() -> runWriter(client,
								requests.foregroundWrites()[worker],
								Operation.FOREGROUND_WRITE,
								options.foregroundWriteRate(),
								options.foregroundWriters(),
								worker,
								control));
			}
			for (int index = 0; index < options.firstLastReaders(); index++) {
				int worker = index;
				submit(executor, futures, control,
						() -> runFirstLastReader(client, requests, options, phase, worker, control));
			}
			for (int index = 0; index < maintenanceWorkers; index++) {
				int worker = index;
				submit(executor, futures, control,
						() -> runWriter(client,
								requests.maintenanceWrites()[worker],
								Operation.MAINTENANCE_WRITE,
								options.maintenanceWriteRate(),
								maintenanceWorkers,
								worker,
								control));
			}
			for (int index = 0; index < cancellationWorkers; index++) {
				int worker = index;
				submit(executor, futures, control,
						() -> runCancellationWorker(client,
								requests.cancellationWrites()[worker], options, worker, control));
			}
			for (int index = 0; index < analyticalWorkers; index++) {
				int worker = index;
				submit(executor, futures, control,
						() -> runAnalyticalReader(client, requests, options, worker, control));
			}
			for (int index = 0; index < controlWorkers; index++) {
				int worker = index;
				submit(executor, futures, control,
						() -> runControlWorker(client, options, worker, control));
			}
			for (int index = 0; index < cdcWorkers; index++) {
				int worker = index;
				submit(executor, futures, control,
						() -> runCdcWorker(client, options, worker, control));
			}
			for (int index = 0; index < physicalWorkers; index++) {
				int worker = index;
				submit(executor, futures, control,
						() -> runPhysicalWorker(client, options, worker, control));
			}
			submit(executor, futures, control,
					() -> monitorAdmission(embedded, options, control));

			if (!control.ready.await(30, TimeUnit.SECONDS)) {
				throw new IllegalStateException("Benchmark workers did not become ready: remaining="
						+ control.ready.getCount());
			}
			control.start.countDown();
			sleepPhase(options.warmupSeconds(), control.stop);
			if (!control.failures.isEmpty()) {
				control.stop.set(true);
				throw new IllegalStateException("Worker failed during warmup", control.failures.peek());
			}
			composite.add(meterRegistry);
			registryAdded = true;
			client.requestTracker().startTracking();
			runtimeTelemetry.start();
			control.startMeasurement();
			sleepPhase(options.measureSeconds(), control.stop);
			client.requestTracker().stopTracking();
			long durationNanos = control.stopMeasurement();
			RuntimeTelemetry telemetry = runtimeTelemetry.stop();
			control.stop.set(true);
			executor.shutdown();
			long shutdownWait = DEADLINE_MILLIS + TimeUnit.SECONDS.toMillis(WORKER_SHUTDOWN_GRACE_SECONDS);
			if (!executor.awaitTermination(shutdownWait, TimeUnit.MILLISECONDS)) {
				executor.shutdownNow();
				if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
					throw new IllegalStateException("Benchmark workers did not terminate");
				}
			}
			for (Future<?> future : futures) {
				future.get();
			}
			if (!control.failures.isEmpty()) {
				throw new IllegalStateException("Benchmark worker failed", control.failures.peek());
			}

			ResourceResult resources = awaitDrain(embedded, server);
			RequestAccounting requestAccounting = client.requestTracker().awaitSnapshot();
			AdmissionResult admission = control.admission.finish(embedded, control.metrics);
			Map<WorkloadProfile, SchedulerProfileMetrics> schedulerMetrics =
					snapshotSchedulerMetrics(meterRegistry);
			PhaseResult result = control.metrics.snapshot(
					round, phase, durationNanos, admission, resources, requestAccounting, schedulerMetrics,
					telemetry);
			printPhase(result);
			return result;
		} finally {
			client.requestTracker().stopTrackingIfActive();
			control.measuring.set(false);
			control.stop.set(true);
			control.start.countDown();
			executor.shutdownNow();
			if (registryAdded) {
				composite.remove(meterRegistry);
			}
			meterRegistry.close();
			runtimeTelemetry.close();
		}
	}

	private static void submit(ExecutorService executor,
			List<Future<?>> futures,
			PhaseControl control,
			ThrowingRunnable worker) {
		futures.add(executor.submit(() -> {
			try {
				worker.run();
			} catch (InterruptedException interrupted) {
				Thread.currentThread().interrupt();
				if (!control.stop.get()) {
					control.failures.add(interrupted);
					control.stop.set(true);
				}
			} catch (Throwable failure) {
				control.failures.add(failure);
				control.stop.set(true);
			}
		}));
	}

	private static void runPointReader(Client client,
			Requests requests,
			Options options,
			int worker,
			PhaseControl control) throws InterruptedException {
		control.ready.countDown();
		control.start.await();
		long requestSequence = worker;
		long operationSequence = worker;
		while (!control.stop.get()) {
			GetRequest request = requests.pointReads()[(int) (requestSequence % requests.pointReads().length)];
			long started = System.nanoTime();
			Outcome outcome;
			String detail = null;
			try {
				GetResponse response = client.blockingWithDeadline().get(request);
				if (!response.hasValue() || response.getValue().size() != options.valueBytes()) {
					throw new IllegalStateException("Point read returned an invalid value");
				}
				outcome = Outcome.SUCCESS;
			} catch (Throwable failure) {
				outcome = classify(failure);
				detail = describe(failure);
			}
			control.record(Operation.POINT_READ,
					outcome,
					System.nanoTime() - started,
					operationSequence++,
					detail);
			requestSequence += options.pointReaders();
		}
	}

	private static void runFirstLastReader(Client client,
			Requests requests,
			Options options,
			Phase phase,
			int worker,
			PhaseControl control) throws InterruptedException {
		Pacer pacer = new Pacer(options.firstLastRate(), options.firstLastReaders(), worker);
		control.ready.countDown();
		control.start.await();
		long requestSequence = (phase == Phase.MAINTENANCE_FLOOD
				? requests.latencyRanges().length / 2L : 0L) + worker;
		long operationSequence = worker;
		while (!control.stop.get()) {
			RangeCase range = requests.latencyRanges()[(int) (requestSequence % requests.latencyRanges().length)];
			long started = System.nanoTime();
			Outcome outcome;
			String detail = null;
			try {
				validateRange(client.blockingWithDeadline().reduceRangeFirstAndLast(range.request()), range);
				outcome = Outcome.SUCCESS;
			} catch (Throwable failure) {
				outcome = classify(failure);
				detail = describe(failure);
			}
			control.record(Operation.FIRST_LAST,
					outcome,
					System.nanoTime() - started,
					operationSequence++,
					detail);
			requestSequence += options.firstLastReaders();
			pacer.awaitNext(control.stop);
		}
	}

	private static void runAnalyticalReader(Client client,
			Requests requests,
			Options options,
			int worker,
			PhaseControl control) throws InterruptedException {
		Pacer pacer = new Pacer(options.analyticalRate(), options.analyticalReaders(), worker);
		control.ready.countDown();
		control.start.await();
		long requestSequence = worker;
		long operationSequence = worker;
		while (!control.stop.get()) {
			RangeCase range = requests.analyticalRanges()[
					(int) (requestSequence % requests.analyticalRanges().length)];
			long started = System.nanoTime();
			Outcome outcome;
			String detail = null;
			try {
				validateRange(client.blockingWithDeadline().reduceRangeFirstAndLast(range.request()), range);
				outcome = Outcome.SUCCESS;
			} catch (Throwable failure) {
				outcome = classify(failure);
				detail = describe(failure);
			}
			control.record(Operation.ANALYTICAL_READ,
					outcome,
					System.nanoTime() - started,
					operationSequence++,
					detail);
			requestSequence += options.analyticalReaders();
			pacer.awaitNext(control.stop);
		}
	}

	private static void runControlWorker(Client client,
			Options options,
			int worker,
			PhaseControl control) throws InterruptedException {
		Pacer pacer = new Pacer(options.controlRate(), options.controlWorkers(), worker);
		var context = wireContext(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH);
		var open = OpenTransactionRequest.newBuilder()
				.setTimeoutMs(DEADLINE_MILLIS)
				.setContext(context)
				.build();
		control.ready.countDown();
		control.start.await();
		long sequence = worker;
		while (!control.stop.get()) {
			long started = System.nanoTime();
			Outcome outcome;
			String detail = null;
			try {
				long transactionId = client.blockingWithDeadline().openTransaction(open).getTransactionId();
				var closed = client.blockingWithDeadline().closeTransaction(CloseTransactionRequest.newBuilder()
						.setTransactionId(transactionId)
						.setTimeoutMs(DEADLINE_MILLIS)
						.setCommit(false)
						.setContext(context)
						.build());
				if (!closed.getSuccessful()) {
					throw new IllegalStateException("CONTROL rollback did not close its transaction");
				}
				outcome = Outcome.SUCCESS;
			} catch (Throwable failure) {
				outcome = classify(failure);
				detail = describe(failure);
			}
			control.record(Operation.CONTROL,
					outcome,
					System.nanoTime() - started,
					sequence++,
					detail);
			pacer.awaitNext(control.stop);
		}
	}

	private static void runCdcWorker(Client client,
			Options options,
			int worker,
			PhaseControl control) throws InterruptedException {
		Pacer pacer = new Pacer(options.cdcRate(), options.cdcWorkers(), worker);
		var poll = CdcPollRequest.newBuilder()
				.setId(cdcSubscription(worker))
				.setMaxEvents(256L)
				.setMaxResponseBytes(4 * 1024 * 1024)
				.build();
		control.ready.countDown();
		control.start.await();
		long sequence = worker;
		while (!control.stop.get()) {
			long started = System.nanoTime();
			Outcome outcome;
			String detail = null;
			try {
				var response = client.blockingWithDeadline().cdcPollBatch(poll);
				if (response.getEventsCount() > 0) {
					long committed = response.getEvents(response.getEventsCount() - 1).getSeq();
					client.blockingWithDeadline().cdcCommit(CdcCommitRequest.newBuilder()
							.setId(cdcSubscription(worker))
							.setSeq(committed)
							.build());
				}
				outcome = Outcome.SUCCESS;
			} catch (Throwable failure) {
				outcome = classify(failure);
				detail = describe(failure);
			}
			control.record(Operation.CDC,
					outcome,
					System.nanoTime() - started,
					sequence++,
					detail);
			pacer.awaitNext(control.stop);
		}
	}

	private static void runPhysicalWorker(Client client,
			Options options,
			int worker,
			PhaseControl control) throws InterruptedException {
		Pacer pacer = new Pacer(options.physicalRate(), options.physicalWorkers(), worker);
		control.ready.countDown();
		control.start.await();
		long sequence = worker;
		while (!control.stop.get()) {
			long started = System.nanoTime();
			Outcome outcome;
			String detail = null;
			try {
				client.blockingWithDeadline().flush(FlushRequest.getDefaultInstance());
				outcome = Outcome.SUCCESS;
			} catch (Throwable failure) {
				outcome = classify(failure);
				detail = describe(failure);
			}
			control.record(Operation.PHYSICAL,
					outcome,
					System.nanoTime() - started,
					sequence++,
					detail);
			pacer.awaitNext(control.stop);
		}
	}

	private static void runWriter(Client client,
			PutRequest[] requests,
			Operation operation,
			long totalRate,
			int workers,
			int worker,
			PhaseControl control) throws InterruptedException {
		Pacer pacer = new Pacer(totalRate, workers, worker);
		control.ready.countDown();
		control.start.await();
		long sequence = worker;
		while (!control.stop.get()) {
			PutRequest request = requests[(int) (sequence & (requests.length - 1))];
			long started = System.nanoTime();
			Outcome outcome;
			String detail = null;
			try {
				client.blockingWithDeadline().put(request);
				outcome = Outcome.SUCCESS;
			} catch (Throwable failure) {
				outcome = classify(failure);
				detail = describe(failure);
			}
			control.record(operation, outcome, System.nanoTime() - started, sequence++, detail);
			pacer.awaitNext(control.stop);
		}
	}

	private static void runCancellationWorker(Client client,
			PutRequest[] requests,
			Options options,
			int worker,
			PhaseControl control) throws InterruptedException {
		int burstSize = Math.min(options.cancellationBurst(), requests.length);
		Pacer pacer = new Pacer(options.cancellationRate(),
				options.cancellationWorkers() * burstSize,
				worker * burstSize);
		control.ready.countDown();
		control.start.await();
		long sequence = worker;
		while (!control.stop.get()) {
			@SuppressWarnings("unchecked")
			ListenableFuture<Empty>[] calls = new ListenableFuture[burstSize];
			long[] started = new long[burstSize];
			for (int index = 0; index < burstSize; index++) {
				started[index] = System.nanoTime();
				calls[index] = client.futureWithDeadline()
						.put(requests[(int) ((sequence + index) & (requests.length - 1))]);
			}
			LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(options.cancellationDelayMillis()));
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
			for (int index = 0; index < burstSize; index++) {
				Outcome outcome;
				String detail = null;
				if (calls[index].cancel(true)) {
					outcome = Outcome.CANCELLED;
				} else {
					try {
						calls[index].get(DEADLINE_MILLIS + 1_000, TimeUnit.MILLISECONDS);
						outcome = Outcome.SUCCESS;
					} catch (Throwable failure) {
						outcome = classify(failure);
						detail = describe(failure);
					}
				}
				control.record(Operation.CANCELLATION,
						outcome,
						System.nanoTime() - started[index],
						sequence++,
						detail);
			}
			pacer.awaitNext(control.stop);
		}
	}

	private static void monitorAdmission(EmbeddedConnection embedded,
			Options options,
			PhaseControl control) throws InterruptedException {
		control.runtimeTelemetry.registerObserverThread();
		control.ready.countDown();
		control.start.await();
		while (!control.stop.get()) {
			if (control.measuring.get()) {
				control.admission.sample(embedded);
				control.runtimeTelemetry.sampleIfDue();
			}
			LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(options.admissionSampleMicros()));
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
		}
	}

	private static IntegrityResult runIntegrityProbe(Client client,
			long columnId,
			Options options) throws InterruptedException {
		long keyBase = 4L << 60;
		byte[][] expected = new byte[options.integrityRequests()][];
		boolean[] acknowledged = new boolean[options.integrityRequests()];
		long writesAcknowledged = 0L;
		long readsMatched = 0L;
		long mismatches = 0L;
		long errors = 0L;
		var errorDetails = new ArrayList<String>();
		client.requestTracker().startTracking();
		try {
			for (int index = 0; index < options.integrityRequests(); index++) {
				long key = keyBase + index;
				expected[index] = valueBytes(options.valueBytes(), options.seed() ^ key);
				var profile = (index & 1) == 0
						? it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.INGEST
						: it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH;
				var request = PutRequest.newBuilder()
						.setTransactionOrUpdateId(0L)
						.setColumnId(columnId)
						.setData(KV.newBuilder()
								.addKeys(keyByteString(key))
								.setValue(ByteString.copyFrom(expected[index])))
						.setContext(wireContext(profile))
						.build();
				try {
					client.blockingWithDeadline().put(request);
					acknowledged[index] = true;
					writesAcknowledged++;
				} catch (Throwable failure) {
					errors++;
					addIntegrityError(errorDetails, "put[" + index + "]", failure);
				}
			}
			for (int index = 0; index < options.integrityRequests(); index++) {
				long key = keyBase + index;
				try {
					GetResponse response = client.blockingWithDeadline().get(GetRequest.newBuilder()
							.setTransactionOrUpdateId(0L)
							.setColumnId(columnId)
							.addKeys(keyByteString(key))
							.setContext(wireContext(
									it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.LATENCY))
							.build());
					if (acknowledged[index] && response.hasValue()
							&& response.getValue().equals(ByteString.copyFrom(expected[index]))) {
						readsMatched++;
					} else {
						mismatches++;
					}
				} catch (Throwable failure) {
					errors++;
					addIntegrityError(errorDetails, "get[" + index + "]", failure);
				}
			}
		} finally {
			client.requestTracker().stopTrackingIfActive();
		}
		RequestAccounting accounting = client.requestTracker().awaitSnapshot();
		return new IntegrityResult(options.integrityRequests(),
				writesAcknowledged,
				options.integrityRequests(),
				readsMatched,
				mismatches,
				errors,
				List.copyOf(errorDetails),
				accounting);
	}

	private static void addIntegrityError(List<String> errors, String operation, Throwable failure) {
		if (errors.size() < MAX_RECORDED_ERRORS) {
			errors.add(operation + ": " + describe(failure));
		}
	}

	private static void validateRange(FirstAndLast response, RangeCase expected) {
		if (!response.hasFirst() || !response.hasLast()
				|| response.getFirst().getKeysCount() != 1
				|| response.getLast().getKeysCount() != 1
				|| !response.getFirst().getKeys(0).equals(expected.firstKey())
				|| !response.getLast().getKeys(0).equals(expected.lastKey())) {
			throw new IllegalStateException("First/last response did not match the requested cold range");
		}
	}

	private static Outcome classify(Throwable failure) {
		Throwable current = unwrap(failure);
		if (current instanceof CancellationException) {
			return Outcome.CANCELLED;
		}
		Status.Code code = statusCode(current);
		return switch (code) {
			case DEADLINE_EXCEEDED -> Outcome.DEADLINE;
			case RESOURCE_EXHAUSTED -> Outcome.REJECTED;
			case CANCELLED -> Outcome.CANCELLED;
			default -> Outcome.ERROR;
		};
	}

	private static Status.Code statusCode(Throwable failure) {
		if (failure instanceof StatusRuntimeException status) {
			return status.getStatus().getCode();
		}
		if (failure instanceof StatusException status) {
			return status.getStatus().getCode();
		}
		return Status.Code.UNKNOWN;
	}

	private static Throwable unwrap(Throwable failure) {
		Throwable current = failure;
		while ((current instanceof ExecutionException || current instanceof java.util.concurrent.CompletionException)
				&& current.getCause() != null) {
			current = current.getCause();
		}
		return current;
	}

	private static String describe(Throwable failure) {
		Throwable unwrapped = unwrap(failure);
		String message = unwrapped.getMessage();
		return unwrapped.getClass().getSimpleName() + (message == null ? "" : ": " + message);
	}

	private static ResourceResult awaitDrain(EmbeddedConnection embedded, GrpcServer server)
			throws InterruptedException {
		long started = System.nanoTime();
		long deadline = started + TimeUnit.SECONDS.toNanos(RESOURCE_DRAIN_TIMEOUT_SECONDS);
		ResourceResult snapshot;
		do {
			snapshot = resourceSnapshot(embedded, server, System.nanoTime() - started);
			if (snapshot.drained()) {
				return snapshot;
			}
			Thread.sleep(10);
		} while (System.nanoTime() < deadline);
		return resourceSnapshot(embedded, server, System.nanoTime() - started);
	}

	private static ResourceResult resourceSnapshot(EmbeddedConnection embedded,
			GrpcServer server,
			long drainNanos) {
		var scheduler = embedded.getScheduler();
		var db = embedded.getInternalDB();
		var admission = scheduler.admissionSnapshot();
		int foregroundQueued = admission.queued().get(WorkloadProfile.INGEST);
		int maintenanceQueued = admission.queued().get(WorkloadProfile.BATCH);
		int foregroundActive = admission.active().get(WorkloadProfile.INGEST);
		int maintenanceActive = admission.active().get(WorkloadProfile.BATCH);
		int totalQueued = admission.totalQueued();
		int totalActive = admission.totalActive();
		long pending = db.getPendingOpsCount();
		int transactions = db.getOpenTransactionsCount();
		int iterators = db.getOpenIteratorsCount();
		int ranges = db.getActiveRangeCursorCount();
		int retainedSnapshots = db.getRetainedRangeSnapshotCount();
		int retainedPermits = db.getRetainedRangePermitCount();
		int retainedWaiters = db.getRetainedRangeWaiterCount();
		int cdcCursors = db.getActiveCdcPollCursorCount();
		int existsMultiRequests = db.getActiveExistsMultiRequestCount();
		int iteratorLeases = server.getActiveIteratorOperationLeaseCountForTesting();
		SchedulerConservation schedulerConservation = schedulerConservation(scheduler);
		boolean drained = totalQueued == 0
				&& totalActive == 0
				&& pending == 0
				&& transactions == 0
				&& iterators == 0
				&& ranges == 0
				&& retainedSnapshots == 0
				&& retainedPermits == 0
				&& retainedWaiters == 0
				&& cdcCursors == 0
				&& existsMultiRequests == 0
				&& iteratorLeases == 0;
		return new ResourceResult(foregroundQueued,
				maintenanceQueued,
				foregroundActive,
				maintenanceActive,
				totalQueued,
				totalActive,
				pending,
				transactions,
				iterators,
				ranges,
				retainedSnapshots,
				retainedPermits,
				retainedWaiters,
				cdcCursors,
				existsMultiRequests,
				iteratorLeases,
				TimeUnit.NANOSECONDS.toMillis(drainNanos),
				drained,
				schedulerConservation);
	}

	private static SchedulerConservation schedulerConservation(RWScheduler scheduler) {
		long accepted = 0L;
		long started = 0L;
		long completed = 0L;
		long failures = 0L;
		long terminalOutcomes = 0L;
		var failuresByPool = new ArrayList<String>();
		for (RWScheduler.Pool pool : RWScheduler.Pool.values()) {
			var snapshot = scheduler.poolSnapshot(pool);
			accepted += snapshot.acceptedTasks();
			started += snapshot.startedTasks();
			completed += snapshot.completedTasks();
			failures += snapshot.failedTasks();
			terminalOutcomes += snapshot.outcomes().values().stream().mapToLong(Long::longValue).sum();
			if (snapshot.queuedTasks() != 0 || snapshot.activeTasks() != 0
					|| snapshot.startedTasks() != snapshot.completedTasks()
					|| snapshot.failedTasks() != 0L
					|| snapshot.outcomes().values().stream().mapToLong(Long::longValue).sum()
					< snapshot.acceptedTasks()) {
				failuresByPool.add(pool.name().toLowerCase(Locale.ROOT)
						+ " queued=" + snapshot.queuedTasks()
						+ " active=" + snapshot.activeTasks()
						+ " started=" + snapshot.startedTasks()
						+ " completed=" + snapshot.completedTasks()
						+ " accepted=" + snapshot.acceptedTasks()
						+ " outcomes=" + snapshot.outcomes()
						+ " failures=" + snapshot.failedTasks());
			}
		}
		return new SchedulerConservation(accepted,
				started,
				completed,
				terminalOutcomes,
				failures,
				List.copyOf(failuresByPool));
	}

	private static Throwable closeAll(Client client, GrpcServer server, EmbeddedConnection embedded) {
		Throwable failure = null;
		failure = closeOne(client, failure);
		failure = closeOne(server, failure);
		if (embedded != null) {
			try {
				embedded.closeTesting();
			} catch (Throwable closeFailure) {
				failure = addFailure(failure, closeFailure);
			}
		}
		return failure;
	}

	private static long awaitNativeLeakDetection(long before) throws InterruptedException {
		for (int attempt = 0; attempt < 3; attempt++) {
			System.gc();
			Thread.sleep(100);
		}
		return Math.max(0, RocksLeakDetector.detectedLeakCount() - before);
	}

	private static Throwable closeOne(AutoCloseable closeable, Throwable existing) {
		if (closeable == null) {
			return existing;
		}
		try {
			closeable.close();
			return existing;
		} catch (Throwable failure) {
			return addFailure(existing, failure);
		}
	}

	private static Throwable addFailure(Throwable existing, Throwable added) {
		if (existing == null) {
			return added;
		}
		existing.addSuppressed(added);
		return existing;
	}

	private static Exception rethrow(Throwable failure) {
		if (failure instanceof Exception exception) {
			return exception;
		}
		return new RuntimeException(failure);
	}

	private static void sleepPhase(int seconds, AtomicBoolean stop) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
		while (!stop.get()) {
			long remaining = deadline - System.nanoTime();
			if (remaining <= 0) {
				return;
			}
			TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(100)));
		}
	}

	private static GateInput gateInput(List<PhaseResult> phases,
			IntegrityResult integrity,
			boolean shutdownClean,
			long nativeLeaksDetected) {
		List<PhaseResult> foreground = phases.stream()
				.filter(phase -> phase.phase() == Phase.FOREGROUND_ONLY)
				.sorted(java.util.Comparator.comparingInt(PhaseResult::round))
				.toList();
		List<PhaseResult> flood = phases.stream()
				.filter(phase -> phase.phase() == Phase.MAINTENANCE_FLOOD)
				.sorted(java.util.Comparator.comparingInt(PhaseResult::round))
				.toList();
		if (foreground.isEmpty() || foreground.size() != flood.size()) {
			throw new IllegalArgumentException("Alternating benchmark phases are incomplete");
		}
		double[] p99Ratios = new double[foreground.size()];
		double[] throughputRatios = new double[foreground.size()];
		for (int index = 0; index < foreground.size(); index++) {
			if (foreground.get(index).round() != flood.get(index).round()) {
				throw new IllegalArgumentException("Alternating benchmark round pairing is incomplete");
			}
			OperationResult baseline = foreground.get(index).operation(Operation.FOREGROUND);
			OperationResult mixed = flood.get(index).operation(Operation.FOREGROUND);
			p99Ratios[index] = baseline.p99Nanos() > 0L
					? mixed.p99Nanos() / (double) baseline.p99Nanos() : Double.POSITIVE_INFINITY;
			throughputRatios[index] = baseline.throughput() > 0.0d
					? mixed.throughput() / baseline.throughput() : 0.0d;
		}
		var progressedProfiles = EnumSet.noneOf(WorkloadProfile.class);
		if (successes(flood, Operation.CONTROL) > 0L) {
			progressedProfiles.add(WorkloadProfile.CONTROL);
		}
		if (successes(flood, Operation.POINT_READ) + successes(flood, Operation.FIRST_LAST) > 0L) {
			progressedProfiles.add(WorkloadProfile.LATENCY);
		}
		if (successes(flood, Operation.ANALYTICAL_READ) > 0L) {
			progressedProfiles.add(WorkloadProfile.ANALYTICAL);
		}
		if (successes(flood, Operation.FOREGROUND_WRITE) > 0L) {
			progressedProfiles.add(WorkloadProfile.INGEST);
		}
		if (successes(flood, Operation.CDC) > 0L) {
			progressedProfiles.add(WorkloadProfile.CDC);
		}
		if (successes(flood, Operation.MAINTENANCE_WRITE) > 0L) {
			progressedProfiles.add(WorkloadProfile.BATCH);
		}
		if (successes(flood, Operation.PHYSICAL) > 0L) {
			progressedProfiles.add(WorkloadProfile.PHYSICAL_MAINTENANCE);
		}
		SchedulerConservation schedulerConservation = aggregateSchedulerConservation(phases);
			return new GateInput(
					deadlines(phases, Operation.FOREGROUND),
					deadlines(phases, Operation.FIRST_LAST),
					unexpectedDeadlines(phases),
				ratioConfidenceInterval(p99Ratios),
				ratioConfidenceInterval(throughputRatios),
				cancellations(flood, Operation.CANCELLATION),
				phases.stream().allMatch(phase -> phase.resources().drained()),
				phases.stream().mapToLong(PhaseResult::unexpectedErrors).sum(),
				phases.stream().mapToLong(phase -> phase.admission().foregroundRejected()).sum(),
				combineRequestAccounting(foreground),
				combineRequestAccounting(flood),
				integrity,
				Set.copyOf(progressedProfiles),
				combinePoolUtilization(flood, RWScheduler.Pool.READ),
				combinePoolUtilization(flood, RWScheduler.Pool.WRITE),
				schedulerConservation,
				schedulerConservation,
					new PriorityEvidence(
							maximumSchedulerMetric(foreground, WorkloadProfile.LATENCY, false),
							maximumSchedulerMetric(flood, WorkloadProfile.LATENCY, true),
							medianSchedulerMetric(flood, WorkloadProfile.LATENCY, true),
							medianSchedulerMetric(flood, WorkloadProfile.ANALYTICAL, true),
							medianSchedulerMetric(flood, WorkloadProfile.INGEST, true),
							medianSchedulerMetric(flood, WorkloadProfile.BATCH, true),
							flood.size(),
							orderedSchedulerRounds(flood, WorkloadProfile.LATENCY, WorkloadProfile.ANALYTICAL),
							orderedSchedulerRounds(flood, WorkloadProfile.INGEST, WorkloadProfile.BATCH)),
				phases.stream().allMatch(phase -> phase.runtimeTelemetry().available()),
				nativeLeaksDetected,
				shutdownClean);
	}

	private static long successes(List<PhaseResult> phases, Operation operation) {
		return phases.stream().mapToLong(phase -> phase.operation(operation).successes()).sum();
	}

	private static long deadlines(List<PhaseResult> phases, Operation operation) {
		return phases.stream().mapToLong(phase -> phase.operation(operation).deadlines()).sum();
	}

	private static long unexpectedDeadlines(List<PhaseResult> phases) {
		long deadlines = 0L;
		for (PhaseResult phase : phases) {
			for (Operation operation : Operation.values()) {
				if (operation != Operation.FOREGROUND && operation != Operation.CANCELLATION) {
					deadlines += phase.operation(operation).deadlines();
				}
			}
		}
		return deadlines;
	}

	private static long cancellations(List<PhaseResult> phases, Operation operation) {
		return phases.stream().mapToLong(phase -> phase.operation(operation).cancellations()).sum();
	}

	private static RequestAccounting combineRequestAccounting(List<PhaseResult> phases) {
		long submitted = 0L;
		long terminal = 0L;
		long inFlight = 0L;
		long duplicateTerminal = 0L;
		long maximumInFlight = 0L;
		var statuses = new EnumMap<Status.Code, Long>(Status.Code.class);
		for (Status.Code code : Status.Code.values()) {
			statuses.put(code, 0L);
		}
		for (PhaseResult phase : phases) {
			RequestAccounting accounting = phase.requestAccounting();
			submitted += accounting.submitted();
			terminal += accounting.terminal();
			inFlight += accounting.inFlight();
			duplicateTerminal += accounting.duplicateTerminal();
			maximumInFlight = Math.max(maximumInFlight, accounting.maximumInFlight());
			for (Status.Code code : Status.Code.values()) {
				statuses.put(code, statuses.get(code) + accounting.statuses().getOrDefault(code, 0L));
			}
		}
		return new RequestAccounting(submitted,
				terminal,
				inFlight,
				duplicateTerminal,
				maximumInFlight,
				Map.copyOf(statuses));
	}

	private static PoolUtilization combinePoolUtilization(List<PhaseResult> phases, RWScheduler.Pool pool) {
		int workerCount = -1;
		long samples = 0L;
		long eligibleBacklogSamples = 0L;
		long policyLimitedBacklogSamples = 0L;
		long saturatingDemandSamples = 0L;
		long fullyBusySamples = 0L;
		long idleWorkerSlots = 0L;
		int maximumActive = 0;
		int maximumConsecutiveIdle = 0;
		boolean exactWaitingWorkerEvidence = true;
		long samplePeriodNanos = -1L;
		for (PhaseResult phase : phases) {
			PoolUtilization utilization = phase.admission().poolUtilization().get(pool);
			if (workerCount < 0) {
				workerCount = utilization.workerCount();
			} else if (workerCount != utilization.workerCount()) {
				throw new IllegalArgumentException("Pool worker count changed between benchmark rounds: " + pool);
			}
			samples += utilization.samples();
			eligibleBacklogSamples += utilization.eligibleBacklogSamples();
			policyLimitedBacklogSamples += utilization.policyLimitedBacklogSamples();
			saturatingDemandSamples += utilization.saturatingDemandSamples();
			fullyBusySamples += utilization.fullyBusySamples();
			idleWorkerSlots += utilization.idleWorkerSlots();
			maximumActive = Math.max(maximumActive, utilization.maximumActive());
			maximumConsecutiveIdle = Math.max(maximumConsecutiveIdle,
					utilization.maximumConsecutiveAvoidableIdleSamples());
			exactWaitingWorkerEvidence &= utilization.exactWaitingWorkerEvidence();
			if (samplePeriodNanos < 0L) {
				samplePeriodNanos = utilization.samplePeriodNanos();
			} else if (samplePeriodNanos != utilization.samplePeriodNanos()) {
				throw new IllegalArgumentException("Admission sampling period changed between benchmark rounds");
			}
		}
		double ratio = saturatingDemandSamples == 0L || workerCount <= 0
				? 0.0d
				: 1.0d - idleWorkerSlots / (double) (saturatingDemandSamples * workerCount);
		return new PoolUtilization(workerCount,
				samples,
				eligibleBacklogSamples,
				policyLimitedBacklogSamples,
				saturatingDemandSamples,
				fullyBusySamples,
				idleWorkerSlots,
				maximumActive,
				maximumConsecutiveIdle,
				Math.max(0.0d, Math.min(1.0d, ratio)),
				exactWaitingWorkerEvidence,
				samplePeriodNanos);
	}

	private static SchedulerConservation aggregateSchedulerConservation(List<PhaseResult> phases) {
		long accepted = 0L;
		long started = 0L;
		long completed = 0L;
		long terminalOutcomes = 0L;
		long failures = 0L;
		var imbalances = new ArrayList<String>();
		for (PhaseResult phase : phases) {
			SchedulerConservation conservation = phase.resources().schedulerConservation();
			accepted = Math.max(accepted, conservation.accepted());
			started = Math.max(started, conservation.started());
			completed = Math.max(completed, conservation.completed());
			terminalOutcomes = Math.max(terminalOutcomes, conservation.terminalOutcomes());
			failures = Math.max(failures, conservation.failures());
			if (!conservation.conserved()) {
				imbalances.add("round=" + phase.round() + ", phase=" + phase.phase().value + ", "
						+ schedulerConservationDetail(conservation));
			}
		}
		return new SchedulerConservation(accepted,
				started,
				completed,
				terminalOutcomes,
				failures,
				List.copyOf(imbalances));
	}

	private static long maximumSchedulerMetric(List<PhaseResult> phases,
			WorkloadProfile profile,
			boolean queue) {
		long maximum = 0L;
		for (PhaseResult phase : phases) {
			var metrics = phase.schedulerMetrics().get(profile);
			long value = queue ? metrics.queueP99Nanos() : metrics.executionP99Nanos();
			if (value <= 0L) {
				return 0L;
			}
			maximum = Math.max(maximum, value);
		}
		return maximum;
	}

	private static long medianSchedulerMetric(List<PhaseResult> phases,
			WorkloadProfile profile,
			boolean queue) {
		long[] values = new long[phases.size()];
		int index = 0;
		for (PhaseResult phase : phases) {
			var metrics = phase.schedulerMetrics().get(profile);
			long value = queue ? metrics.queueP99Nanos() : metrics.executionP99Nanos();
			if (value <= 0L) {
				return 0L;
			}
			values[index++] = value;
		}
		Arrays.sort(values);
		return values.length == 0 ? 0L : values[(values.length - 1) / 2];
	}

	private static int orderedSchedulerRounds(List<PhaseResult> phases,
			WorkloadProfile preferred,
			WorkloadProfile deferred) {
		int ordered = 0;
		for (PhaseResult phase : phases) {
			long preferredQueue = phase.schedulerMetrics().get(preferred).queueP99Nanos();
			long deferredQueue = phase.schedulerMetrics().get(deferred).queueP99Nanos();
			if (preferredQueue > 0L && deferredQueue > 0L && preferredQueue <= deferredQueue) {
				ordered++;
			}
		}
		return ordered;
	}

	/** Mean paired ratio and two-sided 95% Student-t confidence interval. */
	public static RatioConfidenceInterval ratioConfidenceInterval(double[] samples) {
		if (samples.length == 0) {
			return new RatioConfidenceInterval(0, Double.NaN, Double.NaN, Double.NaN);
		}
		double sum = 0.0d;
		for (double sample : samples) {
			if (!Double.isFinite(sample) || sample < 0.0d) {
				return new RatioConfidenceInterval(samples.length,
						sample,
						sample,
						sample);
			}
			sum += sample;
		}
		double mean = sum / samples.length;
		if (samples.length == 1) {
			return new RatioConfidenceInterval(1, mean, mean, mean);
		}
		double squaredDeviations = 0.0d;
		for (double sample : samples) {
			double deviation = sample - mean;
			squaredDeviations += deviation * deviation;
		}
		double standardError = Math.sqrt(squaredDeviations / (samples.length - 1L)) / Math.sqrt(samples.length);
		double margin = studentTCritical95(samples.length - 1) * standardError;
		return new RatioConfidenceInterval(samples.length,
				mean,
				Math.max(0.0d, mean - margin),
				mean + margin);
	}

	private static double studentTCritical95(int degreesOfFreedom) {
		return switch (degreesOfFreedom) {
			case 1 -> 12.706d;
			case 2 -> 4.303d;
			case 3 -> 3.182d;
			case 4 -> 2.776d;
			case 5 -> 2.571d;
			case 6 -> 2.447d;
			case 7 -> 2.365d;
			case 8 -> 2.306d;
			case 9 -> 2.262d;
			case 10 -> 2.228d;
			default -> degreesOfFreedom < 30 ? 2.10d : 1.96d;
		};
	}

	/** Pure acceptance evaluation used by the opt-in runner and deterministic CI tests. */
	public static Acceptance evaluateAcceptance(GateInput input) {
		Objects.requireNonNull(input, "input");
		List<GateCheck> checks = new ArrayList<>();
		checks.add(new GateCheck("foreground_deadlines", input.foregroundDeadlines() == 0,
				"foreground deadline count=" + input.foregroundDeadlines()));
		checks.add(new GateCheck("first_last_deadlines", input.firstLastDeadlines() == 0,
				"first/last deadline count=" + input.firstLastDeadlines()));
		checks.add(new GateCheck("all_operation_deadlines", input.unexpectedDeadlines() == 0,
				"non-cancellation concrete-operation deadline count=" + input.unexpectedDeadlines()));
		var p99Ratio = input.foregroundP99Ratio();
		checks.add(new GateCheck("foreground_p99_ratio",
				p99Ratio.available(),
				"maintenance-flood/foreground-only p99 mean=" + format(p99Ratio.mean())
						+ ", 95% CI=[" + format(p99Ratio.lower95()) + ',' + format(p99Ratio.upper95())
						+ "], rounds=" + p99Ratio.samples()
						+ " (diagnostic; cross-build no-regression gate is authoritative)"));
		var throughputRatio = input.foregroundThroughputRatio();
		checks.add(new GateCheck("foreground_throughput_ratio",
				throughputRatio.available(),
				"maintenance-flood/foreground-only throughput mean=" + format(throughputRatio.mean())
						+ ", 95% CI=[" + format(throughputRatio.lower95()) + ','
						+ format(throughputRatio.upper95()) + "], rounds=" + throughputRatio.samples()
						+ " (diagnostic; cross-build no-regression gate is authoritative)"));
		checks.add(new GateCheck("cancellation_progress", input.cancellations() > 0,
				"cancelled queued calls=" + input.cancellations()));
		checks.add(new GateCheck("transport_request_conservation",
				input.foregroundRequests().conserved() && input.mixedRequests().conserved(),
				"foreground=" + requestAccountingDetail(input.foregroundRequests())
						+ ", mixed=" + requestAccountingDetail(input.mixedRequests())));
		checks.add(new GateCheck("round_trip_integrity", input.integrity().passed(),
				"writes=" + input.integrity().writesAcknowledged() + '/' + input.integrity().writesAttempted()
						+ ", matched_reads=" + input.integrity().readsMatched() + '/'
						+ input.integrity().readsAttempted() + ", mismatches=" + input.integrity().mismatches()
						+ ", errors=" + input.integrity().errors()));
		checks.add(new GateCheck("all_profiles_progress",
				input.progressedProfiles().containsAll(EnumSet.allOf(WorkloadProfile.class)),
				"profiles with successful end-to-end work=" + input.progressedProfiles()));
		checks.add(new GateCheck("read_pool_work_conserving",
				input.readPool().saturatedAndWorkConserving(), poolUtilizationDetail(input.readPool())));
		checks.add(new GateCheck("write_pool_work_conserving",
				input.writePool().saturatedAndWorkConserving(), poolUtilizationDetail(input.writePool())));
		checks.add(new GateCheck("scheduler_counter_conservation",
				input.foregroundScheduler().conserved() && input.mixedScheduler().conserved(),
				"foreground=" + schedulerConservationDetail(input.foregroundScheduler())
						+ ", mixed=" + schedulerConservationDetail(input.mixedScheduler())));
		checks.add(new GateCheck("priority_and_quantum_bound", input.priority().passed(),
				input.priority().detail()));
		checks.add(new GateCheck("queues_and_resources_drained", input.resourcesDrained(),
				"all queues, pending operations, transactions, iterators, and range cursors drained"));
		checks.add(new GateCheck("runtime_telemetry_available", input.runtimeTelemetryAvailable(),
				"process CPU, allocation, heap, direct-memory, RSS, GC, thread, and native-handle telemetry"));
		checks.add(new GateCheck("unexpected_errors", input.unexpectedErrors() == 0,
				"unexpected error count=" + input.unexpectedErrors()));
		checks.add(new GateCheck("foreground_rejections", input.foregroundRejections() == 0,
				"foreground rejection count=" + input.foregroundRejections()));
		checks.add(new GateCheck("native_handle_leaks", input.nativeLeaksDetected() == 0,
				"detected native-handle leaks=" + input.nativeLeaksDetected()));
		checks.add(new GateCheck("clean_shutdown", input.shutdownClean(),
				"gRPC channel, server, schedulers, metrics, and native database closed cleanly"));
		return new Acceptance(List.copyOf(checks));
	}

	private static String requestAccountingDetail(RequestAccounting accounting) {
		return "submitted=" + accounting.submitted() + ", terminal=" + accounting.terminal()
				+ ", in_flight=" + accounting.inFlight() + ", duplicate_terminal="
				+ accounting.duplicateTerminal();
	}

	private static String poolUtilizationDetail(PoolUtilization utilization) {
		return "workers=" + utilization.workerCount() + ", max_active=" + utilization.maximumActive()
				+ ", backlog_samples=" + utilization.eligibleBacklogSamples()
				+ ", policy_limited_backlog_samples=" + utilization.policyLimitedBacklogSamples()
				+ ", saturating_samples=" + utilization.saturatingDemandSamples()
				+ ", utilization=" + format(utilization.utilizationWhileBacklogged())
				+ ", max_consecutive_idle_samples=" + utilization.maximumConsecutiveAvoidableIdleSamples()
				+ ", max_avoidable_idle_ms=" + formatMillis(utilization.maximumAvoidableIdleNanos())
				+ ", exact_waiting_worker_evidence=" + utilization.exactWaitingWorkerEvidence();
	}

	private static String schedulerConservationDetail(SchedulerConservation conservation) {
		return "accepted=" + conservation.accepted() + ", started=" + conservation.started()
				+ ", completed=" + conservation.completed() + ", outcomes=" + conservation.terminalOutcomes()
				+ ", failures=" + conservation.failures() + ", imbalances=" + conservation.failuresByPool();
	}

	private static void printPhase(PhaseResult result) {
		OperationResult foreground = result.operation(Operation.FOREGROUND);
		OperationResult maintenance = result.operation(Operation.MAINTENANCE_WRITE);
		System.out.printf(Locale.ROOT,
				"round %d %s: foreground=%.1f ops/s p99=%.3fms deadlines=%d; maintenance=%.1f ops/s "
						+ "progress=%d rejected=%d; queues fg/max=%d maint/max=%d; active total/max=%d maint/max=%d; "
						+ "rpc=%d/%d conserved=%s; read/write backlog-util=%.3f/%.3f; "
						+ "cpu=%.1fns/op alloc=%.1fB/op rss-peak=%d telemetry=%s%n",
				result.round(),
				result.phase().value,
				foreground.throughput(),
				foreground.p99Nanos() / 1_000_000d,
				foreground.deadlines(),
				maintenance.throughput(),
				maintenance.successes(),
				result.admission().maintenanceRejected(),
				result.admission().maxForegroundQueue(),
				result.admission().maxMaintenanceQueue(),
				result.admission().maxTotalActive(),
				result.admission().maxMaintenanceActive(),
				result.requestAccounting().terminal(),
				result.requestAccounting().submitted(),
				result.requestAccounting().conserved(),
				result.admission().poolUtilization().get(RWScheduler.Pool.READ).utilizationWhileBacklogged(),
				result.admission().poolUtilization().get(RWScheduler.Pool.WRITE).utilizationWhileBacklogged(),
				result.cpuNanosPerOperation(),
				result.allocatedBytesPerOperation(),
				result.runtimeTelemetry().peakRssBytes(),
				result.runtimeTelemetry().available());
	}

	private static Path writeConfig(Path root, Options options) throws IOException {
		Path config = root.resolve("rockserver.conf");
		Files.writeString(config, configText(options), StandardOpenOption.CREATE_NEW);
		return config;
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
				      ingest-queue-capacity: %d
				      batch-queue-capacity: %d
				    }
				  }
				  global: {
				    enable-fast-get: true
				    ingest-behind: false
				    optimistic: false
				    spinning: %s
				    use-direct-io: %s
				    maximum-open-files: -1
				    fallback-column-options: {
				      cache-index-and-filter-blocks: true
				      block-size: "16KiB"
				      write-buffer-size: "%s"
				    }
				  }
				}
				""".formatted(
				escapeHocon(options.databaseName()),
				options.readParallelism(),
				options.writeParallelism(),
				options.foregroundQueueCapacity(),
				options.maintenanceQueueCapacity(),
				options.spinning(),
				options.directIo(),
				escapeHocon(options.writeBufferSize()));
	}

	/** Generated HOCON exposed for deterministic configuration-contract tests. */
	public static String generatedConfigForTesting() {
		return configText(Options.parse(new String[] {"--smoke=true"}));
	}

	private static String escapeHocon(String value) {
		return value.replace("\\", "\\\\").replace("\"", "\\\"");
	}

	private static void writeMetadata(Path output,
			Options options,
			RunEnvironment environment,
			Instant started) throws IOException {
		List<String> lines = new ArrayList<>();
		lines.add("schema=" + RESULT_SCHEMA);
		lines.add("started=" + started);
		lines.add("build_id=" + options.buildId());
		lines.add("build_state=" + options.buildState());
		lines.add("storage_label=" + options.storageLabel());
		lines.add("cache_state=" + options.cacheState());
		lines.add("host_state=" + options.hostState());
		lines.add("dataset_fingerprint=" + datasetFingerprint(options));
		lines.add("comparison_fingerprint=" + comparisonFingerprint(options));
		lines.add("environment_fingerprint=" + environmentFingerprint(environment));
		lines.add("java_version=" + environment.javaVersion());
		lines.add("java_vm=" + environment.javaVm());
		lines.add("java_vendor=" + environment.javaVendor());
		lines.add("java_home=" + environment.javaHome());
		lines.add("java_library_path=" + environment.javaLibraryPath());
		lines.add("jvm_arguments=" + environment.jvmArguments());
		lines.add("jvm_max_memory_bytes=" + environment.jvmMaxMemoryBytes());
		lines.add("os=" + environment.os());
		lines.add("available_processors=" + environment.availableProcessors());
		lines.add("cpu_model=" + environment.cpuModel());
		lines.add("system_load_average=" + environment.systemLoadAverage());
		lines.add("host_memory_total_bytes=" + environment.hostMemory().totalBytes());
		lines.add("host_memory_available_bytes=" + environment.hostMemory().availableBytes());
		lines.add("host_swap_total_bytes=" + environment.hostMemory().swapTotalBytes());
		lines.add("host_swap_free_bytes=" + environment.hostMemory().swapFreeBytes());
		lines.add("storage_mount_point=" + environment.storage().mountPoint());
		lines.add("storage_source=" + environment.storage().source());
		lines.add("storage_filesystem=" + environment.storage().filesystem());
		lines.add("storage_rotational=" + environment.storage().rotational());
		lines.add("storage_model=" + environment.storage().model());
		lines.add("rocksdb_version=" + environment.rocksdbVersion());
		lines.add("dependency_classpath_sha256=" + environment.dependencyClasspathSha256());
		lines.add("harness_classpath_sha256=" + environment.harnessClasspathSha256());
		lines.add("process_id=" + environment.processId());
		lines.add("process_start=" + environment.processStart());
		lines.add("competing_benchmark_processes=" + environment.competingBenchmarkProcesses());
		lines.add("deadline_ms=" + DEADLINE_MILLIS);
		lines.add("options=" + options);
		Files.write(output, lines, StandardOpenOption.CREATE_NEW);
	}

	private static void writeReports(Path root, BenchmarkResult result) throws IOException {
		Files.writeString(root.resolve("results.json"), toJson(result),
				StandardOpenOption.CREATE_NEW);
		Files.writeString(root.resolve("results.md"), toMarkdown(result),
				StandardOpenOption.CREATE_NEW);
		if (result.phases().size() == result.options().rounds() * 2) {
			GrpcOverloadComparison.writeRunInput(
					root.resolve(GrpcOverloadComparison.RUN_INPUT_FILE), comparisonInput(result));
		}
	}

	private static GrpcOverloadComparison.RunInput comparisonInput(BenchmarkResult result) {
		List<PhaseResult> foreground = phaseResults(result.phases(), Phase.FOREGROUND_ONLY);
		List<PhaseResult> mixed = phaseResults(result.phases(), Phase.MAINTENANCE_FLOOD);
		if (foreground.size() != result.options().rounds() || mixed.size() != result.options().rounds()) {
			throw new IllegalArgumentException("Every overload comparison round requires both phases");
		}
		var metrics = new LinkedHashMap<String, Double>();
		metrics.put("foreground-only.foreground-throughput",
				geometricMean(foreground, phase -> successThroughput(phase, Operation.FOREGROUND)));
		metrics.put("mixed.foreground-throughput",
				geometricMean(mixed, phase -> successThroughput(phase, Operation.FOREGROUND)));
		metrics.put("mixed.useful-throughput", geometricMean(mixed, GrpcOverloadBenchmark::usefulThroughput));
		metrics.put("foreground-only.foreground-p99-nanos",
				geometricMean(foreground, phase -> phase.operation(Operation.FOREGROUND).p99Nanos()));
		metrics.put("mixed.foreground-p99-nanos",
				geometricMean(mixed, phase -> phase.operation(Operation.FOREGROUND).p99Nanos()));
		metrics.put("foreground-only.latency-queue-p99-nanos",
				geometricMean(foreground,
						phase -> phase.schedulerMetrics().get(WorkloadProfile.LATENCY).queueP99Nanos()));
		metrics.put("mixed.latency-queue-p99-nanos",
				geometricMean(mixed,
						phase -> phase.schedulerMetrics().get(WorkloadProfile.LATENCY).queueP99Nanos()));
		metrics.put("foreground-only.latency-execution-p99-nanos",
				geometricMean(foreground,
						phase -> phase.schedulerMetrics().get(WorkloadProfile.LATENCY).executionP99Nanos()));
		metrics.put("mixed.latency-execution-p99-nanos",
				geometricMean(mixed,
						phase -> phase.schedulerMetrics().get(WorkloadProfile.LATENCY).executionP99Nanos()));
		metrics.put("foreground-only.cpu-nanos-per-operation",
				geometricMean(foreground, PhaseResult::cpuNanosPerOperation));
		metrics.put("mixed.cpu-nanos-per-operation", geometricMean(mixed, PhaseResult::cpuNanosPerOperation));
		metrics.put("foreground-only.allocated-bytes-per-operation",
				geometricMean(foreground, PhaseResult::allocatedBytesPerOperation));
		metrics.put("mixed.allocated-bytes-per-operation",
				geometricMean(mixed, PhaseResult::allocatedBytesPerOperation));
		metrics.put("foreground-only.peak-live-heap-bytes",
				geometricMean(foreground, phase -> phase.runtimeTelemetry().peakLiveHeapBytes()));
		metrics.put("mixed.peak-live-heap-bytes",
				geometricMean(mixed, phase -> phase.runtimeTelemetry().peakLiveHeapBytes()));
		metrics.put("foreground-only.peak-direct-memory-bytes",
				geometricMean(foreground, phase -> phase.runtimeTelemetry().peakDirectMemoryBytes()));
		metrics.put("mixed.peak-direct-memory-bytes",
				geometricMean(mixed, phase -> phase.runtimeTelemetry().peakDirectMemoryBytes()));
		metrics.put("foreground-only.peak-rss-bytes",
				geometricMean(foreground, phase -> phase.runtimeTelemetry().peakRssBytes()));
		metrics.put("mixed.peak-rss-bytes",
				geometricMean(mixed, phase -> phase.runtimeTelemetry().peakRssBytes()));
		metrics.put("degradation.foreground-throughput-ratio",
				pairedPhaseGeometricRatio(foreground, mixed,
						phase -> successThroughput(phase, Operation.FOREGROUND)));
		metrics.put("degradation.foreground-p99-ratio",
				pairedPhaseGeometricRatio(foreground, mixed,
						phase -> phase.operation(Operation.FOREGROUND).p99Nanos()));
		metrics.put("foreground-only.gc-collections", sum(foreground,
				phase -> phase.runtimeTelemetry().gcCollections()));
		metrics.put("mixed.gc-collections", sum(mixed, phase -> phase.runtimeTelemetry().gcCollections()));
		metrics.put("foreground-only.gc-millis", sum(foreground,
				phase -> phase.runtimeTelemetry().gcMillis()));
		metrics.put("mixed.gc-millis", sum(mixed, phase -> phase.runtimeTelemetry().gcMillis()));
		metrics.put("foreground-only.peak-thread-count", maximum(foreground,
				phase -> phase.runtimeTelemetry().peakThreadCount()));
		metrics.put("mixed.peak-thread-count", maximum(mixed,
				phase -> phase.runtimeTelemetry().peakThreadCount()));
		metrics.put("foreground-only.peak-native-handles", maximum(foreground,
				phase -> phase.runtimeTelemetry().peakNativeHandles()));
		metrics.put("mixed.peak-native-handles", maximum(mixed,
				phase -> phase.runtimeTelemetry().peakNativeHandles()));

		RunEnvironment environment = result.environment();
		String environmentSummary = "os=" + environment.os()
				+ ";cpu=" + environment.cpuModel()
				+ ";processors=" + environment.availableProcessors()
				+ ";memory=" + environment.hostMemory().totalBytes()
				+ ";storage=" + environment.storage()
				+ ";jdk=" + environment.javaVendor() + ' ' + environment.javaVersion()
				+ ";vm=" + environment.javaVm()
				+ ";rocksdb=" + environment.rocksdbVersion()
				+ ";dependencies=" + environment.dependencyClasspathSha256()
				+ ";harness=" + environment.harnessClasspathSha256();
		return new GrpcOverloadComparison.RunInput(
				result.options().buildId(),
				result.options().buildState(),
				result.options().storageLabel(),
				result.options().cacheState(),
				result.options().hostState(),
				datasetFingerprint(result.options()),
				comparisonFingerprint(result.options()),
				environmentFingerprint(environment),
				environmentSummary,
				environment.processId(),
				environment.processStart(),
				result.started().toString(),
				result.finished().toString(),
				result.options().rounds(),
				result.options().enforce(),
				result.acceptance().passed(),
				result.integrity().passed(),
				result.phases().stream().allMatch(phase -> phase.requestAccounting().conserved()),
				result.phases().stream().allMatch(phase -> phase.resources().drained()),
				result.shutdownClean(),
				result.phases().stream().allMatch(phase -> phase.runtimeTelemetry().available()),
				result.nativeLeaksDetected(),
				cancellations(mixed, Operation.CANCELLATION),
				metrics);
	}

	private static List<PhaseResult> phaseResults(List<PhaseResult> phases, Phase phase) {
		return phases.stream()
				.filter(result -> result.phase() == phase)
				.sorted(java.util.Comparator.comparingInt(PhaseResult::round))
				.toList();
	}

	private static double successThroughput(PhaseResult phase, Operation operation) {
		return phase.operation(operation).successes() / Math.max(0.001d, phase.durationMillis() / 1_000d);
	}

	private static double usefulThroughput(PhaseResult phase) {
		long useful = phase.operations().entrySet().stream()
				.filter(entry -> entry.getKey() != Operation.FOREGROUND
						&& entry.getKey() != Operation.CANCELLATION)
				.mapToLong(entry -> entry.getValue().successes())
				.sum();
		return useful / Math.max(0.001d, phase.durationMillis() / 1_000d);
	}

	private static double geometricMean(List<PhaseResult> phases,
			ToDoubleFunction<PhaseResult> metric) {
		double logSum = 0.0d;
		for (PhaseResult phase : phases) {
			double value = metric.applyAsDouble(phase);
			if (!Double.isFinite(value) || value <= 0.0d) {
				return Double.NaN;
			}
			logSum += Math.log(value);
		}
		return Math.exp(logSum / phases.size());
	}

	private static double pairedPhaseGeometricRatio(List<PhaseResult> baseline,
			List<PhaseResult> mixed,
			ToDoubleFunction<PhaseResult> metric) {
		double logSum = 0.0d;
		for (int index = 0; index < baseline.size(); index++) {
			if (baseline.get(index).round() != mixed.get(index).round()) {
				return Double.NaN;
			}
			double first = metric.applyAsDouble(baseline.get(index));
			double second = metric.applyAsDouble(mixed.get(index));
			if (!Double.isFinite(first) || first <= 0.0d || !Double.isFinite(second) || second <= 0.0d) {
				return Double.NaN;
			}
			logSum += Math.log(second / first);
		}
		return Math.exp(logSum / baseline.size());
	}

	private static double sum(List<PhaseResult> phases, ToDoubleFunction<PhaseResult> metric) {
		return phases.stream().mapToDouble(metric).sum();
	}

	private static double maximum(List<PhaseResult> phases, ToDoubleFunction<PhaseResult> metric) {
		return phases.stream().mapToDouble(metric).max().orElse(Double.NaN);
	}

	private static String toJson(BenchmarkResult result) {
		StringBuilder json = new StringBuilder(16_384);
		json.append("{\n  \"schema\": ");
		appendJsonString(json, result.schema());
		json.append(",\n  \"started\": ");
		appendJsonString(json, result.started().toString());
		json.append(",\n  \"finished\": ");
		appendJsonString(json, result.finished().toString());
		json.append(",\n  \"deadline_ms\": ").append(DEADLINE_MILLIS);
		json.append(",\n  \"dataset_fingerprint\": ");
		appendJsonString(json, datasetFingerprint(result.options()));
		json.append(",\n  \"comparison_fingerprint\": ");
		appendJsonString(json, comparisonFingerprint(result.options()));
		json.append(",\n  \"environment_fingerprint\": ");
		appendJsonString(json, environmentFingerprint(result.environment()));
		json.append(",\n  \"environment\": ");
		appendEnvironmentJson(json, result.environment());
		json.append(",\n  \"options\": ");
		appendOptionsJson(json, result.options());
		json.append(",\n  \"phases\": [");
		for (int phaseIndex = 0; phaseIndex < result.phases().size(); phaseIndex++) {
			if (phaseIndex > 0) {
				json.append(',');
			}
			appendPhaseJson(json, result.phases().get(phaseIndex));
		}
		json.append("\n  ],\n  \"integrity\": ");
		appendIntegrityJson(json, result.integrity());
		json.append(",\n  \"shutdown_clean\": ").append(result.shutdownClean());
		json.append(",\n  \"native_handle_leaks_detected\": ").append(result.nativeLeaksDetected());
		json.append(",\n  \"acceptance\": {\n    \"passed\": ").append(result.acceptance().passed());
		json.append(",\n    \"checks\": [");
		for (int index = 0; index < result.acceptance().checks().size(); index++) {
			GateCheck check = result.acceptance().checks().get(index);
			if (index > 0) {
				json.append(',');
			}
			json.append("\n      {\"name\": ");
			appendJsonString(json, check.name());
			json.append(", \"passed\": ").append(check.passed()).append(", \"detail\": ");
			appendJsonString(json, check.detail());
			json.append('}');
		}
		json.append("\n    ]\n  }\n}\n");
		return json.toString();
	}

	private static void appendOptionsJson(StringBuilder json, Options options) {
		json.append("{\n    \"root\": ");
		appendJsonString(json, options.root().toAbsolutePath().normalize().toString());
		json.append(",\n    \"database_name\": ");
		appendJsonString(json, options.databaseName());
		json.append(",\n    \"build_id\": ");
		appendJsonString(json, options.buildId());
		json.append(",\n    \"build_state\": ");
		appendJsonString(json, options.buildState());
		json.append(",\n    \"storage_label\": ");
		appendJsonString(json, options.storageLabel());
		json.append(",\n    \"cache_state\": ");
		appendJsonString(json, options.cacheState());
		json.append(",\n    \"host_state\": ");
		appendJsonString(json, options.hostState());
		json.append(",\n    \"minimum_host_available_gib\": ")
				.append(options.minimumHostAvailableGiB())
				.append(",\n    \"preload_keys\": ").append(options.preloadKeys())
				.append(",\n    \"preload_flush_keys\": ").append(options.preloadFlushKeys())
				.append(",\n    \"value_bytes\": ").append(options.valueBytes())
				.append(",\n    \"warmup_seconds\": ").append(options.warmupSeconds())
				.append(",\n    \"measure_seconds\": ").append(options.measureSeconds())
				.append(",\n    \"rounds\": ").append(options.rounds())
				.append(",\n    \"point_readers\": ").append(options.pointReaders())
				.append(",\n    \"foreground_writers\": ").append(options.foregroundWriters())
				.append(",\n    \"maintenance_writers\": ").append(options.maintenanceWriters())
				.append(",\n    \"first_last_readers\": ").append(options.firstLastReaders())
				.append(",\n    \"cancellation_workers\": ").append(options.cancellationWorkers())
				.append(",\n    \"analytical_readers\": ").append(options.analyticalReaders())
				.append(",\n    \"control_workers\": ").append(options.controlWorkers())
				.append(",\n    \"cdc_workers\": ").append(options.cdcWorkers())
				.append(",\n    \"physical_workers\": ").append(options.physicalWorkers())
				.append(",\n    \"foreground_write_rate\": ").append(options.foregroundWriteRate())
				.append(",\n    \"maintenance_write_rate\": ").append(options.maintenanceWriteRate())
				.append(",\n    \"first_last_rate\": ").append(options.firstLastRate())
				.append(",\n    \"cancellation_rate\": ").append(options.cancellationRate())
				.append(",\n    \"analytical_rate\": ").append(options.analyticalRate())
				.append(",\n    \"control_rate\": ").append(options.controlRate())
				.append(",\n    \"cdc_rate\": ").append(options.cdcRate())
				.append(",\n    \"physical_rate\": ").append(options.physicalRate())
				.append(",\n    \"cancellation_delay_ms\": ").append(options.cancellationDelayMillis())
				.append(",\n    \"cancellation_burst\": ").append(options.cancellationBurst())
				.append(",\n    \"integrity_requests\": ").append(options.integrityRequests())
				.append(",\n    \"point_request_count\": ").append(options.pointRequestCount())
				.append(",\n    \"range_request_count\": ").append(options.rangeRequestCount())
				.append(",\n    \"range_width\": ").append(options.rangeWidth())
				.append(",\n    \"read_parallelism\": ").append(options.readParallelism())
				.append(",\n    \"write_parallelism\": ").append(options.writeParallelism())
				.append(",\n    \"foreground_queue_capacity\": ")
				.append(options.foregroundQueueCapacity())
				.append(",\n    \"maintenance_queue_capacity\": ")
				.append(options.maintenanceQueueCapacity())
				.append(",\n    \"admission_sample_micros\": ").append(options.admissionSampleMicros())
				.append(",\n    \"instrumentation_mode\": ");
		appendJsonString(json, options.instrumentationMode());
		json.append(",\n    \"max_latency_samples\": ").append(options.maxLatencySamples())
				.append(",\n    \"write_buffer_size\": ");
		appendJsonString(json, options.writeBufferSize());
		json.append(",\n    \"direct_io\": ").append(options.directIo())
				.append(",\n    \"spinning\": ").append(options.spinning())
				.append(",\n    \"reuse_preloaded\": ").append(options.reusePreloaded())
				.append(",\n    \"enforce\": ").append(options.enforce())
				.append(",\n    \"smoke\": ").append(options.smoke())
				.append(",\n    \"seed\": ").append(options.seed())
				.append("\n  }");
	}

	private static void appendEnvironmentJson(StringBuilder json, RunEnvironment environment) {
		json.append("{\"java_version\": ");
		appendJsonString(json, environment.javaVersion());
		json.append(", \"java_vm\": ");
		appendJsonString(json, environment.javaVm());
		json.append(", \"java_vendor\": ");
		appendJsonString(json, environment.javaVendor());
		json.append(", \"java_home\": ");
		appendJsonString(json, environment.javaHome());
		json.append(", \"java_library_path\": ");
		appendJsonString(json, environment.javaLibraryPath());
		json.append(", \"jvm_arguments\": [");
		for (int index = 0; index < environment.jvmArguments().size(); index++) {
			if (index > 0) {
				json.append(',');
			}
			appendJsonString(json, environment.jvmArguments().get(index));
		}
		json.append("], \"jvm_max_memory_bytes\": ").append(environment.jvmMaxMemoryBytes())
				.append(", \"os\": ");
		appendJsonString(json, environment.os());
		json.append(", \"available_processors\": ").append(environment.availableProcessors())
				.append(", \"cpu_model\": ");
		appendJsonString(json, environment.cpuModel());
		json.append(", \"rocksdb_version\": ");
		appendJsonString(json, environment.rocksdbVersion());
		json.append(", \"dependency_classpath_sha256\": ");
		appendJsonString(json, environment.dependencyClasspathSha256());
		json.append(", \"harness_classpath_sha256\": ");
		appendJsonString(json, environment.harnessClasspathSha256());
		json.append(", \"process_id\": ").append(environment.processId())
				.append(", \"process_start\": ");
		appendJsonString(json, environment.processStart());
		json.append(", \"system_load_average\": ").append(format(environment.systemLoadAverage()))
				.append(", \"host_memory\": {\"total_bytes\": ")
				.append(environment.hostMemory().totalBytes())
				.append(", \"available_bytes\": ").append(environment.hostMemory().availableBytes())
				.append(", \"swap_total_bytes\": ").append(environment.hostMemory().swapTotalBytes())
				.append(", \"swap_free_bytes\": ").append(environment.hostMemory().swapFreeBytes())
				.append("}, \"storage\": {\"mount_point\": ");
		appendJsonString(json, environment.storage().mountPoint());
		json.append(", \"source\": ");
		appendJsonString(json, environment.storage().source());
		json.append(", \"filesystem\": ");
		appendJsonString(json, environment.storage().filesystem());
		json.append(", \"rotational\": ").append(environment.storage().rotational())
				.append(", \"model\": ");
		appendJsonString(json, environment.storage().model());
		json.append("}, \"competing_benchmark_processes\": [");
		for (int index = 0; index < environment.competingBenchmarkProcesses().size(); index++) {
			if (index > 0) {
				json.append(',');
			}
			appendJsonString(json, environment.competingBenchmarkProcesses().get(index));
		}
		json.append("]}");
	}

	private static void appendPhaseJson(StringBuilder json, PhaseResult phase) {
		json.append("\n    {\n      \"phase\": ");
		appendJsonString(json, phase.phase().value);
		json.append(",\n      \"round\": ").append(phase.round());
		json.append(",\n      \"duration_ms\": ").append(phase.durationMillis());
		json.append(",\n      \"warmup_error_count\": ").append(phase.warmupErrors());
		json.append(",\n      \"operations\": {");
		int operationIndex = 0;
		for (Operation operation : Operation.values()) {
			if (operationIndex++ > 0) {
				json.append(',');
			}
			json.append("\n        ");
			appendJsonString(json, operation.value);
			json.append(": ");
			appendOperationJson(json, phase.operation(operation));
		}
		json.append("\n      },\n      \"admission\": ");
		appendAdmissionJson(json, phase.admission());
		json.append(",\n      \"resources_after_drain\": ");
		appendResourcesJson(json, phase.resources());
		json.append(",\n      \"request_accounting\": ");
		appendRequestAccountingJson(json, phase.requestAccounting());
		json.append(",\n      \"scheduler_profiles\": {");
		int profileIndex = 0;
		for (WorkloadProfile profile : WorkloadProfile.values()) {
			if (profileIndex++ > 0) {
				json.append(',');
			}
			var metrics = phase.schedulerMetrics().get(profile);
			json.append("\n        \"").append(profile.name().toLowerCase(Locale.ROOT)).append("\": {")
					.append("\"queue_p99_ns\": ").append(metrics.queueP99Nanos())
					.append(", \"execution_p99_ns\": ").append(metrics.executionP99Nanos()).append('}');
		}
		json.append("\n      },\n      \"runtime_telemetry\": ");
		appendRuntimeTelemetryJson(json, phase);
		json.append("\n    }");
	}

	private static void appendRuntimeTelemetryJson(StringBuilder json, PhaseResult phase) {
		RuntimeTelemetry telemetry = phase.runtimeTelemetry();
		json.append("{\"process_cpu_ns\": ").append(telemetry.processCpuNanos())
				.append(", \"cpu_ns_per_operation\": ").append(format(phase.cpuNanosPerOperation()))
				.append(", \"allocated_bytes\": ").append(telemetry.allocatedBytes())
				.append(", \"allocated_bytes_per_operation\": ")
				.append(format(phase.allocatedBytesPerOperation()))
				.append(", \"observer_cpu_ns\": ").append(telemetry.observerCpuNanos())
				.append(", \"observer_allocated_bytes\": ").append(telemetry.observerAllocatedBytes())
				.append(", \"gc_collections\": ").append(telemetry.gcCollections())
				.append(", \"gc_millis\": ").append(telemetry.gcMillis())
				.append(", \"peak_live_heap_bytes\": ").append(telemetry.peakLiveHeapBytes())
				.append(", \"peak_direct_memory_bytes\": ").append(telemetry.peakDirectMemoryBytes())
				.append(", \"peak_rss_bytes\": ").append(telemetry.peakRssBytes())
				.append(", \"peak_thread_count\": ").append(telemetry.peakThreadCount())
				.append(", \"peak_native_handles\": ").append(telemetry.peakNativeHandles())
				.append(", \"sample_period_ns\": ").append(RUNTIME_RESOURCE_SAMPLE_NANOS)
				.append(", \"available\": ").append(telemetry.available()).append('}');
	}

	private static void appendOperationJson(StringBuilder json, OperationResult operation) {
		json.append("{\"completed\": ").append(operation.completed())
				.append(", \"successes\": ").append(operation.successes())
				.append(", \"throughput\": ").append(format(operation.throughput()))
				.append(", \"p50_ns\": ").append(operation.p50Nanos())
				.append(", \"p95_ns\": ").append(operation.p95Nanos())
				.append(", \"p99_ns\": ").append(operation.p99Nanos())
				.append(", \"max_ns\": ").append(operation.maxNanos())
				.append(", \"deadlines\": ").append(operation.deadlines())
				.append(", \"rejections\": ").append(operation.rejections())
				.append(", \"cancellations\": ").append(operation.cancellations())
				.append(", \"errors\": ").append(operation.errors())
				.append(", \"latency_samples\": ").append(operation.latencySamples())
				.append(", \"sample_overflow\": ").append(operation.sampleOverflow())
				.append('}');
	}

	private static void appendAdmissionJson(StringBuilder json, AdmissionResult admission) {
		json.append("{\"foreground\": {\"max_queue_depth\": ")
				.append(admission.maxForegroundQueue())
				.append(", \"ending_queue_depth\": ").append(admission.endForegroundQueue())
				.append(", \"max_active\": ").append(admission.maxForegroundActive())
				.append(", \"rejected\": ").append(admission.foregroundRejected())
				.append("}, \"maintenance\": {\"max_queue_depth\": ")
				.append(admission.maxMaintenanceQueue())
				.append(", \"ending_queue_depth\": ").append(admission.endMaintenanceQueue())
				.append(", \"max_active\": ").append(admission.maxMaintenanceActive())
				.append(", \"rejected\": ").append(admission.maintenanceRejected())
				.append("}, \"max_total_active\": ").append(admission.maxTotalActive())
				.append(", \"pools\": {");
		int index = 0;
		for (RWScheduler.Pool pool : RWScheduler.Pool.values()) {
			if (index++ > 0) {
				json.append(',');
			}
			json.append('"').append(pool.name().toLowerCase(Locale.ROOT)).append("\": ");
			appendPoolUtilizationJson(json, admission.poolUtilization().get(pool));
		}
		json.append("}}");
	}

	private static void appendResourcesJson(StringBuilder json, ResourceResult resources) {
		json.append("{\"foreground_queued\": ").append(resources.foregroundQueued())
				.append(", \"maintenance_queued\": ").append(resources.maintenanceQueued())
				.append(", \"foreground_active\": ").append(resources.foregroundActive())
				.append(", \"maintenance_active\": ").append(resources.maintenanceActive())
				.append(", \"total_queued\": ").append(resources.totalQueued())
				.append(", \"total_active\": ").append(resources.totalActive())
				.append(", \"pending_operations\": ").append(resources.pendingOperations())
				.append(", \"open_transactions\": ").append(resources.openTransactions())
				.append(", \"open_iterators\": ").append(resources.openIterators())
				.append(", \"active_range_cursors\": ").append(resources.activeRangeCursors())
				.append(", \"retained_range_snapshots\": ").append(resources.retainedRangeSnapshots())
				.append(", \"retained_range_permits\": ").append(resources.retainedRangePermits())
				.append(", \"retained_range_waiters\": ").append(resources.retainedRangeWaiters())
				.append(", \"active_cdc_poll_cursors\": ").append(resources.activeCdcPollCursors())
				.append(", \"active_exists_multi_requests\": ").append(resources.activeExistsMultiRequests())
				.append(", \"iterator_leases\": ").append(resources.iteratorLeases())
				.append(", \"drain_ms\": ").append(resources.drainMillis())
				.append(", \"drained\": ").append(resources.drained())
				.append(", \"scheduler_conservation\": ");
		appendSchedulerConservationJson(json, resources.schedulerConservation());
		json.append('}');
	}

	private static void appendRequestAccountingJson(StringBuilder json, RequestAccounting accounting) {
		json.append("{\"submitted\": ").append(accounting.submitted())
				.append(", \"terminal\": ").append(accounting.terminal())
				.append(", \"in_flight\": ").append(accounting.inFlight())
				.append(", \"duplicate_terminal\": ").append(accounting.duplicateTerminal())
				.append(", \"maximum_in_flight\": ").append(accounting.maximumInFlight())
				.append(", \"conserved\": ").append(accounting.conserved())
				.append(", \"statuses\": {");
		int index = 0;
		for (Status.Code code : Status.Code.values()) {
			long count = accounting.statuses().getOrDefault(code, 0L);
			if (count == 0L) {
				continue;
			}
			if (index++ > 0) {
				json.append(',');
			}
			json.append('"').append(code.name().toLowerCase(Locale.ROOT)).append("\": ").append(count);
		}
		json.append("}}");
	}

	private static void appendIntegrityJson(StringBuilder json, IntegrityResult integrity) {
		json.append("{\"writes_attempted\": ").append(integrity.writesAttempted())
				.append(", \"writes_acknowledged\": ").append(integrity.writesAcknowledged())
				.append(", \"reads_attempted\": ").append(integrity.readsAttempted())
				.append(", \"reads_matched\": ").append(integrity.readsMatched())
				.append(", \"mismatches\": ").append(integrity.mismatches())
				.append(", \"errors\": ").append(integrity.errors())
				.append(", \"passed\": ").append(integrity.passed())
				.append(", \"request_accounting\": ");
		appendRequestAccountingJson(json, integrity.requestAccounting());
		json.append('}');
	}

	private static void appendPoolUtilizationJson(StringBuilder json, PoolUtilization utilization) {
			json.append("{\"worker_count\": ").append(utilization.workerCount())
					.append(", \"samples\": ").append(utilization.samples())
					.append(", \"eligible_backlog_samples\": ").append(utilization.eligibleBacklogSamples())
					.append(", \"policy_limited_backlog_samples\": ")
					.append(utilization.policyLimitedBacklogSamples())
					.append(", \"saturating_demand_samples\": ").append(utilization.saturatingDemandSamples())
				.append(", \"fully_busy_samples\": ").append(utilization.fullyBusySamples())
				.append(", \"idle_worker_slots\": ").append(utilization.idleWorkerSlots())
				.append(", \"maximum_active\": ").append(utilization.maximumActive())
				.append(", \"maximum_consecutive_avoidable_idle_samples\": ")
				.append(utilization.maximumConsecutiveAvoidableIdleSamples())
				.append(", \"utilization_while_backlogged\": ")
				.append(format(utilization.utilizationWhileBacklogged()))
				.append(", \"sample_period_ns\": ").append(utilization.samplePeriodNanos())
				.append(", \"maximum_avoidable_idle_ns\": ").append(utilization.maximumAvoidableIdleNanos())
				.append(", \"exact_waiting_worker_evidence\": ")
				.append(utilization.exactWaitingWorkerEvidence())
				.append(", \"passed\": ").append(utilization.saturatedAndWorkConserving()).append('}');
	}

	private static void appendSchedulerConservationJson(StringBuilder json,
			SchedulerConservation conservation) {
		json.append("{\"accepted\": ").append(conservation.accepted())
				.append(", \"started\": ").append(conservation.started())
				.append(", \"completed\": ").append(conservation.completed())
				.append(", \"terminal_outcomes\": ").append(conservation.terminalOutcomes())
				.append(", \"failures\": ").append(conservation.failures())
				.append(", \"conserved\": ").append(conservation.conserved())
				.append(", \"imbalances\": [");
		for (int index = 0; index < conservation.failuresByPool().size(); index++) {
			if (index > 0) {
				json.append(',');
			}
			appendJsonString(json, conservation.failuresByPool().get(index));
		}
		json.append("]}");
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
				default -> {
					if (character < 0x20) {
						json.append("\\u%04x".formatted((int) character));
					} else {
						json.append(character);
					}
				}
			}
		}
		json.append('"');
	}

	private static String toMarkdown(BenchmarkResult result) {
		StringBuilder markdown = new StringBuilder(12_000);
		markdown.append("# Rockserver gRPC overload benchmark\n\n")
				.append("- Started: `").append(result.started()).append("`\n")
				.append("- Finished: `").append(result.finished()).append("`\n")
				.append("- Build: `").append(result.options().buildId()).append("` (`")
				.append(result.options().buildState()).append("`)\n")
				.append("- Storage/cache: `").append(result.options().storageLabel()).append("` / `")
				.append(result.options().cacheState()).append("`\n")
				.append("- Host isolation assertion: `").append(result.options().hostState()).append("`\n")
				.append("- Resolved storage: `").append(result.environment().storage().source()).append("` on `")
				.append(result.environment().storage().filesystem()).append("` at `")
				.append(result.environment().storage().mountPoint()).append("`, rotational `")
				.append(result.environment().storage().rotational()).append("`, model `")
				.append(result.environment().storage().model()).append("`\n")
				.append("- Dataset fingerprint: `").append(datasetFingerprint(result.options())).append("`\n")
				.append("- Comparison fingerprint: `").append(comparisonFingerprint(result.options())).append("`\n")
				.append("- Environment fingerprint: `").append(environmentFingerprint(result.environment()))
				.append("`\n")
				.append("- JVM: `").append(result.environment().javaVersion()).append("`, max memory `")
				.append(result.environment().jvmMaxMemoryBytes()).append(" bytes`\n")
				.append("- CPU/native/dependencies: `").append(result.environment().cpuModel()).append("` / RocksDB `")
				.append(result.environment().rocksdbVersion()).append("` / classpath `")
				.append(result.environment().dependencyClasspathSha256()).append("` / harness `")
				.append(result.environment().harnessClasspathSha256()).append("`\n")
				.append("- Process: `").append(result.environment().processId()).append("` started `")
				.append(result.environment().processStart()).append("`\n")
				.append("- Host memory at preflight: `")
				.append(result.environment().hostMemory().availableBytes()).append(" / ")
				.append(result.environment().hostMemory().totalBytes()).append(" bytes available/total`, swap free `")
				.append(result.environment().hostMemory().swapFreeBytes()).append(" bytes`\n")
				.append("- Competing benchmark processes at preflight: `")
				.append(result.environment().competingBenchmarkProcesses()).append("`\n")
				.append("- Fixed client/range deadline: `").append(DEADLINE_MILLIS).append(" ms`\n")
				.append("- Preloaded keys: `").append(result.options().preloadKeys()).append("`\n")
				.append("- Queue capacities: INGEST `").append(result.options().foregroundQueueCapacity())
				.append("`, BATCH `").append(result.options().maintenanceQueueCapacity()).append("`\n\n")
				.append("## Operations\n\n")
				.append("| Round | Phase | Operation | Throughput/s | p50 ms | p95 ms | p99 ms | max ms | Deadlines | Rejected | Cancelled | Errors |\n")
				.append("|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
		for (PhaseResult phase : result.phases()) {
			for (Operation operation : Operation.values()) {
				OperationResult stats = phase.operation(operation);
				markdown.append('|').append(phase.round())
						.append('|').append(phase.phase().value)
						.append('|').append(operation.value)
						.append('|').append(format(stats.throughput()))
						.append('|').append(formatMillis(stats.p50Nanos()))
						.append('|').append(formatMillis(stats.p95Nanos()))
						.append('|').append(formatMillis(stats.p99Nanos()))
						.append('|').append(formatMillis(stats.maxNanos()))
						.append('|').append(stats.deadlines())
						.append('|').append(stats.rejections())
						.append('|').append(stats.cancellations())
						.append('|').append(stats.errors()).append("|\n");
			}
		}
		markdown.append("\n## Runtime telemetry\n\n")
				.append("| Round | Phase | CPU ns/op | Allocated B/op | Observer CPU ns | Observer allocated bytes | Peak heap bytes | Peak direct bytes | Peak RSS bytes | GC count/ms | Peak threads | Peak native handles | Available |\n")
				.append("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n");
		for (PhaseResult phase : result.phases()) {
			RuntimeTelemetry telemetry = phase.runtimeTelemetry();
			markdown.append('|').append(phase.round())
					.append('|').append(phase.phase().value)
					.append('|').append(format(phase.cpuNanosPerOperation()))
					.append('|').append(format(phase.allocatedBytesPerOperation()))
					.append('|').append(telemetry.observerCpuNanos())
					.append('|').append(telemetry.observerAllocatedBytes())
					.append('|').append(telemetry.peakLiveHeapBytes())
					.append('|').append(telemetry.peakDirectMemoryBytes())
					.append('|').append(telemetry.peakRssBytes())
					.append('|').append(telemetry.gcCollections()).append('/').append(telemetry.gcMillis())
					.append('|').append(telemetry.peakThreadCount())
					.append('|').append(telemetry.peakNativeHandles())
					.append('|').append(telemetry.available()).append("|\n");
		}
		markdown.append("\n## Admission and drain\n\n")
				.append("| Round | Phase | FG queue max/end | Maintenance queue max/end | FG active max | Maintenance active max | Total active max | FG rejected | Maintenance rejected | Drain ms | Drained |\n")
				.append("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|\n");
		for (PhaseResult phase : result.phases()) {
			AdmissionResult admission = phase.admission();
			markdown.append('|').append(phase.round())
					.append('|').append(phase.phase().value)
					.append('|').append(admission.maxForegroundQueue()).append('/')
					.append(admission.endForegroundQueue())
					.append('|').append(admission.maxMaintenanceQueue()).append('/')
					.append(admission.endMaintenanceQueue())
					.append('|').append(admission.maxForegroundActive())
					.append('|').append(admission.maxMaintenanceActive())
					.append('|').append(admission.maxTotalActive())
					.append('|').append(admission.foregroundRejected())
					.append('|').append(admission.maintenanceRejected())
					.append('|').append(phase.resources().drainMillis())
					.append('|').append(phase.resources().drained()).append("|\n");
		}
		markdown.append("\n## Request conservation and scheduler utilization\n\n")
				.append("| Round | Phase | RPC submitted/terminal | In flight | Duplicate terminal | Pool | Eligible/policy-limited/saturating samples | Max active/workers | Saturating-demand utilization | Max consecutive avoidable-idle samples |\n")
				.append("|---:|---|---:|---:|---:|---|---:|---:|---:|---:|\n");
		for (PhaseResult phase : result.phases()) {
			for (RWScheduler.Pool pool : List.of(RWScheduler.Pool.READ, RWScheduler.Pool.WRITE)) {
				var accounting = phase.requestAccounting();
				var utilization = phase.admission().poolUtilization().get(pool);
				markdown.append('|').append(phase.round())
						.append('|').append(phase.phase().value)
						.append('|').append(accounting.submitted()).append('/').append(accounting.terminal())
						.append('|').append(accounting.inFlight())
						.append('|').append(accounting.duplicateTerminal())
						.append('|').append(pool)
						.append('|').append(utilization.eligibleBacklogSamples()).append('/')
						.append(utilization.policyLimitedBacklogSamples()).append('/')
						.append(utilization.saturatingDemandSamples())
						.append('|').append(utilization.maximumActive()).append('/').append(utilization.workerCount())
						.append('|').append(format(utilization.utilizationWhileBacklogged()))
						.append('|').append(utilization.maximumConsecutiveAvoidableIdleSamples()).append("|\n");
			}
		}
		markdown.append("\nUnique integrity probe: **")
				.append(result.integrity().passed() ? "PASS" : "FAIL")
				.append("**; acknowledged writes `").append(result.integrity().writesAcknowledged())
				.append('/').append(result.integrity().writesAttempted())
				.append("`, matching reads `").append(result.integrity().readsMatched())
				.append('/').append(result.integrity().readsAttempted())
				.append("`, mismatches `").append(result.integrity().mismatches())
				.append("`, errors `").append(result.integrity().errors()).append("`.\n");
		markdown.append("\n## Acceptance\n\n");
		for (GateCheck check : result.acceptance().checks()) {
			markdown.append("- [").append(check.passed() ? 'x' : ' ').append("] `")
					.append(check.name()).append("`: ").append(check.detail()).append('\n');
		}
		markdown.append("\nOverall: **").append(result.acceptance().passed() ? "PASS" : "FAIL")
				.append("**. Clean shutdown: **").append(result.shutdownClean())
				.append("**. Native-handle leaks detected: **").append(result.nativeLeaksDetected())
				.append("**.\n");
		return markdown.toString();
	}

	private static String formatMillis(long nanos) {
		return format(nanos / 1_000_000d);
	}

	private static String format(double value) {
		if (!Double.isFinite(value)) {
			return value > 0 ? "null" : "0.000";
		}
		return String.format(Locale.ROOT, "%.3f", value);
	}

	private static void printUsage() {
		System.out.println("""
				Compile test sources and build a direct-launch classpath:
				  mvn -q -DskipTests test-compile dependency:build-classpath \
				    -Dmdep.outputFile=target/overload-benchmark.classpath

				Prepare the one-shot gate on a fresh database path from a clean release-candidate checkout:
				  java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \
				    -cp "target/test-classes:target/classes:$(<target/overload-benchmark.classpath)" \
				    it.cavallium.rockserver.core.impl.benchmark.GrpcOverloadBenchmark \
				    --root=/mnt/rockserver-hdd/grpc-mixed-RC-SHA \
				    --build-id=<full-lowercase-RC-SHA> --build-state=clean \
				    --storage-label=hdd-btrfs --cache-state=unknown --host-state=dedicated \
				    --prepare-only=true

				Drop the host page cache after preparation closes, then rerun with identical provenance,
				workload, and database options plus --cache-state=cold --reuse-preloaded=true. The second
				process verifies the dataset, build, workload, and exact config, then atomically consumes
				the root before RocksDB opens. An interrupted or failed measurement cannot be retried on it.

					The five-second deadline, 2x p99, 80% foreground-throughput, eight-millisecond
					avoidable-idle time, and eight-millisecond-plus-indivisible-call queue bounds are fixed.
					Important --name=value options:
					  build-id=<full-lowercase-RC-SHA>  build-state=clean
					  storage-label=hdd-btrfs  cache-state=cold  host-state=dedicated
					  minimum-host-available-gib=8
					  instrumentation-mode=strict
					  preload-keys=1000000  preload-flush-keys=50000  value-bytes=256
					  warmup-seconds=15  measure-seconds=60  rounds=5 (alternating phase order)
					  point-readers=40  foreground-writers=4  maintenance-writers=64
					  first-last-readers=2  cancellation-workers=2  analytical-readers=4
					  control-workers=2  cdc-workers=2  physical-workers=1
					  foreground-write-rate=1000  maintenance-write-rate=0
					  first-last-rate=50  cancellation-rate=100  analytical-rate=0 (unpaced)
					  control-rate=50  cdc-rate=100  physical-rate=1  cancellation-burst=64
					  integrity-requests=1024
				  read-parallelism=20  write-parallelism=36
				  foreground-queue-capacity=4096  maintenance-queue-capacity=512
				  point-request-count=8192  range-request-count=8192  range-width=1024
				  direct-io=false  spinning=false  enforce=true  smoke=false
					  prepare-only=false  reuse-preloaded=true

				Use --smoke=true --enforce=false for a short structural run. Smoke, dirty/unknown builds,
				warm cache, tmpfs/CI storage, short runs, and low-memory starts cannot satisfy release
				acceptance. Every attempt leaves its database, provenance, and reports for inspection.
				""");
	}

	private static Keys keys(long key) {
		return new Keys(Buf.wrap(keyBytes(key)));
	}

	private static byte[] keyBytes(long key) {
		return ByteBuffer.allocate(Long.BYTES).putLong(key).array();
	}

	private static ByteString keyByteString(long key) {
		return ByteString.copyFrom(keyBytes(key));
	}

	private static byte[] valueBytes(int size, long seed) {
		byte[] value = new byte[size];
		long random = seed;
		for (int index = 0; index < value.length; index++) {
			random ^= random << 13;
			random ^= random >>> 7;
			random ^= random << 17;
			value[index] = (byte) random;
		}
		return value;
	}

	private enum Phase {
		FOREGROUND_ONLY("foreground-only"), MAINTENANCE_FLOOD("maintenance-flood");

		private final String value;

		Phase(String value) {
			this.value = value;
		}
	}

	private enum Operation {
		FOREGROUND("foreground"),
		FOREGROUND_WRITE("foreground-write"),
		POINT_READ("point-read"),
		FIRST_LAST("first-last"),
		ANALYTICAL_READ("analytical-read"),
		CONTROL("control"),
		CDC("cdc"),
		PHYSICAL("physical"),
		MAINTENANCE_WRITE("maintenance-write"),
		CANCELLATION("cancellation");

		private final String value;

		Operation(String value) {
			this.value = value;
		}

		private boolean contributesToForeground() {
			return this == FOREGROUND_WRITE || this == POINT_READ || this == FIRST_LAST;
		}
	}

	private enum Outcome {
		SUCCESS, DEADLINE, REJECTED, CANCELLED, ERROR
	}

	private record RangeCase(GetRangeRequest request, ByteString firstKey, ByteString lastKey) {
	}

	private record Requests(GetRequest[] pointReads,
			RangeCase[] latencyRanges,
			RangeCase[] analyticalRanges,
			PutRequest[][] foregroundWrites,
			PutRequest[][] maintenanceWrites,
			PutRequest[][] cancellationWrites) {

		private static Requests create(long columnId, Options options) {
			var latencyContext = it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
					.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.LATENCY)
					.setDeadlineEpochMillis(Long.MAX_VALUE - 1L)
					.build();
			var analyticalContext = it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
					.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.ANALYTICAL)
					.setDeadlineEpochMillis(Long.MAX_VALUE - 1L)
					.build();
			GetRequest[] points = new GetRequest[Math.min(options.pointRequestCount(), options.preloadKeys())];
			for (int index = 0; index < points.length; index++) {
				long key = (long) index * options.preloadKeys() / points.length;
				points[index] = GetRequest.newBuilder()
						.setTransactionOrUpdateId(0)
						.setColumnId(columnId)
						.addKeys(keyByteString(key))
						.setContext(latencyContext)
						.build();
			}

			long rangePositions = options.preloadKeys() - options.rangeWidth() + 1L;
			int rangeCount = (int) Math.min(options.rangeRequestCount(), rangePositions);
			RangeCase[] latencyRanges = new RangeCase[rangeCount];
			RangeCase[] analyticalRanges = new RangeCase[rangeCount];
			SplittableRandom random = new SplittableRandom(options.seed());
			for (int index = 0; index < rangeCount; index++) {
				long bucketStart = (long) index * rangePositions / rangeCount;
				long bucketEnd = (long) (index + 1) * rangePositions / rangeCount;
				long start = bucketEnd > bucketStart
						? random.nextLong(bucketStart, bucketEnd)
						: bucketStart;
				long end = start + options.rangeWidth();
				ByteString first = keyByteString(start);
				ByteString last = keyByteString(end - 1);
				var baseRequest = GetRangeRequest.newBuilder()
						.setTransactionId(0)
						.setColumnId(columnId)
						.addStartKeysInclusive(first)
						.addEndKeysExclusive(keyByteString(end))
						.setTimeoutMs(DEADLINE_MILLIS)
						.setContext(latencyContext);
				latencyRanges[index] = new RangeCase(baseRequest.build(), first, last);
				analyticalRanges[index] = new RangeCase(
						baseRequest.setContext(analyticalContext).build(), first, last);
			}

			return new Requests(
					points,
					latencyRanges,
					analyticalRanges,
					writerRequests(columnId, options.foregroundWriters(), 1L << 60,
							it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.INGEST,
							options.valueBytes()),
					writerRequests(columnId, options.maintenanceWriters(), 1L << 61,
							it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH,
							options.valueBytes()),
					writerRequests(columnId, options.cancellationWorkers(), 3L << 60,
							it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH,
							options.valueBytes()));
		}

		private static PutRequest[][] writerRequests(long columnId,
				int workers,
				long keyBase,
				it.cavallium.rockserver.core.common.api.proto.WorkloadProfile profile,
				int valueBytes) {
			PutRequest[][] requests = new PutRequest[workers][WRITE_REQUEST_VARIANTS];
			for (int worker = 0; worker < workers; worker++) {
				for (int variant = 0; variant < WRITE_REQUEST_VARIANTS; variant++) {
					long key = keyBase + (long) worker * WRITE_REQUEST_VARIANTS + variant;
					requests[worker][variant] = PutRequest.newBuilder()
							.setTransactionOrUpdateId(0)
							.setColumnId(columnId)
							.setData(KV.newBuilder()
									.addKeys(keyByteString(key))
									.setValue(ByteString.copyFrom(valueBytes(valueBytes, key))))
							.setContext(it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
									.setProfile(profile)
									.setDeadlineEpochMillis(Long.MAX_VALUE))
							.build();
				}
			}
			return requests;
		}
	}

	private static final class PhaseControl {

		private final CountDownLatch ready;
		private final CountDownLatch start = new CountDownLatch(1);
		private final AtomicBoolean stop = new AtomicBoolean();
		private final AtomicBoolean measuring = new AtomicBoolean();
		private final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
		private final PhaseMetrics metrics;
		private final AdmissionTracker admission;
		private final RuntimeTelemetryTracker runtimeTelemetry;
		private volatile long measurementStartedNanos;
		private volatile long measurementStoppedNanos;

		private PhaseControl(int workerCount,
				int maxLatencySamples,
				boolean exactWaitingWorkerEvidence,
				long admissionSampleNanos,
				RuntimeTelemetryTracker runtimeTelemetry) {
			this.ready = new CountDownLatch(workerCount);
			this.metrics = new PhaseMetrics(maxLatencySamples);
			this.admission = new AdmissionTracker(exactWaitingWorkerEvidence, admissionSampleNanos);
			this.runtimeTelemetry = runtimeTelemetry;
		}

		private void startMeasurement() {
			measurementStartedNanos = System.nanoTime();
			measuring.set(true);
		}

		private long stopMeasurement() {
			measuring.set(false);
			measurementStoppedNanos = System.nanoTime();
			return Math.max(1, measurementStoppedNanos - measurementStartedNanos);
		}

		private void record(Operation operation,
				Outcome outcome,
				long latencyNanos,
				long sequence,
				String detail) {
			if (measuring.get()) {
				metrics.record(operation, outcome, latencyNanos, sequence, detail);
			} else if (!stop.get()) {
				metrics.recordWarmup(operation, outcome, detail);
			}
		}
	}

	private static final class PhaseMetrics {

		private final EnumMap<Operation, MutableOperationStats> operations = new EnumMap<>(Operation.class);
		private final LongAdder warmupErrors = new LongAdder();
		private final ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();

		private PhaseMetrics(int maxLatencySamples) {
			for (Operation operation : Operation.values()) {
				operations.put(operation, new MutableOperationStats(maxLatencySamples));
			}
		}

		private void record(Operation operation,
				Outcome outcome,
				long latencyNanos,
				long sequence,
				String detail) {
			operations.get(operation).record(outcome, latencyNanos, sequence);
			if (operation.contributesToForeground()) {
				operations.get(Operation.FOREGROUND).record(outcome, latencyNanos, sequence);
			}
			if (outcome == Outcome.ERROR) {
				recordError(operation, detail);
			}
		}

		private void recordWarmup(Operation operation, Outcome outcome, String detail) {
			boolean expected = operation == Operation.MAINTENANCE_WRITE && outcome == Outcome.REJECTED
					|| operation == Operation.CANCELLATION
					&& (outcome == Outcome.CANCELLED || outcome == Outcome.REJECTED || outcome == Outcome.SUCCESS);
			if (outcome != Outcome.SUCCESS && !expected) {
				warmupErrors.increment();
				recordError(operation, detail == null ? outcome.name() : detail);
			}
		}

		private void recordError(Operation operation, String detail) {
			if (errors.size() < MAX_RECORDED_ERRORS) {
				errors.add(operation.value + ": " + (detail == null ? "unknown failure" : detail));
			}
		}

		private PhaseResult snapshot(int round,
				Phase phase,
				long durationNanos,
				AdmissionResult admission,
				ResourceResult resources,
				RequestAccounting requestAccounting,
				Map<WorkloadProfile, SchedulerProfileMetrics> schedulerMetrics,
				RuntimeTelemetry runtimeTelemetry) {
			EnumMap<Operation, OperationResult> results = new EnumMap<>(Operation.class);
			for (Operation operation : Operation.values()) {
				results.put(operation, operations.get(operation).snapshot(durationNanos));
			}
			return new PhaseResult(round,
					phase,
					TimeUnit.NANOSECONDS.toMillis(durationNanos),
					Map.copyOf(results),
					warmupErrors.sum(),
					List.copyOf(errors),
					admission,
					resources,
					requestAccounting,
					schedulerMetrics,
					runtimeTelemetry);
		}

		private long foregroundRejections() {
			return operations.get(Operation.FOREGROUND).rejections.sum();
		}

		private long maintenanceRejections() {
			return operations.get(Operation.MAINTENANCE_WRITE).rejections.sum()
					+ operations.get(Operation.CANCELLATION).rejections.sum();
		}
	}

	private static final class MutableOperationStats {

		private final LongAdder completed = new LongAdder();
		private final LongAdder successes = new LongAdder();
		private final LongAdder deadlines = new LongAdder();
		private final LongAdder rejections = new LongAdder();
		private final LongAdder cancellations = new LongAdder();
		private final LongAdder errors = new LongAdder();
		private final LatencySamples latencySamples;

		private MutableOperationStats(int maxLatencySamples) {
			this.latencySamples = new LatencySamples(maxLatencySamples);
		}

		private void record(Outcome outcome, long latencyNanos, long sequence) {
			completed.increment();
			switch (outcome) {
				case SUCCESS -> successes.increment();
				case DEADLINE -> deadlines.increment();
				case REJECTED -> rejections.increment();
				case CANCELLED -> cancellations.increment();
				case ERROR -> errors.increment();
			}
			latencySamples.record(latencyNanos, sequence);
		}

		private OperationResult snapshot(long durationNanos) {
			long[] sorted = latencySamples.snapshot();
			double throughput = completed.sum() * 1_000_000_000d / durationNanos;
			return new OperationResult(completed.sum(),
					successes.sum(),
					throughput,
					percentile(sorted, 0.50),
					percentile(sorted, 0.95),
					percentile(sorted, 0.99),
					sorted.length == 0 ? 0 : sorted[sorted.length - 1],
					deadlines.sum(),
					rejections.sum(),
					cancellations.sum(),
					errors.sum(),
					sorted.length,
					latencySamples.overflow());
		}
	}

	private static final class LatencySamples {

		private final long[] samples;
		private final AtomicInteger next = new AtomicInteger();
		private final LongAdder overflow = new LongAdder();

		private LatencySamples(int capacity) {
			this.samples = new long[capacity];
		}

		private void record(long latencyNanos, long sequence) {
			// The sequence mixing avoids synchronizing a global sampler while bounding retained data.
			if ((mix64(sequence) & 7L) != 0) {
				return;
			}
			int index = next.getAndIncrement();
			if (index < samples.length) {
				samples[index] = Math.max(0, latencyNanos);
			} else {
				overflow.increment();
			}
		}

		private long[] snapshot() {
			int size = Math.min(next.get(), samples.length);
			long[] copy = Arrays.copyOf(samples, size);
			Arrays.sort(copy);
			return copy;
		}

		private long overflow() {
			return overflow.sum();
		}
	}

	private static long mix64(long value) {
		long mixed = value + 0x9e3779b97f4a7c15L;
		mixed = (mixed ^ (mixed >>> 30)) * 0xbf58476d1ce4e5b9L;
		mixed = (mixed ^ (mixed >>> 27)) * 0x94d049bb133111ebL;
		return mixed ^ (mixed >>> 31);
	}

	/** Percentile helper kept visible for deterministic correctness tests. */
	public static long percentile(long[] sorted, double percentile) {
		if (sorted.length == 0) {
			return 0;
		}
		int index = (int) Math.ceil(percentile * sorted.length) - 1;
		return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
	}

	private static Map<WorkloadProfile, SchedulerProfileMetrics> snapshotSchedulerMetrics(
			BenchmarkMeterRegistry registry) {
		var queue = new EnumMap<WorkloadProfile, Long>(WorkloadProfile.class);
		var execution = new EnumMap<WorkloadProfile, Long>(WorkloadProfile.class);
		for (WorkloadProfile profile : WorkloadProfile.values()) {
			queue.put(profile, 0L);
			execution.put(profile, 0L);
		}
		for (Meter meter : registry.getMeters()) {
			String profileTag = meter.getId().getTag("profile");
			if (profileTag == null || !(meter instanceof Timer timer)) {
				continue;
			}
			WorkloadProfile profile;
			try {
				profile = WorkloadProfile.valueOf(profileTag.toUpperCase(Locale.ROOT));
			} catch (IllegalArgumentException unknownProfile) {
				continue;
			}
			long p99 = timerP99(timer);
			switch (meter.getId().getName()) {
				case "rockserver.workload.queue.wait" -> queue.put(profile, Math.max(queue.get(profile), p99));
				case "rockserver.workload.execution" ->
						execution.put(profile, Math.max(execution.get(profile), p99));
				default -> {
				}
			}
		}
		var result = new EnumMap<WorkloadProfile, SchedulerProfileMetrics>(WorkloadProfile.class);
		for (WorkloadProfile profile : WorkloadProfile.values()) {
			result.put(profile, new SchedulerProfileMetrics(queue.get(profile), execution.get(profile)));
		}
		return Map.copyOf(result);
	}

	private static long timerP99(Timer timer) {
		long maximum = 0L;
		for (var percentile : timer.takeSnapshot().percentileValues()) {
			if (percentile.percentile() >= 0.99d) {
				maximum = Math.max(maximum, (long) percentile.value(TimeUnit.NANOSECONDS));
			}
		}
		return maximum;
	}

	/** Scheduler-internal queue and execution p99 for one profile during one phase. */
	public record SchedulerProfileMetrics(long queueP99Nanos, long executionP99Nanos) {
	}

	private static final class AdmissionTracker {

		private final AtomicInteger maxForegroundQueue = new AtomicInteger();
		private final AtomicInteger maxMaintenanceQueue = new AtomicInteger();
		private final AtomicInteger maxForegroundActive = new AtomicInteger();
		private final AtomicInteger maxMaintenanceActive = new AtomicInteger();
		private final AtomicInteger maxTotalActive = new AtomicInteger();
		private final EnumMap<RWScheduler.Pool, MutablePoolUtilization> poolUtilization =
				new EnumMap<>(RWScheduler.Pool.class);

		private AdmissionTracker(boolean exactWaitingWorkerEvidence, long admissionSampleNanos) {
			for (RWScheduler.Pool pool : POOL_VALUES) {
				poolUtilization.put(pool,
						new MutablePoolUtilization(exactWaitingWorkerEvidence, admissionSampleNanos));
			}
		}

		private void sample(EmbeddedConnection embedded) {
			var scheduler = embedded.getScheduler();
			boolean storagePressure = scheduler.isStoragePressure();
			int foregroundQueue = 0;
			int maintenanceQueue = 0;
			int foregroundActive = 0;
			int maintenanceActive = 0;
			int totalActive = 0;
			for (RWScheduler.Pool pool : POOL_VALUES) {
				var utilization = poolUtilization.get(pool);
				utilization.sample(pool, scheduler, storagePressure);
				foregroundQueue += utilization.lastQueued(WorkloadProfile.INGEST);
				maintenanceQueue += utilization.lastQueued(WorkloadProfile.BATCH);
				foregroundActive += utilization.lastActive(WorkloadProfile.INGEST);
				maintenanceActive += utilization.lastActive(WorkloadProfile.BATCH);
				totalActive += utilization.lastActiveTasks();
			}
			maxForegroundQueue.accumulateAndGet(foregroundQueue, Math::max);
			maxMaintenanceQueue.accumulateAndGet(maintenanceQueue, Math::max);
			maxForegroundActive.accumulateAndGet(foregroundActive, Math::max);
			maxMaintenanceActive.accumulateAndGet(maintenanceActive, Math::max);
			maxTotalActive.accumulateAndGet(totalActive, Math::max);
		}

		private AdmissionResult finish(EmbeddedConnection embedded, PhaseMetrics metrics) {
			sample(embedded);
			var utilization = new EnumMap<RWScheduler.Pool, PoolUtilization>(RWScheduler.Pool.class);
			int foregroundQueue = 0;
			int maintenanceQueue = 0;
			for (var entry : poolUtilization.entrySet()) {
				utilization.put(entry.getKey(), entry.getValue().snapshot());
				foregroundQueue += entry.getValue().lastQueued(WorkloadProfile.INGEST);
				maintenanceQueue += entry.getValue().lastQueued(WorkloadProfile.BATCH);
			}
			return new AdmissionResult(
					maxForegroundQueue.get(),
					maxMaintenanceQueue.get(),
					foregroundQueue,
					maintenanceQueue,
					maxForegroundActive.get(),
					maxMaintenanceActive.get(),
					maxTotalActive.get(),
					metrics.foregroundRejections(),
					metrics.maintenanceRejections(),
					Map.copyOf(utilization));
		}
	}

	private static final class MutablePoolUtilization {

		private final boolean exactWaitingWorkerEvidence;
		private final long samplePeriodNanos;
		private final long[] telemetry = new long[RWScheduler.POOL_TELEMETRY_LENGTH];

		private long samples;
		private long eligibleBacklogSamples;
		private long policyLimitedBacklogSamples;
		private long saturatingDemandSamples;
		private long fullyBusySamples;
		private long idleWorkerSlots;
		private int maximumActive;
		private int consecutiveAvoidableIdleSamples;
		private int maximumConsecutiveAvoidableIdleSamples;
		private int workerCount;

		private MutablePoolUtilization(boolean exactWaitingWorkerEvidence, long samplePeriodNanos) {
			this.exactWaitingWorkerEvidence = exactWaitingWorkerEvidence;
			this.samplePeriodNanos = samplePeriodNanos;
		}

		private void sample(RWScheduler.Pool pool,
				RWScheduler scheduler,
				boolean storagePressure) {
			BenchmarkSchedulerTelemetry.copyPoolTelemetry(scheduler, pool, telemetry);
			samples++;
			workerCount = value(RWScheduler.POOL_TELEMETRY_WORKER_COUNT);
			int activeTasks = lastActiveTasks();
			maximumActive = Math.max(maximumActive, activeTasks);
			if (telemetry[RWScheduler.POOL_TELEMETRY_BATCH_LIMITED] != 0L
					&& lastQueued(WorkloadProfile.BATCH) > 0) {
				policyLimitedBacklogSamples++;
			}
			int eligibleQueued = eligibleQueued(pool, storagePressure);
			if (eligibleQueued <= 0) {
				consecutiveAvoidableIdleSamples = 0;
				return;
			}
			eligibleBacklogSamples++;
			int idleWorkers = exactWaitingWorkerEvidence
					? value(RWScheduler.POOL_TELEMETRY_WAITING_WORKERS) : 0;
			int avoidablyIdleWorkers = Math.min(idleWorkers, eligibleQueued);
			if (avoidablyIdleWorkers == 0) {
				consecutiveAvoidableIdleSamples = 0;
			} else {
				consecutiveAvoidableIdleSamples++;
				maximumConsecutiveAvoidableIdleSamples = Math.max(
						maximumConsecutiveAvoidableIdleSamples, consecutiveAvoidableIdleSamples);
			}
			if ((long) activeTasks + eligibleQueued >= workerCount) {
				saturatingDemandSamples++;
				idleWorkerSlots += idleWorkers;
				if (idleWorkers == 0) {
					fullyBusySamples++;
				}
			}
		}

		private int eligibleQueued(RWScheduler.Pool pool, boolean storagePressure) {
			return switch (pool) {
				case READ -> {
					int queued = 0;
					for (var profile : WORKLOAD_PROFILE_VALUES) {
						if (profile == WorkloadProfile.ANALYTICAL) continue;
						int profileQueued = lastQueued(profile);
						queued += profile == WorkloadProfile.BATCH
								? Math.min(profileQueued, value(RWScheduler.POOL_TELEMETRY_BATCH_ALLOWANCE))
								: profileQueued;
					}
					yield queued;
				}
				case WRITE -> {
					int batchQueued = lastQueued(WorkloadProfile.BATCH);
					yield value(RWScheduler.POOL_TELEMETRY_QUEUED_TASKS) - batchQueued
							+ Math.min(batchQueued, value(RWScheduler.POOL_TELEMETRY_BATCH_ALLOWANCE));
				}
				case CONTROL -> value(RWScheduler.POOL_TELEMETRY_QUEUED_TASKS);
				case PHYSICAL -> storagePressure ? 0 : value(RWScheduler.POOL_TELEMETRY_QUEUED_TASKS);
			};
		}

		private int lastQueued(WorkloadProfile profile) {
			return BenchmarkSchedulerTelemetry.queued(telemetry, profile);
		}

		private int lastActive(WorkloadProfile profile) {
			return BenchmarkSchedulerTelemetry.active(telemetry, profile);
		}

		private int lastActiveTasks() {
			return value(RWScheduler.POOL_TELEMETRY_ACTIVE_TASKS);
		}

		private int value(int index) {
			return Math.toIntExact(telemetry[index]);
		}

		private PoolUtilization snapshot() {
			double utilization = saturatingDemandSamples == 0L || workerCount == 0
					? 0.0d
					: 1.0d - idleWorkerSlots / (double) (saturatingDemandSamples * workerCount);
			return new PoolUtilization(workerCount,
					samples,
					eligibleBacklogSamples,
					policyLimitedBacklogSamples,
					saturatingDemandSamples,
					fullyBusySamples,
					idleWorkerSlots,
					maximumActive,
					maximumConsecutiveAvoidableIdleSamples,
					Math.max(0.0d, Math.min(1.0d, utilization)),
					exactWaitingWorkerEvidence,
					samplePeriodNanos);
		}
	}

	private static final class Pacer {

		private final long intervalNanos;
		private long nextNanos;

		private Pacer(long totalRate, int workers, int worker) {
			this.intervalNanos = totalRate <= 0
					? 0
					: Math.max(1, TimeUnit.SECONDS.toNanos(1) * Math.max(1, workers) / totalRate);
			this.nextNanos = System.nanoTime()
					+ (intervalNanos == 0 ? 0 : intervalNanos * worker / Math.max(1, workers));
		}

		private void awaitNext(AtomicBoolean stop) throws InterruptedException {
			if (intervalNanos == 0) {
				return;
			}
			nextNanos += intervalNanos;
			while (!stop.get()) {
				long now = System.nanoTime();
				long remaining = nextNanos - now;
				if (remaining <= 0) {
					if (remaining < -intervalNanos) {
						nextNanos = now;
					}
					return;
				}
				LockSupport.parkNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(1)));
				if (Thread.interrupted()) {
					throw new InterruptedException();
				}
			}
		}
	}

	private static final class BenchmarkMeterRegistry extends SimpleMeterRegistry {

		@Override
		protected Timer newTimer(Meter.Id id,
				DistributionStatisticConfig distributionStatisticConfig,
				PauseDetector pauseDetector) {
			var benchmarkConfig = DistributionStatisticConfig.builder()
					.percentiles(0.99d)
					.percentilePrecision(3)
					.build()
					.merge(distributionStatisticConfig);
			return super.newTimer(id, benchmarkConfig, pauseDetector);
		}
	}

	private static final class RequestTracker implements ClientInterceptor {

		private final AtomicBoolean measuring = new AtomicBoolean();
		private final LongAdder submitted = new LongAdder();
		private final LongAdder terminal = new LongAdder();
		private final LongAdder duplicateTerminal = new LongAdder();
		private final AtomicLong inFlight = new AtomicLong();
		private final AtomicLong maximumInFlight = new AtomicLong();
		private final EnumMap<Status.Code, LongAdder> statuses = new EnumMap<>(Status.Code.class);

		private RequestTracker() {
			for (Status.Code code : Status.Code.values()) {
				statuses.put(code, new LongAdder());
			}
		}

		private void startTracking() {
			if (inFlight.get() != 0L || measuring.get()) {
				throw new IllegalStateException("A request-accounting window is already active or has in-flight calls");
			}
			submitted.reset();
			terminal.reset();
			duplicateTerminal.reset();
			maximumInFlight.set(0L);
			for (LongAdder status : statuses.values()) {
				status.reset();
			}
			if (!measuring.compareAndSet(false, true)) {
				throw new IllegalStateException("A request-accounting window started concurrently");
			}
		}

		private void stopTracking() {
			if (!measuring.compareAndSet(true, false)) {
				throw new IllegalStateException("No request-accounting window is active");
			}
		}

		private void stopTrackingIfActive() {
			measuring.compareAndSet(true, false);
		}

		private RequestAccounting awaitSnapshot() throws InterruptedException {
			long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DEADLINE_MILLIS);
			while (inFlight.get() != 0L && System.nanoTime() < deadline) {
				LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
				if (Thread.interrupted()) {
					throw new InterruptedException();
				}
			}
			var statusCounts = new EnumMap<Status.Code, Long>(Status.Code.class);
			for (var entry : statuses.entrySet()) {
				statusCounts.put(entry.getKey(), entry.getValue().sum());
			}
			return new RequestAccounting(submitted.sum(),
					terminal.sum(),
					inFlight.get(),
					duplicateTerminal.sum(),
					maximumInFlight.get(),
					Map.copyOf(statusCounts));
		}

		@Override
		public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
				CallOptions callOptions,
				Channel next) {
			return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {

				private final AtomicBoolean closed = new AtomicBoolean();
				private boolean tracked;

				@Override
				public void start(Listener<RespT> responseListener, Metadata headers) {
					tracked = measuring.get();
					if (tracked) {
						submitted.increment();
						long active = inFlight.incrementAndGet();
						maximumInFlight.accumulateAndGet(active, Math::max);
					}
					var trackingListener = new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(
							responseListener) {

						@Override
						public void onClose(Status status, Metadata trailers) {
							finish(status.getCode());
							super.onClose(status, trailers);
						}
					};
					try {
						super.start(trackingListener, headers);
					} catch (Throwable failure) {
						finish(Status.Code.UNKNOWN);
						throw failure;
					}
				}

				private void finish(Status.Code code) {
					if (!tracked) {
						return;
					}
					if (!closed.compareAndSet(false, true)) {
						duplicateTerminal.increment();
						return;
					}
					statuses.get(code).increment();
					terminal.increment();
					inFlight.decrementAndGet();
				}
			};
		}
	}

	private record Client(ManagedChannel channel,
			RocksDBServiceGrpc.RocksDBServiceBlockingStub blocking,
			RocksDBServiceGrpc.RocksDBServiceFutureStub future,
			RequestTracker requestTracker) implements AutoCloseable {

		private static Client open(int port) {
			ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", port)
					.directExecutor()
					.usePlaintext()
					.disableRetry()
					.maxInboundMessageSize(64 * 1024 * 1024)
					.build();
			var requestTracker = new RequestTracker();
			Channel tracked = ClientInterceptors.intercept(channel, requestTracker);
			return new Client(channel,
					RocksDBServiceGrpc.newBlockingStub(tracked),
					RocksDBServiceGrpc.newFutureStub(tracked),
					requestTracker);
		}

		private RocksDBServiceGrpc.RocksDBServiceBlockingStub blockingWithDeadline() {
			return blocking.withDeadlineAfter(DEADLINE_MILLIS, TimeUnit.MILLISECONDS);
		}

		private RocksDBServiceGrpc.RocksDBServiceFutureStub futureWithDeadline() {
			return future.withDeadlineAfter(DEADLINE_MILLIS, TimeUnit.MILLISECONDS);
		}

		@Override
		public void close() throws InterruptedException {
			channel.shutdownNow();
			if (!channel.awaitTermination(10, TimeUnit.SECONDS)) {
				throw new IllegalStateException("gRPC client did not terminate");
			}
		}
	}

	private record ProcessCounters(long cpuNanos,
			long allocatedBytes,
			long observerCpuNanos,
			long observerAllocatedBytes,
			long gcCollections,
			long gcMillis) {
	}

	private static final class RuntimeTelemetryTracker implements AutoCloseable {

		private final OperatingSystemMXBean operatingSystem;
		private final ThreadMXBean allocationBean;
		private final java.lang.management.ThreadMXBean threadBean;
		private final BenchmarkProcessTelemetry.PeakSampler peakSampler =
				new BenchmarkProcessTelemetry.PeakSampler();
		private final boolean strict;
		private boolean active;
		private ProcessCounters before;
		private long observerThreadId = -1L;
		private long nextSampleNanos;

		private RuntimeTelemetryTracker(boolean strict) {
			this.strict = strict;
			var osBean = ManagementFactory.getOperatingSystemMXBean();
			var threads = ManagementFactory.getThreadMXBean();
			this.operatingSystem = osBean instanceof OperatingSystemMXBean extended ? extended : null;
			this.allocationBean = threads instanceof ThreadMXBean extended ? extended : null;
			this.threadBean = threads;
			boolean allocationSupported = allocationBean != null
					&& allocationBean.isThreadAllocatedMemorySupported();
			if (allocationSupported && !allocationBean.isThreadAllocatedMemoryEnabled()) {
				allocationBean.setThreadAllocatedMemoryEnabled(true);
			}
			boolean threadCpuSupported = threadBean.isThreadCpuTimeSupported();
			if (threadCpuSupported && !threadBean.isThreadCpuTimeEnabled()) {
				threadBean.setThreadCpuTimeEnabled(true);
			}
			if (strict && (operatingSystem == null || !allocationSupported || !threadCpuSupported)) {
				throw new IllegalStateException("Strict overload telemetry requires HotSpot process CPU, "
						+ "thread CPU, and thread-allocation MXBeans");
			}
		}

		private synchronized void registerObserverThread() {
			long currentThreadId = Thread.currentThread().threadId();
			if (observerThreadId != -1L && observerThreadId != currentThreadId) {
				throw new IllegalStateException("Runtime telemetry observer thread changed");
			}
			observerThreadId = currentThreadId;
		}

		private synchronized void start() {
			if (active) {
				throw new IllegalStateException("Runtime telemetry window is already active");
			}
			if (strict && observerThreadId < 0L) {
				throw new IllegalStateException("Runtime telemetry observer thread was not registered");
			}
			peakSampler.reset();
			before = captureCounters();
			active = true;
			nextSampleNanos = System.nanoTime() + RUNTIME_RESOURCE_SAMPLE_NANOS;
		}

		private synchronized void sampleIfDue() {
			if (!active) {
				return;
			}
			long now = System.nanoTime();
			if (now < nextSampleNanos) {
				return;
			}
			nextSampleNanos = now + RUNTIME_RESOURCE_SAMPLE_NANOS;
			peakSampler.sample();
		}

		private synchronized RuntimeTelemetry stop() {
			if (!active) {
				throw new IllegalStateException("Runtime telemetry window is not active");
			}
			ProcessCounters after = captureCounters();
			active = false;
			peakSampler.sample();
			var peaks = peakSampler.peaks();
			long observerCpuNanos = delta(before.observerCpuNanos(), after.observerCpuNanos());
			long observerAllocatedBytes = delta(
					before.observerAllocatedBytes(), after.observerAllocatedBytes());
			long cpuNanos = subtractObserver(
					delta(before.cpuNanos(), after.cpuNanos()), observerCpuNanos);
			long allocatedBytes = subtractObserver(
					delta(before.allocatedBytes(), after.allocatedBytes()), observerAllocatedBytes);
			long gcCollections = delta(before.gcCollections(), after.gcCollections());
			long gcMillis = delta(before.gcMillis(), after.gcMillis());
			boolean available = cpuNanos > 0L
					&& allocatedBytes > 0L
					&& gcCollections >= 0L
					&& gcMillis >= 0L
					&& observerCpuNanos >= 0L
					&& observerAllocatedBytes >= 0L
					&& peaks.complete();
			return new RuntimeTelemetry(cpuNanos,
					allocatedBytes,
					observerCpuNanos,
					observerAllocatedBytes,
					gcCollections,
					gcMillis,
					peaks.liveHeapBytes(),
					peaks.directMemoryBytes(),
					peaks.residentSetBytes(),
					peaks.threadCount(),
					peaks.nativeHandles(),
					available);
		}

		private ProcessCounters captureCounters() {
			long cpu = operatingSystem == null ? -1L : operatingSystem.getProcessCpuTime();
			long allocation = allocationBean == null || !allocationBean.isThreadAllocatedMemoryEnabled()
					? -1L : allocationBean.getTotalThreadAllocatedBytes();
			long observerCpu = observerThreadId < 0L || !threadBean.isThreadCpuTimeEnabled()
					? -1L : threadBean.getThreadCpuTime(observerThreadId);
			long observerAllocation = observerThreadId < 0L || allocationBean == null
					? -1L : allocationBean.getThreadAllocatedBytes(observerThreadId);
			long collections = 0L;
			long millis = 0L;
			for (GarbageCollectorMXBean collector : ManagementFactory.getGarbageCollectorMXBeans()) {
				long collectionCount = collector.getCollectionCount();
				long collectionTime = collector.getCollectionTime();
				if (collectionCount < 0L || collectionTime < 0L) {
					collections = -1L;
					millis = -1L;
					break;
				}
				collections += collectionCount;
				millis += collectionTime;
			}
			return new ProcessCounters(cpu, allocation, observerCpu, observerAllocation, collections, millis);
		}

		private static long delta(long before, long after) {
			return before < 0L || after < before ? -1L : after - before;
		}

		private static long subtractObserver(long processDelta, long observerDelta) {
			return processDelta < 0L || observerDelta < 0L
					? -1L : Math.max(0L, processDelta - observerDelta);
		}

		@Override
		public synchronized void close() {
			active = false;
			peakSampler.close();
		}
	}

	/** Process and peak-resource evidence captured only inside a measured phase. */
	public record RuntimeTelemetry(long processCpuNanos,
			long allocatedBytes,
			long observerCpuNanos,
			long observerAllocatedBytes,
			long gcCollections,
			long gcMillis,
			long peakLiveHeapBytes,
			long peakDirectMemoryBytes,
			long peakRssBytes,
			int peakThreadCount,
			long peakNativeHandles,
			boolean available) {
	}

	private record PhaseResult(int round,
			Phase phase,
			long durationMillis,
			Map<Operation, OperationResult> operations,
			long warmupErrors,
			List<String> errorDetails,
			AdmissionResult admission,
			ResourceResult resources,
			RequestAccounting requestAccounting,
			Map<WorkloadProfile, SchedulerProfileMetrics> schedulerMetrics,
			RuntimeTelemetry runtimeTelemetry) {

		private OperationResult operation(Operation operation) {
			return operations.get(operation);
		}

		private long unexpectedErrors() {
			long measured = operations.values().stream().mapToLong(OperationResult::errors).sum();
			// FOREGROUND is an aggregate of three concrete operations; do not count it twice.
			return measured - operation(Operation.FOREGROUND).errors() + warmupErrors;
		}

		private long usefulCompletions() {
			return operations.entrySet().stream()
					.filter(entry -> entry.getKey() != Operation.FOREGROUND
							&& entry.getKey() != Operation.CANCELLATION)
					.mapToLong(entry -> entry.getValue().successes())
					.sum();
		}

		private double cpuNanosPerOperation() {
			long usefulCompletions = usefulCompletions();
			return usefulCompletions == 0L || runtimeTelemetry.processCpuNanos() < 0L
					? Double.NaN : runtimeTelemetry.processCpuNanos() / (double) usefulCompletions;
		}

		private double allocatedBytesPerOperation() {
			long usefulCompletions = usefulCompletions();
			return usefulCompletions == 0L || runtimeTelemetry.allocatedBytes() < 0L
					? Double.NaN : runtimeTelemetry.allocatedBytes() / (double) usefulCompletions;
		}
	}

	/** Client-interceptor proof that every measured RPC reached exactly one terminal status. */
	public record RequestAccounting(long submitted,
			long terminal,
			long inFlight,
			long duplicateTerminal,
			long maximumInFlight,
			Map<Status.Code, Long> statuses) {

		public RequestAccounting {
			statuses = Map.copyOf(statuses);
		}

		public boolean conserved() {
			return submitted > 0L && terminal == submitted && inFlight == 0L && duplicateTerminal == 0L
					&& statuses.values().stream().mapToLong(Long::longValue).sum() == terminal;
		}
	}

	private record OperationResult(long completed,
			long successes,
			double throughput,
			long p50Nanos,
			long p95Nanos,
			long p99Nanos,
			long maxNanos,
			long deadlines,
			long rejections,
			long cancellations,
			long errors,
			int latencySamples,
			long sampleOverflow) {
	}

	private record AdmissionResult(int maxForegroundQueue,
			int maxMaintenanceQueue,
			int endForegroundQueue,
			int endMaintenanceQueue,
			int maxForegroundActive,
			int maxMaintenanceActive,
			int maxTotalActive,
			long foregroundRejected,
			long maintenanceRejected,
			Map<RWScheduler.Pool, PoolUtilization> poolUtilization) {
	}

	/** Sampled evidence that an eligible backlog did not coexist with avoidably idle workers. */
	public record PoolUtilization(int workerCount,
			long samples,
			long eligibleBacklogSamples,
			long policyLimitedBacklogSamples,
			long saturatingDemandSamples,
			long fullyBusySamples,
			long idleWorkerSlots,
			int maximumActive,
			int maximumConsecutiveAvoidableIdleSamples,
			double utilizationWhileBacklogged,
			boolean exactWaitingWorkerEvidence,
			long samplePeriodNanos) {

		public long maximumAvoidableIdleNanos() {
			try {
				return Math.multiplyExact((long) maximumConsecutiveAvoidableIdleSamples, samplePeriodNanos);
			} catch (ArithmeticException overflow) {
				return Long.MAX_VALUE;
			}
		}

		public boolean saturatedAndWorkConserving() {
			return exactWaitingWorkerEvidence
					&& workerCount > 0
					&& (saturatingDemandSamples > 0L || policyLimitedBacklogSamples > 0L)
					&& maximumAvoidableIdleNanos() <= COOPERATIVE_QUANTUM_NANOS + samplePeriodNanos;
		}
	}

	private record ResourceResult(int foregroundQueued,
			int maintenanceQueued,
			int foregroundActive,
			int maintenanceActive,
			int totalQueued,
			int totalActive,
			long pendingOperations,
			int openTransactions,
		int openIterators,
		int activeRangeCursors,
		int retainedRangeSnapshots,
		int retainedRangePermits,
		int retainedRangeWaiters,
		int activeCdcPollCursors,
		int activeExistsMultiRequests,
		int iteratorLeases,
			long drainMillis,
			boolean drained,
			SchedulerConservation schedulerConservation) {
	}

	/** Lifetime scheduler counters must balance after the phase has fully drained. */
	public record SchedulerConservation(long accepted,
			long started,
			long completed,
			long terminalOutcomes,
			long failures,
			List<String> failuresByPool) {

		public SchedulerConservation {
			failuresByPool = List.copyOf(failuresByPool);
		}

		public boolean conserved() {
			return accepted > 0L && started == completed && terminalOutcomes >= accepted
					&& failures == 0L && failuresByPool.isEmpty();
		}
	}

	private static void verifyHostMemory(Options options, HostMemory hostMemory) {
		long requiredBytes = Math.multiplyExact((long) options.minimumHostAvailableGiB(), GIBIBYTE);
		if (requiredBytes == 0L) {
			return;
		}
		if (!hostMemory.known()) {
			throw new IllegalStateException("Host MemAvailable could not be read; refusing to consume a one-shot root");
		}
		if (hostMemory.availableBytes() < requiredBytes) {
			throw new IllegalStateException("Host MemAvailable is " + hostMemory.availableBytes()
					+ " bytes, below the required " + requiredBytes
					+ " bytes; refusing to consume a one-shot root");
		}
	}

	private static void verifyEnvironment(Options options, RunEnvironment environment) {
		if (!options.enforce()) {
			return;
		}
		if (environment.cpuModel().equals("unavailable")
				|| environment.rocksdbVersion().equals("unavailable")
				|| environment.dependencyClasspathSha256().equals("unavailable")
				|| environment.harnessClasspathSha256().equals("unavailable")
				|| environment.processStart().equals(Instant.EPOCH.toString())) {
			throw new IllegalStateException("Enforced overload timing requires CPU, RocksDB/native, "
					+ "dependency/harness classpath, and fresh-process provenance");
		}
	}

	private static void verifyStorage(Options options, StorageEnvironment storage) {
		if (options.storageLabel().equals("ci-structural")) {
			return;
		}
		if (!storageMatchesLabelForTesting(storage, options.storageLabel())) {
			throw new IllegalStateException("Resolved storage does not match --storage-label="
					+ options.storageLabel() + ": " + storage);
		}
	}

	private static void verifyCompetingBenchmarks(Options options, List<String> competingProcesses) {
		if (options.enforce() && !competingProcesses.isEmpty()) {
			throw new IllegalStateException("Competing JVM benchmark processes are active; refusing release timing: "
					+ competingProcesses);
		}
	}

	private static List<String> captureCompetingBenchmarkProcesses() {
		long currentPid = ProcessHandle.current().pid();
		var competitors = new ArrayList<String>();
		boolean currentProcessObserved = false;
		try (var entries = Files.list(Path.of("/proc"))) {
			for (Path entry : entries.toList()) {
				long pid;
				try {
					pid = Long.parseLong(entry.getFileName().toString());
				} catch (NumberFormatException ignored) {
					continue;
				}
				if (pid == currentPid) {
					currentProcessObserved = true;
					continue;
				}
				try {
					String command = Files.readString(entry.resolve("comm")).trim();
					String commandLine = Files.readString(entry.resolve("cmdline")).replace('\0', ' ').trim();
					if (isCompetingBenchmarkCommandForTesting(command, commandLine)) {
						competitors.add(pid + ":" + sha256(commandLine));
					}
				} catch (IOException | RuntimeException ignored) {
					// A process can exit between directory enumeration and reading its command.
				}
			}
		} catch (IOException | RuntimeException ignored) {
			return List.of("process-enumeration-unavailable");
		}
		if (!currentProcessObserved) {
			return List.of("process-enumeration-unavailable");
		}
		competitors.sort(String::compareTo);
		return List.copyOf(competitors);
	}

	/** Pure command classifier used by exclusive-host provenance tests. */
	public static boolean isCompetingBenchmarkCommandForTesting(String command, String commandLine) {
		String executable = Path.of(command.isBlank() ? "unknown" : command).getFileName().toString()
				.toLowerCase(Locale.ROOT);
		String lower = commandLine.toLowerCase(Locale.ROOT);
		return executable.startsWith("java")
				&& (lower.contains("org.openjdk.jmh") || lower.contains("jmh") || lower.contains("benchmark"));
	}

	/** Resolved Linux mount and block-device evidence for the benchmark root. */
	public record StorageEnvironment(String mountPoint,
			String source,
			String filesystem,
			int rotational,
			String model) {

		private static StorageEnvironment capture(Path target) {
			try {
				Path probe = target.toAbsolutePath().normalize();
				while (probe != null && !Files.exists(probe)) {
					probe = probe.getParent();
				}
				if (probe == null) {
					return unavailable();
				}
				String[] best = null;
				int bestLength = -1;
				for (String line : Files.readString(Path.of("/proc/self/mountinfo")).lines().toList()) {
					String[] fields = line.split(" ");
					int separator = -1;
					for (int index = 6; index < fields.length; index++) {
						if (fields[index].equals("-")) {
							separator = index;
							break;
						}
					}
					if (separator < 0 || separator + 2 >= fields.length || fields.length < 6) {
						continue;
					}
					String mountPoint = decodeMountInfo(fields[4]);
					Path mountPath = Path.of(mountPoint).toAbsolutePath().normalize();
					if (probe.startsWith(mountPath) && mountPoint.length() >= bestLength) {
						bestLength = mountPoint.length();
						best = new String[] {mountPoint, decodeMountInfo(fields[separator + 2]),
								fields[separator + 1]};
					}
				}
				if (best == null) {
					return unavailable();
				}
				BlockDeviceEvidence block = blockDeviceEvidence(best[1]);
				return new StorageEnvironment(best[0], best[1], best[2], block.rotational(), block.model());
			} catch (IOException | RuntimeException ignored) {
				return unavailable();
			}
		}

		private static StorageEnvironment unavailable() {
			return new StorageEnvironment("unknown", "unknown", "unknown", -1, "unknown");
		}
	}

	private static String decodeMountInfo(String value) {
		return value.replace("\\040", " ")
				.replace("\\011", "\t")
				.replace("\\012", "\n")
				.replace("\\134", "\\");
	}

	private static BlockDeviceEvidence blockDeviceEvidence(String source) {
		if (!source.startsWith("/dev/")) {
			return new BlockDeviceEvidence(-1, "unknown");
		}
		try {
			String deviceName = Path.of(source).getFileName().toString();
			Path sysDevice = Path.of("/sys/class/block", deviceName).toRealPath();
			Path wholeDevice = Files.isRegularFile(sysDevice.resolve("partition")) ? sysDevice.getParent() : sysDevice;
			int rotational = Integer.parseInt(Files.readString(wholeDevice.resolve("queue/rotational")).trim());
			Path modelFile = wholeDevice.resolve("device/model");
			String model = Files.isRegularFile(modelFile) ? Files.readString(modelFile).trim() : "unknown";
			return new BlockDeviceEvidence(rotational, model);
		} catch (IOException | RuntimeException ignored) {
			return new BlockDeviceEvidence(-1, "unknown");
		}
	}

	private record BlockDeviceEvidence(int rotational, String model) {
	}

	/** Captures storage evidence without creating the target path. */
	public static StorageEnvironment captureStorageForTesting(Path target) {
		return StorageEnvironment.capture(target);
	}

	/** Pure label consistency check used by release-provenance tests. */
	public static boolean storageMatchesLabelForTesting(StorageEnvironment storage, String label) {
		return switch (label) {
			case "ci-structural" -> true;
			case "hdd-btrfs" -> storage.filesystem().equalsIgnoreCase("btrfs")
					&& storage.rotational() == 1;
			case "hdd-zfs" -> storage.filesystem().toLowerCase(Locale.ROOT).contains("zfs")
					&& storage.rotational() != 0;
			case "nvme" -> storage.rotational() == 0
					&& Path.of(storage.source()).getFileName().toString().startsWith("nvme");
			default -> false;
		};
	}

	/** Linux host-memory evidence captured before a prepared root is consumed. */
	public record HostMemory(long totalBytes,
			long availableBytes,
			long swapTotalBytes,
			long swapFreeBytes) {

		private static HostMemory capture() {
			Path meminfo = Path.of("/proc/meminfo");
			if (!Files.isRegularFile(meminfo)) {
				return unavailable();
			}
			try {
				return parseHostMemory(Files.readString(meminfo));
			} catch (IOException | RuntimeException ignored) {
				return unavailable();
			}
		}

		private static HostMemory unavailable() {
			return new HostMemory(-1L, -1L, -1L, -1L);
		}

		public boolean known() {
			return totalBytes > 0L && availableBytes >= 0L && swapTotalBytes >= 0L && swapFreeBytes >= 0L;
		}
	}

	/** Parses /proc/meminfo text for deterministic preflight tests. */
	public static HostMemory parseHostMemoryForTesting(String meminfo) {
		return parseHostMemory(meminfo);
	}

	private static HostMemory parseHostMemory(String meminfo) {
		Map<String, Long> values = new LinkedHashMap<>();
		for (String line : meminfo.lines().toList()) {
			int colon = line.indexOf(':');
			if (colon <= 0) {
				continue;
			}
			String[] fields = line.substring(colon + 1).trim().split("\\s+");
			if (fields.length == 0 || fields[0].isBlank()) {
				continue;
			}
			long value = Long.parseLong(fields[0]);
			if (fields.length > 1 && fields[1].equalsIgnoreCase("kB")) {
				value = Math.multiplyExact(value, 1_024L);
			}
			values.put(line.substring(0, colon), value);
		}
		return new HostMemory(
				values.getOrDefault("MemTotal", -1L),
				values.getOrDefault("MemAvailable", -1L),
				values.getOrDefault("SwapTotal", -1L),
				values.getOrDefault("SwapFree", -1L));
	}

	private record RunEnvironment(String javaVersion,
			String javaVm,
			String javaVendor,
			String javaHome,
			String javaLibraryPath,
			List<String> jvmArguments,
			long jvmMaxMemoryBytes,
			String os,
			int availableProcessors,
			String cpuModel,
			double systemLoadAverage,
			HostMemory hostMemory,
			StorageEnvironment storage,
			String rocksdbVersion,
			String dependencyClasspathSha256,
			String harnessClasspathSha256,
			long processId,
			String processStart,
			List<String> competingBenchmarkProcesses) {

		private static RunEnvironment capture(Path target) {
			var operatingSystem = ManagementFactory.getOperatingSystemMXBean();
			return new RunEnvironment(
					System.getProperty("java.version"),
					System.getProperty("java.vm.name") + " " + System.getProperty("java.vm.version"),
					System.getProperty("java.vendor"),
					System.getProperty("java.home"),
					System.getProperty("java.library.path", ""),
					List.copyOf(ManagementFactory.getRuntimeMXBean().getInputArguments()),
					Runtime.getRuntime().maxMemory(),
					System.getProperty("os.name") + " " + System.getProperty("os.version")
							+ " " + System.getProperty("os.arch"),
					Runtime.getRuntime().availableProcessors(),
					GrpcOverloadBenchmark.cpuModel(),
					operatingSystem.getSystemLoadAverage(),
					HostMemory.capture(),
					StorageEnvironment.capture(target),
					String.valueOf(RocksDB.rocksdbVersion()),
					GrpcOverloadBenchmark.dependencyClasspathSha256(),
					GrpcOverloadBenchmark.codeSourceSha256(GrpcOverloadBenchmark.class),
					ProcessHandle.current().pid(),
					ProcessHandle.current().info().startInstant().orElse(Instant.EPOCH).toString(),
					captureCompetingBenchmarkProcesses());
		}
	}

	private record BenchmarkResult(String schema,
			Instant started,
			Instant finished,
			Options options,
			RunEnvironment environment,
			List<PhaseResult> phases,
			IntegrityResult integrity,
			boolean shutdownClean,
			long nativeLeaksDetected,
			Acceptance acceptance) {
	}

	/** Unique acknowledged writes read back through the same real gRPC channel. */
	public record IntegrityResult(long writesAttempted,
			long writesAcknowledged,
			long readsAttempted,
			long readsMatched,
			long mismatches,
			long errors,
			List<String> errorDetails,
			RequestAccounting requestAccounting) {

		public IntegrityResult {
			errorDetails = List.copyOf(errorDetails);
			Objects.requireNonNull(requestAccounting, "requestAccounting");
		}

		private static IntegrityResult notRun() {
			return new IntegrityResult(0L,
					0L,
					0L,
					0L,
					0L,
					1L,
					List.of("integrity probe did not run"),
					new RequestAccounting(0L, 0L, 0L, 0L, 0L, Map.of()));
		}

		public boolean passed() {
			return writesAttempted > 0L
					&& writesAcknowledged == writesAttempted
					&& readsAttempted == writesAttempted
					&& readsMatched == writesAcknowledged
					&& mismatches == 0L
					&& errors == 0L
					&& requestAccounting.conserved();
		}
	}

	/** Minimal inputs for the pure acceptance gate. */
	public record GateInput(long foregroundDeadlines,
			long firstLastDeadlines,
			long unexpectedDeadlines,
			RatioConfidenceInterval foregroundP99Ratio,
			RatioConfidenceInterval foregroundThroughputRatio,
			long cancellations,
			boolean resourcesDrained,
			long unexpectedErrors,
			long foregroundRejections,
			RequestAccounting foregroundRequests,
			RequestAccounting mixedRequests,
			IntegrityResult integrity,
			Set<WorkloadProfile> progressedProfiles,
			PoolUtilization readPool,
			PoolUtilization writePool,
			SchedulerConservation foregroundScheduler,
			SchedulerConservation mixedScheduler,
			PriorityEvidence priority,
			boolean runtimeTelemetryAvailable,
			long nativeLeaksDetected,
			boolean shutdownClean) {

		public GateInput {
			Objects.requireNonNull(foregroundP99Ratio, "foregroundP99Ratio");
			Objects.requireNonNull(foregroundThroughputRatio, "foregroundThroughputRatio");
			Objects.requireNonNull(foregroundRequests, "foregroundRequests");
			Objects.requireNonNull(mixedRequests, "mixedRequests");
			Objects.requireNonNull(integrity, "integrity");
			progressedProfiles = Set.copyOf(progressedProfiles);
			Objects.requireNonNull(readPool, "readPool");
			Objects.requireNonNull(writePool, "writePool");
			Objects.requireNonNull(foregroundScheduler, "foregroundScheduler");
			Objects.requireNonNull(mixedScheduler, "mixedScheduler");
			Objects.requireNonNull(priority, "priority");
		}
	}

	/** Paired performance ratio summarized with a two-sided 95% confidence interval. */
	public record RatioConfidenceInterval(int samples, double mean, double lower95, double upper95) {

		public boolean available() {
			return samples > 0 && Double.isFinite(mean) && Double.isFinite(lower95) && Double.isFinite(upper95)
					&& mean >= 0.0d && lower95 >= 0.0d && upper95 >= lower95;
		}
	}

	/** Direct scheduler evidence for priority ordering and the eight-millisecond cooperative bound. */
	public record PriorityEvidence(long foregroundLatencyExecutionP99Nanos,
			long maximumMixedLatencyQueueP99Nanos,
			long medianMixedLatencyQueueP99Nanos,
			long medianMixedAnalyticalQueueP99Nanos,
			long medianMixedIngestQueueP99Nanos,
			long medianMixedBatchQueueP99Nanos,
			int rounds,
			int latencyBeforeAnalyticalRounds,
			int ingestBeforeBatchRounds) {

		public boolean passed() {
			if (foregroundLatencyExecutionP99Nanos <= 0L
					|| maximumMixedLatencyQueueP99Nanos <= 0L
					|| medianMixedLatencyQueueP99Nanos <= 0L
					|| medianMixedAnalyticalQueueP99Nanos <= 0L
					|| medianMixedIngestQueueP99Nanos <= 0L
					|| medianMixedBatchQueueP99Nanos <= 0L
					|| rounds <= 0) {
				return false;
			}
			long latencyBound;
			try {
				latencyBound = Math.addExact(foregroundLatencyExecutionP99Nanos, COOPERATIVE_QUANTUM_NANOS);
			} catch (ArithmeticException overflow) {
				latencyBound = Long.MAX_VALUE;
			}
			int requiredOrderedRounds = Math.max(1, rounds - 1);
			return maximumMixedLatencyQueueP99Nanos <= latencyBound
					&& medianMixedLatencyQueueP99Nanos <= medianMixedAnalyticalQueueP99Nanos
					&& medianMixedIngestQueueP99Nanos <= medianMixedBatchQueueP99Nanos
					&& latencyBeforeAnalyticalRounds >= requiredOrderedRounds
					&& ingestBeforeBatchRounds >= requiredOrderedRounds;
		}

		public String detail() {
			return "maximum_latency_queue_p99_ms=" + formatMillis(maximumMixedLatencyQueueP99Nanos)
					+ ", quantum_plus_indivisible_baseline_ms="
					+ formatMillis(foregroundLatencyExecutionP99Nanos + COOPERATIVE_QUANTUM_NANOS)
					+ ", median_latency_queue_p99_ms=" + formatMillis(medianMixedLatencyQueueP99Nanos)
					+ ", median_analytical_queue_p99_ms=" + formatMillis(medianMixedAnalyticalQueueP99Nanos)
					+ ", latency_before_analytical_rounds=" + latencyBeforeAnalyticalRounds + '/' + rounds
					+ ", median_ingest_queue_p99_ms=" + formatMillis(medianMixedIngestQueueP99Nanos)
					+ ", median_batch_queue_p99_ms=" + formatMillis(medianMixedBatchQueueP99Nanos)
					+ ", ingest_before_batch_rounds=" + ingestBeforeBatchRounds + '/' + rounds;
		}
	}

	/** One deterministic acceptance assertion and its measured detail. */
	public record GateCheck(String name, boolean passed, String detail) {
	}

	/** Full acceptance result. */
	public record Acceptance(List<GateCheck> checks) {

		public Acceptance {
			checks = List.copyOf(checks);
		}

		public static Acceptance failed(String detail) {
			return new Acceptance(List.of(new GateCheck("benchmark_completed", false, detail)));
		}

		public boolean passed() {
			return checks.stream().allMatch(GateCheck::passed);
		}

		public String failedSummary() {
			return checks.stream()
					.filter(check -> !check.passed())
					.map(check -> check.name() + " (" + check.detail() + ")")
					.reduce((left, right) -> left + "; " + right)
					.orElse("none");
		}
	}

	private record Options(Path root,
			String databaseName,
			String buildId,
			String buildState,
			String storageLabel,
			String cacheState,
			String hostState,
			int minimumHostAvailableGiB,
			int preloadKeys,
			int preloadFlushKeys,
			int valueBytes,
			int warmupSeconds,
			int measureSeconds,
			int rounds,
			int pointReaders,
			int foregroundWriters,
			int maintenanceWriters,
			int firstLastReaders,
			int cancellationWorkers,
			int analyticalReaders,
			int controlWorkers,
			int cdcWorkers,
			int physicalWorkers,
			long foregroundWriteRate,
			long maintenanceWriteRate,
			long firstLastRate,
			long cancellationRate,
			long analyticalRate,
			long controlRate,
			long cdcRate,
			long physicalRate,
			int cancellationDelayMillis,
			int cancellationBurst,
			int integrityRequests,
			int pointRequestCount,
			int rangeRequestCount,
			int rangeWidth,
			int readParallelism,
			int writeParallelism,
			int foregroundQueueCapacity,
			int maintenanceQueueCapacity,
			int admissionSampleMicros,
			String instrumentationMode,
			int maxLatencySamples,
			String writeBufferSize,
			boolean directIo,
			boolean spinning,
			boolean prepareOnly,
			boolean reusePreloaded,
			boolean enforce,
			boolean smoke,
			long seed) {

		private static final Set<String> KNOWN_OPTIONS = Set.of(
				"root",
				"database-name",
				"build-id",
				"build-state",
				"storage-label",
				"cache-state",
				"host-state",
				"minimum-host-available-gib",
				"preload-keys",
				"preload-flush-keys",
				"value-bytes",
					"warmup-seconds",
					"measure-seconds",
					"rounds",
				"point-readers",
				"foreground-writers",
					"maintenance-writers",
					"first-last-readers",
					"cancellation-workers",
					"analytical-readers",
					"control-workers",
					"cdc-workers",
					"physical-workers",
					"foreground-write-rate",
					"maintenance-write-rate",
					"first-last-rate",
					"cancellation-rate",
					"analytical-rate",
					"control-rate",
					"cdc-rate",
					"physical-rate",
					"cancellation-delay-ms",
					"cancellation-burst",
					"integrity-requests",
				"point-request-count",
				"range-request-count",
				"range-width",
				"read-parallelism",
				"write-parallelism",
				"foreground-queue-capacity",
				"maintenance-queue-capacity",
				"admission-sample-micros",
				"instrumentation-mode",
				"max-latency-samples",
				"write-buffer-size",
				"direct-io",
				"spinning",
				"prepare-only",
				"reuse-preloaded",
				"enforce",
				"smoke",
				"seed");

		private static Options parse(String[] args) {
			Map<String, String> values = new LinkedHashMap<>();
			for (String argument : args) {
				if (!argument.startsWith("--") || !argument.contains("=")) {
					throw new IllegalArgumentException("Options must use --name=value: " + argument);
				}
				int equals = argument.indexOf('=');
				String previous = values.put(argument.substring(2, equals), argument.substring(equals + 1));
				if (previous != null) {
					throw new IllegalArgumentException("Duplicate option: " + argument.substring(0, equals));
				}
			}
			for (String name : values.keySet()) {
				if (!KNOWN_OPTIONS.contains(name)) {
					throw new IllegalArgumentException("Unknown option: --" + name);
				}
			}
			boolean smoke = bool(values, "smoke", false);
			long runId = System.currentTimeMillis();
			Options options = new Options(
					Path.of(values.getOrDefault("root", Path.of(System.getProperty("java.io.tmpdir"),
							"rockserver-overload-" + runId).toString())),
					values.getOrDefault("database-name", "grpc-overload-benchmark"),
					values.getOrDefault("build-id", "unverified"),
					values.getOrDefault("build-state", "unknown"),
					values.getOrDefault("storage-label", "ci-structural"),
					values.getOrDefault("cache-state", "unknown"),
					values.getOrDefault("host-state", "shared"),
					integer(values, "minimum-host-available-gib", smoke ? 0 : MIN_RELEASE_HOST_AVAILABLE_GIB),
					integer(values, "preload-keys", smoke ? 10_000 : 1_000_000),
					integer(values, "preload-flush-keys", smoke ? 2_000 : 50_000),
					integer(values, "value-bytes", 256),
					integer(values, "warmup-seconds", smoke ? 1 : 15),
					integer(values, "measure-seconds", smoke ? 2 : 60),
					integer(values, "rounds", smoke ? 1 : 5),
					integer(values, "point-readers", smoke ? 8 : 40),
					integer(values, "foreground-writers", smoke ? 2 : 4),
					integer(values, "maintenance-writers", smoke ? 8 : 64),
					integer(values, "first-last-readers", smoke ? 1 : 2),
					integer(values, "cancellation-workers", smoke ? 1 : 2),
					integer(values, "analytical-readers", smoke ? 3 : 4),
					integer(values, "control-workers", smoke ? 1 : 2),
					integer(values, "cdc-workers", smoke ? 1 : 2),
					integer(values, "physical-workers", 1),
					longValue(values, "foreground-write-rate", smoke ? 100 : 1_000),
					longValue(values, "maintenance-write-rate", 0),
					longValue(values, "first-last-rate", smoke ? 10 : 50),
					longValue(values, "cancellation-rate", smoke ? 64 : 100),
					longValue(values, "analytical-rate", 0),
					longValue(values, "control-rate", smoke ? 10 : 50),
					longValue(values, "cdc-rate", smoke ? 10 : 100),
					longValue(values, "physical-rate", 1),
					integer(values, "cancellation-delay-ms", 1),
					integer(values, "cancellation-burst", WRITE_REQUEST_VARIANTS),
					integer(values, "integrity-requests", smoke ? 64 : 1_024),
					integer(values, "point-request-count", smoke ? 1_024 : 8_192),
					integer(values, "range-request-count", smoke ? 1_024 : 8_192),
					integer(values, "range-width", smoke ? 64 : 1_024),
					integer(values, "read-parallelism", smoke ? 4 : 20),
					integer(values, "write-parallelism", smoke ? 8 : 36),
					integer(values, "foreground-queue-capacity", 4_096),
					integer(values, "maintenance-queue-capacity", 512),
					integer(values, "admission-sample-micros", 250),
					values.getOrDefault("instrumentation-mode", "strict"),
					integer(values, "max-latency-samples", smoke ? 100_000 : 1_000_000),
					values.getOrDefault("write-buffer-size", "64MiB"),
					bool(values, "direct-io", false),
					bool(values, "spinning", false),
					bool(values, "prepare-only", false),
					bool(values, "reuse-preloaded", false),
					bool(values, "enforce", !smoke),
					smoke,
					longValue(values, "seed", 0x524f434b53455256L));
			options.validate();
			return options;
		}

		private void validate() {
			if (prepareOnly && reusePreloaded) {
				throw new IllegalArgumentException("prepare-only and reuse-preloaded are mutually exclusive");
			}
			if (databaseName.isBlank()) {
				throw new IllegalArgumentException("database-name must not be blank");
			}
			if (!buildId.matches("[A-Za-z0-9._-]+")) {
				throw new IllegalArgumentException("build-id must use only letters, digits, dot, underscore, or dash");
			}
			if (!List.of("clean", "dirty", "unknown").contains(buildState)) {
				throw new IllegalArgumentException("build-state must be clean, dirty, or unknown");
			}
			if (!List.of("hdd-zfs", "hdd-btrfs", "nvme", "ci-structural").contains(storageLabel)) {
				throw new IllegalArgumentException(
						"storage-label must be hdd-zfs, hdd-btrfs, nvme, or ci-structural");
			}
			if (!List.of("cold", "warm", "unknown").contains(cacheState)) {
				throw new IllegalArgumentException("cache-state must be cold, warm, or unknown");
			}
			if (!List.of("dedicated", "shared", "unknown").contains(hostState)) {
				throw new IllegalArgumentException("host-state must be dedicated, shared, or unknown");
			}
			if (minimumHostAvailableGiB < 0 || minimumHostAvailableGiB > 1_024) {
				throw new IllegalArgumentException("minimum-host-available-gib must be between 0 and 1024");
			}
			if (cacheState.equals("cold") && !reusePreloaded) {
				throw new IllegalArgumentException("cache-state=cold requires reuse-preloaded=true");
			}
			if (preloadKeys < 2 || preloadFlushKeys < 1 || valueBytes < 1) {
				throw new IllegalArgumentException("preload and value sizes must be positive");
			}
			if (warmupSeconds < 0 || measureSeconds < 1 || rounds < 1) {
				throw new IllegalArgumentException("warmup must be non-negative and measurement/rounds must be positive");
			}
			if (pointReaders < 1 || foregroundWriters < 1 || maintenanceWriters < 1
					|| firstLastReaders < 1 || cancellationWorkers < 0 || analyticalReaders < 1
					|| controlWorkers < 1 || cdcWorkers < 1 || physicalWorkers < 1) {
				throw new IllegalArgumentException("all mixed-workload worker counts must be positive");
			}
			if (foregroundWriteRate < 0 || maintenanceWriteRate < 0 || firstLastRate < 0
					|| cancellationRate < 0 || analyticalRate < 0 || controlRate < 0
					|| cdcRate < 0 || physicalRate < 0) {
				throw new IllegalArgumentException("rates must be non-negative; zero means unbounded");
			}
			if (cancellationWorkers > 0 && cancellationRate == 0) {
				throw new IllegalArgumentException("cancellation-rate must be positive when cancellation workers exist");
			}
			if (cancellationDelayMillis < 0 || cancellationDelayMillis >= DEADLINE_MILLIS) {
				throw new IllegalArgumentException("cancellation delay must be within the fixed deadline");
			}
			if (cancellationBurst < 1 || cancellationBurst > WRITE_REQUEST_VARIANTS) {
				throw new IllegalArgumentException("cancellation-burst must be between 1 and "
						+ WRITE_REQUEST_VARIANTS);
			}
			if (integrityRequests < 1) {
				throw new IllegalArgumentException("integrity-requests must be positive");
			}
			if ((long) measureSeconds * cancellationRate < (long) cancellationWorkers * cancellationBurst) {
				throw new IllegalArgumentException("measurement window must contain at least one cancellation burst");
			}
			if (pointRequestCount < 1 || rangeRequestCount < 1 || rangeWidth < 1 || rangeWidth > preloadKeys) {
				throw new IllegalArgumentException("request counts and range width are invalid for the preload");
			}
			if (readParallelism < 1 || writeParallelism < 1) {
				throw new IllegalArgumentException("parallelism limits are invalid");
			}
			if (foregroundQueueCapacity < 1 || maintenanceQueueCapacity < 1) {
				throw new IllegalArgumentException("queue capacities must be positive");
			}
			if (admissionSampleMicros < 1 || maxLatencySamples < 1) {
				throw new IllegalArgumentException("sampling settings must be positive");
			}
			if (!List.of("strict", "portable").contains(instrumentationMode)) {
				throw new IllegalArgumentException("instrumentation-mode must be strict or portable");
			}
			if (enforce) {
				if (smoke || !instrumentationMode.equals("strict")
						|| !isFullGitSha(buildId) || !buildState.equals("clean")
						|| !hostState.equals("dedicated")
						|| storageLabel.equals("ci-structural")
						|| minimumHostAvailableGiB < MIN_RELEASE_HOST_AVAILABLE_GIB) {
					throw new IllegalArgumentException("enforced release runs require smoke=false, strict instrumentation, "
							+ "a full lowercase Git SHA, "
							+ "build-state=clean, host-state=dedicated, a hardware storage label, and at least "
							+ MIN_RELEASE_HOST_AVAILABLE_GIB + " GiB host MemAvailable");
				}
				if (preloadKeys < 1_000_000 || valueBytes < 256 || warmupSeconds < 15 || measureSeconds < 60
						|| rounds < 5 || pointReaders < readParallelism || maintenanceWriters < writeParallelism
						|| analyticalReaders < 1 || cancellationWorkers < 2 || integrityRequests < 1_024
						|| rangeWidth < 1_024 || foregroundQueueCapacity < 4_096
						|| maintenanceQueueCapacity < 512) {
					throw new IllegalArgumentException("enforced release runs require the full five-round dataset, "
							+ "duration, saturation, cancellation, integrity, range, and queue profile");
				}
				if (!prepareOnly && (!reusePreloaded || !cacheState.equals("cold"))) {
					throw new IllegalArgumentException(
							"enforced measurement requires a reused prepared dataset and cache-state=cold");
				}
			}
		}

		private static boolean isFullGitSha(String value) {
			return value.matches("[0-9a-f]{40}");
		}

		private static int integer(Map<String, String> values, String name, int defaultValue) {
			return Integer.parseInt(values.getOrDefault(name, Integer.toString(defaultValue)));
		}

		private static long longValue(Map<String, String> values, String name, long defaultValue) {
			return Long.parseLong(values.getOrDefault(name, Long.toString(defaultValue)));
		}

		private static boolean bool(Map<String, String> values, String name, boolean defaultValue) {
			String value = values.getOrDefault(name, Boolean.toString(defaultValue));
			return switch (value) {
				case "true" -> true;
				case "false" -> false;
				default -> throw new IllegalArgumentException(
						"Boolean option --" + name + " must be true or false: " + value);
			};
		}
	}

	@FunctionalInterface
	private interface ThrowingRunnable {

		void run() throws Exception;
	}
}
