package it.cavallium.rockserver.core.impl.benchmark;

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
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RawSstToken;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.SerializedKVBatch.SerializedKVBatchRef;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import it.cavallium.rockserver.core.common.api.proto.ScanRawRequest;
import it.cavallium.rockserver.core.config.DataSize;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.RocksLeakDetector;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;

/**
 * Opt-in paired raw-SST gate over the generated gRPC client/server path.
 *
 * <p>The controller creates one immutable, explicitly flushed SST dataset. It then launches the
 * untouched and candidate production classpaths in alternating order, using the same Java binary,
 * JVM options, database, configuration, and host. Every child warms the complete scan before its
 * measured interval. The controller rejects statistically demonstrated raw-throughput or scheduler
 * queue-p99 regressions. Ordinary Maven tests exercise only the pure comparison and option contracts.</p>
 */
public final class GrpcRawScanBenchmark {

	private static final String RESULT_SCHEMA = "rockserver-grpc-raw-scan-comparison-v4";
	private static final String WORKER_SCHEMA = "rockserver-grpc-raw-scan-worker-v3";
	private static final String DATASET_SCHEMA = "rockserver-grpc-raw-scan-dataset-v2";
	private static final String COLUMN_NAME = "grpc-raw-scan-benchmark";
	private static final long VALUE_SEED = 0x5241575343414e31L;
	private static final long COOPERATIVE_QUANTUM_NANOS = TimeUnit.MILLISECONDS.toNanos(8L);
	private static final long STREAM_DEADLINE_MINUTES = 15L;
	private static final int RAW_MAX_ENTRIES = 65_536;
	private static final int RAW_MAX_SERIALIZED_BYTES = 3 * 1024 * 1024;
	// Keep the controller binary-compatible with the immutable baseline production classes.
	private static final int DEFAULT_RAW_SCAN_FILE_CONCURRENCY = 4;
	private static final long DEFAULT_RAW_SCAN_READAHEAD_BYTES = 8L * 1024L * 1024L;
	private static final int MAX_RAW_SCAN_FILE_CONCURRENCY = 64;
	private static final String PERFORMANCE_BASELINE_SHA =
			"bb4f1a7e90db1fdfd785936594d080e8c4a0ba4e";
	private static final Set<String> WORKER_KEYS = Set.of(
			"schema", "implementation", "round", "build-sha", "classpath-sha256", "dataset-id",
			"bytes-per-second", "entries-per-second", "scans", "entries", "batches", "full-batches",
			"maximum-batch-bytes", "scan-p99-nanos", "queue-p99-nanos", "execution-p99-nanos",
			"cpu-nanos-per-entry", "allocated-bytes-per-entry", "gc-collections", "gc-millis",
			"peak-live-heap-bytes", "peak-direct-memory-bytes", "peak-resident-set-bytes",
			"peak-thread-count", "peak-native-handles", "submitted-requests", "terminal-requests",
			"duplicate-terminals", "in-flight-requests", "non-ok-requests", "scheduler-accepted",
			"scheduler-submission-attempts", "scheduler-accounting-exact", "scheduler-started",
			"scheduler-completed", "scheduler-outcomes", "scheduler-failures",
			"sampler-samples", "saturating-demand-samples", "maximum-active", "worker-count",
			"maximum-parked", "maximum-outstanding", "maximum-avoidable-idle-nanos",
			"exact-waiting-workers", "resources-drained", "native-leaks", "passed");

	private GrpcRawScanBenchmark() {
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
		GrpcRetainedReadBenchmark.verifyBuildCheckout(options.baselineClasses(),
				options.buildBaseline(), options.buildStateBaseline());
		GrpcRetainedReadBenchmark.verifyBuildCheckout(options.candidateClasses(),
				options.buildCandidate(), options.buildStateCandidate());
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

		String currentClassPath = System.getProperty("java.class.path");
		String baselineClassPath = replaceProductionClasses(currentClassPath,
				options.candidateClasses(), options.baselineClasses());
		String candidateClassPath = replaceProductionClasses(currentClassPath,
				options.candidateClasses(), options.candidateClasses());
		String baselineClassPathHash = GrpcRetainedReadBenchmark
				.classPathContentSha256ForTesting(baselineClassPath);
		String candidateClassPathHash = GrpcRetainedReadBenchmark
				.classPathContentSha256ForTesting(candidateClassPath);

		Instant started = Instant.now();
		FileStore store = Files.getFileStore(root);
		writeControllerMetadata(root, options, store, started, datasetId,
				baselineClassPathHash, candidateClassPathHash);
		prepareDataset(shared.resolve("db"), config, options, datasetId);

		List<WorkerResult> results = new ArrayList<>(options.rounds() * 2);
		for (int round = 1; round <= options.rounds(); round++) {
			boolean baselineFirst = (round & 1) == 1;
			if (baselineFirst) {
				results.add(runChild(options, shared, root, round, Implementation.BASELINE,
						baselineClassPath, baselineClassPathHash, datasetId));
				results.add(runChild(options, shared, root, round, Implementation.CANDIDATE,
						candidateClassPath, candidateClassPathHash, datasetId));
			} else {
				results.add(runChild(options, shared, root, round, Implementation.CANDIDATE,
						candidateClassPath, candidateClassPathHash, datasetId));
				results.add(runChild(options, shared, root, round, Implementation.BASELINE,
						baselineClassPath, baselineClassPathHash, datasetId));
			}
		}

		Comparison comparison = compare(results, options);
		writeReports(root, options, store, started, Instant.now(), results, comparison);
		System.out.println(toMarkdown(options, store, started, Instant.now(), results, comparison));
		System.out.println("Machine-readable results: " + root.resolve("results.json"));
		System.out.println("Human-readable results: " + root.resolve("results.md"));
		if (options.enforce() && !comparison.passed()) {
			throw new IllegalStateException("Raw-scan comparison failed: " + comparison.failedSummary());
		}
	}

	private static WorkerResult runChild(Options options,
			Path shared,
			Path root,
			int round,
			Implementation implementation,
			String classPath,
			String classPathHash,
			String datasetId) throws Exception {
		Path output = root.resolve("round-%02d-%s.properties".formatted(round, implementation.value));
		List<String> command = new ArrayList<>(List.of(
				Path.of(System.getProperty("java.home"), "bin", "java").toString(),
				"--enable-native-access=ALL-UNNAMED",
				"-Drockserver.raw.expected-classpath-sha256=" + classPathHash,
				"-Drockserver.raw.dataset-id=" + datasetId,
				"-Xms" + options.childHeap(),
				"-Xmx" + options.childHeap(),
				"-cp", classPath,
				GrpcRawScanBenchmark.class.getName(),
				"--worker=true",
				"--root=" + root,
				"--dataset-root=" + shared,
				"--output=" + output,
				"--implementation=" + implementation.value,
				"--round=" + round,
				"--build-baseline=" + options.buildBaseline(),
				"--build-candidate=" + options.buildCandidate(),
				"--build-state-baseline=" + options.buildStateBaseline(),
				"--build-state-candidate=" + options.buildStateCandidate(),
				"--storage-label=" + options.storageLabel(),
				"--host-state=" + options.hostState(),
				"--preload-keys=" + options.preloadKeys(),
				"--flush-keys=" + options.flushKeys(),
				"--value-bytes=" + options.valueBytes(),
				"--batch-entries=" + options.batchEntries(),
				"--scan-clients=" + options.scanClients(),
				"--read-parallelism=" + options.readParallelism(),
				"--write-parallelism=" + options.writeParallelism()));
		// The immutable baseline predates these CLI options and rejects unknown flags.
		// Its EmbeddedDB also retains the historical hard-coded 4-reader/8-MiB behavior.
		if (implementation == Implementation.CANDIDATE) {
			command.add("--raw-scan-file-concurrency=" + options.rawScanFileConcurrency());
			command.add("--raw-scan-readahead-bytes=" + options.rawScanReadaheadBytes() + "B");
		}
		command.addAll(List.of(
				"--warmup-passes=" + options.warmupPasses(),
				"--measure-seconds=" + options.measureSeconds(),
				"--sample-micros=" + options.sampleMicros(),
				"--instrumentation-mode=" + (implementation == Implementation.CANDIDATE ? "strict" : "portable"),
				"--enforce=false",
				"--smoke=" + options.smoke()));
		System.out.printf(Locale.ROOT, "Starting raw-scan round %d %s%n", round, implementation.value);
		Process process = new ProcessBuilder(command).inheritIO().start();
		if (!process.waitFor(45, TimeUnit.MINUTES)) {
			process.destroyForcibly();
			throw new IllegalStateException("Raw-scan child timed out: round=" + round
					+ " implementation=" + implementation.value);
		}
		if (process.exitValue() != 0) {
			throw new IllegalStateException("Raw-scan child failed with exit " + process.exitValue()
					+ ": round=" + round + " implementation=" + implementation.value);
		}
		return WorkerResult.read(output, implementation, round,
				implementation == Implementation.BASELINE ? options.buildBaseline() : options.buildCandidate(),
				classPathHash, datasetId);
	}

	private static void runWorker(Options options) throws Exception {
		BenchmarkProcessTelemetry.enableAllocationMeasurement();
		String expectedClassPathHash = System.getProperty("rockserver.raw.expected-classpath-sha256", "");
		String expectedDatasetId = System.getProperty("rockserver.raw.dataset-id", "");
		String actualClassPathHash = GrpcRetainedReadBenchmark.classPathContentSha256ForTesting(
				System.getProperty("java.class.path"));
		if (!expectedClassPathHash.matches("[0-9a-f]{64}")
				|| !expectedClassPathHash.equals(actualClassPathHash)) {
			throw new IllegalArgumentException("Raw worker classpath fingerprint mismatch");
		}
		validateDatasetMetadata(options, expectedDatasetId);
		long leaksBefore = RocksLeakDetector.detectedLeakCount();
		EmbeddedConnection embedded = null;
		GrpcServer server = null;
		ManagedChannel channel = null;
		WorkerMeasurement measurement;
		try {
			Path shared = options.datasetRoot().toAbsolutePath().normalize();
			embedded = new EmbeddedConnection(shared.resolve("db"), "grpc-raw-scan-benchmark",
					shared.resolve("rockserver.conf"));
			long columnId = embedded.getSyncApi(RequestContext.batch()).getColumnId(COLUMN_NAME);
			server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0));
			server.start();
			var tracker = new RequestTracker();
			channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
					.directExecutor()
					.usePlaintext()
					.disableRetry()
					.maxInboundMessageSize(64 * 1024 * 1024)
					.build();
			Channel tracked = ClientInterceptors.intercept(channel, tracker);
			var stub = RocksDBServiceGrpc.newBlockingStub(tracked)
					.withDeadlineAfter(STREAM_DEADLINE_MINUTES, TimeUnit.MINUTES);
			var requestBuilder = ScanRawRequest.newBuilder()
					.setColumnId(columnId)
					.setShardIndex(0)
					.setShardCount(1)
					.setContext(it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
							.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH)
							.setDeadlineEpochMillis(Long.MAX_VALUE)
							.build());
			// The untouched baseline may predate the extension. Invoke the new builder
			// method only in the candidate process so the same benchmark can compare
			// baseline raw scanning with the exact resumable candidate path.
			if (options.implementation() == Implementation.CANDIDATE) {
				ResumableProtocol.enable(requestBuilder);
			}
			ScanRawRequest request = requestBuilder.build();

			byte[] expectedValue = valueBytes(options.valueBytes());
			runWarmup(stub, request, options, expectedValue);
			measurement = measure(stub, tracker, request, embedded, options, expectedValue);
			awaitDrain(embedded);
		} finally {
			Throwable closeFailure = null;
			if (channel != null) {
				channel.shutdownNow();
				if (!channel.awaitTermination(10, TimeUnit.SECONDS)) {
					closeFailure = new IllegalStateException("gRPC channel did not terminate");
				}
			}
			if (server != null) {
				try {
					server.close();
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
			if (closeFailure != null) {
				throw rethrow(closeFailure);
			}
		}
		long leaks = awaitNativeLeakDetection(leaksBefore);
		String buildSha = options.implementation() == Implementation.BASELINE
				? options.buildBaseline() : options.buildCandidate();
		WorkerResult result = WorkerResult.from(options, buildSha, actualClassPathHash,
				expectedDatasetId, measurement, leaks);
		result.write(options.output());
		System.out.printf(Locale.ROOT,
				"RAW_RESULT implementation=%s round=%d throughput=%.3f MiB/s entries=%.0f/s "
						+ "scan_p99=%.3fms queue_p99=%.3fms max_active=%d requests=%d/%d passed=%s%n",
				result.implementation().value, result.round(), result.bytesPerSecond() / (1024 * 1024),
				result.entriesPerSecond(), result.scanP99Nanos() / 1_000_000.0,
				result.queueP99Nanos() / 1_000_000.0, result.maximumActive(), result.terminalRequests(),
				result.submittedRequests(), result.passed());
	}

	private static void prepareDataset(Path database,
			Path config,
			Options options,
			String datasetId) throws Exception {
		System.out.printf(Locale.ROOT, "Preparing %,d raw-scan keys with a flush every %,d keys%n",
				options.preloadKeys(), options.flushKeys());
		long leaksBefore = RocksLeakDetector.detectedLeakCount();
		EmbeddedConnection connection = new EmbeddedConnection(database, "grpc-raw-scan-benchmark", config);
		try {
			var api = connection.getSyncApi(RequestContext.batch());
			long columnId = api.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			Buf value = Buf.wrap(valueBytes(options.valueBytes()));
			int loaded = 0;
			while (loaded < options.preloadKeys()) {
				int count = Math.min(options.batchEntries(), options.preloadKeys() - loaded);
				List<Keys> keys = new ArrayList<>(count);
				List<Buf> values = new ArrayList<>(count);
				for (int offset = 0; offset < count; offset++) {
					keys.add(key(loaded + offset));
					values.add(value);
				}
				api.putBatch(columnId, Flux.just(new KVBatch.KVBatchRef(keys, values)), PutBatchMode.WRITE_BATCH);
				loaded += count;
				if (loaded % options.flushKeys() == 0 || loaded == options.preloadKeys()) {
					api.flush();
					System.out.printf(Locale.ROOT, "Prepared %,d / %,d keys%n", loaded, options.preloadKeys());
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
		Files.writeString(database.getParent().resolve("dataset.properties"),
				"schema=" + DATASET_SCHEMA + "\n"
						+ "dataset-id=" + datasetId + "\n"
						+ "preload-keys=" + options.preloadKeys() + "\n"
						+ "flush-keys=" + options.flushKeys() + "\n"
						+ "value-bytes=" + options.valueBytes() + "\n"
						+ "value-seed=" + Long.toUnsignedString(VALUE_SEED) + "\n",
				StandardOpenOption.CREATE_NEW);
	}

	private static void runWarmup(RocksDBServiceGrpc.RocksDBServiceBlockingStub stub,
			ScanRawRequest request,
			Options options,
			byte[] expectedValue) throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(options.scanClients(),
				Thread.ofPlatform().name("raw-scan-warmup-", 0).factory());
		try {
			List<Future<?>> futures = new ArrayList<>();
			for (int client = 0; client < options.scanClients(); client++) {
				futures.add(executor.submit(() -> {
					for (int pass = 0; pass < options.warmupPasses(); pass++) {
						scanOnce(stub, request, options.preloadKeys(), expectedValue,
								options.implementation() == Implementation.CANDIDATE);
					}
					return null;
				}));
			}
			for (Future<?> future : futures) {
				future.get();
			}
		} finally {
			executor.shutdownNow();
			if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
				throw new IllegalStateException("Raw-scan warmup workers did not terminate");
			}
		}
	}

	private static WorkerMeasurement measure(RocksDBServiceGrpc.RocksDBServiceBlockingStub stub,
			RequestTracker tracker,
			ScanRawRequest request,
			EmbeddedConnection embedded,
			Options options,
			byte[] expectedValue) throws Exception {
		var meterRegistry = new BenchmarkMeterRegistry();
		var composite = (CompositeMeterRegistry) embedded.getEmbeddedDB().getMetricsRegistry();
		composite.add(meterRegistry);
		ExecutorService executor = Executors.newFixedThreadPool(options.scanClients() + 1,
				Thread.ofPlatform().name("raw-scan-measure-", 0).factory());
		CountDownLatch ready = new CountDownLatch(options.scanClients() + 1);
		CountDownLatch start = new CountDownLatch(1);
		AtomicBoolean done = new AtomicBoolean();
		LongAdder scans = new LongAdder();
		LongAdder entries = new LongAdder();
		LongAdder bytes = new LongAdder();
		LongAdder batches = new LongAdder();
		LongAdder fullBatches = new LongAdder();
		AtomicInteger maximumBatchBytes = new AtomicInteger();
		ScanLatencyRecorder scanLatencies = new ScanLatencyRecorder(
				Math.max(1_024, options.scanClients() * 1_024));
		PoolSampler sampler = new PoolSampler(options);
		List<Future<?>> futures = new ArrayList<>();
		long[] deadline = new long[1];
		try {
			for (int client = 0; client < options.scanClients(); client++) {
				futures.add(executor.submit(() -> {
					ready.countDown();
					start.await();
					boolean first = true;
					while (first || System.nanoTime() < deadline[0]) {
						first = false;
						long scanStarted = System.nanoTime();
						ScanResult result = scanOnce(stub, request, options.preloadKeys(), expectedValue,
								options.implementation() == Implementation.CANDIDATE);
						scanLatencies.record(System.nanoTime() - scanStarted);
						scans.increment();
						entries.add(result.entries());
						bytes.add(result.serializedBytes());
						batches.add(result.batches());
						fullBatches.add(result.fullBatches());
						maximumBatchBytes.accumulateAndGet(result.maximumBatchBytes(), Math::max);
					}
					return null;
				}));
			}
			Future<?> samplerFuture = executor.submit(() -> {
				ready.countDown();
				start.await();
				while (!done.get()) {
					sampler.sample(embedded);
					LockSupport.parkNanos(options.sampleNanos());
					if (Thread.interrupted()) {
						throw new InterruptedException();
					}
				}
				sampler.sample(embedded);
				return null;
			});
			if (!ready.await(30, TimeUnit.SECONDS)) {
				throw new IllegalStateException("Raw-scan measurement workers did not become ready");
			}
			sampler.resetMeasurementPeaks();
			PoolCounters before = PoolCounters.capture(embedded);
			BenchmarkProcessTelemetry.ProcessSnapshot processBefore =
					BenchmarkProcessTelemetry.processSnapshot();
			tracker.startTracking();
			long started = System.nanoTime();
			deadline[0] = started + TimeUnit.SECONDS.toNanos(options.measureSeconds());
			start.countDown();
			for (Future<?> future : futures) {
				future.get();
			}
			long finished = System.nanoTime();
			BenchmarkProcessTelemetry.ProcessSnapshot processAfter =
					BenchmarkProcessTelemetry.processSnapshot();
			done.set(true);
			samplerFuture.get();
			tracker.stopTracking();
			RequestAccounting accounting = tracker.awaitSnapshot();
			awaitDrain(embedded);
			PoolCounters after = PoolCounters.capture(embedded);
			BenchmarkProcessTelemetry.ProcessDelta process = processAfter.minus(processBefore);
			SchedulerMetrics schedulerMetrics = schedulerMetrics(meterRegistry);
			long[] sortedLatencies = scanLatencies.sorted();
			long measuredEntries = entries.sum();
			return new WorkerMeasurement(finished - started,
					scans.sum(), measuredEntries, bytes.sum(), batches.sum(), fullBatches.sum(),
					maximumBatchBytes.get(), GrpcOverloadBenchmark.percentile(sortedLatencies, 0.99d),
					process.cpuNanos() / (double) measuredEntries,
					process.allocatedBytes() / (double) measuredEntries,
					process.gcCollections(), process.gcMillis(), schedulerMetrics, accounting,
					sampler.snapshot(), after.minus(before), true);
		} finally {
			done.set(true);
			start.countDown();
			tracker.stopTrackingIfActive();
			executor.shutdownNow();
			executor.awaitTermination(10, TimeUnit.SECONDS);
			sampler.close();
			composite.remove(meterRegistry);
			meterRegistry.close();
		}
	}

	private static ScanResult scanOnce(RocksDBServiceGrpc.RocksDBServiceBlockingStub stub,
			ScanRawRequest request,
			int expectedEntries,
			byte[] expectedValue,
			boolean resumable) {
		BitSet seen = new BitSet(expectedEntries);
		long entries = 0L;
		long serializedBytes = 0L;
		long batches = 0L;
		long fullBatches = 0L;
		long completedSsts = 0L;
		int maximumBatchBytes = 0;
		var responses = stub.scanRaw(request);
		while (responses.hasNext()) {
			var response = responses.next();
			if (resumable) {
				String completionToken = ResumableProtocol.completionToken(response);
				if (completionToken != null) {
					validateRawSstToken(completionToken);
					completedSsts++;
				}
				if (ResumableProtocol.completionOnly(response)) {
					continue;
				}
			}
			var bytes = response.getSerialized();
			int serializedSize = bytes.size();
			if (serializedSize < Integer.BYTES || serializedSize > RAW_MAX_SERIALIZED_BYTES) {
				throw new IllegalStateException("Raw wire batch has invalid size: " + serializedSize);
			}
			var batch = new SerializedKVBatchRef(Buf.wrap(bytes.toByteArray()));
			int declaredEntries = batch.serialized().getIntLE(0);
			if (declaredEntries < 1 || declaredEntries > RAW_MAX_ENTRIES) {
				throw new IllegalStateException("Raw wire batch has invalid entry count: " + declaredEntries);
			}
			long decoded = 0L;
			try (var values = batch.decode()) {
				var iterator = values.iterator();
				while (iterator.hasNext()) {
					var kv = iterator.next();
					if (kv.keys().keys().length != 1 || kv.keys().keys()[0].size() != Long.BYTES) {
						throw new IllegalStateException("Raw scan returned an invalid key shape");
					}
					long key = ByteBuffer.wrap(kv.keys().keys()[0].toByteArray()).getLong();
					if (key < 0 || key >= expectedEntries) {
						throw new IllegalStateException("Raw scan returned an out-of-range key: " + key);
					}
					if (seen.get((int) key)) {
						throw new IllegalStateException("Raw scan returned a duplicate key: " + key);
					}
					seen.set((int) key);
					var value = kv.value();
					if (value == null || !Arrays.equals(expectedValue, value.toByteArray())) {
						throw new IllegalStateException("Raw scan returned a corrupt value for key " + key);
					}
					decoded++;
				}
			}
			if (decoded != declaredEntries) {
				throw new IllegalStateException("Raw wire batch declared " + declaredEntries
						+ " entries but decoded " + decoded);
			}
			entries += decoded;
			serializedBytes += serializedSize;
			batches++;
			if (serializedSize >= 2_000_000 || declaredEntries == RAW_MAX_ENTRIES) {
				fullBatches++;
			}
			maximumBatchBytes = Math.max(maximumBatchBytes, serializedSize);
		}
		if (entries != expectedEntries || seen.cardinality() != expectedEntries) {
			throw new IllegalStateException("Raw scan content mismatch: entries=" + entries
					+ " unique=" + seen.cardinality() + " expected=" + expectedEntries);
		}
		if (resumable && completedSsts == 0L) {
			throw new IllegalStateException("Resumable raw scan returned no SST completion tokens");
		}
		return new ScanResult(entries, serializedBytes, batches, fullBatches, maximumBatchBytes);
	}

	private static void validateRawSstToken(String token) {
		try {
			new RawSstToken(token);
		} catch (IllegalArgumentException malformedToken) {
			throw new IllegalStateException(
					"Resumable raw scan returned a malformed SST completion token", malformedToken);
		}
	}

	/**
	 * Keeps candidate-only protobuf accessors out of the benchmark controller and
	 * legacy worker's linked bytecode. The helper is loaded only by candidate workers.
	 */
	private static final class ResumableProtocol {

		private static void enable(ScanRawRequest.Builder request) {
			request.setResumable(true)
					.setCoalesceCompletedSstToken(true);
		}

		private static @Nullable String completionToken(
				it.cavallium.rockserver.core.common.api.proto.ScanRawResponse response) {
			if (response.hasCompletedSstTokenAfterBatch()) {
				return response.getCompletedSstTokenAfterBatch();
			}
			return response.hasCompletedSstToken() ? response.getCompletedSstToken() : null;
		}

		private static boolean completionOnly(
				it.cavallium.rockserver.core.common.api.proto.ScanRawResponse response) {
			return response.hasCompletedSstToken();
		}
	}

	private static SchedulerMetrics schedulerMetrics(BenchmarkMeterRegistry registry) {
		long queue = 0L;
		long execution = 0L;
		for (Meter meter : registry.getMeters()) {
			if (!(meter instanceof Timer timer)
					|| !"batch".equals(meter.getId().getTag("profile"))
					|| !"read".equals(meter.getId().getTag("resource"))) {
				continue;
			}
			long p99 = timerP99(timer);
			if ("rockserver.workload.queue.wait".equals(meter.getId().getName())) {
				queue = Math.max(queue, p99);
			} else if ("rockserver.workload.execution".equals(meter.getId().getName())) {
				execution = Math.max(execution, p99);
			}
		}
		return new SchedulerMetrics(queue, execution);
	}

	private static long timerP99(Timer timer) {
		long result = 0L;
		for (var percentile : timer.takeSnapshot().percentileValues()) {
			if (percentile.percentile() >= 0.99d) {
				result = Math.max(result, (long) percentile.value(TimeUnit.NANOSECONDS));
			}
		}
		return result;
	}

	private static void awaitDrain(EmbeddedConnection embedded) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L);
		do {
			var read = embedded.getScheduler().poolSnapshot(RWScheduler.Pool.READ);
			int outstanding = BenchmarkSchedulerTelemetry.outstandingTasks(read);
			int parked = BenchmarkSchedulerTelemetry.parkedTasks(read, outstanding);
			long attempts = BenchmarkSchedulerTelemetry.submissionAttempts(read);
			long expectedOutcomes = BenchmarkSchedulerTelemetry.exactAccounting()
					? attempts : read.acceptedTasks();
			var db = embedded.getInternalDB();
			if (read.queuedTasks() == 0 && read.activeTasks() == 0 && parked == 0 && outstanding == 0
					&& BenchmarkSchedulerTelemetry.terminalOutcomes(read) == expectedOutcomes
					&& db.getPendingOpsCount() == 0L
					&& db.getOpenTransactionsCount() == 0
					&& db.getOpenIteratorsCount() == 0
					&& db.getActiveRangeCursorCount() == 0) {
				return;
			}
			Thread.sleep(10L);
		} while (System.nanoTime() < deadline);
		throw new IllegalStateException("Raw-scan resources did not drain");
	}

	private static Comparison compare(List<WorkerResult> results, Options options) {
		List<WorkerResult> baseline = byImplementation(results, Implementation.BASELINE);
		List<WorkerResult> candidate = byImplementation(results, Implementation.CANDIDATE);
		if (baseline.size() != options.rounds() || candidate.size() != options.rounds()) {
			return Comparison.failed("Every round must contain one baseline and one candidate result");
		}
		List<String> structuralFailures = new ArrayList<>();
		if (!results.stream().allMatch(WorkerResult::passed)) {
			structuralFailures.add("one or more worker correctness/work-conservation gates failed");
		}
		for (int index = 0; index < baseline.size(); index++) {
			if (baseline.get(index).round() != index + 1 || candidate.get(index).round() != index + 1) {
				structuralFailures.add("round pairing mismatch at pair " + (index + 1));
			}
		}
		Map<String, PairedPerformanceContract.MetricSamples> samples = new LinkedHashMap<>();
		putSamples(samples, "mib-per-second", baseline, candidate,
				result -> result.bytesPerSecond() / (1024.0d * 1024.0d));
		putSamples(samples, "entries-per-second", baseline, candidate, WorkerResult::entriesPerSecond);
		putSamples(samples, "queue-p99", baseline, candidate, WorkerResult::queueP99Nanos);
		putSamples(samples, "execution-p99", baseline, candidate, WorkerResult::executionP99Nanos);
		putSamples(samples, "scan-p99", baseline, candidate, WorkerResult::scanP99Nanos);
		putSamples(samples, "cpu-nanos-per-entry", baseline, candidate, WorkerResult::cpuNanosPerEntry);
		putSamples(samples, "allocated-bytes-per-entry", baseline, candidate,
				WorkerResult::allocatedBytesPerEntry);
		putSamples(samples, "peak-live-heap", baseline, candidate, WorkerResult::peakLiveHeapBytes);
		putSamples(samples, "peak-direct-memory", baseline, candidate, WorkerResult::peakDirectMemoryBytes);
		putSamples(samples, "peak-resident-set", baseline, candidate, WorkerResult::peakResidentSetBytes);
		putSamples(samples, "gc-collections", baseline, candidate, WorkerResult::gcCollections);
		putSamples(samples, "gc-millis", baseline, candidate, WorkerResult::gcMillis);
		putSamples(samples, "peak-thread-count", baseline, candidate, WorkerResult::peakThreadCount);
		putSamples(samples, "peak-native-handles", baseline, candidate, WorkerResult::peakNativeHandles);
		putSamples(samples, "peak-parked", baseline, candidate, WorkerResult::maximumParked);
		putSamples(samples, "peak-outstanding", baseline, candidate, WorkerResult::maximumOutstanding);
		var evaluation = PairedPerformanceContract.evaluate(rawMetricSpecifications(), samples,
				structuralFailures, true);
		Map<String, GrpcOverloadBenchmark.RatioConfidenceInterval> intervals = new LinkedHashMap<>();
		evaluation.metrics().forEach((name, result) -> intervals.put(name, toLegacyInterval(result.interval())));
		return new Comparison(intervals, evaluation.materialImprovements(),
				evaluation.exceptionCandidates(), evaluation.failures());
	}

	private static List<PairedPerformanceContract.MetricSpec> rawMetricSpecifications() {
		return List.of(
				PairedPerformanceContract.MetricSpec.throughput("mib-per-second", true),
				PairedPerformanceContract.MetricSpec.throughput("entries-per-second", true),
				PairedPerformanceContract.MetricSpec.cost("queue-p99", false),
				PairedPerformanceContract.MetricSpec.cost("execution-p99", false),
				PairedPerformanceContract.MetricSpec.cost("scan-p99", true),
				PairedPerformanceContract.MetricSpec.cost("cpu-nanos-per-entry", true),
				PairedPerformanceContract.MetricSpec.allocation("allocated-bytes-per-entry", true),
				PairedPerformanceContract.MetricSpec.cost("peak-live-heap", true),
				PairedPerformanceContract.MetricSpec.cost("peak-direct-memory", true),
				PairedPerformanceContract.MetricSpec.cost("peak-resident-set", true),
				PairedPerformanceContract.MetricSpec.noIncrease("gc-collections"),
				PairedPerformanceContract.MetricSpec.noIncrease("gc-millis"),
				PairedPerformanceContract.MetricSpec.noIncrease("peak-thread-count"),
				PairedPerformanceContract.MetricSpec.noIncrease("peak-native-handles"),
				PairedPerformanceContract.MetricSpec.noIncrease("peak-parked"),
				PairedPerformanceContract.MetricSpec.noIncrease("peak-outstanding"));
	}

	/** Pure v1.3.11 Pareto helper for deterministic raw-suite contract tests. */
	public static PairedPerformanceContract.Evaluation evaluateParetoForTesting(
			Map<String, PairedPerformanceContract.MetricSamples> samples,
			List<String> structuralFailures,
			boolean requireMaterialImprovement) {
		return PairedPerformanceContract.evaluate(rawMetricSpecifications(), samples,
				structuralFailures, requireMaterialImprovement);
	}

	private static void putSamples(Map<String, PairedPerformanceContract.MetricSamples> target,
	                               String name,
	                               List<WorkerResult> baseline,
	                               List<WorkerResult> candidate,
	                               java.util.function.ToDoubleFunction<WorkerResult> metric) {
		target.put(name, new PairedPerformanceContract.MetricSamples(
				baseline.stream().mapToDouble(metric).toArray(),
				candidate.stream().mapToDouble(metric).toArray()));
	}

	private static GrpcOverloadBenchmark.RatioConfidenceInterval toLegacyInterval(
			PairedBenchmarkStatistics.RatioConfidenceInterval interval) {
		return new GrpcOverloadBenchmark.RatioConfidenceInterval(interval.samples(), interval.mean(),
				interval.lower95(), interval.upper95());
	}

	private static List<WorkerResult> byImplementation(List<WorkerResult> results,
			Implementation implementation) {
		return results.stream()
				.filter(result -> result.implementation() == implementation)
				.sorted(java.util.Comparator.comparingInt(WorkerResult::round))
				.toList();
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
				entries.add(entry);
			}
		}
		if (!replaced) {
			throw new IllegalArgumentException("Current classpath does not contain candidate classes: " + candidate);
		}
		return String.join(java.io.File.pathSeparator, entries);
	}

	private static String configText(Options options) {
		return """
				database: {
				  metrics: {
				    database-name: "grpc-raw-scan-benchmark"
				    influx: { enabled: false }
				    jmx: { enabled: false }
				  }
				  parallelism: {
				    read: %d
				    write: %d
				    workload: {
				      batch-queue-capacity: 4096
				      raw-scan-file-concurrency: %d
				      raw-scan-readahead-bytes: "%dB"
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
				""".formatted(options.readParallelism(),
				options.writeParallelism(),
				options.rawScanFileConcurrency(),
				options.rawScanReadaheadBytes());
	}

	private static String datasetId(Options options, String config) {
		String identity = DATASET_SCHEMA + '\n' + options.preloadKeys() + '\n' + options.flushKeys()
				+ '\n' + options.valueBytes() + '\n' + Long.toUnsignedString(VALUE_SEED) + '\n' + config;
		try {
			byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
					.digest(identity.getBytes(StandardCharsets.UTF_8));
			return java.util.HexFormat.of().formatHex(digest);
		} catch (java.security.NoSuchAlgorithmException impossible) {
			throw new IllegalStateException(impossible);
		}
	}

	private static void validateDatasetMetadata(Options options, String expectedDatasetId) throws IOException {
		if (!expectedDatasetId.matches("[0-9a-f]{64}")) {
			throw new IllegalArgumentException("Raw worker requires a valid dataset identity");
		}
		Path metadata = options.datasetRoot().toAbsolutePath().normalize().resolve("dataset.properties");
		Properties values = new Properties();
		try (InputStream stream = Files.newInputStream(metadata)) {
			values.load(stream);
		}
		Set<String> expectedKeys = Set.of("schema", "dataset-id", "preload-keys", "flush-keys",
				"value-bytes", "value-seed");
		if (!values.stringPropertyNames().equals(expectedKeys)) {
			throw new IllegalArgumentException("Raw dataset metadata schema mismatch: " + metadata);
		}
		if (!required(values, "schema").equals(DATASET_SCHEMA)
				|| !required(values, "dataset-id").equals(expectedDatasetId)
				|| integer(values, "preload-keys") != options.preloadKeys()
				|| integer(values, "flush-keys") != options.flushKeys()
				|| integer(values, "value-bytes") != options.valueBytes()
				|| !required(values, "value-seed").equals(Long.toUnsignedString(VALUE_SEED))) {
			throw new IllegalArgumentException("Raw dataset provenance mismatch: " + metadata);
		}
		String actualDatasetId = datasetId(options,
				Files.readString(options.datasetRoot().resolve("rockserver.conf")));
		if (!actualDatasetId.equals(expectedDatasetId)) {
			throw new IllegalArgumentException("Raw dataset configuration fingerprint mismatch");
		}
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static byte[] valueBytes(int size) {
		byte[] value = new byte[size];
		long state = VALUE_SEED;
		for (int index = 0; index < value.length; index++) {
			state ^= state << 13;
			state ^= state >>> 7;
			state ^= state << 17;
			value[index] = (byte) state;
		}
		return value;
	}

	private static long awaitNativeLeakDetection(long before) throws InterruptedException {
		for (int attempt = 0; attempt < 3; attempt++) {
			System.gc();
			Thread.sleep(100L);
		}
		return Math.max(0L, RocksLeakDetector.detectedLeakCount() - before);
	}

	private static Throwable appendFailure(@Nullable Throwable existing, Throwable added) {
		if (existing == null) {
			return added;
		}
		existing.addSuppressed(added);
		return existing;
	}

	private static Exception rethrow(Throwable failure) {
		return failure instanceof Exception exception ? exception : new RuntimeException(failure);
	}

	private static void writeControllerMetadata(Path root,
			Options options,
			FileStore store,
			Instant started,
			String datasetId,
			String baselineClassPathHash,
			String candidateClassPathHash) throws IOException {
		Files.writeString(root.resolve("metadata.properties"),
				"schema=" + RESULT_SCHEMA + "\n"
						+ "comparison-mode=v1.3.11-pareto\n"
						+ "started=" + started + "\n"
						+ "build-baseline=" + options.buildBaseline() + "\n"
						+ "build-candidate=" + options.buildCandidate() + "\n"
						+ "build-state-baseline=" + options.buildStateBaseline() + "\n"
						+ "build-state-candidate=" + options.buildStateCandidate() + "\n"
						+ "dataset-id=" + datasetId + "\n"
						+ "baseline-classpath-sha256=" + baselineClassPathHash + "\n"
						+ "candidate-classpath-sha256=" + candidateClassPathHash + "\n"
						+ "storage-label=" + options.storageLabel() + "\n"
						+ "storage-name=" + store.name() + "\n"
						+ "storage-type=" + store.type() + "\n"
						+ "host-state=" + options.hostState() + "\n"
						+ "baseline-classes=" + options.baselineClasses().toAbsolutePath().normalize() + "\n"
						+ "candidate-classes=" + options.candidateClasses().toAbsolutePath().normalize() + "\n"
						+ "preload-keys=" + options.preloadKeys() + "\n"
						+ "flush-keys=" + options.flushKeys() + "\n"
						+ "value-bytes=" + options.valueBytes() + "\n"
						+ "batch-entries=" + options.batchEntries() + "\n"
						+ "scan-clients=" + options.scanClients() + "\n"
						+ "read-parallelism=" + options.readParallelism() + "\n"
						+ "write-parallelism=" + options.writeParallelism() + "\n"
						+ "raw-scan-file-concurrency=" + options.rawScanFileConcurrency() + "\n"
						+ "raw-scan-readahead-bytes=" + options.rawScanReadaheadBytes() + "\n"
						+ "warmup-passes=" + options.warmupPasses() + "\n"
						+ "measure-seconds=" + options.measureSeconds() + "\n"
						+ "rounds=" + options.rounds() + "\n"
						+ "sample-micros=" + options.sampleMicros() + "\n"
						+ "java=" + System.getProperty("java.runtime.version") + "\n"
						+ "os=" + System.getProperty("os.name") + ' ' + System.getProperty("os.version")
						+ ' ' + System.getProperty("os.arch") + "\n"
						+ "available-processors=" + Runtime.getRuntime().availableProcessors() + "\n"
						+ "jvm-arguments=" + Management.runtimeArguments() + "\n",
				StandardOpenOption.CREATE_NEW);
	}

	private static void writeReports(Path root,
			Options options,
			FileStore store,
			Instant started,
			Instant finished,
			List<WorkerResult> results,
			Comparison comparison) throws IOException {
		Files.writeString(root.resolve("results.md"),
				toMarkdown(options, store, started, finished, results, comparison), StandardOpenOption.CREATE_NEW);
		Files.writeString(root.resolve("results.json"),
				toJson(options, store, started, finished, results, comparison), StandardOpenOption.CREATE_NEW);
	}

	private static String toMarkdown(Options options,
			FileStore store,
			Instant started,
			Instant finished,
			List<WorkerResult> results,
			Comparison comparison) {
		WorkerResult baselineProvenance = firstResult(results, Implementation.BASELINE);
		WorkerResult candidateProvenance = firstResult(results, Implementation.CANDIDATE);
		StringBuilder out = new StringBuilder("# Paired whole-path raw-scan comparison\n\n");
		out.append("- Schema: `").append(RESULT_SCHEMA).append("`\n")
				.append("- Comparison mode: `v1.3.11-pareto`\n")
				.append("- Started / finished: `").append(started).append("` / `").append(finished).append("`\n")
				.append("- Baseline / candidate: `").append(options.buildBaseline()).append("` / `")
				.append(options.buildCandidate()).append("`\n")
				.append("- Dataset: `").append(baselineProvenance.datasetId()).append("`\n")
				.append("- Baseline / candidate classpath SHA-256: `")
				.append(baselineProvenance.classPathSha256()).append("` / `")
				.append(candidateProvenance.classPathSha256()).append("`\n")
				.append("- Storage: `").append(options.storageLabel()).append("`, `")
				.append(store.name()).append("`, `").append(store.type()).append("`\n")
				.append("- Candidate raw-scan file concurrency / readahead: `")
				.append(options.rawScanFileConcurrency()).append("` / `")
				.append(new DataSize(options.rawScanReadaheadBytes())).append("`\n")
				.append("- Host/build state: `").append(options.hostState()).append("`, `")
				.append(options.buildStateBaseline()).append("` / `")
				.append(options.buildStateCandidate()).append("`\n")
				.append("- Acceptance: **").append(comparison.passed() ? "PASS" : "FAIL").append("**\n\n")
				.append("| Round | Build | MiB/s | entries/s | scan p99 ms | scheduler queue p99 ms | max READ active | requests terminal/submitted | worker gate |\n")
				.append("|---:|---|---:|---:|---:|---:|---:|---:|---|\n");
		for (WorkerResult result : results) {
			out.append('|').append(result.round()).append('|').append(result.implementation().value)
					.append('|').append(format(result.bytesPerSecond() / (1024 * 1024)))
					.append('|').append(format(result.entriesPerSecond()))
					.append('|').append(format(result.scanP99Nanos() / 1_000_000.0))
					.append('|').append(format(result.queueP99Nanos() / 1_000_000.0))
					.append('|').append(result.maximumActive())
					.append('|').append(result.terminalRequests()).append('/').append(result.submittedRequests())
					.append('|').append(result.passed() ? "PASS" : "FAIL").append("|\n");
		}
		out.append("\n| Pareto metric | Geometric ratio | 95% confidence interval | Automatic gate | Material |\n")
				.append("|---|---:|---:|---|---|\n");
		for (PairedPerformanceContract.MetricSpec specification : rawMetricSpecifications()) {
			var value = comparison.metrics().get(specification.name());
			boolean passed = comparison.failures().stream()
					.noneMatch(failure -> failure.startsWith(specification.name()));
			boolean material = comparison.materialImprovements().contains(specification.name());
			out.append('|').append(specification.name()).append('|')
					.append(value != null && value.available() ? format(value.mean()) : "n/a").append('|')
					.append(value != null && value.available()
							? "[" + format(value.lower95()) + ", " + format(value.upper95()) + "]"
							: "exact no-increase")
					.append('|').append(passed ? "PASS" : "FAIL").append('|')
					.append(material ? "YES" : "NO").append("|\n");
		}
		out.append("\n- Material improvements: `")
				.append(String.join(", ", comparison.materialImprovements())).append("`\n")
				.append("- Exception candidates (still FAIL; ablation, profiles, and explicit approval required): `")
				.append(String.join(", ", comparison.exceptionCandidates())).append("`\n");
		if (!comparison.failures().isEmpty()) {
			out.append("\nFailures:\n\n");
			for (String failure : comparison.failures()) {
				out.append("- ").append(failure).append('\n');
			}
		}
		return out.toString();
	}

	private static String toJson(Options options,
			FileStore store,
			Instant started,
			Instant finished,
			List<WorkerResult> results,
			Comparison comparison) {
		WorkerResult baselineProvenance = firstResult(results, Implementation.BASELINE);
		WorkerResult candidateProvenance = firstResult(results, Implementation.CANDIDATE);
		StringBuilder out = new StringBuilder("{\n  \"schema\": \"").append(RESULT_SCHEMA)
				.append("\",\n  \"comparison_mode\": \"v1.3.11-pareto")
				.append("\",\n  \"started\": \"").append(started)
				.append("\",\n  \"finished\": \"").append(finished)
				.append("\",\n  \"build_baseline\": \"").append(json(options.buildBaseline()))
				.append("\",\n  \"build_candidate\": \"").append(json(options.buildCandidate()))
				.append("\",\n  \"dataset_id\": \"").append(baselineProvenance.datasetId())
				.append("\",\n  \"classpath_baseline_sha256\": \"")
				.append(baselineProvenance.classPathSha256())
				.append("\",\n  \"classpath_candidate_sha256\": \"")
				.append(candidateProvenance.classPathSha256())
				.append("\",\n  \"host_state\": \"").append(json(options.hostState()))
				.append("\",\n  \"storage_label\": \"").append(json(options.storageLabel()))
				.append("\",\n  \"storage_name\": \"").append(json(store.name()))
				.append("\",\n  \"storage_type\": \"").append(json(store.type()))
				.append("\",\n  \"passed\": ").append(comparison.passed())
				.append(",\n  \"throughput_ratio\": ").append(intervalJson(comparison.throughput()))
				.append(",\n  \"scheduler_queue_p99_ratio\": ").append(intervalJson(comparison.queueP99()))
				.append(",\n  \"scan_p99_ratio\": ").append(intervalJson(comparison.scanP99()))
				.append(",\n  \"pareto_metrics\": {");
		int metricIndex = 0;
		for (Map.Entry<String, GrpcOverloadBenchmark.RatioConfidenceInterval> entry
				: comparison.metrics().entrySet()) {
			if (metricIndex++ > 0) out.append(',');
			out.append("\n    \"").append(json(entry.getKey())).append("\": ")
					.append(intervalJson(entry.getValue()));
		}
		out.append("\n  },\n  \"material_improvements\": [");
		for (int index = 0; index < comparison.materialImprovements().size(); index++) {
			if (index > 0) out.append(',');
			out.append('"').append(json(comparison.materialImprovements().get(index))).append('"');
		}
		out.append("],\n  \"exception_candidates\": [");
		for (int index = 0; index < comparison.exceptionCandidates().size(); index++) {
			if (index > 0) out.append(',');
			out.append('"').append(json(comparison.exceptionCandidates().get(index))).append('"');
		}
		out.append(']')
				.append(",\n  \"rounds\": [\n");
		for (int index = 0; index < results.size(); index++) {
			WorkerResult result = results.get(index);
			out.append("    {\"round\": ").append(result.round())
					.append(", \"implementation\": \"").append(result.implementation().value)
					.append("\", \"build_sha\": \"").append(result.buildSha())
					.append("\", \"classpath_sha256\": \"").append(result.classPathSha256())
					.append("\", \"dataset_id\": \"").append(result.datasetId())
					.append("\", \"bytes_per_second\": ").append(format(result.bytesPerSecond()))
					.append(", \"entries_per_second\": ").append(format(result.entriesPerSecond()))
					.append(", \"scan_p99_nanos\": ").append(result.scanP99Nanos())
					.append(", \"scheduler_queue_p99_nanos\": ").append(result.queueP99Nanos())
					.append(", \"scheduler_execution_p99_nanos\": ").append(result.executionP99Nanos())
					.append(", \"cpu_nanos_per_entry\": ").append(format(result.cpuNanosPerEntry()))
					.append(", \"allocated_bytes_per_entry\": ")
					.append(format(result.allocatedBytesPerEntry()))
					.append(", \"gc_collections\": ").append(result.gcCollections())
					.append(", \"gc_millis\": ").append(result.gcMillis())
					.append(", \"peak_live_heap_bytes\": ").append(result.peakLiveHeapBytes())
					.append(", \"peak_direct_memory_bytes\": ").append(result.peakDirectMemoryBytes())
					.append(", \"peak_resident_set_bytes\": ").append(result.peakResidentSetBytes())
					.append(", \"peak_thread_count\": ").append(result.peakThreadCount())
					.append(", \"peak_native_handles\": ").append(result.peakNativeHandles())
					.append(", \"maximum_read_active\": ").append(result.maximumActive())
					.append(", \"submitted_requests\": ").append(result.submittedRequests())
					.append(", \"terminal_requests\": ").append(result.terminalRequests())
					.append(", \"duplicate_terminals\": ").append(result.duplicateTerminals())
					.append(", \"in_flight_requests\": ").append(result.inFlightRequests())
					.append(", \"non_ok_requests\": ").append(result.nonOkRequests())
					.append(", \"scans\": ").append(result.scans())
					.append(", \"entries\": ").append(result.entries())
					.append(", \"batches\": ").append(result.batches())
					.append(", \"full_batches\": ").append(result.fullBatches())
					.append(", \"maximum_batch_bytes\": ").append(result.maximumBatchBytes())
					.append(", \"scheduler_accepted\": ").append(result.schedulerAccepted())
					.append(", \"scheduler_submission_attempts\": ")
					.append(result.schedulerSubmissionAttempts())
					.append(", \"scheduler_accounting_exact\": ")
					.append(result.schedulerAccountingExact())
					.append(", \"scheduler_started\": ").append(result.schedulerStarted())
					.append(", \"scheduler_completed\": ").append(result.schedulerCompleted())
					.append(", \"scheduler_outcomes\": ").append(result.schedulerOutcomes())
					.append(", \"scheduler_failures\": ").append(result.schedulerFailures())
					.append(", \"sampler_samples\": ").append(result.samplerSamples())
					.append(", \"saturating_demand_samples\": ").append(result.saturatingDemandSamples())
					.append(", \"maximum_avoidable_idle_nanos\": ").append(result.maximumAvoidableIdleNanos())
					.append(", \"maximum_parked\": ").append(result.maximumParked())
					.append(", \"maximum_outstanding\": ").append(result.maximumOutstanding())
					.append(", \"exact_waiting_workers\": ").append(result.exactWaitingWorkers())
					.append(", \"resources_drained\": ").append(result.resourcesDrained())
					.append(", \"native_leaks\": ").append(result.nativeLeaks())
					.append(", \"passed\": ").append(result.passed()).append('}');
			out.append(index + 1 == results.size() ? '\n' : ",\n");
		}
		out.append("  ],\n  \"failures\": [");
		for (int index = 0; index < comparison.failures().size(); index++) {
			if (index > 0) out.append(',');
			out.append("\"").append(json(comparison.failures().get(index))).append("\"");
		}
		return out.append("]\n}\n").toString();
	}

	private static WorkerResult firstResult(List<WorkerResult> results, Implementation implementation) {
		return results.stream().filter(result -> result.implementation() == implementation)
				.findFirst().orElseThrow(() -> new IllegalStateException(
						"Missing raw-scan provenance for " + implementation.value));
	}

	private static String interval(GrpcOverloadBenchmark.RatioConfidenceInterval interval) {
		return "mean=" + format(interval.mean()) + ", 95% CI=[" + format(interval.lower95())
				+ ", " + format(interval.upper95()) + ']';
	}

	private static String intervalJson(GrpcOverloadBenchmark.RatioConfidenceInterval interval) {
		return "{\"samples\": " + interval.samples() + ", \"mean\": "
				+ (interval.available() ? format(interval.mean()) : "null")
				+ ", \"lower_95\": " + (interval.available() ? format(interval.lower95()) : "null")
				+ ", \"upper_95\": " + (interval.available() ? format(interval.upper95()) : "null") + '}';
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
				Paired whole-gRPC raw-SST comparison. The baseline uses the legacy scan and the
				candidate uses resumable scanning with an empty acknowledgement set. Compile tests
				and build the untouched baseline first:

				  java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \\
				    -cp target/test-classes:target/classes:<test-dependencies> \\
				    it.cavallium.rockserver.core.impl.benchmark.GrpcRawScanBenchmark \\
				    --root=/mnt/rockserver-hdd/raw-scan-RC-SHA \\
				    --baseline-classes=/tmp/rockserver-baseline/target/classes \\
				    --candidate-classes=target/classes \\
				    --build-baseline=<full-baseline-sha> --build-candidate=<full-candidate-sha> \\
				    --build-state-baseline=clean --build-state-candidate=clean \\
					--storage-label=hdd-btrfs --host-state=dedicated --enforce=true

				Full defaults: 1,000,000 keys, eight explicit SST flushes, five scan clients,
				20 READ workers, four SST readers/scan, 8 MiB readahead/reader, one complete
				warmup scan/client, 15 measured seconds, alternating
				implementation order, and strict candidate idle instrumentation. Pareto mode fixes ten
				paired rounds, requires every point estimate to be no worse than equality, rejects a
				confidence interval that demonstrates regression, and requires one material primary gain.
				The 0.99/1.02 ceilings are report-only exception candidates, never automatic passes.
				Tune candidates with --raw-scan-file-concurrency=4|6|8 and
				--raw-scan-readahead-bytes=8MiB|32MiB|64MiB; each one-shot root compares the
				candidate setting against the unchanged baseline defaults.
				Use --smoke=true --enforce=false for structural validation only.
				""");
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

	private record ScanResult(long entries,
			long serializedBytes,
			long batches,
			long fullBatches,
			int maximumBatchBytes) {
	}

	private record SchedulerMetrics(long queueP99Nanos, long executionP99Nanos) {
	}

	private static final class ScanLatencyRecorder {

		private final long[] samples;
		private final AtomicLong sequence = new AtomicLong();

		private ScanLatencyRecorder(int capacity) {
			samples = new long[capacity];
		}

		private void record(long nanos) {
			long ordinal = sequence.getAndIncrement();
			samples[(int) Math.floorMod(ordinal, samples.length)] = Math.max(1L, nanos);
		}

		private long[] sorted() {
			int count = Math.toIntExact(Math.min(sequence.get(), samples.length));
			long[] result = Arrays.copyOf(samples, count);
			Arrays.sort(result);
			return result;
		}
	}

	private record RequestAccounting(long submitted,
			long terminal,
			long duplicateTerminal,
			long inFlight,
			long ok,
			long nonOk) {

		private boolean conserved() {
			return submitted > 0L && submitted == terminal && duplicateTerminal == 0L
					&& inFlight == 0L && ok == terminal && nonOk == 0L;
		}
	}

	private record PoolUtilization(long samples,
			long saturatingDemandSamples,
			int maximumActive,
			int workerCount,
			int maximumConsecutiveAvoidableIdleSamples,
			long sampleNanos,
			boolean exactWaitingWorkers,
			int maximumParked,
			int maximumOutstanding,
			BenchmarkProcessTelemetry.Peaks processPeaks) {

		private long maximumAvoidableIdleNanos() {
			return maximumConsecutiveAvoidableIdleSamples * sampleNanos;
		}

		private boolean passed() {
			return samples > 0L && saturatingDemandSamples > 0L && maximumActive == workerCount
					&& (!exactWaitingWorkers
					|| maximumAvoidableIdleNanos() <= COOPERATIVE_QUANTUM_NANOS + sampleNanos);
		}
	}

	private static final class PoolSampler {

		private final Options options;
		private long samples;
		private long saturatingDemandSamples;
		private int maximumActive;
		private int workerCount;
		private int consecutiveAvoidableIdleSamples;
		private int maximumConsecutiveAvoidableIdleSamples;
		private int maximumParked;
		private int maximumOutstanding;
		private final long[] telemetry = new long[RWScheduler.POOL_TELEMETRY_LENGTH];
		private final BenchmarkProcessTelemetry.PeakSampler processPeaks =
				new BenchmarkProcessTelemetry.PeakSampler();

		private PoolSampler(Options options) {
			this.options = options;
		}

		private void resetMeasurementPeaks() {
			processPeaks.reset();
		}

		private void sample(EmbeddedConnection embedded) {
			BenchmarkSchedulerTelemetry.copyPoolTelemetry(
					embedded.getScheduler(), RWScheduler.Pool.READ, telemetry);
			samples++;
			workerCount = value(RWScheduler.POOL_TELEMETRY_WORKER_COUNT);
			int active = value(RWScheduler.POOL_TELEMETRY_ACTIVE_TASKS);
			int queued = value(RWScheduler.POOL_TELEMETRY_QUEUED_TASKS);
			int outstanding = value(RWScheduler.POOL_TELEMETRY_OUTSTANDING_TASKS);
			maximumActive = Math.max(maximumActive, active);
			maximumParked = Math.max(maximumParked, value(RWScheduler.POOL_TELEMETRY_PARKED_TASKS));
			maximumOutstanding = Math.max(maximumOutstanding, outstanding);
			processPeaks.sample();
			boolean saturating = active >= workerCount
					|| (queued > 0 && active + queued >= workerCount);
			if (saturating) {
				saturatingDemandSamples++;
			}
			if (!options.instrumentationMode().equals("strict") || queued == 0) {
				consecutiveAvoidableIdleSamples = 0;
				return;
			}
			if (value(RWScheduler.POOL_TELEMETRY_WAITING_WORKERS) > 0) {
				consecutiveAvoidableIdleSamples++;
				maximumConsecutiveAvoidableIdleSamples = Math.max(
						maximumConsecutiveAvoidableIdleSamples, consecutiveAvoidableIdleSamples);
			} else {
				consecutiveAvoidableIdleSamples = 0;
			}
		}

		private int value(int index) {
			return Math.toIntExact(telemetry[index]);
		}

		private PoolUtilization snapshot() {
			return new PoolUtilization(samples, saturatingDemandSamples, maximumActive, workerCount,
					maximumConsecutiveAvoidableIdleSamples, options.sampleNanos(),
					options.instrumentationMode().equals("strict"), maximumParked, maximumOutstanding,
					processPeaks.peaks());
		}

		private void close() {
			processPeaks.close();
		}
	}

	private record PoolCounters(long submissionAttempts,
			long accepted,
			long started,
			long completed,
			long failed,
			long outcomes,
			boolean exactAccounting) {

		private static PoolCounters capture(EmbeddedConnection embedded) {
			var snapshot = embedded.getScheduler().poolSnapshot(RWScheduler.Pool.READ);
			return new PoolCounters(BenchmarkSchedulerTelemetry.submissionAttempts(snapshot),
					snapshot.acceptedTasks(), snapshot.startedTasks(), snapshot.completedTasks(),
					snapshot.failedTasks(), BenchmarkSchedulerTelemetry.terminalOutcomes(snapshot),
					BenchmarkSchedulerTelemetry.exactAccounting());
		}

		private PoolCounters minus(PoolCounters before) {
			return new PoolCounters(submissionAttempts - before.submissionAttempts,
					accepted - before.accepted, started - before.started,
					completed - before.completed, failed - before.failed, outcomes - before.outcomes,
					exactAccounting && before.exactAccounting);
		}

		private boolean conserved() {
			long expectedOutcomes = exactAccounting ? submissionAttempts : accepted;
			return expectedOutcomes > 0L && started == completed && failed == 0L
					&& outcomes == expectedOutcomes;
		}
	}

	private record WorkerMeasurement(long elapsedNanos,
			long scans,
			long entries,
			long serializedBytes,
			long batches,
			long fullBatches,
			int maximumBatchBytes,
			long scanP99Nanos,
			double cpuNanosPerEntry,
			double allocatedBytesPerEntry,
			long gcCollections,
			long gcMillis,
			SchedulerMetrics schedulerMetrics,
			RequestAccounting requests,
			PoolUtilization utilization,
			PoolCounters counters,
			boolean resourcesDrained) {
	}

	private record WorkerResult(Implementation implementation,
			int round,
			String buildSha,
			String classPathSha256,
			String datasetId,
			double bytesPerSecond,
			double entriesPerSecond,
			long scans,
			long entries,
			long batches,
			long fullBatches,
			int maximumBatchBytes,
			long scanP99Nanos,
			long queueP99Nanos,
			long executionP99Nanos,
			double cpuNanosPerEntry,
			double allocatedBytesPerEntry,
			long gcCollections,
			long gcMillis,
			long peakLiveHeapBytes,
			long peakDirectMemoryBytes,
			long peakResidentSetBytes,
			int peakThreadCount,
			long peakNativeHandles,
			long submittedRequests,
			long terminalRequests,
			long duplicateTerminals,
			long inFlightRequests,
			long nonOkRequests,
			long schedulerAccepted,
			long schedulerSubmissionAttempts,
			boolean schedulerAccountingExact,
			long schedulerStarted,
			long schedulerCompleted,
			long schedulerOutcomes,
			long schedulerFailures,
			long samplerSamples,
			long saturatingDemandSamples,
			int maximumActive,
			int workerCount,
			int maximumParked,
			int maximumOutstanding,
			long maximumAvoidableIdleNanos,
			boolean exactWaitingWorkers,
			boolean resourcesDrained,
			long nativeLeaks,
			boolean passed) {

		private static WorkerResult from(Options options,
				String buildSha,
				String classPathSha256,
				String datasetId,
				WorkerMeasurement measurement,
				long nativeLeaks) {
			double seconds = measurement.elapsedNanos() / 1_000_000_000.0d;
			boolean hasFullBatch = options.smoke() || measurement.fullBatches() > 0L;
			BenchmarkProcessTelemetry.Peaks processPeaks = measurement.utilization().processPeaks();
			boolean passed = measurement.scans() > 0L
					&& measurement.entries() == measurement.scans() * options.preloadKeys()
					&& measurement.batches() > 0L
					&& hasFullBatch
					&& measurement.maximumBatchBytes() <= RAW_MAX_SERIALIZED_BYTES
					&& measurement.scanP99Nanos() > 0L
					&& measurement.schedulerMetrics().queueP99Nanos() > 0L
					&& measurement.schedulerMetrics().executionP99Nanos() > 0L
					&& Double.isFinite(measurement.cpuNanosPerEntry())
					&& measurement.cpuNanosPerEntry() > 0.0d
					&& Double.isFinite(measurement.allocatedBytesPerEntry())
					&& measurement.allocatedBytesPerEntry() > 0.0d
					&& measurement.gcCollections() >= 0L && measurement.gcMillis() >= 0L
					&& processPeaks.complete()
					&& measurement.requests().conserved()
					&& measurement.utilization().passed()
					&& measurement.counters().conserved()
					&& (options.implementation() == Implementation.BASELINE
							|| (measurement.counters().exactAccounting()
							&& BenchmarkSchedulerTelemetry.allocationFreePoolTelemetry()))
					&& measurement.resourcesDrained()
					&& nativeLeaks == 0L;
			return new WorkerResult(options.implementation(), options.round(), buildSha,
					classPathSha256, datasetId,
					measurement.serializedBytes() / seconds, measurement.entries() / seconds,
					measurement.scans(), measurement.entries(), measurement.batches(), measurement.fullBatches(),
					measurement.maximumBatchBytes(), measurement.scanP99Nanos(),
					measurement.schedulerMetrics().queueP99Nanos(),
					measurement.schedulerMetrics().executionP99Nanos(),
					measurement.cpuNanosPerEntry(), measurement.allocatedBytesPerEntry(),
					measurement.gcCollections(), measurement.gcMillis(), processPeaks.liveHeapBytes(),
					processPeaks.directMemoryBytes(), processPeaks.residentSetBytes(),
					processPeaks.threadCount(), processPeaks.nativeHandles(),
					measurement.requests().submitted(), measurement.requests().terminal(),
					measurement.requests().duplicateTerminal(), measurement.requests().inFlight(),
					measurement.requests().nonOk(), measurement.counters().accepted(),
					measurement.counters().submissionAttempts(), measurement.counters().exactAccounting(),
					measurement.counters().started(), measurement.counters().completed(),
					measurement.counters().outcomes(), measurement.counters().failed(),
					measurement.utilization().samples(), measurement.utilization().saturatingDemandSamples(),
					measurement.utilization().maximumActive(), measurement.utilization().workerCount(),
					measurement.utilization().maximumParked(), measurement.utilization().maximumOutstanding(),
					measurement.utilization().maximumAvoidableIdleNanos(),
					measurement.utilization().exactWaitingWorkers(), measurement.resourcesDrained(), nativeLeaks, passed);
		}

		private void write(Path output) throws IOException {
			Properties properties = new Properties();
			properties.setProperty("schema", WORKER_SCHEMA);
			properties.setProperty("implementation", implementation.value);
			properties.setProperty("round", Integer.toString(round));
			properties.setProperty("build-sha", buildSha);
			properties.setProperty("classpath-sha256", classPathSha256);
			properties.setProperty("dataset-id", datasetId);
			properties.setProperty("bytes-per-second", Double.toString(bytesPerSecond));
			properties.setProperty("entries-per-second", Double.toString(entriesPerSecond));
			properties.setProperty("scans", Long.toString(scans));
			properties.setProperty("entries", Long.toString(entries));
			properties.setProperty("batches", Long.toString(batches));
			properties.setProperty("full-batches", Long.toString(fullBatches));
			properties.setProperty("maximum-batch-bytes", Integer.toString(maximumBatchBytes));
			properties.setProperty("scan-p99-nanos", Long.toString(scanP99Nanos));
			properties.setProperty("queue-p99-nanos", Long.toString(queueP99Nanos));
			properties.setProperty("execution-p99-nanos", Long.toString(executionP99Nanos));
			properties.setProperty("cpu-nanos-per-entry", Double.toString(cpuNanosPerEntry));
			properties.setProperty("allocated-bytes-per-entry", Double.toString(allocatedBytesPerEntry));
			properties.setProperty("gc-collections", Long.toString(gcCollections));
			properties.setProperty("gc-millis", Long.toString(gcMillis));
			properties.setProperty("peak-live-heap-bytes", Long.toString(peakLiveHeapBytes));
			properties.setProperty("peak-direct-memory-bytes", Long.toString(peakDirectMemoryBytes));
			properties.setProperty("peak-resident-set-bytes", Long.toString(peakResidentSetBytes));
			properties.setProperty("peak-thread-count", Integer.toString(peakThreadCount));
			properties.setProperty("peak-native-handles", Long.toString(peakNativeHandles));
			properties.setProperty("submitted-requests", Long.toString(submittedRequests));
			properties.setProperty("terminal-requests", Long.toString(terminalRequests));
			properties.setProperty("duplicate-terminals", Long.toString(duplicateTerminals));
			properties.setProperty("in-flight-requests", Long.toString(inFlightRequests));
			properties.setProperty("non-ok-requests", Long.toString(nonOkRequests));
			properties.setProperty("scheduler-accepted", Long.toString(schedulerAccepted));
			properties.setProperty("scheduler-submission-attempts", Long.toString(schedulerSubmissionAttempts));
			properties.setProperty("scheduler-accounting-exact", Boolean.toString(schedulerAccountingExact));
			properties.setProperty("scheduler-started", Long.toString(schedulerStarted));
			properties.setProperty("scheduler-completed", Long.toString(schedulerCompleted));
			properties.setProperty("scheduler-outcomes", Long.toString(schedulerOutcomes));
			properties.setProperty("scheduler-failures", Long.toString(schedulerFailures));
			properties.setProperty("sampler-samples", Long.toString(samplerSamples));
			properties.setProperty("saturating-demand-samples", Long.toString(saturatingDemandSamples));
			properties.setProperty("maximum-active", Integer.toString(maximumActive));
			properties.setProperty("worker-count", Integer.toString(workerCount));
			properties.setProperty("maximum-parked", Integer.toString(maximumParked));
			properties.setProperty("maximum-outstanding", Integer.toString(maximumOutstanding));
			properties.setProperty("maximum-avoidable-idle-nanos", Long.toString(maximumAvoidableIdleNanos));
			properties.setProperty("exact-waiting-workers", Boolean.toString(exactWaitingWorkers));
			properties.setProperty("resources-drained", Boolean.toString(resourcesDrained));
			properties.setProperty("native-leaks", Long.toString(nativeLeaks));
			properties.setProperty("passed", Boolean.toString(passed));
			try (var stream = Files.newOutputStream(output, StandardOpenOption.CREATE_NEW)) {
				properties.store(stream, null);
			}
		}

		private static WorkerResult read(Path input,
				Implementation expectedImplementation,
				int expectedRound,
				String expectedBuildSha,
				String expectedClassPathSha256,
				String expectedDatasetId) throws IOException {
			Properties values = new Properties();
			try (InputStream stream = Files.newInputStream(input)) {
				values.load(stream);
			}
			if (!WORKER_SCHEMA.equals(required(values, "schema"))) {
				throw new IllegalArgumentException("Unknown raw-scan worker schema: " + input);
			}
			if (!values.stringPropertyNames().equals(WORKER_KEYS)) {
				throw new IllegalArgumentException("Raw-scan worker property set does not match "
						+ WORKER_SCHEMA + ": " + input);
			}
			WorkerResult result = new WorkerResult(Implementation.parse(required(values, "implementation")),
					integer(values, "round"), required(values, "build-sha"),
					required(values, "classpath-sha256"), required(values, "dataset-id"),
					decimal(values, "bytes-per-second"),
					decimal(values, "entries-per-second"), number(values, "scans"),
					number(values, "entries"), number(values, "batches"), number(values, "full-batches"),
					integer(values, "maximum-batch-bytes"), number(values, "scan-p99-nanos"),
					number(values, "queue-p99-nanos"), number(values, "execution-p99-nanos"),
					decimal(values, "cpu-nanos-per-entry"), decimal(values, "allocated-bytes-per-entry"),
					number(values, "gc-collections"), number(values, "gc-millis"),
					number(values, "peak-live-heap-bytes"), number(values, "peak-direct-memory-bytes"),
					number(values, "peak-resident-set-bytes"), integer(values, "peak-thread-count"),
					number(values, "peak-native-handles"),
					number(values, "submitted-requests"), number(values, "terminal-requests"),
					number(values, "duplicate-terminals"), number(values, "in-flight-requests"),
					number(values, "non-ok-requests"), number(values, "scheduler-accepted"),
					number(values, "scheduler-submission-attempts"),
					bool(values, "scheduler-accounting-exact"),
					number(values, "scheduler-started"), number(values, "scheduler-completed"),
					number(values, "scheduler-outcomes"), number(values, "scheduler-failures"),
					number(values, "sampler-samples"), number(values, "saturating-demand-samples"),
					integer(values, "maximum-active"), integer(values, "worker-count"),
					integer(values, "maximum-parked"), integer(values, "maximum-outstanding"),
					number(values, "maximum-avoidable-idle-nanos"), bool(values, "exact-waiting-workers"),
					bool(values, "resources-drained"), number(values, "native-leaks"), bool(values, "passed"));
			if (result.implementation() != expectedImplementation || result.round() != expectedRound
					|| !result.buildSha().equals(expectedBuildSha)
					|| !result.classPathSha256().equals(expectedClassPathSha256)
					|| !result.datasetId().equals(expectedDatasetId)) {
				throw new IllegalArgumentException("Raw-scan worker provenance mismatch: " + input);
			}
			return result;
		}
	}

	public record Comparison(Map<String, GrpcOverloadBenchmark.RatioConfidenceInterval> metrics,
			List<String> materialImprovements,
			List<String> exceptionCandidates,
			List<String> failures) {

		public Comparison {
			metrics = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(metrics));
			materialImprovements = List.copyOf(materialImprovements);
			exceptionCandidates = List.copyOf(exceptionCandidates);
			failures = List.copyOf(failures);
		}

		private static Comparison failed(String failure) {
			return new Comparison(Map.of(), List.of(), List.of(), List.of(failure));
		}

		public GrpcOverloadBenchmark.RatioConfidenceInterval throughput() {
			return metric("mib-per-second");
		}

		public GrpcOverloadBenchmark.RatioConfidenceInterval queueP99() {
			return metric("queue-p99");
		}

		public GrpcOverloadBenchmark.RatioConfidenceInterval scanP99() {
			return metric("scan-p99");
		}

		private GrpcOverloadBenchmark.RatioConfidenceInterval metric(String name) {
			return metrics.getOrDefault(name, new GrpcOverloadBenchmark.RatioConfidenceInterval(
					0, Double.NaN, Double.NaN, Double.NaN));
		}

		public boolean passed() {
			return failures.isEmpty();
		}

		private String failedSummary() {
			return failures.isEmpty() ? "none" : String.join("; ", failures);
		}
	}

	private static final class RequestTracker implements ClientInterceptor {

		private final AtomicBoolean measuring = new AtomicBoolean();
		private final LongAdder submitted = new LongAdder();
		private final LongAdder terminal = new LongAdder();
		private final LongAdder duplicateTerminal = new LongAdder();
		private final AtomicLong inFlight = new AtomicLong();
		private final EnumMap<Status.Code, LongAdder> statuses = new EnumMap<>(Status.Code.class);

		private RequestTracker() {
			for (Status.Code code : Status.Code.values()) {
				statuses.put(code, new LongAdder());
			}
		}

		private void startTracking() {
			if (measuring.get() || inFlight.get() != 0L) {
				throw new IllegalStateException("Request tracker is not drained before measurement");
			}
			submitted.reset();
			terminal.reset();
			duplicateTerminal.reset();
			for (LongAdder status : statuses.values()) status.reset();
			measuring.set(true);
		}

		private void stopTracking() {
			measuring.set(false);
		}

		private void stopTrackingIfActive() {
			measuring.set(false);
		}

		private RequestAccounting awaitSnapshot() throws InterruptedException {
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
			while (inFlight.get() != 0L && System.nanoTime() < deadline) Thread.sleep(1L);
			long ok = statuses.get(Status.Code.OK).sum();
			long nonOk = statuses.entrySet().stream()
					.filter(entry -> entry.getKey() != Status.Code.OK)
					.mapToLong(entry -> entry.getValue().sum()).sum();
			return new RequestAccounting(submitted.sum(), terminal.sum(), duplicateTerminal.sum(),
					inFlight.get(), ok, nonOk);
		}

		@Override
		public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
				CallOptions callOptions,
				Channel next) {
			ClientCall<ReqT, RespT> delegate = next.newCall(method, callOptions);
			return new ForwardingClientCall.SimpleForwardingClientCall<>(delegate) {
				private final AtomicBoolean closed = new AtomicBoolean();
				private boolean tracked;

				@Override
				public void start(Listener<RespT> responseListener, Metadata headers) {
					tracked = measuring.get();
					if (tracked) {
						submitted.increment();
						inFlight.incrementAndGet();
					}
					var listener = new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(
							responseListener) {
						@Override
						public void onClose(Status status, Metadata trailers) {
							finish(status.getCode());
							super.onClose(status, trailers);
						}
					};
					try {
						super.start(listener, headers);
					} catch (Throwable failure) {
						finish(Status.Code.UNKNOWN);
						throw failure;
					}
				}

				private void finish(Status.Code code) {
					if (!tracked) return;
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

	private record Options(boolean worker,
			Path root,
			Path datasetRoot,
			Path output,
			Path baselineClasses,
			Path candidateClasses,
			Implementation implementation,
			int round,
			String buildBaseline,
			String buildCandidate,
			String buildStateBaseline,
			String buildStateCandidate,
			String storageLabel,
			String hostState,
			int preloadKeys,
			int flushKeys,
			int valueBytes,
			int batchEntries,
			int scanClients,
			int readParallelism,
			int writeParallelism,
			int rawScanFileConcurrency,
			long rawScanReadaheadBytes,
			int warmupPasses,
			int measureSeconds,
			int rounds,
			int sampleMicros,
			String instrumentationMode,
			String childHeap,
			boolean enforce,
			boolean smoke) {

		private static final Set<String> KNOWN_OPTIONS = Set.of(
				"worker", "root", "dataset-root", "output", "baseline-classes", "candidate-classes",
				"implementation", "round", "build-baseline", "build-candidate", "build-state-baseline",
				"build-state-candidate", "storage-label", "host-state", "preload-keys", "flush-keys",
				"value-bytes", "batch-entries", "scan-clients", "read-parallelism", "write-parallelism",
				"raw-scan-file-concurrency", "raw-scan-readahead-bytes",
				"warmup-passes", "measure-seconds", "rounds", "sample-micros", "instrumentation-mode",
				"child-heap", "enforce", "smoke");

		private static Options parse(String[] args) {
			Map<String, String> values = new LinkedHashMap<>();
			for (String argument : args) {
				if (!argument.startsWith("--") || !argument.contains("=")) {
					throw new IllegalArgumentException("Options must use --name=value: " + argument);
				}
				int equals = argument.indexOf('=');
				String previous = values.put(argument.substring(2, equals), argument.substring(equals + 1));
				if (previous != null) throw new IllegalArgumentException("Duplicate option: " + argument);
			}
			for (String key : values.keySet()) {
				if (!KNOWN_OPTIONS.contains(key)) throw new IllegalArgumentException("Unknown option: --" + key);
			}
			boolean worker = bool(values, "worker", false);
			boolean smoke = bool(values, "smoke", false);
			Path root = Path.of(values.getOrDefault("root", Path.of(System.getProperty("java.io.tmpdir"),
					"rockserver-raw-scan-" + System.currentTimeMillis()).toString()));
			long rawScanReadaheadBytes = new DataSize(values.getOrDefault(
					"raw-scan-readahead-bytes",
					new DataSize(DEFAULT_RAW_SCAN_READAHEAD_BYTES).toString())).longValue();
			Options options = new Options(worker, root,
					Path.of(values.getOrDefault("dataset-root", root.resolve("shared-dataset").toString())),
					Path.of(values.getOrDefault("output", root.resolve("worker.properties").toString())),
					Path.of(values.getOrDefault("baseline-classes", "baseline-target/classes")),
					Path.of(values.getOrDefault("candidate-classes", "target/classes")),
					Implementation.parse(values.getOrDefault("implementation", "candidate")),
					integer(values, "round", 1), values.getOrDefault("build-baseline", "unverified"),
					values.getOrDefault("build-candidate", "unverified"),
					values.getOrDefault("build-state-baseline", "unknown"),
					values.getOrDefault("build-state-candidate", "unknown"),
					values.getOrDefault("storage-label", "ci-structural"),
					values.getOrDefault("host-state", "shared"),
					integer(values, "preload-keys", smoke ? 50_000 : 1_000_000),
					integer(values, "flush-keys", smoke ? 10_000 : 125_000),
					integer(values, "value-bytes", 256), integer(values, "batch-entries", smoke ? 1_000 : 5_000),
					integer(values, "scan-clients", smoke ? 2 : 5),
					integer(values, "read-parallelism", smoke ? 4 : 20),
					integer(values, "write-parallelism", smoke ? 4 : 8),
					integer(values, "raw-scan-file-concurrency",
							DEFAULT_RAW_SCAN_FILE_CONCURRENCY),
					rawScanReadaheadBytes,
					integer(values, "warmup-passes", 1), integer(values, "measure-seconds", smoke ? 2 : 15),
					integer(values, "rounds", smoke ? 1 : 10),
					integer(values, "sample-micros", 250),
					values.getOrDefault("instrumentation-mode", worker ? "strict" : "controller"),
					values.getOrDefault("child-heap", smoke ? "1g" : "4g"),
					bool(values, "enforce", !smoke), smoke);
			options.validate();
			return options;
		}

		private void validate() {
			if (preloadKeys < 1 || flushKeys < 1 || preloadKeys % flushKeys != 0
					|| valueBytes < 1 || batchEntries < 1 || flushKeys % batchEntries != 0) {
				throw new IllegalArgumentException("preload/flush/batch/value dimensions must be positive and exact multiples");
			}
			if (scanClients < 1 || readParallelism < 1 || writeParallelism < 1
					|| rawScanFileConcurrency < 1
					|| rawScanFileConcurrency > MAX_RAW_SCAN_FILE_CONCURRENCY
					|| rawScanReadaheadBytes < 1L
					|| warmupPasses < 1 || measureSeconds < 1 || rounds < 1 || sampleMicros < 1) {
				throw new IllegalArgumentException("worker, raw-scan, duration, round, and sampling dimensions are invalid");
			}
			if (!List.of("strict", "portable", "controller").contains(instrumentationMode)) {
				throw new IllegalArgumentException("instrumentation-mode must be strict, portable, or controller");
			}
			if (worker && instrumentationMode.equals("controller")) {
				throw new IllegalArgumentException("workers require strict or portable instrumentation");
			}
			if (!worker && !smoke && rounds != PairedPerformanceContract.REQUIRED_PAIRS) {
				throw new IllegalArgumentException("raw-SST comparison requires exactly ten paired rounds");
			}
			if (!worker) {
				if (!Files.isDirectory(baselineClasses) || !Files.isDirectory(candidateClasses)) {
					throw new IllegalArgumentException("baseline-classes and candidate-classes must exist");
				}
				if (scanClients * 4 < readParallelism) {
					throw new IllegalArgumentException("scan-clients * four-way SST parallelism must saturate READ workers");
				}
			}
			if (enforce) {
				if (smoke || worker || rounds != PairedPerformanceContract.REQUIRED_PAIRS
						|| preloadKeys < 1_000_000 || flushKeys > 125_000
						|| scanClients * 4 < readParallelism || readParallelism < 20 || measureSeconds < 15
						|| !buildBaseline.matches("[0-9a-f]{40}") || !buildCandidate.matches("[0-9a-f]{40}")
						|| !buildBaseline.equals(PERFORMANCE_BASELINE_SHA)
						|| !buildStateBaseline.equals("clean") || !buildStateCandidate.equals("clean")
						|| !hostState.equals("dedicated") || storageLabel.equals("ci-structural")) {
					throw new IllegalArgumentException("enforced raw-scan comparison requires clean full SHAs, "
							+ "dedicated hardware, the required paired rounds, and saturating 20-worker dimensions");
				}
			}
		}

		private long sampleNanos() {
			return TimeUnit.MICROSECONDS.toNanos(sampleMicros);
		}
	}

	private static final class Management {

		private static String runtimeArguments() {
			return java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
					.collect(Collectors.joining(" "));
		}
	}

	private static String required(Properties values, String key) {
		String value = values.getProperty(key);
		if (value == null) throw new IllegalArgumentException("Missing property: " + key);
		return value;
	}

	private static int integer(Properties values, String key) {
		return Integer.parseInt(required(values, key));
	}

	private static long number(Properties values, String key) {
		return Long.parseLong(required(values, key));
	}

	private static double decimal(Properties values, String key) {
		double value = Double.parseDouble(required(values, key));
		if (!Double.isFinite(value)) {
			throw new IllegalArgumentException("Non-finite decimal property: " + key);
		}
		return value;
	}

	private static boolean bool(Properties values, String key) {
		String value = required(values, key);
		if (!value.equals("true") && !value.equals("false")) {
			throw new IllegalArgumentException("Invalid boolean property: " + key);
		}
		return Boolean.parseBoolean(value);
	}

	private static int integer(Map<String, String> values, String key, int fallback) {
		return Integer.parseInt(values.getOrDefault(key, Integer.toString(fallback)));
	}

	private static boolean bool(Map<String, String> values, String key, boolean fallback) {
		String value = values.getOrDefault(key, Boolean.toString(fallback));
		if (!value.equals("true") && !value.equals("false")) {
			throw new IllegalArgumentException("Invalid boolean option: --" + key);
		}
		return Boolean.parseBoolean(value);
	}
}
