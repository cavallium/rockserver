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
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.SerializedKVBatch.SerializedKVBatchRef;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import it.cavallium.rockserver.core.common.api.proto.ScanRawRequest;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.RocksLeakDetector;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
import java.util.concurrent.ConcurrentLinkedQueue;
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

	private static final String RESULT_SCHEMA = "rockserver-grpc-raw-scan-comparison-v1";
	private static final String WORKER_SCHEMA = "rockserver-grpc-raw-scan-worker-v1";
	private static final String DATASET_SCHEMA = "rockserver-grpc-raw-scan-dataset-v1";
	private static final String COLUMN_NAME = "grpc-raw-scan-benchmark";
	private static final long VALUE_SEED = 0x5241575343414e31L;
	private static final long COOPERATIVE_QUANTUM_NANOS = TimeUnit.MILLISECONDS.toNanos(8L);
	private static final long STREAM_DEADLINE_MINUTES = 15L;
	private static final int RAW_MAX_ENTRIES = 65_536;
	private static final int RAW_MAX_SERIALIZED_BYTES = 3 * 1024 * 1024;

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
		Path root = options.root().toAbsolutePath().normalize();
		if (Files.exists(root)) {
			throw new IllegalArgumentException("Benchmark root already exists; refusing to reuse state: " + root);
		}
		Files.createDirectories(root);
		Path shared = root.resolve("shared-dataset");
		Files.createDirectories(shared);
		Path config = shared.resolve("rockserver.conf");
		Files.writeString(config, configText(options), StandardOpenOption.CREATE_NEW);

		Instant started = Instant.now();
		FileStore store = Files.getFileStore(root);
		writeControllerMetadata(root, options, store, started);
		prepareDataset(shared.resolve("db"), config, options);

		String currentClassPath = System.getProperty("java.class.path");
		String baselineClassPath = replaceProductionClasses(currentClassPath,
				options.candidateClasses(), options.baselineClasses());
		String candidateClassPath = replaceProductionClasses(currentClassPath,
				options.candidateClasses(), options.candidateClasses());
		List<WorkerResult> results = new ArrayList<>(options.rounds() * 2);
		for (int round = 1; round <= options.rounds(); round++) {
			boolean baselineFirst = (round & 1) == 1;
			if (baselineFirst) {
				results.add(runChild(options, shared, root, round, Implementation.BASELINE, baselineClassPath));
				results.add(runChild(options, shared, root, round, Implementation.CANDIDATE, candidateClassPath));
			} else {
				results.add(runChild(options, shared, root, round, Implementation.CANDIDATE, candidateClassPath));
				results.add(runChild(options, shared, root, round, Implementation.BASELINE, baselineClassPath));
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
			String classPath) throws Exception {
		Path output = root.resolve("round-%02d-%s.properties".formatted(round, implementation.value));
		List<String> command = new ArrayList<>(List.of(
				Path.of(System.getProperty("java.home"), "bin", "java").toString(),
				"--enable-native-access=ALL-UNNAMED",
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
				"--write-parallelism=" + options.writeParallelism(),
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
		return WorkerResult.read(output);
	}

	private static void runWorker(Options options) throws Exception {
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
			ScanRawRequest request = ScanRawRequest.newBuilder()
					.setColumnId(columnId)
					.setShardIndex(0)
					.setShardCount(1)
					.setContext(it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
							.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH)
							.setDeadlineEpochMillis(Long.MAX_VALUE)
							.build())
					.build();

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
		WorkerResult result = WorkerResult.from(options, measurement, leaks);
		result.write(options.output());
		System.out.printf(Locale.ROOT,
				"RAW_RESULT implementation=%s round=%d throughput=%.3f MiB/s entries=%.0f/s "
						+ "scan_p99=%.3fms queue_p99=%.3fms max_active=%d requests=%d/%d passed=%s%n",
				result.implementation().value, result.round(), result.bytesPerSecond() / (1024 * 1024),
				result.entriesPerSecond(), result.scanP99Nanos() / 1_000_000.0,
				result.queueP99Nanos() / 1_000_000.0, result.maximumActive(), result.terminalRequests(),
				result.submittedRequests(), result.passed());
	}

	private static void prepareDataset(Path database, Path config, Options options) throws Exception {
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
						+ "preload-keys=" + options.preloadKeys() + "\n"
						+ "flush-keys=" + options.flushKeys() + "\n"
						+ "value-bytes=" + options.valueBytes() + "\n",
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
						scanOnce(stub, request, options.preloadKeys(), expectedValue);
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
		ConcurrentLinkedQueue<Long> scanLatencies = new ConcurrentLinkedQueue<>();
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
						ScanResult result = scanOnce(stub, request, options.preloadKeys(), expectedValue);
						scanLatencies.add(System.nanoTime() - scanStarted);
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
			PoolCounters before = PoolCounters.capture(embedded);
			tracker.startTracking();
			long started = System.nanoTime();
			deadline[0] = started + TimeUnit.SECONDS.toNanos(options.measureSeconds());
			start.countDown();
			for (Future<?> future : futures) {
				future.get();
			}
			long finished = System.nanoTime();
			done.set(true);
			samplerFuture.get();
			tracker.stopTracking();
			RequestAccounting accounting = tracker.awaitSnapshot();
			awaitDrain(embedded);
			PoolCounters after = PoolCounters.capture(embedded);
			SchedulerMetrics schedulerMetrics = schedulerMetrics(meterRegistry);
			long[] sortedLatencies = scanLatencies.stream().mapToLong(Long::longValue).sorted().toArray();
			return new WorkerMeasurement(finished - started,
					scans.sum(), entries.sum(), bytes.sum(), batches.sum(), fullBatches.sum(),
					maximumBatchBytes.get(), GrpcOverloadBenchmark.percentile(sortedLatencies, 0.99d),
					schedulerMetrics, accounting, sampler.snapshot(), after.minus(before), true);
		} finally {
			done.set(true);
			start.countDown();
			tracker.stopTrackingIfActive();
			executor.shutdownNow();
			executor.awaitTermination(10, TimeUnit.SECONDS);
			composite.remove(meterRegistry);
			meterRegistry.close();
		}
	}

	private static ScanResult scanOnce(RocksDBServiceGrpc.RocksDBServiceBlockingStub stub,
			ScanRawRequest request,
			int expectedEntries,
			byte[] expectedValue) {
		BitSet seen = new BitSet(expectedEntries);
		long entries = 0L;
		long serializedBytes = 0L;
		long batches = 0L;
		long fullBatches = 0L;
		int maximumBatchBytes = 0;
		var responses = stub.scanRaw(request);
		while (responses.hasNext()) {
			var bytes = responses.next().getSerialized();
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
					if (!Arrays.equals(expectedValue, kv.value().toByteArray())) {
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
		return new ScanResult(entries, serializedBytes, batches, fullBatches, maximumBatchBytes);
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
			var admission = embedded.getScheduler().admissionSnapshot();
			var db = embedded.getInternalDB();
			if (admission.totalQueued() == 0 && admission.totalActive() == 0
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
		double[] throughputRatios = pairedRatios(baseline, candidate,
				WorkerResult::bytesPerSecond);
		double[] queueRatios = pairedRatios(baseline, candidate,
				result -> result.queueP99Nanos());
		double[] scanRatios = pairedRatios(baseline, candidate,
				result -> result.scanP99Nanos());
		return evaluateComparison(throughputRatios, queueRatios, scanRatios,
				results.stream().allMatch(WorkerResult::passed), options.minimumThroughputRatio(),
				options.maximumQueueP99Ratio(), options.maximumScanP99Ratio());
	}

	private static List<WorkerResult> byImplementation(List<WorkerResult> results,
			Implementation implementation) {
		return results.stream()
				.filter(result -> result.implementation() == implementation)
				.sorted(java.util.Comparator.comparingInt(WorkerResult::round))
				.toList();
	}

	private static double[] pairedRatios(List<WorkerResult> baseline,
			List<WorkerResult> candidate,
			java.util.function.ToDoubleFunction<WorkerResult> metric) {
		double[] ratios = new double[baseline.size()];
		for (int index = 0; index < ratios.length; index++) {
			double base = metric.applyAsDouble(baseline.get(index));
			double next = metric.applyAsDouble(candidate.get(index));
			ratios[index] = base > 0.0d && next > 0.0d ? next / base : Double.POSITIVE_INFINITY;
		}
		return ratios;
	}

	/** Pure acceptance helper used by deterministic tests. */
	public static Comparison evaluateForTesting(double[] baselineThroughput,
			double[] candidateThroughput,
			double[] baselineQueueP99,
			double[] candidateQueueP99,
			double[] baselineScanP99,
			double[] candidateScanP99,
			double minimumThroughputRatio,
			double maximumQueueP99Ratio,
			double maximumScanP99Ratio) {
		if (baselineThroughput.length != candidateThroughput.length
				|| baselineQueueP99.length != candidateQueueP99.length
				|| baselineScanP99.length != candidateScanP99.length
				|| baselineThroughput.length != baselineQueueP99.length
				|| baselineThroughput.length != baselineScanP99.length) {
			throw new IllegalArgumentException("All paired samples must have the same length");
		}
		double[] throughput = new double[baselineThroughput.length];
		double[] queue = new double[baselineThroughput.length];
		double[] scan = new double[baselineThroughput.length];
		for (int index = 0; index < throughput.length; index++) {
			throughput[index] = candidateThroughput[index] / baselineThroughput[index];
			queue[index] = candidateQueueP99[index] / baselineQueueP99[index];
			scan[index] = candidateScanP99[index] / baselineScanP99[index];
		}
		return evaluateComparison(throughput, queue, scan, true, minimumThroughputRatio,
				maximumQueueP99Ratio, maximumScanP99Ratio);
	}

	private static Comparison evaluateComparison(double[] throughputRatios,
			double[] queueRatios,
			double[] scanRatios,
			boolean workersPassed,
			double minimumThroughputRatio,
			double maximumQueueP99Ratio,
			double maximumScanP99Ratio) {
		var throughput = GrpcOverloadBenchmark.ratioConfidenceInterval(throughputRatios);
		var queue = GrpcOverloadBenchmark.ratioConfidenceInterval(queueRatios);
		var scan = GrpcOverloadBenchmark.ratioConfidenceInterval(scanRatios);
		List<String> failures = new ArrayList<>();
		if (!workersPassed) {
			failures.add("one or more worker correctness/work-conservation gates failed");
		}
		// Reject only when the entire confidence interval demonstrates a regression. An interval
		// crossing the equality boundary is inconclusive, not evidence that one build is worse.
		if (!throughput.available() || throughput.upper95() < minimumThroughputRatio) {
			failures.add("candidate raw throughput upper 95% bound is below " + minimumThroughputRatio);
		}
		if (!queue.available() || queue.lower95() > maximumQueueP99Ratio) {
			failures.add("candidate scheduler queue-p99 lower 95% bound exceeds " + maximumQueueP99Ratio);
		}
		if (!scan.available() || scan.lower95() > maximumScanP99Ratio) {
			failures.add("candidate end-to-end scan-p99 lower 95% bound exceeds " + maximumScanP99Ratio);
		}
		return new Comparison(throughput, queue, scan, List.copyOf(failures));
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
				    workload: { batch-queue-capacity: 4096 }
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
				""".formatted(options.readParallelism(), options.writeParallelism());
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
			Instant started) throws IOException {
		Files.writeString(root.resolve("metadata.properties"),
				"schema=" + RESULT_SCHEMA + "\n"
						+ "started=" + started + "\n"
						+ "build-baseline=" + options.buildBaseline() + "\n"
						+ "build-candidate=" + options.buildCandidate() + "\n"
						+ "build-state-baseline=" + options.buildStateBaseline() + "\n"
						+ "build-state-candidate=" + options.buildStateCandidate() + "\n"
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
						+ "warmup-passes=" + options.warmupPasses() + "\n"
						+ "measure-seconds=" + options.measureSeconds() + "\n"
						+ "rounds=" + options.rounds() + "\n"
						+ "sample-micros=" + options.sampleMicros() + "\n"
						+ "minimum-throughput-ratio=" + options.minimumThroughputRatio() + "\n"
						+ "maximum-queue-p99-ratio=" + options.maximumQueueP99Ratio() + "\n"
						+ "maximum-scan-p99-ratio=" + options.maximumScanP99Ratio() + "\n"
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
		StringBuilder out = new StringBuilder("# Paired whole-path raw-scan comparison\n\n");
		out.append("- Schema: `").append(RESULT_SCHEMA).append("`\n")
				.append("- Started / finished: `").append(started).append("` / `").append(finished).append("`\n")
				.append("- Baseline / candidate: `").append(options.buildBaseline()).append("` / `")
				.append(options.buildCandidate()).append("`\n")
				.append("- Storage: `").append(options.storageLabel()).append("`, `")
				.append(store.name()).append("`, `").append(store.type()).append("`\n")
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
		out.append("\n- Candidate/baseline throughput ratio: ").append(interval(comparison.throughput()))
				.append(" (reject if upper bound < ").append(format(options.minimumThroughputRatio())).append(")\n")
				.append("- Candidate/baseline scheduler queue-p99 ratio: ").append(interval(comparison.queueP99()))
				.append(" (reject if lower bound > ").append(format(options.maximumQueueP99Ratio())).append(")\n")
				.append("- Candidate/baseline end-to-end scan-p99 ratio: ").append(interval(comparison.scanP99()))
				.append(" (reject if lower bound > ").append(format(options.maximumScanP99Ratio())).append(")\n");
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
		StringBuilder out = new StringBuilder("{\n  \"schema\": \"").append(RESULT_SCHEMA)
				.append("\",\n  \"started\": \"").append(started)
				.append("\",\n  \"finished\": \"").append(finished)
				.append("\",\n  \"build_baseline\": \"").append(json(options.buildBaseline()))
				.append("\",\n  \"build_candidate\": \"").append(json(options.buildCandidate()))
				.append("\",\n  \"storage_label\": \"").append(json(options.storageLabel()))
				.append("\",\n  \"storage_name\": \"").append(json(store.name()))
				.append("\",\n  \"storage_type\": \"").append(json(store.type()))
				.append("\",\n  \"passed\": ").append(comparison.passed())
				.append(",\n  \"throughput_ratio\": ").append(intervalJson(comparison.throughput()))
				.append(",\n  \"scheduler_queue_p99_ratio\": ").append(intervalJson(comparison.queueP99()))
				.append(",\n  \"scan_p99_ratio\": ").append(intervalJson(comparison.scanP99()))
				.append(",\n  \"rounds\": [\n");
		for (int index = 0; index < results.size(); index++) {
			WorkerResult result = results.get(index);
			out.append("    {\"round\": ").append(result.round())
					.append(", \"implementation\": \"").append(result.implementation().value)
					.append("\", \"bytes_per_second\": ").append(format(result.bytesPerSecond()))
					.append(", \"entries_per_second\": ").append(format(result.entriesPerSecond()))
					.append(", \"scan_p99_nanos\": ").append(result.scanP99Nanos())
					.append(", \"scheduler_queue_p99_nanos\": ").append(result.queueP99Nanos())
					.append(", \"scheduler_execution_p99_nanos\": ").append(result.executionP99Nanos())
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
					.append(", \"scheduler_started\": ").append(result.schedulerStarted())
					.append(", \"scheduler_completed\": ").append(result.schedulerCompleted())
					.append(", \"scheduler_outcomes\": ").append(result.schedulerOutcomes())
					.append(", \"scheduler_failures\": ").append(result.schedulerFailures())
					.append(", \"sampler_samples\": ").append(result.samplerSamples())
					.append(", \"saturating_demand_samples\": ").append(result.saturatingDemandSamples())
					.append(", \"maximum_avoidable_idle_nanos\": ").append(result.maximumAvoidableIdleNanos())
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

	private static String interval(GrpcOverloadBenchmark.RatioConfidenceInterval interval) {
		return "mean=" + format(interval.mean()) + ", 95% CI=[" + format(interval.lower95())
				+ ", " + format(interval.upper95()) + ']';
	}

	private static String intervalJson(GrpcOverloadBenchmark.RatioConfidenceInterval interval) {
		return "{\"samples\": " + interval.samples() + ", \"mean\": " + format(interval.mean())
				+ ", \"lower_95\": " + format(interval.lower95()) + ", \"upper_95\": "
				+ format(interval.upper95()) + '}';
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
				Paired whole-gRPC raw-SST comparison. Compile tests and build the untouched baseline first:

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
				20 READ workers, one complete warmup scan/client, 15 measured seconds, five paired
				rounds, alternating implementation order, strict candidate idle instrumentation,
				and no statistically significant throughput, scheduler queue-p99, or scan-p99 loss.
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
			boolean exactWaitingWorkers) {

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

		private PoolSampler(Options options) {
			this.options = options;
		}

		private void sample(EmbeddedConnection embedded) {
			var snapshot = embedded.getScheduler().poolSnapshot(RWScheduler.Pool.READ);
			samples++;
			workerCount = snapshot.workerCount();
			maximumActive = Math.max(maximumActive, snapshot.activeTasks());
			boolean saturating = snapshot.activeTasks() >= snapshot.workerCount()
					|| (snapshot.queuedTasks() > 0
					&& snapshot.activeTasks() + snapshot.queuedTasks() >= snapshot.workerCount());
			if (saturating) {
				saturatingDemandSamples++;
			}
			if (!options.instrumentationMode().equals("strict") || snapshot.queuedTasks() == 0) {
				consecutiveAvoidableIdleSamples = 0;
				return;
			}
			if (snapshot.waitingWorkers() > 0) {
				consecutiveAvoidableIdleSamples++;
				maximumConsecutiveAvoidableIdleSamples = Math.max(
						maximumConsecutiveAvoidableIdleSamples, consecutiveAvoidableIdleSamples);
			} else {
				consecutiveAvoidableIdleSamples = 0;
			}
		}

		private PoolUtilization snapshot() {
			return new PoolUtilization(samples, saturatingDemandSamples, maximumActive, workerCount,
					maximumConsecutiveAvoidableIdleSamples, options.sampleNanos(),
					options.instrumentationMode().equals("strict"));
		}
	}

	private record PoolCounters(long accepted,
			long started,
			long completed,
			long failed,
			long outcomes) {

		private static PoolCounters capture(EmbeddedConnection embedded) {
			var snapshot = embedded.getScheduler().poolSnapshot(RWScheduler.Pool.READ);
			return new PoolCounters(snapshot.acceptedTasks(), snapshot.startedTasks(), snapshot.completedTasks(),
					snapshot.failedTasks(), snapshot.outcomes().values().stream().mapToLong(Long::longValue).sum());
		}

		private PoolCounters minus(PoolCounters before) {
			return new PoolCounters(accepted - before.accepted, started - before.started,
					completed - before.completed, failed - before.failed, outcomes - before.outcomes);
		}

		private boolean conserved() {
			return accepted > 0L && started == completed && failed == 0L && outcomes == accepted;
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
			SchedulerMetrics schedulerMetrics,
			RequestAccounting requests,
			PoolUtilization utilization,
			PoolCounters counters,
			boolean resourcesDrained) {
	}

	private record WorkerResult(Implementation implementation,
			int round,
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
			long submittedRequests,
			long terminalRequests,
			long duplicateTerminals,
			long inFlightRequests,
			long nonOkRequests,
			long schedulerAccepted,
			long schedulerStarted,
			long schedulerCompleted,
			long schedulerOutcomes,
			long schedulerFailures,
			long samplerSamples,
			long saturatingDemandSamples,
			int maximumActive,
			int workerCount,
			long maximumAvoidableIdleNanos,
			boolean exactWaitingWorkers,
			boolean resourcesDrained,
			long nativeLeaks,
			boolean passed) {

		private static WorkerResult from(Options options, WorkerMeasurement measurement, long nativeLeaks) {
			double seconds = measurement.elapsedNanos() / 1_000_000_000.0d;
			boolean hasFullBatch = options.smoke() || measurement.fullBatches() > 0L;
			boolean passed = measurement.scans() > 0L
					&& measurement.entries() == measurement.scans() * options.preloadKeys()
					&& measurement.batches() > 0L
					&& hasFullBatch
					&& measurement.maximumBatchBytes() <= RAW_MAX_SERIALIZED_BYTES
					&& measurement.scanP99Nanos() > 0L
					&& measurement.schedulerMetrics().queueP99Nanos() > 0L
					&& measurement.schedulerMetrics().executionP99Nanos() > 0L
					&& measurement.requests().conserved()
					&& measurement.utilization().passed()
					&& measurement.counters().conserved()
					&& measurement.resourcesDrained()
					&& nativeLeaks == 0L;
			return new WorkerResult(options.implementation(), options.round(),
					measurement.serializedBytes() / seconds, measurement.entries() / seconds,
					measurement.scans(), measurement.entries(), measurement.batches(), measurement.fullBatches(),
					measurement.maximumBatchBytes(), measurement.scanP99Nanos(),
					measurement.schedulerMetrics().queueP99Nanos(),
					measurement.schedulerMetrics().executionP99Nanos(),
					measurement.requests().submitted(), measurement.requests().terminal(),
					measurement.requests().duplicateTerminal(), measurement.requests().inFlight(),
					measurement.requests().nonOk(), measurement.counters().accepted(),
					measurement.counters().started(), measurement.counters().completed(),
					measurement.counters().outcomes(), measurement.counters().failed(),
					measurement.utilization().samples(), measurement.utilization().saturatingDemandSamples(),
					measurement.utilization().maximumActive(), measurement.utilization().workerCount(),
					measurement.utilization().maximumAvoidableIdleNanos(),
					measurement.utilization().exactWaitingWorkers(), measurement.resourcesDrained(), nativeLeaks, passed);
		}

		private void write(Path output) throws IOException {
			Properties properties = new Properties();
			properties.setProperty("schema", WORKER_SCHEMA);
			properties.setProperty("implementation", implementation.value);
			properties.setProperty("round", Integer.toString(round));
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
			properties.setProperty("submitted-requests", Long.toString(submittedRequests));
			properties.setProperty("terminal-requests", Long.toString(terminalRequests));
			properties.setProperty("duplicate-terminals", Long.toString(duplicateTerminals));
			properties.setProperty("in-flight-requests", Long.toString(inFlightRequests));
			properties.setProperty("non-ok-requests", Long.toString(nonOkRequests));
			properties.setProperty("scheduler-accepted", Long.toString(schedulerAccepted));
			properties.setProperty("scheduler-started", Long.toString(schedulerStarted));
			properties.setProperty("scheduler-completed", Long.toString(schedulerCompleted));
			properties.setProperty("scheduler-outcomes", Long.toString(schedulerOutcomes));
			properties.setProperty("scheduler-failures", Long.toString(schedulerFailures));
			properties.setProperty("sampler-samples", Long.toString(samplerSamples));
			properties.setProperty("saturating-demand-samples", Long.toString(saturatingDemandSamples));
			properties.setProperty("maximum-active", Integer.toString(maximumActive));
			properties.setProperty("worker-count", Integer.toString(workerCount));
			properties.setProperty("maximum-avoidable-idle-nanos", Long.toString(maximumAvoidableIdleNanos));
			properties.setProperty("exact-waiting-workers", Boolean.toString(exactWaitingWorkers));
			properties.setProperty("resources-drained", Boolean.toString(resourcesDrained));
			properties.setProperty("native-leaks", Long.toString(nativeLeaks));
			properties.setProperty("passed", Boolean.toString(passed));
			try (var stream = Files.newOutputStream(output, StandardOpenOption.CREATE_NEW)) {
				properties.store(stream, null);
			}
		}

		private static WorkerResult read(Path input) throws IOException {
			Properties values = new Properties();
			try (InputStream stream = Files.newInputStream(input)) {
				values.load(stream);
			}
			if (!WORKER_SCHEMA.equals(required(values, "schema"))) {
				throw new IllegalArgumentException("Unknown raw-scan worker schema: " + input);
			}
			return new WorkerResult(Implementation.parse(required(values, "implementation")),
					integer(values, "round"), decimal(values, "bytes-per-second"),
					decimal(values, "entries-per-second"), number(values, "scans"),
					number(values, "entries"), number(values, "batches"), number(values, "full-batches"),
					integer(values, "maximum-batch-bytes"), number(values, "scan-p99-nanos"),
					number(values, "queue-p99-nanos"), number(values, "execution-p99-nanos"),
					number(values, "submitted-requests"), number(values, "terminal-requests"),
					number(values, "duplicate-terminals"), number(values, "in-flight-requests"),
					number(values, "non-ok-requests"), number(values, "scheduler-accepted"),
					number(values, "scheduler-started"), number(values, "scheduler-completed"),
					number(values, "scheduler-outcomes"), number(values, "scheduler-failures"),
					number(values, "sampler-samples"), number(values, "saturating-demand-samples"),
					integer(values, "maximum-active"), integer(values, "worker-count"),
					number(values, "maximum-avoidable-idle-nanos"), bool(values, "exact-waiting-workers"),
					bool(values, "resources-drained"), number(values, "native-leaks"), bool(values, "passed"));
		}
	}

	public record Comparison(GrpcOverloadBenchmark.RatioConfidenceInterval throughput,
			GrpcOverloadBenchmark.RatioConfidenceInterval queueP99,
			GrpcOverloadBenchmark.RatioConfidenceInterval scanP99,
			List<String> failures) {

		public Comparison {
			failures = List.copyOf(failures);
		}

		private static Comparison failed(String failure) {
			var unavailable = new GrpcOverloadBenchmark.RatioConfidenceInterval(0,
					Double.NaN, Double.NaN, Double.NaN);
			return new Comparison(unavailable, unavailable, unavailable, List.of(failure));
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
			int warmupPasses,
			int measureSeconds,
			int rounds,
			int sampleMicros,
			String instrumentationMode,
			double minimumThroughputRatio,
			double maximumQueueP99Ratio,
			double maximumScanP99Ratio,
			String childHeap,
			boolean enforce,
			boolean smoke) {

		private static final Set<String> KNOWN_OPTIONS = Set.of(
				"worker", "root", "dataset-root", "output", "baseline-classes", "candidate-classes",
				"implementation", "round", "build-baseline", "build-candidate", "build-state-baseline",
				"build-state-candidate", "storage-label", "host-state", "preload-keys", "flush-keys",
				"value-bytes", "batch-entries", "scan-clients", "read-parallelism", "write-parallelism",
				"warmup-passes", "measure-seconds", "rounds", "sample-micros", "instrumentation-mode",
				"minimum-throughput-ratio", "maximum-queue-p99-ratio", "maximum-scan-p99-ratio",
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
					integer(values, "warmup-passes", 1), integer(values, "measure-seconds", smoke ? 2 : 15),
					integer(values, "rounds", smoke ? 1 : 5), integer(values, "sample-micros", 250),
					values.getOrDefault("instrumentation-mode", worker ? "strict" : "controller"),
					decimal(values, "minimum-throughput-ratio", 1.0d),
					decimal(values, "maximum-queue-p99-ratio", 1.0d),
					decimal(values, "maximum-scan-p99-ratio", 1.0d),
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
					|| warmupPasses < 1 || measureSeconds < 1 || rounds < 1 || sampleMicros < 1) {
				throw new IllegalArgumentException("worker, duration, round, and sampling dimensions must be positive");
			}
			if (!List.of("strict", "portable", "controller").contains(instrumentationMode)) {
				throw new IllegalArgumentException("instrumentation-mode must be strict, portable, or controller");
			}
			if (worker && instrumentationMode.equals("controller")) {
				throw new IllegalArgumentException("workers require strict or portable instrumentation");
			}
			if (!Double.isFinite(minimumThroughputRatio) || minimumThroughputRatio <= 0.0d
					|| !Double.isFinite(maximumQueueP99Ratio) || maximumQueueP99Ratio <= 0.0d
					|| !Double.isFinite(maximumScanP99Ratio) || maximumScanP99Ratio <= 0.0d) {
				throw new IllegalArgumentException("comparison ratios must be finite and positive");
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
				if (smoke || worker || rounds < 5 || preloadKeys < 1_000_000 || flushKeys > 125_000
						|| scanClients * 4 < readParallelism || readParallelism < 20 || measureSeconds < 15
						|| !buildBaseline.matches("[0-9a-f]{40}") || !buildCandidate.matches("[0-9a-f]{40}")
						|| !buildStateBaseline.equals("clean") || !buildStateCandidate.equals("clean")
						|| !hostState.equals("dedicated") || storageLabel.equals("ci-structural")) {
					throw new IllegalArgumentException("enforced raw-scan comparison requires clean full SHAs, "
							+ "dedicated hardware, five full paired rounds, and saturating 20-worker dimensions");
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
		return Double.parseDouble(required(values, key));
	}

	private static boolean bool(Properties values, String key) {
		return Boolean.parseBoolean(required(values, key));
	}

	private static int integer(Map<String, String> values, String key, int fallback) {
		return Integer.parseInt(values.getOrDefault(key, Integer.toString(fallback)));
	}

	private static double decimal(Map<String, String> values, String key, double fallback) {
		return Double.parseDouble(values.getOrDefault(key, Double.toString(fallback)));
	}

	private static boolean bool(Map<String, String> values, String key, boolean fallback) {
		return Boolean.parseBoolean(values.getOrDefault(key, Boolean.toString(fallback)));
	}
}
