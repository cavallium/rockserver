package it.cavallium.rockserver.core.impl.benchmark;

import com.google.protobuf.ByteString;
import com.sun.management.OperatingSystemMXBean;
import com.sun.management.ThreadMXBean;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.api.proto.GetRequest;
import it.cavallium.rockserver.core.common.api.proto.GetResponse;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.api.proto.PutRequest;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Forked end-to-end macrobenchmark for RocksDB unary Get over a stock generated gRPC client.
 *
 * <p>The controller launches one fresh JVM and one fresh, identically populated database for every
 * round/strategy pair. Selector tuning uses hit-only fixed-size workloads. Misses, mixed-size traffic,
 * and concurrent writes are deliberately confined to the diagnostic stage and never select a strategy.
 * Heap and pinned are the only safe candidates; legacy is informational and automatic is validated in
 * held-out forks after the selector has been chosen.</p>
 */
public final class GrpcFastGetBenchmark {

	private static final String STRATEGY_PROPERTY = "rockserver.grpc.fast-get.strategy";
	private static final int KEYS_PER_SIZE = 16;
	private static final int[] FIXED_SIZES = {
			0, 1, 8, 32, 64, 128, 256, 512, 1_024, 2_048, 4_096, 8_192,
			16_384, 32_768, 65_536, 131_072, 262_144, 524_288, 1_048_576, 2_097_152
	};
	private static final long MISSING_KEY = Long.MIN_VALUE;
	private static final long WRITER_KEY = Long.MIN_VALUE + 1;
	private static final String CSV_HEADER = "stage,round,order_position,strategy,band,warmup_actual_ms,throughput,"
			+ "p50_ns,p95_ns,p99_ns,p999_ns,cpu_percent,allocated_bytes_per_op,gc_collections,"
			+ "gc_millis,jit_millis,misses,writes,errors,samples,sample_overflow,measurement_overshoot_ns";

	private GrpcFastGetBenchmark() {
	}

	public static void main(String[] args) throws Exception {
		if (Arrays.asList(args).contains("--help")) {
			printUsage();
			return;
		}
		Options options = Options.parse(args);
		System.setProperty("rockserver.core.print-config", "false");
		if (options.worker()) {
			runWorker(options);
		} else {
			runController(options);
		}
	}

	private static void runController(Options options) throws Exception {
		Path lockPath = Path.of(System.getProperty("java.io.tmpdir"),
				"rockserver-grpc-fast-get-" + System.getProperty("user.name") + ".lock");
		try (FileChannel lockChannel = FileChannel.open(lockPath,
				StandardOpenOption.CREATE, StandardOpenOption.WRITE);
				var lock = lockChannel.tryLock()) {
			if (lock == null) {
				throw new IllegalStateException("Another fast-Get benchmark controller holds " + lockPath);
			}
			runControllerLocked(options);
		}
	}

	private static void runControllerLocked(Options options) throws Exception {
		Path root = options.root().toAbsolutePath().normalize();
		if (Files.exists(root)) {
			throw new IllegalArgumentException("Benchmark root already exists; refusing to reuse state: " + root);
		}
		Files.createDirectories(root);
		writeMetadata(root, options);
		List<Result> results = new ArrayList<>();
		for (int round = 1; round <= options.rounds(); round++) {
			List<BandSpec> bands = selectedBands(options.stage(), options.bands(), round, options.writerRate());
			for (int bandPosition = 0; bandPosition < bands.size(); bandPosition++) {
				BandSpec band = bands.get(bandPosition);
				List<Strategy> order = strategyOrder(options.stage(), round + bandPosition);
				for (int position = 0; position < order.size(); position++) {
					Strategy strategy = order.get(position);
					String identity = "round-%02d-band-%s-%02d-%s".formatted(round, band.name(), position + 1,
							strategy.propertyValue);
					Path workerRoot = root.resolve(identity);
					Path workerCsv = root.resolve(identity + ".csv");
					System.out.printf("Starting isolated fork: stage=%s round=%d band=%s position=%d strategy=%s%n",
							options.stage().value, round, band.name(), position + 1, strategy.propertyValue);
					runChild(options, workerRoot, workerCsv, round, position + 1, strategy, band.name());
					results.addAll(readCsv(workerCsv));
				}
			}
		}
		writeCsv(options.output(), results);
		printSummary(results, options.stage(), options.regressionLimit());
		if (options.stage() == Stage.VALIDATION && options.enforceGate()) {
			verifyAutomaticGate(results, options.regressionLimit());
		}
	}

	private static void runChild(Options options,
			Path workerRoot,
			Path workerCsv,
			int round,
			int orderPosition,
			Strategy strategy,
			String band) throws Exception {
		String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
		String classPath = System.getProperty("java.class.path");
		if (!classPath.contains("test-classes")) {
			throw new IllegalStateException("Run the controller directly with target/test-classes on the classpath; "
					+ "the Maven exec plugin does not expose a reusable worker classpath");
		}
		List<String> command = new ArrayList<>(List.of(
				java,
				"--enable-native-access=ALL-UNNAMED",
				"-Xms" + options.childHeap(),
				"-Xmx" + options.childHeap(),
				"-XX:+AlwaysPreTouch",
				"-XX:-BackgroundCompilation",
				"-cp", classPath,
				GrpcFastGetBenchmark.class.getName(),
				"--worker=true",
				"--stage=" + options.stage().value,
				"--root=" + workerRoot,
				"--output=" + workerCsv,
				"--round=" + round,
				"--order-position=" + orderPosition,
				"--strategy=" + strategy.propertyValue,
				"--bands=" + band,
				"--warmup-ms=" + options.warmupMillis(),
				"--max-warmup-ms=" + options.maxWarmupMillis(),
				"--warmup-quiet-periods=" + options.warmupQuietPeriods(),
				"--warmup-jit-ms-per-second=" + options.warmupJitMillisPerSecond(),
				"--warmup-throughput-tolerance=" + options.warmupThroughputTolerance(),
				"--measure-ms=" + options.measureMillis(),
				"--readers=" + options.readers(),
				"--writer-rate=" + options.writerRate(),
				"--sample-every=" + options.sampleEvery(),
				"--max-samples-per-reader=" + options.maxSamplesPerReader(),
				"--max-jit-ms=" + options.maxJitMillis(),
				"--seed=" + options.seed(),
				"--smoke=" + options.smoke()
		));
		Process process = new ProcessBuilder(command).inheritIO().start();
		Thread cleanup = Thread.ofPlatform().name("grpc-fast-get-child-cleanup").unstarted(
				() -> terminateChild(process));
		Runtime.getRuntime().addShutdownHook(cleanup);
		boolean completed;
		try {
			long timeoutMillis = options.maxWarmupMillis() + options.measureMillis() + 60_000;
			completed = process.waitFor(timeoutMillis, TimeUnit.MILLISECONDS);
			if (!completed) {
				terminateChild(process);
				throw new IllegalStateException("Benchmark worker exceeded its " + timeoutMillis
						+ "ms lifecycle deadline: " + strategy + ", band=" + band);
			}
		} finally {
			try {
				Runtime.getRuntime().removeShutdownHook(cleanup);
			} catch (IllegalStateException shuttingDown) {
				// The registered hook owns cleanup once JVM shutdown has begun.
			}
		}
		int exit = process.exitValue();
		if (exit != 0) {
			throw new IllegalStateException("Benchmark worker exited with status " + exit + ": " + strategy);
		}
	}

	private static void terminateChild(Process process) {
		if (!process.isAlive()) {
			return;
		}
		process.destroy();
		try {
			if (!process.waitFor(10, TimeUnit.SECONDS)) {
				process.destroyForcibly();
				process.waitFor(10, TimeUnit.SECONDS);
			}
		} catch (InterruptedException interrupted) {
			process.destroyForcibly();
			Thread.currentThread().interrupt();
		}
	}

	private static void runWorker(Options options) throws Exception {
		Path root = options.root().toAbsolutePath().normalize();
		if (Files.exists(root)) {
			throw new IllegalArgumentException("Worker root already exists; refusing to reuse state: " + root);
		}
		Files.createDirectories(root);
		Path config = root.resolve("rockserver.conf");
		Files.writeString(config, configText(), StandardOpenOption.CREATE_NEW);
		System.setProperty(STRATEGY_PROPERTY, options.strategy().propertyValue);
		Snapshot.enableAllocationMeasurement();
		List<BandSpec> bandSpecs = selectedBands(options.stage(), options.bands(), options.round(),
				options.writerRate());
		List<Result> results = new ArrayList<>();
		try (var embedded = new EmbeddedConnection(root.resolve("db"), "grpc-fast-get-benchmark", config)) {
			long columnId = populate(embedded.getSyncApi(), bandSpecs);
			try (var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
				server.start();
				try (Client client = Client.open(server.getPort())) {
					preflight(client.stub(), columnId, bandSpecs);
					System.gc();
					Thread.sleep(500);
					for (BandSpec spec : bandSpecs) {
						Band band = createBand(columnId, spec);
						Result result = runBand(client.stub(), columnId, band, options);
						if (!options.smoke() && result.jitMillis() > options.maxJitMillis()) {
							throw new IllegalStateException("Timed interval was not JIT-steady: band=" + result.band()
									+ ", compiled-ms=" + result.jitMillis() + ", limit=" + options.maxJitMillis());
						}
						results.add(result);
						printResult(result);
					}
				}
			}
			long pendingDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (embedded.getInternalDB().getPendingOpsCount() != 0 && System.nanoTime() < pendingDeadline) {
				Thread.sleep(10);
			}
			if (embedded.getInternalDB().getPendingOpsCount() != 0) {
				throw new IllegalStateException("Benchmark leaked database operations: "
						+ embedded.getInternalDB().getPendingOpsCount());
			}
		}
		writeCsv(options.output(), results);
	}

	private static Result runBand(RocksDBServiceGrpc.RocksDBServiceBlockingStub stub,
			long columnId,
			Band band,
			Options options) throws Exception {
		int writerCount = band.writerRate() > 0 ? 1 : 0;
		int parties = options.readers() + writerCount + 1;
		var ready = new CountDownLatch(options.readers() + writerCount);
		var start = new CountDownLatch(1);
		var phases = new Phaser(parties);
		var control = new Control();
		ReaderState[] readers = new ReaderState[options.readers()];
		WriterState writer = writerCount == 0 ? null : new WriterState();
		ExecutorService executor = Executors.newFixedThreadPool(options.readers() + writerCount,
				Thread.ofPlatform().name("grpc-fast-get-bench-", 0).factory());
		try {
			for (int index = 0; index < readers.length; index++) {
				ReaderState state = new ReaderState(options.maxSamplesPerReader());
				readers[index] = state;
				long seed = options.seed() + options.round() * 10_000L + index * 977L;
				executor.submit(() -> runReader(stub, band, options.sampleEvery(), seed, state,
						control, ready, start, phases));
			}
			if (writer != null) {
				PutRequest[] requests = writerRequests(columnId);
				WriterState finalWriter = writer;
				executor.submit(() -> runWriter(stub, requests, band.writerRate(), finalWriter,
						control, ready, start, phases));
			}
			if (!ready.await(30, TimeUnit.SECONDS)) {
				throw new IllegalStateException("Benchmark workers did not become ready");
			}
			start.countDown();
			long warmupMillis = awaitSteadyWarmup(control, readers, options);
			awaitWorkers(phases, 1, 60);
			for (ReaderState reader : readers) {
				reader.resetMeasurements();
			}
			if (writer != null) {
				writer.resetMeasurements();
			}
			Snapshot before = Snapshot.capture();
			control.measurementDeadlineNanos = before.wallNanos
					+ TimeUnit.MILLISECONDS.toNanos(options.measureMillis());
			phases.arriveAndAwaitAdvance();
			sleepUntil(control.measurementDeadlineNanos);
			Snapshot after = Snapshot.capture();
			long overshoot = Math.max(0, after.wallNanos - control.measurementDeadlineNanos);
			phases.arriveAndAwaitAdvance();
			executor.shutdown();
			if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
				executor.shutdownNow();
				throw new IllegalStateException("Benchmark workers did not stop");
			}
			long warmupErrors = Arrays.stream(readers).mapToLong(state -> state.warmupErrors).sum()
					+ (writer == null ? 0 : writer.warmupErrors);
			if (warmupErrors != 0) {
				throw new IllegalStateException("Warmup failed with " + warmupErrors + " request errors");
			}
			return aggregate(band.name(), readers, writer, options, warmupMillis, before, after, overshoot);
		} finally {
			executor.shutdownNow();
		}
	}

	private static void runReader(RocksDBServiceGrpc.RocksDBServiceBlockingStub stub,
			Band band,
			int sampleEvery,
			long seed,
			ReaderState state,
			Control control,
			CountDownLatch ready,
			CountDownLatch start,
			Phaser phases) {
		try {
			long random = seed == 0 ? 0x9e3779b97f4a7c15L : seed;
			long sequence = 0;
			long sampleIndex = 0;
			ready.countDown();
			start.await();
			while (!control.warmupStop) {
				random = nextRandom(random);
				RequestCase request = selectRequest(band, sequence++, random);
				long started = System.nanoTime();
				try {
					validateResponse(stub.get(request.request()), request.expectedSize());
				} catch (Throwable error) {
					state.warmupErrors++;
				}
				long completed = System.nanoTime();
				state.reads++;
				if (request.expectedSize() < 0) {
					state.misses++;
				}
				if (sampleIndex++ % sampleEvery == 0) {
					state.recordLatency(completed - started);
				}
			}
			phases.arriveAndAwaitAdvance();
			while (true) {
				random = nextRandom(random);
				RequestCase request = selectRequest(band, sequence++, random);
				long started = System.nanoTime();
				boolean error = false;
				try {
					validateResponse(stub.get(request.request()), request.expectedSize());
				} catch (Throwable failure) {
					error = true;
				}
				long completed = System.nanoTime();
				if (completed > control.measurementDeadlineNanos) {
					break;
				}
				state.reads++;
				if (request.expectedSize() < 0) {
					state.misses++;
				}
				if (error) {
					state.errors++;
				}
				if (sampleIndex++ % sampleEvery == 0) {
					state.recordLatency(completed - started);
				}
			}
		} catch (Throwable failure) {
			if (control.measurementDeadlineNanos == 0) {
				state.warmupErrors++;
			} else {
				state.errors++;
			}
			if (failure instanceof InterruptedException) {
				Thread.currentThread().interrupt();
			}
		} finally {
			phases.arriveAndDeregister();
		}
	}

	private static void runWriter(RocksDBServiceGrpc.RocksDBServiceBlockingStub stub,
			PutRequest[] requests,
			long rate,
			WriterState state,
			Control control,
			CountDownLatch ready,
			CountDownLatch start,
			Phaser phases) {
		try {
			long interval = Math.max(1, TimeUnit.SECONDS.toNanos(1) / rate);
			long sequence = 0;
			ready.countDown();
			start.await();
			long next = System.nanoTime();
			while (!control.warmupStop) {
				try {
					stub.put(requests[(int) (sequence++ & (requests.length - 1))]);
				} catch (Throwable error) {
					state.warmupErrors++;
				}
				state.writes++;
				next = parkUntilNextRateSlot(next, interval);
			}
			phases.arriveAndAwaitAdvance();
			next = System.nanoTime();
			while (true) {
				long started = System.nanoTime();
				boolean error = false;
				try {
					stub.put(requests[(int) (sequence++ & (requests.length - 1))]);
				} catch (Throwable failure) {
					error = true;
				}
				long completed = System.nanoTime();
				if (completed > control.measurementDeadlineNanos) {
					break;
				}
				state.writes++;
				if (error) {
					state.errors++;
				}
				next = parkUntilNextRateSlot(Math.max(next, started), interval);
			}
		} catch (Throwable failure) {
			if (control.measurementDeadlineNanos == 0) {
				state.warmupErrors++;
			} else {
				state.errors++;
			}
			if (failure instanceof InterruptedException) {
				Thread.currentThread().interrupt();
			}
		} finally {
			phases.arriveAndDeregister();
		}
	}

	private static long parkUntilNextRateSlot(long previous, long interval) {
		long now = System.nanoTime();
		long next = previous + interval;
		if (next < now - interval) {
			next = now + interval;
		}
		long delay = next - now;
		if (delay > 0) {
			LockSupport.parkNanos(delay);
		}
		return next;
	}

	private static long awaitSteadyWarmup(Control control, ReaderState[] readers, Options options) {
		long started = System.nanoTime();
		long minimumDeadline = started + TimeUnit.MILLISECONDS.toNanos(options.warmupMillis());
		long maximumDeadline = started + TimeUnit.MILLISECONDS.toNanos(options.maxWarmupMillis());
		long previousNanos = started;
		long previousCompilationMillis = Snapshot.currentCompilationMillis();
		long previousReads = 0;
		double[] throughputWindow = new double[options.warmupQuietPeriods()];
		int steadyPeriods = 0;
		while (true) {
			long now = System.nanoTime();
			long nextCheck = Math.min(maximumDeadline, now + TimeUnit.SECONDS.toNanos(1));
			sleepUntil(nextCheck);
			now = System.nanoTime();
			long compilationMillis = Snapshot.currentCompilationMillis();
			double compilationMillisPerSecond = (compilationMillis - previousCompilationMillis)
					* 1_000_000_000.0 / Math.max(1, now - previousNanos);
			long reads = Arrays.stream(readers).mapToLong(state -> state.reads).sum();
			double throughput = (reads - previousReads) * 1_000_000_000.0 / Math.max(1, now - previousNanos);
			if (now >= minimumDeadline && compilationMillisPerSecond <= options.warmupJitMillisPerSecond()) {
				if (steadyPeriods < throughputWindow.length) {
					throughputWindow[steadyPeriods++] = throughput;
				} else {
					System.arraycopy(throughputWindow, 1, throughputWindow, 0, throughputWindow.length - 1);
					throughputWindow[throughputWindow.length - 1] = throughput;
				}
			} else {
				steadyPeriods = 0;
			}
			if (steadyPeriods == throughputWindow.length
					&& stableThroughput(throughputWindow, options.warmupThroughputTolerance())) {
				control.warmupStop = true;
				return TimeUnit.NANOSECONDS.toMillis(now - started);
			}
			if (now >= maximumDeadline) {
				control.warmupStop = true;
				throw new IllegalStateException(String.format(Locale.ROOT,
						"Benchmark did not become steady during warmup: last-jit-rate=%.1fms/s, "
								+ "last-throughput=%.0f/s, required-periods=%d, max-warmup-ms=%d",
						compilationMillisPerSecond, throughput, options.warmupQuietPeriods(),
						options.maxWarmupMillis()));
			}
			previousNanos = now;
			previousCompilationMillis = compilationMillis;
			previousReads = reads;
		}
	}

	private static boolean stableThroughput(double[] window, double tolerance) {
		double minimum = Arrays.stream(window).min().orElseThrow();
		double maximum = Arrays.stream(window).max().orElseThrow();
		return minimum > 0 && maximum <= minimum * (1 + tolerance);
	}

	private static RequestCase selectRequest(Band band, long sequence, long random) {
		if (band.missPercent() == 100) {
			return band.missing();
		}
		if (band.missPercent() != 0 && Long.remainderUnsigned(random, 100) < band.missPercent()) {
			return band.missing();
		}
		int index = (int) Long.remainderUnsigned(random ^ sequence, band.hits().length);
		return band.hits()[index];
	}

	private static long nextRandom(long value) {
		value ^= value << 13;
		value ^= value >>> 7;
		return value ^ (value << 17);
	}

	private static void validateResponse(GetResponse response, int expectedSize) {
		if (expectedSize < 0) {
			if (response.hasValue()) {
				throw new IllegalStateException("Missing key returned a value");
			}
		} else if (!response.hasValue() || response.getValue().size() != expectedSize) {
			throw new IllegalStateException("Unexpected value size: expected=" + expectedSize
					+ ", present=" + response.hasValue() + ", actual=" + response.getValue().size());
		}
	}

	private static Result aggregate(String band,
			ReaderState[] readers,
			WriterState writer,
			Options options,
			long warmupMillis,
			Snapshot before,
			Snapshot after,
			long overshoot) {
		long reads = Arrays.stream(readers).mapToLong(state -> state.reads).sum();
		long misses = Arrays.stream(readers).mapToLong(state -> state.misses).sum();
		long errors = Arrays.stream(readers).mapToLong(state -> state.errors).sum()
				+ (writer == null ? 0 : writer.errors);
		long writes = writer == null ? 0 : writer.writes;
		int samples = Arrays.stream(readers).mapToInt(state -> state.sampleCount).sum();
		long overflow = Arrays.stream(readers).mapToLong(state -> state.sampleOverflow).sum();
		long[] latencies = new long[samples];
		int offset = 0;
		for (ReaderState reader : readers) {
			System.arraycopy(reader.latencies, 0, latencies, offset, reader.sampleCount);
			offset += reader.sampleCount;
		}
		Arrays.sort(latencies);
		long elapsedNanos = TimeUnit.MILLISECONDS.toNanos(options.measureMillis());
		return new Result(options.stage(), options.round(), options.orderPosition(), options.strategy(), band,
				warmupMillis,
				reads * 1_000_000_000.0 / elapsedNanos,
				percentile(latencies, 0.50), percentile(latencies, 0.95),
				percentile(latencies, 0.99), percentile(latencies, 0.999),
				(after.cpuNanos - before.cpuNanos) * 100.0 / elapsedNanos,
				reads == 0 ? 0 : (after.allocatedBytes - before.allocatedBytes) / (double) reads,
				after.gcCollections - before.gcCollections,
				after.gcMillis - before.gcMillis,
				after.compilationMillis - before.compilationMillis,
				misses, writes, errors, samples, overflow, overshoot);
	}

	private static long percentile(long[] sorted, double percentile) {
		if (sorted.length == 0) {
			return 0;
		}
		int index = (int) Math.ceil(percentile * sorted.length) - 1;
		return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
	}

	private static void sleepUntil(long deadlineNanos) {
		while (true) {
			long remaining = deadlineNanos - System.nanoTime();
			if (remaining <= 0) {
				return;
			}
			if (remaining > 1_000_000) {
				LockSupport.parkNanos(remaining - 500_000);
			} else {
				Thread.onSpinWait();
			}
		}
	}

	private static void awaitWorkers(Phaser phases, int expectedUnarrived, long timeoutSeconds)
			throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
		while (phases.getUnarrivedParties() != expectedUnarrived) {
			if (System.nanoTime() >= deadline) {
				throw new IllegalStateException("Benchmark workers did not reach the phase boundary");
			}
			Thread.sleep(1);
		}
	}

	private static long populate(RocksDBSyncAPI api, List<BandSpec> bands) {
		long columnId = api.createColumn("benchmark",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		boolean[] populated = populatedSizes(bands);
		for (int sizeIndex = 0; sizeIndex < FIXED_SIZES.length; sizeIndex++) {
			if (!populated[sizeIndex]) {
				continue;
			}
			for (int replica = 0; replica < KEYS_PER_SIZE; replica++) {
				long key = key(sizeIndex, replica);
				api.put(0, columnId, keys(key), Buf.wrap(value(key, FIXED_SIZES[sizeIndex])), RequestType.none());
			}
		}
		api.put(0, columnId, keys(WRITER_KEY), Buf.wrap(value(WRITER_KEY, 1_024)), RequestType.none());
		api.flush();
		return columnId;
	}

	private static void preflight(RocksDBServiceGrpc.RocksDBServiceBlockingStub stub,
			long columnId,
			List<BandSpec> bands) {
		GetResponse missing = stub.get(getRequest(columnId, MISSING_KEY));
		if (missing.hasValue()) {
			throw new IllegalStateException("Preflight missing-key response was present");
		}
		boolean[] populated = populatedSizes(bands);
		for (int sizeIndex = 0; sizeIndex < FIXED_SIZES.length; sizeIndex++) {
			if (!populated[sizeIndex]) {
				continue;
			}
			for (int replica = 0; replica < KEYS_PER_SIZE; replica++) {
				long key = key(sizeIndex, replica);
				byte[] expected = value(key, FIXED_SIZES[sizeIndex]);
				GetResponse response = stub.get(getRequest(columnId, key));
				if (!response.hasValue() || !response.getValue().equals(ByteString.copyFrom(expected))) {
					throw new IllegalStateException("Preflight value mismatch for size=" + FIXED_SIZES[sizeIndex]
							+ ", replica=" + replica);
				}
			}
		}
	}

	private static boolean[] populatedSizes(List<BandSpec> bands) {
		boolean[] populated = new boolean[FIXED_SIZES.length];
		for (BandSpec band : bands) {
			for (int sizeIndex : band.sizeIndexes()) {
				populated[sizeIndex] = true;
			}
		}
		return populated;
	}

	private static Band createBand(long columnId, BandSpec spec) {
		List<RequestCase> hits = new ArrayList<>();
		for (int sizeIndex : spec.sizeIndexes()) {
			for (int replica = 0; replica < KEYS_PER_SIZE; replica++) {
				long key = key(sizeIndex, replica);
				hits.add(new RequestCase(getRequest(columnId, key), FIXED_SIZES[sizeIndex]));
			}
		}
		return new Band(spec.name(), hits.toArray(RequestCase[]::new),
				new RequestCase(getRequest(columnId, MISSING_KEY), -1), spec.missPercent(), spec.writerRate());
	}

	private static GetRequest getRequest(long columnId, long key) {
		return GetRequest.newBuilder()
				.setTransactionOrUpdateId(0)
				.setColumnId(columnId)
				.addKeys(ByteString.copyFrom(keyBytes(key)))
				.build();
	}

	private static PutRequest[] writerRequests(long columnId) {
		PutRequest[] requests = new PutRequest[256];
		ByteString key = ByteString.copyFrom(keyBytes(WRITER_KEY));
		for (int index = 0; index < requests.length; index++) {
			requests[index] = PutRequest.newBuilder()
					.setTransactionOrUpdateId(0)
					.setColumnId(columnId)
					.setData(KV.newBuilder().addKeys(key)
							.setValue(ByteString.copyFrom(value(index, 1_024))))
					.build();
		}
		return requests;
	}

	private static Keys keys(long key) {
		return new Keys(Buf.wrap(keyBytes(key)));
	}

	private static byte[] keyBytes(long key) {
		return ByteBuffer.allocate(Long.BYTES).putLong(key).array();
	}

	private static long key(int sizeIndex, int replica) {
		return ((long) (sizeIndex + 1) << 32) | Integer.toUnsignedLong(replica);
	}

	private static byte[] value(long seed, int size) {
		byte[] value = new byte[size];
		for (int index = 0; index < size; index++) {
			value[index] = (byte) (seed * 31 + index * 17);
		}
		return value;
	}

	private static List<BandSpec> selectedBands(Stage stage, String selection, int round, long writerRate) {
		List<BandSpec> available = stage == Stage.DIAGNOSTICS ? diagnosticBands(writerRate) : fixedBands();
		List<BandSpec> selected;
		if (selection.equals("all")) {
			selected = new ArrayList<>(available);
		} else {
			List<String> names = Arrays.stream(selection.split(",")).map(String::strip)
					.filter(name -> !name.isEmpty()).toList();
			selected = new ArrayList<>(available.stream().filter(band -> names.contains(band.name())).toList());
			if (selected.size() != names.size()) {
				throw new IllegalArgumentException("Unknown or duplicate benchmark band in --bands=" + selection);
			}
		}
		if (selected.size() > 1) {
			int rotation = Math.floorMod(round - 1, selected.size());
			java.util.Collections.rotate(selected, -rotation);
		}
		return selected;
	}

	private static List<BandSpec> fixedBands() {
		List<BandSpec> result = new ArrayList<>();
		for (int index = 0; index < FIXED_SIZES.length; index++) {
			result.add(new BandSpec(Integer.toString(FIXED_SIZES[index]), new int[]{index}, 0, 0));
		}
		return result;
	}

	private static List<BandSpec> diagnosticBands(long writerRate) {
		int[] small = indexesThrough(4_096);
		int[] wide = java.util.stream.IntStream.range(0, FIXED_SIZES.length).toArray();
		return List.of(
				new BandSpec("miss", new int[0], 100, 0),
				new BandSpec("mixed-small", small, 0, 0),
				new BandSpec("mixed-wide", wide, 10, 0),
				new BandSpec("mixed-wide-writes", wide, 10, writerRate));
	}

	private static int[] indexesThrough(int maximumSize) {
		return java.util.stream.IntStream.range(0, FIXED_SIZES.length)
				.filter(index -> FIXED_SIZES[index] <= maximumSize).toArray();
	}

	private static List<Strategy> strategyOrder(Stage stage, int round) {
		List<Strategy> strategies = stage == Stage.TUNING
				? List.of(Strategy.EXACT_HEAP, Strategy.PINNED)
				: List.of(Strategy.EXACT_HEAP, Strategy.PINNED, Strategy.AUTOMATIC, Strategy.LEGACY);
		if (strategies.size() == 2) {
			return round % 2 == 1 ? strategies : strategies.reversed();
		}
		List<Strategy> base = round % 2 == 1 ? strategies : strategies.reversed();
		List<Strategy> order = new ArrayList<>(base);
		java.util.Collections.rotate(order, -Math.floorMod((round - 1) / 2, order.size()));
		return order;
	}

	private static void printResult(Result result) {
		System.out.printf(Locale.ROOT,
				"round=%d position=%d strategy=%-16s band=%-18s warmup=%dms throughput=%10.0f/s "
						+ "p50=%8.1fus p95=%8.1fus p99=%8.1fus p99.9=%8.1fus "
						+ "cpu=%6.1f%% alloc=%10.1fB/op gc=%d/%dms jit=%dms "
						+ "misses=%d writes=%d errors=%d samples=%d%n",
				result.round, result.orderPosition, result.strategy.propertyValue, result.band, result.warmupActualMillis,
				result.throughput, result.p50 / 1_000.0, result.p95 / 1_000.0,
				result.p99 / 1_000.0, result.p999 / 1_000.0, result.cpuPercent,
				result.allocatedBytesPerOp, result.gcCollections, result.gcMillis, result.jitMillis,
				result.misses, result.writes, result.errors, result.samples);
	}

	private static void printSummary(List<Result> results, Stage stage, double regressionLimit) {
		List<String> bands = results.stream().map(Result::band).distinct().toList();
		System.out.println("Median summary (independent JVM forks):");
		for (String band : bands) {
			Map<Strategy, Aggregate> aggregates = aggregates(results, band);
			System.out.print("  band=" + band);
			for (Strategy strategy : Strategy.values()) {
				Aggregate aggregate = aggregates.get(strategy);
				if (aggregate != null) {
					System.out.printf(Locale.ROOT, " %s=%.0f/s,p99=%.1fus",
							strategy.propertyValue, aggregate.throughput, aggregate.p99 / 1_000.0);
				}
			}
			if (stage == Stage.TUNING) {
				Aggregate heap = aggregates.get(Strategy.EXACT_HEAP);
				Aggregate pinned = aggregates.get(Strategy.PINNED);
				boolean pinnedNotWorse = pinned.throughput >= heap.throughput * (1 - regressionLimit)
						&& pinned.p99 <= heap.p99 * (1 + regressionLimit);
				double[] throughputRatios = pairedRatios(results, band, Strategy.PINNED, Strategy.EXACT_HEAP,
						true);
				double[] p99Ratios = pairedRatios(results, band, Strategy.PINNED, Strategy.EXACT_HEAP, false);
				boolean stableThroughputAdvantage = stableAbove(throughputRatios, 1 + regressionLimit);
				boolean stableP99Advantage = stableAbove(p99Ratios, 1 + regressionLimit);
				boolean selectPinned = pinnedNotWorse && (stableThroughputAdvantage || stableP99Advantage);
				System.out.printf(Locale.ROOT, " paired-throughput=%.4f paired-p99=%.4f suggested=%s",
						median(throughputRatios), median(p99Ratios), selectPinned ? "pinned" : "heap");
			}
			System.out.println();
		}
	}

	private static void verifyAutomaticGate(List<Result> results, double regressionLimit) {
		List<String> failures = new ArrayList<>();
		for (String band : results.stream().map(Result::band).distinct().toList()) {
			double[] throughputRatios = automaticOracleRatios(results, band, true);
			double[] p99Ratios = automaticOracleRatios(results, band, false);
			double throughputRatio = median(throughputRatios);
			double p99Ratio = median(p99Ratios);
			boolean stableThroughputRegression = stableBelow(throughputRatios, 1 - regressionLimit);
			boolean stableP99Regression = stableBelow(p99Ratios, 1 / (1 + regressionLimit));
			System.out.printf(Locale.ROOT,
					"Held-out gate band=%s paired-throughput=%.4f paired-p99=%.4f "
							+ "stable-material-regression=%s/%s%n",
					band, throughputRatio, p99Ratio, stableThroughputRegression, stableP99Regression);
			if (throughputRatio < 1 - regressionLimit || p99Ratio < 1 / (1 + regressionLimit)
					|| stableThroughputRegression || stableP99Regression) {
				failures.add("band=%s throughput-ratio=%.4f p99-ratio=%.4f stable=%s/%s".formatted(
						band, throughputRatio, p99Ratio, stableThroughputRegression, stableP99Regression));
			}
		}
		long errors = results.stream().mapToLong(Result::errors).sum();
		long overflows = results.stream().mapToLong(Result::sampleOverflow).sum();
		if (errors != 0 || overflows != 0 || !failures.isEmpty()) {
			throw new IllegalStateException("Automatic fast-Get held-out gate failed: errors=" + errors
					+ ", sample-overflows=" + overflows + ", regressions=" + failures);
		}
		System.out.printf(Locale.ROOT,
				"Automatic mode passed the %.1f%% median throughput/p99 held-out gate in every band.%n",
				regressionLimit * 100);
	}

	private static double[] pairedRatios(List<Result> results,
			String band,
			Strategy numerator,
			Strategy denominator,
			boolean throughput) {
		Map<Integer, Result> numeratorByRound = byRound(results, band, numerator);
		Map<Integer, Result> denominatorByRound = byRound(results, band, denominator);
		if (!numeratorByRound.keySet().equals(denominatorByRound.keySet())) {
			throw new IllegalStateException("Unpaired benchmark rows for band=" + band);
		}
		return numeratorByRound.keySet().stream().sorted().mapToDouble(round -> {
			Result left = numeratorByRound.get(round);
			Result right = denominatorByRound.get(round);
			return throughput ? left.throughput / right.throughput : right.p99 / (double) left.p99;
		}).toArray();
	}

	private static double[] automaticOracleRatios(List<Result> results, String band, boolean throughput) {
		Map<Integer, Result> automatic = byRound(results, band, Strategy.AUTOMATIC);
		Map<Integer, Result> heap = byRound(results, band, Strategy.EXACT_HEAP);
		Map<Integer, Result> pinned = byRound(results, band, Strategy.PINNED);
		if (!automatic.keySet().equals(heap.keySet()) || !automatic.keySet().equals(pinned.keySet())) {
			throw new IllegalStateException("Unpaired validation rows for band=" + band);
		}
		return automatic.keySet().stream().sorted().mapToDouble(round -> {
			Result auto = automatic.get(round);
			Result exact = heap.get(round);
			Result pin = pinned.get(round);
			return throughput
					? auto.throughput / Math.max(exact.throughput, pin.throughput)
					: Math.min(exact.p99, pin.p99) / (double) auto.p99;
		}).toArray();
	}

	private static Map<Integer, Result> byRound(List<Result> results, String band, Strategy strategy) {
		Map<Integer, Result> byRound = new LinkedHashMap<>();
		for (Result result : results) {
			if (result.band.equals(band) && result.strategy == strategy
					&& byRound.put(result.round, result) != null) {
				throw new IllegalStateException("Duplicate row for band=" + band + ", strategy=" + strategy
						+ ", round=" + result.round);
			}
		}
		return byRound;
	}

	private static boolean stableAbove(double[] ratios, double threshold) {
		long favorable = Arrays.stream(ratios).filter(value -> value > threshold).count();
		return binomialUpperTail(ratios.length, Math.toIntExact(favorable)) <= 0.05;
	}

	private static boolean stableBelow(double[] ratios, double threshold) {
		long unfavorable = Arrays.stream(ratios).filter(value -> value < threshold).count();
		return binomialUpperTail(ratios.length, Math.toIntExact(unfavorable)) <= 0.05;
	}

	/** Exact one-sided sign-test probability under p=0.5. */
	private static double binomialUpperTail(int trials, int successes) {
		double probability = 0;
		for (int count = successes; count <= trials; count++) {
			probability += binomialCoefficient(trials, count) / Math.scalb(1.0, trials);
		}
		return probability;
	}

	private static long binomialCoefficient(int n, int k) {
		k = Math.min(k, n - k);
		long value = 1;
		for (int index = 1; index <= k; index++) {
			value = value * (n - k + index) / index;
		}
		return value;
	}

	private static Map<Strategy, Aggregate> aggregates(List<Result> results, String band) {
		Map<Strategy, List<Result>> grouped = new EnumMap<>(Strategy.class);
		for (Result result : results) {
			if (result.band.equals(band)) {
				grouped.computeIfAbsent(result.strategy, ignored -> new ArrayList<>()).add(result);
			}
		}
		Map<Strategy, Aggregate> aggregate = new EnumMap<>(Strategy.class);
		grouped.forEach((strategy, rows) -> aggregate.put(strategy, new Aggregate(
				median(rows.stream().mapToDouble(Result::throughput).toArray()),
				median(rows.stream().mapToLong(Result::p99).toArray()))));
		return aggregate;
	}

	private static double median(double[] values) {
		Arrays.sort(values);
		int middle = values.length / 2;
		return values.length % 2 == 1 ? values[middle] : (values[middle - 1] + values[middle]) / 2;
	}

	private static long median(long[] values) {
		Arrays.sort(values);
		int middle = values.length / 2;
		return values.length % 2 == 1 ? values[middle] : (values[middle - 1] + values[middle]) / 2;
	}

	private static void writeCsv(Path output, List<Result> results) throws Exception {
		Path absolute = output.toAbsolutePath().normalize();
		if (absolute.getParent() != null) {
			Files.createDirectories(absolute.getParent());
		}
		try (BufferedWriter writer = Files.newBufferedWriter(absolute,
				StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			writer.write(CSV_HEADER);
			writer.newLine();
			for (Result result : results) {
				writer.write(result.csv());
				writer.newLine();
			}
		}
		System.out.println("Wrote benchmark results to " + absolute);
	}

	private static List<Result> readCsv(Path input) throws Exception {
		List<Result> results = new ArrayList<>();
		try (BufferedReader reader = Files.newBufferedReader(input)) {
			String header = reader.readLine();
			if (!CSV_HEADER.equals(header)) {
				throw new IllegalStateException("Unexpected worker CSV header in " + input);
			}
			String line;
			while ((line = reader.readLine()) != null) {
				results.add(Result.parse(line));
			}
		}
		return results;
	}

	private static String configText() {
		return """
				database: {
				  global: {
				    enable-fast-get: true
				    ingest-behind: false
				    optimistic: false
				    use-direct-io: false
				    maximum-open-files: -1
				    fallback-column-options: { cache-index-and-filter-blocks: true }
				  }
				}
				""";
	}

	private static void writeMetadata(Path root, Options options) throws Exception {
		List<String> lines = new ArrayList<>();
		lines.add("started=" + Instant.now());
		lines.add("stage=" + options.stage().value);
		lines.add("rounds=" + options.rounds());
		lines.add("bands=" + options.bands());
		lines.add("warmup_ms=" + options.warmupMillis());
		lines.add("max_warmup_ms=" + options.maxWarmupMillis());
		lines.add("warmup_quiet_periods=" + options.warmupQuietPeriods());
		lines.add("warmup_jit_ms_per_second=" + options.warmupJitMillisPerSecond());
		lines.add("warmup_throughput_tolerance=" + options.warmupThroughputTolerance());
		lines.add("measure_ms=" + options.measureMillis());
		lines.add("readers=" + options.readers());
		lines.add("sample_every=" + options.sampleEvery());
		lines.add("max_samples_per_reader=" + options.maxSamplesPerReader());
		lines.add("max_jit_ms=" + options.maxJitMillis());
		lines.add("writer_rate=" + options.writerRate());
		lines.add("seed=" + options.seed());
		lines.add("regression_limit=" + options.regressionLimit());
		lines.add("child_jvm=-Xms" + options.childHeap() + " -Xmx" + options.childHeap()
				+ " -XX:+AlwaysPreTouch -XX:-BackgroundCompilation --enable-native-access=ALL-UNNAMED");
		lines.add("java_version=" + System.getProperty("java.version"));
		lines.add("java_vm=" + System.getProperty("java.vm.name") + " " + System.getProperty("java.vm.version"));
		lines.add("os=" + System.getProperty("os.name") + " " + System.getProperty("os.version")
				+ " " + System.getProperty("os.arch"));
		lines.add("available_processors=" + Runtime.getRuntime().availableProcessors());
		appendFirstMatchingLine(lines, Path.of("/proc/cpuinfo"), "model name", "cpu_");
		appendFirstMatchingLine(lines, Path.of("/proc/self/status"), "Cpus_allowed_list", "process_");
		Path governor = Path.of("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor");
		if (Files.isReadable(governor)) {
			lines.add("cpu0_scaling_governor=" + Files.readString(governor).strip());
		}
		Files.write(root.resolve("metadata.txt"), lines, StandardOpenOption.CREATE_NEW);
	}

	private static void appendFirstMatchingLine(List<String> output,
			Path source,
			String prefix,
			String outputPrefix) throws Exception {
		if (!Files.isReadable(source)) {
			return;
		}
		try (var lines = Files.lines(source)) {
			lines.filter(line -> line.startsWith(prefix)).findFirst()
					.ifPresent(line -> output.add(outputPrefix + line.replace(':', '=').replace(' ', '_')));
		}
	}

	private static void printUsage() {
		System.out.println("""
				Compile and create a reusable direct-launch classpath:
				  mvn -q -DskipTests test-compile dependency:build-classpath \\
				    -Dmdep.outputFile=target/fast-get.classpath

				Run tuning in isolated JVM forks:
				  java --enable-native-access=ALL-UNNAMED \\
				    -cp "target/test-classes:target/classes:$(<target/fast-get.classpath)" \\
				    it.cavallium.rockserver.core.impl.benchmark.GrpcFastGetBenchmark \\
				    --stage=tuning --rounds=5 --warmup-ms=20000 --measure-ms=5000

				Run held-out validation only after applying the tuning selector:
				  use the same command with --stage=validation and a new --root path

				Important options use --name=value:
				  stage=tuning|validation|diagnostics  root=<fresh-path>  output=<csv>
				  rounds=5  bands=all  warmup-ms=20000  max-warmup-ms=60000
				  warmup-quiet-periods=3  warmup-jit-ms-per-second=100
				  warmup-throughput-tolerance=0.10  measure-ms=5000  readers=4
				  writer-rate=500  sample-every=8  max-samples-per-reader=1000000
				  max-jit-ms=1000  child-heap=1g  regression-limit=0.03  smoke=false
				""");
	}

	private enum Stage {
		TUNING("tuning"), VALIDATION("validation"), DIAGNOSTICS("diagnostics");

		private final String value;

		Stage(String value) {
			this.value = value;
		}

		private static Stage parse(String value) {
			return Arrays.stream(values()).filter(stage -> stage.value.equals(value)).findFirst()
					.orElseThrow(() -> new IllegalArgumentException("Unknown stage: " + value));
		}
	}

	private enum Strategy {
		LEGACY("legacy"), EXACT_HEAP("exact-heap"), PINNED("pinned"), AUTOMATIC("automatic");

		private final String propertyValue;

		Strategy(String propertyValue) {
			this.propertyValue = propertyValue;
		}

		private static Strategy parse(String value) {
			return Arrays.stream(values()).filter(strategy -> strategy.propertyValue.equals(value)).findFirst()
					.orElseThrow(() -> new IllegalArgumentException("Unknown strategy: " + value));
		}
	}

	private record BandSpec(String name, int[] sizeIndexes, int missPercent, long writerRate) {
	}

	private record Band(String name, RequestCase[] hits, RequestCase missing, int missPercent, long writerRate) {
	}

	private record RequestCase(GetRequest request, int expectedSize) {
	}

	private static final class Control {
		private volatile boolean warmupStop;
		private volatile long measurementDeadlineNanos;
	}

	private static final class ReaderState {
		private final long[] latencies;
		private int sampleCount;
		private long sampleOverflow;
		private volatile long reads;
		private long misses;
		private long errors;
		private long warmupErrors;

		private ReaderState(int maxSamples) {
			this.latencies = new long[maxSamples];
		}

		private void recordLatency(long latency) {
			if (sampleCount < latencies.length) {
				latencies[sampleCount++] = latency;
			} else {
				sampleOverflow++;
			}
		}

		private void resetMeasurements() {
			sampleCount = 0;
			sampleOverflow = 0;
			reads = 0;
			misses = 0;
			errors = 0;
		}
	}

	private static final class WriterState {
		private long writes;
		private long errors;
		private long warmupErrors;

		private void resetMeasurements() {
			writes = 0;
			errors = 0;
		}
	}

	private record Client(ManagedChannel channel,
			RocksDBServiceGrpc.RocksDBServiceBlockingStub stub) implements AutoCloseable {

		private static Client open(int port) {
			ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", port)
					.directExecutor()
					.usePlaintext()
					.disableRetry()
					.maxInboundMessageSize(64 * 1024 * 1024)
					.build();
			return new Client(channel, RocksDBServiceGrpc.newBlockingStub(channel));
		}

		@Override
		public void close() throws InterruptedException {
			channel.shutdownNow();
			if (!channel.awaitTermination(10, TimeUnit.SECONDS)) {
				throw new IllegalStateException("gRPC client did not terminate");
			}
		}
	}

	private record Snapshot(long wallNanos,
			long cpuNanos,
			long allocatedBytes,
			long gcCollections,
			long gcMillis,
			long compilationMillis) {

		private static void enableAllocationMeasurement() {
			ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
			if (threads.isThreadAllocatedMemorySupported() && !threads.isThreadAllocatedMemoryEnabled()) {
				threads.setThreadAllocatedMemoryEnabled(true);
			}
		}

		private static Snapshot capture() {
			OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
			ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
			long collections = 0;
			long millis = 0;
			for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
				collections += Math.max(0, gc.getCollectionCount());
				millis += Math.max(0, gc.getCollectionTime());
			}
			return new Snapshot(System.nanoTime(), os.getProcessCpuTime(),
					threads.getTotalThreadAllocatedBytes(), collections, millis, currentCompilationMillis());
		}

		private static long currentCompilationMillis() {
			var compiler = ManagementFactory.getCompilationMXBean();
			return compiler != null && compiler.isCompilationTimeMonitoringSupported()
					? compiler.getTotalCompilationTime() : 0;
		}
	}

	private record Aggregate(double throughput, long p99) {
	}

	private record Result(Stage stage,
			int round,
			int orderPosition,
			Strategy strategy,
			String band,
			long warmupActualMillis,
			double throughput,
			long p50,
			long p95,
			long p99,
			long p999,
			double cpuPercent,
			double allocatedBytesPerOp,
			long gcCollections,
			long gcMillis,
			long jitMillis,
			long misses,
			long writes,
			long errors,
			long samples,
			long sampleOverflow,
			long measurementOvershootNanos) {

		private String csv() {
			return String.format(Locale.ROOT,
					"%s,%d,%d,%s,%s,%d,%.3f,%d,%d,%d,%d,%.3f,%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d",
					stage.value, round, orderPosition, strategy.propertyValue, band, warmupActualMillis, throughput,
					p50, p95, p99, p999, cpuPercent, allocatedBytesPerOp, gcCollections, gcMillis,
					jitMillis, misses, writes, errors, samples, sampleOverflow, measurementOvershootNanos);
		}

		private static Result parse(String line) {
			String[] fields = line.split(",", -1);
			if (fields.length != 22) {
				throw new IllegalArgumentException("Invalid benchmark CSV row: " + line);
			}
			return new Result(Stage.parse(fields[0]), Integer.parseInt(fields[1]), Integer.parseInt(fields[2]),
					Strategy.parse(fields[3]), fields[4], Long.parseLong(fields[5]), Double.parseDouble(fields[6]),
					Long.parseLong(fields[7]), Long.parseLong(fields[8]), Long.parseLong(fields[9]),
					Long.parseLong(fields[10]), Double.parseDouble(fields[11]), Double.parseDouble(fields[12]),
					Long.parseLong(fields[13]), Long.parseLong(fields[14]), Long.parseLong(fields[15]),
					Long.parseLong(fields[16]), Long.parseLong(fields[17]), Long.parseLong(fields[18]),
					Long.parseLong(fields[19]), Long.parseLong(fields[20]), Long.parseLong(fields[21]));
		}
	}

	private record Options(boolean worker,
			Stage stage,
			Path root,
			Path output,
			String bands,
			int rounds,
			int round,
			int orderPosition,
			Strategy strategy,
			long warmupMillis,
			long maxWarmupMillis,
			int warmupQuietPeriods,
			double warmupJitMillisPerSecond,
			double warmupThroughputTolerance,
			long measureMillis,
			int readers,
			long writerRate,
			int sampleEvery,
			int maxSamplesPerReader,
			long maxJitMillis,
			long seed,
			double regressionLimit,
			String childHeap,
			boolean enforceGate,
			boolean smoke) {

		private static Options parse(String[] args) {
			Map<String, String> values = new LinkedHashMap<>();
			Set<String> known = Set.of("worker", "stage", "root", "output", "bands", "rounds", "round",
					"order-position", "strategy", "warmup-ms", "max-warmup-ms", "warmup-quiet-periods",
					"warmup-jit-ms-per-second", "warmup-throughput-tolerance", "measure-ms", "readers", "writer-rate",
					"sample-every", "max-samples-per-reader", "max-jit-ms", "seed", "regression-limit", "child-heap",
					"enforce-gate", "smoke");
			for (String argument : args) {
				if (!argument.startsWith("--") || !argument.contains("=")) {
					throw new IllegalArgumentException("Expected --name=value, found: " + argument);
				}
				int equals = argument.indexOf('=');
				String name = argument.substring(2, equals);
				if (!known.contains(name)) {
					throw new IllegalArgumentException("Unknown option: " + name);
				}
				String previous = values.put(name, argument.substring(equals + 1));
				if (previous != null) {
					throw new IllegalArgumentException("Duplicate option: " + name);
				}
			}
			boolean worker = bool(values, "worker", false);
			Stage stage = Stage.parse(values.getOrDefault("stage", "tuning"));
			Path root = values.containsKey("root") ? Path.of(values.get("root"))
					: Path.of("target", "grpc-fast-get-" + stage.value + "-" + Instant.now().toEpochMilli());
			Path output = values.containsKey("output") ? Path.of(values.get("output")) : root.resolve("results.csv");
			Strategy strategy = Strategy.parse(values.getOrDefault("strategy", "exact-heap"));
			Options options = new Options(worker, stage, root, output,
					values.getOrDefault("bands", "all"), integer(values, "rounds", 5),
					integer(values, "round", 1), integer(values, "order-position", 1), strategy,
					longValue(values, "warmup-ms", 20_000), longValue(values, "max-warmup-ms", 60_000),
					integer(values, "warmup-quiet-periods", 3),
					doubleValue(values, "warmup-jit-ms-per-second", 100),
					doubleValue(values, "warmup-throughput-tolerance", 0.10), longValue(values, "measure-ms", 5_000),
					integer(values, "readers", 4), longValue(values, "writer-rate", 500),
					integer(values, "sample-every", 8), integer(values, "max-samples-per-reader", 1_000_000),
					longValue(values, "max-jit-ms", 1_000),
					longValue(values, "seed", 6_840_227_782_638_526_189L),
					doubleValue(values, "regression-limit", 0.03), values.getOrDefault("child-heap", "1g"),
					bool(values, "enforce-gate", true), bool(values, "smoke", false));
			if (options.bands.isBlank() || options.rounds <= 0 || options.round <= 0
					|| options.orderPosition <= 0 || options.warmupMillis < 0
					|| options.maxWarmupMillis < options.warmupMillis + options.warmupQuietPeriods * 1_000L
					|| options.warmupQuietPeriods <= 0 || options.warmupJitMillisPerSecond < 0
					|| options.warmupThroughputTolerance <= 0 || options.warmupThroughputTolerance >= 1
					|| options.measureMillis <= 0
					|| options.readers <= 0 || options.writerRate < 0 || options.sampleEvery <= 0
					|| options.maxSamplesPerReader <= 0 || options.maxJitMillis < 0 || options.regressionLimit < 0
					|| options.regressionLimit >= 1 || !options.childHeap.matches("[1-9][0-9]*[kKmMgG]?")) {
				throw new IllegalArgumentException("Invalid benchmark options");
			}
			if (!worker && !options.smoke && options.rounds < 5) {
				throw new IllegalArgumentException("A release benchmark requires at least five forks; "
						+ "use --smoke=true only for harness validation");
			}
			return options;
		}

		private static int integer(Map<String, String> values, String name, int defaultValue) {
			return Integer.parseInt(values.getOrDefault(name, Integer.toString(defaultValue)));
		}

		private static long longValue(Map<String, String> values, String name, long defaultValue) {
			return Long.parseLong(values.getOrDefault(name, Long.toString(defaultValue)));
		}

		private static double doubleValue(Map<String, String> values, String name, double defaultValue) {
			return Double.parseDouble(values.getOrDefault(name, Double.toString(defaultValue)));
		}

		private static boolean bool(Map<String, String> values, String name, boolean defaultValue) {
			String value = values.getOrDefault(name, Boolean.toString(defaultValue));
			if (!value.equals("true") && !value.equals("false")) {
				throw new IllegalArgumentException("Option --" + name + " must be true or false, found: " + value);
			}
			return Boolean.parseBoolean(value);
		}
	}
}
