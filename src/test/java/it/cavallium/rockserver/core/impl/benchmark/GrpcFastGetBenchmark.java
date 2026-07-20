package it.cavallium.rockserver.core.impl.benchmark;

import com.sun.management.OperatingSystemMXBean;
import com.sun.management.ThreadMXBean;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.BufferedWriter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Opt-in macrobenchmark for the real RocksDB to local-gRPC unary Get path.
 *
 * <p>The default run performs five alternating rounds for legacy protobuf,
 * exact heap, pinned streaming, and automatic selection. It includes every
 * correctness size, misses, two mixed distributions, concurrent readers, and
 * a rate-limited gRPC writer. Results are written as CSV and the process exits
 * unsuccessfully unless automatic mode is within three percent of the best
 * safe strategy for median throughput and median p99 latency in every band.
 */
public final class GrpcFastGetBenchmark {

	private static final int[] FIXED_SIZES = {
			0, 1, 32, 256, 1_024, 4_096, 65_536, 2 * 1_024 * 1_024
	};
	private static final String STRATEGY_PROPERTY = "rockserver.grpc.fast-get.strategy";
	private static final long MISSING_KEY = Long.MAX_VALUE;
	private static final long WRITER_KEY = Long.MAX_VALUE - 1;
	private static final Keys[] FIXED_KEYS = createFixedKeys();
	private static final Keys MISSING_KEYS = createKeys(MISSING_KEY);
	private static final Keys WRITER_KEYS = createKeys(WRITER_KEY);

	private GrpcFastGetBenchmark() {
	}

	public static void main(String[] args) throws Exception {
		if (Arrays.asList(args).contains("--help")) {
			printUsage();
			return;
		}
		Options options = Options.parse(args);
		System.setProperty("rockserver.core.print-config", "false");
		String previousStrategy = System.getProperty(STRATEGY_PROPERTY);
		Path root = options.root().toAbsolutePath().normalize();
		Files.createDirectories(root);
		Path config = root.resolve("rockserver.conf");
		Files.writeString(config, configText(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

		List<Band> bands = selectedBands(options.bands());
		List<Result> results = new ArrayList<>();
		try (var embedded = new EmbeddedConnection(root.resolve("db"), "grpc-fast-get-benchmark", config)) {
			long columnId = populate(embedded.getSyncApi());
			for (int round = 1; round <= options.rounds(); round++) {
				List<Strategy> order = strategyOrder(round);
				for (Band band : bands) {
					for (Strategy strategy : order) {
						System.setProperty(STRATEGY_PROPERTY, strategy.propertyValue);
						try (var server = new GrpcServer(embedded,
								new java.net.InetSocketAddress("127.0.0.1", 0))) {
							server.start();
							try (var client = GrpcConnection.forHostAndPort("grpc-fast-get-benchmark",
									new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
								runPhase(client.getSyncApi(), columnId, band, options, false,
										options.warmupMillis(), round);
								Result result = runPhase(client.getSyncApi(), columnId, band, options, true,
										options.measureMillis(), round);
								result = result.with(round, strategy);
								results.add(result);
								printResult(result);
							}
						}
					}
				}
			}
			if (embedded.getInternalDB().getPendingOpsCount() != 0) {
				throw new IllegalStateException("Benchmark leaked database operations: "
						+ embedded.getInternalDB().getPendingOpsCount());
			}
		} finally {
			restoreProperty(STRATEGY_PROPERTY, previousStrategy);
		}

		writeCsv(options.output(), results);
		verifyAutomaticGate(results, bands, options.regressionLimit());
	}

	private static Result runPhase(RocksDBSyncAPI api,
			long columnId,
			Band band,
			Options options,
			boolean measured,
			long durationMillis,
			int round) throws Exception {
		if (durationMillis == 0) {
			return Result.empty(band.name);
		}
		var metrics = new Metrics(measured, options.sampleEvery());
		var stop = new AtomicBoolean();
		int workers = options.readers() + (options.writerRate() > 0 ? 1 : 0);
		var ready = new CountDownLatch(workers);
		var start = new CountDownLatch(1);
		try (var executor = Executors.newFixedThreadPool(workers,
				Thread.ofPlatform().name("grpc-fast-get-bench-", 0).factory())) {
			for (int reader = 0; reader < options.readers(); reader++) {
				long seed = options.seed() + round * 10_000L + reader;
				executor.submit(() -> runReader(api, columnId, band, metrics, stop, ready, start, seed));
			}
			if (options.writerRate() > 0) {
				executor.submit(() -> runWriter(api, columnId, metrics, stop, ready, start, options.writerRate()));
			}
			if (!ready.await(30, TimeUnit.SECONDS)) {
				throw new IllegalStateException("Benchmark workers did not become ready");
			}

			Snapshot before = Snapshot.capture();
			long startNanos = System.nanoTime();
			start.countDown();
			Thread.sleep(durationMillis);
			stop.set(true);
			executor.shutdown();
			if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
				executor.shutdownNow();
				throw new IllegalStateException("Benchmark workers did not stop");
			}
			long elapsedNanos = System.nanoTime() - startNanos;
			Snapshot after = Snapshot.capture();
			if (!measured) {
				return Result.empty(band.name);
			}
			return metrics.result(band.name, elapsedNanos, before, after);
		}
	}

	private static void runReader(RocksDBSyncAPI api,
			long columnId,
			Band band,
			Metrics metrics,
			AtomicBoolean stop,
			CountDownLatch ready,
			CountDownLatch start,
			long seed) {
		long reads = 0;
		long misses = 0;
		long errors = 0;
		long sampleIndex = 0;
		long[] latencies = metrics.measured ? new long[2_048] : new long[0];
		int latencyCount = 0;
		try {
			var random = new SplittableRandom(seed);
			ready.countDown();
			start.await();
			while (!stop.get()) {
				boolean miss = random.nextInt(100) < 10;
				long key = miss ? MISSING_KEY : band.keys[random.nextInt(band.keys.length)];
				long started = System.nanoTime();
				try {
					Buf value = api.get(0, columnId, keys(key), RequestType.current());
					if (miss ? value != null : value == null || value.size() != sizeForKey(key)) {
						errors++;
					}
				} catch (Throwable error) {
					errors++;
				}
				long latency = System.nanoTime() - started;
				reads++;
				if (miss) {
					misses++;
				}
				if (metrics.measured && sampleIndex++ % metrics.sampleEvery == 0) {
					if (latencyCount == latencies.length) {
						latencies = Arrays.copyOf(latencies, latencies.length * 2);
					}
					latencies[latencyCount++] = latency;
				}
			}
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			errors++;
		} finally {
			metrics.recordReader(reads, misses, errors, latencies, latencyCount);
		}
	}

	private static void runWriter(RocksDBSyncAPI api,
			long columnId,
			Metrics metrics,
			AtomicBoolean stop,
			CountDownLatch ready,
			CountDownLatch start,
			long writerRate) {
		try {
			long interval = Math.max(1L, TimeUnit.SECONDS.toNanos(1) / writerRate);
			long next = System.nanoTime();
			long sequence = 0;
			ready.countDown();
			start.await();
			while (!stop.get()) {
				try {
					api.put(0, columnId, keys(WRITER_KEY), Buf.wrap(value(sequence++, 1_024)), RequestType.none());
					metrics.writes.increment();
				} catch (Throwable error) {
					metrics.errors.increment();
				}
				next += interval;
				long delay = next - System.nanoTime();
				if (delay > 0) {
					LockSupport.parkNanos(delay);
				} else if (delay < -TimeUnit.SECONDS.toNanos(1)) {
					next = System.nanoTime();
				}
			}
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			metrics.errors.increment();
		}
	}

	private static long populate(RocksDBSyncAPI api) {
		long columnId = api.createColumn("benchmark",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		for (int index = 0; index < FIXED_SIZES.length; index++) {
			api.put(0, columnId, keys(index), Buf.wrap(value(index, FIXED_SIZES[index])), RequestType.none());
		}
		api.put(0, columnId, keys(WRITER_KEY), Buf.wrap(value(0, 1_024)), RequestType.none());
		api.flush();
		return columnId;
	}

	private static List<Band> bands() {
		List<Band> result = new ArrayList<>();
		for (int index = 0; index < FIXED_SIZES.length; index++) {
			result.add(new Band(Integer.toString(FIXED_SIZES[index]), new long[]{index}));
		}
		result.add(new Band("mixed-small", new long[]{0, 1, 2, 3, 4, 5}));
		result.add(new Band("mixed-wide", new long[]{0, 1, 2, 3, 4, 5, 6, 7}));
		return result;
	}

	private static List<Band> selectedBands(String selection) {
		List<Band> available = bands();
		if (selection.equals("all")) {
			return available;
		}
		List<String> selected = Arrays.stream(selection.split(","))
				.map(String::strip)
				.filter(name -> !name.isEmpty())
				.toList();
		List<Band> result = available.stream().filter(band -> selected.contains(band.name)).toList();
		if (result.size() != selected.size()) {
			throw new IllegalArgumentException("Unknown or duplicate benchmark band in --bands=" + selection);
		}
		return result;
	}

	private static int sizeForKey(long key) {
		if (key == WRITER_KEY) {
			return 1_024;
		}
		return FIXED_SIZES[Math.toIntExact(key)];
	}

	private static Keys keys(long key) {
		if (key == MISSING_KEY) {
			return MISSING_KEYS;
		}
		if (key == WRITER_KEY) {
			return WRITER_KEYS;
		}
		return FIXED_KEYS[Math.toIntExact(key)];
	}

	private static Keys[] createFixedKeys() {
		Keys[] keys = new Keys[FIXED_SIZES.length];
		for (int index = 0; index < keys.length; index++) {
			keys[index] = createKeys(index);
		}
		return keys;
	}

	private static Keys createKeys(long key) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(key).array()));
	}

	private static byte[] value(long seed, int size) {
		byte[] value = new byte[size];
		for (int index = 0; index < size; index++) {
			value[index] = (byte) (seed * 31 + index * 17);
		}
		return value;
	}

	private static List<Strategy> strategyOrder(int round) {
		return round % 2 == 1
				? List.of(Strategy.LEGACY, Strategy.EXACT_HEAP, Strategy.PINNED, Strategy.AUTOMATIC)
				: List.of(Strategy.AUTOMATIC, Strategy.PINNED, Strategy.EXACT_HEAP, Strategy.LEGACY);
	}

	private static void printResult(Result result) {
		System.out.printf(Locale.ROOT,
				"round=%d strategy=%-9s band=%-11s throughput=%10.0f/s "
						+ "p50=%8.1fus p95=%8.1fus p99=%8.1fus p99.9=%8.1fus "
						+ "cpu=%6.1f%% alloc=%9.1fB/op gc=%d/%dms misses=%d writes=%d errors=%d%n",
				result.round,
				result.strategy.propertyValue,
				result.band,
				result.throughput,
				result.p50 / 1_000.0,
				result.p95 / 1_000.0,
				result.p99 / 1_000.0,
				result.p999 / 1_000.0,
				result.cpuPercent,
				result.allocatedBytesPerOp,
				result.gcCollections,
				result.gcMillis,
				result.misses,
				result.writes,
				result.errors);
	}

	private static void writeCsv(Path output, List<Result> results) throws Exception {
		Path absolute = output.toAbsolutePath().normalize();
		if (absolute.getParent() != null) {
			Files.createDirectories(absolute.getParent());
		}
		try (BufferedWriter writer = Files.newBufferedWriter(absolute,
				StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
			writer.write("round,strategy,band,throughput,p50_ns,p95_ns,p99_ns,p999_ns,cpu_percent,"
					+ "allocated_bytes_per_op,gc_collections,gc_millis,misses,writes,errors\n");
			for (Result result : results) {
				writer.write(String.format(Locale.ROOT,
						"%d,%s,%s,%.3f,%d,%d,%d,%d,%.3f,%.3f,%d,%d,%d,%d,%d%n",
						result.round, result.strategy.propertyValue, result.band, result.throughput,
						result.p50, result.p95, result.p99, result.p999, result.cpuPercent,
						result.allocatedBytesPerOp, result.gcCollections, result.gcMillis,
						result.misses, result.writes, result.errors));
			}
		}
		System.out.println("Wrote benchmark results to " + absolute);
	}

	private static void verifyAutomaticGate(List<Result> results,
			List<Band> bands,
			double regressionLimit) {
		List<String> failures = new ArrayList<>();
		for (Band band : bands) {
			Map<Strategy, List<Result>> byStrategy = new EnumMap<>(Strategy.class);
			for (Result result : results) {
				if (result.band.equals(band.name)) {
					byStrategy.computeIfAbsent(result.strategy, ignored -> new ArrayList<>()).add(result);
				}
			}
			Strategy winner = Arrays.stream(Strategy.values())
					.filter(strategy -> strategy != Strategy.AUTOMATIC)
					.max(java.util.Comparator.comparingDouble(strategy -> medianDouble(byStrategy.get(strategy)
							.stream().mapToDouble(result -> result.throughput).toArray())))
					.orElseThrow();
			Map<Integer, Result> winnerByRound = new LinkedHashMap<>();
			for (Result result : byStrategy.get(winner)) {
				winnerByRound.put(result.round, result);
			}
			List<Result> automatic = byStrategy.get(Strategy.AUTOMATIC);
			double throughputRatio = medianDouble(automatic.stream().mapToDouble(result -> {
				Result safe = Objects.requireNonNull(winnerByRound.get(result.round));
				return result.throughput / safe.throughput;
			}).toArray());
			double p99Ratio = medianDouble(automatic.stream().mapToDouble(result -> {
				Result safe = Objects.requireNonNull(winnerByRound.get(result.round));
				return safe.p99 / (double) result.p99;
			}).toArray());
			if (throughputRatio < 1.0 - regressionLimit
					|| p99Ratio < 1.0 / (1.0 + regressionLimit)) {
				failures.add("band=%s winner=%s throughput-ratio=%.4f p99-ratio=%.4f".formatted(
						band.name, winner.propertyValue, throughputRatio, p99Ratio));
			}
		}
		long errors = results.stream().mapToLong(result -> result.errors).sum();
		if (errors != 0 || !failures.isEmpty()) {
			throw new IllegalStateException("Automatic fast-Get release gate failed: errors=" + errors
					+ ", regressions=" + failures);
		}
		System.out.printf(Locale.ROOT,
				"Automatic mode passed the %.1f%% median throughput/p99 release gate in every band.%n",
				regressionLimit * 100.0);
	}

	private static double medianDouble(double[] values) {
		Arrays.sort(values);
		return values[values.length / 2];
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

	private static void printUsage() {
		System.out.println("""
				Run with:
				  mvn -DskipTests test-compile org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
				    -Dexec.classpathScope=test \
				    -Dexec.mainClass=it.cavallium.rockserver.core.impl.benchmark.GrpcFastGetBenchmark \
				    -Dexec.args="--rounds=5 --warmup-ms=500 --measure-ms=2000"

				Options use --name=value:
				  root=target/grpc-fast-get-benchmark  output=target/grpc-fast-get-benchmark.csv
				  rounds=5  warmup-ms=500  measure-ms=2000  readers=4  writer-rate=500
				  sample-every=8  seed=6840227782638526189  regression-limit=0.03
				""");
	}

	private static void restoreProperty(String name, String value) {
		if (value == null) {
			System.clearProperty(name);
		} else {
			System.setProperty(name, value);
		}
	}

	private enum Strategy {
		LEGACY("legacy"),
		EXACT_HEAP("exact-heap"),
		PINNED("pinned-streaming"),
		AUTOMATIC("automatic");

		private final String propertyValue;

		Strategy(String propertyValue) {
			this.propertyValue = propertyValue;
		}
	}

	private record Band(String name, long[] keys) {
	}

	private static final class Metrics {

		private final boolean measured;
		private final int sampleEvery;
		private final LongAdder reads = new LongAdder();
		private final LongAdder misses = new LongAdder();
		private final LongAdder writes = new LongAdder();
		private final LongAdder errors = new LongAdder();
		private final ConcurrentLinkedQueue<long[]> latencyBatches = new ConcurrentLinkedQueue<>();

		private Metrics(boolean measured, int sampleEvery) {
			this.measured = measured;
			this.sampleEvery = sampleEvery;
		}

		private void recordReader(long readCount,
				long missCount,
				long errorCount,
				long[] latencies,
				int latencyCount) {
			reads.add(readCount);
			misses.add(missCount);
			errors.add(errorCount);
			if (latencyCount != 0) {
				latencyBatches.add(Arrays.copyOf(latencies, latencyCount));
			}
		}

		private Result result(String band, long elapsedNanos, Snapshot before, Snapshot after) {
			int sampleCount = latencyBatches.stream().mapToInt(batch -> batch.length).sum();
			long[] sorted = new long[sampleCount];
			int offset = 0;
			for (long[] batch : latencyBatches) {
				System.arraycopy(batch, 0, sorted, offset, batch.length);
				offset += batch.length;
			}
			Arrays.sort(sorted);
			long readCount = reads.sum();
			double seconds = elapsedNanos / 1_000_000_000.0;
			return new Result(0,
					null,
					band,
					readCount / seconds,
					percentile(sorted, 0.50),
					percentile(sorted, 0.95),
					percentile(sorted, 0.99),
					percentile(sorted, 0.999),
					(after.cpuNanos - before.cpuNanos) * 100.0 / elapsedNanos,
					readCount == 0 ? 0 : (after.allocatedBytes - before.allocatedBytes) / (double) readCount,
					after.gcCollections - before.gcCollections,
					after.gcMillis - before.gcMillis,
					misses.sum(),
					writes.sum(),
					errors.sum());
		}

		private static long percentile(long[] sorted, double percentile) {
			if (sorted.length == 0) {
				return 0;
			}
			int index = (int) Math.ceil(percentile * sorted.length) - 1;
			return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
		}
	}

	private record Snapshot(long cpuNanos, long allocatedBytes, long gcCollections, long gcMillis) {

		private static Snapshot capture() {
			OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
			ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
			if (threads.isThreadAllocatedMemorySupported() && !threads.isThreadAllocatedMemoryEnabled()) {
				threads.setThreadAllocatedMemoryEnabled(true);
			}
			long collections = 0;
			long millis = 0;
			for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
				collections += Math.max(0, gc.getCollectionCount());
				millis += Math.max(0, gc.getCollectionTime());
			}
			return new Snapshot(os.getProcessCpuTime(), threads.getTotalThreadAllocatedBytes(), collections, millis);
		}
	}

	private record Result(int round,
			Strategy strategy,
			String band,
			double throughput,
			long p50,
			long p95,
			long p99,
			long p999,
			double cpuPercent,
			double allocatedBytesPerOp,
			long gcCollections,
			long gcMillis,
			long misses,
			long writes,
			long errors) {

		private static Result empty(String band) {
			return new Result(0, null, band, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
		}

		private Result with(int round, Strategy strategy) {
			return new Result(round, strategy, band, throughput, p50, p95, p99, p999,
					cpuPercent, allocatedBytesPerOp, gcCollections, gcMillis, misses, writes, errors);
		}
	}

	private record Options(Path root,
			Path output,
			String bands,
			int rounds,
			long warmupMillis,
			long measureMillis,
			int readers,
			long writerRate,
			int sampleEvery,
			long seed,
			double regressionLimit) {

		private static Options parse(String[] args) {
			Map<String, String> values = new LinkedHashMap<>();
			for (String argument : args) {
				if (!argument.startsWith("--") || !argument.contains("=")) {
					throw new IllegalArgumentException("Expected --name=value, found: " + argument);
				}
				int equals = argument.indexOf('=');
				values.put(argument.substring(2, equals), argument.substring(equals + 1));
			}
			var options = new Options(
					Path.of(values.getOrDefault("root", "target/grpc-fast-get-benchmark")),
					Path.of(values.getOrDefault("output", "target/grpc-fast-get-benchmark.csv")),
					values.getOrDefault("bands", "all"),
					integer(values, "rounds", 5),
					longValue(values, "warmup-ms", 500),
					longValue(values, "measure-ms", 2_000),
					integer(values, "readers", 4),
					longValue(values, "writer-rate", 500),
					integer(values, "sample-every", 8),
					longValue(values, "seed", 6_840_227_782_638_526_189L),
					doubleValue(values, "regression-limit", 0.03));
			if (options.bands.isBlank() || options.rounds < 5 || options.warmupMillis < 0 || options.measureMillis <= 0
					|| options.readers <= 0 || options.writerRate < 0 || options.sampleEvery <= 0
					|| options.regressionLimit < 0 || options.regressionLimit >= 1) {
				throw new IllegalArgumentException("Invalid benchmark options; rounds must be at least five");
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
	}
}
