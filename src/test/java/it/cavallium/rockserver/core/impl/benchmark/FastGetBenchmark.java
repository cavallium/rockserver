package it.cavallium.rockserver.core.impl.benchmark;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.FirstAndLast;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Opt-in, disk-backed macrobenchmark for {@code database.global.enable-fast-get}.
 *
 * <p>This is deliberately not a JUnit test: the realistic workload may create a large database and run for minutes.
 * Compile test sources, then launch it through {@code exec-maven-plugin} as documented by {@link #printUsage()}.
 */
public final class FastGetBenchmark {

	private static final long VALUE_MAGIC = 0x6A09E667F3BCC909L;
	private static final long MISSING_KEY_BASE = 1L << 60;
	private static final long PHASE_SHUTDOWN_TIMEOUT_SECONDS = 60;
	private static final long PHASE_READY_TIMEOUT_SECONDS = 30;
	private static final String RUN_MARKER_FILE = ".rockserver-fast-get-benchmark";

	private FastGetBenchmark() {
	}

	public static void main(String[] args) throws Exception {
		if (Arrays.asList(args).contains("--help")) {
			printUsage();
			return;
		}

		var options = Options.parse(args);
		System.setProperty("rockserver.core.print-config", "false");
		var runDirectory = createRunDirectory(options.root());

		System.out.printf(Locale.ROOT,
				"FastGet benchmark: root=%s run=%s hotKeys=%d writerKeys=%d valueBytes=%d readers=%d writers=%d "
						+ "writerRate=%d/s warmup=%ss measure=%ss rounds=%d miss=%d%% mutableReads=%d%%%n",
				options.root().toAbsolutePath().normalize(),
				runDirectory.path(),
				options.hotKeys(),
				options.writerKeys(),
				options.valueBytes(),
				options.readers(),
				options.writers(),
				options.writerOpsPerSecond(),
				options.warmupSeconds(),
				options.measureSeconds(),
				options.rounds(),
				options.missPercent(),
				options.mutableReadPercent());
		System.out.printf(Locale.ROOT,
				"Storage: blockCache=%s writeBufferManager=%s writeBuffer=%s directIo=%s compactPreload=%s%n",
				options.blockCache(),
				options.writeBufferManager(),
				options.writeBufferSize(),
				options.directIo(),
				options.compactPreload());

		var results = new ArrayList<ScenarioResult>();
		try {
			for (int round = 1; round <= options.rounds(); round++) {
				for (var scanMode : options.scanModes()) {
					for (boolean fastGet : options.fastGetModesForRound(round)) {
						results.add(runScenario(options, runDirectory.path(), round, fastGet, scanMode));
					}
				}
			}
		} finally {
			if (!options.keepData()) {
				deleteRunDirectory(runDirectory);
			} else {
				System.out.println("Benchmark data retained at " + runDirectory.path());
			}
		}

		printResults(results);
		failOnInvalidResults(results);
	}

	private static ScenarioResult runScenario(Options options,
			Path runRoot,
			int round,
			boolean fastGet,
			ScanMode scanMode)
			throws Exception {
		String scenarioName = "round-%02d-fast-%s-scan-%s".formatted(round, fastGet, scanMode.cliName);
		Path scenarioRoot = runRoot.resolve(scenarioName).normalize();
		if (!scenarioRoot.startsWith(runRoot)) {
			throw new IllegalArgumentException("Scenario path escaped benchmark root: " + scenarioRoot);
		}
		Files.createDirectory(scenarioRoot);
		Path config = writeConfig(scenarioRoot, scenarioName, fastGet, options);

		System.out.printf("%n[%s] opening and loading database...%n", scenarioName);
		ScenarioResult result;
		try (var connection = new EmbeddedConnection(scenarioRoot.resolve("db"), scenarioName, config)) {
			var api = connection.getSyncApi();
			long columnId = api.createColumn("benchmark",
					ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			var keys = BenchmarkKeys.create(options.hotKeys(), options.writerKeys(), options.missKeyCount());
			preload(api, columnId, keys, options);

			long fallbackBeforeWarmup = connection.getInternalDB().getFastGetNativeErrorFallbacksCount();
			if (options.warmupSeconds() > 0) {
				System.out.printf("[%s] warmup %ds...%n", scenarioName, options.warmupSeconds());
				runPhase(api,
						connection.getInternalDB(),
						columnId,
						keys,
						options,
						scanMode,
						Duration.ofSeconds(options.warmupSeconds()),
						options.seed() + round * 10_000L,
						false);
			}

			long fallbackAfterWarmup = connection.getInternalDB().getFastGetNativeErrorFallbacksCount();
			System.out.printf("[%s] measuring %ds...%n", scenarioName, options.measureSeconds());
			var phase = runPhase(api,
					connection.getInternalDB(),
					columnId,
					keys,
					options,
					scanMode,
					Duration.ofSeconds(options.measureSeconds()),
					options.seed() + round * 100_000L,
					true);
			awaitNoPendingOperations(connection.getInternalDB());

			result = new ScenarioResult(round,
					fastGet,
					scanMode,
					phase,
					fallbackAfterWarmup - fallbackBeforeWarmup,
					connection.getInternalDB().getFastGetNativeErrorFallbacksCount() - fallbackAfterWarmup,
					connection.getInternalDB().getPendingOpsCount(),
					connection.getInternalDB().getOpenIteratorsCount(),
					connection.getInternalDB().getOpenTransactionsCount());
		}
		printResult(result);
		return result;
	}

	private static void preload(RocksDBSyncAPI api, long columnId, BenchmarkKeys keys, Options options) {
		int totalKeys = keys.hot().length + keys.writer().length;
		int flushEvery = Math.max(1, (totalKeys + options.preloadFlushes() - 1) / options.preloadFlushes());
		int loaded = 0;
		for (int index = 0; index < keys.hot().length; index++) {
			api.put(0, columnId, keys.hot()[index], value(index, 0, options.valueBytes()), RequestType.none());
			if (++loaded % flushEvery == 0) {
				api.flush();
			}
		}
		for (int index = 0; index < keys.writer().length; index++) {
			long keyId = options.hotKeys() + (long) index;
			api.put(0, columnId, keys.writer()[index], value(keyId, 0, options.valueBytes()), RequestType.none());
			if (++loaded % flushEvery == 0) {
				api.flush();
			}
		}
		api.flush();
		if (options.compactPreload()) {
			api.compact();
		}
		System.out.printf(Locale.ROOT,
				"Loaded %,d non-empty values (%.1f MiB logical), %d flush groups%s.%n",
				totalKeys,
				totalKeys * (double) options.valueBytes() / (1024 * 1024),
				options.preloadFlushes(),
				options.compactPreload() ? ", compacted" : "");
	}

	private static PhaseResult runPhase(RocksDBSyncAPI api,
			EmbeddedDB embeddedDB,
			long columnId,
			BenchmarkKeys keys,
			Options options,
			ScanMode scanMode,
			Duration duration,
			long seed,
			boolean recordLatencies) throws Exception {
		int scannerThreads = scanMode == ScanMode.OFF ? 0 : options.scanners();
		int maintenanceThreads = options.writers() > 0
				&& (options.flushIntervalMillis() > 0 || options.compactIntervalMillis() > 0) ? 1 : 0;
		int threadCount = options.readers() + options.writers() + scannerThreads
				+ options.firstLastWorkers() + maintenanceThreads;
		if (threadCount == 0) {
			throw new IllegalArgumentException("At least one worker must be configured");
		}

		var metrics = new PhaseMetrics();
		var stop = new AtomicBoolean();
		var ready = new CountDownLatch(threadCount);
		var start = new CountDownLatch(1);
		ExecutorService executor = Executors.newFixedThreadPool(threadCount,
				Thread.ofPlatform().name("fast-get-bench-", 0).factory());
		var futures = new ArrayList<Future<?>>(threadCount);
		long startedAt = 0;
		long measurementEndedAt = 0;
		long cleanupStartedAt = 0;
		long cleanupNanos = 0;
		try {
			int workerId = 0;
			for (int i = 0; i < options.readers(); i++) {
				int id = workerId++;
				submitWorker(executor,
						futures,
						metrics,
						stop,
						() -> runReader(api,
								columnId,
								keys,
								options,
								metrics,
								stop,
								ready,
								start,
								seed + id,
								recordLatencies));
			}
			for (int i = 0; i < options.writers(); i++) {
				int id = workerId++;
				submitWorker(executor,
						futures,
						metrics,
						stop,
						() -> runWriter(api,
								columnId,
								keys,
								options,
								metrics,
								stop,
								ready,
								start,
								seed + id));
			}
			for (int i = 0; i < scannerThreads; i++) {
				workerId++;
				submitWorker(executor,
						futures,
						metrics,
						stop,
						() -> runScanner(api,
								columnId,
								scanMode,
								options,
								metrics,
								stop,
								ready,
								start));
			}
			for (int i = 0; i < options.firstLastWorkers(); i++) {
				workerId++;
				submitWorker(executor,
						futures,
						metrics,
						stop,
						() -> runFirstAndLast(api,
								columnId,
								options,
								metrics,
								stop,
								ready,
								start));
			}
			if (maintenanceThreads > 0) {
				submitWorker(executor,
						futures,
						metrics,
						stop,
						() -> runMaintenance(api, options, metrics, stop, ready, start));
			}

			if (!ready.await(PHASE_READY_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
				throw new IllegalStateException("Benchmark workers did not become ready within "
						+ PHASE_READY_TIMEOUT_SECONDS + " seconds (remaining=" + ready.getCount() + ")");
			}
			startedAt = System.nanoTime();
			start.countDown();
			Thread.sleep(duration.toMillis());
			measurementEndedAt = System.nanoTime();
		} finally {
			if (startedAt != 0 && measurementEndedAt == 0) {
				measurementEndedAt = System.nanoTime();
			}
			cleanupStartedAt = System.nanoTime();
			stop.set(true);
			start.countDown();
			executor.shutdown();
			if (!executor.awaitTermination(PHASE_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
				executor.shutdownNow();
				if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
					throw new IllegalStateException("Benchmark workers did not terminate");
				}
			}
			for (var future : futures) {
				try {
					future.get();
				} catch (Throwable throwable) {
					metrics.recordError(throwable);
				}
			}
			cleanupNanos = System.nanoTime() - cleanupStartedAt;
		}
		return metrics.snapshot(measurementEndedAt - startedAt, cleanupNanos, embeddedDB);
	}

	private static void submitWorker(ExecutorService executor,
			List<Future<?>> futures,
			PhaseMetrics metrics,
			AtomicBoolean stop,
			Runnable worker) {
		futures.add(executor.submit(() -> {
			try {
				worker.run();
			} catch (Throwable throwable) {
				metrics.recordError(throwable);
				stop.set(true);
			}
		}));
	}

	private static void runReader(RocksDBSyncAPI api,
			long columnId,
			BenchmarkKeys keys,
			Options options,
			PhaseMetrics metrics,
			AtomicBoolean stop,
			CountDownLatch ready,
			CountDownLatch start,
			long seed,
			boolean recordLatencies) {
		var random = new SplittableRandom(seed);
		var samples = new LongSamples(options.maxLatencySamplesPerReader());
		long operation = 0;
		ready.countDown();
		await(start);
		while (!stop.get()) {
			int readKind = random.nextInt(100);
			boolean miss = readKind < options.missPercent();
			boolean mutable = !miss && readKind < options.missPercent() + options.mutableReadPercent();
			int index = random.nextInt(miss
					? keys.missing().length
					: mutable ? keys.writer().length : keys.hot().length);
			Keys key = miss ? keys.missing()[index] : mutable ? keys.writer()[index] : keys.hot()[index];
			long expectedKey = mutable ? options.hotKeys() + (long) index : index;
			long startedAt = recordLatencies && operation % options.sampleEvery() == 0 ? System.nanoTime() : 0;
			try {
				Buf result = api.get(0, columnId, key, RequestType.current());
				if (miss) {
					if (result != null) {
						metrics.recordError("missing key returned a value");
					} else {
						metrics.missReads.increment();
					}
				} else if (validValue(result, expectedKey, options.valueBytes())) {
					metrics.hitReads.increment();
					if (mutable) {
						metrics.mutableReads.increment();
					}
				} else {
					metrics.recordError("wrong/null value for " + (mutable ? "mutable" : "immutable")
							+ " key " + expectedKey);
				}
			} catch (Throwable throwable) {
				metrics.recordError(throwable);
			}
			if (startedAt != 0) {
				samples.add(System.nanoTime() - startedAt);
			}
			operation++;
		}
		metrics.samples.add(samples);
	}

	private static void runWriter(RocksDBSyncAPI api,
			long columnId,
			BenchmarkKeys keys,
			Options options,
			PhaseMetrics metrics,
			AtomicBoolean stop,
			CountDownLatch ready,
			CountDownLatch start,
			long seed) {
		var random = new SplittableRandom(seed);
		long version = seed;
		long writeIntervalNanos = options.writerOpsPerSecond() > 0
				? Math.max(1L, Math.round(1_000_000_000.0 * options.writers() / options.writerOpsPerSecond()))
				: 0L;
		ready.countDown();
		await(start);
		long nextWriteAt = System.nanoTime();
		while (!stop.get()) {
			if (writeIntervalNanos > 0) {
				while (!stop.get()) {
					long remaining = nextWriteAt - System.nanoTime();
					if (remaining <= 0) {
						break;
					}
					LockSupport.parkNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(1)));
				}
				if (stop.get()) {
					break;
				}
			}
			int index = random.nextInt(keys.writer().length);
			long keyId = options.hotKeys() + (long) index;
			try {
				api.put(0,
						columnId,
						keys.writer()[index],
						value(keyId, ++version, options.valueBytes()),
						RequestType.none());
				metrics.writes.increment();
			} catch (Throwable throwable) {
				metrics.recordError(throwable);
			}
			if (writeIntervalNanos > 0) {
				nextWriteAt += writeIntervalNanos;
				long now = System.nanoTime();
				if (nextWriteAt < now - writeIntervalNanos) {
					nextWriteAt = now;
				}
			}
		}
	}

	private static void runScanner(RocksDBSyncAPI api,
			long columnId,
			ScanMode scanMode,
			Options options,
			PhaseMetrics metrics,
			AtomicBoolean stop,
			CountDownLatch ready,
			CountDownLatch start) {
		ready.countDown();
		await(start);
		while (!stop.get()) {
			var requestType = scanMode == ScanMode.CACHE
					? RequestType.<KV>allInRange()
					: RequestType.<KV>allInRangeNoCache();
			try (var range = api.getRange(0,
					columnId,
					null,
					null,
					false,
					requestType,
					options.scanTimeoutMillis())) {
				var iterator = range.iterator();
				while (!stop.get() && iterator.hasNext()) {
					var row = iterator.next();
					metrics.scanRows.increment();
					metrics.scanBytes.add(row.value() != null ? row.value().size() : 0);
				}
			} catch (RocksDBException exception) {
				if (!stop.get() && exception.getErrorUniqueId() == RocksDBErrorType.READ_DEADLINE_EXCEEDED) {
					metrics.scanDeadlines.increment();
				} else if (!stop.get()) {
					metrics.recordError(exception);
				}
			} catch (Throwable throwable) {
				if (!stop.get()) {
					metrics.recordError(throwable);
				}
			}
		}
	}

	private static void runFirstAndLast(RocksDBSyncAPI api,
			long columnId,
			Options options,
			PhaseMetrics metrics,
			AtomicBoolean stop,
			CountDownLatch ready,
			CountDownLatch start) {
		ready.countDown();
		await(start);
		while (!stop.get()) {
			try {
				FirstAndLast<KV> result = api.reduceRange(0,
						columnId,
						null,
						null,
						false,
						RequestType.firstAndLast(),
						options.firstLastTimeoutMillis());
				if (result.first() == null || result.last() == null) {
					metrics.recordError("firstAndLast unexpectedly returned an empty endpoint");
				} else {
					metrics.firstLast.increment();
				}
			} catch (RocksDBException exception) {
				if (exception.getErrorUniqueId() == RocksDBErrorType.READ_DEADLINE_EXCEEDED) {
					metrics.firstLastDeadlines.increment();
				} else {
					metrics.recordError(exception);
				}
			} catch (Throwable throwable) {
				metrics.recordError(throwable);
			}
		}
	}

	private static void runMaintenance(RocksDBSyncAPI api,
			Options options,
			PhaseMetrics metrics,
			AtomicBoolean stop,
			CountDownLatch ready,
			CountDownLatch start) {
		ready.countDown();
		await(start);
		long nextFlush = options.flushIntervalMillis() > 0
				? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.flushIntervalMillis()) : Long.MAX_VALUE;
		long nextCompact = options.compactIntervalMillis() > 0
				? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.compactIntervalMillis()) : Long.MAX_VALUE;
		while (!stop.get()) {
			long now = System.nanoTime();
			try {
				if (now >= nextFlush) {
					api.flush();
					metrics.flushes.increment();
					nextFlush = now + TimeUnit.MILLISECONDS.toNanos(options.flushIntervalMillis());
				}
				if (now >= nextCompact) {
					api.compact();
					metrics.compactions.increment();
					nextCompact = now + TimeUnit.MILLISECONDS.toNanos(options.compactIntervalMillis());
				}
				Thread.sleep(10);
			} catch (InterruptedException interrupted) {
				Thread.currentThread().interrupt();
				return;
			} catch (Throwable throwable) {
				metrics.recordError(throwable);
			}
		}
	}

	private static Path writeConfig(Path root, String scenarioName, boolean fastGet, Options options)
			throws IOException {
		Path config = root.resolve("rockserver.conf");
		String levels = """
				[
				  { compression: ZSTD, max-dict-bytes: 32KiB },
				  { compression: ZSTD, max-dict-bytes: 32KiB },
				  { compression: ZSTD, max-dict-bytes: 32KiB },
				  { compression: ZSTD, max-dict-bytes: 32KiB },
				  { compression: ZSTD, max-dict-bytes: 32KiB },
				  { compression: ZSTD, max-dict-bytes: 32KiB },
				  { compression: ZSTD, max-dict-bytes: 32KiB }
				]
				""";
		Files.writeString(config, """
				database: {
				  metrics: {
				    database-name: "%s"
				    jmx: { enabled: false }
				  }
				  global: {
				    enable-fast-get: %s
				    ingest-behind: false
				    optimistic: false
				    spinning: %s
				    use-direct-io: %s
				    maximum-open-files: 1024
				    block-cache: "%s"
				    write-buffer-manager: "%s"
				    log-path: "%s"
				    wal-path: "%s"
				    temp-sst-path: "%s"
				    fallback-column-options: {
				      volumes: [{ volume-path: "%s", target-size: "1TiB" }]
				      levels: %s
				      first-level-sst-size: "%s"
				      max-last-level-sst-size: "%s"
				      block-size: "16KiB"
				      write-buffer-size: "%s"
				      memtable-memory-budget-bytes: "%s"
				    }
				  }
				}
				""".formatted(escapeHocon(scenarioName),
				fastGet,
				options.spinning(),
				options.directIo(),
				escapeHocon(options.blockCache()),
				escapeHocon(options.writeBufferManager()),
				escapeHocon(root.resolve("logs").toAbsolutePath().toString()),
				escapeHocon(root.resolve("wal").toAbsolutePath().toString()),
				escapeHocon(root.resolve("temp-sst").toAbsolutePath().toString()),
				escapeHocon(root.resolve("volume").toAbsolutePath().toString()),
				levels,
				escapeHocon(options.firstLevelSstSize()),
				escapeHocon(options.lastLevelSstSize()),
				escapeHocon(options.writeBufferSize()),
				escapeHocon(options.memtableBudget())));
		return config;
	}

	private static Buf value(long keyId, long version, int valueBytes) {
		byte[] value = new byte[valueBytes];
		var buffer = ByteBuffer.wrap(value);
		buffer.putLong(keyId);
		buffer.putLong(version);
		buffer.putLong(VALUE_MAGIC ^ keyId);
		long state = keyId ^ Long.rotateLeft(version, 17) ^ VALUE_MAGIC;
		for (int offset = 24; offset < value.length; offset++) {
			state ^= state << 13;
			state ^= state >>> 7;
			state ^= state << 17;
			value[offset] = (byte) state;
		}
		return Buf.wrap(value);
	}

	private static boolean validValue(Buf value, long expectedKey, int valueBytes) {
		return value != null
				&& value.size() == valueBytes
				&& value.getLong(0) == expectedKey
				&& value.getLong(16) == (VALUE_MAGIC ^ expectedKey);
	}

	private static Keys key(long keyId) {
		byte[] backing = new byte[Long.BYTES + 16];
		ByteBuffer.wrap(backing, 8, Long.BYTES).putLong(keyId);
		return new Keys(Buf.wrap(backing, 8, 8 + Long.BYTES));
	}

	private static void await(CountDownLatch latch) {
		try {
			latch.await();
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Benchmark worker interrupted before start", interrupted);
		}
	}

	private static void awaitNoPendingOperations(EmbeddedDB db) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (db.getPendingOpsCount() != 0 && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
	}

	private static void printResult(ScenarioResult result) {
		var phase = result.phase();
		double seconds = phase.elapsedNanos() / 1_000_000_000.0;
		var latency = phase.latency();
		System.out.printf(Locale.ROOT,
				"RESULT round=%d fast=%-5s scan=%-8s read=%10.0f ops/s (hit=%9.0f mutable=%8.0f miss=%8.0f) "
						+ "p50=%7.1fus p95=%7.1fus p99=%7.1fus max=%8.1fus write=%9.0f/s scan=%7.1f MiB/s "
						+ "firstLast=%d deadline=%d scanDeadline=%d nativeErrorFallback[warmup=%d measured=%d] "
						+ "samples=%d dropped=%d errors=%d cleanup[time=%.1fms pending=%d iter=%d tx=%d]%n",
				result.round(),
				result.fastGet(),
				result.scanMode().cliName,
				(phase.hitReads() + phase.missReads()) / seconds,
				phase.hitReads() / seconds,
				phase.mutableReads() / seconds,
				phase.missReads() / seconds,
				latency.p50() / 1_000.0,
				latency.p95() / 1_000.0,
				latency.p99() / 1_000.0,
				latency.max() / 1_000.0,
				phase.writes() / seconds,
				phase.scanBytes() / seconds / (1024 * 1024),
				phase.firstLast(),
				phase.firstLastDeadlines(),
				phase.scanDeadlines(),
				result.warmupNativeErrorFallbacks(),
				result.measuredNativeErrorFallbacks(),
				phase.latencySamples(),
				phase.droppedLatencySamples(),
				phase.errors(),
				phase.cleanupNanos() / 1_000_000.0,
				result.pendingOps(),
				result.openIterators(),
				result.openTransactions());
		if (!phase.errorSamples().isEmpty()) {
			System.out.println("  error samples: " + String.join(" | ", phase.errorSamples()));
		}
	}

	private static void printResults(List<ScenarioResult> results) {
		System.out.println("\n=== FastGet comparison ===");
		for (var scanMode : ScanMode.values()) {
			var normal = results.stream().filter(result -> !result.fastGet() && result.scanMode() == scanMode).toList();
			var fast = results.stream().filter(result -> result.fastGet() && result.scanMode() == scanMode).toList();
			if (normal.isEmpty() || fast.isEmpty()) {
				continue;
			}
			double normalRate = normal.stream().mapToDouble(ScenarioResult::readRate).average().orElseThrow();
			double fastRate = fast.stream().mapToDouble(ScenarioResult::readRate).average().orElseThrow();
			long fastFallbacks = fast.stream().mapToLong(ScenarioResult::measuredNativeErrorFallbacks).sum();
			System.out.printf(Locale.ROOT,
					"scan=%-8s normal=%,.0f ops/s fast=%,.0f ops/s speedup=%.3fx measured-native-error-fallbacks=%d%n",
					scanMode.cliName,
					normalRate,
					fastRate,
					fastRate / normalRate,
					fastFallbacks);
		}
		long warmupFallbacks = results.stream().mapToLong(ScenarioResult::warmupNativeErrorFallbacks).sum();
		long measuredFallbacks = results.stream().mapToLong(ScenarioResult::measuredNativeErrorFallbacks).sum();
		long typedDeadlines = results.stream().mapToLong(result -> result.phase().firstLastDeadlines()).sum();
		System.out.printf("Native-error fallbacks: warmup=%d measured=%d%n", warmupFallbacks, measuredFallbacks);
		System.out.printf("firstAndLast typed deadlines: %d%n", typedDeadlines);
	}

	private static void failOnInvalidResults(List<ScenarioResult> results) {
		long workerErrors = results.stream().mapToLong(result -> result.phase().errors()).sum();
		long cleanupLeaks = results.stream().filter(ScenarioResult::hasCleanupLeak).count();
		if (workerErrors != 0 || cleanupLeaks != 0) {
			throw new IllegalStateException("Benchmark failed: workerErrors=" + workerErrors
					+ ", scenariosWithCleanupLeaks=" + cleanupLeaks);
		}
	}

	private static void printUsage() {
		System.out.println("""
				Run with:
				  mvn -DskipTests test-compile org.codehaus.mojo:exec-maven-plugin:3.5.0:java \\
				    -Dexec.classpathScope=test \\
				    -Dexec.mainClass=it.cavallium.rockserver.core.impl.benchmark.FastGetBenchmark \\
				    -Dexec.args="--fast=both --scan=all --seconds=10"

				Important options (all use --name=value):
				  root=target/fast-get-benchmark  fast=both|true|false  scan=all|off|cache|no-cache
				  hot-keys=20000  writer-keys=5000  value-bytes=1024  miss-percent=10
				  mutable-read-percent=20  sample-every=64  max-latency-samples-per-reader=250000
				  readers=4  writers=2  writer-ops-per-second=0  scanners=1  first-last-workers=1
				  warmup-seconds=2  seconds=5  rounds=1  first-last-timeout-ms=1
				  block-cache=16MiB  write-buffer-manager=16MiB  write-buffer-size=4MiB
				  direct-io=true  spinning=false  flush-interval-ms=500  compact-interval-ms=0
				  preload-flushes=4  compact-preload=true  keep-data=false

				Use a real HDD-backed --root and a dataset several times larger than --block-cache for production-like I/O.
				Each invocation creates a uniquely named, ownership-marked run directory below --root.
				""");
	}

	private static String escapeHocon(String value) {
		return value.replace("\\", "\\\\").replace("\"", "\\\"");
	}

	private static RunDirectory createRunDirectory(Path configuredRoot) throws IOException {
		Path root = configuredRoot.toAbsolutePath().normalize();
		Files.createDirectories(root);
		Path runRoot = Files.createTempDirectory(root, "run-").toAbsolutePath().normalize();
		String markerToken = UUID.randomUUID().toString();
		try {
			Files.writeString(runRoot.resolve(RUN_MARKER_FILE), markerToken, StandardOpenOption.CREATE_NEW);
		} catch (IOException exception) {
			try {
				Files.deleteIfExists(runRoot);
			} catch (IOException cleanupFailure) {
				exception.addSuppressed(cleanupFailure);
			}
			throw exception;
		}
		return new RunDirectory(runRoot, markerToken);
	}

	private static void deleteRunDirectory(RunDirectory runDirectory) throws IOException {
		Path marker = runDirectory.path().resolve(RUN_MARKER_FILE);
		if (!Files.isRegularFile(marker)
				|| !Files.readString(marker).equals(runDirectory.markerToken())) {
			throw new IOException("Refusing to delete unowned benchmark directory: " + runDirectory.path());
		}
		try (var paths = Files.walk(runDirectory.path())) {
			for (var item : paths.sorted(Comparator.reverseOrder()).toList()) {
				Files.deleteIfExists(item);
			}
		}
	}

	private enum ScanMode {
		OFF("off"),
		CACHE("cache"),
		NO_CACHE("no-cache");

		private final String cliName;

		ScanMode(String cliName) {
			this.cliName = cliName;
		}

		private static EnumSet<ScanMode> parse(String value) {
			if (value.equalsIgnoreCase("all")) {
				return EnumSet.allOf(ScanMode.class);
			}
			var result = EnumSet.noneOf(ScanMode.class);
			for (var token : value.split(",")) {
				result.add(switch (token.strip().toLowerCase(Locale.ROOT)) {
					case "off" -> OFF;
					case "cache" -> CACHE;
					case "no-cache", "nocache" -> NO_CACHE;
					default -> throw new IllegalArgumentException("Unknown scan mode: " + token);
				});
			}
			return result;
		}
	}

	private record RunDirectory(Path path, String markerToken) {
	}

	private record BenchmarkKeys(Keys[] hot, Keys[] writer, Keys[] missing) {

		private static BenchmarkKeys create(int hotKeys, int writerKeys, int missingKeys) {
			var hot = new Keys[hotKeys];
			var writer = new Keys[writerKeys];
			var missing = new Keys[missingKeys];
			for (int i = 0; i < hot.length; i++) {
				hot[i] = key(i);
			}
			for (int i = 0; i < writer.length; i++) {
				writer[i] = key(hotKeys + (long) i);
			}
			for (int i = 0; i < missing.length; i++) {
				missing[i] = key(MISSING_KEY_BASE + i);
			}
			return new BenchmarkKeys(hot, writer, missing);
		}
	}

	private static final class LongSamples {

		private final int maxSize;
		private long[] values;
		private int size;
		private long dropped;

		private LongSamples(int maxSize) {
			this.maxSize = maxSize;
			this.values = new long[Math.min(1_024, maxSize)];
		}

		private void add(long value) {
			if (size == maxSize) {
				dropped++;
				return;
			}
			if (size == values.length) {
				values = Arrays.copyOf(values, Math.min(maxSize, values.length * 2));
			}
			values[size++] = value;
		}

		private int size() {
			return size;
		}

		private long dropped() {
			return dropped;
		}
	}

	private static final class PhaseMetrics {

		private final LongAdder hitReads = new LongAdder();
		private final LongAdder mutableReads = new LongAdder();
		private final LongAdder missReads = new LongAdder();
		private final LongAdder writes = new LongAdder();
		private final LongAdder scanRows = new LongAdder();
		private final LongAdder scanBytes = new LongAdder();
		private final LongAdder scanDeadlines = new LongAdder();
		private final LongAdder firstLast = new LongAdder();
		private final LongAdder firstLastDeadlines = new LongAdder();
		private final LongAdder flushes = new LongAdder();
		private final LongAdder compactions = new LongAdder();
		private final LongAdder errors = new LongAdder();
		private final ConcurrentLinkedQueue<String> errorSamples = new ConcurrentLinkedQueue<>();
		private final ConcurrentLinkedQueue<LongSamples> samples = new ConcurrentLinkedQueue<>();

		private void recordError(Throwable throwable) {
			recordError(throwable.getClass().getSimpleName() + ": " + throwable.getMessage());
		}

		private void recordError(String message) {
			errors.increment();
			if (errorSamples.size() < 8) {
				errorSamples.add(message);
			}
		}

		private PhaseResult snapshot(long elapsedNanos, long cleanupNanos, EmbeddedDB db) {
			int sampleCount = samples.stream().mapToInt(LongSamples::size).sum();
			long droppedSamples = samples.stream().mapToLong(LongSamples::dropped).sum();
			long[] merged = new long[sampleCount];
			int offset = 0;
			for (var sampleSet : samples) {
				System.arraycopy(sampleSet.values, 0, merged, offset, sampleSet.size);
				offset += sampleSet.size;
			}
			Arrays.sort(merged);
			return new PhaseResult(elapsedNanos,
					cleanupNanos,
					hitReads.sum(),
					mutableReads.sum(),
					missReads.sum(),
					writes.sum(),
					scanRows.sum(),
					scanBytes.sum(),
					scanDeadlines.sum(),
					firstLast.sum(),
					firstLastDeadlines.sum(),
					flushes.sum(),
					compactions.sum(),
					errors.sum(),
					List.copyOf(errorSamples),
					Latency.fromSorted(merged),
					sampleCount,
					droppedSamples,
					db.getPendingOpsCount());
		}
	}

	private record Latency(long p50, long p95, long p99, long max) {

		private static Latency fromSorted(long[] values) {
			if (values.length == 0) {
				return new Latency(0, 0, 0, 0);
			}
			return new Latency(percentile(values, 0.50),
					percentile(values, 0.95),
					percentile(values, 0.99),
					values[values.length - 1]);
		}

		private static long percentile(long[] values, double percentile) {
			int index = Math.min(values.length - 1, (int) Math.ceil(values.length * percentile) - 1);
			return values[Math.max(0, index)];
		}
	}

	private record PhaseResult(long elapsedNanos,
			long cleanupNanos,
			long hitReads,
			long mutableReads,
			long missReads,
			long writes,
			long scanRows,
			long scanBytes,
			long scanDeadlines,
			long firstLast,
			long firstLastDeadlines,
			long flushes,
			long compactions,
			long errors,
			List<String> errorSamples,
			Latency latency,
			int latencySamples,
			long droppedLatencySamples,
			long pendingOpsAtSnapshot) {
	}

	private record ScenarioResult(int round,
			boolean fastGet,
			ScanMode scanMode,
			PhaseResult phase,
			long warmupNativeErrorFallbacks,
			long measuredNativeErrorFallbacks,
			long pendingOps,
			int openIterators,
			int openTransactions) {

		private double readRate() {
			return (phase.hitReads() + phase.missReads()) / (phase.elapsedNanos() / 1_000_000_000.0);
		}

		private boolean hasCleanupLeak() {
			return pendingOps != 0 || openIterators != 0 || openTransactions != 0;
		}
	}

	private record Options(Path root,
			boolean runNormal,
			boolean runFast,
			EnumSet<ScanMode> scanModes,
			int hotKeys,
			int writerKeys,
			int missKeyCount,
			int valueBytes,
			int readers,
			int writers,
			long writerOpsPerSecond,
			int scanners,
			int firstLastWorkers,
			int warmupSeconds,
			int measureSeconds,
			int rounds,
			int missPercent,
			int mutableReadPercent,
			int sampleEvery,
			int maxLatencySamplesPerReader,
			long firstLastTimeoutMillis,
			long scanTimeoutMillis,
			long flushIntervalMillis,
			long compactIntervalMillis,
			int preloadFlushes,
			boolean compactPreload,
			boolean directIo,
			boolean spinning,
			boolean keepData,
			String blockCache,
			String writeBufferManager,
			String writeBufferSize,
			String memtableBudget,
			String firstLevelSstSize,
			String lastLevelSstSize,
			long seed) {

		private static Options parse(String[] args) {
			var values = new LinkedHashMap<String, String>();
			for (var arg : args) {
				if (!arg.startsWith("--") || !arg.contains("=")) {
					throw new IllegalArgumentException("Arguments must use --name=value: " + arg);
				}
				int separator = arg.indexOf('=');
				String previous = values.put(arg.substring(2, separator), arg.substring(separator + 1));
				if (previous != null) {
					throw new IllegalArgumentException("Duplicate option: " + arg.substring(0, separator));
				}
			}
			String fast = take(values, "fast", "both").toLowerCase(Locale.ROOT);
			boolean runNormal = fast.equals("both") || fast.equals("false") || fast.equals("normal");
			boolean runFast = fast.equals("both") || fast.equals("true") || fast.equals("fast");
			if (!runNormal && !runFast) {
				throw new IllegalArgumentException("fast must be both, true, or false");
			}
			var options = new Options(Path.of(take(values, "root", "target/fast-get-benchmark")),
					runNormal,
					runFast,
					ScanMode.parse(take(values, "scan", "all")),
					integer(values, "hot-keys", 20_000),
					integer(values, "writer-keys", 5_000),
					integer(values, "miss-keys", 4_096),
					integer(values, "value-bytes", 1_024),
					integer(values, "readers", 4),
					integer(values, "writers", 2),
					longValue(values, "writer-ops-per-second", 0),
					integer(values, "scanners", 1),
					integer(values, "first-last-workers", 1),
					integer(values, "warmup-seconds", 2),
					integer(values, "seconds", 5),
					integer(values, "rounds", 1),
					integer(values, "miss-percent", 10),
					integer(values, "mutable-read-percent", 20),
					integer(values, "sample-every", 64),
					integer(values, "max-latency-samples-per-reader", 250_000),
					longValue(values, "first-last-timeout-ms", 1),
					longValue(values, "scan-timeout-ms", 120_000),
					longValue(values, "flush-interval-ms", 500),
					longValue(values, "compact-interval-ms", 0),
					integer(values, "preload-flushes", 4),
					bool(values, "compact-preload", true),
					bool(values, "direct-io", true),
					bool(values, "spinning", false),
					bool(values, "keep-data", false),
					take(values, "block-cache", "16MiB"),
					take(values, "write-buffer-manager", "16MiB"),
					take(values, "write-buffer-size", "4MiB"),
					take(values, "memtable-budget", "16MiB"),
					take(values, "first-level-sst-size", "4MiB"),
					take(values, "last-level-sst-size", "16MiB"),
					longValue(values, "seed", 0x5EED_F45L));
			if (!values.isEmpty()) {
				throw new IllegalArgumentException("Unknown options: " + values.keySet());
			}
			options.validate();
			return options;
		}

		private void validate() {
			if (hotKeys <= 0 || writerKeys <= 0 || missKeyCount <= 0 || valueBytes < 24
					|| readers <= 0 || writers < 0 || writerOpsPerSecond < 0 || scanners < 0 || firstLastWorkers < 0
					|| warmupSeconds < 0 || measureSeconds <= 0 || rounds <= 0
					|| missPercent < 0 || missPercent > 100 || mutableReadPercent < 0
					|| missPercent + mutableReadPercent > 100 || sampleEvery <= 0
					|| maxLatencySamplesPerReader <= 0
					|| firstLastTimeoutMillis < 0 || scanTimeoutMillis <= 0
					|| flushIntervalMillis < 0 || compactIntervalMillis < 0 || preloadFlushes <= 0) {
				throw new IllegalArgumentException("Invalid non-positive/out-of-range benchmark option; use --help");
			}
			boolean scansEnabled = scanModes.stream().anyMatch(scanMode -> scanMode != ScanMode.OFF);
			if (scansEnabled && scanners == 0) {
				throw new IllegalArgumentException("Selected cache/no-cache scan mode requires --scanners > 0");
			}
			if (!scansEnabled && scanners != 0) {
				throw new IllegalArgumentException("--scanners has no effect when --scan=off; set --scanners=0");
			}
			if (writers == 0 && mutableReadPercent != 0) {
				throw new IllegalArgumentException("Mutable-key reads require --writers > 0, or set --mutable-read-percent=0");
			}
			if (writers == 0 && (flushIntervalMillis != 0 || compactIntervalMillis != 0)) {
				throw new IllegalArgumentException("Maintenance intervals have no effect with --writers=0; set both to 0");
			}
			if (writers == 0 && writerOpsPerSecond != 0) {
				throw new IllegalArgumentException("--writer-ops-per-second requires --writers > 0");
			}
		}

		private List<Boolean> fastGetModesForRound(int round) {
			if (runNormal && runFast) {
				return round % 2 == 0 ? List.of(true, false) : List.of(false, true);
			}
			return List.of(runFast);
		}

		private static String take(Map<String, String> values, String name, String defaultValue) {
			return values.containsKey(name) ? values.remove(name) : defaultValue;
		}

		private static int integer(Map<String, String> values, String name, int defaultValue) {
			return Integer.parseInt(take(values, name, Integer.toString(defaultValue)));
		}

		private static long longValue(Map<String, String> values, String name, long defaultValue) {
			return Long.parseLong(take(values, name, Long.toString(defaultValue)));
		}

		private static boolean bool(Map<String, String> values, String name, boolean defaultValue) {
			String value = take(values, name, Boolean.toString(defaultValue));
			return switch (value.toLowerCase(Locale.ROOT)) {
				case "true" -> true;
				case "false" -> false;
				default -> throw new IllegalArgumentException(name + " must be true or false, got: " + value);
			};
		}
	}
}
