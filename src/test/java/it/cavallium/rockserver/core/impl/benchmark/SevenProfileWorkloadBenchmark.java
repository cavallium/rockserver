package it.cavallium.rockserver.core.impl.benchmark;

import io.micrometer.core.instrument.Counter;
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
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.rocksdb.RocksLeakDetector;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

/**
 * Opt-in, disk-backed workload benchmark that keeps all seven scheduler profiles active.
 * It is deliberately a test-source main class: ordinary Maven tests never run hardware work.
 */
public final class SevenProfileWorkloadBenchmark {

	private static final String RESULT_SCHEMA = "rockserver-seven-profile-workload-v2";
	private static final String DATASET_SCHEMA = "rockserver-seven-profile-dataset-v2";
	private static final String COLUMN_NAME = "seven-profile-workload";
	private static final String CDC_SUBSCRIPTION = "seven-profile-workload-cdc";
	private static final long LATENCY_DEADLINE_MILLIS = 5_000L;
	private static final long FALLBACK_P99_LIMIT_NANOS = TimeUnit.SECONDS.toNanos(1L);
	private static final long RESOURCE_DRAIN_SECONDS = 30L;
	private static final int MAX_ERRORS = 32;
	private static final Comparator<WorkloadKey> WORKLOAD_KEY_ORDER = Comparator
			.comparingInt((WorkloadKey key) -> WorkloadBenchmarkSelector.ALL_PROFILES.indexOf(key.profile()))
			.thenComparing(key -> key.family().name());

	private SevenProfileWorkloadBenchmark() {
	}

	public static void main(String[] args) throws Exception {
		if (contains(args, "--help")) {
			printUsage();
			return;
		}
		var raw = parseArguments(args);
		if (Boolean.parseBoolean(raw.getOrDefault("print-candidates", "false"))) {
			printCandidates(integer(raw, "candidate-min", 4), integer(raw, "candidate-max", 64));
			return;
		}

		Options options = Options.parse(raw);
		System.setProperty("rockserver.core.print-config", "false");
		System.setProperty("it.cavallium.rockserver.leakdetection", "true");
		Instant started = Instant.now();
		long nativeLeaksBefore = RocksLeakDetector.detectedLeakCount();
		PreparedDataset dataset = options.reusePrepared()
				? openPrepared(options)
				: prepare(options);
		if (options.prepareOnly()) {
			long nativeLeaks = awaitNativeLeakDetection(nativeLeaksBefore);
			if (nativeLeaks != 0L) {
				throw new IllegalStateException("Dataset preparation leaked " + nativeLeaks + " native handles");
			}
			System.out.println("Prepared and closed dataset: " + options.root().toAbsolutePath().normalize());
			System.out.println("Evict the host page cache, then rerun with --reuse-prepared=true --cache-state=cold.");
			return;
		}
		if (options.ingestBaselineOnly()) {
			double throughput;
			try {
				throughput = runIsolatedIngestBaseline(dataset, options);
			} finally {
				dataset.connection().closeTesting();
			}
			long nativeLeaks = awaitNativeLeakDetection(nativeLeaksBefore);
			Files.writeString(options.root().resolve("ingest-baseline.properties"),
					"schema=rockserver-ingest-isolated-baseline-v2\n"
							+ "dataset-fingerprint=" + dataset.fingerprint() + "\n"
							+ "comparison-fingerprint=" + comparisonFingerprint(options) + "\n"
							+ "storage-label=" + options.storageLabel() + "\n"
							+ "candidate=" + options.candidate() + "\n"
							+ "throughput=" + Double.toString(throughput) + "\n"
							+ "native-handle-leaks=" + nativeLeaks + "\n",
					StandardOpenOption.CREATE_NEW);
			if (nativeLeaks != 0L) {
				throw new IllegalStateException("Isolated INGEST baseline leaked " + nativeLeaks + " native handles");
			}
			System.out.println("Isolated INGEST throughput: " + format(throughput) + " operations/s");
			return;
		}

		RunSnapshot snapshot = null;
		Throwable runFailure = null;
		Throwable closeFailure = null;
		boolean shutdownClean = false;
		try {
			snapshot = run(dataset, options);
			shutdownClean = true;
		} catch (Throwable failure) {
			runFailure = failure;
		} finally {
			try {
				dataset.connection().closeTesting();
			} catch (Throwable failure) {
				closeFailure = failure;
			}
		}
		long nativeLeaks = awaitNativeLeakDetection(nativeLeaksBefore);
		if (runFailure != null) {
			if (closeFailure != null) {
				runFailure.addSuppressed(closeFailure);
			}
			throw rethrow(runFailure);
		}
		if (closeFailure != null) {
			throw rethrow(closeFailure);
		}
		Objects.requireNonNull(snapshot, "snapshot");
		var result = finishResult(started, Instant.now(), options, dataset.fingerprint(), snapshot,
				shutdownClean, nativeLeaks);
		writeReports(options.root(), result);
		System.out.println(toMarkdown(result));
		System.out.println("Machine-readable result: " + options.root().resolve("results.json").toAbsolutePath());
		System.out.println("Selector input: " + options.root().resolve("selection-input.properties").toAbsolutePath());
		if (options.enforce() && !result.acceptancePassed()) {
			throw new IllegalStateException("Seven-profile benchmark acceptance failed: "
					+ String.join("; ", result.failedChecks()));
		}
	}

	private static PreparedDataset prepare(Options options) throws Exception {
		Path root = options.root().toAbsolutePath().normalize();
		if (Files.exists(root)) {
			throw new IllegalArgumentException("Benchmark root already exists: " + root);
		}
		Files.createDirectories(root);
		Path config = root.resolve("rockserver.conf");
		String configText = configText(options);
		Files.writeString(config, configText, StandardOpenOption.CREATE_NEW);
		String fingerprint = datasetFingerprint(options);
		Files.writeString(root.resolve("dataset.properties"), datasetMarker(options, fingerprint, configText),
				StandardOpenOption.CREATE_NEW);

		var connection = new EmbeddedConnection(root.resolve("db"), options.databaseName(), config);
		boolean success = false;
		try {
			var batch = connection.getSyncApi(RequestContext.batch());
			long columnId = batch.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			preload(batch, columnId, options);
			batch.cdcCreate(CDC_SUBSCRIPTION, null, List.of(columnId), false);
			success = true;
			return new PreparedDataset(connection, columnId, fingerprint);
		} finally {
			if (options.prepareOnly() || !success) {
				connection.closeTesting();
			}
		}
	}

	private static PreparedDataset openPrepared(Options options) throws Exception {
		Path root = options.root().toAbsolutePath().normalize();
		Path marker = root.resolve("dataset.properties");
		Path config = root.resolve("rockserver.conf");
		if (!Files.isRegularFile(marker) || !Files.isRegularFile(config) || !Files.isDirectory(root.resolve("db"))) {
			throw new IllegalArgumentException("Prepared benchmark root is incomplete: " + root);
		}
		if (Files.exists(root.resolve("results.json")) || Files.exists(root.resolve("results.md"))
				|| Files.exists(root.resolve("selection-input.properties"))
				|| Files.exists(root.resolve("ingest-baseline.properties"))) {
			throw new IllegalArgumentException("Prepared root already contains benchmark results: " + root);
		}
		Map<String, String> values = readKeyValues(marker);
		String expectedFingerprint = datasetFingerprint(options);
		String expectedConfig = configText(options);
		if (!DATASET_SCHEMA.equals(values.get("schema"))
				|| !expectedFingerprint.equals(values.get("fingerprint"))
				|| !sha256(expectedConfig).equals(values.get("config-sha256"))
				|| !expectedConfig.equals(Files.readString(config))) {
			throw new IllegalArgumentException("Prepared dataset, seed, dimensions, or configuration do not match");
		}
		var connection = new EmbeddedConnection(root.resolve("db"), options.databaseName(), config);
		long columnId = connection.getSyncApi(RequestContext.batch()).getColumnId(COLUMN_NAME);
		return new PreparedDataset(connection, columnId, expectedFingerprint);
	}

	private static void preload(RocksDBSyncAPI batch, long columnId, Options options) {
		System.out.printf(Locale.ROOT, "Preloading %,d keys (%d bytes/value)%n",
				options.preloadKeys(), options.valueBytes());
		byte[] value = valueBytes(options.valueBytes(), options.seed());
		int batchSize = 256;
		for (int start = 0; start < options.preloadKeys(); start += batchSize) {
			int end = Math.min(options.preloadKeys(), start + batchSize);
			var keys = new ArrayList<Keys>(end - start);
			var values = new ArrayList<Buf>(end - start);
			for (int index = start; index < end; index++) {
				keys.add(key(index));
				values.add(Buf.wrap(value));
			}
			batch.putMulti(0L, columnId, keys, values, RequestType.none());
			if (end % options.preloadFlushKeys() == 0 || end == options.preloadKeys()) {
				batch.flush();
			}
		}
	}

	private static RunSnapshot run(PreparedDataset dataset, Options options) throws Exception {
		var connection = dataset.connection();
		var stats = new RunStats(options.maxLatencySamples());
		var control = new RunControl(workerCount(options), stats);
		var observation = new Observation();
		var pressure = new PressureTracker(options.pressureSeconds() > 0);
		var meterRegistry = new BenchmarkMeterRegistry();
		var composite = (CompositeMeterRegistry) connection.getEmbeddedDB().getMetricsRegistry();
		ExecutorService workers = Executors.newFixedThreadPool(workerCount(options),
				Thread.ofPlatform().name("seven-profile-workload-", 0).factory());
		var futures = new ArrayList<java.util.concurrent.Future<?>>();

		var analytical = connection.getSyncApi(RequestContext.analytical());
		var ingest = connection.getSyncApi(RequestContext.ingest());
		var batch = connection.getSyncApi(RequestContext.batch());
		byte[][] writeValues = writeValues(options);
		List<Keys> cancellationKeys = deterministicKeys(options.preloadKeys(), 256, options.seed() ^ 0x43414e43454cL);

		for (int worker = 0; worker < options.controlWorkers(); worker++) {
			int workerIndex = worker;
			futures.add(workers.submit(() -> runWorker(control, pressure,
					new WorkloadKey(WorkloadProfile.CONTROL, OperationFamily.CONTROL),
					new Pacer(options.controlRate(), options.controlWorkers(), workerIndex), () -> {
						long transactionId = batch.openTransaction(60_000L);
						if (!batch.closeTransaction(transactionId, false)) {
							throw new IllegalStateException("CONTROL rollback did not close its transaction");
						}
					})));
		}
		for (int worker = 0; worker < options.latencyWorkers(); worker++) {
			int workerIndex = worker;
			futures.add(workers.submit(() -> {
				var random = new SplittableRandom(options.seed() + 0x1000L + workerIndex);
				runWorker(control, pressure,
						new WorkloadKey(WorkloadProfile.LATENCY, OperationFamily.POINT_LOOKUP),
						new Pacer(options.latencyRate(), options.latencyWorkers(), workerIndex),
						() -> connection.getSyncApi(RequestContext.latency(Duration.ofMillis(LATENCY_DEADLINE_MILLIS)))
								.get(0L, dataset.columnId(), key(random.nextInt(options.preloadKeys())), RequestType.current()));
			}));
		}
		for (int worker = 0; worker < options.analyticalWorkers(); worker++) {
			int workerIndex = worker;
			futures.add(workers.submit(() -> {
				var random = new SplittableRandom(options.seed() + 0x2000L + workerIndex);
				runWorker(control, pressure,
						new WorkloadKey(WorkloadProfile.ANALYTICAL, OperationFamily.RANGE_PAGE),
						new Pacer(options.analyticalRate(), options.analyticalWorkers(), workerIndex), () -> {
							int available = options.preloadKeys() - options.rangeWidth();
							long start = available == 0 ? 0L : random.nextInt(available + 1);
							try (Stream<?> range = analytical.getRange(0L, dataset.columnId(), key(start),
									key(start + options.rangeWidth()), false, RequestType.allInRange(), 10_000L)) {
								range.count();
							}
						});
			}));
		}
		for (int worker = 0; worker < options.ingestWorkers(); worker++) {
			int workerIndex = worker;
			futures.add(workers.submit(() -> {
				var sequence = new AtomicLong(workerIndex);
				runWorker(control, pressure,
						new WorkloadKey(WorkloadProfile.INGEST, OperationFamily.MUTATION),
						new Pacer(options.ingestRate(), options.ingestWorkers(), workerIndex), () -> {
							long value = sequence.getAndAdd(options.ingestWorkers());
							ingest.put(0L, dataset.columnId(), key((1L << 60) + value % options.writeKeySpace()),
									Buf.wrap(writeValues[(int) (value & (writeValues.length - 1))]), RequestType.none());
						});
			}));
		}
		for (int worker = 0; worker < options.cdcWorkers(); worker++) {
			int workerIndex = worker;
			futures.add(workers.submit(() -> runWorker(control, pressure,
					new WorkloadKey(WorkloadProfile.CDC, OperationFamily.WAL_PAGE),
					new Pacer(options.cdcRate(), options.cdcWorkers(), workerIndex), () -> {
						try (var events = batch.cdcPoll(CDC_SUBSCRIPTION, null, options.cdcMaxEvents())) {
							var page = events.toList();
							if (!page.isEmpty()) {
								batch.cdcCommit(CDC_SUBSCRIPTION, page.getLast().seq());
							}
						}
					})));
		}
		for (int worker = 0; worker < options.batchWorkers(); worker++) {
			int workerIndex = worker;
			futures.add(workers.submit(() -> {
				var sequence = new AtomicLong(workerIndex);
				runWorker(control, pressure,
						new WorkloadKey(WorkloadProfile.BATCH, OperationFamily.MUTATION),
						new Pacer(options.batchRate(), options.batchWorkers(), workerIndex), () -> {
							long value = sequence.getAndAdd(options.batchWorkers());
							batch.put(0L, dataset.columnId(), key((2L << 60) + value % options.writeKeySpace()),
									Buf.wrap(writeValues[(int) (value & (writeValues.length - 1))]), RequestType.none());
						});
			}));
		}
		for (int worker = 0; worker < options.physicalWorkers(); worker++) {
			int workerIndex = worker;
			futures.add(workers.submit(() -> runWorker(control, pressure,
					new WorkloadKey(WorkloadProfile.PHYSICAL_MAINTENANCE, OperationFamily.FLUSH),
					new Pacer(options.physicalRate(), options.physicalWorkers(), workerIndex), batch::flush)));
		}
		for (int worker = 0; worker < options.cancellationWorkers(); worker++) {
			int workerIndex = worker;
			futures.add(workers.submit(() -> runWorker(control, pressure,
					new WorkloadKey(WorkloadProfile.LATENCY, OperationFamily.BOUNDED_FAN_OUT),
					new Pacer(options.cancellationRate(), options.cancellationWorkers(), workerIndex), () -> {
						CompletableFuture<List<Boolean>> request = connection
								.getAsyncApi(RequestContext.latency(Duration.ofMillis(LATENCY_DEADLINE_MILLIS)))
								.existsMultiAsync(0L, dataset.columnId(), cancellationKeys, LATENCY_DEADLINE_MILLIS);
						if (request.cancel(false)) {
							throw new CancellationException("intentional benchmark cancellation");
						}
						request.join();
					})));
		}
		futures.add(workers.submit(() -> sampleLoop(connection, meterRegistry, control, observation, options)));

		control.awaitReady();
		System.out.printf(Locale.ROOT, "Warmup %ds, then measure %ds with candidate %d on %s%n",
				options.warmupSeconds(), options.measureSeconds(), options.candidate(), options.storageLabel());
		control.releaseWorkers();
		sleepPhase(options.warmupSeconds(), control.stop());
		composite.add(meterRegistry);
		control.startMeasurement();
		Thread pressureThread = Thread.ofPlatform().name("seven-profile-pressure").start(
				() -> pressureLoop(connection, control, pressure, options));
		sleepPhase(options.measureSeconds(), control.stop());
		long durationNanos = control.stopMeasurement();
		control.stop().set(true);
		connection.getScheduler().setStoragePressure(false);
		pressureThread.interrupt();
		pressureThread.join(TimeUnit.SECONDS.toMillis(2L));
		workers.shutdownNow();
		if (!workers.awaitTermination(RESOURCE_DRAIN_SECONDS, TimeUnit.SECONDS)) {
			throw new IllegalStateException("Benchmark workers did not terminate");
		}
		for (var future : futures) {
			if (!future.isDone()) {
				future.cancel(true);
			}
		}
		ResourceSnapshot resources = awaitDrain(connection);
		return new RunSnapshot(durationNanos,
				stats.snapshot(durationNanos),
				observation.snapshot(),
				pressure.snapshot(),
				resources,
				snapshotSchedulerMetrics(meterRegistry),
				stats.errors());
	}

	private static double runIsolatedIngestBaseline(PreparedDataset dataset, Options options) throws Exception {
		var stats = new RunStats(options.maxLatencySamples());
		var control = new RunControl(options.ingestWorkers(), stats);
		var pressure = new PressureTracker(false);
		var ingest = dataset.connection().getSyncApi(RequestContext.ingest());
		byte[][] values = writeValues(options);
		ExecutorService workers = Executors.newFixedThreadPool(options.ingestWorkers(),
				Thread.ofPlatform().name("isolated-ingest-baseline-", 0).factory());
		for (int worker = 0; worker < options.ingestWorkers(); worker++) {
			int workerIndex = worker;
			workers.submit(() -> {
				var sequence = new AtomicLong(workerIndex);
				runWorker(control, pressure,
						new WorkloadKey(WorkloadProfile.INGEST, OperationFamily.MUTATION),
						new Pacer(options.ingestRate(), options.ingestWorkers(), workerIndex), () -> {
							long value = sequence.getAndAdd(options.ingestWorkers());
							ingest.put(0L, dataset.columnId(), key((1L << 60) + value % options.writeKeySpace()),
									Buf.wrap(values[(int) (value & (values.length - 1))]), RequestType.none());
						});
			});
		}
		control.awaitReady();
		control.releaseWorkers();
		sleepPhase(options.warmupSeconds(), control.stop());
		control.startMeasurement();
		sleepPhase(options.measureSeconds(), control.stop());
		long durationNanos = control.stopMeasurement();
		control.stop().set(true);
		workers.shutdownNow();
		if (!workers.awaitTermination(RESOURCE_DRAIN_SECONDS, TimeUnit.SECONDS)) {
			throw new IllegalStateException("Isolated INGEST workers did not terminate");
		}
		ResourceSnapshot resources = awaitDrain(dataset.connection());
		if (resources.leakedResources() != 0L || !stats.errors().isEmpty()) {
			throw new IllegalStateException("Isolated INGEST baseline did not drain cleanly: resources="
					+ resources.leakedResources() + ", errors=" + stats.errors());
		}
		var key = new WorkloadKey(WorkloadProfile.INGEST, OperationFamily.MUTATION);
		var measurement = stats.snapshot(durationNanos).get(key);
		if (measurement == null || measurement.throughput() <= 0.0d) {
			throw new IllegalStateException("Isolated INGEST baseline made no progress");
		}
		return measurement.throughput();
	}

	private static void runWorker(RunControl control,
			PressureTracker pressure,
			WorkloadKey key,
			Pacer pacer,
			ThrowingOperation operation) {
		control.ready();
		try {
			control.awaitStart();
			while (!control.stop().get()) {
				pacer.awaitNext(control.stop());
				long started = System.nanoTime();
				Outcome outcome;
				String detail = null;
				try {
					operation.run();
					outcome = Outcome.SUCCESS;
				} catch (Throwable failure) {
					Throwable unwrapped = unwrap(failure);
					outcome = classify(unwrapped);
					detail = describe(unwrapped);
				}
				if (control.measuring().get()) {
					control.stats().record(key, outcome, System.nanoTime() - started, detail);
					if (outcome == Outcome.SUCCESS) {
						pressure.record(key.profile());
					}
				} else if (outcome == Outcome.ERROR && !control.stop().get()) {
					control.stats().recordWarmupError(key, detail);
				}
			}
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private static void sampleLoop(EmbeddedConnection connection,
			BenchmarkMeterRegistry meterRegistry,
			RunControl control,
			Observation observation,
			Options options) {
		control.ready();
		try {
			control.awaitStart();
			while (!control.stop().get()) {
				if (control.measuring().get()) {
					observation.sample(connection, meterRegistry, options.databaseName());
				}
				LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(options.sampleMicros()));
				if (Thread.interrupted()) {
					throw new InterruptedException();
				}
			}
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private static void pressureLoop(EmbeddedConnection connection,
			RunControl control,
			PressureTracker pressure,
			Options options) {
		if (options.pressureSeconds() <= 0) {
			pressure.afterPressure();
			return;
		}
		try {
			long waitNanos = TimeUnit.SECONDS.toNanos(Math.max(1, options.measureSeconds() / 3));
			sleepUntil(control.measurementStartedNanos() + waitNanos, control.stop());
			pressure.duringPressure();
			long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(options.pressureSeconds());
			while (!control.stop().get() && System.nanoTime() < end) {
				connection.getScheduler().setStoragePressure(true);
				Thread.sleep(50L);
			}
			connection.getScheduler().setStoragePressure(false);
			pressure.afterPressure();
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
		} finally {
			connection.getScheduler().setStoragePressure(false);
		}
	}

	private static Result finishResult(Instant started,
			Instant finished,
			Options options,
			String fingerprint,
			RunSnapshot run,
			boolean shutdownClean,
			long nativeLeaks) {
		var profiles = new EnumMap<WorkloadProfile, WorkloadBenchmarkSelector.ProfileMeasurement>(WorkloadProfile.class);
		var checks = new ArrayList<Check>();
		long leakedResources = saturatingAdd(run.resources().leakedResources(), nativeLeaks);
		for (var profile : WorkloadBenchmarkSelector.ALL_PROFILES) {
			double throughput = run.operations().entrySet().stream()
					.filter(entry -> entry.getKey().profile() == profile)
					.mapToDouble(entry -> entry.getValue().throughput())
					.sum();
			long endToEndP99 = run.operations().entrySet().stream()
					.filter(entry -> entry.getKey().profile() == profile)
					.mapToLong(entry -> entry.getValue().p99Nanos())
					.max().orElse(0L);
			long deadlines = run.operations().entrySet().stream()
					.filter(entry -> entry.getKey().profile() == profile)
					.mapToLong(entry -> entry.getValue().deadlines()).sum();
			long errors = run.operations().entrySet().stream()
					.filter(entry -> entry.getKey().profile() == profile)
					.mapToLong(entry -> entry.getValue().errors()).sum();
			long queueP99 = run.schedulerMetrics().entrySet().stream()
					.filter(entry -> entry.getKey().profile() == profile)
					.mapToLong(entry -> entry.getValue().queueP99Nanos()).max().orElse(0L);
			long executionP99 = run.schedulerMetrics().entrySet().stream()
					.filter(entry -> entry.getKey().profile() == profile)
					.mapToLong(entry -> entry.getValue().executionP99Nanos()).max().orElse(0L);
			long rejections = run.schedulerMetrics().entrySet().stream()
					.filter(entry -> entry.getKey().profile() == profile)
					.mapToLong(entry -> entry.getValue().rejections()).sum();
			long cancellations = run.schedulerMetrics().entrySet().stream()
					.filter(entry -> entry.getKey().profile() == profile)
					.mapToLong(entry -> entry.getValue().cancellations()).sum();
			long quantums = run.schedulerMetrics().entrySet().stream()
					.filter(entry -> entry.getKey().profile() == profile)
					.mapToLong(entry -> entry.getValue().quantums()).sum();
			boolean sloPassed = profileSlo(profile, throughput, endToEndP99, deadlines, errors, run, options);
			boolean relevant = switch (profile) {
				case CONTROL, LATENCY, ANALYTICAL, INGEST, CDC -> true;
				case BATCH, PHYSICAL_MAINTENANCE -> false;
			};
			profiles.put(profile, new WorkloadBenchmarkSelector.ProfileMeasurement(
					throughput, queueP99, executionP99, endToEndP99, rejections, cancellations,
					quantums, relevant, sloPassed));
			checks.add(new Check("profile_" + metricName(profile), sloPassed,
					"throughput=" + format(throughput) + ", p99_ms=" + formatMillis(endToEndP99)
							+ ", deadlines=" + deadlines + ", errors=" + errors));
		}
		checks.add(new Check("resources_drained", leakedResources == 0L,
				"logical=" + run.resources().leakedResources() + ", native=" + nativeLeaks));
		checks.add(new Check("storage_pressure_observed",
				!run.pressure().injected() || run.observation().maximumStoragePressure() > 0L,
				"observed=" + run.observation().maximumStoragePressure()));
		checks.add(new Check("shutdown_clean", shutdownClean, "shutdown_clean=" + shutdownClean));
		checks.add(new Check("unexpected_errors", run.errors().isEmpty(),
				"unexpected_error_count=" + run.errors().size()));
		boolean runChecksPassed = checks.stream().allMatch(Check::passed);
		var measurement = new WorkloadBenchmarkSelector.CandidateMeasurement(
				options.candidate(), fingerprint, comparisonFingerprint(options), options.storageLabel(),
				options.seed(), options.enforce(), runChecksPassed, profiles,
				run.observation().maximumCdcLag(), run.observation().maximumRetainedSnapshots(),
				run.observation().maximumStoragePressure(), leakedResources);
		return new Result(RESULT_SCHEMA, started, finished, options, fingerprint, run, measurement,
				List.copyOf(checks), shutdownClean, nativeLeaks);
	}

	private static boolean profileSlo(WorkloadProfile profile,
			double throughput,
			long p99Nanos,
			long deadlines,
			long errors,
			RunSnapshot run,
			Options options) {
		if (throughput <= 0.0d || errors != 0L) {
			return false;
		}
		return switch (profile) {
			case CONTROL -> p99Nanos > 0L && p99Nanos < FALLBACK_P99_LIMIT_NANOS;
			case LATENCY -> deadlines == 0L && p99Nanos > 0L
					&& p99Nanos < TimeUnit.MILLISECONDS.toNanos(LATENCY_DEADLINE_MILLIS);
			case ANALYTICAL -> p99Nanos > 0L && p99Nanos < FALLBACK_P99_LIMIT_NANOS;
			case INGEST -> options.ingestIsolatedBaseline() <= 0.0d
					|| throughput >= options.ingestIsolatedBaseline() * 0.95d;
			case CDC -> run.observation().cdcLagObserved()
					&& run.observation().maximumCdcLag() <= options.cdcLagLimit();
			case BATCH -> !run.pressure().injected()
					|| (run.observation().maximumStoragePressure() > 0L
							&& run.pressure().batchAfterPressure() > 0L);
			case PHYSICAL_MAINTENANCE -> true;
		};
	}

	private static Map<WorkloadKey, SchedulerMetricValues> snapshotSchedulerMetrics(
			BenchmarkMeterRegistry registry) {
		var mutable = new LinkedHashMap<WorkloadKey, MutableSchedulerMetricValues>();
		for (Meter meter : registry.getMeters()) {
			String profileTag = meter.getId().getTag("profile");
			String operationTag = meter.getId().getTag("operation");
			if (profileTag == null || operationTag == null) {
				continue;
			}
			WorkloadKey key;
			try {
				key = new WorkloadKey(parseProfile(profileTag),
						OperationFamily.valueOf(operationTag.toUpperCase(Locale.ROOT)));
			} catch (IllegalArgumentException unknownTag) {
				continue;
			}
			var value = mutable.computeIfAbsent(key, ignored -> new MutableSchedulerMetricValues());
			switch (meter.getId().getName()) {
				case "rockserver.workload.queue.wait" -> {
					if (meter instanceof Timer timer) {
						value.queueP99Nanos = Math.max(value.queueP99Nanos, timerP99(timer));
					}
				}
				case "rockserver.workload.execution" -> {
					if (meter instanceof Timer timer) {
						value.executionP99Nanos = Math.max(value.executionP99Nanos, timerP99(timer));
					}
				}
				case "rockserver.workload.rejections" -> {
					if (meter instanceof Counter counter) {
						value.rejections += Math.round(counter.count());
					}
				}
				case "rockserver.workload.cancellations" -> {
					if (meter instanceof Counter counter) {
						value.cancellations += Math.round(counter.count());
					}
				}
				case "rockserver.workload.quantums" -> {
					if (meter instanceof Counter counter) {
						value.quantums += Math.round(counter.count());
					}
				}
				default -> {
				}
			}
		}
		var result = new LinkedHashMap<WorkloadKey, SchedulerMetricValues>();
		mutable.entrySet().stream().sorted(Map.Entry.comparingByKey(WORKLOAD_KEY_ORDER))
				.forEach(entry -> result.put(entry.getKey(), entry.getValue().snapshot()));
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

	private static WorkloadProfile parseProfile(String value) {
		return switch (value) {
			case "control" -> WorkloadProfile.CONTROL;
			case "latency" -> WorkloadProfile.LATENCY;
			case "analytical" -> WorkloadProfile.ANALYTICAL;
			case "ingest" -> WorkloadProfile.INGEST;
			case "cdc" -> WorkloadProfile.CDC;
			case "batch" -> WorkloadProfile.BATCH;
			case "physical_maintenance" -> WorkloadProfile.PHYSICAL_MAINTENANCE;
			default -> throw new IllegalArgumentException("Unknown workload profile tag: " + value);
		};
	}

	private static ResourceSnapshot awaitDrain(EmbeddedConnection connection) throws InterruptedException {
		long started = System.nanoTime();
		long deadline = started + TimeUnit.SECONDS.toNanos(RESOURCE_DRAIN_SECONDS);
		ResourceSnapshot snapshot;
		do {
			snapshot = resourceSnapshot(connection, System.nanoTime() - started);
			if (snapshot.leakedResources() == 0L) {
				return snapshot;
			}
			Thread.sleep(10L);
		} while (System.nanoTime() < deadline);
		return resourceSnapshot(connection, System.nanoTime() - started);
	}

	private static ResourceSnapshot resourceSnapshot(EmbeddedConnection connection, long drainNanos) {
		var scheduler = connection.getScheduler();
		var database = connection.getInternalDB();
		int queued = 0;
		int active = 0;
		for (var profile : WorkloadBenchmarkSelector.ALL_PROFILES) {
			queued += scheduler.queuedTasks(profile);
			active += scheduler.activeTasks(profile);
		}
		return new ResourceSnapshot(queued,
				active,
				database.getPendingOpsCount(),
				database.getOpenTransactionsCount(),
				database.getOpenIteratorsCount(),
				database.getActiveRangeCursorCount(),
				database.getRetainedRangeSnapshotCount(),
				TimeUnit.NANOSECONDS.toMillis(drainNanos));
	}

	private static void writeReports(Path root, Result result) throws IOException {
		Files.writeString(root.resolve("results.json"), toJson(result),
				StandardOpenOption.CREATE_NEW);
		Files.writeString(root.resolve("results.md"), toMarkdown(result),
				StandardOpenOption.CREATE_NEW);
		WorkloadBenchmarkSelection.writeSelectionInput(root.resolve("selection-input.properties"),
				result.measurement());
	}

	private static String toJson(Result result) {
		var json = new StringBuilder(16_384);
		json.append("{\n  \"schema\": ");
		appendJsonString(json, result.schema());
		json.append(",\n  \"started\": ");
		appendJsonString(json, result.started().toString());
		json.append(",\n  \"finished\": ");
		appendJsonString(json, result.finished().toString());
		json.append(",\n  \"candidate\": ").append(result.options().candidate())
				.append(",\n  \"storage_label\": ");
		appendJsonString(json, result.options().storageLabel());
		json.append(",\n  \"cache_state\": ");
		appendJsonString(json, result.options().cacheState());
		json.append(",\n  \"seed\": ").append(result.options().seed())
				.append(",\n  \"enforced_hardware_run\": ").append(result.options().enforce())
				.append(",\n  \"dataset_fingerprint\": ");
		appendJsonString(json, result.datasetFingerprint());
		json.append(",\n  \"comparison_fingerprint\": ");
		appendJsonString(json, result.measurement().comparisonFingerprint());
		json.append(",\n  \"duration_ms\": ")
				.append(TimeUnit.NANOSECONDS.toMillis(result.run().durationNanos()))
				.append(",\n  \"profiles\": {");
		for (int index = 0; index < WorkloadBenchmarkSelector.ALL_PROFILES.size(); index++) {
			var profile = WorkloadBenchmarkSelector.ALL_PROFILES.get(index);
			var value = result.measurement().profiles().get(profile);
			if (index > 0) {
				json.append(',');
			}
			json.append("\n    \"").append(metricName(profile)).append("\": {")
					.append("\"throughput\": ").append(format(value.throughput()))
					.append(", \"queue_p99_nanos\": ").append(value.queueP99Nanos())
					.append(", \"execution_p99_nanos\": ").append(value.executionP99Nanos())
					.append(", \"end_to_end_p99_nanos\": ").append(value.endToEndP99Nanos())
					.append(", \"maximum_queue_depth\": ")
					.append(result.run().observation().maximumQueued().get(profile))
					.append(", \"maximum_active\": ")
					.append(result.run().observation().maximumActive().get(profile))
					.append(", \"rejections\": ").append(value.rejections())
					.append(", \"cancellations\": ").append(value.cancellations())
					.append(", \"quantum_count\": ").append(value.quantumCount())
					.append(", \"slo_passed\": ").append(value.sloPassed()).append('}');
		}
		json.append("\n  },\n  \"families\": [");
		var familyKeys = new ArrayList<WorkloadKey>();
		familyKeys.addAll(result.run().operations().keySet());
		for (var key : result.run().schedulerMetrics().keySet()) {
			if (!familyKeys.contains(key)) {
				familyKeys.add(key);
			}
		}
		familyKeys.sort(WORKLOAD_KEY_ORDER);
		int familyIndex = 0;
		for (var key : familyKeys) {
			var operation = result.run().operations().getOrDefault(key,
					new OperationMeasurement(0L, 0L, 0.0d, 0L, 0L, 0L, 0L, 0L));
			var scheduler = result.run().schedulerMetrics().getOrDefault(key,
					new SchedulerMetricValues(0L, 0L, 0L, 0L, 0L));
			if (familyIndex++ > 0) {
				json.append(',');
			}
			json.append("\n    {\"profile\": \"").append(metricName(key.profile()))
					.append("\", \"family\": \"").append(metricName(key.family()))
					.append("\", \"throughput\": ").append(format(operation.throughput()))
					.append(", \"queue_p99_nanos\": ").append(scheduler.queueP99Nanos())
					.append(", \"execution_p99_nanos\": ").append(scheduler.executionP99Nanos())
					.append(", \"end_to_end_p99_nanos\": ").append(operation.p99Nanos())
					.append(", \"rejections\": ").append(scheduler.rejections())
					.append(", \"cancellations\": ").append(scheduler.cancellations())
					.append(", \"quantum_count\": ").append(scheduler.quantums())
					.append(", \"errors\": ").append(operation.errors()).append('}');
		}
		json.append("\n  ],\n  \"maximum_cdc_lag\": ").append(result.measurement().maximumCdcLag())
				.append(",\n  \"cdc_lag_observed\": ").append(result.run().observation().cdcLagObserved())
				.append(",\n  \"maximum_retained_snapshots\": ").append(result.measurement().maximumRetainedSnapshots())
				.append(",\n  \"maximum_storage_pressure\": ").append(result.measurement().maximumStoragePressure())
				.append(",\n  \"batch_after_pressure\": ").append(result.run().pressure().batchAfterPressure())
				.append(",\n  \"physical_during_pressure\": ").append(result.run().pressure().physicalDuringPressure())
				.append(",\n  \"resources_after_drain\": {\"queued\": ").append(result.run().resources().queued())
				.append(", \"active\": ").append(result.run().resources().active())
				.append(", \"pending\": ").append(result.run().resources().pending())
				.append(", \"transactions\": ").append(result.run().resources().transactions())
				.append(", \"iterators\": ").append(result.run().resources().iterators())
				.append(", \"range_cursors\": ").append(result.run().resources().rangeCursors())
				.append(", \"retained_snapshots\": ").append(result.run().resources().retainedSnapshots())
				.append(", \"drain_ms\": ").append(result.run().resources().drainMillis()).append("},")
				.append("\n  \"native_handle_leaks\": ").append(result.nativeLeaks())
				.append(",\n  \"shutdown_clean\": ").append(result.shutdownClean())
				.append(",\n  \"workload_checks_passed\": ").append(result.acceptancePassed())
				.append(",\n  \"hardware_acceptance_passed\": ")
				.append(result.measurement().hardwareAcceptancePassed())
				.append(",\n  \"errors\": [");
		for (int index = 0; index < result.run().errors().size(); index++) {
			if (index > 0) {
				json.append(',');
			}
			json.append("\n    ");
			appendJsonString(json, result.run().errors().get(index));
		}
		json.append("\n  ],\n  \"checks\": [");
		for (int index = 0; index < result.checks().size(); index++) {
			var check = result.checks().get(index);
			if (index > 0) {
				json.append(',');
			}
			json.append("\n    {\"name\": ");
			appendJsonString(json, check.name());
			json.append(", \"passed\": ").append(check.passed()).append(", \"detail\": ");
			appendJsonString(json, check.detail());
			json.append('}');
		}
		json.append("\n  ]\n}\n");
		return json.toString();
	}

	private static String toMarkdown(Result result) {
		var markdown = new StringBuilder(8_192);
		markdown.append("# Seven-profile Rockserver workload benchmark\n\n")
				.append("- Candidate: `").append(result.options().candidate()).append("`\n")
				.append("- Storage: `").append(result.options().storageLabel()).append("`\n")
				.append("- Cache state: `").append(result.options().cacheState()).append("`\n")
				.append("- Enforced hardware run: `").append(result.options().enforce()).append("`\n")
				.append("- Seed: `").append(result.options().seed()).append("`\n")
				.append("- Dataset: `").append(result.datasetFingerprint()).append("`\n")
				.append("- Comparison shape: `").append(result.measurement().comparisonFingerprint())
				.append("`\n\n")
				.append("| Profile | Throughput/s | Queue p99 ms | Execution p99 ms | End-to-end p99 ms | Max queued | Max active | Rejected | Cancelled | Quantums | SLO |\n")
				.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n");
		for (var profile : WorkloadBenchmarkSelector.ALL_PROFILES) {
			var value = result.measurement().profiles().get(profile);
			markdown.append('|').append(profile).append('|').append(format(value.throughput()))
					.append('|').append(formatMillis(value.queueP99Nanos()))
					.append('|').append(formatMillis(value.executionP99Nanos()))
					.append('|').append(formatMillis(value.endToEndP99Nanos()))
					.append('|').append(result.run().observation().maximumQueued().get(profile))
					.append('|').append(result.run().observation().maximumActive().get(profile))
					.append('|').append(value.rejections()).append('|').append(value.cancellations())
					.append('|').append(value.quantumCount()).append('|')
					.append(value.sloPassed() ? "PASS" : "FAIL").append("|\n");
		}
		markdown.append("\n## Pressure and resources\n\n")
				.append("- Maximum CDC lag: `").append(result.measurement().maximumCdcLag()).append("`\n")
				.append("- CDC lag meter observed: `").append(result.run().observation().cdcLagObserved()).append("`\n")
				.append("- Maximum retained snapshots: `").append(result.measurement().maximumRetainedSnapshots()).append("`\n")
				.append("- Storage pressure observed: `").append(result.measurement().maximumStoragePressure()).append("`\n")
				.append("- BATCH completions after pressure: `").append(result.run().pressure().batchAfterPressure()).append("`\n")
				.append("- PHYSICAL completions during pressure: `").append(result.run().pressure().physicalDuringPressure()).append("`\n")
				.append("- Leaked resources: `").append(result.measurement().leakedResources()).append("`\n\n")
				.append("## Acceptance\n\n");
		for (var check : result.checks()) {
			markdown.append("- [").append(check.passed() ? 'x' : ' ').append("] `")
					.append(check.name()).append("`: ").append(check.detail()).append('\n');
		}
		if (!result.run().errors().isEmpty()) {
			markdown.append("\n## Unexpected errors (first ").append(MAX_ERRORS).append(")\n\n");
			for (String error : result.run().errors()) {
				markdown.append("- `").append(error.replace("`", "\\`")).append("`\n");
			}
		}
		markdown.append("\nWorkload checks: **").append(result.acceptancePassed() ? "PASS" : "FAIL")
				.append("**. Hardware acceptance: **")
				.append(result.measurement().hardwareAcceptancePassed() ? "PASS" : "NOT PASSED")
				.append("**.\n");
		return markdown.toString();
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
				""".formatted(escapeHocon(options.databaseName()), options.candidate(), options.candidate(),
				options.spinning(), options.directIo(), escapeHocon(options.writeBufferSize()));
	}

	private static String datasetMarker(Options options, String fingerprint, String configText) {
		return """
				schema=%s
				fingerprint=%s
				config-sha256=%s
				candidate=%d
				preload-keys=%d
				value-bytes=%d
				seed=%d
				""".formatted(DATASET_SCHEMA, fingerprint, sha256(configText), options.candidate(),
				options.preloadKeys(), options.valueBytes(), options.seed());
	}

	private static String datasetFingerprint(Options options) {
		return sha256(DATASET_SCHEMA + "\nseed=" + options.seed() + "\npreload-keys=" + options.preloadKeys()
				+ "\npreload-flush-keys=" + options.preloadFlushKeys()
				+ "\nvalue-bytes=" + options.valueBytes());
	}

	private static String comparisonFingerprint(Options options) {
		return sha256("rockserver-seven-profile-comparison-v1"
				+ "\ndataset=" + datasetFingerprint(options)
				+ "\ndatabase-name=" + options.databaseName()
				+ "\ncache-state=" + options.cacheState()
				+ "\nrange-width=" + options.rangeWidth()
				+ "\nwrite-key-space=" + options.writeKeySpace()
				+ "\nwarmup-seconds=" + options.warmupSeconds()
				+ "\nmeasure-seconds=" + options.measureSeconds()
				+ "\npressure-seconds=" + options.pressureSeconds()
				+ "\ncdc-lag-limit=" + options.cdcLagLimit()
				+ "\nworkers=" + options.controlWorkers() + ',' + options.latencyWorkers() + ','
				+ options.analyticalWorkers() + ',' + options.ingestWorkers() + ',' + options.cdcWorkers() + ','
				+ options.batchWorkers() + ',' + options.physicalWorkers() + ',' + options.cancellationWorkers()
				+ "\nrates=" + options.controlRate() + ',' + options.latencyRate() + ',' + options.analyticalRate()
				+ ',' + options.ingestRate() + ',' + options.cdcRate() + ',' + options.batchRate() + ','
				+ options.physicalRate() + ',' + options.cancellationRate()
				+ "\ncdc-max-events=" + options.cdcMaxEvents()
				+ "\nsample-micros=" + options.sampleMicros()
				+ "\nmax-latency-samples=" + options.maxLatencySamples()
				+ "\nwrite-buffer-size=" + options.writeBufferSize()
				+ "\ndirect-io=" + options.directIo()
				+ "\nspinning=" + options.spinning());
	}

	private static Map<String, String> readKeyValues(Path input) throws IOException {
		var values = new LinkedHashMap<String, String>();
		for (String line : Files.readAllLines(input)) {
			int equals = line.indexOf('=');
			if (equals > 0) {
				values.put(line.substring(0, equals), line.substring(equals + 1));
			}
		}
		return values;
	}

	private static String sha256(String value) {
		try {
			return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
					.digest(value.getBytes(StandardCharsets.UTF_8)));
		} catch (NoSuchAlgorithmException impossible) {
			throw new IllegalStateException(impossible);
		}
	}

	private static byte[][] writeValues(Options options) {
		byte[][] values = new byte[64][];
		for (int index = 0; index < values.length; index++) {
			values[index] = valueBytes(options.valueBytes(), options.seed() + index);
		}
		return values;
	}

	private static byte[] valueBytes(int size, long seed) {
		byte[] value = new byte[size];
		long state = seed;
		for (int index = 0; index < size; index++) {
			state ^= state << 13;
			state ^= state >>> 7;
			state ^= state << 17;
			value[index] = (byte) state;
		}
		return value;
	}

	private static List<Keys> deterministicKeys(int bound, int count, long seed) {
		var random = new SplittableRandom(seed);
		var keys = new ArrayList<Keys>(count);
		for (int index = 0; index < count; index++) {
			keys.add(key(random.nextInt(bound)));
		}
		return List.copyOf(keys);
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Outcome classify(Throwable failure) {
		if (failure instanceof CancellationException) {
			return Outcome.CANCELLED;
		}
		if (failure instanceof RocksDBException rocks) {
			return switch (rocks.getErrorUniqueId()) {
				case READ_DEADLINE_EXCEEDED -> Outcome.DEADLINE;
				case SERVER_OVERLOADED -> Outcome.REJECTED;
				default -> Outcome.ERROR;
			};
		}
		return Outcome.ERROR;
	}

	private static Throwable unwrap(Throwable failure) {
		Throwable current = failure;
		while ((current instanceof java.util.concurrent.CompletionException
				|| current instanceof java.util.concurrent.ExecutionException)
				&& current.getCause() != null) {
			current = current.getCause();
		}
		return current;
	}

	private static String describe(Throwable failure) {
		String message = failure.getMessage();
		return failure.getClass().getSimpleName() + (message == null ? "" : ": " + message);
	}

	private static int workerCount(Options options) {
		return options.controlWorkers() + options.latencyWorkers() + options.analyticalWorkers()
				+ options.ingestWorkers() + options.cdcWorkers() + options.batchWorkers()
				+ options.physicalWorkers() + options.cancellationWorkers() + 1;
	}

	private static void sleepPhase(int seconds, AtomicBoolean stop) throws InterruptedException {
		long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
		sleepUntil(end, stop);
	}

	private static void sleepUntil(long endNanos, AtomicBoolean stop) throws InterruptedException {
		while (!stop.get()) {
			long remaining = endNanos - System.nanoTime();
			if (remaining <= 0L) {
				return;
			}
			TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(100L)));
		}
	}

	private static long awaitNativeLeakDetection(long before) throws InterruptedException {
		for (int attempt = 0; attempt < 3; attempt++) {
			System.gc();
			Thread.sleep(100L);
		}
		return Math.max(0L, RocksLeakDetector.detectedLeakCount() - before);
	}

	private static Exception rethrow(Throwable failure) {
		return failure instanceof Exception exception ? exception : new RuntimeException(failure);
	}

	private static long saturatingAdd(long left, long right) {
		return right >= Long.MAX_VALUE - left ? Long.MAX_VALUE : left + right;
	}

	private static boolean contains(String[] args, String expected) {
		return Arrays.asList(args).contains(expected);
	}

	private static Map<String, String> parseArguments(String[] args) {
		var values = new LinkedHashMap<String, String>();
		for (String argument : args) {
			if (!argument.startsWith("--") || !argument.contains("=")) {
				throw new IllegalArgumentException("Expected --name=value, got: " + argument);
			}
			int equals = argument.indexOf('=');
			String old = values.put(argument.substring(2, equals), argument.substring(equals + 1));
			if (old != null) {
				throw new IllegalArgumentException("Duplicate option: " + argument.substring(2, equals));
			}
		}
		return values;
	}

	private static void printCandidates(int minimum, int maximum) {
		List<Integer> candidates = WorkloadBenchmarkSelector.powersOfTwo(minimum, maximum);
		System.out.println("{\"schema\":\"rockserver-workload-candidates-v1\",\"candidates\":"
				+ candidates + "}");
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
		if (value.equalsIgnoreCase("true")) {
			return true;
		}
		if (value.equalsIgnoreCase("false")) {
			return false;
		}
		throw new IllegalArgumentException("--" + name + " must be true or false");
	}

	private static String metricName(Enum<?> value) {
		return value.name().toLowerCase(Locale.ROOT);
	}

	private static String format(double value) {
		return String.format(Locale.ROOT, "%.3f", value);
	}

	private static String formatMillis(long nanos) {
		return format(nanos / 1_000_000d);
	}

	private static String escapeHocon(String value) {
		return value.replace("\\", "\\\\").replace("\"", "\\\"");
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
				default -> json.append(character);
			}
		}
		json.append('"');
	}

	private static void printUsage() {
		System.out.println("""
				Generate powers-of-two candidates:
				  SevenProfileWorkloadBenchmark --print-candidates=true --candidate-min=4 --candidate-max=64

				Prepare one reusable dataset per candidate, close it, then evict the host page cache:
				  SevenProfileWorkloadBenchmark --root=/mnt/bench/candidate-8 --candidate=8 \\
				    --storage-label=hdd-zfs --prepare-only=true

				Run the cold-cache candidate:
				  SevenProfileWorkloadBenchmark --root=/mnt/bench/candidate-8 --candidate=8 \\
				    --storage-label=hdd-zfs --reuse-prepared=true --cache-state=cold --enforce=true \\
				    --ingest-isolated-baseline=10000

				Use a separately prepared root with --ingest-baseline-only=true to produce the matching
				ingest-baseline.properties value before the mixed candidate runs.

				Important options (defaults): preload-keys=1000000 preload-flush-keys=50000
				value-bytes=256 range-width=4096 write-key-space=65536 seed=5931033225068892758
				warmup-seconds=15 measure-seconds=60 pressure-seconds=5 cdc-lag-limit=100000
				control-workers=1 latency-workers=8 analytical-workers=1 ingest-workers=4
				cdc-workers=1 batch-workers=2 physical-workers=1 cancellation-workers=1
				control-rate=50 latency-rate=0 analytical-rate=10 ingest-rate=1000 cdc-rate=50
				batch-rate=100 physical-rate=1 cancellation-rate=25 direct-io=false spinning=false

				The runner never drops caches itself. --cache-state=cold is an operator assertion recorded
				in the output. CI may compile and unit-test the selector but must not claim hardware acceptance.
				""");
	}

	@FunctionalInterface
	private interface ThrowingOperation {
		void run() throws Exception;
	}

	private enum Outcome {
		SUCCESS, DEADLINE, REJECTED, CANCELLED, ERROR
	}

	private enum PressurePhase {
		BEFORE, DURING, AFTER
	}

	private record WorkloadKey(WorkloadProfile profile, OperationFamily family) {
	}

	private record PreparedDataset(EmbeddedConnection connection, long columnId, String fingerprint) {
	}

	private record OperationMeasurement(long completed,
			long successes,
			double throughput,
			long p99Nanos,
			long deadlines,
			long rejections,
			long cancellations,
			long errors) {
	}

	private record SchedulerMetricValues(long queueP99Nanos,
			long executionP99Nanos,
			long rejections,
			long cancellations,
			long quantums) {
	}

	private record ObservationSnapshot(Map<WorkloadProfile, Integer> maximumQueued,
			Map<WorkloadProfile, Integer> maximumActive,
			long maximumCdcLag,
			boolean cdcLagObserved,
			long maximumRetainedSnapshots,
			long maximumStoragePressure) {
	}

	private record PressureSnapshot(boolean injected,
			long batchBeforePressure,
			long batchDuringPressure,
			long batchAfterPressure,
			long physicalDuringPressure) {
	}

	private record ResourceSnapshot(int queued,
			int active,
			long pending,
			int transactions,
			int iterators,
			int rangeCursors,
			int retainedSnapshots,
			long drainMillis) {

		long leakedResources() {
			long total = saturatingAdd(queued, active);
			total = saturatingAdd(total, pending);
			total = saturatingAdd(total, transactions);
			total = saturatingAdd(total, iterators);
			total = saturatingAdd(total, rangeCursors);
			return saturatingAdd(total, retainedSnapshots);
		}
	}

	private record RunSnapshot(long durationNanos,
			Map<WorkloadKey, OperationMeasurement> operations,
			ObservationSnapshot observation,
			PressureSnapshot pressure,
			ResourceSnapshot resources,
			Map<WorkloadKey, SchedulerMetricValues> schedulerMetrics,
			List<String> errors) {
	}

	private record Check(String name, boolean passed, String detail) {
	}

	private record Result(String schema,
			Instant started,
			Instant finished,
			Options options,
			String datasetFingerprint,
			RunSnapshot run,
			WorkloadBenchmarkSelector.CandidateMeasurement measurement,
			List<Check> checks,
			boolean shutdownClean,
			long nativeLeaks) {

		boolean acceptancePassed() {
			return checks.stream().allMatch(Check::passed);
		}

		List<String> failedChecks() {
			return checks.stream().filter(check -> !check.passed())
					.map(check -> check.name() + " (" + check.detail() + ")").toList();
		}
	}

	private static final class RunControl {

		private final CountDownLatch ready;
		private final CountDownLatch start = new CountDownLatch(1);
		private final AtomicBoolean stop = new AtomicBoolean();
		private final AtomicBoolean measuring = new AtomicBoolean();
		private final RunStats stats;
		private volatile long measurementStartedNanos;
		private volatile long measurementStoppedNanos;

		private RunControl(int workerCount, RunStats stats) {
			this.ready = new CountDownLatch(workerCount);
			this.stats = stats;
		}

		private void ready() {
			ready.countDown();
		}

		private void awaitReady() throws InterruptedException {
			ready.await();
		}

		private void releaseWorkers() {
			start.countDown();
		}

		private void awaitStart() throws InterruptedException {
			start.await();
		}

		private void startMeasurement() {
			measurementStartedNanos = System.nanoTime();
			measuring.set(true);
		}

		private long stopMeasurement() {
			measuring.set(false);
			measurementStoppedNanos = System.nanoTime();
			return Math.max(1L, measurementStoppedNanos - measurementStartedNanos);
		}

		private AtomicBoolean stop() {
			return stop;
		}

		private AtomicBoolean measuring() {
			return measuring;
		}

		private RunStats stats() {
			return stats;
		}

		private long measurementStartedNanos() {
			return measurementStartedNanos;
		}
	}

	private static final class RunStats {

		private final int maxLatencySamples;
		private final ConcurrentHashMap<WorkloadKey, MutableOperationStats> operations = new ConcurrentHashMap<>();
		private final ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();
		private final AtomicInteger recordedErrors = new AtomicInteger();

		private RunStats(int maxLatencySamples) {
			this.maxLatencySamples = maxLatencySamples;
		}

		private void record(WorkloadKey key, Outcome outcome, long latencyNanos, String detail) {
			operations.computeIfAbsent(key, ignored -> new MutableOperationStats(maxLatencySamples))
					.record(outcome, latencyNanos);
			if (outcome == Outcome.ERROR) {
				recordError(key, detail);
			}
		}

		private void recordWarmupError(WorkloadKey key, String detail) {
			recordError(key, "warmup: " + detail);
		}

		private void recordError(WorkloadKey key, String detail) {
			if (recordedErrors.getAndIncrement() < MAX_ERRORS) {
				errors.add(key.profile() + "/" + key.family() + ": " + detail);
			}
		}

		private Map<WorkloadKey, OperationMeasurement> snapshot(long durationNanos) {
			var result = new LinkedHashMap<WorkloadKey, OperationMeasurement>();
			operations.entrySet().stream()
					.sorted(Map.Entry.comparingByKey(WORKLOAD_KEY_ORDER))
					.forEach(entry -> result.put(entry.getKey(), entry.getValue().snapshot(durationNanos)));
			return Map.copyOf(result);
		}

		private List<String> errors() {
			return List.copyOf(errors);
		}
	}

	private static final class MutableOperationStats {

		private final LongAdder completed = new LongAdder();
		private final LongAdder successes = new LongAdder();
		private final LongAdder deadlines = new LongAdder();
		private final LongAdder rejections = new LongAdder();
		private final LongAdder cancellations = new LongAdder();
		private final LongAdder errors = new LongAdder();
		private final LatencySamples samples;

		private MutableOperationStats(int maxLatencySamples) {
			this.samples = new LatencySamples(maxLatencySamples);
		}

		private void record(Outcome outcome, long latencyNanos) {
			completed.increment();
			switch (outcome) {
				case SUCCESS -> successes.increment();
				case DEADLINE -> deadlines.increment();
				case REJECTED -> rejections.increment();
				case CANCELLED -> cancellations.increment();
				case ERROR -> errors.increment();
			}
			samples.record(latencyNanos);
		}

		private OperationMeasurement snapshot(long durationNanos) {
			long[] sorted = samples.snapshot();
			return new OperationMeasurement(completed.sum(), successes.sum(),
					successes.sum() * 1_000_000_000d / durationNanos,
					percentile(sorted, 0.99d), deadlines.sum(), rejections.sum(),
					cancellations.sum(), errors.sum());
		}
	}

	private static final class MutableSchedulerMetricValues {

		private long queueP99Nanos;
		private long executionP99Nanos;
		private long rejections;
		private long cancellations;
		private long quantums;

		private SchedulerMetricValues snapshot() {
			return new SchedulerMetricValues(queueP99Nanos, executionP99Nanos, rejections,
					cancellations, quantums);
		}
	}

	private static final class LatencySamples {

		private final long[] samples;
		private final AtomicLong next = new AtomicLong();

		private LatencySamples(int capacity) {
			this.samples = new long[capacity];
		}

		private void record(long latencyNanos) {
			long ordinal = next.getAndIncrement();
			int index;
			if (ordinal < samples.length) {
				index = (int) ordinal;
			} else {
				long candidate = Long.remainderUnsigned(mix64(ordinal), ordinal + 1L);
				if (candidate >= samples.length) {
					return;
				}
				index = (int) candidate;
			}
			samples[index] = Math.max(0L, latencyNanos);
		}

		private long[] snapshot() {
			long[] copy = Arrays.copyOf(samples, (int) Math.min(samples.length, next.get()));
			Arrays.sort(copy);
			return copy;
		}
	}

	private static long mix64(long value) {
		long mixed = value + 0x9e3779b97f4a7c15L;
		mixed = (mixed ^ (mixed >>> 30)) * 0xbf58476d1ce4e5b9L;
		mixed = (mixed ^ (mixed >>> 27)) * 0x94d049bb133111ebL;
		return mixed ^ (mixed >>> 31);
	}

	/** Visible for deterministic correctness tests. */
	public static long percentile(long[] sorted, double percentile) {
		if (sorted.length == 0) {
			return 0L;
		}
		int index = (int) Math.ceil(percentile * sorted.length) - 1;
		return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
	}

	private static final class Observation {

		private final EnumMap<WorkloadProfile, AtomicInteger> maximumQueued = new EnumMap<>(WorkloadProfile.class);
		private final EnumMap<WorkloadProfile, AtomicInteger> maximumActive = new EnumMap<>(WorkloadProfile.class);
		private final AtomicLong maximumCdcLag = new AtomicLong();
		private final AtomicBoolean cdcLagObserved = new AtomicBoolean();
		private final AtomicLong maximumRetainedSnapshots = new AtomicLong();
		private final AtomicLong maximumStoragePressure = new AtomicLong();

		private Observation() {
			for (var profile : WorkloadBenchmarkSelector.ALL_PROFILES) {
				maximumQueued.put(profile, new AtomicInteger());
				maximumActive.put(profile, new AtomicInteger());
			}
		}

		private void sample(EmbeddedConnection connection,
				BenchmarkMeterRegistry meterRegistry,
				String databaseName) {
			var admission = connection.getScheduler().admissionSnapshot();
			for (var profile : WorkloadBenchmarkSelector.ALL_PROFILES) {
				maximumQueued.get(profile).accumulateAndGet(admission.queued().get(profile), Math::max);
				maximumActive.get(profile).accumulateAndGet(admission.active().get(profile), Math::max);
			}
			maximumRetainedSnapshots.accumulateAndGet(
					connection.getInternalDB().getRetainedRangeSnapshotCount(), Math::max);
			maximumStoragePressure.accumulateAndGet(admission.storagePressure() ? 1L : 0L, Math::max);
			var cdcLag = meterRegistry.find("rockserver.workload.cdc.lag")
					.tag("database", databaseName).gauge();
			if (cdcLag != null) {
				cdcLagObserved.set(true);
				maximumCdcLag.accumulateAndGet(Math.max(0L, Math.round(cdcLag.value())), Math::max);
			}
		}

		private ObservationSnapshot snapshot() {
			var queued = new EnumMap<WorkloadProfile, Integer>(WorkloadProfile.class);
			var active = new EnumMap<WorkloadProfile, Integer>(WorkloadProfile.class);
			for (var profile : WorkloadBenchmarkSelector.ALL_PROFILES) {
				queued.put(profile, maximumQueued.get(profile).get());
				active.put(profile, maximumActive.get(profile).get());
			}
			return new ObservationSnapshot(Map.copyOf(queued), Map.copyOf(active), maximumCdcLag.get(),
					cdcLagObserved.get(),
					maximumRetainedSnapshots.get(), maximumStoragePressure.get());
		}
	}

	private static final class PressureTracker {

		private final boolean injected;
		private final java.util.concurrent.atomic.AtomicReference<PressurePhase> phase =
				new java.util.concurrent.atomic.AtomicReference<>(PressurePhase.BEFORE);
		private final LongAdder batchBefore = new LongAdder();
		private final LongAdder batchDuring = new LongAdder();
		private final LongAdder batchAfter = new LongAdder();
		private final LongAdder physicalDuring = new LongAdder();

		private PressureTracker(boolean injected) {
			this.injected = injected;
		}

		private void duringPressure() {
			phase.set(PressurePhase.DURING);
		}

		private void afterPressure() {
			phase.set(PressurePhase.AFTER);
		}

		private void record(WorkloadProfile profile) {
			PressurePhase current = phase.get();
			if (profile == WorkloadProfile.BATCH) {
				switch (current) {
					case BEFORE -> batchBefore.increment();
					case DURING -> batchDuring.increment();
					case AFTER -> batchAfter.increment();
				}
			} else if (profile == WorkloadProfile.PHYSICAL_MAINTENANCE && current == PressurePhase.DURING) {
				physicalDuring.increment();
			}
		}

		private PressureSnapshot snapshot() {
			return new PressureSnapshot(injected, batchBefore.sum(), batchDuring.sum(), batchAfter.sum(),
					physicalDuring.sum());
		}
	}

	private static final class Pacer {

		private final long intervalNanos;
		private long nextNanos;

		private Pacer(long totalRate, int workers, int worker) {
			intervalNanos = totalRate <= 0L ? 0L
					: Math.max(1L, TimeUnit.SECONDS.toNanos(1L) * workers / totalRate);
			nextNanos = System.nanoTime() + (intervalNanos == 0L ? 0L : intervalNanos * worker / workers);
		}

		private void awaitNext(AtomicBoolean stop) throws InterruptedException {
			if (intervalNanos == 0L) {
				return;
			}
			nextNanos += intervalNanos;
			while (!stop.get()) {
				long remaining = nextNanos - System.nanoTime();
				if (remaining <= 0L) {
					return;
				}
				LockSupport.parkNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(1L)));
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

	private record Options(Path root,
			String databaseName,
			int candidate,
			String storageLabel,
			String cacheState,
			long seed,
			int preloadKeys,
			int preloadFlushKeys,
			int valueBytes,
			int rangeWidth,
			long writeKeySpace,
			int warmupSeconds,
			int measureSeconds,
			int pressureSeconds,
			long cdcLagLimit,
			double ingestIsolatedBaseline,
			int controlWorkers,
			int latencyWorkers,
			int analyticalWorkers,
			int ingestWorkers,
			int cdcWorkers,
			int batchWorkers,
			int physicalWorkers,
			int cancellationWorkers,
			long controlRate,
			long latencyRate,
			long analyticalRate,
			long ingestRate,
			long cdcRate,
			long batchRate,
			long physicalRate,
			long cancellationRate,
			long cdcMaxEvents,
			int sampleMicros,
			int maxLatencySamples,
			String writeBufferSize,
			boolean directIo,
			boolean spinning,
			boolean prepareOnly,
			boolean reusePrepared,
			boolean ingestBaselineOnly,
			boolean enforce) {

		private Options {
			Objects.requireNonNull(root, "root");
			Objects.requireNonNull(databaseName, "databaseName");
			Objects.requireNonNull(storageLabel, "storageLabel");
			Objects.requireNonNull(cacheState, "cacheState");
			if ((candidate & (candidate - 1)) != 0 || candidate < 4) {
				throw new IllegalArgumentException("candidate must be a power of two and at least four");
			}
			if (!List.of("hdd-zfs", "nvme", "ci-structural").contains(storageLabel)) {
				throw new IllegalArgumentException("storage-label must be hdd-zfs, nvme, or ci-structural");
			}
			if (!List.of("cold", "warm", "unknown").contains(cacheState)) {
				throw new IllegalArgumentException("cache-state must be cold, warm, or unknown");
			}
			if (preloadKeys < 1 || preloadFlushKeys < 1 || valueBytes < 1
					|| rangeWidth < 1 || rangeWidth > preloadKeys || writeKeySpace < 1L) {
				throw new IllegalArgumentException("dataset dimensions are invalid");
			}
			if (warmupSeconds < 0 || measureSeconds < 1 || pressureSeconds < 0
					|| (pressureSeconds > 0 && (long) pressureSeconds * 3L >= (long) measureSeconds * 2L)
					|| cdcLagLimit < 0L || !Double.isFinite(ingestIsolatedBaseline)
					|| ingestIsolatedBaseline < 0.0d) {
				throw new IllegalArgumentException("duration or CDC lag settings are invalid");
			}
			if (controlWorkers < 1 || latencyWorkers < 1 || analyticalWorkers < 1 || ingestWorkers < 1
					|| cdcWorkers < 1 || batchWorkers < 1 || physicalWorkers < 1 || cancellationWorkers < 1) {
				throw new IllegalArgumentException("every workload producer must have at least one worker");
			}
			if (controlRate < 0L || latencyRate < 0L || analyticalRate < 0L || ingestRate < 0L
					|| cdcRate < 0L || batchRate < 0L || physicalRate < 0L || cancellationRate < 0L
					|| cdcMaxEvents < 1L || sampleMicros < 1 || maxLatencySamples < 1) {
				throw new IllegalArgumentException("rate and sampling settings are invalid");
			}
			if (prepareOnly && reusePrepared) {
				throw new IllegalArgumentException("prepare-only and reuse-prepared are mutually exclusive");
			}
			if (ingestBaselineOnly && enforce) {
				throw new IllegalArgumentException("ingest-baseline-only does not run mixed-workload acceptance");
			}
			if (enforce && (!reusePrepared || !cacheState.equals("cold") || pressureSeconds <= 0
					|| storageLabel.equals("ci-structural") || ingestIsolatedBaseline <= 0.0d)) {
				throw new IllegalArgumentException("enforced hardware runs require a reused cold dataset, a hardware label, "
						+ "positive pressure duration, and a positive ingest-isolated-baseline");
			}
		}

		private static Options parse(Map<String, String> values) {
			String root = values.get("root");
			if (root == null || root.isBlank()) {
				throw new IllegalArgumentException("--root is required");
			}
			return new Options(Path.of(root),
					values.getOrDefault("database-name", "seven-profile-workload"),
					integer(values, "candidate", 8),
					values.getOrDefault("storage-label", "ci-structural"),
					values.getOrDefault("cache-state", "unknown"),
					longValue(values, "seed", 5_931_033_225_068_892_758L),
					integer(values, "preload-keys", 1_000_000),
					integer(values, "preload-flush-keys", 50_000),
					integer(values, "value-bytes", 256),
					integer(values, "range-width", 4_096),
					longValue(values, "write-key-space", 65_536L),
					integer(values, "warmup-seconds", 15),
					integer(values, "measure-seconds", 60),
					integer(values, "pressure-seconds", 5),
					longValue(values, "cdc-lag-limit", 100_000L),
					doubleValue(values, "ingest-isolated-baseline", 0.0d),
					integer(values, "control-workers", 1),
					integer(values, "latency-workers", 8),
					integer(values, "analytical-workers", 1),
					integer(values, "ingest-workers", 4),
					integer(values, "cdc-workers", 1),
					integer(values, "batch-workers", 2),
					integer(values, "physical-workers", 1),
					integer(values, "cancellation-workers", 1),
					longValue(values, "control-rate", 50L),
					longValue(values, "latency-rate", 0L),
					longValue(values, "analytical-rate", 10L),
					longValue(values, "ingest-rate", 1_000L),
					longValue(values, "cdc-rate", 50L),
					longValue(values, "batch-rate", 100L),
					longValue(values, "physical-rate", 1L),
					longValue(values, "cancellation-rate", 25L),
					longValue(values, "cdc-max-events", 4_096L),
					integer(values, "sample-micros", 250),
					integer(values, "max-latency-samples", 1_000_000),
					values.getOrDefault("write-buffer-size", "64MiB"),
					bool(values, "direct-io", false),
					bool(values, "spinning", false),
					bool(values, "prepare-only", false),
					bool(values, "reuse-prepared", false),
					bool(values, "ingest-baseline-only", false),
					bool(values, "enforce", false));
		}
	}
}
