package it.cavallium.rockserver.core.impl.benchmark;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RawScanEvent;
import it.cavallium.rockserver.core.common.RawSstToken;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.RocksLeakDetector;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
import reactor.core.publisher.Flux;

/**
 * Opt-in real-RocksDB acceptance workload for resumable raw-SST backfill under mixed service load.
 * The benchmark owns a new root marked on creation and never deletes or reuses an existing path.
 */
public final class MixedBackfillPressureBenchmark {

	static final String RESULT_SCHEMA = "rockserver-mixed-backfill-pressure-v2";
	private static final String COLUMN = "mixed-pressure";
	private static final String CDC = "mixed-pressure-cdc";
	private static final String MARKER = ".rockserver-mixed-pressure-benchmark";
	private static final int MAX_PRESSURED_BATCH_ACTIVE = 64;
	private static final RWScheduler.Pool[] POOLS = RWScheduler.Pool.values();

	private MixedBackfillPressureBenchmark() {
	}

	public static void main(String[] args) throws Exception {
		Result result = run(Options.parse(args));
		result.assertCorrect();
		result.write();
		System.out.print(result.toMarkdown());
	}

	public static Result run(Options options) throws Exception {
		return run(options, Provenance.captureStrict(options));
	}

	static Result runForTesting(Options options) throws Exception {
		return run(options, Provenance.testing());
	}

	private static Result run(Options options, Provenance provenance) throws Exception {
		options.validate();
		Path root = options.root().toAbsolutePath().normalize();
		if (Files.exists(root)) throw new IllegalArgumentException("Benchmark root already exists: " + root);
		Files.createDirectories(root);
		Files.writeString(root.resolve(MARKER), provenance.marker(), StandardOpenOption.CREATE_NEW);
		Path config = root.resolve("rockserver.conf");
		Files.writeString(config, configText(options), StandardOpenOption.CREATE_NEW);
		Path database = root.resolve("db");
		Keys[] keySpace = keys(options.preloadKeys());
		Set<RawSstToken> checkpoints = Collections.synchronizedSet(new HashSet<>());
		long leaksBefore = RocksLeakDetector.detectedLeakCount();
		long columnId;
		long firstRows;
		long resumedRows;
		try (var connection = new EmbeddedConnection(database, "mixed-pressure", config)) {
			var batch = connection.getSyncApi(RequestContext.batch());
			columnId = batch.createColumn(COLUMN,
					ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			preload(batch, columnId, options, keySpace);
			var cancelled = cancelAfterFirstCheckpoint(connection, columnId, checkpoints, root.resolve("checkpoint.txt"));
			firstRows = cancelled.rows();
			resumedRows = consumeScan(batch.scanRawResumable(columnId, 0, 1, Set.copyOf(checkpoints)),
					checkpoints, root.resolve("checkpoint.txt"), null);
			awaitDrain(connection);
		}
		if (firstRows + resumedRows != options.preloadKeys()) {
			throw new IllegalStateException("resumable scan conservation mismatch: "
					+ firstRows + "+" + resumedRows + " != " + options.preloadKeys());
		}
		List<String> persisted = Files.readAllLines(root.resolve("checkpoint.txt"));
		if (new HashSet<>(persisted).size() != checkpoints.size()) {
			throw new IllegalStateException("checkpoint file contains duplicate or missing tokens");
		}

		Result result;
		try (var reopened = new EmbeddedConnection(database, "mixed-pressure", config)) {
			var batch = reopened.getSyncApi(RequestContext.batch());
			long reopenedColumn = batch.getColumnId(COLUMN);
			long skippedRows = consumeScan(batch.scanRawResumable(reopenedColumn, 0, 1, Set.copyOf(checkpoints)),
					new HashSet<>(), null, null);
			if (skippedRows != 0L) throw new IllegalStateException("reopen did not skip completed SSTs: " + skippedRows);
			batch.cdcCreate(CDC, null, List.of(reopenedColumn), false, java.util.OptionalLong.empty());
			result = measure(reopened, reopenedColumn, options, provenance, keySpace,
					firstRows, resumedRows, checkpoints.size());
			awaitDrain(reopened);
		}
		long leaks = awaitLeaks(leaksBefore);
		if (leaks != 0L) throw new IllegalStateException("mixed pressure benchmark leaked " + leaks + " native handles");
		return result.withNativeLeaks(leaks);
	}

	private static Result measure(EmbeddedConnection connection,
			long columnId,
			Options options,
			Provenance provenance,
			Keys[] keySpace,
			long firstRows,
			long resumedRows,
			int checkpoints) throws Exception {
		var pressureCapWitness = exercisePressuredBatchCap(connection, options);
		BenchmarkProcessTelemetry.enableAllocationMeasurement();
		var stop = new AtomicBoolean();
		var ready = new CountDownLatch(options.writers() + options.latencyReaders() + 5);
		var start = new CountDownLatch(1);
		var backfillRows = new LongAdder();
		var backfillBatches = new LongAdder();
		var ingestWrites = new LongAdder();
		var latencyReads = new LongAdder();
		var cdcEvents = new LongAdder();
		var maintenance = new LongAdder();
		var pressureTransitions = new LongAdder();
		var lastProgress = new AtomicLong();
		var maximumGap = new AtomicLong();
		var maximumCdcLag = new AtomicLong();
		var maximumQueued = new AtomicInteger();
		var maximumParked = new AtomicInteger();
		var maximumOutstanding = new AtomicInteger();
		var latencies = new LatencySamples(options.maximumLatencySamples());
		var failures = Collections.synchronizedList(new ArrayList<Throwable>());
		int threads = options.writers() + options.latencyReaders() + 5;
		ExecutorService executor = Executors.newFixedThreadPool(threads,
				Thread.ofPlatform().name("mixed-pressure-", 0).factory());
		List<Future<?>> futures = new ArrayList<>();
		long[] deadline = new long[1];
		var peaks = new BenchmarkProcessTelemetry.PeakSampler();
		try (peaks) {
			futures.add(submit(executor, failures, () -> {
				ready.countDown(); await(start);
				var api = connection.getSyncApi(RequestContext.batch());
				while (!stop.get()) {
					consumeScan(api.scanRawResumable(columnId, 0, 1, Set.of()), new HashSet<>(), null, eventRows -> {
						backfillRows.add(eventRows); backfillBatches.increment(); lastProgress.set(System.nanoTime());
					});
				}
			}));
			for (int writer = 0; writer < options.writers(); writer++) {
				int lane = writer;
				futures.add(submit(executor, failures, () -> {
					ready.countDown(); await(start);
					var api = connection.getSyncApi(RequestContext.ingest());
					long sequence = lane;
					Buf value = Buf.wrap(value(options.valueBytes()));
					while (!stop.get()) {
						api.put(0L, columnId, keySpace[Math.floorMod((int) sequence, keySpace.length)], value,
								RequestType.none());
						ingestWrites.increment(); sequence += options.writers();
					}
				}));
			}
			for (int reader = 0; reader < options.latencyReaders(); reader++) {
				int lane = reader;
				futures.add(submit(executor, failures, () -> {
					ready.countDown(); await(start);
					var api = connection.getSyncApi(new RequestContext(WorkloadProfile.LATENCY,
							latencyDeadlineEpochMillis(System.currentTimeMillis(), options.measureDuration())));
					long sequence = lane;
					while (!stop.get()) {
						long before = System.nanoTime();
						api.get(0L, columnId, keySpace[Math.floorMod((int) sequence, keySpace.length)],
								RequestType.current());
						latencies.record(System.nanoTime() - before); latencyReads.increment();
						sequence += options.latencyReaders();
					}
				}));
			}
			futures.add(submit(executor, failures, () -> {
				ready.countDown(); await(start);
				var api = connection.getSyncApi(RequestContext.batch());
				while (!stop.get()) {
					try (var events = api.cdcPoll(CDC, null, options.cdcBatchSize())) {
						List<CDCEvent> page = events.toList();
						if (!page.isEmpty()) {
							api.cdcCommit(CDC, page.getLast().seq());
							cdcEvents.add(page.size());
						}
					}
				}
			}));
			futures.add(submit(executor, failures, () -> {
				ready.countDown(); await(start);
				var api = connection.getSyncApi(RequestContext.batch());
				while (!stop.get()) {
					api.flush(); maintenance.increment();
					api.compact(); maintenance.increment();
				}
			}));
			futures.add(submit(executor, failures, () -> {
				ready.countDown(); await(start);
				boolean pressure = false;
				long periodNanos = options.pressurePeriod().toNanos();
				long nextTransition = System.nanoTime();
				try {
					while (!stop.get()) {
						long remaining = nextTransition - System.nanoTime();
						if (remaining > 0L) {
							LockSupport.parkNanos(remaining);
							continue;
						}
						pressure = !pressure;
						connection.getScheduler().setStoragePressure(pressure);
						pressureTransitions.increment();
						nextTransition = System.nanoTime() + periodNanos;
					}
				} finally {
					connection.getScheduler().setStoragePressure(false);
				}
			}));
			futures.add(submit(executor, failures, () -> {
				ready.countDown(); await(start);
				lastProgress.set(System.nanoTime());
				long[][] poolTelemetry = new long[POOLS.length][RWScheduler.POOL_TELEMETRY_LENGTH];
				while (!stop.get()) {
					long now = System.nanoTime();
					long progress = lastProgress.get();
					maximumGap.accumulateAndGet(Math.max(0L, now - progress), Math::max);
					maximumCdcLag.accumulateAndGet(Math.max(0L, ingestWrites.sum() - cdcEvents.sum()), Math::max);
					for (var pool : POOLS) {
						long[] telemetry = poolTelemetry[pool.ordinal()];
						BenchmarkSchedulerTelemetry.copyPoolTelemetry(connection.getScheduler(), pool, telemetry);
						maximumQueued.accumulateAndGet(
								Math.toIntExact(telemetry[RWScheduler.POOL_TELEMETRY_QUEUED_TASKS]), Math::max);
						maximumParked.accumulateAndGet(
								Math.toIntExact(telemetry[RWScheduler.POOL_TELEMETRY_PARKED_TASKS]), Math::max);
						maximumOutstanding.accumulateAndGet(
								Math.toIntExact(telemetry[RWScheduler.POOL_TELEMETRY_OUTSTANDING_TASKS]), Math::max);
					}
					peaks.sample();
					LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
				}
			}));
			if (!ready.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("mixed workers did not become ready");
			peaks.reset();
			var processBefore = BenchmarkProcessTelemetry.processSnapshot();
			long started = System.nanoTime();
			deadline[0] = started + options.measureDuration().toNanos();
			start.countDown();
			while (System.nanoTime() < deadline[0] && failures.isEmpty()) LockSupport.parkNanos(100_000L);
			stop.set(true);
			connection.getScheduler().setStoragePressure(false);
			for (Future<?> future : futures) future.get(30, TimeUnit.SECONDS);
			long elapsed = System.nanoTime() - started;
			var process = BenchmarkProcessTelemetry.processSnapshot().minus(processBefore);
			if (!failures.isEmpty()) throw rethrow(failures.getFirst());
			awaitDrain(connection);
			long completed = backfillBatches.sum() + ingestWrites.sum() + latencyReads.sum() + cdcEvents.sum();
			long p99 = latencies.p99();
			return new Result(options, provenance, firstRows, resumedRows, checkpoints, elapsed,
					backfillRows.sum(), backfillBatches.sum(), ingestWrites.sum(), latencyReads.sum(), cdcEvents.sum(),
					maintenance.sum(), pressureTransitions.sum(), pressureCapWitness.maximumActive(),
					pressureCapWitness.capWitnesses(), maximumGap.get(), maximumCdcLag.get(), p99,
					completed == 0L ? 0.0d : process.cpuNanos() / (double) completed,
					completed == 0L ? 0.0d : process.allocatedBytes() / (double) completed,
					process.gcCollections(), process.gcMillis(), maximumQueued.get(), maximumParked.get(),
					maximumOutstanding.get(), peaks.peaks(), 0L, rootResult(options.root()));
		} finally {
			stop.set(true); start.countDown(); connection.getScheduler().setStoragePressure(false);
			executor.shutdownNow(); executor.awaitTermination(30, TimeUnit.SECONDS);
		}
	}

	/**
	 * Prove that the configured cap is exercised on this database's scheduler without adding a
	 * production counter or relying on sub-millisecond RocksDB quantum duration. All tasks enter
	 * after pressure is enabled and wait on one barrier, so repeated pool snapshots describe the
	 * same stable set of live pressured permits.
	 */
	private static PressureCapWitness exercisePressuredBatchCap(EmbeddedConnection connection,
			Options options) throws Exception {
		var scheduler = connection.getScheduler();
		int cap = options.pressuredBatchMaximumActive();
		int readTasks = Math.min(options.readWorkers(), cap / 2 + cap % 2);
		int writeTasks = cap - readTasks;
		if (writeTasks > options.writeWorkers()) {
			readTasks = Math.addExact(readTasks, writeTasks - options.writeWorkers());
			writeTasks = options.writeWorkers();
		}
		var started = new CountDownLatch(cap);
		var release = new CountDownLatch(1);
		long[][] telemetry = new long[POOLS.length][RWScheduler.POOL_TELEMETRY_LENGTH];
		int maximumActive = 0;
		long witnesses = 0L;
		try {
			scheduler.setStoragePressure(true);
			var read = scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE, Long.MAX_VALUE);
			var write = scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.MUTATION, Long.MAX_VALUE);
			for (int task = 0; task < readTasks; task++) read.execute(() -> holdPermit(started, release));
			for (int task = 0; task < writeTasks; task++) write.execute(() -> holdPermit(started, release));
			if (!started.await(30, TimeUnit.SECONDS)) {
				throw new IllegalStateException("pressured BATCH cap did not become concurrently active: cap="
						+ cap + ", read=" + scheduler.poolSnapshot(RWScheduler.Pool.READ)
						+ ", write=" + scheduler.poolSnapshot(RWScheduler.Pool.WRITE));
			}
			for (int sample = 0; sample < 3; sample++) {
				int active = activeBatchTasks(scheduler, telemetry);
				maximumActive = Math.max(maximumActive, active);
				if (active == cap) witnesses++;
			}
			if (maximumActive != cap || witnesses != 3L) {
				throw new IllegalStateException("pressured BATCH cap snapshot mismatch: configured=" + cap
						+ ", maximum=" + maximumActive + ", witnesses=" + witnesses);
			}
			return new PressureCapWitness(maximumActive, witnesses);
		} finally {
			release.countDown();
			scheduler.setStoragePressure(false);
			awaitDrain(connection);
		}
	}

	private static void holdPermit(CountDownLatch started, CountDownLatch release) {
		started.countDown();
		await(release);
	}

	private static int activeBatchTasks(RWScheduler scheduler, long[][] telemetry) {
		int active = 0;
		for (var pool : POOLS) {
			if (pool != RWScheduler.Pool.READ && pool != RWScheduler.Pool.WRITE) continue;
			long[] values = telemetry[pool.ordinal()];
			BenchmarkSchedulerTelemetry.copyPoolTelemetry(scheduler, pool, values);
			active = Math.addExact(active, BenchmarkSchedulerTelemetry.active(values, WorkloadProfile.BATCH));
		}
		return active;
	}

	private static Future<?> submit(ExecutorService executor, List<Throwable> failures, ThrowingRunnable action) {
		return executor.submit(() -> {
			try { action.run(); } catch (Throwable failure) { failures.add(failure); }
		});
	}

	private static CancelledScan cancelAfterFirstCheckpoint(EmbeddedConnection connection,
			long columnId,
			Set<RawSstToken> checkpoints,
			Path checkpointFile) throws Exception {
		var rows = new LongAdder();
		Flux.from(connection.getAsyncApi(RequestContext.batch())
					.scanRawResumableAsync(columnId, 0, 1, Set.of()))
				.doOnNext(event -> processEvent(event, checkpoints, checkpointFile, value -> rows.add(value), null))
				.takeUntil(MixedBackfillPressureBenchmark::hasCheckpoint)
				.blockLast(Duration.ofSeconds(30));
		if (checkpoints.size() != 1) throw new IllegalStateException("scan did not stop at its first checkpoint");
		awaitDrain(connection);
		return new CancelledScan(rows.sum());
	}

	private static boolean hasCheckpoint(RawScanEvent event) {
		return event instanceof RawScanEvent.SstCompleted
				|| event instanceof RawScanEvent.Batch batch && batch.completedSstToken() != null;
	}

	private static long consumeScan(java.util.stream.Stream<RawScanEvent> stream,
			Set<RawSstToken> checkpoints,
			Path checkpointFile,
			java.util.function.LongConsumer progress) {
		var rows = new LongAdder();
		try (stream) {
			stream.forEach(event -> processEvent(event, checkpoints, checkpointFile, value -> {
				rows.add(value); if (progress != null) progress.accept(value);
			}, null));
		}
		return rows.sum();
	}

	private static void processEvent(RawScanEvent event,
			Set<RawSstToken> checkpoints,
			Path checkpointFile,
			java.util.function.LongConsumer rows,
			CountDownLatch checkpoint) {
		RawSstToken token = null;
		if (event instanceof RawScanEvent.Batch batch) {
			rows.accept(Integer.toUnsignedLong(batch.serialized().getIntLE(0)));
			token = batch.completedSstToken();
		} else if (event instanceof RawScanEvent.SstCompleted completed) {
			token = completed.token();
		}
		if (token != null && checkpoints.add(token)) {
			if (checkpointFile != null) {
				try {
					appendCheckpoint(checkpointFile, token);
				} catch (java.io.IOException failure) {
					throw new java.io.UncheckedIOException(failure);
				}
			}
			if (checkpoint != null) checkpoint.countDown();
		}
	}

	private static void appendCheckpoint(Path checkpointFile, RawSstToken token) throws java.io.IOException {
		boolean createDirectoryEntry = Files.notExists(checkpointFile);
		byte[] value = (token.value() + '\n').getBytes(java.nio.charset.StandardCharsets.US_ASCII);
		try (var channel = java.nio.channels.FileChannel.open(checkpointFile,
				StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND,
				StandardOpenOption.DSYNC)) {
			var buffer = ByteBuffer.wrap(value);
			while (buffer.hasRemaining()) channel.write(buffer);
			channel.force(true);
		}
		if (createDirectoryEntry) {
			try (var directory = java.nio.channels.FileChannel.open(checkpointFile.getParent(),
					StandardOpenOption.READ)) {
				directory.force(true);
			}
		}
	}

	private static void preload(it.cavallium.rockserver.core.common.RocksDBSyncAPI api,
			long columnId,
			Options options,
			Keys[] keySpace) {
		Buf value = Buf.wrap(value(options.valueBytes()));
		for (int index = 0; index < options.preloadKeys(); index++) {
			api.put(0L, columnId, keySpace[index], value, RequestType.none());
			if ((index + 1) % options.flushEvery() == 0) api.flush();
		}
		api.flush();
	}

	private static Keys[] keys(int count) {
		Keys[] keys = new Keys[count];
		for (int index = 0; index < count; index++) {
			byte[] bytes = new byte[Long.BYTES];
			ByteBuffer.wrap(bytes).putLong(index);
			keys[index] = new Keys(Buf.wrap(bytes));
		}
		return keys;
	}

	private static byte[] value(int bytes) {
		byte[] value = new byte[bytes];
		for (int i = 0; i < value.length; i++) value[i] = (byte) (i * 31 + 7);
		return value;
	}

	private static void awaitDrain(EmbeddedConnection connection) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
		while (System.nanoTime() < deadline) {
			boolean drained = true;
			for (var pool : RWScheduler.Pool.values()) drained &= connection.getScheduler().poolSnapshot(pool).drainedAndConserved();
			if (drained && connection.getInternalDB().getPendingOpsCount() == 0L) return;
			LockSupport.parkNanos(100_000L);
		}
		throw new IllegalStateException("mixed benchmark scheduler did not drain");
	}

	private static long awaitLeaks(long before) throws InterruptedException {
		for (int attempt = 0; attempt < 3; attempt++) {
			System.gc();
			Thread.sleep(100L);
		}
		return Math.max(0L, RocksLeakDetector.detectedLeakCount() - before);
	}

	private static Path rootResult(Path root) { return root.resolve("mixed-results.properties"); }

	private static RuntimeException rethrow(Throwable failure) {
		if (failure instanceof RuntimeException runtime) return runtime;
		if (failure instanceof Error error) throw error;
		return new IllegalStateException(failure);
	}

	private static String gitOutput(Path directory, String... arguments) throws Exception {
		var command = new ArrayList<String>(arguments.length + 3);
		command.add("git");
		command.add("-C");
		command.add(directory.toString());
		command.addAll(List.of(arguments));
		var process = new ProcessBuilder(command).redirectErrorStream(true).start();
		var output = new java.util.concurrent.FutureTask<byte[]>(() -> process.getInputStream().readAllBytes());
		Thread.ofVirtual().name("mixed-pressure-provenance").start(output);
		byte[] bytes;
		try {
			if (!process.waitFor(30, TimeUnit.SECONDS)) {
				process.destroyForcibly();
				output.cancel(true);
				throw new IllegalStateException("Git provenance command timed out: " + String.join(" ", command));
			}
			bytes = output.get(30, TimeUnit.SECONDS);
		} catch (InterruptedException interrupted) {
			process.destroyForcibly();
			output.cancel(true);
			Thread.currentThread().interrupt();
			throw interrupted;
		} catch (java.util.concurrent.TimeoutException timeout) {
			process.destroyForcibly();
			output.cancel(true);
			throw new IllegalStateException("Git provenance output drain timed out", timeout);
		}
		String text = new String(bytes, StandardCharsets.UTF_8);
		if (process.exitValue() != 0) {
			throw new IllegalArgumentException("Git provenance command failed: " + text.trim());
		}
		return text.trim();
	}

	private static void await(CountDownLatch latch) {
		try { latch.await(); } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IllegalStateException(failure); }
	}

	static String configText(Options options) {
		return """
				database.global.disable-auto-compactions = true
				database.parallelism.read = %d
				database.parallelism.write = %d
				database.parallelism.workload.raw-scan-file-concurrency = %d
				database.parallelism.workload.pressured-batch-maximum-active = %d
				database.parallelism.workload.pressured-batch-interval = PT0.05S
				""".formatted(options.readWorkers(), options.writeWorkers(), options.rawScanConcurrency(),
				options.pressuredBatchMaximumActive());
	}

	static long latencyDeadlineEpochMillis(long nowEpochMillis, Duration measurement) {
		try {
			long budget = Math.addExact(measurement.toMillis(), TimeUnit.SECONDS.toMillis(30));
			return Math.addExact(nowEpochMillis, budget);
		} catch (ArithmeticException overflow) {
			return Long.MAX_VALUE - 1L;
		}
	}

	@FunctionalInterface private interface ThrowingRunnable { void run() throws Exception; }
	private record CancelledScan(long rows) {}
	private record PressureCapWitness(int maximumActive, long capWitnesses) {}

	record Provenance(String gitHead,
			String productionClassesSha256,
			String harnessClassesSha256,
			boolean checkoutClean) {

		Provenance {
			if (gitHead == null || !gitHead.matches("[0-9a-f]{40}")
					|| productionClassesSha256 == null
					|| !productionClassesSha256.matches("[0-9a-f]{64}")
					|| harnessClassesSha256 == null
					|| !harnessClassesSha256.matches("[0-9a-f]{64}")) {
				throw new IllegalArgumentException("Invalid mixed benchmark provenance");
			}
		}

		private static Provenance captureStrict(Options options) throws Exception {
			if (options.expectedGitHead().isBlank() || options.expectedProductionSha256().isBlank()) {
				throw new IllegalArgumentException("Representative runs require --expected-git-head and "
						+ "--expected-production-sha256");
			}
			Path invocation = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
			Path worktree = Path.of(gitOutput(invocation, "rev-parse", "--show-toplevel"))
					.toAbsolutePath().normalize();
			String head = gitOutput(worktree, "rev-parse", "--verify", "HEAD");
			if (!head.equals(options.expectedGitHead())) {
				throw new IllegalArgumentException("Git HEAD mismatch: expected=" + options.expectedGitHead()
						+ " actual=" + head);
			}
			AdversarialBatchLivenessPairedBenchmark.verifyProductionCheckout(worktree, head);
			String dirty = gitOutput(worktree, "status", "--porcelain=v1", "--untracked-files=all");
			if (!dirty.isEmpty()) {
				throw new IllegalArgumentException("Benchmark checkout is dirty: " + worktree);
			}
			Path expectedProduction = worktree.resolve("target/classes").toRealPath();
			Path expectedHarness = worktree.resolve("target/test-classes").toRealPath();
			Path loadedProduction = codeSource(RWScheduler.class);
			Path loadedHarness = codeSource(MixedBackfillPressureBenchmark.class);
			if (!loadedProduction.equals(expectedProduction) || !loadedHarness.equals(expectedHarness)) {
				throw new IllegalArgumentException("Loaded benchmark classes do not belong to the clean HEAD "
						+ "worktree: production=" + loadedProduction + ", harness=" + loadedHarness);
			}
			String productionSha = contentSha(expectedProduction);
			if (!productionSha.equals(options.expectedProductionSha256())) {
				throw new IllegalArgumentException("Loaded production classes SHA-256 mismatch: expected="
						+ options.expectedProductionSha256() + " actual=" + productionSha);
			}
			return new Provenance(head, productionSha, contentSha(expectedHarness), true);
		}

		static Provenance testing() {
			return new Provenance("0".repeat(40), "1".repeat(64), "2".repeat(64), true);
		}

		private static Path codeSource(Class<?> type) throws Exception {
			return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI()).toRealPath();
		}

		private static String contentSha(Path path) throws java.io.IOException {
			return GrpcRetainedReadBenchmark.classPathContentSha256ForTesting(path.toString());
		}

		private String marker() {
			return "schema=" + RESULT_SCHEMA + '\n'
					+ "git-head=" + gitHead + '\n'
					+ "production-classes-sha256=" + productionClassesSha256 + '\n'
					+ "harness-classes-sha256=" + harnessClassesSha256 + '\n'
					+ "checkout-clean=" + checkoutClean + '\n';
		}
	}

	private static final class LatencySamples {
		private final long[] values;
		private final AtomicInteger cursor = new AtomicInteger();
		private LatencySamples(int capacity) { values = new long[capacity]; }
		private void record(long value) { int index = cursor.getAndIncrement(); if (index < values.length) values[index] = Math.max(1L, value); }
		private long p99() {
			int length = Math.min(cursor.get(), values.length); if (length == 0) return 0L;
			long[] copy = Arrays.copyOf(values, length); Arrays.sort(copy);
			return copy[Math.max(0, (int) Math.ceil(length * 0.99d) - 1)];
		}
	}

	public record Options(Path root,
			int preloadKeys,
			int valueBytes,
			int flushEvery,
			Duration measureDuration,
			int writers,
			int latencyReaders,
			int readWorkers,
			int writeWorkers,
			int rawScanConcurrency,
			int pressuredBatchMaximumActive,
			long cdcBatchSize,
			Duration pressurePeriod,
			Duration maximumZeroProgressGap,
			long maximumCdcLag,
			Duration maximumLatencyP99,
			int maximumLatencySamples,
			double minimumBackfillRowsPerSecond,
			double minimumIngestWritesPerSecond,
			String expectedGitHead,
			String expectedProductionSha256) {

		void validate() {
			if (root == null || preloadKeys < 1 || valueBytes < 1 || flushEvery < 1 || flushEvery > preloadKeys
					|| measureDuration == null || measureDuration.isZero() || measureDuration.isNegative()
					|| writers < 1 || latencyReaders < 1 || readWorkers < 3 || writeWorkers < 3
					|| rawScanConcurrency < 1 || rawScanConcurrency > 64
					|| pressuredBatchMaximumActive < 1
					|| pressuredBatchMaximumActive > MAX_PRESSURED_BATCH_ACTIVE
					|| pressuredBatchMaximumActive > (long) readWorkers + writeWorkers
					|| cdcBatchSize < 1
					|| pressurePeriod == null || pressurePeriod.isZero() || pressurePeriod.isNegative()
					|| maximumZeroProgressGap == null || maximumZeroProgressGap.isZero() || maximumZeroProgressGap.isNegative()
					|| maximumCdcLag < 1 || maximumLatencyP99 == null || maximumLatencyP99.isZero()
					|| maximumLatencyP99.isNegative() || maximumLatencySamples < 1
					|| !Double.isFinite(minimumBackfillRowsPerSecond) || minimumBackfillRowsPerSecond <= 0.0d
					|| !Double.isFinite(minimumIngestWritesPerSecond) || minimumIngestWritesPerSecond <= 0.0d
					|| expectedGitHead == null || expectedProductionSha256 == null
					|| expectedGitHead.isBlank() != expectedProductionSha256.isBlank()
					|| !expectedGitHead.isBlank() && !expectedGitHead.matches("[0-9a-f]{40}")
					|| !expectedProductionSha256.isBlank()
					&& !expectedProductionSha256.matches("[0-9a-f]{64}")) {
				throw new IllegalArgumentException("Invalid mixed backfill pressure options");
			}
		}

		static Options parse(String[] args) {
			var values = new java.util.LinkedHashMap<String, String>();
			for (String argument : args) {
				if (!argument.startsWith("--") || !argument.contains("=")) throw new IllegalArgumentException("Use --name=value");
				int equals = argument.indexOf('=');
				if (values.put(argument.substring(2, equals), argument.substring(equals + 1)) != null) throw new IllegalArgumentException("Duplicate option");
			}
			Set<String> allowed = Set.of("root", "preload-keys", "value-bytes", "flush-every", "measure-ms",
					"writers", "latency-readers", "read-workers", "write-workers", "raw-scan-concurrency",
					"pressured-batch-maximum-active",
					"cdc-batch-size", "pressure-period-ms", "maximum-zero-gap-ms", "maximum-cdc-lag",
					"maximum-latency-p99-ms", "maximum-latency-samples", "minimum-backfill-rows-per-second",
					"minimum-ingest-writes-per-second", "expected-git-head", "expected-production-sha256");
			for (String key : values.keySet()) if (!allowed.contains(key)) throw new IllegalArgumentException("Unknown option --" + key);
			String root = values.get("root"); if (root == null) throw new IllegalArgumentException("--root is required");
			var options = new Options(Path.of(root), integer(values, "preload-keys", 50_000), integer(values, "value-bytes", 256),
					integer(values, "flush-every", 5_000), Duration.ofMillis(longValue(values, "measure-ms", 10_000)),
					integer(values, "writers", 2), integer(values, "latency-readers", 2), integer(values, "read-workers", 8),
					integer(values, "write-workers", 8), integer(values, "raw-scan-concurrency", 4),
					integer(values, "pressured-batch-maximum-active", 1),
					longValue(values, "cdc-batch-size", 8_192), Duration.ofMillis(longValue(values, "pressure-period-ms", 100)),
					Duration.ofMillis(longValue(values, "maximum-zero-gap-ms", 2_000)),
					longValue(values, "maximum-cdc-lag", 100_000), Duration.ofMillis(longValue(values, "maximum-latency-p99-ms", 5_000)),
					integer(values, "maximum-latency-samples", 100_000),
					doubleValue(values, "minimum-backfill-rows-per-second", 100.0d),
					doubleValue(values, "minimum-ingest-writes-per-second", 100.0d),
					values.getOrDefault("expected-git-head", ""),
					values.getOrDefault("expected-production-sha256", ""));
			options.validate();
			return options;
		}
		private static int integer(Map<String, String> values, String key, int fallback) { return Integer.parseInt(values.getOrDefault(key, Integer.toString(fallback))); }
		private static long longValue(Map<String, String> values, String key, long fallback) { return Long.parseLong(values.getOrDefault(key, Long.toString(fallback))); }
		private static double doubleValue(Map<String, String> values, String key, double fallback) { return Double.parseDouble(values.getOrDefault(key, Double.toString(fallback))); }
	}

	public record Result(Options options,
			Provenance provenance,
			long cancelledRows,
			long resumedRows,
			int durableCheckpoints,
			long elapsedNanos,
			long backfillRows,
			long backfillBatches,
			long ingestWrites,
			long latencyReads,
			long cdcEvents,
			long maintenanceOperations,
			long pressureTransitions,
			int maximumPressuredBatchActive,
			long pressuredBatchCapWitnesses,
			long maximumZeroProgressGapNanos,
			long maximumCdcLag,
			long latencyP99Nanos,
			double cpuNanosPerUsefulOperation,
			double allocatedBytesPerUsefulOperation,
			long gcCollections,
			long gcMillis,
			int maximumQueued,
			int maximumParked,
			int maximumOutstanding,
			BenchmarkProcessTelemetry.Peaks peaks,
			long nativeLeaks,
			Path output) {

		Result withNativeLeaks(long leaks) { return new Result(options, provenance, cancelledRows, resumedRows, durableCheckpoints,
				elapsedNanos, backfillRows, backfillBatches, ingestWrites, latencyReads, cdcEvents,
				maintenanceOperations, pressureTransitions, maximumPressuredBatchActive,
				pressuredBatchCapWitnesses, maximumZeroProgressGapNanos, maximumCdcLag,
				latencyP99Nanos, cpuNanosPerUsefulOperation, allocatedBytesPerUsefulOperation,
				gcCollections, gcMillis, maximumQueued, maximumParked, maximumOutstanding, peaks, leaks, output); }

		public void assertCorrect() {
			double seconds = elapsedNanos / 1_000_000_000.0d;
			if (cancelledRows + resumedRows != options.preloadKeys() || durableCheckpoints < 1
					|| provenance == null || !provenance.checkoutClean()
					|| elapsedNanos <= 0L || backfillRows <= 0L || backfillBatches <= 0L || ingestWrites <= 0L
					|| backfillRows / seconds < options.minimumBackfillRowsPerSecond()
					|| ingestWrites / seconds < options.minimumIngestWritesPerSecond()
					|| latencyReads <= 0L || cdcEvents <= 0L || maintenanceOperations <= 0L || pressureTransitions < 2L
					|| maximumPressuredBatchActive != options.pressuredBatchMaximumActive()
					|| pressuredBatchCapWitnesses <= 0L
					|| maximumZeroProgressGapNanos > options.maximumZeroProgressGap().toNanos()
					|| maximumCdcLag > options.maximumCdcLag() || latencyP99Nanos <= 0L
					|| latencyP99Nanos > options.maximumLatencyP99().toNanos()
					|| !Double.isFinite(cpuNanosPerUsefulOperation) || cpuNanosPerUsefulOperation <= 0.0d
					|| !Double.isFinite(allocatedBytesPerUsefulOperation) || allocatedBytesPerUsefulOperation <= 0.0d
					|| gcCollections < 0L || gcMillis < 0L || maximumQueued < 0 || maximumParked < 0
					|| maximumOutstanding < 0 || !peaks.complete() || nativeLeaks != 0L) {
				throw new IllegalStateException("Mixed backfill pressure contract failed: " + this);
			}
		}

		void write() throws java.io.IOException {
			assertCorrect();
			Files.writeString(output, properties(), StandardOpenOption.CREATE_NEW);
			Files.writeString(options.root().resolve("mixed-results.md"), toMarkdown(), StandardOpenOption.CREATE_NEW);
		}

		String properties() {
			return "schema=" + RESULT_SCHEMA + '\n' + "pressure-mode=injected\n"
					+ "git-head=" + provenance.gitHead() + '\n'
					+ "production-classes-sha256=" + provenance.productionClassesSha256() + '\n'
					+ "harness-classes-sha256=" + provenance.harnessClassesSha256() + '\n'
					+ "checkout-clean=" + provenance.checkoutClean() + '\n'
					+ "pressured-batch-witness-mode=held-barrier-scheduler-snapshots\n"
					+ "shutdown-clean=true\nfinal-drained=true\n"
					+ "pressured-batch-maximum-active=" + options.pressuredBatchMaximumActive() + '\n'
					+ "maximum-pressured-batch-active=" + maximumPressuredBatchActive + '\n'
					+ "pressured-batch-cap-witnesses=" + pressuredBatchCapWitnesses + '\n'
					+ "cancelled-rows=" + cancelledRows + '\n'
					+ "resumed-rows=" + resumedRows + '\n' + "durable-checkpoints=" + durableCheckpoints + '\n'
					+ "elapsed-nanos=" + elapsedNanos + '\n' + "backfill-rows=" + backfillRows + '\n'
					+ "backfill-rows-per-second=" + (backfillRows * 1_000_000_000.0d / elapsedNanos) + '\n'
					+ "backfill-batches=" + backfillBatches + '\n' + "ingest-writes=" + ingestWrites + '\n'
					+ "ingest-writes-per-second=" + (ingestWrites * 1_000_000_000.0d / elapsedNanos) + '\n'
					+ "latency-reads=" + latencyReads + '\n' + "cdc-events=" + cdcEvents + '\n'
					+ "maintenance-operations=" + maintenanceOperations + '\n'
					+ "pressure-transitions=" + pressureTransitions + '\n'
					+ "maximum-zero-progress-gap-nanos=" + maximumZeroProgressGapNanos + '\n'
					+ "maximum-cdc-lag=" + maximumCdcLag + '\n' + "latency-p99-nanos=" + latencyP99Nanos + '\n'
					+ "cpu-nanos-per-useful-operation=" + cpuNanosPerUsefulOperation + '\n'
					+ "allocated-bytes-per-useful-operation=" + allocatedBytesPerUsefulOperation + '\n'
					+ "gc-collections=" + gcCollections + '\n' + "gc-millis=" + gcMillis + '\n'
					+ "maximum-queued=" + maximumQueued + '\n' + "maximum-parked=" + maximumParked + '\n'
					+ "maximum-outstanding=" + maximumOutstanding + '\n' + "peak-live-heap-bytes=" + peaks.liveHeapBytes() + '\n'
					+ "peak-direct-memory-bytes=" + peaks.directMemoryBytes() + '\n'
					+ "peak-rss-bytes=" + peaks.residentSetBytes() + '\n' + "peak-threads=" + peaks.threadCount() + '\n'
					+ "peak-native-handles=" + peaks.nativeHandles() + '\n' + "native-leaks=" + nativeLeaks + '\n';
		}

		public String toMarkdown() {
			return "# Mixed backfill pressure benchmark\n\n- Backfill: `" + backfillRows + "` rows\n"
					+ "- Git HEAD: `" + provenance.gitHead() + "`\n"
					+ "- Production classes SHA-256: `" + provenance.productionClassesSha256() + "`\n"
					+ "- Pressured BATCH cap / observed peak / witnesses: `"
					+ options.pressuredBatchMaximumActive() + " / " + maximumPressuredBatchActive + " / "
					+ pressuredBatchCapWitnesses + "`\n"
					+ "- Ingest / CDC: `" + ingestWrites + "` / `" + cdcEvents + "`\n"
					+ "- LATENCY p99: `" + String.format(Locale.ROOT, "%.3f", latencyP99Nanos / 1_000_000.0d) + " ms`\n"
					+ "- Maximum zero-progress gap: `" + String.format(Locale.ROOT, "%.3f", maximumZeroProgressGapNanos / 1_000_000.0d) + " ms`\n"
					+ "- Checkpoint/resume rows: `" + cancelledRows + " + " + resumedRows + "`\n"
					+ "- Result: **PASS**\n";
		}
	}
}
