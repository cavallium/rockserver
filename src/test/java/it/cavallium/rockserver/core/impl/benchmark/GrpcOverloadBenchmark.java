package it.cavallium.rockserver.core.impl.benchmark;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.api.proto.FirstAndLast;
import it.cavallium.rockserver.core.common.api.proto.GetRangeRequest;
import it.cavallium.rockserver.core.common.api.proto.GetRequest;
import it.cavallium.rockserver.core.common.api.proto.GetResponse;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.api.proto.PutRequest;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import it.cavallium.rockserver.core.impl.rocksdb.RocksLeakDetector;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
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
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Opt-in, disk-backed gRPC overload regression benchmark.
 *
 * <p>The runner opens one real RocksDB database, preloads and flushes it, then runs a
 * foreground-only phase followed by a maintenance-flood phase against the same state. The
 * five-second client and range budgets are fixed deliberately: changing the timeout is not a
 * benchmark knob. This class lives in test sources and is not executed by ordinary CI.</p>
 */
public final class GrpcOverloadBenchmark {

	private static final long DEADLINE_MILLIS = 5_000;
	private static final long WORKER_SHUTDOWN_GRACE_SECONDS = 30;
	private static final long RESOURCE_DRAIN_TIMEOUT_SECONDS = 30;
	private static final int WRITE_REQUEST_VARIANTS = 64;
	private static final int MAX_RECORDED_ERRORS = 20;
	private static final String RESULT_SCHEMA = "rockserver-grpc-overload-v1";

	private GrpcOverloadBenchmark() {
	}

	public static void main(String[] args) throws Exception {
		if (Arrays.asList(args).contains("--help")) {
			printUsage();
			return;
		}

		Options options = Options.parse(args);
		System.setProperty("rockserver.core.print-config", "false");
		System.setProperty("it.cavallium.rockserver.leakdetection", "true");
		Instant started = Instant.now();
		long nativeLeaksBefore = RocksLeakDetector.detectedLeakCount();
		Path root;
		Path config;
		if (options.reusePreloaded()) {
			root = openPreparedRoot(options);
			config = root.resolve("rockserver.conf");
			writeMetadata(root.resolve("run-metadata.txt"), options, started);
		} else {
			root = createFreshRoot(options);
			config = writeConfig(root, options);
			writeMetadata(root.resolve("metadata.txt"), options, started);
		}

		List<PhaseResult> phases = new ArrayList<>(2);
		Throwable runFailure = null;
		Throwable closeFailure = null;
		EmbeddedConnection embedded = null;
		GrpcServer server = null;
		Client client = null;
		try {
			embedded = new EmbeddedConnection(root.resolve("db"), options.databaseName(), config);
			long columnId = options.reusePreloaded()
					? embedded.getSyncApi().getColumnId("overload-benchmark")
					: preload(embedded.getSyncApi(), options);
			if (!options.prepareOnly()) {
				Requests requests = Requests.create(columnId, options);
				server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0));
				server.start();
				client = Client.open(server.getPort());
				preflight(client, requests);

				phases.add(runPhase(Phase.FOREGROUND_ONLY, client, embedded, server, requests, options));
				phases.add(runPhase(Phase.MAINTENANCE_FLOOD, client, embedded, server, requests, options));
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
			System.out.println("Drop the host page cache, then rerun with --reuse-preloaded=true and identical options.");
			return;
		}

		boolean shutdownClean = closeFailure == null;
		Acceptance acceptance = phases.size() == 2
				? evaluateAcceptance(gateInput(
						phases.get(0), phases.get(1), options, shutdownClean, nativeLeaksDetected))
				: Acceptance.failed("Both benchmark phases must complete");
		BenchmarkResult result = new BenchmarkResult(
				RESULT_SCHEMA,
				started,
				Instant.now(),
				options,
				List.copyOf(phases),
				shutdownClean,
				nativeLeaksDetected,
				acceptance);
		writeReports(root, result);
		System.out.println(toMarkdown(result));
		System.out.println("Machine-readable results: " + root.resolve("results.json").toAbsolutePath());
		System.out.println("Human-readable results: " + root.resolve("results.md").toAbsolutePath());

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
		Files.writeString(root.resolve(".rockserver-overload-benchmark"), """
				schema=%s
				preload_keys=%d
				value_bytes=%d
				""".formatted(RESULT_SCHEMA, options.preloadKeys(), options.valueBytes()),
				StandardOpenOption.CREATE_NEW);
		return root;
	}

	private static Path openPreparedRoot(Options options) throws IOException {
		Path root = options.root().toAbsolutePath().normalize();
		Path marker = root.resolve(".rockserver-overload-benchmark");
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
		if (!RESULT_SCHEMA.equals(markerValues.get("schema"))
				|| options.preloadKeys() != Integer.parseInt(markerValues.getOrDefault("preload_keys", "-1"))
				|| options.valueBytes() != Integer.parseInt(markerValues.getOrDefault("value_bytes", "-1"))) {
			throw new IllegalArgumentException("Prepared preload settings do not match the requested run");
		}
		if (!Files.readString(config).equals(configText(options))) {
			throw new IllegalArgumentException("Prepared Rockserver configuration does not match the requested run");
		}
		if (Files.exists(root.resolve("results.json")) || Files.exists(root.resolve("results.md"))) {
			throw new IllegalArgumentException("Prepared root already contains benchmark results: " + root);
		}
		return root;
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

	private static void preflight(Client client, Requests requests) {
		GetResponse point = client.blockingWithDeadline().get(requests.pointReads()[0]);
		if (!point.hasValue()) {
			throw new IllegalStateException("Point-read preflight returned an absent value");
		}
		RangeCase range = requests.ranges()[0];
		validateRange(client.blockingWithDeadline().reduceRangeFirstAndLast(range.request()), range);
		client.blockingWithDeadline().put(requests.foregroundWrites()[0][0]);
		client.blockingWithDeadline().put(requests.maintenanceWrites()[0][0]);
	}

	private static PhaseResult runPhase(Phase phase,
			Client client,
			EmbeddedConnection embedded,
			GrpcServer server,
			Requests requests,
			Options options) throws Exception {
		boolean maintenanceFlood = phase == Phase.MAINTENANCE_FLOOD;
		int maintenanceWorkers = maintenanceFlood ? options.maintenanceWriters() : 0;
		int cancellationWorkers = maintenanceFlood ? options.cancellationWorkers() : 0;
		int workerCount = options.pointReaders()
				+ options.foregroundWriters()
				+ options.firstLastReaders()
				+ maintenanceWorkers
				+ cancellationWorkers
				+ 1;
		PhaseControl control = new PhaseControl(workerCount, options.maxLatencySamples());
		ExecutorService executor = Executors.newFixedThreadPool(workerCount,
				Thread.ofPlatform().name("grpc-overload-" + phase.value + "-", 0).factory());
		List<Future<?>> futures = new ArrayList<>(workerCount);
		System.out.printf(Locale.ROOT,
				"Starting %s: warmup=%ds measure=%ds point-readers=%d foreground-writers=%d "
						+ "first-last-readers=%d maintenance-writers=%d cancellation-workers=%d%n",
				phase.value, options.warmupSeconds(), options.measureSeconds(), options.pointReaders(),
				options.foregroundWriters(), options.firstLastReaders(), maintenanceWorkers,
				cancellationWorkers);
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
			control.startMeasurement();
			sleepPhase(options.measureSeconds(), control.stop);
			long durationNanos = control.stopMeasurement();
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
			AdmissionResult admission = control.admission.finish(embedded, control.metrics);
			PhaseResult result = control.metrics.snapshot(phase, durationNanos, admission, resources);
			printPhase(result);
			return result;
		} finally {
			control.measuring.set(false);
			control.stop.set(true);
			control.start.countDown();
			executor.shutdownNow();
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
		long requestSequence = (phase == Phase.MAINTENANCE_FLOOD ? requests.ranges().length / 2L : 0L) + worker;
		long operationSequence = worker;
		while (!control.stop.get()) {
			RangeCase range = requests.ranges()[(int) (requestSequence % requests.ranges().length)];
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
		control.ready.countDown();
		control.start.await();
		while (!control.stop.get()) {
			if (control.measuring.get()) {
				control.admission.sample(embedded);
			}
			LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(options.admissionSampleMicros()));
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
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
		var admission = scheduler.writeAdmissionSnapshot();
		int foregroundQueued = admission.foregroundQueued();
		int maintenanceQueued = admission.maintenanceQueued();
		int foregroundActive = admission.foregroundActive();
		int maintenanceActive = admission.maintenanceActive();
		long pending = db.getPendingOpsCount();
		int transactions = db.getOpenTransactionsCount();
		int iterators = db.getOpenIteratorsCount();
		int ranges = db.getActiveRangeCursorCount();
		int iteratorLeases = server.getActiveIteratorOperationLeaseCountForTesting();
		boolean drained = foregroundQueued == 0
				&& maintenanceQueued == 0
				&& foregroundActive == 0
				&& maintenanceActive == 0
				&& pending == 0
				&& transactions == 0
				&& iterators == 0
				&& ranges == 0
				&& iteratorLeases == 0;
		return new ResourceResult(foregroundQueued,
				maintenanceQueued,
				foregroundActive,
				maintenanceActive,
				pending,
				transactions,
				iterators,
				ranges,
				iteratorLeases,
				TimeUnit.NANOSECONDS.toMillis(drainNanos),
				drained);
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

	private static GateInput gateInput(PhaseResult foreground,
			PhaseResult flood,
			Options options,
			boolean shutdownClean,
			long nativeLeaksDetected) {
		return new GateInput(
				foreground.operation(Operation.FOREGROUND).deadlines()
						+ flood.operation(Operation.FOREGROUND).deadlines(),
				foreground.operation(Operation.FIRST_LAST).deadlines()
						+ flood.operation(Operation.FIRST_LAST).deadlines(),
				foreground.operation(Operation.FOREGROUND).p99Nanos(),
				flood.operation(Operation.FOREGROUND).p99Nanos(),
				flood.operation(Operation.MAINTENANCE_WRITE).successes(),
				flood.operation(Operation.CANCELLATION).cancellations(),
				Math.max(foreground.admission().maxTotalActive(), flood.admission().maxTotalActive()),
				Math.max(foreground.admission().maxMaintenanceActive(),
						flood.admission().maxMaintenanceActive()),
				options.writeParallelism(),
				options.maintenanceWriteParallelism(),
				foreground.resources().drained() && flood.resources().drained(),
				foreground.unexpectedErrors() + flood.unexpectedErrors(),
				foreground.admission().foregroundRejected() + flood.admission().foregroundRejected(),
				nativeLeaksDetected,
				shutdownClean);
	}

	/** Pure acceptance evaluation used by the opt-in runner and deterministic CI tests. */
	public static Acceptance evaluateAcceptance(GateInput input) {
		Objects.requireNonNull(input, "input");
		List<GateCheck> checks = new ArrayList<>();
		checks.add(new GateCheck("foreground_deadlines", input.foregroundDeadlines() == 0,
				"foreground deadline count=" + input.foregroundDeadlines()));
		checks.add(new GateCheck("first_last_deadlines", input.firstLastDeadlines() == 0,
				"first/last deadline count=" + input.firstLastDeadlines()));
		boolean p99Available = input.foregroundOnlyP99Nanos() > 0 && input.maintenanceFloodP99Nanos() > 0;
		double p99Ratio = p99Available
				? (double) input.maintenanceFloodP99Nanos() / input.foregroundOnlyP99Nanos()
				: Double.POSITIVE_INFINITY;
		checks.add(new GateCheck("foreground_p99_ratio", p99Available && p99Ratio <= 2.0,
				"maintenance-flood/foreground-only p99=" + format(p99Ratio) + " (limit=2.000)"));
		checks.add(new GateCheck("maintenance_progress", input.maintenanceSuccesses() > 0,
				"maintenance successful writes=" + input.maintenanceSuccesses()));
		checks.add(new GateCheck("cancellation_progress", input.cancellations() > 0,
				"cancelled queued calls=" + input.cancellations()));
		checks.add(new GateCheck("combined_write_limit", input.maxTotalActive() <= input.writeLimit(),
				"max active writes=" + input.maxTotalActive() + " (limit=" + input.writeLimit() + ")"));
		checks.add(new GateCheck("maintenance_write_limit",
				input.maxMaintenanceActive() <= input.maintenanceLimit(),
				"max active maintenance writes=" + input.maxMaintenanceActive()
						+ " (limit=" + input.maintenanceLimit() + ")"));
		checks.add(new GateCheck("queues_and_resources_drained", input.resourcesDrained(),
				"all queues, pending operations, transactions, iterators, and range cursors drained"));
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

	private static void printPhase(PhaseResult result) {
		OperationResult foreground = result.operation(Operation.FOREGROUND);
		OperationResult maintenance = result.operation(Operation.MAINTENANCE_WRITE);
		System.out.printf(Locale.ROOT,
				"%s: foreground=%.1f ops/s p99=%.3fms deadlines=%d; maintenance=%.1f ops/s "
						+ "progress=%d rejected=%d; queues fg/max=%d maint/max=%d; active total/max=%d maint/max=%d%n",
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
				result.admission().maxMaintenanceActive());
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
				    maintenance-write: %d
				    foreground-write-queue-capacity: %d
				    maintenance-write-queue-capacity: %d
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
				options.maintenanceWriteParallelism(),
				options.foregroundQueueCapacity(),
				options.maintenanceQueueCapacity(),
				options.spinning(),
				options.directIo(),
				escapeHocon(options.writeBufferSize()));
	}

	private static String escapeHocon(String value) {
		return value.replace("\\", "\\\\").replace("\"", "\\\"");
	}

	private static void writeMetadata(Path output, Options options, Instant started) throws IOException {
		List<String> lines = new ArrayList<>();
		lines.add("schema=" + RESULT_SCHEMA);
		lines.add("started=" + started);
		lines.add("java_version=" + System.getProperty("java.version"));
		lines.add("java_vm=" + System.getProperty("java.vm.name") + " "
				+ System.getProperty("java.vm.version"));
		lines.add("os=" + System.getProperty("os.name") + " " + System.getProperty("os.version")
				+ " " + System.getProperty("os.arch"));
		lines.add("available_processors=" + Runtime.getRuntime().availableProcessors());
		lines.add("deadline_ms=" + DEADLINE_MILLIS);
		lines.add("options=" + options);
		Files.write(output, lines, StandardOpenOption.CREATE_NEW);
	}

	private static void writeReports(Path root, BenchmarkResult result) throws IOException {
		Files.writeString(root.resolve("results.json"), toJson(result),
				StandardOpenOption.CREATE_NEW);
		Files.writeString(root.resolve("results.md"), toMarkdown(result),
				StandardOpenOption.CREATE_NEW);
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
		json.append(",\n  \"options\": ");
		appendOptionsJson(json, result.options());
		json.append(",\n  \"phases\": [");
		for (int phaseIndex = 0; phaseIndex < result.phases().size(); phaseIndex++) {
			if (phaseIndex > 0) {
				json.append(',');
			}
			appendPhaseJson(json, result.phases().get(phaseIndex));
		}
		json.append("\n  ],\n  \"shutdown_clean\": ").append(result.shutdownClean());
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
		json.append(",\n    \"preload_keys\": ").append(options.preloadKeys())
				.append(",\n    \"preload_flush_keys\": ").append(options.preloadFlushKeys())
				.append(",\n    \"value_bytes\": ").append(options.valueBytes())
				.append(",\n    \"warmup_seconds\": ").append(options.warmupSeconds())
				.append(",\n    \"measure_seconds\": ").append(options.measureSeconds())
				.append(",\n    \"point_readers\": ").append(options.pointReaders())
				.append(",\n    \"foreground_writers\": ").append(options.foregroundWriters())
				.append(",\n    \"maintenance_writers\": ").append(options.maintenanceWriters())
				.append(",\n    \"first_last_readers\": ").append(options.firstLastReaders())
				.append(",\n    \"cancellation_workers\": ").append(options.cancellationWorkers())
				.append(",\n    \"foreground_write_rate\": ").append(options.foregroundWriteRate())
				.append(",\n    \"maintenance_write_rate\": ").append(options.maintenanceWriteRate())
				.append(",\n    \"first_last_rate\": ").append(options.firstLastRate())
				.append(",\n    \"cancellation_rate\": ").append(options.cancellationRate())
				.append(",\n    \"cancellation_delay_ms\": ").append(options.cancellationDelayMillis())
				.append(",\n    \"cancellation_burst\": ").append(options.cancellationBurst())
				.append(",\n    \"point_request_count\": ").append(options.pointRequestCount())
				.append(",\n    \"range_request_count\": ").append(options.rangeRequestCount())
				.append(",\n    \"range_width\": ").append(options.rangeWidth())
				.append(",\n    \"read_parallelism\": ").append(options.readParallelism())
				.append(",\n    \"write_parallelism\": ").append(options.writeParallelism())
				.append(",\n    \"maintenance_write_parallelism\": ")
				.append(options.maintenanceWriteParallelism())
				.append(",\n    \"foreground_queue_capacity\": ")
				.append(options.foregroundQueueCapacity())
				.append(",\n    \"maintenance_queue_capacity\": ")
				.append(options.maintenanceQueueCapacity())
				.append(",\n    \"admission_sample_micros\": ").append(options.admissionSampleMicros())
				.append(",\n    \"max_latency_samples\": ").append(options.maxLatencySamples())
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

	private static void appendPhaseJson(StringBuilder json, PhaseResult phase) {
		json.append("\n    {\n      \"phase\": ");
		appendJsonString(json, phase.phase().value);
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
		json.append("\n    }");
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
				.append("}, \"max_total_active\": ").append(admission.maxTotalActive()).append('}');
	}

	private static void appendResourcesJson(StringBuilder json, ResourceResult resources) {
		json.append("{\"foreground_queued\": ").append(resources.foregroundQueued())
				.append(", \"maintenance_queued\": ").append(resources.maintenanceQueued())
				.append(", \"foreground_active\": ").append(resources.foregroundActive())
				.append(", \"maintenance_active\": ").append(resources.maintenanceActive())
				.append(", \"pending_operations\": ").append(resources.pendingOperations())
				.append(", \"open_transactions\": ").append(resources.openTransactions())
				.append(", \"open_iterators\": ").append(resources.openIterators())
				.append(", \"active_range_cursors\": ").append(resources.activeRangeCursors())
				.append(", \"iterator_leases\": ").append(resources.iteratorLeases())
				.append(", \"drain_ms\": ").append(resources.drainMillis())
				.append(", \"drained\": ").append(resources.drained()).append('}');
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
				.append("- Fixed client/range deadline: `").append(DEADLINE_MILLIS).append(" ms`\n")
				.append("- Preloaded keys: `").append(result.options().preloadKeys()).append("`\n")
				.append("- Write limits: combined `").append(result.options().writeParallelism())
				.append("`, maintenance `").append(result.options().maintenanceWriteParallelism()).append("`\n")
				.append("- Queue capacities: foreground `").append(result.options().foregroundQueueCapacity())
				.append("`, maintenance `").append(result.options().maintenanceQueueCapacity()).append("`\n\n")
				.append("## Operations\n\n")
				.append("| Phase | Operation | Throughput/s | p50 ms | p95 ms | p99 ms | max ms | Deadlines | Rejected | Cancelled | Errors |\n")
				.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
		for (PhaseResult phase : result.phases()) {
			for (Operation operation : Operation.values()) {
				OperationResult stats = phase.operation(operation);
				markdown.append('|').append(phase.phase().value)
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
		markdown.append("\n## Admission and drain\n\n")
				.append("| Phase | FG queue max/end | Maintenance queue max/end | FG active max | Maintenance active max | Total active max | FG rejected | Maintenance rejected | Drain ms | Drained |\n")
				.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|\n");
		for (PhaseResult phase : result.phases()) {
			AdmissionResult admission = phase.admission();
			markdown.append('|').append(phase.phase().value)
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

				Run the one-shot gate on a fresh database path:
				  java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \
				    -cp "target/test-classes:target/classes:$(<target/overload-benchmark.classpath)" \
				    it.cavallium.rockserver.core.impl.benchmark.GrpcOverloadBenchmark \
				    --root=/mnt/rockserver-hdd/overload-2026-07-23

				For a real cold-cache run, first use the same command and full option set with
				--prepare-only=true. Drop the host page cache after that process closes, then rerun with
				--reuse-preloaded=true. The second run verifies the marker, preload shape, and exact config.

				The five-second deadline and 2x p99 acceptance limit are fixed. Important --name=value options:
				  preload-keys=1000000  preload-flush-keys=50000  value-bytes=256
				  warmup-seconds=15  measure-seconds=60
				  point-readers=8  foreground-writers=4  maintenance-writers=64
				  first-last-readers=2  cancellation-workers=2
				  foreground-write-rate=1000  maintenance-write-rate=0
				  first-last-rate=50  cancellation-rate=100  cancellation-burst=64
				  read-parallelism=20  write-parallelism=36  maintenance-write-parallelism=1
				  foreground-queue-capacity=4096  maintenance-queue-capacity=512
				  point-request-count=8192  range-request-count=8192  range-width=1024
				  direct-io=false  spinning=false  enforce=true  smoke=false
				  prepare-only=false  reuse-preloaded=false

				Use --smoke=true for a short structural run. It does not enforce performance gates unless
				--enforce=true is also supplied. Every run refuses an existing root and leaves its database,
				metadata, results.json, and results.md for inspection.
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
			RangeCase[] ranges,
			PutRequest[][] foregroundWrites,
			PutRequest[][] maintenanceWrites,
			PutRequest[][] cancellationWrites) {

		private static Requests create(long columnId, Options options) {
			GetRequest[] points = new GetRequest[Math.min(options.pointRequestCount(), options.preloadKeys())];
			for (int index = 0; index < points.length; index++) {
				long key = (long) index * options.preloadKeys() / points.length;
				points[index] = GetRequest.newBuilder()
						.setTransactionOrUpdateId(0)
						.setColumnId(columnId)
						.addKeys(keyByteString(key))
						.build();
			}

			long rangePositions = options.preloadKeys() - options.rangeWidth() + 1L;
			int rangeCount = (int) Math.min(options.rangeRequestCount(), rangePositions);
			RangeCase[] ranges = new RangeCase[rangeCount];
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
				GetRangeRequest request = GetRangeRequest.newBuilder()
						.setTransactionId(0)
						.setColumnId(columnId)
						.addStartKeysInclusive(first)
						.addEndKeysExclusive(keyByteString(end))
						.setTimeoutMs(DEADLINE_MILLIS)
						.build();
				ranges[index] = new RangeCase(request, first, last);
			}

			return new Requests(
					points,
					ranges,
					writerRequests(columnId, options.foregroundWriters(), 1L << 60,
							it.cavallium.rockserver.core.common.api.proto.WriteClass.FOREGROUND,
							options.valueBytes()),
					writerRequests(columnId, options.maintenanceWriters(), 1L << 61,
							it.cavallium.rockserver.core.common.api.proto.WriteClass.MAINTENANCE,
							options.valueBytes()),
					writerRequests(columnId, options.cancellationWorkers(), 3L << 60,
							it.cavallium.rockserver.core.common.api.proto.WriteClass.MAINTENANCE,
							options.valueBytes()));
		}

		private static PutRequest[][] writerRequests(long columnId,
				int workers,
				long keyBase,
				it.cavallium.rockserver.core.common.api.proto.WriteClass writeClass,
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
							.setWriteClass(writeClass)
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
		private final AdmissionTracker admission = new AdmissionTracker();
		private volatile long measurementStartedNanos;
		private volatile long measurementStoppedNanos;

		private PhaseControl(int workerCount, int maxLatencySamples) {
			this.ready = new CountDownLatch(workerCount);
			this.metrics = new PhaseMetrics(maxLatencySamples);
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

		private PhaseResult snapshot(Phase phase,
				long durationNanos,
				AdmissionResult admission,
				ResourceResult resources) {
			EnumMap<Operation, OperationResult> results = new EnumMap<>(Operation.class);
			for (Operation operation : Operation.values()) {
				results.put(operation, operations.get(operation).snapshot(durationNanos));
			}
			return new PhaseResult(phase,
					TimeUnit.NANOSECONDS.toMillis(durationNanos),
					Map.copyOf(results),
					warmupErrors.sum(),
					List.copyOf(errors),
					admission,
					resources);
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

	private static final class AdmissionTracker {

		private final AtomicInteger maxForegroundQueue = new AtomicInteger();
		private final AtomicInteger maxMaintenanceQueue = new AtomicInteger();
		private final AtomicInteger maxForegroundActive = new AtomicInteger();
		private final AtomicInteger maxMaintenanceActive = new AtomicInteger();
		private final AtomicInteger maxTotalActive = new AtomicInteger();

		private void sample(EmbeddedConnection embedded) {
			var snapshot = embedded.getScheduler().writeAdmissionSnapshot();
			int foregroundQueue = snapshot.foregroundQueued();
			int maintenanceQueue = snapshot.maintenanceQueued();
			int foregroundActive = snapshot.foregroundActive();
			int maintenanceActive = snapshot.maintenanceActive();
			maxForegroundQueue.accumulateAndGet(foregroundQueue, Math::max);
			maxMaintenanceQueue.accumulateAndGet(maintenanceQueue, Math::max);
			maxForegroundActive.accumulateAndGet(foregroundActive, Math::max);
			maxMaintenanceActive.accumulateAndGet(maintenanceActive, Math::max);
			maxTotalActive.accumulateAndGet(foregroundActive + maintenanceActive, Math::max);
		}

		private AdmissionResult finish(EmbeddedConnection embedded, PhaseMetrics metrics) {
			sample(embedded);
			var snapshot = embedded.getScheduler().writeAdmissionSnapshot();
			return new AdmissionResult(
					maxForegroundQueue.get(),
					maxMaintenanceQueue.get(),
					snapshot.foregroundQueued(),
					snapshot.maintenanceQueued(),
					maxForegroundActive.get(),
					maxMaintenanceActive.get(),
					maxTotalActive.get(),
					metrics.foregroundRejections(),
					metrics.maintenanceRejections());
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

	private record Client(ManagedChannel channel,
			RocksDBServiceGrpc.RocksDBServiceBlockingStub blocking,
			RocksDBServiceGrpc.RocksDBServiceFutureStub future) implements AutoCloseable {

		private static Client open(int port) {
			ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", port)
					.directExecutor()
					.usePlaintext()
					.disableRetry()
					.maxInboundMessageSize(64 * 1024 * 1024)
					.build();
			return new Client(channel,
					RocksDBServiceGrpc.newBlockingStub(channel),
					RocksDBServiceGrpc.newFutureStub(channel));
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

	private record PhaseResult(Phase phase,
			long durationMillis,
			Map<Operation, OperationResult> operations,
			long warmupErrors,
			List<String> errorDetails,
			AdmissionResult admission,
			ResourceResult resources) {

		private OperationResult operation(Operation operation) {
			return operations.get(operation);
		}

		private long unexpectedErrors() {
			long measured = operations.values().stream().mapToLong(OperationResult::errors).sum();
			// FOREGROUND is an aggregate of three concrete operations; do not count it twice.
			return measured - operation(Operation.FOREGROUND).errors() + warmupErrors;
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
			long maintenanceRejected) {
	}

	private record ResourceResult(int foregroundQueued,
			int maintenanceQueued,
			int foregroundActive,
			int maintenanceActive,
			long pendingOperations,
			int openTransactions,
			int openIterators,
			int activeRangeCursors,
			int iteratorLeases,
			long drainMillis,
			boolean drained) {
	}

	private record BenchmarkResult(String schema,
			Instant started,
			Instant finished,
			Options options,
			List<PhaseResult> phases,
			boolean shutdownClean,
			long nativeLeaksDetected,
			Acceptance acceptance) {
	}

	/** Minimal inputs for the pure acceptance gate. */
	public record GateInput(long foregroundDeadlines,
			long firstLastDeadlines,
			long foregroundOnlyP99Nanos,
			long maintenanceFloodP99Nanos,
			long maintenanceSuccesses,
			long cancellations,
			int maxTotalActive,
			int maxMaintenanceActive,
			int writeLimit,
			int maintenanceLimit,
			boolean resourcesDrained,
			long unexpectedErrors,
			long foregroundRejections,
			long nativeLeaksDetected,
			boolean shutdownClean) {
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
			int preloadKeys,
			int preloadFlushKeys,
			int valueBytes,
			int warmupSeconds,
			int measureSeconds,
			int pointReaders,
			int foregroundWriters,
			int maintenanceWriters,
			int firstLastReaders,
			int cancellationWorkers,
			long foregroundWriteRate,
			long maintenanceWriteRate,
			long firstLastRate,
			long cancellationRate,
			int cancellationDelayMillis,
			int cancellationBurst,
			int pointRequestCount,
			int rangeRequestCount,
			int rangeWidth,
			int readParallelism,
			int writeParallelism,
			int maintenanceWriteParallelism,
			int foregroundQueueCapacity,
			int maintenanceQueueCapacity,
			int admissionSampleMicros,
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
				"preload-keys",
				"preload-flush-keys",
				"value-bytes",
				"warmup-seconds",
				"measure-seconds",
				"point-readers",
				"foreground-writers",
				"maintenance-writers",
				"first-last-readers",
				"cancellation-workers",
				"foreground-write-rate",
				"maintenance-write-rate",
				"first-last-rate",
				"cancellation-rate",
				"cancellation-delay-ms",
				"cancellation-burst",
				"point-request-count",
				"range-request-count",
				"range-width",
				"read-parallelism",
				"write-parallelism",
				"maintenance-write-parallelism",
				"foreground-queue-capacity",
				"maintenance-queue-capacity",
				"admission-sample-micros",
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
					integer(values, "preload-keys", smoke ? 10_000 : 1_000_000),
					integer(values, "preload-flush-keys", smoke ? 2_000 : 50_000),
					integer(values, "value-bytes", 256),
					integer(values, "warmup-seconds", smoke ? 1 : 15),
					integer(values, "measure-seconds", smoke ? 2 : 60),
					integer(values, "point-readers", smoke ? 2 : 8),
					integer(values, "foreground-writers", smoke ? 2 : 4),
					integer(values, "maintenance-writers", smoke ? 8 : 64),
					integer(values, "first-last-readers", smoke ? 1 : 2),
					integer(values, "cancellation-workers", smoke ? 1 : 2),
					longValue(values, "foreground-write-rate", smoke ? 100 : 1_000),
					longValue(values, "maintenance-write-rate", 0),
					longValue(values, "first-last-rate", smoke ? 10 : 50),
					longValue(values, "cancellation-rate", smoke ? 64 : 100),
					integer(values, "cancellation-delay-ms", 1),
					integer(values, "cancellation-burst", WRITE_REQUEST_VARIANTS),
					integer(values, "point-request-count", smoke ? 1_024 : 8_192),
					integer(values, "range-request-count", smoke ? 1_024 : 8_192),
					integer(values, "range-width", smoke ? 64 : 1_024),
					integer(values, "read-parallelism", 20),
					integer(values, "write-parallelism", 36),
					integer(values, "maintenance-write-parallelism", 1),
					integer(values, "foreground-queue-capacity", 4_096),
					integer(values, "maintenance-queue-capacity", 512),
					integer(values, "admission-sample-micros", 250),
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
			if (preloadKeys < 2 || preloadFlushKeys < 1 || valueBytes < 1) {
				throw new IllegalArgumentException("preload and value sizes must be positive");
			}
			if (warmupSeconds < 0 || measureSeconds < 1) {
				throw new IllegalArgumentException("warmup must be non-negative and measurement must be positive");
			}
			if (pointReaders < 1 || foregroundWriters < 1 || maintenanceWriters < 1
					|| firstLastReaders < 1 || cancellationWorkers < 0) {
				throw new IllegalArgumentException("foreground/read/maintenance workers must be positive");
			}
			if (foregroundWriteRate < 0 || maintenanceWriteRate < 0 || firstLastRate < 0
					|| cancellationRate < 0) {
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
			if ((long) measureSeconds * cancellationRate < (long) cancellationWorkers * cancellationBurst) {
				throw new IllegalArgumentException("measurement window must contain at least one cancellation burst");
			}
			if (pointRequestCount < 1 || rangeRequestCount < 1 || rangeWidth < 1 || rangeWidth > preloadKeys) {
				throw new IllegalArgumentException("request counts and range width are invalid for the preload");
			}
			if (readParallelism < 1 || writeParallelism < 1 || maintenanceWriteParallelism < 1
					|| maintenanceWriteParallelism > writeParallelism) {
				throw new IllegalArgumentException("parallelism limits are invalid");
			}
			if (foregroundQueueCapacity < 1 || maintenanceQueueCapacity < 1) {
				throw new IllegalArgumentException("queue capacities must be positive");
			}
			if (admissionSampleMicros < 1 || maxLatencySamples < 1) {
				throw new IllegalArgumentException("sampling settings must be positive");
			}
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
