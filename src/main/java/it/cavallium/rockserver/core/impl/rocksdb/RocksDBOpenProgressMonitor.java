package it.cavallium.rockserver.core.impl.rocksdb;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import it.cavallium.rockserver.core.config.DataSize;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

/**
 * Observes the blocking native RocksDB open without adding a callback to every WAL read.
 *
 * <p>RocksDB does not expose WAL recovery byte progress. On Linux, the monitor samples the
 * identities and positions of this process' file descriptors for live WAL files. Direct I/O
 * uses {@code pread()}, which does not advance an FD position, so it also samples the exact
 * native-open thread's Linux I/O counters. The resulting progress is explicitly best-effort:
 * it distinguishes WAL replay, recovery flush, and another native-open phase, but it is not a
 * durability or correctness signal.</p>
 */
public final class RocksDBOpenProgressMonitor implements AutoCloseable {

	public static final String ACTIVE_METRIC = "rocksdb.startup.native.open.active";
	public static final String ELAPSED_METRIC = "rocksdb.startup.native.open.elapsed.seconds";
	public static final String PHASE_METRIC = "rocksdb.startup.native.open.observed.phase";
	public static final String LAST_ACTIVITY_AGE_METRIC =
			"rocksdb.startup.native.open.last.observed.activity.age.seconds";
	public static final String THREAD_IO_AVAILABLE_METRIC =
			"rocksdb.startup.native.open.thread.io.available";
	public static final String THREAD_LOGICAL_READ_BYTES_METRIC =
			"rocksdb.startup.native.open.thread.logical.read.bytes";
	public static final String THREAD_STORAGE_READ_BYTES_METRIC =
			"rocksdb.startup.native.open.thread.storage.read.bytes";
	public static final String CANDIDATE_WAL_BYTES_METRIC = "rocksdb.startup.wal.candidate.bytes";
	public static final String CANDIDATE_WAL_FILES_METRIC = "rocksdb.startup.wal.candidate.files";
	public static final String WAL_PROGRESS_AVAILABLE_METRIC = "rocksdb.startup.wal.progress.available";
	public static final String WAL_PROCESSED_BYTES_METRIC = "rocksdb.startup.wal.observed.processed.bytes";
	public static final String WAL_PROGRESS_METRIC = "rocksdb.startup.wal.observed.progress.ratio";
	public static final String WAL_READ_RATE_METRIC = "rocksdb.startup.wal.observed.read.rate.bytes.per.second";
	public static final String WAL_ETA_METRIC = "rocksdb.startup.wal.observed.eta.seconds";
	public static final String CURRENT_WAL_NUMBER_METRIC = "rocksdb.startup.wal.current.log.number";
	public static final String CURRENT_WAL_OFFSET_METRIC = "rocksdb.startup.wal.current.file.offset.bytes";
	public static final String CURRENT_WAL_SIZE_METRIC = "rocksdb.startup.wal.current.file.size.bytes";
	public static final String RECOVERY_FLUSH_BYTES_METRIC = "rocksdb.startup.wal.recovery.flush.bytes";
	public static final String COMPLETIONS_METRIC = "rocksdb.startup.native.open.completions";
	public static final String DURATION_METRIC = "rocksdb.startup.native.open.duration";

	private static final Pattern WAL_FILE_NAME = Pattern.compile("([0-9]+)\\.log");
	private static final Path PROC_FD_DIRECTORY = Path.of("/proc/self/fd");
	private static final Path PROC_FD_INFO_DIRECTORY = Path.of("/proc/self/fdinfo");
	private static final Path PROC_THREAD_SELF_DIRECTORY = Path.of("/proc/thread-self");
	private static final long SAMPLE_INTERVAL_NANOS = Duration.ofSeconds(1).toNanos();
	private static final long LOG_INTERVAL_NANOS = Duration.ofSeconds(30).toNanos();

	private final Logger logger;
	private final String databaseName;
	private final Path databasePath;
	private final Path walDirectory;
	private final WalInventory inventory;
	private final WalPositionProbe walPositionProbe;
	private final ThreadIoProbe threadIoProbe;
	private final LongSupplier recoveryFlushBytesSource;
	private final LongSupplier nanoTimeSource;
	private final boolean backgroundSampling;
	private final @Nullable Counter successCounter;
	private final @Nullable Counter failureCounter;
	private final @Nullable Timer successTimer;
	private final @Nullable Timer failureTimer;
	private final AtomicBoolean probeFailureLogged = new AtomicBoolean();
	private final AtomicBoolean threadIoFailureLogged = new AtomicBoolean();
	private final AtomicBoolean flushMetricFailureLogged = new AtomicBoolean();

	private volatile State state = State.PREPARED;
	private volatile ObservedPhase observedPhase = ObservedPhase.NATIVE_OPEN_OTHER;
	private volatile long startNanos;
	private volatile long finalElapsedNanos;
	private volatile long lastSampleNanos;
	private volatile long lastObservedActivityNanos;
	private volatile long nextLogNanos;
	private volatile long observedProcessedBytes;
	private volatile boolean walProgressAvailable;
	private volatile boolean threadIoAvailable;
	private volatile long nativeThreadLogicalReadBytes;
	private volatile long nativeThreadStorageReadBytes;
	private volatile boolean threadIoBaselineInitialized;
	private volatile long nativeThreadLogicalReadBaseline;
	private volatile long nativeThreadStorageReadBaseline;
	private volatile long trackedWalNumber = -1L;
	private volatile long trackedWalLogicalReadBaseline;
	private volatile long currentWalNumber = -1L;
	private volatile long currentWalOffsetBytes;
	private volatile long currentWalSizeBytes;
	private volatile double observedReadRateBytesPerSecond;
	private volatile double observedEtaSeconds = Double.NaN;
	private volatile long recoveryFlushBytes;
	private volatile @Nullable Thread samplerThread;

	static RocksDBOpenProgressMonitor prepare(Logger logger,
	                                          String databaseName,
	                                          Path databasePath,
	                                          Path walDirectory,
	                                          @Nullable MeterRegistry meterRegistry,
	                                          LongSupplier recoveryFlushBytesSource) {
		var inventory = scanCandidateWalFiles(walDirectory, logger);
		return new RocksDBOpenProgressMonitor(
				logger,
				databaseName,
				databasePath,
				walDirectory,
				inventory,
				new ProcWalPositionProbe(inventory.files()),
				ProcThreadIoProbe.captureCurrentThread(),
				recoveryFlushBytesSource,
				System::nanoTime,
				meterRegistry,
				true);
	}

	public RocksDBOpenProgressMonitor(Logger logger,
	                                  String databaseName,
	                                  Path databasePath,
	                                  Path walDirectory,
	                                  WalInventory inventory,
	                                  WalPositionProbe walPositionProbe,
	                                  ThreadIoProbe threadIoProbe,
	                                  LongSupplier recoveryFlushBytesSource,
	                                  LongSupplier nanoTimeSource,
	                                  @Nullable MeterRegistry meterRegistry,
	                                  boolean backgroundSampling) {
		this.logger = Objects.requireNonNull(logger, "logger");
		this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
		this.databasePath = Objects.requireNonNull(databasePath, "databasePath");
		this.walDirectory = Objects.requireNonNull(walDirectory, "walDirectory");
		this.inventory = Objects.requireNonNull(inventory, "inventory");
		this.walPositionProbe = Objects.requireNonNull(walPositionProbe, "walPositionProbe");
		this.threadIoProbe = Objects.requireNonNull(threadIoProbe, "threadIoProbe");
		this.recoveryFlushBytesSource = Objects.requireNonNull(recoveryFlushBytesSource,
				"recoveryFlushBytesSource");
		this.nanoTimeSource = Objects.requireNonNull(nanoTimeSource, "nanoTimeSource");
		this.backgroundSampling = backgroundSampling;

		if (meterRegistry == null) {
			this.successCounter = null;
			this.failureCounter = null;
			this.successTimer = null;
			this.failureTimer = null;
		} else {
			registerGauges(meterRegistry);
			this.successCounter = completionCounter(meterRegistry, "success");
			this.failureCounter = completionCounter(meterRegistry, "failure");
			this.successTimer = durationTimer(meterRegistry, "success");
			this.failureTimer = durationTimer(meterRegistry, "failure");
		}
	}

	public void start() {
		Thread threadToStart = null;
		synchronized (this) {
			if (state != State.PREPARED) {
				throw new IllegalStateException("RocksDB native-open monitor already started");
			}
			state = State.ACTIVE;
			startNanos = nanoTimeSource.getAsLong();
			lastSampleNanos = startNanos;
			lastObservedActivityNanos = startNanos;
			nextLogNanos = saturatedAdd(startNanos, LOG_INTERVAL_NANOS);
			sampleLocked(startNanos, false);
			if (backgroundSampling) {
				threadToStart = Thread.ofPlatform()
						.daemon(true)
						.name("rocksdb-open-monitor-" + databaseName)
						.unstarted(this::runSampler);
				samplerThread = threadToStart;
			}
		}

		logger.info("Starting RocksDB native open: database={}, database-path={}, wal-directory={}, "
						+ "candidate-live-wal-files={}, candidate-live-wal-bytes={} ({}), "
						+ "wal-byte-progress-observation={}; WAL recovery and recovery flushes are synchronous",
				databaseName,
				databasePath,
				walDirectory,
				inventory.files().size(),
				inventory.totalBytes(),
				new DataSize(inventory.totalBytes()),
				walProgressAvailable ? "linux-wal-fd-and-thread-io" : "unavailable");
		if (threadToStart != null) {
			threadToStart.start();
		}
	}

	public void succeeded() {
		finish(State.SUCCEEDED, null);
	}

	public void failed(Throwable failure) {
		finish(State.FAILED, Objects.requireNonNull(failure, "failure"));
	}

	public void sampleNow() {
		synchronized (this) {
			if (state == State.ACTIVE) {
				sampleLocked(nanoTimeSource.getAsLong(), true);
			}
		}
	}

	private void runSampler() {
		while (state == State.ACTIVE) {
			try {
				Thread.sleep(Duration.ofNanos(SAMPLE_INTERVAL_NANOS));
			} catch (InterruptedException ignored) {
				Thread.currentThread().interrupt();
				break;
			}
			sampleNow();
		}
	}

	private void finish(State outcome, @Nullable Throwable failure) {
		Thread threadToJoin;
		long elapsedNanos;
		synchronized (this) {
			if (state != State.ACTIVE) {
				return;
			}
			var now = nanoTimeSource.getAsLong();
			sampleLocked(now, false);
			finalElapsedNanos = Math.max(0L, now - startNanos);
			state = outcome;
			observedPhase = outcome == State.SUCCEEDED
					? ObservedPhase.COMPLETE
					: ObservedPhase.FAILED;
			currentWalNumber = -1L;
			currentWalOffsetBytes = 0L;
			currentWalSizeBytes = 0L;
			if (outcome == State.SUCCEEDED) {
				observedProcessedBytes = inventory.totalBytes();
				observedReadRateBytesPerSecond = 0.0d;
				observedEtaSeconds = 0.0d;
			}
			elapsedNanos = finalElapsedNanos;
			threadToJoin = samplerThread;
		}

		if (threadToJoin != null && threadToJoin != Thread.currentThread()) {
			threadToJoin.interrupt();
			boolean interrupted = false;
			while (threadToJoin.isAlive()) {
				try {
					threadToJoin.join();
				} catch (InterruptedException ignored) {
					interrupted = true;
				}
			}
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}

		if (outcome == State.SUCCEEDED) {
			if (successCounter != null) {
				successCounter.increment();
				Objects.requireNonNull(successTimer).record(elapsedNanos, TimeUnit.NANOSECONDS);
			}
			logger.info("Completed RocksDB native open: database={}, outcome=success, elapsed={}, "
							+ "candidate-live-wal-files={}, candidate-live-wal-bytes={} ({}), "
							+ "native-thread-logical-read-bytes={}, native-thread-storage-read-bytes={}, "
							+ "recovery-flush-bytes={} ({})",
					databaseName,
					formatDuration(elapsedNanos),
					inventory.files().size(),
					inventory.totalBytes(),
					new DataSize(inventory.totalBytes()),
					nativeThreadLogicalReadBytes,
					nativeThreadStorageReadBytes,
					recoveryFlushBytes,
					new DataSize(recoveryFlushBytes));
		} else {
			if (failureCounter != null) {
				failureCounter.increment();
				Objects.requireNonNull(failureTimer).record(elapsedNanos, TimeUnit.NANOSECONDS);
			}
			logger.error("Completed RocksDB native open: database={}, outcome=failure, elapsed={}, "
							+ "candidate-live-wal-files={}, candidate-live-wal-bytes={} ({}), "
							+ "observed-wal-processed-bytes={}, native-thread-logical-read-bytes={}, "
							+ "native-thread-storage-read-bytes={}, recovery-flush-bytes={}, error-type={}, error={}",
					databaseName,
					formatDuration(elapsedNanos),
					inventory.files().size(),
					inventory.totalBytes(),
					new DataSize(inventory.totalBytes()),
					observedProcessedBytes,
					nativeThreadLogicalReadBytes,
					nativeThreadStorageReadBytes,
					recoveryFlushBytes,
					failure == null ? "unknown" : failure.getClass().getName(),
					failure == null ? "native open ended without an outcome" : failure.getMessage(),
					failure);
		}
	}

	private void sampleLocked(long now, boolean allowProgressLog) {
		var previousProcessedBytes = observedProcessedBytes;
		var previousFlushBytes = recoveryFlushBytes;
		var previousLogicalReadBytes = nativeThreadLogicalReadBytes;
		var previousStorageReadBytes = nativeThreadStorageReadBytes;
		var sampleDurationNanos = Math.max(0L, now - lastSampleNanos);

		ThreadIoSnapshot threadIoSnapshot;
		try {
			threadIoSnapshot = threadIoProbe.sample();
		} catch (RuntimeException threadIoFailure) {
			threadIoSnapshot = ThreadIoSnapshot.unavailable();
			if (threadIoFailureLogged.compareAndSet(false, true)) {
				logger.warn("Failed to observe native-open thread I/O counters; "
						+ "direct-I/O WAL byte progress may be unavailable", threadIoFailure);
			}
		}
		threadIoAvailable = threadIoSnapshot.available();
		if (threadIoAvailable) {
			if (!threadIoBaselineInitialized) {
				nativeThreadLogicalReadBaseline = threadIoSnapshot.logicalReadChars();
				nativeThreadStorageReadBaseline = threadIoSnapshot.storageReadBytes();
				threadIoBaselineInitialized = true;
			}
			var observedLogicalReadBytes = nonNegativeDifference(
					threadIoSnapshot.logicalReadChars(), nativeThreadLogicalReadBaseline);
			if (observedLogicalReadBytes > nativeThreadLogicalReadBytes) {
				nativeThreadLogicalReadBytes = observedLogicalReadBytes;
			}
			var observedStorageReadBytes = nonNegativeDifference(
					threadIoSnapshot.storageReadBytes(), nativeThreadStorageReadBaseline);
			if (observedStorageReadBytes > nativeThreadStorageReadBytes) {
				nativeThreadStorageReadBytes = observedStorageReadBytes;
			}
		}

		ProbeSnapshot probeSnapshot;
		try {
			probeSnapshot = walPositionProbe.sample();
		} catch (RuntimeException probeFailure) {
			probeSnapshot = ProbeSnapshot.unavailable();
			if (probeFailureLogged.compareAndSet(false, true)) {
				logger.warn("Failed to observe RocksDB WAL recovery file offsets; "
						+ "startup elapsed and recovery-flush metrics remain available", probeFailure);
			}
		}
		walProgressAvailable = inventory.available() && probeSnapshot.available() && threadIoAvailable;

		WalPosition current = null;
		for (var position : probeSnapshot.positions()) {
			if (current == null
					|| position.logNumber() > current.logNumber()
					|| (position.logNumber() == current.logNumber()
					&& position.offsetBytes() > current.offsetBytes())) {
				current = position;
			}
		}

		if (current != null) {
			var walFile = inventory.byLogNumber().get(current.logNumber());
			if (walFile != null) {
				currentWalNumber = current.logNumber();
				var descriptorOffset = Math.min(walFile.sizeBytes(), Math.max(0L, current.offsetBytes()));
				if (trackedWalNumber != current.logNumber()) {
					trackedWalNumber = current.logNumber();
					trackedWalLogicalReadBaseline = threadIoAvailable
							? Math.max(0L, threadIoSnapshot.logicalReadChars() - descriptorOffset)
							: 0L;
				}
				var directIoOffset = threadIoAvailable
						? nonNegativeDifference(
						threadIoSnapshot.logicalReadChars(), trackedWalLogicalReadBaseline)
						: 0L;
				currentWalOffsetBytes = Math.min(walFile.sizeBytes(),
						Math.max(descriptorOffset, directIoOffset));
				currentWalSizeBytes = walFile.sizeBytes();
				var candidateProcessedBytes = saturatedAdd(walFile.bytesBefore(), currentWalOffsetBytes);
				var boundedProcessedBytes = Math.min(inventory.totalBytes(), candidateProcessedBytes);
				observedProcessedBytes = Math.max(previousProcessedBytes, boundedProcessedBytes);
				observedPhase = ObservedPhase.WAL_REPLAY;
			}
		} else {
			currentWalNumber = -1L;
			currentWalOffsetBytes = 0L;
			currentWalSizeBytes = 0L;
		}

		try {
			var currentRecoveryFlushBytes = Math.max(0L, recoveryFlushBytesSource.getAsLong());
			recoveryFlushBytes = Math.max(previousFlushBytes, currentRecoveryFlushBytes);
		} catch (RuntimeException flushMetricFailure) {
			if (flushMetricFailureLogged.compareAndSet(false, true)) {
				logger.warn("Failed to read RocksDB recovery flush byte statistics", flushMetricFailure);
			}
		}

		var walAdvanced = observedProcessedBytes > previousProcessedBytes;
		var flushAdvanced = recoveryFlushBytes > previousFlushBytes;
		var threadReadAdvanced = nativeThreadLogicalReadBytes > previousLogicalReadBytes
				|| nativeThreadStorageReadBytes > previousStorageReadBytes;
		if (walAdvanced || flushAdvanced || threadReadAdvanced) {
			lastObservedActivityNanos = now;
		}
		if (current == null) {
			observedPhase = flushAdvanced
					? ObservedPhase.RECOVERY_FLUSH
					: ObservedPhase.NATIVE_OPEN_OTHER;
		}

		if (sampleDurationNanos > 0L) {
			observedReadRateBytesPerSecond = (observedProcessedBytes - previousProcessedBytes)
					* 1_000_000_000.0d / sampleDurationNanos;
		}
		var remainingBytes = Math.max(0L, inventory.totalBytes() - observedProcessedBytes);
		observedEtaSeconds = observedReadRateBytesPerSecond > 0.0d
				? remainingBytes / observedReadRateBytesPerSecond
				: (remainingBytes == 0L ? 0.0d : Double.NaN);
		lastSampleNanos = now;

		if (allowProgressLog && now >= nextLogNanos) {
			logProgress(now);
			var followingLogNanos = nextLogNanos;
			do {
				followingLogNanos = saturatedAdd(followingLogNanos, LOG_INTERVAL_NANOS);
			} while (followingLogNanos <= now);
			nextLogNanos = followingLogNanos;
		}
	}

	private void logProgress(long now) {
		var progressRatio = progressRatio();
		var currentWal = currentWalNumber >= 0L
				? currentWalNumber + ".log"
				: "none-observed";
		logger.info("RocksDB native open progress: database={}, elapsed={}, observed-phase={}, "
						+ "candidate-live-wal-files={}, candidate-live-wal-bytes={} ({}), "
						+ "wal-progress-available={}, observed-wal-processed-bytes={} ({}), "
						+ "observed-wal-progress-percent={}, current-wal={}, current-wal-offset-bytes={}, "
						+ "current-wal-size-bytes={}, observed-wal-read-rate-bytes-per-second={}, "
						+ "observed-eta-seconds={}, native-thread-logical-read-bytes={}, "
						+ "native-thread-storage-read-bytes={}, recovery-flush-bytes={} ({}), "
						+ "last-observed-activity-age-seconds={}",
				databaseName,
				formatDuration(Math.max(0L, now - startNanos)),
				observedPhase.metricTag,
				inventory.files().size(),
				inventory.totalBytes(),
				new DataSize(inventory.totalBytes()),
				walProgressAvailable,
				observedProcessedBytes,
				new DataSize(observedProcessedBytes),
				String.format(Locale.ROOT, "%.2f", progressRatio * 100.0d),
				currentWal,
				currentWalOffsetBytes,
				currentWalSizeBytes,
				Math.round(observedReadRateBytesPerSecond),
				Double.isFinite(observedEtaSeconds) ? Math.round(observedEtaSeconds) : "unknown",
				nativeThreadLogicalReadBytes,
				nativeThreadStorageReadBytes,
				recoveryFlushBytes,
				new DataSize(recoveryFlushBytes),
				lastObservedActivityAgeSeconds(now));
	}

	private void registerGauges(MeterRegistry registry) {
		gauge(ACTIVE_METRIC, "Whether blocking RocksDB native open is active", null,
				monitor -> monitor.state == State.ACTIVE ? 1.0d : 0.0d, registry);
		gauge(ELAPSED_METRIC, "Elapsed time in blocking RocksDB native open", "seconds",
				RocksDBOpenProgressMonitor::elapsedSeconds, registry);
		for (var phase : ObservedPhase.values()) {
			Gauge.builder(PHASE_METRIC, this,
							monitor -> monitor.observedPhase == phase ? 1.0d : 0.0d)
					.description("Best-effort currently observed RocksDB native-open phase")
					.tag("database", databaseName)
					.tag("phase", phase.metricTag)
					.strongReference(true)
					.register(registry);
		}
		gauge(LAST_ACTIVITY_AGE_METRIC,
				"Age of the last observed native-thread I/O, WAL offset, or recovery-flush advance",
				"seconds",
				monitor -> monitor.lastObservedActivityAgeSeconds(monitor.nanoTimeSource.getAsLong()),
				registry);
		gauge(THREAD_IO_AVAILABLE_METRIC,
				"Whether I/O counters for the exact native-open thread are observable",
				null, monitor -> monitor.threadIoAvailable ? 1.0d : 0.0d, registry);
		gauge(THREAD_LOGICAL_READ_BYTES_METRIC,
				"Logical bytes read by the native-open thread, including cached and direct reads",
				"bytes", monitor -> monitor.nativeThreadLogicalReadBytes, registry);
		gauge(THREAD_STORAGE_READ_BYTES_METRIC,
				"Physical storage bytes read by the native-open thread according to Linux task I/O",
				"bytes", monitor -> monitor.nativeThreadStorageReadBytes, registry);
		gauge(CANDIDATE_WAL_BYTES_METRIC,
				"Bytes in top-level live WAL candidates present before RocksDB native open",
				"bytes", monitor -> monitor.inventory.totalBytes(), registry);
		gauge(CANDIDATE_WAL_FILES_METRIC,
				"Top-level live WAL candidate files present before RocksDB native open",
				null, monitor -> monitor.inventory.files().size(), registry);
		gauge(WAL_PROGRESS_AVAILABLE_METRIC,
				"Whether Linux WAL descriptors and native-open thread I/O are currently observable",
				null, monitor -> monitor.walProgressAvailable ? 1.0d : 0.0d, registry);
		gauge(WAL_PROCESSED_BYTES_METRIC,
				"Best-effort WAL bytes processed based on ascending WAL order, descriptor offsets, and thread I/O",
				"bytes", monitor -> monitor.observedProcessedBytes, registry);
		gauge(WAL_PROGRESS_METRIC,
				"Best-effort fraction of initial live WAL candidate bytes processed",
				"ratio", RocksDBOpenProgressMonitor::progressRatio, registry);
		gauge(WAL_READ_RATE_METRIC,
				"Best-effort observed WAL progress rate",
				"bytes/second", monitor -> monitor.observedReadRateBytesPerSecond, registry);
		gauge(WAL_ETA_METRIC,
				"Best-effort WAL replay ETA at the most recent observed progress rate",
				"seconds", monitor -> monitor.observedEtaSeconds, registry);
		gauge(CURRENT_WAL_NUMBER_METRIC,
				"Currently observed live WAL log number, or -1 when none is observed",
				null, monitor -> monitor.currentWalNumber, registry);
		gauge(CURRENT_WAL_OFFSET_METRIC,
				"Best-effort current WAL read offset from its descriptor or native-open thread I/O",
				"bytes", monitor -> monitor.currentWalOffsetBytes, registry);
		gauge(CURRENT_WAL_SIZE_METRIC,
				"Size of the currently observed WAL file",
				"bytes", monitor -> monitor.currentWalSizeBytes, registry);
		gauge(RECOVERY_FLUSH_BYTES_METRIC,
				"RocksDB FLUSH_WRITE_BYTES observed during native open",
				"bytes", monitor -> monitor.recoveryFlushBytes, registry);
	}

	private void gauge(String name,
	                   String description,
	                   @Nullable String baseUnit,
	                   java.util.function.ToDoubleFunction<RocksDBOpenProgressMonitor> valueFunction,
	                   MeterRegistry registry) {
		var builder = Gauge.builder(name, this, valueFunction)
				.description(description)
				.tag("database", databaseName)
				.strongReference(true);
		if (baseUnit != null) {
			builder.baseUnit(baseUnit);
		}
		builder.register(registry);
	}

	private Counter completionCounter(MeterRegistry registry, String outcome) {
		return Counter.builder(COMPLETIONS_METRIC)
				.description("Completed blocking RocksDB native-open attempts")
				.tag("database", databaseName)
				.tag("outcome", outcome)
				.register(registry);
	}

	private Timer durationTimer(MeterRegistry registry, String outcome) {
		return Timer.builder(DURATION_METRIC)
				.description("Duration of blocking RocksDB native-open attempts")
				.tag("database", databaseName)
				.tag("outcome", outcome)
				.register(registry);
	}

	private double elapsedSeconds() {
		var elapsedNanos = state == State.ACTIVE
				? Math.max(0L, nanoTimeSource.getAsLong() - startNanos)
				: finalElapsedNanos;
		return elapsedNanos / 1_000_000_000.0d;
	}

	private double progressRatio() {
		if (inventory.totalBytes() == 0L) {
			return 1.0d;
		}
		return Math.min(1.0d, observedProcessedBytes / (double) inventory.totalBytes());
	}

	private double lastObservedActivityAgeSeconds(long now) {
		if (state == State.PREPARED) {
			return 0.0d;
		}
		var effectiveNow = state == State.ACTIVE
				? now
				: saturatedAdd(startNanos, finalElapsedNanos);
		return Math.max(0L, effectiveNow - lastObservedActivityNanos) / 1_000_000_000.0d;
	}

	@Override
	public void close() {
		if (state == State.ACTIVE) {
			failed(new IllegalStateException("native open monitor closed before an outcome was recorded"));
		}
	}

	public static WalInventory scanCandidateWalFiles(Path walDirectory, Logger logger) {
		if (!Files.isDirectory(walDirectory)) {
			return new WalInventory(true, List.of());
		}
		var rawFiles = new ArrayList<RawWalFile>();
		try (var paths = Files.list(walDirectory)) {
			paths.forEach(path -> {
				var matcher = WAL_FILE_NAME.matcher(path.getFileName().toString());
				if (!matcher.matches() || !Files.isRegularFile(path)) {
					return;
				}
				try {
					rawFiles.add(new RawWalFile(
							Long.parseLong(matcher.group(1)),
							path.toRealPath(),
							Files.size(path)));
				} catch (IOException | NumberFormatException fileFailure) {
					logger.warn("Could not include WAL candidate {} in native-open progress metrics",
							path, fileFailure);
				}
			});
		} catch (IOException directoryFailure) {
			logger.warn("Could not inventory live WAL candidates before RocksDB native open: {}",
					walDirectory, directoryFailure);
			return new WalInventory(false, List.of());
		}
		rawFiles.sort(Comparator.comparingLong(RawWalFile::logNumber));
		var files = new ArrayList<WalFile>(rawFiles.size());
		long bytesBefore = 0L;
		for (var rawFile : rawFiles) {
			files.add(new WalFile(rawFile.logNumber(), rawFile.path(), rawFile.sizeBytes(), bytesBefore));
			bytesBefore = saturatedAdd(bytesBefore, rawFile.sizeBytes());
		}
		return new WalInventory(true, files);
	}

	private static String formatDuration(long nanos) {
		return Duration.ofNanos(Math.max(0L, nanos)).toString();
	}

	private static long saturatedAdd(long left, long right) {
		if (right > 0L && left > Long.MAX_VALUE - right) {
			return Long.MAX_VALUE;
		}
		return left + right;
	}

	private static long nonNegativeDifference(long value, long baseline) {
		return value >= baseline ? value - baseline : 0L;
	}

	enum ObservedPhase {
		NATIVE_OPEN_OTHER("native-open-other"),
		WAL_REPLAY("wal-replay"),
		RECOVERY_FLUSH("recovery-flush"),
		COMPLETE("complete"),
		FAILED("failed");

		private final String metricTag;

		ObservedPhase(String metricTag) {
			this.metricTag = metricTag;
		}
	}

	private enum State {
		PREPARED,
		ACTIVE,
		SUCCEEDED,
		FAILED
	}

	public record WalInventory(boolean available, List<WalFile> files, long totalBytes,
	                           Map<Long, WalFile> byLogNumber) {

		public WalInventory(boolean available, List<WalFile> files) {
			this(available, List.copyOf(files), totalBytes(files), index(files));
		}

		private static long totalBytes(List<WalFile> files) {
			long total = 0L;
			for (var file : files) {
				total = saturatedAdd(total, file.sizeBytes());
			}
			return total;
		}

		private static Map<Long, WalFile> index(List<WalFile> files) {
			var index = new LinkedHashMap<Long, WalFile>();
			for (var file : files) {
				index.put(file.logNumber(), file);
			}
			return Map.copyOf(index);
		}
	}

	public record WalFile(long logNumber, Path path, long sizeBytes, long bytesBefore) {
	}

	public record WalPosition(long logNumber, long offsetBytes) {
	}

	public record ProbeSnapshot(boolean available, List<WalPosition> positions) {

		public ProbeSnapshot {
			positions = List.copyOf(positions);
		}

		static ProbeSnapshot unavailable() {
			return new ProbeSnapshot(false, List.of());
		}
	}

	@FunctionalInterface
	public interface WalPositionProbe {
		ProbeSnapshot sample();
	}

	public record ThreadIoSnapshot(boolean available, long logicalReadChars, long storageReadBytes) {

		static ThreadIoSnapshot unavailable() {
			return new ThreadIoSnapshot(false, 0L, 0L);
		}
	}

	@FunctionalInterface
	public interface ThreadIoProbe {
		ThreadIoSnapshot sample();
	}

	/**
	 * Samples Linux task-I/O counters for the thread which constructed this probe. Resolving the
	 * magic {@code /proc/thread-self} link eagerly is essential: the sampler runs on another thread,
	 * while the captured task is the one blocked inside the native RocksDB open call.
	 */
	public static final class ProcThreadIoProbe implements ThreadIoProbe {

		private final @Nullable Path taskIoPath;

		private ProcThreadIoProbe(@Nullable Path taskIoPath) {
			this.taskIoPath = taskIoPath;
		}

		public static ProcThreadIoProbe captureCurrentThread() {
			try {
				return new ProcThreadIoProbe(PROC_THREAD_SELF_DIRECTORY.resolve("io").toRealPath());
			} catch (IOException | RuntimeException unavailable) {
				return new ProcThreadIoProbe(null);
			}
		}

		@Override
		public ThreadIoSnapshot sample() {
			if (taskIoPath == null) {
				return ThreadIoSnapshot.unavailable();
			}
			long logicalReadChars = -1L;
			long storageReadBytes = -1L;
			try {
				for (var line : Files.readAllLines(taskIoPath)) {
					var separator = line.indexOf(':');
					if (separator < 0) {
						continue;
					}
					var key = line.substring(0, separator);
					var value = line.substring(separator + 1).trim();
					if ("rchar".equals(key)) {
						logicalReadChars = Long.parseLong(value);
					} else if ("read_bytes".equals(key)) {
						storageReadBytes = Long.parseLong(value);
					}
				}
			} catch (IOException | NumberFormatException unavailable) {
				return ThreadIoSnapshot.unavailable();
			}
			if (logicalReadChars < 0L || storageReadBytes < 0L) {
				return ThreadIoSnapshot.unavailable();
			}
			return new ThreadIoSnapshot(true, logicalReadChars, storageReadBytes);
		}
	}

	private record RawWalFile(long logNumber, Path path, long sizeBytes) {
	}

	public static final class ProcWalPositionProbe implements WalPositionProbe {

		private final Map<Path, Long> logNumbersByPath;

		public ProcWalPositionProbe(List<WalFile> files) {
			var index = new HashMap<Path, Long>();
			for (var file : files) {
				index.put(file.path().toAbsolutePath().normalize(), file.logNumber());
			}
			this.logNumbersByPath = Map.copyOf(index);
		}

		@Override
		public ProbeSnapshot sample() {
			if (!Files.isDirectory(PROC_FD_DIRECTORY) || !Files.isDirectory(PROC_FD_INFO_DIRECTORY)) {
				return ProbeSnapshot.unavailable();
			}
			var positions = new ArrayList<WalPosition>();
			try (DirectoryStream<Path> descriptors = Files.newDirectoryStream(PROC_FD_DIRECTORY)) {
				for (var descriptor : descriptors) {
					try {
						var target = Files.readSymbolicLink(descriptor);
						if (!target.isAbsolute()) {
							target = descriptor.getParent().resolve(target);
						}
						var logNumber = logNumbersByPath.get(target.toAbsolutePath().normalize());
						if (logNumber == null) {
							continue;
						}
						var offset = readDescriptorOffset(PROC_FD_INFO_DIRECTORY.resolve(
								descriptor.getFileName().toString()));
						if (offset >= 0L) {
							positions.add(new WalPosition(logNumber, offset));
						}
					} catch (IOException | NumberFormatException ignored) {
						// File descriptors can close or be reused between the directory and fdinfo reads.
					}
				}
			} catch (IOException directoryFailure) {
				return ProbeSnapshot.unavailable();
			}
			return new ProbeSnapshot(true, positions);
		}

		private static long readDescriptorOffset(Path fdInfo) throws IOException {
			for (var line : Files.readAllLines(fdInfo)) {
				if (line.startsWith("pos:")) {
					return Long.parseLong(line.substring("pos:".length()).trim());
				}
			}
			return -1L;
		}
	}
}
