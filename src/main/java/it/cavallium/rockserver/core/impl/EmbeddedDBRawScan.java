package it.cavallium.rockserver.core.impl;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.SstFileReaderIterator;
import org.rocksdb.SstFileReader;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.Options;
import org.rocksdb.util.SizeUnit;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

/**
 * Raw SST scans of {@link EmbeddedDB}: SST file pinning (hard links), pin recovery and the raw scan state machine.
 *
 * <p>Moved out of {@link EmbeddedDB} unchanged; {@code edb} is the owning database, whose state
 * (fields, locks, executors) stays in {@link EmbeddedDB}.
 */
final class EmbeddedDBRawScan {

	private final EmbeddedDB edb;

	EmbeddedDBRawScan(EmbeddedDB edb) {
		this.edb = edb;
	}

	private Mono<RawScanPinnedSstSet> scheduleRawScanPinCapture(
			Callable<RawScanPinnedSstSet> capture) {
		return Mono.create(sink -> {
			var task = new RawScanPinCaptureTask(capture, sink);
			sink.onCancel(task::cancelDelivery);
			sink.onDispose(task::deliveryDisposed);
			try {
				edb.rawScanPinCaptureExecutor.execute(task);
				// Cancellation may win before execute() publishes the task to the
				// queue. Retry removal after publication so canceled entries never
				// consume the bounded capture admission budget.
				task.removeIfCancelled();
			} catch (RejectedExecutionException rejection) {
				task.reject(rejection);
			}
		});
	}

	final class RawScanPinCaptureTask implements Runnable {

		private static final int QUEUED = 0;
		private static final int RUNNING = 1;
		private static final int FINISHED = 2;
		private static final int CANCELLED = 3;

		private final Callable<RawScanPinnedSstSet> capture;
		private final MonoSink<RawScanPinnedSstSet> sink;
		private final AtomicInteger state = new AtomicInteger(QUEUED);
		private @Nullable Object pendingResult;
		private boolean deliveryCancelled;

		private RawScanPinCaptureTask(Callable<RawScanPinnedSstSet> capture,
				MonoSink<RawScanPinnedSstSet> sink) {
			this.capture = capture;
			this.sink = sink;
		}

		private void cancelDelivery() {
			Object cancelledResult;
			synchronized (this) {
				deliveryCancelled = true;
				cancelledResult = pendingResult;
				pendingResult = null;
			}
			cleanupCancelledResult(cancelledResult);
			if (state.compareAndSet(QUEUED, CANCELLED)) {
				edb.rawScanPinCaptureExecutor.remove(this);
			}
		}

		private void deliveryDisposed() {
			Object cancelledResult = null;
			synchronized (this) {
				if (deliveryCancelled) {
					cancelledResult = pendingResult;
				}
				pendingResult = null;
			}
			cleanupCancelledResult(cancelledResult);
		}

		private void removeIfCancelled() {
			if (state.get() == CANCELLED) {
				edb.rawScanPinCaptureExecutor.remove(this);
			}
		}

		void reject(RejectedExecutionException rejection) {
			if (!state.compareAndSet(QUEUED, FINISHED)) {
				return;
			}
			if (!isDeliveryCancelled()) {
				sink.error(RocksDBException.of(
							RocksDBErrorType.SERVER_OVERLOADED,
							edb.rawScanPinCaptureExecutor.isShutdown()
									? "Raw-scan pin capture executor is shutting down"
									: "Raw-scan pin capture queue is full",
							rejection));
			}
		}

		@Override
		public void run() {
			if (!state.compareAndSet(QUEUED, RUNNING)) {
				return;
			}
			try {
				RawScanPinnedSstSet result = capture.call();
				publishResult(result);
			} catch (Throwable captureFailure) {
				if (!isDeliveryCancelled()) {
					sink.error(captureFailure);
				} else {
					edb.logger.debug("Raw-scan pin capture failed after subscriber cancellation", captureFailure);
				}
			} finally {
				state.set(FINISHED);
			}
		}

		private boolean isDeliveryCancelled() {
			synchronized (this) {
				return deliveryCancelled;
			}
		}

		private void publishResult(RawScanPinnedSstSet result) {
			Object cancelledResult = null;
			boolean emit;
			synchronized (this) {
				pendingResult = ReactorResultOwnership.encode(result);
				emit = !deliveryCancelled;
				if (!emit) {
					cancelledResult = pendingResult;
					pendingResult = null;
				}
			}
			if (emit) {
				sink.success(result);
			} else {
				cleanupCancelledResult(cancelledResult);
			}
		}

		private void cleanupCancelledResult(@Nullable Object encodedResult) {
			if (encodedResult == null) return;
			try {
				ReactorResultOwnership.<RawScanPinnedSstSet>decode(encodedResult).close();
			} catch (Throwable cleanupFailure) {
				edb.logger.warn("Failed to clean raw-scan pins captured after subscriber cancellation",
						cleanupFailure);
			}
		}
	}

	/**
	 * RocksDB counts disable/enable file-deletion calls. A failed enable must therefore
	 * be retried, otherwise one transient error leaves obsolete-file deletion disabled
	 * for the rest of the process lifetime.
	 */
	final class RawScanFileDeletionRecovery {
		private static final long RETRY_DELAY_MILLIS = 1_000L;

		private int pendingEnables;
		private boolean retryScheduled;
		private boolean stopped;

		private synchronized void release(RocksDB rocksDB) {
			if (stopped) {
				return;
			}
			pendingEnables++;
			drain(rocksDB);
		}

		private void drain(RocksDB rocksDB) {
			while (!stopped && pendingEnables > 0) {
				try {
					edb.enableRawScanFileDeletions(rocksDB);
					pendingEnables--;
				} catch (org.rocksdb.RocksDBException error) {
					edb.logger.warn("Failed to re-enable RocksDB file deletions after raw-scan SST pinning; "
							+ "retrying in {} ms",
							RETRY_DELAY_MILLIS,
							error);
					scheduleRetry(rocksDB);
					return;
				}
			}
		}

		private void scheduleRetry(RocksDB rocksDB) {
			if (retryScheduled || stopped) {
				return;
			}
			retryScheduled = true;
			try {
				edb.leakScheduler.schedule(() -> retry(rocksDB), RETRY_DELAY_MILLIS, TimeUnit.MILLISECONDS);
			} catch (RuntimeException schedulingFailure) {
				retryScheduled = false;
				if (!stopped) {
					edb.logger.error("Failed to schedule recovery of RocksDB file deletions; "
							+ "the next raw-scan release will retry", schedulingFailure);
				}
			}
		}

		private synchronized void retry(RocksDB rocksDB) {
			retryScheduled = false;
			drain(rocksDB);
		}

		synchronized void stop() {
			stopped = true;
			pendingEnables = 0;
		}
	}

	private static Path rawScanSourcePath(LiveFileMetaData file) {
		String filePath = file.path();
		if (!filePath.endsWith(".sst")) {
			filePath += file.fileName();
		}
		return Path.of(filePath).toAbsolutePath().normalize();
	}

	private static boolean isRawScanAdmissionOverload(Throwable failure) {
		return failure instanceof RocksDBException rocksDBException
				&& rocksDBException.getErrorUniqueId() == RocksDBErrorType.SERVER_OVERLOADED;
	}

	static boolean isRetryableRawScanAdmissionOverload(Throwable failure) {
		// ScanState appends every failed cleanup stage to the original admission
		// failure before publishing it. Retrying is safe only when releasing the pin
		// claim, column use, operation lease, and test/metric hooks all succeeded.
		return isRawScanAdmissionOverload(failure) && failure.getSuppressed().length == 0;
	}

	private static boolean isSelectedRawScanFile(LiveFileMetaData file,
			String cfName,
			int shardIndex,
			int shardCount) {
		return file.fileName().endsWith(".sst")
				&& new String(file.columnFamilyName(), StandardCharsets.UTF_8).equals(cfName)
				&& (shardCount == 1
						|| Math.floorMod(file.fileName().hashCode(), shardCount)
						== Math.floorMod(shardIndex, shardCount));
	}

	private List<LiveFileMetaData> selectedRawScanFiles(RocksDB rocksDB,
			String cfName,
			int shardIndex,
			int shardCount) {
		return rocksDB.getLiveFilesMetaData()
				.stream()
				.filter(file -> isSelectedRawScanFile(file, cfName, shardIndex, shardCount))
				.sorted(Comparator.comparingInt(LiveFileMetaData::level)
						.thenComparingLong(LiveFileMetaData::smallestSeqno)
						.thenComparing(LiveFileMetaData::fileName))
				.toList();
	}

	private static Path rawScanDatabasePinRoot(Path sourceDirectory, Path databasePath) {
		return sourceDirectory
				.toAbsolutePath()
				.normalize()
				.resolve(EmbeddedDB.RAW_SCAN_PIN_DIRECTORY_NAME)
				.resolve(UUID.nameUUIDFromBytes(databasePath
						.toAbsolutePath()
						.normalize()
						.toString()
						.getBytes(StandardCharsets.UTF_8)).toString());
	}

	void recoverRawScanPinsAtStartup(RocksDB rocksDB,
			Path databasePath,
			DatabaseConfig databaseConfig) throws IOException {
		var sourceDirectories = new LinkedHashSet<Path>();
		sourceDirectories.add(databasePath.toAbsolutePath().normalize());
		try {
			GlobalDatabaseConfig globalConfig = databaseConfig.global();
			addRawScanConfiguredSourceDirectories(sourceDirectories,
					databasePath,
					globalConfig.fallbackColumnOptions());
			for (NamedColumnConfig columnConfig : globalConfig.columnOptions()) {
				addRawScanConfiguredSourceDirectories(sourceDirectories, databasePath, columnConfig);
			}
		} catch (GestaltException configFailure) {
			throw new IOException("Failed to resolve raw-scan SST volumes during startup recovery", configFailure);
		}
		for (LiveFileMetaData file : rocksDB.getLiveFilesMetaData()) {
			Path source = rawScanSourcePath(file);
			Path sourceDirectory = source.getParent();
			if (sourceDirectory != null) {
				sourceDirectories.add(sourceDirectory);
			}
		}

		long recoveredFiles = 0L;
		int recoveredRoots = 0;
		for (Path sourceDirectory : sourceDirectories) {
			Path databasePinRoot = rawScanDatabasePinRoot(sourceDirectory, databasePath);
			if (Files.notExists(databasePinRoot)) {
				continue;
			}
			long filesInRoot;
			try (var paths = Files.walk(databasePinRoot)) {
				filesInRoot = paths.filter(Files::isRegularFile).count();
			}
			deleteRawScanPinTree(databasePinRoot);
			recoveredFiles += filesInRoot;
			recoveredRoots++;
		}
		if (recoveredRoots > 0) {
			edb.logger.warn("Removed {} orphaned raw-scan SST hard links from {} pin roots during startup",
					recoveredFiles,
					recoveredRoots);
		}
	}

	private static void addRawScanConfiguredSourceDirectories(Set<Path> sourceDirectories,
			Path databasePath,
			FallbackColumnConfig columnConfig) throws GestaltException {
		for (RocksDBLoader.DbPathRecord volume : RocksDBLoader.getVolumeConfigs(databasePath, columnConfig)) {
			sourceDirectories.add(volume.path().toAbsolutePath().normalize());
		}
	}

	private Path prepareRawScanPinRoot(Path sourceDirectory) throws IOException {
		Path databasePinRoot = rawScanDatabasePinRoot(sourceDirectory, edb.definitiveDbPath);
		synchronized (edb.rawScanPinInitializationLock) {
			if (edb.initializedRawScanPinRoots.add(databasePinRoot)) {
				// A process crash cannot run Reactor cleanup. Remove only this database's
				// prior pin namespace, before entering the deletion-disabled critical section.
				deleteRawScanPinTree(databasePinRoot);
			}
			Files.createDirectories(databasePinRoot);
		}
		return databasePinRoot;
	}

	private static void deleteRawScanPinTree(Path root) throws IOException {
		if (Files.notExists(root)) {
			return;
		}
		try (var paths = Files.walk(root)) {
			for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
				Files.deleteIfExists(path);
			}
		}
	}

	private void checkRawScanPinDeadline(long startedNanos) throws IOException {
		long maximumDurationNanos = edb.rawScanPinMaxDurationNanos();
		if (System.nanoTime() - startedNanos > maximumDurationNanos) {
			throw new IOException("Raw-scan SST pinning exceeded the configured maximum duration of "
					+ TimeUnit.NANOSECONDS.toMillis(maximumDurationNanos)
					+ " ms");
		}
	}

	private static final class RawScanPinDirectory implements AutoCloseable {
		private final Path path;
		private int retainedFiles;
		private boolean closeRequested;
		private boolean closed;

		private RawScanPinDirectory(Path path) {
			this.path = path;
		}

		private synchronized void retainFile() {
			if (closed || closeRequested) {
				throw new IllegalStateException("Raw-scan pin directory is already closing: " + path);
			}
			retainedFiles++;
		}

		private synchronized void releaseFile() {
			if (retainedFiles <= 0) {
				throw new IllegalStateException("Raw-scan pin directory released without ownership: " + path);
			}
			retainedFiles--;
			if (closeRequested && retainedFiles == 0) {
				deleteDirectory();
			}
		}

		@Override
		public synchronized void close() {
			if (closed) {
				return;
			}
			closeRequested = true;
			if (retainedFiles == 0) {
				deleteDirectory();
			}
		}

		private void deleteDirectory() {
			try {
				Files.deleteIfExists(path);
			} catch (IOException failure) {
				throw new UncheckedIOException("Failed to release raw-scan pin directory " + path, failure);
			}
			closed = true;
		}
	}

	private final class PinnedSstFile implements AutoCloseable {
		private final LiveFileMetaData metadata;
		private final Path path;
		private final long size;
		private final @Nullable RawSstToken token;
		private final RawScanPinDirectory directory;
		private boolean claimed;
		private boolean closeRequested;
		private boolean closed;

		private PinnedSstFile(LiveFileMetaData metadata,
				Path path,
				@Nullable RawSstToken token,
				RawScanPinDirectory directory) {
			this.metadata = metadata;
			this.path = path;
			this.size = metadata.size();
			this.token = token;
			this.directory = directory;
			if (!edb.activeRawScanPinnedFiles.add(path)) {
				throw new IllegalStateException("Raw-scan SST pin path is already active: " + path);
			}
			try {
				directory.retainFile();
			} catch (RuntimeException | Error failure) {
				edb.activeRawScanPinnedFiles.remove(path);
				throw failure;
			}
			edb.activeRawScanPinnedBytes.addAndGet(size);
		}

		private String fileName() {
			return metadata.fileName();
		}

		private Path path() {
			return path;
		}

		private RawSstToken resumableToken() {
			return Objects.requireNonNull(token, "Resumable raw scan SST token");
		}

		private synchronized Claim claim() {
			if (closed || closeRequested || claimed) {
				throw new IllegalStateException("Raw-scan SST pin cannot be claimed: " + path);
			}
			claimed = true;
			return new Claim(this);
		}

		@Override
		public synchronized void close() {
			// The scan-set owns unclaimed files. Once a ScanState claims a pin, only
			// that state may unlink it after its iterator, reader, and options close.
			if (closed) {
				return;
			}
			if (claimed) {
				closeRequested = true;
				return;
			}
			closeClaimedOrUnclaimed();
		}

		private synchronized void releaseClaim() {
			if (closed) {
				return;
			}
			if (!claimed) {
				throw new IllegalStateException("Raw-scan SST pin released without a claim: " + path);
			}
			closeClaimedOrUnclaimed();
		}

		private synchronized void releaseClaimForAdmissionRetry() {
			if (closed) {
				return;
			}
			if (!claimed) {
				throw new IllegalStateException("Raw-scan SST pin retry released without a claim: " + path);
			}
			claimed = false;
			// Cancellation may close the owning scan set while scheduler rejection is
			// unwinding. Honor that deferred close instead of leaving an unowned pin.
			if (closeRequested) {
				closeClaimedOrUnclaimed();
			}
		}

		private void closeClaimedOrUnclaimed() {
			try {
				Files.deleteIfExists(path);
			} catch (IOException failure) {
				throw new UncheckedIOException("Failed to release raw-scan SST pin " + path, failure);
			}
			closed = true;
			claimed = false;
			edb.activeRawScanPinnedFiles.remove(path);
			edb.activeRawScanPinnedBytes.addAndGet(-size);
			directory.releaseFile();
		}

		private static final class Claim implements AutoCloseable {

			private final PinnedSstFile file;
			private final AtomicBoolean closed = new AtomicBoolean();

			private Claim(PinnedSstFile file) {
				this.file = file;
			}

			@Override
			public void close() {
				if (closed.compareAndSet(false, true)) {
					file.releaseClaim();
				}
			}

			private void releaseForAdmissionRetry() {
				if (closed.compareAndSet(false, true)) {
					file.releaseClaimForAdmissionRetry();
				}
			}
		}
	}

	/**
	 * Captures immutable SST contents without holding RocksDB's database-wide
	 * file-deletion switch for the lifetime of the scan. Targets live beside their
	 * source files, so hard links cannot cross a filesystem. There is deliberately no
	 * copy fallback: copying a multi-terabyte column while deletion is disabled would
	 * recreate the unbounded WAL/obsolete-file retention this lease is meant to avoid.
	 */
	private final class RawScanPinnedSstSet implements AutoCloseable {
		private final List<PinnedSstFile> files;
		private final Set<RawScanPinDirectory> scanDirectories;
		private boolean closed;

		private RawScanPinnedSstSet(String cfName,
				int shardIndex,
				int shardCount,
				@Nullable Set<String> completedSstFileNames)
				throws IOException, org.rocksdb.RocksDBException {
			var pinnedFiles = new ArrayList<PinnedSstFile>();
			var createdScanDirectories = new LinkedHashSet<RawScanPinDirectory>();
			var scanId = UUID.randomUUID().toString();
			Throwable captureFailure = null;
			synchronized (edb.rawScanPinAcquisitionLock) {
				RocksDB rocksDB = edb.db.get();
				for (LiveFileMetaData file : selectedRawScanFiles(rocksDB, cfName, shardIndex, shardCount)) {
					if (completedSstFileNames != null && completedSstFileNames.contains(file.fileName())) {
						continue;
					}
					Path source = rawScanSourcePath(file);
					Path sourceDirectory = Objects.requireNonNull(source.getParent(),
							"Raw-scan SST has no parent directory: " + source);
					prepareRawScanPinRoot(sourceDirectory);
				}

				rocksDB.disableFileDeletions();
				long startedNanos = System.nanoTime();
				try {
					var scanDirectoriesBySource = new HashMap<Path, RawScanPinDirectory>();
					for (LiveFileMetaData file : selectedRawScanFiles(rocksDB,
							cfName,
							shardIndex,
							shardCount)) {
						if (completedSstFileNames != null && completedSstFileNames.contains(file.fileName())) {
							continue;
						}
						Path source = rawScanSourcePath(file);
						RawSstToken token = completedSstFileNames == null
								? null
								: new RawSstToken(file.fileName());
						checkRawScanPinDeadline(startedNanos);
						Path sourceDirectory = Objects.requireNonNull(source.getParent(),
								"Raw-scan SST has no parent directory: " + source);
						RawScanPinDirectory scanDirectory = scanDirectoriesBySource.get(sourceDirectory);
						if (scanDirectory == null) {
							Path scanDirectoryPath = prepareRawScanPinRoot(sourceDirectory).resolve(scanId);
							Files.createDirectories(scanDirectoryPath);
							scanDirectory = new RawScanPinDirectory(scanDirectoryPath);
							scanDirectoriesBySource.put(sourceDirectory, scanDirectory);
							createdScanDirectories.add(scanDirectory);
						}
						Path pinnedPath = scanDirectory.path.resolve(source.getFileName().toString());
						edb.createRawScanHardLink(source, pinnedPath);
						pinnedFiles.add(new PinnedSstFile(file, pinnedPath, token, scanDirectory));
						checkRawScanPinDeadline(startedNanos);
					}
				} catch (Throwable failure) {
					captureFailure = failure;
					edb.rawScanPinAcquisitionFailures.increment();
				} finally {
					edb.rawScanFileDeletionRecovery.release(rocksDB);
					edb.rawScanPinAcquisitionTimer.record(System.nanoTime() - startedNanos, TimeUnit.NANOSECONDS);
				}
			}

			if (captureFailure != null) {
				captureFailure = cleanupPinnedFiles(pinnedFiles, createdScanDirectories, captureFailure);
				if (captureFailure instanceof IOException ioFailure) {
					throw ioFailure;
				}
				if (captureFailure instanceof RuntimeException runtimeFailure) {
					throw runtimeFailure;
				}
				if (captureFailure instanceof Error error) {
					throw error;
				}
				throw new IOException("Failed to pin raw-scan SST files", captureFailure);
			}
			this.files = List.copyOf(pinnedFiles);
			this.scanDirectories = Set.copyOf(createdScanDirectories);
		}

		private List<PinnedSstFile> files() {
			return files;
		}

		@Override
		public synchronized void close() {
			if (closed) {
				return;
			}
			closed = true;
			Throwable failure = cleanupPinnedFiles(files, scanDirectories, null);
			if (failure != null) {
				throw new IllegalStateException("Failed to clean up raw-scan SST pins", failure);
			}
		}
	}

	private static @Nullable Throwable cleanupPinnedFiles(Collection<PinnedSstFile> files,
			Collection<RawScanPinDirectory> scanDirectories,
			@Nullable Throwable originalFailure) {
		Throwable failure = originalFailure;
		for (PinnedSstFile file : files) {
			try {
				file.close();
			} catch (Throwable cleanupFailure) {
				failure = EmbeddedDBExistsMulti.appendFailure(failure, cleanupFailure);
			}
		}
		for (RawScanPinDirectory scanDirectory : scanDirectories) {
			try {
				scanDirectory.close();
			} catch (Throwable cleanupFailure) {
				failure = EmbeddedDBExistsMulti.appendFailure(failure, cleanupFailure);
			}
		}
		return failure;
	}

	@FunctionalInterface
	private interface RawScanTerminalMapper<T> {

		T map(@Nullable SerializedKVBatch finalBatch, RawSstToken token);
	}

	private final class ScanState<T> implements RWScheduler.CooperativeCompletionTask,
			LongConsumer,
			Disposable {
		private static final VarHandle DEMAND =
				EmbeddedDB.varHandle(ScanState.class, "demand", long.class);
		private static final VarHandle TERMINATED =
				EmbeddedDB.varHandle(ScanState.class, "terminated", boolean.class);
		private final ColumnInstance col;
		private final ColumnInstance.ColumnUse retainedColumnUse;
		private final String cfName;
		private final PinnedSstFile file;
		private final PinnedSstFile.Claim fileClaim;
		private final FluxSink<T> sink;
		private final long maximumQuantumNanos;
		private final Function<SerializedKVBatch, T> batchMapper;
		private final @Nullable RawScanTerminalMapper<T> terminalMapper;
		private volatile long demand;
		private volatile boolean terminated;
		private volatile boolean cancellationRequested;
		private volatile boolean dispatched;
		private volatile @Nullable RWScheduler.CooperativeHandle handle;
		private @Nullable SstFileReader reader;
		private @Nullable SstFileReaderIterator it;
		private @Nullable ReadOptions readOptions;
		private @Nullable Options options;
		private final Buf outBuf = Buf.create(Math.toIntExact(SizeUnit.MB));
		private int batchSize;
		private int currentBatchBytes;
		private int currentSerializedBatchBytes = Integer.BYTES;
		private boolean completionPrepared;
		private boolean resourcesCleaned;
		private @Nullable Throwable resourceCleanupFailure;
		private @Nullable T pendingSuccessfulEmission;

		private ScanState(ColumnInstance col,
		                  String cfName,
		                  PinnedSstFile file,
		                  FluxSink<T> sink,
		                  long maximumQuantumNanos,
		                  Function<SerializedKVBatch, T> batchMapper,
		                  @Nullable RawScanTerminalMapper<T> terminalMapper) {
			// Reactor eagerly releases the outer scan/session resources on cancellation.
			// Retain the database, column, and this specific pin until the scheduler has
			// returned from the last JNI quantum and closeNativeResources finishes.
			edb.ops.beginOp();
			ColumnInstance.ColumnUse acquiredColumnUse = null;
			PinnedSstFile.Claim acquiredFileClaim = null;
			try {
				acquiredColumnUse = col.acquireUse();
				acquiredFileClaim = file.claim();
				this.col = col;
				this.retainedColumnUse = acquiredColumnUse;
				this.cfName = cfName;
				this.file = file;
				this.fileClaim = acquiredFileClaim;
				this.sink = sink;
				this.maximumQuantumNanos = maximumQuantumNanos;
				this.batchMapper = batchMapper;
				this.terminalMapper = terminalMapper;
			} catch (RuntimeException | Error failure) {
				if (acquiredFileClaim != null) {
					acquiredFileClaim.close();
				}
				if (acquiredColumnUse != null) {
					acquiredColumnUse.close();
				}
				edb.ops.endOp();
				throw failure;
			}
		}

		private void attach(RWScheduler.CooperativeHandle handle) {
			this.handle = Objects.requireNonNull(handle, "handle");
			if (cancellationRequested) {
				handle.dispose();
			} else if (demand > 0L) {
				handle.resume();
			}
		}

		private void open() throws org.rocksdb.RocksDBException {
			if (it != null) {
				return;
			}
			String filePath = file.path().toString();

			Options createdOptions = null;
			SstFileReader createdReader = null;
			ReadOptions createdReadOptions = null;
			SstFileReaderIterator createdIterator = null;
			try {
				ColumnFamilyOptions cfOpts = edb.columnsConifg.get(cfName);
				createdOptions = cfOpts != null ? new Options(edb.dbOptions, cfOpts) : new Options();
				createdOptions.setAllowMmapReads(true);
				createdOptions.setUseDirectReads(false);
				createdOptions.setUseDirectIoForFlushAndCompaction(false);
				createdOptions.setParanoidChecks(false);

				createdReader = new SstFileReader(createdOptions);
				createdReader.open(filePath);
				createdReadOptions = new ReadOptions()
						.setFillCache(false)
						.setIgnoreRangeDeletions(true)
						.setVerifyChecksums(true)
						.setReadaheadSize(edb.workloadSettings.rawScanReadaheadBytes());

				createdIterator = createdReader.newIterator(createdReadOptions);
				createdIterator.seekToFirst();
			} catch (org.rocksdb.RocksDBException | RuntimeException | Error e) {
				edb.logger.warn("Failed to open captured SST file: " + file.fileName(), e);
				try {
					if (createdIterator != null) createdIterator.close();
				} catch (Throwable closeFailure) {
					e.addSuppressed(closeFailure);
				}
				try {
					if (createdReadOptions != null) createdReadOptions.close();
				} catch (Throwable closeFailure) {
					e.addSuppressed(closeFailure);
				}
				try {
					if (createdReader != null) createdReader.close();
				} catch (Throwable closeFailure) {
					e.addSuppressed(closeFailure);
				}
				try {
					if (createdOptions != null) createdOptions.close();
				} catch (Throwable closeFailure) {
					e.addSuppressed(closeFailure);
				}
				throw e;
			}
			options = createdOptions;
			reader = createdReader;
			readOptions = createdReadOptions;
			it = createdIterator;
			var readerOpenedObserver = edb.rawScanReaderOpenedObserver;
			if (readerOpenedObserver != null) {
				readerOpenedObserver.run();
			}
		}

		@Override
		public synchronized RWScheduler.CooperativeResult runCooperatively(
				RWScheduler.CooperativeContext context) {
			dispatched = true;
			// RocksJava native iterators have no protection against a concurrent close.
			// Serialize each JNI quantum with reject/terminal cleanup because gRPC
			// cancellation arrives on a transport thread while this worker may still be
			// inside isValid/key/value/next.
			if (terminated) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			// executeCooperatively may dispatch before it returns the stable handle. Park
			// that first quantum so cancellation and demand can only act through the
			// scheduler-owned logical submission before any native handle is opened.
			if (handle == null) {
				return RWScheduler.CooperativeResult.PARK;
			}
			if (context.terminationRequested()) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			if (demand == 0L) {
				return RWScheduler.CooperativeResult.PARK;
			}
			try {
				open();
				long preemptionStartNanos = 0L;
				while (true) {
					if (context.terminationRequested()) {
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					var iterator = Objects.requireNonNull(it, "Raw scan iterator");
					if (!iterator.isValid()) {
						iterator.status();
						return prepareEndOfFile(context);
					}

					batchSize++;
					byte[] k = iterator.key();
					byte[] v = iterator.value();
					var kBuf = Buf.wrap(k);
					var vBuf = Buf.wrap(v);
					currentBatchBytes += k.length + v.length;

					currentSerializedBatchBytes += col.transcodeBatchKeys(kBuf, vBuf, currentSerializedBatchBytes, outBuf);

					if (col.schema().hasValue()) {
						Buf userValue = col.decodeValue(vBuf);
						var valLen = userValue.size();
						EmbeddedDBRanges.ensureCapacity(outBuf, currentSerializedBatchBytes, Integer.BYTES);
						outBuf.setIntLE(currentSerializedBatchBytes, valLen);
						currentSerializedBatchBytes += Integer.BYTES;
						if (valLen > 0) {
							EmbeddedDBRanges.ensureCapacity(outBuf, currentSerializedBatchBytes, valLen);
							outBuf.setBytesFromBuf(currentSerializedBatchBytes, userValue, 0, valLen);
							currentSerializedBatchBytes += userValue.size();
						}
					} else {
						EmbeddedDBRanges.ensureCapacity(outBuf, currentSerializedBatchBytes, Integer.BYTES);
						outBuf.setIntLE(currentSerializedBatchBytes, 0);
						currentSerializedBatchBytes += Integer.BYTES;
					}

					iterator.next();
					if (batchSize >= EmbeddedDB.RAW_SCAN_MAX_ENTRIES_PER_CHUNK
							|| currentBatchBytes >= EmbeddedDB.RAW_SCAN_MAX_BYTES_PER_CHUNK) {
						if (!iterator.isValid()) {
							iterator.status();
							return prepareEndOfFile(context);
						}
						emitBatch();
						if (demand == 0L) {
							return RWScheduler.CooperativeResult.PARK;
						}
					}

					if (context.preemptionRequested()) {
						long nowNanos = System.nanoTime();
						if (preemptionStartNanos == 0L) {
							preemptionStartNanos = nowNanos;
						} else if (nowNanos - preemptionStartNanos >= maximumQuantumNanos) {
							// Publish useful progress at the same bounded checkpoint where the
							// scheduler yields. Retaining a partial 2 MiB/65,536-row wire batch
							// across paced BATCH turns can make a healthy raw scan appear idle
							// for minutes even though every quantum advances its SST iterator.
							if (batchSize > 0) {
								emitBatch();
								if (demand == 0L) {
									return RWScheduler.CooperativeResult.PARK;
								}
							}
							return RWScheduler.CooperativeResult.YIELD;
						}
					} else {
						preemptionStartNanos = 0L;
					}
				}
			} catch (VirtualMachineError fatal) {
				throw fatal;
			} catch (Throwable failure) {
				context.fail(EmbeddedDBExistsMulti.cooperativeFailure(failure,
						"Cooperative raw scan failed"));
				return RWScheduler.CooperativeResult.COMPLETE;
			}
		}

		private RWScheduler.CooperativeResult prepareEndOfFile(RWScheduler.CooperativeContext context) {
			var localTerminalMapper = terminalMapper;
			if (localTerminalMapper == null) {
				if (batchSize > 0) {
					emitBatch();
				}
			} else {
				var finalBatch = batchSize > 0 ? takeBatch() : null;
				T terminalEmission = Objects.requireNonNull(
						localTerminalMapper.map(finalBatch, file.resumableToken()),
						"Raw scan terminal mapper returned null");
				producedOne();
				pendingSuccessfulEmission = terminalEmission;
			}
			if (context.terminationRequested()) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			prepareSuccessfulCompletion(context);
			return RWScheduler.CooperativeResult.COMPLETE;
		}

		private void prepareSuccessfulCompletion(RWScheduler.CooperativeContext context) {
			var failure = cleanupNativeResources(null, false);
			if (failure != null) {
				if (failure instanceof VirtualMachineError fatal) {
					throw fatal;
				}
				context.fail(EmbeddedDBExistsMulti.cooperativeFailure(failure,
						"Cooperative raw scan cleanup failed"));
				return;
			}
			completionPrepared = true;
		}

		@Override
		public synchronized void completeCooperatively() {
			if (!completionPrepared) {
				throw new IllegalStateException("Scheduler selected RUN before raw scan cleanup completed");
			}
			if (TERMINATED.compareAndSet(this, false, true)) {
				var terminalEmission = pendingSuccessfulEmission;
				pendingSuccessfulEmission = null;
				if (terminalEmission != null) {
					sink.next(terminalEmission);
				}
				sink.complete();
			}
		}

		private void emitBatch() {
			producedOne();
			sink.next(batchMapper.apply(takeBatch()));
		}

		private SerializedKVBatch takeBatch() {
			outBuf.setIntLE(0, batchSize);
			var batch = new SerializedKVBatch.SerializedKVBatchRef(
					outBuf.copyOfRange(0, currentSerializedBatchBytes));
			batchSize = 0;
			currentBatchBytes = 0;
			currentSerializedBatchBytes = Integer.BYTES;
			return batch;
		}

		private void producedOne() {
			while (true) {
				long current = demand;
				if (current == Long.MAX_VALUE) {
					return;
				}
				if (current <= 0L) {
					throw new IllegalStateException("Raw scan produced without downstream demand");
				}
				if (DEMAND.compareAndSet(this, current, current - 1L)) {
					return;
				}
			}
		}

		@Override
		public void accept(long requested) {
			if (requested <= 0L || terminated) {
				return;
			}
			while (true) {
				long current = demand;
				long updated = current + requested;
				if (updated < 0L) {
					updated = Long.MAX_VALUE;
				}
				if (DEMAND.compareAndSet(this, current, updated)) {
					break;
				}
			}
			var currentHandle = handle;
			if (currentHandle != null) {
				currentHandle.resume();
			}
		}

		@Override
		public void reject(RuntimeException failure) {
			boolean admissionRetry = !dispatched && isRawScanAdmissionOverload(failure);
			finish(failure, !(failure instanceof CancellationException), admissionRetry);
		}

		@Override
		public void dispose() {
			if (terminated) {
				return;
			}
			cancellationRequested = true;
			var currentHandle = handle;
			if (currentHandle != null) {
				currentHandle.dispose();
			}
		}

		@Override
		public boolean isDisposed() {
			return terminated;
		}

		private synchronized void finish(@Nullable Throwable originalFailure,
				boolean signalTerminal,
				boolean admissionRetry) {
			if (!TERMINATED.compareAndSet(this, false, true)) {
				return;
			}
			pendingSuccessfulEmission = null;
			var failure = cleanupNativeResources(originalFailure, admissionRetry);
			if (!signalTerminal) {
				return;
			}
			if (failure == null) {
				sink.complete();
			} else {
				sink.error(failure);
			}
		}

		private @Nullable Throwable cleanupNativeResources(@Nullable Throwable originalFailure,
				boolean admissionRetry) {
			if (resourcesCleaned) {
				return resourceCleanupFailure == null
						? originalFailure
						: EmbeddedDBExistsMulti.appendFailure(originalFailure, resourceCleanupFailure);
			}
			resourcesCleaned = true;
			Throwable cleanupFailure = null;
			try {
				closeNativeResources(admissionRetry);
			} catch (Throwable closeFailure) {
				cleanupFailure = EmbeddedDBExistsMulti.appendFailure(cleanupFailure, closeFailure);
			}
			resourceCleanupFailure = cleanupFailure;
			return cleanupFailure == null
					? originalFailure
					: EmbeddedDBExistsMulti.appendFailure(originalFailure, cleanupFailure);
		}

		private void closeNativeResources(boolean admissionRetry) {
			Throwable failure = null;
			var iterator = it;
			it = null;
			if (iterator != null) {
				try {
					iterator.close();
				} catch (Throwable error) {
					failure = error;
				}
			}
			var localReadOptions = readOptions;
			readOptions = null;
			if (localReadOptions != null) {
				try {
					localReadOptions.close();
				} catch (Throwable error) {
					failure = EmbeddedDBExistsMulti.appendFailure(failure, error);
				}
			}
			var localReader = reader;
			reader = null;
			if (localReader != null) {
				try {
					localReader.close();
				} catch (Throwable error) {
					failure = EmbeddedDBExistsMulti.appendFailure(failure, error);
				}
			}
			var localOptions = options;
			options = null;
			if (localOptions != null) {
				try {
					localOptions.close();
				} catch (Throwable error) {
					failure = EmbeddedDBExistsMulti.appendFailure(failure, error);
				}
			}
			try {
				if (admissionRetry) {
					fileClaim.releaseForAdmissionRetry();
				} else {
					fileClaim.close();
				}
			} catch (Throwable error) {
				failure = EmbeddedDBExistsMulti.appendFailure(failure, error);
			}
			try {
				retainedColumnUse.close();
			} catch (Throwable error) {
				failure = EmbeddedDBExistsMulti.appendFailure(failure, error);
			}
			try {
				edb.ops.endOp();
			} catch (Throwable error) {
				failure = EmbeddedDBExistsMulti.appendFailure(failure, error);
			}
			var cleanupObserver = edb.rawScanCleanupObserver;
			if (cleanupObserver != null) {
				try {
					cleanupObserver.run();
				} catch (Throwable error) {
					failure = EmbeddedDBExistsMulti.appendFailure(failure, error);
				}
			}
			if (failure instanceof RuntimeException runtimeException) {
				throw runtimeException;
			}
			if (failure instanceof Error error) {
				throw error;
			}
			if (failure != null) {
				throw new IllegalStateException("Failed to close raw scan state", failure);
			}
		}
	}
	public Flux<SerializedKVBatch> scanRawAsyncInternal(long columnId, int shardIndex, int shardCount) {
		return scanRawAsyncInternal(columnId,
				shardIndex,
				shardCount,
				null,
				Function.identity(),
				null,
				edb.getSchedulerForColumn(columnId).scheduler(WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, Long.MAX_VALUE),
				edb.getSchedulerForColumn(columnId).executor(WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, Long.MAX_VALUE));
	}
	public Flux<SerializedKVBatch> scanRawAsyncInternal(long columnId,
			int shardIndex,
			int shardCount,
			@NotNull Scheduler workloadScheduler) {
		var workloadExecutor = workloadScheduler instanceof IndexedWorkloadScheduler indexedScheduler
				? indexedScheduler.workloadExecutor()
				: edb.getSchedulerForColumn(columnId).executor(WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, Long.MAX_VALUE);
		return scanRawAsyncInternal(columnId,
				shardIndex,
				shardCount,
				null,
				Function.identity(),
				null,
				workloadScheduler,
				workloadExecutor);
	}
	public Flux<SerializedKVBatch> scanRawAsyncInternal(long columnId,
			int shardIndex,
			int shardCount,
			@NotNull Scheduler workloadScheduler,
			@NotNull RWScheduler.WorkloadExecutor workloadExecutor) {
		return scanRawAsyncInternal(columnId,
				shardIndex,
				shardCount,
				null,
				Function.identity(),
				null,
				workloadScheduler,
				workloadExecutor);
	}

	private <T> Flux<T> scanRawAsyncInternal(long columnId,
			int shardIndex,
			int shardCount,
			@Nullable Set<String> completedSstFileNames,
			Function<SerializedKVBatch, T> batchMapper,
			@Nullable RawScanTerminalMapper<T> terminalMapper,
			@NotNull Scheduler workloadScheduler,
			@NotNull RWScheduler.WorkloadExecutor workloadExecutor) {
		long maximumQuantumNanos = edb.workloadSettings.rangeQuantumMaxDuration().toNanos();
		Flux<T> scan = Flux.using(
				() -> {
					edb.ops.beginOp();
					try {
						return edb.acquireColumnUse(columnId);
					} catch (Throwable error) {
						edb.ops.endOp();
						throw error;
					}
				},
				columnUse -> Flux.defer(() -> {
					ColumnInstance col = columnUse.column();
					String cfName;
					try {
						cfName = new String(col.cfh().getName(), StandardCharsets.UTF_8);
					} catch (RocksDBException | org.rocksdb.RocksDBException e) {
						return Flux.error(e);
					}

					return scheduleRawScanPinCapture(
								() -> new RawScanPinnedSstSet(cfName, shardIndex, shardCount, completedSstFileNames))
							.flatMapMany(capturedPins -> Flux.using(
									() -> capturedPins,
									pinnedSsts -> {
										var observer = edb.rawScanFilesCapturedObserver;
										if (observer != null) {
											observer.run();
										}

										Function<PinnedSstFile, Publisher<T>> mapper = file ->
												Flux.defer(() -> Flux.<T>create(rawSink -> {
													var state = new ScanState<>(col, cfName, file, rawSink,
															maximumQuantumNanos, batchMapper, terminalMapper);
													rawSink.onRequest(state);
													rawSink.onCancel(state);
													rawSink.onDispose(state);
													try {
														state.attach(workloadExecutor.executeCooperatively(
																state,
																EmbeddedDB.RAW_SCAN_MAX_BYTES_PER_CHUNK));
													} catch (RuntimeException admissionFailure) {
														state.reject(admissionFailure);
													}
												}, FluxSink.OverflowStrategy.ERROR))
														// Retry only pre-dispatch queue rejection. ScanState keeps the
														// same hard link available, while native/data failures remain terminal.
														.retryWhen(EmbeddedDB.RAW_SCAN_ADMISSION_RETRY)
														.publishOn(Schedulers.parallel(), 1);

										var ssts = Flux.fromIterable(pinnedSsts.files());
										if (shardCount == 1) {
											return ssts.flatMap(mapper, edb.workloadSettings.rawScanFileConcurrency(), 1);
										} else {
											return ssts.concatMap(mapper, 2);
										}
									},
									RawScanPinnedSstSet::close,
									true))
							.doOnDiscard(RawScanPinnedSstSet.class, RawScanPinnedSstSet::close);
				}),
				columnUse -> {
					try {
						columnUse.close();
					} finally {
						edb.ops.endOp();
					}
				},
				true);
		return subscribeOnInitialAdmission(scan, workloadScheduler);
	}

	private static <T> Flux<T> subscribeOnInitialAdmission(Flux<T> scan, Scheduler workloadScheduler) {
		if (!(workloadScheduler instanceof IndexedWorkloadScheduler indexedScheduler)) {
			return scan.subscribeOn(workloadScheduler);
		}
		return Mono.<Void>create(sink -> {
			var registration = Disposables.swap();
			sink.onCancel(registration);
			var initialAdmission = new RawScanInitialAdmission(sink);
			try {
				registration.update(indexedScheduler.executeWhenCapacity(initialAdmission));
			} catch (Throwable failure) {
				sink.error(failure);
			}
		}).thenMany(scan);
	}

	private record RawScanInitialAdmission(MonoSink<Void> sink)
			implements Runnable, RWScheduler.RejectionAwareTask {

		@Override
		public void run() {
			sink.success();
		}

		@Override
		public void reject(RuntimeException failure) {
			sink.error(failure);
		}
	}
	public Flux<RawScanEvent> scanRawResumableAsyncInternal(long columnId,
			int shardIndex,
			int shardCount,
			Set<RawSstToken> completedSsts,
			@NotNull Scheduler workloadScheduler) {
		var workloadExecutor = workloadScheduler instanceof IndexedWorkloadScheduler indexedScheduler
				? indexedScheduler.workloadExecutor()
				: edb.getSchedulerForColumn(columnId).executor(WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, Long.MAX_VALUE);
		return scanRawAsyncInternal(columnId,
				shardIndex,
				shardCount,
				completedSsts.stream()
						.map(RawSstToken::value)
						.collect(Collectors.toUnmodifiableSet()),
				batch -> new RawScanEvent.Batch(batch.serialized()),
				(finalBatch, token) -> finalBatch != null
						? new RawScanEvent.Batch(finalBatch.serialized(), token)
						: new RawScanEvent.SstCompleted(token),
				workloadScheduler,
				workloadExecutor);
	}
	public Stream<SerializedKVBatch> scanRaw(long columnId, int shardIndex, int shardCount) {
		return scanRawAsyncInternal(columnId, shardIndex, shardCount).toStream();
	}
}
