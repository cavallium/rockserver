package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.common.Utils.dummyRocksDBEmptyValue;
import static it.cavallium.rockserver.core.common.Utils.emptyBuf;
import static it.cavallium.rockserver.core.common.Utils.toBuf;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RequestType.RequestDelete;
import it.cavallium.rockserver.core.common.RequestType.RequestEntriesCount;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestGetRange;
import it.cavallium.rockserver.core.common.RequestType.RequestMerge;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RequestType.RequestReduceRange;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Compact;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Flush;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.GetAllColumnDefinitions;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CreateColumn;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteColumn;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteColumnIfExists;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteRange;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.EstimateNumKeys;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.ExistsMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Get;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.GetColumnId;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Put;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.SeekTo;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream.GetRange;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.cavallium.rockserver.core.impl.rocksdb.TransactionalDB.TransactionalOptions;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.reactivestreams.Publisher;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.AbstractSlice;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.CompactRangeOptions.BottommostLevelCompaction;
import org.rocksdb.DBOptions;
import org.rocksdb.DirectSlice;
import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.ReadTier;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.Slice;
import org.rocksdb.SstFileReaderIterator;
import org.rocksdb.SstFileReader;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.Options;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Status.Code;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.Retry;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import org.rocksdb.TransactionLogIterator;
import org.rocksdb.WriteBatch;

public class EmbeddedDB implements RocksDBSyncAPI, InternalConnection, Closeable {

	private static VarHandle varHandle(Class<?> owner, String field, Class<?> type) {
		try {
			return MethodHandles.lookup().findVarHandle(owner, field, type);
		} catch (NoSuchFieldException | IllegalAccessException failure) {
			throw new ExceptionInInitializerError(failure);
		}
	}

	/** Output ownership requested by the embedded gRPC unary-Get fast path. */
	public enum FastGetOutput {
		EXACT_HEAP,
		PINNED,
		AUTOMATIC
	}

	/**
	 * A wire-ready current value. Pinned values remain valid only until this
	 * result is closed; heap and missing results are independently owned.
	 */
	public static final class FastGetResult implements AutoCloseable {

		private final boolean present;
		private final @Nullable Buf value;
		private final boolean pinned;
		private final @Nullable Runnable closeAction;
		private final AtomicBoolean closed = new AtomicBoolean();

		private FastGetResult(boolean present,
				@Nullable Buf value,
				boolean pinned,
				@Nullable Runnable closeAction) {
			this.present = present;
			this.value = value;
			this.pinned = pinned;
			this.closeAction = closeAction;
		}

		public boolean isPresent() {
			return present;
		}

		public @Nullable Buf value() {
			if (closed.get()) {
				throw new IllegalStateException("gRPC Get result has already been closed");
			}
			return value;
		}

		public boolean isPinned() {
			return pinned;
		}

		@Override
		public void close() {
			if (closed.compareAndSet(false, true) && closeAction != null) {
				closeAction.run();
			}
		}
	}

	private enum WriteElisionRequest {
		ENSURE("ensure"),
		PREVIOUS_PRESENCE("previous_presence");

		private final String tag;

		WriteElisionRequest(String tag) {
			this.tag = tag;
		}

		private static @Nullable WriteElisionRequest from(RequestPut<?, ?> requestType) {
			if (requestType instanceof RequestType.RequestEnsure<?>) {
				return ENSURE;
			}
			if (requestType instanceof RequestType.RequestPreviousPresence<?>) {
				return PREVIOUS_PRESENCE;
			}
			return null;
		}
	}

	private enum WriteElisionDecision {
		ELIDED("elided"),
		FALLBACK_NOT_FOUND("fallback_not_found"),
		FALLBACK_DIFFERENT("fallback_different"),
		FALLBACK_INCOMPLETE("fallback_incomplete"),
		BYPASS_OVERSIZED("bypass_oversized"),
		BYPASS_WRITER("bypass_writer");

		private final String tag;

		WriteElisionDecision(String tag) {
			this.tag = tag;
		}
	}

	private static final class PhysicalKey {

		private final byte[] bytes;
		private final int hashCode;

		private PhysicalKey(byte[] bytes) {
			this.bytes = bytes;
			this.hashCode = Arrays.hashCode(bytes);
		}

		@Override
		public boolean equals(Object other) {
			return this == other || other instanceof PhysicalKey key && Arrays.equals(bytes, key.bytes);
		}

		@Override
		public int hashCode() {
			return hashCode;
		}
	}

	private record MultiWriteElisionProbe(PhysicalKey[] physicalKeys, WriteElisionDecision[] decisions) {
	}

	private static final class BucketWriteElisionProbe {

		private final PhysicalKey physicalKey;
		private final ArrayList<Integer> logicalIndexes = new ArrayList<>();
		private int requiredSize;

		private BucketWriteElisionProbe(PhysicalKey physicalKey) {
			this.physicalKey = physicalKey;
		}
	}

	private static final int PINNED_GET_MIN_BYTES_OVERRIDE = Integer.getInteger(
			"rockserver.fast-get.pinned-min-bytes", -1);

	private static final long STORAGE_PRESSURE_PENDING_COMPACTION_BYTES = Math.max(1L, Long.getLong(
			"it.cavallium.rockserver.workload.storage-pressure-pending-compaction-bytes",
			64L * SizeUnit.GB));
	private static final int EXISTS_MULTI_MAX_KEYS_PER_NATIVE_CALL = 4_096;
	private static final long EXISTS_MULTI_MAX_KEY_BYTES_PER_NATIVE_CALL = 2 * SizeUnit.MB;
	private static final int EXISTS_MULTI_MAX_VARIABLE_HASH_BYTES = Arrays.stream(ColumnHashType.values())
			.mapToInt(ColumnHashType::bytesSize)
			.max()
			.orElse(0);
	private static final int WRITE_ELISION_MAX_KEYS_PER_NATIVE_CALL = 4_096;
	private static final long WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL = 2 * SizeUnit.MB;
	private static final int RAW_SCAN_MAX_ENTRIES_PER_CHUNK = 65_536;
	private static final long RAW_SCAN_MAX_BYTES_PER_CHUNK = 2 * SizeUnit.MB;
	// A logical raw scan spans many independently scheduled SST readers. Once its
	// immutable snapshot is pinned, transient queue pressure between files must pause
	// that scan rather than tear down and recapture the multi-terabyte snapshot.
	private static final Retry RAW_SCAN_ADMISSION_RETRY = Retry
			.backoff(Long.MAX_VALUE, Duration.ofMillis(10))
			.maxBackoff(Duration.ofSeconds(1))
			.jitter(0.5d)
			.filter(EmbeddedDB::isRawScanAdmissionOverload);
	private static final long RAW_SCAN_PIN_MAX_DURATION_NANOS = TimeUnit.MILLISECONDS.toNanos(Math.max(1L,
			Long.getLong("it.cavallium.rockserver.raw-scan.pin-max-duration-ms", 120_000L)));
	private static final String RAW_SCAN_PIN_DIRECTORY_NAME = ".rockserver-raw-scan-pins";
	public static final long MAX_TRANSACTION_DURATION_MS = 10_000L;
	private static final String SHUTDOWN_PENDING_OPS_TIMEOUT_MS_PROPERTY
			= "it.cavallium.rockserver.db.shutdown-pending-ops-timeout-ms";
	private static final byte[] COLUMN_SCHEMAS_COLUMN = "_column_schemas_".getBytes(StandardCharsets.UTF_8);
	private static final byte[] MERGE_OPERATORS_COLUMN = "_merge_operators_".getBytes(StandardCharsets.UTF_8);
	private static final byte[] CDC_META_COLUMN = "_cdc_meta_".getBytes(StandardCharsets.UTF_8);
	private static final List<byte[]> SYSTEM_COLUMNS = List.of(RocksDB.DEFAULT_COLUMN_FAMILY,
			COLUMN_SCHEMAS_COLUMN,
			MERGE_OPERATORS_COLUMN,
			CDC_META_COLUMN
	);
	private final Logger logger;
	private final ActionLoggerConsumer actionLogger;
	private final @Nullable Path path;
	private final @NotNull Path definitiveDbPath;
	private final TransactionalDB db;
	private final DBOptions dbOptions;
	private final RWScheduler scheduler;
	private final ScheduledExecutorService leakScheduler;
	private final ScheduledFuture<?> expiredRangeCleanupTask;
	private final ColumnFamilyHandle columnSchemasColumnDescriptorHandle;
	private final ColumnFamilyHandle mergeOperatorsColumnDescriptorHandle;
	private final ColumnFamilyHandle cdcMetaColumnDescriptorHandle;
	private final MergeOperatorRegistry mergeOperatorRegistry;
	private final NonBlockingHashMapLong<ColumnInstance> columns;
	private final Map<String, ColumnFamilyOptions> columnsConifg;
	private final ConcurrentMap<String, Long> columnNamesIndex;
	private final ConcurrentMap<String, ColumnFamilyHandle> unconfiguredColumns;
	private final NonBlockingHashMapLong<Tx> txs;
	private final NonBlockingHashMapLong<REntry<RocksIterator>> its;
	private final Set<ActiveRangeResource> activeRangeResources = ConcurrentHashMap.newKeySet();
	private final AtomicInteger retainedRangeSnapshots = new AtomicInteger();
	private final RetainedQueryLimiter retainedQueryLimiter;
	private final AtomicLong cdcPublishedTailSequence = new AtomicLong();
	private final ConcurrentMap<String, CdcSubscriptionProgress> cdcSubscriptionProgress = new ConcurrentHashMap<>();
	private final long maxRetainedSnapshotAgeMs;
	private final Set<CdcPollCursor> activeCdcPollCursors = ConcurrentHashMap.newKeySet();
	private final Set<AsyncExistsMultiRequest> activeExistsMultiRequests = ConcurrentHashMap.newKeySet();
	private final ConcurrentMap<String, CdcMetadataLock> cdcMetadataLocks = new ConcurrentHashMap<>();
	private final RawScanFileDeletionRecovery rawScanFileDeletionRecovery = new RawScanFileDeletionRecovery();
	private final Object rawScanPinAcquisitionLock = new Object();
	private final Object rawScanPinInitializationLock = new Object();
	private final Set<Path> initializedRawScanPinRoots = new HashSet<>();
	private final Set<Path> activeRawScanPinnedFiles = ConcurrentHashMap.newKeySet();
	private final AtomicLong activeRawScanPinnedBytes = new AtomicLong();
	private final Object asyncExistsMultiAdmissionLock = new Object();
	private final SafeShutdown ops;
	private final AtomicLong resourceLeases = new AtomicLong();
	private final Object columnEditLock = new Object();
	private final DatabaseConfig config;
	private final WorkloadSettings workloadSettings;
	private final RocksDBObjects refs;
	private final Map<String, Cache> caches;
	private final Map<String, Long> cacheCapacities;
	private final MetricsManager metrics;
	private final String name;
	private final List<Meter> meters = new ArrayList<>();
	private final Timer openTransactionTimer;
	private final Timer closeTransactionTimer;
	private final Timer closeFailedUpdateTimer;
	private final Timer createColumnTimer;
	private final Timer deleteColumnTimer;
	private final Timer deleteColumnIfExistsTimer;
	private final Timer getColumnIdTimer;
	private final Timer estimateNumKeysTimer;
	private final Timer putTimer;
	private final Timer putMultiTimer;
	private final Timer putBatchTimer;
	private final Timer deleteRangeTimer;
	private final Timer getTimer;
	private final Timer existsMultiTimer;
	private final Timer openIteratorTimer;
	private final Timer closeIteratorTimer;
	private final Timer seekToTimer;
	private final Timer subsequentTimer;
	private final Timer reduceRangeTimer;
	private final Timer getRangeTimer;
	private final Timer flushTimer;
	private final Timer compactTimer;
	private final Timer getAllColumnDefinitionsTimer;
	private final AtomicBoolean nativeDeleteRangeFallbackLogged = new AtomicBoolean();
	private final Counter cdcEventsEmitted;
	private final Counter cdcBytesEmitted;
	private final Counter rawScanPinAcquisitionFailures;
	private final Timer rawScanPinAcquisitionTimer;
	private final EnumMap<WriteElisionRequest, EnumMap<WriteElisionDecision, Counter>> writeElisionDecisionCounters;
	private final RocksDBStatistics rocksDBStatistics;
	private final boolean fastGet;
	private final @Nullable NativeRocksDBGet fastGetReader;
	private volatile @Nullable Consumer<Boolean> rangeReadOptionsObserver;
	private volatile @Nullable Consumer<Boolean> reduceRangeAsyncIoObserver;
	private volatile @Nullable Runnable rangeIteratorOpenObserver;
	private volatile @Nullable Consumer<Integer> rangeReadChunkSizeObserver;
	private volatile @Nullable Runnable rangeCountChunkObserver;
	private volatile @Nullable LongConsumer rangeCountQuantumItemsObserver;
	private volatile @Nullable Runnable rangeContinuationObserver;
	private volatile @Nullable LongConsumer retainedQueryPermitGrantedObserver;
	private volatile @Nullable Runnable retainedRangeCleanupObserver;
	private volatile @Nullable Runnable iteratorAdvanceStepCompletedObserver;
	private volatile @Nullable Runnable forcedShutdownObserver;
	private volatile @Nullable Runnable existsMultiChunkObserver;
	private volatile @Nullable Runnable existsMultiSnapshotObserver;
	private volatile @Nullable Consumer<Boolean> existsMultiArenaObserver;
	private volatile @Nullable BiConsumer<Integer, Long> writeElisionMultiGetObserver;
	private volatile @Nullable Runnable cdcPollTailCapturedObserver;
	private volatile @Nullable Runnable cdcWalIteratorOpenObserver;
	private volatile @Nullable LongConsumer cdcQuantumMutationsObserver;
	private volatile @Nullable LongConsumer cdcQuantumBytesObserver;
	private volatile @Nullable Runnable cdcLatestValueResolutionObserver;
	private volatile @Nullable Runnable cdcContinuationObserver;
	private volatile @Nullable BiConsumer<String, String> cdcMetadataOperationObserver;
	private volatile @Nullable BiConsumer<String, String> cdcMetadataLoadedObserver;
	private volatile @Nullable Runnable rawScanFilesCapturedObserver;
	private volatile @Nullable Runnable rawScanReaderOpenedObserver;
	private volatile @Nullable Runnable rawScanCleanupObserver;
	private volatile @Nullable Runnable columnMaintenanceObserver;
	private volatile @Nullable LongConsumer columnUseAcquiredObserver;
	private Path tempSSTsPath;

	public EmbeddedDB(@Nullable Path path, String name, @Nullable Path embeddedConfigPath) throws IOException {
		this.path = path;
		this.name = name;
		this.logger = LoggerFactory.getLogger("db." + name);
		this.columns = new NonBlockingHashMapLong<>();
		this.txs = new NonBlockingHashMapLong<>();
		this.its = new NonBlockingHashMapLong<>();
		this.columnNamesIndex = new ConcurrentHashMap<>();
		this.unconfiguredColumns = new ConcurrentHashMap<>();
		this.ops = new SafeShutdown();
		DatabaseConfig config = ConfigParser.parse(embeddedConfigPath);
		this.config = config;
		WorkloadSettings workloadSettings;
		try {
			workloadSettings = WorkloadSettings.resolve(config);
			this.fastGet = config.global().enableFastGet();
		} catch (GestaltException e) {
			throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR,
					"Can't resolve workload or fast-get configuration",
					e);
		}
		this.workloadSettings = workloadSettings;
		this.maxRetainedSnapshotAgeMs = workloadSettings.retainedSnapshotMaximumAge().toMillis();
		int readCap = workloadSettings.readParallelism();
		int writeCap = workloadSettings.writeParallelism();

		this.metrics = new MetricsManager(config);
		Timer loadTimer = createTimer(Tags.of("action", "load"));
		this.openTransactionTimer = createActionTimer(OpenTransaction.class);
		this.closeTransactionTimer = createActionTimer(CloseTransaction.class);
		this.closeFailedUpdateTimer = createActionTimer(CloseFailedUpdate.class);
		this.createColumnTimer = createActionTimer(CreateColumn.class);
		this.deleteColumnTimer = createActionTimer(DeleteColumn.class);
		this.deleteColumnIfExistsTimer = createActionTimer(DeleteColumnIfExists.class);
		this.getColumnIdTimer = createActionTimer(GetColumnId.class);
		this.estimateNumKeysTimer = createActionTimer(EstimateNumKeys.class);
		this.putTimer = createActionTimer(Put.class);
		this.putMultiTimer = createActionTimer(PutMulti.class);
		this.putBatchTimer = createActionTimer(PutBatch.class);
		this.deleteRangeTimer = createActionTimer(DeleteRange.class);
		this.getTimer = createActionTimer(Get.class);
		this.existsMultiTimer = createActionTimer(ExistsMulti.class);
		this.openIteratorTimer = createActionTimer(OpenIterator.class);
		this.closeIteratorTimer = createActionTimer(CloseIterator.class);
		this.seekToTimer = createActionTimer(SeekTo.class);
		this.subsequentTimer = createActionTimer(Subsequent.class);
		this.reduceRangeTimer = createActionTimer(ReduceRange.class);
		this.getRangeTimer = createActionTimer(GetRange.class);
		this.flushTimer = createActionTimer(Flush.class);
		this.compactTimer = createActionTimer(Compact.class);
		this.getAllColumnDefinitionsTimer = createActionTimer(GetAllColumnDefinitions.class);
		this.cdcEventsEmitted = metrics.getRegistry().counter("rockserver.cdc.events", "db", name);
		this.cdcBytesEmitted = metrics.getRegistry().counter("rockserver.cdc.bytes", "db", name);
		this.rawScanPinAcquisitionFailures = metrics.getRegistry().counter(
				"rockserver.raw.scan.pin.acquisition.failures",
				"database",
				name);
		this.rawScanPinAcquisitionTimer = Timer.builder("rockserver.raw.scan.pin.acquisition")
				.description("Time RocksDB file deletion is disabled while raw-scan SSTs are hard-linked")
				.tag("database", name)
				.register(metrics.getRegistry());
		Gauge.builder("rockserver.raw.scan.pinned.files", activeRawScanPinnedFiles, Set::size)
				.description("SST hard links currently retained by raw scans")
				.tag("database", name)
				.register(metrics.getRegistry());
		Gauge.builder("rockserver.raw.scan.pinned.bytes", activeRawScanPinnedBytes, AtomicLong::get)
				.description("Logical bytes in SST files currently retained by raw-scan hard links")
				.baseUnit("bytes")
				.tag("database", name)
				.register(metrics.getRegistry());
		this.writeElisionDecisionCounters = new EnumMap<>(WriteElisionRequest.class);
		for (var request : WriteElisionRequest.values()) {
			var counters = new EnumMap<WriteElisionDecision, Counter>(WriteElisionDecision.class);
			for (var decision : WriteElisionDecision.values()) {
				counters.put(decision, metrics.getRegistry().counter("rockserver.write.elision.decisions",
						"db", name,
						"request_type", request.tag,
						"decision", decision.tag));
			}
			writeElisionDecisionCounters.put(request, counters);
		}

		// Expose gauges to help detect potential resource leaks at runtime
		try {
			var registry = metrics.getRegistry();
			meters.add(Gauge
					.builder("rockserver.open.transactions", txs, m -> (double) m.size())
					.tag("db", name)
					.register(registry));
			meters.add(Gauge
					.builder("rockserver.open.iterators", its, m -> (double) m.size())
					.tag("db", name)
					.register(registry));
			meters.add(Gauge
					.builder("rockserver.pending.ops", this, db -> (double) db.getPendingOpsCount())
					.tag("db", name)
					.register(registry));
		} catch (Throwable ex) {
			logger.error("Failed to load metrics", ex);
		}

		if (Boolean.getBoolean("rockserver.core.print-actions")) {
			var m = MarkerFactory.getMarker("ACTION");
			this.actionLogger = (actionName, actionId, column, key, value, txId, commit, timeoutMs, requestType) -> {
				if (column != null) {
					var c = columns.get(column);
					if (c != null) {
						boolean acquired = false;
						try {
							c.beginUse();
							acquired = true;
							column = new String(c.cfh().getName(), StandardCharsets.UTF_8);
						} catch (IllegalStateException closing) {
							// The column is being retired; keep the numeric id in the log entry.
						} catch (org.rocksdb.RocksDBException e) {
							logger.debug("Failed to resolve column name for logging", e);
						} finally {
							if (acquired) {
								c.endUse();
							}
						}
					}
				}
				if (key instanceof Keys keys && keys.keys().length == 1) {
					var key0 = keys.keys()[0];
					var size = key0.size();
					key = switch (size) {
						case Long.BYTES -> key0 + "(" + key0.getLong(0) + ")";
						case Integer.BYTES -> key0 + "(" + key0.getInt(0) + ")";
						default -> key;
					};
				}
				logger.info(m,
						"DB: {} Action: {} Action ID: {} Column: {} Key: {} Value (or end key): {} TxId: {} Commit: {} Timeout (ms): {} Request type: {}",
						name,
						actionName,
						actionId,
						column,
						key,
						value,
						txId,
						commit,
						timeoutMs,
						requestType
				);
			};
		} else {
			this.actionLogger = (_, _, _, _, _, _, _, _, _) -> {};
		}

		var beforeLoad = Instant.now();
		var loadedDb = RocksDBLoader.load(path, config, logger, metrics.getRegistry(), name);
		try {
			recoverRawScanPinsAtStartup(loadedDb.db().get(), loadedDb.definitiveDbPath(), config);
		} catch (IOException | RuntimeException | Error recoveryFailure) {
			try {
				loadedDb.db().close();
			} catch (Throwable closeFailure) {
				recoveryFailure.addSuppressed(closeFailure);
			}
			try {
				loadedDb.refs().close();
			} catch (Throwable closeFailure) {
				recoveryFailure.addSuppressed(closeFailure);
			}
			throw recoveryFailure;
		}
		this.db = loadedDb.db();
		this.dbOptions = loadedDb.dbOptions();
		this.refs = loadedDb.refs();
		this.caches = loadedDb.caches();
		this.cacheCapacities = loadedDb.cacheCapacities();
		this.definitiveDbPath = loadedDb.definitiveDbPath();
		// Compute upper-bound memory config from database options
		RocksDBStatistics.MemoryUpperBoundConfig memoryUpperBoundConfig;
		try {
			var globalConfig = config.global();
			boolean spinning = globalConfig.spinning();
			boolean useDirectIo = path != null && globalConfig.useDirectIo();
			long compactionReadaheadBytes = 0;
			long writableFileMaxBufferBytes = 0;
			if (useDirectIo) {
				compactionReadaheadBytes = 4 * SizeUnit.MB;
				writableFileMaxBufferBytes = 2 * SizeUnit.MB;
			}
			if (spinning) {
				compactionReadaheadBytes = 16 * SizeUnit.MB;
				writableFileMaxBufferBytes = 8 * SizeUnit.MB;
			}
			int maxBackgroundJobs;
			var configuredMaxBgJobs = globalConfig.maxBackgroundJobs();
			if (configuredMaxBgJobs != null && configuredMaxBgJobs >= 0) {
				maxBackgroundJobs = configuredMaxBgJobs;
			} else {
				var bgJobs = Integer.parseInt(System.getProperty("it.cavallium.dbengine.jobs.background.num", "-1"));
				maxBackgroundJobs = bgJobs >= 0 ? bgJobs : Runtime.getRuntime().availableProcessors();
			}
			memoryUpperBoundConfig = new RocksDBStatistics.MemoryUpperBoundConfig(
					maxBackgroundJobs, compactionReadaheadBytes, writableFileMaxBufferBytes);
		} catch (GestaltException e) {
			memoryUpperBoundConfig = new RocksDBStatistics.MemoryUpperBoundConfig(
					Runtime.getRuntime().availableProcessors(), 0, 0);
		}
		var configuredWalDirectory = dbOptions.walDir();
		var walDirectory = configuredWalDirectory == null || configuredWalDirectory.isBlank()
				? definitiveDbPath
				: Path.of(configuredWalDirectory);
		var walMetricsConfig = new RocksDBStatistics.WalMetricsConfig(
				db.get(), walDirectory, dbOptions.maxTotalWalSize());
		this.rocksDBStatistics = new RocksDBStatistics(name, dbOptions.statistics(), metrics,
				caches, cacheCapacities,
				this::getLongProperty, this::getPerCfLongProperty, memoryUpperBoundConfig, walMetricsConfig);
		this.scheduler = new RWScheduler(
				workloadSettings,
				"db[" + name + "]",
				metrics.getRegistry(),
				name);
		Gauge.builder("rockserver.workload.retained.snapshots", retainedRangeSnapshots, AtomicInteger::get)
				.tag("database", name)
				.register(metrics.getRegistry());
		this.cdcPublishedTailSequence.set(db.get().getLatestSequenceNumber());
		Gauge.builder("rockserver.workload.cdc.lag", this, EmbeddedDB::currentCdcLag)
				.tag("database", name)
				.register(metrics.getRegistry());
		this.fastGetReader = fastGet ? new NativeRocksDBGet(db.get(), (long) readCap + writeCap) : null;
		var leakScheduler = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("db-leak-scheduler"));
		leakScheduler.setRemoveOnCancelPolicy(true);
		this.leakScheduler = leakScheduler;
		this.retainedQueryLimiter = new RetainedQueryLimiter(
				workloadSettings.retainedAnalyticalSnapshots(), leakScheduler);

		leakScheduler.scheduleWithFixedDelay(this::cleanupExpiredTransactionsNow, 1, 1, TimeUnit.MINUTES);

		leakScheduler.scheduleWithFixedDelay(this::cleanupExpiredIteratorsNow, 1, 1, TimeUnit.MINUTES);
		leakScheduler.scheduleWithFixedDelay(this::refreshStoragePressure, 1, 1, TimeUnit.SECONDS);
		long retainedSnapshotSweepMillis = Math.max(1L,
				Math.min(100L, maxRetainedSnapshotAgeMs / 4L));
		this.expiredRangeCleanupTask = leakScheduler.scheduleWithFixedDelay(
				this::cleanupExpiredRangesNow,
				retainedSnapshotSweepMillis,
				retainedSnapshotSweepMillis,
				TimeUnit.MILLISECONDS);

		this.columnsConifg = loadedDb.definitiveColumnFamilyOptionsMap();
		try {
			this.tempSSTsPath = config.global().tempSstPath();
		} catch (GestaltException e) {
			throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, "Can't get wal path");
		}
		var existingColumnSchemasColumnDescriptorOptional = db
				.getStartupColumns()
				.entrySet()
				.stream()
				.filter(e -> Arrays.equals(e.getKey().getName(), COLUMN_SCHEMAS_COLUMN))
				.findAny();
		if (existingColumnSchemasColumnDescriptorOptional.isEmpty()) {
			var columnSchemasColumnOptions = RocksDBLoader.getCompatibilityColumnOptions(refs);
			var columnSchemasColumnDescriptor = new ColumnFamilyDescriptor(COLUMN_SCHEMAS_COLUMN,
					columnSchemasColumnOptions);
			try {
				columnSchemasColumnDescriptorHandle = db.get().createColumnFamily(columnSchemasColumnDescriptor);
				columnsConifg.put(new String(COLUMN_SCHEMAS_COLUMN, StandardCharsets.UTF_8),
						columnSchemasColumnOptions);
			} catch (org.rocksdb.RocksDBException e) {
				throw new IOException("Cannot create system column", e);
			}
		} else {
			this.columnSchemasColumnDescriptorHandle = existingColumnSchemasColumnDescriptorOptional.get().getValue();
		}

		var existingMergeOperatorsColumnDescriptorOptional = db
				.getStartupColumns()
				.entrySet()
				.stream()
				.filter(e -> Arrays.equals(e.getKey().getName(), MERGE_OPERATORS_COLUMN))
				.findAny();
		if (existingMergeOperatorsColumnDescriptorOptional.isEmpty()) {
			var mergeOperatorsColumnOptions = RocksDBLoader.getCompatibilityColumnOptions(refs);
			var mergeOperatorsColumnDescriptor = new ColumnFamilyDescriptor(MERGE_OPERATORS_COLUMN,
					mergeOperatorsColumnOptions);
			try {
				mergeOperatorsColumnDescriptorHandle = db.get().createColumnFamily(mergeOperatorsColumnDescriptor);
				columnsConifg.put(new String(MERGE_OPERATORS_COLUMN, StandardCharsets.UTF_8),
						mergeOperatorsColumnOptions);
			} catch (org.rocksdb.RocksDBException e) {
				throw new IOException("Cannot create system column", e);
			}
		} else {
			this.mergeOperatorsColumnDescriptorHandle = existingMergeOperatorsColumnDescriptorOptional.get().getValue();
		}
		this.mergeOperatorRegistry = new MergeOperatorRegistry(db.get(), mergeOperatorsColumnDescriptorHandle);

		// Ensure CDC meta column-family exists
		var existingCdcMetaColumnDescriptorOptional = db
				.getStartupColumns()
				.entrySet()
				.stream()
				.filter(e -> Arrays.equals(e.getKey().getName(), CDC_META_COLUMN))
				.findAny();
		if (existingCdcMetaColumnDescriptorOptional.isEmpty()) {
			var cdcMetaColumnOptions = RocksDBLoader.getCompatibilityColumnOptions(refs);
			var cdcMetaDescriptor = new ColumnFamilyDescriptor(CDC_META_COLUMN, cdcMetaColumnOptions);
			try {
				cdcMetaColumnDescriptorHandle = db.get().createColumnFamily(cdcMetaDescriptor);
				columnsConifg.put(new String(CDC_META_COLUMN, StandardCharsets.UTF_8), cdcMetaColumnOptions);
			} catch (org.rocksdb.RocksDBException e) {
				throw new IOException("Cannot create CDC meta column", e);
			}
		} else {
			this.cdcMetaColumnDescriptorHandle = existingCdcMetaColumnDescriptorOptional.get().getValue();
		}
		loadCdcSubscriptionProgress();

		// Metrics for merge-operator cache sizes (diagnostics to detect leaks in uploaded operators)
		try {
			var registry = metrics.getRegistry();
			meters.add(Gauge
					.builder("rockserver.merge.operator.names", mergeOperatorRegistry, r -> (double) r.getOperatorsCount())
					.tag("db", name)
					.register(registry));
			meters.add(Gauge
					.builder("rockserver.merge.operator.versions", mergeOperatorRegistry, r -> (double) r.getTotalVersionsCount())
					.tag("db", name)
					.register(registry));
		} catch (Throwable ex) {
			logger.error("Failed to load metrics", ex);
		}

		loadExistingColumns(loadedDb.mergeOperators());
		if (Boolean.parseBoolean(System.getProperty("rockserver.core.print-config", "true"))) {
			logger.info("Database configuration: {}", ConfigPrinter.stringify(config));
		}
		printStartupInfo();
		var afterLoad = Instant.now();

		loadTimer.record(Duration.between(beforeLoad, afterLoad));
	}

	private Timer createActionTimer(Class<? extends RocksDBAPICommand> className) {
		return createTimer(Tags.of("action", className.getSimpleName()));
	}

	private Timer createTimer(Tags tags) {
		var t = Timer
				.builder("rocksdb.operation.timer")
				.publishPercentiles(0.3, 0.5, 0.95)
				.publishPercentileHistogram()
				.tag("database", this.name)
				.tags(tags)
				.register(metrics.getRegistry());
		meters.add(t);
		return t;
	}

	private ColumnSchema decodeColumnSchema(byte[] value) {
		try (var is = new ByteArrayInputStream(value); var dis = new DataInputStream(is)) {
			var check = dis.readByte();
			if (check == 2) {
				var size = dis.readInt();
				var keys = new IntArrayList(size);
				for (int i = 0; i < size; i++) {
					keys.add(dis.readInt());
				}
				size = dis.readInt();
				var colHashTypes = new ObjectArrayList<ColumnHashType>(size);
				for (int i = 0; i < size; i++) {
					colHashTypes.add(ColumnHashType.values()[dis.readUnsignedByte()]);
				}
				var hasValue = dis.readBoolean();
				return new ColumnSchema(keys, colHashTypes, hasValue, null, null, null);
			} else if (check == 3) {
				var size = dis.readInt();
				var keys = new IntArrayList(size);
				for (int i = 0; i < size; i++) {
					keys.add(dis.readInt());
				}
				size = dis.readInt();
				var colHashTypes = new ObjectArrayList<ColumnHashType>(size);
				for (int i = 0; i < size; i++) {
					colHashTypes.add(ColumnHashType.values()[dis.readUnsignedByte()]);
				}
				var hasValue = dis.readBoolean();
				String mergeOperatorName = dis.readBoolean() ? dis.readUTF() : null;
				Long mergeOperatorVersion = dis.readBoolean() ? dis.readLong() : null;
				String mergeOperatorClass = dis.readBoolean() ? dis.readUTF() : null;
				return new ColumnSchema(keys,
						colHashTypes,
						hasValue,
						mergeOperatorName,
						mergeOperatorVersion,
						mergeOperatorClass
				);
			} else {
				throw new IllegalStateException("Unknown schema version: " + check);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private byte[] encodeColumnSchema(ColumnSchema schema) {
		try (var baos = new ByteArrayOutputStream(); var daos = new DataOutputStream(baos)) {
			daos.writeByte(3);
			daos.writeInt(schema.keys().size());
			for (int key : schema.keys()) {
				daos.writeInt(key);
			}
			daos.writeInt(schema.variableTailKeys().size());
			for (ColumnHashType variableTailKey : schema.variableTailKeys()) {
				daos.writeByte(variableTailKey.ordinal());
			}
			daos.writeBoolean(schema.hasValue());
			var mergeOperatorName = schema.mergeOperatorName();
			var mergeOperatorVersion = schema.mergeOperatorVersion();
			var mergeOperatorClass = schema.mergeOperatorClass();
			daos.writeBoolean(mergeOperatorName != null);
			if (mergeOperatorName != null) {
				daos.writeUTF(mergeOperatorName);
			}
			daos.writeBoolean(mergeOperatorVersion != null);
			if (mergeOperatorVersion != null) {
				daos.writeLong(mergeOperatorVersion);
			}
			daos.writeBoolean(mergeOperatorClass != null);
			if (mergeOperatorClass != null) {
				daos.writeUTF(mergeOperatorClass);
			}
			baos.close();
			return baos.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Nullable
	private FFMAbstractMergeOperator resolveMergeOperator(ColumnSchema schema,
			@Nullable FFMAbstractMergeOperator configuredMergeOperator) throws RocksDBException {
		if (configuredMergeOperator != null) {
			return configuredMergeOperator;
		}
		String mergeOperatorName = schema.mergeOperatorName();
		Long mergeOperatorVersion = schema.mergeOperatorVersion();
		String mergeOperatorClass = schema.mergeOperatorClass();
		if ((mergeOperatorName != null || mergeOperatorVersion != null) && mergeOperatorClass != null) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Merge operator name/version and merge operator class cannot be both specified"
			);
		}
		if (mergeOperatorName != null || mergeOperatorVersion != null) {
			if (mergeOperatorName == null || mergeOperatorVersion == null) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Merge operator name and version must both be specified when one of them is set"
				);
			}
			return mergeOperatorRegistry.get(mergeOperatorName, mergeOperatorVersion);
		}
		if (mergeOperatorClass != null && !mergeOperatorClass.isBlank()) {
			try {
				Class<?> clazz = Class.forName(mergeOperatorClass);
				if (!FFMAbstractMergeOperator.class.isAssignableFrom(clazz)) {
					throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR,
							"Merge operator class does not extend FFMAbstractMergeOperator: " + mergeOperatorClass
					);
				}
				@SuppressWarnings("unchecked") Class<? extends FFMAbstractMergeOperator> typed = (Class<? extends FFMAbstractMergeOperator>) clazz;
				return typed.getConstructor().newInstance();
			} catch (ReflectiveOperationException e) {
				throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR,
						"Failed to instantiate merge operator: " + mergeOperatorClass,
						e
				);
			}
		}
		return configuredMergeOperator;
	}


	private long internalRegisterColumn(@NotNull String name,
			@NotNull ColumnFamilyHandle cfh,
			@NotNull ColumnSchema schema,
			@Nullable FFMAbstractMergeOperator mergeOp) {
		long id = cfh.getID();

		if (this.columns.containsKey(id)) {
			throw new IllegalStateException("Column ID already registered: " + id);
		}

		Long previous = this.columnNamesIndex.putIfAbsent(name, id);
		if (previous != null) {
			throw new UnsupportedOperationException("Column already registered: " + name);
		}

		var column = new ColumnInstance(cfh, schema, mergeOp);
		this.columns.put(id, column);

		logger.info("Registered column: " + column);
		return id;
	}

	private void loadExistingColumns(Map<String, FFMAbstractMergeOperator> initialMergeOperators) {
		Map<String, ColumnFamilyHandle> startupHandles = new HashMap<>();
		for (var entry : db.getStartupColumns().entrySet()) {
			try {
				startupHandles.put(new String(entry.getKey().getName(), StandardCharsets.UTF_8), entry.getValue());
			} catch (Exception e) {
				logger.warn("Failed to decode column name", e);
			}
		}

		try (var it = this.db.get().newIterator(columnSchemasColumnDescriptorHandle)) {
			it.seekToFirst();
			while (it.isValid()) {
				var key = it.key();
				var name = new String(key, StandardCharsets.UTF_8);
				ColumnSchema value = decodeColumnSchema(it.value());

				var cfh = startupHandles.get(name);
				if (cfh != null) {
					FFMAbstractMergeOperator mergeOp = resolveMergeOperator(value, initialMergeOperators.get(name));
					internalRegisterColumn(name, cfh, value, mergeOp);
				}
				it.next();
			}
		}

		colIt:
		for (var entry : startupHandles.entrySet()) {
			String name = entry.getKey();
			ColumnFamilyHandle cfh = entry.getValue();
			long id = cfh.getID();

			if (this.columns.containsKey(id)) {
				continue;
			}

			// Skip system columns
			byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
			for (byte[] systemColumn : SYSTEM_COLUMNS) {
				if (Arrays.equals(nameBytes, systemColumn)) {
					continue colIt;
				}
			}

			// Column exists in RocksDB but has no stored schema metadata.
			// Mark as unconfigured — operations are forbidden until createColumn sets the schema.
			logger.info("Found column without stored schema, marking as unconfigured: {}", name);
			unconfiguredColumns.put(name, cfh);
		}
	}

	/**
	 * The column must be unregistered once!!! Do not try to unregister a column that may already be unregistered, or that
	 * may not be registered
	 */
	private ColumnInstance unregisterColumn(long id, @NotNull String name) {
		synchronized (columnEditLock) {
			var col = this.columns.remove(id);
			Objects.requireNonNull(col, () -> "Column does not exist: " + id);

			if (this.columnNamesIndex.remove(name) == null) {
				logger.warn("Column name not found in index during unregister: {}", name);
			}

			ColumnFamilyOptions columnConfig = this.columnsConifg.remove(name);
			if (columnConfig != null) {
				columnConfig.close();
			} else {
				logger.warn("Column config not found during unregister: {}", name);
			}

			return col;
		}
	}

	@VisibleForTesting
	public void closeTesting() throws IOException {
		closeInternal(true);
	}

	@Override
	public void close() throws IOException {
		closeInternal(false);
	}

	private void closeInternal(boolean testing) throws IOException {
		long pendingOpsTimeoutMs = shutdownPendingOpsTimeoutMs();
		try {
			// Statistics reads use the same operation gate as API calls. Stop their
			// producer before closing admission, then make shutdown-aware logical
			// reads release their leases before waiting for genuinely active work.
			logger.info("Closing... stopping background statistics");
			try {
				rocksDBStatistics.close();
			} catch (Throwable error) {
				logger.error("Failed to stop RocksDB statistics during shutdown", error);
			}
			ops.closeAdmission();
			retainedQueryLimiter.close();
			cancelActiveExistsMultiRequests();
			closeActiveCdcPollCursors();
			closeActiveRangeResources();
			logger.info("Waiting for active operations");
			ops.waitForExit(pendingOpsTimeoutMs);
			// Cooperative tasks can release their SafeShutdown operation before their
			// worker publishes terminal scheduler metrics. Join the scheduler before
			// closeResources tears down meters or other observability state.
			if (scheduler != null) {
				scheduler.dispose();
			}
			// Normal shutdown path
			logger.info("Ops finished, closing resources");
			closeResources(false);
			if (testing && (resourceLeases.get() > 0
					|| (scheduler != null && !scheduler.isFullyTerminated())
					|| retainedQueryLimiter.activeCount() > 0
					|| retainedQueryLimiter.waitingCount() > 0
					|| retainedRangeSnapshots.get() > 0
					|| !activeExistsMultiRequests.isEmpty()
					|| !activeCdcPollCursors.isEmpty())) {
				throw new IllegalStateException("Shutdown left resources open: schedulerTerminated="
						+ (scheduler == null || scheduler.isFullyTerminated())
						+ ", leases=" + resourceLeases.get()
						+ ", retainedPermits=" + retainedQueryLimiter.activeCount()
						+ ", retainedWaiters=" + retainedQueryLimiter.waitingCount()
						+ ", retainedSnapshots=" + retainedRangeSnapshots.get()
						+ ", existsMultiRequests=" + activeExistsMultiRequests.size()
						+ ", cdcCursors=" + activeCdcPollCursors.size());
			}
		} catch (TimeoutException e) {
			var forcedObserver = forcedShutdownObserver;
			if (forcedObserver != null) {
				forcedObserver.run();
			}
			logger.error(
					"Some active operations lasted more than {} ms, forcing database shutdown... activeOps={}, resourceLeases={}, openTxs={}, openIterators={}",
					pendingOpsTimeoutMs,
					ops.getPendingOpsCount(),
					resourceLeases.get(),
					txs.size(),
					its.size()
			);
			logger.warn("Timeout! Forcing shutdown");
			// Best-effort forced cleanup of leaked resources to avoid native memory retention
			forceCloseLeakedResources();
			if (scheduler != null) {
				scheduler.disposeNow();
			}
			// After forcing close of leaked resources, proceed to close DB/native resources defensively
			closeResources(true);

			if (testing && (ops.getPendingOpsCount() > 0
					|| (scheduler != null && !scheduler.isFullyTerminated())
					|| resourceLeases.get() > 0
					|| retainedQueryLimiter.activeCount() > 0
					|| retainedQueryLimiter.waitingCount() > 0
					|| retainedRangeSnapshots.get() > 0
					|| !activeExistsMultiRequests.isEmpty()
					|| !activeCdcPollCursors.isEmpty())) {
				throw new IllegalStateException("Some active operations lasted more than " + pendingOpsTimeoutMs
						+ " ms! schedulerTerminated=" + (scheduler == null || scheduler.isFullyTerminated())
						+ ", activeOps=" + ops.getPendingOpsCount() + ", resourceLeases="
						+ resourceLeases.get() + ", retainedPermits=" + retainedQueryLimiter.activeCount()
						+ ", retainedWaiters=" + retainedQueryLimiter.waitingCount()
						+ ", retainedSnapshots=" + retainedRangeSnapshots.get()
						+ ", existsMultiRequests=" + activeExistsMultiRequests.size()
						+ ", cdcCursors=" + activeCdcPollCursors.size()
						+ ", openTxs=" + txs.size() + ", openIterators=" + its.size());
			}
		} finally {
			// Ensure scheduler and leak-scheduler are always torn down
			logger.info("Shutting down schedulers");
			try {
				if (scheduler != null) {
					scheduler.dispose();
				}
			} catch (Throwable t) {
				logger.warn("Failed to dispose RWScheduler", t);
			}
			try {
				shutdownExecutor(leakScheduler);
			} catch (Throwable t) {
				logger.warn("Failed to shutdown leak scheduler", t);
			}
			logger.info("Closed.");
		}
	}

	private static long shutdownPendingOpsTimeoutMs() {
		var value = System.getProperty(SHUTDOWN_PENDING_OPS_TIMEOUT_MS_PROPERTY);
		if (value == null || value.isBlank()) {
			return MAX_TRANSACTION_DURATION_MS;
		}
		try {
			return Math.max(0L, Long.parseLong(value));
		} catch (NumberFormatException ex) {
			return MAX_TRANSACTION_DURATION_MS;
		}
	}

	/**
	 * Close all resources in a safe order. Each step is individually protected so that failure to close one resource does
	 * not prevent others from closing. This method is idempotent with respect to Rocks native handles: close() calls are
	 * wrapped and exceptions are logged.
	 */
	private void closeResources(boolean forced) {
		if (scheduler != null && !scheduler.isFullyTerminated()) {
			logger.error("Leaving database resources allocated because workload pools are still live; "
					+ "terminal callbacks may still need native state and metrics");
			return;
		}
		// Logical range reads, CDC polls, and existsMulti requests keep native read
		// state between bounded scheduler slices. SafeShutdown has already excluded new
		// requests, so close those idle resources before column handles or the database.
		cancelActiveExistsMultiRequests();
		closeActiveCdcPollCursors();
		closeActiveRangeResources();
		if (forced && ops.getPendingOpsCount() > 0) {
			logger.error("Leaving RocksDB native resources allocated because {} operations are still active; "
					+ "freeing DB, column, or FFM memory here could crash the process",
					ops.getPendingOpsCount());
			return;
		}

		// Active operations have drained, so idle server-side resources can now be
		// released without racing a native transaction or iterator call.
		if (!txs.isEmpty()) {
			logger.info("Closing {} open transactions", txs.size());
			for (long transactionId : new ArrayList<>(txs.keySet())) {
				try {
					closeTransactionInternal(transactionId, false);
				} catch (Throwable t) {
					logger.error("Error closing remaining transaction", t);
				}
			}
		}
		if (!its.isEmpty()) {
			logger.info("Closing {} open iterators", its.size());
			for (long iteratorId : new ArrayList<>(its.keySet())) {
				try {
					closeIteratorInternal(iteratorId);
				} catch (Throwable t) {
					logger.error("Error closing remaining iterator", t);
				}
			}
		}
		if (resourceLeases.get() > 0) {
			logger.error("Leaving RocksDB native resources allocated because {} transaction or iterator leases "
					+ "could not be resolved safely", resourceLeases.get());
			return;
		}
		rawScanFileDeletionRecovery.stop();

		// Observability objects may still be used by an operation's terminal accounting,
		// so close them only after the same pending-operation gate as native DB resources.
		logger.info("Closing meters/stats");
		try {
			for (Meter meter : meters) {
				try {
					meter.close();
				} catch (Throwable mt) {
					logger.error("Error closing meter{}", forced ? " (forced)" : "", mt);
				}
			}
		} catch (Throwable t) {
			logger.error("Error while closing meters collection{}", forced ? " (forced)" : "", t);
		}
		try {
			rocksDBStatistics.close();
		} catch (Throwable t) {
			logger.error("Error closing rocksDBStatistics{}", forced ? " (forced)" : "", t);
		}
		try {
			if (metrics != null) {
				metrics.close();
			}
		} catch (Throwable t) {
			logger.error("Error closing metrics manager{}", forced ? " (forced)" : "", t);
		}

		// No operations remain, so pinned values and native key segments can be released before their RocksDB handles.
		if (fastGetReader != null) {
			try {
				fastGetReader.close();
			} catch (Throwable t) {
				logger.error("Error closing native fast-get{}", forced ? " (forced)" : "", t);
			}
		}

		// User column handles
		logger.info("Closing user columns");
		try {
			// Create a copy of values to avoid concurrent modification issues if close() modifies the map
			// though here we are in single-threaded shutdown.
			var cols = new ArrayList<>(columns.values());
			for (ColumnInstance col : cols) {
				try {
					col.close();
				} catch (Throwable t) {
					logger.error("Error closing user column handle{}", forced ? " (forced)" : "", t);
				}
			}
			columns.clear();
		} catch (Throwable t) {
			logger.error("Error while iterating user columns to close{}", forced ? " (forced)" : "", t);
		}

		// System column handles
		logger.info("Closing system columns");
		try {
			columnSchemasColumnDescriptorHandle.close();
		} catch (Throwable t) {
			logger.error("Error closing columnSchemas handle{}", forced ? " (forced)" : "", t);
		}
		try {
			mergeOperatorsColumnDescriptorHandle.close();
		} catch (Throwable t) {
			logger.error("Error closing mergeOperators handle{}", forced ? " (forced)" : "", t);
		}
		try {
			cdcMetaColumnDescriptorHandle.close();
		} catch (Throwable t) {
			logger.error("Error closing cdcMeta handle{}", forced ? " (forced)" : "", t);
		}

		// DB and native refs
		logger.info("Closing DB");
		try {
			db.close();
		} catch (Throwable t) {
			logger.error("Error closing DB{}", forced ? " (forced)" : "", t);
		}
		logger.info("Closing refs");
		try {
			refs.close();
		} catch (Throwable t) {
			logger.error("Error closing refs{}", forced ? " (forced)" : "", t);
		}

		// Drop strong references to ColumnFamilyOptions to help GC and avoid holding onto closed natives
		logger.info("Closing ColumnFamilyOptions");
		try {
			for (ColumnFamilyOptions opt : columnsConifg.values()) {
				try {
					opt.close();
				} catch (Throwable t) {
					logger.error("Error closing ColumnFamilyOptions{}", forced ? " (forced)" : "", t);
				}
			}
			columnsConifg.clear();
		} catch (Throwable t) {
			logger.warn("Error clearing column config", t);
		}

		// Close merge-operator registry AFTER DB/ColumnFamilyOptions so that ownership
		// of merge operators is released by ColumnFamilyOptions first (avoids double-close)
		logger.info("Closing MergeOperatorRegistry");
		try {
			mergeOperatorRegistry.close();
		} catch (Throwable t) {
			logger.error("Error closing mergeOperatorRegistry{}", forced ? " (forced)" : "", t);
		}

		// For in-memory DBs, delete the temporary directory
		try {
			if (path == null) {
				Utils.deleteDirectory(db.getPath());
			}
		} catch (Throwable t) {
			logger.error("Error deleting in-memory DB directory{}", forced ? " (forced)" : "", t);
		}
	}

	private void closeActiveRangeResources() {
		var ranges = new ArrayList<>(activeRangeResources);
		if (!ranges.isEmpty()) {
			logger.info("Closing {} active range resources", ranges.size());
		}
		for (var range : ranges) {
			try {
				range.close();
			} catch (Throwable error) {
				logger.error("Error closing active range resource", error);
			}
		}
	}

	private void closeActiveCdcPollCursors() {
		var cursors = new ArrayList<>(activeCdcPollCursors);
		if (!cursors.isEmpty()) {
			logger.info("Closing {} active CDC poll cursors", cursors.size());
		}
		for (var cursor : cursors) {
			try {
				cursor.close();
			} catch (Throwable error) {
				logger.error("Error closing active CDC poll cursor", error);
			}
		}
	}

	private void cancelActiveExistsMultiRequests() {
		final List<AsyncExistsMultiRequest> requests;
		// Admission starts a SafeShutdown operation while holding this same lock. Once
		// closeAndWait has marked ops closed, this snapshot therefore contains every
		// request that could have been admitted successfully.
		synchronized (asyncExistsMultiAdmissionLock) {
			requests = new ArrayList<>(activeExistsMultiRequests);
		}
		if (!requests.isEmpty()) {
			logger.info("Cancelling {} active existsMulti requests", requests.size());
		}
		for (var request : requests) {
			try {
				request.cancel(true);
			} catch (Throwable error) {
				logger.error("Error cancelling active existsMulti request", error);
			}
		}
	}

	/**
	 * Force-close any remaining transactions/iterators and balance pending ops. Invoked during shutdown if SafeShutdown
	 * times out.
	 */
	private void forceCloseLeakedResources() {
		int closedTx = 0;
		int closedIts = 0;
		try {
			cancelActiveExistsMultiRequests();
			closeActiveCdcPollCursors();
			closeActiveRangeResources();

			try {
				ops.waitForExit(2_000);
			} catch (TimeoutException te) {
				logger.warn("Pending operations still not zero after forced shutdown: {}", ops.getPendingOpsCount());
			}
			if (ops.getPendingOpsCount() > 0) {
				return;
			}

			// Transactions
			for (long transactionId : new ArrayList<>(txs.keySet())) {
				try {
					closeTransactionInternal(transactionId, false);
					closedTx++;
				} catch (Throwable t) {
					logger.warn("Failed to close transaction during forced shutdown", t);
				}
			}

			// Iterators
			for (long iteratorId : new ArrayList<>(its.keySet())) {
				try {
					if (closeIteratorInternal(iteratorId)) {
						closedIts++;
					}
				} catch (Throwable t) {
					logger.warn("Failed to close iterator during forced shutdown", t);
				}
			}
		} catch (Throwable t) {
			logger.warn("forceCloseLeakedResources encountered an error", t);
		} finally {
			logger.info("Forced closed resources. Transactions: {}, Iterators: {}", closedTx, closedIts);
		}
	}

	private void shutdownExecutor(ScheduledExecutorService exec) {
		if (exec == null) {
			return;
		}
		exec.shutdownNow();
		try {
			if (!exec.awaitTermination(10, TimeUnit.SECONDS)) {
				logger.warn("Leak scheduler did not terminate within timeout");
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Run the transactions leak-cleaner logic immediately.
	 */
	@VisibleForTesting
	void cleanupExpiredTransactionsNow() {
		// Skip if shutdown in progress to avoid IllegalStateException from SafeShutdown.beginOp()
		if (!ops.isOpen()) {
			return;
		}
		logger.debug("Cleaning expired transactions...");
		var idsToRemove = new LongArrayList();
		var sampleOverdues = new ArrayList<String>(8);
		var startTime = System.currentTimeMillis();
		try {
			ops.beginOp();
		} catch (IllegalStateException closed) {
			return; // shutting down
		}
		try {
			EmbeddedDB.this.txs.forEach(((txId, tx) -> {
				if (startTime >= tx.expirationTimestamp()) {
					idsToRemove.add((long) txId);
					long overdue = startTime - tx.expirationTimestamp();
					if (sampleOverdues.size() < 16) {
						// Capture a small sample: id, overdueMs, isForUpdate
						sampleOverdues.add("id=" + txId + ", overdueMs=" + overdue + ", forUpdate=" + tx.isFromGetForUpdate());
					}
				}
			}));
			idsToRemove.forEach(id -> {
				try {
					closeTransactionInternal(id, false);
				} catch (Throwable ex) {
					logger.error("Failed to close an expired transaction", ex);
				}
			});
		} finally {
			ops.endOp();
		}
		var endTime = System.currentTimeMillis();
		var removedCount = idsToRemove.size();
		if (removedCount > 10) {
			logger.info("Cleaned {} expired transactions in {}. Sample: {}",
					removedCount,
					Duration.ofMillis(endTime - startTime),
					String.join(" | ", sampleOverdues)
			);
		} else if (removedCount > 2) {
			logger.info("Cleaned {} expired transactions in {}", removedCount, Duration.ofMillis(endTime - startTime));
		} else {
			logger.debug("Cleaned {} expired transactions in {}", removedCount, Duration.ofMillis(endTime - startTime));
		}
	}

	/**
	 * Run the iterators leak-cleaner logic immediately.
	 */
	@VisibleForTesting
	void cleanupExpiredIteratorsNow() {
		// Skip if shutdown in progress to avoid IllegalStateException from SafeShutdown.beginOp()
		if (!ops.isOpen()) {
			return;
		}
		logger.debug("Cleaning expired iterators...");
		var idsToRemove = new LongArrayList();
		var sampleOverdues = new ArrayList<String>(8);
		var startTime = System.currentTimeMillis();
		try {
			ops.beginOp();
		} catch (IllegalStateException closed) {
			return; // shutting down
		}
		try {
			EmbeddedDB.this.its.forEach(((itId, entry) -> {
				if (entry.expirationTimestamp() != null && startTime >= entry.expirationTimestamp()) {
					idsToRemove.add((long) itId);
					long overdue = startTime - entry.expirationTimestamp();
					if (sampleOverdues.size() < 16) {
						sampleOverdues.add("id=" + itId + ", overdueMs=" + overdue);
					}
				}
			}));
			idsToRemove.forEach(id -> {
				try {
					closeIteratorInternal(id);
				} catch (Throwable ex) {
					logger.error("Failed to close an iteration", ex);
				}
			});
		} finally {
			ops.endOp();
		}
		var endTime = System.currentTimeMillis();
		var removedCount = idsToRemove.size();
		if (removedCount > 10) {
			logger.info("Cleaned {} expired iterators in {}. Sample: {}",
					removedCount,
					Duration.ofMillis(endTime - startTime),
					String.join(" | ", sampleOverdues)
			);
		} else if (removedCount > 2) {
			logger.info("Cleaned {} expired iterators in {}", removedCount, Duration.ofMillis(endTime - startTime));
		} else {
			logger.debug("Cleaned {} expired iterators in {}", removedCount, Duration.ofMillis(endTime - startTime));
		}
	}

	/** Release native iterator views held by stalled range subscribers after their logical deadline. */
	@VisibleForTesting
	public void cleanupExpiredRangesNow() {
		if (!ops.isOpen()) {
			return;
		}
		try {
			ops.beginOp();
		} catch (IllegalStateException closed) {
			return;
		}
		try {
			long nowMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
			for (var range : activeRangeResources) {
				try {
					range.expireIfDeadlinePassed(nowMicros);
				} catch (Throwable error) {
					// ScheduledExecutorService suppresses every later execution when one
					// invocation escapes. Keep cleaning the other cursors and future runs.
					logger.warn("Failed to clean an expired range cursor", error);
				}
			}
		} finally {
			ops.endOp();
		}
	}

	private ReadOptions newReadOptions(String label) {
		var ro = new LeakSafeReadOptions(label);
		ro.setAsyncIo(true);
		return ro;
	}

	private ReadOptions newWriteElisionReadOptions() {
		var readOptions = newReadOptions("write-elision-cache-probe");
		readOptions.setReadTier(ReadTier.BLOCK_CACHE_TIER);
		readOptions.setFillCache(false);
		return readOptions;
	}

	private void recordWriteElisionDecision(WriteElisionRequest request, WriteElisionDecision decision) {
		writeElisionDecisionCounters.get(request).get(decision).increment();
	}

	private WriteElisionDecision probeCachedLogicalValue(ColumnInstance col,
			PhysicalKey physicalKey,
			Keys logicalKeys,
			Buf requestedValue) {
		try (var readOptions = newWriteElisionReadOptions()) {
			byte[] rawValue;
			try {
				rawValue = db.get().get(col.cfh(), readOptions, physicalKey.bytes);
			} catch (org.rocksdb.RocksDBException exception) {
				return switch (exception.getStatus().getCode()) {
					case NotFound -> WriteElisionDecision.FALLBACK_NOT_FOUND;
					case Incomplete -> WriteElisionDecision.FALLBACK_INCOMPLETE;
					default -> throw mapWriteElisionProbeFailure(exception);
				};
			}
			if (rawValue == null) {
				return WriteElisionDecision.FALLBACK_NOT_FOUND;
			}
			return cachedLogicalValueEquals(col, logicalKeys, requestedValue, Buf.wrap(rawValue))
					? WriteElisionDecision.ELIDED
					: WriteElisionDecision.FALLBACK_DIFFERENT;
		}
	}

	private static boolean cachedLogicalValueEquals(ColumnInstance col,
			Keys logicalKeys,
			Buf requestedValue,
			Buf rawValue) {
		if (col.hasBuckets()) {
			var bucket = new Bucket(col, rawValue);
			var existing = bucket.getElement(col.getBucketElementKeys(logicalKeys.keys()));
			return existing != null && (!col.schema().hasValue() || Utils.valueEquals(existing, requestedValue));
		}
		return !col.schema().hasValue() || Utils.valueEquals(rawValue, requestedValue);
	}

	private static RocksDBException mapWriteElisionProbeFailure(org.rocksdb.RocksDBException exception) {
		return RocksDBException.of(RocksDBErrorType.PUT_2, exception);
	}

	private static RocksDBException mapWriteElisionProbeFailure(org.rocksdb.Status status) {
		return mapWriteElisionProbeFailure(new org.rocksdb.RocksDBException(status));
	}

	private MultiWriteElisionProbe probeCachedLogicalValues(ColumnInstance col,
			List<Keys> logicalKeys,
			List<Buf> requestedValues) {
		var physicalKeys = new PhysicalKey[logicalKeys.size()];
		var decisions = new WriteElisionDecision[logicalKeys.size()];
		for (int i = 0; i < logicalKeys.size(); i++) {
			col.checkNullableValue(requestedValues.get(i));
			physicalKeys[i] = new PhysicalKey(col.calculateKey(logicalKeys.get(i).keys()).toByteArray());
		}
		if (col.hasBuckets()) {
			probeCachedBuckets(col, logicalKeys, requestedValues, physicalKeys, decisions);
		} else {
			probeCachedDirectValues(col, requestedValues, physicalKeys, decisions);
		}
		return new MultiWriteElisionProbe(physicalKeys, decisions);
	}

	private void probeCachedDirectValues(ColumnInstance col,
			List<Buf> requestedValues,
			PhysicalKey[] physicalKeys,
			WriteElisionDecision[] decisions) {
		int offset = 0;
		try (var readOptions = newWriteElisionReadOptions()) {
			while (offset < physicalKeys.length) {
				var indexes = new ArrayList<Integer>();
				long probeBytes = 0L;
				while (offset < physicalKeys.length && indexes.size() < WRITE_ELISION_MAX_KEYS_PER_NATIVE_CALL) {
					var physicalKey = physicalKeys[offset];
					var requestedValue = requestedValues.get(offset);
					long entryBytes = saturatingAdd(physicalKey.bytes.length,
							col.schema().hasValue() ? requestedValue.size() : 0L);
					if (entryBytes > WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL) {
						decisions[offset++] = WriteElisionDecision.BYPASS_OVERSIZED;
						continue;
					}
					if (!indexes.isEmpty()
							&& entryBytes > WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL - probeBytes) {
						break;
					}
					indexes.add(offset);
					probeBytes += entryBytes;
					offset++;
				}
				if (indexes.isEmpty()) {
					continue;
				}
				try (var arena = Arena.ofConfined()) {
					var nativeKeys = new MemorySegment[indexes.size()];
					var nativeValues = new MemorySegment[indexes.size()];
					for (int i = 0; i < indexes.size(); i++) {
						int logicalIndex = indexes.get(i);
						nativeKeys[i] = copyToNativeSegment(arena, physicalKeys[logicalIndex].bytes);
						nativeValues[i] = arena.allocate(col.schema().hasValue()
								? requestedValues.get(logicalIndex).size()
								: 0);
					}
					var statuses = runWriteElisionMultiGet(col, readOptions, nativeKeys, nativeValues);
					for (int i = 0; i < statuses.size(); i++) {
						int logicalIndex = indexes.get(i);
						var status = statuses.get(i);
						decisions[logicalIndex] = switch (status.status.getCode()) {
							case Ok -> !col.schema().hasValue()
									|| status.requiredSize == requestedValues.get(logicalIndex).size()
									&& memorySegmentEquals(nativeValues[i],
									requestedValues.get(logicalIndex), status.requiredSize)
									? WriteElisionDecision.ELIDED
									: WriteElisionDecision.FALLBACK_DIFFERENT;
							case NotFound -> WriteElisionDecision.FALLBACK_NOT_FOUND;
							case Incomplete -> WriteElisionDecision.FALLBACK_INCOMPLETE;
							default -> throw mapWriteElisionProbeFailure(status.status);
						};
					}
				}
			}
		} catch (org.rocksdb.RocksDBException exception) {
			throw mapWriteElisionProbeFailure(exception);
		}
	}

	private void probeCachedBuckets(ColumnInstance col,
			List<Keys> logicalKeys,
			List<Buf> requestedValues,
			PhysicalKey[] physicalKeys,
			WriteElisionDecision[] decisions) {
		var groupsByKey = new LinkedHashMap<PhysicalKey, BucketWriteElisionProbe>();
		for (int i = 0; i < physicalKeys.length; i++) {
			long individualProbeBytes = saturatingAdd(physicalKeys[i].bytes.length, requestedValues.get(i).size());
			if (individualProbeBytes > WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL) {
				decisions[i] = WriteElisionDecision.BYPASS_OVERSIZED;
				continue;
			}
			groupsByKey.computeIfAbsent(physicalKeys[i], BucketWriteElisionProbe::new).logicalIndexes.add(i);
		}
		var groups = new ArrayList<>(groupsByKey.values());
		var valuePassGroups = new ArrayList<BucketWriteElisionProbe>();
		try (var readOptions = newWriteElisionReadOptions()) {
			int offset = 0;
			while (offset < groups.size()) {
				var chunk = new ArrayList<BucketWriteElisionProbe>();
				long probeBytes = 0L;
				while (offset < groups.size() && chunk.size() < WRITE_ELISION_MAX_KEYS_PER_NATIVE_CALL) {
					var group = groups.get(offset);
					long entryBytes = group.physicalKey.bytes.length;
					if (entryBytes > WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL) {
						setBucketDecision(group, decisions, WriteElisionDecision.BYPASS_OVERSIZED);
						offset++;
						continue;
					}
					if (!chunk.isEmpty()
							&& entryBytes > WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL - probeBytes) {
						break;
					}
					chunk.add(group);
					probeBytes += entryBytes;
					offset++;
				}
				if (chunk.isEmpty()) {
					continue;
				}
				try (var arena = Arena.ofConfined()) {
					var nativeKeys = new MemorySegment[chunk.size()];
					var nativeValues = new MemorySegment[chunk.size()];
					for (int i = 0; i < chunk.size(); i++) {
						nativeKeys[i] = copyToNativeSegment(arena, chunk.get(i).physicalKey.bytes);
						nativeValues[i] = arena.allocate(0);
					}
					var statuses = runWriteElisionMultiGet(col, readOptions, nativeKeys, nativeValues);
					for (int i = 0; i < statuses.size(); i++) {
						var group = chunk.get(i);
						var status = statuses.get(i);
						switch (status.status.getCode()) {
							case Ok -> {
								group.requiredSize = status.requiredSize;
								long valuePassBytes = saturatingAdd(group.physicalKey.bytes.length,
										status.requiredSize);
								if (valuePassBytes > WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL) {
									setBucketDecision(group, decisions, WriteElisionDecision.BYPASS_OVERSIZED);
								} else {
									valuePassGroups.add(group);
								}
							}
							case NotFound -> setBucketDecision(group, decisions, WriteElisionDecision.FALLBACK_NOT_FOUND);
							case Incomplete -> setBucketDecision(group, decisions, WriteElisionDecision.FALLBACK_INCOMPLETE);
							default -> throw mapWriteElisionProbeFailure(status.status);
						}
					}
				}
			}

			offset = 0;
			while (offset < valuePassGroups.size()) {
				var chunk = new ArrayList<BucketWriteElisionProbe>();
				long probeBytes = 0L;
				while (offset < valuePassGroups.size() && chunk.size() < WRITE_ELISION_MAX_KEYS_PER_NATIVE_CALL) {
					var group = valuePassGroups.get(offset);
					long entryBytes = saturatingAdd(group.physicalKey.bytes.length, group.requiredSize);
					if (!chunk.isEmpty()
							&& entryBytes > WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL - probeBytes) {
						break;
					}
					chunk.add(group);
					probeBytes += entryBytes;
					offset++;
				}
				try (var arena = Arena.ofConfined()) {
					var nativeKeys = new MemorySegment[chunk.size()];
					var nativeValues = new MemorySegment[chunk.size()];
					for (int i = 0; i < chunk.size(); i++) {
						var group = chunk.get(i);
						nativeKeys[i] = copyToNativeSegment(arena, group.physicalKey.bytes);
						nativeValues[i] = arena.allocate(group.requiredSize);
					}
					var statuses = runWriteElisionMultiGet(col, readOptions, nativeKeys, nativeValues);
					for (int i = 0; i < statuses.size(); i++) {
						var group = chunk.get(i);
						var status = statuses.get(i);
						switch (status.status.getCode()) {
							case Ok -> {
								if (status.requiredSize > nativeValues[i].byteSize()) {
									var decision = saturatingAdd(group.physicalKey.bytes.length, status.requiredSize)
											> WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL
											? WriteElisionDecision.BYPASS_OVERSIZED
											: WriteElisionDecision.FALLBACK_DIFFERENT;
									setBucketDecision(group, decisions, decision);
									continue;
								}
								var rawBucket = Buf.wrap(copyMemorySegment(nativeValues[i], status.requiredSize));
								var bucket = new Bucket(col, rawBucket);
								for (int logicalIndex : group.logicalIndexes) {
									var existing = bucket.getElement(
											col.getBucketElementKeys(logicalKeys.get(logicalIndex).keys()));
									decisions[logicalIndex] = existing != null
											&& (!col.schema().hasValue()
											|| Utils.valueEquals(existing, requestedValues.get(logicalIndex)))
											? WriteElisionDecision.ELIDED
											: WriteElisionDecision.FALLBACK_DIFFERENT;
								}
							}
							case NotFound -> setBucketDecision(group, decisions, WriteElisionDecision.FALLBACK_NOT_FOUND);
							case Incomplete -> setBucketDecision(group, decisions, WriteElisionDecision.FALLBACK_INCOMPLETE);
							default -> throw mapWriteElisionProbeFailure(status.status);
						}
					}
				}
			}
		} catch (org.rocksdb.RocksDBException exception) {
			throw mapWriteElisionProbeFailure(exception);
		}
	}

	private List<org.rocksdb.ByteBufferGetStatus> runWriteElisionMultiGet(ColumnInstance col,
			ReadOptions readOptions,
			MemorySegment[] nativeKeys,
			MemorySegment[] nativeValues) throws org.rocksdb.RocksDBException {
		var observer = writeElisionMultiGetObserver;
		if (observer != null) {
			long probeBytes = 0L;
			for (int i = 0; i < nativeKeys.length; i++) {
				probeBytes = saturatingAdd(probeBytes, nativeKeys[i].byteSize());
				probeBytes = saturatingAdd(probeBytes, nativeValues[i].byteSize());
			}
			observer.accept(nativeKeys.length, probeBytes);
		}
		return db.get().multiGetByteBuffers(readOptions, List.of(col.cfh()), nativeKeys, nativeValues);
	}

	private static void setBucketDecision(BucketWriteElisionProbe group,
			WriteElisionDecision[] decisions,
			WriteElisionDecision decision) {
		for (int logicalIndex : group.logicalIndexes) {
			decisions[logicalIndex] = decision;
		}
	}

	private static MemorySegment copyToNativeSegment(Arena arena, byte[] value) {
		return arena.allocateFrom(ValueLayout.JAVA_BYTE, value);
	}

	private static MemorySegment copyToNativeSegment(Arena arena, Buf value) {
		var segment = arena.allocate(value.size());
		MemorySegment.copy(value.getBackingByteArray(), value.getBackingByteArrayOffset(), segment,
				ValueLayout.JAVA_BYTE, 0, value.getBackingByteArrayLength());
		return segment;
	}

	private static boolean memorySegmentEquals(MemorySegment actual, Buf expected, int length) {
		if (length != expected.size() || actual.byteSize() < length) {
			return false;
		}
		for (int i = 0; i < length; i++) {
			if (actual.get(ValueLayout.JAVA_BYTE, i) != expected.getByte(i)) {
				return false;
			}
		}
		return true;
	}

	private static byte[] copyMemorySegment(MemorySegment source, int length) {
		return source.asSlice(0, length).toArray(ValueLayout.JAVA_BYTE);
	}

	private ReadOptions newRangeReadOptions(long deadlineMicros,
			boolean fillCache,
			@Nullable AbstractSlice<?> startKeySlice,
			@Nullable AbstractSlice<?> endKeySlice) {
		var ro = newReadOptions("get-range-async-read-options");
		try {
			ro.setDeadline(deadlineMicros);
			ro.setFillCache(fillCache);
			if (startKeySlice != null) {
				ro.setIterateLowerBound(startKeySlice);
			}
			if (endKeySlice != null) {
				ro.setIterateUpperBound(endKeySlice);
			}
			var observer = rangeReadOptionsObserver;
			if (observer != null) {
				observer.accept(ro.fillCache());
			}
			return ro;
		} catch (Throwable throwable) {
			ro.close();
			throw throwable;
		}
	}

	@VisibleForTesting
	public void setRangeReadOptionsObserverForTesting(@Nullable Consumer<Boolean> observer) {
		this.rangeReadOptionsObserver = observer;
	}

	@VisibleForTesting
	public void setReduceRangeAsyncIoObserverForTesting(@Nullable Consumer<Boolean> observer) {
		this.reduceRangeAsyncIoObserver = observer;
	}

	@VisibleForTesting
	public void setRangeIteratorOpenObserverForTesting(@Nullable Runnable observer) {
		this.rangeIteratorOpenObserver = observer;
	}

	@VisibleForTesting
	public void setRangeReadChunkSizeObserverForTesting(@Nullable Consumer<Integer> observer) {
		this.rangeReadChunkSizeObserver = observer;
	}

	@VisibleForTesting
	public void setRangeCountChunkObserverForTesting(@Nullable Runnable observer) {
		this.rangeCountChunkObserver = observer;
	}

	@VisibleForTesting
	public void setRangeCountQuantumItemsObserverForTesting(@Nullable LongConsumer observer) {
		this.rangeCountQuantumItemsObserver = observer;
	}

	@VisibleForTesting
	public void setRangeContinuationObserverForTesting(@Nullable Runnable observer) {
		this.rangeContinuationObserver = observer;
	}

	@VisibleForTesting
	public void setRetainedQueryPermitGrantedObserverForTesting(@Nullable LongConsumer observer) {
		this.retainedQueryPermitGrantedObserver = observer;
	}

	@VisibleForTesting
	public void setRetainedRangeCleanupObserverForTesting(@Nullable Runnable observer) {
		this.retainedRangeCleanupObserver = observer;
	}

	@VisibleForTesting
	public void setIteratorAdvanceStepCompletedObserverForTesting(@Nullable Runnable observer) {
		this.iteratorAdvanceStepCompletedObserver = observer;
	}

	@VisibleForTesting
	public void setForcedShutdownObserverForTesting(@Nullable Runnable observer) {
		this.forcedShutdownObserver = observer;
	}

	@VisibleForTesting
	public void setExistsMultiChunkObserverForTesting(@Nullable Runnable observer) {
		this.existsMultiChunkObserver = observer;
	}

	@VisibleForTesting
	public void setExistsMultiSnapshotObserverForTesting(@Nullable Runnable observer) {
		this.existsMultiSnapshotObserver = observer;
	}

	@VisibleForTesting
	public void setExistsMultiArenaObserverForTesting(@Nullable Consumer<Boolean> observer) {
		this.existsMultiArenaObserver = observer;
	}

	@VisibleForTesting
	public void setWriteElisionMultiGetObserverForTesting(@Nullable BiConsumer<Integer, Long> observer) {
		this.writeElisionMultiGetObserver = observer;
	}

	@VisibleForTesting
	public void setCdcPollTailCapturedObserverForTesting(@Nullable Runnable observer) {
		this.cdcPollTailCapturedObserver = observer;
	}

	@VisibleForTesting
	public void setCdcWalIteratorOpenObserverForTesting(@Nullable Runnable observer) {
		this.cdcWalIteratorOpenObserver = observer;
	}

	@VisibleForTesting
	public void setCdcQuantumObserversForTesting(@Nullable LongConsumer mutationsObserver,
			@Nullable LongConsumer bytesObserver) {
		this.cdcQuantumMutationsObserver = mutationsObserver;
		this.cdcQuantumBytesObserver = bytesObserver;
	}

	@VisibleForTesting
	public void setCdcLatestValueResolutionObserverForTesting(@Nullable Runnable observer) {
		this.cdcLatestValueResolutionObserver = observer;
	}

	@VisibleForTesting
	public void setCdcContinuationObserverForTesting(@Nullable Runnable observer) {
		this.cdcContinuationObserver = observer;
	}

	@VisibleForTesting
	public void setCdcMetadataOperationObserverForTesting(@Nullable BiConsumer<String, String> observer) {
		this.cdcMetadataOperationObserver = observer;
	}

	@VisibleForTesting
	public void setCdcMetadataLoadedObserverForTesting(@Nullable BiConsumer<String, String> observer) {
		this.cdcMetadataLoadedObserver = observer;
	}

	@VisibleForTesting
	public void setRawScanFilesCapturedObserverForTesting(@Nullable Runnable observer) {
		this.rawScanFilesCapturedObserver = observer;
	}

	@VisibleForTesting
	public void setRawScanReaderOpenedObserverForTesting(@Nullable Runnable observer) {
		this.rawScanReaderOpenedObserver = observer;
	}

	@VisibleForTesting
	public void setRawScanCleanupObserverForTesting(@Nullable Runnable observer) {
		this.rawScanCleanupObserver = observer;
	}

	@VisibleForTesting
	public Set<Path> getRawScanPinnedFilesForTesting() {
		return Set.copyOf(activeRawScanPinnedFiles);
	}

	@VisibleForTesting
	public void setColumnMaintenanceObserverForTesting(@Nullable Runnable observer) {
		this.columnMaintenanceObserver = observer;
	}

	@VisibleForTesting
	public void setColumnUseAcquiredObserverForTesting(@Nullable LongConsumer observer) {
		this.columnUseAcquiredObserver = observer;
	}

	private void notifyRangeIteratorOpened() {
		var observer = rangeIteratorOpenObserver;
		if (observer != null) {
			observer.run();
		}
	}

	@Override
	public long openTransaction(long timeoutMs) {
		return openTransaction(timeoutMs, WorkloadProfile.BATCH);
	}

	public long openTransaction(long timeoutMs, WorkloadProfile workloadProfile) {
		var start = System.nanoTime();
		actionLogger.logAction("OpenTransaction", start, null, null, null, null, null, timeoutMs, null);
		ops.beginOp();
		try {
			return allocateTransactionInternal(openTransactionInternal(timeoutMs,
					false,
					workloadProfile,
					true));
		} finally {
			ops.endOp();
			var end = System.nanoTime();
			openTransactionTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private long allocateTransactionInternal(Tx tx) {
		try {
			return FastRandomUtils.allocateNewValue(txs, tx, Long.MIN_VALUE, -2);
		} catch (Throwable error) {
			try {
				closeTransactionInternal(tx, false);
			} catch (Throwable closeError) {
				error.addSuppressed(closeError);
			}
			throw error;
		}
	}

	private Tx openTransactionInternal(long timeoutMs, boolean isFromGetForUpdate) {
		return openTransactionInternal(timeoutMs, isFromGetForUpdate, WorkloadProfile.BATCH, false);
	}

	private Tx openTransactionInternal(long timeoutMs,
			boolean isFromGetForUpdate,
			WorkloadProfile workloadProfile,
			boolean captureSnapshot) {
		Objects.requireNonNull(workloadProfile, "workloadProfile");
		var expirationTimestamp = timeoutMs + System.currentTimeMillis();
		TransactionalOptions txOpts = db.createTransactionalOptions(timeoutMs);
		var writeOpts = new LeakSafeWriteOptions("open-transaction-internal-write-options");
		var rocksObjects = new RocksDBObjects(writeOpts, txOpts);
		org.rocksdb.Transaction transaction = null;
		try {
			transaction = db.beginTransaction(writeOpts, txOpts);
			if (captureSnapshot) {
				// A transaction-backed page is a fresh iterator on every request. Capture the
				// snapshot before the externally usable ID becomes observable, while leaving
				// internal throwaway write transactions on their existing read semantics.
				transaction.setSnapshot();
			}
			var tx = new Tx(transaction,
					isFromGetForUpdate,
					expirationTimestamp,
					rocksObjects,
					workloadProfile);
			transaction = null;
			try {
				retainResourceLease();
				return tx;
			} catch (Throwable error) {
				tx.close();
				throw error;
			}
		} catch (Throwable ex) {
			if (transaction != null) {
				transaction.close();
			}
			rocksObjects.close();
			throw ex;
		}
	}

	@Override
	@Contract("_, false -> true; _, true -> _")
	public boolean closeTransaction(long transactionId, boolean commit) {
		var start = System.nanoTime();
		actionLogger.logAction("CloseTransaction", start, null, null, null, transactionId, commit, null, null);
		try {
			return closeTransactionInternal(transactionId, commit);
		} finally {
			var end = System.nanoTime();
			closeTransactionTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	@Contract("_, false, _ -> true; _, true, _ -> _")
	public boolean closeTransaction(long transactionId,
			boolean commit,
			WorkloadProfile workloadProfile) {
		if (commit) {
			validateTransactionProfile(transactionId, workloadProfile);
		}
		return closeTransaction(transactionId, commit);
	}

	/**
	 * @return false if failed optimistic commit
	 */
	@Contract("_, false -> true; _, true -> _")
	private boolean closeTransactionInternal(long transactionId, boolean commit) {
		var tx = txs.get(transactionId);
		if (tx == null) {
			return handleMissingTransaction(transactionId, commit);
		}
		synchronized (tx) {
			// A queued closer may have read the transaction before the previous closer
			// removed it. Recheck ownership while holding the per-transaction monitor.
			if (txs.get(transactionId) != tx) {
				return handleMissingTransaction(transactionId, commit);
			}
			try {
				var succeeded = closeTransactionExclusively(tx, commit);
				if (!succeeded) {
					// Busy/TryAgain keeps the native transaction open, mapped and counted.
					return false;
				}
				txs.remove(transactionId, tx);
				return true;
			} catch (Throwable ex) {
				txs.remove(transactionId, tx);
				throw ex;
			}
		}
	}

	private static boolean handleMissingTransaction(long transactionId, boolean commit) {
		if (commit) {
			throw RocksDBException.of(RocksDBErrorType.TX_NOT_FOUND,
					"Transaction not found: " + transactionId);
		}
		return true;
	}

	/**
	 * @return false if failed optimistic commit
	 */
	@Contract("_, false -> true; _, true -> _")
	private boolean closeTransactionInternal(@NotNull Tx tx, boolean commit) {
		synchronized (tx) {
			return closeTransactionExclusively(tx, commit);
		}
	}

	@Contract("_, false -> true; _, true -> _")
	private boolean closeTransactionExclusively(@NotNull Tx tx, boolean commit) {
		// Owned transactions are deliberately closed again from several finally blocks.
		// Once the native handle is gone, its resource lease has already been balanced.
		if (!tx.val().isOwningHandle()) {
			return true;
		}
		// Transaction lifetimes are tracked separately from active operations. This lets
		// shutdown drain native calls first and then roll back idle client transactions.
		try {
			if (commit) {
				boolean succeeded;
				try {
					tx.val().commit();
					recordCdcPublishedTail();
					succeeded = true;
				} catch (org.rocksdb.RocksDBException ex) {
					var status = ex.getStatus() != null ? ex.getStatus().getCode() : Code.Ok;
					if (status == Code.Busy || status == Code.TryAgain) {
						succeeded = false;
					} else {
						throw ex;
					}
				}
 			if (!succeeded) {
					// Keep the resource lease while the transaction remains open.
					return false;
				}
			} else {
				if (tx.val().isOwningHandle()) {
					tx.val().rollback();
				}
			}
			tx.close();
			releaseResourceLease();
			return true;
		} catch (org.rocksdb.RocksDBException e) {
			try {
				tx.close();
			} catch (Throwable t) {
				e.addSuppressed(t);
			}
			releaseResourceLease();
			throw RocksDBException.of(RocksDBErrorType.COMMIT_FAILED, "Transaction close failed");
		} catch (Throwable ex) {
			try {
				tx.close();
			} catch (Throwable t) {
				ex.addSuppressed(t);
			}
			releaseResourceLease();
			throw ex;
		}
	}

	@Override
	public void closeFailedUpdate(long updateId) throws RocksDBException {
		var start = System.nanoTime();
		actionLogger.logAction("CloseFailedUpdate", start, null, null, null, updateId, null, null, null);
		try {
			closeTransactionInternal(updateId, false);
		} finally {
			var end = System.nanoTime();
			closeFailedUpdateTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private boolean isCompatibleSchemaChange(ColumnSchema oldSchema, ColumnSchema newSchema) {
		return Objects.equals(oldSchema.keys(), newSchema.keys()) &&
				Objects.equals(oldSchema.variableTailKeys(), newSchema.variableTailKeys()) &&
				oldSchema.hasValue() == newSchema.hasValue();
	}

	private void updateColumnSchema(long colId, String name, ColumnSchema newSchema, FFMAbstractMergeOperator newOp) {
		// Persist to disk
		try {
			byte[] key = name.getBytes(StandardCharsets.UTF_8);
			byte[] value = encodeColumnSchema(newSchema);
			db.get().put(columnSchemasColumnDescriptorHandle, key, value);
			recordCdcPublishedTail();
		} catch (org.rocksdb.RocksDBException e) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "Failed to persist updated schema for column: " + name, e);
		}

		// Update in-memory
		synchronized (columnEditLock) {
			ColumnInstance oldCol = columns.get(colId);
			if (oldCol == null) return;

			FFMAbstractMergeOperator currentOp = oldCol.mergeOperator();
			if (currentOp instanceof DelegatingMergeOperator delegateOp && newOp != null) {
				delegateOp.setDelegate(newOp);
				newOp = delegateOp;
			} else if (newOp != null && currentOp != null) {
				logger.warn("Existing merge operator for column {} is not a DelegatingMergeOperator. Hot swap will only take effect after restart.", name);
			}

			ColumnInstance newCol = oldCol.withSchema(newSchema, newOp);
			columns.put(colId, newCol);
		}
		logger.info("Updated schema for column: {} (MergeOperator changed)", name);
	}

	@Override
	public long createColumn(String name, @NotNull ColumnSchema schema) throws RocksDBException {
		var start = System.nanoTime();
		actionLogger.logAction("CreateColumn", start, name, null, schema, null, null, null, null);
		ops.beginOp();
		try {
			synchronized (columnEditLock) {
				var colId = getColumnIdOrNull(name);
				var col = colId != null ? getColumn(colId) : null;
				if (col != null) {
					if (schema.equals(col.schema())) {
						return colId;
					} else if (isCompatibleSchemaChange(col.schema(), schema)) {
						GlobalDatabaseConfig globalConfig;
						try {
							globalConfig = this.config.global();
						} catch (GestaltException e) {
							throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, "Failed to get global config", e);
						}
						var options = RocksDBLoader.getColumnOptions(name,
								path,
								definitiveDbPath,
								globalConfig,
								logger,
								refs,
								this.path == null,
								caches
						);
						// Force use of configured merge operator if available, or resolve from new schema
						var mergeOp = resolveMergeOperator(schema, options.mergeOperator());
						updateColumnSchema(colId, name, schema, mergeOp);
						return colId;
						} else {
						throw RocksDBException.of(RocksDBErrorType.COLUMN_EXISTS,
								"Column exists, with a different schema: " + name
						);
					}
				} else {
					// Check if this column exists in RocksDB but was loaded without a stored schema
					var unconfiguredCfh = unconfiguredColumns.remove(name);
					if (unconfiguredCfh != null) {
						// Column exists in RocksDB but had no schema. Configure it now.
						logger.info("Configuring schema for previously unconfigured column: {}", name);
						try {
							var options = RocksDBLoader.getColumnOptions(name,
									path,
									definitiveDbPath,
									this.config.global(),
									logger,
									this.refs,
									path == null,
									caches
							);
							var mergeOp = resolveMergeOperator(schema, options.mergeOperator());
							if (mergeOp != null && !(mergeOp instanceof DelegatingMergeOperator)) {
								mergeOp = new DelegatingMergeOperator("Delegating-" + name, mergeOp);
							}
							var prev = columnsConifg.put(name, options.options());
							if (prev != null) {
								prev.close();
							}
							byte[] key = name.getBytes(StandardCharsets.UTF_8);
							byte[] value = encodeColumnSchema(schema);
							db.get().put(columnSchemasColumnDescriptorHandle, key, value);
							recordCdcPublishedTail();
							return internalRegisterColumn(name, unconfiguredCfh, schema, mergeOp);
						} catch (org.rocksdb.RocksDBException | GestaltException e) {
							// Put it back if we failed
							unconfiguredColumns.put(name, unconfiguredCfh);
							throw RocksDBException.of(RocksDBErrorType.COLUMN_CREATE_FAIL, e);
						}
					}
					try {
						var options = RocksDBLoader.getColumnOptions(name,
								path,
								definitiveDbPath,
								this.config.global(),
								logger,
								this.refs,
								path == null,
								caches
						);
						var mergeOp = resolveMergeOperator(schema, options.mergeOperator());
						if (mergeOp != null && !(mergeOp instanceof DelegatingMergeOperator)) {
							mergeOp = new DelegatingMergeOperator("Delegating-" + name, mergeOp);
						}
						if (mergeOp != null) {
							options.options().setMergeOperator(mergeOp);
						}

						var prev = columnsConifg.put(name, options.options());
						if (prev != null) {
							throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.COLUMN_CREATE_FAIL,
									"ColumnsConfig already exists with name \"" + name + "\""
							);
						}
						byte[] key = name.getBytes(StandardCharsets.UTF_8);
						var cf = db.get().createColumnFamily(new ColumnFamilyDescriptor(key, options.options()));
						byte[] value = encodeColumnSchema(schema);
						db.get().put(columnSchemasColumnDescriptorHandle, key, value);
						recordCdcPublishedTail();
						return internalRegisterColumn(name, cf, schema, mergeOp);
					} catch (org.rocksdb.RocksDBException | GestaltException e) {
						throw RocksDBException.of(RocksDBErrorType.COLUMN_CREATE_FAIL, e);
					}
				}
			}
		} finally {
			ops.endOp();
			var end = System.nanoTime();
			createColumnTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	@Override
	public long uploadMergeOperator(String name, String className, byte[] jarData) {
		try {
			return mergeOperatorRegistry.upload(name, className, jarData);
		} finally {
			// Upload consists of several durable registry writes. Refresh even when a
			// later validation/cache step fails after an earlier write succeeded.
			recordCdcPublishedTail();
		}
	}

	@Override
	public Long checkMergeOperator(String name, byte[] hash) {
		return mergeOperatorRegistry.check(name, hash);
	}

	@Override
	public void deleteColumn(long columnId) throws RocksDBException {
		var start = System.nanoTime();
		actionLogger.logAction("DeleteColumn", start, columnId, null, null, null, null, null, null);
		ops.beginOp();
		try {
			synchronized (columnEditLock) {
				var col = getColumn(columnId);
				try {
					var columnName = new String(col.cfh().getName(), StandardCharsets.UTF_8);
					deleteRegisteredColumnLocked(columnId, columnName, col);
				} catch (org.rocksdb.RocksDBException e) {
					throw RocksDBException.of(RocksDBErrorType.COLUMN_DELETE_FAIL, e);
				}
			}
		} finally {
			ops.endOp();
			var end = System.nanoTime();
			deleteColumnTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	@Override
	public boolean deleteColumnIfExists(@NotNull String name) throws RocksDBException {
		Objects.requireNonNull(name, "name");
		var start = System.nanoTime();
		actionLogger.logAction("DeleteColumnIfExists", start, name, null, null, null, null, null, null);
		ops.beginOp();
		try {
			synchronized (columnEditLock) {
				try {
					var columnId = getRegisteredColumnIdOrNull(name);
					if (columnId != null) {
						deleteRegisteredColumnLocked(columnId, name, getColumn(columnId));
						return true;
					}

					var unconfiguredColumn = unconfiguredColumns.get(name);
					if (unconfiguredColumn != null) {
						db.get().dropColumnFamily(unconfiguredColumn);
						unconfiguredColumns.remove(name);
						unconfiguredColumn.close();
						var columnConfig = columnsConifg.remove(name);
						if (columnConfig != null) {
							columnConfig.close();
						} else {
							logger.warn("Column config not found while deleting unconfigured column: {}", name);
						}
						deleteColumnSchemaMetadata(name);
						return true;
					}

					// A previous attempt may have dropped the physical column before metadata cleanup failed.
					deleteColumnSchemaMetadata(name);
					return false;
				} catch (org.rocksdb.RocksDBException e) {
					throw RocksDBException.of(RocksDBErrorType.COLUMN_DELETE_FAIL, e);
				}
			}
		} finally {
			ops.endOp();
			var end = System.nanoTime();
			deleteColumnIfExistsTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private void deleteRegisteredColumnLocked(long columnId,
			@NotNull String name,
			@NotNull ColumnInstance column) throws org.rocksdb.RocksDBException {
		// Closing admission linearizes deletion against every data-path acquisition.
		// Existing owners retain the native handle until they finish; holding the edit
		// lock prevents a same-name replacement from overlapping that retirement.
		column.stopNewUsesAndWait();
		db.get().dropColumnFamily(column.cfh());
		unregisterColumn(columnId, name).closeHandle();
		deleteColumnSchemaMetadata(name);
	}

	private void deleteColumnSchemaMetadata(@NotNull String name) throws org.rocksdb.RocksDBException {
		db.get().delete(columnSchemasColumnDescriptorHandle, name.getBytes(StandardCharsets.UTF_8));
		recordCdcPublishedTail();
	}

	@Override
	public long getColumnId(@NotNull String name) {
		var start = System.nanoTime();
		actionLogger.logAction("GetColumnId", start, name, null, null, null, null, null, null);
		try {
			var columnId = getColumnIdOrNull(name);
			if (columnId == null) {
				throw RocksDBException.of(RocksDBErrorType.COLUMN_NOT_FOUND, "Column not found: " + name);
			} else {
				return columnId;
			}
		} finally {
			var end = System.nanoTime();
			getColumnIdTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	/**
	 * Return RocksDB's estimate of physical keys in this column family.
	 * Exact, bounded, and transaction-aware counts remain implemented by {@link #reduceRange}.
	 */
	@Override
	public long estimateNumKeys(long columnId) {
		var start = System.nanoTime();
		ops.beginOp();
		actionLogger.logAction("EstimateNumKeys", start, columnId, null, null, null, null, null, null);
		ColumnInstance col = null;
		try {
			col = beginColumnUse(columnId);
			try {
				return db.get().getLongProperty(col.cfh(), RocksDBLongProperty.ESTIMATE_NUM_KEYS.getName());
			} catch (org.rocksdb.RocksDBException e) {
				throw RocksDBException.of(RocksDBErrorType.GET_PROPERTY_ERROR, e);
			}
		} finally {
			if (col != null) {
				col.endUse();
			}
			ops.endOp();
			var end = System.nanoTime();
			estimateNumKeysTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private Long getColumnIdOrNull(@NotNull String name) {
		var columnId = getRegisteredColumnIdOrNull(name);
		if (columnId == null) {
			return null;
		}
		var col = columns.get(columnId);
		return col != null && col.isAcceptingUses() ? columnId : null;
	}

	private Long getRegisteredColumnIdOrNull(@NotNull String name) {
		var columnId = (long) columnNamesIndex.getOrDefault(name, -1L);
		ColumnInstance col;
		if (columnId == -1L || (col = columns.get(columnId)) == null || !col.cfh().isOwningHandle()) {
			return null;
		} else {
			return columnId;
		}
	}

	/**
	 * Return long property based on the aggregation mode:
	 * - PER_CF: sum values across all column families
	 * - DB_WIDE: query once without a CF handle
	 * - SINGLE_CF: query once with any single CF handle (for shared-resource properties like block cache)
	 */
	private BigInteger getLongProperty(String name, RocksDBLongProperty.AggregationMode aggregationMode) {
		ops.beginOp();
		try {
			return switch (aggregationMode) {
				case PER_CF -> {
					var val = BigInteger.ZERO;
					for (Entry<Long, ColumnInstance> entry : columns.entrySet()) {
						ColumnInstance ci = entry.getValue();
						if (!tryBeginColumnUse(ci)) {
							continue;
						}
						try {
							val = val.add(new BigInteger(Long.toUnsignedString(db.get().getLongProperty(ci.cfh(), name))));
						} catch (org.rocksdb.RocksDBException e) {
							if (e.getStatus().getCode() == Code.NotFound) {
								// skip
							} else {
								throw new RuntimeException(e);
							}
						} finally {
							ci.endUse();
						}
					}
					yield val;
				}
				case DB_WIDE -> {
					try {
						yield new BigInteger(Long.toUnsignedString(db.get().getLongProperty(name)));
					} catch (org.rocksdb.RocksDBException e) {
						if (e.getStatus().getCode() == Code.NotFound) {
							yield BigInteger.ZERO;
						} else {
							throw new RuntimeException(e);
						}
					}
				}
				case SINGLE_CF -> {
					// Shared-resource properties (e.g. block cache) return the same value
					// for every CF handle, so query with any single one.
					BigInteger value = BigInteger.ZERO;
					for (var entry : columns.entrySet()) {
						var ci = entry.getValue();
						if (!tryBeginColumnUse(ci)) {
							continue;
						}
						try {
							value = new BigInteger(Long.toUnsignedString(db.get().getLongProperty(ci.cfh(), name)));
							break;
						} catch (org.rocksdb.RocksDBException e) {
							if (e.getStatus().getCode() != Code.NotFound) {
								throw new RuntimeException(e);
							}
						} finally {
							ci.endUse();
						}
					}
					yield value;
				}
			};
		} finally {
			ops.endOp();
		}
	}

	private void refreshStoragePressure() {
		try {
			boolean writeStopped = getLongProperty(
					RocksDBLongProperty.IS_WRITE_STOPPED.getName(),
					RocksDBLongProperty.IS_WRITE_STOPPED.getAggregationMode()).signum() > 0;
			var pendingCompactionBytes = getLongProperty(
					RocksDBLongProperty.ESTIMATE_PENDING_COMPACTION_BYTES.getName(),
					RocksDBLongProperty.ESTIMATE_PENDING_COMPACTION_BYTES.getAggregationMode());
			scheduler.setStoragePressure(writeStopped || pendingCompactionBytes.compareTo(
					BigInteger.valueOf(STORAGE_PRESSURE_PENDING_COMPACTION_BYTES)) >= 0);
		} catch (Throwable error) {
			logger.debug("Unable to refresh workload storage-pressure state", error);
		}
	}

	/**
	 * Return per-column-family long property values as a map from column name to value.
	 * Only meaningful for PER_CF properties.
	 */
	private Map<String, Long> getPerCfLongProperty(String name) {
		ops.beginOp();
		try {
			var result = new LinkedHashMap<String, Long>();
			for (Entry<Long, ColumnInstance> entry : columns.entrySet()) {
				ColumnInstance ci = entry.getValue();
				if (!tryBeginColumnUse(ci)) {
					continue;
				}
				try {
					String colName = new String(ci.cfh().getName());
					long value = db.get().getLongProperty(ci.cfh(), name);
					result.merge(colName, value, Long::sum);
				} catch (org.rocksdb.RocksDBException e) {
					if (e.getStatus().getCode() != Code.NotFound) {
						throw new RuntimeException(e);
					}
				} finally {
					ci.endUse();
				}
			}
			return result;
		} finally {
			ops.endOp();
		}
	}

	@Override
	public <T> T put(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		var start = System.nanoTime();
		actionLogger.logAction("Put", start, columnId, keys, value, transactionOrUpdateId, null, null, requestType);
		ops.beginOp();
		ColumnInstance col = null;
		try {
			// Column id
			col = beginColumnUse(columnId);
			Tx tx;
			if (transactionOrUpdateId != 0) {
				tx = getTransaction(transactionOrUpdateId, true);
			} else {
				tx = null;
			}
			long updateId = tx != null && tx.isFromGetForUpdate() ? transactionOrUpdateId : 0L;
			return put(tx, col, updateId, keys, value, requestType);
		} catch (RocksDBRetryException ex) {
			throw ex;
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			if (col != null) {
				col.endUse();
			}
			ops.endOp();
			var end = System.nanoTime();
			putTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	@Override
	public <T> T delete(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull RequestType.RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		var start = System.nanoTime();
		actionLogger.logAction("Delete", start, columnId, keys, null, transactionOrUpdateId, null, null, requestType);
		ops.beginOp();
		ColumnInstance col = null;
		try {
			// Column id
			col = beginColumnUse(columnId);
			Tx tx;
			if (transactionOrUpdateId != 0) {
				tx = getTransaction(transactionOrUpdateId, true);
			} else {
				tx = null;
			}
			long updateId = tx != null && tx.isFromGetForUpdate() ? transactionOrUpdateId : 0L;
			return delete(tx, col, updateId, keys, requestType);
		} catch (RocksDBRetryException ex) {
			throw ex;
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			if (col != null) {
				col.endUse();
			}
			ops.endOp();
			var end = System.nanoTime();
			putTimer.record(end - start, TimeUnit.NANOSECONDS); // Re-use put timer? or new timer? using putTimer for now
		}
	}

	@Override
	public <T> T merge(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		var start = System.nanoTime();
		actionLogger.logAction("Merge", start, columnId, keys, value, transactionOrUpdateId, null, null, requestType);
		ops.beginOp();
		ColumnInstance col = null;
		try {
			col = beginColumnUse(columnId);
			Tx tx;
			if (transactionOrUpdateId != 0) {
				tx = getTransaction(transactionOrUpdateId, true);
			} else {
				tx = null;
			}
			long updateId = tx != null && tx.isFromGetForUpdate() ? transactionOrUpdateId : 0L;
			return merge(tx, col, updateId, keys, value, requestType);
		} catch (RocksDBRetryException ex) {
			throw ex;
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			if (col != null) {
				col.endUse();
			}
			ops.endOp();
			var end = System.nanoTime();
			putTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	@Override
	public <T> List<T> deleteMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keysList,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		var start = System.nanoTime();
		ops.beginOp();
		ColumnInstance col = null;
		try {
			col = beginColumnUse(columnId);
			actionLogger.logAction("deleteMulti (begin)",
					start,
					columnId,
					keysList.size(),
					0,
					transactionOrUpdateId,
					null,
					null,
					requestType
			);

			Tx tx;
			if (transactionOrUpdateId != 0) {
				tx = getTransaction(transactionOrUpdateId, true);
			} else {
				tx = null;
			}
			long updateId = tx != null && tx.isFromGetForUpdate() ? transactionOrUpdateId : 0L;

			if (updateId != 0) {
				return deleteMultiWithUpdateId(tx, updateId, col, keysList, requestType);
			}

			List<T> responses =
					requestType instanceof RequestType.RequestNothing<?> ? null : new ArrayList<>(keysList.size());
			for (int i = 0; i < keysList.size(); i++) {
				var keys = keysList.get(i);
				actionLogger.logAction("deleteMulti (next)",
						start,
						columnId,
						keys,
						null,
						transactionOrUpdateId,
						null,
						null,
						requestType
				);
				T result = delete(tx, col, 0L, keys, requestType);
				if (responses != null) {
					responses.add(result);
				}
			}
			return responses != null ? responses : List.of();
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			if (col != null) {
				col.endUse();
			}
			ops.endOp();
			var end = System.nanoTime();
			putMultiTimer.record(end - start, TimeUnit.NANOSECONDS); // Re-use putMultiTimer
		}
	}

	private <T> List<T> deleteMultiWithUpdateId(Tx tx,
			long updateId,
			ColumnInstance col,
			List<Keys> keysList,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		try {
			boolean committedOwnedTx;
			List<T> responses;
			do {
				boolean savePointSet = false;
				try {
					tx.val().setSavePoint();
					savePointSet = true;
				} catch (org.rocksdb.RocksDBException e) {
					// Handle case where setSavePoint might not be supported or fails
					logger.debug("Failed to set savepoint", e);
				}
				responses = requestType instanceof RequestType.RequestNothing<?> ? null : new ArrayList<>(keysList.size());
				try {
					for (int i = 0; i < keysList.size(); i++) {
						var keys = keysList.get(i);
						T result = delete(tx, col, 0L, keys, requestType);
						if (responses != null) {
							responses.add(result);
						}
					}

					boolean committed = closeTransaction(updateId, true);
					if (!committed) {
						if (savePointSet) {
							try {
								tx.val().rollbackToSavePoint();
							} catch (org.rocksdb.RocksDBException | AssertionError e) {
								logger.debug("Failed to rollback to savepoint during commit failure in deleteMultiWithUpdateId", e);
							}
						}
						closeTransaction(updateId, false);
						throw new RocksDBRetryException();
					}
					committedOwnedTx = true;
				} catch (RocksDBRetryException e) {
					if (savePointSet) {
						try {
							tx.val().rollbackToSavePoint();
						} catch (org.rocksdb.RocksDBException | AssertionError ex) {
							logger.debug("Failed to rollback to savepoint during retry in deleteMultiWithUpdateId", ex);
						}
					}
					throw e;
				} catch (Throwable t) {
					if (savePointSet) {
						try {
							tx.val().rollbackToSavePoint();
						} catch (org.rocksdb.RocksDBException | AssertionError ex) {
							logger.debug("Failed to rollback to savepoint during error in deleteMultiWithUpdateId", ex);
						}
					}
					throw t;
				}
			} while (!committedOwnedTx);
			return responses != null ? responses : List.of();
		} catch (RocksDBRetryException e) {
			throw e;
		} catch (Throwable t) {
			closeTransaction(updateId, false);
			if (t instanceof RocksDBException r) throw r;
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, t);
		}
	}

	@Override
	public void deleteRange(long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive) throws RocksDBException {
		var start = System.nanoTime();
		actionLogger.logAction("DeleteRange",
				start,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				null,
				null,
				null,
				null
		);
		ops.beginOp();
		ColumnInstance col = null;
		try {
			col = beginColumnUse(columnId);
			var beginKey = calculateDeleteRangeBeginKey(col, startKeysInclusive);
			var endKey = calculateDeleteRangeEndKey(col, endKeysExclusive);
			if (Arrays.compareUnsigned(beginKey, endKey) >= 0) {
				return;
			}
			try {
				deleteRangeNative(col, beginKey, endKey);
			} catch (org.rocksdb.RocksDBException e) {
				if (!isDeleteRangeUnsupported(e)) {
					throw RocksDBException.of(RocksDBErrorType.PUT_1, e);
				}
				if (nativeDeleteRangeFallbackLogged.compareAndSet(false, true)) {
					logger.warn("Native RocksDB range delete is not supported by this database handle; falling back to batched point deletes. Further deleteRange fallback messages are suppressed");
				}
				deleteRangeByIterating(col, beginKey, endKey);
			}
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			if (col != null) {
				col.endUse();
			}
			ops.endOp();
			var end = System.nanoTime();
			deleteRangeTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private static boolean isDeleteRangeUnsupported(org.rocksdb.RocksDBException e) {
		if (e.getStatus() != null && e.getStatus().getCode() == Code.NotSupported) {
			return true;
		}
		String message = e.getMessage();
		return message != null
				&& message.contains("DeleteRange")
				&& (message.contains("not implemented") || message.contains("NotSupported"));
	}

	private void deleteRangeNative(ColumnInstance col, byte[] beginKey, byte[] endKey) throws org.rocksdb.RocksDBException {
		var rocks = db.get();
		if (rocks instanceof OptimisticTransactionDB optimisticTransactionDB) {
			rocks = optimisticTransactionDB.getBaseDB();
		}
		try (var w = new LeakSafeWriteOptions(null); var wb = new LeakSafeWriteBatch()) {
			wb.deleteRange(col.cfh(), beginKey, endKey);
			rocks.write(w, wb);
			recordCdcPublishedTail();
		}
	}

	private void deleteRangeByIterating(ColumnInstance col, byte[] beginKey, byte[] endKey) throws RocksDBException {
		var snapshot = db.get().getSnapshot();
		try (var ro = newReadOptions("delete-range-fallback-read-options");
				var startKeySlice = new Slice(beginKey);
				var endKeySlice = new Slice(endKey)) {
			ro.setSnapshot(snapshot);
			ro.setIterateLowerBound(startKeySlice);
			ro.setIterateUpperBound(endKeySlice);
			try (var it = db.get().newIterator(col.cfh(), ro); var wb = new LeakSafeWriteBatch()) {
				int pendingDeletes = 0;
				it.seekToFirst();
				while (it.isValid()) {
					wb.delete(col.cfh(), it.key());
					pendingDeletes++;
					if (pendingDeletes >= 10_000) {
						writeDeleteRangeFallbackBatch(wb);
						wb.clear();
						pendingDeletes = 0;
					}
					it.next();
				}
				if (pendingDeletes > 0) {
					writeDeleteRangeFallbackBatch(wb);
				}
			} catch (org.rocksdb.RocksDBException e) {
				throw RocksDBException.of(RocksDBErrorType.PUT_1, e);
			}
		} finally {
			db.get().releaseSnapshot(snapshot);
		}
	}

	private void writeDeleteRangeFallbackBatch(LeakSafeWriteBatch wb) throws org.rocksdb.RocksDBException {
		try (var w = new LeakSafeWriteOptions(null)) {
			db.get().write(w, wb);
			recordCdcPublishedTail();
		}
	}

	private byte[] calculateDeleteRangeBeginKey(ColumnInstance col, @Nullable Keys startKeysInclusive) {
		if (startKeysInclusive != null && startKeysInclusive.keys().length > 0) {
			return col.calculateKey(startKeysInclusive.keys()).toByteArray();
		}
		return new byte[0];
	}

	private byte[] calculateDeleteRangeEndKey(ColumnInstance col, @Nullable Keys endKeysExclusive) {
		if (endKeysExclusive != null && endKeysExclusive.keys().length > 0) {
			return col.calculateKey(endKeysExclusive.keys()).toByteArray();
		}
		return encodedKeyUpperBound(col.finalKeySizeBytes());
	}

	private static byte[] encodedKeyUpperBound(int keySizeBytes) {
		var endKey = new byte[keySizeBytes + 1];
		Arrays.fill(endKey, 0, keySizeBytes, (byte) 0xff);
		return endKey;
	}

	@Override
	public <T> List<T> putMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keysList,
			@NotNull List<@NotNull Buf> valueList,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		var start = System.nanoTime();
		ops.beginOp();
		ColumnInstance col = null;
		try {
			col = beginColumnUse(columnId);
			actionLogger.logAction("putMulti (begin)",
					start,
					columnId,
					keysList.size(),
					valueList.size(),
					transactionOrUpdateId,
					null,
					null,
					requestType
			);
			if (keysList.size() != valueList.size()) {
				throw new IllegalArgumentException(
						"keys length is different than values length: " + keysList.size() + " != " + valueList.size());
			}

			Tx tx;
			if (transactionOrUpdateId != 0) {
				tx = getTransaction(transactionOrUpdateId, true);
			} else {
				tx = null;
			}
			long updateId = tx != null && tx.isFromGetForUpdate() ? transactionOrUpdateId : 0L;

			if (updateId != 0) {
				return putMultiWithUpdateId(tx, updateId, col, keysList, valueList, requestType);
			}
			if (tx == null && WriteElisionRequest.from(requestType) != null) {
				return putMultiWithWriteElision(start, columnId, col, keysList, valueList, requestType);
			}

			List<T> responses =
					requestType instanceof RequestType.RequestNothing<?>
							|| requestType instanceof RequestType.RequestEnsure<?>
							? null
							: new ArrayList<>(keysList.size());
			for (int i = 0; i < keysList.size(); i++) {
				var keys = keysList.get(i);
				var value = valueList.get(i);
				actionLogger.logAction("putMulti (next)",
						start,
						columnId,
						keys,
						value,
						transactionOrUpdateId,
						null,
						null,
						requestType
				);
				T result = put(tx, col, 0L, keys, value, requestType);
				if (responses != null) {
					responses.add(result);
				}
			}
			return responses != null ? responses : List.of();
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			if (col != null) {
				col.endUse();
			}
			ops.endOp();
			var end = System.nanoTime();
			putMultiTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private <T> List<T> putMultiWithWriteElision(long start,
			long columnId,
			ColumnInstance col,
			List<Keys> keysList,
			List<Buf> valueList,
			RequestPut<? super Buf, T> requestType) {
		var writeElisionRequest = Objects.requireNonNull(WriteElisionRequest.from(requestType));
		var probe = probeCachedLogicalValues(col, keysList, valueList);
		var dirtyPhysicalKeys = new HashSet<PhysicalKey>();
		List<T> responses = requestType instanceof RequestType.RequestEnsure<?>
				? null
				: new ArrayList<>(keysList.size());
		for (int i = 0; i < keysList.size(); i++) {
			var keys = keysList.get(i);
			var value = valueList.get(i);
			actionLogger.logAction("putMulti (next)",
					start,
					columnId,
					keys,
					value,
					0L,
					null,
					null,
					requestType
			);
			var physicalKey = probe.physicalKeys()[i];
			var decision = probe.decisions()[i];
			if (decision != WriteElisionDecision.BYPASS_OVERSIZED
					&& dirtyPhysicalKeys.contains(physicalKey)) {
				decision = probeCachedLogicalValues(col, List.of(keys), List.of(value)).decisions()[0];
			}
			recordWriteElisionDecision(writeElisionRequest, decision);
			T result;
			if (decision == WriteElisionDecision.ELIDED) {
				result = RequestType.safeCast(requestType instanceof RequestType.RequestPreviousPresence<?>
						? Boolean.TRUE
						: null);
			} else {
				result = put(null, col, 0L, keys, value, requestType, false);
				dirtyPhysicalKeys.add(physicalKey);
			}
			if (responses != null) {
				responses.add(result);
			}
		}
		return responses != null ? responses : List.of();
	}

	private <T> List<T> putMultiWithUpdateId(Tx tx,
			long updateId,
			ColumnInstance col,
			List<Keys> keysList,
			List<Buf> valueList,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		try {
			boolean committedOwnedTx;
			List<T> responses;
			do {
				boolean savePointSet = false;
				try {
					tx.val().setSavePoint();
					savePointSet = true;
				} catch (org.rocksdb.RocksDBException e) {
					logger.debug("Failed to set savepoint", e);
				}
				responses = requestType instanceof RequestType.RequestNothing<?>
						|| requestType instanceof RequestType.RequestEnsure<?>
						? null
						: new ArrayList<>(keysList.size());
				try {
					for (int i = 0; i < keysList.size(); i++) {
						var keys = keysList.get(i);
						var value = valueList.get(i);
						T result = put(tx, col, 0L, keys, value, requestType);
						if (responses != null) {
							responses.add(result);
						}
					}

					boolean committed = closeTransaction(updateId, true);
					if (!committed) {
						if (savePointSet) {
							try {
								tx.val().rollbackToSavePoint();
							} catch (org.rocksdb.RocksDBException | AssertionError e) {
								logger.debug("Failed to rollback to savepoint during commit failure in putMultiWithUpdateId", e);
							}
						}
						closeTransaction(updateId, false);
						// We don't know which keys were locked internally by put() because we passed updateId=0 to it.
						// However, if we are here, it means we are in an updateId transaction.
						// Conflict happened during commit.
						// We must throw retry exception so the caller can retry.
						throw new RocksDBRetryException();
					}
					committedOwnedTx = true;
				} catch (RocksDBRetryException e) {
					if (savePointSet) {
						try {
							tx.val().rollbackToSavePoint();
						} catch (org.rocksdb.RocksDBException | AssertionError ex) {
							logger.debug("Failed to rollback to savepoint during retry in putMultiWithUpdateId", ex);
						}
					}
					throw e;
				} catch (Throwable t) {
					if (savePointSet) {
						try {
							tx.val().rollbackToSavePoint();
						} catch (org.rocksdb.RocksDBException | AssertionError ex) {
							logger.debug("Failed to rollback to savepoint during error in putMultiWithUpdateId", ex);
						}
					}
					throw t;
				}
			} while (!committedOwnedTx);
			return responses != null ? responses : List.of();
		} catch (RocksDBRetryException e) {
			throw e;
		} catch (Throwable t) {
			closeTransaction(updateId, false);
			if (t instanceof RocksDBException r) throw r;
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, t);
		}
	}

	@Override
	public <T> List<T> mergeMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keysList,
			@NotNull List<@NotNull Buf> valueList,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		var start = System.nanoTime();
		ops.beginOp();
		ColumnInstance col = null;
		try {
			col = beginColumnUse(columnId);
			actionLogger.logAction("mergeMulti (begin)",
					start,
					columnId,
					keysList.size(),
					valueList.size(),
					transactionOrUpdateId,
					null,
					null,
					requestType
			);
			if (keysList.size() != valueList.size()) {
				throw new IllegalArgumentException(
						"keys length is different than values length: " + keysList.size() + " != " + valueList.size());
			}

			Tx tx;
			if (transactionOrUpdateId != 0) {
				tx = getTransaction(transactionOrUpdateId, true);
			} else {
				tx = null;
			}
			long updateId = tx != null && tx.isFromGetForUpdate() ? transactionOrUpdateId : 0L;

			if (updateId != 0) {
				return mergeMultiWithUpdateId(tx, updateId, col, keysList, valueList, requestType);
			}

			List<T> responses =
					requestType instanceof RequestType.RequestNothing<?> ? null : new ArrayList<>(keysList.size());
			for (int i = 0; i < keysList.size(); i++) {
				var keys = keysList.get(i);
				var value = valueList.get(i);
				actionLogger.logAction("mergeMulti (next)",
						start,
						columnId,
						keys,
						value,
						transactionOrUpdateId,
						null,
						null,
						requestType
				);
				T result = merge(tx, col, 0L, keys, value, requestType);
				if (responses != null) {
					responses.add(result);
				}
			}
			return responses != null ? responses : List.of();
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			if (col != null) {
				col.endUse();
			}
			ops.endOp();
			var end = System.nanoTime();
			putMultiTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private <T> List<T> mergeMultiWithUpdateId(Tx tx,
			long updateId,
			ColumnInstance col,
			List<Keys> keysList,
			List<Buf> valueList,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		try {
			boolean committedOwnedTx;
			List<T> responses;
			do {
				boolean savePointSet = false;
				try {
					tx.val().setSavePoint();
					savePointSet = true;
				} catch (org.rocksdb.RocksDBException e) {
					logger.debug("Failed to set savepoint", e);
				}
				responses = requestType instanceof RequestType.RequestNothing<?> ? null : new ArrayList<>(keysList.size());
				try {
					for (int i = 0; i < keysList.size(); i++) {
						var keys = keysList.get(i);
						var value = valueList.get(i);
						T result = merge(tx, col, 0L, keys, value, requestType);
						if (responses != null) {
							responses.add(result);
						}
					}

					boolean committed = closeTransaction(updateId, true);
					if (!committed) {
						if (savePointSet) {
							try {
								tx.val().rollbackToSavePoint();
							} catch (org.rocksdb.RocksDBException | AssertionError e) {
								logger.debug("Failed to rollback to savepoint during commit failure in mergeMultiWithUpdateId", e);
							}
						}
						closeTransaction(updateId, false);
						throw new RocksDBRetryException();
					}
					committedOwnedTx = true;
				} catch (RocksDBRetryException e) {
					if (savePointSet) {
						try {
							tx.val().rollbackToSavePoint();
						} catch (org.rocksdb.RocksDBException | AssertionError ex) {
							logger.debug("Failed to rollback to savepoint during retry in mergeMultiWithUpdateId", ex);
						}
					}
					throw e;
				} catch (Throwable t) {
					if (savePointSet) {
						try {
							tx.val().rollbackToSavePoint();
						} catch (org.rocksdb.RocksDBException | AssertionError ex) {
							logger.debug("Failed to rollback to savepoint during error in mergeMultiWithUpdateId", ex);
						}
					}
					throw t;
				}
			} while (!committedOwnedTx);
			return responses != null ? responses : List.of();
		} catch (RocksDBRetryException e) {
			throw e;
		} catch (Throwable t) {
			closeTransaction(updateId, false);
			if (t instanceof RocksDBException r) throw r;
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, t);
		}
	}

	public CompletableFuture<Void> putBatchInternal(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull PutBatchMode mode) throws RocksDBException {
		return putBatchInternal(columnId, batchPublisher, mode, scheduler.write());
	}

	public CompletableFuture<Void> putBatchInternal(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull PutBatchMode mode,
			@NotNull Scheduler workloadScheduler) throws RocksDBException {
		var start = System.nanoTime();
		actionLogger.logAction("PutBatch (begin)",
				start,
				columnId,
				"multiple (async)",
				"multiple (async)",
				null,
				null,
				null,
				mode
		);

		Mono<Void> operation = Mono.using(
				() -> new PutBatchState(columnId, mode, start),
				state -> Flux.from(batchPublisher)
						.publishOn(workloadScheduler)
						.doOnNext(state::write)
						.then(Mono.fromRunnable(state::writePending)),
				BatchWriteState::close,
				true
			);
		return operation.subscribeOn(workloadScheduler)
				.onErrorMap(EmbeddedDB::mapBatchWriteFailure)
				.doFinally(ignored -> putBatchTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS))
				.toFuture();
	}

	public CompletableFuture<Void> mergeBatchInternal(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull MergeBatchMode mode) throws RocksDBException {
		return mergeBatchInternal(columnId, batchPublisher, mode, scheduler.write());
	}

	public CompletableFuture<Void> mergeBatchInternal(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull MergeBatchMode mode,
			@NotNull Scheduler workloadScheduler) throws RocksDBException {
		final boolean ingestBehindEnabled;
		try {
			ingestBehindEnabled = config.global().ingestBehind();
		} catch (GestaltException e) {
			throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, e);
		}
		var start = System.nanoTime();
		actionLogger.logAction("MergeBatch (begin)",
				start,
				columnId,
				"multiple (async)",
				"multiple (async)",
				null,
				null,
				null,
				mode
		);

		Mono<Void> operation = Mono.using(
				() -> new MergeBatchState(columnId, mode, ingestBehindEnabled, start),
				state -> AdaptiveBatcher.buffer(
							Flux.from(batchPublisher).publishOn(workloadScheduler),
							128,
							4096,
							Duration.ofMillis(10)
						)
						.doOnNext(state::write)
						.then(Mono.fromRunnable(state::writePending)),
				BatchWriteState::close,
				true
			);
		return operation.subscribeOn(workloadScheduler)
				.onErrorMap(EmbeddedDB::mapBatchWriteFailure)
				.toFuture();
	}

	private static Throwable mapBatchWriteFailure(Throwable failure) {
		var current = failure;
		for (int depth = 0; current != null && depth < 32; depth++) {
			if (current instanceof RocksDBException rocksError) {
				return rocksError;
			}
			if (current.getCause() == current) {
				break;
			}
			current = current.getCause();
		}
		return RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, failure);
	}

	private abstract class BatchWriteState implements AutoCloseable {

		private static final VarHandle LIFECYCLE =
				varHandle(BatchWriteState.class, "lifecycle", int.class);
		private static final int ACTIVE = 1;
		private static final int CLOSE_REQUESTED = 1 << 1;
		private static final int CLOSED = 1 << 2;

		private volatile int lifecycle;

		private BatchWriteState() {
			ops.beginOp();
		}

		protected final void runWhileOpen(Runnable operation) {
			if (!tryEnter()) {
				return;
			}
			try {
				operation.run();
			} finally {
				exit();
			}
		}

		private boolean tryEnter() {
			for (;;) {
				int state = lifecycle;
				if ((state & (CLOSE_REQUESTED | CLOSED)) != 0) {
					return false;
				}
				if (LIFECYCLE.compareAndSet(this, state, state | ACTIVE)) {
					return true;
				}
			}
		}

		private void exit() {
			for (;;) {
				int state = lifecycle;
				int next = state & ~ACTIVE;
				if (LIFECYCLE.compareAndSet(this, state, next)) {
					if ((next & CLOSE_REQUESTED) != 0) {
						closeNow();
					}
					return;
				}
			}
		}

		@Override
		public final void close() {
			for (;;) {
				int state = lifecycle;
				if ((state & CLOSED) != 0) {
					return;
				}
				int next = state | CLOSE_REQUESTED;
				if (LIFECYCLE.compareAndSet(this, state, next)) {
					if ((state & ACTIVE) == 0) {
						closeNow();
					}
					return;
				}
			}
		}

		private void closeNow() {
			for (;;) {
				int state = lifecycle;
				if ((state & CLOSED) != 0) {
					return;
				}
				if ((state & ACTIVE) != 0) {
					return;
				}
				if (LIFECYCLE.compareAndSet(this, state, state | CLOSED)) {
					break;
				}
			}

			try {
				closeResources();
			} catch (Throwable error) {
				logger.error("Failed to close asynchronous batch-write resources", error);
			} finally {
				ops.endOp();
			}
		}

		protected abstract void closeResources();
	}

	private final class PutBatchState extends BatchWriteState {

		private final long columnId;
		private final PutBatchMode mode;
		private final long start;
		private ColumnInstance.ColumnUse columnUse;
		private ColumnInstance col;
		private DBWriter writer;
		private boolean sstEntriesWritten;

		private PutBatchState(long columnId, PutBatchMode mode, long start) {
			this.columnId = columnId;
			this.mode = mode;
			this.start = start;
			try {
				this.columnUse = acquireColumnUse(columnId);
				this.col = columnUse.column();
				this.writer = switch (mode) {
					case WRITE_BATCH, WRITE_BATCH_NO_WAL ->
							new WB(db.get(), new LeakSafeWriteBatch(), mode == PutBatchMode.WRITE_BATCH_NO_WAL);
					case SST_INGESTION, SST_INGEST_BEHIND ->
							getSSTWriter(columnId, null, false, mode == PutBatchMode.SST_INGEST_BEHIND);
				};
			} catch (Throwable error) {
				close();
				throw error;
			}
		}

		private void write(KVBatch batch) {
			runWhileOpen(() -> {
				validateBatch(batch);
				var keyIt = batch.keys().iterator();
				var valueIt = batch.values().iterator();
				while (keyIt.hasNext()) {
					var keys = keyIt.next();
					var value = valueIt.next();
					actionLogger.logAction("PutBatch (next)", start, columnId, keys, value, null, null, null, mode);
					put(writer, col, 0, keys, value, RequestType.none());
					if (writer instanceof SSTWriter) {
						sstEntriesWritten = true;
					}
					flushFullWriteBatch(writer);
				}
			});
		}

		private void writePending() {
			runWhileOpen(() -> {
				switch (writer) {
					case WB wb -> {
						if (wb.wb().count() > 0) {
							wb.writePending();
							recordCdcPublishedTail();
						}
					}
					case SSTWriter sst -> {
						if (sstEntriesWritten) {
							sst.writePending();
							recordCdcPublishedTail();
						}
					}
					case null -> { }
					default -> {
						writer.writePending();
						recordCdcPublishedTail();
					}
				}
			});
		}

		@Override
		protected void closeResources() {
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (Exception error) {
				throw new RuntimeException(error);
			} finally {
				if (columnUse != null) {
					columnUse.close();
				}
			}
		}
	}

	private final class MergeBatchState extends BatchWriteState {

		private final long columnId;
		private final MergeBatchMode mode;
		private final long start;
		private ColumnInstance.ColumnUse columnUse;
		private ColumnInstance col;
		private DBWriter writer;
		private ArrayList<Map.Entry<Keys, Buf>> pendingSstEntries;

		private MergeBatchState(long columnId, MergeBatchMode mode, boolean ingestBehindEnabled, long start) {
			this.columnId = columnId;
			this.mode = mode;
			this.start = start;
			try {
				this.columnUse = acquireColumnUse(columnId);
				this.col = columnUse.column();
				this.writer = switch (mode) {
					case MERGE_WRITE_BATCH, MERGE_WRITE_BATCH_NO_WAL -> {
						if (col.hasBuckets()) {
							yield openTransactionInternal(120_000, false);
						}
						yield new WB(db.get(), new LeakSafeWriteBatch(),
								mode == MergeBatchMode.MERGE_WRITE_BATCH_NO_WAL);
					}
					case MERGE_SST_INGESTION -> {
						pendingSstEntries = new ArrayList<>();
						yield getSSTWriter(columnId, null, false, false);
					}
					case MERGE_SST_INGEST_BEHIND -> {
						if (!ingestBehindEnabled) {
							throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
									"MERGE_SST_INGEST_BEHIND requires database.global.ingest-behind=true");
						}
						pendingSstEntries = new ArrayList<>();
						yield getSSTWriter(columnId, null, false, true);
					}
				};
			} catch (Throwable error) {
				close();
				throw error;
			}
		}

		private void write(List<KVBatch> batches) {
			runWhileOpen(() -> {
				for (var batch : batches) {
					validateBatch(batch);
					var keyIt = batch.keys().iterator();
					var valueIt = batch.values().iterator();
					while (keyIt.hasNext()) {
						var keys = keyIt.next();
						var value = valueIt.next();
						actionLogger.logAction("MergeBatch (next)", start, columnId, keys, value, null, null, null, mode);
						if (writer instanceof SSTWriter) {
							pendingSstEntries.add(Map.entry(keys, value));
						} else {
							merge(writer, col, 0L, keys, value, RequestType.none());
						}
						flushFullWriteBatch(writer);
					}
				}
				if (writer instanceof WB wb && wb.wb().count() > 0) {
					wb.flushAndReset();
					recordCdcPublishedTail();
				}
			});
		}

		private void writePending() {
			runWhileOpen(() -> {
				switch (writer) {
					case WB wb -> {
						if (wb.wb().count() > 0) {
							wb.writePending();
							recordCdcPublishedTail();
						}
					}
					case Tx tx -> {
						if (!closeTransactionInternal(tx, true)) {
							throw new RocksDBRetryException();
						}
					}
					case SSTWriter sst -> {
						if (writeSstEntries(col, sst, pendingSstEntries,
								mode == MergeBatchMode.MERGE_SST_INGEST_BEHIND)) {
							sst.writePending();
							recordCdcPublishedTail();
						}
					}
					case null -> { }
				}
			});
		}

		@Override
		protected void closeResources() {
			try {
				if (writer instanceof Tx tx) {
					if (tx.val().isOwningHandle()) {
						closeTransactionInternal(tx, false);
					}
				} else if (writer != null) {
					try {
						writer.close();
					} catch (Exception error) {
						throw new RuntimeException(error);
					}
				}
			} finally {
				if (columnUse != null) {
					columnUse.close();
				}
			}
		}
	}

	private static void validateBatch(KVBatch batch) {
		int keyCount = batch.keys().size();
		int valueCount = batch.values().size();
		if (keyCount != valueCount) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Batch key/value count mismatch: " + keyCount + " keys, " + valueCount + " values");
		}
	}

	private void flushFullWriteBatch(DBWriter writer) {
		if (writer instanceof WB wb
				&& (wb.wb().count() >= 10_000 || wb.wb().getDataSize() >= 4 * 1024 * 1024)) {
			wb.flushAndReset();
			recordCdcPublishedTail();
		}
	}

	private boolean writeSstEntries(ColumnInstance col,
			SSTWriter sst,
			@Nullable List<Map.Entry<Keys, Buf>> entries,
			boolean ingestBehind) throws RocksDBException {
		if (entries == null || entries.isEmpty()) {
			return false;
		}
		boolean wroteEntry = false;
		entries.sort((a, b) -> {
			var ka = col.calculateKey(a.getKey().keys()).toByteArray();
			var kb = col.calculateKey(b.getKey().keys()).toByteArray();
			return Arrays.compareUnsigned(ka, kb);
		});
		try (var ro = newReadOptions(null)) {
			for (var entry : entries) {
				var keys = entry.getKey();
				var value = entry.getValue();
				Buf calculatedKey = col.calculateKey(keys.keys());
				byte[] keyBytes = Utils.toByteArray(calculatedKey);
				if (col.hasBuckets()) {
					var existingRawBucket = dbGet(null, col, ro, calculatedKey);
					var bucket = existingRawBucket != null ? new Bucket(col, existingRawBucket) : new Bucket(col);
					var bucketElementKeys = col.getBucketElementKeys(keys.keys());
					var existing = bucket.getElement(bucketElementKeys);
					Buf mergedValue;
					if (existing != null) {
						var mergedRes = col.mergeOperator().merge(calculatedKey, existing, List.of(value));
						mergedValue = mergedRes != null ? mergedRes : existing;
					} else {
						mergedValue = value;
					}
					bucket.addElement(bucketElementKeys, mergedValue);
					byte[] valBytes = Utils.toByteArray(bucket.toSegment());
					sst.put(keyBytes, valBytes);
					wroteEntry = true;
				} else {
					Buf existing = dbGet(null, col, ro, calculatedKey);
					var mergeOp = col.mergeOperator();
					if (mergeOp == null) {
						throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "MergeBatch SST requires a merge operator");
					}
					Buf merged = existing != null ? mergeOp.merge(calculatedKey, existing, List.of(value)) : value;
					if (merged != null) {
						byte[] valBytes = Utils.toByteArray(merged);
						sst.put(keyBytes, valBytes);
						wroteEntry = true;
					}
				}
			}
		} catch (org.rocksdb.RocksDBException e) {
			throw RocksDBException.of(RocksDBErrorType.SST_WRITE_3, e);
		}
		return wroteEntry;
	}

	@VisibleForTesting
	public SSTWriter getSSTWriter(long colId,
			@Nullable GlobalDatabaseConfig globalDatabaseConfigOverride,
			boolean forceNoOptions,
			boolean ingestBehind) throws RocksDBException {
		RocksDBLoader.ColumnOptionsWithMerge columnConifg = null;
		RocksDBObjects refs = null;
		try {
			var col = getColumn(colId);
			if (!forceNoOptions) {
				var name = new String(col.cfh().getName(), StandardCharsets.UTF_8);
				refs = new RocksDBObjects();
				if (globalDatabaseConfigOverride != null) {
					columnConifg = RocksDBLoader.getColumnOptions(name,
							path,
							definitiveDbPath,
							globalDatabaseConfigOverride,
							logger,
							refs,
							false,
							caches
					);
				} else {
					try {
						columnConifg = RocksDBLoader.getColumnOptions(name,
								path,
								definitiveDbPath,
								this.config.global(),
								logger,
								refs,
								false,
								caches
						);
					} catch (GestaltException e) {
						throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, e);
					}
					refs = null;
				}
			}
			if (Files.notExists(tempSSTsPath)) {
				Files.createDirectories(tempSSTsPath);
			}
			return SSTWriter.open(tempSSTsPath,
					db,
					col,
					columnConifg != null ? columnConifg.options() : null,
					forceNoOptions,
					ingestBehind,
					refs
			);
		} catch (IOException ex) {
			if (refs != null) {
				refs.close();
			}
			if (columnConifg != null) {
				columnConifg.options().close();
			}
			if (columnConifg != null && columnConifg.mergeOperator() != null) {
				columnConifg.mergeOperator().close();
			}
			throw RocksDBException.of(RocksDBErrorType.SST_WRITE_2, ex);
		} catch (org.rocksdb.RocksDBException ex) {
			if (refs != null) {
				refs.close();
			}
			if (columnConifg != null) {
				columnConifg.options().close();
			}
			if (columnConifg != null && columnConifg.mergeOperator() != null) {
				columnConifg.mergeOperator().close();
			}
			throw RocksDBException.of(RocksDBErrorType.SST_WRITE_3, ex);
		} catch (Throwable ex) {
			if (refs != null) {
				refs.close();
			}
			if (columnConifg != null) {
				columnConifg.options().close();
			}
			if (columnConifg != null && columnConifg.mergeOperator() != null) {
				columnConifg.mergeOperator().close();
			}
			throw ex;
		}
	}

	@Override
	public void putBatch(long columnId, @NotNull Publisher<@NotNull KVBatch> batchPublisher, @NotNull PutBatchMode mode)
			throws RocksDBException {
		try {
			putBatchInternal(columnId, batchPublisher, mode).get();
		} catch (RocksDBException ex) {
			throw ex;
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} catch (Exception ex) {
			throw (RocksDBException) mapBatchWriteFailure(ex);
		}
	}

	@Override
	public void mergeBatch(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull MergeBatchMode mode) throws RocksDBException {
		try {
			mergeBatchInternal(columnId, batchPublisher, mode).get();
		} catch (RocksDBException ex) {
			throw ex;
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} catch (Exception ex) {
			throw (RocksDBException) mapBatchWriteFailure(ex);
		}
	}

	private <U> U put(@Nullable DBWriter optionalDbWriter,
			ColumnInstance col,
			long updateId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, U> callback) throws RocksDBException {
		return put(optionalDbWriter, col, updateId, keys, value, callback, true);
	}

	private <U> U put(@Nullable DBWriter optionalDbWriter,
			ColumnInstance col,
			long updateId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, U> callback,
			boolean allowWriteElisionProbe) throws RocksDBException {
		// Check for null value
		col.checkNullableValue(value);
		try {
			var writeElisionRequest = WriteElisionRequest.from(callback);
			if (writeElisionRequest != null && allowWriteElisionProbe) {
				if (optionalDbWriter == null && updateId == 0L) {
					var physicalKey = new PhysicalKey(col.calculateKey(keys.keys()).toByteArray());
					var decision = probeCachedLogicalValue(col, physicalKey, keys, value);
					recordWriteElisionDecision(writeElisionRequest, decision);
					if (decision == WriteElisionDecision.ELIDED) {
						return RequestType.safeCast(callback instanceof RequestType.RequestPreviousPresence<?>
								? Boolean.TRUE
								: null);
					}
				} else {
					recordWriteElisionDecision(writeElisionRequest, WriteElisionDecision.BYPASS_WRITER);
				}
			}
			boolean requirePreviousValue = RequestType.requiresGettingPreviousValue(callback);
			boolean requirePreviousPresence = RequestType.requiresGettingPreviousPresence(callback);
			boolean needsTx = col.hasBuckets() || requirePreviousValue || requirePreviousPresence;
			if (optionalDbWriter instanceof Tx tx && tx.isFromGetForUpdate() && (requirePreviousValue
					|| requirePreviousPresence)) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"You can't get the previous value or delta, when you are already updating that value"
				);
			}
			if (updateId != 0L && !(optionalDbWriter instanceof Tx)) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Update id must be accompanied with a valid transaction"
				);
			}
			if (col.hasBuckets() && (optionalDbWriter != null && !(optionalDbWriter instanceof Tx))) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Column with buckets don't support write batches"
				);
			}

			U result;
			DBWriter newTx;
			boolean owningNewTx = needsTx && optionalDbWriter == null;
			// Retry using a transaction: transactions are required to handle this kind of data
			newTx = owningNewTx ? this.openTransactionInternal(120_000, false) : optionalDbWriter;
			try {
				boolean didGetForUpdateInternally = false;
				boolean committedOwnedTx;
				do {
					Buf previousValue;
					Buf calculatedKey = col.calculateKey(keys.keys());
					byte[] calculatedKeyArray = calculatedKey.toByteArray();
					if (updateId != 0L) {
						assert !owningNewTx;
						((Tx) newTx).val().setSavePoint();
					}
					if (col.hasBuckets()) {
						assert newTx instanceof Tx;
						var bucketElementKeys = col.getBucketElementKeys(keys.keys());
						try (var readOptions = newReadOptions(null)) {
							var previousRawBucketByteArray
									= ((Tx) newTx).val().getForUpdate(readOptions, col.cfh(), calculatedKeyArray, true);
							didGetForUpdateInternally = true;
							Buf previousRawBucket = toBuf(previousRawBucketByteArray);
							var bucket = previousRawBucket != null ? new Bucket(col, previousRawBucket) : new Bucket(col);
							previousValue = transformResultValue(col, bucket.addElement(bucketElementKeys, value));
							var v = Utils.toByteArray(bucket.toSegment());
							((Tx) newTx).val().put(col.cfh(), calculatedKeyArray, v);
						} catch (org.rocksdb.RocksDBException e) {
							throw RocksDBException.of(RocksDBErrorType.PUT_1, e);
						}
					} else {
						if (RequestType.requiresGettingPreviousValue(callback)) {
							assert newTx instanceof Tx;
							try (var readOptions = newReadOptions(null)) {
								byte[] previousValueByteArray
										= ((Tx) newTx).val().getForUpdate(readOptions, col.cfh(), calculatedKeyArray, true);
								didGetForUpdateInternally = true;
								previousValue = transformResultValue(col, toBuf(previousValueByteArray));
							} catch (org.rocksdb.RocksDBException e) {
								throw RocksDBException.of(RocksDBErrorType.PUT_2, e);
							}
						} else if (RequestType.requiresGettingPreviousPresence(callback)) {
							// todo: in the future this should be replaced with just keyExists
							assert newTx instanceof Tx;
							try (var readOptions = newReadOptions(null)) {
								byte[] previousValueByteArray = ((Tx) newTx)
										.val()
										.getForUpdate(readOptions, col.cfh(), calculatedKeyArray, true);
								didGetForUpdateInternally = true;
								previousValue = previousValueByteArray != null ? emptyBuf() : null;
							} catch (org.rocksdb.RocksDBException e) {
								throw RocksDBException.of(RocksDBErrorType.PUT_2, e);
							}
						} else {
							previousValue = null;
						}
						switch (newTx) {
							case WB wb -> wb.wb().put(col.cfh(), calculatedKeyArray, value.toByteArray());
							case SSTWriter sstWriter -> {
								var valueBB = (col.schema().hasValue() ? value : dummyRocksDBEmptyValue()).toByteArray();
								sstWriter.put(calculatedKeyArray, valueBB);
							}
							case Tx t -> t.val().put(col.cfh(), calculatedKeyArray, value.toByteArray());
							case null -> {
								try (var w = new LeakSafeWriteOptions(null)) {
									var valueBB = (col.schema().hasValue() ? value : dummyRocksDBEmptyValue()).toByteArray();
									db.get().put(col.cfh(), w, calculatedKeyArray, valueBB);
									recordCdcPublishedTail();
								}
							}
						}
					}
					result = RequestType.safeCast(switch (callback) {
						case RequestType.RequestNothing<?> ignored -> null;
						case RequestType.RequestEnsure<?> ignored -> null;
						case RequestType.RequestPrevious<?> ignored -> previousValue;
						case RequestType.RequestPreviousPresence<?> ignored -> previousValue != null;
						case RequestType.RequestChanged<?> ignored -> !Utils.valueEquals(previousValue, value);
						case RequestType.RequestDelta<?> ignored -> new Delta<>(previousValue, value);
					});

					if (updateId != 0L) {
						boolean committed = closeTransaction(updateId, true);
						if (!committed) {
							try {
								((Tx) newTx).val().rollbackToSavePoint();
							} catch (org.rocksdb.RocksDBException | AssertionError e) {
								logger.debug("Failed to rollback to savepoint during commit failure in put", e);
							}
							int undosCount = 0;
							if (((Tx) newTx).isFromGetForUpdate()) {
								undosCount++;
							}
							if (didGetForUpdateInternally) {
								undosCount++;
							}
							for (int i = 0; i < undosCount; i++) {
								((Tx) newTx).val().undoGetForUpdate(col.cfh(), calculatedKeyArray);
							}
							throw new RocksDBRetryException();
						}
					}

					if (owningNewTx) {
						committedOwnedTx = this.closeTransactionInternal((Tx) newTx, true);
						if (!committedOwnedTx) {
							// FIX: We MUST close the failed transaction and open a fresh one.
							// If we reuse it, the C++ WriteBatch grows indefinitely on every retry!
							this.closeTransactionInternal((Tx) newTx, false);
							newTx = this.openTransactionInternal(120_000, false);
							didGetForUpdateInternally = false;
							Thread.yield();
						}
					} else {
						committedOwnedTx = true;
					}
				} while (!committedOwnedTx);
			} finally {
				if (owningNewTx) {
					this.closeTransactionInternal((Tx) newTx, false);
				}
			}
			return result;
		} catch (Exception ex) {
			if (updateId != 0L && !(ex instanceof RocksDBRetryException)) {
				closeTransaction(updateId, false);
			}
			if (ex instanceof RocksDBException rocksDBException) {
				throw rocksDBException;
			} else {
				throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
			}
		}
	}

	private <U> U delete(@Nullable DBWriter optionalDbWriter,
			ColumnInstance col,
			long updateId,
			@NotNull Keys keys,
			RequestType.RequestDelete<? super Buf, U> callback) throws RocksDBException {
		try {
			boolean requirePreviousValue = RequestType.requiresGettingPreviousValue(callback);
			boolean requirePreviousPresence = RequestType.requiresGettingPreviousPresence(callback);
			boolean needsTx = col.hasBuckets() || requirePreviousValue || requirePreviousPresence;
			if (optionalDbWriter instanceof Tx tx && tx.isFromGetForUpdate() && (requirePreviousValue
					|| requirePreviousPresence)) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"You can't get the previous value or delta, when you are already updating that value"
				);
			}
			if (updateId != 0L && !(optionalDbWriter instanceof Tx)) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Update id must be accompanied with a valid transaction"
				);
			}
			if (col.hasBuckets() && (optionalDbWriter != null && !(optionalDbWriter instanceof Tx))) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Column with buckets don't support write batches"
				);
			}

			U result;
			DBWriter newTx;
			boolean owningNewTx = needsTx && optionalDbWriter == null;
			// Retry using a transaction: transactions are required to handle this kind of data
			newTx = owningNewTx ? this.openTransactionInternal(120_000, false) : optionalDbWriter;
			try {
				boolean didGetForUpdateInternally = false;
				boolean committedOwnedTx;
				do {
					Buf previousValue;
					Buf calculatedKey = col.calculateKey(keys.keys());
					byte[] calculatedKeyArray = calculatedKey.toByteArray();
					if (updateId != 0L) {
						assert !owningNewTx;
						((Tx) newTx).val().setSavePoint();
					}
					if (col.hasBuckets()) {
						assert newTx instanceof Tx;
						var bucketElementKeys = col.getBucketElementKeys(keys.keys());
						try (var readOptions = newReadOptions(null)) {
							var previousRawBucketByteArray = ((Tx) newTx)
									.val()
									.getForUpdate(readOptions, col.cfh(), calculatedKeyArray, true);
							didGetForUpdateInternally = true;
							Buf previousRawBucket = toBuf(previousRawBucketByteArray);
							var bucket = previousRawBucket != null ? new Bucket(col, previousRawBucket) : new Bucket(col);
							previousValue = transformResultValue(col, bucket.removeElement(bucketElementKeys));
							var v = Utils.toByteArray(bucket.toSegment());
							((Tx) newTx).val().put(col.cfh(), calculatedKeyArray, v);
						} catch (org.rocksdb.RocksDBException e) {
							throw RocksDBException.of(RocksDBErrorType.PUT_1, e);
						}
					} else {
						if (RequestType.requiresGettingPreviousValue(callback)) {
							assert newTx instanceof Tx;
							try (var readOptions = newReadOptions(null)) {
								byte[] previousValueByteArray;
								previousValueByteArray = ((Tx) newTx)
										.val()
										.getForUpdate(readOptions, col.cfh(), calculatedKeyArray, true);
								didGetForUpdateInternally = true;
								previousValue = transformResultValue(col, toBuf(previousValueByteArray));
							} catch (org.rocksdb.RocksDBException e) {
								throw RocksDBException.of(RocksDBErrorType.PUT_2, e);
							}
						} else if (RequestType.requiresGettingPreviousPresence(callback)) {
							// todo: in the future this should be replaced with just keyExists
							assert newTx instanceof Tx;
							try (var readOptions = newReadOptions(null)) {
								byte[] previousValueByteArray;
								previousValueByteArray = ((Tx) newTx)
										.val()
										.getForUpdate(readOptions, col.cfh(), calculatedKeyArray, true);
								didGetForUpdateInternally = true;
								previousValue = previousValueByteArray != null ? emptyBuf() : null;
							} catch (org.rocksdb.RocksDBException e) {
								throw RocksDBException.of(RocksDBErrorType.PUT_2, e);
							}
						} else {
							previousValue = null;
						}
						switch (newTx) {
							case WB wb -> wb.wb().delete(col.cfh(), calculatedKeyArray);
							case SSTWriter sstWriter -> {
								// SSTWriter doesn't support delete in standard way if using ingest, but here we can't delete from SST.
								// Actually SST ingestion is usually for loading new data.
								// RocksDB SstFileWriter doesn't seem to support delete explicitly?
								// Actually it does if we put empty value? No.
								// Delete is not supported in SST ingestion usually for update-in-place.
								// But we can check if we should support it.
								throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Delete not supported in SST writer mode");
							}
							case Tx t -> t.val().delete(col.cfh(), calculatedKeyArray);
							case null -> {
								try (var w = new LeakSafeWriteOptions(null)) {
									db.get().delete(col.cfh(), w, calculatedKeyArray);
									recordCdcPublishedTail();
								}
							}
						}
					}
					result = RequestType.safeCast(switch (callback) {
						case RequestType.RequestNothing<?> ignored -> null;
						case RequestType.RequestPrevious<?> ignored -> previousValue;
						case RequestType.RequestPreviousPresence<?> ignored -> previousValue != null;
						default -> throw new IllegalStateException("Unexpected value: " + callback);
					});

					if (updateId != 0L) {
						boolean committed = closeTransaction(updateId, true);
						if (!committed) {
							try {
								((Tx) newTx).val().rollbackToSavePoint();
							} catch (org.rocksdb.RocksDBException | AssertionError e) {
								logger.debug("Failed to rollback to savepoint during commit failure in delete", e);
							}
							int undosCount = 0;
							if (((Tx) newTx).isFromGetForUpdate()) {
								undosCount++;
							}
							if (didGetForUpdateInternally) {
								undosCount++;
							}
							for (int i = 0; i < undosCount; i++) {
								((Tx) newTx).val().undoGetForUpdate(col.cfh(), calculatedKeyArray);
							}
							throw new RocksDBRetryException();
						}
					}

					if (owningNewTx) {
						committedOwnedTx = this.closeTransactionInternal((Tx) newTx, true);
						if (!committedOwnedTx) {
							// FIX: We MUST close the failed transaction and open a fresh one.
							// If we reuse it, the C++ WriteBatch grows indefinitely on every retry!
							this.closeTransactionInternal((Tx) newTx, false);
							newTx = this.openTransactionInternal(120_000, false);
							didGetForUpdateInternally = false;
							Thread.yield();
						}
					} else {
						committedOwnedTx = true;
					}
 			} while (!committedOwnedTx);
			} finally {
				if (owningNewTx) {
					this.closeTransactionInternal((Tx) newTx, false);
				}
			}
			return result;
		} catch (Exception ex) {
			if (updateId != 0L && !(ex instanceof RocksDBRetryException)) {
				closeTransaction(updateId, false);
			}
			if (ex instanceof RocksDBException rocksDBException) {
				throw rocksDBException;
			} else {
				throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
			}
		}
	}

	private <U> U merge(@Nullable DBWriter optionalDbWriter,
			ColumnInstance col,
			long updateId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestMerge<? super Buf, U> callback) throws RocksDBException {
		try {
			boolean needsTx = col.hasBuckets() || !(callback instanceof RequestType.RequestNothing<?>);
			if (optionalDbWriter instanceof Tx tx && tx.isFromGetForUpdate() && callback instanceof RequestType.RequestMerged<?>) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"You can't get the merged value, when you are already updating that value"
				);
			}
			if (updateId != 0L && !(optionalDbWriter instanceof Tx)) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Update id must be accompanied with a valid transaction"
				);
			}

			U result;
			DBWriter newTx;
			boolean owningNewTx = needsTx && optionalDbWriter == null;
			// Retry using a transaction: transactions are required to handle this kind of data
			newTx = owningNewTx ? this.openTransactionInternal(120_000, false) : optionalDbWriter;
			try {
				boolean didGetForUpdateInternally = false;
				boolean committedOwnedTx;
				do {
					Buf calculatedKey = col.calculateKey(keys.keys());
					byte[] calculatedKeyArray = calculatedKey.toByteArray();
					if (updateId != 0L) {
						assert !owningNewTx;
						((Tx) newTx).val().setSavePoint();
					}
					if (col.hasBuckets()) {
						assert newTx instanceof Tx;
						var bucketElementKeys = col.getBucketElementKeys(keys.keys());
						try (var readOptions = newReadOptions(null)) {
							var previousRawBucketByteArray = ((Tx) newTx)
									.val()
									.getForUpdate(readOptions, col.cfh(), calculatedKeyArray, true);
							didGetForUpdateInternally = true;
							Buf previousRawBucket = toBuf(previousRawBucketByteArray);
							var bucket = previousRawBucket != null ? new Bucket(col, previousRawBucket) : new Bucket(col);
							var existing = bucket.getElement(bucketElementKeys);
							Buf mergedValue;
							if (existing != null) {
								var mergedRes = col.mergeOperator().merge(calculatedKey, existing, List.of(value));
								mergedValue = mergedRes != null ? mergedRes : existing;
							} else {
								mergedValue = value;
							}
							bucket.addElement(bucketElementKeys, mergedValue);
							var v = Utils.toByteArray(bucket.toSegment());
							((Tx) newTx).val().put(col.cfh(), calculatedKeyArray, v);
							if (callback instanceof RequestType.RequestMerged<?>) {
								var merged = bucket.getElement(col.getBucketElementKeys(keys.keys()));
								result = RequestType.safeCast(transformResultValue(col, merged));
							} else {
								result = null;
							}
						} catch (org.rocksdb.RocksDBException e) {
							throw RocksDBException.of(RocksDBErrorType.PUT_1, e);
						}
					} else {
						switch (newTx) {
							case WB wb -> wb.wb().merge(col.cfh(), calculatedKeyArray, value.toByteArray());
							case SSTWriter ignored ->
									throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Merge not supported with SST writer");
							case Tx t -> t.val().merge(col.cfh(), calculatedKeyArray, value.toByteArray());
							case null -> {
								try (var w = new LeakSafeWriteOptions(null)) {
									db.get().merge(col.cfh(), w, calculatedKeyArray, value.toByteArray());
									recordCdcPublishedTail();
								}
							}
						}
						if (callback instanceof RequestType.RequestMerged<?>) {
							Buf merged;
							try (var readOptions = newReadOptions(null)) {
								merged = dbGet(newTx instanceof Tx ? (Tx) newTx : null, col, readOptions, calculatedKey);
							}
							result = RequestType.safeCast(merged);
						} else {
							result = null;
						}
					}

					if (updateId != 0L) {
						boolean committed = closeTransaction(updateId, true);
						if (!committed) {
							try {
								((Tx) newTx).val().rollbackToSavePoint();
							} catch (org.rocksdb.RocksDBException | AssertionError e) {
								logger.debug("Failed to rollback to savepoint during commit failure in merge", e);
							}
							int undosCount = 0;
							if (((Tx) newTx).isFromGetForUpdate()) {
								undosCount++;
							}
							if (didGetForUpdateInternally) {
								undosCount++;
							}
							for (int i = 0; i < undosCount; i++) {
								((Tx) newTx).val().undoGetForUpdate(col.cfh(), calculatedKeyArray);
							}
							throw new RocksDBRetryException();
						}
					}

					if (owningNewTx) {
						committedOwnedTx = this.closeTransactionInternal((Tx) newTx, true);
						if (!committedOwnedTx) {
							this.closeTransactionInternal((Tx) newTx, false);
							newTx = this.openTransactionInternal(120_000, false);
							didGetForUpdateInternally = false;
							Thread.yield();
						}
					} else {
						committedOwnedTx = true;
					}
				} while (!committedOwnedTx);
			} finally {
				if (owningNewTx) {
					this.closeTransactionInternal((Tx) newTx, false);
				}
			}
			return result;
		} catch (Exception ex) {
			if (updateId != 0L && !(ex instanceof RocksDBRetryException)) {
				closeTransaction(updateId, false);
			}
			if (ex instanceof RocksDBException rocksDBException) {
				throw rocksDBException;
			} else {
				throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
			}
		}
	}

	private Buf transformResultValue(ColumnInstance col, Buf realPreviousValue) {
		return col.schema().hasValue() ? realPreviousValue : (realPreviousValue != null ? emptyBuf() : null);
	}

	/**
	 * Attempts the embedded fast path for a current-value Get.
	 *
	 * @return a result (including a missing result), or {@code null} when this
	 *     column/database must use the ordinary API path.
	 */
	public @Nullable FastGetResult tryGetFast(long columnId, Keys keys, FastGetOutput output) {
		Objects.requireNonNull(keys, "keys");
		Objects.requireNonNull(output, "output");
		if (!fastGet) {
			return null;
		}

		long start = System.nanoTime();
		ColumnInstance col = null;
		boolean operationStarted = false;
		boolean ownershipTransferred = false;
		try {
			col = beginColumnUse(columnId);
			if (col.hasBuckets() || !col.schema().hasValue()) {
				return null;
			}
			ops.beginOp();
			operationStarted = true;

			Buf calculatedKey = col.calculateKey(keys.keys());
			var reader = Objects.requireNonNull(fastGetReader);
			byte[] keyArray = calculatedKey.getBackingByteArray();
			int keyOffset = calculatedKey.getBackingByteArrayOffset();
			int keyLength = calculatedKey.getBackingByteArrayLength();

			if (output == FastGetOutput.EXACT_HEAP) {
				byte[] value = reader.getHeap(col.cfh(), keyArray, keyOffset, keyLength);
				return value == null
						? new FastGetResult(false, null, false, null)
						: new FastGetResult(true, Buf.wrap(value), false, null);
			}

			NativeRocksDBGet.PinnedGetLease lease = reader.getPinned(
					col.cfh(), keyArray, keyOffset, keyLength);
			if (lease == null) {
				return new FastGetResult(false, null, false, null);
			}
			boolean leaseTransferred = false;
			try {
					if (output != FastGetOutput.PINNED
							&& !prefersPinnedFastGet(lease.value().size())) {
						byte[] value = lease.copyAndClose();
						return new FastGetResult(true, Buf.wrap(value), false, null);
					}

					var pinnedValue = lease.value();
					ColumnInstance leasedColumn = col;
				var result = new FastGetResult(true,
						pinnedValue,
						true,
						() -> closePinnedFastGet(lease, leasedColumn, start));
				ownershipTransferred = true;
				leaseTransferred = true;
				return result;
			} finally {
				if (!leaseTransferred) {
					lease.close();
				}
			}
		} catch (org.rocksdb.RocksDBException exception) {
			throw RocksDBException.of(RocksDBErrorType.GET_1, exception);
		} finally {
			if (!ownershipTransferred) {
				if (operationStarted) {
					ops.endOp();
				}
				if (col != null) {
					col.endUse();
				}
				getTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
			}
		}
	}

	private static boolean prefersPinnedFastGet(int valueBytes) {
		if (PINNED_GET_MIN_BYTES_OVERRIDE >= 0) {
			return valueBytes >= PINNED_GET_MIN_BYTES_OVERRIDE;
		}
		return valueBytes >= 32 * 1024;
	}

	private void closePinnedFastGet(NativeRocksDBGet.PinnedGetLease lease,
			ColumnInstance col,
			long start) {
		Throwable failure = null;
		try {
			lease.close();
		} catch (Throwable exception) {
			failure = exception;
		}
		try {
			ops.endOp();
		} catch (Throwable exception) {
			if (failure == null) {
				failure = exception;
			} else {
				failure.addSuppressed(exception);
			}
		}
		try {
			col.endUse();
		} catch (Throwable exception) {
			if (failure == null) {
				failure = exception;
			} else {
				failure.addSuppressed(exception);
			}
		} finally {
			getTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
		}
		if (failure instanceof RuntimeException runtimeException) {
			throw runtimeException;
		}
		if (failure instanceof Error error) {
			throw error;
		}
		if (failure != null) {
			throw new IllegalStateException("Failed to close pinned gRPC Get", failure);
		}
	}

	@Override
	public <T> T get(long transactionOrUpdateId, long columnId, Keys keys, RequestGet<? super Buf, T> requestType)
			throws RocksDBException {
		return get(transactionOrUpdateId,
				columnId,
				keys,
				requestType,
				WorkloadProfile.BATCH);
	}

	public <T> T get(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			RequestGet<? super Buf, T> requestType,
			WorkloadProfile workloadProfile) throws RocksDBException {
		var start = System.nanoTime();
		ColumnInstance col = null;
		ops.beginOp();
		try {
			actionLogger.logAction("Get", start, columnId, keys, null, transactionOrUpdateId, null, null, requestType);
			// Column id
			col = beginColumnUse(columnId);
			Tx prevTx = transactionOrUpdateId != 0
					? getTransaction(transactionOrUpdateId, true, workloadProfile)
					: null;
			Tx tx;
			long updateId;
			if (requestType instanceof RequestType.RequestForUpdate<?>) {
				if (prevTx == null) {
					tx = openTransactionInternal(MAX_TRANSACTION_DURATION_MS,
							true,
							workloadProfile,
							false);
					updateId = allocateTransactionInternal(tx);
				} else {
					tx = prevTx;
					updateId = transactionOrUpdateId;
				}
			} else {
				tx = prevTx;
				updateId = 0;
			}

			try {
				var result = get(tx, updateId, col, keys, requestType);
				actionLogger.logAction("Get (result)",
						start,
						columnId,
						keys,
						result,
						transactionOrUpdateId,
						null,
						null,
						requestType
				);
				return result;
			} catch (Throwable ex) {
				actionLogger.logAction("Get (result)",
						start,
						columnId,
						keys,
						"failure (exception)",
						transactionOrUpdateId,
						null,
						null,
						requestType
				);
				if (tx != prevTx) {
					closeTransaction(updateId, false);
				}
				throw ex;
			}
		} finally {
			if (col != null) {
				col.endUse();
			}
			ops.endOp();
			var end = System.nanoTime();
			getTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	@Override
	public List<Boolean> existsMulti(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			long timeoutMs) throws RocksDBException {
		return existsMulti(transactionId,
				columnId,
				keys,
				timeoutMs,
				WorkloadProfile.BATCH,
				RequestContext.NO_DEADLINE);
	}

	public List<Boolean> existsMulti(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			long timeoutMs,
			WorkloadProfile workloadProfile,
			long contextDeadlineEpochMillis) throws RocksDBException {
		var start = System.nanoTime();
		ops.beginOp();
		try {
			if (keys == null) {
				throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "keys");
			}
			actionLogger.logAction("ExistsMulti",
					start,
					columnId,
					keys.size(),
					null,
					transactionId,
					null,
					timeoutMs,
					null
			);

			var deadlineMicros = readDeadlineMicros(timeoutMs, contextDeadlineEpochMillis);
			var columnUse = acquireColumnUse(columnId);
			try (columnUse) {
				Tx tx = transactionId != 0
						? getTransaction(transactionId, false, workloadProfile)
						: null;
				try (var cursor = new ExistsMultiCursor(columnUse, tx, keys, deadlineMicros)) {
				while (!cursor.readChunk()) {
					// The synchronous API intentionally keeps processing on its caller thread.
				}
				return cursor.result();
				}
			}
		} catch (RocksDBException exception) {
			throw exception;
		} catch (Exception exception) {
			throw RocksDBException.of(RocksDBErrorType.GET_1, exception);
		} finally {
			ops.endOp();
			existsMultiTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
		}
	}

	/**
	 * Execute one bounded native MultiGet per composite scheduler turn while retaining
	 * one logical deadline, snapshot and ordered result accumulator for the request.
	 */
	public CompletableFuture<List<Boolean>> existsMultiAsyncInternal(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			long timeoutMs,
			WorkloadProfile workloadProfile,
			Executor executor,
			long contextDeadlineEpochMillis) {
		long requestStartNanos = System.nanoTime();
		if (keys == null) {
			return CompletableFuture.failedFuture(
					RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "keys"));
		}
		final long deadlineMicros;
		try {
			// Establish the request deadline before validation/copying so admission work
			// for a very large logical request cannot grant the native reads fresh time.
			deadlineMicros = readDeadlineMicros(timeoutMs, contextDeadlineEpochMillis);
		} catch (Throwable error) {
			return CompletableFuture.failedFuture(error);
		}
		// Yielding between native chunks lengthens the interval in which a caller could
		// mutate its list. Retain a stable logical request just as we retain its snapshot.
		final List<Keys> requestKeys;
		try {
			requestKeys = List.copyOf(keys);
		} catch (NullPointerException nullKey) {
			return CompletableFuture.failedFuture(
					RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "keys contains null"));
		} catch (Throwable error) {
			return CompletableFuture.failedFuture(error);
		}
		var request = new AsyncExistsMultiRequest(
				transactionId,
				columnId,
				requestKeys,
				timeoutMs,
				requestStartNanos,
				deadlineMicros,
				workloadProfile,
				executor,
				isCooperativeRetainedProfile(workloadProfile)
						&& !isExistsMultiProvablySingleChunk(requestKeys)
						&& executor instanceof RWScheduler.WorkloadExecutor workloadExecutor
						? workloadExecutor
						: null);
		request.scheduleInitial();
		return request;
	}

	/**
	 * A column-independent upper bound is enough to preserve the ordinary fast path:
	 * fixed key parts retain their input length and variable-key hashes have a known
	 * bounded output. A request below both native limits therefore cannot split.
	 */
	private static boolean isExistsMultiProvablySingleChunk(List<Keys> keys) {
		if (keys.size() > EXISTS_MULTI_MAX_KEYS_PER_NATIVE_CALL) {
			return false;
		}
		long maximumCalculatedBytes = 0L;
		for (var logicalKeys : keys) {
			for (var key : logicalKeys.keys()) {
				if (key == null) {
					return false;
				}
				maximumCalculatedBytes = saturatingAdd(maximumCalculatedBytes,
						Math.max(EXISTS_MULTI_MAX_VARIABLE_HASH_BYTES, key.size()));
				if (maximumCalculatedBytes > EXISTS_MULTI_MAX_KEY_BYTES_PER_NATIVE_CALL) {
					return false;
				}
			}
		}
		return true;
	}

	private static boolean isCooperativeRetainedProfile(WorkloadProfile profile) {
		return switch (profile) {
			case ANALYTICAL, INGEST, BATCH -> true;
			case LATENCY, CONTROL, CDC, PHYSICAL_MAINTENANCE -> false;
		};
	}

	private final class AsyncExistsMultiRequest extends CompletableFuture<List<Boolean>>
			implements Runnable, RWScheduler.CooperativeCompletionTask {

		private static final VarHandle RESOURCES_CLEANED =
				varHandle(AsyncExistsMultiRequest.class, "resourcesCleaned", boolean.class);
		private static final int INITIAL_QUEUED = 0;
		private static final int CHUNK_RUNNING = 1;
		private static final int FINISHED = 2;
		private static final int CHUNK_QUEUED = 3;

		private final long transactionId;
		private final long columnId;
		private final List<Keys> keys;
		private final long timeoutMs;
		private final long requestStartNanos;
		private final long deadlineMicros;
		private final WorkloadProfile workloadProfile;
		private final Executor executor;
		private final @Nullable RWScheduler.WorkloadExecutor cooperativeExecutor;
		private final Object lifecycleLock = new Object();
		private volatile boolean cancelRequested;
		private volatile boolean resourcesCleaned;
		private int state = INITIAL_QUEUED;
		private @Nullable ExistsMultiCursor cursor;
		private volatile @Nullable RWScheduler.CooperativeHandle cooperativeHandle;
		private boolean operationStarted;
		private boolean cooperativeCompletionPrepared;
		private @Nullable List<Boolean> cooperativeResult;
		private @Nullable Throwable resourceCleanupFailure;

		private AsyncExistsMultiRequest(long transactionId,
				long columnId,
				List<Keys> keys,
				long timeoutMs,
				long requestStartNanos,
				long deadlineMicros,
				WorkloadProfile workloadProfile,
				Executor executor,
				@Nullable RWScheduler.WorkloadExecutor cooperativeExecutor) {
			this.transactionId = transactionId;
			this.columnId = columnId;
			this.keys = keys;
			this.timeoutMs = timeoutMs;
			this.requestStartNanos = requestStartNanos;
			this.deadlineMicros = deadlineMicros;
			this.workloadProfile = Objects.requireNonNull(workloadProfile, "workloadProfile");
			this.executor = Objects.requireNonNull(executor, "executor");
			this.cooperativeExecutor = cooperativeExecutor;
		}

		private void scheduleInitial() {
			Throwable schedulingFailure = null;
			synchronized (asyncExistsMultiAdmissionLock) {
				synchronized (lifecycleLock) {
					if (state != INITIAL_QUEUED) {
						return;
					}
					try {
						// Count queue residence as part of the operation. Besides making the
						// request deadline honest, this makes admission linearizable with close().
						ops.beginOp();
						operationStarted = true;
						activeExistsMultiRequests.add(this);
						if (cooperativeExecutor == null) {
							executor.execute(this);
						} else {
							cooperativeHandle = cooperativeExecutor.executeCooperatively(this,
									EXISTS_MULTI_MAX_KEY_BYTES_PER_NATIVE_CALL);
						}
					} catch (Throwable error) {
						state = FINISHED;
						activeExistsMultiRequests.remove(this);
						schedulingFailure = error;
					}
				}
			}
			if (schedulingFailure != null) {
				completeTerminal(null, schedulingFailure);
			}
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			if (cooperativeExecutor != null) {
				synchronized (lifecycleLock) {
					if (state == FINISHED) {
						return false;
					}
					boolean beforeFirstRun = state == INITIAL_QUEUED;
					var handle = Objects.requireNonNull(cooperativeHandle, "cooperativeHandle");
					// Let the scheduler's terminal CAS choose between cancellation,
					// deadline, shutdown, and a concurrently completed RUN before the
					// local flag is published for idempotency.
					boolean cancellationWon = handle.cancel();
					if (cancellationWon) {
						cancelRequested = true;
					}
					return beforeFirstRun && cancellationWon;
				}
			}
			boolean cleanupQueuedRequest = false;
			boolean initialQueuedCancellation = false;
			synchronized (lifecycleLock) {
				switch (state) {
					case INITIAL_QUEUED -> {
						state = FINISHED;
						removeQueuedTask();
						cleanupQueuedRequest = true;
						initialQueuedCancellation = true;
					}
					case CHUNK_RUNNING -> {
						cancelRequested = true;
						return false;
					}
					case CHUNK_QUEUED -> {
						cancelRequested = true;
						state = FINISHED;
						removeQueuedTask();
						cleanupQueuedRequest = true;
					}
					default -> {
						return false;
					}
				}
			}
			if (cleanupQueuedRequest) {
				completeTerminal(null, new java.util.concurrent.CancellationException());
			}
			// Once a native chunk has run, keep the original future observable until
			// cleanup finishes rather than reporting that cancellation won immediately.
			return initialQueuedCancellation;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			final boolean initialize;
			synchronized (lifecycleLock) {
				if (state == INITIAL_QUEUED) {
					state = CHUNK_RUNNING;
					initialize = true;
				} else if (state == CHUNK_QUEUED) {
					state = CHUNK_RUNNING;
					initialize = false;
				} else {
					return RWScheduler.CooperativeResult.COMPLETE;
				}
			}
			if (context.terminationRequested()) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			if (initialize) {
				try {
					initialize();
				} catch (VirtualMachineError fatal) {
					throw fatal;
				} catch (Throwable error) {
					context.fail(cooperativeFailure(mapExistsMultiFailure(error),
							"Cooperative existsMulti initialization failed"));
					return RWScheduler.CooperativeResult.COMPLETE;
				}
			}

			try {
				var activeCursor = Objects.requireNonNull(cursor);
				while (true) {
					if (context.terminationRequested()) {
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					boolean exhausted = activeCursor.readChunk();
					if (context.terminationRequested()) {
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					if (exhausted) {
						prepareCooperativeCompletion(context, activeCursor.result());
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					if (context.preemptionRequested()) {
						synchronized (lifecycleLock) {
							if (state == CHUNK_RUNNING) {
								state = CHUNK_QUEUED;
							}
						}
						return RWScheduler.CooperativeResult.YIELD;
					}
				}
			} catch (VirtualMachineError fatal) {
				throw fatal;
			} catch (Throwable error) {
				context.fail(cooperativeFailure(mapExistsMultiFailure(error),
						"Cooperative existsMulti native read failed"));
				return RWScheduler.CooperativeResult.COMPLETE;
			}
		}

		private void prepareCooperativeCompletion(RWScheduler.CooperativeContext context,
				List<Boolean> result) {
			var cleanupFailure = cleanupTerminalResources(null);
			if (cleanupFailure != null) {
				if (cleanupFailure instanceof VirtualMachineError fatal) {
					throw fatal;
				}
				context.fail(cooperativeFailure(cleanupFailure,
						"Cooperative existsMulti cleanup failed"));
				return;
			}
			synchronized (lifecycleLock) {
				if (state != CHUNK_RUNNING) {
					return;
				}
				if (cooperativeCompletionPrepared) {
					throw new IllegalStateException("existsMulti completion was prepared twice");
				}
				cooperativeResult = result;
				cooperativeCompletionPrepared = true;
			}
		}

		@Override
		public void completeCooperatively() {
			final List<Boolean> result;
			synchronized (lifecycleLock) {
				if (state == FINISHED) {
					return;
				}
				if (state != CHUNK_RUNNING || !cooperativeCompletionPrepared) {
					throw new IllegalStateException("Scheduler selected RUN without an existsMulti result");
				}
				state = FINISHED;
				result = Objects.requireNonNull(cooperativeResult,
						"Prepared existsMulti result");
			}
			complete(result);
		}

		@Override
		public void run() {
			boolean initialize;
			synchronized (lifecycleLock) {
				if (state == INITIAL_QUEUED) {
					state = CHUNK_RUNNING;
					initialize = true;
				} else if (state == CHUNK_QUEUED) {
					state = CHUNK_RUNNING;
					initialize = false;
				} else {
					return;
				}
			}
			if (initialize && cancelRequested) {
				finishRunning(null, new java.util.concurrent.CancellationException());
				return;
			}
			if (initialize) {
				try {
					initialize();
				} catch (Throwable error) {
					finishRunning(null, mapExistsMultiFailure(error));
					return;
				}
			}
			if (cancelRequested) {
				finishRunning(null, new java.util.concurrent.CancellationException());
				return;
			}

			try {
				var activeCursor = Objects.requireNonNull(cursor);
				boolean exhausted = activeCursor.readChunk();
				if (cancelRequested) {
					finishRunning(null, new java.util.concurrent.CancellationException());
				} else if (exhausted) {
					finishRunning(activeCursor.result(), null);
				} else {
					scheduleNext();
				}
			} catch (Throwable error) {
				finishRunning(null, mapExistsMultiFailure(error));
			}
		}

		private void initialize() {
			actionLogger.logAction("ExistsMulti",
					requestStartNanos,
					columnId,
					keys.size(),
					null,
					transactionId,
					null,
					timeoutMs,
					null);
			if (deadlineMicros != Long.MAX_VALUE
					&& TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()) >= deadlineMicros) {
				throw RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED, "Deadline exceeded");
			}
			var columnUse = acquireColumnUse(columnId);
			try {
				Tx tx = transactionId != 0
						? getTransaction(transactionId, false, workloadProfile)
						: null;
				cursor = new ExistsMultiCursor(columnUse, tx, keys, deadlineMicros);
			} catch (Throwable error) {
				columnUse.close();
				throw error;
			}
		}

		private void scheduleNext() {
			Throwable schedulingFailure = null;
			boolean cancelled = false;
			synchronized (lifecycleLock) {
				if (state != CHUNK_RUNNING) {
					return;
				}
				if (cancelRequested) {
					state = FINISHED;
					cancelled = true;
				} else {
					state = CHUNK_QUEUED;
					try {
						executor.execute(this);
					} catch (Throwable error) {
						state = FINISHED;
						schedulingFailure = error;
					}
				}
			}
			if (cancelled) {
				completeTerminal(null, new java.util.concurrent.CancellationException());
			} else if (schedulingFailure != null) {
				completeTerminal(null, schedulingFailure);
			}
		}

		private void finishRunning(@Nullable List<Boolean> result, @Nullable Throwable failure) {
			synchronized (lifecycleLock) {
				if (state != CHUNK_RUNNING) {
					return;
				}
				state = FINISHED;
			}
			completeTerminal(result, failure);
		}

		private void completeTerminal(@Nullable List<Boolean> result, @Nullable Throwable failure) {
			var terminalFailure = cleanupTerminalResources(failure);
			if (terminalFailure != null) {
				completeExceptionally(terminalFailure);
			} else {
				complete(Objects.requireNonNull(result));
			}
		}

		private @Nullable Throwable cleanupTerminalResources(@Nullable Throwable originalFailure) {
			if (!RESOURCES_CLEANED.compareAndSet(this, false, true)) {
				var existingCleanupFailure = resourceCleanupFailure;
				return existingCleanupFailure == null
						? originalFailure
						: appendFailure(originalFailure, existingCleanupFailure);
			}
			Throwable cleanupFailure = null;
			if (cursor != null) {
				try {
					cursor.close();
				} catch (Throwable closeFailure) {
					cleanupFailure = appendFailure(cleanupFailure, closeFailure);
				}
			}
			if (operationStarted) {
				try {
					existsMultiTimer.record(System.nanoTime() - requestStartNanos, TimeUnit.NANOSECONDS);
				} catch (Throwable timerFailure) {
					cleanupFailure = appendFailure(cleanupFailure, timerFailure);
				}
				activeExistsMultiRequests.remove(this);
				try {
					// End last: close() may release meters and native handles as soon as
					// this request is the final SafeShutdown operation. The request must
					// no longer be visible as active before publishing that final release.
					ops.endOp();
				} catch (Throwable endFailure) {
					cleanupFailure = appendFailure(cleanupFailure, endFailure);
				}
			} else {
				activeExistsMultiRequests.remove(this);
			}
			resourceCleanupFailure = cleanupFailure;
			return cleanupFailure == null
					? originalFailure
					: appendFailure(originalFailure, cleanupFailure);
		}

		private void removeQueuedTask() {
			scheduler.removeQueuedTask(executor, this);
		}

		@Override
		public void reject(RuntimeException failure) {
			synchronized (lifecycleLock) {
				if (state == FINISHED) {
					return;
				}
				state = FINISHED;
			}
			completeTerminal(null, failure);
		}
	}

	private final class ExistsMultiCursor implements AutoCloseable {

		private final ColumnInstance.ColumnUse columnUse;
		private final ColumnInstance col;
		private final @Nullable Tx tx;
		private final List<Keys> keys;
		private final ArrayList<Boolean> result;
		private final @Nullable Snapshot snapshot;
		private final @Nullable ReadOptions readOptions;
		private @Nullable ExistsMultiChunk chunk;
		private boolean exhausted;
		private boolean closed;

		private ExistsMultiCursor(ColumnInstance.ColumnUse columnUse,
				@Nullable Tx tx,
				List<Keys> keys,
				long deadlineMicros) {
			this.columnUse = columnUse;
			this.col = columnUse.column();
			this.tx = tx;
			this.keys = keys;
			this.result = new ArrayList<>(keys.size());
			for (var logicalKeys : keys) {
				if (logicalKeys == null) {
					throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "keys contains null");
				}
			}
			if (keys.isEmpty()) {
				this.snapshot = null;
				this.readOptions = null;
				this.exhausted = true;
				return;
			}

			this.chunk = calculateExistsMultiChunk(col, keys, 0);
			Snapshot createdSnapshot = null;
			ReadOptions createdReadOptions = null;
			try {
				if (chunk.nextOffset() < keys.size()) {
					createdSnapshot = db.get().getSnapshot();
					var snapshotObserver = existsMultiSnapshotObserver;
					if (snapshotObserver != null) {
						snapshotObserver.run();
					}
				}
				createdReadOptions = newReadOptions("exists-multi-read-options");
				createdReadOptions.setDeadline(deadlineMicros);
				createdReadOptions.setFillCache(false);
				if (createdSnapshot != null) {
					createdReadOptions.setSnapshot(createdSnapshot);
				}
			} catch (Throwable error) {
				if (createdReadOptions != null) {
					try {
						createdReadOptions.close();
					} catch (Throwable closeFailure) {
						error.addSuppressed(closeFailure);
					}
				}
				if (createdSnapshot != null) {
					try {
						db.get().releaseSnapshot(createdSnapshot);
					} catch (Throwable closeFailure) {
						error.addSuppressed(closeFailure);
					}
				}
				throw error;
			}
			this.snapshot = createdSnapshot;
			this.readOptions = createdReadOptions;
		}

		private boolean readChunk() {
			if (exhausted) {
				return true;
			}
			var currentChunk = Objects.requireNonNull(chunk);
			try {
				if (tx == null && !col.hasBuckets()) {
					result.addAll(existsMultiStatusOnly(col,
							Objects.requireNonNull(readOptions),
							currentChunk.calculatedKeys()));
				} else {
					result.addAll(existsMultiWithValues(tx,
							col,
							Objects.requireNonNull(readOptions),
							currentChunk.logicalKeys(),
							currentChunk.calculatedKeys()));
				}
			} catch (org.rocksdb.RocksDBException exception) {
				throw mapIteratorStatusException(exception);
			}
			var chunkObserver = existsMultiChunkObserver;
			if (chunkObserver != null) {
				chunkObserver.run();
			}
			if (currentChunk.nextOffset() >= keys.size()) {
				exhausted = true;
				return true;
			}
			chunk = calculateExistsMultiChunk(col, keys, currentChunk.nextOffset());
			return false;
		}

		private List<Boolean> result() {
			if (!exhausted) {
				throw new IllegalStateException("ExistsMulti result requested before exhaustion");
			}
			return result;
		}

		@Override
		public void close() {
			if (closed) {
				return;
			}
			closed = true;
			Throwable failure = null;
			if (readOptions != null) {
				try {
					readOptions.close();
				} catch (Throwable error) {
					failure = error;
				}
			}
			if (snapshot != null) {
				try {
					db.get().releaseSnapshot(snapshot);
				} catch (Throwable error) {
					failure = appendFailure(failure, error);
				}
			}
			try {
				columnUse.close();
			} catch (Throwable error) {
				failure = appendFailure(failure, error);
			}
			if (failure instanceof RuntimeException runtimeException) {
				throw runtimeException;
			}
			if (failure instanceof Error error) {
				throw error;
			}
		}
	}

	private static Throwable mapExistsMultiFailure(Throwable error) {
		if (error instanceof RocksDBException) {
			return error;
		}
		return error instanceof Exception
				? RocksDBException.of(RocksDBErrorType.GET_1, error)
				: error;
	}

	private static RuntimeException cooperativeFailure(Throwable failure, String message) {
		if (failure instanceof RuntimeException runtimeException) {
			return runtimeException;
		}
		if (failure instanceof org.rocksdb.RocksDBException rocksDBException) {
			return mapIteratorStatusException(rocksDBException);
		}
		return new RejectedExecutionException(message, failure);
	}

	private static Throwable appendFailure(@Nullable Throwable current, Throwable next) {
		if (current == null) {
			return next;
		}
		if (current != next) {
			current.addSuppressed(next);
		}
		return current;
	}

	private record ExistsMultiChunk(List<Keys> logicalKeys, List<Buf> calculatedKeys, int nextOffset) {
	}

	private ExistsMultiChunk calculateExistsMultiChunk(ColumnInstance col, List<Keys> keys, int offset) {
		int chunkCapacity = Math.min(EXISTS_MULTI_MAX_KEYS_PER_NATIVE_CALL, keys.size() - offset);
		var logicalChunk = new ArrayList<Keys>(chunkCapacity);
		var calculatedChunk = new ArrayList<Buf>(chunkCapacity);
		long calculatedKeyBytes = 0L;
		while (offset < keys.size() && calculatedChunk.size() < EXISTS_MULTI_MAX_KEYS_PER_NATIVE_CALL) {
			var logicalKey = keys.get(offset++);
			var calculatedKey = col.calculateKey(logicalKey.keys());
			logicalChunk.add(logicalKey);
			calculatedChunk.add(calculatedKey);
			calculatedKeyBytes = saturatingAdd(calculatedKeyBytes, calculatedKey.size());
			if (calculatedKeyBytes >= EXISTS_MULTI_MAX_KEY_BYTES_PER_NATIVE_CALL) {
				break;
			}
		}
		return new ExistsMultiChunk(logicalChunk, calculatedChunk, offset);
	}

	private List<Boolean> existsMultiStatusOnly(ColumnInstance col,
			ReadOptions readOptions,
			List<Buf> calculatedKeys) throws org.rocksdb.RocksDBException {
		var arena = Arena.ofConfined();
		var arenaObserver = existsMultiArenaObserver;
		try {
			if (arenaObserver != null) {
				arenaObserver.accept(true);
			}
			var nativeKeys = new MemorySegment[calculatedKeys.size()];
			var emptyValues = new MemorySegment[calculatedKeys.size()];
			for (int i = 0; i < calculatedKeys.size(); i++) {
				nativeKeys[i] = copyToNativeSegment(arena, calculatedKeys.get(i));
				emptyValues[i] = arena.allocate(0);
			}

			var statuses = db.get().multiGetByteBuffers(readOptions, List.of(col.cfh()), nativeKeys, emptyValues);
			var result = new ArrayList<Boolean>(statuses.size());
			for (var status : statuses) {
				result.add(switch (status.status.getCode()) {
					case Ok -> true;
					case NotFound -> false;
					default -> throw mapIteratorStatusException(new org.rocksdb.RocksDBException(status.status));
				});
			}
			return result;
		} finally {
			try {
				arena.close();
			} finally {
				if (arenaObserver != null) {
					arenaObserver.accept(false);
				}
			}
		}
	}

	private List<Boolean> existsMultiWithValues(@Nullable Tx tx,
			ColumnInstance col,
			ReadOptions readOptions,
			List<Keys> logicalKeys,
			List<Buf> calculatedKeys) throws org.rocksdb.RocksDBException {
		var nativeKeys = calculatedKeys.stream().map(Buf::toByteArray).toList();
		List<byte[]> values;
		if (tx == null) {
			values = db.get().multiGetAsList(readOptions,
					Collections.nCopies(nativeKeys.size(), col.cfh()),
					nativeKeys);
		} else {
			values = tx.val().multiGetAsList(readOptions, col.cfh(), nativeKeys);
		}

		var result = new ArrayList<Boolean>(values.size());
		for (int i = 0; i < values.size(); i++) {
			var value = values.get(i);
			if (value == null) {
				result.add(false);
			} else if (col.hasBuckets()) {
				var bucket = new Bucket(col, Buf.wrap(value));
				result.add(bucket.getElement(col.getBucketElementKeys(logicalKeys.get(i).keys())) != null);
			} else {
				result.add(true);
			}
		}
		return result;
	}

	private <T> T get(Tx tx, long updateId, ColumnInstance col, Keys keys, RequestGet<? super Buf, T> callback)
			throws RocksDBException {
		try {
			if (!col.schema().hasValue() && RequestType.requiresGettingCurrentValue(callback)) {
				throw RocksDBException.of(RocksDBErrorType.VALUE_MUST_BE_NULL,
						"The specified callback requires a return value, but this column does not have values!"
				);
			}
			Buf foundValue;
			boolean existsValue;

			Buf calculatedKey = col.calculateKey(keys.keys());
			if (col.hasBuckets()) {
				var bucketElementKeys = col.getBucketElementKeys(keys.keys());
				try {
					Buf previousRawBucket = dbGetWithDefaultOptions(tx, col, calculatedKey);
					if (previousRawBucket != null) {
						var bucket = new Bucket(col, previousRawBucket);
						foundValue = bucket.getElement(bucketElementKeys);
					} else {
						foundValue = null;
					}
					existsValue = foundValue != null;
				} catch (org.rocksdb.RocksDBException e) {
					throw RocksDBException.of(RocksDBErrorType.GET_1, e);
				}
			} else {
				boolean shouldGetCurrent = RequestType.requiresGettingCurrentValue(callback) || (tx != null
						&& callback instanceof RequestType.RequestExists<?>);
				if (shouldGetCurrent) {
					try {
						foundValue = dbGetWithDefaultOptions(tx, col, calculatedKey);
						existsValue = foundValue != null;
					} catch (org.rocksdb.RocksDBException e) {
						throw RocksDBException.of(RocksDBErrorType.PUT_2, e);
					}
				} else if (callback instanceof RequestType.RequestExists<?>) {
					// tx is always null here
					//noinspection ConstantValue
					assert tx == null;
					foundValue = null;
					existsValue = db
							.get()
							.keyExists(col.cfh(),
									calculatedKey.getBackingByteArray(),
									calculatedKey.getBackingByteArrayOffset(),
									calculatedKey.getBackingByteArrayLength()
							);
				} else {
					foundValue = null;
					existsValue = false;
				}
			}
			return RequestType.safeCast(switch (callback) {
				case RequestType.RequestNothing<?> ignored -> null;
				case RequestType.RequestCurrent<?> ignored -> foundValue;
				case RequestType.RequestForUpdate<?> ignored -> {
					assert updateId != 0;
					yield new UpdateContext<>(foundValue, updateId);
				}
				case RequestType.RequestExists<?> ignored -> existsValue;
			});
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		}
	}

	@Override
	public long openIterator(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException {
		return openIterator(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				timeoutMs,
				WorkloadProfile.BATCH);
	}

	public long openIterator(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs,
			WorkloadProfile workloadProfile) throws RocksDBException {
		var start = System.nanoTime();
		// Protect iterator construction as active work; the installed iterator's
		// longer lifetime is accounted separately from shutdown admission.
		ops.beginOp();
		boolean resourceLeaseRetained = false;
		try {
			actionLogger.logAction("OpenIterator",
					start,
					columnId,
					startKeysInclusive,
					endKeysExclusive,
					transactionId,
					reverse,
					timeoutMs,
					null
			);
			var expirationTimestamp = iteratorExpirationTimestamp(timeoutMs);
			var columnUse = acquireColumnUse(columnId);
			var col = columnUse.column();
			var state = new IteratorState(columnUse, reverse);
			RocksIterator it = null;
			REntry<RocksIterator> itEntry = null;
			try {
				var calculatedStartKey = startKeysInclusive != null && startKeysInclusive.keys().length > 0
						? col.calculateKey(startKeysInclusive.keys())
						: null;
				var calculatedEndKey = endKeysExclusive != null && endKeysExclusive.keys().length > 0
						? col.calculateKey(endKeysExclusive.keys())
						: null;
				var startKeySlice = calculatedStartKey != null ? toSlice(calculatedStartKey) : null;
				if (startKeySlice != null) {
					state.add(startKeySlice);
				}
				var endKeySlice = calculatedEndKey != null ? toSlice(calculatedEndKey) : null;
				if (endKeySlice != null) {
					state.add(endKeySlice);
				}

				var ro = newReadOptions("open-iterator-read-options");
				state.add(ro);
				ro.setDeadline(readDeadlineMicros(timeoutMs));
				if (startKeySlice != null) {
					ro.setIterateLowerBound(startKeySlice);
				}
				if (endKeySlice != null) {
					ro.setIterateUpperBound(endKeySlice);
				}
				if (transactionId != 0L) {
					//noinspection resource
					it = getTransaction(transactionId, false, workloadProfile).val().getIterator(ro, col.cfh());
				} else {
					it = db.get().newIterator(col.cfh(), ro);
				}
				state.iterator = it;
				if (reverse) {
					it.seekToLast();
				} else {
					it.seekToFirst();
				}
				checkIteratorStatusIfInvalid(it);

				itEntry = new REntry<>(it, expirationTimestamp, state, workloadProfile);
				retainResourceLease();
				resourceLeaseRetained = true;
				long iteratorId = FastRandomUtils.allocateNewValue(its, itEntry, 1, Long.MAX_VALUE);
				return iteratorId;
			} catch (Throwable ex) {
				try {
					if (itEntry != null) {
						itEntry.close();
					} else {
						if (it != null) {
							it.close();
						}
						state.close();
					}
				} catch (Throwable closeError) {
					ex.addSuppressed(closeError);
				} finally {
					if (resourceLeaseRetained) {
						releaseResourceLease();
					}
				}
				throw ex;
			}
		} finally {
			ops.endOp();
			var end = System.nanoTime();
			openIteratorTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private static long iteratorExpirationTimestamp(long timeoutMs) {
		if (timeoutMs < 0) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Iterator timeout must be non-negative");
		}
		long now = System.currentTimeMillis();
		return timeoutMs >= Long.MAX_VALUE - now ? Long.MAX_VALUE : now + timeoutMs;
	}

	private static long readDeadlineMicros(long timeoutMs) {
		if (timeoutMs < 0) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Read timeout must be non-negative");
		}
		long nowMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
		long timeoutMicros = TimeUnit.MILLISECONDS.toMicros(timeoutMs);
		return timeoutMicros >= Long.MAX_VALUE - nowMicros ? Long.MAX_VALUE : nowMicros + timeoutMicros;
	}

	private static long readDeadlineMicros(long timeoutMs, long contextDeadlineEpochMillis) {
		long operationDeadlineMicros = readDeadlineMicros(timeoutMs);
		if (contextDeadlineEpochMillis == RequestContext.NO_DEADLINE) {
			return operationDeadlineMicros;
		}
		long contextDeadlineMicros = contextDeadlineEpochMillis >= Long.MAX_VALUE / 1_000L
				? Long.MAX_VALUE
				: TimeUnit.MILLISECONDS.toMicros(contextDeadlineEpochMillis);
		return Math.min(operationDeadlineMicros, contextDeadlineMicros);
	}

	/** Compute the one immutable deadline shared by permit waiting, admission and native reads. */
	private ReadDeadline retainedReadDeadline(long timeoutMs,
			long contextDeadlineEpochMillis,
			String timeoutLabel) {
		if (timeoutMs < 0L) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					timeoutLabel + " must be non-negative");
		}
		long nowMillis = System.currentTimeMillis();
		long operationDeadlineMillis = timeoutMs >= Long.MAX_VALUE - nowMillis
				? Long.MAX_VALUE
				: nowMillis + timeoutMs;
		long ageDeadlineMillis = maxRetainedSnapshotAgeMs >= Long.MAX_VALUE - nowMillis
				? Long.MAX_VALUE
				: nowMillis + maxRetainedSnapshotAgeMs;
		long deadlineMillis = Math.min(operationDeadlineMillis, ageDeadlineMillis);
		if (contextDeadlineEpochMillis != RequestContext.NO_DEADLINE) {
			deadlineMillis = Math.min(deadlineMillis, contextDeadlineEpochMillis);
		}
		long deadlineMicros = deadlineMillis >= Long.MAX_VALUE / 1_000L
				? Long.MAX_VALUE
				: TimeUnit.MILLISECONDS.toMicros(deadlineMillis);
		return new ReadDeadline(deadlineMillis, deadlineMicros);
	}

	private final class IteratorState extends RocksDBObjects {

		private final ColumnInstance column;
		private final boolean reverse;
		private RocksIterator iterator;
		private java.util.ListIterator<Entry<Buf[], Buf>> bucketIterator;
		private boolean closed;

		private IteratorState(ColumnInstance.ColumnUse columnUse, boolean reverse) {
			super(columnUse);
			this.column = columnUse.column();
			this.reverse = reverse;
		}
	}

	private record LogicalIteratorStep(boolean present, @Nullable Buf value) {

		private static final LogicalIteratorStep END = new LogicalIteratorStep(false, null);
	}

	@Override
	public void closeIterator(long iteratorId) throws RocksDBException {
		var start = System.nanoTime();
		try {
			actionLogger.logAction("CloseIterator", start, null, null, null, null, null, null, null); // todo: improve logging
			closeIteratorInternal(iteratorId);
		} finally {
			var end = System.nanoTime();
			closeIteratorTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	/**
	 * Atomically claims an iterator before closing it. Removing the map entry is the ownership
	 * transfer that guarantees both the native handle and its resource lease are completed
	 * exactly once when explicit close, leak cleanup, and forced shutdown race.
	 */
	private boolean closeIteratorInternal(long iteratorId) {
		var entry = its.remove(iteratorId);
		if (entry == null) {
			return false;
		}
		try {
			if (entry.objs() instanceof IteratorState state) {
				synchronized (state) {
					state.closed = true;
					state.bucketIterator = null;
					entry.close();
				}
			} else {
				// Compatibility with synthetic entries used by close-race tests.
				entry.close();
			}
		} finally {
			releaseResourceLease();
		}
		return true;
	}

	@Override
	public void seekTo(long iterationId, @NotNull Keys keys) throws RocksDBException {
		var start = System.nanoTime();
		ops.beginOp();
		try {
			actionLogger.logAction("SeekTo", start, null, keys, null, iterationId, null, null, null);
			if (keys == null) {
				throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "Iterator seek keys cannot be null");
			}
			withIterator(iterationId, state -> {
				state.bucketIterator = null;
				if (keys.keys().length == 0) {
					if (state.reverse) {
						state.iterator.seekToLast();
					} else {
						state.iterator.seekToFirst();
					}
					checkIteratorStatusIfInvalid(state.iterator);
					positionBucketCursor(state, null, null);
					return null;
				}

				var calculatedKey = state.column.calculateKey(keys.keys());
				var target = calculatedKey.toByteArray();
				if (state.reverse) {
					state.iterator.seekForPrev(target);
				} else {
					state.iterator.seek(target);
				}
				checkIteratorStatusIfInvalid(state.iterator);
				positionBucketCursor(state, keys, target);
				return null;
			});
		} finally {
			ops.endOp();
			var end = System.nanoTime();
			seekToTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	@Override
	public <T> T subsequent(long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super Buf, T> requestType) throws RocksDBException {
		var start = System.nanoTime();
		ops.beginOp();
		try {
			actionLogger.logAction("Subsequent", start, null, skipCount, takeCount, iterationId, null, null, requestType);
			if (requestType == null) {
				throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "Iterator request type cannot be null");
			}
			if (skipCount < 0 || takeCount < 0) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Iterator skip and take counts must be non-negative");
			}

			return withIterator(iterationId, state -> {
				for (long i = 0; i < skipCount; i++) {
					if (!advanceIterator(state, false).present()) {
						break;
					}
				}

				Object result = switch (requestType) {
					case RequestType.RequestNothing<?> _ -> {
						for (long i = 0; i < takeCount; i++) {
							if (!advanceIterator(state, false).present()) {
								break;
							}
						}
						yield null;
					}
					case RequestType.RequestExists<?> _ -> {
						boolean found = false;
						for (long i = 0; i < takeCount; i++) {
							if (!advanceIterator(state, false).present()) {
								break;
							}
							found = true;
						}
						yield found;
					}
					case RequestType.RequestMulti<?> _ -> {
						var values = new ArrayList<Buf>((int) Math.min(takeCount, 1_024));
						for (long i = 0; i < takeCount; i++) {
							var step = advanceIterator(state, true);
							if (!step.present()) {
								break;
							}
							values.add(Objects.requireNonNull(step.value()));
						}
						yield values;
					}
				};
				return RequestType.safeCast(result);
			});
		} finally {
			ops.endOp();
			var end = System.nanoTime();
			subsequentTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	/**
	 * Advance an explicit iterator without materializing values and report the exact
	 * number of logical entries consumed. Async adapters use the count to stop
	 * scheduling slices immediately when the iterator is exhausted.
	 */
	public long advanceIteratorInternal(long iterationId, long maxCount) {
		var start = System.nanoTime();
		ops.beginOp();
		try {
			if (maxCount < 0) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Iterator advance count must be non-negative");
			}
			return withIterator(iterationId, state -> {
				long advanced = 0L;
				while (advanced < maxCount && advanceIterator(state, false).present()) {
					advanced++;
				}
				var observer = iteratorAdvanceStepCompletedObserver;
				if (observer != null) {
					observer.run();
				}
				return advanced;
			});
		} finally {
			ops.endOp();
			subsequentTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
		}
	}

	private <T> T withIterator(long iteratorId, Function<IteratorState, T> action) {
		var entry = its.get(iteratorId);
		if (entry == null || !(entry.objs() instanceof IteratorState state)) {
			throw iteratorNotFound(iteratorId);
		}
		// The embedded API historically serializes operations on the same iterator.
		// Remote adapters may reject overlapping requests before queueing them, but the
		// native ownership boundary itself must remain race-safe and blocking.
		synchronized (state) {
			if (state.closed || its.get(iteratorId) != entry) {
				throw iteratorNotFound(iteratorId);
			}
			Long expirationTimestamp = entry.expirationTimestamp();
			if (expirationTimestamp != null && System.currentTimeMillis() >= expirationTimestamp) {
				var removed = its.remove(iteratorId);
				if (removed == entry) {
					state.closed = true;
					state.bucketIterator = null;
					try {
						entry.close();
					} finally {
						releaseResourceLease();
					}
				}
				throw iteratorNotFound(iteratorId);
			}
			return action.apply(state);
		}
	}

	private static RocksDBException iteratorNotFound(long iteratorId) {
		return RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "No iterator with id " + iteratorId);
	}

	private LogicalIteratorStep advanceIterator(IteratorState state, boolean materializeValue) {
		while (true) {
			if (state.bucketIterator != null) {
				boolean hasNext = state.reverse ? state.bucketIterator.hasPrevious() : state.bucketIterator.hasNext();
				if (hasNext) {
					var entry = state.reverse ? state.bucketIterator.previous() : state.bucketIterator.next();
					boolean hasMoreInBucket = state.reverse
							? state.bucketIterator.hasPrevious()
							: state.bucketIterator.hasNext();
					if (!hasMoreInBucket) {
						state.bucketIterator = null;
						advanceNativeIterator(state);
						checkIteratorStatusIfInvalid(state.iterator);
					}
					return new LogicalIteratorStep(true, materializeValue ? entry.getValue() : null);
				}
				state.bucketIterator = null;
				advanceNativeIterator(state);
				continue;
			}

			if (!state.iterator.isValid()) {
				checkIteratorStatusIfInvalid(state.iterator);
				return LogicalIteratorStep.END;
			}

			if (state.column.hasBuckets()) {
				var bucket = new Bucket(state.column, toBuf(state.iterator.value()));
				var elements = bucket.getElements();
				state.bucketIterator = elements.listIterator(state.reverse ? elements.size() : 0);
				continue;
			}

			Buf value = null;
			if (materializeValue) {
				value = state.column.schema().hasValue() ? toBuf(state.iterator.value()) : emptyBuf();
			}
			advanceNativeIterator(state);
			checkIteratorStatusIfInvalid(state.iterator);
			return new LogicalIteratorStep(true, value);
		}
	}

	private static void advanceNativeIterator(IteratorState state) {
		if (state.reverse) {
			state.iterator.prev();
		} else {
			state.iterator.next();
		}
	}

	private void positionBucketCursor(IteratorState state, @Nullable Keys exactKeys, @Nullable byte[] target) {
		if (!state.iterator.isValid() || !state.column.hasBuckets()) {
			return;
		}
		var bucket = new Bucket(state.column, toBuf(state.iterator.value()));
		var elements = bucket.getElements();
		int cursor = state.reverse ? elements.size() : 0;
		if (exactKeys != null && target != null && Arrays.equals(state.iterator.key(), target)) {
			var variableKeys = state.column.getBucketElementKeys(exactKeys.keys());
			for (int i = 0; i < elements.size(); i++) {
				var candidate = elements.get(i).getKey();
				if (bucketKeysEqual(candidate, variableKeys)) {
					cursor = state.reverse ? i + 1 : i;
					break;
				}
			}
		}
		state.bucketIterator = elements.listIterator(cursor);
	}

	private static boolean bucketKeysEqual(Buf[] left, Buf[] right) {
		if (left.length != right.length) {
			return false;
		}
		for (int i = 0; i < left.length; i++) {
			if (!Utils.valueEquals(left[i], right[i])) {
				return false;
			}
		}
		return true;
	}

	private static void checkIteratorStatusIfInvalid(RocksIterator iterator) {
		if (iterator.isValid()) {
			return;
		}
		try {
			iterator.status();
		} catch (org.rocksdb.RocksDBException exception) {
			throw mapIteratorStatusException(exception);
		}
	}

	@VisibleForTesting
	public static RocksDBException mapIteratorStatusException(org.rocksdb.RocksDBException exception) {
		var status = exception.getStatus();
		var errorType = status != null && status.getCode() == Code.TimedOut
				? RocksDBErrorType.READ_DEADLINE_EXCEEDED
				: RocksDBErrorType.GET_1;
		return RocksDBException.of(errorType, exception);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T reduceRange(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@NotNull RequestReduceRange<? super KV, T> requestType,
			long timeoutMs) throws RocksDBException {
		var start = System.nanoTime();
		ops.beginOp();
		ColumnInstance col = null;
		try {
			actionLogger.logAction("ReduceRange",
					start,
					columnId,
					startKeysInclusive,
					endKeysExclusive,
					transactionId,
					null,
					timeoutMs,
					requestType
			); // todo: log if reversed or not
			col = beginColumnUse(columnId);


			// A bounded endpoint seek still fans out to every relevant RocksDB merge child.
			// With many SSTs, async_io lets RocksDB submit those child reads together before
			// polling them; disabling it serializes cold index/data-block misses.
			try (var ro = newReadOptions(null)) {
				var asyncIoObserver = reduceRangeAsyncIoObserver;
				if (asyncIoObserver != null) {
					asyncIoObserver.accept(ro.asyncIo());
				}
				ro.setDeadline(readDeadlineMicros(timeoutMs));
				Buf calculatedStartKey = startKeysInclusive != null && startKeysInclusive.keys().length > 0 ? col.calculateKey(
						startKeysInclusive.keys()) : null;
				Buf calculatedEndKey =
						endKeysExclusive != null && endKeysExclusive.keys().length > 0 ? col.calculateKey(endKeysExclusive.keys())
								: null;
				try (var startKeySlice = calculatedStartKey != null ? toSlice(calculatedStartKey) : null; var endKeySlice =
						calculatedEndKey != null ? toSlice(calculatedEndKey) : null) {
					boolean endpointSeek = requestType instanceof RequestType.RequestGetFirstAndLast<?>;
					if (!endpointSeek) {
						if (startKeySlice != null) {
							ro.setIterateLowerBound(startKeySlice);
						}
						if (endKeySlice != null) {
							ro.setIterateUpperBound(endKeySlice);
						}
					}

					RocksIterator it;
					if (transactionId != 0L) {
						//noinspection resource
						it = getTransaction(transactionId, false).val().getIterator(ro, col.cfh());
					} else {
						it = db.get().newIterator(col.cfh(), ro);
					}
					try (it) {
						notifyRangeIteratorOpened();
						return (T) switch (requestType) {
							case RequestEntriesCount<?> _ -> {
								long count = 0;
								it.seekToFirst();
								while (it.isValid()) {
									if (col.hasBuckets()) {
										count += Bucket.readElementCount(toBuf(it.value()));
									} else {
										count++;
									}
									it.next();
								}
								checkIteratorStatusIfInvalid(it);
								yield count;
							}
							case RequestType.RequestGetFirstAndLast<?> _ -> {
								var first = seekLogicalEndpoint(it,
										col,
										reverse,
										calculatedStartKey,
										calculatedEndKey);
								if (first == null) {
									yield new FirstAndLast<>(null, null);
								}
								var last = Objects.requireNonNull(seekLogicalEndpoint(it,
										col,
										!reverse,
										calculatedStartKey,
										calculatedEndKey));
								yield new FirstAndLast<>(first, last);
							}
						};
					}
				}
			}
		} finally {
			if (col != null) {
				col.endUse();
			}
			ops.endOp();
			var end = System.nanoTime();
			reduceRangeTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private @Nullable KV seekLogicalEndpoint(RocksIterator iterator,
			ColumnInstance column,
			boolean fromEnd,
			@Nullable Buf startKeyInclusive,
			@Nullable Buf endKeyExclusive) {
		byte[] inclusiveStart = startKeyInclusive != null ? startKeyInclusive.toByteArray() : null;
		byte[] exclusiveEnd = endKeyExclusive != null ? endKeyExclusive.toByteArray() : null;
		if (fromEnd) {
			if (exclusiveEnd == null) {
				iterator.seekToLast();
			} else {
				iterator.seek(exclusiveEnd);
				if (iterator.isValid()) {
					iterator.prev();
				} else {
					iterator.seekToLast();
				}
			}
		} else {
			if (inclusiveStart == null) {
				iterator.seekToFirst();
			} else {
				iterator.seek(inclusiveStart);
			}
		}
		while (iterator.isValid()) {
			byte[] rawKey = iterator.key();
			if (inclusiveStart != null && Arrays.compareUnsigned(rawKey, inclusiveStart) < 0) {
				if (fromEnd) {
					return null;
				}
				iterator.next();
				continue;
			}
			if (exclusiveEnd != null && Arrays.compareUnsigned(rawKey, exclusiveEnd) >= 0) {
				if (!fromEnd) {
					return null;
				}
				iterator.prev();
				continue;
			}
			var calculatedKey = toBuf(rawKey);
			var calculatedValue = (column.schema().hasValue() || column.hasBuckets())
					? toBuf(iterator.value())
					: emptyBuf();
			if (!column.hasBuckets()) {
				return decodeKV(column, calculatedKey, calculatedValue);
			}
			var elements = new Bucket(column, calculatedValue).getElements();
			if (!elements.isEmpty()) {
				var entry = fromEnd ? elements.getLast() : elements.getFirst();
				return decodeBucketEntry(column, calculatedKey, entry);
			}
			if (fromEnd) {
				iterator.prev();
			} else {
				iterator.next();
			}
		}
		checkIteratorStatusIfInvalid(iterator);
		return null;
	}

	@Override
	public <T> RangePage<T> getRangePage(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@Nullable Keys resumeAfter,
			@NotNull RequestGetRange<? super KV, T> requestType,
			long timeoutMs,
			@NotNull RangeBudget budget) throws RocksDBException {
		Objects.requireNonNull(requestType, "requestType");
		Objects.requireNonNull(budget, "budget");
		if (budget.maxItems() > RangeBudget.DEFAULT_MAX_ITEMS) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Range maxItems exceeds the server maximum of " + RangeBudget.DEFAULT_MAX_ITEMS);
		}
		if (budget.maxBytes() > RangeBudget.DEFAULT_MAX_BYTES) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Range maxBytes exceeds the server maximum of " + RangeBudget.DEFAULT_MAX_BYTES);
		}

		LongAdder totalTime = new LongAdder();
		long start = System.nanoTime();
		long deadlineMicros = readDeadlineMicros(timeoutMs);
		boolean fillCache = !(requestType instanceof RequestType.RequestGetAllInRangeNoCache<?>);
		actionLogger.logAction("GetRangePage",
				start,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				transactionId,
				budget.maxItems(),
				timeoutMs,
				requestType);

		RangeCursor cursor = openRangeCursor(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				deadlineMicros,
				fillCache,
				false,
				totalTime,
				resumeAfter,
				true);
		try {
			var page = cursor.readPage(totalTime, budget);
			@SuppressWarnings("unchecked")
			var typedItems = (List<T>) (List<?>) page.items();
			return new RangePage<>(typedItems, page.resumeAfter(), page.hasMore());
		} finally {
			var closeStart = System.nanoTime();
			try {
				cursor.close();
			} finally {
				totalTime.add(System.nanoTime() - closeStart);
				getRangeTimer.record(totalTime.sum(), TimeUnit.NANOSECONDS);
			}
		}
	}

	@Override
	public <T> Stream<T> getRange(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@NotNull RequestType.RequestGetRange<? super KV, T> requestType,
			long timeoutMs) throws RocksDBException {
		return Flux
				.from(this.getRangeAsyncInternal(transactionId,
						columnId,
						startKeysInclusive,
						endKeysExclusive,
						reverse,
						requestType,
						timeoutMs
				))
				.toStream();
	}

	/**
	 * See: {@link GetRange}.
	 */
	public <T> Publisher<T> getRangeAsyncInternal(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			RequestGetRange<? super KV, T> requestType,
			long timeoutMs) throws RocksDBException {
		return getRangeAsyncInternal(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				requestType,
				timeoutMs,
				RequestContext.NO_DEADLINE,
				WorkloadProfile.BATCH);
	}

	public <T> Publisher<T> getRangeAsyncInternal(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			RequestGetRange<? super KV, T> requestType,
			long timeoutMs,
			long contextDeadlineEpochMillis,
			@NotNull WorkloadProfile workloadProfile) throws RocksDBException {
		LongAdder totalTime = new LongAdder();
		long start = System.nanoTime();
		var deadline = retainedReadDeadline(timeoutMs, contextDeadlineEpochMillis, "Range timeout");
		var workloadExecutor = scheduler.executor(workloadProfile,
				OperationFamily.RANGE_PAGE,
				deadline.epochMillis());
		boolean fillCache = !(requestType instanceof RequestType.RequestGetAllInRangeNoCache<?>);
		actionLogger.logAction("GetRange (begin)",
				start,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				transactionId,
				null,
				timeoutMs,
				requestType
		); // todo: log if reversed or not
		if (!isCooperativeRetainedProfile(workloadProfile)) {
			return Flux.usingWhen(
					openOrdinaryRetainedRangeCursorAsync(transactionId,
							columnId,
							startKeysInclusive,
							endKeysExclusive,
							reverse,
							deadline.micros(),
							fillCache,
							false,
							totalTime,
							workloadExecutor),
					cursor -> Flux.defer(() -> scheduleOrdinaryRangeStep(workloadExecutor,
								() -> cursor.readChunk(totalTime)))
							.publishOn(reactor.core.scheduler.Schedulers.parallel(), 1)
							.doOnNext(chunk -> {
								var chunkObserver = rangeReadChunkSizeObserver;
								if (chunkObserver != null) {
									chunkObserver.accept(chunk.items().size());
								}
								if (!chunk.exhausted()) {
									notifyRangeContinuation();
								}
							})
							.repeat()
							.takeUntil(RangeReadChunk::exhausted)
							.concatMap(chunk -> Flux.fromIterable(EmbeddedDB.<T>castRangeItems(chunk.items())), 0)
							.takeUntilOther(retainedRangeDeadlineSignal(deadline)),
					cursor -> closeRangeCursor(cursor, totalTime, getRangeTimer),
					(cursor, _) -> closeRangeCursor(cursor, totalTime, getRangeTimer),
					cursor -> closeRangeCursor(cursor, totalTime, getRangeTimer));
		}

		return retainedQueryLimiter.acquire(deadline.micros())
				.flatMapMany(permit -> Flux.<RangeReadChunk>create(sink -> {
					var state = new RetainedRangeReadTask(transactionId,
							columnId,
							startKeysInclusive,
							endKeysExclusive,
							reverse,
							deadline.micros(),
							fillCache,
							totalTime,
							workloadExecutor,
							permit,
							sink);
					sink.onRequest(state);
					sink.onCancel(state::cancelDelivery);
					sink.onDispose(state::cancelDelivery);
					state.start();
				}, FluxSink.OverflowStrategy.ERROR)
						.publishOn(reactor.core.scheduler.Schedulers.parallel(), 1)
						.doOnNext(chunk -> {
							var chunkObserver = rangeReadChunkSizeObserver;
							if (chunkObserver != null) {
								chunkObserver.accept(chunk.items().size());
							}
							if (!chunk.exhausted()) {
								notifyRangeContinuation();
							}
						})
						// A single decoded chunk is in flight. Its items drain before publishOn
						// requests another chunk and resumes the same parked scheduler node.
						.concatMap(chunk -> Flux.fromIterable(EmbeddedDB.<T>castRangeItems(chunk.items())), 0)
						.takeUntilOther(retainedRangeDeadlineSignal(deadline)));
	}

	@SuppressWarnings("unchecked")
	private static <T> List<T> castRangeItems(List<KV> items) {
		return (List<T>) (List<?>) items;
	}

	/** Count logical entries in bounded physical slices without materializing one signal per entry. */
	public Mono<Long> countRangeAsyncInternal(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs) {
		return countRangeAsyncInternal(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				timeoutMs,
				RequestContext.NO_DEADLINE,
				WorkloadProfile.BATCH);
	}

	public Mono<Long> countRangeAsyncInternal(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs,
			long contextDeadlineEpochMillis,
			@NotNull WorkloadProfile workloadProfile) {
		LongAdder totalTime = new LongAdder();
		long start = System.nanoTime();
		var deadline = retainedReadDeadline(timeoutMs, contextDeadlineEpochMillis, "Exact count timeout");
		var workloadExecutor = scheduler.executor(workloadProfile,
				OperationFamily.FULL_SCAN_AGGREGATE,
				deadline.epochMillis());
		actionLogger.logAction("ReduceRange (begin)",
				start,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				transactionId,
				null,
				timeoutMs,
				RequestType.entriesCount()
		); // todo: log if reversed or not
		if (!isCooperativeRetainedProfile(workloadProfile)) {
			return Flux.usingWhen(
					openOrdinaryRetainedRangeCursorAsync(transactionId,
							columnId,
							startKeysInclusive,
							endKeysExclusive,
							reverse,
							deadline.micros(),
							false,
							true,
							totalTime,
							workloadExecutor),
					cursor -> Flux.defer(() -> scheduleOrdinaryRangeStep(workloadExecutor, () -> {
						var chunk = cursor.countChunk(totalTime);
						var chunkObserver = rangeCountChunkObserver;
						if (chunkObserver != null) {
							chunkObserver.run();
						}
						return chunk;
					}))
							.publishOn(reactor.core.scheduler.Schedulers.parallel(), 1)
							.doOnNext(chunk -> {
								var sizeObserver = rangeCountQuantumItemsObserver;
								if (sizeObserver != null) {
									sizeObserver.accept(chunk.count());
								}
								if (!chunk.exhausted()) {
									notifyRangeContinuation();
								}
							})
							.repeat()
							.takeUntil(RangeCountChunk::exhausted)
							.takeUntilOther(retainedRangeDeadlineSignal(deadline)),
					cursor -> closeRangeCursor(cursor, totalTime, reduceRangeTimer),
					(cursor, _) -> closeRangeCursor(cursor, totalTime, reduceRangeTimer),
					cursor -> closeRangeCursor(cursor, totalTime, reduceRangeTimer))
					.reduce(0L, (total, chunk) -> total + chunk.count());
		}

		return retainedQueryLimiter.acquire(deadline.micros())
				.flatMap(permit -> Mono.create(sink -> {
					var state = new RetainedRangeCountTask(transactionId,
							columnId,
							startKeysInclusive,
							endKeysExclusive,
							reverse,
							deadline.micros(),
							totalTime,
							workloadExecutor,
							permit,
							sink);
					sink.onCancel(state::cancelDelivery);
					sink.onDispose(state::cancelDelivery);
					state.start();
				}));
	}

	/** Keep the immutable logical-read deadline active while decoded items await demand. */
	private Mono<Void> retainedRangeDeadlineSignal(ReadDeadline deadline) {
		return Mono.defer(() -> {
			long delayMillis = Math.max(0L, deadline.epochMillis() - System.currentTimeMillis());
			return Mono.delay(Duration.ofMillis(delayMillis))
					.then(Mono.error(retainedRangeDeadlineExceeded()));
		});
	}

	private void notifyRangeContinuation() {
		var observer = rangeContinuationObserver;
		if (observer != null) {
			observer.run();
		}
	}

	/** Preserve bounded ordinary submissions for profiles that cannot execute cooperatively. */
	private Mono<RangeCursor> openOrdinaryRetainedRangeCursorAsync(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long deadlineMicros,
			boolean fillCache,
			boolean retainSnapshot,
			LongAdder totalTime,
			RWScheduler.WorkloadExecutor workloadExecutor) {
		return retainedQueryLimiter.acquire(deadlineMicros)
				.flatMap(permit -> scheduleOrdinaryRangeStep(workloadExecutor,
						() -> permit.open(() -> openRangeCursor(transactionId,
								columnId,
								startKeysInclusive,
								endKeysExclusive,
								reverse,
								deadlineMicros,
								fillCache,
								retainSnapshot,
								totalTime,
								null,
								false,
								permit,
								true)),
						RangeCursor::close)
						.doOnError(_ -> permit.close())
						.doOnCancel(permit::close));
	}

	private Mono<Void> closeRangeCursor(RangeCursor cursor, LongAdder totalTime, Timer timer) {
		return Mono.fromRunnable(() -> {
			var closeStart = System.nanoTime();
			try {
				cursor.close();
			} finally {
				totalTime.add(System.nanoTime() - closeStart);
				timer.record(totalTime.sum(), TimeUnit.NANOSECONDS);
			}
		});
	}

	private <T> Mono<T> scheduleOrdinaryRangeStep(RWScheduler.WorkloadExecutor target,
			Callable<T> callable) {
		return scheduleOrdinaryRangeStep(target, callable, null);
	}

	private <T> Mono<T> scheduleOrdinaryRangeStep(RWScheduler.WorkloadExecutor target,
			Callable<T> callable,
			@Nullable Consumer<? super T> lateSuccessCleanup) {
		return Mono.create(sink -> {
			var task = new OrdinaryRetainedRangeTask<>(target, callable, lateSuccessCleanup, sink);
			sink.onCancel(task::cancelDelivery);
			try {
				target.execute(task);
			} catch (Throwable error) {
				task.reject(error instanceof RuntimeException runtime
						? runtime
						: new RejectedExecutionException("Range quantum submission failed", error));
			}
		});
	}

	private final class OrdinaryRetainedRangeTask<T> extends CompletableFuture<T>
			implements Runnable, RWScheduler.RejectionAwareTask {

		private static final VarHandle STATE =
				varHandle(OrdinaryRetainedRangeTask.class, "state", int.class);
		private static final int QUEUED = 0;
		private static final int RUNNING = 1;
		private static final int FINISHED = 2;
		private static final int CANCELLED = 3;

		private final RWScheduler.WorkloadExecutor target;
		private final Callable<T> callable;
		private final @Nullable Consumer<? super T> lateSuccessCleanup;
		private final reactor.core.publisher.MonoSink<T> sink;
		private volatile int state = QUEUED;
		private volatile boolean deliveryCancelled;

		private OrdinaryRetainedRangeTask(RWScheduler.WorkloadExecutor target,
				Callable<T> callable,
				@Nullable Consumer<? super T> lateSuccessCleanup,
				reactor.core.publisher.MonoSink<T> sink) {
			this.target = target;
			this.callable = callable;
			this.lateSuccessCleanup = lateSuccessCleanup;
			this.sink = sink;
		}

		private void cancelDelivery() {
			deliveryCancelled = true;
			if (STATE.compareAndSet(this, QUEUED, CANCELLED)) {
				super.cancel(false);
				scheduler.removeQueuedTask(target, this);
			}
		}

		@Override
		public void run() {
			if (!STATE.compareAndSet(this, QUEUED, RUNNING)) {
				return;
			}
			try {
				var result = callable.call();
				complete(result);
				if (deliveryCancelled) {
					cleanupLateSuccess(result);
				} else {
					sink.success(result);
				}
			} catch (Throwable error) {
				completeExceptionally(error);
				if (!deliveryCancelled) {
					sink.error(error);
				} else {
					logger.debug("Range quantum failed after subscriber cancellation", error);
				}
			} finally {
				state = FINISHED;
			}
		}

		private void cleanupLateSuccess(T result) {
			if (lateSuccessCleanup == null) {
				return;
			}
			try {
				lateSuccessCleanup.accept(result);
			} catch (Throwable cleanupError) {
				logger.warn("Failed to clean a retained range result after cancellation", cleanupError);
			}
		}

		@Override
		public void reject(RuntimeException failure) {
			if (!STATE.compareAndSet(this, QUEUED, FINISHED)) {
				return;
			}
			completeExceptionally(failure);
			if (!deliveryCancelled) {
				sink.error(failure);
			}
		}
	}

	private RangeCursor openRangeCursor(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long deadlineMicros,
			boolean fillCache,
			boolean retainSnapshot,
			LongAdder totalTime) {
		return openRangeCursor(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				deadlineMicros,
				fillCache,
				retainSnapshot,
				totalTime,
				null,
				false,
				null,
				true);
	}

	private RangeCursor openRangeCursor(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long deadlineMicros,
			boolean fillCache,
			boolean retainSnapshot,
			LongAdder totalTime,
			@Nullable Keys resumeAfter,
			boolean deterministicBucketOrder) {
		return openRangeCursor(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				deadlineMicros,
				fillCache,
				retainSnapshot,
				totalTime,
				resumeAfter,
				deterministicBucketOrder,
				null,
				true);
	}

	private RangeCursor openRangeCursor(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long deadlineMicros,
			boolean fillCache,
			boolean retainSnapshot,
			LongAdder totalTime,
			@Nullable Keys resumeAfter,
			boolean deterministicBucketOrder,
			@Nullable RetainedQueryPermit retainedPermit,
			boolean registerActiveResource) {
		ops.beginOp();
		var initializationStart = System.nanoTime();
		ColumnInstance.ColumnUse columnUse = null;
		try {
			columnUse = acquireColumnUse(columnId);
			var col = columnUse.column();
			Buf calculatedStartKey = startKeysInclusive != null && startKeysInclusive.keys().length > 0
					? col.calculateKey(startKeysInclusive.keys())
					: null;
			Buf calculatedEndKey = endKeysExclusive != null && endKeysExclusive.keys().length > 0
					? col.calculateKey(endKeysExclusive.keys())
					: null;
			Buf calculatedResumeKey = resumeAfter != null
					? col.calculateKey(resumeAfter.keys())
					: null;
			validateResumeBounds(calculatedStartKey, calculatedEndKey, calculatedResumeKey);
			var cursor = new RangeCursor(transactionId,
					columnUse,
					calculatedStartKey,
					calculatedEndKey,
					reverse,
					deadlineMicros,
					fillCache,
					retainSnapshot,
					calculatedResumeKey,
					resumeAfter,
					deterministicBucketOrder,
					retainedPermit);
			try {
				if (registerActiveResource) {
					activeRangeResources.add(cursor);
				}
				return cursor;
			} catch (Throwable error) {
				cursor.close();
				throw error;
			}
		} catch (Throwable error) {
			if (columnUse != null) {
				columnUse.close();
			}
			throw error;
		} finally {
			totalTime.add(System.nanoTime() - initializationStart);
			ops.endOp();
		}
	}

	private static void validateResumeBounds(@Nullable Buf startKey,
			@Nullable Buf endKey,
			@Nullable Buf resumeKey) {
		if (resumeKey == null) {
			return;
		}
		var resumeBytes = resumeKey.toByteArray();
		if (startKey != null && Arrays.compareUnsigned(resumeBytes, startKey.toByteArray()) < 0) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"resumeAfter is before the original inclusive range bound");
		}
		if (endKey != null && Arrays.compareUnsigned(resumeBytes, endKey.toByteArray()) >= 0) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"resumeAfter is at or after the original exclusive range bound");
		}
	}

	private abstract class RetainedRangeCooperativeTask
			implements RWScheduler.CooperativeCompletionTask,
			ActiveRangeResource,
			LongConsumer,
			reactor.core.Disposable {

		private static final VarHandle TERMINATED =
				varHandle(RetainedRangeCooperativeTask.class, "terminated", boolean.class);
		private final long transactionId;
		private final long columnId;
		private final @Nullable Keys startKeysInclusive;
		private final @Nullable Keys endKeysExclusive;
		private final boolean reverse;
		private final long deadlineMicros;
		private final boolean fillCache;
		private final boolean retainSnapshot;
		protected final LongAdder totalTime;
		private final RWScheduler.WorkloadExecutor workloadExecutor;
		private final RetainedQueryPermit retainedPermit;
		private final Timer timer;
		private final Object admissionLock = new Object();
		private final Object resourceLock = new Object();
		private volatile boolean terminated;
		private final CountDownLatch cleanupFinished = new CountDownLatch(1);
		private volatile boolean deliveryCancelled;
		protected volatile @Nullable RWScheduler.CooperativeHandle handle;
		private @Nullable RangeCursor cursor;
		private boolean completionPrepared;
		private boolean cleanupStarted;
		private @Nullable Throwable cleanupFailure;

		private RetainedRangeCooperativeTask(long transactionId,
				long columnId,
				@Nullable Keys startKeysInclusive,
				@Nullable Keys endKeysExclusive,
				boolean reverse,
				long deadlineMicros,
				boolean fillCache,
				boolean retainSnapshot,
				LongAdder totalTime,
				RWScheduler.WorkloadExecutor workloadExecutor,
				RetainedQueryPermit retainedPermit,
				Timer timer) {
			this.transactionId = transactionId;
			this.columnId = columnId;
			this.startKeysInclusive = startKeysInclusive;
			this.endKeysExclusive = endKeysExclusive;
			this.reverse = reverse;
			this.deadlineMicros = deadlineMicros;
			this.fillCache = fillCache;
			this.retainSnapshot = retainSnapshot;
			this.totalTime = totalTime;
			this.workloadExecutor = workloadExecutor;
			this.retainedPermit = retainedPermit;
			this.timer = timer;
		}

		protected final void start() {
			if (terminated) {
				return;
			}
			activeRangeResources.add(this);
			synchronized (admissionLock) {
				if (terminated) {
					activeRangeResources.remove(this);
					return;
				}
				try {
					handle = workloadExecutor.executeCooperatively(this,
							workloadSettings.rangeQuantumMaxBytes());
				} catch (RuntimeException admissionFailure) {
					reject(admissionFailure);
				}
			}
		}

		protected final @Nullable RangeCursor cursor(RWScheduler.CooperativeContext context) {
			// The worker can dispatch immediately after admission. Waiting for this
			// short monitor handoff guarantees that the stable handle is installed first.
			synchronized (admissionLock) {
				if (terminated) {
					return null;
				}
			}
			if (context.terminationRequested()) {
				return null;
			}
			synchronized (resourceLock) {
				if (cursor != null) {
					return cursor;
				}
				RangeCursor created = retainedPermit.open(() -> openRangeCursor(transactionId,
						columnId,
						startKeysInclusive,
						endKeysExclusive,
						reverse,
						deadlineMicros,
						fillCache,
						retainSnapshot,
						totalTime,
						null,
						false,
						null,
						false));
				if (terminated) {
					created.close();
					return null;
				}
				cursor = created;
				return created;
			}
		}

		protected final boolean schedulerTerminationRequested(RWScheduler.CooperativeContext context) {
			return context.terminationRequested();
		}

		protected final void cancelDelivery() {
			if (terminated) {
				return;
			}
			deliveryCancelled = true;
			if (deadlineMicros != Long.MAX_VALUE
					&& TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()) >= deadlineMicros) {
				var failure = retainedRangeDeadlineExceeded();
				cancelAndFinish(failure, false, RWScheduler.TerminalOutcome.DEADLINE);
			} else {
				cancelAndFinish(null, false, null);
			}
		}

		@Override
		public final void dispose() {
			cancelDelivery();
		}

		@Override
		public final boolean isDisposed() {
			return terminated;
		}

		@Override
		public final void close() {
			var failure = new RejectedExecutionException("Database is shutting down");
			cancelAndFinish(failure, !deliveryCancelled, RWScheduler.TerminalOutcome.SHUTDOWN);
			awaitCleanupFinished();
		}

		@Override
		public final void expireIfDeadlinePassed(long nowMicros) {
			if (deadlineMicros == Long.MAX_VALUE || nowMicros < deadlineMicros || terminated) {
				return;
			}
			var failure = retainedRangeDeadlineExceeded();
			cancelAndFinish(failure, !deliveryCancelled, RWScheduler.TerminalOutcome.DEADLINE);
		}

		private void cancelAndFinish(@Nullable RuntimeException failure,
				boolean signalTerminal,
				@Nullable RWScheduler.TerminalOutcome outcome) {
			// Serialize cancellation with admission. Otherwise cancellation can observe no
			// handle, release the permit, and then race a late cooperative submission.
			synchronized (admissionLock) {
				if (terminated) {
					return;
				}
				var currentHandle = handle;
				if (currentHandle != null) {
					if (outcome != null && currentHandle instanceof CooperativeTerminationHandle classifiedHandle) {
						classifiedHandle.terminate(outcome, Objects.requireNonNull(failure));
					} else {
						currentHandle.dispose();
					}
					// A queued or parked task is rejected synchronously by the scheduler. An active
					// task is rejected only after its current quantum returns, which keeps cursor
					// cleanup from racing an in-flight JNI call.
					return;
				}
				finish(failure, signalTerminal);
			}
		}

		@Override
		public final void reject(RuntimeException failure) {
			// The scheduler owns first-cause arbitration for admitted tasks. A later
			// deadline, shutdown, or cancellation request must not change the public
			// failure after the scheduler has already selected a terminal outcome.
			finish(failure, !deliveryCancelled);
		}

		protected final void prepareCompletion(RWScheduler.CooperativeContext context) {
			var failure = cleanupResources(null);
			if (failure != null) {
				if (failure instanceof VirtualMachineError fatal) {
					throw fatal;
				}
				context.fail(cooperativeFailure(failure,
						"Cooperative retained range cleanup failed"));
				return;
			}
			synchronized (admissionLock) {
				if (terminated) {
					return;
				}
				if (completionPrepared) {
					throw new IllegalStateException("Retained range completion was prepared twice");
				}
				completionPrepared = true;
			}
		}

		@Override
		public final void completeCooperatively() {
			synchronized (admissionLock) {
				if (terminated) {
					return;
				}
				if (!completionPrepared) {
					throw new IllegalStateException("Scheduler selected RUN without a retained range result");
				}
			}
			if (!TERMINATED.compareAndSet(this, false, true)) {
				return;
			}
			signalTerminal(null);
		}

		protected final void finish(@Nullable Throwable originalFailure, boolean signalTerminal) {
			if (!TERMINATED.compareAndSet(this, false, true)) {
				return;
			}
			var failure = cleanupResources(originalFailure);
			if (signalTerminal) {
				signalTerminal(failure);
			}
		}

		private @Nullable Throwable cleanupResources(@Nullable Throwable originalFailure) {
			if (cleanupStarted) {
				awaitCleanupFinished();
				var existingCleanupFailure = cleanupFailure;
				return existingCleanupFailure == null
						? originalFailure
						: appendFailure(originalFailure, existingCleanupFailure);
			}
			cleanupStarted = true;
			Throwable currentCleanupFailure = null;
			try {
				var cleanupObserver = retainedRangeCleanupObserver;
				if (cleanupObserver != null) {
					try {
						cleanupObserver.run();
					} catch (Throwable observerFailure) {
						currentCleanupFailure = appendFailure(currentCleanupFailure, observerFailure);
					}
				}
				RangeCursor localCursor;
				synchronized (resourceLock) {
					localCursor = cursor;
					cursor = null;
				}
				var closeStart = System.nanoTime();
				try {
					if (localCursor != null) {
						localCursor.close();
					}
				} catch (Throwable closeError) {
					currentCleanupFailure = appendFailure(currentCleanupFailure, closeError);
				} finally {
					totalTime.add(System.nanoTime() - closeStart);
					try {
						retainedPermit.close();
					} catch (Throwable closeError) {
						currentCleanupFailure = appendFailure(currentCleanupFailure, closeError);
					}
					activeRangeResources.remove(this);
					try {
						timer.record(totalTime.sum(), TimeUnit.NANOSECONDS);
					} catch (Throwable timerFailure) {
						currentCleanupFailure = appendFailure(currentCleanupFailure, timerFailure);
					}
				}
			} finally {
				cleanupFailure = currentCleanupFailure;
				cleanupFinished.countDown();
			}
			return currentCleanupFailure == null
					? originalFailure
					: appendFailure(originalFailure, currentCleanupFailure);
		}

		private void awaitCleanupFinished() {
			boolean interrupted = false;
			while (true) {
				try {
					cleanupFinished.await();
					break;
				} catch (InterruptedException ignored) {
					interrupted = true;
				}
			}
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}

		protected abstract void signalTerminal(@Nullable Throwable failure);

		protected final boolean isTerminated() {
			return terminated;
		}

		private boolean hasOpenCursor() {
			synchronized (resourceLock) {
				return cursor != null;
			}
		}

		@Override
		public abstract void accept(long requested);
	}

	private final class RetainedRangeReadTask extends RetainedRangeCooperativeTask {

		private static final VarHandle DEMAND =
				varHandle(RetainedRangeReadTask.class, "demand", long.class);
		private final FluxSink<RangeReadChunk> sink;
		private volatile long demand;

		private RetainedRangeReadTask(long transactionId,
				long columnId,
				@Nullable Keys startKeysInclusive,
				@Nullable Keys endKeysExclusive,
				boolean reverse,
				long deadlineMicros,
				boolean fillCache,
				LongAdder totalTime,
				RWScheduler.WorkloadExecutor workloadExecutor,
				RetainedQueryPermit retainedPermit,
				FluxSink<RangeReadChunk> sink) {
			super(transactionId,
					columnId,
					startKeysInclusive,
					endKeysExclusive,
					reverse,
					deadlineMicros,
					fillCache,
					false,
					totalTime,
					workloadExecutor,
					retainedPermit,
					getRangeTimer);
			this.sink = sink;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			if (isTerminated()) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			if (schedulerTerminationRequested(context)) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			if (demand == 0L) {
				return RWScheduler.CooperativeResult.PARK;
			}
			try {
				var activeCursor = cursor(context);
				if (activeCursor == null) {
					return RWScheduler.CooperativeResult.COMPLETE;
				}
				var chunk = activeCursor.readChunk(totalTime);
				if (isTerminated() || schedulerTerminationRequested(context)) {
					return RWScheduler.CooperativeResult.COMPLETE;
				}
				producedOne();
				sink.next(chunk);
				if (chunk.exhausted()) {
					prepareCompletion(context);
					return RWScheduler.CooperativeResult.COMPLETE;
				}
				if (schedulerTerminationRequested(context)) {
					return RWScheduler.CooperativeResult.COMPLETE;
				}
				// Backpressure is stronger than scheduler competition here. If delivery has
				// not requested another decoded chunk, queueing this node for a competitive
				// redispatch only makes it run once more to discover zero demand and park.
				// A concurrent request records resume while this quantum is active, so the
				// scheduler still requeues the node atomically when PARK is committed.
				return demand == 0L
						? RWScheduler.CooperativeResult.PARK
						: RWScheduler.CooperativeResult.YIELD;
			} catch (VirtualMachineError fatal) {
				throw fatal;
			} catch (Throwable failure) {
				context.fail(cooperativeFailure(failure,
						"Cooperative retained range read failed"));
				return RWScheduler.CooperativeResult.COMPLETE;
			}
		}

		private void producedOne() {
			while (true) {
				long current = demand;
				if (current == Long.MAX_VALUE) {
					return;
				}
				if (current <= 0L) {
					throw new IllegalStateException("Range chunk produced without downstream demand");
				}
				if (DEMAND.compareAndSet(this, current, current - 1L)) {
					return;
				}
			}
		}

		@Override
		public void accept(long requested) {
			if (requested <= 0L || isTerminated()) {
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
		protected void signalTerminal(@Nullable Throwable failure) {
			if (failure == null) {
				sink.complete();
			} else {
				sink.error(failure);
			}
		}
	}

	private final class RetainedRangeCountTask extends RetainedRangeCooperativeTask {

		private final reactor.core.publisher.MonoSink<Long> sink;
		private long count;

		private RetainedRangeCountTask(long transactionId,
				long columnId,
				@Nullable Keys startKeysInclusive,
				@Nullable Keys endKeysExclusive,
				boolean reverse,
				long deadlineMicros,
				LongAdder totalTime,
				RWScheduler.WorkloadExecutor workloadExecutor,
				RetainedQueryPermit retainedPermit,
				reactor.core.publisher.MonoSink<Long> sink) {
			super(transactionId,
					columnId,
					startKeysInclusive,
					endKeysExclusive,
					reverse,
					deadlineMicros,
					false,
					true,
					totalTime,
					workloadExecutor,
					retainedPermit,
					reduceRangeTimer);
			this.sink = sink;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			if (isTerminated()) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			if (schedulerTerminationRequested(context)) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			try {
				var activeCursor = cursor(context);
				if (activeCursor == null) {
					return RWScheduler.CooperativeResult.COMPLETE;
				}
				while (true) {
					var chunk = activeCursor.countChunk(totalTime);
					count += chunk.count();
					var chunkObserver = rangeCountChunkObserver;
					if (chunkObserver != null) {
						chunkObserver.run();
					}
					var sizeObserver = rangeCountQuantumItemsObserver;
					if (sizeObserver != null) {
						sizeObserver.accept(chunk.count());
					}
					if (isTerminated() || schedulerTerminationRequested(context)) {
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					if (chunk.exhausted()) {
						prepareCompletion(context);
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					if (notifyRangeContinuationAndPark()) {
						return RWScheduler.CooperativeResult.PARK;
					}
					if (context.preemptionRequested()) {
						return RWScheduler.CooperativeResult.YIELD;
					}
				}
			} catch (VirtualMachineError fatal) {
				throw fatal;
			} catch (Throwable failure) {
				context.fail(cooperativeFailure(failure,
						"Cooperative retained range count failed"));
				return RWScheduler.CooperativeResult.COMPLETE;
			}
		}

		private boolean notifyRangeContinuationAndPark() {
			var observer = rangeContinuationObserver;
			if (observer == null) {
				return false;
			}
			try {
				reactor.core.scheduler.Schedulers.parallel().schedule(() -> {
					try {
						observer.run();
					} finally {
						var currentHandle = handle;
						if (currentHandle != null) {
							currentHandle.resume();
						}
					}
				});
				return true;
			} catch (RuntimeException schedulingFailure) {
				throw new RejectedExecutionException("Failed to notify a retained count continuation",
						schedulingFailure);
			}
		}

		@Override
		public void accept(long requested) {
			// Mono demand is terminal-result demand, not a native-count quantum budget.
		}

		@Override
		protected void signalTerminal(@Nullable Throwable failure) {
			if (failure == null) {
				sink.success(count);
			} else {
				sink.error(failure);
			}
		}
	}

	/** FIFO admission for logical reads that retain a native point-in-time view between quanta. */
	private final class RetainedQueryLimiter implements AutoCloseable {

		private static final int WAITING = 0;
		private static final int GRANTED = 1;
		private static final int TERMINAL = 2;

		private final int capacity;
		private final ScheduledExecutorService deadlineScheduler;
		private final ArrayDeque<RetainedQueryWaiter> waiters = new ArrayDeque<>();
		private final Set<RetainedQueryPermit> activePermits = new HashSet<>();
		private long nextTicket;
		private boolean closed;

		private RetainedQueryLimiter(int capacity, ScheduledExecutorService deadlineScheduler) {
			this.capacity = capacity;
			this.deadlineScheduler = deadlineScheduler;
		}

		private Mono<RetainedQueryPermit> acquire(long deadlineMicros) {
			return Mono.create(sink -> {
				final RetainedQueryWaiter waiter;
				final Runnable action;
				final boolean queued;
				synchronized (this) {
					waiter = new RetainedQueryWaiter(nextTicket++, deadlineMicros, sink);
					sink.onCancel(waiter::cancel);
					long nowMicros = currentEpochMicros();
					if (closed) {
						waiter.state = TERMINAL;
						action = () -> sink.error(new RejectedExecutionException("Database is shutting down"));
						queued = false;
					} else if (nowMicros >= deadlineMicros) {
						waiter.state = TERMINAL;
						action = () -> sink.error(retainedRangeDeadlineExceeded());
						queued = false;
					} else if (activePermits.size() < capacity && waiters.isEmpty()) {
						action = grantUnsafe(waiter);
						queued = false;
					} else {
						waiters.addLast(waiter);
						action = null;
						queued = true;
					}
				}
				if (queued) {
					waiter.scheduleExpiration();
				} else if (action != null) {
					action.run();
				}
			});
		}

		private Runnable grantUnsafe(RetainedQueryWaiter waiter) {
			var permit = new RetainedQueryPermit(this, waiter.ticket);
			waiter.state = GRANTED;
			waiter.permit = permit;
			activePermits.add(permit);
			return waiter::deliverGrant;
		}

		private void cancel(RetainedQueryWaiter waiter) {
			RetainedQueryPermit permit = null;
			synchronized (this) {
				if (waiter.state == WAITING) {
					waiters.remove(waiter);
					waiter.state = TERMINAL;
				} else if (waiter.state == GRANTED) {
					waiter.state = TERMINAL;
					permit = waiter.permit;
				}
			}
			waiter.cancelExpiration();
			if (permit != null) {
				permit.close();
			}
		}

		private void expire(RetainedQueryWaiter waiter) {
			boolean expired = false;
			synchronized (this) {
				if (waiter.state == WAITING && waiters.remove(waiter)) {
					waiter.state = TERMINAL;
					expired = true;
				}
			}
			if (expired) {
				waiter.sink.error(retainedRangeDeadlineExceeded());
			}
		}

		private void release(RetainedQueryPermit permit) {
			var actions = new ArrayList<Runnable>();
			synchronized (this) {
				if (!activePermits.remove(permit) || closed) {
					return;
				}
				long nowMicros = currentEpochMicros();
				while (activePermits.size() < capacity && !waiters.isEmpty()) {
					var waiter = waiters.removeFirst();
					if (waiter.state != WAITING) {
						continue;
					}
					if (nowMicros >= waiter.deadlineMicros) {
						waiter.state = TERMINAL;
						actions.add(() -> waiter.sink.error(retainedRangeDeadlineExceeded()));
					} else {
						actions.add(grantUnsafe(waiter));
					}
				}
			}
			if (!actions.isEmpty()) {
				try {
					deadlineScheduler.execute(() -> actions.forEach(Runnable::run));
				} catch (RejectedExecutionException shutdown) {
					actions.forEach(Runnable::run);
				}
			}
		}

		private synchronized int activeCount() {
			return activePermits.size();
		}

		private synchronized int waitingCount() {
			return waiters.size();
		}

		@Override
		public void close() {
			final List<RetainedQueryWaiter> pending;
			final List<RetainedQueryPermit> permits;
			synchronized (this) {
				if (closed) {
					return;
				}
				closed = true;
				pending = new ArrayList<>(waiters);
				waiters.clear();
				for (var waiter : pending) {
					waiter.state = TERMINAL;
				}
				permits = new ArrayList<>(activePermits);
			}
			for (var waiter : pending) {
				waiter.cancelExpiration();
				waiter.sink.error(new RejectedExecutionException("Database is shutting down"));
			}
			permits.forEach(RetainedQueryPermit::close);
		}

		private final class RetainedQueryWaiter {

			private final long ticket;
			private final long deadlineMicros;
			private final reactor.core.publisher.MonoSink<RetainedQueryPermit> sink;
			private int state = WAITING;
			private @Nullable RetainedQueryPermit permit;
			private @Nullable ScheduledFuture<?> expiration;

			private RetainedQueryWaiter(long ticket,
					long deadlineMicros,
					reactor.core.publisher.MonoSink<RetainedQueryPermit> sink) {
				this.ticket = ticket;
				this.deadlineMicros = deadlineMicros;
				this.sink = sink;
			}

			private void scheduleExpiration() {
				long delayMicros = Math.max(0L, deadlineMicros - currentEpochMicros());
				final ScheduledFuture<?> scheduled;
				try {
					scheduled = deadlineScheduler.schedule(this::expire, delayMicros, TimeUnit.MICROSECONDS);
				} catch (RejectedExecutionException shutdown) {
					expire();
					return;
				}
				synchronized (RetainedQueryLimiter.this) {
					if (state == WAITING) {
						expiration = scheduled;
					} else {
						scheduled.cancel(false);
					}
				}
			}

			private void deliverGrant() {
				cancelExpiration();
				var granted = Objects.requireNonNull(permit);
				try {
					var observer = retainedQueryPermitGrantedObserver;
					if (observer != null) {
						observer.accept(ticket);
					}
					sink.success(granted);
				} catch (Throwable error) {
					granted.close();
					sink.error(error);
				}
			}

			private void cancel() {
				RetainedQueryLimiter.this.cancel(this);
			}

			private void expire() {
				RetainedQueryLimiter.this.expire(this);
			}

			private void cancelExpiration() {
				var scheduled = expiration;
				if (scheduled != null) {
					scheduled.cancel(false);
				}
			}
		}
	}

	private static final class RetainedQueryPermit implements AutoCloseable {

		private final RetainedQueryLimiter owner;
		private final long ticket;
		private boolean closed;

		private RetainedQueryPermit(RetainedQueryLimiter owner, long ticket) {
			this.owner = owner;
			this.ticket = ticket;
		}

		private synchronized <T> T open(Supplier<T> resourceFactory) {
			if (closed) {
				throw new CancellationException("Retained range permit " + ticket + " was cancelled");
			}
			return resourceFactory.get();
		}

		@Override
		public void close() {
			boolean release;
			synchronized (this) {
				release = !closed;
				closed = true;
			}
			if (release) {
				owner.release(this);
			}
		}
	}

	private static long currentEpochMicros() {
		return TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
	}

	private static RocksDBException retainedRangeDeadlineExceeded() {
		return RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED, "Deadline exceeded");
	}

	private interface ActiveRangeResource {

		void close();

		void expireIfDeadlinePassed(long nowMicros);
	}

	private record RangeCountChunk(long count, boolean exhausted) {
	}

	private record RangeReadChunk(List<KV> items, boolean exhausted) {
	}

	private record ReadDeadline(long epochMillis, long micros) {
	}

	private final class RangeCursor implements ActiveRangeResource {

		private final long transactionId;
		private final ColumnInstance.ColumnUse columnUse;
		private final ColumnInstance col;
		private final boolean reverse;
		private final long deadlineMicros;
		private final boolean fillCache;
		private final boolean deterministicBucketOrder;
		private final AbstractSlice<?> startKeySlice;
		private final AbstractSlice<?> endKeySlice;
		private final @Nullable Snapshot snapshot;
		private final @Nullable RetainedQueryPermit retainedPermit;
		private final ReadOptions readOptions;
		private final RocksIterator iterator;
		private java.util.ListIterator<Entry<Buf[], Buf>> bucketIterator;
		private @Nullable KV pendingRangeItem;
		private long pendingCountItems;
		private long pendingCountBytes;
		private boolean exhausted;
		private boolean expired;
		private boolean closed;

		private RangeCursor(long transactionId,
				ColumnInstance.ColumnUse columnUse,
				@Nullable Buf startKey,
				@Nullable Buf endKey,
				boolean reverse,
				long deadlineMicros,
				boolean fillCache,
				boolean retainSnapshot,
				@Nullable Buf resumeKey,
				@Nullable Keys resumeAfter,
				boolean deterministicBucketOrder,
				@Nullable RetainedQueryPermit retainedPermit) {
			this.transactionId = transactionId;
			this.columnUse = columnUse;
			this.col = columnUse.column();
			this.reverse = reverse;
			this.deadlineMicros = deadlineMicros;
			this.fillCache = fillCache;
			this.deterministicBucketOrder = deterministicBucketOrder;
			this.retainedPermit = retainedPermit;

			AbstractSlice<?> createdStartKeySlice = null;
			AbstractSlice<?> createdEndKeySlice = null;
			ReadOptions createdReadOptions = null;
			Snapshot createdSnapshot = null;
			RocksIterator createdIterator = null;
			try {
				createdStartKeySlice = startKey != null ? toSlice(startKey) : null;
				createdEndKeySlice = endKey != null ? toSlice(endKey) : null;
				createdReadOptions = createReadOptions(createdStartKeySlice,
						createdEndKeySlice);
				if (transactionId != 0L) {
					var transactionSnapshot = getTransaction(transactionId, false).val().getSnapshot();
					if (transactionSnapshot != null) {
						createdReadOptions.setSnapshot(transactionSnapshot);
					}
				}
				if (retainSnapshot && transactionId == 0L) {
					createdSnapshot = db.get().getSnapshot();
					retainedRangeSnapshots.incrementAndGet();
					createdReadOptions.setSnapshot(createdSnapshot);
				}
				createdIterator = createIterator(createdReadOptions);
				positionIterator(createdIterator, resumeKey, resumeAfter);
			} catch (Throwable error) {
				if (createdIterator != null) {
					createdIterator.close();
				}
				if (createdReadOptions != null) {
					createdReadOptions.close();
				}
				if (createdSnapshot != null) {
					try {
						db.get().releaseSnapshot(createdSnapshot);
					} finally {
						retainedRangeSnapshots.decrementAndGet();
					}
				}
				if (createdEndKeySlice != null) {
					createdEndKeySlice.close();
				}
				if (createdStartKeySlice != null) {
					createdStartKeySlice.close();
				}
				throw error;
			}
			this.startKeySlice = createdStartKeySlice;
			this.endKeySlice = createdEndKeySlice;
			this.snapshot = createdSnapshot;
			this.readOptions = createdReadOptions;
			this.iterator = createdIterator;
		}

		private void positionIterator(RocksIterator createdIterator,
				@Nullable Buf resumeKey,
				@Nullable Keys resumeAfter) {
			if (resumeKey == null) {
				if (reverse) {
					createdIterator.seekToLast();
				} else {
					createdIterator.seekToFirst();
				}
				return;
			}

			byte[] resumeBytes = resumeKey.toByteArray();
			if (reverse) {
				createdIterator.seekForPrev(resumeBytes);
			} else {
				createdIterator.seek(resumeBytes);
			}
			if (!createdIterator.isValid()
					|| Arrays.compareUnsigned(createdIterator.key(), resumeBytes) != 0) {
				return;
			}
			if (!col.hasBuckets()) {
				advanceIterator(createdIterator);
				return;
			}

			var elements = orderedBucketElements(createdIterator);
			var resumeVariableKeys = col.getBucketElementKeys(Objects.requireNonNull(resumeAfter).keys());
			int insertionPoint = 0;
			while (insertionPoint < elements.size()
					&& compareKeys(elements.get(insertionPoint).getKey(), resumeVariableKeys) < 0) {
				insertionPoint++;
			}
			while (insertionPoint < elements.size()
					&& compareKeys(elements.get(insertionPoint).getKey(), resumeVariableKeys) == 0) {
				insertionPoint++;
			}
			if (reverse) {
				int beforeResume = insertionPoint;
				while (beforeResume > 0
						&& compareKeys(elements.get(beforeResume - 1).getKey(), resumeVariableKeys) >= 0) {
					beforeResume--;
				}
				if (beforeResume == 0) {
					advanceIterator(createdIterator);
				} else {
					bucketIterator = elements.listIterator(beforeResume);
				}
			} else if (insertionPoint == elements.size()) {
				advanceIterator(createdIterator);
			} else {
				bucketIterator = elements.listIterator(insertionPoint);
			}
		}

		private void advanceIterator(RocksIterator target) {
			if (reverse) {
				target.prev();
			} else {
				target.next();
			}
		}

		private ArrayList<Entry<Buf[], Buf>> orderedBucketElements(RocksIterator target) {
			var elements = new ArrayList<>(new Bucket(col, toBuf(target.value())).getElements());
			if (deterministicBucketOrder) {
				elements.sort((left, right) -> compareKeys(left.getKey(), right.getKey()));
			}
			return elements;
		}

		private int compareKeys(Buf[] left, Buf[] right) {
			for (int i = 0; i < left.length; i++) {
				int compared = Arrays.compareUnsigned(left[i].toByteArray(), right[i].toByteArray());
				if (compared != 0) {
					return compared;
				}
			}
			return Integer.compare(left.length, right.length);
		}

		private ReadOptions createReadOptions(@Nullable AbstractSlice<?> lowerBound,
				@Nullable AbstractSlice<?> upperBound) {
			return newRangeReadOptions(deadlineMicros, fillCache, lowerBound, upperBound);
		}

		private RocksIterator createIterator(ReadOptions options) {
			var created = transactionId != 0L
					? getTransaction(transactionId, false).val().getIterator(options, col.cfh())
					: db.get().newIterator(col.cfh(), options);
			try {
				notifyRangeIteratorOpened();
				return created;
			} catch (Throwable error) {
				created.close();
				throw error;
			}
		}

		private RangePage<KV> readPage(LongAdder totalTime, RangeBudget budget) {
			ops.beginOp();
			var sliceStart = System.nanoTime();
			try {
				synchronized (this) {
					if (expired) {
						throw rangeDeadlineExceeded();
					}
					if (closed || exhausted) {
						return RangePage.empty();
					}

					var results = new ArrayList<KV>(budget.maxItems());
					long encodedBytes = 0L;
					boolean hasMore = false;
					while (results.size() < budget.maxItems()) {
						var next = nextPageItem();
						if (next == null) {
							break;
						}
						long nextBytes = decodedKVBytes(next);
						if (nextBytes > budget.maxBytes() - encodedBytes) {
							if (results.isEmpty()) {
								throw RocksDBException.of(RocksDBErrorType.RANGE_ITEM_TOO_LARGE,
										"Range item requires " + nextBytes
												+ " encoded key/value bytes, exceeding page budget "
												+ budget.maxBytes());
							}
							hasMore = true;
							break;
						}
						results.add(next);
						encodedBytes += nextBytes;
					}
					if (!hasMore && results.size() == budget.maxItems()) {
						hasMore = nextPageItem() != null;
					}
					if (results.isEmpty()) {
						return RangePage.empty();
					}
					return new RangePage<>(results, results.getLast().keys(), hasMore);
				}
			} finally {
				totalTime.add(System.nanoTime() - sliceStart);
				ops.endOp();
			}
		}

		private @Nullable KV nextPageItem() {
			while (!exhausted) {
				if (deadlineMicros != Long.MAX_VALUE
						&& TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()) >= deadlineMicros) {
					throw rangeDeadlineExceeded();
				}
				if (bucketIterator != null) {
					if (reverse ? bucketIterator.hasPrevious() : bucketIterator.hasNext()) {
						var entry = reverse ? bucketIterator.previous() : bucketIterator.next();
						var result = decodeBucketEntry(col, toBuf(iterator.key()), entry);
						boolean bucketHasMore = reverse
								? bucketIterator.hasPrevious()
								: bucketIterator.hasNext();
						if (!bucketHasMore) {
							bucketIterator = null;
							advanceIterator();
						}
						return Objects.requireNonNull(result);
					}
					bucketIterator = null;
					advanceIterator();
					continue;
				}

				if (!iterator.isValid()) {
					checkIteratorStatusIfInvalid(iterator);
					exhausted = true;
					return null;
				}
				if (col.hasBuckets()) {
					var elements = orderedBucketElements(iterator);
					bucketIterator = elements.listIterator(reverse ? elements.size() : 0);
					if (elements.isEmpty()) {
						bucketIterator = null;
						advanceIterator();
					}
					continue;
				}

				var calculatedKey = toBuf(iterator.key());
				var calculatedValue = col.schema().hasValue() ? toBuf(iterator.value()) : emptyBuf();
				var result = Objects.requireNonNull(decodeKV(col, calculatedKey, calculatedValue));
				advanceIterator();
				return result;
			}
			return null;
		}

		private RangeReadChunk readChunk(LongAdder totalTime) {
			ops.beginOp();
			var sliceStart = System.nanoTime();
			try {
				synchronized (this) {
					if (expired) {
						throw rangeDeadlineExceeded();
					}
					if (closed || exhausted) {
						return new RangeReadChunk(List.of(), true);
					}
					int maximumItems = workloadSettings.rangeQuantumMaxItems();
					long maximumBytes = workloadSettings.rangeQuantumMaxBytes();
					long maximumDurationNanos = workloadSettings.rangeQuantumMaxDuration().toNanos();
					var results = new ArrayList<KV>(Math.min(maximumItems, 1_024));
					long decodedBytes = 0L;
					while (results.size() < maximumItems && !exhausted) {
						if (!results.isEmpty() && System.nanoTime() - sliceStart >= maximumDurationNanos) {
							break;
						}
						var next = pendingRangeItem;
						if (next != null) {
							pendingRangeItem = null;
						} else {
							next = nextPageItem();
							if (next == null) {
								break;
							}
						}
						long nextBytes = decodedKVBytes(next);
						if (nextBytes > maximumBytes) {
							throw RocksDBException.of(RocksDBErrorType.RANGE_ITEM_TOO_LARGE,
									"Range item requires " + nextBytes
											+ " encoded key/value bytes, exceeding quantum maximum "
											+ maximumBytes);
						}
						if (!results.isEmpty() && nextBytes > maximumBytes - decodedBytes) {
							pendingRangeItem = next;
							break;
						}
						results.add(next);
						decodedBytes += nextBytes;
					}
					return new RangeReadChunk(results, exhausted && pendingRangeItem == null);
				}
			} finally {
				totalTime.add(System.nanoTime() - sliceStart);
				ops.endOp();
			}
		}

		private RangeCountChunk countChunk(LongAdder totalTime) {
			ops.beginOp();
			var sliceStart = System.nanoTime();
			try {
				synchronized (this) {
					if (expired) {
						throw rangeDeadlineExceeded();
					}
					if (closed || exhausted) {
						return new RangeCountChunk(0L, true);
					}
					long maximumItems = workloadSettings.rangeQuantumMaxItems();
					long maximumBytes = workloadSettings.rangeQuantumMaxBytes();
					long maximumDurationNanos = workloadSettings.rangeQuantumMaxDuration().toNanos();
					long count = 0L;
					long scannedBytes = 0L;
					while (count < maximumItems) {
						if (deadlineMicros != Long.MAX_VALUE
								&& TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()) >= deadlineMicros) {
							throw rangeDeadlineExceeded();
						}
						if (count != 0L && System.nanoTime() - sliceStart >= maximumDurationNanos) {
							break;
						}
						if (pendingCountItems == 0L) {
							if (!iterator.isValid()) {
								checkIteratorStatusIfInvalid(iterator);
								exhausted = true;
								break;
							}
							pendingCountBytes = saturatingAdd(iterator.key().length, iterator.value().length);
							pendingCountItems = col.hasBuckets()
									? Bucket.readElementCount(toBuf(iterator.value()))
									: 1L;
							advanceIterator();
							if (pendingCountItems == 0L) {
								pendingCountBytes = 0L;
								continue;
							}
						}
						if (pendingCountBytes > maximumBytes) {
							throw RocksDBException.of(RocksDBErrorType.RANGE_ITEM_TOO_LARGE,
									"Count input requires " + pendingCountBytes
											+ " encoded bytes, exceeding quantum maximum " + maximumBytes);
						}
						if (count != 0L && pendingCountBytes > maximumBytes - scannedBytes) {
							break;
						}
						scannedBytes += pendingCountBytes;
						pendingCountBytes = 0L;
						long taken = Math.min(maximumItems - count, pendingCountItems);
						count += taken;
						pendingCountItems -= taken;
					}
					if (pendingCountItems == 0L && !iterator.isValid()) {
						checkIteratorStatusIfInvalid(iterator);
						exhausted = true;
					}
					return new RangeCountChunk(count, exhausted && pendingCountItems == 0L);
				}
			} finally {
				totalTime.add(System.nanoTime() - sliceStart);
				ops.endOp();
			}
		}

		private void advanceIterator() {
			if (reverse) {
				iterator.prev();
			} else {
				iterator.next();
			}
		}

		@Override
		public void expireIfDeadlinePassed(long nowMicros) {
			synchronized (this) {
				if (closed || deadlineMicros == Long.MAX_VALUE || nowMicros < deadlineMicros) {
					return;
				}
				expired = true;
			}
			close();
		}

		private RocksDBException rangeDeadlineExceeded() {
			return RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED, "Deadline exceeded");
		}

		@Override
		public void close() {
			try {
				synchronized (this) {
					if (closed) {
						return;
					}
					closed = true;
					exhausted = true;
					bucketIterator = null;
					try {
						iterator.close();
					} finally {
						try {
							readOptions.close();
						} finally {
							try {
								if (snapshot != null) {
									try {
										db.get().releaseSnapshot(snapshot);
									} finally {
										retainedRangeSnapshots.decrementAndGet();
									}
								}
							} finally {
								try {
									if (endKeySlice != null) {
										endKeySlice.close();
									}
								} finally {
									try {
										if (startKeySlice != null) {
											startKeySlice.close();
										}
									} finally {
										columnUse.close();
									}
								}
							}
						}
					}
				}
			} finally {
				try {
					activeRangeResources.remove(this);
				} finally {
					if (retainedPermit != null) {
						retainedPermit.close();
					}
				}
			}
		}
	}

	private static void ensureCapacity(Buf buf, int offset, int len) {
		int required = offset + len;
		if (buf.size() < required) {
			int newSize = buf.size() + (buf.size() >> 1);
			if (newSize < required) {
				newSize = required;
			}
			buf.size(newSize);
		}
	}

	/**
	 * RocksDB counts disable/enable file-deletion calls. A failed enable must therefore
	 * be retried, otherwise one transient error leaves obsolete-file deletion disabled
	 * for the rest of the process lifetime.
	 */
	private final class RawScanFileDeletionRecovery {
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
					enableRawScanFileDeletions(rocksDB);
					pendingEnables--;
				} catch (org.rocksdb.RocksDBException error) {
					logger.warn("Failed to re-enable RocksDB file deletions after raw-scan SST pinning; "
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
				leakScheduler.schedule(() -> retry(rocksDB), RETRY_DELAY_MILLIS, TimeUnit.MILLISECONDS);
			} catch (RuntimeException schedulingFailure) {
				retryScheduled = false;
				if (!stopped) {
					logger.error("Failed to schedule recovery of RocksDB file deletions; "
							+ "the next raw-scan release will retry", schedulingFailure);
				}
			}
		}

		private synchronized void retry(RocksDB rocksDB) {
			retryScheduled = false;
			drain(rocksDB);
		}

		private synchronized void stop() {
			stopped = true;
			pendingEnables = 0;
		}
	}

	@VisibleForTesting
	protected void enableRawScanFileDeletions(RocksDB rocksDB) throws org.rocksdb.RocksDBException {
		rocksDB.enableFileDeletions();
	}

	@VisibleForTesting
	protected void createRawScanHardLink(Path source, Path target) throws IOException {
		Files.createLink(target, source);
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
				.resolve(RAW_SCAN_PIN_DIRECTORY_NAME)
				.resolve(UUID.nameUUIDFromBytes(databasePath
						.toAbsolutePath()
						.normalize()
						.toString()
						.getBytes(StandardCharsets.UTF_8)).toString());
	}

	private void recoverRawScanPinsAtStartup(RocksDB rocksDB,
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
			logger.warn("Removed {} orphaned raw-scan SST hard links from {} pin roots during startup",
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
		Path databasePinRoot = rawScanDatabasePinRoot(sourceDirectory, definitiveDbPath);
		synchronized (rawScanPinInitializationLock) {
			if (initializedRawScanPinRoots.add(databasePinRoot)) {
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

	@VisibleForTesting
	protected long rawScanPinMaxDurationNanos() {
		return RAW_SCAN_PIN_MAX_DURATION_NANOS;
	}

	private void checkRawScanPinDeadline(long startedNanos) throws IOException {
		long maximumDurationNanos = rawScanPinMaxDurationNanos();
		if (System.nanoTime() - startedNanos > maximumDurationNanos) {
			throw new IOException("Raw-scan SST pinning exceeded the configured maximum duration of "
					+ TimeUnit.NANOSECONDS.toMillis(maximumDurationNanos)
					+ " ms");
		}
	}

	private final class PinnedSstFile implements AutoCloseable {
		private final LiveFileMetaData metadata;
		private final Path path;
		private final long size;
		private final @Nullable RawSstToken token;
		private boolean claimed;
		private boolean closeRequested;
		private boolean closed;

		private PinnedSstFile(LiveFileMetaData metadata, Path path, @Nullable RawSstToken token) {
			this.metadata = metadata;
			this.path = path;
			this.size = metadata.size();
			this.token = token;
			if (!activeRawScanPinnedFiles.add(path)) {
				throw new IllegalStateException("Raw-scan SST pin path is already active: " + path);
			}
			activeRawScanPinnedBytes.addAndGet(size);
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
			activeRawScanPinnedFiles.remove(path);
			activeRawScanPinnedBytes.addAndGet(-size);
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
		private final Set<Path> scanDirectories;
		private boolean closed;

		private RawScanPinnedSstSet(String cfName,
				int shardIndex,
				int shardCount,
				@Nullable Set<String> completedSstFileNames)
				throws IOException, org.rocksdb.RocksDBException {
			var pinnedFiles = new ArrayList<PinnedSstFile>();
			var createdScanDirectories = new LinkedHashSet<Path>();
			var scanId = UUID.randomUUID().toString();
			Throwable captureFailure = null;
			synchronized (rawScanPinAcquisitionLock) {
				RocksDB rocksDB = db.get();
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
					var scanDirectoriesBySource = new HashMap<Path, Path>();
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
						Path scanDirectory = scanDirectoriesBySource.get(sourceDirectory);
						if (scanDirectory == null) {
							scanDirectory = prepareRawScanPinRoot(sourceDirectory).resolve(scanId);
							Files.createDirectories(scanDirectory);
							scanDirectoriesBySource.put(sourceDirectory, scanDirectory);
							createdScanDirectories.add(scanDirectory);
						}
						Path pinnedPath = scanDirectory.resolve(source.getFileName().toString());
						createRawScanHardLink(source, pinnedPath);
						pinnedFiles.add(new PinnedSstFile(file, pinnedPath, token));
						checkRawScanPinDeadline(startedNanos);
					}
				} catch (Throwable failure) {
					captureFailure = failure;
					rawScanPinAcquisitionFailures.increment();
				} finally {
					rawScanFileDeletionRecovery.release(rocksDB);
					rawScanPinAcquisitionTimer.record(System.nanoTime() - startedNanos, TimeUnit.NANOSECONDS);
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
			Collection<Path> scanDirectories,
			@Nullable Throwable originalFailure) {
		Throwable failure = originalFailure;
		for (PinnedSstFile file : files) {
			try {
				file.close();
			} catch (Throwable cleanupFailure) {
				failure = appendFailure(failure, cleanupFailure);
			}
		}
		for (Path scanDirectory : scanDirectories) {
			try {
				Files.deleteIfExists(scanDirectory);
			} catch (Throwable cleanupFailure) {
				failure = appendFailure(failure, cleanupFailure);
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
			reactor.core.Disposable {
		private static final VarHandle DEMAND =
				varHandle(ScanState.class, "demand", long.class);
		private static final VarHandle TERMINATED =
				varHandle(ScanState.class, "terminated", boolean.class);
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
			ops.beginOp();
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
				ops.endOp();
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
				ColumnFamilyOptions cfOpts = columnsConifg.get(cfName);
				createdOptions = cfOpts != null ? new Options(dbOptions, cfOpts) : new Options();
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
						.setReadaheadSize(workloadSettings.rawScanReadaheadBytes());

				createdIterator = createdReader.newIterator(createdReadOptions);
				createdIterator.seekToFirst();
			} catch (org.rocksdb.RocksDBException | RuntimeException | Error e) {
				logger.warn("Failed to open captured SST file: " + file.fileName(), e);
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
			var readerOpenedObserver = rawScanReaderOpenedObserver;
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
						ensureCapacity(outBuf, currentSerializedBatchBytes, Integer.BYTES);
						outBuf.setIntLE(currentSerializedBatchBytes, valLen);
						currentSerializedBatchBytes += Integer.BYTES;
						if (valLen > 0) {
							ensureCapacity(outBuf, currentSerializedBatchBytes, valLen);
							outBuf.setBytesFromBuf(currentSerializedBatchBytes, userValue, 0, valLen);
							currentSerializedBatchBytes += userValue.size();
						}
					} else {
						ensureCapacity(outBuf, currentSerializedBatchBytes, Integer.BYTES);
						outBuf.setIntLE(currentSerializedBatchBytes, 0);
						currentSerializedBatchBytes += Integer.BYTES;
					}

					iterator.next();
					if (batchSize >= RAW_SCAN_MAX_ENTRIES_PER_CHUNK
							|| currentBatchBytes >= RAW_SCAN_MAX_BYTES_PER_CHUNK) {
						if (terminalMapper != null && !iterator.isValid()) {
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
							return RWScheduler.CooperativeResult.YIELD;
						}
					} else {
						preemptionStartNanos = 0L;
					}
				}
			} catch (VirtualMachineError fatal) {
				throw fatal;
			} catch (Throwable failure) {
				context.fail(cooperativeFailure(failure,
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
				context.fail(cooperativeFailure(failure,
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
						: appendFailure(originalFailure, resourceCleanupFailure);
			}
			resourcesCleaned = true;
			Throwable cleanupFailure = null;
			try {
				closeNativeResources(admissionRetry);
			} catch (Throwable closeFailure) {
				cleanupFailure = appendFailure(cleanupFailure, closeFailure);
			}
			resourceCleanupFailure = cleanupFailure;
			return cleanupFailure == null
					? originalFailure
					: appendFailure(originalFailure, cleanupFailure);
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
					failure = appendFailure(failure, error);
				}
			}
			var localReader = reader;
			reader = null;
			if (localReader != null) {
				try {
					localReader.close();
				} catch (Throwable error) {
					failure = appendFailure(failure, error);
				}
			}
			var localOptions = options;
			options = null;
			if (localOptions != null) {
				try {
					localOptions.close();
				} catch (Throwable error) {
					failure = appendFailure(failure, error);
				}
			}
			try {
				if (admissionRetry) {
					fileClaim.releaseForAdmissionRetry();
				} else {
					fileClaim.close();
				}
			} catch (Throwable error) {
				failure = appendFailure(failure, error);
			}
			try {
				retainedColumnUse.close();
			} catch (Throwable error) {
				failure = appendFailure(failure, error);
			}
			try {
				ops.endOp();
			} catch (Throwable error) {
				failure = appendFailure(failure, error);
			}
			var cleanupObserver = rawScanCleanupObserver;
			if (cleanupObserver != null) {
				try {
					cleanupObserver.run();
				} catch (Throwable error) {
					failure = appendFailure(failure, error);
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
				scheduler.read(),
				scheduler.readExecutor());
	}

	public Flux<SerializedKVBatch> scanRawAsyncInternal(long columnId,
			int shardIndex,
			int shardCount,
			@NotNull Scheduler workloadScheduler) {
		var workloadExecutor = workloadScheduler instanceof IndexedWorkloadScheduler indexedScheduler
				? indexedScheduler.workloadExecutor()
				: scheduler.readExecutor();
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
		long maximumQuantumNanos = workloadSettings.rangeQuantumMaxDuration().toNanos();
		return Flux.using(
				() -> {
					ops.beginOp();
					try {
						return acquireColumnUse(columnId);
					} catch (Throwable error) {
						ops.endOp();
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

					return Flux.using(
							() -> new RawScanPinnedSstSet(cfName, shardIndex, shardCount, completedSstFileNames),
							pinnedSsts -> {
								var observer = rawScanFilesCapturedObserver;
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
														RAW_SCAN_MAX_BYTES_PER_CHUNK));
											} catch (RuntimeException admissionFailure) {
												state.reject(admissionFailure);
											}
										}, FluxSink.OverflowStrategy.ERROR))
												// Retry only pre-dispatch queue rejection. ScanState keeps the
												// same hard link available, while native/data failures remain terminal.
												.retryWhen(RAW_SCAN_ADMISSION_RETRY)
												.publishOn(reactor.core.scheduler.Schedulers.parallel(), 1);

								var ssts = Flux.fromIterable(pinnedSsts.files());
								if (shardCount == 1) {
									return ssts.flatMap(mapper, workloadSettings.rawScanFileConcurrency(), 1);
								} else {
									return ssts.concatMap(mapper, 2);
								}
							},
							RawScanPinnedSstSet::close,
							true);
				}),
				columnUse -> {
					try {
						columnUse.close();
					} finally {
						ops.endOp();
					}
				},
				true)
				.subscribeOn(workloadScheduler);
	}

	public Flux<RawScanEvent> scanRawResumableAsyncInternal(long columnId,
			int shardIndex,
			int shardCount,
			Set<RawSstToken> completedSsts,
			@NotNull Scheduler workloadScheduler) {
		var workloadExecutor = workloadScheduler instanceof IndexedWorkloadScheduler indexedScheduler
				? indexedScheduler.workloadExecutor()
				: scheduler.readExecutor();
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

	@Override
	public void flush() {
		var start = System.nanoTime();
		ops.beginOp();
		try {
			actionLogger.logAction("Flush", start, null, null, null, null, null, null, null);
			synchronized (columnEditLock) {
				var observer = columnMaintenanceObserver;
				if (observer != null) {
					observer.run();
				}
				db.get().flushWal(true);
				recordCdcPublishedTail();
				try (var fo = new FlushOptions().setWaitForFlush(true).setAllowWriteStall(true)) {
					db
							.get()
							.flush(fo,
									columns
											.values()
											.stream()
											.map(ColumnInstance::cfh)
											.filter(AbstractImmutableNativeReference::isOwningHandle)
											.toList()
							);
				}
			}
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, ex);
		} finally {
			ops.endOp();
			var end = System.nanoTime();
			flushTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	@Override
	public void compact() {
		var start = System.nanoTime();
		ops.beginOp();
		try {
			actionLogger.logAction("Compact", start, null, null, null, null, null, null, null);
			synchronized (columnEditLock) {
				var observer = columnMaintenanceObserver;
				if (observer != null) {
					observer.run();
				}
				var db = this.db.get();
				for (ColumnInstance value : columns.values()) {
					if (value.cfh().isOwningHandle()) {
						String columnName = new String(value.cfh().getName(), StandardCharsets.UTF_8);
						ColumnFamilyOptions columnOptions = columnsConifg.get(columnName);
						try (var cro = new CompactRangeOptions()
								.setAllowWriteStall(false)
								.setExclusiveManualCompaction(true)
								.setChangeLevel(false)
								.setTargetPathId(bottommostCompactionTargetPathId(columnOptions))
								.setMaxSubcompactions(16)
								.setBottommostLevelCompaction(BottommostLevelCompaction.kForceOptimized)) {
							db.compactRange(value.cfh(), null, null, cro);
						}
					}
				}
			}
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, ex);
		} finally {
			ops.endOp();
			var end = System.nanoTime();
			compactTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	@VisibleForTesting
	public static int bottommostCompactionTargetPathId(@Nullable ColumnFamilyOptions options) {
		if (options == null || options.cfPaths() == null || options.cfPaths().size() <= 1) {
			return 0;
		}
		return options.cfPaths().size() - 1;
	}

	@Override
	public Map<String, ColumnSchema> getAllColumnDefinitions() throws RocksDBException {
		var start = System.nanoTime();
		ops.beginOp();
		try {
			actionLogger.logAction("GetAllColumnDefinitions", start, null, null, null, null, null, null, null);
			return columnNamesIndex
					.entrySet()
					.stream()
					.collect(Collectors.toMap(Entry::getKey, e -> columns.get(e.getValue()).schema()));
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, ex);
		} finally {
			ops.endOp();
			var end = System.nanoTime();
			getAllColumnDefinitionsTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private Buf dbGet(Tx tx, ColumnInstance col, ReadOptions readOptions, Buf calculatedKey)
			throws org.rocksdb.RocksDBException {
		if (tx != null) {
			byte[] previousRawBucketByteArray;
			byte[] calculatedKeyArray = calculatedKey.toByteArray();
			if (tx.isFromGetForUpdate()) {
				previousRawBucketByteArray = tx.val().getForUpdate(readOptions, col.cfh(), calculatedKeyArray, true);
			} else {
				previousRawBucketByteArray = tx.val().get(readOptions, col.cfh(), calculatedKeyArray);
			}
			return toBuf(previousRawBucketByteArray);
		} else {
			var db = this.db.get();
			if (fastGet) {
				return dbGetFast(col.cfh(), readOptions, calculatedKey);
			} else {
				var previousRawBucketByteArray = db.get(col.cfh(),
						readOptions,
						calculatedKey.getBackingByteArray(),
						calculatedKey.getBackingByteArrayOffset(),
						calculatedKey.getBackingByteArrayLength()
				);
				return toBuf(previousRawBucketByteArray);
			}
		}
	}

	@Nullable
	private Buf dbGetWithDefaultOptions(@Nullable Tx tx, ColumnInstance col, Buf calculatedKey)
			throws org.rocksdb.RocksDBException {
		if (tx == null && fastGet) {
			return dbGetFast(col.cfh(), calculatedKey);
		}
		try (var readOptions = newReadOptions(null)) {
			return dbGet(tx, col, readOptions, calculatedKey);
		}
	}

	@Nullable
	private Buf dbGetFast(ColumnFamilyHandle cfh, ReadOptions readOptions, Buf calculatedKey)
			throws org.rocksdb.RocksDBException {
		return dbGetFast(cfh, readOptions, calculatedKey, false);
	}

	@Nullable
	private Buf dbGetFast(ColumnFamilyHandle cfh, Buf calculatedKey)
			throws org.rocksdb.RocksDBException {
		return dbGetFast(cfh, null, calculatedKey, true);
	}

	@Nullable
	private Buf dbGetFast(ColumnFamilyHandle cfh,
			@Nullable ReadOptions readOptions,
			Buf calculatedKey,
			boolean useDefaultReadOptions)
			throws org.rocksdb.RocksDBException {
		var reader = Objects.requireNonNull(fastGetReader);
		byte[] value = useDefaultReadOptions
				? reader.getHeap(cfh,
						calculatedKey.getBackingByteArray(),
						calculatedKey.getBackingByteArrayOffset(),
						calculatedKey.getBackingByteArrayLength())
				: reader.getHeap(cfh,
						Objects.requireNonNull(readOptions),
						calculatedKey.getBackingByteArray(),
						calculatedKey.getBackingByteArrayOffset(),
						calculatedKey.getBackingByteArrayLength());
		return toBuf(value);
	}

	private ColumnInstance getColumn(long columnId) {
		var col = columns.get(columnId);
		if (col != null) {
			return col;
		} else {
			throw RocksDBException.of(RocksDBErrorType.COLUMN_NOT_FOUND, "No column with id " + columnId);
		}
	}

	private ColumnInstance beginColumnUse(long columnId) {
		var col = getColumn(columnId);
		try {
			col.beginUse();
		} catch (IllegalStateException closing) {
			throw RocksDBException.of(RocksDBErrorType.COLUMN_NOT_FOUND,
					"Column " + columnId + " is being deleted");
		}
		try {
			var observer = columnUseAcquiredObserver;
			if (observer != null) {
				observer.accept(columnId);
			}
			return col;
		} catch (Throwable error) {
			col.endUse();
			throw error;
		}
	}

	private static boolean tryBeginColumnUse(ColumnInstance column) {
		try {
			column.beginUse();
			return true;
		} catch (IllegalStateException closing) {
			return false;
		}
	}

	private ColumnInstance.ColumnUse acquireColumnUse(long columnId) {
		var col = getColumn(columnId);
		final ColumnInstance.ColumnUse use;
		try {
			use = col.acquireUse();
		} catch (IllegalStateException closing) {
			throw RocksDBException.of(RocksDBErrorType.COLUMN_NOT_FOUND,
					"Column " + columnId + " is being deleted");
		}
		try {
			var observer = columnUseAcquiredObserver;
			if (observer != null) {
				observer.accept(columnId);
			}
			return use;
		} catch (Throwable error) {
			use.close();
			throw error;
		}
	}

	private Tx getTransaction(long transactionId, boolean allowGetForUpdate) {
		var tx = txs.get(transactionId);
		if (tx != null) {
			if (!allowGetForUpdate && tx.isFromGetForUpdate()) {
				throw RocksDBException.of(RocksDBErrorType.RESTRICTED_TRANSACTION,
						"Can't get this transaction, it's for internal use only"
				);
			}
			return tx;
		} else {
			throw RocksDBException.of(RocksDBErrorType.TRANSACTION_NOT_FOUND, "No transaction with id " + transactionId);
		}
	}

	private Tx getTransaction(long transactionId,
			boolean allowGetForUpdate,
			WorkloadProfile requestedProfile) {
		var tx = getTransaction(transactionId, allowGetForUpdate);
		requireResourceProfile("transaction or update",
				transactionId,
				tx.workloadProfile(),
				requestedProfile);
		return tx;
	}

	public void validateTransactionOrUpdateProfile(long transactionOrUpdateId,
			WorkloadProfile requestedProfile) {
		if (transactionOrUpdateId != 0L) {
			getTransaction(transactionOrUpdateId, true, requestedProfile);
		}
	}

	public void validateTransactionProfile(long transactionId, WorkloadProfile requestedProfile) {
		if (transactionId != 0L) {
			var tx = getTransaction(transactionId, true);
			requireResourceProfile("transaction",
					transactionId,
					tx.workloadProfile(),
					requestedProfile);
		}
	}

	public void validateIteratorProfile(long iteratorId, WorkloadProfile requestedProfile) {
		var iterator = its.get(iteratorId);
		if (iterator == null) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Unknown iterator " + iteratorId);
		}
		requireResourceProfile("iterator",
				iteratorId,
				iterator.workloadProfile(),
				requestedProfile);
	}

	private static void requireResourceProfile(String resourceType,
			long id,
			WorkloadProfile boundProfile,
			WorkloadProfile requestedProfile) {
		Objects.requireNonNull(requestedProfile, "requestedProfile");
		if (boundProfile != requestedProfile) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Cannot change " + resourceType + " " + id + " from "
							+ boundProfile + " to " + requestedProfile);
		}
	}

	private void retainResourceLease() {
		for (;;) {
			long current = resourceLeases.get();
			if (current == Long.MAX_VALUE) {
				throw new IllegalStateException("Too many open resource leases");
			}
			if (resourceLeases.compareAndSet(current, current + 1)) {
				return;
			}
		}
	}

	private void releaseResourceLease() {
		for (;;) {
			long current = resourceLeases.get();
			if (current == 0) {
				throw new IllegalStateException("No open resource lease to release");
			}
			if (resourceLeases.compareAndSet(current, current - 1)) {
				return;
			}
		}
	}

	public @Nullable Path getPath() {
		return path;
	}

	@VisibleForTesting
	public TransactionalDB getDb() {
		return db;
	}

	@VisibleForTesting
	public long getPendingOpsCount() {
		long activeOperations = ops.getPendingOpsCount();
		long resources = resourceLeases.get();
		return activeOperations > Long.MAX_VALUE - resources
				? Long.MAX_VALUE
				: activeOperations + resources;
	}

	@VisibleForTesting
	public int getOpenTransactionsCount() {
		return txs.size();
	}

	@VisibleForTesting
	public int getOpenIteratorsCount() {
		return its.size();
	}

	@VisibleForTesting
	public int getActiveRangeCursorCount() {
		int cursors = 0;
		for (var resource : activeRangeResources) {
			if (resource instanceof RangeCursor
					|| resource instanceof RetainedRangeCooperativeTask task && task.hasOpenCursor()) {
				cursors++;
			}
		}
		return cursors;
	}

	@VisibleForTesting
	public int getRetainedRangeSnapshotCount() {
		return retainedRangeSnapshots.get();
	}

	@VisibleForTesting
	public int getRetainedRangePermitCount() {
		return retainedQueryLimiter.activeCount();
	}

	@VisibleForTesting
	public int getRetainedRangeWaiterCount() {
		return retainedQueryLimiter.waitingCount();
	}

	@VisibleForTesting
	public int getActiveCdcPollCursorCount() {
		return activeCdcPollCursors.size();
	}

	@VisibleForTesting
	public int getActiveExistsMultiRequestCount() {
		return activeExistsMultiRequests.size();
	}

	@VisibleForTesting
	public long getCdcLagForTesting() {
		return currentCdcLag();
	}

	@VisibleForTesting
	public long getCdcPublishedTailForTesting() {
		return cdcPublishedTailSequence.get();
	}

	@VisibleForTesting
	public boolean isExpiredRangeCleanupScheduledForTesting() {
		return !expiredRangeCleanupTask.isCancelled() && !expiredRangeCleanupTask.isDone();
	}

	@VisibleForTesting
	public DatabaseConfig getConfig() {
		return config;
	}

	/** Resolved workload limits shared by scheduler and bounded-operation implementations. */
	public WorkloadSettings getWorkloadSettings() {
		return workloadSettings;
	}

	private AbstractSlice<?> toDirectSlice(Buf calculatedKey) {
		return new DirectSlice(calculatedKey.asHeapByteBuffer(), calculatedKey.size());
	}

	private AbstractSlice<?> toSlice(Buf calculatedKey) {
		return new Slice(calculatedKey.asArray());
	}

	private KV decodeKV(ColumnInstance col, Buf calculatedKey, Buf calculatedValue) {
		var keys = col.decodeKeys(calculatedKey, calculatedValue);
		var value = col.decodeValue(calculatedValue);
		return new KV(new Keys(keys), value);
	}

	private KV decodeBucketEntry(ColumnInstance col, Buf calculatedKey, Entry<Buf[], Buf> bucketEntry) {
		int keysCount = col.schema().keysCount();
		Buf[] keys = new Buf[keysCount];
		int firstVariableKeyIndex = keysCount - col.schema().variableLengthKeysCount();

		// Decode fixed keys
		col.decodeFixedKeys(calculatedKey, keys, firstVariableKeyIndex);

		// Copy variable keys
		Buf[] varKeys = bucketEntry.getKey();
		System.arraycopy(varKeys, 0, keys, firstVariableKeyIndex, varKeys.length);

		return new KV(new Keys(keys), bucketEntry.getValue());
	}

	private static long decodedKVBytes(KV kv) {
		long bytes = kv.value() != null ? kv.value().size() : 0L;
		for (var key : kv.keys().keys()) {
			bytes = saturatingAdd(bytes, key.size());
		}
		return bytes;
	}

	private static long saturatingAdd(long left, long right) {
		return right >= Long.MAX_VALUE - left ? Long.MAX_VALUE : left + right;
	}

	@Override
	public RWScheduler getScheduler() {
		return scheduler;
	}

	/** Returns the registry used for this database's operational metrics. */
	public MeterRegistry getMetricsRegistry() {
		return metrics.getRegistry();
	}

	private static interface ActionLoggerConsumer {

		void logAction(String action,
				long actionId,
				Object column,
				Object key,
				Object valueOrEndKey,
				Object txOrUpdateId,
				Object commit,
				Object timeoutMs,
				Object requestType);
	}

	// ============ CDC API (durable CDC using WAL) ============

	private static final int CDC_OP_INDEX_BITS = 20;
	private static final long CDC_OP_INDEX_MASK = (1L << CDC_OP_INDEX_BITS) - 1L;
	private static final long CDC_DEFAULT_MAX_EVENTS = 10_000L;
	private static final int CDC_PREFIXLESS_PROBE_MAX_ATTEMPTS = 3;

	private static final class CdcMetadataLock {

		private final ReentrantLock lock = new ReentrantLock();
		private int users;
	}

	@FunctionalInterface
	private interface CdcMetadataAction<T> {

		T execute() throws org.rocksdb.RocksDBException;
	}

	private CdcMetadataLock acquireCdcMetadataLock(String id) {
		var metadataLock = cdcMetadataLocks.compute(id, (_, current) -> {
			var result = current != null ? current : new CdcMetadataLock();
			result.users++;
			return result;
		});
		metadataLock.lock.lock();
		return metadataLock;
	}

	private void releaseCdcMetadataLock(String id, CdcMetadataLock metadataLock) {
		metadataLock.lock.unlock();
		cdcMetadataLocks.compute(id, (_, current) -> {
			if (current != metadataLock) {
				throw new IllegalStateException("CDC metadata lock changed while in use: " + id);
			}
			return --current.users == 0 ? null : current;
		});
	}

	private <T> T withCdcMetadataLock(String operation,
			String id,
			CdcMetadataAction<T> action) throws org.rocksdb.RocksDBException {
		var operationObserver = cdcMetadataOperationObserver;
		if (operationObserver != null) {
			operationObserver.accept(operation, id);
		}
		var metadataLock = acquireCdcMetadataLock(id);
		try {
			return action.execute();
		} finally {
			releaseCdcMetadataLock(id, metadataLock);
		}
	}

	private void notifyCdcMetadataLoaded(String operation, String id) {
		var loadedObserver = cdcMetadataLoadedObserver;
		if (loadedObserver != null) {
			loadedObserver.accept(operation, id);
		}
	}

	private static long composeCdcSeq(long walSeq, long opIndex) {
		if (walSeq < 0L || opIndex < 0L) {
			throw new IllegalArgumentException("CDC sequence components must be non-negative");
		}
		if (opIndex > CDC_OP_INDEX_MASK) {
			throw new IllegalArgumentException("A CDC WAL batch cannot contain more than "
					+ CDC_OP_INDEX_MASK + " mutations");
		}
		if (walSeq > (Long.MAX_VALUE >>> CDC_OP_INDEX_BITS)) {
			throw new IllegalArgumentException("CDC WAL sequence exceeds the external cursor format");
		}
		return (walSeq << CDC_OP_INDEX_BITS) | opIndex;
	}

	private static long extractCdcWalSeq(long cdcSeq) {
		return cdcSeq >>> CDC_OP_INDEX_BITS;
	}

	private static long extractCdcOpIndex(long cdcSeq) {
		return cdcSeq & CDC_OP_INDEX_MASK;
	}

	private static long extractCdcRocksSequence(long cdcSeq) {
		return Math.addExact(extractCdcWalSeq(cdcSeq), extractCdcOpIndex(cdcSeq));
	}

	private static long cdcLag(long nextExternalSequence, long tailWalSequenceInclusive) {
		long nextRocksSequence;
		try {
			nextRocksSequence = extractCdcRocksSequence(nextExternalSequence);
		} catch (ArithmeticException invalidCursor) {
			return Long.MAX_VALUE;
		}
		if (nextRocksSequence > tailWalSequenceInclusive) {
			return 0L;
		}
		long remaining = tailWalSequenceInclusive - nextRocksSequence;
		return remaining == Long.MAX_VALUE ? Long.MAX_VALUE : remaining + 1L;
	}

	private long currentCdcLag() {
		long tail = cdcPublishedTailSequence.get();
		long maximum = 0L;
		for (var progress : cdcSubscriptionProgress.values()) {
			maximum = Math.max(maximum, cdcLag(progress.effectiveNextSeq(), tail));
			if (maximum == Long.MAX_VALUE) {
				break;
			}
		}
		return maximum;
	}

	private void recordCdcPublishedTail() {
		recordCdcPublishedTail(db.get().getLatestSequenceNumber());
	}

	private void recordCdcPublishedTail(long tailSequenceInclusive) {
		cdcPublishedTailSequence.accumulateAndGet(tailSequenceInclusive, Math::max);
	}

	private CdcSubscriptionProgress rememberCdcSubscription(String id, CdcSubscriptionMeta meta) {
		long committedNextSeq = meta.lastCommittedSeq() == Long.MAX_VALUE
				? Long.MAX_VALUE
				: meta.lastCommittedSeq() + 1L;
		return cdcSubscriptionProgress.compute(id, (_, existing) -> {
			if (existing == null) {
				return new CdcSubscriptionProgress(committedNextSeq);
			}
			existing.recordCommit(committedNextSeq);
			return existing;
		});
	}

	private void recordCdcCursorProgress(CdcSubscriptionProgress progress, long nextSeq) {
		progress.recordCursor(nextSeq);
	}

	private void loadCdcSubscriptionProgress() throws IOException {
		try (var readOptions = newReadOptions("cdc-subscription-progress-read-options");
				var iterator = db.get().newIterator(cdcMetaColumnDescriptorHandle, readOptions)) {
			iterator.seekToFirst();
			while (iterator.isValid()) {
				var key = new String(iterator.key(), StandardCharsets.UTF_8);
				if (key.startsWith("sub:")) {
					rememberCdcSubscription(key.substring("sub:".length()), decodeCdcMeta(iterator.value()));
				}
				iterator.next();
			}
			checkIteratorStatusIfInvalid(iterator);
		} catch (RocksDBException error) {
			throw new IOException("Cannot load CDC subscription progress", error);
		}
	}

	private static final class CdcSubscriptionProgress {

		private final AtomicLong committedNextSeq;
		private final AtomicLong observedNextSeq;

		private CdcSubscriptionProgress(long committedNextSeq) {
			this.committedNextSeq = new AtomicLong(committedNextSeq);
			this.observedNextSeq = new AtomicLong(committedNextSeq);
		}

		private void recordCommit(long nextSeq) {
			committedNextSeq.accumulateAndGet(nextSeq, Math::max);
			observedNextSeq.accumulateAndGet(nextSeq, Math::max);
		}

		private void recordCursor(long nextSeq) {
			observedNextSeq.accumulateAndGet(nextSeq, Math::max);
		}

		private long effectiveNextSeq() {
			return Math.max(committedNextSeq.get(), observedNextSeq.get());
		}
	}

	private record CdcSubscriptionMeta(long lastCommittedSeq, @Nullable long[] columnFilter, boolean emitLatestValues) {}
	private record CdcWalPublication(long latestBeforeFlush, long publishedTailSequence) {}
	private record CdcWalDiscovery(long earliestWalSequence, long publishedTailSequence) {}
	private record CdcPollWindow(long startSeq,
			long maxWalSequenceInclusive,
			CdcSubscriptionMeta subscription,
			CdcSubscriptionProgress progress) {}
	private record CdcPollPage(CdcBatch batch,
			long emittedEvents,
			long emittedBytes,
			long advancedMutations) {}
	private record CdcResolvedPage(List<CDCEvent> events,
			long emittedBytes,
			@Nullable Long continuationSeq) {}
	private record CdcPollCursorStart(CdcPollCursor cursor, CdcPollPage firstPage) {}
	private record CdcStreamPage(long startSeq,
			long remainingEvents,
			long remainingBytes,
			boolean allowOversizedFirstEvent,
			CdcPollPage page) {}

	private byte[] cdcKeyOf(String id) {
		return ("sub:" + id).getBytes(StandardCharsets.UTF_8);
	}

	private void saveCdcMeta(String id, CdcSubscriptionMeta meta) throws org.rocksdb.RocksDBException {
		try (var baos = new ByteArrayOutputStream(); var dos = new DataOutputStream(baos)) {
			dos.writeByte(2); // v2 stores the stable packed WAL-batch cursor
			dos.writeLong(meta.lastCommittedSeq);
			var filter = meta.columnFilter;
			dos.writeBoolean(filter != null);
			if (filter != null) {
				dos.writeInt(filter.length);
				for (long f : filter) {
					dos.writeLong(f);
				}
			}
			// v2: emitLatestValues flag
			dos.writeBoolean(meta.emitLatestValues);
			dos.flush();
			db.get().put(cdcMetaColumnDescriptorHandle, cdcKeyOf(id), baos.toByteArray());
			recordCdcPublishedTail();
		} catch (IOException e) {
			throw new org.rocksdb.RocksDBException(e.getMessage());
		}
	}

	private @Nullable CdcSubscriptionMeta loadCdcMeta(String id) throws org.rocksdb.RocksDBException {
		var val = db.get().get(cdcMetaColumnDescriptorHandle, cdcKeyOf(id));
		if (val == null) {
			return null;
		}
		try {
			return decodeCdcMeta(val);
		} catch (IOException e) {
			throw new org.rocksdb.RocksDBException(e.getMessage());
		}
	}

	private CdcSubscriptionMeta decodeCdcMeta(byte[] val) throws IOException {
		try (var dis = new DataInputStream(new ByteArrayInputStream(val))) {
			int ver = dis.readUnsignedByte();
			long lastCommittedSeq;
			boolean hasFilter;
			long[] filter = null;
			boolean resolved = false;
			if (ver == 1) {
				lastCommittedSeq = dis.readLong();
				hasFilter = dis.readBoolean();
				if (hasFilter) {
					int n = dis.readInt();
					filter = new long[n];
					for (int i = 0; i < n; i++) {
						filter[i] = dis.readLong();
					}
				}
				resolved = false;
			} else if (ver == 2) {
				lastCommittedSeq = dis.readLong();
				hasFilter = dis.readBoolean();
				if (hasFilter) {
					int n = dis.readInt();
					filter = new long[n];
					for (int i = 0; i < n; i++) {
						filter[i] = dis.readLong();
					}
				}
				resolved = dis.readBoolean();
			} else {
				throw new IOException("Unknown CDC meta version: " + ver);
			}
			return new CdcSubscriptionMeta(lastCommittedSeq, filter, resolved);
		}
	}

	private CdcWalDiscovery findEarliestAvailableWal() {
		for (int attempt = 0; attempt < CDC_PREFIXLESS_PROBE_MAX_ATTEMPTS; attempt++) {
			try {
				var result = findEarliestAvailableWalAttempt();
				if (result.isPresent()) {
					return result.get();
				}
			} catch (org.rocksdb.RocksDBException error) {
				try {
					if (handleCdcIteratorStatus(error)) {
						continue;
					}
				} catch (org.rocksdb.RocksDBException operationalError) {
					throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, operationalError);
				}
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, error);
			}
		}
		throw new RocksDBRetryException();
	}

	private long findEarliestAvailableWalSeq() {
		return findEarliestAvailableWal().earliestWalSequence();
	}

	private Optional<CdcWalDiscovery> findEarliestAvailableWalAttempt() throws org.rocksdb.RocksDBException {
		var publication = publishCdcWal();
		return probeEarliestAvailableWal(publication);
	}

	private CdcWalPublication publishCdcWal() throws org.rocksdb.RocksDBException {
		long latestBeforeFlush = getLatestCdcWalSequence();
		// getUpdatesSince reads WAL files, so publish the application WAL buffer without
		// forcing an fsync. The sequence captured before the flush is the conservative
		// publication boundary: a concurrent mutation after that capture is intentionally
		// deferred to the next logical poll, even if this flush happens to include it.
		flushCdcWalForPrefixlessProbe();
		recordCdcPublishedTail();
		return new CdcWalPublication(latestBeforeFlush, latestBeforeFlush);
	}

	private Optional<CdcWalDiscovery> probeEarliestAvailableWal(CdcWalPublication publication)
			throws org.rocksdb.RocksDBException {
		var earliest = probeEarliestAvailableWalSeq();
		if (earliest.isPresent()) {
			return Optional.of(new CdcWalDiscovery(earliest.getAsLong(), publication.publishedTailSequence()));
		}

		long latestAfterProbe = getLatestCdcWalSequence();
		if (publication.latestBeforeFlush() == latestAfterProbe) {
			return Optional.of(new CdcWalDiscovery(latestAfterProbe + 1,
					publication.publishedTailSequence())); // sequence-stable empty DB
		}
		return Optional.empty();
	}

	private Mono<CdcWalPublication> publishCdcWalAsync() {
		return scheduleTracked(scheduler.scheduler(WorkloadProfile.CDC,
				OperationFamily.FLUSH,
				RequestContext.NO_DEADLINE), this::publishCdcWal);
	}

	private Mono<CdcWalDiscovery> findEarliestAvailableWalAsync() {
		return findEarliestAvailableWalAsync(0);
	}

	private Mono<CdcWalDiscovery> findEarliestAvailableWalAsync(int attempt) {
		if (attempt >= CDC_PREFIXLESS_PROBE_MAX_ATTEMPTS) {
			return Mono.error(new RocksDBRetryException());
		}
		return publishCdcWalAsync()
				.flatMap(publication -> scheduleTracked(scheduler.cdc(),
						() -> probeEarliestAvailableWal(publication)))
				.flatMap(result -> result.<Mono<CdcWalDiscovery>>map(Mono::just)
						.orElseGet(() -> findEarliestAvailableWalAsync(attempt + 1)))
				.onErrorResume(org.rocksdb.RocksDBException.class, error -> {
					try {
						if (handleCdcIteratorStatus(error)) {
							return findEarliestAvailableWalAsync(attempt + 1);
						}
					} catch (org.rocksdb.RocksDBException operationalError) {
						return Mono.error(RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, operationalError));
					}
					return Mono.error(RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, error));
				});
	}

	@VisibleForTesting
	protected long getLatestCdcWalSequence() {
		return db.get().getLatestSequenceNumber();
	}

	@VisibleForTesting
	protected void flushCdcWalForPrefixlessProbe() throws org.rocksdb.RocksDBException {
		db.get().flushWal(false);
	}

	@VisibleForTesting
	protected OptionalLong probeEarliestAvailableWalSeq() throws org.rocksdb.RocksDBException {
		try (TransactionLogIterator it = db.get().getUpdatesSince(0)) {
			if (it != null && it.isValid()) {
				return OptionalLong.of(readCdcBatchSequenceAndClose(it.getBatch()));
			}
			if (it != null) {
				// Propagate the exact tail-refresh status to the bounded outer retry loop.
				it.status();
			}
			return OptionalLong.empty();
		}
	}

	@VisibleForTesting
	protected long readCdcBatchSequenceAndClose(TransactionLogIterator.BatchResult batch) {
		try (var ignored = batch.writeBatch()) {
			return batch.sequenceNumber();
		}
	}

	@Override
	public long cdcGetEarliestAvailableSequence() throws RocksDBException {
		ops.beginOp();
		try {
			return composeCdcSeq(findEarliestAvailableWal().earliestWalSequence(), 0);
		} finally {
			ops.endOp();
		}
	}

	@Override
	public long cdcCreate(@NotNull String id, @Nullable Long fromSeq, @Nullable List<Long> columnIds)
			throws RocksDBException {
		return cdcCreate(id, fromSeq, columnIds, null, null);
	}

	@Override
	public long cdcCreate(@NotNull String id,
			@Nullable Long fromSeq,
			@Nullable List<Long> columnIds,
			@Nullable Boolean emitLatestValues) throws RocksDBException {
		return cdcCreate(id, fromSeq, columnIds, emitLatestValues, null);
	}

	@Override
	public long cdcCreate(@NotNull String id,
			@Nullable Long fromSeq,
			@Nullable List<Long> columnIds,
			@Nullable Boolean emitLatestValues,
			@Nullable OptionalLong expectedLastCommitted) throws RocksDBException {
		Objects.requireNonNull(id, "id");
		ops.beginOp();
		try {
			// Prefix-less WAL discovery can flush/probe repeatedly and belongs to the
			// dedicated CDC lane. Resolve it before taking the per-subscription metadata
			// lock so a slow probe cannot park an interactive watermark read behind it.
			final long prefixlessStartSeq = fromSeq != null && fromSeq == 0L
					? composeCdcSeq(findEarliestAvailableWalSeq(), 0)
					: 0L;
			return withCdcMetadataLock("create", id, () -> {
				var existing = loadCdcMeta(id);
				notifyCdcMetadataLoaded("create", id);
				validateCdcCreatePrecondition(id, existing, expectedLastCommitted);
				long startSeq;
				if (fromSeq != null) {
					startSeq = fromSeq == 0L ? prefixlessStartSeq : fromSeq;
				} else {
					if (existing != null) {
						startSeq = existing.lastCommittedSeq + 1;
					} else {
						long latestWal = db.get().getLatestSequenceNumber();
						startSeq = composeCdcSeq(latestWal + 1, 0);
					}
				}

				long lastCommittedToPersist;
				if (existing == null) {
					lastCommittedToPersist = startSeq - 1;
				} else {
					lastCommittedToPersist = existing.lastCommittedSeq;
				}
				long[] filter = existing != null ? existing.columnFilter : null;
				if (columnIds != null) {
					filter = columnIds.stream().mapToLong(Long::longValue).toArray();
				}
				boolean resolved = existing != null ? existing.emitLatestValues : false;
				if (emitLatestValues != null) {
					resolved = emitLatestValues;
				}
				var updated = new CdcSubscriptionMeta(lastCommittedToPersist, filter, resolved);
				saveCdcMeta(id, updated);
				rememberCdcSubscription(id, updated);
				return startSeq;
			});
		} catch (org.rocksdb.RocksDBException e) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
		} finally {
			ops.endOp();
		}
	}

	private static void validateCdcCreatePrecondition(String id,
			@Nullable CdcSubscriptionMeta existing,
			@Nullable OptionalLong expectedLastCommitted) {
		if (expectedLastCommitted == null) {
			return;
		}
		if (expectedLastCommitted.isEmpty()) {
			if (existing == null) {
				return;
			}
			throw RocksDBException.of(RocksDBErrorType.CDC_SUBSCRIPTION_CHANGED,
					"CDC subscription '" + id + "' was expected to be absent but currently has lastCommittedSeq="
							+ existing.lastCommittedSeq);
		}
		long expected = expectedLastCommitted.getAsLong();
		if (existing != null && existing.lastCommittedSeq == expected) {
			return;
		}
		String actual = existing == null ? "absent" : Long.toString(existing.lastCommittedSeq);
		throw RocksDBException.of(RocksDBErrorType.CDC_SUBSCRIPTION_CHANGED,
				"CDC subscription '" + id + "' changed: expected lastCommittedSeq=" + expected
						+ " but was " + actual);
	}

	@Override
	public void cdcDelete(@NotNull String id) throws RocksDBException {
		Objects.requireNonNull(id, "id");
		ops.beginOp();
		try {
			withCdcMetadataLock("delete", id, () -> {
				db.get().delete(cdcMetaColumnDescriptorHandle, cdcKeyOf(id));
				cdcSubscriptionProgress.remove(id);
				recordCdcPublishedTail();
				return null;
			});
		} catch (org.rocksdb.RocksDBException e) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
		} finally {
			ops.endOp();
		}
	}

	@Override
	public OptionalLong cdcGetLastCommittedSequence(@NotNull String id) throws RocksDBException {
		Objects.requireNonNull(id, "id");
		ops.beginOp();
		try {
			return withCdcMetadataLock("get", id, () -> {
				var existing = loadCdcMeta(id);
				if (existing == null) {
					return OptionalLong.empty();
				}
				rememberCdcSubscription(id, existing);
				return OptionalLong.of(existing.lastCommittedSeq);
			});
		} catch (org.rocksdb.RocksDBException e) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
		} finally {
			ops.endOp();
		}
	}

	@Override
	public void cdcCommit(@NotNull String id, long seq) throws RocksDBException {
		Objects.requireNonNull(id, "id");
		ops.beginOp();
		try {
			commitCdcMetadata(id, seq);
		} catch (org.rocksdb.RocksDBException e) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
		} finally {
			ops.endOp();
		}
	}

	public CompletableFuture<Void> cdcCommitAsyncInternal(@NotNull String id, long seq) {
		return scheduleMustCompleteTracked(scheduler.scheduler(WorkloadProfile.CDC,
				OperationFamily.MUTATION,
				RequestContext.NO_DEADLINE), () -> {
			Objects.requireNonNull(id, "id");
			try {
				commitCdcMetadata(id, seq);
			} catch (org.rocksdb.RocksDBException error) {
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, error);
			}
			return (Void) null;
		}).toFuture();
	}

	private void commitCdcMetadata(String id, long seq) throws org.rocksdb.RocksDBException {
		withCdcMetadataLock("commit", id, () -> {
			var existing = loadCdcMeta(id);
			notifyCdcMetadataLoaded("commit", id);
			if (existing == null) {
				throw cdcSubscriptionNotFound(id);
			}
			long newCommitted = Math.max(existing.lastCommittedSeq, seq);
			var updated = new CdcSubscriptionMeta(newCommitted, existing.columnFilter, existing.emitLatestValues);
			saveCdcMeta(id, updated);
			rememberCdcSubscription(id, updated);
			return null;
		});
	}

	private static RocksDBException cdcSubscriptionNotFound(String id) {
		return RocksDBException.of(RocksDBErrorType.CDC_SUBSCRIPTION_NOT_FOUND,
				"CDC subscription not found: " + id);
	}

	private static final long CDC_HARD_MAX_EVENTS = 10_000;
	private static final long CDC_MAX_BYTES_PER_POLL = 16 * 1024 * 1024; // 16MB
	private static final String CDC_TAIL_REFRESH_STATUS = "Create a new iterator to fetch the new tail.";
	private static final String CDC_GAP_STATUS = "Gap in sequence numbers";
	private static final String CDC_REQUIRED_SEQUENCE_GAP_STATUS =
			"Gap in sequence number. Could not seek to required sequence number";
	private static final String CDC_START_SEQUENCE_MISSING_STATUS =
			"Start sequence was not found, skipping to the next available";

	private long captureCdcPollTail(long publishedTail) {
		var observer = cdcPollTailCapturedObserver;
		if (observer != null) {
			observer.run();
		}
		return publishedTail;
	}

	@VisibleForTesting
	protected WriteBatchIterator.Cursor createCdcWriteBatchCursor(WriteBatch writeBatch)
			throws org.rocksdb.RocksDBException {
		return WriteBatchIterator.cursor(writeBatch.data());
	}

	private static boolean checkCdcIteratorStatus(TransactionLogIterator iterator) throws org.rocksdb.RocksDBException {
		try {
			iterator.status();
			return false;
		} catch (org.rocksdb.RocksDBException error) {
			return handleCdcIteratorStatus(error);
		}
	}

	@VisibleForTesting
	protected static boolean handleCdcIteratorStatus(org.rocksdb.RocksDBException error)
			throws org.rocksdb.RocksDBException {
		var status = error.getStatus();
		var code = status != null ? status.getCode() : null;
		var state = status != null ? status.getState() : error.getMessage();

		// A concurrent append can move the WAL tail after this iterator was created. RocksDB
		// reports that snapshot boundary as TryAgain and requires the next poll to use a fresh
		// iterator; the prefix already collected by this poll is complete and safe to return.
		if (code == Code.TryAgain && CDC_TAIL_REFRESH_STATUS.equals(state)) {
			return true;
		}

		// These are the exact continuity-loss states emitted by RocksDB 11.1.2's
		// TransactionLogIteratorImpl. Do not classify unrelated Corruption/IOError statuses as
		// CDC gaps: those must remain operational failures rather than trigger a projection rebuild.
		boolean missingWalContinuity = (code == Code.NotFound && CDC_GAP_STATUS.equals(state))
				|| (code == Code.Corruption && (CDC_REQUIRED_SEQUENCE_GAP_STATUS.equals(state)
				|| CDC_START_SEQUENCE_MISSING_STATUS.equals(state)));
		if (missingWalContinuity) {
			throw new CdcGapDetectedException("Gap detected in WAL: " + state, error);
		}

		throw error;
	}

	private class EventCollector extends WriteBatch.Handler {

		private final List<CDCEvent> out;
		private final @Nullable it.unimi.dsi.fastutil.longs.LongSet filter;
		private final boolean preserveKeys;
		private long walSeq;
		private long firstOpIndex;
		private long skipBeforeSeq;
		private int seenOps = 0;
		private int produced = 0;
		private long accumulatedBytes = 0;
		private long scannedBytes = 0;
		private long maxToProduce;
		private long maxBytes;
		private long maxScannedBytes;
		private long quantumDeadlineNanos;
		private boolean rejectedLastMutation;
		private boolean allowOversizedFirstEvent;
		private boolean allowOversizedScannedMutation;

		boolean rejectedLastMutation() {
			return rejectedLastMutation;
		}

		long getAccumulatedBytes() {
			return accumulatedBytes;
		}

		long getScannedBytes() {
			return scannedBytes;
		}

		int getSeenOps() {
			return seenOps;
		}

		EventCollector(List<CDCEvent> out,
				@Nullable it.unimi.dsi.fastutil.longs.LongSet filter,
				boolean preserveKeys) {
			this.out = out;
			this.filter = filter;
			this.preserveKeys = preserveKeys;
		}

		void reset(long walSeq,
				long firstOpIndex,
				long skipBeforeSeq,
				long maxToProduce,
				long maxBytes,
				boolean allowOversizedFirstEvent,
				long maxScannedBytes,
				boolean allowOversizedScannedMutation,
				long quantumDeadlineNanos) {
			this.walSeq = walSeq;
			this.firstOpIndex = firstOpIndex;
			this.skipBeforeSeq = skipBeforeSeq;
			this.maxToProduce = maxToProduce;
			this.maxBytes = maxBytes;
			this.allowOversizedFirstEvent = allowOversizedFirstEvent;
			this.maxScannedBytes = maxScannedBytes;
			this.allowOversizedScannedMutation = allowOversizedScannedMutation;
			this.quantumDeadlineNanos = quantumDeadlineNanos;
			this.seenOps = 0;
			this.produced = 0;
			this.accumulatedBytes = 0;
			this.scannedBytes = 0;
			this.rejectedLastMutation = false;
		}

		@Override
		public boolean shouldContinue() {
			return !rejectedLastMutation
					&& produced < maxToProduce
					&& (seenOps == 0 || scannedBytes < maxScannedBytes)
					&& (seenOps == 0 || System.nanoTime() < quantumDeadlineNanos);
		}

		private void trackAndMaybeEmitByCfId(int columnFamilyId, byte[] key, @Nullable byte[] value, CDCEvent.Op op) {
			long mutationBytes = saturatingAdd(key.length, value != null ? value.length : 0L);
			if (!trackScannedMutation(mutationBytes)) {
				return;
			}

			long opIndex = firstOpIndex + seenOps - 1L;
			long seq = composeCdcSeq(walSeq, opIndex);
			if (seq < skipBeforeSeq) {
				return;
			}

			long colId = (long) columnFamilyId;
			var colInstance = EmbeddedDB.this.columns.get(colId);
			if (colInstance == null) {
				return;
			}
			if (filter != null && !filter.contains(colId)) {
				return;
			}

			byte[] finalKey = key;
			if (!preserveKeys && colInstance.hasBuckets()) {
				int fixedCount = colInstance.schema().fixedLengthKeysCount();
				int fixedBytes = 0;
				for (int i = 0; i < fixedCount; i++) {
					fixedBytes += colInstance.schema().key(i);
				}
				if (fixedBytes < finalKey.length) {
					finalKey = java.util.Arrays.copyOf(finalKey, fixedBytes);
				}
			}

			long eventSize = (long)finalKey.length + (value != null ? value.length : 0);
			// Check byte limit. Allow at least one event even if it exceeds limit to ensure progress.
			if ((!allowOversizedFirstEvent || produced > 0) && eventSize > maxBytes - accumulatedBytes) {
				rejectedLastMutation = true;
				return;
			}
			
			out.add(new CDCEvent(seq, colId, Buf.wrap(finalKey), value != null ? Buf.wrap(value) : emptyBuf(), op));
			produced++;
			accumulatedBytes += eventSize;
			EmbeddedDB.this.cdcEventsEmitted.increment();
			EmbeddedDB.this.cdcBytesEmitted.increment(finalKey.length + (value != null ? value.length : 0));
		}

		private boolean trackScannedMutation(long mutationBytes) {
			long remaining = Math.max(0L, maxScannedBytes - scannedBytes);
			if (mutationBytes > remaining && !(allowOversizedScannedMutation && seenOps == 0)) {
				rejectedLastMutation = true;
				return false;
			}
			seenOps++;
			scannedBytes = saturatingAdd(scannedBytes, mutationBytes);
			return true;
		}

		private void trackUnemittedMutation(long mutationBytes) {
			trackScannedMutation(mutationBytes);
		}


		@Override
		public void put(int cfId, byte[] key, byte[] value) {
			trackAndMaybeEmitByCfId(cfId, key, value, CDCEvent.Op.PUT);
		}

		@Override
		public void merge(int cfId, byte[] key, byte[] value) {
			trackAndMaybeEmitByCfId(cfId, key, value, CDCEvent.Op.MERGE);
		}

		@Override
		public void delete(int cfId, byte[] key) {
			trackAndMaybeEmitByCfId(cfId, key, null, CDCEvent.Op.DELETE);
		}

		@Override
		public void singleDelete(int cfId, byte[] key) {
			trackAndMaybeEmitByCfId(cfId, key, null, CDCEvent.Op.DELETE);
		}

		@Override
		public void putBlobIndex(int cfId, byte[] key, byte[] value) throws org.rocksdb.RocksDBException {
			trackAndMaybeEmitByCfId(cfId, key, value, CDCEvent.Op.PUT);
		}

		// DeleteRange must be counted as it consumes a sequence number
		@Override
		public void deleteRange(int cfId, byte[] beginKey, byte[] endKey) {
			trackUnemittedMutation(saturatingAdd(beginKey.length, endKey.length));
		}

		// Default CF methods - skip data but MUST count ops
		@Override
		public void put(byte[] key, byte[] value) {
			trackUnemittedMutation(saturatingAdd(key.length, value.length));
		}

		@Override
		public void merge(byte[] key, byte[] value) {
			trackUnemittedMutation(saturatingAdd(key.length, value.length));
		}

		@Override
		public void delete(byte[] key) {
			trackUnemittedMutation(key.length);
		}

		@Override
		public void singleDelete(byte[] key) {
			trackUnemittedMutation(key.length);
		}

		@Override
		public void deleteRange(byte[] beginKey, byte[] endKey) {
			trackUnemittedMutation(saturatingAdd(beginKey.length, endKey.length));
		}

		// WAL metadata and transaction markers are not included in WriteBatch.count(), so
		// they do not consume RocksDB sequence numbers and must not advance the CDC op index.
		@Override
		public void logData(byte[] blob) {}

		@Override
		public void markBeginPrepare() {}

		@Override
		public void markEndPrepare(byte[] xid) {}

		@Override
		public void markCommit(byte[] xid) {}

		@Override
		public void markRollback(byte[] xid) {}

		@Override
		public void markNoop(boolean emptyBatch) {}

		@Override
		public void markCommitWithTimestamp(byte[] xid, byte[] ts) {}
	}

	private CdcResolvedPage resolveLatestCdcValues(CdcSubscriptionMeta meta,
			List<CDCEvent> result,
			long maxEvents,
			long maxBytes,
			boolean allowOversizedFirstGroup) {
		if (!meta.emitLatestValues || result.isEmpty()) {
			return new CdcResolvedPage(result, cdcEventsBytes(result), null);
		}

		var transformed = new ArrayList<CDCEvent>(result.size());
		var keysToResolve = new ArrayList<byte[]>();
		var cfHandles = new ArrayList<ColumnFamilyHandle>();
		var indicesToResolve = new IntArrayList();
		var resolvedColumns = new ArrayList<ColumnInstance>();
		var columnUses = new HashMap<Long, ColumnInstance.ColumnUse>();

		try {
		for (int i = 0; i < result.size(); i++) {
			var event = result.get(i);
			var columnUse = columnUses.get(event.columnId());
			if (columnUse == null) {
				try {
					columnUse = acquireColumnUse(event.columnId());
					columnUses.put(event.columnId(), columnUse);
				} catch (RocksDBException error) {
					if (error.getErrorUniqueId() != RocksDBErrorType.COLUMN_NOT_FOUND) {
						throw error;
					}
				}
			}
			var column = columnUse != null ? columnUse.column() : null;
			if (column != null) {
				// Resolve every operation to the latest value to ensure monotonicity and
				// avoid "time travel" corruption.
				keysToResolve.add(event.key().asArray());
				cfHandles.add(column.cfh());
				resolvedColumns.add(column);
				indicesToResolve.add(i);
			}
		}

		if (keysToResolve.isEmpty()) {
			transformed.addAll(result);
			return new CdcResolvedPage(transformed, cdcEventsBytes(transformed), null);
		}

		var resolutionObserver = cdcLatestValueResolutionObserver;
		if (resolutionObserver != null) {
			resolutionObserver.run();
		}
		final List<byte[]> resolvedValues;
		try {
			resolvedValues = db.get().multiGetAsList(cfHandles, keysToResolve);
		} catch (org.rocksdb.RocksDBException error) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, error);
		}

		int resolutionIndex = 0;
		long transformedBytes = 0L;
		for (int i = 0; i < result.size(); i++) {
			var event = result.get(i);
			ColumnInstance column = null;
			Buf finalKey = event.key();
			var sequenceGroup = new ArrayList<CDCEvent>();
			if (resolutionIndex < indicesToResolve.size() && indicesToResolve.getInt(resolutionIndex) == i) {
				column = resolvedColumns.get(resolutionIndex);
				byte[] valueBytes = resolvedValues.get(resolutionIndex++);
				if (valueBytes != null) {
					if (column != null && column.hasBuckets()) {
						var bucketBytes = Buf.wrap(valueBytes);
						int elementCount = Bucket.readElementCount(bucketBytes);
						if (elementCount < 0 || elementCount > CDC_HARD_MAX_EVENTS) {
							throw RocksDBException.of(RocksDBErrorType.CDC_RESPONSE_TOO_LARGE,
									"Resolved CDC bucket at seq " + event.seq() + " contains " + elementCount
											+ " elements; the atomic sequence-group limit is " + CDC_HARD_MAX_EVENTS);
						}
						var elements = new Bucket(column, bucketBytes).getElements();
						int fixedCount = column.schema().fixedLengthKeysCount();
						int fixedBytes = 0;
						for (int k = 0; k < fixedCount; k++) {
							fixedBytes += column.schema().key(k);
						}
						Buf fixedPart = finalKey.subList(0, fixedBytes);
						for (var element : elements) {
							Buf[] variableKeys = element.getKey();
							long totalSize = fixedBytes;
							for (Buf variableKey : variableKeys) {
								totalSize += variableKey.size();
							}
							Buf realKey = Buf.createZeroes(Math.toIntExact(totalSize));
							realKey.setBytesFromBuf(0, fixedPart, 0, fixedBytes);
							int offset = fixedBytes;
							for (Buf variableKey : variableKeys) {
								int length = variableKey.size();
								realKey.setBytesFromBuf(offset, variableKey, 0, length);
								offset += length;
							}
							sequenceGroup.add(new CDCEvent(event.seq(),
									event.columnId(),
									realKey,
									element.getValue(),
									CDCEvent.Op.PUT));
						}
					} else {
						sequenceGroup.add(new CDCEvent(event.seq(),
								event.columnId(),
								finalKey,
								Buf.wrap(valueBytes),
								CDCEvent.Op.PUT));
					}
				} else {
					Buf deleteKey = finalKey;
					if (column != null && column.hasBuckets()) {
						int fixedCount = column.schema().fixedLengthKeysCount();
						int fixedBytes = 0;
						for (int k = 0; k < fixedCount; k++) {
							fixedBytes += column.schema().key(k);
						}
						if (fixedBytes < finalKey.size()) {
							deleteKey = Buf.wrap(Arrays.copyOf(finalKey.toByteArray(), fixedBytes));
						}
					}
					sequenceGroup.add(new CDCEvent(event.seq(),
							event.columnId(),
							deleteKey,
							emptyBuf(),
							CDCEvent.Op.DELETE));
				}
			} else {
				sequenceGroup.add(new CDCEvent(event.seq(),
						event.columnId(),
						finalKey,
						event.value(),
						event.op()));
			}

			long groupBytes = cdcEventsBytes(sequenceGroup);
			long remainingEvents = Math.max(0L, maxEvents - transformed.size());
			long remainingBytes = Math.max(0L, maxBytes - transformedBytes);
			boolean fits = sequenceGroup.size() <= remainingEvents && groupBytes <= remainingBytes;
			if (!fits && !(allowOversizedFirstGroup && transformed.isEmpty())) {
				return new CdcResolvedPage(transformed, transformedBytes, event.seq());
			}
			transformed.addAll(sequenceGroup);
			transformedBytes = saturatingAdd(transformedBytes, groupBytes);
		}
		return new CdcResolvedPage(transformed, transformedBytes, null);
		} finally {
			for (var columnUse : columnUses.values()) {
				columnUse.close();
			}
		}
	}

	private static long cdcEventsBytes(List<CDCEvent> events) {
		long bytes = 0L;
		for (var event : events) {
			bytes = saturatingAdd(bytes, event.key().size());
			if (event.value() != null) {
				bytes = saturatingAdd(bytes, event.value().size());
			}
		}
		return bytes;
	}

	private CdcPollCursor openCdcPollCursor(CdcSubscriptionMeta subscription,
			long startSeq,
			long maxWalSequenceInclusive) throws org.rocksdb.RocksDBException {
		var cursor = new CdcPollCursor(subscription, startSeq, maxWalSequenceInclusive);
		try {
			activeCdcPollCursors.add(cursor);
			cursor.scheduleExpiry();
			return cursor;
		} catch (RuntimeException | Error error) {
			cursor.close();
			throw error;
		}
	}

	/**
	 * One fixed-tail CDC view. Event-only async polls retain this cursor across
	 * bounded scheduler slices so fairness does not require repeatedly seeking the
	 * WAL or rebuilding the subscription filter.
	 */
	private final class CdcPollCursor implements AutoCloseable {

		private final CdcSubscriptionMeta subscription;
		private final it.unimi.dsi.fastutil.longs.LongSet filter;
		private final long initialExternalSequence;
		private final long initialRocksSequence;
		private final long maxWalSequenceInclusive;
		private final long maximumAgeDeadlineNanos;
		private final ReentrantLock cursorLock = new ReentrantLock();
		private final AtomicBoolean closeRequested = new AtomicBoolean();
		private volatile @Nullable ScheduledFuture<?> expiryTask;
		private TransactionLogIterator iterator;
		private @Nullable WriteBatchIterator.Cursor batchCursor;
		private long batchWalSequence;
		private long nextSeq;
		private boolean firstBatch = true;
		private volatile boolean exhausted;
		private volatile boolean closed;
		private volatile boolean expired;

		private CdcPollCursor(CdcSubscriptionMeta subscription,
				long startSeq,
				long maxWalSequenceInclusive) throws org.rocksdb.RocksDBException {
			this.subscription = subscription;
			this.filter = subscription.columnFilter == null
					? null
					: new it.unimi.dsi.fastutil.longs.LongOpenHashSet(subscription.columnFilter);
			this.initialExternalSequence = startSeq;
			this.initialRocksSequence = extractCdcRocksSequence(startSeq);
			this.maxWalSequenceInclusive = maxWalSequenceInclusive;
			this.nextSeq = startSeq;
			long createdNanos = System.nanoTime();
			long maximumAgeNanos = TimeUnit.MILLISECONDS.toNanos(maxRetainedSnapshotAgeMs);
			this.maximumAgeDeadlineNanos = maximumAgeNanos >= Long.MAX_VALUE - createdNanos
					? Long.MAX_VALUE
					: createdNanos + maximumAgeNanos;

			if (initialRocksSequence > maxWalSequenceInclusive) {
				exhausted = true;
				return;
			}

			var iteratorObserver = cdcWalIteratorOpenObserver;
			if (iteratorObserver != null) {
				iteratorObserver.run();
			}
			try {
				// A composed CDC cursor can point inside one WAL batch. RocksDB seeks
				// transaction-log iterators by the batch's first sequence, so reopen the
				// containing batch and let EventCollector skip the earlier op indices.
				iterator = db.get().getUpdatesSince(extractCdcWalSeq(startSeq));
			} catch (org.rocksdb.RocksDBException error) {
				if (error.getMessage() != null
						&& error.getMessage().contains("Requested sequence not yet written")) {
					exhausted = true;
					return;
				}
				throw error;
			}
		}

		private void scheduleExpiry() {
			long remainingNanos = Math.max(0L, maximumAgeDeadlineNanos - System.nanoTime());
			expiryTask = leakScheduler.schedule(this::expire, remainingNanos, TimeUnit.NANOSECONDS);
		}

		private void expire() {
			expired = true;
			close();
		}

		private CdcPollPage readPage(long maxEvents,
				long maxBytes,
				boolean allowOversizedFirstEvent,
				long maxScannedMutations,
				long maxScannedBytes,
				long maximumDurationNanos) {
			cursorLock.lock();
			try {
				return readPageLocked(maxEvents,
						maxBytes,
						allowOversizedFirstEvent,
						maxScannedMutations,
						maxScannedBytes,
						maximumDurationNanos);
			} finally {
				if (closeRequested.get()) {
					closeLocked();
				}
				cursorLock.unlock();
			}
		}

		private CdcPollPage readPageLocked(long maxEvents,
				long maxBytes,
				boolean allowOversizedFirstEvent,
				long maxScannedMutations,
				long maxScannedBytes,
				long maximumDurationNanos) {
			if (expired || System.nanoTime() >= maximumAgeDeadlineNanos) {
				expired = true;
				throw cdcCursorDeadlineExceeded();
			}
			if (closed || closeRequested.get()) {
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "CDC poll cursor is closed");
			}
			if (exhausted || iterator == null) {
				return emptyPage();
			}

			long effectiveMax = Math.min(maxEvents > 0 ? maxEvents : CDC_DEFAULT_MAX_EVENTS,
					CDC_HARD_MAX_EVENTS);
			List<CDCEvent> result = new ArrayList<>((int) Math.min(1_024L, effectiveMax));
			long accumulatedBytes = 0L;
			long scannedMutations = 0L;
			long scannedBytes = 0L;
			long advancedMutations = 0L;
			long quantumStartNanos = System.nanoTime();
			long durationDeadlineNanos = maximumDurationNanos >= Long.MAX_VALUE - quantumStartNanos
					? Long.MAX_VALUE
					: quantumStartNanos + maximumDurationNanos;
			long quantumDeadlineNanos = Math.min(durationDeadlineNanos, maximumAgeDeadlineNanos);

			try (var handler = new EventCollector(result, filter, subscription.emitLatestValues)) {
				while (result.size() < effectiveMax
						&& scannedMutations < maxScannedMutations
						&& scannedBytes < maxScannedBytes) {
					if (scannedMutations > 0L && System.nanoTime() >= quantumDeadlineNanos) {
						break;
					}
					if (batchCursor == null) {
						if (!iterator.isValid()) {
							break;
						}
						var batchResult = iterator.getBatch();
						try (var writeBatch = batchResult.writeBatch()) {
							batchWalSequence = batchResult.sequenceNumber();
							if (firstBatch) {
								if (batchWalSequence > initialRocksSequence) {
									throw new CdcGapDetectedException("Gap detected in WAL. Requested RocksDB sequence: "
											+ initialRocksSequence + ", but earliest available is: " + batchWalSequence
											+ " (external cursor " + initialExternalSequence + ")");
								}
								firstBatch = false;
							}
							if (batchWalSequence > maxWalSequenceInclusive) {
								exhausted = true;
								break;
							}
							try {
								batchCursor = createCdcWriteBatchCursor(writeBatch);
							} catch (Exception error) {
								throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR,
										"Failed to parse WriteBatch at seq " + batchWalSequence,
										error);
							}
						}
					}

					var currentBatch = Objects.requireNonNull(batchCursor);
					long firstOpIndex = currentBatch.recordsRead();
					handler.reset(batchWalSequence,
							firstOpIndex,
							nextSeq,
							effectiveMax - result.size(),
							maxBytes - accumulatedBytes,
							allowOversizedFirstEvent && result.isEmpty(),
							maxScannedBytes - scannedBytes,
							scannedMutations == 0L,
							quantumDeadlineNanos);
					long remainingScanBudget = maxScannedMutations - scannedMutations;
					int sliceRecords = (int) Math.min(Integer.MAX_VALUE, remainingScanBudget);
					try {
						currentBatch.iterate(handler, sliceRecords);
					} catch (Exception error) {
						throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR,
								"Failed to parse WriteBatch at seq " + batchWalSequence,
								error);
					}

					accumulatedBytes += handler.getAccumulatedBytes();
					scannedMutations += handler.getSeenOps();
					scannedBytes = saturatingAdd(scannedBytes, handler.getScannedBytes());
					if (handler.rejectedLastMutation()) {
						currentBatch.rewindLastRecord();
					}
					advancedMutations = saturatingAdd(advancedMutations,
							Math.max(0L, currentBatch.recordsRead() - firstOpIndex));
					boolean batchFinished = currentBatch.isFinished();
					long parsedNextSeq = batchFinished
							? composeCdcSeq(Math.addExact(batchWalSequence, currentBatch.recordsRead()), 0L)
							: composeCdcSeq(batchWalSequence, currentBatch.recordsRead());
					nextSeq = Math.max(nextSeq, parsedNextSeq);

					if (handler.rejectedLastMutation()
							|| result.size() >= effectiveMax
							|| (accumulatedBytes >= maxBytes && !result.isEmpty())
							|| scannedMutations >= maxScannedMutations
							|| scannedBytes >= maxScannedBytes) {
						break;
					}

					if (batchFinished) {
						batchCursor = null;
						iterator.next();
					} else {
						// The handler stopped before the scan budget. Its event/byte boundary
						// is the continuation boundary for this logical result.
						break;
					}
				}

				if (batchCursor == null && !iterator.isValid()) {
					try {
						checkCdcIteratorStatus(iterator);
					} catch (org.rocksdb.RocksDBException error) {
						throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, error);
					}
					exhausted = true;
				}
				if (!exhausted && (expired || System.nanoTime() >= maximumAgeDeadlineNanos)) {
					expired = true;
					throw cdcCursorDeadlineExceeded();
				}
			} catch (RocksDBException error) {
				throw error;
			} catch (Exception error) {
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, error);
			}

			var mutationsObserver = cdcQuantumMutationsObserver;
			if (mutationsObserver != null) {
				mutationsObserver.accept(scannedMutations);
			}
			var bytesObserver = cdcQuantumBytesObserver;
			if (bytesObserver != null) {
				bytesObserver.accept(scannedBytes);
			}
			var resolvedPage = resolveLatestCdcValues(subscription,
					result,
					effectiveMax,
					maxBytes,
					allowOversizedFirstEvent);
			if (expired || System.nanoTime() >= maximumAgeDeadlineNanos) {
				expired = true;
				throw cdcCursorDeadlineExceeded();
			}
			if (resolvedPage.continuationSeq() != null) {
				// The native parser may already be ahead because latest-value expansion is
				// evaluated after a batched multi-get. End this logical cursor at the first
				// complete sequence group that did not fit; the next poll can reopen exactly
				// there without truncating or losing bucket siblings.
				nextSeq = resolvedPage.continuationSeq();
				exhausted = true;
			}
			return new CdcPollPage(new CdcBatch(resolvedPage.events(), nextSeq),
					resolvedPage.events().size(),
					resolvedPage.emittedBytes(),
					advancedMutations);
		}

		private CdcPollPage emptyPage() {
			return new CdcPollPage(new CdcBatch(Collections.emptyList(), nextSeq), 0L, 0L, 0L);
		}

		private RocksDBException cdcCursorDeadlineExceeded() {
			return RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
					"CDC poll cursor exceeded retained resource maximum age");
		}

		private boolean isExhausted() {
			return !expired && (exhausted || closed);
		}

		@Override
		public void close() {
			closeRequested.set(true);
			if (!cursorLock.tryLock()) {
				// A running native slice owns the cursor and will close it in its
				// finally block. Shutdown must remain free to enforce its own timeout.
				return;
			}
			try {
				closeLocked();
			} finally {
				cursorLock.unlock();
			}
		}

		private void closeLocked() {
			if (closed) {
				return;
			}
			closed = true;
			exhausted = true;
			batchCursor = null;
			if (iterator != null) {
				iterator.close();
				iterator = null;
			}
			var scheduledExpiry = expiryTask;
			if (scheduledExpiry != null) {
				scheduledExpiry.cancel(false);
			}
			activeCdcPollCursors.remove(this);
		}
	}

	@Override
	public @NotNull java.util.stream.Stream<CDCEvent> cdcPoll(@NotNull String id, @Nullable Long fromSeq, long maxEvents)
			throws RocksDBException {
		ops.beginOp();
		try {
			long requestedEvents = normalizeCdcMaxEvents(maxEvents);
			long startSeq;
			long maxWalSequenceInclusive;
			CdcSubscriptionMeta meta;
			try {
				meta = loadCdcMeta(id);
				if (meta == null) {
					throw cdcSubscriptionNotFound(id);
				}
				if (fromSeq != null && fromSeq == 0L) {
					var discovery = findEarliestAvailableWal();
					startSeq = composeCdcSeq(discovery.earliestWalSequence(), 0);
					maxWalSequenceInclusive = captureCdcPollTail(discovery.publishedTailSequence());
				} else {
					// Publish the application WAL buffer once before fixing this logical
					// poll's tail. Continuation pages must neither flush nor chase appends.
					var publication = publishCdcWal();
					startSeq = fromSeq != null ? fromSeq : meta.lastCommittedSeq + 1;
					maxWalSequenceInclusive = captureCdcPollTail(publication.publishedTailSequence());
				}
			} catch (org.rocksdb.RocksDBException e) {
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
			}
			var events = new ArrayList<CDCEvent>((int) Math.min(1_024L, requestedEvents));
			long pageStart = startSeq;
			long remainingEvents = requestedEvents;
			long remainingBytes = CDC_MAX_BYTES_PER_POLL;
			boolean allowOversizedFirstEvent = true;
			var progress = rememberCdcSubscription(id, meta);
			try (var cursor = openCdcPollCursor(meta, startSeq, maxWalSequenceInclusive)) {
				while (remainingEvents > 0L && (allowOversizedFirstEvent || remainingBytes > 0L)) {
					var page = cursor.readPage(remainingEvents,
							Math.min(remainingBytes, workloadSettings.cdcQuantumMaxBytes()),
							allowOversizedFirstEvent,
							workloadSettings.cdcQuantumMaxMutations(),
							workloadSettings.cdcQuantumMaxBytes(),
							workloadSettings.cdcQuantumMaxDuration().toNanos());
					var batch = page.batch();
				recordCdcCursorProgress(progress, batch.nextSeq());
					long pageBytes = page.emittedBytes();
					events.addAll(batch.events());
					remainingEvents = Math.max(0L, remainingEvents - page.emittedEvents());
					remainingBytes = pageBytes >= remainingBytes ? 0L : remainingBytes - pageBytes;
					allowOversizedFirstEvent &= page.emittedEvents() == 0L;
					long nextSeq = batch.nextSeq();
					if (cursor.isExhausted()
							|| extractCdcRocksSequence(nextSeq) > maxWalSequenceInclusive
							|| nextSeq == pageStart && page.advancedMutations() == 0L) {
						break;
					}
					pageStart = nextSeq;
				}
			} catch (org.rocksdb.RocksDBException error) {
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, error);
			}
			return events.stream();
		} finally {
			ops.endOp();
		}
	}

	public @NotNull org.reactivestreams.Publisher<CDCEvent> cdcPollAsyncInternal(@NotNull String id,
			@Nullable Long fromSeq,
			long maxEvents) throws RocksDBException {
		long requestedEvents = normalizeCdcMaxEvents(maxEvents);
		return prepareCdcPollStartAsync(id, fromSeq)
				.flatMapMany(window -> cdcPollPages(window, requestedEvents)
						.concatMapIterable(page -> page.page().batch().events(), 1));
	}

	public @NotNull Mono<CdcBatch> cdcPollBatchAsyncInternal(@NotNull String id, @Nullable Long fromSeq, long maxEvents)
			throws RocksDBException {
		long requestedEvents = normalizeCdcMaxEvents(maxEvents);
		return prepareCdcPollStartAsync(id, fromSeq)
				.flatMap(window -> cdcPollPages(window, requestedEvents)
						.collect(() -> new CdcBatchAccumulator(window.startSeq(), requestedEvents),
								CdcBatchAccumulator::add)
						.map(CdcBatchAccumulator::toBatch))
				// Mapping and emitting a materialized CDC page must not retain a scarce
				// RocksDB read worker after the bounded poll step has completed.
				.publishOn(reactor.core.scheduler.Schedulers.parallel());
	}

	private Flux<CdcStreamPage> cdcPollPages(CdcPollWindow window, long requestedEvents) {
		return Flux.usingWhen(
				openAndReadCdcPollCursorAsync(window,
						requestedEvents,
						CDC_MAX_BYTES_PER_POLL,
						true),
				cursorStart -> {
					var cursor = cursorStart.cursor();
					return Mono.just(new CdcStreamPage(window.startSeq(),
							requestedEvents,
							CDC_MAX_BYTES_PER_POLL,
							true,
							cursorStart.firstPage()))
							.expand(page -> {
								long emitted = page.page().emittedEvents();
								long remainingEvents = Math.max(0L, page.remainingEvents() - emitted);
								long emittedBytes = page.page().emittedBytes();
								long remainingBytes = emittedBytes >= page.remainingBytes()
										? 0L
										: page.remainingBytes() - emittedBytes;
								long nextSeq = page.page().batch().nextSeq();
								if (remainingEvents == 0L
										|| (remainingBytes == 0L && emitted > 0L)
										|| cursor.isExhausted()
									|| extractCdcRocksSequence(nextSeq) > window.maxWalSequenceInclusive()
										|| nextSeq == page.startSeq()
										&& page.page().advancedMutations() == 0L) {
									return Mono.empty();
								}
								boolean allowOversizedFirstEvent = page.allowOversizedFirstEvent()
										&& emitted == 0L;
								return readCdcPollCursorPageAsync(cursor,
										remainingEvents,
										remainingBytes,
										allowOversizedFirstEvent)
										.map(nextPage -> new CdcStreamPage(nextSeq,
												remainingEvents,
												remainingBytes,
												allowOversizedFirstEvent,
												nextPage));
							}, 1);
				},
				cursorStart -> closeCdcPollCursorAsync(cursorStart.cursor()),
				(cursorStart, _) -> closeCdcPollCursorAsync(cursorStart.cursor()),
				cursorStart -> closeCdcPollCursorAsync(cursorStart.cursor()))
				.doOnNext(page -> recordCdcCursorProgress(window.progress(),
						page.page().batch().nextSeq()));
	}

	private static final class CdcBatchAccumulator {
		private final List<CDCEvent> events;
		private long nextSeq;

		private CdcBatchAccumulator(long startSeq, long requestedEvents) {
			this.nextSeq = startSeq;
			this.events = new ArrayList<>((int) Math.min(1_024L, requestedEvents));
		}

		private void add(CdcStreamPage page) {
			var batch = page.page().batch();
			events.addAll(batch.events());
			nextSeq = batch.nextSeq();
		}

		private CdcBatch toBatch() {
			return new CdcBatch(events, nextSeq);
		}
	}

	private static long normalizeCdcMaxEvents(long maxEvents) {
		long requested = maxEvents > 0 ? maxEvents : CDC_DEFAULT_MAX_EVENTS;
		return Math.min(requested, CDC_HARD_MAX_EVENTS);
	}

	private Mono<CdcPollWindow> prepareCdcPollStartAsync(@NotNull String id, @Nullable Long fromSeq) {
		return scheduleTracked(this.scheduler.cdc(), () -> {
			var meta = loadCdcMeta(id);
			if (meta == null) {
				throw cdcSubscriptionNotFound(id);
			}
			return meta;
		}).flatMap(meta -> {
			var progress = rememberCdcSubscription(id, meta);
			if (fromSeq != null && fromSeq == 0L) {
				return findEarliestAvailableWalAsync()
						.map(discovery -> new CdcPollWindow(
								composeCdcSeq(discovery.earliestWalSequence(), 0),
								captureCdcPollTail(discovery.publishedTailSequence()),
								meta,
								progress));
			}
			long startSeq = fromSeq != null ? fromSeq : meta.lastCommittedSeq + 1;
			return publishCdcWalAsync()
					.map(publication -> new CdcPollWindow(startSeq,
							captureCdcPollTail(publication.publishedTailSequence()),
							meta,
							progress));
		});
	}

	private Mono<CdcPollCursorStart> openAndReadCdcPollCursorAsync(CdcPollWindow window,
			long maxEvents,
			long maxBytes,
			boolean allowOversizedFirstEvent) {
		return scheduleTracked(this.scheduler.cdc(), () -> {
			var cursor = openCdcPollCursor(window.subscription(),
					window.startSeq(),
					window.maxWalSequenceInclusive());
			try {
				return new CdcPollCursorStart(cursor,
							cursor.readPage(maxEvents,
									Math.min(maxBytes, workloadSettings.cdcQuantumMaxBytes()),
									allowOversizedFirstEvent,
									workloadSettings.cdcQuantumMaxMutations(),
									workloadSettings.cdcQuantumMaxBytes(),
									workloadSettings.cdcQuantumMaxDuration().toNanos()));
			} catch (RuntimeException | Error error) {
				cursor.close();
				throw error;
			}
		}, cursorStart -> cursorStart.cursor().close());
	}

	private Mono<CdcPollPage> readCdcPollCursorPageAsync(CdcPollCursor cursor,
			long maxEvents,
			long maxBytes,
			boolean allowOversizedFirstEvent) {
		var continuationObserver = cdcContinuationObserver;
		if (continuationObserver != null) {
			continuationObserver.run();
		}
		return scheduleTracked(this.scheduler.cdc(),
				() -> cursor.readPage(maxEvents,
						Math.min(maxBytes, workloadSettings.cdcQuantumMaxBytes()),
						allowOversizedFirstEvent,
						workloadSettings.cdcQuantumMaxMutations(),
						workloadSettings.cdcQuantumMaxBytes(),
						workloadSettings.cdcQuantumMaxDuration().toNanos()));
	}

	private Mono<Void> closeCdcPollCursorAsync(CdcPollCursor cursor) {
		return scheduleTracked(this.scheduler.control(), () -> {
			cursor.close();
			return (Void) null;
		}).onErrorResume(_ -> {
			// SafeShutdown may already be closed, or the control scheduler may reject
			// during teardown. The cursor must still release its native handle.
			cursor.close();
			return Mono.empty();
		});
	}

	/**
	 * Schedule a native database step while keeping SafeShutdown accounting tied to
	 * actual task completion. Cancellation stops delivery to the subscriber but does
	 * not pretend that an already queued or running JNI call has finished.
	 */
	private <T> Mono<T> scheduleTracked(reactor.core.scheduler.Scheduler target, Callable<T> callable) {
		return scheduleTracked(target, callable, null);
	}

	private <T> Mono<T> scheduleTracked(reactor.core.scheduler.Scheduler target,
			Callable<T> callable,
			@Nullable Consumer<? super T> lateSuccessCleanup) {
		return Mono.create(sink -> {
			final int queued = 0;
			final int running = 1;
			final int finished = 2;
			final int cancelledBeforeStart = 3;
			var emissionLock = new Object();
			var cancelled = new AtomicBoolean();
			var state = new AtomicInteger(queued);
			var task = Disposables.swap();
			try {
				ops.beginOp();
			} catch (Throwable error) {
				sink.error(error);
				return;
			}
			sink.onCancel(() -> {
				synchronized (emissionLock) {
					cancelled.set(true);
				}
				if (state.compareAndSet(queued, cancelledBeforeStart)) {
					// The queued callable will never own the SafeShutdown lease.
					task.dispose();
					ops.endOp();
				} else {
					// A running native call owns the lease until its finally block.
					task.dispose();
				}
			});
			try {
				task.replace(target.schedule(() -> {
					if (!state.compareAndSet(queued, running)) {
						return;
					}
					try {
						var result = callable.call();
						boolean late;
						synchronized (emissionLock) {
							late = cancelled.get();
							if (!late) {
								sink.success(result);
							}
						}
						if (late && lateSuccessCleanup != null) {
							try {
								lateSuccessCleanup.accept(result);
							} catch (Throwable cleanupError) {
								logger.warn("Failed to clean a CDC native result after subscriber cancellation",
										cleanupError);
							}
						}
					} catch (Throwable error) {
						boolean late;
						synchronized (emissionLock) {
							late = cancelled.get();
							if (!late) {
								sink.error(error);
							}
						}
						if (late) {
							logger.debug("CDC native task failed after subscriber cancellation", error);
						}
					} finally {
						state.set(finished);
						ops.endOp();
					}
				}));
			} catch (Throwable error) {
				if (state.compareAndSet(queued, finished)) {
					ops.endOp();
					boolean late;
					synchronized (emissionLock) {
						late = cancelled.get();
						if (!late) {
							sink.error(error);
						}
					}
					if (late) {
						logger.debug("CDC task scheduling failed after subscriber cancellation", error);
					}
				}
			}
		});
	}

	/**
	 * A protected mutation owns its shutdown lease from acceptance through completion.
	 * Subscriber cancellation suppresses delivery but never removes the queued task.
	 */
	private <T> Mono<T> scheduleMustCompleteTracked(Scheduler target, Callable<T> callable) {
		return Mono.create(sink -> {
			var emissionLock = new Object();
			var cancelled = new AtomicBoolean();
			try {
				ops.beginOp();
			} catch (Throwable error) {
				sink.error(error);
				return;
			}
			sink.onCancel(() -> {
				synchronized (emissionLock) {
					cancelled.set(true);
				}
			});
			try {
				target.schedule(() -> {
					try {
						var result = callable.call();
						synchronized (emissionLock) {
							if (!cancelled.get()) {
								sink.success(result);
							}
						}
					} catch (Throwable error) {
						synchronized (emissionLock) {
							if (!cancelled.get()) {
								sink.error(error);
							} else {
								logger.warn("Must-complete CDC task failed after subscriber cancellation", error);
							}
						}
					} finally {
						ops.endOp();
					}
				});
			} catch (Throwable error) {
				ops.endOp();
				synchronized (emissionLock) {
					if (!cancelled.get()) {
						sink.error(error);
					} else {
						logger.warn("Must-complete CDC task was rejected after subscriber cancellation", error);
					}
				}
			}
		});
	}
	private void printStartupInfo() {
		if (!Boolean.parseBoolean(System.getProperty("rockserver.core.print-config", "true"))) {
			return;
		}

		StringBuilder sb = new StringBuilder();

		// Columns
		sb.append("\n");
		ConsoleTable columnsTable = new ConsoleTable();
		columnsTable.setHeaders("ID", "Name", "Schema", "Merge Operator");

		var sortedCols = new ArrayList<>(this.columns.values());
		sortedCols.sort(Comparator.comparingLong(c -> c.cfh().getID()));

		for (var col : sortedCols) {
			long id = col.cfh().getID();
			String name;
			try {
				name = new String(col.cfh().getName(), StandardCharsets.UTF_8);
			} catch (Exception e) {
				name = "???";
			}
			String schema = formatSchema(col.schema());
			String mergeOp = formatMergeOp(col);

			columnsTable.addRow(String.valueOf(id), name, schema, mergeOp);
		}

		// Show unconfigured columns (exist in RocksDB but have no stored schema)
		for (var entry : unconfiguredColumns.entrySet()) {
			long id = entry.getValue().getID();
			columnsTable.addRow(String.valueOf(id), entry.getKey(), "<unconfigured>", "-");
		}

		sb.append(columnsTable.toString());

		// Merge Operators
		sb.append("\n");
		ConsoleTable opsTable = new ConsoleTable();
		opsTable.setHeaders("Name", "Version", "Class");

		var ops = mergeOperatorRegistry.listAll();
		ops.sort(Comparator.comparing(MergeOperatorRegistry.MergeOperatorInfo::name).thenComparingLong(MergeOperatorRegistry.MergeOperatorInfo::version));

		for (var op : ops) {
			opsTable.addRow(op.name(), String.valueOf(op.version()), op.className());
		}
		sb.append(opsTable.toString());

		// CDC Subscriptions
		sb.append("\n");
		ConsoleTable cdcTable = new ConsoleTable();
		cdcTable.setHeaders("ID", "Seq", "Resolved", "Filter (Col IDs)");

		try (var it = this.db.get().newIterator(cdcMetaColumnDescriptorHandle)) {
			it.seekToFirst();
			while (it.isValid()) {
				var keyBytes = it.key();
				String key = new String(keyBytes, StandardCharsets.UTF_8);
				if (key.startsWith("sub:")) {
					String id = key.substring(4);
					CdcSubscriptionMeta meta = decodeCdcMeta(it.value());
					if (meta != null) {
						String filterStr = "-";
						if (meta.columnFilter != null) {
							StringBuilder fs = new StringBuilder();
							for(long colId : meta.columnFilter) {
								if (fs.length() > 0) fs.append(",");
								fs.append(colId);
							}
							filterStr = fs.toString();
						}
						cdcTable.addRow(id, String.valueOf(meta.lastCommittedSeq), String.valueOf(meta.emitLatestValues), filterStr);
					}
				}
				it.next();
			}
		} catch (IOException e) {
			logger.warn("Failed to read CDC metadata for startup info", e);
		}
		sb.append(cdcTable.toString());

		logger.info("Startup Information:{}", sb.toString());
	}

	private String formatSchema(ColumnSchema s) {
		StringBuilder sb = new StringBuilder();
		sb.append("Fixed:").append(s.fixedLengthKeysCount());
		if (!s.variableTailKeys().isEmpty()) {
			sb.append(", Var:[");
			for (int i=0; i<s.variableTailKeys().size(); i++) {
				if (i>0) sb.append(",");
				sb.append(s.variableTailKeys().get(i).name());
			}
			sb.append("]");
		}
		sb.append(", Val:").append(s.hasValue() ? "Y" : "N");
		return sb.toString();
	}

	private String formatMergeOp(ColumnInstance col) {
		var s = col.schema();
		if (s.mergeOperatorName() != null) {
			return s.mergeOperatorName() + " (v" + s.mergeOperatorVersion() + ")";
		}
		if (s.mergeOperatorClass() != null) {
			String cls = s.mergeOperatorClass();
			return cls.substring(cls.lastIndexOf('.') + 1);
		}
		if (col.mergeOperator() != null) {
			return "Configured (" + col.mergeOperator().getClass().getSimpleName() + ")";
		}
		return "-";
	}
}
