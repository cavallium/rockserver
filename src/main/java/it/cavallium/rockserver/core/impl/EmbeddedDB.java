package it.cavallium.rockserver.core.impl;

import it.cavallium.rockserver.core.common.SstMaintenance;

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
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Stream;

import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.reactivestreams.Publisher;
import org.rocksdb.AbstractSlice;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.PinnedMultiGet;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Status.Code;
import org.rocksdb.Transaction;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.Retry;
import org.rocksdb.WriteBatch;

import it.cavallium.rockserver.core.impl.EmbeddedDBStorageMetrics.*;
import it.cavallium.rockserver.core.impl.EmbeddedDBWriteElision.*;
import it.cavallium.rockserver.core.impl.EmbeddedDBExistsMulti.*;
import it.cavallium.rockserver.core.impl.EmbeddedDBIterators.*;
import it.cavallium.rockserver.core.impl.EmbeddedDBRanges.*;
import it.cavallium.rockserver.core.impl.EmbeddedDBRawScan.*;

public class EmbeddedDB implements RocksDBSyncAPI, InternalConnection, Closeable {

	static VarHandle varHandle(Class<?> owner, String field, Class<?> type) {
		try {
			// The owners are nested classes of EmbeddedDB and of its package-private parts (EmbeddedDBRanges, ...);
			// a private lookup in the owner reaches their private fields across those classes.
			return MethodHandles.privateLookupIn(owner, MethodHandles.lookup()).findVarHandle(owner, field, type);
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

		FastGetResult(boolean present,
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

	static final int PINNED_GET_MIN_BYTES_OVERRIDE = Integer.getInteger(
			"rockserver.fast-get.pinned-min-bytes", -1);

	private static final String STORAGE_PRESSURE_PENDING_COMPACTION_BYTES_PROPERTY =
			"it.cavallium.rockserver.workload.storage-pressure-pending-compaction-bytes";
	static final long STORAGE_PRESSURE_PENDING_COMPACTION_BYTES_OVERRIDE =
			storagePressurePendingCompactionBytesOverride();
	static final String ROCKSDB_IS_WRITE_STOPPED_PROPERTY =
			RocksDBLongProperty.IS_WRITE_STOPPED.getName();
	static final String ROCKSDB_ACTUAL_DELAYED_WRITE_RATE_PROPERTY =
			RocksDBLongProperty.ACTUAL_DELAYED_WRITE_RATE.getName();
	static final String ROCKSDB_ESTIMATE_PENDING_COMPACTION_BYTES_PROPERTY =
			RocksDBLongProperty.ESTIMATE_PENDING_COMPACTION_BYTES.getName();
	static final StoragePressureColumn[] EMPTY_STORAGE_PRESSURE_COLUMNS = new StoragePressureColumn[0];
	static final int EXISTS_MULTI_MAX_KEYS_PER_NATIVE_CALL = 4_096;
	static final long EXISTS_MULTI_MAX_KEY_BYTES_PER_NATIVE_CALL = 2 * SizeUnit.MB;
	static final int EXISTS_MULTI_MAX_VARIABLE_HASH_BYTES = Arrays.stream(ColumnHashType.values())
			.mapToInt(ColumnHashType::bytesSize)
			.max()
			.orElse(0);
	static final int WRITE_ELISION_MAX_KEYS_PER_NATIVE_CALL = 4_096;
	static final long WRITE_ELISION_MAX_PROBE_BYTES_PER_NATIVE_CALL = 2 * SizeUnit.MB;
	static final int RAW_SCAN_MAX_ENTRIES_PER_CHUNK = 65_536;
	static final long RAW_SCAN_MAX_BYTES_PER_CHUNK = 2 * SizeUnit.MB;
	// A logical raw scan spans many independently scheduled SST readers. Once its
	// immutable snapshot is pinned, transient queue pressure between files must pause
	// that scan rather than tear down and recapture the multi-terabyte snapshot.
	static final Retry RAW_SCAN_ADMISSION_RETRY = Retry
			.backoff(Long.MAX_VALUE, Duration.ofMillis(10))
			.maxBackoff(Duration.ofSeconds(1))
			.jitter(0.5d)
			.filter(EmbeddedDBRawScan::isRetryableRawScanAdmissionOverload);
	private static final long RAW_SCAN_PIN_MAX_DURATION_NANOS = TimeUnit.MILLISECONDS.toNanos(Math.max(1L,
			Long.getLong("it.cavallium.rockserver.raw-scan.pin-max-duration-ms", 120_000L)));
	static final String RAW_SCAN_PIN_DIRECTORY_NAME = ".rockserver-raw-scan-pins";
	public static final long MAX_TRANSACTION_DURATION_MS = 10_000L;
	private static final String SHUTDOWN_PENDING_OPS_TIMEOUT_MS_PROPERTY
			= "it.cavallium.rockserver.db.shutdown-pending-ops-timeout-ms";
	static final byte[] COLUMN_SCHEMAS_COLUMN = "_column_schemas_".getBytes(StandardCharsets.UTF_8);
	static final byte[] MERGE_OPERATORS_COLUMN = "_merge_operators_".getBytes(StandardCharsets.UTF_8);
	/**
	 * Column family of the removed CDC feature. Databases created by older versions still contain it, and RocksDB
	 * must open every column family: it is opened if present, never created, and never read or written.
	 */
	private static final byte[] LEGACY_CDC_META_COLUMN = "_cdc_meta_".getBytes(StandardCharsets.UTF_8);
	static final List<byte[]> SYSTEM_COLUMNS = List.of(RocksDB.DEFAULT_COLUMN_FAMILY,
			COLUMN_SCHEMAS_COLUMN,
			MERGE_OPERATORS_COLUMN,
			LEGACY_CDC_META_COLUMN
	);

	record StoragePressureColumn(long id,
			ColumnFamilyHandle handle,
			@Nullable ColumnInstance registeredColumn,
			long effectiveSoftPendingCompactionBytesLimit, int effectiveLevel0SlowdownWritesTrigger,
			int effectiveMaxWriteBufferNumber, int effectiveMinWriteBufferNumberToMerge) {
	}

	private static long storagePressurePendingCompactionBytesOverride() {
		Long configured = Long.getLong(STORAGE_PRESSURE_PENDING_COMPACTION_BYTES_PROPERTY);
		return configured == null ? 0L : Math.max(1L, configured);
	}

	final EmbeddedDBColumns columnOps = new EmbeddedDBColumns(this);
	final EmbeddedDBStorageMetrics storageMetrics = new EmbeddedDBStorageMetrics(this);
	final EmbeddedDBWriteElision writeElision = new EmbeddedDBWriteElision(this);
	final EmbeddedDBWrites writes = new EmbeddedDBWrites(this);
	final EmbeddedDBBatchWrites batchWrites = new EmbeddedDBBatchWrites(this);
	final EmbeddedDBPointReads pointReads = new EmbeddedDBPointReads(this);
	final EmbeddedDBExistsMulti existsMultiOps = new EmbeddedDBExistsMulti(this);
	final EmbeddedDBIterators iterators = new EmbeddedDBIterators(this);
	final EmbeddedDBRanges ranges = new EmbeddedDBRanges(this);
	final EmbeddedDBRawScan rawScan = new EmbeddedDBRawScan(this);
	final EmbeddedDBMaintenance maintenance = new EmbeddedDBMaintenance(this);
	final Logger logger;
	final ActionLoggerConsumer actionLogger;
	final @Nullable Path path;
	final @NotNull Path definitiveDbPath;
	final TransactionalDB db;
	final DBOptions dbOptions;
	final RWScheduler scheduler;
	final ThreadPoolExecutor rawScanPinCaptureExecutor;
	final ScheduledExecutorService leakScheduler;
	private final ScheduledFuture<?> expiredRangeCleanupTask;
	final ColumnFamilyHandle columnSchemasColumnDescriptorHandle;
	final ColumnFamilyHandle mergeOperatorsColumnDescriptorHandle;
	private final @Nullable ColumnFamilyHandle legacyCdcMetaColumnHandle;
	final MergeOperatorRegistry mergeOperatorRegistry;
	final NonBlockingHashMapLong<ColumnInstance> columns;
	volatile StoragePressureColumn[] storagePressureColumns = EMPTY_STORAGE_PRESSURE_COLUMNS;
	final Map<String, ColumnFamilyOptions> columnsConifg;
	final ConcurrentMap<String, Long> columnNamesIndex;
	final ConcurrentMap<String, ColumnFamilyHandle> unconfiguredColumns;
	private final NonBlockingHashMapLong<Tx> txs;
	final NonBlockingHashMapLong<REntry<RocksIterator>> its;
	final Set<ActiveRangeResource> activeRangeResources = ConcurrentHashMap.newKeySet();
	final AtomicInteger retainedRangeSnapshots = new AtomicInteger();
	final AtomicInteger activeNativeMultiGets = new AtomicInteger();
	final PointReadCompletionCounters pointReadCompletions = new PointReadCompletionCounters();
	final AtomicLong bulkReadTailNanos = new AtomicLong();
	final AtomicLong nativeBulkReadCompletions = new AtomicLong();
	final AtomicLong nativeBulkReadKeys = new AtomicLong();
	final RetainedQueryLimiter retainedQueryLimiter;
	final long maxRetainedSnapshotAgeMs;
	final Set<AsyncExistsMultiRequest> activeExistsMultiRequests = ConcurrentHashMap.newKeySet();
	final RawScanFileDeletionRecovery rawScanFileDeletionRecovery = rawScan.new RawScanFileDeletionRecovery();
	final Object rawScanPinAcquisitionLock = new Object();
	final Object rawScanPinInitializationLock = new Object();
	final Set<Path> initializedRawScanPinRoots = new HashSet<>();
	final Set<Path> activeRawScanPinnedFiles = ConcurrentHashMap.newKeySet();
	final AtomicLong activeRawScanPinnedBytes = new AtomicLong();
	final Object asyncExistsMultiAdmissionLock = new Object();
	final SafeShutdown ops;
	private final AtomicLong resourceLeases = new AtomicLong();
	final Object columnEditLock = new Object();
	final DatabaseConfig config;
	final Map<String, RWScheduler> workloadGroups = new LinkedHashMap<>();
	final Map<String, StoragePressureSignal> workloadGroupPressure = new LinkedHashMap<>();
	final Map<String, String> columnWorkloadGroups = new HashMap<>();
	final ConcurrentHashMap<Long, String> columnWorkloadNames = new ConcurrentHashMap<>();
	String fallbackWorkloadGroup = "default";

	final WorkloadSettings workloadSettings;
	final RocksDBObjects refs;
	final Map<String, Cache> caches;
	final Map<String, Cache> metadataCaches;
	final MetricsManager metrics;
	final ExistsMultiPerfSampler existsMultiPerfSampler;
	final String name;
	final List<Meter> meters = new ArrayList<>();
	private final Timer openTransactionTimer;
	private final Timer closeTransactionTimer;
	private final Timer closeFailedUpdateTimer;
	final Timer createColumnTimer;
	final Timer deleteColumnTimer;
	final Timer deleteColumnIfExistsTimer;
	final Timer getColumnIdTimer;
	final Timer estimateNumKeysTimer;
	final Timer putTimer;
	final Timer putMultiTimer;
	final Timer putBatchTimer;
	final Timer deleteRangeTimer;
	final Timer getTimer;
	final Timer existsMultiTimer;
	final Timer openIteratorTimer;
	final Timer closeIteratorTimer;
	final Timer seekToTimer;
	final Timer subsequentTimer;
	final Timer reduceRangeTimer;
	final Timer getRangeTimer;
	final Timer flushTimer;
	final Timer compactTimer;
	final String sstMaintenanceSession = UUID.randomUUID().toString();
	final Timer getAllColumnDefinitionsTimer;
	final AtomicBoolean nativeDeleteRangeFallbackLogged = new AtomicBoolean();
	final Counter storagePressureRefreshFailures;
	final StoragePressureSignal storagePressureSignal;
	final Counter rawScanPinAcquisitionFailures;
	final Timer rawScanPinAcquisitionTimer;
	final EnumMap<WriteElisionRequest, EnumMap<WriteElisionDecision, Counter>> writeElisionDecisionCounters;
	private final RocksDBStatistics rocksDBStatistics;
	@Nullable CompactionIoController compactionIoController;
	volatile @Nullable Runnable compactionIoSampleObserver;
	final boolean fastGet;
	final @Nullable NativeRocksDBGet fastGetReader;
	private volatile @Nullable Consumer<Boolean> rangeReadOptionsObserver;
	volatile @Nullable Consumer<Boolean> reduceRangeAsyncIoObserver;
	private volatile @Nullable Runnable rangeIteratorOpenObserver;
	volatile @Nullable Consumer<Integer> rangeReadChunkSizeObserver;
	volatile @Nullable Runnable rangeCountChunkObserver;
	volatile @Nullable LongConsumer rangeCountQuantumItemsObserver;
	volatile @Nullable Runnable rangeContinuationObserver;
	volatile @Nullable LongConsumer retainedQueryPermitGrantedObserver;
	volatile @Nullable Runnable retainedRangeCleanupObserver;
	volatile @Nullable Runnable iteratorAdvanceStepCompletedObserver;
	private volatile @Nullable Runnable forcedShutdownObserver;
	volatile @Nullable Runnable existsMultiChunkObserver;
	volatile @Nullable Runnable batchWriteObserver;
	volatile @Nullable Runnable batchFinalWriteObserver;
	volatile @Nullable Runnable existsMultiSnapshotObserver;
	volatile @Nullable Consumer<Boolean> existsMultiArenaObserver;
	volatile @Nullable BiConsumer<Integer, Long> writeElisionMultiGetObserver;
	volatile @Nullable Runnable rawScanFilesCapturedObserver;
	private volatile @Nullable Runnable rawScanHardLinkObserver;
	volatile @Nullable Runnable rawScanReaderOpenedObserver;
	volatile @Nullable Runnable rawScanCleanupObserver;
	volatile @Nullable Runnable columnMaintenanceObserver;
	private volatile @Nullable LongConsumer columnUseAcquiredObserver;
	volatile @Nullable Runnable storagePressurePollAdmittedObserver;
	Path tempSSTsPath;

	public EmbeddedDB(@Nullable Path path, String name, @Nullable Path embeddedConfigPath) throws IOException {
		this(path, name, embeddedConfigPath, null);
	}

	@VisibleForTesting
	EmbeddedDB(@Nullable Path path,
			String name,
			@Nullable Path embeddedConfigPath,
			@Nullable RWScheduler schedulerOverride) throws IOException {
		this.path = path;
		this.name = name;
		this.logger = LoggerFactory.getLogger("db." + name);
		this.columns = new NonBlockingHashMapLong<>();
		this.storagePressureSignal = STORAGE_PRESSURE_PENDING_COMPACTION_BYTES_OVERRIDE == 0L
				? new StoragePressureSignal()
				: new StoragePressureSignal(STORAGE_PRESSURE_PENDING_COMPACTION_BYTES_OVERRIDE);
		this.txs = new NonBlockingHashMapLong<>();
		this.its = new NonBlockingHashMapLong<>();
		this.columnNamesIndex = new ConcurrentHashMap<>();
		this.unconfiguredColumns = new ConcurrentHashMap<>();
		this.ops = new SafeShutdown();
		DatabaseConfig config = ConfigParser.parse(embeddedConfigPath);
		this.config = config;
		WorkloadSettings workloadSettings;
        final boolean tablePropertiesEnabled;
        final long tablePropertiesIntervalSeconds;
		try {
			workloadSettings = WorkloadSettings.resolve(config);
			this.fastGet = config.global().enableFastGet();
            tablePropertiesEnabled = config.metrics().tablePropertiesEnabled();
            tablePropertiesIntervalSeconds = config.metrics().tablePropertiesIntervalSeconds();
            if (tablePropertiesIntervalSeconds < 60 || tablePropertiesIntervalSeconds > Long.MAX_VALUE / 1_000_000_000L) {
                throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR,
                        "table-properties-interval-seconds must be at least 60 and fit in nanoseconds");
            }
		} catch (GestaltException e) {
			throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR,
					"Can't resolve workload, fast-get or metrics configuration",
					e);
		}
		this.workloadSettings = workloadSettings;
		try {
			fallbackWorkloadGroup = Objects.requireNonNullElse(config.global().fallbackColumnOptions().workloadGroup(), "default");
			for (var column : config.global().columnOptions()) {
				columnWorkloadGroups.put(column.name(), Objects.requireNonNullElse(column.workloadGroup(), fallbackWorkloadGroup));
			}
		} catch (GestaltException failure) {
			throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, failure);
		}
		this.maxRetainedSnapshotAgeMs = workloadSettings.retainedSnapshotMaximumAge().toMillis();
		int readCap = workloadSettings.readParallelism();
		int writeCap = workloadSettings.writeParallelism();

		this.metrics = new MetricsManager(config);
		this.existsMultiPerfSampler = ExistsMultiPerfSampler.configured(metrics.getRegistry(), name);
		Timer loadTimer = storageMetrics.createTimer(Tags.of("action", "load"));
		this.openTransactionTimer = storageMetrics.createActionTimer(OpenTransaction.class);
		this.closeTransactionTimer = storageMetrics.createActionTimer(CloseTransaction.class);
		this.closeFailedUpdateTimer = storageMetrics.createActionTimer(CloseFailedUpdate.class);
		this.createColumnTimer = storageMetrics.createActionTimer(CreateColumn.class);
		this.deleteColumnTimer = storageMetrics.createActionTimer(DeleteColumn.class);
		this.deleteColumnIfExistsTimer = storageMetrics.createActionTimer(DeleteColumnIfExists.class);
		this.getColumnIdTimer = storageMetrics.createActionTimer(GetColumnId.class);
		this.estimateNumKeysTimer = storageMetrics.createActionTimer(EstimateNumKeys.class);
		this.putTimer = storageMetrics.createActionTimer(Put.class);
		this.putMultiTimer = storageMetrics.createActionTimer(PutMulti.class);
		this.putBatchTimer = storageMetrics.createActionTimer(PutBatch.class);
		this.deleteRangeTimer = storageMetrics.createActionTimer(DeleteRange.class);
		this.getTimer = storageMetrics.createActionTimer(Get.class);
		this.existsMultiTimer = storageMetrics.createActionTimer(ExistsMulti.class);
		this.openIteratorTimer = storageMetrics.createActionTimer(OpenIterator.class);
		this.closeIteratorTimer = storageMetrics.createActionTimer(CloseIterator.class);
		this.seekToTimer = storageMetrics.createActionTimer(SeekTo.class);
		this.subsequentTimer = storageMetrics.createActionTimer(Subsequent.class);
		this.reduceRangeTimer = storageMetrics.createActionTimer(ReduceRange.class);
		this.getRangeTimer = storageMetrics.createActionTimer(GetRange.class);
		this.flushTimer = storageMetrics.createActionTimer(Flush.class);
		this.compactTimer = storageMetrics.createActionTimer(Compact.class);
		this.getAllColumnDefinitionsTimer = storageMetrics.createActionTimer(GetAllColumnDefinitions.class);
		this.storagePressureRefreshFailures = metrics.getRegistry().counter(
				"rockserver.workload.storage.pressure.refresh.failures",
				"database",
				name);
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
			rawScan.recoverRawScanPinsAtStartup(loadedDb.db().get(), loadedDb.definitiveDbPath(), config);
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
		this.metadataCaches = loadedDb.metadataCaches();
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
			if (globalConfig.adaptiveCompactionIo()) {
				compactionReadaheadBytes = dbOptions.useDirectReads() && !dbOptions.allowMmapReads()
						? Optional.ofNullable(globalConfig.adaptiveCompactionReadaheadMaxSize())
								.map(DataSize::longValue)
								.orElse(CompactionIoBudget.MAX_READAHEAD_BYTES) : 0;
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
				caches, loadedDb.cacheCapacities(), metadataCaches, loadedDb.metadataCacheCapacities(),
				storageMetrics::getLongProperty, storageMetrics::getPerCfLongProperty, memoryUpperBoundConfig, walMetricsConfig);
		this.scheduler = schedulerOverride != null
				? schedulerOverride
				: new RWScheduler(
						workloadSettings,
						"db[" + name + "]",
						metrics.getRegistry(),
						name);
		try {
			for (var group : WorkloadSettings.resolveGroups(config).entrySet()) {
				var groupScheduler = new RWScheduler(group.getValue(),
						"db[" + name + "]-" + group.getKey(), metrics.getRegistry(),
						name, group.getKey());
				workloadGroups.put(group.getKey(), groupScheduler);
				scheduler.ownGroup(groupScheduler);
				workloadGroupPressure.put(group.getKey(), EmbeddedDBStorageMetrics.newWorkloadGroupPressureSignal());
			}
		} catch (GestaltException failure) {
			throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, failure);
		}
		if (!workloadGroups.isEmpty()) workloadGroupPressure.put("default", EmbeddedDBStorageMetrics.newWorkloadGroupPressureSignal());
		storageMetrics.registerStoragePressureSignalMetrics(metrics.getRegistry());
		// Pin capture performs filesystem work while RocksDB file deletion is
		// disabled. Keep that serialized critical section on one owned thread so
		// capture waiters never occupy the latency-sensitive read pool.
		this.rawScanPinCaptureExecutor = new ThreadPoolExecutor(
				1,
				1,
				0L,
				TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<>(workloadSettings.batchQueueCapacity()),
				new NamedThreadFactory("db-raw-scan-pin-capture"),
				new ThreadPoolExecutor.AbortPolicy());
		Gauge.builder("rockserver.workload.retained.snapshots", retainedRangeSnapshots, AtomicInteger::get)
				.tag("database", name)
				.register(metrics.getRegistry());
		this.fastGetReader = fastGet ? new NativeRocksDBGet(db.get(), (long) readCap + writeCap) : null;
		var leakScheduler = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("db-leak-scheduler"));
		leakScheduler.setRemoveOnCancelPolicy(true);
		this.leakScheduler = leakScheduler;
		this.retainedQueryLimiter = ranges.new RetainedQueryLimiter(
				workloadSettings.retainedAnalyticalSnapshots(), leakScheduler);

		leakScheduler.scheduleWithFixedDelay(this::cleanupExpiredTransactionsNow, 1, 1, TimeUnit.MINUTES);

		leakScheduler.scheduleWithFixedDelay(this::cleanupExpiredIteratorsNow, 1, 1, TimeUnit.MINUTES);
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
			var columnSchemasColumnOptions = RocksDBLoader.getCompatibilityColumnOptions(refs, path, definitiveDbPath, config);
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
			var mergeOperatorsColumnOptions = RocksDBLoader.getCompatibilityColumnOptions(refs, path, definitiveDbPath, config);
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

		this.legacyCdcMetaColumnHandle = db
				.getStartupColumns()
				.entrySet()
				.stream()
				.filter(e -> Arrays.equals(e.getKey().getName(), LEGACY_CDC_META_COLUMN))
				.map(Map.Entry::getValue)
				.findAny()
				.orElse(null);
		storageMetrics.registerSystemStoragePressureColumns();

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

		columnOps.loadExistingColumns(loadedDb.mergeOperators());
        if (tablePropertiesEnabled) {
            rocksDBStatistics.setTablePropertiesMetrics(new TablePropertiesMetrics(name, metrics.getRegistry(),
                    tablePropertiesIntervalSeconds, maintenance::collectTablePropertiesMetrics));
        }
		// Do not publish this object to the polling thread until column options,
		// system handles, and registered-column pressure entries are complete.
		leakScheduler.scheduleWithFixedDelay(storageMetrics::refreshStoragePressure, 1, 1, TimeUnit.SECONDS);
		if (Boolean.parseBoolean(System.getProperty("rockserver.core.print-config", "true"))) {
			logger.info("Database configuration: {}", ConfigPrinter.stringify(config));
		}
		columnOps.printStartupInfo();
		var afterLoad = Instant.now();

		loadTimer.record(Duration.between(beforeLoad, afterLoad));
		if (loadedDb.compactionIoLimiter() != null) {
			try {
				long readaheadCeiling;
				try {
					readaheadCeiling = dbOptions.useDirectReads() && !dbOptions.allowMmapReads()
							? Optional.ofNullable(config.global().adaptiveCompactionReadaheadMaxSize())
									.map(DataSize::longValue)
									.orElse(CompactionIoBudget.MAX_READAHEAD_BYTES) : 0;
				} catch (GestaltException failure) {
					throw new RuntimeException("Failed to read adaptive readahead ceiling", failure);
				}
				var readPoolTelemetry = new long[RWScheduler.POOL_TELEMETRY_LENGTH];
				var groupReadPoolTelemetry = new long[RWScheduler.POOL_TELEMETRY_LENGTH];
				compactionIoController = new CompactionIoController(name, loadedDb.compactionIoLimiter(),
						() -> storageMetrics.sampleCompactionIo(loadedDb.compactionIoLimiter(), readPoolTelemetry, groupReadPoolTelemetry), metrics.getRegistry(),
						readaheadCeiling, storageMetrics::applyCompactionReadahead);
			} catch (RuntimeException | Error failure) {
				try { close(); } catch (Throwable cleanupFailure) { failure.addSuppressed(cleanupFailure); }
				throw failure;
			}
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
			if (compactionIoController != null) compactionIoController.close();
			logger.info("Closing... stopping background statistics");
			try {
				rocksDBStatistics.close();
			} catch (Throwable error) {
				logger.error("Failed to stop RocksDB statistics during shutdown", error);
			}
			ops.closeAdmission();
			retainedQueryLimiter.close();
			cancelActiveExistsMultiRequests();
			closeActiveRangeResources();
			logger.info("Waiting for active operations");
			ops.waitForExit(pendingOpsTimeoutMs);
			shutdownRawScanPinCaptureExecutor();
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
					|| !rawScanPinCaptureExecutor.isTerminated()
					|| retainedQueryLimiter.activeCount() > 0
					|| retainedQueryLimiter.waitingCount() > 0
					|| retainedRangeSnapshots.get() > 0
					|| !activeExistsMultiRequests.isEmpty())) {
				throw new IllegalStateException("Shutdown left resources open: schedulerTerminated="
						+ (scheduler == null || scheduler.isFullyTerminated())
						+ ", rawScanPinCaptureTerminated=" + rawScanPinCaptureExecutor.isTerminated()
						+ ", leases=" + resourceLeases.get()
						+ ", retainedPermits=" + retainedQueryLimiter.activeCount()
						+ ", retainedWaiters=" + retainedQueryLimiter.waitingCount()
						+ ", retainedSnapshots=" + retainedRangeSnapshots.get()
						+ ", existsMultiRequests=" + activeExistsMultiRequests.size());
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
			shutdownRawScanPinCaptureExecutor();
			// After forcing close of leaked resources, proceed to close DB/native resources defensively
			closeResources(true);

			if (testing && (ops.getPendingOpsCount() > 0
					|| (scheduler != null && !scheduler.isFullyTerminated())
					|| !rawScanPinCaptureExecutor.isTerminated()
					|| resourceLeases.get() > 0
					|| retainedQueryLimiter.activeCount() > 0
					|| retainedQueryLimiter.waitingCount() > 0
					|| retainedRangeSnapshots.get() > 0
					|| !activeExistsMultiRequests.isEmpty())) {
				throw new IllegalStateException("Some active operations lasted more than " + pendingOpsTimeoutMs
						+ " ms! schedulerTerminated=" + (scheduler == null || scheduler.isFullyTerminated())
						+ ", rawScanPinCaptureTerminated=" + rawScanPinCaptureExecutor.isTerminated()
						+ ", activeOps=" + ops.getPendingOpsCount() + ", resourceLeases="
						+ resourceLeases.get() + ", retainedPermits=" + retainedQueryLimiter.activeCount()
						+ ", retainedWaiters=" + retainedQueryLimiter.waitingCount()
						+ ", retainedSnapshots=" + retainedRangeSnapshots.get()
						+ ", existsMultiRequests=" + activeExistsMultiRequests.size()
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
				shutdownRawScanPinCaptureExecutor();
			} catch (Throwable t) {
				logger.warn("Failed to shutdown raw-scan pin capture scheduler", t);
			}
			try {
				shutdownExecutor(leakScheduler, "Leak scheduler");
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
		// Logical range reads and existsMulti requests keep native read state between
		// bounded scheduler slices. SafeShutdown has already excluded new requests, so
		// close those idle resources before column handles or the database.
		cancelActiveExistsMultiRequests();
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
					iterators.closeIteratorInternal(iteratorId);
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
		if (legacyCdcMetaColumnHandle != null) {
			try {
				legacyCdcMetaColumnHandle.close();
			} catch (Throwable t) {
				logger.error("Error closing the legacy CDC column handle{}", forced ? " (forced)" : "", t);
			}
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
					if (iterators.closeIteratorInternal(iteratorId)) {
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

	private void shutdownExecutor(ExecutorService exec, String description) {
		if (exec == null) {
			return;
		}
		exec.shutdownNow();
		awaitExecutorTermination(exec, description);
	}

	private void awaitExecutorTermination(ExecutorService exec, String description) {
		try {
			if (!exec.awaitTermination(10, TimeUnit.SECONDS)) {
				logger.warn("{} did not terminate within timeout", description);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private void shutdownRawScanPinCaptureExecutor() {
		var shutdownFailure = new RejectedExecutionException("Raw-scan pin capture scheduler is shutting down");
		for (Runnable queuedTask : rawScanPinCaptureExecutor.shutdownNow()) {
			if (queuedTask instanceof RawScanPinCaptureTask captureTask) {
				captureTask.reject(shutdownFailure);
			}
		}
		awaitExecutorTermination(rawScanPinCaptureExecutor, "Raw-scan pin capture scheduler");
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
					iterators.closeIteratorInternal(id);
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
			for (var range : activeRangeResources) {
				try {
					range.expireIfDeadlinePassed();
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

	ReadOptions newReadOptions(String label) {
		var ro = new LeakSafeReadOptions(label);
		ro.setAsyncIo(true);
		return ro;
	}

	ReadOptions newPointReadOptions(String label) {
		var ro = newReadOptions(label);
		ro.setAsyncIo(false);
		return ro;
	}

	ReadOptions newRangeReadOptions(long deadlineMicros,
			boolean fillCache,
			@Nullable AbstractSlice<?> startKeySlice,
			@Nullable AbstractSlice<?> endKeySlice) {
		var ro = newReadOptions("get-range-async-read-options");
		try {
			// Retained iterators may move between workers; native async prefetch owns a thread-local ring.
			ro.setAsyncIo(false);
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

	/** Runs at the start of each batch write of a streamed putBatch or mergeBatch, before the final write. */
	@VisibleForTesting
	public void setBatchWriteObserverForTesting(@Nullable Runnable observer) {
		this.batchWriteObserver = observer;
	}

	/** Runs at the start of the final write of a streamed putBatch or mergeBatch. */
	@VisibleForTesting
	public void setBatchFinalWriteObserverForTesting(@Nullable Runnable observer) {
		this.batchFinalWriteObserver = observer;
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
	public void setRawScanFilesCapturedObserverForTesting(@Nullable Runnable observer) {
		this.rawScanFilesCapturedObserver = observer;
	}

	@VisibleForTesting
	public void setRawScanHardLinkObserverForTesting(@Nullable Runnable observer) {
		this.rawScanHardLinkObserver = observer;
	}

	@VisibleForTesting
	public int getRawScanPinCaptureQueueSizeForTesting() {
		return rawScanPinCaptureExecutor.getQueue().size();
	}

	@VisibleForTesting
	public long getRawScanPinCaptureQueueCapacityForTesting() {
		var queue = rawScanPinCaptureExecutor.getQueue();
		return (long) queue.size() + queue.remainingCapacity();
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

	@VisibleForTesting
	public void setStoragePressurePollAdmittedObserverForTesting(@Nullable Runnable observer) {
		this.storagePressurePollAdmittedObserver = observer;
	}

	@VisibleForTesting
	public void refreshStoragePressureForTesting() {
		storageMetrics.refreshStoragePressure();
	}

	@VisibleForTesting
	public void failStoragePressureRefreshForTesting(Throwable failure) {
		storageMetrics.handleStoragePressureRefreshFailure(failure);
	}

	@VisibleForTesting
	public int getStoragePressureColumnCountForTesting() {
		return storagePressureColumns.length;
	}

	@VisibleForTesting
	public boolean isAcceptingOperationsForTesting() {
		return ops.isOpen();
	}

	@VisibleForTesting
	public boolean hasStoragePressureColumnForTesting(long columnId) {
		for (StoragePressureColumn pressureColumn : storagePressureColumns) {
			if (pressureColumn.id() == columnId) {
				return true;
			}
		}
		return false;
	}

	@VisibleForTesting
	public long getStoragePressureColumnLimitForTesting(long columnId) {
		for (StoragePressureColumn pressureColumn : storagePressureColumns) {
			if (pressureColumn.id() == columnId) {
				return pressureColumn.effectiveSoftPendingCompactionBytesLimit();
			}
		}
		throw new IllegalArgumentException("Column is not registered for storage-pressure polling: " + columnId);
	}

	void notifyRangeIteratorOpened() {
		var observer = rangeIteratorOpenObserver;
		if (observer != null) {
			observer.run();
		}
	}

	@Override
	public long openTransaction(Duration transactionLeaseTtl) {
		return openTransaction(LeaseTtl.toMillisCeil(transactionLeaseTtl, "transactionLeaseTtl"),
				WorkloadProfile.BATCH);
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

	/**
	 * Opens a write transaction for the apply thread of replication, which is the only writer of the database. Its
	 * writes are not tracked for conflicts (nothing else writes them, so commit has nothing to validate), no snapshot
	 * is taken and, if {@code disableWal}, its commit skips the WAL, so it survives a crash only once flushed: the
	 * replication log replays the writes a crash loses. It is held by the caller instead of being registered under
	 * an id. Its
	 * operations run the same code as the public ones with a transaction, so keys, schemas, merges and results are
	 * the same byte for byte, without the per-call bookkeeping of a client operation: the transaction lookup and
	 * lock, column use counting, action logging, operation counters and timers. The caller must not use it from
	 * several threads, must not delete a column while it is open, and must close it before the database closes.
	 */
	public ApplyWriter openApplyWriter(boolean disableWal) {
		ops.beginOp();
		try {
			return new ApplyWriter(openTransactionInternal(APPLY_WRITER_EXPIRATION_MS, false, WorkloadProfile.BATCH,
					false, disableWal, true), disableWal);
		} finally {
			ops.endOp();
		}
	}

	/**
	 * The schema of column {@code columnId}, or null if there is none. For the replication apply, which splits the
	 * writes of a group by key among several {@link ApplyWriter}s and needs to know which key parts name a RocksDB key.
	 */
	public @Nullable ColumnSchema columnSchemaOrNull(long columnId) {
		var col = columns.get(columnId);
		return col == null ? null : col.schema();
	}

	/**
	 * Expiration of an {@link ApplyWriter}'s native transaction. It must never expire while it is used: on a
	 * pessimistic database an expired transaction fails its commit, which would look like a rejected entry on this
	 * replica only.
	 */
	private static final long APPLY_WRITER_EXPIRATION_MS = 24L * 60 * 60 * 1000;

	/** A write transaction of the replication apply thread, see {@link #openApplyWriter}. */
	public final class ApplyWriter implements AutoCloseable {

		private final Tx tx;
		private final boolean disableWal;

		private ApplyWriter(Tx tx, boolean disableWal) {
			this.tx = tx;
			this.disableWal = disableWal;
		}

		public <T> T put(long columnId, @NotNull Keys keys, @NotNull Buf value, RequestPut<? super Buf, T> requestType) {
			return writes.put(tx, getColumn(columnId), 0L, keys, value, requestType);
		}

		/** Like {@link EmbeddedDB#putMulti} with a transaction. */
		public <T> List<T> putMulti(long columnId, @NotNull List<Keys> keysList, @NotNull List<@NotNull Buf> valueList,
				RequestPut<? super Buf, T> requestType) {
			try {
				var col = getColumn(columnId);
				if (keysList.size() != valueList.size()) {
					throw new IllegalArgumentException(
							"keys length is different than values length: " + keysList.size() + " != " + valueList.size());
				}
				List<T> responses = requestType instanceof RequestType.RequestNothing<?>
						|| requestType instanceof RequestType.RequestEnsure<?> ? null : new ArrayList<>(keysList.size());
				for (int i = 0; i < keysList.size(); i++) {
					T result = writes.put(tx, col, 0L, keysList.get(i), valueList.get(i), requestType);
					if (responses != null) {
						responses.add(result);
					}
				}
				return responses != null ? responses : List.of();
			} catch (RocksDBException ex) {
				throw ex;
			} catch (Exception ex) {
				throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
			}
		}

		public <T> T delete(long columnId, @NotNull Keys keys, RequestDelete<? super Buf, T> requestType) {
			return writes.delete(tx, getColumn(columnId), 0L, keys, requestType);
		}

		/** Like {@link EmbeddedDB#deleteMulti} with a transaction. */
		public <T> List<T> deleteMulti(long columnId, @NotNull List<Keys> keysList,
				RequestDelete<? super Buf, T> requestType) {
			var col = getColumn(columnId);
			List<T> responses =
					requestType instanceof RequestType.RequestNothing<?> ? null : new ArrayList<>(keysList.size());
			for (Keys keys : keysList) {
				T result = writes.delete(tx, col, 0L, keys, requestType);
				if (responses != null) {
					responses.add(result);
				}
			}
			return responses != null ? responses : List.of();
		}

		/**
		 * Like {@link EmbeddedDB#merge} with a transaction, and the operand is checked at once: see
		 * {@link #checkMerge}.
		 */
		public <T> T merge(long columnId, @NotNull Keys keys, @NotNull Buf value,
				RequestMerge<? super Buf, T> requestType) {
			var col = getColumn(columnId);
			T result = writes.merge(tx, col, 0L, keys, value, requestType);
			checkMerge(col, keys, requestType);
			return result;
		}

		/**
		 * Runs the merge operator on {@code keys} right after an operand was staged for it, by reading the key through
		 * this transaction (its batch over the database), unless the merge did that already: a request that returns
		 * the merged value, or a bucketed column, merged in Java. An operator that fails here throws
		 * {@link RocksDBErrorType#MERGE_OPERATOR_FAILED}, so the caller rolls the operand back instead of storing one
		 * that every later read, flush and compaction of the key would fail to merge. One read per merge.
		 */
		private void checkMerge(ColumnInstance col, Keys keys, RequestMerge<? super Buf, ?> requestType) {
			if (!(requestType instanceof RequestType.RequestNothing<?>) || col.hasBuckets()
					|| col.mergeOperator() == null) {
				return;
			}
			FFMAbstractMergeOperator.takeFailureOnThisThread();
			try {
				pointReads.get(tx, 0L, col, keys, RequestType.exists());
			} catch (RuntimeException ex) {
				var operatorFailure = FFMAbstractMergeOperator.takeFailureOnThisThread();
				if (operatorFailure == null) {
					throw ex;
				}
				var failure = mergeOperatorFailed(operatorFailure);
				failure.addSuppressed(ex);
				throw failure;
			}
		}

		/** Like {@link EmbeddedDB#mergeMulti} with a transaction; each operand is checked, see {@link #checkMerge}. */
		public <T> List<T> mergeMulti(long columnId, @NotNull List<Keys> keysList,
				@NotNull List<@NotNull Buf> valueList, RequestMerge<? super Buf, T> requestType) {
			try {
				var col = getColumn(columnId);
				if (keysList.size() != valueList.size()) {
					throw new IllegalArgumentException(
							"keys length is different than values length: " + keysList.size() + " != " + valueList.size());
				}
				List<T> responses =
						requestType instanceof RequestType.RequestNothing<?> ? null : new ArrayList<>(keysList.size());
				for (int i = 0; i < keysList.size(); i++) {
					T result = writes.merge(tx, col, 0L, keysList.get(i), valueList.get(i), requestType);
					checkMerge(col, keysList.get(i), requestType);
					if (responses != null) {
						responses.add(result);
					}
				}
				return responses != null ? responses : List.of();
			} catch (RocksDBException ex) {
				throw ex;
			} catch (Exception ex) {
				throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
			}
		}

		/** A read through the transaction: it sees the writes made to it. Not {@code forUpdate}. */
		public <T> T get(long columnId, Keys keys, RequestGet<? super Buf, T> requestType) {
			if (requestType instanceof RequestType.RequestForUpdate<?>) {
				throw new UnsupportedOperationException("An apply writer does not run updates");
			}
			return pointReads.get(tx, 0L, getColumn(columnId), keys, requestType);
		}

		/** Marks the current state: {@link #rollbackToSavePoint} undoes the writes made after it. */
		public void setSavePoint() {
			try {
				tx.val().setSavePoint();
			} catch (org.rocksdb.RocksDBException e) {
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
			}
		}

		/** Tests only: the serialized native batch of this writer's writes so far, what {@link #commitWith} joins. */
		@VisibleForTesting
		public byte[] batchDataForTesting() {
			try {
				return tx.val().getWriteBatch().getWriteBatch().data();
			} catch (org.rocksdb.RocksDBException e) {
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
			}
		}

		/** Undoes the writes made since the latest {@link #setSavePoint}, and forgets it. */
		public void rollbackToSavePoint() {
			try {
				tx.val().rollbackToSavePoint();
			} catch (org.rocksdb.RocksDBException e) {
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
			}
		}

		/**
		 * Commits the writes and closes the transaction. Returns false, with the transaction still open, only for an
		 * optimistic conflict, which nothing should cause: it tracks no keys.
		 */
		public boolean commit() {
			return closeTransactionInternal(tx, true);
		}

		/** Rolls back what was not committed, and closes the transaction. Safe to call again. */
		@Override
		public void close() {
			closeTransactionInternal(tx, false);
		}

		/**
		 * The serialized write batches {@code parts}, in order, as one batch: what RocksDB's {@code WriteBatchInternal::Append}
		 * makes (RocksJava has no call for it). A batch is a 12-byte header (8-byte sequence, which the database assigns
		 * on write, and a 4-byte count, both little endian) followed by its records, each self-delimiting and independent of
		 * its position; so the records of every part one after the other, under a header of sequence 0 and the summed
		 * count, are the batch that holds each part's operations in order.
		 */
		@VisibleForTesting
		public static byte[] joinWriteBatches(List<byte[]> parts) {
			int count = 0;
			long size = WRITE_BATCH_HEADER;
			for (byte[] part : parts) {
				count += (part[8] & 0xFF) | (part[9] & 0xFF) << 8 | (part[10] & 0xFF) << 16 | (part[11] & 0xFF) << 24;
				size += part.length - WRITE_BATCH_HEADER;
			}
			if (size > Integer.MAX_VALUE - 16) {
				throw RocksDBException.of(RocksDBErrorType.WRITE_BATCH_1, "Apply writes too large for one batch: "
						+ size + " bytes");
			}
			byte[] joined = new byte[(int) size];
			// Sequence 0 (the database assigns it), then the count, little endian.
			joined[8] = (byte) count;
			joined[9] = (byte) (count >>> 8);
			joined[10] = (byte) (count >>> 16);
			joined[11] = (byte) (count >>> 24);
			int offset = WRITE_BATCH_HEADER;
			for (byte[] part : parts) {
				int length = part.length - WRITE_BATCH_HEADER;
				System.arraycopy(part, WRITE_BATCH_HEADER, joined, offset, length);
				offset += length;
			}
			return joined;
		}

		/**
		 * Commits the writes of {@code others} and of this writer in <b>one</b> atomic database write, in that order,
		 * then closes them all: a crash keeps all of them or none. For the partitioned apply of replication, whose
		 * writers were filled by different threads and hold writes to different keys. Every writer must have been
		 * opened with the same {@code disableWal} as this one, and none may be in use meanwhile. Their native batches
		 * are joined as RocksDB's own {@code WriteBatchInternal::Append} does: one header (sequence, count) followed by
		 * the records of each batch. If any of them fails, all are closed and the exception is thrown.
		 */
		public void commitWith(List<ApplyWriter> others) {
			try {
				var parts = new ArrayList<byte[]>(others.size() + 1);
				for (int i = 0; i <= others.size(); i++) {
					var writer = i < others.size() ? others.get(i) : this;
					if (writer.disableWal != disableWal) {
						throw new IllegalArgumentException("Apply writers with different WAL settings");
					}
					var batch = writer.tx.val().getWriteBatch().getWriteBatch();
					if (batch.count() == 0) {
						continue;
					}
					parts.add(batch.data());
				}
				if (parts.isEmpty()) {
					return;
				}
				byte[] joined = joinWriteBatches(parts);
				try (var writeOptions = new LeakSafeWriteOptions("apply-writers-commit");
						var batch = new WriteBatch(joined)) {
					if (disableWal) {
						writeOptions.setDisableWAL(true);
					}
					db.get().write(writeOptions, batch);
				}
			} catch (org.rocksdb.RocksDBException e) {
				throw RocksDBException.of(RocksDBErrorType.COMMIT_FAILED, "Apply writers commit failed", e);
			} finally {
				// Their writes are in the database now (or the commit failed): closing rolls back nothing that counts.
				RuntimeException closeFailure = null;
				for (int i = 0; i <= others.size(); i++) {
					try {
						(i < others.size() ? others.get(i) : this).close();
					} catch (RuntimeException ex) {
						if (closeFailure == null) {
							closeFailure = ex;
						}
					}
				}
				if (closeFailure != null) {
					throw closeFailure;
				}
			}
		}
	}

	/** Bytes of the header of a serialized {@link WriteBatch}: an 8-byte sequence and a 4-byte count. */
	private static final int WRITE_BATCH_HEADER = 12;

	long allocateTransactionInternal(Tx tx) {
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

	Tx openTransactionInternal(long timeoutMs, boolean isFromGetForUpdate) {
		return openTransactionInternal(timeoutMs, isFromGetForUpdate, WorkloadProfile.BATCH, false);
	}

	Tx openTransactionInternal(long timeoutMs,
			boolean isFromGetForUpdate,
			WorkloadProfile workloadProfile,
			boolean captureSnapshot) {
		return openTransactionInternal(timeoutMs, isFromGetForUpdate, workloadProfile, captureSnapshot, false, false);
	}

	Tx openTransactionInternal(long timeoutMs,
			boolean isFromGetForUpdate,
			WorkloadProfile workloadProfile,
			boolean captureSnapshot,
			boolean disableWal,
			boolean untracked) {
		Objects.requireNonNull(workloadProfile, "workloadProfile");
		if (timeoutMs < 0L) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Transaction timeout must be non-negative");
		}
		var expirationTimestamp = saturatingAdd(System.currentTimeMillis(), timeoutMs);
		TransactionalOptions txOpts = db.createTransactionalOptions(timeoutMs);
		var writeOpts = new LeakSafeWriteOptions("open-transaction-internal-write-options");
		if (disableWal) {
			writeOpts.setDisableWAL(true);
		}
		var rocksObjects = new RocksDBObjects(writeOpts, txOpts);
		Transaction transaction = null;
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
					workloadProfile,
					untracked);
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

	/**
	 * Marks the current state of an open transaction: {@link #rollbackToSavePoint} undoes the writes made after it.
	 * For a caller that owns the transaction and does not share it between threads.
	 */
	public void setSavePoint(long transactionId) {
		var tx = txs.get(transactionId);
		if (tx == null) {
			throw RocksDBException.of(RocksDBErrorType.TX_NOT_FOUND, "Transaction not found: " + transactionId);
		}
		synchronized (tx) {
			try {
				tx.val().setSavePoint();
			} catch (org.rocksdb.RocksDBException e) {
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
			}
		}
	}

	/** Undoes the writes made to an open transaction since its latest {@link #setSavePoint}, and forgets it. */
	public void rollbackToSavePoint(long transactionId) {
		var tx = txs.get(transactionId);
		if (tx == null) {
			throw RocksDBException.of(RocksDBErrorType.TX_NOT_FOUND, "Transaction not found: " + transactionId);
		}
		synchronized (tx) {
			try {
				tx.val().rollbackToSavePoint();
			} catch (org.rocksdb.RocksDBException e) {
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
			}
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
	boolean closeTransactionInternal(long transactionId, boolean commit) {
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
	boolean closeTransactionInternal(@NotNull Tx tx, boolean commit) {
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

	@Override
	public long createColumn(String name, @NotNull ColumnSchema schema) throws RocksDBException {
		return columnOps.createColumn(name, schema);
	}

	@Override
	public long uploadMergeOperator(String name, String className, byte[] jarData) {
		return columnOps.uploadMergeOperator(name, className, jarData);
	}

	@Override
	public Long checkMergeOperator(String name, byte[] hash) {
		return columnOps.checkMergeOperator(name, hash);
	}

	@Override
	public void deleteColumn(long columnId) throws RocksDBException {
		columnOps.deleteColumn(columnId);
	}

	@Override
	public boolean deleteColumnIfExists(@NotNull String name) throws RocksDBException {
		return columnOps.deleteColumnIfExists(name);
	}

	@Override
	public long getColumnId(@NotNull String name) {
		return columnOps.getColumnId(name);
	}

	/**
	 * Return RocksDB's estimate of physical keys in this column family.
	 * Exact, bounded, and transaction-aware counts remain implemented by {@link #reduceRange}.
	 */
	@Override
	public long estimateNumKeys(long columnId) {
		return columnOps.estimateNumKeys(columnId);
	}

	@VisibleForTesting
	public void setCompactionIoSampleObserverForTesting(@Nullable Runnable observer) {
		compactionIoSampleObserver = observer;
	}

	@Override
	public <T> T put(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		return writes.<T>put(transactionOrUpdateId, columnId, keys, value, requestType);
	}

	@Override
	public <T> T delete(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull RequestType.RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		return writes.<T>delete(transactionOrUpdateId, columnId, keys, requestType);
	}

	@Override
	public <T> T merge(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		return writes.<T>merge(transactionOrUpdateId, columnId, keys, value, requestType);
	}

	@Override
	public <T> List<T> deleteMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keysList,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		return writes.<T>deleteMulti(transactionOrUpdateId, columnId, keysList, requestType);
	}

	@Override
	public void deleteRange(long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive) throws RocksDBException {
		writes.deleteRange(columnId, startKeysInclusive, endKeysExclusive);
	}

	/**
	 * Like {@link #deleteRange(long, Keys, Keys)}, but the write skips the WAL, so it survives a crash only once
	 * flushed. Only for replication, which replays the writes lost by a crash from its own log.
	 */
	public void deleteRangeUnlogged(long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive) throws RocksDBException {
		writes.deleteRangeUnlogged(columnId, startKeysInclusive, endKeysExclusive);
	}

	@Override
	public <T> List<T> putMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keysList,
			@NotNull List<@NotNull Buf> valueList,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		return writes.<T>putMulti(transactionOrUpdateId, columnId, keysList, valueList, requestType);
	}

	@Override
	public <T> List<T> mergeMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keysList,
			@NotNull List<@NotNull Buf> valueList,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		return writes.<T>mergeMulti(transactionOrUpdateId, columnId, keysList, valueList, requestType);
	}
	public CompletableFuture<Void> putBatchInternal(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull PutBatchMode mode) throws RocksDBException {
		return batchWrites.putBatchInternal(columnId, batchPublisher, mode);
	}
	public CompletableFuture<Void> putBatchInternal(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull PutBatchMode mode,
			@NotNull Scheduler workloadScheduler) throws RocksDBException {
		return batchWrites.putBatchInternal(columnId, batchPublisher, mode, workloadScheduler);
	}
	public CompletableFuture<Void> mergeBatchInternal(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull MergeBatchMode mode) throws RocksDBException {
		return batchWrites.mergeBatchInternal(columnId, batchPublisher, mode);
	}
	public CompletableFuture<Void> mergeBatchInternal(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull MergeBatchMode mode,
			@NotNull Scheduler workloadScheduler) throws RocksDBException {
		return batchWrites.mergeBatchInternal(columnId, batchPublisher, mode, workloadScheduler);
	}

	@VisibleForTesting
	public SSTWriter getSSTWriter(long colId,
			@Nullable GlobalDatabaseConfig globalDatabaseConfigOverride,
			boolean forceNoOptions,
			boolean ingestBehind) throws RocksDBException {
		return batchWrites.getSSTWriter(colId, globalDatabaseConfigOverride, forceNoOptions, ingestBehind);
	}

	@Override
	public void putBatch(long columnId, @NotNull Publisher<@NotNull KVBatch> batchPublisher, @NotNull PutBatchMode mode)
			throws RocksDBException {
		batchWrites.putBatch(columnId, batchPublisher, mode);
	}

	@Override
	public void mergeBatch(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull MergeBatchMode mode) throws RocksDBException {
		batchWrites.mergeBatch(columnId, batchPublisher, mode);
	}

	/**
	 * The failure of a merge operator: the same on every replica that runs the same operator over the same data. See
	 * {@link FFMAbstractMergeOperator#takeFailureOnThisThread}.
	 */
	public static RocksDBException mergeOperatorFailed(Throwable operatorFailure) {
		// Described without what differs between processes (addresses, identity hashes): the text becomes a result.
		return RocksDBException.of(RocksDBErrorType.MERGE_OPERATOR_FAILED,
				"Merge operator failed: " + Failures.describe(operatorFailure), operatorFailure);
	}

	/**
	 * Attempts the embedded fast path for a current-value Get.
	 *
	 * @return a result (including a missing result), or {@code null} when this
	 *     column/database must use the ordinary API path.
	 */
	public @Nullable FastGetResult tryGetFast(long columnId, Keys keys, FastGetOutput output) {
		return pointReads.tryGetFast(columnId, keys, output);
	}

	@Override
	public <T> T get(long transactionOrUpdateId, long columnId, Keys keys, RequestGet<? super Buf, T> requestType)
			throws RocksDBException {
		return pointReads.<T>get(transactionOrUpdateId, columnId, keys, requestType);
	}
	public <T> T get(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			RequestGet<? super Buf, T> requestType,
			WorkloadProfile workloadProfile) throws RocksDBException {
		return pointReads.<T>get(transactionOrUpdateId, columnId, keys, requestType, workloadProfile);
	}

	@Override
	public MultiGetPage getMulti(long columnId, @NotNull List<Keys> keys, @NotNull RangeBudget budget) {
		return pointReads.getMulti(columnId, keys, budget);
	}
	public MultiGetPage getMulti(long columnId, @NotNull List<Keys> keys, @NotNull RangeBudget budget,
			WorkloadProfile workloadProfile, long contextMonotonicDeadlineNanos) {
		return pointReads.getMulti(columnId, keys, budget, workloadProfile, contextMonotonicDeadlineNanos);
	}
	public CompletableFuture<MultiGetPage> getMultiAsyncInternal(long columnId, @NotNull List<Keys> keys,
			@NotNull RangeBudget budget, WorkloadProfile workloadProfile, Executor executor,
			long contextMonotonicDeadlineNanos) {
		return pointReads.getMultiAsyncInternal(columnId, keys, budget, workloadProfile, executor, contextMonotonicDeadlineNanos);
	}

	/**
	 * Reads an ordered page whose value views remain valid until the result is closed.
	 * The caller must retain the result throughout any asynchronous serialization.
	 */
	public PinnedMultiGetResult getMultiPinned(long columnId, @NotNull List<Keys> keys,
			@NotNull RangeBudget budget, WorkloadProfile workloadProfile,
			long monotonicDeadlineNanos, BooleanSupplier cancelled) {
		return pointReads.getMultiPinned(columnId, keys, budget, workloadProfile, monotonicDeadlineNanos, cancelled);
	}

	/** Owns the native pins; page access and serialization must finish before close. */
	public final class PinnedMultiGetResult implements AutoCloseable {
		private final MultiGetPage page;
		private final @Nullable PinnedMultiGet pinned;
		private final ColumnInstance.ColumnUse columnUse;
		private final AtomicBoolean closed = new AtomicBoolean();

		PinnedMultiGetResult(MultiGetPage page, @Nullable PinnedMultiGet pinned,
				ColumnInstance.ColumnUse columnUse) {
			this.page = page;
			this.pinned = pinned;
			this.columnUse = columnUse;
		}

		public MultiGetPage page() {
			if (closed.get()) {
				throw new IllegalStateException("Pinned MultiGet result has already been closed");
			}
			return page;
		}

		@Override
		public void close() {
			if (!closed.compareAndSet(false, true)) {
				return;
			}
			try {
				if (pinned != null) {
					pinned.close();
				}
			} finally {
				try {
					columnUse.close();
				} finally {
					ops.endOp();
				}
			}
		}
	}

	@Override
	public List<Boolean> existsMulti(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys) throws RocksDBException {
		return existsMultiOps.existsMulti(transactionId, columnId, keys);
	}
	public List<Boolean> existsMulti(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			WorkloadProfile workloadProfile,
			long contextMonotonicDeadlineNanos) throws RocksDBException {
		return existsMultiOps.existsMulti(transactionId, columnId, keys, workloadProfile, contextMonotonicDeadlineNanos);
	}

	/**
	 * Execute one bounded native MultiGet per composite scheduler turn while retaining
	 * one logical deadline, snapshot and ordered result accumulator for the request.
	 */
	public CompletableFuture<List<Boolean>> existsMultiAsyncInternal(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			WorkloadProfile workloadProfile,
			Executor executor,
			long contextMonotonicDeadlineNanos) {
		return existsMultiOps.existsMultiAsyncInternal(transactionId, columnId, keys, workloadProfile, executor, contextMonotonicDeadlineNanos);
	}

	@Override
	public long openIterator(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			Duration iteratorLeaseTtl) throws RocksDBException {
		return iterators.openIterator(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, iteratorLeaseTtl);
	}
	public long openIterator(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs,
			WorkloadProfile workloadProfile) throws RocksDBException {
		return iterators.openIterator(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, timeoutMs, workloadProfile);
	}

	@Override
	public void closeIterator(long iteratorId) throws RocksDBException {
		iterators.closeIterator(iteratorId);
	}

	/**
	 * Closes every iterator open on the column named {@code name}, if it exists, and returns how many it closed. A
	 * column deletion waits for the iterators of the column to end ({@code stopNewUsesAndWait}): a replicated server
	 * calls this before it applies one, so that its apply does not stop until a client closes them or their lease ends.
	 * Their owners then get "no iterator" on their next call, as after an expiry.
	 */
	public int closeIteratorsOfColumn(@NotNull String name) {
		return iterators.closeIteratorsOfColumn(name);
	}

	/** {@link #closeIteratorsOfColumn(String)} by column id; 0 if the column does not exist. */
	public int closeIteratorsOfColumn(long columnId) {
		return iterators.closeIteratorsOfColumn(columnId);
	}

	@Override
	public void seekTo(long iterationId, @NotNull Keys keys) throws RocksDBException {
		iterators.seekTo(iterationId, keys);
	}

	@Override
	public <T> T subsequent(long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super Buf, T> requestType) throws RocksDBException {
		return iterators.<T>subsequent(iterationId, skipCount, takeCount, requestType);
	}

	/**
	 * Advance an explicit iterator without materializing values and report the exact
	 * number of logical entries consumed. Async adapters use the count to stop
	 * scheduling slices immediately when the iterator is exhausted.
	 */
	public long advanceIteratorInternal(long iterationId, long maxCount) {
		return iterators.advanceIteratorInternal(iterationId, maxCount);
	}

	/**
	 * Advance one cooperative iterator slice. With no scheduler competition this retains the
	 * configured item-sized fast path. Once a peer needs the worker, elapsed service is checked
	 * after every logical iterator step, so the non-preemptible tail is one RocksDB iterator step
	 * rather than an entire fixed 4,096-entry call.
	 */
	public IteratorAdvanceQuantum advanceIteratorQuantumInternal(long iterationId,
			long maxCount,
			long maximumDurationNanos,
			RWScheduler.CooperativeContext context) {
		return iterators.advanceIteratorQuantumInternal(iterationId, maxCount, maximumDurationNanos, context);
	}

	/**
	 * Materialize one cooperative MULTI slice. Item count is always bounded. Byte and monotonic
	 * time limits become active only while another scheduler profile competes, preserving idle
	 * throughput while bounding peer delay to the configured budget plus one indivisible value
	 * decode/iterator advance.
	 */
	public IteratorMultiQuantum readIteratorQuantumInternal(long iterationId,
			long maxCount,
			long maximumBytes,
			long maximumDurationNanos,
			RWScheduler.CooperativeContext context) {
		return iterators.readIteratorQuantumInternal(iterationId, maxCount, maximumBytes, maximumDurationNanos, context);
	}

	public record IteratorAdvanceQuantum(long advanced, boolean exhausted, boolean checkpointRequested) {
	}

	public record IteratorMultiQuantum(List<Buf> values,
			long decodedBytes,
			boolean exhausted,
			boolean checkpointRequested) {
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
			@NotNull RequestReduceRange<? super KV, T> requestType) throws RocksDBException {
		return ranges.<T>reduceRange(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType);
	}
	public <T> T reduceRange(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@NotNull RequestReduceRange<? super KV, T> requestType,
			long monotonicDeadlineNanos) throws RocksDBException {
		return ranges.<T>reduceRange(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, monotonicDeadlineNanos);
	}

	@Override
	public <T> RangePage<T> getRangePage(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@Nullable Keys resumeAfter,
			@NotNull RequestGetRange<? super KV, T> requestType,
			@NotNull RangeBudget budget) throws RocksDBException {
		return ranges.<T>getRangePage(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, resumeAfter, requestType, budget);
	}
	public <T> RangePage<T> getRangePage(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@Nullable Keys resumeAfter,
			@NotNull RequestGetRange<? super KV, T> requestType,
			@NotNull RangeBudget budget,
			long monotonicDeadlineNanos) throws RocksDBException {
		return ranges.<T>getRangePage(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, resumeAfter, requestType, budget, monotonicDeadlineNanos);
	}

	@Override
	public <T> Stream<T> getRange(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@NotNull RequestType.RequestGetRange<? super KV, T> requestType) throws RocksDBException {
		return ranges.<T>getRange(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType);
	}

	/**
	 * See: {@link GetRange}.
	 */
	public <T> Publisher<T> getRangeAsyncInternal(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			RequestGetRange<? super KV, T> requestType) throws RocksDBException {
		return ranges.<T>getRangeAsyncInternal(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType);
	}
	public <T> Publisher<T> getRangeAsyncInternal(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			RequestGetRange<? super KV, T> requestType,
			RequestContext context,
			@NotNull WorkloadProfile workloadProfile) throws RocksDBException {
		return ranges.<T>getRangeAsyncInternal(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, context, workloadProfile);
	}

	/** Count logical entries in bounded physical slices without materializing one signal per entry. */
	public Mono<Long> countRangeAsyncInternal(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse) {
		return ranges.countRangeAsyncInternal(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse);
	}
	public Mono<Long> countRangeAsyncInternal(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			RequestContext context,
			@NotNull WorkloadProfile workloadProfile) {
		return ranges.countRangeAsyncInternal(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, context, workloadProfile);
	}

	@VisibleForTesting
	protected void enableRawScanFileDeletions(RocksDB rocksDB) throws org.rocksdb.RocksDBException {
		rocksDB.enableFileDeletions();
	}

	@VisibleForTesting
	protected void createRawScanHardLink(Path source, Path target) throws IOException {
		var observer = rawScanHardLinkObserver;
		if (observer != null) {
			observer.run();
		}
		Files.createLink(target, source);
	}

	@VisibleForTesting
	protected long rawScanPinMaxDurationNanos() {
		return RAW_SCAN_PIN_MAX_DURATION_NANOS;
	}
	public Flux<SerializedKVBatch> scanRawAsyncInternal(long columnId, int shardIndex, int shardCount) {
		return rawScan.scanRawAsyncInternal(columnId, shardIndex, shardCount);
	}
	public Flux<SerializedKVBatch> scanRawAsyncInternal(long columnId,
			int shardIndex,
			int shardCount,
			@NotNull Scheduler workloadScheduler) {
		return rawScan.scanRawAsyncInternal(columnId, shardIndex, shardCount, workloadScheduler);
	}
	public Flux<SerializedKVBatch> scanRawAsyncInternal(long columnId,
			int shardIndex,
			int shardCount,
			@NotNull Scheduler workloadScheduler,
			@NotNull RWScheduler.WorkloadExecutor workloadExecutor) {
		return rawScan.scanRawAsyncInternal(columnId, shardIndex, shardCount, workloadScheduler, workloadExecutor);
	}
	public Flux<RawScanEvent> scanRawResumableAsyncInternal(long columnId,
			int shardIndex,
			int shardCount,
			Set<RawSstToken> completedSsts,
			@NotNull Scheduler workloadScheduler) {
		return rawScan.scanRawResumableAsyncInternal(columnId, shardIndex, shardCount, completedSsts, workloadScheduler);
	}
	public Stream<SerializedKVBatch> scanRaw(long columnId, int shardIndex, int shardCount) {
		return rawScan.scanRaw(columnId, shardIndex, shardCount);
	}

	@Override
	public void flush() {
		maintenance.flush();
	}

	@Override
	public void flushWal() {
		maintenance.flushWal();
	}

	/**
	 * Starts a flush of every column's memtables and returns without waiting for it: for a caller that will flush
	 * synchronously soon (a checkpoint) and wants that flush to find little left to write.
	 */
	public void requestFlush() {
		maintenance.requestFlush();
	}

    @Override
    public ColumnTableProperties getTableProperties(long columnId) {
    	return maintenance.getTableProperties(columnId);
    }

    @Override
    public SstMaintenance.Metadata getSstMetadata(long columnId, int level) {
    	return maintenance.getSstMetadata(columnId, level);
    }

    @Override
    public SstMaintenance.Result compactFiles(SstMaintenance.Request request) {
    	return maintenance.compactFiles(request);
    }

	@Override
	public void compact() {
		maintenance.compact();
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
		return columnOps.getAllColumnDefinitions();
	}

	Buf dbGet(Tx tx, ColumnInstance col, ReadOptions readOptions, Buf calculatedKey)
			throws org.rocksdb.RocksDBException {
		return dbGet(tx, col, readOptions, calculatedKey, false);
	}

	Buf dbGet(Tx tx, ColumnInstance col, ReadOptions readOptions, Buf calculatedKey, boolean forUpdate)
			throws org.rocksdb.RocksDBException {
		long readStarted = System.nanoTime();
		boolean recordRead = tx != null ? !(forUpdate || tx.isFromGetForUpdate()) : !fastGet;
		try {
			if (tx != null) {
				byte[] previousRawBucketByteArray;
				byte[] calculatedKeyArray = EmbeddedDBWriteElision.jniArray(calculatedKey);
				if (forUpdate || tx.isFromGetForUpdate()) {
					previousRawBucketByteArray = writes.pointGetForUpdate(tx, readOptions, col.cfh(), calculatedKeyArray);
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

		} finally { if (recordRead) storageMetrics.recordCompactionForegroundRead(readStarted, 0); }
	}

	@Nullable
	Buf dbGetWithDefaultOptions(@Nullable Tx tx, ColumnInstance col, Buf calculatedKey)
			throws org.rocksdb.RocksDBException {
		return dbGetWithDefaultOptions(tx, col, calculatedKey, false);
	}

	@Nullable
	Buf dbGetWithDefaultOptions(@Nullable Tx tx, ColumnInstance col, Buf calculatedKey, boolean forUpdate)
			throws org.rocksdb.RocksDBException {
		if (tx == null && fastGet) {
			return dbGetFast(col.cfh(), calculatedKey);
		}
		try (var readOptions = newPointReadOptions(null)) {
			return dbGet(tx, col, readOptions, calculatedKey, forUpdate);
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
		long readStarted = System.nanoTime();
		try {
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

		} finally { storageMetrics.recordCompactionForegroundRead(readStarted, 0); }
	}

	ColumnInstance getColumn(long columnId) {
		var col = columns.get(columnId);
		if (col != null) {
			return col;
		} else {
			throw RocksDBException.of(RocksDBErrorType.COLUMN_NOT_FOUND, "No column with id " + columnId);
		}
	}

	ColumnInstance beginColumnUse(long columnId) {
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

	static boolean tryBeginColumnUse(ColumnInstance column) {
		try {
			column.beginUse();
			return true;
		} catch (IllegalStateException closing) {
			return false;
		}
	}

	ColumnInstance.ColumnUse acquireColumnUse(long columnId) {
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

	void requireLiveTransaction(long updateId, Tx tx) {
		if (txs.get(updateId) != tx || !tx.val().isOwningHandle()) {
			throw RocksDBException.of(RocksDBErrorType.TRANSACTION_NOT_FOUND, "No transaction with id " + updateId);
		}
	}

	Tx getTransaction(long transactionId, boolean allowGetForUpdate) {
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

	Tx getTransaction(long transactionId,
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

	void retainResourceLease() {
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

	void releaseResourceLease() {
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
	public int getActiveExistsMultiRequestCount() {
		return activeExistsMultiRequests.size();
	}



	@VisibleForTesting
	public boolean isExpiredRangeCleanupScheduledForTesting() {
		return !expiredRangeCleanupTask.isCancelled() && !expiredRangeCleanupTask.isDone();
	}

	@VisibleForTesting
	public DatabaseConfig getConfig() {
		return config;
	}

	/** No native lookup or extra permit acquisition is needed to select column admission. */
	public RWScheduler getSchedulerForColumn(long columnId) {
		return workloadGroups.getOrDefault(columnWorkloadNames.getOrDefault(columnId, "default"), scheduler);
	}

	public RWScheduler getSchedulerForIterator(long iteratorId) {
		var entry = its.get(iteratorId);
		return entry != null && entry.objs() instanceof IteratorState state ? state.workloadScheduler : scheduler;
	}

	/** Database-wide operations, including transaction commit and WAL polling, use global admission. */
	public RWScheduler getSchedulerForCommand(RocksDBAPICommand<?, ?, ?> command) {
		return switch (command) {
			case RocksDBAPICommand.RocksDBAPICommandSingle.DeleteColumn value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.EstimateNumKeys value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.Put<?> value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.Delete<?> value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<?> value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.DeleteRange value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<?> value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.Merge<?> value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<?> value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.Get<?> value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.ExistsMulti value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.GetMulti value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<?> value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.GetRangePage<?> value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?> value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandStream.ScanRaw value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.RocksDBAPICommandStream.ScanRawResumable value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.GetTableProperties value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.GetSstMetadata value -> getSchedulerForColumn(value.columnId());
			case RocksDBAPICommand.CompactFiles value -> getSchedulerForColumn(value.request().columnId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.SeekTo value -> getSchedulerForIterator(value.iterationId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent<?> value -> getSchedulerForIterator(value.iterationId());
			default -> scheduler;
		};
	}

	/** Resolved workload limits shared by scheduler and bounded-operation implementations. */
	public WorkloadSettings getWorkloadSettings() {
		return workloadSettings;
	}

	AbstractSlice<?> toSlice(Buf calculatedKey) {
		return new Slice(calculatedKey.asArray());
	}

	KV decodeKV(ColumnInstance col, Buf calculatedKey, Buf calculatedValue) {
		var keys = col.decodeKeys(calculatedKey, calculatedValue);
		var value = col.decodeValue(calculatedValue);
		return new KV(new Keys(keys), value);
	}

	KV decodeBucketEntry(ColumnInstance col, Buf calculatedKey, Entry<Buf[], Buf> bucketEntry) {
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

	static long decodedKVBytes(KV kv) {
		long bytes = kv.value() != null ? kv.value().size() : 0L;
		for (var key : kv.keys().keys()) {
			bytes = saturatingAdd(bytes, key.size());
		}
		return bytes;
	}

	static long saturatingAdd(long left, long right) {
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

	static interface ActionLoggerConsumer {

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

	/**
	 * Schedule a native database step while keeping SafeShutdown accounting tied to
	 * actual task completion. Cancellation stops delivery to the subscriber but does
	 * not pretend that an already queued or running JNI call has finished.
	 */
	<T> Mono<T> scheduleTracked(Scheduler target, Callable<T> callable) {
		return scheduleTracked(target, callable, null, 0L);
	}

	<T> Mono<T> scheduleTracked(Scheduler target,
			Callable<T> callable,
			@Nullable Consumer<? super T> lateSuccessCleanup,
			long estimatedBytes) {
		if (estimatedBytes < 0L) {
			throw new IllegalArgumentException("estimatedBytes must not be negative");
		}
		return Mono.create(sink -> {
			final int queued = 0;
			final int running = 1;
			final int finished = 2;
			final int cancelledBeforeStart = 3;
			var state = new AtomicInteger(queued);
			var task = Disposables.swap();
			final class TrackedNativeTask implements Runnable,
					RWScheduler.EstimatedWork,
					RWScheduler.RejectionAwareTask {

				private @Nullable Object pendingResult;
				private boolean deliveryCancelled;

				@Override
				public long estimatedBytes() {
					return estimatedBytes;
				}

				@Override
				public void run() {
					if (!state.compareAndSet(queued, running)) {
						return;
					}
					try {
						var result = callable.call();
						publishResult(result);
					} catch (Throwable error) {
						if (!isDeliveryCancelled()) {
							sink.error(error);
						} else {
							logger.debug("Native task failed after subscriber cancellation", error);
						}
					} finally {
						state.set(finished);
						ops.endOp();
					}
				}

				@Override
				public void reject(RuntimeException failure) {
					failBeforeStart(failure, "Tracked native task was rejected after subscriber cancellation");
				}

				private void failBeforeStart(Throwable failure, String lateMessage) {
					if (!state.compareAndSet(queued, finished)) {
						return;
					}
					ops.endOp();
					if (!isDeliveryCancelled()) {
						sink.error(failure);
					} else {
						logger.debug(lateMessage, failure);
					}
				}

				private void cancelDelivery() {
					Object cancelledResult;
					synchronized (this) {
						deliveryCancelled = true;
						cancelledResult = pendingResult;
						pendingResult = null;
					}
					cleanupCancelledResult(cancelledResult);
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

				private synchronized boolean isDeliveryCancelled() {
					return deliveryCancelled;
				}

				private void publishResult(T result) {
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
					if (encodedResult == null || lateSuccessCleanup == null) return;
					try {
						lateSuccessCleanup.accept(ReactorResultOwnership.decode(encodedResult));
					} catch (Throwable cleanupError) {
						logger.warn("Failed to clean a native result after subscriber cancellation",
								cleanupError);
					}
				}
			}
			var trackedTask = new TrackedNativeTask();
			try {
				ops.beginOp();
			} catch (Throwable error) {
				sink.error(error);
				return;
			}
			sink.onCancel(() -> {
				trackedTask.cancelDelivery();
				if (state.compareAndSet(queued, cancelledBeforeStart)) {
					// The queued callable will never own the SafeShutdown lease.
					task.dispose();
					ops.endOp();
				} else {
					// A running native call owns the lease until its finally block.
					task.dispose();
				}
			});
			sink.onDispose(trackedTask::deliveryDisposed);
			try {
				task.replace(target.schedule(trackedTask));
			} catch (Throwable error) {
				trackedTask.failBeforeStart(error,
						"Task scheduling failed after subscriber cancellation");
			}
		});
	}
}
