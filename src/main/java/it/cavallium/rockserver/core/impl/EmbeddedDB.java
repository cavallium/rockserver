package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.common.Utils.dummyRocksDBEmptyValue;
import static it.cavallium.rockserver.core.common.Utils.emptyBuf;
import static it.cavallium.rockserver.core.common.Utils.toBuf;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
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
import reactor.core.publisher.Mono;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import org.rocksdb.TransactionLogIterator;
import org.rocksdb.WriteBatch;
import reactor.core.publisher.SynchronousSink;

public class EmbeddedDB implements RocksDBSyncAPI, InternalConnection, Closeable {

	private static final long ITERATOR_REFRESH_INTERVAL = 1_000_000;
	private static final int RANGE_READ_CHUNK_SIZE = 1_024;
	private static final int RANGE_READ_MAX_PHYSICAL_KEYS_PER_CHUNK = 4_096;
	private static final long RANGE_READ_MAX_DECODED_BYTES_PER_CHUNK = 2 * SizeUnit.MB;
	private static final int EXISTS_MULTI_MAX_KEYS_PER_NATIVE_CALL = 4_096;
	private static final long EXISTS_MULTI_MAX_KEY_BYTES_PER_NATIVE_CALL = 2 * SizeUnit.MB;
	private static final int RAW_SCAN_MAX_ENTRIES_PER_CHUNK = 65_536;
	private static final long RAW_SCAN_MAX_BYTES_PER_CHUNK = 2 * SizeUnit.MB;
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
	private final Set<CdcPollCursor> activeCdcPollCursors = ConcurrentHashMap.newKeySet();
	private final SafeShutdown ops;
	private final Object columnEditLock = new Object();
	private final DatabaseConfig config;
	private final RocksDBObjects refs;
	private final @Nullable Cache cache;
	private final MetricsManager metrics;
	private final String name;
	private final List<Meter> meters = new ArrayList<>();
	private final Timer openTransactionTimer;
	private final Timer closeTransactionTimer;
	private final Timer closeFailedUpdateTimer;
	private final Timer createColumnTimer;
	private final Timer deleteColumnTimer;
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
	private final RocksDBStatistics rocksDBStatistics;
	private final boolean fastGet;
	private final @Nullable FFMRocksDBGet fastGetReader;
	private final LongAdder fastGetNativeErrorFallbacks = new LongAdder();
	private volatile long getRangeIteratorRefreshInterval = ITERATOR_REFRESH_INTERVAL;
	private volatile @Nullable Consumer<Boolean> rangeReadOptionsObserver;
	private volatile @Nullable Runnable rangeIteratorOpenObserver;
	private volatile @Nullable Consumer<Integer> rangeReadChunkSizeObserver;
	private volatile @Nullable Runnable rangeCountChunkObserver;
	private volatile @Nullable Runnable existsMultiChunkObserver;
	private volatile @Nullable Runnable existsMultiSnapshotObserver;
	private volatile @Nullable Runnable cdcPollTailCapturedObserver;
	private volatile @Nullable Runnable cdcWalIteratorOpenObserver;
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

		this.metrics = new MetricsManager(config);
		Timer loadTimer = createTimer(Tags.of("action", "load"));
		this.openTransactionTimer = createActionTimer(OpenTransaction.class);
		this.closeTransactionTimer = createActionTimer(CloseTransaction.class);
		this.closeFailedUpdateTimer = createActionTimer(CloseFailedUpdate.class);
		this.createColumnTimer = createActionTimer(CreateColumn.class);
		this.deleteColumnTimer = createActionTimer(DeleteColumn.class);
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
					.builder("rockserver.pending.ops", ops, m -> (double) m.getPendingOpsCount())
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
						try {
							column = new String(c.cfh().getName(), StandardCharsets.UTF_8);
						} catch (org.rocksdb.RocksDBException e) {
							logger.debug("Failed to resolve column name for logging", e);
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
		this.config = config;
		var loadedDb = RocksDBLoader.load(path, config, logger);
		this.db = loadedDb.db();
		this.dbOptions = loadedDb.dbOptions();
		this.refs = loadedDb.refs();
		this.cache = loadedDb.cache();
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
		this.rocksDBStatistics = new RocksDBStatistics(name, dbOptions.statistics(), metrics, cache, this::getLongProperty, this::getPerCfLongProperty, memoryUpperBoundConfig);
		try {
			int readCap = Objects.requireNonNullElse(config.parallelism().read(), Runtime.getRuntime().availableProcessors());
			int writeCap = Objects.requireNonNullElse(config.parallelism().write(),
					Runtime.getRuntime().availableProcessors()
			);
			this.scheduler = new RWScheduler(readCap, writeCap, "db[" + name + "]");
			this.fastGet = config.global().enableFastGet();
			this.fastGetReader = fastGet ? new FFMRocksDBGet(db.get(), (long) readCap + writeCap) : null;
		} catch (GestaltException e) {
			throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, "Can't get the scheduler parallelism");
		}
		this.leakScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("db-leak-scheduler"));

		leakScheduler.scheduleWithFixedDelay(this::cleanupExpiredTransactionsNow, 1, 1, TimeUnit.MINUTES);

		leakScheduler.scheduleWithFixedDelay(this::cleanupExpiredIteratorsNow, 1, 1, TimeUnit.MINUTES);
		this.expiredRangeCleanupTask = leakScheduler.scheduleWithFixedDelay(
				this::cleanupExpiredRangesNow, 1, 1, TimeUnit.MINUTES);

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
			var columnSchemasColumnDescriptor = new ColumnFamilyDescriptor(COLUMN_SCHEMAS_COLUMN);
			try {
				columnSchemasColumnDescriptorHandle = db.get().createColumnFamily(columnSchemasColumnDescriptor);
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
			var mergeOperatorsColumnDescriptor = new ColumnFamilyDescriptor(MERGE_OPERATORS_COLUMN);
			try {
				mergeOperatorsColumnDescriptorHandle = db.get().createColumnFamily(mergeOperatorsColumnDescriptor);
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
			var cdcMetaDescriptor = new ColumnFamilyDescriptor(CDC_META_COLUMN);
			try {
				cdcMetaColumnDescriptorHandle = db.get().createColumnFamily(cdcMetaDescriptor);
			} catch (org.rocksdb.RocksDBException e) {
				throw new IOException("Cannot create CDC meta column", e);
			}
		} else {
			this.cdcMetaColumnDescriptorHandle = existingCdcMetaColumnDescriptorOptional.get().getValue();
		}

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
	private ColumnInstance unregisterColumn(long id) {
		synchronized (columnEditLock) {
			var col = this.columns.remove(id);
			Objects.requireNonNull(col, () -> "Column does not exist: " + id);
			String name;
			try {
				name = new String(col.cfh().getName(), StandardCharsets.UTF_8);
			} catch (org.rocksdb.RocksDBException e) {
				throw new RuntimeException(e);
			}

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
			logger.info("Closing... waiting for ops");
			ops.closeAndWait(pendingOpsTimeoutMs);
			// Normal shutdown path
			logger.info("Ops finished, closing resources");
			closeResources(false);
		} catch (TimeoutException e) {
			logger.error(
					"Some operations lasted more than {} ms, forcing database shutdown... pendingOps={}, openTxs={}, openIterators={}",
					pendingOpsTimeoutMs,
					ops.getPendingOpsCount(),
					txs.size(),
					its.size()
			);
			logger.warn("Timeout! Forcing shutdown");
			// Best-effort forced cleanup of leaked resources to avoid native memory retention
			forceCloseLeakedResources();
			// After forcing close of leaked resources, proceed to close DB/native resources defensively
			closeResources(true);

			if (testing && ops.getPendingOpsCount() > 0) {
				throw new IllegalStateException("Some operations lasted more than " + pendingOpsTimeoutMs + " ms! pendingOps=" + ops.getPendingOpsCount() + ", openTxs=" + txs.size() + ", openIterators=" + its.size());
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
		// Meters and statistics
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

		// Logical range and CDC polls keep their native iterators between bounded
		// scheduler slices. SafeShutdown has already excluded new/in-flight slices, so
		// close those idle cursors before column handles or the database itself.
		closeActiveCdcPollCursors();
		closeActiveRangeResources();

		// Close any remaining transactions to avoid hanging on DB close
		if (!txs.isEmpty()) {
			logger.warn("Closing {} remaining transactions", txs.size());
			for (long transactionId : new ArrayList<>(txs.keySet())) {
				try {
					closeTransactionInternal(transactionId, false);
				} catch (Throwable t) {
					logger.error("Error closing remaining transaction", t);
				}
			}
		}
		// Close any remaining iterators
		if (!its.isEmpty()) {
			logger.warn("Closing {} remaining iterators", its.size());
			for (long iteratorId : new ArrayList<>(its.keySet())) {
				try {
					closeIteratorInternal(iteratorId);
				} catch (Throwable t) {
					logger.error("Error closing remaining iterator", t);
				}
			}
		}
		if (forced && ops.getPendingOpsCount() > 0) {
			logger.error("Leaving RocksDB native resources allocated because {} operations are still active; "
					+ "freeing DB, column, or FFM memory here could crash the process",
					ops.getPendingOpsCount());
			return;
		}

		// No operations remain, so native key scratch and C-API wrapper segments can be released before their handles.
		if (fastGetReader != null) {
			try {
				fastGetReader.close();
			} catch (Throwable t) {
				logger.error("Error closing FFM fast-get{}", forced ? " (forced)" : "", t);
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
			logger.info("Closing {} active range cursors", ranges.size());
		}
		for (var range : ranges) {
			try {
				range.close();
			} catch (Throwable error) {
				logger.error("Error closing active range cursor", error);
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

	/**
	 * Force-close any remaining transactions/iterators and balance pending ops. Invoked during shutdown if SafeShutdown
	 * times out.
	 */
	private void forceCloseLeakedResources() {
		int closedTx = 0;
		int closedIts = 0;
		try {
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

			try {
				ops.waitForExit(2_000);
			} catch (TimeoutException te) {
				logger.warn("Pending operations still not zero after forced shutdown: {}", ops.getPendingOpsCount());
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
	public void setGetRangeIteratorRefreshIntervalForTesting(long refreshInterval) {
		if (refreshInterval <= 0) {
			throw new IllegalArgumentException("refreshInterval must be positive");
		}
		this.getRangeIteratorRefreshInterval = refreshInterval;
	}

	@VisibleForTesting
	public void setRangeReadOptionsObserverForTesting(@Nullable Consumer<Boolean> observer) {
		this.rangeReadOptionsObserver = observer;
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
	public void setExistsMultiChunkObserverForTesting(@Nullable Runnable observer) {
		this.existsMultiChunkObserver = observer;
	}

	@VisibleForTesting
	public void setExistsMultiSnapshotObserverForTesting(@Nullable Runnable observer) {
		this.existsMultiSnapshotObserver = observer;
	}

	@VisibleForTesting
	public void setCdcPollTailCapturedObserverForTesting(@Nullable Runnable observer) {
		this.cdcPollTailCapturedObserver = observer;
	}

	@VisibleForTesting
	public void setCdcWalIteratorOpenObserverForTesting(@Nullable Runnable observer) {
		this.cdcWalIteratorOpenObserver = observer;
	}

	private void notifyRangeIteratorOpened() {
		var observer = rangeIteratorOpenObserver;
		if (observer != null) {
			observer.run();
		}
	}

	@Override
	public long openTransaction(long timeoutMs) {
		var start = System.nanoTime();
		actionLogger.logAction("OpenTransaction", start, null, null, null, null, null, timeoutMs, null);
		try {
			return allocateTransactionInternal(openTransactionInternal(timeoutMs, false));
		} finally {
			var end = System.nanoTime();
			openTransactionTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private long allocateTransactionInternal(Tx tx) {
		return FastRandomUtils.allocateNewValue(txs, tx, Long.MIN_VALUE, -2);
	}

	private Tx openTransactionInternal(long timeoutMs, boolean isFromGetForUpdate) {
		// Open the transaction operation, do not close until the transaction has been closed
		ops.beginOp();
		try {
			var expirationTimestamp = timeoutMs + System.currentTimeMillis();
			TransactionalOptions txOpts = db.createTransactionalOptions(timeoutMs);
			var writeOpts = new LeakSafeWriteOptions("open-transaction-internal-write-options");
			var rocksObjects = new RocksDBObjects(writeOpts, txOpts);
			try {
				return new Tx(db.beginTransaction(writeOpts, txOpts), isFromGetForUpdate, expirationTimestamp, rocksObjects);
			} catch (Throwable ex) {
				rocksObjects.close();
				throw ex;
			}
		} catch (Throwable ex) {
			ops.endOp();
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
		// Once the native handle is gone, its lifetime operation has already been balanced.
		if (!tx.val().isOwningHandle()) {
			return true;
		}
		// The transaction's lifetime is already counted by openTransactionInternal().
		// Starting a second operation here is both redundant and incorrect during shutdown:
		// closing forbids new operations, but an already-open transaction must still be able
		// to commit or roll back so its original operation can drain.
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
					// Do not call endOp here, since the transaction is still open
					return false;
				}
			} else {
				if (tx.val().isOwningHandle()) {
					tx.val().rollback();
				}
			}
			tx.close();
			// Close the transaction operation started on open
			ops.endOp();
			return true;
		} catch (org.rocksdb.RocksDBException e) {
			try {
				tx.close();
			} catch (Throwable t) {
				e.addSuppressed(t);
			}
			// Balance the open op
			ops.endOp();
			throw RocksDBException.of(RocksDBErrorType.COMMIT_FAILED, "Transaction close failed");
		} catch (Throwable ex) {
			try {
				tx.close();
			} catch (Throwable t) {
				ex.addSuppressed(t);
			}
			// Balance the open op
			ops.endOp();
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

			ColumnInstance newCol = new ColumnInstance(oldCol.cfh(), newSchema, newOp);
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
								cache
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
									cache
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
								cache
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
		return mergeOperatorRegistry.upload(name, className, jarData);
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
					db.get().dropColumnFamily(col.cfh());
					unregisterColumn(columnId).close();
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
		try {
			var col = getColumn(columnId);
			try {
				return db.get().getLongProperty(col.cfh(), RocksDBLongProperty.ESTIMATE_NUM_KEYS.getName());
			} catch (org.rocksdb.RocksDBException e) {
				throw RocksDBException.of(RocksDBErrorType.GET_PROPERTY_ERROR, e);
			}
		} finally {
			ops.endOp();
			var end = System.nanoTime();
			estimateNumKeysTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private Long getColumnIdOrNull(@NotNull String name) {
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
						try {
							val = val.add(new BigInteger(Long.toUnsignedString(db.get().getLongProperty(ci.cfh(), name))));
						} catch (org.rocksdb.RocksDBException e) {
							if (e.getStatus().getCode() == Code.NotFound) {
								// skip
							} else {
								throw new RuntimeException(e);
							}
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
					var firstEntry = columns.entrySet().stream().findFirst().orElse(null);
					if (firstEntry == null) {
						yield BigInteger.ZERO;
					}
					try {
						yield new BigInteger(Long.toUnsignedString(db.get().getLongProperty(firstEntry.getValue().cfh(), name)));
					} catch (org.rocksdb.RocksDBException e) {
						if (e.getStatus().getCode() == Code.NotFound) {
							yield BigInteger.ZERO;
						} else {
							throw new RuntimeException(e);
						}
					}
				}
			};
		} finally {
			ops.endOp();
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
				try {
					String colName = new String(ci.cfh().getName());
					long value = db.get().getLongProperty(ci.cfh(), name);
					result.merge(colName, value, Long::sum);
				} catch (org.rocksdb.RocksDBException e) {
					if (e.getStatus().getCode() != Code.NotFound) {
						throw new RuntimeException(e);
					}
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
		try {
			// Column id
			var col = getColumn(columnId);
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
		try {
			// Column id
			var col = getColumn(columnId);
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
		try {
			var col = getColumn(columnId);
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
		try {
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

			var col = getColumn(columnId);
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
		try {
			var col = getColumn(columnId);
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
		try {
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

			var col = getColumn(columnId);
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

			List<T> responses =
					requestType instanceof RequestType.RequestNothing<?> ? null : new ArrayList<>(keysList.size());
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
			var end = System.nanoTime();
			putMultiTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
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
				responses = requestType instanceof RequestType.RequestNothing<?> ? null : new ArrayList<>(keysList.size());
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
		try {
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

			var col = getColumn(columnId);
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
						.publishOn(scheduler.write())
						.doOnNext(state::write)
						.then(Mono.fromRunnable(state::writePending)),
				BatchWriteState::close,
				true
			);
		return operation.subscribeOn(scheduler.write())
				.onErrorMap(error -> !(error instanceof RocksDBException),
						error -> RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, error))
				.doFinally(ignored -> putBatchTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS))
				.toFuture();
	}

	public CompletableFuture<Void> mergeBatchInternal(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull MergeBatchMode mode) throws RocksDBException {
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
							Flux.from(batchPublisher).publishOn(scheduler.write()),
							128,
							4096,
							Duration.ofMillis(10)
						)
						.doOnNext(state::write)
						.then(Mono.fromRunnable(state::writePending)),
				BatchWriteState::close,
				true
			);
		return operation.subscribeOn(scheduler.write())
				.onErrorMap(error -> !(error instanceof RocksDBException),
						error -> RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, error))
				.toFuture();
	}

	private abstract class BatchWriteState implements AutoCloseable {

		private static final int ACTIVE = 1;
		private static final int CLOSE_REQUESTED = 1 << 1;
		private static final int CLOSED = 1 << 2;

		private final AtomicInteger lifecycle = new AtomicInteger();

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
				int state = lifecycle.get();
				if ((state & (CLOSE_REQUESTED | CLOSED)) != 0) {
					return false;
				}
				if (lifecycle.compareAndSet(state, state | ACTIVE)) {
					return true;
				}
			}
		}

		private void exit() {
			for (;;) {
				int state = lifecycle.get();
				int next = state & ~ACTIVE;
				if (lifecycle.compareAndSet(state, next)) {
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
				int state = lifecycle.get();
				if ((state & CLOSED) != 0) {
					return;
				}
				int next = state | CLOSE_REQUESTED;
				if (lifecycle.compareAndSet(state, next)) {
					if ((state & ACTIVE) == 0) {
						closeNow();
					}
					return;
				}
			}
		}

		private void closeNow() {
			for (;;) {
				int state = lifecycle.get();
				if ((state & CLOSED) != 0) {
					return;
				}
				if ((state & ACTIVE) != 0) {
					return;
				}
				if (lifecycle.compareAndSet(state, state | CLOSED)) {
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
		private ColumnInstance col;
		private DBWriter writer;
		private boolean sstEntriesWritten;

		private PutBatchState(long columnId, PutBatchMode mode, long start) {
			this.columnId = columnId;
			this.mode = mode;
			this.start = start;
			try {
				this.col = getColumn(columnId);
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
						}
					}
					case SSTWriter sst -> {
						if (sstEntriesWritten) {
							sst.writePending();
						}
					}
					case null -> { }
					default -> writer.writePending();
				}
			});
		}

		@Override
		protected void closeResources() {
			if (writer != null) {
				try {
					writer.close();
				} catch (Exception error) {
					throw new RuntimeException(error);
				}
			}
		}
	}

	private final class MergeBatchState extends BatchWriteState {

		private final long columnId;
		private final MergeBatchMode mode;
		private final long start;
		private ColumnInstance col;
		private DBWriter writer;
		private ArrayList<Map.Entry<Keys, Buf>> pendingSstEntries;

		private MergeBatchState(long columnId, MergeBatchMode mode, boolean ingestBehindEnabled, long start) {
			this.columnId = columnId;
			this.mode = mode;
			this.start = start;
			try {
				this.col = getColumn(columnId);
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
				}
			});
		}

		private void writePending() {
			runWhileOpen(() -> {
				switch (writer) {
					case WB wb -> {
						if (wb.wb().count() > 0) {
							wb.writePending();
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
						}
					}
					case null -> { }
				}
			});
		}

		@Override
		protected void closeResources() {
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

	private static void flushFullWriteBatch(DBWriter writer) {
		if (writer instanceof WB wb
				&& (wb.wb().count() >= 10_000 || wb.wb().getDataSize() >= 4 * 1024 * 1024)) {
			wb.flushAndReset();
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
							null
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
								null
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
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
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
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		}
	}

	private <U> U put(@Nullable DBWriter optionalDbWriter,
			ColumnInstance col,
			long updateId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, U> callback) throws RocksDBException {
		// Check for null value
		col.checkNullableValue(value);
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
								}
							}
						}
					}
					result = RequestType.safeCast(switch (callback) {
						case RequestType.RequestNothing<?> ignored -> null;
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

	@Override
	public <T> T get(long transactionOrUpdateId, long columnId, Keys keys, RequestGet<? super Buf, T> requestType)
			throws RocksDBException {
		var start = System.nanoTime();
		try {
			actionLogger.logAction("Get", start, columnId, keys, null, transactionOrUpdateId, null, null, requestType);
			// Column id
			var col = getColumn(columnId);
			Tx prevTx = transactionOrUpdateId != 0 ? getTransaction(transactionOrUpdateId, true) : null;
			Tx tx;
			long updateId;
			if (requestType instanceof RequestType.RequestForUpdate<?>) {
				if (prevTx == null) {
					tx = openTransactionInternal(MAX_TRANSACTION_DURATION_MS, true);
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
			var end = System.nanoTime();
			getTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	@Override
	public List<Boolean> existsMulti(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			long timeoutMs) throws RocksDBException {
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

			var deadlineMicros = readDeadlineMicros(timeoutMs);
			var col = getColumn(columnId);
			Tx tx = transactionId != 0 ? getTransaction(transactionId, false) : null;
			if (keys.isEmpty()) {
				return List.of();
			}

			for (var logicalKeys : keys) {
				if (logicalKeys == null) {
					throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "keys contains null");
				}
			}

			// Calculate the first bounded native page before deciding whether an explicit
			// snapshot is necessary. A single native MultiGet already has one consistent
			// view, so the common small-request path must not pay the snapshot lifecycle
			// cost. Only genuinely split logical requests need a view pinned across calls.
			var chunk = calculateExistsMultiChunk(col, keys, 0);
			Snapshot snapshot = chunk.nextOffset() < keys.size() ? db.get().getSnapshot() : null;
			try {
				if (snapshot != null) {
					var snapshotObserver = existsMultiSnapshotObserver;
					if (snapshotObserver != null) {
						snapshotObserver.run();
					}
				}
				try (var readOptions = newReadOptions("exists-multi-read-options")) {
					readOptions.setDeadline(deadlineMicros);
					readOptions.setFillCache(false);
					if (snapshot != null) {
						readOptions.setSnapshot(snapshot);
					}
					var result = new ArrayList<Boolean>(keys.size());
					while (true) {
						if (tx == null && !col.hasBuckets()) {
							result.addAll(existsMultiStatusOnly(col, readOptions, chunk.calculatedKeys()));
						} else {
							result.addAll(existsMultiWithValues(tx,
									col,
									readOptions,
									chunk.logicalKeys(),
									chunk.calculatedKeys()));
						}
						var chunkObserver = existsMultiChunkObserver;
						if (chunkObserver != null) {
							chunkObserver.run();
						}
						if (chunk.nextOffset() >= keys.size()) {
							break;
						}
						chunk = calculateExistsMultiChunk(col, keys, chunk.nextOffset());
					}
					return result;
				} catch (org.rocksdb.RocksDBException exception) {
					throw mapIteratorStatusException(exception);
				}
			} finally {
				if (snapshot != null) {
					db.get().releaseSnapshot(snapshot);
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
		var nativeKeys = new ArrayList<ByteBuffer>(calculatedKeys.size());
		var emptyValues = new ArrayList<ByteBuffer>(calculatedKeys.size());
		for (var calculatedKey : calculatedKeys) {
			var nativeKey = ByteBuffer.allocateDirect(calculatedKey.size());
			nativeKey.put(calculatedKey.getBackingByteArray(),
					calculatedKey.getBackingByteArrayOffset(),
					calculatedKey.getBackingByteArrayLength());
			nativeKey.flip();
			nativeKeys.add(nativeKey);
			emptyValues.add(ByteBuffer.allocateDirect(0));
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
		ops.beginOp();
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
		} finally {
			ops.endOp();
		}
	}

	@Override
	public long openIterator(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException {
		var start = System.nanoTime();
		// Open an operation that ends when the iterator is closed
		ops.beginOp();
		boolean installed = false;
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
			var col = getColumn(columnId);
			var state = new IteratorState(col, reverse);
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
					it = getTransaction(transactionId, false).val().getIterator(ro, col.cfh());
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

				itEntry = new REntry<>(it, expirationTimestamp, state);
				long iteratorId = FastRandomUtils.allocateNewValue(its, itEntry, 1, Long.MAX_VALUE);
				installed = true;
				return iteratorId;
			} catch (Throwable ex) {
				if (itEntry != null) {
					itEntry.close();
				} else {
					if (it != null) {
						it.close();
					}
					state.close();
				}
				throw ex;
			}
		} finally {
			if (!installed) {
				ops.endOp();
			}
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

	private final class IteratorState extends RocksDBObjects {

		private final ColumnInstance column;
		private final boolean reverse;
		private RocksIterator iterator;
		private java.util.ListIterator<Entry<Buf[], Buf>> bucketIterator;
		private boolean closed;

		private IteratorState(ColumnInstance column, boolean reverse) {
			this.column = column;
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
	 * transfer that guarantees both the native handle and its lifetime operation are completed
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
			// Balance the pending operation started in openIterator.
			ops.endOp();
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
						ops.endOp();
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
			var col = getColumn(columnId);


			try (var ro = newReadOptions(null)) {
				ro.setDeadline(readDeadlineMicros(timeoutMs));
				Buf calculatedStartKey = startKeysInclusive != null && startKeysInclusive.keys().length > 0 ? col.calculateKey(
						startKeysInclusive.keys()) : null;
				Buf calculatedEndKey =
						endKeysExclusive != null && endKeysExclusive.keys().length > 0 ? col.calculateKey(endKeysExclusive.keys())
								: null;
				try (var startKeySlice = calculatedStartKey != null ? toSlice(calculatedStartKey) : null; var endKeySlice =
						calculatedEndKey != null ? toSlice(calculatedEndKey) : null) {
					if (startKeySlice != null) {
						ro.setIterateLowerBound(startKeySlice);
					}
					if (endKeySlice != null) {
						ro.setIterateUpperBound(endKeySlice);
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
								var first = seekLogicalEndpoint(it, col, reverse);
								if (first == null) {
									yield new FirstAndLast<>(null, null);
								}
								var last = Objects.requireNonNull(seekLogicalEndpoint(it, col, !reverse));
								yield new FirstAndLast<>(first, last);
							}
						};
					}
				}
			}
		} finally {
			ops.endOp();
			var end = System.nanoTime();
			reduceRangeTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private @Nullable KV seekLogicalEndpoint(RocksIterator iterator, ColumnInstance column, boolean fromEnd) {
		if (fromEnd) {
			iterator.seekToLast();
		} else {
			iterator.seekToFirst();
		}
		while (iterator.isValid()) {
			var calculatedKey = toBuf(iterator.key());
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
		LongAdder totalTime = new LongAdder();
		long start = System.nanoTime();
		long deadlineMicros = readDeadlineMicros(timeoutMs);
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

		return Flux.<List<T>, RangeCursor>generate(
				() -> openRangeCursor(transactionId,
						columnId,
						startKeysInclusive,
						endKeysExclusive,
						reverse,
						deadlineMicros,
						fillCache,
						true,
						totalTime),
				(cursor, sink) -> {
					@SuppressWarnings("unchecked")
					var chunk = (List<T>) (List<?>) cursor.readChunk(totalTime);
					var chunkObserver = rangeReadChunkSizeObserver;
					if (chunkObserver != null) {
						chunkObserver.accept(chunk.size());
					}
					if (chunk.isEmpty() && cursor.isExhausted()) {
						sink.complete();
					} else {
						sink.next(chunk);
					}
					return cursor;
				},
				cursor -> {
					var closeStart = System.nanoTime();
					try {
						cursor.close();
					} finally {
						totalTime.add(System.nanoTime() - closeStart);
						getRangeTimer.record(totalTime.sum(), TimeUnit.NANOSECONDS);
					}
				})
				.subscribeOn(scheduler.read())
				// Flatten before crossing the delivery boundary. With prefetch one, the
				// current decoded page must be exhausted before generate is asked for the
				// next native slice; publishOn then buffers at most one individual row rather
				// than a second decoded page. Slow consumers still never park a RocksDB worker.
				.concatMapIterable(Function.identity(), 1)
				.publishOn(reactor.core.scheduler.Schedulers.parallel(), 1);
	}

	/** Count logical entries in bounded physical slices without materializing one signal per entry. */
	public Mono<Long> countRangeAsyncInternal(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs) {
		LongAdder totalTime = new LongAdder();
		long start = System.nanoTime();
		long deadlineMicros = readDeadlineMicros(timeoutMs);
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

		return Flux.<Long, RangeCursor>generate(
				() -> openRangeCursor(transactionId,
						columnId,
						startKeysInclusive,
						endKeysExclusive,
						reverse,
						deadlineMicros,
						false,
						false,
						totalTime),
				(cursor, sink) -> {
					var chunk = cursor.countChunk(totalTime);
					var chunkObserver = rangeCountChunkObserver;
					if (chunkObserver != null) {
						chunkObserver.run();
					}
					if (chunk.count() != 0L || !chunk.exhausted()) {
						sink.next(chunk.count());
					} else {
						sink.complete();
					}
					return cursor;
				},
				cursor -> {
					var closeStart = System.nanoTime();
					try {
						cursor.close();
					} finally {
						totalTime.add(System.nanoTime() - closeStart);
						reduceRangeTimer.record(totalTime.sum(), TimeUnit.NANOSECONDS);
					}
				})
				.subscribeOn(scheduler.read())
				.publishOn(reactor.core.scheduler.Schedulers.parallel(), 1)
				.reduce(0L, Long::sum);
	}

	private RangeCursor openRangeCursor(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long deadlineMicros,
			boolean fillCache,
			boolean allowIteratorRefresh,
			LongAdder totalTime) {
		ops.beginOp();
		var initializationStart = System.nanoTime();
		try {
			var col = getColumn(columnId);
			Buf calculatedStartKey = startKeysInclusive != null && startKeysInclusive.keys().length > 0
					? col.calculateKey(startKeysInclusive.keys())
					: null;
			Buf calculatedEndKey = endKeysExclusive != null && endKeysExclusive.keys().length > 0
					? col.calculateKey(endKeysExclusive.keys())
					: null;
			var cursor = new RangeCursor(transactionId,
					col,
					calculatedStartKey,
					calculatedEndKey,
					reverse,
					deadlineMicros,
					fillCache,
					allowIteratorRefresh,
					getRangeIteratorRefreshInterval);
			try {
				activeRangeResources.add(cursor);
				return cursor;
			} catch (Throwable error) {
				cursor.close();
				throw error;
			}
		} finally {
			totalTime.add(System.nanoTime() - initializationStart);
			ops.endOp();
		}
	}

	private interface ActiveRangeResource {

		void close();

		void expireIfDeadlinePassed(long nowMicros);
	}

	private record RangeCountChunk(long count, boolean exhausted) {
	}

	private final class RangeCursor implements ActiveRangeResource {

		private final long transactionId;
		private final ColumnInstance col;
		private final boolean reverse;
		private final long deadlineMicros;
		private final boolean fillCache;
		private final boolean allowIteratorRefresh;
		private final long refreshInterval;
		private final AbstractSlice<?> startKeySlice;
		private final AbstractSlice<?> endKeySlice;
		private ReadOptions readOptions;
		private RocksIterator iterator;
		private java.util.ListIterator<Entry<Buf[], Buf>> bucketIterator;
		private long logicalItemsSinceRefresh;
		private boolean exhausted;
		private boolean expired;
		private boolean closed;

		private RangeCursor(long transactionId,
				ColumnInstance col,
				@Nullable Buf startKey,
				@Nullable Buf endKey,
				boolean reverse,
				long deadlineMicros,
				boolean fillCache,
				boolean allowIteratorRefresh,
				long refreshInterval) {
			this.transactionId = transactionId;
			this.col = col;
			this.reverse = reverse;
			this.deadlineMicros = deadlineMicros;
			this.fillCache = fillCache;
			this.allowIteratorRefresh = allowIteratorRefresh;
			this.refreshInterval = refreshInterval;

			AbstractSlice<?> createdStartKeySlice = null;
			AbstractSlice<?> createdEndKeySlice = null;
			ReadOptions createdReadOptions = null;
			RocksIterator createdIterator = null;
			try {
				createdStartKeySlice = startKey != null ? toSlice(startKey) : null;
				createdEndKeySlice = endKey != null ? toSlice(endKey) : null;
				createdReadOptions = createReadOptions(createdStartKeySlice,
						createdEndKeySlice);
				createdIterator = createIterator(createdReadOptions);
				if (reverse) {
					createdIterator.seekToLast();
				} else {
					createdIterator.seekToFirst();
				}
			} catch (Throwable error) {
				if (createdIterator != null) {
					createdIterator.close();
				}
				if (createdReadOptions != null) {
					createdReadOptions.close();
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
			this.readOptions = createdReadOptions;
			this.iterator = createdIterator;
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

		private List<KV> readChunk(LongAdder totalTime) {
			ops.beginOp();
			var sliceStart = System.nanoTime();
			try {
				synchronized (this) {
					if (expired) {
						throw rangeDeadlineExceeded();
					}
					if (closed || exhausted) {
						return List.of();
					}
					var results = new ArrayList<KV>(RANGE_READ_CHUNK_SIZE);
					int physicalKeysExamined = 0;
					long decodedBytes = 0L;
					while (results.size() < RANGE_READ_CHUNK_SIZE
							&& (results.isEmpty() || decodedBytes < RANGE_READ_MAX_DECODED_BYTES_PER_CHUNK)
							&& !exhausted) {
						if (bucketIterator != null) {
							if (reverse ? bucketIterator.hasPrevious() : bucketIterator.hasNext()) {
								var entry = reverse ? bucketIterator.previous() : bucketIterator.next();
								var kv = decodeBucketEntry(col, toBuf(iterator.key()), entry);
								var decoded = Objects.requireNonNull(kv);
								results.add(decoded);
								decodedBytes = saturatingAdd(decodedBytes, decodedKVBytes(decoded));
								boolean bucketHasMore = reverse
										? bucketIterator.hasPrevious()
										: bucketIterator.hasNext();
								if (!bucketHasMore) {
									bucketIterator = null;
									advanceIterator();
								}
								recordLogicalItemsAndMaybeRefresh(1L);
								continue;
							}
							bucketIterator = null;
							advanceIterator();
							recordLogicalItemsAndMaybeRefresh(0L);
							continue;
						}

						if (!iterator.isValid()) {
							checkIteratorStatusIfInvalid(iterator);
							exhausted = true;
							break;
						}
						if (physicalKeysExamined >= RANGE_READ_MAX_PHYSICAL_KEYS_PER_CHUNK) {
							break;
						}
						physicalKeysExamined++;

						if (col.hasBuckets()) {
							var elements = new Bucket(col, toBuf(iterator.value())).getElements();
							bucketIterator = elements.listIterator(reverse ? elements.size() : 0);
							if (elements.isEmpty()) {
								bucketIterator = null;
								advanceIterator();
								recordLogicalItemsAndMaybeRefresh(0L);
							}
						} else {
							var calculatedKey = toBuf(iterator.key());
							var calculatedValue = col.schema().hasValue() ? toBuf(iterator.value()) : emptyBuf();
							var decoded = Objects.requireNonNull(decodeKV(col, calculatedKey, calculatedValue));
							results.add(decoded);
							decodedBytes = saturatingAdd(decodedBytes, decodedKVBytes(decoded));
							advanceIterator();
							recordLogicalItemsAndMaybeRefresh(1L);
						}
					}
					return results;
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
					long count = 0L;
					int physicalKeysExamined = 0;
					while (physicalKeysExamined < RANGE_READ_MAX_PHYSICAL_KEYS_PER_CHUNK) {
						if (!iterator.isValid()) {
							checkIteratorStatusIfInvalid(iterator);
							exhausted = true;
							break;
						}
						long logicalItems = col.hasBuckets()
								? Bucket.readElementCount(toBuf(iterator.value()))
								: 1L;
						count += logicalItems;
						physicalKeysExamined++;
						advanceIterator();
						recordLogicalItemsAndMaybeRefresh(logicalItems);
					}
					return new RangeCountChunk(count, exhausted);
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

		private void recordLogicalItemsAndMaybeRefresh(long items) {
			if (items > Long.MAX_VALUE - logicalItemsSinceRefresh) {
				logicalItemsSinceRefresh = Long.MAX_VALUE;
			} else {
				logicalItemsSinceRefresh += items;
			}
			if (allowIteratorRefresh
					&& transactionId == 0L
					&& bucketIterator == null
					&& logicalItemsSinceRefresh >= refreshInterval
					&& iterator.isValid()) {
				refreshIterator();
				logicalItemsSinceRefresh = 0L;
			}
		}

		private void refreshIterator() {
			var iteratorKey = iterator.key();
			var currentKey = Arrays.copyOf(iteratorKey, iteratorKey.length);
			iterator.close();
			readOptions.close();
			iterator = null;
			readOptions = null;

			ReadOptions replacementOptions = null;
			RocksIterator replacementIterator = null;
			try {
				replacementOptions = createReadOptions(startKeySlice, endKeySlice);
				replacementIterator = createIterator(replacementOptions);
				if (reverse) {
					replacementIterator.seekForPrev(currentKey);
				} else {
					replacementIterator.seek(currentKey);
				}
				readOptions = replacementOptions;
				iterator = replacementIterator;
			} catch (Throwable error) {
				if (replacementIterator != null) {
					replacementIterator.close();
				}
				if (replacementOptions != null) {
					replacementOptions.close();
				}
				throw error;
			}
		}

		private synchronized boolean isExhausted() {
			if (expired) {
				throw rangeDeadlineExceeded();
			}
			return exhausted || closed;
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
						if (iterator != null) {
							iterator.close();
							iterator = null;
						}
					} finally {
						try {
							if (readOptions != null) {
								readOptions.close();
								readOptions = null;
							}
						} finally {
							try {
								if (endKeySlice != null) {
									endKeySlice.close();
								}
							} finally {
								if (startKeySlice != null) {
									startKeySlice.close();
								}
							}
						}
					}
				}
			} finally {
				activeRangeResources.remove(this);
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

	private final class ScanState implements AutoCloseable {
		private final ColumnInstance col;
		private final LiveFileMetaData file;
		private final SstFileReader reader;
		private final SstFileReaderIterator it;
		private final ReadOptions readOptions;
		private final Options options;
		private boolean closed = false;
		private final Buf outBuf = Buf.create(Math.toIntExact(SizeUnit.MB));

		private ScanState(ColumnInstance col, String cfName, LiveFileMetaData file) throws org.rocksdb.RocksDBException {
			this.col = col;
			this.file = file;
			ColumnFamilyOptions cfOpts = columnsConifg.get(cfName);
			this.options = cfOpts != null ? new Options(dbOptions, cfOpts) : new Options();
			this.options.setAllowMmapReads(true);
			this.options.setUseDirectReads(false);
			this.options.setUseDirectIoForFlushAndCompaction(false);
			this.options.setParanoidChecks(false);

			this.reader = new SstFileReader(options);
			String filePath = file.path();
			if (!filePath.endsWith(".sst")) {
				filePath = filePath + file.fileName();
			}

			ReadOptions tmpReadOptions = null;
			SstFileReaderIterator tmpIt = null;
			try {
				this.reader.open(filePath);
				tmpReadOptions = new ReadOptions()
						.setFillCache(false)
						.setIgnoreRangeDeletions(true)
						.setVerifyChecksums(true)
						.setReadaheadSize(8 * SizeUnit.MB);

				tmpIt = reader.newIterator(tmpReadOptions);
				tmpIt.seekToFirst();
			} catch (org.rocksdb.RocksDBException e) {
				if (e.getMessage() != null && e.getMessage().contains("No such file or directory")) {
					logger.debug("SST file missing during scan (ignoring): " + file.fileName());
				} else {
					logger.warn("Failed to open SST file: " + file.fileName(), e);
					this.reader.close();
					this.options.close();
					throw e;
				}
			}
			this.readOptions = tmpReadOptions;
			this.it = tmpIt;
		}

		private synchronized void generateNext(SynchronousSink<SerializedKVBatch> sink) {
			if (closed) {
				sink.complete();
				return;
			}
			if (it == null) {
				sink.complete();
				return;
			}
			if (!it.isValid()) {
				try {
					it.status();
				} catch (org.rocksdb.RocksDBException e) {
					sink.error(e);
					return;
				}
				sink.complete();
				return;
			}

			int batchSize = 0;
			int currentBatchBytes = 0;
			int currentSerializedBatchBytes = Integer.BYTES;

			try {
				while (it.isValid()) {
					if (closed) return;
					batchSize++;
					byte[] k = it.key();
					byte[] v = it.value();
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

					it.next();
					if (batchSize >= RAW_SCAN_MAX_ENTRIES_PER_CHUNK
							|| currentBatchBytes >= RAW_SCAN_MAX_BYTES_PER_CHUNK) {
						break;
					}
				}
			} catch (Exception e) {
				sink.error(e);
				return;
			}

			if (batchSize > 0) {
				outBuf.setIntLE(0, batchSize);
				sink.next(new SerializedKVBatch.SerializedKVBatchRef(outBuf.copyOfRange(0, currentSerializedBatchBytes)));
			} else {
				try {
					it.status();
				} catch (org.rocksdb.RocksDBException e) {
					sink.error(e);
					return;
				}
				sink.complete();
			}
		}

		@Override
		public synchronized void close() {
			if (!closed) {
				closed = true;
				if (it != null) it.close();
				if (readOptions != null) readOptions.close();
				reader.close();
				options.close();
			}
		}
	}

	public Flux<SerializedKVBatch> scanRawAsyncInternal(long columnId, int shardIndex, int shardCount) {
		return Flux.defer(() -> {
			ColumnInstance col = getColumn(columnId);
			String cfName;
			try {
				cfName = new String(col.cfh().getName(), StandardCharsets.UTF_8);
			} catch (RocksDBException | org.rocksdb.RocksDBException e) {
				return Flux.error(e);
			}

			List<LiveFileMetaData> files = getDb().get().getLiveFilesMetaData();

			Function<LiveFileMetaData, Publisher<SerializedKVBatch>> mapper = f ->
				Flux.<SerializedKVBatch, ScanState>generate(
						() -> new ScanState(col, cfName, f),
						(state, sink) -> {
							state.generateNext(sink);
							return state;
						},
						ScanState::close)
						.subscribeOn(scheduler.read())
						.publishOn(reactor.core.scheduler.Schedulers.parallel(), 1);

			var ssts = Flux.fromIterable(files)
					.filter(f -> new String(f.columnFamilyName(), StandardCharsets.UTF_8).equals(cfName))
					.filter(f -> f.fileName().endsWith(".sst"));


			Flux<SerializedKVBatch> exec;
			if (shardCount == 1) {
				exec = ssts.flatMap(mapper, 4, 1);
			} else {
				exec = ssts
						.filter(m -> Math.floorMod(m.fileName().hashCode(), shardCount)
								== Math.floorMod(shardIndex, shardCount))
						.concatMap(mapper, 2);
			}
			return exec;
		})
				.doFirst(ops::beginOp)
				.doFinally(s -> ops.endOp())
				.subscribeOn(scheduler.read());
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
			db.get().flushWal(true);
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
			var db = this.db.get();
			for (ColumnInstance value : columns.values()) {
				if (value.cfh().isOwningHandle()) {
					try (var cro = new CompactRangeOptions()
							.setAllowWriteStall(false)
							.setExclusiveManualCompaction(true)
							.setChangeLevel(false)
							.setMaxSubcompactions(16)
							.setBottommostLevelCompaction(BottommostLevelCompaction.kForceOptimized)) {
						var cfhOpts = db.getOptions(value.cfh());
						var autoCompactionsEnabled = !cfhOpts.disableAutoCompactions();
						if (autoCompactionsEnabled) {
							cfhOpts.setDisableAutoCompactions(true);
						}
						db.compactRange(value.cfh(), null, null, cro);
						if (autoCompactionsEnabled) {
							cfhOpts.setDisableAutoCompactions(false);
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
		var db = this.db.get();
		try {
			var reader = Objects.requireNonNull(fastGetReader);
			byte[] value = useDefaultReadOptions
					? reader.get(cfh,
							calculatedKey.getBackingByteArray(),
							calculatedKey.getBackingByteArrayOffset(),
							calculatedKey.getBackingByteArrayLength())
					: reader.get(cfh,
							Objects.requireNonNull(readOptions),
							calculatedKey.getBackingByteArray(),
							calculatedKey.getBackingByteArrayOffset(),
							calculatedKey.getBackingByteArrayLength());
			return toBuf(value);
		} catch (FFMRocksDBGet.NativeError nativeError) {
			if (!nativeError.isRocksDBStatus()) {
				var exception = new org.rocksdb.RocksDBException("FFM fast-get failed: " + nativeError.getMessage());
				exception.initCause(nativeError);
				throw exception;
			}
			// The C API exposes errors as strings. Retry only an actual RocksDB status through JNI so callers retain the
			// exact typed Status (timeout, corruption, I/O error, etc.); infrastructure failures above never downgrade.
			fastGetNativeErrorFallbacks.increment();
			if (readOptions != null) {
				return toBuf(db.get(cfh,
						readOptions,
						calculatedKey.getBackingByteArray(),
						calculatedKey.getBackingByteArrayOffset(),
						calculatedKey.getBackingByteArrayLength()
				));
			}
			try (var fallbackReadOptions = newReadOptions("ffm-fast-get-error-fallback")) {
				return toBuf(db.get(cfh,
						fallbackReadOptions,
						calculatedKey.getBackingByteArray(),
						calculatedKey.getBackingByteArrayOffset(),
						calculatedKey.getBackingByteArrayLength()
				));
			}
		}
	}

	private ColumnInstance getColumn(long columnId) {
		var col = columns.get(columnId);
		if (col != null) {
			return col;
		} else {
			throw RocksDBException.of(RocksDBErrorType.COLUMN_NOT_FOUND, "No column with id " + columnId);
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

	public @Nullable Path getPath() {
		return path;
	}

	@VisibleForTesting
	public TransactionalDB getDb() {
		return db;
	}

	@VisibleForTesting
	public long getPendingOpsCount() {
		return ops.getPendingOpsCount();
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
		return activeRangeResources.size();
	}

	@VisibleForTesting
	public boolean isExpiredRangeCleanupScheduledForTesting() {
		return !expiredRangeCleanupTask.isCancelled() && !expiredRangeCleanupTask.isDone();
	}

	@VisibleForTesting
	public long getFastGetNativeCallsCount() {
		return fastGetReader != null ? fastGetReader.getCallsCount() : 0;
	}

	@VisibleForTesting
	public long getFastGetNativeErrorFallbacksCount() {
		return fastGetNativeErrorFallbacks.sum();
	}

	@VisibleForTesting
	public int getFastGetRetainedStateCount() {
		return fastGetReader != null ? fastGetReader.getRetainedStateCount() : 0;
	}

	@VisibleForTesting
	public int getFastGetRetainedStateCapacity() {
		return fastGetReader != null ? fastGetReader.getRetainedStateCapacity() : 0;
	}

	@VisibleForTesting
	public boolean isFastGetReaderClosed() {
		return fastGetReader == null || fastGetReader.isClosed();
	}

	@VisibleForTesting
	public DatabaseConfig getConfig() {
		return config;
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

	private static final int CDC_OP_INDEX_BITS = 20; // up to ~1M ops per batch
	private static final long CDC_OP_INDEX_MASK = (1L << CDC_OP_INDEX_BITS) - 1L;
	private static final long CDC_DEFAULT_MAX_EVENTS = 10_000L;
	private static final int CDC_PREFIXLESS_PROBE_MAX_ATTEMPTS = 3;

	private static long composeCdcSeq(long walSeq, int opIndex) {
		return (walSeq << CDC_OP_INDEX_BITS) | (opIndex & CDC_OP_INDEX_MASK);
	}

	private static long extractWalSeq(long cdcSeq) {
		return cdcSeq >>> CDC_OP_INDEX_BITS;
	}

	private static int extractOpIndex(long cdcSeq) {
		return (int) (cdcSeq & CDC_OP_INDEX_MASK);
	}

	private record CdcSubscriptionMeta(long lastCommittedSeq, @Nullable long[] columnFilter, boolean emitLatestValues) {}
	private record CdcPollWindow(long startSeq,
			long maxWalSequenceInclusive,
			CdcSubscriptionMeta subscription) {}
	private record CdcPollPage(CdcBatch batch, long emittedEvents, long emittedBytes) {}
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
			dos.writeByte(2); // version
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

	private long findEarliestAvailableWalSeq() {
		for (int attempt = 0; attempt < CDC_PREFIXLESS_PROBE_MAX_ATTEMPTS; attempt++) {
			try {
				var result = findEarliestAvailableWalSeqAttempt();
				if (result.isPresent()) {
					return result.getAsLong();
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

	private OptionalLong findEarliestAvailableWalSeqAttempt() throws org.rocksdb.RocksDBException {
		long latestBeforeFlush = getLatestCdcWalSequence();
		// getUpdatesSince reads WAL files, so publish the application WAL buffer without
		// forcing an fsync. Explicit flush() remains the durability boundary.
		flushCdcWalForPrefixlessProbe();

		var earliest = probeEarliestAvailableWalSeq();
		if (earliest.isPresent()) {
			return earliest;
		}

		long latestAfterProbe = getLatestCdcWalSequence();
		if (latestBeforeFlush == latestAfterProbe) {
			return OptionalLong.of(latestAfterProbe + 1); // sequence-stable empty DB
		}
		return OptionalLong.empty();
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
	public long cdcCreate(@NotNull String id, @Nullable Long fromSeq, @Nullable List<Long> columnIds)
			throws RocksDBException {
		return cdcCreate(id, fromSeq, columnIds, null);
	}

	public long cdcCreate(@NotNull String id,
			@Nullable Long fromSeq,
			@Nullable List<Long> columnIds,
			@Nullable Boolean emitLatestValues) throws RocksDBException {
		Objects.requireNonNull(id, "id");
		ops.beginOp();
		try {
			var existing = loadCdcMeta(id);
			long startSeq;
			if (fromSeq != null) {
				startSeq = fromSeq == 0L ? composeCdcSeq(findEarliestAvailableWalSeq(), 0) : fromSeq;
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
			saveCdcMeta(id, new CdcSubscriptionMeta(lastCommittedToPersist, filter, resolved));
			return startSeq;
		} catch (org.rocksdb.RocksDBException e) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
		} finally {
			ops.endOp();
		}
	}

	@Override
	public void cdcDelete(@NotNull String id) throws RocksDBException {
		Objects.requireNonNull(id, "id");
		ops.beginOp();
		try {
			db.get().delete(cdcMetaColumnDescriptorHandle, cdcKeyOf(id));
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
			var existing = loadCdcMeta(id);
			if (existing == null) {
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "CDC subscription not found: " + id);
			}
			long newCommitted = Math.max(existing.lastCommittedSeq, seq);
			saveCdcMeta(id, new CdcSubscriptionMeta(newCommitted, existing.columnFilter, existing.emitLatestValues));
		} catch (org.rocksdb.RocksDBException e) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
		} finally {
			ops.endOp();
		}
	}

	private static final long CDC_HARD_MAX_EVENTS = 10_000;
	private static final long CDC_MAX_SCANNED_MUTATIONS_PER_POLL = 4_096;
	private static final long CDC_MAX_BYTES_PER_POLL = 16 * 1024 * 1024; // 16MB
	private static final String CDC_TAIL_REFRESH_STATUS = "Create a new iterator to fetch the new tail.";
	private static final String CDC_GAP_STATUS = "Gap in sequence numbers";
	private static final String CDC_REQUIRED_SEQUENCE_GAP_STATUS =
			"Gap in sequence number. Could not seek to required sequence number";
	private static final String CDC_START_SEQUENCE_MISSING_STATUS =
			"Start sequence was not found, skipping to the next available";

	private long captureCdcPollTail() {
		long tail = db.get().getLatestSequenceNumber();
		var observer = cdcPollTailCapturedObserver;
		if (observer != null) {
			observer.run();
		}
		return tail;
	}

	@VisibleForTesting
	protected void iterateCdcWriteBatch(WriteBatch writeBatch, WriteBatch.Handler handler)
			throws org.rocksdb.RocksDBException {
		writeBatch.iterate(handler);
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

		// These are the exact continuity-loss states emitted by RocksDB 10.10.1's
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
		private int skipFirstOps;
		private int seenOps = 0;
		private int produced = 0;
		private long accumulatedBytes = 0;
		private long maxToProduce;
		private long maxBytes;
		private int limitReachedAtOpIndex = -1;
		private boolean allowOversizedFirstEvent;

		int getLimitReachedAtOpIndex() {
			return limitReachedAtOpIndex;
		}

		long getAccumulatedBytes() {
			return accumulatedBytes;
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
				int skipFirstOps,
				long maxToProduce,
				long maxBytes,
				boolean allowOversizedFirstEvent) {
			this.walSeq = walSeq;
			this.skipFirstOps = skipFirstOps;
			this.maxToProduce = maxToProduce;
			this.maxBytes = maxBytes;
			this.allowOversizedFirstEvent = allowOversizedFirstEvent;
			this.seenOps = 0;
			this.produced = 0;
			this.accumulatedBytes = 0;
			this.limitReachedAtOpIndex = -1;
		}

		@Override
		public boolean shouldContinue() {
			return limitReachedAtOpIndex == -1;
		}

		private void trackAndMaybeEmitByCfId(int columnFamilyId, byte[] key, @Nullable byte[] value, CDCEvent.Op op) {
			seenOps++;

			if (limitReachedAtOpIndex != -1) {
				return;
			}
			if (produced >= maxToProduce) {
				limitReachedAtOpIndex = seenOps - 1;
				return;
			}

			if (seenOps <= skipFirstOps) {
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
			if ((!allowOversizedFirstEvent || produced > 0) && accumulatedBytes + eventSize > maxBytes) {
				limitReachedAtOpIndex = seenOps - 1; // Stop BEFORE processing this op
				return;
			}

			int opIndex = seenOps - 1; // zero-based
			long seq = composeCdcSeq(walSeq, opIndex);
			
			out.add(new CDCEvent(seq, colId, Buf.wrap(finalKey), value != null ? Buf.wrap(value) : emptyBuf(), op));
			produced++;
			accumulatedBytes += eventSize;
			EmbeddedDB.this.cdcEventsEmitted.increment();
			EmbeddedDB.this.cdcBytesEmitted.increment(finalKey.length + (value != null ? value.length : 0));

			if (produced >= maxToProduce) {
				limitReachedAtOpIndex = seenOps;
			}
		}

		private void trackUnemittedMutation() {
			seenOps++;
			if (limitReachedAtOpIndex == -1 && produced >= maxToProduce) {
				limitReachedAtOpIndex = seenOps - 1;
			}
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
			trackUnemittedMutation();
		}

		// Default CF methods - skip data but MUST count ops
		@Override
		public void put(byte[] key, byte[] value) {
			trackUnemittedMutation();
		}

		@Override
		public void merge(byte[] key, byte[] value) {
			trackUnemittedMutation();
		}

		@Override
		public void delete(byte[] key) {
			trackUnemittedMutation();
		}

		@Override
		public void singleDelete(byte[] key) {
			trackUnemittedMutation();
		}

		@Override
		public void deleteRange(byte[] beginKey, byte[] endKey) {
			trackUnemittedMutation();
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

	private CdcPollPage cdcPollPage(String id,
			long fromSeq,
			long maxEvents,
			long maxBytes,
			boolean allowOversizedFirstEvent,
			long maxScannedMutations,
			long maxWalSequenceInclusive) {
		try {
			var meta = loadCdcMeta(id);
			if (meta == null) {
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "CDC subscription not found: " + id);
			}
			return cdcPollPage(meta,
					fromSeq,
					maxEvents,
					maxBytes,
					allowOversizedFirstEvent,
					maxScannedMutations,
					maxWalSequenceInclusive);
		} catch (org.rocksdb.RocksDBException e) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
		}
	}

	private CdcPollPage cdcPollPage(CdcSubscriptionMeta subscription,
			long fromSeq,
			long maxEvents,
			long maxBytes,
			boolean allowOversizedFirstEvent,
			long maxScannedMutations,
			long maxWalSequenceInclusive) {
		try (var cursor = openCdcPollCursor(subscription, fromSeq, maxWalSequenceInclusive)) {
			return cursor.readPage(maxEvents,
					maxBytes,
					allowOversizedFirstEvent,
					maxScannedMutations);
		} catch (org.rocksdb.RocksDBException e) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
		}
	}

	private List<CDCEvent> resolveLatestCdcValues(CdcSubscriptionMeta meta, List<CDCEvent> result) {
		if (!meta.emitLatestValues || result.isEmpty()) {
			return result;
		}

		var transformed = new ArrayList<CDCEvent>(result.size());
		var keysToResolve = new ArrayList<byte[]>();
		var cfHandles = new ArrayList<ColumnFamilyHandle>();
		var indicesToResolve = new IntArrayList();

		for (int i = 0; i < result.size(); i++) {
			var event = result.get(i);
			var column = columns.get(event.columnId());
			if (column != null) {
				// Resolve every operation to the latest value to ensure monotonicity and
				// avoid "time travel" corruption.
				keysToResolve.add(event.key().asArray());
				cfHandles.add(column.cfh());
				indicesToResolve.add(i);
			}
		}

		if (keysToResolve.isEmpty()) {
			transformed.addAll(result);
			return transformed;
		}

		final List<byte[]> resolvedValues;
		try {
			resolvedValues = db.get().multiGetAsList(cfHandles, keysToResolve);
		} catch (org.rocksdb.RocksDBException error) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, error);
		}

		int resolutionIndex = 0;
		for (int i = 0; i < result.size(); i++) {
			var event = result.get(i);
			var column = columns.get(event.columnId());
			Buf finalKey = event.key();
			if (resolutionIndex < indicesToResolve.size() && indicesToResolve.getInt(resolutionIndex) == i) {
				byte[] valueBytes = resolvedValues.get(resolutionIndex++);
				if (valueBytes != null) {
					if (column != null && column.hasBuckets()) {
						var elements = new Bucket(column, Buf.wrap(valueBytes)).getElements();
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
							Buf realKey = Buf.createZeroes((int) totalSize);
							realKey.setBytesFromBuf(0, fixedPart, 0, fixedBytes);
							int offset = fixedBytes;
							for (Buf variableKey : variableKeys) {
								int length = variableKey.size();
								realKey.setBytesFromBuf(offset, variableKey, 0, length);
								offset += length;
							}
							transformed.add(new CDCEvent(event.seq(),
									event.columnId(),
									realKey,
									element.getValue(),
									CDCEvent.Op.PUT));
						}
					} else {
						transformed.add(new CDCEvent(event.seq(),
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
					transformed.add(new CDCEvent(event.seq(),
							event.columnId(),
							deleteKey,
							emptyBuf(),
							CDCEvent.Op.DELETE));
				}
			} else {
				transformed.add(new CDCEvent(event.seq(),
						event.columnId(),
						finalKey,
						event.value(),
						event.op()));
			}
		}
		return transformed;
	}

	private CdcPollCursor openCdcPollCursor(CdcSubscriptionMeta subscription,
			long startSeq,
			long maxWalSequenceInclusive) throws org.rocksdb.RocksDBException {
		var cursor = new CdcPollCursor(subscription, startSeq, maxWalSequenceInclusive);
		try {
			activeCdcPollCursors.add(cursor);
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
		private final long initialWalSequence;
		private final int initialOperationIndex;
		private final long maxWalSequenceInclusive;
		private TransactionLogIterator iterator;
		private long nextSeq;
		private boolean firstBatch = true;
		private boolean exhausted;
		private boolean closed;

		private CdcPollCursor(CdcSubscriptionMeta subscription,
				long startSeq,
				long maxWalSequenceInclusive) throws org.rocksdb.RocksDBException {
			this.subscription = subscription;
			this.filter = subscription.columnFilter == null
					? null
					: new it.unimi.dsi.fastutil.longs.LongOpenHashSet(subscription.columnFilter);
			this.initialWalSequence = extractWalSeq(startSeq);
			this.initialOperationIndex = extractOpIndex(startSeq);
			this.maxWalSequenceInclusive = maxWalSequenceInclusive;
			this.nextSeq = startSeq;

			if (initialWalSequence > maxWalSequenceInclusive) {
				exhausted = true;
				return;
			}

			var iteratorObserver = cdcWalIteratorOpenObserver;
			if (iteratorObserver != null) {
				iteratorObserver.run();
			}
			try {
				iterator = db.get().getUpdatesSince(initialWalSequence);
			} catch (org.rocksdb.RocksDBException error) {
				if (error.getMessage() != null
						&& error.getMessage().contains("Requested sequence not yet written")) {
					exhausted = true;
					return;
				}
				throw error;
			}
		}

		private synchronized CdcPollPage readPage(long maxEvents,
				long maxBytes,
				boolean allowOversizedFirstEvent,
				long maxScannedMutations) {
			if (closed) {
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

			try (var handler = new EventCollector(result, filter, subscription.emitLatestValues)) {
				while (result.size() < effectiveMax
						&& scannedMutations < maxScannedMutations
						&& iterator.isValid()) {
					var batchResult = iterator.getBatch();
					try (var writeBatch = batchResult.writeBatch()) {
						long batchWalSequence = batchResult.sequenceNumber();
						if (firstBatch) {
							if (batchWalSequence > initialWalSequence) {
								throw new CdcGapDetectedException("Gap detected in WAL. Requested WAL seq: "
										+ initialWalSequence + ", but earliest available is: " + batchWalSequence);
							}
							firstBatch = false;
						}
						if (batchWalSequence > maxWalSequenceInclusive) {
							exhausted = true;
							break;
						}

						long sequenceDifference = initialWalSequence - batchWalSequence;
						int skipOps = sequenceDifference >= 0
								? (int) sequenceDifference + initialOperationIndex
								: 0;
						handler.reset(batchWalSequence,
								skipOps,
								effectiveMax - result.size(),
								maxBytes - accumulatedBytes,
								allowOversizedFirstEvent && result.isEmpty());
						try {
							iterateCdcWriteBatch(writeBatch, handler);
						} catch (Exception error) {
							throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR,
									"Failed to parse WriteBatch at seq " + batchWalSequence,
									error);
						}

						accumulatedBytes += handler.getAccumulatedBytes();
						scannedMutations += Math.max(0, handler.getSeenOps() - skipOps);
						int stopIndex = handler.getLimitReachedAtOpIndex();
						int batchMutationCount = writeBatch.count();
						boolean fullyConsumed = stopIndex == -1 || stopIndex >= batchMutationCount;
						if (!fullyConsumed) {
							nextSeq = composeCdcSeq(batchWalSequence, stopIndex);
							// A TransactionLogIterator cannot resume in the middle of its current
							// WriteBatch. This only happens at the logical event/byte boundary, so
							// the caller will terminate the poll and return this continuation.
							exhausted = true;
							break;
						}

						nextSeq = composeCdcSeq(batchWalSequence + batchMutationCount, 0);
						// Move past the consumed batch before yielding a fairness slice. The
						// next scheduled task can then continue on this same native iterator.
						iterator.next();
						if (result.size() >= effectiveMax
								|| (accumulatedBytes >= maxBytes && !result.isEmpty())
								|| scannedMutations >= maxScannedMutations) {
							break;
						}
					}
				}

				if (!iterator.isValid()) {
					try {
						checkCdcIteratorStatus(iterator);
					} catch (org.rocksdb.RocksDBException error) {
						throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, error);
					}
					exhausted = true;
				}
			} catch (RocksDBException error) {
				throw error;
			} catch (Exception error) {
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, error);
			}

			long emittedEvents = result.size();
			result = resolveLatestCdcValues(subscription, result);
			return new CdcPollPage(new CdcBatch(result, nextSeq), emittedEvents, accumulatedBytes);
		}

		private CdcPollPage emptyPage() {
			return new CdcPollPage(new CdcBatch(Collections.emptyList(), nextSeq), 0L, 0L);
		}

		private synchronized boolean isExhausted() {
			return exhausted || closed;
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
					if (iterator != null) {
						iterator.close();
						iterator = null;
					}
				}
			} finally {
				activeCdcPollCursors.remove(this);
			}
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
			try {
				var meta = loadCdcMeta(id);
				if (meta == null) {
					throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR,
							"CDC subscription not found: " + id);
				}
				if (fromSeq != null && fromSeq == 0L) {
					startSeq = composeCdcSeq(findEarliestAvailableWalSeq(), 0);
				} else {
					// Publish the application WAL buffer once before fixing this logical
					// poll's tail. Continuation pages must neither flush nor chase appends.
					db.get().flushWal(false);
					startSeq = fromSeq != null ? fromSeq : meta.lastCommittedSeq + 1;
				}
				maxWalSequenceInclusive = captureCdcPollTail();
			} catch (org.rocksdb.RocksDBException e) {
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
			}
			var events = new ArrayList<CDCEvent>((int) Math.min(1_024L, requestedEvents));
			long pageStart = startSeq;
			long remainingEvents = requestedEvents;
			long remainingBytes = CDC_MAX_BYTES_PER_POLL;
			boolean allowOversizedFirstEvent = true;
			while (remainingEvents > 0L && (allowOversizedFirstEvent || remainingBytes > 0L)) {
				var page = cdcPollPage(id,
						pageStart,
						remainingEvents,
						remainingBytes,
						allowOversizedFirstEvent,
						Long.MAX_VALUE,
						maxWalSequenceInclusive);
				var batch = page.batch();
				long pageBytes = page.emittedBytes();
				events.addAll(batch.events());
				remainingEvents = Math.max(0L, remainingEvents - page.emittedEvents());
				remainingBytes = pageBytes >= remainingBytes ? 0L : remainingBytes - pageBytes;
				allowOversizedFirstEvent &= page.emittedEvents() == 0L;
				long nextSeq = batch.nextSeq();
				if (extractWalSeq(nextSeq) > maxWalSequenceInclusive || nextSeq == pageStart) {
					break;
				}
				pageStart = nextSeq;
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
				.flatMapMany(window -> Flux.usingWhen(
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
												|| extractWalSeq(nextSeq) > window.maxWalSequenceInclusive()
												|| nextSeq == page.startSeq()) {
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
									}, 1)
									.concatMapIterable(page -> page.page().batch().events(), 1);
						},
						cursorStart -> closeCdcPollCursorAsync(cursorStart.cursor()),
						(cursorStart, _) -> closeCdcPollCursorAsync(cursorStart.cursor()),
						cursorStart -> closeCdcPollCursorAsync(cursorStart.cursor())));
	}

	public @NotNull Mono<CdcBatch> cdcPollBatchAsyncInternal(@NotNull String id, @Nullable Long fromSeq, long maxEvents)
			throws RocksDBException {
		long requestedEvents = normalizeCdcMaxEvents(maxEvents);
		return prepareCdcPollStartAsync(id, fromSeq)
				.flatMap(window -> cdcPollPreparedAsync(window.subscription(),
						window.startSeq(),
						requestedEvents,
						window.maxWalSequenceInclusive()))
				// Mapping and emitting a materialized CDC page must not retain a scarce
				// RocksDB read worker after the bounded poll step has completed.
				.publishOn(reactor.core.scheduler.Schedulers.parallel());
	}

	private static long normalizeCdcMaxEvents(long maxEvents) {
		long requested = maxEvents > 0 ? maxEvents : CDC_DEFAULT_MAX_EVENTS;
		return Math.min(requested, CDC_HARD_MAX_EVENTS);
	}

	private Mono<CdcPollWindow> prepareCdcPollStartAsync(@NotNull String id, @Nullable Long fromSeq) {
		return scheduleTracked(this.scheduler.cdc(), () -> {
			var meta = loadCdcMeta(id);
			if (meta == null) {
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR,
						"CDC subscription not found: " + id);
			}
			long startSeq;
			if (fromSeq != null && fromSeq == 0L) {
				// Prefix-less WAL discovery includes its own bounded flush/probe loop and
				// therefore belongs to the CDC lane, never a read/write worker or the
				// independently serialized flush/compact lane.
				startSeq = composeCdcSeq(findEarliestAvailableWalSeq(), 0);
			} else {
				// Publish the application WAL buffer without forcing an fsync. Explicit
				// flush() remains the durability boundary.
				db.get().flushWal(false);
				startSeq = fromSeq != null ? fromSeq : meta.lastCommittedSeq + 1;
			}
			return new CdcPollWindow(startSeq, captureCdcPollTail(), meta);
		});
	}

	private Mono<CdcBatch> cdcPollPreparedAsync(CdcSubscriptionMeta subscription,
			long startSeq,
			long maxEvents,
			long maxWalSequenceInclusive) {
		return scheduleTracked(this.scheduler.read(),
				() -> cdcPollPage(subscription,
						startSeq,
						maxEvents,
						CDC_MAX_BYTES_PER_POLL,
						true,
						CDC_MAX_SCANNED_MUTATIONS_PER_POLL,
						maxWalSequenceInclusive).batch());
	}

	private Mono<CdcPollCursorStart> openAndReadCdcPollCursorAsync(CdcPollWindow window,
			long maxEvents,
			long maxBytes,
			boolean allowOversizedFirstEvent) {
		return scheduleTracked(this.scheduler.read(), () -> {
			var cursor = openCdcPollCursor(window.subscription(),
					window.startSeq(),
					window.maxWalSequenceInclusive());
			try {
				return new CdcPollCursorStart(cursor,
						cursor.readPage(maxEvents,
								maxBytes,
								allowOversizedFirstEvent,
								CDC_MAX_SCANNED_MUTATIONS_PER_POLL));
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
		return scheduleTracked(this.scheduler.read(),
				() -> cursor.readPage(maxEvents,
						maxBytes,
						allowOversizedFirstEvent,
						CDC_MAX_SCANNED_MUTATIONS_PER_POLL));
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
