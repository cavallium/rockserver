package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.common.Utils.dummyRocksDBEmptyValue;
import static it.cavallium.rockserver.core.common.Utils.emptyBuf;
import static it.cavallium.rockserver.core.common.Utils.toBuf;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kExistsWithValue;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kExistsWithoutValue;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.StampedLock;
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
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
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
import org.rocksdb.Holder;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.SstFileReaderIterator;
import org.rocksdb.SstFileReader;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.Options;
import org.rocksdb.Status.Code;
import org.rocksdb.TableProperties;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import org.rocksdb.TransactionLogIterator;
import org.rocksdb.WriteBatch;
import reactor.core.publisher.SynchronousSink;

public class EmbeddedDB implements RocksDBSyncAPI, InternalConnection, Closeable {

	private static final int INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES = 4096;
	private static final long ITERATOR_REFRESH_INTERVAL = 1_000_000;
	public static final long MAX_TRANSACTION_DURATION_MS = 10_000L;
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
	private final Timer putTimer;
	private final Timer putMultiTimer;
	private final Timer putBatchTimer;
	private final Timer getTimer;
	private final Timer openIteratorTimer;
	private final Timer closeIteratorTimer;
	private final Timer seekToTimer;
	private final Timer subsequentTimer;
	private final Timer reduceRangeTimer;
	private final Timer getRangeTimer;
	private final Timer flushTimer;
	private final Timer compactTimer;
	private final Timer getAllColumnDefinitionsTimer;
	private final Counter cdcEventsEmitted;
	private final Counter cdcBytesEmitted;
	private final RocksDBStatistics rocksDBStatistics;
	private final boolean fastGet;
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
		this.putTimer = createActionTimer(Put.class);
		this.putMultiTimer = createActionTimer(PutMulti.class);
		this.putBatchTimer = createActionTimer(PutBatch.class);
		this.getTimer = createActionTimer(Get.class);
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
		} catch (GestaltException e) {
			throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, "Can't get the scheduler parallelism");
		}
		this.leakScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("db-leak-scheduler"));

		leakScheduler.scheduleWithFixedDelay(this::cleanupExpiredTransactionsNow, 1, 1, TimeUnit.MINUTES);

		leakScheduler.scheduleWithFixedDelay(this::cleanupExpiredIteratorsNow, 1, 1, TimeUnit.MINUTES);

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
		// Wait for 10 seconds
		try {
			logger.info("Closing... waiting for ops");
			ops.closeAndWait(MAX_TRANSACTION_DURATION_MS);
			// Normal shutdown path
			logger.info("Ops finished, closing resources");
			closeResources(false);
		} catch (TimeoutException e) {
			logger.error(
					"Some operations lasted more than 10 seconds, forcing database shutdown... pendingOps={}, openTxs={}, openIterators={}",
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
				throw new IllegalStateException("Some operations lasted more than 10 seconds! pendingOps=" + ops.getPendingOpsCount() + ", openTxs=" + txs.size() + ", openIterators=" + its.size());
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

		// Close any remaining transactions to avoid hanging on DB close
		if (!txs.isEmpty()) {
			logger.warn("Closing {} remaining transactions", txs.size());
			for (var tx : txs.values()) {
				try {
					tx.close();
				} catch (Throwable t) {
					logger.error("Error closing remaining transaction", t);
				} finally {
					ops.endOp(); // Balance
				}
			}
			txs.clear();
		}
		// Close any remaining iterators
		if (!its.isEmpty()) {
			logger.warn("Closing {} remaining iterators", its.size());
			for (var it : its.values()) {
				try {
					it.close();
				} catch (Throwable t) {
					logger.error("Error closing remaining iterator", t);
				} finally {
					ops.endOp(); // Balance
				}
			}
			its.clear();
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

	/**
	 * Force-close any remaining transactions/iterators and balance pending ops. Invoked during shutdown if SafeShutdown
	 * times out.
	 */
	private void forceCloseLeakedResources() {
		int closedTx = 0;
		int closedIts = 0;
		try {
			// Transactions
			for (var tx : txs.values()) {
				try {
					tx.close();
				} catch (Throwable t) {
					logger.warn("Failed to close transaction during forced shutdown", t);
				} finally {
					// Balance the beginOp done at transaction open
					ops.endOp();
					closedTx++;
				}
			}
			txs.clear();

			// Iterators
			for (var it : its.values()) {
				try {
					it.close();
				} catch (Throwable t) {
					logger.warn("Failed to close iterator during forced shutdown", t);
				} finally {
					// Balance the beginOp done at iterator open
					ops.endOp();
					closedIts++;
				}
			}
			its.clear();

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
				var tx = EmbeddedDB.this.txs.remove(id);
				if (tx != null) {
					try {
						if (tx.val().isOwningHandle()) {
							tx.val().rollback();
						}
					} catch (Throwable ex) {
						logger.error("Failed to rollback a transaction", ex);
					}
					try {
						tx.close();
					} catch (Throwable ex) {
						logger.error("Failed to close a transaction", ex);
					}
					// Balance the pending op started in openTransaction
					ops.endOp();
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
				var it = EmbeddedDB.this.its.remove(id);
				if (it != null) {
					try {
						it.close();
					} catch (Throwable ex) {
						logger.error("Failed to close an iteration", ex);
					}
					// Balance the pending op started in openIterator
					ops.endOp();
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

	private ReadOptions newReadOptions(String label) {
		var ro = new LeakSafeReadOptions(label);
		ro.setAsyncIo(true);
		return ro;
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
		if (tx != null) {
			try {
				var succeeded = closeTransactionInternal(tx, commit);
				if (!succeeded) {
					return false;
				}
				txs.remove(transactionId, tx);
				return true;
			} catch (Throwable ex) {
				txs.remove(transactionId, tx);
				throw ex;
			}
		} else {
			// Transaction not found
			if (commit) {
				throw RocksDBException.of(RocksDBErrorType.TX_NOT_FOUND, "Transaction not found: " + transactionId);
			} else {
				return true;
			}
		}
	}

	/**
	 * @return false if failed optimistic commit
	 */
	@Contract("_, false -> true; _, true -> _")
	private boolean closeTransactionInternal(@NotNull Tx tx, boolean commit) {
		ops.beginOp();
		// Transaction found
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
		} finally {
			ops.endOp();
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
		try {
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
			var cf = new CompletableFuture<Void>();
			Flux
					.from(batchPublisher)
					.subscribeOn(scheduler.write())
					.publishOn(scheduler.write())
					.subscribe(new Subscriber<>() {
						private boolean stopped;
						private Subscription subscription;
						private ColumnInstance col;
						private ArrayList<AutoCloseable> refs;
						private DBWriter writer;

						@Override
						public void onSubscribe(Subscription subscription) {
							ops.beginOp();

							try {
								// Column id
								col = getColumn(columnId);
								refs = new ArrayList<>();

								writer = switch (mode) {
									case WRITE_BATCH, WRITE_BATCH_NO_WAL -> {
										var wb = new WB(db.get(), new LeakSafeWriteBatch(), mode == PutBatchMode.WRITE_BATCH_NO_WAL);
										refs.add(wb);
										yield wb;
									}
									case SST_INGESTION, SST_INGEST_BEHIND -> {
										var sstWriter = getSSTWriter(columnId, null, false, mode == PutBatchMode.SST_INGEST_BEHIND);
										refs.add(sstWriter);
										yield sstWriter;
									}
								};
							} catch (Throwable ex) {
								doFinally();
								throw ex;
							}
							this.subscription = subscription;
							subscription.request(1);
						}

						@Override
						public void onNext(KVBatch kvBatch) {
							if (stopped) {
								return;
							}
							var keyIt = kvBatch.keys().iterator();
							var valueIt = kvBatch.values().iterator();
							try {
								while (keyIt.hasNext()) {
									var keys = keyIt.next();
									var value = valueIt.next();
									actionLogger.logAction("PutBatch (next)", start, columnId, keys, value, null, null, null, mode);
									put(writer, col, 0, keys, value, RequestType.none());
								}
							} catch (RocksDBException ex) {
								doFinally();
								cf.completeExceptionally(ex);
								return;
							} catch (Throwable ex) {
								doFinally();
								var ex2 = RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
								cf.completeExceptionally(ex2);
								return;
							}
							subscription.request(1);
						}

						@Override
						public void onError(Throwable throwable) {
							cf.completeExceptionally(throwable);
							doFinally();
						}

						@Override
						public void onComplete() {
							try {
								try {
									writer.writePending();
								} catch (Throwable ex) {
									cf.completeExceptionally(ex);
									return;
								}
								cf.complete(null);
							} finally {
								doFinally();
							}
						}

						private void doFinally() {
							try {
								stopped = true;
								for (int i = refs.size() - 1; i >= 0; i--) {
									try {
										var c = refs.get(i);
										if (c instanceof AbstractImmutableNativeReference fr) {
											if (fr.isOwningHandle()) {
												c.close();
											}
										} else {
											c.close();
										}
									} catch (Exception ex) {
										logger.error("Failed to close reference during batch write", ex);
									}
								}
							} finally {
								ops.endOp();
								var end = System.nanoTime();
								putBatchTimer.record(end - start, TimeUnit.NANOSECONDS);
							}
						}
					});
			return cf;
		} catch (RocksDBException ex) {
			var end = System.nanoTime();
			putBatchTimer.record(end - start, TimeUnit.NANOSECONDS);
			throw ex;
		} catch (Exception ex) {
			var end = System.nanoTime();
			putBatchTimer.record(end - start, TimeUnit.NANOSECONDS);
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		}
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
		try {
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
			var cf = new CompletableFuture<Void>();
			Flux<KVBatch> source = Flux.from(batchPublisher).subscribeOn(scheduler.write()).publishOn(scheduler.write());
			AdaptiveBatcher.buffer(source, 128, 4096, Duration.ofMillis(10)).subscribe(new Subscriber<>() {
				private boolean stopped;
				private Subscription subscription;
				private ColumnInstance col;
				private ArrayList<AutoCloseable> refs;
				private DBWriter writer;
				private ArrayList<Map.Entry<Keys, Buf>> pendingSstEntries;

				@Override
				public void onSubscribe(Subscription subscription) {
					ops.beginOp();

					try {
						col = getColumn(columnId);
						refs = new ArrayList<>();
						writer = switch (mode) {
							case MERGE_WRITE_BATCH -> {
								if (col.hasBuckets()) {
									var tx = openTransactionInternal(120_000, false);
									refs.add(tx);
									yield tx;
								}
								var wb = new WB(db.get(), new LeakSafeWriteBatch(), false);
								refs.add(wb);
								yield wb;
							}
							case MERGE_WRITE_BATCH_NO_WAL -> {
								if (col.hasBuckets()) {
									var tx = openTransactionInternal(120_000, false);
									refs.add(tx);
									yield tx;
								}
								var wb = new WB(db.get(), new LeakSafeWriteBatch(), true);
								refs.add(wb);
								yield wb;
							}
							case MERGE_SST_INGESTION -> {
								var sst = getSSTWriter(columnId, null, false, false);
								refs.add(sst);
								pendingSstEntries = new ArrayList<>();
								yield sst;
							}
							case MERGE_SST_INGEST_BEHIND -> {
								if (!ingestBehindEnabled) {
									throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
											"MERGE_SST_INGEST_BEHIND requires database.global.ingest-behind=true"
									);
								}
								var sst = getSSTWriter(columnId, null, false, true);
								refs.add(sst);
								pendingSstEntries = new ArrayList<>();
								yield sst;
							}
						};
					} catch (Throwable ex) {
						doFinally();
						throw ex;
					}
					this.subscription = subscription;
					subscription.request(1);
				}

				@Override
				public void onNext(List<KVBatch> batches) {
					if (stopped) {
						return;
					}
					try {
						for (var kvBatch : batches) {
							var keyIt = kvBatch.keys().iterator();
							var valueIt = kvBatch.values().iterator();
							while (keyIt.hasNext()) {
								var keys = keyIt.next();
								var value = valueIt.next();
								actionLogger.logAction("MergeBatch (next)", start, columnId, keys, value, null, null, null, mode);
								switch (writer) {
									case SSTWriter ignored -> pendingSstEntries.add(Map.entry(keys, value));
									default -> merge(writer, col, 0L, keys, value, RequestType.none());
								}
							}
						}
						// Flush WB immediately after processing the group of batches
						if (writer instanceof WB wb) {
							if (wb.wb().count() > 0) {
								wb.writePending();
								wb.wb().clear();
							}
						}
					} catch (RocksDBException ex) {
						doFinally();
						cf.completeExceptionally(ex);
						return;
					} catch (Throwable ex) {
						doFinally();
						var ex2 = RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
						cf.completeExceptionally(ex2);
						return;
					}
					subscription.request(1);
				}

				@Override
				public void onError(Throwable throwable) {
					cf.completeExceptionally(throwable);
					doFinally();
				}

 			@Override
				public void onComplete() {
					try {
						switch (writer) {
							case WB wb -> wb.writePending();
							case Tx tx -> closeTransactionInternal(tx, true);
							case SSTWriter sst -> {
								writeSstEntries(col, sst, pendingSstEntries, mode == MergeBatchMode.MERGE_SST_INGEST_BEHIND);
								sst.writePending();
							}
							case null -> {
							}
						}
						cf.complete(null);
					} catch (Throwable ex) {
						cf.completeExceptionally(ex);
					} finally {
						doFinally();
					}
				}

 			private void doFinally() {
					if (!stopped) {
						stopped = true;
						ops.endOp();
						if (subscription != null) {
							subscription.cancel();
						}
						if (refs != null) {
							for (var ref : refs) {
								try {
									ref.close();
								} catch (Exception ex) {
									logger.debug("Failed to close resource in MergeBatch", ex);
								}
							}
						}
					}
				}
			});
			return cf;
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		}
	}

	private void writeSstEntries(ColumnInstance col,
			SSTWriter sst,
			@Nullable List<Map.Entry<Keys, Buf>> entries,
			boolean ingestBehind) throws RocksDBException {
		if (entries == null || entries.isEmpty()) {
			return;
		}
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
					}
				}
			}
		} catch (org.rocksdb.RocksDBException e) {
			throw RocksDBException.of(RocksDBErrorType.SST_WRITE_3, e);
		}
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
				try (var readOptions = newReadOptions(null)) {
					Buf previousRawBucket = dbGet(tx, col, readOptions, calculatedKey);
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
					try (var readOptions = newReadOptions(null)) {
						foundValue = dbGet(tx, col, readOptions, calculatedKey);
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
			Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException {
		var start = System.nanoTime();
		// Open an operation that ends when the iterator is closed
		ops.beginOp();
		try {
			actionLogger.logAction("OpenIterator",
					start,
					columnId,
					null,
					null,
					null,
					null,
					timeoutMs,
					null
			); // todo: improve logging
			var expirationTimestamp = timeoutMs + System.currentTimeMillis();
			var col = getColumn(columnId);
			RocksIterator it;
			var ro = newReadOptions("open-iterator-read-options");
			try {
				if (transactionId > 0L) {
					//noinspection resource
					it = getTransaction(transactionId, false).val().getIterator(ro, col.cfh());
				} else {
					it = db.get().newIterator(col.cfh(), ro);
				}
			} catch (Throwable ex) {
				ro.close();
				throw ex;
			}
			var itEntry = new REntry<>(it, expirationTimestamp, new RocksDBObjects(ro));
			try {
				return FastRandomUtils.allocateNewValue(its, itEntry, 1, Long.MAX_VALUE);
			} catch (Throwable ex) {
				itEntry.close();
				throw ex;
			}
		} catch (Throwable ex) {
			ops.endOp();
			var end = System.nanoTime();
			openIteratorTimer.record(end - start, TimeUnit.NANOSECONDS);
			throw ex;
		}
	}

	@Override
	public void closeIterator(long iteratorId) throws RocksDBException {
		var start = System.nanoTime();
		try {
			actionLogger.logAction("CloseIterator", start, null, null, null, null, null, null, null); // todo: improve logging
			var entry = its.remove(iteratorId);
			if (entry != null) {
				try {
					entry.close();
				} finally {
					// Balance the pending op started in openIterator
					ops.endOp();
				}
			} else {
				// If iterator not found, ignore
			}
		} finally {
			var end = System.nanoTime();
			closeIteratorTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	@Override
	public void seekTo(long iterationId, @NotNull Keys keys) throws RocksDBException {
		var start = System.nanoTime();
		ops.beginOp();
		try {
			actionLogger.logAction("SeekTo", start, null, null, null, null, null, null, null); // todo: improve logging
			throw new UnsupportedOperationException();
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
			actionLogger.logAction("Subsequent", start, null, null, null, null, null, null, null); // todo: improve logging
			throw new UnsupportedOperationException();
		} finally {
			ops.endOp();
			var end = System.nanoTime();
			subsequentTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
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
					if (transactionId > 0L) {
						//noinspection resource
						it = getTransaction(transactionId, false).val().getIterator(ro, col.cfh());
					} else {
						it = db.get().newIterator(col.cfh(), ro);
					}
					try (it) {
						return (T) switch (requestType) {
							case RequestEntriesCount<?> _ -> {
								if (col.hasBuckets() || calculatedStartKey != null || calculatedEndKey != null || path == null) {
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
									yield count;
								} else {
									Map<String, TableProperties> props;
									try {
										props = db.get().getPropertiesOfAllTables(col.cfh());
									} catch (org.rocksdb.RocksDBException e) {
										throw RocksDBException.of(RocksDBErrorType.GET_PROPERTY_ERROR, e);
									}
									long entries = 0;
									for (TableProperties tableProperties : props.values()) {
										entries += tableProperties.getNumEntries();
									}
									yield entries;
								}
							}
							case RequestType.RequestGetFirstAndLast<?> _ -> {
								if (!reverse) {
									it.seekToFirst();
								} else {
									it.seekToLast();
								}
								if (!it.isValid()) {
									yield new FirstAndLast<>(null, null);
								}
								var calculatedKey = toBuf(it.key());
								var calculatedValue = (col.schema().hasValue() || col.hasBuckets()) ? toBuf(it.value()) : emptyBuf();
								KV first;
								if (col.hasBuckets()) {
									var bucket = new Bucket(col, calculatedValue);
									var elements = bucket.getElements();
									var entry = !reverse ? elements.get(0) : elements.get(elements.size() - 1);
									first = decodeBucketEntry(col, calculatedKey, entry);
								} else {
									first = decodeKV(col, calculatedKey, calculatedValue);
								}

								if (!reverse) {
									it.seekToLast();
								} else {
									it.seekToFirst();
								}

								calculatedKey = toBuf(it.key());
								calculatedValue = (col.schema().hasValue() || col.hasBuckets()) ? toBuf(it.value()) : emptyBuf();
								KV last;
								if (col.hasBuckets()) {
									var bucket = new Bucket(col, calculatedValue);
									var elements = bucket.getElements();
									var entry = !reverse ? elements.get(elements.size() - 1) : elements.get(0);
									last = decodeBucketEntry(col, calculatedKey, entry);
								} else {
									last = decodeKV(col, calculatedKey, calculatedValue);
								}
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

		final class IteratorResources {

			private final ColumnInstance col;
			private ReadOptions ro;
			private final AbstractSlice<?> startKeySlice;
			private final AbstractSlice<?> endKeySlice;
			private RocksIterator it;
			private final StampedLock resourceLock = new StampedLock();
			private java.util.ListIterator<Entry<Buf[], Buf>> bucketIterator;
			private long itemsRead = 0;
			private final boolean canRefresh;
			private boolean closed = false;

			IteratorResources(ColumnInstance col,
					ReadOptions ro,
					AbstractSlice<?> startKeySlice,
					AbstractSlice<?> endKeySlice,
					RocksIterator it,
					boolean canRefresh) {
				this.col = col;
				this.ro = ro;
				this.startKeySlice = startKeySlice;
				this.endKeySlice = endKeySlice;
				this.it = it;
				this.canRefresh = canRefresh;
			}

			public void refresh(boolean reverse) throws RocksDBException {
				if (!canRefresh) return;
				long stamp = resourceLock.writeLock();
				try {
					if (closed) return;
					if (!it.isValid()) return;

					byte[] currentKey = it.key(); // Copy key

					it.close();
					ro.close();

					var newRo = newReadOptions("get-range-async-read-options");
					try {
						if (startKeySlice != null) {
							newRo.setIterateLowerBound(startKeySlice);
						}
						if (endKeySlice != null) {
							newRo.setIterateUpperBound(endKeySlice);
						}

						RocksIterator newIt = db.get().newIterator(col.cfh(), newRo);
						try {
							if (reverse) {
								newIt.seekForPrev(currentKey);
							} else {
								newIt.seek(currentKey);
							}

							this.ro = newRo;
							this.it = newIt;
						} catch (Throwable t) {
							newIt.close();
							throw t;
						}
					} catch (Throwable t) {
						newRo.close();
						throw t;
					}
				} finally {
					resourceLock.unlockWrite(stamp);
				}
			}

			public void close() {
				var wl = resourceLock.writeLock();
				try {
					if (closed) return;
					closed = true;
					it.close();
					ro.close();
					if (endKeySlice != null) {
						endKeySlice.close();
					}
					if (startKeySlice != null) {
						startKeySlice.close();
					}
				} finally {
					resourceLock.unlockWrite(wl);
				}
			}

		}

		return Flux.<T, IteratorResources>generate(() -> {
					ops.beginOp();
					try {
						var initializationStartTime = System.nanoTime();
						var col = getColumn(columnId);

						IteratorResources res;

						var ro = newReadOptions("get-range-async-read-options");
						try {
							Buf calculatedStartKey = startKeysInclusive != null && startKeysInclusive.keys().length > 0 ? col.calculateKey(
									startKeysInclusive.keys()) : null;
							Buf calculatedEndKey =
									endKeysExclusive != null && endKeysExclusive.keys().length > 0 ? col.calculateKey(endKeysExclusive.keys())
											: null;
							var startKeySlice = calculatedStartKey != null ? toSlice(calculatedStartKey) : null;
							try {
								var endKeySlice = calculatedEndKey != null ? toSlice(calculatedEndKey) : null;
								try {
									if (startKeySlice != null) {
										ro.setIterateLowerBound(startKeySlice);
									}
									if (endKeySlice != null) {
										ro.setIterateUpperBound(endKeySlice);
									}

									RocksIterator it;
									if (transactionId > 0L) {
										//noinspection resource
										it = getTransaction(transactionId, false).val().getIterator(ro, col.cfh());
									} else {
										it = db.get().newIterator(col.cfh(), ro);
									}
									res = new IteratorResources(col, ro, startKeySlice, endKeySlice, it, transactionId == 0);
								} catch (Throwable ex) {
									if (endKeySlice != null) {
										endKeySlice.close();
									}
									throw ex;
								}
							} catch (Throwable ex) {
								if (startKeySlice != null) {
									startKeySlice.close();
								}
								throw ex;
							}
						} catch (Throwable ex) {
							ro.close();
							throw ex;
						} finally {
							totalTime.add(System.nanoTime() - initializationStartTime);
						}

						var seekStartTime = System.nanoTime();
						try {
							if (!reverse) {
								res.it.seekToFirst();
							} else {
								res.it.seekToLast();
							}
							return res;
						} catch (Throwable ex) {
							res.close();
							throw ex;
						} finally {
							totalTime.add(System.nanoTime() - seekStartTime);
						}
					} catch (Throwable t) {
						ops.endOp();
						throw t;
					}
				}, (res, sink) -> {
					T nextResult = null;
					var nextTime = System.nanoTime();
					var readLock = res.resourceLock.readLock();
					try {
						while (nextResult == null) {
							if (res.bucketIterator != null) {
								if (!reverse ? res.bucketIterator.hasNext() : res.bucketIterator.hasPrevious()) {
									var entry = !reverse ? res.bucketIterator.next() : res.bucketIterator.previous();
									var calculatedKey = toBuf(res.it.key());
									var kv = decodeBucketEntry(res.col, calculatedKey, entry);
									//noinspection unchecked
									nextResult = (T) Objects.requireNonNull(kv);
								} else {
									res.bucketIterator = null;
									if (!reverse) {
										res.it.next();
									} else {
										res.it.prev();
									}
								}
							} else {
								if (!res.it.isValid()) {
									nextResult = null;
									break;
								}
								var calculatedValue = (res.col.schema().hasValue() || res.col.hasBuckets()) ? toBuf(res.it.value()) : emptyBuf();
								if (res.col.hasBuckets()) {
									var bucket = new Bucket(res.col, calculatedValue);
									var elements = bucket.getElements();
									res.bucketIterator = elements.listIterator(!reverse ? 0 : elements.size());
								} else {
									var calculatedKey = toBuf(res.it.key());
									var kv = decodeKV(res.col, calculatedKey, calculatedValue);
									if (!reverse) {
										res.it.next();
									} else {
										res.it.prev();
									}
									//noinspection unchecked
									nextResult = (T) Objects.requireNonNull(kv);
								}
							}
						}
					} finally {
						res.resourceLock.unlockRead(readLock);
						totalTime.add(System.nanoTime() - nextTime);
					}

					if (nextResult != null) {
						sink.next(nextResult);
						res.itemsRead++;
						if (res.canRefresh && res.itemsRead >= ITERATOR_REFRESH_INTERVAL && res.bucketIterator == null) {
							try {
								res.refresh(reverse);
								res.itemsRead = 0;
							} catch (RocksDBException e) {
								sink.error(e);
							}
						}
					} else {
						sink.complete();
					}
					return res;
				}, res -> {
					var closeTime = System.nanoTime();
					try {
						res.close();
					} finally {
						totalTime.add(System.nanoTime() - closeTime);
						ops.endOp();
						getRangeTimer.record(totalTime.sum(), TimeUnit.NANOSECONDS);
					}
				}
		).subscribeOn(scheduler.read());
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

			Function<LiveFileMetaData, Publisher<SerializedKVBatch>> mapper = f -> {
				class ScanState implements AutoCloseable {
					private final SstFileReader reader;
					private final SstFileReaderIterator it;
					private final ReadOptions readOptions;
					private final Options options;
					private boolean closed = false;
					private final Buf outBuf = Buf.create(Math.toIntExact(SizeUnit.MB));

					public ScanState() throws org.rocksdb.RocksDBException {
						ColumnFamilyOptions cfOpts = columnsConifg.get(cfName);
						this.options = cfOpts != null ? new Options(dbOptions, cfOpts) : new Options();
						this.options.setAllowMmapReads(true);
						this.options.setUseDirectReads(false);
						this.options.setUseDirectIoForFlushAndCompaction(false);
						this.options.setParanoidChecks(false);

						this.reader = new SstFileReader(options);
						String filePath = f.path();
						if (!filePath.endsWith(".sst")) {
							filePath = filePath + f.fileName();
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
								logger.debug("SST file missing during scan (ignoring): " + f.fileName());
							} else {
								logger.warn("Failed to open SST file: " + f.fileName(), e);
								this.options.close();
								throw e;
							}
						}
						this.readOptions = tmpReadOptions;
						this.it = tmpIt;
					}

					public synchronized void generateNext(SynchronousSink<SerializedKVBatch> sink) {
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
								if (batchSize >= 65536 || currentBatchBytes >= 2 * SizeUnit.MB) {
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
							if (reader != null) reader.close();
							if (options != null) options.close();
						}
					}
				}

				return Flux.<SerializedKVBatch, ScanState>generate(
						ScanState::new,
						(state, sink) -> {
							state.generateNext(sink);
							return state;
						},
						ScanState::close
				);
			};

			var ssts = Flux.fromIterable(files)
					.filter(f -> new String(f.columnFamilyName(), StandardCharsets.UTF_8).equals(cfName))
					.filter(f -> f.fileName().endsWith(".sst"));


			Flux<SerializedKVBatch> exec;
			if (shardCount == 1) {
				exec = ssts
						.parallel(4, 1)
						.runOn(getScheduler().read(), 1)
						.flatMap(mapper, false, 8, 1)
						.sequential();
			} else {
				exec = ssts
						.filter(m -> m.fileName().hashCode() % shardCount == shardIndex % shardCount)
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
			try (var fo = new FlushOptions()) {
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
				return dbGetIndirect(col.cfh(), readOptions, calculatedKey);
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
	private Buf dbGetDirect(ColumnFamilyHandle cfh, ReadOptions readOptions, Buf calculatedKey)
			throws org.rocksdb.RocksDBException {
		// Get the key nio buffer to pass to RocksDB
		ByteBuffer keyNioBuffer = calculatedKey.asHeapByteBuffer();

		// Create a direct result buffer because RocksDB works only with direct buffers
		var resultBuffer = ByteBuffer.allocate(INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES);
		var keyMayExist = this.db.get().keyMayExist(cfh, readOptions, keyNioBuffer.rewind(), resultBuffer.clear());
		return switch (keyMayExist.exists) {
			case kNotExist -> null;
			case kExistsWithValue, kExistsWithoutValue -> {
				// At the beginning, size reflects the expected size, then it becomes the real data size
				int size = keyMayExist.exists == kExistsWithValue ? keyMayExist.valueLength : -1;
				if (keyMayExist.exists == kExistsWithoutValue || size > resultBuffer.limit()) {
					if (size > resultBuffer.capacity()) {
						resultBuffer = ByteBuffer.allocate(size);
					}
					size = this.db.get().get(cfh, readOptions, keyNioBuffer.rewind(), resultBuffer.clear());
				}

				if (size == RocksDB.NOT_FOUND) {
					yield null;
				} else if (size == resultBuffer.limit()) {
					yield Utils.fromHeapByteBuffer(resultBuffer);
				} else {
					throw new IllegalStateException("size (" + size + ") != read size (" + resultBuffer.limit() + ")");
				}
			}
		};
	}

	@Nullable
	private Buf dbGetIndirect(ColumnFamilyHandle cfh, ReadOptions readOptions, Buf calculatedKey)
			throws org.rocksdb.RocksDBException {
		var valueHolder = new Holder<byte[]>();
		var keyMayExist = this.db
				.get()
				.keyMayExist(cfh,
						readOptions,
						calculatedKey.getBackingByteArray(),
						calculatedKey.getBackingByteArrayOffset(),
						calculatedKey.getBackingByteArrayLength(),
						valueHolder
				);
		if (keyMayExist) {
			var value = valueHolder.getValue();
			if (value != null && value.length
					!= 0 // todo: this is put in place to bypass a bug in rocksdb keyMayExist. It may return a 0-length array even if a value has some data...
			) {
				return Buf.wrap(value);
			} else {
				return toBuf(this.db
						.get()
						.get(cfh,
								readOptions,
								calculatedKey.getBackingByteArray(),
								calculatedKey.getBackingByteArrayOffset(),
								calculatedKey.getBackingByteArrayLength()
						));
			}
		} else {
			return null;
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
		try {
			// Fix: Flush WAL to ensure getUpdatesSince sees the latest operations
			db.get().flushWal(true);
		} catch (org.rocksdb.RocksDBException e) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
		}

		try (TransactionLogIterator it = db.get().getUpdatesSince(0)) {
			if (it != null && it.isValid()) {
				return it.getBatch().sequenceNumber();
			}
			return db.get().getLatestSequenceNumber() + 1; // empty DB
		} catch (org.rocksdb.RocksDBException e) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
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
	private static final long CDC_MAX_BYTES_PER_POLL = 16 * 1024 * 1024; // 16MB

	private class EventCollector extends WriteBatch.Handler {

		private final List<CDCEvent> out;
		private final @Nullable it.unimi.dsi.fastutil.longs.LongSet filter;
		private final long walSeq;
		private final int skipFirstOps;
		private int seenOps = 0;
		private int produced = 0;
		private long accumulatedBytes = 0;
		private final long maxToProduce;
		private final long maxBytes;
		private final boolean preserveKeys;
		// Fix: Track the exact op index where we hit the limit
		private int limitReachedAtOpIndex = -1;

		int getProducedCount() {
			return produced;
		}

		int getLimitReachedAtOpIndex() {
			return limitReachedAtOpIndex;
		}

		// FIX: Expose total iterated operations count
		int getSeenOps() {
			return seenOps;
		}

		EventCollector(List<CDCEvent> out,
				@Nullable it.unimi.dsi.fastutil.longs.LongSet filter,
				long walSeq,
				int skipFirstOps,
				long maxToProduce,
				long maxBytes,
				boolean preserveKeys) {
			this.out = out;
			this.filter = filter;
			this.walSeq = walSeq;
			this.skipFirstOps = skipFirstOps;
			this.maxToProduce = maxToProduce;
			this.maxBytes = maxBytes;
			this.preserveKeys = preserveKeys;
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
			if (!EmbeddedDB.this.columns.containsKey(colId)) {
				return;
			}
			if (filter != null && !filter.contains(colId)) {
				return;
			}

			byte[] finalKey = key;
			var colInstance = EmbeddedDB.this.columns.get(colId);
			if (!preserveKeys && colInstance != null && colInstance.hasBuckets()) {
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
			if (produced > 0 && accumulatedBytes + eventSize > maxBytes) {
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
			seenOps++;
		}

		// Default CF methods - skip data but MUST count ops
		@Override
		public void put(byte[] key, byte[] value) {
			seenOps++;
		}

		@Override
		public void merge(byte[] key, byte[] value) {
			seenOps++;
		}

		@Override
		public void delete(byte[] key) {
			seenOps++;
		}

		@Override
		public void singleDelete(byte[] key) {
			seenOps++;
		}

		@Override
		public void deleteRange(byte[] beginKey, byte[] endKey) {
			seenOps++;
		}

		// FIX: Count LogData and other markers to keep index in sync
		@Override
		public void logData(byte[] blob) {
			seenOps++;
		}

		@Override
		public void markBeginPrepare() {
			seenOps++;
		}

		@Override
		public void markEndPrepare(byte[] xid) {
			seenOps++;
		}

		@Override
		public void markCommit(byte[] xid) {
			seenOps++;
		}

		@Override
		public void markRollback(byte[] xid) {
			seenOps++;
		}

		@Override
		public void markNoop(boolean emptyBatch) {
			seenOps++;
		}

		@Override
		public void markCommitWithTimestamp(byte[] xid, byte[] ts) {
			seenOps++;
		}
	}

	private CdcBatch cdcPollOnce(String id, long fromSeq, long maxEvents) {
		try {
			var meta = loadCdcMeta(id);
			if (meta == null) {
				throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "CDC subscription not found: " + id);
			}
			long effectiveMax = maxEvents > 0 ? maxEvents : CDC_DEFAULT_MAX_EVENTS;
			if (effectiveMax > CDC_HARD_MAX_EVENTS) {
				effectiveMax = CDC_HARD_MAX_EVENTS;
			}
			long startSeq = fromSeq != 0 ? fromSeq : composeCdcSeq(findEarliestAvailableWalSeq(), 0);
			long walStart = extractWalSeq(startSeq);
			int opStart = extractOpIndex(startSeq);

			// Start iterator from the batch containing walStart
			long iteratorStartWal = walStart;

			List<CDCEvent> result = new ArrayList<>((int) Math.min(1024, effectiveMax));
			long nextSeq = startSeq;

			db.get().flushWal(true);

			// Check latest sequence to avoid "Requested sequence not yet written" exception in common case
			long latestSeq = db.get().getLatestSequenceNumber();
			if (walStart > latestSeq) {
				return new CdcBatch(java.util.Collections.emptyList(), fromSeq);
			}

			// Optimize: create filter set once
			it.unimi.dsi.fastutil.longs.LongSet filterSet = meta.columnFilter == null ? null : new it.unimi.dsi.fastutil.longs.LongOpenHashSet(meta.columnFilter);

			TransactionLogIterator it;
			try {
				it = db.get().getUpdatesSince(iteratorStartWal);
			} catch (org.rocksdb.RocksDBException e) {
				if (e.getMessage() != null && e.getMessage().contains("Requested sequence not yet written")) {
					return new CdcBatch(java.util.Collections.emptyList(), fromSeq);
				}
				throw e;
			}

			boolean firstBatch = true;
			try (it) {
				while (result.size() < effectiveMax && it != null && it.isValid()) {
					try {
						var br = it.getBatch();
						try {
							long batchWalSeq = br.sequenceNumber();

							if (firstBatch) {
								if (batchWalSeq > iteratorStartWal) {
									throw new CdcGapDetectedException("Gap detected in WAL. Requested WAL seq: " + iteratorStartWal + ", but earliest available is: " + batchWalSeq);
								}
								firstBatch = false;
							}

							// Skip batch if completely before start point (safe check)
							// If we somehow got a batch strictly before walStart (unlikely with getUpdatesSince), skipping it is correct.

							long diff = walStart - batchWalSeq;
							int skipOps = 0;
							if (diff >= 0) {
								skipOps = (int) diff + opStart;
							}

							var handler = new EventCollector(result,
									filterSet,
									batchWalSeq,
									skipOps,
									effectiveMax - result.size(),
									CDC_MAX_BYTES_PER_POLL,
									meta.emitLatestValues
							);
							//noinspection resource
							try {
								WriteBatchIterator.iterate(br.writeBatch().data(), handler);
							} catch (Exception e) {
								throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "Failed to parse WriteBatch at seq " + batchWalSeq, e);
							}

							int stopIndex = handler.getLimitReachedAtOpIndex();

							if (stopIndex != -1) {
								// We stopped in the middle of the batch
								nextSeq = composeCdcSeq(batchWalSeq, stopIndex);
							} else {
								// We finished the batch.
                                // Point to the end of this batch.
                                // The next poll will re-scan this batch, skip all items, and move to the next.
                                // This is safer than guessing the next sequence number.
									nextSeq = composeCdcSeq(batchWalSeq, handler.getSeenOps());
								}

							if (result.size() >= effectiveMax) {
								break;
							}

							it.next();
						} finally {
							br.writeBatch().close();
						}
					} catch (Exception e) {
						// On error, return what we have
						return new CdcBatch(result, nextSeq);
					}
				}
			}

			// If subscription requires resolved values, transform events accordingly for non-bucketed columns
			if (meta.emitLatestValues && !result.isEmpty()) {
				var transformed = new ArrayList<CDCEvent>(result.size());
				var keysToResolve = new ArrayList<byte[]>();
				var cfHandles = new ArrayList<ColumnFamilyHandle>();
				var indicesToResolve = new IntArrayList();

				// First pass: identify events that need resolution
				for (int i = 0; i < result.size(); i++) {
					CDCEvent ev = result.get(i);
					var col = this.columns.get(ev.columnId());
					if (col != null) {
						// We resolve ALL operations (PUT, MERGE, DELETE)
						// to the latest value to ensure monotonicity and avoid "time travel" corruption.
						keysToResolve.add(ev.key().asArray());
						cfHandles.add(col.cfh());
						indicesToResolve.add(i);
					}
				}

				if (!keysToResolve.isEmpty()) {
					List<byte[]> resolvedValues;
					try {
						// Batch read for performance
						resolvedValues = db.get().multiGetAsList(cfHandles, keysToResolve);
					} catch (org.rocksdb.RocksDBException e) {
						throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
					}

					int resolutionIndex = 0;
					for (int i = 0; i < result.size(); i++) {
						CDCEvent ev = result.get(i);
						var col = this.columns.get(ev.columnId());
						Buf finalKey = ev.key();
 					if (resolutionIndex < indicesToResolve.size() && indicesToResolve.getInt(resolutionIndex) == i) {
 						// This event was marked for resolution
 						byte[] valBytes = resolvedValues.get(resolutionIndex);
 						resolutionIndex++;

 						if (valBytes != null) {
 							// Key exists in current state -> Emit PUT with latest value
 							if (col != null && col.hasBuckets()) {
 								// Expand bucket into individual events with real keys
 								var bucket = new Bucket(col, Buf.wrap(valBytes));
 								var elements = bucket.getElements();

 								int fixedCount = col.schema().fixedLengthKeysCount();
 								int fixedBytes = 0;
 								for (int k = 0; k < fixedCount; k++) {
 									fixedBytes += col.schema().key(k);
 								}
 								// Extract fixed part from the original key (which is Fixed | Hash)
 								Buf fixedPart = finalKey.subList(0, fixedBytes);

 								for (var elem : elements) {
 									Buf[] varKeys = elem.getKey();
 									long totalSize = fixedBytes;
 									for (Buf vk : varKeys) totalSize += vk.size();

 									Buf realKey = Buf.createZeroes((int) totalSize);
 									realKey.setBytesFromBuf(0, fixedPart, 0, fixedBytes);
 									int offset = fixedBytes;
 									for (Buf vk : varKeys) {
 										int len = vk.size();
 										realKey.setBytesFromBuf(offset, vk, 0, len);
 										offset += len;
 									}

 									transformed.add(new CDCEvent(ev.seq(), ev.columnId(), realKey, elem.getValue(), CDCEvent.Op.PUT));
 								}
 							} else {
 								transformed.add(new CDCEvent(ev.seq(), ev.columnId(), finalKey, Buf.wrap(valBytes), CDCEvent.Op.PUT));
 							}
 						} else {
 							// Key does not exist in current state -> Emit DELETE
 							Buf delKey = finalKey;
 							if (col != null && col.hasBuckets()) {
 								// For DELETE on bucketed column, we can't know the variable keys.
 								// Truncate to Fixed Key to avoid exposing Hash.
 								int fixedCount = col.schema().fixedLengthKeysCount();
 								int fixedBytes = 0;
 								for (int k = 0; k < fixedCount; k++) {
 									fixedBytes += col.schema().key(k);
 								}
 								if (fixedBytes < finalKey.size()) {
 									delKey = Buf.wrap(java.util.Arrays.copyOf(finalKey.toByteArray(), fixedBytes));
 								}
 							}
 							transformed.add(new CDCEvent(ev.seq(), ev.columnId(), delKey, emptyBuf(), CDCEvent.Op.DELETE));
 						}
 					} else {
 						// Keep original event (bucketed column or unknown column)
 						transformed.add(new CDCEvent(ev.seq(), ev.columnId(), finalKey, ev.value(), ev.op()));
 					}
					}
				} else {
					// No events to resolve
					transformed.addAll(result);
				}
				result = transformed;
			}
			return new CdcBatch(result, nextSeq);
		} catch (org.rocksdb.RocksDBException e) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
		}
	}

	@Override
	public @NotNull java.util.stream.Stream<CDCEvent> cdcPoll(@NotNull String id, @Nullable Long fromSeq, long maxEvents)
			throws RocksDBException {
		ops.beginOp();
		try {
			long startSeq;
			if (fromSeq != null) {
				startSeq = fromSeq;
			} else {
				try {
					var meta = loadCdcMeta(id);
					if (meta == null) {
						throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "CDC subscription not found: " + id);
					}
					startSeq = meta.lastCommittedSeq + 1;
				} catch (org.rocksdb.RocksDBException e) {
					throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
				}
			}
			var batch = cdcPollOnce(id, startSeq, maxEvents);
			return batch.events().stream();
		} finally {
			ops.endOp();
		}
	}

	public @NotNull org.reactivestreams.Publisher<CDCEvent> cdcPollAsyncInternal(@NotNull String id,
			@Nullable Long fromSeq,
			long maxEvents) throws RocksDBException {
		long maxEv = maxEvents > 0 ? maxEvents : CDC_DEFAULT_MAX_EVENTS;
		// Note: This returns a Publisher for a single poll batch. Caller is responsible for looping/state.
		return Flux.defer(() -> {
			ops.beginOp();
			try {
				long startSeqInternal;
				if (fromSeq != null) {
					startSeqInternal = fromSeq;
				} else {
					var meta = loadCdcMeta(id);
					if (meta == null) {
						throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "CDC subscription not found: " + id);
					}
					startSeqInternal = meta.lastCommittedSeq + 1;
				}
				return Flux.fromIterable(cdcPollOnce(id, startSeqInternal, maxEv).events());
			} catch (Throwable t) {
				return Flux.error(t);
			} finally {
				ops.endOp();
			}
		}).subscribeOn(this.scheduler.read());
	}

	public @NotNull Mono<CdcBatch> cdcPollBatchAsyncInternal(@NotNull String id, @Nullable Long fromSeq, long maxEvents)
			throws RocksDBException {
		long maxEv = maxEvents > 0 ? maxEvents : CDC_DEFAULT_MAX_EVENTS;
		return Mono.fromCallable(() -> {
			ops.beginOp();
			try {
				long startSeq;
				if (fromSeq != null) {
					startSeq = fromSeq;
				} else {
					var meta = loadCdcMeta(id);
					if (meta == null) {
						throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "CDC subscription not found: " + id);
					}
					startSeq = meta.lastCommittedSeq + 1;
				}
				return cdcPollOnce(id, startSeq, maxEv);
			} finally {
				ops.endOp();
			}
		}).subscribeOn(this.scheduler.read());
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
