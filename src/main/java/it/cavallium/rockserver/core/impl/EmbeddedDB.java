package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.common.Utils.dummyRocksDBEmptyValue;
import static it.cavallium.rockserver.core.common.Utils.emptyBuf;
import static it.cavallium.rockserver.core.common.Utils.toBuf;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kExistsWithValue;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kExistsWithoutValue;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RequestType.RequestEntriesCount;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;
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
import org.rocksdb.DBOptions;
import org.rocksdb.DirectSlice;
import org.rocksdb.Holder;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Status.Code;
import org.rocksdb.TableProperties;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

public class EmbeddedDB implements RocksDBSyncAPI, InternalConnection, Closeable {

	private static final int INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES = 4096;
	public static final long MAX_TRANSACTION_DURATION_MS = 10_000L;
	private static final byte[] COLUMN_SCHEMAS_COLUMN = "_column_schemas_".getBytes(StandardCharsets.UTF_8);
	private final Logger logger;
	private final @Nullable Path path;
	private final TransactionalDB db;
	private final DBOptions dbOptions;
	private final RWScheduler scheduler;
	private final ScheduledExecutorService leakScheduler;
	private final ColumnFamilyHandle columnSchemasColumnDescriptorHandle;
	private final NonBlockingHashMapLong<ColumnInstance> columns;
	private final Map<String, ColumnFamilyOptions> columnsConifg;
	private final ConcurrentMap<String, Long> columnNamesIndex;
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

		var beforeLoad = Instant.now();
		this.config = config;
		var loadedDb = RocksDBLoader.load(path, config, logger);
		this.db = loadedDb.db();
		this.dbOptions = loadedDb.dbOptions();
		this.refs = loadedDb.refs();
		this.cache = loadedDb.cache();
		this.rocksDBStatistics = new RocksDBStatistics(name, dbOptions.statistics(), metrics, cache);
		try {
			int readCap = Objects.requireNonNullElse(config.parallelism().read(), Runtime.getRuntime().availableProcessors());
			int writeCap = Objects.requireNonNullElse(config.parallelism().write(), Runtime.getRuntime().availableProcessors());
			this.scheduler = new RWScheduler(readCap, writeCap, "db");
			this.fastGet = config.global().enableFastGet();
		} catch (GestaltException e) {
			throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, "Can't get the scheduler parallelism");
		}
		this.leakScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("db-leak-scheduler"));

		leakScheduler.scheduleWithFixedDelay(() -> {
			logger.debug("Cleaning expired transactions...");
			var idsToRemove = new LongArrayList();
			var startTime = System.currentTimeMillis();
			ops.beginOp();
			try {
				EmbeddedDB.this.txs.forEach(((txId, tx) -> {
					if (startTime >= tx.expirationTimestamp()) {
						idsToRemove.add((long) txId);
					}
				}));
				idsToRemove.forEach(id -> {
					var tx = EmbeddedDB.this.txs.remove(id);
					if (tx != null) {
						try {
							if (tx.val().isOwningHandle()) {
								tx.val().rollback();
							}
							tx.close();
						} catch (Throwable ex) {
							logger.error("Failed to close a transaction", ex);
						}
					}
				});
			} finally {
				ops.endOp();
			}
			var endTime = System.currentTimeMillis();
			var removedCount = idsToRemove.size();
			if (removedCount > 2) {
				logger.info("Cleaned {} expired transactions in {}, please debug leaked transactions if this number is too high",
						removedCount, Duration.ofMillis(endTime - startTime));
			} else {
				logger.debug("Cleaned {} expired transactions in {}", removedCount, Duration.ofMillis(endTime - startTime));
			}
		}, 1, 1, TimeUnit.MINUTES);

		leakScheduler.scheduleWithFixedDelay(() -> {
			logger.debug("Cleaning expired iterators...");
			var idsToRemove = new LongArrayList();
			var startTime = System.currentTimeMillis();
			ops.beginOp();
			try {
				EmbeddedDB.this.its.forEach(((itId, entry) -> {
					if (entry.expirationTimestamp() != null && startTime >= entry.expirationTimestamp()) {
						idsToRemove.add((long) itId);
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
					}
				});
			} finally {
				ops.endOp();
			}
			var endTime = System.currentTimeMillis();
			var removedCount = idsToRemove.size();
			if (removedCount > 2) {
				logger.info("Cleaned {} expired iterators in {}, please debug leaked iterators if this number is too high",
						removedCount, Duration.ofMillis(endTime - startTime));
			} else {
				logger.debug("Cleaned {} expired iterators in {}", removedCount, Duration.ofMillis(endTime - startTime));
			}
		}, 1, 1, TimeUnit.MINUTES);

		this.columnsConifg = loadedDb.definitiveColumnFamilyOptionsMap();
        try {
            this.tempSSTsPath = config.global().tempSstPath();
        } catch (GestaltException e) {
            throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, "Can't get wal path");
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
		try (var it = this.db.get().newIterator(columnSchemasColumnDescriptorHandle)) {
			it.seekToFirst();
			while (it.isValid()) {
				var key = it.key();
				ColumnSchema value = decodeColumnSchema(it.value());
				this.db
						.getStartupColumns()
						.entrySet()
						.stream()
						.filter(entry -> Arrays.equals(entry.getKey().getName(), key))
						.findAny()
						.ifPresent(entry -> registerColumn(new ColumnInstance(entry.getValue(), value)));
				it.next();
			}
		}
		if (Boolean.parseBoolean(System.getProperty("rockserver.core.print-config", "true"))) {
			logger.info("Database configuration: {}", ConfigPrinter.stringify(config));
		}
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
			assert check == 2;
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
			return new ColumnSchema(keys, colHashTypes, hasValue);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private byte[] encodeColumnSchema(ColumnSchema schema) {
		try (var baos = new ByteArrayOutputStream(); var daos = new DataOutputStream(baos)) {
			daos.writeByte(2);
			daos.writeInt(schema.keys().size());
			for (int key : schema.keys()) {
				daos.writeInt(key);
			}
			daos.writeInt(schema.variableTailKeys().size());
			for (ColumnHashType variableTailKey : schema.variableTailKeys()) {
				daos.writeByte(variableTailKey.ordinal());
			}
			daos.writeBoolean(schema.hasValue());
			baos.close();
			return baos.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * The column must be registered once!!!
	 * Do not try to register a column that may already be registered
	 */
	private long registerColumn(@NotNull ColumnInstance column) {
		synchronized (columnEditLock) {
			try {
				var columnName = new String(column.cfh().getName(), StandardCharsets.UTF_8);
				long hashCode = columnName.hashCode();
				long id;
				if (Objects.equals(this.columnNamesIndex.get(columnName), hashCode)) {
					id = hashCode;
				} else if (this.columns.get(hashCode) == null) {
					id = hashCode;
					this.columns.put(id, column);
				} else {
					id = FastRandomUtils.allocateNewValue(this.columns, column, 1, Long.MAX_VALUE);
				}
				Long previous = this.columnNamesIndex.putIfAbsent(columnName, id);
				if (previous != null) {
					//noinspection resource
					this.columns.remove(id);
					throw new UnsupportedOperationException("Column already registered!");
				}
				logger.info("Registered column: " + column);
				return id;
			} catch (org.rocksdb.RocksDBException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * The column must be unregistered once!!!
	 * Do not try to unregister a column that may already be unregistered, or that may not be registered
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
			// Unregister the column name from the index avoiding race conditions
			int retries = 0;
			while (this.columnNamesIndex.remove(name) == null && retries++ < 5_000) {
				Thread.yield();
			}
			if (retries >= 5000) {
				throw new IllegalStateException("Can't find column in column names index: " + name);
			}

			ColumnFamilyOptions columnConfig;
			while ((columnConfig = this.columnsConifg.remove(name)) == null && retries++ < 5_000) {
				Thread.yield();
			}
			if (columnConfig != null) {
				columnConfig.close();
			}
			if (retries >= 5000) {
				throw new IllegalStateException("Can't find column in column names index: " + name);
			}

			return col;
		}
	}

	@Override
	public void close() throws IOException {
		// Wait for 10 seconds
		try {
			ops.closeAndWait(MAX_TRANSACTION_DURATION_MS);
			columnSchemasColumnDescriptorHandle.close();
			db.close();
			refs.close();
			if (path == null) {
				Utils.deleteDirectory(db.getPath());
			}
			for (Meter meter : meters) {
				meter.close();
			}
			rocksDBStatistics.close();
			if (metrics != null) {
				metrics.close();
			}
			leakScheduler.close();
		} catch (TimeoutException e) {
			logger.error("Some operations lasted more than 10 seconds, forcing database shutdown...");
		}
	}

	private ReadOptions newReadOptions() {
		var ro = new ReadOptions() {
			{
				RocksLeakDetector.register(this, owningHandle_);
			}
		};
		ro.setAsyncIo(true);
		return ro;
	}

	@Override
	public long openTransaction(long timeoutMs) {
		var start = System.nanoTime();
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
			var writeOpts = new WriteOptions() {
				{
					RocksLeakDetector.register(this, owningHandle_);
				}
			};
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
			// Close the transaction operation
			ops.endOp();
			return true;
		} catch (org.rocksdb.RocksDBException e) {
			// Close the transaction operation
			ops.endOp();
			throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.COMMIT_FAILED, "Transaction close failed");
		} catch (Throwable ex) {
			ops.endOp();
			throw  ex;
		} finally {
			ops.endOp();
		}
	}

	@Override
	public void closeFailedUpdate(long updateId) throws it.cavallium.rockserver.core.common.RocksDBException {
		var start = System.nanoTime();
		try {
			closeTransactionInternal(updateId, false);
		} finally {
			var end = System.nanoTime();
			closeFailedUpdateTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	@Override
	public long createColumn(String name, @NotNull ColumnSchema schema) throws it.cavallium.rockserver.core.common.RocksDBException {
		var start = System.nanoTime();
		ops.beginOp();
		try {
			synchronized (columnEditLock) {
				var colId = getColumnIdOrNull(name);
				var col = colId != null ? getColumn(colId) : null;
				if (col != null) {
					if (schema.equals(col.schema())) {
						return colId;
					} else {
						throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.COLUMN_EXISTS,
								"Column exists, with a different schema: " + name
						);
					}
				} else {
					try {
						var options = RocksDBLoader.getColumnOptions(name, this.config.global(),
								logger, this.refs, path == null, cache);
						var prev = columnsConifg.put(name, options);
						if (prev != null) {
							throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.COLUMN_CREATE_FAIL,
									"ColumnsConfig already exists with name \"" + name + "\"");
						}
						byte[] key = name.getBytes(StandardCharsets.UTF_8);
						var cf = db.get().createColumnFamily(new ColumnFamilyDescriptor(key, options));
						byte[] value = encodeColumnSchema(schema);
						db.get().put(columnSchemasColumnDescriptorHandle, key, value);
						return registerColumn(new ColumnInstance(cf, schema));
					} catch (org.rocksdb.RocksDBException | GestaltException e) {
						throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.COLUMN_CREATE_FAIL, e);
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
	public void deleteColumn(long columnId) throws it.cavallium.rockserver.core.common.RocksDBException {
		var start = System.nanoTime();
		ops.beginOp();
		try {
			synchronized (columnEditLock) {
				var col = getColumn(columnId);
				try {
					db.get().dropColumnFamily(col.cfh());
					unregisterColumn(columnId).close();
				} catch (org.rocksdb.RocksDBException e) {
					throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.COLUMN_DELETE_FAIL, e);
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
		try {
			var columnId = getColumnIdOrNull(name);
			if (columnId == null) {
				throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.COLUMN_NOT_FOUND,
						"Column not found: " + name);
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

	@Override
	public <T> T put(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, T> requestType) throws it.cavallium.rockserver.core.common.RocksDBException {
		var start = System.nanoTime();
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
		} catch (it.cavallium.rockserver.core.common.RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			ops.endOp();
			var end = System.nanoTime();
			putTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	@Override
	public <T> List<T> putMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestPut<? super Buf, T> requestType) throws it.cavallium.rockserver.core.common.RocksDBException {
		var start = System.nanoTime();
		try {
			if (keys.size() != values.size()) {
				throw new IllegalArgumentException("keys length is different than values length: " + keys.size() + " != " + values.size());
			}
			List<T> responses = requestType instanceof RequestType.RequestNothing<?> ? null : new ArrayList<>(keys.size());
			for (int i = 0; i < keys.size(); i++) {
				var result = put(transactionOrUpdateId, columnId, keys.get(i), values.get(i), requestType);
				if (responses != null) {
					responses.add(result);
				}
			}
			return responses != null ? responses : List.of();
		} finally {
			var end = System.nanoTime();
			putMultiTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	public CompletableFuture<Void> putBatchInternal(long columnId,
						 @NotNull Publisher<@NotNull KVBatch> batchPublisher,
						 @NotNull PutBatchMode mode) throws it.cavallium.rockserver.core.common.RocksDBException {
		try {
			var cf = new CompletableFuture<Void>();
			batchPublisher.subscribe(new Subscriber<>() {
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
								var wb = new WB(db.get(), new WriteBatch() {
									{
										RocksLeakDetector.register(this, owningHandle_);
									}
								}, mode == PutBatchMode.WRITE_BATCH_NO_WAL);
								refs.add(wb);
								yield wb;
							}
							case SST_INGESTION, SST_INGEST_BEHIND -> {
								var sstWriter = getSSTWriter(columnId, null, false, mode == PutBatchMode.SST_INGEST_BEHIND);								refs.add(sstWriter);
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
							var key = keyIt.next();
							var value = valueIt.next();
							put(writer, col, 0, key, value, RequestType.none());
						}
					} catch (it.cavallium.rockserver.core.common.RocksDBException ex) {
						doFinally();
						cf.completeExceptionally(ex);
						return;
					} catch (Throwable ex) {
						doFinally();
						var ex2 = it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
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
					ops.endOp();
				}
			});
			return cf;
		} catch (it.cavallium.rockserver.core.common.RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		}
	}

	@VisibleForTesting
	public SSTWriter getSSTWriter(long colId,
								  @Nullable GlobalDatabaseConfig globalDatabaseConfigOverride,
								  boolean forceNoOptions,
								  boolean ingestBehind) throws it.cavallium.rockserver.core.common.RocksDBException {
		try {
			var col = getColumn(colId);
			ColumnFamilyOptions columnConifg;
			RocksDBObjects refs;
			if (!forceNoOptions) {
				var name = new String(col.cfh().getName(), StandardCharsets.UTF_8);
				refs = new RocksDBObjects();
				if (globalDatabaseConfigOverride != null) {
					columnConifg = RocksDBLoader.getColumnOptions(name, globalDatabaseConfigOverride, logger, refs, false, null);
				} else {
					try {
						columnConifg = RocksDBLoader.getColumnOptions(name, this.config.global(), logger, refs, false, null);
					} catch (GestaltException e) {
						throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, e);
					}
					refs = null;
				}
			} else {
				columnConifg = null;
				refs = null;
			}
			if (Files.notExists(tempSSTsPath)) {
				Files.createDirectories(tempSSTsPath);
			}
			return SSTWriter.open(tempSSTsPath, db, col, columnConifg, forceNoOptions, ingestBehind, refs);
		} catch (IOException ex) {
			throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.SST_WRITE_2, ex);
		} catch (org.rocksdb.RocksDBException ex) {
			throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.SST_WRITE_3, ex);
		}
	}

	@Override
	public void putBatch(long columnId,
						 @NotNull Publisher<@NotNull KVBatch> batchPublisher,
						 @NotNull PutBatchMode mode) throws it.cavallium.rockserver.core.common.RocksDBException {
		var start = System.nanoTime();
		try {
			putBatchInternal(columnId, batchPublisher, mode).get();
		} catch (it.cavallium.rockserver.core.common.RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			var end = System.nanoTime();
			putBatchTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private <U> U put(@Nullable DBWriter optionalDbWriter,
			ColumnInstance col,
			long updateId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, U> callback) throws it.cavallium.rockserver.core.common.RocksDBException {
		// Check for null value
		col.checkNullableValue(value);
		try {
			boolean requirePreviousValue = RequestType.requiresGettingPreviousValue(callback);
			boolean requirePreviousPresence = RequestType.requiresGettingPreviousPresence(callback);
			boolean needsTx = col.hasBuckets()
					|| requirePreviousValue
					|| requirePreviousPresence;
			if (optionalDbWriter instanceof Tx tx && tx.isFromGetForUpdate() && (requirePreviousValue || requirePreviousPresence)) {
				throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"You can't get the previous value or delta, when you are already updating that value");
			}
			if (updateId != 0L && !(optionalDbWriter instanceof Tx)) {
				throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Update id must be accompanied with a valid transaction");
			}
			if (col.hasBuckets() && (optionalDbWriter != null && !(optionalDbWriter instanceof Tx))) {
				throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Column with buckets don't support write batches");
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
					if (updateId != 0L) {
						assert !owningNewTx;
						((Tx) newTx).val().setSavePoint();
					}
					if (col.hasBuckets()) {
						assert newTx instanceof Tx;
						var bucketElementKeys = col.getBucketElementKeys(keys.keys());
						try (var readOptions = newReadOptions()) {
							var previousRawBucketByteArray = ((Tx) newTx).val().getForUpdate(readOptions, col.cfh(), calculatedKey.toByteArray(), true);
							didGetForUpdateInternally = true;
							Buf previousRawBucket = toBuf(previousRawBucketByteArray);
							var bucket = previousRawBucket != null ? new Bucket(col, previousRawBucket) : new Bucket(col);
							previousValue = transformResultValue(col, bucket.addElement(bucketElementKeys, value));
							var k = Utils.toByteArray(calculatedKey);
							var v = Utils.toByteArray(bucket.toSegment());
							((Tx) newTx).val().put(col.cfh(), k, v);
						} catch (org.rocksdb.RocksDBException e) {
							throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_1, e);
						}
					} else {
						if (RequestType.requiresGettingPreviousValue(callback)) {
							assert newTx instanceof Tx;
							try (var readOptions = newReadOptions()) {
								byte[] previousValueByteArray;
								previousValueByteArray = ((Tx) newTx).val().getForUpdate(readOptions, col.cfh(), calculatedKey.toByteArray(), true);
								didGetForUpdateInternally = true;
								previousValue = transformResultValue(col, toBuf(previousValueByteArray));
							} catch (org.rocksdb.RocksDBException e) {
								throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_2, e);
							}
						} else if (RequestType.requiresGettingPreviousPresence(callback)) {
							// todo: in the future this should be replaced with just keyExists
							assert newTx instanceof Tx;
							try (var readOptions = newReadOptions()) {
								byte[] previousValueByteArray;
								previousValueByteArray = ((Tx) newTx).val().getForUpdate(readOptions, col.cfh(), calculatedKey.toByteArray(), true);
								didGetForUpdateInternally = true;
								previousValue = previousValueByteArray != null ? Utils.emptyBuf() : null;
							} catch (org.rocksdb.RocksDBException e) {
								throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_2, e);
							}
						} else {
							previousValue = null;
						}
						switch (newTx) {
							case WB wb -> wb.wb().put(col.cfh(), calculatedKey.toByteArray(), value.toByteArray());
							case SSTWriter sstWriter -> {
								var keyBB = calculatedKey.toByteArray();
								var valueBB = (col.schema().hasValue() ? value : dummyRocksDBEmptyValue()).toByteArray();
								sstWriter.put(keyBB, valueBB);
							}
							case Tx t -> t.val().put(col.cfh(), calculatedKey.toByteArray(), value.toByteArray());
							case null -> {
								try (var w = new WriteOptions() {
									{
										RocksLeakDetector.register(this, owningHandle_);
									}
								}) {
									var keyBB = calculatedKey.toByteArray();
									var valueBB = (col.schema().hasValue() ? value : dummyRocksDBEmptyValue()).toByteArray();
									db.get().put(col.cfh(), w, keyBB, valueBB);
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
							((Tx) newTx).val().rollbackToSavePoint();
							int undosCount = 0;
							if (((Tx) newTx).isFromGetForUpdate()) {
								undosCount++;
							}
							if (didGetForUpdateInternally) {
								undosCount++;
							}
							for (int i = 0; i < undosCount; i++) {
								((Tx) newTx).val().undoGetForUpdate(col.cfh(), Utils.toByteArray(calculatedKey));
							}
							throw new RocksDBRetryException();
						}
					}

					if (owningNewTx) {
						committedOwnedTx = this.closeTransactionInternal((Tx) newTx, true);
						if (!committedOwnedTx) {
							if (didGetForUpdateInternally) {
								((Tx) newTx).val().undoGetForUpdate(col.cfh(), Utils.toByteArray(calculatedKey));
							}
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
			if (ex instanceof it.cavallium.rockserver.core.common.RocksDBException rocksDBException) {
				throw rocksDBException;
			} else {
				throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
			}
		}
	}

	private Buf transformResultValue(ColumnInstance col, Buf realPreviousValue) {
		return col.schema().hasValue() ? realPreviousValue : (realPreviousValue != null ? emptyBuf() : null);
	}

	@Override
	public <T> T get(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			RequestGet<? super Buf, T> requestType) throws it.cavallium.rockserver.core.common.RocksDBException {
		var start = System.nanoTime();
		try {
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
				return get(tx, updateId, col, keys, requestType);
			} catch (Throwable ex) {
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

	private <T> T get(Tx tx,
			long updateId,
			ColumnInstance col,
			Keys keys,
			RequestGet<? super Buf, T> callback) throws it.cavallium.rockserver.core.common.RocksDBException {
		ops.beginOp();
		try {
			if (!col.schema().hasValue() && RequestType.requiresGettingCurrentValue(callback)) {
				throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.VALUE_MUST_BE_NULL,
						"The specified callback requires a return value, but this column does not have values!");
			}
			Buf foundValue;
			boolean existsValue;

			Buf calculatedKey = col.calculateKey(keys.keys());
			if (col.hasBuckets()) {
				var bucketElementKeys = col.getBucketElementKeys(keys.keys());
				try (var readOptions = newReadOptions()) {
					Buf previousRawBucket = dbGet(tx, col, readOptions, calculatedKey);
					if (previousRawBucket != null) {
						var bucket = new Bucket(col, previousRawBucket);
						foundValue = bucket.getElement(bucketElementKeys);
					} else {
						foundValue = null;
					}
					existsValue = foundValue != null;
				} catch (org.rocksdb.RocksDBException e) {
					throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.GET_1, e);
				}
			} else {
				boolean shouldGetCurrent = RequestType.requiresGettingCurrentValue(callback)
						|| (tx != null && callback instanceof RequestType.RequestExists<?>);
				if (shouldGetCurrent) {
					try (var readOptions = newReadOptions()) {
						foundValue = dbGet(tx, col, readOptions, calculatedKey);
						existsValue = foundValue != null;
					} catch (org.rocksdb.RocksDBException e) {
						throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_2, e);
					}
				} else if (callback instanceof RequestType.RequestExists<?>) {
					// tx is always null here
					//noinspection ConstantValue
					assert tx == null;
					foundValue = null;
					existsValue = db.get().keyExists(col.cfh(),
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
		} catch (it.cavallium.rockserver.core.common.RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
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
			long timeoutMs) throws it.cavallium.rockserver.core.common.RocksDBException {
		var start = System.nanoTime();
		// Open an operation that ends when the iterator is closed
		ops.beginOp();
		try {
			var expirationTimestamp = timeoutMs + System.currentTimeMillis();
			var col = getColumn(columnId);
			RocksIterator it;
			var ro = newReadOptions();
			if (transactionId > 0L) {
				//noinspection resource
				it = getTransaction(transactionId, false).val().getIterator(ro, col.cfh());
			} else {
				it = db.get().newIterator(col.cfh(), ro);
			}
			var itEntry = new REntry<>(it, expirationTimestamp, new RocksDBObjects(ro));
            return FastRandomUtils.allocateNewValue(its, itEntry, 1, Long.MAX_VALUE);
		} catch (Throwable ex) {
			ops.endOp();
			var end = System.nanoTime();
			openIteratorTimer.record(end - start, TimeUnit.NANOSECONDS);
			throw ex;
		}
	}

	@Override
	public void closeIterator(long iteratorId) throws it.cavallium.rockserver.core.common.RocksDBException {
		var start = System.nanoTime();
		ops.beginOp();
		try {
			// Should close the iterator operation
			throw new UnsupportedOperationException();
		} finally {
			ops.endOp();
			var end = System.nanoTime();
			closeIteratorTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	@Override
	public void seekTo(long iterationId, @NotNull Keys keys)
			throws it.cavallium.rockserver.core.common.RocksDBException {
		var start = System.nanoTime();
		ops.beginOp();
		try {
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
			@NotNull RequestType.RequestIterate<? super Buf, T> requestType) throws it.cavallium.rockserver.core.common.RocksDBException {
		var start = System.nanoTime();
		ops.beginOp();
		try {
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
							 RequestType.@NotNull RequestReduceRange<? super KV, T> requestType,
							 long timeoutMs) throws it.cavallium.rockserver.core.common.RocksDBException {
		var start = System.nanoTime();
		ops.beginOp();
		try {
			var col = getColumn(columnId);

			if (requestType instanceof RequestType.RequestGetFirstAndLast<?>
					|| requestType instanceof RequestType.RequestEntriesCount<?>) {
				if (col.hasBuckets()) {
					throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.UNSUPPORTED_COLUMN_TYPE,
							"Can't execute this request type on a column with buckets");
				}
			}

			try (var ro = newReadOptions()) {
				Buf calculatedStartKey = startKeysInclusive != null && startKeysInclusive.keys().length > 0 ? col.calculateKey(startKeysInclusive.keys()) : null;
				Buf calculatedEndKey = endKeysExclusive != null && endKeysExclusive.keys().length > 0 ? col.calculateKey(endKeysExclusive.keys()) : null;
				try (var startKeySlice = calculatedStartKey != null ? toSlice(calculatedStartKey) : null;
					 var endKeySlice = calculatedEndKey != null ? toSlice(calculatedEndKey) : null) {
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
								if (calculatedStartKey != null || calculatedEndKey != null || path == null) {
									long count = 0;
									it.seekToFirst();
									while (it.isValid()) {
										count++;
										it.next();
									}
									yield count;
								} else {
									Map<String, TableProperties> props ;
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
								var calculatedValue = col.schema().hasValue() ? toBuf(it.value()) : emptyBuf();
								var first = decodeKVNoBuckets(col, calculatedKey, calculatedValue);

								if (!reverse) {
									it.seekToLast();
								} else {
									it.seekToFirst();
								}

								calculatedKey = toBuf(it.key());
								calculatedValue = col.schema().hasValue() ? toBuf(it.value()) : emptyBuf();
								var last = decodeKVNoBuckets(col, calculatedKey, calculatedValue);
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
			long timeoutMs) throws it.cavallium.rockserver.core.common.RocksDBException {
		return Flux
				.from(this.getRangeAsyncInternal(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs))
				.toStream();
	}

	/** See: {@link it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream.GetRange}. */
	public <T> Publisher<T> getRangeAsyncInternal(long transactionId,
										   long columnId,
										   @Nullable Keys startKeysInclusive,
										   @Nullable Keys endKeysExclusive,
										   boolean reverse,
										   RequestType.RequestGetRange<? super KV, T> requestType,
										   long timeoutMs) throws it.cavallium.rockserver.core.common.RocksDBException {
		LongAdder totalTime =  new LongAdder();
		record Resources(ColumnInstance col, ReadOptions ro, AbstractSlice<?> startKeySlice,
						 AbstractSlice<?> endKeySlice, RocksIterator it) {
			public void close() {
				ro.close();
				if (startKeySlice != null) startKeySlice.close();
				if (endKeySlice != null) endKeySlice.close();
				it.close();
			}
		}
		return Flux.using(() -> {
			var initializationStartTime = System.nanoTime();
			var col = getColumn(columnId);

			if (requestType instanceof RequestType.RequestGetAllInRange<?>) {
				if (col.hasBuckets()) {
					throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.UNSUPPORTED_COLUMN_TYPE,
							"Can't get the range elements of a column with buckets");
				}
			}

			var ro = newReadOptions();
			try {
				Buf calculatedStartKey = startKeysInclusive != null && startKeysInclusive.keys().length > 0 ? col.calculateKey(startKeysInclusive.keys()) : null;
				Buf calculatedEndKey = endKeysExclusive != null && endKeysExclusive.keys().length > 0 ? col.calculateKey(endKeysExclusive.keys()) : null;
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
						return new Resources(col, ro, startKeySlice, endKeySlice, it);
					} catch (Throwable ex) {
						if (endKeySlice != null) endKeySlice.close();
						throw ex;
					}
				} catch (Throwable ex) {
					if (startKeySlice != null) startKeySlice.close();
					throw ex;
				}
			} catch (Throwable ex) {
				ro.close();
				throw ex;
			} finally {
				totalTime.add(System.nanoTime() - initializationStartTime);
			}
		}, res -> Flux.<T, RocksIterator>generate(() -> {
			var seekStartTime = System.nanoTime();
			try {
				if (!reverse) {
					res.it.seekToFirst();
				} else {
					res.it.seekToLast();
				}
				return res.it;
			} finally {
				totalTime.add(System.nanoTime() - seekStartTime);
			}
		}, (it, sink) -> {
			var nextTime = System.nanoTime();
			try {
				if (!it.isValid()) {
					sink.complete();
				} else {
					var calculatedKey = toBuf(it.key());
					var calculatedValue = res.col.schema().hasValue() ? toBuf(it.value()) : emptyBuf();
					if (!reverse) {
						res.it.next();
					} else {
						res.it.prev();
					}
					var kv = decodeKVNoBuckets(res.col, calculatedKey, calculatedValue);

					//noinspection unchecked
					sink.next((T) kv);
				}
				return it;
			} finally {
				totalTime.add(System.nanoTime() - nextTime);
			}
		}), resources -> {
			var closeTime = System.nanoTime();
			try {
				resources.close();
			} finally {
				totalTime.add(System.nanoTime() - closeTime);
			}
		})
		.subscribeOn(scheduler.read())
		.doFirst(ops::beginOp)
		.doFinally(_ -> {
			ops.endOp();
			getRangeTimer.record(totalTime.sum(), TimeUnit.NANOSECONDS);
		});
	}

	private Buf dbGet(Tx tx,
								ColumnInstance col, ReadOptions readOptions,
								Buf calculatedKey) throws org.rocksdb.RocksDBException {
		if (tx != null) {
			byte[] previousRawBucketByteArray;
			if (tx.isFromGetForUpdate()) {
				previousRawBucketByteArray = tx.val().getForUpdate(readOptions, col.cfh(), calculatedKey.toByteArray(), true);
			} else {
				previousRawBucketByteArray = tx.val().get(readOptions, col.cfh(), calculatedKey.toByteArray());
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
		var keyMayExist = this.db.get().keyMayExist(cfh, readOptions, calculatedKey.getBackingByteArray(), calculatedKey.getBackingByteArrayOffset(), calculatedKey.getBackingByteArrayLength(), valueHolder);
		if (keyMayExist) {
			var value = valueHolder.getValue();
			if (value != null) {
				return Buf.wrap(value);
			} else {
				return toBuf(this.db.get().get(cfh, readOptions, calculatedKey.getBackingByteArray(), calculatedKey.getBackingByteArrayOffset(), calculatedKey.getBackingByteArrayLength()));
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
			throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.COLUMN_NOT_FOUND,
					"No column with id " + columnId);
		}
	}

	private Tx getTransaction(long transactionId, boolean allowGetForUpdate) {
		var tx = txs.get(transactionId);
		if (tx != null) {
			if (!allowGetForUpdate && tx.isFromGetForUpdate()) {
				throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.RESTRICTED_TRANSACTION,
						"Can't get this transaction, it's for internal use only");
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
	public DatabaseConfig getConfig() {
		return config;
	}

	private AbstractSlice<?> toDirectSlice(Buf calculatedKey) {
		return new DirectSlice(calculatedKey.asHeapByteBuffer(), calculatedKey.size());
	}

	private AbstractSlice<?> toSlice(Buf calculatedKey) {
		return new Slice(calculatedKey.asArray());
	}

	private KV decodeKVNoBuckets(ColumnInstance col, Buf calculatedKey, Buf calculatedValue) {
		var keys = col.decodeKeys(calculatedKey, calculatedValue);
		return new KV(new Keys(keys), calculatedValue);
	}

	private KV decodeKV(ColumnInstance col, Buf calculatedKey, Buf calculatedValue) {
		var keys = col.decodeKeys(calculatedKey, calculatedValue);
		// todo: implement
		throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED,
				"Bucket column type not implemented, implement them");
	}

	@Override
	public RWScheduler getScheduler() {
		return scheduler;
	}
}
