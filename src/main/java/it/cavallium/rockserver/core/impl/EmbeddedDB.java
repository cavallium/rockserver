package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.common.Utils.toMemorySegment;
import static it.cavallium.rockserver.core.impl.ColumnInstance.BIG_ENDIAN_BYTES;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kExistsWithValue;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kExistsWithoutValue;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RequestType.RequestEntriesCount;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.cavallium.rockserver.core.impl.rocksdb.TransactionalDB.TransactionalOptions;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.rocksdb.*;
import org.rocksdb.Status.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class EmbeddedDB implements RocksDBSyncAPI, Closeable {

	private static final int INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES = 4096;
	public static final long MAX_TRANSACTION_DURATION_MS = 10_000L;
	private static final boolean USE_FAST_GET = true;
	private static final byte[] COLUMN_SCHEMAS_COLUMN = "_column_schemas_".getBytes(StandardCharsets.UTF_8);
	private static final KV NO_MORE_RESULTS = new KV(new Keys(), null);
	private final Logger logger;
	private final @Nullable Path path;
	private final TransactionalDB db;
	private final DBOptions dbOptions;
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
	private Path tempSSTsPath;

	public EmbeddedDB(@Nullable Path path, String name, @Nullable Path embeddedConfigPath) throws IOException {
		this.path = path;
		this.logger = LoggerFactory.getLogger("db." + name);
		this.columns = new NonBlockingHashMapLong<>();
		this.txs = new NonBlockingHashMapLong<>();
		this.its = new NonBlockingHashMapLong<>();
		this.columnNamesIndex = new ConcurrentHashMap<>();
		this.ops = new SafeShutdown();
		DatabaseConfig config = ConfigParser.parse(embeddedConfigPath);
		this.config = config;
		var loadedDb = RocksDBLoader.load(path, config, logger);
		this.db = loadedDb.db();
		this.dbOptions = loadedDb.dbOptions();
		this.refs = loadedDb.refs();
		this.cache = loadedDb.cache();
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
		} catch (TimeoutException e) {
			logger.error("Some operations lasted more than 10 seconds, forcing database shutdown...");
		}
	}

	@Override
	public long openTransaction(long timeoutMs) {
		return allocateTransactionInternal(openTransactionInternal(timeoutMs, false));
	}

	private long allocateTransactionInternal(Tx tx) {
		return FastRandomUtils.allocateNewValue(txs, tx, Long.MIN_VALUE, -2);
	}

	private Tx openTransactionInternal(long timeoutMs, boolean isFromGetForUpdate) {
		// Open the transaction operation, do not close until the transaction has been closed
		ops.beginOp();
		try {
			TransactionalOptions txOpts = db.createTransactionalOptions(timeoutMs);
			var writeOpts = new WriteOptions();
			return new Tx(db.beginTransaction(writeOpts, txOpts), isFromGetForUpdate, new RocksDBObjects(writeOpts, txOpts));
		} catch (Throwable ex) {
			ops.endOp();
			throw ex;
		}
	}

	@Override
	public boolean closeTransaction(long transactionId, boolean commit) {
		var tx = txs.get(transactionId);
		if (tx != null) {
			try {
				var committed = closeTransaction(tx, commit);
				if (committed) {
					txs.remove(transactionId, tx);
				}
				return committed;
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

	private boolean closeTransaction(@NotNull Tx tx, boolean commit) {
		ops.beginOp();
		try {
			// Transaction found
			try {
				if (commit) {
					if (!commitTxOptimistically(tx)) {
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
			}
		} finally {
			ops.endOp();
		}
	}

	@Override
	public void closeFailedUpdate(long updateId) throws it.cavallium.rockserver.core.common.RocksDBException {
		this.closeTransaction(updateId, false);
	}

	private boolean commitTxOptimistically(@NotNull Tx tx) throws org.rocksdb.RocksDBException {
		try {
			tx.val().commit();
			return true;
		} catch (org.rocksdb.RocksDBException ex) {
			var status = ex.getStatus() != null ? ex.getStatus().getCode() : Code.Ok;
			if (status == Code.Busy || status == Code.TryAgain) {
				return false;
			}
			throw ex;
		}
	}

	@Override
	public long createColumn(String name, @NotNull ColumnSchema schema) throws it.cavallium.rockserver.core.common.RocksDBException {
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
		}
	}

	@Override
	public void deleteColumn(long columnId) throws it.cavallium.rockserver.core.common.RocksDBException {
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
		}
	}

	@Override
	public long getColumnId(@NotNull String name) {
		var columnId = getColumnIdOrNull(name);
		if (columnId == null) {
			throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.COLUMN_NOT_FOUND,
					"Column not found: " + name);
		} else {
			return columnId;
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
	public <T> T put(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull MemorySegment value,
			RequestPut<? super MemorySegment, T> requestType) throws it.cavallium.rockserver.core.common.RocksDBException {
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
			return put(arena, tx, col, updateId, keys, value, requestType);
		} catch (it.cavallium.rockserver.core.common.RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			ops.endOp();
		}
	}

	@Override
	public <T> List<T> putMulti(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull MemorySegment> values,
			RequestPut<? super MemorySegment, T> requestType) throws it.cavallium.rockserver.core.common.RocksDBException {
		if (keys.size() != values.size()) {
			throw new IllegalArgumentException("keys length is different than values length: " + keys.size() + " != " + values.size());
		}
		List<T> responses = requestType instanceof RequestType.RequestNothing<?> ? null : new ArrayList<>(keys.size());
		for (int i = 0; i < keys.size(); i++) {
			var result = put(arena, transactionOrUpdateId, columnId, keys.get(i), values.get(i), requestType);
			if (responses != null) {
				responses.add(result);
			}
		}
		return responses != null ? responses : List.of();
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
								var wb = new WB(db.get(), new WriteBatch(), mode == PutBatchMode.WRITE_BATCH_NO_WAL);
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
					try (var arena = Arena.ofConfined()) {
						while (keyIt.hasNext()) {
							var key = keyIt.next();
							var value = valueIt.next();
							put(arena, writer, col, 0, key, value, RequestType.none());
						}
					} catch (it.cavallium.rockserver.core.common.RocksDBException ex) {
						doFinally();
						throw ex;
					} catch (Exception ex) {
						doFinally();
						throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
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
		try {
			putBatchInternal(columnId, batchPublisher, mode).get();
        } catch (it.cavallium.rockserver.core.common.RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		}
	}

	/**
	 * @param txConsumer this can be called multiple times, if the optimistic transaction failed
	 */
	public <T extends DBWriter, R> R wrapWithTransactionIfNeeded(@Nullable T tx, boolean needTransaction,
																 ExFunction<@Nullable T, R> txConsumer) throws Exception {
		if (needTransaction) {
			return ensureWrapWithTransaction(tx, txConsumer);
		} else {
			return txConsumer.apply(tx);
		}
	}


	/**
	 * @param txConsumer this can be called multiple times, if the optimistic transaction failed
	 */
	public <T extends DBWriter, R> R ensureWrapWithTransaction(@Nullable T tx,
															   ExFunction<@NotNull T, R> txConsumer) throws Exception {
		R result;
		if (tx == null) {
			// Retry using a transaction: transactions are required to handle this kind of data
			var newTx = this.openTransactionInternal(Long.MAX_VALUE, false);
			try {
				boolean committed;
				do {
                    //noinspection unchecked
                    result = txConsumer.apply((T) newTx);
					committed = this.closeTransaction(newTx, true);
					if (!committed) {
						Thread.yield();
					}
				} while (!committed);
			} finally {
				this.closeTransaction(newTx, false);
			}
		} else {
			result = txConsumer.apply(tx);
		}
		return result;
	}

	private <U> U put(Arena arena,
			@Nullable DBWriter optionalDbWriter,
			ColumnInstance col,
			long updateId,
			@NotNull Keys keys,
			@NotNull MemorySegment value,
			RequestPut<? super MemorySegment, U> callback) throws it.cavallium.rockserver.core.common.RocksDBException {
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
			return wrapWithTransactionIfNeeded(optionalDbWriter, needsTx, dbWriter -> {
				MemorySegment previousValue;
				MemorySegment calculatedKey = col.calculateKey(arena, keys.keys());
				if (updateId != 0L) {
					assert dbWriter instanceof Tx;
					((Tx) dbWriter).val().setSavePoint();
				}
				if (col.hasBuckets()) {
					assert dbWriter instanceof Tx;
					var bucketElementKeys = col.getBucketElementKeys(keys.keys());
					try (var readOptions = new ReadOptions()) {
						var previousRawBucketByteArray = ((Tx) dbWriter).val().getForUpdate(readOptions, col.cfh(), calculatedKey.toArray(BIG_ENDIAN_BYTES), true);
						MemorySegment previousRawBucket = toMemorySegment(arena, previousRawBucketByteArray);
						var bucket = previousRawBucket != null ? new Bucket(col, previousRawBucket) : new Bucket(col);
						previousValue = transformResultValue(col, bucket.addElement(bucketElementKeys, value));
						var k = Utils.toByteArray(calculatedKey);
						var v = Utils.toByteArray(bucket.toSegment(arena));
						((Tx) dbWriter).val().put(col.cfh(), k, v);
					} catch (org.rocksdb.RocksDBException e) {
						throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_1, e);
					}
				} else {
					if (RequestType.requiresGettingPreviousValue(callback)) {
						assert dbWriter instanceof Tx;
						try (var readOptions = new ReadOptions()) {
							byte[] previousValueByteArray;
							previousValueByteArray = ((Tx) dbWriter).val().getForUpdate(readOptions, col.cfh(), calculatedKey.toArray(BIG_ENDIAN_BYTES), true);
							previousValue = transformResultValue(col, toMemorySegment(arena, previousValueByteArray));
						} catch (org.rocksdb.RocksDBException e) {
							throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_2, e);
						}
					} else if (RequestType.requiresGettingPreviousPresence(callback)) {
						// todo: in the future this should be replaced with just keyExists
						assert dbWriter instanceof Tx;
						try (var readOptions = new ReadOptions()) {
							byte[] previousValueByteArray;
							previousValueByteArray = ((Tx) dbWriter).val().getForUpdate(readOptions, col.cfh(), calculatedKey.toArray(BIG_ENDIAN_BYTES), true);
							previousValue = previousValueByteArray != null ? MemorySegment.NULL : null;
						} catch (org.rocksdb.RocksDBException e) {
							throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_2, e);
						}
					} else {
						previousValue = null;
					}
					switch (dbWriter) {
						case WB wb -> wb.wb().put(col.cfh(), Utils.toByteArray(calculatedKey), Utils.toByteArray(value));
						case SSTWriter sstWriter -> {
							var keyBB = calculatedKey.asByteBuffer();
							ByteBuffer valueBB = (col.schema().hasValue() ? value : Utils.dummyEmptyValue()).asByteBuffer();
							sstWriter.put(keyBB, valueBB);
						}
						case Tx t -> t.val().put(col.cfh(), Utils.toByteArray(calculatedKey), Utils.toByteArray(value));
						case null -> {
							try (var w = new WriteOptions()) {
								var keyBB = calculatedKey.asByteBuffer();
								ByteBuffer valueBB = (col.schema().hasValue() ? value : Utils.dummyEmptyValue()).asByteBuffer();
								db.get().put(col.cfh(), w, keyBB, valueBB);
							}
						}
					}
				}
				U result = RequestType.safeCast(switch (callback) {
					case RequestType.RequestNothing<?> ignored -> null;
					case RequestType.RequestPrevious<?> ignored -> previousValue;
					case RequestType.RequestPreviousPresence<?> ignored -> previousValue != null;
					case RequestType.RequestChanged<?> ignored -> !Utils.valueEquals(previousValue, value);
					case RequestType.RequestDelta<?> ignored -> new Delta<>(previousValue, value);
				});

				if (updateId != 0L) {
					if (!closeTransaction(updateId, true)) {
						((Tx) dbWriter).val().rollbackToSavePoint();
						((Tx) dbWriter).val().undoGetForUpdate(col.cfh(), Utils.toByteArray(calculatedKey));
						throw new RocksDBRetryException();
					}
				}

				return result;
			});
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

	private MemorySegment transformResultValue(ColumnInstance col, MemorySegment realPreviousValue) {
		return col.schema().hasValue() ? realPreviousValue : (realPreviousValue != null ? MemorySegment.NULL : null);
	}

	@Override
	public <T> T get(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			Keys keys,
			RequestGet<? super MemorySegment, T> requestType) throws it.cavallium.rockserver.core.common.RocksDBException {
		// Column id
		var col = getColumn(columnId);
		Tx tx = transactionOrUpdateId != 0 ? getTransaction(transactionOrUpdateId, true) : null;
		long updateId;
		if (requestType instanceof RequestType.RequestForUpdate<?>) {
			if (tx == null) {
				tx = openTransactionInternal(MAX_TRANSACTION_DURATION_MS, true);
				updateId = allocateTransactionInternal(tx);
			} else {
				updateId = transactionOrUpdateId;
			}
		} else {
			updateId = 0;
		}

		try {
			return get(arena, tx, updateId, col, keys, requestType);
		} catch (Throwable ex) {
			if (updateId != 0 && tx.isFromGetForUpdate()) {
				closeTransaction(updateId, false);
			}
			throw ex;
		}
	}

	private <T> T get(Arena arena,
			Tx tx,
			long updateId,
			ColumnInstance col,
			Keys keys,
			RequestGet<? super MemorySegment, T> callback) throws it.cavallium.rockserver.core.common.RocksDBException {
		ops.beginOp();
		try {
			if (!col.schema().hasValue() && RequestType.requiresGettingCurrentValue(callback)) {
				throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.VALUE_MUST_BE_NULL,
						"The specified callback requires a return value, but this column does not have values!");
			}
			MemorySegment foundValue;
			boolean existsValue;

			MemorySegment calculatedKey = col.calculateKey(arena, keys.keys());
			if (col.hasBuckets()) {
				var bucketElementKeys = col.getBucketElementKeys(keys.keys());
				try (var readOptions = new ReadOptions()) {
					MemorySegment previousRawBucket = dbGet(tx, col, arena, readOptions, calculatedKey);
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
					try (var readOptions = new ReadOptions()) {
						foundValue = dbGet(tx, col, arena, readOptions, calculatedKey);
						existsValue = foundValue != null;
					} catch (org.rocksdb.RocksDBException e) {
						throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_2, e);
					}
				} else if (callback instanceof RequestType.RequestExists<?>) {
					// tx is always null here
					//noinspection ConstantValue
					assert tx == null;
					foundValue = null;
					existsValue = db.get().keyExists(col.cfh(), calculatedKey.asByteBuffer());
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
	public long openIterator(Arena arena,
			long transactionId,
			long columnId,
			Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws it.cavallium.rockserver.core.common.RocksDBException {
		// Open an operation that ends when the iterator is closed
		ops.beginOp();
		try {
			var col = getColumn(columnId);
			RocksIterator it;
			var ro = new ReadOptions();
			if (transactionId > 0L) {
				//noinspection resource
				it = getTransaction(transactionId, false).val().getIterator(ro, col.cfh());
			} else {
				it = db.get().newIterator(col.cfh(), ro);
			}
			var itEntry = new REntry<>(it, new RocksDBObjects(ro));
            return FastRandomUtils.allocateNewValue(its, itEntry, 1, Long.MAX_VALUE);
		} catch (Throwable ex) {
			ops.endOp();
			throw ex;
		}
	}

	@Override
	public void closeIterator(long iteratorId) throws it.cavallium.rockserver.core.common.RocksDBException {
		ops.beginOp();
		try {
			// Should close the iterator operation
			throw new UnsupportedOperationException();
		} finally {
			ops.endOp();
		}
	}

	@Override
	public void seekTo(Arena arena, long iterationId, @NotNull Keys keys)
			throws it.cavallium.rockserver.core.common.RocksDBException {
		ops.beginOp();
		try {
			throw new UnsupportedOperationException();
		} finally {
			ops.endOp();
		}
	}

	@Override
	public <T> T subsequent(Arena arena,
			long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super MemorySegment, T> requestType) throws it.cavallium.rockserver.core.common.RocksDBException {
		ops.beginOp();
		try {
			throw new UnsupportedOperationException();
		} finally {
			ops.endOp();
		}
	}

	@SuppressWarnings("unchecked")
    @Override
	public <T> T reduceRange(Arena arena,
							 long transactionId,
							 long columnId,
							 @Nullable Keys startKeysInclusive,
							 @Nullable Keys endKeysExclusive,
							 boolean reverse,
							 RequestType.@NotNull RequestReduceRange<? super KV, T> requestType,
							 long timeoutMs) throws it.cavallium.rockserver.core.common.RocksDBException {
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

			try (var ro = new ReadOptions()) {
				MemorySegment calculatedStartKey = startKeysInclusive != null && startKeysInclusive.keys().length > 0 ? col.calculateKey(arena, startKeysInclusive.keys()) : null;
				MemorySegment calculatedEndKey = endKeysExclusive != null && endKeysExclusive.keys().length > 0 ? col.calculateKey(arena, endKeysExclusive.keys()) : null;
				try (var startKeySlice = calculatedStartKey != null ? toDirectSlice(calculatedStartKey) : null;
					 var endKeySlice = calculatedEndKey != null ? toDirectSlice(calculatedEndKey) : null) {
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
								var calculatedKey = toMemorySegment(arena, it.key());
								var calculatedValue = col.schema().hasValue() ? toMemorySegment(it.value()) : MemorySegment.NULL;
								var first = decodeKVNoBuckets(arena, col, calculatedKey, calculatedValue);

								if (!reverse) {
									it.seekToLast();
								} else {
									it.seekToFirst();
								}

								calculatedKey = toMemorySegment(arena, it.key());
								calculatedValue = col.schema().hasValue() ? toMemorySegment(it.value()) : MemorySegment.NULL;
								var last = decodeKVNoBuckets(arena, col, calculatedKey, calculatedValue);
								yield new FirstAndLast<>(first, last);
							}
						};
					}
				}
			}
		} finally {
			ops.endOp();
		}
	}

	@Override
	public <T> Stream<T> getRange(Arena arena, long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.@NotNull RequestGetRange<? super KV, T> requestType, long timeoutMs) throws it.cavallium.rockserver.core.common.RocksDBException {
		return Flux
				.from(this.getRangeAsyncInternal(arena, transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs))
				.toStream();
	}

	/** See: {@link it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream.GetRange}. */
	public <T> Publisher<T> getRangeAsyncInternal(Arena arena,
										   long transactionId,
										   long columnId,
										   @Nullable Keys startKeysInclusive,
										   @Nullable Keys endKeysExclusive,
										   boolean reverse,
										   RequestType.RequestGetRange<? super KV, T> requestType,
										   long timeoutMs) throws it.cavallium.rockserver.core.common.RocksDBException {
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
			var col = getColumn(columnId);

			if (requestType instanceof RequestType.RequestGetAllInRange<?>) {
				if (col.hasBuckets()) {
					throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.UNSUPPORTED_COLUMN_TYPE,
							"Can't get the range elements of a column with buckets");
				}
			}

			var ro = new ReadOptions();
			try {
				MemorySegment calculatedStartKey = startKeysInclusive != null && startKeysInclusive.keys().length > 0 ? col.calculateKey(arena, startKeysInclusive.keys()) : null;
				MemorySegment calculatedEndKey = endKeysExclusive != null && endKeysExclusive.keys().length > 0 ? col.calculateKey(arena, endKeysExclusive.keys()) : null;
				var startKeySlice = calculatedStartKey != null ? toDirectSlice(calculatedStartKey) : null;
				try {
					var endKeySlice = calculatedEndKey != null ? toDirectSlice(calculatedEndKey) : null;
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
			}
		}, res -> Flux.<T, RocksIterator>generate(() -> {
            if (!reverse) {
                res.it.seekToFirst();
            } else {
                res.it.seekToLast();
            }
            return res.it;
        }, (it, sink) -> {
            if (!it.isValid()) {
                sink.complete();
            } else {
                var calculatedKey = toMemorySegment(arena, it.key());
                var calculatedValue = res.col.schema().hasValue() ? toMemorySegment(it.value()) : MemorySegment.NULL;
				//noinspection unchecked
				sink.next((T) decodeKVNoBuckets(arena, res.col, calculatedKey, calculatedValue));
				if (!reverse) {
					res.it.next();
				} else {
					res.it.prev();
				}
            }
            return it;
        }), Resources::close)
				.subscribeOn(Schedulers.boundedElastic())
				.doFirst(ops::beginOp)
				.doFinally(_ -> ops.endOp());
	}

	private MemorySegment dbGet(Tx tx,
								ColumnInstance col,
								Arena arena,
								ReadOptions readOptions,
								MemorySegment calculatedKey) throws org.rocksdb.RocksDBException {
		if (tx != null) {
			byte[] previousRawBucketByteArray;
			if (tx.isFromGetForUpdate()) {
				previousRawBucketByteArray = tx.val().getForUpdate(readOptions, col.cfh(), calculatedKey.toArray(BIG_ENDIAN_BYTES), true);
			} else {
				previousRawBucketByteArray = tx.val().get(readOptions, col.cfh(), calculatedKey.toArray(BIG_ENDIAN_BYTES));
			}
			return toMemorySegment(arena, previousRawBucketByteArray);
		} else {
			var db = this.db.get();
			if (USE_FAST_GET) {
				return dbGetDirect(arena, col.cfh(), readOptions, calculatedKey);
			} else {
				var previousRawBucketByteArray = db.get(col.cfh(), readOptions, calculatedKey.toArray(BIG_ENDIAN_BYTES));
				return toMemorySegment(arena, previousRawBucketByteArray);
			}
		}
	}

	@Nullable
	private MemorySegment dbGetDirect(Arena arena, ColumnFamilyHandle cfh, ReadOptions readOptions, MemorySegment calculatedKey)
			throws org.rocksdb.RocksDBException {
		// Get the key nio buffer to pass to RocksDB
		ByteBuffer keyNioBuffer = calculatedKey.asByteBuffer();

		// Create a direct result buffer because RocksDB works only with direct buffers
		var resultBuffer = arena.allocate(INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES).asByteBuffer();
		var keyMayExist = this.db.get().keyMayExist(cfh, readOptions, keyNioBuffer.rewind(), resultBuffer.clear());
		return switch (keyMayExist.exists) {
			case kNotExist -> null;
			case kExistsWithValue, kExistsWithoutValue -> {
				// At the beginning, size reflects the expected size, then it becomes the real data size
				int size = keyMayExist.exists == kExistsWithValue ? keyMayExist.valueLength : -1;
				if (keyMayExist.exists == kExistsWithoutValue || size > resultBuffer.limit()) {
					if (size > resultBuffer.capacity()) {
						resultBuffer = arena.allocate(size).asByteBuffer();
					}
					size = this.db.get().get(cfh, readOptions, keyNioBuffer.rewind(), resultBuffer.clear());
				}

				if (size == RocksDB.NOT_FOUND) {
					yield null;
				} else if (size == resultBuffer.limit()) {
					yield MemorySegment.ofBuffer(resultBuffer);
				} else {
					throw new IllegalStateException("size (" + size + ") != read size (" + resultBuffer.limit() + ")");
				}
			}
		};
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
			throw new NoSuchElementException("No transaction with id " + transactionId);
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

	private AbstractSlice<?> toDirectSlice(MemorySegment calculatedKey) {
		return new DirectSlice(calculatedKey.asByteBuffer(), (int) calculatedKey.byteSize());
	}

	private KV decodeKVNoBuckets(Arena arena, ColumnInstance col, MemorySegment calculatedKey, MemorySegment calculatedValue) {
		var keys = col.decodeKeys(arena, calculatedKey, calculatedValue);
		return new KV(new Keys(keys), calculatedValue);
	}

	private KV decodeKV(Arena arena, ColumnInstance col, MemorySegment calculatedKey, MemorySegment calculatedValue) {
		var keys = col.decodeKeys(arena, calculatedKey, calculatedValue);
		// todo: implement
		throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED,
				"Bucket column type not implemented, implement them");
	}
}
