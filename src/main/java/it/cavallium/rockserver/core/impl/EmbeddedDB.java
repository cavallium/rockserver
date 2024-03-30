package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.common.Utils.toMemorySegment;
import static it.cavallium.rockserver.core.impl.ColumnInstance.BIG_ENDIAN_BYTES;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kExistsWithValue;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kExistsWithoutValue;

import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestNothing;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Delta;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.UpdateContext;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.config.ConfigPrinter;
import it.cavallium.rockserver.core.config.DatabaseConfig;
import it.cavallium.rockserver.core.impl.rocksdb.REntry;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBObjects;
import it.cavallium.rockserver.core.impl.rocksdb.TransactionalDB;
import it.cavallium.rockserver.core.impl.rocksdb.TransactionalDB.TransactionalOptions;
import it.cavallium.rockserver.core.impl.rocksdb.Tx;
import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Status.Code;
import org.rocksdb.WriteOptions;

public class EmbeddedDB implements RocksDBSyncAPI, Closeable {

	private static final int INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES = 4096;
	public static final long MAX_TRANSACTION_DURATION_MS = 10_000L;
	private static final boolean USE_FAST_GET = true;
	private final Logger logger;
	private final @Nullable Path path;
	private final TransactionalDB db;
	private final NonBlockingHashMapLong<ColumnInstance> columns;
	private final ConcurrentMap<String, Long> columnNamesIndex;
	private final NonBlockingHashMapLong<Tx> txs;
	private final NonBlockingHashMapLong<REntry<RocksIterator>> its;
	private final SafeShutdown ops;
	private final Object columnEditLock = new Object();

	public EmbeddedDB(@Nullable Path path, String name, @Nullable Path embeddedConfigPath) {
		this.path = path;
		this.logger = Logger.getLogger("db." + name);
		this.columns = new NonBlockingHashMapLong<>();
		this.txs = new NonBlockingHashMapLong<>();
		this.its = new NonBlockingHashMapLong<>();
		this.columnNamesIndex = new ConcurrentHashMap<>();
		this.ops = new SafeShutdown();
		DatabaseConfig config = ConfigParser.parse(embeddedConfigPath);
		this.db = RocksDBLoader.load(path, config, logger);
		if (Boolean.parseBoolean(System.getProperty("rockserver.core.print-config", "true"))) {
			logger.log(Level.INFO, "Database configuration: {0}", ConfigPrinter.stringify(config));
		}
	}

	/**
	 * The column must be registered once!!!
	 * Do not try to register a column that may already be registered
	 */
	private long registerColumn(@NotNull ColumnInstance column) {
		try {
			var columnName = new String(column.cfh().getName(), StandardCharsets.UTF_8);
			long id = FastRandomUtils.allocateNewValue(this.columns, column, 1, Long.MAX_VALUE);
			Long previous = this.columnNamesIndex.putIfAbsent(columnName, id);
			if (previous != null) {
				//noinspection resource
				this.columns.remove(id);
				throw new UnsupportedOperationException("Column already registered!");
			}
			return id;
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * The column must be unregistered once!!!
	 * Do not try to unregister a column that may already be unregistered, or that may not be registered
	 */
	private ColumnInstance unregisterColumn(long id) {
		var col = this.columns.remove(id);
		Objects.requireNonNull(col, () -> "Column does not exist: " + id);
		String name;
		try {
			name = new String(col.cfh().getName(), StandardCharsets.UTF_8);
		} catch (RocksDBException e) {
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
		return col;
	}

	@Override
	public void close() throws IOException {
		// Wait for 10 seconds
		try {
			ops.closeAndWait(MAX_TRANSACTION_DURATION_MS);
			if (path == null) {
				Utils.deleteDirectory(db.getPath());
			}
		} catch (TimeoutException e) {
			logger.log(Level.SEVERE, "Some operations lasted more than 10 seconds, forcing database shutdown...");
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
				throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.TX_NOT_FOUND, "Transaction not found: " + transactionId);
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
			} catch (RocksDBException e) {
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

	private boolean commitTxOptimistically(@NotNull Tx tx) throws RocksDBException {
		try {
			tx.val().commit();
			return true;
		} catch (RocksDBException ex) {
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
						var cf = db.get().createColumnFamily(new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8)));
						return registerColumn(new ColumnInstance(cf, schema));
					} catch (RocksDBException e) {
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
				} catch (RocksDBException e) {
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
			@NotNull MemorySegment @NotNull [] keys,
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
			@NotNull List<@NotNull MemorySegment @NotNull []> keys,
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

	/**
	 * @param txConsumer this can be called multiple times, if the optimistic transaction failed
	 */
	public <R> R wrapWithTransactionIfNeeded(@Nullable Tx tx, boolean needTransaction,
			ExFunction<@Nullable Tx, R> txConsumer) throws Exception {
		if (needTransaction) {
			return ensureWrapWithTransaction(tx, txConsumer);
		} else {
			return txConsumer.apply(tx);
		}
	}


	/**
	 * @param txConsumer this can be called multiple times, if the optimistic transaction failed
	 */
	public <R> R ensureWrapWithTransaction(@Nullable Tx tx,
			ExFunction<@NotNull Tx, R> txConsumer) throws Exception {
		R result;
		if (tx == null) {
			// Retry using a transaction: transactions are required to handle this kind of data
			var newTx = this.openTransactionInternal(Long.MAX_VALUE, false);
			try {
				boolean committed;
				do {
					result = txConsumer.apply(newTx);
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
			@Nullable Tx optionalTxOrUpdate,
			ColumnInstance col,
			long updateId,
			@NotNull MemorySegment @NotNull[] keys,
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
			if (optionalTxOrUpdate != null && optionalTxOrUpdate.isFromGetForUpdate() && (requirePreviousValue || requirePreviousPresence)) {
				throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"You can't get the previous value or delta, when you are already updating that value");
			}
			if (updateId != 0L && optionalTxOrUpdate == null) {
				throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Update id must be accompanied with a valid transaction");
			}
			return wrapWithTransactionIfNeeded(optionalTxOrUpdate, needsTx, tx -> {
				MemorySegment previousValue;
				MemorySegment calculatedKey = col.calculateKey(arena, keys);
				if (updateId != 0L) {
					assert tx != null;
					tx.val().setSavePoint();
				}
				if (col.hasBuckets()) {
					assert tx != null;
					var bucketElementKeys = col.getBucketElementKeys(keys);
					try (var readOptions = new ReadOptions()) {
						var previousRawBucketByteArray = tx.val().getForUpdate(readOptions, col.cfh(), calculatedKey.toArray(BIG_ENDIAN_BYTES), true);
						MemorySegment previousRawBucket = toMemorySegment(arena, previousRawBucketByteArray);
						var bucket = previousRawBucket != null ? new Bucket(col, previousRawBucket) : new Bucket(col);
						previousValue = transformResultValue(col, bucket.addElement(bucketElementKeys, value));
								tx.val().put(col.cfh(), Utils.toByteArray(calculatedKey), Utils.toByteArray(bucket.toSegment(arena)));
					} catch (RocksDBException e) {
						throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_1, e);
					}
				} else {
					if (RequestType.requiresGettingPreviousValue(callback)) {
						assert tx != null;
						try (var readOptions = new ReadOptions()) {
							byte[] previousValueByteArray;
							previousValueByteArray = tx.val().getForUpdate(readOptions, col.cfh(), calculatedKey.toArray(BIG_ENDIAN_BYTES), true);
							previousValue = transformResultValue(col, toMemorySegment(arena, previousValueByteArray));
						} catch (RocksDBException e) {
							throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_2, e);
						}
					} else if (RequestType.requiresGettingPreviousPresence(callback)) {
						// todo: in the future this should be replaced with just keyExists
						assert tx != null;
						try (var readOptions = new ReadOptions()) {
							byte[] previousValueByteArray;
							previousValueByteArray = tx.val().getForUpdate(readOptions, col.cfh(), calculatedKey.toArray(BIG_ENDIAN_BYTES), true);
							previousValue = previousValueByteArray != null ? MemorySegment.NULL : null;
						} catch (RocksDBException e) {
							throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_2, e);
						}
					} else {
						previousValue = null;
					}
					if (tx != null) {
						tx.val().put(col.cfh(), Utils.toByteArray(calculatedKey), Utils.toByteArray(value));
					} else {
						try (var w = new WriteOptions()) {
							var keyBB = calculatedKey.asByteBuffer();
							ByteBuffer valueBB = (col.schema().hasValue() ? value : Utils.dummyEmptyValue()).asByteBuffer();
							db.get().put(col.cfh(), w, keyBB, valueBB);
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
						tx.val().rollbackToSavePoint();
						tx.val().undoGetForUpdate(col.cfh(), Utils.toByteArray(calculatedKey));
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
			MemorySegment @NotNull [] keys,
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
			MemorySegment @NotNull [] keys,
			RequestGet<? super MemorySegment, T> callback) throws it.cavallium.rockserver.core.common.RocksDBException {
		ops.beginOp();
		try {
			if (!col.schema().hasValue() && RequestType.requiresGettingCurrentValue(callback)) {
				throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.VALUE_MUST_BE_NULL,
						"The specified callback requires a return value, but this column does not have values!");
			}
			MemorySegment foundValue;
			boolean existsValue;

			MemorySegment calculatedKey = col.calculateKey(arena, keys);
			if (col.hasBuckets()) {
				var bucketElementKeys = col.getBucketElementKeys(keys);
				try (var readOptions = new ReadOptions()) {
					MemorySegment previousRawBucket = dbGet(tx, col, arena, readOptions, calculatedKey);
					if (previousRawBucket != null) {
						var bucket = new Bucket(col, previousRawBucket);
						foundValue = bucket.getElement(bucketElementKeys);
					} else {
						foundValue = null;
					}
					existsValue = foundValue != null;
				} catch (RocksDBException e) {
					throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.GET_1, e);
				}
			} else {
				boolean shouldGetCurrent = RequestType.requiresGettingCurrentValue(callback)
						|| (tx != null && callback instanceof RequestType.RequestExists<?>);
				if (shouldGetCurrent) {
					try (var readOptions = new ReadOptions()) {
						foundValue = dbGet(tx, col, arena, readOptions, calculatedKey);
						existsValue = foundValue != null;
					} catch (RocksDBException e) {
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
			MemorySegment @NotNull [] startKeysInclusive,
			@Nullable MemorySegment[] endKeysExclusive,
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
				it = db.get().newIterator(col.cfh());
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
	public void seekTo(Arena arena, long iterationId, @NotNull MemorySegment @NotNull [] keys)
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

	private MemorySegment dbGet(Tx tx,
			ColumnInstance col,
			Arena arena,
			ReadOptions readOptions,
			MemorySegment calculatedKey) throws RocksDBException {
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
			throws RocksDBException {
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

}
