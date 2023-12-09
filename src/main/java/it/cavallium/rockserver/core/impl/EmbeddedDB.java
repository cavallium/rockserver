package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.common.Utils.toMemorySegment;
import static it.cavallium.rockserver.core.impl.ColumnInstance.BIG_ENDIAN_BYTES;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kExistsWithValue;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kExistsWithoutValue;

import it.cavallium.rockserver.core.common.Callback;
import it.cavallium.rockserver.core.common.Callback.CallbackExists;
import it.cavallium.rockserver.core.common.Callback.GetCallback;
import it.cavallium.rockserver.core.common.Callback.IteratorCallback;
import it.cavallium.rockserver.core.common.Callback.PutCallback;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Delta;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.config.ConfigPrinter;
import it.cavallium.rockserver.core.config.DatabaseConfig;
import it.cavallium.rockserver.core.impl.rocksdb.REntry;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBObjects;
import it.cavallium.rockserver.core.impl.rocksdb.TransactionalDB;
import it.cavallium.rockserver.core.impl.rocksdb.TransactionalDB.TransactionalOptions;
import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
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
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

public class EmbeddedDB implements RocksDBAPI, Closeable {

	private static final int INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES = 4096;
	private static final boolean USE_FAST_GET = true;
	private final Logger logger;
	private final @Nullable Path path;
	private final TransactionalDB db;
	private final NonBlockingHashMapLong<ColumnInstance> columns;
	private final ConcurrentMap<String, Long> columnNamesIndex;
	private final NonBlockingHashMapLong<REntry<Transaction>> txs;
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
			ops.closeAndWait(10_000);
			if (path == null) {
				Utils.deleteDirectory(db.getPath());
			}
		} catch (TimeoutException e) {
			logger.log(Level.SEVERE, "Some operations lasted more than 10 seconds, forcing database shutdown...");
		}
	}

	@Override
	public long openTransaction(long timeoutMs) {
		return FastRandomUtils.allocateNewValue(txs, openTransactionInternal(timeoutMs), Long.MIN_VALUE, -2);
	}

	private REntry<Transaction> openTransactionInternal(long timeoutMs) {
		// Open the transaction operation, do not close until the transaction has been closed
		ops.beginOp();
		try {
			TransactionalOptions txOpts = db.createTransactionalOptions(timeoutMs);
			var writeOpts = new WriteOptions();
			return new REntry<>(db.beginTransaction(writeOpts, txOpts), new RocksDBObjects(writeOpts, txOpts));
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

	private boolean closeTransaction(@NotNull REntry<Transaction> tx, boolean commit) {
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

	private boolean commitTxOptimistically(@NotNull REntry<Transaction> tx) throws RocksDBException {
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
			long transactionId,
			long columnId,
			@NotNull MemorySegment @NotNull [] keys,
			@NotNull MemorySegment value,
			PutCallback<? super MemorySegment, T> callback) throws it.cavallium.rockserver.core.common.RocksDBException {
		ops.beginOp();
		try {
			// Column id
			var col = getColumn(columnId);
			REntry<Transaction> tx;
			if (transactionId != 0) {
				tx = getTransaction(transactionId);
			} else {
				tx = null;
			}
			return put(arena, tx, col, keys, value, callback);
		} catch (it.cavallium.rockserver.core.common.RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			ops.endOp();
		}
	}

	/**
	 * @param txConsumer this can be called multiple times, if the optimistic transaction failed
	 */
	public <R> R wrapWithTransactionIfNeeded(@Nullable REntry<Transaction> tx, boolean needTransaction,
			ExFunction<@Nullable REntry<Transaction>, R> txConsumer) throws Exception {
		if (needTransaction) {
			return ensureWrapWithTransaction(tx, txConsumer);
		} else {
			return txConsumer.apply(tx);
		}
	}


	/**
	 * @param txConsumer this can be called multiple times, if the optimistic transaction failed
	 */
	public <R> R ensureWrapWithTransaction(@Nullable REntry<Transaction> tx,
			ExFunction<@NotNull REntry<Transaction>, R> txConsumer) throws Exception {
		R result;
		if (tx == null) {
			// Retry using a transaction: transactions are required to handle this kind of data
			var newTx = this.openTransactionInternal(Long.MAX_VALUE);
			try {
				boolean committed;
				do {
					result = txConsumer.apply(newTx);
					committed = this.closeTransaction(newTx, true);
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
			@Nullable REntry<Transaction> optionalTx,
			ColumnInstance col,
			@NotNull MemorySegment @NotNull[] keys,
			@NotNull MemorySegment value,
			PutCallback<? super MemorySegment, U> callback) throws it.cavallium.rockserver.core.common.RocksDBException {
		// Check for null value
		col.checkNullableValue(value);
		try {
			boolean needsTx = col.hasBuckets()
					|| Callback.requiresGettingPreviousValue(callback)
					|| Callback.requiresGettingPreviousPresence(callback);
			return wrapWithTransactionIfNeeded(optionalTx, needsTx, tx -> {
				MemorySegment previousValue;
				MemorySegment calculatedKey = col.calculateKey(arena, keys);
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
					if (Callback.requiresGettingPreviousValue(callback)) {
						assert tx != null;
						try (var readOptions = new ReadOptions()) {
							byte[] previousValueByteArray;
							previousValueByteArray = tx.val().getForUpdate(readOptions, col.cfh(), calculatedKey.toArray(BIG_ENDIAN_BYTES), true);
							previousValue = transformResultValue(col, toMemorySegment(arena, previousValueByteArray));
						} catch (RocksDBException e) {
							throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_2, e);
						}
					} else if (Callback.requiresGettingPreviousPresence(callback)) {
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
				return Callback.safeCast(switch (callback) {
					case Callback.CallbackVoid<?> ignored -> null;
					case Callback.CallbackPrevious<?> ignored -> previousValue;
					case Callback.CallbackPreviousPresence<?> ignored -> previousValue != null;
					case Callback.CallbackChanged<?> ignored -> !Utils.valueEquals(previousValue, value);
					case Callback.CallbackDelta<?> ignored -> new Delta<>(previousValue, value);
				});
			});
		} catch (it.cavallium.rockserver.core.common.RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		}
	}

	private MemorySegment transformResultValue(ColumnInstance col, MemorySegment realPreviousValue) {
		return col.schema().hasValue() ? realPreviousValue : (realPreviousValue != null ? MemorySegment.NULL : null);
	}

	@Override
	public <T> T get(Arena arena,
			long transactionId,
			long columnId,
			MemorySegment @NotNull [] keys,
			GetCallback<? super MemorySegment, T> callback) throws it.cavallium.rockserver.core.common.RocksDBException {
		ops.beginOp();
		try {
			// Column id
			var col = getColumn(columnId);

			if (!col.schema().hasValue() && Callback.requiresGettingCurrentValue(callback)) {
				throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.VALUE_MUST_BE_NULL,
						"The specified callback requires a return value, but this column does not have values!");
			}

			MemorySegment foundValue;
			boolean existsValue;
			REntry<Transaction> tx;
			if (transactionId != 0) {
				tx = getTransaction(transactionId);
			} else {
				tx = null;
			}
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
				boolean shouldGetCurrent = Callback.requiresGettingCurrentValue(callback)
						|| (tx != null && callback instanceof Callback.CallbackExists<?>);
				if (shouldGetCurrent) {
					try (var readOptions = new ReadOptions()) {
						foundValue = dbGet(tx, col, arena, readOptions, calculatedKey);
						existsValue = foundValue != null;
					} catch (RocksDBException e) {
						throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.PUT_2, e);
					}
				} else if (callback instanceof Callback.CallbackExists<?>) {
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
			return Callback.safeCast(switch (callback) {
				case Callback.CallbackVoid<?> ignored -> null;
				case Callback.CallbackCurrent<?> ignored -> foundValue;
				case Callback.CallbackExists<?> ignored -> existsValue;
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
				it = getTransaction(transactionId).val().getIterator(ro, col.cfh());
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
			@NotNull IteratorCallback<? super MemorySegment, T> callback) throws it.cavallium.rockserver.core.common.RocksDBException {
		ops.beginOp();
		try {
			throw new UnsupportedOperationException();
		} finally {
			ops.endOp();
		}
	}

	private MemorySegment dbGet(REntry<Transaction> tx,
			ColumnInstance col,
			Arena arena,
			ReadOptions readOptions,
			MemorySegment calculatedKey) throws RocksDBException {
		if (tx != null) {
			var previousRawBucketByteArray = tx.val().get(col.cfh(), readOptions, calculatedKey.toArray(BIG_ENDIAN_BYTES));
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

	private REntry<Transaction> getTransaction(long transactionId) {
		var tx = txs.get(transactionId);
		if (tx != null) {
			return tx;
		} else {
			throw new NoSuchElementException("No transaction with id " + transactionId);
		}
	}

	public @Nullable Path getPath() {
		return path;
	}

}
