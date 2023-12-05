package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.common.Utils.toMemorySegment;
import static it.cavallium.rockserver.core.impl.ColumnInstance.BIG_ENDIAN_BYTES;
import static java.lang.foreign.MemorySegment.NULL;
import static java.util.Objects.requireNonNullElse;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kExistsWithValue;
import static org.rocksdb.KeyMayExist.KeyMayExistEnum.kExistsWithoutValue;

import it.cavallium.rockserver.core.common.Callback.GetCallback;
import it.cavallium.rockserver.core.common.Callback.IteratorCallback;
import it.cavallium.rockserver.core.common.Callback.PutCallback;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Callback;
import it.cavallium.rockserver.core.common.Delta;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.config.DatabaseConfig;
import it.cavallium.rockserver.core.impl.rocksdb.REntry;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.github.gestalt.config.builder.GestaltBuilder;
import org.github.gestalt.config.exceptions.GestaltException;
import org.github.gestalt.config.source.ClassPathConfigSource;
import org.github.gestalt.config.source.FileConfigSource;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

public class EmbeddedDB implements RocksDBAPI, Closeable {

	private static final int INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES = 4096;
	private static final boolean USE_FAST_GET = true;
	private final Logger logger;
	private final Path path;
	private final Path embeddedConfigPath;
	private final DatabaseConfig config;
	private TransactionalDB db;
	private final NonBlockingHashMapLong<ColumnInstance> columns;
	private final ConcurrentMap<String, Long> columnNamesIndex;
	private final NonBlockingHashMapLong<REntry<Transaction>> txs;
	private final SafeShutdown ops;
	private final Object columnEditLock = new Object();

	public EmbeddedDB(Path path, @Nullable Path embeddedConfigPath) {
		this.path = path;
		this.embeddedConfigPath = embeddedConfigPath;
		this.logger = Logger.getLogger("db");
		this.columns = new NonBlockingHashMapLong<>();
		this.txs = new NonBlockingHashMapLong<>();
		this.columnNamesIndex = new ConcurrentHashMap<>();
		this.ops = new SafeShutdown();

		var gsb = new GestaltBuilder();
		try {
			gsb.addSource(new ClassPathConfigSource("it/cavallium/rockserver/core/resources/default.conf"));
			if (embeddedConfigPath != null) {
				gsb.addSource(new FileConfigSource(this.embeddedConfigPath));
			}
			var gestalt = gsb
					.addDecoder(new DataSizeDecoder())
					.addDefaultConfigLoaders()
					.addDefaultDecoders()
					.build();
			gestalt.loadConfigs();

			this.config = gestalt.getConfig("database", DatabaseConfig.class);
			logger.log(Level.INFO, "Database configuration: {0}", DatabaseConfig.stringify(this.config));
		} catch (GestaltException e) {
			throw new it.cavallium.rockserver.core.common.RocksDBException(RocksDBErrorType.CONFIG_ERROR, e);
		}
	}

	/**
	 * The column must be registered once!!!
	 * Do not try to register a column that may already be registered
	 */
	private long registerColumn(ColumnInstance column) {
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
		} catch (TimeoutException e) {
			logger.log(Level.SEVERE, "Some operations lasted more than 10 seconds, forcing database shutdown...");
		}
	}

	@Override
	public long openTransaction(long timeoutMs) {
		ops.beginOp();
		TransactionalOptions txOpts = db.createTransactionalOptions();
		var writeOpts = new WriteOptions();
		var tx = new REntry<>(db.beginTransaction(writeOpts, txOpts), new RocksDBObjects(writeOpts, txOpts));
		return FastRandomUtils.allocateNewValue(txs, tx, Long.MIN_VALUE, -2);
	}

	@Override
	public void closeTransaction(long transactionId) {
		var tx = txs.remove(transactionId);
		if (tx != null) {
			try {
				tx.close();
			} finally {
				ops.endOp();
			}
		} else {
			throw new NoSuchElementException("Transaction not found: " + transactionId);
		}
	}

	@Override
	public long createColumn(String name, ColumnSchema schema) throws it.cavallium.rockserver.core.common.RocksDBException {
		synchronized (columnEditLock) {
			var colId = getColumnIdOrNull(name);
			var col = colId != null ? getColumn(colId) : null;
			if (col != null) {
				if (schema.equals(col.schema())) {
					return colId;
				} else {
					throw new it.cavallium.rockserver.core.common.RocksDBException(RocksDBErrorType.COLUMN_EXISTS,
							"Column exists, with a different schema: " + name
					);
				}
			} else {
				try {
					var cf = db.get().createColumnFamily(new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8)));
					return registerColumn(new ColumnInstance(cf, schema));
				} catch (RocksDBException e) {
					throw new it.cavallium.rockserver.core.common.RocksDBException(RocksDBErrorType.COLUMN_CREATE_FAIL, e);
				}
			}
		}
	}

	@Override
	public void deleteColumn(long columnId) throws it.cavallium.rockserver.core.common.RocksDBException {
		synchronized (columnEditLock) {
			var col = getColumn(columnId);
			try {
				db.get().dropColumnFamily(col.cfh());
				unregisterColumn(columnId).close();
			} catch (RocksDBException e) {
				throw new it.cavallium.rockserver.core.common.RocksDBException(RocksDBErrorType.COLUMN_DELETE_FAIL, e);
			}
		}
	}

	@Override
	public long getColumnId(String name) {
		var columnId = getColumnIdOrNull(name);
		if (columnId == null) {
			throw new it.cavallium.rockserver.core.common.RocksDBException(RocksDBErrorType.COLUMN_NOT_FOUND,
					"Column not found: " + name);
		} else {
			return columnId;
		}
	}

	private Long getColumnIdOrNull(String name) {
		var columnId = (long) columnNamesIndex.getOrDefault(name, -1L);
		ColumnInstance col;
		if (columnId == -1L || (col = columns.get(columnId)) == null || !col.cfh().isOwningHandle()) {
			return null;
		} else {
			return columnId;
		}
	}

	@Override
	public void put(long transactionId,
			long columnId,
			MemorySegment[] keys,
			@Nullable MemorySegment value,
			PutCallback<? super MemorySegment> callback) throws it.cavallium.rockserver.core.common.RocksDBException {
		ops.beginOp();
		try (var arena = Arena.ofConfined()) {
			// Column id
			var col = getColumn(columnId);
			// Check for null value
			col.checkNullableValue(value);

			MemorySegment previousValue;
			REntry<Transaction> tx;
			if (transactionId != 0) {
				tx = getTransaction(transactionId);
			} else {
				tx = null;
			}
			MemorySegment calculatedKey = col.calculateKey(arena, keys);
			if (col.hasBuckets()) {
				if (tx != null) {
					var bucketElementKeys = col.getBucketElementKeys(keys);
					try (var readOptions = new ReadOptions()) {
						var previousRawBucketByteArray = tx.val().get(col.cfh(), readOptions, calculatedKey.toArray(BIG_ENDIAN_BYTES));
						MemorySegment previousRawBucket = Utils.toMemorySegment(arena, previousRawBucketByteArray);
						var bucket = new Bucket(col, previousRawBucket);
						previousValue = bucket.addElement(bucketElementKeys, value);
						tx.val().put(col.cfh(), toByteArray(calculatedKey), toByteArray(bucket.toSegment(arena)));
					} catch (RocksDBException e) {
						throw new it.cavallium.rockserver.core.common.RocksDBException(RocksDBErrorType.PUT_1, e);
					}
				} else {
					// Retry using a transaction: transactions are required to handle this kind of data
					var newTransactionId = this.openTransaction(Long.MAX_VALUE);
					try {
						put(newTransactionId, columnId, keys, value, callback);
						return;
					} finally {
						this.closeTransaction(newTransactionId);
					}
				}
			} else {
				if (Callback.requiresGettingPreviousValue(callback)) {
					try (var readOptions = new ReadOptions()) {
						byte[] previousValueByteArray;
						if (tx != null) {
							previousValueByteArray = tx.val().get(col.cfh(), readOptions, calculatedKey.toArray(BIG_ENDIAN_BYTES));
						} else {
							previousValueByteArray = db.get().get(col.cfh(), readOptions, calculatedKey.toArray(BIG_ENDIAN_BYTES));
						}
						previousValue = toMemorySegment(arena, previousValueByteArray);
					} catch (RocksDBException e) {
						throw new it.cavallium.rockserver.core.common.RocksDBException(RocksDBErrorType.PUT_2, e);
					}
				} else {
					previousValue = null;
				}
				if (tx != null) {
					tx.val().put(col.cfh(), toByteArray(calculatedKey), toByteArray(requireNonNullElse(value, NULL)));
				} else {
					try (var w = new WriteOptions()) {
						db.get().put(col.cfh(), w, calculatedKey.asByteBuffer(), requireNonNullElse(value, NULL).asByteBuffer());
					}
				}
			}
			switch (callback) {
				case Callback.CallbackVoid<? super MemorySegment> ignored -> {}
				case Callback.CallbackPrevious<? super MemorySegment> c -> c.onPrevious(previousValue);
				case Callback.CallbackChanged c -> c.onChanged(valueEquals(previousValue, value));
				case Callback.CallbackDelta<? super MemorySegment> c -> c.onSuccess(new Delta<>(previousValue, value));
				default -> throw new it.cavallium.rockserver.core.common.RocksDBException(RocksDBErrorType.PUT_3,
						"Unexpected value: " + callback);
			}
		} catch (it.cavallium.rockserver.core.common.RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new it.cavallium.rockserver.core.common.RocksDBException(RocksDBErrorType.PUT_UNKNOWN, ex);
		} finally {
			ops.endOp();
		}
	}

	@Override
	public void get(long transactionId, long columnId, MemorySegment[] keys, GetCallback<? super MemorySegment> callback)
			throws it.cavallium.rockserver.core.common.RocksDBException {
		ops.beginOp();
		try (var arena = Arena.ofConfined()) {
			// Column id
			var col = getColumn(columnId);

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
					var bucket = new Bucket(col, previousRawBucket);
					foundValue = bucket.getElement(bucketElementKeys);
					existsValue = foundValue != null;
				} catch (RocksDBException e) {
					throw new it.cavallium.rockserver.core.common.RocksDBException(RocksDBErrorType.GET_1, e);
				}
			} else {
				boolean shouldGetCurrent = Callback.requiresGettingCurrentValue(callback)
						|| (tx != null && callback instanceof Callback.CallbackExists<? super MemorySegment>);
				if (shouldGetCurrent) {
					try (var readOptions = new ReadOptions()) {
						foundValue = dbGet(tx, col, arena, readOptions, calculatedKey);
						existsValue = foundValue != null;
					} catch (RocksDBException e) {
						throw new it.cavallium.rockserver.core.common.RocksDBException(RocksDBErrorType.PUT_2, e);
					}
				} else if (callback instanceof Callback.CallbackExists<? super MemorySegment>) {
					// tx is always null here
					//noinspection ConstantValue
					assert tx == null;
					foundValue = null;
					existsValue = db.get().keyExists(calculatedKey.asByteBuffer());
				} else {
					foundValue = null;
					existsValue = false;
				}
			}
			switch (callback) {
				case Callback.CallbackVoid<? super MemorySegment> ignored -> {}
				case Callback.CallbackCurrent<? super MemorySegment> c -> c.onCurrent(foundValue);
				case Callback.CallbackExists<? super MemorySegment> c -> c.onExists(existsValue);
				default -> throw new it.cavallium.rockserver.core.common.RocksDBException(RocksDBErrorType.PUT_3,
						"Unexpected value: " + callback);
			}
		} catch (it.cavallium.rockserver.core.common.RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new it.cavallium.rockserver.core.common.RocksDBException(RocksDBErrorType.PUT_UNKNOWN, ex);
		} finally {
			ops.endOp();
		}
	}

	@Override
	public long openIterator(long transactionId,
			long columnId,
			MemorySegment[] startKeysInclusive,
			@Nullable MemorySegment[] endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws it.cavallium.rockserver.core.common.RocksDBException {
		return 0;
	}

	@Override
	public void closeIterator(long iteratorId) throws it.cavallium.rockserver.core.common.RocksDBException {

	}

	@Override
	public void seekTo(long iterationId, MemorySegment[] keys)
			throws it.cavallium.rockserver.core.common.RocksDBException {

	}

	@Override
	public void subsequent(long iterationId,
			long skipCount,
			long takeCount,
			IteratorCallback<? super MemorySegment> callback) throws it.cavallium.rockserver.core.common.RocksDBException {

	}

	private MemorySegment dbGet(REntry<Transaction> tx,
			ColumnInstance col,
			Arena arena,
			ReadOptions readOptions,
			MemorySegment calculatedKey) throws RocksDBException {
		if (tx != null) {
			var previousRawBucketByteArray = tx.val().get(col.cfh(), readOptions, calculatedKey.toArray(BIG_ENDIAN_BYTES));
			return Utils.toMemorySegment(arena, previousRawBucketByteArray);
		} else {
			var db = this.db.get();
			if (USE_FAST_GET) {
				return dbGetDirect(arena, readOptions, calculatedKey);
			} else {
				var previousRawBucketByteArray = db.get(col.cfh(), readOptions, calculatedKey.toArray(BIG_ENDIAN_BYTES));
				return Utils.toMemorySegment(arena, previousRawBucketByteArray);
			}
		}
	}

	@Nullable
	private MemorySegment dbGetDirect(Arena arena, ReadOptions readOptions, MemorySegment calculatedKey)
			throws RocksDBException {
		// Get the key nio buffer to pass to RocksDB
		ByteBuffer keyNioBuffer = calculatedKey.asByteBuffer();

		// Create a direct result buffer because RocksDB works only with direct buffers
		var resultBuffer = arena.allocate(INITIAL_DIRECT_READ_BYTE_BUF_SIZE_BYTES).asByteBuffer();
		var keyMayExist = this.db.get().keyMayExist(readOptions, keyNioBuffer.rewind(), resultBuffer.clear());
		return switch (keyMayExist.exists) {
			case kNotExist -> null;
			case kExistsWithValue, kExistsWithoutValue -> {
				// At the beginning, size reflects the expected size, then it becomes the real data size
				int size = keyMayExist.exists == kExistsWithValue ? keyMayExist.valueLength : -1;
				if (keyMayExist.exists == kExistsWithoutValue || size > resultBuffer.limit()) {
					if (size > resultBuffer.capacity()) {
						resultBuffer = arena.allocate(size).asByteBuffer();
					}
					size = this.db.get().get(readOptions, keyNioBuffer.rewind(), resultBuffer.clear());
				}

				if (size == RocksDB.NOT_FOUND) {
					yield null;
				} else if (size == resultBuffer.limit()) {
					yield MemorySegment.ofBuffer(resultBuffer.position(0));
				} else {
					throw new IllegalStateException("size (" + size + ") != read size (" + resultBuffer.limit() + ")");
				}
			}
		};
	}

	private boolean valueEquals(MemorySegment previousValue, MemorySegment currentValue) {
		previousValue = requireNonNullElse(previousValue, NULL);
		currentValue = requireNonNullElse(currentValue, NULL);
		return MemorySegment.mismatch(previousValue, 0, previousValue.byteSize(), currentValue, 0, currentValue.byteSize()) != -1;
	}

	public static byte[] toByteArray(MemorySegment memorySegment) {
		return memorySegment.toArray(BIG_ENDIAN_BYTES);
	}

	private ColumnInstance getColumn(long columnId) {
		var col = columns.get(columnId);
		if (col != null) {
			return col;
		} else {
			throw new it.cavallium.rockserver.core.common.RocksDBException(RocksDBErrorType.COLUMN_NOT_FOUND,
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

	public Path getPath() {
		return path;
	}

}
