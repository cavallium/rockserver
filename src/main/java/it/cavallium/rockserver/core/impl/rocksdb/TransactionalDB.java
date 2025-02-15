package it.cavallium.rockserver.core.impl.rocksdb;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.OptimisticTransactionOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionOptions;
import org.rocksdb.WriteOptions;

public sealed interface TransactionalDB extends Closeable {

	static TransactionalDB create(String path, RocksDB db,
			List<ColumnFamilyDescriptor> descriptors,
			ArrayList<ColumnFamilyHandle> handles,
			DatabaseTasks databaseTasks) {
		return switch (db) {
			case OptimisticTransactionDB optimisticTransactionDB -> new OptimisticTransactionalDB(path, optimisticTransactionDB, descriptors, handles, databaseTasks);
			case TransactionDB transactionDB -> new PessimisticTransactionalDB(path, transactionDB, descriptors, handles, databaseTasks);
			default -> throw new UnsupportedOperationException("This database is not transactional");
		};
	}

	TransactionalOptions createTransactionalOptions(long timeoutMs);

	String getPath();

	RocksDB get();
	Map<ColumnFamilyDescriptor, ColumnFamilyHandle> getStartupColumns();
	/**
	 * Starts a new Transaction.
	 * <p>
	 * Caller is responsible for calling {@link #close()} on the returned
	 * transaction when it is no longer needed.
	 *
	 * @param writeOptions Any write options for the transaction
	 * @return a new transaction
	 */
	Transaction beginTransaction(final WriteOptions writeOptions);

	/**
	 * Starts a new Transaction.
	 * <p>
	 * Caller is responsible for calling {@link #close()} on the returned
	 * transaction when it is no longer needed.
	 *
	 * @param writeOptions Any write options for the transaction
	 * @param transactionOptions Any options for the transaction
	 * @return a new transaction
	 */
	Transaction beginTransaction(final WriteOptions writeOptions,
			final TransactionalOptions transactionOptions);

	/**
	 * Starts a new Transaction.
	 * <p>
	 * Caller is responsible for calling {@link #close()} on the returned
	 * transaction when it is no longer needed.
	 *
	 * @param writeOptions Any write options for the transaction
	 * @param oldTransaction this Transaction will be reused instead of allocating
	 *     a new one. This is an optimization to avoid extra allocations
	 *     when repeatedly creating transactions.
	 * @return The oldTransaction which has been reinitialized as a new
	 *     transaction
	 */
	Transaction beginTransaction(final WriteOptions writeOptions,
			final Transaction oldTransaction);

	/**
	 * Starts a new Transaction.
	 * <p>
	 * Caller is responsible for calling {@link #close()} on the returned
	 * transaction when it is no longer needed.
	 *
	 * @param writeOptions Any write options for the transaction
	 * @param transactionOptions Any options for the transaction
	 * @param oldTransaction this Transaction will be reused instead of allocating
	 *     a new one. This is an optimization to avoid extra allocations
	 *     when repeatedly creating transactions.
	 * @return The oldTransaction which has been reinitialized as a new
	 *     transaction
	 */
	Transaction beginTransaction(final WriteOptions writeOptions,
			final TransactionalOptions transactionOptions, final Transaction oldTransaction);

	sealed interface TransactionalOptions extends Closeable {

		@Override
		void close();
	}

	abstract sealed class BaseTransactionalDB<RDB extends RocksDB> implements TransactionalDB {

		private final String path;
		protected final RDB db;
		private final DatabaseTasks databaseTasks;
		private final List<ColumnFamilyDescriptor> descriptors;
		private final ArrayList<ColumnFamilyHandle> handles;
		private final Object closeLock = new Object();
		private volatile boolean closed = false;

		public BaseTransactionalDB(String path, RDB db,
				List<ColumnFamilyDescriptor> descriptors,
				ArrayList<ColumnFamilyHandle> handles,
				DatabaseTasks databaseTasks) {
			this.path = path;
			this.db = db;
			this.descriptors = descriptors;
			this.handles = handles;
			this.databaseTasks = databaseTasks;

			databaseTasks.start();
		}

		@Override
		public final String getPath() {
			return path;
		}

		@Override
		public final RocksDB get() {
			return db;
		}

		@Override
		public Map<ColumnFamilyDescriptor, ColumnFamilyHandle> getStartupColumns() {
			var cols = new HashMap<ColumnFamilyDescriptor, ColumnFamilyHandle>();
			assert this.descriptors.size() == this.handles.size();
			for (int i = 0; i < descriptors.size(); i++) {
				cols.put(this.descriptors.get(i), this.handles.get(i));
			}
			return cols;
		}

		@Override
		public void close() throws IOException {
			if (this.closed) {
				return;
			}
			synchronized (this.closeLock) {
				if (this.closed) {
					return;
				}
				this.closed = true;
				List<Exception> exceptions = new ArrayList<>();
				try {
					databaseTasks.close();
				} catch (Exception ex) {
					exceptions.add(ex);
				}
				try {
					if (db.isOwningHandle()) {
						db.flushWal(true);
					}
				} catch (RocksDBException e) {
					exceptions.add(e);
				}
				try (var options = new FlushOptions().setWaitForFlush(true).setAllowWriteStall(true)) {
					if (db.isOwningHandle()) {
						var openHandles = handles.stream().filter(AbstractImmutableNativeReference::isOwningHandle).toList();
						db.flush(options, openHandles);
					}
				} catch (RocksDBException e) {
					exceptions.add(e);
				}
				db.cancelAllBackgroundWork(true);
				try {
					if (db.isOwningHandle()) {
						db.closeE();
					}
				} catch (RocksDBException e) {
					exceptions.add(e);
				}
				for (ColumnFamilyHandle handle : handles) {
					try {
						if (handle.isOwningHandle()) {
							handle.close();
						}
					} catch (Exception ex) {
						exceptions.add(ex);
					}
				}
				if (!exceptions.isEmpty()) {
					IOException ex;
					if (exceptions.size() == 1) {
						ex = new IOException("Failed to close the database", exceptions.getFirst());
					} else {
						ex = new IOException("Failed to close the database");
						exceptions.forEach(ex::addSuppressed);
					}
					throw ex;
				}
			}
		}
	}

	final class PessimisticTransactionalDB extends BaseTransactionalDB<TransactionDB> {

		public PessimisticTransactionalDB(String path, TransactionDB db,
				List<ColumnFamilyDescriptor> descriptors,
				ArrayList<ColumnFamilyHandle> handles,
				DatabaseTasks databaseTasks) {
			super(path, db, descriptors, handles, databaseTasks);
		}

		@Override
		public TransactionalOptions createTransactionalOptions(long timeoutMs) {
			return new TransactionalOptionsPessimistic(new TransactionOptions() {
				{
					RocksLeakDetector.register(this, "create-transactional-options-transaction-options", owningHandle_);
				}
			}.setExpiration(timeoutMs));
		}

		@Override
		public Transaction beginTransaction(WriteOptions writeOptions) {
			return db.beginTransaction(writeOptions);
		}

		@Override
		public Transaction beginTransaction(WriteOptions writeOptions, TransactionalOptions transactionOptions) {
			return db.beginTransaction(writeOptions,
					((TransactionalOptionsPessimistic) transactionOptions).transactionOptions
			);
		}

		@Override
		public Transaction beginTransaction(WriteOptions writeOptions, Transaction oldTransaction) {
			return db.beginTransaction(writeOptions, oldTransaction);
		}

		@Override
		public Transaction beginTransaction(WriteOptions writeOptions,
				TransactionalOptions transactionOptions,
				Transaction oldTransaction) {
			return db.beginTransaction(writeOptions,
					((TransactionalOptionsPessimistic) transactionOptions).transactionOptions,
					oldTransaction
			);
		}

		private record TransactionalOptionsPessimistic(TransactionOptions transactionOptions) implements
				TransactionalOptions {

			@Override
			public void close() {
				transactionOptions.close();
			}
		}
	}

	final class OptimisticTransactionalDB extends BaseTransactionalDB<OptimisticTransactionDB> {

		public OptimisticTransactionalDB(String path, OptimisticTransactionDB db,
				List<ColumnFamilyDescriptor> descriptors,
				ArrayList<ColumnFamilyHandle> handles,
				DatabaseTasks databaseTasks) {
			super(path, db, descriptors, handles, databaseTasks);
		}

		@Override
		public TransactionalOptions createTransactionalOptions(long timeoutMs) {
			return new TransactionalOptionsOptimistic(new OptimisticTransactionOptions() {
				{
					RocksLeakDetector.register(this, "create-transactional-options-optimistic-transaction-options", owningHandle_);
				}
			});
		}

		@Override
		public Transaction beginTransaction(WriteOptions writeOptions) {
			return db.beginTransaction(writeOptions);
		}

		@Override
		public Transaction beginTransaction(WriteOptions writeOptions, TransactionalOptions transactionOptions) {
			return db.beginTransaction(writeOptions,
					((TransactionalOptionsOptimistic) transactionOptions).transactionOptions
			);
		}

		@Override
		public Transaction beginTransaction(WriteOptions writeOptions, Transaction oldTransaction) {
			return db.beginTransaction(writeOptions, oldTransaction);
		}

		@Override
		public Transaction beginTransaction(WriteOptions writeOptions,
				TransactionalOptions transactionOptions,
				Transaction oldTransaction) {
			return db.beginTransaction(writeOptions,
					((TransactionalOptionsOptimistic) transactionOptions).transactionOptions,
					oldTransaction
			);
		}

		private record TransactionalOptionsOptimistic(OptimisticTransactionOptions transactionOptions) implements
				TransactionalOptions {

			@Override
			public void close() {
				transactionOptions.close();
			}
		}
	}
}
