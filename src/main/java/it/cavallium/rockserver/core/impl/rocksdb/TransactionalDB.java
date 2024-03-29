package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.impl.rocksdb.TransactionalDB.BaseTransactionalDB;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.OptimisticTransactionOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionOptions;
import org.rocksdb.WriteOptions;

public sealed interface TransactionalDB extends Closeable {

	static TransactionalDB create(String path, RocksDB db, DatabaseTasks databaseTasks) {
		return switch (db) {
			case OptimisticTransactionDB optimisticTransactionDB -> new OptimisticTransactionalDB(path, optimisticTransactionDB, databaseTasks);
			case TransactionDB transactionDB -> new PessimisticTransactionalDB(path, transactionDB, databaseTasks);
			default -> throw new UnsupportedOperationException("This database is not transactional");
		};
	}

	TransactionalOptions createTransactionalOptions(long timeoutMs);

	String getPath();

	RocksDB get();
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

		public BaseTransactionalDB(String path, RDB db, DatabaseTasks databaseTasks) {
			this.path = path;
			this.db = db;
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
		public void close() throws IOException {
			List<Exception> exceptions = new ArrayList<>();
			try {
				databaseTasks.close();
			} catch (Exception ex) {
				exceptions.add(ex);
			}
			try {
				db.closeE();
			} catch (RocksDBException e) {
				exceptions.add(e);
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

	final class PessimisticTransactionalDB extends BaseTransactionalDB<TransactionDB> {

		public PessimisticTransactionalDB(String path, TransactionDB db, DatabaseTasks databaseTasks) {
			super(path, db, databaseTasks);
		}

		@Override
		public TransactionalOptions createTransactionalOptions(long timeoutMs) {
			return new TransactionalOptionsPessimistic(new TransactionOptions().setExpiration(timeoutMs));
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

		public OptimisticTransactionalDB(String path, OptimisticTransactionDB db, DatabaseTasks databaseTasks) {
			super(path, db, databaseTasks);
		}

		@Override
		public TransactionalOptions createTransactionalOptions(long timeoutMs) {
			return new TransactionalOptionsOptimistic(new OptimisticTransactionOptions());
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
