package it.cavallium.rockserver.core.impl.rocksdb;

import java.io.Closeable;
import java.io.IOException;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.OptimisticTransactionOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionOptions;
import org.rocksdb.WriteOptions;

public sealed interface TransactionalDB extends Closeable {

	static TransactionalDB create(String path, RocksDB db) {
		return switch (db) {
			case OptimisticTransactionDB optimisticTransactionDB -> new OptimisticTransactionalDB(path, optimisticTransactionDB);
			case TransactionDB transactionDB -> new PessimisticTransactionalDB(path, transactionDB);
			default -> throw new UnsupportedOperationException("This database is not transactional");
		};
	}

	TransactionalOptions createTransactionalOptions();

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

	final class PessimisticTransactionalDB implements TransactionalDB {

		private final String path;
		private final TransactionDB db;

		public PessimisticTransactionalDB(String path, TransactionDB db) {
			this.path = path;
			this.db = db;
		}

		@Override
		public TransactionalOptions createTransactionalOptions() {
			return new TransactionalOptionsPessimistic(new TransactionOptions());
		}

		@Override
		public String getPath() {
			return path;
		}

		@Override
		public RocksDB get() {
			return db;
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

		@Override
		public void close() throws IOException {
			try {
				db.closeE();
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
		}

		private record TransactionalOptionsPessimistic(TransactionOptions transactionOptions) implements
				TransactionalOptions {

			@Override
			public void close() {
				transactionOptions.close();
			}
		}
	}

	final class OptimisticTransactionalDB implements TransactionalDB {

		private final String path;
		private final OptimisticTransactionDB db;

		public OptimisticTransactionalDB(String path, OptimisticTransactionDB db) {
			this.path = path;
			this.db = db;
		}

		@Override
		public TransactionalOptions createTransactionalOptions() {
			return new TransactionalOptionsOptimistic(new OptimisticTransactionOptions());
		}

		@Override
		public String getPath() {
			return path;
		}

		@Override
		public RocksDB get() {
			return db;
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

		@Override
		public void close() throws IOException {
			try {
				db.closeE();
			} catch (RocksDBException e) {
				throw new IOException(e);
			}
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
