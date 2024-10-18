package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public sealed interface RocksDBAPICommand<R> {

	R handleSync(RocksDBSyncAPI api);
	CompletionStage<R> handleAsync(RocksDBAsyncAPI api);

	/**
	 * Open a transaction
	 * <p>
	 * Returns the transaction id
	 *
	 * @param timeoutMs timeout in milliseconds
	 */
	record OpenTransaction(long timeoutMs) implements RocksDBAPICommand<Long> {

		@Override
		public Long handleSync(RocksDBSyncAPI api) {
			return api.openTransaction(timeoutMs);
		}

		@Override
		public CompletionStage<Long> handleAsync(RocksDBAsyncAPI api) {
			return api.openTransactionAsync(timeoutMs);
		}

	}
	/**
	 * Close a transaction
	 * <p>
	 * Returns true if committed, if false, you should try again
	 *
	 * @param transactionId transaction id to close
	 * @param commit true to commit the transaction, false to rollback it
	 */
	record CloseTransaction(long transactionId, boolean commit) implements RocksDBAPICommand<Boolean> {

		@Override
		public Boolean handleSync(RocksDBSyncAPI api) {
			return api.closeTransaction(transactionId, commit);
		}

		@Override
		public CompletionStage<Boolean> handleAsync(RocksDBAsyncAPI api) {
			return api.closeTransactionAsync(transactionId, commit);
		}

	}
	/**
	 * Close a failed update, discarding all changes
	 *
	 * @param updateId update id to close
	 */
	record CloseFailedUpdate(long updateId) implements RocksDBAPICommand<Void> {

		@Override
		public Void handleSync(RocksDBSyncAPI api) {
			api.closeFailedUpdate(updateId);
			return null;
		}

		@Override
		public CompletionStage<Void> handleAsync(RocksDBAsyncAPI api) {
			return api.closeFailedUpdateAsync(updateId);
		}

	}
	/**
	 * Create a column
	 * <p>
	 * Returns the column id
	 *
	 * @param name   column name
	 * @param schema column key-value schema
	 */
	record CreateColumn(String name, @NotNull ColumnSchema schema) implements RocksDBAPICommand<Long> {

		@Override
		public Long handleSync(RocksDBSyncAPI api) {
			return api.createColumn(name, schema);
		}

		@Override
		public CompletionStage<Long> handleAsync(RocksDBAsyncAPI api) {
			return api.createColumnAsync(name, schema);
		}

	}
	/**
	 * Delete a column
	 * @param columnId column id
	 */
	record DeleteColumn(long columnId) implements RocksDBAPICommand<Void> {

		@Override
		public Void handleSync(RocksDBSyncAPI api) {
			api.deleteColumn(columnId);
			return null;
		}

		@Override
		public CompletionStage<Void> handleAsync(RocksDBAsyncAPI api) {
			return api.deleteColumnAsync(columnId);
		}

	}
	/**
	 * Get column id by name
	 * <p>
	 * Returns the column id
	 *
	 * @param name column name
	 */
	record GetColumnId(@NotNull String name) implements RocksDBAPICommand<Long> {

		@Override
		public Long handleSync(RocksDBSyncAPI api) {
			return api.getColumnId(name);
		}

		@Override
		public CompletionStage<Long> handleAsync(RocksDBAsyncAPI api) {
			return api.getColumnIdAsync(name);
		}

	}
	/**
	 * Put an element into the specified position
	 * @param arena arena
	 * @param transactionOrUpdateId transaction id, update id, or 0
	 * @param columnId column id
	 * @param keys column keys, or empty array if not needed
	 * @param value value, or null if not needed
	 * @param requestType the request type determines which type of data will be returned.
	 */
	record Put<T>(Arena arena,
								long transactionOrUpdateId,
								long columnId,
								Keys keys,
								@NotNull MemorySegment value,
								RequestPut<? super MemorySegment, T> requestType) implements RocksDBAPICommand<T> {

		@Override
		public T handleSync(RocksDBSyncAPI api) {
			return api.put(arena, transactionOrUpdateId, columnId, keys, value, requestType);
		}

		@Override
		public CompletionStage<T> handleAsync(RocksDBAsyncAPI api) {
			return api.putAsync(arena, transactionOrUpdateId, columnId, keys, value, requestType);
		}

		@Override
		public String toString() {
			var sb = new StringBuilder("PUT");
			if (transactionOrUpdateId != 0) {
				sb.append(" tx:").append(transactionOrUpdateId);
			}
			sb.append(" column:").append(columnId);
			sb.append(" keys:").append(keys);
			sb.append(" value:").append(Utils.toPrettyString(value));
			sb.append(" expected:").append(requestType.getRequestTypeId());
			return sb.toString();
		}
	}
	/**
	 * Put multiple elements into the specified positions
	 * @param arena arena
	 * @param transactionOrUpdateId transaction id, update id, or 0
	 * @param columnId column id
	 * @param keys multiple lists of column keys
	 * @param values multiple values, or null if not needed
	 * @param requestType the request type determines which type of data will be returned.
	 */
	record PutMulti<T>(Arena arena, long transactionOrUpdateId, long columnId,
					   @NotNull List<Keys> keys,
					   @NotNull List<@NotNull MemorySegment> values,
					   RequestPut<? super MemorySegment, T> requestType) implements RocksDBAPICommand<List<T>> {

		@Override
		public List<T> handleSync(RocksDBSyncAPI api) {
			return api.putMulti(arena, transactionOrUpdateId, columnId, keys, values, requestType);
		}

		@Override
		public CompletionStage<List<T>> handleAsync(RocksDBAsyncAPI api) {
			return api.putMultiAsync(arena, transactionOrUpdateId, columnId, keys, values, requestType);
		}

		@Override
		public String toString() {
			var sb = new StringBuilder("PUT_MULTI");
			if (transactionOrUpdateId != 0) {
				sb.append(" tx:").append(transactionOrUpdateId);
			}
			sb.append(" column:").append(columnId);
			sb.append(" expected:").append(requestType.getRequestTypeId());
			sb.append(" multi:[");
			for (int i = 0; i < keys.size(); i++) {
				if (i > 0) sb.append(",");
				sb.append(" keys:").append(keys.get(i));
				sb.append(" value:").append(Utils.toPrettyString(values.get(i)));
			}
			sb.append("]");
			return sb.toString();
		}
	}
	/**
	 * Put multiple elements into the specified positions
	 * @param columnId column id
	 * @param batchPublisher publisher of batches of keys and values
	 * @param mode put batch mode
	 */
	record PutBatch(long columnId,
					@NotNull org.reactivestreams.Publisher<@NotNull KVBatch> batchPublisher,
					@NotNull PutBatchMode mode) implements RocksDBAPICommand<Void> {

		@Override
		public Void handleSync(RocksDBSyncAPI api) {
			api.putBatch(columnId, batchPublisher, mode);
			return null;
		}

		@Override
		public CompletionStage<Void> handleAsync(RocksDBAsyncAPI api) {
			return api.putBatchAsync(columnId, batchPublisher, mode);
		}

		@Override
		public String toString() {
			var sb = new StringBuilder("PUT_BATCH");
			sb.append(" column:").append(columnId);
			sb.append(" mode:").append(mode);
			sb.append(" batch:[...]");
			return sb.toString();
		}
	}
	/**
	 * Get an element from the specified position
	 * @param arena arena
	 * @param transactionOrUpdateId transaction id, update id for retry operations, or 0
	 * @param columnId column id
	 * @param keys column keys, or empty array if not needed
	 * @param requestType the request type determines which type of data will be returned.
	 */
	record Get<T>(Arena arena,
								long transactionOrUpdateId,
								long columnId,
								Keys keys,
								RequestGet<? super MemorySegment, T> requestType) implements RocksDBAPICommand<T> {

		@Override
		public T handleSync(RocksDBSyncAPI api) {
			return api.get(arena, transactionOrUpdateId, columnId, keys, requestType);
		}

		@Override
		public CompletionStage<T> handleAsync(RocksDBAsyncAPI api) {
			return api.getAsync(arena, transactionOrUpdateId, columnId, keys, requestType);
		}

		@Override
		public String toString() {
			var sb = new StringBuilder("GET");
			if (transactionOrUpdateId != 0) {
				sb.append(" tx:").append(transactionOrUpdateId);
			}
			sb.append(" column:").append(columnId);
			sb.append(" keys:").append(keys);
			sb.append(" expected:").append(requestType.getRequestTypeId());
			return sb.toString();
		}
	}
	/**
	 * Open an iterator
	 * <p>
	 * Returns the iterator id
	 *
	 * @param arena arena
	 * @param transactionId transaction id, or 0
	 * @param columnId column id
	 * @param startKeysInclusive start keys, inclusive. [] means "the beginning"
	 * @param endKeysExclusive end keys, exclusive. Null means "the end"
	 * @param reverse if true, seek in reverse direction
	 * @param timeoutMs timeout in milliseconds
	 */
	record OpenIterator(Arena arena,
											long transactionId,
											long columnId,
											Keys startKeysInclusive,
											@Nullable Keys endKeysExclusive,
											boolean reverse,
											long timeoutMs) implements RocksDBAPICommand<Long> {

		@Override
		public Long handleSync(RocksDBSyncAPI api) {
			return api.openIterator(arena, transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, timeoutMs);
		}

		@Override
		public CompletionStage<Long> handleAsync(RocksDBAsyncAPI api) {
			return api.openIteratorAsync(arena,
					transactionId,
					columnId,
					startKeysInclusive,
					endKeysExclusive,
					reverse,
					timeoutMs
			);
		}

	}
	/**
	 * Close an iterator
	 * @param iteratorId iterator id
	 */
	record CloseIterator(long iteratorId) implements RocksDBAPICommand<Void> {

		@Override
		public Void handleSync(RocksDBSyncAPI api) {
			api.closeIterator(iteratorId);
			return null;
		}

		@Override
		public CompletionStage<Void> handleAsync(RocksDBAsyncAPI api) {
			return api.closeIteratorAsync(iteratorId);
		}

	}
	/**
	 * Seek to the specific element during an iteration, or the subsequent one if not found
	 * @param arena arena
	 * @param iterationId iteration id
	 * @param keys keys, inclusive. [] means "the beginning"
	 */
	record SeekTo(Arena arena, long iterationId, Keys keys) implements
			RocksDBAPICommand<Void> {

		@Override
		public Void handleSync(RocksDBSyncAPI api) {
			api.seekTo(arena, iterationId, keys);
			return null;
		}

		@Override
		public CompletionStage<Void> handleAsync(RocksDBAsyncAPI api) {
			return api.seekToAsync(arena, iterationId, keys);
		}

	}
	/**
	 * Get the subsequent element during an iteration
	 * @param arena arena
	 * @param iterationId iteration id
	 * @param skipCount number of elements to skip
	 * @param takeCount number of elements to take
	 * @param requestType the request type determines which type of data will be returned.
	 */
	record Subsequent<T>(Arena arena,
										long iterationId,
										long skipCount,
										long takeCount,
										@NotNull RequestType.RequestIterate<? super MemorySegment, T> requestType)
			implements RocksDBAPICommand<T> {

		@Override
		public T handleSync(RocksDBSyncAPI api) {
			return api.subsequent(arena, iterationId, skipCount, takeCount, requestType);
		}

		@Override
		public CompletionStage<T> handleAsync(RocksDBAsyncAPI api) {
			return api.subsequentAsync(arena, iterationId, skipCount, takeCount, requestType);
		}

	}
	/**
	 * Get some values in a range
	 * <p>
	 * Returns the result
	 *
	 * @param arena arena
	 * @param transactionId transaction id, or 0
	 * @param columnId column id
	 * @param startKeysInclusive start keys, inclusive. [] means "the beginning"
	 * @param endKeysExclusive end keys, exclusive. Null means "the end"
	 * @param reverse if true, seek in reverse direction
	 * @param requestType the request type determines which type of data will be returned.
	 * @param timeoutMs timeout in milliseconds
	 */
	record GetRange<T>(Arena arena,
					   long transactionId,
					   long columnId,
					   @Nullable Keys startKeysInclusive,
					   @Nullable Keys endKeysExclusive,
					   boolean reverse,
					   RequestType.RequestGetRange<? super KV, T> requestType,
					   long timeoutMs) implements RocksDBAPICommand<T> {

		@Override
		public T handleSync(RocksDBSyncAPI api) {
			return api.getRange(arena,
					transactionId,
					columnId,
					startKeysInclusive,
					endKeysExclusive,
					reverse,
					requestType,
					timeoutMs
			);
		}

		@Override
		public CompletableFuture<T> handleAsync(RocksDBAsyncAPI api) {
			return api.getRangeAsync(arena,
					transactionId,
					columnId,
					startKeysInclusive,
					endKeysExclusive,
					reverse,
					requestType,
					timeoutMs
			);
		}

	}
}
