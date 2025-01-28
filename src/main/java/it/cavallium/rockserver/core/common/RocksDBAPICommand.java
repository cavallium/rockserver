package it.cavallium.rockserver.core.common;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;

public sealed interface RocksDBAPICommand<RESULT_ITEM_TYPE, SYNC_RESULT, ASYNC_RESULT> {

	SYNC_RESULT handleSync(RocksDBSyncAPI api);
	ASYNC_RESULT handleAsync(RocksDBAsyncAPI api);

	boolean isReadOnly();

	sealed interface RocksDBAPICommandSingle<R> extends RocksDBAPICommand<R, R, CompletableFuture<R>> {


		/**
		 * Open a transaction
		 * <p>
		 * Returns the transaction id
		 *
		 * @param timeoutMs timeout in milliseconds
		 */
		record OpenTransaction(long timeoutMs) implements RocksDBAPICommandSingle<Long> {

			@Override
			public Long handleSync(RocksDBSyncAPI api) {
				return api.openTransaction(timeoutMs);
			}

			@Override
			public CompletableFuture<Long> handleAsync(RocksDBAsyncAPI api) {
				return api.openTransactionAsync(timeoutMs);
			}

			@Override
			public boolean isReadOnly() {
				return false;
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
		record CloseTransaction(long transactionId, boolean commit) implements RocksDBAPICommandSingle<Boolean> {

			@Override
			public Boolean handleSync(RocksDBSyncAPI api) {
				return api.closeTransaction(transactionId, commit);
			}

			@Override
			public CompletableFuture<Boolean> handleAsync(RocksDBAsyncAPI api) {
				return api.closeTransactionAsync(transactionId, commit);
			}

			@Override
			public boolean isReadOnly() {
				return false;
			}

		}
		/**
		 * Close a failed update, discarding all changes
		 *
		 * @param updateId update id to close
		 */
		record CloseFailedUpdate(long updateId) implements RocksDBAPICommandSingle<Void> {

			@Override
			public Void handleSync(RocksDBSyncAPI api) {
				api.closeFailedUpdate(updateId);
				return null;
			}

			@Override
			public CompletableFuture<Void> handleAsync(RocksDBAsyncAPI api) {
				return api.closeFailedUpdateAsync(updateId);
			}

			@Override
			public boolean isReadOnly() {
				return true;
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
		record CreateColumn(String name, @NotNull ColumnSchema schema) implements RocksDBAPICommandSingle<Long> {

			@Override
			public Long handleSync(RocksDBSyncAPI api) {
				return api.createColumn(name, schema);
			}

			@Override
			public CompletableFuture<Long> handleAsync(RocksDBAsyncAPI api) {
				return api.createColumnAsync(name, schema);
			}

			@Override
			public boolean isReadOnly() {
				return false;
			}

		}
		/**
		 * Delete a column
		 * @param columnId column id
		 */
		record DeleteColumn(long columnId) implements RocksDBAPICommandSingle<Void> {

			@Override
			public Void handleSync(RocksDBSyncAPI api) {
				api.deleteColumn(columnId);
				return null;
			}

			@Override
			public CompletableFuture<Void> handleAsync(RocksDBAsyncAPI api) {
				return api.deleteColumnAsync(columnId);
			}

			@Override
			public boolean isReadOnly() {
				return false;
			}

		}
		/**
		 * Get column id by name
		 * <p>
		 * Returns the column id
		 *
		 * @param name column name
		 */
		record GetColumnId(@NotNull String name) implements RocksDBAPICommandSingle<Long> {

			@Override
			public Long handleSync(RocksDBSyncAPI api) {
				return api.getColumnId(name);
			}

			@Override
			public CompletableFuture<Long> handleAsync(RocksDBAsyncAPI api) {
				return api.getColumnIdAsync(name);
			}

			@Override
			public boolean isReadOnly() {
				return true;
			}

		}
		/**
		 * Put an element into the specified position
		 *
		 * @param transactionOrUpdateId transaction id, update id, or 0
		 * @param columnId              column id
		 * @param keys                  column keys, or empty array if not needed
		 * @param value                 value, or null if not needed
		 * @param requestType           the request type determines which type of data will be returned.
		 */
		record Put<T>(long transactionOrUpdateId,
					  long columnId,
					  Keys keys,
					  @NotNull Buf value,
					  RequestPut<? super Buf, T> requestType) implements RocksDBAPICommandSingle<T> {

			@Override
			public T handleSync(RocksDBSyncAPI api) {
				return api.put(transactionOrUpdateId, columnId, keys, value, requestType);
			}

			@Override
			public CompletableFuture<T> handleAsync(RocksDBAsyncAPI api) {
				return api.putAsync(transactionOrUpdateId, columnId, keys, value, requestType);
			}

			@Override
			public boolean isReadOnly() {
				return false;
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
		 *
		 * @param transactionOrUpdateId transaction id, update id, or 0
		 * @param columnId              column id
		 * @param keys                  multiple lists of column keys
		 * @param values                multiple values, or null if not needed
		 * @param requestType           the request type determines which type of data will be returned.
		 */
		record PutMulti<T>(long transactionOrUpdateId, long columnId,
											 @NotNull List<Keys> keys,
											 @NotNull List<@NotNull Buf> values,
											 RequestPut<? super Buf, T> requestType) implements RocksDBAPICommandSingle<List<T>> {

			@Override
			public List<T> handleSync(RocksDBSyncAPI api) {
				return api.putMulti(transactionOrUpdateId, columnId, keys, values, requestType);
			}

			@Override
			public CompletableFuture<List<T>> handleAsync(RocksDBAsyncAPI api) {
				return api.putMultiAsync(transactionOrUpdateId, columnId, keys, values, requestType);
			}

			@Override
			public boolean isReadOnly() {
				return false;
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
						@NotNull PutBatchMode mode) implements RocksDBAPICommandSingle<Void> {

			@Override
			public Void handleSync(RocksDBSyncAPI api) {
				api.putBatch(columnId, batchPublisher, mode);
				return null;
			}

			@Override
			public CompletableFuture<Void> handleAsync(RocksDBAsyncAPI api) {
				return api.putBatchAsync(columnId, batchPublisher, mode);
			}

			@Override
			public boolean isReadOnly() {
				return false;
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
		 * @param transactionOrUpdateId transaction id, update id for retry operations, or 0
		 * @param columnId column id
		 * @param keys column keys, or empty array if not needed
		 * @param requestType the request type determines which type of data will be returned.
		 */
		record Get<T>(long transactionOrUpdateId,
					  long columnId,
					  Keys keys,
					  RequestGet<? super Buf, T> requestType) implements RocksDBAPICommandSingle<T> {

			@Override
			public T handleSync(RocksDBSyncAPI api) {
				return api.get(transactionOrUpdateId, columnId, keys, requestType);
			}

			@Override
			public CompletableFuture<T> handleAsync(RocksDBAsyncAPI api) {
				return api.getAsync(transactionOrUpdateId, columnId, keys, requestType);
			}

			@Override
			public boolean isReadOnly() {
				return requestType.getRequestTypeId() != RequestTypeId.FOR_UPDATE;
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
		 * @param transactionId transaction id, or 0
		 * @param columnId column id
		 * @param startKeysInclusive start keys, inclusive. [] means "the beginning"
		 * @param endKeysExclusive end keys, exclusive. Null means "the end"
		 * @param reverse if true, seek in reverse direction
		 * @param timeoutMs timeout in milliseconds
		 */
		record OpenIterator(long transactionId,
							long columnId,
							Keys startKeysInclusive,
							@Nullable Keys endKeysExclusive,
							boolean reverse,
							long timeoutMs) implements RocksDBAPICommandSingle<Long> {

			@Override
			public Long handleSync(RocksDBSyncAPI api) {
				return api.openIterator(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, timeoutMs);
			}

			@Override
			public CompletableFuture<Long> handleAsync(RocksDBAsyncAPI api) {
				return api.openIteratorAsync(transactionId,
						columnId,
						startKeysInclusive,
						endKeysExclusive,
						reverse,
						timeoutMs
				);
			}

			@Override
			public boolean isReadOnly() {
				return true;
			}

		}
		/**
		 * Close an iterator
		 * @param iteratorId iterator id
		 */
		record CloseIterator(long iteratorId) implements RocksDBAPICommandSingle<Void> {

			@Override
			public Void handleSync(RocksDBSyncAPI api) {
				api.closeIterator(iteratorId);
				return null;
			}

			@Override
			public CompletableFuture<Void> handleAsync(RocksDBAsyncAPI api) {
				return api.closeIteratorAsync(iteratorId);
			}

			@Override
			public boolean isReadOnly() {
				return true;
			}

		}
		/**
		 * Seek to the specific element during an iteration, or the subsequent one if not found
		 * @param iterationId iteration id
		 * @param keys keys, inclusive. [] means "the beginning"
		 */
		record SeekTo(long iterationId, Keys keys) implements RocksDBAPICommandSingle<Void> {

			@Override
			public Void handleSync(RocksDBSyncAPI api) {
				api.seekTo(iterationId, keys);
				return null;
			}

			@Override
			public CompletableFuture<Void> handleAsync(RocksDBAsyncAPI api) {
				return api.seekToAsync(iterationId, keys);
			}

			@Override
			public boolean isReadOnly() {
				return true;
			}

		}
		/**
		 * Get the subsequent element during an iteration
		 * @param iterationId iteration id
		 * @param skipCount number of elements to skip
		 * @param takeCount number of elements to take
		 * @param requestType the request type determines which type of data will be returned.
		 */
		record Subsequent<T>(long iterationId,
							 long skipCount,
							 long takeCount,
							 @NotNull RequestType.RequestIterate<? super Buf, T> requestType)
				implements RocksDBAPICommandSingle<T> {

			@Override
			public T handleSync(RocksDBSyncAPI api) {
				return api.subsequent(iterationId, skipCount, takeCount, requestType);
			}

			@Override
			public CompletableFuture<T> handleAsync(RocksDBAsyncAPI api) {
				return api.subsequentAsync(iterationId, skipCount, takeCount, requestType);
			}

			@Override
			public boolean isReadOnly() {
				return true;
			}

		}
		/**
		 * Reduce values in a range
		 * <p>
		 * Returns the result
		 *
		 * @param transactionId transaction id, or 0
		 * @param columnId column id
		 * @param startKeysInclusive start keys, inclusive. [] means "the beginning"
		 * @param endKeysExclusive end keys, exclusive. Null means "the end"
		 * @param reverse if true, seek in reverse direction
		 * @param requestType the request type determines which type of data will be returned.
		 * @param timeoutMs timeout in milliseconds
		 */
		record ReduceRange<T>(long transactionId,
							  long columnId,
							  @Nullable Keys startKeysInclusive,
							  @Nullable Keys endKeysExclusive,
							  boolean reverse,
							  RequestType.RequestReduceRange<? super KV, T> requestType,
							  long timeoutMs) implements RocksDBAPICommandSingle<T> {

			@Override
			public T handleSync(RocksDBSyncAPI api) {
				return api.reduceRange(transactionId,
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
				return api.reduceRangeAsync(transactionId,
						columnId,
						startKeysInclusive,
						endKeysExclusive,
						reverse,
						requestType,
						timeoutMs
				);
			}

			@Override
			public boolean isReadOnly() {
				return true;
			}

		}
	}
	sealed interface RocksDBAPICommandStream<R> extends RocksDBAPICommand<R, Stream<R>, Publisher<R>> {

		/**
		 * Get some values in a range
		 * <p>
		 * Returns the result
		 *
		 * @param transactionId transaction id, or 0
		 * @param columnId column id
		 * @param startKeysInclusive start keys, inclusive. [] means "the beginning"
		 * @param endKeysExclusive end keys, exclusive. Null means "the end"
		 * @param reverse if true, seek in reverse direction
		 * @param requestType the request type determines which type of data will be returned.
		 * @param timeoutMs timeout in milliseconds
		 */
		record GetRange<T>(long transactionId,
						   long columnId,
						   @Nullable Keys startKeysInclusive,
						   @Nullable Keys endKeysExclusive,
						   boolean reverse,
						   RequestType.RequestGetRange<? super KV, T> requestType,
						   long timeoutMs) implements RocksDBAPICommandStream<T> {

			@Override
			public Stream<T> handleSync(RocksDBSyncAPI api) {
				return api.getRange(transactionId,
						columnId,
						startKeysInclusive,
						endKeysExclusive,
						reverse,
						requestType,
						timeoutMs
				);
			}

			@Override
			public Publisher<T> handleAsync(RocksDBAsyncAPI api) {
				return api.getRangeAsync(transactionId,
						columnId,
						startKeysInclusive,
						endKeysExclusive,
						reverse,
						requestType,
						timeoutMs
				);
			}

			@Override
			public boolean isReadOnly() {
				return true;
			}

		}
	}
}
