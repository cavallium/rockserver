package it.cavallium.rockserver.core.common;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RequestType.RequestDelete;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import java.util.List;
import java.util.Map;
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
		record CreateColumn(String name,
							@NotNull ColumnSchema schema) implements RocksDBAPICommandSingle<Long> {

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
		 * Upload a merge operator
		 * @param name operator name
		 * @param className class name
		 * @param jarData jar payload
		 */
		record UploadMergeOperator(String name, String className, byte[] jarData) implements RocksDBAPICommandSingle<Long> {

			@Override
			public Long handleSync(RocksDBSyncAPI api) {
				return api.uploadMergeOperator(name, className, jarData);
			}

			@Override
			public CompletableFuture<Long> handleAsync(RocksDBAsyncAPI api) {
				return api.uploadMergeOperatorAsync(name, className, jarData);
			}

			@Override
			public boolean isReadOnly() {
				return false;
			}

			@Override
			public String toString() {
				return "UPLOAD_MERGE_OPERATOR name:" + name + " class:" + className;
			}
		}
		/**
		 * Check if merge operator exists
		 * @param name name
		 * @param hash hash
		 */
		record CheckMergeOperator(String name, byte[] hash) implements RocksDBAPICommandSingle<Long> {

			@Override
			public Long handleSync(RocksDBSyncAPI api) {
				return api.checkMergeOperator(name, hash);
			}

			@Override
			public CompletableFuture<Long> handleAsync(RocksDBAsyncAPI api) {
				return api.checkMergeOperatorAsync(name, hash);
			}

			@Override
			public boolean isReadOnly() {
				return true;
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
		 * Delete an element from the specified position
		 *
		 * @param transactionOrUpdateId transaction id, update id, or 0
		 * @param columnId              column id
		 * @param keys                  column keys
		 * @param requestType           the request type determines which type of data will be returned.
		 */
		record Delete<T>(long transactionOrUpdateId,
					  long columnId,
					  Keys keys,
					  RequestDelete<? super Buf, T> requestType) implements RocksDBAPICommandSingle<T> {

			@Override
			public T handleSync(RocksDBSyncAPI api) {
				return api.delete(transactionOrUpdateId, columnId, keys, requestType);
			}

			@Override
			public CompletableFuture<T> handleAsync(RocksDBAsyncAPI api) {
				return api.deleteAsync(transactionOrUpdateId, columnId, keys, requestType);
			}

			@Override
			public boolean isReadOnly() {
				return false;
			}

			@Override
			public String toString() {
				var sb = new StringBuilder("DELETE");
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
		 * Delete multiple elements from the specified positions
		 *
		 * @param transactionOrUpdateId transaction id, update id, or 0
		 * @param columnId              column id
		 * @param keys                  multiple lists of column keys
		 * @param requestType           the request type determines which type of data will be returned.
		 */
		record DeleteMulti<T>(long transactionOrUpdateId, long columnId,
											 @NotNull List<Keys> keys,
											 RequestDelete<? super Buf, T> requestType) implements RocksDBAPICommandSingle<List<T>> {

			@Override
			public List<T> handleSync(RocksDBSyncAPI api) {
				return api.deleteMulti(transactionOrUpdateId, columnId, keys, requestType);
			}

			@Override
			public CompletableFuture<List<T>> handleAsync(RocksDBAsyncAPI api) {
				return api.deleteMultiAsync(transactionOrUpdateId, columnId, keys, requestType);
			}

			@Override
			public boolean isReadOnly() {
				return false;
			}

			@Override
			public String toString() {
				var sb = new StringBuilder("DELETE_MULTI");
				if (transactionOrUpdateId != 0) {
					sb.append(" tx:").append(transactionOrUpdateId);
				}
				sb.append(" column:").append(columnId);
				sb.append(" expected:").append(requestType.getRequestTypeId());
				sb.append(" multi:[");
				for (int i = 0; i < keys.size(); i++) {
					if (i > 0) sb.append(",");
					sb.append(" keys:").append(keys.get(i));
				}
				sb.append("]");
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
		 * Merge an element with the specified position
		 *
		 * @param transactionOrUpdateId transaction id, update id, or 0
		 * @param columnId              column id
		 * @param keys                  column keys, or empty array if not needed
		 * @param value                 operand value
		 * @param requestType           determines whether to return the merged value
		 */
		record Merge<T>(long transactionOrUpdateId,
				  long columnId,
				  Keys keys,
				  @NotNull Buf value,
				  RequestType.RequestMerge<? super Buf, T> requestType) implements RocksDBAPICommandSingle<T> {

			@Override
			public T handleSync(RocksDBSyncAPI api) {
				return api.merge(transactionOrUpdateId, columnId, keys, value, requestType);
			}

			@Override
			public CompletableFuture<T> handleAsync(RocksDBAsyncAPI api) {
				return api.mergeAsync(transactionOrUpdateId, columnId, keys, value, requestType);
			}

			@Override
			public boolean isReadOnly() {
				return false;
			}

			@Override
			public String toString() {
				var sb = new StringBuilder("MERGE");
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
		 * Merge multiple elements into the specified positions
		 *
		 * @param transactionOrUpdateId transaction id, update id, or 0
		 * @param columnId              column id
		 * @param keys                  multiple lists of column keys
		 * @param values                multiple values
		 * @param requestType           determines whether to return merged values
		 */
		record MergeMulti<T>(long transactionOrUpdateId, long columnId,
						 @NotNull List<Keys> keys,
						 @NotNull List<@NotNull Buf> values,
						 RequestType.RequestMerge<? super Buf, T> requestType) implements RocksDBAPICommandSingle<List<T>> {

			@Override
			public List<T> handleSync(RocksDBSyncAPI api) {
				return api.mergeMulti(transactionOrUpdateId, columnId, keys, values, requestType);
			}

			@Override
			public CompletableFuture<List<T>> handleAsync(RocksDBAsyncAPI api) {
				return api.mergeMultiAsync(transactionOrUpdateId, columnId, keys, values, requestType);
			}

			@Override
			public boolean isReadOnly() {
				return false;
			}

			@Override
			public String toString() {
				var sb = new StringBuilder("MERGE_MULTI");
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
		 * Merge multiple elements using a publisher of batches
		 * @param columnId column id
		 * @param batchPublisher publisher of batches of keys and values
		 * @param mode merge batch mode
		 */
		record MergeBatch(long columnId,
					 @NotNull org.reactivestreams.Publisher<@NotNull KVBatch> batchPublisher,
					 @NotNull MergeBatchMode mode) implements RocksDBAPICommandSingle<Void> {

			@Override
			public Void handleSync(RocksDBSyncAPI api) {
				api.mergeBatch(columnId, batchPublisher, mode);
				return null;
			}

			@Override
			public CompletableFuture<Void> handleAsync(RocksDBAsyncAPI api) {
				return api.mergeBatchAsync(columnId, batchPublisher, mode);
			}

			@Override
			public boolean isReadOnly() {
				return false;
			}

			@Override
			public String toString() {
				var sb = new StringBuilder("MERGE_BATCH");
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

		/**
		 * Scan raw SST files
		 */
		record ScanRaw(long columnId, int shardIndex, int shardCount) implements RocksDBAPICommandStream<SerializedKVBatch> {

			@Override
			public Stream<SerializedKVBatch> handleSync(RocksDBSyncAPI api) {
				return api.scanRaw(columnId, shardIndex, shardCount);
			}

			@Override
			public Publisher<SerializedKVBatch> handleAsync(RocksDBAsyncAPI api) {
				return api.scanRawAsync(columnId, shardIndex, shardCount);
			}

			@Override
			public boolean isReadOnly() {
				return true;
			}
		}

		/**
		 * CDC poll streaming command
		 */
		record CdcPoll(String id,
				   @Nullable Long fromSeq,
				   long maxEvents) implements RocksDBAPICommandStream<CDCEvent> {

			@Override
			public Stream<CDCEvent> handleSync(RocksDBSyncAPI api) {
				return api.cdcPoll(id, fromSeq, maxEvents);
			}

			@Override
			public Publisher<CDCEvent> handleAsync(RocksDBAsyncAPI api) {
				return api.cdcPollAsync(id, fromSeq, maxEvents);
			}

			@Override
			public boolean isReadOnly() {
				return true;
			}
		}
	}
	/**
	 * Flush the database
	 */
	record Flush() implements RocksDBAPICommandSingle<Void> {

		@Override
		public Void handleSync(RocksDBSyncAPI api) {
			api.flush();
			return null;
		}

		@Override
		public CompletableFuture<Void> handleAsync(RocksDBAsyncAPI api) {
			return api.flushAsync();
		}

		@Override
		public boolean isReadOnly() {
			return true;
		}

	}
	/**
	 * Compacts the database
	 */
	record Compact() implements RocksDBAPICommandSingle<Void> {

		@Override
		public Void handleSync(RocksDBSyncAPI api) {
			api.compact();
			return null;
		}

		@Override
		public CompletableFuture<Void> handleAsync(RocksDBAsyncAPI api) {
			return api.compactAsync();
		}

		@Override
		public boolean isReadOnly() {
			return true;
		}

	}
	/**
	 * Returns all column definitions
	 */
	record GetAllColumnDefinitions() implements RocksDBAPICommandSingle<Map<String, ColumnSchema>> {

		@Override
		public Map<String, ColumnSchema> handleSync(RocksDBSyncAPI api) {
			return api.getAllColumnDefinitions();
		}

		@Override
		public CompletableFuture<Map<String, ColumnSchema>> handleAsync(RocksDBAsyncAPI api) {
			return api.getAllColumnDefinitionsAsync();
		}

		@Override
		public boolean isReadOnly() {
			return true;
		}

	}

	/**
	 * Create or update a CDC subscription
	 */
 record CdcCreate(String id, @Nullable Long fromSeq, @Nullable List<Long> columnIds, @Nullable Boolean emitLatestValues) implements RocksDBAPICommandSingle<Long> {

        @Override
        public Long handleSync(RocksDBSyncAPI api) {
            return api.cdcCreate(id, fromSeq, columnIds, emitLatestValues);
        }

        @Override
        public CompletableFuture<Long> handleAsync(RocksDBAsyncAPI api) {
            return api.cdcCreateAsync(id, fromSeq, columnIds, emitLatestValues);
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }
    }

	/**
	 * Delete a CDC subscription
	 */
	record CdcDelete(String id) implements RocksDBAPICommandSingle<Void> {

		@Override
		public Void handleSync(RocksDBSyncAPI api) {
			api.cdcDelete(id);
			return null;
		}

		@Override
		public CompletableFuture<Void> handleAsync(RocksDBAsyncAPI api) {
			return api.cdcDeleteAsync(id);
		}

		@Override
		public boolean isReadOnly() {
			return false;
		}
	}

	/**
	 * Commit CDC sequence for a subscription
	 */
	record CdcCommit(String id, long seq) implements RocksDBAPICommandSingle<Void> {

		@Override
		public Void handleSync(RocksDBSyncAPI api) {
			api.cdcCommit(id, seq);
			return null;
		}

		@Override
		public CompletableFuture<Void> handleAsync(RocksDBAsyncAPI api) {
			return api.cdcCommitAsync(id, seq);
		}

		@Override
		public boolean isReadOnly() {
			return false;
		}
	}
}
