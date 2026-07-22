package it.cavallium.rockserver.core.common;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestMerge;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RequestType.RequestDelete;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Compact;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Flush;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.GetAllColumnDefinitions;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CheckMergeOperator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CreateColumn;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteColumn;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteColumnIfExists;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Delete;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteRange;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.EstimateNumKeys;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.ExistsMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Get;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.GetColumnId;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Put;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Merge;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.SeekTo;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.UploadMergeOperator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream.GetRange;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream.ScanRaw;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface RocksDBSyncAPI extends RocksDBSyncAPIRequestHandler {

	/** See: {@link OpenTransaction}. */
	default long openTransaction(long timeoutMs) throws RocksDBException {
		return requestSync(new OpenTransaction(timeoutMs));
	}

	/** See: {@link CloseTransaction}. */
	default boolean closeTransaction(long transactionId, boolean commit) throws RocksDBException {
		return requestSync(new CloseTransaction(transactionId, commit));
	}

	/** Close a transaction, classifying commit work when {@code commit} is true. */
	default boolean closeTransaction(long transactionId, boolean commit, @NotNull WriteClass writeClass)
			throws RocksDBException {
		return requestSync(new CloseTransaction(transactionId, commit, writeClass));
	}

	/** See: {@link CloseFailedUpdate}. */
	default void closeFailedUpdate(long updateId) throws RocksDBException {
		requestSync(new CloseFailedUpdate(updateId));
	}

	/** See: {@link CreateColumn}. */
	default long createColumn(String name, @NotNull ColumnSchema schema) throws RocksDBException {
		return requestSync(new CreateColumn(name, schema));
	}

	default long createColumn(String name, @NotNull ColumnSchema schema, @NotNull WriteClass writeClass)
			throws RocksDBException {
		return requestSync(new CreateColumn(name, schema, writeClass));
	}

	/** See: {@link UploadMergeOperator}. */
	default long uploadMergeOperator(String name, String className, byte[] jarData) throws RocksDBException {
		return requestSync(new UploadMergeOperator(name, className, jarData));
	}

	default Long checkMergeOperator(String name, byte[] hash) throws RocksDBException {
		return requestSync(new CheckMergeOperator(name, hash));
	}

	default long ensureMergeOperator(String name, String className, byte[] jarData) throws RocksDBException {
		byte[] hash;
		try {
			java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
			hash = digest.digest(jarData);
		} catch (java.security.NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
		Long existing = checkMergeOperator(name, hash);
		if (existing != null) {
			return existing;
		}
		return uploadMergeOperator(name, className, jarData);
	}

	/** See: {@link DeleteColumn}. */
	default void deleteColumn(long columnId) throws RocksDBException {
		requestSync(new DeleteColumn(columnId));
	}

	default void deleteColumn(long columnId, @NotNull WriteClass writeClass) throws RocksDBException {
		requestSync(new DeleteColumn(columnId, writeClass));
	}

	/** Atomically delete a column by name when it exists. */
	default boolean deleteColumnIfExists(@NotNull String name) throws RocksDBException {
		return requestSync(new DeleteColumnIfExists(name));
	}

	default boolean deleteColumnIfExists(@NotNull String name, @NotNull WriteClass writeClass)
			throws RocksDBException {
		return requestSync(new DeleteColumnIfExists(name, writeClass));
	}

	/** See: {@link GetColumnId}. */
	default long getColumnId(@NotNull String name) throws RocksDBException {
		return requestSync(new GetColumnId(name));
	}

	/**
	 * Return RocksDB's unbounded estimate of physical keys in a column.
	 *
	 * <p>Unlike {@link #reduceRange(long, long, Keys, Keys, boolean,
	 * RequestType.RequestReduceRange, long)} with {@link RequestType#entriesCount()}, this method
	 * is approximate, ignores transaction-local state, and cannot be bounded. For bucketed columns,
	 * it estimates physical buckets rather than logical entries.</p>
	 */
	default long estimateNumKeys(long columnId) throws RocksDBException {
		return requestSync(new EstimateNumKeys(columnId));
	}

	/** See: {@link Put}. */
	default <T> T put(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		return requestSync(new Put<>(transactionOrUpdateId, columnId, keys, value, requestType));
	}

	default <T> T put(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, T> requestType,
			@NotNull WriteClass writeClass) throws RocksDBException {
		return requestSync(new Put<>(transactionOrUpdateId, columnId, keys, value, requestType, writeClass));
	}

	/** See: {@link Delete}. */
	default <T> T delete(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		return requestSync(new Delete<>(transactionOrUpdateId, columnId, keys, requestType));
	}

	default <T> T delete(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			RequestDelete<? super Buf, T> requestType,
			@NotNull WriteClass writeClass) throws RocksDBException {
		return requestSync(new Delete<>(transactionOrUpdateId, columnId, keys, requestType, writeClass));
	}

	/** See: {@link DeleteMulti}. */
	default <T> List<T> deleteMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		return requestSync(new DeleteMulti<>(transactionOrUpdateId, columnId, keys, requestType));
	}

	default <T> List<T> deleteMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			RequestDelete<? super Buf, T> requestType,
			@NotNull WriteClass writeClass) throws RocksDBException {
		return requestSync(new DeleteMulti<>(transactionOrUpdateId, columnId, keys, requestType, writeClass));
	}

	/** See: {@link DeleteRange}. */
	default void deleteRange(long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive) throws RocksDBException {
		requestSync(new DeleteRange(columnId, startKeysInclusive, endKeysExclusive));
	}

	default void deleteRange(long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			@NotNull WriteClass writeClass) throws RocksDBException {
		requestSync(new DeleteRange(columnId, startKeysInclusive, endKeysExclusive, writeClass));
	}

	/** See: {@link Merge}. */
	default <T> T merge(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			@NotNull Buf value,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		return requestSync(new Merge<>(transactionOrUpdateId, columnId, keys, value, requestType));
	}

	default <T> T merge(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			@NotNull Buf value,
			RequestMerge<? super Buf, T> requestType,
			@NotNull WriteClass writeClass) throws RocksDBException {
		return requestSync(new Merge<>(transactionOrUpdateId, columnId, keys, value, requestType, writeClass));
	}

	/** See: {@link PutMulti}. */
	default <T> List<T> putMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		return requestSync(new PutMulti<>(transactionOrUpdateId, columnId, keys, values, requestType));
	}

	default <T> List<T> putMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestPut<? super Buf, T> requestType,
			@NotNull WriteClass writeClass) throws RocksDBException {
		return requestSync(new PutMulti<>(transactionOrUpdateId, columnId, keys, values, requestType, writeClass));
	}

	/** See: {@link MergeMulti}. */
	default <T> List<T> mergeMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		return requestSync(new MergeMulti<>(transactionOrUpdateId, columnId, keys, values, requestType));
	}

	default <T> List<T> mergeMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestMerge<? super Buf, T> requestType,
			@NotNull WriteClass writeClass) throws RocksDBException {
		return requestSync(new MergeMulti<>(transactionOrUpdateId, columnId, keys, values, requestType, writeClass));
	}

	/** See: {@link PutBatch}. */
	default void putBatch(long columnId,
					  @NotNull org.reactivestreams.Publisher<@NotNull KVBatch> batchPublisher,
					  @NotNull PutBatchMode mode) throws RocksDBException {
		requestSync(new PutBatch(columnId, batchPublisher, mode));
	}

	default void putBatch(long columnId,
			@NotNull org.reactivestreams.Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull PutBatchMode mode,
			@NotNull WriteClass writeClass) throws RocksDBException {
		requestSync(new PutBatch(columnId, batchPublisher, mode, writeClass));
	}

	/** See: {@link MergeBatch}. */
	default void mergeBatch(long columnId,
				   @NotNull org.reactivestreams.Publisher<@NotNull KVBatch> batchPublisher,
				   @NotNull MergeBatchMode mode) throws RocksDBException {
		requestSync(new MergeBatch(columnId, batchPublisher, mode));
	}

	default void mergeBatch(long columnId,
			@NotNull org.reactivestreams.Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull MergeBatchMode mode,
			@NotNull WriteClass writeClass) throws RocksDBException {
		requestSync(new MergeBatch(columnId, batchPublisher, mode, writeClass));
	}

	/** See: {@link Get}. */
	default <T> T get(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			RequestGet<? super Buf, T> requestType) throws RocksDBException {
		return requestSync(new Get<>(transactionOrUpdateId, columnId, keys, requestType));
	}

	/** See: {@link ExistsMulti}. */
	default List<Boolean> existsMulti(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			long timeoutMs) throws RocksDBException {
		return requestSync(new ExistsMulti(transactionId, columnId, keys, timeoutMs));
	}

	/** See: {@link OpenIterator}. */
	default long openIterator(long transactionId,
			long columnId,
			Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException {
		return requestSync(new OpenIterator(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, timeoutMs));
	}

	/** See: {@link CloseIterator}. */
	default void closeIterator(long iteratorId) throws RocksDBException {
		requestSync(new CloseIterator(iteratorId));
	}

	/** See: {@link SeekTo}. */
	default void seekTo(long iterationId, Keys keys) throws RocksDBException {
		requestSync(new SeekTo(iterationId, keys));
	}

	/** See: {@link Subsequent}. */
	default <T> T subsequent(long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super Buf, T> requestType) throws RocksDBException {
		return requestSync(new Subsequent<>(iterationId, skipCount, takeCount, requestType));
	}

	/** See: {@link ReduceRange}. */
	default <T> T reduceRange(long transactionId,
							  long columnId,
							  @Nullable Keys startKeysInclusive,
							  @Nullable Keys endKeysExclusive,
							  boolean reverse,
							  @NotNull RequestType.RequestReduceRange<? super KV, T> requestType,
							  long timeoutMs) throws RocksDBException {
		return requestSync(new ReduceRange<>(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs));
	}

	/** See: {@link GetRange}. */
	default <T> Stream<T> getRange(long transactionId,
								   long columnId,
								   @Nullable Keys startKeysInclusive,
								   @Nullable Keys endKeysExclusive,
								   boolean reverse,
								   @NotNull RequestType.RequestGetRange<? super KV, T> requestType,
								   long timeoutMs) throws RocksDBException {
		return requestSync(new GetRange<>(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs));
	}

	/** See: {@link ScanRaw}. */
	default Stream<SerializedKVBatch> scanRaw(long columnId, int shardIndex, int shardCount) {
		return requestSync(new ScanRaw(columnId, shardIndex, shardCount));
	}

	/** See: {@link Flush}. */
	default void flush() {
		requestSync(new Flush());
	}

	/** See: {@link Compact}. */
	default void compact() {
		requestSync(new Compact());
	}

	/** See: {@link GetAllColumnDefinitions}. */
	default Map<String, ColumnSchema> getAllColumnDefinitions() throws RocksDBException {
		return requestSync(new GetAllColumnDefinitions());
	}

    // CDC API
    /** Create or update a CDC subscription. Returns the start sequence. */
    default long cdcCreate(@NotNull String id, @Nullable Long fromSeq, @Nullable List<Long> columnIds) throws RocksDBException {
        return cdcCreate(id, fromSeq, columnIds, null, null);
    }

    /** Create or update a CDC subscription. Returns the start sequence. */
    default long cdcCreate(@NotNull String id, @Nullable Long fromSeq, @Nullable List<Long> columnIds, @Nullable Boolean emitLatestValues) throws RocksDBException {
        return cdcCreate(id, fromSeq, columnIds, emitLatestValues, null);
    }

    /**
     * Atomically create or update a CDC subscription if its durable checkpoint still matches.
     * {@code null} disables the precondition, empty requires absence, and a present value requires an exact match.
     */
    default long cdcCreate(@NotNull String id,
                           @Nullable Long fromSeq,
                           @Nullable List<Long> columnIds,
                           @Nullable Boolean emitLatestValues,
                           @Nullable OptionalLong expectedLastCommitted) throws RocksDBException {
        return requestSync(new RocksDBAPICommand.CdcCreate(
                id, fromSeq, columnIds, emitLatestValues, expectedLastCommitted));
    }

    /** Delete a CDC subscription */
    default void cdcDelete(@NotNull String id) throws RocksDBException {
        requestSync(new RocksDBAPICommand.CdcDelete(id));
    }

	/** Return the earliest CDC cursor still available in this database's WAL. */
	default long cdcGetEarliestAvailableSequence() throws RocksDBException {
		return requestSync(new RocksDBAPICommand.CdcGetEarliestAvailableSequence());
	}

    /** Return the durable last committed sequence for a CDC subscription, if it exists. */
    default OptionalLong cdcGetLastCommittedSequence(@NotNull String id) throws RocksDBException {
        return requestSync(new RocksDBAPICommand.CdcGetLastCommittedSequence(id));
    }

    /** Commit the last processed CDC sequence for a subscription */
    default void cdcCommit(@NotNull String id, long seq) throws RocksDBException {
        requestSync(new RocksDBAPICommand.CdcCommit(id, seq));
    }

    /** Poll CDC events as a blocking Stream */
    default Stream<CDCEvent> cdcPoll(@NotNull String id, @Nullable Long fromSeq, long maxEvents) throws RocksDBException {
        return requestSync(new RocksDBAPICommand.RocksDBAPICommandStream.CdcPoll(id, fromSeq, maxEvents));
    }
}
