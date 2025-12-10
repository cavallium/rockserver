package it.cavallium.rockserver.core.common;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestMerge;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Compact;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Flush;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.GetAllColumnDefinitions;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CreateColumn;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteColumn;
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
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import java.util.List;
import java.util.Map;
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

	/** See: {@link CloseFailedUpdate}. */
	default void closeFailedUpdate(long updateId) throws RocksDBException {
		requestSync(new CloseFailedUpdate(updateId));
	}

	/** See: {@link CreateColumn}. */
	default long createColumn(String name, @NotNull ColumnSchema schema) throws RocksDBException {
		return requestSync(new CreateColumn(name, schema));
	}

	/** See: {@link UploadMergeOperator}. */
	default long uploadMergeOperator(String name, String className, byte[] jarData) throws RocksDBException {
		return requestSync(new UploadMergeOperator(name, className, jarData));
	}

	/** See: {@link DeleteColumn}. */
	default void deleteColumn(long columnId) throws RocksDBException {
		requestSync(new DeleteColumn(columnId));
	}

	/** See: {@link GetColumnId}. */
	default long getColumnId(@NotNull String name) throws RocksDBException {
		return requestSync(new GetColumnId(name));
	}

	/** See: {@link Put}. */
	default <T> T put(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		return requestSync(new Put<>(transactionOrUpdateId, columnId, keys, value, requestType));
	}

	/** See: {@link Merge}. */
	default <T> T merge(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			@NotNull Buf value,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		return requestSync(new Merge<>(transactionOrUpdateId, columnId, keys, value, requestType));
	}

	/** See: {@link PutMulti}. */
	default <T> List<T> putMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		return requestSync(new PutMulti<>(transactionOrUpdateId, columnId, keys, values, requestType));
	}

	/** See: {@link MergeMulti}. */
	default <T> List<T> mergeMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		return requestSync(new MergeMulti<>(transactionOrUpdateId, columnId, keys, values, requestType));
	}

	/** See: {@link PutBatch}. */
	default void putBatch(long columnId,
					  @NotNull org.reactivestreams.Publisher<@NotNull KVBatch> batchPublisher,
					  @NotNull PutBatchMode mode) throws RocksDBException {
		requestSync(new PutBatch(columnId, batchPublisher, mode));
	}

	/** See: {@link MergeBatch}. */
	default void mergeBatch(long columnId,
				   @NotNull org.reactivestreams.Publisher<@NotNull KVBatch> batchPublisher,
				   @NotNull MergeBatchMode mode) throws RocksDBException {
		requestSync(new MergeBatch(columnId, batchPublisher, mode));
	}

	/** See: {@link Get}. */
	default <T> T get(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			RequestGet<? super Buf, T> requestType) throws RocksDBException {
		return requestSync(new Get<>(transactionOrUpdateId, columnId, keys, requestType));
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
        return requestSync(new RocksDBAPICommand.CdcCreate(id, fromSeq, columnIds));
    }

    /** Delete a CDC subscription */
    default void cdcDelete(@NotNull String id) throws RocksDBException {
        requestSync(new RocksDBAPICommand.CdcDelete(id));
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
