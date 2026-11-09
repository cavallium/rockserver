package it.cavallium.rockserver.core.common;

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
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.GetRangePage;
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
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream.ScanRawResumable;
import it.cavallium.buffer.Buf;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;

/**
 * The asynchronous API.
 *
 * <p><b>Write order.</b> Writes that are not awaited have no relative order, on one node or replicated: a put sent
 * before an earlier put to the same key completed may be applied first, by a single-node server (whose write pool
 * runs requests in parallel), by a replicated node (which runs each call on a thread of its own), or by a client
 * session that resends a write whose outcome was unknown. Order comes from awaiting each write before sending the
 * next, from an update of the key ({@code getForUpdate}, whose updates of one key run one at a time), a transaction,
 * or a batch ({@code putBatch} applies its chunks in order).</p>
 */
public interface RocksDBAsyncAPI extends RocksDBAsyncAPIRequestHandler {

	/** See: {@link OpenTransaction}. */
	default CompletableFuture<Long> openTransactionAsync(Duration transactionLeaseTtl) throws RocksDBException {
		return requestAsync(new OpenTransaction(transactionLeaseTtl));
	}

	/** See: {@link CloseTransaction}. */
	default CompletableFuture<Boolean> closeTransactionAsync(long transactionId, boolean commit) throws RocksDBException {
		return requestAsync(new CloseTransaction(transactionId, commit));
	}

	/** Close a transaction, classifying commit work when {@code commit} is true. */
	/** See: {@link CloseFailedUpdate}. */
	default CompletableFuture<Void> closeFailedUpdateAsync(long updateId) throws RocksDBException {
		return requestAsync(new CloseFailedUpdate(updateId));
	}

	/** See: {@link CreateColumn}. */
	default CompletableFuture<Long> createColumnAsync(String name, @NotNull ColumnSchema schema) throws RocksDBException {
		return requestAsync(new CreateColumn(name, schema));
	}

	/** See: {@link UploadMergeOperator}. */
	default CompletableFuture<Long> uploadMergeOperatorAsync(String name, String className, byte[] jarData) throws RocksDBException {
		return requestAsync(new UploadMergeOperator(name, className, jarData));
	}

	default CompletableFuture<Long> checkMergeOperatorAsync(String name, byte[] hash) {
		return requestAsync(new CheckMergeOperator(name, hash));
	}

	/**
	 * Uploads the merge operator unless one with the same jar is there ({@link #checkMergeOperatorAsync}, then
	 * {@link #uploadMergeOperatorAsync}), and answers its version. The future's {@code cancel} reaches the call in flight,
	 * as a single call's does (R48-3): before the upload is made it stops the check and nothing is uploaded; after, it is
	 * the upload's own cancel, so an upload whose copy was sent with client sessions refuses it and ends OUTCOME_UNKNOWN.
	 */
	default CompletableFuture<Long> ensureMergeOperatorAsync(String name, String className, byte[] jarData) {
		byte[] hash;
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			hash = digest.digest(jarData);
		} catch (NoSuchAlgorithmException e) {
			return CompletableFuture.failedFuture(e);
		}
		return ChainedCall.of(checkMergeOperatorAsync(name, hash), existing -> existing != null
				? CompletableFuture.completedFuture(existing)
				: uploadMergeOperatorAsync(name, className, jarData));
	}

	/** See: {@link DeleteColumn}. */
	default CompletableFuture<Void> deleteColumnAsync(long columnId) throws RocksDBException {
		return requestAsync(new DeleteColumn(columnId));
	}

	/** Atomically delete a column by name when it exists. */
	default CompletableFuture<Boolean> deleteColumnIfExistsAsync(@NotNull String name) throws RocksDBException {
		return requestAsync(new DeleteColumnIfExists(name));
	}

	/** See: {@link GetColumnId}. */
	default CompletableFuture<Long> getColumnIdAsync(@NotNull String name) throws RocksDBException {
		return requestAsync(new GetColumnId(name));
	}

    /** See {@link RocksDBSyncAPI#getTableProperties(long)}. */
    default CompletableFuture<ColumnTableProperties> getTablePropertiesAsync(long columnId) {
        return requestAsync(new RocksDBAPICommand.GetTableProperties(columnId));
    }

	/**
	 * Asynchronously return RocksDB's unbounded estimate of physical keys in a column.
	 * See {@link RocksDBSyncAPI#estimateNumKeys(long)} for its deliberately approximate semantics.
	 */

	default CompletableFuture<Long> estimateNumKeysAsync(long columnId) throws RocksDBException {
		return requestAsync(new EstimateNumKeys(columnId));
	}

	/** See: {@link Put}. */
	default <T> CompletableFuture<T> putAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		return requestAsync(new Put<>(transactionOrUpdateId, columnId, keys, value, requestType));
	}

	/** See: {@link Delete}. */
	default <T> CompletableFuture<T> deleteAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		return requestAsync(new Delete<>(transactionOrUpdateId, columnId, keys, requestType));
	}

	/** See: {@link DeleteMulti}. */
	default <T> CompletableFuture<List<T>> deleteMultiAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		return requestAsync(new DeleteMulti<>(transactionOrUpdateId, columnId, keys, requestType));
	}

	/** See: {@link DeleteRange}. */
	default CompletableFuture<Void> deleteRangeAsync(long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive) throws RocksDBException {
		return requestAsync(new DeleteRange(columnId, startKeysInclusive, endKeysExclusive));
	}

	/** See: {@link Merge}. */
	default <T> CompletableFuture<T> mergeAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		return requestAsync(new Merge<>(transactionOrUpdateId, columnId, keys, value, requestType));
	}

	/** See: {@link PutMulti}. */
	default <T> CompletableFuture<List<T>> putMultiAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		return requestAsync(new PutMulti<>(transactionOrUpdateId, columnId, keys, values, requestType));
	}

	/** See: {@link MergeMulti}. */
	default <T> CompletableFuture<List<T>> mergeMultiAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		return requestAsync(new MergeMulti<>(transactionOrUpdateId, columnId, keys, values, requestType));
	}

	/** See: {@link PutBatch}. */
	default CompletableFuture<Void> putBatchAsync(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull PutBatchMode mode) throws RocksDBException {
		return requestAsync(new PutBatch(columnId, batchPublisher, mode));
	}

	/** See: {@link MergeBatch}. */
	default CompletableFuture<Void> mergeBatchAsync(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull MergeBatchMode mode) throws RocksDBException {
		return requestAsync(new MergeBatch(columnId, batchPublisher, mode));
	}

	/** See: {@link Get}. */
	default <T> CompletableFuture<T> getAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			RequestGet<? super Buf, T> requestType) throws RocksDBException {
		return requestAsync(new Get<>(transactionOrUpdateId, columnId, keys, requestType));
	}

	/** Read an ordered, bounded prefix of current values without a transaction.
	 * See {@link MultiGetPage} for continuation, limits, and physical bucket accounting. */
	default CompletableFuture<MultiGetPage> getMultiAsync(long columnId, List<Keys> keys, RangeBudget budget) {
		return requestAsync(new RocksDBAPICommand.RocksDBAPICommandSingle.GetMulti(columnId, keys, budget));
	}

	/** See: {@link ExistsMulti}. */
	default CompletableFuture<List<Boolean>> existsMultiAsync(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys) throws RocksDBException {
		return requestAsync(new ExistsMulti(transactionId, columnId, keys));
	}

	/** See: {@link OpenIterator}. */
	default CompletableFuture<Long> openIteratorAsync(long transactionId,
			long columnId,
			@NotNull Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			Duration iteratorLeaseTtl) throws RocksDBException {
		return requestAsync(new OpenIterator(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				iteratorLeaseTtl
		));
	}

	/** See: {@link CloseIterator}. */
	default CompletableFuture<Void> closeIteratorAsync(long iteratorId) throws RocksDBException {
		return requestAsync(new CloseIterator(iteratorId));
	}

	/** See: {@link SeekTo}. */
	default CompletableFuture<Void> seekToAsync(long iterationId, @NotNull Keys keys) throws RocksDBException {
		return requestAsync(new SeekTo(iterationId, keys));
	}

	/** See: {@link Subsequent}. */
	default <T> CompletableFuture<T> subsequentAsync(long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super Buf, T> requestType) throws RocksDBException {
		return requestAsync(new Subsequent<>(iterationId, skipCount, takeCount, requestType));
	}

	/** See: {@link ReduceRange}. */
	default <T> CompletableFuture<T> reduceRangeAsync(long transactionId,
													  long columnId,
													  @Nullable Keys startKeysInclusive,
													  @Nullable Keys endKeysExclusive,
												  boolean reverse,
												  RequestType.RequestReduceRange<? super KV, T> requestType) throws RocksDBException {
		return requestAsync(new ReduceRange<>(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				requestType
		));
	}

	/** Return one bounded range page, excluding {@code resumeAfter} in either direction. */
	default <T> CompletableFuture<RangePage<T>> getRangePageAsync(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@Nullable Keys resumeAfter,
			@NotNull RequestType.RequestGetRange<? super KV, T> requestType,
			@NotNull RangeBudget budget) throws RocksDBException {
		return requestAsync(new GetRangePage<>(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				resumeAfter,
				requestType,
				budget));
	}

	/** See: {@link GetRange}. */
	default <T> Publisher<T> getRangeAsync(long transactionId,
										   long columnId,
										   @Nullable Keys startKeysInclusive,
										   @Nullable Keys endKeysExclusive,
									   boolean reverse,
									   RequestType.RequestGetRange<? super KV, T> requestType) throws RocksDBException {
		return requestAsync(new GetRange<>(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				requestType
		));
	}

	/** See: {@link ScanRaw}. */
	default Publisher<SerializedKVBatch> scanRawAsync(long columnId, int shardIndex, int shardCount) {
		return requestAsync(new ScanRaw(columnId, shardIndex, shardCount));
	}

	/**
	 * Scan raw SSTs and emit a completion token after each fully emitted file.
	 * A non-empty SST carries its token on its final {@link RawScanEvent.Batch};
	 * an SST with no point-data batch emits {@link RawScanEvent.SstCompleted}.
	 * Tokens acknowledged in {@code completedSsts} skip only an exact SST that is
	 * still live in the same database.
	 */
	default Publisher<RawScanEvent> scanRawResumableAsync(long columnId,
			int shardIndex,
			int shardCount,
			Set<RawSstToken> completedSsts) {
		return requestAsync(new ScanRawResumable(columnId, shardIndex, shardCount, completedSsts));
	}

    /** See: {@link RocksDBSyncAPI#getSstMetadata(long, int)}. */
    default CompletableFuture<SstMaintenance.Metadata> getSstMetadataAsync(long columnId, int level) {
        return requestAsync(new RocksDBAPICommand.GetSstMetadata(columnId, level));
    }
    default CompletableFuture<SstMaintenance.Result> compactFilesAsync(SstMaintenance.Request request) {
        return requestAsync(new RocksDBAPICommand.CompactFiles(request));
    }

	/** See: {@link Flush}. */
	default CompletableFuture<Void> flushAsync() {
		return requestAsync(new Flush());
	}

	/** Sync the WAL to stable storage. Older implementations safely fall back to a full flush. */
	default CompletableFuture<Void> flushWalAsync() {
		return flushAsync();
	}

	/** See: {@link Compact}. */
	default CompletableFuture<Void> compactAsync() {
		return requestAsync(new Compact());
	}

	/** See: {@link GetAllColumnDefinitions}. */
	default CompletableFuture<Map<String, ColumnSchema>> getAllColumnDefinitionsAsync() {
		return requestAsync(new GetAllColumnDefinitions());
	}
}
