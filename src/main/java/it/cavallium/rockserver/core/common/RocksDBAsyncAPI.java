package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
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
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.SeekTo;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;

public interface RocksDBAsyncAPI extends RocksDBAsyncAPIRequestHandler {

	/** See: {@link OpenTransaction}. */
	default CompletableFuture<Long> openTransactionAsync(long timeoutMs) throws RocksDBException {
		return requestAsync(new OpenTransaction(timeoutMs));
	}

	/** See: {@link CloseTransaction}. */
	default CompletableFuture<Boolean> closeTransactionAsync(long transactionId, boolean commit) throws RocksDBException {
		return requestAsync(new CloseTransaction(transactionId, commit));
	}

	/** See: {@link CloseFailedUpdate}. */
	default CompletableFuture<Void> closeFailedUpdateAsync(long updateId) throws RocksDBException {
		return requestAsync(new CloseFailedUpdate(updateId));
	}

	/** See: {@link CreateColumn}. */
	default CompletableFuture<Long> createColumnAsync(String name, @NotNull ColumnSchema schema) throws RocksDBException {
		return requestAsync(new CreateColumn(name, schema));
	}

	/** See: {@link DeleteColumn}. */
	default CompletableFuture<Void> deleteColumnAsync(long columnId) throws RocksDBException {
		return requestAsync(new DeleteColumn(columnId));
	}

	/** See: {@link GetColumnId}. */
	default CompletableFuture<Long> getColumnIdAsync(@NotNull String name) throws RocksDBException {
		return requestAsync(new GetColumnId(name));
	}

	/** See: {@link Put}. */
	default <T> CompletableFuture<T> putAsync(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull MemorySegment value,
			RequestPut<? super MemorySegment, T> requestType) throws RocksDBException {
		return requestAsync(new Put<>(arena, transactionOrUpdateId, columnId, keys, value, requestType));
	}

	/** See: {@link PutMulti}. */
	default <T> CompletableFuture<List<T>> putMultiAsync(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			@NotNull List<@NotNull MemorySegment> values,
			RequestPut<? super MemorySegment, T> requestType) throws RocksDBException {
		return requestAsync(new PutMulti<>(arena, transactionOrUpdateId, columnId, keys, values, requestType));
	}

	/** See: {@link PutBatch}. */
	default CompletableFuture<Void> putBatchAsync(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull PutBatchMode mode) throws RocksDBException {
		return requestAsync(new PutBatch(columnId, batchPublisher, mode));
	}

	/** See: {@link Get}. */
	default <T> CompletableFuture<T> getAsync(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			RequestGet<? super MemorySegment, T> requestType) throws RocksDBException {
		return requestAsync(new Get<>(arena, transactionOrUpdateId, columnId, keys, requestType));
	}

	/** See: {@link OpenIterator}. */
	default CompletableFuture<Long> openIteratorAsync(Arena arena,
			long transactionId,
			long columnId,
			@NotNull Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException {
		return requestAsync(new OpenIterator(arena,
				transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				timeoutMs
		));
	}

	/** See: {@link CloseIterator}. */
	default CompletableFuture<Void> closeIteratorAsync(long iteratorId) throws RocksDBException {
		return requestAsync(new CloseIterator(iteratorId));
	}

	/** See: {@link SeekTo}. */
	default CompletableFuture<Void> seekToAsync(Arena arena, long iterationId, @NotNull Keys keys) throws RocksDBException {
		return requestAsync(new SeekTo(arena, iterationId, keys));
	}

	/** See: {@link Subsequent}. */
	default <T> CompletableFuture<T> subsequentAsync(Arena arena,
			long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super MemorySegment, T> requestType) throws RocksDBException {
		return requestAsync(new Subsequent<>(arena, iterationId, skipCount, takeCount, requestType));
	}

	/** See: {@link ReduceRange}. */
	default <T> CompletableFuture<T> reduceRangeAsync(Arena arena,
													  long transactionId,
													  long columnId,
													  @Nullable Keys startKeysInclusive,
													  @Nullable Keys endKeysExclusive,
													  boolean reverse,
													  RequestType.RequestGetRange<? super KV, T> requestType,
													  long timeoutMs) throws RocksDBException {
		return requestAsync(new ReduceRange<>(arena,
				transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				requestType,
				timeoutMs
		));
	}

	/** See: {@link GetRange}. */
	default <T> Publisher<T> getRangeAsync(Arena arena,
										   long transactionId,
										   long columnId,
										   @Nullable Keys startKeysInclusive,
										   @Nullable Keys endKeysExclusive,
										   boolean reverse,
										   RequestType.RequestGetRange<? super KV, T> requestType,
										   long timeoutMs) throws RocksDBException {
		throw RocksDBException.of(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
				"GetRangeStream is not implemented");
	}
}
