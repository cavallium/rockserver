package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.CloseFailedUpdate;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.CloseIterator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.CloseTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.CreateColumn;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.DeleteColumn;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Get;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.GetColumnId;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.OpenIterator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.OpenTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Put;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.SeekTo;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Subsequent;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface RocksDBAsyncAPI extends RocksDBAsyncAPIRequestHandler {

	/** See: {@link OpenTransaction}. */
	default CompletionStage<Long> openTransactionAsync(long timeoutMs) throws RocksDBException {
		return requestAsync(new OpenTransaction(timeoutMs));
	}

	/** See: {@link CloseTransaction}. */
	default CompletionStage<Boolean> closeTransactionAsync(long transactionId, boolean commit) throws RocksDBException {
		return requestAsync(new CloseTransaction(transactionId, commit));
	}

	/** See: {@link CloseFailedUpdate}. */
	default CompletionStage<Void> closeFailedUpdateAsync(long updateId) throws RocksDBException {
		return requestAsync(new CloseFailedUpdate(updateId));
	}

	/** See: {@link CreateColumn}. */
	default CompletionStage<Long> createColumnAsync(String name, @NotNull ColumnSchema schema) throws RocksDBException {
		return requestAsync(new CreateColumn(name, schema));
	}

	/** See: {@link DeleteColumn}. */
	default CompletionStage<Void> deleteColumnAsync(long columnId) throws RocksDBException {
		return requestAsync(new DeleteColumn(columnId));
	}

	/** See: {@link GetColumnId}. */
	default CompletionStage<Long> getColumnIdAsync(@NotNull String name) throws RocksDBException {
		return requestAsync(new GetColumnId(name));
	}

	/** See: {@link Put}. */
	default <T> CompletionStage<T> putAsync(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			@NotNull MemorySegment @NotNull [] keys,
			@NotNull MemorySegment value,
			RequestPut<? super MemorySegment, T> requestType) throws RocksDBException {
		return requestAsync(new Put<>(arena, transactionOrUpdateId, columnId, keys, value, requestType));
	}

	/** See: {@link Get}. */
	default <T> CompletionStage<T> getAsync(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			@NotNull MemorySegment @NotNull [] keys,
			RequestGet<? super MemorySegment, T> requestType) throws RocksDBException {
		return requestAsync(new Get<>(arena, transactionOrUpdateId, columnId, keys, requestType));
	}

	/** See: {@link OpenIterator}. */
	default CompletionStage<Long> openIteratorAsync(Arena arena,
			long transactionId,
			long columnId,
			@NotNull MemorySegment @NotNull [] startKeysInclusive,
			@NotNull MemorySegment @Nullable [] endKeysExclusive,
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
	default CompletionStage<Void> closeIteratorAsync(long iteratorId) throws RocksDBException {
		return requestAsync(new CloseIterator(iteratorId));
	}

	/** See: {@link SeekTo}. */
	default CompletionStage<Void> seekToAsync(Arena arena, long iterationId, @NotNull MemorySegment @NotNull [] keys) throws RocksDBException {
		return requestAsync(new SeekTo(arena, iterationId, keys));
	}

	/** See: {@link Subsequent}. */
	default <T> CompletionStage<T> subsequentAsync(Arena arena,
			long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super MemorySegment, T> requestType) throws RocksDBException {
		return requestAsync(new Subsequent<>(arena, iterationId, skipCount, takeCount, requestType));
	}
}
