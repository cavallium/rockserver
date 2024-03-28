package it.cavallium.rockserver.core.common;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;

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

	/** See: {@link DeleteColumn}. */
	default void deleteColumn(long columnId) throws RocksDBException {
		requestSync(new DeleteColumn(columnId));
	}

	/** See: {@link GetColumnId}. */
	default long getColumnId(@NotNull String name) throws RocksDBException {
		return requestSync(new GetColumnId(name));
	}

	/** See: {@link Put}. */
	default <T> T put(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			@NotNull MemorySegment @NotNull [] keys,
			@NotNull MemorySegment value,
			RequestPut<? super MemorySegment, T> requestType) throws RocksDBException {
		return requestSync(new Put<>(arena, transactionOrUpdateId, columnId, keys, value, requestType));
	}

	/** See: {@link Get}. */
	default <T> T get(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			@NotNull MemorySegment @NotNull [] keys,
			RequestGet<? super MemorySegment, T> requestType) throws RocksDBException {
		return requestSync(new Get<>(arena, transactionOrUpdateId, columnId, keys, requestType));
	}

	/** See: {@link OpenIterator}. */
	default long openIterator(Arena arena,
			long transactionId,
			long columnId,
			@NotNull MemorySegment @NotNull [] startKeysInclusive,
			@NotNull MemorySegment @Nullable [] endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException {
		return requestSync(new OpenIterator(arena, transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, timeoutMs));
	}

	/** See: {@link CloseIterator}. */
	default void closeIterator(long iteratorId) throws RocksDBException {
		requestSync(new CloseIterator(iteratorId));
	}

	/** See: {@link SeekTo}. */
	default void seekTo(Arena arena, long iterationId, @NotNull MemorySegment @NotNull [] keys) throws RocksDBException {
		requestSync(new SeekTo(arena, iterationId, keys));
	}

	/** See: {@link Subsequent}. */
	default <T> T subsequent(Arena arena,
			long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super MemorySegment, T> requestType) throws RocksDBException {
		return requestSync(new Subsequent<>(arena, iterationId, skipCount, takeCount, requestType));
	}
}
