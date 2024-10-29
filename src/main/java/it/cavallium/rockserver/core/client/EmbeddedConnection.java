package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.InternalConnection;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;

public class EmbeddedConnection extends BaseConnection implements RocksDBAPI, InternalConnection {

	private final EmbeddedDB db;
	public static final URI PRIVATE_MEMORY_URL = URI.create("memory://private");

	public EmbeddedConnection(@Nullable Path path, String name, @Nullable Path embeddedConfig) throws IOException {
		super(name);
		this.db = new EmbeddedDB(path, name, embeddedConfig);
	}

	@Override
	public void close() throws IOException {
		db.close();
		super.close();
	}

	@Override
	public URI getUrl() {
		return Optional.ofNullable(db.getPath()).map(Path::toUri).orElse(PRIVATE_MEMORY_URL);
	}

	@Override
	public RocksDBSyncAPI getSyncApi() {
		return this;
	}

	@Override
	public RocksDBAsyncAPI getAsyncApi() {
		return this;
	}

	@Override
	public long openTransaction(long timeoutMs) {
		return db.openTransaction(timeoutMs);
	}

	@Override
	public boolean closeTransaction(long transactionId, boolean commit) {
		return db.closeTransaction(transactionId, commit);
	}

	@Override
	public void closeFailedUpdate(long updateId) throws RocksDBException {
		db.closeFailedUpdate(updateId);
	}

	@Override
	public long createColumn(String name, @NotNull ColumnSchema schema) {
		return db.createColumn(name, schema);
	}

	@Override
	public void deleteColumn(long columnId) throws RocksDBException {
		db.deleteColumn(columnId);
	}

	@Override
	public long getColumnId(@NotNull String name) {
		return db.getColumnId(name);
	}

	@Override
	public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> req) {
		return req.handleSync(this);
	}

	@SuppressWarnings("unchecked")
    @Override
	public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> req) {
		return (RA) switch (req) {
			case RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch putBatch -> this.putBatchAsync(putBatch.columnId(), putBatch.batchPublisher(), putBatch.mode());
			case RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?> getRange -> this.getRangeAsync(getRange.arena(), getRange.transactionId(), getRange.columnId(), getRange.startKeysInclusive(), getRange.endKeysExclusive(), getRange.reverse(), getRange.requestType(), getRange.timeoutMs());
			case RocksDBAPICommand.RocksDBAPICommandSingle<?> _ -> CompletableFuture.supplyAsync(() -> req.handleSync(this),
					(req.isReadOnly() ? db.getScheduler().readExecutor() : db.getScheduler().writeExecutor()));
			case RocksDBAPICommand.RocksDBAPICommandStream<?> _ -> throw RocksDBException.of(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, "The request of type " + req.getClass().getName() + " is not implemented in class " + this.getClass().getName());
		};
	}

	@Override
	public <T> T put(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull MemorySegment value,
			RequestPut<? super MemorySegment, T> requestType) throws RocksDBException {
		return db.put(arena, transactionOrUpdateId, columnId, keys, value, requestType);
	}

	@Override
	public <T> List<T> putMulti(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull MemorySegment> values,
			RequestPut<? super MemorySegment, T> requestType) throws RocksDBException {
		return db.putMulti(arena, transactionOrUpdateId, columnId, keys, values, requestType);
	}

	@Override
	public CompletableFuture<Void> putBatchAsync(long columnId,
												 @NotNull Publisher<@NotNull KVBatch> batchPublisher,
												 @NotNull PutBatchMode mode) throws RocksDBException {
		return db.putBatchInternal(columnId, batchPublisher, mode);
	}

	@Override
	public void putBatch(long columnId,
						 @NotNull Publisher<@NotNull KVBatch> batchPublisher,
						 @NotNull PutBatchMode mode) throws RocksDBException {
		db.putBatch(columnId, batchPublisher, mode);
	}

	@Override
	public <T> T get(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			Keys keys,
			RequestGet<? super MemorySegment, T> requestType) throws RocksDBException {
		return db.get(arena, transactionOrUpdateId, columnId, keys, requestType);
	}

	@Override
	public long openIterator(Arena arena,
			long transactionId,
			long columnId,
			@NotNull Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException {
		return db.openIterator(arena, transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, timeoutMs);
	}

	@Override
	public void closeIterator(long iteratorId) throws RocksDBException {
		db.closeIterator(iteratorId);
	}

	@Override
	public void seekTo(Arena arena, long iterationId, Keys keys) throws RocksDBException {
		db.seekTo(arena, iterationId, keys);
	}

	@Override
	public <T> T subsequent(Arena arena,
			long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super MemorySegment, T> requestType) throws RocksDBException {
		return db.subsequent(arena, iterationId, skipCount, takeCount, requestType);
	}

	@Override
	public <T> T reduceRange(Arena arena, long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.@NotNull RequestReduceRange<? super KV, T> requestType, long timeoutMs) throws RocksDBException {
		return db.reduceRange(arena, transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs);
	}

	@Override
	public <T> Stream<T> getRange(Arena arena, long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.@NotNull RequestGetRange<? super KV, T> requestType, long timeoutMs) throws RocksDBException {
		return db.getRange(arena, transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs);
	}

	@Override
	public <T> Publisher<T> getRangeAsync(Arena arena, long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.RequestGetRange<? super KV, T> requestType, long timeoutMs) throws RocksDBException {
		return db.getRangeAsyncInternal(arena, transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs);
	}

	@Override
	public RWScheduler getScheduler() {
		return db.getScheduler();
	}
}
