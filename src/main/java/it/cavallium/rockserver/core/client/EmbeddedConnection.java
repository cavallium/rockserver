package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class EmbeddedConnection extends BaseConnection implements RocksDBAPI {

	private final EmbeddedDB db;
	public static final URI PRIVATE_MEMORY_URL = URI.create("memory://private");
	private final ExecutorService exeuctor;

	public EmbeddedConnection(@Nullable Path path, String name, @Nullable Path embeddedConfig) throws IOException {
		super(name);
		this.db = new EmbeddedDB(path, name, embeddedConfig);
		this.exeuctor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
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
	public <R> R requestSync(RocksDBAPICommand<R> req) {
		return req.handleSync(this);
	}

	@Override
	public <R> CompletableFuture<R> requestAsync(RocksDBAPICommand<R> req) {
		return CompletableFuture.supplyAsync(() -> req.handleSync(this), exeuctor);
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
}
