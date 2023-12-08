package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.Callback.GetCallback;
import it.cavallium.rockserver.core.common.Callback.IteratorCallback;
import it.cavallium.rockserver.core.common.Callback.PutCallback;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.net.URI;
import java.nio.file.Path;
import java.util.Optional;

import org.jetbrains.annotations.Nullable;

public class EmbeddedConnection extends BaseConnection {

	private final EmbeddedDB db;
	public static final URI PRIVATE_MEMORY_URL = URI.create("memory://private");

	public EmbeddedConnection(@Nullable Path path, String name, @Nullable Path embeddedConfig) {
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
	public long openTransaction(long timeoutMs) {
		return db.openTransaction(timeoutMs);
	}

	@Override
	public boolean closeTransaction(long transactionId, boolean commit) {
		return db.closeTransaction(transactionId, commit);
	}

	@Override
	public long createColumn(String name, ColumnSchema schema) {
		return db.createColumn(name, schema);
	}

	@Override
	public void deleteColumn(long columnId) throws RocksDBException {
		db.deleteColumn(columnId);
	}

	@Override
	public long getColumnId(String name) {
		return db.getColumnId(name);
	}

	@Override
	public <T> T put(Arena arena,
			long transactionId,
			long columnId,
			MemorySegment[] keys,
			@Nullable MemorySegment value,
			PutCallback<? super MemorySegment, T> callback) throws RocksDBException {
		return db.put(arena, transactionId, columnId, keys, value, callback);
	}

	@Override
	public <T> T get(Arena arena,
			long transactionId,
			long columnId,
			MemorySegment[] keys,
			GetCallback<? super MemorySegment, T> callback) throws RocksDBException {
		return db.get(arena, transactionId, columnId, keys, callback);
	}

	@Override
	public long openIterator(Arena arena,
			long transactionId,
			long columnId,
			MemorySegment[] startKeysInclusive,
			@Nullable MemorySegment[] endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException {
		return db.openIterator(arena, transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, timeoutMs);
	}

	@Override
	public void closeIterator(long iteratorId) throws RocksDBException {
		db.closeIterator(iteratorId);
	}

	@Override
	public void seekTo(Arena arena, long iterationId, MemorySegment[] keys) throws RocksDBException {
		db.seekTo(arena, iterationId, keys);
	}

	@Override
	public <T> T subsequent(Arena arena,
			long iterationId,
			long skipCount,
			long takeCount,
			IteratorCallback<? super MemorySegment, T> callback) throws RocksDBException {
		return db.subsequent(arena, iterationId, skipCount, takeCount, callback);
	}
}
