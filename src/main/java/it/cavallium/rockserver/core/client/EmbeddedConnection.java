package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.Callback.GetCallback;
import it.cavallium.rockserver.core.common.Callback.IteratorCallback;
import it.cavallium.rockserver.core.common.Callback.PutCallback;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.net.URI;
import java.nio.file.Path;
import org.jetbrains.annotations.Nullable;

public class EmbeddedConnection extends BaseConnection {

	private final EmbeddedDB db;

	public EmbeddedConnection(Path path, String name, Path embeddedConfig) {
		super(name);
		this.db = new EmbeddedDB(path, embeddedConfig);
	}

	@Override
	public void close() throws IOException {
		db.close();
		super.close();
	}

	@Override
	public URI getUrl() {
		return db.getPath().toUri();
	}

	@Override
	public long openTransaction(long timeoutMs) {
		return db.openTransaction(timeoutMs);
	}

	@Override
	public void closeTransaction(long transactionId) {
		db.closeTransaction(transactionId);
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
	public void put(long transactionId,
			long columnId,
			MemorySegment[] keys,
			@Nullable MemorySegment value,
			PutCallback<? super MemorySegment> callback) throws RocksDBException {
		db.put(transactionId, columnId, keys, value, callback);
	}

	@Override
	public void get(long transactionId, long columnId, MemorySegment[] keys, GetCallback<? super MemorySegment> callback)
			throws RocksDBException {
		db.get(transactionId, columnId, keys, callback);
	}

	@Override
	public long openIterator(long transactionId,
			long columnId,
			MemorySegment[] startKeysInclusive,
			@Nullable MemorySegment[] endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException {
		return db.openIterator(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, timeoutMs);
	}

	@Override
	public void closeIterator(long iteratorId) throws RocksDBException {
		db.closeIterator(iteratorId);
	}

	@Override
	public void seekTo(long iterationId, MemorySegment[] keys) throws RocksDBException {
		db.seekTo(iterationId, keys);
	}

	@Override
	public void subsequent(long iterationId,
			long skipCount,
			long takeCount,
			IteratorCallback<? super MemorySegment> callback) throws RocksDBException {
		db.subsequent(iterationId, skipCount, takeCount, callback);
	}
}
