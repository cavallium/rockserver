package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.Callback.GetCallback;
import it.cavallium.rockserver.core.common.Callback.IteratorCallback;
import it.cavallium.rockserver.core.common.Callback.PutCallback;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RocksDBException;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.net.SocketAddress;
import org.jetbrains.annotations.Nullable;

public abstract class SocketConnection extends BaseConnection {

	private final SocketAddress address;

	public SocketConnection(SocketAddress address, String name) {
		super(name);
		this.address = address;
	}

	public SocketAddress getAddress() {
		return address;
	}

	@Override
	public void close() throws IOException {
		super.close();
	}
	@Override
	public long openTransaction(long timeoutMs) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean closeTransaction(long transactionId, boolean commit) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long createColumn(String name, ColumnSchema schema) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void deleteColumn(long columnId) throws RocksDBException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getColumnId(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T put(long transactionId,
			long columnId,
			MemorySegment[] keys,
			@Nullable MemorySegment value,
			PutCallback<? super MemorySegment, T> callback) throws RocksDBException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T get(long transactionId,
			long columnId,
			MemorySegment[] keys,
			GetCallback<? super MemorySegment, T> callback) throws RocksDBException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long openIterator(long transactionId,
			long columnId,
			MemorySegment[] startKeysInclusive,
			@Nullable MemorySegment[] endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void closeIterator(long iteratorId) throws RocksDBException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void seekTo(long iterationId, MemorySegment[] keys) throws RocksDBException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T subsequent(long iterationId,
			long skipCount,
			long takeCount,
			IteratorCallback<? super MemorySegment, T> callback) throws RocksDBException {
		throw new UnsupportedOperationException();
	}
}
