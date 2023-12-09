package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.Callback.GetCallback;
import it.cavallium.rockserver.core.common.Callback.IteratorCallback;
import it.cavallium.rockserver.core.common.Callback.PutCallback;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RocksDBException;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.net.SocketAddress;
import org.jetbrains.annotations.NotNull;
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
	public long createColumn(String name, @NotNull ColumnSchema schema) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void deleteColumn(long columnId) throws RocksDBException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getColumnId(@NotNull String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T put(Arena arena,
			long transactionId,
			long columnId,
			MemorySegment @NotNull [] keys,
			@NotNull MemorySegment value,
			PutCallback<? super MemorySegment, T> callback) throws RocksDBException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T get(Arena arena,
			long transactionId,
			long columnId,
			MemorySegment @NotNull [] keys,
			GetCallback<? super MemorySegment, T> callback) throws RocksDBException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long openIterator(Arena arena,
			long transactionId,
			long columnId,
			MemorySegment @NotNull [] startKeysInclusive,
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
	public void seekTo(Arena arena, long iterationId, MemorySegment @NotNull [] keys) throws RocksDBException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T subsequent(Arena arena,
			long iterationId,
			long skipCount,
			long takeCount,
			@NotNull IteratorCallback<? super MemorySegment, T> callback) throws RocksDBException {
		throw new UnsupportedOperationException();
	}
}
