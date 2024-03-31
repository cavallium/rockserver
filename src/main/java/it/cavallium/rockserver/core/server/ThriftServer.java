package it.cavallium.rockserver.core.server;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.UpdateContext;
import it.cavallium.rockserver.core.common.api.ColumnHashType;
import it.cavallium.rockserver.core.common.api.ColumnSchema;
import it.cavallium.rockserver.core.common.api.Delta;
import it.cavallium.rockserver.core.common.api.OptionalBinary;
import it.cavallium.rockserver.core.common.api.RocksDB.Iface;
import it.cavallium.rockserver.core.common.api.RocksDB.Processor;
import it.cavallium.rockserver.core.common.api.UpdateBegin;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.foreign.ValueLayout.OfByte;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.jetbrains.annotations.NotNull;

public class ThriftServer extends Server {

	private static final OfByte BYTE_BE = ValueLayout.JAVA_BYTE.withOrder(ByteOrder.BIG_ENDIAN);

	public ThriftServer(RocksDBConnection client, String http2Host, int http2Port) throws IOException {
		super(client);
		var handler = new ThriftHandler(this.getClient());

		try {
			var serverTransport = new TNonblockingServerSocket(new InetSocketAddress(http2Host, http2Port));
			var server = new TThreadedSelectorServer(new TThreadedSelectorServer.Args(serverTransport)
					.processor(new Processor<>(handler))
			);

			server.serve();
		} catch (TTransportException e) {
			throw new IOException("Can't open server socket", e);
		}
	}

	private static @NotNull List<@NotNull Keys> keysToRecords(Arena arena, @NotNull List<@NotNull List< @NotNull ByteBuffer>> keysMulti) {
		return keysMulti.stream().map(keys -> keysToRecord(arena, keys)).toList();
	}

	private static Keys keysToRecord(Arena arena, List<@NotNull ByteBuffer> keys) {
		if (keys == null) {
			return null;
		}
		var result = new MemorySegment[keys.size()];
		int i = 0;
		for (ByteBuffer key : keys) {
			result[i] = keyToRecord(arena, key);
			i++;
		}
		return new Keys(result);
	}

	private static @NotNull List<@NotNull MemorySegment> keyToRecords(Arena arena, @NotNull List<@NotNull ByteBuffer> keyMulti) {
		return keyMulti.stream().map(key -> keyToRecord(arena, key)).toList();
	}

	private static @NotNull MemorySegment keyToRecord(Arena arena, @NotNull ByteBuffer key) {
		if (key.isDirect()) {
			return MemorySegment.ofBuffer(key);
		} else {
			return arena.allocate(key.remaining()).copyFrom(MemorySegment.ofBuffer(key));
		}
	}

	private static it.cavallium.rockserver.core.common.ColumnSchema columnSchemaToRecord(ColumnSchema schema) {
		return it.cavallium.rockserver.core.common.ColumnSchema.of(new IntArrayList(schema.getFixedKeys()),
				hashTypesToRecord(schema.getVariableTailKeys()),
				schema.isHasValue()
		);
	}

	private static ObjectArrayList<it.cavallium.rockserver.core.common.ColumnHashType> hashTypesToRecord(List<ColumnHashType> variableTailKeys) {
		var result = new ObjectArrayList<it.cavallium.rockserver.core.common.ColumnHashType>();
		for (ColumnHashType variableTailKey : variableTailKeys) {
			result.add(hashTypeToRecord(variableTailKey));
		}
		return result;
	}

	private static it.cavallium.rockserver.core.common.ColumnHashType hashTypeToRecord(ColumnHashType variableTailKey) {
		return it.cavallium.rockserver.core.common.ColumnHashType.valueOf(variableTailKey.name());
	}

	private static OptionalBinary mapResult(MemorySegment memorySegment) {
		var result = new OptionalBinary();
		return memorySegment != null ? result.setValue(ByteBuffer.wrap(memorySegment.toArray(BYTE_BE))) : result;
	}

	private static UpdateBegin mapResult(UpdateContext<MemorySegment> context) {
		return new UpdateBegin()
				.setUpdateId(context.updateId())
				.setPrevious(context.previous() != null ? context.previous().asByteBuffer() : null);
	}

	private static Delta mapResult(it.cavallium.rockserver.core.common.Delta<MemorySegment> delta) {
		return new Delta()
				.setPrevious(delta.previous() != null ? delta.previous().asByteBuffer() : null)
				.setCurrent(delta.current() != null ? delta.current().asByteBuffer() : null);
	}

	private static List<OptionalBinary> mapResult(List<MemorySegment> multi) {
		return multi.stream().map(ThriftServer::mapResult).toList();
	}

	private static class ThriftHandler implements Iface {

		private final RocksDBConnection client;

		public ThriftHandler(RocksDBConnection client) {
			this.client = client;
		}


		@Override
		public long openTransaction(long timeoutMs) {
			return client.getSyncApi().openTransaction(timeoutMs);
		}

		@Override
		public boolean closeTransaction(long timeoutMs, boolean commit) {
			return client.getSyncApi().closeTransaction(timeoutMs, commit);
		}

		@Override
		public void closeFailedUpdate(long updateId) {
			client.getSyncApi().closeFailedUpdate(updateId);
		}

		@Override
		public long createColumn(String name, ColumnSchema schema) {
			return client.getSyncApi().createColumn(name, columnSchemaToRecord(schema));
		}

		@Override
		public void deleteColumn(long columnId) {
			client.getSyncApi().deleteColumn(columnId);
		}

		@Override
		public long getColumnId(String name) {
			return client.getSyncApi().getColumnId(name);
		}

		@Override
		public void putFast(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value) {
			this.put(transactionOrUpdateId, columnId, keys, value);
		}

		@Override
		public void put(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value) {
			try (var arena = Arena.ofConfined()) {
				client.getSyncApi().put(arena, transactionOrUpdateId, columnId, keysToRecord(arena, keys), keyToRecord(arena, value), RequestType.none());
			}
		}

		@Override
		public void putMulti(long transactionOrUpdateId,
				long columnId,
				List<List<ByteBuffer>> keysMulti,
				List<ByteBuffer> valueMulti) {
			try (var arena = Arena.ofConfined()) {
				client.getSyncApi().putMulti(arena, transactionOrUpdateId, columnId, keysToRecords(arena, keysMulti), keyToRecords(arena, valueMulti), RequestType.none());
			}
		}

		@Override
		public OptionalBinary putGetPrevious(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value) {
			try (var arena = Arena.ofConfined()) {
				return ThriftServer.mapResult(client.getSyncApi().put(arena, transactionOrUpdateId, columnId, keysToRecord(arena, keys), keyToRecord(arena, value), RequestType.previous()));
			}
		}

		@Override
		public Delta putGetDelta(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value) {
			try (var arena = Arena.ofConfined()) {
				return ThriftServer.mapResult(client.getSyncApi().put(arena, transactionOrUpdateId, columnId, keysToRecord(arena, keys), keyToRecord(arena, value), RequestType.delta()));
			}
		}

		@Override
		public boolean putGetChanged(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value) {
			try (var arena = Arena.ofConfined()) {
				return client.getSyncApi().put(arena, transactionOrUpdateId, columnId, keysToRecord(arena, keys), keyToRecord(arena, value), RequestType.changed());
			}
		}

		@Override
		public boolean putGetPreviousPresence(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value) {
			try (var arena = Arena.ofConfined()) {
				return client.getSyncApi().put(arena, transactionOrUpdateId, columnId, keysToRecord(arena, keys), keyToRecord(arena, value), RequestType.previousPresence());
			}
		}

		@Override
		public OptionalBinary get(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys) {
			try (var arena = Arena.ofConfined()) {
				return ThriftServer.mapResult(client.getSyncApi().get(arena, transactionOrUpdateId, columnId, keysToRecord(arena, keys), RequestType.current()));
			}
		}

		@Override
		public UpdateBegin getForUpdate(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys) {
			try (var arena = Arena.ofConfined()) {
				return mapResult(client.getSyncApi().get(arena, transactionOrUpdateId, columnId, keysToRecord(arena, keys), RequestType.forUpdate()));
			}
		}

		@Override
		public boolean exists(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys) {
			try (var arena = Arena.ofConfined()) {
				return client.getSyncApi().get(arena, transactionOrUpdateId, columnId, keysToRecord(arena, keys), RequestType.exists());
			}
		}

		@Override
		public long openIterator(long transactionId,
				long columnId,
				List<ByteBuffer> startKeysInclusive,
				List<ByteBuffer> endKeysExclusive,
				boolean reverse,
				long timeoutMs) {
			try (var arena = Arena.ofConfined()) {
				return client.getSyncApi().openIterator(arena, transactionId, columnId, keysToRecord(arena, startKeysInclusive), keysToRecord(arena, endKeysExclusive), reverse, timeoutMs);
			}
		}

		@Override
		public void closeIterator(long iteratorId) {
			client.getSyncApi().closeIterator(iteratorId);
		}

		@Override
		public void seekTo(long iterationId, List<ByteBuffer> keys) {
			try (var arena = Arena.ofConfined()) {
				client.getSyncApi().seekTo(arena, iterationId, keysToRecord(arena, keys));
			}
		}

		@Override
		public void subsequent(long iterationId, long skipCount, long takeCount) {
			try (var arena = Arena.ofConfined()) {
				client.getSyncApi().subsequent(arena, iterationId, skipCount, takeCount, RequestType.none());
			}
		}

		@Override
		public boolean subsequentExists(long iterationId,
				long skipCount,
				long takeCount) {
			try (var arena = Arena.ofConfined()) {
				return client.getSyncApi().subsequent(arena, iterationId, skipCount, takeCount, RequestType.exists());
			}
		}

		@Override
		public List<OptionalBinary> subsequentMultiGet(long iterationId,
				long skipCount,
				long takeCount) {
			try (var arena = Arena.ofConfined()) {
				return mapResult(client.getSyncApi().subsequent(arena, iterationId, skipCount, takeCount, RequestType.multi()));
			}
		}
	}
}
