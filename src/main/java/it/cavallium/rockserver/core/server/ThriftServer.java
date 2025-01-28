package it.cavallium.rockserver.core.server;

import static it.cavallium.rockserver.core.common.Utils.asByteBuffer;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.UpdateContext;
import it.cavallium.rockserver.core.common.Utils;
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
import it.cavallium.buffer.Buf;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.thrift.TException;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftServer extends Server {

	private static final Logger LOG = LoggerFactory.getLogger(ThriftServer.class.getName());

	private final Thread thriftThread;
	private final TThreadedSelectorServer server;

	public ThriftServer(RocksDBConnection client, String http2Host, int http2Port) throws IOException {
		super(client);
		var handler = new ThriftHandler(this.getClient());

		try {
			var serverTransport = new TNonblockingServerSocket(new InetSocketAddress(http2Host, http2Port));
			this.server = new TThreadedSelectorServer(new TThreadedSelectorServer.Args(serverTransport)
					.processor(new Processor<>(handler))
			);

			this.thriftThread = Thread.ofPlatform().name("Thrift server thread").unstarted(server::serve);
			LOG.info("Thrift RocksDB server is listening at " + http2Host + ":" + http2Port);
		} catch (TTransportException e) {
			throw new IOException("Can't open server socket", e);
		}
	}

	public void start() {
		thriftThread.start();
	}

	private static @NotNull List<@NotNull Keys> keysToRecords(@NotNull List<@NotNull List< @NotNull ByteBuffer>> keysMulti) {
		return keysMulti.stream().map(ThriftServer::keysToRecord).toList();
	}

	private static Keys keysToRecord(List<@NotNull ByteBuffer> keys) {
		if (keys == null) {
			return null;
		}
		var result = new Buf[keys.size()];
		int i = 0;
		for (ByteBuffer key : keys) {
			result[i] = keyToRecord(key);
			i++;
		}
		return new Keys(result);
	}

	private static @NotNull List<@NotNull Buf> keyToRecords(@NotNull List<@NotNull ByteBuffer> keyMulti) {
		return keyMulti.stream().map(ThriftServer::keyToRecord).toList();
	}

	private static @NotNull Buf keyToRecord(@NotNull ByteBuffer key) {
		return Utils.fromByteBuffer(key);
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

	private static OptionalBinary mapResult(Buf buf) {
		var result = new OptionalBinary();
		return buf != null ? result.setValue(asByteBuffer(buf)) : result;
	}

	private static UpdateBegin mapResult(UpdateContext<Buf> context) {
		return new UpdateBegin()
				.setUpdateId(context.updateId())
				.setPrevious(context.previous() != null ? asByteBuffer(context.previous()) : null);
	}

	private static Delta mapResult(it.cavallium.rockserver.core.common.Delta<Buf> delta) {
		return new Delta()
				.setPrevious(delta.previous() != null ? asByteBuffer(delta.previous()) : null)
				.setCurrent(delta.current() != null ? asByteBuffer(delta.current()) : null);
	}

	private static List<OptionalBinary> mapResult(List<Buf> multi) {
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
		public boolean closeTransaction(long transactionId, boolean commit) throws TException {
			return client.getSyncApi().closeTransaction(transactionId, commit);
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
			client.getSyncApi().put(transactionOrUpdateId, columnId, keysToRecord(keys), keyToRecord(value), RequestType.none());
		}

		@Override
		public void putMulti(long transactionOrUpdateId,
				long columnId,
				List<List<ByteBuffer>> keysMulti,
				List<ByteBuffer> valueMulti) {
			client.getSyncApi().putMulti(transactionOrUpdateId, columnId, keysToRecords(keysMulti), keyToRecords(
					valueMulti), RequestType.none());
		}

		@Override
		public OptionalBinary putGetPrevious(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value) {
			return ThriftServer.mapResult(client.getSyncApi().put(transactionOrUpdateId, columnId, keysToRecord(keys), keyToRecord(value), RequestType.previous()));
		}

		@Override
		public Delta putGetDelta(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value) {
			return ThriftServer.mapResult(client.getSyncApi().put(transactionOrUpdateId, columnId, keysToRecord(keys), keyToRecord(value), RequestType.delta()));
		}

		@Override
		public boolean putGetChanged(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value) {
			return client.getSyncApi().put(transactionOrUpdateId, columnId, keysToRecord(keys), keyToRecord(value), RequestType.changed());
		}

		@Override
		public boolean putGetPreviousPresence(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value) {
			return client.getSyncApi().put(transactionOrUpdateId, columnId, keysToRecord(keys), keyToRecord(value), RequestType.previousPresence());
		}

		@Override
		public OptionalBinary get(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys) {
			return ThriftServer.mapResult(client.getSyncApi().get(transactionOrUpdateId, columnId, keysToRecord(keys), RequestType.current()));
		}

		@Override
		public UpdateBegin getForUpdate(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys) {
			return mapResult(client.getSyncApi().get(transactionOrUpdateId, columnId, keysToRecord(keys), RequestType.forUpdate()));
		}

		@Override
		public boolean exists(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys) {
			return client.getSyncApi().get(transactionOrUpdateId, columnId, keysToRecord(keys), RequestType.exists());
		}

		@Override
		public long openIterator(long transactionId,
				long columnId,
				List<ByteBuffer> startKeysInclusive,
				List<ByteBuffer> endKeysExclusive,
				boolean reverse,
				long timeoutMs) {
			return client.getSyncApi().openIterator(transactionId, columnId, keysToRecord(startKeysInclusive), keysToRecord(
					endKeysExclusive), reverse, timeoutMs);
		}

		@Override
		public void closeIterator(long iteratorId) {
			client.getSyncApi().closeIterator(iteratorId);
		}

		@Override
		public void seekTo(long iterationId, List<ByteBuffer> keys) {
			client.getSyncApi().seekTo(iterationId, keysToRecord(keys));
		}

		@Override
		public void subsequent(long iterationId, long skipCount, long takeCount) {
			client.getSyncApi().subsequent(iterationId, skipCount, takeCount, RequestType.none());
		}

		@Override
		public boolean subsequentExists(long iterationId,
				long skipCount,
				long takeCount) {
			return client.getSyncApi().subsequent(iterationId, skipCount, takeCount, RequestType.exists());
		}

		@Override
		public List<OptionalBinary> subsequentMultiGet(long iterationId,
				long skipCount,
				long takeCount) {
			return mapResult(client.getSyncApi().subsequent(iterationId, skipCount, takeCount, RequestType.multi()));
		}
	}

	@Override
	public void close() throws IOException {
		LOG.info("Thrift server is shutting down...");
		this.server.stop();
		super.close();
	}
}
