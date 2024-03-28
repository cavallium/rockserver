package it.cavallium.rockserver.core.server;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.UpdateContext;
import it.cavallium.rockserver.core.common.api.ColumnHashType;
import it.cavallium.rockserver.core.common.api.ColumnSchema;
import it.cavallium.rockserver.core.common.api.Delta;
import it.cavallium.rockserver.core.common.api.OptionalBinary;
import it.cavallium.rockserver.core.common.api.RocksDB;
import it.cavallium.rockserver.core.common.api.RocksDB.AsyncIface;
import it.cavallium.rockserver.core.common.api.RocksDB.AsyncProcessor;
import it.cavallium.rockserver.core.common.api.UpdateBegin;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TNonblockingServer.Args;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.jetbrains.annotations.NotNull;

public class ThriftServer extends Server {

	private final AsyncIface handler;

	public ThriftServer(RocksDBConnection client, String http2Host, int http2Port) throws IOException {
		super(client);
		this.handler = new RocksDB.AsyncIface() {
			@Override
			public void openTransaction(long timeoutMs, AsyncMethodCallback<Long> resultHandler) {
				client.getAsyncApi().openTransactionAsync(timeoutMs).whenComplete(handleResult(resultHandler));
			}

			@Override
			public void closeTransaction(long timeoutMs, boolean commit, AsyncMethodCallback<Boolean> resultHandler) {
				client.getAsyncApi().closeTransactionAsync(timeoutMs, commit).whenComplete(handleResult(resultHandler));
			}

			@Override
			public void closeFailedUpdate(long updateId, AsyncMethodCallback<Void> resultHandler) {
				client.getAsyncApi().closeFailedUpdateAsync(updateId).whenComplete(handleResult(resultHandler));
			}

			@Override
			public void createColumn(String name, ColumnSchema schema, AsyncMethodCallback<Long> resultHandler) {
				client.getAsyncApi().createColumnAsync(name, columnSchemaToRecord(schema))
						.whenComplete(handleResult(resultHandler));
			}

			@Override
			public void deleteColumn(long columnId, AsyncMethodCallback<Void> resultHandler) {
				client.getAsyncApi().deleteColumnAsync(columnId).whenComplete(handleResult(resultHandler));
			}

			@Override
			public void getColumnId(String name, AsyncMethodCallback<Long> resultHandler) {
				client.getAsyncApi().getColumnIdAsync(name).whenComplete(handleResult(resultHandler));
			}

			@Override
			public void put(long transactionOrUpdateId,
					long columnId,
					List<ByteBuffer> keys,
					ByteBuffer value,
					AsyncMethodCallback<Void> resultHandler) {
				var arena = Arena.ofShared();
				client.getAsyncApi()
						.putAsync(arena, transactionOrUpdateId, columnId, keysToRecord(keys), keyToRecord(value), RequestType.none())
						.whenComplete(handleResultWithArena(arena, resultHandler));
			}

			@Override
			public void putGetPrevious(long transactionOrUpdateId,
					long columnId,
					List<ByteBuffer> keys,
					ByteBuffer value,
					AsyncMethodCallback<OptionalBinary> resultHandler) {
				var arena = Arena.ofShared();
				client.getAsyncApi()
						.putAsync(arena, transactionOrUpdateId, columnId, keysToRecord(keys), keyToRecord(value), RequestType.previous())
						.thenApply(ThriftServer::mapResult)
						.whenComplete(handleResultWithArena(arena, resultHandler));
			}

			@Override
			public void putGetDelta(long transactionOrUpdateId,
					long columnId,
					List<ByteBuffer> keys,
					ByteBuffer value,
					AsyncMethodCallback<Delta> resultHandler) {
				var arena = Arena.ofShared();
				client.getAsyncApi()
						.putAsync(arena, transactionOrUpdateId, columnId, keysToRecord(keys), keyToRecord(value), RequestType.delta())
						.thenApply(ThriftServer::mapResult)
						.whenComplete(handleResultWithArena(arena, resultHandler));
			}

			@Override
			public void putGetChanged(long transactionOrUpdateId,
					long columnId,
					List<ByteBuffer> keys,
					ByteBuffer value,
					AsyncMethodCallback<Boolean> resultHandler) {
				var arena = Arena.ofShared();
				client.getAsyncApi()
						.putAsync(arena, transactionOrUpdateId, columnId, keysToRecord(keys), keyToRecord(value), RequestType.changed())
						.whenComplete(handleResultWithArena(arena, resultHandler));
			}

			@Override
			public void putGetPreviousPresence(long transactionOrUpdateId,
					long columnId,
					List<ByteBuffer> keys,
					ByteBuffer value,
					AsyncMethodCallback<Boolean> resultHandler) {
				var arena = Arena.ofShared();
				client.getAsyncApi()
						.putAsync(arena, transactionOrUpdateId, columnId, keysToRecord(keys), keyToRecord(value), RequestType.previousPresence())
						.whenComplete(handleResultWithArena(arena, resultHandler));
			}

			@Override
			public void get(long transactionOrUpdateId,
					long columnId,
					List<ByteBuffer> keys,
					AsyncMethodCallback<OptionalBinary> resultHandler) {
				var arena = Arena.ofShared();
				client.getAsyncApi()
						.getAsync(arena, transactionOrUpdateId, columnId, keysToRecord(keys), RequestType.current())
						.thenApply(ThriftServer::mapResult)
						.whenComplete(handleResultWithArena(arena, resultHandler));
			}

			@Override
			public void getForUpdate(long transactionOrUpdateId,
					long columnId,
					List<ByteBuffer> keys,
					AsyncMethodCallback<UpdateBegin> resultHandler) {
				var arena = Arena.ofShared();
				client.getAsyncApi()
						.getAsync(arena, transactionOrUpdateId, columnId, keysToRecord(keys), RequestType.forUpdate())
						.thenApply(ThriftServer::mapResult)
						.whenComplete(handleResultWithArena(arena, resultHandler));
			}

			@Override
			public void exists(long transactionOrUpdateId,
					long columnId,
					List<ByteBuffer> keys,
					AsyncMethodCallback<Boolean> resultHandler) {
				var arena = Arena.ofShared();
				client.getAsyncApi()
						.getAsync(arena, transactionOrUpdateId, columnId, keysToRecord(keys), RequestType.exists())
						.whenComplete(handleResultWithArena(arena, resultHandler));
			}

			@Override
			public void openIterator(long transactionId,
					long columnId,
					List<ByteBuffer> startKeysInclusive,
					List<ByteBuffer> endKeysExclusive,
					boolean reverse,
					long timeoutMs,
					AsyncMethodCallback<Long> resultHandler) {
				var arena = Arena.ofShared();
				client.getAsyncApi()
						.openIteratorAsync(arena, transactionId, columnId, keysToRecord(startKeysInclusive), keysToRecord(endKeysExclusive), reverse, timeoutMs)
						.whenComplete(handleResultWithArena(arena, resultHandler));
			}

			@Override
			public void closeIterator(long iteratorId, AsyncMethodCallback<Void> resultHandler) {
				client.getAsyncApi()
						.closeIteratorAsync(iteratorId)
						.whenComplete(handleResult(resultHandler));
			}

			@Override
			public void seekTo(long iterationId, List<ByteBuffer> keys, AsyncMethodCallback<Void> resultHandler) {
				var arena = Arena.ofShared();
				client.getAsyncApi()
						.seekToAsync(arena, iterationId, keysToRecord(keys))
						.whenComplete(handleResultWithArena(arena, resultHandler));
			}

			@Override
			public void subsequent(long iterationId, long skipCount, long takeCount, AsyncMethodCallback<Void> resultHandler) {
				var arena = Arena.ofShared();
				client.getAsyncApi()
						.subsequentAsync(arena, iterationId, skipCount, takeCount, RequestType.none())
						.whenComplete(handleResultWithArena(arena, resultHandler));
			}

			@Override
			public void subsequentExists(long iterationId,
					long skipCount,
					long takeCount,
					AsyncMethodCallback<Boolean> resultHandler) {
				var arena = Arena.ofShared();
				client.getAsyncApi()
						.subsequentAsync(arena, iterationId, skipCount, takeCount, RequestType.exists())
						.whenComplete(handleResultWithArena(arena, resultHandler));
			}

			@Override
			public void subsequentMultiGet(long iterationId,
					long skipCount,
					long takeCount,
					AsyncMethodCallback<List<OptionalBinary>> resultHandler) {
				var arena = Arena.ofShared();
				client.getAsyncApi()
						.subsequentAsync(arena, iterationId, skipCount, takeCount, RequestType.multi())
						.thenApply(ThriftServer::mapResult)
						.whenComplete(handleResultWithArena(arena, resultHandler));
			}
		};

		try {
			var serverTransport = new TNonblockingServerSocket(new InetSocketAddress(http2Host, http2Port));
			var server = new TNonblockingServer(new Args(serverTransport).processor(new AsyncProcessor<>(handler)));

			server.serve();
		} catch (TTransportException e) {
			throw new IOException("Can't open server socket", e);
		}

	}

	private @NotNull MemorySegment [] keysToRecord(List<@NotNull ByteBuffer> keys) {
		if (keys == null) {
			return null;
		}
		var result = new MemorySegment[keys.size()];
		int i = 0;
		for (ByteBuffer key : keys) {
			result[i] = keyToRecord(key);
			i++;
		}
		return result;
	}

	private @NotNull MemorySegment keyToRecord(@NotNull ByteBuffer key) {
		return MemorySegment.ofBuffer(key);
	}

	private it.cavallium.rockserver.core.common.ColumnSchema columnSchemaToRecord(ColumnSchema schema) {
		return it.cavallium.rockserver.core.common.ColumnSchema.of(new IntArrayList(schema.getFixedKeys()),
				hashTypesToRecord(schema.getVariableTailKeys()),
				schema.isHasValue()
		);
	}

	private ObjectArrayList<it.cavallium.rockserver.core.common.ColumnHashType> hashTypesToRecord(List<ColumnHashType> variableTailKeys) {
		var result = new ObjectArrayList<it.cavallium.rockserver.core.common.ColumnHashType>();
		for (ColumnHashType variableTailKey : variableTailKeys) {
			result.add(hashTypeToRecord(variableTailKey));
		}
		return result;
	}

	private it.cavallium.rockserver.core.common.ColumnHashType hashTypeToRecord(ColumnHashType variableTailKey) {
		return it.cavallium.rockserver.core.common.ColumnHashType.valueOf(variableTailKey.name());
	}

	private static <T> BiConsumer<? super T, ? super Throwable> handleResult(AsyncMethodCallback<T> resultHandler) {
		return (result, error) -> {
			if (error != null) {
				if (error instanceof Exception ex) {
					resultHandler.onError(ex);
				} else {
					resultHandler.onError(new Exception(error));
				}
			} else {
				resultHandler.onComplete(result);
			}
		};
	}

	private static <T> BiConsumer<? super T, ? super Throwable> handleResultWithArena(Arena arena,
			AsyncMethodCallback<T> resultHandler) {
		return (result, error) -> {
			arena.close();
			if (error != null) {
				if (error instanceof Exception ex) {
					resultHandler.onError(ex);
				} else {
					resultHandler.onError(new Exception(error));
				}
			} else {
				resultHandler.onComplete(result);
			}
		};
	}

	private static OptionalBinary mapResult(MemorySegment memorySegment) {
		return memorySegment != null ? new OptionalBinary().setValue(memorySegment.asByteBuffer()) : null;
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
}
