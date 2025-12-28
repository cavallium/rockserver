package it.cavallium.rockserver.core.client;

import static it.cavallium.rockserver.core.common.Utils.asByteBuffer;
import static it.cavallium.rockserver.core.common.Utils.toBuf;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestChanged;
import it.cavallium.rockserver.core.common.RequestType.RequestCurrent;
import it.cavallium.rockserver.core.common.RequestType.RequestDelta;
import it.cavallium.rockserver.core.common.RequestType.RequestEntriesCount;
import it.cavallium.rockserver.core.common.RequestType.RequestExists;
import it.cavallium.rockserver.core.common.RequestType.RequestForUpdate;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestGetAllInRange;
import it.cavallium.rockserver.core.common.RequestType.RequestGetFirstAndLast;
import it.cavallium.rockserver.core.common.RequestType.RequestGetRange;
import it.cavallium.rockserver.core.common.RequestType.RequestIterate;
import it.cavallium.rockserver.core.common.RequestType.RequestMerge;
import it.cavallium.rockserver.core.common.RequestType.RequestMulti;
import it.cavallium.rockserver.core.common.RequestType.RequestNothing;
import it.cavallium.rockserver.core.common.RequestType.RequestPrevious;
import it.cavallium.rockserver.core.common.RequestType.RequestPreviousPresence;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RequestType.RequestReduceRange;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.api.OptionalBinary;
import it.cavallium.rockserver.core.common.api.RocksDB;
import it.cavallium.rockserver.core.common.api.RocksDBThriftException;
import it.cavallium.rockserver.core.common.api.UpdateBegin;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class ThriftConnection extends BaseConnection implements RocksDBAPI {

	private final URI uri;
	private final TTransport transport;
	private final RocksDB.Client client;
	private final ExecutorService executor;

	public ThriftConnection(String name, String host, int port) throws TException {
		super(name);
		this.uri = URI.create("thrift://" + host + ":" + port);
		this.transport = new TFramedTransport(new TSocket(host, port));
		this.transport.open();
		this.client = new RocksDB.Client(new TBinaryProtocol(this.transport));
		this.executor = Executors.newCachedThreadPool();
	}

	@Override
	public URI getUrl() {
		return uri;
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
	public void close() throws IOException {
		transport.close();
		executor.shutdown();
		super.close();
	}

	// --- Sync API ---

	@Override
	public long openTransaction(long timeoutMs) {
		try {
			return client.openTransaction(timeoutMs);
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@Override
	public boolean closeTransaction(long transactionId, boolean commit) {
		try {
			return client.closeTransaction(transactionId, commit);
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@Override
	public void closeFailedUpdate(long updateId) {
		try {
			client.closeFailedUpdate(updateId);
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@Override
	public long createColumn(String name, ColumnSchema schema) {
		try {
			return client.createColumn(name, mapSchema(schema));
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@Override
	public long uploadMergeOperator(String name, String className, byte[] jarData) {
		try {
			return client.uploadMergeOperator(name, className, ByteBuffer.wrap(jarData));
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@Override
	public Long checkMergeOperator(String name, byte[] hash) {
		throw new UnsupportedOperationException("checkMergeOperator not implemented for Thrift");
	}

	@Override
	public void deleteColumn(long columnId) {
		try {
			client.deleteColumn(columnId);
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@Override
	public long getColumnId(String name) {
		try {
			return client.getColumnId(name);
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T put(long transactionOrUpdateId, long columnId, Keys keys, Buf value, RequestPut<? super Buf, T> requestType) {
		try {
			if (requestType == null) throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "Request type cannot be null");
			if (requestType instanceof RequestNothing) {
				client.put(transactionOrUpdateId, columnId, mapKeys(keys), mapBuf(value));
				return null;
			} else if (requestType instanceof RequestPrevious) {
				return (T) mapOptionalBinary(client.putGetPrevious(transactionOrUpdateId, columnId, mapKeys(keys), mapBuf(value)));
			} else if (requestType instanceof RequestDelta) {
				return (T) mapDelta(client.putGetDelta(transactionOrUpdateId, columnId, mapKeys(keys), mapBuf(value)));
			} else if (requestType instanceof RequestChanged) {
				return (T) (Boolean) client.putGetChanged(transactionOrUpdateId, columnId, mapKeys(keys), mapBuf(value));
			} else if (requestType instanceof RequestPreviousPresence) {
				return (T) (Boolean) client.putGetPreviousPresence(transactionOrUpdateId, columnId, mapKeys(keys), mapBuf(value));
			}
			throw new UnsupportedOperationException("Request type " + requestType + " not supported");
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T merge(long transactionOrUpdateId, long columnId, Keys keys, Buf value, RequestMerge<? super Buf, T> requestType) {
		try {
			if (requestType == null) throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "Request type cannot be null");
			if (requestType instanceof RequestNothing) {
				client.merge(transactionOrUpdateId, columnId, mapKeys(keys), mapBuf(value));
				return null;
			} else if (requestType instanceof RequestType.RequestMerged) {
				return (T) mapOptionalBinary(client.mergeGetMerged(transactionOrUpdateId, columnId, mapKeys(keys), mapBuf(value)));
			}
			throw new UnsupportedOperationException("Request type " + requestType + " not supported");
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> List<T> putMulti(long transactionOrUpdateId, long columnId, List<Keys> keys, List<Buf> values, RequestPut<? super Buf, T> requestType) {
		try {
			if (requestType == null) throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "Request type cannot be null");
			if (requestType instanceof RequestNothing) {
				client.putMulti(transactionOrUpdateId, columnId, mapKeysList(keys), mapBufList(values));
				return Collections.nCopies(keys.size(), null);
			} else if (requestType instanceof RequestPrevious) {
				return (List<T>) mapOptionalBinaryList(client.putMultiGetPrevious(transactionOrUpdateId, columnId, mapKeysList(keys), mapBufList(values)));
			} else if (requestType instanceof RequestDelta) {
				return (List<T>) client.putMultiGetDelta(transactionOrUpdateId, columnId, mapKeysList(keys), mapBufList(values))
						.stream().map(this::mapDelta).collect(Collectors.toList());
			} else if (requestType instanceof RequestChanged) {
				return (List<T>) client.putMultiGetChanged(transactionOrUpdateId, columnId, mapKeysList(keys), mapBufList(values));
			} else if (requestType instanceof RequestPreviousPresence) {
				return (List<T>) client.putMultiGetPreviousPresence(transactionOrUpdateId, columnId, mapKeysList(keys), mapBufList(values));
			}
			throw new UnsupportedOperationException("Request type " + requestType + " not supported");
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> List<T> mergeMulti(long transactionOrUpdateId, long columnId, List<Keys> keys, List<Buf> values, RequestMerge<? super Buf, T> requestType) {
		try {
			if (requestType == null) throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "Request type cannot be null");
			if (requestType instanceof RequestNothing) {
				client.mergeMulti(transactionOrUpdateId, columnId, mapKeysList(keys), mapBufList(values));
				return Collections.nCopies(keys.size(), null);
			} else if (requestType instanceof RequestType.RequestMerged) {
				return (List<T>) mapOptionalBinaryList(client.mergeMultiGetMerged(transactionOrUpdateId, columnId, mapKeysList(keys), mapBufList(values)));
			}
			throw new UnsupportedOperationException("Request type " + requestType + " not supported");
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@Override
	public void putBatch(long columnId, Publisher<KVBatch> batchPublisher, PutBatchMode mode) {
		try {
			List<it.cavallium.rockserver.core.common.api.KV> data = new ArrayList<>();
			Flux.from(batchPublisher).toIterable().forEach(batch -> {
				List<Keys> keys = batch.keys();
				List<Buf> values = batch.values();
				for (int i = 0; i < keys.size(); i++) {
					data.add(new it.cavallium.rockserver.core.common.api.KV(mapKeys(keys.get(i)), mapBuf(values.get(i))));
				}
			});
			it.cavallium.rockserver.core.common.api.PutBatchMode thriftMode = switch (mode) {
				case WRITE_BATCH -> it.cavallium.rockserver.core.common.api.PutBatchMode.WRITE_BATCH;
				case WRITE_BATCH_NO_WAL -> it.cavallium.rockserver.core.common.api.PutBatchMode.WRITE_BATCH_NO_WAL;
				case SST_INGESTION -> it.cavallium.rockserver.core.common.api.PutBatchMode.SST_INGESTION;
				case SST_INGEST_BEHIND -> it.cavallium.rockserver.core.common.api.PutBatchMode.SST_INGEST_BEHIND;
			};
			client.putBatch(columnId, data, thriftMode);
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@Override
	public void mergeBatch(long columnId, Publisher<KVBatch> batchPublisher, MergeBatchMode mode) {
		try {
			List<it.cavallium.rockserver.core.common.api.KV> data = new ArrayList<>();
			Flux.from(batchPublisher).toIterable().forEach(batch -> {
				List<Keys> keys = batch.keys();
				List<Buf> values = batch.values();
				for (int i = 0; i < keys.size(); i++) {
					data.add(new it.cavallium.rockserver.core.common.api.KV(mapKeys(keys.get(i)), mapBuf(values.get(i))));
				}
			});
			it.cavallium.rockserver.core.common.api.MergeBatchMode thriftMode = switch (mode) {
				case MERGE_WRITE_BATCH -> it.cavallium.rockserver.core.common.api.MergeBatchMode.MERGE_WRITE_BATCH;
				case MERGE_WRITE_BATCH_NO_WAL -> it.cavallium.rockserver.core.common.api.MergeBatchMode.MERGE_WRITE_BATCH_NO_WAL;
				case MERGE_SST_INGESTION -> it.cavallium.rockserver.core.common.api.MergeBatchMode.MERGE_SST_INGESTION;
				case MERGE_SST_INGEST_BEHIND -> it.cavallium.rockserver.core.common.api.MergeBatchMode.MERGE_SST_INGEST_BEHIND;
			};
			client.mergeBatch(columnId, data, thriftMode);
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(long transactionOrUpdateId, long columnId, Keys keys, RequestGet<? super Buf, T> requestType) {
		try {
			if (requestType == null) throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "Request type cannot be null");
			if (requestType instanceof RequestCurrent) {
				return (T) mapOptionalBinary(client.get(transactionOrUpdateId, columnId, mapKeys(keys)));
			} else if (requestType instanceof RequestForUpdate) {
				UpdateBegin ub = client.getForUpdate(transactionOrUpdateId, columnId, mapKeys(keys));
				return (T) new it.cavallium.rockserver.core.common.UpdateContext<>(
						ub.isSetPrevious() ? Utils.fromByteBuffer(ub.previous) : null,
						ub.getUpdateId());
			} else if (requestType instanceof RequestExists) {
				return (T) (Boolean) client.exists(transactionOrUpdateId, columnId, mapKeys(keys));
			}
			throw new UnsupportedOperationException("Request type " + requestType + " not supported");
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@Override
	public long openIterator(long transactionId, long columnId, Keys startKeysInclusive, Keys endKeysExclusive, boolean reverse, long timeoutMs) {
		try {
			return client.openIterator(transactionId, columnId, mapKeys(startKeysInclusive), mapKeys(endKeysExclusive), reverse, timeoutMs);
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@Override
	public void closeIterator(long iteratorId) {
		try {
			client.closeIterator(iteratorId);
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@Override
	public void seekTo(long iterationId, Keys keys) {
		try {
			client.seekTo(iterationId, mapKeys(keys));
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T subsequent(long iterationId, long skipCount, long takeCount, RequestIterate<? super Buf, T> requestType) {
		try {
			if (requestType == null) throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "Request type cannot be null");
			if (requestType instanceof RequestNothing) {
				client.subsequent(iterationId, skipCount, takeCount);
				return null;
			} else if (requestType instanceof RequestExists) {
				return (T) (Boolean) client.subsequentExists(iterationId, skipCount, takeCount);
			} else if (requestType instanceof RequestMulti) {
				return (T) mapOptionalBinaryList(client.subsequentMultiGet(iterationId, skipCount, takeCount));
			}
			throw new UnsupportedOperationException("Request type " + requestType + " not supported");
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@Override
	public <T> T reduceRange(long transactionId, long columnId, Keys startKeysInclusive, Keys endKeysExclusive, boolean reverse, RequestReduceRange<? super KV, T> requestType, long timeoutMs) {
		try {
			if (requestType == null) throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "Request type cannot be null");
			if (requestType instanceof RequestEntriesCount) {
				return (T) Long.valueOf(client.reduceRangeEntriesCount(transactionId, columnId, mapKeys(startKeysInclusive), mapKeys(endKeysExclusive), reverse, timeoutMs));
			} else if (requestType instanceof RequestGetFirstAndLast) {
				it.cavallium.rockserver.core.common.api.FirstAndLast fl = client.reduceRangeFirstAndLast(transactionId, columnId, mapKeys(startKeysInclusive), mapKeys(endKeysExclusive), reverse, timeoutMs);
				return (T) new it.cavallium.rockserver.core.common.FirstAndLast<>(
						fl.isSetFirst() ? mapKV(fl.getFirst()) : null,
						fl.isSetLast() ? mapKV(fl.getLast()) : null
				);
			}
			throw new UnsupportedOperationException("Reduce range type " + requestType + " not implemented in Thrift client");
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@Override
	public <T> Stream<T> getRange(long transactionId, long columnId, Keys startKeysInclusive, Keys endKeysExclusive, boolean reverse, RequestGetRange<? super KV, T> requestType, long timeoutMs) {
		try {
			if (requestType == null) throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "Request type cannot be null");
			if (requestType instanceof RequestGetAllInRange) {
				List<it.cavallium.rockserver.core.common.api.KV> list = client.getAllInRange(transactionId, columnId, mapKeys(startKeysInclusive), mapKeys(endKeysExclusive), reverse, timeoutMs);
				return list.stream().map(this::mapKV).map(k -> (T) k);
			}
			throw new UnsupportedOperationException("Get range type " + requestType + " not implemented");
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@Override
	public void flush() {
		try {
			client.flush();
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@Override
	public void compact() {
		try {
			client.compact();
		} catch (TException e) {
			throw wrap(e);
		}
	}

	@Override
	public Map<String, ColumnSchema> getAllColumnDefinitions() {
		try {
			return client.getAllColumnDefinitions().stream()
					.collect(Collectors.toMap(
							it.cavallium.rockserver.core.common.api.Column::getName,
							c -> mapSchemaFromThrift(c.getSchema())
					));
		} catch (TException e) {
			throw wrap(e);
		}
	}

	// --- Async API ---

	@Override
	public CompletableFuture<Long> openTransactionAsync(long timeoutMs) {
		return CompletableFuture.supplyAsync(() -> openTransaction(timeoutMs), executor);
	}

	@Override
	public CompletableFuture<Boolean> closeTransactionAsync(long transactionId, boolean commit) {
		return CompletableFuture.supplyAsync(() -> closeTransaction(transactionId, commit), executor);
	}

	@Override
	public CompletableFuture<Void> closeFailedUpdateAsync(long updateId) {
		return CompletableFuture.runAsync(() -> closeFailedUpdate(updateId), executor);
	}

	@Override
	public CompletableFuture<Long> createColumnAsync(String name, ColumnSchema schema) {
		return CompletableFuture.supplyAsync(() -> createColumn(name, schema), executor);
	}

	@Override
	public CompletableFuture<Long> uploadMergeOperatorAsync(String name, String className, byte[] jarData) {
		return CompletableFuture.supplyAsync(() -> uploadMergeOperator(name, className, jarData), executor);
	}

	@Override
	public CompletableFuture<Long> checkMergeOperatorAsync(String name, byte[] hash) {
		return CompletableFuture.failedFuture(new UnsupportedOperationException("checkMergeOperator not implemented for Thrift"));
	}

	@Override
	public CompletableFuture<Void> deleteColumnAsync(long columnId) {
		return CompletableFuture.runAsync(() -> deleteColumn(columnId), executor);
	}

	@Override
	public CompletableFuture<Long> getColumnIdAsync(String name) {
		return CompletableFuture.supplyAsync(() -> getColumnId(name), executor);
	}

	@Override
	public <T> CompletableFuture<T> putAsync(long transactionOrUpdateId, long columnId, Keys keys, Buf value, RequestPut<? super Buf, T> requestType) {
		return CompletableFuture.supplyAsync(() -> put(transactionOrUpdateId, columnId, keys, value, requestType), executor);
	}

	@Override
	public <T> CompletableFuture<T> mergeAsync(long transactionOrUpdateId, long columnId, Keys keys, Buf value, RequestMerge<? super Buf, T> requestType) {
		return CompletableFuture.supplyAsync(() -> merge(transactionOrUpdateId, columnId, keys, value, requestType), executor);
	}

	@Override
	public <T> CompletableFuture<List<T>> putMultiAsync(long transactionOrUpdateId, long columnId, List<Keys> keys, List<Buf> values, RequestPut<? super Buf, T> requestType) {
		return CompletableFuture.supplyAsync(() -> putMulti(transactionOrUpdateId, columnId, keys, values, requestType), executor);
	}

	@Override
	public <T> CompletableFuture<List<T>> mergeMultiAsync(long transactionOrUpdateId, long columnId, List<Keys> keys, List<Buf> values, RequestMerge<? super Buf, T> requestType) {
		return CompletableFuture.supplyAsync(() -> mergeMulti(transactionOrUpdateId, columnId, keys, values, requestType), executor);
	}

	@Override
	public CompletableFuture<Void> putBatchAsync(long columnId, Publisher<KVBatch> batchPublisher, PutBatchMode mode) {
		return CompletableFuture.runAsync(() -> putBatch(columnId, batchPublisher, mode), executor);
	}

	@Override
	public CompletableFuture<Void> mergeBatchAsync(long columnId, Publisher<KVBatch> batchPublisher, MergeBatchMode mode) {
		return CompletableFuture.runAsync(() -> mergeBatch(columnId, batchPublisher, mode), executor);
	}

	@Override
	public <T> CompletableFuture<T> getAsync(long transactionOrUpdateId, long columnId, Keys keys, RequestGet<? super Buf, T> requestType) {
		return CompletableFuture.supplyAsync(() -> get(transactionOrUpdateId, columnId, keys, requestType), executor);
	}

	@Override
	public CompletableFuture<Long> openIteratorAsync(long transactionId, long columnId, Keys startKeysInclusive, Keys endKeysExclusive, boolean reverse, long timeoutMs) {
		return CompletableFuture.supplyAsync(() -> openIterator(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, timeoutMs), executor);
	}

	@Override
	public CompletableFuture<Void> closeIteratorAsync(long iteratorId) {
		return CompletableFuture.runAsync(() -> closeIterator(iteratorId), executor);
	}

	@Override
	public CompletableFuture<Void> seekToAsync(long iterationId, Keys keys) {
		return CompletableFuture.runAsync(() -> seekTo(iterationId, keys), executor);
	}

	@Override
	public <T> CompletableFuture<T> subsequentAsync(long iterationId, long skipCount, long takeCount, RequestIterate<? super Buf, T> requestType) {
		return CompletableFuture.supplyAsync(() -> subsequent(iterationId, skipCount, takeCount, requestType), executor);
	}

	@Override
	public <T> CompletableFuture<T> reduceRangeAsync(long transactionId, long columnId, Keys startKeysInclusive, Keys endKeysExclusive, boolean reverse, RequestReduceRange<? super KV, T> requestType, long timeoutMs) {
		return CompletableFuture.supplyAsync(() -> reduceRange(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs), executor);
	}

	@Override
	public <T> Publisher<T> getRangeAsync(long transactionId, long columnId, Keys startKeysInclusive, Keys endKeysExclusive, boolean reverse, RequestGetRange<? super KV, T> requestType, long timeoutMs) {
		return Flux.create(sink -> {
			executor.submit(() -> {
				try {
					Stream<T> stream = getRange(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs);
					stream.forEach(sink::next);
					sink.complete();
				} catch (Throwable e) {
					sink.error(e);
				}
			});
		});
	}

	@Override
	public CompletableFuture<Void> flushAsync() {
		return CompletableFuture.runAsync(this::flush, executor);
	}

	@Override
	public CompletableFuture<Void> compactAsync() {
		return CompletableFuture.runAsync(this::compact, executor);
	}

	@Override
	public CompletableFuture<Map<String, ColumnSchema>> getAllColumnDefinitionsAsync() {
		return CompletableFuture.supplyAsync(this::getAllColumnDefinitions, executor);
	}

    @Override
	public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> req) {
		return req.handleSync(this);
	}

	@Override
	public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> req) {
        if (req instanceof RocksDBAPICommand.RocksDBAPICommandStream) {
             // Let handleAsync dispatch to getRangeAsync etc
        }
		return req.handleAsync(this);
	}

	// --- Helpers ---

	private RocksDBException wrap(TException e) {
		if (e instanceof RocksDBThriftException re) {
			var type = RocksDBErrorType.valueOf(re.getErrorType().name());
			if (type == RocksDBErrorType.UPDATE_RETRY) {
				return new RocksDBRetryException();
			}
			return RocksDBException.of(
					type,
					re.getMessage()
			);
		}
		return RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
	}

	private List<ByteBuffer> mapKeys(Keys keys) {
		if (keys == null) return null;
		List<ByteBuffer> list = new ArrayList<>();
		for (Buf b : keys.keys()) {
			list.add(mapBuf(b));
		}
		return list;
	}

	private List<List<ByteBuffer>> mapKeysList(List<Keys> keysList) {
		if (keysList == null) return null;
		List<List<ByteBuffer>> result = new ArrayList<>(keysList.size());
		for (Keys k : keysList) {
			result.add(mapKeys(k));
		}
		return result;
	}

	private ByteBuffer mapBuf(Buf buf) {
		return buf == null ? null : Utils.asByteBuffer(buf);
	}

	private List<ByteBuffer> mapBufList(List<Buf> bufList) {
		if (bufList == null) return null;
		List<ByteBuffer> result = new ArrayList<>(bufList.size());
		for (Buf b : bufList) {
			result.add(mapBuf(b));
		}
		return result;
	}

	private Buf mapOptionalBinary(OptionalBinary opt) {
		if (opt == null || !opt.isSetValue()) return null;
		return Utils.fromByteBuffer(opt.value);
	}

	private it.cavallium.rockserver.core.common.Delta<Buf> mapDelta(it.cavallium.rockserver.core.common.api.Delta delta) {
		return new it.cavallium.rockserver.core.common.Delta<>(
				delta.isSetPrevious() ? Utils.fromByteBuffer(delta.previous) : null,
				delta.isSetCurrent() ? Utils.fromByteBuffer(delta.current) : null
		);
	}

	private List<Buf> mapOptionalBinaryList(List<OptionalBinary> list) {
		if (list == null) return Collections.emptyList();
		List<Buf> result = new ArrayList<>(list.size());
		for (OptionalBinary b : list) {
			result.add(mapOptionalBinary(b));
		}
		return result;
	}

	private KV mapKV(it.cavallium.rockserver.core.common.api.KV kv) {
		Buf[] keys = new Buf[kv.getKeys().size()];
		for (int i = 0; i < kv.getKeys().size(); i++) {
			keys[i] = Utils.fromByteBuffer(kv.getKeys().get(i));
		}
		return new KV(new Keys(keys), Utils.fromByteBuffer(kv.bufferForValue()));
	}

	private it.cavallium.rockserver.core.common.api.ColumnSchema mapSchema(ColumnSchema schema) {
		it.cavallium.rockserver.core.common.api.ColumnSchema s = new it.cavallium.rockserver.core.common.api.ColumnSchema();
		s.setFixedKeys(new ArrayList<>(schema.keys()));
		s.setVariableTailKeys(schema.variableTailKeys().stream()
				.map(t -> it.cavallium.rockserver.core.common.api.ColumnHashType.valueOf(t.name()))
				.collect(Collectors.toList()));
		s.setHasValue(schema.hasValue());
		if (schema.mergeOperatorName() != null) {
			s.setMergeOperatorName(schema.mergeOperatorName());
		}
		if (schema.mergeOperatorVersion() != null) {
			s.setMergeOperatorVersion(schema.mergeOperatorVersion());
		}
		return s;
	}

	private ColumnSchema mapSchemaFromThrift(it.cavallium.rockserver.core.common.api.ColumnSchema schema) {
		return ColumnSchema.of(
				it.unimi.dsi.fastutil.ints.IntArrayList.wrap(schema.getFixedKeys().stream().mapToInt(i -> i).toArray()),
				it.unimi.dsi.fastutil.objects.ObjectArrayList.wrap(schema.getVariableTailKeys().stream()
						.map(t -> it.cavallium.rockserver.core.common.ColumnHashType.valueOf(t.name()))
						.toArray(it.cavallium.rockserver.core.common.ColumnHashType[]::new)),
				schema.isHasValue(),
				schema.isSetMergeOperatorName() ? schema.getMergeOperatorName() : null,
				schema.isSetMergeOperatorVersion() ? schema.getMergeOperatorVersion() : null
		);
	}
}
