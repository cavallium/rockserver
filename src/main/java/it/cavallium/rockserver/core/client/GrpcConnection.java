package it.cavallium.rockserver.core.client;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestChanged;
import it.cavallium.rockserver.core.common.RequestType.RequestCurrent;
import it.cavallium.rockserver.core.common.RequestType.RequestDelta;
import it.cavallium.rockserver.core.common.RequestType.RequestExists;
import it.cavallium.rockserver.core.common.RequestType.RequestForUpdate;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestMulti;
import it.cavallium.rockserver.core.common.RequestType.RequestNothing;
import it.cavallium.rockserver.core.common.RequestType.RequestPrevious;
import it.cavallium.rockserver.core.common.RequestType.RequestPreviousPresence;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.UpdateContext;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc.RocksDBServiceBlockingStub;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc.RocksDBServiceFutureStub;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc.RocksDBServiceStub;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcConnection extends BaseConnection implements RocksDBAPI {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcConnection.class);
	private static final Executor DIRECT_EXECUTOR = MoreExecutors.directExecutor();
	private final ManagedChannel channel;
	private final RocksDBServiceBlockingStub blockingStub;
	private final RocksDBServiceStub asyncStub;
	private final RocksDBServiceFutureStub futureStub;
	private final URI address;

	public GrpcConnection(String name, HostAndPort address) {
		super(name);
		var channelBuilder = ManagedChannelBuilder
				.forAddress(address.host(), address.port())
				.usePlaintext();
		this.channel = channelBuilder.build();
		this.blockingStub = RocksDBServiceGrpc.newBlockingStub(channel);
		this.asyncStub = RocksDBServiceGrpc.newStub(channel);
		this.futureStub = RocksDBServiceGrpc.newFutureStub(channel);
		this.address = URI.create("http://" + address.host() + ":" + address.port());
	}

	@Override
	public URI getUrl() {
		return address;
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
	public <R> R requestSync(RocksDBAPICommand<R> req) {
		var asyncResponse = req.handleAsync(this);
		return asyncResponse
				.toCompletableFuture()
				.join();
	}

	@Override
	public CompletableFuture<Long> openTransactionAsync(long timeoutMs) throws RocksDBException {
		var request = OpenTransactionRequest.newBuilder()
				.setTimeoutMs(timeoutMs)
				.build();
		return toResponse(this.futureStub.openTransaction(request), OpenTransactionResponse::getTransactionId);
	}

	@Override
	public CompletableFuture<Boolean> closeTransactionAsync(long transactionId, boolean commit) throws RocksDBException {
		var request = CloseTransactionRequest.newBuilder()
				.setTransactionId(transactionId)
				.setCommit(commit)
				.build();
		return toResponse(this.futureStub.closeTransaction(request), CloseTransactionResponse::getSuccessful);
	}

	@Override
	public CompletableFuture<Void> closeFailedUpdateAsync(long updateId) throws RocksDBException {
		var request = CloseFailedUpdateRequest.newBuilder()
				.setUpdateId(updateId)
				.build();
		return toResponse(this.futureStub.closeFailedUpdate(request), _ -> null);
	}

	@Override
	public CompletableFuture<Long> createColumnAsync(String name, @NotNull ColumnSchema schema) throws RocksDBException {
		var request = CreateColumnRequest.newBuilder()
				.setName(name)
				.setSchema(mapColumnSchema(schema))
				.build();
		return toResponse(this.futureStub.createColumn(request), CreateColumnResponse::getColumnId);
	}

	@Override
	public CompletableFuture<Void> deleteColumnAsync(long columnId) throws RocksDBException {
		var request = DeleteColumnRequest.newBuilder()
				.setColumnId(columnId)
				.build();
		return toResponse(this.futureStub.deleteColumn(request), _ -> null);
	}

	@Override
	public CompletableFuture<Long> getColumnIdAsync(@NotNull String name) throws RocksDBException {
		var request = GetColumnIdRequest.newBuilder()
				.setName(name)
				.build();
		return toResponse(this.futureStub.getColumnId(request), GetColumnIdResponse::getColumnId);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> CompletableFuture<T> putAsync(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull MemorySegment value,
			RequestPut<? super MemorySegment, T> requestType) throws RocksDBException {
		var request = PutRequest.newBuilder()
				.setTransactionOrUpdateId(transactionOrUpdateId)
				.setColumnId(columnId)
				.setData(mapKV(keys, value))
				.build();
		return (CompletableFuture<T>) switch (requestType) {
			case RequestNothing<?> _ -> toResponse(this.futureStub.put(request), _ -> null);
			case RequestPrevious<?> _ ->
					toResponse(this.futureStub.putGetPrevious(request), GrpcConnection::mapPrevious);
			case RequestDelta<?> _ ->
					toResponse(this.futureStub.putGetDelta(request), GrpcConnection::mapDelta);
			case RequestChanged<?> _ ->
					toResponse(this.futureStub.putGetChanged(request), Changed::getChanged);
			case RequestType.RequestPreviousPresence<?> _ ->
					toResponse(this.futureStub.putGetPreviousPresence(request), PreviousPresence::getPresent);
		};
	}

	@Override
	public <T> CompletableFuture<List<T>> putMultiAsync(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			@NotNull List<@NotNull Keys> allKeys,
			@NotNull List<@NotNull MemorySegment> allValues,
			RequestPut<? super MemorySegment, T> requestType) throws RocksDBException {
		var count = allKeys.size();
		if (count != allValues.size()) {
			throw new IllegalArgumentException("Keys length is different than values length! "
					+ count + " != " + allValues.size());
		}

		CompletableFuture<List<T>> responseObserver;

		if (requestType instanceof RequestType.RequestNothing<?>) {
			var putBatchRequestBuilder = PutBatchRequest.newBuilder()
					.setTransactionOrUpdateId(transactionOrUpdateId)
					.setColumnId(columnId);

			var it1 = allKeys.iterator();
			var it2 = allValues.iterator();

			while (it1.hasNext()) {
				var k = it1.next();
				var v = it2.next();
				putBatchRequestBuilder.addData(mapKV(k, v));
			}

			return toResponse(futureStub.putBatch(putBatchRequestBuilder.build()), _ -> null);
		}

		var initialRequest = PutMultiRequest.newBuilder()
				.setInitialRequest(PutMultiInitialRequest.newBuilder()
						.setTransactionOrUpdateId(transactionOrUpdateId)
						.setColumnId(columnId)
						.build())
				.build();

		StreamObserver<PutMultiRequest> requestPublisher = switch (requestType) {
			case RequestPrevious<?> _ -> {
				var thisResponseObserver = new CollectListMappedStreamObserver<Previous, @Nullable MemorySegment>(
						GrpcConnection::mapPrevious, count);
				//noinspection unchecked
				responseObserver = (CompletableFuture<List<T>>) (CompletableFuture<?>) thisResponseObserver;
				yield this.asyncStub.putMultiGetPrevious(thisResponseObserver);
			}
			case RequestDelta<?> _ -> {
				var thisResponseObserver = new CollectListMappedStreamObserver<>(GrpcConnection::mapDelta, count);
				//noinspection unchecked
				responseObserver = (CompletableFuture<List<T>>) (CompletableFuture<?>) thisResponseObserver;
				yield this.asyncStub.putMultiGetDelta(thisResponseObserver);
			}
			case RequestChanged<?> _ -> {
				var thisResponseObserver = new CollectListMappedStreamObserver<>(Changed::getChanged, count);
				//noinspection unchecked
				responseObserver = (CompletableFuture<List<T>>) (CompletableFuture<?>) thisResponseObserver;
				yield this.asyncStub.putMultiGetChanged(thisResponseObserver);
			}
			case RequestPreviousPresence<?> _ -> {
				var thisResponseObserver = new CollectListMappedStreamObserver<>(PreviousPresence::getPresent, count);
				//noinspection unchecked
				responseObserver = (CompletableFuture<List<T>>) (CompletableFuture<?>) thisResponseObserver;
				yield this.asyncStub.putMultiGetPreviousPresence(thisResponseObserver);
			}
		};

		requestPublisher.onNext(initialRequest);

		var it1 = allKeys.iterator();
		var it2 = allValues.iterator();

		while (it1.hasNext()) {
			var keys = it1.next();
			var value = it2.next();
			requestPublisher.onNext(PutMultiRequest.newBuilder()
					.setData(mapKV(keys, value))
					.build());
		}

		return responseObserver;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> CompletableFuture<T> getAsync(Arena arena,
			long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			RequestGet<? super MemorySegment, T> requestType) throws RocksDBException {
		var request = GetRequest.newBuilder()
				.setTransactionOrUpdateId(transactionOrUpdateId)
				.setColumnId(columnId)
				.addAllKeys(mapKeys(keys))
				.build();
		if (requestType instanceof RequestType.RequestForUpdate<?>) {
			return toResponse(this.futureStub.getForUpdate(request), x -> (T) new UpdateContext<>(
					x.hasPrevious() ? mapByteString(x.getPrevious()) : null,
					x.getUpdateId()
			));
		} else {
			return toResponse(this.futureStub.get(request), x -> switch (requestType) {
				case RequestNothing<?> _ -> null;
				case RequestType.RequestCurrent<?> _ -> x.hasValue() ? (T) mapByteString(x.getValue()) : null;
				case RequestType.RequestForUpdate<?> _ -> throw new IllegalStateException();
				case RequestType.RequestExists<?> _ -> (T) (Boolean) x.hasValue();
			});
		}
	}

	@Override
	public CompletableFuture<Long> openIteratorAsync(Arena arena,
			long transactionId,
			long columnId,
			@NotNull Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException {
		var request = OpenIteratorRequest.newBuilder()
				.setTransactionId(transactionId)
				.setColumnId(columnId)
				.addAllStartKeysInclusive(mapKeys(startKeysInclusive))
				.addAllEndKeysExclusive(mapKeys(endKeysExclusive))
				.setReverse(reverse)
				.setTimeoutMs(timeoutMs)
				.build();
		return toResponse(this.futureStub.openIterator(request), OpenIteratorResponse::getIteratorId);
	}

	@Override
	public CompletableFuture<Void> closeIteratorAsync(long iteratorId) throws RocksDBException {
		var request = CloseIteratorRequest.newBuilder()
				.setIteratorId(iteratorId)
				.build();
		return toResponse(this.futureStub.closeIterator(request), _ -> null);
	}

	@Override
	public CompletableFuture<Void> seekToAsync(Arena arena, long iterationId, @NotNull Keys keys) throws RocksDBException {
		var request = SeekToRequest.newBuilder()
				.setIterationId(iterationId)
				.addAllKeys(mapKeys(keys))
				.build();
		return toResponse(this.futureStub.seekTo(request), _ -> null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> CompletableFuture<T> subsequentAsync(Arena arena,
			long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super MemorySegment, T> requestType) throws RocksDBException {
		var request = SubsequentRequest.newBuilder()
				.setIterationId(iterationId)
				.setSkipCount(skipCount)
				.setTakeCount(takeCount)
				.build();
		return switch (requestType) {
			case RequestNothing<?> _ -> toResponse(this.futureStub.subsequent(request), _ -> null);
			case RequestExists<?> _ ->
					(CompletableFuture<T>) toResponse(this.futureStub.subsequentExists(request), PreviousPresence::getPresent);
			case RequestMulti<?> _ -> {
				CollectListMappedStreamObserver<KV, MemorySegment> responseObserver
						= new CollectListMappedStreamObserver<>(kv -> mapByteString(kv.getValue()));
				this.asyncStub.subsequentMultiGet(request, responseObserver);
				yield (CompletableFuture<T>) responseObserver;
			}
		};
	}

	private static it.cavallium.rockserver.core.common.Delta<MemorySegment> mapDelta(Delta x) {
		return new it.cavallium.rockserver.core.common.Delta<>(
				x.hasPrevious() ? mapByteString(x.getPrevious()) : null,
				x.hasCurrent() ? mapByteString(x.getCurrent()) : null
		);
	}

	@Nullable
	private static MemorySegment mapPrevious(Previous x) {
		return x.hasPrevious() ? mapByteString(x.getPrevious()) : null;
	}

	private static MemorySegment mapByteString(ByteString data) {
		return data != null ? MemorySegment.ofBuffer(data.asReadOnlyByteBuffer()) : null;
	}

	private static KV mapKV(@NotNull Keys keys, @NotNull MemorySegment value) {
		return KV.newBuilder()
				.addAllKeys(mapKeys(keys))
				.setValue(mapValue(value))
				.build();
	}

	private static Iterable<? extends ByteString> mapKeys(Keys keys) {
		if (keys == null) return List.of();
		return Iterables.transform(Arrays.asList(keys.keys()), k -> UnsafeByteOperations.unsafeWrap(k.asByteBuffer()));
	}

	private static ByteString mapValue(@NotNull MemorySegment value) {
		return UnsafeByteOperations.unsafeWrap(value.asByteBuffer());
	}

	private static it.cavallium.rockserver.core.common.api.proto.ColumnSchema mapColumnSchema(@NotNull ColumnSchema schema) {
		return it.cavallium.rockserver.core.common.api.proto.ColumnSchema.newBuilder()
				.addAllFixedKeys(mapFixedKeys(schema))
				.addAllVariableTailKeys(mapVariableTailKeys(schema))
				.setHasValue(schema.hasValue())
				.build();
	}

	private static Iterable<Integer> mapFixedKeys(@NotNull ColumnSchema schema) {
		var result = new IntArrayList(schema.fixedLengthKeysCount());
		for (int i = 0; i < schema.fixedLengthKeysCount(); i++) {
			result.add(schema.key(i));
		}
		return result;
	}

	private static Iterable<ColumnHashType> mapVariableTailKeys(@NotNull ColumnSchema schema) {
		var result = new ArrayList<ColumnHashType>(schema.variableTailKeys().size());
		for (it.cavallium.rockserver.core.common.ColumnHashType variableTailKey : schema.variableTailKeys()) {
			result.add(switch (variableTailKey) {
				case XXHASH32 -> ColumnHashType.XXHASH32;
				case XXHASH8 -> ColumnHashType.XXHASH8;
				case ALLSAME8 -> ColumnHashType.ALLSAME8;
			});
		}
		return result;
	}

	private static <T, U> CompletableFuture<U> toResponse(ListenableFuture<T> listenableFuture, Function<T, U> mapper) {
		var cf = new CompletableFuture<U>() {
			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				boolean cancelled = listenableFuture.cancel(mayInterruptIfRunning);
				super.cancel(cancelled);
				return cancelled;
			}
		};

		Futures.addCallback(listenableFuture, new FutureCallback<>() {
			@Override
			public void onSuccess(T result) {
				cf.complete(mapper.apply(result));
			}

			@Override
			public void onFailure(@NotNull Throwable t) {
				cf.completeExceptionally(t);
			}
		}, DIRECT_EXECUTOR);

		return cf;
	}

	private static <T> CompletableFuture<T> toResponse(ListenableFuture<T> listenableFuture) {
		var cf = new CompletableFuture<T>() {
			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				boolean cancelled = listenableFuture.cancel(mayInterruptIfRunning);
				super.cancel(cancelled);
				return cancelled;
			}
		};

		Futures.addCallback(listenableFuture, new FutureCallback<>() {
			@Override
			public void onSuccess(T result) {
				cf.complete(result);
			}

			@Override
			public void onFailure(@NotNull Throwable t) {
				cf.completeExceptionally(t);
			}
		}, DIRECT_EXECUTOR);

		return cf;
	}

	@Override
	public void close() {
		try {
			if (this.channel != null) {
				this.channel.shutdown();
			}
		} catch (Exception ex) {
			LOG.error("Failed to close channel", ex);
		}
		try {
			if (this.channel != null) {
				this.channel.awaitTermination(1, TimeUnit.MINUTES);
			}
		} catch (InterruptedException e) {
			LOG.error("Failed to wait channel termination", e);
			try {
				this.channel.shutdownNow();
			} catch (Exception ex) {
				LOG.error("Failed to close channel", ex);
			}
		}
	}
}
