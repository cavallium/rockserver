package it.cavallium.rockserver.core.client;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.FirstAndLast;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestType.RequestChanged;
import it.cavallium.rockserver.core.common.RequestType.RequestDelta;
import it.cavallium.rockserver.core.common.RequestType.RequestExists;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestMulti;
import it.cavallium.rockserver.core.common.RequestType.RequestNothing;
import it.cavallium.rockserver.core.common.RequestType.RequestPrevious;
import it.cavallium.rockserver.core.common.RequestType.RequestPreviousPresence;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.api.proto.ColumnHashType;
import it.cavallium.rockserver.core.common.api.proto.Delta;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc.RocksDBServiceFutureStub;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc.RocksDBServiceStub;
import it.unimi.dsi.fastutil.ints.Int2ObjectFunction;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static it.cavallium.rockserver.core.common.Utils.toMemorySegment;

public class GrpcConnection extends BaseConnection implements RocksDBAPI {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcConnection.class);
	private static final Executor DIRECT_EXECUTOR = MoreExecutors.directExecutor();
	private final ManagedChannel channel;
	private final RocksDBServiceStub asyncStub;
	private final RocksDBServiceFutureStub futureStub;
	private final ReactorRocksDBServiceGrpc.ReactorRocksDBServiceStub reactiveStub;
	private final URI address;

	private GrpcConnection(String name, SocketAddress socketAddress, URI address) {
		super(name);
		NettyChannelBuilder channelBuilder;
		if (socketAddress instanceof InetSocketAddress inetSocketAddress) {
			channelBuilder = NettyChannelBuilder
					.forAddress(inetSocketAddress.getHostString(), inetSocketAddress.getPort());
		} else {
			channelBuilder = NettyChannelBuilder
					.forAddress(socketAddress);
		}

		channelBuilder
				.directExecutor()
				.usePlaintext();
		if (socketAddress instanceof DomainSocketAddress _) {
			channelBuilder
					.eventLoopGroup(new EpollEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2))
					.channelType(EpollDomainSocketChannel.class);
		} else {
			channelBuilder
					.eventLoopGroup(new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2))
					.channelType(NioSocketChannel.class);
		}
		this.channel = channelBuilder.build();
		this.asyncStub = RocksDBServiceGrpc.newStub(channel);
		this.futureStub = RocksDBServiceGrpc.newFutureStub(channel);
		this.reactiveStub = ReactorRocksDBServiceGrpc.newReactorStub(channel);
		this.address = address;
	}

	public static GrpcConnection forHostAndPort(String name, HostAndPort address) {
		return new GrpcConnection(name,
				new InetSocketAddress(address.host(), address.port()),
				URI.create("http://" + address.host() + ":" + address.port())
		);
	}

	public static GrpcConnection forPath(String name, Path unixSocketPath) {
		return new GrpcConnection(name,
				new DomainSocketAddress(unixSocketPath.toFile()),
				URI.create("unix://" + unixSocketPath)
		);
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

	@SuppressWarnings("unchecked")
    @Override
	public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> req) {
		return (RS) switch (req) {
			case RocksDBAPICommand.RocksDBAPICommandSingle<?> _ -> {
				var asyncResponse = (CompletableFuture<R>) req.handleAsync(this);
				yield asyncResponse.join();
			}
            case RocksDBAPICommand.RocksDBAPICommandStream<?> _ -> {
				var asyncResponse = (Publisher<R>) req.handleAsync(this);
				yield Flux.from(asyncResponse).toStream();
			}
        };
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

	@SuppressWarnings("unchecked")
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

		if (requestType instanceof RequestType.RequestNothing<?> && transactionOrUpdateId == 0L) {
			return putBatchAsync(columnId, Flux.just(new KVBatch(allKeys, allValues)), PutBatchMode.WRITE_BATCH)
					.thenApply(_ -> List.of());
		}

		var initialRequest = PutMultiRequest.newBuilder()
				.setInitialRequest(PutMultiInitialRequest.newBuilder()
						.setTransactionOrUpdateId(transactionOrUpdateId)
						.setColumnId(columnId)
						.build())
				.build();

		Mono<PutMultiRequest> initialRequestMono = Mono.just(initialRequest);
		Flux<PutMultiRequest> dataRequestsFlux = Flux.fromIterable(() -> GrpcConnection
				.map(allKeys.iterator(), allValues.iterator(), (keys, value) -> PutMultiRequest.newBuilder()
						.setData(mapKV(keys, value))
						.build()));
		var inputRequests = initialRequestMono.concatWith(dataRequestsFlux);

		return (CompletableFuture<List<T>>) (switch (requestType) {
			case RequestNothing<?> _ ->
					this.reactiveStub.putMulti(inputRequests)
							.ignoreElement()
							.toFuture();
			case RequestPrevious<?> _ ->
					this.reactiveStub.putMultiGetPrevious(inputRequests)
							.collect(() -> new ArrayList<@Nullable MemorySegment>(),
									(list, value) -> list.add(GrpcConnection.mapPrevious(value)))
							.toFuture();
			case RequestDelta<?> _ ->
					this.reactiveStub.putMultiGetDelta(inputRequests)
							.map(GrpcConnection::mapDelta)
							.collectList()
							.toFuture();
			case RequestChanged<?> _ ->
					this.reactiveStub.putMultiGetChanged(inputRequests)
							.map(Changed::getChanged)
							.collectList()
							.toFuture();
			case RequestPreviousPresence<?> _ ->
					this.reactiveStub.putMultiGetPreviousPresence(inputRequests)
							.map(PreviousPresence::getPresent)
							.collectList()
							.toFuture();
		});
	}

	@Override
	public CompletableFuture<Void> putBatchAsync(long columnId,
												 @NotNull Publisher<@NotNull KVBatch> batchPublisher,
												 @NotNull PutBatchMode mode) throws RocksDBException {
		var initialRequest = Mono.just(PutBatchRequest.newBuilder()
				.setInitialRequest(PutBatchInitialRequest.newBuilder()
						.setColumnId(columnId)
						.setMode(switch (mode) {
							case WRITE_BATCH -> it.cavallium.rockserver.core.common.api.proto.PutBatchMode.WRITE_BATCH;
							case WRITE_BATCH_NO_WAL -> it.cavallium.rockserver.core.common.api.proto.PutBatchMode.WRITE_BATCH_NO_WAL;
							case SST_INGESTION -> it.cavallium.rockserver.core.common.api.proto.PutBatchMode.SST_INGESTION;
							case SST_INGEST_BEHIND -> it.cavallium.rockserver.core.common.api.proto.PutBatchMode.SST_INGEST_BEHIND;
						})
						.build())
				.build());
		var nextRequests = Flux.from(batchPublisher).map(batch -> {
			var request = PutBatchRequest.newBuilder();
			request.setData(mapKVBatch(batch));
			return request.build();
		});
		var inputFlux = initialRequest.concatWith(nextRequests);
		return reactiveStub.putBatch(inputFlux).then().toFuture();
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
			case RequestMulti<?> _ ->
					(CompletableFuture<T>) this.reactiveStub.subsequentMultiGet(request)
							.map(kv -> mapByteString(kv.getValue()))
							.collectList()
							.toFuture();
		};
	}

	@SuppressWarnings("unchecked")
    @Override
	public <T> CompletableFuture<T> reduceRangeAsync(Arena arena, long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.RequestReduceRange<? super it.cavallium.rockserver.core.common.KV, T> requestType, long timeoutMs) throws RocksDBException {
		var request = GetRangeRequest.newBuilder()
				.setTransactionId(transactionId)
				.setColumnId(columnId)
				.addAllStartKeysInclusive(mapKeys(startKeysInclusive))
				.addAllEndKeysExclusive(mapKeys(endKeysExclusive))
				.setReverse(reverse)
				.setTimeoutMs(timeoutMs)
				.build();
		return (CompletableFuture<T>) switch (requestType) {
			case RequestType.RequestGetFirstAndLast<?> _ ->
					toResponse(this.futureStub.reduceRangeFirstAndLast(request), result -> new FirstAndLast<>(
							result.hasFirst() ? mapKV(arena, result.getFirst()) : null,
							result.hasLast() ? mapKV(arena, result.getLast()) : null
					));
			case RequestType.RequestEntriesCount<?> _ ->
					toResponse(this.futureStub.reduceRangeEntriesCount(request), EntriesCount::getCount);
			default -> throw new UnsupportedOperationException();
		};
	}

	@SuppressWarnings("unchecked")
    @Override
	public <T> Publisher<T> getRangeAsync(Arena arena, long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.RequestGetRange<? super it.cavallium.rockserver.core.common.KV, T> requestType, long timeoutMs) throws RocksDBException {
		var request = GetRangeRequest.newBuilder()
				.setTransactionId(transactionId)
				.setColumnId(columnId)
				.addAllStartKeysInclusive(mapKeys(startKeysInclusive))
				.addAllEndKeysExclusive(mapKeys(endKeysExclusive))
				.setReverse(reverse)
				.setTimeoutMs(timeoutMs)
				.build();
		return (Publisher<T>) switch (requestType) {
			case RequestType.RequestGetAllInRange<?> _ -> reactiveStub.getAllInRange(request)
					.map(kv -> mapKV(arena, kv));
		};
	}

	private static it.cavallium.rockserver.core.common.Delta<MemorySegment> mapDelta(Delta x) {
		return new it.cavallium.rockserver.core.common.Delta<>(
				x.hasPrevious() ? mapByteString(x.getPrevious()) : null,
				x.hasCurrent() ? mapByteString(x.getCurrent()) : null
		);
	}

	public static <A, B, C> Iterator<C> map(Iterator<A> a, Iterator<B> b, BiFunction<A, B, C> f) {
		return new Iterator<>() {
			public boolean hasNext() {
				return a.hasNext() && b.hasNext(); // This uses the shorter of the two `Iterator`s.
			}

			public C next() {
				return f.apply(a.next(), b.next());
			}
		};
	}

	@Nullable
	private static MemorySegment mapPrevious(Previous x) {
		return x.hasPrevious() ? mapByteString(x.getPrevious()) : null;
	}

	private static MemorySegment mapByteString(ByteString data) {
		return data != null ? MemorySegment.ofBuffer(data.asReadOnlyByteBuffer()) : null;
	}

	private static it.cavallium.rockserver.core.common.api.proto.KVBatch mapKVBatch(@NotNull KVBatch kvBatch) {
		return it.cavallium.rockserver.core.common.api.proto.KVBatch.newBuilder()
				.addAllEntries(mapKVList(kvBatch.keys(), kvBatch.values()))
				.build();
	}

	private static Iterable<KV> mapKVList(@NotNull List<Keys> keys, @NotNull List<MemorySegment> values) {
		return new Iterable<>() {
			@Override
			public @NotNull Iterator<KV> iterator() {
				var it1 = keys.iterator();
				var it2 = values.iterator();
				return new Iterator<>() {
					@Override
					public boolean hasNext() {
						return it1.hasNext();
					}

					@Override
					public KV next() {
						return mapKV(it1.next(), it2.next());
					}
				};
			}
		};
	}

	private static KV mapKV(@NotNull Keys keys, @NotNull MemorySegment value) {
		return KV.newBuilder()
				.addAllKeys(mapKeys(keys))
				.setValue(mapValue(value))
				.build();
	}

	private static it.cavallium.rockserver.core.common.KV mapKV(Arena arena, @NotNull KV entry) {
		return new it.cavallium.rockserver.core.common.KV(
				mapKeys(arena, entry.getKeysCount(), entry::getKeys),
				toMemorySegment(arena, entry.getValue())
		);
	}

	private static Keys mapKeys(Arena arena, int count, Int2ObjectFunction<ByteString> keyGetterAt) {
		var segments = new MemorySegment[count];
		for (int i = 0; i < count; i++) {
			segments[i] = toMemorySegment(arena, keyGetterAt.apply(i));
		}
		return new Keys(segments);
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
				cf.completeExceptionally(mapGrpcStatusError(t));
			}
		}, DIRECT_EXECUTOR);

		return cf;
	}

	private static final String grpcRocksDbErrorPrefixString = "RocksDBError: [uid:";

	private static Throwable mapGrpcStatusError(@NotNull Throwable t) {
		if (t instanceof StatusRuntimeException statusRuntimeException
				&& statusRuntimeException.getStatus() == Status.INTERNAL
				&& statusRuntimeException.getStatus().getDescription() != null
				&& statusRuntimeException.getStatus().getDescription().startsWith(grpcRocksDbErrorPrefixString)) {
			var desc = statusRuntimeException.getStatus().getDescription();
			var closeIndex = desc.indexOf(']');
			var errorCode = desc.substring(grpcRocksDbErrorPrefixString.length(), closeIndex);
			var errorDescription = desc.substring(closeIndex + 1);
			return RocksDBException.of(RocksDBErrorType.valueOf(errorCode), errorDescription);
		} else {
			return t;
		}
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
