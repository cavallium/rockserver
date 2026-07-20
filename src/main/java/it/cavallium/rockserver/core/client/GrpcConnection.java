package it.cavallium.rockserver.core.client;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.nio.NioIoHandler;
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
import it.cavallium.rockserver.core.common.RequestType.RequestMerge;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.SerializedKVBatch.SerializedKVBatchRef;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.api.proto.ColumnHashType;
import it.cavallium.rockserver.core.common.api.proto.Delta;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc.RocksDBServiceFutureStub;
import it.unimi.dsi.fastutil.ints.Int2IntFunction;
import it.unimi.dsi.fastutil.ints.Int2ObjectFunction;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.cavallium.buffer.Buf;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;

import static it.cavallium.rockserver.core.common.Utils.toBuf;

public class GrpcConnection extends BaseConnection implements RocksDBAPI {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcConnection.class);
    private static final Executor DIRECT_EXECUTOR = MoreExecutors.directExecutor();
    private static final int CHANNEL_SHUTDOWN_TIMEOUT_SECONDS = 30;
    private final ManagedChannel channel;
    private final EventLoopGroup eventLoopGroup;
    private final RocksDBServiceFutureStub futureStub;
    private final ReactorRocksDBServiceGrpc.ReactorRocksDBServiceStub reactiveStub;
    private final URI address;
    private final AtomicBoolean closed = new AtomicBoolean();

    @SuppressWarnings("unused")
    private GrpcConnection(String name, URI address, ManagedChannel channel, EventLoopGroup eventLoopGroup) {
        super(name);
        this.channel = channel;
        this.eventLoopGroup = eventLoopGroup;
        this.futureStub = null;
        this.reactiveStub = null;
        this.address = address;
    }

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
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(5, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .enableRetry()
                .maxRetryAttempts(Integer.MAX_VALUE)
                .defaultServiceConfig(Map.of(
                        "methodConfig", List.of(Map.of(
                                "name", List.of(Map.of()),
                                "retryPolicy", Map.of(
                                        "maxAttempts", 1000000000.0,
                                        "initialBackoff", "0.5s",
                                        "maxBackoff", "30s",
                                        "backoffMultiplier", 2.0,
                                        "retryableStatusCodes", List.of("UNAVAILABLE", "RESOURCE_EXHAUSTED", "ABORTED")
                                )
                        ))
                ));
        EventLoopGroup eventLoopGroup;
        if (socketAddress instanceof DomainSocketAddress _) {
            eventLoopGroup = new MultiThreadIoEventLoopGroup(
                    Runtime.getRuntime().availableProcessors() * 2,
                    EpollIoHandler.newFactory()
            );
            channelBuilder
                    .eventLoopGroup(eventLoopGroup)
                    .channelType(EpollDomainSocketChannel.class);
        } else {
            eventLoopGroup = new MultiThreadIoEventLoopGroup(
                    Runtime.getRuntime().availableProcessors() * 2,
                    NioIoHandler.newFactory()
            );
            channelBuilder
                    .eventLoopGroup(eventLoopGroup)
                    .channelType(NioSocketChannel.class);
        }
        channelBuilder.intercept(new RetryLoggingInterceptor());
        this.eventLoopGroup = eventLoopGroup;
        this.channel = channelBuilder.build();
        this.futureStub = RocksDBServiceGrpc.newFutureStub(channel);
        this.reactiveStub = ReactorRocksDBServiceGrpc.newReactorStub(channel);
        this.address = address;
    }

    @SuppressWarnings("HttpUrlsUsage")
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
        requireOpen();
        return (RS) switch (req) {
            case RocksDBAPICommand.RocksDBAPICommandSingle<?> _ -> {
                var asyncResponse = (CompletableFuture<R>) req.handleAsync(this);
                yield awaitSyncResponse(asyncResponse);
            }
            case RocksDBAPICommand.RocksDBAPICommandStream<?> _ -> {
                var asyncResponse = (Publisher<R>) req.handleAsync(this);
                yield Flux.from(asyncResponse).toStream();
            }
        };
    }

    @Override
    public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> req) {
        requireOpen();
        return req.handleAsync(this);
    }

    @Override
    public CompletableFuture<Long> openTransactionAsync(long timeoutMs) throws RocksDBException {
        var request = OpenTransactionRequest.newBuilder()
                .setTimeoutMs(timeoutMs)
                .build();
        return toResponse(futureStub().openTransaction(request), OpenTransactionResponse::getTransactionId);
    }

    @Override
    public CompletableFuture<Boolean> closeTransactionAsync(long transactionId, boolean commit) throws RocksDBException {
        var request = CloseTransactionRequest.newBuilder()
                .setTransactionId(transactionId)
                .setCommit(commit)
                .build();
        return toResponse(futureStub().closeTransaction(request), CloseTransactionResponse::getSuccessful);
    }

    @Override
    public CompletableFuture<Void> closeFailedUpdateAsync(long updateId) throws RocksDBException {
        var request = CloseFailedUpdateRequest.newBuilder()
                .setUpdateId(updateId)
                .build();
        return toResponse(futureStub().closeFailedUpdate(request), _ -> null);
    }

    @Override
    public CompletableFuture<Long> createColumnAsync(String name,
                                                     @NotNull ColumnSchema schema) throws RocksDBException {
        var requestBuilder = CreateColumnRequest.newBuilder()
                .setName(name)
                .setSchema(mapColumnSchema(schema));
        var request = requestBuilder.build();
        return toResponse(futureStub().createColumn(request), CreateColumnResponse::getColumnId);
    }

    @Override
    public CompletableFuture<Long> uploadMergeOperatorAsync(String name, String className, byte[] jarData) {
        return toResponse(futureStub().uploadMergeOperator(UploadMergeOperatorRequest.newBuilder()
                .setOperatorName(name)
                .setClassName(className)
                .setJarPayload(ByteString.copyFrom(jarData))
                .build()), UploadMergeOperatorResponse::getVersion);
    }

    @Override
    public CompletableFuture<Long> checkMergeOperatorAsync(String name, byte[] hash) {
        return toResponse(futureStub().checkMergeOperator(CheckMergeOperatorRequest.newBuilder()
                .setOperatorName(name)
                .setHash(UnsafeByteOperations.unsafeWrap(hash))
                .build()), resp -> resp.hasVersion() ? resp.getVersion() : null);
    }

    @Override
    public CompletableFuture<Void> deleteColumnAsync(long columnId) throws RocksDBException {
        var request = DeleteColumnRequest.newBuilder()
                .setColumnId(columnId)
                .build();
        return toResponse(futureStub().deleteColumn(request), _ -> null);
    }

    @Override
    public CompletableFuture<Long> getColumnIdAsync(@NotNull String name) throws RocksDBException {
        var request = GetColumnIdRequest.newBuilder()
                .setName(name)
                .build();
        return toResponse(futureStub().getColumnId(request), GetColumnIdResponse::getColumnId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> CompletableFuture<T> putAsync(long transactionOrUpdateId,
                                             long columnId,
                                             @NotNull Keys keys,
                                             @NotNull Buf value,
                                             RequestPut<? super Buf, T> requestType) throws RocksDBException {
        var request = PutRequest.newBuilder()
                .setTransactionOrUpdateId(transactionOrUpdateId)
                .setColumnId(columnId)
                .setData(mapKV(keys, value))
                .build();
        if (requestType == null) {
            throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Null request type");
        }
        return (CompletableFuture<T>) switch (requestType) {
            case RequestNothing<?> _ -> toResponse(futureStub().put(request), _ -> null);
            case RequestPrevious<?> _ ->
                    toResponse(futureStub().putGetPrevious(request), GrpcConnection::mapPrevious);
            case RequestDelta<?> _ -> toResponse(futureStub().putGetDelta(request), GrpcConnection::mapDelta);
            case RequestChanged<?> _ -> toResponse(futureStub().putGetChanged(request), Changed::getChanged);
            case RequestType.RequestPreviousPresence<?> _ ->
                    toResponse(futureStub().putGetPreviousPresence(request), PreviousPresence::getPresent);
        };
    }

    @Override
    public <T> CompletableFuture<T> deleteAsync(long transactionOrUpdateId,
                                                long columnId,
                                                @NotNull Keys keys,
                                                RequestType.RequestDelete<? super Buf, T> requestType) throws RocksDBException {
        if (closed.get()) {
            return CompletableFuture.failedFuture(closedException());
        }
        return CompletableFuture.failedFuture(unsupportedGrpcOperation("delete"));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> CompletableFuture<T> mergeAsync(long transactionOrUpdateId,
                                               long columnId,
                                               @NotNull Keys keys,
                                               @NotNull Buf value,
                                               RequestMerge<? super Buf, T> requestType) throws RocksDBException {
        var request = MergeRequest.newBuilder()
                .setTransactionOrUpdateId(transactionOrUpdateId)
                .setColumnId(columnId)
                .setData(mapKV(keys, value))
                .build();
        if (requestType == null) {
            throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Null request type");
        }
        return (CompletableFuture<T>) switch (requestType) {
            case RequestNothing<?> _ -> toResponse(futureStub().merge(request), _ -> null);
            case RequestType.RequestMerged<?> _ -> toResponse(futureStub().mergeGetMerged(request), x ->
                    x.hasMerged() ? mapByteString(x.getMerged()) : null);
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> CompletableFuture<List<T>> putMultiAsync(long transactionOrUpdateId,
                                                        long columnId,
                                                        @NotNull List<@NotNull Keys> allKeys,
                                                        @NotNull List<@NotNull Buf> allValues,
                                                        RequestPut<? super Buf, T> requestType) throws RocksDBException {
        if (requestType == null) {
            throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Null request type");
        }
        var requiredKeys = requireGrpcField(allKeys, "gRPC client multi-key entries");
        var requiredValues = requireGrpcField(allValues, "gRPC client value entries");
        var count = requiredKeys.size();
        requireMatchingEntryCount("gRPC client multi-write", count, requiredValues.size());

        if (requestType instanceof RequestType.RequestNothing<?> && transactionOrUpdateId == 0L && count > 1000) {
            return FutureUtils.mapCancellation(Flux.range(0, (count + 999) / 1000)
                    .concatMap(i -> {
                        int start = i * 1000;
                        int end = Math.min(count, start + 1000);
                        return Mono.fromFuture(() -> putMultiAsync(
                                transactionOrUpdateId,
                                columnId,
                                requiredKeys.subList(start, end),
                                requiredValues.subList(start, end),
                                requestType
                        ));
                    })
                    .then()
                    .toFuture(), _ -> List.of());
        }

        var initialRequest = PutMultiRequest.newBuilder()
                .setInitialRequest(PutMultiInitialRequest.newBuilder()
                        .setTransactionOrUpdateId(transactionOrUpdateId)
                        .setColumnId(columnId)
                        .build())
                .build();

        Mono<PutMultiRequest> initialRequestMono = Mono.just(initialRequest);
        Flux<PutMultiRequest> dataRequestsFlux = Flux.fromIterable(() -> GrpcConnection
                .map(requiredKeys.iterator(), requiredValues.iterator(), (keys, value) -> PutMultiRequest.newBuilder()
                        .setData(mapKV(keys, value))
                        .build()));
        var inputRequests = initialRequestMono.concatWith(dataRequestsFlux);

        return (CompletableFuture<List<T>>) (switch (requestType) {
            case RequestNothing<?> _ -> toResponse(reactiveStub().putMulti(inputRequests)
                    .ignoreElement()
                    .toFuture());
            case RequestPrevious<?> _ -> toResponse(reactiveStub().putMultiGetPrevious(inputRequests)
                    .collect(() -> new ArrayList<@Nullable Buf>(),
                            (list, value) -> list.add(GrpcConnection.mapPrevious(value)))
                    .toFuture());
            case RequestDelta<?> _ -> toResponse(reactiveStub().putMultiGetDelta(inputRequests)
                    .map(GrpcConnection::mapDelta)
                    .collectList()
                    .toFuture());
            case RequestChanged<?> _ -> toResponse(reactiveStub().putMultiGetChanged(inputRequests)
                    .map(Changed::getChanged)
                    .collectList()
                    .toFuture());
            case RequestPreviousPresence<?> _ -> toResponse(reactiveStub().putMultiGetPreviousPresence(inputRequests)
                    .map(PreviousPresence::getPresent)
                    .collectList()
                    .toFuture());
        });
    }

    @Override
    public <T> CompletableFuture<List<T>> deleteMultiAsync(long transactionOrUpdateId,
                                                           long columnId,
                                                           @NotNull List<Keys> keys,
                                                           RequestType.RequestDelete<? super Buf, T> requestType) throws RocksDBException {
        if (closed.get()) {
            return CompletableFuture.failedFuture(closedException());
        }
        return CompletableFuture.failedFuture(unsupportedGrpcOperation("deleteMulti"));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> CompletableFuture<List<T>> mergeMultiAsync(long transactionOrUpdateId,
                                                          long columnId,
                                                          @NotNull List<@NotNull Keys> allKeys,
                                                          @NotNull List<@NotNull Buf> allValues,
                                                          RequestMerge<? super Buf, T> requestType) throws RocksDBException {
        if (requestType == null) {
            throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Null request type");
        }
        var requiredKeys = requireGrpcField(allKeys, "gRPC client multi-key entries");
        var requiredValues = requireGrpcField(allValues, "gRPC client value entries");
        var count = requiredKeys.size();
        requireMatchingEntryCount("gRPC client multi-merge", count, requiredValues.size());

        if (requestType instanceof RequestType.RequestNothing<?> && transactionOrUpdateId == 0L && count > 1000) {
            return FutureUtils.mapCancellation(Flux.range(0, (count + 999) / 1000)
                    .concatMap(i -> {
                        int start = i * 1000;
                        int end = Math.min(count, start + 1000);
                        return Mono.fromFuture(() -> mergeMultiAsync(
                                transactionOrUpdateId,
                                columnId,
                                requiredKeys.subList(start, end),
                                requiredValues.subList(start, end),
                                requestType
                        ));
                    })
                    .then()
                    .toFuture(), _ -> List.of());
        }

        var initialRequest = MergeMultiRequest.newBuilder()
                .setInitialRequest(MergeMultiInitialRequest.newBuilder()
                        .setTransactionOrUpdateId(transactionOrUpdateId)
                        .setColumnId(columnId)
                        .build())
                .build();

        Mono<MergeMultiRequest> initialRequestMono = Mono.just(initialRequest);
        Flux<MergeMultiRequest> dataRequestsFlux = Flux.fromIterable(() -> GrpcConnection
                .map(requiredKeys.iterator(), requiredValues.iterator(), (keys, value) -> MergeMultiRequest.newBuilder()
                        .setData(mapKV(keys, value))
                        .build()));
        var inputRequests = initialRequestMono.concatWith(dataRequestsFlux);

        return (CompletableFuture<List<T>>) (switch (requestType) {
            case RequestNothing<?> _ -> toResponse(reactiveStub().mergeMulti(inputRequests)
                    .ignoreElement()
                    .toFuture());
            case RequestType.RequestMerged<?> _ -> toResponse(reactiveStub().mergeMultiGetMerged(inputRequests)
                    .<List<@Nullable Buf>>collect(ArrayList::new,
                            (list, value) -> list.add(mapMerged(value)))
                    .toFuture());
        });
    }

    @Override
    public Publisher<SerializedKVBatch> scanRawAsync(long columnId, int shardIndex, int shardCount) {
        return reactiveStub().scanRaw(ScanRawRequest.newBuilder()
                        .setColumnId(columnId)
                        .setShardIndex(shardIndex)
                        .setShardCount(shardCount)
                        .build())
                .map(batch -> new SerializedKVBatchRef(Buf.wrap(batch.getSerialized().toByteArray())));
    }

    @Override
    public CompletableFuture<Void> putBatchAsync(long columnId,
                                                 @NotNull Publisher<@NotNull KVBatch> batchPublisher,
                                                 @NotNull PutBatchMode mode) throws RocksDBException {
        var initialRequest = Mono.just(PutBatchRequest.newBuilder()
                .setInitialRequest(PutBatchInitialRequest.newBuilder()
                        .setColumnId(columnId)
                        .setMode(mapPutBatchMode(mode))
                        .build())
                .build());
        var nextRequests = Flux.from(requireGrpcField(batchPublisher, "gRPC client batch publisher")).map(batch -> {
            var request = PutBatchRequest.newBuilder();
            request.setData(mapKVBatch(batch));
            return request.build();
        });
        var inputFlux = initialRequest.concatWith(nextRequests);
        return toResponse(reactiveStub().putBatch(inputFlux).then().toFuture());
    }

    @Override
    public CompletableFuture<Void> mergeBatchAsync(long columnId,
                                                   @NotNull Publisher<@NotNull KVBatch> batchPublisher,
                                                   @NotNull it.cavallium.rockserver.core.common.MergeBatchMode mode) throws RocksDBException {
        var initialRequest = Mono.just(MergeBatchRequest.newBuilder()
                .setInitialRequest(MergeBatchInitialRequest.newBuilder()
                        .setColumnId(columnId)
                        .setMode(mapMergeBatchMode(mode))
                        .build())
                .build());
        var nextRequests = Flux.from(requireGrpcField(batchPublisher, "gRPC client batch publisher")).map(batch -> {
            var request = MergeBatchRequest.newBuilder();
            request.setData(mapKVBatch(batch));
            return request.build();
        });
        var inputFlux = initialRequest.concatWith(nextRequests);
        return toResponse(reactiveStub().mergeBatch(inputFlux).then().toFuture());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> CompletableFuture<T> getAsync(long transactionOrUpdateId,
                                             long columnId,
                                             @NotNull Keys keys,
                                             RequestGet<? super Buf, T> requestType) throws RocksDBException {
        if (requestType == null) {
            throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "requestType");
        }
        var request = GetRequest.newBuilder()
                .setTransactionOrUpdateId(transactionOrUpdateId)
                .setColumnId(columnId)
                .addAllKeys(requiredMapKeys("gRPC client get keys", keys))
                .build();
        if (requestType instanceof RequestType.RequestForUpdate<?>) {
            return toResponse(futureStub().getForUpdate(request), x -> (T) new UpdateContext<>(
                    x.hasPrevious() ? mapByteString(x.getPrevious()) : null,
                    x.getUpdateId()
            ));
        } else if (requestType instanceof RequestType.RequestExists<?>) {
            return toResponse(futureStub().exists(request), x -> (T) (Boolean) x.getPresent());
        } else {
            return toResponse(futureStub().get(request), x -> switch (requestType) {
                case RequestNothing<?> _ -> null;
                case RequestType.RequestCurrent<?> _ -> x.hasValue() ? (T) mapByteString(x.getValue()) : null;
                case RequestType.RequestForUpdate<?> _ -> throw new IllegalStateException(
                        "RequestForUpdate must be handled by getForUpdate before mapping a standard get response"
                );
            });
        }
    }

    @Override
    public CompletableFuture<Long> openIteratorAsync(long transactionId,
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
        return toResponse(futureStub().openIterator(request), OpenIteratorResponse::getIteratorId);
    }

    @Override
    public CompletableFuture<Void> closeIteratorAsync(long iteratorId) throws RocksDBException {
        var request = CloseIteratorRequest.newBuilder()
                .setIteratorId(iteratorId)
                .build();
        return toResponse(futureStub().closeIterator(request), _ -> null);
    }

    @Override
    public CompletableFuture<Void> seekToAsync(long iterationId, @NotNull Keys keys) throws RocksDBException {
        var request = SeekToRequest.newBuilder()
                .setIterationId(iterationId)
                .addAllKeys(requiredMapKeys("gRPC client iterator seek keys", keys))
                .build();
        return toResponse(futureStub().seekTo(request), _ -> null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> CompletableFuture<T> subsequentAsync(long iterationId,
                                                    long skipCount,
                                                    long takeCount,
                                                    @NotNull RequestType.RequestIterate<? super Buf, T> requestType) throws RocksDBException {
        var request = SubsequentRequest.newBuilder()
                .setIterationId(iterationId)
                .setSkipCount(skipCount)
                .setTakeCount(takeCount)
                .build();
        return switch (requestType) {
            case RequestNothing<?> _ -> toResponse(futureStub().subsequent(request), _ -> null);
            case RequestExists<?> _ ->
                    (CompletableFuture<T>) toResponse(futureStub().subsequentExists(request), PreviousPresence::getPresent);
            case RequestMulti<?> _ -> (CompletableFuture<T>) toResponse(reactiveStub().subsequentMultiGet(request)
                    .map(kv -> mapByteString(kv.getValue()))
                    .collectList()
                    .toFuture());
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> CompletableFuture<T> reduceRangeAsync(long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.
            RequestReduceRange<? super it.cavallium.rockserver.core.common.KV, T> requestType, long timeoutMs) throws RocksDBException {
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
                    toResponse(futureStub().reduceRangeFirstAndLast(request), result -> new FirstAndLast<>(
                            result.hasFirst() ? mapKV(result.getFirst()) : null,
                            result.hasLast() ? mapKV(result.getLast()) : null
                    ));
            case RequestType.RequestEntriesCount<?> _ ->
                    toResponse(futureStub().reduceRangeEntriesCount(request), EntriesCount::getCount);
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Publisher<T> getRangeAsync(long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.
            RequestGetRange<? super it.cavallium.rockserver.core.common.KV, T> requestType, long timeoutMs) throws RocksDBException {
        var request = GetRangeRequest.newBuilder()
                .setTransactionId(transactionId)
                .setColumnId(columnId)
                .addAllStartKeysInclusive(mapKeys(startKeysInclusive))
                .addAllEndKeysExclusive(mapKeys(endKeysExclusive))
                .setReverse(reverse)
                .setTimeoutMs(timeoutMs)
                .build();
        return (Publisher<T>) switch (requestType) {
            case RequestType.RequestGetAllInRange<?> _ -> toResponse(reactiveStub().getAllInRange(request)
                    .map(GrpcConnection::mapKV));
        };
    }

    // ============ CDC Async API ============

    @Override
    public CompletableFuture<Long> cdcCreateAsync(@NotNull String id, @Nullable Long fromSeq, @Nullable List<Long> columnIds) throws RocksDBException {
        var builder = CdcCreateRequest.newBuilder().setId(id);
        if (fromSeq != null) builder.setFromSeq(fromSeq);
        if (columnIds != null) builder.addAllColumnIds(columnIds);
        return toResponse(futureStub().cdcCreate(builder.build()), CdcCreateResponse::getStartSeq);
    }

    @Override
    public CompletableFuture<Long> cdcCreateAsync(@NotNull String id, @Nullable Long fromSeq, @Nullable List<Long> columnIds, @Nullable Boolean resolvedValues) throws RocksDBException {
        var builder = CdcCreateRequest.newBuilder().setId(id);
        if (fromSeq != null) builder.setFromSeq(fromSeq);
        if (columnIds != null) builder.addAllColumnIds(columnIds);
        if (resolvedValues != null) builder.setResolvedValues(resolvedValues);
        return toResponse(futureStub().cdcCreate(builder.build()), CdcCreateResponse::getStartSeq);
    }

    @Override
    public CompletableFuture<Void> cdcDeleteAsync(@NotNull String id) throws RocksDBException {
        return toResponse(futureStub().cdcDelete(CdcDeleteRequest.newBuilder().setId(id).build()), _ -> null);
    }

    @Override
    public CompletableFuture<Void> cdcCommitAsync(@NotNull String id, long seq) throws RocksDBException {
        return toResponse(futureStub().cdcCommit(CdcCommitRequest.newBuilder().setId(id).setSeq(seq).build()), _ -> null);
    }

    @Override
    public Publisher<CDCEvent> cdcPollAsync(@NotNull String id, @Nullable Long fromSeq, long maxEvents) throws RocksDBException {
        var builder = CdcPollRequest.newBuilder().setId(id).setMaxEvents(maxEvents);
        if (fromSeq != null) builder.setFromSeq(fromSeq);
        return reactiveStub().cdcPoll(builder.build()).map(GrpcConnection::mapCDCEvent);
    }

    @Override
    public Mono<CdcBatch> cdcPollBatchAsync(@NotNull String id, @Nullable Long fromSeq, long maxEvents) {
        var builder = CdcPollRequest.newBuilder().setId(id).setMaxEvents(maxEvents);
        if (fromSeq != null) builder.setFromSeq(fromSeq);
        return Mono.fromFuture(toResponse(futureStub().cdcPollBatch(builder.build()), response -> new CdcBatch(
                response.getEventsList().stream().map(GrpcConnection::mapCDCEvent).toList(),
                response.getNextSeq()
        )));
    }

    private static CDCEvent mapCDCEvent(it.cavallium.rockserver.core.common.api.proto.CDCEvent ev) {
        var op = switch (ev.getOp()) {
            case PUT -> CDCEvent.Op.PUT;
            case DELETE -> CDCEvent.Op.DELETE;
            case MERGE -> CDCEvent.Op.MERGE;
            case UNRECOGNIZED -> throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
                    "gRPC CDC event op is unrecognized");
        };
        return new CDCEvent(ev.getSeq(), ev.getColumnId(), mapByteString(ev.getKey()), mapByteString(ev.getValue()), op);
    }

    @Nullable
    private static Buf mapMerged(Merged merged) {
        return merged.hasMerged() ? mapByteString(merged.getMerged()) : null;
    }

    @Override
    public CompletableFuture<Void> flushAsync() {
        var request = FlushRequest.newBuilder()
                .build();
        return toResponse(futureStub().flush(request), _ -> null);
    }

    @Override
    public CompletableFuture<Void> compactAsync() {
        var request = CompactRequest.newBuilder()
                .build();
        return toResponse(futureStub().compact(request), _ -> null);
    }

    @Override
    public CompletableFuture<Map<String, ColumnSchema>> getAllColumnDefinitionsAsync() {
        var request = GetAllColumnDefinitionsRequest.newBuilder()
                .build();
        return toResponse(futureStub().getAllColumnDefinitions(request),
                response -> response.getColumnsList().stream()
                        .collect(Collectors.toMap(Column::getName, col -> unmapColumnSchema(col.getSchema()))));
    }

    private static it.cavallium.rockserver.core.common.Delta<Buf> mapDelta(Delta x) {
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
    private static Buf mapPrevious(Previous x) {
        return x.hasPrevious() ? mapByteString(x.getPrevious()) : null;
    }

    private static Buf mapByteString(ByteString data) {
        return Utils.toBuf(data);
    }

    private static it.cavallium.rockserver.core.common.api.proto.KVBatch mapKVBatch(@NotNull KVBatch kvBatch) {
        var requiredBatch = requireGrpcField(kvBatch, "gRPC client batch");
        var list = mapKVList(requiredBatch.keys(), requiredBatch.values());
        return it.cavallium.rockserver.core.common.api.proto.KVBatch.newBuilder()
                .addAllEntries(list)
                .build();
    }

    private static Iterable<KV> mapKVList(@NotNull List<Keys> keys, @NotNull List<Buf> values) {
        var requiredKeys = requireGrpcField(keys, "gRPC client batch keys");
        var requiredValues = requireGrpcField(values, "gRPC client batch values");
        TransportRequestLimits.requireBatchEntries("gRPC client batch entries", requiredKeys.size());
        requireMatchingEntryCount("gRPC client batch", requiredKeys.size(), requiredValues.size());
        List<KV> result = new ArrayList<>(requiredKeys.size());
        var it1 = requiredKeys.iterator();
        var it2 = requiredValues.iterator();
        while (it1.hasNext()) {
            result.add(mapKV(it1.next(), it2.next()));
        }
        return result;
    }

    private static KV mapKV(@NotNull Keys keys, @NotNull Buf value) {
        return KV.newBuilder()
                .addAllKeys(requiredMapKeys("gRPC client KV keys", keys))
                .setValue(mapValue(value))
                .build();
    }

    private static it.cavallium.rockserver.core.common.KV mapKV(@NotNull KV entry) {
        var requiredEntry = requireGrpcField(entry, "gRPC peer KV");
        return new it.cavallium.rockserver.core.common.KV(
                mapKeys(requiredEntry.getKeysCount(), requiredEntry::getKeys),
                toBuf(requiredEntry.getValue())
        );
    }

    private static Keys mapKeys(int count, Int2ObjectFunction<ByteString> keyGetterAt) {
        var requiredKeyGetterAt = requireGrpcField(keyGetterAt, "gRPC peer key getter");
        TransportRequestLimits.requireKeyParts("gRPC peer key parts", count);
        var segments = new Buf[count];
        for (int i = 0; i < count; i++) {
            segments[i] = toBuf(requireGrpcField(requiredKeyGetterAt.apply(i),
                    "gRPC peer key part " + i));
        }
        return new Keys(segments);
    }

    private static Iterable<? extends ByteString> mapKeys(Keys keys) {
        if (keys == null) return List.of();
        return requiredMapKeys("gRPC client optional key boundary", keys);
    }

    private static Iterable<? extends ByteString> requiredMapKeys(String description, Keys keys) {
        var requiredKeys = requireGrpcField(keys, description);
        var keyParts = requireGrpcField(requiredKeys.keys(), description + " parts");
        TransportRequestLimits.requireKeyParts("gRPC client key parts", keyParts.length);
        return Iterables.transform(Arrays.asList(keyParts),
                k -> {
                    var requiredKeyPart = requireGrpcField(k, description + " part");
                    return UnsafeByteOperations.unsafeWrap(requiredKeyPart.getBackingByteArray(),
                            requiredKeyPart.getBackingByteArrayOffset(),
                            requiredKeyPart.getBackingByteArrayLength());
                });
    }

    private static ByteString mapValue(@NotNull Buf value) {
        var requiredValue = requireGrpcField(value, "gRPC client value");
        return UnsafeByteOperations.unsafeWrap(requiredValue.getBackingByteArray(),
                requiredValue.getBackingByteArrayOffset(),
                requiredValue.getBackingByteArrayLength());
    }

    private static it.cavallium.rockserver.core.common.api.proto.ColumnSchema mapColumnSchema(@NotNull ColumnSchema schema) {
        var requiredSchema = requireGrpcField(schema, "gRPC client column schema");
        var builder = it.cavallium.rockserver.core.common.api.proto.ColumnSchema.newBuilder()
                .addAllFixedKeys(mapFixedKeys(requiredSchema))
                .addAllVariableTailKeys(mapVariableTailKeys(requiredSchema))
                .setHasValue(requiredSchema.hasValue());
        GrpcColumnSchemaCompatibility.setMergeOperatorFields(builder, requiredSchema);
        return builder.build();
    }

    private static it.cavallium.rockserver.core.common.api.proto.PutBatchMode mapPutBatchMode(PutBatchMode mode) {
        return switch (requireGrpcField(mode, "gRPC client put batch mode")) {
            case WRITE_BATCH -> it.cavallium.rockserver.core.common.api.proto.PutBatchMode.WRITE_BATCH;
            case WRITE_BATCH_NO_WAL -> it.cavallium.rockserver.core.common.api.proto.PutBatchMode.WRITE_BATCH_NO_WAL;
            case SST_INGESTION -> it.cavallium.rockserver.core.common.api.proto.PutBatchMode.SST_INGESTION;
            case SST_INGEST_BEHIND -> it.cavallium.rockserver.core.common.api.proto.PutBatchMode.SST_INGEST_BEHIND;
        };
    }

    private static it.cavallium.rockserver.core.common.api.proto.MergeBatchMode mapMergeBatchMode(
            it.cavallium.rockserver.core.common.MergeBatchMode mode) {
        return switch (requireGrpcField(mode, "gRPC client merge batch mode")) {
            case MERGE_WRITE_BATCH -> it.cavallium.rockserver.core.common.api.proto.MergeBatchMode.MERGE_WRITE_BATCH;
            case MERGE_WRITE_BATCH_NO_WAL ->
                    it.cavallium.rockserver.core.common.api.proto.MergeBatchMode.MERGE_WRITE_BATCH_NO_WAL;
            case MERGE_SST_INGESTION ->
                    it.cavallium.rockserver.core.common.api.proto.MergeBatchMode.MERGE_SST_INGESTION;
            case MERGE_SST_INGEST_BEHIND ->
                    it.cavallium.rockserver.core.common.api.proto.MergeBatchMode.MERGE_SST_INGEST_BEHIND;
        };
    }

    private static void requireMatchingEntryCount(String context, int keyCount, int valueCount) {
        if (keyCount != valueCount) {
            throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
                    context + " keys count " + keyCount + " does not match values count " + valueCount);
        }
    }

    private static <T> T requireGrpcField(T value, String description) {
        if (value == null) {
            throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
                    description + " must not be null");
        }
        return value;
    }

    private static ColumnSchema unmapColumnSchema(it.cavallium.rockserver.core.common.api.proto.ColumnSchema schema) {
        return ColumnSchema.of(unmapKeysLength(schema.getFixedKeysCount(), schema::getFixedKeys),
                unmapVariableTailKeys(schema.getVariableTailKeysCount(), schema::getVariableTailKeys),
                schema.getHasValue(),
                schema.hasMergeOperatorName() ? schema.getMergeOperatorName() : null,
                schema.hasMergeOperatorVersion() ? schema.getMergeOperatorVersion() : null,
                GrpcColumnSchemaCompatibility.getMergeOperatorClass(schema)
        );
    }

    private static IntList unmapKeysLength(int count, Int2IntFunction keyGetterAt) {
        var l = new IntArrayList(count);
        for (int i = 0; i < count; i++) {
            l.add((int) keyGetterAt.apply(i));
        }
        return l;
    }

    private static ObjectList<it.cavallium.rockserver.core.common.ColumnHashType> unmapVariableTailKeys(int count,
                                                                                                        Int2ObjectFunction<it.cavallium.rockserver.core.common.api.proto.ColumnHashType> variableTailKeyGetterAt) {
        var l = new ObjectArrayList<it.cavallium.rockserver.core.common.ColumnHashType>(count);
        for (int i = 0; i < count; i++) {
            l.add(switch (variableTailKeyGetterAt.apply(i)) {
                case XXHASH32 -> it.cavallium.rockserver.core.common.ColumnHashType.XXHASH32;
                case XXHASH8 -> it.cavallium.rockserver.core.common.ColumnHashType.XXHASH8;
                case ALLSAME8 -> it.cavallium.rockserver.core.common.ColumnHashType.ALLSAME8;
                case FIXEDINTEGER32 -> it.cavallium.rockserver.core.common.ColumnHashType.FIXEDINTEGER32;
                case UNRECOGNIZED -> throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
                        "gRPC peer schema variable tail key hash type is unrecognized");
            });
        }
        return l;
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
                case FIXEDINTEGER32 -> ColumnHashType.FIXEDINTEGER32;
            });
        }
        return result;
    }

    private static <T> CompletableFuture<T> toResponse(CompletableFuture<T> future) {
        return FutureUtils.propagateCancellation(
                future.exceptionallyCompose(ex -> CompletableFuture.failedFuture(mapGrpcStatusError(ex))),
                future
        );
    }

    private static <T> T awaitSyncResponse(CompletableFuture<T> response) {
        return FutureUtils.awaitInterruptibly(response, "gRPC synchronous request");
    }

    private static RocksDBException unsupportedGrpcOperation(String operation) {
        return RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED, operation + " is not exposed by the gRPC transport");
    }

    private static <T> Flux<T> toResponse(Flux<T> flux) {
        return flux.onErrorMap(GrpcConnection::mapGrpcStatusError);
    }

    private static <T, U> CompletableFuture<U> toResponse(ListenableFuture<T> listenableFuture, Function<T, U> mapper) {
        var response = new CompletableFuture<U>();

        Futures.addCallback(listenableFuture, new FutureCallback<>() {
            @Override
            public void onSuccess(T result) {
                try {
                    response.complete(mapper.apply(result));
                } catch (Throwable failure) {
                    completeCallbackFailure(response, failure);
                }
            }

            @Override
            public void onFailure(@NotNull Throwable t) {
                try {
                    response.completeExceptionally(mapGrpcStatusError(t));
                } catch (Throwable failure) {
                    completeCallbackFailure(response, failure);
                }
            }
        }, DIRECT_EXECUTOR);

        return FutureUtils.propagateCancellation(response, listenableFuture);
    }

    private static void completeCallbackFailure(CompletableFuture<?> response, Throwable failure) {
        var cause = unwrapCompletionException(failure);
        if (cause instanceof Error error) {
            response.completeExceptionally(error);
            throw error;
        }
        response.completeExceptionally(mapGrpcResponseMappingFailure(cause));
    }

    private static Throwable mapGrpcResponseMappingFailure(Throwable failure) {
        if (failure instanceof RocksDBException) {
            return failure;
        }
        return RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "Failed to map gRPC response", failure);
    }

    private static Throwable unwrapCompletionException(Throwable failure) {
        if (failure instanceof CompletionException completionException && completionException.getCause() != null) {
            return unwrapCompletionException(completionException.getCause());
        }
        return failure;
    }

    private static final java.util.Set<Code> RETRYABLE_STATUS_CODES = java.util.Set.of(
            Code.UNAVAILABLE, Code.RESOURCE_EXHAUSTED, Code.ABORTED
    );

    private static class RetryLoggingInterceptor implements ClientInterceptor {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                @Override
                public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {
                    var loggingListener = new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
                        @Override
                        public void onClose(Status status, Metadata trailers) {
                            if (!status.isOk() && RETRYABLE_STATUS_CODES.contains(status.getCode())) {
                                LOG.warn("gRPC call to {} failed with retryable status {}: {}. Retry will be attempted automatically.",
                                        method.getFullMethodName(), status.getCode(), status.getDescription());
                            }
                            super.onClose(status, trailers);
                        }
                    };
                    super.start(loggingListener, headers);
                }
            };
        }
    }

    private static final String grpcRocksDbErrorPrefixString = "RocksDBError: [uid:";

    private static Throwable mapGrpcStatusError(@NotNull Throwable t) {
        var cause = unwrapCompletionException(t);
        if (cause instanceof StatusRuntimeException statusRuntimeException
                && statusRuntimeException.getStatus().getCode() == Code.INTERNAL
                && statusRuntimeException.getStatus().getDescription() != null
                && statusRuntimeException.getStatus().getDescription().startsWith(grpcRocksDbErrorPrefixString)) {
            var desc = statusRuntimeException.getStatus().getDescription();
            var closeIndex = desc.indexOf(']');
            var errorCode = desc.substring(grpcRocksDbErrorPrefixString.length(), closeIndex);
            var errorDescription = desc.substring(closeIndex + 2);
            var errorType = RocksDBErrorType.valueOf(errorCode);
            if (errorType == RocksDBErrorType.UPDATE_RETRY) {
                return new RocksDBRetryException();
            } else {
                return RocksDBException.of(errorType, errorDescription);
            }
        } else {
            return cause;
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        Throwable failure = closeEventLoopGroup(closeChannel(null));
        if (failure != null) {
            throwCloseFailure(failure);
        }
    }

    private RocksDBServiceFutureStub futureStub() {
        requireOpen();
        return futureStub;
    }

    private ReactorRocksDBServiceGrpc.ReactorRocksDBServiceStub reactiveStub() {
        requireOpen();
        return reactiveStub;
    }

    private void requireOpen() {
        if (closed.get()) {
            throw closedException();
        }
    }

    private static IllegalStateException closedException() {
        return new IllegalStateException("gRPC connection is closed");
    }

    private Throwable closeChannel(Throwable failure) {
        var interruption = new CloseInterruption();
        try {
            if (this.channel != null) {
                this.channel.shutdown();
                if (channelTerminationTimedOut(interruption)) {
                    this.channel.shutdownNow();
                    if (channelTerminationTimedOut(interruption)) {
                        failure = addCloseFailure(failure,
                                new IllegalStateException("gRPC channel did not terminate after forced shutdown"));
                    }
                }
            }
        } catch (RuntimeException | Error e) {
            failure = addCloseFailure(failure, e);
        } finally {
            interruption.restore();
        }
        return failure;
    }

    private boolean channelTerminationTimedOut(CloseInterruption interruption) {
        var deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(CHANNEL_SHUTDOWN_TIMEOUT_SECONDS);
        while (true) {
            try {
                var remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    return true;
                }
                return !this.channel.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
            } catch (InterruptedException _) {
                interruption.record();
            }
        }
    }

    private Throwable closeEventLoopGroup(Throwable failure) {
        if (this.eventLoopGroup == null) {
            return failure;
        }
        try {
            var termination = this.eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
            if (!termination.awaitUninterruptibly(10, TimeUnit.SECONDS)) {
                return addCloseFailure(failure,
                        new IllegalStateException("gRPC event loop group did not terminate after shutdown"));
            }
            if (!termination.isSuccess()) {
                var cause = termination.cause();
                return addCloseFailure(failure, cause != null
                        ? cause
                        : new IllegalStateException("gRPC event loop group shutdown failed"));
            }
        } catch (RuntimeException | Error e) {
            return addCloseFailure(failure, e);
        }
        return failure;
    }

    private static Throwable addCloseFailure(Throwable failure, Throwable addition) {
        if (failure == null) {
            return addition;
        }
        if (failure != addition) {
            failure.addSuppressed(addition);
        }
        return failure;
    }

    private static void throwCloseFailure(Throwable failure) {
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IllegalStateException("Unexpected gRPC client close failure", failure);
    }

    private static final class CloseInterruption {

        private boolean interrupted;

        void record() {
            interrupted = true;
        }

        void restore() {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
