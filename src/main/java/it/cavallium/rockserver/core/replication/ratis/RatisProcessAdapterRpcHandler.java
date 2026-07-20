package it.cavallium.rockserver.core.replication.ratis;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import static it.cavallium.rockserver.core.common.FutureUtils.propagateCancellation;

public final class RatisProcessAdapterRpcHandler {

    private final PartitionClientResolver partitionClientResolver;
    private final MetadataReloader metadataReloader;

    public RatisProcessAdapterRpcHandler(PartitionClientResolver partitionClientResolver) {
        this(partitionClientResolver, () -> CompletableFuture.failedFuture(
                new RatisFeatureUnavailableException("metadata reload is not configured")
        ));
    }

    public RatisProcessAdapterRpcHandler(PartitionClientResolver partitionClientResolver,
                                         MetadataReloader metadataReloader) {
        this.partitionClientResolver = requireNonNull(partitionClientResolver, "partitionClientResolver");
        this.metadataReloader = requireNonNull(metadataReloader, "metadataReloader");
    }

    public CompletableFuture<RatisProcessAdapterRpcResponse> handle(RatisProcessAdapterRpcRequest request) {
        var checkedRequest = requireNonNull(request, "request");
        try {
            if (checkedRequest.operation() == RatisProcessAdapterRpcOperation.RELOAD_METADATA) {
                return operationResponse(
                        checkedRequest,
                        metadataReloader.reload(),
                        _ -> RatisProcessAdapterRpcResponse.payload(checkedRequest.requestId(), new byte[0])
                );
            }
            var client = requireAdapterNonNull(
                    partitionClientResolver.resolve(checkedRequest.partitionId()),
                    "resolved partition client"
            );
            return dispatch(checkedRequest, client);
        } catch (RuntimeException e) {
            return CompletableFuture.completedFuture(operationFailure(checkedRequest, e));
        }
    }

    private CompletableFuture<RatisProcessAdapterRpcResponse> dispatch(RatisProcessAdapterRpcRequest request,
                                                                       RatisPartitionClient client) {
        return switch (request.operation()) {
            case PROPOSE -> operationResponse(
                    request,
                    client.propose(request.payload()),
                    commit -> {
                        var resolvedCommit = requireAdapterNonNull(commit, "commit");
                        return RatisProcessAdapterRpcResponse.commit(
                                request.requestId(),
                                resolvedCommit.index(),
                                resolvedCommit.committed(),
                                resolvedCommit.hasResponsePayload() ? resolvedCommit.responsePayload() : new byte[0]
                        );
                    }
            );
            case WAIT_FOR_COMMIT -> operationResponse(
                    request,
                    client.waitForCommit(request.index()),
                    commit -> {
                        var resolvedCommit = requireAdapterNonNull(commit, "commit");
                        return RatisProcessAdapterRpcResponse.commit(
                                request.requestId(),
                                resolvedCommit.index(),
                                resolvedCommit.committed()
                        );
                    }
            );
            case QUERY_LINEARIZABLE -> operationResponse(
                    request,
                    client.queryLinearizable(request.payload()),
                    payload -> RatisProcessAdapterRpcResponse.payload(
                            request.requestId(),
                            requireAdapterNonNull(payload, "payload")
                    )
            );
            case QUERY_STALE -> operationResponse(
                    request,
                    client.queryStale(request.payload(), request.minAppliedIndex()),
                    payload -> RatisProcessAdapterRpcResponse.payload(
                            request.requestId(),
                            requireAdapterNonNull(payload, "payload")
                    )
            );
            case QUERY_LOCAL -> operationResponse(
                    request,
                    client.queryLocal(request.payload()),
                    payload -> RatisProcessAdapterRpcResponse.payload(
                            request.requestId(),
                            requireAdapterNonNull(payload, "payload")
                    )
            );
            case RELOAD_METADATA -> CompletableFuture.failedFuture(
                    new IllegalStateException("metadata reload must be handled before partition dispatch")
            );
        };
    }

    private static <T> CompletableFuture<RatisProcessAdapterRpcResponse> operationResponse(
            RatisProcessAdapterRpcRequest request,
            CompletableFuture<T> operation,
            Function<T, RatisProcessAdapterRpcResponse> mapper) {
        var checkedOperation = requireAdapterNonNull(operation, "operation");
        var checkedMapper = requireNonNull(mapper, "mapper");
        var response = checkedOperation
                .thenApply(value -> requireAdapterNonNull(checkedMapper.apply(value), "response"))
                .exceptionally(failure -> operationFailure(request, failure));
        return propagateCancellation(response, checkedOperation);
    }

    private static RatisProcessAdapterRpcResponse operationFailure(RatisProcessAdapterRpcRequest request,
                                                                   Throwable failure) {
        var unwrapped = unwrap(failure);
        if (unwrapped instanceof Error error) {
            throw error;
        }
        return error(request, unwrapped);
    }

    private static RatisProcessAdapterRpcResponse error(RatisProcessAdapterRpcRequest request, Throwable failure) {
        var unwrapped = unwrap(failure);
        return RatisProcessAdapterRpcResponse.error(
                request.requestId(),
                errorType(unwrapped),
                RatisProcessAdapterRpcLimits.sanitizeErrorMessage(unwrapped)
        );
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException completionException && completionException.getCause() != null) {
            return unwrap(completionException.getCause());
        }
        return failure;
    }

    private static String errorType(Throwable failure) {
        if (failure instanceof RatisProcessAdapterRpcException rpcException) {
            return rpcException.errorType();
        }
        if (failure instanceof IllegalArgumentException) {
            return "BAD_REQUEST";
        }
        if (failure instanceof RatisFeatureUnavailableException) {
            return "UNSUPPORTED";
        }
        return "ADAPTER_ERROR";
    }

    private static RatisProcessAdapterRpcException adapterContractFailure(String message) {
        return new RatisProcessAdapterRpcException("ADAPTER_ERROR", message);
    }

    @FunctionalInterface
    public interface PartitionClientResolver {

        RatisPartitionClient resolve(String partitionId);
    }

    @FunctionalInterface
    public interface MetadataReloader {

        CompletableFuture<Void> reload();
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static <T> T requireAdapterNonNull(T value, String name) {
        if (value == null) {
            throw adapterContractFailure(name + " must not be null");
        }
        return value;
    }
}
