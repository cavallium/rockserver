package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static it.cavallium.rockserver.core.common.FutureUtils.mapCancellation;

public final class RatisProcessAdapterClientFactory implements RatisAdapterFactory {

    private final String implementationName;
    private final RatisProcessAdapterRpcTransport transport;
    private final AtomicLong requestSequence = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();

    public RatisProcessAdapterClientFactory(String implementationName,
                                            RatisProcessAdapterRpcTransport transport) {
        this.implementationName = RatisAdapterBoundary.requireSingleLineNonBlank(
                implementationName,
                "implementationName"
        );
        this.transport = requireNonNull(transport, "transport");
    }

    @Override
    public RatisAdapterBoundary boundary() {
        return RatisAdapterBoundary.externalProcess(implementationName);
    }

    @Override
    public RatisPartitionClient openPartition(RatisPartitionDescriptor descriptor) {
        requireOpen();
        return new ProcessRatisPartitionClient(requireNonNull(descriptor, "descriptor"));
    }

    @Override
    public CompletableFuture<Void> reloadMetadata() {
        requireOpen();
        return mapCancellation(
                send(RatisProcessAdapterRpcRequest.reloadMetadata(nextRequestId(
                        RatisProcessAdapterRpcRequest.ADAPTER_CONTROL_PARTITION_ID
                ))),
                _ -> null
        );
    }

    @Override
    public void close() {
        closed.set(true);
        transport.close();
    }

    private String nextRequestId(String partitionId) {
        return partitionId + "-" + requestSequence.incrementAndGet();
    }

    private CompletableFuture<RatisProcessAdapterRpcResponse> send(RatisProcessAdapterRpcRequest request) {
        requireOpen();
        try {
            var responseFuture = transport.send(request);
            if (responseFuture == null) {
                return CompletableFuture.failedFuture(adapterContractFailure(
                        "Ratis adapter RPC transport returned null response future"
                ));
            }
            return mapCancellation(
                    responseFuture,
                    response -> requireMatchingResponse(request, response)
            );
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Ratis process adapter client factory is closed");
        }
    }

    private static RatisProcessAdapterRpcResponse requireMatchingResponse(RatisProcessAdapterRpcRequest request,
                                                                          RatisProcessAdapterRpcResponse response) {
        if (response == null) {
            throw adapterContractFailure("Ratis adapter RPC transport returned null response");
        }
        if (!request.requestId().equals(response.requestId())) {
            throw adapterContractFailure("Ratis adapter RPC response id " + response.requestId()
                    + " does not match request id " + request.requestId());
        }
        if (!response.success()) {
            throw new RatisProcessAdapterRpcException(response.errorType(), response.message());
        }
        requireSuccessfulResponseShape(request, response);
        return response;
    }

    private static RatisProcessAdapterRpcException adapterContractFailure(String message) {
        return new RatisProcessAdapterRpcException("ADAPTER_ERROR", message);
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static void requireSuccessfulResponseShape(RatisProcessAdapterRpcRequest request,
                                                       RatisProcessAdapterRpcResponse response) {
        switch (request.operation()) {
            case PROPOSE -> {
            }
            case WAIT_FOR_COMMIT -> {
                if (response.payload().length != 0) {
                    throw adapterContractFailure(request.operation() + " response must not include a payload");
                }
            }
            case QUERY_LINEARIZABLE, QUERY_STALE, QUERY_LOCAL -> {
                if (response.commitIndex() != 0 || response.committed()) {
                    throw adapterContractFailure(request.operation() + " response must not include commit fields");
                }
            }
            case RELOAD_METADATA -> {
                if (response.payload().length != 0 || response.commitIndex() != 0 || response.committed()) {
                    throw adapterContractFailure("RELOAD_METADATA response must be empty");
                }
            }
        }
    }

    private final class ProcessRatisPartitionClient implements RatisPartitionClient {

        private final RatisPartitionDescriptor descriptor;
        private volatile boolean closed;

        private ProcessRatisPartitionClient(RatisPartitionDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        @Override
        public RatisPartitionDescriptor descriptor() {
            return descriptor;
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            requireOpen();
            var request = RatisProcessAdapterRpcRequest.propose(
                    nextRequestId(descriptor.partitionId()),
                    descriptor.partitionId(),
                    payload
            );
            return mapCancellation(
                    send(request),
                    response -> new ReplicatedLogCommit(
                            response.commitIndex(),
                            response.committed(),
                            response.payload().length == 0 ? null : response.payload()
                    )
            );
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
            requireOpen();
            var request = RatisProcessAdapterRpcRequest.waitForCommit(
                    nextRequestId(descriptor.partitionId()),
                    descriptor.partitionId(),
                    index
            );
            return mapCancellation(
                    send(request),
                    response -> new ReplicatedLogCommit(
                            response.commitIndex(),
                            response.committed()
                    )
            );
        }

        @Override
        public CompletableFuture<byte[]> queryLinearizable(byte[] requestPayload) {
            requireOpen();
            return query(RatisProcessAdapterRpcRequest.queryLinearizable(
                    nextRequestId(descriptor.partitionId()),
                    descriptor.partitionId(),
                    requestPayload
            ));
        }

        @Override
        public CompletableFuture<byte[]> queryStale(byte[] requestPayload, long minAppliedIndex) {
            requireOpen();
            return query(RatisProcessAdapterRpcRequest.queryStale(
                    nextRequestId(descriptor.partitionId()),
                    descriptor.partitionId(),
                    requestPayload,
                    minAppliedIndex
            ));
        }

        @Override
        public CompletableFuture<byte[]> queryLocal(byte[] requestPayload) {
            requireOpen();
            return query(RatisProcessAdapterRpcRequest.queryLocal(
                    nextRequestId(descriptor.partitionId()),
                    descriptor.partitionId(),
                    requestPayload
            ));
        }

        @Override
        public void close() {
            closed = true;
        }

        private CompletableFuture<byte[]> query(RatisProcessAdapterRpcRequest request) {
            return mapCancellation(send(request), RatisProcessAdapterRpcResponse::payload);
        }

        private void requireOpen() {
            RatisProcessAdapterClientFactory.this.requireOpen();
            if (closed) {
                throw new IllegalStateException("Ratis process adapter partition client is closed");
            }
        }
    }
}
