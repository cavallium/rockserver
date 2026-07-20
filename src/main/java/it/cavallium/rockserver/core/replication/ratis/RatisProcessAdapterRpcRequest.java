package it.cavallium.rockserver.core.replication.ratis;

import java.util.Arrays;
import java.util.Objects;

public record RatisProcessAdapterRpcRequest(int protocolVersion,
                                            String requestId,
                                            String partitionId,
                                            RatisProcessAdapterRpcOperation operation,
                                            byte[] payload,
                                            long index,
                                            long minAppliedIndex) {

    public static final int CURRENT_PROTOCOL_VERSION = 1;
    public static final String ADAPTER_CONTROL_PARTITION_ID = "__adapter__";

    public RatisProcessAdapterRpcRequest {
        if (protocolVersion != CURRENT_PROTOCOL_VERSION) {
            throw new IllegalArgumentException("Unsupported Ratis adapter RPC protocol version: " + protocolVersion);
        }
        requestId = RatisProcessAdapterRpcLimits.requireRequestId(requestId);
        partitionId = RatisProcessAdapterRpcLimits.requirePartitionId(partitionId);
        if (operation == null) {
            throw new IllegalArgumentException("operation must not be null");
        }
        payload = RatisProcessAdapterRpcLimits.copyPayload(payload);
        if (index < 0) {
            throw new IllegalArgumentException("index must be >= 0");
        }
        if (minAppliedIndex < 0) {
            throw new IllegalArgumentException("minAppliedIndex must be >= 0");
        }
        validateOperationFields(operation, partitionId, payload, index, minAppliedIndex);
    }

    public static RatisProcessAdapterRpcRequest propose(String requestId, String partitionId, byte[] payload) {
        return new RatisProcessAdapterRpcRequest(
                CURRENT_PROTOCOL_VERSION,
                requestId,
                partitionId,
                RatisProcessAdapterRpcOperation.PROPOSE,
                payload,
                0,
                0
        );
    }

    public static RatisProcessAdapterRpcRequest waitForCommit(String requestId, String partitionId, long index) {
        return new RatisProcessAdapterRpcRequest(
                CURRENT_PROTOCOL_VERSION,
                requestId,
                partitionId,
                RatisProcessAdapterRpcOperation.WAIT_FOR_COMMIT,
                new byte[0],
                index,
                0
        );
    }

    public static RatisProcessAdapterRpcRequest queryLinearizable(String requestId, String partitionId, byte[] payload) {
        return new RatisProcessAdapterRpcRequest(
                CURRENT_PROTOCOL_VERSION,
                requestId,
                partitionId,
                RatisProcessAdapterRpcOperation.QUERY_LINEARIZABLE,
                payload,
                0,
                0
        );
    }

    public static RatisProcessAdapterRpcRequest queryStale(String requestId,
                                                           String partitionId,
                                                           byte[] payload,
                                                           long minAppliedIndex) {
        return new RatisProcessAdapterRpcRequest(
                CURRENT_PROTOCOL_VERSION,
                requestId,
                partitionId,
                RatisProcessAdapterRpcOperation.QUERY_STALE,
                payload,
                0,
                minAppliedIndex
        );
    }

    public static RatisProcessAdapterRpcRequest queryLocal(String requestId, String partitionId, byte[] payload) {
        return new RatisProcessAdapterRpcRequest(
                CURRENT_PROTOCOL_VERSION,
                requestId,
                partitionId,
                RatisProcessAdapterRpcOperation.QUERY_LOCAL,
                payload,
                0,
                0
        );
    }

    public static RatisProcessAdapterRpcRequest reloadMetadata(String requestId) {
        return new RatisProcessAdapterRpcRequest(
                CURRENT_PROTOCOL_VERSION,
                requestId,
                ADAPTER_CONTROL_PARTITION_ID,
                RatisProcessAdapterRpcOperation.RELOAD_METADATA,
                new byte[0],
                0,
                0
        );
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RatisProcessAdapterRpcRequest that)) {
            return false;
        }
        return protocolVersion == that.protocolVersion
                && index == that.index
                && minAppliedIndex == that.minAppliedIndex
                && requestId.equals(that.requestId)
                && partitionId.equals(that.partitionId)
                && operation == that.operation
                && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        var result = Objects.hash(protocolVersion, requestId, partitionId, operation, index, minAppliedIndex);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    private static void validateOperationFields(RatisProcessAdapterRpcOperation operation,
                                                String partitionId,
                                                byte[] payload,
                                                long index,
                                                long minAppliedIndex) {
        switch (operation) {
            case PROPOSE, QUERY_LINEARIZABLE, QUERY_LOCAL -> {
                requireZero(index, "index", operation);
                requireZero(minAppliedIndex, "minAppliedIndex", operation);
            }
            case QUERY_STALE -> requireZero(index, "index", operation);
            case WAIT_FOR_COMMIT -> {
                requireEmptyPayload(payload, operation);
                requireZero(minAppliedIndex, "minAppliedIndex", operation);
            }
            case RELOAD_METADATA -> {
                if (!ADAPTER_CONTROL_PARTITION_ID.equals(partitionId)) {
                    throw new IllegalArgumentException("RELOAD_METADATA must target " + ADAPTER_CONTROL_PARTITION_ID);
                }
                requireEmptyPayload(payload, operation);
                requireZero(index, "index", operation);
                requireZero(minAppliedIndex, "minAppliedIndex", operation);
            }
        }
    }

    private static void requireEmptyPayload(byte[] payload, RatisProcessAdapterRpcOperation operation) {
        if (payload.length != 0) {
            throw new IllegalArgumentException(operation + " payload must be empty");
        }
    }

    private static void requireZero(long value, String name, RatisProcessAdapterRpcOperation operation) {
        if (value != 0) {
            throw new IllegalArgumentException(operation + " " + name + " must be 0");
        }
    }
}
