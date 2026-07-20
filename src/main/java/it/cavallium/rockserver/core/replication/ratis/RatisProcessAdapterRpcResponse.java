package it.cavallium.rockserver.core.replication.ratis;

import java.util.Arrays;
import java.util.Objects;

public record RatisProcessAdapterRpcResponse(int protocolVersion,
                                             String requestId,
                                             boolean success,
                                             byte[] payload,
                                             long commitIndex,
                                             boolean committed,
                                             String errorType,
                                             String message) {

    public static final int CURRENT_PROTOCOL_VERSION = 1;

    public RatisProcessAdapterRpcResponse {
        if (protocolVersion != CURRENT_PROTOCOL_VERSION) {
            throw new IllegalArgumentException("Unsupported Ratis adapter RPC protocol version: " + protocolVersion);
        }
        requestId = RatisProcessAdapterRpcLimits.requireRequestId(requestId);
        payload = RatisProcessAdapterRpcLimits.copyPayload(payload);
        if (commitIndex < 0) {
            throw new IllegalArgumentException("commitIndex must be >= 0");
        }
        errorType = RatisProcessAdapterRpcLimits.requireErrorType(errorType);
        message = RatisProcessAdapterRpcLimits.requireErrorMessage(message);
        validateErrorFields(success, payload, commitIndex, committed, errorType, message);
    }

    public static RatisProcessAdapterRpcResponse commit(String requestId, long commitIndex, boolean committed) {
        return commit(requestId, commitIndex, committed, new byte[0]);
    }

    public static RatisProcessAdapterRpcResponse commit(String requestId,
                                                       long commitIndex,
                                                       boolean committed,
                                                       byte[] payload) {
        return new RatisProcessAdapterRpcResponse(
                CURRENT_PROTOCOL_VERSION,
                requestId,
                true,
                payload,
                commitIndex,
                committed,
                "",
                ""
        );
    }

    public static RatisProcessAdapterRpcResponse payload(String requestId, byte[] payload) {
        return new RatisProcessAdapterRpcResponse(
                CURRENT_PROTOCOL_VERSION,
                requestId,
                true,
                payload,
                0,
                false,
                "",
                ""
        );
    }

    public static RatisProcessAdapterRpcResponse error(String requestId, String errorType, String message) {
        return new RatisProcessAdapterRpcResponse(
                CURRENT_PROTOCOL_VERSION,
                requestId,
                false,
                new byte[0],
                0,
                false,
                errorType,
                message
        );
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RatisProcessAdapterRpcResponse that)) {
            return false;
        }
        return protocolVersion == that.protocolVersion
                && success == that.success
                && commitIndex == that.commitIndex
                && committed == that.committed
                && requestId.equals(that.requestId)
                && Arrays.equals(payload, that.payload)
                && errorType.equals(that.errorType)
                && message.equals(that.message);
    }

    @Override
    public int hashCode() {
        var result = Objects.hash(protocolVersion, requestId, success, commitIndex, committed, errorType, message);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    private static void validateErrorFields(boolean success,
                                            byte[] payload,
                                            long commitIndex,
                                            boolean committed,
                                            String errorType,
                                            String message) {
        if (success) {
            if (!errorType.isEmpty() || !message.isEmpty()) {
                throw new IllegalArgumentException("successful response must not include error fields");
            }
            if (committed && commitIndex == 0) {
                throw new IllegalArgumentException("committed response must include a positive commitIndex");
            }
            return;
        }
        if (errorType.isBlank()) {
            throw new IllegalArgumentException("error response must include errorType");
        }
        if (message.isBlank()) {
            throw new IllegalArgumentException("error response must include message");
        }
        if (payload.length != 0 || commitIndex != 0 || committed) {
            throw new IllegalArgumentException("error response must not include success payload or commit fields");
        }
    }
}
