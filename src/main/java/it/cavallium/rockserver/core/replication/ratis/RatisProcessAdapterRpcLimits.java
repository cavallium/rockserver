package it.cavallium.rockserver.core.replication.ratis;

public final class RatisProcessAdapterRpcLimits {

    /**
     * Leaves more than 1 MiB for the line-oriented RPC envelope after canonical Base64 expansion inside the default
     * 64 MiB socket frame.
     */
    public static final int MAX_PAYLOAD_BYTES = 47 * 1024 * 1024;
    public static final int MAX_ENCODED_PAYLOAD_CHARACTERS = ((MAX_PAYLOAD_BYTES + 2) / 3) * 4;
    public static final int MAX_REQUEST_ID_CHARACTERS = 2048;
    public static final int MAX_PARTITION_ID_CHARACTERS = 1024;
    public static final int MAX_ERROR_TYPE_CHARACTERS = 128;
    public static final int MAX_ERROR_MESSAGE_CHARACTERS = 8192;

    private RatisProcessAdapterRpcLimits() {
    }

    public static int requirePayloadBytes(int payloadBytes) {
        if (payloadBytes < 0 || payloadBytes > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("Ratis adapter RPC payload bytes must be between 0 and "
                    + MAX_PAYLOAD_BYTES + ": " + payloadBytes);
        }
        return payloadBytes;
    }

    public static int requireEncodedPayloadCharacters(int encodedPayloadCharacters) {
        if (encodedPayloadCharacters < 0 || encodedPayloadCharacters > MAX_ENCODED_PAYLOAD_CHARACTERS) {
            throw new IllegalArgumentException("Encoded Ratis adapter RPC payload characters must be between 0 and "
                    + MAX_ENCODED_PAYLOAD_CHARACTERS + ": " + encodedPayloadCharacters);
        }
        return encodedPayloadCharacters;
    }

    static String requireRequestId(String requestId) {
        return requireSingleLineNonBlank(requestId, "requestId", MAX_REQUEST_ID_CHARACTERS);
    }

    static String requirePartitionId(String partitionId) {
        return requireSingleLineNonBlank(partitionId, "partitionId", MAX_PARTITION_ID_CHARACTERS);
    }

    static String requireErrorType(String errorType) {
        return requireSingleLine(errorType, "errorType", MAX_ERROR_TYPE_CHARACTERS);
    }

    static String requireErrorTypeNonBlank(String errorType) {
        return requireSingleLineNonBlank(errorType, "errorType", MAX_ERROR_TYPE_CHARACTERS);
    }

    static String requireErrorMessage(String message) {
        return requireSingleLine(message, "message", MAX_ERROR_MESSAGE_CHARACTERS);
    }

    static String sanitizeErrorMessage(Throwable failure) {
        var message = failure == null ? null : failure.getMessage();
        var fallback = failure == null ? "unknown failure" : failure.getClass().getSimpleName();
        return RatisProcessAdapterText.sanitizeSingleLine(message, fallback, MAX_ERROR_MESSAGE_CHARACTERS);
    }

    static byte[] copyPayload(byte[] payload) {
        return requirePayload(payload).clone();
    }

    static byte[] requirePayload(byte[] payload) {
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        requirePayloadBytes(payload.length);
        return payload;
    }

    static String requireEncodedPayload(String payload) {
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        requireEncodedPayloadCharacters(payload.length());
        return payload;
    }

    private static String requireSingleLineNonBlank(String value, String name, int maxCharacters) {
        var requiredValue = requireSingleLine(value, name, maxCharacters);
        if (requiredValue.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return requiredValue;
    }

    private static String requireSingleLine(String value, String name, int maxCharacters) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        if (value.length() > maxCharacters) {
            throw new IllegalArgumentException(name + " must not exceed " + maxCharacters + " characters");
        }
        if (RatisProcessAdapterText.containsUnsafeLineCharacter(value)) {
            throw new IllegalArgumentException(name + " must not contain NUL or line separators");
        }
        return value;
    }
}
