package it.cavallium.rockserver.core.replication.ratis;

public record RatisProcessAdapterHealth(int protocolVersion,
                                        RatisProcessAdapterHealthStatus status,
                                        String message) {

    public static final int CURRENT_PROTOCOL_VERSION = 1;
    public static final int MAX_MESSAGE_CHARACTERS = 256;

    public RatisProcessAdapterHealth {
        if (protocolVersion < 1) {
            throw new IllegalArgumentException("protocolVersion must be positive");
        }
        requireStatus(status);
        requireMessage(message);
    }

    public static RatisProcessAdapterHealth ready() {
        return new RatisProcessAdapterHealth(CURRENT_PROTOCOL_VERSION, RatisProcessAdapterHealthStatus.READY, "");
    }

    public static RatisProcessAdapterHealth degraded(String message) {
        return new RatisProcessAdapterHealth(
                CURRENT_PROTOCOL_VERSION,
                RatisProcessAdapterHealthStatus.DEGRADED,
                requireNonBlankMessage(message)
        );
    }

    public boolean isReady() {
        return protocolVersion == CURRENT_PROTOCOL_VERSION && status == RatisProcessAdapterHealthStatus.READY;
    }

    private static void requireStatus(RatisProcessAdapterHealthStatus status) {
        if (status == null) {
            throw new IllegalArgumentException("status must not be null");
        }
    }

    private static String requireNonBlankMessage(String message) {
        var requiredMessage = requireMessage(message);
        if (requiredMessage.isBlank()) {
            throw new IllegalArgumentException("message must not be blank");
        }
        return requiredMessage;
    }

    private static String requireMessage(String message) {
        if (message == null) {
            throw new IllegalArgumentException("message must not be null");
        }
        if (message.length() > MAX_MESSAGE_CHARACTERS) {
            throw new IllegalArgumentException("message must not exceed " + MAX_MESSAGE_CHARACTERS + " characters");
        }
        if (RatisProcessAdapterText.containsUnsafeLineCharacter(message)) {
            throw new IllegalArgumentException("message must not contain NUL or line separators");
        }
        return message;
    }
}
