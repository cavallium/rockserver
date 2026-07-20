package it.cavallium.rockserver.core.cluster.client;

public final class PartitionReadRouteException extends RuntimeException {

    private final PartitionReadFailureKind kind;

    public PartitionReadRouteException(PartitionReadFailureKind kind, String message) {
        this(kind, message, null);
    }

    public PartitionReadRouteException(PartitionReadFailureKind kind,
                                       String message,
                                       Throwable cause) {
        super(message, cause);
        this.kind = requireArgument(kind, "kind");
    }

    public PartitionReadFailureKind kind() {
        return kind;
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
