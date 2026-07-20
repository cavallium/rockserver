package it.cavallium.rockserver.core.cluster.client;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;

import org.jetbrains.annotations.Nullable;

public final class PartitionWriteRouteException extends RuntimeException {

    private final PartitionWriteFailureKind kind;
    private final ClusterMetadata refreshedMetadata;

    public PartitionWriteRouteException(PartitionWriteFailureKind kind, String message) {
        this(kind, message, null, null);
    }

    public PartitionWriteRouteException(PartitionWriteFailureKind kind,
                                        String message,
                                        @Nullable Throwable cause) {
        this(kind, message, cause, null);
    }

    public PartitionWriteRouteException(PartitionWriteFailureKind kind,
                                        String message,
                                        @Nullable Throwable cause,
                                        @Nullable ClusterMetadata refreshedMetadata) {
        super(message, cause);
        this.kind = requireArgument(kind, "kind");
        this.refreshedMetadata = refreshedMetadata;
    }

    public static PartitionWriteRouteException notLeader(ClusterMetadata refreshedMetadata) {
        requireArgument(refreshedMetadata, "refreshedMetadata");
        return new PartitionWriteRouteException(
                PartitionWriteFailureKind.NOT_LEADER,
                "partition leader changed",
                null,
                refreshedMetadata
        );
    }

    public PartitionWriteFailureKind kind() {
        return kind;
    }

    public @Nullable ClusterMetadata refreshedMetadata() {
        return refreshedMetadata;
    }

    public boolean retryable() {
        return switch (kind) {
            case NOT_LEADER, STALE_ROUTE, TIMEOUT -> true;
            case INVALID_REQUEST, ROUTE_FAILED, DEADLINE_EXPIRED, BACKPRESSURE, RETRY_EXHAUSTED -> false;
        };
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
