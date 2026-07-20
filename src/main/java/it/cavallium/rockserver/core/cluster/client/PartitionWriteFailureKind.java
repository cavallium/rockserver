package it.cavallium.rockserver.core.cluster.client;

public enum PartitionWriteFailureKind {
    INVALID_REQUEST,
    NOT_LEADER,
    STALE_ROUTE,
    ROUTE_FAILED,
    TIMEOUT,
    DEADLINE_EXPIRED,
    BACKPRESSURE,
    RETRY_EXHAUSTED
}
