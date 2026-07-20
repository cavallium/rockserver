package it.cavallium.rockserver.core.cluster.client;

public enum PartitionReadFailureKind {
    STALE_ROUTE,
    TIMEOUT,
    QUERY_FAILED,
    DEADLINE_EXPIRED,
    RESOURCE_LIMIT_EXCEEDED
}
