package it.cavallium.rockserver.core.cluster.rebalance;

public enum HotPartitionReason {
    TOTAL_OPS,
    WRITE_OPS,
    BYTES,
    LATENCY,
    QUEUE_DEPTH
}
