package it.cavallium.rockserver.core.cluster.rebalance;

public enum AdminRebalanceAction {
    SPLIT_PARTITION,
    MERGE_PARTITIONS,
    TRANSFER_LEADER,
    MOVE_REPLICA
}
