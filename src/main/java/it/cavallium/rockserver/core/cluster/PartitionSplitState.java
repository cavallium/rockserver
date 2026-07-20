package it.cavallium.rockserver.core.cluster;

public enum PartitionSplitState {
    ACTIVE,
    SPLIT_PREPARING,
    SPLIT_SOURCE,
    SPLIT_TARGET,
    SPLIT_FINALIZING
}
