package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.rockserver.core.cluster.ShardId;

public record ReplicatedCdcToken(long metadataEpoch,
                                 ShardId partitionId,
                                 long partitionEpoch,
                                 long appliedIndex,
                                 int eventOrdinal) {

    public ReplicatedCdcToken {
        requireNonNegative(metadataEpoch, "metadataEpoch");
        if (partitionId == null) {
            throw new IllegalArgumentException("partitionId must not be null");
        }
        requireNonNegative(partitionEpoch, "partitionEpoch");
        requireNonNegative(appliedIndex, "appliedIndex");
        if (eventOrdinal < 0) {
            throw new IllegalArgumentException("eventOrdinal must be >= 0");
        }
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }
}
