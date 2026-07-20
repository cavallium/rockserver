package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.rockserver.core.cluster.ShardId;

public record RatisPartitionSnapshotMetadata(ShardId partitionId,
                                             long partitionEpoch,
                                             long ratisTerm,
                                             long ratisIndex,
                                             long metadataEpoch,
                                             long schemaEpoch,
                                             long dedupeWatermark,
                                             long cdcWatermark) {

    public RatisPartitionSnapshotMetadata {
        requirePartitionId(partitionId);
        requireNonNegative(partitionEpoch, "partitionEpoch");
        requireNonNegative(ratisTerm, "ratisTerm");
        requireNonNegative(ratisIndex, "ratisIndex");
        requireNonNegative(metadataEpoch, "metadataEpoch");
        requireNonNegative(schemaEpoch, "schemaEpoch");
        requireNonNegative(dedupeWatermark, "dedupeWatermark");
        requireNonNegative(cdcWatermark, "cdcWatermark");
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    private static void requirePartitionId(ShardId partitionId) {
        if (partitionId == null) {
            throw new IllegalArgumentException("partitionId must not be null");
        }
    }
}
