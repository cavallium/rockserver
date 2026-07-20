package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.rockserver.core.cluster.ShardId;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

public record BackupManifest(String backupId,
                             long metadataBackupEpoch,
                             long metadataEpoch,
                             List<PartitionCut> partitionCuts) {

    public BackupManifest {
        requireBackupId(backupId);
        requireNonNegative(metadataBackupEpoch, "metadataBackupEpoch");
        requireNonNegative(metadataEpoch, "metadataEpoch");
        if (metadataBackupEpoch < metadataEpoch) {
            throw new IllegalArgumentException("metadataBackupEpoch must be >= metadataEpoch");
        }
        var requiredPartitionCuts = requireArgument(partitionCuts, "partitionCuts");
        if (requiredPartitionCuts.isEmpty()) {
            throw new IllegalArgumentException("backup manifest must include at least one partition cut");
        }
        partitionCuts = requiredPartitionCuts.stream()
                .map(cut -> requireArgument(cut, "partitionCuts entry"))
                .sorted(Comparator.comparing(PartitionCut::partitionId))
                .toList();
        var partitionIds = new HashSet<ShardId>();
        for (PartitionCut cut : partitionCuts) {
            if (!partitionIds.add(cut.partitionId())) {
                throw new IllegalArgumentException("backup manifest contains duplicate partition cut: "
                        + cut.partitionId());
            }
            if (cut.observedMetadataBackupEpoch() != metadataBackupEpoch) {
                throw new IllegalArgumentException("partition backup cut must observe metadata backup epoch "
                        + metadataBackupEpoch);
            }
        }
    }

    public record PartitionCut(ShardId partitionId,
                               long partitionEpoch,
                               long ratisTerm,
                               long ratisIndex,
                               long observedMetadataBackupEpoch) {

        public PartitionCut {
            requireArgument(partitionId, "partitionId");
            requireNonNegative(partitionEpoch, "partitionEpoch");
            requireNonNegative(ratisTerm, "ratisTerm");
            requireNonNegative(ratisIndex, "ratisIndex");
            requireNonNegative(observedMetadataBackupEpoch, "observedMetadataBackupEpoch");
        }
    }

    private static void requireBackupId(String value) {
        var requiredValue = requireArgument(value, "backupId");
        if (requiredValue.isBlank()) {
            throw new IllegalArgumentException("backupId must not be blank");
        }
        if (requiredValue.indexOf('\r') >= 0 || requiredValue.indexOf('\n') >= 0) {
            throw new IllegalArgumentException("backupId must be a single line");
        }
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
