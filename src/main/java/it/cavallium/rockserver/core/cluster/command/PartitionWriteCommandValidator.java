package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.PartitionBoundaryRules;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;

import java.util.Arrays;

public final class PartitionWriteCommandValidator {

    private PartitionWriteCommandValidator() {
    }

    public static void validate(ClusterMetadata metadata,
                                PartitionDescriptor partition,
                                PartitionWriteCommand command) {
        var requiredMetadata = requireArgument(metadata, "metadata");
        var requiredPartition = requireArgument(partition, "partition");
        var requiredCommand = requireArgument(command, "command");
        if (requiredCommand.seal() == null) {
            validateLegacyUnsealed(requiredMetadata, requiredPartition, requiredCommand);
        } else {
            validateSealedForRouting(requiredMetadata, requiredPartition, requiredCommand);
        }
    }

    public static void validateForReplay(ClusterMetadata metadata,
                                         PartitionDescriptor partition,
                                         PartitionWriteCommand command) {
        var requiredMetadata = requireArgument(metadata, "metadata");
        var requiredPartition = requireArgument(partition, "partition");
        var requiredCommand = requireArgument(command, "command");
        if (requiredCommand.seal() == null) {
            validateLegacyUnsealed(requiredMetadata, requiredPartition, requiredCommand);
        } else {
            validateSealedForReplay(requiredPartition, requiredCommand);
        }
    }

    public static void validateForProposal(ClusterMetadata metadata,
                                           PartitionDescriptor partition,
                                           PartitionWriteCommand command) {
        var requiredMetadata = requireArgument(metadata, "metadata");
        var requiredPartition = requireArgument(partition, "partition");
        var requiredCommand = requireArgument(command, "command");
        if (requiredCommand.seal() == null) {
            throw new IllegalArgumentException("partition write proposal must be sealed");
        }
        validateSealedForRouting(requiredMetadata, requiredPartition, requiredCommand);
    }

    private static void validateLegacyUnsealed(ClusterMetadata metadata,
                                               PartitionDescriptor partition,
                                               PartitionWriteCommand command) {
        Long columnId = null;
        ClusterColumnMetadata column = null;
        for (var mutation : command.mutations()) {
            if (columnId == null) {
                columnId = mutation.columnId();
                column = metadata.requireColumn(columnId);
                if (column.schemaEpoch() != command.schemaEpoch()) {
                    throw new IllegalArgumentException("partition write schema epoch does not match column schema epoch");
                }
            } else if (mutation.columnId() != columnId) {
                throw new IllegalArgumentException("partition write command must target one column");
            }

            var calculatedKey = PartitionBoundaryRules.routingKey(column.schema(), mutation.keys());
            if (!partition.containsCalculatedKey(calculatedKey)) {
                throw new IllegalArgumentException("partition write key is outside the target partition");
            }
        }
    }

    private static void validateSealedForRouting(ClusterMetadata metadata,
                                                 PartitionDescriptor partition,
                                                 PartitionWriteCommand command) {
        var seal = requireSeal(command.seal());
        validateSealedForReplay(partition, command);
        if (seal.metadataEpoch() > metadata.epoch()) {
            throw new IllegalArgumentException("sealed partition write metadata epoch is newer than route metadata");
        }
        if (seal.partitionEpoch() != partition.epoch()) {
            throw new IllegalArgumentException("sealed partition write partition epoch does not match route partition");
        }
        if (!seal.hasSameRange(partition.startKeyInclusive(), partition.endKeyExclusive())) {
            throw new IllegalArgumentException("sealed partition write range does not match route partition");
        }
        if (seal.metadataEpoch() != metadata.epoch()) {
            return;
        }

        var column = metadata.requireColumn(command.mutations().getFirst().columnId());
        if (column.schemaEpoch() != command.schemaEpoch()) {
            throw new IllegalArgumentException("partition write schema epoch does not match column schema epoch");
        }
        for (var mutation : command.mutations()) {
            var expectedKey = PartitionBoundaryRules.routingKey(column.schema(), mutation.keys());
            if (!Arrays.equals(expectedKey.toByteArray(), mutation.requiredCalculatedKey().toByteArray())) {
                throw new IllegalArgumentException("sealed partition write calculated key does not match column schema");
            }
            if (!partition.containsCalculatedKey(expectedKey)) {
                throw new IllegalArgumentException("partition write key is outside the target partition");
            }
        }
    }

    private static void validateSealedForReplay(PartitionDescriptor partition, PartitionWriteCommand command) {
        var seal = requireSeal(command.seal());
        if (!seal.partitionId().equals(partition.partitionId())) {
            throw new IllegalArgumentException("sealed partition write targets a different partition");
        }
        long columnId = command.mutations().getFirst().columnId();
        for (var mutation : command.mutations()) {
            if (mutation.columnId() != columnId) {
                throw new IllegalArgumentException("partition write command must target one column");
            }
            if (mutation.calculatedKey() == null) {
                throw new IllegalArgumentException("sealed partition write mutation is missing its calculated key");
            }
            if (!seal.containsCalculatedKey(mutation.requiredCalculatedKey())) {
                throw new IllegalArgumentException("sealed partition write calculated key is outside admitted range");
            }
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static PartitionWriteCommandSeal requireSeal(PartitionWriteCommandSeal seal) {
        if (seal == null) {
            throw new IllegalStateException("seal must not be null");
        }
        return seal;
    }
}
