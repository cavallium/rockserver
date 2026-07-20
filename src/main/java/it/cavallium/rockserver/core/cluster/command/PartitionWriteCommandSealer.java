package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.PartitionBoundaryRules;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;

import java.util.ArrayList;

public final class PartitionWriteCommandSealer {

    private PartitionWriteCommandSealer() {
    }

    public static PartitionWriteCommand seal(ClusterMetadata metadata, PartitionWriteCommand command) {
        var requiredMetadata = requireArgument(metadata, "metadata");
        var requiredCommand = requireArgument(command, "command");
        if (requiredCommand.seal() != null) {
            var partition = requiredMetadata.requirePartition(requiredCommand.seal().partitionId());
            PartitionWriteCommandValidator.validate(requiredMetadata, partition, requiredCommand);
            return requiredCommand;
        }

        Long columnId = null;
        ClusterColumnMetadata column = null;
        PartitionDescriptor partition = null;
        var mutations = new ArrayList<ReplicatedMutation>(requiredCommand.mutations().size());
        for (var mutation : requiredCommand.mutations()) {
            if (columnId == null) {
                columnId = mutation.columnId();
                column = requiredMetadata.requireColumn(columnId);
                if (column.schemaEpoch() != requiredCommand.schemaEpoch()) {
                    throw new IllegalArgumentException("partition write schema epoch does not match column schema epoch");
                }
            } else if (mutation.columnId() != columnId) {
                throw new IllegalArgumentException("partition write command must target one column");
            }

            var calculatedKey = PartitionBoundaryRules.routingKey(column.schema(), mutation.keys());
            if (partition == null) {
                partition = PartitionBoundaryRules.requirePartition(requiredMetadata.partitionDescriptors(), calculatedKey);
            } else if (!partition.containsCalculatedKey(calculatedKey)) {
                throw new IllegalArgumentException("partition write key is outside the target partition");
            }
            mutations.add(mutation.withCalculatedKey(calculatedKey));
        }

        var sealedPartition = requirePartition(partition);
        var result = new PartitionWriteCommand(
                requiredCommand.requestId(),
                requiredCommand.schemaEpoch(),
                mutations,
                requiredCommand.responseMode(),
                new PartitionWriteCommandSeal(requiredMetadata.epoch(), sealedPartition.partitionId(), sealedPartition.epoch(),
                        sealedPartition.startKeyInclusive(), sealedPartition.endKeyExclusive())
        );
        PartitionWriteCommandValidator.validate(requiredMetadata, sealedPartition, result);
        return result;
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static PartitionDescriptor requirePartition(PartitionDescriptor partition) {
        if (partition == null) {
            throw new IllegalStateException("partition must not be null");
        }
        return partition;
    }
}
