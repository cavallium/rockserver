package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.PartitionBoundaryRules;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.ShardId;

final class PartitionWriteCommandReplayContext {

    private PartitionWriteCommandReplayContext() {
    }

    static Buf calculatedKey(ClusterMetadata metadata,
                             PartitionWriteCommand command,
                             ReplicatedMutation mutation) {
        var requiredMetadata = requireArgument(metadata, "metadata");
        var requiredCommand = requireArgument(command, "command");
        var requiredMutation = requireArgument(mutation, "mutation");
        if (requiredCommand.seal() != null) {
            return requiredMutation.requiredCalculatedKey();
        }
        var column = requiredMetadata.requireColumn(requiredMutation.columnId());
        if (column.schemaEpoch() != requiredCommand.schemaEpoch()) {
            throw new IllegalArgumentException("legacy unsealed partition write cannot replay after schema evolution");
        }
        return PartitionBoundaryRules.routingKey(column.schema(), requiredMutation.keys());
    }

    static long metadataEpoch(ClusterMetadata metadata, PartitionWriteCommand command) {
        var requiredMetadata = requireArgument(metadata, "metadata");
        var requiredCommand = requireArgument(command, "command");
        return requiredCommand.seal() == null ? requiredMetadata.epoch() : requiredCommand.seal().metadataEpoch();
    }

    static ReplayPartition partition(PartitionDescriptor partition, PartitionWriteCommand command) {
        var requiredPartition = requireArgument(partition, "partition");
        var requiredCommand = requireArgument(command, "command");
        var seal = requiredCommand.seal();
        if (seal == null) {
            return new ReplayPartition(requiredPartition.partitionId(), requiredPartition.epoch());
        }
        if (!seal.partitionId().equals(requiredPartition.partitionId())) {
            throw new IllegalArgumentException("sealed partition write targets a different partition");
        }
        return new ReplayPartition(seal.partitionId(), seal.partitionEpoch());
    }

    record ReplayPartition(ShardId partitionId, long partitionEpoch) {
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
