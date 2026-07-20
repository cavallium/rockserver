package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.PartitionBoundaryRules;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;

public final class PartitionPointReadCommandValidator {

    private PartitionPointReadCommandValidator() {
    }

    public static Buf calculatedKey(ClusterMetadata metadata, PartitionPointReadCommand command) {
        var requiredMetadata = requireArgument(metadata, "metadata");
        var requiredCommand = requireArgument(command, "command");
        var column = requiredMetadata.requireColumn(requiredCommand.columnId());
        if (column.schemaEpoch() != requiredCommand.schemaEpoch()) {
            throw new IllegalArgumentException("partition point read schema epoch does not match column schema epoch");
        }
        return PartitionBoundaryRules.routingKey(column.schema(), requiredCommand.keys());
    }

    public static Buf validateForPartition(ClusterMetadata metadata,
                                           PartitionDescriptor partition,
                                           PartitionPointReadCommand command) {
        var requiredMetadata = requireArgument(metadata, "metadata");
        var requiredPartition = requireArgument(partition, "partition");
        var requiredCommand = requireArgument(command, "command");
        var calculatedKey = calculatedKey(requiredMetadata, requiredCommand);
        if (!requiredPartition.containsCalculatedKey(calculatedKey)) {
            throw new IllegalArgumentException("partition point read key is outside the target partition");
        }
        return calculatedKey;
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
