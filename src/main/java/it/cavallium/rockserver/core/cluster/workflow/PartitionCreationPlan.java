package it.cavallium.rockserver.core.cluster.workflow;

import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;

public record PartitionCreationPlan(MetadataCommand.CreatePartition command) {

    public PartitionCreationPlan {
        if (command == null) {
            throw new IllegalArgumentException("command must not be null");
        }
    }
}
