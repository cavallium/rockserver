package it.cavallium.rockserver.core.cluster.workflow;

import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;

public record PartitionMergePlan(MetadataCommand.MergePartitions mergeCommand,
                                 MetadataCommand.RecordCdcTokenTranslation cdcTokenTranslationCommand) {

    public PartitionMergePlan {
        requireArgument(mergeCommand, "mergeCommand");
        requireArgument(cdcTokenTranslationCommand, "cdcTokenTranslationCommand");
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
