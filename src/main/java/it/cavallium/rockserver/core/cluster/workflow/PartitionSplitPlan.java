package it.cavallium.rockserver.core.cluster.workflow;

import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;

public record PartitionSplitPlan(MetadataCommand.SplitPartition splitCommand,
                                 MetadataCommand.RecordCdcTokenTranslation cdcTokenTranslationCommand) {

    public PartitionSplitPlan {
        requireArgument(splitCommand, "splitCommand");
        requireArgument(cdcTokenTranslationCommand, "cdcTokenTranslationCommand");
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
