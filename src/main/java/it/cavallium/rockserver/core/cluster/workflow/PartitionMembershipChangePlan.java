package it.cavallium.rockserver.core.cluster.workflow;

import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;

public record PartitionMembershipChangePlan(MembershipChangeKind kind,
                                            MetadataCommand.ChangePartitionPeers command) {

    public PartitionMembershipChangePlan {
        requireArgument(kind, "kind");
        requireArgument(command, "command");
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
