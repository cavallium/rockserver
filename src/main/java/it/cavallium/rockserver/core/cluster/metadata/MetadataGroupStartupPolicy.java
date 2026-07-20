package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ReplicaGroupMetadata;
import it.cavallium.rockserver.core.cluster.ReplicaRole;

public final class MetadataGroupStartupPolicy {

    public static final String GENERATED_LIVE_METADATA_REQUIRES_SNAPSHOT =
            "metadata group cannot start from generated live metadata without a Ratis snapshot";

    private MetadataGroupStartupPolicy() {
    }

    public static boolean requiresSnapshot(ClusterMetadata metadata) {
        requireArgument(metadata, "metadata");
        return !isNoSnapshotReplaySeed(metadata);
    }

    public static boolean isNoSnapshotReplaySeed(ClusterMetadata metadata) {
        requireArgument(metadata, "metadata");
        return metadata.epoch() == 0
                && metadata.shards().values().stream().allMatch(MetadataGroupStartupPolicy::isBootstrapReplicaGroup)
                && metadata.columnsById().isEmpty()
                && metadata.partitionsById().isEmpty()
                && metadata.cdcTokenTranslationsByEpoch().isEmpty()
                && metadata.schemaChangesById().isEmpty()
                && metadata.backupManifestsById().isEmpty();
    }

    private static boolean isBootstrapReplicaGroup(ReplicaGroupMetadata group) {
        if (group.term() != 0 || group.configIndex() != 0) {
            return false;
        }
        return group.members().stream().allMatch(member ->
                member.role() == ReplicaRole.VOTER
                        && member.matchIndex() == 0
                        && member.appliedIndex() == 0
                        && member.servingReads());
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
