package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.rockserver.core.cluster.ClusterId;

import java.util.List;

import org.jetbrains.annotations.Nullable;

public record BootstrapPlan(BootstrapMode mode,
                            BootstrapManifest manifest,
                            @Nullable ClusterId sourceClusterId) {

    public BootstrapPlan {
        var requiredMode = requireArgument(mode, "mode");
        var requiredManifest = requireArgument(manifest, "manifest");
        if (requiredMode == BootstrapMode.DISASTER_RESTORE) {
            var requiredSourceClusterId = requireArgument(sourceClusterId, "sourceClusterId");
            if (requiredSourceClusterId.equals(requiredManifest.clusterId())) {
                throw new IllegalArgumentException("disaster restore must bootstrap into a new cluster ID");
            }
        } else if (sourceClusterId != null) {
            throw new IllegalArgumentException("sourceClusterId is only valid for disaster restore");
        }
        if (requiredMode == BootstrapMode.SINGLE_NODE_DEVELOPMENT && requiredManifest.initialPeers().size() != 1) {
            throw new IllegalArgumentException("single-node development bootstrap requires exactly one peer");
        }
    }

    public static BootstrapPlan newCluster(BootstrapManifest manifest) {
        return new BootstrapPlan(BootstrapMode.NEW_CLUSTER, manifest, null);
    }

    public static BootstrapPlan singleNodeDevelopment(ClusterId clusterId,
                                                      String metadataGroupId,
                                                      BootstrapPeer peer,
                                                      long bootstrapEpoch) {
        return new BootstrapPlan(
                BootstrapMode.SINGLE_NODE_DEVELOPMENT,
                new BootstrapManifest(clusterId, metadataGroupId, List.of(peer), bootstrapEpoch),
                null
        );
    }

    public static BootstrapPlan disasterRestore(ClusterId sourceClusterId, BootstrapManifest restoreManifest) {
        return new BootstrapPlan(BootstrapMode.DISASTER_RESTORE, restoreManifest, sourceClusterId);
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
