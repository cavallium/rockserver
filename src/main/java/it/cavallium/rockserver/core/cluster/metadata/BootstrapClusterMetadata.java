package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ClusterNodeMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;

import java.util.LinkedHashMap;
import java.util.Map;

public final class BootstrapClusterMetadata {

    private BootstrapClusterMetadata() {
    }

    public static ClusterMetadata fromPlan(BootstrapPlan plan) {
        var requiredPlan = requirePlan(plan);
        var manifest = requiredPlan.manifest();
        var nodes = new LinkedHashMap<NodeId, ClusterNodeMetadata>();
        for (var peer : manifest.initialPeers()) {
            nodes.put(peer.nodeId(), new ClusterNodeMetadata(
                    peer.nodeId(),
                    peer.publicEndpoint(),
                    peer.ratisEndpoint(),
                    peer.failureDomain()
            ));
        }
        return new ClusterMetadata(
                manifest.clusterId(),
                manifest.bootstrapEpoch(),
                Map.of(),
                Map.of(),
                Map.of(),
                nodes
        );
    }

    private static BootstrapPlan requirePlan(BootstrapPlan plan) {
        if (plan == null) {
            throw new IllegalArgumentException("plan must not be null");
        }
        return plan;
    }
}
