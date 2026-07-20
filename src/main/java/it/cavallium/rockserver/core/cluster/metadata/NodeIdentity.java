package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.NodeId;

public record NodeIdentity(ClusterId clusterId, NodeId nodeId) {

    public NodeIdentity {
        requireArgument(clusterId, "clusterId");
        requireArgument(nodeId, "nodeId");
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
