package it.cavallium.rockserver.core.cluster.workflow;

import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.ShardId;

public record ReplicaCatchUpPlan(ShardId partitionId,
                                 long partitionEpoch,
                                 NodeId replicaId,
                                 long requiredMatchIndex,
                                 long requiredAppliedIndex) {

    public ReplicaCatchUpPlan {
        requireArgument(partitionId, "partitionId");
        if (partitionEpoch < 0) {
            throw new IllegalArgumentException("partitionEpoch must be >= 0");
        }
        requireArgument(replicaId, "replicaId");
        if (requiredMatchIndex < 0) {
            throw new IllegalArgumentException("requiredMatchIndex must be >= 0");
        }
        if (requiredAppliedIndex < 0) {
            throw new IllegalArgumentException("requiredAppliedIndex must be >= 0");
        }
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
