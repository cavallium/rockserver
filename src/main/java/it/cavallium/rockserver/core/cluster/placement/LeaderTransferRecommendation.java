package it.cavallium.rockserver.core.cluster.placement;

import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.ShardId;

public record LeaderTransferRecommendation(ShardId partitionId,
                                           long partitionEpoch,
                                           NodeId fromLeader,
                                           NodeId toLeader,
                                           int projectedSourceLeaderCount,
                                           int projectedTargetLeaderCount) {

    public LeaderTransferRecommendation {
        requireArgument(partitionId, "partitionId");
        if (partitionEpoch < 0) {
            throw new IllegalArgumentException("partitionEpoch must be >= 0");
        }
        requireArgument(fromLeader, "fromLeader");
        requireArgument(toLeader, "toLeader");
        if (fromLeader.equals(toLeader)) {
            throw new IllegalArgumentException("leader transfer source and target must be distinct");
        }
        if (projectedSourceLeaderCount < 0) {
            throw new IllegalArgumentException("projectedSourceLeaderCount must be >= 0");
        }
        if (projectedTargetLeaderCount < 0) {
            throw new IllegalArgumentException("projectedTargetLeaderCount must be >= 0");
        }
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
