package it.cavallium.rockserver.core.cluster.rebalance;

import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.ShardId;

import org.jetbrains.annotations.Nullable;

public record AdminRebalanceRequest(String requestId,
                                    AdminRebalanceAction action,
                                    ShardId partitionId,
                                    @Nullable ShardId secondaryPartitionId,
                                    @Nullable NodeId targetNodeId,
                                    long expectedPartitionEpoch,
                                    String reason,
                                    boolean dryRun) {

    public AdminRebalanceRequest {
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        requireArgument(action, "action");
        requireArgument(partitionId, "partitionId");
        if (expectedPartitionEpoch < 0) {
            throw new IllegalArgumentException("expectedPartitionEpoch must be >= 0");
        }
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("reason must not be blank");
        }
        switch (action) {
            case MERGE_PARTITIONS -> requireArgument(secondaryPartitionId, "secondaryPartitionId");
            case TRANSFER_LEADER, MOVE_REPLICA -> requireArgument(targetNodeId, "targetNodeId");
            case SPLIT_PARTITION -> {
            }
        }
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
