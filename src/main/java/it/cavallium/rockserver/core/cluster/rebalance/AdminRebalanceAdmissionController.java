package it.cavallium.rockserver.core.cluster.rebalance;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;

public final class AdminRebalanceAdmissionController {

    public AdminRebalanceDecision admit(ClusterMetadata metadata,
                                        long expectedMetadataEpoch,
                                        AdminRebalanceRequest request,
                                        int runningRebalanceOperations,
                                        int maxConcurrentRebalanceOperations) {
        var requiredMetadata = requireArgument(metadata, "metadata");
        var requiredRequest = requireArgument(request, "request");
        if (expectedMetadataEpoch < 0) {
            throw new IllegalArgumentException("expectedMetadataEpoch must be >= 0");
        }
        if (requiredMetadata.epoch() != expectedMetadataEpoch) {
            throw new IllegalArgumentException("metadata epoch does not match expectedMetadataEpoch");
        }
        if (runningRebalanceOperations < 0) {
            throw new IllegalArgumentException("runningRebalanceOperations must be >= 0");
        }
        if (maxConcurrentRebalanceOperations <= 0) {
            throw new IllegalArgumentException("maxConcurrentRebalanceOperations must be > 0");
        }
        if (runningRebalanceOperations >= maxConcurrentRebalanceOperations) {
            throw new IllegalStateException("too many rebalance operations are already running");
        }
        var partition = requiredMetadata.requirePartition(requiredRequest.partitionId());
        if (partition.epoch() != requiredRequest.expectedPartitionEpoch()) {
            throw new IllegalArgumentException("partition epoch does not match expectedPartitionEpoch");
        }
        if (partition.isSplitting()) {
            throw new IllegalStateException("partition already has a topology change in progress");
        }
        if (requiredRequest.secondaryPartitionId() != null) {
            var secondaryPartition = requiredMetadata.requirePartition(requiredRequest.secondaryPartitionId());
            if (secondaryPartition.isSplitting()) {
                throw new IllegalStateException("secondary partition already has a topology change in progress");
            }
        }
        return new AdminRebalanceDecision(requiredRequest, requiredRequest.dryRun()
                ? "rebalance request accepted for dry run"
                : "rebalance request admitted");
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
