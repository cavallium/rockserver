package it.cavallium.rockserver.core.cluster.rebalance;

import java.util.List;

public record HotPartition(PartitionLoadReport report,
                           List<HotPartitionReason> reasons) {

    public HotPartition {
        requireArgument(report, "report");
        var requiredReasons = requireArgument(reasons, "reasons");
        if (requiredReasons.isEmpty()) {
            throw new IllegalArgumentException("reasons must not be empty");
        }
        for (HotPartitionReason reason : requiredReasons) {
            requireArgument(reason, "reasons entry");
        }
        reasons = List.copyOf(requiredReasons);
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
