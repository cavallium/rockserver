package it.cavallium.rockserver.core.cluster.rebalance;

public record HotPartitionThresholds(double minTotalOpsPerSecond,
                                     double minWriteOpsPerSecond,
                                     double minBytesPerSecond,
                                     double maxAverageLatencyMillis,
                                     int maxPendingRequests,
                                     long maxReportAgeMillis) {

    public HotPartitionThresholds {
        requirePositive(minTotalOpsPerSecond, "minTotalOpsPerSecond");
        requirePositive(minWriteOpsPerSecond, "minWriteOpsPerSecond");
        requirePositive(minBytesPerSecond, "minBytesPerSecond");
        requirePositive(maxAverageLatencyMillis, "maxAverageLatencyMillis");
        if (maxPendingRequests <= 0) {
            throw new IllegalArgumentException("maxPendingRequests must be > 0");
        }
        if (maxReportAgeMillis < 0) {
            throw new IllegalArgumentException("maxReportAgeMillis must be >= 0");
        }
    }

    private static void requirePositive(double value, String name) {
        if (!Double.isFinite(value) || value <= 0.0d) {
            throw new IllegalArgumentException(name + " must be finite and > 0");
        }
    }
}
