package it.cavallium.rockserver.core.cluster;

public record NodeHealthReport(NodeId nodeId,
                               NodeHealthStatus status,
                               long reportedAtMillis,
                               long availableDiskBytes,
                               long totalDiskBytes,
                               long usedMemoryBytes,
                               long maxMemoryBytes,
                               int partitionReplicaCount,
                               double processCpuLoad) {

    public NodeHealthReport {
        requireArgument(nodeId, "nodeId");
        requireArgument(status, "status");
        requireNonNegative(reportedAtMillis, "reportedAtMillis");
        requireNonNegative(availableDiskBytes, "availableDiskBytes");
        requireNonNegative(totalDiskBytes, "totalDiskBytes");
        if (availableDiskBytes > totalDiskBytes) {
            throw new IllegalArgumentException("availableDiskBytes must be <= totalDiskBytes");
        }
        requireNonNegative(usedMemoryBytes, "usedMemoryBytes");
        requireNonNegative(maxMemoryBytes, "maxMemoryBytes");
        if (usedMemoryBytes > maxMemoryBytes) {
            throw new IllegalArgumentException("usedMemoryBytes must be <= maxMemoryBytes");
        }
        if (partitionReplicaCount < 0) {
            throw new IllegalArgumentException("partitionReplicaCount must be >= 0");
        }
        if (Double.isNaN(processCpuLoad) || processCpuLoad < 0.0d || processCpuLoad > 1.0d) {
            throw new IllegalArgumentException("processCpuLoad must be between 0.0 and 1.0");
        }
    }

    public boolean isFresh(long nowMillis, long maxAgeMillis) {
        requireNonNegative(nowMillis, "nowMillis");
        requireNonNegative(maxAgeMillis, "maxAgeMillis");
        return reportedAtMillis <= nowMillis && nowMillis - reportedAtMillis <= maxAgeMillis;
    }

    public boolean canAcceptReplica(long minAvailableDiskBytes,
                                    double maxMemoryUtilization,
                                    long nowMillis,
                                    long maxReportAgeMillis) {
        requireNonNegative(minAvailableDiskBytes, "minAvailableDiskBytes");
        if (Double.isNaN(maxMemoryUtilization) || maxMemoryUtilization < 0.0d || maxMemoryUtilization > 1.0d) {
            throw new IllegalArgumentException("maxMemoryUtilization must be between 0.0 and 1.0");
        }
        return status == NodeHealthStatus.HEALTHY
                && isFresh(nowMillis, maxReportAgeMillis)
                && availableDiskBytes >= minAvailableDiskBytes
                && memoryUtilization() <= maxMemoryUtilization;
    }

    public double diskUtilization() {
        if (totalDiskBytes == 0) {
            return 1.0d;
        }
        return 1.0d - ((double) availableDiskBytes / (double) totalDiskBytes);
    }

    public double memoryUtilization() {
        if (maxMemoryBytes == 0) {
            return 1.0d;
        }
        return (double) usedMemoryBytes / (double) maxMemoryBytes;
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
