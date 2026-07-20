package it.cavallium.rockserver.core.cluster.rebalance;

import it.cavallium.rockserver.core.cluster.ShardId;

public record PartitionLoadReport(ShardId partitionId,
                                  long partitionEpoch,
                                  long observedAtMillis,
                                  double readOpsPerSecond,
                                  double writeOpsPerSecond,
                                  double bytesPerSecond,
                                  double averageLatencyMillis,
                                  int pendingRequests) {

    public PartitionLoadReport {
        if (partitionId == null) {
            throw new IllegalArgumentException("partitionId must not be null");
        }
        requireNonNegative(partitionEpoch, "partitionEpoch");
        requireNonNegative(observedAtMillis, "observedAtMillis");
        requireFiniteNonNegative(readOpsPerSecond, "readOpsPerSecond");
        requireFiniteNonNegative(writeOpsPerSecond, "writeOpsPerSecond");
        requireFiniteNonNegative(bytesPerSecond, "bytesPerSecond");
        requireFiniteNonNegative(averageLatencyMillis, "averageLatencyMillis");
        if (pendingRequests < 0) {
            throw new IllegalArgumentException("pendingRequests must be >= 0");
        }
    }

    public double totalOpsPerSecond() {
        return readOpsPerSecond + writeOpsPerSecond;
    }

    public boolean isFresh(long nowMillis, long maxAgeMillis) {
        requireNonNegative(nowMillis, "nowMillis");
        requireNonNegative(maxAgeMillis, "maxAgeMillis");
        return observedAtMillis <= nowMillis && nowMillis - observedAtMillis <= maxAgeMillis;
    }

    private static void requireFiniteNonNegative(double value, String name) {
        if (!Double.isFinite(value) || value < 0.0d) {
            throw new IllegalArgumentException(name + " must be finite and >= 0");
        }
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }
}
