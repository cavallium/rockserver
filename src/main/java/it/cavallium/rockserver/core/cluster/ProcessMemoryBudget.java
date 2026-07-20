package it.cavallium.rockserver.core.cluster;

public record ProcessMemoryBudget(long totalBytes,
                                  long blockCacheBytes,
                                  long writeBufferBytes,
                                  int maxOpenPartitionReplicas) {

    public ProcessMemoryBudget {
        if (totalBytes <= 0) {
            throw new IllegalArgumentException("totalBytes must be > 0");
        }
        requireNonNegative(blockCacheBytes, "blockCacheBytes");
        requireNonNegative(writeBufferBytes, "writeBufferBytes");
        if (maxOpenPartitionReplicas <= 0) {
            throw new IllegalArgumentException("maxOpenPartitionReplicas must be > 0");
        }
        if (blockCacheBytes > totalBytes - writeBufferBytes) {
            throw new IllegalArgumentException("block cache and write buffer budgets exceed total process budget");
        }
    }

    public long reservedBytes() {
        return blockCacheBytes + writeBufferBytes;
    }

    public long unreservedBytes() {
        return totalBytes - reservedBytes();
    }

    public long writeBufferBytesPerPartitionReplica() {
        return writeBufferBytes / maxOpenPartitionReplicas;
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }
}
