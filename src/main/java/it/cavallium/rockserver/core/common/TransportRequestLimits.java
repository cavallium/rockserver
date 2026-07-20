package it.cavallium.rockserver.core.common;

public final class TransportRequestLimits {

    public static final int MAX_KEY_PARTS = 1024;
    public static final int MAX_BATCH_ENTRIES = 16_384;

    private TransportRequestLimits() {
    }

    public static void requireKeyParts(String context, int count) {
        requireCount(context, count, MAX_KEY_PARTS);
    }

    public static void requireBatchEntries(String context, int count) {
        requireCount(context, count, MAX_BATCH_ENTRIES);
    }

    public static void requireAdditionalBatchEntries(String context, int existingCount, int additionalCount) {
        if (existingCount < 0 || additionalCount < 0) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
                    context + " count " + ((long) existingCount + additionalCount) + " exceeds limit "
                            + MAX_BATCH_ENTRIES
            );
        }
        requireCount(context, (long) existingCount + additionalCount, MAX_BATCH_ENTRIES);
    }

    private static void requireCount(String context, long count, int max) {
        if (count < 0 || count > max) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
                    context + " count " + count + " exceeds limit " + max
            );
        }
    }
}
