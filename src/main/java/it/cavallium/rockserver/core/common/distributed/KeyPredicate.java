package it.cavallium.rockserver.core.common.distributed;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/** Physical-key equality condition: null expects absence, empty bytes expect a present empty value. */
public final class KeyPredicate {
    private final UUID columnId;
    private final byte[] key;
    private final byte[] expectedValue;

    public KeyPredicate(UUID columnId, byte[] key, byte[] expectedValue) {
        this.columnId = Objects.requireNonNull(columnId, "columnId");
        Objects.requireNonNull(key, "key");
        if ((long) key.length + (expectedValue == null ? 0 : expectedValue.length)
                > DistributedCommandCodec.MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException("Predicate exceeds command byte limit");
        }
        this.key = key.clone();
        this.expectedValue = expectedValue == null ? null : expectedValue.clone();
    }

    public UUID columnId() { return columnId; }
    public byte[] key() { return key.clone(); }
    public byte[] expectedValue() { return expectedValue == null ? null : expectedValue.clone(); }
    int encodedSize() { return 25 + key.length + (expectedValue == null ? 0 : expectedValue.length); }

    @Override public boolean equals(Object other) {
        return other instanceof KeyPredicate predicate && columnId.equals(predicate.columnId)
                && Arrays.equals(key, predicate.key) && Arrays.equals(expectedValue, predicate.expectedValue);
    }
    @Override public int hashCode() {
        return 31 * (31 * columnId.hashCode() + Arrays.hashCode(key)) + Arrays.hashCode(expectedValue);
    }
}
