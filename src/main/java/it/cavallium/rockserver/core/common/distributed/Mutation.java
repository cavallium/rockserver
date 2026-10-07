package it.cavallium.rockserver.core.common.distributed;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/** A physical mutation. DELETE_RANGE uses key/value as inclusive/exclusive bounds. */
public final class Mutation {
    public enum Kind { PUT, DELETE, MERGE, DELETE_RANGE }
    public enum ReturnMode { NONE, PREVIOUS, CURRENT }

    private final Kind kind;
    private final UUID columnId;
    private final byte[] key;
    private final byte[] value;
    private final ReturnMode returnMode;

    public Mutation(Kind kind, UUID columnId, byte[] key, byte[] value) {
        this(kind, columnId, key, value, ReturnMode.NONE);
    }

    public Mutation(Kind kind, UUID columnId, byte[] key, byte[] value, ReturnMode returnMode) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.columnId = Objects.requireNonNull(columnId, "columnId");
        this.returnMode = Objects.requireNonNull(returnMode, "returnMode");
        if (kind == Kind.DELETE_RANGE && returnMode != ReturnMode.NONE) {
            throw new IllegalArgumentException("Range deletion has no point return value");
        }
        Objects.requireNonNull(key, "key");
        if (kind == Kind.DELETE ? value != null : value == null) {
            throw new IllegalArgumentException("Only DELETE has a null value");
        }
        if ((long) key.length + (value == null ? 0 : value.length) > DistributedCommandCodec.MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException("Mutation exceeds command byte limit");
        }
        if (kind == Kind.DELETE_RANGE && Arrays.compareUnsigned(key, value) >= 0) {
            throw new IllegalArgumentException("Range lower bound must precede upper bound");
        }
        this.key = key.clone();
        this.value = value == null ? null : value.clone();
    }

    public Kind kind() { return kind; }
    public UUID columnId() { return columnId; }
    public byte[] key() { return key.clone(); }
    public byte[] value() { return value == null ? null : value.clone(); }
    public ReturnMode returnMode() { return returnMode; }

    int encodedSize() { return 26 + key.length + (value == null ? 0 : value.length); }

    @Override
    public boolean equals(Object other) {
        return other instanceof Mutation mutation && kind == mutation.kind
                && returnMode == mutation.returnMode && columnId.equals(mutation.columnId) && Arrays.equals(key, mutation.key)
                && Arrays.equals(value, mutation.value);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * Objects.hash(kind, columnId, returnMode) + Arrays.hashCode(key)) + Arrays.hashCode(value);
    }
}
