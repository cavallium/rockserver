package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;

import java.util.Arrays;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class ReplicatedMutation {

    private final ReplicatedMutationOp operation;
    private final long columnId;
    private final Keys keys;
    private final Buf value;
    private final Buf calculatedKey;

    public ReplicatedMutation(ReplicatedMutationOp operation, long columnId, Keys keys, @Nullable Buf value) {
        this(operation, columnId, keys, value, null);
    }

    public ReplicatedMutation(ReplicatedMutationOp operation,
                              long columnId,
                              Keys keys,
                              @Nullable Buf value,
                              @Nullable Buf calculatedKey) {
        this.operation = requireArgument(operation, "operation");
        if (columnId < 0) {
            throw new IllegalArgumentException("columnId must be >= 0");
        }
        this.columnId = columnId;
        this.keys = copyKeys(requireArgument(keys, "keys"));
        this.value = copyValue(value);
        this.calculatedKey = copyValue(calculatedKey);
        if (this.operation == ReplicatedMutationOp.DELETE && value != null) {
            throw new IllegalArgumentException("delete mutations must not contain a value");
        }
        if ((this.operation == ReplicatedMutationOp.PUT || this.operation == ReplicatedMutationOp.MERGE)
                && value == null) {
            throw new IllegalArgumentException(this.operation + " mutations must contain a value");
        }
    }

    public static ReplicatedMutation put(long columnId, Keys keys, @NotNull Buf value) {
        return new ReplicatedMutation(ReplicatedMutationOp.PUT, columnId, keys, value);
    }

    public static ReplicatedMutation delete(long columnId, Keys keys) {
        return new ReplicatedMutation(ReplicatedMutationOp.DELETE, columnId, keys, null);
    }

    public static ReplicatedMutation merge(long columnId, Keys keys, @NotNull Buf value) {
        return new ReplicatedMutation(ReplicatedMutationOp.MERGE, columnId, keys, value);
    }

    public ReplicatedMutationOp operation() {
        return operation;
    }

    public long columnId() {
        return columnId;
    }

    public Keys keys() {
        return copyKeys(keys);
    }

    public @Nullable Buf value() {
        return copyValue(value);
    }

    public @NotNull Buf requiredValue() {
        if (value == null) {
            throw new IllegalStateException("Mutation does not contain a value");
        }
        return copyRequiredValue(value);
    }

    public @Nullable Buf calculatedKey() {
        return copyValue(calculatedKey);
    }

    public @NotNull Buf requiredCalculatedKey() {
        if (calculatedKey == null) {
            throw new IllegalStateException("Mutation does not contain a sealed calculated key");
        }
        return copyRequiredValue(calculatedKey);
    }

    public ReplicatedMutation withCalculatedKey(@NotNull Buf newCalculatedKey) {
        var requiredCalculatedKey = requireArgument(newCalculatedKey, "newCalculatedKey");
        if (calculatedKey != null && !Arrays.equals(calculatedKey.toByteArray(), requiredCalculatedKey.toByteArray())) {
            throw new IllegalArgumentException("Mutation calculated key is already sealed with a different value");
        }
        return new ReplicatedMutation(operation, columnId, keys, value, requiredCalculatedKey);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ReplicatedMutation that)) {
            return false;
        }
        return columnId == that.columnId
                && operation == that.operation
                && keys.equals(that.keys)
                && Arrays.equals(valueBytes(value), valueBytes(that.value))
                && Arrays.equals(valueBytes(calculatedKey), valueBytes(that.calculatedKey));
    }

    @Override
    public int hashCode() {
        int result = operation.hashCode();
        result = 31 * result + Long.hashCode(columnId);
        result = 31 * result + keys.hashCode();
        result = 31 * result + Arrays.hashCode(valueBytes(value));
        result = 31 * result + Arrays.hashCode(valueBytes(calculatedKey));
        return result;
    }

    @Override
    public String toString() {
        return "ReplicatedMutation[operation=" + operation
                + ", columnId=" + columnId
                + ", keys=" + keys
                + ", valueBytes=" + (value == null ? 0 : value.toByteArray().length)
                + ", calculatedKeyBytes=" + (calculatedKey == null ? 0 : calculatedKey.toByteArray().length)
                + "]";
    }

    private static Keys copyKeys(Keys source) {
        var sourceKeys = requireArgument(source.keys(), "keys array");
        var copiedKeys = new Buf[sourceKeys.length];
        for (int i = 0; i < sourceKeys.length; i++) {
            copiedKeys[i] = copyRequiredValue(requireArgument(sourceKeys[i], "keys[" + i + "]"));
        }
        return new Keys(copiedKeys);
    }

    private static @Nullable Buf copyValue(@Nullable Buf value) {
        return value == null ? null : Buf.wrap(value.toByteArray()).freeze();
    }

    private static Buf copyRequiredValue(Buf value) {
        return Buf.wrap(value.toByteArray()).freeze();
    }

    private static byte @Nullable [] valueBytes(@Nullable Buf value) {
        return value == null ? null : value.toByteArray();
    }

    private static <T> T requireArgument(@Nullable T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
