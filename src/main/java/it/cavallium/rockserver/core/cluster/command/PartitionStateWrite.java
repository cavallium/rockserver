package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;

import org.jetbrains.annotations.Nullable;

public record PartitionStateWrite(Operation operation, Buf key, @Nullable Buf value) {

    public enum Operation {
        PUT,
        DELETE,
        MERGE
    }

    public PartitionStateWrite {
        var requiredOperation = requireArgument(operation, "operation");
        var requiredKey = requireArgument(key, "key");
        key = Buf.wrap(requiredKey.toByteArray()).freeze();
        value = value == null ? null : Buf.wrap(value.toByteArray()).freeze();
        if (requiredOperation == Operation.DELETE && value != null) {
            throw new IllegalArgumentException("delete writes must not contain a value");
        }
        if ((requiredOperation == Operation.PUT || requiredOperation == Operation.MERGE) && value == null) {
            throw new IllegalArgumentException(requiredOperation + " writes must contain a value");
        }
    }

    public static PartitionStateWrite put(Buf key, Buf value) {
        return new PartitionStateWrite(Operation.PUT, key, value);
    }

    public static PartitionStateWrite delete(Buf key) {
        return new PartitionStateWrite(Operation.DELETE, key, null);
    }

    public static PartitionStateWrite merge(Buf key, Buf value) {
        return new PartitionStateWrite(Operation.MERGE, key, value);
    }

    public Buf requiredValue() {
        if (value == null) {
            throw new IllegalStateException("write does not contain a value");
        }
        return value;
    }

    private static <T> T requireArgument(@Nullable T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
