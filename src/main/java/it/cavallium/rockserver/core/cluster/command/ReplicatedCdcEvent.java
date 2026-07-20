package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;

import org.jetbrains.annotations.Nullable;

public record ReplicatedCdcEvent(ReplicatedCdcToken token,
                                 long schemaEpoch,
                                 long columnId,
                                 Buf key,
                                 @Nullable Buf value,
                                 ReplicatedMutationOp operation) {

    public ReplicatedCdcEvent {
        requireArgument(token, "token");
        if (schemaEpoch < 0) {
            throw new IllegalArgumentException("schemaEpoch must be >= 0");
        }
        if (columnId < 0) {
            throw new IllegalArgumentException("columnId must be >= 0");
        }
        key = copy(requireArgument(key, "key"));
        value = copy(value);
        requireArgument(operation, "operation");
        if (operation == ReplicatedMutationOp.DELETE && value != null) {
            throw new IllegalArgumentException("delete CDC events must not contain a value");
        }
        if ((operation == ReplicatedMutationOp.PUT || operation == ReplicatedMutationOp.MERGE) && value == null) {
            throw new IllegalArgumentException(operation + " CDC events must contain a value");
        }
    }

    @Override
    public Buf key() {
        return copy(key);
    }

    @Override
    public @Nullable Buf value() {
        return copy(value);
    }

    private static @Nullable Buf copy(@Nullable Buf value) {
        return value == null ? null : Buf.wrap(value.toByteArray()).freeze();
    }

    private static <T> T requireArgument(@Nullable T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
