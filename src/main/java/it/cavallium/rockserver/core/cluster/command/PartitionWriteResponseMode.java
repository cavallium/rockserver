package it.cavallium.rockserver.core.cluster.command;

import java.util.List;

public enum PartitionWriteResponseMode {
    NONE(0),
    PREVIOUS_VALUE(1),
    PREVIOUS_PRESENCE(2),
    DELTA(3),
    CHANGED(4),
    MERGED_VALUE(5);

    private final int code;

    PartitionWriteResponseMode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public boolean producesResponse() {
        return this != NONE;
    }

    public void validate(List<ReplicatedMutation> mutations) {
        requireArgument(mutations, "mutations");
        for (var mutation : mutations) {
            requireArgument(mutation, "mutation");
            if (!isCompatibleWith(mutation.operation())) {
                throw new IllegalArgumentException("partition write response mode " + this
                        + " is not compatible with " + mutation.operation());
            }
        }
    }

    public boolean isCompatibleWith(ReplicatedMutationOp operation) {
        return switch (this) {
            case NONE -> true;
            case PREVIOUS_VALUE, PREVIOUS_PRESENCE -> operation == ReplicatedMutationOp.PUT
                    || operation == ReplicatedMutationOp.DELETE;
            case DELTA, CHANGED -> operation == ReplicatedMutationOp.PUT;
            case MERGED_VALUE -> operation == ReplicatedMutationOp.MERGE;
        };
    }

    public static PartitionWriteResponseMode fromCode(int code) {
        for (var mode : values()) {
            if (mode.code == code) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown partition write response mode: " + code);
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
