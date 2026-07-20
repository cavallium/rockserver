package it.cavallium.rockserver.core.cluster.command;

public enum ReplicatedMutationOp {
    PUT(1),
    DELETE(2),
    MERGE(3);

    private final int code;

    ReplicatedMutationOp(int code) {
        this.code = code;
    }

    int code() {
        return code;
    }

    static ReplicatedMutationOp fromCode(int code) {
        for (var operation : values()) {
            if (operation.code == code) {
                return operation;
            }
        }
        throw new IllegalArgumentException("Unknown replicated mutation operation code: " + code);
    }
}
