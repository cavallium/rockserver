package it.cavallium.rockserver.core.cluster.command;

import java.util.Arrays;

public record ReplicatedLogCommit(long index, boolean committed, byte[] responsePayload) {

    public ReplicatedLogCommit(long index, boolean committed) {
        this(index, committed, null);
    }

    public ReplicatedLogCommit {
        if (index < 0) {
            throw new IllegalArgumentException("index must be >= 0");
        }
        if (committed && index == 0) {
            throw new IllegalArgumentException("committed log index must be > 0");
        }
        responsePayload = responsePayload == null ? null : responsePayload.clone();
    }

    @Override
    public byte[] responsePayload() {
        return responsePayload == null ? null : responsePayload.clone();
    }

    public boolean hasResponsePayload() {
        return responsePayload != null;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ReplicatedLogCommit that)) {
            return false;
        }
        return index == that.index
                && committed == that.committed
                && Arrays.equals(responsePayload, that.responsePayload);
    }

    @Override
    public int hashCode() {
        var result = Long.hashCode(index);
        result = 31 * result + Boolean.hashCode(committed);
        result = 31 * result + Arrays.hashCode(responsePayload);
        return result;
    }
}
