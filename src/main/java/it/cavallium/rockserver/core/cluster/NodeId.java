package it.cavallium.rockserver.core.cluster;

import java.io.Serial;
import java.io.Serializable;

import org.jetbrains.annotations.NotNull;

public record NodeId(String value) implements Comparable<NodeId>, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public NodeId {
        SingleLineStrings.requireNonBlank(value, "node id");
    }

    public static NodeId of(String value) {
        return new NodeId(value);
    }

    @Override
    public int compareTo(@NotNull NodeId other) {
        return value.compareTo(other.value);
    }

    @Override
    public @NotNull String toString() {
        return value;
    }
}
