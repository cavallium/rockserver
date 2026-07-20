package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;

public final class MetadataStateMachine {

    private ClusterMetadata metadata;
    private long lastAppliedIndex;

    public MetadataStateMachine(ClusterMetadata metadata) {
        this(metadata, 0);
    }

    public MetadataStateMachine(ClusterMetadata metadata, long initialLastAppliedIndex) {
        this.metadata = requireArgument(metadata, "metadata");
        if (initialLastAppliedIndex < 0) {
            throw new IllegalArgumentException("initialLastAppliedIndex must be >= 0");
        }
        this.lastAppliedIndex = initialLastAppliedIndex;
    }

    public synchronized ApplyResult applyCommitted(long logIndex, byte[] payload) {
        if (logIndex < 0) {
            throw new IllegalArgumentException("logIndex must be >= 0");
        }
        requireArgument(payload, "payload");
        if (logIndex <= lastAppliedIndex) {
            return new ApplyResult(logIndex, false, metadata);
        }

        var command = MetadataCommandValidator.decodeCommitted(payload);
        metadata = MetadataCommandApplier.apply(metadata, command);
        lastAppliedIndex = logIndex;
        return new ApplyResult(logIndex, true, metadata);
    }

    public synchronized ClusterMetadata metadata() {
        return metadata;
    }

    public synchronized long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    public record ApplyResult(long logIndex, boolean applied, ClusterMetadata metadata) {

        public ApplyResult {
            if (logIndex < 0) {
                throw new IllegalArgumentException("logIndex must be >= 0");
            }
            requireArgument(metadata, "metadata");
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
