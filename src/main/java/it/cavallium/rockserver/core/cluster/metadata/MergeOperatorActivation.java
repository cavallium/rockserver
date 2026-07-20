package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.buffer.Buf;

public record MergeOperatorActivation(String name,
                                      long version,
                                      Buf artifactHash) {

    public MergeOperatorActivation {
        var requiredName = requireArgument(name, "name");
        if (requiredName.isBlank()) {
            throw new IllegalArgumentException("merge operator name must not be blank");
        }
        if (requiredName.indexOf('\r') >= 0 || requiredName.indexOf('\n') >= 0) {
            throw new IllegalArgumentException("merge operator name must be a single line");
        }
        if (version < 0) {
            throw new IllegalArgumentException("merge operator version must be >= 0");
        }
        var requiredArtifactHash = requireArgument(artifactHash, "artifactHash");
        if (requiredArtifactHash.isEmpty()) {
            throw new IllegalArgumentException("merge operator artifact hash must not be empty");
        }
        artifactHash = Buf.wrap(requiredArtifactHash.toByteArray()).freeze();
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
