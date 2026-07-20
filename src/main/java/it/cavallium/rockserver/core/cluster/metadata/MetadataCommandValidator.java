package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;

public final class MetadataCommandValidator {

    public static final int MAX_PROPOSAL_PAYLOAD_BYTES = 1024 * 1024;

    private MetadataCommandValidator() {
    }

    public static MetadataCommand decodeForProposal(byte[] payload) {
        return decodeBounded(payload, "Metadata proposal payload");
    }

    public static MetadataCommand decodeCommitted(byte[] payload) {
        return decodeBounded(payload, "Metadata committed payload");
    }

    private static MetadataCommand decodeBounded(byte[] payload, String description) {
        requireArgument(payload, "payload");
        if (payload.length > MAX_PROPOSAL_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(description + " exceeds "
                    + MAX_PROPOSAL_PAYLOAD_BYTES + " bytes: " + payload.length);
        }
        return MetadataCommandCodec.decode(payload);
    }

    public static ClusterMetadata projectForProposal(ClusterMetadata metadata, MetadataCommand command) {
        return MetadataCommandApplier.apply(
                requireArgument(metadata, "metadata"),
                requireArgument(command, "command")
        );
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
