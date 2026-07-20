package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;

public record ClientDedupeState(ClientId clientId,
                                long leaseEpoch,
                                long acknowledgedSequence,
                                long leaseExpiresAtAppliedIndex) {

    public static final long NO_EXPIRY_APPLIED_INDEX = Long.MAX_VALUE;

    public ClientDedupeState {
        requireArgument(clientId, "clientId");
        requireNonNegative(leaseEpoch, "leaseEpoch");
        requireNonNegative(acknowledgedSequence, "acknowledgedSequence");
        requireNonNegative(leaseExpiresAtAppliedIndex, "leaseExpiresAtAppliedIndex");
    }

    public ClientRequestId acknowledgedWatermark() {
        return new ClientRequestId(clientId, acknowledgedSequence);
    }

    public static ClientDedupeState acknowledged(ClientRequestId requestId) {
        return acknowledged(requestId, 0, NO_EXPIRY_APPLIED_INDEX);
    }

    public static ClientDedupeState acknowledged(ClientRequestId requestId,
                                                 long leaseEpoch,
                                                 long leaseExpiresAtAppliedIndex) {
        var requiredRequestId = requireArgument(requestId, "requestId");
        return new ClientDedupeState(
                requiredRequestId.clientId(),
                leaseEpoch,
                requiredRequestId.sequence(),
                leaseExpiresAtAppliedIndex
        );
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
