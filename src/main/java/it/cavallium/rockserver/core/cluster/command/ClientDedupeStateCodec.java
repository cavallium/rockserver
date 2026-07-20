package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.rockserver.core.cluster.ClientId;

import java.nio.ByteBuffer;
import java.util.UUID;

public final class ClientDedupeStateCodec {

    public static final int FORMAT_VERSION = 1;

    private static final int ENCODED_SIZE = Integer.BYTES + 5 * Long.BYTES;

    private ClientDedupeStateCodec() {
    }

    public static byte[] encode(ClientDedupeState state) {
        var requiredState = requireArgument(state, "state");
        var clientUuid = requiredState.clientId().value();
        return ByteBuffer.allocate(ENCODED_SIZE)
                .putInt(FORMAT_VERSION)
                .putLong(clientUuid.getMostSignificantBits())
                .putLong(clientUuid.getLeastSignificantBits())
                .putLong(requiredState.leaseEpoch())
                .putLong(requiredState.acknowledgedSequence())
                .putLong(requiredState.leaseExpiresAtAppliedIndex())
                .array();
    }

    public static ClientDedupeState decode(byte[] payload) {
        var requiredPayload = requireArgument(payload, "payload");
        if (requiredPayload.length != ENCODED_SIZE) {
            throw new IllegalArgumentException("Invalid client dedupe state payload length: " + requiredPayload.length);
        }
        var buffer = ByteBuffer.wrap(requiredPayload);
        var version = buffer.getInt();
        if (version != FORMAT_VERSION) {
            throw new IllegalArgumentException("Unsupported client dedupe state version: " + version);
        }
        var clientId = new ClientId(new UUID(buffer.getLong(), buffer.getLong()));
        return new ClientDedupeState(
                clientId,
                buffer.getLong(),
                buffer.getLong(),
                buffer.getLong()
        );
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
