package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.command.ClientDedupeState;
import it.cavallium.rockserver.core.cluster.command.ClientDedupeStateCodec;

import java.util.UUID;

import org.junit.jupiter.api.Test;

class ClientDedupeStateCodecTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var requestId = new ClientRequestId(new ClientId(new UUID(0, 1)), 17);

        assertIllegalArgument("clientId must not be null",
                () -> new ClientDedupeState(null, 0, 0, 0));
        assertIllegalArgument("requestId must not be null",
                () -> ClientDedupeState.acknowledged(null));
        assertIllegalArgument("requestId must not be null",
                () -> ClientDedupeState.acknowledged(null, 1, 2));
        assertIllegalArgument("state must not be null",
                () -> ClientDedupeStateCodec.encode(null));
        assertIllegalArgument("payload must not be null",
                () -> ClientDedupeStateCodec.decode(null));
        assertEquals(requestId, ClientDedupeState.acknowledged(requestId).acknowledgedWatermark());
    }

    @Test
    void roundTripsClientLeaseAndAcknowledgedWatermark() {
        var clientId = new ClientId(new UUID(0, 1));
        var state = new ClientDedupeState(clientId, 5, 17, 99);

        var decoded = ClientDedupeStateCodec.decode(ClientDedupeStateCodec.encode(state));

        assertEquals(state, decoded);
        assertEquals(new ClientRequestId(clientId, 17), decoded.acknowledgedWatermark());
    }

    @Test
    void buildsAcknowledgedStateFromRequestId() {
        var requestId = new ClientRequestId(new ClientId(new UUID(0, 1)), 17);

        var state = ClientDedupeState.acknowledged(requestId);

        assertEquals(requestId.clientId(), state.clientId());
        assertEquals(0, state.leaseEpoch());
        assertEquals(17, state.acknowledgedSequence());
        assertEquals(ClientDedupeState.NO_EXPIRY_APPLIED_INDEX, state.leaseExpiresAtAppliedIndex());
    }

    @Test
    void rejectsMalformedPayloads() {
        assertThrows(IllegalArgumentException.class, () -> ClientDedupeStateCodec.decode(new byte[1]));

        var payload = ClientDedupeStateCodec.encode(new ClientDedupeState(new ClientId(new UUID(0, 1)), 5, 17, 99));
        payload[3] = 2;

        assertThrows(IllegalArgumentException.class, () -> ClientDedupeStateCodec.decode(payload));
    }

    @Test
    void rejectsNegativeLeaseOrWatermarkFields() {
        var clientId = new ClientId(new UUID(0, 1));

        assertThrows(IllegalArgumentException.class, () -> new ClientDedupeState(clientId, -1, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new ClientDedupeState(clientId, 0, -1, 0));
        assertThrows(IllegalArgumentException.class, () -> new ClientDedupeState(clientId, 0, 0, -1));
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        assertEquals(message, assertThrows(IllegalArgumentException.class, operation::run).getMessage());
    }
}
