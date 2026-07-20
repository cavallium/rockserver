package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcEvent;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcEventCodec;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcToken;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationOp;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32C;

import org.junit.jupiter.api.Test;

class ReplicatedCdcEventCodecTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var token = new ReplicatedCdcToken(7, new ShardId(3), 5, 11, 0);

        assertIllegalArgument("partitionId must not be null",
                () -> new ReplicatedCdcToken(7, null, 5, 11, 0));
        assertIllegalArgument("token must not be null",
                () -> new ReplicatedCdcEvent(null, 2, 4, buf("key"), buf("value"), ReplicatedMutationOp.PUT));
        assertIllegalArgument("key must not be null",
                () -> new ReplicatedCdcEvent(token, 2, 4, null, buf("value"), ReplicatedMutationOp.PUT));
        assertIllegalArgument("operation must not be null",
                () -> new ReplicatedCdcEvent(token, 2, 4, buf("key"), buf("value"), null));
        assertIllegalArgument("event must not be null",
                () -> ReplicatedCdcEventCodec.encode(null));
        assertIllegalArgument("payload must not be null",
                () -> ReplicatedCdcEventCodec.decode(null));
    }

    @Test
    void roundTripsPutEvent() {
        var event = event(ReplicatedMutationOp.PUT, "key", "value");

        var decoded = ReplicatedCdcEventCodec.decode(ReplicatedCdcEventCodec.encode(event));

        assertEventEquals(event, decoded);
    }

    @Test
    void roundTripsDeleteEventWithoutValue() {
        var event = event(ReplicatedMutationOp.DELETE, "key", null);

        var decoded = ReplicatedCdcEventCodec.decode(ReplicatedCdcEventCodec.encode(event));

        assertEventEquals(event, decoded);
    }

    @Test
    void rejectsOversizedDecodedKeyLengthBeforeAllocation() {
        var payload = wrapWithChecksum(bodyWithDeclaredKeyLength(Integer.MAX_VALUE));

        assertEquals("Invalid replicated CDC event byte length: " + Integer.MAX_VALUE,
                assertThrows(IllegalArgumentException.class, () -> ReplicatedCdcEventCodec.decode(payload))
                        .getMessage());
    }

    @Test
    void rejectsWithinLimitDecodedKeyLengthBeyondRemainingPayloadBeforeAllocation() {
        var payload = wrapWithChecksum(bodyWithDeclaredKeyLength(64 * 1024 * 1024));

        assertEquals("Unexpected end of replicated CDC event payload",
                assertThrows(IllegalArgumentException.class, () -> ReplicatedCdcEventCodec.decode(payload))
                        .getMessage());
    }

    private static void assertEventEquals(ReplicatedCdcEvent expected, ReplicatedCdcEvent actual) {
        assertEquals(expected.token(), actual.token());
        assertEquals(expected.schemaEpoch(), actual.schemaEpoch());
        assertEquals(expected.columnId(), actual.columnId());
        assertEquals(expected.operation(), actual.operation());
        assertArrayEquals(expected.key().toByteArray(), actual.key().toByteArray());
        if (expected.value() == null) {
            assertNull(actual.value());
        } else {
            assertArrayEquals(expected.value().toByteArray(), actual.value().toByteArray());
        }
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        assertEquals(message, assertThrows(IllegalArgumentException.class, operation::run).getMessage());
    }

    private static byte[] bodyWithDeclaredKeyLength(int length) {
        var encoded = ReplicatedCdcEventCodec.encode(event(ReplicatedMutationOp.PUT, "key", "value"));
        var buffer = ByteBuffer.wrap(encoded);
        int bodyLength = buffer.getInt();
        byte[] body = new byte[bodyLength];
        buffer.get(body);
        var prefixLength = 2 * Integer.BYTES
                + 4 * Long.BYTES
                + Integer.BYTES
                + 2 * Long.BYTES
                + Integer.BYTES;
        var prefix = Arrays.copyOf(body, prefixLength);
        var malformedBody = ByteBuffer.allocate(prefix.length + Integer.BYTES);
        malformedBody.put(prefix);
        malformedBody.putInt(length);
        return malformedBody.array();
    }

    private static byte[] wrapWithChecksum(byte[] body) {
        var crc = new CRC32C();
        crc.update(body, 0, body.length);
        var payload = ByteBuffer.allocate(Integer.BYTES + body.length + Long.BYTES);
        payload.putInt(body.length);
        payload.put(body);
        payload.putLong(crc.getValue());
        return payload.array();
    }

    private static ReplicatedCdcEvent event(ReplicatedMutationOp operation, String key, String value) {
        return new ReplicatedCdcEvent(
                new ReplicatedCdcToken(7, new ShardId(3), 5, 11, 0),
                2,
                4,
                buf(key),
                value == null ? null : buf(value),
                operation
        );
    }

    private static Buf buf(String value) {
        return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
    }
}
