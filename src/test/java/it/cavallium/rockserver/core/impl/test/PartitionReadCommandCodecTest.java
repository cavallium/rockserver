package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommandCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommandCodec;
import it.cavallium.rockserver.core.common.Keys;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32C;

import org.junit.jupiter.api.Test;

class PartitionReadCommandCodecTest {

    @Test
    void readCommandsRejectNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("keys must not be null", () -> new PartitionPointReadCommand(3, 4, null));
        assertIllegalArgument("keys[0] must not be null",
                () -> new PartitionPointReadCommand(3, 4, new Keys((Buf) null)));
        assertIllegalArgument("keys array must not be null",
                () -> new PartitionPointReadCommand(3, 4, new Keys((Buf[]) null)));
        assertIllegalArgument("keys[0] must not be null",
                () -> new PartitionRangeReadCommand(3, 4, new Keys((Buf) null), null, false, 10));
        assertIllegalArgument("keys[0] must not be null",
                () -> new PartitionRangeReadCommand(3, 4, null, new Keys((Buf) null), false, 10));
        assertIllegalArgument("keys array must not be null",
                () -> new PartitionRangeReadCommand(3, 4, new Keys((Buf[]) null), null, false, 10));
        assertIllegalArgument("command must not be null", () -> PartitionPointReadCommandCodec.encode(null));
        assertIllegalArgument("payload must not be null", () -> PartitionPointReadCommandCodec.decode(null));
        assertIllegalArgument("command must not be null", () -> PartitionRangeReadCommandCodec.encode(null));
        assertIllegalArgument("payload must not be null", () -> PartitionRangeReadCommandCodec.decode(null));
    }

    @Test
    void pointReadRejectsOversizedDecodedKeyPartLengthBeforeAllocation() {
        var payload = wrapWithChecksum(pointReadBodyWithDeclaredFirstKeyPartLength(Integer.MAX_VALUE));

        assertEquals("Invalid partition point read command byte length: " + Integer.MAX_VALUE,
                assertThrows(IllegalArgumentException.class, () -> PartitionPointReadCommandCodec.decode(payload))
                        .getMessage());
    }

    @Test
    void rangeReadRejectsOversizedDecodedKeyPartLengthBeforeAllocation() {
        var payload = wrapWithChecksum(rangeReadBodyWithDeclaredFirstStartKeyPartLength(Integer.MAX_VALUE));

        assertEquals("Invalid partition range read command byte length: " + Integer.MAX_VALUE,
                assertThrows(IllegalArgumentException.class, () -> PartitionRangeReadCommandCodec.decode(payload))
                        .getMessage());
    }

    @Test
    void readsRejectWithinLimitDecodedKeyPartLengthsBeyondRemainingPayloadBeforeAllocation() {
        var pointPayload = wrapWithChecksum(pointReadBodyWithDeclaredFirstKeyPartLength(64 * 1024 * 1024));
        var rangePayload = wrapWithChecksum(rangeReadBodyWithDeclaredFirstStartKeyPartLength(64 * 1024 * 1024));

        assertEquals("Unexpected end of partition point read command payload",
                assertThrows(IllegalArgumentException.class, () -> PartitionPointReadCommandCodec.decode(pointPayload))
                        .getMessage());
        assertEquals("Unexpected end of partition range read command payload",
                assertThrows(IllegalArgumentException.class, () -> PartitionRangeReadCommandCodec.decode(rangePayload))
                        .getMessage());
    }

    @Test
    void rangeReadRejectsMaxEntriesAboveCommandLimit() {
        var failure = assertThrows(IllegalArgumentException.class, () -> new PartitionRangeReadCommand(3,
                4,
                new Keys(buf("start")),
                null,
                false,
                PartitionRangeReadCommand.MAX_ENTRIES + 1));

        assertEquals("maxEntries must be <= " + PartitionRangeReadCommand.MAX_ENTRIES, failure.getMessage());
    }

    @Test
    void rangeReadRejectsDecodedMaxEntriesAboveCommandLimit() {
        var payload = wrapWithChecksum(rangeReadBodyWithMaxEntries(PartitionRangeReadCommand.MAX_ENTRIES + 1));

        var failure = assertThrows(IllegalArgumentException.class,
                () -> PartitionRangeReadCommandCodec.decode(payload));

        assertEquals("maxEntries must be <= " + PartitionRangeReadCommand.MAX_ENTRIES, failure.getMessage());
    }

    private static byte[] pointReadBodyWithDeclaredFirstKeyPartLength(int length) {
        var encoded = PartitionPointReadCommandCodec.encode(
                new PartitionPointReadCommand(3, 4, new Keys(buf("key")))
        );
        var prefixLength = 2 * Integer.BYTES + 2 * Long.BYTES + Integer.BYTES;
        return bodyWithDeclaredLength(encoded, prefixLength, length);
    }

    private static byte[] rangeReadBodyWithDeclaredFirstStartKeyPartLength(int length) {
        var encoded = PartitionRangeReadCommandCodec.encode(
                new PartitionRangeReadCommand(3, 4, new Keys(buf("start")), null, false, 10)
        );
        var prefixLength = 2 * Integer.BYTES + 2 * Long.BYTES + Byte.BYTES + Integer.BYTES;
        return bodyWithDeclaredLength(encoded, prefixLength, length);
    }

    private static byte[] rangeReadBodyWithMaxEntries(int maxEntries) {
        var encoded = PartitionRangeReadCommandCodec.encode(
                new PartitionRangeReadCommand(3, 4, new Keys(buf("start")), null, false, 10)
        );
        var buffer = ByteBuffer.wrap(encoded);
        int bodyLength = buffer.getInt();
        byte[] body = new byte[bodyLength];
        buffer.get(body);
        ByteBuffer.wrap(body).putInt(body.length - Integer.BYTES, maxEntries);
        return body;
    }

    private static byte[] bodyWithDeclaredLength(byte[] encoded, int prefixLength, int length) {
        var buffer = ByteBuffer.wrap(encoded);
        int bodyLength = buffer.getInt();
        byte[] body = new byte[bodyLength];
        buffer.get(body);
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

    private static Buf buf(String value) {
        return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }
}
