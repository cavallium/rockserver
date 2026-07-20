package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.client.PartitionRangeScanCursor;
import it.cavallium.rockserver.core.cluster.client.PartitionRangeScanCursorCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommandCodec;
import it.cavallium.rockserver.core.common.Keys;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32C;

import org.junit.jupiter.api.Test;

class PartitionRangeScanCursorCodecTest {

    @Test
    void roundTripsCursor() {
        var cursor = cursor();

        var decoded = PartitionRangeScanCursorCodec.decode(PartitionRangeScanCursorCodec.encode(cursor));

        assertEquals(cursor, decoded);
    }

    @Test
    void rejectsChecksumMismatch() {
        var payload = PartitionRangeScanCursorCodec.encode(cursor());
        payload[payload.length - 1] ^= 1;

        assertThrows(IllegalArgumentException.class, () -> PartitionRangeScanCursorCodec.decode(payload));
    }

    @Test
    void rejectsTrailingBytesAfterChecksum() {
        var payload = PartitionRangeScanCursorCodec.encode(cursor());
        var payloadWithTrailingByte = Arrays.copyOf(payload, payload.length + 1);

        assertThrows(IllegalArgumentException.class, () -> PartitionRangeScanCursorCodec.decode(payloadWithTrailingByte));
    }

    @Test
    void rejectsOversizedDecodedCommandLengthBeforeAllocation() {
        var payload = wrapWithChecksum(cursorBodyWithDeclaredCommandLength(Integer.MAX_VALUE));

        assertEquals("Invalid partition range scan cursor byte length: " + Integer.MAX_VALUE,
                assertThrows(IllegalArgumentException.class, () -> PartitionRangeScanCursorCodec.decode(payload))
                        .getMessage());
    }

    @Test
    void rejectsWithinLimitDecodedCommandLengthBeyondRemainingPayloadBeforeAllocation() {
        var payload = wrapWithChecksum(cursorBodyWithDeclaredCommandLength(64 * 1024 * 1024));

        assertEquals("Unexpected end of partition range scan cursor payload",
                assertThrows(IllegalArgumentException.class, () -> PartitionRangeScanCursorCodec.decode(payload))
                        .getMessage());
    }

    @Test
    void rejectsEmbeddedRangeCommandAboveMaxEntriesLimit() {
        var payload = wrapWithChecksum(cursorBodyWithCommandPayload(
                rangeReadPayloadWithMaxEntries(PartitionRangeReadCommand.MAX_ENTRIES + 1)));

        var failure = assertThrows(IllegalArgumentException.class, () -> PartitionRangeScanCursorCodec.decode(payload));

        assertEquals("maxEntries must be <= " + PartitionRangeReadCommand.MAX_ENTRIES, failure.getMessage());
    }

    private static byte[] cursorBodyWithDeclaredCommandLength(int length) {
        var encoded = PartitionRangeScanCursorCodec.encode(cursor());
        var buffer = ByteBuffer.wrap(encoded);
        int bodyLength = buffer.getInt();
        byte[] body = new byte[bodyLength];
        buffer.get(body);
        var prefixLength = 3 * Integer.BYTES + Long.BYTES;
        var prefix = Arrays.copyOf(body, prefixLength);
        var malformedBody = ByteBuffer.allocate(prefix.length + Integer.BYTES);
        malformedBody.put(prefix);
        malformedBody.putInt(length);
        return malformedBody.array();
    }

    private static byte[] cursorBodyWithCommandPayload(byte[] commandPayload) {
        return ByteBuffer.allocate(4 * Integer.BYTES + Long.BYTES + commandPayload.length)
                .putInt(0x52534352)
                .putInt(1)
                .putLong(7)
                .putInt(0)
                .putInt(commandPayload.length)
                .put(commandPayload)
                .array();
    }

    private static byte[] rangeReadPayloadWithMaxEntries(int maxEntries) {
        var encoded = PartitionRangeReadCommandCodec.encode(
                new PartitionRangeReadCommand(3, 4, new Keys(buf("start")), null, false, 10)
        );
        var buffer = ByteBuffer.wrap(encoded);
        int bodyLength = buffer.getInt();
        byte[] body = new byte[bodyLength];
        buffer.get(body);
        ByteBuffer.wrap(body).putInt(body.length - Integer.BYTES, maxEntries);
        return wrapWithChecksum(body);
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

    private static PartitionRangeScanCursor cursor() {
        return new PartitionRangeScanCursor(
                7,
                new PartitionRangeReadCommand(3, 4, new Keys(buf("start")), null, false, 10),
                0
        );
    }

    private static Buf buf(String value) {
        return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
    }
}
