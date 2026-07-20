package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadResponseCodec;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32C;

import org.junit.jupiter.api.Test;

class PartitionRangeReadResponseCodecTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("entries must not be null",
                () -> PartitionRangeReadResponseCodec.encodeEntries(null));
        assertIllegalArgument("payload must not be null",
                () -> PartitionRangeReadResponseCodec.decodeEntries(null));
        assertIllegalArgument("entry must not be null",
                () -> PartitionRangeReadResponseCodec.encodeEntries(Collections.singletonList(null)));
        assertIllegalArgument("keys must not be null",
                () -> PartitionRangeReadResponseCodec.encodeEntries(List.of(new KV(null, null))));
        assertIllegalArgument("keyParts must not be null",
                () -> PartitionRangeReadResponseCodec.encodeEntries(List.of(new KV(new Keys((Buf[]) null), null))));
        assertIllegalArgument("keyParts[0] must not be null",
                () -> PartitionRangeReadResponseCodec.encodeEntries(List.of(new KV(new Keys((Buf) null), null))));
    }

    @Test
    void roundTripsRangeEntries() {
        var entries = List.of(
                kv("a", "left"),
                new KV(new Keys(buf("b")), null),
                kv("c", "")
        );

        var decoded = PartitionRangeReadResponseCodec.decodeEntries(
                PartitionRangeReadResponseCodec.encodeEntries(entries));

        assertEquals(3, decoded.size());
        assertEquals(new Keys(buf("a")), decoded.getFirst().keys());
        assertArrayEquals(bytes("left"), decoded.getFirst().value().toByteArray());
        assertEquals(new Keys(buf("b")), decoded.get(1).keys());
        assertNull(decoded.get(1).value());
        assertEquals(new Keys(buf("c")), decoded.get(2).keys());
        assertArrayEquals(new byte[0], decoded.get(2).value().toByteArray());
    }

    @Test
    void rejectsChecksumMismatch() {
        var payload = PartitionRangeReadResponseCodec.encodeEntries(List.of(kv("a", "left")));
        payload[payload.length - 1] ^= 1;

        assertThrows(IllegalArgumentException.class, () -> PartitionRangeReadResponseCodec.decodeEntries(payload));
    }

    @Test
    void rejectsTrailingBytes() {
        var payload = PartitionRangeReadResponseCodec.encodeEntries(List.of(kv("a", "left")));
        var payloadWithTrailingByte = Arrays.copyOf(payload, payload.length + 1);

        assertThrows(IllegalArgumentException.class,
                () -> PartitionRangeReadResponseCodec.decodeEntries(payloadWithTrailingByte));
    }

    @Test
    void rejectsOversizedDecodedFieldBeforeAllocation() {
        var payload = wrapWithChecksum(bodyWithDeclaredFirstKeyLength(Integer.MAX_VALUE));

        assertEquals("Invalid partition range read response byte length: " + Integer.MAX_VALUE,
                assertThrows(IllegalArgumentException.class,
                        () -> PartitionRangeReadResponseCodec.decodeEntries(payload)).getMessage());
    }

    @Test
    void rejectsEntryCountAboveCallerLimitBeforePreallocation() {
        var body = ByteBuffer.allocate(3 * Integer.BYTES)
                .putInt(0x52535250)
                .putInt(1)
                .putInt(2)
                .array();

        assertEquals("Partition range read response entry count 2 exceeds limit 1",
                assertThrows(IllegalArgumentException.class,
                        () -> PartitionRangeReadResponseCodec.decodeEntries(wrapWithChecksum(body), 1))
                        .getMessage());
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        assertEquals(message, assertThrows(IllegalArgumentException.class, operation::run).getMessage());
    }

    private static byte[] bodyWithDeclaredFirstKeyLength(int length) {
        var encoded = PartitionRangeReadResponseCodec.encodeEntries(List.of(kv("a", "left")));
        var buffer = ByteBuffer.wrap(encoded);
        int bodyLength = buffer.getInt();
        byte[] body = new byte[bodyLength];
        buffer.get(body);
        var prefixLength = 3 * Integer.BYTES + Integer.BYTES;
        var malformedBody = ByteBuffer.allocate(prefixLength + Integer.BYTES);
        malformedBody.put(Arrays.copyOf(body, prefixLength));
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

    private static KV kv(String key, String value) {
        return new KV(new Keys(buf(key)), buf(value));
    }

    private static Buf buf(String value) {
        return Buf.wrap(bytes(value)).freeze();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
