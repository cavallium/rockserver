package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseCodec;
import it.cavallium.rockserver.core.common.Delta;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;

import org.junit.jupiter.api.Test;

class PartitionWriteResponseCodecTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("responses must not be null",
                () -> PartitionWriteResponseCodec.encodeResponses(null));
        assertIllegalArgument("payload must not be null",
                () -> PartitionWriteResponseCodec.decodeResponses(null));
    }

    @Test
    void roundTripsResponseValuesForDedupeStorage() {
        var responses = new ArrayList<Object>();
        responses.add(buf("previous"));
        responses.add(Boolean.TRUE);
        responses.add(new Delta<>(buf("old"), buf("new")));
        responses.add(null);

        var decoded = PartitionWriteResponseCodec.decodeResponses(
                PartitionWriteResponseCodec.encodeResponses(responses));

        assertEquals(4, decoded.size());
        assertArrayEquals(buf("previous").toByteArray(), assertInstanceOf(Buf.class, decoded.getFirst()).toByteArray());
        assertEquals(Boolean.TRUE, decoded.get(1));
        var delta = assertInstanceOf(Delta.class, decoded.get(2));
        assertArrayEquals(buf("old").toByteArray(), assertInstanceOf(Buf.class, delta.previous()).toByteArray());
        assertArrayEquals(buf("new").toByteArray(), assertInstanceOf(Buf.class, delta.current()).toByteArray());
        assertNull(decoded.get(3));
    }

    @Test
    void rejectsUnsupportedResponseType() {
        assertThrows(IllegalArgumentException.class,
                () -> PartitionWriteResponseCodec.encodeResponses(List.of("unsupported")));
    }

    @Test
    void rejectsChecksumMismatch() {
        var payload = PartitionWriteResponseCodec.encodeResponses(List.of(buf("value")));
        payload[payload.length - 1] ^= 1;

        assertThrows(IllegalArgumentException.class, () -> PartitionWriteResponseCodec.decodeResponses(payload));
    }

    @Test
    void rejectsTrailingBytes() {
        var payload = PartitionWriteResponseCodec.encodeResponses(List.of(Boolean.FALSE));
        var payloadWithTrailingByte = Arrays.copyOf(payload, payload.length + 1);

        assertThrows(IllegalArgumentException.class,
                () -> PartitionWriteResponseCodec.decodeResponses(payloadWithTrailingByte));
    }

    @Test
    void rejectsOversizedDecodedBufferLengthBeforeAllocation() {
        var payload = wrapWithChecksum(bodyWithDeclaredFirstBufferLength(Integer.MAX_VALUE));

        assertEquals("Invalid partition write response byte length: " + Integer.MAX_VALUE,
                assertThrows(IllegalArgumentException.class, () -> PartitionWriteResponseCodec.decodeResponses(payload))
                        .getMessage());
    }

    @Test
    void rejectsWithinLimitDecodedBufferLengthBeyondRemainingPayloadBeforeAllocation() {
        var payload = wrapWithChecksum(bodyWithDeclaredFirstBufferLength(64 * 1024 * 1024));

        assertEquals("Unexpected end of partition write response payload",
                assertThrows(IllegalArgumentException.class, () -> PartitionWriteResponseCodec.decodeResponses(payload))
                        .getMessage());
    }

    @Test
    void rejectsResponseCountThatCannotFitRemainingPayloadBeforePreallocation() {
        var body = ByteBuffer.allocate(3 * Integer.BYTES)
                .putInt(0x52535052)
                .putInt(1)
                .putInt(1_000_000)
                .array();

        assertEquals("Invalid partition write response count for remaining payload: 1000000",
                assertThrows(IllegalArgumentException.class,
                        () -> PartitionWriteResponseCodec.decodeResponses(wrapWithChecksum(body)))
                        .getMessage());
    }

    @Test
    void rejectsResponseCountAboveCallerLimitBeforePreallocation() {
        var body = ByteBuffer.allocate(3 * Integer.BYTES)
                .putInt(0x52535052)
                .putInt(1)
                .putInt(2)
                .array();

        assertEquals("Partition write response count 2 exceeds limit 1",
                assertThrows(IllegalArgumentException.class,
                        () -> PartitionWriteResponseCodec.decodeResponses(wrapWithChecksum(body), 1))
                        .getMessage());
    }

    @Test
    void rejectsDeeplyNestedDeltaResponses() {
        Object response = null;
        for (int depth = 0; depth < 65; depth++) {
            response = new Delta<>(response, null);
        }
        var deeplyNestedResponse = response;

        assertEquals("Partition write response nesting depth exceeds limit 64",
                assertThrows(IllegalArgumentException.class,
                        () -> PartitionWriteResponseCodec.encodeResponses(List.of(deeplyNestedResponse)))
                        .getMessage());
        assertEquals("Partition write response nesting depth exceeds limit 64",
                assertThrows(IllegalArgumentException.class,
                        () -> PartitionWriteResponseCodec.decodeResponses(
                                wrapWithChecksum(bodyWithNestedDeltaDepth(65))))
                        .getMessage());
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        assertEquals(message, assertThrows(IllegalArgumentException.class, operation::run).getMessage());
    }

    private static byte[] bodyWithDeclaredFirstBufferLength(int length) {
        var encoded = PartitionWriteResponseCodec.encodeResponses(List.of(buf("value")));
        var buffer = ByteBuffer.wrap(encoded);
        int bodyLength = buffer.getInt();
        byte[] body = new byte[bodyLength];
        buffer.get(body);
        var prefixLength = 3 * Integer.BYTES + Byte.BYTES;
        var prefix = Arrays.copyOf(body, prefixLength);
        var malformedBody = ByteBuffer.allocate(prefix.length + Integer.BYTES);
        malformedBody.put(prefix);
        malformedBody.putInt(length);
        return malformedBody.array();
    }

    private static byte[] bodyWithNestedDeltaDepth(int nestingDepth) {
        var body = ByteBuffer.allocate(3 * Integer.BYTES + nestingDepth);
        body.putInt(0x52535052);
        body.putInt(1);
        body.putInt(1);
        for (int depth = 0; depth < nestingDepth; depth++) {
            body.put((byte) 3);
        }
        return body.array();
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
}
