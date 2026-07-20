package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutation;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationCodec;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationOp;
import it.cavallium.rockserver.core.common.Keys;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32C;

import org.junit.jupiter.api.Test;

class ReplicatedMutationCodecTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("operation must not be null",
                () -> new ReplicatedMutation(null, 1, new Keys(buf("k")), null));
        assertIllegalArgument("keys must not be null",
                () -> new ReplicatedMutation(ReplicatedMutationOp.PUT, 1, null, buf("v")));
        assertIllegalArgument("keys[0] must not be null",
                () -> ReplicatedMutation.put(1, new Keys((Buf) null), buf("v")));
        assertIllegalArgument("keys array must not be null",
                () -> ReplicatedMutation.put(1, new Keys((Buf[]) null), buf("v")));
        assertIllegalArgument("newCalculatedKey must not be null",
                () -> ReplicatedMutation.put(1, new Keys(buf("k")), buf("v")).withCalculatedKey(null));
        assertIllegalArgument("mutation must not be null",
                () -> ReplicatedMutationCodec.encode(null));
        assertIllegalArgument("mutations must not be null",
                () -> ReplicatedMutationCodec.encodeBatch(null));
        assertIllegalArgument("mutation must not be null",
                () -> ReplicatedMutationCodec.encodeBatch(Collections.singletonList(null)));
        assertIllegalArgument("payload must not be null",
                () -> ReplicatedMutationCodec.decode(null));
        assertIllegalArgument("payload must not be null",
                () -> ReplicatedMutationCodec.decodeBatch(null));
    }

    @Test
    void roundTripsPutWithCompositeKey() {
        var mutation = ReplicatedMutation.put(
                42,
                new Keys(buf("account"), buf("user-1")),
                buf("value")
        );

        var decoded = ReplicatedMutationCodec.decode(ReplicatedMutationCodec.encode(mutation));

        assertMutationEquals(mutation, decoded);
    }

    @Test
    void roundTripsDeleteWithoutValue() {
        var mutation = ReplicatedMutation.delete(7, new Keys(buf("user-2")));

        var decoded = ReplicatedMutationCodec.decode(ReplicatedMutationCodec.encode(mutation));

        assertEquals(ReplicatedMutationOp.DELETE, decoded.operation());
        assertMutationEquals(mutation, decoded);
    }

    @Test
    void roundTripsMergePayload() {
        var mutation = ReplicatedMutation.merge(9, new Keys(buf("counter")), buf("delta"));

        var decoded = ReplicatedMutationCodec.decode(ReplicatedMutationCodec.encode(mutation));

        assertMutationEquals(mutation, decoded);
    }

    @Test
    void roundTripsSealedCalculatedKey() {
        var mutation = ReplicatedMutation.put(42, new Keys(buf("logical-key")), buf("value"))
                .withCalculatedKey(buf("calculated-key"));

        var decoded = ReplicatedMutationCodec.decode(ReplicatedMutationCodec.encode(mutation));

        assertMutationEquals(mutation, decoded);
    }

    @Test
    void decodesLegacyVersionOneMutationWithoutCalculatedKey() {
        var decoded = ReplicatedMutationCodec.decode(legacyVersionOnePutPayload());

        assertEquals(ReplicatedMutationOp.PUT, decoded.operation());
        assertNull(decoded.calculatedKey());
        assertArrayEquals(buf("value").toByteArray(), decoded.requiredValue().toByteArray());
    }

    @Test
    void roundTripsMutationBatch() {
        var mutations = List.of(
                ReplicatedMutation.put(42, new Keys(buf("account"), buf("user-1")), buf("value")),
                ReplicatedMutation.delete(7, new Keys(buf("user-2"))),
                ReplicatedMutation.merge(9, new Keys(buf("counter")), buf("delta"))
        );

        var decoded = ReplicatedMutationCodec.decodeBatch(ReplicatedMutationCodec.encodeBatch(mutations));

        assertEquals(mutations.size(), decoded.size());
        for (int i = 0; i < mutations.size(); i++) {
            assertMutationEquals(mutations.get(i), decoded.get(i));
        }
    }

    @Test
    void decodeBatchAcceptsSingleMutationPayload() {
        var mutation = ReplicatedMutation.put(1, new Keys(buf("k")), buf("v"));

        var decoded = ReplicatedMutationCodec.decodeBatch(ReplicatedMutationCodec.encode(mutation));

        assertEquals(1, decoded.size());
        assertMutationEquals(mutation, decoded.getFirst());
    }

    @Test
    void singleMutationDecodeRejectsMultiMutationBatch() {
        var mutations = List.of(
                ReplicatedMutation.put(1, new Keys(buf("k1")), buf("v1")),
                ReplicatedMutation.put(1, new Keys(buf("k2")), buf("v2"))
        );

        assertThrows(IllegalArgumentException.class,
                () -> ReplicatedMutationCodec.decode(ReplicatedMutationCodec.encodeBatch(mutations)));
    }

    @Test
    void rejectsChecksumMismatch() {
        var payload = ReplicatedMutationCodec.encode(ReplicatedMutation.put(1, new Keys(buf("k")), buf("v")));
        payload[payload.length - 1] ^= 1;

        assertThrows(IllegalArgumentException.class, () -> ReplicatedMutationCodec.decode(payload));
    }

    @Test
    void rejectsTrailingBytes() {
        var payload = ReplicatedMutationCodec.encode(ReplicatedMutation.put(1, new Keys(buf("k")), buf("v")));
        var payloadWithTrailingByte = Arrays.copyOf(payload, payload.length + 1);

        assertThrows(IllegalArgumentException.class, () -> ReplicatedMutationCodec.decode(payloadWithTrailingByte));
    }

    @Test
    void rejectsUnsupportedFutureMutationVersion() {
        var payload = payloadWithMutationVersion(3);

        assertEquals("Unsupported replicated mutation version: 3",
                assertThrows(IllegalArgumentException.class, () -> ReplicatedMutationCodec.decode(payload))
                        .getMessage());
    }

    @Test
    void rejectsUnsupportedLegacyMutationVersion() {
        var payload = payloadWithMutationVersion(0);

        assertEquals("Unsupported replicated mutation version: 0",
                assertThrows(IllegalArgumentException.class, () -> ReplicatedMutationCodec.decode(payload))
                        .getMessage());
    }

    @Test
    void rejectsOversizedDecodedByteLengthBeforeAllocation() {
        var payload = wrapWithChecksum(bodyWithDeclaredFirstKeyPartLength(Integer.MAX_VALUE));

        assertEquals("Invalid replicated mutation byte length: " + Integer.MAX_VALUE,
                assertThrows(IllegalArgumentException.class, () -> ReplicatedMutationCodec.decode(payload))
                        .getMessage());
    }

    @Test
    void rejectsWithinLimitDecodedByteLengthBeyondRemainingPayloadBeforeAllocation() {
        var payload = wrapWithChecksum(bodyWithDeclaredFirstKeyPartLength(64 * 1024 * 1024));

        assertEquals("Unexpected end of replicated mutation payload",
                assertThrows(IllegalArgumentException.class, () -> ReplicatedMutationCodec.decode(payload))
                        .getMessage());
    }

    @Test
    void rejectsBatchCountThatCannotFitRemainingPayloadBeforePreallocation() {
        var body = ByteBuffer.allocate(3 * Integer.BYTES)
                .putInt(0x52534D42)
                .putInt(1)
                .putInt(1_000_000)
                .array();

        assertEquals("Invalid replicated mutation batch size for remaining payload: 1000000",
                assertThrows(IllegalArgumentException.class,
                        () -> ReplicatedMutationCodec.decodeBatch(wrapWithChecksum(body)))
                        .getMessage());
    }

    @Test
    void rejectsBatchCountAboveCallerLimitBeforePreallocation() {
        var body = ByteBuffer.allocate(3 * Integer.BYTES)
                .putInt(0x52534D42)
                .putInt(2)
                .putInt(2)
                .array();

        assertEquals("Replicated mutation batch size 2 exceeds limit 1",
                assertThrows(IllegalArgumentException.class,
                        () -> ReplicatedMutationCodec.decodeBatch(wrapWithChecksum(body), 1))
                        .getMessage());
    }

    @Test
    void rejectsEmptyMutationBatch() {
        assertThrows(IllegalArgumentException.class, () -> ReplicatedMutationCodec.encodeBatch(List.of()));
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        assertEquals(message, assertThrows(IllegalArgumentException.class, operation::run).getMessage());
    }

    private static void assertMutationEquals(ReplicatedMutation expected, ReplicatedMutation actual) {
        assertEquals(expected.operation(), actual.operation());
        assertEquals(expected.columnId(), actual.columnId());
        assertEquals(expected.keys(), actual.keys());
        if (expected.calculatedKey() == null) {
            assertNull(actual.calculatedKey());
        } else {
            assertArrayEquals(expected.calculatedKey().toByteArray(), actual.calculatedKey().toByteArray());
        }
        if (expected.value() == null) {
            assertNull(actual.value());
        } else {
            assertArrayEquals(expected.value().toByteArray(), actual.value().toByteArray());
        }
    }

    private static byte[] payloadWithMutationVersion(int version) {
        var encoded = ReplicatedMutationCodec.encode(ReplicatedMutation.put(1, new Keys(buf("k")), buf("v")));
        var buffer = ByteBuffer.wrap(encoded);
        int bodyLength = buffer.getInt();
        byte[] body = new byte[bodyLength];
        buffer.get(body);
        ByteBuffer.wrap(body).putInt(Integer.BYTES, version);
        return wrapWithChecksum(body);
    }

    private static byte[] legacyVersionOnePutPayload() {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(0x52534D31);
            out.writeInt(1);
            out.writeByte(1);
            out.writeLong(42);
            out.writeInt(1);
            writeBytes(out, buf("key").toByteArray());
            out.writeBoolean(true);
            writeBytes(out, buf("value").toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return wrapWithChecksum(bytes.toByteArray());
    }

    private static byte[] bodyWithDeclaredFirstKeyPartLength(int length) {
        var encoded = ReplicatedMutationCodec.encode(ReplicatedMutation.put(1, new Keys(buf("k")), buf("v")));
        var buffer = ByteBuffer.wrap(encoded);
        int bodyLength = buffer.getInt();
        byte[] body = new byte[bodyLength];
        buffer.get(body);
        var prefixLength = 2 * Integer.BYTES + Byte.BYTES + Long.BYTES + Integer.BYTES;
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

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static Buf buf(String value) {
        return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
    }
}
