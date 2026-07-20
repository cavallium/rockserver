package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.SerializedKVBatch;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class SerializedKVBatchTest {

    @Test
    void decodeReadsValidSingleEntryBatch() {
        var decoded = batch(payload(out -> {
            writeIntLE(out, 1);
            out.write(1);
            writeBytes(out, "key");
            writeBytes(out, "value");
        })).decode().toList();

        assertEquals(1, decoded.size());
        assertEquals(new Keys(buf("key")), decoded.getFirst().keys());
        assertArrayEquals(bytes("value"), decoded.getFirst().value().toByteArray());
    }

    @Test
    void decodeRejectsNegativeBatchSize() {
        var failure = assertThrows(IllegalArgumentException.class,
                () -> batch(payload(out -> writeIntLE(out, -1))).decode());

        assertEquals("Serialized KV batch size must not be negative: -1", failure.getMessage());
    }

    @Test
    void decodeRejectsBatchSizeThatCannotFitRemainingPayload() {
        var failure = assertThrows(IllegalArgumentException.class,
                () -> batch(payload(out -> writeIntLE(out, 1))).decode());

        assertEquals("Serialized KV batch size 1 exceeds remaining payload capacity: 0", failure.getMessage());
    }

    @Test
    void decodeRejectsKeyCountThatCannotFitRemainingPayload() {
        var failure = assertThrows(IllegalArgumentException.class,
                () -> batch(payload(out -> {
                    writeIntLE(out, 1);
                    out.write(2);
                    writeIntLE(out, 0);
                })).decode().toList());

        assertEquals("Serialized KV key count 2 exceeds remaining payload capacity: 1", failure.getMessage());
    }

    @Test
    void decodeRejectsNegativeKeyLength() {
        var failure = assertThrows(IllegalArgumentException.class,
                () -> batch(payload(out -> {
                    writeIntLE(out, 1);
                    out.write(1);
                    writeIntLE(out, -1);
                    writeIntLE(out, 0);
                })).decode().toList());

        assertEquals("Serialized KV key length must not be negative: -1", failure.getMessage());
    }

    @Test
    void decodeRejectsNegativeValueLength() {
        var failure = assertThrows(IllegalArgumentException.class,
                () -> batch(payload(out -> {
                    writeIntLE(out, 1);
                    out.write(0);
                    writeIntLE(out, -1);
                })).decode().toList());

        assertEquals("Serialized KV value length must not be negative: -1", failure.getMessage());
    }

    @Test
    void decodeRejectsTrailingBytesAfterDeclaredEntries() {
        var failure = assertThrows(IllegalArgumentException.class,
                () -> batch(payload(out -> {
                    writeIntLE(out, 0);
                    out.write(99);
                })).decode().toList());

        assertEquals("Serialized KV batch has trailing bytes: 1", failure.getMessage());
    }

    private static SerializedKVBatch batch(byte[] payload) {
        return new SerializedKVBatch.SerializedKVBatchRef(Buf.wrap(payload));
    }

    private static Buf buf(String value) {
        return Buf.wrap(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] payload(PayloadWriter writer) {
        var out = new ByteArrayOutputStream();
        writer.write(out);
        return out.toByteArray();
    }

    private static void writeBytes(ByteArrayOutputStream out, String value) {
        var bytes = bytes(value);
        writeIntLE(out, bytes.length);
        out.writeBytes(bytes);
    }

    private static void writeIntLE(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 24) & 0xFF);
    }

    @FunctionalInterface
    private interface PayloadWriter {

        void write(ByteArrayOutputStream out);
    }
}
