package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.internal.BoundedByteArrayOutputStream;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32C;

public final class PartitionRangeReadResponseCodec {

    private static final int MAGIC = 0x52535250;
    private static final int VERSION = 1;
    private static final int MAX_ENTRIES = PartitionRangeReadCommand.MAX_ENTRIES;
    private static final int MAX_KEY_PARTS = 1024;
    private static final int MAX_PARTITION_RANGE_READ_RESPONSE_BODY_BYTES = 64 * 1024 * 1024;
    private static final int MAX_PARTITION_RANGE_READ_RESPONSE_FIELD_BYTES = 64 * 1024 * 1024;

    private PartitionRangeReadResponseCodec() {
    }

    public static byte[] encodeEntries(List<KV> entries) {
        requireArgument(entries, "entries");
        if (entries.size() > MAX_ENTRIES) {
            throw new IllegalArgumentException("Too many partition range read response entries: " + entries.size());
        }
        var body = serializeBody(entries);
        if (body.length > MAX_PARTITION_RANGE_READ_RESPONSE_BODY_BYTES) {
            throw new IllegalArgumentException("Invalid partition range read response body length: " + body.length);
        }
        var crc = new CRC32C();
        crc.update(body, 0, body.length);

        var payload = ByteBuffer.allocate(Integer.BYTES + body.length + Long.BYTES);
        payload.putInt(body.length);
        payload.put(body);
        payload.putLong(crc.getValue());
        return payload.array();
    }

    public static List<KV> decodeEntries(byte[] payload) {
        return decodeEntries(payload, MAX_ENTRIES);
    }

    public static List<KV> decodeEntries(byte[] payload, int maxEntries) {
        requireArgument(payload, "payload");
        if (maxEntries < 0 || maxEntries > MAX_ENTRIES) {
            throw new IllegalArgumentException("maxEntries must be between 0 and " + MAX_ENTRIES);
        }
        if (payload.length < Integer.BYTES + Long.BYTES) {
            throw new IllegalArgumentException("Partition range read response payload is too short");
        }

        var buffer = ByteBuffer.wrap(payload);
        int bodyLength = buffer.getInt();
        if (bodyLength < 0
                || bodyLength > MAX_PARTITION_RANGE_READ_RESPONSE_BODY_BYTES
                || bodyLength > buffer.remaining() - Long.BYTES) {
            throw new IllegalArgumentException("Invalid partition range read response body length: " + bodyLength);
        }

        byte[] body = new byte[bodyLength];
        buffer.get(body);
        long expectedCrc = buffer.getLong();
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes after partition range read response checksum");
        }

        var crc = new CRC32C();
        crc.update(body, 0, body.length);
        if (crc.getValue() != expectedCrc) {
            throw new IllegalArgumentException("Partition range read response checksum mismatch");
        }

        return deserializeBody(body, maxEntries);
    }

    private static byte[] serializeBody(List<KV> entries) {
        var bytes = new BoundedByteArrayOutputStream(
                MAX_PARTITION_RANGE_READ_RESPONSE_BODY_BYTES,
                "partition range read response body"
        );
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeInt(entries.size());
            for (var entry : entries) {
                writeEntry(out, entry);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode partition range read response", e);
        }
        return bytes.toByteArray();
    }

    private static List<KV> deserializeBody(byte[] body, int maxEntries) {
        try (var in = new DataInputStream(new ByteArrayInputStream(body))) {
            int magic = in.readInt();
            if (magic != MAGIC) {
                throw new IllegalArgumentException("Invalid partition range read response magic");
            }
            int version = in.readInt();
            if (version != VERSION) {
                throw new IllegalArgumentException("Unsupported partition range read response version: " + version);
            }
            int entryCount = in.readInt();
            if (entryCount < 0 || entryCount > MAX_ENTRIES) {
                throw new IllegalArgumentException("Invalid partition range read response entry count: " + entryCount);
            }
            if (entryCount > maxEntries) {
                throw new IllegalArgumentException("Partition range read response entry count "
                        + entryCount
                        + " exceeds limit "
                        + maxEntries);
            }
            if (entryCount > in.available() / Integer.BYTES) {
                throw new IllegalArgumentException("Invalid partition range read response entry count for remaining payload: "
                        + entryCount);
            }
            var entries = new ArrayList<KV>(entryCount);
            for (int i = 0; i < entryCount; i++) {
                entries.add(readEntry(in));
            }
            if (in.available() != 0) {
                throw new IllegalArgumentException("Trailing bytes in partition range read response body");
            }
            return Collections.unmodifiableList(entries);
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed partition range read response payload", e);
        }
    }

    private static void writeEntry(DataOutputStream out, KV entry) throws IOException {
        requireArgument(entry, "entry");
        writeKeys(out, entry.keys());
        writeNullableBuf(out, entry.value());
    }

    private static KV readEntry(DataInputStream in) throws IOException {
        return new KV(readKeys(in), readNullableBuf(in));
    }

    private static void writeKeys(DataOutputStream out, Keys keys) throws IOException {
        requireArgument(keys, "keys");
        var keyParts = requireArgument(keys.keys(), "keyParts");
        if (keyParts.length > MAX_KEY_PARTS) {
            throw new IllegalArgumentException("Too many partition range read response key parts: " + keyParts.length);
        }
        out.writeInt(keyParts.length);
        for (int i = 0; i < keyParts.length; i++) {
            writeBytes(out, requireArgument(keyParts[i], "keyParts[" + i + "]").toByteArray());
        }
    }

    private static Keys readKeys(DataInputStream in) throws IOException {
        int keyPartsCount = in.readInt();
        if (keyPartsCount < 0 || keyPartsCount > MAX_KEY_PARTS) {
            throw new IllegalArgumentException("Invalid partition range read response key part count: " + keyPartsCount);
        }
        if (keyPartsCount > in.available() / Integer.BYTES) {
            throw new IllegalArgumentException("Invalid partition range read response key part count for remaining payload: "
                    + keyPartsCount);
        }
        var keyParts = new Buf[keyPartsCount];
        for (int i = 0; i < keyPartsCount; i++) {
            keyParts[i] = Buf.wrap(readBytes(in)).freeze();
        }
        return new Keys(keyParts);
    }

    private static void writeNullableBuf(DataOutputStream out, Buf value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            writeBytes(out, value.toByteArray());
        }
    }

    private static Buf readNullableBuf(DataInputStream in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }
        return Buf.wrap(readBytes(in)).freeze();
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        if (value.length > MAX_PARTITION_RANGE_READ_RESPONSE_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid partition range read response byte length: " + value.length);
        }
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_PARTITION_RANGE_READ_RESPONSE_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid partition range read response byte length: " + length);
        }
        if (length > in.available()) {
            throw new IllegalArgumentException("Unexpected end of partition range read response payload");
        }
        byte[] value = in.readNBytes(length);
        if (value.length != length) {
            throw new IllegalArgumentException("Unexpected end of partition range read response payload");
        }
        return value;
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
