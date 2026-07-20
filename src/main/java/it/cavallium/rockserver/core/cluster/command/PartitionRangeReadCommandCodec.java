package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.internal.BoundedByteArrayOutputStream;
import it.cavallium.rockserver.core.common.Keys;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import org.jetbrains.annotations.Nullable;

public final class PartitionRangeReadCommandCodec {

    private static final int MAGIC = 0x52535252;
    private static final int VERSION = 1;
    private static final int MAX_KEY_PARTS = 1024;
    private static final int MAX_PARTITION_RANGE_READ_COMMAND_BODY_BYTES = 64 * 1024 * 1024;
    private static final int MAX_PARTITION_RANGE_READ_COMMAND_FIELD_BYTES = 64 * 1024 * 1024;

    private PartitionRangeReadCommandCodec() {
    }

    public static byte[] encode(PartitionRangeReadCommand command) {
        requireArgument(command, "command");
        var body = serializeBody(command);
        if (body.length > MAX_PARTITION_RANGE_READ_COMMAND_BODY_BYTES) {
            throw new IllegalArgumentException("Invalid partition range read command body length: " + body.length);
        }
        var crc = new CRC32C();
        crc.update(body, 0, body.length);

        var payload = ByteBuffer.allocate(Integer.BYTES + body.length + Long.BYTES);
        payload.putInt(body.length);
        payload.put(body);
        payload.putLong(crc.getValue());
        return payload.array();
    }

    public static PartitionRangeReadCommand decode(byte[] payload) {
        requireArgument(payload, "payload");
        if (payload.length < Integer.BYTES + Long.BYTES) {
            throw new IllegalArgumentException("Partition range read command payload is too short");
        }

        var buffer = ByteBuffer.wrap(payload);
        int bodyLength = buffer.getInt();
        if (bodyLength < 0
                || bodyLength > MAX_PARTITION_RANGE_READ_COMMAND_BODY_BYTES
                || bodyLength > buffer.remaining() - Long.BYTES) {
            throw new IllegalArgumentException("Invalid partition range read command body length: " + bodyLength);
        }

        byte[] body = new byte[bodyLength];
        buffer.get(body);
        long expectedCrc = buffer.getLong();
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes after partition range read command checksum");
        }

        var crc = new CRC32C();
        crc.update(body, 0, body.length);
        if (crc.getValue() != expectedCrc) {
            throw new IllegalArgumentException("Partition range read command checksum mismatch");
        }

        return deserializeBody(body);
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }

    private static byte[] serializeBody(PartitionRangeReadCommand command) {
        var bytes = new BoundedByteArrayOutputStream(
                MAX_PARTITION_RANGE_READ_COMMAND_BODY_BYTES,
                "partition range read command body"
        );
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeLong(command.schemaEpoch());
            out.writeLong(command.columnId());
            writeNullableKeys(out, command.startKeysInclusive());
            writeNullableKeys(out, command.endKeysExclusive());
            out.writeBoolean(command.reverse());
            out.writeInt(command.maxEntries());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode partition range read command", e);
        }
        return bytes.toByteArray();
    }

    private static PartitionRangeReadCommand deserializeBody(byte[] body) {
        try (var in = new DataInputStream(new ByteArrayInputStream(body))) {
            int magic = in.readInt();
            if (magic != MAGIC) {
                throw new IllegalArgumentException("Invalid partition range read command magic");
            }
            int version = in.readInt();
            if (version != VERSION) {
                throw new IllegalArgumentException("Unsupported partition range read command version: " + version);
            }
            long schemaEpoch = in.readLong();
            long columnId = in.readLong();
            var startKeysInclusive = readNullableKeys(in);
            var endKeysExclusive = readNullableKeys(in);
            boolean reverse = in.readBoolean();
            int maxEntries = in.readInt();
            if (in.available() != 0) {
                throw new IllegalArgumentException("Trailing bytes in partition range read command body");
            }
            return new PartitionRangeReadCommand(schemaEpoch,
                    columnId,
                    startKeysInclusive,
                    endKeysExclusive,
                    reverse,
                    maxEntries);
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed partition range read command payload", e);
        }
    }

    private static void writeNullableKeys(DataOutputStream out, @Nullable Keys keys) throws IOException {
        out.writeBoolean(keys != null);
        if (keys != null) {
            writeKeys(out, keys);
        }
    }

    private static void writeKeys(DataOutputStream out, Keys keys) throws IOException {
        var keyParts = keys.keys();
        if (keyParts.length > MAX_KEY_PARTS) {
            throw new IllegalArgumentException("Too many range read key parts: " + keyParts.length);
        }
        out.writeInt(keyParts.length);
        for (var keyPart : keyParts) {
            writeBytes(out, keyPart.toByteArray());
        }
    }

    private static @Nullable Keys readNullableKeys(DataInputStream in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }
        int keyPartsCount = in.readInt();
        if (keyPartsCount < 0 || keyPartsCount > MAX_KEY_PARTS) {
            throw new IllegalArgumentException("Invalid range read key part count: " + keyPartsCount);
        }
        if (keyPartsCount > in.available() / Integer.BYTES) {
            throw new IllegalArgumentException("Invalid range read key part count for remaining payload: "
                    + keyPartsCount);
        }
        var keyParts = new Buf[keyPartsCount];
        for (int i = 0; i < keyPartsCount; i++) {
            keyParts[i] = Buf.wrap(readBytes(in)).freeze();
        }
        return new Keys(keyParts);
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        if (value.length > MAX_PARTITION_RANGE_READ_COMMAND_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid partition range read command byte length: " + value.length);
        }
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_PARTITION_RANGE_READ_COMMAND_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid partition range read command byte length: " + length);
        }
        if (length > in.available()) {
            throw new IllegalArgumentException("Unexpected end of partition range read command payload");
        }
        byte[] value = in.readNBytes(length);
        if (value.length != length) {
            throw new IllegalArgumentException("Unexpected end of partition range read command payload");
        }
        return value;
    }
}
