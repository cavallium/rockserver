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

public final class PartitionPointReadCommandCodec {

    private static final int MAGIC = 0x52535052;
    private static final int VERSION = 1;
    private static final int MAX_KEY_PARTS = 1024;
    private static final int MAX_PARTITION_POINT_READ_COMMAND_BODY_BYTES = 64 * 1024 * 1024;
    private static final int MAX_PARTITION_POINT_READ_COMMAND_FIELD_BYTES = 64 * 1024 * 1024;

    private PartitionPointReadCommandCodec() {
    }

    public static byte[] encode(PartitionPointReadCommand command) {
        requireArgument(command, "command");
        var body = serializeBody(command);
        if (body.length > MAX_PARTITION_POINT_READ_COMMAND_BODY_BYTES) {
            throw new IllegalArgumentException("Invalid partition point read command body length: " + body.length);
        }
        var crc = new CRC32C();
        crc.update(body, 0, body.length);

        var payload = ByteBuffer.allocate(Integer.BYTES + body.length + Long.BYTES);
        payload.putInt(body.length);
        payload.put(body);
        payload.putLong(crc.getValue());
        return payload.array();
    }

    public static PartitionPointReadCommand decode(byte[] payload) {
        requireArgument(payload, "payload");
        if (payload.length < Integer.BYTES + Long.BYTES) {
            throw new IllegalArgumentException("Partition point read command payload is too short");
        }

        var buffer = ByteBuffer.wrap(payload);
        int bodyLength = buffer.getInt();
        if (bodyLength < 0
                || bodyLength > MAX_PARTITION_POINT_READ_COMMAND_BODY_BYTES
                || bodyLength > buffer.remaining() - Long.BYTES) {
            throw new IllegalArgumentException("Invalid partition point read command body length: " + bodyLength);
        }

        byte[] body = new byte[bodyLength];
        buffer.get(body);
        long expectedCrc = buffer.getLong();
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes after partition point read command checksum");
        }

        var crc = new CRC32C();
        crc.update(body, 0, body.length);
        if (crc.getValue() != expectedCrc) {
            throw new IllegalArgumentException("Partition point read command checksum mismatch");
        }

        return deserializeBody(body);
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }

    private static byte[] serializeBody(PartitionPointReadCommand command) {
        var bytes = new BoundedByteArrayOutputStream(
                MAX_PARTITION_POINT_READ_COMMAND_BODY_BYTES,
                "partition point read command body"
        );
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeLong(command.schemaEpoch());
            out.writeLong(command.columnId());
            var keyParts = command.keys().keys();
            if (keyParts.length > MAX_KEY_PARTS) {
                throw new IllegalArgumentException("Too many point read key parts: " + keyParts.length);
            }
            out.writeInt(keyParts.length);
            for (var keyPart : keyParts) {
                writeBytes(out, keyPart.toByteArray());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode partition point read command", e);
        }
        return bytes.toByteArray();
    }

    private static PartitionPointReadCommand deserializeBody(byte[] body) {
        try (var in = new DataInputStream(new ByteArrayInputStream(body))) {
            int magic = in.readInt();
            if (magic != MAGIC) {
                throw new IllegalArgumentException("Invalid partition point read command magic");
            }
            int version = in.readInt();
            if (version != VERSION) {
                throw new IllegalArgumentException("Unsupported partition point read command version: " + version);
            }
            long schemaEpoch = in.readLong();
            long columnId = in.readLong();
            int keyPartsCount = in.readInt();
            if (keyPartsCount < 0 || keyPartsCount > MAX_KEY_PARTS) {
                throw new IllegalArgumentException("Invalid point read key part count: " + keyPartsCount);
            }
            if (keyPartsCount > in.available() / Integer.BYTES) {
                throw new IllegalArgumentException("Invalid point read key part count for remaining payload: "
                        + keyPartsCount);
            }
            var keyParts = new Buf[keyPartsCount];
            for (int i = 0; i < keyPartsCount; i++) {
                keyParts[i] = Buf.wrap(readBytes(in)).freeze();
            }
            if (in.available() != 0) {
                throw new IllegalArgumentException("Trailing bytes in partition point read command body");
            }
            return new PartitionPointReadCommand(schemaEpoch, columnId, new Keys(keyParts));
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed partition point read command payload", e);
        }
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        if (value.length > MAX_PARTITION_POINT_READ_COMMAND_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid partition point read command byte length: " + value.length);
        }
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_PARTITION_POINT_READ_COMMAND_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid partition point read command byte length: " + length);
        }
        if (length > in.available()) {
            throw new IllegalArgumentException("Unexpected end of partition point read command payload");
        }
        byte[] value = in.readNBytes(length);
        if (value.length != length) {
            throw new IllegalArgumentException("Unexpected end of partition point read command payload");
        }
        return value;
    }
}
