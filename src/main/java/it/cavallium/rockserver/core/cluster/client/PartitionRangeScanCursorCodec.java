package it.cavallium.rockserver.core.cluster.client;

import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommandCodec;
import it.cavallium.rockserver.core.cluster.internal.BoundedByteArrayOutputStream;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

public final class PartitionRangeScanCursorCodec {

    private static final int MAGIC = 0x52534352;
    private static final int VERSION = 1;
    private static final int MAX_PARTITION_RANGE_SCAN_CURSOR_BODY_BYTES = 64 * 1024 * 1024;
    private static final int MAX_PARTITION_RANGE_SCAN_CURSOR_FIELD_BYTES = 64 * 1024 * 1024;

    private PartitionRangeScanCursorCodec() {
    }

    public static byte[] encode(PartitionRangeScanCursor cursor) {
        requireArgument(cursor, "cursor");
        var body = serializeBody(cursor);
        if (body.length > MAX_PARTITION_RANGE_SCAN_CURSOR_BODY_BYTES) {
            throw new IllegalArgumentException("Invalid partition range scan cursor body length: " + body.length);
        }
        var crc = new CRC32C();
        crc.update(body, 0, body.length);

        var payload = ByteBuffer.allocate(Integer.BYTES + body.length + Long.BYTES);
        payload.putInt(body.length);
        payload.put(body);
        payload.putLong(crc.getValue());
        return payload.array();
    }

    public static PartitionRangeScanCursor decode(byte[] payload) {
        requireArgument(payload, "payload");
        if (payload.length < Integer.BYTES + Long.BYTES) {
            throw new IllegalArgumentException("Partition range scan cursor payload is too short");
        }

        var buffer = ByteBuffer.wrap(payload);
        int bodyLength = buffer.getInt();
        if (bodyLength < 0
                || bodyLength > MAX_PARTITION_RANGE_SCAN_CURSOR_BODY_BYTES
                || bodyLength > buffer.remaining() - Long.BYTES) {
            throw new IllegalArgumentException("Invalid partition range scan cursor body length: " + bodyLength);
        }

        byte[] body = new byte[bodyLength];
        buffer.get(body);
        long expectedCrc = buffer.getLong();
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes after partition range scan cursor checksum");
        }

        var crc = new CRC32C();
        crc.update(body, 0, body.length);
        if (crc.getValue() != expectedCrc) {
            throw new IllegalArgumentException("Partition range scan cursor checksum mismatch");
        }

        return deserializeBody(body);
    }

    private static byte[] serializeBody(PartitionRangeScanCursor cursor) {
        var bytes = new BoundedByteArrayOutputStream(
                MAX_PARTITION_RANGE_SCAN_CURSOR_BODY_BYTES,
                "partition range scan cursor body"
        );
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeLong(cursor.metadataEpoch());
            out.writeInt(cursor.nextSegmentIndex());
            writeBytes(out, PartitionRangeReadCommandCodec.encode(cursor.command()));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode partition range scan cursor", e);
        }
        return bytes.toByteArray();
    }

    private static PartitionRangeScanCursor deserializeBody(byte[] body) {
        try (var in = new DataInputStream(new ByteArrayInputStream(body))) {
            int magic = in.readInt();
            if (magic != MAGIC) {
                throw new IllegalArgumentException("Invalid partition range scan cursor magic");
            }
            int version = in.readInt();
            if (version != VERSION) {
                throw new IllegalArgumentException("Unsupported partition range scan cursor version: " + version);
            }
            long metadataEpoch = in.readLong();
            int nextSegmentIndex = in.readInt();
            var command = PartitionRangeReadCommandCodec.decode(readBytes(in));
            if (in.available() != 0) {
                throw new IllegalArgumentException("Trailing bytes in partition range scan cursor body");
            }
            return new PartitionRangeScanCursor(metadataEpoch, command, nextSegmentIndex);
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed partition range scan cursor payload", e);
        }
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        if (value.length > MAX_PARTITION_RANGE_SCAN_CURSOR_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid partition range scan cursor byte length: " + value.length);
        }
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_PARTITION_RANGE_SCAN_CURSOR_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid partition range scan cursor byte length: " + length);
        }
        if (length > in.available()) {
            throw new IllegalArgumentException("Unexpected end of partition range scan cursor payload");
        }
        byte[] value = in.readNBytes(length);
        if (value.length != length) {
            throw new IllegalArgumentException("Unexpected end of partition range scan cursor payload");
        }
        return value;
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
