package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.internal.BoundedByteArrayOutputStream;
import it.cavallium.rockserver.core.common.Delta;

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

public final class PartitionWriteResponseCodec {

    private static final int MAGIC = 0x52535052;
    private static final int VERSION = 1;
    private static final int MAX_RESPONSES = 1_000_000;
    private static final int MAX_RESPONSE_NESTING_DEPTH = 64;
    private static final int MAX_PARTITION_WRITE_RESPONSE_BODY_BYTES = 64 * 1024 * 1024;
    private static final int MAX_PARTITION_WRITE_RESPONSE_FIELD_BYTES = 64 * 1024 * 1024;

    private static final int TYPE_NULL = 0;
    private static final int TYPE_BUF = 1;
    private static final int TYPE_BOOLEAN = 2;
    private static final int TYPE_DELTA = 3;

    private PartitionWriteResponseCodec() {
    }

    public static byte[] encodeResponses(List<?> responses) {
        requireArgument(responses, "responses");
        if (responses.size() > MAX_RESPONSES) {
            throw new IllegalArgumentException("Too many partition write responses: " + responses.size());
        }
        var body = serializeBody(responses);
        if (body.length > MAX_PARTITION_WRITE_RESPONSE_BODY_BYTES) {
            throw new IllegalArgumentException("Invalid partition write response body length: " + body.length);
        }
        var payload = ByteBuffer.allocate(Integer.BYTES + body.length + Long.BYTES);
        payload.putInt(body.length);
        payload.put(body);
        payload.putLong(crc32c(body));
        return payload.array();
    }

    public static List<Object> decodeResponses(byte[] payload) {
        return decodeResponses(payload, MAX_RESPONSES);
    }

    public static List<Object> decodeResponses(byte[] payload, int maxResponses) {
        requireArgument(payload, "payload");
        if (maxResponses < 0 || maxResponses > MAX_RESPONSES) {
            throw new IllegalArgumentException("maxResponses must be between 0 and " + MAX_RESPONSES);
        }
        if (payload.length < Integer.BYTES + Long.BYTES) {
            throw new IllegalArgumentException("Partition write response payload is too short");
        }

        var buffer = ByteBuffer.wrap(payload);
        int bodyLength = buffer.getInt();
        if (bodyLength < 0
                || bodyLength > MAX_PARTITION_WRITE_RESPONSE_BODY_BYTES
                || bodyLength > buffer.remaining() - Long.BYTES) {
            throw new IllegalArgumentException("Invalid partition write response body length: " + bodyLength);
        }

        byte[] body = new byte[bodyLength];
        buffer.get(body);
        long expectedCrc = buffer.getLong();
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes after partition write response checksum");
        }

        if (crc32c(body) != expectedCrc) {
            throw new IllegalArgumentException("Partition write response checksum mismatch");
        }

        return deserializeBody(body, maxResponses);
    }

    private static byte[] serializeBody(List<?> responses) {
        var bytes = new BoundedByteArrayOutputStream(
                MAX_PARTITION_WRITE_RESPONSE_BODY_BYTES,
                "partition write response body"
        );
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeInt(responses.size());
            for (var response : responses) {
                writeResponse(out, response, 0);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode partition write response", e);
        }
        return bytes.toByteArray();
    }

    private static List<Object> deserializeBody(byte[] body, int maxResponses) {
        try (var in = new DataInputStream(new ByteArrayInputStream(body))) {
            int magic = in.readInt();
            if (magic != MAGIC) {
                throw new IllegalArgumentException("Invalid partition write response magic");
            }
            int version = in.readInt();
            if (version != VERSION) {
                throw new IllegalArgumentException("Unsupported partition write response version: " + version);
            }
            int responseCount = in.readInt();
            if (responseCount < 0 || responseCount > MAX_RESPONSES) {
                throw new IllegalArgumentException("Invalid partition write response count: " + responseCount);
            }
            if (responseCount > maxResponses) {
                throw new IllegalArgumentException("Partition write response count "
                        + responseCount
                        + " exceeds limit "
                        + maxResponses);
            }
            if (responseCount > in.available()) {
                throw new IllegalArgumentException("Invalid partition write response count for remaining payload: "
                        + responseCount);
            }
            var responses = new ArrayList<>(responseCount);
            for (int i = 0; i < responseCount; i++) {
                responses.add(readResponse(in, 0));
            }
            if (in.available() != 0) {
                throw new IllegalArgumentException("Trailing bytes in partition write response body");
            }
            return Collections.unmodifiableList(responses);
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed partition write response payload", e);
        }
    }

    private static void writeResponse(DataOutputStream out, Object response, int nestingDepth) throws IOException {
        requireResponseNestingDepth(nestingDepth);
        switch (response) {
            case null -> out.writeByte(TYPE_NULL);
            case Buf buf -> {
                out.writeByte(TYPE_BUF);
                writeBytes(out, buf.toByteArray());
            }
            case Boolean bool -> {
                out.writeByte(TYPE_BOOLEAN);
                out.writeBoolean(bool);
            }
            case Delta<?>(var previous, var current) -> {
                out.writeByte(TYPE_DELTA);
                writeResponse(out, previous, nestingDepth + 1);
                writeResponse(out, current, nestingDepth + 1);
            }
            default -> throw new IllegalArgumentException("Unsupported partition write response type: "
                    + response.getClass().getName());
        }
    }

    private static Object readResponse(DataInputStream in, int nestingDepth) throws IOException {
        requireResponseNestingDepth(nestingDepth);
        return switch (in.readUnsignedByte()) {
            case TYPE_NULL -> null;
            case TYPE_BUF -> Buf.wrap(readBytes(in)).freeze();
            case TYPE_BOOLEAN -> in.readBoolean();
            case TYPE_DELTA -> new Delta<>(
                    readResponse(in, nestingDepth + 1),
                    readResponse(in, nestingDepth + 1)
            );
            default -> throw new IllegalArgumentException("Unknown partition write response type");
        };
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        if (value.length > MAX_PARTITION_WRITE_RESPONSE_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid partition write response byte length: " + value.length);
        }
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_PARTITION_WRITE_RESPONSE_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid partition write response byte length: " + length);
        }
        if (length > in.available()) {
            throw new IllegalArgumentException("Unexpected end of partition write response payload");
        }
        byte[] value = in.readNBytes(length);
        if (value.length != length) {
            throw new IllegalArgumentException("Unexpected end of partition write response payload");
        }
        return value;
    }

    private static void requireResponseNestingDepth(int nestingDepth) {
        if (nestingDepth > MAX_RESPONSE_NESTING_DEPTH) {
            throw new IllegalArgumentException("Partition write response nesting depth exceeds limit "
                    + MAX_RESPONSE_NESTING_DEPTH);
        }
    }

    private static long crc32c(byte[] body) {
        var crc = new CRC32C();
        crc.update(body, 0, body.length);
        return crc.getValue();
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
