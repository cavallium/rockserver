package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.internal.BoundedByteArrayOutputStream;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

public final class ReplicatedCdcEventCodec {

    private static final int MAGIC = 0x52534345;
    private static final int VERSION = 1;
    private static final int MAX_REPLICATED_CDC_EVENT_BODY_BYTES = 64 * 1024 * 1024;
    private static final int MAX_REPLICATED_CDC_EVENT_FIELD_BYTES = 64 * 1024 * 1024;

    private ReplicatedCdcEventCodec() {
    }

    public static byte[] encode(ReplicatedCdcEvent event) {
        requireArgument(event, "event");
        var body = serializeBody(event);
        if (body.length > MAX_REPLICATED_CDC_EVENT_BODY_BYTES) {
            throw new IllegalArgumentException("Invalid replicated CDC event body length: " + body.length);
        }
        var crc = new CRC32C();
        crc.update(body, 0, body.length);
        return ByteBuffer.allocate(Integer.BYTES + body.length + Long.BYTES)
                .putInt(body.length)
                .put(body)
                .putLong(crc.getValue())
                .array();
    }

    public static ReplicatedCdcEvent decode(byte[] payload) {
        requireArgument(payload, "payload");
        if (payload.length < Integer.BYTES + Long.BYTES) {
            throw new IllegalArgumentException("Replicated CDC event payload is too short");
        }
        var buffer = ByteBuffer.wrap(payload);
        int bodyLength = buffer.getInt();
        if (bodyLength < 0
                || bodyLength > MAX_REPLICATED_CDC_EVENT_BODY_BYTES
                || bodyLength > buffer.remaining() - Long.BYTES) {
            throw new IllegalArgumentException("Invalid replicated CDC event body length: " + bodyLength);
        }
        byte[] body = new byte[bodyLength];
        buffer.get(body);
        long expectedCrc = buffer.getLong();
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes after replicated CDC event checksum");
        }
        var crc = new CRC32C();
        crc.update(body, 0, body.length);
        if (crc.getValue() != expectedCrc) {
            throw new IllegalArgumentException("Replicated CDC event checksum mismatch");
        }
        return deserializeBody(body);
    }

    private static byte[] serializeBody(ReplicatedCdcEvent event) {
        var bytes = new BoundedByteArrayOutputStream(
                MAX_REPLICATED_CDC_EVENT_BODY_BYTES,
                "replicated CDC event body"
        );
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            var token = event.token();
            out.writeLong(token.metadataEpoch());
            out.writeLong(token.partitionId().value());
            out.writeLong(token.partitionEpoch());
            out.writeLong(token.appliedIndex());
            out.writeInt(token.eventOrdinal());
            out.writeLong(event.schemaEpoch());
            out.writeLong(event.columnId());
            out.writeInt(event.operation().code());
            writeBytes(out, event.key().toByteArray());
            var value = event.value();
            if (value == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                writeBytes(out, value.toByteArray());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode replicated CDC event", e);
        }
        return bytes.toByteArray();
    }

    private static ReplicatedCdcEvent deserializeBody(byte[] body) {
        try (var in = new DataInputStream(new ByteArrayInputStream(body))) {
            int magic = in.readInt();
            if (magic != MAGIC) {
                throw new IllegalArgumentException("Invalid replicated CDC event magic");
            }
            int version = in.readInt();
            if (version != VERSION) {
                throw new IllegalArgumentException("Unsupported replicated CDC event version: " + version);
            }
            var token = new ReplicatedCdcToken(
                    in.readLong(),
                    new ShardId(in.readLong()),
                    in.readLong(),
                    in.readLong(),
                    in.readInt()
            );
            long schemaEpoch = in.readLong();
            long columnId = in.readLong();
            var operation = ReplicatedMutationOp.fromCode(in.readInt());
            var key = Buf.wrap(readBytes(in)).freeze();
            Buf value = null;
            if (in.readBoolean()) {
                value = Buf.wrap(readBytes(in)).freeze();
            }
            if (in.available() != 0) {
                throw new IllegalArgumentException("Trailing bytes in replicated CDC event body");
            }
            return new ReplicatedCdcEvent(token, schemaEpoch, columnId, key, value, operation);
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed replicated CDC event payload", e);
        }
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        if (value.length > MAX_REPLICATED_CDC_EVENT_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid replicated CDC event byte length: " + value.length);
        }
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_REPLICATED_CDC_EVENT_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid replicated CDC event byte length: " + length);
        }
        if (length > in.available()) {
            throw new IllegalArgumentException("Unexpected end of replicated CDC event payload");
        }
        byte[] value = in.readNBytes(length);
        if (value.length != length) {
            throw new IllegalArgumentException("Unexpected end of replicated CDC event payload");
        }
        return value;
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
