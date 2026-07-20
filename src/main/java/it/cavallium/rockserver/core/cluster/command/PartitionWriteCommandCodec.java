package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.internal.BoundedByteArrayOutputStream;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.zip.CRC32C;

import org.jetbrains.annotations.Nullable;

public final class PartitionWriteCommandCodec {

    private static final int MAGIC = 0x52535057;
    private static final int VERSION = 3;
    private static final int MIN_VERSION = 1;
    private static final int MAX_PARTITION_WRITE_COMMAND_BODY_BYTES = 64 * 1024 * 1024;
    private static final int MAX_PARTITION_WRITE_COMMAND_FIELD_BYTES = 64 * 1024 * 1024;

    private PartitionWriteCommandCodec() {
    }

    public static byte[] encode(PartitionWriteCommand command) {
        requireArgument(command, "command");
        var body = serializeBody(command);
        if (body.length > MAX_PARTITION_WRITE_COMMAND_BODY_BYTES) {
            throw new IllegalArgumentException("Invalid partition write command body length: " + body.length);
        }
        var payload = ByteBuffer.allocate(Integer.BYTES + body.length + Long.BYTES);
        payload.putInt(body.length);
        payload.put(body);
        payload.putLong(checksum(body));
        return payload.array();
    }

    public static PartitionWriteCommand decode(byte[] payload) {
        requireArgument(payload, "payload");
        if (payload.length < Integer.BYTES + Long.BYTES) {
            throw new IllegalArgumentException("Partition write command payload is too short");
        }

        var buffer = ByteBuffer.wrap(payload);
        int bodyLength = buffer.getInt();
        if (bodyLength < 0
                || bodyLength > MAX_PARTITION_WRITE_COMMAND_BODY_BYTES
                || bodyLength > buffer.remaining() - Long.BYTES) {
            throw new IllegalArgumentException("Invalid partition write command body length: " + bodyLength);
        }

        byte[] body = new byte[bodyLength];
        buffer.get(body);
        long expectedCrc = buffer.getLong();
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes after partition write command checksum");
        }

        if (checksum(body) != expectedCrc) {
            throw new IllegalArgumentException("Partition write command checksum mismatch");
        }

        return deserializeBody(body);
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }

    private static byte[] serializeBody(PartitionWriteCommand command) {
        var bytes = new BoundedByteArrayOutputStream(
                MAX_PARTITION_WRITE_COMMAND_BODY_BYTES,
                "partition write command body"
        );
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            var requestId = command.requestId();
            var clientUuid = requestId.clientId().value();
            out.writeLong(clientUuid.getMostSignificantBits());
            out.writeLong(clientUuid.getLeastSignificantBits());
            out.writeLong(requestId.sequence());
            out.writeLong(command.schemaEpoch());
            out.writeInt(command.responseMode().code());
            var seal = command.seal();
            out.writeBoolean(seal != null);
            if (seal != null) {
                out.writeLong(seal.metadataEpoch());
                out.writeLong(seal.partitionId().value());
                out.writeLong(seal.partitionEpoch());
                writeNullableBuf(out, seal.startKeyInclusive());
                writeNullableBuf(out, seal.endKeyExclusive());
            }
            writeBytes(out, ReplicatedMutationCodec.encodeBatch(command.mutations()));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode partition write command", e);
        }
        return bytes.toByteArray();
    }

    private static PartitionWriteCommand deserializeBody(byte[] body) {
        try (var in = new DataInputStream(new ByteArrayInputStream(body))) {
            int magic = in.readInt();
            if (magic != MAGIC) {
                throw new IllegalArgumentException("Invalid partition write command magic");
            }
            int version = in.readInt();
            if (version < MIN_VERSION || version > VERSION) {
                throw new IllegalArgumentException("Unsupported partition write command version: " + version);
            }
            var clientId = new ClientId(new UUID(in.readLong(), in.readLong()));
            var requestId = new ClientRequestId(clientId, in.readLong());
            long schemaEpoch = in.readLong();
            var responseMode = version >= 2
                    ? PartitionWriteResponseMode.fromCode(in.readInt())
                    : PartitionWriteResponseMode.NONE;
            var seal = version == 3 && in.readBoolean()
                    ? new PartitionWriteCommandSeal(in.readLong(), new ShardId(in.readLong()), in.readLong(),
                    readNullableBuf(in), readNullableBuf(in))
                    : null;
            var mutations = ReplicatedMutationCodec.decodeBatch(readBytes(in), PartitionWriteCommand.MAX_MUTATIONS);
            if (in.available() != 0) {
                throw new IllegalArgumentException("Trailing bytes in partition write command body");
            }
            return new PartitionWriteCommand(requestId, schemaEpoch, mutations, responseMode, seal);
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed partition write command payload", e);
        }
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        if (value.length > MAX_PARTITION_WRITE_COMMAND_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid partition write command byte length: " + value.length);
        }
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_PARTITION_WRITE_COMMAND_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid partition write command byte length: " + length);
        }
        if (length > in.available()) {
            throw new IllegalArgumentException("Unexpected end of partition write command payload");
        }
        byte[] value = in.readNBytes(length);
        if (value.length != length) {
            throw new IllegalArgumentException("Unexpected end of partition write command payload");
        }
        return value;
    }

    private static long checksum(byte[] body) {
        var crc = new CRC32C();
        crc.update(body, 0, body.length);
        return crc.getValue();
    }

    private static void writeNullableBuf(DataOutputStream out, @Nullable Buf value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            writeBytes(out, value.toByteArray());
        }
    }

    private static @Nullable Buf readNullableBuf(DataInputStream in) throws IOException {
        return in.readBoolean() ? Buf.wrap(readBytes(in)).freeze() : null;
    }

}
