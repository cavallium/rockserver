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
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

import org.jetbrains.annotations.Nullable;

public final class ReplicatedMutationCodec {

    private static final int MIN_SERIALIZED_MUTATION_BYTES = Byte.BYTES + Long.BYTES + Integer.BYTES + Byte.BYTES;
    private static final int MAGIC = 0x52534D31;
    private static final int BATCH_MAGIC = 0x52534D42;
    private static final int VERSION = 2;
    private static final int MIN_VERSION = 1;
    private static final int MAX_KEY_PARTS = 1024;
    private static final int MAX_BATCH_MUTATIONS = 1_000_000;
    private static final int MAX_REPLICATED_MUTATION_BODY_BYTES = 64 * 1024 * 1024;
    private static final int MAX_REPLICATED_MUTATION_FIELD_BYTES = 64 * 1024 * 1024;

    private ReplicatedMutationCodec() {
    }

    public static byte[] encode(ReplicatedMutation mutation) {
        requireArgument(mutation, "mutation");
        byte[] body = serializeSingleBody(mutation);
        return encodeWithChecksum(body);
    }

    public static byte[] encodeBatch(List<ReplicatedMutation> mutations) {
        requireArgument(mutations, "mutations");
        if (mutations.isEmpty()) {
            throw new IllegalArgumentException("replicated mutation batch must not be empty");
        }
        if (mutations.size() > MAX_BATCH_MUTATIONS) {
            throw new IllegalArgumentException("Too many replicated mutations in one batch: " + mutations.size());
        }
        byte[] body = serializeBatchBody(mutations);
        return encodeWithChecksum(body);
    }

    private static byte[] encodeWithChecksum(byte[] body) {
        if (body.length > MAX_REPLICATED_MUTATION_BODY_BYTES) {
            throw new IllegalArgumentException("Invalid replicated mutation body length: " + body.length);
        }
        var crc = new CRC32C();
        crc.update(body, 0, body.length);

        var payload = ByteBuffer.allocate(Integer.BYTES + body.length + Long.BYTES);
        payload.putInt(body.length);
        payload.put(body);
        payload.putLong(crc.getValue());
        return payload.array();
    }

    public static ReplicatedMutation decode(byte[] payload) {
        var mutations = decodeBatch(payload, 1);
        if (mutations.size() != 1) {
            throw new IllegalArgumentException("Expected one replicated mutation but found " + mutations.size());
        }
        return mutations.getFirst();
    }

    public static List<ReplicatedMutation> decodeBatch(byte[] payload) {
        return decodeBatch(payload, MAX_BATCH_MUTATIONS);
    }

    public static List<ReplicatedMutation> decodeBatch(byte[] payload, int maxMutations) {
        requireArgument(payload, "payload");
        if (maxMutations <= 0 || maxMutations > MAX_BATCH_MUTATIONS) {
            throw new IllegalArgumentException("Invalid replicated mutation batch limit: " + maxMutations);
        }
        if (payload.length < Integer.BYTES + Long.BYTES) {
            throw new IllegalArgumentException("Replicated mutation payload is too short");
        }

        var buffer = ByteBuffer.wrap(payload);
        int bodyLength = buffer.getInt();
        if (bodyLength < 0
                || bodyLength > MAX_REPLICATED_MUTATION_BODY_BYTES
                || bodyLength > buffer.remaining() - Long.BYTES) {
            throw new IllegalArgumentException("Invalid replicated mutation body length: " + bodyLength);
        }

        byte[] body = new byte[bodyLength];
        buffer.get(body);
        long expectedCrc = buffer.getLong();
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes after replicated mutation checksum");
        }

        var crc = new CRC32C();
        crc.update(body, 0, body.length);
        if (crc.getValue() != expectedCrc) {
            throw new IllegalArgumentException("Replicated mutation checksum mismatch");
        }

        return deserializeBody(body, maxMutations);
    }

    private static byte[] serializeSingleBody(ReplicatedMutation mutation) {
        var bytes = new BoundedByteArrayOutputStream(
                MAX_REPLICATED_MUTATION_BODY_BYTES,
                "replicated mutation body"
        );
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            writeMutation(out, mutation);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode replicated mutation", e);
        }
        return bytes.toByteArray();
    }

    private static byte[] serializeBatchBody(List<ReplicatedMutation> mutations) {
        var bytes = new BoundedByteArrayOutputStream(
                MAX_REPLICATED_MUTATION_BODY_BYTES,
                "replicated mutation body"
        );
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(BATCH_MAGIC);
            out.writeInt(VERSION);
            out.writeInt(mutations.size());
            for (var mutation : mutations) {
                writeMutation(out, requireArgument(mutation, "mutation"));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode replicated mutation batch", e);
        }
        return bytes.toByteArray();
    }

    private static List<ReplicatedMutation> deserializeBody(byte[] body, int maxMutations) {
        try (var in = new DataInputStream(new ByteArrayInputStream(body))) {
            int magic = in.readInt();
            if (magic != MAGIC && magic != BATCH_MAGIC) {
                throw new IllegalArgumentException("Invalid replicated mutation magic");
            }
            int version = in.readInt();
            if (version < MIN_VERSION || version > VERSION) {
                throw new IllegalArgumentException("Unsupported replicated mutation version: " + version);
            }

            List<ReplicatedMutation> mutations;
            if (magic == MAGIC) {
                mutations = List.of(readMutation(in, version));
            } else {
                int mutationCount = in.readInt();
                if (mutationCount <= 0 || mutationCount > MAX_BATCH_MUTATIONS) {
                    throw new IllegalArgumentException("Invalid replicated mutation batch size: " + mutationCount);
                }
                if (mutationCount > maxMutations) {
                    throw new IllegalArgumentException("Replicated mutation batch size "
                            + mutationCount
                            + " exceeds limit "
                            + maxMutations);
                }
                if (mutationCount > in.available() / MIN_SERIALIZED_MUTATION_BYTES) {
                    throw new IllegalArgumentException("Invalid replicated mutation batch size for remaining payload: "
                            + mutationCount);
                }
                var batch = new ArrayList<ReplicatedMutation>(mutationCount);
                for (int i = 0; i < mutationCount; i++) {
                    batch.add(readMutation(in, version));
                }
                mutations = List.copyOf(batch);
            }
            if (in.available() != 0) {
                throw new IllegalArgumentException("Trailing bytes in replicated mutation body");
            }
            return mutations;
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed replicated mutation payload", e);
        }
    }

    private static void writeMutation(DataOutputStream out, ReplicatedMutation mutation) throws IOException {
        out.writeByte(mutation.operation().code());
        out.writeLong(mutation.columnId());

        var keyParts = mutation.keys().keys();
        if (keyParts.length > MAX_KEY_PARTS) {
            throw new IllegalArgumentException("Too many replicated mutation key parts: " + keyParts.length);
        }
        out.writeInt(keyParts.length);
        for (var keyPart : keyParts) {
            writeBytes(out, keyPart.toByteArray());
        }

        writeNullableValue(out, mutation.calculatedKey());
        var value = mutation.value();
        writeNullableValue(out, value);
    }

    private static ReplicatedMutation readMutation(DataInputStream in, int version) throws IOException {
        var operation = ReplicatedMutationOp.fromCode(in.readUnsignedByte());
        long columnId = in.readLong();
        int keyPartsCount = in.readInt();
        if (keyPartsCount < 0 || keyPartsCount > MAX_KEY_PARTS) {
            throw new IllegalArgumentException("Invalid replicated mutation key part count: " + keyPartsCount);
        }
        if (keyPartsCount > in.available() / Integer.BYTES) {
            throw new IllegalArgumentException("Invalid replicated mutation key part count for remaining payload: "
                    + keyPartsCount);
        }
        var keyParts = new Buf[keyPartsCount];
        for (int i = 0; i < keyPartsCount; i++) {
            keyParts[i] = Buf.wrap(readBytes(in)).freeze();
        }

        Buf calculatedKey = version >= 2 ? readNullableValue(in) : null;
        Buf value = readNullableValue(in);
        return new ReplicatedMutation(operation, columnId, new Keys(keyParts), value, calculatedKey);
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        if (value.length > MAX_REPLICATED_MUTATION_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid replicated mutation byte length: " + value.length);
        }
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_REPLICATED_MUTATION_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid replicated mutation byte length: " + length);
        }
        if (length > in.available()) {
            throw new IllegalArgumentException("Unexpected end of replicated mutation payload");
        }
        byte[] value = in.readNBytes(length);
        if (value.length != length) {
            throw new IllegalArgumentException("Unexpected end of replicated mutation payload");
        }
        return value;
    }

    private static @Nullable Buf readNullableValue(DataInputStream in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }
        return Buf.wrap(readBytes(in)).freeze();
    }

    private static void writeNullableValue(DataOutputStream out, @Nullable Buf value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            writeBytes(out, value.toByteArray());
        }
    }

    private static <T> T requireArgument(@Nullable T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

}
