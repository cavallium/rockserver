package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.UUID;

/** Canonical big-endian v1 encoding, with explicit tags and bounded length prefixes. */
public final class DistributedCommandCodec {
    public static final int FORMAT_VERSION = 1;
    public static final int MAX_MUTATIONS = 256;
    public static final int MAX_ENCODED_BYTES = 2 * 1024 * 1024;
    // version, group UUID, range epoch, schema digest, session UUID/epoch/sequence, count
    static final int HEADER_BYTES = 4 + 16 + 8 + 32 + 16 + 8 + 8 + 4;

    private DistributedCommandCodec() {}

    public static byte[] encode(DistributedCommand command) {
        Objects.requireNonNull(command, "command");
        int size = HEADER_BYTES;
        for (Mutation mutation : command.mutations()) size += mutation.encodedSize();
        ByteBuffer output = ByteBuffer.allocate(size);
        output.putInt(FORMAT_VERSION);
        putUuid(output, command.groupId());
        output.putLong(command.rangeEpoch()).put(command.schemaDigest());
        putUuid(output, command.operationId().sessionId());
        output.putLong(command.operationId().epoch()).putLong(command.operationId().sequence());
        output.putInt(command.mutations().size());
        for (Mutation mutation : command.mutations()) {
            output.put((byte) switch (mutation.kind()) {
                case PUT -> 1; case DELETE -> 2; case MERGE -> 3; case DELETE_RANGE -> 4;
            });
            output.put((byte) switch (mutation.returnMode()) {
                case NONE -> 0; case PREVIOUS -> 1; case CURRENT -> 2;
            });
            putUuid(output, mutation.columnId());
            byte[] key = mutation.key();
            byte[] value = mutation.value();
            output.putInt(key.length).put(key);
            output.putInt(value == null ? -1 : value.length);
            if (value != null) output.put(value);
        }
        return output.array();
    }

    public static DistributedCommand decode(byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length < HEADER_BYTES || encoded.length > MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException("Invalid command byte length");
        }
        ByteBuffer input = ByteBuffer.wrap(encoded);
        if (input.getInt() != FORMAT_VERSION) throw new IllegalArgumentException("Unsupported command version");
        UUID group = getUuid(input);
        long rangeEpoch = input.getLong();
        byte[] schema = new byte[32];
        input.get(schema);
        OperationId operation = new OperationId(getUuid(input), input.getLong(), input.getLong());
        int count = input.getInt();
        if (count <= 0 || count > MAX_MUTATIONS || count > input.remaining() / 26) {
            throw new IllegalArgumentException("Invalid mutation count");
        }
        var mutations = new ArrayList<Mutation>(count);
        for (int i = 0; i < count; i++) {
            require(input, 26);
            Mutation.Kind kind = switch (input.get()) {
                case 1 -> Mutation.Kind.PUT; case 2 -> Mutation.Kind.DELETE;
                case 3 -> Mutation.Kind.MERGE; case 4 -> Mutation.Kind.DELETE_RANGE;
                default -> throw new IllegalArgumentException("Unknown mutation kind");
            };
            Mutation.ReturnMode mode = switch (input.get()) {
                case 0 -> Mutation.ReturnMode.NONE; case 1 -> Mutation.ReturnMode.PREVIOUS;
                case 2 -> Mutation.ReturnMode.CURRENT;
                default -> throw new IllegalArgumentException("Unknown return mode");
            };
            UUID column = getUuid(input);
            int keyLength = input.getInt();
            if (keyLength < 0 || keyLength > input.remaining() - 4) {
                throw new IllegalArgumentException("Invalid key length");
            }
            // Check both lengths and the operation shape before allocating either array.
            int valueLength = input.getInt(input.position() + keyLength);
            int available = input.remaining() - keyLength - 4;
            if (kind == Mutation.Kind.DELETE ? valueLength != -1 : valueLength < 0 || valueLength > available) {
                throw new IllegalArgumentException("Invalid value length");
            }
            byte[] key = new byte[keyLength];
            input.get(key);
            input.getInt();
            byte[] value = valueLength == -1 ? null : new byte[valueLength];
            if (value != null) input.get(value);
            mutations.add(new Mutation(kind, column, key, value, mode));
        }
        if (input.hasRemaining()) throw new IllegalArgumentException("Trailing command bytes");
        return new DistributedCommand(group, rangeEpoch, schema, operation, mutations);
    }

    static byte[] digest(DistributedCommand command) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(encode(command));
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError("SHA-256 unavailable", impossible);
        }
    }

    private static void require(ByteBuffer input, int count) {
        if (input.remaining() < count) throw new IllegalArgumentException("Truncated mutation");
    }

    private static void putUuid(ByteBuffer output, UUID uuid) {
        output.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
    }

    private static UUID getUuid(ByteBuffer input) {
        return new UUID(input.getLong(), input.getLong());
    }
}
