package it.cavallium.rockserver.core.cluster;

import it.cavallium.buffer.Buf;

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class RockserverStorageKeyspace {

    public static final int FORMAT_VERSION = 1;
    public static final int PREFIX_BYTES = 4;

    private static final byte MAGIC_0 = 'R';
    private static final byte MAGIC_1 = 'K';

    private RockserverStorageKeyspace() {
    }

    public enum Namespace {
        USER_DATA(0x10),
        SCHEMA(0x20),
        MERGE_ARTIFACT(0x21),
        DEDUPE_RESPONSE(0x30),
        REPLICA_METADATA(0x31),
        DEDUPE_CLIENT_STATE(0x32),
        CDC_EVENT(0x40),
        ANTI_ENTROPY(0x50);

        private final int code;

        Namespace(int code) {
            this.code = code;
        }

        public int code() {
            return code;
        }

        private static Namespace fromCode(int code) {
            for (var namespace : values()) {
                if (namespace.code == code) {
                    return namespace;
                }
            }
            throw new IllegalArgumentException("Unknown rockserver storage key namespace: " + code);
        }
    }

    public static Buf namespacePrefix(Namespace namespace) {
        var requiredNamespace = requireArgument(namespace, "namespace");
        return key(requiredNamespace, 0, ignored -> {
        });
    }

    public static Buf userDataPrefix(long columnId) {
        requireNonNegative(columnId, "columnId");
        return key(Namespace.USER_DATA, Long.BYTES, buffer -> buffer.putLong(columnId));
    }

    public static Buf userDataColumnEndExclusive(long columnId) {
        requireNonNegative(columnId, "columnId");
        if (columnId == Long.MAX_VALUE) {
            return namespacePrefix(Namespace.SCHEMA);
        }
        return userDataPrefix(columnId + 1);
    }

    public static Buf userData(long columnId, Buf calculatedKey) {
        requireNonNegative(columnId, "columnId");
        var calculatedKeyBytes = requireBytes(calculatedKey, "calculatedKey");
        return key(Namespace.USER_DATA, Long.BYTES + calculatedKeyBytes.length, buffer -> {
            buffer.putLong(columnId);
            buffer.put(calculatedKeyBytes);
        });
    }

    public static Buf userDataCalculatedKey(Buf userDataKey, long expectedColumnId) {
        requireNonNegative(expectedColumnId, "expectedColumnId");
        requireMinimumPrefix(userDataKey);
        requireMagic(userDataKey);
        if (namespace(userDataKey) != Namespace.USER_DATA) {
            throw new IllegalArgumentException("Rockserver storage key is not a user-data key");
        }
        var keyBytes = userDataKey.toByteArray();
        if (keyBytes.length < PREFIX_BYTES + Long.BYTES) {
            throw new IllegalArgumentException("Rockserver user-data key is too short");
        }
        var columnId = ByteBuffer.wrap(keyBytes, PREFIX_BYTES, Long.BYTES).getLong();
        if (columnId != expectedColumnId) {
            throw new IllegalArgumentException("Rockserver user-data key column " + columnId
                    + " does not match expected column " + expectedColumnId);
        }
        return Buf.wrap(Arrays.copyOfRange(keyBytes, PREFIX_BYTES + Long.BYTES, keyBytes.length)).freeze();
    }

    public static Buf schema(long columnId, long schemaEpoch) {
        requireNonNegative(columnId, "columnId");
        requireNonNegative(schemaEpoch, "schemaEpoch");
        return key(Namespace.SCHEMA, 2 * Long.BYTES, buffer -> {
            buffer.putLong(columnId);
            buffer.putLong(schemaEpoch);
        });
    }

    public static Buf mergeArtifact(Buf artifactHash) {
        var artifactHashBytes = requireBytes(artifactHash, "artifactHash");
        return key(Namespace.MERGE_ARTIFACT, lengthPrefixedSize(artifactHashBytes), buffer ->
                writeLengthPrefixed(buffer, artifactHashBytes));
    }

    public static Buf dedupeResponse(ClientRequestId requestId) {
        var requiredRequestId = requireArgument(requestId, "requestId");
        return key(Namespace.DEDUPE_RESPONSE, 3 * Long.BYTES, buffer -> {
            var clientUuid = requiredRequestId.clientId().value();
            buffer.putLong(clientUuid.getMostSignificantBits());
            buffer.putLong(clientUuid.getLeastSignificantBits());
            buffer.putLong(requiredRequestId.sequence());
        });
    }

    public static Buf dedupeClientState(ClientId clientId) {
        var requiredClientId = requireArgument(clientId, "clientId");
        return key(Namespace.DEDUPE_CLIENT_STATE, 2 * Long.BYTES, buffer -> {
            var clientUuid = requiredClientId.value();
            buffer.putLong(clientUuid.getMostSignificantBits());
            buffer.putLong(clientUuid.getLeastSignificantBits());
        });
    }

    public static Buf lastAppliedIndex() {
        return key(Namespace.REPLICA_METADATA, Integer.BYTES, buffer -> buffer.putInt(1));
    }

    public static Buf cdcEvent(long appliedIndex, int eventOrdinal) {
        requireNonNegative(appliedIndex, "appliedIndex");
        requireEventOrdinal(eventOrdinal);
        return key(Namespace.CDC_EVENT, Long.BYTES + Integer.BYTES, buffer -> {
            buffer.putLong(appliedIndex);
            buffer.putInt(eventOrdinal);
        });
    }

    public static Buf antiEntropy(long metadataEpoch, Buf rangeStartInclusive, Buf rangeEndExclusive) {
        requireNonNegative(metadataEpoch, "metadataEpoch");
        var startBytes = requireBytes(rangeStartInclusive, "rangeStartInclusive");
        var endBytes = requireBytes(rangeEndExclusive, "rangeEndExclusive");
        return key(Namespace.ANTI_ENTROPY,
                Long.BYTES + lengthPrefixedSize(startBytes) + lengthPrefixedSize(endBytes),
                buffer -> {
                    buffer.putLong(metadataEpoch);
                    writeLengthPrefixed(buffer, startBytes);
                    writeLengthPrefixed(buffer, endBytes);
                });
    }

    public static int formatVersion(Buf key) {
        requireMinimumPrefix(key);
        requireMagic(key);
        return Byte.toUnsignedInt(key.getByte(2));
    }

    public static Namespace namespace(Buf key) {
        var version = formatVersion(key);
        if (version != FORMAT_VERSION) {
            throw new IllegalArgumentException("Unsupported rockserver storage key format version: " + version);
        }
        return Namespace.fromCode(Byte.toUnsignedInt(key.getByte(3)));
    }

    private static Buf key(Namespace namespace, int bodySize, KeyBodyWriter writer) {
        var bytes = new byte[PREFIX_BYTES + bodySize];
        var buffer = ByteBuffer.wrap(bytes);
        buffer.put(MAGIC_0);
        buffer.put(MAGIC_1);
        buffer.put((byte) FORMAT_VERSION);
        buffer.put((byte) namespace.code());
        writer.write(buffer);
        if (buffer.hasRemaining()) {
            throw new IllegalStateException("Rockserver storage key writer left " + buffer.remaining() + " bytes");
        }
        return Buf.wrap(bytes).freeze();
    }

    private static int lengthPrefixedSize(byte[] bytes) {
        return Integer.BYTES + bytes.length;
    }

    private static void writeLengthPrefixed(ByteBuffer buffer, byte[] bytes) {
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    private static byte[] requireBytes(Buf buf, String name) {
        var requiredBuf = requireArgument(buf, name);
        return requiredBuf.toByteArray();
    }

    private static void requireMinimumPrefix(Buf key) {
        var requiredKey = requireArgument(key, "key");
        if (requiredKey.size() < PREFIX_BYTES) {
            throw new IllegalArgumentException("Rockserver storage key is too short");
        }
    }

    private static void requireMagic(Buf key) {
        if (key.getByte(0) != MAGIC_0 || key.getByte(1) != MAGIC_1) {
            throw new IllegalArgumentException("Invalid rockserver storage key magic");
        }
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    private static void requireEventOrdinal(int eventOrdinal) {
        if (eventOrdinal < 0) {
            throw new IllegalArgumentException("eventOrdinal must be >= 0");
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    @FunctionalInterface
    private interface KeyBodyWriter {
        void write(ByteBuffer buffer);
    }
}
