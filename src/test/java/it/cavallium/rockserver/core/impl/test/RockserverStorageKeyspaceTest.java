package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.RockserverStorageKeyspace;
import it.cavallium.rockserver.core.cluster.RockserverStorageKeyspace.Namespace;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

class RockserverStorageKeyspaceTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var requestId = new ClientRequestId(new ClientId(new UUID(0, 1)), 1);

        assertIllegalArgument("namespace must not be null", () -> RockserverStorageKeyspace.namespacePrefix(null));
        assertIllegalArgument("calculatedKey must not be null", () -> RockserverStorageKeyspace.userData(1, null));
        assertIllegalArgument("key must not be null", () -> RockserverStorageKeyspace.userDataCalculatedKey(null, 1));
        assertIllegalArgument("artifactHash must not be null", () -> RockserverStorageKeyspace.mergeArtifact(null));
        assertIllegalArgument("requestId must not be null", () -> RockserverStorageKeyspace.dedupeResponse(null));
        assertIllegalArgument("clientId must not be null", () -> RockserverStorageKeyspace.dedupeClientState(null));
        assertIllegalArgument("rangeStartInclusive must not be null",
                () -> RockserverStorageKeyspace.antiEntropy(1, null, buf()));
        assertIllegalArgument("rangeEndExclusive must not be null",
                () -> RockserverStorageKeyspace.antiEntropy(1, buf(), null));
        assertIllegalArgument("key must not be null", () -> RockserverStorageKeyspace.formatVersion(null));
        assertIllegalArgument("key must not be null", () -> RockserverStorageKeyspace.namespace(null));

        assertEquals(Namespace.DEDUPE_RESPONSE, RockserverStorageKeyspace.namespace(
                RockserverStorageKeyspace.dedupeResponse(requestId)));
    }

    @Test
    void namespaceCodesAreStable() {
        assertEquals(0x10, Namespace.USER_DATA.code());
        assertEquals(0x20, Namespace.SCHEMA.code());
        assertEquals(0x21, Namespace.MERGE_ARTIFACT.code());
        assertEquals(0x30, Namespace.DEDUPE_RESPONSE.code());
        assertEquals(0x31, Namespace.REPLICA_METADATA.code());
        assertEquals(0x32, Namespace.DEDUPE_CLIENT_STATE.code());
        assertEquals(0x40, Namespace.CDC_EVENT.code());
        assertEquals(0x50, Namespace.ANTI_ENTROPY.code());
    }

    @Test
    void encodesUserDataWithVersionedColumnPrefix() {
        var calculatedKey = buf(0x01, 0xff);

        var prefix = RockserverStorageKeyspace.userDataPrefix(42);
        var key = RockserverStorageKeyspace.userData(42, calculatedKey);
        var nextColumn = RockserverStorageKeyspace.userDataColumnEndExclusive(42);

        assertEquals(RockserverStorageKeyspace.FORMAT_VERSION, RockserverStorageKeyspace.formatVersion(key));
        assertEquals(Namespace.USER_DATA, RockserverStorageKeyspace.namespace(key));
        assertEquals(calculatedKey, RockserverStorageKeyspace.userDataCalculatedKey(key, 42));
        assertArrayEquals(expected(Namespace.USER_DATA, Long.BYTES, buffer -> buffer.putLong(42)),
                prefix.toByteArray());
        assertArrayEquals(expected(Namespace.USER_DATA, Long.BYTES, buffer -> buffer.putLong(43)),
                nextColumn.toByteArray());
        assertArrayEquals(expected(Namespace.USER_DATA, Long.BYTES + calculatedKey.size(), buffer -> {
            buffer.putLong(42);
            buffer.put(calculatedKey.toByteArray());
        }), key.toByteArray());
    }

    @Test
    void userDataKeysSortByColumnThenCalculatedKey() {
        var column1Low = RockserverStorageKeyspace.userData(1, buf(0x01));
        var column1High = RockserverStorageKeyspace.userData(1, buf(0xff));
        var column2Low = RockserverStorageKeyspace.userData(2, buf(0x00));

        var keys = new ArrayList<>(List.of(column2Low, column1High, column1Low));

        keys.sort((left, right) -> Arrays.compareUnsigned(left.toByteArray(), right.toByteArray()));

        assertArrayEquals(column1Low.toByteArray(), keys.get(0).toByteArray());
        assertArrayEquals(column1High.toByteArray(), keys.get(1).toByteArray());
        assertArrayEquals(column2Low.toByteArray(), keys.get(2).toByteArray());
    }

    @Test
    void allInternalFamiliesHaveDistinctVersionedNamespaces() {
        var requestId = new ClientRequestId(new ClientId(new UUID(0x0102030405060708L, 0x1112131415161718L)), 9);

        assertEquals(Namespace.SCHEMA, RockserverStorageKeyspace.namespace(
                RockserverStorageKeyspace.schema(42, 7)));
        assertEquals(Namespace.MERGE_ARTIFACT, RockserverStorageKeyspace.namespace(
                RockserverStorageKeyspace.mergeArtifact(buf(0xaa, 0xbb))));
        assertEquals(Namespace.DEDUPE_RESPONSE, RockserverStorageKeyspace.namespace(
                RockserverStorageKeyspace.dedupeResponse(requestId)));
        assertEquals(Namespace.DEDUPE_CLIENT_STATE, RockserverStorageKeyspace.namespace(
                RockserverStorageKeyspace.dedupeClientState(requestId.clientId())));
        assertEquals(Namespace.REPLICA_METADATA, RockserverStorageKeyspace.namespace(
                RockserverStorageKeyspace.lastAppliedIndex()));
        assertEquals(Namespace.CDC_EVENT, RockserverStorageKeyspace.namespace(
                RockserverStorageKeyspace.cdcEvent(100, 3)));
        assertEquals(Namespace.ANTI_ENTROPY, RockserverStorageKeyspace.namespace(
                RockserverStorageKeyspace.antiEntropy(8, buf(0x01), buf(0x02))));
    }

    @Test
    void encodesClientDedupeStateKeyByClientId() {
        var clientId = new ClientId(new UUID(0x0102030405060708L, 0x1112131415161718L));

        assertArrayEquals(expected(Namespace.DEDUPE_CLIENT_STATE, 2 * Long.BYTES, buffer -> {
            buffer.putLong(0x0102030405060708L);
            buffer.putLong(0x1112131415161718L);
        }), RockserverStorageKeyspace.dedupeClientState(clientId).toByteArray());
    }

    @Test
    void encodesLastAppliedReplicaMetadataKey() {
        assertArrayEquals(expected(Namespace.REPLICA_METADATA, Integer.BYTES, buffer -> buffer.putInt(1)),
                RockserverStorageKeyspace.lastAppliedIndex().toByteArray());
    }

    @Test
    void antiEntropyKeyLengthPrefixesVariableRangeBounds() {
        var key = RockserverStorageKeyspace.antiEntropy(7, buf(0x01, 0x02), buf(0x03));

        assertArrayEquals(expected(Namespace.ANTI_ENTROPY,
                Long.BYTES + Integer.BYTES + 2 + Integer.BYTES + 1,
                buffer -> {
                    buffer.putLong(7);
                    buffer.putInt(2);
                    buffer.put(new byte[]{0x01, 0x02});
                    buffer.putInt(1);
                    buffer.put((byte) 0x03);
                }), key.toByteArray());
    }

    @Test
    void rejectsMalformedOrUnsupportedPrefixes() {
        assertThrows(IllegalArgumentException.class, () -> RockserverStorageKeyspace.formatVersion(buf(0x52)));
        assertThrows(IllegalArgumentException.class,
                () -> RockserverStorageKeyspace.namespace(buf(0x00, 0x4b, 0x01, 0x10)));

        var unsupportedVersion = buf(0x52, 0x4b, 0x02, 0x10);

        assertEquals(2, RockserverStorageKeyspace.formatVersion(unsupportedVersion));
        assertThrows(IllegalArgumentException.class, () -> RockserverStorageKeyspace.namespace(unsupportedVersion));
    }

    @Test
    void rejectsNegativeIdsAndOrdinals() {
        assertThrows(IllegalArgumentException.class, () -> RockserverStorageKeyspace.userData(-1, buf()));
        assertThrows(IllegalArgumentException.class, () -> RockserverStorageKeyspace.schema(1, -1));
        assertThrows(IllegalArgumentException.class, () -> RockserverStorageKeyspace.cdcEvent(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> RockserverStorageKeyspace.cdcEvent(1, -1));
        assertThrows(IllegalArgumentException.class, () -> RockserverStorageKeyspace.antiEntropy(-1, buf(), buf()));
    }

    private static void assertIllegalArgument(String expectedMessage, org.junit.jupiter.api.function.Executable action) {
        var failure = assertThrows(IllegalArgumentException.class, action);

        assertEquals(expectedMessage, failure.getMessage());
    }

    private static byte[] expected(Namespace namespace, int bodySize, Consumer<ByteBuffer> bodyWriter) {
        var buffer = ByteBuffer.allocate(RockserverStorageKeyspace.PREFIX_BYTES + bodySize);
        buffer.put((byte) 0x52);
        buffer.put((byte) 0x4b);
        buffer.put((byte) RockserverStorageKeyspace.FORMAT_VERSION);
        buffer.put((byte) namespace.code());
        bodyWriter.accept(buffer);
        return buffer.array();
    }

    private static Buf buf(int... values) {
        var bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return Buf.wrap(bytes);
    }
}
