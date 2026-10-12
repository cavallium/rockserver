package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class RangeDirectoryCodecTest {
    private static final UUID CLUSTER = new UUID(1, 1), DATABASE = new UUID(1, 2), DOMAIN = new UUID(1, 3);
    private static final UUID A = new UUID(2, 1), B = new UUID(2, 2);
    private byte[] key(int n) { return new byte[]{(byte) n}; }
    private ReplicaDescriptor replica(int n, ReplicaDescriptor.Role role) {
        return new ReplicaDescriptor(new UUID(3, n), new UUID(4, n), new UUID(5, n), "n" + n, "127.0.0.1:" + (8000 + n), role);
    }
    private List<ReplicaDescriptor> replicas() {
        return List.of(replica(3, ReplicaDescriptor.Role.VOTER), replica(1, ReplicaDescriptor.Role.VOTER), replica(2, ReplicaDescriptor.Role.VOTER));
    }
    private RangeDescriptor range(int id, byte[] lower, byte[] upper, RangeDescriptor.TopologyState state) {
        return new RangeDescriptor(new UUID(6, id), new UUID(7, id), 8, 9, lower, upper, state, replicas());
    }
    private RangeDirectory directory(int prefix, List<RangeDescriptor> ranges) {
        return new RangeDirectory(CLUSTER, DATABASE, DOMAIN, 10, new byte[32], 11, Set.of(A, B), prefix, ranges);
    }
    private RangeDirectory standard() {
        return directory(0, List.of(range(1, null, key(0x80), RangeDescriptor.TopologyState.ACTIVE),
                range(2, key(0x80), key(0xf0), RangeDescriptor.TopologyState.ACTIVE),
                range(3, key(0xf0), null, RangeDescriptor.TopologyState.ACTIVE)));
    }

    @Test
    void canonicalRoundTripOwnsArraysAndCollectionsAndHashesGeneration() throws Exception {
        byte[] schema = new byte[32], boundary = key(0x80);
        var source = new ArrayList<>(List.of(range(2, boundary, null, RangeDescriptor.TopologyState.ACTIVE),
                range(1, null, boundary, RangeDescriptor.TopologyState.ACTIVE)));
        var columns = new HashSet<>(Set.of(A, B));
        var directory = new RangeDirectory(CLUSTER, DATABASE, DOMAIN, 10, schema, 11, columns, 0, source);
        byte[] encoded = RangeDirectoryCodec.encode(directory);
        schema[0] = 99;
        boundary[0] = 99;
        source.clear();
        columns.clear();
        directory.schemaDigest()[0] = 99;
        directory.payloadDigest()[0] ^= 1;
        directory.ranges().getFirst().upperExclusive()[0] = 99;
        assertThrows(UnsupportedOperationException.class, () -> directory.ranges().clear());
        assertThrows(UnsupportedOperationException.class, () -> directory.columns().clear());
        assertThrows(UnsupportedOperationException.class, () -> directory.ranges().getFirst().replicas().clear());
        assertArrayEquals(encoded, RangeDirectoryCodec.encode(directory));
        assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(encoded), directory.payloadDigest());
        var decoded = RangeDirectoryCodec.decode(encoded);
        assertEquals(CLUSTER, decoded.clusterId());
        assertEquals(DATABASE, decoded.databaseId());
        assertEquals(DOMAIN, decoded.domainId());
        assertEquals(10, decoded.schemaEpoch());
        assertEquals(11, decoded.generation());
        assertEquals(directory.ranges(), decoded.ranges());
        assertArrayEquals(encoded, RangeDirectoryCodec.encode(decoded));
        assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(encoded), decoded.payloadDigest());
        var next = new RangeDirectory(CLUSTER, DATABASE, DOMAIN, 10, new byte[32], 12, Set.of(A, B), 0, directory.ranges());
        assertFalse(Arrays.equals(directory.payloadDigest(), next.payloadDigest()));
    }

    @Test
    void routesSignedLogicalIdBytesInUnsignedPhysicalOrder() {
        byte[] zero = ByteBuffer.allocate(8).putLong(0).array();
        byte[] negativeMinimum = ByteBuffer.allocate(8).putLong(Long.MIN_VALUE).array();
        byte[] negativeOne = ByteBuffer.allocate(8).putLong(-1).array();
        var directory = directory(0, List.of(range(1, null, negativeMinimum, RangeDescriptor.TopologyState.ACTIVE),
                range(2, negativeMinimum, negativeOne, RangeDescriptor.TopologyState.ACTIVE),
                range(3, negativeOne, null, RangeDescriptor.TopologyState.ACTIVE)));
        assertEquals(new UUID(6, 1), directory.route(A, zero).rangeId());
        assertEquals(new UUID(6, 2), directory.route(A, negativeMinimum).rangeId());
        assertEquals(new UUID(6, 3), directory.route(A, negativeOne).rangeId());
        assertThrows(IllegalArgumentException.class, () -> directory.route(UUID.randomUUID(), zero));
        assertThrows(NullPointerException.class, () -> directory.route(A, null));
    }

    @Test
    void fixedPrefixColocatesColumnsAndKeepsExclusiveUpperSuffixCandidate() {
        var directory = directory(1, standard().ranges());
        assertEquals(directory.route(A, new byte[]{(byte) 0x80, 0}), directory.route(B, new byte[]{(byte) 0x80, (byte) 0xff}));
        assertThrows(IllegalArgumentException.class, () -> directory.route(A, new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> directory.intersect(A, new byte[0], new byte[0], false));
        assertThrows(IllegalArgumentException.class, () -> directory(2, standard().ranges()));
        assertEquals(List.of(standard().ranges().get(1)), directory.intersect(A,
                new byte[]{(byte) 0x80, 1}, new byte[]{(byte) 0x80, 2}, false));
        assertEquals(2, directory.intersect(A, key(0x7f), new byte[]{(byte) 0x80, 1}, false).size());
        assertEquals(1, directory.intersect(A, key(0x7f), key(0x80), false).size());
    }

    @Test
    void intervalTraversalIsOrderedForwardAndReverseAndRejectsInactiveRoutes() {
        var directory = standard();
        assertEquals(directory.ranges().subList(0, 2), directory.intersect(A, key(0x7f), key(0xf0), false));
        assertEquals(List.of(directory.ranges().get(1), directory.ranges().get(0)), directory.intersect(A, key(0x7f), key(0xf0), true));
        assertEquals(directory.ranges(), directory.intersect(A, null, null, false));
        assertTrue(directory.intersect(A, key(1), key(1), false).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> directory.intersect(A, key(2), key(1), false));
        for (var state : List.of(RangeDescriptor.TopologyState.FENCED, RangeDescriptor.TopologyState.RETIRED)) {
            var inactive = directory(0, List.of(range(1, null, null, state)));
            assertThrows(IllegalStateException.class, () -> inactive.route(A, key(1)));
            assertThrows(IllegalStateException.class, () -> inactive.intersect(A, null, null, false));
            assertEquals(state, RangeDirectoryCodec.decode(RangeDirectoryCodec.encode(inactive)).ranges().getFirst().topologyState());
        }
    }

    @Test
    void coverageRejectsGapsOverlapAndRepeatedIdentitiesWithoutClaimingBucketPolicy() {
        assertThrows(IllegalArgumentException.class, () -> directory(0, List.of(range(1, key(1), null, RangeDescriptor.TopologyState.ACTIVE))));
        assertThrows(IllegalArgumentException.class, () -> directory(0, List.of(range(1, null, key(1), RangeDescriptor.TopologyState.ACTIVE))));
        assertThrows(IllegalArgumentException.class, () -> directory(0, List.of(range(1, null, key(1), RangeDescriptor.TopologyState.ACTIVE),
                range(2, key(2), null, RangeDescriptor.TopologyState.ACTIVE))));
        assertThrows(IllegalArgumentException.class, () -> directory(0, List.of(range(1, null, key(2), RangeDescriptor.TopologyState.ACTIVE),
                range(2, key(1), null, RangeDescriptor.TopologyState.ACTIVE))));
        assertThrows(IllegalArgumentException.class, () -> directory(0, List.of(range(1, null, key(1), RangeDescriptor.TopologyState.ACTIVE),
                range(1, key(1), null, RangeDescriptor.TopologyState.ACTIVE))));
        // Full-key routing accepts a supplied byte boundary; schema topology policy must authorize it separately.
        var opaqueBoundary = new byte[]{10, 1};
        assertDoesNotThrow(() -> directory(0, List.of(range(1, null, opaqueBoundary, RangeDescriptor.TopologyState.ACTIVE),
                range(2, opaqueBoundary, null, RangeDescriptor.TopologyState.ACTIVE))));
    }

    @Test
    void replicaLimitsAndCredentialFreeAsciiEndpointsAreValidated() {
        var sixteen = new ArrayList<ReplicaDescriptor>();
        for (int i = 1; i <= 16; i++) sixteen.add(replica(i, i <= 5 ? ReplicaDescriptor.Role.VOTER : ReplicaDescriptor.Role.LISTENER));
        var range = new RangeDescriptor(new UUID(6, 1), new UUID(7, 1), 0, 0, null, null, RangeDescriptor.TopologyState.ACTIVE, sixteen);
        assertEquals(16, RangeDirectoryCodec.decode(RangeDirectoryCodec.encode(directory(0, List.of(range)))).ranges().getFirst().replicas().size());
        sixteen.add(replica(17, ReplicaDescriptor.Role.LISTENER));
        assertThrows(IllegalArgumentException.class, () -> new RangeDescriptor(new UUID(6, 1), new UUID(7, 1), 0, 0,
                null, null, RangeDescriptor.TopologyState.ACTIVE, sixteen));
        assertThrows(IllegalArgumentException.class, () -> new RangeDescriptor(new UUID(6, 1), new UUID(7, 1), 0, 0,
                null, null, RangeDescriptor.TopologyState.ACTIVE, Collections.nCopies(3, replica(1, ReplicaDescriptor.Role.VOTER))));
        var reference = replica(1, ReplicaDescriptor.Role.VOTER);
        for (String address : List.of("user:password@host:1234", "https://host:1234", "host:65536", "höst:1234", "host:0")) {
            assertThrows(IllegalArgumentException.class, () -> new ReplicaDescriptor(reference.nodeId(), reference.nodeIncarnation(),
                    reference.replicaId(), reference.peerId(), address, reference.role()));
        }
    }

    @Test
    void boundedCodecRejectsEveryTruncationTrailingBytesAndMalformedTagsAndLengths() {
        byte[] encoded = RangeDirectoryCodec.encode(directory(0, List.of(range(1, null, null, RangeDescriptor.TopologyState.ACTIVE))));
        for (int length = 0; length < encoded.length; length++) {
            byte[] truncated = Arrays.copyOf(encoded, length);
            assertThrows(IllegalArgumentException.class, () -> RangeDirectoryCodec.decode(truncated));
        }
        assertThrows(IllegalArgumentException.class, () -> RangeDirectoryCodec.decode(Arrays.copyOf(encoded, encoded.length + 1)));
        assertThrows(IllegalArgumentException.class, () -> RangeDirectoryCodec.decode(new byte[RangeDirectoryCodec.MAX_ENCODED_BYTES + 1]));
        // Fixed header has two columns, then one range with null bounds and three replicas.
        int range = 144, state = range + 56, replica = range + 61;
        int[][] badInts = {{0, 2}, {104, -1}, {104, 257}, {140, 1025}, {range + 48, -2}, {range + 52, Integer.MAX_VALUE},
                {state + 1, 17}, {replica + 49, Integer.MAX_VALUE}};
        for (int[] bad : badInts) {
            byte[] changed = encoded.clone();
            ByteBuffer.wrap(changed).putInt(bad[0], bad[1]);
            assertThrows(IllegalArgumentException.class, () -> RangeDirectoryCodec.decode(changed), "offset=" + bad[0]);
        }
        byte[] unknownState = encoded.clone(); unknownState[state] = 127;
        assertThrows(IllegalArgumentException.class, () -> RangeDirectoryCodec.decode(unknownState));
        byte[] unknownRole = encoded.clone(); unknownRole[replica + 48] = 127;
        assertThrows(IllegalArgumentException.class, () -> RangeDirectoryCodec.decode(unknownRole));
        byte[] columnsOutOfOrder = encoded.clone();
        byte[] first = Arrays.copyOfRange(encoded, 108, 124);
        System.arraycopy(encoded, 124, columnsOutOfOrder, 108, 16);
        System.arraycopy(first, 0, columnsOutOfOrder, 124, 16);
        assertThrows(IllegalArgumentException.class, () -> RangeDirectoryCodec.decode(columnsOutOfOrder));
    }

    @Test
    void directoryEnforcesRangeColumnAndTotalByteCeilings() {
        var columns = new HashSet<UUID>();
        for (int i = 0; i < 256; i++) columns.add(new UUID(9, i));
        assertDoesNotThrow(() -> new RangeDirectory(CLUSTER, DATABASE, DOMAIN, 0, new byte[32], 0, columns, 0,
                List.of(range(1, null, null, RangeDescriptor.TopologyState.ACTIVE))));
        columns.add(new UUID(9, 256));
        assertThrows(IllegalArgumentException.class, () -> new RangeDirectory(CLUSTER, DATABASE, DOMAIN, 0, new byte[32], 0, columns, 0,
                List.of(range(1, null, null, RangeDescriptor.TopologyState.ACTIVE))));
        var maximumRanges = new ArrayList<RangeDescriptor>();
        for (int i = 0; i < 1024; i++) {
            byte[] lower = i == 0 ? null : ByteBuffer.allocate(4).putInt(i).array();
            byte[] upper = i == 1023 ? null : ByteBuffer.allocate(4).putInt(i + 1).array();
            maximumRanges.add(range(i, lower, upper, RangeDescriptor.TopologyState.ACTIVE));
        }
        var atCountLimit = directory(0, maximumRanges);
        assertEquals(1024, RangeDirectoryCodec.decode(RangeDirectoryCodec.encode(atCountLimit)).ranges().size());
        int baseBytes = RangeDirectoryCodec.encode(directory(0, List.of(range(1, null, new byte[0], RangeDescriptor.TopologyState.ACTIVE),
                range(2, new byte[0], null, RangeDescriptor.TopologyState.ACTIVE)))).length;
        byte[] exactBoundary = new byte[(RangeDirectoryCodec.MAX_ENCODED_BYTES - baseBytes) / 2];
        var atByteLimit = directory(0, List.of(range(1, null, exactBoundary, RangeDescriptor.TopologyState.ACTIVE),
                range(2, exactBoundary, null, RangeDescriptor.TopologyState.ACTIVE)));
        byte[] exactEncoding = RangeDirectoryCodec.encode(atByteLimit);
        assertEquals(RangeDirectoryCodec.MAX_ENCODED_BYTES, exactEncoding.length);
        assertArrayEquals(exactEncoding, RangeDirectoryCodec.encode(RangeDirectoryCodec.decode(exactEncoding)));
        byte[] hugeBoundary = new byte[RangeDirectoryCodec.MAX_ENCODED_BYTES / 2];
        assertThrows(IllegalArgumentException.class, () -> directory(0, List.of(range(1, null, hugeBoundary, RangeDescriptor.TopologyState.ACTIVE),
                range(2, hugeBoundary, null, RangeDescriptor.TopologyState.ACTIVE))));
        assertThrows(IllegalArgumentException.class, () -> directory(0,
                Collections.nCopies(1025, range(1, null, null, RangeDescriptor.TopologyState.ACTIVE))));
    }
}
