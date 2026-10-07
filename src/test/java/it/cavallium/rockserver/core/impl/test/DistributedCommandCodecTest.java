package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.DistributedCommand;
import it.cavallium.rockserver.core.common.distributed.DistributedCommandCodec;
import it.cavallium.rockserver.core.common.distributed.Mutation;
import it.cavallium.rockserver.core.common.distributed.OperationId;
import it.cavallium.rockserver.core.common.distributed.KeyPredicate;
import it.cavallium.rockserver.core.common.distributed.CommandResult;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;

class DistributedCommandCodecTest {
    private static final UUID GROUP = new UUID(1, 2);
    private static final UUID COLUMN = new UUID(3, 4);
    private static final OperationId OPERATION = new OperationId(new UUID(5, 6), 7, 8);
    private static final int HEADER_BYTES = 100;

    private DistributedCommand command(List<Mutation> mutations) {
        return new DistributedCommand(GROUP, 9, new byte[32], OPERATION, mutations);
    }

    private Mutation put() {
        return new Mutation(Mutation.Kind.PUT, COLUMN, new byte[]{10}, new byte[]{11});
    }

    @Test
    void roundTripRetainsAllKindsAndNoncommutativeOperandOrder() {
        List<Mutation> mutations = List.of(put(),
                new Mutation(Mutation.Kind.MERGE, COLUMN, new byte[]{10}, new byte[]{1, 2}),
                new Mutation(Mutation.Kind.MERGE, COLUMN, new byte[]{10}, new byte[]{2, 1}),
                new Mutation(Mutation.Kind.DELETE, COLUMN, new byte[0], null),
                new Mutation(Mutation.Kind.DELETE_RANGE, COLUMN, new byte[]{0x7f}, new byte[]{(byte) 0x80}));
        DistributedCommand original = command(mutations);
        byte[] encoded = DistributedCommandCodec.encode(original);
        DistributedCommand decoded = DistributedCommandCodec.decode(encoded);
        assertEquals(2, decoded.formatVersion());
        assertTrue(decoded.predicates().isEmpty());
        assertEquals(GROUP, decoded.groupId());
        assertEquals(9, decoded.rangeEpoch());
        assertEquals(OPERATION, decoded.operationId());
        assertEquals(mutations, decoded.mutations());
        assertArrayEquals(encoded, DistributedCommandCodec.encode(decoded));
        assertArrayEquals(original.payloadDigest(), decoded.payloadDigest());
    }

    @Test
    void ownsInputAndOutputArraysAndMutationList() {
        byte[] key = {10};
        byte[] value = {11};
        byte[] schema = new byte[32];
        var mutations = new ArrayList<>(List.of(new Mutation(Mutation.Kind.PUT, COLUMN, key, value)));
        var command = new DistributedCommand(GROUP, 9, schema, OPERATION, mutations);
        byte[] before = DistributedCommandCodec.encode(command);
        key[0] = 99;
        value[0] = 99;
        schema[0] = 99;
        mutations.clear();
        command.schemaDigest()[0] = 99;
        command.mutations().getFirst().key()[0] = 99;
        command.mutations().getFirst().value()[0] = 99;
        command.payloadDigest()[0] = 99;
        assertThrows(UnsupportedOperationException.class, () -> command.mutations().clear());
        assertArrayEquals(before, DistributedCommandCodec.encode(command));
        var decoded = DistributedCommandCodec.decode(before);
        before[HEADER_BYTES + 22] = 99;
        assertEquals(10, decoded.mutations().getFirst().key()[0]);
    }

    @Test
    void canonicalDigestCoversMetadataAndOrderedPayload() throws Exception {
        var first = new Mutation(Mutation.Kind.MERGE, COLUMN, new byte[]{1}, new byte[]{2});
        var second = new Mutation(Mutation.Kind.MERGE, COLUMN, new byte[]{1}, new byte[]{3});
        var original = command(List.of(first, second));
        assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(DistributedCommandCodec.encode(original)),
                original.payloadDigest());
        assertFalse(Arrays.equals(original.payloadDigest(), command(List.of(second, first)).payloadDigest()));
        var changedOperation = new DistributedCommand(GROUP, 9, new byte[32],
                new OperationId(OPERATION.sessionId(), 7, 9), original.mutations());
        assertFalse(Arrays.equals(original.payloadDigest(), changedOperation.payloadDigest()));
        byte[] schema = new byte[32];
        schema[0] = 1;
        assertFalse(Arrays.equals(original.payloadDigest(),
                new DistributedCommand(GROUP, 9, schema, OPERATION, original.mutations()).payloadDigest()));
        assertFalse(Arrays.equals(original.payloadDigest(),
                new DistributedCommand(new UUID(1, 3), 9, new byte[32], OPERATION, original.mutations()).payloadDigest()));
        assertFalse(Arrays.equals(original.payloadDigest(),
                new DistributedCommand(GROUP, 10, new byte[32], OPERATION, original.mutations()).payloadDigest()));
        ByteBuffer bytes = ByteBuffer.wrap(DistributedCommandCodec.encode(command(List.of(put()))));
        assertEquals(2, bytes.getInt());
        assertEquals(1, bytes.getLong());
        assertEquals(2, bytes.getLong());
        assertEquals(9, bytes.getLong());
        assertEquals(1, bytes.get(HEADER_BYTES));
    }

    @Test
    void rejectsEveryTruncatedPrefixAndTrailingBytes() {
        byte[] encoded = DistributedCommandCodec.encode(command(List.of(put(), put())));
        for (int size = 0; size < encoded.length; size++) {
            byte[] truncated = Arrays.copyOf(encoded, size);
            assertThrows(IllegalArgumentException.class, () -> DistributedCommandCodec.decode(truncated));
        }
        assertThrows(IllegalArgumentException.class,
                () -> DistributedCommandCodec.decode(Arrays.copyOf(encoded, encoded.length + 1)));
    }

    @Test
    void rejectsUnknownTagsImpossibleLengthsAndInvalidMetadata() {
        byte[] original = DistributedCommandCodec.encode(command(List.of(put())));
        int[][] corruptions = {{0, 3}, {20, -1}, {76, -1}, {84, -1}, {92, 0}, {92, 257}, {96, -1}, {96, 257},
                {HEADER_BYTES + 18, -1}, {HEADER_BYTES + 18, Integer.MAX_VALUE},
                {HEADER_BYTES + 23, -2}, {HEADER_BYTES + 23, -1}, {HEADER_BYTES + 23, Integer.MAX_VALUE}};
        for (int[] corruption : corruptions) {
            byte[] bytes = original.clone();
            ByteBuffer.wrap(bytes).putInt(corruption[0], corruption[1]);
            assertThrows(IllegalArgumentException.class, () -> DistributedCommandCodec.decode(bytes),
                    "offset=" + corruption[0] + " value=" + corruption[1]);
        }
        byte[] unknownKind = original.clone();
        unknownKind[HEADER_BYTES] = 127;
        assertThrows(IllegalArgumentException.class, () -> DistributedCommandCodec.decode(unknownKind));
        byte[] deleteWithValue = original.clone();
        deleteWithValue[HEADER_BYTES] = 2;
        assertThrows(IllegalArgumentException.class, () -> DistributedCommandCodec.decode(deleteWithValue));
    }

    @Test
    void enforcesExactMutationAndEncodedByteCeilings() {
        var fullCount = command(Collections.nCopies(256, put()));
        assertEquals(256, DistributedCommandCodec.decode(DistributedCommandCodec.encode(fullCount)).mutations().size());
        assertThrows(IllegalArgumentException.class, () -> command(Collections.nCopies(257, put())));
        assertThrows(IllegalArgumentException.class, () -> command(List.of()));
        int valueBytes = DistributedCommandCodec.MAX_ENCODED_BYTES - HEADER_BYTES - 26;
        var atLimit = command(List.of(new Mutation(Mutation.Kind.PUT, COLUMN, new byte[0], new byte[valueBytes])));
        byte[] encoded = DistributedCommandCodec.encode(atLimit);
        assertEquals(DistributedCommandCodec.MAX_ENCODED_BYTES, encoded.length);
        assertArrayEquals(encoded, DistributedCommandCodec.encode(DistributedCommandCodec.decode(encoded)));
        assertThrows(IllegalArgumentException.class,
                () -> command(List.of(new Mutation(Mutation.Kind.PUT, COLUMN, new byte[0], new byte[valueBytes + 1]))));
        assertThrows(IllegalArgumentException.class,
                () -> DistributedCommandCodec.decode(new byte[DistributedCommandCodec.MAX_ENCODED_BYTES + 1]));
    }

    @Test
    void validatesMutationShapeAndUnsignedRangeBounds() {
        assertThrows(IllegalArgumentException.class, () -> new Mutation(Mutation.Kind.DELETE, COLUMN, new byte[0], new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> new Mutation(Mutation.Kind.MERGE, COLUMN, new byte[0], null));
        assertThrows(IllegalArgumentException.class, () -> new Mutation(Mutation.Kind.DELETE_RANGE, COLUMN,
                new byte[]{(byte) 0x80}, new byte[]{0x7f}));
        assertThrows(IllegalArgumentException.class, () -> new Mutation(Mutation.Kind.DELETE_RANGE, COLUMN,
                new byte[]{1}, new byte[]{1}));
        assertThrows(IllegalArgumentException.class, () -> new OperationId(GROUP, -1, 0));
        assertThrows(IllegalArgumentException.class, () -> new OperationId(GROUP, 0, -1));
        assertThrows(IllegalArgumentException.class, () -> new ApplyPosition(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> new ApplyPosition(0, -1));
        assertEquals(0, new ApplyPosition(1, 0).index());
        assertEquals(0, new ApplyPosition(0, 0).index());
        assertEquals(2, new ApplyPosition(1, 2).index());
        assertThrows(IllegalArgumentException.class,
                () -> new DistributedCommand(GROUP, 0, new byte[31], OPERATION, List.of(put())));
    }

    @Test
    void legacyGoldenBytesAndHashRemainExactWithoutRetryUpgrade() {
        // Exact committed v1 layout: 96-byte header, PUT/NONE tag, column, key and value.
        byte[] golden = HexFormat.of().parseHex(
                "00000001000000000000000100000000000000020000000000000009"
                        + "0000000000000000000000000000000000000000000000000000000000000000"
                        + "0000000000000005000000000000000600000000000000070000000000000008"
                        + "00000001" + "0100" + "0000000000000003" + "0000000000000004" + "000000010a000000010b");
        var legacy = DistributedCommandCodec.decode(golden);
        assertEquals(1, legacy.formatVersion());
        assertEquals(List.of(put()), legacy.mutations());
        assertTrue(legacy.predicates().isEmpty());
        assertArrayEquals(golden, DistributedCommandCodec.encode(legacy));
        assertEquals("14bafae7b26d174898fb65c1c5471e23bf81bc4e87b7e4f082b5988ac43bf8df",
                HexFormat.of().formatHex(legacy.payloadDigest()));
        assertEquals(2, command(legacy.mutations()).formatVersion());
        assertFalse(Arrays.equals(legacy.payloadDigest(), command(legacy.mutations()).payloadDigest()));
        // All legacy return/operation tags also survive decode and re-encode unchanged.
        var modern = command(List.of(
                new Mutation(Mutation.Kind.PUT, COLUMN, new byte[]{1}, new byte[]{2}, Mutation.ReturnMode.PREVIOUS),
                new Mutation(Mutation.Kind.MERGE, COLUMN, new byte[]{1}, new byte[]{3}, Mutation.ReturnMode.CURRENT),
                new Mutation(Mutation.Kind.DELETE, COLUMN, new byte[]{1}, null),
                new Mutation(Mutation.Kind.DELETE_RANGE, COLUMN, new byte[]{1}, new byte[]{2})));
        byte[] v2 = DistributedCommandCodec.encode(modern);
        byte[] v1 = new byte[v2.length - 4];
        System.arraycopy(v2, 0, v1, 0, 96);
        System.arraycopy(v2, 100, v1, 96, v2.length - 100);
        ByteBuffer.wrap(v1).putInt(1);
        var decoded = DistributedCommandCodec.decode(v1);
        assertEquals(modern.mutations(), decoded.mutations());
        assertArrayEquals(v1, DistributedCommandCodec.encode(decoded));
    }

    @Test
    void predicatesOwnArraysAndDistinguishAbsentFromPresentEmptyValues() {
        byte[] key = {10};
        byte[] expected = {11};
        var equals = new KeyPredicate(COLUMN, key, expected);
        var absent = new KeyPredicate(COLUMN, key, null);
        var empty = new KeyPredicate(COLUMN, key, new byte[0]);
        var predicates = new ArrayList<>(List.of(equals, absent, empty));
        var command = new DistributedCommand(GROUP, 9, new byte[32], OPERATION, List.of(put()), predicates);
        byte[] before = DistributedCommandCodec.encode(command);
        key[0] = 99;
        expected[0] = 99;
        predicates.clear();
        equals.key()[0] = 99;
        equals.expectedValue()[0] = 99;
        assertThrows(UnsupportedOperationException.class, () -> command.predicates().clear());
        assertArrayEquals(before, DistributedCommandCodec.encode(command));
        var decoded = DistributedCommandCodec.decode(before);
        assertEquals(command.predicates(), decoded.predicates());
        assertNull(decoded.predicates().get(1).expectedValue());
        assertArrayEquals(new byte[0], decoded.predicates().get(2).expectedValue());
        assertNotEquals(absent, empty);
        assertEquals(2, decoded.formatVersion());
        assertArrayEquals(before, DistributedCommandCodec.encode(decoded));
        int firstPredicateKey = HEADER_BYTES + 28 + 21;
        before[firstPredicateKey] = 99;
        assertArrayEquals(new byte[]{10}, decoded.predicates().getFirst().key());
    }

    @Test
    void predicatesAreOrderedAndEveryIdentityComponentChangesPayloadHash() {
        var absent = new KeyPredicate(COLUMN, new byte[]{10}, null);
        var expected = new KeyPredicate(COLUMN, new byte[]{11}, new byte[]{12});
        var baseline = new DistributedCommand(GROUP, 9, new byte[32], OPERATION, List.of(put()), List.of(absent, expected));
        var variants = List.of(
                List.of(expected, absent),
                List.of(new KeyPredicate(COLUMN, new byte[]{10}, new byte[0]), expected),
                List.of(new KeyPredicate(COLUMN, new byte[]{9}, null), expected),
                List.of(absent, new KeyPredicate(COLUMN, new byte[]{11}, new byte[]{13})),
                List.of(new KeyPredicate(new UUID(3, 5), new byte[]{10}, null), expected));
        for (List<KeyPredicate> variant : variants) {
            var changed = new DistributedCommand(GROUP, 9, new byte[32], OPERATION, List.of(put()), variant);
            assertFalse(Arrays.equals(baseline.payloadDigest(), changed.payloadDigest()));
        }
    }

    @Test
    void conditionalEnvelopeBoundsCountAllItemsAndFullEncoding() {
        var predicate = new KeyPredicate(COLUMN, new byte[0], null);
        var full = new DistributedCommand(GROUP, 9, new byte[32], OPERATION,
                List.of(put()), Collections.nCopies(255, predicate));
        assertEquals(255, DistributedCommandCodec.decode(DistributedCommandCodec.encode(full)).predicates().size());
        assertThrows(IllegalArgumentException.class, () -> new DistributedCommand(GROUP, 9, new byte[32], OPERATION,
                List.of(put()), Collections.nCopies(256, predicate)));
        int expectedBytes = DistributedCommandCodec.MAX_ENCODED_BYTES - HEADER_BYTES - 28 - 25;
        var atLimit = new DistributedCommand(GROUP, 9, new byte[32], OPERATION, List.of(put()),
                List.of(new KeyPredicate(COLUMN, new byte[0], new byte[expectedBytes])));
        byte[] encoded = DistributedCommandCodec.encode(atLimit);
        assertEquals(DistributedCommandCodec.MAX_ENCODED_BYTES, encoded.length);
        assertArrayEquals(encoded, DistributedCommandCodec.encode(DistributedCommandCodec.decode(encoded)));
        assertThrows(IllegalArgumentException.class, () -> new DistributedCommand(GROUP, 9, new byte[32], OPERATION,
                List.of(put()), List.of(new KeyPredicate(COLUMN, new byte[0], new byte[expectedBytes + 1]))));
        assertThrows(IllegalArgumentException.class,
                () -> new KeyPredicate(COLUMN, new byte[0], new byte[DistributedCommandCodec.MAX_ENCODED_BYTES + 1]));
    }

    @Test
    void conditionalDecodeRejectsEveryTruncationInvalidLengthsCountsAndUnknownTags() {
        var command = new DistributedCommand(GROUP, 9, new byte[32], OPERATION, List.of(put()),
                List.of(new KeyPredicate(COLUMN, new byte[]{10}, new byte[]{11})));
        byte[] encoded = DistributedCommandCodec.encode(command);
        for (int length = 0; length < encoded.length; length++) {
            byte[] truncated = Arrays.copyOf(encoded, length);
            assertThrows(IllegalArgumentException.class, () -> DistributedCommandCodec.decode(truncated));
        }
        int predicate = HEADER_BYTES + 28;
        int[][] badInts = {{96, -1}, {96, 256}, {predicate + 17, -1}, {predicate + 17, Integer.MAX_VALUE},
                {predicate + 22, -2}, {predicate + 22, Integer.MAX_VALUE}};
        for (int[] bad : badInts) {
            byte[] changed = encoded.clone();
            ByteBuffer.wrap(changed).putInt(bad[0], bad[1]);
            assertThrows(IllegalArgumentException.class, () -> DistributedCommandCodec.decode(changed));
        }
        byte[] unknown = encoded.clone();
        unknown[predicate] = 2;
        assertThrows(IllegalArgumentException.class, () -> DistributedCommandCodec.decode(unknown));
        assertThrows(IllegalArgumentException.class,
                () -> DistributedCommandCodec.decode(Arrays.copyOf(encoded, encoded.length + 1)));
    }

    @Test void acknowledgementsPreserveLegacyIdentityAndRequireEarlierOwnSessionSequences() {
        var legacy = command(List.of(put()));
        assertEquals("0b4d9745c677d17fcf5f59a37bdfcabed9b3dedef3e19178b53ccd90818647e7",
                HexFormat.of().formatHex(legacy.payloadDigest()));
        assertEquals(2, legacy.formatVersion()); assertEquals(-1, legacy.acknowledgedThrough());
        assertArrayEquals(DistributedCommandCodec.encode(legacy),
                DistributedCommandCodec.encode(DistributedCommandCodec.decode(DistributedCommandCodec.encode(legacy))));
        var ack = new DistributedCommand(GROUP, 9, new byte[32], OPERATION, List.of(), List.of(), 7);
        var encoded = DistributedCommandCodec.encode(ack);
        var decoded = DistributedCommandCodec.decode(encoded);
        assertEquals(3, decoded.formatVersion()); assertEquals(7, decoded.acknowledgedThrough());
        assertEquals(OPERATION, decoded.operationId()); assertTrue(decoded.mutations().isEmpty());
        assertArrayEquals(encoded, DistributedCommandCodec.encode(decoded));
        assertArrayEquals(ack.payloadDigest(), decoded.payloadDigest());
        var earlier = new DistributedCommand(GROUP, 9, new byte[32], OPERATION, List.of(), List.of(), 6);
        assertFalse(Arrays.equals(ack.payloadDigest(), earlier.payloadDigest()));
        for (long floor : new long[]{-1, 8, Long.MAX_VALUE}) {
            assertThrows(IllegalArgumentException.class, () -> new DistributedCommand(GROUP,9,new byte[32],OPERATION,List.of(),List.of(),floor));
        }
        assertThrows(IllegalArgumentException.class, () -> new DistributedCommand(GROUP,9,new byte[32],OPERATION,
                List.of(),List.of(new KeyPredicate(COLUMN,new byte[0],null)),7));
        for (int length=0; length<encoded.length; length++) {
            var truncated = Arrays.copyOf(encoded,length);
            assertThrows(IllegalArgumentException.class, () -> DistributedCommandCodec.decode(truncated));
        }
        assertThrows(IllegalArgumentException.class, () -> DistributedCommandCodec.decode(Arrays.copyOf(encoded,encoded.length+1)));
    }

    @Test void acknowledgementHeaderConsumesTheExistingEncodedByteBudget() {
        int payload = DistributedCommandCodec.MAX_ENCODED_BYTES - 108 - 26;
        var exact = new DistributedCommand(GROUP,9,new byte[32],OPERATION,
                List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[0],new byte[payload])),List.of(),7);
        var encoded = DistributedCommandCodec.encode(exact);
        assertEquals(DistributedCommandCodec.MAX_ENCODED_BYTES,encoded.length);
        assertArrayEquals(encoded,DistributedCommandCodec.encode(DistributedCommandCodec.decode(encoded)));
        assertThrows(IllegalArgumentException.class, () -> new DistributedCommand(GROUP,9,new byte[32],OPERATION,
                List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[0],new byte[payload+1])),List.of(),7));
    }

    @Test
    void conflictResultHasExplicitCodeAndCannotCarryValues() {
        for (var status : CommandResult.Status.values()) {
            var result = new CommandResult(status, List.of());
            int code = switch (status) { case SUCCESS -> 0; case RESULT_TOO_LARGE -> 1; case CONFLICT -> 2; case SESSION_EXPIRED -> 3; };
            assertArrayEquals(ByteBuffer.allocate(8).putInt(code).putInt(0).array(), result.encode());
            assertEquals(status, CommandResult.decode(result.encode()).status());
        }
        assertThrows(IllegalArgumentException.class,
                () -> new CommandResult(CommandResult.Status.CONFLICT, List.of(new byte[0])));
        assertThrows(IllegalArgumentException.class,
                () -> CommandResult.decode(ByteBuffer.allocate(12).putInt(2).putInt(1).putInt(-1).array()));
        assertThrows(IllegalArgumentException.class,
                () -> CommandResult.decode(ByteBuffer.allocate(8).putInt(4).putInt(0).array()));
    }
}
