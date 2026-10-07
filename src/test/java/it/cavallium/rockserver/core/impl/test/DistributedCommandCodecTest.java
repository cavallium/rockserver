package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.DistributedCommand;
import it.cavallium.rockserver.core.common.distributed.DistributedCommandCodec;
import it.cavallium.rockserver.core.common.distributed.Mutation;
import it.cavallium.rockserver.core.common.distributed.OperationId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DistributedCommandCodecTest {
    private static final UUID GROUP = new UUID(1, 2);
    private static final UUID COLUMN = new UUID(3, 4);
    private static final OperationId OPERATION = new OperationId(new UUID(5, 6), 7, 8);
    private static final int HEADER_BYTES = 96;

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
        assertEquals(1, decoded.formatVersion());
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
        assertEquals(1, bytes.getInt());
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
        int[][] corruptions = {{0, 2}, {20, -1}, {76, -1}, {84, -1}, {92, 0}, {92, 257},
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
}
