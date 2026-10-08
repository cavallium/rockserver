package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.PhysicalRangePage;
import it.cavallium.rockserver.core.common.distributed.TransactionScope;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ScopedTransactionRpcTest {
    private static final UUID OWNER = new UUID(1, 2), ID = new UUID(3, 4), COLUMN = new UUID(5, 6);

    @Test void framedKindsAreExplicitAndBodiesAreOwned() {
        for (int kind : new int[]{ScopedTransactionRpc.POINT, ScopedTransactionRpc.STATE, ScopedTransactionRpc.OPEN,
                ScopedTransactionRpc.GET, ScopedTransactionRpc.PAGE, ScopedTransactionRpc.PLAN, ScopedTransactionRpc.CLOSE, ScopedTransactionRpc.STAGE}) {
            byte[] body = {1, 2, 3};
            var packet = new ScopedTransactionRpc.Packet(kind, body); body[0] = 9;
            assertArrayEquals(new byte[]{1, 2, 3}, packet.body()); packet.body()[0] = 9;
            byte[] frame = ScopedTransactionRpc.frame(kind, packet.body());
            var decoded = ScopedTransactionRpc.parse(frame, ScopedTransactionRpc.MAX_REQUEST_BYTES); frame[16] = 9;
            assertEquals(kind, decoded.kind()); assertArrayEquals(new byte[]{1, 2, 3}, decoded.body());
            decoded.body()[0] = 9; assertEquals(1, decoded.body()[0]);
        }
    }

    @Test void framesRejectVersionMagicKindLengthTrailingAndBudgets() {
        byte[] valid = ScopedTransactionRpc.frame(ScopedTransactionRpc.GET, new byte[]{1});
        for (int length = 0; length < valid.length; length++) {
            byte[] truncated = Arrays.copyOf(valid, length);
            assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.parse(truncated, ScopedTransactionRpc.MAX_REQUEST_BYTES));
        }
        for (int offset : new int[]{0, 4, 8, 12}) {
            byte[] bad = valid.clone(); ByteBuffer.wrap(bad).putInt(offset, -1);
            assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.parse(bad, ScopedTransactionRpc.MAX_REQUEST_BYTES));
        }
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.parse(Arrays.copyOf(valid, valid.length + 1), ScopedTransactionRpc.MAX_REQUEST_BYTES));
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.parse(valid, valid.length - 1));
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.frame(99, new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.parse(new byte[ScopedTransactionRpc.MAX_REQUEST_BYTES + 1], ScopedTransactionRpc.MAX_REQUEST_BYTES));
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.frame(ScopedTransactionRpc.PAGE, new byte[ScopedTransactionRpc.MAX_REPLY_BYTES]));
    }

    @Test void handlesRoundTripAndRejectInvalidOrTruncatedIdentity() {
        var handle = new ScopedTransactionRpc.Handle(OWNER, ID, 0, new ApplyPosition(1, 0));
        var output = ByteBuffer.allocate(ScopedTransactionRpc.HANDLE_BYTES);
        ScopedTransactionRpc.writeHandle(output, handle);
        assertEquals(ScopedTransactionRpc.HANDLE_BYTES, output.position());
        assertEquals(handle, ScopedTransactionRpc.readHandle(ByteBuffer.wrap(output.array())));
        for (int length = 0; length < output.capacity(); length++) {
            byte[] truncated = Arrays.copyOf(output.array(), length);
            assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.readHandle(ByteBuffer.wrap(truncated)));
        }
        for (int offset : new int[]{32, 40, 48}) {
            byte[] bad = output.array().clone(); ByteBuffer.wrap(bad).putLong(offset, -1);
            assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.readHandle(ByteBuffer.wrap(bad)));
        }
        byte[] zeroTerm = output.array().clone(); ByteBuffer.wrap(zeroTerm).putLong(40, 0);
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.readHandle(ByteBuffer.wrap(zeroTerm)));
        assertThrows(IllegalArgumentException.class, () -> new ScopedTransactionRpc.Handle(OWNER, ID, -1, new ApplyPosition(1, 0)));
    }

    @Test void blobsValidateLengthBeforeAllocationAndPreserveNullEmptyDifference() {
        var output = ByteBuffer.allocate(13);
        ScopedTransactionRpc.putBlob(output, null); ScopedTransactionRpc.putBlob(output, new byte[0]);
        ScopedTransactionRpc.putBlob(output, new byte[]{7}); output.flip();
        assertNull(ScopedTransactionRpc.blob(output, 1, true));
        assertArrayEquals(new byte[0], ScopedTransactionRpc.blob(output, 1, false));
        assertArrayEquals(new byte[]{7}, ScopedTransactionRpc.blob(output, 1, false));
        for (int length : new int[]{-2, -1, 2, Integer.MAX_VALUE}) {
            var bad = ByteBuffer.allocate(5).putInt(length).put((byte) 0).flip();
            assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.blob(bad, 1, false));
        }
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.blob(ByteBuffer.allocate(3), 1, true));
    }

    @Test void scopeEncodingIsCanonicalAndOwnsBounds() {
        byte[] lower = {1};
        var point = TransactionScope.Span.point(COLUMN, lower);
        var range = TransactionScope.Span.range(COLUMN, lower, new byte[]{5});
        var unbounded = TransactionScope.Span.range(ID, null, null);
        var left = new TransactionScope(List.of(point, range, unbounded));
        var right = new TransactionScope(List.of(unbounded, range, point));
        byte[] encoded = ScopedTransactionRpc.scopeBytes(left);
        assertArrayEquals(encoded, ScopedTransactionRpc.scopeBytes(right));
        var decoded = ScopedTransactionRpc.decodeScope(ByteBuffer.wrap(encoded));
        lower[0] = 9; Arrays.fill(encoded, (byte) 0);
        assertTrue(decoded.contains(COLUMN, new byte[]{1}));
        assertFalse(decoded.contains(COLUMN, new byte[]{9}));
        assertTrue(decoded.containsRange(ID, null, null));
        decoded.spans().getLast().lowerInclusive()[0] = 9;
        assertTrue(decoded.contains(COLUMN, new byte[]{1}));
    }

    @Test void scopeRejectsTruncationCountsFlagsPointBoundsOrderingAndTrailingData() {
        byte[] encoded = ScopedTransactionRpc.scopeBytes(new TransactionScope(List.of(TransactionScope.Span.point(COLUMN, new byte[]{1}))));
        for (int length = 0; length < encoded.length; length++) {
            byte[] truncated = Arrays.copyOf(encoded, length);
            assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.decodeScope(ByteBuffer.wrap(truncated)));
        }
        for (int count : new int[]{-1, 0, TransactionScope.MAX_SPANS + 1, Integer.MAX_VALUE}) {
            byte[] bad = encoded.clone(); ByteBuffer.wrap(bad).putInt(count);
            assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.decodeScope(ByteBuffer.wrap(bad)));
        }
        byte[] flag = encoded.clone(); flag[20] = 2;
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.decodeScope(ByteBuffer.wrap(flag)));
        byte[] upper = encoded.clone(); ByteBuffer.wrap(upper).putInt(upper.length - 4, 0);
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.decodeScope(ByteBuffer.wrap(upper)));
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.decodeScope(ByteBuffer.wrap(Arrays.copyOf(encoded, encoded.length + 1))));
        byte[] two = ScopedTransactionRpc.scopeBytes(new TransactionScope(List.of(
                TransactionScope.Span.point(COLUMN, new byte[]{1}), TransactionScope.Span.point(COLUMN, new byte[]{2}))));
        int spanSize = (two.length - 4) / 2;
        byte[] first = Arrays.copyOfRange(two, 4, 4 + spanSize);
        System.arraycopy(two, 4 + spanSize, two, 4, spanSize); System.arraycopy(first, 0, two, 4 + spanSize, spanSize);
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.decodeScope(ByteBuffer.wrap(two)));
    }

    @Test void aggregateScopeBudgetIsCheckedAcrossBounds() {
        int bytes = TransactionScope.MAX_BOUND_BYTES / 2 + 1;
        var encoded = ByteBuffer.allocate(4 + 2 * (25 + bytes)).putInt(2);
        for (int i = 0; i < 2; i++) {
            ScopedTransactionRpc.uuid(encoded, COLUMN); encoded.put((byte) 1);
            ScopedTransactionRpc.putBlob(encoded, new byte[bytes]); ScopedTransactionRpc.putBlob(encoded, null);
        }
        encoded.flip();
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.decodeScope(encoded));
    }

    @Test void pagesRoundTripRowsAndDeriveContinuation() {
        for (boolean hasMore : new boolean[]{false, true}) {
            var page = new PhysicalRangePage(List.of(new PhysicalRangePage.Row(new byte[]{1}, new byte[]{2}),
                    new PhysicalRangePage.Row(new byte[]{3}, new byte[0])), hasMore);
            byte[] encoded = ScopedTransactionRpc.pageBytes(page);
            var decoded = ScopedTransactionRpc.decodePage(encoded); Arrays.fill(encoded, (byte) 0);
            assertEquals(page.rows(), decoded.rows()); assertEquals(hasMore, decoded.hasMore());
            assertArrayEquals(new byte[]{3}, decoded.resumeAfter());
            decoded.resumeAfter()[0] = 9; assertArrayEquals(new byte[]{3}, decoded.resumeAfter());
        }
        var empty = ScopedTransactionRpc.decodePage(ScopedTransactionRpc.pageBytes(new PhysicalRangePage(List.of(), false)));
        assertTrue(empty.rows().isEmpty()); assertNull(empty.resumeAfter()); assertFalse(empty.hasMore());
    }

    @Test void pagesRejectTruncationCountNullRowsBadFlagAndTrailingData() {
        byte[] encoded = ScopedTransactionRpc.pageBytes(new PhysicalRangePage(
                List.of(new PhysicalRangePage.Row(new byte[]{1}, new byte[]{2})), true));
        for (int length = 0; length < encoded.length; length++) {
            byte[] truncated = Arrays.copyOf(encoded, length);
            assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.decodePage(truncated));
        }
        for (int count : new int[]{-1, PhysicalRangePage.MAX_ITEMS + 1, Integer.MAX_VALUE}) {
            byte[] bad = encoded.clone(); ByteBuffer.wrap(bad).putInt(count);
            assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.decodePage(bad));
        }
        byte[] nullKey = encoded.clone(); ByteBuffer.wrap(nullKey).putInt(4, -1);
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.decodePage(nullKey));
        byte[] flag = encoded.clone(); flag[flag.length - 1] = 2;
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.decodePage(flag));
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.decodePage(Arrays.copyOf(encoded, encoded.length + 1)));
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.decodePage(ByteBuffer.allocate(5).putInt(0).put((byte) 1).array()));
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.decodePage(new byte[PhysicalRangePage.MAX_BYTES + 1]));
        int keyBytes = PhysicalRangePage.MAX_BYTES / 2;
        var oversizedContinuation = ByteBuffer.allocate(13 + keyBytes).putInt(1).putInt(keyBytes)
                .put(new byte[keyBytes]).putInt(0).put((byte) 0).array();
        assertThrows(IllegalArgumentException.class, () -> ScopedTransactionRpc.decodePage(oversizedContinuation));
    }
}
