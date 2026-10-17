package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MultiGetPage;
import it.cavallium.rockserver.core.common.RangeBudget;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MultiGetContractTest {

    @Test
    void boundedPrefixesRetainMissingSlotsOrderAndDuplicateKeys(@TempDir Path directory) throws Exception {
        try (var connection = new EmbeddedConnection(directory.resolve("db"), "multi-get-prefix", null)) {
            var api = connection.getSyncApi(RequestContext.batch());
            long column = api.createColumn("messages", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            var first = Buf.wrap(new byte[8]);
            var third = Buf.wrap(new byte[]{3, 3, 3, 3, 3, 3, 3, 3});
            api.put(0, column, key(1), first, RequestType.none());
            api.put(0, column, key(3), third, RequestType.none());
            var keys = List.of(key(3), key(2), key(1), key(3));
            var expected = Arrays.asList(third, null, first, third);
            var actual = new ArrayList<Buf>();
            var budget = new RangeBudget(4, 72);
            do {
                var remaining = keys.subList(actual.size(), keys.size());
                var page = api.getMulti(column, remaining, budget);
                page.validateResponse(remaining.size(), budget);
                assertFalse(page.values().isEmpty(), "every continuation must advance the input prefix");
                assertEquals(expected.subList(actual.size(), actual.size() + page.values().size()), page.values());
                actual.addAll(page.values());
                assertEquals(actual.size() < keys.size(), page.hasMore());
            } while (actual.size() < keys.size());
            assertEquals(expected, actual);
            assertEquals(List.of(), api.getMulti(column, List.of(), new RangeBudget(1, 72)).values());
        }
    }

    @Test
    void borrowedPinnedMultiGetResultViewsExpireWhenOwnerCloses(@TempDir Path directory) throws Exception {
        try (var connection = new EmbeddedConnection(directory.resolve("db"), "multi-get-owner", null)) {
            var api = connection.getSyncApi(RequestContext.batch());
            long column = api.createColumn("messages", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            byte[] expected = {1, 2, 3};
            api.put(0, column, key(1), Buf.wrap(expected), RequestType.none());
            var db = connection.getInternalDB();
            var pending = db.getPendingOpsCount();
            var owner = db.getMultiPinned(column, List.of(key(1), key(2), key(1)),
                    new RangeBudget(3, 128), WorkloadProfile.BATCH, Long.MAX_VALUE, () -> false);
            Buf borrowed;
            try (owner) {
                var page = owner.page();
                assertEquals(Arrays.asList(Buf.wrap(expected), null, Buf.wrap(expected)), page.values());
                assertFalse(page.hasMore());
                borrowed = page.values().getFirst();
                assertArrayEquals(expected, borrowed.toByteArray());
                assertEquals(pending + 1, db.getPendingOpsCount());
            }
            assertThrows(IllegalStateException.class, owner::page);
            assertThrows(IllegalStateException.class, borrowed::toByteArray);
            owner.close();
            assertEquals(pending, db.getPendingOpsCount());
            api.deleteColumn(column);
        }
    }

    @Test
    void ordinaryMultiGetValuesSurviveOwnerAndDatabaseClose(@TempDir Path directory) throws Exception {
        MultiGetPage page;
        byte[] expected = {5, 6, 7};
        try (var connection = new EmbeddedConnection(directory.resolve("db"), "multi-get-copy", null)) {
            var api = connection.getSyncApi(RequestContext.batch());
            long column = api.createColumn("messages", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            api.put(0, column, key(1), Buf.wrap(expected), RequestType.none());
            var pending = connection.getInternalDB().getPendingOpsCount();
            page = api.getMulti(column, List.of(key(1), key(2), key(1)), new RangeBudget(3, 128));
            assertEquals(pending, connection.getInternalDB().getPendingOpsCount());
            api.put(0, column, key(1), Buf.wrap(new byte[]{9}), RequestType.none());
            api.deleteColumn(column);
        }
        assertEquals(Arrays.asList(Buf.wrap(expected), null, Buf.wrap(expected)), page.values());
        assertArrayEquals(expected, page.values().getFirst().toByteArray());
    }

    @Test
    void cancellationAfterAdmissionAndExpiredDeadlineReleaseLeases(@TempDir Path directory) throws Exception {
        try (var connection = new EmbeddedConnection(directory.resolve("db"), "multi-get-cancel", null)) {
            var api = connection.getSyncApi(RequestContext.batch());
            long column = api.createColumn("messages", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            var db = connection.getInternalDB();
            var pending = db.getPendingOpsCount();
            var keys = List.of(key(1));
            var budget = new RangeBudget(1, 128);
            assertThrows(CancellationException.class, () -> db.getMultiPinned(column, keys, budget,
                    WorkloadProfile.BATCH, Long.MAX_VALUE, () -> db.getPendingOpsCount() > pending));
            assertEquals(pending, db.getPendingOpsCount());
            var failure = assertThrows(RocksDBException.class, () -> db.getMultiPinned(column, keys, budget,
                    WorkloadProfile.BATCH, 0L, () -> false));
            assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED, failure.getErrorUniqueId());
            assertEquals(pending, db.getPendingOpsCount());
            api.deleteColumn(column);
        }
    }

    @Test
    void oversizedFirstValueCanStillBeReadIndividually(@TempDir Path directory) throws Exception {
        try (var connection = new EmbeddedConnection(directory.resolve("db"), "multi-get-large-item", null)) {
            var api = connection.getSyncApi(RequestContext.batch());
            long column = api.createColumn("messages", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            var large = Buf.wrap(new byte[128]);
            api.put(0, column, key(1), large, RequestType.none());
            var error = assertThrows(RocksDBException.class,
                    () -> api.getMulti(column, List.of(key(1)), new RangeBudget(1, 64)));
            assertEquals(RocksDBException.RocksDBErrorType.GET_MULTI_ITEM_TOO_LARGE, error.getErrorUniqueId());
            assertEquals(large, api.get(0, column, key(1), RequestType.current()));
            assertEquals(List.of(large), api.getMulti(column, List.of(key(1)), new RangeBudget(1, 160)).values());
            var tinyBudget = assertThrows(RocksDBException.class,
                    () -> api.getMulti(column, List.of(key(2)), new RangeBudget(1, 16)));
            assertEquals(RocksDBException.RocksDBErrorType.GET_MULTI_ITEM_TOO_LARGE, tinyBudget.getErrorUniqueId());
        }
    }

    @Test
    void pageCopiesNullableSlotsAndRejectsNonProgressingContinuation() {
        var source = new ArrayList<Buf>(Arrays.asList(null, Buf.wrap(new byte[]{1})));
        var page = new MultiGetPage(source, true);
        source.clear();
        assertEquals(2, page.values().size());
        assertEquals(null, page.values().getFirst());
        assertThrows(UnsupportedOperationException.class, () -> page.values().clear());
        assertThrows(IllegalArgumentException.class, () -> new MultiGetPage(List.of(), true));
        page.validateResponse(3, new RangeBudget(3, 64));
        assertThrows(IllegalStateException.class, () -> page.validateResponse(2, new RangeBudget(2, 64)));
        assertThrows(IllegalStateException.class, () -> page.validateResponse(3, new RangeBudget(3, 32)));
    }

    private static Keys key(int value) {
        return new Keys(Buf.wrap(new byte[]{(byte) value}));
    }
}
