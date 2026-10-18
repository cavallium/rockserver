package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MultiGetPage;
import it.cavallium.rockserver.core.common.RangeBudget;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.test.DBTest.ConnectionConfig;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Timeout(120)
class MultiGetTransportTest {

    @TempDir
    Path directory;

    static Stream<Arguments> allImplementations() {
        return DBTest.allImplementations();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allImplementations")
    void collidingBucketValuesRetainOrderAcrossPagesErrorsAndReopen(String name,
            ConnectionConfig config) throws Exception {
        Path database = directory.resolve("db");
        var value = Buf.wrap(new byte[]{3, 4, 5});
        var empty = Buf.wrap(new byte[0]);
        var keys = List.of(key(3), key(2), key(1), key(3));
        var expected = Arrays.asList(value, null, empty, value);
        MultiGetPage retained;
        try (var embedded = new EmbeddedConnection(database, "multi-get-buckets", null);
                var testDB = new TestDB(embedded, config)) {
            var api = testDB.getAPI();
            long column = api.createColumn("buckets",
                    ColumnSchema.of(IntList.of(1), ObjectList.of(ColumnHashType.ALLSAME8), true));
            api.put(0, column, key(1), empty, RequestType.none());
            api.put(0, column, key(3), value, RequestType.none());
            var pending = embedded.getInternalDB().getPendingOpsCount();
            var budget = new RangeBudget(2, 1024);
            var actual = new ArrayList<Buf>();
            while (actual.size() < keys.size()) {
                var remaining = keys.subList(actual.size(), keys.size());
                var page = api.getMulti(column, remaining, budget);
                page.validateResponse(remaining.size(), budget);
                assertFalse(page.values().isEmpty(), name);
                actual.addAll(page.values());
            }
            assertEquals(expected, actual, name);
            assertEquals(pending, embedded.getInternalDB().getPendingOpsCount());

            // Physical bucket bytes count even when the selected logical value is empty.
            var failure = assertThrows(RocksDBException.class,
                    () -> api.getMulti(column, List.of(key(1)), new RangeBudget(1, 32)));
            assertEquals(RocksDBException.RocksDBErrorType.GET_MULTI_ITEM_TOO_LARGE,
                    failure.getErrorUniqueId());
            assertEquals(pending, embedded.getInternalDB().getPendingOpsCount());
            retained = api.getMulti(column, keys, new RangeBudget(4, 1024));
            assertEquals(expected, retained.values(), "a failed request must leave the connection usable");
            assertFalse(retained.hasMore());
            api.flush();
        }
        assertEquals(expected, retained.values(), "transport values must outlive the database owner");
        try (var embedded = new EmbeddedConnection(database, "multi-get-buckets-reopen", null);
                var testDB = new TestDB(embedded, config)) {
            var api = testDB.getAPI();
            long column = api.getColumnId("buckets");
            assertEquals(expected, api.getMulti(column, keys, new RangeBudget(4, 1024)).values());
            api.deleteColumn(column);
        }
    }

    private static Keys key(int value) {
        return new Keys(Buf.wrap(new byte[]{7}), Buf.wrap(new byte[]{(byte) value}));
    }
}
