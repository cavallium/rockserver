package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.*;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class StandaloneTransactionOpenScopeTest {
    private static final Duration TTL = Duration.ofSeconds(30);
    private EmbeddedConnection connection;
    private EmbeddedDB db;
    private long column, other;

    @BeforeEach void open(@TempDir Path directory) throws Exception {
        var config = directory.resolve("db.conf");
        Files.writeString(config, "database: { global: { optimistic: false } }");
        connection = new EmbeddedConnection(directory.resolve("db"), "scoped-open", config);
        db = connection.getEmbeddedDB();
        var schema = ColumnSchema.of(IntList.of(1), ObjectList.of(), true);
        column = db.createColumn("first", schema);
        other = db.createColumn("second", schema);
    }

    @AfterEach void close() throws Exception { connection.close(); }
    private static Keys key(int value) { return new Keys(new Buf[]{value(value)}); }
    private static Buf value(int value) { return Buf.wrap(new byte[]{(byte) value}); }
    private static TransactionOpenScope scope(TransactionOpenScope.Span... spans) {
        return new TransactionOpenScope(List.of(spans));
    }
    private void rejected(org.junit.jupiter.api.function.Executable access) {
        assertEquals(RocksDBException.RocksDBErrorType.RESTRICTED_TRANSACTION,
                assertThrows(RocksDBException.class, access).getErrorUniqueId());
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void snapshotIsCapturedBeforeFirstPointOrRangeRead(boolean explicit) {
        db.put(0, column, key(10), value(1), RequestType.none());
        db.put(0, other, key(20), value(2), RequestType.none());
        var declaration = explicit ? scope(TransactionOpenScope.Span.range(column, key(10), key(12)),
                TransactionOpenScope.Span.point(other, key(20))) : null;
        long tx = db.openTransaction(declaration, TTL);
        db.put(0, column, key(10), value(3), RequestType.none());
        db.put(0, column, key(11), value(4), RequestType.none());
        db.delete(0, other, key(20), RequestType.none());
        assertEquals(value(1), db.get(tx, column, key(10), RequestType.current()));
        assertEquals(List.of(true, false), db.existsMulti(tx, column, List.of(key(10), key(11))));
        assertEquals(value(2), db.get(tx, other, key(20), RequestType.current()));
        assertEquals(1L, db.<Long>reduceRange(tx, column, key(10), key(12), false, RequestType.entriesCount()).longValue());
        assertEquals(1, db.getRangePage(tx, column, key(10), key(12), false, null,
                RequestType.allInRange(), RangeBudget.DEFAULT).items().size());
        long iterator = db.openIterator(tx, column, key(10), key(12), false, TTL);
        db.closeIterator(iterator);
        db.closeTransaction(tx, false);
    }

    @Test void rejectsOutsideFootprintBeforeAnyMultiWriteOrIteratorEffect() {
        long tx = db.openTransaction(scope(TransactionOpenScope.Span.range(column, key(10), key(20))), TTL);
        rejected(() -> db.get(tx, other, key(10), RequestType.current()));
        rejected(() -> db.put(tx, column, key(20), value(1), RequestType.none()));
        rejected(() -> db.delete(tx, column, key(9), RequestType.none()));
        rejected(() -> db.merge(tx, column, key(20), value(1), RequestType.none()));
        rejected(() -> db.putMulti(tx, column, List.of(key(10), key(20)), List.of(value(1), value(2)), RequestType.none()));
        rejected(() -> db.existsMulti(tx, column, List.of(key(10), key(20))));
        rejected(() -> db.openIterator(tx, column, null, null, false, TTL));
        rejected(() -> db.reduceRange(tx, column, key(10), key(21), false, RequestType.entriesCount()));
        rejected(() -> db.getRangePage(tx, column, key(9), key(20), false, null,
                RequestType.allInRange(), RangeBudget.DEFAULT));
        assertEquals(0, db.getOpenIteratorsCount());
        assertNull(db.get(tx, column, key(10), RequestType.current()));
        db.put(tx, column, key(10), value(7), RequestType.none());
        assertTrue(db.closeTransaction(tx, true));
        assertEquals(value(7), db.get(0, column, key(10), RequestType.current()));
        assertNull(db.get(0, column, key(20), RequestType.current()));
    }

    @Test void adjacentDeclarationsAndCollisionPointsCoverWholePhysicalUnits() {
        var bucketColumn = db.createColumn("buckets", ColumnSchema.of(IntList.of(1),
                ObjectList.of(ColumnHashType.ALLSAME8), true));
        var first = new Keys(new Buf[]{value(1), value(2)});
        var collision = new Keys(new Buf[]{value(1), value(3)});
        long tx = db.openTransaction(scope(TransactionOpenScope.Span.range(column, key(10), key(15)),
                TransactionOpenScope.Span.range(column, key(15), key(20)),
                TransactionOpenScope.Span.point(bucketColumn, first)), TTL);
        db.put(tx, bucketColumn, collision, value(8), RequestType.none());
        assertEquals(value(8), db.get(tx, bucketColumn, collision, RequestType.current()));
        assertEquals(0L, db.<Long>reduceRange(tx, column, key(10), key(20), false, RequestType.entriesCount()).longValue());
        assertTrue(db.closeTransaction(tx, true));
        assertEquals(value(8), db.get(0, bucketColumn, collision, RequestType.current()));
    }

    @Test void fixedPrefixDeclarationAuthorizesEquivalentFullKeyBounds() {
        long composite = db.createColumn("composite", ColumnSchema.of(IntList.of(1, 1), ObjectList.of(), true));
        var inside = new Keys(new Buf[]{value(1), value(7)});
        var lower = new Keys(new Buf[]{value(1), value(0)});
        var upper = new Keys(new Buf[]{value(2), value(0)});
        long tx = db.openTransaction(scope(TransactionOpenScope.Span.range(composite, key(1), key(2))), TTL);
        db.put(tx, composite, inside, value(9), RequestType.none());
        assertEquals(value(9), db.get(tx, composite, inside, RequestType.current()));
        assertEquals(1L, db.<Long>reduceRange(tx, composite, lower, upper, false, RequestType.entriesCount()).longValue());
        assertEquals(1, db.getRangePage(tx, composite, lower, upper, false, null,
                RequestType.allInRange(), RangeBudget.DEFAULT).items().size());
        rejected(() -> db.get(tx, composite, upper, RequestType.current()));
        long iterator = db.openIterator(tx, composite, lower, upper, false, TTL);
        db.closeIterator(iterator);
        assertTrue(db.closeTransaction(tx, true));
    }

    @Test void sameHashLogicalRangeDeclarationAuthorizesWholeBucket() {
        long bucketColumn = db.createColumn("range-buckets", ColumnSchema.of(IntList.of(1),
                ObjectList.of(ColumnHashType.ALLSAME8), true));
        var lower = new Keys(new Buf[]{value(1), value(2)});
        var upper = new Keys(new Buf[]{value(1), value(4)});
        var outsideLogicalBounds = new Keys(new Buf[]{value(1), value(9)});
        var otherPrefix = new Keys(new Buf[]{value(2), value(3)});
        long tx = db.openTransaction(scope(TransactionOpenScope.Span.range(bucketColumn, lower, upper)), TTL);
        db.put(tx, bucketColumn, outsideLogicalBounds, value(8), RequestType.none());
        assertEquals(value(8), db.get(tx, bucketColumn, outsideLogicalBounds, RequestType.current()));
        rejected(() -> db.put(tx, bucketColumn, otherPrefix, value(9), RequestType.none()));
        assertTrue(db.closeTransaction(tx, true));
        assertEquals(value(8), db.get(0, bucketColumn, outsideLogicalBounds, RequestType.current()));
        assertNull(db.get(0, bucketColumn, otherPrefix, RequestType.current()));
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void nullMultiKeyRetainsTypedValidationWithOrWithoutScope(boolean explicit) {
        long tx = db.openTransaction(explicit ? scope(TransactionOpenScope.Span.point(column, key(10))) : null, TTL);
        try {
            assertEquals(RocksDBException.RocksDBErrorType.NULL_ARGUMENT,
                    assertThrows(RocksDBException.class, () -> db.existsMulti(tx, column,
                            java.util.Arrays.asList(key(10), null))).getErrorUniqueId());
        } finally { db.closeTransaction(tx, false); }
    }

    @Test void invalidColumnAndSchemaBoundsAllocateNoTransactionOrLease() {
        long pending = db.getPendingOpsCount();
        for (var invalid : List.of(
                scope(TransactionOpenScope.Span.point(Long.MAX_VALUE, key(1))),
                scope(TransactionOpenScope.Span.point(column, new Keys(new Buf[]{Buf.wrap(new byte[2])}))),
                scope(TransactionOpenScope.Span.range(column, key(20), key(10))))) {
            assertThrows(RocksDBException.class, () -> db.openTransaction(invalid, TTL));
            assertEquals(0, db.getOpenTransactionsCount());
            assertEquals(pending, db.getPendingOpsCount());
        }
    }
}
