package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.toBufSimple;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.impl.TablePropertiesAccumulator;
import it.cavallium.rockserver.core.common.*;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Stream;
import java.util.List;
import java.util.Map;
import org.rocksdb.TableProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Timeout(120)
class TablePropertiesTest {
    @TempDir Path tempDir;
    static Stream<Arguments> allImplementations() { return DBTest.allImplementations(); }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allImplementations")
    void reportsPersistedPropertiesAcrossTransports(String name, DBTest.ConnectionConfig connection) throws Exception {
        try (var embedded = new EmbeddedConnection(tempDir, "table-properties", null);
             var db = new TestDB(embedded, connection)) {
            var api = db.getAPI();
            long column = api.createColumn("entries", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            var empty = api.getTableProperties(column);
            assertEquals(0, empty.tableCount());
            assertEquals(0, empty.numEntries());
            assertTrue(empty.compressions().isEmpty());
            for (int i = 0; i < 100; i++) {
                api.put(0, column, new Keys(toBufSimple(i)), toBufSimple(i), RequestType.none());
            }
            assertEquals(empty, api.getTableProperties(column), "must not flush memtables");
            api.flush();
            var properties = api.getTableProperties(column);
            assertEquals(100, properties.numEntries());
            assertTrue(properties.tableCount() > 0);
            assertTrue(properties.dataSize() > 0);
            assertTrue(properties.indexSize() > 0);
            assertTrue(properties.rawKeySize() >= 100);
            assertEquals(100, properties.rawValueSize());
            assertTrue(properties.numDataBlocks() > 0);
            assertEquals(properties.tableCount(), properties.compressions().values().stream().mapToLong(Long::longValue).sum());
            assertEquals(properties, api.getTablePropertiesAsync(column).join());
            assertEquals(properties, embedded.getSyncApi(RequestContext.analytical()).getTableProperties(column));
            assertThrows(UnsupportedOperationException.class, () -> properties.compressions().clear());
            api.deleteColumn(column);
            assertThrows(RocksDBException.class, () -> api.getTableProperties(column));
        }
    }

    @Test
    void tableMetricsConfigurationIsOptInAndRejectsUnsafeIntervals() throws Exception {
        var defaults = it.cavallium.rockserver.core.config.ConfigParser.parseDefault().metrics();
        assertFalse(defaults.tablePropertiesEnabled());
        assertEquals(300, defaults.tablePropertiesIntervalSeconds());
        var config = tempDir.resolve("metrics.conf");
        java.nio.file.Files.writeString(config, "database.metrics { table-properties-enabled: true, table-properties-interval-seconds: 60 }");
        var configured = it.cavallium.rockserver.core.config.ConfigParser.parse(config).metrics();
        assertTrue(configured.tablePropertiesEnabled());
        assertEquals(60, configured.tablePropertiesIntervalSeconds());
        try (var db = new it.cavallium.rockserver.core.impl.EmbeddedDB(tempDir.resolve("db"), "metrics-enabled", config)) {
            long column = db.createColumn("entries", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            db.put(0, column, new Keys(toBufSimple(1)), toBufSimple(2), RequestType.none());
            db.flush();
            var method = db.getClass().getDeclaredMethod("collectTablePropertiesMetrics");
            method.setAccessible(true);
            @SuppressWarnings("unchecked")
            var collection = (reactor.core.publisher.Mono<java.util.Map<String, ColumnTableProperties>>) method.invoke(db);
            assertEquals(1, collection.block(Duration.ofSeconds(10)).get("entries").numEntries());
        }
        java.nio.file.Files.writeString(config, "database.metrics.table-properties-interval-seconds: 1");
        assertThrows(RocksDBException.class, () -> new EmbeddedConnection(tempDir.resolve("bad"), "invalid-metrics", config));
    }

    @Test
    void rejectsLatencyAndIngestLanes() throws Exception {
        try (var db = new EmbeddedConnection(tempDir, "table-properties-admission", null)) {
            var batch = db.getSyncApi(RequestContext.batch());
            long column = batch.createColumn("entries", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            assertThrows(RocksDBException.class, () -> db.getSyncApi(RequestContext.latency(Duration.ofSeconds(5))).getTableProperties(column));
            assertThrows(RocksDBException.class, () -> db.getSyncApi(RequestContext.ingest()).getTableProperties(column));
            assertEquals(0, db.getSyncApi(RequestContext.analytical()).getTableProperties(column).tableCount());
        }
    }

    private static ColumnTableProperties aggregate(java.util.Collection<TableProperties> tables) {
        try {
            var method = Class.forName("it.cavallium.rockserver.core.impl.EmbeddedDB")
                    .getDeclaredMethod("aggregateTableProperties", java.util.Collection.class);
            method.setAccessible(true);
            return (ColumnTableProperties) method.invoke(null, tables);
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException cause) throw cause;
            throw new AssertionError(e.getCause());
        } catch (ReflectiveOperationException e) { throw new AssertionError(e); }
    }

    @Test
    void aggregatesSizesButKeepsFormatsAndTimestampSemantics() {
        var a = mock(TableProperties.class);
        var b = mock(TableProperties.class);
        when(a.getDataSize()).thenReturn(10L);
        when(b.getDataSize()).thenReturn(20L);
        when(a.getNumEntries()).thenReturn(100L);
        when(b.getNumEntries()).thenReturn(200L);
        when(a.getNumDeletions()).thenReturn(5L);
        when(b.getNumRangeDeletions()).thenReturn(2L);
        when(a.getFormatVersion()).thenReturn(5L);
        when(b.getFormatVersion()).thenReturn(6L);
        when(a.getCompressionName()).thenReturn("ZSTD");
        when(b.getCompressionName()).thenReturn("LZ4");
        when(a.getCreationTime()).thenReturn(50L);
        when(b.getOldestKeyTime()).thenReturn(30L);
        var p = aggregate(List.of(a, b));
        assertEquals(2, p.tableCount());
        assertEquals(30, p.dataSize());
        assertEquals(300, p.numEntries());
        assertEquals(5, p.numDeletions());
        assertEquals(2, p.numRangeDeletions());
        assertEquals(50, p.oldestCreationTime());
        assertEquals(50, p.newestCreationTime());
        assertEquals(30, p.oldestKeyTime());
        assertEquals(Map.of(5L, 1L, 6L, 1L), p.formatVersions());
        assertEquals(Map.of("ZSTD", 1L, "LZ4", 1L), p.compressions());
    }

    @Test
    void rejectsOverflowRatherThanWrapping() {
        var p = mock(TableProperties.class);
        when(p.getDataSize()).thenReturn(Long.MAX_VALUE);
        assertThrows(ArithmeticException.class, () -> aggregate(List.of(p, p)));
        when(p.getDataSize()).thenReturn(-1L);
        assertThrows(ArithmeticException.class, () -> aggregate(List.of(p)));
    }

    private static void addRow(TablePropertiesAccumulator accumulator, long[] numbers, String[] names) {
        accumulator.add(
                numbers[0], numbers[1], numbers[2],
                numbers[3], numbers[4], numbers[5],
                numbers[6], numbers[7], numbers[8],
                numbers[9], numbers[10], numbers[11],
                numbers[12], numbers[13], numbers[14],
                numbers[15], numbers[16], numbers[17],
                numbers[18], numbers[19], numbers[20],
                names[0], names[1], names[2],
                names[3], names[4], names[5]);
    }

    @Test
    void scalarRowsAndNativeGettersPreserveEveryProperty() {
        var first = new long[21];
        var second = new long[21];
        for (int i = 0; i < first.length; i++) {
            first[i] = i + 1;
            second[i] = 2L * (i + 1);
        }
        // Native unknown column-family sentinel is INT32_MAX, not zero.
        first[20] = 0x7fff_ffffL;
        var names = new String[] {"filter", "comparator", "merge", "prefix", "collectors", "compression"};
        var a = mock(TableProperties.class);
        var b = mock(TableProperties.class);
        when(a.getDataSize()).thenReturn(first[0]);
        when(b.getDataSize()).thenReturn(second[0]);
        when(a.getIndexSize()).thenReturn(first[1]);
        when(b.getIndexSize()).thenReturn(second[1]);
        when(a.getIndexPartitions()).thenReturn(first[2]);
        when(b.getIndexPartitions()).thenReturn(second[2]);
        when(a.getTopLevelIndexSize()).thenReturn(first[3]);
        when(b.getTopLevelIndexSize()).thenReturn(second[3]);
        when(a.getFilterSize()).thenReturn(first[4]);
        when(b.getFilterSize()).thenReturn(second[4]);
        when(a.getRawKeySize()).thenReturn(first[5]);
        when(b.getRawKeySize()).thenReturn(second[5]);
        when(a.getRawValueSize()).thenReturn(first[6]);
        when(b.getRawValueSize()).thenReturn(second[6]);
        when(a.getNumDataBlocks()).thenReturn(first[7]);
        when(b.getNumDataBlocks()).thenReturn(second[7]);
        when(a.getNumEntries()).thenReturn(first[8]);
        when(b.getNumEntries()).thenReturn(second[8]);
        when(a.getNumDeletions()).thenReturn(first[9]);
        when(b.getNumDeletions()).thenReturn(second[9]);
        when(a.getNumMergeOperands()).thenReturn(first[10]);
        when(b.getNumMergeOperands()).thenReturn(second[10]);
        when(a.getNumRangeDeletions()).thenReturn(first[11]);
        when(b.getNumRangeDeletions()).thenReturn(second[11]);
        when(a.getSlowCompressionEstimatedDataSize()).thenReturn(first[12]);
        when(b.getSlowCompressionEstimatedDataSize()).thenReturn(second[12]);
        when(a.getFastCompressionEstimatedDataSize()).thenReturn(first[13]);
        when(b.getFastCompressionEstimatedDataSize()).thenReturn(second[13]);
        when(a.getCreationTime()).thenReturn(first[14]);
        when(b.getCreationTime()).thenReturn(second[14]);
        when(a.getOldestKeyTime()).thenReturn(first[15]);
        when(b.getOldestKeyTime()).thenReturn(second[15]);
        when(a.getFormatVersion()).thenReturn(first[16]);
        when(b.getFormatVersion()).thenReturn(second[16]);
        when(a.getFixedKeyLen()).thenReturn(first[17]);
        when(b.getFixedKeyLen()).thenReturn(second[17]);
        when(a.getIndexKeyIsUserKey()).thenReturn(first[18]);
        when(b.getIndexKeyIsUserKey()).thenReturn(second[18]);
        when(a.getIndexValueIsDeltaEncoded()).thenReturn(first[19]);
        when(b.getIndexValueIsDeltaEncoded()).thenReturn(second[19]);
        when(a.getColumnFamilyId()).thenReturn(first[20]);
        when(b.getColumnFamilyId()).thenReturn(second[20]);
        when(b.getFilterPolicyName()).thenReturn(names[0]);
        when(b.getComparatorName()).thenReturn(names[1]);
        when(b.getMergeOperatorName()).thenReturn(names[2]);
        when(b.getPrefixExtractorName()).thenReturn(names[3]);
        when(b.getPropertyCollectorsNames()).thenReturn(names[4]);
        when(b.getCompressionName()).thenReturn(names[5]);
        var accumulator = new TablePropertiesAccumulator();
        addRow(accumulator, first, new String[6]);
        addRow(accumulator, second, names);
        var result = accumulator.result();
        assertEquals(new ColumnTableProperties(
                2, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42,
                15, 30, 16,
                Map.of(17L, 1L, 34L, 1L), Map.of(18L, 1L, 36L, 1L),
                Map.of(19L, 1L, 38L, 1L), Map.of(20L, 1L, 40L, 1L),
                Map.of(0x7fff_ffffL, 1L, 42L, 1L),
                Map.of("", 1L, "filter", 1L), Map.of("", 1L, "comparator", 1L),
                Map.of("", 1L, "merge", 1L), Map.of("", 1L, "prefix", 1L),
                Map.of("", 1L, "collectors", 1L), Map.of("", 1L, "compression", 1L)), result);
        assertEquals(result, aggregate(List.of(a, b)));
        addRow(accumulator, first, new String[6]);
        assertEquals(2, result.tableCount());
        assertEquals(Map.of("", 1L, "compression", 1L), result.compressions());
        assertEquals(3, accumulator.result().tableCount());
        assertEquals(Map.of("", 2L, "compression", 1L), accumulator.result().compressions());
    }

    @Test
    void scalarRowsRejectEveryUnsignedOverflowAndEverySumOverflow() {
        for (int i = 0; i < 21; i++) {
            var row = new long[21];
            row[i] = -1;
            assertThrows(ArithmeticException.class,
                    () -> addRow(new TablePropertiesAccumulator(), row, new String[6]), "numeric field " + i);
        }
        for (int i = 0; i < 14; i++) {
            var row = new long[21];
            row[i] = Long.MAX_VALUE;
            var accumulator = new TablePropertiesAccumulator();
            addRow(accumulator, row, new String[6]);
            assertThrows(ArithmeticException.class,
                    () -> addRow(accumulator, row, new String[6]), "sum field " + i);
        }
    }
}
