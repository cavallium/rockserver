package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.toBufSimple;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
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
}
