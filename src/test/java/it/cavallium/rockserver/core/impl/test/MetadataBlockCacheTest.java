package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.*;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.config.ConfigPrinter;
import it.cavallium.rockserver.core.config.DatabaseConfig;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.Cache;
import org.rocksdb.FlushOptions;
import org.rocksdb.HyperClockCache;
import org.rocksdb.ReadOptions;
import org.rocksdb.TickerType;
import org.slf4j.LoggerFactory;

class MetadataBlockCacheTest {
    @TempDir Path tempDir;

    private DatabaseConfig config(String text) throws Exception {
        return ConfigParser.parse(Files.writeString(tempDir.resolve("config.conf"), text));
    }

    @Test
    void defaultsAndConfiguredBudgetsRoundTrip() throws Exception {
        assertEquals(0, ConfigParser.parseDefault().global().blockCacheMetadataSize().longValue());
        var original = config("""
                database.global.block-cache-metadata-size = 2MiB
                database.global.block-caches = [{name: sender, size: 4MiB, metadata-size: 1MiB},
                  {name: legacy, size: 2MiB}]
                """);
        var reparsed = config("database=" + ConfigPrinter.stringify(original));
        assertEquals(original.global().blockCacheMetadataSize(), reparsed.global().blockCacheMetadataSize());
        assertEquals(original.global().blockCaches()[0].metadataSize(), reparsed.global().blockCaches()[0].metadataSize());
        assertNull(reparsed.global().blockCaches()[1].metadataSize());
    }

    @Test
    void rejectsInvalidBudgetsAndUncachedMetadataBeforeDatabaseCreation() throws Exception {
        String[] invalid = {
                "database.global.block-cache-metadata-size = -1B",
                "database.global.block-cache-metadata-size = 512MiB",
                "database.global.block-caches = [{name: sender, size: 4MiB, metadata-size: 4MiB}]",
                "database.global.block-caches = [{name: sender, size: 4MiB}, {name: sender, size: 4MiB}]",
                "database.global.block-cache = 9223372036854775807B",
                "database.global.block-cache-metadata-size = 1MiB\n"
                        + "database.global.fallback-column-options.cache-index-and-filter-blocks = false"
        };
        for (int i = 0; i < invalid.length; i++) {
            var parsed = config(invalid[i]);
            Path dbPath = tempDir.resolve("invalid-" + i);
            var failure = assertThrows(RocksDBException.class,
                    () -> RocksDBLoader.load(dbPath, parsed, LoggerFactory.getLogger(getClass())));
            assertEquals(RocksDBErrorType.CONFIG_ERROR, failure.getErrorUniqueId());
            assertFalse(Files.exists(dbPath));
        }
    }

    @Test
    void zeroMetadataPreservesLegacyDefaultWithOnlyWriteBufferAllowance() throws Exception {
        var parsed = config("""
                database.global.block-cache = 0B
                database.global.write-buffer-manager = 2MiB
                """);
        var loaded = RocksDBLoader.load(tempDir.resolve("legacy"), parsed, LoggerFactory.getLogger(getClass()));
        try {
            assertTrue(loaded.metadataCaches().isEmpty());
            assertEquals(2L << 20, loaded.cacheCapacities().get("default"));
        } finally {
            loaded.db().close();
            loaded.refs().close();
        }
    }

    @Test
    void dynamicallyCreatedColumnKeepsMetadataPoolAfterReopen() throws Exception {
        Path configPath = Files.writeString(tempDir.resolve("dynamic.conf"), """
                database.metrics.jmx.enabled = false
                database.metrics.influx.enabled = false
                database.global.block-cache = 4MiB
                database.global.block-cache-metadata-size = 1MiB
                database.global.write-buffer-manager = 2MiB
                database.global.fallback-column-options.pin-index-and-filter-blocks = false
                """);
        Path dbPath = tempDir.resolve("dynamic");
        var key = new Keys(Buf.wrap(new byte[4]));
        var value = Buf.wrap(new byte[128]);
        for (int iteration = 0; iteration < 2; iteration++) {
            Cache metadata;
            var registry = new SimpleMeterRegistry();
            try (var connection = new EmbeddedConnection(dbPath, "dynamic", configPath)) {
                ((CompositeMeterRegistry) connection.getInternalDB().getMetricsRegistry()).add(registry);
                var api = connection.getSyncApi(RequestContext.batch());
                long column = api.createColumn("created-after-open", ColumnSchema.of(
                        IntList.of(4), ObjectList.of(), true));
                if (iteration == 0) {
                    api.put(0, column, key, value, RequestType.none());
                    api.flush();
                }
                Buf actual = api.get(0, column, key, RequestType.current());
                assertEquals(value, actual);
                var internal = connection.getInternalDB();
                var field = EmbeddedDB.class.getDeclaredField("metadataCaches");
                field.setAccessible(true);
                metadata = (Cache) ((Map<?, ?>) field.get(internal)).get("default");
                assertTrue(metadata.getUsage() > 0, "dynamic column metadata must reach the reserved pool");
                assertEquals(6L << 20, registry.get("rocksdb.cache.named")
                        .tag("cache", "default").tag("field", "capacity").gauge().value());
                assertEquals(1L << 20, registry.get("rocksdb.cache.metadata")
                        .tag("cache", "default").tag("field", "capacity").gauge().value());
            } finally {
                registry.close();
            }
            assertFalse(metadata.isOwningHandle(), "embedded shutdown closes its metadata cache");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void splitsDefaultAndNamedCachesAndRetainsMetadataAcrossDataTrafficAndReopen(boolean partitionFilters) throws Exception {
        var parsed = config("""
                database.global.use-clock-cache = true
                database.global.disable-auto-compactions = true
                database.global.block-cache = 4MiB
                database.global.block-cache-metadata-size = 1MiB
                database.global.write-buffer-manager = 2MiB
                database.global.block-caches = [{name: sender, size: 2MiB, metadata-size: 1MiB}]
                database.global.fallback-column-options: {
                  pin-index-and-filter-blocks: false
                  partition-filters: %s
                  block-size: 4KiB
                  volumes: []
                  levels: []
                  bloom-filter: {bits-per-key: 10, use-ribbon: false, optimize-for-hits: false}
                }
                database.global.column-options = [
                  ${database.global.fallback-column-options} {name: default},
                  ${database.global.fallback-column-options} {name: sender, block-cache-name: sender}
                ]
                """.formatted(partitionFilters));
        Path dbPath = tempDir.resolve("database");
        byte[] value = new byte[4096];
        new Random(42).nextBytes(value);
        for (int iteration = 0; iteration < 2; iteration++) {
            var loaded = RocksDBLoader.load(dbPath, parsed, LoggerFactory.getLogger(getClass()));
            var data = loaded.caches();
            var metadata = loaded.metadataCaches();
            try {
                assertEquals(5L << 20, loaded.cacheCapacities().get("default"));
                assertEquals(1L << 20, loaded.metadataCacheCapacities().get("default"));
                assertEquals(1L << 20, loaded.cacheCapacities().get("sender"));
                assertEquals(1L << 20, loaded.metadataCacheCapacities().get("sender"));
                for (String name : data.keySet()) {
                    assertInstanceOf(HyperClockCache.class, data.get(name));
                    assertInstanceOf(HyperClockCache.class, metadata.get(name));
                    assertNotSame(data.get(name), metadata.get(name));
                    assertEquals(1, loaded.refs().asList().stream().filter(ref -> ref == data.get(name)).count());
                    assertEquals(1, loaded.refs().asList().stream().filter(ref -> ref == metadata.get(name)).count());
                }
                var db = loaded.db().get();
                for (var handle : loaded.db().getStartupColumns().values()) {
                    String name = new String(handle.getName(), java.nio.charset.StandardCharsets.UTF_8);
                    if (iteration == 0) {
                        for (int key = 0; key < 2048; key++) {
                            db.put(handle, ByteBuffer.allocate(4).putInt(key).array(), value);
                        }
                        try (var flush = new FlushOptions().setWaitForFlush(true)) {
                            db.flush(flush, handle);
                        }
                    }
                    try (var reads = new ReadOptions().setFillCache(false)) {
                        assertArrayEquals(value, db.get(handle, reads, new byte[4]));
                    }
                    assertArrayEquals(value, db.get(handle, new byte[4]));
                    assertTrue(metadata.get(name).getUsage() > 0, "SST metadata must use its separate cache");
                    long dataAdds = loaded.dbOptions().statistics().getTickerCount(TickerType.BLOCK_CACHE_DATA_ADD);
                    long metadataUsage = metadata.get(name).getUsage();
                    for (int key = 0; key < 2048; key++) {
                        assertArrayEquals(value, db.get(handle, ByteBuffer.allocate(4).putInt(key).array()));
                    }
                    assertTrue(data.get(name).getUsage() > 0, "data blocks must populate the data cache");
                    assertTrue(loaded.dbOptions().statistics().getTickerCount(TickerType.BLOCK_CACHE_DATA_ADD) > dataAdds,
                            "the workload must actually insert data blocks into cache");
                    assertTrue(metadata.get(name).getUsage() >= metadataUsage,
                            "data traffic must not evict SST metadata");
                }
            } finally {
                loaded.db().close();
                assertTrue(metadata.values().stream().allMatch(Cache::isOwningHandle));
                loaded.refs().close();
            }
            assertTrue(data.values().stream().noneMatch(Cache::isOwningHandle));
            assertTrue(metadata.values().stream().noneMatch(Cache::isOwningHandle));
        }
    }
    @Test
    void bulkExistsWarmsColdPartitionMetadataAndReusesItOnRepeat() throws Exception {
        Path configPath = Files.writeString(tempDir.resolve("bulk-cache.conf"), """
                database.metrics.jmx.enabled = false
                database.metrics.influx.enabled = false
                database.global.adaptive-compaction-io = false
                database.global.disable-auto-compactions = true
                database.global.maximum-open-files = 32
                database.global.use-clock-cache = true
                database.global.block-cache-high-priority-ratio = 0
                database.global.block-cache = 4MiB
                database.global.write-buffer-manager = 8MiB
                database.global.block-caches = [{name: bulk, size: 3MiB, metadata-size: 2MiB}]
                database.global.fallback-column-options: {
                  block-cache-name: bulk
                  cache-index-and-filter-blocks: true
                  pin-index-and-filter-blocks: false
                  partition-filters: true
                  block-size: 1KiB
                  levels: []
                  bloom-filter: {bits-per-key: 10, use-ribbon: false, optimize-for-hits: false}
                }
                """);
        Path dbPath = tempDir.resolve("bulk-cache-db");
        var dbField = EmbeddedDB.class.getDeclaredField("db");
        dbField.setAccessible(true);
        var columnsField = EmbeddedDB.class.getDeclaredField("columns");
        columnsField.setAccessible(true);
        byte[] value = new byte[256];
        var random = new Random(42);
        try (var connection = new EmbeddedConnection(dbPath, "bulk-cache-create", configPath)) {
            var api = connection.getSyncApi(RequestContext.batch());
            long column = api.createColumn("entries", ColumnSchema.of(IntList.of(4), ObjectList.of(), true));
            var internal = connection.getInternalDB();
            var nativeDb = ((it.cavallium.rockserver.core.impl.rocksdb.TransactionalDB) dbField.get(internal)).get();
            var handle = ((it.cavallium.rockserver.core.impl.ColumnInstance)
                    ((Map<?, ?>) columnsField.get(internal)).get(column)).cfh();
            try (var flush = new FlushOptions().setWaitForFlush(true)) {
                for (int file = 0; file < 4; file++) {
                    for (int key = 0; key < 8192; key++) {
                        random.nextBytes(value);
                        nativeDb.put(handle, ByteBuffer.allocate(4).putInt((file * 8192 + key) * 2).array(), value);
                    }
                    nativeDb.flush(flush, handle);
                }
            }
            // L0 readers eagerly prefetch metadata. Persist several L1 SSTs so reopening leaves leaf partitions cold.
            var inputFiles = nativeDb.getLiveFilesMetaData().stream()
                    .filter(file -> new String(file.columnFamilyName(), java.nio.charset.StandardCharsets.UTF_8).equals("entries"))
                    .map(org.rocksdb.LiveFileMetaData::fileName).toList();
            try (var compact = new org.rocksdb.CompactionOptions().setOutputFileSizeLimit(2L << 20)
                    .setCompression(org.rocksdb.CompressionType.NO_COMPRESSION);
                    var job = new org.rocksdb.CompactionJobInfo()) {
                nativeDb.compactFiles(compact, handle, inputFiles, 1, 0, job);
            }
            var files = nativeDb.getLiveFilesMetaData().stream()
                    .filter(file -> new String(file.columnFamilyName(), java.nio.charset.StandardCharsets.UTF_8).equals("entries"))
                    .toList();
            assertTrue(files.size() >= 4, "the fixture must contain several independently cold SSTs");
            assertTrue(files.stream().allMatch(file -> file.level() > 0), "L0 prefetch must not warm the regression fixture");
            assertTrue(nativeDb.getPropertiesOfAllTables(handle).values().stream()
                    .mapToLong(org.rocksdb.TableProperties::getIndexPartitions).sum() >= 16,
                    "the persisted fixture must contain many leaf metadata partitions");
        }
        // Reopening creates fresh role caches. Do not prime the user column with point Gets.
        try (var connection = new EmbeddedConnection(dbPath, "bulk-cache-read", configPath)) {
            var internal = connection.getInternalDB();
            var api = connection.getSyncApi(RequestContext.batch(java.time.Duration.ofSeconds(10)));
            long column = api.getColumnId("entries");
            var keys = new java.util.ArrayList<Keys>();
            var expected = new java.util.ArrayList<Boolean>();
            for (int key = 0; key < 4097; key++) {
                int stored = key * 32768 / 4097;
                keys.add(new Keys(Buf.wrap(ByteBuffer.allocate(4).putInt(stored * 2 + key % 2).array())));
                expected.add(key % 2 == 0);
            }
            var metadataField = EmbeddedDB.class.getDeclaredField("metadataCaches");
            metadataField.setAccessible(true);
            Cache metadata = (Cache) ((Map<?, ?>) metadataField.get(internal)).get("bulk");
            var collectorField = EmbeddedDB.class.getDeclaredField("rocksDBStatistics");
            collectorField.setAccessible(true);
            var collector = collectorField.get(internal);
            var executorField = collector.getClass().getDeclaredField("executor");
            executorField.setAccessible(true);
            var collectorThread = (Thread) executorField.get(collector);
            assertTimeoutPreemptively(java.time.Duration.ofSeconds(5), () -> {
                while (collectorThread.getState() != Thread.State.TIMED_WAITING) Thread.sleep(10);
            });
            var optionsField = EmbeddedDB.class.getDeclaredField("dbOptions");
            optionsField.setAccessible(true);
            try (var statistics = ((org.rocksdb.DBOptions) optionsField.get(internal)).statistics()) {
                long usageBefore = metadata.getUsage();
                long filterAddsBefore = statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_ADD);
                long indexAddsBefore = statistics.getTickerCount(TickerType.BLOCK_CACHE_INDEX_ADD);
                long dataAddsBefore = statistics.getTickerCount(TickerType.BLOCK_CACHE_DATA_ADD);
                long missesBefore = statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS)
                        + statistics.getTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS);
                assertEquals(expected, api.existsMulti(0, column, keys));
                long filterAdds = statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_ADD) - filterAddsBefore;
                long indexAdds = statistics.getTickerCount(TickerType.BLOCK_CACHE_INDEX_ADD) - indexAddsBefore;
                long firstMisses = statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS)
                        + statistics.getTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS) - missesBefore;
                assertTrue(filterAdds >= 16, "bulk reads must admit filter partitions, not only table-reader roots: adds="
                        + filterAdds + ", indexAdds=" + indexAdds + ", misses=" + firstMisses
                        + ", metadataUsage=" + usageBefore + "->" + metadata.getUsage());
                assertTrue(indexAdds >= 16, "bulk reads must admit index partitions, not only table-reader roots: adds=" + indexAdds);
                assertTrue(firstMisses >= 16, "the request must reach cold SST partition metadata");
                assertTrue(metadata.getUsage() > usageBefore, "the dedicated metadata cache must be populated by bulk reads");
                long hitsBefore = statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT)
                        + statistics.getTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT);
                long missesAfterFirst = statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS)
                        + statistics.getTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS);
                assertEquals(expected, api.existsMulti(0, column, keys));
                long repeatMisses = statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS)
                        + statistics.getTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS) - missesAfterFirst;
                long repeatHits = statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT)
                        + statistics.getTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT) - hitsBefore;
                assertTrue(repeatHits > 0, "the repeated bulk request must reuse cached metadata");
                assertTrue(repeatMisses * 2 < firstMisses, "metadata reuse must materially reduce repeated native metadata misses");
                System.out.printf("bulk metadata cache: filterAdds=%d indexAdds=%d firstMisses=%d repeatMisses=%d repeatHits=%d usage=%d->%d dataAdds=%d%n",
                        filterAdds, indexAdds, firstMisses, repeatMisses, repeatHits, usageBefore, metadata.getUsage(),
                        statistics.getTickerCount(TickerType.BLOCK_CACHE_DATA_ADD) - dataAddsBefore);
                assertEquals(0, internal.getActiveExistsMultiRequestCount());
            }
        }
    }

}
