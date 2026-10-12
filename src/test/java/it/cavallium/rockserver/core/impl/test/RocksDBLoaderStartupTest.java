package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;

import static org.junit.jupiter.api.Assertions.*;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.config.DatabaseConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

class RocksDBLoaderStartupTest {
    @TempDir Path directory;

    private DatabaseConfig config(String text) throws Exception {
        return ConfigParser.parse(Files.writeString(directory.resolve("config.conf"), text));
    }

    private String snapshots(Path snapshotDirectory) {
        return """
                database.global.block-cache = 4MiB
                database.global.block-cache-metadata-size = 1MiB
                database.global.write-buffer-manager = 1MiB
                database.global.hot-cache-snapshot: {enabled: true, directory: "%s"}
                """.formatted(snapshotDirectory);
    }

    @Test
    void disabledFeaturesKeepCanonicalCacheAndOpenFlowAcrossReopen() throws Exception {
        var config = config("""
                database.global.block-cache = 4MiB
                database.global.write-buffer-manager = 1MiB
                """);
        Path dbPath = directory.resolve("database");
        for (int iteration = 0; iteration < 2; iteration++) {
            var loaded = RocksDBLoader.load(dbPath, config, LoggerFactory.getLogger(getClass()));
            try {
                assertFalse(loaded.startupResources().settings().admission().enabled());
                assertTrue(loaded.startupResources().budgets().isEmpty());
                assertNull(loaded.startupResources().admissionConfig());
                assertTrue(loaded.startupResources().snapshotCaches().isEmpty());
                assertInstanceOf(org.rocksdb.LRUCache.class, loaded.cache());
                assertEquals(5L << 20, loaded.cacheCapacities().get("default"));
                var db = loaded.db().get();
                if (iteration == 0) db.put(new byte[]{1}, new byte[]{2});
                assertArrayEquals(new byte[]{2}, db.get(new byte[]{1}));
                loaded.startupResources().stopSnapshotJobs();
                loaded.startupResources().stopSnapshotJobs();
            } finally {
                loaded.db().close();
                loaded.refs().close();
            }
        }
    }

    @Test
    void rejectsTransientDatabaseAndOverlappingDbWalAndSstBeforeAllocation() throws Exception {
        var logger = LoggerFactory.getLogger(getClass());
        var memoryConfig = config(snapshots(directory.resolve("snapshot")));
        var memoryFailure = assertThrows(RocksDBException.class,
                () -> RocksDBLoader.load(null, memoryConfig, logger));
        assertEquals(RocksDBErrorType.CONFIG_ERROR, memoryFailure.getErrorUniqueId());
        assertFalse(Files.exists(directory.resolve("snapshot")));

        Path db = directory.resolve("database");
        Path wal = directory.resolve("wal");
        Path sst = directory.resolve("sst");
        for (Path overlap : List.of(db, db.resolve("snapshot"), directory, wal, sst)) {
            var parsed = config(snapshots(overlap) + """
                    database.global.wal-path = "%s"
                    database.global.fallback-column-options.volumes = [{volume-path: "%s", target-size: 1GiB}]
                    """.formatted(wal, sst));
            var failure = assertThrows(RocksDBException.class,
                    () -> RocksDBLoader.load(db, parsed, logger));
            assertEquals(RocksDBErrorType.CONFIG_ERROR, failure.getErrorUniqueId());
            assertFalse(Files.exists(db));
            assertFalse(Files.exists(wal));
            assertFalse(Files.exists(sst));
        }
    }

    @Test
    void canonicalPathSafetyIncludesMissingPathsUnderSymlinkedParents() throws Exception {
        Path physical = Files.createDirectory(directory.resolve("physical"));
        Path link = Files.createSymbolicLink(directory.resolve("link"), physical);
        Path db = physical.resolve("database");
        var parsed = config(snapshots(link.resolve("database/snapshot")));
        var failure = assertThrows(RocksDBException.class,
                () -> RocksDBLoader.load(db, parsed, LoggerFactory.getLogger(getClass())));
        assertEquals(RocksDBErrorType.CONFIG_ERROR, failure.getErrorUniqueId());
        assertFalse(Files.exists(db));
    }

    @Test
    void rejectsDistinctStorageIdsForTheSamePhysicalDirectoryBeforeAllocation() throws Exception {
        Path physical = Files.createDirectory(directory.resolve("storage"));
        Path alias = Files.createSymbolicLink(directory.resolve("storage-alias"), physical);
        var parsed = config("""
                database.global.open-files-async = true
                database.global.maximum-open-files = -1
                database.global.startup-io: {
                  enabled: true
                  default-storage-id: first
                  storage-groups: [
                    {id: first, paths: ["%s"]},
                    {id: second, paths: ["%s"]}
                  ]
                }
                """.formatted(physical, alias));
        Path db = directory.resolve("database");
        var failure = assertThrows(RocksDBException.class,
                () -> RocksDBLoader.load(db, parsed, LoggerFactory.getLogger(getClass())));
        assertEquals(RocksDBErrorType.CONFIG_ERROR, failure.getErrorUniqueId());
        assertFalse(Files.exists(db));
    }

    private String namespace(Path db, String name, boolean metadata) throws Exception {
        var method = RocksDBLoader.class.getDeclaredMethod("snapshotNamespace", Path.class, String.class, boolean.class);
        method.setAccessible(true);
        return (String) method.invoke(null, db, name, metadata);
    }

    @Test
    void namespacesAreStableAndExclusiveAcrossDbCacheAndPool() throws Exception {
        Path db = directory.resolve("db");
        String namespace = namespace(db, "default", false);
        assertEquals(namespace, namespace(db, "default", false));
        assertTrue(namespace.matches("[A-Za-z0-9_-]{1,64}"));
        assertNotEquals(namespace, namespace(db, "default", true));
        assertNotEquals(namespace, namespace(db, "named", false));
        assertNotEquals(namespace, namespace(directory.resolve("other"), "default", false));
    }
}
