package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport;
import it.cavallium.rockserver.core.common.distributed.TransactionScope;
import it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.DistributedCommand;
import it.cavallium.rockserver.core.common.distributed.Mutation;
import it.cavallium.rockserver.core.common.distributed.OperationId;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.config.DatabaseConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.WriteBufferManager;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

class DistributedStoragePoolTest {
    @TempDir(factory = DiskTestDirectory.class) Path directory;
    private static final UUID COLUMN = UUID.randomUUID();
    private static final UUID DOMAIN = UUID.randomUUID();
    private static final UUID SESSION = UUID.randomUUID();
    private static final Map<UUID, String> COLUMNS = Map.of(COLUMN, "shared_data");

    private DatabaseConfig parse(String text) throws Exception {
        Path config = directory.resolve("pool.conf");
        Files.writeString(config, text);
        return ConfigParser.parse(config);
    }
    private DistributedStoragePool open(Path path, DatabaseConfig config) {
        return DistributedStoragePool.open(path, config, LoggerFactory.getLogger(getClass()));
    }
    private ReplicatedRangeStore range(DistributedStoragePool pool, UUID group, byte[] lower, byte[] upper)
            throws Exception {
        return pool.openRange(new RangeOwnership.Owner(group, 1),
                new RangeOwnership.Interval(DOMAIN, Set.of(COLUMN), lower, upper), new byte[32], COLUMNS, Set.of());
    }
    private DistributedCommand put(UUID group, byte[] key, byte[] value) {
        return new DistributedCommand(group, 1, new byte[32], new OperationId(SESSION, 1, 0),
                List.of(new Mutation(Mutation.Kind.PUT, COLUMN, key, value)));
    }

    @Test void disjointRangesShareOneDatabaseAndPhysicalFamiliesAndReopen() throws Exception {
        var config = ConfigParser.parseDefault();
        Path path = directory.resolve("shared");
        UUID leftGroup = UUID.randomUUID(), rightGroup = UUID.randomUUID();
        byte[] boundary = {(byte) 0x80}, low = {1}, high = {(byte) 0x90};
        try (var pool = open(path, config)) {
            var left = range(pool, leftGroup, null, boundary);
            int families = physicalFamilyCount(pool);
            var right = range(pool, rightGroup, boundary, null);
            assertEquals(families, physicalFamilyCount(pool));
            left.apply(new ApplyPosition(1, 0), put(leftGroup, low, new byte[] {11}), new byte[0]);
            right.apply(new ApplyPosition(1, 0), put(rightGroup, high, new byte[] {22}), new byte[0]);
            try (var pin = left.capture()) { assertArrayEquals(new byte[] {11}, pin.get(COLUMN, low)); }
            try (var pin = right.capture()) { assertArrayEquals(new byte[] {22}, pin.get(COLUMN, high)); }
        }
        try (var pool = open(path, config)) {
            var left = range(pool, leftGroup, null, boundary);
            var right = range(pool, rightGroup, boundary, null);
            assertEquals(new ApplyPosition(1, 0), left.applied());
            assertEquals(new ApplyPosition(1, 0), right.applied());
            try (var pin = right.capture()) { assertArrayEquals(new byte[] {22}, pin.get(COLUMN, high)); }
        }
    }

    @Test void pinRefusesCloseRetainsAllNativeOwnersAndAllowsRetryAfterRelease() throws Exception {
        var config = parse("""
                database.global: {
                  block-cache: "8MiB"
                  write-buffer-manager: "4MiB"
                  block-caches: [{ name: "range-data", size: "2MiB" }]
                  fallback-column-options.block-cache-name: "range-data"
                }
                """);
        var pool = open(directory.resolve("pins"), config);
        var loaded = loaded(pool);
        assertTrue(loaded.caches().containsKey("range-data"));
        var manager = loaded.refs().asList().stream().filter(WriteBufferManager.class::isInstance)
                .map(WriteBufferManager.class::cast).findFirst().orElseThrow();
        UUID group = UUID.randomUUID();
        var range = range(pool, group, null, null);
        byte[] key = {1}, value = {7};
        range.apply(new ApplyPosition(1, 0), put(group, key, value), new byte[0]);
        var pin = range.capture();
        try {
            assertFalse(pool.close(Duration.ZERO));
            assertTrue(loaded.db().get().isOwningHandle());
            assertTrue(loaded.dbOptions().isOwningHandle());
            assertTrue(manager.isOwningHandle());
            assertTrue(loaded.caches().values().stream().allMatch(cache -> cache.isOwningHandle()));
            assertTrue(loaded.definitiveColumnFamilyOptionsMap().values().stream()
                    .allMatch(options -> options.isOwningHandle()));
            assertTrue(loaded.refs().asList().stream().filter(AbstractImmutableNativeReference.class::isInstance)
                    .map(AbstractImmutableNativeReference.class::cast).allMatch(ref -> ref.isOwningHandle()));
            assertArrayEquals(value, pin.get(COLUMN, key));
            assertThrows(IllegalStateException.class, () -> range(pool, UUID.randomUUID(), null, null));
        } finally { pin.close(); pool.close(); }
        assertFalse(loaded.db().get().isOwningHandle());
        assertFalse(loaded.dbOptions().isOwningHandle());
        assertFalse(manager.isOwningHandle());
        assertTrue(pool.close(Duration.ZERO));
    }

    @Test void explicitInternalFamiliesReopenWithoutFallbackMergeAndStandaloneKeepsUserOptions() throws Exception {
        var config = parse("""
                database.global.fallback-column-options.merge-operator-class:
                  "it.cavallium.rockserver.core.impl.MyStringAppendOperator"
                """);
        Path path = directory.resolve("internal");
        UUID group = UUID.randomUUID();
        for (int attempt = 0; attempt < 2; attempt++) {
            try (var pool = open(path, config)) {
                assertNull(loaded(pool).mergeOperators().get(DistributedStoragePool.STATE_COLUMN));
                assertNull(loaded(pool).mergeOperators().get(DistributedStoragePool.RECEIPTS_COLUMN));
                var range = range(pool, group, null, null);
                if (attempt == 0) range.applyNoop(new ApplyPosition(1, 0));
                else assertEquals(new ApplyPosition(1, 0), range.applied());
            }
        }
        var standalone = RocksDBLoader.load(path, config, LoggerFactory.getLogger(getClass()));
        try {
            assertNotNull(standalone.mergeOperators().get(DistributedStoragePool.STATE_COLUMN));
            assertNotNull(standalone.mergeOperators().get(DistributedStoragePool.RECEIPTS_COLUMN));
        } finally { standalone.db().close(); standalone.refs().close(); }
    }

    @Test void pessimisticRejectedBeforeCreatingDatabaseAndAliasesAndMergeMismatchRejected() throws Exception {
        var pessimistic = parse("database.global.optimistic: false");
        Path rejected = directory.resolve("rejected");
        assertThrows(IllegalArgumentException.class, () -> open(rejected, pessimistic));
        assertFalse(Files.exists(rejected));
        try (var pool = open(directory.resolve("bindings"), ConfigParser.parseDefault())) {
            range(pool, UUID.randomUUID(), null, new byte[] {5});
            var upper = new RangeOwnership.Interval(DOMAIN, Set.of(COLUMN), new byte[] {5}, null);
            assertThrows(IllegalArgumentException.class, () -> pool.openRange(new RangeOwnership.Owner(UUID.randomUUID(), 1),
                    upper, new byte[32], Map.of(COLUMN, "different"), Set.of()));
            assertThrows(IllegalArgumentException.class, () -> pool.openRange(new RangeOwnership.Owner(UUID.randomUUID(), 1),
                    upper, new byte[32], COLUMNS, Set.of(COLUMN)));
            UUID alias = UUID.randomUUID();
            assertThrows(IllegalArgumentException.class, () -> pool.openRange(new RangeOwnership.Owner(UUID.randomUUID(), 1),
                    new RangeOwnership.Interval(UUID.randomUUID(), Set.of(alias), new byte[] {5}, null),
                    new byte[32], Map.of(alias, "shared_data"), Set.of()));
        }
    }

    @Test void poolExportUsesOwnedOptionsAndHistoricalCutAndRejectsForeignOrClosedAdmission() throws Exception {
        var config = ConfigParser.parseDefault();
        var limits = new NativeRangeSstExport.Limits(10, 1024, 1024 * 1024, Duration.ofSeconds(10));
        try (var pool = open(directory.resolve("export-source"), config);
                var other = open(directory.resolve("export-other"), config)) {
            UUID group = UUID.randomUUID();
            var store = range(pool, group, null, null);
            store.apply(new ApplyPosition(1, 0), put(group, new byte[] {1}, new byte[] {7}), new byte[0]);
            var scope = new TransactionScope(List.of(TransactionScope.Span.range(COLUMN, null, null)));
            try (var snapshot = store.capture(scope)) {
                store.apply(new ApplyPosition(1, 1), new DistributedCommand(group, 1, new byte[32],
                        new OperationId(SESSION, 1, 1), List.of(new Mutation(Mutation.Kind.PUT, COLUMN,
                        new byte[] {1}, new byte[] {8}))), new byte[0]);
                assertThrows(IllegalArgumentException.class, () -> other.exportSst(directory.resolve("foreign.sst"),
                        snapshot, COLUMN, null, null, limits));
                assertThrows(IllegalArgumentException.class, () -> pool.exportSst(directory.resolve("wrong-column.sst"),
                        snapshot, UUID.randomUUID(), null, null, limits));
                var manifest = pool.exportSst(directory.resolve("owned.sst"), snapshot, COLUMN, null, null, limits);
                assertEquals(new ApplyPosition(1, 0), manifest.position());
                assertEquals(1, manifest.rows());
                assertEquals(1, store.activeSnapshotPins());
                try (var options = new org.rocksdb.Options().setCreateIfMissing(true);
                        var target = org.rocksdb.RocksDB.open(options, directory.resolve("export-target").toString());
                        var ingest = new org.rocksdb.IngestExternalFileOptions().setMoveFiles(false)) {
                    target.ingestExternalFile(List.of(manifest.file().toString()), ingest);
                    assertArrayEquals(new byte[] {7}, target.get(new byte[] {1}));
                }
                assertFalse(pool.close(Duration.ZERO));
                assertThrows(IllegalStateException.class, () -> pool.exportSst(directory.resolve("closing.sst"),
                        snapshot, COLUMN, null, null, limits));
                assertArrayEquals(new byte[] {7}, snapshot.get(COLUMN, new byte[] {1}));
            }
            assertTrue(pool.close(Duration.ofSeconds(10)));
        }
    }

    private RocksDBLoader.LoadedDb loaded(DistributedStoragePool pool) throws Exception {
        var field = DistributedStoragePool.class.getDeclaredField("loaded");
        field.setAccessible(true);
        return (RocksDBLoader.LoadedDb) field.get(pool);
    }

    private int physicalFamilyCount(DistributedStoragePool pool) throws Exception {
        try (var options = new org.rocksdb.Options()) {
            return org.rocksdb.RocksDB.listColumnFamilies(options, loaded(pool).definitiveDbPath().toString()).size();
        }
    }
}
