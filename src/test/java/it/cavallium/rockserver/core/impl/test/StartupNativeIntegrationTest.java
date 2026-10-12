package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.*;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.ColumnInstance;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.rocksdb.NativeStartupBridge;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import it.cavallium.rockserver.core.impl.rocksdb.SnapshotCache;
import it.cavallium.rockserver.core.impl.rocksdb.TransactionalDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.CompactionOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.slf4j.LoggerFactory;

/** Explicit candidate-only gate: routine builds do not require the experimental C ABI. */
@EnabledIfSystemProperty(named = "rockserver.startup.native.tests", matches = "true")
@Timeout(90)
class StartupNativeIntegrationTest {
    private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(4), ObjectList.of(), true);
    private static final int FILES = 6, KEYS_PER_FILE = 256;
    private static final Pattern COUNTER = Pattern.compile("\\\"([^\\\"]+)\\\":(\\d+)");
    @TempDir Path directory;

    @Test
    void embeddedReadsCaptureAndReplayAcrossReopenAndDynamicColumnLifecycle() throws Exception {
        Path dbPath = directory.resolve("db");
        Path config = config("workflow", true, "0B");
        seed(dbPath, config, "seed");
        List<SnapshotCache> closedCaches;
        try (var connection = new EmbeddedConnection(dbPath, "capture", config)) {
            var internal = connection.getInternalDB();
            var resources = resources(internal);
            closedCaches = resources.snapshotCaches();
            assertEquals(2, closedCaches.size());
            assertTrue(resources.settings().admission().enabled());
            verifyReads(connection);
            await(() -> closedCaches.stream().allMatch(c -> counter(c, "feedback") == 0), "idle capture permission");
            // Re-read after feedback becomes idle so the collector observes actual native blocks.
            verifyReads(connection);
            assertTrue(closedCaches.stream().allMatch(c -> counter(c, "sampled") > 0));
            for (var cache : closedCaches) {
                var result = cache.snapshot();
                assertEquals(0, result.status(), result::message);
                assertTrue(counter(cache, "snapshots") > 0);
                assertTrue(counter(cache, "snapshot_bytes") > 128);
                assertEquals(0, counter(cache, "errors"));
            }
            var api = connection.getSyncApi(RequestContext.batch());
            long temporary = api.createColumn("temporary", SCHEMA);
            api.put(0, temporary, key(7), value(7), RequestType.none());
            assertEquals(value(7), api.get(0, temporary, key(7), RequestType.current()));
            api.deleteColumn(temporary);
            assertAdmissionConservation(resources.budgets().values().iterator().next().snapshot());
        }
        closedCaches.forEach(cache -> {
            assertFalse(cache.isOwningHandle());
            assertThrows(IllegalStateException.class, cache::stats);
        });
        assertControllerStopped("capture");
        for (int restart = 0; restart < 2; restart++) {
            try (var connection = new EmbeddedConnection(dbPath, "replay-" + restart, config)) {
                verifyReads(connection);
                var caches = resources(connection.getInternalDB()).snapshotCaches();
                assertTrue(counter(caches.getFirst(), "data_hits") > 0, caches.getFirst()::stats);
                assertTrue(counter(caches.getLast(), "index_hits") + counter(caches.getLast(), "filter_hits") > 0,
                        caches.getLast()::stats);
                var api = connection.getSyncApi(RequestContext.batch());
                long recreated = api.createColumn("temporary", SCHEMA);
                assertNull(api.get(0, recreated, key(7), RequestType.current()));
                api.deleteColumn(recreated);
            }
            assertControllerStopped("replay-" + restart);
        }
    }

    @Test
    void twoDatabasesShareOneNativeCapAndClosingOneLeavesTheOtherUsable() throws Exception {
        String storage = "shared_" + UUID.randomUUID().toString().replace('-', '_');
        Path config = config(storage, true, "0B");
        Path firstPath = directory.resolve("first"), secondPath = directory.resolve("second");
        seed(firstPath, config, "seed-first");
        seed(secondPath, config, "seed-second");
        // A third publisher retains the shared counter owner for conservation checks after both DBs close.
        try (var observer = NativeStartupBridge.createBudget(new NativeStartupBridge.BudgetConfig(storage, 1, 20, 2000))) {
            try (var first = new EmbeddedConnection(firstPath, "shared-first", config);
                    var second = new EmbeddedConnection(secondPath, "shared-second", config)) {
                try {
                    assertTrue(observer.snapshot().publishers() >= 3);
                    verifyReads(first);
                    verifyReads(second);
                    await(() -> observer.snapshot().active() == 0 && observer.snapshot().queued() == 0,
                            "both asynchronous opens drain");
                    assertTrue(observer.snapshot().completedFindTable() >= FILES * 2);
                    assertEquals(observer.snapshot().admitted(),
                            resources(second.getInternalDB()).budgets().get(storage).snapshot().admitted());
                    assertAdmissionConservation(observer.snapshot());
                } finally {
                    first.close();
                }
                assertControllerStopped("shared-first");
                verifyReads(second);
                assertAdmissionConservation(observer.snapshot());
                assertTrue(observer.snapshot().publishers() >= 2);
            }
            assertControllerStopped("shared-second");
            var drained = observer.snapshot();
            assertEquals(0, drained.active());
            assertEquals(0, drained.queued());
            assertEquals(drained.admitted(), drained.released());
            assertEquals(1, drained.publishers());
            assertAdmissionConservation(drained);
        }
    }

    @Test
    void liveSnapshotNamespaceContentionRejectsSecondLoaderWithoutDisturbingOwner() throws Exception {
        Path dbPath = directory.resolve("exclusive");
        Path config = config("exclusive", true, "0B");
        seed(dbPath, config, "seed-exclusive");
        try (var owner = new EmbeddedConnection(dbPath, "exclusive-owner", config)) {
            var parsed = ConfigParser.parse(config);
            var failure = assertThrows(IllegalStateException.class,
                    () -> RocksDBLoader.load(dbPath, parsed, LoggerFactory.getLogger(getClass())));
            assertTrue(failure.getMessage().toLowerCase().contains("cache"), failure::toString);
            verifyReads(owner);
            assertEquals(2, resources(owner.getInternalDB()).snapshotCaches().size());
        }
        assertControllerStopped("exclusive-owner");
        try (var reopened = new EmbeddedConnection(dbPath, "exclusive-reopened", config)) {
            verifyReads(reopened);
        }
    }

    @Test
    void actualNativeCaptureCancellationReturnsBeforeHolderDisposal() throws Exception {
        Path dbPath = directory.resolve("cancel");
        Path config = config("cancel", true, "128KiB");
        seed(dbPath, config, "seed-cancel");
        try (var connection = new EmbeddedConnection(dbPath, "cancel-owner", config)) {
            verifyReads(connection);
            var cache = resources(connection.getInternalDB()).snapshotCaches().getFirst();
            await(() -> counter(cache, "feedback") == 0, "idle capture permission");
            verifyReads(connection);
            assertTrue(counter(cache, "sampled") > 0);
            try (var executor = Executors.newSingleThreadExecutor()) {
                var capture = executor.submit(cache::snapshot);
                await(() -> stagingFileExists() || capture.isDone(), "native staging file creation");
                assertFalse(capture.isDone(), "paced native capture must still own its staging file");
                cache.feedback(1);
                var result = capture.get(10, TimeUnit.SECONDS);
                assertEquals(1, result.status(), result::message);
                assertTrue(counter(cache, "aborts") > 0, cache::stats);
                assertTrue(cache.isOwningHandle(), "cancellation releases capture ownership before DB closes holder");
            }
        }
        assertControllerStopped("cancel-owner");
    }

    private Path config(String storage, boolean snapshots, String writeCeiling) throws Exception {
        return Files.writeString(directory.resolve(storage + ".conf"), """
                database.metrics.jmx.enabled = false
                database.metrics.influx.enabled = false
                database.global.enable-fast-get = true
                database.global.adaptive-compaction-io = false
                database.global.use-direct-io = false
                database.global.disable-auto-compactions = true
                database.global.maximum-open-files = -1
                database.global.open-files-async = true
                database.global.max-file-opening-threads = 4
                database.global.block-cache = 4MiB
                database.global.block-cache-metadata-size = 1MiB
                database.global.write-buffer-manager = 1MiB
                database.global.startup-io: {
                  enabled: true
                  default-storage-id: "%s"
                  storage-groups: [{id: "%s", maximum-opens: 1, progress-interval: PT0.02S,
                    stale-after: PT2S, paths: ["%s"]}]
                }
                database.global.hot-cache-snapshot: {
                  enabled: %s
                  directory: "%s"
                  data: {disk-budget: 8MiB, snapshot-budget: 2MiB, collector-entries: 1024,
                    index-ram: 256KiB, maximum-block: 64KiB, lookup-buffer: 1MiB,
                    minimum-free-disk: 0B, snapshot-deadline: PT30S, period: PT0S,
                    sample-every: 1, lookup-concurrency: 2, read-ceiling: 0B, write-ceiling: "%s"}
                  metadata: ${database.global.hot-cache-snapshot.data}
                }
                database.global.fallback-column-options: {
                  cache-index-and-filter-blocks: true
                  pin-index-and-filter-blocks: false
                  partition-filters: true
                  block-size: 4KiB
                  volumes: []
                  levels: []
                  bloom-filter: {bits-per-key: 10, use-ribbon: false, optimize-for-hits: false}
                }
                """.formatted(storage, storage, directory, snapshots, directory.resolve("snapshots"), writeCeiling));
    }

    private void seed(Path dbPath, Path config, String name) throws Exception {
        try (var connection = new EmbeddedConnection(dbPath, name, config)) {
            var api = connection.getSyncApi(RequestContext.batch());
            long column = api.createColumn("entries", SCHEMA);
            var internal = connection.getInternalDB();
            var nativeDb = ((TransactionalDB) field(internal, "db")).get();
            var handle = ((ColumnInstance) ((Map<?, ?>) field(internal, "columns")).get(column)).cfh();
            try (var flush = new FlushOptions().setWaitForFlush(true)) {
                for (int file = 0; file < FILES; file++) {
                    for (int offset = 0; offset < KEYS_PER_FILE; offset++) {
                        int key = file * KEYS_PER_FILE + offset;
                        nativeDb.put(handle, keyBytes(key), valueBytes(key));
                    }
                    nativeDb.flush(flush, handle);
                }
            }
            var inputs = nativeDb.getLiveFilesMetaData().stream()
                    .filter(f -> new String(f.columnFamilyName(), java.nio.charset.StandardCharsets.UTF_8).equals("entries"))
                    .map(org.rocksdb.LiveFileMetaData::fileName).toList();
            try (var compact = new CompactionOptions().setOutputFileSizeLimit(1L << 20)
                    .setCompression(CompressionType.NO_COMPRESSION); var job = new CompactionJobInfo()) {
                nativeDb.compactFiles(compact, handle, inputs, 1, 0, job);
            }
            assertTrue(nativeDb.getLiveFilesMetaData().stream().filter(f ->
                    new String(f.columnFamilyName(), java.nio.charset.StandardCharsets.UTF_8).equals("entries")).count() >= FILES);
        }
        assertControllerStopped(name);
    }

    private void verifyReads(EmbeddedConnection connection) throws Exception {
        var api = connection.getSyncApi(RequestContext.batch(Duration.ofSeconds(10)));
        long column = api.getColumnId("entries");
        var keys = new ArrayList<Keys>();
        // Keep a bounded hot tail resident in DATA, with metadata from every file.
        for (int index = 0; index < 64; index++) {
            int number = FILES * KEYS_PER_FILE - 64 + index;
            assertEquals(value(number), api.get(0, column, key(number), RequestType.current()));
            keys.add(key(number));
        }
        assertEquals(java.util.Collections.nCopies(keys.size(), true), api.existsMulti(0, column, keys));
        for (int file = 0; file < FILES; file++) {
            int number = file * KEYS_PER_FILE;
            assertEquals(value(number), api.get(0, column, key(number), RequestType.current()));
        }
        try (var pinned = connection.getInternalDB().tryGetFast(column, keys.getFirst(), EmbeddedDB.FastGetOutput.PINNED)) {
            assertNotNull(pinned);
            assertTrue(pinned.isPresent());
            assertTrue(pinned.isPinned());
            assertEquals(value(FILES * KEYS_PER_FILE - 64), pinned.value());
        }
    }

    private static void assertAdmissionConservation(NativeStartupBridge.AdmissionStats stats) {
        assertEquals(stats.active(), stats.admitted() - stats.released());
        assertTrue(stats.peakActive() <= 1, stats::toString);
        assertTrue(stats.completedFindTable() <= stats.released(), stats::toString);
        assertTrue(stats.failed() <= stats.completedFindTable(), stats::toString);
    }

    private boolean stagingFileExists() {
        try (var files = Files.walk(directory.resolve("snapshots"))) {
            return files.anyMatch(p -> p.getFileName().toString().startsWith("tmp-"));
        } catch (java.io.IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }

    private static long counter(SnapshotCache cache, String name) {
        var match = COUNTER.matcher(cache.stats());
        while (match.find()) if (match.group(1).equals(name)) return Long.parseLong(match.group(2));
        throw new AssertionError("Missing native counter " + name + ": " + cache.stats());
    }

    private static void await(BooleanSupplier condition, String description) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) fail("Timed out waiting for " + description);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
    }

    private static Object field(Object owner, String name) throws Exception {
        var field = owner.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(owner);
    }

    private static RocksDBLoader.StartupResources resources(EmbeddedDB db) throws Exception {
        return (RocksDBLoader.StartupResources) field(db, "startupResources");
    }

    private static void assertControllerStopped(String name) {
        assertFalse(Thread.getAllStackTraces().keySet().stream().anyMatch(t ->
                t.isAlive() && t.getName().equals("startup-io[" + name + "]")), "controller worker must join before close returns");
    }

    private static byte[] keyBytes(int number) { return ByteBuffer.allocate(4).putInt(number).array(); }
    private static Keys key(int number) { return new Keys(Buf.wrap(keyBytes(number))); }
    private static byte[] valueBytes(int number) {
        byte[] value = new byte[4096];
        new Random(number).nextBytes(value);
        return value;
    }
    private static Buf value(int number) { return Buf.wrap(valueBytes(number)); }
}
