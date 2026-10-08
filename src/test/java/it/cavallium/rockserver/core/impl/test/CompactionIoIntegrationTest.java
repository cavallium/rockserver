package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
class CompactionIoIntegrationTest {
    private Path config(Path root, String extra) throws Exception {
        var path = root.resolve("adaptive.conf");
        Files.writeString(path, "database.global.adaptive-compaction-io=true\n"
                + "database.global.write-buffer-manager=\"16MiB\"\n"
                + "database.global.fallback-column-options.volumes=[{volume-path=\".\",target-size=\"10GiB\"}]\n"
                + extra + "\ndatabase.global.column-options=[${database.global.fallback-column-options}{name:\"default\"}]\n");
        return path;
    }
    @Test void putFlushReadCloseAndReopenWithActiveController(@TempDir Path root) throws Exception {
        var cfg = config(root, "database.global.fallback-column-options.compaction-ttl=PT24H\n");
        var dbPath = root.resolve("db");
        var key = new Keys(new Buf[]{Buf.wrap(new byte[]{1})});
        var value = Buf.wrap(new byte[]{2, 3});
        try (var connection = new EmbeddedConnection(dbPath, "adaptive-workflow", cfg)) {
            var api = connection.getSyncApi(RequestContext.latency(java.time.Duration.ofSeconds(10)));
            var col = connection.getSyncApi(RequestContext.batch()).createColumn("data", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            for (int i = 0; i < 3; i++) {
                api.put(0, col, key, value, RequestType.none());
                connection.getSyncApi(RequestContext.batch()).flush();
                assertEquals(value, api.get(0, col, key, RequestType.current()));
            }
            connection.getSyncApi(RequestContext.batch()).compact();
            assertTrue(connection.getInternalDB().getMetricsRegistry().find("rockserver.compaction.completed")
                    .timers().stream().anyMatch(timer -> timer.count() > 0));
            // Force at least one actual leased native sample, including histogram/limiter counters.
            var sampled = new CountDownLatch(1);
            connection.getInternalDB().setCompactionIoSampleObserverForTesting(sampled::countDown);
            assertTrue(sampled.await(5, TimeUnit.SECONDS));
        }
        try (var connection = new EmbeddedConnection(dbPath, "adaptive-reopen", cfg)) {
            var api = connection.getSyncApi(RequestContext.latency(java.time.Duration.ofSeconds(10)));
            assertEquals(value, api.get(0, api.getColumnId("data"), key, RequestType.current()));
        }
    }
    @Test void closeJoinsSamplerBeforeClosingNativeResources(@TempDir Path root) throws Exception {
        var connection = new EmbeddedConnection(root.resolve("db"), "adaptive-close", config(root, ""));
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var closing = new CountDownLatch(1);
        connection.getInternalDB().setCompactionIoSampleObserverForTesting(() -> {
            entered.countDown();
            try { assertTrue(release.await(10, TimeUnit.SECONDS)); }
            catch (InterruptedException failure) { throw new AssertionError(failure); }
        });
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var close = executor.submit(() -> { closing.countDown(); connection.closeTesting(); return null; });
            try {
                assertTrue(closing.await(5, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
                assertTrue(connection.getInternalDB().getPendingOpsCount() > 0);
            } finally { release.countDown(); }
            close.get(10, TimeUnit.SECONDS);
        } finally { release.countDown(); connection.close(); }
        try (var reopened = new EmbeddedConnection(root.resolve("db"), "adaptive-close-reopen", config(root, ""))) {
            assertNotNull(reopened.getSyncApi(RequestContext.latency(java.time.Duration.ofSeconds(10))));
        }
    }
    @Test void ttlDefaultsExplicitPolicyAndNativeLimiterOptions(@TempDir Path root) throws Exception {
        for (String ttl : new String[]{"", "PT0S", "PT48H"}) {
            var cfg = config(root, ttl.isEmpty() ? "" : "database.global.fallback-column-options.compaction-ttl=" + ttl);
            var loaded = RocksDBLoader.load(root.resolve("db" + ttl), ConfigParser.parse(cfg), LoggerFactory.getLogger(getClass()));
            try {
                assertEquals(ttl.isEmpty() ? -2 : ttl.equals("PT0S") ? 0 : 2 * 86400,
                        loaded.definitiveColumnFamilyOptionsMap().get("default").ttl());
                assertNotNull(loaded.compactionIoLimiter());
                assertEquals(16 * 1024 * 1024 / 5, loaded.compactionIoLimiter().getBytesPerSecond());
                assertEquals(0, loaded.dbOptions().compactionReadaheadSize());
            } finally { loaded.db().close(); loaded.refs().close(); }
        }
    }
    @Test void failedSamplerRestoresNativeProbeBudget(@TempDir Path root) throws Exception {
        RocksDBLoader.loadLibrary();
        var registry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        try (var limiter = new org.rocksdb.RateLimiter(1_000_000, 100_000,
                org.rocksdb.RateLimiter.DEFAULT_FAIRNESS, org.rocksdb.RateLimiterMode.ALL_IO, false)) {
            var calls = new java.util.concurrent.atomic.AtomicInteger();
            var reduced = new java.util.concurrent.atomic.AtomicLong();
            var failedTwice = new CountDownLatch(1);
            java.util.function.Supplier<it.cavallium.rockserver.core.impl.CompactionIoBudget.Sample> sample = () -> {
                int call = calls.incrementAndGet();
                if (call >= 7) {
                    if (call == 7) reduced.set(limiter.getBytesPerSecond());
                    else failedTwice.countDown();
                    throw new IllegalStateException("injected sampling failure");
                }
                return new it.cavallium.rockserver.core.impl.CompactionIoBudget.Sample(call * 1_000_000_000L,
                        call * 10, call * 1000, call * 1_000_000L, true, false, false, true);
            };
            var constructor = Class.forName("it.cavallium.rockserver.core.impl.CompactionIoController")
                    .getDeclaredConstructor(String.class, org.rocksdb.RateLimiter.class,
                            java.util.function.Supplier.class, io.micrometer.core.instrument.MeterRegistry.class);
            constructor.setAccessible(true);
            try (var controller = (AutoCloseable) constructor.newInstance("sample-failure", limiter, sample, registry)) {
                assertTrue(failedTwice.await(12, TimeUnit.SECONDS));
                assertEquals(750_000, reduced.get());
                assertEquals(1_000_000, limiter.getBytesPerSecond());
            }
        } finally { registry.close(); }
    }
    @Test void zeroConfiguredWriteBufferStillBootstrapsAboveAlignmentFloor(@TempDir Path root) throws Exception {
        var cfg = config(root, "database.global.write-buffer-manager=0\n");
        var loaded = RocksDBLoader.load(root.resolve("zero-wbm"), ConfigParser.parse(cfg), LoggerFactory.getLogger(getClass()));
        try {
            assertTrue(loaded.compactionIoLimiter().getBytesPerSecond()
                    > it.cavallium.rockserver.core.impl.CompactionIoBudget.MIN_BYTES_PER_SECOND);
        } finally { loaded.db().close(); loaded.refs().close(); }
    }
    @Test void invalidTtlAndDisabledNativeProtectionsAreRejected(@TempDir Path root) throws Exception {
        for (String policy : new String[]{"fallback-column-options.compaction-ttl=PT-1S",
                "fallback-column-options.compaction-ttl=PT1.5S", "disable-auto-compactions=true",
                "disable-write-slowdown=true", "max-background-jobs=0", "fallback-column-options.disable-auto-compactions=true"}) {
            var cfg = config(root, "database.global." + policy);
            assertThrows(RocksDBException.class, () -> RocksDBLoader.load(root.resolve("invalid"),
                    ConfigParser.parse(cfg), LoggerFactory.getLogger(getClass())), policy);
        }
    }
}
