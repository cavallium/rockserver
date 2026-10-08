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
        var cfg = config(root, "database.global.fallback-column-options.compaction-ttl=PT24H\n"
                + "database.global.block-cache=4MiB\n"
                + "database.global.block-cache-metadata-size=1MiB\n");
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
    @Test void healthyNativeSamplerReadsL0FilesAndPollsWithoutFailures(@TempDir Path root) throws Exception {
        var cfg = config(root, "database.metrics.jmx.enabled=false\n"
                + "database.metrics.influx.enabled=false\n");
        var registry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        try (var connection = new EmbeddedConnection(root.resolve("db"), "healthy-native-sampler", cfg)) {
            var internal = connection.getInternalDB();
            ((io.micrometer.core.instrument.composite.CompositeMeterRegistry) internal.getMetricsRegistry()).add(registry);
            var api = connection.getSyncApi(RequestContext.batch());
            var column = api.createColumn("sampler-data", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            var controllerField = internal.getClass().getDeclaredField("compactionIoController");
            controllerField.setAccessible(true);
            var controller = controllerField.get(internal);
            var limiterField = controller.getClass().getDeclaredField("limiter");
            limiterField.setAccessible(true);
            var limiter = (org.rocksdb.RateLimiter) limiterField.get(controller);
            var sampleMethod = internal.getClass().getDeclaredMethod("sampleCompactionIo", org.rocksdb.RateLimiter.class);
            sampleMethod.setAccessible(true);
            var empty = (it.cavallium.rockserver.core.impl.CompactionIoBudget.Sample) sampleMethod.invoke(internal, limiter);
            assertTrue(empty.valid());
            assertFalse(empty.pressure());
            assertTrue(empty.recoveryClear());

            api.put(0, column, new Keys(Buf.wrap(new byte[]{1})), Buf.wrap(new byte[]{2}), RequestType.none());
            api.flush();
            var dbField = internal.getClass().getDeclaredField("db");
            dbField.setAccessible(true);
            var nativeDb = ((it.cavallium.rockserver.core.impl.rocksdb.TransactionalDB) dbField.get(internal)).get();
            var columnsField = internal.getClass().getDeclaredField("columns");
            columnsField.setAccessible(true);
            var registeredColumn = (it.cavallium.rockserver.core.impl.ColumnInstance)
                    ((java.util.Map<?, ?>) columnsField.get(internal)).get(column);
            var handle = registeredColumn.cfh();
            long levelZeroFiles = Long.parseUnsignedLong(nativeDb.getProperty(handle, "rocksdb.num-files-at-level0"));
            assertTrue(levelZeroFiles > 0, "the regression must sample a real flushed L0 SST");
            var perColumnProperty = internal.getClass().getDeclaredMethod("getPerCfLongProperty", String.class);
            perColumnProperty.setAccessible(true);
            var counts = (java.util.Map<?, ?>) perColumnProperty.invoke(internal, "rocksdb.num-files-at-level0");
            assertEquals(Long.valueOf(levelZeroFiles), counts.get("sampler-data"));
            var aggregateProperty = internal.getClass().getDeclaredMethod("getLongProperty", String.class,
                    it.cavallium.rockserver.core.impl.RocksDBLongProperty.AggregationMode.class);
            aggregateProperty.setAccessible(true);
            assertEquals(java.math.BigInteger.valueOf(levelZeroFiles), aggregateProperty.invoke(internal,
                    "rocksdb.num-files-at-level0", it.cavallium.rockserver.core.impl.RocksDBLongProperty.AggregationMode.PER_CF));
            assertThrows(org.rocksdb.RocksDBException.class,
                    () -> nativeDb.getLongProperty(handle, "rocksdb.num-files-at-level0"),
                    "the native registry exposes this count only through the string-property API");
            assertEquals(Long.valueOf(0), ((java.util.Map<?, ?>) perColumnProperty.invoke(internal,
                    "rocksdb.num-files-at-level1")).get("sampler-data"));
            assertTrue(((java.util.Map<?, ?>) perColumnProperty.invoke(internal, "rocksdb.num-files-at-level9")).isEmpty());
            // Isolate the cached CF trigger from DB-wide delay using the real flushed L0 count.
            assertEquals(0, nativeDb.getLongProperty("rocksdb.actual-delayed-write-rate"));
            var configuredColumnsField = internal.getClass().getDeclaredField("columnsConifg");
            configuredColumnsField.setAccessible(true);
            var columnOptions = (org.rocksdb.ColumnFamilyOptions)
                    ((java.util.Map<?, ?>) configuredColumnsField.get(internal)).get("sampler-data");
            var pressureColumnsField = internal.getClass().getDeclaredField("storagePressureColumns");
            pressureColumnsField.setAccessible(true);
            var originalPressureColumns = (Object[]) pressureColumnsField.get(internal);
            var cachePressureColumn = internal.getClass().getDeclaredMethod("storagePressureColumn", String.class,
                    org.rocksdb.ColumnFamilyHandle.class);
            cachePressureColumn.setAccessible(true);
            int originalSlowdown = columnOptions.level0SlowdownWritesTrigger();
            try {
                for (int trigger : new int[]{1, 2, -1}) {
                    columnOptions.setLevel0SlowdownWritesTrigger(trigger);
                    var configured = cachePressureColumn.invoke(internal, "sampler-data", handle);
                    var cachedTrigger = configured.getClass().getDeclaredMethod("effectiveLevel0SlowdownWritesTrigger");
                    cachedTrigger.setAccessible(true);
                    assertEquals(trigger, cachedTrigger.invoke(configured));
                    var constructor = configured.getClass().getDeclaredConstructors()[0];
                    constructor.setAccessible(true);
                    configured = constructor.newInstance((long) handle.getID(), handle, registeredColumn,
                            columnOptions.softPendingCompactionBytesLimit(), cachedTrigger.invoke(configured));
                    var snapshot = originalPressureColumns.clone();
                    var cachedHandle = configured.getClass().getDeclaredMethod("handle");
                    cachedHandle.setAccessible(true);
                    for (int index = 0; index < snapshot.length; index++) {
                        if (((org.rocksdb.ColumnFamilyHandle) cachedHandle.invoke(snapshot[index])).getID() == handle.getID()) {
                            snapshot[index] = configured;
                        }
                    }
                    pressureColumnsField.set(internal, snapshot);
                    var classified = (it.cavallium.rockserver.core.impl.CompactionIoBudget.Sample) sampleMethod.invoke(internal, limiter);
                    assertEquals(trigger >= 0 && levelZeroFiles >= trigger, classified.urgentPressure());
                    assertEquals(classified.urgentPressure(), classified.pressure());
                    assertEquals(!classified.urgentPressure(), classified.recoveryClear());
                }
            } finally {
                pressureColumnsField.set(internal, originalPressureColumns);
                columnOptions.setLevel0SlowdownWritesTrigger(originalSlowdown);
            }
            var flushed = (it.cavallium.rockserver.core.impl.CompactionIoBudget.Sample) sampleMethod.invoke(internal, limiter);
            assertTrue(flushed.valid());
            assertFalse(flushed.pressure());
            assertTrue(flushed.recoveryClear());
            var polls = new CountDownLatch(2);
            internal.setCompactionIoSampleObserverForTesting(polls::countDown);
            assertTrue(polls.await(5, TimeUnit.SECONDS));
            assertTimeoutPreemptively(java.time.Duration.ofSeconds(2), () -> {
                while (internal.getPendingOpsCount() != 0) Thread.sleep(10);
            });
            assertEquals(0, registry.get("rockserver.compaction.io.adjustment.failures").gauge().value());
            var collectorField = internal.getClass().getDeclaredField("rocksDBStatistics");
            collectorField.setAccessible(true);
            var collector = collectorField.get(internal);
            var executorField = collector.getClass().getDeclaredField("executor");
            executorField.setAccessible(true);
            var collectorThread = (Thread) executorField.get(collector);
            var optionsField = internal.getClass().getDeclaredField("dbOptions");
            optionsField.setAccessible(true);
            var readCounter = registry.get("rocksdb.statistics").tag("ticker_name", "NUMBER_KEYS_READ").counter();
            var writeCounter = registry.get("rocksdb.statistics").tag("ticker_name", "NUMBER_KEYS_WRITTEN").counter();
            double initialReadExport = readCounter.count();
            double initialWriteExport = writeCounter.count();
            api.put(0, column, new Keys(Buf.wrap(new byte[]{1})), Buf.wrap(new byte[]{2}), RequestType.none());
            assertNotNull(api.get(0, column, new Keys(Buf.wrap(new byte[]{1})), RequestType.current()));
            collectorThread.interrupt();
            assertTimeoutPreemptively(java.time.Duration.ofSeconds(5), () -> {
                while (readCounter.count() <= initialReadExport || writeCounter.count() <= initialWriteExport) Thread.sleep(10);
            });
            assertTimeoutPreemptively(java.time.Duration.ofSeconds(5), () -> {
                while (registry.find("rocksdb.property.long").tag("property_name", "rocksdb.num-files-at-level0")
                        .tag("column_family", "sampler-data").gauge() == null) Thread.sleep(10);
            });
            assertEquals(levelZeroFiles, registry.get("rocksdb.property.long")
                    .tag("property_name", "rocksdb.num-files-at-level0").tag("column_family", "sampler-data").gauge().value());
            try (var nativeStatistics = ((org.rocksdb.DBOptions) optionsField.get(internal)).statistics()) {
                long previousReads = nativeStatistics.getTickerCount(org.rocksdb.TickerType.NUMBER_KEYS_READ);
                long previousWrites = nativeStatistics.getTickerCount(org.rocksdb.TickerType.NUMBER_KEYS_WRITTEN);
                double previousReadExport = readCounter.count();
                double previousWriteExport = writeCounter.count();
                long previousCompletions = ((it.cavallium.rockserver.core.impl.CompactionIoBudget.Sample)
                        sampleMethod.invoke(internal, limiter)).foregroundCompletions();
                for (int cycle = 0; cycle < 5; cycle++) {
                    if (cycle == 3) nativeStatistics.reset();
                    api.put(0, column, new Keys(Buf.wrap(new byte[]{1})), Buf.wrap(new byte[]{2}), RequestType.none());
                    assertNotNull(api.get(0, column, new Keys(Buf.wrap(new byte[]{1})), RequestType.current()));
                    long reads = nativeStatistics.getTickerCount(org.rocksdb.TickerType.NUMBER_KEYS_READ);
                    long writes = nativeStatistics.getTickerCount(org.rocksdb.TickerType.NUMBER_KEYS_WRITTEN);
                    double expectedReadExport = previousReadExport + (cycle == 3 ? reads : reads - previousReads);
                    double expectedWriteExport = previousWriteExport + (cycle == 3 ? writes : writes - previousWrites);
                    collectorThread.interrupt();
                    assertTimeoutPreemptively(java.time.Duration.ofSeconds(5), () -> {
                        while (readCounter.count() < expectedReadExport || writeCounter.count() < expectedWriteExport) Thread.sleep(10);
                    });
                    assertEquals(expectedReadExport, readCounter.count(), "each native read must be exported once");
                    assertEquals(expectedWriteExport, writeCounter.count(), "each native write must be exported once");
                    assertEquals(reads, nativeStatistics.getTickerCount(org.rocksdb.TickerType.NUMBER_KEYS_READ));
                    assertEquals(writes, nativeStatistics.getTickerCount(org.rocksdb.TickerType.NUMBER_KEYS_WRITTEN));
                    var sample = (it.cavallium.rockserver.core.impl.CompactionIoBudget.Sample) sampleMethod.invoke(internal, limiter);
                    assertTrue(sample.valid());
                    assertEquals(reads + writes, sample.foregroundCompletions());
                    if (cycle != 3) assertTrue(sample.foregroundCompletions() > previousCompletions);
                    else assertTrue(sample.foregroundCompletions() < previousCompletions, "a genuine native reset starts a new epoch");
                    previousReads = reads;
                    previousWrites = writes;
                    previousReadExport = expectedReadExport;
                    previousWriteExport = expectedWriteExport;
                    previousCompletions = sample.foregroundCompletions();
                }
            }
            assertEquals(0, registry.get("rockserver.compaction.io.adjustment.failures").gauge().value());
            assertEquals(0, registry.get("rockserver.compaction.io.baseline.read.micros").gauge().value());
        } finally { registry.close(); }
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
                        call * 10, call * 1000, call * 1_000_000L, true, false, false, true, true, call * 10, true);
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
    @Test void closeRestoresUsefulRateBeforeJoinAndAfterLateSamplerAdjustment() throws Exception {
        RocksDBLoader.loadLibrary();
        var registry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var restored = new CountDownLatch(1);
        var lateThrottle = new CountDownLatch(1);
        var restoreSeen = new java.util.concurrent.atomic.AtomicBoolean();
        try (var limiter = new org.rocksdb.RateLimiter(1_000_000, 100_000,
                org.rocksdb.RateLimiter.DEFAULT_FAIRNESS, org.rocksdb.RateLimiterMode.ALL_IO, false) {
            @Override public void setBytesPerSecond(long bytes) {
                super.setBytesPerSecond(bytes);
                if (bytes == 1_000_000) { restoreSeen.set(true); restored.countDown(); }
                else if (restoreSeen.get() && bytes == 750_000) lateThrottle.countDown();
            }
        }; var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var calls = new java.util.concurrent.atomic.AtomicInteger();
            java.util.function.Supplier<it.cavallium.rockserver.core.impl.CompactionIoBudget.Sample> sample = () -> {
                int call = calls.incrementAndGet();
                if (call == 7) {
                    entered.countDown();
                    try { assertTrue(release.await(10, TimeUnit.SECONDS)); }
                    catch (InterruptedException failure) { throw new AssertionError(failure); }
                }
                return new it.cavallium.rockserver.core.impl.CompactionIoBudget.Sample(call * 1_000_000_000L,
                        call * 10, call * 1000, call * 1_000_000L, true, false, false, true, true, call * 10, true);
            };
            var constructor = Class.forName("it.cavallium.rockserver.core.impl.CompactionIoController")
                    .getDeclaredConstructor(String.class, org.rocksdb.RateLimiter.class,
                            java.util.function.Supplier.class, io.micrometer.core.instrument.MeterRegistry.class);
            constructor.setAccessible(true);
            try (var controller = (AutoCloseable) constructor.newInstance("shutdown-restore", limiter, sample, registry)) {
                assertTrue(entered.await(12, TimeUnit.SECONDS));
                assertEquals(750_000, limiter.getBytesPerSecond());
                var close = executor.submit(() -> { controller.close(); return null; });
                try {
                    assertTrue(restored.await(5, TimeUnit.SECONDS));
                    assertEquals(1_000_000, limiter.getBytesPerSecond());
                    assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
                } finally { release.countDown(); }
                close.get(5, TimeUnit.SECONDS);
                assertTrue(lateThrottle.await(1, TimeUnit.SECONDS));
                assertEquals(1_000_000, limiter.getBytesPerSecond());
                limiter.close();
                controller.close(); // Repeat close must not touch the now-disposed limiter.
            }
        } finally { release.countDown(); registry.close(); }
    }
    @Test void zeroConfiguredWriteBufferStillBootstrapsAboveAlignmentFloor(@TempDir Path root) throws Exception {
        var cfg = config(root, "database.global.write-buffer-manager=0\ndatabase.global.block-cache=8MiB\n");
        var loaded = RocksDBLoader.load(root.resolve("zero-wbm"), ConfigParser.parse(cfg), LoggerFactory.getLogger(getClass()));
        try {
            assertEquals((8L << 20) / 2 / 5, loaded.compactionIoLimiter().getBytesPerSecond());
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
