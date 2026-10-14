package it.cavallium.rockserver.core.impl.rocksdb;

import static it.cavallium.rockserver.core.impl.rocksdb.StartupProductionQualification.*;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import jdk.jfr.Enabled;
import jdk.jfr.Event;
import jdk.jfr.EventType;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/** Paired in-process service qualification; production loader, schedulers and feedback controller own all policy. */
public final class StartupEmbeddedQualification {
    @Name("rockserver.QualificationWrite") @Enabled(false) @StackTrace(false)
    static final class PublicWrite extends Event {
        public long sequence;
        public boolean failed = true;
        static final EventType TYPE = EventType.getEventType(PublicWrite.class);
    }
    @Name("rockserver.QualificationForeground") @Enabled(false) @StackTrace(false)
    static final class Foreground extends Event {
        public String mode;
        public boolean failed = true;
        static final EventType TYPE = EventType.getEventType(Foreground.class);
    }
    private static final String EMBEDDED_MARKER = ".startup-embedded-qualification";
    private static Keys keys(byte[] bytes) { return new Keys(Buf.wrap(bytes)); }
    private static void verifyValue(Buf actual, int id) {
        if (!Objects.equals(actual, Buf.wrap(value(id)))) throw new IllegalStateException("Data mismatch: " + id);
    }
    private static Object field(Object owner, String name) throws ReflectiveOperationException {
        var field = owner.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(owner);
    }
    private static long[] telemetry(RWScheduler scheduler, RWScheduler.Pool pool) {
        var result = new long[RWScheduler.POOL_TELEMETRY_LENGTH];
        scheduler.copyPoolTelemetry(pool, result);
        return result;
    }
    private static Map<String, Long> schedulerStats(RWScheduler scheduler) {
        var r = telemetry(scheduler, RWScheduler.Pool.READ);
        var w = telemetry(scheduler, RWScheduler.Pool.WRITE);
        return Map.of("active", r[RWScheduler.POOL_TELEMETRY_ACTIVE_TASKS] + w[RWScheduler.POOL_TELEMETRY_ACTIVE_TASKS],
                "queued", r[RWScheduler.POOL_TELEMETRY_QUEUED_TASKS] + w[RWScheduler.POOL_TELEMETRY_QUEUED_TASKS],
                "accepted", r[RWScheduler.POOL_TELEMETRY_ACCEPTED_TASKS] + w[RWScheduler.POOL_TELEMETRY_ACCEPTED_TASKS],
                "terminal", r[RWScheduler.POOL_TELEMETRY_TERMINAL_OUTCOMES] + w[RWScheduler.POOL_TELEMETRY_TERMINAL_OUTCOMES]);
    }
    private static String codeSource(Class<?> type) {
        var source = type.getProtectionDomain().getCodeSource();
        if (source == null) throw new IllegalStateException("Missing CodeSource for " + type.getName());
        return source.getLocation().toExternalForm();
    }
    private static Map<String, Object> nativeIdentity() throws ClassNotFoundException {
        String expected = System.getProperty("rockserver.startup.expected-native-sha256");
        if (expected == null || !expected.matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("Required expected native SHA256 JVM property is missing or malformed");
        String expectedHome = System.getProperty("rockserver.startup.expected-user-home");
        String actualHome = System.getProperty("user.home");
        if (expectedHome == null || expectedHome.isEmpty() || !expectedHome.equals(actualHome))
            throw new IllegalArgumentException("Effective user.home mismatch before native loading: expected="
                    + expectedHome + ", actual=" + actualHome);
        // Initialize and fingerprint the actual dladdr-selected SO equally in every mode, before DB timers.
        String actual = NativeStartupBridge.cacheCompatibilityId();
        if (!actual.equals(expected))
            throw new IllegalStateException("Loaded native SHA256 mismatch: expected=" + expected + ", actual=" + actual);
        return Map.of("loaded_native_sha256", actual, "expected_native_sha256", expected,
                "user_home", actualHome, "code_sources", Map.of(
                    "org.rocksdb.RocksDB", codeSource(org.rocksdb.RocksDB.class),
                    "org.rocksdb.NativeLibraryLoader", codeSource(Class.forName("org.rocksdb.NativeLibraryLoader", false,
                            org.rocksdb.RocksDB.class.getClassLoader())),
                    "it.cavallium.rockserver.core.impl.EmbeddedDB", codeSource(EmbeddedDB.class)));
    }
    private static Map<String, Object> serviceHistogram(Histogram histogram) {
        var result = new LinkedHashMap<String, Object>(histogram.report());
        result.put("service_failures", result.remove("native_failures"));
        return result;
    }
    private static Map<String, Object> cacheOccupancy(RocksDBLoader.StartupResources resources) {
        var c = resources.snapshotCaches();
        return Map.of("data", Map.of("usage_bytes", c.getFirst().getUsage(), "pinned_usage_bytes", c.getFirst().getPinnedUsage()),
                "metadata", Map.of("usage_bytes", c.getLast().getUsage(), "pinned_usage_bytes", c.getLast().getPinnedUsage()));
    }
    private static <T> T awaitRequest(Future<T> request, long deadline) throws Exception {
        try {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new TimeoutException("Measured workload deadline expired");
            return request.get(remaining, TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            request.cancel(false);
            Thread.currentThread().interrupt();
            throw interrupted;
        } catch (TimeoutException timeout) {
            request.cancel(false);
            throw timeout;
        }
    }
    private static Map<String, Long> settledScheduler(RWScheduler scheduler) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        for (;;) {
            var stats = schedulerStats(scheduler);
            if (stats.get("active") == 0 && stats.get("queued") == 0 && stats.get("accepted").equals(stats.get("terminal"))) return stats;
            if (System.nanoTime() >= deadline) throw new IllegalStateException("Service scheduler did not settle: " + stats);
            Thread.sleep(1);
        }
    }
    private static Map<String, String> cacheStats(RocksDBLoader.StartupResources resources) {
        var c = resources.snapshotCaches();
        return c.isEmpty() ? Map.of() : stats(c.getFirst(), c.getLast());
    }
    private static void awaitCapturePermission(RocksDBLoader.StartupResources resources) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        // Observe actual controller feedback; never force a healthy sample or override native pressure.
        while (resources.snapshotCaches().stream().anyMatch(c -> !c.stats().contains("\"feedback\":0"))) {
            if (System.nanoTime() >= deadline) throw new IllegalStateException("Controller did not permit idle capture: " + cacheStats(resources));
            Thread.sleep(25);
        }
    }
    private static int captureIdle(RocksDBLoader.StartupResources resources) throws Exception {
        awaitCapturePermission(resources);
        var c = resources.snapshotCaches();
        int status = captureBoth(c.getFirst(), c.getLast());
        awaitCapturePermission(resources);
        return status;
    }
    private static Path config(Path db, Path cache, String mode, boolean direct, int dataSample, int metadataSample, long period, long readCeilingBytes) throws Exception {
        boolean seed = mode.equals("seed"), snapshots = seed || mode.equals("cache") || mode.equals("both");
        boolean admission = mode.equals("admission") || mode.equals("both");
        String text = """
                database.metrics.jmx.enabled = false
                database.metrics.influx.enabled = false
                database.parallelism: {read: 4, write: 4}
                database.global: {
                  enable-fast-get: true
                  adaptive-compaction-io: false
                  use-direct-io: %s
                  allow-rocksdb-memory-mapping: false
                  disable-auto-compactions: true
                  maximum-open-files: -1
                  max-file-opening-threads: 4
                  open-files-async: %s
                  use-clock-cache: true
                  block-cache: 32MiB
                  block-cache-metadata-size: 4MiB
                  write-buffer-manager: 0B
                  startup-io: {enabled: %s, default-storage-id: embedded_qualification,
                    storage-groups: [{id: embedded_qualification, maximum-opens: 4,
                      progress-interval: PT0.1S, stale-after: PT2S, paths: ["%s"]}]}
                  hot-cache-snapshot: {enabled: %s, directory: "%s",
                    data: {disk-budget: 36MiB, snapshot-budget: 12MiB, collector-entries: 8192, index-ram: 4MiB,
                      maximum-block: 256KiB, lookup-buffer: 2MiB, minimum-free-disk: 64MiB,
                      snapshot-deadline: PT5S, period: "PT%sS", sample-every: %d, lookup-concurrency: 2,
                      read-ceiling: "%dB", write-ceiling: 8MiB},
                    metadata: {disk-budget: 12MiB, snapshot-budget: 4MiB, collector-entries: 2048, index-ram: 2MiB,
                      maximum-block: 256KiB, lookup-buffer: 2MiB, minimum-free-disk: 64MiB,
                      snapshot-deadline: PT5S, period: "PT%sS", sample-every: %d, lookup-concurrency: 2,
                      read-ceiling: "%dB", write-ceiling: 8MiB}}
                  fallback-column-options: {volumes: [], levels: [], first-level-sst-size: 16MiB, max-last-level-sst-size: 16MiB,
                    write-buffer-size: 8MiB, block-size: 16KiB, cache-index-and-filter-blocks: true,
                    pin-index-and-filter-blocks: false}
                }
                """.formatted(direct, !seed && !mode.equals("verify"), admission, db, snapshots, cache,
                seed ? "0" : period / 1000.0, dataSample, readCeilingBytes,
                seed ? "0" : period / 1000.0, metadataSample, readCeilingBytes);
        return Files.writeString(db.resolveSibling(db.getFileName() + ".embedded.conf"), text);
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 14) throw new IllegalArgumentException("db cache seed|none|cache|admission|both|verify datasetMiB operations direct dataSampling metadataSampling periodMs workloadTimeoutSeconds seedHotKeys seedPasses startupWindowMs readCeilingMiB");
        long readCeilingMiB = Long.parseLong(args[13]);
        if (readCeilingMiB < 0 || readCeilingMiB > Long.MAX_VALUE / MIB)
            throw new IllegalArgumentException("Read ceiling must be nonnegative and representable in signed bytes/second");
        long readCeilingBytes = Math.multiplyExact(readCeilingMiB, MIB);
        Path db = Path.of(args[0]).toAbsolutePath(), cache = Path.of(args[1]).toAbsolutePath();
        String mode = args[2];
        if (!Set.of("seed", "none", "cache", "admission", "both", "verify").contains(mode)) throw new IllegalArgumentException(mode);
        int mib = Integer.parseInt(args[3]), operations = Integer.parseInt(args[4]);
        boolean direct = Boolean.parseBoolean(args[5]), seed = mode.equals("seed"), checking = mode.equals("verify");
        int dataSample = Integer.parseInt(args[6]), metadataSample = Integer.parseInt(args[7]);
        long period = Long.parseLong(args[8]);
        int workloadSeconds = Integer.parseInt(args[9]);
        if (workloadSeconds < 10 || workloadSeconds > 3600) throw new IllegalArgumentException("Workload timeout bound");
        if (mib < 1 || mib > 8192 || operations < 1 || operations > 1_000_000 || period < 100 || period > 86400000)
            throw new IllegalArgumentException("Unbounded workload");
        int count = Math.toIntExact(mib * MIB / VALUE_BYTES);
        int seedHotKeys = Integer.parseInt(args[10]), seedPasses = Integer.parseInt(args[11]);
        long startupWindowMs = Long.parseLong(args[12]);
        int effectiveSeedHotKeys = Math.min(count, seedHotKeys);
        if (seedHotKeys < 1 || seedHotKeys > 262144 || seedPasses < 1 || seedPasses > 1024
                || (long) effectiveSeedHotKeys * seedPasses > 1_048_576
                || startupWindowMs < 100 || startupWindowMs > 60000)
            throw new IllegalArgumentException("Warm-history/window bound");
        if (seed && Files.exists(db)) throw new IllegalArgumentException("Seed requires a new task-owned DB path");
        if (!seed && !Files.readString(db.resolve(EMBEDDED_MARKER)).equals("v1:" + count)) throw new IllegalArgumentException("Fixture marker mismatch");
        System.setProperty("rockserver.core.print-config", "false");
        var output = new LinkedHashMap<String, Object>();
        output.put("mode", mode); output.put("fixture_keys", count); output.put("direct_reads", direct);
        output.put("seed_hot_keys_requested", seedHotKeys); output.put("seed_hot_keys_effective", effectiveSeedHotKeys);
        output.put("seed_passes", seedPasses); output.put("startup_window_ms", startupWindowMs);
        output.put("scope", "EmbeddedConnection public in-process service APIs; network/transport excluded");
        output.put("primary_cache_capacity_bytes", CAPACITY);
        output.put("cache_geometry", "Loader HCC DATA28MiB/METADATA4MiB; loader WBM auto16MiB charged within DATA; matched modes");
        output.put("data_sample_every", dataSample); output.put("metadata_sample_every", metadataSample);
        output.put("capture_period_ms", period); output.put("capture_period_override", period != 300000);
        output.put("read_ceiling_mib", readCeilingMiB); output.put("read_ceiling_bytes_per_second", readCeilingBytes);
        output.put("read_ceiling_scope", readCeilingBytes == 0 ? "unlimited, matching production default" : "explicit standalone benchmark profile");
        output.put("workload_timeout_seconds", workloadSeconds);
        output.put("bounded_inflight_requests_per_lane", 1);
        output.put("caller_lanes", "two asynchronous read lanes plus one asynchronous put/readback lane");
        output.put("native_identity", nativeIdentity());
        Path conf = config(db, cache, mode, direct, dataSample, metadataSample, period, readCeilingBytes);
        output.put("effective_config", Files.readString(conf));
        long beforeOpen = System.nanoTime();
        List<SnapshotCache> retainedCaches = List.of();
        long beforeClose;
        try (var connection = new EmbeddedConnection(db, "embedded-qualification", conf)) {
            long openedAt = System.nanoTime();
            output.put("embedded_connection_open_ns", openedAt - beforeOpen);
            var api = connection.getSyncApi(RequestContext.batch(Duration.ofSeconds(workloadSeconds)));
            long column = seed ? api.createColumn("entries", ColumnSchema.of(IntList.of(8), ObjectList.of(), true)) : api.getColumnId("entries");
            var internal = connection.getInternalDB();
            var resources = (RocksDBLoader.StartupResources) field(internal, "startupResources");
            retainedCaches = resources.snapshotCaches();
            var scheduler = internal.getScheduler();
            var budget = resources.budgets().values().stream().findFirst().orElse(null);
            if (seed) {
                for (int i = 0; i < count; i++) {
                    api.put(0, column, keys(key(i)), Buf.wrap(value(i)), RequestType.none());
                    if ((i + 1) % 2048 == 0) api.flush();
                }
                api.flush(); api.compact();
                var files = api.getSstMetadata(column, -1).files();
                output.put("fixture_live_sst_files", files.size());
                output.put("fixture_l0_files", files.stream().filter(f -> f.level() == 0).count());
                if (files.isEmpty() || files.stream().anyMatch(f -> f.level() == 0))
                    throw new IllegalStateException("Fixture must contain compacted non-L0 SSTs");
                // Native sampling rejects observations under pressure; do not override the real controller.
                awaitCapturePermission(resources);
                output.put("seed_cache_stats_before_history", cacheStats(resources));
                output.put("seed_cache_occupancy_before_history", cacheOccupancy(resources));
                int[] history = new int[effectiveSeedHotKeys];
                for (int i = 0; i < history.length; i++) history[i] = i;
                var order = new Random(271828);
                for (int pass = 0; pass < seedPasses; pass++) {
                    // The original default remains one sequential pass; declared repeated passes are shuffled.
                    if (seedPasses > 1) for (int i = history.length - 1; i > 0; i--) {
                        int selected = order.nextInt(i + 1), old = history[i];
                        history[i] = history[selected]; history[selected] = old;
                    }
                    for (int id : history) verifyValue(api.get(0, column, keys(key(id)), RequestType.current()), id);
                }
                output.put("seed_verified_foreground_gets", (long) history.length * seedPasses);
                output.put("seed_history_order", seedPasses == 1 ? "sequential" : "deterministic per-pass shuffle");
                output.put("seed_cache_stats_after_history", cacheStats(resources));
                output.put("seed_cache_occupancy_after_history", cacheOccupancy(resources));
                int status = captureIdle(resources);
                output.put("capture_status", status); output.put("cache_stats", cacheStats(resources));
                output.put("seed_cache_occupancy_after_capture", cacheOccupancy(resources));
                if (status != 0) throw new IllegalStateException("Seed durable capture failed: " + json(output));
                output.put("cache_compatibility_id", resources.snapshotCaches().getFirst().compatibilityId());
                Files.writeString(db.resolve(EMBEDDED_MARKER), "v1:" + count);
            } else if (checking) {
                for (int i = 0; i < WRITES; i++) verifyValue(api.get(0, column, keys(writeKey(i)), RequestType.current()), ~i);
                output.put("persisted_writes_verified", WRITES);
            } else {
                var async = connection.getAsyncApi(RequestContext.batch(Duration.ofSeconds(workloadSeconds)));
                var point = new Histogram(); var hot = new Histogram(); var cold = new Histogram();
                var bulk = new Histogram(); var write = new Histogram(); var overlap = new Histogram(); var steady = new Histogram();
                var startupPoint = new Histogram(); var startupHot = new Histogram();
                var startupBulk = new Histogram(); var startupWrites = new Histogram();
                var samples = Collections.synchronizedList(new ArrayList<Map<String, Object>>());
                var running = new AtomicBoolean(true); var opening = new AtomicBoolean(); var readers = new AtomicInteger(2);
                var completedWrites = new AtomicLong(); var duringReads = new AtomicLong();
                var start = new CountDownLatch(1);
                var schedulerBefore = settledScheduler(scheduler);
                var foregroundEvent = Foreground.TYPE.isEnabled() ? new Foreground() : null;
                if (foregroundEvent != null) { foregroundEvent.mode = mode; foregroundEvent.begin(); }
                long started = System.nanoTime();
                long workDeadline = started + TimeUnit.SECONDS.toNanos(workloadSeconds);
                output.put("scheduler_before", schedulerBefore);
                output.put("cache_stats_at_foreground_start", cacheStats(resources));
                if (budget != null) output.put("admission_at_foreground_start", admission(budget));
                long foregroundEnded;
                try (var pool = Executors.newFixedThreadPool(4)) {
                    var monitor = pool.submit(() -> {
                        try {
                            while (running.get()) {
                                var sample = new LinkedHashMap<String, Object>();
                                sample.put("elapsed_ns", System.nanoTime() - started);
                                sample.put("scheduler", schedulerStats(scheduler));
                                if (budget != null) {
                                    var counters = budget.snapshot(); opening.set(counters.active() + counters.queued() > 0);
                                    sample.put("admission", admission(budget));
                                }
                                sample.put("cache_stats", cacheStats(resources));
                                samples.add(sample);
                                Thread.sleep(100);
                            }
                        } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                    });
                    var tasks = new ArrayList<Future<?>>();
                    for (int lane = 0; lane < 2; lane++) {
                        final int salt = lane;
                        tasks.add(pool.submit(() -> {
                            try {
                                start.await(); var random = new Random(314159L + salt);
                                for (int op = 0; op < operations; op++) {
                                    int id = op % 2 == 0 ? random.nextInt(Math.min(count, 1024)) : random.nextInt(count);
                                    boolean activeOpen = opening.get();
                                    if (op % 4 == 0) {
                                        var batch = new ArrayList<Keys>();
                                        for (int i = 0; i < 8; i++) batch.add(keys(key((id + i * 997) % count)));
                                        long before = System.nanoTime(); var values = awaitRequest(async.existsMultiAsync(0, column, batch), workDeadline);
                                        long ns = System.nanoTime() - before; bulk.record(ns); (activeOpen ? overlap : steady).record(ns);
                                        if (before - openedAt < startupWindowMs * 1_000_000L) startupBulk.record(ns);
                                        if (!values.equals(Collections.nCopies(8, true))) throw new IllegalStateException("ExistsMulti mismatch");
                                    } else {
                                        Keys requestKey = keys(key(id));
                                        long before = System.nanoTime(); Buf value = awaitRequest(async.getAsync(0, column, requestKey, RequestType.current()), workDeadline);
                                        long ns = System.nanoTime() - before; point.record(ns); (op % 2 == 0 ? hot : cold).record(ns); (activeOpen ? overlap : steady).record(ns);
                                        if (before - openedAt < startupWindowMs * 1_000_000L) {
                                            startupPoint.record(ns);
                                            if (op % 2 == 0) startupHot.record(ns);
                                        }
                                        verifyValue(value, id);
                                    }
                                }
                            } catch (Exception failure) { throw new CompletionException(failure); }
                            finally { readers.decrementAndGet(); }
                        }));
                    }
                    tasks.add(pool.submit(() -> {
                        try {
                            start.await();
                            for (int sequence = 0; sequence < WRITES || readers.get() > 0; sequence++) {
                                int id = sequence % WRITES; long cycle = System.nanoTime(); boolean activeReaders = readers.get() > 0;
                                Keys requestKey = keys(writeKey(id)); Buf requestValue = Buf.wrap(value(~id));
                                var writeEvent = PublicWrite.TYPE.isEnabled() ? new PublicWrite() : null;
                                if (writeEvent != null) { writeEvent.sequence = sequence; writeEvent.begin(); }
                                long before = System.nanoTime();
                                long ns;
                                try {
                                    awaitRequest(async.putAsync(0, column, requestKey, requestValue, RequestType.none()), workDeadline);
                                    ns = System.nanoTime() - before;
                                    if (writeEvent != null) writeEvent.failed = false;
                                } finally {
                                    if (writeEvent != null) { writeEvent.end(); writeEvent.commit(); }
                                }
                                write.record(ns);
                                if (before - openedAt < startupWindowMs * 1_000_000L) startupWrites.record(ns);
                                verifyValue(awaitRequest(async.getAsync(0, column, requestKey, RequestType.current()), workDeadline), ~id);
                                completedWrites.incrementAndGet(); if (activeReaders) duringReads.incrementAndGet();
                                long pause = 1_000_000_000L / 128 - (System.nanoTime() - cycle);
                                if (pause > 0) TimeUnit.NANOSECONDS.sleep(pause);
                            }
                        } catch (Exception failure) { throw new CompletionException(failure); }
                    }));
                    start.countDown();
                    try {
                        for (var task : tasks) awaitRequest(task, workDeadline);
                        foregroundEnded = System.nanoTime();
                        if (foregroundEvent != null) foregroundEvent.failed = false;
                        output.put("cache_stats_before_final_capture", cacheStats(resources));
                        if (budget != null) output.put("admission_at_foreground_end", admission(budget));
                    }
                    finally {
                        try {
                            if (foregroundEvent != null) { foregroundEvent.end(); foregroundEvent.commit(); }
                        } finally {
                            running.set(false); pool.shutdownNow(); monitor.get(10, TimeUnit.SECONDS);
                        }
                    }
                }
                output.put("foreground_elapsed_ns", foregroundEnded - started);
                output.put("startup_post_open", Map.of(
                        "scope", "Measured-lane public API invocations starting in [0, window) after EmbeddedConnection open returns; includes admission/queue, native execution and future await; excludes setup, open/WAL, close, value verification and writer verification Gets",
                        "window_ms", startupWindowMs, "window_elapsed_ns", Math.min(foregroundEnded - openedAt, startupWindowMs * 1_000_000L),
                        "foreground_launch_offset_ns", started - openedAt,
                        "point", serviceHistogram(startupPoint), "hot_point", serviceHistogram(startupHot),
                        "exists_multi", serviceHistogram(startupBulk), "writes", serviceHistogram(startupWrites)));
                output.put("point", serviceHistogram(point)); output.put("hot_point", serviceHistogram(hot)); output.put("cold_point", serviceHistogram(cold));
                output.put("exists_multi", serviceHistogram(bulk)); output.put("writes", serviceHistogram(write));
                output.put("completed_writes", completedWrites.get()); output.put("writes_during_read_tasks", duringReads.get());
                output.put("opening_overlap", serviceHistogram(overlap)); output.put("steady_or_unclassified", serviceHistogram(steady));
                output.put("overlap_classification", budget == null ? "unclassified: admission disabled" : "approximate: latest100ms actual admission sample");
                var schedulerAfter = settledScheduler(scheduler);
                long accepted = schedulerAfter.get("accepted") - schedulerBefore.get("accepted");
                long terminal = schedulerAfter.get("terminal") - schedulerBefore.get("terminal");
                long requests = 2L * operations + 2 * completedWrites.get();
                if (accepted < requests || accepted != terminal)
                    throw new IllegalStateException("Public asynchronous requests lacked conserved admission: " + accepted + "/" + terminal + " expected at least " + requests);
                output.put("scheduler_after", schedulerAfter);
                output.put("scheduler_accepted_delta", accepted); output.put("scheduler_terminal_delta", terminal);
                output.put("verified_public_request_completions", requests); output.put("samples", samples);
                if (!resources.snapshotCaches().isEmpty()) {
                    output.put("capture_status", captureIdle(resources));
                    output.put("final_capture_scope", "post-workload, actual idle controller feedback; no feedback override, excluded from timed workload");
                    output.put("cache_stats", cacheStats(resources));
                    output.put("cache_usage_bytes", resources.snapshotCaches().stream().mapToLong(SnapshotCache::getUsage).sum());
                }
                if (budget != null) output.put("admission_before_close", admission(budget));
            }
            beforeClose = System.nanoTime();
        }
        output.put("embedded_connection_close_ns", System.nanoTime() - beforeClose);
        if (retainedCaches.stream().anyMatch(SnapshotCache::isOwningHandle))
            throw new IllegalStateException("Snapshot holders remained open after service shutdown");
        output.put("snapshot_holders_closed", true);
        output.put("data_mismatches", 0); output.put("status", "PASS");
        System.out.println(json(output));
    }
}
