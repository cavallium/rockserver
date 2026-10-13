package it.cavallium.rockserver.core.impl.rocksdb;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.rocksdb.*;
import it.cavallium.rockserver.core.impl.StartupIoPolicy;
import it.cavallium.rockserver.core.impl.StartupReadCompletions;

/** Fresh-JVM production bridge fixture; intentionally excludes service/network acceptance. */
public final class StartupProductionQualification {
    static final long MIB = 1024 * 1024, CAPACITY = 32 * MIB;
    static final int VALUE_BYTES = 4096, WRITES = 128;
    static final String MARKER = ".startup-qualification";
    static byte[] key(int i) { return ByteBuffer.allocate(8).putInt(0).putInt(i).array(); }
    static byte[] writeKey(int i) { return ByteBuffer.allocate(8).putInt(1).putInt(i).array(); }
    static byte[] value(int i) {
        byte[] bytes = new byte[VALUE_BYTES];
        new Random(i).nextBytes(bytes);
        return bytes;
    }
    static void verify(byte[] actual, int i) {
        if (!Arrays.equals(actual, value(i))) throw new IllegalStateException("Data mismatch: " + i);
    }
    static final class Histogram {
        final long[] buckets = new long[512];
        long count;
        long failures;
        synchronized void record(long nanos) {
            long duration = Math.max(1, nanos);
            int shift = Math.max(0, 60 - Long.numberOfLeadingZeros(duration));
            int bucket = duration < 8 ? (int) duration : shift * 8 + (int) (duration >>> shift);
            buckets[bucket]++;
            count++;
        }
        synchronized void record(long nanos, boolean failed) {
            record(nanos);
            if (failed) failures++;
        }
        synchronized long percentile(int percentile) {
            if (count == 0) return 0;
            long rank = (count * percentile + 99) / 100, seen = 0;
            for (int i = 0; i < buckets.length; i++) {
                seen += buckets[i];
                if (seen >= rank) {
                    if (i < 8) return i;
                    int shift = i / 8 - 1;
                    return ((9L + i % 8) << shift) - 1;
                }
            }
            throw new AssertionError();
        }
        synchronized Map<String, Object> report() {
            return Map.of("operations", count, "native_failures", failures, "p50_upper_ns", percentile(50),
                    "p95_upper_ns", percentile(95), "p99_upper_ns", percentile(99));
        }
    }
    static String json(Object value) {
        if (value instanceof Map<?, ?> map) {
            var entries = new ArrayList<String>();
            map.forEach((k, v) -> entries.add(json(k.toString()) + ":" + json(v)));
            return "{" + String.join(",", entries) + "}";
        }
        if (value instanceof Iterable<?> list) {
            var entries = new ArrayList<String>();
            list.forEach(v -> entries.add(json(v)));
            return "[" + String.join(",", entries) + "]";
        }
        if (value instanceof String text) return "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\"";
        return String.valueOf(value);
    }
    static Map<String, Object> admission(NativeStartupBridge.Budget budget) {
        var s = budget.snapshot();
        return Map.ofEntries(Map.entry("active", s.active()), Map.entry("queued", s.queued()),
                Map.entry("admitted", s.admitted()), Map.entry("completed_find_table", s.completedFindTable()),
                Map.entry("owned_successful_file_opens", s.ownedSuccessfulFileOpens()),
                Map.entry("failed", s.failed()), Map.entry("peak_active", s.peakActive()),
                Map.entry("logical_bytes", s.logicalBytes()), Map.entry("limit", s.limit()),
                Map.entry("pressured_admissions", s.pressuredAdmissions()));
    }
    static NativeStartupBridge.SnapshotResult capture(SnapshotCache cache) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (true) {
            var result = cache.snapshot();
            if (result.status() != 1 || !result.message().equals("snapshot already running") || System.nanoTime() >= deadline)
                return result;
            Thread.sleep(25);
        }
    }
    static final class Foreground {
        long accepted, terminal;
        int active, queued;
        synchronized void accept() { accepted++; queued++; }
        synchronized void start() { queued--; active++; }
        synchronized void finish() { active--; terminal++; }
        synchronized StartupIoPolicy.InputSample sample(long epoch, long now, StartupReadCompletions.Snapshot reads) {
            return new StartupIoPolicy.InputSample(epoch, now, true, true, 3, active, queued, accepted, terminal, false, reads);
        }
    }
    static SnapshotCache snapshotCache(Path path, boolean seed, boolean metadata, int sampling, long periodMs) {
        return new SnapshotCache(new NativeStartupBridge.CacheConfig(1, -1, (metadata ? 4 : 28) * MIB,
                0, (metadata ? 12 : 36) * MIB, (metadata ? 4 : 12) * MIB,
                metadata ? 2048 : 8192, (metadata ? 2 : 4) * MIB, 256 * 1024,
                64 * MIB, 2 * MIB, 5000, seed ? 0 : periodMs, 8 * MIB, 8 * MIB,
                sampling, 2, .5, path, metadata ? "metadata" : "data"));
    }
    static Map<String, String> stats(SnapshotCache data, SnapshotCache metadata) {
        return Map.of("data", data.stats(), "metadata", metadata.stats());
    }
    static int captureBoth(SnapshotCache data, SnapshotCache metadata) throws InterruptedException {
        int dataStatus = capture(data).status(), metadataStatus = capture(metadata).status();
        return Math.max(dataStatus, metadataStatus);
    }
    static void flushToLevelOne(RocksDB db, FlushOptions flush) throws RocksDBException {
        db.flush(flush);
        var inputs = db.getLiveFilesMetaData().stream().filter(f -> f.level() == 0)
                .map(LiveFileMetaData::fileName).toList();
        if (!inputs.isEmpty()) try (var compact = new CompactionOptions().setOutputFileSizeLimit(16 * MIB)
                .setCompression(CompressionType.NO_COMPRESSION); var job = new CompactionJobInfo()) {
            db.compactFiles(compact, inputs, 1, 0, job);
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 12) throw new IllegalArgumentException("db cache seed|none|cache|admission|both|verify datasetMiB readOperations directReads dataSampleEvery metadataSampleEvery capturePeriodMs seedHotKeys seedPasses startupWindowMs");
        Path dbPath = Path.of(args[0]), cachePath = Path.of(args[1]);
        String mode = args[2];
        if (!Set.of("seed", "none", "cache", "admission", "both", "verify").contains(mode)) throw new IllegalArgumentException(mode);
        boolean seed = mode.equals("seed"), checking = mode.equals("verify");
        boolean cacheEnabled = seed || mode.equals("cache") || mode.equals("both");
        boolean admissionEnabled = mode.equals("admission") || mode.equals("both");
        int mib = Integer.parseInt(args[3]), operations = Integer.parseInt(args[4]);
        boolean direct = Boolean.parseBoolean(args[5]);
        int dataSampling = Integer.parseInt(args[6]), metadataSampling = Integer.parseInt(args[7]);
        long capturePeriodMs = Long.parseLong(args[8]);
        int seedHotKeys = Integer.parseInt(args[9]), seedPasses = Integer.parseInt(args[10]);
        long startupWindowMs = Long.parseLong(args[11]);
        if (capturePeriodMs < 100 || capturePeriodMs > 86400000) throw new IllegalArgumentException("Capture period bound");
        if (mib < 1 || mib > 1024 || operations < 1 || operations > 1_000_000) throw new IllegalArgumentException("Unbounded fixture");
        int keys = Math.toIntExact(mib * MIB / VALUE_BYTES);
        int effectiveSeedHotKeys = Math.min(keys, seedHotKeys);
        if (seedHotKeys < 1 || seedHotKeys > 262144 || seedPasses < 1 || seedPasses > 1024
                || (long) effectiveSeedHotKeys * seedPasses > 1_048_576
                || startupWindowMs < 100 || startupWindowMs > 60000)
            throw new IllegalArgumentException("Warm-history/window bound");
        if (seed && Files.exists(dbPath)) throw new IllegalArgumentException("Seed needs new database path");
        if (!seed && !Files.readString(dbPath.resolve(MARKER)).equals("v1:" + keys)) throw new IllegalArgumentException("Fixture marker mismatch");
        RocksDBLoader.loadLibrary();
        var output = new LinkedHashMap<String, Object>();
        output.put("seed_hot_keys_requested", seedHotKeys); output.put("seed_hot_keys_effective", effectiveSeedHotKeys);
        output.put("seed_passes", seedPasses); output.put("startup_window_ms", startupWindowMs);
        output.put("mode", mode); output.put("direct_reads", direct); output.put("fixture_keys", keys);
        output.put("primary_cache_capacity_bytes", CAPACITY);
        output.put("cache_geometry", "AutoHyperClockCache data28MiB/metadata4MiB, estimatedEntryCharge0/shards-1; matched baseline/candidate");
        output.put("data_sample_every", dataSampling); output.put("metadata_sample_every", metadataSampling);
        output.put("capture_period_ms", capturePeriodMs);
        output.put("capture_period_override", capturePeriodMs != 300000);
        output.put("feedback_policy_window_ns", 1_000_000_000L);
        output.put("snapshot_budget_bytes", 16 * MIB); output.put("disk_budget_bytes", 48 * MIB);
        var hotPoint = new Histogram(); var coldPoint = new Histogram();
        var point = new Histogram(); var multi = new Histogram(); var writes = new Histogram();
        var startupPoint = new Histogram(); var startupHot = new Histogram();
        var startupBulk = new Histogram(); var startupWrites = new Histogram();
        var overlap = new Histogram(); var steady = new Histogram();
        var samples = Collections.synchronizedList(new ArrayList<Map<String, Object>>());
        var running = new AtomicBoolean(true);
        SnapshotCache snapshot = cacheEnabled ? snapshotCache(cachePath, seed, false, dataSampling, capturePeriodMs) : null;
        SnapshotCache metadataSnapshot = cacheEnabled ? snapshotCache(cachePath, seed, true, metadataSampling, capturePeriodMs) : null;
        try (Cache cache = snapshot != null ? snapshot : new HyperClockCache(28 * MIB, 0, -1, false);
             Cache metadataCache = metadataSnapshot != null ? metadataSnapshot : new HyperClockCache(4 * MIB, 0, -1, false);
             Options options = new Options().setCreateIfMissing(seed).setErrorIfExists(seed)
                     .setCompressionType(CompressionType.NO_COMPRESSION).setDisableAutoCompactions(true)
                     .setWriteBufferSize((seed ? 16 : 8) * MIB).setMaxOpenFiles(-1).setMaxFileOpeningThreads(4)
                     .setOpenFilesAsync(!seed && !checking).setSkipStatsUpdateOnDbOpen(!seed)
                     .setUseDirectReads(direct).setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache).setMetadataBlockCache(metadataCache)
                             .setCacheIndexAndFilterBlocks(true));
             WriteOptions writeOptions = new WriteOptions();
             FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
            NativeStartupBridge.Budget budget = admissionEnabled ? NativeStartupBridge.createBudget(
                    new NativeStartupBridge.BudgetConfig("qualification", 4, 100, 2000)) : null;
            NativeStartupBridge.AdmissionConfig config = budget == null ? null : NativeStartupBridge.createAdmissionConfig(budget);
            try {
                if (config != null) config.attach(options);
                long beforeOpen = System.nanoTime();
                try (RocksDB db = RocksDB.open(options, dbPath.toString())) {
                    try {
                        output.put("open_wal_recovery_ns", System.nanoTime() - beforeOpen);
                        if (seed) {
                            for (int i = 0; i < keys; i++) {
                                db.put(writeOptions, key(i), value(i));
                                if ((i + 1) % 2048 == 0) flushToLevelOne(db, flush);
                            }
                            flushToLevelOne(db, flush);
                            output.put("fixture_live_sst_files", db.getLiveFilesMetaData().size());
                            output.put("fixture_l0_files", db.getLiveFilesMetaData().stream().filter(f -> f.level() == 0).count());
                            int[] history = new int[effectiveSeedHotKeys];
                            for (int i = 0; i < history.length; i++) history[i] = i;
                            var order = new Random(271828);
                            for (int pass = 0; pass < seedPasses; pass++) {
                                // Default one-pass history preserves the original sequential seed.
                                if (seedPasses > 1) for (int i = history.length - 1; i > 0; i--) {
                                    int selected = order.nextInt(i + 1), old = history[i];
                                    history[i] = history[selected]; history[selected] = old;
                                }
                                for (int id : history) verify(db.get(key(id)), id);
                            }
                            output.put("seed_verified_foreground_gets", (long) history.length * seedPasses);
                            output.put("seed_history_order", seedPasses == 1 ? "sequential" : "deterministic per-pass shuffle");
                            int status = captureBoth(snapshot, metadataSnapshot);
                            output.put("capture_status", status);
                            if (status != 0) throw new IllegalStateException("Fixture capture status: " + status);
                            output.put("cache_stats", stats(snapshot, metadataSnapshot));
                            output.put("cache_compatibility_id", snapshot.compatibilityId());
                            Files.writeString(dbPath.resolve(MARKER), "v1:" + keys);
                        } else if (checking) {
                            for (int i = 0; i < WRITES; i++) verify(db.get(writeKey(i)), ~i);
                            output.put("persisted_writes_verified", WRITES);
                        } else {
                            ExecutorService pool = Executors.newFixedThreadPool(4);
                            long started = System.nanoTime();
                            var foreground = new Foreground();
                            var completions = new StartupReadCompletions();
                            var policy = new StartupIoPolicy();
                            var openingSampled = new AtomicBoolean();
                            var readersRemaining = new AtomicInteger(2);
                            var completedWrites = new AtomicLong();
                            var writesDuringReads = new AtomicLong();
                            Future<?> monitor = pool.submit(() -> {
                                try {
                                    long lastEpoch = -1;
                                    while (running.get()) {
                                        long now = System.nanoTime();
                                        var sample = new LinkedHashMap<String, Object>();
                                        sample.put("elapsed_ns", now - started);
                                        sample.put("completed_writes", completedWrites.get());
                                        sample.put("writes_during_read_tasks", writesDuringReads.get());
                                        long epoch = 1 + (now - started) / 1_000_000_000L;
                                        if (epoch != lastEpoch) {
                                            var reads = completions.snapshot(epoch);
                                            var input = foreground.sample(epoch, now, reads);
                                            var decision = policy.sample(input, System.nanoTime());
                                            sample.put("policy_pressure_mask", decision.pressureMask());
                                            sample.put("policy_capture_pressure", decision.capturePressure());
                                            sample.put("policy_qualified_healthy", decision.qualifiedHealthy());
                                            sample.put("foreground_active", input.active());
                                            sample.put("foreground_queued", input.queued());
                                            sample.put("foreground_accepted", input.accepted());
                                            sample.put("foreground_terminal", input.terminal());
                                            sample.put("point_completed_in_epoch", reads.point().count());
                                            sample.put("bulk_completed_in_epoch", reads.bulk().count());
                                            sample.put("point_failures_in_epoch", reads.point().failures());
                                            sample.put("bulk_failures_in_epoch", reads.bulk().failures());
                                            if (budget != null) budget.feedback(decision.pressureMask(), epoch);
                                            if (snapshot != null) {
                                                int mask = decision.capturePressure() ? StartupIoPolicy.FOREGROUND : 0;
                                                snapshot.feedback(mask); metadataSnapshot.feedback(mask);
                                            }
                                            lastEpoch = epoch;
                                        }
                                        if (budget != null) {
                                            var s = budget.snapshot();
                                            openingSampled.set(s.active() + s.queued() > 0);
                                            sample.put("admission", admission(budget));
                                            sample.put("opening_active_or_queued", s.active() + s.queued());
                                        }
                                        if (snapshot != null) {
                                            sample.put("cache_stats", stats(snapshot, metadataSnapshot));
                                            sample.put("cache_usage_bytes", snapshot.getUsage() + metadataSnapshot.getUsage());
                                        }
                                        samples.add(sample);
                                        Thread.sleep(100);
                                    }
                                } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                            });
                            var tasks = new ArrayList<Future<?>>();
                            for (int lane = 0; lane < 2; lane++) {
                                final int salt = lane;
                                foreground.accept();
                                tasks.add(pool.submit(() -> {
                                    foreground.start();
                                    try (ReadOptions read = new ReadOptions()) {
                                        Random random = new Random(314159L + salt);
                                        for (int op = 0; op < operations; op++) {
                                            int id = op % 2 == 0 ? random.nextInt(Math.min(keys, 1024)) : random.nextInt(keys);
                                            boolean opening = openingSampled.get();
                                            if (op % 4 == 0) {
                                                var batch = new ArrayList<byte[]>(); var ids = new int[8];
                                                for (int i = 0; i < ids.length; i++) { ids[i] = (id + i * 997) % keys; batch.add(key(ids[i])); }
                                                List<byte[]> values;
                                                long before = System.nanoTime(), ns; boolean failed = true;
                                                try { values = db.multiGetAsList(read, batch); failed = false; }
                                                finally {
                                                    ns = System.nanoTime() - before;
                                                    completions.recordBulk(ns, batch.size(), failed);
                                                    if (before - started < startupWindowMs * 1_000_000L) startupBulk.record(ns, failed);
                                                }
                                                multi.record(ns); (opening ? overlap : steady).record(ns);
                                                for (int i = 0; i < ids.length; i++) verify(values.get(i), ids[i]);
                                            } else {
                                                byte[] nativeKey = key(id), actual;
                                                long before = System.nanoTime(), ns; boolean failed = true;
                                                try { actual = db.get(read, nativeKey); failed = false; }
                                                finally {
                                                    ns = System.nanoTime() - before;
                                                    completions.recordPoint(ns, failed);
                                                    if (before - started < startupWindowMs * 1_000_000L) {
                                                        startupPoint.record(ns, failed);
                                                        if (op % 2 == 0) startupHot.record(ns, failed);
                                                    }
                                                }
                                                point.record(ns); (op % 2 == 0 ? hotPoint : coldPoint).record(ns); (opening ? overlap : steady).record(ns);
                                                verify(actual, id);
                                            }
                                        }
                                    } catch (Exception failure) { throw new CompletionException(failure); }
                                    finally { readersRemaining.decrementAndGet(); foreground.finish(); }
                                }));
                            }
                            foreground.accept();
                            tasks.add(pool.submit(() -> {
                                foreground.start();
                                try {
                                    for (int sequence = 0; sequence < WRITES || readersRemaining.get() > 0; sequence++) {
                                        int i = sequence % WRITES;
                                        long cycleStarted = System.nanoTime();
                                        boolean readersActive = readersRemaining.get() > 0;
                                        byte[] nativeKey = writeKey(i), nativeValue = value(~i);
                                        long before = System.nanoTime(), nativeNanos;
                                        boolean writeFailed = true;
                                        try { db.put(writeOptions, nativeKey, nativeValue); writeFailed = false; }
                                        finally {
                                            nativeNanos = System.nanoTime() - before;
                                            if (before - started < startupWindowMs * 1_000_000L) startupWrites.record(nativeNanos, writeFailed);
                                        }
                                        writes.record(nativeNanos);
                                        byte[] actual; boolean failed = true;
                                        before = System.nanoTime();
                                        try { actual = db.get(nativeKey); failed = false; }
                                        finally { completions.recordPoint(System.nanoTime() - before, failed); }
                                        verify(actual, ~i);
                                        completedWrites.incrementAndGet();
                                        if (readersActive) writesDuringReads.incrementAndGet();
                                        long pause = 1_000_000_000L / 128 - (System.nanoTime() - cycleStarted);
                                        if (pause > 0) TimeUnit.NANOSECONDS.sleep(pause);
                                    }
                                } catch (Exception failure) { throw new CompletionException(failure); }
                                finally { foreground.finish(); }
                            }));
                            long foregroundEnded;
                            try { for (Future<?> task : tasks) task.get(); foregroundEnded = System.nanoTime(); }
                            finally {
                                running.set(false);
                                pool.shutdown();
                                if (!pool.awaitTermination(300, TimeUnit.SECONDS)) throw new IllegalStateException("Foreground workers did not terminate");
                                monitor.get();
                            }
                            output.put("foreground_elapsed_ns", foregroundEnded - started);
                            output.put("startup_post_open", Map.of(
                                    "scope", "Foreground calls whose native start is in [0, window) after foreground launch; excludes open/WAL, close, and value verification; point excludes writer verification reads",
                                    "window_ms", startupWindowMs, "window_elapsed_ns", Math.min(foregroundEnded - started, startupWindowMs * 1_000_000L),
                                    "point", startupPoint.report(), "hot_point", startupHot.report(),
                                    "multi_get", startupBulk.report(), "writes", startupWrites.report()));
                            output.put("completed_writes", completedWrites.get());
                            output.put("writes_during_read_tasks", writesDuringReads.get());
                            output.put("writer_target_max_ops_per_second", 128);
                            output.put("point", point.report()); output.put("hot_point", hotPoint.report()); output.put("cold_point", coldPoint.report()); output.put("multi_get", multi.report()); output.put("writes", writes.report());
                            output.put("opening_overlap", overlap.report()); output.put("steady_or_unclassified", steady.report());
                            output.put("overlap_classification", budget == null ? "unclassified: admission disabled" : "approximate: latest100ms admission sample");
                            var finalForeground = foreground.sample(1 + (System.nanoTime() - started) / 1_000_000_000L,
                                    System.nanoTime(), null);
                            output.put("foreground_accepted", finalForeground.accepted()); output.put("foreground_terminal", finalForeground.terminal());
                            if (snapshot != null) {
                                output.put("cache_stats_before_final_capture", stats(snapshot, metadataSnapshot));
                                output.put("manual_final_capture_override", "post-workload explicit capture; excluded from performance");
                                snapshot.feedback(0); metadataSnapshot.feedback(0);
                                output.put("capture_status", captureBoth(snapshot, metadataSnapshot));
                                output.put("cache_stats", stats(snapshot, metadataSnapshot)); output.put("cache_usage_bytes", snapshot.getUsage() + metadataSnapshot.getUsage());
                            }
                            if (budget != null) output.put("admission_before_close", admission(budget));
                            output.put("samples", samples);
                        }
                        // Required production ownership order: snapshot workers join before DB destruction.
                    } finally { if (snapshot != null) { snapshot.stop(); metadataSnapshot.stop(); } }
                }
                if (budget != null) {
                    output.put("admission_after_close", admission(budget));
                    var s = budget.snapshot();
                    if (s.active() != 0 || s.queued() != 0 || s.failed() != 0 || s.peakActive() > 4)
                        throw new IllegalStateException("Admission resource/failure bound violated: " + s);
                }
            } finally { if (config != null) config.close(); if (budget != null) budget.close(); }
        }
        output.put("data_mismatches", 0); output.put("status", "PASS");
        System.out.println(json(output));
    }
}
