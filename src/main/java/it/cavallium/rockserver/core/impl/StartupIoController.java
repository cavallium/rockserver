package it.cavallium.rockserver.core.impl;

import it.cavallium.rockserver.core.impl.rocksdb.NativeStartupBridge;
import it.cavallium.rockserver.core.impl.rocksdb.SnapshotCache;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;
import java.util.function.ToDoubleFunction;

/** Publishes one coherent foreground window and joins before native owners close. */
final class StartupIoController implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(StartupIoController.class);
    private static final long EPOCH_ORIGIN = System.nanoTime();
    private final ScheduledExecutorService executor;
    private final MeterRegistry registry;
    private final List<Meter> meters = new ArrayList<>();
    private final AtomicLong failures = new AtomicLong();
    private boolean closed;

    StartupIoController(String name, Map<String, NativeStartupBridge.Budget> budgets,
            Map<String, Set<String>> storageWorkloads, Map<String, List<SnapshotCache>> workloadCaches,
            Map<String, org.rocksdb.Cache> dataCaches, Map<String, org.rocksdb.Cache> metadataCaches,
            LongFunction<Map<String, StartupIoPolicy.InputSample>> sampler, MeterRegistry registry) {
        this.registry = registry;
        var policies = new HashMap<String, StartupIoPolicy>();
        executor = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform()
                .daemon().name("startup-io[" + name + "]").factory());
        try {
            var statistics = new HashMap<String, AtomicReference<NativeStartupBridge.AdmissionStats>>();
            for (String storage : budgets.keySet()) {
                var values = new AtomicReference<NativeStartupBridge.AdmissionStats>();
                statistics.put(storage, values);
                admissionGauge(name, storage, "active", values, NativeStartupBridge.AdmissionStats::active);
                admissionGauge(name, storage, "queued", values, NativeStartupBridge.AdmissionStats::queued);
                admissionGauge(name, storage, "limit", values, NativeStartupBridge.AdmissionStats::limit);
                admissionGauge(name, storage, "pressure.mask", values, NativeStartupBridge.AdmissionStats::pressureMask);
                admissionGauge(name, storage, "admitted", values, NativeStartupBridge.AdmissionStats::admitted);
                admissionGauge(name, storage, "released", values, NativeStartupBridge.AdmissionStats::released);
                admissionGauge(name, storage, "findtable.calls", values, NativeStartupBridge.AdmissionStats::completedFindTable);
                admissionGauge(name, storage, "findtable.failed", values, NativeStartupBridge.AdmissionStats::failed);
                admissionGauge(name, storage, "file.opens", values, NativeStartupBridge.AdmissionStats::ownedSuccessfulFileOpens);
            }
            var cacheStatistics = new HashMap<SnapshotCache, AtomicReference<NativeStartupBridge.CacheStats>>();
            registerCacheGauges(name, "data", dataCaches, cacheStatistics);
            registerCacheGauges(name, "metadata", metadataCaches, cacheStatistics);
            meters.add(Gauge.builder("rockserver.startup.io.feedback.failures", failures, AtomicLong::doubleValue)
                    .tag("db", name).register(registry));
            Runnable poll = () -> {
                long epoch = 1 + (System.nanoTime() - EPOCH_ORIGIN) / TimeUnit.SECONDS.toNanos(1);
                try {
                    var samples = sampler.apply(epoch);
                    var decisions = new HashMap<String, StartupIoPolicy.Decision>();
                    long now = System.nanoTime();
                    for (var entry : samples.entrySet()) {
                        decisions.put(entry.getKey(), policies.computeIfAbsent(entry.getKey(), _ -> new StartupIoPolicy())
                                .sampleRecorded(name, entry.getKey(), epoch, entry.getValue(), now));
                    }
                    for (var entry : budgets.entrySet()) {
                        int mask = storagePressureMask(storageWorkloads.getOrDefault(entry.getKey(), Set.of()), decisions);
                        entry.getValue().feedback(mask, epoch);
                        StartupIoJfr.storage(name, entry.getKey(), epoch, mask, false);
                        statistics.get(entry.getKey()).set(entry.getValue().snapshot());
                    }
                    // A shared cache is paused if any of its users reports pressure.
                    var masks = new HashMap<SnapshotCache, Integer>();
                    for (var entry : workloadCaches.entrySet()) {
                        var decision = decisions.get(entry.getKey());
                        int mask = decision == null || decision.capturePressure() ? StartupIoPolicy.FOREGROUND : 0;
                        for (var cache : entry.getValue()) masks.merge(cache, mask, (a, b) -> a | b);
                    }
                    masks.forEach(SnapshotCache::feedback);
                    cacheStatistics.forEach((cache, values) -> values.set(cache.statsSnapshot()));
                } catch (VirtualMachineError fatal) {
                    throw fatal;
                } catch (Throwable failure) {
                    if (failures.getAndIncrement() == 0)
                        LOG.warn("Startup I/O feedback failed for database '{}'", name, failure);
                    for (var entry : budgets.entrySet()) {
                        try {
                            entry.getValue().feedback(StartupIoPolicy.UNKNOWN | StartupIoPolicy.FOREGROUND, epoch);
                            StartupIoJfr.storage(name, entry.getKey(), epoch, StartupIoPolicy.UNKNOWN | StartupIoPolicy.FOREGROUND, true);
                        }
                        catch (RuntimeException | LinkageError ignored) { }
                    }
                    for (var caches : workloadCaches.values()) for (var cache : caches) {
                        try { cache.feedback(StartupIoPolicy.FOREGROUND); }
                        catch (RuntimeException | LinkageError ignored) { }
                    }
                }
            };
            executor.scheduleWithFixedDelay(poll, 0, 1, TimeUnit.SECONDS);
        }
        catch (RuntimeException | Error failure) { close(); throw failure; }
    }

    /** Idle owners are neutral peers; only measured healthy owners can qualify this DB's growth. */
    static int storagePressureMask(Set<String> owners, Map<String, StartupIoPolicy.Decision> decisions) {
        if (owners.isEmpty()) return StartupIoPolicy.UNKNOWN | StartupIoPolicy.FOREGROUND;
        int mask = 0;
        boolean qualifiedOwner = false, allNeutral = true;
        for (String owner : owners) {
            var decision = decisions.get(owner);
            if (decision != null && decision.idleNeutral()) continue;
            allNeutral = false;
            if (decision == null) {
                mask |= StartupIoPolicy.UNKNOWN | StartupIoPolicy.FOREGROUND;
            } else {
                mask |= decision.pressureMask();
                qualifiedOwner |= !owner.equals(":shared") && decision.qualifiedHealthy();
            }
        }
        if (mask != 0) return mask;
        if (qualifiedOwner) return 0;
        return allNeutral ? StartupIoPolicy.IDLE_NEUTRAL : StartupIoPolicy.UNKNOWN;
    }

    private void registerCacheGauges(String name, String role, Map<String, org.rocksdb.Cache> caches,
            Map<SnapshotCache, AtomicReference<NativeStartupBridge.CacheStats>> statistics) {
        caches.forEach((cacheName, cache) -> {
            if (!(cache instanceof SnapshotCache snapshot)) return;
            var values = statistics.computeIfAbsent(snapshot, _ -> new AtomicReference<>());
            cacheGauge(name, cacheName, role, "sample.candidates", values, NativeStartupBridge.CacheStats::sampleCandidates);
            cacheGauge(name, cacheName, role, "sampled", values, NativeStartupBridge.CacheStats::sampled);
            cacheGauge(name, cacheName, role, "dropped", values, NativeStartupBridge.CacheStats::dropped);
            cacheGauge(name, cacheName, role, "snapshots", values, NativeStartupBridge.CacheStats::snapshots);
            cacheGauge(name, cacheName, role, "snapshot.bytes", values, NativeStartupBridge.CacheStats::snapshotBytes);
            cacheGauge(name, cacheName, role, "aborts", values, NativeStartupBridge.CacheStats::aborts);
            cacheGauge(name, cacheName, role, "errors", values, NativeStartupBridge.CacheStats::errors);
            cacheGauge(name, cacheName, role, "rejected", values, NativeStartupBridge.CacheStats::rejected);
            cacheGauge(name, cacheName, role, "hits.data", values, NativeStartupBridge.CacheStats::dataHits);
            cacheGauge(name, cacheName, role, "hits.index", values, NativeStartupBridge.CacheStats::indexHits);
            cacheGauge(name, cacheName, role, "hits.filter", values, NativeStartupBridge.CacheStats::filterHits);
            cacheGauge(name, cacheName, role, "hits.dictionary", values, NativeStartupBridge.CacheStats::dictionaryHits);
            cacheGauge(name, cacheName, role, "replay.bytes", values, NativeStartupBridge.CacheStats::replayBytes);
            cacheGauge(name, cacheName, role, "lookup.reserved.bytes", values, NativeStartupBridge.CacheStats::lookupReservedBytes);
            cacheGauge(name, cacheName, role, "decoder.oversized", values, NativeStartupBridge.CacheStats::oversizedDecoderRejections);
            cacheGauge(name, cacheName, role, "pressure.mask", values, NativeStartupBridge.CacheStats::feedback);
        });
    }

    private void cacheGauge(String name, String cache, String role, String suffix,
            AtomicReference<NativeStartupBridge.CacheStats> values,
            ToDoubleFunction<NativeStartupBridge.CacheStats> read) {
        meters.add(Gauge.builder("rockserver.startup.cache." + suffix, values,
                v -> v.get() == null ? Double.NaN : read.applyAsDouble(v.get()))
                .tag("db", name).tag("cache", cache).tag("role", role)
                .description("Snapshot-cache poll; hits are secondary replay, sample candidates are sampled observations")
                .register(registry));
    }

    private void admissionGauge(String name, String storage, String suffix,
            AtomicReference<NativeStartupBridge.AdmissionStats> values,
            ToDoubleFunction<NativeStartupBridge.AdmissionStats> read) {
        meters.add(Gauge.builder("rockserver.startup.open." + suffix, values,
                v -> v.get() == null ? Double.NaN : read.applyAsDouble(v.get()))
                .tag("db", name).tag("storage", storage)
                .description(suffix.equals("file.opens")
                        ? "Successful filesystem opens owned by admitted callers, including later reader failure; not device reads"
                        : "Shared physical-storage admission; FindTable calls include table-cache hits")
                .register(registry));
    }

    @Override public synchronized void close() {
        if (closed) return;
        executor.shutdown();
        boolean interrupted = false;
        for (;;) {
            try { if (executor.awaitTermination(1, TimeUnit.SECONDS)) break; }
            catch (InterruptedException ignored) { interrupted = true; }
        }
        closed = true;
        meters.forEach(registry::remove);
        if (interrupted) Thread.currentThread().interrupt();
    }
}
