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
            }
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
                                .sample(entry.getValue(), now, entry.getKey().equals(":shared")));
                    }
                    for (var entry : budgets.entrySet()) {
                        Set<String> groups = storageWorkloads.get(entry.getKey());
                        if (groups == null || groups.isEmpty()) groups = samples.keySet();
                        boolean hasUsers = groups.stream().anyMatch(group -> !group.equals(":shared"));
                        int mask = hasUsers ? 0 : StartupIoPolicy.UNKNOWN;
                        for (var group : groups) {
                            var decision = decisions.get(group);
                            // The optional cross-column cohort has no scheduler demand.
                            // Its valid idle window is neutral to the actual storage users.
                            if (group.equals(":shared") && decision != null
                                    && decision.pressureMask() == StartupIoPolicy.UNKNOWN && !decision.capturePressure()) continue;
                            mask |= decision == null ? StartupIoPolicy.UNKNOWN | StartupIoPolicy.FOREGROUND
                                    : decision.pressureMask();
                        }
                        entry.getValue().feedback(mask, epoch);
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
                } catch (VirtualMachineError fatal) {
                    throw fatal;
                } catch (Throwable failure) {
                    if (failures.getAndIncrement() == 0)
                        LOG.warn("Startup I/O feedback failed for database '{}'", name, failure);
                    for (var budget : budgets.values()) {
                        try { budget.feedback(StartupIoPolicy.UNKNOWN | StartupIoPolicy.FOREGROUND, epoch); }
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

    private void admissionGauge(String name, String storage, String suffix,
            AtomicReference<NativeStartupBridge.AdmissionStats> values,
            ToDoubleFunction<NativeStartupBridge.AdmissionStats> read) {
        meters.add(Gauge.builder("rockserver.startup.open." + suffix, values,
                v -> v.get() == null ? Double.NaN : read.applyAsDouble(v.get()))
                .tag("db", name).tag("storage", storage)
                .description("Shared physical-storage admission; FindTable calls include table-cache hits")
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
