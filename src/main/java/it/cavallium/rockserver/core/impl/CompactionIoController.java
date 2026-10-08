package it.cavallium.rockserver.core.impl;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.rocksdb.RateLimiter;

/** Owns the only policy thread. Close joins it before any sampled native object may close. */
final class CompactionIoController implements AutoCloseable {
    private final java.util.concurrent.ScheduledExecutorService executor;
    private final List<Meter> meters;
    private final MeterRegistry registry;
    private final AtomicLong failures = new AtomicLong();

    CompactionIoController(String name, RateLimiter limiter, Supplier<CompactionIoBudget.Sample> sample,
                           MeterRegistry registry) {
        this.registry = registry;
        var budget = new CompactionIoBudget(limiter.getBytesPerSecond());
        meters = List.of(
                Gauge.builder("rockserver.compaction.io.budget", budget, CompactionIoBudget::budget)
                        .tag("db", name).baseUnit("bytes/second").register(registry),
                Gauge.builder("rockserver.compaction.io.state", budget, b -> b.state().ordinal())
                        .tag("db", name).register(registry),
                Gauge.builder("rockserver.compaction.io.adjustment.failures", failures, AtomicLong::doubleValue)
                        .tag("db", name).register(registry));
        executor = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform()
                .daemon().name("compaction-io[" + name + "]").factory());
        try {
            executor.scheduleWithFixedDelay(() -> {
                try {
                    long next = budget.sample(sample.get());
                    if (next != limiter.getBytesPerSecond()) limiter.setBytesPerSecond(next);
                } catch (VirtualMachineError fatal) {
                    throw fatal;
                } catch (Throwable failure) {
                    failures.incrementAndGet();
                    long restored = budget.sample(new CompactionIoBudget.Sample(System.nanoTime(), 0, 0, 0,
                            false, false, false, false));
                    try {
                        if (restored != limiter.getBytesPerSecond()) limiter.setBytesPerSecond(restored);
                    } catch (RuntimeException ignored) { failures.incrementAndGet(); }
                }
            }, 1, 1, TimeUnit.SECONDS);
        } catch (RuntimeException | Error failure) {
            close();
            throw failure;
        }
    }

    @Override public void close() {
        executor.shutdown();
        boolean interrupted = false;
        for (;;) {
            try {
                if (executor.awaitTermination(1, TimeUnit.DAYS)) break;
            } catch (InterruptedException ignored) { interrupted = true; }
        }
        meters.forEach(registry::remove);
        if (interrupted) Thread.currentThread().interrupt();
    }
}
