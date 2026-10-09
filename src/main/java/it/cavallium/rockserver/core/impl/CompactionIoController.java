package it.cavallium.rockserver.core.impl;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.LongUnaryOperator;
import org.rocksdb.RateLimiter;

/** Owns the only policy thread. Close joins it before any sampled native object may close. */
final class CompactionIoController implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CompactionIoController.class);
    private final java.util.concurrent.ScheduledExecutorService executor;
    private final List<Meter> meters;
    private final MeterRegistry registry;
    private final AtomicLong failures = new AtomicLong();
    private final RateLimiter limiter;
    private final CompactionIoBudget budget;
    private final Runnable poll;
    private final AtomicLong appliedReadahead = new AtomicLong();
    private boolean closed, readaheadDirty;

    CompactionIoController(String name, RateLimiter limiter, Supplier<CompactionIoBudget.Sample> sample,
                           MeterRegistry registry) {
        this(name, limiter, sample, registry, 0, bytes -> bytes);
    }

    CompactionIoController(String name, RateLimiter limiter, Supplier<CompactionIoBudget.Sample> sample,
                           MeterRegistry registry, long readaheadCeiling, LongUnaryOperator applyReadahead) {
        this(name, limiter, sample, registry, readaheadCeiling, applyReadahead, true);
    }

    // Manual scheduling keeps actuator failure/settling tests deterministic on this same owned executor.
    CompactionIoController(String name, RateLimiter limiter, Supplier<CompactionIoBudget.Sample> sample,
                           MeterRegistry registry, long readaheadCeiling, LongUnaryOperator applyReadahead,
                           boolean automaticPolling) {
        this.registry = registry;
        this.limiter = limiter;
        budget = new CompactionIoBudget(limiter.getBytesPerSecond(), readaheadCeiling);
        meters = List.of(
                Gauge.builder("rockserver.compaction.io.budget", budget, CompactionIoBudget::budget)
                        .tag("db", name).baseUnit("bytes/second").register(registry),
                Gauge.builder("rockserver.compaction.io.baseline.read.micros", budget, CompactionIoBudget::baselineReadMicros)
                        .tag("db", name).baseUnit("microseconds").register(registry),
                Gauge.builder("rockserver.compaction.io.state", budget, b -> b.state().ordinal())
                        .tag("db", name).register(registry),
                Gauge.builder("rockserver.compaction.io.readahead", appliedReadahead, AtomicLong::doubleValue)
                        .tag("db", name).baseUnit("bytes")
                        .description("Last successfully acknowledged DB readahead option for new compaction jobs/readers; existing buffers do not resize")
                        .register(registry),
                Gauge.builder("rockserver.compaction.io.adjustment.failures", failures, AtomicLong::doubleValue)
                        .tag("db", name).register(registry));
        executor = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform()
                .daemon().name("compaction-io[" + name + "]").factory());
        poll = () -> {
                try {
                    long next = budget.sample(sample.get());
                    if (next != limiter.getBytesPerSecond()) limiter.setBytesPerSecond(next);
                    long target = budget.readaheadBytes();
                    if (readaheadDirty || target != appliedReadahead.get()) {
                        readaheadDirty = true;
                        long actual = applyReadahead.applyAsLong(target);
                        if (actual != target) throw new IllegalStateException("DB readahead acknowledgement did not match target");
                        appliedReadahead.set(actual);
                        budget.readaheadApplied(actual);
                        readaheadDirty = false;
                    }
                } catch (VirtualMachineError fatal) {
                    throw fatal;
                } catch (Throwable failure) {
                    if (failures.getAndIncrement() == 0) {
                        LOG.warn("Compaction I/O sampling or adjustment failed for database '{}'", name, failure);
                    }
                    // A failed adjustment must not turn missing feedback into a later growth retry.
                    if (readaheadDirty) budget.readaheadApplyFailed();
                    long restored = budget.sample(new CompactionIoBudget.Sample(System.nanoTime(), 0, 0, 0,
                            false, false, false, false));
                    try {
                        if (restored != limiter.getBytesPerSecond()) limiter.setBytesPerSecond(restored);
                    } catch (RuntimeException ignored) { failures.incrementAndGet(); }
                }
            };
        try {
            if (automaticPolling) executor.scheduleWithFixedDelay(poll, 1, 1, TimeUnit.SECONDS);
        } catch (RuntimeException | Error failure) {
            close();
            throw failure;
        }
    }

    @Override public synchronized void close() {
        if (closed) return;
        executor.shutdown();
        restoreShutdownBudget();
        boolean interrupted = false;
        for (;;) {
            try {
                if (executor.awaitTermination(1, TimeUnit.DAYS)) break;
            } catch (InterruptedException ignored) { interrupted = true; }
        }
        // An in-flight sample may have overwritten the first restore before it exited.
        restoreShutdownBudget();
        closed = true;
        meters.forEach(registry::remove);
        if (interrupted) Thread.currentThread().interrupt();
    }

    private void restoreShutdownBudget() {
        try { limiter.setBytesPerSecond(budget.shutdownBudget()); }
        catch (RuntimeException failure) { failures.incrementAndGet(); }
    }
}
