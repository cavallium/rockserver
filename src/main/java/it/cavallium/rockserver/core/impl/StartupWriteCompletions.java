package it.cavallium.rockserver.core.impl;

/** Completed native DB-write/commit calls, independently calibrated from point and bulk reads. */
public final class StartupWriteCompletions {
    // Reuse the bounded striped drain/overflow machinery, without exposing read kinds or key counts.
    private final StartupReadCompletions counters = new StartupReadCompletions();
    private Snapshot previous;
    private final java.util.concurrent.atomic.AtomicInteger active = new java.util.concurrent.atomic.AtomicInteger();

    public void record(long nanos, boolean failed) { counters.recordPoint(nanos, failed); }
    public synchronized long begin() { active.incrementAndGet(); return System.nanoTime(); }
    public long complete(long started, boolean failed) {
        long nanos = Math.max(0L, System.nanoTime() - started);
        try { record(nanos, failed); } finally { active.decrementAndGet(); }
        return nanos;
    }
    public synchronized Snapshot snapshot(long epoch) {
        var cut = counters.snapshot(epoch);
        if (previous != null && previous.epoch() == epoch) return previous;
        var calls = cut.point();
        return previous = new Snapshot(epoch, cut.generation(), cut.pointCompleted(), active.get(),
                new Cohort(calls.count(), calls.nanos(), calls.tailNanos(), calls.failures()), cut.valid());
    }
    public synchronized void reset() {
        if (active.get() != 0) throw new IllegalStateException("Cannot reset active native writes");
        counters.reset(); previous = null;
    }
    public record Cohort(long count, long nanos, long tailNanos, long failures) {
        public boolean valid() {
            return new StartupReadCompletions.Cohort(count, nanos, tailNanos, count, failures).valid();
        }
    }
    public record Snapshot(long epoch, long generation, long completed, int active, Cohort calls, boolean valid) {}
}
