package it.cavallium.rockserver.core.impl;

/** Bounded completed-call telemetry. Native calls and their clocks stay outside stripe locks. */
public final class StartupReadCompletions {
    private final Stripe[] stripes = new Stripe[64];
    private long generation;
    private Snapshot previous;

    public StartupReadCompletions() {
        for (int i = 0; i < stripes.length; i++) stripes[i] = new Stripe();
    }

    public void recordPoint(long nanos, boolean failed) { record(nanos, 1, failed, false); }
    public void recordBulk(long nanos, long keys, boolean failed) { record(nanos, keys, failed, true); }

    private void record(long nanos, long keys, boolean failed, boolean bulk) {
        if (nanos < 0 || keys < 0) throw new IllegalArgumentException("Negative completion measurement");
        var stripe = stripes[(int) Thread.currentThread().threadId() & (stripes.length - 1)];
        synchronized (stripe) {
            var counter = bulk ? stripe.bulk : stripe.point;
            try {
                counter.count = Math.addExact(counter.count, 1);
                counter.nanos = Math.addExact(counter.nanos, nanos);
                counter.keys = Math.addExact(counter.keys, keys);
                counter.failures = Math.addExact(counter.failures, failed ? 1 : 0);
                counter.tail = Math.max(counter.tail, nanos);
            } catch (ArithmeticException overflow) { stripe.valid = false; }
        }
    }

    /** One vector cut per epoch; all consumers must share this returned snapshot. */
    public synchronized Snapshot snapshot(long epoch) {
        if (epoch < 0 || previous != null && epoch < previous.epoch())
            throw new IllegalArgumentException("Epoch must be monotonic");
        if (previous != null && previous.epoch() == epoch) return previous;
        var point = new Counter();
        var bulk = new Counter();
        boolean valid = true;
        for (var stripe : stripes) {
            synchronized (stripe) {
                valid &= stripe.valid;
                try { point.add(stripe.point); bulk.add(stripe.bulk); }
                catch (ArithmeticException overflow) { valid = false; }
                stripe.point.clear(); stripe.bulk.clear();
            }
        }
        long pointTotal = previous == null ? 0 : previous.pointCompleted();
        long bulkTotal = previous == null ? 0 : previous.bulkCompleted();
        try {
            pointTotal = Math.addExact(pointTotal, point.count);
            bulkTotal = Math.addExact(bulkTotal, bulk.count);
        } catch (ArithmeticException overflow) { valid = false; }
        previous = new Snapshot(epoch, generation, pointTotal, bulkTotal, point.cohort(), bulk.cohort(), valid);
        return previous;
    }

    /** A reset discards the old generation explicitly; consumers must rebootstrap. */
    public synchronized void reset() {
        for (var stripe : stripes) {
            synchronized (stripe) { stripe.point.clear(); stripe.bulk.clear(); stripe.valid = true; }
        }
        generation = Math.incrementExact(generation);
        previous = null;
    }

    /** tailNanos is the maximum completed-call duration in this drained epoch, never a percentile. */
    public record Cohort(long count, long nanos, long tailNanos, long keys, long failures) {
        public boolean valid() {
            return count >= 0 && nanos >= 0 && tailNanos >= 0 && keys >= 0 && failures >= 0
                    && failures <= count && tailNanos <= nanos
                    && (count == 0 || nanos / count < tailNanos
                    || nanos / count == tailNanos && nanos % count == 0)
                    && (count != 0 || nanos == 0 && tailNanos == 0 && keys == 0 && failures == 0);
        }
    }
    public record Snapshot(long epoch, long generation, long pointCompleted, long bulkCompleted,
                           Cohort point, Cohort bulk, boolean valid) {}

    private static final class Stripe {
        final Counter point = new Counter(), bulk = new Counter();
        boolean valid = true;
    }
    private static final class Counter {
        long count, nanos, tail, keys, failures;
        void add(Counter other) {
            count = Math.addExact(count, other.count); nanos = Math.addExact(nanos, other.nanos);
            keys = Math.addExact(keys, other.keys); failures = Math.addExact(failures, other.failures);
            tail = Math.max(tail, other.tail);
        }
        Cohort cohort() { return new Cohort(count, nanos, tail, keys, failures); }
        void clear() { count = nanos = tail = keys = failures = 0; }
    }
}
