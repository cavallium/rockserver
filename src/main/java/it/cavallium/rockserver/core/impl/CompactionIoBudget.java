package it.cavallium.rockserver.core.impl;

/** Single-threaded policy; inputs are cumulative native counters, never metric scrape values. */
public final class CompactionIoBudget {
    public static final long MIN_BYTES_PER_SECOND = 4096 * 10; // one aligned page per 100ms refill
    public static final long MAX_BYTES_PER_SECOND = Long.MAX_VALUE / 1_000_000;
    public enum State { BOOTSTRAP, TRACKING, PROBE, RECOVERY }
    public record Sample(long nanos, long readCount, long readMicros, long bytes,
                         boolean background, boolean pressure, boolean stopped, boolean valid,
                         boolean foregroundPending, long foregroundCompletions, boolean recoveryClear,
                         boolean urgentPressure) {
        public Sample(long nanos, long readCount, long readMicros, long bytes,
                      boolean background, boolean pressure, boolean stopped, boolean valid,
                      boolean foregroundPending, long foregroundCompletions, boolean recoveryClear) {
            this(nanos, readCount, readMicros, bytes, background, pressure, stopped, valid,
                    foregroundPending, foregroundCompletions, recoveryClear, pressure || stopped);
        }
        public Sample(long nanos, long readCount, long readMicros, long bytes,
                      boolean background, boolean pressure, boolean stopped, boolean valid) {
            this(nanos, readCount, readMicros, bytes, background, pressure, stopped, valid, false, 0, !pressure && !stopped);
        }
    }
    private final long seed;
    private volatile long budget;
    private volatile long shutdownBudget;
    private volatile State state = State.BOOTSTRAP;
    private Sample previous;
    private long windowNanos, reads, micros, bytes;
    private boolean background, pressuredWindow, pendingForeground, noProgressProbeUsed;
    private long completions;
    private volatile double baseline;
    private double recentAchieved, lastAchieved;
    private long preThrottle, probeBudget;
    private double probeLatency, probeAchieved, probeForegroundRate;
    private int settle, cooldown, healthyRecovery;

    public CompactionIoBudget(long seed) {
        this.seed = bound(seed);
        budget = shutdownBudget = this.seed;
    }
    public long budget() { return budget; }
    public double baselineReadMicros() { return baseline; }
    public long shutdownBudget() { return shutdownBudget; }
    public State state() { return state; }
    private static long bound(double value) {
        return (long) Math.max(MIN_BYTES_PER_SECOND, Math.min(MAX_BYTES_PER_SECOND, value));
    }
    private void clearWindow() {
        windowNanos = reads = micros = bytes = completions = 0;
        background = pressuredWindow = pendingForeground = false;
    }

    public long sample(Sample sample) {
        var old = previous;
        previous = sample;
        boolean nativePressure = sample.urgentPressure || sample.stopped;
        if (nativePressure && state != State.RECOVERY) {
            // Restore useful measured service once; repeated stalled polls must not ramp.
            boolean tracking = state == State.TRACKING;
            state = State.RECOVERY;
            healthyRecovery = 0;
            double usefulService = Math.max(preThrottle, Math.max(recentAchieved, probeBudget));
            if (usefulService > 0 && tracking) usefulService = Math.max(usefulService, budget);
            budget = bound(usefulService > 0 ? usefulService : seed);
            probeBudget = 0;
            clearWindow();
        }
        if (!sample.valid || old == null || !old.valid || sample.nanos <= old.nanos
                || sample.readCount < 0 || sample.readMicros < 0 || sample.bytes < 0 || sample.foregroundCompletions < 0
                || sample.nanos - old.nanos > 3_000_000_000L || sample.readCount < old.readCount
                || sample.readMicros < old.readMicros || sample.bytes < old.bytes
                || sample.foregroundCompletions < old.foregroundCompletions) {
            if (state == State.PROBE) finishProbe(false, 0);
            if (state == State.RECOVERY) healthyRecovery = 0;
            clearWindow();
            return publishBudget();
        }
        long elapsed = sample.nanos - old.nanos;
        long completed = sample.readCount - old.readCount;
        long transferred = sample.bytes - old.bytes;
        pressuredWindow |= nativePressure;
        pendingForeground |= sample.foregroundPending;
        long completedActions = sample.foregroundCompletions - old.foregroundCompletions;
        if (completedActions > 0) noProgressProbeUsed = false;
        completions += Math.max(0, completedActions);
        windowNanos += elapsed;
        reads += completed;
        micros += sample.readMicros - old.readMicros;
        bytes += transferred;
        background |= sample.background || old.background || transferred > 0;
        if (windowNanos < 5_000_000_000L) return publishBudget();
        double achieved = bytes * (1_000_000_000d / windowNanos);
        double latency = reads >= 32 && completions >= 32 ? (double) micros / reads : Double.NaN;
        double foregroundRate = completions * (1_000_000_000d / windowNanos);
        boolean active = background && bytes > 0;
        boolean pressureInWindow = pressuredWindow;
        boolean noForegroundProgress = pendingForeground && completions == 0;
        long completedInWindow = completions;
        boolean foregroundIdle = !pendingForeground;
        clearWindow();
        if (state == State.RECOVERY) {
            if (achieved > 0) recentAchieved = recentAchieved == 0 ? achieved : recentAchieved * .5 + achieved * .5;
            if (pressureInWindow) {
                healthyRecovery = 0;
                if (achieved >= budget * .8) budget = bound(Math.max(budget, Math.min(budget * 1.1, recentAchieved * 1.25)));
                return publishBudget();
            }
            if (++healthyRecovery >= 2) { state = State.BOOTSTRAP; baseline = 0; }
            return publishBudget();
        }
        lastAchieved = active ? achieved : 0;
        if (state == State.PROBE) {
            if (--settle > 0) return publishBudget();
            finishProbe(active && Double.isFinite(latency) && latency > 0 && latency <= probeLatency * .9
                    && probeAchieved > 0 && achieved <= probeAchieved * .9
                    && foregroundRate >= probeForegroundRate * .9, latency);
            return publishBudget();
        }
        if (!active) {
            if (Double.isFinite(latency) && latency > 0) {
                baseline = baseline == 0 ? latency : baseline * .8 + latency * .2;
            }
            return publishBudget();
        }
        recentAchieved = recentAchieved == 0 ? achieved : recentAchieved * .5 + achieved * .5;
        if (state == State.BOOTSTRAP) {
            // One active window learns actual service; no device MB/s assumption.
            budget = bound(achieved);
            preThrottle = bound(achieved);
            state = State.TRACKING;
            if (Double.isFinite(latency) && latency > 0) {
                baseline = latency;
                beginProbe(latency, foregroundRate);
            }
            return publishBudget();
        }
        // Cache hits/mutations or proven foreground idleness can establish progress without block misses.
        if (!Double.isFinite(latency) || latency <= 0) {
            if ((completedInWindow >= 32 || foregroundIdle) && achieved >= budget * .8) {
                budget = bound(Math.max(budget, Math.min(budget * 1.1, recentAchieved * 1.25)));
            } else if (noForegroundProgress && !noProgressProbeUsed && baseline > 0) {
                noProgressProbeUsed = true;
                beginProbe(baseline, foregroundRate);
            }
            return publishBudget();
        }
        if (baseline == 0) baseline = latency;
        if (cooldown == 0) {
            beginProbe(latency, foregroundRate);
        } else {
            cooldown--;
            if (latency < baseline * 1.2) {
                baseline = baseline * .9 + latency * .1;
                if (achieved >= budget * .8) {
                    budget = bound(Math.max(budget, Math.min(budget * 1.1, recentAchieved * 1.25)));
                }
            }
        }
        return publishBudget();
    }
    private long publishBudget() {
        shutdownBudget = bound(Math.max(Math.max(seed, budget),
                Math.max(Math.max(preThrottle, probeBudget), recentAchieved)));
        return budget;
    }
    private void finishProbe(boolean improved, double latency) {
        if (improved) baseline = latency;
        else {
            budget = probeBudget;
            if (Double.isFinite(latency) && latency > 0) baseline = baseline * .5 + latency * .5;
        }
        probeBudget = 0;
        state = State.TRACKING;
        cooldown = 12;
    }
    private void beginProbe(double latency, double foregroundRate) {
        long candidate = bound(Math.min(budget * .75, lastAchieved > 0 ? lastAchieved * .75 : budget));
        if (candidate >= budget) {
            baseline = latency;
            cooldown = 12;
            return;
        }
        probeBudget = budget;
        probeLatency = latency;
        probeAchieved = lastAchieved;
        probeForegroundRate = foregroundRate;
        preThrottle = Math.max(preThrottle, bound(recentAchieved));
        budget = candidate;
        state = State.PROBE;
        settle = 2;
    }
}
