package it.cavallium.rockserver.core.impl;

/** Single-threaded policy; inputs are cumulative native counters, never metric scrape values. */
public final class CompactionIoBudget {
    public static final long MIN_BYTES_PER_SECOND = 4096 * 10; // one aligned page per 100ms refill
    public static final long MAX_BYTES_PER_SECOND = Long.MAX_VALUE / 1_000_000;
    public static final long MAX_READAHEAD_BYTES = 16L << 20;
    public enum State { BOOTSTRAP, TRACKING, PROBE, RECOVERY }
    public record Sample(long nanos, long readCount, long readMicros, long bytes,
                         boolean background, boolean pressure, boolean stopped, boolean valid,
                         boolean foregroundPending, long foregroundCompletions, boolean recoveryClear,
                         boolean urgentPressure, long compactionReadBytes, long compactionReadMicros,
                         long compactionReadCount, long prefetchCount, long prefetchBytes) {
        public Sample(long nanos, long readCount, long readMicros, long bytes,
                      boolean background, boolean pressure, boolean stopped, boolean valid,
                      boolean foregroundPending, long foregroundCompletions, boolean recoveryClear,
                      boolean urgentPressure) {
            this(nanos, readCount, readMicros, bytes, background, pressure, stopped, valid,
                    foregroundPending, foregroundCompletions, recoveryClear, urgentPressure, 0, 0, 0, 0, 0);
        }
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
    private final long seed, readaheadCeiling;
    private volatile long readaheadBytes;
    private long appliedReadahead, readWindowNanos, compactionBytes, compactionMicros, compactionReads,
            prefetchEvents, prefetchedBytes, settleUntil, probeHoldUntil;
    private double serviceRate, prefetchMean, priorPrefetchMean;
    private long priorReadahead;
    private int reliableGrowthWindows;
    private boolean readWindowTracking = true, foregroundHealthy, readaheadSettling, readaheadUnknown,
            previousReadWindowReliable, prefetchPopulationStable;
    private volatile long budget;
    private volatile long shutdownBudget;
    private volatile State state = State.BOOTSTRAP;
    private Sample previous;
    private long windowNanos, reads, micros, bytes;
    private boolean background, pressuredWindow, readaheadPressure, pendingForeground, noProgressProbeUsed;
    private long completions;
    private volatile double baseline;
    private double recentAchieved, lastAchieved;
    private long preThrottle, probeBudget;
    private double probeLatency, probeAchieved, probeForegroundRate;
    private int settle, cooldown, healthyRecovery, probeBenefits;
    private double firstProbeBenefitLatency;

    public CompactionIoBudget(long seed) { this(seed, 0); }
    public CompactionIoBudget(long seed, long readaheadCeiling) {
        if (readaheadCeiling < 0 || readaheadCeiling > MAX_READAHEAD_BYTES) {
            throw new IllegalArgumentException("Readahead ceiling must be between 0 and 16MiB");
        }
        this.readaheadCeiling = readaheadCeiling;
        this.seed = bound(seed);
        budget = shutdownBudget = this.seed;
    }
    public long budget() { return budget; }
    public long readaheadBytes() { return readaheadBytes; }
    void readaheadApplied(long bytes) {
        if (bytes < 0 || bytes > readaheadCeiling) throw new IllegalArgumentException("Invalid applied readahead");
        if (bytes != appliedReadahead || readaheadUnknown) {
            priorReadahead = appliedReadahead;
            priorPrefetchMean = prefetchMean;
            settleUntil = previous.nanos + 60_000_000_000L;
            probeHoldUntil = previous.nanos + 180_000_000_000L;
            readaheadSettling = true;
            clearReadWindow();
        }
        appliedReadahead = readaheadBytes = bytes;
        readaheadUnknown = false;
    }
    void readaheadApplyFailed() {
        // Native installation can precede a failing OPTIONS write. Force a conservative correction, even from cached 0.
        readaheadBytes = 0;
        if (!readaheadUnknown) {
            settleUntil = previous.nanos + 60_000_000_000L;
            probeHoldUntil = previous.nanos + 180_000_000_000L;
        }
        readaheadUnknown = readaheadSettling = true;
        clearReadWindow();
    }
    public double baselineReadMicros() { return baseline; }
    public long shutdownBudget() { return shutdownBudget; }
    public State state() { return state; }
    private static long bound(double value) {
        return (long) Math.max(MIN_BYTES_PER_SECOND, Math.min(MAX_BYTES_PER_SECOND, value));
    }
    private void clearWindow() {
        windowNanos = reads = micros = bytes = completions = 0;
        background = pressuredWindow = readaheadPressure = pendingForeground = false;
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
            clearReadWindow();
            foregroundHealthy = false;
            return publishBudget();
        }
        long elapsed = sample.nanos - old.nanos;
        observeReadService(sample, old, elapsed);
        long completed = sample.readCount - old.readCount;
        long transferred = sample.bytes - old.bytes;
        pressuredWindow |= nativePressure;
        readaheadPressure |= sample.pressure || old.pressure;
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
        boolean readaheadPressureInWindow = readaheadPressure;
        boolean noForegroundProgress = pendingForeground && completions == 0;
        long completedInWindow = completions;
        boolean foregroundIdle = !pendingForeground;
        foregroundHealthy = active && Double.isFinite(latency) && latency > 0 && baseline > 0
                && latency <= baseline * 1.2 && !readaheadPressureInWindow;
        clearWindow();
        readWindowTracking &= foregroundHealthy;
        evaluateReadService(sample);
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
            boolean benefit = active && Double.isFinite(latency) && latency > 0 && latency <= probeLatency * .9
                    && probeAchieved > 0 && achieved <= probeAchieved * .9
                    && foregroundRate >= probeForegroundRate * .9;
            if (!benefit) finishProbe(false, latency);
            else if (++probeBenefits >= 2) finishProbe(true, Math.max(firstProbeBenefitLatency, latency));
            else firstProbeBenefitLatency = latency;
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
        if (cooldown == 0 && !rateProbeBlocked()) {
            beginProbe(latency, foregroundRate);
        } else {
            if (cooldown > 0) cooldown--;
            if (latency < baseline * 1.2) {
                baseline = baseline * .9 + latency * .1;
                if (achieved >= budget * .8) {
                    budget = bound(Math.max(budget, Math.min(budget * 1.1, recentAchieved * 1.25)));
                }
            }
        }
        return publishBudget();
    }
    private long admissionReadaheadLimit() {
        long bytes = Math.min(readaheadCeiling, budget / 10) / 4096 * 4096;
        return bytes < 65536 ? 0 : bytes;
    }
    private void clearReadWindow() {
        readWindowNanos = compactionBytes = compactionMicros = compactionReads = prefetchEvents = prefetchedBytes = 0;
        readWindowTracking = true;
        reliableGrowthWindows = 0;
        previousReadWindowReliable = prefetchPopulationStable = false;
    }
    private boolean rateProbeBlocked() {
        if (!readaheadSettling && !readaheadUnknown) return false;
        if (previous.nanos >= probeHoldUntil) return false;
        return previous.nanos < settleUntil || !prefetchPopulationStable;
    }
    private void observeReadService(Sample sample, Sample old, long elapsed) {
        if (readaheadCeiling == 0) return;
        if (sample.compactionReadBytes < old.compactionReadBytes || sample.compactionReadMicros < old.compactionReadMicros
                || sample.compactionReadCount < old.compactionReadCount || sample.prefetchCount < old.prefetchCount
                || sample.prefetchBytes < old.prefetchBytes || sample.compactionReadBytes < 0
                || sample.compactionReadMicros < 0 || sample.compactionReadCount < 0
                || sample.prefetchCount < 0 || sample.prefetchBytes < 0) {
            clearReadWindow(); serviceRate = 0;
            return;
        }
        readWindowNanos += elapsed;
        compactionBytes += sample.compactionReadBytes - old.compactionReadBytes;
        compactionMicros += sample.compactionReadMicros - old.compactionReadMicros;
        compactionReads += sample.compactionReadCount - old.compactionReadCount;
        prefetchEvents += sample.prefetchCount - old.prefetchCount;
        prefetchedBytes += sample.prefetchBytes - old.prefetchBytes;
        readWindowTracking &= state == State.TRACKING && !sample.pressure && !old.pressure;
    }
    private void evaluateReadService(Sample sample) {
        if (readWindowNanos < 30_000_000_000L) return;
        long events = prefetchEvents;
        double mean = events > 0 ? (double) prefetchedBytes / events : 0;
        double rawRate = compactionMicros > 0 ? compactionBytes * (1_000_000d / compactionMicros) : 0;
        boolean reliable = compactionReads >= 32 && compactionBytes > 0 && rawRate > 0 && Double.isFinite(rawRate);
        boolean tracking = readWindowTracking && foregroundHealthy;
        readWindowNanos = compactionBytes = compactionMicros = compactionReads = prefetchEvents = prefetchedBytes = 0;
        readWindowTracking = true;
        if (!reliable) {
            reliableGrowthWindows = 0;
            previousReadWindowReliable = prefetchPopulationStable = false;
            return;
        }
        prefetchPopulationStable = previousReadWindowReliable
                && Math.abs(mean - prefetchMean) <= Math.max(4096, prefetchMean * .25);
        previousReadWindowReliable = true;
        // Summed reader time avoids concurrency inflating the estimate. Bound upward innovations, not safety decreases.
        serviceRate = serviceRate == 0 ? rawRate : serviceRate * .8 + Math.min(rawRate, serviceRate * 1.25) * .2;
        prefetchMean = mean;
        if (readaheadSettling && !readaheadUnknown && sample.nanos >= settleUntil) {
            boolean effect = appliedReadahead == 0 ? events == 0
                    : priorReadahead == 0 ? events >= 32 && mean > 0
                    : events >= 32 && (appliedReadahead > priorReadahead
                            ? mean >= priorPrefetchMean + (appliedReadahead - priorReadahead) * .5
                            : mean <= priorPrefetchMean - (priorReadahead - appliedReadahead) * .5);
            if (effect) readaheadSettling = false;
        }
        if (state == State.PROBE || readaheadUnknown) { reliableGrowthWindows = 0; return; }
        // 100ms estimated reader service and admission bounds, with 25% headroom BEFORE the operator ceiling.
        long usable = (long) Math.min(MAX_READAHEAD_BYTES,
                Math.min(Math.min(serviceRate, rawRate), budget) / 12.5);
        long target = Math.min(readaheadCeiling / 4096 * 4096, Long.highestOneBit(usable));
        if (target < 65536) target = 0;
        if (target < readaheadBytes && (target == 0 || target <= readaheadBytes * .75)) {
            readaheadBytes = target;
            reliableGrowthWindows = 0;
        } else if (tracking && state == State.TRACKING && !readaheadSettling && target > readaheadBytes) {
            if (++reliableGrowthWindows >= 2) {
                readaheadBytes = Math.min(target, readaheadBytes == 0 ? 65536 : readaheadBytes * 2);
                reliableGrowthWindows = 0;
            }
        } else reliableGrowthWindows = 0;
    }
    private long publishBudget() {
        // Freeze the second actuator during a rate probe. Its admission-time target can temporarily be exceeded;
        // physical bytes remain charged/chunked, and existing job buffers never had instantaneous resizing guarantees.
        if (state != State.PROBE) readaheadBytes = Math.min(readaheadBytes, admissionReadaheadLimit());
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
        clearReadWindow();
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
        probeBenefits = 0;
        clearReadWindow();
    }
}
