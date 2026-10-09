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
                         long compactionReadCount, long prefetchCount, long prefetchBytes,
                         long bulkCount, long bulkMicros, long bulkKeys, int readWorkers, int readActive,
                         int latencyReadActive, int latencyReadQueued, long readCompletions, boolean bulkPending) {
        public Sample(long nanos, long readCount, long readMicros, long bytes,
                      boolean background, boolean pressure, boolean stopped, boolean valid,
                      boolean foregroundPending, long foregroundCompletions, boolean recoveryClear,
                      boolean urgentPressure, long compactionReadBytes, long compactionReadMicros,
                      long compactionReadCount, long prefetchCount, long prefetchBytes,
                      long bulkCount, long bulkMicros, long bulkKeys, int readWorkers, int readActive,
                      int latencyReadActive, int latencyReadQueued, long readCompletions) {
            this(nanos, readCount, readMicros, bytes, background, pressure, stopped, valid,
                    foregroundPending, foregroundCompletions, recoveryClear, urgentPressure,
                    compactionReadBytes, compactionReadMicros, compactionReadCount, prefetchCount, prefetchBytes,
                    bulkCount, bulkMicros, bulkKeys, readWorkers, readActive, latencyReadActive, latencyReadQueued,
                    readCompletions, false);
        }
        public Sample(long nanos, long readCount, long readMicros, long bytes,
                      boolean background, boolean pressure, boolean stopped, boolean valid,
                      boolean foregroundPending, long foregroundCompletions, boolean recoveryClear,
                      boolean urgentPressure, long compactionReadBytes, long compactionReadMicros,
                      long compactionReadCount, long prefetchCount, long prefetchBytes) {
            this(nanos, readCount, readMicros, bytes, background, pressure, stopped, valid,
                    foregroundPending, foregroundCompletions, recoveryClear, urgentPressure,
                    compactionReadBytes, compactionReadMicros, compactionReadCount, prefetchCount, prefetchBytes,
                    0, 0, 0, 0, 0, 0, 0, 0);
        }
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
    private long bulkCalls, bulkTime, bulkKeys, readSaturatedNanos, readPendingNanos, readQueuedNanos, readCompleted;
    private boolean bulkEpochValid = true, bulkSeen, bulkIdle = true, bulkComparable, bulkHealthy,
            readGrowthVeto, readPressureClear, bulkNoProgressProbeUsed, probePointProtected, probeBulkProtected,
            probeReadSaturated, probeReadPressureClear, probeBulkRebaseAllowed;
    private int bulkIdleWindows, bulkCalibrationWindows, readClearWindows;
    private volatile double bulkBaseline;
    private double bulkShape, calibrationShape, calibrationLatency, bulkLatency, bulkCallRate, bulkKeyRate,
            lastBulkCallRate, lastBulkKeyRate, probeBulkLatency, probeBulkShape, probeBulkCallRate, probeBulkKeyRate;

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
    public double bulkBaselineMicrosPerKey() { return bulkBaseline; }
    public double baselineReadMicros() { return baseline; }
    public long shutdownBudget() { return shutdownBudget; }
    public State state() { return state; }
    private static long bound(double value) {
        return (long) Math.max(MIN_BYTES_PER_SECOND, Math.min(MAX_BYTES_PER_SECOND, value));
    }
    private void clearWindow() {
        windowNanos = reads = micros = bytes = completions = 0;
        background = pressuredWindow = readaheadPressure = pendingForeground = false;
        bulkCalls = bulkTime = bulkKeys = readSaturatedNanos = readPendingNanos = readQueuedNanos = readCompleted = 0;
        bulkEpochValid = true;
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
            invalidateBulk();
            clearWindow();
            clearReadWindow();
            foregroundHealthy = false;
            return publishBudget();
        }
        long elapsed = sample.nanos - old.nanos;
        observeReadService(sample, old, elapsed);
        accumulateBulk(sample, old, elapsed);
        if (!bulkEpochValid && state == State.PROBE) finishProbe(false, Double.NaN);
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
        boolean bulkNoProgress = bulkSeen && bulkCalls == 0 && readPendingNanos >= windowNanos / 2
                && (readSaturatedNanos >= windowNanos / 2 || readCompleted == 0);
        updateBulk(windowNanos);
        boolean pointReliable = Double.isFinite(latency) && latency > 0;
        boolean pointHealthy = !pointReliable || baseline > 0 && latency <= baseline * 1.2;
        boolean bulkSafe = bulkIdle || bulkComparable && bulkHealthy;
        boolean growthAllowed = !readGrowthVeto && bulkSafe;
        foregroundHealthy = active && (pointReliable || bulkComparable) && pointHealthy
                && growthAllowed && !readaheadPressureInWindow;
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
            boolean pointOk = !probePointProtected || pointReliable && latency <= probeLatency * 1.05
                    && foregroundRate >= probeForegroundRate * .9;
            boolean bulkOk = !probeBulkProtected ? bulkIdle : bulkComparable
                    && compatibleShape(bulkShape, probeBulkShape) && bulkLatency <= probeBulkLatency * 1.05
                    && bulkCallRate >= probeBulkCallRate * .9 && bulkKeyRate >= probeBulkKeyRate * .9;
            boolean pointBenefit = probePointProtected && pointReliable && latency <= probeLatency * .9;
            boolean bulkBenefit = probeBulkProtected && bulkComparable && bulkLatency <= probeBulkLatency * .9;
            boolean benefit = active && pointOk && bulkOk
                    && (probeReadSaturated ? bulkBenefit : pointBenefit || bulkBenefit)
                    && probeAchieved > 0 && achieved <= probeAchieved * .9;
            probeBulkRebaseAllowed = !benefit && probeBulkProtected && bulkComparable && pointOk && bulkOk
                    && active && probeAchieved > 0 && achieved <= probeAchieved * .9
                    && probeReadPressureClear && readPressureClear;
            if (!benefit) finishProbe(false, latency);
            else if (++probeBenefits >= 2) finishProbe(true, pointReliable ? Math.max(firstProbeBenefitLatency, latency) : Double.NaN);
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
            if (pointReliable) baseline = latency;
            if ((pointReliable || bulkComparable) && bulkSafe) beginProbe(latency, foregroundRate);
            return publishBudget();
        }
        if (bulkNoProgress && !bulkNoProgressProbeUsed && bulkBaseline > 0) {
            bulkNoProgressProbeUsed = true;
            beginProbe(latency, foregroundRate);
            return publishBudget();
        }
        // Cache hits/mutations or proven foreground idleness cannot conceal an active slow/sparse bulk lane.
        if (!pointReliable && !bulkComparable) {
            if (growthAllowed && (completedInWindow >= 32 || foregroundIdle) && achieved >= budget * .8) {
                budget = bound(Math.max(budget, Math.min(budget * 1.1, recentAchieved * 1.25)));
            } else if (noForegroundProgress && !noProgressProbeUsed && baseline > 0) {
                noProgressProbeUsed = true;
                beginProbe(baseline, foregroundRate);
            }
            return publishBudget();
        }
        if (pointReliable && baseline == 0) baseline = latency;
        if (cooldown == 0 && !rateProbeBlocked() && (bulkIdle || bulkComparable)) {
            beginProbe(latency, foregroundRate);
        } else {
            if (cooldown > 0) cooldown--;
            if (pointHealthy && growthAllowed) {
                if (pointReliable) baseline = baseline * .9 + latency * .1;
                if (achieved >= budget * .8) {
                    budget = bound(Math.max(budget, Math.min(budget * 1.1, recentAchieved * 1.25)));
                }
            }
        }
        return publishBudget();
    }
    private static boolean compatibleShape(double shape, double reference) {
        return reference > 0 && Math.abs(shape / reference - 1) <= .25;
    }
    private void invalidateBulk() {
        bulkBaseline = 0; bulkCalibrationWindows = bulkIdleWindows = readClearWindows = 0;
        bulkComparable = bulkHealthy = false;
        if (bulkSeen) bulkIdle = false;
        readGrowthVeto = true; readPressureClear = false; // Invalid samples cannot clear a pressure latch.
    }
    private void accumulateBulk(Sample sample, Sample old, long elapsed) {
        if (sample.bulkCount < old.bulkCount || sample.bulkMicros < old.bulkMicros || sample.bulkKeys < old.bulkKeys
                || sample.readCompletions < old.readCompletions || sample.bulkCount < 0 || sample.bulkMicros < 0
                || sample.bulkKeys < 0 || sample.readWorkers < 0 || sample.readActive < 0
                || sample.latencyReadActive < 0 || sample.latencyReadQueued < 0) {
            bulkEpochValid = false; invalidateBulk(); return;
        }
        bulkCalls += sample.bulkCount - old.bulkCount;
        bulkTime += sample.bulkMicros - old.bulkMicros;
        bulkKeys += sample.bulkKeys - old.bulkKeys;
        readCompleted += sample.readCompletions - old.readCompletions;
        if (old.readWorkers > 0 && old.readActive >= old.readWorkers && old.latencyReadQueued > 0)
            readSaturatedNanos += elapsed;
        if (old.bulkPending || sample.bulkPending) {
            bulkSeen = true; bulkIdle = false;
            readPendingNanos += elapsed;
        }
        if (old.latencyReadQueued > 0) readQueuedNanos += elapsed;
        if (sample.bulkCount > old.bulkCount) bulkNoProgressProbeUsed = false;
    }
    private void updateBulk(long elapsed) {
        bulkComparable = bulkHealthy = false;
        if (!bulkEpochValid) return;
        if (readSaturatedNanos >= elapsed / 2) { readGrowthVeto = true; readClearWindows = 0; }
        else if (readSaturatedNanos <= elapsed / 5) {
            if (++readClearWindows >= 2) readGrowthVeto = false;
        } else readClearWindows = 0;
        readPressureClear = !readGrowthVeto && readQueuedNanos == 0;
        if (bulkCalls == 0) {
            bulkCalibrationWindows = 0;
            if (readPendingNanos == 0 && ++bulkIdleWindows >= 2) bulkIdle = true;
            else if (readPendingNanos > 0) {
                bulkIdleWindows = 0;
                if (bulkSeen) bulkIdle = false; // New unresolved read activity revokes a previously proven idle lane.
            }
            return;
        }
        bulkSeen = true; bulkIdle = false; bulkIdleWindows = 0;
        if (bulkCalls < 32 || bulkKeys <= 0 || bulkTime <= 0) { bulkCalibrationWindows = 0; return; }
        double shape = (double) bulkKeys / bulkCalls;
        bulkLatency = (double) bulkTime / bulkKeys; // Independent microseconds/key; never mixed with block-miss means.
        bulkCallRate = bulkCalls * (1_000_000_000d / elapsed);
        bulkKeyRate = bulkKeys * (1_000_000_000d / elapsed);
        if (bulkBaseline == 0 || !compatibleShape(shape, bulkShape)) {
            if (bulkCalibrationWindows == 0 || !compatibleShape(shape, calibrationShape)) {
                bulkCalibrationWindows = 1; calibrationShape = shape; calibrationLatency = bulkLatency;
                bulkBaseline = 0; return;
            }
            bulkBaseline = (calibrationLatency + bulkLatency) / 2;
            bulkCalibrationWindows = 0;
        }
        bulkShape = shape;
        bulkComparable = true;
        bulkHealthy = bulkLatency <= bulkBaseline * 1.2;
        lastBulkCallRate = bulkCallRate; lastBulkKeyRate = bulkKeyRate;
        if (bulkHealthy && !readGrowthVeto && state == State.TRACKING)
            bulkBaseline = bulkBaseline * .9 + bulkLatency * .1;
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
        if (improved) {
            if (Double.isFinite(latency) && latency > 0) baseline = latency;
            if (probeBulkProtected && bulkComparable) bulkBaseline = bulkLatency;
        }
        else {
            budget = probeBudget;
            if (Double.isFinite(latency) && latency > 0) baseline = baseline * .5 + latency * .5;
            // A stable media/workset shift may not respond to throttling; only clear, guarded no-benefit trials rebase it.
            if (probeBulkRebaseAllowed) bulkBaseline = bulkBaseline * .5 + bulkLatency * .5;
        }
        probeBulkRebaseAllowed = false;
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
        probePointProtected = Double.isFinite(latency) && latency > 0;
        probeBulkProtected = bulkSeen && !bulkIdle;
        probeBulkLatency = bulkComparable ? bulkLatency : bulkBaseline;
        probeBulkShape = bulkShape;
        probeBulkCallRate = bulkComparable ? bulkCallRate : lastBulkCallRate;
        probeBulkKeyRate = bulkComparable ? bulkKeyRate : lastBulkKeyRate;
        probeReadSaturated = readGrowthVeto && probeBulkProtected;
        probeReadPressureClear = readPressureClear;
        probeBulkRebaseAllowed = false;
        preThrottle = Math.max(preThrottle, bound(recentAchieved));
        budget = candidate;
        state = State.PROBE;
        settle = 2;
        probeBenefits = 0;
        clearReadWindow();
    }
}
