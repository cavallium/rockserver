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
                         int latencyReadActive, int latencyReadQueued, long readCompletions, boolean bulkPending,
                         long pointTailMicros, long bulkTailMicros, long nativePointCompletions, long nativeBulkCompletions, long nativeBulkKeys) {
        public Sample(long nanos, long readCount, long readMicros, long bytes,
                      boolean background, boolean pressure, boolean stopped, boolean valid,
                      boolean foregroundPending, long foregroundCompletions, boolean recoveryClear,
                      boolean urgentPressure, long compactionReadBytes, long compactionReadMicros,
                      long compactionReadCount, long prefetchCount, long prefetchBytes,
                      long bulkCount, long bulkMicros, long bulkKeys, int readWorkers, int readActive,
                      int latencyReadActive, int latencyReadQueued, long readCompletions, boolean bulkPending) {
            this(nanos, readCount, readMicros, bytes, background, pressure, stopped, valid,
                    foregroundPending, foregroundCompletions, recoveryClear, urgentPressure,
                    compactionReadBytes, compactionReadMicros, compactionReadCount, prefetchCount, prefetchBytes,
                    bulkCount, bulkMicros, bulkKeys, readWorkers, readActive, latencyReadActive, latencyReadQueued,
                    readCompletions, bulkPending, 0, 0, 0, 0, 0);
        }
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
            prefetchEvents, prefetchedBytes, settleUntil, probeHoldUntil, lastReadServiceNanos;
    private double serviceRate, prefetchMean, priorPrefetchMean;
    private double pointTailBaseline, bulkTailBaseline, pointTailCalibration, bulkTailCalibration,
            bulkTailShape, bulkTailCalibrationShape, lastBulkTailShape, probeBulkTailShape;
    private long lastPointTail, lastBulkTail, probePointTail, probeBulkTail;
    private long pointTailWindow, bulkTailWindow, nativePoints, nativeBulks, nativeBulkKeys;
    private int pointTailWindows, bulkTailWindows;
    private boolean latencyUnsafe, tailUnsafe;
    private double queueAnchor = -1, lastPointMean, lastBulkMean;
    private long queuedIntegral, safetyGrowthUntil;
    private int queueWorkers = -1, queueBadPolls, stableLoadedWindows;
    private long priorReadahead;
    private int reliableGrowthWindows;
    private boolean readWindowTracking = true, foregroundHealthy, readaheadSettling, readaheadUnknown,
            previousReadWindowReliable, prefetchPopulationStable;
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
        background = pressuredWindow = pendingForeground = false;
        bulkCalls = bulkTime = bulkKeys = readSaturatedNanos = readPendingNanos = readQueuedNanos = readCompleted = 0;
        bulkEpochValid = true;
        pointTailWindow = bulkTailWindow = nativePoints = nativeBulks = nativeBulkKeys = 0;
        latencyUnsafe = tailUnsafe = false;
        queuedIntegral = 0;
    }

    public long sample(Sample sample) {
        var old = previous;
        previous = sample;
        boolean nativePressure = sample.urgentPressure || sample.stopped;
        if (sample.stopped) reduceReadahead(0);
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
            if (sample.valid && old != null && old.valid
                    && (sample.nativePointCompletions < old.nativePointCompletions || sample.nativePointCompletions < 0
                        || sample.nativeBulkCompletions < old.nativeBulkCompletions || sample.nativeBulkCompletions < 0
                        || sample.nativeBulkKeys < old.nativeBulkKeys || sample.nativeBulkKeys < 0)) {
                pointTailBaseline = bulkTailBaseline = 0;
            }
            pointTailWindows = bulkTailWindows = 0;
            lastPointMean = lastBulkMean = 0;
            reduceReadahead(0);
            return publishBudget();
        }
        long elapsed = sample.nanos - old.nanos;
        observeReadService(sample, old, elapsed);
        accumulateBulk(sample, old, elapsed);
        observeForegroundSafety(sample, old);
        if (!bulkEpochValid && state == State.PROBE) finishProbe(false, Double.NaN);
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
        boolean noForegroundProgress = pendingForeground && completions == 0 && nativePoints == 0 && nativeBulks == 0;
        long completedInWindow = completions;
        boolean foregroundIdle = !pendingForeground;
        boolean bulkNoProgress = bulkSeen && bulkCalls == 0 && nativeBulks == 0 && readPendingNanos >= windowNanos / 2
                && (readSaturatedNanos >= windowNanos / 2 || readCompleted == 0);
        double queueMean = (double) queuedIntegral / windowNanos;
        if (queueAnchor >= 0 && queueMean > queueAnchor + Math.max(2, queueAnchor * .2)) {
            queueAnchor = queueMean;
            reduceReadahead(0);
            latencyUnsafe = true;
        }
        long previousPointTail = lastPointTail, previousBulkTail = lastBulkTail;
        double previousBulkTailShape = lastBulkTailShape;
        long pointTailInWindow = pointTailWindow, bulkTailInWindow = bulkTailWindow;
        double nativeBulkShapeInWindow = nativeBulks > 0 ? (double) nativeBulkKeys / nativeBulks : 0;
        lastPointTail = nativePoints >= 32 ? pointTailInWindow : 0;
        lastBulkTail = nativeBulks >= 32 && nativeBulkShapeInWindow > 0 ? bulkTailInWindow : 0;
        lastBulkTailShape = nativeBulkShapeInWindow;
        updateBulk(windowNanos);
        boolean pointReliable = Double.isFinite(latency) && latency > 0;
        boolean pointHealthy = !pointReliable || baseline > 0 && latency <= baseline * 1.2;
        boolean bulkSafe = bulkIdle || bulkComparable && bulkHealthy;
        boolean pointTailActive = nativePoints > 0 || pointTailWindow > 0;
        boolean bulkTailActive = nativeBulks > 0 || bulkTailWindow > 0;
        boolean tailsCalibrated = (!pointTailActive || pointTailBaseline > 0 && nativePoints >= 32)
                && (bulkIdle && !bulkTailActive || bulkComparable && bulkTailBaseline > 0 && nativeBulks >= 32);
        boolean growthAllowed = !readGrowthVeto && bulkSafe; // Byte-rate demand remains a separate policy.
        boolean unhealthyMean = pointReliable && baseline > 0 && latency > baseline * 1.2
                || bulkComparable && bulkBaseline > 0 && bulkLatency > bulkBaseline * 1.2;
        if (unhealthyMean || noForegroundProgress || bulkNoProgress) reduceReadahead(0);
        boolean ownPointReliable = !pointTailActive || nativePoints >= 32 && pointTailInWindow > 0;
        boolean ownBulkReliable = bulkIdle && !bulkTailActive
                || bulkComparable && nativeBulks >= 32 && nativeBulkShapeInWindow > 0 && bulkTailInWindow > 0;
        boolean sameBulkShape = previousBulkTailShape == 0 || nativeBulkShapeInWindow == 0
                || compatibleShape(nativeBulkShapeInWindow, previousBulkTailShape);
        boolean stationary = !latencyUnsafe && !unhealthyMean && !noForegroundProgress && !bulkNoProgress
                && ownPointReliable && ownBulkReliable && bulkSafe
                && (!pointReliable || lastPointMean == 0 || latency <= lastPointMean * 1.1)
                && (!bulkComparable || !sameBulkShape || lastBulkMean == 0 || bulkLatency <= lastBulkMean * 1.1)
                && (pointTailInWindow == 0 || previousPointTail == 0 || pointTailInWindow <= previousPointTail * 1.1)
                && (bulkTailInWindow == 0 || previousBulkTail == 0 || !sameBulkShape
                        || bulkTailInWindow <= previousBulkTail * 1.1)
                && (queueAnchor < 0 || queueMean <= queueAnchor + Math.max(2, queueAnchor * .2));
        if (stationary) {
            if (queueAnchor < 0) queueAnchor = queueMean; // Provisional until the second stationary window confirms it.
            stableLoadedWindows = Math.min(2, stableLoadedWindows + 1);
            if (stableLoadedWindows >= 2 && baseline == 0 && pointReliable) baseline = latency;
        } else stableLoadedWindows = 0;
        lastPointMean = pointReliable ? latency : 0;
        lastBulkMean = bulkComparable ? bulkLatency : 0;
        updateForegroundTails(nativePoints >= 32, stationary, unhealthyMean);
        pointHealthy = !pointReliable || baseline > 0 && latency <= baseline * 1.2;
        boolean readaheadGrowthAllowed = stableLoadedWindows >= 2 && tailsCalibrated && !latencyUnsafe
                && sample.nanos >= safetyGrowthUntil;
        boolean unsafeTailWindow = tailUnsafe;
        foregroundHealthy = active && (pointReliable || bulkComparable) && pointHealthy
                && readaheadGrowthAllowed;
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
            boolean pointTailOk = probePointTail == 0 || pointTailInWindow > 0
                    && pointTailInWindow <= probePointTail * 1.2;
            boolean bulkTailOk = probeBulkTail == 0 || bulkTailInWindow > 0
                    && bulkTailInWindow <= probeBulkTail * 1.2
                    && (nativeBulkShapeInWindow == 0 || compatibleShape(nativeBulkShapeInWindow, probeBulkTailShape));
            boolean benefit = !unsafeTailWindow && pointTailOk && bulkTailOk && active && pointOk && bulkOk
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
    private void observeForegroundSafety(Sample sample, Sample old) {
        if (sample.pointTailMicros < 0 || sample.bulkTailMicros < 0
                || sample.nativePointCompletions < old.nativePointCompletions || sample.nativePointCompletions < 0
                || sample.nativeBulkCompletions < old.nativeBulkCompletions || sample.nativeBulkCompletions < 0
                || sample.nativeBulkKeys < old.nativeBulkKeys || sample.nativeBulkKeys < 0) {
            reduceReadahead(0);
            pointTailBaseline = bulkTailBaseline = 0;
            pointTailWindows = bulkTailWindows = 0;
            latencyUnsafe = tailUnsafe = true;
            return;
        }
        boolean pointSlow = sample.pointTailMicros > 0 && pointTailBaseline > 0
                && sample.pointTailMicros > pointTailBaseline * 1.2;
        long calls = sample.nativeBulkCompletions - old.nativeBulkCompletions;
        long keys = sample.nativeBulkKeys - old.nativeBulkKeys;
        boolean bulkShapeMatches = calls > 0 && keys > 0 && compatibleShape((double) keys / calls, bulkTailShape);
        if (calls > 0 && keys > 0 && bulkTailBaseline > 0 && !bulkShapeMatches) {
            bulkTailBaseline = 0; bulkTailWindows = 0;
            readWindowTracking = false;
            stableLoadedWindows = 0;
        }
        boolean bulkSlow = sample.bulkTailMicros > 0 && bulkTailBaseline > 0 && (bulkShapeMatches || calls == 0 || keys == 0)
                && sample.bulkTailMicros > bulkTailBaseline * 1.2;
        if (queueWorkers < 0) queueWorkers = sample.readWorkers;
        else if (queueWorkers != sample.readWorkers) {
            queueWorkers = sample.readWorkers;
            queueAnchor = -1;
            lastPointMean = lastBulkMean = 0;
            pointTailWindows = bulkTailWindows = 0;
            reduceReadahead(0);
            clearReadWindow();
        }
        queuedIntegral += (long) old.latencyReadQueued * (sample.nanos - old.nanos);
        if (queueAnchor >= 0) {
            double deadband = Math.max(2, queueAnchor * .2);
            boolean full = sample.readWorkers > 0 && sample.readActive >= sample.readWorkers;
            if (full && sample.latencyReadQueued > queueAnchor + deadband) {
                if (++queueBadPolls >= 2) {
                    queueAnchor = sample.latencyReadQueued;
                    queueBadPolls = 0;
                    reduceReadahead(0);
                    latencyUnsafe = true;
                }
            } else queueBadPolls = 0;
            // Oscillation below an unsafe anchor still cannot supply stationary growth evidence.
            if (full && sample.latencyReadQueued > old.latencyReadQueued + deadband) latencyUnsafe = true;
        }
        boolean noProgress = sample.foregroundPending && sample.foregroundCompletions == old.foregroundCompletions
                && sample.nativePointCompletions == old.nativePointCompletions && calls == 0
                || sample.bulkPending && calls == 0;
        if (pointSlow || bulkSlow || noProgress) {
            reduceReadahead(0); latencyUnsafe = true;
        }
        if (pointSlow || bulkSlow) tailUnsafe = true;
        nativePoints += sample.nativePointCompletions - old.nativePointCompletions;
        nativeBulks += calls;
        nativeBulkKeys += keys;
        pointTailWindow = Math.max(pointTailWindow, sample.pointTailMicros);
        bulkTailWindow = Math.max(bulkTailWindow, sample.bulkTailMicros);
    }
    private void updateForegroundTails(boolean pointReliable, boolean healthy, boolean pressure) {
        if (pressure || latencyUnsafe || !healthy) {
            pointTailWindows = bulkTailWindows = 0;
            return;
        }
        if (pointReliable && pointTailWindow > 0) {
            if (pointTailBaseline == 0) {
                if (++pointTailWindows >= 2) pointTailBaseline = (pointTailCalibration + pointTailWindow) / 2;
                else pointTailCalibration = pointTailWindow;
            } else if (state == State.TRACKING || state == State.RECOVERY)
                pointTailBaseline = pointTailBaseline * .9 + Math.min(pointTailBaseline, pointTailWindow) * .1;
        } else pointTailWindows = 0;
        if (bulkComparable && nativeBulks >= 32 && nativeBulkKeys > 0 && bulkTailWindow > 0) {
            double shape = (double) nativeBulkKeys / nativeBulks;
            if (bulkTailBaseline == 0 || !compatibleShape(shape, bulkTailShape)) {
                if (bulkTailWindows == 0 || !compatibleShape(shape, bulkTailCalibrationShape)) {
                    bulkTailWindows = 1;
                    bulkTailCalibrationShape = shape;
                    bulkTailCalibration = bulkTailWindow;
                } else if (++bulkTailWindows >= 2) {
                    bulkTailBaseline = (bulkTailCalibration + bulkTailWindow) / 2;
                    bulkTailShape = shape;
                }
            } else if (state == State.TRACKING || state == State.RECOVERY)
                bulkTailBaseline = bulkTailBaseline * .9 + Math.min(bulkTailBaseline, bulkTailWindow) * .1;
        } else bulkTailWindows = 0;
    }
    private static boolean compatibleShape(double shape, double reference) {
        return reference > 0 && Math.abs(shape / reference - 1) <= .25;
    }
    private void invalidateBulk() {
        bulkTailWindows = 0;
        reduceReadahead(0);
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
            invalidateBulk(); bulkEpochValid = false; return;
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
    private long serviceReadaheadLimit(double rate) {
        long usable = (long) Math.min(MAX_READAHEAD_BYTES, rate / 12.5);
        long target = Math.min(readaheadCeiling / 4096 * 4096, Long.highestOneBit(usable));
        return target < 65536 ? 0 : target;
    }
    private void reduceReadahead(long limit) {
        boolean unsafe = limit == 0 || limit < readaheadBytes;
        if (unsafe) {
            safetyGrowthUntil = Math.max(safetyGrowthUntil, previous.nanos + 60_000_000_000L);
            stableLoadedWindows = queueBadPolls = 0;
            reliableGrowthWindows = 0;
            readWindowTracking = false;
        }
        if (limit < readaheadBytes) {
            readaheadBytes = limit;
            if (state == State.PROBE) {
                // Changing both actuators invalidates any causal benefit attributed to the byte-rate trial.
                finishProbe(false, Double.NaN);
                clearWindow();
            }
            reliableGrowthWindows = 0;
            readWindowTracking = false;
        }
        if (unsafe) latencyUnsafe = true;
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
            reduceReadahead(0);
            return;
        }
        long count = sample.compactionReadCount - old.compactionReadCount;
        long transferred = sample.compactionReadBytes - old.compactionReadBytes;
        long readTime = sample.compactionReadMicros - old.compactionReadMicros;
        if (count > 0 && transferred > 0 && readTime > 0) {
            lastReadServiceNanos = sample.nanos;
            double rate = transferred * (1_000_000d / readTime);
            if (serviceRate > 0 && rate < serviceRate) serviceRate = rate;
            reduceReadahead(readTime / count >= 100_000 ? 0 : serviceReadaheadLimit(Math.min(rate, budget)));
        } else if (count > 0 || sample.background && (lastReadServiceNanos == 0
                || sample.nanos - lastReadServiceNanos >= 3_000_000_000L)) {
            reduceReadahead(0);
        }
        readWindowNanos += elapsed;
        compactionBytes += sample.compactionReadBytes - old.compactionReadBytes;
        compactionMicros += sample.compactionReadMicros - old.compactionReadMicros;
        compactionReads += sample.compactionReadCount - old.compactionReadCount;
        prefetchEvents += sample.prefetchCount - old.prefetchCount;
        prefetchedBytes += sample.prefetchBytes - old.prefetchBytes;
        readWindowTracking &= state != State.PROBE;
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
        serviceRate = serviceRate == 0 || rawRate < serviceRate ? rawRate
                : serviceRate * .8 + Math.min(rawRate, serviceRate * 1.25) * .2;
        prefetchMean = mean;
        if (readaheadSettling && !readaheadUnknown && sample.nanos >= settleUntil) {
            boolean effect = appliedReadahead == 0 ? events == 0
                    : priorReadahead == 0 ? events >= 32 && mean > 0
                    : events >= 32 && (appliedReadahead > priorReadahead
                            ? mean >= priorPrefetchMean + (appliedReadahead - priorReadahead) * .5
                            : mean <= priorPrefetchMean - (priorReadahead - appliedReadahead) * .5);
            if (effect) readaheadSettling = false;
        }
        if (readaheadUnknown) { reliableGrowthWindows = 0; return; }
        // 100ms estimated reader service and admission bounds, with 25% headroom BEFORE the operator ceiling.
        long target = serviceReadaheadLimit(Math.min(Math.min(serviceRate, rawRate), budget));
        if (target < readaheadBytes && (target == 0 || target <= readaheadBytes * .75)) {
            reduceReadahead(target);
        } else if (tracking && state != State.PROBE && !readaheadSettling && target > readaheadBytes) {
            if (++reliableGrowthWindows >= 2) {
                readaheadBytes = Math.min(target, readaheadBytes == 0 ? 65536 : readaheadBytes * 2);
                reliableGrowthWindows = 0;
            }
        } else reliableGrowthWindows = 0;
    }
    private long publishBudget() {
        // A byte-rate probe may block growth, but never a latency-safety reduction.
        reduceReadahead(admissionReadaheadLimit());
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
        // A healthy rate trial must fit the acknowledged reader cap. Safety cuts run first and remove this floor.
        if (foregroundHealthy) candidate = Math.max(candidate, bound(Math.ceil(readaheadBytes * 12.5)));
        if (candidate >= budget) {
            baseline = latency;
            cooldown = 12;
            return;
        }
        probeBudget = budget;
        probeLatency = latency;
        probeAchieved = lastAchieved;
        probeForegroundRate = foregroundRate;
        probePointTail = lastPointTail;
        probeBulkTail = lastBulkTail;
        probeBulkTailShape = lastBulkTailShape;
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
