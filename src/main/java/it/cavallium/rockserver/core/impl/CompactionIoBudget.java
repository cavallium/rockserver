package it.cavallium.rockserver.core.impl;

/** Single-threaded policy; inputs are cumulative native counters, never metric scrape values. */
public final class CompactionIoBudget {
    public static final long MIN_BYTES_PER_SECOND = 4096 * 10; // one aligned page per 100ms refill
    public static final long MAX_BYTES_PER_SECOND = Long.MAX_VALUE / 1_000_000;
    public static final long MAX_READAHEAD_BYTES = 16L << 20;
    /** PROBE: the budget was lowered and the next windows decide whether the lower rate is kept (see beginProbe). */
    public enum State { BOOTSTRAP, TRACKING, PROBE, RECOVERY }
    /**
     * Multiplicative decrease on measured foreground harm, applied to the background rate actually achieved in the
     * harmful window (not to the budget, which may be twice that): it removes the slack and cuts the real rate by
     * 30% in one step. A kept decrease needs the background rate to fall by 10% and reads to get 10% faster, so the
     * step must be well over 10%; CUBIC uses the same factor, Reno 0.5 (which would give up half of a device that
     * is only marginally over capacity).
     */
    static final double HARM_DECREASE = .7;
    /** Consecutive harmful windows (5 s each) before a decrease: one slow window is noise, two are 10 s of slow reads. */
    static final int HARM_WINDOWS = 2;
    /** Windows after a decrease in which the budget does not grow: the settle window and two measured ones. */
    static final int HOLD_WINDOWS = 3;
    /** Below this share of the rate at which harm was measured, growth is +10% of that rate per saturated window. */
    static final double CEILING_APPROACH = .9;
    /** From there on: +5% of that rate every 6th saturated healthy window (30 s), so that harm recurs rarely. */
    static final int CEILING_STEP_WINDOWS = 6;
    /** Windows with background I/O and no harm (10 minutes) after which the harmful rate is forgotten. */
    static final int CEILING_MEMORY_WINDOWS = 120;
    /** Windows after a decrease that did not help, or that the floor did not allow, before the next one. */
    static final int COOLDOWN_WINDOWS = 12;
    public record Sample(long nanos, long readCount, long readMicros, long bytes,
                         boolean background, boolean pressure, boolean stopped, boolean valid,
                         boolean foregroundPending, long foregroundCompletions, boolean recoveryClear,
                         boolean urgentPressure, long compactionReadBytes, long compactionReadMicros,
                         long compactionReadCount, long prefetchCount, long prefetchBytes,
                         long bulkCount, long bulkMicros, long bulkKeys, int readWorkers, int readActive,
                         int latencyReadActive, int latencyReadQueued, long readCompletions, boolean bulkPending,
                         long pointTailMicros, long bulkTailMicros, long nativePointCompletions, long nativeBulkCompletions, long nativeBulkKeys, long nativePointReadNanos, long bulkFoundKeys, long bulkReturnedBytes, long readFailures, long readAccepted, long readTerminalOutcomes, long readNonRunOutcomes, int readTopologyWorkers) {
        public Sample(long nanos, long readCount, long readMicros, long bytes,
                         boolean background, boolean pressure, boolean stopped, boolean valid,
                         boolean foregroundPending, long foregroundCompletions, boolean recoveryClear,
                         boolean urgentPressure, long compactionReadBytes, long compactionReadMicros,
                         long compactionReadCount, long prefetchCount, long prefetchBytes,
                         long bulkCount, long bulkMicros, long bulkKeys, int readWorkers, int readActive,
                         int latencyReadActive, int latencyReadQueued, long readCompletions, boolean bulkPending,
                         long pointTailMicros, long bulkTailMicros, long nativePointCompletions, long nativeBulkCompletions, long nativeBulkKeys, long nativePointReadNanos, long bulkFoundKeys, long bulkReturnedBytes, long readFailures, long readAccepted, long readTerminalOutcomes, long readNonRunOutcomes) {
            this(nanos, readCount, readMicros, bytes, background, pressure, stopped, valid,
                    foregroundPending, foregroundCompletions, recoveryClear, urgentPressure,
                    compactionReadBytes, compactionReadMicros, compactionReadCount, prefetchCount, prefetchBytes,
                    bulkCount, bulkMicros, bulkKeys, readWorkers, readActive, latencyReadActive, latencyReadQueued,
                    readCompletions, bulkPending, pointTailMicros, bulkTailMicros, nativePointCompletions,
                    nativeBulkCompletions, nativeBulkKeys, nativePointReadNanos, bulkFoundKeys, bulkReturnedBytes,
                    readFailures, readAccepted, readTerminalOutcomes, readNonRunOutcomes, readWorkers);
        }
        public Sample(long nanos, long readCount, long readMicros, long bytes,
                         boolean background, boolean pressure, boolean stopped, boolean valid,
                         boolean foregroundPending, long foregroundCompletions, boolean recoveryClear,
                         boolean urgentPressure, long compactionReadBytes, long compactionReadMicros,
                         long compactionReadCount, long prefetchCount, long prefetchBytes,
                         long bulkCount, long bulkMicros, long bulkKeys, int readWorkers, int readActive,
                         int latencyReadActive, int latencyReadQueued, long readCompletions, boolean bulkPending,
                         long pointTailMicros, long bulkTailMicros, long nativePointCompletions, long nativeBulkCompletions, long nativeBulkKeys, long nativePointReadNanos, long bulkFoundKeys, long bulkReturnedBytes, long readFailures, long readAccepted) {
            this(nanos, readCount, readMicros, bytes, background, pressure, stopped, valid,
                    foregroundPending, foregroundCompletions, recoveryClear, urgentPressure,
                    compactionReadBytes, compactionReadMicros, compactionReadCount, prefetchCount, prefetchBytes,
                    bulkCount, bulkMicros, bulkKeys, readWorkers, readActive, latencyReadActive, latencyReadQueued,
                    readCompletions, bulkPending, pointTailMicros, bulkTailMicros, nativePointCompletions,
                    nativeBulkCompletions, nativeBulkKeys, nativePointReadNanos, bulkFoundKeys, bulkReturnedBytes,
                    readFailures, readAccepted, -1, -1);
        }
        public Sample(long nanos, long readCount, long readMicros, long bytes,
                         boolean background, boolean pressure, boolean stopped, boolean valid,
                         boolean foregroundPending, long foregroundCompletions, boolean recoveryClear,
                         boolean urgentPressure, long compactionReadBytes, long compactionReadMicros,
                         long compactionReadCount, long prefetchCount, long prefetchBytes,
                         long bulkCount, long bulkMicros, long bulkKeys, int readWorkers, int readActive,
                         int latencyReadActive, int latencyReadQueued, long readCompletions, boolean bulkPending,
                         long pointTailMicros, long bulkTailMicros, long nativePointCompletions, long nativeBulkCompletions, long nativeBulkKeys, long nativePointReadNanos, long bulkFoundKeys, long bulkReturnedBytes, long readFailures) {
            this(nanos, readCount, readMicros, bytes, background, pressure, stopped, valid,
                    foregroundPending, foregroundCompletions, recoveryClear, urgentPressure,
                    compactionReadBytes, compactionReadMicros, compactionReadCount, prefetchCount, prefetchBytes,
                    bulkCount, bulkMicros, bulkKeys, readWorkers, readActive, latencyReadActive, latencyReadQueued,
                    readCompletions, bulkPending, pointTailMicros, bulkTailMicros, nativePointCompletions,
                    nativeBulkCompletions, nativeBulkKeys, nativePointReadNanos, bulkFoundKeys, bulkReturnedBytes,
                    readFailures, -1);
        }
        public Sample(long nanos, long readCount, long readMicros, long bytes,
                         boolean background, boolean pressure, boolean stopped, boolean valid,
                         boolean foregroundPending, long foregroundCompletions, boolean recoveryClear,
                         boolean urgentPressure, long compactionReadBytes, long compactionReadMicros,
                         long compactionReadCount, long prefetchCount, long prefetchBytes,
                         long bulkCount, long bulkMicros, long bulkKeys, int readWorkers, int readActive,
                         int latencyReadActive, int latencyReadQueued, long readCompletions, boolean bulkPending,
                         long pointTailMicros, long bulkTailMicros, long nativePointCompletions, long nativeBulkCompletions, long nativeBulkKeys, long nativePointReadNanos) {
            this(nanos, readCount, readMicros, bytes, background, pressure, stopped, valid,
                    foregroundPending, foregroundCompletions, recoveryClear, urgentPressure,
                    compactionReadBytes, compactionReadMicros, compactionReadCount, prefetchCount, prefetchBytes,
                    bulkCount, bulkMicros, bulkKeys, readWorkers, readActive, latencyReadActive, latencyReadQueued,
                    readCompletions, bulkPending, pointTailMicros, bulkTailMicros, nativePointCompletions,
                    nativeBulkCompletions, nativeBulkKeys, nativePointReadNanos, -1, -1, -1);
        }
        public Sample(long nanos, long readCount, long readMicros, long bytes,
                      boolean background, boolean pressure, boolean stopped, boolean valid,
                      boolean foregroundPending, long foregroundCompletions, boolean recoveryClear,
                      boolean urgentPressure, long compactionReadBytes, long compactionReadMicros,
                      long compactionReadCount, long prefetchCount, long prefetchBytes,
                      long bulkCount, long bulkMicros, long bulkKeys, int readWorkers, int readActive,
                      int latencyReadActive, int latencyReadQueued, long readCompletions, boolean bulkPending,
                      long pointTailMicros, long bulkTailMicros, long nativePointCompletions,
                      long nativeBulkCompletions, long nativeBulkKeys) {
            this(nanos, readCount, readMicros, bytes, background, pressure, stopped, valid,
                    foregroundPending, foregroundCompletions, recoveryClear, urgentPressure,
                    compactionReadBytes, compactionReadMicros, compactionReadCount, prefetchCount, prefetchBytes,
                    bulkCount, bulkMicros, bulkKeys, readWorkers, readActive, latencyReadActive, latencyReadQueued,
                    readCompletions, bulkPending, pointTailMicros, bulkTailMicros, nativePointCompletions,
                    nativeBulkCompletions, nativeBulkKeys, 0);
        }
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
    /**
     * The rate the limiter was constructed with (write buffer / 5: one memtable flushes in about 5 s). It is the
     * flush-safe floor: no state lowers, trials or keeps the budget under it, whatever foreground latency does.
     */
    private final long seed;
    private final long readaheadCeiling;
    private volatile long readaheadBytes;
    private long appliedReadahead, readWindowNanos, compactionBytes, compactionMicros, compactionReads,
            prefetchEvents, prefetchedBytes, settleUntil, probeHoldUntil, lastReadServiceNanos,
            recentReadBytes, recentReadMicros, recentReadCount;
    private double serviceRate, prefetchMean, priorPrefetchMean;
    private double pointTailBaseline, bulkTailBaseline, bulkTailShape, lastBulkTailShape, probeBulkTailShape;
    private long lastPointTail, lastBulkTail, probePointTail, probeBulkTail;
    private long pointTailWindow, bulkTailWindow, nativePoints, nativeBulks, nativeBulkKeys;
    private boolean latencyUnsafe, tailUnsafe, nonTailUnsafe, tailCalibrationNeeded = true;
    private int tailBlockWindows, tailBlocks;
    private long tailOffProbeHoldUntil;
    private long tailBlockPointMax, tailBlockBulkMax, firstPointMax, firstBulkMax;
    private long tailBlockReads, tailBlockPointNanos, tailBlockKeys, tailBlockBulkMicros;
    private double firstPointMean, firstBulkMean, tailBlockShape, firstTailShape;
    private boolean tailBlockPointActive, tailBlockBulkActive;
    private double queueAnchor = -1;
    private volatile double lastPointMean;
    private int pointReferenceWindows, pointReferenceIndex;
    private final double[] pointReferenceMeans = new double[12];
    private double pointReferenceMax, pointReferenceShape;
    private long pointReferenceCap;
    private boolean pointReferenceBulkActive;
    private int bulkReferenceWindows, bulkReferenceIndex;
    private final double[] bulkReferenceMeans = new double[12];
    private double bulkReferenceShape, bulkReferenceNativeShape;
    private long bulkReferenceCap;
    private boolean bulkReferencePointActive;
    private long queuedIntegral, safetyGrowthUntil;
    private double queueReferenceDepth, queueReferenceMix, queueReferenceShape;
    private int queueReferenceWindows;
    private boolean queueReferencePoint, probeQueueProtected;
    private double probeQueueDepth, probeReadCompletionRate, probeNativeBulkCallRate, probeNativeBulkKeyRate,
            probeBulkFoundRate, probeBulkReturnedRate, probeBulkReadMix, probeReadArrivalRate;
    private long bulkFoundKeys, bulkReturnedBytes, readAccepted, readTerminalOutcomes, probeQueueRemovals;
    private boolean bulkContentValid = true;
    private int queueWorkers = -1, queueBadPolls, stableLoadedWindows;
    private long priorReadahead;
    private int reliableGrowthWindows;
    private boolean readWindowTracking = true, foregroundHealthy, readaheadSettling, readaheadUnknown,
            previousReadWindowReliable, prefetchPopulationStable;
    private volatile long budget;
    private volatile long shutdownBudget;
    private volatile State state = State.BOOTSTRAP;
    private Sample previous;
    private long windowNanos, reads, pointNanos, bytes;
    private boolean background, pressuredWindow, pendingForeground, noProgressProbeUsed;
    private long completions;
    private volatile double baseline;
    /** See {@link #followed}: the baseline as the healthy-window average last wrote it, and its lowest level since. */
    private double followedBaseline, baselineAnchor;
    private double recentAchieved, lastAchieved;
    private long preThrottle, probeBudget;
    private double probeLatency, probeAchieved, probeForegroundRate, probePointCallRate;
    private int settle, cooldown, healthyRecovery, probeBenefits;
    private long beforeGrowth;
    /** Consecutive completed TRACKING windows with background I/O and a mean read latency over 1.2x its baseline. */
    private int harmWindows;
    /** Windows until measured harm may lower the budget again (set when a decrease did not help or was at the floor). */
    private int harmCooldown;
    private int hold, ceilingWindows, harmFreeWindows;
    /**
     * The background rate at which foreground harm was last measured (0: none known). While it is known, growth is
     * gentle (see {@link #grown}); it is forgotten after {@link #CEILING_MEMORY_WINDOWS} harm-free windows and when
     * urgent pressure opens the budget.
     */
    private volatile double ceiling;
    /** Whether the reduction under test answers measured harm (else it is a trial from a healthy state). */
    private boolean probeForHarm;
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
            pointReferenceWindows = 0; pointReferenceMax = 0;
            bulkReferenceWindows = 0;
            queueReferenceWindows = 0; probeQueueProtected = false;
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
        pointReferenceWindows = 0; pointReferenceMax = 0;
        bulkReferenceWindows = 0;
        queueReferenceWindows = 0; probeQueueProtected = false;
        clearReadWindow();
    }
    public double bulkBaselineMicrosPerKey() { return bulkBaseline; }
    public double baselineReadMicros() { return baseline; }
    public double pointReadMicros() { double mean = lastPointMean; return mean > 0 ? mean : Double.NaN; }
    public long shutdownBudget() { return shutdownBudget; }
    public State state() { return state; }
    /** The background rate at which foreground harm was last measured, or 0 when none is remembered. */
    public double harmCeilingBytesPerSecond() { return ceiling; }
    private static long bound(double value) {
        return (long) Math.max(MIN_BYTES_PER_SECOND, Math.min(MAX_BYTES_PER_SECOND, value));
    }
    private void clearWindow() {
        windowNanos = reads = pointNanos = bytes = completions = 0;
        background = pressuredWindow = pendingForeground = false;
        bulkCalls = bulkTime = bulkKeys = readSaturatedNanos = readPendingNanos = readQueuedNanos = readCompleted = 0;
        bulkEpochValid = true;
        pointTailWindow = bulkTailWindow = nativePoints = nativeBulks = nativeBulkKeys = 0;
        latencyUnsafe = tailUnsafe = nonTailUnsafe = false;
        queuedIntegral = 0;
        bulkFoundKeys = bulkReturnedBytes = readAccepted = readTerminalOutcomes = 0; bulkContentValid = true;
    }

    public long sample(Sample sample) {
        var old = previous;
        previous = sample;
        boolean pointEpochFailure = sample.valid && old != null && old.valid
                && (sample.nativePointCompletions < 0 || sample.nativePointReadNanos < 0
                    || sample.nativePointCompletions < old.nativePointCompletions
                    || sample.nativePointReadNanos < old.nativePointReadNanos);
        boolean nativePressure = sample.urgentPressure || sample.stopped;
        if (sample.stopped) reduceReadahead(0);
        if (nativePressure && state != State.RECOVERY) {
            // Restore useful measured service once; repeated stalled polls must not ramp.
            boolean tracking = state == State.TRACKING;
            state = State.RECOVERY;
            healthyRecovery = 0;
            beforeGrowth = 0;
            harmWindows = harmCooldown = hold = 0;
            ceiling = 0; // Urgent pressure wins: what harm taught is learned again after it.
            queueReferenceWindows = 0; probeQueueProtected = false;
            double usefulService = Math.max(preThrottle, Math.max(recentAchieved, probeBudget));
            if (usefulService > 0 && tracking) usefulService = Math.max(usefulService, budget);
            budget = bound(Math.max(seed, usefulService)); // Writes are delayed or stopped: never under the flush floor.
            probeBudget = 0;
            clearWindow();
        }
        if (!sample.valid || old == null || !old.valid || sample.nanos <= old.nanos
                || pointEpochFailure || sample.readTopologyWorkers < 0 || sample.bytes < 0 || sample.foregroundCompletions < 0
                || sample.nanos - old.nanos > 3_000_000_000L || sample.bytes < old.bytes
                || sample.foregroundCompletions < old.foregroundCompletions) {
            if (state == State.PROBE) finishProbe(false, 0);
            if (state == State.RECOVERY) healthyRecovery = 0;
            harmWindows = 0;
            invalidateBulk();
            clearWindow();
            clearReadWindow();
            recentReadBytes = recentReadMicros = recentReadCount = 0;
            foregroundHealthy = false;
            if (pointEpochFailure) { baseline = 0; pointTailBaseline = 0; tailCalibrationNeeded = true; }
            if (sample.valid && old != null && old.valid
                    && (sample.nativeBulkCompletions < old.nativeBulkCompletions || sample.nativeBulkCompletions < 0
                        || sample.nativeBulkKeys < old.nativeBulkKeys || sample.nativeBulkKeys < 0)) {
                pointTailBaseline = bulkTailBaseline = 0;
                tailCalibrationNeeded = true; resetTailBlocks();
            }
            lastPointMean = 0;
            queueReferenceWindows = 0; probeQueueProtected = false;
            reduceReadahead(0);
            return publishBudget();
        }
        long elapsed = sample.nanos - old.nanos;
        observeReadService(sample, old, elapsed);
        accumulateBulk(sample, old, elapsed);
        observeForegroundSafety(sample, old);
        if (!bulkEpochValid && state == State.PROBE) finishProbe(false, Double.NaN);
        long completed = sample.nativePointCompletions - old.nativePointCompletions;
        long transferred = sample.bytes - old.bytes;
        pressuredWindow |= nativePressure;
        pendingForeground |= sample.foregroundPending;
        long completedActions = sample.foregroundCompletions - old.foregroundCompletions;
        if (completedActions > 0) noProgressProbeUsed = false;
        completions += Math.max(0, completedActions);
        windowNanos += elapsed;
        reads += completed;
        pointNanos += sample.nativePointReadNanos - old.nativePointReadNanos;
        bytes += transferred;
        background |= sample.background || old.background || transferred > 0;
        if (windowNanos < 5_000_000_000L) return publishBudget();
        double achieved = bytes * (1_000_000_000d / windowNanos);
        double latency = reads >= 32 && pointNanos > 0 ? pointNanos / (reads * 1000d) : Double.NaN;
        double foregroundRate = completions * (1_000_000_000d / windowNanos);
        double pointCallRate = reads * (1_000_000_000d / windowNanos);
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
        boolean growthAllowed = !readGrowthVeto && bulkSafe; // Byte-rate demand remains a separate policy.
        boolean unhealthyMean = pointReliable && baseline > 0 && latency > baseline * 1.2
                || bulkComparable && bulkBaseline > 0 && bulkLatency > bulkBaseline * 1.2;
        if (unhealthyMean || noForegroundProgress || bulkNoProgress) reduceReadahead(0);
        boolean ownPointReliable = !pointTailActive || pointReliable && pointTailInWindow > 0;
        boolean ownBulkReliable = bulkIdle && !bulkTailActive
                || bulkComparable && nativeBulks >= 32 && nativeBulkShapeInWindow > 0 && bulkTailInWindow > 0;
        boolean stationary = !nonTailUnsafe && !unhealthyMean && !noForegroundProgress && !bulkNoProgress
                && ownPointReliable && ownBulkReliable && bulkSafe
                && (queueAnchor < 0 || queueMean <= queueAnchor + Math.max(2, queueAnchor * .2));
        if (stationary) {
            if (queueAnchor < 0) queueAnchor = queueMean; // Provisional until the second stationary window confirms it.
            stableLoadedWindows = Math.min(2, stableLoadedWindows + 1);
            if (stableLoadedWindows >= 2 && baseline == 0 && pointReliable) baseline = latency;
        } else stableLoadedWindows = 0;
        lastPointMean = pointReliable ? latency : 0;
        updateForegroundTails(stationary && !unhealthyMean, pointReliable, pointTailActive, bulkTailActive);
        boolean tailsCalibrated = !tailCalibrationNeeded
                && (!pointTailActive || pointTailBaseline > 0 && nativePoints >= 32)
                && (!bulkTailActive || bulkComparable && bulkTailBaseline > 0 && nativeBulks >= 32);
        // A cold reference may tighten only to the envelope of a full comparable minute, even in Recovery.
        if (state != State.PROBE && stationary && pointReliable && tailsCalibrated && !latencyUnsafe
                && sample.nanos >= safetyGrowthUntil && !readaheadUnknown && !readaheadSettling && readaheadBytes == appliedReadahead) {
            if (pointReferenceWindows > 0 && (pointReferenceCap != appliedReadahead
                    || pointReferenceBulkActive != bulkTailActive
                    || bulkTailActive && !compatibleShape(nativeBulkShapeInWindow, pointReferenceShape))) {
                pointReferenceWindows = 0; pointReferenceMax = 0;
            }
            if (pointReferenceWindows == 0) {
                pointReferenceIndex = 0;
                pointReferenceCap = appliedReadahead;
                pointReferenceShape = nativeBulkShapeInWindow;
                pointReferenceBulkActive = bulkTailActive;
            }
            pointReferenceMeans[pointReferenceIndex] = latency;
            pointReferenceIndex = (pointReferenceIndex + 1) % pointReferenceMeans.length;
            pointReferenceWindows = Math.min(pointReferenceMeans.length, pointReferenceWindows + 1);
            if (pointReferenceWindows == pointReferenceMeans.length) {
                pointReferenceMax = 0;
                for (double mean : pointReferenceMeans) pointReferenceMax = Math.max(pointReferenceMax, mean);
                baseline = Math.min(baseline, pointReferenceMax);
            }
        } else { pointReferenceWindows = 0; pointReferenceMax = 0; }
        if (state != State.PROBE && stationary && bulkComparable && bulkContentValid && ownBulkReliable && tailsCalibrated && !latencyUnsafe
                && sample.nanos >= safetyGrowthUntil && !readaheadUnknown && !readaheadSettling && readaheadBytes == appliedReadahead) {
            if (bulkReferenceWindows > 0 && (bulkReferenceCap != appliedReadahead
                    || bulkReferencePointActive != pointTailActive
                    || !compatibleShape(bulkShape, bulkReferenceShape)
                    || !compatibleShape(nativeBulkShapeInWindow, bulkReferenceNativeShape))) bulkReferenceWindows = 0;
            if (bulkReferenceWindows == 0) {
                bulkReferenceIndex = 0;
                bulkReferenceCap = appliedReadahead;
                bulkReferenceShape = bulkShape;
                bulkReferenceNativeShape = nativeBulkShapeInWindow;
                bulkReferencePointActive = pointTailActive;
            }
            bulkReferenceMeans[bulkReferenceIndex] = bulkLatency;
            bulkReferenceIndex = (bulkReferenceIndex + 1) % bulkReferenceMeans.length;
            bulkReferenceWindows = Math.min(bulkReferenceMeans.length, bulkReferenceWindows + 1);
            if (bulkReferenceWindows == bulkReferenceMeans.length) {
                double maximum = 0;
                for (double mean : bulkReferenceMeans) maximum = Math.max(maximum, mean);
                bulkBaseline = Math.min(bulkBaseline, maximum);
            }
        } else bulkReferenceWindows = 0;
        bulkHealthy = bulkComparable && bulkLatency <= bulkBaseline * 1.2;
        pointHealthy = !pointReliable || baseline > 0 && latency <= baseline * 1.2;
        boolean readaheadGrowthAllowed = stableLoadedWindows >= 2 && tailsCalibrated && !latencyUnsafe
                && sample.nanos >= safetyGrowthUntil;
        boolean unsafeTailWindow = tailUnsafe;
        // Pending task depth is a queue-pressure proxy, not queue duration or RPC success.
        double readCompletionRate = readCompleted * (1_000_000_000d / windowNanos);
        double readArrivalRate = readAccepted * (1_000_000_000d / windowNanos);
        double nativeBulkCallRate = nativeBulks * (1_000_000_000d / windowNanos);
        double nativeBulkKeyRate = nativeBulkKeys * (1_000_000_000d / windowNanos);
        double bulkFoundRate = bulkFoundKeys * (1_000_000_000d / windowNanos);
        double bulkReturnedRate = bulkReturnedBytes * (1_000_000_000d / windowNanos);
        double bulkReadMix = readCompleted > 0 ? (double) nativeBulks / readCompleted : 0;
        boolean queueWorkReliable = bulkContentValid && bulkEpochValid && bulkFoundKeys > 0 && bulkReturnedBytes > 0
                && ownPointReliable && ownBulkReliable && bulkComparable && readCompleted >= 32
                && nativeBulks >= 32 && !latencyUnsafe && !readaheadUnknown && !readaheadSettling;
        if (state != State.PROBE) {
            boolean qualified = state == State.TRACKING && queueWorkReliable && stationary
                    && queueMean >= 2 && readSaturatedNanos >= windowNanos / 2 && readAccepted >= readTerminalOutcomes;
            if (qualified) {
                boolean comparable = queueReferenceWindows > 0 && queueReferencePoint == pointTailActive
                        && compatibleShape(nativeBulkShapeInWindow, queueReferenceShape)
                        && Math.abs(bulkReadMix / queueReferenceMix - 1) <= .05
                        && Math.abs(queueMean - queueReferenceDepth) <= Math.max(2, queueReferenceDepth * .2);
                queueReferenceWindows = comparable ? Math.min(2, queueReferenceWindows + 1) : 1;
                queueReferenceDepth = queueMean; queueReferenceMix = bulkReadMix;
                queueReferenceShape = nativeBulkShapeInWindow; queueReferencePoint = pointTailActive;
                probeQueueDepth = queueMean; probeReadCompletionRate = readCompletionRate;
                probeReadArrivalRate = readArrivalRate;
                probeNativeBulkCallRate = nativeBulkCallRate; probeNativeBulkKeyRate = nativeBulkKeyRate;
                probeBulkFoundRate = bulkFoundRate; probeBulkReturnedRate = bulkReturnedRate;
                probeBulkReadMix = bulkReadMix;
            } else queueReferenceWindows = 0;
        }
        foregroundHealthy = active && (pointReliable || bulkComparable) && pointHealthy
                && readaheadGrowthAllowed;
        clearWindow();
        if (hold > 0) hold--;
        readWindowTracking &= foregroundHealthy;
        evaluateReadService(sample);
        if (state == State.RECOVERY) {
            if (achieved > 0) recentAchieved = recentAchieved == 0 ? achieved : recentAchieved * .5 + achieved * .5;
            if (pressureInWindow) {
                healthyRecovery = 0;
                if (achieved >= budget * .8) budget = bound(Math.max(budget, achieved * 2));
                return publishBudget();
            }
            if (++healthyRecovery >= 2) { state = State.BOOTSTRAP; baseline = 0; }
            return publishBudget();
        }
        lastAchieved = active ? achieved : 0;
        if (state == State.PROBE) {
            if (--settle > 0) return publishBudget();
            boolean pointOk = !probePointProtected || pointReliable && latency <= probeLatency * 1.05
                    && foregroundRate >= probeForegroundRate * .9 && pointCallRate >= probePointCallRate * .9;
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
            boolean queueBenefit = probeQueueProtected && queueWorkReliable
                    && probeQueueDepth - queueMean > Math.max(2, probeQueueDepth * .2) + probeQueueRemovals
                    && readCompletionRate >= probeReadCompletionRate && readArrivalRate >= probeReadArrivalRate
                    && nativeBulkCallRate >= probeNativeBulkCallRate && nativeBulkKeyRate >= probeNativeBulkKeyRate
                    && bulkFoundRate >= probeBulkFoundRate && bulkReturnedRate >= probeBulkReturnedRate
                    && foregroundRate >= probeForegroundRate && pointCallRate >= probePointCallRate
                    && Math.abs(bulkReadMix / probeBulkReadMix - 1) <= .05;
            boolean benefit = !unsafeTailWindow && pointTailOk && bulkTailOk && active && pointOk && bulkOk
                    && ((probeReadSaturated ? bulkBenefit : pointBenefit || bulkBenefit) || queueBenefit)
                    && probeAchieved > 0 && achieved <= probeAchieved * .9;
            probeBulkRebaseAllowed = !benefit && probeBulkProtected && bulkComparable && pointOk && bulkOk
                    && active && probeAchieved > 0 && achieved <= probeAchieved * .9
                    && probeReadPressureClear && readPressureClear;
            if (!benefit) finishProbe(false, latency);
            else if (++probeBenefits >= 2) finishProbe(true, pointReliable ? Math.max(firstProbeBenefitLatency, latency) : Double.NaN);
            else firstProbeBenefitLatency = latency;
            return publishBudget();
        }
        // Harm: slow reads (same definition as everywhere above) in a window in which background I/O ran. Whether
        // that I/O used the whole budget does not matter: it is the rate in use that was too high.
        boolean harm = active && unhealthyMean;
        harmWindows = harm ? harmWindows + 1 : 0;
        if (harmWindows >= HARM_WINDOWS) harmFreeWindows = 0;
        else if (active && ceiling > 0 && ++harmFreeWindows >= CEILING_MEMORY_WINDOWS) ceiling = 0;
        if (!active) {
            if (Double.isFinite(latency) && latency > 0) {
                baseline = baseline == 0 ? latency : baseline * .8 + latency * .2;
            }
            return publishBudget();
        }
        recentAchieved = recentAchieved == 0 ? achieved : recentAchieved * .5 + achieved * .5;
        if (state == State.BOOTSTRAP) {
            // A window the limiter did not saturate measured demand, not service: it must not lower the budget.
            if (achieved >= budget * .8) budget = bound(Math.max(seed, achieved));
            preThrottle = budget;
            state = State.TRACKING;
            if (pointReliable) baseline = latency;
            harmWindows = 0;
            if ((pointReliable || bulkComparable) && bulkSafe) beginProbe(latency, foregroundRate, pointCallRate, false);
            return publishBudget();
        }
        // The once-only no-progress trials wait for a window that saturated the limiter instead of being used up.
        if (bulkNoProgress && !bulkNoProgressProbeUsed && bulkBaseline > 0 && limiterBound()) {
            bulkNoProgressProbeUsed = true;
            beginProbe(latency, foregroundRate, pointCallRate, false);
            return publishBudget();
        }
        // Cache hits/mutations or proven foreground idleness cannot conceal an active slow/sparse bulk lane.
        if (!pointReliable && !bulkComparable) {
            if (growthAllowed && (completedInWindow >= 32 || foregroundIdle) && achieved >= budget * .8) {
                budget = grown(achieved);
            } else if (noForegroundProgress && !noProgressProbeUsed && baseline > 0 && limiterBound()) {
                noProgressProbeUsed = true;
                beginProbe(baseline, foregroundRate, pointCallRate, false);
            }
            return publishBudget();
        }
        if (pointReliable && baseline == 0) baseline = latency;
        long undo = beforeGrowth;
        beforeGrowth = 0;
        boolean reductionAllowed = !rateProbeBlocked() && (bulkIdle || bulkComparable);
        if (harm && undo > 0 && undo < budget) {
            // The window after a doubling had slow reads: take the step back at once (it is the likely cause, and
            // the budget before it was healthy). If the step did raise the background rate, that rate harms.
            if (achieved > undo) rememberHarm(achieved);
            budget = undo;
            hold = HOLD_WINDOWS;
        } else {
            // Slow reads in HARM_WINDOWS consecutive windows: HARM_DECREASE of the achieved rate, kept or undone by
            // the next windows. Healthy reads: a trial from a window that saturated the limiter, when one is due.
            if (harm ? harmWindows >= HARM_WINDOWS && harmCooldown == 0 && reductionAllowed
                        && beginProbe(latency, foregroundRate, pointCallRate, true)
                    : cooldown == 0 && reductionAllowed
                        && beginProbe(latency, foregroundRate, pointCallRate, false)) return publishBudget();
            // No reduction started (a skipped one costs no window): baseline tracking and growth go on.
            if (pointHealthy && growthAllowed) {
                if (pointReliable) baseline = followed(latency);
                if (achieved >= budget * .8) budget = grown(achieved);
            }
        }
        if (cooldown > 0) cooldown--;
        if (harmCooldown > 0) harmCooldown--;
        return publishBudget();
    }
    private void observeForegroundSafety(Sample sample, Sample old) {
        if (sample.pointTailMicros < 0 || sample.bulkTailMicros < 0
                || sample.nativePointCompletions < old.nativePointCompletions || sample.nativePointCompletions < 0
                || sample.nativeBulkCompletions < old.nativeBulkCompletions || sample.nativeBulkCompletions < 0
                || sample.nativeBulkKeys < old.nativeBulkKeys || sample.nativeBulkKeys < 0) {
            reduceReadahead(0);
            pointTailBaseline = bulkTailBaseline = 0;
            queueReferenceWindows = 0; probeQueueProtected = false;
            tailCalibrationNeeded = true; resetTailBlocks();
            latencyUnsafe = tailUnsafe = true;
            return;
        }
        long pointCalls = sample.nativePointCompletions - old.nativePointCompletions;
        long pointTime = sample.nativePointReadNanos - old.nativePointReadNanos;
        double pointMean = pointCalls >= 32 && pointTime > 0 ? pointTime / (pointCalls * 1000d) : Double.NaN;
        boolean historicalPressure = baseline > 0 && pointMean > baseline * 1.2;
        boolean pointActivity = pointCalls > 0 || pointTime > 0 || sample.pointTailMicros > 0;
        boolean probePressure = state == State.PROBE && pointActivity
                && (!probePointProtected || !Double.isFinite(probeLatency) || probeLatency <= 0
                    || pointMean > probeLatency * 1.2);
        boolean alreadyOff = readaheadBytes == 0 && appliedReadahead == 0 && !readaheadUnknown;
        if (historicalPressure) reduceReadahead(0);
        // Optional I/O uses history; the byte trial independently protects its qualified point population.
        if (state == State.PROBE && (probePressure || historicalPressure && !alreadyOff))
            finishProbe(false, Double.NaN);
        boolean pointSlow = sample.pointTailMicros > 0 && pointTailBaseline > 0
                && sample.pointTailMicros > pointTailBaseline * 1.2;
        long calls = sample.nativeBulkCompletions - old.nativeBulkCompletions;
        long keys = sample.nativeBulkKeys - old.nativeBulkKeys;
        boolean bulkShapeMatches = calls > 0 && keys > 0 && compatibleShape((double) keys / calls, bulkTailShape);
        if (calls > 0 && keys > 0 && bulkTailBaseline > 0 && !bulkShapeMatches) {
            bulkTailBaseline = 0;
            tailCalibrationNeeded = true; resetTailBlocks();
            readWindowTracking = false;
            stableLoadedWindows = 0;
        }
        boolean bulkSlow = sample.bulkTailMicros > 0 && bulkTailBaseline > 0 && (bulkShapeMatches || calls == 0 || keys == 0)
                && sample.bulkTailMicros > bulkTailBaseline * 1.2;
        boolean missingOwnReference = pointTailBaseline == 0
                && (sample.pointTailMicros > 0 || sample.nativePointCompletions > old.nativePointCompletions)
                || bulkTailBaseline == 0 && (sample.bulkTailMicros > 0 || calls > 0);
        if (missingOwnReference) {
            if (!tailCalibrationNeeded) resetTailBlocks();
            tailCalibrationNeeded = true;
            if (readaheadBytes > 0 || appliedReadahead > 0) reduceReadahead(0);
        }
        if (queueWorkers < 0) queueWorkers = sample.readTopologyWorkers;
        else if (queueWorkers != sample.readTopologyWorkers) {
            queueWorkers = sample.readTopologyWorkers;
            queueAnchor = -1;
            queueReferenceWindows = 0; probeQueueProtected = false;
            lastPointMean = 0;
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
            if (full && sample.latencyReadQueued > old.latencyReadQueued + deadband) latencyUnsafe = nonTailUnsafe = true;
        }
        boolean noProgress = sample.foregroundPending && sample.foregroundCompletions == old.foregroundCompletions
                && sample.nativePointCompletions == old.nativePointCompletions && calls == 0
                || sample.bulkPending && calls == 0;
        if (noProgress) reduceReadahead(0);
        if (pointSlow || bulkSlow) {
            boolean on = readaheadBytes > 0 || appliedReadahead > 0 || readaheadUnknown;
            if (on) {
                boolean otherUnsafe = nonTailUnsafe;
                reduceReadahead(0);
                nonTailUnsafe = otherUnsafe;
            }
            tailCalibrationNeeded = true;
            latencyUnsafe = tailUnsafe = true;
        }
        nativePoints += sample.nativePointCompletions - old.nativePointCompletions;
        nativeBulks += calls;
        nativeBulkKeys += keys;
        pointTailWindow = Math.max(pointTailWindow, sample.pointTailMicros);
        bulkTailWindow = Math.max(bulkTailWindow, sample.bulkTailMicros);
    }
    private void resetTailBlocks() {
        tailBlockWindows = tailBlocks = 0;
        tailBlockPointMax = tailBlockBulkMax = firstPointMax = firstBulkMax = 0;
        tailBlockReads = tailBlockPointNanos = tailBlockKeys = tailBlockBulkMicros = 0;
        tailBlockShape = firstTailShape = firstPointMean = firstBulkMean = 0;
    }
    private void updateForegroundTails(boolean healthy, boolean pointReliable, boolean pointActive, boolean bulkActive) {
        // Maxima are noisy observations, not quantiles. Learn an envelope only with optional I/O OFF.
        if (readaheadBytes != 0 || appliedReadahead != 0 || readaheadUnknown || readaheadSettling
                || state == State.PROBE || previous.nanos < safetyGrowthUntil || !healthy) {
            resetTailBlocks();
            return;
        }
        if (!tailCalibrationNeeded) return;
        if (tailOffProbeHoldUntil == 0) tailOffProbeHoldUntil = previous.nanos + 60_000_000_000L;
        double shape = nativeBulks > 0 ? (double) nativeBulkKeys / nativeBulks : 0;
        if (tailBlockWindows > 0 && (pointActive != tailBlockPointActive || bulkActive != tailBlockBulkActive
                || bulkActive && !compatibleShape(shape, tailBlockShape))) resetTailBlocks();
        if (tailBlockWindows == 0) {
            tailBlockPointActive = pointActive;
            tailBlockBulkActive = bulkActive;
            tailBlockShape = shape;
        }
        tailBlockPointMax = Math.max(tailBlockPointMax, pointTailWindow);
        tailBlockBulkMax = Math.max(tailBlockBulkMax, bulkTailWindow);
        if (pointReliable) { tailBlockReads += reads; tailBlockPointNanos += pointNanos; }
        tailBlockKeys += bulkKeys; tailBlockBulkMicros += bulkTime;
        if (++tailBlockWindows < 6) return;
        double pointMean = tailBlockReads > 0 ? tailBlockPointNanos / (tailBlockReads * 1000d) : 0;
        double bulkMean = tailBlockKeys > 0 ? (double) tailBlockBulkMicros / tailBlockKeys : 0;
        if (tailBlocks == 0) {
            firstPointMax = tailBlockPointMax; firstBulkMax = tailBlockBulkMax;
            firstPointMean = pointMean; firstBulkMean = bulkMean; firstTailShape = shape;
            tailBlocks = 1;
        } else if ((firstPointMax > 0) == pointActive && (firstBulkMax > 0) == bulkActive
                && (!pointActive || pointMean == 0 || firstPointMean == 0 || pointMean <= firstPointMean * 1.1)
                && (!bulkActive || compatibleShape(shape, firstTailShape) && bulkMean <= firstBulkMean * 1.1)) {
            pointTailBaseline = Math.max(firstPointMax, tailBlockPointMax);
            bulkTailBaseline = Math.max(firstBulkMax, tailBlockBulkMax);
            bulkTailShape = shape;
            tailCalibrationNeeded = false;
            tailOffProbeHoldUntil = 0;
            // The obsolete tail reference alone must not veto this qualified OFF epoch.
            latencyUnsafe = nonTailUnsafe; tailUnsafe = false;
            resetTailBlocks();
            return;
        } else {
            resetTailBlocks();
            return;
        }
        tailBlockWindows = 0;
        tailBlockPointMax = tailBlockBulkMax = tailBlockReads = tailBlockPointNanos = tailBlockKeys = tailBlockBulkMicros = 0;
    }
    private static boolean compatibleShape(double shape, double reference) {
        return reference > 0 && Math.abs(shape / reference - 1) <= .25;
    }
    private void invalidateBulk() {
        pointReferenceWindows = 0; pointReferenceMax = 0;
        bulkReferenceWindows = 0;
        reduceReadahead(0);
        queueReferenceWindows = 0; probeQueueProtected = false;
        bulkBaseline = 0; bulkCalibrationWindows = bulkIdleWindows = readClearWindows = 0;
        bulkComparable = bulkHealthy = false;
        if (bulkSeen) bulkIdle = false;
        readGrowthVeto = true; readPressureClear = false; // Invalid samples cannot clear a pressure latch.
    }
    private void accumulateBulk(Sample sample, Sample old, long elapsed) {
        if (sample.bulkCount < old.bulkCount || sample.bulkMicros < old.bulkMicros || sample.bulkKeys < old.bulkKeys
                || sample.readCompletions < old.readCompletions || sample.bulkCount < 0 || sample.bulkMicros < 0
                || sample.bulkKeys < 0 || sample.readWorkers < 0 || sample.readTopologyWorkers < 0 || sample.readActive < 0
                || sample.latencyReadActive < 0 || sample.latencyReadQueued < 0) {
            invalidateBulk(); bulkEpochValid = false; return;
        }
        bulkCalls += sample.bulkCount - old.bulkCount;
        bulkTime += sample.bulkMicros - old.bulkMicros;
        bulkKeys += sample.bulkKeys - old.bulkKeys;
        readCompleted += sample.readCompletions - old.readCompletions;
        if (sample.bulkFoundKeys < 0 || old.bulkFoundKeys < 0 || sample.bulkReturnedBytes < 0 || old.bulkReturnedBytes < 0
                || sample.bulkFoundKeys < old.bulkFoundKeys || sample.bulkReturnedBytes < old.bulkReturnedBytes
                || sample.readFailures < 0 || old.readFailures < 0 || sample.readFailures != old.readFailures
                || sample.readAccepted < 0 || old.readAccepted < 0 || sample.readAccepted < old.readAccepted
                || sample.readTerminalOutcomes < 0 || old.readTerminalOutcomes < 0
                || sample.readTerminalOutcomes < old.readTerminalOutcomes
                || sample.readNonRunOutcomes < 0 || old.readNonRunOutcomes < 0
                || sample.readNonRunOutcomes < old.readNonRunOutcomes
                || sample.readNonRunOutcomes > sample.readTerminalOutcomes) {
            bulkContentValid = false;
            queueReferenceWindows = 0; probeQueueProtected = false;
        } else {
            readAccepted += sample.readAccepted - old.readAccepted;
            readTerminalOutcomes += sample.readTerminalOutcomes - old.readTerminalOutcomes;
            if (state == State.PROBE) probeQueueRemovals += sample.readNonRunOutcomes - old.readNonRunOutcomes;
            bulkFoundKeys += sample.bulkFoundKeys - old.bulkFoundKeys;
            bulkReturnedBytes += sample.bulkReturnedBytes - old.bulkReturnedBytes;
        }
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
            nonTailUnsafe = true;
            resetTailBlocks();
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
        if (unsafe) latencyUnsafe = nonTailUnsafe = true;
    }
    private void clearReadWindow() {
        readWindowNanos = compactionBytes = compactionMicros = compactionReads = prefetchEvents = prefetchedBytes = 0;
        readWindowTracking = true;
        reliableGrowthWindows = 0;
        previousReadWindowReliable = prefetchPopulationStable = false;
    }
    private boolean rateProbeBlocked() {
        if (!readaheadUnknown && readaheadBytes != appliedReadahead) return true;
        if ((tailBlockWindows > 0 || tailBlocks > 0) && previous.nanos < tailOffProbeHoldUntil) return true;
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
                || old.compactionReadBytes < 0 || old.compactionReadMicros < 0 || old.compactionReadCount < 0
                || sample.prefetchCount < 0 || sample.prefetchBytes < 0) {
            clearReadWindow(); serviceRate = 0;
            recentReadBytes = recentReadMicros = recentReadCount = 0;
            lastReadServiceNanos = 0;
            reduceReadahead(0);
            return;
        }
        long count = sample.compactionReadCount - old.compactionReadCount;
        long transferred = sample.compactionReadBytes - old.compactionReadBytes;
        long readTime = sample.compactionReadMicros - old.compactionReadMicros;
        // Read timings are per I/O; COMPACT_READ_BYTES is published in job batches, independently.
        if (count > 0 && readTime > 0) {
            lastReadServiceNanos = sample.nanos;
            if (readTime / count >= 100_000) reduceReadahead(0);
            reduceReadahead(serviceReadaheadLimit(budget));
        } else if (count > 0 || readTime > 0 || sample.background && (lastReadServiceNanos == 0
                || sample.nanos - lastReadServiceNanos >= 3_000_000_000L)) {
            reduceReadahead(0);
        }
        readWindowNanos += elapsed;
        compactionBytes += transferred;
        compactionMicros += readTime;
        compactionReads += count;
        prefetchEvents += sample.prefetchCount - old.prefetchCount;
        prefetchedBytes += sample.prefetchBytes - old.prefetchBytes;
        if (count > 0 && readTime > 0 && recentReadCount >= 32 && recentReadBytes > 0 && recentReadMicros > 0) {
            double reference = recentReadBytes * (1_000_000d / recentReadMicros);
            double rate = (recentReadBytes + (double) compactionBytes)
                    * (1_000_000d / (recentReadMicros + (double) compactionMicros));
            if (rate <= reference * .75 || readaheadBytes > rate / 10)
                reduceReadahead(serviceReadaheadLimit(Math.min(rate, budget)));
        }
        readWindowTracking &= state != State.PROBE;
    }
    private void evaluateReadService(Sample sample) {
        if (readWindowNanos < 30_000_000_000L) return;
        long events = prefetchEvents;
        double mean = events > 0 ? (double) prefetchedBytes / events : 0;
        double rawRate = compactionMicros > 0 ? compactionBytes * (1_000_000d / compactionMicros) : 0;
        boolean reliable = compactionReads >= 32 && compactionBytes > 0 && rawRate > 0 && Double.isFinite(rawRate);
        boolean tracking = readWindowTracking && foregroundHealthy;
        double priorRate = serviceRate;
        if (reliable) {
            recentReadBytes = compactionBytes;
            recentReadMicros = compactionMicros;
            recentReadCount = compactionReads;
        }
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
        if (target < readaheadBytes && (priorRate > 0 && rawRate <= priorRate * .75
                || readaheadBytes > rawRate / 10)) {
            reduceReadahead(target);
        } else if (tracking && state != State.PROBE && !readaheadSettling && target > readaheadBytes) {
            if (++reliableGrowthWindows >= 2) {
                readaheadBytes = Math.min(target, readaheadBytes == 0 ? 65536 : readaheadBytes * 2);
                reliableGrowthWindows = 0;
            }
        } else reliableGrowthWindows = 0;
    }
    /**
     * Called only for a window that saturated the limiter (achieved >= 80% of budget) with a healthy foreground.
     * Two phases, as slow start and congestion avoidance:
     * <ul>
     * <li>no harmful rate is known: capacity is unknown and the limiter is the bottleneck, so the budget becomes
     * twice the achieved rate (40 KiB/s reaches 200 MB/s in 13 windows, not 90);</li>
     * <li>harm was measured at {@link #ceiling}: +10% of it per window up to 90% of it, then +5% of it every 6th
     * window, never over 1.25x the achieved rate. The rate that harmed is approached quickly, reached again
     * slowly (the device or the workload may have changed since), and passed just as slowly.</li>
     * </ul>
     * Nothing grows in the {@link #HOLD_WINDOWS} windows after a decrease.
     */
    private long grown(double achieved) {
        if (hold > 0) return budget;
        if (ceiling > 0) {
            double step;
            if (budget < ceiling * CEILING_APPROACH) step = Math.min(ceiling * .1, ceiling * CEILING_APPROACH - budget);
            else if (++ceilingWindows >= CEILING_STEP_WINDOWS) step = ceiling * .05;
            else return budget;
            ceilingWindows = 0;
            return bound(Math.max(budget, Math.min(budget + step, achieved * 1.25)));
        }
        long next = bound(Math.max(budget, achieved * 2));
        if (next > budget) beforeGrowth = budget;
        return next;
    }
    private void rememberHarm(double rate) {
        ceiling = rate;
        harmFreeWindows = ceilingWindows = 0;
    }
    private long publishBudget() {
        // A byte-rate probe may block growth, but never a latency-safety reduction.
        reduceReadahead(admissionReadaheadLimit());
        shutdownBudget = bound(Math.max(Math.max(seed, budget),
                Math.max(Math.max(preThrottle, probeBudget), recentAchieved)));
        return budget;
    }
    /**
     * The point baseline after a healthy window with background I/O. The 10% moving average takes the noise of 5 s
     * means out of the reference, but alone it follows any degradation of under 2% per window without end (a
     * window at 1.2x the baseline is healthy and raises it by 2%). So it may fall freely and rise to at most 1.2x
     * its lowest level since the baseline was last set by something that shows the latency is not the limiter's
     * doing: a window without background I/O, the result of a rate trial, a limiter that cannot go lower, a
     * calibration. Reads slower than 1.44x that level are unhealthy until one of those happens again.
     */
    private double followed(double latency) {
        if (baseline != followedBaseline) baselineAnchor = baseline; // Set elsewhere since the last healthy window.
        double next = Math.min(baseline * .9 + latency * .1, baselineAnchor * 1.2);
        baselineAnchor = Math.min(baselineAnchor, next);
        return followedBaseline = next;
    }
    /** Whether the last window's background I/O used at least 80% of the budget: the limiter was its bottleneck. */
    private boolean limiterBound() { return lastAchieved >= budget * .8; }
    private void finishProbe(boolean improved, double latency) {
        if (improved) {
            // The lower rate made reads faster: the rate before it is one at which background I/O harms them.
            rememberHarm(probeAchieved);
            harmCooldown = 0; // If reads are still slow, the next decrease follows without waiting.
            // After measured harm the reference only falls: reads that got 10% faster may still be slow.
            if (Double.isFinite(latency) && latency > 0)
                baseline = probeForHarm && baseline > 0 ? Math.min(baseline, latency) : latency;
            if (probeBulkProtected && bulkComparable)
                bulkBaseline = probeForHarm && bulkBaseline > 0 ? Math.min(bulkBaseline, bulkLatency) : bulkLatency;
        }
        else {
            budget = probeBudget;
            // A decrease that did not make slow reads faster is not repeated at once. A trial from a healthy state
            // that showed nothing says nothing about reads that become slow later: those are answered at once.
            if (probeForHarm) harmCooldown = COOLDOWN_WINDOWS;
            if (Double.isFinite(latency) && latency > 0) baseline = baseline * .5 + latency * .5;
            // A stable media/workset shift may not respond to throttling; only clear, guarded no-benefit trials rebase it.
            if (probeBulkRebaseAllowed) bulkBaseline = bulkBaseline * .5 + bulkLatency * .5;
        }
        probeBulkRebaseAllowed = false;
        probeBudget = 0;
        queueReferenceWindows = 0; probeQueueProtected = false;
        state = State.TRACKING;
        cooldown = COOLDOWN_WINDOWS;
        clearReadWindow();
    }
    /**
     * Lowers the budget and enters PROBE, where the next windows decide: the lower rate is kept if reads got 10%
     * faster in two consecutive windows while the background rate fell by 10%, otherwise the budget is restored.
     * <ul>
     * <li>{@code harm}: the answer to measured foreground harm. The new rate is {@link #HARM_DECREASE} of the
     * achieved rate, whether or not that rate saturated the limiter.</li>
     * <li>otherwise a trial from a healthy state, at 75% of the achieved rate. It says something only while the
     * limiter is the bottleneck of background I/O: a window that used under 80% of the budget starts none and
     * learns nothing.</li>
     * </ul>
     * Neither goes under the flush floor (the seed). A rate that is not at least 10% under the achieved one is
     * not tried, because it could never be kept: the limiter is as low as it may go for this background (which
     * includes every background at or under the flush floor), so the current latency becomes the reference and
     * the next attempt is {@link #COOLDOWN_WINDOWS} windows later.
     * @return whether the budget was lowered
     */
    private boolean beginProbe(double latency, double foregroundRate, double pointCallRate, boolean harm) {
        long candidate = Math.max(seed, bound(Math.min(budget, lastAchieved) * (harm ? HARM_DECREASE : .75)));
        // A healthy rate trial must fit the acknowledged reader cap. Safety cuts run first and remove this floor.
        if (foregroundHealthy) candidate = Math.max(candidate, bound(Math.ceil(readaheadBytes * 12.5)));
        boolean lowest = candidate >= budget || candidate > lastAchieved * .9;
        if (!lowest && !harm && !limiterBound()) return false;
        pointReferenceWindows = 0; pointReferenceMax = 0;
        bulkReferenceWindows = 0;
        tailOffProbeHoldUntil = 0;
        resetTailBlocks();
        if (lowest) {
            if (Double.isFinite(latency) && latency > 0) baseline = latency;
            cooldown = harmCooldown = COOLDOWN_WINDOWS;
            return false;
        }
        beforeGrowth = 0;
        probeForHarm = harm;
        hold = HOLD_WINDOWS;
        probeQueueRemovals = 0;
        probeQueueProtected = queueReferenceWindows >= 2;
        queueReferenceWindows = 0;
        probeBudget = budget;
        probeLatency = latency;
        probeAchieved = lastAchieved;
        probeForegroundRate = foregroundRate;
        probePointCallRate = pointCallRate;
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
        return true;
    }
}
