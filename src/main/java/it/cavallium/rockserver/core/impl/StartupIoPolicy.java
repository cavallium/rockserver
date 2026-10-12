package it.cavallium.rockserver.core.impl;

/** Single-publisher policy. Latencies describe completed native calls, not scheduler queue residence. */
public final class StartupIoPolicy {
    public static final int FOREGROUND = 1, URGENT = 2, UNKNOWN = 4;
    public static final long MAX_SAMPLE_AGE_NANOS = 2_000_000_000L;
    public static final int CALIBRATION_WINDOWS = 3, RECOVERY_WINDOWS = 3;
    private final Baseline point = new Baseline(), bulk = new Baseline();
    private InputSample previous;
    private Decision last;
    private boolean recovering;
    private int goodWindows;

    /** complete means scheduler and native windows were read successfully for this epoch.
     * accepted/terminal are cumulative scheduler outcomes, including cancellation before execution.
     * urgentRecovery is shared storage recovery pressure, independent of this group's read demand.
     */
    public record InputSample(long epoch, long sampledNanos, boolean telemetryValid, boolean complete,
                              int workers, int active, int queued, long accepted, long terminal,
                              boolean urgentRecovery, StartupReadCompletions.Snapshot reads) {}
    /** A zero mask permits admission growth only when qualifiedHealthy is also true.
     * Capture uses its own pressure flag so a valid idle group need not fabricate healthy feedback.
     */
    public record Decision(int pressureMask, boolean capturePressure, boolean qualifiedHealthy) {}

    /** Fresh idle recovery windows permit capture again, without qualifying admission growth. */
    public Decision sample(InputSample input, long nowNanos) {
        if (input == null) { recovering = true; return last = unknown(true); }
        // Repeated consumers cannot add a healthy window or drain a tail again.
        if (previous != null && input.epoch() == previous.epoch()) {
            if (input.equals(previous) && fresh(input, nowNanos)) return last;
            recovering = true; goodWindows = 0;
            return last = new Decision(UNKNOWN | FOREGROUND | (input.urgentRecovery() ? URGENT : 0), true, false);
        }
        boolean reset = previous != null && (input.epoch() < previous.epoch()
                || input.sampledNanos() <= previous.sampledNanos()
                || input.accepted() < previous.accepted() || input.terminal() < previous.terminal()
                || input.reads() != null && previous.reads() != null
                   && (input.reads().generation() != previous.reads().generation()
                   || input.reads().pointCompleted() < previous.reads().pointCompleted()
                   || input.reads().bulkCompleted() < previous.reads().bulkCompleted()));
        if (reset) { point.reset(); bulk.reset(); }
        boolean valid = !reset && fresh(input, nowNanos) && valid(input) && conserved(input);
        previous = input;
        if (input.urgentRecovery()) {
            recovering = true; goodWindows = 0;
            return last = new Decision(URGENT | (valid ? 0 : UNKNOWN), true, false);
        }
        if (!valid) { recovering = true; return last = unknown(true); }
        var reads = input.reads();
        boolean busy = input.active() > 0 || input.queued() > 0 || input.accepted() > input.terminal();
        boolean completed = reads.point().count() > 0 || reads.bulk().count() > 0;
        boolean pressure = reads.point().failures() > 0 || reads.bulk().failures() > 0
                || input.queued() > 0
                || point.slow(reads.point()) || bulk.slow(reads.bulk());
        if (pressure) {
            recovering = true; goodWindows = 0;
            return last = new Decision(FOREGROUND, true, false);
        }
        if (!completed) {
            if (!busy && recovering) {
                if (++goodWindows >= RECOVERY_WINDOWS) { recovering = false; goodWindows = 0; }
                return last = new Decision(UNKNOWN | (recovering ? FOREGROUND : 0), recovering, false);
            }
            goodWindows = 0;
            return last = new Decision(UNKNOWN | (busy || recovering ? FOREGROUND : 0),
                    busy || recovering, false);
        }
        point.observe(reads.point()); bulk.observe(reads.bulk());
        boolean calibrated = (reads.point().count() == 0 || point.ready())
                && (reads.bulk().count() == 0 || bulk.ready());
        if (!calibrated) return last = unknown(busy || recovering);
        if (recovering && ++goodWindows < RECOVERY_WINDOWS)
            return last = new Decision(UNKNOWN, true, false);
        recovering = false; goodWindows = 0;
        return last = new Decision(0, false, true);
    }

    private boolean conserved(InputSample input) {
        if (previous == null || input.reads() == null || previous.reads() == null) return true;
        var current = input.reads(); var old = previous.reads();
        return current.pointCompleted() - old.pointCompleted() == current.point().count()
                && current.bulkCompleted() - old.bulkCompleted() == current.bulk().count();
    }

    private Decision unknown(boolean capturePressure) {
        goodWindows = 0;
        return new Decision(UNKNOWN | (capturePressure ? FOREGROUND : 0), capturePressure, false);
    }
    private static boolean fresh(InputSample input, long now) {
        long age = now - input.sampledNanos();
        return age >= 0 && age <= MAX_SAMPLE_AGE_NANOS;
    }
    private static boolean valid(InputSample input) {
        var reads = input.reads();
        return input.epoch() >= 0 && input.telemetryValid() && input.complete() && input.workers() > 0
                && input.active() >= 0 && input.active() <= input.workers() && input.queued() >= 0
                && input.accepted() >= 0 && input.terminal() >= 0 && input.terminal() <= input.accepted()
                && reads != null && reads.epoch() == input.epoch() && reads.generation() >= 0
                && reads.valid() && reads.point() != null && reads.bulk() != null && reads.pointCompleted() >= reads.point().count()
                && reads.bulkCompleted() >= reads.bulk().count()
                && reads.point().valid() && reads.bulk().valid()
                && reads.point().keys() == reads.point().count();
    }
    private static final class Baseline {
        double mean, tail;
        int windows;
        boolean ready() { return windows >= CALIBRATION_WINDOWS; }
        boolean slow(StartupReadCompletions.Cohort cohort) {
            return ready() && cohort.count() > 0
                    && ((double) cohort.nanos() / cohort.count() > mean * 2
                    || cohort.tailNanos() > tail * 4);
        }
        void observe(StartupReadCompletions.Cohort cohort) {
            if (cohort.count() == 0 || cohort.nanos() == 0 || ready()) return;
            double sampleMean = (double) cohort.nanos() / cohort.count();
            // Freeze the calibrated baseline: sustained overload must not normalize itself.
            mean += (sampleMean - mean) / (windows + 1);
            tail = Math.max(tail, cohort.tailNanos());
            if (windows < CALIBRATION_WINDOWS) windows++;
        }
        void reset() { windows = 0; mean = tail = 0; }
    }
}
