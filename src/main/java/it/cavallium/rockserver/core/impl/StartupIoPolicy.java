package it.cavallium.rockserver.core.impl;

/** Single-publisher policy. Latencies describe completed native calls, not scheduler queue residence. */
public final class StartupIoPolicy {
    public static final int FOREGROUND = 1, URGENT = 2, UNKNOWN = 4, IDLE_NEUTRAL = 8;
    public static final long MAX_SAMPLE_AGE_NANOS = 2_000_000_000L;
    public static final int CALIBRATION_WINDOWS = 3, RECOVERY_WINDOWS = 3;
    private final Baseline point = new Baseline(), bulk = new Baseline(), write = new Baseline();
    private InputSample previous;
    private Decision last;
    private boolean recovering;
    private int goodWindows;

    /** complete means scheduler and native windows were read successfully for this epoch.
     * accepted/terminal are cumulative scheduler outcomes, including cancellation before execution.
     * urgentRecovery is shared storage recovery pressure, independent of this group's foreground demand.
     * writeOutstanding covers queued/executing write tasks; writes separately counts exact native attempts.
     */
    public record InputSample(long epoch, long sampledNanos, boolean telemetryValid, boolean complete,
                              int workers, int active, int queued, long accepted, long terminal,
                              boolean urgentRecovery, boolean writeOutstanding, StartupReadCompletions.Snapshot reads, StartupWriteCompletions.Snapshot writes) {}
    /** A zero mask permits admission growth only when qualifiedHealthy is also true.
     * idleNeutral identifies fresh, valid, conserved idle windows after recovery, never completed-but-uncalibrated work.
     * Capture uses its own pressure flag so idle need not fabricate healthy feedback.
     */
    public record Decision(int pressureMask, boolean capturePressure, boolean qualifiedHealthy, boolean idleNeutral) {}

    /** Fresh idle recovery windows permit capture again, without qualifying admission growth. */
    public Decision sample(InputSample input, long nowNanos) { return sample(input, nowNanos, null); }

    Decision sampleRecorded(String database, String workload, long epoch, InputSample input, long nowNanos) {
        var event = StartupIoJfr.policy();
        if (event == null) return sample(input, nowNanos);
        event.database = database; event.workload = workload; event.epoch = epoch; event.inputPresent = input != null;
        if (input != null) {
            event.inputEpoch = input.epoch(); event.sampleAgeNanos = nowNanos - input.sampledNanos();
            event.telemetryValid = input.telemetryValid(); event.complete = input.complete();
            event.fresh = fresh(input, nowNanos); event.workers = input.workers();
            event.active = input.active(); event.queued = input.queued(); event.accepted = input.accepted();
            event.terminal = input.terminal(); event.writeOutstanding = input.writeOutstanding();
            event.urgentRecovery = input.urgentRecovery();
            var reads = input.reads(); var writes = input.writes();
            event.readsPresent = reads != null; event.writesPresent = writes != null;
            event.structurallyValid = valid(input);
            if (reads != null) {
                event.readValid = reads.valid();
                event.readGeneration = reads.generation(); event.pointCompleted = reads.pointCompleted(); event.bulkCompleted = reads.bulkCompleted();
                if (reads.point() != null) { var c = reads.point(); event.pointCount = c.count(); event.pointNanos = c.nanos(); event.pointMaxNanos = c.tailNanos(); event.pointFailures = c.failures(); }
                if (reads.bulk() != null) { var c = reads.bulk(); event.bulkCount = c.count(); event.bulkNanos = c.nanos(); event.bulkMaxNanos = c.tailNanos(); event.bulkFailures = c.failures(); }
            }
            if (writes != null) {
                event.writeValid = writes.valid();
                event.writeGeneration = writes.generation(); event.writeCompleted = writes.completed(); event.activeWrites = writes.active();
                if (writes.calls() != null) { var c = writes.calls(); event.writeCount = c.count(); event.writeNanos = c.nanos(); event.writeMaxNanos = c.tailNanos(); event.writeFailures = c.failures(); }
            }
        }
        var decision = sample(input, nowNanos, event);
        event.recovering = recovering; event.recoveryWindows = goodWindows;
        event.pointReady = point.ready(); event.bulkReady = bulk.ready(); event.writeReady = write.ready();
        event.pointWindows = point.windows; event.bulkWindows = bulk.windows; event.writeWindows = write.windows;
        event.pointMeanNanos = point.mean; event.pointBaselineMaxNanos = point.tail;
        event.bulkMeanNanos = bulk.mean; event.bulkBaselineMaxNanos = bulk.tail;
        event.writeMeanNanos = write.mean; event.writeBaselineMaxNanos = write.tail;
        event.pressureMask = decision.pressureMask(); event.capturePressure = decision.capturePressure();
        event.qualifiedHealthy = decision.qualifiedHealthy(); event.idleNeutral = decision.idleNeutral(); event.commit();
        return decision;
    }

    private static Decision diagnosed(StartupIoJfr.Policy event, String reason, Decision decision) {
        if (event != null) event.reason = reason;
        return decision;
    }

    private Decision sample(InputSample input, long nowNanos, StartupIoJfr.Policy event) {
        if (input == null) { recovering = true; return last = diagnosed(event, "missing_input", unknown(true)); }
        // Repeated consumers cannot add a healthy window or drain a tail again.
        if (previous != null && input.epoch() == previous.epoch()) {
            if (input.equals(previous) && fresh(input, nowNanos)) return diagnosed(event, "repeated_epoch", last);
            recovering = true; goodWindows = 0;
            return last = diagnosed(event, "changed_or_stale_repeated_epoch", new Decision(UNKNOWN | FOREGROUND | (input.urgentRecovery() ? URGENT : 0), true, false, false));
        }
        boolean reset = previous != null && (previous.reads() == null || previous.writes() == null
                || input.epoch() < previous.epoch()
                || input.sampledNanos() <= previous.sampledNanos()
                || input.accepted() < previous.accepted() || input.terminal() < previous.terminal()
                || input.reads() != null && previous.reads() != null
                   && (input.reads().generation() != previous.reads().generation()
                   || input.reads().pointCompleted() < previous.reads().pointCompleted()
                   || input.reads().bulkCompleted() < previous.reads().bulkCompleted())
                || input.writes() != null && previous.writes() != null
                   && (input.writes().generation() != previous.writes().generation()
                   || input.writes().completed() < previous.writes().completed()));
        if (reset) { point.reset(); bulk.reset(); write.reset(); }
        boolean valid = !reset && fresh(input, nowNanos) && valid(input) && conserved(input);
        if (event != null) {
            event.windowEvaluated = true; event.windowValid = valid;
            event.reset = reset; event.structurallyValid = valid(input);
            event.conservationChecked = !reset && fresh(input, nowNanos) && event.structurallyValid;
            event.conserved = event.conservationChecked && conserved(input);
        }
        previous = input;
        if (input.urgentRecovery()) {
            recovering = true; goodWindows = 0;
            return last = diagnosed(event, "urgent_recovery", new Decision(URGENT | (valid ? 0 : UNKNOWN), true, false, false));
        }
        if (!valid) { recovering = true; return last = diagnosed(event, "invalid_window", unknown(true)); }
        var reads = input.reads();
        var writes = input.writes().calls();
        boolean busy = input.writeOutstanding() || input.writes().active() > 0 || input.active() > 0 || input.queued() > 0 || input.accepted() > input.terminal();
        boolean completed = reads.point().count() > 0 || reads.bulk().count() > 0 || writes.count() > 0;
        if (event != null) {
            event.pointSlow = point.slow(reads.point()); event.bulkSlow = bulk.slow(reads.bulk());
            event.writeSlow = write.slow(writes.count(), writes.nanos(), writes.tailNanos());
        }
        boolean pressure = reads.point().failures() > 0 || reads.bulk().failures() > 0 || writes.failures() > 0
                || input.queued() > 0
                || point.slow(reads.point()) || bulk.slow(reads.bulk()) || write.slow(writes.count(), writes.nanos(), writes.tailNanos());
        if (pressure) {
            recovering = true; goodWindows = 0;
            return last = diagnosed(event, "failure_queue_or_slow_cohort", new Decision(FOREGROUND, true, false, false));
        }
        if (!completed) {
            if (!busy && recovering) {
                if (++goodWindows >= RECOVERY_WINDOWS) { recovering = false; goodWindows = 0; }
                return last = diagnosed(event, "idle_recovery", new Decision(UNKNOWN | (recovering ? FOREGROUND : 0), recovering, false, !recovering));
            }
            goodWindows = 0;
            return last = diagnosed(event, "no_completions", new Decision(UNKNOWN | (busy || recovering ? FOREGROUND : 0),
                    busy || recovering, false, !busy && !recovering));
        }
        if (input.writeOutstanding() || input.writes().active() > 0)
            return last = diagnosed(event, "outstanding_write", unknown(true));
        point.observe(reads.point()); bulk.observe(reads.bulk()); write.observe(writes.count(), writes.nanos(), writes.tailNanos());
        boolean calibrated = (reads.point().count() == 0 || point.ready())
                && (reads.bulk().count() == 0 || bulk.ready())
                && (writes.count() == 0 || write.ready());
        if (!calibrated) return last = diagnosed(event, "calibrating", unknown(busy || recovering));
        if (recovering && ++goodWindows < RECOVERY_WINDOWS)
            return last = diagnosed(event, "completed_recovery", new Decision(UNKNOWN, true, false, false));
        recovering = false; goodWindows = 0;
        return last = diagnosed(event, "qualified_healthy", new Decision(0, false, true, false));
    }

    private boolean conserved(InputSample input) {
        if (previous == null) return true;
        if (input.reads() == null || previous.reads() == null || input.writes() == null || previous.writes() == null) return false;
        var current = input.reads(); var old = previous.reads();
        return current.pointCompleted() - old.pointCompleted() == current.point().count()
                && current.bulkCompleted() - old.bulkCompleted() == current.bulk().count()
                && input.writes().completed() - previous.writes().completed() == input.writes().calls().count();
    }

    private Decision unknown(boolean capturePressure) {
        goodWindows = 0;
        return new Decision(UNKNOWN | (capturePressure ? FOREGROUND : 0), capturePressure, false, false);
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
                && reads.point().keys() == reads.point().count()
                && input.writes() != null && input.writes().epoch() == input.epoch() && input.writes().generation() >= 0
                && input.writes().active() >= 0 && input.writes().valid() && input.writes().calls() != null && input.writes().calls().valid()
                && input.writes().completed() >= input.writes().calls().count();
    }
    private static final class Baseline {
        double mean, tail;
        int windows;
        boolean ready() { return windows >= CALIBRATION_WINDOWS; }
        boolean slow(StartupReadCompletions.Cohort cohort) { return slow(cohort.count(), cohort.nanos(), cohort.tailNanos()); }
        boolean slow(long count, long nanos, long tailNanos) {
            return ready() && count > 0 && ((double) nanos / count > mean * 2 || tailNanos > tail * 4);
        }
        void observe(StartupReadCompletions.Cohort cohort) {
            observe(cohort.count(), cohort.nanos(), cohort.tailNanos());
        }
        void observe(long count, long nanos, long tailNanos) {
            if (count == 0 || nanos == 0 || ready()) return;
            double sampleMean = (double) nanos / count;
            // Freeze the calibrated baseline: sustained overload must not normalize itself.
            mean += (sampleMean - mean) / (windows + 1);
            tail = Math.max(tail, tailNanos);
            if (windows < CALIBRATION_WINDOWS) windows++;
        }
        void reset() { windows = 0; mean = tail = 0; }
    }
}
