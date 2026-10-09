package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.CompactionIoBudget;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CompactionIoBudgetTest {
    private static final long SEED = 1_000_000;
    private static final class Trace {
        final CompactionIoBudget policy = new CompactionIoBudget(SEED);
        long nanos, reads, micros, bytes, completions;
        Trace() { poll(0, 0, 0, false, false); }
        long poll(long count, long latency, long transferred, boolean pressure, boolean pending) {
            nanos += 1_000_000_000L; reads += count; micros += count * latency; bytes += transferred;
            return policy.sample(new CompactionIoBudget.Sample(nanos, reads, micros, bytes,
                    transferred > 0, pressure, pressure, true, pending, completions, !pressure));
        }
        void window(long count, long latency, long transferred) {
            for (int i = 0; i < 5; i++) {
                completions += count;
                poll(count, latency, transferred, false, false);
            }
        }
    }
    @Test void bootstrapLearnsAndProbeKeepsOnlyMeasuredImprovement() {
        var t = new Trace();
        t.window(10, 100, SEED);
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
        assertEquals(750_000, t.policy.budget());
        assertEquals(SEED, t.policy.shutdownBudget());
        t.window(10, 80, 750_000); t.window(10, 80, 750_000);
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state(), "one beneficial window is not sustained evidence");
        t.window(10, 80, 750_000);
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        assertEquals(750_000, t.policy.budget());
    }
    @Test void transientProbeBenefitThenReboundRestoresPriorRateImmediately() {
        var t = new Trace();
        t.window(10, 100, SEED);
        t.window(10, 80, 750_000); // discarded settling window
        t.window(10, 80, 750_000); // first qualifying benefit
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
        assertEquals(100, t.policy.baselineReadMicros(), "one transient benefit cannot lower the baseline");
        t.window(10, 120, 750_000);
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        assertEquals(SEED, t.policy.budget(), "rebound must restore now, not after the cooldown");
    }

    @Test void probeExpiresWhenMissesOrBackgroundWorkDisappear() {
        for (boolean noBackground : new boolean[]{false, true}) {
            var t = new Trace(); t.window(10, 100, SEED);
            t.window(0, 0, noBackground ? 0 : 800_000); t.window(0, 0, noBackground ? 0 : 800_000);
            assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
            assertEquals(1_000_000, t.policy.budget());
        }
    }
    @Test void nativeStopRestoresFiniteServiceWithoutRunawayWhenNoIoCompletes() {
        var t = new Trace(); t.window(10, 100, SEED);
        long restored = t.poll(0, 0, 0, true, false);
        assertEquals(1_000_000, restored);
        for (int i = 0; i < 100; i++) assertEquals(restored, t.poll(0, 0, 0, true, false));
        assertEquals(CompactionIoBudget.State.RECOVERY, t.policy.state());
        t.window(0, 0, 0); t.window(0, 0, 0); t.window(0, 0, 0);
        assertEquals(CompactionIoBudget.State.BOOTSTRAP, t.policy.state());
    }
    @Test void sustainedPressureCanGrowBeyondSeedOnlyWithMeasuredService() {
        var t = new Trace();
        for (int i = 0; i < 100; i++) t.poll(0, 0, t.policy.budget(), true, true);
        assertTrue(t.policy.budget() > SEED * 4);
        assertEquals(CompactionIoBudget.State.RECOVERY, t.policy.state());
    }
    @Test void observedStopOverridesPartialSampleAndRecoveryRequiresUrgentClearance() {
        var p = new CompactionIoBudget(SEED);
        p.sample(new CompactionIoBudget.Sample(1, 0, 0, 0, false, true, true, false));
        assertEquals(CompactionIoBudget.State.RECOVERY, p.state());
        for (int i = 1; i < 20; i++) p.sample(new CompactionIoBudget.Sample(i * 1_000_000_000L,
                0, 0, 0, false, false, false, true, false, 0, false));
        assertEquals(CompactionIoBudget.State.BOOTSTRAP, p.state(),
                "soft hysteresis alone must not retain urgent recovery");
    }
    @Test void probeTestsServiceWhenBudgetExceedsActualConsumption() {
        var t = new Trace(); t.window(10, 100, SEED);
        t.window(10, 100, SEED); t.window(10, 100, SEED); // restore prior budget
        for (int i = 0; i < 13; i++) t.window(10, 100, 100_000);
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
        assertEquals(75_000, t.policy.budget());
        t.window(10, 100, 75_000); t.window(10, 100, 75_000);
        assertEquals(SEED, t.policy.budget());
    }
    @Test void insufficientResetAndStaleCountersHoldAndAbortProbeSafely() {
        var t = new Trace(); t.window(0, 0, SEED);
        long budget = t.policy.budget();
        for (int i = 0; i < 20; i++) assertEquals(budget, t.poll(1, 100, SEED, false, true));
        assertEquals(budget, t.policy.sample(new CompactionIoBudget.Sample(t.nanos + 9_000_000_000L,
                0, 0, 0, true, false, false, true)));
        var probing = new Trace(); probing.window(10, 100, SEED);
        probing.policy.sample(new CompactionIoBudget.Sample(probing.nanos + 1, 0, 0, 0, true, false, false, true));
        assertEquals(1_000_000, probing.policy.budget());
        assertEquals(CompactionIoBudget.State.TRACKING, probing.policy.state());
    }
    @Test void corroboratedNoProgressGetsOnlyOneBoundedProbe() {
        var t = new Trace(); t.window(10, 100, SEED);
        t.window(10, 100, SEED); t.window(10, 100, SEED); // restore failed probe
        for (int i = 0; i < 5; i++) t.poll(0, 0, SEED, false, true);
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
        for (int i = 0; i < 10; i++) t.poll(0, 0, SEED, false, true);
        long restored = t.policy.budget();
        for (int i = 0; i < 60; i++) assertEquals(restored, t.poll(0, 0, SEED, false, true));
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
    }
    @Test void completedCacheHitsOrMutationsPermitDemandGrowthWithoutBlockMisses() {
        var t = new Trace(); t.window(0, 0, SEED);
        long initial = t.policy.budget();
        for (int i = 0; i < 30; i++) {
            t.completions += 10;
            t.poll(0, 0, t.policy.budget(), false, true);
        }
        assertTrue(t.policy.budget() > initial);
    }
    @Test void healthyGrowthNeedsDemandAndStaysBoundedDespiteJitter() {
        var t = new Trace(); t.window(10, 100, SEED); t.window(10, 100, SEED); t.window(10, 100, SEED);
        long budget = t.policy.budget();
        t.window(10, 105, 100_000);
        assertEquals(budget, t.policy.budget());
        t.window(10, 95, budget);
        assertTrue(t.policy.budget() <= budget * 1.1);
        assertTrue(t.policy.budget() > 0);
        assertEquals(CompactionIoBudget.MIN_BYTES_PER_SECOND, new CompactionIoBudget(Long.MIN_VALUE).budget());
        assertEquals(CompactionIoBudget.MAX_BYTES_PER_SECOND, new CompactionIoBudget(Long.MAX_VALUE).budget());
    }
    @Test void startupBlockReadsWithoutForegroundCompletionsDoNotCalibrateLatency() {
        var t = new Trace();
        for (int second = 0; second < 5; second++) t.poll(10, 20, SEED, false, false);
        assertEquals(0, t.policy.baselineReadMicros());
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state(),
                "startup block reads without completed foreground work cannot calibrate foreground latency");
        assertEquals(SEED, t.policy.budget());
    }
    @Test void mediaShiftWithForegroundProgressDoesNotBackOffPermanently() {
        var t = new Trace();
        for (int second = 0; second < 5; second++) t.poll(10, 20, SEED, false, false);
        for (int window = 0; window < 60; window++) {
            for (int second = 0; second < 5; second++) {
                t.completions += 3_000;
                t.poll(100, 3_000 + window % 3 * 3_000, Math.min(SEED, t.policy.budget()), false, true);
            }
        }
        assertTrue(t.policy.budget() >= SEED * .75,
                "latency unresponsive to throttling must not erase observed sustainable background service");
    }

    @Test void highLatencyWithoutProbeBenefitRestoresServiceAndHonorsCooldown() {
        var t = new Trace();
        t.window(10, 100, SEED);
        t.window(10, 100, 750_000); t.window(10, 100, 750_000);
        assertEquals(SEED, t.policy.budget());
        for (int window = 0; window < 12; window++) {
            t.window(10, 6_000, SEED);
            assertEquals(SEED, t.policy.budget(), "high latency must respect failed-probe cooldown");
        }
        t.window(10, 6_000, SEED);
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
        t.window(10, 6_000, 750_000); t.window(10, 6_000, 750_000);
        assertEquals(SEED, t.policy.budget(), "no latency response must restore the pre-probe budget");
    }
    @Test void apparentLatencyBenefitNeedsReducedBackgroundServiceAndForegroundProgress() {
        for (int scenario = 0; scenario < 4; scenario++) {
            var t = new Trace();
            t.window(20, 100, SEED);
            for (int second = 0; second < 10; second++) {
                t.completions += scenario == 2 ? 10 : scenario == 3 ? 0 : 20;
                t.poll(20, 50, scenario == 0 ? SEED : scenario == 1 ? 0 : 750_000, false, true);
            }
            assertEquals(SEED, t.policy.budget(),
                    "unchanged/zero background service or fewer foreground completions cannot justify a cut");
            assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        }
    }
    @Test void missingProbeSampleRestoresServiceAndIdleBlockReadsDoNotLearnBaseline() {
        var t = new Trace();
        t.window(10, 100, SEED);
        t.policy.sample(new CompactionIoBudget.Sample(t.nanos + 1, 0, 0, 0, false, false, false, false));
        assertEquals(SEED, t.policy.budget());
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        var idle = new Trace();
        idle.window(10, 100, SEED);
        idle.window(10, 80, 750_000); idle.window(10, 80, 750_000); idle.window(10, 80, 750_000);
        assertEquals(80, idle.policy.baselineReadMicros());
        for (int second = 0; second < 20; second++) idle.poll(10, 20, SEED, false, false);
        assertEquals(80, idle.policy.baselineReadMicros(), "uncorroborated block I/O must not pollute a calibrated baseline");
    }
    @Test void floorSkipsNoOpProbeAndCanGrowWithHealthyMeasuredService() {
        var t = new Trace();
        t.window(10, 100, CompactionIoBudget.MIN_BYTES_PER_SECOND);
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        assertEquals(100, t.policy.baselineReadMicros());
        for (int window = 0; window < 5; window++) t.window(10, 100, t.policy.budget());
        assertTrue(t.policy.budget() > CompactionIoBudget.MIN_BYTES_PER_SECOND);
    }

    @Test void urgentRecoveryYieldsToForegroundCalibrationInsideSoftDebtCorridor() {
        var p = new CompactionIoBudget(SEED);
        p.sample(new CompactionIoBudget.Sample(0, 0, 0, 0, false, true, true, true));
        for (int second = 1; second <= 15; second++) {
            p.sample(new CompactionIoBudget.Sample(second * 1_000_000_000L,
                    second * 100, second * 600_000, second * (SEED / 4),
                    true, false, false, true, true, second * 3_000, false));
        }
        assertEquals(CompactionIoBudget.State.PROBE, p.state(),
                "soft debt hysteresis alone must not bypass foreground calibration indefinitely");
        assertEquals(SEED / 4 * .75, p.budget());
        for (int second = 16; second <= 25; second++) {
            p.sample(new CompactionIoBudget.Sample(second * 1_000_000_000L,
                    second * 100, second * 600_000, 15 * (SEED / 4) + (second - 15) * 187_500,
                    true, false, false, true, true, second * 3_000, false));
        }
        assertEquals(SEED / 4, p.budget(), "an ineffective latency probe must restore observed useful service");
        assertEquals(CompactionIoBudget.State.TRACKING, p.state());
    }
    @Test void urgentEntryRestoresKnownServiceInsteadOfAnUnrelatedStartupSeed() {
        var t = new Trace();
        t.window(10, 100, SEED / 4);
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
        t.policy.sample(new CompactionIoBudget.Sample(t.nanos + 1, 0, 0, 0, false, true, true, false));
        assertEquals(SEED / 4, t.policy.budget());
        assertEquals(CompactionIoBudget.State.RECOVERY, t.policy.state());
    }

    @Test void softPressureAloneCannotEnterUrgentRecovery() {
        var t = new Trace();
        t.window(0, 0, SEED);
        for (int second = 1; second <= 5; second++) {
            t.policy.sample(new CompactionIoBudget.Sample(t.nanos + second * 1_000_000_000L,
                    second * 100, second * 600_000, t.bytes + second * SEED,
                    true, true, false, true, true, second * 3_000, false, false));
        }
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
    }
    @Test void knownUrgencyOverridesAnIncompleteSnapshotAndInvalidWindowsRestartClearance() {
        var t = new Trace();
        t.window(10, 100, SEED / 4);
        t.policy.sample(new CompactionIoBudget.Sample(t.nanos + 1, 0, 0, 0,
                false, false, false, false, true, 0, false, true));
        assertEquals(SEED / 4, t.policy.budget());
        assertEquals(CompactionIoBudget.State.RECOVERY, t.policy.state());
        var p = new CompactionIoBudget(SEED);
        p.sample(new CompactionIoBudget.Sample(0, 0, 0, 0, false, true, true, true));
        for (int second = 1; second <= 12; second++) {
            p.sample(new CompactionIoBudget.Sample(second * 1_000_000_000L, 0, 0, 0,
                    false, false, false, second != 6, false, 0, false));
        }
        assertEquals(CompactionIoBudget.State.RECOVERY, p.state(), "incomplete samples cannot count toward clearance");
        for (int second = 13; second <= 17; second++) {
            p.sample(new CompactionIoBudget.Sample(second * 1_000_000_000L, 0, 0, 0,
                    false, false, false, true, false, 0, false));
        }
        assertEquals(CompactionIoBudget.State.BOOTSTRAP, p.state());
    }

    @Test void urgentReentryBeforeCalibrationDoesNotReuseAStaleStartupSeed() {
        var p = new CompactionIoBudget(SEED);
        p.sample(new CompactionIoBudget.Sample(0, 0, 0, 0, false, true, true, true));
        for (int second = 1; second <= 15; second++) {
            p.sample(new CompactionIoBudget.Sample(second * 1_000_000_000L, 0, 0, second * (SEED / 4),
                    true, second <= 5, second <= 5, true, false, 0, false));
        }
        assertEquals(CompactionIoBudget.State.BOOTSTRAP, p.state());
        assertEquals(SEED, p.budget());
        p.sample(new CompactionIoBudget.Sample(16_000_000_000L, 0, 0, 4 * SEED, true, true, true, true));
        assertEquals(SEED / 4, p.budget(), "known service must replace the startup seed before recalibration");
    }

}
