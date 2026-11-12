package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.CompactionIoBudget;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CompactionIoBudgetTest {
    private static final long SEED = 1_000_000;
    /**
     * The limiter's own seed (the flush-safe floor) of the default trace. No trial and no kept reduction goes under
     * it, so a trace that exercises rate trials starts under its working rate: its first saturated window lifts the
     * budget from FLOOR to SEED, and the trials run between the two. {@code new Trace(SEED)} is a server at its floor.
     */
    private static final long FLOOR = SEED / 4;
    private static final class Trace {
        final CompactionIoBudget policy;
        long nanos, reads, micros, bytes, completions, nativePoints, nativeNanos;
        boolean point = true, timed = true;
        long pointMax;
        Trace() { this(FLOOR); }
        Trace(long seed) { policy = new CompactionIoBudget(seed); poll(0, 0, 0, false, false); }
        long poll(long count, long latency, long transferred, boolean pressure, boolean pending) {
            nanos += 1_000_000_000L; reads += count; micros += count * latency; bytes += transferred;
            if (point) { nativePoints += count; if (timed) nativeNanos += count * latency * 1000; }
            return policy.sample(new CompactionIoBudget.Sample(nanos, reads, micros, bytes,
                    transferred > 0, pressure, pressure, true, pending, completions, !pressure, pressure,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false,
                    point ? Math.max(latency, pointMax) : 0, 0, nativePoints, 0, 0, nativeNanos));
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
    @Test void offProbeUsesCurrentComparablePointReferenceInsteadOfStaleHistoricalMean() {
        for (long currentMean : new long[]{800, 1000}) {
            var t = new Trace(); t.pointMax = 1500;
            t.window(40, 500, SEED);
            t.window(40, 500, 750000); t.window(40, 500, 750000);
            assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
            assertEquals(500, t.policy.baselineReadMicros());
            for (int second = 0; second < 100 && t.policy.state() != CompactionIoBudget.State.PROBE; second++) {
                t.completions += 40; t.poll(40, 1000, SEED, false, true);
            }
            assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
            assertEquals(0, t.policy.readaheadBytes());
            assertEquals(500, t.policy.baselineReadMicros(), "historical mean stays distinct from the new workset's probe reference");
            long candidate = t.policy.budget();
            t.completions += 40; t.poll(40, currentMean, 750000, false, true);
            assertEquals(CompactionIoBudget.State.PROBE, t.policy.state(),
                    "an improved or unchanged owned mean against the probe reference must not abort after one poll");
            assertEquals(candidate, t.policy.budget());
            for (int second = 0; second < 14; second++) {
                t.completions += 40; t.poll(40, currentMean, 750000, false, true);
            }
            assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
            assertEquals(currentMean == 800 ? candidate : SEED, t.policy.budget(),
                    "two beneficial windows may accept; a stable unchanged workset must not fabricate benefit");
            assertEquals(currentMean == 800 ? 800 : 750, t.policy.baselineReadMicros(), .001);
        }
    }
    @Test void offProbeRejectsWorseningOrMissingPointReferenceImmediately() throws Exception {
        for (int scenario = 0; scenario < 4; scenario++) {
            var t = new Trace(); t.pointMax = 1500;
            t.window(40, 500, SEED);
            var reference = CompactionIoBudget.class.getDeclaredField("probeLatency"); reference.setAccessible(true);
            reference.setDouble(t.policy, scenario == 2 ? Double.NaN : scenario == 3 ? 0 : 1000);
            if (scenario == 1) {
                var protectedPoint = CompactionIoBudget.class.getDeclaredField("probePointProtected");
                protectedPoint.setAccessible(true); protectedPoint.setBoolean(t.policy, false);
            }
            t.completions += 40; t.poll(40, scenario == 0 ? 1300 : 800, 750000, false, true);
            assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
            assertEquals(SEED, t.policy.budget());
        }
    }
    @Test void probeRejectsNewOrWorseningPointLaneWithoutAnyHistoricalBreach() throws Exception {
        for (int scenario = 0; scenario < 3; scenario++) {
            var t = new Trace(); t.pointMax = 1500; t.window(40, 500, SEED);
            if (scenario < 2) {
                var protectedPoint = CompactionIoBudget.class.getDeclaredField("probePointProtected");
                protectedPoint.setAccessible(true); protectedPoint.setBoolean(t.policy, false);
                if (scenario == 0) {
                    var historical = CompactionIoBudget.class.getDeclaredField("baseline");
                    historical.setAccessible(true); historical.setDouble(t.policy, 0);
                }
            } else {
                var reference = CompactionIoBudget.class.getDeclaredField("probeLatency");
                reference.setAccessible(true); reference.setDouble(t.policy, 200);
            }
            long count = scenario < 2 ? 1 : 40; // A newly unprotected lane cannot be hidden by low current call volume.
            t.completions += count; t.poll(count, 250, 750000, false, true);
            assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state(),
                    "new or worsened trial activity must fail closed even below a historical500us reference");
            assertEquals(SEED, t.policy.budget());
        }
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
    @Test void aDemandLimitedWindowNeverStartsATrial() {
        // It was probeTestsServiceWhenBudgetExceedsActualConsumption: 100 KB/s of background I/O under a 1 MB/s budget
        // started a trial at 75 KB/s, the rate the next memtable flush was then throttled to.
        var t = new Trace(); t.window(10, 100, SEED);
        t.window(10, 100, SEED); t.window(10, 100, SEED); // restore prior budget
        assertEquals(SEED, t.policy.budget());
        for (int i = 0; i < 40; i++) {
            // Faster, equal and much slower point reads: none says anything about a limiter that limits nothing.
            t.window(10, i % 3 == 0 ? 80 : i % 3 == 1 ? 100 : 300, 100_000);
            assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state(), "window " + i);
            assertEquals(SEED, t.policy.budget(), "window " + i);
        }
        t.window(10, 100, SEED);
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state(), "the limiter is the bottleneck again");
        assertEquals(750_000, t.policy.budget());
    }
    @Test void aTrialNeverGoesUnderTheSeed() {
        var t = new Trace(800_000);
        t.window(10, 100, SEED);
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
        assertEquals(800_000, t.policy.budget(), "75% of the measured rate is under the flush floor: the trial stops at it");
        t.window(10, 80, 800_000); t.window(10, 80, 800_000); t.window(10, 80, 800_000);
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        assertEquals(800_000, t.policy.budget(), "the reduction that helped point reads is kept, at the floor");
        // At the floor no trial can start, however slow the point reads are and whatever the background does.
        for (int i = 0; i < 40; i++) {
            t.window(10, 500, i % 2 == 0 ? 800_000 : 100_000);
            assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state(), "window " + i);
            assertTrue(t.policy.budget() >= 800_000, "window " + i + " budget " + t.policy.budget());
        }
    }
    @Test void foregroundHarmReducesTheBudgetToTheSeedAndNoFurther() {
        var t = new Trace(SEED);
        t.window(10, 100, SEED);
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state(), "no trial at the flush floor");
        assertEquals(SEED, t.policy.budget());
        t.window(10, 100, SEED); t.window(10, 100, 2 * SEED); t.window(10, 100, 4 * SEED);
        assertEquals(8 * SEED, t.policy.budget());
        // Background I/O always moves the whole budget, and every trial makes point reads 20% faster: each one is kept.
        long lowest = Long.MAX_VALUE;
        int kept = 0;
        for (int i = 0; i < 150; i++) {
            boolean trial = t.policy.state() == CompactionIoBudget.State.PROBE;
            long before = t.policy.budget();
            t.window(10, trial ? 160 : 200, before);
            if (trial && t.policy.state() == CompactionIoBudget.State.TRACKING && t.policy.budget() == before) kept++;
            lowest = Math.min(lowest, t.policy.budget());
            assertTrue(t.policy.budget() >= SEED, "window " + i + " budget " + t.policy.budget());
            assertTrue(t.policy.shutdownBudget() >= SEED);
        }
        assertTrue(kept >= 2, "kept trials: " + kept);
        assertEquals(SEED, lowest, "measured harm takes the budget down to the flush floor");
    }
    @Test void aSlowWindowAfterAGrowthStepIsTakenBackEvenWhenATrialIsDue() {
        var t = new Trace(SEED);
        t.window(10, 100, SEED); // BOOTSTRAP at the floor: no trial, 12 windows until the next one is due
        for (int i = 0; i < 11; i++) t.window(10, 100, 100_000);
        assertEquals(SEED, t.policy.budget());
        t.window(10, 100, SEED);
        assertEquals(2 * SEED, t.policy.budget());
        t.window(10, 200, 2 * SEED); // a trial is due in this window; it would start from the harmful budget
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        assertEquals(SEED, t.policy.budget());
        assertEquals(100, t.policy.baselineReadMicros(), 1, "the slow reads of the undone step are not the new baseline");
    }
    @Test void noTraceTakesTheBudgetUnderTheSeed() {
        for (long seed : new long[]{CompactionIoBudget.MIN_BYTES_PER_SECOND, 300_000, SEED, 64 * SEED}) {
            var random = new java.util.Random(seed);
            var t = new Trace(seed);
            long rate = seed, latency = 100;
            boolean pressure = false;
            for (int second = 0; second < 20_000; second++) {
                if (random.nextInt(5) == 0) {
                    rate = switch (random.nextInt(6)) {
                        case 0 -> 0;
                        case 1 -> 100;
                        case 2 -> t.policy.budget() / 4;
                        case 3 -> t.policy.budget() * 3 / 4;
                        case 4 -> t.policy.budget();
                        default -> t.policy.budget() * 2;
                    };
                    latency = 50L << random.nextInt(6);
                    pressure = random.nextInt(40) == 0;
                }
                long count = random.nextInt(4) == 0 ? 0 : 10;
                t.completions += count;
                if (random.nextInt(200) == 0) {
                    t.policy.sample(new CompactionIoBudget.Sample(t.nanos + 1, 0, 0, 0, false, pressure, pressure, false));
                } else t.poll(count, latency, rate, pressure, random.nextBoolean());
                assertTrue(t.policy.budget() >= seed, "seed " + seed + " second " + second + " budget "
                        + t.policy.budget() + " in " + t.policy.state());
                assertTrue(t.policy.shutdownBudget() >= seed);
            }
        }
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
    @Test void aDemandLimitedWindowDoesNotUseUpTheNoProgressTrial() {
        var t = new Trace(); t.window(10, 100, SEED);
        t.window(10, 100, SEED); t.window(10, 100, SEED); // restore failed probe
        for (int i = 0; i < 20; i++) {
            t.poll(0, 0, 100_000, false, true);
            assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
            assertEquals(SEED, t.policy.budget());
        }
        for (int i = 0; i < 5; i++) t.poll(0, 0, SEED, false, true);
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state(), "the first window that saturated the limiter");
        assertEquals(750_000, t.policy.budget());
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
    @Test void tinyFirstActiveWindowDoesNotCollapseTheBudget() {
        // Production 2026-11-12: a few-KB flush right after start set the budget to the 40,960 B/s floor for 12 minutes.
        var t = new Trace(SEED);
        t.window(10, 100, 100);
        assertEquals(SEED, t.policy.budget(), "100 B/s of demand says nothing about the device");
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state(), "a trial at the floor cannot slow 100 B/s");
        t.window(0, 0, 100);
        assertEquals(SEED, t.policy.budget());
        t.window(10, 100, SEED);
        assertTrue(t.policy.budget() >= SEED, "the first real flush runs at the seed or faster");
    }
    @Test void saturatedHealthyWindowsGrowMultiplicativelyFromTheFloor() {
        var t = new Trace(CompactionIoBudget.MIN_BYTES_PER_SECOND);
        t.window(10, 100, CompactionIoBudget.MIN_BYTES_PER_SECOND);
        assertEquals(CompactionIoBudget.MIN_BYTES_PER_SECOND, t.policy.budget());
        for (int window = 0; window < 12; window++) t.window(10, 100, t.policy.budget());
        // 40,960 * 2^12 = 167 MB/s after one minute; +10% per window gave 40,960 * 1.1^12 = 128 KB/s.
        assertTrue(t.policy.budget() >= 150_000_000, "budget " + t.policy.budget());
    }
    @Test void measuredForegroundHarmKeepsTheReductionAndSlowsGrowth() {
        var t = new Trace();
        t.window(10, 100, SEED);
        t.window(10, 80, 750_000); t.window(10, 80, 750_000); t.window(10, 80, 750_000);
        assertEquals(750_000, t.policy.budget(), "throttling measurably helped point reads: the reduction is kept");
        t.window(10, 80, 750_000);
        assertTrue(t.policy.budget() > 750_000 && t.policy.budget() <= 825_000, "budget " + t.policy.budget());
        long budget = t.policy.budget();
        t.window(10, 200, budget);
        assertEquals(budget, t.policy.budget(), "a saturated window with slow point reads must not grow");
    }
    @Test void aDoublingFollowedBySlowPointReadsIsTakenBack() {
        var t = new Trace(); t.window(10, 100, SEED); t.window(10, 100, SEED); t.window(10, 100, SEED);
        assertEquals(SEED, t.policy.budget());
        t.window(10, 100, SEED);
        assertEquals(2 * SEED, t.policy.budget());
        t.window(10, 200, 2 * SEED);
        assertEquals(SEED, t.policy.budget(), "point reads at twice the baseline right after the step");
        t.window(10, 100, SEED);
        assertTrue(t.policy.budget() > SEED && t.policy.budget() <= SEED * 1.1, "budget " + t.policy.budget());
    }
    @Test void healthyGrowthNeedsDemandAndStaysBoundedDespiteJitter() {
        var t = new Trace(); t.window(10, 100, SEED); t.window(10, 100, SEED); t.window(10, 100, SEED);
        long budget = t.policy.budget();
        t.window(10, 105, 100_000);
        assertEquals(budget, t.policy.budget());
        t.window(10, 95, budget);
        // No measured foreground harm: a saturated window doubles at most (it was +10%, 90 windows from the floor).
        assertTrue(t.policy.budget() > budget);
        assertTrue(t.policy.budget() <= budget * 2);
        assertEquals(CompactionIoBudget.MIN_BYTES_PER_SECOND, new CompactionIoBudget(Long.MIN_VALUE).budget());
        assertEquals(CompactionIoBudget.MAX_BYTES_PER_SECOND, new CompactionIoBudget(Long.MAX_VALUE).budget());
    }
    @Test void startupBlockReadsWithoutForegroundCompletionsDoNotCalibrateLatency() {
        var t = new Trace(); t.point = false;
        for (int second = 0; second < 5; second++) t.poll(10, 20, SEED, false, false);
        assertEquals(0, t.policy.baselineReadMicros());
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state(),
                "startup block reads without completed foreground work cannot calibrate foreground latency");
        assertEquals(SEED, t.policy.budget());
    }
    @Test void ownPointCallsCalibrateWithoutUnrelatedGlobalKeyCompletions() {
        var t = new Trace();
        for (int second = 0; second < 5; second++) t.poll(10, 20, SEED, false, false);
        assertEquals(20, t.policy.baselineReadMicros());
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
    }
    @Test void pointProbeCannotAcceptMissingOwnedTimeOrPointProgressHiddenByWrites() {
        for (boolean missingTime : new boolean[]{true, false}) {
            var t = new Trace(); t.window(20, 100, SEED);
            if (missingTime) t.timed = false;
            for (int second = 0; second < 10; second++) {
                t.completions += 20; // Writes keep the global progress rate unchanged.
                t.poll(missingTime ? 20 : 10, 50, 750000, false, true);
            }
            assertEquals(SEED, t.policy.budget(), "only the measured point lane may prove its latency benefit");
            assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        }
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
        idle.point = false;
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
                    true, false, false, true, true, second * 3_000, false, false,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false,
                    6000, 0, second * 100, 0, 0, second * 600_000_000L));
        }
        assertEquals(CompactionIoBudget.State.TRACKING, p.state(),
                "soft debt hysteresis alone must not bypass foreground calibration indefinitely");
        assertEquals(6000, p.baselineReadMicros(), "recovery ended and the point lane is calibrated");
        assertEquals(SEED, p.budget(), "a quarter of the seed is demand, not service: no trial starts under it");
        for (int second = 16; second <= 25; second++) {
            p.sample(new CompactionIoBudget.Sample(second * 1_000_000_000L,
                    second * 100, second * 600_000, 15 * (SEED / 4) + (second - 15) * 187_500,
                    true, false, false, true, true, second * 3_000, false, false,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false,
                    6000, 0, second * 100, 0, 0, second * 600_000_000L));
        }
        assertEquals(SEED, p.budget(), "slow point reads over a background the limiter does not limit lower nothing");
        assertEquals(CompactionIoBudget.State.TRACKING, p.state());
    }
    @Test void urgentEntryRestoresKnownServiceInsteadOfAnUnrelatedStartupSeed() {
        var t = new Trace();
        t.window(10, 100, SEED);
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
        assertEquals(SEED * 3 / 4, t.policy.budget());
        t.policy.sample(new CompactionIoBudget.Sample(t.nanos + 1, 0, 0, 0, false, true, true, false));
        assertEquals(SEED, t.policy.budget(), "the budget in force before the trial: not the trial rate, not the limiter's seed");
        assertEquals(CompactionIoBudget.State.RECOVERY, t.policy.state());
    }

    @Test void softPressureAloneCannotEnterUrgentRecovery() {
        var t = new Trace();
        t.window(0, 0, SEED);
        for (int second = 1; second <= 5; second++) {
            t.policy.sample(new CompactionIoBudget.Sample(t.nanos + second * 1_000_000_000L,
                    second * 100, second * 600_000, t.bytes + second * SEED,
                    true, true, false, true, true, second * 3_000, false, false,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false,
                    6000, 0, second * 100, 0, 0, second * 600_000_000L));
        }
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
    }
    @Test void knownUrgencyOverridesAnIncompleteSnapshotAndInvalidWindowsRestartClearance() {
        var t = new Trace(SEED);
        t.window(10, 100, SEED / 4);
        t.policy.sample(new CompactionIoBudget.Sample(t.nanos + 1, 0, 0, 0,
                false, false, false, false, true, 0, false, true));
        assertEquals(SEED, t.policy.budget());
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

    @Test void urgentReentryBeforeCalibrationStaysAtTheFlushFloor() {
        var p = new CompactionIoBudget(SEED);
        p.sample(new CompactionIoBudget.Sample(0, 0, 0, 0, false, true, true, true));
        for (int second = 1; second <= 15; second++) {
            p.sample(new CompactionIoBudget.Sample(second * 1_000_000_000L, 0, 0, second * (SEED / 4),
                    true, second <= 5, second <= 5, true, false, 0, false));
        }
        assertEquals(CompactionIoBudget.State.BOOTSTRAP, p.state());
        assertEquals(SEED, p.budget());
        p.sample(new CompactionIoBudget.Sample(16_000_000_000L, 0, 0, 4 * SEED, true, true, true, true));
        assertEquals(SEED, p.budget(), "a quarter of the seed was what the background asked for while writes were"
                + " stopped; a stop must not put the limiter under the rate that flushes a memtable in 5 s");
    }

}
