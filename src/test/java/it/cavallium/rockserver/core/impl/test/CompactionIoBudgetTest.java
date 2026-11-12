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
        /** Compaction debt of every following poll: 0 (none) to 1 (RocksDB starts to delay writes). */
        double debt;
        Trace() { this(FLOOR); }
        Trace(long seed) { policy = new CompactionIoBudget(seed); poll(0, 0, 0, false, false); }
        /** {@code pressure}: urgent pressure that is not a write stall (a column one memtable short of a stop). */
        long poll(long count, long latency, long transferred, boolean pressure, boolean pending) {
            return poll(count, latency, transferred, pressure, false, false, pending);
        }
        /** {@code stopped}, {@code delayed}: what RocksDB reports of its write controller; either is a write stall. */
        long poll(long count, long latency, long transferred, boolean urgent, boolean stopped, boolean delayed, boolean pending) {
            boolean pressure = urgent || stopped || delayed;
            nanos += 1_000_000_000L; reads += count; micros += count * latency; bytes += transferred;
            if (point) { nativePoints += count; if (timed) nativeNanos += count * latency * 1000; }
            return policy.sample(new CompactionIoBudget.Sample(nanos, reads, micros, bytes,
                    transferred > 0, pressure, stopped, true, pending, completions, !pressure, pressure,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false,
                    point ? Math.max(latency, pointMax) : 0, 0, nativePoints, 0, 0, nativeNanos)
                    .withCompactionDebt(debt).withWriteDelayed(delayed));
        }
        /** A poll with writes stopped ({@code delayed}: delayed), reads at {@code latency}, background at {@code transferred}. */
        long stall(boolean delayed, long latency, long transferred) {
            completions += 10;
            return poll(10, latency, transferred, false, !delayed, delayed, true);
        }
        /** A poll without pressure. */
        long clear(long latency, long transferred) {
            completions += 10;
            return poll(10, latency, transferred, false, true);
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
            assertEquals(currentMean == 800 ? 500 : 750, t.policy.baselineReadMicros(), .001,
                    "a kept decrease does not raise the reference (800 is still slow); one that did not help moves it halfway");
            if (currentMean != 800) continue;
            for (int second = 0; second < 5; second++) {
                t.completions += 40; t.poll(40, 800, candidate, false, true);
            }
            assertEquals(CompactionIoBudget.State.PROBE, t.policy.state(), "still slow after a decrease that helped: the next one follows");
            assertEquals((long) (candidate * .7), t.policy.budget());
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
    @Test void urgentPressureRestoresFiniteServiceWithoutRunawayWhenNoIoCompletes() {
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
        assertEquals(CompactionIoBudget.UNLIMITED_BYTES_PER_SECOND,
                p.sample(new CompactionIoBudget.Sample(1, 0, 0, 0, false, true, true, false)));
        assertEquals(CompactionIoBudget.State.UNLIMITED, p.state());
        // 10 s of polls without the stop, then the two windows without urgent pressure that end RECOVERY.
        for (int i = 1; i < 23; i++) p.sample(new CompactionIoBudget.Sample(i * 1_000_000_000L,
                0, 0, 0, false, false, false, true, false, 0, false));
        assertEquals(CompactionIoBudget.State.BOOTSTRAP, p.state(),
                "soft hysteresis alone must not retain urgent recovery");
    }
    @Test void aDemandLimitedWindowWithoutSustainedSlowReadsChangesNothing() {
        // It was probeTestsServiceWhenBudgetExceedsActualConsumption: 100 KB/s of background I/O under a 1 MB/s budget
        // started a trial at 75 KB/s, the rate the next memtable flush was then throttled to. Here the background is
        // 400 KB/s, above the 250 KB/s flush floor, and uses 40% of the budget.
        var t = new Trace(); t.window(10, 100, SEED);
        t.window(10, 100, SEED); t.window(10, 100, SEED); // restore prior budget
        assertEquals(SEED, t.policy.budget());
        for (int i = 0; i < 40; i++) {
            // Faster and equal point reads, and one slow window at a time: no trial (the limiter limits nothing)
            // and no decrease (one slow window is not harm yet).
            t.window(10, i % 3 == 0 ? 80 : i % 3 == 1 ? 100 : 300, 400_000);
            assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state(), "window " + i);
            assertEquals(SEED, t.policy.budget(), "window " + i);
        }
        t.window(10, 100, SEED);
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state(), "the limiter is the bottleneck again, but the"
                + " trial after BOOTSTRAP showed that a lower rate does not make reads faster: none is repeated");
        assertEquals(2 * SEED, t.policy.budget());
    }
    @Test void aTrialFromAHealthyStateIsRepeatedOnlyWhileItHelps() {
        // BOOTSTRAP takes its baseline with background I/O running. Each trial that makes reads 20% faster is kept
        // and followed by the next one 12 windows later; the first that shows nothing is the last.
        var t = new Trace();
        t.window(10, 100, SEED);
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
        t.window(10, 80, 750_000); t.window(10, 80, 750_000); t.window(10, 80, 750_000);
        assertEquals(750_000, t.policy.budget());
        int waited = 0;
        while (t.policy.state() != CompactionIoBudget.State.PROBE && waited < 20) { t.window(10, 80, t.policy.budget()); waited++; }
        assertEquals(13, waited, "the next trial: 12 windows of cooldown, then the first saturated one");
        long before = 950_000; // 750,000 + 10% + 5% + 5% of the 1 MB/s at which the first trial found harm
        assertEquals(before * 3 / 4, t.policy.budget());
        t.window(10, 80, t.policy.budget()); t.window(10, 80, t.policy.budget());
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        assertEquals(before, t.policy.budget(), "no faster reads: restored");
        for (int i = 0; i < 60; i++) {
            long budget = t.policy.budget();
            t.window(10, 80, budget);
            assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state(), "window " + i);
            assertTrue(t.policy.budget() >= budget, "window " + i);
        }
    }
    @Test void slowReadsUnderAHalfUsedBudgetLowerItFromTheAchievedRate() {
        // After a healthy doubling the budget is twice the background rate. Slow reads there started no trial (the
        // limiter was not saturated), so the limiter had become a fixed cap. The harm is in the rate in use.
        var t = new Trace(); t.window(10, 100, SEED);
        t.window(10, 100, SEED); t.window(10, 100, SEED); // restore prior budget
        t.window(10, 100, SEED);
        assertEquals(2 * SEED, t.policy.budget());
        t.window(10, 100, SEED);
        assertEquals(2 * SEED, t.policy.budget(), "half of the budget in use, healthy reads: nothing changes");
        t.window(10, 200, SEED);
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        assertEquals(2 * SEED, t.policy.budget(), "one slow window is not harm yet");
        t.window(10, 200, SEED);
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
        assertEquals(700_000, t.policy.budget(), "70% of the 1 MB/s that background I/O moved, not of the 2 MB/s budget");
        t.window(10, 100, 700_000); t.window(10, 100, 700_000); t.window(10, 100, 700_000);
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        assertEquals(700_000, t.policy.budget(), "reads are fast again at the lower rate: it is kept");
        assertEquals(100, t.policy.baselineReadMicros(), .001);
    }
    @Test void slowerPointReadsOverABackgroundUnderTheFlushFloorBecomeTheBaseline() {
        // The limiter may not go under the seed, so it can do nothing about a background of a tenth of it: without
        // an idle window and without a trial, the slower reads must still become the reference (within 13 windows).
        var t = new Trace(SEED);
        t.window(10, 100, 100_000);
        assertEquals(100, t.policy.baselineReadMicros());
        int accepted = -1;
        for (int i = 0; i < 20 && accepted < 0; i++) {
            t.window(10, 200, 100_000);
            assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
            assertEquals(SEED, t.policy.budget());
            if (200 <= t.policy.baselineReadMicros() * 1.2) accepted = i;
        }
        assertTrue(accepted >= 0 && accepted <= 13, "accepted at window " + accepted);
    }
    /** A background of 40% of the budget, above the flush floor: no trial from a healthy state and no idle window. */
    private static Trace unsaturatedAboveTheFloor() {
        var t = new Trace(); t.window(10, 100, SEED);
        t.window(10, 100, SEED); t.window(10, 100, SEED); // restore prior budget
        t.window(10, 100, 400_000);
        assertEquals(100, t.policy.baselineReadMicros(), .001);
        return t;
    }
    @Test void aRampOfPointLatencyIsNotFollowedAsHealthy() {
        // +1.5% per window was followed without end (a ratio to the baseline of 1.15, always healthy); +15% is the
        // fast case. Both must be over 1.2x the baseline within ln(1.44) / ln(growth) windows.
        for (double growth : new double[]{1.015, 1.15}) {
            var t = unsaturatedAboveTheFloor();
            int bound = (int) Math.ceil(Math.log(1.44) / Math.log(growth)), detected = -1;
            double latency = 100;
            for (int i = 1; i <= 60 && detected < 0; i++) {
                latency *= growth;
                t.window(10, Math.round(latency), 400_000);
                assertEquals(SEED, t.policy.budget());
                assertTrue(t.policy.baselineReadMicros() <= 120.001, "baseline " + t.policy.baselineReadMicros());
                if (Math.round(latency) > t.policy.baselineReadMicros() * 1.2) detected = i;
            }
            assertTrue(detected > 0 && detected <= bound, growth + ": detected at " + detected + ", bound " + bound);
            t.window(10, Math.round(latency * growth), 400_000);
            assertEquals(CompactionIoBudget.State.PROBE, t.policy.state(), "the second slow window is answered");
            assertEquals(280_000, t.policy.budget(), "with 70% of the background rate, which used 40% of the budget");
        }
    }
    @Test void aWorksetShiftIsAcceptedInWindowsWithoutBackgroundIo() {
        var t = unsaturatedAboveTheFloor();
        t.window(10, 200, 400_000);
        assertTrue(t.policy.baselineReadMicros() <= 120.001, "twice the latency while background I/O runs is not accepted");
        int accepted = -1;
        for (int i = 1; i <= 10 && accepted < 0; i++) {
            t.window(10, 200, 0);
            if (200 <= t.policy.baselineReadMicros() * 1.2) accepted = i;
        }
        assertTrue(accepted > 0 && accepted <= 6, "accepted at idle window " + accepted);
        double reference = t.policy.baselineReadMicros();
        for (int i = 0; i < 30; i++) {
            t.window(10, 200, 400_000);
            assertTrue(200 <= t.policy.baselineReadMicros() * 1.2, "the accepted shift stays healthy with background I/O");
            assertEquals(SEED, t.policy.budget());
        }
        assertTrue(t.policy.baselineReadMicros() > reference, "and the average goes on from the accepted level");
    }
    @Test void aWorksetShiftIsAcceptedAfterDecreasesThatDidNotHelp() {
        // Background I/O never stops and reads are twice as slow whatever its rate. Each decrease is undone by the
        // window that shows no faster reads and moves the reference halfway; the next one is 12 windows later.
        var t = unsaturatedAboveTheFloor();
        int accepted = -1, decreases = 0;
        for (int i = 0; i < 60 && accepted < 0; i++) {
            boolean tracking = t.policy.state() == CompactionIoBudget.State.TRACKING;
            t.window(10, 200, Math.min(400_000, t.policy.budget()));
            if (t.policy.state() == CompactionIoBudget.State.PROBE) {
                if (tracking) decreases++;
                assertEquals(280_000, t.policy.budget());
            } else {
                assertEquals(SEED, t.policy.budget(), "window " + i);
                if (200 <= t.policy.baselineReadMicros() * 1.2) accepted = i;
            }
        }
        assertEquals(2, decreases);
        assertTrue(accepted > 0 && accepted <= 20, "accepted at window " + accepted);
    }
    @Test void noisyPointMeansAreNotTakenForDegradation() {
        var t = unsaturatedAboveTheFloor();
        long[] noise = {100, 115, 88, 110, 85, 118, 92, 105};
        for (int i = 0; i < 400; i++) {
            long latency = noise[i % noise.length];
            t.window(10, latency, 400_000);
            assertTrue(latency <= t.policy.baselineReadMicros() * 1.2,
                    "window " + i + ": " + latency + " over baseline " + t.policy.baselineReadMicros());
        }
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
            boolean pressure = false, raised = false, unlimited = false;
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
                    t.debt = random.nextInt(3) == 0 ? random.nextDouble() * 1.2 : 0;
                }
                long count = random.nextInt(4) == 0 ? 0 : 10;
                t.completions += count;
                long applied;
                if (random.nextInt(200) == 0) {
                    applied = t.policy.sample(new CompactionIoBudget.Sample(t.nanos + 1, 0, 0, 0, false, pressure, pressure, false));
                } else applied = t.poll(count, latency, rate, pressure, pressure && second % 3 == 0,
                        pressure && second % 3 == 1, random.nextBoolean());
                assertTrue(t.policy.budget() >= seed, "seed " + seed + " second " + second + " budget "
                        + t.policy.budget() + " in " + t.policy.state());
                // What goes to the limiter: the budget or the unlimited rate, so never under the seed and never 0.
                assertEquals(t.policy.state() == CompactionIoBudget.State.UNLIMITED
                        ? CompactionIoBudget.UNLIMITED_BYTES_PER_SECOND : t.policy.budget(), applied);
                unlimited |= applied == CompactionIoBudget.UNLIMITED_BYTES_PER_SECOND;
                assertTrue(t.policy.shutdownBudget() >= seed);
                // The floor that compaction debt raises: never under the seed, and the budget never under it.
                long floor = t.policy.floorBytesPerSecond();
                assertTrue(floor >= seed && t.policy.budget() >= floor, "seed " + seed + " second " + second + " budget "
                        + t.policy.budget() + " under the floor " + floor + " in " + t.policy.state());
                raised |= floor > seed;
            }
            assertTrue(raised, "the trace must raise the floor above the seed");
            assertTrue(unlimited, "the trace must stall writes");
        }
    }
    @Test void urgentPressureAlwaysWins() {
        // Whatever the controller was doing (growing, testing a reduction after slow reads, waiting after RECOVERY):
        // a poll under urgent pressure puts it in RECOVERY with a budget not under the one in force, and nothing
        // lowers the budget while the pressure lasts. When the pressure is a write stall (stopped or delayed
        // writes) the limiter is unlimited in that poll, and the same holds for the budget kept for afterwards.
        for (long seed : new long[]{CompactionIoBudget.MIN_BYTES_PER_SECOND, 300_000, SEED, 64 * SEED}) {
            var random = new java.util.Random(seed + 1);
            var t = new Trace(seed);
            long rate = seed, latency = 100;
            boolean pressure = false;
            int entries = 0, fromReduced = 0, kind = 0, stalls = 0;
            for (int second = 0; second < 40_000; second++) {
                if (random.nextInt(5) == 0) {
                    rate = switch (random.nextInt(5)) {
                        case 0 -> 0;
                        case 1 -> t.policy.budget() / 3;
                        case 2 -> t.policy.budget() * 9 / 10;
                        default -> t.policy.budget();
                    };
                    latency = random.nextInt(3) == 0 ? 400 : 100;
                    if (random.nextInt(pressure ? 3 : 60) == 0) { pressure = !pressure; kind = random.nextInt(3); }
                }
                boolean stopped = pressure && kind == 1, delayed = pressure && kind == 2;
                long before = t.policy.budget(), applied;
                var stateBefore = t.policy.state();
                t.completions += 10;
                if (random.nextInt(100) == 0) {
                    applied = t.policy.sample(new CompactionIoBudget.Sample(t.nanos + 1, 0, 0, 0, false, pressure, stopped, false)
                            .withWriteDelayed(delayed));
                } else applied = t.poll(10, latency, rate, pressure, stopped, delayed, true);
                String at = "seed " + seed + " second " + second + " from " + stateBefore;
                var state = t.policy.state();
                assertEquals(state == CompactionIoBudget.State.UNLIMITED ? CompactionIoBudget.UNLIMITED_BYTES_PER_SECOND
                        : t.policy.budget(), applied, at);
                // The hold after a stall ends in RECOVERY, with or without urgent pressure, and nowhere else.
                if (stateBefore == CompactionIoBudget.State.UNLIMITED)
                    assertTrue(state == CompactionIoBudget.State.UNLIMITED || state == CompactionIoBudget.State.RECOVERY, at);
                if (!pressure) continue;
                if (stopped || delayed) assertEquals(CompactionIoBudget.State.UNLIMITED, state, at);
                else if (stateBefore != CompactionIoBudget.State.UNLIMITED) assertEquals(CompactionIoBudget.State.RECOVERY, state, at);
                assertTrue(t.policy.budget() >= before, at + ": " + before + " -> " + t.policy.budget());
                if (stateBefore != CompactionIoBudget.State.RECOVERY && stateBefore != CompactionIoBudget.State.UNLIMITED) entries++;
                if (stateBefore == CompactionIoBudget.State.PROBE) fromReduced++;
                if (state == CompactionIoBudget.State.UNLIMITED && stateBefore != state) stalls++;
            }
            assertTrue(entries > 50 && fromReduced > 0 && stalls > 50,
                    "entries " + entries + ", from a reduced budget " + fromReduced + ", stalls " + stalls);
        }
    }
    private static final long UNLIMITED = CompactionIoBudget.UNLIMITED_BYTES_PER_SECOND;
    /** TRACKING at 16 MB/s after five saturated healthy windows, with a background that then uses a quarter of it. */
    private static Trace trackingAtSixteen() {
        var t = new Trace(SEED);
        for (int i = 0; i < 5; i++) t.window(10, 100, t.policy.budget());
        for (int i = 0; i < 3; i++) t.window(10, 100, 4 * SEED);
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        assertEquals(16 * SEED, t.policy.budget());
        assertEquals(100, t.policy.baselineReadMicros(), 1e-9);
        return t;
    }
    @Test void aStopOrADelayAtAnyPollOfAWindowOpensTheLimiterInThatPoll() {
        for (boolean delayed : new boolean[]{false, true}) {
            for (int offset = 0; offset < 5; offset++) {
                var t = trackingAtSixteen();
                for (int i = 0; i < offset; i++) assertEquals(16 * SEED, t.clear(100, 4 * SEED));
                String at = (delayed ? "delayed" : "stopped") + " at poll " + offset + " of the window";
                assertEquals(UNLIMITED, t.stall(delayed, 100, 4 * SEED), at);
                assertEquals(CompactionIoBudget.State.UNLIMITED, t.policy.state(), at);
                assertEquals(UNLIMITED, t.policy.rateBytesPerSecond(), at);
                assertEquals(16 * SEED, t.policy.budget(), "the budget is kept for afterwards");
            }
        }
        // The first poll of a database, with a sample that is otherwise unusable.
        for (boolean delayed : new boolean[]{false, true}) {
            var p = new CompactionIoBudget(SEED);
            assertEquals(UNLIMITED, p.sample(new CompactionIoBudget.Sample(1, 0, 0, 0, false, true, !delayed, false)
                    .withWriteDelayed(delayed)));
            assertEquals(SEED, p.budget());
        }
        // Not for urgent pressure that is not a stall (with 2 write buffers: every flush in progress), which
        // keeps the law of RECOVERY, nor for pending compaction bytes at the soft limit alone.
        var t = trackingAtSixteen();
        assertEquals(16 * SEED, t.poll(10, 100, 4 * SEED, true, true));
        assertEquals(CompactionIoBudget.State.RECOVERY, t.policy.state());
        assertTrue(UNLIMITED > 0 && UNLIMITED <= Long.MAX_VALUE / 1_000_000,
                "rate x refill period in microseconds must fit 64 bits for every period up to 1 s");
    }
    @Test void theLimiterStaysOpenWhileTheStallLastsAndForTenSecondsOfClearPollsAfterIt() {
        for (boolean delayed : new boolean[]{false, true}) {
            var t = trackingAtSixteen();
            // Unlimited, the background moves 60 MB/s and reads are 20x slower: neither is evidence of anything.
            for (int i = 0; i < 37; i++) {
                assertEquals(UNLIMITED, t.stall(delayed, 2000, 60 * SEED), "stalled poll " + i);
                assertEquals(16 * SEED, t.policy.budget());
            }
            for (int i = 1; i <= 10; i++) {
                assertEquals(UNLIMITED, t.clear(2000, 60 * SEED), "clear poll " + i + " of the hold");
                assertEquals(CompactionIoBudget.State.UNLIMITED, t.policy.state());
            }
            assertEquals(16 * SEED, t.clear(2000, 60 * SEED), "10 s between two polls without a stall: the budget returns");
            assertEquals(CompactionIoBudget.State.RECOVERY, t.policy.state());
        }
    }
    @Test void afterAStallTheLimiterReturnsToTheBudgetInForceBeforeIt() {
        // From TRACKING above the last measured service: that budget, not the floor, not the 60 MB/s of the burst.
        var t = trackingAtSixteen();
        for (int i = 0; i < 20; i++) t.stall(i % 2 == 0, 2000, 60 * SEED);
        long returned = UNLIMITED;
        for (int i = 0; i < 11; i++) returned = t.clear(2000, 60 * SEED);
        assertEquals(16 * SEED, returned);
        assertEquals(SEED, t.policy.floorBytesPerSecond());
        // From a reduction under test: the budget before the reduction (what RECOVERY restores), not the reduced one.
        var reduced = new Trace();
        reduced.window(10, 100, SEED);
        assertEquals(CompactionIoBudget.State.PROBE, reduced.policy.state());
        assertEquals(SEED * 3 / 4, reduced.policy.budget());
        assertEquals(UNLIMITED, reduced.stall(true, 100, SEED));
        assertEquals(SEED, reduced.policy.budget());
        for (int i = 0; i < 10; i++) assertEquals(UNLIMITED, reduced.clear(100, SEED));
        assertEquals(SEED, reduced.clear(100, SEED));
        // From the seed, before anything was measured: the seed.
        var fresh = new Trace(SEED);
        assertEquals(UNLIMITED, fresh.stall(false, 100, 60 * SEED));
        for (int i = 0; i < 10; i++) assertEquals(UNLIMITED, fresh.clear(100, 60 * SEED));
        assertEquals(SEED, fresh.clear(100, 60 * SEED));
        // From RECOVERY that doubled under urgent pressure with a saturated limiter: what it had reached.
        var urgent = new Trace(SEED);
        for (int i = 0; i < 12; i++) urgent.poll(0, 0, urgent.policy.budget(), true, true);
        long reached = urgent.policy.budget();
        assertTrue(reached >= 4 * SEED, "budget " + reached);
        assertEquals(UNLIMITED, urgent.stall(false, 100, 60 * SEED));
        for (int i = 0; i < 10; i++) assertEquals(UNLIMITED, urgent.clear(100, 60 * SEED));
        assertEquals(reached, urgent.clear(100, 60 * SEED));
    }
    @Test void nothingIsLearnedWhileTheLimiterIsUnlimitedNorFromTheReadsRightAfter() {
        var t = trackingAtSixteen();
        t.debt = 1; // The floor at full debt is the last known background service: it shows what is remembered of it.
        t.clear(100, 4 * SEED);
        long budget = t.policy.budget(), known = t.policy.floorBytesPerSecond(), shutdown = t.policy.shutdownBudget();
        assertTrue(known > SEED && known <= budget, "known service " + known);
        // 30 s stalled and the 10 s of the hold: every window moves 60 MB/s (over 80% of any budget here, so each
        // would count as saturated) with reads 20x slower than the baseline.
        for (int i = 0; i < 41; i++) {
            if (i == 30) t.debt = 0;
            long applied = i < 30 ? t.stall(i % 3 == 0, 2000, 60 * SEED) : t.clear(2000, 60 * SEED);
            String at = "poll " + i;
            if (i < 40) assertEquals(UNLIMITED, applied, at); else assertEquals(budget, applied, at);
            assertEquals(budget, t.policy.budget(), at + ": no capacity from an unlimited window");
            if (i < 30) assertEquals(known, t.policy.floorBytesPerSecond(), at + ": no service from an unlimited window");
            assertEquals(shutdown, t.policy.shutdownBudget(), at);
            assertEquals(100, t.policy.baselineReadMicros(), 1e-9, at + ": no baseline from an unlimited window");
            assertEquals(0, t.policy.harmCeilingBytesPerSecond(), at + ": no harmful rate from an unlimited window");
            assertTrue(Double.isNaN(t.policy.pointReadMicros()), at + ": no window was completed");
        }
        assertEquals(CompactionIoBudget.State.RECOVERY, t.policy.state());
        // Right after: the limiter is back, reads are still 3x the old baseline while the backlog drains. RECOVERY
        // compares nothing for two windows, then BOOTSTRAP takes its baseline from the reads as they are.
        for (int window = 0; window < 2; window++) {
            t.window(10, 300, 4 * SEED);
            assertEquals(budget, t.policy.budget());
        }
        assertEquals(CompactionIoBudget.State.BOOTSTRAP, t.policy.state());
        assertEquals(0, t.policy.baselineReadMicros(), "the baseline from before the stall is not used again");
        t.window(10, 300, 4 * SEED);
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        assertEquals(300, t.policy.baselineReadMicros(), 1e-9);
        for (int window = 0; window < 6; window++) {
            t.window(10, 300 - 30 * window, 4 * SEED);
            assertEquals(budget, t.policy.budget(), "slow reads after a stall are not harm: window " + window);
            assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        }
        assertEquals(0, t.policy.harmCeilingBytesPerSecond());
        // The service known afterwards is the budget BOOTSTRAP started from, not the 60 MB/s of the burst.
        t.debt = 1;
        t.clear(150, 4 * SEED);
        assertEquals(budget, t.policy.floorBytesPerSecond());
        assertEquals(budget, t.policy.budget());
    }
    @Test void aStallThatComesAndGoesDoesNotMoveTheLimiterBackAndForth() {
        // Every other poll, then with gaps of up to 10 clear polls: one opening, no closing.
        var t = trackingAtSixteen();
        for (int i = 0; i < 60; i++) {
            long applied = i % 2 == 0 ? t.stall(i % 4 == 0, 2000, 60 * SEED) : t.clear(2000, 60 * SEED);
            assertEquals(UNLIMITED, applied, "poll " + i);
            assertEquals(16 * SEED, t.policy.budget());
        }
        for (int gap = 1; gap <= 10; gap++) {
            for (int i = 0; i < gap; i++) assertEquals(UNLIMITED, t.clear(2000, 60 * SEED), "gap " + gap + " poll " + i);
            assertEquals(UNLIMITED, t.stall(true, 2000, 60 * SEED));
        }
        // Random episodes: the limiter's rate changes twice per stall that is followed by 11 clear polls, never more.
        var random = new java.util.Random(7);
        long last = UNLIMITED;
        int changes = 0, closings = 0, clear = 0;
        boolean stalled = true;
        for (int i = 0; i < 20_000; i++) {
            if (random.nextInt(stalled ? 2 : 9) == 0) stalled = !stalled;
            boolean open = last == UNLIMITED;
            long applied = stalled ? t.stall(random.nextBoolean(), 2000, 60 * SEED)
                    : t.clear(100, open ? 60 * SEED : Math.min(4 * SEED, t.policy.budget()));
            clear = stalled ? 0 : clear + 1;
            if (stalled) assertEquals(UNLIMITED, applied, "poll " + i);
            if ((applied == UNLIMITED) != open) {
                changes++;
                if (open) { closings++; assertEquals(11, clear, "poll " + i + ": closed after 10 s without a stall"); }
            }
            if (applied != UNLIMITED) assertTrue(clear >= 11, "poll " + i);
            last = applied;
        }
        assertTrue(closings > 100 && changes <= 2 * closings + 1, "changes " + changes + ", closings " + closings);
    }
    @Test void anUnusablePollDoesNotCloseAnOpenLimiterBeforeTheHold() {
        var t = trackingAtSixteen();
        assertEquals(UNLIMITED, t.stall(false, 100, 4 * SEED));
        // A sample that could not be completed says nothing of a stall; one whose time does not advance neither.
        for (int i = 1; i <= 10; i++) {
            t.nanos += 1_000_000_000L;
            assertEquals(UNLIMITED, t.policy.sample(new CompactionIoBudget.Sample(t.nanos, 0, 0, 0, false, false, false, false)));
            assertEquals(UNLIMITED, t.policy.sample(new CompactionIoBudget.Sample(t.nanos, 0, 0, 0, false, false, false, false)));
        }
        t.nanos += 1_000_000_000L;
        assertEquals(16 * SEED, t.policy.sample(new CompactionIoBudget.Sample(t.nanos, 0, 0, 0, false, false, false, false)));
        assertEquals(CompactionIoBudget.State.RECOVERY, t.policy.state());
    }
    /**
     * Background I/O at 8 MB/s under a 16 MB/s budget; reads are slow while it moves over 3 MB/s, and three
     * decreases (5.6, 3.92, 2.744 MB/s) are kept. Ends in TRACKING with healthy reads.
     */
    private static Trace reducedByHarm() {
        var t = new Trace(SEED);
        t.window(10, 100, SEED);
        for (int i = 0; i < 4; i++) t.window(10, 100, t.policy.budget());
        assertEquals(16 * SEED, t.policy.budget());
        t.window(10, 100, 8 * SEED);
        for (int i = 0; i < 13; i++) {
            long rate = Math.min(8 * SEED, t.policy.budget());
            t.window(10, rate <= 3 * SEED ? 100 : t.policy.state() == CompactionIoBudget.State.PROBE ? 150 : 200, rate);
        }
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        assertEquals(2_744_000, t.policy.budget(), 2); // 8 MB/s x 0.7^3, in whole bytes
        return t;
    }
    @Test void risingCompactionDebtRaisesTheFloorTowardsTheLastKnownService() {
        var t = reducedByHarm();
        long reduced = t.policy.budget(), last = reduced, known = 0;
        assertEquals(SEED, t.policy.floorBytesPerSecond());
        for (int percent = 0; percent <= 100; percent += 5) {
            t.debt = percent / 100d;
            t.poll(10, 100, SEED, false, true);
            long floor = t.policy.floorBytesPerSecond(), budget = t.policy.budget();
            if (percent <= 50) {
                assertEquals(SEED, floor, "debt " + percent + "%");
                assertEquals(reduced, budget, "half of the way to a write slowdown changes nothing");
            } else {
                assertTrue(floor > SEED && budget >= floor && budget >= last, "debt " + percent + "%: floor " + floor + ", budget " + budget);
                if (percent == 100) known = floor;
                else assertTrue(floor < 8 * SEED);
            }
            assertEquals(percent / 100d, t.policy.compactionDebt(), 1e-9);
            last = budget;
        }
        assertEquals(KNOWN, known, "floor at full debt");
        assertEquals(known, t.policy.budget());
    }
    /**
     * What background I/O was known to get before slow reads took it away in {@link #reducedByHarm}: the 50% moving
     * average of its rate at the first decrease (1, 1, 2, 4, 8, 8, 8, 8 MB/s), which RECOVERY would restore too.
     */
    private static final long KNOWN = 7_671_875;
    @Test void risingCompactionDebtEndsAReductionUnderTest() {
        var t = reducedByHarm();
        t.window(10, 200, t.policy.budget()); t.window(10, 200, t.policy.budget());
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
        t.debt = .9;
        t.poll(10, 200, t.policy.budget(), false, true);
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        assertEquals(t.policy.floorBytesPerSecond(), t.policy.budget());
        assertEquals(SEED + (KNOWN - SEED) * 8 / 10, t.policy.budget(), "80% of the way from the seed to the last known service");
    }
    @Test void slowReadsCannotLowerTheBudgetUnderTheDebtFloor() {
        var t = reducedByHarm();
        t.debt = .75;
        t.poll(10, 100, SEED, false, true);
        long floor = t.policy.floorBytesPerSecond();
        assertEquals((SEED + KNOWN) / 2, floor, "half of the way from the seed to the last known service");
        assertEquals(floor, t.policy.budget());
        double baseline = t.policy.baselineReadMicros();
        for (int i = 0; i < 60; i++) {
            t.window(10, 300, t.policy.budget());
            assertTrue(t.policy.budget() >= floor, "window " + i + " budget " + t.policy.budget());
            assertTrue(t.policy.floorBytesPerSecond() >= floor);
            assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        }
        assertEquals(baseline, t.policy.baselineReadMicros(), "slow reads at a floor that debt raised are not accepted"
                + " as the reference: the floor does not last");
    }
    @Test void fallingCompactionDebtLetsTheFloorReturnToTheSeed() {
        var t = reducedByHarm();
        t.debt = 1;
        t.poll(10, 100, SEED, false, true);
        long opened = t.policy.budget();
        assertEquals(KNOWN, opened);
        long floor = opened;
        for (int percent = 95; percent >= 0; percent -= 5) {
            t.debt = percent / 100d;
            t.poll(10, 100, SEED, false, true);
            assertTrue(t.policy.floorBytesPerSecond() <= floor, "debt " + percent + "%");
            floor = t.policy.floorBytesPerSecond();
        }
        assertEquals(SEED, floor);
        assertEquals(opened, t.policy.budget(), "the floor going down lowers nothing by itself");
        long lowest = opened;
        for (int i = 0; i < 80; i++) {
            t.window(10, t.policy.state() == CompactionIoBudget.State.PROBE ? 150 : 200, t.policy.budget());
            lowest = Math.min(lowest, t.policy.budget());
        }
        assertEquals(SEED, lowest, "slow reads that answer to the rate take the budget down to the seed again");
    }
    @Test void unsampledOrInvalidDebtIsNoDebt() {
        for (double debt : new double[]{Double.NaN, -1, 0}) {
            var t = reducedByHarm();
            long reduced = t.policy.budget();
            t.debt = debt;
            t.poll(10, 100, SEED, false, true);
            assertEquals(SEED, t.policy.floorBytesPerSecond());
            assertEquals(0, t.policy.compactionDebt());
            assertEquals(reduced, t.policy.budget());
        }
        var t = reducedByHarm();
        t.debt = 7; // over the trigger: as at the trigger
        t.poll(10, 100, SEED, false, true);
        assertEquals(1, t.policy.compactionDebt());
        assertEquals(KNOWN, t.policy.budget());
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
        assertEquals(SEED, t.policy.harmCeilingBytesPerSecond(), "and the rate before it is the one that harms");
        // Towards the harmful rate: +10% of it per saturated healthy window up to 90% of it, then +5% every 6th.
        t.window(10, 80, 750_000);
        assertEquals(850_000, t.policy.budget());
        t.window(10, 80, 850_000);
        assertEquals(900_000, t.policy.budget());
        for (int i = 0; i < 5; i++) {
            t.window(10, 80, 900_000);
            assertEquals(900_000, t.policy.budget(), "window " + i);
        }
        t.window(10, 80, 900_000);
        assertEquals(950_000, t.policy.budget());
        t.window(10, 200, 950_000);
        assertEquals(950_000, t.policy.budget(), "a saturated window with slow point reads must not grow");
    }
    @Test void growthTowardsTheHarmfulRateDoesNotStallOnRounding() {
        // 90% of 1,000,003 is not a whole number of bytes: the budget stopped one byte under it and never took the
        // +5% steps, which start at 90%.
        var t = new Trace();
        t.window(10, 100, 1_000_003); t.window(10, 100, 750_002); t.window(10, 100, 750_002);
        t.window(10, 200, 1_000_003); t.window(10, 200, 1_000_003);
        t.window(10, 100, 700_002); t.window(10, 100, 700_002); t.window(10, 100, 700_002);
        assertEquals(700_002, t.policy.budget());
        assertEquals(1_000_003, t.policy.harmCeilingBytesPerSecond());
        for (int i = 0; i < 14; i++) t.window(10, 100, t.policy.budget());
        assertTrue(t.policy.budget() >= 1_000_003, "budget " + t.policy.budget());
    }
    @Test void aDoublingFollowedBySlowPointReadsIsTakenBack() {
        var t = new Trace(); t.window(10, 100, SEED); t.window(10, 100, SEED); t.window(10, 100, SEED);
        assertEquals(SEED, t.policy.budget());
        t.window(10, 100, SEED);
        assertEquals(2 * SEED, t.policy.budget());
        t.window(10, 200, 2 * SEED);
        assertEquals(SEED, t.policy.budget(), "point reads at twice the baseline right after the step");
        assertEquals(0, t.policy.harmCeilingBytesPerSecond(), "one slow window is not yet a harmful rate");
        for (int i = 0; i < 2; i++) {
            t.window(10, 100, SEED);
            assertEquals(SEED, t.policy.budget(), "no growth in the 3 windows after a decrease (the slow one was the first)");
        }
        t.window(10, 100, SEED);
        assertEquals(2 * SEED, t.policy.budget(), "the doubling is tried again");
        t.window(10, 200, 2 * SEED);
        assertEquals(SEED, t.policy.budget());
        assertEquals(2 * SEED, t.policy.harmCeilingBytesPerSecond(), "taken back twice in a row: 2 MB/s harms");
        t.window(10, 100, SEED); t.window(10, 100, SEED);
        assertEquals(SEED, t.policy.budget());
        t.window(10, 100, SEED);
        assertEquals(1_200_000, t.policy.budget(), "then +10% of the rate that harmed, not a doubling back to it");
    }
    @Test void aDoublingThatStoodItsFirstWindowIsNotTheFirstOfTwoTakenBack() {
        var t = new Trace(); t.window(10, 100, SEED); t.window(10, 100, SEED); t.window(10, 100, SEED);
        t.window(10, 100, SEED);
        t.window(10, 200, 2 * SEED); // taken back
        t.window(10, 100, SEED); t.window(10, 100, SEED); t.window(10, 100, SEED);
        assertEquals(2 * SEED, t.policy.budget());
        t.window(10, 100, 2 * SEED); // this doubling stands, and the next one follows
        assertEquals(4 * SEED, t.policy.budget());
        t.window(10, 200, 4 * SEED);
        assertEquals(2 * SEED, t.policy.budget());
        assertEquals(0, t.policy.harmCeilingBytesPerSecond(), "a single slow window again");
    }
    @Test void aDoublingThatDidNotRaiseTheBackgroundRateTeachesNoCeiling() {
        var t = new Trace(); t.window(10, 100, SEED); t.window(10, 100, SEED); t.window(10, 100, SEED);
        t.window(10, 100, SEED);
        assertEquals(2 * SEED, t.policy.budget());
        t.window(10, 200, SEED); // background I/O did not use the step: the slow reads are not its doing
        assertEquals(SEED, t.policy.budget(), "the unused step is taken back all the same");
        assertEquals(0, t.policy.harmCeilingBytesPerSecond());
    }
    @Test void theHarmfulRateIsForgottenAfterTenHarmFreeMinutes() {
        var t = new Trace();
        t.window(10, 100, SEED); t.window(10, 100, 750_000); t.window(10, 100, 750_000);
        t.window(10, 200, SEED); t.window(10, 200, SEED);
        t.window(10, 100, 700_000); t.window(10, 100, 700_000); t.window(10, 100, 700_000);
        assertEquals(700_000, t.policy.budget());
        assertEquals(SEED, t.policy.harmCeilingBytesPerSecond());
        for (int i = 0; i < 118; i++) {
            // Background I/O under 80% of the budget, and one slow window now and then (noise, not harm).
            t.window(10, i % 10 == 9 ? 200 : 100, 400_000);
            assertEquals(SEED, t.policy.harmCeilingBytesPerSecond(), "window " + i);
            assertEquals(700_000, t.policy.budget());
        }
        t.window(10, 100, 400_000); t.window(10, 100, 400_000);
        assertEquals(0, t.policy.harmCeilingBytesPerSecond());
        t.window(10, 100, 700_000);
        assertEquals(1_400_000, t.policy.budget(), "capacity is unknown again: a saturated healthy window doubles");
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
        int reduced = 0;
        for (int window = 0; window < 60; window++) {
            for (int second = 0; second < 5; second++) {
                t.completions += 3_000;
                t.poll(100, 3_000 + window % 3 * 3_000, Math.min(SEED, t.policy.budget()), false, true);
            }
            // A reduction is 70% or 75% of the achieved rate while its effect is measured; none is kept or goes further.
            if (t.policy.state() == CompactionIoBudget.State.PROBE) {
                reduced++;
                assertTrue(t.policy.budget() >= SEED * 7 / 10, "window " + window + " budget " + t.policy.budget());
            } else assertTrue(t.policy.budget() >= SEED, "window " + window + " budget " + t.policy.budget()
                    + ": latency unresponsive to throttling must not erase observed sustainable background service");
        }
        assertTrue(reduced <= 12, "windows at a reduced rate: " + reduced);
    }

    @Test void highLatencyWithoutProbeBenefitRestoresServiceAndHonorsCooldown() {
        var t = new Trace();
        t.window(10, 100, SEED);
        t.window(10, 100, 750_000); t.window(10, 100, 750_000);
        assertEquals(SEED, t.policy.budget());
        t.window(10, 6_000, SEED);
        assertEquals(SEED, t.policy.budget(), "one slow window");
        t.window(10, 6_000, SEED);
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state(), "slow reads are answered at the second window,"
                + " also right after a trial from a healthy state that showed nothing");
        assertEquals(700_000, t.policy.budget());
        t.window(10, 6_000, 700_000); t.window(10, 6_000, 700_000);
        assertEquals(SEED, t.policy.budget(), "no latency response must restore the budget");
        for (int window = 0; window < 12; window++) {
            t.window(10, 6_000, SEED);
            assertEquals(SEED, t.policy.budget(), "high latency must respect the cooldown of a decrease that did not help");
        }
        t.window(10, 6_000, SEED);
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
        t.window(10, 6_000, 700_000); t.window(10, 6_000, 700_000);
        assertEquals(SEED, t.policy.budget(), "no latency response must restore the budget");
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
        p.sample(new CompactionIoBudget.Sample(0, 0, 0, 0, false, true, false, true));
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
        t.policy.sample(new CompactionIoBudget.Sample(t.nanos + 1, 0, 0, 0, false, true, false, false));
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
        p.sample(new CompactionIoBudget.Sample(0, 0, 0, 0, false, true, false, true));
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
        p.sample(new CompactionIoBudget.Sample(0, 0, 0, 0, false, true, false, true));
        for (int second = 1; second <= 15; second++) {
            p.sample(new CompactionIoBudget.Sample(second * 1_000_000_000L, 0, 0, second * (SEED / 4),
                    true, second <= 5, false, true, false, 0, false));
        }
        assertEquals(CompactionIoBudget.State.BOOTSTRAP, p.state());
        assertEquals(SEED, p.budget());
        p.sample(new CompactionIoBudget.Sample(16_000_000_000L, 0, 0, 4 * SEED, true, true, false, true));
        assertEquals(SEED, p.budget(), "a quarter of the seed was what the background asked for under pressure;"
                + " pressure must not put the limiter under the rate that flushes a memtable in 5 s");
    }

}
