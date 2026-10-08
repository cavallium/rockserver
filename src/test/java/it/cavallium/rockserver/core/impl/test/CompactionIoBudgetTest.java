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
            for (int i = 0; i < 5; i++) poll(count, latency, transferred, false, false);
        }
    }
    @Test void bootstrapLearnsAndProbeKeepsOnlyMeasuredImprovement() {
        var t = new Trace();
        t.window(10, 100, SEED);
        assertEquals(CompactionIoBudget.State.PROBE, t.policy.state());
        assertEquals(750_000, t.policy.budget());
        t.window(10, 80, 750_000); t.window(10, 80, 750_000);
        assertEquals(CompactionIoBudget.State.TRACKING, t.policy.state());
        assertEquals(750_000, t.policy.budget());
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
    @Test void observedStopOverridesPartialSampleAndRecoveryRequiresClearance() {
        var p = new CompactionIoBudget(SEED);
        p.sample(new CompactionIoBudget.Sample(1, 0, 0, 0, false, true, true, false));
        assertEquals(CompactionIoBudget.State.RECOVERY, p.state());
        for (int i = 1; i < 20; i++) p.sample(new CompactionIoBudget.Sample(i * 1_000_000_000L,
                0, 0, 0, false, false, false, true, false, 0, false));
        assertEquals(CompactionIoBudget.State.RECOVERY, p.state());
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
}
