package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.CompactionIoBudget;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CompactionBulkFeedbackTest {
    private static final long RATE = 256L << 20;
    private static final class Trace {
        final CompactionIoBudget p = new CompactionIoBudget(RATE, 16L << 20);
        long nanos, pointReads, pointTime, done, bytes, bulkCount, bulkTime, bulkKeys, readDone,
                compBytes, compTime, compCalls, prefCount, prefBytes, applied;
        long pointLatency = 22000, bulkCallMicros = 60000, transferred = RATE, writes = 10000;
        int calls = 120, shape = 72, workers = 8, active = 0, latencyActive = 0, queued = 0;
        boolean point = true, pressure, bulkPending;
        Trace() { step(); }
        void step() {
            nanos += 1_000_000_000L;
            if (point) { pointReads += 100; pointTime += 100 * pointLatency; }
            done += writes;
            bytes += transferred;
            bulkCount += calls; bulkTime += calls * bulkCallMicros; bulkKeys += (long) calls * shape;
            readDone += calls;
            compBytes += 64L << 20; compTime += 250000; compCalls += 1000;
            if (applied > 0) { prefCount += 32; prefBytes += 32 * applied; }
            p.sample(new CompactionIoBudget.Sample(nanos, pointReads, pointTime, bytes, true,
                    pressure, pressure, true, true, done, !pressure, pressure,
                    compBytes, compTime, compCalls, prefCount, prefBytes,
                    bulkCount, bulkTime, bulkKeys, workers, active, latencyActive, queued, readDone, bulkPending));
            if (p.readaheadBytes() != applied) {
                applied = p.readaheadBytes();
                try {
                    var m = CompactionIoBudget.class.getDeclaredMethod("readaheadApplied", long.class);
                    m.setAccessible(true); m.invoke(p, applied);
                } catch (ReflectiveOperationException failure) { throw new AssertionError(failure); }
            }
        }
        void seconds(int n) { for (int i = 0; i < n; i++) step(); }
        void saturated() { active = latencyActive = workers; queued = 200; }
        void prime() { seconds(20); assertEquals(CompactionIoBudget.State.TRACKING, p.state()); }
        void probe() {
            for (int i = 0; i < 100 && p.state() != CompactionIoBudget.State.PROBE; i++) step();
            assertEquals(CompactionIoBudget.State.PROBE, p.state());
        }
        boolean veto() {
            try { var f = CompactionIoBudget.class.getDeclaredField("readGrowthVeto"); f.setAccessible(true); return f.getBoolean(p); }
            catch (ReflectiveOperationException failure) { throw new AssertionError(failure); }
        }
    }
    @Test void healthyPointAndWritesCannotHideSaturatedModeratelySlowerBulk() {
        var t = new Trace(); t.prime(); long rate = t.p.budget();
        t.bulkCallMicros = 70000; t.saturated(); t.seconds(50);
        assertTrue(t.veto()); assertEquals(rate, t.p.budget()); assertEquals(0, t.applied);
        assertEquals(60000d / 72, t.p.bulkBaselineMicrosPerKey(), .001,
                "hot queues must not teach the controller that the slower lane is healthy");
        t.active = t.latencyActive = t.queued = 0; t.bulkCallMicros = 60000;
        t.seconds(5); assertTrue(t.veto(), "one clear window is insufficient");
        t.seconds(5); assertFalse(t.veto());
    }
    @Test void pointBenefitCannotAcceptBulkRegressionDuringSaturatedProbe() {
        var t = new Trace(); t.prime(); t.bulkCallMicros = 70000; t.saturated(); t.probe();
        long prior = t.p.budget() / 3 * 4;
        t.transferred = RATE * 3 / 4; t.pointLatency = 17000; t.bulkCallMicros = 80000;
        t.seconds(10);
        assertEquals(CompactionIoBudget.State.TRACKING, t.p.state());
        assertEquals(prior, t.p.budget(), 4, "bulk regression must restore despite healthier block misses");
    }
    @Test void sustainedBulkBenefitWithProtectedPointAndBothProgressMeasuresNeedsTwoWindows() {
        var t = new Trace(); t.prime(); t.bulkCallMicros = 70000; t.saturated(); t.probe();
        long reduced = t.p.budget(); t.transferred = RATE * 3 / 4; t.bulkCallMicros = 60000;
        t.seconds(10); assertEquals(CompactionIoBudget.State.PROBE, t.p.state());
        t.seconds(5); assertEquals(CompactionIoBudget.State.TRACKING, t.p.state());
        assertEquals(reduced, t.p.budget());
    }
    @Test void smallerBatchCannotMasqueradeAsPreservedBulkWork() {
        var t = new Trace(); t.prime(); t.bulkCallMicros = 70000; t.saturated(); t.probe();
        long reduced = t.p.budget(); t.transferred = RATE * 3 / 4;
        t.shape = 54; t.bulkCallMicros = 35000; t.pointLatency = 17000; t.seconds(10);
        assertEquals(CompactionIoBudget.State.TRACKING, t.p.state());
        assertTrue(t.p.budget() > reduced, "75% key progress cannot justify a cut despite a comparable shape");
    }
    @Test void bulkAloneCanCalibrateAndSupportGrowthWithoutBlockMissQuorum() {
        var t = new Trace(); t.point = false; t.seconds(500);
        assertEquals(0, t.p.baselineReadMicros());
        assertTrue(t.p.bulkBaselineMicrosPerKey() > 0); assertTrue(t.applied > 0);
    }
    @Test void resetSparseAndStillActiveZeroBulkCannotClearVetoOrIncrease() {
        var t = new Trace(); t.prime(); t.saturated(); t.seconds(5); assertTrue(t.veto());
        t.bulkCount = t.bulkTime = t.bulkKeys = 0; t.calls = 0; t.bulkPending = true;
        t.seconds(10); assertTrue(t.veto()); assertEquals(0, t.applied);
        t.active = 0; t.latencyActive = 1; t.queued = 0;
        t.seconds(20); assertEquals(0, t.applied, "zero completion is not idle while latency reads remain active");
        t.latencyActive = 0; t.bulkPending = false; t.seconds(10); assertFalse(t.veto());
        t.calls = 1; t.seconds(180); assertEquals(0, t.applied, "sparse bulk cannot authorize growth");
    }
    @Test void shapeChangeRecalibratesIndependentlyRatherThanMixingLaneMeans() {
        var t = new Trace(); t.prime(); t.shape = 256; t.bulkCallMicros = 256000;
        t.seconds(5); assertEquals(0, t.p.bulkBaselineMicrosPerKey());
        t.seconds(5); assertEquals(1000, t.p.bulkBaselineMicrosPerKey(), .001);
        assertEquals(22000, t.p.baselineReadMicros(), .001);
    }
    @Test void healthyWritesDoNotResetTheOnceOnlyBulkNoProgressEscape() {
        var t = new Trace(); t.prime(); t.saturated(); t.calls = 0; t.bulkPending = true; t.writes = 1_000_000;
        t.seconds(5); assertEquals(CompactionIoBudget.State.PROBE, t.p.state());
        t.seconds(10); assertEquals(CompactionIoBudget.State.TRACKING, t.p.state());
        long rate = t.p.budget();
        for (int i = 0; i < 90; i++) { t.step(); assertEquals(rate, t.p.budget()); }
        assertEquals(0, t.applied);
    }
    @Test void stableSlowShiftRebasesOnlyAfterGuardedNoBenefitTrialsWithReadHeadroom() {
        for (boolean queued : new boolean[]{false, true}) {
            var t = new Trace(); t.prime(); t.calls = 20; t.bulkCallMicros = 120000;
            if (queued) t.saturated();
            for (int i = 0; i < 600; i++) {
                t.transferred = t.p.state() == CompactionIoBudget.State.PROBE ? RATE * 3 / 4 : RATE;
                t.step();
            }
            if (queued) {
                assertEquals(60000d / 72, t.p.bulkBaselineMicrosPerKey(), .001);
                assertTrue(t.veto()); assertEquals(0, t.applied);
            } else {
                assertTrue(t.p.bulkBaselineMicrosPerKey() >= 100000d / 72,
                        "reliable unresponsive slow work must not create a permanent false veto");
                assertTrue(t.applied > 0);
            }
        }
    }

    @Test void previouslyIdleBulkIsProtectedAgainWhenUnknownReadsBecomeActive() {
        var t = new Trace(); t.prime(); t.calls = 0; t.seconds(10);
        assertEquals(CompactionIoBudget.State.TRACKING, t.p.state());
        t.active = t.latencyActive = 1; t.bulkPending = true; // New long read: no completed bulk histogram point and no queue yet.
        t.seconds(5);
        assertEquals(CompactionIoBudget.State.PROBE, t.p.state(), "read no-progress escape must protect the renewed unknown lane");
        long reduced = t.p.budget(); t.transferred = RATE * 3 / 4; t.pointLatency = 17000;
        t.seconds(10);
        assertEquals(CompactionIoBudget.State.TRACKING, t.p.state());
        assertTrue(t.p.budget() > reduced, "point improvement cannot accept while renewed bulk remains unobserved");
        t.seconds(90); assertEquals(0, t.applied);
    }

    @Test void firstInflightBulkIsProtectedBeforeAnyCompletion() {
        var t = new Trace(); t.calls = 0; t.bulkPending = true; t.writes = 1_000_000;
        t.seconds(5);
        assertEquals(CompactionIoBudget.State.TRACKING, t.p.state());
        long rate = t.p.budget();
        t.pointLatency = 17000;
        t.seconds(100);
        assertEquals(rate, t.p.budget(), "no completed bulk reference can authorize growth");
        assertEquals(0, t.applied);
    }

    @Test void historicalBulkCanBecomeIdleWhilePointOnlyReadsStayBusy() {
        var t = new Trace(); t.prime(); t.calls = 0;
        t.active = t.latencyActive = 4; t.bulkPending = false;
        t.seconds(500);
        assertTrue(t.applied > 0, "busy points must not create a permanent unknown bulk veto");
        t.saturated(); long rate = t.p.budget(); t.seconds(5);
        assertTrue(t.veto(), "read saturation remains independent of proven bulk idle");
        t.seconds(10); assertTrue(t.p.budget() <= rate);
    }

    @Test void nativeUrgencyRetainsAuthorityDespiteBulkSaturation() {
        var t = new Trace(); t.prime(); t.saturated(); t.bulkCallMicros = 200000; t.pressure = true;
        t.step(); assertEquals(CompactionIoBudget.State.RECOVERY, t.p.state());
    }
}
