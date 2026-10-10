package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.CompactionIoBudget;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CompactionBulkFeedbackTest {
    private static final long RATE = 256L << 20;
    private static final class Trace {
        final CompactionIoBudget p = new CompactionIoBudget(RATE, 16L << 20);
        long nanos, pointReads, pointTime, done, bytes, bulkCount, bulkTime, bulkKeys, readDone,
                compBytes, compTime, compCalls, prefCount, prefBytes, applied, nativePoints, nativeBulks, nativeBulkKeys, nativePointNanos;
        long pointLatency = 22000, bulkCallMicros = 60000, transferred = RATE, writes = 10000, bulkTail, pointTail,
                compTransferred = 64L << 20, compReadMicros = 250000;
        int bytePeriod = 1, bytePhase, sampleIndex, compReadCalls = 1000;
        int metadataReads;
        long metadataMicros = 1000, nativePointMean = -1;
        int calls = 120, shape = 72, workers = 8, active = 0, latencyActive = 0, queued = 0, extraPointCalls;
        boolean point = true, pointNative = true, bulkStats = true, pressure, stopped, bulkPending;
        Trace() { step(); }
        void step() {
            nanos += 1_000_000_000L;
            if (point) { pointReads += 100; pointTime += 100 * pointLatency; }
            pointReads += metadataReads; pointTime += metadataReads * metadataMicros;
            long pointCalls = (point && pointNative ? 100 : 0) + extraPointCalls;
            nativePoints += pointCalls;
            nativePointNanos += pointCalls * (nativePointMean >= 0 ? nativePointMean : pointLatency) * 1000;
            done += writes;
            bytes += transferred;
            if (bulkStats) { bulkCount += calls; bulkTime += calls * bulkCallMicros; bulkKeys += (long) calls * shape; }
            nativeBulks += calls; nativeBulkKeys += (long) calls * shape;
            readDone += calls;
            if ((++sampleIndex + bytePhase) % bytePeriod == 0) compBytes += compTransferred;
            compTime += compReadMicros; compCalls += compReadCalls;
            if (applied > 0) { prefCount += 32; prefBytes += 32 * applied; }
            p.sample(new CompactionIoBudget.Sample(nanos, pointReads, pointTime, bytes, true,
                    pressure, stopped, true, true, done, !pressure, pressure,
                    compBytes, compTime, compCalls, prefCount, prefBytes,
                    bulkCount, bulkTime, bulkKeys, workers, active, latencyActive, queued, readDone, bulkPending, point && pointNative || extraPointCalls > 0 ? (pointTail > 0 ? pointTail : pointLatency) : 0,
                    bulkTail > 0 ? bulkTail : calls > 0 ? bulkCallMicros : 0, nativePoints, nativeBulks, nativeBulkKeys, nativePointNanos));
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
            for (int i = 0; i < 220 && p.state() != CompactionIoBudget.State.PROBE; i++) step();
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
                assertTrue(t.applied > 0, "a complete healthy OFF epoch may qualify a stationary new tail envelope");
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

    @Test void singleRawBulkTailDisablesWithoutRegressingNormalizedMeanOrPointLane() {
        var t = new Trace(); t.seconds(500); assertTrue(t.applied > 0);
        double mean = t.p.bulkBaselineMicrosPerKey();
        t.bulkTail = 120000; t.step();
        assertEquals(0, t.applied);
        assertEquals(mean, t.p.bulkBaselineMicrosPerKey(), .001);
    }
    @Test void largerBatchRecalibratesRawTailInsteadOfMixingDifferentCallShapes() {
        var t = new Trace(); t.seconds(500); long previous = t.applied;
        t.shape *= 3; t.bulkCallMicros *= 3;
        t.step();
        assertTrue(previous > 0);
        assertEquals(0, t.applied, "a new own-call shape must be calibrated with optional I/O OFF");
        t.seconds(500);
        assertTrue(t.applied > 0);
        assertTrue(t.p.bulkBaselineMicrosPerKey() > 0);
        t.bulkTail = t.bulkCallMicros * 2; t.step();
        assertEquals(0, t.applied, "the new comparable shape must get its own tail protection");
    }
    @Test void freshBulkTailWithoutSamePollCountDeltaStillShrinksAndAbortsProbe() throws Exception {
        var t = new Trace(); t.seconds(2000); assertEquals(16L << 20, t.applied);
        var state = CompactionIoBudget.class.getDeclaredField("state"); state.setAccessible(true);
        var prior = CompactionIoBudget.class.getDeclaredField("probeBudget"); prior.setAccessible(true);
        var rate = CompactionIoBudget.class.getDeclaredField("budget"); rate.setAccessible(true);
        long restored = t.p.budget(); prior.setLong(t.p, restored);
        rate.setLong(t.p, Math.max(restored * 3 / 4, 200L << 20));
        state.set(t.p, CompactionIoBudget.State.PROBE);
        t.calls = 0; t.bulkTail = 120000; // Max drains after cumulative native/SDK snapshots were already taken.
        t.step();
        assertEquals(0, t.applied, "a credible fresh max cannot require a matching current counter delta");
        assertEquals(CompactionIoBudget.State.TRACKING, t.p.state());
        assertEquals(restored, t.p.budget());
    }
    @Test void byteProbeWithReadaheadAlreadyZeroCannotAcceptMeanBenefitWithBadTail() throws Exception {
        var t = new Trace(); t.seconds(500); assertTrue(t.applied > 0);
        t.bulkTail = 120000; t.step(); assertEquals(0, t.applied); t.bulkTail = 0;
        t.calls = 0; t.bulkPending = true; t.probe();
        t.calls = 120; t.bulkPending = false;
        var before = CompactionIoBudget.class.getDeclaredField("probeBudget"); before.setAccessible(true);
        long prior = before.getLong(t.p);
        t.transferred = RATE * 3 / 4; t.pointLatency = 17000; t.bulkTail = 120000;
        t.seconds(10);
        assertEquals(CompactionIoBudget.State.TRACKING, t.p.state());
        assertEquals(prior, t.p.budget(), 4, "improved means cannot accept a byte cut while native bulk tails doubled");
        assertEquals(0, t.applied);
    }
    @Test void healthyBulkCannotHideSparseNativePointReadsWithNoBlockMisses() {
        var t = new Trace(); t.point = false; t.extraPointCalls = 1;
        t.seconds(600);
        assertTrue(t.p.bulkBaselineMicrosPerKey() > 0);
        assertEquals(0, t.applied, "a native point lane must calibrate its own tail even when reads hit cache");
    }
    @Test void startupUnderOscillatingQueuePressureStillProtectsRawTailsDuringRateProbe() throws Exception {
        var t = new Trace(); t.saturated();
        for (int i = 0; i < 20; i++) { t.queued = i % 2 == 0 ? 200 : 400; t.step(); }
        var growthTail = CompactionIoBudget.class.getDeclaredField("bulkTailBaseline"); growthTail.setAccessible(true);
        assertEquals(0, growthTail.getDouble(t.p), "unstable queue trajectory must not teach a growth baseline");
        for (int i = 0; i < 100 && t.p.state() != CompactionIoBudget.State.PROBE; i++) {
            t.queued = i % 2 == 0 ? 200 : 400; t.step();
        }
        assertEquals(CompactionIoBudget.State.PROBE, t.p.state());
        assertEquals(0, growthTail.getDouble(t.p));
        var before = CompactionIoBudget.class.getDeclaredField("probeBudget"); before.setAccessible(true);
        long prior = before.getLong(t.p);
        t.transferred = RATE * 3 / 4; t.pointLatency = 17000; t.bulkCallMicros = 50000; t.bulkTail = 120000;
        for (int i = 0; i < 10; i++) { t.queued = i % 2 == 0 ? 200 : 400; t.step(); }
        assertEquals(CompactionIoBudget.State.TRACKING, t.p.state());
        assertEquals(prior, t.p.budget(), 4, "an uncalibrated startup probe must reject a raw-tail regression");
        assertEquals(0, t.applied);
    }
    @Test void bulkMetadataBlockMissesCannotInventAnActiveNativePointLane() {
        var t = new Trace(); t.pointNative = false;
        t.seconds(500);
        assertEquals(0, t.p.baselineReadMicros(), "bulk metadata misses cannot initialize an owned point-call mean");
        assertTrue(t.p.bulkBaselineMicrosPerKey() > 0);
        assertTrue(t.applied > 0, "bulk block misses cannot require a nonexistent native point-call tail baseline");
    }
    @Test void missingSdkBulkFeedbackCannotTurnObservedNativeCallsIntoAnIdleLane() {
        var t = new Trace(); t.bulkStats = false; t.seconds(600);
        assertTrue(t.nativeBulks > 32);
        assertEquals(0, t.p.bulkBaselineMicrosPerKey());
        assertEquals(0, t.applied, "observed native bulk calls with no comparable SDK mean cannot authorize growth");
    }
    @Test void steadyLoadedReadLaneCanStartBoundedPrefetchWithoutAnIdleWindow() {
        var t = new Trace(); t.active = t.latencyActive = t.workers; t.queued = 16;
        t.bulkCallMicros = 128000;
        t.seconds(240);
        assertTrue(t.applied >= 65536, "steady full workers and a fixed queue must permit a measured prefetch seed");
        assertTrue(t.applied <= 16L << 20);
        assertTrue(t.veto(), "byte-rate demand pressure remains distinct from readahead latency safety");
    }
    @Test void steadyRecoveryCanStartBoundedPrefetchWhileDebtPersists() {
        var t = new Trace(); t.active = t.latencyActive = t.workers; t.queued = 16;
        t.bulkCallMicros = 128000; t.pressure = true;
        t.seconds(240);
        assertEquals(CompactionIoBudget.State.RECOVERY, t.p.state());
        assertTrue(t.applied >= 65536, "debt recovery cannot require idleness before any useful measured prefetch");
        t.bulkTail = 256000; t.step();
        assertEquals(0, t.applied, "recovery cannot protect a cap once measured foreground tails worsen");
    }
    @Test void readOnlySteadyBulkUsesItsOwnProgressDuringTrackingAndRecovery() {
        for (boolean recovery : new boolean[]{false, true}) {
            var t = new Trace(); t.point = false; t.writes = 0;
            t.active = t.latencyActive = t.workers; t.queued = 16; t.bulkPending = true;
            t.bulkCallMicros = 128000; t.pressure = recovery;
            t.seconds(240);
            assertTrue(t.applied >= 65536, "dense native bulk completions are progress even without point/write keys");
            assertEquals(0, t.done - 10000, "the generic key counter must stay flat during this read-only trace");
            if (recovery) assertEquals(CompactionIoBudget.State.RECOVERY, t.p.state());
        }
    }
    @Test void calibratedQueueGrowthCutsWithinTwoPollsAndRecoversCautiously() {
        var t = new Trace(); t.active = t.latencyActive = t.workers; t.queued = 16;
        t.bulkCallMicros = 128000; t.seconds(240); assertTrue(t.applied >= 65536);
        t.queued = 24; t.step(); t.step();
        assertEquals(0, t.applied, "two sustained above-anchor full-pool observations are a fast admission safety cut");
        t.seconds(59); assertEquals(0, t.applied, "stationary evidence cannot bypass the 60s cooldown");
        t.seconds(180);
        assertTrue(t.applied >= 65536 && t.applied <= 256 * 1024, "stable loaded recovery begins with bounded small reads");
    }
    @Test void risingAndOscillatingQueuesCannotRatchetTheirWayIntoRegrowth() {
        for (boolean rising : new boolean[]{false, true}) {
            var t = new Trace(); t.active = t.latencyActive = t.workers; t.queued = 16;
            t.bulkCallMicros = 128000; t.seconds(240); assertTrue(t.applied >= 65536);
            t.queued = 24; t.step(); t.step(); assertEquals(0, t.applied);
            for (int i = 0; i < 240; i++) {
                t.queued = rising ? t.queued + 2 : i % 2 == 0 ? 16 : 24;
                t.step(); assertEquals(0, t.applied, "a changing queue cannot supply stationary growth evidence");
            }
        }
    }
    @Test void sdkResetPreservesOwnReferenceUntilCompleteHealthyOffCalibration() throws Exception {
        var t = new Trace(); t.active = t.latencyActive = t.workers; t.queued = 16;
        t.bulkCallMicros = 128000; t.seconds(240); assertTrue(t.applied >= 65536);
        var tail = CompactionIoBudget.class.getDeclaredField("bulkTailBaseline"); tail.setAccessible(true);
        double healthy = tail.getDouble(t.p);
        t.bulkCount = t.bulkTime = t.bulkKeys = 0; t.bulkTail = 256000;
        t.step(); assertEquals(0, t.applied);
        assertEquals(healthy, tail.getDouble(t.p), .001, "SDK epoch reset is not an own native-call health reset");
        t.seconds(240);
        assertTrue(t.applied >= 65536);
        assertEquals(256000, tail.getDouble(t.p), .001, "only a complete healthy OFF epoch replaces the old envelope");
    }
    @Test void laggedCompactionBytesDoNotSuppressHealthyLoadedPrefetchAcrossPulsePhases() {
        for (int phase = 0; phase < 5; phase++) {
            var t = new Trace(); t.active = t.latencyActive = t.workers; t.queued = 16;
            t.bulkCallMicros = 128000;
            t.bytePeriod = 5; t.bytePhase = phase; t.compTransferred = 16L << 20; t.compReadCalls = 64;
            t.seconds(1200);
            assertTrue(t.applied >= 65536, "healthy per-read timing must remain progress between job byte publications, phase " + phase);
            long stable = t.applied; t.seconds(300);
            assertEquals(stable, t.applied, "publication phase cannot drive artificial shrink/regrowth");
        }
    }
    @Test void pulsePhaseAndSmallRateJitterCannotCrossHealthyReadaheadLadderBoundaries() {
        for (long pulse : new long[]{1L << 20, 16L << 20}) {
            for (int phase = 0; phase < 5; phase++) {
                var t = new Trace(); t.active = t.latencyActive = t.workers; t.queued = 16;
                t.bulkCallMicros = 128000; t.bytePeriod = 5; t.bytePhase = phase;
                t.compTransferred = pulse; t.compReadCalls = 64; t.seconds(1200);
                long cap = pulse / 16;
                assertEquals(cap, t.applied);
                for (int window = 0; window < 12; window++) {
                    t.compReadMicros = window % 2 == 0 ? 260000 : 240000;
                    for (int poll = 0; poll < 30; poll++) {
                        t.step(); assertEquals(cap, t.applied, "rounding or byte-publication phase is not service regression");
                    }
                }
            }
        }
    }
    @Test void subHundredMillisecondReadCollapseShrinksFromRetainedCohortOnFirstPoll() {
        var t = new Trace(); t.active = t.latencyActive = t.workers; t.queued = 16;
        t.bulkCallMicros = 128000; t.bytePeriod = 5; t.compTransferred = 16L << 20; t.compReadCalls = 64;
        t.seconds(1200); assertEquals(1L << 20, t.applied);
        t.compReadMicros = 6000000; // 93.75ms/read: below the immediate duration limit, but a real service collapse.
        t.step(); assertTrue(t.applied <= 512 * 1024);
    }
    @Test void readTimingStalenessAndCounterResetRemainUnsafeDespiteLaterBytePulses() throws Exception {
        for (boolean reset : new boolean[]{false, true}) {
            var t = new Trace(); t.active = t.latencyActive = t.workers; t.queued = 16;
            t.bulkCallMicros = 128000; t.bytePeriod = 5; t.compTransferred = 16L << 20; t.compReadCalls = 64;
            t.seconds(1200); assertTrue(t.applied > 0);
            if (reset) {
                t.compBytes = t.compTime = t.compCalls = 0;
                t.step();
                for (String name : new String[]{"recentReadBytes", "recentReadMicros", "recentReadCount"}) {
                    var field = CompactionIoBudget.class.getDeclaredField(name); field.setAccessible(true);
                    assertEquals(0, field.getLong(t.p), "counter reset must discard the retained service epoch");
                }
            } else {
                t.compReadCalls = 0; t.compReadMicros = 0; t.seconds(3);
            }
            assertEquals(0, t.applied);
        }
    }
    @Test void stationaryNoisyTailMaximaMustNotRatchetHealthyReferenceOrPreventPrefetchGrowth() throws Exception {
        for (boolean pointLane : new boolean[]{true, false}) {
            var t = new Trace(); t.active = t.latencyActive = t.workers; t.queued = 16;
            if (pointLane) t.pointTail = 200000; else t.bulkTail = 200000;
            t.seconds(500);
            var reference = CompactionIoBudget.class.getDeclaredField(pointLane ? "pointTailBaseline" : "bulkTailBaseline");
            reference.setAccessible(true);
            assertEquals(200000, reference.getDouble(t.p), .001);
            for (int cycle = 0; cycle < 80; cycle++) {
                if (pointLane) t.pointTail = 80000; else t.bulkTail = 80000;
                t.seconds(10);
                if (pointLane) t.pointTail = 200000; else t.bulkTail = 200000;
                t.seconds(5);
            }
            assertTrue(t.applied >= 65536, "a stationary 80/80/200ms max sequence must not permanently veto growth in "
                    + (pointLane ? "point" : "bulk") + " lane; reference=" + reference.getDouble(t.p));
        }
    }
    @Test void offEnvelopeUsesWholeBlockMaximaInsteadOfAveragingLowWindows() throws Exception {
        var t = new Trace(); t.active = t.latencyActive = t.workers; t.queued = 16;
        for (int cycle = 0; cycle < 50; cycle++) {
            t.pointTail = t.bulkTail = 80000; t.seconds(10);
            t.pointTail = t.bulkTail = 200000; t.seconds(5);
        }
        assertTrue(t.applied > 0);
        for (String name : new String[]{"pointTailBaseline", "bulkTailBaseline"}) {
            var f = CompactionIoBudget.class.getDeclaredField(name); f.setAccessible(true);
            assertEquals(200000, f.getDouble(t.p), .001);
        }
    }
    @Test void positiveCapFreezesBothTailReferencesThroughLowerMaximaAndRejectsRealSpike() throws Exception {
        var t = new Trace(); t.pointTail = t.bulkTail = 200000; t.seconds(600);
        assertTrue(t.applied > 0);
        for (String name : new String[]{"pointTailBaseline", "bulkTailBaseline"}) {
            var f = CompactionIoBudget.class.getDeclaredField(name); f.setAccessible(true);
            assertEquals(200000, f.getDouble(t.p), .001);
        }
        t.pointTail = t.bulkTail = 80000; t.seconds(180); assertTrue(t.applied > 0);
        for (String name : new String[]{"pointTailBaseline", "bulkTailBaseline"}) {
            var f = CompactionIoBudget.class.getDeclaredField(name); f.setAccessible(true);
            assertEquals(200000, f.getDouble(t.p), .001, "positive optional I/O cannot teach a lower envelope");
        }
        t.bulkTail = 250000; t.step(); assertEquals(0, t.applied);
    }
    @Test void offTailCalibrationCannotLearnAwaySimultaneousQueueOrMeanDeterioration() throws Exception {
        for (boolean hotQueue : new boolean[]{true, false}) {
            var t = new Trace(); t.active = t.latencyActive = t.workers; t.queued = 16;
            t.seconds(600); assertTrue(t.applied > 0);
            var reference = CompactionIoBudget.class.getDeclaredField("bulkTailBaseline"); reference.setAccessible(true);
            double healthy = reference.getDouble(t.p);
            t.bulkTail = 120000;
            if (!hotQueue) t.bulkCallMicros = 100000;
            t.step(); assertEquals(0, t.applied);
            for (int i = 0; i < 300; i++) {
                if (hotQueue) t.queued += 4;
                t.step(); assertEquals(0, t.applied);
            }
            assertEquals(healthy, reference.getDouble(t.p), .001,
                    "tail-only permission cannot erase concurrent non-tail safety evidence");
        }
    }
    @Test void newlyActiveOwnLaneStartsOffCalibrationFromPositiveAndAlreadyOffCaps() throws Exception {
        for (boolean newPoint : new boolean[]{true, false}) {
            for (boolean alreadyOff : new boolean[]{true, false}) {
                var t = new Trace();
                if (newPoint) t.point = false; else t.calls = 0;
                t.seconds(600); assertTrue(t.applied > 0);
                var reference = CompactionIoBudget.class.getDeclaredField(newPoint ? "pointTailBaseline" : "bulkTailBaseline");
                reference.setAccessible(true); assertEquals(0, reference.getDouble(t.p));
                if (alreadyOff) {
                    t.compReadMicros = (long) t.compReadCalls * 100000;
                    t.step(); assertEquals(0, t.applied); t.compReadMicros = 250000;
                }
                if (newPoint) t.point = true; else t.calls = 120;
                t.step(); assertEquals(0, t.applied, "an uncalibrated new lane must protect the live cap");
                t.seconds(60); assertEquals(0, reference.getDouble(t.p), "cooldown plus an incomplete OFF epoch cannot learn");
                t.seconds(180);
                assertTrue(reference.getDouble(t.p) > 0, "both complete OFF blocks must learn the newly active lane");
                assertTrue(t.applied >= 65536, "a calibrated new lane must permit bounded regrowth");
            }
        }
    }
    @Test void denseCachedPointLaneIgnoresRareVariableMetadataMissesDuringOffCalibration() throws Exception {
        var t = new Trace(); t.point = false; t.extraPointCalls = 100; t.metadataReads = 1;
        t.active = t.latencyActive = t.workers; t.queued = 16;
        t.seconds(85); // First complete OFF block: only five SDK metadata misses per window.
        t.metadataMicros = 10000;
        t.seconds(30); // Second block's sparse SDK misses are not a reliable point-mean comparator.
        var reference = CompactionIoBudget.class.getDeclaredField("pointTailBaseline"); reference.setAccessible(true);
        assertEquals(t.pointLatency, reference.getDouble(t.p), .001,
                "dense native cache hits must qualify despite below-quorum metadata latency variation");
        t.seconds(180); assertTrue(t.applied >= 65536);
    }
    @Test void newUnreferencedPointLaneAbortsBulkOnlyProbeBeforeApparentBulkBenefit() throws Exception {
        var t = new Trace(); t.point = false; t.prime(); t.probe();
        assertEquals(0, t.p.baselineReadMicros());
        var reference = CompactionIoBudget.class.getDeclaredField("probePointProtected"); reference.setAccessible(true);
        assertFalse(reference.getBoolean(t.p));
        var prior = CompactionIoBudget.class.getDeclaredField("probeBudget"); prior.setAccessible(true);
        long restored = prior.getLong(t.p);
        t.extraPointCalls = 1; t.nativePointMean = t.pointTail = 250;
        t.transferred = RATE * 3 / 4; t.bulkCallMicros = 50000;
        t.step();
        assertEquals(CompactionIoBudget.State.TRACKING, t.p.state(),
                "a new point lane must not be ignored by a bulk-only trial even with one fresh call");
        assertEquals(restored, t.p.budget());
    }
    @Test void boundedPeriodicPointMeansCanCompleteOffBlocksAndSeedPrefetch() throws Exception {
        var t = new Trace(); t.nativePointMean = 800; t.pointTail = 2000; t.bulkCallMicros = 115000;
        t.active = t.latencyActive = t.workers; t.queued = 16;
        t.seconds(20); assertEquals(800, t.p.baselineReadMicros(), .001);
        for (int cycle = 0; cycle < 30 && t.applied == 0; cycle++) {
            t.nativePointMean = 200; t.seconds(5);
            t.nativePointMean = 800; t.seconds(5);
        }
        var reference = CompactionIoBudget.class.getDeclaredField("pointTailBaseline"); reference.setAccessible(true);
        assertEquals(2000, reference.getDouble(t.p), .001,
                "two comparable30s blocks with a500us weighted mean must learn the fixed2ms envelope");
        assertTrue(t.applied >= 65536, "safe periodic point means must permit bounded prefetch seeding");
        assertEquals(800, t.p.baselineReadMicros(), .001);
        t.nativePointMean = 1000; t.step();
        assertEquals(0, t.applied, "a genuine mean breach must still cut on the first dense poll with unchanged max");
    }
    @Test void sustainedPointMeanDriftStillRejectsSecondOffBlockBelowFastThreshold() throws Exception {
        var t = new Trace(); t.nativePointMean = 500; t.pointTail = 2000; t.bulkCallMicros = 115000;
        t.active = t.latencyActive = t.workers; t.queued = 16;
        t.seconds(85); // Six complete5s windows after the60s cooldown form the first30s block.
        var blocks = CompactionIoBudget.class.getDeclaredField("tailBlocks"); blocks.setAccessible(true);
        assertEquals(1, blocks.getInt(t.p));
        t.nativePointMean = 570; t.seconds(30); // +14% exceeds block stationarity but stays below historical+20%.
        var reference = CompactionIoBudget.class.getDeclaredField("pointTailBaseline"); reference.setAccessible(true);
        assertEquals(0, reference.getDouble(t.p));
        assertEquals(0, t.applied);
        assertEquals(0, blocks.getInt(t.p), "the second weighted mean must still satisfy the whole-block drift guard");
    }
    @Test void boundedPeriodicBulkMeansCanCompleteOffBlocksAndSeedPrefetch() throws Exception {
        var t = new Trace(); t.nativePointMean = 800; t.pointTail = 2000;
        t.bulkCallMicros = 60000; t.bulkTail = 120000;
        t.active = t.latencyActive = t.workers; t.queued = 16;
        t.seconds(20);
        for (int cycle = 0; cycle < 30 && t.applied == 0; cycle++) {
            t.bulkCallMicros = 50000; t.seconds(5);
            t.bulkCallMicros = 60000; t.seconds(5);
        }
        var reference = CompactionIoBudget.class.getDeclaredField("bulkTailBaseline"); reference.setAccessible(true);
        assertEquals(120000, reference.getDouble(t.p), .001,
                "two comparable30s blocks with a55ms weighted bulk mean must learn the fixed120ms envelope");
        assertTrue(t.applied >= 65536, "safe periodic bulk means must permit bounded prefetch seeding");
        t.bulkTail = 150000; t.step();
        assertEquals(0, t.applied, "a genuine tail breach must still cut on the first poll");
    }
    @Test void sustainedBulkMeanDriftStillRejectsSecondOffBlockBelowFastThreshold() throws Exception {
        var t = new Trace(); t.nativePointMean = 800; t.pointTail = 2000;
        t.bulkCallMicros = 60000; t.bulkTail = 120000;
        t.active = t.latencyActive = t.workers; t.queued = 16;
        t.seconds(20); t.bulkCallMicros = 50000; t.seconds(65);
        var blocks = CompactionIoBudget.class.getDeclaredField("tailBlocks"); blocks.setAccessible(true);
        assertEquals(1, blocks.getInt(t.p));
        t.bulkCallMicros = 57000; t.seconds(30);
        var reference = CompactionIoBudget.class.getDeclaredField("bulkTailBaseline"); reference.setAccessible(true);
        assertEquals(0, reference.getDouble(t.p));
        assertEquals(0, t.applied);
        assertEquals(0, blocks.getInt(t.p), "+14% weighted bulk drift must still reject the second whole block");
    }
    @Test void nativeUrgencyRetainsAuthorityDespiteBulkSaturation() {
        var t = new Trace(); t.prime(); t.saturated(); t.bulkCallMicros = 200000; t.pressure = true;
        t.step(); assertEquals(CompactionIoBudget.State.RECOVERY, t.p.state());
    }
}
