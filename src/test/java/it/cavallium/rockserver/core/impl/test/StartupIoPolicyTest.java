package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.StartupIoPolicy;
import it.cavallium.rockserver.core.impl.StartupReadCompletions;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class StartupIoPolicyTest {
    private final StartupIoPolicy policy = new StartupIoPolicy();
    private final StartupReadCompletions counters = new StartupReadCompletions();
    private long epoch;
    private StartupIoPolicy.InputSample input(int active, int queued, boolean urgent) {
        epoch++;
        return new StartupIoPolicy.InputSample(epoch, epoch * 100_000_000, true, true,
                2, active, queued, 100, active + queued == 0 ? 100 : 99, urgent, counters.snapshot(epoch));
    }
    private StartupIoPolicy.Decision sample(int active, int queued, boolean urgent) {
        var input = input(active, queued, urgent);
        return policy.sample(input, input.sampledNanos());
    }
    private void calibrate() {
        for (int i = 0; i < StartupIoPolicy.CALIBRATION_WINDOWS; i++) {
            counters.recordPoint(100, false); counters.recordBulk(200, 4, false);
            var decision = sample(0, 0, false);
            assertEquals(i == StartupIoPolicy.CALIBRATION_WINDOWS - 1, decision.qualifiedHealthy());
        }
    }
    @Test void bootstrapRequiresRealCompletedWindowsAndIdleNeverGrowsAdmission() {
        var idle = sample(0, 0, false);
        assertEquals(StartupIoPolicy.UNKNOWN, idle.pressureMask());
        assertFalse(idle.capturePressure()); assertFalse(idle.qualifiedHealthy());
        calibrate();
        assertFalse(sample(0, 0, false).qualifiedHealthy());
        var busy = sample(2, 1, false);
        assertTrue(busy.capturePressure()); assertFalse(busy.qualifiedHealthy());
        assertNotEquals(0, busy.pressureMask() & StartupIoPolicy.FOREGROUND);
    }
    @Test void measuredMeanAndWindowMaximumCausePressureAndRecoveryNeedsFreshWindows() {
        calibrate(); counters.recordPoint(1000, false);
        assertEquals(StartupIoPolicy.FOREGROUND, sample(0, 0, false).pressureMask());
        for (int i = 0; i < StartupIoPolicy.RECOVERY_WINDOWS; i++) {
            counters.recordPoint(100, false);
            var input = input(0, 0, false); var result = policy.sample(input, input.sampledNanos());
            assertEquals(i == StartupIoPolicy.RECOVERY_WINDOWS - 1, result.qualifiedHealthy());
            assertSame(result, policy.sample(input, input.sampledNanos()));
        }
    }
    @Test void urgentRecoveryStalenessMalformedAndCounterResetNeverDefaultHealthy() {
        calibrate(); assertEquals(StartupIoPolicy.URGENT, sample(0, 0, true).pressureMask());
        var input = input(0, 1, false);
        var stale = policy.sample(input, input.sampledNanos() + StartupIoPolicy.MAX_SAMPLE_AGE_NANOS + 1);
        assertEquals(StartupIoPolicy.FOREGROUND | StartupIoPolicy.UNKNOWN, stale.pressureMask());
        counters.reset(); counters.recordPoint(100, false);
        assertFalse(sample(0, 0, false).qualifiedHealthy());
        var bad = new StartupIoPolicy.InputSample(++epoch, epoch * 100_000_000, false, false,
                -1, 0, 0, 0, 0, false, null);
        assertFalse(policy.sample(bad, bad.sampledNanos()).qualifiedHealthy());
    }
    @Test void malformedAndUnconservedNativeWindowsFailClosed() {
        calibrate();
        var p = new StartupReadCompletions.Cohort(1, 100, 100, 1, 0);
        var b = new StartupReadCompletions.Cohort(0, 0, 0, 0, 0);
        var forged = new StartupReadCompletions.Snapshot(++epoch, 0, 3, 3, p, b, true);
        var input = new StartupIoPolicy.InputSample(epoch, epoch * 100_000_000, true, true,
                2, 0, 0, 100, 100, false, forged);
        assertEquals(StartupIoPolicy.FOREGROUND | StartupIoPolicy.UNKNOWN,
                policy.sample(input, input.sampledNanos()).pressureMask());
        var malformed = new StartupReadCompletions.Snapshot(++epoch, 0, 3, 3, null, b, true);
        input = new StartupIoPolicy.InputSample(epoch, epoch * 100_000_000, true, true,
                2, 0, 0, 100, 100, false, malformed);
        assertFalse(policy.sample(input, input.sampledNanos()).qualifiedHealthy());
    }

    @Test void failuresAndEmptyOrZeroTimeCompletionsCannotCalibrate() {
        for (int i = 0; i < 6; i++) {
            counters.recordPoint(0, false); assertFalse(sample(0, 0, false).qualifiedHealthy());
        }
        counters.recordBulk(100, 5, true);
        assertEquals(StartupIoPolicy.FOREGROUND, sample(0, 0, false).pressureMask());
    }

    @Test void idleFailureBecomesNeutralAfterFreshWindowsWithoutQualifyingGrowth() {
        counters.recordBulk(100, 2, true);
        var failed = input(0, 0, false);
        assertTrue(policy.sample(failed, failed.sampledNanos()).capturePressure());
        for (int i = 0; i < StartupIoPolicy.RECOVERY_WINDOWS; i++) {
            var idle = input(0, 0, false);
            var decision = policy.sample(idle, idle.sampledNanos());
            assertEquals(i < StartupIoPolicy.RECOVERY_WINDOWS - 1, decision.capturePressure());
            assertFalse(decision.qualifiedHealthy());
            assertSame(decision, policy.sample(idle, idle.sampledNanos()));
        }
        var idle = input(0, 0, false);
        assertEquals(StartupIoPolicy.UNKNOWN, policy.sample(idle, idle.sampledNanos()).pressureMask());
    }

    @Test void aRepeatedHealthyWindowCannotUndoMissingOrContradictoryFeedback() {
        calibrate();
        counters.recordPoint(100, false);
        var healthy = input(0, 0, false);
        assertTrue(policy.sample(healthy, healthy.sampledNanos()).qualifiedHealthy());
        assertTrue(policy.sample(null, healthy.sampledNanos()).capturePressure());
        assertFalse(policy.sample(healthy, healthy.sampledNanos()).qualifiedHealthy());
        var urgent = new StartupIoPolicy.InputSample(healthy.epoch(), healthy.sampledNanos(), true, true,
                2, 0, 0, 100, 100, true, healthy.reads());
        assertTrue(policy.sample(urgent, urgent.sampledNanos()).capturePressure());
        assertNotEquals(0, policy.sample(healthy, healthy.sampledNanos()).pressureMask() & StartupIoPolicy.URGENT);
    }
}
