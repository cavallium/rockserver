package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.*;
import it.cavallium.rockserver.core.impl.StartupIoPolicy;
import it.cavallium.rockserver.core.impl.StartupReadCompletions;
import it.cavallium.rockserver.core.impl.StartupWriteCompletions;
import org.junit.jupiter.api.Test;

class StartupWriteFeedbackTest {
    private final StartupIoPolicy policy = new StartupIoPolicy();
    private final StartupReadCompletions reads = new StartupReadCompletions();
    private final StartupWriteCompletions writes = new StartupWriteCompletions();
    private long epoch;
    private StartupIoPolicy.InputSample input(boolean outstanding) {
        epoch++;
        return new StartupIoPolicy.InputSample(epoch, epoch * 100_000_000, true, true,
                8, 0, 0, 100, 100, false, outstanding, reads.snapshot(epoch), writes.snapshot(epoch));
    }
    private StartupIoPolicy.Decision sample(boolean outstanding) {
        var sample = input(outstanding);
        return policy.sample(sample, sample.sampledNanos());
    }
    private void healthyWindow() { reads.recordPoint(100, false); writes.record(100, false); }
    private void calibrate() {
        for (int i = 0; i < StartupIoPolicy.CALIBRATION_WINDOWS; i++) {
            healthyWindow(); sample(false);
        }
    }
    @Test void healthyReadsCannotHideSlowCompletedWritesWithNoQueue() {
        calibrate(); reads.recordPoint(100, false); writes.record(1000, false);
        var pressure = sample(false);
        assertEquals(StartupIoPolicy.FOREGROUND, pressure.pressureMask());
        assertFalse(pressure.qualifiedHealthy()); assertTrue(pressure.capturePressure());
        for (int i = 0; i < StartupIoPolicy.RECOVERY_WINDOWS; i++) {
            healthyWindow();
            assertEquals(i == StartupIoPolicy.RECOVERY_WINDOWS - 1, sample(false).qualifiedHealthy());
        }
    }
    @Test void activeNativeWriterAndOutstandingStagingCannotBeIdleOrReadQualified() {
        calibrate();
        long started = writes.begin();
        reads.recordPoint(100, false);
        var active = sample(false);
        assertFalse(active.qualifiedHealthy()); assertFalse(active.idleNeutral()); assertTrue(active.capturePressure());
        assertThrows(IllegalStateException.class, writes::reset);
        writes.complete(started, false);
        reads.recordPoint(100, false);
        assertFalse(sample(true).qualifiedHealthy());
        for (int i = 0; i < StartupIoPolicy.RECOVERY_WINDOWS; i++) {
            healthyWindow();
            assertEquals(i == StartupIoPolicy.RECOVERY_WINDOWS - 1, sample(false).qualifiedHealthy());
        }
    }
    @Test void failuresUncalibratedWritesAndIdleHaveDifferentMeanings() {
        reads.recordPoint(100, false); writes.record(100, false);
        assertFalse(sample(false).qualifiedHealthy());
        assertTrue(sample(false).idleNeutral());
        writes.record(100, true);
        assertEquals(StartupIoPolicy.FOREGROUND, sample(false).pressureMask());
        for (int i = 0; i < StartupIoPolicy.RECOVERY_WINDOWS; i++)
            assertEquals(i == StartupIoPolicy.RECOVERY_WINDOWS - 1, sample(false).idleNeutral());
    }
    @Test void nullWritePredecessorAndGenerationResetRebootstrapWithoutNpe() {
        var valid = input(false);
        var malformed = new StartupIoPolicy.InputSample(valid.epoch(), valid.sampledNanos(), true, true,
                8, 0, 0, 100, 100, false, false, valid.reads(), null);
        assertFalse(policy.sample(malformed, malformed.sampledNanos()).qualifiedHealthy());
        assertFalse(sample(false).idleNeutral());
        writes.reset();
        assertFalse(sample(false).idleNeutral());
        for (int i = 0; i < StartupIoPolicy.RECOVERY_WINDOWS; i++) sample(false);
        assertTrue(sample(false).idleNeutral());
    }
    @Test void writeCohortIsOneCutPerEpochWithFailureAndGenerationConservation() {
        writes.record(7, false); writes.record(11, true);
        var first = writes.snapshot(1);
        assertEquals(2, first.completed()); assertEquals(18, first.calls().nanos());
        assertEquals(11, first.calls().tailNanos()); assertEquals(1, first.calls().failures());
        writes.record(13, false);
        assertSame(first, writes.snapshot(1));
        var next = writes.snapshot(2);
        assertEquals(1, next.calls().count()); assertEquals(3, next.completed());
        writes.reset(); var reset = writes.snapshot(3);
        assertEquals(1, reset.generation()); assertEquals(0, reset.completed());
    }
    @Test void unconservedWriteTotalsAndStaleWriteWindowsFailClosed() {
        calibrate(); var normal = input(false);
        var empty = new StartupWriteCompletions.Cohort(0, 0, 0, 0);
        var bad = new StartupWriteCompletions.Snapshot(normal.epoch(), 0, normal.writes().completed() + 1, 0, empty, true);
        var malformed = new StartupIoPolicy.InputSample(normal.epoch(), normal.sampledNanos(), true, true,
                8, 0, 0, 100, 100, false, false, normal.reads(), bad);
        assertFalse(policy.sample(malformed, malformed.sampledNanos()).qualifiedHealthy());
        var next = input(false);
        assertFalse(policy.sample(next, next.sampledNanos() + StartupIoPolicy.MAX_SAMPLE_AGE_NANOS + 1).idleNeutral());
    }
    @Test void resetAndBeginHaveOneShortBoundaryWithoutHoldingTheMonitorAcrossNativeWork() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var returned = new java.util.concurrent.CountDownLatch(1);
        var token = new java.util.concurrent.atomic.AtomicLong();
        var worker = Thread.ofPlatform().unstarted(() -> {
            entered.countDown(); token.set(writes.begin()); returned.countDown();
        });
        try {
            synchronized (writes) {
                worker.start();
                assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
                long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
                while (worker.getState() != Thread.State.BLOCKED && returned.getCount() != 0) {
                    if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
                    if (System.nanoTime() >= deadline) fail("begin did not reach its synchronized boundary");
                    Thread.onSpinWait();
                }
                assertEquals(Thread.State.BLOCKED, worker.getState());
                writes.reset();
                assertEquals(1, returned.getCount());
            }
            assertTrue(returned.await(5, java.util.concurrent.TimeUnit.SECONDS));
            assertThrows(IllegalStateException.class, writes::reset);
        } finally {
            boolean interrupted = Thread.interrupted();
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
            while (worker.isAlive() && System.nanoTime() < deadline) {
                try { worker.join(1); } catch (InterruptedException ignored) { interrupted = true; }
            }
            if (returned.getCount() == 0) writes.complete(token.get(), false);
            if (interrupted) Thread.currentThread().interrupt();
            assertFalse(worker.isAlive());
        }
        writes.reset();
        assertEquals(2, writes.snapshot(1).generation());
    }

}
