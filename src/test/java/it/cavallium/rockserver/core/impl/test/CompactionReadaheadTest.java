package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.CompactionIoBudget;

import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.rocksdb.RateLimiter;
import static org.junit.jupiter.api.Assertions.*;

class CompactionReadaheadTest {
    private static final long MIB = 1L << 20;
    private static void acknowledge(CompactionIoBudget policy, long bytes) {
        try {
            var method = CompactionIoBudget.class.getDeclaredMethod("readaheadApplied", long.class);
            method.setAccessible(true); method.invoke(policy, bytes);
        } catch (ReflectiveOperationException failure) { throw new AssertionError(failure); }
    }
    private AutoCloseable controller(String name, RateLimiter limiter, Input input, SimpleMeterRegistry registry,
                                      java.util.function.LongUnaryOperator apply) throws Exception {
        var constructor = Class.forName("it.cavallium.rockserver.core.impl.CompactionIoController")
                .getDeclaredConstructor(String.class, RateLimiter.class, java.util.function.Supplier.class,
                        io.micrometer.core.instrument.MeterRegistry.class, long.class,
                        java.util.function.LongUnaryOperator.class, boolean.class);
        constructor.setAccessible(true);
        return (AutoCloseable) constructor.newInstance(name, limiter,
                (java.util.function.Supplier<CompactionIoBudget.Sample>) input::next, registry, 16 * MIB, apply, false);
    }
    private static final class Input {
        long nanos = System.nanoTime(), reads, micros, bytes, completions, compBytes, compMicros, compCount,
                prefetchCount, prefetchBytes, applied, readerSize, readerChangeAt = Long.MAX_VALUE;
        long rate = 256 * MIB, serviceRate = 256 * MIB, latency = 100, readerDelay;
        int readCalls = 1000;
        boolean pending = true, progress = true, pressure;
        CompactionIoBudget.Sample next() {
            nanos += 1_000_000_000L;
            if (nanos >= readerChangeAt) readerSize = applied;
            if (progress) { reads += 100; micros += 100 * latency; completions += 100; }
            bytes += rate;
            long readBytes = Math.min(rate, readCalls * 65536L);
            compBytes += readBytes; compMicros += (long) (readBytes * (1_000_000d / serviceRate)); compCount += readCalls;
            if (readerSize > 0) { prefetchCount += 32; prefetchBytes += 32 * readerSize; }
            return new CompactionIoBudget.Sample(nanos, reads, micros, bytes, true, pressure, pressure, true,
                    pending, completions, !pressure, pressure, compBytes, compMicros, compCount, prefetchCount, prefetchBytes);
        }
        void applied(long size) { applied = size; readerChangeAt = nanos + readerDelay; }
    }
    private static final class Trace {
        final Input in = new Input();
        final CompactionIoBudget policy;
        int changes;
        long changedAt;
        Trace(long ceiling) { policy = new CompactionIoBudget(in.rate, ceiling); step(); }
        void step() {
            policy.sample(in.next());
            long next = policy.readaheadBytes();
            if (next != in.applied) {
                if (next > in.applied) assertTrue(in.applied == 0 ? next == 65536 : next <= in.applied * 2);
                in.applied(next); acknowledge(policy, next); changes++; changedAt = in.nanos;
            }
        }
        void seconds(int count) { for (int i = 0; i < count; i++) step(); }
        void until(long size) { for (int i = 0; i < 3000 && in.applied < size; i++) step(); assertEquals(size, in.applied); }
    }
    @Test void denseHealthyEvidenceReachesCeilingWithBoundedGrowthAndSettling() {
        var t = new Trace(16 * MIB);
        t.until(16 * MIB);
        assertEquals(9, t.changes); // 64KiB through 16MiB, not tiny five-second writes.
        int changes = t.changes;
        t.seconds(180);
        assertEquals(changes, t.changes, "stable targets must not rewrite OPTIONS");
    }
    @Test void summedReaderTimeConstrainsSizeRatherThanConcurrentWallThroughput() {
        var t = new Trace(16 * MIB);
        t.in.serviceRate = (long) (1.6 * MIB); // 16MiB / ten summed reader seconds, not a one-second wall sample.
        t.until(128 * 1024);
        t.seconds(600);
        assertEquals(128 * 1024, t.in.applied);
    }
    @Test void zeroSubUsefulCeilingsAndSparseCallsNeverIncrease() {
        for (long ceiling : new long[]{0, 4095, 65535}) {
            var t = new Trace(ceiling); t.seconds(600); assertEquals(0, t.changes);
        }
        var sparse = new Trace(16 * MIB); sparse.in.readCalls = 1;
        sparse.seconds(600); assertEquals(0, sparse.changes);
    }
    @Test void badEndingForegroundWindowPreventsSecondServiceConfirmation() {
        var t = new Trace(16 * MIB); t.seconds(70); // now at second 71; first reliable service window was healthy.
        t.in.latency = 300; t.seconds(5);
        assertEquals(0, t.in.applied, "current, not previous five-second foreground health must gate growth");
    }
    @Test void earlierBadForegroundWindowCannotBeHiddenByHealthyEndingWindow() {
        var t = new Trace(16 * MIB); t.seconds(45);
        t.in.latency = 300; t.seconds(5);
        t.in.latency = 100; t.seconds(25);
        assertEquals(0, t.in.applied, "every constituent foreground window must be healthy");
    }
    @Test void upwardOutlierAndSmallJitterDoNotWobbleOptionSize() {
        var t = new Trace(16 * MIB); t.in.serviceRate = 20 * MIB; t.until(MIB);
        t.seconds(300); int changes = t.changes;
        t.in.serviceRate = 2000 * MIB; t.seconds(30);
        t.in.serviceRate = 20 * MIB;
        for (int i = 0; i < 8; i++) { t.in.serviceRate = (i % 2 == 0 ? 19 : 21) * MIB; t.seconds(30); }
        assertEquals(MIB, t.in.applied); assertEquals(changes, t.changes);
    }
    @Test void delayedUnconfirmedReadersFreezeGrowthButCannotDisableRateProbesForever() {
        var t = new Trace(16 * MIB); t.in.readerDelay = Long.MAX_VALUE / 4;
        t.until(65536); long change = t.changedAt;
        boolean probe = false;
        for (int i = 0; i < 200; i++) {
            t.step();
            if (t.in.nanos - change < 60_000_000_000L) assertNotEquals(CompactionIoBudget.State.PROBE, t.policy.state());
            else probe |= t.policy.state() == CompactionIoBudget.State.PROBE;
        }
        assertTrue(probe, "old jobs cannot indefinitely disable foreground protection");
        assertEquals(65536, t.in.applied, "elapsed time alone cannot confirm a new reader population");
    }
    @Test void foregroundNoProgressAndNativeUrgencyBypassReadSizeBlackout() {
        for (boolean urgency : new boolean[]{false, true}) {
            var t = new Trace(16 * MIB); t.in.readerDelay = Long.MAX_VALUE / 4; t.until(65536);
            t.in.progress = false; t.in.pressure = urgency;
            t.seconds(5);
            assertEquals(urgency ? CompactionIoBudget.State.RECOVERY : CompactionIoBudget.State.PROBE, t.policy.state());
            assertEquals(65536, t.in.applied, "escape must not grow read size");
        }
    }
    @Test void sparseAndResetServiceEpochsHoldInsteadOfRamp() {
        var t = new Trace(16 * MIB); t.until(65536);
        int changes = t.changes;
        t.in.compBytes = t.in.compMicros = t.in.compCount = t.in.prefetchCount = t.in.prefetchBytes = 0;
        t.in.readCalls = 1; t.seconds(180);
        assertEquals(changes, t.changes); assertEquals(65536, t.in.applied);
    }
    @Test void frozenProbeMayTemporarilyExceedAdmissionTimeTargetWithoutRaisingReadSize() {
        var t = new Trace(MIB); t.in.rate = 13_107_200;
        t.until(MIB);
        boolean observed = false;
        for (int i = 0; i < 240; i++) {
            t.step();
            if (t.policy.state() == CompactionIoBudget.State.PROBE) {
                assertEquals(MIB, t.in.applied);
                observed |= t.in.applied > t.policy.budget() / 10;
            }
        }
        assertTrue(observed, "freeze isolates the rate probe; admission-time target is not an all-state latency guarantee");
    }
    @Test void recoveryClampsReadSizeToRestoredUsefulBudgetAndNeverGrows() throws Exception {
        for (long service : new long[]{2 * MIB, CompactionIoBudget.MIN_BYTES_PER_SECOND}) {
            var p = new CompactionIoBudget(256 * MIB, 16 * MIB);
            p.sample(new CompactionIoBudget.Sample(1, 0, 0, 0, true, false, false, true));
            acknowledge(p, MIB);
            for (var entry : java.util.Map.of("budget", service, "preThrottle", service, "probeBudget", 0L).entrySet()) {
                var field = CompactionIoBudget.class.getDeclaredField(entry.getKey()); field.setAccessible(true);
                field.setLong(p, entry.getValue());
            }
            var state = CompactionIoBudget.class.getDeclaredField("state"); state.setAccessible(true);
            state.set(p, CompactionIoBudget.State.PROBE); // Existing frozen size with a reduced rate.
            p.sample(new CompactionIoBudget.Sample(1_000_000_001L, 0, 0, 0, true, true, true, true));
            assertEquals(CompactionIoBudget.State.RECOVERY, p.state());
            long expected = service / 10 / 4096 * 4096;
            if (expected < 65536) expected = 0;
            assertEquals(expected, p.readaheadBytes());
            long size = p.readaheadBytes();
            for (int i = 2; i < 40; i++) {
                p.sample(new CompactionIoBudget.Sample(i * 1_000_000_000L, 0, 0, 0, true, true, true, true));
                assertEquals(size, p.readaheadBytes(), "urgent polls cannot grow or restore a larger size");
            }
        }
    }
    @Test void gaugePublishesOnlySuccessfulAckAndStableOptionIsNotRewritten() throws Exception {
        RocksDBLoader.loadLibrary();
        var input = new Input(); var calls = new AtomicInteger(); var registry = new SimpleMeterRegistry();
        try (var limiter = new RateLimiter(input.rate, 100_000, RateLimiter.DEFAULT_FAIRNESS,
                org.rocksdb.RateLimiterMode.ALL_IO, false);
             var controller = controller("acknowledged-readahead", limiter, input, registry, size -> {
                 calls.incrementAndGet(); input.applied(size); return size;
             })) {
            var executorField = controller.getClass().getDeclaredField("executor"); executorField.setAccessible(true);
            var executor = (ScheduledExecutorService) executorField.get(controller);
            var pollField = controller.getClass().getDeclaredField("poll"); pollField.setAccessible(true);
            var poll = (Runnable) pollField.get(controller);
            for (int i = 0; i < 100 && calls.get() == 0; i++) executor.submit(poll).get(5, TimeUnit.SECONDS);
            assertEquals(65536, registry.get("rockserver.compaction.io.readahead").gauge().value());
            for (int i = 0; i < 10; i++) executor.submit(poll).get(5, TimeUnit.SECONDS);
            assertEquals(1, calls.get());
            assertEquals(0, registry.get("rockserver.compaction.io.adjustment.failures").gauge().value());
        } finally { registry.close(); }
    }

    @Test void closeWaitsForOwnedActuatorBeforeNativeDisposal() throws Exception {
        RocksDBLoader.loadLibrary();
        var input = new Input(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var disposed = new AtomicBoolean(); var registry = new SimpleMeterRegistry();
        try (var limiter = new RateLimiter(input.rate, 100_000, RateLimiter.DEFAULT_FAIRNESS,
                org.rocksdb.RateLimiterMode.ALL_IO, false);
             var controller = controller("close-actuator", limiter, input, registry, size -> {
                 entered.countDown();
                 try { assertTrue(release.await(5, TimeUnit.SECONDS)); }
                 catch (InterruptedException failure) { throw new AssertionError(failure); }
                 assertFalse(disposed.get()); return size;
             });
             var closing = Executors.newVirtualThreadPerTaskExecutor()) {
            var budgetField = controller.getClass().getDeclaredField("budget"); budgetField.setAccessible(true);
            var sizeField = CompactionIoBudget.class.getDeclaredField("readaheadBytes"); sizeField.setAccessible(true);
            sizeField.setLong(budgetField.get(controller), 65536); // Isolate actuator lifetime from policy timing.
            var executorField = controller.getClass().getDeclaredField("executor"); executorField.setAccessible(true);
            var executor = (ScheduledExecutorService) executorField.get(controller);
            var pollField = controller.getClass().getDeclaredField("poll"); pollField.setAccessible(true);
            var pending = executor.submit((Runnable) pollField.get(controller));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var close = closing.submit(() -> { controller.close(); return null; });
            try { assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS)); }
            finally { release.countDown(); }
            close.get(5, TimeUnit.SECONDS); pending.get(5, TimeUnit.SECONDS);
            disposed.set(true); limiter.close(); controller.close();
        } finally { release.countDown(); registry.close(); }
    }

    @Test void successfulControllerAckAndMutateThenThrowForceCorrectionEvenFromCachedZero() throws Exception {
        RocksDBLoader.loadLibrary();
        var input = new Input(); var actual = new AtomicLong(); var calls = new AtomicInteger();
        var registry = new SimpleMeterRegistry();
        try (var limiter = new RateLimiter(input.rate, 100_000, RateLimiter.DEFAULT_FAIRNESS,
                org.rocksdb.RateLimiterMode.ALL_IO, false);
             var controller = controller("dirty-readahead", limiter, input, registry, size -> {
                 actual.set(size);
                 if (calls.incrementAndGet() == 1) throw new IllegalStateException("mutated before persistence failed");
                 input.applied(size); return size;
             })) {
            var executorField = controller.getClass().getDeclaredField("executor"); executorField.setAccessible(true);
            var executor = (ScheduledExecutorService) executorField.get(controller);
            var pollField = controller.getClass().getDeclaredField("poll"); pollField.setAccessible(true);
            var poll = (Runnable) pollField.get(controller);
            for (int i = 0; i < 100 && calls.get() == 0; i++) executor.submit(poll).get(5, TimeUnit.SECONDS);
            assertEquals(65536, actual.get());
            assertEquals(0, registry.get("rockserver.compaction.io.readahead").gauge().value());
            assertEquals(1, registry.get("rockserver.compaction.io.adjustment.failures").gauge().value());
            executor.submit(poll).get(5, TimeUnit.SECONDS);
            assertEquals(0, actual.get()); assertEquals(2, calls.get(), "dirty target0 cannot be skipped against cached0");
            for (int i = 0; i < 10; i++) executor.submit(poll).get(5, TimeUnit.SECONDS);
            assertEquals(2, calls.get(), "stable successful targets cause no setter calls");
        } finally { registry.close(); }
    }
}
