package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.rocksdb.*;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.distributed.SnapshotRetentionExpiredException;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PoolSnapshotAdmissionTest {
    private final AtomicLong now = new AtomicLong();
    private final PoolSnapshotAdmission.Limits limits = new PoolSnapshotAdmission.Limits(2, 100, Duration.ofSeconds(10), 20);
    private TestGuard guard(long bytes, long free) {
        return new TestGuard(limits, () -> new TestSample(bytes, free, now.get()), now::get);
    }
    @Test void quotaCountsEveryLeaseAndRefundsExactlyOnce() {
        var guard = guard(40, 30);
        var first = guard.reserve();
        var second = guard.reserve();
        assertEquals(new PoolSnapshotAdmission.Stats(2, 80, Duration.ZERO), guard.stats());
        assertOverloaded(guard::reserve);
        first.close(); first.close();
        assertEquals(40, guard.stats().estimatedBytes());
        try (var third = guard.reserve()) { assertEquals(2, guard.stats().pins()); }
        second.close();
        assertEquals(0, guard.stats().pins());
        assertEquals(0, guard.stats().estimatedBytes());
    }
    @Test void bytePressureAndFreeSpaceRejectWithoutReservation() {
        var tooLarge = guard(101, 30);
        assertOverloaded(tooLarge::reserve);
        assertEquals(0, tooLarge.stats().pins());
        var noSpace = guard(20, 19);
        assertOverloaded(noSpace::reserve);
        assertEquals(0, noSpace.stats().estimatedBytes());
    }
    @Test void unusableMetricsFailClosed() {
        for (var sample : new TestSample[] {
                new TestSample(-1, 30, 0),
                new TestSample(1, -1, 0),
                new TestSample(1, 30, -Duration.ofSeconds(6).toNanos()),
                new TestSample(1, 30, 1)}) {
            var guard = new TestGuard(limits, () -> sample, now::get);
            assertOverloaded(guard::reserve);
            assertEquals(0, guard.stats().pins());
        }
        var failed = new TestGuard(limits, () -> { throw new IOException("filesystem unavailable"); }, now::get);
        assertOverloaded(failed::reserve);
        assertEquals(0, failed.stats().pins());
    }
    @Test void expiredLivePinBlocksAdmissionAndUseWithoutRefund() {
        var guard = guard(20, 30);
        var pin = guard.reserve();
        now.set(Duration.ofSeconds(10).toNanos());
        assertThrows(SnapshotRetentionExpiredException.class, pin::checkUse);
        assertOverloaded(guard::reserve);
        assertEquals(Duration.ofSeconds(10), guard.stats().oldestAge());
        assertEquals(1, guard.stats().pins());
        pin.close();
        try (var next = guard.reserve()) { next.checkUse(); }
    }
    @Test void ongoingGrowthAndSpacePressureBlockUseButKeepCharge() {
        var bytes = new AtomicLong(20);
        var free = new AtomicLong(30);
        var guard = new TestGuard(limits, () -> new TestSample(bytes.get(), free.get(), now.get()), now::get);
        try (var pin = guard.reserve()) {
            bytes.set(101);
            assertOverloaded(pin::checkUse);
            assertEquals(20, guard.stats().estimatedBytes());
            bytes.set(20); free.set(19);
            assertOverloaded(pin::checkUse);
            assertEquals(1, guard.stats().pins());
            free.set(30); pin.checkUse();
        }
        assertEquals(0, guard.stats().pins());
    }
    // Product module opens rocksdb to rockserver.core.test; keep sampler/clock seams package-private.
    private record TestSample(long estimatedBytes, long minimumFreeBytes, long sampledAtNanos) {}
    @FunctionalInterface private interface TestSource { TestSample sample() throws Exception; }
    private static final class TestGuard {
        private final PoolSnapshotAdmission guard;
        private TestGuard(PoolSnapshotAdmission.Limits limits, TestSource source, java.util.function.LongSupplier clock) {
            try {
                Class<?> sourceType = Class.forName(PoolSnapshotAdmission.class.getName() + "$Source");
                Class<?> sampleType = Class.forName(PoolSnapshotAdmission.class.getName() + "$Sample");
                var sampleConstructor = sampleType.getDeclaredConstructor(long.class, long.class, long.class);
                sampleConstructor.setAccessible(true);
                Object proxy = java.lang.reflect.Proxy.newProxyInstance(sourceType.getClassLoader(), new Class<?>[] {sourceType},
                        (ignored, method, arguments) -> {
                            TestSample value = source.sample();
                            return sampleConstructor.newInstance(value.estimatedBytes(), value.minimumFreeBytes(), value.sampledAtNanos());
                        });
                var constructor = PoolSnapshotAdmission.class.getDeclaredConstructor(PoolSnapshotAdmission.Limits.class, sourceType, java.util.function.LongSupplier.class);
                constructor.setAccessible(true);
                guard = constructor.newInstance(limits, proxy, clock);
            } catch (ReflectiveOperationException failure) { throw new AssertionError(failure); }
        }
        private TestLease reserve() { return new TestLease(invoke(guard, "reserve")); }
        private PoolSnapshotAdmission.Stats stats() { return guard.stats(); }
    }
    private static final class TestLease implements AutoCloseable {
        private final Object lease;
        private TestLease(Object lease) { this.lease = lease; }
        private void checkUse() { invoke(lease, "checkUse"); }
        @Override public void close() { invoke(lease, "close"); }
    }
    private static Object invoke(Object owner, String name) {
        try {
            var method = owner.getClass().getDeclaredMethod(name);
            method.setAccessible(true);
            return method.invoke(owner);
        } catch (java.lang.reflect.InvocationTargetException failure) {
            if (failure.getCause() instanceof RuntimeException cause) throw cause;
            if (failure.getCause() instanceof Error cause) throw cause;
            throw new AssertionError(failure.getCause());
        } catch (ReflectiveOperationException failure) { throw new AssertionError(failure); }
    }
    private static void assertOverloaded(org.junit.jupiter.api.function.Executable action) {
        var failure = assertThrows(RocksDBException.class, action);
        assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED, failure.getErrorUniqueId());
    }
}
