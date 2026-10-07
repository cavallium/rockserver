package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.RangeDescriptor;
import it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor;
import org.apache.ratis.client.RaftClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Pure resource ownership checks; no Raft RPCs, sockets or server processes. */
class RatisClientPoolTest {
    private static RangeDescriptor route(int group, long epoch, long config, int incarnation) {
        var peer = new ReplicaDescriptor(new UUID(1,1), new UUID(2,incarnation), new UUID(3,1),
                "n1", "127.0.0.1:8001", ReplicaDescriptor.Role.VOTER);
        return new RangeDescriptor(new UUID(4,group), new UUID(5,group), epoch, config, null, null,
                RangeDescriptor.TopologyState.ACTIVE, List.of(peer));
    }
    private static RaftClient client(AtomicInteger closes) {
        return client(() -> closes.incrementAndGet());
    }
    @FunctionalInterface private interface Close { void run() throws IOException; }
    private static RaftClient client(Close close) {
        return (RaftClient) Proxy.newProxyInstance(RaftClient.class.getClassLoader(), new Class<?>[]{RaftClient.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("close")) { close.run(); return null; }
                    if (method.getName().equals("toString")) return "fixture";
                    throw new AssertionError("Unexpected Raft operation: " + method);
                });
    }

    @Test void reusesClientsAndEvictsOnlyIdleRoutesWithExactEpochAndMembership() throws Exception {
        var opens = new AtomicInteger();
        var closes = new AtomicInteger();
        try (var pool = new RatisClientPool(1, descriptor -> { opens.incrementAndGet(); return client(closes); })) {
            RaftClient first;
            try (var one = pool.acquire(route(1,1,1,1)); var two = pool.acquire(route(1,1,1,1))) {
                first = one.client();
                assertSame(first, two.client());
                assertEquals(1, opens.get());
                assertThrows(IOException.class, () -> pool.acquire(route(2,1,1,1)));
                one.close(); one.close();
                assertThrows(IllegalStateException.class, one::client);
                assertEquals(0, closes.get());
            }
            try (var again = pool.acquire(route(1,1,1,1))) { assertSame(first, again.client()); }
            for (var changed : List.of(route(1,2,1,1), route(1,2,2,1), route(1,2,2,2), route(2,2,2,2))) {
                try (var next = pool.acquire(changed)) { assertNotSame(first, next.client()); }
            }
            assertEquals(5, opens.get());
            assertEquals(4, closes.get());
        }
        assertEquals(5, closes.get());
    }

    @Test void oneCreationServesConcurrentCallersWithoutHoldingPoolMonitorAcrossSetup() throws Exception {
        var entered = new CountDownLatch(1);
        var proceed = new CountDownLatch(1);
        var opens = new AtomicInteger();
        var closes = new AtomicInteger();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor();
                var pool = new RatisClientPool(1, descriptor -> {
                    opens.incrementAndGet(); entered.countDown();
                    try { if (!proceed.await(5, TimeUnit.SECONDS)) throw new IOException("fixture timeout"); }
                    catch (InterruptedException failure) { throw new IOException(failure); }
                    return client(closes);
                })) {
            var first = executor.submit(() -> pool.acquire(route(1,1,1,1)));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var second = executor.submit(() -> pool.acquire(route(1,1,1,1)));
            try {
                executor.submit(() -> assertThrows(IOException.class, () -> pool.acquire(route(2,1,1,1))))
                        .get(1, TimeUnit.SECONDS);
                assertEquals(1, opens.get());
            } finally { proceed.countDown(); }
            try (var a = first.get(5, TimeUnit.SECONDS); var b = second.get(5, TimeUnit.SECONDS)) {
                assertSame(a.client(), b.client());
            }
        }
        assertEquals(1, closes.get());
    }

    @Test void retirementRunsOutsideMonitorAndPrecedesReplacementCreation() throws Exception {
        var entered = new CountDownLatch(1);
        var proceed = new CountDownLatch(1);
        var opens = new AtomicInteger();
        var closes = new AtomicInteger();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor();
                var pool = new RatisClientPool(1, descriptor -> {
                    int number = opens.incrementAndGet();
                    return client(() -> {
                        if (number == 1) {
                            entered.countDown();
                            try { if (!proceed.await(5, TimeUnit.SECONDS)) throw new IOException("fixture timeout"); }
                            catch (InterruptedException failure) { throw new IOException(failure); }
                        }
                        closes.incrementAndGet();
                    });
                })) {
            try (var first = pool.acquire(route(1,1,1,1))) { assertNotNull(first.client()); }
            var replacement = executor.submit(() -> pool.acquire(route(2,1,1,1)));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            try {
                executor.submit(() -> assertThrows(IOException.class, () -> pool.acquire(route(3,1,1,1))))
                        .get(1, TimeUnit.SECONDS);
                assertEquals(1, opens.get());
                assertEquals(0, closes.get());
            } finally { proceed.countDown(); }
            try (var next = replacement.get(5, TimeUnit.SECONDS)) { assertNotNull(next.client()); }
            assertEquals(2, opens.get());
            assertEquals(1, closes.get());
        }
        assertEquals(2, closes.get());
    }

    @Test void closeStopsAdmissionButRetainsBorrowedClientsUntilRetry() throws Exception {
        var closes = new AtomicInteger();
        var pool = new RatisClientPool(1, descriptor -> client(closes));
        var lease = pool.acquire(route(1,1,1,1));
        try {
            assertThrows(IOException.class, pool::close);
            assertEquals(0, closes.get());
            assertNotNull(lease.client());
            assertThrows(IOException.class, () -> pool.acquire(route(1,1,1,1)));
        } finally { lease.close(); pool.close(); }
        pool.close();
        assertEquals(1, closes.get());
    }

    @Test void failedSetupReleasesSlotAndFailedRetirementStopsGrowthAndRemainsOwned() throws Exception {
        var attempts = new AtomicInteger();
        var closes = new AtomicInteger();
        try (var pool = new RatisClientPool(1, descriptor -> {
            if (attempts.incrementAndGet() == 1) throw new IOException("setup failed");
            return client(closes);
        })) {
            assertThrows(IOException.class, () -> pool.acquire(route(1,1,1,1)));
            try (var lease = pool.acquire(route(1,1,1,1))) { assertNotNull(lease.client()); }
        }
        assertEquals(2, attempts.get()); assertEquals(1, closes.get());
        var releases = new AtomicInteger();
        var opens = new AtomicInteger();
        var pool = new RatisClientPool(1, descriptor -> {
            opens.incrementAndGet();
            return client(() -> { if (releases.incrementAndGet() == 1) throw new IOException("close failed"); });
        });
        try (var lease = pool.acquire(route(1,1,1,1))) { assertNotNull(lease.client()); }
        assertThrows(IOException.class, () -> pool.acquire(route(2,1,1,1)));
        assertThrows(IOException.class, () -> pool.acquire(route(3,1,1,1)));
        assertEquals(1, opens.get());
        pool.close(); pool.close();
        assertEquals(2, releases.get());
    }
}
