package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership;
import it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.*;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RangeOwnershipTest {
    private static final UUID DOMAIN = new UUID(0, 1);
    private static final UUID COLUMN = new UUID(0, 2);

    private Owner owner(long id) { return new Owner(new UUID(1, id), 7); }
    private byte[] key(int value) { return new byte[]{(byte) value}; }
    private Interval interval(byte[] lower, byte[] upper) { return new Interval(DOMAIN, Set.of(COLUMN), lower, upper); }

    @Test
    void immutableIntervalsUseUnsignedBoundsAndInfinity() {
        var guard = new RangeOwnership();
        byte[] lower = key(0x7f);
        byte[] upper = key(0xff);
        var columns = new HashSet<>(Set.of(COLUMN));
        var interval = new Interval(DOMAIN, columns, lower, upper);
        lower[0] = 0;
        upper[0] = 0;
        columns.clear();
        interval.lowerInclusive()[0] = 0;
        assertArrayEquals(key(0x7f), interval.lowerInclusive());
        assertArrayEquals(key(0xff), interval.upperExclusive());
        assertEquals(Set.of(COLUMN), interval.columns());
        assertEquals(interval, interval(key(0x7f), key(0xff)));
        guard.register(owner(1), interval, State.ACTIVE);
        guard.register(owner(2), interval(null, key(0x7f)), State.FENCED);
        guard.register(owner(3), interval(key(0xff), null), State.ACTIVE);
        assertThrows(IllegalStateException.class,
                () -> guard.register(owner(4), interval(key(0x80), key(0x90)), State.FENCED));
        assertThrows(IllegalArgumentException.class, () -> interval(key(0x80), key(0x7f)));
        assertThrows(IllegalArgumentException.class, () -> interval(key(1), key(1)));
    }

    @Test
    void identitiesPreventStaleTokensAndCrossDomainColumnAliasing() {
        var guard = new RangeOwnership();
        var original = owner(1);
        guard.register(original, interval(null, null), State.FENCED);
        var stale = new Owner(original.groupId(), original.epoch() - 1);
        assertThrows(IllegalStateException.class, () -> guard.activate(stale));
        assertThrows(IllegalStateException.class, () -> guard.claimInstall(stale));
        assertThrows(IllegalStateException.class, () -> guard.register(stale, interval(null, null), State.FENCED));
        assertThrows(IllegalStateException.class, () -> guard.register(owner(2),
                new Interval(UUID.randomUUID(), Set.of(COLUMN), null, null), State.FENCED));
        assertThrows(IllegalArgumentException.class, () -> guard.register(owner(3),
                new Interval(DOMAIN, Set.of(UUID.randomUUID()), null, null), State.FENCED));
        assertDoesNotThrow(() -> guard.register(owner(4),
                new Interval(UUID.randomUUID(), Set.of(UUID.randomUUID()), null, null), State.FENCED));
        assertThrows(IllegalArgumentException.class,
                () -> guard.register(owner(5), interval(null, null), State.INSTALLING));
    }

    @Test
    void splitIsAtomicRequiresFenceAndAdmittedBucketBoundary() {
        var guard = new RangeOwnership();
        var parent = owner(1);
        guard.register(parent, interval(key(0x10), key(0xf0)), State.ACTIVE);
        assertThrows(IllegalStateException.class,
                () -> guard.split(parent, key(0x80), owner(2), owner(3), (i, k) -> true));
        guard.fence(parent);
        // Example fixed bucket-prefix policy: a boundary must be two bytes with a zero suffix.
        BoundaryPolicy buckets = (i, k) -> k.length == 2 && k[1] == 0;
        assertThrows(IllegalArgumentException.class,
                () -> guard.split(parent, new byte[]{(byte) 0x80, 1}, owner(2), owner(3), buckets));
        assertEquals(State.FENCED, guard.get(parent).state());
        assertThrows(IllegalStateException.class, () -> guard.get(owner(2)));
        guard.register(owner(3), interval(key(0xf0), null), State.FENCED);
        assertThrows(IllegalStateException.class,
                () -> guard.split(parent, key(0x80), owner(2), owner(3), (i, k) -> true));
        assertEquals(State.FENCED, guard.get(parent).state());
        assertThrows(IllegalStateException.class, () -> guard.get(owner(2)));
        var children = guard.split(parent, new byte[]{(byte) 0x80, 0}, owner(2), owner(4), buckets);
        assertEquals(State.RETIRED, guard.get(parent).state());
        assertEquals(State.FENCED, children.getFirst().state());
        assertArrayEquals(new byte[]{(byte) 0x80, 0}, children.getFirst().interval().upperExclusive());
        assertArrayEquals(children.getFirst().interval().upperExclusive(),
                children.getLast().interval().lowerInclusive());
        assertThrows(IllegalStateException.class, () -> guard.activate(parent));
        assertThrows(IllegalStateException.class, () -> guard.claimCleanup(parent));
        guard.activate(owner(2));
        assertThrows(IllegalStateException.class, () -> guard.claimCleanup(parent));
    }

    @Test
    void mergeRequiresAdjacentCompatibleFencesAndTransfersAllOrNone() {
        var guard = new RangeOwnership();
        guard.register(owner(1), interval(null, key(0x80)), State.FENCED);
        guard.register(owner(2), interval(key(0x80), key(0xf0)), State.ACTIVE);
        guard.register(owner(3), interval(key(0xf1), null), State.FENCED);
        assertThrows(IllegalStateException.class, () -> guard.merge(owner(1), owner(2), owner(4)));
        guard.fence(owner(2));
        assertThrows(IllegalArgumentException.class, () -> guard.merge(owner(2), owner(3), owner(4)));
        assertThrows(IllegalArgumentException.class, () -> guard.merge(owner(2), owner(1), owner(4)));
        assertThrows(IllegalStateException.class, () -> guard.merge(owner(1), owner(2), owner(3)));
        assertEquals(State.FENCED, guard.get(owner(1)).state());
        assertEquals(State.FENCED, guard.get(owner(2)).state());
        assertThrows(IllegalStateException.class, () -> guard.get(owner(4)));
        var merged = guard.merge(owner(1), owner(2), owner(4));
        assertEquals(interval(null, key(0xf0)), merged.interval());
        assertEquals(State.FENCED, merged.state());
        assertEquals(State.RETIRED, guard.get(owner(1)).state());
        assertEquals(State.RETIRED, guard.get(owner(2)).state());
        assertThrows(IllegalStateException.class, () -> guard.claimCleanup(owner(1)));
        assertThrows(IllegalStateException.class, () -> guard.claimInstall(owner(2)));
    }

    @Test
    void installationLeaseBlocksOverlappingChangesButDoesNotHoldMetadataLock() throws Exception {
        var guard = new RangeOwnership();
        guard.register(owner(1), interval(null, key(0x80)), State.FENCED);
        guard.register(owner(2), interval(key(0x80), null), State.FENCED);
        var acquired = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var installing = executor.submit(() -> {
                try (var lease = guard.claimInstall(owner(1))) {
                    assertEquals(State.INSTALLING, lease.ownership().state());
                    assertEquals(DOMAIN, lease.ownership().interval().domainId());
                    acquired.countDown();
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                }
                return null;
            });
            try {
                assertTrue(acquired.await(5, TimeUnit.SECONDS));
                assertEquals(State.INSTALLING, guard.get(owner(1)).state());
                assertThrows(IllegalStateException.class, () -> guard.activate(owner(1)));
                assertThrows(IllegalStateException.class, () -> guard.retire(owner(1)));
                assertThrows(IllegalStateException.class, () -> guard.claimInstall(owner(1)));
                assertThrows(IllegalStateException.class, () -> guard.claimCleanup(owner(1)));
                assertThrows(IllegalStateException.class,
                        () -> guard.split(owner(1), key(1), owner(3), owner(4), (i, k) -> true));
                assertThrows(IllegalStateException.class,
                        () -> guard.register(owner(5), interval(key(1), key(2)), State.RETIRED));
                assertEquals(State.ACTIVE, executor.submit(() -> guard.activate(owner(2))).get(1, TimeUnit.SECONDS).state());
            } finally {
                release.countDown();
            }
            installing.get(5, TimeUnit.SECONDS);
        }
        assertEquals(State.FENCED, guard.get(owner(1)).state());
        guard.activate(owner(1));
    }

    @Test
    void cleanupLeaseBlocksReuseAndBoundaryCallbackRunsOutsideLock() throws Exception {
        var guard = new RangeOwnership();
        guard.register(owner(1), interval(null, key(0x80)), State.FENCED);
        guard.retire(owner(1));
        var lease = guard.claimCleanup(owner(1));
        assertThrows(IllegalStateException.class,
                () -> guard.register(owner(2), interval(null, key(0x80)), State.FENCED));
        assertThrows(IllegalStateException.class, () -> guard.claimCleanup(owner(1)));
        lease.close();
        lease.close();
        guard.register(owner(2), interval(null, key(0x80)), State.FENCED);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            guard.split(owner(2), key(0x40), owner(3), owner(4), (parent, split) -> {
                try {
                    executor.submit(() -> guard.register(owner(5), interval(key(0x80), null), State.FENCED))
                            .get(1, TimeUnit.SECONDS);
                    split[0] = 0; // Policy cannot mutate the actual boundary.
                    return true;
                } catch (Exception failure) {
                    throw new AssertionError(failure);
                }
            });
        }
        assertArrayEquals(key(0x40), guard.get(owner(3)).interval().upperExclusive());
    }
}
