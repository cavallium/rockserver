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

    @Test void recoveryRegistersExactBootstrapMiddleAndLatestNativeCheckpoints() {
        var guard=new RangeOwnership(); var added=new UUID(0,3);
        var bootstrap=interval(null,key(0x40));
        var middle=new Interval(DOMAIN,Set.of(COLUMN,added),key(0x40),key(0x80));
        var latest=new Interval(DOMAIN,Set.of(added),key(0x80),null);
        var expected=new java.util.HashMap<Owner,Interval>(java.util.Map.of(owner(1),bootstrap,owner(2),middle,owner(3),latest));
        var recovery=guard.prepareSchemaRecovery(DOMAIN,expected); expected.clear();
        guard.registerRecoveredSchema(owner(3),latest,State.FENCED,recovery);
        guard.registerRecoveredSchema(owner(1),bootstrap,State.FENCED,recovery);
        guard.registerRecoveredSchema(owner(2),middle,State.FENCED,recovery);
        assertThrows(IllegalArgumentException.class,() -> guard.registerRecoveredSchema(owner(4),latest,State.FENCED,recovery));
        assertThrows(IllegalArgumentException.class,() -> guard.prepareSchemaRecovery(DOMAIN,java.util.Map.of(owner(1),bootstrap)));
        var other=new RangeOwnership();
        assertThrows(IllegalArgumentException.class,() -> other.registerRecoveredSchema(owner(1),bootstrap,State.FENCED,recovery));
        assertThrows(IllegalArgumentException.class,() -> other.prepareSchemaRecovery(new UUID(0,9),java.util.Map.of(owner(1),bootstrap)));
        guard.activate(owner(1));
        assertThrows(IllegalArgumentException.class,() -> guard.register(owner(5),interval(key(0x90),key(0x91)),State.RETIRED));
    }

    @Test void transitionCompletionCanWaitWithoutClosingOrReleasingSourceOwners() {
        var guard=new RangeOwnership(); var source=interval(null,null); var target=new Interval(DOMAIN,Set.of(),null,null);
        guard.register(owner(1),source,State.FENCED);
        var token=guard.beginSchemaTransition(new UUID(3,1),DOMAIN,source.columns(),target.columns());
        assertFalse(guard.tryCompleteSchemaTransition(token)); assertEquals(source,guard.get(owner(1)).interval());
        try (var lease=guard.claimSchemaReplacement(owner(1),source,target,token)) {
            lease.publish(); assertFalse(guard.tryCompleteSchemaTransition(token));
        }
        assertTrue(guard.tryCompleteSchemaTransition(token));
        assertThrows(IllegalStateException.class,() -> guard.tryCompleteSchemaTransition(token));
    }

    @Test void delayedOwnersCrossColumnSetCyclesWithoutCountingAheadOwnersAsOldSources() {
        var guard=new RangeOwnership();
        var a=new Interval(DOMAIN,Set.of(),null,key(0x40));
        var b=new Interval(DOMAIN,Set.of(),key(0x40),key(0x80));
        var c=new Interval(DOMAIN,Set.of(),key(0x80),null);
        var initial=guard.prepareSchemaRecovery(DOMAIN,java.util.Map.of(owner(1),a,owner(2),b,owner(3),c));
        for (var entry:java.util.Map.of(owner(1),a,owner(2),b,owner(3),c).entrySet()) guard.registerRecoveredSchema(entry.getKey(),entry.getValue(),State.FENCED,initial);
        // Owner1 is logically two generations ahead with the same empty column set.
        var create=guard.beginRecoveredSchemaTransition(new UUID(3,1),DOMAIN,Set.of(),Set.of(COLUMN),initial,Set.of(owner(2),owner(3)));
        var createdB=new Interval(DOMAIN,Set.of(COLUMN),key(0x40),key(0x80));
        try (var lease=guard.claimSchemaReplacement(owner(2),b,createdB,create)) { lease.publish(); }
        assertFalse(guard.tryCompleteSchemaTransition(create));
        var afterCreate=guard.prepareSchemaRecovery(DOMAIN,java.util.Map.of(owner(1),a,owner(2),createdB,owner(3),c));
        var delete=guard.beginRecoveredSchemaTransition(new UUID(3,2),DOMAIN,Set.of(COLUMN),Set.of(),afterCreate,Set.of(owner(2)));
        try (var lease=guard.claimSchemaReplacement(owner(2),createdB,b,delete)) { lease.publish(); }
        assertTrue(guard.tryCompleteSchemaTransition(delete));
        var afterDelete=guard.prepareSchemaRecovery(DOMAIN,java.util.Map.of(owner(1),a,owner(2),b,owner(3),c));
        assertSame(create,guard.beginRecoveredSchemaTransition(new UUID(3,1),DOMAIN,Set.of(),Set.of(COLUMN),afterDelete,Set.of(owner(3))));
        var createdC=new Interval(DOMAIN,Set.of(COLUMN),key(0x80),null);
        try (var lease=guard.claimSchemaReplacement(owner(3),c,createdC,create)) { lease.publish(); }
        assertTrue(guard.tryCompleteSchemaTransition(create));
        assertEquals(Set.of(),guard.get(owner(1)).interval().columns()); assertEquals(Set.of(),guard.get(owner(2)).interval().columns());
    }

    @Test void schemaDeletionMovesEachFencedOwnerWithoutRelaxingOrdinaryDomainChecks() {
        var guard=new RangeOwnership(); var left=owner(1); var right=owner(2);
        var a=interval(null,key(0x80)); var b=interval(key(0x80),null);
        guard.register(left,a,State.ACTIVE); guard.register(right,b,State.FENCED);
        var change=new UUID(3,1); var token=guard.beginSchemaTransition(change,DOMAIN,Set.of(COLUMN),Set.of());
        assertSame(token,guard.beginSchemaTransition(change,DOMAIN,Set.of(COLUMN),Set.of()));
        var targetA=new Interval(DOMAIN,Set.of(),null,key(0x80)); var targetB=new Interval(DOMAIN,Set.of(),key(0x80),null);
        assertThrows(IllegalStateException.class,() -> guard.replaceSchema(left,a,targetA,token));
        guard.fence(left);
        try (var lease=guard.claimInstall(left)) { assertThrows(IllegalStateException.class,() -> guard.replaceSchema(left,a,targetA,token)); }
        assertThrows(IllegalArgumentException.class,() -> guard.replaceSchema(left,a,new Interval(DOMAIN,Set.of(),null,key(0x81)),token));
        assertThrows(IllegalStateException.class,() -> guard.replaceSchema(new Owner(left.groupId(),6),a,targetA,token));
        assertEquals(State.FENCED,guard.replaceSchema(left,a,targetA,token).state());
        assertEquals(Set.of(COLUMN),guard.get(right).interval().columns());
        assertThrows(IllegalArgumentException.class,() -> guard.register(owner(3),new Interval(DOMAIN,Set.of(),key(0x81),key(0x82)),State.RETIRED));
        assertThrows(IllegalStateException.class,() -> guard.completeSchemaTransition(token));
        assertThrows(IllegalStateException.class,() -> guard.beginSchemaTransition(new UUID(3,2),DOMAIN,Set.of(COLUMN),Set.of()));
        guard.replaceSchema(right,b,targetB,token);
        try (var lease=guard.claimInstall(right)) { assertThrows(IllegalStateException.class,() -> guard.completeSchemaTransition(token)); }
        guard.completeSchemaTransition(token);
        assertThrows(IllegalStateException.class,() -> guard.replaceSchema(left,a,targetA,token));
        guard.activate(left); guard.activate(right);
        assertThrows(IllegalStateException.class,() -> guard.register(owner(4),new Interval(DOMAIN,Set.of(),null,null),State.ACTIVE));
        assertDoesNotThrow(() -> guard.register(owner(5),new Interval(UUID.randomUUID(),Set.of(),null,null),State.ACTIVE));
    }

    @Test void schemaCreationStillRejectsCrossDomainPhysicalOverlap() {
        var guard=new RangeOwnership(); var extra=new UUID(0,3); var original=interval(null,null);
        guard.register(owner(1),original,State.FENCED);
        guard.register(owner(2),new Interval(new UUID(0,4),Set.of(extra),null,null),State.ACTIVE);
        var token=guard.beginSchemaTransition(new UUID(3,1),DOMAIN,Set.of(COLUMN),Set.of(COLUMN,extra));
        var target=new Interval(DOMAIN,Set.of(COLUMN,extra),null,null);
        assertThrows(IllegalStateException.class,() -> guard.validateSchemaReplacement(owner(1),original,target,token));
        assertThrows(IllegalStateException.class,() -> guard.replaceSchema(owner(1),original,target,token));
        assertEquals(original,guard.get(owner(1)).interval());
        var foreign=new RangeOwnership().beginSchemaTransition(new UUID(3,1),DOMAIN,Set.of(COLUMN),Set.of(COLUMN,extra));
        assertThrows(IllegalStateException.class,() -> guard.replaceSchema(owner(1),original,target,foreign));
    }

    @Test void schemaLeaseProtectsBothColumnSetsAndPublishesOnlyBeforeClose() {
        var guard=new RangeOwnership(); var extra=new UUID(0,3); var original=interval(null,null);
        var target=new Interval(DOMAIN,Set.of(COLUMN,extra),null,null);
        guard.register(owner(1),original,State.FENCED);
        var token=guard.beginSchemaTransition(new UUID(3,1),DOMAIN,Set.of(COLUMN),target.columns());
        var abandoned=guard.claimSchemaReplacement(owner(1),original,target,token);
        assertEquals(State.INSTALLING,guard.get(owner(1)).state());
        for (var column:Set.of(COLUMN,extra)) assertThrows(IllegalStateException.class,() -> guard.register(owner(2),new Interval(new UUID(0,4),Set.of(column),null,null),State.ACTIVE));
        assertThrows(IllegalStateException.class,() -> guard.claimInstall(owner(1)));
        assertThrows(IllegalStateException.class,() -> guard.completeSchemaTransition(token));
        abandoned.close(); assertEquals(original,guard.get(owner(1)).interval());
        assertThrows(IllegalStateException.class,abandoned::publish);
        var installed=guard.claimSchemaReplacement(owner(1),original,target,token);
        assertEquals(target,installed.publish().interval()); assertEquals(target,installed.publish().interval());
        assertEquals(State.INSTALLING,guard.get(owner(1)).state());
        assertThrows(IllegalStateException.class,() -> guard.activate(owner(1)));
        installed.close(); installed.close(); assertEquals(target,guard.get(owner(1)).interval());
        assertEquals(State.FENCED,guard.get(owner(1)).state()); guard.completeSchemaTransition(token);
        assertThrows(IllegalStateException.class,installed::publish);
        guard.activate(owner(1));
    }

    @Test void recoveryRegistersOnlyQualifiedMixedFencedGenerationsAndCanAdvanceAgain() {
        var guard=new RangeOwnership(); var extra=new UUID(0,3); var changed=Set.of(COLUMN,extra);
        var token=guard.beginSchemaTransition(new UUID(3,1),DOMAIN,Set.of(COLUMN),changed);
        var source=interval(key(0x80),null); var target=new Interval(DOMAIN,changed,null,key(0x80));
        assertThrows(IllegalArgumentException.class,() -> guard.registerSchema(owner(1),target,State.ACTIVE,token));
        guard.registerSchema(owner(1),target,State.FENCED,token); guard.registerSchema(owner(2),source,State.FENCED,token);
        guard.replaceSchema(owner(2),source,new Interval(DOMAIN,changed,key(0x80),null),token); guard.completeSchemaTransition(token);
        var next=guard.beginSchemaTransition(new UUID(3,2),DOMAIN,changed,Set.of(COLUMN));
        assertThrows(IllegalStateException.class,() -> guard.registerSchema(owner(3),target,State.RETIRED,token));
        guard.replaceSchema(owner(1),target,interval(null,key(0x80)),next);
        guard.replaceSchema(owner(2),new Interval(DOMAIN,changed,key(0x80),null),source,next);
        guard.completeSchemaTransition(next);
        assertEquals(Set.of(COLUMN),guard.get(owner(1)).interval().columns());
    }

    @Test void publishedDeletionStillReservesRemovedColumnUntilLeaseClose() {
        var guard=new RangeOwnership(); var kept=new UUID(0,3);
        var source=new Interval(DOMAIN,Set.of(COLUMN,kept),null,null);
        var target=new Interval(DOMAIN,Set.of(kept),null,null);
        var foreign=new Interval(new UUID(0,4),Set.of(COLUMN),null,null);
        guard.register(owner(1),source,State.FENCED);
        var token=guard.beginSchemaTransition(new UUID(3,1),DOMAIN,source.columns(),target.columns());
        var lease=guard.claimSchemaReplacement(owner(1),source,target,token); lease.publish();
        assertEquals(target,guard.get(owner(1)).interval());
        assertThrows(IllegalStateException.class,() -> guard.register(owner(2),foreign,State.ACTIVE));
        lease.close(); guard.completeSchemaTransition(token);
        assertDoesNotThrow(() -> guard.register(owner(2),foreign,State.ACTIVE));
    }

    @Test void retiredSourceDoesNotPinFutureDomainColumnSets() {
        var guard=new RangeOwnership(); var original=interval(null,null);
        guard.register(owner(1),original,State.FENCED); guard.retire(owner(1));
        var token=guard.beginSchemaTransition(new UUID(3,1),DOMAIN,Set.of(COLUMN),Set.of());
        guard.registerSchema(owner(2),new Interval(DOMAIN,Set.of(),null,null),State.FENCED,token);
        guard.completeSchemaTransition(token);
        assertEquals(State.RETIRED,guard.get(owner(1)).state());
        assertThrows(IllegalArgumentException.class,() -> guard.beginSchemaTransition(new UUID(3,2),DOMAIN,Set.of(),Set.of()));
    }

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
