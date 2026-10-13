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
    @Test void stableClientIdentityIsPartOfBoundedRouteAndCannotAliasAnotherIssuer() throws Exception {
        var closes=new AtomicInteger();var ids=new java.util.ArrayList<UUID>();UUID first=UUID.randomUUID(),second=UUID.randomUUID();
        try(var pool=new RatisClientPool(1,range->{throw new AssertionError("Identity route required");},null,(range,id)->{ids.add(id);return client(closes);})) {
            RaftClient original;
            try(var one=pool.acquireClient(route(1,1,1,1),first);var same=pool.acquireClient(route(1,1,1,1),first)) {
                original=one.client();assertSame(original,same.client());assertThrows(IOException.class,()->pool.acquireClient(route(1,1,1,1),second));
            }
            try(var replacement=pool.acquireClient(route(1,1,1,1),second)){assertNotSame(original,replacement.client());}
            assertEquals(List.of(first,second),ids);assertEquals(1,closes.get());
        }
        assertEquals(2,closes.get());
    }
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
    @Test void ownerAndOrdinaryClientsShareCapacityButNeverShareRouteKeysOrFactories() throws Exception {
        var ordinaryOpens=new AtomicInteger(); var ownerOpens=new AtomicInteger(); var closes=new AtomicInteger(); var route=route(1,1,1,1);
        try (var pool=new RatisClientPool(2,descriptor -> { ordinaryOpens.incrementAndGet(); return client(closes); },(descriptor,owner) -> { assertTrue(descriptor.replicas().contains(owner)); ownerOpens.incrementAndGet(); return client(closes); })) {
            var ordinary=pool.acquire(route);
            try (var owner=pool.acquireOwner(route,route.replicas().getFirst()); var same=pool.acquireOwner(route,route.replicas().getFirst())) {
                assertSame(owner.client(),same.client()); assertNotSame(ordinary.client(),owner.client()); assertEquals(1,ordinaryOpens.get()); assertEquals(1,ownerOpens.get());
                var nextRoute=route(2,1,1,1); assertThrows(IOException.class,() -> pool.acquireOwner(nextRoute,nextRoute.replicas().getFirst()));
                ordinary.close();
                try (var next=pool.acquireOwner(nextRoute,nextRoute.replicas().getFirst())) {
                    assertEquals(1,closes.get()); assertNotSame(owner.client(),next.client()); assertNotNull(owner.client());
                    assertThrows(IOException.class,() -> pool.acquire(route(3,1,1,1)));
                }
            } finally { ordinary.close(); }
            assertEquals(1,ordinaryOpens.get()); assertEquals(2,ownerOpens.get());
        }
        assertEquals(3,closes.get());
    }

    @Test void ownerReuseTracksConfigurationAndFullReplicaIncarnationWithoutOrdinaryDiscovery() throws Exception {
        var opens=new AtomicInteger(); var closes=new AtomicInteger();
        try (var pool=new RatisClientPool(1,descriptor -> { throw new AssertionError("Owner requests must never use ordinary discovery"); },(descriptor,owner) -> { opens.incrementAndGet(); return client(closes); })) {
            var original=route(1,1,1,1); RaftClient first;
            try (var lease=pool.acquireOwner(original,original.replicas().getFirst()); var same=pool.acquireOwner(original,original.replicas().getFirst())) { first=lease.client(); assertSame(first,same.client()); }
            for (var changed:List.of(route(1,1,2,1),route(1,1,2,2),route(1,2,2,2))) {
                try (var lease=pool.acquireOwner(changed,changed.replicas().getFirst())) { assertNotSame(first,lease.client()); }
            }
            var latest=route(1,2,2,2); assertThrows(IllegalArgumentException.class,() -> pool.acquireOwner(latest,original.replicas().getFirst()));
            assertEquals(4,opens.get()); assertEquals(3,closes.get());
        }
        assertEquals(4,closes.get());
    }

    @Test void ownerValidationRequiresEveryFrozenMemberFieldBeforeOpeningAnyClient() throws Exception {
        var opens=new AtomicInteger(); var route=route(1,1,1,1); var owner=route.replicas().getFirst();
        var changed=List.of(
                new ReplicaDescriptor(new UUID(1,2),owner.nodeIncarnation(),owner.replicaId(),owner.peerId(),owner.address(),owner.role()),
                new ReplicaDescriptor(owner.nodeId(),new UUID(2,2),owner.replicaId(),owner.peerId(),owner.address(),owner.role()),
                new ReplicaDescriptor(owner.nodeId(),owner.nodeIncarnation(),new UUID(3,2),owner.peerId(),owner.address(),owner.role()),
                new ReplicaDescriptor(owner.nodeId(),owner.nodeIncarnation(),owner.replicaId(),"n2",owner.address(),owner.role()),
                new ReplicaDescriptor(owner.nodeId(),owner.nodeIncarnation(),owner.replicaId(),owner.peerId(),"127.0.0.1:8002",owner.role()),
                new ReplicaDescriptor(owner.nodeId(),owner.nodeIncarnation(),owner.replicaId(),owner.peerId(),owner.address(),ReplicaDescriptor.Role.LISTENER));
        try (var pool=new RatisClientPool(1,descriptor -> { throw new AssertionError("No ordinary discovery"); },(descriptor,target) -> { opens.incrementAndGet(); return client(new AtomicInteger()); })) {
            for (var target:changed) assertThrows(IllegalArgumentException.class,() -> pool.acquireOwner(route,target));
            assertEquals(0,opens.get());
        }
    }

    @Test void legacyPoolRejectsOwnerModeBeforeEvictingAnIdleOrdinaryClient() throws Exception {
        var opens=new AtomicInteger(); var closes=new AtomicInteger(); var route=route(1,1,1,1);
        try (var pool=new RatisClientPool(1,descriptor -> { opens.incrementAndGet(); return client(closes); })) {
            RaftClient original; try (var lease=pool.acquire(route)) { original=lease.client(); }
            assertThrows(IOException.class,() -> pool.acquireOwner(route,route.replicas().getFirst())); assertEquals(0,closes.get());
            try (var lease=pool.acquire(route)) { assertSame(original,lease.client()); } assertEquals(1,opens.get());
        }
        assertEquals(1,closes.get());
    }

    @Test void blockedOwnerCreationIsSharedAndShutdownRetainsTheActualResourceUntilLeasesDrain() throws Exception {
        var entered=new CountDownLatch(1); var proceed=new CountDownLatch(1); var opens=new AtomicInteger(); var closes=new AtomicInteger(); var route=route(1,1,1,1);
        var pool=new RatisClientPool(1,descriptor -> { throw new AssertionError("No ordinary discovery"); },(descriptor,owner) -> {
            opens.incrementAndGet(); entered.countDown();
            try { if (!proceed.await(5,TimeUnit.SECONDS)) throw new IOException("fixture timeout"); } catch (InterruptedException failure) { throw new IOException(failure); }
            return client(closes);
        });
        try (var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var first=executor.submit(() -> pool.acquireOwner(route,route.replicas().getFirst())); assertTrue(entered.await(5,TimeUnit.SECONDS));
            var second=executor.submit(() -> pool.acquireOwner(route,route.replicas().getFirst()));
            try {
                executor.submit(() -> assertThrows(IOException.class,() -> pool.acquire(route(2,1,1,1)))).get(1,TimeUnit.SECONDS);
                assertEquals(0,closes.get()); assertEquals(1,opens.get());
            } finally { proceed.countDown(); }
            try (var a=first.get(5,TimeUnit.SECONDS); var b=second.get(5,TimeUnit.SECONDS)) {
                assertSame(a.client(),b.client()); assertNotNull(a.client()); assertThrows(IOException.class,pool::close);
                assertThrows(IOException.class,() -> pool.acquireOwner(route,route.replicas().getFirst())); assertEquals(0,closes.get());
            }
        } finally { proceed.countDown(); pool.close(); }
        assertEquals(1,closes.get());
    }

    @Test void failedOwnerSetupReclaimsSlotAndFailedOwnerRetirementRetainsTheResource() throws Exception {
        var attempts=new AtomicInteger(); var closes=new AtomicInteger(); var route=route(1,1,1,1);
        try (var pool=new RatisClientPool(1,descriptor -> client(closes),(descriptor,owner) -> { if (attempts.incrementAndGet()==1) throw new IOException("owner setup failed"); return client(closes); })) {
            assertThrows(IOException.class,() -> pool.acquireOwner(route,route.replicas().getFirst()));
            try (var owner=pool.acquireOwner(route,route.replicas().getFirst())) { assertNotNull(owner.client()); }
        }
        assertEquals(2,attempts.get()); assertEquals(1,closes.get());
        var ordinaryOpens=new AtomicInteger(); var releases=new AtomicInteger();
        var pool=new RatisClientPool(1,descriptor -> { ordinaryOpens.incrementAndGet(); return client(closes); },(descriptor,owner) -> client(() -> { if (releases.incrementAndGet()==1) throw new IOException("owner close failed"); }));
        try (var owner=pool.acquireOwner(route,route.replicas().getFirst())) { assertNotNull(owner.client()); }
        assertThrows(IOException.class,() -> pool.acquire(route(2,1,1,1))); assertEquals(0,ordinaryOpens.get());
        assertThrows(IOException.class,() -> pool.acquireOwner(route,route.replicas().getFirst())); pool.close(); pool.close(); assertEquals(2,releases.get());
    }

    @Test void snapshotAsyncWaiterReturnsBeforeAnotherOpeningAndSharesAggregateCapacity()throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var closes=new AtomicInteger();var opens=new AtomicInteger();
        try(var executor=Executors.newVirtualThreadPerTaskExecutor();var pool=new RatisClientPool(1,r->client(closes),null,null,r->{opens.incrementAndGet();entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new IOException("fixture timeout");}catch(InterruptedException failure){throw new IOException(failure);}return client(closes);},(r,o)->client(closes))){var opening=executor.submit(()->pool.acquireSnapshotAsync(route(1,1,1,1),null));assertTrue(entered.await(5,TimeUnit.SECONDS));var second=pool.acquireSnapshotAsync(route(1,1,1,1),null);assertFalse(second.isDone());assertThrows(IOException.class,()->pool.acquire(route(2,1,1,1)));assertEquals(1,opens.get());release.countDown();try(var a=opening.get(5,TimeUnit.SECONDS).get(5,TimeUnit.SECONDS);var b=second.get(5,TimeUnit.SECONDS)){assertSame(a.client(),b.client());}}
        assertEquals(1,closes.get());
    }
    @Test void asynchronousIdleRetirementKeepsReplacementAndCancelledWaiterChargedUntilActualClose()throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var closes=new AtomicInteger();var opens=new AtomicInteger();
        var pool=new RatisClientPool(1,r->client(closes),null,null,r->{int n=opens.incrementAndGet();return client(()->{if(n==1){entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new IOException("fixture timeout");}catch(InterruptedException failure){throw new IOException(failure);}}closes.incrementAndGet();});},(r,o)->client(closes));
        try{try(var first=pool.acquireSnapshotAsync(route(1,1,1,1),null).get(5,TimeUnit.SECONDS)){}var next=pool.acquireSnapshotAsync(route(2,1,1,1),null);assertTrue(entered.await(5,TimeUnit.SECONDS));assertFalse(next.isDone());assertTrue(next.cancel(false));assertEquals(1,opens.get());assertThrows(IOException.class,pool::close);assertEquals(0,closes.get());release.countDown();for(int i=0;i<100&&opens.get()!=2;i++)Thread.sleep(10);assertEquals(2,opens.get());for(int i=0;i<100;i++){try{pool.close();break;}catch(IOException active){if(i==99)throw active;Thread.sleep(10);}}assertEquals(2,closes.get());}finally{release.countDown();pool.close();}
    }

    @Test void interruptedSyncEvictionCannotFreePhysicalCapacityBeforeOldCloseSettles()throws Exception {
        var closingOld=new CountDownLatch(1);var releaseOld=new CountDownLatch(1);var callerExited=new CountDownLatch(1);var replacementOpened=new CountDownLatch(1);var opens=new AtomicInteger();var closes=new AtomicInteger();var callerFailure=new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var pool=new RatisClientPool(1,r->{int n=opens.incrementAndGet();if(n==2)replacementOpened.countDown();return client(()->{if(n==1){closingOld.countDown();try{if(!releaseOld.await(5,TimeUnit.SECONDS))throw new IOException("fixture timeout");}catch(InterruptedException failure){Thread.currentThread().interrupt();throw new IOException(failure);}}closes.incrementAndGet();});},null,null,r->client(closes),(r,o)->client(closes));
        try{try(var original=pool.acquire(route(1,1,1,1))){}var caller=Thread.ofVirtual().start(()->{try(var ignored=pool.acquire(route(2,1,1,1))){callerFailure.set(new AssertionError("Interrupted waiter obtained a lease"));}catch(Throwable failure){callerFailure.set(failure);}finally{callerExited.countDown();}});assertTrue(closingOld.await(5,TimeUnit.SECONDS));caller.interrupt();assertTrue(callerExited.await(5,TimeUnit.SECONDS));assertInstanceOf(java.io.InterruptedIOException.class,callerFailure.get());assertEquals(1,opens.get());assertEquals(0,closes.get());assertThrows(IOException.class,()->pool.acquire(route(3,1,1,1)));assertThrows(IOException.class,()->RatisRoutedClient.awaitSnapshot(pool.acquireSnapshotAsync(route(3,1,1,1),null)));assertEquals(1,opens.get());assertThrows(IOException.class,pool::close);releaseOld.countDown();assertTrue(replacementOpened.await(5,TimeUnit.SECONDS));for(int n=0;n<100;n++){try{pool.close();break;}catch(IOException actualOwner){if(n==99)throw actualOwner;Thread.sleep(10);}}assertEquals(2,closes.get());}finally{releaseOld.countDown();pool.close();}
    }

}
