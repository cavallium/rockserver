package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ReplicaPhysicalAPI;
import it.cavallium.rockserver.core.common.SstMaintenance;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual shared pools/CFs; no network or logical aggregation. */
class RatisNodePhysicalApiTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final RatisNodeMaintenance.Policy POLICY=new RatisNodeMaintenance.Policy(true,true,4,128,1024*1024);
    RatisNodeRuntime open(RatisNodeMultiDomainTest.Fixture f,RatisNodeRuntimeTest.FakeServer fake,RatisNodeRuntime.Startup startup)throws Exception{return RatisNodeRuntime.open(f.plan,f.env.nodeRoot,ConfigParser.parseDefault(),f.env.catalogue.epochs,f.clients,f.env.scheduler,f.env.timer,()->20L,f.settings(),startup,fake::create,f.schemas);}
    void start(RatisNodeRuntime node)throws Exception {node.start();RatisTopologySeedRuntimeTest.field(node,"poolPressure",RatisPoolPressureService.class).refresh().get(5,TimeUnit.SECONDS);}
    @Test void sharedPoolDiscoveryDeduplicatesRangeCfsAndReturnsPhysicalVectorWithoutChangingUserPrefix()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;
        try(var f=helper.new Fixture()){
            var fake=new RatisNodeRuntimeTest.FakeServer();ReplicaPhysicalAPI.Target stale;
            try(var node=open(f,fake,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT)){
                start(node);var api=node.physicalApi(POLICY);assertSame(api,node.physicalApi(POLICY));
                assertThrows(java.io.IOException.class,()->node.physicalApi(new RatisNodeMaintenance.Policy(false,true,4,128,1024*1024)));
                var scope=new ReplicaPhysicalAPI.Scope(null,null);
                var empty=api.observe(scope).get(5,TimeUnit.SECONDS);
                for(var value:empty){assertEquals(0,value.tables().tableCount());assertTrue(value.tables().comparators().isEmpty());assertEquals("leveldb.BytewiseComparator",value.actualCFComparator());}
                var cuts=new HashMap<UUID,ApplyPosition>();var receipts=new HashMap<UUID,Map<String,String>>();
                for(var domain:f.schemas){var range=f.plan.directory(domain.domainId()).ranges().getFirst();var store=f.store(fake,range.groupId());var column=domain.columnIds().iterator().next();var command=new DistributedCommand(range.groupId(),range.rangeEpoch(),domain.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{7})));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,store.applied().index()+1),command)).status());cuts.put(range.groupId(),store.applied());receipts.put(range.groupId(),receipts(store));}
                var targets=api.discover(scope).get(5,TimeUnit.SECONDS);assertEquals(2,targets.size(),"Two domains and three local ranges share exactly two actual CFs");assertEquals(1,targets.stream().map(ReplicaPhysicalAPI.Target::session).distinct().count());assertEquals(2,targets.stream().map(ReplicaPhysicalAPI.Target::nativeColumnId).distinct().count());stale=targets.getFirst();
                var pool=node.nativePool(RatisPoolFixtures.ID);var calls=new java.util.concurrent.atomic.AtomicInteger();pool.localMaintenanceObserver(ignored->calls.incrementAndGet());api.flush(scope).get(5,TimeUnit.SECONDS);assertEquals(1,calls.get(),"Whole shared pool flush occurs once including metadata CFs");
                var wrongId=new ReplicaPhysicalAPI.Target(stale.node(),stale.incarnation(),stale.process(),stale.pool(),stale.session(),stale.column(),stale.nativeColumnId()+1);int beforeId=calls.get();assertThrows(ExecutionException.class,()->api.metadata(wrongId,-1).get(5,TimeUnit.SECONDS));assertEquals(beforeId,calls.get());
                var observations=api.observe(scope).get(5,TimeUnit.SECONDS);assertEquals(2,observations.size());for(var value:observations){assertTrue(value.estimatedKeys()>0);assertTrue(value.tables().numEntries()>0);assertEquals("leveldb.BytewiseComparator",value.actualCFComparator());assertEquals(RatisNodeMaintenanceTest.normalProperties(pool,value.target().column()),value.tables());}
                api.compact(scope).get(5,TimeUnit.SECONDS);
                for(var target:targets){var metadata=api.metadata(target,-1).get(5,TimeUnit.SECONDS);assertEquals(target.nativeColumnId(),metadata.columnId());var file=metadata.files().stream().filter(x->x.level()>=1).findFirst().orElseThrow();var wrong=new SstMaintenance.Request(metadata.columnId(),"old-sst-session",List.of(file.name()),file.level(),file.pathId(),1024*1024,16*1024*1024,1,true);int before=calls.get();assertThrows(ExecutionException.class,()->api.compactFiles(target,wrong).get(5,TimeUnit.SECONDS));assertEquals(before,calls.get(),"Stale SST session rejects before actual compaction/inspection");}
                for(var entry:cuts.entrySet()){var store=f.store(fake,entry.getKey());assertEquals(entry.getValue(),store.applied());assertEquals(receipts.get(entry.getKey()),receipts(store),"Physical operations never change USER receipts");}pool.localMaintenanceObserver(ignored->{});
            }
            try(var node=open(f,new RatisNodeRuntimeTest.FakeServer(),RatisNodeRuntime.Startup.RECOVER)){start(node);var api=node.physicalApi(POLICY);assertThrows(IllegalArgumentException.class,()->api.metadata(stale,-1));}
        }
    }
    Map<String,String> receipts(it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore store)throws Exception {
        var result=new TreeMap<String,String>();try(var pin=store.captureForRecovery()){pin.visitReceipts((key,value)->result.put(HexFormat.of().formatHex(key),Base64.getEncoder().encodeToString(value)));}return result;
    }
    @Test void pressureDeniedQueuedPhysicalWorkStopsWithoutReleasingRunningNativeWorkEarly()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;
        try(var f=helper.new Fixture();var node=open(f,new RatisNodeRuntimeTest.FakeServer(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT)){
            start(node);var api=node.physicalApi(POLICY);var target=api.discover(new ReplicaPhysicalAPI.Scope(null,null)).get(5,TimeUnit.SECONDS).getFirst();var maintenance=node.localMaintenance(POLICY);
            var calls=new java.util.concurrent.atomic.AtomicInteger();node.nativePool(RatisPoolFixtures.ID).localMaintenanceObserver(ignored->calls.incrementAndGet());f.env.scheduler.setStoragePressure(true);
            var queued=api.metadata(target,-1);assertEquals(1,maintenance.pending());maintenance.stop().get(5,TimeUnit.SECONDS);assertEquals(0,maintenance.pending());assertThrows(ExecutionException.class,()->queued.get(5,TimeUnit.SECONDS));assertEquals(0,calls.get());node.close();assertEquals(0,calls.get());
            // A later clear sample on the borrowed scheduler can dispatch the stopped node's inert queue entry.
            f.env.scheduler.setStoragePressure(false);var dispatched=new CompletableFuture<Void>();f.env.scheduler.executor(it.cavallium.rockserver.core.common.WorkloadProfile.PHYSICAL_MAINTENANCE,it.cavallium.rockserver.core.common.OperationFamily.COMPACTION,Long.MAX_VALUE).execute(()->dispatched.complete(null));dispatched.get(5,TimeUnit.SECONDS);assertEquals(0,calls.get());assertEquals(0,maintenance.pending());
        }
    }
    @Test void cancellationAndNodeCloseRetainActualPhysicalLeaseUntilJniBoundaryReturns()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;
        try(var f=helper.new Fixture();var node=open(f,new RatisNodeRuntimeTest.FakeServer(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT)){
            start(node);var api=node.physicalApi(POLICY);var target=api.discover(new ReplicaPhysicalAPI.Scope(null,null)).get(5,TimeUnit.SECONDS).getFirst();
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);node.nativePool(RatisPoolFixtures.ID).localMaintenanceObserver(ignored->{entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("Physical native owner did not drain");}catch(InterruptedException failure){throw new AssertionError(failure);}});
            var maintenance=node.localMaintenance(POLICY);var observed=api.metadata(target,-1);try{assertTrue(entered.await(5,TimeUnit.SECONDS));assertTrue(observed.cancel(false));assertEquals(1,maintenance.pending());assertThrows(java.io.IOException.class,node::close);assertEquals(1,maintenance.pending());}
            finally{release.countDown();}node.close();
        }
    }
    @Test void scopeCreditSurvivesFirstRetainedResultAndCancelledSecondNativeInspection()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;
        var fake=new RatisNodeRuntimeTest.FakeServer();
        try(var f=helper.new Fixture();var node=open(f,fake,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT)) {
            start(node);
            var api=node.physicalApi(POLICY);var maintenance=node.localMaintenance(POLICY);
            for(var domain:f.schemas) {
                var range=f.plan.directory(domain.domainId()).ranges().getFirst();var store=f.store(fake,range.groupId());
                var command=new DistributedCommand(range.groupId(),range.rangeEpoch(),domain.digest(),new OperationId(UUID.randomUUID(),1,0),
                        List.of(new Mutation(Mutation.Kind.PUT,domain.columnIds().iterator().next(),new byte[]{1},new byte[]{7})));
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,store.applied().index()+1),command)).status());
            }
            api.flush(new ReplicaPhysicalAPI.Scope(null,null)).get(5,TimeUnit.SECONDS);
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var calls=new java.util.concurrent.atomic.AtomicInteger();
            var pool=node.nativePool(RatisPoolFixtures.ID);
            pool.localMaintenanceObserver(column->{if(calls.incrementAndGet()==2){entered.countDown();RatisTopologyFreezeTest.await(release);}});
            var scope=new ReplicaPhysicalAPI.Scope(null,null);
            var operation=api.observe(scope);
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS));
                assertEquals(2,calls.get(),"First CF result remains retained while second inspection owns native lifetime");
                assertEquals(POLICY.maximumMetadataBytes(),maintenance.observationBytes());
                assertThrows(IllegalStateException.class,()->api.observe(scope));
                assertTrue(operation.cancel(false));
                assertEquals(POLICY.maximumMetadataBytes(),maintenance.observationBytes());
                assertEquals(1,maintenance.pending());
                var stopped=maintenance.stop();assertFalse(stopped.isDone());
                release.countDown();stopped.get(5,TimeUnit.SECONDS);
                assertEquals(0,maintenance.pending());assertEquals(0,maintenance.observationBytes());
            } finally {release.countDown();pool.localMaintenanceObserver(ignored->{});maintenance.stop().get(5,TimeUnit.SECONDS);}
        }
    }

    @Test void queuedObservationStopRefundsCreditWithoutEnteringNativeInspection()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;
        try(var f=helper.new Fixture();var node=open(f,new RatisNodeRuntimeTest.FakeServer(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT)) {
            start(node);var api=node.physicalApi(POLICY);var maintenance=node.localMaintenance(POLICY);
            var calls=new java.util.concurrent.atomic.AtomicInteger();node.nativePool(RatisPoolFixtures.ID).localMaintenanceObserver(column->calls.incrementAndGet());
            f.env.scheduler.setStoragePressure(true);
            var operation=api.observe(new ReplicaPhysicalAPI.Scope(null,null));
            assertEquals(POLICY.maximumMetadataBytes(),maintenance.observationBytes());assertEquals(1,maintenance.pending());
            maintenance.stop().get(5,TimeUnit.SECONDS);
            assertThrows(ExecutionException.class,()->operation.get(5,TimeUnit.SECONDS));
            assertEquals(0,maintenance.observationBytes());assertEquals(0,maintenance.pending());assertEquals(0,calls.get());
            f.env.scheduler.setStoragePressure(false);
            var drained=new CompletableFuture<Void>();f.env.scheduler.executor(it.cavallium.rockserver.core.common.WorkloadProfile.PHYSICAL_MAINTENANCE,
                    it.cavallium.rockserver.core.common.OperationFamily.COMPACTION,Long.MAX_VALUE).execute(()->drained.complete(null));
            drained.get(5,TimeUnit.SECONDS);assertEquals(0,calls.get());
        }
    }

    @Test void scopeThatCannotRetainEveryResponseRejectsBeforeFirstInspection()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;
        try(var f=helper.new Fixture();var node=open(f,new RatisNodeRuntimeTest.FakeServer(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT)) {
            start(node);
            var policy=new RatisNodeMaintenance.Policy(true,true,4,128,16*1024);
            var api=node.physicalApi(policy);var maintenance=node.localMaintenance(policy);
            var calls=new java.util.concurrent.atomic.AtomicInteger();node.nativePool(RatisPoolFixtures.ID).localMaintenanceObserver(column->calls.incrementAndGet());
            assertThrows(ExecutionException.class,()->api.observe(new ReplicaPhysicalAPI.Scope(null,null)).get(5,TimeUnit.SECONDS));
            assertEquals(0,calls.get());assertEquals(0,maintenance.observationBytes());assertEquals(0,maintenance.pending());
        }
    }
}
