package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static it.cavallium.rockserver.distributed.ratis.RatisSnapshotCohortRpc.*;
import static org.junit.jupiter.api.Assertions.*;

class RatisSnapshotCohortServiceTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable, RatisSnapshotCohortService.Host {
        final RatisExternalSnapshotPinsTest helper=new RatisExternalSnapshotPinsTest();
        final RatisExternalSnapshotPinsTest.Fixture nativeState;
        final UUID node=new UUID(860,1),incarnation=new UUID(860,2),process=new UUID(860,3),poolIdentity=new UUID(860,4);
        final RWScheduler scheduler=RWScheduler.forTesting(1,1,1,4,1,"cohort-test");
        final RatisSnapshotCohortService service;
        final AtomicInteger qualifications=new AtomicInteger();
        volatile UUID poolIdentityOverride;volatile boolean changeAfterCapture;
        volatile CountDownLatch qualificationEntered,qualificationRelease;
        Fixture(String name)throws Exception {this(name,null,null);}
        Fixture(String name,RatisNodeResourceBudget shared,UUID identity)throws Exception {if(identity!=null)poolIdentityOverride=identity;helper.root=root;nativeState=helper.new Fixture(name);service=new RatisSnapshotCohortService(this,scheduler,RatisSnapshotCohortService.Limits.defaults(),nativeState.clock::get,shared==null?budget(Map.of(poolId(),new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(256L<<20,16),new RatisNodeResourceBudget.Lane(256L<<20,16))),256L<<20):shared);}
        public UUID nodeId(){return node;}public UUID nodeIncarnation(){return incarnation;}public UUID processIncarnation(){return process;}public UUID poolId(){return poolIdentityOverride==null?poolIdentity:poolIdentityOverride;}
        public DistributedStoragePool pool(){return nativeState.pool;}
        public PoolSnapshotAdmission.Limits poolLimits(){return new PoolSnapshotAdmission.Limits(1,1L<<30,java.time.Duration.ofSeconds(60),0);}
        public RatisSnapshotCohortService.Member member(UUID group,SnapshotFence fence,boolean ready)throws Exception {
            int index=-1;for(int n=0;n<2;n++)if(nativeState.stores.get(n).ownership().owner().groupId().equals(group))index=n;
            if(index<0)throw new IOException("Unknown actual group");var store=nativeState.stores.get(index);int read=ready?qualifications.incrementAndGet():0;
            if(ready && qualificationEntered!=null){qualificationEntered.countDown();if(!qualificationRelease.await(10,TimeUnit.SECONDS))throw new IOException("Qualification did not drain");}
            if(fence!=null && !Arrays.equals(fence.encode(),nativeState.fence.encode()))throw new IOException("Changed native gate");
            var cfg=store.configuration();return new RatisSnapshotCohortService.Member(store,nativeState.registries.get(index),new RatisSnapshotOwner(group,1,new byte[32],nativeState.request(index).owner(),"peer-"+index,null),new ReplicaDescriptor(node,incarnation,new UUID(861,index),"peer-"+index,"127.0.0.1:"+(18000+index),ReplicaDescriptor.Role.VOTER),changeAfterCapture && read>2?2:1,1,hash(cfg.data()),new ApplyPosition(1,2));
        }
        Request request(){var members=new ArrayList<RatisDatabaseBackupJournal.Member>();var captures=new HashMap<UUID,RatisSnapshotCaptureRequest>();for(int n=0;n<2;n++){var request=nativeState.request(n);var group=nativeState.stores.get(n).ownership().owner().groupId();members.add(new RatisDatabaseBackupJournal.Member(request.fence().fenceId(),group,request.snapshotId(),request.owner(),new UUID(861,n),request.ttl().toMillis(),hash(ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_CAPTURE,request.encode()))));captures.put(group,request);}return new Request(new RatisDatabaseBackupJournal.CohortRequest(new UUID(862,1),incarnation,process,poolId(),members),captures);}
        public boolean originalGone(Reference original){return false;}
        public void close()throws Exception {try{service.close();}finally{scheduler.dispose();nativeState.close();}}
    }
    static RatisNodeResourceBudget budget(Map<UUID,RatisNodeResourceBudget.Limits> pools,long bytes){return new RatisNodeResourceBudget(new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(bytes,16),new RatisNodeResourceBudget.Lane(bytes,16)),pools);}
    @Test void sharedNodeActionBudgetRejectsOtherPoolAndCancellationRetainsActualWork()throws Exception {
        UUID a=new UUID(864,1),b=new UUID(864,2);var limits=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(256L<<20,16),new RatisNodeResourceBudget.Lane(256L<<20,16));var graph=budget(Map.of(a,limits,b,limits),256L<<20);
        try(var first=new Fixture("node-action-a",graph,a);var second=new Fixture("node-action-b",graph,b)){
            var request=first.request();first.qualificationEntered=new CountDownLatch(1);first.qualificationRelease=new CountDownLatch(1);var future=first.service.query(request.cohort().members().getFirst().groupId(),CAPTURE,request(request));assertTrue(first.qualificationEntered.await(10,TimeUnit.SECONDS));long held=graph.stats().protectedLane().bytes();
            try(var pressure=graph.reserve(a,RatisNodeResourceBudget.Kind.PROTECTED,(256L<<20)-held,1)){
                var other=second.request();assertThrows(IOException.class,()->second.service.query(other.cohort().members().getFirst().groupId(),CAPTURE,request(other)));assertEquals(0,second.qualifications.get());assertEquals((256L<<20),graph.stats().protectedLane().bytes());future.cancel(false);assertThrows(IOException.class,first.service::close);assertEquals((256L<<20),graph.stats().protectedLane().bytes());
            }finally{first.qualificationRelease.countDown();}
            long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);while(graph.stats().protectedLane().bytes()!=0 && System.nanoTime()<until)Thread.onSpinWait();assertEquals(0,graph.stats().protectedLane().bytes());
        }
    }
    @Test void dictionaryRoundTripOneNativeCutExactRetryAndIndependentCancel()throws Exception {
        try(var f=new Fixture("service-cut")) {
            var request=f.request();var bytes=request(request);assertEquals(72+request.cohort().members().size()*124+4+f.nativeState.fence.encode().length,bytes.length);var decoded=request(bytes);assertArrayEquals(bytes,request(decoded));
            var result=f.service.capture(decoded);assertEquals(Phase.CAPTURED,result.phase());assertEquals(2,result.captures().size());assertEquals(1,f.pool().snapshotAdmissionStats().pins());assertEquals(0,f.nativeState.factories.get());
            for(int n=0;n<2;n++){var group=f.nativeState.stores.get(n).ownership().owner().groupId();assertArrayEquals(new byte[]{(byte)(n+1)},f.nativeState.registries.get(n).get(result.captures().get(group).token(),f.nativeState.column,f.nativeState.key(n)));}
            assertArrayEquals(reply(result),reply(f.service.capture(request)));assertArrayEquals(reply(result),reply(f.service.state(reference(request))));
            var released=f.service.cancel(reference(request));assertEquals(Phase.RELEASED,released.phase());assertEquals(0,f.pool().snapshotAdmissionStats().pins());assertEquals(Phase.RELEASED,f.service.capture(request).phase());assertEquals(0,f.nativeState.factories.get());
            byte[] changed=bytes.clone();changed[0]^=1;assertThrows(IllegalArgumentException.class,()->reply(reply(result),reference(request(changed))));
        }
    }
    @Test void changedQualificationAfterPhysicalCaptureCannotPublishAndCannotRecapture()throws Exception {
        try(var f=new Fixture("service-fenced")) {var request=f.request();f.changeAfterCapture=true;assertThrows(IOException.class,()->f.service.capture(request));assertEquals(0,f.pool().snapshotAdmissionStats().pins());assertEquals(Phase.RELEASED,f.service.capture(request).phase());assertEquals(0,f.nativeState.factories.get());for(var registry:f.nativeState.registries)assertEquals(0,registry.stats().pins());}
    }
    @Test void cancelReachesBusyCaptureAndChargesDrainOnlyAfterActualQualificationReturns()throws Exception {
        try(var f=new Fixture("service-cancel")) {var request=f.request();f.qualificationEntered=new CountDownLatch(1);f.qualificationRelease=new CountDownLatch(1);var future=f.service.query(request.cohort().members().getFirst().groupId(),CAPTURE,request(request));assertTrue(f.qualificationEntered.await(10,TimeUnit.SECONDS));var cancelled=reply(f.service.query(request.cohort().members().getFirst().groupId(),CANCEL,reference(reference(request))).get(10,TimeUnit.SECONDS),reference(request));assertEquals(Phase.DRAINING,cancelled.phase());assertFalse(future.isDone());assertThrows(IOException.class,f.service::close);f.qualificationRelease.countDown();assertThrows(ExecutionException.class,()->future.get(10,TimeUnit.SECONDS));assertEquals(0,f.pool().snapshotAdmissionStats().pins());assertEquals(0,f.nativeState.factories.get());}
    }
    @Test void originalProcessIdentityAndOriginalDeadlineNeverRenew()throws Exception {
        try(var f=new Fixture("service-expiry")){var request=f.request();f.service.capture(request);f.nativeState.clock.set(java.time.Duration.ofSeconds(10).toNanos());assertEquals(Phase.RELEASED,f.service.state(reference(request)).phase());assertEquals(Phase.RELEASED,f.service.capture(request).phase());var old=reference(request);assertThrows(RatisSnapshotLostException.class,()->f.service.state(new Reference(old.cohortId(),old.nodeIncarnation(),UUID.randomUUID(),old.poolId(),old.digest())));}
    }
}
