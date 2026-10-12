package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

class RatisExternalSnapshotPinsTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final Duration ttl=Duration.ofSeconds(10);
    final class Fixture implements AutoCloseable {
        final RatisSnapshotAdmissionTest helper=new RatisSnapshotAdmissionTest();final RatisSnapshotAdmissionTest.Home home;
        final DistributedStoragePool pool;final UUID column=new UUID(850,1),domain=new UUID(850,2);final List<ReplicatedRangeStore> stores=new ArrayList<>();final SnapshotFence fence;
        final AtomicLong clock=new AtomicLong();final AtomicInteger factories=new AtomicInteger();final List<RatisSnapshotPins> registries=new ArrayList<>();
        Fixture(String name)throws Exception {
            helper.root=root;helper.helper.root=root;home=helper.new Home(name+"-home");pool=DistributedStoragePool.open(root.resolve(name+"-data"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));pool.configureSnapshotAdmission(new PoolSnapshotAdmission.Limits(1,1L<<30,Duration.ofSeconds(60),0));
            var participants=new ArrayList<SnapshotFence.Participant>();for(int n=0;n<2;n++){var group=new UUID(851,n+1);byte[] lower=n==0?null:new byte[]{(byte)128},upper=n==0?new byte[]{(byte)128}:null;var store=pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(domain,Set.of(column),lower,upper),new byte[32],Map.of(column,"cohort-data"),Set.of());store.initializeConfiguration(new byte[]{1});store.applyNoop(new ApplyPosition(1,0));store.enableScopedHistory(RangeMutationHistory.Limits.defaults());store.apply(new ApplyPosition(1,1),write(store,n,0,n+1));stores.add(store);var scope=new TransactionScope(List.of(TransactionScope.Span.range(column,lower,upper)));participants.add(new SnapshotFence.Participant(group,group,1,new byte[32],new OperationId(new UUID(852,n+1),1,0),new OperationId(new UUID(852,n+1),1,1),scope));}
            fence=new SnapshotFence(helper.helper.cluster,helper.helper.database,domain,new UUID(853,1),home.f.groupId,1,new OperationId(new UUID(853,2),1,0),0,1,new byte[32],participants);
            for(int n=0;n<2;n++){var store=stores.get(n);store.applyGateInstall(new ApplyPosition(1,2),fence);registries.add(new RatisSnapshotPins(new UUID(854,n+1),store.ownership().owner().groupId(),RatisSnapshotPins.Limits.defaults(),clock::get,(homes,definition)->{factories.incrementAndGet();throw new AssertionError("External identity must not capture latest");}));}
        }
        byte[] key(int n){return new byte[]{(byte)(n==0?10:200)};}
        DistributedCommand write(ReplicatedRangeStore store,int n,long sequence,int value){return new DistributedCommand(store.ownership().owner().groupId(),1,new byte[32],new OperationId(new UUID(855,n+1),1,sequence),List.of(new Mutation(Mutation.Kind.PUT,column,key(n),new byte[]{(byte)value})));}
        RatisSnapshotCaptureRequest request(int n){return new RatisSnapshotCaptureRequest(fence,new UUID(854,n+1),new UUID(856,n+1),ttl);}
        DistributedStoragePool.SnapshotCohort capture()throws Exception{return pool.captureGateCohort(stores.stream().map(s->new DistributedStoragePool.GateCaptureRequest(s,fence)).toList());}
        ReplicatedRangeStore.ScopedSnapshot scoped(DistributedStoragePool.SnapshotCohort cohort,int n)throws Exception {var store=stores.get(n);return store.scopeCohortPin(cohort.pins().get(store.ownership().owner().groupId()),fence);}
        public void close()throws Exception{for(var registry:registries)registry.close();pool.close();home.close();}
    }
    @Test void allReservationsPrecedeOneNativeCutAndOrdinaryRetriesNeverCallFactory()throws Exception {
        try(var f=new Fixture("adopt")) {
            var reservations=new ArrayList<RatisSnapshotPins.ExternalReservation>();for(int n=0;n<2;n++){var request=f.request(n);var registry=f.registries.get(n);reservations.add(registry.reserveExternal(request,()->0));assertThrows(IOException.class,()->registry.capture(f.home.homes,f.fence,request.owner(),request.snapshotId(),ttl,()->0));}
            for(int n=0;n<2;n++)f.registries.get(n).markPhysicalStarted(reservations.get(n));try(var cohort=f.capture()) {
                var tokens=new ArrayList<SnapshotFenceStatus.Capture>();for(int n=0;n<2;n++){var registry=f.registries.get(n);var result=registry.publishExternal(reservations.get(n),f.scoped(cohort,n));tokens.add(result.token());assertNull(result.handle());assertEquals(result.token(),registry.capture(f.home.homes,f.fence,f.request(n).owner(),f.request(n).snapshotId(),ttl,()->0));assertArrayEquals(new byte[]{(byte)(n+1)},registry.get(result.token(),f.column,f.key(n)));}
                assertEquals(1,f.pool.snapshotAdmissionStats().pins());assertEquals(0,f.factories.get());var terminal=SnapshotFenceStatus.begin(f.fence);for(int n=0;n<2;n++)terminal=terminal.ready(f.stores.get(n).ownership().owner().groupId(),new ApplyPosition(1,2));for(int n=0;n<2;n++)terminal=terminal.capture(f.stores.get(n).ownership().owner().groupId(),tokens.get(n));terminal=terminal.complete();for(int n=0;n<2;n++){var store=f.stores.get(n);store.applyGateRelease(new ApplyPosition(1,3),terminal);store.apply(new ApplyPosition(1,4),f.write(store,n,1,9));assertArrayEquals(new byte[]{(byte)(n+1)},f.registries.get(n).get(tokens.get(n),f.column,f.key(n)));}
                for(int n=0;n<2;n++){try{f.registries.get(n).release(tokens.get(n));}catch(IOException draining){assertTrue(f.pool.snapshotAdmissionStats().pins()>0);}}for(var registry:f.registries)registry.expire();assertEquals(0,f.pool.snapshotAdmissionStats().pins());for(var registry:f.registries)assertEquals(0,registry.stats().pins());assertEquals(0,f.factories.get());
            }
        }
    }
    @Test void rollbackRefundsOnlyBeforePhysicalStartAndStartedFailureLeavesExactTombstone()throws Exception {
        try(var f=new Fixture("rollback")) {
            var registry=f.registries.getFirst();var request=f.request(0);assertThrows(RatisSnapshotLostException.class,()->registry.reserveExternal(new RatisSnapshotCaptureRequest(f.fence,UUID.randomUUID(),request.snapshotId(),ttl),()->0));assertThrows(IllegalArgumentException.class,()->registry.reserveExternal(new RatisSnapshotCaptureRequest(f.fence,request.owner(),request.snapshotId(),ttl,RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED),()->0));
            var rollback=registry.reserveExternal(request,()->0);registry.cancelExternal(rollback);assertEquals(0,registry.stats().entries());assertEquals(0,registry.stats().bytes());var started=registry.reserveExternal(request,()->0);long charge=registry.stats().bytes();registry.markPhysicalStarted(started);registry.cancelExternal(started);assertEquals(charge,registry.stats().bytes());assertThrows(IOException.class,()->registry.capture(f.home.homes,f.fence,request.owner(),request.snapshotId(),ttl,()->0));registry.failExternal(started);assertEquals(RatisSnapshotPins.TOMBSTONE_BYTES,registry.stats().bytes());assertThrows(RatisSnapshotLostException.class,()->registry.capture(f.home.homes,f.fence,request.owner(),request.snapshotId(),ttl,()->0));assertThrows(IllegalArgumentException.class,()->registry.reserveExternal(new RatisSnapshotCaptureRequest(f.fence,request.owner(),UUID.randomUUID(),ttl),()->0));assertThrows(IllegalArgumentException.class,()->registry.reserveExternal(new RatisSnapshotCaptureRequest(f.fence,request.owner(),request.snapshotId(),Duration.ofSeconds(11)),()->0));registry.retireBefore(2);assertThrows(RatisSnapshotLostException.class,()->registry.reserveExternal(request,()->2));assertEquals(0,f.factories.get());
        }
    }
    @Test void closedNativeAliasCannotPublishOrRetryAsHistoricalCapture()throws Exception {
        try(var f=new Fixture("closed")) {
            var registry=f.registries.getFirst();var reservation=registry.reserveExternal(f.request(0),()->0);registry.markPhysicalStarted(reservation);try(var cohort=f.capture()) {var pin=f.scoped(cohort,0);pin.close();assertThrows(RatisSnapshotLostException.class,()->registry.publishExternal(reservation,pin));assertThrows(RatisSnapshotLostException.class,()->registry.capture(f.home.homes,f.fence,f.request(0).owner(),f.request(0).snapshotId(),ttl,()->0));assertEquals(0,f.factories.get());}registry.expire();assertEquals(0,registry.stats().pins());assertEquals(RatisSnapshotPins.TOMBSTONE_BYTES,registry.stats().bytes());
        }
    }
    @Test void partialPublicationCancelAdoptsAndRetainsPinUntilActualNativeUseDrains()throws Exception {
        try(var f=new Fixture("partial");var executor=Executors.newSingleThreadExecutor()) {
            RatisSnapshotPins first=f.registries.get(0),second=f.registries.get(1);var a=first.reserveExternal(f.request(0),()->0);var b=second.reserveExternal(f.request(1),()->0);first.markPhysicalStarted(a);second.markPhysicalStarted(b);try(var cohort=f.capture()) {
                var token=first.publishExternal(a,f.scoped(cohort,0)).token();var pin=f.scoped(cohort,1);var entered=new CountDownLatch(1);var finish=new CountDownLatch(1);var nativeRead=executor.submit(()->{pin.visitColumn(f.column,null,null,(key,value)->{entered.countDown();try{if(!finish.await(10,TimeUnit.SECONDS))throw new IOException("Native drain timeout");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException(interrupted);}});return null;});assertTrue(entered.await(10,TimeUnit.SECONDS));long charge=second.stats().bytes();second.cancelExternal(b);assertThrows(RatisSnapshotLostException.class,()->second.publishExternal(b,pin));assertEquals(charge,second.stats().bytes());assertEquals(1,second.stats().pins());assertThrows(IOException.class,()->first.release(token));assertEquals(1,f.pool.snapshotAdmissionStats().pins());finish.countDown();nativeRead.get(10,TimeUnit.SECONDS);second.expire();first.expire();assertEquals(0,f.pool.snapshotAdmissionStats().pins());assertEquals(0,first.stats().pins());assertEquals(0,second.stats().pins());assertEquals(0,f.factories.get());assertThrows(RatisSnapshotLostException.class,()->second.capture(f.home.homes,f.fence,f.request(1).owner(),f.request(1).snapshotId(),ttl,()->0));
            }
        }
    }
}
