package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.SstMaintenance;
import it.cavallium.rockserver.core.common.distributed.DistributedCommandCodec;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
class RatisNodeMaintenanceTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static final UUID NODE=new UUID(0,500),INCARNATION=new UUID(0,501);
    private static final RatisNodeMaintenance.Policy POLICY=new RatisNodeMaintenance.Policy(true,true,1,128,1024*1024);
    private static final class Threads implements AutoCloseable {final RWScheduler scheduler=new RWScheduler(3,3,"maintenance");@Override public void close(){scheduler.dispose();}}
    @Test void physicalFilesPreviewExecutionAndPropertiesKeepRaftMarker() throws Exception {
        try(var f=new RatisTopologyFreezeTest.Fixture(root);var threads=new Threads()) {
            try(var maintenance=new RatisNodeMaintenance(NODE,INCARNATION,List.of(f.pool),threads.scheduler,POLICY)) {
                var target=maintenance.target(f.pool,f.column);
                f.apply(f.propose(DistributedCommandCodec.encode(f.put(0,9))),1).get(5,TimeUnit.SECONDS);
                var cut=f.store.applied();
                maintenance.flush(target).get(5,TimeUnit.SECONDS);
                var first=maintenance.metadata(target,-1).get(5,TimeUnit.SECONDS);
                assertFalse(first.files().isEmpty());
                assertTrue(maintenance.tableProperties(target).get(5,TimeUnit.SECONDS).entries()>0);
                maintenance.compact(target).get(5,TimeUnit.SECONDS);
                var metadata=maintenance.metadata(target,-1).get(5,TimeUnit.SECONDS);
                var file=metadata.files().stream().filter(value->value.level()>=1).findFirst().orElseThrow();
                var request=new SstMaintenance.Request(metadata.columnId(),metadata.session(),List.of(file.name()),file.level(),file.pathId(),1024*1024,16*1024*1024,1,false);
                assertFalse(maintenance.compactFiles(target,request).get(5,TimeUnit.SECONDS).executed());
                var execute=new SstMaintenance.Request(request.columnId(),request.session(),request.files(),request.level(),request.outputPathId(),request.outputFileSizeLimit(),request.maxInputBytes(),request.maxSubcompactions(),true);
                assertTrue(maintenance.compactFiles(target,execute).get(5,TimeUnit.SECONDS).executed());
                assertEquals(cut,f.store.applied());assertArrayEquals(new byte[]{9},f.store.readCurrentPoint(f.column,new byte[]{1}));
                assertEquals(0,f.store.activeSnapshotPins());assertEquals(0,f.reads.get());
                maintenance.stop().get(5,TimeUnit.SECONDS);
            }
        }
    }
    @Test void boundedScalarTableInspectionRejectsOversizedInventoryAndRetainsActualInspectionSlot()throws Exception {
        try(var f=new RatisTopologyFreezeTest.Fixture(root);var workers=Executors.newVirtualThreadPerTaskExecutor()){
            for(int n=0;n<2;n++){f.apply(f.propose(DistributedCommandCodec.encode(f.put(n,9+n))),n+1).get(5,TimeUnit.SECONDS);f.pool.flushLocal(f.pool.maintenanceSession(),f.column);}
            var cut=f.store.applied();var properties=f.pool.tablePropertiesLocal(f.pool.maintenanceSession(),f.column,128,1024*1024);
            assertTrue(properties.files()>=2);assertTrue(properties.entries()>=2);assertTrue(properties.dataBytes()>0);
            assertThrows(org.rocksdb.RocksDBException.class,()->f.pool.tablePropertiesLocal(f.pool.maintenanceSession(),f.column,1,1024*1024));
            assertThrows(org.rocksdb.RocksDBException.class,()->f.pool.tablePropertiesLocal(f.pool.maintenanceSession(),f.column,128,1));
            assertEquals(cut,f.store.applied());assertArrayEquals(new byte[]{10},f.store.readCurrentPoint(f.column,new byte[]{1}));
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);f.pool.localMaintenanceObserver(column->{entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("inspection drain timeout");}catch(InterruptedException failed){throw new AssertionError(failed);}});
            var owner=workers.submit(()->f.pool.tablePropertiesLocal(f.pool.maintenanceSession(),f.column,128,1024*1024));
            try{assertTrue(entered.await(5,TimeUnit.SECONDS));assertThrows(java.io.IOException.class,()->f.pool.tablePropertiesLocal(f.pool.maintenanceSession(),f.column,128,1024*1024));assertFalse(f.pool.close(Duration.ofMillis(10)));release.countDown();assertEquals(properties,owner.get(5,TimeUnit.SECONDS));}
            finally{release.countDown();f.pool.localMaintenanceObserver(column->{});}
        }
    }
    @Test void staleNodeProcessColumnAndSessionRejectBeforeNativeWork() throws Exception {
        try(var f=new RatisTopologyFreezeTest.Fixture(root);var threads=new Threads()) {
            try(var maintenance=new RatisNodeMaintenance(NODE,INCARNATION,List.of(f.pool),threads.scheduler,POLICY);
                var other=new RatisNodeMaintenance(NODE,INCARNATION,List.of(f.pool),threads.scheduler,POLICY)) {
                var target=maintenance.target(f.pool,f.column);var calls=new java.util.concurrent.atomic.AtomicInteger();f.pool.localMaintenanceObserver(column->calls.incrementAndGet());
                assertThrows(IllegalArgumentException.class,()->other.flush(target));
                var wrongIncarnation=new RatisNodeMaintenance.Target(target.node(),UUID.randomUUID(),target.process(),target.pool(),target.column());
                assertThrows(IllegalArgumentException.class,()->maintenance.flush(wrongIncarnation));
                var wrongColumn=new RatisNodeMaintenance.Target(target.node(),target.incarnation(),target.process(),target.pool(),UUID.randomUUID());
                assertThrows(ExecutionException.class,()->maintenance.flush(wrongColumn).get(5,TimeUnit.SECONDS));
                assertThrows(IllegalArgumentException.class,()->f.pool.flushLocal(UUID.randomUUID(),f.column));
                assertEquals(0,calls.get());
                assertThrows(ExecutionException.class,()->maintenance.metadata(target,-1).thenCompose(meta->maintenance.compactFiles(target,new SstMaintenance.Request(meta.columnId(),"foreign-session",List.of("1.sst"),1,0,1024,1024,1,false))).get(5,TimeUnit.SECONDS));
                maintenance.stop().get(5,TimeUnit.SECONDS);other.stop().get(5,TimeUnit.SECONDS);
            }
        }
    }
    @Test void cancelledObserverAndPoolCloseRetainActualNativeOwnership() throws Exception {
        try(var f=new RatisTopologyFreezeTest.Fixture(root);var threads=new Threads()) {
            try(var maintenance=new RatisNodeMaintenance(NODE,INCARNATION,List.of(f.pool),threads.scheduler,POLICY)) {
                var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
                f.pool.localMaintenanceObserver(column->{entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("release timeout");}catch(InterruptedException failure){Thread.currentThread().interrupt();throw new AssertionError(failure);}});
                var target=maintenance.target(f.pool,f.column);var operation=maintenance.flush(target);
                assertTrue(entered.await(5,TimeUnit.SECONDS));
                try {
                    assertTrue(operation.cancel(false));assertEquals(1,maintenance.pending());
                    assertThrows(IllegalStateException.class,()->maintenance.metadata(target,-1));
                    var stopped=maintenance.stop();assertFalse(stopped.isDone());
                    assertFalse(f.pool.close(Duration.ofMillis(10)));assertEquals(1,maintenance.pending());
                    release.countDown();stopped.get(5,TimeUnit.SECONDS);assertEquals(0,maintenance.pending());
                } finally {release.countDown();maintenance.stop().get(5,TimeUnit.SECONDS);}
            }
        }
    }
}
