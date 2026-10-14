package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.SstMaintenance;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool;
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
    static it.cavallium.rockserver.core.common.ColumnTableProperties normalProperties(
            it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool pool,UUID column)throws Exception {
        var loaded=RatisTopologySeedRuntimeTest.field(pool,"loaded",it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader.LoadedDb.class);
        @SuppressWarnings("unchecked")
        var handles=(java.util.Map<String,org.rocksdb.ColumnFamilyHandle>)RatisTopologySeedRuntimeTest.field(pool,"handles",java.util.Map.class);
        long id=pool.physicalColumnId(pool.maintenanceSession(),column);
        var handle=handles.values().stream().filter(h->Integer.toUnsignedLong(h.getID())==id).findFirst().orElseThrow();
        var aggregate=new it.cavallium.rockserver.core.impl.TablePropertiesAccumulator();
        for(var table:loaded.db().get().getPropertiesOfAllTables(handle).values())aggregate.add(table);
        return aggregate.result();
    }
    @Test void physicalFilesPreviewExecutionAndPropertiesKeepRaftMarker() throws Exception {
        try(var f=new RatisTopologyFreezeTest.Fixture(root);var threads=new Threads()) {
            try(var maintenance=new RatisNodeMaintenance(NODE,INCARNATION,List.of(f.pool),threads.scheduler,POLICY)) {
                var target=maintenance.target(f.pool,f.column);
                f.apply(f.propose(DistributedCommandCodec.encode(f.put(0,9))),1).get(5,TimeUnit.SECONDS);
                var cut=f.store.applied();
                maintenance.flush(target).get(5,TimeUnit.SECONDS);
                var first=maintenance.metadata(target,-1).get(5,TimeUnit.SECONDS);
                assertFalse(first.files().isEmpty());
                assertTrue(maintenance.tableProperties(target).get(5,TimeUnit.SECONDS).numEntries()>0);
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
    @Test void boundedFullTableInspectionRejectsOversizedInventoryAndRetainsActualInspectionSlot()throws Exception {
        try(var f=new RatisTopologyFreezeTest.Fixture(root);var workers=Executors.newVirtualThreadPerTaskExecutor()){
            for(int n=0;n<2;n++){f.apply(f.propose(DistributedCommandCodec.encode(f.put(n,9+n))),n+1).get(5,TimeUnit.SECONDS);f.pool.flushLocal(f.pool.maintenanceSession(),f.column);}
            var cut=f.store.applied();var properties=f.pool.tablePropertiesLocal(f.pool.maintenanceSession(),f.column,128,1024*1024);
            assertTrue(properties.tableCount()>=2);assertTrue(properties.numEntries()>=2);assertTrue(properties.dataSize()>0);
            assertEquals(normalProperties(f.pool,f.column),properties,"All 29 portable fields match the normal native projection");
            assertThrows(org.rocksdb.RocksDBException.class,()->f.pool.tablePropertiesLocal(f.pool.maintenanceSession(),f.column,1,1024*1024));
            assertThrows(IllegalArgumentException.class,()->f.pool.tablePropertiesLocal(f.pool.maintenanceSession(),f.column,128,1));
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
                var wrongIncarnation=new RatisNodeMaintenance.Target(target.node(),UUID.randomUUID(),target.process(),target.pool(),target.column(),target.nativeColumnId());
                assertThrows(IllegalArgumentException.class,()->maintenance.flush(wrongIncarnation));
                var wrongColumn=new RatisNodeMaintenance.Target(target.node(),target.incarnation(),target.process(),target.pool(),UUID.randomUUID(),target.nativeColumnId());
                assertThrows(ExecutionException.class,()->maintenance.flush(wrongColumn).get(5,TimeUnit.SECONDS));
                assertThrows(IllegalArgumentException.class,()->f.pool.flushLocal(UUID.randomUUID(),f.column));
                var wrongId=new RatisNodeMaintenance.Target(target.node(),target.incarnation(),target.process(),target.pool(),target.column(),target.nativeColumnId()+1);
                assertThrows(IllegalArgumentException.class,()->other.observe(target));
                assertThrows(ExecutionException.class,()->maintenance.observe(wrongColumn).get(5,TimeUnit.SECONDS));
                assertThrows(ExecutionException.class,()->maintenance.observe(wrongId).get(5,TimeUnit.SECONDS));
                assertEquals(0,calls.get());
                assertEquals(0,maintenance.observationBytes());
                assertThrows(ExecutionException.class,()->maintenance.metadata(target,-1).thenCompose(meta->maintenance.compactFiles(target,new SstMaintenance.Request(meta.columnId(),"foreign-session",List.of("1.sst"),1,0,1024,1024,1,false))).get(5,TimeUnit.SECONDS));
                maintenance.stop().get(5,TimeUnit.SECONDS);other.stop().get(5,TimeUnit.SECONDS);
            }
        }
    }
    @Test void cancelledObserverAndPoolCloseRetainActualNativeOwnership() throws Exception {
        try(var f=new RatisTopologyFreezeTest.Fixture(root);var threads=new Threads()) {
            try(var maintenance=new RatisNodeMaintenance(NODE,INCARNATION,List.of(f.pool),threads.scheduler,POLICY)) {
                var target=maintenance.target(f.pool,f.column);
                var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
                f.pool.localMaintenanceObserver(column->{entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("release timeout");}catch(InterruptedException failure){Thread.currentThread().interrupt();throw new AssertionError(failure);}});
                var operation=maintenance.flush(target);
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
    @Test void fullProjectionMatchesNormalPropertiesForMultipleSstsMergesAndRangeDeletes()throws Exception {
        UUID column=new UUID(0,601),group=new UUID(0,602),domain=new UUID(0,603);
        byte[] jar=RatisMergeFrontendTest.jar();
        var operator=new DistributedSchema.Operator("property-append",1,RatisMergeFrontendTest.AppendOperator.class.getName(),
                OperatorUpload.sha256(jar),1,1,"append-contract",1);
        var shape=new it.cavallium.rockserver.core.common.ColumnSchema(it.unimi.dsi.fastutil.ints.IntList.of(1),
                it.unimi.dsi.fastutil.objects.ObjectList.of(),true,operator.name(),operator.version(),operator.className());
        var schema=new DistributedSchema(new UUID(0,604),new UUID(0,605),domain,1,0,
                List.of(new DistributedSchema.Column(1,column,"data",shape,operator)));
        var path=root.resolve("properties-merge");
        DistributedStoragePool.stageOperator(path,operator,new it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract("append-contract",1,true),jar);
        try(var pool=DistributedStoragePool.open(path,it.cavallium.rockserver.core.config.ConfigParser.parseDefault(),
                org.slf4j.LoggerFactory.getLogger(getClass()),List.of(schema),java.util.Map.of(column,"data"),java.util.Map.of());
            var threads=new Threads();
            var maintenance=new RatisNodeMaintenance(NODE,INCARNATION,List.of(pool),threads.scheduler,POLICY)) {
            var store=pool.openRange(new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Owner(group,1),
                    new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Interval(domain,java.util.Set.of(column),null,null),
                    schema.digest(),java.util.Map.of(column,"data"),java.util.Set.of(column));
            store.enableLogicalSchema(schema);
            var target=maintenance.target(pool,column);
            var empty=maintenance.observe(target).get(5,TimeUnit.SECONDS);
            assertEquals(0,empty.tables().tableCount());assertTrue(empty.tables().comparators().isEmpty());
            assertEquals("leveldb.BytewiseComparator",empty.actualCFComparator());
            var mutations=List.of(
                    List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{7}),new Mutation(Mutation.Kind.PUT,column,new byte[]{3},new byte[]{8})),
                    List.of(new Mutation(Mutation.Kind.MERGE,column,new byte[]{2},new byte[]{9})),
                    List.of(new Mutation(Mutation.Kind.DELETE_RANGE,column,new byte[]{3},new byte[]{4}),new Mutation(Mutation.Kind.DELETE,column,new byte[]{1},null)));
            for(int i=0;i<mutations.size();i++) {
                var command=new DistributedCommand(group,1,schema.digest(),new OperationId(new UUID(0,606),1,i),mutations.get(i));
                var applied=store.applied();
                var next=new ApplyPosition(applied==null?1:Math.max(1,applied.term()),applied==null?0:Math.addExact(applied.index(),1));
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(next,command)).status());
                maintenance.flush(target).get(5,TimeUnit.SECONDS);
            }
            var cut=store.applied();
            var receiptReader=new RatisNodePhysicalApiTest();
            var receipts=receiptReader.receipts(store);
            var first=maintenance.observe(target).get(5,TimeUnit.SECONDS);
            assertTrue(first.tables().tableCount()>=3);
            assertTrue(first.tables().numMergeOperands()>0);assertTrue(first.tables().numRangeDeletions()>0);
            assertTrue(first.tables().numDeletions()>0);
            assertEquals(normalProperties(pool,column),first.tables());
            assertEquals(first,maintenance.observe(target).get(5,TimeUnit.SECONDS));
            assertEquals(cut,store.applied());assertEquals(receipts,receiptReader.receipts(store));
            assertArrayEquals(new byte[]{9},store.readCurrentPoint(column,new byte[]{2}));
            assertEquals(0,maintenance.observationBytes());
        }
    }

    @Test void observationPlannerReservesWholeScopeAndTightensCapsWithoutRaisingPolicy() {
        var one=DistributedStoragePool.TablePropertyLimits.forScope(1,128,1024*1024);
        var many=DistributedStoragePool.TablePropertyLimits.forScope(3,128,1024*1024);
        assertTrue(one.files()<=128);assertTrue(many.files()<one.files());
        assertTrue(many.scopeBytes()<=1024*1024);
        assertEquals(2*many.inventoryBytes()+3*many.projectionBytes()+8*many.blockBytes()+8*many.stringBytes()+4096,many.workingBytes());
        assertTrue(many.retainedBytes()>=3L*(4096+4096L*many.files()+2*many.totalStringBytes()+2*many.stringBytes()));
        assertThrows(IllegalArgumentException.class,()->DistributedStoragePool.TablePropertyLimits.forScope(65536,128,1024*1024));
        assertThrows(IllegalArgumentException.class,()->DistributedStoragePool.TablePropertyLimits.forScope(1,128,1));
    }
}
