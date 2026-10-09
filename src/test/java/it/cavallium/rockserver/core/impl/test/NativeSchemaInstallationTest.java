package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

class NativeSchemaInstallationTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final UUID cluster=UUID.randomUUID(),database=UUID.randomUUID(),domain=UUID.randomUUID(),column=UUID.randomUUID(),group=UUID.randomUUID();
    DistributedSchema source() { return new DistributedSchema(cluster,database,domain,0,0,List.of(new DistributedSchema.Column(1,column,"data",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null))); }
    SchemaChange.Definition change(DistributedSchema source,DistributedSchema target,List<RangeDescriptor> ranges) {
        var directory=new RangeDirectory(cluster,database,domain,source.schemaEpoch(),source.digest(),0,source.columnIds(),0,ranges);
        return new SchemaChange.Definition(UUID.randomUUID(),new OperationId(UUID.randomUUID(),1,0),directory,source,target);
    }
    RangeDescriptor descriptor(UUID group,byte[] lo,byte[] hi) {
        var replica=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"peer","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        return new RangeDescriptor(UUID.randomUUID(),group,1,1,lo,hi,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
    }
    DistributedStoragePool open(Path path) { return DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass())); }
    ReplicatedRangeStore store(DistributedStoragePool pool,UUID group,DistributedSchema schema,byte[] lo,byte[] hi) throws Exception {
        return pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(domain,schema.columnIds(),lo,hi),schema.digest(),Map.of(column,"data"),Set.of());
    }
    @Test void lastColumnDeletionRetainsReceiptAndReopensFromOriginalBootstrap() throws Exception {
        var source=source(); var target=new DistributedSchema(cluster,database,domain,1,0,List.of());
        var change=change(source,target,List.of(descriptor(group,null,null))); var path=root.resolve("last");
        var command=new DistributedCommand(group,1,source.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{7})));
        NativeSchemaInstallation installed; byte[] result;
        try(var pool=open(path)) {
            var field=DistributedStoragePool.class.getDeclaredField("loaded"); field.setAccessible(true); var loaded=(RocksDBLoader.LoadedDb)field.get(pool);
            int poolOwnedReferences=loaded.refs().asList().size();
            var store=store(pool,group,source,null,null);
            var columnOptions=loaded.definitiveColumnFamilyOptionsMap().get("data"); assertTrue(columnOptions.isOwningHandle());
            assertEquals(poolOwnedReferences,loaded.refs().asList().size(),"Dynamic options have a separate owner");
            store.enableLogicalSchema(source); result=store.apply(new ApplyPosition(1,0),command);
            var closedPin=store.capture(); closedPin.close();
            var fence=store.applySchemaFence(new ApplyPosition(1,1),change.header());
            installed=pool.installSchema(store,new ApplyPosition(1,2),change,Map.of(),Set.of());
            assertEquals(Set.of(),store.ownership().interval().columns()); assertArrayEquals(target.digest(),store.schemaDigest());
            assertFalse(columnOptions.isOwningHandle(),"Drained ordinary dynamic column options retire after DROP");
            if(loaded.cache()!=null) assertTrue(loaded.cache().isOwningHandle(),"Dropping one column must retain shared caches");
            store.validate(command); assertArrayEquals(result,store.apply(new ApplyPosition(1,3),command));
            assertArrayEquals(installed.encode(),pool.installSchema(store,new ApplyPosition(1,4),change,Map.of(),Set.of()).encode());
            assertEquals(fence,store.applySchemaFence(new ApplyPosition(1,5),change.header()));
            assertArrayEquals(source.digest(),closedPin.schemaDigest()); assertEquals(Set.of(column),closedPin.columns()); assertEquals(Set.of(column),closedPin.interval().columns());
            assertThrows(IllegalStateException.class,store::capture);
            var image=root.resolve("empty.image"); var limits=new NativeRangeImage.Limits(3*1024*1024,32*1024*1024,256);
            NativeRangeImage.write(image,store,limits); assertEquals(Set.of(),NativeRangeImage.inspect(image,limits).interval().columns());
        }
        try(var pool=open(path)) {
            var store=store(pool,group,source,null,null); assertArrayEquals(target.digest(),store.schemaDigest()); store.validate(command);
            assertArrayEquals(installed.encode(),store.schemaInstallation().encode()); assertEquals(Set.of(),store.ownership().interval().columns());
            assertEquals(installed.fence(),store.applySchemaFence(new ApplyPosition(2,6),change.header()));
        }
    }
    @Test void interruptedInstallResumesAfterEachDurableBoundary() throws Exception {
        for(var boundary:DistributedStoragePool.SchemaInstallStage.values()) {
            var source=source(); var added=UUID.randomUUID(); var columns=new ArrayList<>(source.columns());
            columns.add(new DistributedSchema.Column(2,added,"new",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null));
            var target=new DistributedSchema(cluster,database,domain,1,0,columns); var change=change(source,target,List.of(descriptor(group,null,null)));
            var path=root.resolve(boundary.name());
            try(var pool=open(path)) {
                var store=store(pool,group,source,null,null); store.enableLogicalSchema(source); store.applyNoop(new ApplyPosition(1,0)); store.applySchemaFence(new ApplyPosition(1,1),change.header());
                assertThrows(IllegalStateException.class,() -> pool.installSchema(store,new ApplyPosition(1,2),change,Map.of(column,"data",added,"new"),Set.of(),stage -> { if(stage==boundary) throw new IllegalStateException("Interruption"); }));
            }
            try(var pool=open(path)) {
                var store=store(pool,group,source,null,null); assertArrayEquals(target.digest(),store.schemaDigest());
                assertNotNull(store.schemaInstallation()); assertEquals(new ApplyPosition(1,2),store.applied()); assertThrows(IllegalStateException.class,store::capture);
            }
        }
    }
    @Test void partialLocalDeletionRetainsSharedHandleUntilOtherGroupInstalls() throws Exception {
        var source=source(); var target=new DistributedSchema(cluster,database,domain,1,0,List.of());
        UUID rightGroup=UUID.randomUUID(); byte[] boundary={(byte)128};
        var change=change(source,target,List.of(descriptor(group,null,boundary),descriptor(rightGroup,boundary,null)));
        var path=root.resolve("partial");
        try(var pool=open(path)) {
            var left=store(pool,group,source,null,boundary); var right=store(pool,rightGroup,source,boundary,null);
            left.enableLogicalSchema(source); right.enableLogicalSchema(source);
            left.applyNoop(new ApplyPosition(1,0)); right.applyNoop(new ApplyPosition(1,0));
            left.applySchemaFence(new ApplyPosition(1,1),change.header());
            pool.installSchema(left,new ApplyPosition(1,2),change,Map.of(),Set.of());
            assertNull(right.schemaInstallation()); assertArrayEquals(source.digest(),right.schemaDigest());
            try(var pin=right.capture()) { assertNull(pin.get(column,new byte[]{(byte)129})); }
            right.applySchemaFence(new ApplyPosition(1,1),change.header()); pool.installSchema(right,new ApplyPosition(1,2),change,Map.of(),Set.of());
            assertEquals(Set.of(),right.ownership().interval().columns());
        }
        try(var pool=open(path)) {
            var left=store(pool,group,source,null,boundary); var right=store(pool,rightGroup,source,boundary,null);
            assertEquals(Set.of(),left.ownership().interval().columns()); assertEquals(Set.of(),right.ownership().interval().columns());
        }
    }

    @Test void createSharesOnePhysicalFamilyAcrossBothLocalGenerationsAndReopens() throws Exception {
        var source=source(); var added=UUID.randomUUID(); var columns=new ArrayList<>(source.columns());
        columns.add(new DistributedSchema.Column(2,added,"new",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null));
        var target=new DistributedSchema(cluster,database,domain,1,0,columns); UUID other=UUID.randomUUID(); byte[] boundary={(byte)128};
        var change=change(source,target,List.of(descriptor(group,null,boundary),descriptor(other,boundary,null))); var path=root.resolve("shared-create");
        byte[] originalProof;
        try(var pool=open(path)) {
            var left=store(pool,group,source,null,boundary); var right=store(pool,other,source,boundary,null);
            left.enableLogicalSchema(source); right.enableLogicalSchema(source);
            left.applyNoop(new ApplyPosition(1,0)); right.applyNoop(new ApplyPosition(1,0));
            left.applySchemaFence(new ApplyPosition(1,1),change.header()); right.applySchemaFence(new ApplyPosition(1,1),change.header());
            originalProof=pool.installSchema(left,new ApplyPosition(1,2),change,Map.of(column,"data",added,"new"),Set.of()).encode();
            pool.installSchema(right,new ApplyPosition(1,2),change,Map.of(column,"data",added,"new"),Set.of());
            try(var options=new org.rocksdb.Options()) {
                var families=org.rocksdb.RocksDB.listColumnFamilies(options,path.toString());
                assertEquals(1,families.stream().filter(name -> Arrays.equals(name,"new".getBytes(java.nio.charset.StandardCharsets.US_ASCII))).count());
            }
        }
        try(var pool=open(path)) {
            var left=store(pool,group,source,null,boundary); var right=store(pool,other,source,boundary,null);
            assertArrayEquals(target.digest(),left.schemaDigest()); assertArrayEquals(target.digest(),right.schemaDigest());
            assertArrayEquals(originalProof,pool.installSchema(left,new ApplyPosition(2,3),change,Map.of(column,"data",added,"new"),Set.of()).encode());
            assertEquals(new ApplyPosition(2,3),left.applied());
        }
    }

    @Test void partialDeleteReopenDoesNotDropColumnOfUnopenedSourceGroup() throws Exception {
        var source=source(); var target=new DistributedSchema(cluster,database,domain,1,0,List.of()); UUID other=UUID.randomUUID(); byte[] boundary={(byte)128};
        var change=change(source,target,List.of(descriptor(group,null,boundary),descriptor(other,boundary,null))); var path=root.resolve("partial-reopen");
        try(var pool=open(path)) {
            var left=store(pool,group,source,null,boundary); var right=store(pool,other,source,boundary,null);
            left.enableLogicalSchema(source); right.enableLogicalSchema(source); left.applyNoop(new ApplyPosition(1,0)); right.applyNoop(new ApplyPosition(1,0));
            left.applySchemaFence(new ApplyPosition(1,1),change.header()); pool.installSchema(left,new ApplyPosition(1,2),change,Map.of(),Set.of());
        }
        try(var pool=open(path)) {
            var left=store(pool,group,source,null,boundary); assertEquals(Set.of(),left.ownership().interval().columns());
            var right=store(pool,other,source,boundary,null); right.enableLogicalSchema(source);
            try(var pin=right.capture()) { assertNull(pin.get(column,new byte[]{(byte)129})); }
            right.applySchemaFence(new ApplyPosition(1,1),change.header()); pool.installSchema(right,new ApplyPosition(1,2),change,Map.of(),Set.of());
        }
    }

    @Test void releasedGateRetainsOriginalDecisionAfterColumnDeletionAndRecovery() throws Exception {
        var source=source(); var target=new DistributedSchema(cluster,database,domain,1,0,List.of()); var change=change(source,target,List.of(descriptor(group,null,null)));
        var session=UUID.randomUUID();
        var gate=new SnapshotFence(cluster,database,domain,UUID.randomUUID(),UUID.randomUUID(),1,new OperationId(session,1,0),0,0,source.digest(),
                List.of(new SnapshotFence.Participant(group,group,1,source.digest(),new OperationId(session,1,1),new OperationId(session,1,2),
                        new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{1}))))));
        var terminal=SnapshotFenceStatus.begin(gate).abort(); var path=root.resolve("released-gate");
        try(var pool=open(path)) {
            var store=store(pool,group,source,null,null); store.enableLogicalSchema(source); store.applyNoop(new ApplyPosition(1,0));
            store.applyGateInstall(new ApplyPosition(1,1),gate); store.applyGateRelease(new ApplyPosition(1,2),terminal);
            store.applySchemaFence(new ApplyPosition(1,3),change.header()); pool.installSchema(store,new ApplyPosition(1,4),change,Map.of(),Set.of());
            assertFalse(store.estimateGateRelease(terminal).existing().active());
            assertEquals(new ApplyPosition(1,2),store.applyGateRelease(new ApplyPosition(1,5),terminal).gate().released());
            assertFalse(store.applyGateInstall(new ApplyPosition(1,6),gate).gate().active());
            var image=root.resolve("released-gate.image"); var limits=new NativeRangeImage.Limits(3*1024*1024,32*1024*1024,256);
            NativeRangeImage.write(image,store,limits); NativeRangeImage.inspect(image,limits);
        }
        try(var pool=open(root.resolve("released-gate.import"))) {
            var store=pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(domain,Set.of(),null,null),target.digest(),Map.of(),Set.of());
            store.enableLogicalSchema(target); store.fence();
            store.installSnapshot(root.resolve("released-gate.image"),new NativeRangeImage.Limits(3*1024*1024,32*1024*1024,256)); store.activate();
            assertFalse(store.snapshotGate(gate.fenceId()).active());
            assertEquals(new ApplyPosition(1,2),store.applyGateRelease(new ApplyPosition(2,7),terminal).gate().released());
            assertThrows(IllegalStateException.class,store::capture);
        }
        try(var pool=open(path)) {
            var store=store(pool,group,source,null,null); assertFalse(store.snapshotGate(gate.fenceId()).active());
            assertEquals(new ApplyPosition(1,2),store.applyGateRelease(new ApplyPosition(2,7),terminal).gate().released());
        }
    }
    @Test void interruptedBirthCannotBeAdoptedByAnotherDomainBeforeOrAfterReopen() throws Exception {
        for(var boundary:List.of(DistributedStoragePool.SchemaInstallStage.INTENT,DistributedStoragePool.SchemaInstallStage.COLUMN_FAMILIES)) {
            var source=source(); var added=UUID.randomUUID(); var columns=new ArrayList<>(source.columns());
            columns.add(new DistributedSchema.Column(2,added,"new",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null));
            var target=new DistributedSchema(cluster,database,domain,1,0,columns); var change=change(source,target,List.of(descriptor(group,null,null))); var path=root.resolve("birth-"+boundary);
            UUID foreignColumn=UUID.randomUUID(),foreignGroup=UUID.randomUUID(),foreignDomain=UUID.randomUUID();
            try(var pool=open(path)) {
                var store=store(pool,group,source,null,null); store.enableLogicalSchema(source); store.applyNoop(new ApplyPosition(1,0)); store.applySchemaFence(new ApplyPosition(1,1),change.header());
                assertThrows(IllegalStateException.class,() -> pool.installSchema(store,new ApplyPosition(1,2),change,Map.of(column,"data",added,"new"),Set.of(),stage -> { if(stage==boundary) throw new IllegalStateException("Interruption"); }));
                assertThrows(IllegalArgumentException.class,() -> pool.openRange(new RangeOwnership.Owner(foreignGroup,1),new RangeOwnership.Interval(foreignDomain,Set.of(foreignColumn),null,null),new byte[32],Map.of(foreignColumn,"new"),Set.of()));
            }
            try(var pool=open(path)) {
                assertThrows(IllegalArgumentException.class,() -> pool.openRange(new RangeOwnership.Owner(foreignGroup,1),new RangeOwnership.Interval(foreignDomain,Set.of(foreignColumn),null,null),new byte[32],Map.of(foreignColumn,"new"),Set.of()));
                var store=store(pool,group,source,null,null); assertArrayEquals(target.digest(),store.schemaDigest());
            }
        }
    }

    @Test void closeTimeoutRetainsOwnersWhileSamplerStillUsesHandles() throws Exception {
        var pool=open(root.resolve("sample-close"));
        var loadedField=DistributedStoragePool.class.getDeclaredField("loaded"); loadedField.setAccessible(true); var loaded=(RocksDBLoader.LoadedDb)loadedField.get(pool);
        var lifetimeField=DistributedStoragePool.class.getDeclaredField("handleLifetime"); lifetimeField.setAccessible(true);
        var lifetime=(java.util.concurrent.locks.ReentrantReadWriteLock)lifetimeField.get(pool);
        lifetime.readLock().lock();
        try(var executor=java.util.concurrent.Executors.newSingleThreadExecutor()) {
            var closing=executor.submit(() -> pool.close(java.time.Duration.ofMillis(25)));
            assertFalse(closing.get(2,java.util.concurrent.TimeUnit.SECONDS)); assertTrue(loaded.db().get().isOwningHandle());
            assertTrue(loaded.dbOptions().isOwningHandle());
        } finally { lifetime.readLock().unlock(); pool.close(); }
        assertFalse(loaded.db().get().isOwningHandle());
    }
    @Test void unknownBirthForceRetainsReservationAndRetryForcesItAgain() throws Exception {
        for(boolean persisted:List.of(false,true)) {
        var source=source(); UUID added=UUID.randomUUID(); var columns=new ArrayList<>(source.columns());
        columns.add(new DistributedSchema.Column(2,added,"new",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null));
        var target=new DistributedSchema(cluster,database,domain,1,0,columns); var change=change(source,target,List.of(descriptor(group,null,null)));
        try(var pool=open(root.resolve("birth-unknown-"+persisted))) {
            var store=store(pool,group,source,null,null); store.enableLogicalSchema(source); store.applyNoop(new ApplyPosition(1,0)); store.applySchemaFence(new ApplyPosition(1,1),change.header());
            var loadedField=DistributedStoragePool.class.getDeclaredField("loaded"); loadedField.setAccessible(true); var loaded=(RocksDBLoader.LoadedDb)loadedField.get(pool);
            var nativeDb=(org.rocksdb.OptimisticTransactionDB)loaded.db().get();
            var failingDb=org.mockito.Mockito.mock(org.rocksdb.OptimisticTransactionDB.class,org.mockito.AdditionalAnswers.delegatesTo(nativeDb));
            var unknown=new java.util.concurrent.atomic.AtomicBoolean(true);
            org.mockito.Mockito.doAnswer(invocation -> {
                var cf=invocation.getArgument(0,org.rocksdb.ColumnFamilyHandle.class); var options=invocation.getArgument(1,org.rocksdb.WriteOptions.class);
                var key=invocation.getArgument(2,byte[].class); var value=invocation.getArgument(3,byte[].class);
                if(unknown.getAndSet(false)) {
                    if(persisted) nativeDb.put(cf,options,key,value);
                    throw new org.rocksdb.RocksDBException("Unknown registry force result");
                }
                nativeDb.put(cf,options,key,value); return null;
            }).when(failingDb).put(org.mockito.ArgumentMatchers.any(org.rocksdb.ColumnFamilyHandle.class),org.mockito.ArgumentMatchers.any(org.rocksdb.WriteOptions.class),org.mockito.ArgumentMatchers.any(byte[].class),org.mockito.ArgumentMatchers.any(byte[].class));
            var dbField=loaded.db().getClass().getSuperclass().getDeclaredField("db"); dbField.setAccessible(true); dbField.set(loaded.db(),failingDb);
            try {
                assertThrows(org.rocksdb.RocksDBException.class,() -> pool.installSchema(store,new ApplyPosition(1,2),change,Map.of(column,"data",added,"new"),Set.of()));
                UUID foreign=UUID.randomUUID();
                assertThrows(IllegalArgumentException.class,() -> pool.openRange(new RangeOwnership.Owner(UUID.randomUUID(),1),new RangeOwnership.Interval(UUID.randomUUID(),Set.of(foreign),null,null),new byte[32],Map.of(foreign,"new"),Set.of()));
                assertArrayEquals(target.digest(),pool.installSchema(store,new ApplyPosition(1,2),change,Map.of(column,"data",added,"new"),Set.of()).definition().targetSchema().digest());
                org.mockito.Mockito.verify(failingDb,org.mockito.Mockito.times(2)).put(org.mockito.ArgumentMatchers.any(org.rocksdb.ColumnFamilyHandle.class),org.mockito.ArgumentMatchers.any(org.rocksdb.WriteOptions.class),org.mockito.ArgumentMatchers.any(byte[].class),org.mockito.ArgumentMatchers.any(byte[].class));
            } finally { dbField.set(loaded.db(),nativeDb); }
        }
        }
    }

    @Test void changedBoundsRejectInstallBeforeIntentAndRejectRecoveryBeforeOpeningTarget() throws Exception {
        var source=source(); UUID added=UUID.randomUUID(); var columns=new ArrayList<>(source.columns());
        columns.add(new DistributedSchema.Column(2,added,"new",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null));
        var target=new DistributedSchema(cluster,database,domain,1,0,columns); byte[] boundary={(byte)128};
        var change=change(source,target,List.of(descriptor(group,null,boundary),descriptor(UUID.randomUUID(),boundary,null)));
        try(var pool=open(root.resolve("bounds-live"))) {
            var store=store(pool,group,source,null,null); store.enableLogicalSchema(source); store.applyNoop(new ApplyPosition(1,0)); store.applySchemaFence(new ApplyPosition(1,1),change.header());
            assertThrows(IllegalArgumentException.class,() -> pool.installSchema(store,new ApplyPosition(1,2),change,Map.of(column,"data",added,"new"),Set.of()));
            assertEquals(new ApplyPosition(1,1),store.applied()); assertNull(store.schemaInstallation());
            try(var options=new org.rocksdb.Options()) {
                assertTrue(org.rocksdb.RocksDB.listColumnFamilies(options,root.resolve("bounds-live").toString()).stream().noneMatch(name -> Arrays.equals(name,"new".getBytes(java.nio.charset.StandardCharsets.US_ASCII))));
            }
        }
        var path=root.resolve("bounds-recover");
        try(var pool=open(path)) {
            var store=store(pool,group,source,null,boundary); store.enableLogicalSchema(source); store.applyNoop(new ApplyPosition(1,0)); store.applySchemaFence(new ApplyPosition(1,1),change.header());
            pool.installSchema(store,new ApplyPosition(1,2),change,Map.of(column,"data",added,"new"),Set.of());
        }
        try(var pool=open(path)) {
            assertThrows(IllegalArgumentException.class,() -> store(pool,group,source,null,null));
            assertArrayEquals(target.digest(),store(pool,group,source,null,boundary).schemaDigest());
        }
    }

    @Test void inconsistentInstalledMetadataRejectsImageExportAndNativeReopen() throws Exception {
        for(String corruption:List.of("missing-current","missing-history","changed-current","missing-fence","future-history")) {
            var source=source(); UUID added=UUID.randomUUID(); var columns=new ArrayList<>(source.columns());
            columns.add(new DistributedSchema.Column(2,added,"new",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null));
            var target=new DistributedSchema(cluster,database,domain,1,0,columns); var change=change(source,target,List.of(descriptor(group,null,null)));
            var path=root.resolve(corruption); var image=root.resolve(corruption+".image");
            try(var pool=open(path)) {
                var store=store(pool,group,source,null,null); store.enableLogicalSchema(source); store.applyNoop(new ApplyPosition(1,0)); store.applySchemaFence(new ApplyPosition(1,1),change.header());
                var installed=pool.installSchema(store,new ApplyPosition(1,2),change,Map.of(column,"data",added,"new"),Set.of());
                store.applyNoop(new ApplyPosition(1,3)); store.applyNoop(new ApplyPosition(1,4)); store.applyNoop(new ApplyPosition(1,5));
                var loadedField=DistributedStoragePool.class.getDeclaredField("loaded"); loadedField.setAccessible(true); var loaded=(RocksDBLoader.LoadedDb)loadedField.get(pool);
                var state=loaded.db().getStartupColumns().entrySet().stream().filter(entry -> Arrays.equals(entry.getKey().getName(),DistributedStoragePool.STATE_COLUMN.getBytes(java.nio.charset.StandardCharsets.US_ASCII))).findFirst().orElseThrow().getValue();
                byte[] currentKey=java.nio.ByteBuffer.allocate(17).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).put((byte)0x28).array();
                byte[] historyKey=java.nio.ByteBuffer.allocate(33).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).put((byte)0x2a).putLong(change.changeId().getMostSignificantBits()).putLong(change.changeId().getLeastSignificantBits()).array();
                try(var options=new org.rocksdb.WriteOptions().setSync(true).setDisableWAL(false)) {
                    switch(corruption) {
                        case "missing-current" -> loaded.db().get().delete(state,options,currentKey);
                        case "missing-history" -> loaded.db().get().delete(state,options,historyKey);
                        case "changed-current" -> {
                            var changed=new NativeSchemaInstallation(change,installed.fence(),new ApplyPosition(1,3),installed.sourceNames(),installed.targetNames(),installed.sourceMerges(),installed.targetMerges());
                            loaded.db().get().put(state,options,currentKey,changed.encode());
                        }
                        case "missing-fence" -> loaded.db().get().delete(state,options,java.nio.ByteBuffer.allocate(17).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).put((byte)0x27).array());
                        case "future-history" -> {
                            UUID later=UUID.randomUUID(); var futureColumns=new ArrayList<>(target.columns());
                            futureColumns.add(new DistributedSchema.Column(3,later,"later",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null));
                            var futureSchema=new DistributedSchema(cluster,database,domain,2,0,futureColumns);
                            var future=new SchemaChange.Definition(UUID.randomUUID(),new OperationId(UUID.randomUUID(),1,0),change.targetDirectory(),target,futureSchema);
                            var futureRecord=new NativeSchemaInstallation(future,new NativeSchemaFence(future.header(),group,new ApplyPosition(1,3)),new ApplyPosition(1,4),installed.targetNames(),Map.of(column,"data",added,"new",later,"later"),Set.of(),Set.of());
                            var futureKey=java.nio.ByteBuffer.allocate(33).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).put((byte)0x2a).putLong(future.changeId().getMostSignificantBits()).putLong(future.changeId().getLeastSignificantBits()).array();
                            loaded.db().get().put(state,options,futureKey,futureRecord.encode());
                        }
                        default -> throw new AssertionError(corruption);
                    }
                }
                var limits=new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,256);
                assertThrows(java.io.IOException.class,() -> NativeRangeImage.write(image,store,limits),corruption);
                assertFalse(java.nio.file.Files.exists(image),"Invalid metadata cannot publish a recovery image: "+corruption);
            }
            assertThrows(RuntimeException.class,() -> { try(var pool=open(path)) { store(pool,group,source,null,null); } },corruption);
        }
    }

}
