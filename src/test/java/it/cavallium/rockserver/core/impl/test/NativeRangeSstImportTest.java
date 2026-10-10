package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class NativeRangeSstImportTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final UUID group=UUID.randomUUID(),column=UUID.randomUUID(),domain=UUID.randomUUID();
    DistributedSchema schema() { return new DistributedSchema(UUID.randomUUID(),UUID.randomUUID(),domain,0,0,List.of(new DistributedSchema.Column(1,column,"data",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null))); }
    DistributedStoragePool open(Path path) { return DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass())); }
    NativeSstImportManifest.File file(Path path,boolean delete) throws Exception {
        RocksDB.loadLibrary(); try(var env=new EnvOptions();var options=new Options();var writer=new SstFileWriter(env,options)) { writer.open(path.toString()); if(delete)writer.delete(new byte[]{2}); else { writer.put(new byte[]{2},new byte[]{7}); writer.put(new byte[]{3},new byte[]{8}); } writer.finish(); }
        return new NativeSstImportManifest.File(UUID.randomUUID(),Files.size(path),MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)),delete?1:2,new byte[]{2},new byte[]{delete?(byte)2:(byte)3});
    }
    NativeSstImportManifest manifest(DistributedSchema schema,NativeSstImportManifest.File file) { return new NativeSstImportManifest(UUID.randomUUID(),group,1,domain,schema.digest(),new byte[]{1},new byte[]{5},List.of(new NativeSstImportManifest.Column(column,"data",false,List.of(file)))); }
    @Test void nativeIngestRetainsOriginalAndRequiresDistinctPublicationBeforeServingAndReopen() throws Exception {
        var schema=schema(); Path source=root.resolve("source.sst"),db=root.resolve("db"); var file=file(source,false); var manifest=manifest(schema,file); NativeSstImportManifest.Receipt receipt;
        assertArrayEquals(manifest.encode(),NativeSstImportManifest.decode(manifest.encode()).encode());
        try(var pool=open(db)) {
            var store=pool.openNewFencedRange(manifest,schema); assertEquals(RangeOwnership.State.FENCED,store.ownership().state()); assertTrue(store.importPending()); assertThrows(IllegalStateException.class,store::activate);
            receipt=pool.importSst(store,manifest,Map.of(file.fileId(),source),new ApplyPosition(1,0));
            assertTrue(Files.exists(source)); assertTrue(store.importPending()); assertEquals(new ApplyPosition(1,0),store.applied()); assertThrows(IllegalStateException.class,store::capture);
            assertArrayEquals(receipt.encode(),pool.importSst(store,manifest,Map.of(),new ApplyPosition(1,1)).encode());
        }
        try(var pool=open(db)) {
            var store=pool.openNewFencedRange(manifest,schema); assertTrue(store.importPending()); assertArrayEquals(receipt.encode(),store.importReceipt().encode());
            assertThrows(IllegalStateException.class,()->store.installSnapshot(source,new NativeRangeImage.Limits(1024,1024*1024,256)));
            pool.activateImport(store,new NativeRangeSstImport.Activation(receipt,UUID.randomUUID(),new ApplyPosition(2,10),new byte[32],new ApplyPosition(2,2)));
            try(var pin=store.capture()) { assertArrayEquals(new byte[]{7},pin.get(column,new byte[]{2})); assertArrayEquals(new byte[]{8},pin.get(column,new byte[]{3})); assertNull(pin.get(column,new byte[]{4})); }
            Path image=root.resolve("activated.image"); var limits=new NativeRangeImage.Limits(3*1024*1024,32L*1024*1024,256); NativeRangeImage.write(image,store,limits); NativeRangeImage.inspect(image,limits);
            store.fence(); store.installSnapshot(image,limits); store.activate(); try(var pin=store.capture()) { assertArrayEquals(new byte[]{7},pin.get(column,new byte[]{2})); }
        }
        try(var pool=open(db)) {
            var store=pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(domain,schema.columnIds(),new byte[]{1},new byte[]{5}),schema.digest(),Map.of(column,"data"),Set.of()); store.enableLogicalSchema(schema); assertFalse(store.importPending()); try(var pin=store.capture()) { assertArrayEquals(new byte[]{7},pin.get(column,new byte[]{2})); }
            assertThrows(IllegalStateException.class,()->pool.openNewFencedRange(manifest,schema));
        }
    }
    @Test void interruptionAfterNativeIngestionReopensFencedAndReplaysOnlyOwnedUserInterval() throws Exception {
        var schema=schema(); Path source=root.resolve("retry.sst"),db=root.resolve("retry-db"); var file=file(source,false); var manifest=manifest(schema,file); UUID neighbourId=UUID.randomUUID();
        try(var pool=open(db)) {
            var neighbour=pool.openRange(new RangeOwnership.Owner(neighbourId,1),new RangeOwnership.Interval(domain,schema.columnIds(),new byte[]{5},null),schema.digest(),Map.of(column,"data"),Set.of()); neighbour.enableLogicalSchema(schema);
            neighbour.apply(new ApplyPosition(1,0),new DistributedCommand(neighbour.ownership().owner().groupId(),1,schema.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{6},new byte[]{9}))));
            var store=pool.openNewFencedRange(manifest,schema); store.applyNoop(new ApplyPosition(1,0),new byte[]{4,5});
            assertThrows(IOException.class,()->pool.importSst(store,manifest,Map.of(file.fileId(),source),new ApplyPosition(1,1),stage->{ if(stage==NativeRangeSstImport.Stage.COLUMN_INGESTED)throw new IOException("Controlled interrupted import"); }));
            assertEquals(new ApplyPosition(1,0),store.applied()); assertNull(store.importReceipt()); assertThrows(IllegalStateException.class,store::activate);
        }
        Files.delete(source);
        try(var pool=open(db)) {
            var store=pool.openNewFencedRange(manifest,schema); assertTrue(store.importPending()); var receipt=pool.importSst(store,manifest,Map.of(),new ApplyPosition(1,1));
            pool.activateImport(store,new NativeRangeSstImport.Activation(receipt,UUID.randomUUID(),new ApplyPosition(2,10),new byte[32],new ApplyPosition(2,2)));
            try(var pin=store.capture()) { assertArrayEquals(new byte[]{7},pin.get(column,new byte[]{2})); }
            var configuration=store.configuration(); assertNotNull(configuration); assertArrayEquals(new byte[]{4,5},configuration.data());
            var neighbour=pool.openRange(new RangeOwnership.Owner(neighbourId,1),new RangeOwnership.Interval(domain,schema.columnIds(),new byte[]{5},null),schema.digest(),Map.of(column,"data"),Set.of()); neighbour.enableLogicalSchema(schema); try(var pin=neighbour.capture()) { assertArrayEquals(new byte[]{9},pin.get(column,new byte[]{6})); }
        }
    }
    @Test void deletionTableAndWrongKeyRangeAreRejectedBeforeIntent() throws Exception {
        var schema=schema(); Path source=root.resolve("delete.sst"); var file=file(source,true); var manifest=manifest(schema,file);
        try(var pool=open(root.resolve("reject-db"))) { var store=pool.openNewFencedRange(manifest,schema); assertThrows(IOException.class,()->pool.importSst(store,manifest,Map.of(file.fileId(),source),new ApplyPosition(1,0))); assertNull(store.applied()); assertNull(store.importReceipt()); }
        assertThrows(IllegalArgumentException.class,()->new NativeSstImportManifest(UUID.randomUUID(),group,1,domain,schema.digest(),new byte[]{3},new byte[]{5},manifest.columns()));
    }
    @Test void historicalEmptiedIntervalCannotBeAdoptedByDifferentGroupAfterReopen() throws Exception {
        var schema=schema(); Path db=root.resolve("historical"),source=root.resolve("historical.sst"); var file=file(source,false); var manifest=manifest(schema,file); UUID oldGroup=UUID.randomUUID();
        try(var pool=open(db)) {
            var old=pool.openRange(new RangeOwnership.Owner(oldGroup,1),new RangeOwnership.Interval(domain,schema.columnIds(),new byte[]{1},new byte[]{5}),schema.digest(),Map.of(column,"data"),Set.of()); old.enableLogicalSchema(schema);
            old.apply(new ApplyPosition(1,0),new DistributedCommand(oldGroup,1,schema.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{2},new byte[]{7}))));
            old.apply(new ApplyPosition(1,1),new DistributedCommand(oldGroup,1,schema.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.DELETE,column,new byte[]{2},null))));
            try(var pin=old.capture()) { assertNull(pin.get(column,new byte[]{2})); }
        }
        try(var pool=open(db)) { assertThrows(IllegalStateException.class,()->pool.openNewFencedRange(manifest,schema)); var reopened=pool.openRange(new RangeOwnership.Owner(oldGroup,1),new RangeOwnership.Interval(domain,schema.columnIds(),new byte[]{1},new byte[]{5}),schema.digest(),Map.of(column,"data"),Set.of()); assertEquals(new ApplyPosition(1,1),reopened.applied()); }
    }
    @Test void unknownBirthCompletionRetainsExactIdentityAndAtomicHistoryAcrossRetryAndReopen() throws Exception {
        for(boolean afterPersistence:new boolean[]{false,true})for(boolean reopen:new boolean[]{false,true}) {
            var schema=schema(); Path db=root.resolve("birth-"+afterPersistence+"-"+reopen),source=root.resolve("birth-"+afterPersistence+"-"+reopen+".sst"); var file=file(source,false); var manifest=manifest(schema,file);
            var changed=new NativeSstImportManifest(UUID.randomUUID(),group,1,domain,schema.digest(),manifest.lowerInclusive(),manifest.upperExclusive(),manifest.columns());
            try(var pool=open(db)) {
                pool.importBirthForceObserver(after->{ if(after==afterPersistence)throw new RocksDBException("Controlled unknown birth completion"); });
                assertThrows(RocksDBException.class,()->pool.openNewFencedRange(manifest,schema));
                assertThrows(IllegalArgumentException.class,()->pool.openNewFencedRange(changed,schema));
                var foreign=new NativeSstImportManifest(UUID.randomUUID(),UUID.randomUUID(),1,domain,schema.digest(),manifest.lowerInclusive(),manifest.upperExclusive(),manifest.columns());
                assertThrows(IllegalStateException.class,()->pool.openNewFencedRange(foreign,schema));
                pool.importBirthForceObserver(after->{});
                if(!reopen) { var store=pool.openNewFencedRange(manifest,schema); assertSame(store,pool.openNewFencedRange(manifest,schema)); pool.importSst(store,manifest,Map.of(file.fileId(),source),new ApplyPosition(1,0)); assertTrue(store.importPending()); }
            }
            try(var pool=open(db)) {
                if(afterPersistence || !reopen)assertThrows(IllegalArgumentException.class,()->pool.openNewFencedRange(changed,schema));
                var store=pool.openNewFencedRange(manifest,schema); assertEquals(RangeOwnership.State.FENCED,store.ownership().state());
                var receipt=pool.importSst(store,manifest,Map.of(file.fileId(),source),new ApplyPosition(1,reopen?0:1)); assertEquals(manifest.importId(),receipt.importId());
            }
        }
    }
    @Test void rejectedReservedPhysicalBindingDoesNotCreateVirginBirthOrPoisonCorrectedRequest() throws Exception {
        var schema=schema(); Path source=root.resolve("binding.sst"); var file=file(source,false); var valid=manifest(schema,file);
        var invalid=new NativeSstImportManifest(valid.importId(),group,1,domain,schema.digest(),valid.lowerInclusive(),valid.upperExclusive(),List.of(new NativeSstImportManifest.Column(column,"default",false,List.of(file))));
        try(var pool=open(root.resolve("binding-db"))) {
            var forces=new java.util.concurrent.atomic.AtomicInteger(); pool.importBirthForceObserver(after->forces.incrementAndGet());
            assertThrows(IllegalArgumentException.class,()->pool.openNewFencedRange(invalid,schema)); assertEquals(0,forces.get());
            var store=pool.openNewFencedRange(valid,schema); assertEquals(2,forces.get()); pool.importSst(store,valid,Map.of(file.fileId(),source),new ApplyPosition(1,0));
        }
    }
    @Test void publishedImportRetainsOriginalEvidenceThroughColumnDeletionImageAndOriginalBootstrapReopen() throws Exception {
        var sourceSchema=schema(); var target=new DistributedSchema(sourceSchema.clusterId(),sourceSchema.databaseId(),domain,1,0,List.of());
        var replica=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"local","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        var descriptor=new RangeDescriptor(UUID.randomUUID(),group,1,1,new byte[]{1},new byte[]{5},RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        var before=new RangeDescriptor(UUID.randomUUID(),UUID.randomUUID(),1,1,null,new byte[]{1},RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        var after=new RangeDescriptor(UUID.randomUUID(),UUID.randomUUID(),1,1,new byte[]{5},null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        var directory=new RangeDirectory(sourceSchema.clusterId(),sourceSchema.databaseId(),domain,0,sourceSchema.digest(),0,sourceSchema.columnIds(),0,List.of(before,descriptor,after));
        var deletion=new SchemaChange.Definition(UUID.randomUUID(),new OperationId(UUID.randomUUID(),1,0),directory,sourceSchema,target);
        Path original=root.resolve("schema-original.sst"),db=root.resolve("schema-import-db"),image=root.resolve("schema-import.image"); var file=file(original,false); var manifest=manifest(sourceSchema,file); NativeSstImportManifest.Receipt receipt;
        var limits=new NativeRangeImage.Limits(3*1024*1024,32L*1024*1024,256);
        try(var pool=open(db)) {
            var store=pool.openNewFencedRange(manifest,sourceSchema); receipt=pool.importSst(store,manifest,Map.of(file.fileId(),original),new ApplyPosition(1,0));
            pool.activateImport(store,new NativeRangeSstImport.Activation(receipt,UUID.randomUUID(),new ApplyPosition(2,10),new byte[32],new ApplyPosition(1,1)));
            store.applySchemaFence(new ApplyPosition(1,2),deletion.header()); var installed=pool.installSchema(store,new ApplyPosition(1,3),deletion,Map.of(),Set.of()); store.applySchemaActivation(new ApplyPosition(1,4),installed.proof(),UUID.randomUUID(),new ApplyPosition(2,20));
            assertArrayEquals(receipt.encode(),pool.importSst(store,manifest,Map.of(),new ApplyPosition(1,5)).encode());
            assertEquals(Set.of(),NativeRangeImage.write(image,store,limits).interval().columns()); NativeRangeImage.inspect(image,limits);
        }
        try(var pool=open(db)) {
            var store=pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(domain,sourceSchema.columnIds(),manifest.lowerInclusive(),manifest.upperExclusive()),sourceSchema.digest(),Map.of(column,"data"),Set.of());
            assertFalse(store.importPending()); assertArrayEquals(target.digest(),store.schemaDigest()); assertArrayEquals(receipt.encode(),store.importReceipt().encode());
            assertArrayEquals(receipt.encode(),pool.importSst(store,manifest,Map.of(),new ApplyPosition(2,6)).encode()); NativeRangeImage.write(root.resolve("schema-reopened.image"),store,limits);
            store.fence(); store.installSnapshot(image,limits); store.activate(); assertFalse(store.importPending()); assertArrayEquals(target.digest(),store.schemaDigest());
        }
    }
    @Test void fixedValueBeyondDistributedReadBoundRejectsBeforeInstallingOrUserMutation() throws Exception {
        var schema=schema(); Path original=root.resolve("oversized-value.sst"); RocksDB.loadLibrary();
        try(var env=new EnvOptions();var options=new Options();var writer=new SstFileWriter(env,options)) { writer.open(original.toString()); writer.put(new byte[]{2},new byte[DistributedCommandCodec.MAX_ENCODED_BYTES+1]); writer.finish(); }
        var file=new NativeSstImportManifest.File(UUID.randomUUID(),Files.size(original),MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(original)),1,new byte[]{2},new byte[]{2}); var manifest=manifest(schema,file);
        try(var pool=open(root.resolve("oversized-value-db"))) {
            var store=pool.openNewFencedRange(manifest,schema); var stages=new ArrayList<NativeRangeSstImport.Stage>();
            assertThrows(IOException.class,()->pool.importSst(store,manifest,Map.of(file.fileId(),original),new ApplyPosition(1,0),stages::add));
            assertFalse(stages.contains(NativeRangeSstImport.Stage.INTENT_FORCED)); assertNull(store.applied()); assertNull(store.importReceipt()); assertTrue(store.importPending());
        }
    }
    @Test void pendingOriginalPositionCannotBeConsumedByAccountingOrChangedByRetryAfterReopen() throws Exception {
        var schema=schema(); Path db=root.resolve("pending-position"),original=root.resolve("pending-position.sst"); var file=file(original,false); var manifest=manifest(schema,file);
        try(var pool=open(db)) {
            var store=pool.openNewFencedRange(manifest,schema);
            assertThrows(IOException.class,()->pool.importSst(store,manifest,Map.of(file.fileId(),original),new ApplyPosition(1,0),stage->{ if(stage==NativeRangeSstImport.Stage.INTENT_FORCED)throw new IOException("Controlled pending import"); }));
            assertThrows(IllegalStateException.class,()->store.applyNoop(new ApplyPosition(1,0))); assertThrows(IllegalStateException.class,()->store.applyNoop(new ApplyPosition(1,1)));
            assertThrows(IllegalArgumentException.class,()->pool.importSst(store,manifest,Map.of(),new ApplyPosition(2,0))); assertNull(store.applied());
        }
        try(var pool=open(db)) {
            var store=pool.openNewFencedRange(manifest,schema); assertThrows(IllegalStateException.class,()->store.applyNoop(new ApplyPosition(1,0)));
            var receipt=pool.importSst(store,manifest,Map.of(),new ApplyPosition(1,0)); assertEquals(new ApplyPosition(1,0),receipt.position()); assertEquals(receipt.position(),store.applied()); store.applyNoop(new ApplyPosition(1,1));
        }
    }
    private void replaceMetadata(DistributedStoragePool pool,int tag,byte[] value) throws Exception {
        var loadedField=DistributedStoragePool.class.getDeclaredField("loaded"); loadedField.setAccessible(true); var loaded=(RocksDBLoader.LoadedDb)loadedField.get(pool);
        var handlesField=DistributedStoragePool.class.getDeclaredField("handles"); handlesField.setAccessible(true); var handles=(Map<?,?>)handlesField.get(pool); var state=(ColumnFamilyHandle)handles.get(DistributedStoragePool.STATE_COLUMN);
        byte[] key=java.nio.ByteBuffer.allocate(17).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).put((byte)tag).array();
        try(var options=new WriteOptions().setDisableWAL(false).setSync(true)) { loaded.db().get().put(state,options,key,value); }
    }
    @Test void equalIndexDifferentTermReceiptAndActivationRejectRecoveryAndImage() throws Exception {
        var schema=schema(); Path original=root.resolve("conflicting-proof.sst"),receiptDb=root.resolve("conflicting-receipt"),activationDb=root.resolve("conflicting-activation"); var file=file(original,false); var manifest=manifest(schema,file);
        try(var pool=open(receiptDb)) {
            var store=pool.openNewFencedRange(manifest,schema); var receipt=pool.importSst(store,manifest,Map.of(file.fileId(),original),new ApplyPosition(1,0));
            var conflict=new NativeSstImportManifest.Receipt(receipt.importId(),group,1,receipt.manifestDigest(),new ApplyPosition(0,0)); assertThrows(IllegalArgumentException.class,()->conflict.requireCut(store.applied())); replaceMetadata(pool,0x42,conflict.encode());
        }
        try(var pool=open(receiptDb)) { assertThrows(IllegalArgumentException.class,()->pool.openNewFencedRange(manifest,schema)); }
        try(var pool=open(activationDb)) {
            var store=pool.openNewFencedRange(manifest,schema); var receipt=pool.importSst(store,manifest,Map.of(file.fileId(),original),new ApplyPosition(1,0)); var catalogue=UUID.randomUUID();
            pool.activateImport(store,new NativeRangeSstImport.Activation(receipt,catalogue,new ApplyPosition(2,10),new byte[32],new ApplyPosition(2,1)));
            var conflict=new NativeRangeSstImport.Activation(receipt,catalogue,new ApplyPosition(2,10),new byte[32],new ApplyPosition(1,1)); assertThrows(IllegalArgumentException.class,()->conflict.requireCut(store.applied())); replaceMetadata(pool,0x43,conflict.encode());
            assertThrows(IOException.class,()->NativeRangeImage.write(root.resolve("conflicting.image"),store,new NativeRangeImage.Limits(3*1024*1024,32L*1024*1024,256)));
        }
        try(var pool=open(activationDb)) { assertThrows(IllegalArgumentException.class,()->pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(domain,schema.columnIds(),manifest.lowerInclusive(),manifest.upperExclusive()),schema.digest(),manifest.physicalNames(),Set.of())); }
    }
    @Test void closedStoreCannotBeActivatedOrBecomeServingAgain() throws Exception {
        var schema=schema(); Path db=root.resolve("closed-import"),original=root.resolve("closed-import.sst"); var file=file(original,false); var manifest=manifest(schema,file);
        try(var pool=open(db)) {
            var store=pool.openNewFencedRange(manifest,schema); var receipt=pool.importSst(store,manifest,Map.of(file.fileId(),original),new ApplyPosition(1,0)); var authority=new NativeRangeSstImport.Activation(receipt,UUID.randomUUID(),new ApplyPosition(2,10),new byte[32],new ApplyPosition(1,1));
            assertTrue(store.close(java.time.Duration.ofSeconds(1))); assertThrows(IllegalStateException.class,()->pool.activateImport(store,authority)); assertEquals(RangeOwnership.State.FENCED,store.ownership().state()); assertTrue(store.importPending()); assertEquals(new ApplyPosition(1,0),store.applied());
        }
        try(var pool=open(db)) { var store=pool.openNewFencedRange(manifest,schema); assertTrue(store.importPending()); assertThrows(IllegalStateException.class,store::capture); }
    }
}
