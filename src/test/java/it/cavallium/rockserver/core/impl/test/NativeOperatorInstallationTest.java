package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.FlushOptions;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

class NativeOperatorInstallationTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeSchemaInstallationTest fixture=new NativeSchemaInstallationTest();
    public static class AppendOperator extends FFMByteArrayMergeOperator {
        public AppendOperator(String identity) { super(identity); }
        @Override public byte[] merge(byte[] key,byte[] existing,List<byte[]> operands) {
            var result=new java.io.ByteArrayOutputStream(); if(existing!=null) result.writeBytes(existing);
            operands.forEach(result::writeBytes); return result.toByteArray();
        }
    }
    DistributedSchema empty(long epoch) { return new DistributedSchema(fixture.cluster,fixture.database,fixture.domain,epoch,0,List.of()); }
    DistributedSchema target(byte[] jar) throws Exception {
        var column=fixture.source().column(fixture.column);
        var operator=new DistributedSchema.Operator("append",1,AppendOperator.class.getName(),MessageDigest.getInstance("SHA-256").digest(jar),1,1,"append-contract",1);
        return new DistributedSchema(fixture.cluster,fixture.database,fixture.domain,1,0,List.of(new DistributedSchema.Column(column.apiId(),column.columnId(),column.name(),new it.cavallium.rockserver.core.common.ColumnSchema(column.schema().keys(),column.schema().variableTailKeys(),column.schema().hasValue(),operator.name(),operator.version(),operator.className()),operator)));
    }
    DistributedStoragePool open(Path path,DistributedSchema original) { return DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()),original,Map.of(),Map.of()); }
    ReplicatedRangeStore range(DistributedStoragePool pool,DistributedSchema original) throws Exception {
        return pool.openRange(new RangeOwnership.Owner(fixture.group,1),new RangeOwnership.Interval(fixture.domain,Set.of(),null,null),original.digest(),Map.of(),Set.of());
    }
    RocksDBLoader.LoadedDb loaded(DistributedStoragePool pool) throws Exception {
        var field=DistributedStoragePool.class.getDeclaredField("loaded"); field.setAccessible(true); return (RocksDBLoader.LoadedDb)field.get(pool);
    }
    @Test void stagedCreateMergeCompactReopenDeleteKeepsBridgeUntilDatabaseClose() throws Exception {
        byte[] jar=DistributedMergeOperatorRegistryTest.classes(AppendOperator.class); var source=empty(0); var target=target(jar); var terminal=empty(2);
        var path=root.resolve("pool"); var operator=target.column(fixture.column).operator();
        DistributedStoragePool.stageOperator(path,operator,new MergeOperatorRegistry.DeterministicContract("append-contract",1,true),jar);
        var create=fixture.change(source,target,List.of(fixture.descriptor(fixture.group,null,null)));
        var command=new DistributedCommand(fixture.group,1,target.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.MERGE,fixture.column,new byte[]{1},new byte[]{7})));
        byte[] receipt;
        try(var pool=open(path,source)) {
            var store=range(pool,source); store.enableLogicalSchema(source); store.applyNoop(new ApplyPosition(1,0)); store.applySchemaFence(new ApplyPosition(1,1),create.header());
            var installed=pool.installSchema(store,new ApplyPosition(1,2),create,Map.of(fixture.column,"data"),Set.of(fixture.column));
            store.applySchemaActivation(new ApplyPosition(1,3),installed.proof(),UUID.randomUUID(),new ApplyPosition(2,10));
            receipt=store.apply(new ApplyPosition(1,4),command); assertArrayEquals(new byte[]{7},store.readCurrentPoint(fixture.column,new byte[]{1}));
            var db=loaded(pool).db().get(); try(var flush=new FlushOptions().setWaitForFlush(true)) { db.flush(flush); } db.compactRange();
        }
        FFMAbstractMergeOperator retired; var image=root.resolve("terminal.image");
        var limits=new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,256);
        try(var pool=open(path,source)) {
            var store=range(pool,source); assertArrayEquals(target.digest(),store.schemaDigest()); assertArrayEquals(receipt,store.apply(new ApplyPosition(2,5),command));
            var next=new DistributedCommand(fixture.group,1,target.digest(),new OperationId(UUID.randomUUID(),1,0),command.mutations());
            store.apply(new ApplyPosition(2,6),next); assertArrayEquals(new byte[]{7,7},store.readCurrentPoint(fixture.column,new byte[]{1}));
            retired=loaded(pool).mergeOperators().get("data"); assertTrue(retired.isOwningHandle());
            var deletion=new SchemaChange.Definition(UUID.randomUUID(),new OperationId(UUID.randomUUID(),1,0),create.targetDirectory(),target,terminal);
            store.applySchemaFence(new ApplyPosition(2,7),deletion.header()); var installed=pool.installSchema(store,new ApplyPosition(2,8),deletion,Map.of(),Set.of());
            store.applySchemaActivation(new ApplyPosition(2,9),installed.proof(),UUID.randomUUID(),new ApplyPosition(3,10));
            assertTrue(retired.isOwningHandle(),"Dropped callback owner must survive background native references until DB close");
            NativeRangeImage.write(image,store,limits);
        }
        assertFalse(retired.isOwningHandle());
        try(var pool=open(path,source)) { assertEquals(Set.of(),range(pool,source).ownership().interval().columns()); assertFalse(loaded(pool).mergeOperators().containsKey("data")); }
        var imported=root.resolve("imported-terminal");
        try(var pool=open(imported,terminal)) { var store=range(pool,terminal); store.enableLogicalSchema(terminal); store.fence(); store.installSnapshot(image,limits); store.activate(); assertArrayEquals(receipt,store.apply(new ApplyPosition(3,10),command)); }
        try(var pool=open(imported,terminal)) { assertArrayEquals(terminal.digest(),range(pool,terminal).schemaDigest()); assertFalse(loaded(pool).mergeOperators().containsKey("data")); }
    }
    @Test void interruptedDdlReopensWithExactStagedOwnerAndMissingArtifactFailsBeforeOpen() throws Exception {
        byte[] jar=DistributedMergeOperatorRegistryTest.classes(AppendOperator.class); var source=empty(0); var target=target(jar);
        for(var boundary:List.of(DistributedStoragePool.SchemaInstallStage.INTENT,DistributedStoragePool.SchemaInstallStage.COLUMN_FAMILIES)) {
            var path=root.resolve(boundary.name()); var operator=target.column(fixture.column).operator();
            DistributedStoragePool.stageOperator(path,operator,new MergeOperatorRegistry.DeterministicContract("append-contract",1,true),jar);
            var change=fixture.change(source,target,List.of(fixture.descriptor(fixture.group,null,null)));
            try(var pool=open(path,source)) {
                var store=range(pool,source); store.enableLogicalSchema(source); store.applyNoop(new ApplyPosition(1,0)); store.applySchemaFence(new ApplyPosition(1,1),change.header());
                assertThrows(org.rocksdb.RocksDBException.class,() -> pool.installSchema(store,new ApplyPosition(1,2),change,Map.of(fixture.column,"data"),Set.of(fixture.column),stage -> { if(stage==boundary) throw new org.rocksdb.RocksDBException("controlled boundary"); }));
            }
            try(var pool=open(path,source)) { var store=range(pool,source); assertArrayEquals(target.digest(),store.schemaDigest()); assertNotNull(loaded(pool).mergeOperators().get("data")); }
            var artifact=path.resolveSibling(path.getFileName()+".operators").resolve(HexFormat.of().formatHex(operator.artifactSha256())+".jar");
            byte[] exact=Files.readAllBytes(artifact); Files.delete(artifact); assertThrows(RuntimeException.class,() -> open(path,source));
            Files.write(artifact,new byte[]{1}); assertThrows(RuntimeException.class,() -> open(path,source)); Files.write(artifact,exact);
            try(var pool=open(path,source)) { assertArrayEquals(target.digest(),range(pool,source).schemaDigest()); }
        }
    }
    @Test void missingCurrentInNonemptyNativeDirectoryCannotAuthorizeNewBootstrap() throws Exception {
        var path=root.resolve("unidentified"); var source=empty(0);
        try(var pool=open(path,source)) { range(pool,source).enableLogicalSchema(source); }
        var index=path.resolveSibling(path.getFileName()+".operators").resolve("bindings"); byte[] original=Files.readAllBytes(index);
        Files.delete(path.resolve("CURRENT"));
        assertThrows(IllegalStateException.class,() -> DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()),fixture.source(),Map.of(fixture.column,"new"),Map.of()));
        assertArrayEquals(original,Files.readAllBytes(index));
        assertFalse(Files.exists(path.resolve("CURRENT")));
    }

    @Test void readOnlyMetadataOpenDoesNotRequireUserMergeCallbacks() throws Exception {
        org.rocksdb.RocksDB.loadLibrary(); var path=root.resolve("readonly-probe");
        var names=List.of("default","data",DistributedStoragePool.STATE_COLUMN);
        try(var dbOptions=new org.rocksdb.DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
            var plain=new org.rocksdb.ColumnFamilyOptions(); var merge=new org.rocksdb.ColumnFamilyOptions();
            var operator=new org.rocksdb.StringAppendOperator()) {
            merge.setMergeOperator(operator); var handles=new ArrayList<org.rocksdb.ColumnFamilyHandle>();
            var descriptors=List.of(new org.rocksdb.ColumnFamilyDescriptor(names.get(0).getBytes(),plain),new org.rocksdb.ColumnFamilyDescriptor(names.get(1).getBytes(),merge),new org.rocksdb.ColumnFamilyDescriptor(names.get(2).getBytes(),plain));
            try(var db=org.rocksdb.RocksDB.open(dbOptions,path.toString(),descriptors,handles)) {
                try { assertEquals(names.get(2),new String(handles.get(2).getName()));
                    db.merge(handles.get(1),new byte[]{1},new byte[]{7}); db.put(handles.get(2),new byte[]{1},new byte[]{9});
                    assertArrayEquals(new byte[]{9},db.get(handles.get(2),new byte[]{1}),"Metadata exists before close");
                    try(var flush=new FlushOptions().setWaitForFlush(true)) { db.flush(flush,handles); }
                    try(var writes=new org.rocksdb.WriteOptions().setSync(true)) { db.put(handles.get(2),writes,new byte[]{2},new byte[]{8}); }
                    assertArrayEquals(new byte[]{8},db.get(handles.get(2),new byte[]{2}),"Forced WAL metadata exists before close");
                } finally { handles.forEach(org.rocksdb.ColumnFamilyHandle::close); }
            }
        }
        try(var dbOptions=new org.rocksdb.DBOptions(); var plain=new org.rocksdb.ColumnFamilyOptions()) {
            var handles=new ArrayList<org.rocksdb.ColumnFamilyHandle>();
            var descriptors=names.stream().map(name -> new org.rocksdb.ColumnFamilyDescriptor(name.getBytes(),plain)).toList();
            try(var db=org.rocksdb.RocksDB.openReadOnly(dbOptions,path.toString(),descriptors,handles)) {
                try { assertEquals(names.get(2),new String(handles.get(2).getName()));
                    assertArrayEquals(new byte[]{9},db.get(handles.get(2),new byte[]{1}),"Read-only SST metadata");
                    assertArrayEquals(new byte[]{8},db.get(handles.get(2),new byte[]{2}),"Read-only forced WAL metadata");
                    assertThrows(org.rocksdb.RocksDBException.class,() -> db.put(handles.get(2),new byte[]{2},new byte[]{3}));
                } finally { handles.forEach(org.rocksdb.ColumnFamilyHandle::close); }
            }
        }
    }

    @Test void rejectedAndCanceledPreparationDoesNotPublishPhysicalBindings() throws Exception {
        byte[] jar=DistributedMergeOperatorRegistryTest.classes(AppendOperator.class); var source=empty(0); var target=target(jar); var path=root.resolve("prepare");
        DistributedStoragePool.stageOperator(path,target.column(fixture.column).operator(),new MergeOperatorRegistry.DeterministicContract("append-contract",1,true),jar);
        var change=fixture.change(source,target,List.of(fixture.descriptor(fixture.group,null,null)));
        try(var pool=open(path,source)) {
            var store=range(pool,source); store.enableLogicalSchema(source); store.applyNoop(new ApplyPosition(1,0)); store.applySchemaFence(new ApplyPosition(1,1),change.header());
            var index=path.resolveSibling(path.getFileName()+".operators").resolve("bindings"); byte[] original=Files.readAllBytes(index);
            assertThrows(IllegalArgumentException.class,() -> pool.prepareSchemaInstallation(store,change,Map.of(fixture.column,DistributedStoragePool.STATE_COLUMN),Set.of(fixture.column)));
            assertArrayEquals(original,Files.readAllBytes(index));
            try(var canceled=pool.prepareSchemaInstallation(store,change,Map.of(fixture.column,"canceled"),Set.of(fixture.column))) { assertArrayEquals(original,Files.readAllBytes(index)); }
            try(var accepted=pool.prepareSchemaInstallation(store,change,Map.of(fixture.column,"accepted"),Set.of(fixture.column))) {
                assertArrayEquals(original,Files.readAllBytes(index)); var installed=pool.installSchema(store,new ApplyPosition(1,2),accepted); assertEquals("accepted",installed.targetNames().get(fixture.column));
            }
            assertFalse(Arrays.equals(original,Files.readAllBytes(index)));
        }
        try(var pool=open(path,source)) { assertArrayEquals(target.digest(),range(pool,source).schemaDigest()); assertNotNull(loaded(pool).mergeOperators().get("accepted")); }
    }
    @Test void allPlainMetadataBindingsReopenAndChangedOriginalFingerprintFails() throws Exception {
        var source=empty(0); var path=root.resolve("metadata"); var names=new HashMap<UUID,String>();
        for(var name:List.of("catalogue","sessions","home-a","home-b")) names.put(UUID.randomUUID(),name);
        try(var pool=DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()),source,Map.of(),names)) {
            for(var entry:names.entrySet()) pool.openRange(new RangeOwnership.Owner(UUID.randomUUID(),1),new RangeOwnership.Interval(UUID.randomUUID(),Set.of(entry.getKey()),null,null),source.digest(),Map.of(entry.getKey(),entry.getValue()),Set.of()).applyNoop(new ApplyPosition(1,0));
            for(var name:names.values()) assertNull(loaded(pool).mergeOperators().get(name));
        }
        try(var pool=DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()),source,Map.of(),names)) { assertTrue(loaded(pool).definitiveColumnFamilyOptionsMap().keySet().containsAll(names.values())); }
        var changed=new HashMap<>(names); changed.put(names.keySet().iterator().next(),"different");
        assertThrows(IllegalStateException.class,() -> DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()),source,Map.of(),changed));
        assertThrows(IllegalStateException.class,() -> DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()),empty(1),Map.of(),names));
    }
    @Test void foreignCacheWithSameOriginalBootstrapCannotAttachDifferentInstalledOperator() throws Exception {
        var source=empty(0); byte[] jar=DistributedMergeOperatorRegistryTest.classes(AppendOperator.class); var target=target(jar);
        var paths=List.of(root.resolve("first-cache"),root.resolve("foreign-cache"));
        for(int i=0;i<paths.size();i++) {
            var original=target.column(fixture.column).operator();
            var operator=i==0 ? original : new DistributedSchema.Operator(original.name(),original.version(),original.className(),original.artifactSha256(),original.valueCodecVersion(),original.operandCodecVersion(),original.deterministicContract(),2);
            var column=target.column(fixture.column); var schema=new DistributedSchema(fixture.cluster,fixture.database,fixture.domain,1,0,List.of(new DistributedSchema.Column(column.apiId(),column.columnId(),column.name(),column.schema(),operator)));
            var path=paths.get(i); DistributedStoragePool.stageOperator(path,operator,new MergeOperatorRegistry.DeterministicContract("append-contract",operator.deterministicContractVersion(),true),jar);
            var change=fixture.change(source,schema,List.of(fixture.descriptor(fixture.group,null,null)));
            try(var pool=open(path,source)) { var store=range(pool,source); store.enableLogicalSchema(source); store.applyNoop(new ApplyPosition(1,0)); store.applySchemaFence(new ApplyPosition(1,1),change.header()); pool.installSchema(store,new ApplyPosition(1,2),change,Map.of(fixture.column,"data"),Set.of(fixture.column)); }
        }
        var first=paths.getFirst().resolveSibling(paths.getFirst().getFileName()+".operators"); var foreign=paths.getLast().resolveSibling(paths.getLast().getFileName()+".operators");
        byte[] good=Files.readAllBytes(first.resolve("bindings"));
        try(var entries=Files.newDirectoryStream(foreign)) { for(var file:entries) if(!file.getFileName().toString().equals("lock")) Files.copy(file,first.resolve(file.getFileName()),StandardCopyOption.REPLACE_EXISTING); }
        var nativePath=paths.getFirst(); byte[] current=Files.readAllBytes(nativePath.resolve("CURRENT"));
        var manifest=nativePath.resolve(new String(current,java.nio.charset.StandardCharsets.US_ASCII).trim()); byte[] manifestBytes=Files.readAllBytes(manifest);
        var rejected=assertThrows(IllegalStateException.class,() -> open(nativePath,source)); assertInstanceOf(java.io.IOException.class,rejected.getCause());
        assertTrue(rejected.getCause().getMessage().contains("operator tuple"));
        assertArrayEquals(current,Files.readAllBytes(nativePath.resolve("CURRENT"))); assertArrayEquals(manifestBytes,Files.readAllBytes(manifest),"Read-only preflight cannot mutate native schema");
        Files.write(first.resolve("bindings"),good); try(var pool=open(nativePath,source)) { assertArrayEquals(target.digest(),range(pool,source).schemaDigest()); }
    }

    DistributedSchema bucketTarget(byte[] jar) throws Exception {
        var fixed=target(jar); var column=fixed.columns().getFirst(); var operator=column.operator();
        var shape=it.cavallium.rockserver.core.common.ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(it.cavallium.rockserver.core.common.ColumnHashType.ALLSAME8),true,operator.name(),operator.version(),operator.className());
        return new DistributedSchema(fixed.clusterId(),fixed.databaseId(),fixed.domainId(),fixed.schemaEpoch(),0,List.of(new DistributedSchema.Column(column.apiId(),column.columnId(),column.name(),shape,operator)));
    }
    FFMAbstractMergeOperator delegate(FFMAbstractMergeOperator wrapper) throws Exception {
        var field=DistributedBucketMergeOperator.class.getDeclaredField("delegate"); field.setAccessible(true); return (FFMAbstractMergeOperator)field.get(wrapper);
    }
    @Test void typedBucketInstallationUsesDeferredNativeOperandsAndRetainsChildThroughDropAndDbClose() throws Exception {
        byte[] jar=DistributedMergeOperatorRegistryTest.classes(AppendOperator.class); var source=empty(0); var target=bucketTarget(jar); var terminal=empty(2); Path path=root.resolve("typed-bucket");
        var operator=target.column(fixture.column).operator(); DistributedStoragePool.stageOperator(path,operator,new MergeOperatorRegistry.DeterministicContract("append-contract",1,true),jar);
        var create=fixture.change(source,target,List.of(fixture.descriptor(fixture.group,null,null))); FFMAbstractMergeOperator wrapped,child;
        try(var pool=open(path,source)) {
            var store=range(pool,source); store.enableLogicalSchema(source); store.applyNoop(new ApplyPosition(1,0)); store.applySchemaFence(new ApplyPosition(1,1),create.header());
            var installed=pool.installSchema(store,new ApplyPosition(1,2),create,Map.of(fixture.column,"data"),Set.of(fixture.column)); store.applySchemaActivation(new ApplyPosition(1,3),installed.proof(),UUID.randomUUID(),new ApplyPosition(2,10));
            wrapped=loaded(pool).mergeOperators().get("data"); assertInstanceOf(DistributedBucketMergeOperator.class,wrapped); child=delegate(wrapped);
            var db=loaded(pool); var cf=db.db().getStartupColumns().entrySet().stream().filter(e -> Arrays.equals(e.getKey().getName(),"data".getBytes())).map(Map.Entry::getValue).findFirst().orElse(null);
            if(cf==null) { var handles=DistributedStoragePool.class.getDeclaredField("handles"); handles.setAccessible(true); cf=((Map<String,org.rocksdb.ColumnFamilyHandle>)handles.get(pool)).get("data"); }
            byte[] key={7,0}; var one=List.of(new byte[]{1}); var two=List.of(new byte[]{2});
            db.db().get().merge(cf,key,BucketMergeOperand.encode(BucketMergeOperand.Type.PUT,one,new byte[]{10}));
            db.db().get().merge(cf,key,BucketMergeOperand.encode(BucketMergeOperand.Type.PUT,two,new byte[]{20}));
            db.db().get().merge(cf,key,BucketMergeOperand.encode(BucketMergeOperand.Type.MERGE,one,new byte[]{11}));
            try(var flush=new FlushOptions().setWaitForFlush(true)) { db.db().get().flush(flush); } db.db().get().compactRange(cf);
            var result=DistributedBucket.decode(target.column(fixture.column).schema(),db.db().get().get(cf,key)); assertArrayEquals(new byte[]{10,11},result.get(one)); assertArrayEquals(new byte[]{20},result.get(two));
            var delete=new SchemaChange.Definition(UUID.randomUUID(),new OperationId(UUID.randomUUID(),1,0),create.targetDirectory(),target,terminal); store.applySchemaFence(new ApplyPosition(1,4),delete.header());
            var removed=pool.installSchema(store,new ApplyPosition(1,5),delete,Map.of(),Set.of()); store.applySchemaActivation(new ApplyPosition(1,6),removed.proof(),UUID.randomUUID(),new ApplyPosition(2,20));
            assertTrue(wrapped.isOwningHandle()); assertTrue(child.isOwningHandle(),"Dropped bucket delegate must remain owned until native DB release");
        }
        assertFalse(wrapped.isOwningHandle()); assertFalse(child.isOwningHandle());
    }
    @Test void closingUnappliedTypedBucketInstallPlanReleasesWrapperAndDelegate() throws Exception {
        byte[] jar=DistributedMergeOperatorRegistryTest.classes(AppendOperator.class); var source=empty(0); var target=bucketTarget(jar); var path=root.resolve("cancel-bucket-plan");
        var operator=target.column(fixture.column).operator(); DistributedStoragePool.stageOperator(path,operator,new MergeOperatorRegistry.DeterministicContract("append-contract",1,true),jar);
        var create=fixture.change(source,target,List.of(fixture.descriptor(fixture.group,null,null)));
        try(var pool=open(path,source)) {
            var store=range(pool,source); store.enableLogicalSchema(source); store.applyNoop(new ApplyPosition(1,0)); store.applySchemaFence(new ApplyPosition(1,1),create.header());
            var plan=pool.prepareSchemaInstallation(store,create,Map.of(fixture.column,"data"),Set.of(fixture.column));
            var field=plan.getClass().getDeclaredField("options"); field.setAccessible(true); var prepared=((Map<?,?>)field.get(plan)).get("data");
            var options=prepared.getClass().getDeclaredField("options"); options.setAccessible(true); var callback=((RocksDBLoader.ColumnOptionsWithMerge)options.get(prepared)).mergeOperator(); var child=delegate(callback);
            assertTrue(callback.isOwningHandle()); assertTrue(child.isOwningHandle()); plan.close(); assertFalse(callback.isOwningHandle()); assertFalse(child.isOwningHandle());
        }
    }

    @Test void qualifiedBucketBootstrapReopenAndImageImportKeepTheNativeWrapper() throws Exception {
        byte[] jar=DistributedMergeOperatorRegistryTest.classes(AppendOperator.class); var schema=bucketTarget(jar); var operator=schema.column(fixture.column).operator(); Path path=root.resolve("bucket-bootstrap"),image=root.resolve("bucket-bootstrap.image");
        var names=Map.of(fixture.column,"bucket"); var limits=new NativeRangeImage.Limits(3*1024*1024,32*1024*1024,256);
        DistributedStoragePool.stageOperator(path,operator,new MergeOperatorRegistry.DeterministicContract("append-contract",1,true),jar);
        for(int round=0;round<2;round++) try(var pool=DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()),schema,names,Map.of())) {
            var store=pool.openRange(new RangeOwnership.Owner(fixture.group,1),new RangeOwnership.Interval(fixture.domain,schema.columnIds(),null,null),schema.digest(),names,Set.of(fixture.column)); store.enableLogicalSchema(schema);
            assertInstanceOf(DistributedBucketMergeOperator.class,loaded(pool).mergeOperators().get("bucket"));
            if(round==0) {
                store.applyNoop(new ApplyPosition(1,0));
                var seed=DistributedCommand.bucketData(fixture.group,1,schema.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(Mutation.bucketPut(fixture.column,new byte[]{7,0},List.of(new byte[]{1}),new byte[]{10},Mutation.ReturnMode.NONE),Mutation.bucketPut(fixture.column,new byte[]{7,0},List.of(new byte[]{2}),new byte[]{20},Mutation.ReturnMode.NONE)),List.of(),-1);
                store.apply(new ApplyPosition(1,1),seed);
                var merge=DistributedCommand.bucketData(fixture.group,1,schema.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(Mutation.bucketMerge(fixture.column,new byte[]{7,0},List.of(new byte[]{1}),new byte[]{11},Mutation.ReturnMode.CURRENT)),List.of(),-1);
                assertArrayEquals(new byte[]{10,11},CommandResult.decode(store.apply(new ApplyPosition(1,2),merge)).value(0));
                try(var flush=new FlushOptions().setWaitForFlush(true)) { loaded(pool).db().get().flush(flush); } loaded(pool).db().get().compactRange();
            }
            try(var pin=store.capture()) { var bucket=DistributedBucket.decode(schema.column(fixture.column).schema(),pin.get(fixture.column,new byte[]{7,0})); assertArrayEquals(new byte[]{10,11},bucket.get(List.of(new byte[]{1}))); assertArrayEquals(new byte[]{20},bucket.get(List.of(new byte[]{2}))); }
            if(round==1) NativeRangeImage.write(image,store,limits);
        }
        Path target=root.resolve("bucket-import"); DistributedStoragePool.stageOperator(target,operator,new MergeOperatorRegistry.DeterministicContract("append-contract",1,true),jar);
        try(var pool=DistributedStoragePool.open(target,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()),schema,names,Map.of())) {
            var store=pool.openRange(new RangeOwnership.Owner(fixture.group,1),new RangeOwnership.Interval(fixture.domain,schema.columnIds(),null,null),schema.digest(),names,Set.of(fixture.column)); store.enableLogicalSchema(schema); store.fence(); store.installSnapshot(image,limits); store.activate();
            assertInstanceOf(DistributedBucketMergeOperator.class,loaded(pool).mergeOperators().get("bucket"));
            var merge=DistributedCommand.bucketData(fixture.group,1,schema.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(Mutation.bucketMerge(fixture.column,new byte[]{7,0},List.of(new byte[]{2}),new byte[]{21},Mutation.ReturnMode.CURRENT)),List.of(),-1);
            assertArrayEquals(new byte[]{20,21},CommandResult.decode(store.apply(new ApplyPosition(1,3),merge)).value(0));
            try(var pin=store.capture()) { var bucket=DistributedBucket.decode(schema.column(fixture.column).schema(),pin.get(fixture.column,new byte[]{7,0})); assertArrayEquals(new byte[]{10,11},bucket.get(List.of(new byte[]{1}))); }
        }
    }

}
