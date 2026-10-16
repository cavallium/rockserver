package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

class NativeBucketMergeTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final UUID cluster=new UUID(31,1),database=new UUID(31,2),domain=new UUID(31,3),group=new UUID(31,4),column=new UUID(31,5),session=new UUID(31,6);
    public static class KeySensitiveOperator extends FFMByteArrayMergeOperator {
        public KeySensitiveOperator(String name) { super(name); }
        @Override public byte[] merge(byte[] key,byte[] existing,List<byte[]> operands) {
            if(operands.getLast().length==1 && operands.getLast()[0]==0) return null;
            var bytes=new java.io.ByteArrayOutputStream(); if(existing!=null) bytes.writeBytes(existing);
            for(var operand:operands) { bytes.writeBytes(operand); bytes.write(key[0]); } return bytes.toByteArray();
        }
    }
    byte[] key(int row) { return new byte[]{(byte)row,0}; }
    List<byte[]> member(int n) { return List.of(new byte[]{(byte)n}); }
    Mutation put(int n,byte[] value,Mutation.ReturnMode mode) { return Mutation.bucketPut(column,key(7),member(n),value,mode); }
    Mutation merge(int n,byte[] value,Mutation.ReturnMode mode) { return Mutation.bucketMerge(column,key(7),member(n),value,mode); }
    Mutation delete(int n,Mutation.ReturnMode mode) { return Mutation.bucketDelete(column,key(7),member(n),mode); }
    final class Fixture implements AutoCloseable {
        final DistributedSchema schema; final DistributedStoragePool pool; final ReplicatedRangeStore store;
        Fixture(Path path,boolean tracked) throws Exception {
            byte[] jar=DistributedMergeOperatorRegistryTest.classes(KeySensitiveOperator.class);
            var operator=new DistributedSchema.Operator("key-append",1,KeySensitiveOperator.class.getName(),MessageDigest.getInstance("SHA-256").digest(jar),3,2,"key-append",1);
            var shape=new ColumnSchema(IntArrayList.of(1,1),ObjectArrayList.of(ColumnHashType.ALLSAME8),true,operator.name(),operator.version(),operator.className());
            schema=new DistributedSchema(cluster,database,domain,1,0,List.of(new DistributedSchema.Column(1,column,"bucket",shape,operator)));
            DistributedStoragePool.stageOperator(path,operator,new MergeOperatorRegistry.DeterministicContract("key-append",1,true),jar);
            pool=DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()),List.of(schema),Map.of(column,"bucket"),Map.of());
            store=pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(domain,schema.columnIds(),null,null),schema.digest(),Map.of(column,"bucket"),Set.of(column)); store.enableLogicalSchema(schema);
            if(tracked) { store.initializeConfiguration(new byte[]{1}); store.enableScopedHistory(RangeMutationHistory.Limits.defaults()); }
        }
        DistributedCommand command(long sequence,Mutation... effects) { return DistributedCommand.bucketData(group,1,schema.digest(),new OperationId(session,1,sequence),List.of(effects),List.of(),-1); }
        CommandResult apply(long index,DistributedCommand command) throws Exception { return CommandResult.decode(store.apply(new ApplyPosition(1,index),command)); }
        DistributedBucket row() throws Exception { try(var pin=store.capture()) { return schema.decodeBucket(column,key(7),pin.get(column,key(7))); } }
        byte[] member(int n) throws Exception { return row().get(NativeBucketMergeTest.this.member(n)); }
        @Override public void close() throws Exception { pool.close(); }
    }
    RocksDBLoader.LoadedDb loaded(DistributedStoragePool pool) throws Exception { var field=DistributedStoragePool.class.getDeclaredField("loaded"); field.setAccessible(true); return (RocksDBLoader.LoadedDb)field.get(pool); }
    org.rocksdb.ColumnFamilyHandle handle(DistributedStoragePool pool) throws Exception { var field=DistributedStoragePool.class.getDeclaredField("handles"); field.setAccessible(true); return ((Map<String,org.rocksdb.ColumnFamilyHandle>)field.get(pool)).get("bucket"); }
    @Test void mixedCollisionsSelectedReturnsFlushReopenAndReceiptReplayPreserveOrderedOperands() throws Exception {
        var path=root.resolve("ordered"); var image=root.resolve("ordered.image"); var limits=new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,256); DistributedCommand original; byte[] receipt;
        try(var f=new Fixture(path,false)) {
            f.apply(0,f.command(0,put(1,new byte[]{10},Mutation.ReturnMode.NONE),put(2,new byte[]{20},Mutation.ReturnMode.CURRENT)));
            try(var old=f.store.capture()) {
                original=f.command(1,put(1,new byte[]{11},Mutation.ReturnMode.PREVIOUS),merge(1,new byte[]{1,2},Mutation.ReturnMode.CURRENT),delete(1,Mutation.ReturnMode.PREVIOUS),merge(1,new byte[]{3,4,5},Mutation.ReturnMode.CURRENT),merge(1,new byte[]{0},Mutation.ReturnMode.CURRENT),merge(2,new byte[]{9},Mutation.ReturnMode.NONE));
                var result=f.apply(1,original); receipt=result.encode(); assertEquals(CommandResult.Status.SUCCESS,result.status()); assertEquals(5,result.valueCount());
                assertArrayEquals(new byte[]{10},result.value(0));
                try(var standalone=new KeySensitiveOperator("standalone")) { assertArrayEquals(standalone.merge(key(7),new byte[]{11},List.of(new byte[]{1,2})),result.value(1)); }
                assertArrayEquals(new byte[]{11,1,2,7},result.value(2)); assertArrayEquals(new byte[]{3,4,5},result.value(3)); assertArrayEquals(result.value(3),result.value(4));
                assertArrayEquals(new byte[]{20,9,7},f.member(2)); assertArrayEquals(new byte[]{3,4,5},f.member(1)); assertArrayEquals(new byte[]{2},f.row().members().getFirst().tails().getFirst());
                var previous=f.schema.decodeBucket(column,key(7),old.get(column,key(7))); assertArrayEquals(new byte[]{10},previous.get(member(1))); assertArrayEquals(new byte[]{20},previous.get(member(2)));
            }
            assertArrayEquals(receipt,f.apply(2,original).encode()); assertArrayEquals(new byte[]{20,9,7},f.member(2));
            var db=loaded(f.pool).db().get(); try(var flush=new org.rocksdb.FlushOptions().setWaitForFlush(true)) { db.flush(flush,handle(f.pool)); } db.compactRange(handle(f.pool));
            NativeRangeImage.write(image,f.store,limits);
        }
        for(boolean install:List.of(false,true)) try(var f=new Fixture(install ? root.resolve("imported") : path,false)) {
            if(install) { f.store.fence(); f.store.installSnapshot(image,limits); f.store.activate(); }
            assertArrayEquals(receipt,f.apply(3,original).encode()); assertArrayEquals(new byte[]{20,9,7},f.member(2));
            f.apply(4,f.command(2,delete(1,Mutation.ReturnMode.NONE),delete(2,Mutation.ReturnMode.CURRENT))); assertTrue(f.row().members().isEmpty());
            try(var pin=f.store.capture()) { assertArrayEquals(new byte[0],pin.get(column,key(7))); }
        }
    }
    PreparedParticipant.Preparation prepare(Fixture f,DistributedCommand base,TransactionScope scope,UUID transaction) throws Exception {
        DistributedCommand accepted; try(var pin=f.store.captureTracked(scope)) { accepted=f.store.planScopedCommit(pin,base,0,new UUID(31,70)); }
        var participant=new TransactionStatus.Participant(domain, group, group, 1, f.schema.digest(), PreparedParticipant.Preparation.planDigest(accepted), null, false);
        var registration=TransactionStatus.begin(cluster, database, transaction, base.operationId(), List.of(new DomainBinding(domain, 0, new byte[32], f.schema.schemaEpoch(), f.schema.digest())), new byte[32], List.of(participant));
        return new PreparedParticipant.Preparation(new UUID(31,71),1,new OperationId(new UUID(31,72),1,base.operationId().sequence()),registration,accepted);
    }
    ParticipantFinalization commit(PreparedParticipant prepared) {
        var p=prepared.preparation(); var ready=p.registration().prepared(group,p.business().payloadDigest(),prepared.position());
        return new ParticipantFinalization(p.homeGroup(),p.homeEpoch(),group,new OperationId(new UUID(31,73),1,p.operation().sequence()),ready.decide(TransactionStatus.Phase.COMMIT));
    }
    @Test void stagedCompletePrefixRollbackRestagePreparedReopenAndFinalizationNeverDuplicateOperands() throws Exception {
        var path=root.resolve("staged"); var scope=new TransactionScope(List.of(TransactionScope.Span.point(column,key(7)))); PreparedParticipant.Preparation preparation; ParticipantFinalization terminal;
        try(var f=new Fixture(path,true)) {
            f.store.applyNoop(new ApplyPosition(1,0)); f.apply(1,f.command(0,put(1,new byte[]{10},Mutation.ReturnMode.NONE),put(2,new byte[]{20},Mutation.ReturnMode.NONE)));
            var prefix=f.command(1,merge(1,new byte[]{1},Mutation.ReturnMode.CURRENT),put(2,new byte[]{30},Mutation.ReturnMode.NONE));
            try(var pin=f.store.captureTracked(scope)) {
                try(var staged=pin.stage(prefix)) { assertArrayEquals(new byte[]{10,1,7},staged.result().value(0)); assertArrayEquals(new byte[]{10,1,7},staged.getMember(column,key(7),member(1))); assertArrayEquals(new byte[]{30},staged.getMember(column,key(7),member(2))); }
                assertArrayEquals(new byte[]{10},f.member(1)); assertArrayEquals(new byte[]{20},f.member(2));
                var complete=f.command(1,merge(1,new byte[]{1},Mutation.ReturnMode.CURRENT),put(2,new byte[]{30},Mutation.ReturnMode.NONE),merge(1,new byte[]{2},Mutation.ReturnMode.CURRENT));
                try(var staged=pin.stage(complete)) { assertArrayEquals(new byte[]{10,1,7,2,7},staged.result().value(1)); assertArrayEquals(new byte[]{10,1,7,2,7},staged.getMember(column,key(7),member(1))); }
                preparation=prepare(f,complete,scope,new UUID(31,74));
            }
            f.store.applyPrepared(new ApplyPosition(1,2),preparation); terminal=commit(f.store.preparedParticipant(preparation.transactionId())); assertArrayEquals(new byte[]{10},f.member(1)); assertArrayEquals(new byte[]{20},f.member(2));
        }
        try(var f=new Fixture(path,true)) {
            assertNotNull(f.store.preparedParticipant(preparation.transactionId())); var committed=NativeParticipantRetirementTest.appliedCommit(f.store,new ApplyPosition(1,3),terminal);
            assertArrayEquals(new byte[]{10,1,7,2,7},f.member(1)); assertArrayEquals(new byte[]{30},f.member(2));
            assertArrayEquals(preparation.business().acceptance().result(),f.store.committedResult(preparation.transactionId(),preparation.business().payloadDigest()));
            assertEquals(committed,NativeParticipantRetirementTest.appliedCommit(f.store,new ApplyPosition(1,4),terminal)); f.store.applyPrepared(new ApplyPosition(1,5),preparation);
            assertArrayEquals(new byte[]{10,1,7,2,7},f.member(1));
        }
    }
    @Test void plainBucketAndRawPhysicalMergeCannotBypassActivatedMemberOperator() throws Exception {
        var plain=new NativeBucketApplyTest(); try(var f=plain.new Fixture(root.resolve("plain"),true)) {
            var command=plain.command(f.schema,0,List.of(Mutation.bucketMerge(plain.bucket,plain.physical(7),plain.member(1),new byte[]{1},Mutation.ReturnMode.NONE)),List.of(),-1);
            assertThrows(IllegalArgumentException.class,() -> f.store.apply(new ApplyPosition(1,0),command)); assertNull(f.store.applied());
        }
        try(var f=new Fixture(root.resolve("raw-rejected"),false)) {
            var raw=new DistributedCommand(group,1,f.schema.digest(),new OperationId(session,1,0),List.of(new Mutation(Mutation.Kind.MERGE,column,key(7),new byte[]{1})));
            assertThrows(IllegalArgumentException.class,() -> f.store.apply(new ApplyPosition(1,0),raw)); assertNull(f.store.applied());
        }
    }
}
