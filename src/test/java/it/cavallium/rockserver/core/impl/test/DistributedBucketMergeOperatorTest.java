package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.*;
import it.cavallium.rockserver.core.impl.rocksdb.DistributedBucketMergeOperator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;

class DistributedBucketMergeOperatorTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    static { RocksDB.loadLibrary(); }
    static ColumnSchema shape() { return ColumnSchema.of(IntArrayList.of(1),ObjectArrayList.of(ColumnHashType.ALLSAME8),true); }
    static List<byte[]> member(int value) { return List.of(new byte[]{(byte)value}); }
    static byte[] operand(BucketMergeOperand.Type type,int member,int value) { return BucketMergeOperand.encode(type,member(member),type==BucketMergeOperand.Type.DELETE ? null : new byte[]{(byte)value}); }
    static final class Recorder extends FFMByteArrayMergeOperator {
        int calls; byte[] key;
        Recorder() { super("native-member-test"); }
        @Override public byte[] merge(byte[] key,byte[] existing,List<byte[]> operands) {
            calls++; this.key=key.clone(); if(operands.getFirst()[0]==99) return null;
            var bytes=new java.io.ByteArrayOutputStream(); bytes.writeBytes(existing); operands.forEach(bytes::writeBytes); return bytes.toByteArray();
        }
    }
    @Test void orderedCollisionMembersNullNoopAbsentSeedAndEmptyDeleteUseStandaloneSemantics() {
        var delegate=new Recorder();
        try(var wrapper=new DistributedBucketMergeOperator(shape(),delegate)) {
            byte[] key={7,0}; var initial=DistributedBucket.empty(shape()).put(member(1),new byte[]{10}).put(member(2),new byte[]{20});
            byte[] encoded=wrapper.merge(key,initial.encode(),List.of(operand(BucketMergeOperand.Type.MERGE,1,11),operand(BucketMergeOperand.Type.MERGE,2,99),operand(BucketMergeOperand.Type.MERGE,3,30),operand(BucketMergeOperand.Type.PUT,4,40),operand(BucketMergeOperand.Type.DELETE,4,0)));
            var result=DistributedBucket.decode(shape(),encoded); assertArrayEquals(new byte[]{10,11},result.get(member(1))); assertArrayEquals(new byte[]{20},result.get(member(2))); assertArrayEquals(new byte[]{30},result.get(member(3))); assertNull(result.get(member(4)));
            assertEquals(2,delegate.calls); assertArrayEquals(key,delegate.key); assertNull(wrapper.partialMergeMulti(key,List.of(operand(BucketMergeOperand.Type.PUT,1,1),operand(BucketMergeOperand.Type.DELETE,1,0))));
            assertArrayEquals(new byte[0],wrapper.merge(key,encoded,List.of(operand(BucketMergeOperand.Type.DELETE,1,0),operand(BucketMergeOperand.Type.DELETE,2,0),operand(BucketMergeOperand.Type.DELETE,3,0))));
        }
        assertFalse(delegate.isOwningHandle());
    }
    @Test void actualNativeFullMergeFlushCompactionAndReopenRetainOnlyAddressedMembers() throws Exception {
        var path=root.resolve("native-wrapper");
        for(int round=0;round<2;round++) {
            var delegate=new Recorder();
            try(var wrapper=new DistributedBucketMergeOperator(shape(),delegate); var options=new Options().setCreateIfMissing(true).setMergeOperator(wrapper)) {
                try(var db=RocksDB.open(options,path.toString()); var flush=new FlushOptions().setWaitForFlush(true)) {
                    byte[] key={7,0};
                    if(round==0) { db.put(key,DistributedBucket.empty(shape()).put(member(2),new byte[]{20}).encode()); db.merge(key,operand(BucketMergeOperand.Type.MERGE,1,10)); }
                    db.merge(key,operand(BucketMergeOperand.Type.MERGE,1,11)); db.merge(key,operand(BucketMergeOperand.Type.MERGE,2,99)); db.flush(flush); db.compactRange();
                    var result=DistributedBucket.decode(shape(),db.get(key)); assertArrayEquals(round==0 ? new byte[]{10,11} : new byte[]{10,11,11},result.get(member(1))); assertArrayEquals(new byte[]{20},result.get(member(2))); assertTrue(delegate.calls>0); assertArrayEquals(key,delegate.key);
                }
            }
            assertFalse(delegate.isOwningHandle());
        }
    }
    @Test void constructionAdoptsPrivateDependencyAndRejectsUnownedHelpersWithoutLeakingPointers() {
        Recorder delegate; DistributedBucketMergeOperator wrapper;
        try(var construction=FFMAbstractMergeOperator.trackConstruction()) {
            delegate=new Recorder(); wrapper=new DistributedBucketMergeOperator(shape(),delegate); construction.adopt(wrapper);
        }
        assertTrue(delegate.isOwningHandle()); assertTrue(wrapper.isOwningHandle()); wrapper.close(); assertFalse(delegate.isOwningHandle()); assertFalse(wrapper.isOwningHandle());
        assertThrows(IllegalStateException.class,() -> wrapper.merge(new byte[]{7,0},null,List.of(operand(BucketMergeOperand.Type.MERGE,1,1))));
        Recorder orphan,child; DistributedBucketMergeOperator failed;
        try(var construction=FFMAbstractMergeOperator.trackConstruction()) {
            orphan=new Recorder(); child=new Recorder(); failed=new DistributedBucketMergeOperator(shape(),child);
            assertThrows(IllegalArgumentException.class,() -> construction.adopt(failed));
        }
        assertFalse(orphan.isOwningHandle()); assertFalse(child.isOwningHandle()); assertFalse(failed.isOwningHandle());
        Recorder invalid;
        try(var construction=FFMAbstractMergeOperator.trackConstruction()) {
            invalid=new Recorder(); assertThrows(IllegalArgumentException.class,() -> new DistributedBucketMergeOperator(ColumnSchema.of(IntArrayList.of(1),ObjectArrayList.of(),true),invalid));
        }
        assertFalse(invalid.isOwningHandle());
    }
}
