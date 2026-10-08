package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** Native ordered bucket effects/receipts; no consensus, transport or power-loss claims. */
class NativeBucketApplyTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final UUID cluster=new UUID(1,1),database=new UUID(1,2),domain=new UUID(1,3),group=new UUID(1,4),session=new UUID(1,5),
            bucket=new UUID(2,1),fixed=new UUID(2,2),keyOnly=new UUID(2,3),hashed=new UUID(2,4);
    final Map<UUID,String> names=Map.of(bucket,"bucket",fixed,"fixed",keyOnly,"keyOnly",hashed,"hashed");
    ColumnSchema bucketShape(boolean value,ColumnHashType hash) { return ColumnSchema.of(IntArrayList.of(1),ObjectArrayList.of(hash),value); }
    DistributedSchema schema() {
        return new DistributedSchema(cluster,database,domain,1,0,List.of(
                new DistributedSchema.Column(1,bucket,"bucket",bucketShape(true,ColumnHashType.ALLSAME8),null),
                new DistributedSchema.Column(2,fixed,"fixed",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null),
                new DistributedSchema.Column(3,keyOnly,"keyOnly",bucketShape(false,ColumnHashType.ALLSAME8),null),
                new DistributedSchema.Column(4,hashed,"hashed",bucketShape(true,ColumnHashType.XXHASH8),null)));
    }
    byte[] physical(int fixed) { return new byte[]{(byte)fixed,0}; }
    List<byte[]> member(int key) { return List.of(new byte[]{(byte)key}); }
    Mutation put(UUID column,int row,int key,byte[] value,Mutation.ReturnMode returns) { return Mutation.bucketPut(column,physical(row),member(key),value,returns); }
    DistributedCommand command(DistributedSchema schema,long sequence,List<Mutation> mutations,List<KeyPredicate> predicates,long ack) {
        return DistributedCommand.bucketData(group,1,schema.digest(),new OperationId(session,1,sequence),mutations,predicates,ack);
    }
    final class Fixture implements AutoCloseable {
        final DistributedSchema schema=schema(); final DistributedStoragePool pool; final ReplicatedRangeStore store;
        Fixture(Path path,boolean qualify) throws Exception {
            pool=DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
            store=pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(domain,schema.columnIds(),null,null),schema.digest(),names,Set.of());
            if (qualify) store.enableLogicalSchema(schema);
        }
        CommandResult apply(long index,DistributedCommand command) throws Exception { return CommandResult.decode(store.apply(new ApplyPosition(1,index),command)); }
        byte[] raw(UUID column,byte[] key) throws Exception { try (var pin=store.capture()) { return pin.get(column,key); } }
        DistributedBucket row(int row) throws Exception { return schema.decodeBucket(bucket,physical(row),raw(bucket,physical(row))); }
        public void close() throws Exception { pool.close(); }
    }
    @Test void orderedCollisionsOriginalPredicatesMixedColumnsAndRetrySaveLogicalReturns() throws Exception {
        try (var f=new Fixture(root.resolve("ordered"),true)) {
            var original=command(f.schema,0,List.of(
                    new Mutation(Mutation.Kind.PUT,fixed,new byte[]{7},new byte[]{5},Mutation.ReturnMode.PREVIOUS),
                    put(bucket,7,1,new byte[]{1},Mutation.ReturnMode.NONE),put(bucket,7,1,new byte[]{2},Mutation.ReturnMode.PREVIOUS),
                    put(bucket,7,2,new byte[]{3},Mutation.ReturnMode.CURRENT),Mutation.bucketDelete(bucket,physical(7),member(1),Mutation.ReturnMode.PREVIOUS),
                    put(bucket,7,1,new byte[]{4},Mutation.ReturnMode.CURRENT)),List.of(KeyPredicate.bucketMember(bucket,physical(7),member(1),null)),-1);
            var result=f.apply(0,original); assertEquals(CommandResult.Status.SUCCESS,result.status()); assertEquals(5,result.valueCount());
            assertNull(result.value(0)); assertArrayEquals(new byte[]{1},result.value(1)); assertArrayEquals(new byte[]{3},result.value(2));
            assertArrayEquals(new byte[]{2},result.value(3)); assertArrayEquals(new byte[]{4},result.value(4));
            assertArrayEquals(new byte[]{2},f.row(7).members().getFirst().tails().getFirst()); assertArrayEquals(new byte[]{5},f.raw(fixed,new byte[]{7}));
            var later=command(f.schema,1,List.of(put(bucket,7,1,new byte[]{9},Mutation.ReturnMode.NONE)),List.of(),-1); f.apply(1,later);
            assertArrayEquals(result.encode(),f.apply(2,original).encode()); assertArrayEquals(new byte[]{9},f.row(7).get(member(1)));
            var ack=command(f.schema,2,List.of(Mutation.bucketDelete(bucket,physical(7),member(2),Mutation.ReturnMode.NONE)),List.of(),1); f.apply(3,ack);
            assertEquals(1,f.store.receiptUsage().retainedCount()); assertNull(f.row(7).get(member(2))); assertEquals(new ApplyPosition(1,3),f.store.applied());
        }
    }
    @Test void keyOnlyAbsenceConflictAndDeletingLastMemberPreserveEmptyPhysicalRow() throws Exception {
        try (var f=new Fixture(root.resolve("presence"),true)) {
            var insert=command(f.schema,0,List.of(put(keyOnly,7,1,new byte[0],Mutation.ReturnMode.PREVIOUS),put(keyOnly,7,2,new byte[0],Mutation.ReturnMode.CURRENT)),List.of(),-1);
            var values=f.apply(0,insert); assertNull(values.value(0)); assertArrayEquals(new byte[0],values.value(1));
            var conflict=command(f.schema,1,List.of(new Mutation(Mutation.Kind.PUT,fixed,new byte[]{1},new byte[]{9}),Mutation.bucketDelete(keyOnly,physical(7),member(2),Mutation.ReturnMode.NONE)),
                    List.of(KeyPredicate.bucketMember(keyOnly,physical(7),member(1),null)),-1);
            assertEquals(CommandResult.Status.CONFLICT,f.apply(1,conflict).status()); assertNull(f.raw(fixed,new byte[]{1}));
            var remove=command(f.schema,2,List.of(Mutation.bucketDelete(keyOnly,physical(7),member(1),Mutation.ReturnMode.PREVIOUS),Mutation.bucketDelete(keyOnly,physical(7),member(2),Mutation.ReturnMode.CURRENT)),List.of(),-1);
            var deleted=f.apply(2,remove); assertArrayEquals(new byte[0],deleted.value(0)); assertNull(deleted.value(1)); assertArrayEquals(new byte[0],f.raw(keyOnly,physical(7)));
        }
    }
    @Test void bucketAndAggregateLimitsSaveNoEffectsEvenWhenNoneIsReturned() throws Exception {
        try (var f=new Fixture(root.resolve("limits"),true)) {
            var huge=DistributedBucket.empty(f.schema.column(bucket).schema()).put(List.of(new byte[0]),new byte[DistributedBucket.MAX_BYTES-512]);
            var seed=new DistributedCommand(group,1,f.schema.digest(),new OperationId(session,1,0),List.of(new Mutation(Mutation.Kind.PUT,bucket,physical(1),huge.encode()))); f.apply(0,seed);
            f.apply(1,command(f.schema,1,List.of(put(bucket,1,1,new byte[495],Mutation.ReturnMode.NONE)),List.of(),-1));
            assertEquals(DistributedBucket.MAX_BYTES,f.raw(bucket,physical(1)).length,"Stored bucket may exceed one raw command envelope while fitting a complete reply");
            var overflow=command(f.schema,2,List.of(new Mutation(Mutation.Kind.PUT,fixed,new byte[]{1},new byte[]{9}),put(bucket,1,2,new byte[1024],Mutation.ReturnMode.NONE)),List.of(),-1);
            assertEquals(CommandResult.Status.RESULT_TOO_LARGE,f.apply(2,overflow).status()); assertNull(f.raw(fixed,new byte[]{1})); assertNull(f.row(1).get(member(2)));
            long index=3; var mutations=new ArrayList<Mutation>();
            for (int row=10;row<19;row++) {
                var raw=DistributedBucket.empty(f.schema.column(bucket).schema()).put(member(1),new byte[1024*1024]).put(member(2),new byte[]{2}).encode();
                f.apply(index,new DistributedCommand(group,1,f.schema.digest(),new OperationId(session,1,index),List.of(new Mutation(Mutation.Kind.PUT,bucket,physical(row),raw)))); index++;
                mutations.add(Mutation.bucketDelete(bucket,physical(row),member(2),Mutation.ReturnMode.NONE));
            }
            mutations.add(new Mutation(Mutation.Kind.PUT,fixed,new byte[]{2},new byte[]{9})); var aggregate=command(f.schema,index,mutations,List.of(),-1);
            assertEquals(CommandResult.Status.RESULT_TOO_LARGE,f.apply(index,aggregate).status()); assertNull(f.raw(fixed,new byte[]{2}));
            for (int row=10;row<19;row++) assertArrayEquals(new byte[]{2},f.row(row).get(member(2)));
            assertEquals(CommandResult.Status.RESULT_TOO_LARGE,f.apply(index+1,overflow).status());
        }
    }
    @Test void qualificationMalformedRowsAndPrecomputedPlansCannotEscapeAtomicApply() throws Exception {
        try (var f=new Fixture(root.resolve("invalid"),false)) {
            var logical=command(f.schema,0,List.of(put(bucket,7,1,new byte[]{1},Mutation.ReturnMode.NONE)),List.of(),-1);
            assertThrows(UnsupportedOperationException.class,() -> f.apply(0,logical)); f.store.enableLogicalSchema(f.schema);
            assertThrows(IllegalArgumentException.class,() -> f.store.apply(new ApplyPosition(1,0),logical,new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode()));
            assertDoesNotThrow(() -> command(f.schema,0,List.of(put(bucket,7,1,new byte[]{1},Mutation.ReturnMode.NONE),new Mutation(Mutation.Kind.DELETE_RANGE,fixed,new byte[]{1},new byte[]{2})),List.of(),-1));
            f.apply(0,new DistributedCommand(group,1,f.schema.digest(),new OperationId(session,1,1),List.of(new Mutation(Mutation.Kind.PUT,bucket,physical(7),new byte[]{1,2,3}))));
            assertThrows(IllegalArgumentException.class,() -> f.apply(1,logical)); assertEquals(new ApplyPosition(1,0),f.store.applied()); assertEquals(1,f.store.receiptUsage().retainedCount());
        }
    }
    @Test void reopenAndSnapshotInstallKeepOriginalBucketReceiptAndLogicalState() throws Exception {
        Path image=root.resolve("image"), source=root.resolve("source"); var limits=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4);
        DistributedCommand original; byte[] saved;
        try (var f=new Fixture(source,true)) {
            f.store.initializeConfiguration(new byte[]{1}); original=command(f.schema,0,List.of(put(bucket,7,1,new byte[]{9},Mutation.ReturnMode.PREVIOUS)),List.of(),-1);
            saved=f.apply(0,original).encode(); try (var pin=f.store.capture()) { NativeRangeImage.write(image,pin,f.store.ownership().interval(),limits); }
        }
        try (var f=new Fixture(source,true)) { assertArrayEquals(saved,f.apply(1,original).encode()); assertArrayEquals(new byte[]{9},f.row(7).get(member(1))); }
        try (var f=new Fixture(root.resolve("target"),true)) {
            f.store.fence(); f.store.installSnapshot(image,limits); f.store.activate(); assertArrayEquals(saved,f.apply(1,original).encode()); assertArrayEquals(new byte[]{9},f.row(7).get(member(1)));
        }
    }
    @Test void misplacedStoredMembersAndProjectionMismatchFailBeforeAnyUserEffects() throws Exception {
        try (var f=new Fixture(root.resolve("misplaced"),true)) {
            var encoded=f.schema.encodeLogicalKey(hashed,new Keys(it.cavallium.buffer.Buf.wrap(new byte[]{7}),it.cavallium.buffer.Buf.wrap(new byte[]{1})));
            int other=2;
            while (other<256 && Arrays.equals(encoded.physicalKey(),f.schema.encodeLogicalKey(hashed,new Keys(it.cavallium.buffer.Buf.wrap(new byte[]{7}),it.cavallium.buffer.Buf.wrap(new byte[]{(byte)other}))).physicalKey())) other++;
            assertTrue(other<256); var row=DistributedBucket.empty(f.schema.column(hashed).schema()).put(member(other),new byte[]{9});
            f.apply(0,new DistributedCommand(group,1,f.schema.digest(),new OperationId(session,1,1),List.of(new Mutation(Mutation.Kind.PUT,hashed,encoded.physicalKey(),row.encode()))));
            var incoming=command(f.schema,0,List.of(new Mutation(Mutation.Kind.PUT,fixed,new byte[]{1},new byte[]{9}),Mutation.bucketPut(hashed,encoded.physicalKey(),member(1),new byte[]{1},Mutation.ReturnMode.NONE)),List.of(),-1);
            assertThrows(IllegalArgumentException.class,() -> f.apply(1,incoming)); assertNull(f.raw(fixed,new byte[]{1})); assertEquals(new ApplyPosition(1,0),f.store.applied());
        }
        var prefix=new DistributedSchema(cluster,database,domain,1,1,schema().columns());
        try (var pool=DistributedStoragePool.open(root.resolve("bad-bounds"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()))) {
            var store=pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(domain,prefix.columnIds(),new byte[]{1,0},new byte[]{2,0}),prefix.digest(),names,Set.of());
            assertThrows(IllegalArgumentException.class,() -> store.enableLogicalSchema(prefix)); assertNull(store.applied());
        }
    }
    @Test void returnedMemberBudgetOverflowDiscardsEveryStagedChange() throws Exception {
        try (var f=new Fixture(root.resolve("result-limit"),true)) {
            byte[] old=new byte[1024*1024], replacement=old.clone(); replacement[0]=1;
            var row=DistributedBucket.empty(f.schema.column(bucket).schema()).put(member(1),old);
            f.apply(0,new DistributedCommand(group,1,f.schema.digest(),new OperationId(session,1,0),List.of(new Mutation(Mutation.Kind.PUT,bucket,physical(7),row.encode()))));
            var request=command(f.schema,1,List.of(new Mutation(Mutation.Kind.PUT,fixed,new byte[]{1},new byte[]{9}),put(bucket,7,2,new byte[]{2},Mutation.ReturnMode.NONE),
                    Mutation.bucketDelete(bucket,physical(7),member(1),Mutation.ReturnMode.PREVIOUS),put(bucket,7,1,replacement,Mutation.ReturnMode.CURRENT)),List.of(),-1);
            assertEquals(CommandResult.Status.RESULT_TOO_LARGE,f.apply(1,request).status()); assertNull(f.raw(fixed,new byte[]{1})); assertNull(f.row(7).get(member(2))); assertArrayEquals(old,f.row(7).get(member(1)));
        }
    }

}
