package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native prepared-state substrate only; a Raft prepare envelope/reservation handoff and finalization remain separate. */
class NativePreparedParticipantTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeBucketApplyTest outer=new NativeBucketApplyTest();
    byte[] key(int n) { return new byte[]{(byte)n}; }
    TransactionScope point(UUID column,int n) { return new TransactionScope(List.of(TransactionScope.Span.point(column,key(n)))); }
    DistributedCommand put(NativeBucketApplyTest.Fixture f,long sequence,int n,int value) { return new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,sequence),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,key(n),key(value)))); }
    void enable(NativeBucketApplyTest.Fixture f) throws Exception { f.store.initializeConfiguration(new byte[]{1}); f.store.enableScopedHistory(RangeMutationHistory.Limits.defaults()); f.store.applyNoop(new ApplyPosition(1,0)); }
    PreparedParticipant.Preparation preparation(NativeBucketApplyTest.Fixture f,DistributedCommand base,TransactionScope scope,long n) throws Exception {
        DistributedCommand accepted;
        try (var pin=f.store.captureTracked(scope)) { accepted=f.store.planScopedCommit(pin,base,0,new UUID(0,99)); }
        var participant=new TransactionStatus.Participant(outer.group,outer.group,1,f.schema.digest(),PreparedParticipant.Preparation.planDigest(accepted),null,false);
        var registration=TransactionStatus.begin(outer.cluster,outer.database,outer.domain,new UUID(0,n),base.operationId(),1,f.schema.digest(),new byte[32],List.of(participant));
        return new PreparedParticipant.Preparation(new UUID(0,100),1,new OperationId(new UUID(0,101),1,n),registration,accepted);
    }
    @Test void completeScopeLocksAbsenceReadsAndRangesWithoutInstallingBusinessEffects() throws Exception {
        try (var f=outer.new Fixture(root.resolve("locks"),true)) {
            enable(f); f.apply(1,put(f,0,1,1));
            var scope=new TransactionScope(List.of(TransactionScope.Span.range(outer.fixed,key(1),key(4)),TransactionScope.Span.point(outer.bucket,outer.physical(7))));
            var base=put(f,10,2,2); var preparation=preparation(f,base,scope,1);
            try (var old=f.store.captureTracked(scope)) {
                var record=f.store.applyPrepared(new ApplyPosition(1,2),preparation); assertEquals(new ApplyPosition(1,2),record.position());
                assertNull(f.raw(outer.fixed,key(2))); assertArrayEquals(key(1),old.get(outer.fixed,key(1))); assertEquals(RangeMutationHistory.Status.ACTIVE,old.status());
                assertThrows(IllegalStateException.class,() -> f.store.planScopedCommit(old,put(f,11,3,3),0,new UUID(0,99)));
                assertThrows(IllegalStateException.class,() -> f.store.captureTracked(point(outer.fixed,3)));
                assertThrows(IllegalStateException.class,() -> f.store.captureReadable(point(outer.fixed,1)));
                assertThrows(IllegalStateException.class,() -> f.store.estimateReceiptRetention(put(f,12,3,3)));
                assertThrows(IllegalStateException.class,() -> f.apply(3,put(f,12,3,3))); assertEquals(new ApplyPosition(1,2),f.store.applied());
                var predicate=new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,13),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,key(4),key(4))),List.of(new KeyPredicate(outer.fixed,key(3),null)));
                assertThrows(IllegalStateException.class,() -> f.store.estimateReceiptRetention(predicate));
                assertThrows(IllegalArgumentException.class,() -> f.store.estimateReceiptRetention(put(f,10,4,4)));
                var reusedControl=new DistributedCommand(outer.group,1,f.schema.digest(),preparation.operation(),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,key(4),key(4))));
                assertThrows(IllegalArgumentException.class,() -> f.store.estimateReceiptRetention(reusedControl));
                var bucket=outer.command(f.schema,14,List.of(outer.put(outer.bucket,7,2,key(2),Mutation.ReturnMode.NONE)),List.of(),-1);
                assertThrows(IllegalStateException.class,() -> f.store.estimateReceiptRetention(bucket));
                assertEquals(CommandResult.Status.SUCCESS,f.apply(3,put(f,15,4,4)).status(),"Exclusive upper boundary remains disjoint");
                try (var disjoint=f.store.captureReadable(point(outer.fixed,4))) { assertArrayEquals(key(4),disjoint.get(outer.fixed,key(4))); }
                assertEquals(record,f.store.applyPrepared(new ApplyPosition(1,4),preparation)); assertEquals(1,f.store.preparedParticipantCount());
                var floor=DistributedCommand.advanceSessionEpochFloor(outer.group,1,f.schema.digest(),new OperationId(new UUID(0,102),2,0),2); f.apply(5,floor);
                assertEquals(record,f.store.preparedParticipant(preparation.transactionId())); assertThrows(IllegalStateException.class,() -> f.store.captureReadable(point(outer.fixed,2)));
            }
            assertEquals(0,f.store.activeSnapshotPins());
        }
    }
    @Test void nativeReopenAndPortableInstallRecoverPreparedPlanReceiptAndLockOwnership() throws Exception {
        Path source=root.resolve("source"),image=root.resolve("prepared-image"); var limits=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4);
        PreparedParticipant record;
        try (var f=outer.new Fixture(source,true)) {
            enable(f); var scope=new TransactionScope(List.of(TransactionScope.Span.point(outer.bucket,outer.physical(7))));
            var base=outer.command(f.schema,0,List.of(outer.put(outer.bucket,7,1,key(9),Mutation.ReturnMode.PREVIOUS)),List.of(),-1);
            var preparation=preparation(f,base,scope,1); record=f.store.applyPrepared(new ApplyPosition(1,1),preparation);
            assertEquals(record,PreparedParticipant.decode(record.encode())); assertArrayEquals(preparation.encode(),PreparedParticipant.Preparation.decode(preparation.encode()).encode());
            try (var pin=f.store.capture()) { NativeRangeImage.write(image,pin,f.store.ownership().interval(),limits); }
            NativeRangeImage.inspect(image,limits); assertNull(f.raw(outer.bucket,outer.physical(7)));
        }
        try (var f=outer.new Fixture(source,true)) {
            assertEquals(record,f.store.preparedParticipant(record.preparation().transactionId())); assertEquals(1,f.store.preparedParticipantCount());
            f.store.fence(); f.store.activate(); assertThrows(IllegalStateException.class,() -> f.store.captureReadable(record.preparation().scope()));
            assertNull(f.raw(outer.bucket,outer.physical(7)));
        }
        try (var f=outer.new Fixture(root.resolve("target"),true)) {
            f.store.fence(); f.store.installSnapshot(image,limits); f.store.activate();
            assertArrayEquals(record.encode(),f.store.preparedParticipant(record.preparation().transactionId()).encode());
            assertThrows(IllegalStateException.class,() -> f.store.captureTracked(record.preparation().scope())); assertNull(f.raw(outer.bucket,outer.physical(7)));
            assertEquals(0,f.store.receiptUsage().retainedCount(),"Prepared receipts have their independent bounded lifetime");
        }
    }
    @Test void duplicateIdentitiesAndOverlappingAcceptedPreparationsCannotReplaceAStoredPlan() throws Exception {
        try (var f=outer.new Fixture(root.resolve("collisions"),true)) {
            enable(f); var one=preparation(f,put(f,0,1,1),point(outer.fixed,1),1);
            var overlap=preparation(f,put(f,1,1,2),point(outer.fixed,1),2);
            var sameOperation=preparation(f,put(f,0,2,2),point(outer.fixed,2),3);
            var differentBusiness=preparation(f,put(f,2,2,2),point(outer.fixed,2),4);
            var crossedControl=new PreparedParticipant.Preparation(one.homeGroup(),one.homeEpoch(),one.business().operationId(),differentBusiness.registration(),differentBusiness.business());
            var crossedBase=new DistributedCommand(outer.group,1,f.schema.digest(),one.operation(),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,key(2),key(2))));
            var crossedBusiness=preparation(f,crossedBase,point(outer.fixed,2),5);
            var record=f.store.applyPrepared(new ApplyPosition(1,1),one);
            assertThrows(IllegalArgumentException.class,() -> f.store.applyPrepared(new ApplyPosition(1,2),crossedControl));
            assertThrows(IllegalArgumentException.class,() -> f.store.applyPrepared(new ApplyPosition(1,2),crossedBusiness));
            assertThrows(IllegalStateException.class,() -> f.store.applyPrepared(new ApplyPosition(1,2),overlap));
            assertThrows(IllegalArgumentException.class,() -> f.store.applyPrepared(new ApplyPosition(1,2),sameOperation));
            var changed=new PreparedParticipant.Preparation(one.homeGroup(),one.homeEpoch()+1,one.operation(),one.registration(),one.business());
            assertThrows(IllegalArgumentException.class,() -> f.store.applyPrepared(new ApplyPosition(1,2),changed));
            assertEquals(new ApplyPosition(1,1),f.store.applied()); assertEquals(1,f.store.preparedParticipantCount()); assertArrayEquals(record.encode(),f.store.preparedParticipant(one.transactionId()).encode());
            byte[] malformed=record.encode(); java.nio.ByteBuffer.wrap(malformed).putInt(72,-1); assertThrows(IllegalArgumentException.class,() -> PreparedParticipant.decode(malformed));
        }
    }
    @Test void corruptPreparedMetadataCannotBeExportedOrReactivated() throws Exception {
        try (var f=outer.new Fixture(root.resolve("corrupt"),true)) {
            enable(f); var preparation=preparation(f,put(f,0,1,1),point(outer.fixed,1),1); var record=f.store.applyPrepared(new ApplyPosition(1,1),preparation);
            var dbField=ReplicatedRangeStore.class.getDeclaredField("db"); var stateField=ReplicatedRangeStore.class.getDeclaredField("state"); dbField.setAccessible(true); stateField.setAccessible(true);
            var db=(org.rocksdb.RocksDB)dbField.get(f.store); var state=(org.rocksdb.ColumnFamilyHandle)stateField.get(f.store);
            byte[] key=java.nio.ByteBuffer.allocate(33).putLong(outer.group.getMostSignificantBits()).putLong(outer.group.getLeastSignificantBits()).put((byte)0x20).putLong(0).putLong(1).array();
            byte[] corrupt=record.encode(); java.nio.ByteBuffer.wrap(corrupt).putInt(72,-1);
            try (var options=new org.rocksdb.WriteOptions().setSync(true)) { db.put(state,options,key,corrupt); }
            try (var pin=f.store.capture()) {
                assertThrows(java.io.IOException.class,() -> NativeRangeImage.write(root.resolve("corrupt-image"),pin,f.store.ownership().interval(),new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4)));
            }
            f.store.fence(); assertThrows(IllegalArgumentException.class,f.store::activate); assertFalse(f.store.admissionOpen());
            assertThrows(IllegalStateException.class,() -> f.store.captureReadable(point(outer.fixed,1)));
        }
    }
    @Test void durablePreparedCountAdmissionCannotBeDonatedByOrdinaryReceiptGc() throws Exception {
        try (var f=outer.new Fixture(root.resolve("capacity"),true)) {
            enable(f);
            for (int n=0;n<ReplicatedRangeStore.MAX_PREPARED_PARTICIPANTS;n++) {
                var preparation=preparation(f,put(f,n,n,n),point(outer.fixed,n),n); f.store.applyPrepared(new ApplyPosition(1,n+1),preparation);
            }
            assertEquals(64,f.store.preparedParticipantCount()); assertEquals(0,f.store.receiptUsage().retainedCount()); assertTrue(f.store.preparedParticipantBytes()<=ReplicatedRangeStore.MAX_PREPARED_BYTES);
            var overflow=preparation(f,put(f,64,64,64),point(outer.fixed,64),64);
            assertThrows(IllegalStateException.class,() -> f.store.applyPrepared(new ApplyPosition(1,65),overflow)); assertEquals(new ApplyPosition(1,64),f.store.applied());
            assertEquals(64,f.store.preparedParticipantCount()); assertEquals(0,f.store.activeSnapshotPins());
        }
    }
}
