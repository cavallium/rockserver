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
        var participant=new TransactionStatus.Participant(outer.domain, outer.group, outer.group, 1, f.schema.digest(), PreparedParticipant.Preparation.planDigest(accepted), null, false);
        var registration=TransactionStatus.begin(outer.cluster, outer.database, new UUID(0,n), base.operationId(), List.of(new DomainBinding(outer.domain, 0, new byte[32], 1, f.schema.digest())), new byte[32], List.of(participant));
        return new PreparedParticipant.Preparation(new UUID(0,100),1,new OperationId(new UUID(0,101),1,n),registration,accepted);
    }
    @Test void completeScopeLocksAbsenceReadsAndRangesWithoutInstallingBusinessEffects() throws Exception {
        try (var f=outer.new Fixture(root.resolve("locks"),true)) {
            enable(f); f.apply(1,put(f,0,1,1));
            var scope=new TransactionScope(List.of(TransactionScope.Span.range(outer.fixed,key(1),key(4)),TransactionScope.Span.point(outer.bucket,outer.physical(7))));
            var base=put(f,10,2,2); var preparation=preparation(f,base,scope,1);
            try (var old=f.store.captureTracked(scope)) {
                assertTrue(f.store.applyPrepared(new ApplyPosition(1,2),preparation).prepared()); var record=f.store.preparedParticipant(preparation.transactionId()); assertEquals(new ApplyPosition(1,2),record.position());
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
                assertEquals(ParticipantVote.prepared(record.position()),f.store.applyPrepared(new ApplyPosition(1,4),preparation)); assertEquals(1,f.store.preparedParticipantCount());
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
            var preparation=preparation(f,base,scope,1); assertTrue(f.store.applyPrepared(new ApplyPosition(1,1),preparation).prepared()); record=f.store.preparedParticipant(preparation.transactionId());
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
            assertTrue(f.store.applyPrepared(new ApplyPosition(1,1),one).prepared()); var record=f.store.preparedParticipant(one.transactionId());
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
            enable(f); var preparation=preparation(f,put(f,0,1,1),point(outer.fixed,1),1); assertTrue(f.store.applyPrepared(new ApplyPosition(1,1),preparation).prepared()); var record=f.store.preparedParticipant(preparation.transactionId());
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
    ParticipantFinalization abort(PreparedParticipant.Preparation preparation) {
        return new ParticipantFinalization(preparation.homeGroup(),preparation.homeEpoch(),outer.group,new OperationId(new UUID(0,110),3,preparation.operation().sequence()),preparation.registration().decide(TransactionStatus.Phase.ABORT));
    }
    @Test void abortBeforePrepareRetainsNegativeReceiptAcrossReopenAndImageInstall() throws Exception {
        Path source=root.resolve("abort-source"),image=root.resolve("abort-image"); var limits=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4);
        PreparedParticipant.Preparation preparation; AbortedParticipant fence;
        try (var f=outer.new Fixture(source,true)) {
            enable(f); preparation=preparation(f,put(f,0,1,9),point(outer.fixed,1),1);
            fence=NativeParticipantRetirementTest.appliedAbort(f.store,new ApplyPosition(1,1),abort(preparation)); assertNull(f.store.rejectedParticipant(fence.finalization().transactionId()));
            assertEquals(ParticipantVote.negative(),f.store.applyPrepared(new ApplyPosition(1,2),preparation));
            assertNull(f.store.preparedParticipant(preparation.transactionId())); assertNull(f.raw(outer.fixed,key(1))); assertEquals(0,f.store.receiptUsage().retainedCount());
            assertEquals(ParticipantVote.negative(),f.store.applyPrepared(new ApplyPosition(2,3),preparation));
            assertEquals(new ApplyPosition(1,2),f.store.rejectedParticipant(preparation.transactionId()).position());
            try (var pin=f.store.capture()) { assertEquals(ParticipantVote.negative(),pin.preparationVote(preparation)); NativeRangeImage.write(image,pin,f.store.ownership().interval(),limits); }
            NativeRangeImage.inspect(image,limits);
            try (var read=f.store.captureReadable(point(outer.fixed,1))) { assertNull(read.get(outer.fixed,key(1))); }
            var changed=new PreparedParticipant.Preparation(preparation.homeGroup(),preparation.homeEpoch(),new OperationId(new UUID(0,120),1,0),preparation.registration(),preparation.business());
            assertThrows(IllegalArgumentException.class,() -> f.store.applyPrepared(new ApplyPosition(2,4),changed)); assertEquals(new ApplyPosition(2,3),f.store.applied());
        }
        try (var f=outer.new Fixture(source,true)) {
            assertEquals(ParticipantVote.negative(),f.store.recoveredPreparationVote(preparation));
            assertEquals(fence.finalization(),f.store.abortedParticipant(preparation.transactionId()).finalization());
            assertEquals(ParticipantVote.negative(),f.store.applyPrepared(new ApplyPosition(2,4),preparation));
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.store.apply(new ApplyPosition(2,5),put(f,1,1,7))).status()); assertArrayEquals(key(7),f.raw(outer.fixed,key(1)));
        }
        try (var f=outer.new Fixture(root.resolve("abort-target"),true)) {
            f.store.fence(); f.store.installSnapshot(image,limits);
            assertEquals(ParticipantVote.negative(),f.store.recoveredPreparationVote(preparation)); f.store.activate();
            assertEquals(ParticipantVote.negative(),f.store.applyPrepared(new ApplyPosition(2,4),preparation));
            assertEquals(0,f.store.preparedParticipantCount()); assertNull(f.raw(outer.fixed,key(1)));
        }
    }
    @Test void rejectionBeforeHomeAbortIsPermanentWithoutEffectsOrPreparedLocksAcrossReopenAndInstall() throws Exception {
        Path source=root.resolve("rejection-source"),image=root.resolve("rejection-image");
        var limits=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4);
        PreparedParticipant.Preparation preparation;
        try (var f=outer.new Fixture(source,true)) {
            enable(f); preparation=preparation(f,put(f,0,1,9),point(outer.fixed,1),1);
            assertEquals(TransactionStatus.Phase.PREPARING,preparation.registration().phase());
            assertEquals(ParticipantVote.negative(),f.store.applyRejected(new ApplyPosition(1,1),preparation));
            assertNull(f.store.abortedParticipant(preparation.transactionId())); assertNull(f.store.finalizedParticipant(preparation.transactionId()));
            assertNull(f.store.preparedParticipant(preparation.transactionId())); assertNull(f.raw(outer.fixed,key(1)));
            assertEquals(1,f.store.preparedCapacity().count()); assertEquals(0,f.store.receiptUsage().retainedCount());
            assertEquals(ParticipantVote.negative(),f.store.estimatePreparation(preparation).existing());
            try (var read=f.store.captureReadable(point(outer.fixed,1))) { assertNull(read.get(outer.fixed,key(1))); }
            f.apply(2,put(f,1,1,7)); // A negative vote owns identities, not a read/write scope.
            assertEquals(ParticipantVote.negative(),f.store.applyPrepared(new ApplyPosition(2,3),preparation));
            assertArrayEquals(key(7),f.raw(outer.fixed,key(1))); assertEquals(new ApplyPosition(1,1),f.store.rejectedParticipant(preparation.transactionId()).position());
            var changed=new PreparedParticipant.Preparation(preparation.homeGroup(),preparation.homeEpoch(),new OperationId(new UUID(0,120),1,0),preparation.registration(),preparation.business());
            assertThrows(IllegalArgumentException.class,() -> f.store.applyRejected(new ApplyPosition(2,4),changed));
            assertThrows(IllegalArgumentException.class,() -> f.store.estimateReceiptRetention(put(f,0,2,8)));
            var controlCollision=new DistributedCommand(outer.group,1,f.schema.digest(),preparation.operation(),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,key(2),key(8))));
            assertThrows(IllegalArgumentException.class,() -> f.store.estimateReceiptRetention(controlCollision));
            try (var pin=f.store.capture()) { assertEquals(ParticipantVote.negative(),pin.preparationVote(preparation)); NativeRangeImage.write(image,pin,f.store.ownership().interval(),limits); }
            NativeRangeImage.inspect(image,limits);
        }
        try (var f=outer.new Fixture(source,true)) {
            assertEquals(ParticipantVote.negative(),f.store.recoveredPreparationVote(preparation));
            assertNull(f.store.abortedParticipant(preparation.transactionId()));
            assertEquals(ParticipantVote.negative(),f.store.applyPrepared(new ApplyPosition(2,4),preparation));
            var finalization=abort(preparation); assertFalse(f.store.estimateFinalization(finalization).newFence());
            NativeParticipantRetirementTest.appliedAbort(f.store,new ApplyPosition(2,5),finalization);
            assertEquals(1,f.store.preparedCapacity().count()); assertEquals(ParticipantVote.negative(),f.store.finalizedParticipant(preparation.transactionId()).vote());
            assertArrayEquals(key(7),f.raw(outer.fixed,key(1)));
        }
        try (var f=outer.new Fixture(root.resolve("rejection-target"),true)) {
            f.store.fence(); f.store.installSnapshot(image,limits);
            assertEquals(ParticipantVote.negative(),f.store.recoveredPreparationVote(preparation));
            f.store.activate(); assertEquals(ParticipantVote.negative(),f.store.applyPrepared(new ApplyPosition(2,4),preparation));
            assertNull(f.store.abortedParticipant(preparation.transactionId())); assertArrayEquals(key(7),f.raw(outer.fixed,key(1)));
        }
    }
    @Test void rejectionCapacityReservesAbortAndNegativeCannotReplaceAPositiveVote() throws Exception {
        try (var f=outer.new Fixture(root.resolve("rejection-capacity"),true)) {
            enable(f); var plans=new java.util.ArrayList<PreparedParticipant.Preparation>();
            for (int n=0;n<ReplicatedRangeStore.MAX_PREPARED_PARTICIPANTS;n++) {
                var p=preparation(f,put(f,n,n,n),point(outer.fixed,n),n); plans.add(p);
                assertEquals(ParticipantVote.negative(),f.store.applyRejected(new ApplyPosition(1,n+1),p));
            }
            assertEquals(64,f.store.preparedCapacity().count());
            var overflow=preparation(f,put(f,64,64,64),point(outer.fixed,64),64);
            assertThrows(IllegalStateException.class,() -> f.store.applyRejected(new ApplyPosition(1,65),overflow));
            assertThrows(IllegalStateException.class,() -> f.store.estimatePreparation(overflow));
            assertEquals(new ApplyPosition(1,64),f.store.applied());
            for (int n=0;n<plans.size();n++) {
                var finalization=abort(plans.get(n)); assertFalse(f.store.estimateFinalization(finalization).newFence());
                NativeParticipantRetirementTest.appliedAbort(f.store,new ApplyPosition(1,65+n),finalization);
            }
            assertEquals(64,f.store.preparedCapacity().count()); f.store.fence(); f.store.activate();
            assertTrue(f.store.preparedCapacity().bytes()<=ReplicatedRangeStore.MAX_PREPARED_BYTES);
        }
        try (var f=outer.new Fixture(root.resolve("positive-reject"),true)) {
            enable(f); var p=preparation(f,put(f,0,1,9),point(outer.fixed,1),1);
            var vote=f.store.applyPrepared(new ApplyPosition(1,1),p);
            assertThrows(IllegalArgumentException.class,() -> f.store.applyRejected(new ApplyPosition(1,2),p));
            assertEquals(new ApplyPosition(1,1),f.store.applied()); assertEquals(vote,f.store.recoveredPreparationVote(p));
        }
    }
    @Test void preparedAbortReleasesScopeButRetainsPositiveVoteAndBusinessIdentity() throws Exception {
        try (var f=outer.new Fixture(root.resolve("positive-abort"),true)) {
            enable(f); var preparation=preparation(f,put(f,0,1,9),point(outer.fixed,1),9);
            var vote=f.store.applyPrepared(new ApplyPosition(1,1),preparation); var fence=NativeParticipantRetirementTest.appliedAbort(f.store,new ApplyPosition(2,2),abort(preparation));
            assertEquals(vote,f.store.applyPrepared(new ApplyPosition(2,3),preparation)); assertNull(f.raw(outer.fixed,key(1)));
            assertNull(f.store.rejectedParticipant(fence.finalization().transactionId())); assertThrows(IllegalArgumentException.class,() -> f.store.estimateReceiptRetention(put(f,0,1,9)));
            try (var read=f.store.captureReadable(point(outer.fixed,1))) { assertNull(read.get(outer.fixed,key(1))); }
            var next=preparation(f,put(f,1,1,7),point(outer.fixed,1),2); assertTrue(f.store.applyPrepared(new ApplyPosition(2,4),next).prepared());
            try (var pin=f.store.capture()) { var image=root.resolve("overlap-image"); var limits=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4); NativeRangeImage.write(image,pin,f.store.ownership().interval(),limits); NativeRangeImage.inspect(image,limits); }
            f.store.fence(); f.store.activate(); assertThrows(IllegalStateException.class,() -> f.store.captureReadable(point(outer.fixed,1)));
        }
        try (var reopened=outer.new Fixture(root.resolve("positive-abort"),true)) { assertThrows(IllegalStateException.class,() -> reopened.store.captureReadable(point(outer.fixed,1))); }
        try (var installed=outer.new Fixture(root.resolve("overlap-target"),true)) {
            installed.store.fence(); installed.store.installSnapshot(root.resolve("overlap-image"),new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4)); installed.store.activate();
            assertThrows(IllegalStateException.class,() -> installed.store.captureReadable(point(outer.fixed,1)));
        }
    }
    @Test void lateNegativePrepareCannotReuseAnotherOwnerOrAnOrdinaryReceipt() throws Exception {
        try (var f=outer.new Fixture(root.resolve("negative-identities"),true)) {
            enable(f); var late=preparation(f,put(f,0,1,9),point(outer.fixed,1),1);
            var active=preparation(f,put(f,1,2,7),point(outer.fixed,2),2); f.store.applyPrepared(new ApplyPosition(1,1),active);
            NativeParticipantRetirementTest.appliedAbort(f.store,new ApplyPosition(1,2),abort(late));
            var colliding=new PreparedParticipant.Preparation(late.homeGroup(),late.homeEpoch(),active.operation(),late.registration(),late.business());
            assertThrows(IllegalArgumentException.class,() -> f.store.applyPrepared(new ApplyPosition(1,3),colliding)); assertEquals(new ApplyPosition(1,2),f.store.applied());
            var receiptId=new OperationId(new UUID(0,140),1,0);
            var ordinary=new DistributedCommand(outer.group,1,f.schema.digest(),receiptId,List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,key(3),key(7)))); f.apply(3,ordinary);
            var receiptCollision=new PreparedParticipant.Preparation(late.homeGroup(),late.homeEpoch(),receiptId,late.registration(),late.business());
            assertThrows(IllegalArgumentException.class,() -> f.store.applyPrepared(new ApplyPosition(1,4),receiptCollision)); assertEquals(new ApplyPosition(1,3),f.store.applied());
            var otherAbort=NativeParticipantRetirementTest.appliedAbort(f.store,new ApplyPosition(1,4),abort(active));
            var fenceCollision=new PreparedParticipant.Preparation(late.homeGroup(),late.homeEpoch(),otherAbort.finalization().operation(),late.registration(),late.business());
            assertThrows(IllegalArgumentException.class,() -> f.store.applyPrepared(new ApplyPosition(1,5),fenceCollision));
            assertEquals(ParticipantVote.negative(),f.store.applyPrepared(new ApplyPosition(1,5),late)); f.store.fence(); f.store.activate();
        }
    }
    @Test void corruptAbortOwnershipIsRejectedByImageExportBeforeInstallation() throws Exception {
        try (var f=outer.new Fixture(root.resolve("corrupt-abort"),true)) {
            enable(f); var late=preparation(f,put(f,0,1,9),point(outer.fixed,1),1); var active=preparation(f,put(f,1,2,7),point(outer.fixed,2),2);
            f.store.applyPrepared(new ApplyPosition(1,1),active); var abort=NativeParticipantRetirementTest.appliedAbort(f.store,new ApplyPosition(1,2),abort(late)); f.store.applyPrepared(new ApplyPosition(1,3),late);
            var corrupt=new RejectedParticipant(outer.group,late.homeGroup(),late.homeEpoch(),active.operation(),late.registration(),late.digest(),new ApplyPosition(1,3));
            var dbField=ReplicatedRangeStore.class.getDeclaredField("db"); var stateField=ReplicatedRangeStore.class.getDeclaredField("state"); dbField.setAccessible(true); stateField.setAccessible(true);
            byte[] key=java.nio.ByteBuffer.allocate(33).putLong(outer.group.getMostSignificantBits()).putLong(outer.group.getLeastSignificantBits()).put((byte)0x23).putLong(0).putLong(1).array();
            ((org.rocksdb.RocksDB)dbField.get(f.store)).put((org.rocksdb.ColumnFamilyHandle)stateField.get(f.store),key,corrupt.encode());
            try (var pin=f.store.capture()) { assertThrows(java.io.IOException.class,() -> NativeRangeImage.write(root.resolve("bad-abort-image"),pin,f.store.ownership().interval(),new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4))); }
            f.store.fence(); assertThrows(IllegalArgumentException.class,f.store::activate); assertFalse(f.store.admissionOpen());
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
            var first=f.store.preparedParticipant(new UUID(0,0)); var fence=NativeParticipantRetirementTest.appliedAbort(f.store,new ApplyPosition(1,65),abort(first.preparation())); assertNull(f.store.rejectedParticipant(fence.finalization().transactionId()));
            assertEquals(64,f.store.preparedCapacity().count()); assertTrue(f.store.preparedCapacity().bytes()<=ReplicatedRangeStore.MAX_PREPARED_BYTES);
            try (var read=f.store.captureReadable(point(outer.fixed,0))) { assertNull(read.get(outer.fixed,key(0))); }

        }
    }
}
