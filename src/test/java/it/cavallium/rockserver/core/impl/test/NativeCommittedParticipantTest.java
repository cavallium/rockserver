package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Trusted native finalization fixtures; no Raft FINALIZE admission or quorum claim. */
class NativeCommittedParticipantTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativePreparedParticipantTest helper=new NativePreparedParticipantTest();
    final NativeBucketApplyTest outer=helper.outer;
    byte[] key(int n) { return helper.key(n); }
    ParticipantFinalization commit(PreparedParticipant prepared) {
        var p=prepared.preparation(); var decision=p.registration().prepared(outer.group,PreparedParticipant.Preparation.planDigest(p.business()),prepared.position()).decide(TransactionStatus.Phase.COMMIT);
        return new ParticipantFinalization(p.homeGroup(),p.homeEpoch(),outer.group,new OperationId(new UUID(0,150),3,p.operation().sequence()),decision);
    }
    @Test void orderedEffectsResultAndLockReleaseCommitTogetherWhileOldCutsRemainHistorical() throws Exception {
        try (var f=outer.new Fixture(root.resolve("ordered"),true)) {
            helper.enable(f);
            f.apply(1,outer.command(f.schema,0,List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,key(1),key(1)),new Mutation(Mutation.Kind.PUT,outer.fixed,key(3),key(3)),outer.put(outer.bucket,7,1,key(1),Mutation.ReturnMode.NONE)),List.of(),-1));
            var scope=new TransactionScope(List.of(TransactionScope.Span.range(outer.fixed,key(1),key(4)),TransactionScope.Span.point(outer.bucket,outer.physical(7))));
            var base=outer.command(f.schema,1,List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,key(1),key(5),Mutation.ReturnMode.PREVIOUS),new Mutation(Mutation.Kind.DELETE_RANGE,outer.fixed,key(1),key(3)),new Mutation(Mutation.Kind.PUT,outer.fixed,key(2),key(9),Mutation.ReturnMode.CURRENT),outer.put(outer.bucket,7,1,key(5),Mutation.ReturnMode.PREVIOUS),outer.put(outer.bucket,7,2,key(6),Mutation.ReturnMode.NONE)),List.of(),-1);
            var preparation=helper.preparation(f,base,scope,1);
            try (var old=f.store.captureReadable(scope); var tracked=f.store.captureTracked(scope)) {
                var vote=f.store.applyPrepared(new ApplyPosition(1,2),preparation); assertThrows(IllegalStateException.class,() -> f.store.captureReadable(scope));
                var prepared=f.store.preparedParticipant(preparation.transactionId()); var intent=commit(prepared);
                var receipt=NativeParticipantRetirementTest.appliedCommit(f.store,new ApplyPosition(1,3),intent); assertEquals(new ApplyPosition(1,3),receipt.position());
                assertNull(f.raw(outer.fixed,key(1))); assertArrayEquals(key(9),f.raw(outer.fixed,key(2))); assertArrayEquals(key(3),f.raw(outer.fixed,key(3)));
                assertArrayEquals(key(5),f.row(7).get(outer.member(1))); assertArrayEquals(key(6),f.row(7).get(outer.member(2)));
                assertArrayEquals(key(1),old.get(outer.fixed,key(1))); assertNull(old.get(outer.fixed,key(2)));
                try (var current=f.store.captureReadable(scope)) { assertArrayEquals(key(9),current.get(outer.fixed,key(2))); }
                byte[] result=f.store.committedResult(preparation.transactionId(),preparation.business().payloadDigest()); assertArrayEquals(preparation.business().acceptance().result(),result);
                assertEquals(3,CommandResult.decode(result).valueCount());
                assertThrows(IllegalStateException.class,() -> f.store.planScopedCommit(tracked,helper.put(f,100,2,7),0,new UUID(0,99))); assertEquals(1,f.store.receiptUsage().retainedCount(),"Finalization result uses the preparation reservation");
                f.store.apply(new ApplyPosition(1,4),helper.put(f,99,2,7));
                assertEquals(receipt,NativeParticipantRetirementTest.appliedCommit(f.store,new ApplyPosition(1,5),intent)); assertArrayEquals(key(7),f.raw(outer.fixed,key(2)));
                assertEquals(vote,f.store.applyPrepared(new ApplyPosition(1,6),preparation)); assertArrayEquals(result,f.store.committedResult(preparation.transactionId(),preparation.business().payloadDigest()));
                byte[] wrong=new byte[32]; assertThrows(IllegalArgumentException.class,() -> f.store.committedResult(preparation.transactionId(),wrong));
                assertThrows(IllegalArgumentException.class,() -> NativeParticipantRetirementTest.appliedAbort(f.store,new ApplyPosition(1,7),helper.abort(preparation))); assertEquals(new ApplyPosition(1,6),f.store.applied());
            }
        }
    }
    @Test void originalGrantExpiryDoesNotPreventReservedCommitOrEraseItsResult() throws Exception {
        try (var f=outer.new Fixture(root.resolve("expired"),true)) {
            helper.enable(f); var preparation=helper.preparation(f,helper.put(f,0,1,9),helper.point(outer.fixed,1),1); f.store.applyPrepared(new ApplyPosition(1,1),preparation);
            var intent=commit(f.store.preparedParticipant(preparation.transactionId()));
            f.apply(2,DistributedCommand.advanceSessionEpochFloor(outer.group,1,f.schema.digest(),new OperationId(new UUID(0,155),2,0),2));
            var receipt=NativeParticipantRetirementTest.appliedCommit(f.store,new ApplyPosition(2,3),intent); assertArrayEquals(key(9),f.raw(outer.fixed,key(1)));
            assertEquals(receipt,NativeParticipantRetirementTest.appliedCommit(f.store,new ApplyPosition(2,4),intent)); assertEquals(ParticipantVote.prepared(new ApplyPosition(1,1)),f.store.applyPrepared(new ApplyPosition(2,5),preparation));
            assertArrayEquals(preparation.business().acceptance().result(),f.store.committedResult(preparation.transactionId(),preparation.business().payloadDigest()));
            assertThrows(IllegalArgumentException.class,() -> f.store.estimateReceiptRetention(new DistributedCommand(outer.group,1,f.schema.digest(),intent.operation(),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,key(2),key(2))))));
        }
    }
    @Test void reopenAndPortableInstallRetainResultAndAllowOverlappingActivePlanInEitherUuidOrder() throws Exception {
        Path source=root.resolve("source"),image=root.resolve("commit-image"); var limits=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4);
        PreparedParticipant.Preparation preparation; CommittedParticipant receipt;
        try (var f=outer.new Fixture(source,true)) {
            helper.enable(f); preparation=helper.preparation(f,helper.put(f,0,1,9),helper.point(outer.fixed,1),9); f.store.applyPrepared(new ApplyPosition(1,1),preparation);
            receipt=NativeParticipantRetirementTest.appliedCommit(f.store,new ApplyPosition(2,2),commit(f.store.preparedParticipant(preparation.transactionId())));
            var next=helper.preparation(f,helper.put(f,1,1,7),helper.point(outer.fixed,1),2); f.store.applyPrepared(new ApplyPosition(2,3),next);
            try (var pin=f.store.capture()) { NativeRangeImage.write(image,pin,f.store.ownership().interval(),limits); } NativeRangeImage.inspect(image,limits);
        }
        for (boolean install:new boolean[]{false,true}) try (var f=outer.new Fixture(install ? root.resolve("target") : source,true)) {
            if (install) { f.store.fence(); f.store.installSnapshot(image,limits); f.store.activate(); }
            assertEquals(receipt,f.store.committedParticipant(preparation.transactionId())); assertArrayEquals(key(9),f.raw(outer.fixed,key(1)));
            assertArrayEquals(preparation.business().acceptance().result(),f.store.committedResult(preparation.transactionId(),preparation.business().payloadDigest()));
            assertThrows(IllegalStateException.class,() -> f.store.captureReadable(helper.point(outer.fixed,1)));
            assertEquals(receipt,NativeParticipantRetirementTest.appliedCommit(f.store,new ApplyPosition(2,4),receipt.finalization())); assertArrayEquals(key(9),f.raw(outer.fixed,key(1)));
        }
    }
    @Test void finalizationAtPreparationCapUsesReservedSpace() throws Exception {
        try (var f=outer.new Fixture(root.resolve("capacity"),true)) {
            helper.enable(f);
            for (int n=0;n<64;n++) { var p=helper.preparation(f,helper.put(f,n,n,n),helper.point(outer.fixed,n),n); f.store.applyPrepared(new ApplyPosition(1,n+1),p); }
            var first=f.store.preparedParticipant(new UUID(0,0)); NativeParticipantRetirementTest.appliedCommit(f.store,new ApplyPosition(2,65),commit(first));
            assertEquals(64,f.store.preparedCapacity().count()); assertTrue(f.store.preparedCapacity().bytes()<=ReplicatedRangeStore.MAX_PREPARED_BYTES);
            assertEquals(0,f.store.receiptUsage().retainedCount()); try (var read=f.store.captureReadable(helper.point(outer.fixed,0))) { assertArrayEquals(key(0),read.get(outer.fixed,key(0))); }
        }
    }
    @Test void absentPreparationChangedVoteAndRecoveryIdentityCollisionFailBeforeEffects() throws Exception {
        try (var f=outer.new Fixture(root.resolve("reject"),true)) {
            helper.enable(f); var p=helper.preparation(f,helper.put(f,0,1,9),helper.point(outer.fixed,1),1);
            var intent=commit(new PreparedParticipant(p,new ApplyPosition(1,1)));
            assertThrows(IllegalStateException.class,() -> NativeParticipantRetirementTest.appliedCommit(f.store,new ApplyPosition(1,2),intent)); assertNull(f.raw(outer.fixed,key(1)));
            f.store.applyPrepared(new ApplyPosition(1,1),p);
            var changed=commit(new PreparedParticipant(p,new ApplyPosition(1,2))); assertThrows(IllegalArgumentException.class,() -> NativeParticipantRetirementTest.appliedCommit(f.store,new ApplyPosition(1,3),changed));
            var ordinary=new DistributedCommand(outer.group,1,f.schema.digest(),intent.operation(),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,key(3),key(3)))); f.apply(2,ordinary);
            assertThrows(IllegalArgumentException.class,() -> NativeParticipantRetirementTest.appliedCommit(f.store,new ApplyPosition(1,3),intent)); assertNull(f.raw(outer.fixed,key(1))); assertEquals(new ApplyPosition(1,2),f.store.applied());
        }
    }
    @Test void malformedCommitBindingOrMissingRetainedPlanBlocksExportAndActivation() throws Exception {
        for (boolean remove:new boolean[]{false,true}) try (var f=outer.new Fixture(root.resolve("corrupt-"+remove),true)) {
            helper.enable(f); var p=helper.preparation(f,helper.put(f,0,1,9),helper.point(outer.fixed,1),1); f.store.applyPrepared(new ApplyPosition(1,1),p);
            var receipt=NativeParticipantRetirementTest.appliedCommit(f.store,new ApplyPosition(2,2),commit(f.store.preparedParticipant(p.transactionId())));
            var dbField=ReplicatedRangeStore.class.getDeclaredField("db"); var stateField=ReplicatedRangeStore.class.getDeclaredField("state"); dbField.setAccessible(true); stateField.setAccessible(true);
            var db=(org.rocksdb.RocksDB)dbField.get(f.store); var state=(org.rocksdb.ColumnFamilyHandle)stateField.get(f.store);
            byte[] key=java.nio.ByteBuffer.allocate(33).putLong(outer.group.getMostSignificantBits()).putLong(outer.group.getLeastSignificantBits()).put((byte)(remove ? 0x20 : 0x22)).putLong(0).putLong(1).array();
            if (remove) db.delete(state,key);
            else {
                var wrongDecision=p.registration().prepared(outer.group,p.business().payloadDigest(),new ApplyPosition(2,1)).decide(TransactionStatus.Phase.COMMIT);
                var wrong=new ParticipantFinalization(p.homeGroup(),p.homeEpoch(),outer.group,receipt.finalization().operation(),wrongDecision);
                db.put(state,key,new CommittedParticipant(wrong,receipt.position()).encode());
            }
            assertThrows(IllegalArgumentException.class,() -> f.store.recoveredFinalization(receipt.finalization()));
            try (var pin=f.store.capture()) { assertThrows(java.io.IOException.class,() -> NativeRangeImage.write(root.resolve("bad-image-"+remove),pin,f.store.ownership().interval(),new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4))); }
            f.store.fence(); assertThrows(IllegalArgumentException.class,f.store::activate); assertFalse(f.store.admissionOpen());
        }
    }

}
