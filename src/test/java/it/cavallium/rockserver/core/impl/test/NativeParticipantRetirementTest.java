package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native family retirement and aggregate replay fencing; no RPC/server authority claim. */
class NativeParticipantRetirementTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativePreparedParticipantTest helper=new NativePreparedParticipantTest();
    final NativeBucketApplyTest outer=helper.outer;
    final NativeRangeImage.Limits limits=new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,4);
    static CommittedParticipant appliedCommit(ReplicatedRangeStore store,ApplyPosition cut,ParticipantFinalization intent) throws Exception {
        var result=store.applyFinalization(cut,intent); assertFalse(result.retired()); var record=store.committedParticipant(intent.transactionId()); assertNotNull(record); assertEquals(record.position(),result.position()); return record;
    }
    static AbortedParticipant appliedAbort(ReplicatedRangeStore store,ApplyPosition cut,ParticipantFinalization intent) throws Exception {
        var result=store.applyFinalization(cut,intent); assertFalse(result.retired()); var record=store.abortedParticipant(intent.transactionId()); assertNotNull(record); assertEquals(record.position(),result.position()); return record;
    }
    ParticipantFinalization commit(PreparedParticipant prepared) {
        var p=prepared.preparation(); var ready=p.registration().prepared(outer.group,p.business().payloadDigest(),prepared.position());
        return new ParticipantFinalization(p.homeGroup(),p.homeEpoch(),outer.group,new OperationId(new UUID(0,180),0,p.operation().sequence()),ready.decide(TransactionStatus.Phase.COMMIT));
    }
    ParticipantRetirement retirement(ParticipantFinalization terminal,ApplyPosition original) { return new ParticipantRetirement(terminal,original,terminal.decision().finalized(outer.group).consume()); }
    ParticipantIdentity identity(PreparedParticipant.Preparation p) { return new ParticipantIdentity(p.homeGroup(),p.homeEpoch(),outer.group,p.registration()); }
    void floor(NativeBucketApplyTest.Fixture f,long index) throws Exception { f.apply(index,DistributedCommand.advanceSessionEpochFloor(outer.group,1,f.schema.digest(),new OperationId(new UUID(0,181),2,index),2)); }
    @Test void commitRetiresItsWholeFamilyWithoutChangingUserRowsAndOldPinsKeepTheirOwnProof() throws Exception {
        Path source=root.resolve("commit"),image=root.resolve("retired-image"); PreparedParticipant.Preparation preparation; ParticipantFinalization terminal; ParticipantRetirement retirement;
        try (var f=outer.new Fixture(source,true)) {
            helper.enable(f); preparation=helper.preparation(f,helper.put(f,0,1,9),helper.point(outer.fixed,1),1); f.store.applyPrepared(new ApplyPosition(1,1),preparation);
            terminal=commit(f.store.preparedParticipant(preparation.transactionId())); var original=f.store.applyFinalization(new ApplyPosition(1,2),terminal); retirement=retirement(terminal,original.position());
            assertThrows(IllegalStateException.class,() -> f.store.applyParticipantRetirement(new ApplyPosition(1,3),retirement)); assertEquals(1,f.store.preparedCapacity().count());
            floor(f,3); assertTrue(f.store.estimateParticipantRetirement(retirement));
            try (var old=f.store.capture()) {
                f.store.applyParticipantRetirement(new ApplyPosition(1,4),retirement); assertEquals(0,f.store.preparedCapacity().count()); assertEquals(0,f.store.preparedCapacity().bytes()); assertEquals(0,f.store.preparedCapacity().scopeBytes()); assertFalse(f.store.estimateParticipantRetirement(retirement));
                assertNull(f.store.preparedParticipant(preparation.transactionId())); assertNull(f.store.finalizedParticipant(preparation.transactionId())); assertArrayEquals(helper.key(9),f.raw(outer.fixed,helper.key(1)));
                assertTrue(old.preparationVote(preparation).prepared()); assertEquals(original,old.finalizationResult(terminal)); assertNull(old.participantFence(identity(preparation)));
                assertTrue(f.store.recoveredPreparationVote(preparation).retired()); assertTrue(f.store.recoveredFinalization(terminal).retired());
                var witness=f.store.participantFence(identity(preparation)); assertEquals(2,witness.minimumEpoch()); assertEquals(new ApplyPosition(1,4),witness.position());
                var vote=f.store.applyPrepared(new ApplyPosition(1,5),preparation); assertTrue(vote.retired()); assertNull(vote.position()); assertEquals(new ApplyPosition(1,5),vote.retirementProof().position());
                var finalized=f.store.applyFinalization(new ApplyPosition(1,6),terminal); assertTrue(finalized.retired()); assertNull(finalized.position()); assertEquals(new ApplyPosition(1,6),finalized.retirement().position());
                assertTrue(f.store.applyRejected(new ApplyPosition(1,7),preparation).retired()); f.store.applyParticipantRetirement(new ApplyPosition(1,8),retirement);
                assertEquals(0,f.store.preparedCapacity().count()); assertArrayEquals(helper.key(9),f.raw(outer.fixed,helper.key(1)));
            }
            try (var pin=f.store.capture()) { NativeRangeImage.write(image,pin,f.store.ownership().interval(),limits); assertTrue(pin.preparationVote(preparation).retired()); assertEquals(pin.position(),pin.participantFence(identity(preparation)).position()); }
            NativeRangeImage.inspect(image,limits);
        }
        for (boolean install:new boolean[]{false,true}) try (var f=outer.new Fixture(install ? root.resolve("target") : source,true)) {
            if (install) { f.store.fence(); f.store.installSnapshot(image,limits); assertTrue(f.store.recoveredFinalization(terminal).retired()); f.store.activate(); }
            assertEquals(0,f.store.preparedCapacity().count()); assertTrue(f.store.participantFence(identity(preparation)).minimumEpoch()>preparation.business().operationId().epoch()); assertArrayEquals(helper.key(9),f.raw(outer.fixed,helper.key(1)));
            assertTrue(f.store.applyPrepared(new ApplyPosition(2,9),preparation).retired()); assertEquals(new ApplyPosition(2,9),f.store.applied());
        }
    }
    @Test void rejectedAndAbortOnlyFamiliesRetireAsOneUnionSlotAndLateNoCannotRecreateThem() throws Exception {
        for (boolean rejected:new boolean[]{false,true}) try (var f=outer.new Fixture(root.resolve("abort-"+rejected),true)) {
            helper.enable(f); var p=helper.preparation(f,helper.put(f,0,1,9),helper.point(outer.fixed,1),1); long index=1;
            if (rejected) { assertEquals(ParticipantVote.negative(),f.store.applyRejected(new ApplyPosition(1,index++),p)); assertEquals(1,f.store.preparedCapacity().count()); }
            var terminal=helper.abort(p); var original=f.store.applyFinalization(new ApplyPosition(1,index++),terminal); var retirement=retirement(terminal,original.position());
            assertEquals(1,f.store.preparedCapacity().count()); floor(f,index++); f.store.applyParticipantRetirement(new ApplyPosition(1,index++),retirement);
            assertNull(f.store.abortedParticipant(p.transactionId())); assertNull(f.store.rejectedParticipant(p.transactionId())); assertEquals(0,f.store.preparedCapacity().count()); assertEquals(0,f.store.preparedCapacity().bytes());
            assertTrue(f.store.applyRejected(new ApplyPosition(1,index++),p).retired()); assertTrue(f.store.applyPrepared(new ApplyPosition(1,index++),p).retired()); assertNull(f.raw(outer.fixed,helper.key(1))); assertEquals(0,f.store.preparedCapacity().count());
        }
    }
    @Test void unresolvedMatchingPlanRemainsFinalizableBelowFloorButCannotBeRetiredWithoutExactTerminalProof() throws Exception {
        try (var f=outer.new Fixture(root.resolve("unresolved"),true)) {
            helper.enable(f); var p=helper.preparation(f,helper.put(f,0,1,9),helper.point(outer.fixed,1),1); f.store.applyPrepared(new ApplyPosition(1,1),p); var terminal=commit(f.store.preparedParticipant(p.transactionId()));
            floor(f,2); assertNull(f.store.participantFence(identity(p))); assertFalse(f.store.estimateFinalization(terminal).retired()); assertTrue(f.store.recoveredPreparationVote(p).prepared());
            assertThrows(IllegalArgumentException.class,() -> f.store.applyParticipantRetirement(new ApplyPosition(1,3),retirement(terminal,new ApplyPosition(1,2))));
            var original=f.store.applyFinalization(new ApplyPosition(1,3),terminal); assertFalse(original.retired()); assertArrayEquals(helper.key(9),f.raw(outer.fixed,helper.key(1)));
            var wrongControl=new ParticipantFinalization(terminal.homeGroup(),terminal.homeEpoch(),outer.group,new OperationId(new UUID(0,182),0,0),terminal.decision());
            assertThrows(IllegalArgumentException.class,() -> f.store.applyParticipantRetirement(new ApplyPosition(1,4),retirement(wrongControl,original.position())));
            assertThrows(IllegalArgumentException.class,() -> f.store.applyParticipantRetirement(new ApplyPosition(1,4),retirement(terminal,new ApplyPosition(1,4))));
            f.store.applyParticipantRetirement(new ApplyPosition(1,4),retirement(terminal,original.position())); assertEquals(0,f.store.preparedCapacity().count());
        }
    }
    @Test void neverPreparedExpiredAbortReturnsOnlyFloorProofAndNoTerminalRow() throws Exception {
        try (var f=outer.new Fixture(root.resolve("absent"),true)) {
            helper.enable(f); var p=helper.preparation(f,helper.put(f,0,1,9),helper.point(outer.fixed,1),1); var terminal=helper.abort(p);
            assertNull(f.store.participantFence(identity(p))); floor(f,1); var result=f.store.applyFinalization(new ApplyPosition(1,2),terminal);
            assertTrue(result.retired()); assertNull(result.position()); assertNull(f.store.finalizedParticipant(p.transactionId())); assertEquals(0,f.store.preparedCapacity().count());
            assertTrue(f.store.estimatePreparation(p).existing().retired()); assertTrue(f.store.estimateRejection(p).existing().retired()); assertTrue(f.store.estimateFinalization(terminal).retired());
            var wrong=new TransactionStatus.Participant(outer.group,outer.group,2,f.schema.digest(),p.business().payloadDigest(),null,false);
            var definition=TransactionStatus.begin(outer.cluster,outer.database,outer.domain,p.transactionId(),p.business().operationId(),1,f.schema.digest(),new byte[32],List.of(wrong));
            assertThrows(IllegalArgumentException.class,() -> f.store.participantFence(new ParticipantIdentity(p.homeGroup(),p.homeEpoch(),outer.group,definition)));
        }
    }
    @Test void equalIndexDifferentTermFamilyCannotAuthorizeRetirementOrExport() throws Exception {
        try (var f=outer.new Fixture(root.resolve("crossed-cut"),true)) {
            helper.enable(f); var p=helper.preparation(f,helper.put(f,0,1,9),helper.point(outer.fixed,1),1); var terminal=helper.abort(p);
            var original=f.store.applyFinalization(new ApplyPosition(1,1),terminal); floor(f,2); f.store.applyNoop(new ApplyPosition(2,3));
            var dbField=ReplicatedRangeStore.class.getDeclaredField("db"); var stateField=ReplicatedRangeStore.class.getDeclaredField("state"); dbField.setAccessible(true); stateField.setAccessible(true);
            var db=(org.rocksdb.RocksDB)dbField.get(f.store); var state=(org.rocksdb.ColumnFamilyHandle)stateField.get(f.store);
            byte[] key=java.nio.ByteBuffer.allocate(33).putLong(outer.group.getMostSignificantBits()).putLong(outer.group.getLeastSignificantBits()).put((byte)0x21).putLong(p.transactionId().getMostSignificantBits()).putLong(p.transactionId().getLeastSignificantBits()).array();
            db.put(state,key,new AbortedParticipant(terminal,new ApplyPosition(1,3)).encode());
            assertThrows(IllegalArgumentException.class,() -> f.store.participantFence(identity(p))); assertThrows(IllegalArgumentException.class,() -> f.store.estimateParticipantRetirement(retirement(terminal,original.position())));
            try (var pin=f.store.capture()) { assertThrows(java.io.IOException.class,() -> NativeRangeImage.write(root.resolve("bad-cut-image"),pin,f.store.ownership().interval(),limits)); }
            f.store.fence(); assertThrows(IllegalStateException.class,f.store::activate);
        }
    }
    @Test void unreportedPositiveVoteStillRequiresAbortStrictlyAfterNativePreparation() throws Exception {
        try (var f=outer.new Fixture(root.resolve("causal-abort"),true)) {
            helper.enable(f); var p=helper.preparation(f,helper.put(f,0,1,9),helper.point(outer.fixed,1),1); f.store.applyPrepared(new ApplyPosition(1,1),p); var terminal=helper.abort(p);
            assertNull(terminal.participant().prepared()); f.store.applyFinalization(new ApplyPosition(1,2),terminal); floor(f,3);
            var dbField=ReplicatedRangeStore.class.getDeclaredField("db"); var stateField=ReplicatedRangeStore.class.getDeclaredField("state"); dbField.setAccessible(true); stateField.setAccessible(true);
            var db=(org.rocksdb.RocksDB)dbField.get(f.store); var state=(org.rocksdb.ColumnFamilyHandle)stateField.get(f.store);
            byte[] key=java.nio.ByteBuffer.allocate(33).putLong(outer.group.getMostSignificantBits()).putLong(outer.group.getLeastSignificantBits()).put((byte)0x21).putLong(p.transactionId().getMostSignificantBits()).putLong(p.transactionId().getLeastSignificantBits()).array();
            db.put(state,key,new AbortedParticipant(terminal,new ApplyPosition(1,1)).encode()); var impossible=retirement(terminal,new ApplyPosition(1,1));
            assertThrows(IllegalArgumentException.class,() -> f.store.recoveredFinalization(terminal)); assertThrows(IllegalArgumentException.class,() -> f.store.estimateParticipantRetirement(impossible)); assertThrows(IllegalArgumentException.class,() -> f.store.applyParticipantRetirement(new ApplyPosition(1,4),impossible)); assertEquals(new ApplyPosition(1,3),f.store.applied());
            try (var pin=f.store.capture()) { assertThrows(java.io.IOException.class,() -> NativeRangeImage.write(root.resolve("bad-causal-image"),pin,f.store.ownership().interval(),limits)); }
            f.store.fence(); assertThrows(IllegalArgumentException.class,f.store::activate);
        }
    }
    @Test void reclaimedCapacityAllowsUuidReuseWhileOldReplaysNeverTouchTheNewBusinessFamily() throws Exception {
        try (var f=outer.new Fixture(root.resolve("capacity"),true)) {
            helper.enable(f); PreparedParticipant.Preparation old=null;
            for (int n=0;n<64;n++) { var p=helper.preparation(f,helper.put(f,n,n+1,1),helper.point(outer.fixed,n+1),n+1); f.store.applyPrepared(new ApplyPosition(1,n+1),p); if (n==0) old=p; }
            var original=Objects.requireNonNull(old); var terminal=commit(f.store.preparedParticipant(original.transactionId())); var committed=f.store.applyFinalization(new ApplyPosition(1,65),terminal); floor(f,66);
            var retirement=retirement(terminal,committed.position()); long beforeBytes=f.store.preparedCapacity().bytes(); f.store.applyParticipantRetirement(new ApplyPosition(1,67),retirement); assertEquals(63,f.store.preparedCapacity().count()); assertTrue(f.store.preparedCapacity().bytes()<beforeBytes);
            var business=new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(new UUID(0,183),2,0),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,helper.key(1),helper.key(9))));
            var planned=helper.preparation(f,business,helper.point(outer.fixed,1),1); var replacement=new PreparedParticipant.Preparation(planned.homeGroup(),planned.homeEpoch(),new OperationId(new UUID(0,184),2,0),planned.registration(),planned.business());
            f.store.applyPrepared(new ApplyPosition(1,68),replacement); assertEquals(64,f.store.preparedCapacity().count());
            assertTrue(f.store.applyPrepared(new ApplyPosition(1,69),original).retired()); assertTrue(f.store.applyFinalization(new ApplyPosition(1,70),terminal).retired()); f.store.applyParticipantRetirement(new ApplyPosition(1,71),retirement);
            assertEquals(replacement,f.store.preparedParticipant(original.transactionId()).preparation()); assertArrayEquals(helper.key(1),f.raw(outer.fixed,helper.key(1))); assertEquals(64,f.store.preparedCapacity().count());
            assertTrue(f.store.applyPrepared(new ApplyPosition(1,1),original).retired()); assertTrue(f.store.applyFinalization(new ApplyPosition(1,65),terminal).retired()); f.store.applyParticipantRetirement(new ApplyPosition(1,67),retirement); assertEquals(new ApplyPosition(1,71),f.store.applied());
            assertThrows(IllegalArgumentException.class,() -> f.store.applyPrepared(new ApplyPosition(2,71),original));
            var changed=new PreparedParticipant.Preparation(replacement.homeGroup(),replacement.homeEpoch()+1,replacement.operation(),replacement.registration(),replacement.business()); assertThrows(IllegalArgumentException.class,() -> f.store.applyPrepared(new ApplyPosition(1,72),changed));
            assertEquals(replacement,f.store.preparedParticipant(original.transactionId()).preparation());
        }
    }
}
