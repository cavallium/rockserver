package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.CdcGapDetectedException;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class NativeDistributedCdcTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativePreparedParticipantTest helper=new NativePreparedParticipantTest();
    final NativeBucketApplyTest outer=helper.outer;
    final DistributedCdcCheckpoint.Lineage lineage=new DistributedCdcCheckpoint.Lineage(outer.group,outer.group,1);
    ReplicatedRangeStore.CdcPage poll(NativeBucketApplyTest.Fixture f,DistributedCdcCheckpoint.Cursor from) throws Exception {
        return f.store.pollCdc(lineage,from,256,8*1024*1024);
    }

    @Test void successfulLogicalEffectsOnlyAndDuplicateDoesNotRepeatAfterReopenOrImageInstall() throws Exception {
        Path source=root.resolve("source"),image=root.resolve("image"); var limits=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4);
        DistributedCommand original;
        try(var f=outer.new Fixture(source,true)) {
            f.store.enableCdc(outer.group);
            original=outer.command(f.schema,0,List.of(outer.put(outer.bucket,7,1,new byte[]{9},Mutation.ReturnMode.NONE),
                    new Mutation(Mutation.Kind.PUT,outer.fixed,new byte[]{1},new byte[]{2}),
                    new Mutation(Mutation.Kind.DELETE_RANGE,outer.fixed,new byte[]{3},new byte[]{5})),List.of(),-1);
            assertEquals(CommandResult.Status.SUCCESS,f.apply(0,original).status());
            var failed=new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,1),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,new byte[]{1},new byte[]{99})),
                    List.of(new KeyPredicate(outer.fixed,new byte[]{1},new byte[]{88})),-1);
            assertEquals(CommandResult.Status.CONFLICT,f.apply(1,failed).status()); f.apply(2,original);
            var page=poll(f,DistributedCdcCheckpoint.Cursor.beginning()); assertEquals(1,page.records().size());
            var record=page.records().getFirst(); assertEquals(original.mutations(),record.command().mutations()); assertNull(record.finalization());
            assertEquals(List.of(0,1,2),record.events().stream().map(e->e.cursor().mutationOrdinal()).toList());
            assertEquals(record.end(),page.next());
            assertThrows(it.cavallium.rockserver.core.common.RocksDBException.class,()->f.store.pollCdc(lineage,DistributedCdcCheckpoint.Cursor.beginning(),1,1));
            try(var pin=f.store.capture()) { NativeRangeImage.write(image,pin,f.store.ownership().interval(),limits); }
        }
        for(boolean install:new boolean[]{false,true}) try(var f=outer.new Fixture(install ? root.resolve("target") : source,true)) {
            f.store.enableCdc(outer.group);
            if(install) { f.store.fence(); f.store.installSnapshot(image,limits); f.store.activate(); }
            f.apply(3,original); var page=poll(f,DistributedCdcCheckpoint.Cursor.beginning()); assertEquals(1,page.records().size());
            assertEquals(original.mutations(),page.records().getFirst().command().mutations());
            assertEquals(new ApplyPosition(1,3),page.cut());
        }
    }

    @Test void preparedAndAbortedPlansEmitNothingCommitHasFrozenDecisionAndOriginalBusinessPlan() throws Exception {
        var commits=new NativeCommittedParticipantTest();
        try(var f=outer.new Fixture(root.resolve("transactions"),true)) {
            f.store.enableCdc(outer.group); helper.enable(f);
            var preparation=helper.preparation(f,helper.put(f,0,1,9),helper.point(outer.fixed,1),1);
            f.store.applyPrepared(new ApplyPosition(1,1),preparation);
            assertTrue(poll(f,DistributedCdcCheckpoint.Cursor.beginning()).records().isEmpty());
            var intent=commits.commit(f.store.preparedParticipant(preparation.transactionId()));
            NativeParticipantRetirementTest.appliedCommit(f.store,new ApplyPosition(1,2),intent);
            NativeParticipantRetirementTest.appliedCommit(f.store,new ApplyPosition(1,3),intent);
            var aborted=helper.preparation(f,helper.put(f,1,2,8),helper.point(outer.fixed,2),2);
            f.store.applyPrepared(new ApplyPosition(1,4),aborted);
            NativeParticipantRetirementTest.appliedAbort(f.store,new ApplyPosition(1,5),helper.abort(aborted));
            var page=poll(f,DistributedCdcCheckpoint.Cursor.beginning()); assertEquals(1,page.records().size());
            var record=page.records().getFirst(); assertEquals(intent,record.finalization()); assertEquals(preparation.transactionId(),record.finalization().transactionId());
            assertEquals(new ApplyPosition(1,2),record.position()); assertEquals(preparation.business().mutations(),record.command().mutations());
            assertArrayEquals(record.encode(),DistributedCdcRecord.decode(record.encode()).encode());
            assertThrows(IllegalArgumentException.class,()->new DistributedCdcRecord(lineage,new ApplyPosition(1,6),aborted.business(),intent));
        }
    }

    @Test void boundedRetentionEvictsWholeEnvelopesAndAcknowledgedReceiptsDoNotEraseCdc() throws Exception {
        try(var f=outer.new Fixture(root.resolve("retention"),true)) {
            f.store.enableCdc(outer.group);
            for(int i=0;i<=4096;i++) {
                var command=new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,i),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,new byte[]{1},new byte[]{2})),List.of(),i==4096 ? i-1L : -1);
                f.apply(i,command);
            }
            assertThrows(CdcGapDetectedException.class,()->poll(f,DistributedCdcCheckpoint.Cursor.beginning()));
            var floor=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,0));
            var page=f.store.pollCdc(lineage,floor,1,8*1024*1024);
            assertEquals(floor,page.floor()); assertEquals(1,page.records().size()); assertEquals(new ApplyPosition(1,1),page.records().getFirst().position());
            assertEquals(DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,1)),page.next());
            assertEquals(1,f.store.receiptUsage().retainedCount());
            var nativeState=state(f); assertNull(nativeState.db().get(nativeState.state(),cell(0x50,0))); assertNull(nativeState.db().get(nativeState.state(),cell(0x52,0)));
            assertNotNull(nativeState.db().get(nativeState.state(),cell(0x52,1)));
        }
        try(var f=outer.new Fixture(root.resolve("retention"),true)) {
            f.store.enableCdc(outer.group);
            assertThrows(CdcGapDetectedException.class,()->poll(f,DistributedCdcCheckpoint.Cursor.beginning()));
            assertEquals(new ApplyPosition(1,1),f.store.pollCdc(lineage,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,0)),1,8*1024*1024).records().getFirst().position());
        }
    }

    @Test void enablingAfterExistingDataRequiresSnapshotResynchronization() throws Exception {
        try(var f=outer.new Fixture(root.resolve("baseline"),true)) {
            f.apply(0,helper.put(f,0,1,9)); f.store.enableCdc(outer.group);
            assertThrows(CdcGapDetectedException.class,()->poll(f,DistributedCdcCheckpoint.Cursor.beginning()));
            var cut=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,0)); assertTrue(poll(f,cut).records().isEmpty());
            f.apply(1,helper.put(f,1,2,8)); assertEquals(1,poll(f,cut).records().size());
        }
    }

    @Test void emptyGenesisHistoryRetainsItsFloorAcrossReopen() throws Exception {
        Path source=root.resolve("genesis");
        try(var f=outer.new Fixture(source,true)) {
            f.store.enableCdc(outer.group); f.store.applyNoop(new ApplyPosition(1,0));
            assertEquals(DistributedCdcCheckpoint.Cursor.beginning(),poll(f,DistributedCdcCheckpoint.Cursor.beginning()).floor());
        }
        try(var f=outer.new Fixture(source,true)) {
            f.store.enableCdc(outer.group);
            assertEquals(DistributedCdcCheckpoint.Cursor.beginning(),poll(f,DistributedCdcCheckpoint.Cursor.beginning()).floor());
        }
    }

    @Test void expiredReplayPreservesTheExistingDataBaselineAcrossReopen() throws Exception {
        Path source=root.resolve("expired-replay"); var baseline=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,1));
        try(var f=outer.new Fixture(source,true)) {
            var original=helper.put(f,0,1,9); f.apply(0,original);
            f.apply(1,new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,1),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,new byte[]{2},new byte[]{8})),List.of(),0));
            f.store.enableCdc(outer.group); assertEquals(CommandResult.Status.SESSION_EXPIRED,f.apply(2,original).status());
            assertTrue(poll(f,baseline).records().isEmpty());
        }
        try(var f=outer.new Fixture(source,true)) {
            f.store.enableCdc(outer.group); assertEquals(baseline,poll(f,baseline).floor());
        }
    }

    @Test void preparationWithoutEffectsPreservesTheExistingDataBaselineAcrossReopen() throws Exception {
        Path source=root.resolve("prepare-baseline"); var baseline=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,1));
        try(var f=outer.new Fixture(source,true)) {
            helper.enable(f); f.apply(1,helper.put(f,0,1,9)); f.store.enableCdc(outer.group);
            var preparation=helper.preparation(f,helper.put(f,1,2,8),helper.point(outer.fixed,2),1);
            f.store.applyPrepared(new ApplyPosition(1,2),preparation); assertTrue(poll(f,baseline).records().isEmpty());
        }
        try(var f=outer.new Fixture(source,true)) {
            f.store.enableCdc(outer.group); assertEquals(baseline,poll(f,baseline).floor());
        }
    }

    @Test void interruptedSnapshotWithCopiedCdcRowsCanReopenAndResumeInstallation() throws Exception {
        Path image=root.resolve("resume-image"),target=root.resolve("resume-target"); var limits=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4);
        try(var f=outer.new Fixture(root.resolve("resume-source"),true)) {
            f.store.enableCdc(outer.group); f.apply(0,helper.put(f,0,1,9));
            try(var pin=f.store.capture()) { NativeRangeImage.write(image,pin,f.store.ownership().interval(),limits); }
        }
        try(var f=outer.new Fixture(target,true)) {
            f.store.enableCdc(outer.group); f.apply(0,helper.put(f,0,1,7)); f.store.fence();
            assertThrows(java.io.IOException.class,()->f.store.installSnapshot(image,limits,batches->{ if(batches==2) throw new java.io.IOException("Interrupted after copying CDC rows"); }));
        }
        try(var f=outer.new Fixture(target,true)) {
            assertNull(f.store.applied()); f.store.enableCdc(outer.group);
            assertThrows(IllegalStateException.class,()->poll(f,DistributedCdcCheckpoint.Cursor.beginning()));
            f.store.installSnapshot(image,limits); f.store.activate();
            var page=poll(f,DistributedCdcCheckpoint.Cursor.beginning()); assertEquals(1,page.records().size());
            assertArrayEquals(new byte[]{9},f.raw(outer.fixed,new byte[]{1}));
        }
    }

    @Test void corruptedRetentionSummaryRejectsImageAndReopen() throws Exception {
        Path source=root.resolve("corrupt");
        try(var f=outer.new Fixture(source,true)) {
            f.store.enableCdc(outer.group); f.apply(0,helper.put(f,0,1,9));
            var dbField=ReplicatedRangeStore.class.getDeclaredField("db"); var stateField=ReplicatedRangeStore.class.getDeclaredField("state");
            dbField.setAccessible(true); stateField.setAccessible(true);
            var db=(org.rocksdb.RocksDB)dbField.get(f.store); var state=(org.rocksdb.ColumnFamilyHandle)stateField.get(f.store);
            byte[] key=cell(0x51,0);
            byte[] summary=db.get(state,key); java.nio.ByteBuffer.wrap(summary).putInt(60,2); db.put(state,key,summary);
            try(var pin=f.store.capture()) {
                assertThrows(java.io.IOException.class,()->NativeRangeImage.write(root.resolve("invalid-image"),pin,f.store.ownership().interval(),new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4)));
            }
        }
        try(var f=outer.new Fixture(source,true)) { assertThrows(IllegalArgumentException.class,()->f.store.enableCdc(outer.group)); }
    }

    @Test void tinyPollUsesCachedMetadataBeforeLoadingTheNextEnvelope() throws Exception {
        try(var f=outer.new Fixture(root.resolve("metadata-budget"),true)) {
            f.store.enableCdc(outer.group); f.apply(0,helper.put(f,0,1,9));
            var dbField=ReplicatedRangeStore.class.getDeclaredField("db"); var stateField=ReplicatedRangeStore.class.getDeclaredField("state");
            dbField.setAccessible(true); stateField.setAccessible(true);
            var db=(org.rocksdb.RocksDB)dbField.get(f.store); var state=(org.rocksdb.ColumnFamilyHandle)stateField.get(f.store);
            byte[] key=java.nio.ByteBuffer.allocate(25).putLong(outer.group.getMostSignificantBits()).putLong(outer.group.getLeastSignificantBits()).put((byte)0x50).putLong(0).array();
            int required=4+db.get(state,key).length; db.put(state,key,new byte[]{0});
            var large=assertThrows(ReplicatedRangeStore.CdcTooLarge.class,()->f.store.pollCdc(lineage,DistributedCdcCheckpoint.Cursor.beginning(),1,1));
            assertEquals(required,large.requiredPayloadBytes()); assertEquals(new ApplyPosition(1,0),large.page().cut());
            assertThrows(IllegalArgumentException.class,()->poll(f,DistributedCdcCheckpoint.Cursor.beginning()));
        }
    }
    @Test void cursorWitnessRequiresActualMutationOrCurrentCompletedBoundary() throws Exception {
        try(var f=outer.new Fixture(root.resolve("witness"),true)) {
            f.store.enableCdc(outer.group); f.store.applyNoop(new ApplyPosition(1,0)); f.apply(1,helper.put(f,0,1,9)); f.store.applyNoop(new ApplyPosition(1,2));
            var event=new DistributedCdcCheckpoint.Cursor(new ApplyPosition(1,1),0);
            assertTrue(poll(f,event).fromVerified());
            assertTrue(poll(f,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,1))).fromVerified());
            assertTrue(poll(f,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,2))).fromVerified());
            assertFalse(poll(f,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,0))).fromVerified());
            assertFalse(poll(f,new DistributedCdcCheckpoint.Cursor(new ApplyPosition(1,1),1)).fromVerified());
        }
    }
    @Test void noEffectTailNeverBecomesAnUnretainedDeliveryEndpoint() throws Exception {
        Path source=root.resolve("stable-delivery"); DistributedCdcCheckpoint.Cursor endpoint;
        try(var f=outer.new Fixture(source,true)) {
            f.store.enableCdc(outer.group); f.store.applyNoop(new ApplyPosition(1,0)); f.apply(1,helper.put(f,0,1,9)); f.store.applyNoop(new ApplyPosition(1,2));
            var page=poll(f,DistributedCdcCheckpoint.Cursor.beginning()); endpoint=page.records().getLast().end();
            assertEquals(new ApplyPosition(1,2),page.cut()); assertEquals(endpoint,page.next());
            f.store.applyNoop(new ApplyPosition(2,3));
            assertTrue(poll(f,endpoint).fromVerified());
            var idle=poll(f,endpoint); assertTrue(idle.records().isEmpty()); assertEquals(endpoint,idle.next()); assertEquals(new ApplyPosition(2,3),idle.cut());
        }
        try(var f=outer.new Fixture(source,true)) {
            f.store.enableCdc(outer.group); var idle=poll(f,endpoint);
            assertTrue(idle.fromVerified()); assertEquals(endpoint,idle.next());
        }
    }

    private static final long DAY=24L*60*60*1000;
    private record NativeState(org.rocksdb.RocksDB db,org.rocksdb.ColumnFamilyHandle state) {}
    NativeState state(NativeBucketApplyTest.Fixture f) throws Exception {
        var db=ReplicatedRangeStore.class.getDeclaredField("db"); var state=ReplicatedRangeStore.class.getDeclaredField("state"); db.setAccessible(true); state.setAccessible(true);
        return new NativeState((org.rocksdb.RocksDB)db.get(f.store),(org.rocksdb.ColumnFamilyHandle)state.get(f.store));
    }
    byte[] cell(int tag,long index) {
        var out=java.nio.ByteBuffer.allocate(tag==0x51 ? 17 : 25).putLong(outer.group.getMostSignificantBits()).putLong(outer.group.getLeastSignificantBits()).put((byte)tag);
        if(tag!=0x51) out.putLong(index); return out.array();
    }
    long age(NativeBucketApplyTest.Fixture f,long index) throws Exception { var nativeState=state(f); return java.nio.ByteBuffer.wrap(nativeState.db().get(nativeState.state(),cell(0x52,index))).getLong(); }
    DistributedCommand tick(NativeBucketApplyTest.Fixture f,long generation,long time,boolean bind) {
        var authority=new SessionAuthority(outer.cluster,outer.database,generation,time,1);
        var control=bind ? new RangeSessionControl(authority,new SessionEpochLedger.Grant(1,outer.session,0,DAY),4096) : new RangeSessionControl(authority,null,0);
        return DistributedCommand.sessionControl(outer.group,1,f.schema.digest(),new OperationId(UUID.randomUUID(),1,0),control,-1);
    }
    void clock(NativeBucketApplyTest.Fixture f,long index,long generation,long time) throws Exception { assertEquals(CommandResult.Status.SUCCESS,f.apply(index,tick(f,generation,time,false)).status()); }
    @Test void committedZeroClockAnchorsPendingAndCatchupPreservesTheNewCohortUntilExactAgeBoundary() throws Exception {
        try(var f=outer.new Fixture(root.resolve("logical-age"),true)) {
            f.store.enableCdc(outer.group); f.apply(0,helper.put(f,0,1,9)); assertEquals(-1,age(f,0)); f.store.enableSessionControls();
            assertEquals(CommandResult.Status.SUCCESS,f.apply(1,tick(f,1,0,true)).status()); assertEquals(0,age(f,0));
            f.apply(2,helper.put(f,1,2,8)); assertEquals(-1,age(f,2)); clock(f,3,2,0); assertEquals(-1,age(f,2));
            clock(f,4,1,0); assertEquals(-1,age(f,2)); assertEquals(2,poll(f,DistributedCdcCheckpoint.Cursor.beginning()).records().size());
            var catchup=tick(f,3,DAY,false); assertEquals(CommandResult.Status.SUCCESS,f.apply(5,catchup).status());
            var floor=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,0)); assertThrows(CdcGapDetectedException.class,()->poll(f,DistributedCdcCheckpoint.Cursor.beginning()));
            assertEquals(1,poll(f,floor).records().size()); assertEquals(DAY,age(f,2));
            clock(f,6,4,DAY); f.apply(7,catchup); assertEquals(DAY,age(f,2));
            clock(f,8,5,2*DAY-1); assertEquals(1,poll(f,floor).records().size()); clock(f,9,6,2*DAY);
            var emptyFloor=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,2)); assertTrue(poll(f,emptyFloor).records().isEmpty()); assertEquals(emptyFloor,poll(f,emptyFloor).floor());
            var nativeState=state(f); assertNull(nativeState.db().get(nativeState.state(),cell(0x50,2))); assertNull(nativeState.db().get(nativeState.state(),cell(0x52,2)));
        }
    }
    @Test void logicalClockAndAgeCellsSurviveReopenAndNativeImageImport() throws Exception {
        Path source=root.resolve("age-source"),image=root.resolve("age-image"); var limits=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4);
        try(var f=outer.new Fixture(source,true)) {
            f.store.enableCdc(outer.group); f.apply(0,helper.put(f,0,1,9)); f.store.enableSessionControls(); f.apply(1,tick(f,1,0,true)); f.apply(2,helper.put(f,1,2,8)); clock(f,3,2,DAY);
            NativeRangeImage.write(image,f.store,limits);
        }
        for(boolean install:new boolean[]{false,true}) try(var f=outer.new Fixture(install ? root.resolve("age-target") : source,true)) {
            f.store.enableCdc(outer.group); if(install) { f.store.fence(); f.store.installSnapshot(image,limits); f.store.activate(); }
            f.store.enableSessionControls(); var floor=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,0)); assertEquals(DAY,age(f,2)); clock(f,4,3,DAY); assertEquals(DAY,age(f,2));
            clock(f,5,4,2*DAY); assertTrue(poll(f,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,2))).records().isEmpty());
            assertThrows(CdcGapDetectedException.class,()->poll(f,floor));
        }
    }
    @Test void byteRetentionChargesAgeRowsAndDeletesTheirCellsWithTheirEvents() throws Exception {
        try(var f=outer.new Fixture(root.resolve("age-bytes"),true)) {
            f.store.enableCdc(outer.group); byte[] value=new byte[2_000_000];
            for(int i=0;i<34;i++) f.apply(i,new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,i),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,new byte[]{1},value))));
            var nativeState=state(f); byte[] summary=nativeState.db().get(nativeState.state(),cell(0x51,0)); assertEquals(80,summary.length);
            var fields=java.nio.ByteBuffer.wrap(summary); int count=fields.getInt(60); long bytes=fields.getLong(64); assertTrue(count<34); assertTrue(bytes<=64L*1024*1024);
            long floor=fields.getLong(48); long expected=0;
            for(long i=0;i<34;i++) { byte[] record=nativeState.db().get(nativeState.state(),cell(0x50,i)),age=nativeState.db().get(nativeState.state(),cell(0x52,i)); if(i<=floor) { assertNull(record); assertNull(age); } else { assertNotNull(record); assertEquals(8,age.length); expected+=58L+record.length; } }
            assertEquals(expected,bytes);
        }
    }
    @Test void ageEvictionUsesCachedMetadataWithoutDecodingExpiredPayload() throws Exception {
        Path path=root.resolve("expired-payload");
        try(var f=outer.new Fixture(path,true)) {
            f.store.enableCdc(outer.group); f.apply(0,helper.put(f,0,1,9)); f.store.enableSessionControls(); f.apply(1,tick(f,1,0,true)); var nativeState=state(f);
            nativeState.db().put(nativeState.state(),cell(0x50,0),new byte[]{0}); clock(f,2,2,DAY);
            assertNull(nativeState.db().get(nativeState.state(),cell(0x50,0))); assertNull(nativeState.db().get(nativeState.state(),cell(0x52,0)));
            assertTrue(poll(f,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,0))).records().isEmpty());
        }
        try(var f=outer.new Fixture(path,true)) { f.store.enableCdc(outer.group); assertTrue(poll(f,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,0))).records().isEmpty()); }
    }
    @Test void malformedMissingOrOutOfOrderAgeCellsRejectCurrentFormatRecoveryAndImage() throws Exception {
        for(int corruption=0;corruption<5;corruption++) {
            Path path=root.resolve("bad-age-"+corruption);
            try(var f=outer.new Fixture(path,true)) {
                f.store.enableCdc(outer.group); f.apply(0,helper.put(f,0,1,9)); f.store.enableSessionControls(); f.apply(1,tick(f,1,0,true)); f.apply(2,helper.put(f,1,2,8));
                var nativeState=state(f);
                switch(corruption) {
                    case 0 -> nativeState.db().delete(nativeState.state(),cell(0x52,0));
                    case 1 -> nativeState.db().put(nativeState.state(),cell(0x52,3),java.nio.ByteBuffer.allocate(8).putLong(-1).array());
                    case 2 -> nativeState.db().put(nativeState.state(),cell(0x52,0),new byte[7]);
                    case 3 -> { nativeState.db().put(nativeState.state(),cell(0x52,0),java.nio.ByteBuffer.allocate(8).putLong(-1).array()); nativeState.db().put(nativeState.state(),cell(0x52,2),java.nio.ByteBuffer.allocate(8).putLong(0).array()); }
                    case 4 -> nativeState.db().put(nativeState.state(),cell(0x52,0),java.nio.ByteBuffer.allocate(8).putLong(1).array());
                }
                try(var pin=f.store.capture()) { assertThrows(java.io.IOException.class,()->NativeRangeImage.write(root.resolve("bad-age-image-"+path.getFileName()),pin,f.store.ownership().interval(),new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4))); }
            }
            try(var f=outer.new Fixture(path,true)) { assertThrows(IllegalArgumentException.class,()->f.store.enableCdc(outer.group)); }
        }
    }

    @Test void enablingCdcAfterAnExistingAuthorityDoesNotAnchorPendingWritesOnTheOldClock() throws Exception {
        Path path=root.resolve("authority-baseline"); var baseline=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,0));
        try(var f=outer.new Fixture(path,true)) {
            f.store.enableSessionControls(); var authority=new SessionAuthority(outer.cluster,outer.database,1,100,1);
            var control=new RangeSessionControl(authority,new SessionEpochLedger.Grant(1,outer.session,100,100+DAY),4096);
            var binding=DistributedCommand.sessionControl(outer.group,1,f.schema.digest(),new OperationId(UUID.randomUUID(),1,0),control,-1);
            assertEquals(CommandResult.Status.SUCCESS,f.apply(0,binding).status()); f.store.enableCdc(outer.group);
            f.apply(1,helper.put(f,0,1,9)); assertEquals(-1,age(f,1)); clock(f,2,2,100); assertEquals(-1,age(f,1));
        }
        try(var f=outer.new Fixture(path,true)) {
            f.store.enableCdc(outer.group); f.store.enableSessionControls(); clock(f,3,3,100); assertEquals(-1,age(f,1));
            clock(f,4,4,100+10*DAY); var page=poll(f,baseline); assertEquals(1,page.records().size()); assertEquals(baseline,page.floor()); assertEquals(100+10*DAY,age(f,1));
        }
    }

    @Test void imageWithoutCdcSeedsItsImportedBaselineFromThePersistedAuthority() throws Exception {
        Path image=root.resolve("authority-only-image"); var limits=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4);
        try(var f=outer.new Fixture(root.resolve("authority-only-source"),true)) {
            f.store.enableSessionControls(); var authority=new SessionAuthority(outer.cluster,outer.database,1,100,1);
            var control=new RangeSessionControl(authority,new SessionEpochLedger.Grant(1,outer.session,100,100+DAY),4096);
            f.apply(0,DistributedCommand.sessionControl(outer.group,1,f.schema.digest(),new OperationId(UUID.randomUUID(),1,0),control,-1));
            NativeRangeImage.write(image,f.store,limits);
        }
        try(var f=outer.new Fixture(root.resolve("authority-only-target"),true)) {
            f.store.enableCdc(outer.group); f.store.fence(); f.store.installSnapshot(image,limits); f.store.activate(); f.store.enableSessionControls();
            var baseline=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,0)); f.apply(1,helper.put(f,0,1,9)); clock(f,2,2,100); assertEquals(-1,age(f,1));
            clock(f,3,3,100+10*DAY); assertEquals(1,poll(f,baseline).records().size()); assertEquals(baseline,poll(f,baseline).floor());
        }
    }

    @Test void pendingCohortClockMustMatchPersistedAuthorityInImageAndReopen() throws Exception {
        for(long forged:new long[]{50,100+DAY}) {
            Path path=root.resolve("forged-clock-"+forged);
            try(var f=outer.new Fixture(path,true)) {
                f.store.enableSessionControls();
                var authority=new SessionAuthority(outer.cluster,outer.database,1,100,1);
                var control=new RangeSessionControl(authority,new SessionEpochLedger.Grant(1,outer.session,100,100+DAY),4096);
                f.apply(0,DistributedCommand.sessionControl(outer.group,1,f.schema.digest(),new OperationId(UUID.randomUUID(),1,0),control,-1));
                f.store.enableCdc(outer.group); f.apply(1,helper.put(f,0,1,9)); assertEquals(-1,age(f,1));
                var nativeState=state(f); byte[] key=cell(0x51,0);
                byte[] summary=nativeState.db().get(nativeState.state(),key); java.nio.ByteBuffer.wrap(summary).putLong(72,forged); nativeState.db().put(nativeState.state(),key,summary);
                try(var pin=f.store.capture()) { assertThrows(java.io.IOException.class,()->NativeRangeImage.write(root.resolve("forged-clock-image-"+forged),pin,f.store.ownership().interval(),new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4))); }
            }
            try(var f=outer.new Fixture(path,true)) { assertThrows(IllegalArgumentException.class,()->f.store.enableCdc(outer.group)); }
        }
    }

}
