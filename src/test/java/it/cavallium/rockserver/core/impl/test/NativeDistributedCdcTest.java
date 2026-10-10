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
            assertEquals(DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,2)),page.next());
            assertThrows(IllegalArgumentException.class,()->f.store.pollCdc(lineage,DistributedCdcCheckpoint.Cursor.beginning(),1,1));
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
            byte[] key=java.nio.ByteBuffer.allocate(17).putLong(outer.group.getMostSignificantBits()).putLong(outer.group.getLeastSignificantBits()).put((byte)0x51).array();
            byte[] summary=db.get(state,key); java.nio.ByteBuffer.wrap(summary).putInt(60,2); db.put(state,key,summary);
            try(var pin=f.store.capture()) {
                assertThrows(java.io.IOException.class,()->NativeRangeImage.write(root.resolve("invalid-image"),pin,f.store.ownership().interval(),new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4)));
            }
        }
        try(var f=outer.new Fixture(source,true)) { assertThrows(IllegalArgumentException.class,()->f.store.enableCdc(outer.group)); }
    }
}
