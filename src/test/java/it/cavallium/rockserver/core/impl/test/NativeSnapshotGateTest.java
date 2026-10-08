package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Trusted native gate lifecycle; adapter admission, home provenance and quorum acceptance are separate. */
class NativeSnapshotGateTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativePreparedParticipantTest helper=new NativePreparedParticipantTest();
    final NativeBucketApplyTest outer=helper.outer;
    final NativeRangeImage.Limits limits=new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,4);
    byte[] key(int n) { return helper.key(n); }
    SnapshotFence fence(NativeBucketApplyTest.Fixture f,int id,int point) {
        return new SnapshotFence(outer.cluster,outer.database,outer.domain,new UUID(0,500+id),new UUID(0,400),1,7,1,f.schema.digest(),
                List.of(new SnapshotFence.Participant(outer.group,outer.group,1,f.schema.digest(),new OperationId(new UUID(0,401),1,id*2L),new OperationId(new UUID(0,401),1,id*2L+1),helper.point(outer.fixed,point))));
    }
    SnapshotFenceStatus completed(SnapshotFence fence,ApplyPosition installed,ApplyPosition captured) {
        return SnapshotFenceStatus.begin(fence).ready(outer.group,installed).capture(outer.group,new SnapshotFenceStatus.Capture(new UUID(0,600),new UUID(0,601),captured)).complete();
    }
    @Test void gateRejectsNewEffectsAndPreparesWhileExactReceiptsAndRetainedPinsStayStable() throws Exception {
        try (var f=outer.new Fixture(root.resolve("gate"),true)) {
            helper.enable(f); var original=helper.put(f,0,1,1); f.apply(1,original);
            var late=helper.preparation(f,helper.put(f,10,1,9),helper.point(outer.fixed,1),10);
            var fence=fence(f,1,1); var installed=f.store.applyGateInstall(new ApplyPosition(1,2),fence);
            assertEquals(new ApplyPosition(1,2),installed.installed()); assertTrue(f.store.gateBlockers(fence).isEmpty());
            try (var tracked=f.store.captureTracked(helper.point(outer.fixed,1))) {
                assertThrows(IllegalStateException.class,() -> f.store.planScopedCommit(tracked,helper.put(f,11,1,8),0,new UUID(0,603)));
            }
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.store.apply(new ApplyPosition(1,3),original)).status());
            var blocked=helper.put(f,1,1,7); assertThrows(IllegalStateException.class,() -> f.store.estimateReceiptRetention(blocked));
            assertEquals(CommandResult.Status.CONFLICT,CommandResult.decode(f.store.apply(new ApplyPosition(1,4),blocked)).status());
            assertArrayEquals(key(1),f.raw(outer.fixed,key(1)));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(5,helper.put(f,2,2,8)).status());
            assertEquals(ParticipantVote.negative(),f.store.applyPrepared(new ApplyPosition(1,6),late));
            assertEquals(ParticipantVote.negative(),f.store.applyPrepared(new ApplyPosition(2,7),late));
            try (var pin=f.store.captureGate(fence)) {
                assertArrayEquals(key(1),pin.get(outer.fixed,key(1)));
                var terminal=completed(fence,installed.installed(),pin.position());
                assertFalse(f.store.applyGateRelease(new ApplyPosition(2,8),terminal).active());
                assertEquals(CommandResult.Status.CONFLICT,CommandResult.decode(f.store.apply(new ApplyPosition(2,9),blocked)).status());
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.store.apply(new ApplyPosition(2,10),helper.put(f,3,1,7))).status());
                assertArrayEquals(key(1),pin.get(outer.fixed,key(1))); assertArrayEquals(key(7),f.raw(outer.fixed,key(1)));
                assertEquals(new ApplyPosition(2,8),f.store.applyGateRelease(new ApplyPosition(2,11),terminal).released());
                assertFalse(f.store.applyGateInstall(new ApplyPosition(2,12),fence).active());
                assertThrows(IllegalStateException.class,() -> f.store.captureGate(fence));
            }
            assertEquals(0,f.store.activeSnapshotPins());
        }
    }
    @Test void installedGateWaitsForPriorPreparedWorkAndAllowsBothTaggedDecisionsToDrain() throws Exception {
        for (boolean commit:new boolean[]{false,true}) try (var f=outer.new Fixture(root.resolve("drain-"+commit),true)) {
            helper.enable(f); var prior=helper.preparation(f,helper.put(f,0,1,9),helper.point(outer.fixed,1),1);
            var late=helper.preparation(f,helper.put(f,1,1,7),helper.point(outer.fixed,1),2);
            f.store.applyPrepared(new ApplyPosition(1,1),prior); var fence=fence(f,1,1);
            f.store.applyGateInstall(new ApplyPosition(1,2),fence);
            assertEquals(1,f.store.gateBlockers(fence).size()); assertThrows(IllegalStateException.class,() -> f.store.captureGate(fence));
            assertEquals(ParticipantVote.negative(),f.store.applyPrepared(new ApplyPosition(1,3),late));
            var prepared=f.store.preparedParticipant(prior.transactionId());
            var decision=commit ? prior.registration().prepared(outer.group,prior.business().payloadDigest(),prepared.position()).decide(TransactionStatus.Phase.COMMIT) : prior.registration().decide(TransactionStatus.Phase.ABORT);
            var finalization=new ParticipantFinalization(prior.homeGroup(),prior.homeEpoch(),outer.group,new OperationId(new UUID(0,602),2,0),decision);
            if (commit) f.store.applyCommit(new ApplyPosition(2,4),finalization); else f.store.applyAbort(new ApplyPosition(2,4),finalization);
            assertTrue(f.store.gateBlockers(fence).isEmpty());
            try (var pin=f.store.captureGate(fence)) { assertArrayEquals(commit ? key(9) : null,pin.get(outer.fixed,key(1))); }
            f.store.applyGateRelease(new ApplyPosition(2,5),SnapshotFenceStatus.begin(fence).abort());
        }
    }
    @Test void activeGatesSurviveReopenAndImageInstallUntilReplicatedRelease() throws Exception {
        Path source=root.resolve("source"),image=root.resolve("image"); SnapshotFence fence;
        try (var f=outer.new Fixture(source,true)) {
            helper.enable(f); fence=fence(f,1,1); f.store.applyGateInstall(new ApplyPosition(1,1),fence);
            try (var pin=f.store.capture()) { NativeRangeImage.write(image,pin,f.store.ownership().interval(),limits); }
            NativeRangeImage.inspect(image,limits);
        }
        for (boolean install:new boolean[]{false,true}) try (var f=outer.new Fixture(install ? root.resolve("target") : source,true)) {
            if (install) { f.store.fence(); f.store.installSnapshot(image,limits); f.store.activate(); }
            assertTrue(f.store.snapshotGate(fence.fenceId()).active());
            assertThrows(IllegalStateException.class,() -> f.store.estimateReceiptRetention(helper.put(f,0,1,9)));
            assertEquals(CommandResult.Status.CONFLICT,CommandResult.decode(f.store.apply(new ApplyPosition(1,2),helper.put(f,0,1,9))).status());
            try (var pin=f.store.captureGate(fence)) { assertNull(pin.get(outer.fixed,key(1))); }
            f.store.applyGateRelease(new ApplyPosition(2,3),SnapshotFenceStatus.begin(fence).abort());
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.store.apply(new ApplyPosition(2,4),helper.put(f,1,1,7))).status());
            var releasedImage=root.resolve("released-"+install);
            try (var pin=f.store.capture()) { NativeRangeImage.write(releasedImage,pin,f.store.ownership().interval(),limits); }
            NativeRangeImage.inspect(releasedImage,limits);
            f.store.fence(); f.store.activate(); assertFalse(f.store.applyGateInstall(new ApplyPosition(2,5),fence).active());
        }
    }
    @Test void releaseBeforeInstallSuppressesLateInstallationAndCompleteCannotInventInstallation() throws Exception {
        try (var f=outer.new Fixture(root.resolve("release-first"),true)) {
            helper.enable(f); var fence=fence(f,1,1); var terminal=SnapshotFenceStatus.begin(fence).abort();
            var released=f.store.applyGateRelease(new ApplyPosition(1,1),terminal); assertNull(released.installed()); assertFalse(released.active());
            assertNull(f.store.applyGateInstall(new ApplyPosition(1,2),fence).installed());
            assertEquals(CommandResult.Status.SUCCESS,f.apply(3,helper.put(f,0,1,9)).status());
            var another=fence(f,2,2); var forged=completed(another,new ApplyPosition(1,1),new ApplyPosition(1,1));
            assertThrows(IllegalArgumentException.class,() -> f.store.applyGateRelease(new ApplyPosition(1,4),forged));
            assertEquals(new ApplyPosition(1,3),f.store.applied());
            f.store.fence(); f.store.activate(); assertFalse(f.store.applyGateInstall(new ApplyPosition(1,4),fence).active());
        }
    }
    @Test void allInstalledGatesCanReleaseAtCapacityAndOverlapsFailWithoutMutation() throws Exception {
        try (var f=outer.new Fixture(root.resolve("gate-capacity"),true)) {
            helper.enable(f); var fences=new ArrayList<SnapshotFence>();
            for (int i=0;i<ReplicatedRangeStore.MAX_SNAPSHOT_GATES;i++) { var fence=fence(f,i,i); fences.add(fence); f.store.applyGateInstall(new ApplyPosition(1,i+1),fence); }
            var overlap=fence(f,100,0); var before=f.store.gateCapacity();
            assertThrows(IllegalStateException.class,() -> f.store.estimateGateInstall(overlap)); assertEquals(before,f.store.gateCapacity());
            var overflow=fence(f,101,100); assertThrows(IllegalStateException.class,() -> f.store.estimateGateInstall(overflow));
            for (int i=0;i<fences.size();i++) f.store.applyGateRelease(new ApplyPosition(1,17+i),SnapshotFenceStatus.begin(fences.get(i)).abort());
            assertEquals(16,f.store.gateCapacity().count()); assertTrue(f.store.gateCapacity().bytes()<=ReplicatedRangeStore.MAX_SNAPSHOT_GATE_BYTES);
            assertFalse(f.store.applyGateInstall(new ApplyPosition(2,33),fences.getFirst()).active());
            f.store.fence(); f.store.activate(); assertEquals(16,f.store.gateCapacity().count());
        }
    }
    @Test void imageReplacementDropsPriorGateNamespaceBeforeRecoveringPreparedOwners() throws Exception {
        var image=root.resolve("replacement-image"); PreparedParticipant.Preparation preparation;
        try (var f=outer.new Fixture(root.resolve("replacement-source"),true)) {
            helper.enable(f); preparation=helper.preparation(f,helper.put(f,0,1,9),helper.point(outer.fixed,1),1);
            f.store.applyPrepared(new ApplyPosition(1,1),preparation);
            try (var pin=f.store.capture()) { NativeRangeImage.write(image,pin,f.store.ownership().interval(),limits); }
        }
        try (var f=outer.new Fixture(root.resolve("replacement-target"),true)) {
            helper.enable(f); var base=fence(f,1,2); var participant=base.participant(outer.group);
            var old=new SnapshotFence(base.clusterId(),base.databaseId(),base.domainId(),base.fenceId(),base.homeGroup(),base.homeEpoch(),base.directoryGeneration(),base.schemaEpoch(),base.schemaDigest(),
                    List.of(new SnapshotFence.Participant(participant.rangeId(),participant.groupId(),participant.rangeEpoch(),participant.schemaDigest(),preparation.operation(),participant.releaseOperation(),participant.scope())));
            f.store.applyGateInstall(new ApplyPosition(1,1),old); f.store.fence(); f.store.installSnapshot(image,limits); f.store.activate();
            assertEquals(0,f.store.gateCapacity().count()); assertNull(f.store.snapshotGate(old.fenceId()));
            assertEquals(ParticipantVote.prepared(new ApplyPosition(1,1)),f.store.recoveredPreparationVote(preparation));
            assertThrows(IllegalStateException.class,() -> f.store.captureReadable(helper.point(outer.fixed,1)));
        }
    }
    @Test void overlappingGateImageIsRejectedBeforeInstallationOrActivation() throws Exception {
        try (var f=outer.new Fixture(root.resolve("corrupt-gates"),true)) {
            helper.enable(f); var first=fence(f,1,1); var second=fence(f,2,2);
            f.store.applyGateInstall(new ApplyPosition(1,1),first); f.store.applyGateInstall(new ApplyPosition(1,2),second);
            var corrupt=SnapshotGate.install(fence(f,2,1),outer.group,new ApplyPosition(1,2));
            var dbField=ReplicatedRangeStore.class.getDeclaredField("db"); var stateField=ReplicatedRangeStore.class.getDeclaredField("state");
            dbField.setAccessible(true); stateField.setAccessible(true);
            byte[] recordKey=java.nio.ByteBuffer.allocate(33).putLong(outer.group.getMostSignificantBits()).putLong(outer.group.getLeastSignificantBits()).put((byte)0x24).putLong(second.fenceId().getMostSignificantBits()).putLong(second.fenceId().getLeastSignificantBits()).array();
            ((org.rocksdb.RocksDB)dbField.get(f.store)).put((org.rocksdb.ColumnFamilyHandle)stateField.get(f.store),recordKey,corrupt.encode());
            try (var pin=f.store.capture()) { assertThrows(java.io.IOException.class,() -> NativeRangeImage.write(root.resolve("bad-gate-image"),pin,f.store.ownership().interval(),limits)); }
            f.store.fence(); assertThrows(IllegalArgumentException.class,f.store::activate); assertFalse(f.store.admissionOpen());
        }
    }
    @Test void gateControlsCannotStealOrDonateDurableOperationIdentities() throws Exception {
        try (var f=outer.new Fixture(root.resolve("gate-identities"),true)) {
            helper.enable(f); var fence=fence(f,1,1); var control=fence.participant(outer.group).installOperation();
            var ordinary=new DistributedCommand(outer.group,1,f.schema.digest(),control,List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,key(2),key(2))));
            f.apply(1,ordinary); assertThrows(IllegalArgumentException.class,() -> f.store.applyGateInstall(new ApplyPosition(1,2),fence));
            var gate=fence(f,2,1); f.store.applyGateInstall(new ApplyPosition(1,2),gate);
            var collision=new DistributedCommand(outer.group,1,f.schema.digest(),gate.participant(outer.group).releaseOperation(),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,key(2),key(3))));
            assertThrows(IllegalArgumentException.class,() -> f.store.estimateReceiptRetention(collision));
            assertThrows(IllegalArgumentException.class,() -> f.store.apply(new ApplyPosition(1,3),collision));
            assertEquals(new ApplyPosition(1,2),f.store.applied());
        }
    }
}
