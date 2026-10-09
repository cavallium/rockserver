package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native gate retention and reclamation only; HOME consumption provenance is supplied by the adapter. */
class NativeSnapshotGateRetirementTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeSnapshotGateTest helper=new NativeSnapshotGateTest();
    final NativePreparedParticipantTest prepared=helper.helper;
    final NativeBucketApplyTest outer=helper.outer;
    SnapshotFence fence(NativeBucketApplyTest.Fixture f,int id,int point) { return helper.fence(f,id,point); }
    SnapshotFence rebind(SnapshotFence old,OperationId operation,int point) {
        var local=old.participant(outer.group); var controls=new UUID(0,1200);
        return new SnapshotFence(old.clusterId(),old.databaseId(),old.domainId(),old.fenceId(),old.homeGroup(),old.homeEpoch(),operation,old.directoryGeneration(),old.schemaEpoch(),old.schemaDigest(),List.of(
                new SnapshotFence.Participant(local.rangeId(),local.groupId(),local.rangeEpoch(),local.schemaDigest(),new OperationId(controls,2,point*2L),new OperationId(controls,2,point*2L+1),prepared.point(outer.fixed,point))));
    }
    void floor(NativeBucketApplyTest.Fixture f,long index) throws Exception { f.store.apply(new ApplyPosition(1,index),DistributedCommand.advanceSessionEpochFloor(outer.group,1,f.schema.digest(),new OperationId(new UUID(0,1201),2,index),2)); }
    @Test void consumedCompleteReclaimsGateWhileHistoricalPinsRemainReadableAcrossReopenAndImage() throws Exception {
        Path source=root.resolve("complete"),image=root.resolve("complete.image"); SnapshotFence fence; SnapshotGateRetirement retirement; long last;
        try (var f=outer.new Fixture(source,true)) {
            prepared.enable(f); f.apply(1,prepared.put(f,0,1,7)); fence=fence(f,1,1); var installed=f.store.applyGateInstall(new ApplyPosition(1,2),fence).gate();
            try (var captured=f.store.captureGate(fence)) {
                var complete=helper.completed(fence,installed.installed(),captured.position()); var released=f.store.applyGateRelease(new ApplyPosition(1,3),complete).gate(); retirement=new SnapshotGateRetirement(complete.consume(),outer.group,released.released()); floor(f,4);
                assertNull(f.store.snapshotFence(fence)); assertTrue(f.store.estimateSnapshotGateRetirement(retirement)); var usage=f.store.receiptUsage();
                try (var old=f.store.capture()) {
                    f.store.applySnapshotGateRetirement(new ApplyPosition(1,5),retirement); assertEquals(0,f.store.gateCapacity().count()); assertEquals(0,f.store.gateCapacity().bytes()); assertEquals(usage.retainedCount(),f.store.receiptUsage().retainedCount()); assertEquals(usage.retainedBytes(),f.store.receiptUsage().retainedBytes());
                    assertEquals(released,old.snapshotGateResult(fence).gate()); assertNull(old.snapshotFence(fence)); assertFalse(f.store.estimateSnapshotGateRetirement(retirement));
                    var write=new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(new UUID(0,1202),2,0),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,prepared.key(1),prepared.key(9)))); f.store.apply(new ApplyPosition(1,6),write);
                    assertArrayEquals(prepared.key(7),captured.get(outer.fixed,prepared.key(1))); assertArrayEquals(prepared.key(9),f.raw(outer.fixed,prepared.key(1)));
                    assertTrue(f.store.applyGateInstall(new ApplyPosition(1,7),fence).retired()); assertTrue(f.store.applyGateRelease(new ApplyPosition(1,8),retirement.consumed()).retired()); f.store.applySnapshotGateRetirement(new ApplyPosition(1,9),retirement); last=9;
                    assertNull(f.store.snapshotGate(fence.fenceId())); var proof=f.store.snapshotFence(fence); assertEquals(2,proof.minimumEpoch()); assertEquals(new ApplyPosition(1,9),proof.position()); assertNull(f.store.snapshotGateResult(fence).gate());
                }
            }
            try (var pin=f.store.capture()) { assertTrue(pin.snapshotGateResult(fence).retired()); assertEquals(pin.position(),pin.snapshotFence(fence).position()); NativeRangeImage.write(image,pin,f.store.ownership().interval(),helper.limits); }
        }
        NativeRangeImage.inspect(image,helper.limits);
        for (boolean install:new boolean[]{false,true}) try (var f=outer.new Fixture(install ? root.resolve("complete-target") : source,true)) {
            if (install) { f.store.fence(); f.store.installSnapshot(image,helper.limits); f.store.activate(); }
            assertEquals(2,f.store.snapshotRetentionFloor()); assertTrue(f.store.snapshotGateResult(fence).retired()); assertEquals(0,f.store.gateCapacity().count()); assertArrayEquals(prepared.key(9),f.raw(outer.fixed,prepared.key(1)));
            assertTrue(f.store.applyGateInstall(new ApplyPosition(2,last+1),fence).retired()); f.store.applySnapshotGateRetirement(new ApplyPosition(2,last+2),retirement); assertNull(f.store.snapshotGate(fence.fenceId()));
        }
    }
    @Test void releaseBeforeInstallAbortCanBeCollectedWithoutLeavingALateInstallTombstone() throws Exception {
        try (var f=outer.new Fixture(root.resolve("abort-first"),true)) {
            prepared.enable(f); var fence=fence(f,1,1); var terminal=SnapshotFenceStatus.begin(fence).abort(); var released=f.store.applyGateRelease(new ApplyPosition(1,1),terminal).gate(); assertNull(released.installed());
            assertEquals(released,f.store.applyGateInstall(new ApplyPosition(1,2),fence).gate()); floor(f,3); var retirement=new SnapshotGateRetirement(terminal.consume(),outer.group,released.released()); f.store.applySnapshotGateRetirement(new ApplyPosition(1,4),retirement);
            var late=f.store.applyGateInstall(new ApplyPosition(1,5),fence); assertTrue(late.retired()); assertNull(late.gate()); assertTrue(f.store.applyGateRelease(new ApplyPosition(1,6),terminal.consume()).retired()); assertEquals(0,f.store.gateCapacity().count()); assertNull(f.store.snapshotGate(fence.fenceId()));
        }
    }
    @Test void activeExpiredGateCannotCollectOrCreateNewPinsButStillReleasesAndKeepsExistingPin() throws Exception {
        try (var f=outer.new Fixture(root.resolve("active-expired"),true)) {
            prepared.enable(f); var fence=fence(f,1,1); f.store.applyGateInstall(new ApplyPosition(1,1),fence);
            try (var pin=f.store.captureGate(fence)) {
                floor(f,2); assertTrue(f.store.snapshotGateResult(fence).gate().active()); assertNull(f.store.snapshotFence(fence)); assertThrows(SnapshotRetentionExpiredException.class,() -> f.store.captureGate(fence)); assertNull(pin.get(outer.fixed,prepared.key(1)));
                var consumed=SnapshotFenceStatus.begin(fence).abort().consume(); var retirement=new SnapshotGateRetirement(consumed,outer.group,new ApplyPosition(1,3)); assertThrows(IllegalArgumentException.class,() -> f.store.estimateSnapshotGateRetirement(retirement)); assertThrows(IllegalArgumentException.class,() -> f.store.applySnapshotGateRetirement(new ApplyPosition(1,3),retirement)); assertEquals(new ApplyPosition(1,2),f.store.applied());
                assertEquals(new ApplyPosition(1,3),f.store.applyGateRelease(new ApplyPosition(1,3),consumed).gate().released()); f.store.applySnapshotGateRetirement(new ApplyPosition(1,4),retirement); assertEquals(0,f.store.gateCapacity().count()); assertNull(pin.get(outer.fixed,prepared.key(1)));
            }
        }
    }
    @Test void oldReplaysNeverTouchANewBusinessAtTheSameSnapshotUuid() throws Exception {
        try (var f=outer.new Fixture(root.resolve("uuid-reuse"),true)) {
            prepared.enable(f); var old=fence(f,1,1); f.store.applyGateInstall(new ApplyPosition(1,1),old); var consumed=SnapshotFenceStatus.begin(old).abort().consume(); f.store.applyGateRelease(new ApplyPosition(1,2),consumed); floor(f,3); var retirement=new SnapshotGateRetirement(consumed,outer.group,new ApplyPosition(1,2)); f.store.applySnapshotGateRetirement(new ApplyPosition(1,4),retirement);
            var next=rebind(old,new OperationId(new UUID(0,1203),2,0),2); var nativeNext=f.store.applyGateInstall(new ApplyPosition(1,5),next).gate();
            assertTrue(f.store.snapshotGateResult(old).retired()); assertTrue(f.store.applyGateInstall(new ApplyPosition(1,6),old).retired()); assertTrue(f.store.applyGateRelease(new ApplyPosition(1,7),consumed).retired()); f.store.applySnapshotGateRetirement(new ApplyPosition(1,8),retirement);
            assertEquals(nativeNext,f.store.snapshotGate(old.fenceId())); assertEquals(1,f.store.gateCapacity().count()); assertNull(f.store.snapshotFence(next));
            assertTrue(f.store.applyGateInstall(new ApplyPosition(1,1),old).retired()); assertTrue(f.store.applyGateRelease(new ApplyPosition(1,2),consumed).retired()); f.store.applySnapshotGateRetirement(new ApplyPosition(1,4),retirement); assertEquals(new ApplyPosition(1,8),f.store.applied());
            assertThrows(IllegalArgumentException.class,() -> f.store.applyGateInstall(new ApplyPosition(2,8),old)); var changed=new SnapshotFence(next.clusterId(),next.databaseId(),next.domainId(),next.fenceId(),next.homeGroup(),next.homeEpoch(),next.operation(),next.directoryGeneration()+1,next.schemaEpoch(),next.schemaDigest(),next.participants()); assertThrows(IllegalArgumentException.class,() -> f.store.snapshotGateResult(changed));
            assertEquals(nativeNext,f.store.snapshotGate(old.fenceId()));
        }
    }
    @Test void sixteenReleasedFamiliesReclaimCapacityAndImageRetainsOnlyTheNewGate() throws Exception {
        var image=root.resolve("capacity.image"); SnapshotFence fresh,first; long last;
        try (var f=outer.new Fixture(root.resolve("capacity"),true)) {
            prepared.enable(f); var fences=new ArrayList<SnapshotFence>(); var retirements=new ArrayList<SnapshotGateRetirement>();
            for (int n=0;n<16;n++) { var fence=fence(f,n,n+1); fences.add(fence); f.store.applyGateInstall(new ApplyPosition(1,n+1),fence); }
            for (int n=0;n<16;n++) { var consumed=SnapshotFenceStatus.begin(fences.get(n)).abort().consume(); var release=f.store.applyGateRelease(new ApplyPosition(1,n+17),consumed).gate(); retirements.add(new SnapshotGateRetirement(consumed,outer.group,release.released())); }
            first=fences.getFirst(); assertEquals(16,f.store.gateCapacity().count()); floor(f,33); long bytes=f.store.gateCapacity().bytes(); f.store.applySnapshotGateRetirement(new ApplyPosition(1,34),retirements.getFirst()); assertEquals(15,f.store.gateCapacity().count()); assertTrue(f.store.gateCapacity().bytes()<bytes);
            fresh=rebind(fence(f,99,17),new OperationId(new UUID(0,1204),2,0),17); f.store.applyGateInstall(new ApplyPosition(1,35),fresh); assertEquals(16,f.store.gateCapacity().count());
            int index=35; for (int n=1;n<16;n++) f.store.applySnapshotGateRetirement(new ApplyPosition(1,++index),retirements.get(n)); f.store.applyGateRelease(new ApplyPosition(1,++index),SnapshotFenceStatus.begin(fresh).abort().consume()); last=index; assertEquals(1,f.store.gateCapacity().count());
            try (var pin=f.store.capture()) { NativeRangeImage.write(image,pin,f.store.ownership().interval(),helper.limits); }
        }
        try (var f=outer.new Fixture(root.resolve("capacity-target"),true)) {
            f.store.fence(); f.store.installSnapshot(image,helper.limits); f.store.activate(); assertEquals(1,f.store.gateCapacity().count()); assertNotNull(f.store.snapshotGate(fresh.fenceId()).released()); assertTrue(f.store.snapshotGateResult(first).retired()); assertTrue(f.store.applyGateInstall(new ApplyPosition(2,last+1),first).retired()); assertEquals(1,f.store.gateCapacity().count());
        }
    }
    @Test void businessAnchorCollisionsAndWrongTermCutsAreRejectedByNativeAndImage() throws Exception {
        try (var f=outer.new Fixture(root.resolve("anchor-collision"),true)) {
            prepared.enable(f); var fence=fence(f,1,1); var user=new DistributedCommand(outer.group,1,f.schema.digest(),fence.operation(),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,prepared.key(2),prepared.key(2)))); f.store.apply(new ApplyPosition(1,1),user);
            assertThrows(IllegalArgumentException.class,() -> f.store.estimateGateInstall(fence)); assertThrows(IllegalArgumentException.class,() -> f.store.applyGateInstall(new ApplyPosition(1,2),fence));
        }
        try (var f=outer.new Fixture(root.resolve("wrong-cut"),true)) {
            prepared.enable(f); var fence=fence(f,1,1); f.store.applyGateInstall(new ApplyPosition(1,1),fence); var terminal=SnapshotFenceStatus.begin(fence).abort(); var released=f.store.applyGateRelease(new ApplyPosition(2,2),terminal).gate();
            var dbField=ReplicatedRangeStore.class.getDeclaredField("db"); var stateField=ReplicatedRangeStore.class.getDeclaredField("state"); dbField.setAccessible(true); stateField.setAccessible(true);
            byte[] key=java.nio.ByteBuffer.allocate(33).putLong(outer.group.getMostSignificantBits()).putLong(outer.group.getLeastSignificantBits()).put((byte)0x24).putLong(fence.fenceId().getMostSignificantBits()).putLong(fence.fenceId().getLeastSignificantBits()).array();
            var wrong=new SnapshotGate(fence,outer.group,released.installed(),new ApplyPosition(1,2),released.decisionDigest()); ((org.rocksdb.RocksDB)dbField.get(f.store)).put((org.rocksdb.ColumnFamilyHandle)stateField.get(f.store),key,wrong.encode());
            assertThrows(IllegalArgumentException.class,() -> f.store.snapshotGateResult(fence)); assertThrows(IllegalArgumentException.class,() -> f.store.estimateSnapshotGateRetirement(new SnapshotGateRetirement(terminal.consume(),outer.group,released.released())));
            try (var pin=f.store.capture()) { assertThrows(java.io.IOException.class,() -> NativeRangeImage.write(root.resolve("wrong-cut.image"),pin,f.store.ownership().interval(),helper.limits)); }
            f.store.fence(); assertThrows(IllegalStateException.class,f.store::activate);
        }
    }
}
