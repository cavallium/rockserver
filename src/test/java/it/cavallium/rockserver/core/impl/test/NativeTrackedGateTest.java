package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class NativeTrackedGateTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeSnapshotGateTest helper=new NativeSnapshotGateTest();
    final NativePreparedParticipantTest prepared=helper.helper;
    final NativeBucketApplyTest outer=helper.outer;
    @Test void gateSnapshotAndHistoryRegisterAtSameCutThenRetainOriginalValuesAcrossReleaseAndAba() throws Exception {
        try (var f=outer.new Fixture(root.resolve("tracked-gate"),true)) {
            prepared.enable(f); f.apply(1,prepared.put(f,0,1,7)); var fence=helper.fence(f,1,1); f.store.applyGateInstall(new ApplyPosition(1,2),fence);
            try (var tracked=f.store.captureTrackedGate(fence); var readonly=f.store.captureGate(fence)) {
                assertEquals(new ApplyPosition(1,2),tracked.position()); assertEquals(readonly.position(),tracked.position()); assertEquals(1,f.store.activeTrackedSnapshotPins()); assertTrue(f.store.trackedSnapshotScopeBytes()>0); assertArrayEquals(prepared.key(7),tracked.get(outer.fixed,prepared.key(1)));
                assertThrows(IllegalStateException.class,() -> f.store.planReadCertification(tracked,new OperationId(outer.session,1,10),0,new UUID(0,99)));
                f.store.applyGateRelease(new ApplyPosition(1,3),SnapshotFenceStatus.begin(fence).abort()); f.apply(4,prepared.put(f,1,1,8)); f.apply(5,prepared.put(f,2,1,7));
                assertArrayEquals(prepared.key(7),readonly.get(outer.fixed,prepared.key(1))); assertArrayEquals(prepared.key(7),tracked.get(outer.fixed,prepared.key(1)));
                assertThrows(IllegalStateException.class,() -> f.store.planReadCertification(tracked,new OperationId(outer.session,1,10),0,new UUID(0,99))); assertEquals(RangeMutationHistory.Status.CONFLICT,tracked.status());
            }
            assertEquals(0,f.store.activeTrackedSnapshotPins()); assertEquals(0,f.store.trackedSnapshotScopeBytes()); assertEquals(0,f.store.activeSnapshotPins());
        }
    }
    @Test void gateTrackedCaptureHonorsCapacityAndBusinessFloorWithoutChangingReadonlyCaptureOwnership() throws Exception {
        try (var f=outer.new Fixture(root.resolve("tracked-capacity"),true)) {
            f.store.initializeConfiguration(new byte[]{1}); f.store.enableScopedHistory(new RangeMutationHistory.Limits(1,1024,1024*1024,1024*1024)); f.store.applyNoop(new ApplyPosition(1,0));
            var fence=helper.fence(f,1,1); f.store.applyGateInstall(new ApplyPosition(1,1),fence);
            try (var tracked=f.store.captureTrackedGate(fence); var readonly=f.store.captureGate(fence)) {
                assertThrows(IllegalStateException.class,() -> f.store.captureTrackedGate(fence)); assertEquals(1,f.store.activeTrackedSnapshotPins()); assertEquals(2,f.store.activeSnapshotPins());
                f.apply(2,DistributedCommand.advanceSessionEpochFloor(outer.group,1,f.schema.digest(),new OperationId(new UUID(0,77),2,0),2)); assertThrows(SnapshotRetentionExpiredException.class,() -> f.store.captureTrackedGate(fence)); assertNull(readonly.get(outer.fixed,prepared.key(1)));
                f.store.applyGateRelease(new ApplyPosition(1,3),SnapshotFenceStatus.begin(fence).abort()); assertNull(tracked.get(outer.fixed,prepared.key(1)));
            }
            assertEquals(0,f.store.activeSnapshotPins()); assertEquals(0,f.store.activeTrackedSnapshotPins());
        }
    }
}
