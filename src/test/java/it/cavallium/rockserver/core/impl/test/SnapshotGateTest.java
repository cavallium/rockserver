package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SnapshotGateTest {
    final SnapshotFenceStatusTest helper=new SnapshotFenceStatusTest();
    SnapshotGate gate() { return SnapshotGate.install(helper.helper.fence(),helper.helper.a,helper.aReady); }
    @Test void completeReleaseRequiresExactReadyAndCaptureBeforeRelease() {
        var gate=gate(); assertTrue(gate.active()); assertNull(gate.released()); assertNull(gate.decisionDigest());
        assertTrue(gate.ownsOperation(helper.helper.operation(0))); assertTrue(gate.ownsOperation(helper.helper.operation(1))); assertFalse(gate.ownsOperation(helper.helper.operation(2)));
        var complete=helper.captured().complete(); var position=new ApplyPosition(2,6); var released=gate.release(complete,position);
        assertFalse(released.active()); assertEquals(helper.aReady,released.installed()); assertEquals(position,released.released()); assertArrayEquals(complete.digest(),released.decisionDigest());
        assertSame(released,released.release(complete,new ApplyPosition(3,7)));
        assertThrows(IllegalArgumentException.class,() -> gate.release(helper.ready(),position));
        assertThrows(IllegalArgumentException.class,() -> gate.release(complete,new ApplyPosition(1,3)));
        assertThrows(IllegalArgumentException.class,() -> SnapshotGate.install(complete.fence(),helper.helper.a,new ApplyPosition(1,1)).release(complete,position));
        assertThrows(IllegalArgumentException.class,() -> released.release(helper.captured().abort(),new ApplyPosition(3,7)));
        assertThrows(IllegalArgumentException.class,() -> SnapshotGate.released(complete,helper.helper.a,position));
    }
    @Test void abortCanFenceLateInstallButCannotContradictAnObservedReadyProof() {
        var initial=SnapshotFenceStatus.begin(helper.helper.fence()); var abort=initial.abort();
        var fence=SnapshotGate.released(abort,helper.helper.a,new ApplyPosition(1,1)); assertNull(fence.installed()); assertFalse(fence.active()); assertArrayEquals(abort.digest(),fence.decisionDigest());
        assertSame(fence,fence.release(abort,new ApplyPosition(2,3)));
        assertThrows(IllegalArgumentException.class,() -> SnapshotGate.released(helper.ready().abort(),helper.helper.a,new ApplyPosition(2,6)));
        assertThrows(IllegalArgumentException.class,() -> fence.release(helper.ready().abort(),new ApplyPosition(2,6)));
        var installed=gate(); var released=installed.release(initial.abort(),new ApplyPosition(1,3)); assertFalse(released.active());
        assertThrows(IllegalArgumentException.class,() -> installed.release(helper.ready().abort(),new ApplyPosition(1,2)));
        var changedReady=initial.ready(helper.helper.a,new ApplyPosition(1,1)).abort();
        assertThrows(IllegalArgumentException.class,() -> installed.release(changedReady,new ApplyPosition(1,3)));
        assertTrue(installed.active(),"Gate ownership persists until a matching terminal decision releases it");
    }
    @Test void gateCodecOwnsDigestAndRejectsMalformedOrUnboundStates() {
        var release=gate().release(helper.captured().complete(),new ApplyPosition(2,6)); byte[] digest=release.decisionDigest();
        var owned=new SnapshotGate(release.fence(),release.participantGroup(),release.installed(),release.released(),digest); Arrays.fill(digest,(byte)0); Arrays.fill(owned.decisionDigest(),(byte)1); assertEquals(release,owned); assertEquals(release.hashCode(),owned.hashCode());
        var before=SnapshotGate.released(SnapshotFenceStatus.begin(helper.helper.fence()).abort(),helper.helper.a,new ApplyPosition(1,1));
        for (var gate:List.of(gate(),release,before)) {
            assertTrue(gate.encode().length<=SnapshotGate.MAX_BYTES); byte[] bytes=gate.encode(); var decoded=SnapshotGate.decode(bytes); Arrays.fill(bytes,(byte)0); assertEquals(gate,decoded);
        }
        byte[] bytes=release.encode();
        for (int n=0;n<bytes.length;n++) { byte[] truncated=Arrays.copyOf(bytes,n); assertThrows(IllegalArgumentException.class,() -> SnapshotGate.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> SnapshotGate.decode(Arrays.copyOf(bytes,bytes.length+1)));
        byte[] flags=bytes.clone(); flags[4+release.fence().encode().length+16]=0; assertThrows(IllegalArgumentException.class,() -> SnapshotGate.decode(flags));
        byte[] length=bytes.clone(); ByteBuffer.wrap(length).putInt(0,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,() -> SnapshotGate.decode(length));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotGate(release.fence(),release.participantGroup(),null,null,null));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotGate(release.fence(),release.participantGroup(),release.installed(),release.released(),null));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotGate(release.fence(),release.participantGroup(),release.installed(),null,new byte[32]));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotGate(release.fence(),release.participantGroup(),release.installed(),release.installed(),new byte[32]));
        assertThrows(IllegalArgumentException.class,() -> SnapshotGate.install(release.fence(),UUID.randomUUID(),new ApplyPosition(1,1)));
    }
}
