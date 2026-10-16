package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SnapshotGateRetirementTest {
    final SnapshotFenceStatusTest helper=new SnapshotFenceStatusTest();
    @Test void consumptionIsOneTerminalStepAndNeverChangesTheReleaseDecisionDigest() {
        var complete=helper.captured().complete(); var consumed=complete.consume(); consumed.requireSuccessorOf(complete);
        assertEquals(complete.generation()+1,consumed.generation()); assertTrue(consumed.consumed()); assertSame(consumed,consumed.consume()); assertEquals(complete,consumed.terminalDecision());
        var gate=SnapshotGate.install(complete.fence(),helper.helper.a,helper.aReady); var released=gate.release(complete,new ApplyPosition(2,6));
        assertArrayEquals(released.decisionDigest(),consumed.terminalDecision().digest()); assertSame(released,released.release(consumed,new ApplyPosition(3,7))); assertTrue(released.ownsOperation(complete.fence().operation()));
        assertThrows(IllegalArgumentException.class,() -> SnapshotFenceStatus.begin(complete.fence()).consume()); assertThrows(IllegalArgumentException.class,() -> complete.requireSuccessorOf(consumed));
        var abort=SnapshotFenceStatus.begin(complete.fence()).abort(); assertArrayEquals(SnapshotGate.released(abort,helper.helper.a,new ApplyPosition(1,1)).decisionDigest(),SnapshotGate.released(abort.consume(),helper.helper.a,new ApplyPosition(1,1)).decisionDigest());
    }
    @Test void retirementCodecRequiresConsumptionAndRealOrderedRelease() {
        var consumed=helper.captured().complete().consume(); var request=new SnapshotGateRetirement(consumed,helper.helper.a,new ApplyPosition(2,6));
        assertTrue(request.encode().length<=SnapshotGateRetirement.MAX_BYTES); byte[] bytes=request.encode(); var decoded=SnapshotGateRetirement.decode(bytes); Arrays.fill(bytes,(byte)0); assertEquals(request,decoded); assertArrayEquals(request.digest(),decoded.digest());
        assertThrows(IllegalArgumentException.class,() -> new SnapshotGateRetirement(consumed.terminalDecision(),helper.helper.a,new ApplyPosition(2,6)));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotGateRetirement(consumed,helper.helper.a,new ApplyPosition(2,3)));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotGateRetirement(consumed,helper.helper.a,new ApplyPosition(0,6)));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotGateRetirement(consumed,UUID.randomUUID(),new ApplyPosition(2,6)));
        assertThrows(IllegalArgumentException.class,() -> SnapshotGateRetirement.decode(Arrays.copyOf(request.encode(),request.encode().length-1)));
        assertThrows(IllegalArgumentException.class,() -> SnapshotGateRetirement.decode(Arrays.copyOf(request.encode(),request.encode().length+1)));
    }
    @Test void floorProofIsDistinctFromAReceiptAndRequiresMonotoneExactTermCuts() {
        var fence=helper.helper.fence(); var proof=new SnapshotFenceProof(fence,helper.helper.a,2,new ApplyPosition(2,6));
        assertEquals(proof,SnapshotFenceProof.decode(proof.encode())); assertTrue(new SnapshotFenceProof(fence,helper.helper.a,3,new ApplyPosition(3,7)).covers(proof)); assertFalse(new SnapshotFenceProof(fence,helper.helper.a,2,new ApplyPosition(3,6)).covers(proof));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotFenceProof(fence,helper.helper.a,1,new ApplyPosition(2,6)));
        var retired=SnapshotGateResult.retired(proof); assertTrue(retired.retired()); assertNull(retired.gate());
        var retained=SnapshotGateResult.retained(SnapshotGate.install(fence,helper.helper.a,helper.aReady)); assertFalse(retained.retired()); assertNull(retained.retirement());
        assertThrows(IllegalArgumentException.class,() -> new SnapshotGateResult(null,null)); assertThrows(IllegalArgumentException.class,() -> new SnapshotGateResult(retained.gate(),proof));
        assertThrows(IllegalArgumentException.class,() -> helper.ready().capture(helper.helper.a,helper.capture(1,new ApplyPosition(2,2))));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotFence(fence.clusterId(), fence.databaseId(), fence.fenceId(), fence.homeGroup(), fence.homeEpoch(), fence.participant(helper.helper.a).installOperation(), fence.domains(), fence.participants()));
    }
}
