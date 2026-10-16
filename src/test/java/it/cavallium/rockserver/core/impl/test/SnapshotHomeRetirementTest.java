package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SnapshotHomeRetirementTest {
    final UUID home=new UUID(0,1),column=new UUID(0,2),left=new UUID(0,3),right=new UUID(-1,4);
    SnapshotFence fence(long sequence) {
        var scope=new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{1})));
        return new SnapshotFence(new UUID(0,5), new UUID(0,6), new UUID(0,8), home, 1, new OperationId(new UUID(0,9),1,sequence), List.of(new DomainBinding(new UUID(0,7), 0, new byte[32], 1, new byte[32])), List.of(new SnapshotFence.Participant(new UUID(0,7), right, right, 1, new byte[32], new OperationId(new UUID(0,10),1,2), new OperationId(new UUID(0,10),1,3), scope),
                        new SnapshotFence.Participant(new UUID(0,7), left, left, 1, new byte[32], new OperationId(new UUID(0,10),1,0), new OperationId(new UUID(0,10),1,1), scope)));
    }
    SnapshotHomeIdentity identity(SnapshotFence fence) { return new SnapshotHomeIdentity(home,1,new byte[32],column,fence); }
    SnapshotFenceStatus consumed(SnapshotFence fence) {
        return SnapshotFenceStatus.begin(fence).ready(left,new ApplyPosition(1,3)).ready(right,new ApplyPosition(1,4))
                .capture(left,new SnapshotFenceStatus.Capture(new UUID(0,11),new UUID(0,12),new ApplyPosition(2,5)))
                .capture(right,new SnapshotFenceStatus.Capture(new UUID(0,13),new UUID(0,14),new ApplyPosition(2,6))).complete().consume();
    }
    SnapshotFenceProof proof(SnapshotFence fence,UUID group,long term,long index) { return new SnapshotFenceProof(fence,group,2,new ApplyPosition(term,index)); }
    List<SnapshotFenceProof> proofs(SnapshotFence fence) { return List.of(proof(fence,right,3,9),proof(fence,left,3,8)); }

    @Test void compactRetirementStoresOneStatusAndFortyBytesPerCanonicalDataGroup() {
        var fence=fence(0); var consumed=consumed(fence); var identity=identity(fence);
        var equivalent=SnapshotFence.decode(fence.encode()); assertNotSame(fence,equivalent);
        var supplied=new ArrayList<>(proofs(equivalent)); var retirement=new SnapshotHomeRetirement(identity,consumed,supplied); supplied.clear();
        assertEquals(80+consumed.encode().length+40*2,retirement.encode().length);
        assertEquals(List.of(left,right),retirement.participants().stream().map(SnapshotFenceProof::participantGroup).toList());
        assertSame(fence,retirement.identity().fence()); assertSame(fence,retirement.consumed().fence());
        for (var proof:retirement.participants()) assertSame(fence,proof.fence());
        assertThrows(UnsupportedOperationException.class,() -> retirement.participants().clear());
        var copy=SnapshotHomeRetirement.decode(retirement.encode()); assertEquals(retirement,copy); assertArrayEquals(retirement.encode(),copy.encode()); assertArrayEquals(retirement.digest(),copy.digest());
        assertSame(copy.identity().fence(),copy.consumed().fence());
        for (var proof:copy.participants()) assertSame(copy.identity().fence(),proof.fence());
    }

    @Test void consumptionAndEveryExactDataFenceAreRequired() {
        var fence=fence(0); var identity=identity(fence); var consumed=consumed(fence);
        assertThrows(IllegalArgumentException.class,() -> new SnapshotHomeRetirement(identity,consumed,List.of()));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotHomeRetirement(identity,consumed,List.of(proof(fence,left,3,8))));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotHomeRetirement(identity,consumed,List.of(proof(fence,left,3,8),proof(fence,left,3,9))));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotHomeRetirement(identity,consumed,List.of(proof(fence,left,3,8),proof(fence(1),right,3,9))));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotHomeRetirement(identity(fence(1)),consumed,proofs(fence)));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotHomeRetirement(identity,consumed.terminalDecision(),proofs(fence)));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotHomeRetirement(identity,SnapshotFenceStatus.begin(fence),proofs(fence)));
        assertThrows(IllegalArgumentException.class,() -> proof(fence,new UUID(0,99),3,9));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotFenceProof(fence,left,1,new ApplyPosition(3,8)));
        var abort=SnapshotFenceStatus.begin(fence).abort().consume();
        assertEquals(abort,new SnapshotHomeRetirement(identity,abort,proofs(fence)).consumed());
    }

    @Test void dataCutsMustStrictlyFollowRecordedReadyOrCapturePositions() {
        var fence=fence(0); var identity=identity(fence); var consumed=consumed(fence);
        assertThrows(IllegalArgumentException.class,() -> new SnapshotHomeRetirement(identity,consumed,List.of(proof(fence,left,2,5),proof(fence,right,3,9))));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotHomeRetirement(identity,consumed,List.of(proof(fence,left,3,4),proof(fence,right,3,9))));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotHomeRetirement(identity,consumed,List.of(proof(fence,left,1,8),proof(fence,right,3,9))));
        var abortedReady=SnapshotFenceStatus.begin(fence).ready(left,new ApplyPosition(2,3)).abort().consume();
        assertThrows(IllegalArgumentException.class,() -> new SnapshotHomeRetirement(identity,abortedReady,List.of(proof(fence,left,2,3),proof(fence,right,3,9))));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotHomeRetirement(identity,abortedReady,List.of(proof(fence,left,1,8),proof(fence,right,3,9))));
        assertEquals(abortedReady,new SnapshotHomeRetirement(identity,abortedReady,proofs(fence)).consumed());
    }

    @Test void retirementCodecRejectsNoncanonicalOrIncompleteProofSets() {
        var fence=fence(0); var consumed=consumed(fence); var bytes=new SnapshotHomeRetirement(identity(fence),consumed,proofs(fence)).encode();
        int first=80+consumed.encode().length;
        byte[] reversed=bytes.clone(); System.arraycopy(bytes,first+40,reversed,first,40); System.arraycopy(bytes,first,reversed,first+40,40);
        assertThrows(IllegalArgumentException.class,() -> SnapshotHomeRetirement.decode(reversed));
        byte[] duplicate=bytes.clone(); System.arraycopy(bytes,first,duplicate,first+40,40);
        assertThrows(IllegalArgumentException.class,() -> SnapshotHomeRetirement.decode(duplicate));
        byte[] wrongCount=bytes.clone(); ByteBuffer.wrap(wrongCount).putInt(first-4,1);
        assertThrows(IllegalArgumentException.class,() -> SnapshotHomeRetirement.decode(wrongCount));
        byte[] invalidFloor=bytes.clone(); ByteBuffer.wrap(invalidFloor).putLong(first+16,1);
        assertThrows(IllegalArgumentException.class,() -> SnapshotHomeRetirement.decode(invalidFloor));
        assertThrows(IllegalArgumentException.class,() -> SnapshotHomeRetirement.decode(Arrays.copyOf(bytes,bytes.length+1)));
        for (int n=0;n<bytes.length;n++) { byte[] truncated=Arrays.copyOf(bytes,n); assertThrows(IllegalArgumentException.class,() -> SnapshotHomeRetirement.decode(truncated)); }
    }

    @Test void homeIdentityOwnsPlacementValuesAndHasACanonicalCodec() {
        var fence=fence(0); byte[] schema=new byte[32]; var identity=new SnapshotHomeIdentity(home,1,schema,column,fence); schema[0]=1;
        assertEquals(0,identity.schemaDigest()[0]); identity.schemaDigest()[0]=2; assertEquals(0,identity.schemaDigest()[0]);
        assertSame(fence,identity.fence()); assertEquals(fence.operation(),identity.business()); assertEquals(fence.fenceId(),identity.snapshotId());
        assertEquals(76+fence.encode().length,identity.encode().length); var copy=SnapshotHomeIdentity.decode(identity.encode());
        assertEquals(identity,copy); assertEquals(identity.hashCode(),copy.hashCode()); assertArrayEquals(identity.digest(),copy.digest());
        assertThrows(IllegalArgumentException.class,() -> new SnapshotHomeIdentity(new UUID(0,99),1,new byte[32],column,fence));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotHomeIdentity(home,2,new byte[32],column,fence));
        byte[] badLength=identity.encode(); ByteBuffer.wrap(badLength).putInt(72,0); assertThrows(IllegalArgumentException.class,() -> SnapshotHomeIdentity.decode(badLength));
        assertThrows(IllegalArgumentException.class,() -> SnapshotHomeIdentity.decode(Arrays.copyOf(identity.encode(),identity.encode().length+1)));
    }

    @Test void homeProofCoverageRequiresMonotoneFloorAndCutForTheSamePlacementAndBusiness() {
        var identity=identity(fence(0)); var proof=new SnapshotHomeFenceProof(identity,2,new ApplyPosition(2,7));
        assertEquals(proof,SnapshotHomeFenceProof.decode(proof.encode())); assertEquals(28+identity.encode().length,proof.encode().length);
        var advanced=new SnapshotHomeFenceProof(identity,3,new ApplyPosition(3,8)); assertTrue(proof.covers(proof)); assertTrue(advanced.covers(proof)); assertFalse(proof.covers(advanced));
        assertFalse(new SnapshotHomeFenceProof(identity,2,new ApplyPosition(3,8)).covers(advanced));
        assertFalse(new SnapshotHomeFenceProof(identity,3,new ApplyPosition(3,7)).covers(proof));
        assertFalse(new SnapshotHomeFenceProof(identity,3,new ApplyPosition(3,6)).covers(proof));
        assertFalse(new SnapshotHomeFenceProof(identity,3,new ApplyPosition(1,8)).covers(proof));
        var otherColumn=new SnapshotHomeIdentity(home,1,new byte[32],new UUID(0,99),identity.fence());
        assertFalse(new SnapshotHomeFenceProof(otherColumn,3,new ApplyPosition(3,8)).covers(proof));
        assertFalse(new SnapshotHomeFenceProof(identity(fence(1)),3,new ApplyPosition(3,8)).covers(proof)); assertFalse(proof.covers(null));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotHomeFenceProof(identity,1,new ApplyPosition(2,7)));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotHomeFenceProof(identity,2,new ApplyPosition(0,7)));
        byte[] badLength=proof.encode(); ByteBuffer.wrap(badLength).putInt(0,0); assertThrows(IllegalArgumentException.class,() -> SnapshotHomeFenceProof.decode(badLength));
    }

    @Test void expiredAbsenceCarriesTheDefinitionButNoDecisionConsumptionOrOriginalReceipt() {
        var fence=fence(0); var before=SnapshotFenceStatus.begin(fence); var proof=new SnapshotHomeFenceProof(identity(fence),2,new ApplyPosition(1,0));
        var complete=consumed(fence); var abort=before.abort().consume();
        assertEquals(proof,new SnapshotHomeFenceProof(identity(complete.fence()),2,new ApplyPosition(1,0)));
        assertEquals(proof,new SnapshotHomeFenceProof(identity(abort.fence()),2,new ApplyPosition(1,0)));
        var definitionOnly=SnapshotFenceStatus.begin(SnapshotHomeFenceProof.decode(proof.encode()).identity().fence());
        assertEquals(before,definitionOnly); assertEquals(SnapshotFenceStatus.Phase.INSTALLING,definitionOnly.phase());
        assertFalse(definitionOnly.terminal()); assertFalse(definitionOnly.consumed()); assertEquals(0,proof.position().index());
        assertThrows(IllegalArgumentException.class,definitionOnly::terminalDecision);
    }
}
