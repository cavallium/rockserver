package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ParticipantFinalizationTest {
    final PreparedParticipantTest fixture=new PreparedParticipantTest();
    final OperationId recovery=new OperationId(new UUID(0,90),2,0);
    ParticipantFinalization finalize(TransactionStatus decision) { return new ParticipantFinalization(fixture.home,1,fixture.group,recovery,decision); }
    TransactionStatus commit() { return fixture.preparation().registration().prepared(fixture.group,PreparedParticipant.Preparation.planDigest(fixture.accepted()),new ApplyPosition(1,1)).decide(TransactionStatus.Phase.COMMIT); }
    @Test void terminalDecisionIdentitySurvivesAcknowledgementsConsumptionAndRecoveryIdentityChanges() {
        var commit=commit(); var original=finalize(commit); var consumed=finalize(commit.finalized(fixture.group).consume());
        assertEquals(original,consumed); assertEquals(original.hashCode(),consumed.hashCode()); assertEquals(original,ParticipantFinalization.decode(consumed.encode()));
        assertEquals(0,consumed.decision().generation()); assertFalse(consumed.decision().consumed()); assertFalse(consumed.participant().finalized());
        var retried=new ParticipantFinalization(fixture.home,1,fixture.group,new OperationId(recovery.sessionId(),3,1),commit);
        assertNotEquals(original,retried); assertArrayEquals(original.decisionDigest(),retried.decisionDigest());
        byte[] bytes=original.encode(); var decoded=ParticipantFinalization.decode(bytes); Arrays.fill(bytes,(byte)0); assertEquals(original,decoded);
        byte[] digest=original.decisionDigest(); Arrays.fill(digest,(byte)0); assertArrayEquals(original.decisionDigest(),decoded.decisionDigest());
    }
    @Test void commitRequiresExactNativeVoteAndEveryFrozenDefinition() {
        var prepared=new PreparedParticipant(fixture.preparation(),new ApplyPosition(1,1)); var intent=finalize(commit()); intent.requirePrepared(prepared);
        assertThrows(IllegalArgumentException.class,() -> intent.requirePrepared(new PreparedParticipant(fixture.preparation(),new ApplyPosition(1,2))));
        var wrongHome=new ParticipantFinalization(new UUID(0,80),1,fixture.group,recovery,commit()); assertThrows(IllegalArgumentException.class,() -> wrongHome.requirePrepared(prepared));
        var wrongEpoch=new ParticipantFinalization(fixture.home,2,fixture.group,recovery,commit()); assertThrows(IllegalArgumentException.class,() -> wrongEpoch.requirePrepared(prepared));
        var registration=fixture.preparation().registration();
        var changed=TransactionStatus.begin(registration.clusterId(), registration.databaseId(), registration.transactionId(), registration.operation(), List.of(new DomainBinding(registration.domains().getFirst().domainId(), 0, new byte[32], 2, registration.domains().getFirst().schemaDigest())), registration.planDigest(), registration.participants())
                .prepared(fixture.group,PreparedParticipant.Preparation.planDigest(fixture.accepted()),new ApplyPosition(1,1)).decide(TransactionStatus.Phase.COMMIT);
        assertThrows(IllegalArgumentException.class,() -> finalize(changed).requirePrepared(prepared));
        assertFalse(Arrays.equals(intent.decisionDigest(),finalize(registration.decide(TransactionStatus.Phase.ABORT)).decisionDigest()));
    }
    @Test void abortWithoutVoteMatchesAnUnreportedPrepareButCannotChangeKnownEvidence() {
        var registration=fixture.preparation().registration(); var prepared=new PreparedParticipant(fixture.preparation(),new ApplyPosition(1,1));
        var abort=finalize(registration.decide(TransactionStatus.Phase.ABORT)); abort.requirePreparation(fixture.preparation()); abort.requirePrepared(prepared);
        assertNull(abort.participant().prepared());
        var voted=registration.prepared(fixture.group,PreparedParticipant.Preparation.planDigest(fixture.accepted()),new ApplyPosition(1,2)).decide(TransactionStatus.Phase.ABORT);
        assertThrows(IllegalArgumentException.class,() -> finalize(voted).requirePrepared(prepared));
        assertThrows(IllegalArgumentException.class,() -> finalize(registration));
        assertThrows(IllegalArgumentException.class,() -> new ParticipantFinalization(fixture.home,1,fixture.group,registration.operation(),voted));
        var reused=new ParticipantFinalization(fixture.home,1,fixture.group,fixture.prepareId,voted);
        assertThrows(IllegalArgumentException.class,() -> reused.requirePreparation(fixture.preparation()));
        assertThrows(IllegalArgumentException.class,() -> new ParticipantFinalization(fixture.home,1,new UUID(0,81),recovery,voted));
    }
    @Test void decoderRejectsTruncationLengthsAndMutableProgressInSubmittedBytes() {
        byte[] valid=finalize(commit()).encode(); assertEquals(ParticipantFinalization.HEADER_BYTES+commit().encode().length,valid.length);
        for (int n=0;n<valid.length;n++) { byte[] truncated=Arrays.copyOf(valid,n); assertThrows(IllegalArgumentException.class,() -> ParticipantFinalization.decode(truncated)); }
        byte[] length=valid.clone(); ByteBuffer.wrap(length).putInt(72,-1); assertThrows(IllegalArgumentException.class,() -> ParticipantFinalization.decode(length));
        assertThrows(IllegalArgumentException.class,() -> ParticipantFinalization.decode(Arrays.copyOf(valid,valid.length+1)));
        int generationOffset=4*16+2*Long.BYTES+32; // Four UUIDs, operation epoch/sequence, plan digest.
        byte[] generation=valid.clone(); ByteBuffer.wrap(generation).putLong(ParticipantFinalization.HEADER_BYTES+generationOffset,1);
        assertThrows(IllegalArgumentException.class,() -> ParticipantFinalization.decode(generation));
        byte[] ack=valid.clone(); ack[ack.length-1]=1;
        assertThrows(IllegalArgumentException.class,() -> ParticipantFinalization.decode(ack));
    }
}
