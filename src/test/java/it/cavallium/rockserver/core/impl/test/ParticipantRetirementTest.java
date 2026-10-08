package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;

import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ParticipantRetirementTest {
    final UUID home=new UUID(0,1),data=new UUID(0,2),transaction=new UUID(0,3),column=new UUID(0,4);
    TransactionStatus initial() {
        return TransactionStatus.begin(new UUID(0,5),new UUID(0,6),new UUID(0,7),transaction,
                new OperationId(new UUID(0,8),1,0),1,new byte[32],new byte[32],
                List.of(new TransactionStatus.Participant(data,data,1,new byte[32],new byte[32],null,false)));
    }
    ParticipantIdentity identity(TransactionStatus status) { return new ParticipantIdentity(home,1,data,status); }
    ParticipantFenceProof fence(TransactionStatus status,long floor,long term,long index) {
        return new ParticipantFenceProof(identity(status),floor,new ApplyPosition(term,index));
    }
    ParticipantFinalization finalization(TransactionStatus status) {
        return new ParticipantFinalization(home,1,data,new OperationId(new UUID(0,9),0,0),status);
    }
    DistributedCommand command(TransactionStatus before,TransactionStatus next) {
        byte[] key=ByteBuffer.allocate(16).putLong(transaction.getMostSignificantBits()).putLong(transaction.getLeastSignificantBits()).array();
        return new DistributedCommand(home,1,new byte[32],new OperationId(new UUID(0,10),0,0),
                List.of(new Mutation(Mutation.Kind.PUT,column,key,next.encode())),List.of(new KeyPredicate(column,key,before.encode())));
    }

    @Test void identityCanonicalizesAllHomeProgressToItsInitialDefinition() {
        var initial=initial(); var ready=initial.prepared(data,new byte[32],new ApplyPosition(1,1));
        var consumed=ready.decide(TransactionStatus.Phase.COMMIT).finalized(data).consume();
        for (var status:List.of(initial,ready,ready.decide(TransactionStatus.Phase.ABORT),consumed)) {
            var identity=identity(status); assertEquals(identity(initial),identity); assertEquals(initial,identity.definition());
            assertEquals(initial.operation(),identity.business()); assertEquals(transaction,identity.transactionId());
            assertEquals(initial.participants().getFirst(),identity.participant());
            assertEquals(identity,ParticipantIdentity.decode(identity.encode())); assertArrayEquals(identity(initial).digest(),identity.digest());
        }
        byte[] noncanonical=identity(initial).encode(); System.arraycopy(consumed.encode(),0,noncanonical,44,consumed.encode().length);
        assertThrows(IllegalArgumentException.class,() -> ParticipantIdentity.decode(noncanonical));
    }

    @Test void fenceCoverageRequiresMonotoneFloorAndCutForTheExactOriginalIdentity() {
        var original=fence(initial(),2,2,5); var advanced=fence(initial(),3,3,6);
        assertEquals(original,ParticipantFenceProof.decode(original.encode())); assertTrue(original.covers(original)); assertTrue(advanced.covers(original));
        assertFalse(original.covers(advanced)); assertFalse(fence(initial(),2,3,6).covers(advanced));
        assertFalse(fence(initial(),3,3,5).covers(original)); assertFalse(fence(initial(),3,3,4).covers(original)); assertFalse(fence(initial(),3,1,6).covers(original));
        var other=new ParticipantFenceProof(new ParticipantIdentity(new UUID(0,11),1,data,initial()),3,new ApplyPosition(3,6));
        assertFalse(other.covers(original)); assertFalse(original.covers(null));
        assertThrows(IllegalArgumentException.class,() -> fence(initial(),1,2,5));
        assertThrows(IllegalArgumentException.class,() -> fence(initial(),2,0,5));
        assertEquals(new ApplyPosition(1,0),fence(initial(),2,1,0).position());
    }

    @Test void retirementRequiresConsumedFullyAcknowledgedMatchingTerminalHome() {
        var ready=initial().prepared(data,new byte[32],new ApplyPosition(1,1)); var commit=ready.decide(TransactionStatus.Phase.COMMIT);
        var consumed=commit.finalized(data).consume(); var finalization=finalization(commit); var position=new ApplyPosition(2,3);
        var retirement=new ParticipantRetirement(finalization,position,consumed);
        assertEquals(retirement,ParticipantRetirement.decode(retirement.encode())); assertEquals(identity(initial()),retirement.identity());
        assertThrows(IllegalArgumentException.class,() -> new ParticipantRetirement(finalization,position,commit));
        assertThrows(IllegalArgumentException.class,() -> new ParticipantRetirement(finalization,position,commit.finalized(data)));
        var abort=ready.decide(TransactionStatus.Phase.ABORT).finalized(data).consume();
        assertThrows(IllegalArgumentException.class,() -> new ParticipantRetirement(finalization,position,abort));
        assertThrows(IllegalArgumentException.class,() -> new ParticipantRetirement(finalization,new ApplyPosition(1,1),consumed));
    }

    @Test void retirementCannotBecomeANativeVoteOrFinalizationReceipt() {
        var terminal=initial().decide(TransactionStatus.Phase.ABORT); var proof=fence(terminal,2,1,0); var vote=ParticipantVote.retired(proof);
        assertTrue(vote.retired()); assertFalse(vote.prepared()); assertNull(vote.position());
        assertThrows(IllegalArgumentException.class,() -> new ParticipantOutcome(finalization(terminal),new ApplyPosition(2,3),vote,null));
        var retired=ParticipantFinalizationResult.decode(ParticipantFinalizationResult.retired(proof).encode());
        assertTrue(retired.retired()); assertNull(retired.position()); assertEquals(proof,retired.retirement());
        var applied=ParticipantFinalizationResult.applied(new ApplyPosition(2,3)); assertEquals(applied,ParticipantFinalizationResult.decode(applied.encode()));
        assertFalse(applied.retired()); assertNull(applied.retirement());
        assertThrows(IllegalArgumentException.class,() -> new ParticipantFinalizationResult(new ApplyPosition(2,3),proof));
        assertThrows(IllegalArgumentException.class,() -> new ParticipantFinalizationResult(null,null));
        assertThrows(IllegalArgumentException.class,() -> ParticipantFinalizationResult.applied(new ApplyPosition(1,0)));
        byte[] malformed=retired.encode(); malformed[0]=2;
        assertThrows(IllegalArgumentException.class,() -> ParticipantFinalizationResult.decode(malformed));
        assertThrows(IllegalArgumentException.class,() -> ParticipantFinalizationResult.decode(Arrays.copyOf(applied.encode(),16)));
    }

    @Test void fencedHomeAckAcceptsFreshCoveringEvidenceOnlyForAbort() {
        var abort=initial().decide(TransactionStatus.Phase.ABORT); var proof=fence(abort,2,1,0);
        var recovery=new HomeRecovery(HomeRecovery.Action.ACK,command(abort,abort.finalized(data)),HomeRecovery.Proof.from(proof));
        assertTrue(recovery.proof().fenced()); assertEquals(recovery,HomeRecovery.decode(recovery.encode()));
        recovery.requireFence(proof); recovery.requireFence(fence(abort,3,2,1));
        assertThrows(IllegalArgumentException.class,() -> recovery.requireFence(null));
        var crossed=new ParticipantFenceProof(new ParticipantIdentity(home,2,data,initial()),2,new ApplyPosition(1,0));
        assertThrows(IllegalArgumentException.class,() -> recovery.requireFence(crossed));
        assertThrows(IllegalArgumentException.class,() -> recovery.requireOutcome(new ParticipantOutcome(finalization(abort),new ApplyPosition(2,3),null,null)));
        var commit=initial().prepared(data,new byte[32],new ApplyPosition(1,1)).decide(TransactionStatus.Phase.COMMIT);
        assertThrows(IllegalArgumentException.class,() -> new HomeRecovery(HomeRecovery.Action.ACK,command(commit,commit.finalized(data)),HomeRecovery.Proof.from(fence(commit,2,2,3))));
        byte[] wrongDigest=proof.identity().digest(); wrongDigest[0]^=1;
        assertThrows(IllegalArgumentException.class,() -> new HomeRecovery(HomeRecovery.Action.ACK,command(abort,abort.finalized(data)),
                new HomeRecovery.Proof(data,proof.position(),wrongDigest,true,proof.minimumEpoch())));
        byte[] malformed=recovery.encode(); int commandBytes=ByteBuffer.wrap(malformed).getInt(5); malformed[9+commandBytes]=2;
        assertThrows(IllegalArgumentException.class,() -> HomeRecovery.decode(malformed));
        assertThrows(IllegalArgumentException.class,() -> new HomeRecovery.Proof(data,proof.position(),proof.identity().digest(),false,2));
    }
}
