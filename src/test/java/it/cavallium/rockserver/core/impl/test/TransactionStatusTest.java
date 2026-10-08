package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TransactionStatusTest {
    final UUID cluster=new UUID(0,1),database=new UUID(0,2),domain=new UUID(0,3),transaction=new UUID(0,4),session=new UUID(0,5),a=new UUID(0,6),b=new UUID(-1,0);
    TransactionStatus.Participant participant(UUID group) { return new TransactionStatus.Participant(group,group,1,new byte[32],new byte[32],null,false); }
    TransactionStatus initial() { return TransactionStatus.begin(cluster,database,domain,transaction,new OperationId(session,1,0),1,new byte[32],new byte[32],List.of(participant(b),participant(a))); }
    TransactionStatus prepared() { return initial().prepared(a,new byte[32],new ApplyPosition(1,1)).prepared(b,new byte[32],new ApplyPosition(2,2)); }
    @Test void canonicalCurrentEncodingOwnsArraysAndFreezesIdentity() {
        var status=initial(); assertEquals(a,status.participants().getFirst().groupId()); assertEquals(status,TransactionStatus.decode(status.encode()));
        byte[] bytes=status.encode(); var decoded=TransactionStatus.decode(bytes); Arrays.fill(bytes,(byte)9); assertEquals(status,decoded);
        var p=participant(a); p.schemaDigest()[0]=1; assertEquals(0,p.schemaDigest()[0]); p.planDigest()[0]=1; assertEquals(0,p.planDigest()[0]);
        var vote=status.prepared(a,new byte[32],new ApplyPosition(1,1)); vote.requireSuccessorOf(status); assertSame(vote,vote.prepared(a,new byte[32],new ApplyPosition(1,1)));
        assertThrows(IllegalArgumentException.class,() -> vote.prepared(a,new byte[32],new ApplyPosition(1,2)));
        assertThrows(IllegalArgumentException.class,() -> status.prepared(a,new byte[32],new ApplyPosition(1,0)));
        byte[] hash=new byte[32]; hash[0]=1; assertThrows(IllegalArgumentException.class,() -> status.prepared(a,hash,new ApplyPosition(1,1)));
        assertThrows(IllegalArgumentException.class,() -> status.prepared(UUID.randomUUID(),new byte[32],new ApplyPosition(1,1)));
    }
    @Test void votesDecisionFinalizationAndConsumptionAdvanceOneStepWithoutEarlyGc() {
        var initial=initial(); assertThrows(IllegalArgumentException.class,() -> initial.decide(TransactionStatus.Phase.COMMIT));
        var ready=prepared(); var vote=initial.prepared(a,new byte[32],new ApplyPosition(1,1)); ready.requireSuccessorOf(vote);
        var commit=ready.decide(TransactionStatus.Phase.COMMIT); commit.requireSuccessorOf(ready); assertFalse(commit.fullyFinalized()); assertFalse(commit.gcEligible());
        assertSame(commit,commit.decide(TransactionStatus.Phase.COMMIT)); assertThrows(IllegalArgumentException.class,() -> commit.decide(TransactionStatus.Phase.ABORT));
        assertThrows(IllegalArgumentException.class,commit::consume); assertThrows(IllegalArgumentException.class,() -> commit.prepared(a,new byte[32],new ApplyPosition(1,1)));
        var one=commit.finalized(a); one.requireSuccessorOf(commit); var both=one.finalized(b); both.requireSuccessorOf(one);
        assertTrue(both.fullyFinalized()); assertFalse(both.gcEligible()); var consumed=both.consume(); consumed.requireSuccessorOf(both); assertTrue(consumed.gcEligible());
        assertEquals(consumed,TransactionStatus.decode(consumed.encode())); assertSame(consumed,consumed.consume()); assertSame(consumed,consumed.finalized(a));
        var abort=initial.decide(TransactionStatus.Phase.ABORT); abort.requireSuccessorOf(initial);
        assertThrows(IllegalArgumentException.class,() -> abort.decide(TransactionStatus.Phase.COMMIT));
        assertTrue(abort.finalized(a).finalized(b).consume().gcEligible(),"ABORT acknowledgements must later be backed by participant durable late-prepare fences");
    }
    @Test void decodingRejectsMalformedFlagsCountsPositionsOrderingAndTruncation() {
        byte[] valid=initial().encode();
        for (int n=0;n<valid.length;n++) { byte[] truncated=Arrays.copyOf(valid,n); assertThrows(IllegalArgumentException.class,() -> TransactionStatus.decode(truncated)); }
        for (int offset:new int[]{0,181,182}) { byte[] invalid=valid.clone(); if (offset==181) invalid[offset]=2; else ByteBuffer.wrap(invalid).putInt(offset,-1); assertThrows(IllegalArgumentException.class,() -> TransactionStatus.decode(invalid)); }
        byte[] vote=valid.clone(); ByteBuffer.wrap(vote).putLong(TransactionStatus.HEADER_BYTES+104,1); // A term without a positive prepare index is not evidence.
        assertThrows(IllegalArgumentException.class,() -> TransactionStatus.decode(vote));
        byte[] flags=valid.clone(); flags[TransactionStatus.HEADER_BYTES+120]=2; assertThrows(IllegalArgumentException.class,() -> TransactionStatus.decode(flags));
        byte[] order=valid.clone(); System.arraycopy(valid,TransactionStatus.HEADER_BYTES,order,TransactionStatus.HEADER_BYTES+TransactionStatus.PARTICIPANT_BYTES,TransactionStatus.PARTICIPANT_BYTES);
        System.arraycopy(valid,TransactionStatus.HEADER_BYTES+TransactionStatus.PARTICIPANT_BYTES,order,TransactionStatus.HEADER_BYTES,TransactionStatus.PARTICIPANT_BYTES);
        assertThrows(IllegalArgumentException.class,() -> TransactionStatus.decode(order));
        assertThrows(IllegalArgumentException.class,() -> TransactionStatus.decode(Arrays.copyOf(valid,valid.length+1)));
        assertThrows(IllegalArgumentException.class,() -> TransactionStatus.begin(cluster,database,domain,transaction,new OperationId(session,1,0),1,new byte[32],new byte[32],List.of(participant(a),participant(a))));
    }
    @Test void successorValidationRejectsBatchVotesChangedDefinitionsRegressionsAndGenerationOverflow() {
        var initial=initial(); var ready=prepared(); assertThrows(IllegalArgumentException.class,() -> ready.requireSuccessorOf(initial));
        var other=TransactionStatus.begin(cluster,database,domain,UUID.randomUUID(),initial.operation(),1,new byte[32],new byte[32],initial.participants()).prepared(a,new byte[32],new ApplyPosition(1,1));
        assertThrows(IllegalArgumentException.class,() -> other.requireSuccessorOf(initial));
        byte[] exhausted=initial.encode(); ByteBuffer.wrap(exhausted).putLong(172,Long.MAX_VALUE); var maximum=TransactionStatus.decode(exhausted);
        assertThrows(IllegalArgumentException.class,() -> maximum.prepared(a,new byte[32],new ApplyPosition(1,1)));
        byte[] jump=ready.encode(); ByteBuffer.wrap(jump).putLong(172,initial.generation()+1); var forged=TransactionStatus.decode(jump);
        assertThrows(IllegalArgumentException.class,() -> forged.requireSuccessorOf(initial));
        var participants=new ArrayList<TransactionStatus.Participant>(); for (int i=0;i<TransactionStatus.MAX_PARTICIPANTS;i++) participants.add(participant(new UUID(0,100+i)));
        var max=TransactionStatus.begin(cluster,database,domain,transaction,initial.operation(),1,new byte[32],new byte[32],participants);
        assertEquals(TransactionStatus.MAX_BYTES,max.encode().length); assertEquals(max,TransactionStatus.decode(max.encode())); participants.add(participant(new UUID(0,200)));
        assertThrows(IllegalArgumentException.class,() -> TransactionStatus.begin(cluster,database,domain,transaction,initial.operation(),1,new byte[32],new byte[32],participants));
    }
}
