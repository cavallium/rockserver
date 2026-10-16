package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TransactionStatusTest {
    final UUID cluster=new UUID(0,1),database=new UUID(0,2),domain=new UUID(0,3),transaction=new UUID(0,4),session=new UUID(0,5),a=new UUID(0,6),b=new UUID(-1,0);
    TransactionStatus.Participant participant(UUID group) { return new TransactionStatus.Participant(domain, group, group, 1, new byte[32], new byte[32], null, false); }
    TransactionStatus initial() { return TransactionStatus.begin(cluster, database, transaction, new OperationId(session,1,0), List.of(new DomainBinding(domain, 0, new byte[32], 1, new byte[32])), new byte[32], List.of(participant(b),participant(a))); }
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
        for (int offset:new int[]{121,122}) { byte[] invalid=valid.clone(); if (offset==121) invalid[offset]=2; else ByteBuffer.wrap(invalid).putInt(offset,-1); assertThrows(IllegalArgumentException.class,() -> TransactionStatus.decode(invalid)); }
        byte[] vote=valid.clone(); ByteBuffer.wrap(vote).putLong(TransactionStatus.HEADER_BYTES+DomainBinding.BYTES+120,1); // A term without a positive prepare index is not evidence.
        assertThrows(IllegalArgumentException.class,() -> TransactionStatus.decode(vote));
        byte[] flags=valid.clone(); flags[TransactionStatus.HEADER_BYTES+DomainBinding.BYTES+136]=2; assertThrows(IllegalArgumentException.class,() -> TransactionStatus.decode(flags));
        byte[] order=valid.clone(); System.arraycopy(valid,TransactionStatus.HEADER_BYTES+DomainBinding.BYTES,order,TransactionStatus.HEADER_BYTES+DomainBinding.BYTES+TransactionStatus.PARTICIPANT_BYTES,TransactionStatus.PARTICIPANT_BYTES);
        System.arraycopy(valid,TransactionStatus.HEADER_BYTES+DomainBinding.BYTES+TransactionStatus.PARTICIPANT_BYTES,order,TransactionStatus.HEADER_BYTES+DomainBinding.BYTES,TransactionStatus.PARTICIPANT_BYTES);
        assertThrows(IllegalArgumentException.class,() -> TransactionStatus.decode(order));
        assertThrows(IllegalArgumentException.class,() -> TransactionStatus.decode(Arrays.copyOf(valid,valid.length+1)));
        assertThrows(IllegalArgumentException.class,() -> TransactionStatus.begin(cluster, database, transaction, new OperationId(session,1,0), List.of(new DomainBinding(domain, 0, new byte[32], 1, new byte[32])), new byte[32], List.of(participant(a),participant(a))));
    }
    @Test void successorValidationRejectsBatchVotesChangedDefinitionsRegressionsAndGenerationOverflow() {
        var initial=initial(); var ready=prepared(); assertThrows(IllegalArgumentException.class,() -> ready.requireSuccessorOf(initial));
        var other=TransactionStatus.begin(cluster, database, UUID.randomUUID(), initial.operation(), List.of(new DomainBinding(domain, 0, new byte[32], 1, new byte[32])), new byte[32], initial.participants()).prepared(a,new byte[32],new ApplyPosition(1,1));
        assertThrows(IllegalArgumentException.class,() -> other.requireSuccessorOf(initial));
        byte[] exhausted=initial.encode(); ByteBuffer.wrap(exhausted).putLong(112,Long.MAX_VALUE); var maximum=TransactionStatus.decode(exhausted);
        assertThrows(IllegalArgumentException.class,() -> maximum.prepared(a,new byte[32],new ApplyPosition(1,1)));
        byte[] jump=ready.encode(); ByteBuffer.wrap(jump).putLong(112,initial.generation()+1); var forged=TransactionStatus.decode(jump);
        assertThrows(IllegalArgumentException.class,() -> forged.requireSuccessorOf(initial));
        var participants=new ArrayList<TransactionStatus.Participant>(); for (int i=0;i<TransactionStatus.MAX_PARTICIPANTS;i++) participants.add(participant(new UUID(0,100+i)));
        var max=TransactionStatus.begin(cluster, database, transaction, initial.operation(), List.of(new DomainBinding(domain, 0, new byte[32], 1, new byte[32])), new byte[32], participants);
        assertTrue(max.encode().length<=TransactionStatus.MAX_BYTES); assertEquals(max,TransactionStatus.decode(max.encode())); participants.add(participant(new UUID(0,200)));
        assertThrows(IllegalArgumentException.class,() -> TransactionStatus.begin(cluster, database, transaction, initial.operation(), List.of(new DomainBinding(domain, 0, new byte[32], 1, new byte[32])), new byte[32], participants));
    }
    TransactionStatus reservation(int count) { return TransactionStatus.reserve(OperationRerouteOriginTest.origin(count), new DomainBinding(OperationRerouteOriginTest.origin(count).domainId(), 0, OperationRerouteOriginTest.origin(count).targetDirectoryDigest(), 1, new byte[32])); }
    TransactionStatus accepted(TransactionStatus reserved,List<TransactionStatus.Participant> participants) { return TransactionStatus.begin(reserved.clusterId(), reserved.databaseId(), reserved.transactionId(), reserved.operation(), reserved.domains(), OperationRerouteOriginTest.digest(9), participants); }
    @Test void reservationHasNoEffectsAndPromotionInstallsAcceptedHashOnceBeforeOrdinaryVotes() {
        var reserved=reservation(2); reserved.requireInitial(); assertTrue(reserved.reserved()); assertFalse(reserved.terminal()); assertFalse(reserved.fullyFinalized()); assertFalse(reserved.gcEligible()); assertTrue(reserved.participants().isEmpty()); assertEquals(reserved,reserved.asInitial()); assertEquals(reserved,TransactionStatus.decode(reserved.encode()));
        assertThrows(IllegalArgumentException.class,reserved::terminalDecision); assertThrows(IllegalArgumentException.class,reserved::consume); assertThrows(IllegalArgumentException.class,()->reserved.prepared(a,new byte[32],new ApplyPosition(1,1))); assertThrows(IllegalArgumentException.class,()->reserved.finalized(a)); assertThrows(IllegalArgumentException.class,()->reserved.decide(TransactionStatus.Phase.COMMIT));
        var accepted=accepted(reserved,List.of(participant(b),participant(a))); var promoted=reserved.promote(accepted); promoted.requireInitial(); promoted.requireSuccessorOf(reserved); assertEquals(1,promoted.generation()); assertFalse(promoted.reserved()); assertEquals(reserved.origin(),promoted.origin()); assertArrayEquals(accepted.planDigest(),promoted.planDigest()); assertFalse(Arrays.equals(promoted.planDigest(),promoted.origin().planDigest())); assertFalse(promoted.sameDefinition(reserved));
        assertThrows(IllegalArgumentException.class,()->promoted.promote(accepted)); assertThrows(IllegalArgumentException.class,()->reserved.promote(accepted.prepared(a,new byte[32],new ApplyPosition(1,1))));
        var ready=promoted.prepared(a,new byte[32],new ApplyPosition(1,1)).prepared(b,new byte[32],new ApplyPosition(1,2)); var commit=ready.decide(TransactionStatus.Phase.COMMIT); assertEquals(promoted,commit.asInitial()); assertEquals(reserved.origin(),commit.terminalDecision().origin()); assertEquals(commit.terminalDecision(),TransactionStatus.decode(commit.terminalDecision().encode()));
        var consumed=commit.finalized(a).finalized(b).consume(); assertTrue(consumed.gcEligible()); assertEquals(reserved.origin(),consumed.origin()); assertEquals(promoted,consumed.asInitial());
    }
    @Test void emptyAbortFencesPromotionAndPayloadCollisionUsesTheSameHomeIdentity() {
        var reserved=reservation(2); var aborted=reserved.decide(TransactionStatus.Phase.ABORT); aborted.requireSuccessorOf(reserved); assertEquals(1,aborted.generation()); assertTrue(aborted.terminal()); assertTrue(aborted.fullyFinalized()); assertFalse(aborted.gcEligible()); assertTrue(aborted.participants().isEmpty()); assertEquals(reserved,aborted.asInitial()); assertEquals(reserved.origin(),aborted.terminalDecision().origin());
        var consumed=aborted.consume(); consumed.requireSuccessorOf(aborted); assertTrue(consumed.gcEligible()); assertEquals(consumed,TransactionStatus.decode(consumed.encode())); assertThrows(IllegalArgumentException.class,()->aborted.promote(accepted(reserved,List.of(participant(a),participant(b)))));
        var o=reserved.origin(); var different=new OperationRerouteOrigin(o.clusterId(),o.databaseId(),o.domainId(),o.source(),o.operation(),OperationRerouteOriginTest.digest(99),o.absenceCommitment(),o.absenceCut(),o.targetDirectoryDigest(),o.planDigest(),2,o.firstControl()); var competing=TransactionStatus.reserve(different, new DomainBinding(different.domainId(), 0, different.targetDirectoryDigest(), 1, new byte[32])); assertEquals(reserved.transactionId(),competing.transactionId()); assertFalse(reserved.sameDefinition(competing));
        assertThrows(IllegalArgumentException.class,()->competing.promote(accepted(competing,List.of(participant(a),participant(b)))).requireSuccessorOf(reserved)); assertThrows(IllegalArgumentException.class,()->accepted(reserved,List.of(participant(a),participant(b))).requireSuccessorOf(reserved)); assertThrows(IllegalArgumentException.class,()->reserved.promote(accepted(reserved,List.of(participant(a)))));
    }
    @Test void maximumPromotedReservationAndMalformedGenerationOriginAndPhaseAreBounded() {
        var reserved=reservation(TransactionStatus.MAX_PARTICIPANTS); var participants=new ArrayList<TransactionStatus.Participant>(); for(int i=0;i<TransactionStatus.MAX_PARTICIPANTS;i++) participants.add(participant(new UUID(0,100+i))); var promoted=reserved.promote(accepted(reserved,participants)); assertEquals(TransactionStatus.HEADER_BYTES+DomainBinding.BYTES+OperationRerouteOrigin.MAX_BYTES+TransactionStatus.MAX_PARTICIPANTS*TransactionStatus.PARTICIPANT_BYTES,promoted.encode().length); assertEquals(promoted,TransactionStatus.decode(promoted.encode()));
        var originLength=reserved.encode(); ByteBuffer.wrap(originLength).putInt(127,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,()->TransactionStatus.decode(originLength));
        var generation=reserved.encode(); ByteBuffer.wrap(generation).putLong(112,1); assertThrows(IllegalArgumentException.class,()->TransactionStatus.decode(generation)); var preparing=promoted.encode(); ByteBuffer.wrap(preparing).putLong(112,0); assertThrows(IllegalArgumentException.class,()->TransactionStatus.decode(preparing));
        var missingOrigin=initial().encode(); missingOrigin[120]=(byte)TransactionStatus.Phase.RESERVED.ordinal(); assertThrows(IllegalArgumentException.class,()->TransactionStatus.decode(missingOrigin)); var unconsumed=reserved.encode(); unconsumed[121]=1; assertThrows(IllegalArgumentException.class,()->TransactionStatus.decode(unconsumed));
        var noOp=promoted.encode(); ByteBuffer.wrap(noOp).putLong(112,2); var decoded=TransactionStatus.decode(noOp); assertThrows(IllegalArgumentException.class,()->decoded.requireSuccessorOf(promoted));
    }
    @Test void noEffectAbortChoosesOneCanonicalBusinessOutcomeAcrossReplayAndConsumption() {
        var reserved=reservation(2); assertEquals(TransactionStatus.NoEffectOutcome.NONE,reserved.noEffectOutcome());
        var conflict=reserved.decide(TransactionStatus.Phase.ABORT); var expired=reserved.abortReservation(TransactionStatus.NoEffectOutcome.SESSION_EXPIRED);
        for(var terminal:List.of(conflict,expired)) {
            terminal.requireSuccessorOf(reserved); assertTrue(terminal.sameDefinition(reserved)); assertTrue(terminal.participants().isEmpty()); assertEquals(terminal,TransactionStatus.decode(terminal.encode())); assertEquals(terminal.noEffectOutcome(),terminal.terminalDecision().noEffectOutcome()); assertEquals(TransactionStatus.NoEffectOutcome.NONE,terminal.asInitial().noEffectOutcome()); assertEquals(reserved,terminal.asInitial());
            var consumed=terminal.consume(); consumed.requireSuccessorOf(terminal); assertEquals(terminal.noEffectOutcome(),consumed.noEffectOutcome()); assertEquals(terminal.terminalDecision(),consumed.terminalDecision()); assertArrayEquals(terminal.terminalBusinessResult().encode(),consumed.terminalBusinessResult().encode()); assertSame(consumed,consumed.decide(TransactionStatus.Phase.ABORT));
        }
        assertEquals(CommandResult.Status.CONFLICT,conflict.terminalBusinessResult().status()); assertEquals(CommandResult.Status.SESSION_EXPIRED,expired.terminalBusinessResult().status()); assertEquals(0,expired.terminalBusinessResult().valueCount());
        assertThrows(IllegalArgumentException.class,()->reserved.abortReservation(TransactionStatus.NoEffectOutcome.NONE)); assertThrows(IllegalArgumentException.class,reserved::terminalBusinessResult); assertThrows(IllegalArgumentException.class,()->expired.abortReservation(TransactionStatus.NoEffectOutcome.CONFLICT));
        var flip=conflict.consume().encode(); flip[126]=(byte)TransactionStatus.NoEffectOutcome.SESSION_EXPIRED.ordinal(); var changed=TransactionStatus.decode(flip); assertTrue(changed.sameDefinition(conflict)); assertThrows(IllegalArgumentException.class,()->changed.requireSuccessorOf(conflict));
        var unset=expired.encode(); unset[126]=0; assertThrows(IllegalArgumentException.class,()->TransactionStatus.decode(unset)); var invalid=reserved.encode(); invalid[126]=127; assertThrows(IllegalArgumentException.class,()->TransactionStatus.decode(invalid));
        var ordinaryAbort=initial().decide(TransactionStatus.Phase.ABORT); assertEquals(TransactionStatus.NoEffectOutcome.NONE,ordinaryAbort.noEffectOutcome()); assertEquals(CommandResult.Status.CONFLICT,ordinaryAbort.terminalBusinessResult().status()); var forbidden=ordinaryAbort.encode(); forbidden[126]=1; assertThrows(IllegalArgumentException.class,()->TransactionStatus.decode(forbidden)); assertThrows(IllegalArgumentException.class,()->ordinaryAbort.abortReservation(TransactionStatus.NoEffectOutcome.SESSION_EXPIRED));
        assertThrows(IllegalArgumentException.class,()->prepared().decide(TransactionStatus.Phase.COMMIT).terminalBusinessResult());
    }

}
