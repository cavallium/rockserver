package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ParticipantOutcomeTest {
    final CommittedParticipantTest helper=new CommittedParticipantTest();
    ParticipantOutcome commit() { return new ParticipantOutcome(helper.intent(),new ApplyPosition(2,2),ParticipantVote.prepared(helper.prepared.position()),helper.prepared.preparation().business().acceptance().result()); }
    ParticipantFinalization abort() { var p=helper.prepared.preparation(); return new ParticipantFinalization(p.homeGroup(),p.homeEpoch(),p.business().groupId(),helper.intent().operation(),p.registration().decide(TransactionStatus.Phase.ABORT)); }
    @Test void terminalOutcomeOwnsResultAndRoundTripsAllPrepareEvidenceShapes() {
        var commit=commit(); assertEquals(commit,ParticipantOutcome.decode(commit.encode())); byte[] result=commit.result(); Arrays.fill(result,(byte)0); assertEquals(commit,commit());
        for (var vote:Arrays.asList(null,ParticipantVote.negative(),ParticipantVote.prepared(new ApplyPosition(1,1)))) {
            var outcome=new ParticipantOutcome(abort(),new ApplyPosition(2,2),vote,null); assertEquals(outcome,ParticipantOutcome.decode(outcome.encode())); assertEquals(outcome.hashCode(),ParticipantOutcome.decode(outcome.encode()).hashCode());
        }
        assertThrows(IllegalArgumentException.class,() -> new ParticipantOutcome(helper.intent(),new ApplyPosition(2,2),null,commit.result()));
        assertThrows(IllegalArgumentException.class,() -> new ParticipantOutcome(helper.intent(),new ApplyPosition(2,2),ParticipantVote.negative(),commit.result()));
        assertThrows(IllegalArgumentException.class,() -> new ParticipantOutcome(helper.intent(),new ApplyPosition(2,2),commit.vote(),null));
        assertThrows(IllegalArgumentException.class,() -> new ParticipantOutcome(abort(),new ApplyPosition(2,2),null,commit.result()));
        assertThrows(IllegalArgumentException.class,() -> new ParticipantOutcome(helper.intent(),new ApplyPosition(2,2),commit.vote(),new CommandResult(CommandResult.Status.CONFLICT,List.of()).encode()));
    }
    @Test void decoderRejectsMalformedLengthsKindsTruncationAndChangedPrepareEvidence() {
        byte[] valid=commit().encode(); for (int n=0;n<valid.length;n++) { byte[] truncated=Arrays.copyOf(valid,n); assertThrows(IllegalArgumentException.class,() -> ParticipantOutcome.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> ParticipantOutcome.decode(Arrays.copyOf(valid,valid.length+1)));
        byte[] kind=valid.clone(); kind[16]=3; assertThrows(IllegalArgumentException.class,() -> ParticipantOutcome.decode(kind));
        byte[] length=valid.clone(); ByteBuffer.wrap(length).putInt(33,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,() -> ParticipantOutcome.decode(length));
        assertThrows(IllegalArgumentException.class,() -> new ParticipantOutcome(helper.intent(),new ApplyPosition(2,3),ParticipantVote.prepared(new ApplyPosition(1,2)),commit().result()));
        assertThrows(IllegalArgumentException.class,() -> new ParticipantOutcome(abort(),new ApplyPosition(1,1),ParticipantVote.prepared(new ApplyPosition(1,1)),null));
    }
}
