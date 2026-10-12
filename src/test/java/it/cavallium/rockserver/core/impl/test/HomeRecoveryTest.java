package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class HomeRecoveryTest {
    final UUID cluster=new UUID(0,1),database=new UUID(0,2),domain=new UUID(0,3),transaction=new UUID(0,4),home=new UUID(0,5),column=new UUID(0,6),data=new UUID(0,7);
    TransactionStatus initial() { return TransactionStatus.begin(cluster,database,domain,transaction,new OperationId(new UUID(0,8),1,0),1,new byte[32],new byte[32],List.of(new TransactionStatus.Participant(data,data,1,new byte[32],new byte[32],null,false))); }
    DistributedCommand command(TransactionStatus before,TransactionStatus next,OperationId operation) {
        UUID identity=before==null ? next.transactionId() : before.transactionId(); byte[] key=ByteBuffer.allocate(16).putLong(identity.getMostSignificantBits()).putLong(identity.getLeastSignificantBits()).array();
        return new DistributedCommand(home,1,new byte[32],operation,List.of(new Mutation(Mutation.Kind.PUT,column,key,next.encode())),List.of(new KeyPredicate(column,key,before==null ? null : before.encode())));
    }
    OperationId control(long n) { return new OperationId(new UUID(0,9),0,n); }
    ParticipantOutcome abort(TransactionStatus terminal,long sequence) { return new ParticipantOutcome(new ParticipantFinalization(home,1,data,new OperationId(new UUID(0,10),0,sequence),terminal),new ApplyPosition(2,3),null,null); }
    @Test void currentAbortAndAckOwnValuesAndRequireExactQualifiedOutcomeBinding() {
        var before=initial(); var terminal=before.decide(TransactionStatus.Phase.ABORT);
        var stop=new HomeRecovery(HomeRecovery.Action.ABORT,command(before,terminal,control(0)),null); var outcome=abort(terminal,0);
        var ack=new HomeRecovery(HomeRecovery.Action.ACK,command(terminal,terminal.finalized(data),control(1)),HomeRecovery.Proof.from(outcome));
        for (var record:List.of(stop,ack)) {
            byte[] encoded=record.encode(); assertTrue(HomeRecovery.isRecovery(encoded)); assertEquals(record,HomeRecovery.decode(encoded)); assertEquals(record.hashCode(),HomeRecovery.decode(encoded).hashCode()); Arrays.fill(encoded,(byte)0); assertEquals(record,HomeRecovery.decode(record.encode()));
            for (int n=0;n<record.encode().length;n++) { byte[] truncated=Arrays.copyOf(record.encode(),n); assertThrows(IllegalArgumentException.class,() -> HomeRecovery.decode(truncated)); }
        }
        ack.requireOutcome(outcome);
        ack.requireOutcome(new ParticipantOutcome(outcome.finalization(),outcome.position(),ParticipantVote.negative(),null));
        byte[] digest=ack.proof().digest(); Arrays.fill(digest,(byte)1); ack.requireOutcome(outcome);
        assertThrows(IllegalArgumentException.class,() -> ack.requireOutcome(abort(terminal,1)));
        var differentHome=new ParticipantOutcome(new ParticipantFinalization(UUID.randomUUID(),1,data,outcome.finalization().operation(),terminal),outcome.position(),null,null);
        assertThrows(IllegalArgumentException.class,() -> ack.requireOutcome(differentHome));
        assertThrows(IllegalArgumentException.class,() -> stop.requireOutcome(outcome));
    }
    @Test void commitAckBindsPositiveVoteResultAndWinningFinalizationIdentity() {
        var ready=initial().prepared(data,new byte[32],new ApplyPosition(1,1)); var commit=ready.decide(TransactionStatus.Phase.COMMIT);
        var finalization=new ParticipantFinalization(home,1,data,new OperationId(new UUID(0,10),0,0),commit);
        var result=new CommandResult(CommandResult.Status.SUCCESS,List.of(new byte[]{8}));
        var outcome=new ParticipantOutcome(finalization,new ApplyPosition(2,3),ParticipantVote.prepared(new ApplyPosition(1,1)),result.encode());
        var ack=new HomeRecovery(HomeRecovery.Action.ACK,command(commit,commit.finalized(data),control(0)),HomeRecovery.Proof.from(outcome)); ack.requireOutcome(outcome);
        assertEquals(ack,HomeRecovery.decode(ack.encode()));
        var changedResult=new ParticipantOutcome(finalization,outcome.position(),outcome.vote(),new CommandResult(CommandResult.Status.SUCCESS,List.of(new byte[]{9})).encode());
        assertThrows(IllegalArgumentException.class,() -> ack.requireOutcome(changedResult));
        var foreign=new ParticipantFinalization(home,1,data,new OperationId(new UUID(0,10),0,1),commit);
        assertThrows(IllegalArgumentException.class,() -> ack.requireOutcome(new ParticipantOutcome(foreign,outcome.position(),outcome.vote(),result.encode())));
    }
    @Test void onlyExistingAbortOrOneAckTransitionsUseTheProtectedStream() {
        var before=initial(); var terminal=before.decide(TransactionStatus.Phase.ABORT); var completed=terminal.finalized(data);
        assertThrows(IllegalArgumentException.class,() -> new HomeRecovery(HomeRecovery.Action.ABORT,command(null,before,control(0)),null));
        assertThrows(IllegalArgumentException.class,() -> new HomeRecovery(HomeRecovery.Action.ABORT,command(before,terminal,new OperationId(new UUID(0,9),1,0)),null));
        assertThrows(IllegalArgumentException.class,() -> new HomeRecovery(HomeRecovery.Action.ACK,command(completed,completed.consume(),control(0)),HomeRecovery.Proof.from(abort(terminal,0))));
        assertThrows(IllegalArgumentException.class,() -> new HomeRecovery(HomeRecovery.Action.ACK,command(terminal,completed,control(0)),null));
        assertThrows(IllegalArgumentException.class,() -> new HomeRecovery(HomeRecovery.Action.ABORT,command(terminal,completed,control(0)),null));
        var ready=before.prepared(data,new byte[32],new ApplyPosition(1,1));
        assertThrows(IllegalArgumentException.class,() -> new HomeRecovery(HomeRecovery.Action.ABORT,command(before,ready,control(0)),null));
        assertThrows(IllegalArgumentException.class,() -> new HomeRecovery(HomeRecovery.Action.ABORT,command(ready,ready.decide(TransactionStatus.Phase.COMMIT),control(0)),null));
    }
    @Test void receiptKeepsOriginalOutcomeAndExactCompletedPosition() {
        var before=initial(); var recovery=new HomeRecovery(HomeRecovery.Action.ABORT,command(before,before.decide(TransactionStatus.Phase.ABORT),control(0)),null);
        for (var status:List.of(CommandResult.Status.SUCCESS,CommandResult.Status.CONFLICT)) {
            var receipt=new HomeRecoveryReceipt(recovery,new ApplyPosition(1,2),new CommandResult(status,List.of())); var copy=HomeRecoveryReceipt.decode(receipt.encode());
            assertEquals(recovery,copy.recovery()); assertEquals(receipt.position(),copy.position()); assertArrayEquals(receipt.result().encode(),copy.result().encode());
            receipt.requireCut(new ApplyPosition(1,2)); receipt.requireCut(new ApplyPosition(2,3)); assertThrows(IllegalArgumentException.class,() -> receipt.requireCut(new ApplyPosition(2,2)));
            for (int n=0;n<receipt.encode().length;n++) { byte[] truncated=Arrays.copyOf(receipt.encode(),n); assertThrows(IllegalArgumentException.class,() -> HomeRecoveryReceipt.decode(truncated)); }
        }
        assertThrows(IllegalArgumentException.class,() -> new HomeRecoveryReceipt(recovery,new ApplyPosition(1,1),new CommandResult(CommandResult.Status.SESSION_EXPIRED,List.of())));
    }    @Test void reservedHomeUsesProtectedNoEffectAbortAndNeverAcknowledgesReservation() {
        var origin=new OperationRerouteOrigin(cluster,database,domain,new DistributedCdcCheckpoint.Lineage(data,data,1),initial().operation(),new byte[32],new byte[32],new ApplyPosition(2,3),new byte[32],new byte[32],1,new OperationId(new UUID(0,9),1,10)); var reserved=TransactionStatus.reserve(origin,1,new byte[32]); var aborted=reserved.decide(TransactionStatus.Phase.ABORT);
        var recovery=new HomeRecovery(HomeRecovery.Action.ABORT,command(reserved,aborted,control(0)),null); assertEquals(reserved,recovery.before()); assertEquals(aborted,recovery.next()); assertEquals(recovery,HomeRecovery.decode(recovery.encode())); assertTrue(recovery.next().fullyFinalized()); assertEquals(origin,recovery.next().origin()); assertTrue(recovery.next().consume().gcEligible());
        assertThrows(IllegalArgumentException.class,()->new HomeRecovery(HomeRecovery.Action.ACK,command(reserved,aborted,control(1)),new HomeRecovery.Proof(data,new ApplyPosition(1,1),new byte[32])));
        var accepted=TransactionStatus.begin(cluster,database,domain,reserved.transactionId(),reserved.operation(),1,new byte[32],new byte[32],List.of(new TransactionStatus.Participant(data,data,1,new byte[32],new byte[32],null,false))); var promoted=reserved.promote(accepted);
        assertThrows(IllegalArgumentException.class,()->new HomeRecovery(HomeRecovery.Action.ABORT,command(reserved,promoted,control(1)),null));
    }

}
