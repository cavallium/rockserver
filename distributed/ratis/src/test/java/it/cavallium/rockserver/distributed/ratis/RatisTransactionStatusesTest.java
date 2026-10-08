package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Home CAS through native apply/receipts. No participant lock or distributed commit acceptance claim. */
class RatisTransactionStatusesTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final UUID a=new UUID(0,20),b=new UUID(0,21);
    TransactionStatus initial(RangeDirectory directory) {
        var participants=List.of(new TransactionStatus.Participant(a,a,1,new byte[32],new byte[32],null,false),new TransactionStatus.Participant(b,b,1,new byte[32],new byte[32],null,false));
        return TransactionStatus.begin(directory.clusterId(),directory.databaseId(),new UUID(0,30),new UUID(0,31),new OperationId(new UUID(0,32),1,0),1,new byte[32],new byte[32],participants);
    }
    OperationId operation(long n) { return new OperationId(new UUID(0,40),1,n); }
    @Test void decisionRaceLostRepliesAndConflictedIdentityRetriesNeverChangeTheChosenOutcome() throws Exception {
        var helper=new RatisTransactionRpcTest(); helper.root=root; var validation=new AtomicReference<Consumer<DistributedCommand>>(command -> {});
        for (boolean abortFirst:new boolean[]{false,true}) try (var f=new RatisRetentionAdmissionTest.Fixture(root.resolve("home-"+abortFirst),RatisRangeStateMachine.RetentionLimits.defaults(),command -> validation.get().accept(command))) {
            var directory=helper.directory(f); validation.set(RatisTransactionStatuses.validator(directory.clusterId(),directory.databaseId(),f.groupId,1,new byte[32],f.column));
            var writes=helper.new Controlled(f);
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException {
                    try { byte[] reply=f.machine.queryLinearizable(Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.pointRequest(column,key)))).get(5,TimeUnit.SECONDS).getContent().toByteArray(); return ScopedTransactionRpc.pointValue(reply,TransactionStatus.MAX_BYTES); }
                    catch (Exception failure) { throw new IOException(failure); }
                }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return writes.command(route,command); }
            };
            try (var routed=new RatisRoutedClient(directory,transport)) {
                var statuses=new RatisTransactionStatuses(routed,f.column); var initial=initial(directory); assertNull(statuses.read(initial.transactionId()));
                var begin=statuses.prepare(operation(0),null,initial); writes.lose=true; assertThrows(IOException.class,() -> statuses.execute(begin));
                assertEquals(CommandResult.Status.SUCCESS,statuses.execute(begin).status()); assertEquals(initial,statuses.read(initial.transactionId()));
                assertThrows(IOException.class,() -> statuses.readFinalization(initial.transactionId(),a,operation(90)));
                var first=initial.prepared(a,new byte[32],new ApplyPosition(1,1)); var second=initial.prepared(b,new byte[32],new ApplyPosition(1,2));
                var voteA=statuses.prepare(operation(1),initial,first); var voteB=statuses.prepare(operation(2),initial,second);
                var pendingA=f.propose(voteA.command()); var pendingB=f.propose(voteB.command());
                assertEquals(CommandResult.Status.SUCCESS,f.apply(pendingA,++writes.index)); assertEquals(CommandResult.Status.CONFLICT,f.apply(pendingB,++writes.index));
                assertEquals(CommandResult.Status.CONFLICT,statuses.execute(voteB).status(),"Confirmed conflict keeps its original receipt");
                var ready=first.prepared(b,new byte[32],new ApplyPosition(1,2)); assertEquals(CommandResult.Status.SUCCESS,statuses.execute(statuses.prepare(operation(3),first,ready)).status());
                var commit=ready.decide(TransactionStatus.Phase.COMMIT); var abort=ready.decide(TransactionStatus.Phase.ABORT);
                var chosen=abortFirst ? abort : commit; var rejected=abortFirst ? commit : abort;
                var win=statuses.prepare(operation(4),ready,chosen); var lose=statuses.prepare(operation(5),ready,rejected);
                var pendingWin=f.propose(win.command()); var pendingLose=f.propose(lose.command());
                assertEquals(CommandResult.Status.SUCCESS,f.apply(pendingWin,++writes.index)); assertEquals(CommandResult.Status.CONFLICT,f.apply(pendingLose,++writes.index));
                assertEquals(chosen,statuses.read(initial.transactionId())); assertEquals(CommandResult.Status.SUCCESS,statuses.execute(win).status());
                var finalizeA=statuses.readFinalization(initial.transactionId(),a,operation(90));
                assertEquals(f.groupId,finalizeA.homeGroup()); assertEquals(1,finalizeA.homeEpoch()); assertEquals(chosen.terminalDecision(),finalizeA.decision());
                assertThrows(IllegalArgumentException.class,() -> chosen.decide(rejected.phase()));
                var one=chosen.finalized(a); var all=one.finalized(b); var consumed=all.consume();
                statuses.execute(statuses.prepare(operation(6),chosen,one)); statuses.execute(statuses.prepare(operation(7),one,all)); statuses.execute(statuses.prepare(operation(8),all,consumed));
                assertTrue(statuses.read(initial.transactionId()).gcEligible()); assertEquals(finalizeA,statuses.readFinalization(initial.transactionId(),a,operation(90))); assertEquals(CommandResult.Status.CONFLICT,statuses.execute(lose).status());
                f.machine.takeSnapshot(); f.machine.pause(); f.machine.reinitialize(); assertEquals(consumed,statuses.read(initial.transactionId()));
                assertEquals(CommandResult.Status.SUCCESS,statuses.execute(begin).status(),"Original begin receipt does not reset later home state"); assertEquals(consumed,statuses.read(initial.transactionId()));
                assertTrue(f.store.receiptUsage().retainedCount()>1,"Status consumption does not implicitly ACK CAS receipts");
                var ack=routed.prepareAcknowledgement(operation(9),f.column,begin.command().mutations().getFirst().key(),8);
                assertEquals(CommandResult.Status.SUCCESS,statuses.execute(ack).status()); assertEquals(1,f.store.receiptUsage().retainedCount());
                assertEquals(CommandResult.Status.SESSION_EXPIRED,statuses.execute(begin).status()); assertEquals(consumed,statuses.read(initial.transactionId()),"Receipt GC never deletes the transaction decision");
                var recoveryTarget=TransactionStatus.begin(directory.clusterId(),directory.databaseId(),initial.domainId(),new UUID(0,50),initial.operation(),1,new byte[32],new byte[32],initial.participants());
                statuses.execute(statuses.prepare(operation(10),null,recoveryTarget));
                var floor=DistributedCommand.advanceSessionEpochFloor(f.groupId,1,new byte[32],new OperationId(f.admin,2,0),2);
                assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(floor),++writes.index));
                var recoveredVote=recoveryTarget.prepared(a,new byte[32],new ApplyPosition(1,1));
                assertEquals(CommandResult.Status.SESSION_EXPIRED,statuses.execute(statuses.prepare(operation(11),recoveryTarget,recoveredVote)).status());
                assertEquals(recoveryTarget,statuses.read(recoveryTarget.transactionId()));
                var recoveryId=new OperationId(new UUID(0,51),2,0);
                assertEquals(CommandResult.Status.SUCCESS,statuses.execute(statuses.prepare(recoveryId,recoveryTarget,recoveredVote)).status());
                var recoveredAbort=recoveredVote.decide(TransactionStatus.Phase.ABORT);
                assertEquals(CommandResult.Status.SUCCESS,statuses.execute(statuses.prepare(new OperationId(recoveryId.sessionId(),2,1),recoveredVote,recoveredAbort)).status());
                assertEquals(1,statuses.read(recoveryTarget.transactionId()).operation().epoch(),"Recovery operation epochs do not rewrite the frozen business identity");
                f.assertReleased();
            }
        }
    }
    @Test void validatorRejectsAcceptedCasBypassExtraEffectsDeletionAndForgedTransitions() throws Exception {
        var helper=new RatisTransactionRpcTest(); helper.root=root;
        try (var f=helper.fixture("envelopes"); var routed=new RatisRoutedClient(helper.directory(f),helper.new Controlled(f))) {
            var directory=helper.directory(f); var statuses=new RatisTransactionStatuses(routed,f.column); var initial=initial(directory);
            var validator=RatisTransactionStatuses.validator(directory.clusterId(),directory.databaseId(),f.groupId,1,new byte[32],f.column);
            var base=statuses.prepare(operation(0),null,initial).command(); validator.accept(base);
            byte[] key=base.mutations().getFirst().key();
            var scope=new TransactionScope(List.of(TransactionScope.Span.point(f.column,key)));
            var accepted=DistributedCommand.acceptedTransaction(base,new TransactionAcceptance(new ApplyPosition(1,0),0,new UUID(0,9),scope,new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode()));
            assertThrows(IllegalArgumentException.class,() -> validator.accept(accepted));
            var delete=new DistributedCommand(f.groupId,1,new byte[32],operation(1),List.of(new Mutation(Mutation.Kind.DELETE,f.column,key,null)),base.predicates());
            assertThrows(IllegalArgumentException.class,() -> validator.accept(delete));
            var extra=new DistributedCommand(f.groupId,1,new byte[32],operation(2),List.of(base.mutations().getFirst(),base.mutations().getFirst()),base.predicates());
            assertThrows(IllegalArgumentException.class,() -> validator.accept(extra));
            assertThrows(IllegalArgumentException.class,() -> statuses.prepare(operation(3),null,initial.decide(TransactionStatus.Phase.ABORT)));
            var jump=initial.prepared(a,new byte[32],new ApplyPosition(1,1)).prepared(b,new byte[32],new ApplyPosition(1,2));
            assertThrows(IllegalArgumentException.class,() -> statuses.prepare(operation(4),initial,jump));
        }
    }
}
