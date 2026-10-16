package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisAcceptedProjectionClientTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisCapturedTransactionPlanTest helper() { var h=new RatisCapturedTransactionPlanTest(); h.root=root; return h; }
    RatisAtomicClient.Identity identity(RatisCapturedTransactionPlanTest.Captured x) { return new RatisAtomicClient.Identity(new UUID(190,1),x.operation(100),x.operation(110)); }
    RatisTransactionPlan.Accepted accepted(RatisCapturedTransactionPlanTest.Captured x,RatisAtomicClient.Identity identity) throws Exception {
        var plan=x.plan();
        return plan.acceptCaptured(identity.transaction(),x.accept(plan),Map.of(x.left,x.operation(110),x.right,x.operation(111)),Map.of(x.left,x.operation(174),x.right,x.operation(175)));
    }
    @Test void asyncAcceptedProjectionCommitsOriginalHandlesWithoutOpeningOrStagingAgain() throws Exception {
        var h=helper();
        try (var x=h.new Captured("accepted-client"); var client=x.f.client()) {
            var identity=identity(x); var accepted=accepted(x,identity); int captures=x.f.captureRequests.get();
            try (var operation=client.executeAccepted(x.f.root.resolve("accepted.bin"),identity,accepted,RequestContext.batch())) {
                var result=operation.completion().get(10,TimeUnit.SECONDS);
                assertEquals(CommandResult.Status.SUCCESS,result.status()); assertArrayEquals(new byte[]{0},result.value(0)); assertTrue(operation.terminalReady()); assertFalse(operation.busy());
                assertTrue(operation.snapshot().observedHome().fullyFinalized()); assertFalse(operation.snapshot().observedHome().consumed());
                assertEquals(0,x.f.opens.get()); assertEquals(captures,x.f.captureRequests.get()); assertArrayEquals(new byte[]{7},x.f.get(71,RatisAtomicClientTest.key(2)));
            }
            assertEquals(0,client.stats().operations());
        }
    }
    @Test void lostPrepareReplyRecoversTheSameAcceptedJournalAndNativeVotes() throws Exception {
        var h=helper();
        try (var x=h.new Captured("accepted-reopen"); var client=x.f.client()) {
            var identity=identity(x); var accepted=accepted(x,identity); Path path=x.f.root.resolve("accepted-reopen.bin"); x.f.losePrepareGroup=x.right;
            try (var operation=client.executeAccepted(path,identity,accepted,RequestContext.batch())) {
                assertThrows(ExecutionException.class,() -> operation.completion().get(10,TimeUnit.SECONDS)); assertFalse(operation.terminalReady());
            }
            int captures=x.f.captureRequests.get();
            try (var operation=client.reopen(path,RequestContext.batch())) {
                assertEquals(CommandResult.Status.SUCCESS,operation.completion().get(10,TimeUnit.SECONDS).status());
                for (var participant:operation.snapshot().participants()) {
                    var original=x.cuts.participant(participant.intent().route().groupId()).certifiedHandle();
                    assertEquals(original.id(),participant.intent().captureId()); assertEquals(original.position(),participant.intent().acceptedBusiness().acceptance().snapshot());
                }
                assertEquals(0,x.f.opens.get()); assertEquals(captures,x.f.captureRequests.get());
            }
        }
    }
    @Test void acceptedControlBlockMismatchFailsBeforeAdmissionOrRemoteEffects() throws Exception {
        var h=helper();
        try (var x=h.new Captured("accepted-validation"); var client=x.f.client()) {
            var identity=identity(x); var accepted=accepted(x,identity); int writes=x.f.writes(),calls=x.f.rpcs.get();
            var changed=new RatisAtomicClient.Identity(identity.transaction(),identity.business(),x.operation(400));
            assertThrows(IllegalArgumentException.class,() -> client.executeAccepted(x.f.root.resolve("invalid.bin"),changed,accepted,RequestContext.batch()));
            assertEquals(0,client.stats().operations()); assertEquals(writes,x.f.writes()); assertEquals(calls,x.f.rpcs.get());
        }
    }
    @Test void oversizedAggregateAcceptedResultIsRejectedBeforeBecomingAnUnknownOperation() throws Exception {
        var h=helper();
        try (var x=h.new Captured("accepted-result-budget"); var client=x.f.client()) {
            var identity=identity(x);
            var plan=RatisTransactionPlan.captured(List.of(x.f.directory),identity.business(),List.of(
                    new Mutation(Mutation.Kind.PUT,x.column,new byte[]{2},new byte[]{7},Mutation.ReturnMode.PREVIOUS),
                    new Mutation(Mutation.Kind.PUT,x.column,new byte[]{(byte)200},new byte[]{7},Mutation.ReturnMode.PREVIOUS)),List.of(),x.cuts);
            var commands=new HashMap<UUID,DistributedCommand>();
            for (var part:plan.partitions()) {
                var original=x.cuts.participant(part.route().groupId()).certifiedHandle();
                var audit=new TransactionAcceptance(original.position(),original.generation(),original.owner(),part.scope(),new CommandResult(CommandResult.Status.SUCCESS,List.of(new byte[1100*1024])).encode());
                commands.put(part.route().groupId(),DistributedCommand.acceptedTransaction(part.command(),audit));
            }
            var accepted=plan.acceptCaptured(identity.transaction(),commands,Map.of(x.left,x.operation(110),x.right,x.operation(111)),Map.of(x.left,x.operation(174),x.right,x.operation(175)));
            int writes=x.f.writes(),calls=x.f.rpcs.get();
            assertThrows(IOException.class,() -> client.executeAccepted(x.f.root.resolve("oversized.bin"),identity,accepted,RequestContext.batch()));
            assertEquals(0,client.stats().operations()); assertEquals(writes,x.f.writes()); assertEquals(calls,x.f.rpcs.get());
        }
    }
}
