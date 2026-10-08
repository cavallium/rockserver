package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** Native admission ownership with controlled Ratis contexts; no append/quorum/server-process acceptance. */
class RatisProposalFootprintsTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    TransactionScope scope(RatisRetentionAdmissionTest.Fixture f,int key) { return new TransactionScope(List.of(TransactionScope.Span.point(f.column,new byte[]{(byte)key}))); }
    DistributedCommand put(RatisRetentionAdmissionTest.Fixture f,long sequence,int key) {
        return new DistributedCommand(f.groupId,1,new byte[32],new OperationId(f.session,1,sequence),List.of(new Mutation(Mutation.Kind.PUT,f.column,new byte[]{(byte)key},new byte[]{7})));
    }
    RatisRetentionAdmissionTest.Fixture fixture(String name) throws Exception { return new RatisRetentionAdmissionTest.Fixture(root.resolve(name),RatisRangeStateMachine.RetentionLimits.defaults()); }
    @Test void ordinaryOverlapPipelinesExclusiveScopesBlockOnlyIntersectingNewEffects() throws Exception {
        try (var f=fixture("pipeline")) {
            var a=f.propose(put(f,0,0)); var b=f.propose(put(f,1,0)); assertNull(a.getException()); assertNull(b.getException());
            assertEquals(2,f.machine.proposalFootprintStats().ordinary()); assertNull(f.machine.reserveProposalScope(scope(f,0)));
            try (var disjoint=f.machine.reserveProposalScope(scope(f,1))) {
                assertNotNull(disjoint); assertTrue(disjoint.active());
                assertNotNull(f.propose(put(f,2,1)).getException()); var independent=f.propose(put(f,3,2)); assertNull(independent.getException());
                f.machine.cancelTransaction(independent); f.machine.cancelTransaction(a); assertNull(f.machine.reserveProposalScope(scope(f,0)));
                assertEquals(CommandResult.Status.SUCCESS,f.apply(b,1));
                try (var resolved=f.machine.reserveProposalScope(scope(f,0))) {
                    assertNotNull(resolved); var retry=f.propose(put(f,1,0)); assertNull(retry.getException(),"Saved outcomes carry no future business-write footprint");
                    assertEquals(CommandResult.Status.SUCCESS,f.apply(retry,2)); assertTrue(resolved.active()); assertEquals(0,f.machine.proposalFootprintStats().ordinary());
                }
            }
            f.assertReleased(); assertEquals(new ProposalFootprints.Stats(0,0,0,0),f.machine.proposalFootprintStats());
        }
    }
    @Test void entryBearingCancellationRetainsReceiptAndWriteOwnershipUntilNativeApply() throws Exception {
        try (var f=new RatisRetentionAdmissionTest.Fixture(root.resolve("ambiguous"),new RatisRangeStateMachine.RetentionLimits(1,92,1,92))) {
            var original=f.propose(f.put(0)); var duplicate=f.propose(f.put(0)); f.machine.preAppendTransaction(original); original.initLogEntry(1,1);
            f.machine.cancelTransaction(original); f.machine.cancelTransaction(duplicate);
            assertEquals(1,f.counter("reservedReceiptCount")); assertEquals(1,f.counter("dataAdmissions")); assertEquals(1,f.machine.proposalFootprintStats().ordinary());
            assertNotNull(f.propose(f.put(1)).getException(),"Possibly appended outcomes cannot donate receipt capacity"); assertNull(f.machine.reserveProposalScope(scope(f,0)));
            var retry=f.propose(f.put(0)); assertNull(retry.getException()); assertEquals(1,f.machine.proposalFootprintStats().ordinary()); f.machine.cancelTransaction(retry);
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.machine.applyTransaction(original).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status());
            assertEquals(1,f.store.receiptUsage().retainedCount()); f.assertReleased();
            try (var scope=f.machine.reserveProposalScope(scope(f,0))) { assertNotNull(scope); }
            f.machine.cancelTransaction(original); assertEquals(new ProposalFootprints.Stats(0,0,0,0),f.machine.proposalFootprintStats());
        }
    }
    @Test void sameTermGenerationDoesNotClearOrphansButReadyNewTermBarrierDoes() throws Exception {
        try (var f=fixture("term")) {
            var original=f.propose(f.put(0)); f.machine.preAppendTransaction(original); original.initLogEntry(1,1); f.machine.cancelTransaction(original);
            f.machine.notifyNotLeader(List.of()); assertNull(f.machine.reserveProposalScope(scope(f,0))); assertEquals(1,f.counter("reservedReceiptCount"));
            assertNotNull(f.propose(f.put(0)).getException(),"Same-term earlier generation still owns its uncertain append");
            f.leadership.set(new RatisRangeStateMachine.Leadership(2,false)); f.machine.notifyTermIndexUpdated(2,1);
            assertThrows(java.io.IOException.class,() -> f.machine.reserveProposalScope(scope(f,0))); assertEquals(1,f.machine.proposalFootprintStats().ordinary());
            f.leadership.set(new RatisRangeStateMachine.Leadership(2,true));
            try (var reserved=f.machine.reserveProposalScope(scope(f,0))) { assertNotNull(reserved); assertEquals(2,reserved.term()); assertEquals(0,f.counter("reservedReceiptCount")); }
            var replacement=f.propose(f.put(0)); assertNull(replacement.getException()); f.machine.cancelTransaction(original);
            assertEquals(1,f.machine.proposalFootprintStats().ordinary()); assertNull(f.machine.reserveProposalScope(scope(f,0)));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(replacement,2)); f.assertReleased(); assertEquals(0,f.machine.proposalFootprintStats().ordinary());
        }
    }
    @Test void snapshotRecoveryRequiresAnActualSavedOutcomeBeforeSettlingAnOrphan() throws Exception {
        try (var f=fixture("uncovered")) {
            var original=f.propose(f.put(0)); f.machine.preAppendTransaction(original); original.initLogEntry(1,1); f.machine.cancelTransaction(original);
            assertEquals(0,f.machine.takeSnapshot()); f.machine.pause(); f.machine.reinitialize();
            assertEquals(1,f.machine.proposalFootprintStats().ordinary()); assertEquals(1,f.counter("reservedReceiptCount")); assertNull(f.machine.reserveProposalScope(scope(f,0)));
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.machine.applyTransaction(original).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status()); f.assertReleased();
        }
        try (var f=fixture("covered")) {
            var original=f.propose(f.put(0)); f.machine.preAppendTransaction(original); original.initLogEntry(1,1); f.machine.cancelTransaction(original);
            f.store.apply(new ApplyPosition(1,1),f.put(0)); // A verified snapshot can cover a committed entry whose local RPC owner was lost.
            assertEquals(1,f.machine.takeSnapshot()); f.machine.pause(); f.machine.reinitialize(); f.assertReleased();
            assertEquals(0,f.machine.proposalFootprintStats().ordinary()); try (var reserved=f.machine.reserveProposalScope(scope(f,0))) { assertNotNull(reserved); }
        }
    }
    @Test void recoveredOutcomeSettlesFootprintBeforeItsLiveRpcOwnerReturns() throws Exception {
        try (var f=fixture("covered-live-owner")) {
            var original=f.propose(f.put(0)); f.machine.preAppendTransaction(original); original.initLogEntry(1,1);
            f.store.apply(new ApplyPosition(1,1),f.put(0));
            assertEquals(1,f.machine.takeSnapshot()); f.machine.pause(); f.machine.reinitialize();
            assertEquals(0,f.counter("reservedReceiptCount")); assertEquals(0,f.machine.proposalFootprintStats().ordinary());
            assertEquals(1,f.counter("dataAdmissions"),"A live callback retains its payload owner until release");
            try (var reserved=f.machine.reserveProposalScope(scope(f,0))) { assertNotNull(reserved); }
            f.machine.cancelTransaction(original); f.assertReleased(); f.machine.cancelTransaction(original); f.assertReleased();
        }
    }
    @Test void staleCandidateCloseCannotReleaseReplacementGenerationOwnership() throws Exception {
        try (var f=fixture("candidate-generation")) {
            var old=f.machine.reserveProposalScope(scope(f,0)); assertNotNull(old); f.machine.notifyNotLeader(List.of()); assertFalse(old.active());
            try (var replacement=f.machine.reserveProposalScope(scope(f,0))) {
                assertNotNull(replacement); old.close(); old.close(); assertTrue(replacement.active()); assertEquals(1,f.machine.proposalFootprintStats().candidates());
                assertNotNull(f.propose(f.put(0)).getException());
            }
            var admitted=f.propose(f.put(0)); assertNull(admitted.getException()); f.machine.cancelTransaction(admitted); f.assertReleased();
        }
    }
    @Test void differentLogicalMembersShareOnePhysicalBucketProposalScope() throws Exception {
        var schema=new RatisBucketSessionTest().schema(true);
        try (var f=new RatisSessionPropagationTest.Fixture(root.resolve("bucket"),true,schema.digest(),schema)) {
            var client=new RatisLogicalClient(schema,f.routed,f.ledger,f.ledger.grants().getFirst(),RatisSessionClient.Limits.defaults());
            var keys=new RatisBucketSessionTest();
            var a=client.preparePut(71,keys.key(1),Buf.wrap(new byte[]{1}),Mutation.ReturnMode.NONE);
            var b=client.preparePut(71,keys.key(2),Buf.wrap(new byte[]{2}),Mutation.ReturnMode.NONE);
            f.routed.execute(client.prepareBinding(f.catalogue.epochs,new OperationId(f.outer.maintenance,1,100),a,-1));
            var group=a.descriptor().groupId(); var machine=f.machines.get(group); var raft=f.raftGroups.get(group); var peer=raft.getPeers().iterator().next();
            var first=propose(machine,raft,peer,a.command()); var second=propose(machine,raft,peer,b.command()); assertNull(first.getException()); assertNull(second.getException());
            var physical=new TransactionScope(List.of(TransactionScope.Span.point(f.outer.data,a.command().mutations().getFirst().key())));
            assertNull(machine.reserveProposalScope(physical)); machine.cancelTransaction(first); assertNull(machine.reserveProposalScope(physical)); machine.cancelTransaction(second);
            try (var reservation=machine.reserveProposalScope(physical)) { assertNotNull(reservation); }
            assertEquals(0,machine.proposalFootprintStats().ordinary());
        }
    }
    static TransactionContext propose(RatisRangeStateMachine machine,RaftGroup group,RaftPeer peer,DistributedCommand command) throws Exception {
        return machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(group.getGroupId()).setCallId(1)
                .setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command)))).build());
    }
}
