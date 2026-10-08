package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled native/RPC handoff; no servers or network. */
class RatisParticipantRetirementRpcTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final RatisFinalizationRpcTest owner=new RatisFinalizationRpcTest();
        final RatisFinalizationRpcTest.Fixture f;
        final ParticipantOutcome outcome;
        final TransactionStatus consumed;
        final ParticipantRetirement retirement;
        Fixture(String name,boolean commit,boolean consume) throws Exception {
            owner.root=root; f=owner.new Fixture(name,commit);
            f.data.routed.executeFinalization(f.operation);
            outcome=f.data.routed.finalizedParticipant(f.data.f.groupId,f.initial.transactionId());
            var ack=f.decision.finalized(f.data.f.groupId);
            f.home.statuses.executeRecovery(f.home.statuses.prepareRecovery(new OperationId(new UUID(71,1),0,0),f.decision,ack,outcome));
            consumed=ack.consume();
            if (consume) f.home.statuses.execute(f.home.statuses.prepare(new OperationId(new UUID(71,2),1,0),ack,consumed));
            retirement=new ParticipantRetirement(outcome.finalization(),outcome.position(),consumed);
        }
        void floor() throws IOException {
            f.data.wire.command(f.data.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(f.data.f.groupId,1,new byte[32],new OperationId(new UUID(71,3),5,0),5));
        }
        byte[] prepare(boolean fresh) throws IOException {
            return ScopedTransactionRpc.parse(f.data.wire.transaction(f.operation.descriptor(),ScopedTransactionRpc.frame(ScopedTransactionRpc.PARTICIPANT_RETIRE_PREPARE,RatisParticipantRecord.encode(retirement)),fresh),ScopedTransactionRpc.MAX_REPLY_BYTES).body();
        }
        void released() throws Exception { f.released(); assertEquals(0,f.data.f.counter("retirementSlots")); assertEquals(0,f.data.f.counter("retirementInputBytes")); }
        public void close() throws Exception { f.close(); }
    }
    @Test void lostRetirementReplyReplaysWithoutHomeOrAppendAndLateRequestsStayRetiredAfterReload() throws Exception {
        for (boolean commit:new boolean[]{false,true}) try (var x=new Fixture("retire-"+commit,commit,true)) {
            x.floor(); var f=x.f; var reads=new java.util.concurrent.atomic.AtomicInteger(); f.home.wire.afterRead=reads::incrementAndGet;
            f.data.wire.lose=true; assertThrows(IOException.class,() -> f.data.routed.retireParticipant(x.retirement));
            assertEquals(1,reads.get()); assertNull(f.data.routed.finalizedParticipant(f.data.f.groupId,f.initial.transactionId()));
            long index=f.data.wire.index; f.home.wire.afterRead=() -> { throw new AssertionError("Retired proof must avoid HOME"); };
            f.data.routed.retireParticipant(x.retirement); assertEquals(index,f.data.wire.index);
            var proof=f.data.routed.participantFence(x.retirement.identity()); assertNotNull(proof);
            assertTrue(f.data.routed.executeParticipant(f.preparation).retired());
            var retired=assertThrows(RatisParticipantRetiredException.class,() -> f.data.routed.executeFinalization(f.operation)); assertEquals(proof.identity(),retired.proof().identity());
            f.data.f.machine.takeSnapshot(); f.data.f.machine.pause(); f.data.f.machine.reinitialize(); index=f.data.wire.index;
            f.data.routed.retireParticipant(x.retirement); assertEquals(index,f.data.wire.index);
            assertThrows(RatisParticipantRetiredException.class,() -> f.data.routed.executeFinalization(f.operation));
            assertArrayEquals(commit ? new byte[]{7} : null,f.data.routed.read(f.data.f.column,new byte[]{2})); x.released();
        }
    }
    @Test void retirementNeedsBothConsumedHomeAndNativeFloorAndCancellationDropsOnlyReadyAuthority() throws Exception {
        try (var x=new Fixture("unconsumed",true,false)) {
            x.floor(); long index=x.f.data.wire.index;
            assertThrows(IOException.class,() -> x.f.data.routed.retireParticipant(x.retirement));
            assertEquals(index,x.f.data.wire.index); assertNotNull(x.f.data.routed.finalizedParticipant(x.f.data.f.groupId,x.f.initial.transactionId())); x.released();
        }
        try (var x=new Fixture("unfenced",true,true)) {
            assertThrows(IOException.class,() -> x.f.data.routed.retireParticipant(x.retirement)); x.released();
            x.floor(); assertThrows(IOException.class,() -> x.prepare(false)); x.released();
            assertEquals(0,x.prepare(true)[32]); assertEquals(1,x.f.data.f.counter("retirementSlots"));
            x.f.data.routed.cancelParticipantRetirement(x.retirement); x.released();
            assertNotNull(x.f.data.wire.propose(RatisParticipantRecord.encode(x.retirement)).getException());
            x.f.data.routed.retireParticipant(x.retirement); x.released();
        }
    }
    @Test void submittedRetirementRetainsInputThroughCancellationAndSnapshotCoverage() throws Exception {
        for (boolean covered:new boolean[]{false,true}) try (var x=new Fixture("submitted-"+covered,true,true)) {
            x.floor(); x.prepare(true); var f=x.f;
            var pending=f.data.wire.propose(RatisParticipantRecord.encode(x.retirement)); assertNull(pending.getException());
            f.data.f.machine.preAppendTransaction(pending); pending.initLogEntry(1,++f.data.wire.index); f.data.f.machine.cancelTransaction(pending);
            f.data.routed.cancelParticipantRetirement(x.retirement); assertEquals(0,f.data.f.counter("retirementSlots"));
            assertEquals(1,f.data.f.counter("protectedAdmissions")); assertTrue(f.data.f.counter("retirementInputBytes")>0);
            final org.apache.ratis.protocol.Message reply;
            if (covered) {
                f.data.f.store.applyParticipantRetirement(new ApplyPosition(1,f.data.wire.index),x.retirement);
                f.data.f.machine.takeSnapshot(); f.data.f.machine.pause(); var future=f.data.f.machine.applyTransaction(pending);
                f.data.f.machine.reinitialize(); reply=future.get(5,TimeUnit.SECONDS);
            } else reply=f.data.f.machine.applyTransaction(pending).get(5,TimeUnit.SECONDS);
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(reply.getContent().toByteArray()).status());
            assertNotNull(f.data.routed.participantFence(x.retirement.identity())); x.released();
        }
    }
    @Test void firstRetirementInNewReadyTermRetainsItsFreshHomeAuthority() throws Exception {
        try (var x=new Fixture("newterm",true,true)) {
            x.floor(); var f=x.f;
            f.data.f.machine.notifyNotLeader(List.of()); f.data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,true));
            f.data.f.machine.notifyTermIndexUpdated(2,++f.data.wire.index);
            f.data.routed.retireParticipant(x.retirement); assertNotNull(f.data.routed.participantFence(x.retirement.identity())); x.released();
        }
    }
    @Test void invalidatedHomeReadKeepsInputChargeUntilItActuallyReturns() throws Exception {
        try (var x=new Fixture("blocked",true,true); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            x.floor(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            x.f.home.wire.afterRead=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("HOME read timed out"); } catch (InterruptedException failure) { throw new AssertionError(failure); } };
            var preparing=executor.submit(() -> { x.f.data.routed.retireParticipant(x.retirement); return null; });
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); assertEquals(1,x.f.data.f.counter("retirementSlots"));
                x.f.data.f.machine.notifyNotLeader(List.of()); assertEquals(1,x.f.data.f.counter("retirementSlots")); assertTrue(x.f.data.f.counter("retirementInputBytes")>0);
                assertThrows(IOException.class,() -> x.f.data.routed.cancelParticipantRetirement(x.retirement));
            } finally { release.countDown(); }
            assertThrows(ExecutionException.class,() -> preparing.get(5,TimeUnit.SECONDS)); x.released();
        }
    }
}
