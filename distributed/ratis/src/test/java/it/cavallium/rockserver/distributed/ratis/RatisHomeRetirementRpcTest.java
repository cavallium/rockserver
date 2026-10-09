package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled native and fixed DATA-reader RPCs; no servers or network. */
class RatisHomeRetirementRpcTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final RatisParticipantRetirementRpcTest owner=new RatisParticipantRetirementRpcTest();
        final RatisParticipantRetirementRpcTest.Fixture participant;
        final RatisFinalizationRpcTest.Fixture f;
        Fixture(String name,boolean commit) throws Exception { owner.root=root; participant=owner.new Fixture(name,commit,true); f=participant.f; }
        void floor() throws IOException {
            f.home.wire.command(f.home.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(f.home.f.groupId,1,new byte[32],new OperationId(new UUID(81,1),5,0),5));
        }
        HomeRetirement retirement() throws IOException {
            var identity=f.home.statuses.identity(participant.consumed,f.home.directory.ranges().getFirst());
            return new HomeRetirement(identity,participant.consumed,List.of(f.data.routed.participantFence(participant.retirement.identity())));
        }
        HomeRetirement ready() throws IOException { participant.floor(); f.data.routed.retireParticipant(participant.retirement); floor(); return retirement(); }
        byte[] prepare(HomeRetirement retirement,boolean fresh) throws IOException {
            return ScopedTransactionRpc.parse(f.home.wire.transaction(f.home.directory.ranges().getFirst(),ScopedTransactionRpc.frame(ScopedTransactionRpc.HOME_RETIRE_PREPARE,RatisHomeRetirementRecord.encode(retirement)),fresh),ScopedTransactionRpc.MAX_REPLY_BYTES).body();
        }
        void released() throws Exception { participant.released(); f.home.f.assertReleased(); assertEquals(0,f.home.f.counter("homeRetirementSlots")); assertEquals(0,f.home.f.counter("homeRetirementInputBytes")); assertEquals(0,f.home.f.machine.proposalFootprintStats().candidates()); }
        public void close() throws Exception { participant.close(); }
    }
    @Test void consumedHomeRequiresQualifiedDataRetirementAndItsOwnBusinessFloor() throws Exception {
        try (var x=new Fixture("requirements",true)) {
            var fake=new ParticipantFenceProof(x.participant.retirement.identity(),5,new ApplyPosition(1,10000));
            var request=new HomeRetirement(x.f.home.statuses.identity(x.participant.consumed,x.f.home.directory.ranges().getFirst()),x.participant.consumed,List.of(fake));
            assertThrows(IOException.class,() -> x.f.home.statuses.retire(request)); x.released();
            x.floor(); assertThrows(IOException.class,() -> x.f.home.statuses.retire(request)); x.released();
            assertEquals(x.participant.consumed,x.f.home.statuses.read(x.f.initial.transactionId()));
            x.participant.floor(); x.f.data.routed.retireParticipant(x.participant.retirement);
            var actual=x.retirement(); assertThrows(IOException.class,() -> x.prepare(actual,false)); x.released();
            x.f.home.statuses.retire(actual); assertNull(x.f.home.statuses.read(x.f.initial.transactionId())); assertNotNull(x.f.home.statuses.fence(actual.identity())); x.released();
        }
    }
    @Test void lostReplyAndReloadDoNotReadDataAgainOrRecreateOldHomeWithFreshControl() throws Exception {
        for (boolean commit:new boolean[]{false,true}) try (var x=new Fixture("retry-"+commit,commit)) {
            var retirement=x.ready(); var f=x.f;
            f.home.wire.lose=true; assertThrows(IOException.class,() -> f.home.statuses.retire(retirement)); long index=f.home.wire.index;
            f.data.wire.beforeFence=() -> { throw new AssertionError("Retained HOME retirement must not query DATA again"); };
            f.home.statuses.retire(retirement); assertEquals(index,f.home.wire.index);
            var recovery=f.home.statuses.prepareRecovery(new OperationId(new UUID(71,1),0,0),f.decision,f.decision.finalized(f.data.f.groupId),x.participant.outcome);
            var retired=assertThrows(RatisHomeRetiredException.class,() -> f.home.statuses.executeRecovery(recovery)); assertEquals(retirement.identity(),retired.proof().identity());
            assertThrows(RatisHomeRetiredException.class,() -> f.home.statuses.recoveryResult(recovery)); assertEquals(index,f.home.wire.index);
            assertEquals(CommandResult.Status.SESSION_EXPIRED,f.home.statuses.execute(f.home.statuses.prepare(new OperationId(new UUID(81,2),6,0),null,f.initial)).status());
            assertNull(f.home.statuses.read(f.initial.transactionId()));
            f.home.f.machine.takeSnapshot(); f.home.f.machine.pause(); f.home.f.machine.reinitialize(); index=f.home.wire.index;
            f.home.statuses.retire(retirement); assertEquals(index,f.home.wire.index);
            var initial=f.initial; var next=TransactionStatus.begin(initial.clusterId(),initial.databaseId(),initial.domainId(),initial.transactionId(),new OperationId(new UUID(81,3),6,0),initial.schemaEpoch(),initial.schemaDigest(),initial.planDigest(),initial.asInitial().participants());
            assertEquals(CommandResult.Status.SUCCESS,f.home.statuses.execute(f.home.statuses.prepare(new OperationId(new UUID(81,4),6,0),null,next)).status());
            f.home.statuses.retire(retirement); assertEquals(next,f.home.statuses.read(next.transactionId()));
            assertThrows(RatisHomeRetiredException.class,() -> f.home.statuses.executeRecovery(recovery)); x.released();
        }
    }
    @Test void orphanConflictCleanupNeedsNoDataReaderOrInventedDecision() throws Exception {
        var helper=new RatisParticipantPrepareTest(); helper.root=root;
        try (var home=helper.new Node("orphan",true)) {
            UUID absent=new UUID(82,1); var initial=TransactionStatus.begin(helper.cluster,helper.database,new UUID(82,2),new UUID(82,3),new OperationId(new UUID(82,4),1,0),1,new byte[32],new byte[32],
                    List.of(new TransactionStatus.Participant(absent,absent,1,new byte[32],new byte[32],null,false)));
            var frozen=home.statuses.prepareRecovery(new OperationId(new UUID(82,5),0,0),initial,initial.decide(TransactionStatus.Phase.ABORT),null);
            assertEquals(CommandResult.Status.CONFLICT,home.statuses.executeRecovery(frozen).result().status()); assertEquals(1,home.f.store.homeRecoveryCapacity().count());
            home.wire.command(home.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(home.f.groupId,1,new byte[32],new OperationId(new UUID(82,6),3,0),3));
            var request=new HomeRetirement(home.statuses.identity(initial,home.directory.ranges().getFirst()),null,List.of());
            home.statuses.retire(request); assertEquals(0,home.f.store.homeRecoveryCapacity().count()); assertNull(home.statuses.read(initial.transactionId()));
            assertThrows(RatisHomeRetiredException.class,() -> home.statuses.executeRecovery(frozen)); home.f.assertReleased();
        }
    }
    @Test void readyCancellationSubmittedUnknownAndSnapshotCoveragePreserveOwnership() throws Exception {
        for (boolean covered:new boolean[]{false,true}) try (var x=new Fixture("submitted-"+covered,true)) {
            var retirement=x.ready(); var f=x.f; x.prepare(retirement,true); assertEquals(1,f.home.f.counter("homeRetirementSlots"));
            f.home.routed.cancelHomeRetirement(retirement); x.released(); assertNotNull(f.home.wire.propose(RatisHomeRetirementRecord.encode(retirement)).getException());
            x.prepare(retirement,true); var pending=f.home.wire.propose(RatisHomeRetirementRecord.encode(retirement)); assertNull(pending.getException());
            f.home.f.machine.preAppendTransaction(pending); pending.initLogEntry(1,++f.home.wire.index); f.home.f.machine.cancelTransaction(pending);
            f.home.routed.cancelHomeRetirement(retirement); assertEquals(1,f.home.f.counter("protectedAdmissions")); assertTrue(f.home.f.counter("homeRetirementInputBytes")>0);
            final org.apache.ratis.protocol.Message reply;
            if (covered) {
                f.home.f.store.applyHomeRetirement(new ApplyPosition(1,f.home.wire.index),retirement); f.home.f.machine.takeSnapshot(); f.home.f.machine.pause();
                var future=f.home.f.machine.applyTransaction(pending); f.home.f.machine.reinitialize(); reply=future.get(5,TimeUnit.SECONDS);
            } else reply=f.home.f.machine.applyTransaction(pending).get(5,TimeUnit.SECONDS);
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(reply.getContent().toByteArray()).status()); x.released();
        }
    }
    @Test void blockedDataProofKeepsChargeUntilReturnAndNewReadyTermCanAcquireAuthority() throws Exception {
        try (var x=new Fixture("blocked",true); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var retirement=x.ready(); var f=x.f; var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            f.data.wire.beforeFence=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("DATA proof timed out"); } catch (InterruptedException failure) { throw new AssertionError(failure); } };
            var preparing=executor.submit(() -> { f.home.statuses.retire(retirement); return null; });
            try { assertTrue(entered.await(5,TimeUnit.SECONDS)); assertEquals(1,f.home.f.counter("homeRetirementSlots")); f.home.f.machine.notifyNotLeader(List.of()); assertEquals(1,f.home.f.counter("homeRetirementSlots")); assertTrue(f.home.f.counter("homeRetirementInputBytes")>0); assertThrows(IOException.class,() -> f.home.routed.cancelHomeRetirement(retirement)); }
            finally { release.countDown(); }
            assertThrows(ExecutionException.class,() -> preparing.get(5,TimeUnit.SECONDS)); x.released(); f.data.wire.beforeFence=null;
            f.home.f.leadership.set(new RatisRangeStateMachine.Leadership(2,true)); f.home.f.machine.notifyTermIndexUpdated(2,++f.home.wire.index);
            f.home.statuses.retire(retirement); x.released();
        }
    }
}
