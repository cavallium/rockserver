package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled snapshot HOME and DATA retirement RPCs; no servers or network. */
class RatisSnapshotHomeRetirementRpcTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final RatisSnapshotGateRetirementRpcTest owner=new RatisSnapshotGateRetirementRpcTest();
        final RatisSnapshotGateRetirementRpcTest.Fixture data;
        final RatisSnapshotGateRpcTest.Fixture f;
        Fixture(String name) throws Exception { owner.root=root; data=owner.new Fixture(name,true,true); f=data.f; }
        void homeFloor() throws IOException { f.home.wire.command(f.home.routed.directory().ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(f.home.f.groupId,1,new byte[32],new OperationId(new UUID(101,1),4,0),4)); }
        SnapshotHomeRetirement ready() throws IOException {
            data.floor(); f.data.routed.retireSnapshotGate(data.retirement); homeFloor();
            return new SnapshotHomeRetirement(f.home.homes.identity(f.fence,f.home.routed.directory().ranges().getFirst()),data.consumed,List.of(f.data.routed.snapshotFence(f.fence,f.data.f.groupId)));
        }
        byte[] prepare(SnapshotHomeRetirement retirement,boolean fresh) throws IOException {
            return ScopedTransactionRpc.parse(f.home.wire.transaction(f.home.routed.directory().ranges().getFirst(),ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_HOME_RETIRE_PREPARE,RatisSnapshotHomeRetirementRecord.encode(retirement)),fresh),ScopedTransactionRpc.MAX_REPLY_BYTES).body();
        }
        void released() throws Exception { data.released(); f.home.f.assertReleased(); assertEquals(0,f.home.f.counter("snapshotHomeRetirementSlots")); assertEquals(0,f.home.f.counter("snapshotHomeRetirementInputBytes")); assertEquals(0,f.home.f.machine.proposalFootprintStats().candidates()); }
        public void close() throws Exception { data.close(); }
    }
    @Test void homeDeletionNeedsBothItsOwnFloorAndEveryQualifiedDataFence() throws Exception {
        try (var x=new Fixture("requirements")) {
            var fake=new SnapshotFenceProof(x.f.fence,x.f.data.f.groupId,4,new ApplyPosition(1,10000));
            var identity=x.f.home.homes.identity(x.f.fence,x.f.home.routed.directory().ranges().getFirst()); var request=new SnapshotHomeRetirement(identity,x.data.consumed,List.of(fake));
            assertThrows(IOException.class,() -> x.f.home.homes.retire(request)); x.released(); x.homeFloor();
            assertThrows(IOException.class,() -> x.f.home.homes.retire(request)); assertEquals(x.data.consumed,x.f.home.homes.read(x.f.fence.fenceId())); x.released();
            x.data.floor(); x.f.data.routed.retireSnapshotGate(x.data.retirement);
            var actual=new SnapshotHomeRetirement(identity,x.data.consumed,List.of(x.f.data.routed.snapshotFence(x.f.fence,x.f.data.f.groupId)));
            assertThrows(IOException.class,() -> x.prepare(actual,false)); x.released(); x.f.home.homes.retire(actual); assertNull(x.f.home.homes.read(x.f.fence.fenceId())); assertNotNull(x.f.home.homes.fence(identity)); x.released();
        }
    }
    @Test void lostReplyReloadAndNewerUuidFamilyDoNotRepeatDataReadsOrRecreateOldHome() throws Exception {
        try (var x=new Fixture("lost")) {
            var retirement=x.ready(); var f=x.f; f.home.wire.lose=true; assertThrows(IOException.class,() -> f.home.homes.retire(retirement)); long index=f.home.wire.index;
            f.data.wire.beforeFence=() -> { throw new AssertionError("Retired HOME must not reread DATA"); };
            f.home.homes.retire(retirement); assertEquals(index,f.home.wire.index); assertNull(f.home.homes.read(f.fence.fenceId()));
            assertEquals(CommandResult.Status.SESSION_EXPIRED,f.home.homes.execute(f.home.homes.prepare(new OperationId(new UUID(101,2),5,0),null,SnapshotFenceStatus.begin(f.fence))).status());
            f.home.f.machine.takeSnapshot(); f.home.f.machine.pause(); f.home.f.machine.reinitialize(); index=f.home.wire.index;
            f.home.homes.retire(retirement); assertEquals(index,f.home.wire.index);
            var old=f.fence; var next=new SnapshotFence(old.clusterId(), old.databaseId(), old.fenceId(), old.homeGroup(), old.homeEpoch(), new OperationId(new UUID(101,3),5,0), old.domains(), old.participants());
            assertEquals(CommandResult.Status.SUCCESS,f.home.homes.execute(f.home.homes.prepare(new OperationId(new UUID(101,4),5,0),null,SnapshotFenceStatus.begin(next))).status());
            f.home.homes.retire(retirement); assertEquals(next,f.home.homes.read(next.fenceId()).fence()); assertEquals(old,f.home.homes.fence(retirement.identity()).identity().fence()); x.released();
        }
    }
    @Test void readyCancellationSubmittedUnknownAndSnapshotCoveragePreserveActualOwnership() throws Exception {
        for (boolean covered:new boolean[]{false,true}) try (var x=new Fixture("submitted-"+covered)) {
            var retirement=x.ready(); var f=x.f; x.prepare(retirement,true); assertEquals(1,f.home.f.counter("snapshotHomeRetirementSlots")); f.home.routed.cancelSnapshotHomeRetirement(retirement); x.released();
            assertNotNull(f.home.wire.propose(RatisSnapshotHomeRetirementRecord.encode(retirement)).getException()); x.prepare(retirement,true);
            var pending=f.home.wire.propose(RatisSnapshotHomeRetirementRecord.encode(retirement)); assertNull(pending.getException()); f.home.f.machine.preAppendTransaction(pending); pending.initLogEntry(1,++f.home.wire.index); f.home.f.machine.cancelTransaction(pending);
            f.home.routed.cancelSnapshotHomeRetirement(retirement); assertEquals(1,f.home.f.counter("protectedAdmissions")); assertTrue(f.home.f.counter("snapshotHomeRetirementInputBytes")>0);
            final org.apache.ratis.protocol.Message reply;
            if (covered) { f.home.f.store.applySnapshotHomeRetirement(new ApplyPosition(1,f.home.wire.index),retirement); f.home.f.machine.takeSnapshot(); f.home.f.machine.pause(); var future=f.home.f.machine.applyTransaction(pending); f.home.f.machine.reinitialize(); reply=future.get(5,TimeUnit.SECONDS); }
            else reply=f.home.f.machine.applyTransaction(pending).get(5,TimeUnit.SECONDS);
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(reply.getContent().toByteArray()).status()); x.released();
        }
    }
    @Test void fencedAbsenceSettlesNeverAppendedInstallLeaseBeforeHomeRetirement() throws Exception {
        var owner=new RatisSnapshotGateRpcTest(); owner.root=root;
        try (var f=owner.new Fixture("unsubmitted")) {
            assertTrue(f.data.routed.authorizeSnapshotGate(f.install).drained()); assertEquals(1,f.data.f.machine.proposalGateStats().gates());
            var terminal=f.initial.abort(); f.home.transition(f.initial,terminal); var consumed=terminal.consume(); f.home.transition(terminal,consumed);
            f.data.wire.command(f.data.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(f.data.f.groupId,1,new byte[32],new OperationId(new UUID(101,10),3,0),3));
            var proof=f.data.routed.snapshotFence(f.fence,f.data.f.groupId); assertNotNull(proof);
            assertEquals(0,f.data.f.machine.proposalGateStats().gates()); assertEquals(0,f.data.f.machine.snapshotPreparationSlots()); assertEquals(0,f.data.f.machine.reservedSnapshotGateCount());
            f.home.wire.command(f.home.routed.directory().ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(f.home.f.groupId,1,new byte[32],new OperationId(new UUID(101,11),4,0),4));
            f.home.homes.retire(new SnapshotHomeRetirement(f.home.homes.identity(f.fence,f.home.routed.directory().ranges().getFirst()),consumed,List.of(proof)));
            var fresh=new DistributedCommand(f.data.f.groupId,1,new byte[32],new OperationId(new UUID(101,12),4,0),List.of(new Mutation(Mutation.Kind.PUT,f.data.f.column,new byte[]{2},new byte[]{9})));
            assertEquals(CommandResult.Status.SUCCESS,f.data.wire.command(f.data.directory.ranges().getFirst(),fresh).status()); assertArrayEquals(new byte[]{9},f.data.routed.read(f.data.f.column,new byte[]{2}));
            assertTrue(f.data.routed.executeSnapshotGate(f.install).retired()); owner.helper.released(f.data);
        }
    }
    @Test void blockedDataReadRetainsInputUntilReturnAndNewReadyTermCanAcquireFreshAuthority() throws Exception {
        try (var x=new Fixture("blocked"); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var retirement=x.ready(); var f=x.f; var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            f.data.wire.beforeFence=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("DATA read timed out"); } catch (InterruptedException failure) { throw new AssertionError(failure); } };
            var preparing=executor.submit(() -> { f.home.homes.retire(retirement); return null; });
            try { assertTrue(entered.await(5,TimeUnit.SECONDS)); assertEquals(1,f.home.f.counter("snapshotHomeRetirementSlots")); f.home.f.machine.notifyNotLeader(List.of()); assertEquals(1,f.home.f.counter("snapshotHomeRetirementSlots")); assertTrue(f.home.f.counter("snapshotHomeRetirementInputBytes")>0); assertThrows(IOException.class,() -> f.home.routed.cancelSnapshotHomeRetirement(retirement)); }
            finally { release.countDown(); }
            assertThrows(ExecutionException.class,() -> preparing.get(5,TimeUnit.SECONDS)); x.released(); f.data.wire.beforeFence=null;
            f.home.f.leadership.set(new RatisRangeStateMachine.Leadership(2,true)); f.home.f.machine.notifyTermIndexUpdated(2,++f.home.wire.index); f.home.homes.retire(retirement); x.released();
        }
    }
}
