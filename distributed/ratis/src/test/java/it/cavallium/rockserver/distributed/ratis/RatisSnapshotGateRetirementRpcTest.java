package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled native/RPC snapshot retirement; no server processes or network. */
class RatisSnapshotGateRetirementRpcTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final RatisSnapshotGateRpcTest owner=new RatisSnapshotGateRpcTest();
        final RatisSnapshotGateRpcTest.Fixture f;
        final SnapshotFenceStatus terminal,consumed;
        final RatisRoutedClient.PreparedSnapshotOperation release;
        final SnapshotGateRetirement retirement;
        Fixture(String name,boolean install,boolean consume) throws Exception {
            owner.root=root; f=owner.new Fixture(name);
            if (install) f.data.routed.executeSnapshotGate(f.install);
            release=f.abort(); terminal=release.request().terminal();
            var receipt=f.data.routed.executeSnapshotGate(release); consumed=terminal.consume();
            if (consume) f.home.transition(terminal,consumed);
            retirement=new SnapshotGateRetirement(consumed,f.data.f.groupId,receipt.position());
        }
        void floor() throws IOException { f.data.wire.command(f.data.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(f.data.f.groupId,1,new byte[32],new OperationId(new UUID(91,1),3,0),3)); }
        byte[] prepare(boolean fresh) throws IOException { return ScopedTransactionRpc.parse(f.data.wire.transaction(f.install.descriptor(),ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_RETIRE_PREPARE,RatisSnapshotRetirementRecord.encode(retirement)),fresh),ScopedTransactionRpc.MAX_REPLY_BYTES).body(); }
        void released() throws Exception { owner.helper.released(f.data); assertEquals(0,f.data.f.counter("snapshotRetirementSlots")); assertEquals(0,f.data.f.counter("snapshotRetirementInputBytes")); }
        public void close() throws Exception { f.close(); }
    }
    @Test void consumedAuthorityAndStrictDataFloorAreRequiredBeforeDeletion() throws Exception {
        try (var x=new Fixture("unconsumed",true,false)) {
            x.floor(); assertThrows(IOException.class,() -> x.f.data.routed.retireSnapshotGate(x.retirement)); assertNotNull(x.f.data.f.store.snapshotGate(x.f.fence.fenceId())); x.released();
        }
        try (var x=new Fixture("unfenced",true,true)) {
            assertThrows(IOException.class,() -> x.f.data.routed.retireSnapshotGate(x.retirement)); x.released();
            x.floor(); assertThrows(IOException.class,() -> x.prepare(false)); x.released();
            x.f.data.routed.retireSnapshotGate(x.retirement); assertNull(x.f.data.f.store.snapshotGate(x.f.fence.fenceId())); x.released();
        }
    }
    @Test void lostGcReplyReloadAndLateInstallReleaseUseRetirementProofWithoutFakeReceiptsOrHomeReads() throws Exception {
        for (boolean installed:new boolean[]{false,true}) try (var x=new Fixture("retry-"+installed,installed,true)) {
            x.floor(); var f=x.f; f.data.wire.lose=true; assertThrows(IOException.class,() -> f.data.routed.retireSnapshotGate(x.retirement)); long index=f.data.wire.index;
            f.home.wire.afterRead=() -> { throw new AssertionError("Retired gate must not reread HOME"); };
            f.data.routed.retireSnapshotGate(x.retirement); assertEquals(index,f.data.wire.index);
            var proof=f.data.routed.snapshotFence(f.fence,f.data.f.groupId); assertNotNull(proof);
            for (var operation:List.of(f.install,x.release)) { var result=f.data.routed.executeSnapshotGate(operation); assertTrue(result.retired()); assertFalse(result.suppressed()); assertNull(result.position()); assertEquals(proof.fence(),result.retirement().fence()); }
            assertTrue(f.data.routed.snapshotReadiness(f.fence,f.data.f.groupId).retired()); assertFalse(f.data.routed.helpSnapshotFence(f.fence,f.data.f.groupId)); assertEquals(index,f.data.wire.index);
            assertThrows(RatisSnapshotLostException.class,() -> f.data.f.machine.captureSnapshotPin(f.home.homes,f.fence,f.data.f.machine.snapshotCaptureOwner(),UUID.randomUUID(),Duration.ofSeconds(1)));
            assertEquals(0,f.data.f.machine.snapshotPinStats().entries());
            f.data.f.machine.takeSnapshot(); f.data.f.machine.pause(); f.data.f.machine.reinitialize(); index=f.data.wire.index;
            f.data.routed.retireSnapshotGate(x.retirement); assertEquals(index,f.data.wire.index); assertTrue(f.data.routed.executeSnapshotGate(x.release).retired()); x.released();
        }
    }
    @Test void readyCancellationUnknownAppendAndSnapshotCoverageRetainInputUntilNativeSettlement() throws Exception {
        for (boolean covered:new boolean[]{false,true}) try (var x=new Fixture("submitted-"+covered,true,true)) {
            x.floor(); var f=x.f; x.prepare(true); assertEquals(1,f.data.f.counter("snapshotRetirementSlots"));
            f.data.routed.cancelSnapshotRetirement(x.retirement); x.released(); assertNotNull(f.data.wire.propose(RatisSnapshotRetirementRecord.encode(x.retirement)).getException());
            x.prepare(true); var pending=f.data.wire.propose(RatisSnapshotRetirementRecord.encode(x.retirement)); assertNull(pending.getException()); f.data.f.machine.preAppendTransaction(pending); pending.initLogEntry(1,++f.data.wire.index); f.data.f.machine.cancelTransaction(pending);
            f.data.routed.cancelSnapshotRetirement(x.retirement); assertEquals(1,f.data.f.counter("protectedAdmissions")); assertTrue(f.data.f.counter("snapshotRetirementInputBytes")>0);
            final org.apache.ratis.protocol.Message reply;
            if (covered) { f.data.f.store.applySnapshotGateRetirement(new ApplyPosition(1,f.data.wire.index),x.retirement); f.data.f.machine.takeSnapshot(); f.data.f.machine.pause(); var future=f.data.f.machine.applyTransaction(pending); f.data.f.machine.reinitialize(); reply=future.get(5,TimeUnit.SECONDS); }
            else reply=f.data.f.machine.applyTransaction(pending).get(5,TimeUnit.SECONDS);
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(reply.getContent().toByteArray()).status()); x.released();
        }
    }
    @Test void gcBetweenHomeReadyReadAndNativeCaptureReturnsLostWithoutAllocatingAReplacement() throws Exception {
        var pins=new RatisSnapshotPinsTest(); pins.root=root;
        try (var f=pins.new Fixture("capture-race"); var nativePin=f.data.f.machine.captureSnapshotGate(f.home.homes,f.fence)) {
            var token=new SnapshotFenceStatus.Capture(f.owner,UUID.randomUUID(),nativePin.position());
            f.home.wire.afterRead=() -> {
                f.home.wire.afterRead=null;
                try {
                    f.complete(token); var terminal=f.home.homes.read(f.fence.fenceId()); var consumed=terminal.consume(); f.home.transition(terminal,consumed);
                    var released=f.data.f.store.snapshotGate(f.fence.fenceId()).released();
                    f.data.wire.command(f.data.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(f.data.f.groupId,1,new byte[32],new OperationId(new UUID(91,2),3,0),3));
                    f.data.f.store.applySnapshotGateRetirement(new ApplyPosition(1,++f.data.wire.index),new SnapshotGateRetirement(consumed,f.data.f.groupId,released));
                } catch (Exception failure) { throw new AssertionError(failure); }
            };
            assertThrows(RatisSnapshotLostException.class,() -> f.data.f.machine.captureSnapshotPin(f.home.homes,f.fence,f.owner,UUID.randomUUID(),Duration.ofSeconds(10)));
            assertEquals(0,f.data.f.machine.snapshotPinStats().pins()); assertEquals(0,f.data.f.machine.snapshotPinStats().entries());
        }
    }
    @Test void invalidatedBlockedHomeReadRetainsChargeAndNewReadyTermCanRetry() throws Exception {
        try (var x=new Fixture("blocked",true,true); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            x.floor(); var f=x.f; var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            f.home.wire.afterRead=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("HOME read timed out"); } catch (InterruptedException failure) { throw new AssertionError(failure); } };
            var preparing=executor.submit(() -> { f.data.routed.retireSnapshotGate(x.retirement); return null; });
            try { assertTrue(entered.await(5,TimeUnit.SECONDS)); assertEquals(1,f.data.f.counter("snapshotRetirementSlots")); f.data.f.machine.notifyNotLeader(List.of()); assertEquals(1,f.data.f.counter("snapshotRetirementSlots")); assertTrue(f.data.f.counter("snapshotRetirementInputBytes")>0); assertThrows(IOException.class,() -> f.data.routed.cancelSnapshotRetirement(x.retirement)); }
            finally { release.countDown(); }
            assertThrows(ExecutionException.class,() -> preparing.get(5,TimeUnit.SECONDS)); x.released(); f.home.wire.afterRead=null;
            f.data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,true)); f.data.f.machine.notifyTermIndexUpdated(2,++f.data.wire.index); f.data.routed.retireSnapshotGate(x.retirement); x.released();
        }
    }
}
