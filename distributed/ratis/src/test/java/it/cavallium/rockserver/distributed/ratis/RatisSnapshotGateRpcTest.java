package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled qualified remote gates and native recovery, without Ratis server processes or network. */
class RatisSnapshotGateRpcTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSnapshotAdmissionTest helper=new RatisSnapshotAdmissionTest();
    byte[] helpRequest(SnapshotFence fence) { var body=ByteBuffer.allocate(48); ScopedTransactionRpc.snapshotBinding(body,ScopedTransactionRpc.SnapshotBinding.of(fence)); return body.array(); }
    final class Fixture implements AutoCloseable {
        final RatisSnapshotAdmissionTest.Home home;
        final RatisParticipantPrepareTest.Node data;
        final SnapshotFence fence;
        final SnapshotFenceStatus initial;
        final RatisRoutedClient.PreparedSnapshotOperation install;
        Fixture(String name) throws Exception {
            helper.root=root; helper.helper.root=root; helper.helper.snapshotRecovery=(group,prepared) -> false;
            home=helper.new Home(name+"-home"); data=helper.helper.new Node(name+"-data",false,null,home.homes,System::nanoTime);
            fence=helper.fence(home,data,1,helper.scope(data,1,4)); initial=home.register(fence); install=data.routed.prepareSnapshotGate(RatisSnapshotRecord.install(fence,data.f.groupId));
        }
        RatisRoutedClient.PreparedSnapshotOperation abort() throws Exception { var terminal=initial.abort(); home.transition(initial,terminal); return data.routed.prepareSnapshotGate(RatisSnapshotRecord.release(terminal,data.f.groupId)); }
        public void close() throws Exception { data.close(); home.close(); }
    }
    @Test void repeatedWaitKeepsOneGateAndCancellationReleasesOnlyReadyUnsubmittedAuthority() throws Exception {
        try (var f=new Fixture("wait")) {
            var old=f.data.f.propose(f.data.f.put(2)); assertNull(old.getException());
            assertFalse(f.data.routed.authorizeSnapshotGate(f.install).drained()); assertEquals(1,f.data.f.machine.snapshotPreparationSlots());
            assertThrows(RatisSnapshotWaitingException.class,() -> f.data.routed.executeSnapshotGate(f.install));
            assertFalse(f.data.routed.authorizeSnapshotGate(f.install).drained()); assertEquals(1,f.data.f.machine.snapshotPreparationSlots()); assertEquals(0,f.data.wire.index);
            assertNotNull(f.data.f.propose(f.data.f.put(3)).getException());
            f.data.routed.cancelSnapshotGate(f.install); assertEquals(0,f.data.f.machine.snapshotPreparationSlots()); assertEquals(0,f.data.f.machine.proposalGateStats().gates());
            f.data.f.machine.cancelTransaction(old); helper.released(f.data);
            f.data.routed.authorizeSnapshotGate(f.install); var pending=f.data.wire.propose(f.install.bytes()); assertNull(pending.getException());
            f.data.f.machine.preAppendTransaction(pending); pending.initLogEntry(1,++f.data.wire.index); f.data.f.machine.cancelTransaction(pending);
            f.data.routed.cancelSnapshotGate(f.install); assertEquals(1,f.data.f.machine.proposalGateStats().gates()); assertEquals(1,f.data.f.machine.reservedSnapshotGateCount());
            var receipt=RatisSnapshotRecord.Result.decode(f.data.f.machine.applyTransaction(pending).get(5,TimeUnit.SECONDS).getContent().toByteArray());
            assertEquals(receipt,f.data.routed.executeSnapshotGate(f.install)); assertEquals(1,f.data.wire.index); helper.released(f.data);
        }
    }
    @Test void lostInstallAndReleaseRepliesRecoverNativeProofWithoutHomeReadsOrAnotherAppend() throws Exception {
        try (var f=new Fixture("lost")) {
            f.data.wire.lose=true; assertThrows(IOException.class,() -> f.data.routed.executeSnapshotGate(f.install));
            var gate=f.data.f.store.snapshotGate(f.fence.fenceId()); assertNotNull(gate.installed()); long index=f.data.wire.index;
            f.home.wire.afterRead=() -> { throw new AssertionError("Retained native gate proof must not reread HOME"); };
            assertEquals(gate.installed(),f.data.routed.executeSnapshotGate(f.install).position()); assertEquals(index,f.data.wire.index);
            f.home.wire.afterRead=null; assertFalse(f.data.routed.helpSnapshotFence(f.fence,f.data.f.groupId));
            var release=f.abort(); f.data.wire.lose=true; assertThrows(IOException.class,() -> f.data.routed.executeSnapshotGate(release));
            var released=f.data.f.store.snapshotGate(f.fence.fenceId()).released(); index=f.data.wire.index;
            f.home.wire.afterRead=() -> { throw new AssertionError("Release retry must use exact native proof"); };
            var old=f.install.descriptor(); var changed=new RangeDescriptor(old.rangeId(),old.groupId(),old.rangeEpoch(),1,null,null,RangeDescriptor.TopologyState.ACTIVE,old.replicas());
            var directory=f.data.directory; f.data.routed.refresh(new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),1,directory.columns(),0,List.of(changed)));
            var restored=f.data.routed.restoreSnapshotGate(release.request(),old); f.data.f.machine.takeSnapshot(); f.data.f.machine.pause(); f.data.f.machine.reinitialize();
            assertEquals(released,f.data.routed.executeSnapshotGate(restored).position()); assertEquals(index,f.data.wire.index); assertEquals(0,restored.descriptor().configurationVersion()); helper.released(f.data);
        }
    }
    @Test void releaseBeforeInstallSuppressesLateInstallAndAllAuthorityQueriesRequireFreshDataBinding() throws Exception {
        try (var f=new Fixture("suppressed")) {
            var release=f.abort(); var receipt=f.data.routed.executeSnapshotGate(release); long index=f.data.wire.index;
            assertTrue(f.data.routed.executeSnapshotGate(f.install).suppressed()); assertEquals(index,f.data.wire.index); assertEquals(receipt,f.data.routed.executeSnapshotGate(release));
            assertFalse(f.data.routed.helpSnapshotFence(f.fence,f.data.f.groupId));
            for (int kind:List.of(ScopedTransactionRpc.SNAPSHOT_GATE_PREPARE,ScopedTransactionRpc.SNAPSHOT_GATE_CANCEL,ScopedTransactionRpc.SNAPSHOT_HELP)) {
                byte[] body=kind==ScopedTransactionRpc.SNAPSHOT_HELP ? helpRequest(f.fence) : f.install.bytes(); var request=Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(kind,body)));
                assertThrows(ExecutionException.class,() -> f.data.f.machine.query(request).get(5,TimeUnit.SECONDS));
                assertThrows(ExecutionException.class,() -> f.home.f.machine.queryLinearizable(request).get(5,TimeUnit.SECONDS));
            }
            helper.released(f.data);
        }
    }
    @Test void stalePreparationCannotJoinAnIdenticalBlockedFreshGateRequest() throws Exception {
        try (var f=new Fixture("fresh-coalesce")) {
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            f.home.wire.afterRead=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("HOME read timed out"); } catch (InterruptedException e) { throw new AssertionError(e); } };
            var message=Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_GATE_PREPARE,f.install.bytes())));
            var fresh=f.data.f.machine.queryLinearizable(message);
            try { assertTrue(entered.await(5,TimeUnit.SECONDS)); assertThrows(ExecutionException.class,() -> f.data.f.machine.query(message).get(100,TimeUnit.MILLISECONDS)); }
            finally { release.countDown(); }
            fresh.get(5,TimeUnit.SECONDS); f.data.routed.cancelSnapshotGate(f.install); helper.released(f.data);
        }
    }
    @Test void authorityReplyDigestAndDispositionAreValidatedBeforeAppend() throws Exception {
        try (var f=new Fixture("bad-reply")) {
            for (boolean wrongDigest:new boolean[]{false,true}) try (var client=new RatisRoutedClient(f.data.directory,new RatisRoutedClient.Transport() {
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
                public byte[] participant(RangeDescriptor route,byte[] bytes) { throw new AssertionError("Malformed authority cannot append"); }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) {
                    assertTrue(fresh); var body=ByteBuffer.allocate(41); var digest=f.install.request().digest(); if (wrongDigest) digest[0]^=1;
                    ScopedTransactionRpc.putBlob(body,digest); body.put((byte)(wrongDigest ? 1 : 3)); ScopedTransactionRpc.putBlob(body,null);
                    return ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_GATE_PREPARE,body.array());
                }
            })) { assertThrows(IOException.class,() -> client.executeSnapshotGate(f.install)); }
            assertEquals(0,f.data.wire.index); helper.released(f.data);
        }
    }
    @Test void helpReturnsIncompleteFutureAndCancelledCallerCannotDonateActualRecoveryOwnership() throws Exception {
        var two=new RatisSnapshotCoordinatorTest(); two.root=root;
        try (var f=two.new Fixture("async-help")) {
            f.committedTransactionWithRightStillPrepared(); f.home.register(f.fence);
            f.data.executeSnapshotGate(f.data.prepareSnapshotGate(RatisSnapshotRecord.install(f.fence,f.right.f.groupId)));
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var original=f.recovery;
            f.recovery=(group,prepared) -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Test recovery wait timed out"); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException(e); } return original.step(group,prepared); };
            var request=Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_HELP,helpRequest(f.fence))));
            var future=f.right.f.machine.queryLinearizable(request);
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); assertFalse(future.isDone()); assertTrue(future.cancel(false)); assertEquals(1,f.right.snapshotHelp.activeJobs());
                assertNull(f.data.finalizedParticipant(f.right.f.groupId,two.tx),"Independent native OUTCOME must remain available while HELP owns CONTROL");
                var drained=f.right.snapshotHelp.stop(); assertFalse(drained.isDone()); assertTrue(drained.cancel(false)); assertFalse(f.right.snapshotHelp.stop().isDone());
            } finally { release.countDown(); }
            f.right.snapshotHelp.stop().get(5,TimeUnit.SECONDS); assertNotNull(f.right.f.store.committedParticipant(two.tx)); assertEquals(0,f.right.snapshotHelp.activeJobs());
        }
    }
}
