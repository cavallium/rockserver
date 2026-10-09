package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Real native HOME admission through controlled framed RPCs, without a server or network. */
class RatisHomeRecoveryRpcTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisHomeRecoveryAdmissionTest helper=new RatisHomeRecoveryAdmissionTest();
    final class Fixture implements AutoCloseable {
        final RatisHomeRecoveryAdmissionTest.ProofReader proof;
        final RatisRetentionAdmissionTest.Fixture nativeHome;
        final RangeDirectory directory;
        final RatisRoutedClient client;
        final RatisTransactionStatuses homes;
        final TransactionStatus initial;
        final Wire wire;
        Fixture(String name) throws Exception {
            helper.root=root; proof=helper.new ProofReader(); nativeHome=helper.fixture(name,proof,4096,true);
            var replica=new ReplicaDescriptor(new UUID(20,1),new UUID(20,2),new UUID(20,3),"home","localhost:12345",ReplicaDescriptor.Role.VOTER);
            var route=new RangeDescriptor(nativeHome.groupId,nativeHome.groupId,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
            directory=new RangeDirectory(helper.cluster,helper.database,new UUID(20,4),0,new byte[32],0,Set.of(helper.column),0,List.of(route));
            wire=new Wire(this); client=new RatisRoutedClient(directory,wire); homes=new RatisTransactionStatuses(client,helper.column);
            initial=helper.initial(1); nativeHome.apply(nativeHome.propose(helper.cas(nativeHome,0,null,initial,false)),1); wire.index=1;
        }
        RatisRoutedClient.PreparedHomeRecoveryOperation abort(long n) { return homes.prepareRecovery(new OperationId(nativeHome.admin,0,n),initial,initial.decide(TransactionStatus.Phase.ABORT),null); }
        RatisRoutedClient.PreparedHomeRecoveryOperation ack(long n) {
            var terminal=initial.decide(TransactionStatus.Phase.ABORT);
            helper.outcome.set(new ParticipantOutcome(new ParticipantFinalization(nativeHome.groupId,1,helper.participant,new OperationId(new UUID(21,1),0,0),terminal),new ApplyPosition(7,50),null,null));
            return homes.prepareRecovery(new OperationId(nativeHome.admin,0,n),terminal,terminal.finalized(helper.participant),helper.outcome.get());
        }
        public void close() throws Exception { client.close(); proof.close(); nativeHome.close(); }
    }
    final class Wire implements RatisRoutedClient.Transport {
        final Fixture f; long index; int appends; boolean loseReply,closed; byte[] forged,forgedLookup,forgedPreparation;
        final List<RangeDescriptor> routes=new ArrayList<>();
        Wire(Fixture f) { this.f=f; }
        public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError("Protected recovery must not use ordinary command transport"); }
        public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
        public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException {
            assertFalse(closed); assertTrue(fresh); routes.add(route);
            if (forgedPreparation!=null && ScopedTransactionRpc.kind(request,ScopedTransactionRpc.MAX_REQUEST_BYTES)==ScopedTransactionRpc.HOME_RECOVERY_PREPARE) {
                var recovery=HomeRecovery.decode(ScopedTransactionRpc.parse(request,ScopedTransactionRpc.MAX_REQUEST_BYTES).body());
                var body=ByteBuffer.allocate(40+forgedPreparation.length); ScopedTransactionRpc.putBlob(body,recovery.digest()); ScopedTransactionRpc.putBlob(body,forgedPreparation); return ScopedTransactionRpc.frame(ScopedTransactionRpc.HOME_RECOVERY_PREPARE,body.array());
            }
            if (forgedLookup!=null && ScopedTransactionRpc.kind(request,ScopedTransactionRpc.MAX_REQUEST_BYTES)==ScopedTransactionRpc.HOME_RECOVERY_RESULT) {
                var body=ByteBuffer.allocate(4+forgedLookup.length); ScopedTransactionRpc.putBlob(body,forgedLookup); return ScopedTransactionRpc.frame(ScopedTransactionRpc.HOME_RECOVERY_RESULT,body.array());
            }
            try { return f.nativeHome.machine.queryLinearizable(Message.valueOf(ByteString.copyFrom(request))).get(5,TimeUnit.SECONDS).getContent().toByteArray(); }
            catch (Exception failure) { throw new IOException("Controlled HOME query failed",failure); }
        }
        public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException {
            assertFalse(closed); routes.add(route); appends++;
            try {
                var result=HomeRecoveryResult.applied(helper.apply(f.nativeHome,helper.propose(f.nativeHome,HomeRecovery.decode(bytes)),++index)).encode();
                if (loseReply) { loseReply=false; throw new IOException("Lost applied HOME reply"); }
                return forged==null ? result : forged;
            } catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException(failure); }
        }
        public void close() { closed=true; }
    }
    @Test void lostAppliedReplyAndRouteRefreshRecoverOriginalReceiptAfterGrantExpiryAndSnapshotReload() throws Exception {
        try (var f=new Fixture("retry")) {
            var operation=f.abort(0); assertNull(f.homes.recoveryResult(operation)); f.wire.loseReply=true;
            assertThrows(IOException.class,() -> f.homes.executeRecovery(operation));
            var receipt=f.homes.recoveryResult(operation); assertEquals(CommandResult.Status.SUCCESS,receipt.result().status()); assertEquals(new ApplyPosition(1,2),receipt.position());
            var old=operation.descriptor(); var changed=new RangeDescriptor(old.rangeId(),old.groupId(),old.rangeEpoch(),1,null,null,RangeDescriptor.TopologyState.ACTIVE,old.replicas());
            f.client.refresh(new RangeDirectory(helper.cluster,helper.database,f.directory.domainId(),0,new byte[32],1,f.directory.columns(),0,List.of(changed)));
            var restored=f.homes.restoreRecovery(HomeRecovery.decode(operation.bytes()),old); assertEquals(0,restored.descriptor().configurationVersion());
            var floor=DistributedCommand.advanceSessionEpochFloor(f.nativeHome.groupId,1,new byte[32],new OperationId(new UUID(22,1),5,0),5);
            f.nativeHome.apply(f.nativeHome.propose(floor),++f.wire.index);
            assertEquals(receipt,f.homes.executeRecovery(restored));
            f.nativeHome.machine.takeSnapshot(); f.nativeHome.machine.pause(); f.nativeHome.machine.reinitialize();
            assertEquals(receipt,f.homes.recoveryResult(restored)); assertEquals(receipt,f.homes.executeRecovery(restored));
            assertEquals(1,f.wire.appends,"Lost reply retries use retained receipt rather than repeat log append");
            assertTrue(f.wire.routes.stream().allMatch(route -> route.configurationVersion()==0)); helper.released(f.nativeHome);
        }
    }
    @Test void ackRequiresFreshDataProofAndBothAppliedAndLookupReceiptsBindTheFrozenRequest() throws Exception {
        try (var f=new Fixture("proof")) {
            f.homes.executeRecovery(f.abort(0)); var operation=f.ack(1); var actual=helper.outcome.get(); helper.outcome.set(null);
            assertThrows(IOException.class,() -> f.homes.executeRecovery(operation)); assertEquals(1,f.wire.appends); helper.released(f.nativeHome);
            helper.outcome.set(actual); var other=f.abort(2);
            var forged=HomeRecoveryResult.applied(new HomeRecoveryReceipt(other.recovery(),new ApplyPosition(1,3),new CommandResult(CommandResult.Status.SUCCESS,List.of()))).encode();
            f.wire.forgedPreparation=forged; assertThrows(IOException.class,() -> f.homes.executeRecovery(operation)); assertEquals(1,f.wire.appends); f.wire.forgedPreparation=null;
            f.wire.forged=forged; assertThrows(IOException.class,() -> f.homes.executeRecovery(operation)); assertEquals(2,f.wire.appends); f.wire.forged=null;
            f.wire.forgedLookup=forged; assertThrows(IOException.class,() -> f.homes.recoveryResult(operation)); f.wire.forgedLookup=null;
            var receipt=f.homes.recoveryResult(operation); assertEquals(new ApplyPosition(1,3),receipt.position());
            assertEquals(receipt,f.homes.executeRecovery(operation)); assertEquals(2,f.wire.appends,"Retained receipt finishes retry without another append"); helper.released(f.nativeHome);
        }
    }
    @Test void freshCancellationReleasesOnlyUnsubmittedAuthorityAndNeverAnUnknownAppend() throws Exception {
        try (var f=new Fixture("cancel")) {
            var operation=f.abort(0); f.homes.authorizeRecovery(operation); assertEquals(1,f.nativeHome.machine.homeRecoverySlots());
            f.homes.cancelRecovery(operation); helper.released(f.nativeHome); assertNotNull(helper.propose(f.nativeHome,operation.recovery()).getException());
            f.homes.authorizeRecovery(operation); TransactionContext pending=helper.propose(f.nativeHome,operation.recovery()); assertNull(pending.getException());
            f.nativeHome.machine.preAppendTransaction(pending); pending.initLogEntry(1,++f.wire.index); f.nativeHome.machine.cancelTransaction(pending);
            f.homes.cancelRecovery(operation); assertEquals(1,f.nativeHome.machine.proposalFootprintStats().candidates()); assertEquals(operation.bytes().length,f.nativeHome.machine.homeRecoveryInputBytes());
            var result=HomeRecoveryResult.decode(f.nativeHome.machine.applyTransaction(pending).get(5,TimeUnit.SECONDS).getContent().toByteArray()).receipt();
            assertEquals(result,f.homes.recoveryResult(operation)); helper.released(f.nativeHome);
        }
    }
    @Test void nativeProvenRetryReattachesWhileAnEarlierHoldFreeRpcOwnerRemainsActive() throws Exception {
        try (var f=new Fixture("native-owner")) {
            var operation=f.abort(0); var receipt=f.homes.executeRecovery(operation);
            var pending=helper.propose(f.nativeHome,operation.recovery()); assertNull(pending.getException());
            assertEquals(operation.bytes().length,f.nativeHome.machine.homeRecoveryInputBytes());
            f.homes.authorizeRecovery(operation); assertEquals(0,f.nativeHome.machine.homeRecoverySlots()); assertEquals(0,f.nativeHome.machine.proposalFootprintStats().candidates());
            assertEquals(receipt,f.homes.executeRecovery(operation)); assertEquals(operation.bytes().length,f.nativeHome.machine.homeRecoveryInputBytes());
            f.nativeHome.machine.cancelTransaction(pending); helper.released(f.nativeHome);
        }
    }
    @Test void cancellationDuringBlockedProofRetainsInputUntilActualReadReturns() throws Exception {
        try (var f=new Fixture("cancel-read"); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            f.homes.executeRecovery(f.abort(0)); var operation=f.ack(1); f.proof.block=true;
            var preparing=executor.submit(() -> { f.homes.authorizeRecovery(operation); return null; }); assertTrue(f.proof.entered.await(5,TimeUnit.SECONDS));
            f.homes.cancelRecovery(operation); assertEquals(1,f.nativeHome.machine.homeRecoverySlots()); assertEquals(operation.bytes().length,f.nativeHome.machine.homeRecoveryInputBytes());
            assertThrows(IOException.class,() -> f.homes.authorizeRecovery(operation));
            f.proof.release.countDown(); assertThrows(ExecutionException.class,() -> preparing.get(5,TimeUnit.SECONDS)); helper.released(f.nativeHome);
            assertEquals(CommandResult.Status.SUCCESS,f.homes.executeRecovery(operation).result().status()); helper.released(f.nativeHome);
        }
    }
    @Test void allRecoveryQueriesRequireFreshBarrierAndRejectDataRole() throws Exception {
        try (var f=new Fixture("barrier"); var data=new RatisRetentionAdmissionTest.Fixture(root.resolve("wrong-role"),RatisRangeStateMachine.RetentionLimits.defaults())) {
            var operation=f.abort(0);
            for (int kind:List.of(ScopedTransactionRpc.HOME_RECOVERY_PREPARE,ScopedTransactionRpc.HOME_RECOVERY_RESULT,ScopedTransactionRpc.HOME_RECOVERY_CANCEL)) {
                var request=Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(kind,operation.bytes())));
                assertThrows(ExecutionException.class,() -> f.nativeHome.machine.query(request).get(5,TimeUnit.SECONDS));
                assertThrows(ExecutionException.class,() -> data.machine.queryLinearizable(request).get(5,TimeUnit.SECONDS));
            }
            helper.released(f.nativeHome); helper.released(data);
        }
    }
    @Test void stalePreparationCannotJoinAnIdenticalBlockedFreshHomeRequest() throws Exception {
        try (var f=new Fixture("fresh-coalesce")) {
            f.homes.executeRecovery(f.abort(0)); var operation=f.ack(1); f.proof.block=true;
            var message=Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.HOME_RECOVERY_PREPARE,operation.bytes())));
            var fresh=f.nativeHome.machine.queryLinearizable(message);
            try { assertTrue(f.proof.entered.await(5,TimeUnit.SECONDS)); assertThrows(ExecutionException.class,() -> f.nativeHome.machine.query(message).get(100,TimeUnit.MILLISECONDS)); }
            finally { f.proof.release.countDown(); }
            fresh.get(5,TimeUnit.SECONDS); f.homes.cancelRecovery(operation); helper.released(f.nativeHome);
        }
    }
    @Test void routedCloseWaitsThroughBlockedAuthorizationAndFollowingAppend() throws Exception {
        try (var f=new Fixture("close"); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            f.homes.executeRecovery(f.abort(0)); var operation=f.ack(1); f.proof.block=true;
            var execute=executor.submit(() -> f.homes.executeRecovery(operation)); assertTrue(f.proof.entered.await(5,TimeUnit.SECONDS));
            var closing=executor.submit(() -> { f.client.close(); return null; });
            try { assertThrows(TimeoutException.class,() -> closing.get(50,TimeUnit.MILLISECONDS)); assertFalse(f.wire.closed); }
            finally { f.proof.release.countDown(); }
            assertEquals(CommandResult.Status.SUCCESS,execute.get(5,TimeUnit.SECONDS).result().status()); closing.get(5,TimeUnit.SECONDS); assertTrue(f.wire.closed); assertEquals(2,f.wire.appends); helper.released(f.nativeHome);
        }
    }
}
