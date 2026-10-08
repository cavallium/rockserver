package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Forced control recovery with real native state and controlled transport; no network/server fixture. */
class RatisDurableSnapshotTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSnapshotCaptureRpcTest helper=new RatisSnapshotCaptureRpcTest();
    RatisSnapshotCaptureRpcTest.Fixture fixture(String name) throws Exception { helper.root=root; return helper.new Fixture(name,true,false); }
    OperationId control(long sequence) { return new OperationId(new UUID(0,8100),1,sequence); }
    RatisDurableSnapshot create(Path path,RatisSnapshotCaptureRpcTest.Fixture f) throws Exception {
        return RatisDurableSnapshot.create(path,f.home.homes,f.client,f.initial,f.home.homes.prepare(control(0),null,f.initial),f.data.directory.ranges());
    }
    void installed(RatisDurableSnapshot journal,RatisSnapshotCaptureRpcTest.Fixture f) throws Exception {
        assertEquals(f.initial,journal.refreshHome()); assertTrue(journal.resolveHomeFromObserved());
        journal.freezeInstall(f.data.f.groupId); assertNotNull(journal.executeInstall(f.data.f.groupId).position()); assertTrue(journal.verifyReady(f.data.f.groupId));
    }
    void transition(RatisDurableSnapshot journal,RatisSnapshotCaptureRpcTest.Fixture f,SnapshotFenceStatus next,long sequence) throws Exception {
        journal.queueHome(f.home.homes.prepare(control(sequence),journal.snapshot().observedHome(),next));
        assertEquals(CommandResult.Status.SUCCESS,journal.executeHome().status());
    }
    RatisSnapshotCoordinator coordinator(RatisDurableSnapshot journal,RatisSnapshotCaptureRpcTest.Fixture f) throws Exception {
        return new RatisSnapshotCoordinator(journal,f.home.homes,f.client,
                last -> new OperationId(last.sessionId(),last.epoch(),last.sequence()+1),UUID::randomUUID,Duration.ofSeconds(10));
    }
    @Test void coordinatorCompletesForcedWorkflowWithOneRaftWritePerStep() throws Exception {
        try (var f=fixture("driver"); var journal=create(root.resolve("driver.journal"),f);
                var driver=coordinator(journal,f)) {
            boolean complete=false;
            for (int n=0;n<20 && !complete;n++) {
                long before=f.data.wire.index+f.home.wire.index; var step=driver.step(); complete=step.complete();
                assertTrue(f.data.wire.index+f.home.wire.index-before<=1,"One Raft mutation per step");
                if (!journal.snapshot().observedHome().terminal()) assertTrue(f.data.f.store.snapshotGate(f.fence.fenceId()).active());
            }
            assertTrue(complete); assertEquals(SnapshotFenceStatus.Phase.COMPLETE,journal.snapshot().observedHome().phase());
            var progress=journal.snapshot().participant(f.data.f.groupId);
            assertArrayEquals(new byte[]{7},f.client.snapshotGet(progress.capture().target(),progress.captureToken(),f.data.f.column,new byte[]{2}));
        }
    }
    @Test void coordinatorRetainsUndrainedInstallAuthorityAcrossWaitSteps() throws Exception {
        try (var f=fixture("waiting")) {
            f.home.transition(f.initial,f.initial.abort());
            try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) { helper.helper.append(f.data,release); }
            var fence=helper.helper.fence(f.home,f.data,19,helper.helper.scope(f.data,1,4)); var initial=SnapshotFenceStatus.begin(fence);
            var old=f.data.f.propose(f.data.f.put(3)); assertNull(old.getException());
            try (var journal=RatisDurableSnapshot.create(root.resolve("waiting.journal"),f.home.homes,f.client,initial,f.home.homes.prepare(control(0),null,initial),f.data.directory.ranges());
                    var driver=coordinator(journal,f)) {
                driver.step();
                assertEquals(RatisSnapshotCoordinator.Action.WAITING,driver.step().action());
                assertEquals(RatisSnapshotCoordinator.Action.WAITING,driver.step().action());
                assertEquals(1,f.data.f.machine.snapshotPreparationSlots()); assertEquals(1,f.data.f.machine.proposalGateStats().gates());
                assertNotNull(f.data.f.propose(f.data.f.put(1)).getException());
                f.data.f.machine.notifyNotLeader(List.of());
                f.data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,true));
                f.data.f.machine.notifyTermIndexUpdated(2,++f.data.wire.index);
                assertEquals(RatisSnapshotCoordinator.Action.INSTALLED,driver.step().action());
                assertNotNull(f.data.f.store.snapshotGate(fence.fenceId()).installed());
            }
        }
    }
    @Test void coordinatorRetriesLostInstallReplyInsteadOfWaitingOnRetiredAuthority() throws Exception {
        Path path=root.resolve("install-retry.journal");
        try (var f=fixture("install-retry")) {
            f.home.transition(f.initial,f.initial.abort());
            try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) { helper.helper.append(f.data,release); }
            var fence=helper.helper.fence(f.home,f.data,20,helper.helper.scope(f.data,1,4)); var initial=SnapshotFenceStatus.begin(fence);
            try (var journal=RatisDurableSnapshot.create(path,f.home.homes,f.client,initial,f.home.homes.prepare(control(0),null,initial),f.data.directory.ranges()); var driver=coordinator(journal,f)) {
                driver.step(); f.data.wire.lose=true; assertThrows(IOException.class,driver::step);
                assertNull(journal.snapshot().participant(f.data.f.groupId).installReceipt());
            }
            long applied=f.data.wire.index;
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client); var driver=coordinator(journal,f)) {
                assertEquals(RatisSnapshotCoordinator.Action.INSTALLED,driver.step().action()); assertEquals(applied,f.data.wire.index);
                assertNotNull(journal.snapshot().participant(f.data.f.groupId).installReceipt());
            }
        }
    }
    @Test void closeCancelsOnlyUndispatchedFrozenInstallAndReopenRetainsExactRequest() throws Exception {
        Path path=root.resolve("cancel-wait.journal");
        try (var f=fixture("cancel-wait")) {
            f.home.transition(f.initial,f.initial.abort());
            try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) { helper.helper.append(f.data,release); }
            var fence=helper.helper.fence(f.home,f.data,21,helper.helper.scope(f.data,1,4)); var initial=SnapshotFenceStatus.begin(fence);
            var old=f.data.f.propose(f.data.f.put(3)); assertNull(old.getException());
            try (var journal=RatisDurableSnapshot.create(path,f.home.homes,f.client,initial,f.home.homes.prepare(control(0),null,initial),f.data.directory.ranges())) {
                var driver=coordinator(journal,f); driver.step(); assertEquals(RatisSnapshotCoordinator.Action.WAITING,driver.step().action());
                assertEquals(1,f.data.f.machine.proposalGateStats().gates()); driver.close(); assertEquals(0,f.data.f.machine.proposalGateStats().gates());
                assertTrue(journal.snapshot().participant(f.data.f.groupId).installFrozen()); assertNull(journal.snapshot().participant(f.data.f.groupId).installReceipt()); assertThrows(IOException.class,driver::step);
                f.data.f.machine.cancelTransaction(old);
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client); var driver=coordinator(journal,f)) { assertEquals(RatisSnapshotCoordinator.Action.INSTALLED,driver.step().action()); }
        }
    }
    @Test void cancelledCloseViewKeepsJournalLockedUntilActualInstallCancellationReturns() throws Exception {
        Path path=root.resolve("cancel-owned.journal");
        try (var f=fixture("cancel-owned"); var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            f.home.transition(f.initial,f.initial.abort());
            try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) { helper.helper.append(f.data,release); }
            var fence=helper.helper.fence(f.home,f.data,22,helper.helper.scope(f.data,1,4)); var initial=SnapshotFenceStatus.begin(fence);
            var old=f.data.f.propose(f.data.f.put(3)); assertNull(old.getException());
            var entered=new java.util.concurrent.CountDownLatch(1); var release=new java.util.concurrent.CountDownLatch(1); var done=new java.util.concurrent.CountDownLatch(1);
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
                public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException { return f.data.wire.participant(route,bytes); }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException {
                    if (ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES).kind()==ScopedTransactionRpc.SNAPSHOT_GATE_CANCEL) {
                        entered.countDown(); try { if (!release.await(5,java.util.concurrent.TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); } catch (InterruptedException failure) { throw new IOException(failure); }
                    }
                    return f.data.wire.transaction(route,bytes,fresh);
                }
            };
            try (var client=new RatisRoutedClient(f.data.directory,transport); var journal=RatisDurableSnapshot.create(path,f.home.homes,client,initial,f.home.homes.prepare(control(0),null,initial),f.data.directory.ranges());
                    var driver=new RatisSnapshotCoordinator(journal,f.home.homes,client,last -> control(last.sequence()+1),UUID::randomUUID,Duration.ofSeconds(10))) {
                driver.step(); assertEquals(RatisSnapshotCoordinator.Action.WAITING,driver.step().action());
                var view=executor.submit(() -> { try { driver.close(); return null; } finally { done.countDown(); } });
                try {
                    assertTrue(entered.await(5,java.util.concurrent.TimeUnit.SECONDS)); assertTrue(view.cancel(false)); assertThrows(IOException.class,journal::close); assertThrows(IOException.class,driver::close);
                    assertThrows(IOException.class,() -> RatisDurableSnapshot.reopen(path,f.home.homes,client));
                } finally { release.countDown(); }
                assertTrue(done.await(5,java.util.concurrent.TimeUnit.SECONDS)); assertEquals(0,f.data.f.machine.proposalGateStats().gates());
                f.data.f.machine.cancelTransaction(old); journal.close();
                try (var reopened=RatisDurableSnapshot.reopen(path,f.home.homes,client)) { assertTrue(reopened.snapshot().participant(f.data.f.groupId).installFrozen()); assertNull(reopened.snapshot().participant(f.data.f.groupId).installReceipt()); }
            } finally { release.countDown(); }
        }
    }
    @Test void observedForeignCaptureIsDurablyLostBeforeTheNextPolicyStep() throws Exception {
        Path path=root.resolve("foreign.journal");
        try (var f=fixture("foreign")) {
            var group=f.data.f.groupId;
            try (var journal=create(path,f)) {
                installed(journal,f); var ready=f.initial.ready(group,journal.snapshot().participant(group).readyProof());
                f.home.transition(f.initial,ready); var token=f.client.executeSnapshotCapture(f.operation()); f.home.transition(ready,ready.capture(group,token));
                journal.refreshHome(); assertTrue(journal.snapshot().participant(group).captureLost());
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client); var driver=coordinator(journal,f)) {
                assertTrue(journal.snapshot().participant(group).captureLost()); assertEquals(RatisSnapshotCoordinator.Action.ABORTED,driver.step().action());
                assertTrue(driver.step().complete()); assertFalse(f.data.f.store.snapshotGate(f.fence.fenceId()).active());
            }
        }
    }
    RatisSnapshotCoordinator proofOnlyDriver(RatisDurableSnapshot journal,RatisSnapshotCaptureRpcTest.Fixture f) throws Exception {
        return new RatisSnapshotCoordinator(journal,f.home.homes,f.client,last -> control(last.sequence()+1),UUID::randomUUID,Duration.ofSeconds(10));
    }
    @Test void forcedReceiptsReopenAndResolveProofWithoutWriteAuthority() throws Exception {
        Path path=root.resolve("receipt-proof.journal");
        try (var f=fixture("receipt-proof")) {
            var group=f.data.f.groupId;
            try (var journal=create(path,f)) {
                journal.refreshHome(); journal.resolveHomeFromObserved(); journal.freezeInstall(group); f.loseGateProof=true;
                assertThrows(IOException.class,() -> journal.executeInstall(group));
                assertNotNull(journal.snapshot().participant(group).installReceipt()); assertNull(journal.snapshot().participant(group).nativeGate());
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client); var driver=proofOnlyDriver(journal,f)) {
                assertEquals(RatisSnapshotCoordinator.Action.INSTALLED,driver.step().action());
                transition(journal,f,journal.snapshot().observedHome().abort(),1); journal.freezeRelease(group);
                try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) {
                    f.loseGateProof=true; assertThrows(IOException.class,() -> journal.executeRelease(group));
                }
                assertNotNull(journal.snapshot().participant(group).releaseReceipt()); assertTrue(journal.snapshot().participant(group).nativeGate().active());
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client); var driver=proofOnlyDriver(journal,f)) {
                assertTrue(driver.step().complete()); assertFalse(journal.snapshot().participant(group).nativeGate().active());
            }
        }
    }
    @Test void lostCaptureAndReleaseRepliesReopenWithExactRequestsAndKeepHistoricalReads() throws Exception {
        Path path=root.resolve("complete.journal");
        try (var f=fixture("complete")) {
            var group=f.data.f.groupId; byte[] frozen;
            try (var journal=create(path,f)) {
                installed(journal,f); var proof=journal.snapshot().participant(group).readyProof();
                transition(journal,f,f.initial.ready(group,proof),1);
                var operation=f.operation(); frozen=operation.bytes(); journal.freezeCapture(operation);
                f.lose=true; assertThrows(IOException.class,() -> journal.executeCapture(group));
                assertNull(journal.snapshot().participant(group).captureToken()); assertEquals(1,f.data.f.machine.snapshotPinStats().pins());
            }
            SnapshotFenceStatus.Capture token; RatisRoutedClient.SnapshotTarget target;
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client)) {
                var operation=journal.snapshot().participant(group).capture(); assertArrayEquals(frozen,operation.bytes()); target=operation.target();
                token=journal.executeCapture(group); var ready=journal.snapshot().observedHome();
                transition(journal,f,ready.capture(group,token),2); transition(journal,f,journal.snapshot().observedHome().complete(),3);
                assertTrue(f.data.f.store.snapshotGate(f.fence.fenceId()).active()); journal.freezeRelease(group);
                try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) {
                    f.data.wire.lose=true; assertThrows(IOException.class,() -> journal.executeRelease(group));
                }
                assertFalse(f.data.f.store.snapshotGate(f.fence.fenceId()).active());
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client)) {
                assertNotNull(journal.executeRelease(group)); assertEquals(f.data.f.store.snapshotGate(f.fence.fenceId()).released(),journal.snapshot().participant(group).releaseReceipt());
                assertEquals(token,journal.executeCapture(group));
                assertArrayEquals(new byte[]{7},f.client.snapshotGet(target,token,f.data.f.column,new byte[]{2}));
            }
        }
    }
    @Test void expiredCaptureCannotBeRecreatedOrReportedAndAbortReleasesTheGate() throws Exception {
        Path path=root.resolve("lost.journal");
        try (var f=fixture("lost")) {
            var group=f.data.f.groupId;
            try (var journal=create(path,f)) {
                installed(journal,f); transition(journal,f,f.initial.ready(group,journal.snapshot().participant(group).readyProof()),1);
                journal.freezeCapture(f.operation()); var token=journal.executeCapture(group);
                f.clock.set(Duration.ofSeconds(10).toNanos()); f.data.f.machine.expireTransactions();
                assertThrows(RatisSnapshotLostException.class,() -> journal.executeCapture(group));
                assertTrue(journal.snapshot().participant(group).captureLost());
                var captured=journal.snapshot().observedHome().capture(group,token);
                assertThrows(IllegalArgumentException.class,() -> journal.queueHome(f.home.homes.prepare(control(2),journal.snapshot().observedHome(),captured)));
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client)) {
                assertThrows(RatisSnapshotLostException.class,() -> journal.executeCapture(group));
                transition(journal,f,journal.snapshot().observedHome().abort(),2); journal.freezeRelease(group);
                try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) { journal.executeRelease(group); }
                assertFalse(f.data.f.store.snapshotGate(f.fence.fenceId()).active()); assertEquals(0,f.data.f.store.activeSnapshotPins());
            }
        }
    }
    @Test void homeProgressNeedsVerifiedEvidenceAndJournalHasExclusiveOwnership() throws Exception {
        Path path=root.resolve("evidence.journal");
        try (var f=fixture("evidence"); var journal=create(path,f)) {
            var group=f.data.f.groupId; journal.refreshHome(); journal.resolveHomeFromObserved();
            var fake=f.initial.ready(group,f.data.f.store.snapshotGate(f.fence.fenceId()).installed());
            assertThrows(IllegalArgumentException.class,() -> journal.queueHome(f.home.homes.prepare(control(1),f.initial,fake)));
            assertThrows(IOException.class,() -> RatisDurableSnapshot.reopen(path,f.home.homes,f.client));
            assertThrows(IllegalStateException.class,() -> journal.freezeRelease(group));
            journal.freezeInstall(group); journal.executeInstall(group); assertTrue(journal.verifyReady(group)); transition(journal,f,fake,1);
            var token=new SnapshotFenceStatus.Capture(UUID.randomUUID(),UUID.randomUUID(),journal.snapshot().participant(group).readyProof());
            assertThrows(IllegalArgumentException.class,() -> journal.queueHome(f.home.homes.prepare(control(2),fake,fake.capture(group,token))));
        }
    }
    @Test void lostHomeReplyIsResolvedFromFreshObservedProgressBeforeAnotherCas() throws Exception {
        Path path=root.resolve("home.journal");
        try (var f=fixture("home")) {
            var group=f.data.f.groupId;
            try (var journal=create(path,f)) {
                installed(journal,f); var ready=f.initial.ready(group,journal.snapshot().participant(group).readyProof());
                journal.queueHome(f.home.homes.prepare(control(1),f.initial,ready)); f.home.wire.lose=true;
                assertThrows(IOException.class,journal::executeHome); assertNull(journal.snapshot().homeResult());
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client)) {
                var frozen=journal.snapshot().pendingHome();
                assertThrows(IllegalStateException.class,() -> journal.queueHome(f.home.homes.prepare(control(2),f.initial,f.initial.abort())));
                journal.refreshHome(); assertTrue(journal.resolveHomeFromObserved()); assertTrue(journal.snapshot().homeSuperseded());
                assertArrayEquals(DistributedCommandCodec.encode(frozen.command()),DistributedCommandCodec.encode(journal.snapshot().pendingHome().command()));
                transition(journal,f,journal.snapshot().observedHome().abort(),2);
            }
        }
    }
}
