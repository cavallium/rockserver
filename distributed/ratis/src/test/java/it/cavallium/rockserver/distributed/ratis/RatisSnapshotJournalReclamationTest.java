package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native owner pins and HOME retirement through controlled transports, without servers or network. */
class RatisSnapshotJournalReclamationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisDurableSnapshotTest helper=new RatisDurableSnapshotTest();
    RatisSnapshotCaptureRpcTest.Fixture fixture(String name) throws Exception { helper.root=root; return helper.fixture(name); }
    void complete(RatisDurableSnapshot journal,RatisSnapshotCaptureRpcTest.Fixture f) throws Exception {
        try (var driver=helper.coordinator(journal,f)) { for (int n=0;n<20;n++) if (driver.step().complete()) return; }
        fail("Snapshot driver did not complete");
    }
    void retire(RatisDurableSnapshot journal,RatisSnapshotCaptureRpcTest.Fixture f) throws Exception {
        journal.consume(helper.control(100)); journal.executeHome(); var consumed=journal.snapshot().observedHome();
        new RatisSnapshotHomeJournalRetirementTest().retire(f.home,f.data,consumed,journal.snapshot().participant(f.data.f.groupId).releaseReceipt());
        assertThrows(RatisSnapshotHomeRetiredException.class,journal::refreshHome); assertNotNull(journal.snapshot().homeRetirementProof());
    }
    @Test void lostReleaseReplyReopensWithOriginalTokenAndOnlyExplicitSettledReclamationRemovesJournal() throws Exception {
        var path=root.resolve("release.journal");
        try (var f=fixture("release")) {
            SnapshotFenceStatus.Capture token;
            try (var journal=helper.create(path,f)) {
                complete(journal,f); var group=f.data.f.groupId; token=journal.snapshot().participant(group).captureToken(); assertNotNull(token);
                assertThrows(IllegalStateException.class,journal::reclaimRetired); retire(journal,f); journal.requestRelease(); assertTrue(journal.snapshot().releaseRequested());
                assertThrows(IllegalStateException.class,journal::reclaimRetired); // Gate/HOME GC leaves the immutable owner pin alive.
                f.lose=true; assertThrows(IOException.class,() -> journal.releaseCapture(group)); assertFalse(journal.snapshot().participant(group).captureReleased());
                assertThrows(RatisSnapshotHomeRetiredException.class,() -> journal.executeCapture(group)); assertTrue(Files.exists(path));
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,RatisTransactionBindings.of(f.client))) {
                var group=f.data.f.groupId; assertEquals(token,journal.snapshot().participant(group).captureToken()); journal.releaseCapture(group); assertTrue(journal.snapshot().participant(group).captureReleased()); journal.releaseCapture(group);
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,RatisTransactionBindings.of(f.client))) {
                assertTrue(journal.snapshot().participant(f.data.f.groupId).captureReleased()); journal.reclaimRetired(); assertFalse(Files.exists(path)); assertTrue(Files.exists(path.resolveSibling(path.getFileName()+".lock"))); assertThrows(IOException.class,journal::snapshot);
                assertThrows(IOException.class,() -> helper.create(path,f));
            }
            assertThrows(IOException.class,() -> RatisDurableSnapshot.reopen(path,f.home.homes,RatisTransactionBindings.of(f.client)));
        }
    }
    @Test void lostInitialTokenRecoversOnlyTheFrozenPinAfterHomeRetirementAndExpiredPinsSettleLost() throws Exception {
        for (boolean expired:new boolean[]{false,true}) {
            var path=root.resolve("unknown-token-"+expired+".journal");
            try (var f=fixture("unknown-token-"+expired)) {
                var group=f.data.f.groupId; SnapshotFenceStatus.Capture token;
                try (var journal=helper.create(path,f)) {
                    helper.installed(journal,f); var ready=f.initial.ready(group,journal.snapshot().participant(group).readyProof()); helper.transition(journal,f,ready,1);
                    var request=f.operation(); journal.freezeCapture(request); f.lose=true; assertThrows(IOException.class,() -> journal.executeCapture(group)); assertNull(journal.snapshot().participant(group).captureToken());
                    assertThrows(IllegalStateException.class,() -> journal.releaseCapture(group)); token=f.client.executeSnapshotCapture(request);
                    var captured=ready.capture(group,token); f.home.transition(ready,captured); var complete=captured.complete(); f.home.transition(captured,complete);
                    var released=f.client.executeSnapshotGate(f.client.prepareSnapshotGate(RatisSnapshotRecord.release(complete,group))).position();
                    var consumed=complete.consume(); f.home.transition(complete,consumed); assertEquals(consumed,journal.refreshHome());
                    new RatisSnapshotHomeJournalRetirementTest().retire(f.home,f.data,consumed,released); assertThrows(RatisSnapshotHomeRetiredException.class,journal::refreshHome);
                    if (expired) { f.clock.set(TimeUnit.SECONDS.toNanos(11)); f.data.f.machine.expireTransactions(); }
                    assertEquals(expired ? RatisDurableSnapshot.CaptureReleaseProgress.LOST : RatisDurableSnapshot.CaptureReleaseProgress.RECOVERED,journal.releaseCapture(group));
                    if (!expired) { assertEquals(token,journal.snapshot().participant(group).captureToken()); assertFalse(journal.snapshot().participant(group).captureReleased()); assertThrows(IllegalStateException.class,journal::reclaimRetired); }
                }
                try (var reopened=RatisDurableSnapshot.reopen(path,f.home.homes,RatisTransactionBindings.of(f.client))) {
                    assertEquals(expired ? RatisDurableSnapshot.CaptureReleaseProgress.LOST : RatisDurableSnapshot.CaptureReleaseProgress.RELEASED,reopened.releaseCapture(group));
                    reopened.reclaimRetired(); assertFalse(Files.exists(path));
                }
                assertEquals(0,f.data.f.machine.snapshotPinStats().pins());
            }
        }
    }
    @Test void consumedRetainedHomeRecoversOriginalLostTokenButCannotCreateAFirstSeenPin() throws Exception {
        for (boolean created:new boolean[]{false,true}) {
            var path=root.resolve("retained-consumed-"+created+".journal");
            try (var f=fixture("retained-consumed-"+created)) {
                var group=f.data.f.groupId; SnapshotFenceStatus.Capture token=null;
                try (var journal=helper.create(path,f)) {
                    helper.installed(journal,f); var ready=f.initial.ready(group,journal.snapshot().participant(group).readyProof()); helper.transition(journal,f,ready,1);
                    var request=f.operation(); journal.freezeCapture(request); SnapshotFenceStatus terminal;
                    if (created) {
                        f.lose=true; assertThrows(IOException.class,() -> journal.executeCapture(group)); token=f.client.executeSnapshotCapture(request);
                        var captured=ready.capture(group,token); f.home.transition(ready,captured); terminal=captured.complete(); f.home.transition(captured,terminal);
                    } else { terminal=ready.abort(); f.home.transition(ready,terminal); }
                    f.client.executeSnapshotGate(f.client.prepareSnapshotGate(RatisSnapshotRecord.release(terminal,group)));
                    var consumed=terminal.consume(); f.home.transition(terminal,consumed); assertEquals(consumed,journal.refreshHome());
                    assertNotNull(f.home.homes.read(f.fence.fenceId())); assertNull(journal.snapshot().homeRetirementProof()); assertNull(journal.snapshot().participant(group).captureToken());
                    assertEquals(created ? RatisDurableSnapshot.CaptureReleaseProgress.RECOVERED : RatisDurableSnapshot.CaptureReleaseProgress.LOST,journal.releaseCapture(group));
                    if (created) assertEquals(token,journal.snapshot().participant(group).captureToken());
                }
                try (var reopened=RatisDurableSnapshot.reopen(path,f.home.homes,RatisTransactionBindings.of(f.client))) {
                    assertEquals(created ? RatisDurableSnapshot.CaptureReleaseProgress.RELEASED : RatisDurableSnapshot.CaptureReleaseProgress.LOST,reopened.releaseCapture(group));
                    assertThrows(IllegalStateException.class,reopened::reclaimRetired); // HOME is consumed, but its retirement is still unproven.
                }
                assertEquals(0,f.data.f.machine.snapshotPinStats().pins());
            }
        }
    }
    @Test void explicitReleaseBeforeCompletePreventsCoordinatorPublishingAClosedPin() throws Exception {
        var path=root.resolve("release-before-complete.journal");
        try (var f=fixture("release-before-complete"); var journal=helper.create(path,f)) {
            var group=f.data.f.groupId; helper.installed(journal,f); var ready=f.initial.ready(group,journal.snapshot().participant(group).readyProof()); helper.transition(journal,f,ready,1);
            var request=f.operation(); journal.freezeCapture(request); var token=journal.executeCapture(group); var captured=ready.capture(group,token); helper.transition(journal,f,captured,2);
            assertEquals(RatisDurableSnapshot.CaptureReleaseProgress.RELEASED,journal.releaseCapture(group)); assertTrue(journal.snapshot().participant(group).captureLost());
            assertThrows(IllegalArgumentException.class,() -> journal.queueHome(f.home.homes.prepare(helper.control(3),captured,captured.complete())));
            try (var driver=helper.coordinator(journal,f)) { assertEquals(RatisSnapshotCoordinator.Action.ABORTED,driver.step().action()); }
            assertEquals(SnapshotFenceStatus.Phase.ABORT,journal.snapshot().observedHome().phase());
        }
    }
    @Test void unresolvedCompleteRetainsThePinAndExactHomeRequestUntilItsResultIsResolved() throws Exception {
        var path=root.resolve("pending-complete.journal");
        try (var f=fixture("pending-complete"); var journal=helper.create(path,f)) {
            var group=f.data.f.groupId; helper.installed(journal,f); var ready=f.initial.ready(group,journal.snapshot().participant(group).readyProof()); helper.transition(journal,f,ready,1);
            var request=f.operation(); journal.freezeCapture(request); var token=journal.executeCapture(group); var captured=ready.capture(group,token); helper.transition(journal,f,captured,2);
            journal.queueHome(f.home.homes.prepare(helper.control(3),captured,captured.complete())); var exact=DistributedCommandCodec.encode(journal.snapshot().pendingHome().command());
            assertThrows(IllegalStateException.class,() -> journal.releaseCapture(group)); assertFalse(journal.snapshot().participant(group).captureLost()); assertEquals(1,f.data.f.machine.snapshotPinStats().pins());
            assertArrayEquals(exact,DistributedCommandCodec.encode(journal.snapshot().pendingHome().command()));
            assertEquals(CommandResult.Status.SUCCESS,journal.executeHome().status()); assertEquals(RatisDurableSnapshot.CaptureReleaseProgress.RELEASED,journal.releaseCapture(group));
            assertEquals(0,f.data.f.machine.snapshotPinStats().pins());
        }
    }
    @Test void wholeLeaseReleaseSurvivesReopenAndAbortsPartialInstallWithoutAcquiringCaptures() throws Exception {
        var path=root.resolve("release-direction.journal");
        try (var f=fixture("release-direction")) {
            try (var journal=helper.create(path,f)) {
                helper.installed(journal,f); journal.requestRelease(); journal.requestRelease(); assertTrue(journal.snapshot().releaseRequested());
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,RatisTransactionBindings.of(f.client)); var driver=helper.coordinator(journal,f)) {
                assertTrue(journal.snapshot().releaseRequested()); long before=f.home.wire.index+f.data.wire.index; assertEquals(RatisSnapshotCoordinator.Action.ABORTED,driver.step().action()); assertEquals(1,f.home.wire.index+f.data.wire.index-before);
                assertEquals(SnapshotFenceStatus.Phase.ABORT,journal.snapshot().observedHome().phase()); assertNull(journal.snapshot().participant(f.data.f.groupId).capture()); assertEquals(0,f.data.f.machine.snapshotPinStats().pins());
                assertTrue(driver.step().complete()); assertNotNull(journal.snapshot().participant(f.data.f.groupId).releaseReceipt());
            }
        }
    }
    @Test void wholeLeaseReleasePreservesQueuedCompleteUntilItWinsThenReleasesGates() throws Exception {
        var path=root.resolve("queued-complete-release-direction.journal");
        try (var f=fixture("queued-complete-release-direction")) {
            try (var journal=helper.create(path,f)) {
                var group=f.data.f.groupId; helper.installed(journal,f); var ready=f.initial.ready(group,journal.snapshot().participant(group).readyProof()); helper.transition(journal,f,ready,1);
                var request=f.operation(); journal.freezeCapture(request); var token=journal.executeCapture(group); var captured=ready.capture(group,token); helper.transition(journal,f,captured,2);
                journal.queueHome(f.home.homes.prepare(helper.control(3),captured,captured.complete())); var exact=DistributedCommandCodec.encode(journal.snapshot().pendingHome().command()); journal.requestRelease();
                assertArrayEquals(exact,DistributedCommandCodec.encode(journal.snapshot().pendingHome().command())); assertFalse(journal.snapshot().participant(group).captureLost());
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,RatisTransactionBindings.of(f.client)); var driver=helper.coordinator(journal,f)) {
                assertTrue(journal.snapshot().releaseRequested()); assertEquals(RatisSnapshotCoordinator.Action.RETRIED_HOME,driver.step().action()); assertEquals(SnapshotFenceStatus.Phase.COMPLETE,journal.snapshot().observedHome().phase());
                assertTrue(driver.step().complete()); assertEquals(SnapshotFenceStatus.Phase.COMPLETE,journal.snapshot().observedHome().phase()); assertNotNull(journal.snapshot().participant(f.data.f.groupId).releaseReceipt()); assertEquals(1,f.data.f.machine.snapshotPinStats().pins());
            }
        }
    }
    @Test void declaredTtlSurvivesReopenBeforeFirstCaptureAndRejectsChangedLifetimeWithoutRpc() throws Exception {
        var path=root.resolve("declared-ttl.journal"); var ttl=Duration.ofSeconds(30);
        try (var f=fixture("declared-ttl")) {
            try (var journal=RatisDurableSnapshot.create(path,f.home.homes,RatisTransactionBindings.of(f.client),f.initial,f.home.homes.prepare(helper.control(0),null,f.initial),f.data.directory.ranges(),ttl)) {
                assertEquals(ttl,journal.snapshot().requestedTtl()); assertNull(journal.snapshot().participant(f.data.f.groupId).capture());
            }
            int owners=f.owners.get(),homes=f.homes.get();
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,RatisTransactionBindings.of(f.client))) {
                assertEquals(owners,f.owners.get()); assertEquals(homes,f.homes.get()); assertEquals(ttl,journal.snapshot().requestedTtl());
                helper.installed(journal,f); var ready=f.initial.ready(f.data.f.groupId,journal.snapshot().participant(f.data.f.groupId).readyProof()); helper.transition(journal,f,ready,1);
                var wrong=f.operation(); owners=f.owners.get(); homes=f.homes.get(); assertThrows(IllegalArgumentException.class,() -> journal.freezeCapture(wrong)); assertEquals(owners,f.owners.get()); assertEquals(homes,f.homes.get());
                var exact=f.client.prepareSnapshotCapture(wrong.target(),wrong.request().snapshotId(),ttl); journal.freezeCapture(exact);
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,RatisTransactionBindings.of(f.client))) { assertEquals(ttl,journal.snapshot().requestedTtl()); assertEquals(ttl,journal.snapshot().participant(f.data.f.groupId).capture().request().ttl()); }
            try (var generic=helper.create(root.resolve("generic-ttl.journal"),f)) { assertNull(generic.snapshot().requestedTtl()); }
        }
    }
    @Test void semanticCaptureLossDoesNotSettleTheOriginalLivePin() throws Exception {
        var path=root.resolve("semantic-loss.journal");
        try (var f=fixture("semantic-loss"); var journal=helper.create(path,f)) {
            complete(journal,f); var group=f.data.f.groupId; var original=journal.snapshot().participant(group);
            journal.markCaptureLost(group); assertTrue(journal.snapshot().participant(group).captureLost());
            assertArrayEquals(new byte[]{7},f.client.snapshotGet(original.capture().target(),original.captureToken(),f.data.f.column,new byte[]{2}));
            retire(journal,f); assertThrows(IllegalStateException.class,journal::reclaimRetired);
            assertFalse(journal.snapshot().participant(group).captureReleaseLost()); journal.releaseCapture(group);
            assertTrue(journal.snapshot().participant(group).captureReleased()); journal.reclaimRetired(); assertFalse(Files.exists(path));
        }
    }
    @Test void establishedOwnerLossIsForcedBeforeReclamation() throws Exception {
        var path=root.resolve("lost.journal"); var lost=new AtomicBoolean();
        try (var f=fixture("lost"); var data=new RatisRoutedClient(f.data.directory,new RatisRoutedClient.Transport() {
            public byte[] read(RangeDescriptor route,java.util.UUID column,byte[] key) { throw new AssertionError(); }
            public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
            public byte[] participant(RangeDescriptor route,byte[] record) throws IOException { return f.data.wire.participant(route,record); }
            public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException { return f.data.wire.transaction(route,request,fresh); }
            public java.util.concurrent.CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] request) throws IOException {
                if(lost.get())throw new RatisSnapshotLostException("Controlled original owner is gone");var message=Message.valueOf(ByteString.copyFrom(request));return (owner==null?f.data.f.machine.queryLinearizable(message):f.data.f.machine.queryStale(message,0)).thenApply(reply->reply.getContent().toByteArray());
            }
        }); var journal=RatisDurableSnapshot.create(path,f.home.homes,RatisTransactionBindings.of(data),f.initial,f.home.homes.prepare(helper.control(0),null,f.initial),f.data.directory.ranges())) {
            complete(journal,f); retire(journal,f); lost.set(true); journal.releaseCapture(f.data.f.groupId);
            assertTrue(journal.snapshot().participant(f.data.f.groupId).captureLost()); assertTrue(journal.snapshot().participant(f.data.f.groupId).captureReleaseLost()); assertFalse(journal.snapshot().participant(f.data.f.groupId).captureReleased());
            journal.close(); try (var reopened=RatisDurableSnapshot.reopen(path,f.home.homes,RatisTransactionBindings.of(data))) { assertTrue(reopened.snapshot().participant(f.data.f.groupId).captureLost()); assertTrue(reopened.snapshot().participant(f.data.f.groupId).captureReleaseLost()); reopened.reclaimRetired(); }
            assertFalse(Files.exists(path));
        }
    }
    @Test void blockedOwnerReleaseRetainsJournalOwnershipUntilActualTransportReturn() throws Exception {
        var path=root.resolve("blocked.journal"); var entered=new CountDownLatch(1); var resume=new CountDownLatch(1); var block=new AtomicBoolean();
        try (var f=fixture("blocked"); var data=new RatisRoutedClient(f.data.directory,new RatisRoutedClient.Transport() {
            public byte[] read(RangeDescriptor route,java.util.UUID column,byte[] key) { throw new AssertionError(); }
            public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
            public byte[] participant(RangeDescriptor route,byte[] record) throws IOException { return f.data.wire.participant(route,record); }
            public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException { return f.data.wire.transaction(route,request,fresh); }
            public java.util.concurrent.CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] request) throws IOException {
                if(block.get()&&ScopedTransactionRpc.kind(request,ScopedTransactionRpc.MAX_REQUEST_BYTES)==ScopedTransactionRpc.SNAPSHOT_RELEASE){entered.countDown();try{if(!resume.await(5,TimeUnit.SECONDS))throw new IOException("Release barrier timed out");}catch(InterruptedException failure){Thread.currentThread().interrupt();throw new IOException(failure);}}
                var message=Message.valueOf(ByteString.copyFrom(request));return (owner==null?f.data.f.machine.queryLinearizable(message):f.data.f.machine.queryStale(message,0)).thenApply(reply->reply.getContent().toByteArray());
            }
        }); var journal=RatisDurableSnapshot.create(path,f.home.homes,RatisTransactionBindings.of(data),f.initial,f.home.homes.prepare(helper.control(0),null,f.initial),f.data.directory.ranges())) {
            complete(journal,f); block.set(true);
            var release=CompletableFuture.runAsync(() -> { try { journal.releaseCapture(f.data.f.groupId); } catch (IOException failure) { throw new CompletionException(failure); } });
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); assertThrows(IOException.class,journal::close); assertThrows(IOException.class,journal::reclaimRetired); assertThrows(IOException.class,() -> RatisDurableSnapshot.reopen(path,f.home.homes,RatisTransactionBindings.of(data)));
            } finally { resume.countDown(); }
            release.get(5,TimeUnit.SECONDS); assertTrue(journal.snapshot().participant(f.data.f.groupId).captureReleased()); journal.close();
            try (var reopened=RatisDurableSnapshot.reopen(path,f.home.homes,RatisTransactionBindings.of(data))) { assertTrue(reopened.snapshot().participant(f.data.f.groupId).captureReleased()); }
        } finally { resume.countDown(); }
    }
}
