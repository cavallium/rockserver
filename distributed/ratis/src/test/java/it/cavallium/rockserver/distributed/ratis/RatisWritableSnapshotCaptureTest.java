package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Original-cut writable capture over controlled query/append/native owners. */
class RatisWritableSnapshotCaptureTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisSnapshotCaptureRpcTest helper() {
        var h=new RatisSnapshotCaptureRpcTest(); h.root=root; h.helper.root=root; h.helper.helper.root=root; return h;
    }
    RatisRoutedClient.PreparedSnapshotCapture operation(RatisSnapshotCaptureRpcTest.Fixture f) throws Exception {
        return f.client.prepareWritableSnapshotCapture(f.client.snapshotTarget(f.fence,f.data.f.groupId),UUID.randomUUID(),Duration.ofSeconds(10));
    }
    void releaseGate(RatisSnapshotCaptureRpcTest h,RatisSnapshotCaptureRpcTest.Fixture f) throws Exception {
        var ready=f.home.homes.read(f.fence.fenceId()); f.home.transition(ready,ready.abort());
        try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) { h.helper.append(f.data,release); }
    }
    @Test void lostCaptureReplyRetainsOneOwnerAndOriginalCutThroughStagingPrepareAndCommit() throws Exception {
        var h=helper();
        try (var txhome=h.helper.helper.new Node("writable-txhome",true); var f=h.new Fixture("writable",true,true)) {
            var request=operation(f); f.lose=true; assertThrows(IOException.class,() -> f.client.executeWritableSnapshotCapture(request));
            assertEquals(1,f.data.f.store.activeTrackedSnapshotPins()); assertEquals(1,f.data.f.store.activeSnapshotPins());
            int reads=f.homes.get(); var result=f.client.executeWritableSnapshotCapture(request);
            assertEquals(result,f.client.executeWritableSnapshotCapture(request)); assertEquals(reads,f.homes.get());
            assertThrows(IllegalArgumentException.class,() -> f.client.executeSnapshotCapture(request));
            var readonly=new RatisSnapshotCaptureRequest(f.fence,request.request().owner(),request.request().snapshotId(),request.request().ttl());
            assertThrows(IOException.class,() -> f.client.executeSnapshotCapture(f.client.restoreSnapshotCapture(request.target(),readonly)));
            try (var transaction=f.data.routed.capturedTransaction(request,result)) {
                assertEquals(result.token().position(),transaction.snapshot()); assertArrayEquals(new byte[]{7},transaction.get(f.data.f.column,new byte[]{2}));
                releaseGate(h,f); var business=f.data.f.put(3); var staged=transaction.stage(business.mutations());
                assertArrayEquals(new byte[]{7},transaction.get(f.data.f.column,new byte[]{3})); assertNull(f.data.routed.read(f.data.f.column,new byte[]{3}));
                var accepted=transaction.participantIntent(business,staged); assertEquals(result.token().position(),accepted.acceptance().snapshot());
                var registration=new RatisRemoteParticipantAuthorizationTest().register(txhome,f.data,accepted);
                var proposal=transaction.authorizeParticipant(accepted,h.helper.helper.control(0),registration.transactionId());
                var vote=f.data.routed.executeParticipant(proposal); assertTrue(vote.prepared());
                var prepared=registration.prepared(f.data.f.groupId,accepted.payloadDigest(),vote.position());
                txhome.statuses.execute(txhome.statuses.prepare(new OperationId(new UUID(0,4),1,1),registration,prepared));
                var committed=prepared.decide(TransactionStatus.Phase.COMMIT);
                txhome.statuses.execute(txhome.statuses.prepare(new OperationId(new UUID(0,4),1,2),prepared,committed));
                f.data.routed.executeFinalization(f.data.routed.prepareFinalization(new ParticipantFinalization(txhome.f.groupId,1,f.data.f.groupId,h.helper.helper.control(1),committed)));
                assertArrayEquals(new byte[]{7},f.data.routed.read(f.data.f.column,new byte[]{3}));
            }
            f.client.releaseSnapshot(request.target(),result.token()); assertEquals(0,f.data.f.machine.snapshotPinStats().pins()); assertEquals(0,f.data.f.store.activeSnapshotPins());
        }
    }
    @Test void originalTrackedCutRejectsAbaWithoutRecaptureAndExpiresAtItsOriginalDeadline() throws Exception {
        var h=helper();
        try (var txhome=h.helper.helper.new Node("aba-txhome",true); var f=h.new Fixture("aba-capture",true,true)) {
            var request=operation(f); var result=f.client.executeWritableSnapshotCapture(request);
            try (var transaction=f.data.routed.capturedTransaction(request,result)) {
                var accepted=transaction.readCertification(new OperationId(f.data.f.session,1,100));
                var registration=new RatisRemoteParticipantAuthorizationTest().register(txhome,f.data,accepted); releaseGate(h,f);
                for (int n=0;n<2;n++) f.data.wire.command(transaction.descriptor(),new DistributedCommand(f.data.f.groupId,1,new byte[32],new OperationId(f.data.f.session,1,10+n),List.of(new Mutation(Mutation.Kind.PUT,f.data.f.column,new byte[]{2},new byte[]{(byte)(n==0 ? 8 : 7)}))));
                assertArrayEquals(new byte[]{7},transaction.get(f.data.f.column,new byte[]{2}));
                assertThrows(RatisPreparationUnavailableException.class,() -> transaction.authorizeParticipant(accepted,h.helper.helper.control(0),registration.transactionId()));
                assertNull(f.data.routed.preparedParticipant(f.data.f.groupId,registration.transactionId()));
            }
            f.clock.set(Duration.ofSeconds(10).toNanos());
            assertThrows(RatisSnapshotLostException.class,() -> f.client.executeWritableSnapshotCapture(request));
            f.data.f.machine.expireTransactions();
            assertEquals(0,f.data.f.store.activeSnapshotPins()); assertEquals(0,f.data.f.machine.snapshotPinStats().pins());
        }
    }
    @Test void writableTokensCannotUseImmutableReadsAndLeadershipLossNeverOpensANewCut() throws Exception {
        var h=helper();
        try (var f=h.new Fixture("lost-owner",true,true)) {
            var request=operation(f); var result=f.client.executeWritableSnapshotCapture(request);
            assertThrows(IOException.class,() -> f.client.snapshotGet(request.target(),result.token(),f.data.f.column,new byte[]{2}));
            try (var transaction=f.data.routed.capturedTransaction(request,result)) {
                f.data.f.machine.notifyNotLeader(List.of()); f.data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,false));
                assertThrows(IOException.class,() -> transaction.get(f.data.f.column,new byte[]{2}));
                assertThrows(RatisSnapshotLostException.class,() -> f.client.executeWritableSnapshotCapture(request));
            }
            f.client.releaseSnapshot(request.target(),result.token()); assertEquals(0,f.data.f.store.activeTrackedSnapshotPins());
        }
    }
    @Test void snapshotChargeWaitsForTheScopedNativeOwnerToActuallyDrain() throws Exception {
        var h=new RatisReadCertificationTest(); h.root=root;
        var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
        try (var f=h.new Fixture("shared-drain"); var executor=Executors.newVirtualThreadPerTaskExecutor();
             var registry=new RatisScopedTransactions(f.data.f.machine,RatisScopedTransactions.Limits.defaults(),() -> 0,f.data.f.machine::prepareScopedCommit,(pin,command) -> {
                 entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timed out"); return pin.stage(command);
             });
             var pins=new RatisSnapshotPins(f.data.f.machine.transactionOwnerIncarnation(),f.data.f.groupId,RatisSnapshotPins.Limits.defaults(),() -> 0,f.data.f.machine::captureSnapshotGate,
                     ReplicatedRangeStore.ScopedSnapshot::get,ReplicatedRangeStore.ScopedSnapshot::close,registry::captureSnapshot,registry::releaseAndDrain,registry::snapshotLive)) {
            var captured=pins.capture(f.snapshots.homes,f.fence,f.data.f.machine.transactionOwnerIncarnation(),UUID.randomUUID(),Duration.ofSeconds(10),() -> 0,RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED);
            var command=new DistributedCommand(f.data.f.groupId,1,new byte[32],new OperationId(captured.handle().id(),0,0),f.data.f.put(3).mutations());
            var pending=executor.submit(() -> registry.stage(captured.handle(),command));
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); long bytes=pins.stats().bytes(); pins.release(captured.token());
                assertEquals(1,pins.stats().pins()); assertEquals(bytes,pins.stats().bytes()); assertEquals(1,registry.chargedHandles()); assertEquals(1,f.data.f.store.activeTrackedSnapshotPins());
            } finally { release.countDown(); }
            assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS)); pins.expire();
            assertEquals(0,pins.stats().pins()); assertEquals(RatisSnapshotPins.TOMBSTONE_BYTES,pins.stats().bytes()); assertEquals(0,registry.chargedHandles()); assertEquals(0,f.data.f.store.activeSnapshotPins());
        } finally { release.countDown(); }
    }
    @Test void publishedOwnerRemainsChargedWhenFactoryFinallyFailsOnUnrelatedCleanup() throws Exception {
        var h=new RatisReadCertificationTest(); h.root=root;
        var fail=new java.util.concurrent.atomic.AtomicBoolean(true);
        try (var f=h.new Fixture("publication-failure");
             var registry=new RatisScopedTransactions(f.data.f.machine,RatisScopedTransactions.Limits.defaults(),() -> 0,f.data.f.machine::prepareScopedCommit,ReplicatedRangeStore.ScopedHistoryPin::stage,pin -> {
                 if (fail.get() && pin.scope().contains(f.data.f.column,new byte[]{5})) throw new IOException("Unrelated native close failed"); pin.close();
             });
             var pins=new RatisSnapshotPins(f.data.f.machine.transactionOwnerIncarnation(),f.data.f.groupId,RatisSnapshotPins.Limits.defaults(),() -> 0,f.data.f.machine::captureSnapshotGate,
                     ReplicatedRangeStore.ScopedSnapshot::get,ReplicatedRangeStore.ScopedSnapshot::close,registry::captureSnapshot,registry::releaseAndDrain,registry::snapshotLive)) {
            var old=registry.open(UUID.randomUUID(),new TransactionScope(List.of(TransactionScope.Span.point(f.data.f.column,new byte[]{5}))),Duration.ofSeconds(10));
            f.snapshots.wire.afterRead=() -> { try { assertThrows(IOException.class,() -> registry.releaseAndDrain(old)); } catch (Throwable problem) { throw new AssertionError(problem); } };
            var id=UUID.randomUUID();
            assertThrows(IOException.class,() -> pins.capture(f.snapshots.homes,f.fence,f.data.f.machine.transactionOwnerIncarnation(),id,Duration.ofSeconds(10),() -> 0,RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED));
            assertEquals(1,pins.stats().pins()); assertTrue(pins.stats().bytes()>RatisSnapshotPins.TOMBSTONE_BYTES);
            assertEquals(1,registry.chargedHandles()); assertEquals(1,f.data.f.store.activeTrackedSnapshotPins());
            fail.set(false); pins.expire(); registry.expire();
            assertEquals(0,pins.stats().pins()); assertEquals(RatisSnapshotPins.TOMBSTONE_BYTES,pins.stats().bytes()); assertEquals(0,registry.chargedHandles());
            f.snapshots.wire.afterRead=() -> { throw new AssertionError("Failed capture cannot silently recapture"); };
            assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.snapshots.homes,f.fence,f.data.f.machine.transactionOwnerIncarnation(),id,Duration.ofSeconds(10),() -> 0,RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED));
        } finally { fail.set(false); }
    }
    @Test void captureCannotPublishSuccessAfterTheTrackedOwnerClosesBeforeFactoryReturn() throws Exception {
        var h=new RatisReadCertificationTest(); h.root=root;
        try (var f=h.new Fixture("publication-closed");
             var registry=new RatisScopedTransactions(f.data.f.machine,RatisScopedTransactions.Limits.defaults(),() -> 0);
             var pins=new RatisSnapshotPins(f.data.f.machine.transactionOwnerIncarnation(),f.data.f.groupId,RatisSnapshotPins.Limits.defaults(),() -> 0,f.data.f.machine::captureSnapshotGate,
                     ReplicatedRangeStore.ScopedSnapshot::get,ReplicatedRangeStore.ScopedSnapshot::close,(id,homes,fence,opened,ttl,ownership) -> {
                         var handle=registry.captureSnapshot(id,homes,fence,opened,ttl,ownership); assertTrue(registry.releaseAndDrain(handle)); return handle;
                     },registry::releaseAndDrain,registry::snapshotLive)) {
            var id=UUID.randomUUID();
            assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.snapshots.homes,f.fence,f.data.f.machine.transactionOwnerIncarnation(),id,Duration.ofSeconds(10),() -> 0,RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED));
            assertEquals(0,pins.stats().pins()); assertEquals(RatisSnapshotPins.TOMBSTONE_BYTES,pins.stats().bytes()); assertEquals(0,f.data.f.store.activeTrackedSnapshotPins());
        }
    }
}
