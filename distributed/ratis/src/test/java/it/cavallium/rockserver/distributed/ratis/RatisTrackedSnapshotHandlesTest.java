package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Shared scoped-registry ownership for tracked snapshot cuts, with controlled native/proxy work. */
class RatisTrackedSnapshotHandlesTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final Duration ttl=Duration.ofSeconds(10);
    RatisReadCertificationTest helper() { var helper=new RatisReadCertificationTest(); helper.root=root; return helper; }
    @Test void captureReservesBeforeNativeWorkAndRetriesOnlyTheSameOriginalDefinitionAndDeadline() throws Exception {
        var helper=helper();
        try (var f=helper.new Fixture("adoption")) {
            var clock=new AtomicLong(); var reads=new AtomicInteger(); UUID id=UUID.randomUUID();
            try (var registry=new RatisScopedTransactions(f.data.f.machine,RatisScopedTransactions.Limits.defaults(),clock::get)) {
                f.snapshots.wire.afterRead=() -> { assertFalse(Thread.holdsLock(registry)); assertEquals(1,registry.chargedHandles()); reads.incrementAndGet(); };
                var handle=registry.captureSnapshot(id,f.snapshots.homes,f.fence,0,ttl); assertEquals(1,reads.get()); assertEquals(1,f.data.f.store.activeTrackedSnapshotPins());
                assertArrayEquals(new byte[]{7},registry.get(handle,f.data.f.column,new byte[]{2}));
                f.snapshots.wire.afterRead=() -> { throw new AssertionError("Exact retry must not recapture or reread HOME"); };
                clock.set(ttl.toNanos()-1); var replayed=new AtomicReference<ScopedTransactionRpc.Handle>();
                assertEquals(handle,registry.captureSnapshot(id,f.snapshots.homes,f.fence,0,ttl,value -> { assertFalse(Thread.holdsLock(registry)); replayed.set(value); }));
                assertEquals(handle,replayed.get()); assertTrue(registry.snapshotLive(handle));
                assertThrows(IllegalArgumentException.class,() -> registry.open(id,f.fence.participant(f.data.f.groupId).scope(),ttl));
                assertThrows(IllegalArgumentException.class,() -> registry.captureSnapshot(id,f.snapshots.homes,f.fence,1,ttl));
                assertThrows(IllegalArgumentException.class,() -> registry.captureSnapshot(id,f.snapshots.homes,f.fence,0,Duration.ofSeconds(11)));
                var changed=new SnapshotFence(f.fence.clusterId(),f.fence.databaseId(),f.fence.domainId(),f.fence.fenceId(),f.fence.homeGroup(),f.fence.homeEpoch(),f.fence.operation(),f.fence.directoryGeneration()+1,f.fence.schemaEpoch(),f.fence.schemaDigest(),f.fence.participants());
                assertThrows(IllegalArgumentException.class,() -> registry.captureSnapshot(id,f.snapshots.homes,changed,0,ttl));
                clock.set(ttl.toNanos()); registry.expire(); assertEquals(0,registry.chargedHandles()); assertEquals(0,f.data.f.store.activeTrackedSnapshotPins());
                assertThrows(IOException.class,() -> registry.captureSnapshot(id,f.snapshots.homes,f.fence,0,ttl)); assertEquals(0,registry.chargedHandles());
                assertTrue(registry.releaseAndDrain(handle)); assertTrue(registry.releaseAndDrain(handle));
            }
        }
    }
    @Test void exhaustedMetadataAdmissionNeverStartsTrackedCaptureAndOrdinaryNonceCannotBecomeSnapshot() throws Exception {
        var helper=helper();
        try (var f=helper.new Fixture("admission")) {
            var limits=new RatisScopedTransactions.Limits(1,1,DistributedCommandCodec.MAX_ENCODED_BYTES,RatisScopedTransactions.STAGING_BUILD_BYTES,ttl);
            try (var registry=new RatisScopedTransactions(f.data.f.machine,limits,() -> 0)) {
                f.snapshots.wire.afterRead=() -> { throw new AssertionError("Admission failure must precede native capture"); };
                assertThrows(IOException.class,() -> registry.captureSnapshot(UUID.randomUUID(),f.snapshots.homes,f.fence,0,ttl));
                assertEquals(0,registry.chargedHandles()); assertEquals(0,f.data.f.store.activeTrackedSnapshotPins());
            }
            f.snapshots.wire.afterRead=null;
            try (var registry=new RatisScopedTransactions(f.data.f.machine,RatisScopedTransactions.Limits.defaults(),() -> 0)) {
                var id=UUID.randomUUID(); var scope=f.fence.participant(f.data.f.groupId).scope(); var ordinary=registry.open(id,scope,ttl);
                assertThrows(IllegalArgumentException.class,() -> registry.captureSnapshot(id,f.snapshots.homes,f.fence,0,ttl));
                assertTrue(registry.releaseAndDrain(ordinary));
            }
        }
    }
    @Test void blockedCaptureKeepsItsReservationUntilActualReturnEvenAfterExpiration() throws Exception {
        var helper=helper();
        try (var f=helper.new Fixture("busy-capture"); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var clock=new AtomicLong(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            try (var registry=new RatisScopedTransactions(f.data.f.machine,RatisScopedTransactions.Limits.defaults(),clock::get)) {
                f.snapshots.wire.afterRead=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); } catch (InterruptedException interrupted) { throw new AssertionError(interrupted); } };
                var pending=executor.submit(() -> registry.captureSnapshot(UUID.randomUUID(),f.snapshots.homes,f.fence,0,ttl));
                try {
                    assertTrue(entered.await(5,TimeUnit.SECONDS)); assertEquals(1,registry.chargedHandles());
                    clock.set(ttl.toNanos()); registry.expire(); assertEquals(1,registry.chargedHandles()); assertFalse(pending.isDone());
                } finally { release.countDown(); }
                assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS));
                assertEquals(0,registry.chargedHandles()); assertEquals(0,f.data.f.store.activeTrackedSnapshotPins());
            } finally { release.countDown(); }
        }
    }
    @Test void releaseAndDrainDoesNotDonateBusyStagingOrPretendMissingHandleMeansDrained() throws Exception {
        var helper=helper();
        try (var f=helper.new Fixture("busy-stage"); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            try (var registry=new RatisScopedTransactions(f.data.f.machine,RatisScopedTransactions.Limits.defaults(),() -> 0,f.data.f.machine::prepareScopedCommit,(pin,command) -> {
                entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); return pin.stage(command);
            })) {
                var handle=registry.captureSnapshot(UUID.randomUUID(),f.snapshots.homes,f.fence,0,ttl);
                var command=new DistributedCommand(f.data.f.groupId,1,new byte[32],new OperationId(handle.id(),0,0),List.of(new Mutation(Mutation.Kind.PUT,f.data.f.column,new byte[]{2},new byte[]{9})));
                var pending=executor.submit(() -> registry.stage(handle,command));
                try {
                    assertTrue(entered.await(5,TimeUnit.SECONDS)); assertFalse(registry.releaseAndDrain(handle)); assertFalse(registry.releaseAndDrain(handle));
                    assertEquals(1,registry.chargedHandles()); assertEquals(RatisScopedTransactions.STAGING_BUILD_BYTES,registry.retainedStagingBytes());
                    assertEquals(1,f.data.f.store.activeTrackedSnapshotPins());
                } finally { release.countDown(); }
                assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS));
                assertTrue(registry.releaseAndDrain(handle)); assertEquals(0,registry.chargedHandles()); assertEquals(0,registry.retainedStagingBytes()); assertEquals(0,f.data.f.store.activeTrackedSnapshotPins());
            } finally { release.countDown(); }
        }
    }
    @Test void releaseAndDrainRetainsPlanningOwnershipUntilTheNativePlanActuallyReturns() throws Exception {
        var helper=helper();
        try (var f=helper.new Fixture("busy-plan"); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            try (var registry=new RatisScopedTransactions(f.data.f.machine,RatisScopedTransactions.Limits.defaults(),() -> 0,(pin,command,deadline) -> {
                entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); return f.data.f.machine.prepareScopedCommit(pin,command,deadline);
            })) {
                var handle=registry.captureSnapshot(UUID.randomUUID(),f.snapshots.homes,f.fence,0,ttl); f.releaseGate();
                var command=new DistributedCommand(f.data.f.groupId,1,new byte[32],new OperationId(f.data.f.session,1,100),List.of(new Mutation(Mutation.Kind.PUT,f.data.f.column,new byte[]{2},new byte[]{9})));
                var pending=executor.submit(() -> registry.plan(handle,command));
                try {
                    assertTrue(entered.await(5,TimeUnit.SECONDS)); assertFalse(registry.releaseAndDrain(handle)); assertFalse(registry.releaseAndDrain(handle));
                    assertEquals(1,registry.chargedHandles()); assertEquals(DistributedCommandCodec.MAX_ENCODED_BYTES,registry.retainedPlanBytes());
                } finally { release.countDown(); }
                assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS));
                assertTrue(registry.releaseAndDrain(handle)); assertEquals(0,registry.chargedHandles()); assertEquals(0,registry.retainedPlanBytes());
                assertEquals(0,f.data.f.store.activeTrackedSnapshotPins()); helper.helper.released(f.data);
            } finally { release.countDown(); }
        }
    }
    @Test void publishedCaptureOwnershipSurvivesAnUnrelatedRetiredCloseFailure() throws Exception {
        var helper=helper();
        try (var f=helper.new Fixture("publication-failure"); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var fail=new AtomicBoolean(true); var closes=new AtomicInteger();
            var failedPin=new AtomicReference<ReplicatedRangeStore.ScopedHistoryPin>(); var published=new AtomicReference<ScopedTransactionRpc.Handle>();
            try (var registry=new RatisScopedTransactions(f.data.f.machine,RatisScopedTransactions.Limits.defaults(),() -> 0,f.data.f.machine::prepareScopedCommit,ReplicatedRangeStore.ScopedHistoryPin::stage,pin -> {
                closes.incrementAndGet(); failedPin.compareAndSet(null,pin);
                if (fail.get() && failedPin.get()==pin) throw new IOException("Unrelated retired native close failed");
                pin.close();
            })) {
                var old=registry.open(UUID.randomUUID(),f.fence.participant(f.data.f.groupId).scope(),ttl);
                f.snapshots.wire.afterRead=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); } catch (InterruptedException interrupted) { throw new AssertionError(interrupted); } };
                var pending=executor.submit(() -> registry.captureSnapshot(UUID.randomUUID(),f.snapshots.homes,f.fence,0,ttl,handle -> {
                    assertFalse(Thread.holdsLock(registry)); published.set(handle);
                }));
                try {
                    assertTrue(entered.await(5,TimeUnit.SECONDS)); assertThrows(IOException.class,() -> registry.releaseAndDrain(old)); assertEquals(2,registry.chargedHandles());
                } finally { release.countDown(); }
                var failure=assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS)); assertInstanceOf(IOException.class,failure.getCause());
                var retained=published.get(); assertNotNull(retained,"Ownership is published before unrelated finally cleanup can fail");
                assertEquals(2,registry.chargedHandles()); assertEquals(2,f.data.f.store.activeTrackedSnapshotPins());
                int attempted=closes.get(); assertTrue(registry.snapshotLive(retained)); assertEquals(attempted,closes.get(),"Lease inspection never cleans unrelated retired owners");
                assertThrows(IOException.class,() -> registry.releaseAndDrain(retained));
                assertEquals(1,registry.chargedHandles()); assertEquals(1,f.data.f.store.activeTrackedSnapshotPins(),"The received handle drains its own native owner despite unrelated failure");
                assertTrue(registry.releaseAndDrain(retained));
                fail.set(false); assertTrue(registry.releaseAndDrain(old)); assertEquals(0,registry.chargedHandles()); assertEquals(0,f.data.f.store.activeTrackedSnapshotPins());
            } finally { release.countDown(); }
        }
    }
    @Test void snapshotLiveChecksExactOwnershipWithoutClosingMissingOrInvalidatedHandles() throws Exception {
        var helper=helper();
        try (var f=helper.new Fixture("live-check"); var registry=new RatisScopedTransactions(f.data.f.machine,RatisScopedTransactions.Limits.defaults(),() -> 0)) {
            var handle=registry.captureSnapshot(UUID.randomUUID(),f.snapshots.homes,f.fence,0,ttl); assertTrue(registry.snapshotLive(handle));
            assertFalse(registry.snapshotLive(new ScopedTransactionRpc.Handle(handle.owner(),handle.id(),handle.generation(),new ApplyPosition(handle.position().term(),handle.position().index()+1))));
            assertTrue(registry.releaseAndDrain(handle)); assertFalse(registry.snapshotLive(handle));
            var live=registry.captureSnapshot(UUID.randomUUID(),f.snapshots.homes,f.fence,0,ttl);
            f.data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,false)); assertFalse(registry.snapshotLive(live));
            assertEquals(1,registry.chargedHandles()); assertEquals(1,f.data.f.store.activeTrackedSnapshotPins(),"Owner loss inspection does not donate outstanding native ownership");
        }
    }
    @Test void failedNativeCloseKeepsOwnershipAndRepeatedReleaseCanFinishTheDrain() throws Exception {
        var helper=helper();
        try (var f=helper.new Fixture("failed-close")) {
            var fail=new AtomicBoolean(true);
            try (var registry=new RatisScopedTransactions(f.data.f.machine,RatisScopedTransactions.Limits.defaults(),() -> 0,f.data.f.machine::prepareScopedCommit,ReplicatedRangeStore.ScopedHistoryPin::stage,pin -> {
                if (fail.get()) throw new IOException("Controlled native close failed"); pin.close();
            })) {
                var handle=registry.captureSnapshot(UUID.randomUUID(),f.snapshots.homes,f.fence,0,ttl);
                assertThrows(IOException.class,() -> registry.releaseAndDrain(handle)); assertEquals(1,registry.chargedHandles()); assertEquals(1,f.data.f.store.activeTrackedSnapshotPins());
                assertThrows(IOException.class,() -> registry.releaseAndDrain(handle)); assertEquals(1,registry.chargedHandles());
                fail.set(false); assertTrue(registry.releaseAndDrain(handle)); assertEquals(0,registry.chargedHandles()); assertEquals(0,f.data.f.store.activeTrackedSnapshotPins());
                assertTrue(registry.releaseAndDrain(new ScopedTransactionRpc.Handle(UUID.randomUUID(),handle.id(),handle.generation(),handle.position())));
            }
        }
    }
}
