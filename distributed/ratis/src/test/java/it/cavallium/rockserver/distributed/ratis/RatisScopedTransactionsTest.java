package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisScopedTransactionsTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final AtomicLong nanos=new AtomicLong(100);
    TransactionScope scope(RatisRetentionAdmissionTest.Fixture f,int key) { return new TransactionScope(List.of(TransactionScope.Span.point(f.column,new byte[]{(byte)key}))); }
    ReentrantLock lane(RatisRetentionAdmissionTest.Fixture f) throws Exception { var field=f.store.getClass().getDeclaredField("applyLane"); field.setAccessible(true); return (ReentrantLock)field.get(f.store); }
    RatisRetentionAdmissionTest.Fixture fixture(String name) throws Exception { return new RatisRetentionAdmissionTest.Fixture(root.resolve(name),RatisRangeStateMachine.RetentionLimits.defaults(),command -> {},nanos::get); }
    RatisScopedTransactions registry(RatisRetentionAdmissionTest.Fixture f) { return new RatisScopedTransactions(f.machine,new RatisScopedTransactions.Limits(1,4096,2L*1024*1024,Duration.ofSeconds(1)),nanos::get); }
    @Test void expiredOpeningRetainsItsCountAndScopeAdmissionUntilNativeCaptureReturns() throws Exception {
        try (var f=fixture("opening"); var registry=registry(f); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var lane=lane(f); lane.lock(); Future<ScopedTransactionRpc.Handle> pending;
            try {
                pending=executor.submit(() -> registry.open(new UUID(0,1),scope(f,0),Duration.ofMillis(1)));
                await(() -> registry.chargedHandles()==1); nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(2)); registry.expire();
                assertEquals(1,registry.chargedHandles()); assertFalse(pending.isDone());
                assertThrows(IOException.class,() -> registry.open(new UUID(0,2),scope(f,1),Duration.ofMillis(1)));
            } finally { lane.unlock(); }
            assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS)); assertEquals(0,registry.chargedHandles()); assertEquals(0,f.store.activeTrackedSnapshotPins());
            var replacement=registry.open(new UUID(0,3),scope(f,1),Duration.ofMillis(1)); registry.release(replacement); assertEquals(0,registry.chargedHandles());
        }
    }
    @Test void expiredPlannerKeepsPayloadAndExclusiveOwnershipUntilActualNativeReturn() throws Exception {
        try (var f=fixture("planning"); var registry=registry(f); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var handle=registry.open(new UUID(0,1),scope(f,0),Duration.ofMillis(1)); var lane=lane(f); lane.lock(); Future<DistributedCommand> pending;
            try {
                pending=executor.submit(() -> registry.plan(handle,f.put(0)));
                await(() -> registry.retainedPlanBytes()>0 && f.machine.proposalFootprintStats().candidates()==1);
                nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(2)); registry.expire(); assertFalse(pending.isDone());
                assertEquals(1,registry.chargedHandles()); assertEquals(2L*1024*1024,registry.retainedPlanBytes());
                assertEquals(1,f.machine.proposalFootprintStats().candidates()); registry.release(handle); assertEquals(1,registry.chargedHandles());
            } finally { lane.unlock(); }
            assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS)); assertEquals(0,registry.chargedHandles()); assertEquals(0,registry.retainedPlanBytes());
            assertEquals(0,f.store.activeTrackedSnapshotPins()); assertEquals(0,f.machine.proposalFootprintStats().candidates()); f.assertReleased();
        }
    }
    private static void await(java.util.function.BooleanSupplier condition) {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime()<deadline) LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        assertTrue(condition.getAsBoolean(),"Controlled native boundary did not become active");
    }
    @Test void expiryDoesNotMistakeSuccessfulPlanningPinCloseForHandleLoss() throws Exception {
        try (var f=fixture("plan-publication"); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var frozen=new CountDownLatch(1); var publish=new CountDownLatch(1);
            try (var registry=new RatisScopedTransactions(f.machine,new RatisScopedTransactions.Limits(1,4096,2L*1024*1024,Duration.ofSeconds(1)),nanos::get,(pin,command,deadline) -> {
                var accepted=f.machine.prepareScopedCommit(pin,command,deadline); frozen.countDown();
                if (!publish.await(5,TimeUnit.SECONDS)) { accepted.close(); throw new IOException("Fixture publication timed out"); }
                return accepted;
            })) {
                var handle=registry.open(new UUID(0,1),scope(f,0),Duration.ofSeconds(1));
                var planning=executor.submit(() -> registry.plan(handle,f.put(0)));
                try { assertTrue(frozen.await(5,TimeUnit.SECONDS)); assertEquals(0,f.store.activeTrackedSnapshotPins()); registry.expire(); }
                finally { publish.countDown(); }
                var accepted=planning.get(5,TimeUnit.SECONDS); assertArrayEquals(DistributedCommandCodec.encode(accepted),DistributedCommandCodec.encode(registry.plan(handle,f.put(0))));
                assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(accepted),1)); registry.release(handle); assertEquals(0,registry.chargedHandles()); f.assertReleased();
            }
        }
    }
    @Test void reusedOpenNonceGetsANewLeaseAndStaleCloseCannotReleaseItsReplacement() throws Exception {
        try (var f=fixture("nonce-reuse"); var registry=registry(f)) {
            UUID nonce=new UUID(0,1); var old=registry.open(nonce,scope(f,0),Duration.ofMillis(1));
            nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(2)); registry.expire(); assertEquals(0,registry.chargedHandles());
            var replacement=registry.open(nonce,scope(f,0),Duration.ofMillis(1)); assertNotEquals(old,replacement);
            registry.release(old); assertEquals(1,registry.chargedHandles()); assertNull(registry.get(replacement,f.column,new byte[]{0}));
            assertThrows(IOException.class,() -> registry.get(old,f.column,new byte[]{0})); registry.release(replacement); assertEquals(0,registry.chargedHandles());
        }
    }
    @Test void cachedUnsubmittedPlanRemainsBoundToItsNativeTermEvenWithoutALossCallback() throws Exception {
        try (var f=fixture("cached-term"); var registry=registry(f)) {
            var handle=registry.open(new UUID(0,1),scope(f,0),Duration.ofSeconds(1)); registry.plan(handle,f.put(0));
            f.leadership.set(new RatisRangeStateMachine.Leadership(2,true)); f.machine.notifyTermIndexUpdated(2,1);
            assertThrows(IOException.class,() -> registry.plan(handle,f.put(0))); assertEquals(0,registry.chargedHandles());
            assertEquals(0,f.machine.proposalFootprintStats().candidates()); f.assertReleased();
        }
    }
}
