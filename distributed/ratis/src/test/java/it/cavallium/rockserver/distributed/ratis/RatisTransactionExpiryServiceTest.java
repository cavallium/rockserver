package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.distributed.RangeDirectoryCodec;
import it.cavallium.rockserver.core.common.distributed.TransactionScope;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisTransactionExpiryServiceTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    @Test void timerQuantumExpiresIdleHandlesWithoutAnotherReadAndVisitsGroupsRoundRobin() throws Exception {
        var nanos=new AtomicLong(100);
        try (var first=fixture("first",nanos::get); var second=fixture("second",nanos::get)) {
            open(first); open(second);
            var supplied=new ArrayList<>(List.of(first.machine,second.machine));
            try (var env=new Env(supplied,1)) {
                supplied.clear(); // service owns an immutable membership cut
                nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(2));
                var timer=new RatisSessionClockServiceTest.ManualTimer(); env.service.start(timer,Duration.ofMillis(100));
                var gate=block(env.scheduler);
                try {
                    timer.fire(); var quantum=env.service.tick();
                    assertEquals(1,env.scheduler.queuedTasks(WorkloadProfile.CONTROL));
                    gate.release.countDown(); quantum.get(5,TimeUnit.SECONDS); idle(env.scheduler);
                    assertEquals(0,first.store.activeTrackedSnapshotPins());
                    assertEquals(1,second.store.activeTrackedSnapshotPins(),"One visit must not scan every group");
                    env.service.tick().get(5,TimeUnit.SECONDS); idle(env.scheduler);
                    assertEquals(0,second.store.activeTrackedSnapshotPins()); assertNull(env.service.lastFailure());
                    env.service.stop().get(5,TimeUnit.SECONDS); assertTrue(timer.cancelled); assertFalse(timer.isDisposed());
                } finally { gate.release.countDown(); }
            }
        }
    }

    @Test void runningExpiryCoalescesAndStopRetainsNativeOwnershipUntilItReturns() throws Exception {
        var nanos=new AtomicLong(100); var armed=new AtomicBoolean();
        var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
        LongSupplier time=() -> {
            if (armed.get()) {
                entered.countDown();
                try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Expiry gate timed out"); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
            }
            return nanos.get();
        };
        try (var f=fixture("running",time); var env=new Env(List.of(f.machine),1)) {
            open(f); nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(2)); armed.set(true);
            var timer=new RatisSessionClockServiceTest.ManualTimer(); env.service.start(timer,Duration.ofMillis(100)); timer.fire();
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); assertEquals(1,f.store.activeTrackedSnapshotPins());
                var view=env.service.tick(); assertTrue(view.cancel(false));
                var active=RatisTransactionExpiryService.class.getDeclaredField("active"); active.setAccessible(true);
                var task=active.get(env.service); var result=task.getClass().getDeclaredField("result"); result.setAccessible(true);
                var actual=(CompletableFuture<?>)result.get(task); int observers=actual.getNumberOfDependents();
                for (int i=0;i<100;i++) timer.fire();
                assertEquals(observers,actual.getNumberOfDependents(),"Periodic coalescing must not accumulate observers");
                assertEquals(0,env.scheduler.queuedTasks(WorkloadProfile.CONTROL));
                var drain=env.service.stop(); assertFalse(drain.isDone());
                assertTrue(timer.cancelled); assertEquals(1,f.store.activeTrackedSnapshotPins());
                assertTrue(drain.cancel(false)); var actualDrain=env.service.stop(); assertFalse(actualDrain.isDone());
                release.countDown(); actualDrain.get(5,TimeUnit.SECONDS);
                assertEquals(0,f.store.activeTrackedSnapshotPins()); assertNull(env.service.lastFailure());
                assertThrows(ExecutionException.class,() -> env.service.tick().get(5,TimeUnit.SECONDS));
            } finally { release.countDown(); }
        }
    }

    @Test void queuedStopRemovesOneCoalescedAdmissionAndSettlesObservers() throws Exception {
        try (var env=new Env(List.of(),1)) {
            var gate=block(env.scheduler);
            try {
                var queued=env.service.tick(); var coalesced=env.service.tick();
                assertEquals(1,env.scheduler.queuedTasks(WorkloadProfile.CONTROL));
                env.service.stop().get(5,TimeUnit.SECONDS);
                assertEquals(0,env.scheduler.queuedTasks(WorkloadProfile.CONTROL));
                assertInstanceOf(CancellationException.class,assertThrows(ExecutionException.class,() -> queued.get(5,TimeUnit.SECONDS)).getCause());
                assertInstanceOf(CancellationException.class,assertThrows(ExecutionException.class,() -> coalesced.get(5,TimeUnit.SECONDS)).getCause());
                assertNotNull(env.service.lastFailure());
            } finally { gate.release.countDown(); }
        }
    }

    @Test void schedulerShutdownRejectsQueuedExpiryAndSettlesDrain() throws Exception {
        try (var env=new Env(List.of(),1)) {
            var gate=block(env.scheduler);
            try {
                var queued=env.service.tick(); assertEquals(1,env.scheduler.queuedTasks(WorkloadProfile.CONTROL));
                env.scheduler.disposeNow();
                assertThrows(ExecutionException.class,() -> queued.get(5,TimeUnit.SECONDS));
                env.service.stop().get(5,TimeUnit.SECONDS); assertNotNull(env.service.lastFailure());
            } finally { gate.release.countDown(); }
        }
    }

    @Test void invalidMembershipVisitsAndPeriodsFailBeforeScheduling() throws Exception {
        try (var env=new Env(List.of(),1)) {
            assertThrows(IllegalArgumentException.class,() -> new RatisTransactionExpiryService(env.scheduler,List.of(),0));
            assertThrows(IllegalArgumentException.class,() -> new RatisTransactionExpiryService(env.scheduler,List.of(),33));
            assertThrows(IllegalArgumentException.class,() -> new RatisTransactionExpiryService(env.scheduler,
                    Collections.nCopies(RangeDirectoryCodec.MAX_RANGES+2,null),1));
            assertThrows(IllegalArgumentException.class,() -> env.service.start(new RatisSessionClockServiceTest.ManualTimer(),Duration.ZERO));
            env.service.tick().get(5,TimeUnit.SECONDS); idle(env.scheduler); assertNull(env.service.lastFailure());
        }
    }

    private RatisRetentionAdmissionTest.Fixture fixture(String name,LongSupplier nanos) throws Exception {
        return new RatisRetentionAdmissionTest.Fixture(root.resolve(name),RatisRangeStateMachine.RetentionLimits.defaults(),command -> {},nanos);
    }
    private static void open(RatisRetentionAdmissionTest.Fixture fixture) throws Exception {
        byte[] scope=ScopedTransactionRpc.scopeBytes(new TransactionScope(List.of(TransactionScope.Span.point(fixture.column,new byte[]{0}))));
        var body=ByteBuffer.allocate(24+scope.length); ScopedTransactionRpc.uuid(body,UUID.randomUUID()); body.putLong(1).put(scope);
        var request=Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.OPEN,body.array())));
        fixture.machine.queryLinearizable(request).get(5,TimeUnit.SECONDS);
        assertEquals(1,fixture.store.activeTrackedSnapshotPins());
    }
    private record Gate(CountDownLatch release) {}
    private static Gate block(RWScheduler scheduler) throws Exception {
        int workers=scheduler.poolSnapshot(RWScheduler.Pool.CONTROL).workerCount(); assertTrue(workers>0);
        var entered=new CountDownLatch(workers); var release=new CountDownLatch(1);
        for (int i=0;i<workers;i++) scheduler.executor(WorkloadProfile.CONTROL,OperationFamily.CONTROL,RequestContext.NO_TIMEOUT).execute(() -> {
            entered.countDown(); try { release.await(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        });
        assertTrue(entered.await(5,TimeUnit.SECONDS)); return new Gate(release);
    }
    private static void idle(RWScheduler scheduler) throws Exception {
        var settled=new CompletableFuture<Void>();
        scheduler.executor(WorkloadProfile.CONTROL,OperationFamily.CONTROL,RequestContext.NO_TIMEOUT).execute(() -> settled.complete(null));
        settled.get(5,TimeUnit.SECONDS);
    }
    private static final class Env implements AutoCloseable {
        final RWScheduler scheduler=RWScheduler.forTesting(1,1,1,4,1,"transaction-expiry-test");
        final RatisTransactionExpiryService service;
        Env(List<RatisRangeStateMachine> machines,int visits) { service=new RatisTransactionExpiryService(scheduler,machines,visits); }
        public void close() throws Exception {
            service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5));
        }
    }
}
