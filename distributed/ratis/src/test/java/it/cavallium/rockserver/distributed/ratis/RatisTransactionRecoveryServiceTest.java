package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisTransactionRecoveryServiceTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RWScheduler scheduler() { return RWScheduler.forTesting(1,1,1,4,1,"transaction-recovery-test"); }
    static void idle(RWScheduler scheduler) throws Exception { var done=new CompletableFuture<Void>(); scheduler.executor(WorkloadProfile.CONTROL,OperationFamily.CONTROL,RequestContext.NO_TIMEOUT).execute(() -> done.complete(null)); done.get(5,TimeUnit.SECONDS); }
    @Test void dynamicQuantumCoalescesCancellationAndDrainsActualWork() throws Exception {
        var scheduler=scheduler(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var calls=new java.util.concurrent.atomic.AtomicInteger();
        var service=new RatisTransactionRecoveryService(scheduler,() -> { calls.incrementAndGet(); entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); });
        try {
            assertThrows(IllegalStateException.class,service::remaining);
            var first=service.tick(); assertTrue(entered.await(5,TimeUnit.SECONDS)); var second=service.tick(); assertTrue(first.cancel(false));
            var stopped=service.stop(); assertFalse(stopped.isDone()); assertTrue(stopped.cancel(false)); var actual=service.stop(); assertFalse(actual.isDone());
            release.countDown(); actual.get(5,TimeUnit.SECONDS); second.get(5,TimeUnit.SECONDS); idle(scheduler); assertEquals(1,calls.get());
            assertThrows(ExecutionException.class,() -> service.tick().get(5,TimeUnit.SECONDS));
        } finally { release.countDown(); service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
    private static void rejected(CompletableFuture<?> view,String message) throws Exception {
        var failure=assertThrows(ExecutionException.class,() -> view.get(5,TimeUnit.SECONDS));
        assertInstanceOf(RejectedExecutionException.class,failure.getCause()); assertEquals(message,failure.getCause().getMessage());
    }
    @Test void blockedQuantumBoundsTickAndDrainViewsEvenWhenCallersCancelOrFloodTimers() throws Exception {
        var scheduler=scheduler(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var calls=new java.util.concurrent.atomic.AtomicInteger();
        var service=new RatisTransactionRecoveryService(scheduler,() -> {
            calls.incrementAndGet(); entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout");
        });
        try {
            var views=new ArrayList<CompletableFuture<Void>>(); views.add(service.tick()); assertTrue(entered.await(5,TimeUnit.SECONDS));
            for (int i=1;i<RatisTransactionRecoveryService.MAX_WAITERS;i++) views.add(service.tick());
            var active=RatisTransactionRecoveryService.class.getDeclaredField("active"); active.setAccessible(true); var task=active.get(service);
            var resultField=task.getClass().getDeclaredField("result"); resultField.setAccessible(true);
            var result=(CompletableFuture<?>)resultField.get(task); int observers=result.getNumberOfDependents();
            assertEquals(RatisTransactionRecoveryService.MAX_WAITERS,observers);
            for (var view:views) assertTrue(view.cancel(false));
            var timer=new RatisSessionClockServiceTest.ManualTimer(); service.start(timer,Duration.ofSeconds(1));
            for (int i=0;i<100;i++) { timer.fire(); rejected(service.tick(),"Recovery tick waiter capacity exhausted"); }
            assertEquals(observers,result.getNumberOfDependents()); assertEquals(0,scheduler.queuedTasks(WorkloadProfile.CONTROL));
            for (int i=0;i<RatisTransactionRecoveryService.MAX_WAITERS;i++) {
                var drain=service.stop(); assertFalse(drain.isDone()); assertTrue(drain.cancel(false));
            }
            for (int i=0;i<100;i++) rejected(service.stop(),"Recovery drain waiter capacity exhausted");
            assertTrue(timer.cancelled); assertEquals(1,calls.get());
            release.countDown(); idle(scheduler); service.stop().get(5,TimeUnit.SECONDS); assertNull(service.lastFailure());
        } finally { release.countDown(); idle(scheduler); service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
    @Test void resultAndStopCallbacksCanReenterWithoutDonatingActualTaskOwnership() throws Exception {
        for (boolean stopBeforeReturn:new boolean[]{false,true}) {
            var scheduler=scheduler(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var calls=new java.util.concurrent.atomic.AtomicInteger();
            var service=new RatisTransactionRecoveryService(scheduler,() -> {
                calls.incrementAndGet(); entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout");
            });
            try {
                var first=service.tick(); assertTrue(entered.await(5,TimeUnit.SECONDS));
                var callback=first.thenRun(() -> {
                    assertFalse(Thread.holdsLock(service));
                    if (stopBeforeReturn) assertTrue(service.tick().isCompletedExceptionally());
                    else service.tick().join(); // completion publication still coalesces into this actual task
                    service.stop().thenRun(() -> { assertFalse(Thread.holdsLock(service)); service.stop().join(); }).join();
                    assertEquals(1,calls.get());
                });
                var drainCallback=stopBeforeReturn ? service.stop().thenRun(() -> {
                    assertFalse(Thread.holdsLock(service)); service.stop().join(); assertTrue(service.tick().isCompletedExceptionally());
                }) : CompletableFuture.<Void>completedFuture(null);
                release.countDown(); callback.get(5,TimeUnit.SECONDS); drainCallback.get(5,TimeUnit.SECONDS); idle(scheduler); assertEquals(1,calls.get());
            } finally { release.countDown(); idle(scheduler); service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
        }
    }
    @Test void timerQuantaFinishRecoveryWithoutConsumingAndRetireCompletedPolling() throws Exception {
        var helper=new RatisTransactionRecoveryTest(); helper.root=root;
        try (var f=helper.new Fixture("timer"); var recovery=f.recovery()) {
            var scheduler=scheduler(); var service=new RatisTransactionRecoveryService(scheduler,List.of(recovery),1);
            try {
                var timer=new RatisSessionClockServiceTest.ManualTimer(); service.start(timer,Duration.ofMillis(100)); var reads=new java.util.concurrent.atomic.AtomicInteger(); f.home.wire.afterRead=reads::incrementAndGet;
                for (int n=0;n<16 && service.remaining()>0;n++) { timer.fire(); service.tick().get(5,TimeUnit.SECONDS); idle(scheduler); }
                assertEquals(0,service.remaining()); var home=f.home.statuses.read(f.plan.initial().transactionId()); assertTrue(home.fullyFinalized()); assertFalse(home.consumed()); int before=reads.get();
                for (int n=0;n<3;n++) { timer.fire(); service.tick().get(5,TimeUnit.SECONDS); idle(scheduler); } assertEquals(before,reads.get(),"Completed transactions no longer issue background reads");
                service.stop().get(5,TimeUnit.SECONDS); assertTrue(timer.cancelled); assertNotNull(f.journal.snapshot().participant(f.data.f.groupId).outcome()); assertNull(service.lastFailure());
            } finally { service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
        }
    }
    @Test void cancelledViewsAndStopWaitForActualRemoteAuthorizationAndDoNotCloseBorrowedOwners() throws Exception {
        var helper=new RatisTransactionRecoveryTest(); helper.root=root;
        try (var f=helper.new Fixture("running"); var later=helper.new Fixture("later")) {
            f.journal.executeHome(); var abort=f.plan.initial().decide(TransactionStatus.Phase.ABORT); f.journal.queueRecovery(abort); f.journal.executeHome();
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            f.data.wire.beforeFinalization=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); } catch (InterruptedException failure) { throw new AssertionError(failure); } };
            try (var recovery=new RatisTransactionRecovery(f.journal,f.home.statuses,RatisTransactionBindings.of(f.data.routed),RatisTransactionRecoveryTest::next); var laterRecovery=later.recovery()) {
                var scheduler=scheduler(); var service=new RatisTransactionRecoveryService(scheduler,List.of(recovery,laterRecovery),2);
                try {
                    var view=service.tick(); assertTrue(entered.await(5,TimeUnit.SECONDS)); assertTrue(view.cancel(false)); assertThrows(IOException.class,recovery::close);
                    var drain=service.stop(); assertFalse(drain.isDone()); assertTrue(drain.cancel(false)); var actual=service.stop(); assertFalse(actual.isDone());
                    release.countDown(); actual.get(5,TimeUnit.SECONDS); idle(scheduler); f.data.wire.beforeFinalization=null; assertNotNull(f.journal.snapshot().participant(f.data.f.groupId).outcome()); assertEquals(0,later.home.wire.index,"Stop must not begin the next transaction in this quantum");
                    helper.drain(f,recovery); assertFalse(f.journal.snapshot().observedHome().consumed()); assertThrows(ExecutionException.class,() -> service.tick().get(5,TimeUnit.SECONDS));
                } finally { release.countDown(); service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
            }
        }
    }
    @Test void aFailingTransactionRotatesBehindOtherRecoveries() throws Exception {
        var helper=new RatisTransactionRecoveryTest(); helper.root=root;
        try (var first=helper.new Fixture("first"); var second=helper.new Fixture("second")) {
            first.journal.executeHome(); second.journal.executeHome();
            first.data.wire.beforeFinalization=() -> { throw new IllegalStateException("Qualified finalization RPC unavailable"); };
            try (var failed=new RatisTransactionRecovery(first.journal,first.home.statuses,RatisTransactionBindings.of(first.data.routed),RatisTransactionRecoveryTest::next); var healthy=second.recovery()) {
                var scheduler=scheduler(); var service=new RatisTransactionRecoveryService(scheduler,List.of(failed,healthy),1);
                try {
                    for (int n=0;n<16 && service.remaining()>1;n++) { try { service.tick().get(5,TimeUnit.SECONDS); } catch (ExecutionException expected) { assertInstanceOf(IOException.class,expected.getCause()); } idle(scheduler); }
                    assertEquals(1,service.remaining()); assertTrue(second.journal.snapshot().observedHome().fullyFinalized()); assertFalse(second.journal.snapshot().observedHome().consumed()); assertEquals(TransactionStatus.Phase.ABORT,first.journal.snapshot().observedHome().phase());
                } finally { service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
            }
        }
    }
    @Test void queuedStopRemovesOneCoalescedAdmission() throws Exception {
        var scheduler=scheduler(); var service=new RatisTransactionRecoveryService(scheduler,List.of(),1); var release=new CountDownLatch(1); int workers=scheduler.poolSnapshot(RWScheduler.Pool.CONTROL).workerCount(); var entered=new CountDownLatch(workers);
        try {
            for (int n=0;n<workers;n++) scheduler.executor(WorkloadProfile.CONTROL,OperationFamily.CONTROL,RequestContext.NO_TIMEOUT).execute(() -> { entered.countDown(); try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }); assertTrue(entered.await(5,TimeUnit.SECONDS));
            var first=service.tick(); var second=service.tick();
            for (int i=2;i<RatisTransactionRecoveryService.MAX_WAITERS;i++) assertFalse(service.tick().isDone());
            assertTrue(first.cancel(false)); rejected(service.tick(),"Recovery tick waiter capacity exhausted");
            assertEquals(1,scheduler.queuedTasks(WorkloadProfile.CONTROL)); service.stop().get(5,TimeUnit.SECONDS); assertEquals(0,scheduler.queuedTasks(WorkloadProfile.CONTROL));
            assertThrows(CancellationException.class,() -> first.get(5,TimeUnit.SECONDS)); assertThrows(ExecutionException.class,() -> second.get(5,TimeUnit.SECONDS));
        } finally { release.countDown(); service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
}
