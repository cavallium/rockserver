package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Reserved executor ownership only; no native store, Raft server or network fixture. */
class RatisSnapshotHelpServiceTest {
    final UUID group=new UUID(1,1);
    SnapshotFence fence(int id) {
        UUID column=new UUID(1,2); var control=new UUID(1,3);
        return new SnapshotFence(new UUID(1,4), new UUID(1,5), new UUID(2,id), new UUID(1,7), 1, new OperationId(new UUID(2,id),1,0), List.of(new DomainBinding(new UUID(1,6), 0, new byte[32], 1, new byte[32])), List.of(
                new SnapshotFence.Participant(new UUID(1,6), group, group, 1, new byte[32], new OperationId(control,0,id*2L), new OperationId(control,0,id*2L+1), new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{1}))))));
    }
    RWScheduler scheduler() { return RWScheduler.forTesting(1,1,1,4,1,"snapshot-help-test"); }
    @Test void coalescedViewsAndCancelledStopRetainActualRunningOwnership() throws Exception {
        var scheduler=scheduler(); var service=new RatisSnapshotHelpService(scheduler);
        var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var calls=new AtomicInteger();
        try {
            var first=service.submit(group,ScopedTransactionRpc.SnapshotBinding.of(fence(1)),() -> { calls.incrementAndGet(); entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); return true; });
            assertTrue(entered.await(5,TimeUnit.SECONDS)); var second=service.submit(group,ScopedTransactionRpc.SnapshotBinding.of(fence(1)),() -> { throw new AssertionError("Coalesced job must keep original action"); });
            assertTrue(first.cancel(false)); assertEquals(1,service.activeJobs());
            var stopped=service.stop(); assertFalse(stopped.isDone()); assertTrue(stopped.cancel(false)); var actual=service.stop(); assertFalse(actual.isDone());
            assertThrows(ExecutionException.class,() -> service.submit(group,ScopedTransactionRpc.SnapshotBinding.of(fence(2)),() -> true).get(5,TimeUnit.SECONDS));
            release.countDown(); actual.get(5,TimeUnit.SECONDS); assertTrue(second.get(5,TimeUnit.SECONDS)); assertEquals(1,calls.get()); assertEquals(0,service.activeJobs());
        } finally { release.countDown(); service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
    @Test void sixteenQueuedJobsShareOneGlobalBoundAndStopRemovesThemWithoutWork() throws Exception {
        var scheduler=scheduler(); var service=new RatisSnapshotHelpService(scheduler); var release=new CountDownLatch(1); var calls=new AtomicInteger();
        int workers=scheduler.poolSnapshot(RWScheduler.Pool.CONTROL).workerCount(); var entered=new CountDownLatch(workers);
        try {
            var executor=scheduler.executor(WorkloadProfile.CONTROL,OperationFamily.CONTROL,RequestContext.NO_TIMEOUT);
            for (int n=0;n<workers;n++) executor.execute(() -> { entered.countDown(); try { release.await(); } catch (InterruptedException failure) { Thread.currentThread().interrupt(); } });
            assertTrue(entered.await(5,TimeUnit.SECONDS)); var views=new ArrayList<CompletableFuture<Boolean>>();
            for (int n=0;n<RatisSnapshotHelpService.MAX_JOBS;n++) views.add(service.submit(group,ScopedTransactionRpc.SnapshotBinding.of(fence(n)),() -> { calls.incrementAndGet(); return true; }));
            assertEquals(16,service.activeJobs()); assertEquals(16,scheduler.queuedTasks(WorkloadProfile.CONTROL));
            assertThrows(ExecutionException.class,() -> service.submit(group,ScopedTransactionRpc.SnapshotBinding.of(fence(17)),() -> true).get(5,TimeUnit.SECONDS));
            var duplicate=service.submit(group,ScopedTransactionRpc.SnapshotBinding.of(fence(0)),() -> { throw new AssertionError(); }); assertEquals(16,service.activeJobs());
            service.stop().get(5,TimeUnit.SECONDS); assertEquals(0,service.activeJobs()); assertEquals(0,scheduler.queuedTasks(WorkloadProfile.CONTROL)); assertEquals(0,calls.get());
            for (var view:views) assertThrows(ExecutionException.class,() -> view.get(5,TimeUnit.SECONDS)); assertThrows(ExecutionException.class,() -> duplicate.get(5,TimeUnit.SECONDS));
        } finally { release.countDown(); service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
    @Test void pendingResultAndStopViewsHaveFiniteBudgetsEvenWhenCancelled() throws Exception {
        var scheduler=scheduler(); var service=new RatisSnapshotHelpService(scheduler); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); CompletableFuture<Void> drain=null;
        try {
            var first=service.submit(group,ScopedTransactionRpc.SnapshotBinding.of(fence(1)),() -> { entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); return true; });
            assertTrue(entered.await(5,TimeUnit.SECONDS)); var views=new ArrayList<CompletableFuture<Boolean>>(); views.add(first);
            for (int n=1;n<RatisSnapshotHelpService.MAX_WAITERS;n++) views.add(service.submit(group,ScopedTransactionRpc.SnapshotBinding.of(fence(1)),() -> { throw new AssertionError(); }));
            assertTrue(views.get(1).cancel(false)); assertThrows(ExecutionException.class,() -> service.submit(group,ScopedTransactionRpc.SnapshotBinding.of(fence(1)),() -> true).get(5,TimeUnit.SECONDS));
            drain=service.stop(); var stops=new ArrayList<CompletableFuture<Void>>(); stops.add(drain);
            for (int n=1;n<RatisSnapshotHelpService.MAX_WAITERS;n++) stops.add(service.stop());
            assertTrue(stops.get(1).cancel(false)); assertThrows(ExecutionException.class,() -> service.stop().get(5,TimeUnit.SECONDS));
            release.countDown(); drain.get(5,TimeUnit.SECONDS); assertTrue(first.get(5,TimeUnit.SECONDS)); service.stop().get(5,TimeUnit.SECONDS);
        } finally { release.countDown(); if (drain!=null) drain.get(5,TimeUnit.SECONDS); service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
    @Test void completedNativeActionDropsClosureBeforeReentrantBlockedPublication() throws Exception {
        var scheduler=scheduler(); var service=new RatisSnapshotHelpService(scheduler); var entered=new CountDownLatch(1); var releaseAction=new CountDownLatch(1); var published=new CountDownLatch(1); var releaseCallback=new CountDownLatch(1);
        try {
            var view=service.submit(group,ScopedTransactionRpc.SnapshotBinding.of(fence(1)),() -> { entered.countDown(); if (!releaseAction.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); return true; });
            assertTrue(entered.await(5,TimeUnit.SECONDS)); final Object task;
            synchronized (service) { var field=RatisSnapshotHelpService.class.getDeclaredField("tasks"); field.setAccessible(true); task=((Map<?,?>)field.get(service)).values().iterator().next(); }
            var callback=view.thenRun(() -> {
                try { service.stop().get(5,TimeUnit.SECONDS); published.countDown(); if (!releaseCallback.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); }
                catch (Exception failure) { throw new CompletionException(failure); }
            });
            releaseAction.countDown(); assertTrue(published.await(5,TimeUnit.SECONDS)); assertEquals(0,service.activeJobs());
            var field=task.getClass().getDeclaredField("action"); field.setAccessible(true); assertNull(field.get(task)); releaseCallback.countDown(); callback.get(5,TimeUnit.SECONDS);
        } finally { releaseAction.countDown(); releaseCallback.countDown(); service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
    @Test void queuedCancellationDrainsEveryJobBeforePublishingReentrantStopCallbacks() throws Exception {
        var scheduler=scheduler(); var service=new RatisSnapshotHelpService(scheduler); var release=new CountDownLatch(1);
        int workers=scheduler.poolSnapshot(RWScheduler.Pool.CONTROL).workerCount(); var entered=new CountDownLatch(workers);
        try {
            var executor=scheduler.executor(WorkloadProfile.CONTROL,OperationFamily.CONTROL,RequestContext.NO_TIMEOUT);
            for (int n=0;n<workers;n++) executor.execute(() -> { entered.countDown(); try { release.await(); } catch (InterruptedException failure) { Thread.currentThread().interrupt(); } });
            assertTrue(entered.await(5,TimeUnit.SECONDS)); var callbacks=new ArrayList<CompletableFuture<Void>>();
            for (int n=0;n<2;n++) {
                var view=service.submit(group,ScopedTransactionRpc.SnapshotBinding.of(fence(n)),() -> { throw new AssertionError("Queued action must not run"); });
                callbacks.add(view.handle((progress,failure) -> { assertNotNull(failure); try { service.stop().get(5,TimeUnit.SECONDS); } catch (Exception bad) { throw new CompletionException(bad); } return null; }));
            }
            service.stop().get(5,TimeUnit.SECONDS); for (var callback:callbacks) callback.get(5,TimeUnit.SECONDS); assertEquals(0,service.activeJobs());
        } finally { release.countDown(); service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
    @Test void failureReleasesCapacityForAnExactKeyRetry() throws Exception {
        var scheduler=scheduler(); var service=new RatisSnapshotHelpService(scheduler);
        try {
            assertThrows(ExecutionException.class,() -> service.submit(group,ScopedTransactionRpc.SnapshotBinding.of(fence(1)),() -> { throw new IOException("Unavailable prepared owner"); }).get(5,TimeUnit.SECONDS));
            assertEquals(0,service.activeJobs()); assertFalse(service.submit(group,ScopedTransactionRpc.SnapshotBinding.of(fence(1)),() -> false).get(5,TimeUnit.SECONDS)); assertEquals(0,service.activeJobs());
        } finally { service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
}
