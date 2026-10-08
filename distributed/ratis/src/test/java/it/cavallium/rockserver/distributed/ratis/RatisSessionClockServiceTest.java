package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import static org.junit.jupiter.api.Assertions.*;

class RatisSessionClockServiceTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void sharedSchedulerAdvancesClockAndCompletesFrozenRoundBeforeAdvancingAgain() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root)) {
            var wall=new AtomicLong(20);
            try (var env=new Env(root,f,wall)) {
                var one=env.service.tick().get(5,TimeUnit.SECONDS);
                assertEquals(RatisCatalogueClock.Outcome.APPLIED,one.clock().outcome()); assertEquals(1,one.propagation().completed());
                wall.set(30); var two=env.service.tick().get(5,TimeUnit.SECONDS);
                assertNull(two.clock()); assertTrue(two.propagation().complete());
                assertEquals(20,f.catalogue.epochs.read().logicalTimeMillis(),"Incomplete round pins its source target");
                assertEquals(30,env.service.tick().get(5,TimeUnit.SECONDS).clock().authority().logicalTimeMillis());
                assertTrue(env.service.tick().get(5,TimeUnit.SECONDS).propagation().complete());
                int sent=f.submissions; long log=f.catalogue.next.get(); wall.set(15);
                var unchanged=env.service.tick().get(5,TimeUnit.SECONDS);
                assertEquals(RatisCatalogueClock.Outcome.OBSERVED,unchanged.clock().outcome()); assertEquals(30,unchanged.clock().authority().logicalTimeMillis());
                assertEquals(sent,f.submissions); assertEquals(log,f.catalogue.next.get());
            }
            int sent=f.submissions; long log=f.catalogue.next.get();
            try (var recovered=new Env(root,f,wall)) {
                assertTrue(recovered.service.tick().get(5,TimeUnit.SECONDS).propagation().complete());
                assertEquals(sent,f.submissions); assertEquals(log,f.catalogue.next.get());
                for (var store:f.stores.values()) assertEquals(1,store.receiptUsage().retainedCount());
            }
        }
    }
    @Test void timerCoalescesIntoOneBatchQuantumAndCancelledViewsDoNotReleaseActualWork() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root); var env=new Env(root,f,new AtomicLong(20))) {
            var started=new CountDownLatch(1); var release=new CountDownLatch(1); f.started=started; f.release=release;
            var timer=new ManualTimer(); env.service.start(timer,Duration.ofSeconds(1)); timer.fire();
            try {
                assertTrue(started.await(5,TimeUnit.SECONDS));
                assertEquals(1,env.scheduler.activeTasks(WorkloadProfile.BATCH));
                var view=env.service.tick(); assertTrue(view.cancel(false));
                var active=RatisSessionClockService.class.getDeclaredField("active"); active.setAccessible(true); var task=active.get(env.service);
                var result=task.getClass().getDeclaredField("result"); result.setAccessible(true);
                var internal=(CompletableFuture<?>)result.get(task); int observers=internal.getNumberOfDependents();
                for (int i=0;i<100;i++) timer.fire();
                assertEquals(observers,internal.getNumberOfDependents(),"Discarded timer ticks must not accumulate future observers");
                assertEquals(0,env.scheduler.queuedTasks(WorkloadProfile.BATCH));
                var drained=env.service.stop(); assertFalse(drained.isDone()); assertTrue(timer.cancelled); assertFalse(timer.isDisposed());
                assertThrows(IOException.class,env.driver::close);
                assertThrows(ExecutionException.class,() -> env.service.tick().get(5,TimeUnit.SECONDS));
                release.countDown(); drained.get(5,TimeUnit.SECONDS);
                assertNotNull(env.service.latest()); assertNull(env.service.lastFailure()); assertEquals(1,f.submissions);
            } finally { release.countDown(); }
        }
    }
    @Test void queuedStopRemovesAdmissionWithoutStartingJournalWork() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root); var env=new Env(root,f,new AtomicLong(20))) {
            var started=new CountDownLatch(1); var release=new CountDownLatch(1);
            env.scheduler.executor(RequestContext.ingest(),OperationFamily.MUTATION).execute(() -> {
                started.countDown(); try { release.await(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            });
            try {
                assertTrue(started.await(5,TimeUnit.SECONDS));
                var queued=env.service.tick();
                for (int i=0;i<20;i++) env.service.tick();
                assertEquals(1,env.scheduler.queuedTasks(WorkloadProfile.BATCH));
                var drained=env.service.stop(); drained.get(5,TimeUnit.SECONDS);
                assertEquals(0,env.scheduler.queuedTasks(WorkloadProfile.BATCH));
                assertInstanceOf(CancellationException.class,assertThrows(ExecutionException.class,() -> queued.get(5,TimeUnit.SECONDS)).getCause());
                assertEquals(0,f.submissions); assertNull(env.clock.pendingCommand()); assertNull(env.driver.progress().target());
                assertNotNull(env.service.lastFailure());
            } finally { release.countDown(); }
        }
    }
    @Test void schedulerShutdownRejectsQueuedTickAndSettlesItsDrain() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root); var env=new Env(root,f,new AtomicLong(20))) {
            var started=new CountDownLatch(1); var release=new CountDownLatch(1);
            env.scheduler.executor(RequestContext.ingest(),OperationFamily.MUTATION).execute(() -> {
                started.countDown(); try { release.await(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            });
            try {
                assertTrue(started.await(5,TimeUnit.SECONDS)); var queued=env.service.tick();
                assertEquals(1,env.scheduler.queuedTasks(WorkloadProfile.BATCH)); env.scheduler.disposeNow();
                assertThrows(ExecutionException.class,() -> queued.get(5,TimeUnit.SECONDS)); env.service.stop().get(5,TimeUnit.SECONDS);
                assertEquals(0,f.submissions); assertNull(env.clock.pendingCommand()); assertNotNull(env.service.lastFailure());
            } finally { release.countDown(); }
        }
    }
    @Test void blockedCompletionCallbackKeepsAdmissionCoalescedAfterNativeDrain() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root); var env=new Env(root,f,new AtomicLong(20))) {
            var started=new CountDownLatch(1); var release=new CountDownLatch(1); f.started=started; f.release=release;
            var publishing=new CountDownLatch(1); var returnCallback=new CountDownLatch(1);
            var view=env.service.tick();
            view.thenRun(() -> {
                publishing.countDown();
                try { returnCallback.await(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            });
            try {
                assertTrue(started.await(5,TimeUnit.SECONDS)); release.countDown();
                assertTrue(publishing.await(5,TimeUnit.SECONDS));
                for (int i=0;i<20;i++) assertNotNull(env.service.tick().get(5,TimeUnit.SECONDS));
                assertEquals(1,f.submissions); assertEquals(0,env.scheduler.queuedTasks(WorkloadProfile.BATCH));
                env.service.stop().get(5,TimeUnit.SECONDS); // journal I/O has drained; callback publication still owns the admission gate
            } finally { release.countDown(); returnCallback.countDown(); }
        }
    }
    @Test void queuedClockLosesEligibilityBeforeDispatchWithoutTouchingJournals() throws Exception {
        var eligible=new AtomicBoolean(true);
        try (var f=new RatisSessionPropagationTest.Fixture(root); var env=new Env(root,f,new AtomicLong(20),eligible::get)) {
            var started=new CountDownLatch(1); var release=new CountDownLatch(1);
            env.scheduler.executor(RequestContext.ingest(),OperationFamily.MUTATION).execute(() -> {
                started.countDown(); try { release.await(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            });
            try {
                assertTrue(started.await(5,TimeUnit.SECONDS)); var queued=env.service.tick();
                assertEquals(1,env.scheduler.queuedTasks(WorkloadProfile.BATCH)); eligible.set(false); release.countDown();
                assertInstanceOf(CancellationException.class,assertThrows(ExecutionException.class,() -> queued.get(5,TimeUnit.SECONDS)).getCause());
                env.service.stop().get(5,TimeUnit.SECONDS);
                assertEquals(0,f.submissions); assertNull(env.clock.pendingCommand()); assertNull(env.clock.observed()); assertNull(env.driver.progress().target());
            } finally { release.countDown(); }
        }
    }
    static final class Env implements AutoCloseable {
        final RWScheduler scheduler=RWScheduler.forTesting(1,1,1,4,1,"session-clock-test");
        final RatisCatalogueClock clock; final RatisSessionPropagation driver; final RatisSessionClockService service;
        Env(Path root,RatisSessionPropagationTest.Fixture f,AtomicLong time) throws Exception { this(root,f,time,() -> true); }
        Env(Path root,RatisSessionPropagationTest.Fixture f,AtomicLong time,BooleanSupplier eligible) throws Exception {
            Path directory=root.resolve("propagation"); Files.createDirectories(directory);
            clock=new RatisCatalogueClock(root.resolve("catalogue-clock.bin"),f.catalogue.epochs);
            driver=new RatisSessionPropagation(directory,f.catalogue.epochs,f.routed,2);
            service=new RatisSessionClockService(scheduler,clock,driver,time::get,1,eligible);
        }
        public void close() throws Exception {
            service.stop().get(5,TimeUnit.SECONDS); driver.close(); clock.close(); scheduler.disposeGracefully().block(Duration.ofSeconds(5));
        }
    }
    static final class ManualTimer implements Scheduler {
        Runnable periodic; volatile boolean cancelled,disposed;
        void fire() { if (!cancelled && !disposed) periodic.run(); }
        public Disposable schedulePeriodically(Runnable task,long initial,long period,TimeUnit unit) {
            periodic=task; cancelled=false;
            return new Disposable() { public void dispose() { cancelled=true; } public boolean isDisposed() { return cancelled; } };
        }
        public Disposable schedule(Runnable task) { throw new UnsupportedOperationException(); }
        public Worker createWorker() { throw new UnsupportedOperationException(); }
        public void dispose() { disposed=true; }
        public boolean isDisposed() { return disposed; }
    }
}
