package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.lang.reflect.Proxy;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.Message;
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
    @Test void threeScopesShareOneBudgetAndAllMustFinishBeforeAnotherClockAdvance() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root)) {
            var wall=new AtomicLong(20);
            try (var env=new MultiEnv(root,f,wall,List.of(f.catalogue.epochs,f.catalogue.epochs,f.catalogue.epochs))) {
                for (int i=1;i<=6;i++) {
                    var status=env.service.tick().get(5,TimeUnit.SECONDS);
                    assertEquals(i,f.submissions,"One total participant visit per quantum across all scopes");
                    assertEquals(i==6,status.complete()); assertEquals(3,status.scopes().size());
                    assertSame(status.scopes().getFirst(),status.propagation());
                    for (var scope:status.scopes()) assertEquals(20,scope.target().logicalTimeMillis());
                    assertEquals(i<4 ? 1 : 2,status.scopes().get((i-1)%3).completed());
                    wall.set(30); assertEquals(20,f.catalogue.epochs.read().logicalTimeMillis());
                }
                var next=env.service.tick().get(5,TimeUnit.SECONDS); assertEquals(30,next.clock().authority().logicalTimeMillis());
                for (var scope:next.scopes()) assertEquals(next.clock().authority(),scope.target());
                assertThrows(UnsupportedOperationException.class,() -> next.scopes().clear());
                assertThrows(IllegalArgumentException.class,() -> new RatisSessionClockService(env.scheduler,env.clock,List.of(),wall::get,1));
                assertThrows(IllegalArgumentException.class,() -> new RatisSessionClockService(env.scheduler,env.clock,List.of(env.drivers.getFirst(),env.drivers.getFirst()),wall::get,1));
                assertThrows(IllegalArgumentException.class,() -> new RatisSessionClockService(env.scheduler,env.clock,List.of(env.drivers.getFirst(),env.drivers.get(1),env.drivers.get(2),env.drivers.getFirst()),wall::get,1));
            }
        }
    }
    @Test void unavailableParticipantsCannotStarveOtherScopesOrMultiplyTheQuantum() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root);
             var env=new MultiEnv(root,f,new AtomicLong(20),List.of(f.catalogue.epochs,f.catalogue.epochs,f.catalogue.epochs))) {
            f.failed.add(f.groups.getFirst());
            for (int i=1;i<=6;i++) {
                var status=env.service.tick().get(5,TimeUnit.SECONDS); assertEquals(i,f.submissions);
                assertEquals(i>3 ? 1 : 0,status.scopes().get((i-1)%3).completed());
                assertFalse(status.complete()); assertEquals(20,f.catalogue.epochs.read().logicalTimeMillis());
            }
            f.failed.clear();
            for (int i=0;i<3;i++) env.service.tick().get(5,TimeUnit.SECONDS);
            assertTrue(env.service.latest().complete()); assertEquals(9,f.submissions);
        }
    }
    @Test void restartedPartialScopeBeginDischargesPersistedClockObligationBeforeAdvancing() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root)) {
            var wall=new AtomicLong(20); var fail=new AtomicBoolean(true);
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
                if (!method.getName().equals("sendReadOnly")) throw new AssertionError(method);
                if (fail.getAndSet(false)) throw new IOException("home catalogue unavailable");
                return f.catalogue.client.io().sendReadOnly((Message)args[0]);
            });
            var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> switch (method.getName()) {
                case "getGroupId" -> f.catalogue.client.getGroupId(); case "io" -> io; default -> throw new AssertionError(method);
            });
            var epochs=new RatisSessionEpochs(client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.sessions);
            try (var env=new MultiEnv(root,f,wall,List.of(f.catalogue.epochs,epochs))) {
                assertThrows(ExecutionException.class,() -> env.service.tick().get(5,TimeUnit.SECONDS));
                assertEquals(20,env.clock.observed().logicalTimeMillis());
                assertEquals(20,env.drivers.getFirst().progress().target().logicalTimeMillis());
                assertNull(env.drivers.get(1).progress().target()); assertEquals(0,f.submissions);
            }
            wall.set(30);
            try (var env=new MultiEnv(root,f,wall,List.of(f.catalogue.epochs,f.catalogue.epochs))) {
                for (int i=0;i<4;i++) {
                    var status=env.service.tick().get(5,TimeUnit.SECONDS); assertNull(status.clock());
                    for (var scope:status.scopes()) assertEquals(20,scope.target().logicalTimeMillis());
                    assertEquals(20,f.catalogue.epochs.read().logicalTimeMillis());
                }
                assertTrue(env.service.latest().complete());
                assertEquals(30,env.service.tick().get(5,TimeUnit.SECONDS).clock().authority().logicalTimeMillis());
            }
        }
    }
    static final class MultiEnv implements AutoCloseable {
        final RWScheduler scheduler=RWScheduler.forTesting(1,1,1,4,1,"multi-session-clock-test");
        final RatisCatalogueClock clock; final List<RatisSessionPropagation> drivers=new ArrayList<>(); final RatisSessionClockService service;
        MultiEnv(Path root,RatisSessionPropagationTest.Fixture f,AtomicLong time,List<RatisSessionEpochs> epochs) throws Exception {
            clock=new RatisCatalogueClock(root.resolve("multi-clock.bin"),f.catalogue.epochs);
            for (int i=0;i<epochs.size();i++) drivers.add(new RatisSessionPropagation(Files.createDirectories(root.resolve("scope-"+i)),epochs.get(i),f.routed,2));
            service=new RatisSessionClockService(scheduler,clock,drivers,time::get,1);
        }
        public void close() throws Exception {
            service.stop().get(5,TimeUnit.SECONDS); for (var driver:drivers) driver.close(); clock.close();
            scheduler.disposeGracefully().block(Duration.ofSeconds(5));
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
