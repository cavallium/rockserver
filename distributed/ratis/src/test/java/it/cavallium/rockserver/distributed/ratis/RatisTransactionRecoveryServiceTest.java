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
    @Test void cancelledViewsAndStopWaitForActualAuthorityAndDoNotCloseBorrowedOwners() throws Exception {
        var helper=new RatisTransactionRecoveryTest(); helper.root=root;
        try (var f=helper.new Fixture("running"); var later=helper.new Fixture("later")) {
            f.journal.executeHome(); var abort=f.plan.initial().decide(TransactionStatus.Phase.ABORT); f.journal.queueHome(f.home.statuses.prepare(helper.journals.homeOperation(1),f.plan.initial(),abort)); f.journal.executeHome();
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            try (var recovery=new RatisTransactionRecovery(f.journal,f.home.statuses,f.data.routed,RatisTransactionRecoveryTest::next,(g,t,c) -> {
                entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); } catch (InterruptedException e) { throw new IOException(e); } return f.authorize(g,t,c);
            }); var laterRecovery=later.recovery()) {
                var scheduler=scheduler(); var service=new RatisTransactionRecoveryService(scheduler,List.of(recovery,laterRecovery),2);
                try {
                    var view=service.tick(); assertTrue(entered.await(5,TimeUnit.SECONDS)); assertTrue(view.cancel(false)); assertThrows(IOException.class,recovery::close);
                    var drain=service.stop(); assertFalse(drain.isDone()); assertTrue(drain.cancel(false)); var actual=service.stop(); assertFalse(actual.isDone());
                    release.countDown(); actual.get(5,TimeUnit.SECONDS); idle(scheduler); assertNotNull(f.journal.snapshot().participant(f.data.f.groupId).outcome()); assertEquals(0,later.home.wire.index,"Stop must not begin the next transaction in this quantum");
                    helper.drain(f,recovery); assertFalse(f.journal.snapshot().observedHome().consumed()); assertThrows(ExecutionException.class,() -> service.tick().get(5,TimeUnit.SECONDS));
                } finally { release.countDown(); service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
            }
        }
    }
    @Test void aFailingTransactionRotatesBehindOtherRecoveries() throws Exception {
        var helper=new RatisTransactionRecoveryTest(); helper.root=root;
        try (var first=helper.new Fixture("first"); var second=helper.new Fixture("second")) {
            first.journal.executeHome(); second.journal.executeHome();
            try (var failed=new RatisTransactionRecovery(first.journal,first.home.statuses,first.data.routed,last -> { throw new IOException("Unavailable control grant"); },first::authorize); var healthy=second.recovery()) {
                var scheduler=scheduler(); var service=new RatisTransactionRecoveryService(scheduler,List.of(failed,healthy),1);
                try {
                    for (int n=0;n<16 && service.remaining()>1;n++) { try { service.tick().get(5,TimeUnit.SECONDS); } catch (ExecutionException expected) { assertInstanceOf(IOException.class,expected.getCause()); } idle(scheduler); }
                    assertEquals(1,service.remaining()); assertTrue(second.journal.snapshot().observedHome().fullyFinalized()); assertFalse(second.journal.snapshot().observedHome().consumed()); assertEquals(TransactionStatus.Phase.PREPARING,first.journal.snapshot().observedHome().phase());
                } finally { service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
            }
        }
    }
    @Test void queuedStopRemovesOneCoalescedAdmission() throws Exception {
        var scheduler=scheduler(); var service=new RatisTransactionRecoveryService(scheduler,List.of(),1); var release=new CountDownLatch(1); int workers=scheduler.poolSnapshot(RWScheduler.Pool.CONTROL).workerCount(); var entered=new CountDownLatch(workers);
        try {
            for (int n=0;n<workers;n++) scheduler.executor(WorkloadProfile.CONTROL,OperationFamily.CONTROL,RequestContext.NO_TIMEOUT).execute(() -> { entered.countDown(); try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }); assertTrue(entered.await(5,TimeUnit.SECONDS));
            var first=service.tick(); var second=service.tick(); assertEquals(1,scheduler.queuedTasks(WorkloadProfile.CONTROL)); service.stop().get(5,TimeUnit.SECONDS); assertEquals(0,scheduler.queuedTasks(WorkloadProfile.CONTROL));
            assertThrows(ExecutionException.class,() -> first.get(5,TimeUnit.SECONDS)); assertThrows(ExecutionException.class,() -> second.get(5,TimeUnit.SECONDS));
        } finally { release.countDown(); service.stop().get(5,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
}
