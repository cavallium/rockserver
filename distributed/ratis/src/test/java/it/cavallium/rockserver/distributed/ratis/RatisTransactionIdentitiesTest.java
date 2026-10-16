package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisTransactionIdentitiesTest {
    @TempDir Path root;
    final SessionEpochLedger.Grant grant=new SessionEpochLedger.Grant(1,new UUID(200,1),0,1000);
    RWScheduler scheduler() { return RWScheduler.forTesting(1,1,1,4,1,"identity-reservation"); }
    @Test void forcedDisjointReservationsSurviveReopenAndNeverReuseAnExhaustedWindow() throws Exception {
        var scheduler=scheduler();
        try {
            RatisRocksDBAdapter.Allocation first;
            try (var identities=RatisTransactionIdentities.create(root,grant,2048,scheduler)) {
                first=identities.allocate(RequestContext.batch(),List.of()).get(5,TimeUnit.SECONDS);
                assertEquals(0,first.identity().snapshot().business().sequence()); assertEquals(513,first.identity().commit().business().sequence());
                assertTrue(first.identity().snapshot().lastControl().sequence()<first.identity().commit().business().sequence()); assertEquals(770,identities.nextSequence());
                assertFalse(Files.exists(first.snapshot())); assertFalse(Files.exists(first.commit()));
            }
            try (var identities=RatisTransactionIdentities.reopen(root,grant,2048,scheduler)) {
                var second=identities.allocate(RequestContext.batch(),List.of()).get(5,TimeUnit.SECONDS);
                assertEquals(770,second.identity().snapshot().business().sequence()); assertEquals(1540,identities.nextSequence());
                assertNotEquals(first.identity().snapshot().fence(),second.identity().snapshot().fence()); assertNotEquals(first.commit(),second.commit());
                var error=assertThrows(ExecutionException.class,() -> identities.allocate(RequestContext.batch(),List.of()).get(5,TimeUnit.SECONDS));
                assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,((RocksDBException)error.getCause()).getErrorUniqueId()); assertEquals(1540,identities.nextSequence());
            }
            try (var identities=RatisTransactionIdentities.reopen(root,grant,2048,scheduler)) { assertEquals(1540,identities.nextSequence()); }
        } finally { scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
    @Test void exclusiveBindingAndMissingUsedStateCannotCreateReplacementIdentities() throws Exception {
        var scheduler=scheduler();
        try {
            try (var identities=RatisTransactionIdentities.create(root,grant,2048,scheduler)) {
                assertThrows(IOException.class,() -> RatisTransactionIdentities.reopen(root,grant,2048,scheduler));
                identities.allocate(RequestContext.batch(),List.of()).get(5,TimeUnit.SECONDS);
            }
            assertThrows(IOException.class,() -> RatisTransactionIdentities.create(root,grant,2048,scheduler));
            assertThrows(IOException.class,() -> RatisTransactionIdentities.reopen(root,grant,4096,scheduler));
            var changed=new SessionEpochLedger.Grant(2,grant.sessionId(),0,1000); assertThrows(IOException.class,() -> RatisTransactionIdentities.reopen(root,changed,2048,scheduler));
            Files.delete(root.resolve("identities.bin")); assertThrows(IOException.class,() -> RatisTransactionIdentities.reopen(root,grant,2048,scheduler));
            assertThrows(IOException.class,() -> RatisTransactionIdentities.create(root,grant,2048,scheduler));
        } finally { scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
    @Test void cancelledObserverDoesNotReleaseQueuedReservationOwnershipOrRecycleItsBlock() throws Exception {
        var scheduler=scheduler(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
        try (var identities=RatisTransactionIdentities.create(root,grant,2048,scheduler)) {
            scheduler.executor(RequestContext.batch(),OperationFamily.MUTATION).execute(() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Queue barrier timed out"); } catch (InterruptedException interrupted) { throw new AssertionError(interrupted); } },1);
            assertTrue(entered.await(5,TimeUnit.SECONDS)); var observer=identities.allocate(RequestContext.batch(),List.of()); assertTrue(observer.cancel(false));
            assertThrows(IOException.class,identities::close); assertEquals(0,identities.nextSequence()); release.countDown();
            long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5); boolean closed=false;
            while (!closed && System.nanoTime()<until) try { identities.close(); closed=true; } catch (IOException busy) { Thread.sleep(1); }
            assertTrue(closed);
            try (var reopened=RatisTransactionIdentities.reopen(root,grant,2048,scheduler)) { assertEquals(770,reopened.nextSequence()); }
        } finally { release.countDown(); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
    @Test void stalledCheckpointDoesNotBlockAdmissionOrCloseOwnershipChecksOnCallerThreads() throws Exception {
        var scheduler=scheduler(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
        try (var identities=RatisTransactionIdentities.create(root,grant,2048,scheduler,(file,bytes) -> {
            if (java.nio.ByteBuffer.wrap(bytes).getLong(48)>0) { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Checkpoint barrier timed out"); } catch (InterruptedException interrupted) { throw new IOException(interrupted); } }
            SessionJournalFiles.write(file,bytes,96);
        })) {
            var first=identities.allocate(RequestContext.batch(),List.of()); assertTrue(entered.await(5,TimeUnit.SECONDS));
            try {
                assertTimeoutPreemptively(Duration.ofSeconds(1),() -> assertThrows(IOException.class,identities::close));
                var second=assertTimeoutPreemptively(Duration.ofSeconds(1),() -> identities.allocate(RequestContext.batch(),List.of()));
                assertTimeoutPreemptively(Duration.ofSeconds(1),() -> assertEquals(0,identities.nextSequence()));
                assertFalse(first.isDone()); assertFalse(second.isDone()); release.countDown();
                assertEquals(0,first.get(5,TimeUnit.SECONDS).identity().snapshot().business().sequence());
                assertEquals(770,second.get(5,TimeUnit.SECONDS).identity().snapshot().business().sequence());
            } finally { release.countDown(); }
        } finally { release.countDown(); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
}
