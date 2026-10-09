package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Forced scheduler/transport orderings over native stores; no server processes or network. */
class RatisAtomicOwnershipTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    void await(CountDownLatch latch) { try { if (!latch.await(5,TimeUnit.SECONDS)) throw new AssertionError("Controlled transport wait timed out"); } catch (InterruptedException interrupted) { throw new AssertionError(interrupted); } }
    @Test void cancelledWaitersCannotRefundActualOperationCapacityOrCloseItsOwners() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root)) {
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var once=new AtomicBoolean();
            f.beforeAuthorize=() -> { if (once.compareAndSet(false,true)) { entered.countDown(); await(release); } };
            var client=new RatisAtomicClient(f.schema,f.homes,f.data,f.scheduler,new RatisAtomicClient.Limits(1,RatisAtomicClient.MAX_BYTES));
            var operation=client.executeAtomic(root.resolve("cancel.bin"),f.identity(0),RatisAtomicClientTest.changes(),RatisAtomicClientTest.absentIdentity(),RatisAtomicClientTest.TTL,RequestContext.batch());
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); var views=new ArrayList<CompletableFuture<CommandResult>>();
                for (int n=0;n<RatisAtomicClient.MAX_WAITERS;n++) views.add(operation.completion());
                for (int n=0;n<views.size()-1;n++) assertTrue(views.get(n).cancel(false));
                var exhausted=assertThrows(ExecutionException.class,() -> operation.completion().get(5,TimeUnit.SECONDS)); assertInstanceOf(RejectedExecutionException.class,exhausted.getCause());
                assertEquals(1,client.stats().operations()); assertTrue(client.stats().bytes()>0);
                assertThrows(IOException.class,() -> client.executeAtomic(root.resolve("other.bin"),f.identity(1),RatisAtomicClientTest.changes(),List.of(),RatisAtomicClientTest.TTL,RequestContext.batch()));
                assertThrows(IOException.class,operation::close); assertThrows(IOException.class,client::close);
                assertSame(operation,client.operation(operation.path())); assertTrue(Files.exists(operation.path()));
                release.countDown(); assertEquals(CommandResult.Status.SUCCESS,views.getLast().get(10,TimeUnit.SECONDS).status());
                assertFalse(operation.snapshot().observedHome().consumed()); operation.close(); assertEquals(new RatisAtomicClient.Stats(0,0),client.stats()); client.close();
                try (var reader=f.client(); var reopened=reader.reopen(operation.path(),RequestContext.batch())) { assertEquals(CommandResult.Status.SUCCESS,reopened.completion().get(10,TimeUnit.SECONDS).status()); assertEquals(2,f.opens.get()); }
            } finally { release.countDown(); operation.close(); client.close(); }
        }
    }
    @Test void expiredDeadlineAfterAuthorizationCannotAppendPrepareAndResumeKeepsOriginalCaptures() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root); var client=f.client()) {
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var once=new AtomicBoolean();
            f.beforeAuthorize=() -> { if (once.compareAndSet(false,true)) { entered.countDown(); await(release); } };
            try (var operation=client.executeAtomic(root.resolve("deadline.bin"),f.identity(0),RatisAtomicClientTest.changes(),RatisAtomicClientTest.absentIdentity(),RatisAtomicClientTest.TTL,RequestContext.batch(Duration.ofSeconds(1)))) {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); long writes=f.writes(); var ids=operation.snapshot().participants().stream().map(p -> p.intent().captureId()).toList();
                try { assertFalse(release.await(1100,TimeUnit.MILLISECONDS)); } finally { release.countDown(); }
                assertThrows(ExecutionException.class,() -> operation.completion().get(10,TimeUnit.SECONDS)); assertEquals(writes,f.writes());
                for (var group:f.groups) assertNull(f.stores.get(group).preparedParticipant(f.identity(0).transaction()));
                assertEquals(CommandResult.Status.SUCCESS,operation.resume(RequestContext.batch()).get(10,TimeUnit.SECONDS).status());
                assertEquals(ids,operation.snapshot().participants().stream().map(p -> p.intent().captureId()).toList()); assertEquals(2,f.opens.get());
            } finally { release.countDown(); }
        }
    }
    @Test void facadeRejectsGenericJournalsOutsideItsReservedHomeBlockBeforeAnyReplay() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root); var client=f.client()) {
            f.beforeAuthorize=() -> { throw new IllegalStateException("Controlled unavailable authorizer"); };
            RatisDurableTransaction.Snapshot saved;
            try (var operation=client.executeAtomic(root.resolve("original.bin"),f.identity(0),RatisAtomicClientTest.changes(),RatisAtomicClientTest.absentIdentity(),RatisAtomicClientTest.TTL,RequestContext.batch())) {
                assertThrows(ExecutionException.class,() -> operation.completion().get(10,TimeUnit.SECONDS)); saved=operation.snapshot();
            }
            f.beforeAuthorize=null; var path=root.resolve("wrong-home-block.bin");
            try (var generic=RatisDurableTransaction.create(path,f.homes,f.data,saved.initial(),f.homes.prepare(new OperationId(f.session,f.grant.epoch(),700),null,saved.initial()),saved.participants().stream().map(RatisDurableTransaction.ParticipantProgress::intent).toList())) { assertNotNull(generic.snapshot()); }
            long writes=f.writes(); int opens=f.opens.get();
            try (var operation=client.reopen(path,RequestContext.batch())) {
                var failure=assertThrows(ExecutionException.class,() -> operation.completion().get(10,TimeUnit.SECONDS)); assertInstanceOf(IOException.class,failure.getCause());
                assertEquals(writes,f.writes()); assertEquals(opens,f.opens.get()); assertTrue(Files.exists(path));
                assertThrows(ExecutionException.class,() -> operation.resume(RequestContext.batch()).get(10,TimeUnit.SECONDS)); assertEquals(writes,f.writes());
            }
        }
    }
    @Test void unavailableOriginalCaptureAfterCloseAbortsWithoutReplacementCutsOrEffects() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root)) {
            f.beforeAuthorize=() -> { throw new IllegalStateException("Authorization response unavailable"); }; Path path=root.resolve("unavailable.bin");
            try (var client=f.client(); var operation=client.executeAtomic(path,f.identity(0),RatisAtomicClientTest.changes(),RatisAtomicClientTest.absentIdentity(),RatisAtomicClientTest.TTL,RequestContext.batch())) { assertThrows(ExecutionException.class,() -> operation.completion().get(10,TimeUnit.SECONDS)); }
            int opens=f.opens.get(); f.beforeAuthorize=null;
            try (var client=f.client(); var resumed=client.reopen(path,RequestContext.batch())) {
                assertEquals(CommandResult.Status.CONFLICT,resumed.completion().get(10,TimeUnit.SECONDS).status()); assertEquals(TransactionStatus.Phase.ABORT,resumed.snapshot().observedHome().phase());
                assertEquals(opens,f.opens.get()); assertNull(f.get(71,RatisAtomicClientTest.key(2))); assertNull(f.get(72,RatisAtomicClientTest.key(200)));
                assertFalse(resumed.snapshot().observedHome().consumed());
            }
        }
    }
    @Test void rejectionPersistenceFailureFencesResumeUntilTheOriginalRecordIsReopened() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root); var client=f.client()) {
            try (var seed=client.executeAtomic(root.resolve("seed.bin"),f.identity(0),RatisAtomicClientTest.changes(),RatisAtomicClientTest.absentIdentity(),RatisAtomicClientTest.TTL,RequestContext.batch())) { assertEquals(CommandResult.Status.SUCCESS,seed.completion().get(10,TimeUnit.SECONDS).status()); }
            Path path=root.resolve("fenced-rejection.bin"); byte[] exact; long writes=f.writes();
            try (var operation=client.executeAtomic(path,f.identity(1),RatisAtomicClientTest.changes(),RatisAtomicClientTest.absentIdentity(),RatisAtomicClientTest.TTL,RequestContext.batch())) {
                assertEquals(CommandResult.Status.CONFLICT,operation.completion().get(10,TimeUnit.SECONDS).status()); assertTrue(operation.rejectedBeforeRegistration());
                exact=Files.readAllBytes(path); byte[] corrupt=exact.clone(); corrupt[corrupt.length-1]^=1; Files.write(path,corrupt);
                assertThrows(ExecutionException.class,() -> operation.consume(RequestContext.batch()).get(10,TimeUnit.SECONDS));
                assertThrows(ExecutionException.class,() -> operation.resume(RequestContext.batch()).get(10,TimeUnit.SECONDS)); assertEquals(writes,f.writes());
            }
            Files.write(path,exact);
            try (var reopened=client.reopen(path,RequestContext.batch())) { assertEquals(CommandResult.Status.CONFLICT,reopened.completion().get(10,TimeUnit.SECONDS).status()); reopened.consume(RequestContext.batch()).get(10,TimeUnit.SECONDS); assertEquals(writes,f.writes()); }
        }
    }
}
