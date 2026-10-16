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
import static it.cavallium.rockserver.distributed.ratis.RatisAtomicClientTest.*;
import static it.cavallium.rockserver.distributed.ratis.RatisSnapshotClientTest.*;

/** Actual client/task ownership over native pins and controlled transports; no network or server processes. */
class RatisSnapshotOwnershipTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    void await(CountDownLatch latch) { try { if (!latch.await(5,TimeUnit.SECONDS)) throw new AssertionError("Snapshot barrier timed out"); } catch (InterruptedException interrupted) { throw new AssertionError(interrupted); } }
    void pinsClosed(Fixture f) { for (var group:f.groups) assertEquals(0,f.machines.get(group).snapshotPinStats().pins()); }
    @Test void cancelledCursorViewsRetainActualReaderAndCursorChargesUntilReturn() throws Exception {
        try (var f=new Fixture(root,true)) {
            write(f,0,7,"seed.bin"); var client=new RatisSnapshotClient(RatisTransactionBindings.of(f.schema,f.data.directory(f.schema),f.data),f.snapshots,f.scheduler,new RatisSnapshotClient.Limits(1,RatisSnapshotClient.MAX_BYTES));
            var lease=client.open(root.resolve("read.bin"),snapshotIdentity(f,1),scope(f),RatisSnapshotClientTest.TTL,RequestContext.batch()); ready(lease);
            var cursor=lease.openCursor(74,null,null,false); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var once=new AtomicBoolean();
            f.beforeSnapshotRead=() -> { if (once.compareAndSet(false,true)) { entered.countDown(); await(release); } };
            var first=cursor.nextPage(new RangeBudget(1,64),RequestContext.batch());
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); var views=new ArrayList<CompletableFuture<RangePage<KV>>>(); views.add(first);
                for (int n=1;n<RatisSnapshotClient.MAX_WAITERS;n++) views.add(cursor.pendingPage());
                for (int n=0;n<views.size()-1;n++) assertTrue(views.get(n).cancel(false));
                var exhausted=assertThrows(ExecutionException.class,() -> cursor.pendingPage().get(5,TimeUnit.SECONDS)); assertInstanceOf(RejectedExecutionException.class,exhausted.getCause());
                long charged=client.stats().bytes(); cursor.close(); assertEquals(charged,client.stats().bytes()); assertEquals(1,client.stats().reads()); assertEquals(1,client.stats().cursors());
                assertThrows(ExecutionException.class,() -> lease.release(RequestContext.batch()).get(5,TimeUnit.SECONDS)); assertFalse(lease.snapshot().releaseRequested());
                assertThrows(IOException.class,lease::close); assertThrows(IOException.class,client::close);
                release.countDown(); assertEquals(member(2,"alpha"),views.getLast().get(5,TimeUnit.SECONDS).items().getFirst().keys());
                assertEquals(0,client.stats().reads()); assertEquals(0,client.stats().cursors()); lease.close(); client.close(); assertEquals(new RatisSnapshotClient.Stats(0,0,0,0),client.stats());
                try (var other=RatisSnapshotClientTest.client(f); var restored=other.reopen(lease.path(),RequestContext.batch())) { ready(restored); restored.release(RequestContext.batch()).get(10,TimeUnit.SECONDS); pinsClosed(f); }
            } finally { release.countDown(); cursor.close(); lease.close(); client.close(); }
        }
    }
    @Test void failedFinalDeadlineCheckDoesNotAdvanceTheUndeliveredCursorPage() throws Exception {
        try (var f=new Fixture(root,true); var client=RatisSnapshotClientTest.client(f)) {
            write(f,0,7,"deadline-seed.bin");
            try (var lease=client.open(root.resolve("deadline.bin"),snapshotIdentity(f,1),scope(f),RatisSnapshotClientTest.TTL,RequestContext.batch())) {
                ready(lease); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var once=new AtomicBoolean();
                f.beforeSnapshotRead=() -> { if (once.compareAndSet(false,true)) { entered.countDown(); await(release); } };
                try (var cursor=lease.openCursor(74,null,null,false)) {
                    var page=cursor.nextPage(new RangeBudget(1,64),RequestContext.batch(Duration.ofSeconds(1))); assertTrue(entered.await(5,TimeUnit.SECONDS));
                    try { assertFalse(release.await(1100,TimeUnit.MILLISECONDS)); } finally { release.countDown(); }
                    assertThrows(ExecutionException.class,() -> page.get(5,TimeUnit.SECONDS));
                    assertEquals(member(2,"alpha"),cursor.nextPage(new RangeBudget(1,64),RequestContext.batch()).get(5,TimeUnit.SECONDS).items().getFirst().keys());
                    var second=cursor.nextPage(new RangeBudget(1,64),RequestContext.batch()).get(5,TimeUnit.SECONDS); assertEquals(member(2,"beta"),second.items().getFirst().keys());
                    int rpcs=f.rpcs.get(); assertEquals(second,cursor.pendingPage().get(5,TimeUnit.SECONDS)); assertEquals(rpcs,f.rpcs.get());
                } finally { release.countDown(); }
                lease.release(RequestContext.batch()).get(10,TimeUnit.SECONDS); pinsClosed(f);
            }
        }
    }
    @Test void readAndCursorByteAdmissionRejectBeforeRemoteWorkWithoutLeakingCharges() throws Exception {
        try (var f=new Fixture(root,true); var client=new RatisSnapshotClient(RatisTransactionBindings.of(f.schema,f.data.directory(f.schema),f.data),f.snapshots,f.scheduler,new RatisSnapshotClient.Limits(1,4L*1024*1024))) {
            write(f,0,7,"budget-seed.bin");
            try (var lease=client.open(root.resolve("budget.bin"),snapshotIdentity(f,1),scope(f),RatisSnapshotClientTest.TTL,RequestContext.batch())) {
                ready(lease); var before=client.stats(); int rpcs=f.rpcs.get();
                assertThrows(ExecutionException.class,() -> lease.get(71,key(2),RequestContext.batch()).get(5,TimeUnit.SECONDS));
                assertThrows(IOException.class,() -> lease.openCursor(74,null,null,false)); assertEquals(before,client.stats()); assertEquals(rpcs,f.rpcs.get());
                lease.release(RequestContext.batch()).get(10,TimeUnit.SECONDS); pinsClosed(f);
            }
        }
    }
    @Test void cancelledReleaseKeepsTheJournalAndClosedReadDirectionUntilOwnerRpcReturns() throws Exception {
        try (var f=new Fixture(root,true)) {
            write(f,0,7,"release-seed.bin"); var client=RatisSnapshotClientTest.client(f);
            var lease=client.open(root.resolve("release.bin"),snapshotIdentity(f,1),scope(f),RatisSnapshotClientTest.TTL,RequestContext.batch()); ready(lease);
            var entered=new CountDownLatch(1); var resume=new CountDownLatch(1); var once=new AtomicBoolean();
            f.beforeSnapshotRelease=() -> { if (once.compareAndSet(false,true)) { entered.countDown(); await(resume); } };
            var first=lease.release(RequestContext.batch());
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); assertTrue(first.cancel(false)); var actual=lease.release(RequestContext.batch());
                assertFalse(actual.isDone()); assertTrue(lease.snapshot().releaseRequested()); assertTrue(lease.snapshot().observedHome().consumed());
                lost(lease.get(71,key(2),RequestContext.batch())); assertThrows(IOException.class,lease::close); assertThrows(IOException.class,client::close);
                assertThrows(IOException.class,() -> RatisDurableSnapshot.reopen(lease.path(),f.snapshots,RatisTransactionBindings.of(f.data)));
                resume.countDown(); actual.get(5,TimeUnit.SECONDS); pinsClosed(f); lease.close(); client.close();
                int captures=f.captureRequests.get();
                try (var other=RatisSnapshotClientTest.client(f); var restored=other.reopen(lease.path(),RequestContext.batch())) { lost(restored.completion()); lost(restored.get(71,key(2),RequestContext.batch())); assertEquals(captures,f.captureRequests.get()); }
            } finally { resume.countDown(); lease.close(); client.close(); }
        }
    }
    @Test void explicitReleaseAfterUnknownCaptureRecoversOnlyItsOriginalTokenWithoutWaitingForGc() throws Exception {
        try (var f=new Fixture(root,true); var client=RatisSnapshotClientTest.client(f)) {
            write(f,0,7,"unknown-seed.bin"); f.loseCaptureGroup=f.groups.getFirst();
            try (var lease=client.open(root.resolve("unknown.bin"),snapshotIdentity(f,1),scope(f),RatisSnapshotClientTest.TTL,RequestContext.batch())) {
                assertThrows(ExecutionException.class,() -> lease.completion().get(10,TimeUnit.SECONDS)); var saved=lease.snapshot().participant(f.groups.getFirst());
                assertNotNull(saved.capture()); assertNull(saved.captureToken()); UUID original=saved.capture().request().snapshotId(); int captures=f.captureRequests.get();
                lease.release(RequestContext.batch()).get(10,TimeUnit.SECONDS); var released=lease.snapshot();
                assertEquals(SnapshotFenceStatus.Phase.ABORT,released.observedHome().phase()); assertTrue(released.observedHome().consumed()); assertNull(released.homeRetirementProof());
                assertEquals(original,released.participant(f.groups.getFirst()).captureToken().snapshotId()); assertEquals(captures+1,f.captureRequests.get()); pinsClosed(f);
                lost(lease.get(71,key(2),RequestContext.batch()));
            }
        }
    }
    @Test void returnedPageKeysCannotCorruptCachedOutcomeOrPrivateContinuation() throws Exception {
        try (var f=new Fixture(root,true); var client=RatisSnapshotClientTest.client(f)) {
            write(f,0,7,"copy-seed.bin");
            try (var lease=client.open(root.resolve("copy.bin"),snapshotIdentity(f,1),scope(f),RatisSnapshotClientTest.TTL,RequestContext.batch())) {
                ready(lease);
                try (var cursor=lease.openCursor(74,null,null,false)) {
                    var first=cursor.nextPage(new RangeBudget(1,64),RequestContext.batch()).get(5,TimeUnit.SECONDS);
                    first.items().getFirst().keys().keys()[1]=it.cavallium.buffer.Buf.wrap(new byte[]{99}); first.resumeAfter().keys()[1]=it.cavallium.buffer.Buf.wrap(new byte[]{98});
                    int rpcs=f.rpcs.get(); var replay=cursor.pendingPage().get(5,TimeUnit.SECONDS);
                    assertEquals(member(2,"alpha"),replay.items().getFirst().keys()); assertEquals(member(2,"alpha"),replay.resumeAfter()); assertEquals(rpcs,f.rpcs.get());
                    assertEquals(member(2,"beta"),cursor.nextPage(new RangeBudget(1,64),RequestContext.batch()).get(5,TimeUnit.SECONDS).items().getFirst().keys());
                }
                lease.release(RequestContext.batch()).get(10,TimeUnit.SECONDS); pinsClosed(f);
            }
        }
    }
}
