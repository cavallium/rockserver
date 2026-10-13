package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisAtomicClientTest.*;
import static it.cavallium.rockserver.distributed.ratis.RatisSnapshotClientTest.*;

/** Writable native overlay publication and original-handle reuse through public asynchronous lease reads. */
class RatisWritableLeaseTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final class Wire implements RatisRoutedClient.Transport {
        final RatisRoutedClient.Transport delegate; final AtomicInteger stages=new AtomicInteger();
        volatile UUID loseGroup,blockGroup,blockGateReleaseGroup; volatile CountDownLatch entered,release;
        Wire(Fixture f) { delegate=f.transport(); }
        public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return delegate.read(route,column,key); }
        public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return delegate.command(route,command); }
        public byte[] participant(RangeDescriptor route,byte[] record) throws IOException {
            byte[] reply=delegate.participant(route,record);
            if (route.groupId().equals(blockGateReleaseGroup) && RatisSnapshotRecord.isSnapshot(record) && RatisSnapshotRecord.decode(record).isRelease()) {
                entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); }
            }
            return reply;
        }
        public java.util.concurrent.CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] request) throws IOException { return delegate.snapshot(route,owner,request); }
        public RangeStateView stateView(RangeDescriptor route) throws IOException { return delegate.stateView(route); }
        public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException {
            boolean stage=ScopedTransactionRpc.kind(request,ScopedTransactionRpc.MAX_REQUEST_BYTES)==ScopedTransactionRpc.STAGE;
            byte[] reply=delegate.transaction(route,request,fresh);
            if (stage) {
                stages.incrementAndGet();
                if (route.groupId().equals(blockGroup)) {
                    entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); }
                }
                if (route.groupId().equals(loseGroup)) { loseGroup=null; throw new IOException("Lost applied STAGE reply"); }
            }
            return reply;
        }
    }
    RatisSnapshotClient client(Fixture f,RatisRoutedClient data) { return new RatisSnapshotClient(f.schema,f.snapshots,data,f.scheduler,RatisSnapshotClient.Limits.defaults(),RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED); }
    static RatisTransactionPlan prefix(Fixture f,RatisSnapshotClient.Lease lease,List<RatisLogicalClient.Change> changes) throws Exception {
        var logical=RatisLogicalClient.plan(f.schema,changes,List.of());
        return RatisTransactionPlan.captured(f.directory,f.identity(4).business(),logical.mutations(),logical.predicates(),lease.snapshot());
    }
    static List<RatisLogicalClient.Change> edits() {
        return List.of(RatisLogicalClient.Change.put(71,key(2),value(9),Mutation.ReturnMode.CURRENT),
                RatisLogicalClient.Change.delete(74,member(200,"alpha"),Mutation.ReturnMode.PREVIOUS),
                RatisLogicalClient.Change.put(74,member(200,"gamma"),value(11),Mutation.ReturnMode.CURRENT));
    }
    static void failure(CompletableFuture<?> view) { assertThrows(ExecutionException.class,() -> view.get(10,TimeUnit.SECONDS)); }
    @Test void aggregateStageResultBudgetSupportsManyTinyParticipantsAndRejectsCombinedOverflow() throws Exception {
        long bytes=8; var tiny=new CommandResult(CommandResult.Status.SUCCESS,List.of(new byte[]{7}));
        for (int n=0;n<64;n++) bytes=RatisSnapshotClient.checkedResultBytes(bytes,tiny);
        assertEquals(8+64L*(tiny.encode().length-8),bytes); assertTrue(bytes<CommandResult.MAX_BYTES);
        assertEquals(bytes,RatisSnapshotClient.checkedResultBytes(bytes,new CommandResult(CommandResult.Status.SUCCESS,List.of())));
        var half=new CommandResult(CommandResult.Status.SUCCESS,List.of(new byte[CommandResult.MAX_BYTES/2]));
        long first=RatisSnapshotClient.checkedResultBytes(8,half);
        assertThrows(IOException.class,() -> RatisSnapshotClient.checkedResultBytes(first,half));
    }
    @Test void writableStageReadsAndGlobalPagesUseOriginalHandlesAndCommitFreezeIsExplicit() throws Exception {
        try (var f=new Fixture(root,true)) {
            write(f,0,7,"seed.bin"); var wire=new Wire(f);
            try (var data=new RatisRoutedClient(f.directory,wire); var client=client(f,data); var lease=client.open(root.resolve("write-lease.bin"),snapshotIdentity(f,1),scope(f),RatisSnapshotClientTest.TTL,RequestContext.batch())) {
                ready(lease); int opened=f.opens.get(); var cursor=lease.openCursor(74,null,null,false);
                var plan=prefix(f,lease,edits()); var results=lease.stagePrefix(plan,RequestContext.batch()).get(10,TimeUnit.SECONDS); assertEquals(2,results.size());
                assertEquals(0,client.stats().cursors()); failure(cursor.nextPage(new RangeBudget(1,32),RequestContext.batch()));
                assertArrayEquals(new byte[]{9},lease.get(71,key(2),RequestContext.batch()).get(5,TimeUnit.SECONDS).toByteArray());
                assertNull(lease.get(74,member(200,"alpha"),RequestContext.batch()).get(5,TimeUnit.SECONDS));
                assertArrayEquals(new byte[]{11},lease.get(74,member(200,"gamma"),RequestContext.batch()).get(5,TimeUnit.SECONDS).toByteArray());
                assertEquals(List.of(member(2,"alpha"),member(2,"beta"),member(200,"beta"),member(200,"gamma")),scan(lease,74,false));
                assertEquals(List.of(member(200,"gamma"),member(200,"beta"),member(2,"beta"),member(2,"alpha")),scan(lease,74,true));
                assertArrayEquals(new byte[]{7},f.get(71,key(2))); assertArrayEquals(new byte[]{7},f.get(74,member(200,"alpha"))); assertNull(f.get(74,member(200,"gamma")));
                int sent=wire.stages.get(); lease.stagePrefix(plan,RequestContext.batch()).get(5,TimeUnit.SECONDS); assertEquals(sent,wire.stages.get());
                var accepted=lease.preparedPrefix(f.identity(4)); assertEquals(2,accepted.intents().size());
                for (var intent:accepted.intents()) assertEquals(lease.snapshot().participant(intent.route().groupId()).certifiedHandle().id(),intent.captureId());
                failure(lease.get(71,key(2),RequestContext.batch())); failure(lease.release(RequestContext.batch())); assertThrows(IOException.class,lease::close);
                lease.undoCommitFreeze(); assertArrayEquals(new byte[]{9},lease.get(71,key(2),RequestContext.batch()).get(5,TimeUnit.SECONDS).toByteArray());
                lease.preparedPrefix(f.identity(4)); lease.finishCommitOwnership(); lease.release(RequestContext.batch()).get(10,TimeUnit.SECONDS);
                assertEquals(opened,f.opens.get());
            }
        }
    }
    @Test void noEffectPartitionsSkipStageAndStillCertifyTheirOriginalFullReadScope() throws Exception {
        try (var f=new Fixture(root,true)) {
            write(f,0,7,"seed-readonly-parts.bin"); var wire=new Wire(f);
            try (var data=new RatisRoutedClient(f.directory,wire); var client=client(f,data); var lease=client.open(root.resolve("read-parts.bin"),snapshotIdentity(f,1),scope(f),RatisSnapshotClientTest.TTL,RequestContext.batch())) {
                ready(lease); int opens=f.opens.get();
                var unstaged=lease.preparedPrefix(f.identity(4)); assertEquals(2,unstaged.intents().size());
                for (var intent:unstaged.intents()) assertTrue(intent.acceptedBusiness().readCertification());
                lease.undoCommitFreeze();
                var plan=prefix(f,lease,List.of(RatisLogicalClient.Change.put(71,key(2),value(9),Mutation.ReturnMode.CURRENT)));
                var results=lease.stagePrefix(plan,RequestContext.batch()).get(10,TimeUnit.SECONDS); assertEquals(2,results.size()); assertEquals(1,wire.stages.get());
                assertEquals(0,results.get(f.groups.getLast()).valueCount());
                var accepted=lease.preparedPrefix(f.identity(4)); assertTrue(accepted.intents().getLast().acceptedBusiness().readCertification());
                for (var intent:accepted.intents()) assertArrayEquals(ScopedTransactionRpc.scopeBytes(lease.snapshot().initial().fence().participant(intent.route().groupId()).scope()),ScopedTransactionRpc.scopeBytes(intent.acceptedBusiness().acceptance().scope()));
                lease.finishCommitOwnership(); lease.release(RequestContext.batch()).get(10,TimeUnit.SECONDS); assertEquals(opens,f.opens.get());
            }
        }
    }
    @Test void uncertainFullPrefixBlocksReadsAndOnlyAnExactRetryCanPublishItsOverlay() throws Exception {
        try (var f=new Fixture(root,true)) {
            write(f,0,7,"seed-unknown.bin"); var wire=new Wire(f);
            try (var data=new RatisRoutedClient(f.directory,wire); var client=client(f,data); var lease=client.open(root.resolve("unknown-prefix.bin"),snapshotIdentity(f,1),scope(f),RatisSnapshotClientTest.TTL,RequestContext.batch())) {
                ready(lease); var plan=prefix(f,lease,edits()); var cursor=lease.openCursor(71,null,null,false); long before=client.stats().bytes(); wire.loseGroup=f.groups.getLast();
                failure(lease.stagePrefix(plan,RequestContext.batch())); assertTrue(client.stats().bytes()>before);
                failure(lease.get(71,key(2),RequestContext.batch())); failure(cursor.nextPage(new RangeBudget(1,32),RequestContext.batch())); assertThrows(IOException.class,() -> lease.openCursor(71,null,null,false));
                assertThrows(IOException.class,() -> lease.preparedPrefix(f.identity(4)));
                var changed=prefix(f,lease,List.of(RatisLogicalClient.Change.put(71,key(2),value(8),Mutation.ReturnMode.NONE)));
                failure(lease.stagePrefix(changed,RequestContext.batch())); int sent=wire.stages.get();
                lease.stagePrefix(plan,RequestContext.batch()).get(10,TimeUnit.SECONDS); assertEquals(sent+1,wire.stages.get(),"Only the unacknowledged participant request is replayed");
                assertArrayEquals(new byte[]{9},lease.get(71,key(2),RequestContext.batch()).get(5,TimeUnit.SECONDS).toByteArray());
                assertEquals(0,client.stats().cursors()); lease.release(RequestContext.batch()).get(10,TimeUnit.SECONDS);
            }
        }
    }
    @Test void crashedWritableFrontendCanReopenOnlyCleanupWithoutRestoringItsStagedPrefix() throws Exception {
        try (var f=new Fixture(root,true)) {
            write(f,0,7,"seed-cleanup.bin"); var wire=new Wire(f); Path path=root.resolve("cleanup-only.bin");
            try (var data=new RatisRoutedClient(f.directory,wire)) {
                try (var client=client(f,data); var lease=client.open(path,snapshotIdentity(f,1),scope(f),RatisSnapshotClientTest.TTL,RequestContext.batch())) {
                    ready(lease); var plan=prefix(f,lease,edits()); lease.stagePrefix(plan,RequestContext.batch()).get(10,TimeUnit.SECONDS);
                    assertArrayEquals(new byte[]{9},lease.get(71,key(2),RequestContext.batch()).get(5,TimeUnit.SECONDS).toByteArray());
                }
                int stages=wire.stages.get(),captures=f.captureRequests.get();
                try (var client=client(f,data)) {
                    assertThrows(IOException.class,() -> client.reopen(path,RequestContext.batch()));
                    try (var cleanup=client.reopenForRelease(path,RequestContext.batch())) {
                        failure(cleanup.get(71,key(2),RequestContext.batch())); cleanup.release(RequestContext.batch()).get(10,TimeUnit.SECONDS);
                        failure(cleanup.page(74,null,null,false,null,new RangeBudget(1,32),RequestContext.batch()));
                        assertEquals(stages,wire.stages.get()); assertEquals(captures,f.captureRequests.get());
                        for (var group:f.groups) assertEquals(0,f.stores.get(group).activeTrackedSnapshotPins());
                    }
                    assertEquals(0,client.stats().bytes());
                }
                assertArrayEquals(new byte[]{7},f.get(71,key(2)),"Cleanup does not publish the lost frontend overlay");
            }
        }
    }
    @Test void lateAcquisitionDoesNotPublishWritableReadOrStageAuthorityBeforeExactResume() throws Exception {
        try (var f=new Fixture(root,true)) {
            write(f,0,7,"seed-acquire-deadline.bin"); var wire=new Wire(f); wire.blockGateReleaseGroup=f.groups.getLast(); wire.entered=new CountDownLatch(1); wire.release=new CountDownLatch(1);
            try (var data=new RatisRoutedClient(f.directory,wire); var client=client(f,data); var lease=client.open(root.resolve("late-acquire.bin"),snapshotIdentity(f,1),scope(f),RatisSnapshotClientTest.TTL,RequestContext.batch(Duration.ofSeconds(1)))) {
                try {
                    assertTrue(wire.entered.await(5,TimeUnit.SECONDS)); TimeUnit.MILLISECONDS.sleep(1100); wire.release.countDown(); failure(lease.completion());
                    failure(lease.get(71,key(2),RequestContext.batch()));
                    var plan=prefix(f,lease,edits()); failure(lease.stagePrefix(plan,RequestContext.batch())); assertEquals(0,wire.stages.get());
                } finally { wire.release.countDown(); wire.blockGateReleaseGroup=null; }
                int captures=f.captureRequests.get(); lease.resume(RequestContext.batch()).get(10,TimeUnit.SECONDS);
                assertArrayEquals(new byte[]{7},lease.get(71,key(2),RequestContext.batch()).get(5,TimeUnit.SECONDS).toByteArray()); assertEquals(captures,f.captureRequests.get());
                lease.release(RequestContext.batch()).get(10,TimeUnit.SECONDS);
            }
        }
    }
    @Test void cancelledStageObserverRetainsActualChargesAndLateDeadlineRequiresExactRecovery() throws Exception {
        try (var f=new Fixture(root,true)) {
            write(f,0,7,"seed-deadline.bin"); var wire=new Wire(f);
            try (var data=new RatisRoutedClient(f.directory,wire); var client=client(f,data); var lease=client.open(root.resolve("deadline-prefix.bin"),snapshotIdentity(f,1),scope(f),RatisSnapshotClientTest.TTL,RequestContext.batch())) {
                ready(lease); var plan=prefix(f,lease,edits()); wire.blockGroup=f.groups.getFirst(); wire.entered=new CountDownLatch(1); wire.release=new CountDownLatch(1);
                long before=client.stats().bytes(); var view=lease.stagePrefix(plan,RequestContext.batch(Duration.ofMillis(300)));
                try {
                    assertTrue(wire.entered.await(5,TimeUnit.SECONDS)); assertTrue(view.cancel(false)); assertTrue(client.stats().bytes()>before);
                    assertThrows(IOException.class,lease::close);
                    var actual=lease.stagePrefix(plan,RequestContext.batch()); TimeUnit.MILLISECONDS.sleep(350); wire.release.countDown(); failure(actual);
                    failure(lease.get(71,key(2),RequestContext.batch())); assertTrue(client.stats().bytes()>before);
                } finally { wire.release.countDown(); wire.blockGroup=null; }
                lease.stagePrefix(plan,RequestContext.batch()).get(10,TimeUnit.SECONDS);
                assertArrayEquals(new byte[]{9},lease.get(71,key(2),RequestContext.batch()).get(5,TimeUnit.SECONDS).toByteArray());
                lease.release(RequestContext.batch()).get(10,TimeUnit.SECONDS); lease.close(); assertEquals(0,client.stats().leases()); assertEquals(0,client.stats().bytes());
            }
        }
    }
}
