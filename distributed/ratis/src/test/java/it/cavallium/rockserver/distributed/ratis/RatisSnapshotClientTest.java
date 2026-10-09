package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisAtomicClientTest.*;

/** Public scoped snapshots over native groups and controlled owner dispatch, without network/quorum fixtures. */
class RatisSnapshotClientTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final Duration TTL=Duration.ofSeconds(30);
    static TransactionScope scope(Fixture f) {
        return new TransactionScope(f.schema.columns().stream().map(column -> TransactionScope.Span.range(column.columnId(),null,null)).toList());
    }
    static RatisSnapshotClient.Identity snapshotIdentity(Fixture f,int block) {
        long business=(512L+1)*block;
        return new RatisSnapshotClient.Identity(f.identity(block).transaction(),new OperationId(f.session,1,business),new OperationId(f.session,1,business+1));
    }
    static RatisSnapshotClient client(Fixture f) {
        return new RatisSnapshotClient(f.schema,f.snapshots,f.data,f.scheduler,RatisSnapshotClient.Limits.defaults());
    }
    static List<RatisLogicalClient.Change> image(int value) {
        var changes=new ArrayList<RatisLogicalClient.Change>();
        for (long column=71;column<=73;column++) for (int prefix:new int[]{2,200}) changes.add(RatisLogicalClient.Change.put(column,key(prefix),RatisAtomicClientTest.value(value),Mutation.ReturnMode.NONE));
        for (int prefix:new int[]{2,200}) for (String text:List.of("alpha","beta")) changes.add(RatisLogicalClient.Change.put(74,member(prefix,text),RatisAtomicClientTest.value(value),Mutation.ReturnMode.NONE));
        return changes;
    }
    static void write(Fixture f,int identity,int value,String name) throws Exception {
        try (var atomic=f.client(); var operation=atomic.executeAtomic(f.root.resolve(name),f.identity(identity),image(value),List.of(),TTL,RequestContext.batch())) {
            f.complete(operation,CommandResult.Status.SUCCESS);
        }
    }
    static void ready(RatisSnapshotClient.Lease lease) throws Exception {
        lease.completion().get(10,TimeUnit.SECONDS);
        var saved=lease.snapshot(); assertEquals(SnapshotFenceStatus.Phase.COMPLETE,saved.observedHome().phase()); assertFalse(saved.observedHome().consumed());
        assertEquals(2,saved.participants().size());
        for (var participant:saved.participants()) {
            assertNotNull(participant.readyProof()); assertNotNull(participant.captureToken()); assertNotNull(participant.releaseReceipt()); assertFalse(participant.nativeGate().active());
        }
    }
    static List<Keys> scan(RatisSnapshotClient.Lease lease,long column,boolean reverse) throws Exception {
        var keys=new ArrayList<Keys>(); Keys continuation=null;
        for (int n=0;n<16;n++) {
            var page=lease.page(column,null,null,reverse,continuation,new RangeBudget(1,64),RequestContext.batch()).get(5,TimeUnit.SECONDS);
            assertTrue(page.items().size()<=1);
            if (!page.items().isEmpty()) { assertEquals(page.items().getLast().keys(),page.resumeAfter()); keys.add(page.items().getLast().keys()); }
            if (!page.hasMore()) return keys;
            assertFalse(page.items().isEmpty()); assertNotEquals(continuation,page.resumeAfter()); continuation=page.resumeAfter();
        }
        throw new AssertionError("Bounded page continuation did not terminate");
    }
    static void lost(CompletableFuture<?> view) {
        var failure=assertThrows(ExecutionException.class,() -> view.get(5,TimeUnit.SECONDS));
        Throwable cause=failure.getCause();
        while (cause!=null && !(cause instanceof RatisSnapshotLostException)) cause=cause.getCause();
        assertInstanceOf(RatisSnapshotLostException.class,cause);
    }
    @Test void scopedImageKeepsAllFourColumnsConsistentAfterAtomicWritesAndPagesBothGroups() throws Exception {
        try (var f=new Fixture(root,true)) {
            write(f,0,7,"seed-atomic.bin");
            Path path=root.resolve("scope.bin");
            try (var snapshots=client(f); var lease=snapshots.open(path,snapshotIdentity(f,1),scope(f),TTL,RequestContext.batch())) {
                ready(lease); write(f,4,9,"update-atomic.bin");
                for (long column=71;column<=73;column++) for (int prefix:new int[]{2,200}) {
                    assertArrayEquals(new byte[]{7},lease.get(column,key(prefix),RequestContext.batch()).get(5,TimeUnit.SECONDS).toByteArray());
                    assertArrayEquals(new byte[]{9},f.get(column,key(prefix)));
                }
                for (int prefix:new int[]{2,200}) for (String text:List.of("alpha","beta")) {
                    assertArrayEquals(new byte[]{7},lease.get(74,member(prefix,text),RequestContext.batch()).get(5,TimeUnit.SECONDS).toByteArray());
                    assertArrayEquals(new byte[]{9},f.get(74,member(prefix,text)));
                }
                assertEquals(List.of(key(2),key(200)),scan(lease,71,false)); assertEquals(List.of(key(200),key(2)),scan(lease,71,true));
                var forward=List.of(member(2,"alpha"),member(2,"beta"),member(200,"alpha"),member(200,"beta"));
                assertEquals(forward,scan(lease,74,false)); var reverse=new ArrayList<>(forward); Collections.reverse(reverse); assertEquals(reverse,scan(lease,74,true));
                var bounded=lease.page(74,member(2,"beta"),member(200,"beta"),false,null,new RangeBudget(8,256),RequestContext.batch()).get(5,TimeUnit.SECONDS);
                assertEquals(List.of(member(2,"beta"),member(200,"alpha")),bounded.items().stream().map(KV::keys).toList());
                assertFalse(bounded.hasMore()); assertFalse(lease.snapshot().observedHome().consumed());
                var closedCursor=lease.openCursor(74,null,null,false);
                assertEquals(member(2,"alpha"),closedCursor.nextPage(new RangeBudget(1,64),RequestContext.batch()).get(5,TimeUnit.SECONDS).resumeAfter());
                closedCursor.close(); assertEquals(0,snapshots.stats().cursors());
                assertThrows(ExecutionException.class,() -> closedCursor.nextPage(new RangeBudget(1,64),RequestContext.batch()).get(5,TimeUnit.SECONDS));
                try (var cursor=lease.openCursor(74,null,null,false)) {
                    assertEquals(1,snapshots.stats().cursors());
                    for (var expected:forward) assertEquals(List.of(expected),cursor.nextPage(new RangeBudget(1,64),RequestContext.batch()).get(5,TimeUnit.SECONDS).items().stream().map(KV::keys).toList());
                    assertTrue(cursor.nextPage(new RangeBudget(1,64),RequestContext.batch()).get(5,TimeUnit.SECONDS).items().isEmpty());
                }
                assertEquals(0,snapshots.stats().cursors());
                var releasedCursor=lease.openCursor(71,null,null,false);
                lease.release(RequestContext.batch()).get(10,TimeUnit.SECONDS); assertTrue(lease.snapshot().observedHome().consumed());
                assertEquals(0,snapshots.stats().cursors()); lost(releasedCursor.nextPage(new RangeBudget(1,64),RequestContext.batch()));
                for (var group:f.groups) assertEquals(0,f.machines.get(group).snapshotPinStats().pins());
                lost(lease.get(71,key(2),RequestContext.batch()));
            }
            int captures=f.captureRequests.get();
            try (var snapshots=client(f); var reopened=snapshots.reopen(path,RequestContext.batch())) {
                lost(reopened.completion()); lost(reopened.get(71,key(2),RequestContext.batch()));
                assertEquals(captures,f.captureRequests.get(),"Released historical pins are never recreated on reopen");
            }
        }
    }
    @Test void fixedPrefixBoundsPageOnlyTheDeclaredCollisionColumnAcrossBothGroups() throws Exception {
        try (var f=new Fixture(root,true)) {
            write(f,0,7,"prefix-seed.bin"); var column=f.schema.column(74).columnId();
            var declared=new TransactionScope(List.of(TransactionScope.Span.range(column,new byte[]{2},new byte[]{(byte)201})));
            try (var snapshots=client(f); var lease=snapshots.open(root.resolve("prefix-snapshot.bin"),snapshotIdentity(f,1),declared,TTL,RequestContext.batch())) {
                ready(lease); Keys lower=key(2),upper=key(201);
                var expected=List.of(member(2,"alpha"),member(2,"beta"),member(200,"alpha"),member(200,"beta"));
                for (boolean reverse:new boolean[]{false,true}) {
                    var ordered=new ArrayList<>(expected); if (reverse) Collections.reverse(ordered);
                    var page=lease.page(74,lower,upper,reverse,null,new RangeBudget(8,256),RequestContext.batch()).get(5,TimeUnit.SECONDS);
                    assertEquals(ordered,page.items().stream().map(KV::keys).toList()); assertFalse(page.hasMore());
                    Keys continuation=null;
                    for (int n=0;n<ordered.size();n++) {
                        var one=lease.page(74,lower,upper,reverse,continuation,new RangeBudget(1,64),RequestContext.batch()).get(5,TimeUnit.SECONDS);
                        assertEquals(List.of(ordered.get(n)),one.items().stream().map(KV::keys).toList());
                        assertEquals(n+1<ordered.size(),one.hasMore()); continuation=one.resumeAfter();
                    }
                    try (var cursor=lease.openCursor(74,lower,upper,reverse)) {
                        for (var item:ordered) assertEquals(List.of(item),cursor.nextPage(new RangeBudget(1,64),RequestContext.batch()).get(5,TimeUnit.SECONDS).items().stream().map(KV::keys).toList());
                        assertTrue(cursor.nextPage(new RangeBudget(1,64),RequestContext.batch()).get(5,TimeUnit.SECONDS).items().isEmpty());
                    }
                }
                assertArrayEquals(new byte[]{7},lease.get(74,member(200,"beta"),RequestContext.batch()).get(5,TimeUnit.SECONDS).toByteArray());
                int calls=f.rpcs.get();
                assertThrows(ExecutionException.class,() -> lease.get(71,key(2),RequestContext.batch()).get(5,TimeUnit.SECONDS));
                assertEquals(calls,f.rpcs.get(),"A column outside the declared scope is rejected before owner dispatch");
                assertArrayEquals(new byte[]{7},lease.get(74,member(2,"alpha"),RequestContext.batch()).get(5,TimeUnit.SECONDS).toByteArray());
                for (var participant:lease.snapshot().participants()) for (var span:participant.capture().request().fence().participant(participant.route().groupId()).scope().spans()) {
                    assertEquals(column,span.columnId());
                    assertTrue(Arrays.compareUnsigned(span.lowerInclusive(),new byte[]{2})>=0); assertTrue(Arrays.compareUnsigned(span.upperExclusive(),new byte[]{(byte)201})<=0);
                }
                lease.release(RequestContext.batch()).get(10,TimeUnit.SECONDS);
            }
        }
    }
    @Test void lostCaptureReplyReopensExactFrozenNonceAndOwnerWithoutAnotherNativePin() throws Exception {
        try (var f=new Fixture(root,true)) {
            write(f,0,7,"seed-lost.bin"); int transactionOpens=f.opens.get(); Path path=root.resolve("lost-capture.bin"); Map<UUID,UUID> frozen=new HashMap<>();
            f.loseCaptureGroup=f.groups.getFirst();
            try (var snapshots=client(f); var lease=snapshots.open(path,snapshotIdentity(f,1),scope(f),TTL,RequestContext.batch())) {
                var failure=assertThrows(ExecutionException.class,() -> lease.completion().get(10,TimeUnit.SECONDS)); assertInstanceOf(IOException.class,failure.getCause());
                for (var participant:lease.snapshot().participants()) if (participant.capture()!=null) frozen.put(participant.route().groupId(),participant.capture().request().snapshotId());
                assertFalse(frozen.isEmpty()); assertEquals(1,f.machines.get(f.groups.getFirst()).snapshotPinStats().pins());
            }
            try (var snapshots=client(f); var lease=snapshots.reopen(path,RequestContext.batch())) {
                ready(lease);
                for (var participant:lease.snapshot().participants()) {
                    if (frozen.containsKey(participant.route().groupId())) assertEquals(frozen.get(participant.route().groupId()),participant.captureToken().snapshotId());
                    assertEquals(1,f.machines.get(participant.route().groupId()).snapshotPinStats().entries()); assertEquals(1,f.machines.get(participant.route().groupId()).snapshotPinStats().pins());
                }
                assertArrayEquals(new byte[]{7},lease.get(74,member(2,"alpha"),RequestContext.batch()).get(5,TimeUnit.SECONDS).toByteArray());
                assertEquals(transactionOpens,f.opens.get(),"Snapshot acquisition and retry do not open scoped transaction replacements");
                lease.release(RequestContext.batch()).get(10,TimeUnit.SECONDS);
            }
        }
    }
    @Test void snapshotHelpsAnAlreadyChosenCommitBeforeCapturingAnyParticipantImage() throws Exception {
        try (var f=new Fixture(root,true)) {
            f.losePrepareGroup=f.groups.getLast();
            try (var atomic=f.client(); var operation=atomic.executeAtomic(root.resolve("prepared-before-snapshot.bin"),f.identity(0),image(7),List.of(),TTL,RequestContext.batch())) {
                var failure=assertThrows(ExecutionException.class,() -> operation.completion().get(10,TimeUnit.SECONDS)); assertInstanceOf(IOException.class,failure.getCause());
                for (var group:f.groups) assertNotNull(f.stores.get(group).preparedParticipant(f.identity(0).transaction()));
            }
            var voted=f.homes.read(f.identity(0).transaction()); long control=200;
            assertNull(voted.participants().getLast().prepared());
            for (var participant:List.copyOf(voted.participants())) if (participant.prepared()==null) {
                var prepared=f.stores.get(participant.groupId()).preparedParticipant(voted.transactionId());
                var next=voted.prepared(participant.groupId(),participant.planDigest(),prepared.position());
                assertEquals(CommandResult.Status.SUCCESS,f.homes.execute(f.homes.prepare(new OperationId(f.session,1,control++),voted,next)).status()); voted=next;
            }
            var committed=voted.decide(TransactionStatus.Phase.COMMIT);
            assertEquals(CommandResult.Status.SUCCESS,f.homes.execute(f.homes.prepare(new OperationId(f.session,1,control),voted,committed)).status());
            assertFalse(committed.fullyFinalized());
            var captures=new java.util.concurrent.atomic.AtomicInteger();
            f.beforeSnapshotCapture=() -> {
                try {
                    var fence=f.snapshots.read(snapshotIdentity(f,1).fence()).fence();
                    for (var group:f.groups) {
                        var outcome=f.stores.get(group).finalizedParticipant(committed.transactionId());
                        assertNotNull(outcome,"Every old native blocker has a terminal outcome before the first image capture");
                        assertEquals(TransactionStatus.Phase.COMMIT,outcome.finalization().decision().phase());
                        assertEquals(f.identity(0).business(),outcome.finalization().decision().operation());
                        assertTrue(outcome.finalization().decision().sameDefinition(committed));
                        assertTrue(f.stores.get(group).gateBlockers(fence).isEmpty(),"Retained PREPARE history is no longer an unresolved gate blocker");
                    }
                    for (long column=71;column<=73;column++) for (int prefix:new int[]{2,200}) assertArrayEquals(new byte[]{7},f.get(column,key(prefix)));
                    for (int prefix:new int[]{2,200}) for (String text:List.of("alpha","beta")) assertArrayEquals(new byte[]{7},f.get(74,member(prefix,text)));
                } catch (IOException unavailable) { throw new java.io.UncheckedIOException(unavailable); }
                captures.incrementAndGet();
            };
            try (var snapshots=client(f); var lease=snapshots.open(root.resolve("help-commit.bin"),snapshotIdentity(f,1),scope(f),TTL,RequestContext.batch())) {
                ready(lease); assertTrue(f.helped.get()>0); assertTrue(captures.get()>=2);
                var applied=new HashMap<UUID,ApplyPosition>(); for (var group:f.groups) applied.put(group,f.stores.get(group).applied());
                // Native READY establishes the data cut. Durable HOME acknowledgements are separate
                // control bookkeeping, resumed by the same bounded registry service used at runtime.
                var recovery=new RatisTransactionRecoveryService(f.scheduler,() -> f.snapshotRecoveries.cycle(4));
                try {
                    for (int n=0;n<8 && !f.homes.read(committed.transactionId()).fullyFinalized();n++) recovery.tick().get(5,TimeUnit.SECONDS);
                    assertTrue(f.homes.read(committed.transactionId()).fullyFinalized()); assertFalse(f.homes.read(committed.transactionId()).consumed());
                    for (var group:f.groups) assertEquals(applied.get(group),f.stores.get(group).applied(),"ACK replay never reapplies the old business effects");
                } finally { recovery.stop().get(5,TimeUnit.SECONDS); }
                for (long column=71;column<=73;column++) for (int prefix:new int[]{2,200}) assertArrayEquals(new byte[]{7},lease.get(column,key(prefix),RequestContext.batch()).get(5,TimeUnit.SECONDS).toByteArray());
                for (int prefix:new int[]{2,200}) for (String text:List.of("alpha","beta")) assertArrayEquals(new byte[]{7},lease.get(74,member(prefix,text),RequestContext.batch()).get(5,TimeUnit.SECONDS).toByteArray());
                for (var group:f.groups) {
                    var outcome=f.stores.get(group).finalizedParticipant(committed.transactionId()); assertNotNull(outcome);
                    assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(outcome.result()).status());
                }
                lease.release(RequestContext.batch()).get(10,TimeUnit.SECONDS);
                for (var group:f.groups) assertEquals(0,f.machines.get(group).snapshotPinStats().pins());
            }
        }
    }
    @Test void explicitNativeOwnerLossIsPermanentAndNeverFallsBackToLatestCapture() throws Exception {
        try (var f=new Fixture(root,true)) {
            write(f,0,7,"seed-owner.bin"); Path path=root.resolve("owner-loss.bin");
            try (var snapshots=client(f); var lease=snapshots.open(path,snapshotIdentity(f,1),scope(f),TTL,RequestContext.batch())) {
                ready(lease); int captures=f.captureRequests.get(); f.machines.get(f.groups.getFirst()).beginShutdown();
                lost(lease.get(71,key(2),RequestContext.batch())); assertEquals(captures,f.captureRequests.get());
            }
            int captures=f.captureRequests.get();
            try (var snapshots=client(f); var lease=snapshots.reopen(path,RequestContext.batch())) {
                lease.completion().get(10,TimeUnit.SECONDS); lost(lease.get(71,key(2),RequestContext.batch())); assertEquals(captures,f.captureRequests.get());
                lease.release(RequestContext.batch()).get(10,TimeUnit.SECONDS);
            }
        }
    }
}
