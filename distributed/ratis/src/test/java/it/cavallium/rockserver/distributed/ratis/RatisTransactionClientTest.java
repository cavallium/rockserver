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

/** Public writable facade over real native groups and controlled dispatch, without server processes or network. */
class RatisTransactionClientTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final RequestContext CONTEXT=RequestContext.batch();
    static final Duration TTL=Duration.ofSeconds(30);
    Keys key(int value) { return RatisAtomicClientTest.key(value); }
    Keys member(int value,String tail) { return RatisAtomicClientTest.member(value,tail); }
    Buf value(int value) { return RatisAtomicClientTest.value(value); }
    RatisTransactionClient.Identity identity(RatisAtomicClientTest.Fixture f) {
        return new RatisTransactionClient.Identity(new RatisSnapshotClient.Identity(new UUID(190,1),new OperationId(f.session,1,0),new OperationId(f.session,1,1)),
                new RatisAtomicClient.Identity(new UUID(190,2),new OperationId(f.session,1,600),new OperationId(f.session,1,601)));
    }
    TransactionScope scope(RatisAtomicClientTest.Fixture f) {
        return new TransactionScope(List.of(TransactionScope.Span.range(f.schema.column(71).columnId(),new byte[]{1},new byte[]{(byte)205}),
                TransactionScope.Span.range(f.schema.column(74).columnId(),new byte[]{2},new byte[]{(byte)201})));
    }
    RatisTransactionClient client(RatisAtomicClientTest.Fixture f) { return new RatisTransactionClient(RatisTransactionBindings.of(f.schema,f.data.directory(f.schema),f.data),f.snapshots,f.homes,f.scheduler,RatisTransactionClient.Limits.defaults()); }
    void seed(RatisAtomicClientTest.Fixture f) throws Exception {
        put(f,71,key(2),1,900); put(f,71,key(200),3,901);
        for (int prefix:new int[]{2,200}) {
            var changes=List.of(RatisLogicalClient.Change.put(74,member(prefix,"alpha"),value(prefix==2 ? 4 : 6),Mutation.ReturnMode.NONE),RatisLogicalClient.Change.put(74,member(prefix,"beta"),value(prefix==2 ? 5 : 7),Mutation.ReturnMode.NONE));
            var physical=RatisLogicalClient.plan(scopeColumnId -> f.schema,changes,List.of(),RatisTransactionPlan.MAX_BYTES);
            assertEquals(CommandResult.Status.SUCCESS,f.data.execute(f.data.prepare(new OperationId(f.session,1,prefix==2 ? 902 : 903),physical.mutations(),physical.predicates())).status());
        }
    }
    void put(RatisAtomicClientTest.Fixture f,long column,Keys key,int value,long sequence) throws IOException {
        var physical=RatisLogicalClient.plan(scopeColumnId -> f.schema,List.of(RatisLogicalClient.Change.put(column,key,value(value),Mutation.ReturnMode.NONE)),List.of(),RatisTransactionPlan.MAX_BYTES);
        assertEquals(CommandResult.Status.SUCCESS,f.data.execute(f.data.prepare(new OperationId(f.session,1,sequence),physical.mutations(),physical.predicates())).status());
    }
    List<RatisLogicalClient.Change> prefix() { return List.of(RatisLogicalClient.Change.put(71,key(2),value(7),Mutation.ReturnMode.PREVIOUS),RatisLogicalClient.Change.put(74,member(2,"beta"),value(8),Mutation.ReturnMode.CURRENT)); }
    List<Keys> keys(RangePage<KV> page) { return page.items().stream().map(KV::keys).toList(); }
    void acquired(RatisTransactionClient.Lease lease,RatisAtomicClientTest.Fixture f) throws Exception {
        lease.completion().get(10,TimeUnit.SECONDS); assertEquals(RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED,lease.snapshot().captureMode()); assertEquals(SnapshotFenceStatus.Phase.COMPLETE,lease.snapshot().observedHome().phase()); assertEquals(0,f.opens.get());
    }

    @Test void stagedPointBucketsPagesAndCursorCommitOriginalCutsWithoutOpenOrConsumption() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("commit"),true)) {
            seed(f); var id=identity(f);
            try (var client=client(f); var lease=client.open(f.root.resolve("snapshot.journal"),f.root.resolve("commit.journal"),id,scope(f),TTL,CONTEXT)) {
                acquired(lease,f); var saved=lease.snapshot(); assertArrayEquals(new byte[]{1},lease.get(71,key(2),CONTEXT).get(5,TimeUnit.SECONDS).toByteArray());
                var staged=lease.stage(prefix(),List.of(),CONTEXT).get(5,TimeUnit.SECONDS); assertEquals(2,staged.size()); assertEquals(0,staged.get(f.groups.getLast()).valueCount()); assertEquals(2,staged.get(f.groups.getFirst()).valueCount());
                assertArrayEquals(new byte[]{7},lease.get(71,key(2),CONTEXT).get(5,TimeUnit.SECONDS).toByteArray()); assertArrayEquals(new byte[]{8},lease.get(74,member(2,"beta"),CONTEXT).get(5,TimeUnit.SECONDS).toByteArray()); assertArrayEquals(new byte[]{3},lease.get(71,key(200),CONTEXT).get(5,TimeUnit.SECONDS).toByteArray());
                var first=lease.page(74,key(2),key(201),false,null,new RangeBudget(2,1024),CONTEXT).get(5,TimeUnit.SECONDS); assertEquals(List.of(member(2,"alpha"),member(2,"beta")),keys(first)); assertTrue(first.hasMore());
                assertEquals(List.of(member(200,"alpha"),member(200,"beta")),keys(lease.page(74,key(2),key(201),false,first.resumeAfter(),RangeBudget.DEFAULT,CONTEXT).get(5,TimeUnit.SECONDS)));
                try (var cursor=lease.openCursor(71,key(1),key(205),false)) {
                    assertEquals(List.of(key(2)),keys(cursor.nextPage(new RangeBudget(1,64),CONTEXT).get(5,TimeUnit.SECONDS)));
                    assertEquals(List.of(key(200)),keys(cursor.nextPage(new RangeBudget(1,64),CONTEXT).get(5,TimeUnit.SECONDS)));
                }
                lease.stage(prefix(),List.of(),CONTEXT).get(5,TimeUnit.SECONDS);
                var extended=new ArrayList<>(prefix()); extended.add(RatisLogicalClient.Change.put(74,member(2,"gamma"),value(9),Mutation.ReturnMode.CURRENT));
                lease.stage(extended,List.of(new RatisLogicalClient.Condition(74,member(2,"gamma"),null)),CONTEXT).get(5,TimeUnit.SECONDS);
                assertNull(f.homes.read(id.commit().transaction())); int captures=f.captureRequests.get();
                var operation=lease.commit(CONTEXT); assertSame(operation,lease.commit(CONTEXT)); assertSame(operation,lease.commitOperation());
                var result=operation.completion().get(10,TimeUnit.SECONDS); assertEquals(CommandResult.Status.SUCCESS,result.status()); assertEquals(3,result.valueCount());
                assertArrayEquals(new byte[]{1},result.value(0)); assertArrayEquals(new byte[]{8},result.value(1)); assertArrayEquals(new byte[]{9},result.value(2));
                for (var progress:operation.snapshot().participants()) {
                    var original=saved.participant(progress.intent().route().groupId()).certifiedHandle(); assertEquals(original.id(),progress.intent().captureId()); assertEquals(original.position(),progress.intent().acceptedBusiness().acceptance().snapshot());
                }
                assertEquals(0,f.opens.get()); assertEquals(captures,f.captureRequests.get()); assertFalse(f.homes.read(id.commit().transaction()).consumed());
                lease.release(CONTEXT).get(10,TimeUnit.SECONDS); assertFalse(f.homes.read(id.commit().transaction()).consumed()); assertSame(operation,lease.commit(CONTEXT));
            }
            assertArrayEquals(new byte[]{7},f.get(71,key(2))); assertArrayEquals(new byte[]{8},f.get(74,member(2,"beta"))); assertArrayEquals(new byte[]{9},f.get(74,member(2,"gamma")));
        }
    }

    @Test void changedReadOnlyParticipantAbortsAllStagedEffectsAndStillRequiresExplicitRelease() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("conflict"),true); var client=client(f)) {
            seed(f); var id=identity(f);
            try (var lease=client.open(f.root.resolve("snapshot.journal"),f.root.resolve("commit.journal"),id,scope(f),TTL,CONTEXT)) {
                acquired(lease,f); lease.stage(prefix(),List.of(),CONTEXT).get(5,TimeUnit.SECONDS); put(f,71,key(200),9,990);
                var operation=lease.commit(CONTEXT); assertEquals(CommandResult.Status.CONFLICT,operation.completion().get(10,TimeUnit.SECONDS).status());
                assertEquals(TransactionStatus.Phase.ABORT,f.homes.read(id.commit().transaction()).phase()); assertFalse(f.homes.read(id.commit().transaction()).consumed());
                assertArrayEquals(new byte[]{1},f.get(71,key(2))); assertArrayEquals(new byte[]{5},f.get(74,member(2,"beta"))); assertArrayEquals(new byte[]{9},f.get(71,key(200)));
                lease.release(CONTEXT).get(10,TimeUnit.SECONDS); assertEquals(0,f.opens.get()); assertFalse(f.homes.read(id.commit().transaction()).consumed());
            }
        }
    }

    @Test void unknownOriginalPrepareReplyReopensExactCommitWithoutNewCaptures() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("unknown"),true)) {
            seed(f); var id=identity(f); var path=f.root.resolve("commit.journal"); int captures;
            try (var client=client(f); var lease=client.open(f.root.resolve("snapshot.journal"),path,id,scope(f),TTL,CONTEXT)) {
                acquired(lease,f); lease.stage(prefix(),List.of(),CONTEXT).get(5,TimeUnit.SECONDS); captures=f.captureRequests.get(); f.losePrepareGroup=f.groups.getLast();
                var operation=lease.commit(CONTEXT); assertThrows(ExecutionException.class,() -> operation.completion().get(10,TimeUnit.SECONDS));
                assertThrows(ExecutionException.class,() -> lease.release(CONTEXT).get(5,TimeUnit.SECONDS)); assertNotNull(f.stores.get(f.groups.getLast()).preparedParticipant(id.commit().transaction()));
            }
            var ids=new HashMap<UUID,UUID>(); var commands=new HashMap<UUID,byte[]>();
            try (var journal=RatisDurableTransaction.reopen(path,f.homes,RatisTransactionBindings.of(f.data))) { for (var progress:journal.snapshot().participants()) { ids.put(progress.intent().route().groupId(),progress.intent().captureId()); commands.put(progress.intent().route().groupId(),DistributedCommandCodec.encode(progress.intent().acceptedBusiness())); } }
            try (var client=client(f); var operation=client.reopenCommit(path,CONTEXT)) {
                assertEquals(CommandResult.Status.SUCCESS,operation.completion().get(10,TimeUnit.SECONDS).status());
                for (var progress:operation.snapshot().participants()) { assertEquals(ids.get(progress.intent().route().groupId()),progress.intent().captureId()); assertArrayEquals(commands.get(progress.intent().route().groupId()),DistributedCommandCodec.encode(progress.intent().acceptedBusiness())); }
                assertFalse(f.homes.read(id.commit().transaction()).consumed()); assertEquals(0,f.opens.get()); assertEquals(captures,f.captureRequests.get());
            }
            assertArrayEquals(new byte[]{7},f.get(71,key(2))); assertArrayEquals(new byte[]{8},f.get(74,member(2,"beta")));
        }
    }

    @Test void rollbackAndReleaseOnlyReopenNeverRestoreAnEphemeralPrefix() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("rollback"),true)) {
            seed(f); var id=identity(f); var snapshot=f.root.resolve("snapshot.journal"); var commit=f.root.resolve("commit.journal");
            try (var client=client(f); var lease=client.open(snapshot,commit,id,scope(f),TTL,CONTEXT)) {
                acquired(lease,f); lease.stage(prefix(),List.of(),CONTEXT).get(5,TimeUnit.SECONDS); assertNull(f.homes.read(id.commit().transaction()));
            }
            int captures=f.captureRequests.get();
            try (var client=client(f)) {
                assertThrows(IOException.class,() -> client.open(snapshot,commit,id,scope(f),TTL,CONTEXT));
                try (var cleanup=client.reopenForRelease(snapshot,CONTEXT)) { cleanup.completion().get(10,TimeUnit.SECONDS); cleanup.resume(CONTEXT).get(5,TimeUnit.SECONDS); }
                assertEquals(0,client.stats().snapshots().leases()); assertEquals(0,client.stats().commits().operations());
            }
            assertArrayEquals(new byte[]{1},f.get(71,key(2))); assertArrayEquals(new byte[]{5},f.get(74,member(2,"beta"))); assertNull(f.homes.read(id.commit().transaction())); assertFalse(Files.exists(commit)); assertEquals(0,f.opens.get()); assertEquals(captures,f.captureRequests.get());
        }
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("explicit-rollback"),true); var client=client(f)) {
            seed(f); var id=identity(f);
            try (var lease=client.open(f.root.resolve("snapshot.journal"),f.root.resolve("commit.journal"),id,scope(f),TTL,CONTEXT)) {
                acquired(lease,f); lease.stage(prefix(),List.of(),CONTEXT).get(5,TimeUnit.SECONDS); lease.rollback(CONTEXT).get(10,TimeUnit.SECONDS);
                assertThrows(IOException.class,() -> lease.commit(CONTEXT)); assertNull(f.homes.read(id.commit().transaction())); assertArrayEquals(new byte[]{1},f.get(71,key(2)));
            }
        }
    }

    @Test void identityPathAndCommitAdmissionGuardsRetainMutableOwnership() throws Exception {
        var session=new UUID(0,1); var snapshotId=new RatisSnapshotClient.Identity(new UUID(191,1),new OperationId(session,1,0),new OperationId(session,1,1));
        assertThrows(IllegalArgumentException.class,() -> new RatisTransactionClient.Identity(snapshotId,new RatisAtomicClient.Identity(new UUID(191,2),new OperationId(session,1,10),new OperationId(session,1,11))));
        assertThrows(IllegalArgumentException.class,() -> new RatisTransactionClient.Identity(snapshotId,new RatisAtomicClient.Identity(snapshotId.fence(),new OperationId(session,1,600),new OperationId(session,1,601))));
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("guards"),true)) {
            seed(f); var limits=new RatisTransactionClient.Limits(RatisSnapshotClient.Limits.defaults(),new RatisAtomicClient.Limits(1,128L*1024*1024));
            try (var client=new RatisTransactionClient(RatisTransactionBindings.of(f.schema,f.data.directory(f.schema),f.data),f.snapshots,f.homes,f.scheduler,limits)) {
                var snapshot=f.root.resolve("snapshot.journal"); var commit=f.root.resolve("commit.journal"); var id=identity(f);
                try (var lease=client.open(snapshot,commit,id,scope(f),TTL,CONTEXT)) {
                    acquired(lease,f); int captures=f.captureRequests.get();
                    assertThrows(IOException.class,() -> client.open(commit,f.root.resolve("other-commit"),id,scope(f),TTL,CONTEXT));
                    assertThrows(IOException.class,() -> client.open(f.root.resolve("other-snapshot"),snapshot,id,scope(f),TTL,CONTEXT));
                    assertThrows(IOException.class,() -> client.reopenCommit(snapshot,CONTEXT)); assertThrows(IOException.class,() -> client.reopenForRelease(commit,CONTEXT)); assertEquals(captures,f.captureRequests.get());
                    lease.stage(prefix(),List.of(),CONTEXT).get(5,TimeUnit.SECONDS);
                    try (var occupying=client.reopenCommit(f.root.resolve("missing-commit"),CONTEXT)) {
                        assertThrows(ExecutionException.class,() -> occupying.completion().get(5,TimeUnit.SECONDS)); assertThrows(IOException.class,() -> lease.commit(CONTEXT)); assertNull(lease.commitOperation());
                        lease.stage(prefix(),List.of(),CONTEXT).get(5,TimeUnit.SECONDS); assertArrayEquals(new byte[]{7},lease.get(71,key(2),CONTEXT).get(5,TimeUnit.SECONDS).toByteArray());
                    }
                    var operation=lease.commit(CONTEXT); assertEquals(CommandResult.Status.SUCCESS,operation.completion().get(10,TimeUnit.SECONDS).status()); lease.release(CONTEXT).get(10,TimeUnit.SECONDS);
                }
            }
            assertEquals(0,f.opens.get()); assertArrayEquals(new byte[]{7},f.get(71,key(2)));
        }
    }
}
