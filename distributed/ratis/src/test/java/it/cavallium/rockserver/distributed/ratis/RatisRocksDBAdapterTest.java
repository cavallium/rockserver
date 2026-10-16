package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Explicit scoped transactions over controlled dispatch and real native DATA/HOME state. */
class RatisRocksDBAdapterTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final RequestContext CONTEXT=RequestContext.batch();
    static final Duration TTL=Duration.ofSeconds(30);
    static Keys key(int n) { return RatisAtomicClientTest.key(n); }
    static TransactionOpenScope scope(long... columns) {
        return new TransactionOpenScope(Arrays.stream(columns).mapToObj(column -> TransactionOpenScope.Span.range(column,null,null)).toList());
    }
    static Keys member(int n,String tail) { return RatisAtomicClientTest.member(n,tail); }
    static Buf value(int n) { return RatisAtomicClientTest.value(n); }
    static <T> T await(CompletableFuture<T> future) throws Exception { return future.get(10,TimeUnit.SECONDS); }
    static void bytes(int expected,Buf actual) { assertNotNull(actual); assertArrayEquals(new byte[]{(byte)expected},actual.toByteArray()); }
    static void error(RocksDBException.RocksDBErrorType expected,CompletableFuture<?> future) {
        var failure=assertThrows(ExecutionException.class,() -> await(future));
        assertEquals(expected,assertInstanceOf(RocksDBException.class,failure.getCause()).getErrorUniqueId());
    }
    static final class Allocations implements RatisRocksDBAdapter.AllocationSource {
        final RatisAtomicClientTest.Fixture f;
        final AtomicInteger count=new AtomicInteger();
        final List<RatisRocksDBAdapter.Allocation> issued=new ArrayList<>();
        Allocations(RatisAtomicClientTest.Fixture f) { this.f=f; }
        public synchronized CompletableFuture<RatisRocksDBAdapter.Allocation> allocate(RequestContext context,List<RatisBackendSessions.Target> dataTargets) {
            int index=count.getAndIncrement(); long base=index*770L;
            var identity=new RatisTransactionClient.Identity(
                    new RatisSnapshotClient.Identity(new UUID(220,index*2+1),new OperationId(f.session,1,base),new OperationId(f.session,1,base+1)),
                    new RatisAtomicClient.Identity(new UUID(220,index*2+2),new OperationId(f.session,1,base+513),new OperationId(f.session,1,base+514)));
            var allocation=new RatisRocksDBAdapter.Allocation(f.root.resolve("snapshot-"+index),f.root.resolve("commit-"+index),identity);
            issued.add(allocation); return CompletableFuture.completedFuture(allocation);
        }
    }
    static RatisTransactionClient client(RatisAtomicClientTest.Fixture f,RatisRoutedClient data) {
        return new RatisTransactionClient(RatisTransactionBindings.of(f.schema,data.directory(f.schema),data),f.snapshots,f.homes,f.scheduler,RatisTransactionClient.Limits.defaults());
    }
    static void seed(RatisAtomicClientTest.Fixture f,long column,Keys key,Buf value,long sequence) throws IOException {
        var plan=RatisLogicalClient.plan(scopeColumnId -> f.schema,List.of(RatisLogicalClient.Change.put(column,key,value,Mutation.ReturnMode.NONE)),List.of(),RatisTransactionPlan.MAX_BYTES);
        assertEquals(CommandResult.Status.SUCCESS,f.data.execute(f.data.prepare(new OperationId(f.session,1,sequence),plan.mutations(),plan.predicates())).status());
    }

    @Test void explicitMultiColumnTwoGroupFlowPreservesOrderedReturnsAndReadYourWrites() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("flow"),true); var transactions=client(f,f.data)) {
            seed(f,71,key(2),value(1),1900); seed(f,71,key(200),value(3),1901);
            seed(f,73,key(2),Buf.wrap(new byte[0]),1902); seed(f,74,member(2,"alpha"),value(4),1903);
            var allocations=new Allocations(f);
            try (var adapter=new RatisRocksDBAdapter(RatisTransactionBindings.of(f.schema,f.directory,(transactions).bindings().domain((f.schema).domainId()).client()),transactions,allocations,null,null)) {
                var api=adapter.api(CONTEXT); long id=await(api.openTransactionAsync(scope(71,72,73,74), TTL));
                var update=await(api.getAsync(id,71,key(2),RequestType.<Buf>forUpdate())); assertEquals(id,update.updateId()); bytes(1,update.previous());
                assertTrue(await(api.getAsync(id,73,key(2),RequestType.<Buf>exists()))); assertEquals(0,await(api.getAsync(id,73,key(2),RequestType.<Buf>current())).size());
                bytes(1,await(api.putAsync(id,71,key(2),value(7),RequestType.<Buf>previous())));
                assertFalse(await(api.putAsync(id,71,key(2),value(7),RequestType.<Buf>changed())));
                var delta=await(api.putAsync(id,71,key(2),value(8),RequestType.<Buf>delta())); bytes(7,delta.previous()); bytes(8,delta.current());
                assertTrue(await(api.putAsync(id,72,key(200),value(9),RequestType.<Buf>changed())));
                var previous=await(api.putMultiAsync(id,71,List.of(key(200),key(200)),List.of(value(10),value(11)),RequestType.<Buf>previous()));
                bytes(3,previous.get(0)); bytes(10,previous.get(1));
                assertEquals(List.of(),await(api.putMultiAsync(id,72,List.of(key(201)),List.of(value(12)),RequestType.<Buf>none())));
                assertEquals(List.of(),await(api.putMultiAsync(id,72,List.of(key(201)),List.of(value(13)),RequestType.<Buf>ensure())));
                bytes(13,await(api.getAsync(id,72,key(201),RequestType.<Buf>current())));
                assertEquals(List.of(),await(api.deleteMultiAsync(id,72,List.of(key(201)),RequestType.<Buf>none())));
                assertNull(await(api.getAsync(id,72,key(201),RequestType.<Buf>current())));
                int beforeEmpty=f.rpcs.get();
                assertEquals(List.of(),await(api.putMultiAsync(id,72,List.of(),List.of(),RequestType.<Buf>none())));
                assertEquals(List.of(),await(api.putMultiAsync(id,72,List.of(),List.of(),RequestType.<Buf>ensure())));
                assertEquals(List.of(),await(api.deleteMultiAsync(id,72,List.of(),RequestType.<Buf>none())));
                assertEquals(beforeEmpty,f.rpcs.get());
                assertTrue(await(api.deleteAsync(id,73,key(2),RequestType.<Buf>previousPresence())));
                await(api.putAsync(id,74,member(2,"beta"),value(5),RequestType.<Buf>none()));
                bytes(8,await(api.getAsync(id,71,key(2),RequestType.<Buf>current()))); bytes(11,await(api.getAsync(id,71,key(200),RequestType.<Buf>current())));
                assertEquals(List.of(false,false),await(api.existsMultiAsync(id,73,List.of(key(200),key(2)))));
                var page=await(api.getRangePageAsync(id,74,key(2),key(3),false,null,RequestType.<KV>allInRange(),RangeBudget.DEFAULT));
                assertEquals(List.of(member(2,"alpha"),member(2,"beta")),page.items().stream().map(KV::keys).toList());
                assertTrue(await(api.closeTransactionAsync(id,true))); assertEquals(0,adapter.stats().transactions());
                assertEquals(1,allocations.count.get()); assertEquals(0,f.opens.get()); assertTrue(f.homes.read(allocations.issued.getFirst().identity().commit().transaction()).consumed());
                assertTrue(await(api.closeTransactionAsync(id,false)));
                error(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,api.getAsync(0,71,key(2),RequestType.<Buf>current()));
                error(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,api.putAsync(0,71,key(2),value(1),RequestType.<Buf>none()));
                error(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,api.deleteRangeAsync(71,null,null));
            }
            assertArrayEquals(new byte[]{8},f.get(71,key(2))); assertArrayEquals(new byte[]{11},f.get(71,key(200))); assertArrayEquals(new byte[]{9},f.get(72,key(200))); assertNull(f.get(73,key(2)));
            assertEquals(0,transactions.stats().snapshots().leases());
        }
    }

    @Test void conflictRetainsOriginalIdAndUnknownPrepareReplyRetriesOriginalJournal() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("retry"),true); var transactions=client(f,f.data)) {
            seed(f,71,key(2),value(1),1900); seed(f,71,key(200),value(3),1901); var allocations=new Allocations(f);
            try (var adapter=new RatisRocksDBAdapter(RatisTransactionBindings.of(f.schema,f.directory,(transactions).bindings().domain((f.schema).domainId()).client()),transactions,allocations,null,null)) {
                var api=adapter.api(CONTEXT); long rejected=await(api.openTransactionAsync(scope(71,72,73,74), TTL));
                await(api.putAsync(rejected,71,key(2),value(7),RequestType.<Buf>none())); seed(f,71,key(200),value(4),1902);
                assertFalse(await(api.closeTransactionAsync(rejected,true))); int captures=f.captureRequests.get();
                assertFalse(await(api.closeTransactionAsync(rejected,true))); assertEquals(1,allocations.count.get()); assertEquals(captures,f.captureRequests.get()); assertEquals(1,adapter.stats().transactions());
                assertArrayEquals(new byte[]{1},f.get(71,key(2))); assertTrue(await(api.closeTransactionAsync(rejected,false))); assertTrue(await(api.closeTransactionAsync(rejected,false)));
                long id=await(api.openTransactionAsync(scope(71,72,73,74), TTL)); await(api.putAsync(id,72,key(2),value(9),RequestType.<Buf>none()));
                captures=f.captureRequests.get(); var original=allocations.issued.getLast(); f.losePrepareGroup=f.groups.getLast();
                assertThrows(ExecutionException.class,() -> await(api.closeTransactionAsync(id,true)));
                assertNotNull(f.stores.get(f.groups.getLast()).preparedParticipant(original.identity().commit().transaction()));
                assertTrue(await(api.closeTransactionAsync(id,true))); assertEquals(2,allocations.count.get()); assertEquals(captures,f.captureRequests.get()); assertEquals(0,f.opens.get());
                try (var journal=RatisDurableTransaction.reopen(original.commit(),f.homes,RatisTransactionBindings.of(f.data))) {
                    assertEquals(CommandResult.Status.SUCCESS,journal.result().status());
                    for (var participant:journal.snapshot().participants()) assertNotNull(participant.intent().captureId());
                }
            }
            assertArrayEquals(new byte[]{9},f.get(72,key(2)));
        }
    }

    @Test void activeStageOwnsInputsUntilActualReturnAndDoesNotQueueDifferentOperations() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("copies"),true)) {
            var entered=new CountDownLatch(1); var proceed=new CountDownLatch(1); var base=f.transport();
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return base.read(route,column,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return base.command(route,command); }
                public byte[] participant(RangeDescriptor route,byte[] record) throws IOException { return base.participant(route,record); }
                public java.util.concurrent.CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] request) throws IOException { return base.snapshot(route,owner,request); }
                public RangeStateView stateView(RangeDescriptor route) throws IOException { return base.stateView(route); }
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException {
                    if (ScopedTransactionRpc.kind(request,ScopedTransactionRpc.MAX_REQUEST_BYTES)==ScopedTransactionRpc.STAGE) {
                        entered.countDown(); try { if (!proceed.await(10,TimeUnit.SECONDS)) throw new IOException("Blocked stage timed out"); } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IOException(failure); }
                    }
                    return base.transaction(route,request,fresh);
                }
            };
            try (var data=new RatisRoutedClient(f.directory,transport); var transactions=client(f,data)) {
                var allocations=new Allocations(f);
                try (var adapter=new RatisRocksDBAdapter(RatisTransactionBindings.of(f.schema,f.directory,(transactions).bindings().domain((f.schema).domainId()).client()),transactions,allocations,null,null)) {
                    var api=adapter.api(CONTEXT); long id=await(api.openTransactionAsync(scope(71,72,73,74), TTL)); byte[] rawKey={2},rawValue={7};
                    var keys=new ArrayList<>(List.of(new Keys(Buf.wrap(rawKey)))); var values=new ArrayList<>(List.of(Buf.wrap(rawValue)));
                    var write=api.putMultiAsync(id,71,keys,values,RequestType.<Buf>delta());
                    try {
                        assertTrue(entered.await(5,TimeUnit.SECONDS)); rawKey[0]=3; rawValue[0]=8; keys.clear(); values.clear();
                        error(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,api.getAsync(id,71,key(2),RequestType.<Buf>current()));
                    } finally { proceed.countDown(); }
                    var results=await(write); assertNull(results.getFirst().previous()); bytes(7,results.getFirst().current());
                    bytes(7,await(api.getAsync(id,71,key(2),RequestType.<Buf>current()))); assertNull(await(api.getAsync(id,71,key(3),RequestType.<Buf>current())));
                    assertTrue(await(api.closeTransactionAsync(id,false))); assertNull(f.get(71,key(2))); assertEquals(0,f.opens.get());
                }
            }
        }
    }

    @Test void keyOnlyCurrentIsRejectedBeforeAllocationOrNativeReads() throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("key-only"),true)) {
            var original=f.schema.column(71);var schema=new DistributedSchema(f.schema.clusterId(),f.schema.databaseId(),f.schema.domainId(),f.schema.schemaEpoch(),1,List.of(new DistributedSchema.Column(71,original.columnId(),original.name(),ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),false),null)));
            var directory=new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),schema.schemaEpoch(),schema.digest(),0,schema.columnIds(),1,f.directory.ranges());var allocations=new Allocations(f);
            try(var routed=new RatisRoutedClient(directory,f.transport());var transactions=new RatisTransactionClient(RatisTransactionBindings.of(schema,directory,routed),f.snapshots,f.homes,f.scheduler,RatisTransactionClient.Limits.defaults());var adapter=new RatisRocksDBAdapter(transactions.bindings(),transactions,allocations,null,null)) {
                int rpcs=f.rpcs.get();error(RocksDBException.RocksDBErrorType.VALUE_MUST_BE_NULL,adapter.api(CONTEXT).getAsync(1,71,key(2),RequestType.<Buf>current()));assertEquals(0,allocations.count.get());assertEquals(rpcs,f.rpcs.get());
            }
        }
    }
    @Test void openCapturesDeclaredPointsBeforeFirstReadAndConflictRetainsThatCut() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("eager-points"),true); var transactions=client(f,f.data)) {
            seed(f,71,key(2),value(1),1900);seed(f,72,key(200),value(2),1901);
            var allocations=new Allocations(f);
            try(var adapter=new RatisRocksDBAdapter(RatisTransactionBindings.of(f.schema,f.directory,(transactions).bindings().domain((f.schema).domainId()).client()),transactions,allocations,null,null)) {
                var declaration=new TransactionOpenScope(List.of(TransactionOpenScope.Span.point(71,key(2)),TransactionOpenScope.Span.point(72,key(200))));
                var api=adapter.api(CONTEXT);long id=await(api.openTransactionAsync(declaration,TTL));int captures=f.captureRequests.get();assertTrue(captures>0);
                // No operation on the handle precedes these independently committed writes.
                seed(f,71,key(2),value(3),1902);seed(f,72,key(200),value(4),1903);
                bytes(1,await(api.getAsync(id,71,key(2),RequestType.<Buf>current())));bytes(2,await(api.getAsync(id,72,key(200),RequestType.<Buf>current())));
                await(api.putAsync(id,72,key(200),value(9),RequestType.none()));
                assertFalse(await(api.closeTransactionAsync(id,true)));assertFalse(await(api.closeTransactionAsync(id,true)));
                assertEquals(1,adapter.stats().transactions());assertEquals(1,allocations.count.get());assertEquals(captures,f.captureRequests.get());
                assertTrue(await(api.closeTransactionAsync(id,false)));assertArrayEquals(new byte[]{4},f.get(72,key(200)));
            }
        }
    }
    @Test void invalidDeclarationsAllocateNoHandleIdentityOrCapture() throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("scope-preflight"),true);var transactions=client(f,f.data)) {
            var allocations=new Allocations(f);var ids=new AtomicInteger();
            try(var adapter=new RatisRocksDBAdapter(RatisTransactionBindings.of(f.schema,f.directory,(transactions).bindings().domain((f.schema).domainId()).client()),transactions,allocations,null,null,ids::incrementAndGet)) {
                var api=adapter.api(CONTEXT);int captures=f.captureRequests.get();
                error(RocksDBException.RocksDBErrorType.TX_SCOPE_REQUIRED,api.openTransactionAsync(null,TTL));
                assertThrows(ExecutionException.class,()->await(api.openTransactionAsync(scope(999),TTL)));
                var backward=new TransactionOpenScope(List.of(TransactionOpenScope.Span.range(71,key(3),key(2))));
                assertThrows(ExecutionException.class,()->await(api.openTransactionAsync(backward,TTL)));
                assertEquals(0,ids.get());assertEquals(0,allocations.count.get());assertEquals(0,adapter.stats().transactions());assertEquals(captures,f.captureRequests.get());
                var ranges=new ArrayList<RangeDescriptor>();var original=f.directory.ranges().getFirst();
                for(int n=0;n<65;n++)ranges.add(new RangeDescriptor(new UUID(300,n+1),new UUID(301,n+1),1,0,n==0?null:new byte[]{(byte)n},n==64?null:new byte[]{(byte)(n+1)},RangeDescriptor.TopologyState.ACTIVE,original.replicas()));
                var directory=new RangeDirectory(f.schema.clusterId(),f.schema.databaseId(),f.schema.domainId(),f.schema.schemaEpoch(),f.schema.digest(),0,f.schema.columnIds(),1,ranges);
                try(var bounded=new RatisRocksDBAdapter(RatisTransactionBindings.of(f.schema,directory,(transactions).bindings().domain((f.schema).domainId()).client()),transactions,allocations,null,null,ids::incrementAndGet)) {
                    assertThrows(ExecutionException.class,()->await(bounded.api(CONTEXT).openTransactionAsync(scope(71),TTL)));
                    assertEquals(0,ids.get());assertEquals(0,allocations.count.get());assertEquals(0,bounded.stats().transactions());assertEquals(captures,f.captureRequests.get());
                }
            }
        }
    }
    @Test void leaderLossRejectsOriginalWritableCaptureWithoutRecapture() throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("scope-leader-loss"),true);var transactions=client(f,f.data)) {
            var allocations=new Allocations(f);
            try(var adapter=new RatisRocksDBAdapter(RatisTransactionBindings.of(f.schema,f.directory,(transactions).bindings().domain((f.schema).domainId()).client()),transactions,allocations,null,null)) {
                var api=adapter.api(CONTEXT);long id=await(api.openTransactionAsync(new TransactionOpenScope(List.of(TransactionOpenScope.Span.point(71,key(2)))),TTL));int captures=f.captureRequests.get();
                f.machines.get(f.groups.getFirst()).notifyNotLeader(List.of());
                assertThrows(ExecutionException.class,()->await(api.putAsync(id,71,key(2),value(9),RequestType.none())));
                assertEquals(captures,f.captureRequests.get());assertEquals(1,allocations.count.get());assertNull(f.get(71,key(2)));
                assertTrue(await(api.closeTransactionAsync(id,false)));
            }
        }
    }

}
