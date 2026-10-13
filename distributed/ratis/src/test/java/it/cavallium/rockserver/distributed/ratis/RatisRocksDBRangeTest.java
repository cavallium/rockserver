package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisRocksDBAdapterTest.*;

/** Common range APIs use frozen native snapshot owners, including cancellation and staged writes. */
class RatisRocksDBRangeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static List<Keys> keys(RangePage<KV> page) { return page.items().stream().map(KV::keys).toList(); }
    static List<Integer> numbers(List<Buf> values) { return values.stream().map(value -> Byte.toUnsignedInt(value.toByteArray()[0])).toList(); }
    static void drained(RatisSnapshotClient snapshots) {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
        while (snapshots.stats().leases()!=0 && System.nanoTime()<deadline) LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
        assertEquals(0,snapshots.stats().leases()); assertEquals(0,snapshots.stats().reads()); assertEquals(0,snapshots.stats().cursors());
    }
    static final class RangeAllocations implements RatisRocksDBAdapter.AllocationSource {
        final RatisAtomicClientTest.Fixture f; final AtomicInteger count=new AtomicInteger();
        RangeAllocations(RatisAtomicClientTest.Fixture f) { this.f=f; }
        public CompletableFuture<RatisRocksDBAdapter.Allocation> allocate(RequestContext context) {
            int n=count.getAndIncrement(); long base=n*513L;
            var snapshot=new RatisSnapshotClient.Identity(new UUID(230,n*2+1),new OperationId(f.session,1,base),new OperationId(f.session,1,base+1));
            // Range helpers borrow only the snapshot block; this disjoint commit block is never dispatched.
            var commit=new RatisAtomicClient.Identity(new UUID(230,n*2+2),new OperationId(f.session,1,10000+n*257L),new OperationId(f.session,1,10001+n*257L));
            return CompletableFuture.completedFuture(new RatisRocksDBAdapter.Allocation(f.root.resolve("range-"+n),f.root.resolve("unused-commit-"+n),new RatisTransactionClient.Identity(snapshot,commit)));
        }
    }
    static void buckets(RatisAtomicClientTest.Fixture f) throws IOException {
        seed(f,74,member(2,"alpha"),value(1),1900); seed(f,74,member(2,"beta"),value(2),1901);
        seed(f,74,member(200,"alpha"),value(3),1902); seed(f,74,member(200,"beta"),value(4),1903);
    }
    static RatisSnapshotClient snapshots(RatisAtomicClientTest.Fixture f) { return new RatisSnapshotClient(f.schema,f.snapshots,f.data,f.scheduler,RatisSnapshotClient.Limits.defaults()); }

    @Test void zeroTransactionPagesCrossGroupsAndUseExclusiveLogicalResumeInBothDirections() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("pages"),true); var transactions=client(f,f.data); var snapshots=snapshots(f)) {
            buckets(f); var source=new RangeAllocations(f);
            try (var ranges=new RatisRocksDBRanges(f.schema,f.directory,snapshots,source); var adapter=new RatisRocksDBAdapter(f.schema,f.directory,transactions,new Allocations(f),null,ranges)) {
                var api=adapter.api(CONTEXT);
                var first=await(api.getRangePageAsync(0,74,key(2),key(201),false,null,RequestType.<KV>allInRange(),new RangeBudget(2,1024)));
                assertEquals(List.of(member(2,"alpha"),member(2,"beta")),keys(first)); assertTrue(first.hasMore());
                var second=await(api.getRangePageAsync(0,74,key(2),key(201),false,first.resumeAfter(),RequestType.<KV>allInRange(),RangeBudget.DEFAULT));
                assertEquals(List.of(member(200,"alpha"),member(200,"beta")),keys(second)); assertFalse(second.hasMore());
                var reverse=await(api.getRangePageAsync(0,74,key(2),key(201),true,member(200,"beta"),RequestType.<KV>allInRange(),RangeBudget.DEFAULT));
                assertEquals(List.of(member(200,"alpha"),member(2,"beta"),member(2,"alpha")),keys(reverse));
                assertEquals(3,source.count.get()); assertEquals(0,f.opens.get()); drained(snapshots);
            }
        }
    }

    @Test void retainedIteratorsKeepOldCutAndSeekInclusivelyAcrossMissingKeys() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("iterators"),true); var transactions=client(f,f.data); var snapshots=snapshots(f)) {
            seed(f,71,key(2),value(1),1900); seed(f,71,key(4),value(2),1901); seed(f,71,key(200),value(3),1902);
            var source=new RangeAllocations(f);
            try (var ranges=new RatisRocksDBRanges(f.schema,f.directory,snapshots,source); var adapter=new RatisRocksDBAdapter(f.schema,f.directory,transactions,new Allocations(f),null,ranges)) {
                var api=adapter.api(CONTEXT); long id=await(api.openIteratorAsync(0,71,key(1),key(201),false,TTL));
                seed(f,71,key(4),value(9),1903);
                var fresh=await(api.getRangePageAsync(0,71,key(1),key(201),false,null,RequestType.<KV>allInRange(),RangeBudget.DEFAULT)); bytes(9,fresh.items().get(1).value());
                await(api.seekToAsync(id,key(3))); assertEquals(List.of(2,3),numbers(await(api.subsequentAsync(id,0,2,RequestType.<Buf>multi()))));
                await(api.seekToAsync(id,key(2))); assertNull(await(api.subsequentAsync(id,0,1,RequestType.<Buf>none())));
                assertEquals(List.of(3),numbers(await(api.subsequentAsync(id,1,1,RequestType.<Buf>multi()))));
                assertFalse(await(api.subsequentAsync(id,0,1,RequestType.<Buf>exists()))); await(api.closeIteratorAsync(id));
                assertThrows(ExecutionException.class,() -> await(api.subsequentAsync(id,0,1,RequestType.<Buf>multi())));
                long reverse=await(api.openIteratorAsync(0,71,key(1),key(201),true,TTL)); await(api.seekToAsync(reverse,key(199)));
                assertEquals(List.of(9,1),numbers(await(api.subsequentAsync(reverse,0,2,RequestType.<Buf>multi()))));
                await(api.closeIteratorAsync(reverse)); assertEquals(3,source.count.get()); drained(snapshots);
            }
        }
    }

    @Test void collisionMemberSeekRetainsLogicalCaptureBounds() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("member-seek"),true); var transactions=client(f,f.data); var snapshots=snapshots(f)) {
            buckets(f); var source=new RangeAllocations(f);
            try (var ranges=new RatisRocksDBRanges(f.schema,f.directory,snapshots,source); var adapter=new RatisRocksDBAdapter(f.schema,f.directory,transactions,new Allocations(f),null,ranges)) {
                var api=adapter.api(CONTEXT);
                long iterator=await(api.openIteratorAsync(0,74,member(2,"beta"),member(200,"beta"),true,TTL));
                await(api.seekToAsync(iterator,member(200,"alpha")));
                assertEquals(List.of(3,2),numbers(await(api.subsequentAsync(iterator,0,3,RequestType.<Buf>multi()))));
                await(api.seekToAsync(iterator,member(200,"aardvark")));
                assertEquals(List.of(2),numbers(await(api.subsequentAsync(iterator,0,2,RequestType.<Buf>multi()))));
                await(api.closeIteratorAsync(iterator));
                var owner=await(ranges.open(74,member(2,"beta"),member(200,"beta"),false,TTL,CONTEXT));
                assertThrows(ExecutionException.class,() -> await(owner.page(member(2,"alpha"),member(200,"beta"),false,null,RangeBudget.DEFAULT,CONTEXT)));
                var narrowed=await(owner.page(key(200),member(200,"beta"),false,null,RangeBudget.DEFAULT,CONTEXT));
                assertEquals(List.of(member(200,"alpha")),keys(narrowed));
                await(owner.release(CONTEXT)); owner.closeLocal(); drained(snapshots);
            }
        }
    }

    @Test void reductionsAndDemandDrivenStreamReadLogicalRows() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("reductions"),true); var transactions=client(f,f.data); var snapshots=snapshots(f)) {
            buckets(f); var source=new RangeAllocations(f);
            try (var ranges=new RatisRocksDBRanges(f.schema,f.directory,snapshots,source); var adapter=new RatisRocksDBAdapter(f.schema,f.directory,transactions,new Allocations(f),null,ranges)) {
                var api=adapter.api(CONTEXT);
                assertEquals(4L,await(api.reduceRangeAsync(0,74,key(2),key(201),false,RequestType.<KV>entriesCount())));
                var ends=await(api.reduceRangeAsync(0,74,key(2),key(201),false,RequestType.<KV>firstAndLast()));
                assertEquals(member(2,"alpha"),ends.first().keys()); assertEquals(member(200,"beta"),ends.last().keys());
                var stream=api.getRangeAsync(0,74,key(2),key(201),false,RequestType.<KV>allInRange()); assertEquals(2,source.count.get());
                StepVerifier.create(Flux.from(stream),0).thenRequest(1).assertNext(row -> assertEquals(member(2,"alpha"),row.keys())).thenRequest(3)
                        .assertNext(row -> assertEquals(member(2,"beta"),row.keys())).assertNext(row -> assertEquals(member(200,"alpha"),row.keys())).assertNext(row -> assertEquals(member(200,"beta"),row.keys())).verifyComplete();
                assertEquals(3,source.count.get()); drained(snapshots);
            }
        }
    }

    @Test void firstAndLastUsesOneCutAcrossBothEdgesAndSupportsReverseAndEmptyRanges() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("edges"),true); var transactions=client(f,f.data); var snapshots=snapshots(f)) {
            seed(f,71,key(2),value(1),1900); seed(f,71,key(200),value(3),1901); var source=new RangeAllocations(f);
            try (var ranges=new RatisRocksDBRanges(f.schema,f.directory,snapshots,source); var adapter=new RatisRocksDBAdapter(f.schema,f.directory,transactions,new Allocations(f),null,ranges)) {
                var reads=new AtomicInteger();
                f.beforeSnapshotRead=() -> { if (reads.incrementAndGet()==2) try { seed(f,71,key(200),value(9),1902); } catch (IOException failure) { throw new CompletionException(failure); } };
                FirstAndLast<KV> ends;
                try { ends=await(adapter.api(CONTEXT).reduceRangeAsync(0,71,key(1),key(201),false,RequestType.<KV>firstAndLast())); }
                finally { f.beforeSnapshotRead=null; }
                assertTrue(reads.get()>=2); bytes(1,ends.first().value()); bytes(3,ends.last().value()); assertArrayEquals(new byte[]{9},f.get(71,key(200)));
                var reverse=await(adapter.api(CONTEXT).reduceRangeAsync(0,71,key(1),key(201),true,RequestType.<KV>firstAndLast())); bytes(9,reverse.first().value()); bytes(1,reverse.last().value());
                var empty=await(adapter.api(CONTEXT).reduceRangeAsync(0,71,key(5),key(6),false,RequestType.<KV>firstAndLast())); assertNull(empty.first()); assertNull(empty.last());
                assertEquals(3,source.count.get()); drained(snapshots);
            }
        }
    }

    @Test void cancellationDuringNativePageRetainsOwnerUntilActualCallbackReturns() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("cancel"),true); var transactions=client(f,f.data); var snapshots=snapshots(f)) {
            seed(f,71,key(2),value(1),1900); var entered=new CountDownLatch(1); var proceed=new CountDownLatch(1); var source=new RangeAllocations(f);
            try (var ranges=new RatisRocksDBRanges(f.schema,f.directory,snapshots,source); var adapter=new RatisRocksDBAdapter(f.schema,f.directory,transactions,new Allocations(f),null,ranges)) {
                var stream=adapter.api(CONTEXT).getRangeAsync(0,71,key(1),key(3),false,RequestType.<KV>allInRange()); assertEquals(0,source.count.get()); assertEquals(0,snapshots.stats().leases());
                f.beforeSnapshotRead=() -> { entered.countDown(); try { if (!proceed.await(5,TimeUnit.SECONDS)) throw new AssertionError("Native page was not unblocked"); } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); } };
                try {
                    StepVerifier.create(Flux.from(stream),0).thenRequest(1).then(() -> {
                        try { assertTrue(entered.await(5,TimeUnit.SECONDS)); } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
                        assertEquals(1,snapshots.stats().leases()); assertEquals(1,snapshots.stats().reads());
                    }).thenCancel().verify(Duration.ofSeconds(10));
                    assertEquals(1,snapshots.stats().leases()); assertEquals(1,snapshots.stats().reads()); assertThrows(IOException.class,snapshots::close); assertEquals(1,source.count.get());
                } finally { f.beforeSnapshotRead=null; proceed.countDown(); }
                drained(snapshots); assertEquals(0,f.opens.get());
            }
        }
    }

    @Test void expiredIteratorIdFailsAndReleasesItsOriginalCut() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("expired"),true); var transactions=client(f,f.data); var snapshots=snapshots(f)) {
            seed(f,71,key(2),value(1),1900); var source=new RangeAllocations(f);
            try (var ranges=new RatisRocksDBRanges(f.schema,f.directory,snapshots,source); var adapter=new RatisRocksDBAdapter(f.schema,f.directory,transactions,new Allocations(f),null,ranges)) {
                var api=adapter.api(CONTEXT); long id=await(api.openIteratorAsync(0,71,key(1),key(3),false,Duration.ofSeconds(1)));
                Thread.sleep(1100);
                error(RocksDBException.RocksDBErrorType.TX_NOT_FOUND,api.subsequentAsync(id,0,1,RequestType.<Buf>multi()));
                await(api.closeIteratorAsync(id)); drained(snapshots); assertEquals(1,source.count.get());
            }
        }
    }

    @Test void coalescedTransactionEdgeObserversOwnIndependentKeysAndValues() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("edge-observers"),true)) {
            seed(f,71,key(2),value(1),1900); seed(f,71,key(200),value(3),1901);
            var base=f.transport(); var entered=new CountDownLatch(1); var proceed=new CountDownLatch(1); var block=new AtomicBoolean(true);
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return base.read(route,column,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return base.command(route,command); }
                public byte[] participant(RangeDescriptor route,byte[] record) throws IOException { return base.participant(route,record); }
                public java.util.concurrent.CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] request) throws IOException { return base.snapshot(route,owner,request); }
                public RangeStateView stateView(RangeDescriptor route) throws IOException { return base.stateView(route); }
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException {
                    if (ScopedTransactionRpc.kind(request,ScopedTransactionRpc.MAX_REQUEST_BYTES)==ScopedTransactionRpc.PAGE && block.compareAndSet(true,false)) {
                        entered.countDown(); try { if (!proceed.await(5,TimeUnit.SECONDS)) throw new IOException("Page callback not unblocked"); } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IOException(failure); }
                    }
                    return base.transaction(route,request,fresh);
                }
            };
            try (var data=new RatisRoutedClient(f.directory,transport); var transactions=client(f,data);
                 var adapter=new RatisRocksDBAdapter(f.schema,f.directory,transactions,new Allocations(f),null,null)) {
                var api=adapter.api(CONTEXT); long tx=await(api.openTransactionAsync(TTL));
                CompletableFuture<FirstAndLast<KV>> first=api.reduceRangeAsync(tx,71,key(1),key(201),false,RequestType.<KV>firstAndLast()),second;
                try { assertTrue(entered.await(5,TimeUnit.SECONDS)); second=api.reduceRangeAsync(tx,71,key(1),key(201),false,RequestType.<KV>firstAndLast()); }
                finally { proceed.countDown(); }
                var a=await(first); var b=await(second); assertNotSame(a,b); assertNotSame(a.first().value(),b.first().value()); assertNotSame(a.last().value(),b.last().value());
                a.first().keys().keys()[0]=key(99).keys()[0]; a.last().keys().keys()[0]=key(98).keys()[0];
                assertEquals(key(2),b.first().keys()); assertEquals(key(200),b.last().keys()); bytes(1,b.first().value()); bytes(3,b.last().value());
                await(api.closeTransactionAsync(tx,false));
            } finally { proceed.countDown(); }
        }
    }

    @Test void stagedWriteInvalidatesAlreadyBufferedTransactionRows() throws Exception { bufferedStage(false,false); }
    @Test void uncertainStageInvalidatesAlreadyBufferedTransactionRows() throws Exception { bufferedStage(true,false); }
    @Test void transactionCloseInvalidatesAlreadyBufferedRows() throws Exception { bufferedStage(false,true); }
    void bufferedStage(boolean loseReply,boolean closeInstead) throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("buffered-"+loseReply+"-"+closeInstead),true)) {
            seed(f,71,key(2),value(1),1900); seed(f,71,key(200),value(3),1901);
            var base=f.transport(); var lose=new AtomicBoolean(loseReply);
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return base.read(route,column,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return base.command(route,command); }
                public byte[] participant(RangeDescriptor route,byte[] record) throws IOException { return base.participant(route,record); }
                public java.util.concurrent.CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] request) throws IOException { return base.snapshot(route,owner,request); }
                public RangeStateView stateView(RangeDescriptor route) throws IOException { return base.stateView(route); }
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException {
                    byte[] result=base.transaction(route,request,fresh);
                    if (ScopedTransactionRpc.kind(request,ScopedTransactionRpc.MAX_REQUEST_BYTES)==ScopedTransactionRpc.STAGE && lose.compareAndSet(true,false)) throw new IOException("Lost applied STAGE reply");
                    return result;
                }
            };
            try (var data=new RatisRoutedClient(f.directory,transport); var transactions=client(f,data);
                 var adapter=new RatisRocksDBAdapter(f.schema,f.directory,transactions,new Allocations(f),null,null)) {
                var api=adapter.api(CONTEXT); long tx=await(api.openTransactionAsync(TTL));
                var first=new CompletableFuture<KV>(); var terminal=new CompletableFuture<Throwable>(); var rows=new AtomicInteger();
                var subscriber=new reactor.core.publisher.BaseSubscriber<KV>() {
                    protected void hookOnSubscribe(org.reactivestreams.Subscription subscription) { request(1); }
                    protected void hookOnNext(KV row) { rows.incrementAndGet(); first.complete(row); }
                    protected void hookOnError(Throwable failure) { terminal.complete(failure); }
                    protected void hookOnComplete() { terminal.complete(null); }
                };
                Flux.from(api.getRangeAsync(tx,71,key(1),key(201),false,RequestType.<KV>allInRange())).subscribe(subscriber);
                bytes(1,await(first).value());
                if (closeInstead) await(api.closeTransactionAsync(tx,false));
                else if (loseReply) assertThrows(ExecutionException.class,() -> await(api.putAsync(tx,71,key(200),value(8),RequestType.<Buf>none())));
                else await(api.putAsync(tx,71,key(200),value(8),RequestType.<Buf>none()));
                subscriber.request(1);
                var failure=assertInstanceOf(RocksDBException.class,await(terminal)); assertEquals(RocksDBException.RocksDBErrorType.TX_NOT_FOUND,failure.getErrorUniqueId()); assertEquals(1,rows.get());
                if (loseReply) await(api.putAsync(tx,71,key(200),value(8),RequestType.<Buf>none()));
                if (!closeInstead) await(api.closeTransactionAsync(tx,false)); subscriber.dispose(); assertArrayEquals(new byte[]{3},f.get(71,key(200)));
            }
        }
    }

    @Test void transactionRangesSeeStagedPrefixAndNeverSilentlyContinueAcrossPrefixChanges() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("transaction"),true); var transactions=client(f,f.data); var snapshots=snapshots(f)) {
            seed(f,71,key(2),value(1),1900); seed(f,71,key(200),value(3),1901); var source=new RangeAllocations(f);
            try (var ranges=new RatisRocksDBRanges(f.schema,f.directory,snapshots,source); var adapter=new RatisRocksDBAdapter(f.schema,f.directory,transactions,new Allocations(f),null,ranges)) {
                var api=adapter.api(CONTEXT); long tx=await(api.openTransactionAsync(TTL)); await(api.putAsync(tx,71,key(2),value(7),RequestType.<Buf>none()));
                var page=await(api.getRangePageAsync(tx,71,key(1),key(201),false,null,RequestType.<KV>allInRange(),RangeBudget.DEFAULT)); bytes(7,page.items().getFirst().value());
                long iterator=await(api.openIteratorAsync(tx,71,key(1),key(201),false,TTL)); assertEquals(List.of(7),numbers(await(api.subsequentAsync(iterator,0,1,RequestType.<Buf>multi()))));
                await(api.putAsync(tx,71,key(200),value(8),RequestType.<Buf>none()));
                assertThrows(ExecutionException.class,() -> await(api.subsequentAsync(iterator,0,1,RequestType.<Buf>multi())));
                var ends=await(api.reduceRangeAsync(tx,71,key(1),key(201),true,RequestType.<KV>firstAndLast())); bytes(8,ends.first().value()); bytes(7,ends.last().value());
                StepVerifier.create(Flux.from(api.getRangeAsync(tx,71,key(1),key(201),false,RequestType.<KV>allInRange())),0).thenRequest(1)
                        .assertNext(row -> bytes(7,row.value())).thenRequest(1).assertNext(row -> bytes(8,row.value())).verifyComplete();
                await(api.closeTransactionAsync(tx,false)); assertThrows(ExecutionException.class,() -> await(api.subsequentAsync(iterator,0,1,RequestType.<Buf>multi())));
                assertEquals(0,source.count.get()); assertArrayEquals(new byte[]{1},f.get(71,key(2))); assertArrayEquals(new byte[]{3},f.get(71,key(200))); assertEquals(0,f.opens.get());
            }
        }
    }
}
