package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.DistributedSchema;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import static org.junit.jupiter.api.Assertions.*;

/** Deterministic actual-return boundaries without server processes or native storage fixtures. */
class RatisRocksDBIteratorLifecycleTest {
    static final RequestContext CONTEXT=RequestContext.batch();
    static Keys key(int value) { return new Keys(Buf.wrap(new byte[]{(byte)value})); }
    static KV row(int value) { return new KV(key(value),Buf.wrap(new byte[]{(byte)value})); }
    static DistributedSchema schema() {
        return new DistributedSchema(new UUID(240,1),new UUID(240,2),new UUID(240,3),1,1,
                List.of(new DistributedSchema.Column(71,new UUID(240,4),"rows",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null)));
    }
    static <T> T await(CompletableFuture<T> future) throws Exception { return future.get(5,TimeUnit.SECONDS); }
    static void latch(CountDownLatch latch) {
        try { if (!latch.await(5,TimeUnit.SECONDS)) throw new AssertionError("Controlled callback did not arrive"); }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
    }
    static final class Resource implements RatisRocksDBIterators.Resource {
        final CompletableFuture<RangePage<KV>> page=new CompletableFuture<>();
        final CompletableFuture<Void> released=new CompletableFuture<>();
        final CountDownLatch requested=new CountDownLatch(1),releaseEntered=new CountDownLatch(1);
        final AtomicInteger pageCalls=new AtomicInteger(),releaseCalls=new AtomicInteger(),localCloses=new AtomicInteger();
        volatile Runnable releaseHook=() -> {};
        public CompletableFuture<RangePage<KV>> page(Keys lower,Keys upper,boolean reverse,Keys resume,RangeBudget budget,RequestContext context) {
            pageCalls.incrementAndGet(); requested.countDown(); return page;
        }
        public CompletableFuture<Void> release(RequestContext context) { releaseCalls.incrementAndGet(); releaseEntered.countDown(); releaseHook.run(); return released; }
        public void closeLocal() throws IOException { localCloses.incrementAndGet(); }
        void finishPage() { page.complete(new RangePage<>(List.of(row(1),row(2)),key(2),false)); }
    }
    static final class Observer implements Subscriber<KV> {
        final CopyOnWriteArrayList<Integer> values=new CopyOnWriteArrayList<>();
        final CompletableFuture<Throwable> terminal=new CompletableFuture<>();
        final AtomicReference<Subscription> subscription=new AtomicReference<>();
        public void onSubscribe(Subscription subscription) { this.subscription.set(subscription); subscription.request(1); }
        public void onNext(KV value) { values.add(Byte.toUnsignedInt(value.value().toByteArray()[0])); }
        public void onError(Throwable failure) { terminal.complete(failure); }
        public void onComplete() { terminal.complete(null); }
    }
    static void empty(RatisRocksDBIterators iterators) {
        assertEquals(0,iterators.stats().iterators()); assertEquals(0,iterators.stats().streams()); assertEquals(0,iterators.stats().bytes());
    }

    @Test void concurrentDemandCannotOvertakeFirstRowWhilePageCompletionPublishesIt() throws Exception {
        var iterators=new RatisRocksDBIterators(scopeColumnId -> schema()); var resource=new Resource(); resource.released.complete(null);
        var observer=new Observer(); var copying=new CountDownLatch(1); var publish=new CountDownLatch(1);
        Buf delegate=Buf.wrap(new byte[]{1});
        Buf blocked=(Buf)Proxy.newProxyInstance(Buf.class.getClassLoader(),new Class<?>[]{Buf.class},(proxy,method,args) -> {
            if (method.getName().equals("toByteArray") && method.getParameterCount()==0) { copying.countDown(); latch(publish); }
            try { return method.invoke(delegate,args); } catch (InvocationTargetException failure) { throw failure.getCause(); }
        });
        try (var executor=Executors.newFixedThreadPool(2)) {
            iterators.stream(0,71,key(1),key(3),false,() -> CompletableFuture.completedFuture(resource),CONTEXT).subscribe(observer);
            latch(resource.requested);
            var completion=CompletableFuture.runAsync(() -> resource.page.complete(new RangePage<>(List.of(new KV(key(1),blocked),row(2)),key(2),false)),executor);
            try {
                latch(copying);
                await(CompletableFuture.runAsync(() -> observer.subscription.get().request(1),executor));
                assertTrue(observer.values.isEmpty(),"The later request must not emit row2 before row1 has returned from publication");
                assertFalse(observer.terminal.isDone()); assertEquals(1,resource.pageCalls.get());
            } finally { publish.countDown(); }
            await(completion); assertNull(await(observer.terminal)); assertEquals(List.of(1,2),observer.values);
            assertEquals(1,resource.releaseCalls.get()); assertEquals(1,resource.localCloses.get()); empty(iterators);
        } finally { publish.countDown(); observer.subscription.get().cancel(); }
    }

    @Test void closeAllWaitsForActualIteratorPageAndRelease() throws Exception { closeEntry(true); }
    @Test void transactionInvalidationWaitsForActualIteratorPageAndRelease() throws Exception { closeEntry(false); }
    void closeEntry(boolean all) throws Exception {
        var iterators=new RatisRocksDBIterators(scopeColumnId -> schema()); var resource=new Resource();
        long id=await(iterators.open(42,71,key(1),key(3),false,Duration.ofSeconds(30),() -> CompletableFuture.completedFuture(resource),CONTEXT));
        var reading=iterators.subsequent(id,0,2,RequestType.RequestTypeId.MULTI,CONTEXT); latch(resource.requested);
        var closing=all ? iterators.closeAll(CONTEXT) : iterators.invalidateTransaction(42,CONTEXT);
        try {
            assertFalse(closing.isDone(),"Closing ownership must wait for the actual blocked page callback");
            assertFalse(reading.isDone()); assertEquals(0,resource.releaseCalls.get()); assertEquals(1,iterators.stats().iterators());
            resource.finishPage(); latch(resource.releaseEntered);
            assertFalse(closing.isDone(),"A started release is not proof that its actual callback has returned"); assertEquals(0,resource.localCloses.get()); assertEquals(1,iterators.stats().iterators());
        } finally { resource.finishPage(); resource.released.complete(null); }
        await(closing); var failure=assertThrows(ExecutionException.class,() -> await(reading));
        assertEquals(RocksDBException.RocksDBErrorType.TX_NOT_FOUND,assertInstanceOf(RocksDBException.class,failure.getCause()).getErrorUniqueId());
        assertEquals(1,resource.releaseCalls.get()); assertEquals(1,resource.localCloses.get()); empty(iterators);
    }

    @Test void invalidatingSubscribedTransactionStreamSignalsFailureAndWaitsForNativeDrain() throws Exception {
        var iterators=new RatisRocksDBIterators(scopeColumnId -> schema()); var resource=new Resource(); var observer=new Observer();
        iterators.stream(42,71,key(1),key(3),false,() -> CompletableFuture.completedFuture(resource),CONTEXT).subscribe(observer); latch(resource.requested);
        var closing=iterators.invalidateTransaction(42,CONTEXT);
        try {
            assertFalse(closing.isDone()); assertEquals(0,resource.releaseCalls.get()); assertEquals(1,iterators.stats().streams());
            resource.finishPage(); latch(resource.releaseEntered);
            var failure=assertInstanceOf(RocksDBException.class,await(observer.terminal)); assertEquals(RocksDBException.RocksDBErrorType.TX_NOT_FOUND,failure.getErrorUniqueId());
            assertTrue(observer.values.isEmpty()); assertFalse(closing.isDone()); assertEquals(0,resource.localCloses.get());
        } finally { resource.finishPage(); resource.released.complete(null); observer.subscription.get().cancel(); }
        await(closing); assertEquals(1,resource.localCloses.get()); empty(iterators);
    }

    @Test void closeAllSubscribedStreamWaitsForActualPageAndRelease() throws Exception {
        var iterators=new RatisRocksDBIterators(scopeColumnId -> schema()); var resource=new Resource(); var observer=new Observer();
        iterators.stream(0,71,key(1),key(3),false,() -> CompletableFuture.completedFuture(resource),CONTEXT).subscribe(observer); latch(resource.requested);
        var closing=iterators.closeAll(CONTEXT);
        try {
            assertFalse(closing.isDone()); assertEquals(0,resource.releaseCalls.get()); resource.finishPage(); latch(resource.releaseEntered);
            assertFalse(closing.isDone()); assertEquals(1,iterators.stats().streams()); assertEquals(0,resource.localCloses.get());
        } finally { resource.finishPage(); resource.released.complete(null); observer.subscription.get().cancel(); }
        await(closing); assertEquals(1,resource.releaseCalls.get()); assertEquals(1,resource.localCloses.get()); empty(iterators);
    }

    @Test void expiryReleaseCallbackCannotHoldRegistryMonitorOrBlockIndependentAdmission() throws Exception {
        var iterators=new RatisRocksDBIterators(scopeColumnId -> schema()); var resource=new Resource(); var releaseReturns=new CountDownLatch(1);
        resource.released.complete(null); resource.releaseHook=() -> latch(releaseReturns);
        long id=await(iterators.open(42,71,key(1),key(3),false,Duration.ofMillis(100),() -> CompletableFuture.completedFuture(resource),CONTEXT)); Thread.sleep(150);
        try (var executor=Executors.newFixedThreadPool(2)) {
            var expiring=CompletableFuture.runAsync(() -> {
                var failure=assertThrows(RocksDBException.class,() -> iterators.transaction(id)); assertEquals(RocksDBException.RocksDBErrorType.TX_NOT_FOUND,failure.getErrorUniqueId());
            },executor);
            try {
                latch(resource.releaseEntered);
                assertEquals(1,await(CompletableFuture.supplyAsync(() -> iterators.stats().iterators(),executor)));
                var healthy=new Resource(); healthy.released.complete(null);
                long next=await(CompletableFuture.supplyAsync(() -> {
                    try { return await(iterators.open(43,71,key(1),key(3),false,Duration.ofSeconds(30),() -> CompletableFuture.completedFuture(healthy),CONTEXT)); }
                    catch (Exception failure) { throw new CompletionException(failure); }
                },executor));
                await(iterators.close(next,CONTEXT)); assertFalse(expiring.isDone());
            } finally { releaseReturns.countDown(); }
            await(expiring); assertEquals(1,resource.localCloses.get()); empty(iterators);
        } finally { releaseReturns.countDown(); }
    }
}
