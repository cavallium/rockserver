package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Per-subscription ownership, independent from observer termination and controlled actual futures. */
class RatisOwnedStreamDrainTest {
    static final RequestContext CONTEXT=RatisRocksDBIteratorLifecycleTest.CONTEXT;
    static <T> T await(CompletableFuture<T> future) throws Exception { return RatisRocksDBIteratorLifecycleTest.await(future); }
    static Keys key(int n) { return RatisRocksDBIteratorLifecycleTest.key(n); }
    @Test void cancellationBeforeSubscriptionDrainsWithoutStartingAcquisition() throws Exception {
        var iterators=new RatisRocksDBIterators(RatisRocksDBIteratorLifecycleTest.schema()); var calls=new AtomicInteger();
        var owned=iterators.streamOwned(0,71,key(1),key(3),false,() -> { calls.incrementAndGet(); throw new AssertionError("Cancelled subscription must not acquire"); },CONTEXT);
        assertFalse(owned.drained().isDone()); owned.cancelObserver().run(); await(owned.drained());
        var observer=new RatisRocksDBIteratorLifecycleTest.Observer(); owned.observer().subscribe(observer); assertNull(await(observer.terminal)); assertEquals(0,calls.get()); RatisRocksDBIteratorLifecycleTest.empty(iterators);
    }
    @Test void cancelledAcquisitionAndCancelledPageWaitForActualReleaseAndClose() throws Exception {
        for (boolean acquiring:List.of(true,false)) {
            var iterators=new RatisRocksDBIterators(RatisRocksDBIteratorLifecycleTest.schema()); var resource=new RatisRocksDBIteratorLifecycleTest.Resource();
            var ready=new CompletableFuture<RatisRocksDBIterators.Resource>(); if (!acquiring) ready.complete(resource);
            var owned=iterators.streamOwned(0,71,key(1),key(3),false,() -> new RatisRocksDBIterators.Acquisition(ready,CompletableFuture.completedFuture(null)),CONTEXT);
            var observer=new RatisRocksDBIteratorLifecycleTest.Observer(); owned.observer().subscribe(observer); observer.subscription.get().cancel();
            assertFalse(owned.drained().isDone()); assertEquals(1,iterators.stats().streams()); assertEquals(0,resource.releaseCalls.get());
            if (acquiring) ready.complete(resource); else resource.finishPage();
            assertFalse(owned.drained().isDone()); assertEquals(1,resource.releaseCalls.get()); assertEquals(0,resource.localCloses.get()); assertTrue(observer.values.isEmpty());
            resource.released.complete(null); await(owned.drained()); assertEquals(1,resource.localCloses.get()); RatisRocksDBIteratorLifecycleTest.empty(iterators);
        }
    }
    @Test void failedCleanupRetainsLifetimeDrainUntilActualRetrySucceeds() throws Exception {
        var iterators=new RatisRocksDBIterators(RatisRocksDBIteratorLifecycleTest.schema()); var releasing=new CompletableFuture<Void>(); var calls=new AtomicInteger(); var closes=new AtomicInteger();
        RatisRocksDBIterators.Resource resource=new RatisRocksDBIterators.Resource() {
            public CompletableFuture<RangePage<KV>> page(Keys low,Keys high,boolean reverse,Keys resume,RangeBudget budget,RequestContext context) { return CompletableFuture.completedFuture(new RangePage<>(List.of(RatisRocksDBIteratorLifecycleTest.row(1)),key(1),false)); }
            public CompletableFuture<Void> release(RequestContext context) { return calls.incrementAndGet()==1 ? CompletableFuture.failedFuture(new IOException("Unknown release")) : releasing; }
            public void closeLocal() { closes.incrementAndGet(); }
        };
        var owned=iterators.streamOwned(0,71,key(1),key(3),false,() -> new RatisRocksDBIterators.Acquisition(CompletableFuture.completedFuture(resource),CompletableFuture.completedFuture(null)),CONTEXT);
        var observer=new RatisRocksDBIteratorLifecycleTest.Observer(); owned.observer().subscribe(observer);
        assertInstanceOf(IOException.class,await(observer.terminal)); assertFalse(owned.drained().isDone()); assertEquals(1,iterators.stats().streams()); assertEquals(0,closes.get());
        var liveReady=new CompletableFuture<RatisRocksDBIterators.Resource>(); var liveResource=new RatisRocksDBIteratorLifecycleTest.Resource();
        var live=iterators.streamOwned(0,71,key(1),key(3),false,() -> new RatisRocksDBIterators.Acquisition(liveReady,CompletableFuture.completedFuture(null)),CONTEXT);
        var liveObserver=new RatisRocksDBIteratorLifecycleTest.Observer(); live.observer().subscribe(liveObserver);
        var retry=iterators.retryStoppedFlowsCleanup(); assertFalse(retry.isDone()); assertFalse(live.drained().isDone()); assertEquals(0,liveResource.releaseCalls.get()); releasing.complete(null); await(retry); await(owned.drained()); assertEquals(1,closes.get()); assertEquals(1,iterators.stats().streams());
        liveReady.complete(liveResource); assertEquals(1,liveResource.pageCalls.get(),"Nonclosing retry must leave live acquisition/page usable");
        liveObserver.subscription.get().cancel(); liveResource.finishPage(); liveResource.released.complete(null); await(live.drained()); RatisRocksDBIteratorLifecycleTest.empty(iterators);
    }
    @Test void publicColdStreamCreatesIndependentOwnershipForRepeatedSubscription() throws Exception {
        var iterators=new RatisRocksDBIterators(RatisRocksDBIteratorLifecycleTest.schema()); var acquisitions=new AtomicInteger();
        var stream=iterators.stream(0,71,key(1),key(3),false,() -> {
            acquisitions.incrementAndGet(); var resource=new RatisRocksDBIteratorLifecycleTest.Resource(); resource.finishPage(); resource.released.complete(null); return CompletableFuture.completedFuture(resource);
        },CONTEXT);
        assertEquals(0,acquisitions.get()); assertEquals(2,await(stream.collectList().toFuture()).size()); assertEquals(2,await(stream.collectList().toFuture()).size()); assertEquals(2,acquisitions.get()); RatisRocksDBIteratorLifecycleTest.empty(iterators);
    }
    @Test void adapterOwnedDispatchTracksFailedAcquisitionCleanupAfterObserverError() throws Exception { adapter(true); }
    @Test void adapterOwnedDispatchTracksActualPageAndReleaseAfterCancellation() throws Exception { adapter(false); }
    void adapter(boolean failedAcquisition) throws Exception {
        var schema=RatisRocksDBIteratorLifecycleTest.schema(); var replica=new ReplicaDescriptor(new UUID(1,1),new UUID(1,2),new UUID(1,3),"fixture","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        var directory=new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),schema.schemaEpoch(),schema.digest(),0,schema.columnIds(),schema.routingPrefixBytes(),List.of(new RangeDescriptor(new UUID(2,1),new UUID(2,2),1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica))));
        var snapshots=mock(RatisSnapshotClient.class); var lease=mock(RatisSnapshotClient.Lease.class); var acquiring=new CompletableFuture<Void>(); var reading=new CompletableFuture<RangePage<KV>>(); var releasing=new CompletableFuture<Void>(); var closes=new AtomicInteger();
        when(lease.completion()).thenReturn(acquiring);
        when(lease.page(anyLong(),any(),any(),anyBoolean(),any(),any(),any())).thenReturn(reading);
        when(lease.release(any())).thenReturn(releasing); doAnswer(call -> { closes.incrementAndGet(); return null; }).when(lease).close();
        when(snapshots.open(any(),any(),any(),any(),any())).thenReturn(lease);
        UUID session=new UUID(3,1);
        var identity=new RatisTransactionClient.Identity(new RatisSnapshotClient.Identity(new UUID(3,2),new OperationId(session,1,0),new OperationId(session,1,1)),new RatisAtomicClient.Identity(new UUID(3,3),new OperationId(session,1,513),new OperationId(session,1,514)));
        RatisRocksDBAdapter.AllocationSource allocations=context -> CompletableFuture.completedFuture(new RatisRocksDBAdapter.Allocation(Path.of("snapshot"),Path.of("commit"),identity));
        var ranges=new RatisRocksDBRanges(schema,directory,snapshots,allocations);
        var adapter=new RatisRocksDBAdapter(schema,directory,mock(RatisTransactionClient.class),allocations,null,ranges);
        var request=new RocksDBAPICommand.RocksDBAPICommandStream.GetRange<KV>(0,71,key(1),key(3),false,RequestType.<KV>allInRange());
        var dispatched=adapter.dispatchOwned(request,CONTEXT); assertFalse(dispatched.drained().isDone());
        @SuppressWarnings("unchecked") var stream=(Flux<KV>)dispatched.observer(); var observer=new RatisRocksDBIteratorLifecycleTest.Observer(); stream.subscribe(observer);
        try {
            if (failedAcquisition) {
                acquiring.completeExceptionally(new IOException("Failed snapshot acquisition")); assertInstanceOf(IOException.class,await(observer.terminal));
            } else {
                acquiring.complete(null); observer.subscription.get().cancel(); assertFalse(dispatched.drained().isDone()); verify(lease,never()).release(any()); reading.complete(new RangePage<>(List.of(RatisRocksDBIteratorLifecycleTest.row(1)),key(1),false));
            }
            assertFalse(dispatched.drained().isDone()); assertEquals(1,ranges.stats()); assertEquals(0,closes.get());
            releasing.complete(null); await(dispatched.drained()); assertEquals(0,ranges.stats()); assertEquals(1,closes.get());
        } finally { acquiring.complete(null); reading.complete(new RangePage<>(List.of(),null,false)); releasing.complete(null); dispatched.cancelObserver().run(); adapter.close(); ranges.close(); }
    }
    @Test void failedFinitePageAcquisitionRetainsActualOwnerUntilCleanupRetry() throws Exception { finite(0); }
    @Test void failedFirstAndLastAcquisitionRetainsActualOwnerUntilCleanupRetry() throws Exception { finite(1); }
    @Test void failedIteratorAcquisitionRetainsActualOwnerUntilCleanupRetry() throws Exception { finite(2); }
    void finite(int operation) throws Exception {
        var schema=RatisRocksDBIteratorLifecycleTest.schema(); var replica=new ReplicaDescriptor(new UUID(4,1),new UUID(4,2),new UUID(4,3),"fixture","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        var directory=new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),schema.schemaEpoch(),schema.digest(),0,schema.columnIds(),schema.routingPrefixBytes(),List.of(new RangeDescriptor(new UUID(5,1),new UUID(5,2),1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica))));
        var snapshots=mock(RatisSnapshotClient.class); var lease=mock(RatisSnapshotClient.Lease.class); var acquiring=new CompletableFuture<Void>(); var releasing=new CompletableFuture<Void>(); var closes=new AtomicInteger();
        when(lease.completion()).thenReturn(acquiring); when(lease.release(any())).thenReturn(CompletableFuture.failedFuture(new IOException("Unknown first release")),releasing);
        doAnswer(call -> { closes.incrementAndGet(); return null; }).when(lease).close(); when(snapshots.open(any(),any(),any(),any(),any())).thenReturn(lease);
        UUID session=new UUID(6,1); var identity=new RatisTransactionClient.Identity(new RatisSnapshotClient.Identity(new UUID(6,2),new OperationId(session,1,0),new OperationId(session,1,1)),new RatisAtomicClient.Identity(new UUID(6,3),new OperationId(session,1,513),new OperationId(session,1,514)));
        RatisRocksDBAdapter.AllocationSource allocations=context -> CompletableFuture.completedFuture(new RatisRocksDBAdapter.Allocation(Path.of("finite-snapshot"),Path.of("finite-commit"),identity));
        var ranges=new RatisRocksDBRanges(schema,directory,snapshots,allocations); var adapter=new RatisRocksDBAdapter(schema,directory,mock(RatisTransactionClient.class),allocations,null,ranges);
        RocksDBAPICommand<?,?,?> request=switch (operation) {
            case 0 -> new RocksDBAPICommand.RocksDBAPICommandSingle.GetRangePage<KV>(0,71,key(1),key(3),false,null,RequestType.<KV>allInRange(),RangeBudget.DEFAULT);
            case 1 -> new RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<FirstAndLast<KV>>(0,71,key(1),key(3),false,RequestType.<KV>firstAndLast());
            case 2 -> new RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator(0,71,key(1),key(3),false,Duration.ofSeconds(30));
            default -> throw new AssertionError();
        };
        var dispatched=adapter.dispatchOwned(request,CONTEXT); assertFalse(dispatched.drained().isDone());
        try {
            acquiring.completeExceptionally(new IOException("Actual acquisition failure"));
            var failure=assertThrows(ExecutionException.class,() -> await((CompletableFuture<?>)dispatched.observer())); assertInstanceOf(IOException.class,failure.getCause());
            assertFalse(dispatched.drained().isDone()); assertEquals(1,ranges.stats()); assertEquals(0,closes.get());
            var retry=adapter.retryStoppedFlowsCleanup(); assertFalse(retry.isDone()); verify(lease,atLeast(2)).release(any());
            releasing.complete(null); await(retry); await(dispatched.drained()); assertEquals(0,ranges.stats()); assertEquals(1,closes.get());
        } finally { acquiring.complete(null); releasing.complete(null); adapter.retryStoppedFlowsCleanup(); dispatched.cancelObserver().run(); adapter.close(); ranges.close(); }
    }

}
