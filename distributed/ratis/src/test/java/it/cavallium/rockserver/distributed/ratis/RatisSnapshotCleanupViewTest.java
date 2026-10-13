package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Borrowed-route capability and ownership checks; no fabricated native process-GONE reply. */
class RatisSnapshotCleanupViewTest {
    final UUID cluster=UUID.randomUUID(),database=UUID.randomUUID(),domain=UUID.randomUUID(),column=UUID.randomUUID(),group=UUID.randomUUID();
    final ReplicaDescriptor owner=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"local","localhost:19010",ReplicaDescriptor.Role.VOTER);
    final RangeDescriptor range=new RangeDescriptor(UUID.randomUUID(),group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(owner));
    final RangeDirectory original=new RangeDirectory(cluster,database,domain,0,new byte[32],0,Set.of(column),0,List.of(range));
    private SnapshotFence frozen(){var session=UUID.randomUUID();return RatisSnapshotPlan.create(original,new TransactionScope(List.of(TransactionScope.Span.range(column,null,null))),UUID.randomUUID(),new OperationId(session,1,0),UUID.randomUUID(),1,new OperationId(session,1,1)).fence();}
    @Test void advancedPlacementCleanupKeepsIdentityPoolAndRejectsUserOrNewCaptureWork()throws Exception {
        var closes=new AtomicInteger();var calls=new AtomicInteger();var captures=new AtomicInteger();var fence=frozen();
        var transport=new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor r,UUID c,byte[] key){throw new AssertionError("USER read");}public CommandResult command(RangeDescriptor r,DistributedCommand command){throw new AssertionError("USER mutation");}public byte[] transaction(RangeDescriptor r,byte[] request,boolean fresh){calls.incrementAndGet();assertEquals(range,r);assertEquals(ScopedTransactionRpc.SNAPSHOT_GATE_STATE,ScopedTransactionRpc.kind(request,ScopedTransactionRpc.MAX_REQUEST_BYTES));return ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_GATE_STATE,ByteBuffer.allocate(4).putInt(-1).array());}public java.util.concurrent.CompletableFuture<byte[]> snapshot(RangeDescriptor r,ReplicaDescriptor owner,byte[] request)throws IOException{captures.incrementAndGet();throw new IOException("Unexpected capture transport");}public void close(){closes.incrementAndGet();}};
        try(var parent=new RatisRoutedClient(original,transport)) {
            var replacement=new RangeDescriptor(UUID.randomUUID(),UUID.randomUUID(),2,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(owner));parent.refresh(new RangeDirectory(cluster,database,domain,0,new byte[32],1,Set.of(column),0,List.of(replacement)));
            try(var view=parent.borrowedSnapshotCleanupView(original,fence)){assertArrayEquals(parent.sessionIdentityDigest(),view.sessionIdentityDigest());assertEquals(original,view.directory());assertNull(view.snapshotGate(fence,group));assertEquals(1,calls.get());assertThrows(IOException.class,()->view.read(column,new byte[]{1}));var command=new DistributedCommand(group,1,new byte[32],new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{2})));assertThrows(IOException.class,()->view.execute(view.restoreSessionCommand(command,range)));var target=view.restoreSnapshotTarget(fence,range,owner,UUID.randomUUID());assertThrows(IllegalStateException.class,()->view.prepareSnapshotCapture(target,UUID.randomUUID(),java.time.Duration.ofSeconds(30)));var forged=view.restoreSnapshotCapture(target,new RatisSnapshotCaptureRequest(fence,target.incarnation(),UUID.randomUUID(),java.time.Duration.ofSeconds(30)));assertThrows(IOException.class,()->view.executeSnapshotCapture(forged));assertEquals(0,captures.get());assertThrows(IOException.class,parent::close);assertEquals(0,closes.get());}
            assertEquals(0,closes.get());
        }
        assertEquals(1,closes.get());
    }
    @Test void viewCloseWaitsForActualOldRouteCallAndParentRetainsPooledTransport()throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var closes=new AtomicInteger();var fence=frozen();
        var transport=new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor r,UUID c,byte[] key){throw new AssertionError();}public CommandResult command(RangeDescriptor r,DistributedCommand command){throw new AssertionError();}public byte[] transaction(RangeDescriptor r,byte[] request,boolean fresh)throws IOException{entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new IOException("fixture timeout");}catch(InterruptedException failure){Thread.currentThread().interrupt();throw new IOException(failure);}return ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_GATE_STATE,ByteBuffer.allocate(4).putInt(-1).array());}public void close(){closes.incrementAndGet();}};
        try(var parent=new RatisRoutedClient(original,transport);var executor=Executors.newVirtualThreadPerTaskExecutor()){var view=parent.borrowedSnapshotCleanupView(original,fence);var query=executor.submit(()->view.snapshotGate(fence,group));assertTrue(entered.await(5,TimeUnit.SECONDS));var closing=executor.submit(()->{view.close();return null;});assertThrows(IOException.class,parent::close);assertEquals(0,closes.get());release.countDown();query.get(5,TimeUnit.SECONDS);closing.get(5,TimeUnit.SECONDS);assertEquals(0,closes.get());}
        assertEquals(1,closes.get());
    }
    @Test void readOnlyNamespaceSurvivesNewBootstrapSchemaButCleanupIsStillBoundToOriginalFence()throws Exception {
        byte[] nextSchema=new byte[32];nextSchema[0]=1;var latest=new RangeDirectory(cluster,database,domain,1,nextSchema,1,Set.of(column,UUID.randomUUID()),0,List.of(range));var fence=frozen();var calls=new AtomicInteger();
        var transport=new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor r,UUID c,byte[] key){throw new AssertionError();}public CommandResult command(RangeDescriptor r,DistributedCommand command){throw new AssertionError();}public byte[] transaction(RangeDescriptor r,byte[] bytes,boolean fresh){calls.incrementAndGet();return ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_GATE_STATE,ByteBuffer.allocate(4).putInt(-1).array());}};
        try(var old=new RatisRoutedClient(original,transport);var parent=new RatisRoutedClient(latest,transport);var view=parent.borrowedSnapshotCleanupView(original,fence)){assertArrayEquals(old.snapshotNamespaceDigest(),parent.snapshotNamespaceDigest());assertFalse(Arrays.equals(old.sessionIdentityDigest(),parent.sessionIdentityDigest()));assertNull(view.snapshotGate(fence,group));assertEquals(1,calls.get());assertThrows(IOException.class,()->view.snapshotGate(frozen(),group));assertEquals(1,calls.get());}
    }

    @Test void cancelledAsyncCohortObserverKeepsBorrowAndCallUntilActualReply()throws Exception {
        var actual=new CompletableFuture<byte[]>();var closes=new AtomicInteger();var fence=frozen();var started=new CountDownLatch(1);
        var transport=new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor r,UUID c,byte[] key){throw new AssertionError();}public CommandResult command(RangeDescriptor r,DistributedCommand command){throw new AssertionError();}public CompletableFuture<byte[]> snapshot(RangeDescriptor r,ReplicaDescriptor replica,byte[] request){assertEquals(owner,replica);assertEquals(RatisSnapshotCohortRpc.STATE,ScopedTransactionRpc.kind(request,RatisSnapshotCohortRpc.MAX_REQUEST_BYTES));return actual;}public void close(){closes.incrementAndGet();}};
        try(var parent=new RatisRoutedClient(original,transport);var executor=Executors.newVirtualThreadPerTaskExecutor()){var view=parent.borrowedSnapshotCleanupView(original,fence);var target=view.restoreSnapshotTarget(fence,range,owner,UUID.randomUUID());var observer=view.cohortQuery(target,RatisSnapshotCohortRpc.STATE,new byte[96]);assertFalse(observer.isDone());assertTrue(observer.cancel(false));assertFalse(actual.isCancelled());var closing=executor.submit(()->{started.countDown();view.close();return null;});assertTrue(started.await(5,TimeUnit.SECONDS));assertFalse(closing.isDone());assertThrows(IOException.class,parent::close);assertEquals(0,closes.get());actual.complete(ScopedTransactionRpc.frame(RatisSnapshotCohortRpc.STATE,new byte[0]));closing.get(5,TimeUnit.SECONDS);assertEquals(0,closes.get());}
        assertEquals(1,closes.get());
    }

}
