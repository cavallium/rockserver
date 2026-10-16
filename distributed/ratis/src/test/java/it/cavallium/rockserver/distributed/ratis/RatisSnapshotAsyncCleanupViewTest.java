package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Frozen cleanup capability guards and actual future lifetime; no native success claim. */
class RatisSnapshotAsyncCleanupViewTest {
    @Test void originalReleaseForwardsActualFuturesButDeniedWorkNeverReachesBorrowedTransport()throws Exception{
        UUID cluster=UUID.randomUUID(),database=UUID.randomUUID(),domain=UUID.randomUUID(),column=UUID.randomUUID(),group=UUID.randomUUID();
        var owner=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"local","localhost:19010",ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(UUID.randomUUID(),group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(owner));
        var original=new RangeDirectory(cluster,database,domain,0,new byte[32],0,Set.of(column),0,List.of(range));
        var session=UUID.randomUUID();var fence=RatisSnapshotPlan.create(List.of(original),new TransactionScope(List.of(TransactionScope.Span.range(column,null,null))),UUID.randomUUID(),new OperationId(session,1,0),UUID.randomUUID(),1,new OperationId(session,1,1)).fence();
        var applied=new CompletableFuture<byte[]>();var calls=new AtomicInteger();var appends=new AtomicInteger();var closes=new AtomicInteger();
        var transport=new RatisRoutedClient.Transport(){
            public byte[] read(RangeDescriptor route,UUID col,byte[] key){throw new AssertionError("Sync USER read");}
            public CommandResult command(RangeDescriptor route,DistributedCommand command){throw new AssertionError("Sync USER write");}
            public CompletableFuture<byte[]> readAsync(RangeDescriptor route,UUID col,byte[] key){throw new AssertionError("Async USER read");}
            public CompletableFuture<CommandResult> commandAsync(RangeDescriptor route,DistributedCommand command){throw new AssertionError("Async USER write");}
            public CompletableFuture<byte[]> transactionAsync(RangeDescriptor route,byte[] request,boolean fresh){
                assertEquals(range,route);assertTrue(fresh);calls.incrementAndGet();var packet=ScopedTransactionRpc.parse(request,ScopedTransactionRpc.MAX_REQUEST_BYTES);
                if(packet.kind()==ScopedTransactionRpc.SNAPSHOT_GATE_STATE)return CompletableFuture.completedFuture(ScopedTransactionRpc.frame(packet.kind(),ByteBuffer.allocate(4).putInt(-1).array()));
                assertEquals(ScopedTransactionRpc.SNAPSHOT_GATE_PREPARE,packet.kind());var record=RatisSnapshotRecord.decode(packet.body());assertTrue(record.isRelease());
                var reply=ByteBuffer.allocate(41).putInt(32).put(record.digest()).put((byte)1).putInt(-1).array();return CompletableFuture.completedFuture(ScopedTransactionRpc.frame(packet.kind(),reply));
            }
            public CompletableFuture<byte[]> participantAsync(RangeDescriptor route,byte[] record){assertEquals(range,route);assertTrue(RatisSnapshotRecord.decode(record).isRelease());appends.incrementAndGet();return applied;}
            public void close(){closes.incrementAndGet();}
        };
        try(var parent=new RatisRoutedClient(original,transport);var executor=Executors.newVirtualThreadPerTaskExecutor()){
            var replacement=new RangeDescriptor(UUID.randomUUID(),UUID.randomUUID(),2,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(owner));
            parent.refresh(new RangeDirectory(cluster,database,domain,0,new byte[32],1,Set.of(column),0,List.of(replacement)));
            var view=parent.borrowedSnapshotCleanupView(original,fence);
            try{
                assertNull(view.snapshotGateAsync(fence,group).get(5,TimeUnit.SECONDS));assertEquals(1,calls.get());
                var other=RatisSnapshotPlan.create(List.of(original),new TransactionScope(List.of(TransactionScope.Span.range(column,null,null))),UUID.randomUUID(),new OperationId(session,1,0),UUID.randomUUID(),1,new OperationId(session,1,1)).fence();
                assertThrows(ExecutionException.class,()->view.snapshotGateAsync(other,group).get(5,TimeUnit.SECONDS));
                assertThrows(ExecutionException.class,()->view.readWithRouteAsync(column,new byte[]{1}).get(5,TimeUnit.SECONDS));
                byte[] newerSchema=new byte[32];newerSchema[0]=1;
                var unqualified=new DistributedCommand(group,1,newerSchema,new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{2})));
                assertThrows(IllegalArgumentException.class,()->view.restoreSessionCommand(unqualified,range));
                var command=new DistributedCommand(group,1,new byte[32],new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{2})));
                assertThrows(ExecutionException.class,()->view.executeAsync(view.restoreSessionCommand(command,range)).get(5,TimeUnit.SECONDS));
                assertThrows(ExecutionException.class,()->view.executeSnapshotGateAsync(view.restoreSnapshotGate(RatisSnapshotRecord.install(fence,group),range)).get(5,TimeUnit.SECONDS));
                assertEquals(1,calls.get());assertEquals(0,appends.get());
                var release=view.restoreSnapshotGate(RatisSnapshotRecord.release(SnapshotFenceStatus.begin(fence).abort(),group),range);
                var observer=view.executeSnapshotGateAsync(release);assertEquals(2,calls.get());assertEquals(1,appends.get());assertFalse(observer.isDone());
                observer.cancel(false);assertFalse(applied.isCancelled());var started=new CountDownLatch(1);var closing=executor.submit(()->{started.countDown();view.close();return null;});assertTrue(started.await(5,TimeUnit.SECONDS));
                assertFalse(closing.isDone());assertThrows(IOException.class,parent::close);assertEquals(0,closes.get());
                applied.complete(new RatisSnapshotRecord.Result(new ApplyPosition(1,1)).encode());closing.get(5,TimeUnit.SECONDS);assertEquals(0,closes.get());
            }finally{applied.complete(new RatisSnapshotRecord.Result(new ApplyPosition(1,1)).encode());view.close();}
        }
        assertEquals(1,closes.get());
    }
}
