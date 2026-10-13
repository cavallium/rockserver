package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native HOME/gates, controlled asynchronous reply ownership; no socket or quorum. */
class RatisSnapshotAsyncMetadataTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void pressuredBatchYieldsWhileOriginalHomeReplyAndJournalOwnerRemainActive()throws Exception {
        var helper=new RatisSnapshotCoordinatorTest();helper.root=root;
        var scheduler=RWScheduler.forTesting(1,1,1,4,1,"snapshot-async");
        try(var f=helper.new Fixture("async-metadata")){
            var held=new CompletableFuture<byte[]>();var arrived=new CountDownLatch(1);var first=new AtomicBoolean(true);
            var transport=new RatisRoutedClient.Transport(){
                public byte[] read(RangeDescriptor route,UUID column,byte[] key)throws IOException{return f.home.wire.read(route,column,key);}
                public CommandResult command(RangeDescriptor route,DistributedCommand command)throws IOException{return f.home.wire.command(route,command);}
                public CompletableFuture<byte[]> readAsync(RangeDescriptor route,UUID column,byte[] key)throws IOException{
                    var value=f.home.wire.read(route,column,key);
                    if(first.compareAndSet(true,false)){arrived.countDown();return held.thenApply(ignored->value);}return CompletableFuture.completedFuture(value);
                }
                public CompletableFuture<CommandResult> commandAsync(RangeDescriptor route,DistributedCommand command)throws IOException{return CompletableFuture.completedFuture(f.home.wire.command(route,command));}
                public CompletableFuture<byte[]> transactionAsync(RangeDescriptor route,byte[] bytes,boolean fresh)throws IOException{return f.home.f.machine.queryLinearizable(org.apache.ratis.protocol.Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(bytes))).thenApply(reply->reply.getContent().toByteArray());}
            };
            try(var routed=new RatisRoutedClient(f.home.routed.directory(),transport)){
                var homes=new RatisSnapshotFences(routed,f.home.f.column);var initial=SnapshotFenceStatus.begin(f.fence);
                try(var journal=RatisDurableSnapshot.create(root.resolve("async.journal"),homes,f.data,initial,homes.prepare(helper.control(20),null,initial),f.directory.ranges());
                    var coordinator=new RatisSnapshotCoordinator(journal,homes,f.data,last->new OperationId(last.sessionId(),last.epoch(),last.sequence()+1),UUID::randomUUID,Duration.ofSeconds(10))){
                    scheduler.setStoragePressure(true);
                    var observed=new CompletableFuture<RatisSnapshotCoordinator.Step>();
                    var actual=new java.util.concurrent.atomic.AtomicReference<CompletableFuture<RatisSnapshotCoordinator.Step>>();
                    scheduler.scheduler(RequestContext.batch(),OperationFamily.METADATA).schedule(()->{var work=coordinator.stepAsync(RatisSnapshotCoordinator.StopAt.GATES_READY);actual.set(work);work.whenComplete((value,failure)->{if(failure==null)observed.complete(value);else observed.completeExceptionally(failure);});});
                    assertTrue(arrived.await(10,TimeUnit.SECONDS));assertFalse(observed.isDone());
                    assertThrows(IOException.class,coordinator::close);assertThrows(IOException.class,journal::close);
                    var unrelated=new CompletableFuture<Void>();scheduler.scheduler(RequestContext.batch(),OperationFamily.METADATA).schedule(()->unrelated.complete(null));unrelated.get(10,TimeUnit.SECONDS);
                    observed.cancel(false);assertThrows(IOException.class,journal::close);held.complete(new byte[0]);
                    // A canceled observer cannot cancel native/journal completion. The next quantum
                    // is admitted only after the actual HOME result has been forced.
                    actual.get().get(10,TimeUnit.SECONDS);
                    boolean ready=false;for(int n=0;n<32&&!ready;n++)ready=coordinator.stepAsync(RatisSnapshotCoordinator.StopAt.GATES_READY).get(10,TimeUnit.SECONDS).action()==RatisSnapshotCoordinator.Action.GATES_READY;
                    assertTrue(ready);journal.requestRelease();boolean released=false;for(int n=0;n<32&&!released;n++)released=coordinator.stepAsync(RatisSnapshotCoordinator.StopAt.RELEASED).get(10,TimeUnit.SECONDS).complete();assertTrue(released);
                    var saved=journal.snapshot();journal.consume(new OperationId(saved.lastHome().sessionId(),saved.lastHome().epoch(),saved.lastHome().sequence()+1));assertEquals(CommandResult.Status.SUCCESS,journal.executeHomeAsync().get(10,TimeUnit.SECONDS).status());assertTrue(journal.refreshHomeAsync().get(10,TimeUnit.SECONDS).consumed());
                }
            }
        }finally{scheduler.dispose();}
    }
}
