package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.reactivestreams.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisAtomicClientTest.*;

/** Common facade with real native DATA and separate exclusive bulk/point grants; controlled transport only. */
class RatisConnectionBatchesTest {
    final RatisPointFixtureBudget pointBudget=new RatisPointFixtureBudget();
    @org.junit.jupiter.api.AfterEach void closePointBudget()throws Exception{pointBudget.close();}
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static <T> T await(CompletableFuture<T> future) throws Exception { return future.get(10,TimeUnit.SECONDS); }
    static KVBatch batch() { return new KVBatch.KVBatchRef(List.of(key(2),key(3),key(4)),List.of(value(1),value(2),value(3))); }
    final class Fixture implements AutoCloseable {
        final RatisAtomicClientTest.Fixture f=new RatisAtomicClientTest.Fixture(root.resolve("native"),true);
        final Path connectionRoot=root.resolve("connection");
        final RatisNodePlan.Catalogue binding;
        final RatisRoutedClient routed;
        final List<DistributedCommand> writes=new CopyOnWriteArrayList<>();
        final List<SessionEpochLedger.Grant> grants=new ArrayList<>();
        final List<Path> windows=new ArrayList<>();
        final AtomicBoolean lose=new AtomicBoolean(); final AtomicLong sequence=new AtomicLong(102);
        volatile CountDownLatch entered,release;
        final RatisRocksDBConnection.GenerationFactory factory;
        Fixture() throws Exception {
            binding=new RatisNodePlan.Catalogue(f.outer.catalogue,1,new byte[32],f.outer.directoryColumn,f.outer.sessions,f.directory.ranges().getFirst().replicas());
            var schemas=new RatisSchemaCatalogue(f.catalogue.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
            assertEquals(CommandResult.Status.SUCCESS,schemas.execute(schemas.prepare(f.outer.operation(100),f.schema)).status());
            var catalogue=new RatisCatalogue(f.catalogue.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepare(f.outer.operation(101),null,f.directory)).status());
            var transport=f.transport();
            routed=new RatisRoutedClient(f.directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return transport.read(route,column,key); }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException { return transport.transaction(route,bytes,fresh); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException {
                    boolean data=command.kind()==DistributedCommand.Kind.DATA && !command.mutations().isEmpty(); if(data) writes.add(command);
                    var result=transport.command(route,command);
                    if(data && entered!=null) { entered.countDown(); try { if(!release.await(10,TimeUnit.SECONDS)) throw new IOException("Controlled DATA reply barrier timed out"); } catch(InterruptedException interrupted) { throw new IOException(interrupted); } }
                    if(data && lose.getAndSet(false)) throw new IOException("Lost applied common batch reply"); return result;
                }
            });
            factory=RatisRocksDBConnection.durableGenerationFactory(routed,f.scheduler,f.snapshots,f.homes,f.catalogue.epochs,f.catalogue.epochs.catalogue(f.outer.directoryColumn),header->f.catalogue.epochs.topologyCatalogue(f.outer.directoryColumn).readDefinition(header.definitionDigest()).sourceDirectory(),new RatisRocksDBAdapterTest.Allocations(f),new RatisRocksDBConnection.SessionFactory(List.of(routed),(path,schema,bindings)-> {var directory=bindings.directory(schema.domainId());
                Path window=path.resolve("window.bin"); if(Files.exists(window)) return RatisDurableSessionClient.reopen(window,routed);
                var previous=f.catalogue.epochs.read(); var next=previous.issue(UUID.randomUUID(),1000); var grant=next.grants().getLast();
                assertEquals(CommandResult.Status.SUCCESS,f.catalogue.epochs.execute(f.catalogue.epochs.prepare(f.outer.operation(sequence.getAndIncrement()),previous,next)).status());
                for(var group:f.groups) { var control=f.catalogue.epochs.prepareSessionBinding(directory,group,new OperationId(f.admin,next.nextEpoch(),sequence.getAndIncrement()),grant,32,-1); assertEquals(CommandResult.Status.SUCCESS,routed.execute(routed.prepareSessionControl(control)).status()); }
                grants.add(grant); windows.add(window); return RatisDurableSessionClient.create(window,routed,next,grant,RatisSessionClient.Limits.defaults());
            }),List.of(),pointBudget.shared);
        }
        RatisRocksDBConnection open(int capacity) throws Exception { var replicas=new RatisSchemaReplicaClient(1); return new RatisRocksDBConnection(URI.create("ratis://controlled/batches"),connectionRoot,java.util.Map.of(f.schema.domainId(),routed),(name,shape)->f.schema.domainId(),f.scheduler,binding,f.catalogue.client,replicas,null,null,factory,new RatisRocksDBConnection.Limits(8,64,8,128L*1024*1024,capacity),List.of(replicas), pointBudget.shared); }
        public void close() throws Exception { routed.close(); f.close(); }
    }
    static RemoteMutationToken prepare(RemoteMutationAsyncAPI api)throws Exception {
        var owner=await(api.owners(71,RequestContext.batch())).stream().filter(candidate->candidate.kind()==RemoteMutationToken.Kind.BULK).findFirst().orElseThrow();UUID request=UUID.randomUUID();long sequence=owner.nextRequestSequence();return await(api.prepareBulk(owner,request,sequence,71,false,RequestContext.batch()));
    }
    static void acknowledge(RemoteMutationAsyncAPI api,RemoteMutationToken token,RemoteMutationStatus status)throws Exception {
        assertEquals(RemoteMutationStatus.State.TERMINAL,status.state());assertEquals(CommandResult.Status.SUCCESS,status.outcome());await(api.acknowledge(token,status.resultDigest(),RequestContext.batch()));
    }
    /** Caller owns subscription/demand; the remote API receives only identified copied batches. */
    static final class CallerStream implements Subscriber<KVBatch> {
        final RemoteMutationAsyncAPI api;final RemoteMutationToken token;final CompletableFuture<RemoteMutationStatus> result=new CompletableFuture<>(){@Override public boolean cancel(boolean interrupt){stop();return super.cancel(false);}};final AtomicBoolean finishing=new AtomicBoolean(),stopped=new AtomicBoolean();
        volatile Subscription source;volatile CompletableFuture<RemoteMutationStatus> pending;long ordinal;volatile boolean complete;
        CallerStream(RemoteMutationAsyncAPI api,RemoteMutationToken token,Publisher<KVBatch> publisher){this.api=api;this.token=token;publisher.subscribe(this);}
        public void onSubscribe(Subscription subscription){if(source!=null){subscription.cancel();return;}source=subscription;subscription.request(1);}
        public void onNext(KVBatch batch){if(result.isDone())return;var copied=api.appendBatch(token,ordinal++,batch,RemoteMutationInputs.batch(71,false,batch),RequestContext.batch());pending=copied;copied.whenComplete((status,failure)->{if(result.isDone())return;if(failure!=null){fail(failure);return;}pending=null;if(complete)finish();else source.request(1);});}
        void stop(){if(!stopped.compareAndSet(false,true))return;if(source!=null)source.cancel();var copied=pending;if(copied!=null)copied.cancel(false);}
        void fail(Throwable failure){stop();result.completeExceptionally(failure);}
        public void onError(Throwable failure){fail(failure);}
        public void onComplete(){complete=true;if(pending==null)finish();}
        void finish(){if(result.isDone() || !finishing.compareAndSet(false,true))return;var finishing=api.finishInput(token,RequestContext.batch());pending=finishing;finishing.whenComplete((status,failure)->{if(failure==null)result.complete(status);else fail(failure);});}
    }
    @Test void identifiedWriteBatchUsesOneNativeCommandAndSeparateExclusiveGrants() throws Exception {
        try(var f=new Fixture(); var connection=f.open(8)) {
            assertEquals(2,f.grants.size());assertNotEquals(f.grants.get(0).sessionId(),f.grants.get(1).sessionId());assertNotEquals(f.windows.get(0),f.windows.get(1));
            assertEquals(4096,connection.batchLimits().maximumInputItems());assertEquals(8*1024*1024,connection.batchLimits().maximumEncodedInputBytes());
            var remote=connection.getRemoteMutationApi();var token=prepare(remote);var caller=new CallerStream(remote,token,RatisRocksDBBatchesTest.one(batch()));var terminal=await(caller.result);assertEquals(3,terminal.appliedMutations());acknowledge(remote,token,terminal);
            assertEquals(1,f.writes.size());assertEquals(3,f.writes.getFirst().mutations().size());assertEquals(List.of(Mutation.Kind.PUT,Mutation.Kind.PUT,Mutation.Kind.PUT),f.writes.getFirst().mutations().stream().map(Mutation::kind).toList());
            for(int i=2;i<=4;i++)assertArrayEquals(new byte[]{(byte)(i-1)},await(connection.getAsyncApi(RequestContext.batch()).getAsync(0,71,key(i),RequestType.<Buf>current())).toByteArray());
            var recovery=await(connection.batchRecoveryStatus(RequestContext.batch())).getFirst();assertEquals(RatisRocksDBBatches.RecoveryState.NONE,recovery.state());assertTrue(recovery.progress().sourceComplete());assertEquals(3,recovery.progress().acknowledgedMutations());
            var subscribed=new AtomicInteger();Publisher<KVBatch> forbidden=ignored->subscribed.incrementAndGet();
            for(var mode:PutBatchMode.values())assertThrows(ExecutionException.class,()->await(connection.getAsyncApi(RequestContext.batch()).putBatchAsync(71,forbidden,mode)));
            for(var mode:MergeBatchMode.values())assertThrows(ExecutionException.class,()->await(connection.getAsyncApi(RequestContext.batch()).mergeBatchAsync(71,forbidden,mode)));
            assertEquals(0,subscribed.get());assertEquals(1,f.writes.size());assertEquals(0,f.f.catalogue.closes.get());
        }
    }
    @Test void identifiedLostReplyColdReopenRequiresExactTokenAndNeverReplaysUnseenTail() throws Exception {
        try(var f=new Fixture()) {
            RemoteMutationToken token;
            try(var connection=f.open(8)) {
                var remote=connection.getRemoteMutationApi();token=prepare(remote);f.lose.set(true);var input=batch();assertThrows(ExecutionException.class,()->await(remote.appendBatch(token,0,input,RemoteMutationInputs.batch(71,false,input),RequestContext.batch())));
                var original=await(remote.status(token,RequestContext.batch()));assertEquals(RemoteMutationStatus.State.PENDING,original.state());assertEquals(1,f.writes.size());
            }
            try(var connection=f.open(8)) {
                var remote=connection.getRemoteMutationApi();var changed=new RemoteMutationToken(token.clusterId(),token.databaseId(),token.domainId(),token.ownerId(),UUID.randomUUID(),token.ownerEpoch(),token.requestSequence(),token.kind(),token.schemaDigest(),token.directoryDigest(),token.inputDigest());
                assertThrows(ExecutionException.class,()->await(remote.resume(changed,RequestContext.batch())));var foreign=new RemoteMutationToken(token.clusterId(),token.databaseId(),token.domainId(),token.ownerId(),token.requestId(),token.ownerEpoch(),token.requestSequence(),token.kind(),new byte[32],token.directoryDigest(),token.inputDigest());assertThrows(ExecutionException.class,()->await(remote.resume(foreign,RequestContext.batch())));assertEquals(1,f.writes.size());var untouched=await(remote.status(token,RequestContext.batch()));assertEquals(RemoteMutationStatus.State.PENDING,untouched.state());assertEquals(0,untouched.appliedMutations(),"Rejected token must not drain the original copied chunk");assertEquals(1,untouched.copiedBatches());
                assertThrows(ExecutionException.class,()->await(connection.getAsyncApi(RequestContext.batch()).putAsync(0,71,key(5),value(7),RequestType.none())));
                var result=await(remote.resume(token,RequestContext.batch()));assertEquals(RemoteMutationStatus.State.PENDING,result.state());assertEquals(3,result.appliedMutations());assertEquals(1,result.copiedBatches());assertEquals(2,f.writes.size());assertArrayEquals(DistributedCommandCodec.encode(f.writes.getFirst()),DistributedCommandCodec.encode(f.writes.getLast()));
                var finished=await(remote.finishInput(token,RequestContext.batch()));acknowledge(remote,token,finished);
                var next=prepare(remote);var input=new KVBatch.KVBatchRef(List.of(key(5)),List.of(value(7)));await(remote.appendBatch(next,0,input,RemoteMutationInputs.batch(71,false,input),RequestContext.batch()));acknowledge(remote,next,await(remote.finishInput(next,RequestContext.batch())));
                assertEquals(3,f.writes.size());assertArrayEquals(new byte[]{7},await(connection.getAsyncApi(RequestContext.batch()).getAsync(0,71,key(5),RequestType.<Buf>current())).toByteArray());assertEquals(2,f.grants.size(),"Reopen must retain each original grant");
            }
        }
    }
    @Test void canceledCallerBatchKeepsCapacityAndCloseOwnershipThroughActualReply() throws Exception {
        try(var f=new Fixture()) {
            var connection=f.open(1);var remote=connection.getRemoteMutationApi();var token=prepare(remote);f.entered=new CountDownLatch(1);f.release=new CountDownLatch(1);var demanded=new AtomicInteger();var canceled=new AtomicBoolean();
            Publisher<KVBatch> source=subscriber->subscriber.onSubscribe(new Subscription(){public void request(long n){assertEquals(1,n);demanded.incrementAndGet();subscriber.onNext(batch());}public void cancel(){canceled.set(true);}});
            try {
                var caller=new CallerStream(remote,token,source);assertTrue(f.entered.await(5,TimeUnit.SECONDS));assertTrue(caller.result.cancel(false));assertTrue(canceled.get());
                var rejected=assertThrows(ExecutionException.class,()->await(connection.getAsyncApi(RequestContext.batch()).getAsync(0,71,key(2),RequestType.<Buf>current())));assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,assertInstanceOf(RocksDBException.class,rejected.getCause()).getErrorUniqueId());
                assertThrows(IOException.class,connection::close);f.release.countDown();var drained=new CompletableFuture<Void>();f.f.scheduler.executor(RequestContext.batch(),OperationFamily.MUTATION).execute(()->drained.complete(null));await(drained);connection.close();assertEquals(1,demanded.get());
            }finally{f.release.countDown();connection.close();}
            f.entered=null;
            try(var reopened=f.open(1)){var resumed=reopened.getRemoteMutationApi();var saved=await(resumed.resume(token,RequestContext.batch()));assertEquals(3,saved.appliedMutations());assertEquals(RemoteMutationStatus.State.PENDING,saved.state());assertEquals(1,f.writes.size());acknowledge(resumed,token,await(resumed.finishInput(token,RequestContext.batch())));assertEquals(1,demanded.get());}
        }
    }
}
