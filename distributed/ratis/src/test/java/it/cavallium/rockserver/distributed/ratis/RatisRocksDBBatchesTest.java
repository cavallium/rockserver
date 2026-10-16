package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.reactivestreams.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Real native groups with controlled synchronous dispatch; no server, socket or quorum assertions. */
class RatisRocksDBBatchesTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final RatisAtomicClientTest.Fixture f=new RatisAtomicClientTest.Fixture(root,true);
        final RatisRoutedClient routed; final RatisDurableSessionClient session; final RatisRocksDBBatches batches;
        final RatisRequestBudget budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults());
        final List<DistributedCommand> writes=new CopyOnWriteArrayList<>(); final AtomicBoolean lose=new AtomicBoolean(); volatile CountDownLatch entered,release; final Path window=root.resolve("bulk-window.bin"),journal=root.resolve("bulk-request.bin");
        Fixture() throws Exception {
            var ledger=f.ledger.issue(new UUID(280,1),1000); var grant=ledger.grants().getLast();
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.epochs.execute(f.catalogue.epochs.prepare(f.outer.operation(2),f.ledger,ledger)).status()); int n=0;
            for(var group:f.groups) { var binding=f.catalogue.epochs.prepareSessionBinding(f.directory,group,new OperationId(f.admin,2,n++),grant,32,-1); assertEquals(CommandResult.Status.SUCCESS,f.data.execute(f.data.prepareSessionControl(binding)).status()); }
            var transport=f.transport(); routed=new RatisRoutedClient(f.directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return transport.read(route,column,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { writes.add(command); var result=transport.command(route,command); if(entered!=null) { entered.countDown(); try { if(!release.await(10,TimeUnit.SECONDS)) throw new IOException("Controlled bulk RPC timed out"); } catch(InterruptedException interrupted) { throw new IOException(interrupted); } } if(lose.getAndSet(false)) throw new IOException("Lost applied batch reply"); return result; }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException { return transport.transaction(route,bytes,fresh); }
            });
            session=RatisDurableSessionClient.create(window,routed,ledger,grant,RatisSessionClient.Limits.defaults()); batches=open(session);
        }
        RatisRocksDBBatches open(RatisDurableSessionClient selected)throws IOException{return openShared(f.schema,f.directory,routed,selected,f.scheduler,journal,budget);}
        public void close() throws Exception { batches.close(); session.close(); routed.close(); f.close();budget.close(); }
    }
    static RatisRocksDBBatches openShared(DistributedSchema schema,RangeDirectory directory,RatisRoutedClient routed,RatisDurableSessionClient session,it.cavallium.rockserver.core.impl.RWScheduler scheduler,Path journal,RatisRequestBudget budget)throws IOException {
        var owner=budget.owner(RemoteMutationToken.Kind.BULK,RangeDirectoryCodec.encodedSize(routed.directory(schema)),RatisRequestBudget.files(journal,journal.resolveSibling(journal.getFileName()+".pending")));
        try{return new RatisRocksDBBatches(schema,directory,routed,session,scheduler,journal,RatisRocksDBBatches.SessionOwnership.BORROWED,budget,owner);}catch(IOException|RuntimeException|Error failure){owner.close();throw failure;}
    }
    static Publisher<KVBatch> one(KVBatch batch) {
        return subscriber -> subscriber.onSubscribe(new Subscription() {
            int count; boolean stopped;
            public synchronized void request(long n) { assertEquals(1,n); if(stopped) return; if(count++==0) subscriber.onNext(batch); else subscriber.onComplete(); }
            public synchronized void cancel() { stopped=true; }
        });
    }
    static RemoteMutationToken prepare(RatisRocksDBBatches batches)throws IOException {var owner=batches.owner();return batches.prepare(owner,UUID.randomUUID(),owner.nextRequestSequence(),71,false);}
    /** Caller owns subscription, demand, completion and cancellation; the backend sees only identified copied input. */
    static final class CallerStream implements Subscriber<KVBatch> {
        final RatisRocksDBBatches batches;final RemoteMutationToken token;final RequestContext context;final CompletableFuture<RatisRocksDBBatches.Progress> result=new CompletableFuture<>(){public boolean completeExceptionally(Throwable failure){cancelUpstream();return super.completeExceptionally(failure);}public boolean cancel(boolean interrupt){cancelUpstream();return super.cancel(interrupt);}};
        final java.util.concurrent.atomic.AtomicBoolean inFlight=new java.util.concurrent.atomic.AtomicBoolean(),eof=new java.util.concurrent.atomic.AtomicBoolean(),finishing=new java.util.concurrent.atomic.AtomicBoolean(),upstreamCancelled=new java.util.concurrent.atomic.AtomicBoolean();volatile Subscription subscription;volatile CompletableFuture<RatisRocksDBBatches.Progress> pending;long ordinal;
        CallerStream(RatisRocksDBBatches batches,RemoteMutationToken token,Publisher<KVBatch> source,RequestContext context){this.batches=batches;this.token=token;this.context=context;result.whenComplete((value,error) -> {if(error!=null){var observer=pending;if(observer!=null)observer.cancel(false);}});source.subscribe(this);}
        void cancelUpstream(){var source=subscription;if(source!=null && upstreamCancelled.compareAndSet(false,true))source.cancel();}
        public void onSubscribe(Subscription source){subscription=source;if(result.isDone())cancelUpstream();else source.request(1);}
        public void onNext(KVBatch batch){if(result.isDone())return;if(!inFlight.compareAndSet(false,true)){onError(new IllegalStateException("Caller requested overlapping copied inputs"));return;}pending=batches.append(token,ordinal,batch,RemoteMutationInputs.batch(71,false,batch),context);pending.whenComplete((progress,error) -> {inFlight.set(false);if(result.isDone())return;if(error!=null){onError(error);return;}ordinal++;if(eof.get())finish();else subscription.request(1);});}
        public void onError(Throwable failure){result.completeExceptionally(failure);}
        public void onComplete(){eof.set(true);if(!inFlight.get())finish();}
        void finish(){if(result.isDone() || !finishing.compareAndSet(false,true))return;pending=batches.finishInput(token,context);pending.whenComplete((progress,error) -> {if(error==null)result.complete(progress);else result.completeExceptionally(error);});}
    }
    @Test void interleavedGroupsBecomeWholeNativeChunksWithOriginalWithinGroupOrder() throws Exception {
        try(var x=new Fixture()) {
            var keys=List.of(RatisAtomicClientTest.key(2),RatisAtomicClientTest.key(200),RatisAtomicClientTest.key(3),RatisAtomicClientTest.key(201),RatisAtomicClientTest.key(2));var values=List.of(RatisAtomicClientTest.value(1),RatisAtomicClientTest.value(2),RatisAtomicClientTest.value(3),RatisAtomicClientTest.value(4),RatisAtomicClientTest.value(5));var token=prepare(x.batches);var batch=new KVBatch.KVBatchRef(keys,values);
            var observer=x.batches.append(token,0,batch,RemoteMutationInputs.batch(71,false,batch),RequestContext.batch());observer.get(10,TimeUnit.SECONDS);x.batches.actualCompletion(observer).get(10,TimeUnit.SECONDS);assertEquals(2,x.writes.size());assertEquals(List.of(3,2),x.writes.stream().map(c -> c.mutations().size()).toList());assertEquals(List.of((byte)2,(byte)3,(byte)2),x.writes.getFirst().mutations().stream().map(m -> m.key()[0]).toList());assertArrayEquals(new byte[]{5},x.routed.read(x.f.schema.column(71).columnId(),new byte[]{2}));
            assertFalse(x.batches.progress().sourceComplete());var progress=x.batches.finishInput(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);assertTrue(progress.sourceComplete());assertEquals(2,progress.acknowledgedChunks());assertEquals(5,progress.acknowledgedMutations());assertEquals(RatisRocksDBBatches.RecoveryState.COMPLETE,x.batches.recoveryState());x.batches.acknowledgeResult(token,x.batches.status(token).resultDigest());assertEquals(RatisRocksDBBatches.RecoveryState.NONE,x.batches.recoveryState());
        }
    }
    @Test void lostAppliedReplyReopenUsesOriginalIdAndOnlyCopiedInputWithoutInventingEof() throws Exception {
        try(var x=new Fixture()) {
            var token=prepare(x.batches);x.lose.set(true);var batch=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(2),RatisAtomicClientTest.key(3)),List.of(RatisAtomicClientTest.value(1),RatisAtomicClientTest.value(2)));var observer=x.batches.append(token,0,batch,RemoteMutationInputs.batch(71,false,batch),RequestContext.batch());assertThrows(ExecutionException.class,() -> observer.get(10,TimeUnit.SECONDS));x.batches.actualCompletion(observer).get(10,TimeUnit.SECONDS);var original=x.writes.getFirst().operationId();assertEquals(1,x.writes.size());x.batches.close();x.session.close();
            try(var session=RatisDurableSessionClient.reopen(x.window,x.routed);var batches=x.open(session)) {
                var progress=batches.resumeRetained(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);assertEquals(2,progress.acknowledgedMutations());assertFalse(progress.sourceComplete());assertEquals(original,x.writes.getLast().operationId());assertArrayEquals(DistributedCommandCodec.encode(x.writes.getFirst()),DistributedCommandCodec.encode(x.writes.getLast()));assertEquals(RatisRocksDBBatches.RecoveryState.PENDING,batches.recoveryState());batches.finishInput(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);batches.acknowledgeResult(token,batches.status(token).resultDigest());
            }
        }
    }
    @Test void tokenlessModesAndWrongProfileRejectBeforeSubscriptionOrData() throws Exception {
        try(var x=new Fixture();var transactions=RatisRocksDBAdapterTest.client(x.f,x.f.data);var adapter=new RatisRocksDBAdapter(RatisTransactionBindings.of(x.f.schema,x.f.directory,(transactions).bindings().domain((x.f.schema).domainId()).client()),transactions,new RatisRocksDBAdapterTest.Allocations(x.f),null,null,new java.util.concurrent.atomic.AtomicLong()::incrementAndGet,x.batches)) {
            var subscribed=new AtomicBoolean();Publisher<KVBatch> source=ignored -> subscribed.set(true);var api=adapter.api(RequestContext.batch());assertThrows(ExecutionException.class,() -> api.putBatchAsync(71,source,PutBatchMode.WRITE_BATCH_NO_WAL).get());assertThrows(ExecutionException.class,() -> api.mergeBatchAsync(71,source,MergeBatchMode.MERGE_SST_INGESTION).get());assertThrows(ExecutionException.class,() -> api.putBatchAsync(71,source,PutBatchMode.WRITE_BATCH).get());assertFalse(subscribed.get());assertNull(x.batches.progress());
            var token=prepare(x.batches);var batch=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(2)),List.of(RatisAtomicClientTest.value(9)));assertThrows(ExecutionException.class,() -> x.batches.append(token,0,batch,RemoteMutationInputs.batch(71,false,batch),RequestContext.latency(java.time.Duration.ofSeconds(1))).get());assertTrue(x.writes.isEmpty());assertEquals(0,x.batches.progress().copiedBatches());
        }
    }
    @Test void mutableInputIsCopiedBeforeReturningToCallerAndDemandIsOne() throws Exception {
        try(var x=new Fixture()) {
            var token=prepare(x.batches);byte[] key={2},value={7};var keys=new ArrayList<>(List.of(new Keys(it.cavallium.buffer.Buf.wrap(key))));var values=new ArrayList<>(List.of(it.cavallium.buffer.Buf.wrap(value)));var demands=new java.util.concurrent.atomic.AtomicInteger();
            Publisher<KVBatch> publisher=subscriber -> subscriber.onSubscribe(new Subscription(){boolean sent;public void request(long n){assertEquals(1,n);demands.incrementAndGet();if(sent)return;sent=true;subscriber.onNext(new KVBatch.KVBatchRef(keys,values));key[0]=99;value[0]=99;keys.clear();values.clear();subscriber.onComplete();}public void cancel(){}});
            var caller=new CallerStream(x.batches,token,publisher,RequestContext.batch());caller.result.get(10,TimeUnit.SECONDS);assertEquals(1,demands.get());assertEquals(1,x.writes.size());assertArrayEquals(new byte[]{2},x.writes.getFirst().mutations().getFirst().key());assertArrayEquals(new byte[]{7},x.writes.getFirst().mutations().getFirst().value());assertTrue(x.batches.progress().sourceComplete());x.batches.acknowledgeResult(token,x.batches.status(token).resultDigest());
        }
    }
    @Test void cancelDuringActualRpcRetainsOwnershipAndCopiedInputWithoutRequestingTail() throws Exception {
        try(var x=new Fixture()) {
            var token=prepare(x.batches);x.entered=new CountDownLatch(1);x.release=new CountDownLatch(1);var demands=new java.util.concurrent.atomic.AtomicInteger();var canceled=new AtomicBoolean();var subscribed=new java.util.concurrent.atomic.AtomicInteger();var batch=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(2),RatisAtomicClientTest.key(3)),List.of(RatisAtomicClientTest.value(1),RatisAtomicClientTest.value(2)));
            Publisher<KVBatch> publisher=subscriber -> {subscribed.incrementAndGet();subscriber.onSubscribe(new Subscription(){public void request(long n){assertEquals(1,n);demands.incrementAndGet();subscriber.onNext(batch);}public void cancel(){canceled.set(true);}});};var caller=new CallerStream(x.batches,token,publisher,RequestContext.batch());assertTrue(x.entered.await(10,TimeUnit.SECONDS));var actual=x.batches.actualCompletion(caller.pending);assertEquals(2,x.batches.progress().retainedChunks()==1?x.writes.getFirst().mutations().size():-1);assertEquals(RatisRocksDBBatches.RecoveryState.ACTIVE,x.batches.recoveryState());caller.result.cancel(false);assertTrue(canceled.get());assertFalse(actual.isDone());assertThrows(IOException.class,x.batches::close);assertThrows(IOException.class,x.session::close);x.release.countDown();actual.get(10,TimeUnit.SECONDS);assertEquals(1,demands.get());assertEquals(2,x.batches.progress().acknowledgedMutations());assertFalse(x.batches.progress().sourceComplete());var saved=x.batches.resumeRetained(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);assertEquals(2,saved.acknowledgedMutations());assertFalse(saved.sourceComplete());assertEquals(1,x.writes.size());assertEquals(1,subscribed.get());x.batches.finishInput(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);x.batches.acknowledgeResult(token,x.batches.status(token).resultDigest());
        }
    }
    @Test void deadlineExpiresObserverButKeepsBlockedRpcOwnershipUntilActualReturn() throws Exception {
        try(var x=new Fixture()) {
            var token=prepare(x.batches);x.entered=new CountDownLatch(1);x.release=new CountDownLatch(1);var canceled=new AtomicBoolean();var demands=new java.util.concurrent.atomic.AtomicInteger();var batch=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(2),RatisAtomicClientTest.key(3)),List.of(RatisAtomicClientTest.value(1),RatisAtomicClientTest.value(2)));
            Publisher<KVBatch> publisher=subscriber -> subscriber.onSubscribe(new Subscription(){public void request(long n){assertEquals(1,n);demands.incrementAndGet();subscriber.onNext(batch);}public void cancel(){canceled.set(true);}});var caller=new CallerStream(x.batches,token,publisher,RequestContext.batch(java.time.Duration.ofMillis(500)));assertTrue(x.entered.await(5,TimeUnit.SECONDS));var actual=x.batches.actualCompletion(caller.pending);assertThrows(ExecutionException.class,() -> caller.result.get(5,TimeUnit.SECONDS));assertTrue(canceled.get());assertFalse(actual.isDone());assertThrows(IOException.class,x.batches::close);assertThrows(IOException.class,x.session::close);assertEquals(RatisRocksDBBatches.RecoveryState.ACTIVE,x.batches.recoveryState());x.release.countDown();actual.get(5,TimeUnit.SECONDS);assertEquals(1,demands.get());assertEquals(2,x.batches.progress().acknowledgedMutations());assertFalse(x.batches.progress().sourceComplete());var progress=x.batches.resumeRetained(token,RequestContext.batch()).get(5,TimeUnit.SECONDS);assertEquals(2,progress.acknowledgedMutations());assertEquals(1,x.writes.size());assertFalse(progress.sourceComplete());x.batches.finishInput(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);x.batches.acknowledgeResult(token,x.batches.status(token).resultDigest());
        }
    }
    @Test void acknowledgementGapReopenDoesNotSubmitAnAlreadyConsumedChunk() throws Exception {
        try(var x=new Fixture()) {
            var token=prepare(x.batches);x.batches.beforeAcknowledgementMarker(() -> {throw new IllegalStateException("Controlled ACK marker gap");});var batch=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(2),RatisAtomicClientTest.key(3)),List.of(RatisAtomicClientTest.value(1),RatisAtomicClientTest.value(2)));var observer=x.batches.append(token,0,batch,RemoteMutationInputs.batch(71,false,batch),RequestContext.batch());assertThrows(ExecutionException.class,() -> observer.get(10,TimeUnit.SECONDS));x.batches.actualCompletion(observer).get(10,TimeUnit.SECONDS);assertEquals(1,x.writes.size());x.batches.close();x.session.close();
            try(var session=RatisDurableSessionClient.reopen(x.window,x.routed);var batches=x.open(session)){var saved=batches.resumeRetained(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);assertEquals(2,saved.acknowledgedMutations());assertEquals(1,saved.acknowledgedChunks());assertEquals(1,x.writes.size());batches.finishInput(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);batches.acknowledgeResult(token,batches.status(token).resultDigest());}
        }
    }
    @Test void copiedCountsAndCompleteCommandByteBoundsRejectBeforeAnySubmission() throws Exception {
        try(var x=new Fixture()) {
            var token=prepare(x.batches);var tooMany=new KVBatch.KVBatchRef(Collections.nCopies(RatisRocksDBBatches.MAX_INPUT_ITEMS+1,RatisAtomicClientTest.key(2)),Collections.nCopies(RatisRocksDBBatches.MAX_INPUT_ITEMS+1,RatisAtomicClientTest.value(1)));assertThrows(ExecutionException.class,() -> x.batches.append(token,0,tooMany,new byte[32],RequestContext.batch()).get(10,TimeUnit.SECONDS));assertTrue(x.writes.isEmpty());assertEquals(0,x.batches.resumeRetained(token,RequestContext.batch()).get(10,TimeUnit.SECONDS).copiedBatches());var tooWide=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(2)),List.of(it.cavallium.buffer.Buf.wrap(new byte[RatisRocksDBBatches.MAX_CHUNK_BYTES])));assertThrows(ExecutionException.class,() -> x.batches.append(token,0,tooWide,RemoteMutationInputs.batch(71,false,tooWide),RequestContext.batch()).get(10,TimeUnit.SECONDS));assertTrue(x.writes.isEmpty());assertEquals(0,x.batches.progress().acknowledgedChunks());x.batches.finishInput(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);x.batches.acknowledgeResult(token,x.batches.status(token).resultDigest());
        }
    }
    @Test void silentCallerPublisherDeadlineDoesNotInventBackendTailOrEof() throws Exception {
        try(var x=new Fixture()) {
            var token=prepare(x.batches);var requested=new CountDownLatch(1);var canceled=new AtomicBoolean();var subscribed=new java.util.concurrent.atomic.AtomicInteger();Publisher<KVBatch> publisher=subscriber -> {subscribed.incrementAndGet();subscriber.onSubscribe(new Subscription(){public void request(long n){assertEquals(1,n);requested.countDown();}public void cancel(){canceled.set(true);}});};var caller=new CallerStream(x.batches,token,publisher,RequestContext.batch());caller.result.orTimeout(250,TimeUnit.MILLISECONDS);assertTrue(requested.await(5,TimeUnit.SECONDS));assertThrows(ExecutionException.class,() -> caller.result.get(5,TimeUnit.SECONDS));assertTrue(canceled.get());assertTrue(x.writes.isEmpty());assertEquals(RatisRocksDBBatches.RecoveryState.PENDING,x.batches.recoveryState());x.batches.close();x.session.close();
            try(var session=RatisDurableSessionClient.reopen(x.window,x.routed);var batches=x.open(session)){var progress=batches.resumeRetained(token,RequestContext.batch()).get(5,TimeUnit.SECONDS);assertEquals(0,progress.copiedBatches());assertFalse(progress.sourceComplete());assertEquals(0,progress.acknowledgedChunks());assertTrue(x.writes.isEmpty());assertEquals(1,subscribed.get());batches.finishInput(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);batches.acknowledgeResult(token,batches.status(token).resultDigest());}
        }
    }

    @Test void completedInputRetryNeverDrainsAnotherUnresolvedCopiedBatch()throws Exception {
        try(var x=new Fixture()){
            var token=prepare(x.batches);var first=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(2)),List.of(RatisAtomicClientTest.value(1)));var second=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(3)),List.of(RatisAtomicClientTest.value(2)));
            x.batches.append(token,0,first,RemoteMutationInputs.batch(71,false,first),RequestContext.batch()).get(10,TimeUnit.SECONDS);x.lose.set(true);var failed=x.batches.append(token,1,second,RemoteMutationInputs.batch(71,false,second),RequestContext.batch());assertThrows(ExecutionException.class,()->failed.get(10,TimeUnit.SECONDS));x.batches.actualCompletion(failed).get(10,TimeUnit.SECONDS);int writes=x.writes.size();
            var unchanged=x.batches.append(token,0,first,RemoteMutationInputs.batch(71,false,first),RequestContext.batch()).get(10,TimeUnit.SECONDS);assertEquals(writes,x.writes.size());assertTrue(unchanged.retainedChunks()>0);x.batches.append(token,1,second,RemoteMutationInputs.batch(71,false,second),RequestContext.batch()).get(10,TimeUnit.SECONDS);x.batches.finishInput(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);x.batches.acknowledgeResult(token,x.batches.status(token).resultDigest());
        }
    }

    public static class AppendOperator extends it.cavallium.rockserver.core.impl.FFMByteArrayMergeOperator {
        public AppendOperator(String identity) { super(identity); }
        @Override public byte[] merge(byte[] key,byte[] existing,List<byte[]> operands) { var bytes=new java.io.ByteArrayOutputStream(); if(existing!=null) bytes.writeBytes(existing); operands.forEach(bytes::writeBytes); return bytes.toByteArray(); }
    }
    @Test void collidingMemberMergesUseOneNativeCommandAndLostReplyDoesNotReapplyOperands() throws Exception {
        UUID cluster=UUID.randomUUID(),database=UUID.randomUUID(),domain=UUID.randomUUID(),group=UUID.randomUUID(),column=UUID.randomUUID(),user=UUID.randomUUID();
        var artifact=new java.io.ByteArrayOutputStream(); String entry=AppendOperator.class.getName().replace('.','/')+".class";
        try(var jar=new java.util.jar.JarOutputStream(artifact)) { jar.putNextEntry(new java.util.jar.JarEntry(entry)); try(var input=AppendOperator.class.getClassLoader().getResourceAsStream(entry)) { assertNotNull(input); input.transferTo(jar); } jar.closeEntry(); }
        byte[] bytes=artifact.toByteArray(); var operator=new DistributedSchema.Operator("append",1,AppendOperator.class.getName(),java.security.MessageDigest.getInstance("SHA-256").digest(bytes),1,2,"append",1);
        var shape=new ColumnSchema(it.unimi.dsi.fastutil.ints.IntArrayList.of(1,1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(ColumnHashType.ALLSAME8),true,operator.name(),operator.version(),operator.className());
        var schema=new DistributedSchema(cluster,database,domain,1,1,List.of(new DistributedSchema.Column(1,column,"bucket",shape,operator)));
        var replica=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"peer","127.0.0.1:1",ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(UUID.randomUUID(),group,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica)); var directory=new RangeDirectory(cluster,database,domain,1,schema.digest(),0,schema.columnIds(),1,List.of(range));
        var path=root.resolve("native-merge"); it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool.stageOperator(path,operator,new it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract("append",1,true),bytes);
        var scheduler=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(1,1,1,4,1,"native-bulk-merge");
        try(var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults());var pool=it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool.open(path,it.cavallium.rockserver.core.config.ConfigParser.parseDefault(),org.slf4j.LoggerFactory.getLogger(getClass()),List.of(schema),Map.of(column,"bucket"),Map.of())) {
            var store=pool.openRange(new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Owner(group,1),new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Interval(domain,schema.columnIds(),null,null),schema.digest(),Map.of(column,"bucket"),Set.of(column)); store.enableLogicalSchema(schema); store.enableSessionControls();
            var ledger=SessionEpochLedger.initial(cluster,database,10).issue(user,1000); var grant=ledger.grants().getLast();
            store.apply(new ApplyPosition(1,0),DistributedCommand.sessionControl(group,1,schema.digest(),new OperationId(UUID.randomUUID(),1,0),RangeSessionControl.bind(ledger,grant,32),-1));
            var position=new java.util.concurrent.atomic.AtomicLong(); var writes=new ArrayList<DistributedCommand>(); var lose=new AtomicBoolean(true);
            try(var routed=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID col,byte[] key) throws IOException { try { return store.readCurrentPoint(col,key); } catch(org.rocksdb.RocksDBException failure) { throw new IOException(failure); } }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { writes.add(command); try { var result=CommandResult.decode(store.apply(new ApplyPosition(1,position.incrementAndGet()),command)); if(lose.getAndSet(false)) throw new IOException("Lost merged native chunk reply"); return result; } catch(org.rocksdb.RocksDBException failure) { throw new IOException(failure); } }
            })) {
                var window=root.resolve("merge-window.bin"); var journal=root.resolve("merge-bulk.bin"); var owner=UUID.randomUUID(); var request=UUID.randomUUID(); RemoteMutationToken token; var batch=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.member(10,"a"),RatisAtomicClientTest.member(10,"b"),RatisAtomicClientTest.member(10,"a")),List.of(RatisAtomicClientTest.value(1),RatisAtomicClientTest.value(9),it.cavallium.buffer.Buf.wrap(new byte[]{2,3})));
                try(var session=RatisDurableSessionClient.create(window,routed,ledger,grant,RatisSessionClient.Limits.defaults()); var batches=openShared(schema,directory,routed,session,scheduler,journal,budget)) {
                    token=batches.prepare(batches.owner(),request,0,1,true); assertEquals(0,writes.size());
                    var observer=batches.append(token,0,batch,RemoteMutationInputs.batch(1,true,batch),RequestContext.batch()); assertThrows(ExecutionException.class,() -> observer.get(10,TimeUnit.SECONDS)); batches.actualCompletion(observer).get(10,TimeUnit.SECONDS);
                    assertEquals(1,writes.size()); assertEquals(3,writes.getFirst().mutations().size()); assertTrue(writes.getFirst().mutations().stream().allMatch(m -> m.kind()==Mutation.Kind.BUCKET_MERGE));
                }
                try(var session=RatisDurableSessionClient.reopen(window,routed); var batches=openShared(schema,directory,routed,session,scheduler,journal,budget)) {
                    var progress=batches.resumeRetained(token,RequestContext.batch()).get(10,TimeUnit.SECONDS); assertEquals(3,progress.acknowledgedMutations()); assertEquals(writes.getFirst().operationId(),writes.getLast().operationId());
                    byte[] raw=routed.read(column,new byte[]{10,0}); var bucket=schema.decodeBucket(column,new byte[]{10,0},raw); assertArrayEquals(new byte[]{1,2,3},bucket.get(List.of("a".getBytes()))); assertArrayEquals(new byte[]{9},bucket.get(List.of("b".getBytes()))); int count=writes.size(); batches.append(token,0,batch,RemoteMutationInputs.batch(1,true,batch),RequestContext.batch()).get(10,TimeUnit.SECONDS); assertEquals(count,writes.size());
                    var tiny=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.member(10,"a")),List.of(RatisAtomicClientTest.value(4)));byte[] tinyDigest=RemoteMutationInputs.batch(1,true,tiny);
                    for(int ordinal=1;ordinal<=4100;ordinal++)batches.append(token,ordinal,tiny,tinyDigest,RequestContext.batch()).get(10,TimeUnit.SECONDS);
                    var expired=assertThrows(ExecutionException.class,()->batches.append(token,0,batch,RemoteMutationInputs.batch(1,true,batch),RequestContext.batch()).get(10,TimeUnit.SECONDS));assertInstanceOf(RatisBulkInputExpiredException.class,expired.getCause());assertEquals(RemoteMutationStatus.State.PENDING,batches.status(token).state());assertEquals(0,batches.owner().nextRequestSequence());
                    int applied=writes.size();batches.append(token,4100,tiny,tinyDigest,RequestContext.batch()).get(10,TimeUnit.SECONDS);assertEquals(applied,writes.size());
                }
                try(var session=RatisDurableSessionClient.reopen(window,routed);var batches=openShared(schema,directory,routed,session,scheduler,journal,budget)){
                    var tiny=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.member(10,"a")),List.of(RatisAtomicClientTest.value(4)));byte[] digest=RemoteMutationInputs.batch(1,true,tiny);int applied=writes.size();batches.append(token,4100,tiny,digest,RequestContext.batch()).get(10,TimeUnit.SECONDS);assertEquals(applied,writes.size());batches.append(token,4101,tiny,digest,RequestContext.batch()).get(10,TimeUnit.SECONDS);
                    var expected=new byte[4104];expected[0]=1;expected[1]=2;expected[2]=3;Arrays.fill(expected,3,expected.length,(byte)4);assertArrayEquals(expected,schema.decodeBucket(column,new byte[]{10,0},routed.read(column,new byte[]{10,0})).get(List.of("a".getBytes())));
                    batches.finishInput(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);batches.acknowledgeResult(token,batches.status(token).resultDigest());
                }
            }
        } finally { scheduler.disposeGracefully().block(java.time.Duration.ofSeconds(10)); }
    }

}
