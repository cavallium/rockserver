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
            session=RatisDurableSessionClient.create(window,routed,ledger,grant,RatisSessionClient.Limits.defaults()); batches=new RatisRocksDBBatches(f.schema,routed,session,f.scheduler,journal,RatisRocksDBBatches.SessionOwnership.BORROWED);
        }
        public void close() throws Exception { batches.close(); session.close(); routed.close(); f.close(); }
    }
    static Publisher<KVBatch> one(KVBatch batch) {
        return subscriber -> subscriber.onSubscribe(new Subscription() {
            int count; boolean stopped;
            public synchronized void request(long n) { assertEquals(1,n); if(stopped) return; if(count++==0) subscriber.onNext(batch); else subscriber.onComplete(); }
            public synchronized void cancel() { stopped=true; }
        });
    }
    @Test void interleavedGroupsBecomeWholeNativeChunksWithOriginalWithinGroupOrder() throws Exception {
        try(var x=new Fixture()) {
            var keys=List.of(RatisAtomicClientTest.key(2),RatisAtomicClientTest.key(200),RatisAtomicClientTest.key(3),RatisAtomicClientTest.key(201),RatisAtomicClientTest.key(2));
            var values=List.of(RatisAtomicClientTest.value(1),RatisAtomicClientTest.value(2),RatisAtomicClientTest.value(3),RatisAtomicClientTest.value(4),RatisAtomicClientTest.value(5));
            var observer=x.batches.putBatch(71,one(new KVBatch.KVBatchRef(keys,values)),PutBatchMode.WRITE_BATCH,RequestContext.batch()); observer.get(10,TimeUnit.SECONDS); x.batches.actualCompletion(observer).get(10,TimeUnit.SECONDS);
            assertEquals(2,x.writes.size()); assertEquals(List.of(3,2),x.writes.stream().map(c -> c.mutations().size()).toList());
            assertEquals(List.of((byte)2,(byte)3,(byte)2),x.writes.getFirst().mutations().stream().map(m -> m.key()[0]).toList());
            assertArrayEquals(new byte[]{5},x.routed.read(x.f.schema.column(71).columnId(),new byte[]{2}));
            var progress=x.batches.progress(); assertTrue(progress.sourceComplete()); assertEquals(2,progress.acknowledgedChunks()); assertEquals(5,progress.acknowledgedMutations()); assertEquals(RatisRocksDBBatches.RecoveryState.NONE,x.batches.recoveryState());
        }
    }
    @Test void lostAppliedReplyReopenUsesOriginalIdAndOnlyCopiedInputWithoutInventingEof() throws Exception {
        try(var x=new Fixture()) {
            x.lose.set(true); var batch=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(2),RatisAtomicClientTest.key(3)),List.of(RatisAtomicClientTest.value(1),RatisAtomicClientTest.value(2)));
            var observer=x.batches.putBatch(71,one(batch),PutBatchMode.WRITE_BATCH,RequestContext.batch()); assertThrows(ExecutionException.class,() -> observer.get(10,TimeUnit.SECONDS)); x.batches.actualCompletion(observer).get(10,TimeUnit.SECONDS);
            var original=x.writes.getFirst().operationId(); assertEquals(1,x.writes.size()); x.batches.close(); x.session.close();
            try(var session=RatisDurableSessionClient.reopen(x.window,x.routed); var batches=new RatisRocksDBBatches(x.f.schema,x.routed,session,x.f.scheduler,x.journal,RatisRocksDBBatches.SessionOwnership.BORROWED)) {
                var progress=batches.resumeRetained(RequestContext.batch()).get(10,TimeUnit.SECONDS); assertEquals(2,progress.acknowledgedMutations()); assertFalse(progress.sourceComplete());
                assertEquals(original,x.writes.getLast().operationId()); assertArrayEquals(DistributedCommandCodec.encode(x.writes.getFirst()),DistributedCommandCodec.encode(x.writes.getLast())); assertEquals(RatisRocksDBBatches.RecoveryState.NONE,batches.recoveryState());
            }
        }
    }
    @Test void unsupportedModesRejectBeforeSubscription() throws Exception {
        try(var x=new Fixture()) {
            var subscribed=new AtomicBoolean(); Publisher<KVBatch> source=ignored -> subscribed.set(true);
            assertThrows(ExecutionException.class,() -> x.batches.putBatch(71,source,PutBatchMode.WRITE_BATCH_NO_WAL,RequestContext.batch()).get());
            assertThrows(ExecutionException.class,() -> x.batches.mergeBatch(71,source,MergeBatchMode.MERGE_SST_INGESTION,RequestContext.batch()).get()); assertFalse(subscribed.get());
            assertThrows(ExecutionException.class,() -> x.batches.putBatch(71,source,PutBatchMode.WRITE_BATCH,RequestContext.latency(java.time.Duration.ofSeconds(1))).get()); assertFalse(subscribed.get()); assertNull(x.batches.progress());
        }
    }
    @Test void mutableInputIsCopiedBeforeReturningOnNextAndDemandIsOne() throws Exception {
        try(var x=new Fixture()) {
            byte[] key={2},value={7}; var keys=new ArrayList<>(List.of(new Keys(it.cavallium.buffer.Buf.wrap(key)))); var values=new ArrayList<>(List.of(it.cavallium.buffer.Buf.wrap(value))); var demands=new java.util.concurrent.atomic.AtomicInteger();
            Publisher<KVBatch> publisher=subscriber -> subscriber.onSubscribe(new Subscription() {
                boolean sent;
                public void request(long n) { assertEquals(1,n); demands.incrementAndGet(); if(sent) return; sent=true; subscriber.onNext(new KVBatch.KVBatchRef(keys,values)); key[0]=99; value[0]=99; keys.clear(); values.clear(); subscriber.onComplete(); }
                public void cancel() {}
            });
            x.batches.putBatch(71,publisher,PutBatchMode.WRITE_BATCH,RequestContext.batch()).get(10,TimeUnit.SECONDS);
            assertEquals(1,demands.get()); assertEquals(1,x.writes.size()); assertArrayEquals(new byte[]{2},x.writes.getFirst().mutations().getFirst().key()); assertArrayEquals(new byte[]{7},x.writes.getFirst().mutations().getFirst().value()); assertTrue(x.batches.progress().sourceComplete());
        }
    }
    @Test void cancelDuringActualRpcRetainsOwnershipAndKnownInputWithoutRequestingTail() throws Exception {
        try(var x=new Fixture()) {
            x.entered=new CountDownLatch(1); x.release=new CountDownLatch(1); var demands=new java.util.concurrent.atomic.AtomicInteger(); var canceled=new AtomicBoolean();
            var batch=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(2),RatisAtomicClientTest.key(3)),List.of(RatisAtomicClientTest.value(1),RatisAtomicClientTest.value(2)));
            Publisher<KVBatch> publisher=subscriber -> subscriber.onSubscribe(new Subscription() { public void request(long n) { assertEquals(1,n); demands.incrementAndGet(); subscriber.onNext(batch); } public void cancel() { canceled.set(true); } });
            var observer=x.batches.putBatch(71,publisher,PutBatchMode.WRITE_BATCH,RequestContext.batch()); assertTrue(x.entered.await(10,TimeUnit.SECONDS));
            var actual=x.batches.actualCompletion(observer); assertEquals(2,x.batches.progress().retainedChunks()==1 ? x.writes.getFirst().mutations().size() : -1); assertEquals(RatisRocksDBBatches.RecoveryState.ACTIVE,x.batches.recoveryState());
            observer.cancel(false); assertTrue(canceled.get()); assertFalse(actual.isDone()); assertThrows(IOException.class,x.batches::close); assertThrows(IOException.class,x.session::close);
            x.release.countDown(); actual.get(10,TimeUnit.SECONDS); assertEquals(1,demands.get()); assertEquals(2,x.batches.progress().acknowledgedMutations()); assertFalse(x.batches.progress().sourceComplete());
            var saved=x.batches.resumeRetained(RequestContext.batch()).get(10,TimeUnit.SECONDS); assertEquals(2,saved.acknowledgedMutations()); assertFalse(saved.sourceComplete()); assertEquals(1,x.writes.size());
        }
    }
    @Test void deadlineExpiresObserverButKeepsBlockedRpcOwnershipUntilActualReturn() throws Exception {
        try(var x=new Fixture()) {
            x.entered=new CountDownLatch(1); x.release=new CountDownLatch(1); var canceled=new AtomicBoolean(); var demands=new java.util.concurrent.atomic.AtomicInteger();
            var batch=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(2),RatisAtomicClientTest.key(3)),List.of(RatisAtomicClientTest.value(1),RatisAtomicClientTest.value(2)));
            Publisher<KVBatch> publisher=subscriber -> subscriber.onSubscribe(new Subscription() { public void request(long n) { assertEquals(1,n); demands.incrementAndGet(); subscriber.onNext(batch); } public void cancel() { canceled.set(true); } });
            var observer=x.batches.putBatch(71,publisher,PutBatchMode.WRITE_BATCH,RequestContext.batch(java.time.Duration.ofMillis(500)));
            assertTrue(x.entered.await(5,TimeUnit.SECONDS)); var actual=x.batches.actualCompletion(observer);
            assertThrows(ExecutionException.class,() -> observer.get(5,TimeUnit.SECONDS)); assertTrue(canceled.get()); assertFalse(actual.isDone());
            assertThrows(IOException.class,x.batches::close); assertThrows(IOException.class,x.session::close); assertEquals(RatisRocksDBBatches.RecoveryState.ACTIVE,x.batches.recoveryState());
            x.release.countDown(); actual.get(5,TimeUnit.SECONDS); assertEquals(1,demands.get()); assertEquals(2,x.batches.progress().acknowledgedMutations()); assertFalse(x.batches.progress().sourceComplete());
            var progress=x.batches.resumeRetained(RequestContext.batch()).get(5,TimeUnit.SECONDS); assertEquals(2,progress.acknowledgedMutations()); assertEquals(1,x.writes.size()); assertFalse(progress.sourceComplete());
        }
    }
    @Test void acknowledgementGapReopenDoesNotSubmitAnAlreadyConsumedChunk() throws Exception {
        try(var x=new Fixture()) {
            x.batches.beforeAcknowledgementMarker(() -> { throw new IllegalStateException("Controlled ACK marker gap"); });
            var batch=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(2),RatisAtomicClientTest.key(3)),List.of(RatisAtomicClientTest.value(1),RatisAtomicClientTest.value(2)));
            var observer=x.batches.putBatch(71,one(batch),PutBatchMode.WRITE_BATCH,RequestContext.batch()); assertThrows(ExecutionException.class,() -> observer.get(10,TimeUnit.SECONDS)); x.batches.actualCompletion(observer).get(10,TimeUnit.SECONDS);
            assertEquals(1,x.writes.size()); x.batches.close(); x.session.close();
            try(var session=RatisDurableSessionClient.reopen(x.window,x.routed); var batches=new RatisRocksDBBatches(x.f.schema,x.routed,session,x.f.scheduler,x.journal,RatisRocksDBBatches.SessionOwnership.BORROWED)) {
                var saved=batches.resumeRetained(RequestContext.batch()).get(10,TimeUnit.SECONDS); assertEquals(2,saved.acknowledgedMutations()); assertEquals(1,saved.acknowledgedChunks()); assertEquals(1,x.writes.size());
            }
        }
    }
    @Test void copiedCountsAndCompleteCommandByteBoundsRejectBeforeAnySubmission() throws Exception {
        try(var x=new Fixture()) {
            var tooMany=new KVBatch.KVBatchRef(Collections.nCopies(RatisRocksDBBatches.MAX_INPUT_ITEMS+1,RatisAtomicClientTest.key(2)),Collections.nCopies(RatisRocksDBBatches.MAX_INPUT_ITEMS+1,RatisAtomicClientTest.value(1)));
            assertThrows(ExecutionException.class,() -> x.batches.putBatch(71,one(tooMany),PutBatchMode.WRITE_BATCH,RequestContext.batch()).get(10,TimeUnit.SECONDS)); assertTrue(x.writes.isEmpty());
            x.batches.resumeRetained(RequestContext.batch()).get(10,TimeUnit.SECONDS);
            var tooWide=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(2)),List.of(it.cavallium.buffer.Buf.wrap(new byte[RatisRocksDBBatches.MAX_CHUNK_BYTES])));
            assertThrows(ExecutionException.class,() -> x.batches.putBatch(71,one(tooWide),PutBatchMode.WRITE_BATCH,RequestContext.batch()).get(10,TimeUnit.SECONDS)); assertTrue(x.writes.isEmpty()); assertEquals(0,x.batches.progress().acknowledgedChunks());
        }
    }

    @Test void silentPublisherDeadlineSettlesIdleOwnershipWithoutInventingTailOrEof() throws Exception {
        try(var x=new Fixture()) {
            var requested=new CountDownLatch(1); var canceled=new AtomicBoolean();
            Publisher<KVBatch> publisher=subscriber -> subscriber.onSubscribe(new Subscription() { public void request(long n) { assertEquals(1,n); requested.countDown(); } public void cancel() { canceled.set(true); } });
            var observer=x.batches.putBatch(71,publisher,PutBatchMode.WRITE_BATCH,RequestContext.batch(java.time.Duration.ofMillis(250))); assertTrue(requested.await(5,TimeUnit.SECONDS));
            assertThrows(ExecutionException.class,() -> observer.get(5,TimeUnit.SECONDS)); x.batches.actualCompletion(observer).get(5,TimeUnit.SECONDS); assertTrue(canceled.get()); assertTrue(x.writes.isEmpty()); assertEquals(RatisRocksDBBatches.RecoveryState.PENDING,x.batches.recoveryState());
            x.batches.close(); x.session.close();
            try(var session=RatisDurableSessionClient.reopen(x.window,x.routed); var batches=new RatisRocksDBBatches(x.f.schema,x.routed,session,x.f.scheduler,x.journal,RatisRocksDBBatches.SessionOwnership.BORROWED)) {
                var progress=batches.resumeRetained(RequestContext.batch()).get(5,TimeUnit.SECONDS); assertEquals(0,progress.copiedBatches()); assertFalse(progress.sourceComplete()); assertEquals(0,progress.acknowledgedChunks()); assertTrue(x.writes.isEmpty());
            }
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
        try(var pool=it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool.open(path,it.cavallium.rockserver.core.config.ConfigParser.parseDefault(),org.slf4j.LoggerFactory.getLogger(getClass()),schema,Map.of(column,"bucket"),Map.of())) {
            var store=pool.openRange(new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Owner(group,1),new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Interval(domain,schema.columnIds(),null,null),schema.digest(),Map.of(column,"bucket"),Set.of(column)); store.enableLogicalSchema(schema); store.enableSessionControls();
            var ledger=SessionEpochLedger.initial(cluster,database,10).issue(user,1000); var grant=ledger.grants().getLast();
            store.apply(new ApplyPosition(1,0),DistributedCommand.sessionControl(group,1,schema.digest(),new OperationId(UUID.randomUUID(),1,0),RangeSessionControl.bind(ledger,grant,32),-1));
            var position=new java.util.concurrent.atomic.AtomicLong(); var writes=new ArrayList<DistributedCommand>(); var lose=new AtomicBoolean(true);
            try(var routed=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID col,byte[] key) throws IOException { try { return store.readCurrentPoint(col,key); } catch(org.rocksdb.RocksDBException failure) { throw new IOException(failure); } }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { writes.add(command); try { var result=CommandResult.decode(store.apply(new ApplyPosition(1,position.incrementAndGet()),command)); if(lose.getAndSet(false)) throw new IOException("Lost merged native chunk reply"); return result; } catch(org.rocksdb.RocksDBException failure) { throw new IOException(failure); } }
            })) {
                var window=root.resolve("merge-window.bin"); var journal=root.resolve("merge-bulk.bin");
                try(var session=RatisDurableSessionClient.create(window,routed,ledger,grant,RatisSessionClient.Limits.defaults()); var batches=new RatisRocksDBBatches(schema,routed,session,scheduler,journal,RatisRocksDBBatches.SessionOwnership.BORROWED)) {
                    var batch=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.member(10,"a"),RatisAtomicClientTest.member(10,"b"),RatisAtomicClientTest.member(10,"a")),List.of(RatisAtomicClientTest.value(1),RatisAtomicClientTest.value(9),it.cavallium.buffer.Buf.wrap(new byte[]{2,3})));
                    var observer=batches.mergeBatch(1,one(batch),MergeBatchMode.MERGE_WRITE_BATCH,RequestContext.batch()); assertThrows(ExecutionException.class,() -> observer.get(10,TimeUnit.SECONDS)); batches.actualCompletion(observer).get(10,TimeUnit.SECONDS);
                    assertEquals(1,writes.size()); assertEquals(3,writes.getFirst().mutations().size()); assertTrue(writes.getFirst().mutations().stream().allMatch(m -> m.kind()==Mutation.Kind.BUCKET_MERGE));
                }
                try(var session=RatisDurableSessionClient.reopen(window,routed); var batches=new RatisRocksDBBatches(schema,routed,session,scheduler,journal,RatisRocksDBBatches.SessionOwnership.BORROWED)) {
                    var progress=batches.resumeRetained(RequestContext.batch()).get(10,TimeUnit.SECONDS); assertEquals(3,progress.acknowledgedMutations()); assertEquals(writes.getFirst().operationId(),writes.getLast().operationId());
                    byte[] raw=routed.read(column,new byte[]{10,0}); var bucket=schema.decodeBucket(column,new byte[]{10,0},raw); assertArrayEquals(new byte[]{1,2,3},bucket.get(List.of("a".getBytes()))); assertArrayEquals(new byte[]{9},bucket.get(List.of("b".getBytes())));
                }
            }
        } finally { scheduler.disposeGracefully().block(java.time.Duration.ofSeconds(10)); }
    }

}
