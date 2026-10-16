package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisRocksDBDirectTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final RatisPointFixtureBudget budget=new RatisPointFixtureBudget();
        final RatisAtomicClientTest.Fixture f=new RatisAtomicClientTest.Fixture(root,true);
        final SessionEpochLedger ledger;
        final SessionEpochLedger.Grant grant;
        final RatisDurableSessionClient session;
        final RatisRocksDBDirect direct;
        final RatisTransactionClient transactions;
        final RatisRocksDBAdapter adapter;
        final RatisRoutedClient routed;
        final Path window,journal;
        final AtomicBoolean lose=new AtomicBoolean(); final java.util.concurrent.atomic.AtomicInteger loseAfter=new java.util.concurrent.atomic.AtomicInteger(-1); final List<OperationId> writes=new ArrayList<>();
        boolean abandoned;
        Fixture() throws Exception {
            ledger=f.ledger.issue(new UUID(250,1),1000); grant=ledger.grants().getLast();
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.epochs.execute(f.catalogue.epochs.prepare(f.outer.operation(2),f.ledger,ledger)).status());
            int n=0; for (var group:f.groups) {
                var binding=f.catalogue.epochs.prepareSessionBinding(f.directory,group,new OperationId(f.admin,2,n++),grant,32,-1);
                assertEquals(CommandResult.Status.SUCCESS,f.data.execute(f.data.prepareSessionControl(binding)).status());
            }
            var transport=f.transport(); routed=new RatisRoutedClient(f.directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return transport.read(route,column,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException {
                    synchronized (writes) { writes.add(command.operationId()); }
                    var result=transport.command(route,command); if (lose.getAndSet(false) || loseAfter.getAndUpdate(left -> left<0 ? -1 : left-1)==0) throw new IOException("Lost direct applied reply"); return result;
                }
                public CompletableFuture<CommandResult> commandAsync(RangeDescriptor route,DistributedCommand command)throws IOException {
                    synchronized(writes){writes.add(command.operationId());}return transport.commandAsync(route,command).thenApply(result->{if(lose.getAndSet(false) || loseAfter.getAndUpdate(left->left<0?-1:left-1)==0)throw new CompletionException(new IOException("Lost direct applied reply"));return result;});
                }
                public CompletableFuture<byte[]> transactionAsync(RangeDescriptor route,byte[] bytes,boolean fresh)throws IOException{return transport.transactionAsync(route,bytes,fresh);}
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException { return transport.transaction(route,bytes,fresh); }
            });
            window=root.resolve("direct-window.bin"); session=RatisDurableSessionClient.create(window,routed,ledger,grant,RatisSessionClient.Limits.defaults());
            journal=root.resolve("direct-request.bin"); direct=budget.open(f.schema,routed,session,f.scheduler,journal,window); transactions=RatisRocksDBAdapterTest.client(f,f.data);
            adapter=new RatisRocksDBAdapter(RatisTransactionBindings.of(f.schema,f.directory,(transactions).bindings().domain((f.schema).domainId()).client()),transactions,new RatisRocksDBAdapterTest.Allocations(f),direct,null);
        }
        public void close() throws Exception { if (!abandoned) adapter.close(); direct.close(); session.close(); routed.close(); transactions.close(); budget.close(); f.close(); }
    }
    static RemoteMutationToken prepare(RatisRocksDBDirect direct,long column,List<Keys> keys,List<Buf> values,RatisRocksDBDirect.WriteKind kind,RequestType.RequestTypeId callback,boolean multi) throws Exception {
        var owner=direct.owner();var request=UUID.randomUUID();long sequence=owner.nextRequestSequence();return direct.prepareSequence(owner,request,sequence,column,keys,values,kind,callback,multi,RequestContext.batch()).get(5,TimeUnit.SECONDS);
    }
    @Test void directTypedPointAndSequentialMultiWritesNeedNoTransactionCapture() throws Exception {
        try(var x=new Fixture()) {
            var api=x.adapter.api(RequestContext.batch());var key=RatisAtomicClientTest.key(2);
            var first=prepare(x.direct,71,List.of(key),List.of(RatisAtomicClientTest.value(1)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.PREVIOUS,false);
            assertNull(x.direct.execute(first,RequestContext.batch()).get(5,TimeUnit.SECONDS));assertNull(CommandResult.decode(x.direct.resultPage(first,0,1,CommandResult.MAX_BYTES).getFirst()).value(0));assertEquals(RatisRocksDBDirect.RecoveryState.COMPLETE,x.direct.recoveryState());x.direct.acknowledgeResult(first,x.direct.status(first).resultDigest());
            var token=prepare(x.direct,71,List.of(key,key,RatisAtomicClientTest.key(200)),List.of(RatisAtomicClientTest.value(2),RatisAtomicClientTest.value(3),RatisAtomicClientTest.value(4)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.PREVIOUS,true);
            var previous=(List<?>)x.direct.execute(token,RequestContext.batch()).get(5,TimeUnit.SECONDS);assertArrayEquals(new byte[]{1},((Buf)previous.get(0)).toByteArray());assertArrayEquals(new byte[]{2},((Buf)previous.get(1)).toByteArray());assertNull(previous.get(2));assertEquals(3,x.direct.resultPage(token,0,3,CommandResult.MAX_BYTES).size());x.direct.acknowledgeResult(token,x.direct.status(token).resultDigest());
            assertEquals(List.of(true,true),api.existsMultiAsync(0,71,List.of(key,RatisAtomicClientTest.key(200))).get(5,TimeUnit.SECONDS));assertArrayEquals(new byte[]{3},api.getAsync(0,71,key,RequestType.<Buf>current()).get(5,TimeUnit.SECONDS).toByteArray());
            var deletion=prepare(x.direct,71,List.of(key),List.of(),RatisRocksDBDirect.WriteKind.DELETE,RequestType.RequestTypeId.NOTHING,true);assertEquals(List.of(),x.direct.execute(deletion,RequestContext.batch()).get(5,TimeUnit.SECONDS));x.direct.acknowledgeResult(deletion,x.direct.status(deletion).resultDigest());assertFalse(api.getAsync(0,71,key,RequestType.<Buf>exists()).get(5,TimeUnit.SECONDS));
            assertEquals(0,x.f.captureRequests.get());assertEquals(0,x.f.opens.get());assertEquals(0,x.adapter.stats().transactions());assertTrue(x.session.pending().isEmpty());
        }
    }
    @Test void unknownLaterMultiWriteResumesOnlyItsOriginalPendingCommand() throws Exception {
        try(var x=new Fixture()) {
            var first=RatisAtomicClientTest.key(2);var second=RatisAtomicClientTest.key(200);var seed=prepare(x.direct,71,List.of(first),List.of(RatisAtomicClientTest.value(1)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false);x.direct.execute(seed,RequestContext.batch()).get(5,TimeUnit.SECONDS);x.direct.acknowledgeResult(seed,x.direct.status(seed).resultDigest());
            var token=prepare(x.direct,71,List.of(first,second),List.of(RatisAtomicClientTest.value(2),RatisAtomicClientTest.value(3)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.PREVIOUS,true);x.loseAfter.set(1);assertThrows(ExecutionException.class,() -> x.direct.execute(token,RequestContext.batch()).get(5,TimeUnit.SECONDS));var retained=x.session.pending().getFirst().command().operationId();x.abandoned=true;x.direct.close();x.session.close();
            try(var session=RatisDurableSessionClient.reopen(x.window,x.f.data);var direct=x.budget.open(x.f.schema,x.f.data,session,x.f.scheduler,x.journal,x.window)) {
                var result=(List<?>)direct.execute(token,RequestContext.batch()).get(5,TimeUnit.SECONDS);assertArrayEquals(new byte[]{1},((Buf)result.get(0)).toByteArray());assertNull(result.get(1));assertEquals(2,direct.resultPage(token,0,2,CommandResult.MAX_BYTES).size());direct.acknowledgeResult(token,direct.status(token).resultDigest());
                synchronized(x.writes){assertEquals(1,Collections.frequency(x.writes,retained));assertEquals(3,x.writes.size(),"Seed plus two effects; recovery resolves only the original receipt");}assertEquals(0,x.f.captureRequests.get());assertTrue(session.pending().isEmpty());
            }
        }
    }
    @Test void durableDirectPendingSurvivesFrontendReopenWithoutNewIdentity() throws Exception {
        try(var x=new Fixture()) {
            var key=RatisAtomicClientTest.key(2);var value=RatisAtomicClientTest.value(9);var token=prepare(x.direct,71,List.of(key),List.of(value),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.PREVIOUS,false);x.lose.set(true);assertThrows(ExecutionException.class,() -> x.direct.execute(token,RequestContext.batch()).get(5,TimeUnit.SECONDS));var original=x.session.pending().getFirst().command().operationId();x.direct.close();x.session.close();
            try(var session=RatisDurableSessionClient.reopen(x.window,x.f.data);var direct=x.budget.open(x.f.schema,x.f.data,session,x.f.scheduler,x.journal,x.window)) {
                assertEquals(original,session.pending().getFirst().command().operationId());assertThrows(ExecutionException.class,() -> direct.prepareSequence(direct.owner(),token.requestId(),token.requestSequence(),71,List.of(key),List.of(value),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.CHANGED,false,RequestContext.batch()).get(5,TimeUnit.SECONDS));assertNull(direct.execute(token,RequestContext.batch()).get(5,TimeUnit.SECONDS));direct.acknowledgeResult(token,direct.status(token).resultDigest());assertTrue(session.pending().isEmpty());assertArrayEquals(new byte[]{9},x.f.get(71,key));
            }
        }
    }
    @Test void failureAfterAcknowledgementRetainsOriginalFrontendResultAcrossReopen() throws Exception {
        try(var x=new Fixture()) {
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);x.direct.afterAcknowledgement(() -> {entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("ACK barrier timed out");}catch(InterruptedException failure){throw new AssertionError(failure);}});
            var key=RatisAtomicClientTest.key(2);var value=RatisAtomicClientTest.value(9);var token=prepare(x.direct,71,List.of(key),List.of(value),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.PREVIOUS,false);var pending=x.direct.execute(token,RequestContext.batch(Duration.ofMillis(500)));
            try{assertTrue(entered.await(5,TimeUnit.SECONDS));assertTrue(x.session.pending().isEmpty());assertThrows(IOException.class,x.direct::close);Thread.sleep(550);}finally{release.countDown();}assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS));int effects=x.f.writes();x.abandoned=true;x.direct.close();x.session.close();
            try(var session=RatisDurableSessionClient.reopen(x.window,x.f.data);var direct=x.budget.open(x.f.schema,x.f.data,session,x.f.scheduler,x.journal,x.window)) {
                assertNull(direct.execute(token,RequestContext.batch()).get(5,TimeUnit.SECONDS));assertEquals(RatisRocksDBDirect.RecoveryState.COMPLETE,direct.recoveryState());assertEquals(effects,x.f.writes());assertArrayEquals(new byte[]{9},x.f.get(71,key));direct.acknowledgeResult(token,direct.status(token).resultDigest());assertEquals(RatisRocksDBDirect.RecoveryState.NONE,direct.recoveryState());
            }
        }
    }
    @Test void definiteSessionExpiryRequiresExplicitAckBeforeAnotherRequest() throws Exception {
        try(var x=new Fixture()) {
            var expired=x.ledger.advanceTime(1000);assertEquals(CommandResult.Status.SUCCESS,x.f.catalogue.epochs.execute(x.f.catalogue.epochs.prepare(x.f.outer.operation(3),x.ledger,expired)).status());int n=0;for(var group:x.f.groups){var clock=DistributedCommand.sessionControl(group,1,x.f.schema.digest(),new OperationId(x.f.admin,3,n++),RangeSessionControl.advance(expired),-1);assertEquals(CommandResult.Status.SUCCESS,x.f.data.execute(x.f.data.prepareSessionControl(clock)).status());}
            for(int key:new int[]{2,3}) {
                var token=prepare(x.direct,71,List.of(RatisAtomicClientTest.key(key)),List.of(RatisAtomicClientTest.value(9)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false);var failure=assertThrows(ExecutionException.class,() -> x.direct.execute(token,RequestContext.batch()).get(5,TimeUnit.SECONDS));assertInstanceOf(RocksDBException.class,failure.getCause());assertEquals(CommandResult.Status.SESSION_EXPIRED,x.direct.status(token).outcome());assertEquals(RatisRocksDBDirect.RecoveryState.COMPLETE,x.direct.recoveryState());assertTrue(x.session.pending().isEmpty());x.direct.acknowledgeResult(token,x.direct.status(token).resultDigest());
            }
            synchronized(x.writes){assertEquals(2,x.writes.size());assertNotEquals(x.writes.getFirst(),x.writes.getLast());}
        }
    }
    @Test void tokenlessAdapterMutationsRejectBeforeDataOrPublisherSubscription() throws Exception {
        try(var x=new Fixture()) {
            var api=x.adapter.api(RequestContext.batch());var subscribed=new AtomicBoolean();org.reactivestreams.Publisher<KVBatch> source=ignored -> subscribed.set(true);
            assertThrows(ExecutionException.class,() -> api.putAsync(0,71,RatisAtomicClientTest.key(2),RatisAtomicClientTest.value(9),RequestType.<Buf>none()).get());assertThrows(ExecutionException.class,() -> api.deleteRangeAsync(71,null,null).get());assertThrows(ExecutionException.class,() -> api.putBatchAsync(71,source,PutBatchMode.WRITE_BATCH).get());assertThrows(ExecutionException.class,() -> api.mergeBatchAsync(71,source,MergeBatchMode.MERGE_WRITE_BATCH).get());assertFalse(subscribed.get());assertTrue(x.writes.isEmpty());assertTrue(x.session.pristine());assertEquals(0,x.f.opens.get());
        }
    }
}
