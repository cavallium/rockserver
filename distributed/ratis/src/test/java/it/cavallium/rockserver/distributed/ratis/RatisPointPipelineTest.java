package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisPointPipelineTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    record Submitted(RangeDescriptor route,DistributedCommand command,CompletableFuture<CommandResult> reply) {}
    @Test void oneWriteWorkerSubmitsTwoOwnedRpcFuturesAndCancellationKeepsCreditUntilAckAndColdReopen()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("native"),true);var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())) {
            var calls=new LinkedBlockingQueue<Submitted>();var replies=new CopyOnWriteArrayList<Submitted>();var drains=new ArrayList<CompletableFuture<Void>>();var managers=new ArrayList<RatisPointOwnerManager>();var transport=f.transport();
            try(var routed=new RatisRoutedClient(f.directory,new RatisRoutedClient.Transport(){
                public byte[] read(RangeDescriptor route,UUID column,byte[] key)throws IOException{return transport.read(route,column,key);}
                public CommandResult command(RangeDescriptor route,DistributedCommand command)throws IOException{throw new IOException("POINT pipeline must use actual asynchronous transport");}
                public CompletableFuture<CommandResult> commandAsync(RangeDescriptor route,DistributedCommand command){var result=new CompletableFuture<CommandResult>();var submitted=new Submitted(route,command,result);replies.add(submitted);calls.add(submitted);return result;}
                public CompletableFuture<byte[]> transactionAsync(RangeDescriptor route,byte[] bytes,boolean fresh)throws IOException{return transport.transactionAsync(route,bytes,fresh);}
            })) {
                Throwable problem=null;try {
                var controls=new AtomicLong();UUID issuer=UUID.randomUUID();
                RatisRocksDBConnection.SessionFactory sessions=new RatisRocksDBConnection.SessionFactory(List.of(routed),(path,schema,bindings)->{var directory=bindings.directory();
                    Path window=path.resolve("window.bin");if(Files.exists(window))return RatisDurableSessionClient.reopen(window,routed);
                    var before=f.catalogue.epochs.read();var next=before.issue(UUID.randomUUID(),1000);var grant=next.grants().getLast();
                    assertEquals(CommandResult.Status.SUCCESS,f.catalogue.epochs.execute(f.catalogue.epochs.prepare(new OperationId(issuer,0,controls.getAndIncrement()),before,next)).status());
                    for(var group:f.groups)assertEquals(CommandResult.Status.SUCCESS,f.data.execute(f.data.prepareSessionControl(f.catalogue.epochs.prepareSessionBinding(directory,group,new OperationId(issuer,next.minimumEpoch(),controls.getAndIncrement()),grant,32,-1))).status());
                    return RatisDurableSessionClient.create(window,routed,next,grant,RatisSessionClient.Limits.defaults());
                });
                Path ownersRoot=root.resolve("owners");List<RemoteMutationOwner> identities;RemoteMutationToken firstToken,secondToken;
                try(var owners=new RatisPointOwnerManager(f.schema,f.directory,routed,f.scheduler,ownersRoot,budget,sessions,f.catalogue.epochs,f.catalogue.epochs.catalogue(f.outer.directoryColumn),header->{throw new IOException("No reroute in this workflow");},f.homes)) {
                    managers.add(owners);identities=owners.available();assertEquals(2,identities.size());assertNotEquals(identities.get(0).ownerId(),identities.get(1).ownerId());
                    var first=owners.select(identities.get(0));var second=owners.select(identities.get(1));byte[] key={2},value={9};
                    var prepared=first.prepareSequence(identities.get(0),UUID.randomUUID(),0,71,List.of(new Keys(Buf.wrap(key))),List.of(Buf.wrap(value)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch());key[0]=3;value[0]=8;firstToken=prepared.get(10,TimeUnit.SECONDS);
                    secondToken=RatisRocksDBDirectTest.prepare(second,71,List.of(RatisAtomicClientTest.key(4)),List.of(RatisAtomicClientTest.value(7)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false);
                    var left=first.execute(firstToken,RequestContext.batch());var leftDrained=first.actualCompletion(left);var right=second.execute(secondToken,RequestContext.batch());drains.add(leftDrained);drains.add(second.actualCompletion(right));
                    var a=calls.poll(10,TimeUnit.SECONDS);var b=calls.poll(10,TimeUnit.SECONDS);assertNotNull(a);assertNotNull(b,"One WRITE worker must release while the first real RPC is unresolved");assertEquals(a.route().groupId(),b.route().groupId());assertNotEquals(a.command().operationId().sessionId(),b.command().operationId().sessionId());assertEquals(2,budget.retainedRequests());
                    long charged=budget.chargedBytes();assertTrue(left.cancel(false));assertFalse(a.reply().isCancelled());assertFalse(b.reply().isCancelled());assertFalse(leftDrained.isDone());assertEquals(charged,budget.chargedBytes());
                    Submitted secondCall=a.command().operationId().sessionId().equals(secondToken.ownerId())?a:b,firstCall=secondCall==a?b:a;
                    secondCall.reply().complete(transport.commandAsync(secondCall.route(),secondCall.command()).get(10,TimeUnit.SECONDS));right.get(10,TimeUnit.SECONDS);second.acknowledgeResult(secondToken,second.status(secondToken).resultDigest());assertEquals(1,budget.retainedRequests());assertFalse(leftDrained.isDone());
                    firstCall.reply().complete(transport.commandAsync(firstCall.route(),firstCall.command()).get(10,TimeUnit.SECONDS));leftDrained.get(10,TimeUnit.SECONDS);assertEquals(RemoteMutationStatus.State.TERMINAL,first.status(firstToken).state());first.acknowledgeResult(firstToken,first.status(firstToken).resultDigest());assertEquals(0,budget.retainedRequests());assertArrayEquals(new byte[]{9},f.get(71,RatisAtomicClientTest.key(2)));assertNull(f.get(71,RatisAtomicClientTest.key(3)));
                }
                try(var cold=new RatisPointOwnerManager(f.schema,f.directory,routed,f.scheduler,ownersRoot,budget,sessions,f.catalogue.epochs,f.catalogue.epochs.catalogue(f.outer.directoryColumn),header->{throw new IOException("No reroute in this workflow");},f.homes)) {
                    managers.add(cold);assertEquals(identities.stream().map(RemoteMutationOwner::ownerId).toList(),cold.available().stream().map(RemoteMutationOwner::ownerId).toList());assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,cold.select(firstToken).status(firstToken).state());assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,cold.select(secondToken).status(secondToken).state());assertThrows(ExecutionException.class,()->cold.select(firstToken).execute(firstToken,RequestContext.batch()).get(10,TimeUnit.SECONDS));assertNull(calls.poll());
                    var actor=cold.select(firstToken);var retained=RatisRocksDBDirectTest.prepare(actor,71,List.of(RatisAtomicClientTest.key(6)),List.of(RatisAtomicClientTest.value(5)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false);Submitted lost;
                    try(var competing=budget.work(budget.limits().maximumBytes()-budget.chargedBytes()-(1L<<20))) {
                        var unknown=actor.execute(retained,RequestContext.batch());drains.add(actor.actualCompletion(unknown));lost=calls.poll(10,TimeUnit.SECONDS);assertNotNull(lost);lost.reply().completeExceptionally(new IOException("Lost submit before native acceptance"));assertThrows(ExecutionException.class,()->unknown.get(10,TimeUnit.SECONDS));assertEquals(RemoteMutationStatus.State.PENDING,actor.status(retained).state());assertEquals(1,budget.retainedRequests());assertNull(f.get(71,RatisAtomicClientTest.key(6)));
                    }
                    var retry=actor.execute(retained,RequestContext.batch());drains.add(actor.actualCompletion(retry));var same=calls.poll(10,TimeUnit.SECONDS);assertNotNull(same);assertArrayEquals(DistributedCommandCodec.encode(lost.command()),DistributedCommandCodec.encode(same.command()));same.reply().complete(transport.commandAsync(same.route(),same.command()).get(10,TimeUnit.SECONDS));retry.get(10,TimeUnit.SECONDS);actor.acknowledgeResult(retained,actor.status(retained).resultDigest());assertEquals(0,budget.retainedRequests());assertArrayEquals(new byte[]{5},f.get(71,RatisAtomicClientTest.key(6)));
                    var uncertain=RatisRocksDBDirectTest.prepare(actor,71,List.of(RatisAtomicClientTest.key(8)),List.of(RatisAtomicClientTest.value(3)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false);var writing=actor.execute(uncertain,RequestContext.batch());drains.add(actor.actualCompletion(writing));var submitted=calls.poll(10,TimeUnit.SECONDS);assertNotNull(submitted);submitted.reply().complete(transport.commandAsync(submitted.route(),submitted.command()).get(10,TimeUnit.SECONDS));writing.get(10,TimeUnit.SECONDS);byte[] digest=actor.status(uncertain).resultDigest();actor.afterRetirementForce(()->{throw new IllegalStateException("Lost retirement force reply");});assertThrows(IllegalStateException.class,()->actor.acknowledgeResult(uncertain,digest));assertEquals(List.of(secondToken.ownerId()),cold.available().stream().map(RemoteMutationOwner::ownerId).toList());assertSame(cold.all().get(1),cold.select(identities.get(1)));assertEquals(1,budget.retainedRequests(),"Uncertain floor retains input credit until cold resolution");

                }
                }catch(Exception|Error failure){problem=failure;throw failure;}finally{
                    for(var submitted:replies)submitted.reply().completeExceptionally(new java.io.InterruptedIOException("Pipeline fixture cleanup"));
                    try{CompletableFuture.allOf(drains.toArray(CompletableFuture[]::new)).get(20,TimeUnit.SECONDS);for(var manager:managers)manager.close();}catch(Exception cleanup){if(problem!=null)problem.addSuppressed(cleanup);else throw cleanup;}
                }
            }
        }
    }
    @Test void sharedEnvelopeRejectsBeforeCopyAndRetainedOwnerConstruction()throws Exception {
        var limits=new RemoteMutationOwnerLimits(2,2,0,0,2,32L<<20,512L<<20,512,16L<<20,8L<<20,1);
        try(var budget=new RatisRequestBudget(limits);var first=budget.owner(RemoteMutationToken.Kind.POINT,1024,0);var second=budget.owner(RemoteMutationToken.Kind.POINT,1024,0)) {
            first.opened(0,false);second.opened(0,false);long used=budget.chargedBytes();assertThrows(RocksDBException.class,()->budget.owner(RemoteMutationToken.Kind.POINT,1024,0));assertThrows(RocksDBException.class,()->budget.request(RemoteMutationToken.Kind.POINT,new RatisRequestBudget.Geometry(2L<<20,2L<<20,2L<<20,2L<<20,2L<<20,1).bytes()));assertEquals(used,budget.chargedBytes());assertEquals(0,budget.retainedRequests());
        }
    }
}
