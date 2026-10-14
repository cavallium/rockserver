package it.cavallium.rockserver.core.impl.test;

import com.google.protobuf.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.*;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.server.RemoteMutationGrpcService;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.KVBatch;

class RemoteMutationGrpcContractTest {
    private static final RequestContext CONTEXT=RequestContext.ingest();
    private static final byte[] DIGEST=new byte[32];
    static class Backend implements RemoteMutationAsyncAPI {
        final RemoteMutationOwner owner=new RemoteMutationOwner(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),3,7,RemoteMutationToken.Kind.POINT,DIGEST,DIGEST);
        RemoteMutationToken token;RemoteMutationStatus status;int prepares,executes,resumes,acks,calls;CompletableFuture<RemoteMutationStatus> execution;
        void observed(RequestContext c){assertEquals(CONTEXT,c);calls++;}
        void original(RemoteMutationToken t){if(!token.equals(t))throw new IllegalArgumentException("Altered token");}
        @Override public CompletableFuture<List<RemoteMutationOwner>> owners(long column,RequestContext c){observed(c);return CompletableFuture.completedFuture(List.of(owner));}
        @Override public CompletableFuture<RemoteMutationToken> preparePoint(RemoteMutationOwner o,UUID id,long sequence,long column,List<Keys> keys,List<Buf> values,Mutation.Kind kind,RequestType.RequestTypeId callback,boolean multi,RequestContext c){observed(c);if(!owner.sameAuthority(o) || sequence!=owner.nextRequestSequence())throw new IllegalArgumentException("Altered authority/sequence");var input=RemoteMutationInputs.point(column,kind.ordinal(),callback,multi,keys,values);var t=new RemoteMutationToken(o.clusterId(),o.databaseId(),o.domainId(),o.ownerId(),id,o.ownerEpoch(),sequence,o.kind(),o.schemaDigest(),o.directoryDigest(),input);if(token!=null && !token.equals(t))throw new IllegalArgumentException("Altered input");token=t;prepares++;status=new RemoteMutationStatus(t,RemoteMutationStatus.State.PREPARED,0,0,0,null,null);return CompletableFuture.completedFuture(t);}
        @Override public CompletableFuture<RemoteMutationStatus> execute(RemoteMutationToken t,RequestContext c){observed(c);original(t);executes++;status=terminal();execution=new CompletableFuture<>();return execution;}
        RemoteMutationStatus terminal(){return new RemoteMutationStatus(token,RemoteMutationStatus.State.TERMINAL,1,0,1,CommandResult.Status.SUCCESS,DIGEST);}
        @Override public CompletableFuture<RemoteMutationStatus> status(RemoteMutationToken t,RequestContext c){observed(c);original(t);return CompletableFuture.completedFuture(status);}
        @Override public CompletableFuture<RemoteMutationStatus> resume(RemoteMutationToken t,RequestContext c){observed(c);original(t);resumes++;return CompletableFuture.completedFuture(status);}
        @Override public CompletableFuture<RemoteMutationResultPage> results(RemoteMutationToken t,int offset,int items,int bytes,RequestContext c){observed(c);original(t);return CompletableFuture.completedFuture(new RemoteMutationResultPage(status,offset,offset+1,List.of(new CommandResult(CommandResult.Status.SUCCESS,List.of(new byte[]{9})).encode())));}
        @Override public CompletableFuture<RemoteMutationStatus> acknowledge(RemoteMutationToken t,byte[] d,RequestContext c){observed(c);original(t);if(!Arrays.equals(d,DIGEST))throw new IllegalArgumentException("Altered ACK");acks++;status=new RemoteMutationStatus(t,RemoteMutationStatus.State.ACKNOWLEDGED,1,0,1,CommandResult.Status.SUCCESS,DIGEST);return CompletableFuture.completedFuture(status);}
        @Override public CompletableFuture<RemoteMutationToken> prepareRange(RemoteMutationOwner o,UUID id,long seq,long col,Keys lower,Keys upper,RequestContext c){throw new UnsupportedOperationException();}
        @Override public CompletableFuture<RemoteMutationToken> prepareBulk(RemoteMutationOwner o,UUID id,long seq,long col,boolean merge,RequestContext c){throw new UnsupportedOperationException();}
        @Override public CompletableFuture<RemoteMutationStatus> appendBatch(RemoteMutationToken t,long ordinal,KVBatch batch,byte[] d,RequestContext c){throw new UnsupportedOperationException();}
        @Override public CompletableFuture<RemoteMutationStatus> finishInput(RemoteMutationToken t,RequestContext c){throw new UnsupportedOperationException();}
    }
    /** Generated future stub and service, with protobuf round trips but no socket or server. */
    static Channel channel(RemoteMutationGrpcService service){return new Channel(){
        @Override public String authority(){return "direct-test";}
        @Override public <Q,R> ClientCall<Q,R> newCall(MethodDescriptor<Q,R> method,CallOptions options){return new ClientCall<>(){
            Listener<R> listener;Q request;final Context.CancellableContext context=Context.current().withCancellation();
            @Override public void start(Listener<R> l,Metadata headers){listener=l;}
            @Override public void request(int n){}
            @Override public void cancel(String message,Throwable cause){context.cancel(cause);listener.onClose(Status.CANCELLED,new Metadata());}
            @Override public void sendMessage(Q message){request=method.parseRequest(method.streamRequest(message));}
            @Override public void halfClose(){context.run(()->{try{var dispatch=Arrays.stream(service.getClass().getMethods()).filter(m->m.getName().equals(method.getBareMethodName()) && m.getParameterCount()==2).findFirst().orElseThrow();dispatch.invoke(service,request,new StreamObserver<R>(){
                @Override public void onNext(R response){if(!context.isCancelled())listener.onMessage(method.parseResponse(method.streamResponse(response)));}
                @Override public void onError(Throwable error){if(!context.isCancelled())listener.onClose(Status.fromThrowable(error),new Metadata());}
                @Override public void onCompleted(){if(!context.isCancelled())listener.onClose(Status.OK,new Metadata());}
            });}catch(Exception error){listener.onClose(Status.INTERNAL.withCause(error),new Metadata());}});}
        };}
    };}
    static RemoteMutationToken prepare(GrpcRemoteMutationClient client,Backend b,UUID id,List<Buf> values){return client.preparePoint(b.owner,id,7,1,List.of(new Keys(Buf.wrap(new byte[]{1}))),values,Mutation.Kind.PUT,RequestType.RequestTypeId.PREVIOUS,false,CONTEXT).join();}
    @Test void lostReplyRecoversOriginalIdentityAndRequiresExplicitAck(){
        var backend=new Backend();var client=new GrpcRemoteMutationClient(channel(new RemoteMutationGrpcService(backend)));assertEquals(backend.owner,client.owners(1,CONTEXT).join().getFirst());var token=prepare(client,backend,UUID.randomUUID(),List.of(Buf.wrap(new byte[]{2})));assertEquals(7,token.requestSequence());var lost=client.execute(token,CONTEXT);assertTrue(lost.cancel(false));assertFalse(backend.execution.isCancelled());backend.execution.complete(backend.status);
        assertEquals(RemoteMutationStatus.State.TERMINAL,client.status(token,CONTEXT).join().state());assertEquals(token,client.resume(token,CONTEXT).join().token());var page=client.results(token,0,1,32,CONTEXT).join();assertFalse(page.hasMore());assertEquals(9,CommandResult.decode(page.rows().getFirst()).value(0)[0]);assertEquals(0,backend.acks);assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,client.acknowledge(token,DIGEST,CONTEXT).join().state());assertEquals(1,backend.prepares);assertEquals(1,backend.executes);assertEquals(1,backend.resumes);assertEquals(1,backend.acks);
    }
    @Test void alteredInputsAuthoritySequenceAndAckReject(){
        var backend=new Backend();var client=new GrpcRemoteMutationClient(channel(new RemoteMutationGrpcService(backend)));var id=UUID.randomUUID();var token=prepare(client,backend,id,List.of(Buf.wrap(new byte[]{2})));assertThrows(CompletionException.class,()->prepare(client,backend,id,List.of(Buf.wrap(new byte[]{3}))));assertThrows(CompletionException.class,()->client.preparePoint(backend.owner,id,8,1,List.of(new Keys(Buf.wrap(new byte[]{1}))),List.of(Buf.wrap(new byte[]{2})),Mutation.Kind.PUT,RequestType.RequestTypeId.PREVIOUS,false,CONTEXT).join());var o=backend.owner;var crossed=new RemoteMutationOwner(o.clusterId(),o.databaseId(),o.domainId(),UUID.randomUUID(),o.ownerEpoch(),7,o.kind(),o.schemaDigest(),o.directoryDigest());assertThrows(CompletionException.class,()->client.preparePoint(crossed,id,7,1,List.of(new Keys(Buf.wrap(new byte[]{1}))),List.of(Buf.wrap(new byte[]{2})),Mutation.Kind.PUT,RequestType.RequestTypeId.PREVIOUS,false,CONTEXT).join());assertThrows(CompletionException.class,()->client.acknowledge(token,new byte[31],CONTEXT).join());assertEquals(1,backend.prepares);assertEquals(0,backend.executes);assertEquals(0,backend.acks);
    }
    @Test void malformedFramesRejectBeforeEffectsAndPagesStayBounded(){
        var backend=new Backend();var service=new RemoteMutationGrpcService(backend);var reply=new CompletableFuture<RemoteMutationObservation>();service.execute(RemoteMutationTokenRequest.newBuilder().setToken(ByteString.copyFrom(new byte[193])).build(),observer(reply));assertTrue(reply.isCompletedExceptionally());assertEquals(0,backend.calls);
        var client=new GrpcRemoteMutationClient(channel(service));var token=prepare(client,backend,UUID.randomUUID(),List.of(Buf.wrap(new byte[]{2})));backend.status=backend.terminal();var wire=RemoteMutationPage.newBuilder().setStatus(RemoteMutationWire.status(backend.status)).setOffset(0).setNextOffset(1).addRows(ByteString.copyFrom(new byte[]{1})).setHasMore(false).build();assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.page(wire,token,0,1,32));var row=new CommandResult(CommandResult.Status.SUCCESS,List.of(new byte[]{1})).encode();var valid=wire.toBuilder().setRows(0,ByteString.copyFrom(row)).build();assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.page(valid,token,0,1,row.length-1));assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.page(valid.toBuilder().setHasMore(true).build(),token,0,1,32));assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.budget(0,257,32));assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.token(ByteString.copyFrom(new byte[192])));
    }
    @Test void expiredIsUnknownAndSessionExpiredIsTerminal(){var b=new Backend();var client=new GrpcRemoteMutationClient(channel(new RemoteMutationGrpcService(b)));var t=prepare(client,b,UUID.randomUUID(),List.of(Buf.wrap(new byte[]{2})));b.status=new RemoteMutationStatus(t,RemoteMutationStatus.State.EXPIRED,-1,-1,0,null,null);assertFalse(client.status(t,CONTEXT).join().progressKnown());b.status=new RemoteMutationStatus(t,RemoteMutationStatus.State.TERMINAL,1,0,0,CommandResult.Status.SESSION_EXPIRED,DIGEST);assertEquals(CommandResult.Status.SESSION_EXPIRED,client.status(t,CONTEXT).join().outcome());}
    @Test void rangeAndBulkKeepExplicitPreparationAndBatchIdentity(){
        var b=new Backend(){
            final Map<Long,byte[]> batches=new HashMap<>();
            RemoteMutationToken prepared(RemoteMutationOwner o,UUID id,long seq,byte[] input){token=new RemoteMutationToken(o.clusterId(),o.databaseId(),o.domainId(),o.ownerId(),id,o.ownerEpoch(),seq,o.kind(),o.schemaDigest(),o.directoryDigest(),input);status=new RemoteMutationStatus(token,RemoteMutationStatus.State.PREPARED,0,0,0,null,null);prepares++;return token;}
            @Override public CompletableFuture<RemoteMutationToken> prepareRange(RemoteMutationOwner o,UUID id,long seq,long col,Keys lower,Keys upper,RequestContext c){observed(c);assertNull(lower);assertEquals(new Keys(Buf.wrap(new byte[]{5})),upper);return CompletableFuture.completedFuture(prepared(o,id,seq,RemoteMutationInputs.range(col,lower,upper)));}
            @Override public CompletableFuture<RemoteMutationToken> prepareBulk(RemoteMutationOwner o,UUID id,long seq,long col,boolean merge,RequestContext c){observed(c);return CompletableFuture.completedFuture(prepared(o,id,seq,RemoteMutationInputs.bulkHeader(col,merge)));}
            @Override public CompletableFuture<RemoteMutationStatus> appendBatch(RemoteMutationToken t,long ordinal,KVBatch batch,byte[] digest,RequestContext c){observed(c);original(t);if(!Arrays.equals(digest,RemoteMutationInputs.batch(1,false,batch)))throw new IllegalArgumentException("Changed batch input digest");var old=batches.get(ordinal);if(old!=null && !Arrays.equals(old,digest) || old==null && ordinal!=batches.size())throw new IllegalArgumentException("Changed batch ordinal");batches.put(ordinal,digest);status=new RemoteMutationStatus(token,RemoteMutationStatus.State.PENDING,0,batches.size(),0,null,null);return CompletableFuture.completedFuture(status);}
            @Override public CompletableFuture<RemoteMutationStatus> finishInput(RemoteMutationToken t,RequestContext c){observed(c);original(t);return CompletableFuture.completedFuture(status);}
        };
        var client=new GrpcRemoteMutationClient(channel(new RemoteMutationGrpcService(b)));var o=b.owner;var range=new RemoteMutationOwner(o.clusterId(),o.databaseId(),o.domainId(),o.ownerId(),o.ownerEpoch(),7,RemoteMutationToken.Kind.RANGE,DIGEST,DIGEST);assertEquals(RemoteMutationToken.Kind.RANGE,client.prepareRange(range,UUID.randomUUID(),7,1,null,new Keys(Buf.wrap(new byte[]{5})),CONTEXT).join().kind());var bulk=new RemoteMutationOwner(o.clusterId(),o.databaseId(),o.domainId(),o.ownerId(),o.ownerEpoch(),7,RemoteMutationToken.Kind.BULK,DIGEST,DIGEST);var token=client.prepareBulk(bulk,UUID.randomUUID(),7,1,false,CONTEXT).join();var batch=new KVBatch.KVBatchRef(List.of(new Keys(Buf.wrap(new byte[]{1}))),List.of(Buf.wrap(new byte[]{2})));var digest=RemoteMutationInputs.batch(1,false,batch);assertEquals(1,client.appendBatch(token,0,batch,digest,CONTEXT).join().copiedBatches());assertEquals(1,client.appendBatch(token,0,batch,digest,CONTEXT).join().copiedBatches());assertThrows(CompletionException.class,()->client.appendBatch(token,2,batch,digest,CONTEXT).join());assertThrows(CompletionException.class,()->client.appendBatch(token,0,batch,DIGEST,CONTEXT).join());assertEquals(1,client.finishInput(token,CONTEXT).join().copiedBatches());assertEquals(0,b.executes);assertEquals(0,b.acks);
    }
    @Test void callerAndTransportDeadlineAreCombinedBeforeBackendAdmission(){
        var scheduler=Executors.newSingleThreadScheduledExecutor();try{
            var b=new Backend(){@Override public CompletableFuture<List<RemoteMutationOwner>> owners(long column,RequestContext c){assertEquals(WorkloadProfile.INGEST,c.profile());assertTrue(c.timeoutNanos()>0);assertTrue(c.timeoutNanos()<=TimeUnit.SECONDS.toNanos(1));calls++;return CompletableFuture.completedFuture(List.of(owner));}};
            var service=new RemoteMutationGrpcService(b);var result=new CompletableFuture<RemoteMutationOwnersResponse>();var context=Context.current().withDeadlineAfter(1,TimeUnit.SECONDS,scheduler);try{context.run(()->service.owners(RemoteMutationOwnersRequest.newBuilder().setColumn(1).setContext(it.cavallium.rockserver.core.common.cdc.DistributedCdcWire.context(RequestContext.ingest(java.time.Duration.ofSeconds(5)),TimeUnit.SECONDS.toNanos(5))).build(),observer(result)));assertEquals(1,result.join().getOwnersCount());assertEquals(1,b.calls);}finally{context.cancel(null);}
            var method=service.bindBoundedService().getMethods().stream().filter(m->m.getMethodDescriptor().getBareMethodName().equals("owners")).findFirst().orElseThrow();assertThrows(StatusRuntimeException.class,()->method.getMethodDescriptor().getRequestMarshaller().parse(new java.io.ByteArrayInputStream(new byte[RemoteMutationWire.MAX_FRAME_BYTES+1])));
        }finally{scheduler.shutdownNow();}
    }
    static <T> StreamObserver<T> observer(CompletableFuture<T> result){return new StreamObserver<>(){public void onNext(T value){result.complete(value);}public void onError(Throwable error){result.completeExceptionally(error);}public void onCompleted(){}};}
}
