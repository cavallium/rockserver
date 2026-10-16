package it.cavallium.rockserver.core.client;

import com.google.common.util.concurrent.*;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.cdc.DistributedCdcWire;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.common.export.ExportWire;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.KVBatch;

/** Borrowed-channel explicit recovery facade. Never chooses identities, retries or acknowledges. */
public final class GrpcRemoteMutationClient implements RemoteMutationAsyncAPI {
    private final RemoteMutationServiceGrpc.RemoteMutationServiceFutureStub stub;
    public GrpcRemoteMutationClient(Channel channel) { stub=RemoteMutationServiceGrpc.newFutureStub(Objects.requireNonNull(channel)).withMaxInboundMessageSize(RemoteMutationWire.MAX_FRAME_BYTES); }
    private <W,T> CompletableFuture<T> invoke(RequestContext context,BiFunction<RemoteMutationServiceGrpc.RemoteMutationServiceFutureStub,it.cavallium.rockserver.core.common.api.proto.RequestContext,ListenableFuture<W>> call,Function<W,T> mapper) {
        try { var bound=BoundRequestContext.bind(context);long remaining=bound.remainingNanos();var wire=DistributedCdcWire.context(context,remaining);var active=remaining==RequestContext.NO_TIMEOUT?stub:stub.withDeadlineAfter(remaining,TimeUnit.NANOSECONDS);var source=call.apply(active,wire);var result=new CompletableFuture<T>();
            Futures.addCallback(source,new FutureCallback<>() {public void onSuccess(W value){try{result.complete(mapper.apply(value));}catch(Throwable failure){result.completeExceptionally(failure);}}public void onFailure(Throwable failure){result.completeExceptionally(ExportWire.clientError(failure));}},MoreExecutors.directExecutor());result.whenComplete((value,failure)->{if(result.isCancelled())source.cancel(false);});return result;
        } catch(Throwable failure){return CompletableFuture.failedFuture(ExportWire.clientError(failure));}
    }
    private static RemoteMutationTokenRequest request(RemoteMutationToken token,it.cavallium.rockserver.core.common.api.proto.RequestContext context){return RemoteMutationTokenRequest.newBuilder().setToken(RemoteMutationWire.token(token)).setContext(context).build();}
    @Override public CompletableFuture<List<RemoteMutationOwner>> owners(long column,RequestContext context){return invoke(context,(s,c)->s.owners(RemoteMutationOwnersRequest.newBuilder().setColumn(column).setContext(c).build()),v->{RemoteMutationWire.bounded(v);if(v.getOwnersCount()>RemoteMutationWire.MAX_ENTRIES)throw new IllegalArgumentException("Too many mutation owners");return v.getOwnersList().stream().map(RemoteMutationWire::owner).toList();});}
    @Override public CompletableFuture<List<RemoteMutationOwner>> atomicOwners(List<Long> columns,RequestContext context){return invoke(context,(s,c)->s.atomicOwners(RemoteMutationAtomicOwnersRequest.newBuilder().addAllColumns(RemoteMutationWire.columns(columns)).setContext(c).build()),v->{RemoteMutationWire.bounded(v);if(v.getOwnersCount()>RemoteMutationWire.MAX_ENTRIES)throw new IllegalArgumentException("Too many atomic owners");return v.getOwnersList().stream().map(w->{var owner=RemoteMutationWire.owner(w);if(owner.kind()!=RemoteMutationToken.Kind.ATOMIC)throw new IllegalArgumentException("Not an atomic owner");return owner;}).toList();});}
    @Override public CompletableFuture<RemoteMutationToken> prepareAtomic(RemoteMutationOwner owner,UUID id,long sequence,List<AtomicMutation> mutations,List<AtomicPredicate> predicates,long ttl,RequestContext context){
        final byte[][] digest={null};return invoke(context,(s,c)->{if(owner.kind()!=RemoteMutationToken.Kind.ATOMIC || sequence<0)throw new IllegalArgumentException("Invalid atomic owner/sequence");RemoteMutationInputs.atomicSize(mutations,predicates,ttl);var request=RemoteMutationWire.bounded(RemoteMutationPrepareAtomicRequest.newBuilder().setOwner(RemoteMutationWire.owner(owner)).setRequestId(DistributedCdcWire.uuid(id)).setRequestSequence(sequence).addAllMutations(RemoteMutationWire.mutations(mutations)).addAllPredicates(RemoteMutationWire.predicates(predicates)).setTtlMillis(ttl).setContext(c).build());var frozen=RemoteMutationWire.atomic(request.getMutationsList(),request.getPredicatesList(),ttl);digest[0]=RemoteMutationInputs.atomic(frozen.mutations(),frozen.predicates(),ttl);return s.prepareAtomic(request);},v->RemoteMutationWire.prepared(v,owner,id,sequence,digest[0]));
    }
    @Override public CompletableFuture<RemoteMutationToken> preparePoint(RemoteMutationOwner owner,UUID id,long sequence,long column,List<Keys> keys,List<Buf> values,Mutation.Kind kind,RequestType.RequestTypeId callback,boolean multi,RequestContext context){
        final byte[][] input={null};
        return invoke(context,(s,c)->{
            var request=RemoteMutationWire.bounded(RemoteMutationPreparePointRequest.newBuilder().setOwner(RemoteMutationWire.owner(owner)).setRequestId(DistributedCdcWire.uuid(id)).setRequestSequence(sequence).setColumn(column).addAllEntries(RemoteMutationWire.entries(keys,values)).setValuesPresent(!values.isEmpty()).setKind(kind.name()).setCallback(callback.name()).setMulti(multi).setContext(c).build());
            input[0]=RemoteMutationInputs.point(column,kind.ordinal(),callback,multi,RemoteMutationWire.keys(request.getEntriesList()),RemoteMutationWire.values(request.getEntriesList(),request.getValuesPresent()));
            return s.preparePoint(request);
        },v->RemoteMutationWire.prepared(v,owner,id,sequence,input[0]));
    }
    @Override public CompletableFuture<RemoteMutationToken> prepareRange(RemoteMutationOwner owner,UUID id,long sequence,long column,Keys lower,Keys upper,RequestContext context){
        final byte[][] input={null};
        return invoke(context,(s,c)->{
            var b=RemoteMutationPrepareRangeRequest.newBuilder().setOwner(RemoteMutationWire.owner(owner)).setRequestId(DistributedCdcWire.uuid(id)).setRequestSequence(sequence).setColumn(column).setContext(c);if(lower!=null)b.setLower(RemoteMutationWire.key(lower));if(upper!=null)b.setUpper(RemoteMutationWire.key(upper));var request=RemoteMutationWire.bounded(b.build());
            input[0]=RemoteMutationInputs.range(column,request.hasLower()?RemoteMutationWire.key(request.getLower()):null,request.hasUpper()?RemoteMutationWire.key(request.getUpper()):null);
            return s.prepareRange(request);
        },v->RemoteMutationWire.prepared(v,owner,id,sequence,input[0]));
    }
    @Override public CompletableFuture<RemoteMutationToken> prepareBulk(RemoteMutationOwner owner,UUID id,long sequence,long column,boolean merge,RequestContext context){return invoke(context,(s,c)->s.prepareBulk(RemoteMutationPrepareBulkRequest.newBuilder().setOwner(RemoteMutationWire.owner(owner)).setRequestId(DistributedCdcWire.uuid(id)).setRequestSequence(sequence).setColumn(column).setMerge(merge).setContext(c).build()),v->RemoteMutationWire.prepared(v,owner,id,sequence,RemoteMutationInputs.bulkHeader(column,merge)));}
    @Override public CompletableFuture<RemoteMutationStatus> execute(RemoteMutationToken token,RequestContext context){return invoke(context,(s,c)->s.execute(request(token,c)),v->RemoteMutationWire.status(v,token));}
    @Override public CompletableFuture<RemoteMutationStatus> resume(RemoteMutationToken token,RequestContext context){return invoke(context,(s,c)->s.resume(request(token,c)),v->RemoteMutationWire.status(v,token));}
    @Override public CompletableFuture<RemoteMutationStatus> status(RemoteMutationToken token,RequestContext context){return invoke(context,(s,c)->s.status(request(token,c)),v->RemoteMutationWire.status(v,token));}
    @Override public CompletableFuture<RemoteMutationStatus> finishInput(RemoteMutationToken token,RequestContext context){return invoke(context,(s,c)->s.finishInput(request(token,c)),v->RemoteMutationWire.status(v,token));}
    @Override public CompletableFuture<RemoteMutationStatus> acknowledge(RemoteMutationToken token,byte[] digest,RequestContext context){return invoke(context,(s,c)->s.acknowledge(RemoteMutationAcknowledgeRequest.newBuilder().setToken(RemoteMutationWire.token(token)).setResultDigest(ByteString.copyFrom(RemoteMutationWire.digest(ByteString.copyFrom(digest)))).setContext(c).build()),v->RemoteMutationWire.status(v,token));}
    @Override public CompletableFuture<RemoteMutationStatus> appendBatch(RemoteMutationToken token,long ordinal,KVBatch batch,byte[] digest,RequestContext context){return invoke(context,(s,c)->{if(ordinal<0)throw new IllegalArgumentException("Negative batch ordinal");return s.appendBatch(RemoteMutationWire.bounded(RemoteMutationAppendRequest.newBuilder().setToken(RemoteMutationWire.token(token)).setOrdinal(ordinal).addAllEntries(RemoteMutationWire.entries(batch.keys(),batch.values())).setInputDigest(ByteString.copyFrom(RemoteMutationWire.digest(ByteString.copyFrom(digest)))).setContext(c).build()));},v->RemoteMutationWire.status(v,token));}
    @Override public CompletableFuture<RemoteMutationResultPage> results(RemoteMutationToken token,int offset,int items,int bytes,RequestContext context){return invoke(context,(s,c)->{RemoteMutationWire.budget(offset,items,bytes);return s.results(RemoteMutationResultsRequest.newBuilder().setToken(RemoteMutationWire.token(token)).setOffset(offset).setMaximumItems(items).setMaximumBytes(bytes).setContext(c).build());},v->RemoteMutationWire.page(v,token,offset,items,bytes));}
}
