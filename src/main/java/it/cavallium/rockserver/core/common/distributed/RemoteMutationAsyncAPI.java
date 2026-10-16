package it.cavallium.rockserver.core.common.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** Durable caller-owned mutation lifecycle. Persist the selected owner, UUID and sequence before prepare. */
public interface RemoteMutationAsyncAPI {
    int MAX_POINT_ITEMS=256,MAX_POINT_INPUT_BYTES=16*1024*1024,MAX_BATCH_ITEMS=4096,MAX_BATCH_INPUT_BYTES=8*1024*1024;
    CompletableFuture<List<RemoteMutationOwner>> owners(long column,RequestContext context);
    /** Discover the complete selected-column authority before durably choosing UUID and sequence. */
    CompletableFuture<List<RemoteMutationOwner>> atomicOwners(List<Long> columns,RequestContext context);
    CompletableFuture<RemoteMutationToken> prepareAtomic(RemoteMutationOwner expectedOwner,UUID requestId,long requestSequence,List<AtomicMutation> mutations,List<AtomicPredicate> predicates,long ttlMillis,RequestContext context);
    /** Caller must persist owner/UUID/sequence before this call and recover that same identity after ambiguity.
     * Results remain retained until explicit acknowledgement; this method never selects a new identity on retry. */
    default CompletableFuture<RemoteMutationStatus> executeAtomic(RemoteMutationOwner owner,UUID requestId,long requestSequence,List<AtomicMutation> mutations,List<AtomicPredicate> predicates,long ttlMillis,RequestContext context){
        java.util.Objects.requireNonNull(context);long opened=System.nanoTime();
        return prepareAtomic(owner,requestId,requestSequence,mutations,predicates,ttlMillis,context).thenCompose(token->{
            if(context.timeoutNanos()==RequestContext.NO_TIMEOUT)return execute(token,context);
            long remaining=context.timeoutNanos()-(System.nanoTime()-opened);
            if(remaining<=0)return CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("Atomic request deadline expired; recover the original prepared identity"));
            return execute(token,new RequestContext(context.profile(),remaining));
        });
    }
    CompletableFuture<RemoteMutationToken> preparePoint(RemoteMutationOwner expectedOwner,UUID requestId,long requestSequence,long column,List<Keys> keys,List<Buf> values,Mutation.Kind kind,RequestType.RequestTypeId callback,boolean multi,RequestContext context);
    CompletableFuture<RemoteMutationToken> prepareRange(RemoteMutationOwner expectedOwner,UUID requestId,long requestSequence,long column,Keys lower,Keys upper,RequestContext context);
    CompletableFuture<RemoteMutationToken> prepareBulk(RemoteMutationOwner expectedOwner,UUID requestId,long requestSequence,long column,boolean merge,RequestContext context);
    /** Execute only the prepared token; returned results require explicit acknowledgement. */
    CompletableFuture<RemoteMutationStatus> execute(RemoteMutationToken token,RequestContext context);
    /** Resume only original retained input, plans, identities and captures. Never resubscribe a bulk source. */
    CompletableFuture<RemoteMutationStatus> resume(RemoteMutationToken token,RequestContext context);
    CompletableFuture<RemoteMutationStatus> status(RemoteMutationToken token,RequestContext context);
    /** Budgets apply to encoded CommandResult rows. Page metadata carries the observed stable count/digest. */
    CompletableFuture<RemoteMutationResultPage> results(RemoteMutationToken token,int offset,int maximumItems,int maximumBytes,RequestContext context);
    CompletableFuture<RemoteMutationStatus> acknowledge(RemoteMutationToken token,byte[] resultDigest,RequestContext context);
    CompletableFuture<RemoteMutationStatus> appendBatch(RemoteMutationToken token,long ordinal,KVBatch batch,byte[] inputDigest,RequestContext context);
    CompletableFuture<RemoteMutationStatus> finishInput(RemoteMutationToken token,RequestContext context);
}
