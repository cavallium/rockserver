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
