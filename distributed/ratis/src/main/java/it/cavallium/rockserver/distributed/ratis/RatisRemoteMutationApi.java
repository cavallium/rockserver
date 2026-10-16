package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

/** Common mutation lifecycle over a borrowed connection. Each composite operation shares one deadline. */
public final class RatisRemoteMutationApi implements RemoteMutationAsyncAPI {
    private final RatisRocksDBConnection connection;
    private final RWScheduler scheduler;
    RatisRemoteMutationApi(RatisRocksDBConnection connection,RWScheduler scheduler){this.connection=Objects.requireNonNull(connection);this.scheduler=Objects.requireNonNull(scheduler);}
    @Override public CompletableFuture<List<RemoteMutationOwner>> atomicOwners(List<Long> columns,RequestContext context){return connection.atomicMutationOwners(columns,context);}
    @Override public CompletableFuture<RemoteMutationToken> prepareAtomic(RemoteMutationOwner owner,UUID id,long sequence,List<AtomicMutation> mutations,List<AtomicPredicate> predicates,long ttlMillis,RequestContext context){return connection.prepareAtomicMutation(owner,id,sequence,mutations,predicates,ttlMillis,context);}
    @Override public CompletableFuture<List<RemoteMutationOwner>> owners(long column,RequestContext context){return connection.remoteMutationOwners(column,context);}
    @Override public CompletableFuture<RemoteMutationToken> preparePoint(RemoteMutationOwner owner,UUID id,long sequence,long column,List<Keys> keys,List<Buf> values,Mutation.Kind kind,RequestType.RequestTypeId callback,boolean multi,RequestContext context){
        try {
            var write=switch(kind){case PUT -> RatisRocksDBDirect.WriteKind.PUT;case DELETE -> RatisRocksDBDirect.WriteKind.DELETE;case MERGE -> RatisRocksDBDirect.WriteKind.MERGE;default -> throw new IllegalArgumentException("Point input requires PUT, DELETE or MERGE");};
            if(keys.isEmpty() || keys.size()>MAX_POINT_ITEMS || !multi && keys.size()!=1 || write==RatisRocksDBDirect.WriteKind.DELETE && !values.isEmpty() || write!=RatisRocksDBDirect.WriteKind.DELETE && values.size()!=keys.size())throw new IllegalArgumentException("Invalid point input count");
            return connection.preparePointMutation(owner,id,sequence,column,keys,values,write,callback,multi,context);
        }catch(Exception failure){return CompletableFuture.failedFuture(failure);}
    }
    @Override public CompletableFuture<RemoteMutationToken> prepareRange(RemoteMutationOwner owner,UUID id,long sequence,long column,Keys lower,Keys upper,RequestContext context){try{return connection.prepareRangeMutation(owner,id,sequence,column,copyBound(lower),copyBound(upper),context);}catch(Exception failure){return CompletableFuture.failedFuture(failure);}}
    @Override public CompletableFuture<RemoteMutationToken> prepareBulk(RemoteMutationOwner owner,UUID id,long sequence,long column,boolean merge,RequestContext context){return connection.prepareBulkMutation(owner,id,sequence,column,merge,context);}
    @Override public CompletableFuture<RemoteMutationStatus> execute(RemoteMutationToken token,RequestContext context){return statusAfter(token,context,remaining -> connection.executeRemoteMutation(token,remaining),true);}
    @Override public CompletableFuture<RemoteMutationStatus> resume(RemoteMutationToken token,RequestContext context){return execute(token,context);}
    @Override public CompletableFuture<RemoteMutationStatus> status(RemoteMutationToken token,RequestContext context){return connection.remoteMutationStatus(token,context);}
    @Override public CompletableFuture<RemoteMutationStatus> acknowledge(RemoteMutationToken token,byte[] digest,RequestContext context){try{byte[] frozen=digest.clone();return statusAfter(token,context,remaining -> connection.acknowledgeRemoteMutation(token,frozen,remaining),false);}catch(Exception failure){return CompletableFuture.failedFuture(failure);}}
    @Override public CompletableFuture<RemoteMutationStatus> appendBatch(RemoteMutationToken token,long ordinal,KVBatch batch,byte[] digest,RequestContext context){
        try {if(batch.keys().size()!=batch.values().size() || batch.keys().size()>MAX_BATCH_ITEMS)throw new IllegalArgumentException("Invalid bulk input count");var frozen=copy(batch.keys(),batch.values(),MAX_BATCH_INPUT_BYTES);byte[] hash=digest.clone();return statusAfter(token,context,remaining -> connection.appendRemoteBatch(token,ordinal,frozen,hash,remaining),false);}catch(Exception failure){return CompletableFuture.failedFuture(failure);}
    }
    @Override public CompletableFuture<RemoteMutationStatus> finishInput(RemoteMutationToken token,RequestContext context){return statusAfter(token,context,remaining -> connection.finishRemoteInput(token,remaining),false);}
    @Override public CompletableFuture<RemoteMutationResultPage> results(RemoteMutationToken token,int offset,int maximumItems,int maximumBytes,RequestContext context){
        try {
            if(offset<0 || maximumItems<1 || maximumItems>RemoteMutationResultPage.MAX_ITEMS || maximumBytes<1 || maximumBytes>RemoteMutationResultPage.MAX_ROW_BYTES)throw new IllegalArgumentException("Invalid result page budget");
            var call=new Call<RemoteMutationResultPage>(context);call.observe(connection.remoteMutationStatus(token,call.remaining()),(status,failure) -> {
                if(failure!=null){call.fail(failure);return;}if(!status.progressKnown()){call.fail(new IOException("Original result progress is unavailable: "+status.state()));return;}
                if(offset>status.retainedResults()){call.fail(new IllegalArgumentException("Result offset exceeds observed count"));return;}int count=Math.min(maximumItems,status.retainedResults()-offset);
                if(count==0){call.view.complete(new RemoteMutationResultPage(status,offset,offset,List.of()));return;}
                call.observe(connection.remoteMutationResults(token,offset,count,maximumBytes,call.remaining()),(rows,error) -> {if(error!=null)call.fail(error);else call.view.complete(new RemoteMutationResultPage(status,offset,Math.addExact(offset,rows.size()),rows));});
            });return call.view;
        }catch(Exception failure){return CompletableFuture.failedFuture(failure);}
    }
    private CompletableFuture<RemoteMutationStatus> statusAfter(RemoteMutationToken token,RequestContext context,Function<RequestContext,CompletableFuture<?>> operation,boolean terminalFailureIsOutcome){
        try {
            Objects.requireNonNull(token);var call=new Call<RemoteMutationStatus>(context);call.observe(operation.apply(call.remaining()),(value,failure) -> call.observe(connection.remoteMutationStatus(token,call.remaining()),(status,error) -> {
                if(error!=null){if(failure!=null && error!=failure)failure.addSuppressed(error);call.fail(failure==null?error:failure);}
                else if(failure!=null && (!terminalFailureIsOutcome || status.state()!=RemoteMutationStatus.State.TERMINAL))call.fail(failure);
                else call.view.complete(status);
            }));return call.view;
        }catch(Exception failure){return CompletableFuture.failedFuture(failure);}
    }
    private final class Call<T> {
        final RequestContext context;final long deadline;final CompletableFuture<T> view=new CompletableFuture<>();final AtomicReference<CompletableFuture<?>> active=new AtomicReference<>();
        Call(RequestContext context){this.context=Objects.requireNonNull(context);deadline=scheduler.resolveMonotonicDeadline(context);view.whenComplete((value,failure) -> {if(view.isCancelled()){var stage=active.get();if(stage!=null)stage.cancel(false);}});}
        RequestContext remaining(){if(deadline==Long.MAX_VALUE)return context;long nanos=scheduler.remainingMonotonicDeadlineNanos(deadline);if(nanos<=0)throw new CompletionException(new TimeoutException("Remote mutation request deadline expired"));return new RequestContext(context.profile(),nanos);}
        <V> void observe(CompletableFuture<V> stage,BiConsumer<V,Throwable> completion){active.set(stage);if(view.isDone()){stage.cancel(false);return;}stage.whenComplete((value,failure) -> {if(view.isDone())return;try{completion.accept(value,unwrap(failure));}catch(Throwable error){fail(error);}});}
        void fail(Throwable failure){view.completeExceptionally(unwrap(failure));}
    }
    private static Throwable unwrap(Throwable failure){while(failure instanceof CompletionException || failure instanceof ExecutionException){if(failure.getCause()==null)break;failure=failure.getCause();}return failure;}
    private static Keys copyBound(Keys keys){if(keys==null)return null;return copy(List.of(keys),List.of(),TransactionScope.MAX_BOUND_BYTES).keys().getFirst();}
    private static KVBatch copy(List<Keys> keys,List<Buf> values,long maximumBytes){
        var copiedKeys=new ArrayList<Keys>(keys.size());var copiedValues=new ArrayList<Buf>(values.size());long bytes=0;
        for(var key:keys){var components=Objects.requireNonNull(key).keys();if(components.length>DistributedSchemaCodec.MAX_KEY_COMPONENTS)throw new IllegalArgumentException("Too many logical key components");var parts=new Buf[components.length];for(int n=0;n<parts.length;n++){bytes=Math.addExact(bytes,components[n].size());if(bytes>maximumBytes)throw new IllegalArgumentException("Copied logical input exceeds byte bound");parts[n]=Buf.wrap(components[n].toByteArray());}copiedKeys.add(new Keys(parts));}
        for(var value:values){if(value==null){copiedValues.add(null);continue;}bytes=Math.addExact(bytes,value.size());if(bytes>maximumBytes)throw new IllegalArgumentException("Copied logical input exceeds byte bound");copiedValues.add(Buf.wrap(value.toByteArray()));}
        return new KVBatch.KVBatchRef(List.copyOf(copiedKeys),Collections.unmodifiableList(copiedValues));
    }
}
