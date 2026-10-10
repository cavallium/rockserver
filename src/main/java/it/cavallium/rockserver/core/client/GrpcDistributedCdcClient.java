package it.cavallium.rockserver.core.client;

import com.google.common.util.concurrent.*;
import io.grpc.Channel;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.api.cdc.proto.*;
import it.cavallium.rockserver.core.common.cdc.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.common.export.ExportWire;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Vector CDC client; its channel remains borrowed. Future cancellation detaches the RPC observation. */
public final class GrpcDistributedCdcClient implements DistributedCdcAsyncAPI {
    private final DistributedCdcServiceGrpc.DistributedCdcServiceFutureStub stub;
    public GrpcDistributedCdcClient(Channel channel) { stub=DistributedCdcServiceGrpc.newFutureStub(Objects.requireNonNull(channel)).withMaxInboundMessageSize(DistributedCdcWire.MAX_FRAME_BYTES); }
    private <W,T> CompletableFuture<T> invoke(RequestContext context,
            BiFunction<DistributedCdcServiceGrpc.DistributedCdcServiceFutureStub,it.cavallium.rockserver.core.common.api.proto.RequestContext,ListenableFuture<W>> call,Function<W,T> mapper) {
        try {
            var bound=BoundRequestContext.bind(context); long remaining=bound.remainingNanos(); var wire=DistributedCdcWire.context(context,remaining);
            var active=remaining==RequestContext.NO_TIMEOUT ? stub : stub.withDeadlineAfter(remaining,TimeUnit.NANOSECONDS); var source=call.apply(active,wire); var result=new CompletableFuture<T>();
            Futures.addCallback(source,new FutureCallback<>() {
                public void onSuccess(W value) { try { result.complete(mapper.apply(value)); } catch(Throwable error) { result.completeExceptionally(error); } }
                public void onFailure(Throwable error) { result.completeExceptionally(ExportWire.clientError(error)); }
            },MoreExecutors.directExecutor()); result.whenComplete((value,error)->{ if(result.isCancelled()) source.cancel(false); }); return result;
        } catch(Throwable error) { return CompletableFuture.failedFuture(ExportWire.clientError(error)); }
    }
    private CdcSubscriptionRequest subscription(UUID id,it.cavallium.rockserver.core.common.api.proto.RequestContext context) { return CdcSubscriptionRequest.newBuilder().setSubscriptionId(DistributedCdcWire.uuid(id)).setContext(context).build(); }
    @Override public CompletableFuture<RangeDirectory> context(UUID id,RequestContext context) { return invoke(context,(s,c)->s.context(CdcContextRequest.newBuilder().setDomainId(DistributedCdcWire.uuid(id)).setContext(c).build()),v->DistributedCdcWire.directory(v,id)); }
    @Override public CompletableFuture<DistributedCdcSubscriptions.Status> create(DistributedCdcSubscription.Definition definition,RequestContext context) { return invoke(context,(s,c)->s.create(DistributedCdcWire.bounded(CdcCreateRequest.newBuilder().setDefinition(DistributedCdcWire.definition(definition)).setContext(c).build(),DistributedCdcWire.MAX_REQUEST_BYTES)),v->DistributedCdcWire.status(v,definition)); }
    @Override public CompletableFuture<DistributedCdcSubscriptions.Status> recover(UUID id,RequestContext context) { return invoke(context,(s,c)->s.recover(subscription(id,c)),v->DistributedCdcWire.status(v,id)); }
    @Override public CompletableFuture<DistributedCdcSubscriptions.Status> status(UUID id,RequestContext context) { return invoke(context,(s,c)->s.status(subscription(id,c)),v->DistributedCdcWire.status(v,id)); }
    @Override public CompletableFuture<DistributedCdcSubscriptions.Poll> poll(UUID id,int records,int bytes,RequestContext context) { return invoke(context,(s,c)->{ DistributedCdcWire.pollBudget(records,bytes); return s.poll(CdcPollRequest.newBuilder().setSubscriptionId(DistributedCdcWire.uuid(id)).setMaxRecords(records).setMaxBytes(bytes).setContext(c).build()); },v->DistributedCdcWire.poll(v,id,records,bytes)); }
    @Override public CompletableFuture<DistributedCdcSubscriptions.Status> acknowledge(UUID id,UUID batch,RequestContext context) { return invoke(context,(s,c)->s.acknowledge(CdcAcknowledgeRequest.newBuilder().setSubscriptionId(DistributedCdcWire.uuid(id)).setBatchId(DistributedCdcWire.uuid(batch)).setContext(c).build()),v->DistributedCdcWire.status(v,id)); }
    @Override public CompletableFuture<DistributedCdcSubscriptions.Status> closeSubscription(UUID id,RequestContext context) { return invoke(context,(s,c)->s.closeSubscription(subscription(id,c)),v->DistributedCdcWire.status(v,id)); }
}
