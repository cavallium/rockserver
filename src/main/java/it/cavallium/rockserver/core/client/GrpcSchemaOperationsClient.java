package it.cavallium.rockserver.core.client;

import com.google.common.util.concurrent.*;
import io.grpc.Channel;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.cdc.DistributedCdcWire;
import it.cavallium.rockserver.core.common.export.ExportWire;
import it.cavallium.rockserver.core.common.schema.*;
import it.cavallium.rockserver.core.common.schema.SchemaOperations;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Borrowed channel; cancellation detaches RPC observation, never replaces a retained request. */
public final class GrpcSchemaOperationsClient implements SchemaOperationsAsyncAPI {
    private final SchemaOperationsServiceGrpc.SchemaOperationsServiceFutureStub stub;
    public GrpcSchemaOperationsClient(Channel channel){stub=SchemaOperationsServiceGrpc.newFutureStub(Objects.requireNonNull(channel)).withMaxInboundMessageSize(SchemaOperations.MAX_FRAME_BYTES);}
    private <W,T>CompletableFuture<T> invoke(RequestContext context,BiFunction<SchemaOperationsServiceGrpc.SchemaOperationsServiceFutureStub,it.cavallium.rockserver.core.common.api.proto.RequestContext,ListenableFuture<W>> call,Function<W,T> mapper){
        try{var bound=BoundRequestContext.bind(context);long remaining=bound.remainingNanos();var active=remaining==RequestContext.NO_TIMEOUT?stub:stub.withDeadlineAfter(remaining,TimeUnit.NANOSECONDS);var source=call.apply(active,DistributedCdcWire.context(context,remaining));var result=new CompletableFuture<T>();
            Futures.addCallback(source,new FutureCallback<>(){public void onSuccess(W value){try{result.complete(mapper.apply(value));}catch(Throwable failure){result.completeExceptionally(failure);}}public void onFailure(Throwable failure){result.completeExceptionally(ExportWire.clientError(failure));}},MoreExecutors.directExecutor());result.whenComplete((value,failure)->{if(result.isCancelled())source.cancel(false);});return result;
        }catch(Throwable failure){return CompletableFuture.failedFuture(ExportWire.clientError(failure));}
    }
    private static SchemaOperationRequest request(UUID id,it.cavallium.rockserver.core.common.api.proto.RequestContext context){return SchemaOperationRequest.newBuilder().setRequestId(DistributedCdcWire.uuid(Objects.requireNonNull(id))).setContext(context).build();}
    public CompletableFuture<List<SchemaOperations.Status>> inventory(RequestContext context){return invoke(context,(s,c)->s.inventory(SchemaOperationInventoryRequest.newBuilder().setContext(c).build()),SchemaOperationsWire::inventory);}
    public CompletableFuture<SchemaOperations.Status> status(UUID id,RequestContext context){return invoke(context,(s,c)->s.status(request(id,c)),v->SchemaOperationsWire.status(v,id));}
    public CompletableFuture<SchemaOperations.Status> resume(UUID id,RequestContext context){return invoke(context,(s,c)->s.resume(request(id,c)),v->SchemaOperationsWire.status(v,id));}
    public CompletableFuture<SchemaOperations.Status> abort(UUID id,RequestContext context){return invoke(context,(s,c)->s.abort(request(id,c)),v->SchemaOperationsWire.status(v,id));}
}
