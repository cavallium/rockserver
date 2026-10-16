package it.cavallium.rockserver.core.server;

import com.google.protobuf.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.cdc.DistributedCdcWire;
import it.cavallium.rockserver.core.common.export.ExportWire;
import it.cavallium.rockserver.core.common.schema.*;
import it.cavallium.rockserver.core.common.schema.SchemaOperations;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Lazy borrowed backend; RPC cancellation never releases the retained schema owner. */
public final class SchemaOperationsGrpcService extends SchemaOperationsServiceGrpc.SchemaOperationsServiceImplBase {
    private final Supplier<SchemaOperationsAsyncAPI> backend;
    public SchemaOperationsGrpcService(Supplier<SchemaOperationsAsyncAPI> backend){this.backend=Objects.requireNonNull(backend);}
    public ServerServiceDefinition bindBoundedService(){
        var generated=bindService();var descriptor=ServiceDescriptor.newBuilder(generated.getServiceDescriptor().getName()).setSchemaDescriptor(generated.getServiceDescriptor().getSchemaDescriptor());var methods=new java.util.ArrayList<ServerMethodDefinition<?,?>>();for(var method:generated.getMethods()){var bounded=bounded(method);methods.add(bounded);descriptor.addMethod(bounded.getMethodDescriptor());}var builder=ServerServiceDefinition.builder(descriptor.build());methods.forEach(method->add(builder,method));return builder.build();
    }
    private static <Q,R> void add(ServerServiceDefinition.Builder builder,ServerMethodDefinition<Q,R> method){builder.addMethod(method);}
    private static <Q,R> ServerMethodDefinition<Q,R> bounded(ServerMethodDefinition<Q,R> method){
        var descriptor=method.getMethodDescriptor();var delegate=descriptor.getRequestMarshaller();var bounded=new MethodDescriptor.Marshaller<Q>(){public java.io.InputStream stream(Q value){return delegate.stream(value);}public Q parse(java.io.InputStream input){final byte[] bytes;try{bytes=input.readNBytes(SchemaOperations.MAX_FRAME_BYTES+1);}catch(java.io.IOException failure){throw Status.INTERNAL.withCause(failure).asRuntimeException();}if(bytes.length>SchemaOperations.MAX_FRAME_BYTES)throw Status.RESOURCE_EXHAUSTED.withDescription("Schema operation request exceeds byte bound").asRuntimeException();return delegate.parse(new java.io.ByteArrayInputStream(bytes));}};return ServerMethodDefinition.create(descriptor.toBuilder(bounded,descriptor.getResponseMarshaller()).build(),method.getServerCallHandler());
    }
    private <T,R extends MessageLite>void invoke(MessageLite request,boolean hasContext,it.cavallium.rockserver.core.common.api.proto.RequestContext wire,StreamObserver<R> observer,BiFunction<SchemaOperationsAsyncAPI,RequestContext,CompletableFuture<T>> call,Function<T,R> mapper){
        var grpc=Context.current();try{
            SchemaOperationsWire.bounded(request);if(!hasContext)throw new IllegalArgumentException("Schema operation context is required");var context=DistributedCdcWire.context(wire);
            if(grpc.isCancelled())throw Status.CANCELLED.asRuntimeException();var deadline=grpc.getDeadline();if(deadline!=null){long remaining=Math.min(context.timeoutNanos(),deadline.timeRemaining(TimeUnit.NANOSECONDS));if(remaining<=0)throw Status.DEADLINE_EXCEEDED.asRuntimeException();context=new RequestContext(context.profile(),remaining);}
            var source=call.apply(Objects.requireNonNull(backend.get()),context);
            source.whenComplete((value,failure)->{if(grpc.isCancelled())return;if(failure!=null){observer.onError(ExportWire.serverError(failure));return;}try{var reply=SchemaOperationsWire.bounded(mapper.apply(Objects.requireNonNull(value)));observer.onNext(reply);observer.onCompleted();}catch(Throwable error){observer.onError(ExportWire.serverError(error));}});
        }catch(Throwable failure){observer.onError(ExportWire.serverError(failure));}
    }
    @Override public void inventory(SchemaOperationInventoryRequest request,StreamObserver<SchemaOperationInventory> reply){this.<List<SchemaOperations.Status>,SchemaOperationInventory>invoke(request,request.hasContext(),request.getContext(),reply,SchemaOperationsAsyncAPI::inventory,SchemaOperationsWire::inventory);}
    private void act(SchemaOperationRequest request,StreamObserver<SchemaOperationStatus> reply,int action){this.<SchemaOperations.Status,SchemaOperationStatus>invoke(request,request.hasContext(),request.getContext(),reply,(api,context)->{var id=DistributedCdcWire.uuid(request.getRequestId());return switch(action){case 0->api.status(id,context);case 1->api.resume(id,context);case 2->api.abort(id,context);default->throw new AssertionError();};},value->{var status=SchemaOperationsWire.status(SchemaOperations.require(value,DistributedCdcWire.uuid(request.getRequestId())));return status;});}
    @Override public void status(SchemaOperationRequest request,StreamObserver<SchemaOperationStatus> reply){act(request,reply,0);}
    @Override public void resume(SchemaOperationRequest request,StreamObserver<SchemaOperationStatus> reply){act(request,reply,1);}
    @Override public void abort(SchemaOperationRequest request,StreamObserver<SchemaOperationStatus> reply){act(request,reply,2);}
}
