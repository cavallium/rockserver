package it.cavallium.rockserver.core.server;

import com.google.protobuf.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import it.cavallium.rockserver.core.client.*;
import it.cavallium.rockserver.core.common.ReplicaPhysicalAPI;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.export.ExportWire;
import java.util.concurrent.*;
import java.util.function.*;

/** Borrowed operator authority. Cancellation/deadline detaches observation, retaining native ownership. */
public final class ReplicaPhysicalGrpcService extends ReplicaPhysicalServiceGrpc.ReplicaPhysicalServiceImplBase {
    private final ReplicaPhysicalAPI backend;
    public ReplicaPhysicalGrpcService(ReplicaPhysicalAPI backend){this.backend=backend;}
    public ServerServiceDefinition bindBoundedService(){var generated=bindService();var descriptor=ServiceDescriptor.newBuilder(generated.getServiceDescriptor().getName()).setSchemaDescriptor(generated.getServiceDescriptor().getSchemaDescriptor());var methods=new java.util.ArrayList<ServerMethodDefinition<?,?>>();for(var method:generated.getMethods()){var bounded=bounded(method);methods.add(bounded);descriptor.addMethod(bounded.getMethodDescriptor());}var service=ServerServiceDefinition.builder(descriptor.build());methods.forEach(method->add(service,method));return service.build();}
    private static <Q,R> void add(ServerServiceDefinition.Builder builder,ServerMethodDefinition<Q,R> method){builder.addMethod(method);}
    private static <Q,R> ServerMethodDefinition<Q,R> bounded(ServerMethodDefinition<Q,R> method){var descriptor=method.getMethodDescriptor();var delegate=descriptor.getRequestMarshaller();var bounded=new MethodDescriptor.Marshaller<Q>(){public java.io.InputStream stream(Q value){return delegate.stream(value);}public Q parse(java.io.InputStream input){final byte[] bytes;try{bytes=input.readNBytes(ReplicaPhysicalWire.MAX_REQUEST_BYTES+1);}catch(java.io.IOException error){throw Status.INTERNAL.withCause(error).asRuntimeException();}if(bytes.length>ReplicaPhysicalWire.MAX_REQUEST_BYTES)throw Status.RESOURCE_EXHAUSTED.withDescription("Physical request exceeds byte budget").asRuntimeException();return delegate.parse(new java.io.ByteArrayInputStream(bytes));}};return ServerMethodDefinition.create(descriptor.toBuilder(bounded,descriptor.getResponseMarshaller()).build(),method.getServerCallHandler());}
    private <T,R extends MessageLite> void invoke(MessageLite request,boolean hasContext,ReplicaPhysicalContext context,StreamObserver<R> observer,Supplier<CompletableFuture<T>> call,Function<T,R> mapper){
        var grpc=Context.current();var view=new CompletableFuture<R>();
        Context.CancellationListener cancelled=c->view.completeExceptionally(Status.CANCELLED.asRuntimeException());
        view.whenComplete((value,error)->{grpc.removeListener(cancelled);if(grpc.isCancelled())return;if(error!=null)observer.onError(error instanceof TimeoutException?Status.DEADLINE_EXCEEDED.asRuntimeException():ExportWire.serverError(error));else{observer.onNext(value);observer.onCompleted();}});
        try{ReplicaPhysicalWire.bounded(request,ReplicaPhysicalWire.MAX_REQUEST_BYTES);if(!hasContext)throw new IllegalArgumentException("Missing physical operator context");long remaining=ReplicaPhysicalWire.timeout(context);var deadline=grpc.getDeadline();if(deadline!=null)remaining=Math.min(remaining,deadline.timeRemaining(TimeUnit.NANOSECONDS));if(remaining<=0)throw Status.DEADLINE_EXCEEDED.asRuntimeException();if(backend==null)throw Status.UNIMPLEMENTED.withDescription("No physical operator authority injected").asRuntimeException();grpc.addListener(cancelled,Runnable::run);if(grpc.isCancelled())return;if(remaining!=Long.MAX_VALUE)view.orTimeout(remaining,TimeUnit.NANOSECONDS);
            if(view.isDone())return;call.get().whenComplete((value,error)->{if(view.isDone())return;if(error!=null){view.completeExceptionally(error);return;}try{view.complete(ReplicaPhysicalWire.bounded(mapper.apply(value),ReplicaPhysicalWire.MAX_FRAME_BYTES));}catch(Throwable failure){view.completeExceptionally(failure);}});
        }catch(Throwable error){view.completeExceptionally(error);}
    }
    private static ReplicaPhysicalAPI.Scope scope(ReplicaPhysicalScopeRequest request){if(!request.hasScope())throw new IllegalArgumentException("Missing physical scope");return ReplicaPhysicalWire.scope(request.getScope());}
    @Override public void discover(ReplicaPhysicalScopeRequest r,StreamObserver<ReplicaPhysicalTargets> o){invoke(r,r.hasContext(),r.getContext(),o,()->backend.discover(scope(r)),v->ReplicaPhysicalWire.targets(v,scope(r)));}
    @Override public void observe(ReplicaPhysicalScopeRequest r,StreamObserver<ReplicaPhysicalObservations> o){invoke(r,r.hasContext(),r.getContext(),o,()->backend.observe(scope(r)),v->ReplicaPhysicalWire.observations(v,scope(r)));}
    @Override public void flush(ReplicaPhysicalScopeRequest r,StreamObserver<Empty> o){invoke(r,r.hasContext(),r.getContext(),o,()->backend.flush(scope(r)),v->Empty.getDefaultInstance());}
    @Override public void compact(ReplicaPhysicalScopeRequest r,StreamObserver<Empty> o){invoke(r,r.hasContext(),r.getContext(),o,()->backend.compact(scope(r)),v->Empty.getDefaultInstance());}
    private static ReplicaPhysicalAPI.Target target(boolean present,ReplicaPhysicalTarget value){if(!present)throw new IllegalArgumentException("Missing qualified physical target");return ReplicaPhysicalWire.target(value);}
    @Override public void metadata(ReplicaPhysicalMetadataRequest r,StreamObserver<ReplicaPhysicalMetadata> o){invoke(r,r.hasContext(),r.getContext(),o,()->{if(r.getLevel() < -1)throw new IllegalArgumentException("Invalid physical metadata level");return backend.metadata(target(r.hasTarget(),r.getTarget()),r.getLevel());},v->ReplicaPhysicalWire.metadata(v,ReplicaPhysicalWire.target(r.getTarget()),r.getLevel()));}
    private static it.cavallium.rockserver.core.common.SstMaintenance.Request request(ReplicaPhysicalCompactRequest r){if(!r.hasRequest())throw new IllegalArgumentException("Missing physical compaction request");var target=target(r.hasTarget(),r.getTarget());var value=GrpcPortableWire.request(r.getRequest());ReplicaPhysicalWire.request(target,value);return value;}
    @Override public void compactFiles(ReplicaPhysicalCompactRequest r,StreamObserver<ReplicaPhysicalCompaction> o){invoke(r,r.hasContext(),r.getContext(),o,()->{var value=request(r);return backend.compactFiles(ReplicaPhysicalWire.target(r.getTarget()),value);},v->ReplicaPhysicalWire.result(v,ReplicaPhysicalWire.target(r.getTarget()),request(r)));}
}
