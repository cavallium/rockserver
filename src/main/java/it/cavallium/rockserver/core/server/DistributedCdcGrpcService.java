package it.cavallium.rockserver.core.server;

import com.google.protobuf.MessageLite;
import io.grpc.Context;
import io.grpc.Status;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.api.cdc.proto.*;
import it.cavallium.rockserver.core.common.cdc.*;
import it.cavallium.rockserver.core.common.export.ExportWire;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import reactor.core.publisher.Mono;

/** Cancellation detaches the observer; the backend retains actual durable CDC work. */
public final class DistributedCdcGrpcService extends ReactorDistributedCdcServiceGrpc.DistributedCdcServiceImplBase {
    public static final int MAX_REQUEST_BYTES=DistributedCdcWire.MAX_REQUEST_BYTES;
    private final DistributedCdcAsyncAPI backend;
    public DistributedCdcGrpcService(DistributedCdcAsyncAPI backend) { this.backend=backend; }
    public io.grpc.ServerServiceDefinition bindBoundedService() {
        var generated=bindService(); var methods=new java.util.ArrayList<io.grpc.ServerMethodDefinition<?,?>>();
        var descriptor=io.grpc.ServiceDescriptor.newBuilder(generated.getServiceDescriptor().getName()).setSchemaDescriptor(generated.getServiceDescriptor().getSchemaDescriptor());
        for(var method:generated.getMethods()) { var bounded=boundedMethod(method); methods.add(bounded); descriptor.addMethod(bounded.getMethodDescriptor()); }
        var service=io.grpc.ServerServiceDefinition.builder(descriptor.build()); methods.forEach(method->addMethod(service,method)); return service.build();
    }
    private static <Q,R> io.grpc.ServerMethodDefinition<Q,R> boundedMethod(io.grpc.ServerMethodDefinition<Q,R> method) {
        var descriptor=method.getMethodDescriptor(); var delegate=descriptor.getRequestMarshaller();
        var bounded=new io.grpc.MethodDescriptor.Marshaller<Q>() {
            public java.io.InputStream stream(Q value) { return delegate.stream(value); }
            public Q parse(java.io.InputStream input) {
                final byte[] bytes; try { bytes=input.readNBytes(MAX_REQUEST_BYTES+1); } catch(java.io.IOException failure) { throw Status.INTERNAL.withCause(failure).asRuntimeException(); }
                if(bytes.length>MAX_REQUEST_BYTES) throw Status.RESOURCE_EXHAUSTED.withDescription("Vector CDC request exceeds 128 KiB").asRuntimeException();
                return delegate.parse(new java.io.ByteArrayInputStream(bytes));
            }
        };
        return io.grpc.ServerMethodDefinition.create(descriptor.toBuilder(bounded,descriptor.getResponseMarshaller()).build(),method.getServerCallHandler());
    }
    private static <Q,R> void addMethod(io.grpc.ServerServiceDefinition.Builder builder,io.grpc.ServerMethodDefinition<Q,R> method) { builder.addMethod(method); }
    private RequestContext context(it.cavallium.rockserver.core.common.api.proto.RequestContext value) {
        var context=DistributedCdcWire.context(value); var deadline=Context.current().getDeadline(); if(deadline==null) return context;
        long remaining=Math.min(context.timeoutNanos(),deadline.timeRemaining(TimeUnit.NANOSECONDS)); if(remaining<=0) throw Status.DEADLINE_EXCEEDED.asRuntimeException(); return new RequestContext(context.profile(),remaining);
    }
    private <T,R extends MessageLite> Mono<R> invoke(MessageLite request,boolean hasContext,it.cavallium.rockserver.core.common.api.proto.RequestContext wire,
            Function<RequestContext,CompletableFuture<T>> command,Function<T,R> mapper) {
        return Mono.defer(()->{
            if(backend==null) return Mono.error(Status.UNIMPLEMENTED.withDescription("Vector CDC is unavailable").asRuntimeException());
            DistributedCdcWire.bounded(request,MAX_REQUEST_BYTES); if(!hasContext) throw new IllegalArgumentException("Missing CDC request context");
            return Mono.fromFuture(command.apply(context(wire)),true).map(value->DistributedCdcWire.bounded(mapper.apply(value),DistributedCdcWire.MAX_FRAME_BYTES)).switchIfEmpty(Mono.error(new IllegalArgumentException("Empty CDC backend reply")));
        }).onErrorMap(ExportWire::serverError);
    }
    @Override public Mono<CdcDirectory> context(CdcContextRequest request) {
        return invoke(request,request.hasContext(),request.getContext(),c->backend.context(DistributedCdcWire.uuid(request.getDomainId()),c),v->DistributedCdcWire.directory(v,DistributedCdcWire.uuid(request.getDomainId())));
    }
    @Override public Mono<CdcStatus> create(CdcCreateRequest request) {
        return invoke(request,request.hasContext(),request.getContext(),c->{ if(!request.hasDefinition()) throw new IllegalArgumentException("Missing CDC definition"); return backend.create(DistributedCdcWire.definition(request.getDefinition()),c); },v->DistributedCdcWire.status(v,DistributedCdcWire.definition(request.getDefinition())));
    }
    @Override public Mono<CdcStatus> recover(CdcSubscriptionRequest request) { return invoke(request,request.hasContext(),request.getContext(),c->backend.recover(DistributedCdcWire.uuid(request.getSubscriptionId()),c),v->DistributedCdcWire.status(v,DistributedCdcWire.uuid(request.getSubscriptionId()))); }
    @Override public Mono<CdcStatus> status(CdcSubscriptionRequest request) { return invoke(request,request.hasContext(),request.getContext(),c->backend.status(DistributedCdcWire.uuid(request.getSubscriptionId()),c),v->DistributedCdcWire.status(v,DistributedCdcWire.uuid(request.getSubscriptionId()))); }
    @Override public Mono<CdcPoll> poll(CdcPollRequest request) {
        return invoke(request,request.hasContext(),request.getContext(),c->{ DistributedCdcWire.pollBudget(request.getMaxRecords(),request.getMaxBytes()); return backend.poll(DistributedCdcWire.uuid(request.getSubscriptionId()),request.getMaxRecords(),request.getMaxBytes(),c); },v->DistributedCdcWire.poll(v,DistributedCdcWire.uuid(request.getSubscriptionId()),request.getMaxRecords(),request.getMaxBytes()));
    }
    @Override public Mono<CdcStatus> acknowledge(CdcAcknowledgeRequest request) { return invoke(request,request.hasContext(),request.getContext(),c->backend.acknowledge(DistributedCdcWire.uuid(request.getSubscriptionId()),DistributedCdcWire.uuid(request.getBatchId()),c),v->DistributedCdcWire.status(v,DistributedCdcWire.uuid(request.getSubscriptionId()))); }
    @Override public Mono<CdcStatus> closeSubscription(CdcSubscriptionRequest request) { return invoke(request,request.hasContext(),request.getContext(),c->backend.closeSubscription(DistributedCdcWire.uuid(request.getSubscriptionId()),c),v->DistributedCdcWire.status(v,DistributedCdcWire.uuid(request.getSubscriptionId()))); }
}
