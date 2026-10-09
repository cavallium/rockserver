package it.cavallium.rockserver.core.server;

import com.google.protobuf.Empty;
import io.grpc.Context;
import io.grpc.Status;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.api.export.proto.*;
import it.cavallium.rockserver.core.common.export.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import reactor.core.publisher.Mono;

/** A configured export route; backend state outlives cancelled transport observations. */
public final class ExportGrpcService extends ReactorRocksDBExportServiceGrpc.RocksDBExportServiceImplBase {
    private final ExportAsyncAPI backend;
    /** Null explicitly installs a disabled route. */
    public ExportGrpcService(ExportAsyncAPI backend) { this.backend = backend; }

    public static final int MAX_REQUEST_BYTES = 64 * 1024;

    /** Export requests are small identities and limits, even when their unknown fields are large. */
    public io.grpc.ServerServiceDefinition bindBoundedService() {
        var generated = bindService();
        var methods = new java.util.ArrayList<io.grpc.ServerMethodDefinition<?, ?>>();
        var descriptor = io.grpc.ServiceDescriptor.newBuilder(generated.getServiceDescriptor().getName())
                .setSchemaDescriptor(generated.getServiceDescriptor().getSchemaDescriptor());
        for (var method : generated.getMethods()) {
            var bounded = boundedMethod(method);
            methods.add(bounded);
            descriptor.addMethod(bounded.getMethodDescriptor());
        }
        var service = io.grpc.ServerServiceDefinition.builder(descriptor.build());
        methods.forEach(method -> addMethod(service, method));
        return service.build();
    }
    private static <Q, R> io.grpc.ServerMethodDefinition<Q, R> boundedMethod(io.grpc.ServerMethodDefinition<Q, R> method) {
        var descriptor = method.getMethodDescriptor();
        var delegate = descriptor.getRequestMarshaller();
        var bounded = new io.grpc.MethodDescriptor.Marshaller<Q>() {
            @Override public java.io.InputStream stream(Q value) { return delegate.stream(value); }
            @Override public Q parse(java.io.InputStream input) {
                final byte[] bytes;
                try { bytes = input.readNBytes(MAX_REQUEST_BYTES + 1); }
                catch (java.io.IOException failure) { throw Status.INTERNAL.withCause(failure).asRuntimeException(); }
                if (bytes.length > MAX_REQUEST_BYTES) throw Status.RESOURCE_EXHAUSTED
                        .withDescription("Export request exceeds 64 KiB").asRuntimeException();
                return delegate.parse(new java.io.ByteArrayInputStream(bytes));
            }
        };
        return io.grpc.ServerMethodDefinition.create(descriptor.toBuilder(bounded, descriptor.getResponseMarshaller()).build(), method.getServerCallHandler());
    }
    private static <Q, R> void addMethod(io.grpc.ServerServiceDefinition.Builder builder, io.grpc.ServerMethodDefinition<Q, R> method) {
        builder.addMethod(method);
    }

    private RequestContext context(it.cavallium.rockserver.core.common.api.proto.RequestContext wire) {
        var context = ExportWire.context(wire);
        var deadline = Context.current().getDeadline();
        if (deadline == null) return context;
        long remaining = Math.min(context.timeoutNanos(), deadline.timeRemaining(TimeUnit.NANOSECONDS));
        if (remaining <= 0) throw Status.DEADLINE_EXCEEDED.asRuntimeException();
        return new RequestContext(context.profile(), remaining);
    }
    private <T, R> Mono<R> invoke(it.cavallium.rockserver.core.common.api.proto.RequestContext wire,
                                java.util.function.Supplier<ExportCommand<T>> command, Function<T, R> mapper) {
        return Mono.defer(() -> {
            if (backend == null) return Mono.error(Status.UNIMPLEMENTED.withDescription("Export route is unavailable").asRuntimeException());
            var context = context(wire);
            var request = command.get();
            return Mono.fromFuture(request.handleAsync(backend, context), true).map(mapper);
        }).onErrorMap(ExportWire::serverError);
    }
    private Mono<Empty> control(it.cavallium.rockserver.core.common.api.proto.RequestContext wire,
                                java.util.function.Supplier<ExportCommand<Void>> command) {
        return Mono.defer(() -> {
            if (backend == null) return Mono.error(Status.UNIMPLEMENTED.asRuntimeException());
            var context = context(wire);
            return Mono.fromFuture(command.get().handleAsync(backend, context), true).thenReturn(Empty.getDefaultInstance());
        }).onErrorMap(ExportWire::serverError);
    }
    @Override public Mono<ExportStatus> createExport(ExportCreateRequest request) {
        return invoke(request.getContext(), () -> new ExportCommand.Create(ExportWire.create(request)), ExportWire::status);
    }
    @Override public Mono<ExportStatus> recoverExport(ExportJobRequest request) {
        return invoke(request.getContext(), () -> new ExportCommand.Recover(ExportWire.uuid(request.getJobId())), value -> ExportWire.status(value, ExportWire.uuid(request.getJobId())));
    }
    @Override public Mono<ExportStatus> stepExport(ExportJobRequest request) {
        return invoke(request.getContext(), () -> new ExportCommand.Step(ExportWire.uuid(request.getJobId())), value -> ExportWire.status(value, ExportWire.uuid(request.getJobId())));
    }
    @Override public Mono<ExportStatus> exportStatus(ExportJobRequest request) {
        return invoke(request.getContext(), () -> new ExportCommand.Status(ExportWire.uuid(request.getJobId())), value -> ExportWire.status(value, ExportWire.uuid(request.getJobId())));
    }
    @Override public Mono<ExportManifestPage> exportManifest(ExportManifestRequest request) {
        return invoke(request.getContext(), () -> new ExportCommand.Manifest(ExportWire.manifestRequest(request)),
                value -> ExportWire.manifest(value, ExportWire.manifestRequest(request)));
    }
    @Override public Mono<ExportFilePage> readExport(ExportReadRequest request) {
        return invoke(request.getContext(), () -> new ExportCommand.Read(ExportWire.readRequest(request)),
                value -> ExportWire.file(value, ExportWire.readRequest(request)));
    }
    @Override public Mono<Empty> acknowledgeExport(ExportAcknowledgeRequest request) {
        return control(request.getContext(), () -> new ExportCommand.Acknowledge(ExportWire.id(request.getId())));
    }
    @Override public Mono<Empty> releaseExport(ExportJobRequest request) {
        return control(request.getContext(), () -> new ExportCommand.Release(ExportWire.uuid(request.getJobId())));
    }
    @Override public Mono<Empty> cancelExport(ExportJobRequest request) {
        return control(request.getContext(), () -> new ExportCommand.Cancel(ExportWire.uuid(request.getJobId())));
    }
    @Override public Mono<ExportCapabilities> getExportCapabilities(Empty request) {
        return Mono.just(ExportCapabilities.newBuilder().setAvailable(backend != null)
                .setRawScanMode(ExportRawScanMode.IMMUTABLE_EXPORT_JOBS).setMaximumColumns(ExportJobs.MAX_COLUMNS)
                .setMaximumChunks(ExportJobs.MAX_CHUNKS).setMaximumManifestItems(ExportJobs.MAX_MANIFEST_ITEMS)
                .setMaximumManifestBytes(ExportJobs.MAX_MANIFEST_BYTES).setMaximumFilePageBytes(ExportJobs.MAX_FILE_PAGE_BYTES).build());
    }
}
