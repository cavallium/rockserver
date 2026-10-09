package it.cavallium.rockserver.core.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Empty;
import io.grpc.Channel;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.api.export.proto.*;
import it.cavallium.rockserver.core.common.export.*;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Export-only client. Its channel remains owned by the caller. */
public final class GrpcExportClient implements ExportAsyncAPI {
    private final RocksDBExportServiceGrpc.RocksDBExportServiceFutureStub stub;
    public GrpcExportClient(Channel channel) {
        stub = RocksDBExportServiceGrpc.newFutureStub(Objects.requireNonNull(channel)).withMaxInboundMessageSize(ExportWire.MAX_FRAME_BYTES);
    }
    private <W, T> CompletableFuture<T> invoke(RequestContext context,
            BiFunction<RocksDBExportServiceGrpc.RocksDBExportServiceFutureStub, it.cavallium.rockserver.core.common.api.proto.RequestContext, ListenableFuture<W>> call,
            Function<W, T> mapper) {
        var bound = BoundRequestContext.bind(context);
        try {
            long remaining = bound.remainingNanos();
            var wire = ExportWire.context(context, remaining);
            var active = remaining == RequestContext.NO_TIMEOUT ? stub : stub.withDeadlineAfter(remaining, TimeUnit.NANOSECONDS);
            var source = call.apply(active, wire);
            var result = new CompletableFuture<T>();
            Futures.addCallback(source, new FutureCallback<>() {
                @Override public void onSuccess(W value) {
                    try { result.complete(mapper.apply(value)); }
                    catch (Throwable error) { result.completeExceptionally(error); }
                }
                @Override public void onFailure(Throwable error) { result.completeExceptionally(ExportWire.clientError(error)); }
            }, MoreExecutors.directExecutor());
            result.whenComplete((value, error) -> { if (result.isCancelled()) source.cancel(false); });
            return result;
        } catch (Throwable error) { return CompletableFuture.failedFuture(ExportWire.clientError(error)); }
    }
    private ExportJobRequest job(UUID id, it.cavallium.rockserver.core.common.api.proto.RequestContext context) {
        return ExportJobRequest.newBuilder().setJobId(ExportWire.uuid(id)).setContext(context).build();
    }
    @Override public CompletableFuture<ExportJobs.Status> createExport(ExportJobs.Create request, RequestContext context) {
        return invoke(context, (s, c) -> s.createExport(ExportWire.create(request).toBuilder().setContext(c).build()), ExportWire::status);
    }
    @Override public CompletableFuture<ExportJobs.Status> recoverExport(UUID id, RequestContext context) {
        return invoke(context, (s, c) -> s.recoverExport(job(id, c)), value -> ExportWire.status(value, id));
    }
    @Override public CompletableFuture<ExportJobs.Status> stepExport(UUID id, RequestContext context) {
        return invoke(context, (s, c) -> s.stepExport(job(id, c)), value -> ExportWire.status(value, id));
    }
    @Override public CompletableFuture<ExportJobs.Status> exportStatus(UUID id, RequestContext context) {
        return invoke(context, (s, c) -> s.exportStatus(job(id, c)), value -> ExportWire.status(value, id));
    }
    @Override public CompletableFuture<ExportJobs.ManifestPage> exportManifest(ExportJobs.ManifestRequest request, RequestContext context) {
        return invoke(context, (s, c) -> s.exportManifest(ExportManifestRequest.newBuilder().setJobId(ExportWire.uuid(request.jobId()))
                .setOffset(request.offset()).setMaximumItems(request.maximumItems()).setMaximumBytes(request.maximumBytes()).setContext(c).build()),
                value -> ExportWire.manifest(value, request));
    }
    @Override public CompletableFuture<ExportJobs.FilePage> readExport(ExportJobs.ReadRequest request, RequestContext context) {
        return invoke(context, (s, c) -> s.readExport(ExportReadRequest.newBuilder().setId(ExportWire.id(request.id()))
                .setOffset(request.offset()).setMaximumBytes(request.maximumBytes()).setContext(c).build()), value -> ExportWire.file(value, request));
    }
    @Override public CompletableFuture<Void> acknowledgeExport(ExportJobs.ChunkId id, RequestContext context) {
        return invoke(context, (s, c) -> s.acknowledgeExport(ExportAcknowledgeRequest.newBuilder().setId(ExportWire.id(id)).setContext(c).build()), value -> null);
    }
    @Override public CompletableFuture<Void> releaseExport(UUID id, RequestContext context) {
        return invoke(context, (s, c) -> s.releaseExport(job(id, c)), value -> null);
    }
    @Override public CompletableFuture<Void> cancelExport(UUID id, RequestContext context) {
        return invoke(context, (s, c) -> s.cancelExport(job(id, c)), value -> null);
    }
    public CompletableFuture<ExportCapabilities> capabilities(RequestContext context) {
        return invoke(context, (s, c) -> s.getExportCapabilities(Empty.getDefaultInstance()), Function.identity());
    }
}
