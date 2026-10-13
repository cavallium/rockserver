package it.cavallium.rockserver.core.common.export;

import it.cavallium.rockserver.core.common.RequestContext;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** Async full-column export lifecycle. Artifacts are addressed only by their immutable identity. */
public interface ExportAsyncAPI {
    CompletableFuture<ExportJobs.Status> createExport(ExportJobs.Create request, RequestContext context);
    CompletableFuture<ExportJobs.Status> recoverExport(UUID jobId, RequestContext context);
    CompletableFuture<ExportJobs.Status> stepExport(UUID jobId, RequestContext context);
    CompletableFuture<ExportJobs.Status> exportStatus(UUID jobId, RequestContext context);
    CompletableFuture<ExportJobs.ManifestPage> exportManifest(ExportJobs.ManifestRequest request, RequestContext context);
    CompletableFuture<ExportJobs.WitnessPage> exportWitness(ExportJobs.WitnessRequest request, RequestContext context);
    CompletableFuture<ExportJobs.FilePage> readExport(ExportJobs.ReadRequest request, RequestContext context);
    CompletableFuture<Void> acknowledgeExport(ExportJobs.ChunkId id, RequestContext context);
    CompletableFuture<Void> releaseExport(UUID jobId, RequestContext context);
    CompletableFuture<Void> cancelExport(UUID jobId, RequestContext context);
}
