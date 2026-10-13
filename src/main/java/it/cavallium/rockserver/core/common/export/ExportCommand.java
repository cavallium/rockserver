package it.cavallium.rockserver.core.common.export;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.WorkloadCost;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** Typed export dispatch and server-derived admission metadata. */
public sealed interface ExportCommand<R> {
    CompletableFuture<R> handleAsync(ExportAsyncAPI api, RequestContext context);

    default OperationFamily operationFamily() {
        return switch (this) {
            case Create _, Recover _ -> OperationFamily.MUTATION;
            case Step _ -> OperationFamily.FULL_SCAN_AGGREGATE;
            case Status _ -> OperationFamily.METADATA;
            case Manifest _, Read _, Witness _ -> OperationFamily.RANGE_PAGE;
            case Acknowledge _, Release _, Cancel _ -> OperationFamily.CONTROL;
        };
    }

    default long estimatedBytes() {
        return switch (this) {
            case Create create -> Math.min(WorkloadCost.MAX_ESTIMATED_BYTES, 128L + 8L * create.request.columns().size());
            case Manifest manifest -> Math.min(WorkloadCost.MAX_ESTIMATED_BYTES, 32L + manifest.request.maximumBytes());
            case Witness witness -> Math.min(WorkloadCost.MAX_ESTIMATED_BYTES, 128L + witness.request.maximumBytes());
            case Read read -> Math.min(WorkloadCost.MAX_ESTIMATED_BYTES, 128L + read.request.maximumBytes());
            case Recover _, Step _ -> WorkloadCost.MAX_ESTIMATED_BYTES;
            case Status _, Release _, Cancel _ -> 16L;
            case Acknowledge _ -> 120L;
        };
    }

    /** Default service guarantee; callers still supply their timeout contract. */
    default WorkloadProfile defaultProfile() {
        return switch (this) {
            case Create _, Recover _, Step _ -> WorkloadProfile.BATCH;
            case Acknowledge _, Release _, Cancel _ -> WorkloadProfile.CONTROL;
            case Status _, Manifest _, Read _, Witness _ -> WorkloadProfile.LATENCY;
        };
    }

    /** Protected control admission covers short lifecycle decisions; artifact cleanup runs separately. */
    default WorkloadProfile protectedProfile() {
        return operationFamily() == OperationFamily.CONTROL ? WorkloadProfile.CONTROL : null;
    }

    record Create(ExportJobs.Create request) implements ExportCommand<ExportJobs.Status> {
        public Create { Objects.requireNonNull(request, "request"); }
        @Override public CompletableFuture<ExportJobs.Status> handleAsync(ExportAsyncAPI api, RequestContext context) {
            return api.createExport(request, context);
        }
    }
    record Recover(UUID jobId) implements ExportCommand<ExportJobs.Status> {
        public Recover { Objects.requireNonNull(jobId, "jobId"); }
        @Override public CompletableFuture<ExportJobs.Status> handleAsync(ExportAsyncAPI api, RequestContext context) {
            return api.recoverExport(jobId, context);
        }
    }
    record Step(UUID jobId) implements ExportCommand<ExportJobs.Status> {
        public Step { Objects.requireNonNull(jobId, "jobId"); }
        @Override public CompletableFuture<ExportJobs.Status> handleAsync(ExportAsyncAPI api, RequestContext context) {
            return api.stepExport(jobId, context);
        }
    }
    record Status(UUID jobId) implements ExportCommand<ExportJobs.Status> {
        public Status { Objects.requireNonNull(jobId, "jobId"); }
        @Override public CompletableFuture<ExportJobs.Status> handleAsync(ExportAsyncAPI api, RequestContext context) {
            return api.exportStatus(jobId, context);
        }
    }
    record Manifest(ExportJobs.ManifestRequest request) implements ExportCommand<ExportJobs.ManifestPage> {
        public Manifest { Objects.requireNonNull(request, "request"); }
        @Override public CompletableFuture<ExportJobs.ManifestPage> handleAsync(ExportAsyncAPI api, RequestContext context) {
            return api.exportManifest(request, context);
        }
    }
    record Witness(ExportJobs.WitnessRequest request) implements ExportCommand<ExportJobs.WitnessPage> {
        public Witness { Objects.requireNonNull(request, "request"); }
        @Override public CompletableFuture<ExportJobs.WitnessPage> handleAsync(ExportAsyncAPI api, RequestContext context) {
            return api.exportWitness(request, context);
        }
    }
    record Read(ExportJobs.ReadRequest request) implements ExportCommand<ExportJobs.FilePage> {
        public Read { Objects.requireNonNull(request, "request"); }
        @Override public CompletableFuture<ExportJobs.FilePage> handleAsync(ExportAsyncAPI api, RequestContext context) {
            return api.readExport(request, context);
        }
    }
    record Acknowledge(ExportJobs.ChunkId id) implements ExportCommand<Void> {
        public Acknowledge { Objects.requireNonNull(id, "id"); }
        @Override public CompletableFuture<Void> handleAsync(ExportAsyncAPI api, RequestContext context) {
            return api.acknowledgeExport(id, context);
        }
    }
    record Release(UUID jobId) implements ExportCommand<Void> {
        public Release { Objects.requireNonNull(jobId, "jobId"); }
        @Override public CompletableFuture<Void> handleAsync(ExportAsyncAPI api, RequestContext context) {
            return api.releaseExport(jobId, context);
        }
    }
    record Cancel(UUID jobId) implements ExportCommand<Void> {
        public Cancel { Objects.requireNonNull(jobId, "jobId"); }
        @Override public CompletableFuture<Void> handleAsync(ExportAsyncAPI api, RequestContext context) {
            return api.cancelExport(jobId, context);
        }
    }
}
