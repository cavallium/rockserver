package it.cavallium.rockserver.core.common.export;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.api.export.proto.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;

/** Strict, bounded conversion for the host-independent export transport. */
public final class ExportWire {
    public static final int MAX_FRAME_BYTES = 8 * 1024 * 1024;
    private ExportWire() {}
    public static ByteString uuid(UUID id) {
        return ByteString.copyFrom(ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array());
    }
    public static UUID uuid(ByteString id) {
        if (id.size() != 16) throw new IllegalArgumentException("UUID must contain exactly 16 bytes");
        var buffer = id.asReadOnlyByteBuffer();
        return new UUID(buffer.getLong(), buffer.getLong());
    }
    public static RequestContext context(it.cavallium.rockserver.core.common.api.proto.RequestContext value) {
        if (value.equals(it.cavallium.rockserver.core.common.api.proto.RequestContext.getDefaultInstance())) return RequestContext.batch();
        return new RequestContext(WorkloadProfile.fromWireValue(value.getProfileValue()), value.getTimeoutNanos());
    }
    public static it.cavallium.rockserver.core.common.api.proto.RequestContext context(RequestContext value, long remaining) {
        if (remaining <= 0) throw Status.DEADLINE_EXCEEDED.asRuntimeException();
        return it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
                .setProfileValue(value.profile().wireValue()).setTimeoutNanos(remaining).build();
    }
    public static ExportJobs.Create create(ExportCreateRequest value) {
        columns(value.getColumnsCount());
        if (!value.hasLimits()) throw new IllegalArgumentException("Missing export limits");
        var l = value.getLimits();
        return new ExportJobs.Create(uuid(value.getRequestId()), value.getColumnsList(), new ExportJobs.Limits(
                l.getMaximumChunks(), l.getMaximumJournalBytes(), l.getMaximumDiskBytes(), l.getMaximumRows(),
                l.getMaximumPayloadBytes(), l.getMaximumFileBytes(), Duration.ofNanos(l.getMaximumChunkDurationNanos()),
                Duration.ofMillis(l.getSnapshotTtlMillis())));
    }
    public static ExportCreateRequest create(ExportJobs.Create value) {
        var l = value.limits();
        return ExportCreateRequest.newBuilder().setRequestId(uuid(value.requestId())).addAllColumns(value.columns())
                .setLimits(ExportLimits.newBuilder().setMaximumChunks(l.maximumChunks()).setMaximumJournalBytes(l.maximumJournalBytes())
                        .setMaximumDiskBytes(l.maximumDiskBytes()).setMaximumRows(l.maximumRows()).setMaximumPayloadBytes(l.maximumPayloadBytes())
                        .setMaximumFileBytes(l.maximumFileBytes()).setMaximumChunkDurationNanos(l.maximumChunkDuration().toNanos())
                        .setSnapshotTtlMillis(l.snapshotTtl().toMillis())).build();
    }
    private static void columns(int count) {
        if (count < 1 || count > ExportJobs.MAX_COLUMNS) throw new IllegalArgumentException("Invalid export column count");
    }
    public static ExportChunkId id(ExportJobs.ChunkId v) {
        return ExportChunkId.newBuilder().setJobId(uuid(v.jobId())).setGroupId(uuid(v.groupId())).setRangeLineage(uuid(v.rangeLineage()))
                .setRangeEpoch(v.rangeEpoch()).setSnapshotTerm(v.snapshotTerm()).setSnapshotIndex(v.snapshotIndex())
                .setOrdinal(v.ordinal()).setContentHash(ByteString.copyFrom(v.contentHash())).build();
    }
    public static ExportJobs.ChunkId id(ExportChunkId v) {
        if (v.getContentHash().size() != 32) throw new IllegalArgumentException("Invalid content checksum");
        return new ExportJobs.ChunkId(uuid(v.getJobId()), uuid(v.getGroupId()), uuid(v.getRangeLineage()), v.getRangeEpoch(),
                v.getSnapshotTerm(), v.getSnapshotIndex(), v.getOrdinal(), v.getContentHash().toByteArray());
    }
    public static ExportStatus status(ExportJobs.Status v) {
        return ExportStatus.newBuilder().setJobId(uuid(v.jobId())).setPhase(ExportPhase.valueOf(v.phase().name()))
                .addAllColumns(v.columns()).setParticipantCount(v.participantCount()).setChunkCount(v.chunkCount())
                .setAcknowledgedCount(v.acknowledgedCount()).setFileBytes(v.fileBytes()).setCleanupPending(v.cleanupPending())
                .setDefinitionDigest(ByteString.copyFrom(v.definitionDigest())).setDefinitionBytes(v.definitionBytes())
                .setRosterGeneration(v.rosterGeneration()).setDomainCount(v.domainCount())
                .setCaptureDigest(ByteString.copyFrom(v.captureDigest())).setCaptureBytes(v.captureBytes()).build();
    }
    public static ExportJobs.Status status(ExportStatus v) {
        columns(v.getColumnsCount());
        if (v.getDefinitionDigest().size() != 32
                || !(v.getCaptureDigest().isEmpty() && v.getCaptureBytes() == 0
                     || v.getCaptureDigest().size() == 32 && v.getCaptureBytes() > 0 && v.getCaptureBytes() <= ExportJobs.MAX_WITNESS_BYTES))
            throw new IllegalArgumentException("Invalid export witness digest summary");
        return new ExportJobs.Status(uuid(v.getJobId()), ExportJobs.Phase.valueOf(v.getPhase().name()), v.getColumnsList(),
                v.getParticipantCount(), v.getChunkCount(), v.getAcknowledgedCount(), v.getFileBytes(), v.getCleanupPending(),
                v.getDefinitionDigest().toByteArray(), v.getDefinitionBytes(), v.getRosterGeneration(), v.getDomainCount(),
                v.getCaptureDigest().toByteArray(), v.getCaptureBytes());
    }
    public static ExportStatus status(ExportJobs.Status value, UUID expectedJob) {
        if (!value.jobId().equals(expectedJob)) throw new IllegalArgumentException("Wrong status job identity");
        return status(value);
    }
    public static ExportJobs.Status status(ExportStatus value, UUID expectedJob) {
        var result = status(value);
        if (!result.jobId().equals(expectedJob)) throw new IllegalArgumentException("Wrong status job identity");
        return result;
    }
    public static ExportWitnessRequest witnessRequest(ExportJobs.WitnessRequest v) {
        return ExportWitnessRequest.newBuilder().setJobId(uuid(v.jobId())).setKind(ExportWitnessKind.valueOf(v.kind().name()))
                .setExpectedDigest(ByteString.copyFrom(v.expectedDigest())).setOffset(v.offset()).setMaximumBytes(v.maximumBytes()).build();
    }
    public static ExportJobs.WitnessRequest witnessRequest(ExportWitnessRequest v) {
        if (v.getExpectedDigest().size() != 32) throw new IllegalArgumentException("Invalid witness digest");
        return new ExportJobs.WitnessRequest(uuid(v.getJobId()), ExportJobs.WitnessKind.valueOf(v.getKind().name()),
                v.getExpectedDigest().toByteArray(), v.getOffset(), v.getMaximumBytes());
    }
    public static ExportWitnessPage witness(ExportJobs.WitnessPage v, ExportJobs.WitnessRequest request) {
        var bytes = v.bytes();
        validateWitness(v.jobId(), v.kind(), v.digest(), v.offset(), bytes.length, request);
        return ExportWitnessPage.newBuilder().setJobId(uuid(v.jobId())).setKind(ExportWitnessKind.valueOf(v.kind().name()))
                .setDigest(ByteString.copyFrom(v.digest())).setTotalBytes(v.totalBytes()).setOffset(v.offset())
                .setData(ByteString.copyFrom(bytes)).setHasMore(v.hasMore()).build();
    }
    public static ExportJobs.WitnessPage witness(ExportWitnessPage v, ExportJobs.WitnessRequest request) {
        if (v.getDigest().size() != 32 || v.getData().size() > request.maximumBytes())
            throw new IllegalArgumentException("Invalid witness response size");
        var job = uuid(v.getJobId());
        var kind = ExportJobs.WitnessKind.valueOf(v.getKind().name());
        var digest = v.getDigest().toByteArray();
        validateWitness(job, kind, digest, v.getOffset(), v.getData().size(), request);
        return new ExportJobs.WitnessPage(job, kind, digest, v.getTotalBytes(), v.getOffset(), v.getData().toByteArray(), v.getHasMore());
    }
    private static void validateWitness(UUID job, ExportJobs.WitnessKind kind, byte[] digest, long offset, int bytes,
                                        ExportJobs.WitnessRequest request) {
        if (!job.equals(request.jobId()) || kind != request.kind() || !java.util.Arrays.equals(digest, request.expectedDigest())
                || offset != request.offset() || bytes > request.maximumBytes())
            throw new IllegalArgumentException("Invalid witness response identity or bounds");
    }
    public static ExportJobs.ManifestRequest manifestRequest(ExportManifestRequest v) {
        return new ExportJobs.ManifestRequest(uuid(v.getJobId()), v.getOffset(), v.getMaximumItems(), v.getMaximumBytes());
    }
    public static ExportJobs.ReadRequest readRequest(ExportReadRequest v) {
        return new ExportJobs.ReadRequest(id(v.getId()), v.getOffset(), v.getMaximumBytes());
    }
    public static ExportChunk chunk(ExportJobs.Chunk v) {
        var b = ExportChunk.newBuilder().setId(id(v.id())).setColumnId(v.columnId()).setRows(v.rows())
                .setPayloadBytes(v.payloadBytes()).setFileBytes(v.fileBytes()).setHasMore(v.hasMore());
        var lower = v.lowerInclusive(); var upper = v.upperExclusive();
        if (lower != null) b.setLowerInclusive(ByteString.copyFrom(lower));
        if (upper != null) b.setUpperExclusive(ByteString.copyFrom(upper));
        return b.build();
    }
    public static ExportJobs.Chunk chunk(ExportChunk v) {
        if ((long)v.getLowerInclusive().size() + v.getUpperExclusive().size() > ExportJobs.MAX_BOUND_BYTES)
            throw new IllegalArgumentException("Export bounds too large");
        return new ExportJobs.Chunk(id(v.getId()), v.getColumnId(), v.hasLowerInclusive() ? v.getLowerInclusive().toByteArray() : null,
                v.hasUpperExclusive() ? v.getUpperExclusive().toByteArray() : null, v.getRows(), v.getPayloadBytes(), v.getFileBytes(), v.getHasMore());
    }
    public static ExportManifestPage manifest(ExportJobs.ManifestPage v, ExportJobs.ManifestRequest request) {
        validateManifest(v, request);
        var b = ExportManifestPage.newBuilder().setNext(v.next()).setHasMore(v.hasMore());
        v.chunks().forEach(c -> b.addChunks(chunk(c)));
        return b.build();
    }
    public static ExportJobs.ManifestPage manifest(ExportManifestPage v, ExportJobs.ManifestRequest request) {
        if (v.getChunksCount() > request.maximumItems() || v.getSerializedSize() > MAX_FRAME_BYTES)
            throw new IllegalArgumentException("Oversized manifest response");
        long bytes = 9;
        for (var c : v.getChunksList()) bytes += ExportJobs.CHUNK_METADATA_BYTES + (long)c.getLowerInclusive().size() + c.getUpperExclusive().size();
        if (bytes > request.maximumBytes()) throw new IllegalArgumentException("Oversized manifest response");
        var result = new ExportJobs.ManifestPage(v.getChunksList().stream().map(ExportWire::chunk).toList(), v.getNext(), v.getHasMore());
        validateManifest(result, request);
        return result;
    }
    private static void validateManifest(ExportJobs.ManifestPage v, ExportJobs.ManifestRequest request) {
        long bytes = 9;
        var identities = new java.util.HashSet<ExportJobs.ChunkId>();
        for (var c : v.chunks()) {
            bytes += c.estimatedBytes();
            if (!c.id().jobId().equals(request.jobId()) || !identities.add(c.id()))
                throw new IllegalArgumentException("Wrong or duplicate manifest chunk identity");
        }
        if (v.chunks().size() > request.maximumItems() || bytes > request.maximumBytes()
                || v.next() != request.offset() + v.chunks().size()) throw new IllegalArgumentException("Invalid manifest response bounds");
    }
    public static ExportFilePage file(ExportJobs.FilePage v, ExportJobs.ReadRequest request) {
        var bytes = v.bytes();
        validateFile(v.id(), v.offset(), bytes.length, request);
        return ExportFilePage.newBuilder().setId(id(v.id())).setOffset(v.offset()).setData(ByteString.copyFrom(bytes)).setHasMore(v.hasMore()).build();
    }
    public static ExportJobs.FilePage file(ExportFilePage v, ExportJobs.ReadRequest request) {
        var identity = id(v.getId());
        validateFile(identity, v.getOffset(), v.getData().size(), request);
        return new ExportJobs.FilePage(identity, v.getOffset(), v.getData().toByteArray(), v.getHasMore());
    }
    private static void validateFile(ExportJobs.ChunkId id, long offset, int bytes, ExportJobs.ReadRequest request) {
        if (!id.equals(request.id()) || offset != request.offset() || bytes > request.maximumBytes())
            throw new IllegalArgumentException("Invalid file response identity or bounds");
    }
    public static Throwable serverError(Throwable error) {
        while (error instanceof java.util.concurrent.CompletionException && error.getCause() != null) error = error.getCause();
        if (error instanceof io.grpc.StatusException || error instanceof io.grpc.StatusRuntimeException) return error;
        if (error instanceof RocksDBException rocks) {
            var status = switch (rocks.getErrorUniqueId()) {
                case READ_DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED;
                case SERVER_OVERLOADED -> Status.RESOURCE_EXHAUSTED;
                case PUT_INVALID_REQUEST -> Status.INVALID_ARGUMENT;
                case TRANSACTION_NOT_FOUND, CDC_SUBSCRIPTION_NOT_FOUND -> Status.NOT_FOUND;
                case COMPACTION_CONFLICT -> Status.ABORTED;
                case CDC_RESPONSE_TOO_LARGE -> Status.FAILED_PRECONDITION;
                default -> Status.INTERNAL;
            };
            return status.withDescription(rocks.getLocalizedMessage()).asRuntimeException();
        }
        var status = error instanceof java.util.concurrent.TimeoutException ? Status.DEADLINE_EXCEEDED
                : error instanceof java.util.concurrent.RejectedExecutionException ? Status.RESOURCE_EXHAUSTED
                : error instanceof IllegalArgumentException ? Status.INVALID_ARGUMENT : Status.INTERNAL;
        return status.withCause(error).asRuntimeException();
    }
    public static Throwable clientError(Throwable error) {
        var status = Status.fromThrowable(error);
        var description = status.getDescription();
        String prefix = "RocksDBError: [uid:";
        if (description != null && description.startsWith(prefix)) {
            int end = description.indexOf(']', prefix.length());
            if (end >= 0) try {
                var type = RocksDBException.RocksDBErrorType.valueOf(description.substring(prefix.length(), end));
                if (type == RocksDBException.RocksDBErrorType.UPDATE_RETRY) return new it.cavallium.rockserver.core.common.RocksDBRetryException();
                return RocksDBException.of(type, description.substring(end + 1).stripLeading());
            } catch (IllegalArgumentException ignored) { }
        }
        return status.getCode() == Status.Code.DEADLINE_EXCEEDED
                ? RocksDBException.of(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED, "Deadline exceeded") : error;
    }
}
