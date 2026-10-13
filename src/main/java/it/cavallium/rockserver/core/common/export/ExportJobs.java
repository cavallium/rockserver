package it.cavallium.rockserver.core.common.export;

import it.cavallium.rockserver.core.common.distributed.DatabaseDomainRoster;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Bounded, host-independent export requests and results. */
public final class ExportJobs {
    public static final int MAX_COLUMNS = 256;
    public static final Duration MAX_SNAPSHOT_TTL = Duration.ofHours(24);
    public static final int MAX_CHUNKS = 65536;
    public static final int MAX_BOUND_BYTES = 2 * 1024 * 1024;
    public static final int MAX_MANIFEST_ITEMS = 128;
    public static final int MAX_MANIFEST_BYTES = 4 * 1024 * 1024;
    public static final int MIN_PAGE_BYTES = 64 * 1024;
    public static final int MAX_FILE_PAGE_BYTES = 1024 * 1024;
    public static final int MAX_WITNESS_BYTES = 64 * 1024 * 1024;
    public static final int MAX_WITNESS_PAGE_BYTES = 1024 * 1024;
    /** Fixed manifest metadata bytes, excluding the two physical bounds. */
    public static final int CHUNK_METADATA_BYTES = 153;
    private ExportJobs() {}

    public record Create(UUID requestId, List<Long> columns, Limits limits) {
        public Create {
            Objects.requireNonNull(requestId, "requestId");
            columns = selectedColumns(columns);
            Objects.requireNonNull(limits, "limits");
        }
    }

    public record Limits(int maximumChunks, long maximumJournalBytes, long maximumDiskBytes,
                         int maximumRows, long maximumPayloadBytes, long maximumFileBytes,
                         Duration maximumChunkDuration, Duration snapshotTtl) {
        public Limits {
            Objects.requireNonNull(maximumChunkDuration, "maximumChunkDuration");
            Objects.requireNonNull(snapshotTtl, "snapshotTtl");
            if (maximumChunks < 1 || maximumChunks > MAX_CHUNKS
                    || maximumJournalBytes < 2L * (MAX_MANIFEST_BYTES + 4096) + 1024
                    || maximumJournalBytes > 64L * 1024 * 1024
                    || maximumRows < 1 || maximumPayloadBytes < 1 || maximumFileBytes < 1
                    || maximumDiskBytes < maximumFileBytes
                    || maximumChunkDuration.isZero() || maximumChunkDuration.isNegative()
                    || snapshotTtl.isZero() || snapshotTtl.isNegative()
                    || snapshotTtl.compareTo(MAX_SNAPSHOT_TTL) > 0) {
                throw new IllegalArgumentException("Invalid export limits");
            }
            long ttlMillis = snapshotTtl.toMillis();
            if (ttlMillis < 1 || !snapshotTtl.equals(Duration.ofMillis(ttlMillis))) {
                throw new IllegalArgumentException("Snapshot TTL must be positive whole milliseconds within 24 hours");
            }
            maximumChunkDuration.toNanos();
        }
        public static Limits defaults() {
            return new Limits(4096, 64L * 1024 * 1024, 1024L * 1024 * 1024,
                    65536, 16L * 1024 * 1024, 32L * 1024 * 1024,
                    Duration.ofSeconds(30), Duration.ofSeconds(60));
        }
    }

    public enum Phase { CAPTURING, EXPORTING, MATERIALIZED, READY, SNAPSHOT_LOST, CANCELLED, RELEASED }

    public record Status(UUID jobId, Phase phase, List<Long> columns, int participantCount,
                         int chunkCount, int acknowledgedCount, long fileBytes, boolean cleanupPending,
                         byte[] definitionDigest, long definitionBytes, long rosterGeneration, int domainCount,
                         byte[] captureDigest, long captureBytes) {
        public Status {
            Objects.requireNonNull(jobId, "jobId");
            Objects.requireNonNull(phase, "phase");
            columns = selectedColumns(columns);
            Objects.requireNonNull(definitionDigest, "definitionDigest");
            Objects.requireNonNull(captureDigest, "captureDigest");
            if (participantCount < 0 || chunkCount < 0 || chunkCount > MAX_CHUNKS
                    || acknowledgedCount < 0 || acknowledgedCount > chunkCount || fileBytes < 0
                    || definitionDigest.length != 32 || definitionBytes < 1 || definitionBytes > MAX_WITNESS_BYTES
                    || rosterGeneration < 0 || domainCount < 1 || domainCount > DatabaseDomainRoster.MAX_DOMAINS
                    || !(captureDigest.length == 0 && captureBytes == 0
                         || captureDigest.length == 32 && captureBytes > 0 && captureBytes <= MAX_WITNESS_BYTES)) {
                throw new IllegalArgumentException("Invalid export status");
            }
            if (captureDigest.length == 0 && (phase == Phase.EXPORTING || phase == Phase.MATERIALIZED || phase == Phase.READY))
                throw new IllegalArgumentException("Export phase requires its forced capture witness");
            definitionDigest = definitionDigest.clone();
            captureDigest = captureDigest.clone();
        }
        @Override public byte[] definitionDigest() { return definitionDigest.clone(); }
        @Override public byte[] captureDigest() { return captureDigest.clone(); }
        @Override public boolean equals(Object other) {
            return other instanceof Status status && jobId.equals(status.jobId) && phase == status.phase
                    && columns.equals(status.columns) && participantCount == status.participantCount
                    && chunkCount == status.chunkCount && acknowledgedCount == status.acknowledgedCount
                    && fileBytes == status.fileBytes && cleanupPending == status.cleanupPending
                    && definitionBytes == status.definitionBytes && rosterGeneration == status.rosterGeneration
                    && domainCount == status.domainCount && captureBytes == status.captureBytes
                    && Arrays.equals(definitionDigest, status.definitionDigest) && Arrays.equals(captureDigest, status.captureDigest);
        }
        @Override public int hashCode() {
            return 31 * (31 * Objects.hash(jobId, phase, columns, participantCount, chunkCount, acknowledgedCount,
                    fileBytes, cleanupPending, definitionBytes, rosterGeneration, domainCount, captureBytes)
                    + Arrays.hashCode(definitionDigest)) + Arrays.hashCode(captureDigest);
        }
    }

    public enum WitnessKind { DEFINITION, CAPTURES }

    public record WitnessRequest(UUID jobId, WitnessKind kind, byte[] expectedDigest, long offset, int maximumBytes) {
        public WitnessRequest {
            Objects.requireNonNull(jobId, "jobId");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(expectedDigest, "expectedDigest");
            if (expectedDigest.length != 32 || offset < 0 || offset > MAX_WITNESS_BYTES
                    || maximumBytes < MIN_PAGE_BYTES || maximumBytes > MAX_WITNESS_PAGE_BYTES)
                throw new IllegalArgumentException("Invalid export witness request");
            expectedDigest = expectedDigest.clone();
        }
        @Override public byte[] expectedDigest() { return expectedDigest.clone(); }
        @Override public boolean equals(Object other) {
            return other instanceof WitnessRequest request && jobId.equals(request.jobId) && kind == request.kind
                    && offset == request.offset && maximumBytes == request.maximumBytes
                    && Arrays.equals(expectedDigest, request.expectedDigest);
        }
        @Override public int hashCode() {
            return 31 * Objects.hash(jobId, kind, offset, maximumBytes) + Arrays.hashCode(expectedDigest);
        }
    }

    public record WitnessPage(UUID jobId, WitnessKind kind, byte[] digest, long totalBytes, long offset, byte[] bytes, boolean hasMore) {
        public WitnessPage {
            Objects.requireNonNull(jobId, "jobId");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(digest, "digest");
            Objects.requireNonNull(bytes, "bytes");
            if (digest.length != 32 || totalBytes < 1 || totalBytes > MAX_WITNESS_BYTES
                    || offset < 0 || offset > totalBytes || bytes.length > MAX_WITNESS_PAGE_BYTES
                    || bytes.length > totalBytes - offset || hasMore != (offset + bytes.length < totalBytes)
                    || hasMore && bytes.length == 0)
                throw new IllegalArgumentException("Invalid export witness page");
            digest = digest.clone();
            bytes = bytes.clone();
        }
        @Override public byte[] digest() { return digest.clone(); }
        @Override public byte[] bytes() { return bytes.clone(); }
        @Override public boolean equals(Object other) {
            return other instanceof WitnessPage page && jobId.equals(page.jobId) && kind == page.kind
                    && totalBytes == page.totalBytes && offset == page.offset && hasMore == page.hasMore
                    && Arrays.equals(digest, page.digest) && Arrays.equals(bytes, page.bytes);
        }
        @Override public int hashCode() {
            return 31 * (31 * Objects.hash(jobId, kind, totalBytes, offset, hasMore) + Arrays.hashCode(digest)) + Arrays.hashCode(bytes);
        }
    }

    public record ChunkId(UUID jobId, UUID groupId, UUID rangeLineage, long rangeEpoch,
                          long snapshotTerm, long snapshotIndex, long ordinal, byte[] contentHash) {
        public ChunkId {
            Objects.requireNonNull(jobId, "jobId");
            Objects.requireNonNull(groupId, "groupId");
            Objects.requireNonNull(rangeLineage, "rangeLineage");
            Objects.requireNonNull(contentHash, "contentHash");
            if (rangeEpoch < 0 || snapshotTerm < 1 || snapshotIndex < 0 || ordinal < 0
                    || contentHash.length != 32) throw new IllegalArgumentException("Invalid export chunk identity");
            contentHash = contentHash.clone();
        }
        @Override public byte[] contentHash() { return contentHash.clone(); }
        @Override public boolean equals(Object other) {
            return other instanceof ChunkId id && jobId.equals(id.jobId) && groupId.equals(id.groupId)
                    && rangeLineage.equals(id.rangeLineage) && rangeEpoch == id.rangeEpoch
                    && snapshotTerm == id.snapshotTerm && snapshotIndex == id.snapshotIndex
                    && ordinal == id.ordinal && Arrays.equals(contentHash, id.contentHash);
        }
        @Override public int hashCode() {
            return 31 * Objects.hash(jobId, groupId, rangeLineage, rangeEpoch, snapshotTerm, snapshotIndex, ordinal)
                    + Arrays.hashCode(contentHash);
        }
    }

    public record Chunk(ChunkId id, long columnId, byte[] lowerInclusive, byte[] upperExclusive,
                        long rows, long payloadBytes, long fileBytes, boolean hasMore) {
        public Chunk {
            Objects.requireNonNull(id, "id");
            if ((long) length(lowerInclusive) + length(upperExclusive) > MAX_BOUND_BYTES
                    || rows < 0 || payloadBytes < 0 || fileBytes < 0
                    || rows == 0 && (payloadBytes != 0 || fileBytes != 0 || hasMore)
                    || rows > 0 && fileBytes == 0
                    || hasMore && upperExclusive == null
                    || lowerInclusive != null && upperExclusive != null
                    && Arrays.compareUnsigned(lowerInclusive, upperExclusive) >= 0) {
                throw new IllegalArgumentException("Invalid export chunk");
            }
            lowerInclusive = copy(lowerInclusive);
            upperExclusive = copy(upperExclusive);
        }
        @Override public byte[] lowerInclusive() { return copy(lowerInclusive); }
        @Override public byte[] upperExclusive() { return copy(upperExclusive); }
        public int estimatedBytes() { return CHUNK_METADATA_BYTES + length(lowerInclusive) + length(upperExclusive); }
    }

    public record ManifestRequest(UUID jobId, int offset, int maximumItems, int maximumBytes) {
        public ManifestRequest {
            Objects.requireNonNull(jobId, "jobId");
            if (offset < 0 || offset > MAX_CHUNKS || maximumItems < 1 || maximumItems > MAX_MANIFEST_ITEMS
                    || maximumBytes < MIN_PAGE_BYTES || maximumBytes > MAX_MANIFEST_BYTES) {
                throw new IllegalArgumentException("Invalid export manifest request");
            }
        }
    }

    public record ManifestPage(List<Chunk> chunks, int next, boolean hasMore) {
        public ManifestPage {
            chunks = List.copyOf(chunks);
            long bytes = 9;
            for (var chunk : chunks) bytes += chunk.estimatedBytes();
            if (chunks.size() > MAX_MANIFEST_ITEMS || bytes > MAX_MANIFEST_BYTES
                    || next < chunks.size() || next > MAX_CHUNKS
                    || hasMore && (chunks.isEmpty() || next == MAX_CHUNKS)) {
                throw new IllegalArgumentException("Invalid export manifest page");
            }
        }
    }

    public record ReadRequest(ChunkId id, long offset, int maximumBytes) {
        public ReadRequest {
            Objects.requireNonNull(id, "id");
            if (offset < 0 || maximumBytes < MIN_PAGE_BYTES || maximumBytes > MAX_FILE_PAGE_BYTES) {
                throw new IllegalArgumentException("Invalid export file request");
            }
        }
    }

    public record FilePage(ChunkId id, long offset, byte[] bytes, boolean hasMore) {
        public FilePage {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(bytes, "bytes");
            if (offset < 0 || bytes.length > MAX_FILE_PAGE_BYTES || offset > Long.MAX_VALUE - bytes.length
                    || hasMore && bytes.length == 0) throw new IllegalArgumentException("Invalid export file page");
            bytes = bytes.clone();
        }
        @Override public byte[] bytes() { return bytes.clone(); }
    }

    private static List<Long> selectedColumns(List<Long> columns) {
        Objects.requireNonNull(columns, "columns");
        if (columns.isEmpty() || columns.size() > MAX_COLUMNS) throw new IllegalArgumentException("Invalid export columns");
        var sorted = columns.stream().map(column -> Objects.requireNonNull(column, "column")).sorted().toList();
        for (int i = 1; i < sorted.size(); i++) {
            if (sorted.get(i - 1).equals(sorted.get(i))) throw new IllegalArgumentException("Duplicate export column");
        }
        return sorted;
    }
    private static int length(byte[] value) { return value == null ? 0 : value.length; }
    private static byte[] copy(byte[] value) { return value == null ? null : value.clone(); }
}
