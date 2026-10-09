package it.cavallium.rockserver.core.common.export;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Bounded, host-independent export requests and results. */
public final class ExportJobs {
    public static final int MAX_COLUMNS = 256;
    public static final int MAX_CHUNKS = 65536;
    public static final int MAX_BOUND_BYTES = 2 * 1024 * 1024;
    public static final int MAX_MANIFEST_ITEMS = 128;
    public static final int MAX_MANIFEST_BYTES = 4 * 1024 * 1024;
    public static final int MIN_PAGE_BYTES = 64 * 1024;
    public static final int MAX_FILE_PAGE_BYTES = 1024 * 1024;
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
                    || snapshotTtl.compareTo(Duration.ofSeconds(60)) > 0) {
                throw new IllegalArgumentException("Invalid export limits");
            }
            long ttlMillis = snapshotTtl.toMillis();
            if (ttlMillis < 1 || !snapshotTtl.equals(Duration.ofMillis(ttlMillis))) {
                throw new IllegalArgumentException("Snapshot TTL must be 1..60000 whole milliseconds");
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
                         int chunkCount, int acknowledgedCount, long fileBytes, boolean cleanupPending) {
        public Status {
            Objects.requireNonNull(jobId, "jobId");
            Objects.requireNonNull(phase, "phase");
            columns = selectedColumns(columns);
            if (participantCount < 0 || chunkCount < 0 || chunkCount > MAX_CHUNKS
                    || acknowledgedCount < 0 || acknowledgedCount > chunkCount || fileBytes < 0) {
                throw new IllegalArgumentException("Invalid export status");
            }
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
