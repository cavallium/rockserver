package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport.CheckpointSnapshotManifest;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.zip.CRC32C;

public record RatisPartitionSnapshotManifest(int formatVersion,
                                             ShardId partitionId,
                                             long partitionEpoch,
                                             long ratisTerm,
                                             long ratisIndex,
                                             long metadataEpoch,
                                             long schemaEpoch,
                                             long dedupeWatermark,
                                             long cdcWatermark,
                                             int checkpointFileCount,
                                             long checkpointTotalSizeBytes,
                                             int checkpointChunkSizeBytes,
                                             long checkpointManifestCrc32c,
                                             long manifestCrc32c) implements Serializable {

    public static final int FORMAT_VERSION = 1;

    @Serial
    private static final long serialVersionUID = 1L;

    public RatisPartitionSnapshotManifest {
        if (formatVersion != FORMAT_VERSION) {
            throw new IllegalArgumentException("formatVersion must be " + FORMAT_VERSION);
        }
        requireNonNull(partitionId, "partitionId");
        requireNonNegative(partitionEpoch, "partitionEpoch");
        requireNonNegative(ratisTerm, "ratisTerm");
        requireNonNegative(ratisIndex, "ratisIndex");
        requireNonNegative(metadataEpoch, "metadataEpoch");
        requireNonNegative(schemaEpoch, "schemaEpoch");
        requireNonNegative(dedupeWatermark, "dedupeWatermark");
        requireNonNegative(cdcWatermark, "cdcWatermark");
        requireNonNegative(checkpointFileCount, "checkpointFileCount");
        requireNonNegative(checkpointTotalSizeBytes, "checkpointTotalSizeBytes");
        if (checkpointChunkSizeBytes <= 0
                || checkpointChunkSizeBytes > RocksDBCheckpointSnapshotSupport.MAX_CHUNK_SIZE_BYTES) {
            throw new IllegalArgumentException("checkpointChunkSizeBytes must be between 1 and "
                    + RocksDBCheckpointSnapshotSupport.MAX_CHUNK_SIZE_BYTES);
        }
        requireCrc32c(checkpointManifestCrc32c, "checkpointManifestCrc32c");
        requireCrc32c(manifestCrc32c, "manifestCrc32c");
        validateManifestChecksum(
                formatVersion,
                partitionId,
                partitionEpoch,
                ratisTerm,
                ratisIndex,
                metadataEpoch,
                schemaEpoch,
                dedupeWatermark,
                cdcWatermark,
                checkpointFileCount,
                checkpointTotalSizeBytes,
                checkpointChunkSizeBytes,
                checkpointManifestCrc32c,
                manifestCrc32c
        );
    }

    public static RatisPartitionSnapshotManifest create(RatisPartitionSnapshotMetadata metadata,
                                                        CheckpointSnapshotManifest checkpointManifest) {
        requireNonNull(metadata, "metadata");
        requireNonNull(checkpointManifest, "checkpointManifest");
        if (checkpointManifest.commitIndex() != metadata.ratisIndex()) {
            throw new IllegalArgumentException("checkpoint commit index " + checkpointManifest.commitIndex()
                    + " does not match Ratis snapshot index " + metadata.ratisIndex());
        }
        long checkpointManifestCrc32c = checkpointManifestChecksum(checkpointManifest);
        long manifestCrc32c = manifestChecksum(
                FORMAT_VERSION,
                metadata.partitionId(),
                metadata.partitionEpoch(),
                metadata.ratisTerm(),
                metadata.ratisIndex(),
                metadata.metadataEpoch(),
                metadata.schemaEpoch(),
                metadata.dedupeWatermark(),
                metadata.cdcWatermark(),
                checkpointManifest.fileCount(),
                checkpointManifest.totalSizeBytes(),
                checkpointManifest.chunkSizeBytes(),
                checkpointManifestCrc32c
        );
        return new RatisPartitionSnapshotManifest(
                FORMAT_VERSION,
                metadata.partitionId(),
                metadata.partitionEpoch(),
                metadata.ratisTerm(),
                metadata.ratisIndex(),
                metadata.metadataEpoch(),
                metadata.schemaEpoch(),
                metadata.dedupeWatermark(),
                metadata.cdcWatermark(),
                checkpointManifest.fileCount(),
                checkpointManifest.totalSizeBytes(),
                checkpointManifest.chunkSizeBytes(),
                checkpointManifestCrc32c,
                manifestCrc32c
        );
    }

    public void validateCheckpointManifest(CheckpointSnapshotManifest checkpointManifest) {
        requireNonNull(checkpointManifest, "checkpointManifest");
        if (checkpointManifest.commitIndex() != ratisIndex) {
            throw new IllegalArgumentException("checkpoint commit index " + checkpointManifest.commitIndex()
                    + " does not match Ratis snapshot index " + ratisIndex);
        }
        if (checkpointManifest.fileCount() != checkpointFileCount) {
            throw new IllegalArgumentException("checkpoint file count " + checkpointManifest.fileCount()
                    + " does not match Ratis snapshot manifest file count " + checkpointFileCount);
        }
        if (checkpointManifest.totalSizeBytes() != checkpointTotalSizeBytes) {
            throw new IllegalArgumentException("checkpoint total size " + checkpointManifest.totalSizeBytes()
                    + " does not match Ratis snapshot manifest total size " + checkpointTotalSizeBytes);
        }
        if (checkpointManifest.chunkSizeBytes() != checkpointChunkSizeBytes) {
            throw new IllegalArgumentException("checkpoint chunk size " + checkpointManifest.chunkSizeBytes()
                    + " does not match Ratis snapshot manifest chunk size " + checkpointChunkSizeBytes);
        }
        long actualChecksum = checkpointManifestChecksum(checkpointManifest);
        if (actualChecksum != checkpointManifestCrc32c) {
            throw new IllegalArgumentException("checkpoint manifest checksum mismatch");
        }
    }

    public void validateManifestChecksum() {
        validateManifestChecksum(
                formatVersion,
                partitionId,
                partitionEpoch,
                ratisTerm,
                ratisIndex,
                metadataEpoch,
                schemaEpoch,
                dedupeWatermark,
                cdcWatermark,
                checkpointFileCount,
                checkpointTotalSizeBytes,
                checkpointChunkSizeBytes,
                checkpointManifestCrc32c,
                manifestCrc32c
        );
    }

    private static void validateManifestChecksum(int formatVersion,
                                                 ShardId partitionId,
                                                 long partitionEpoch,
                                                 long ratisTerm,
                                                 long ratisIndex,
                                                 long metadataEpoch,
                                                 long schemaEpoch,
                                                 long dedupeWatermark,
                                                 long cdcWatermark,
                                                 int checkpointFileCount,
                                                 long checkpointTotalSizeBytes,
                                                 int checkpointChunkSizeBytes,
                                                 long checkpointManifestCrc32c,
                                                 long manifestCrc32c) {
        long actualChecksum = manifestChecksum(
                formatVersion,
                partitionId,
                partitionEpoch,
                ratisTerm,
                ratisIndex,
                metadataEpoch,
                schemaEpoch,
                dedupeWatermark,
                cdcWatermark,
                checkpointFileCount,
                checkpointTotalSizeBytes,
                checkpointChunkSizeBytes,
                checkpointManifestCrc32c
        );
        if (actualChecksum != manifestCrc32c) {
            throw new IllegalArgumentException("Ratis partition snapshot manifest checksum mismatch");
        }
    }

    public static long checkpointManifestChecksum(CheckpointSnapshotManifest checkpointManifest) {
        requireNonNull(checkpointManifest, "checkpointManifest");
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(checkpointManifest.formatVersion());
            out.writeLong(checkpointManifest.commitIndex());
            out.writeInt(checkpointManifest.fileCount());
            out.writeLong(checkpointManifest.totalSizeBytes());
            out.writeInt(checkpointManifest.chunkSizeBytes());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to checksum checkpoint snapshot manifest", e);
        }
        return checksum(bytes.toByteArray());
    }

    private static long manifestChecksum(int formatVersion,
                                         ShardId partitionId,
                                         long partitionEpoch,
                                         long ratisTerm,
                                         long ratisIndex,
                                         long metadataEpoch,
                                         long schemaEpoch,
                                         long dedupeWatermark,
                                         long cdcWatermark,
                                         int checkpointFileCount,
                                         long checkpointTotalSizeBytes,
                                         int checkpointChunkSizeBytes,
                                         long checkpointManifestCrc32c) {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(formatVersion);
            out.writeLong(partitionId.value());
            out.writeLong(partitionEpoch);
            out.writeLong(ratisTerm);
            out.writeLong(ratisIndex);
            out.writeLong(metadataEpoch);
            out.writeLong(schemaEpoch);
            out.writeLong(dedupeWatermark);
            out.writeLong(cdcWatermark);
            out.writeInt(checkpointFileCount);
            out.writeLong(checkpointTotalSizeBytes);
            out.writeInt(checkpointChunkSizeBytes);
            out.writeLong(checkpointManifestCrc32c);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to checksum Ratis partition snapshot manifest", e);
        }
        return checksum(bytes.toByteArray());
    }

    private static long checksum(byte[] bytes) {
        var checksum = new CRC32C();
        checksum.update(bytes);
        return checksum.getValue();
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    private static void requireCrc32c(long value, String name) {
        if (value < 0 || value > 0xFFFF_FFFFL) {
            throw new IllegalArgumentException(name + " must be a CRC32C value");
        }
    }

    private static void requireNonNull(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
