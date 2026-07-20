package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport.CheckpointSnapshotManifest;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport.SnapshotLimits;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport.SnapshotInstaller;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

import org.rocksdb.RocksDB;

public final class RockserverRatisSnapshotSupport {

    private final RocksDBCheckpointSnapshotSupport checkpointSupport;

    public RockserverRatisSnapshotSupport(RocksDBCheckpointSnapshotSupport checkpointSupport) {
        this.checkpointSupport = requireNonNull(checkpointSupport, "checkpointSupport");
    }

    public static RockserverRatisSnapshotSupport forRocksDB(RocksDB db,
                                                            Path workDirectory,
                                                            SnapshotInstaller snapshotInstaller) {
        requireNonNull(db, "db");
        requireNonNull(workDirectory, "workDirectory");
        requireNonNull(snapshotInstaller, "snapshotInstaller");
        return new RockserverRatisSnapshotSupport(RocksDBCheckpointSnapshotSupport.forRocksDB(
                db,
                workDirectory,
                snapshotInstaller
        ));
    }

    public static RockserverRatisSnapshotSupport forRocksDB(RocksDB db,
                                                            Path workDirectory,
                                                            SnapshotInstaller snapshotInstaller,
                                                            int chunkSizeBytes) {
        return forRocksDB(
                db,
                workDirectory,
                snapshotInstaller,
                chunkSizeBytes,
                RocksDBCheckpointSnapshotSupport.DEFAULT_LIMITS
        );
    }

    public static RockserverRatisSnapshotSupport forRocksDB(RocksDB db,
                                                            Path workDirectory,
                                                            SnapshotInstaller snapshotInstaller,
                                                            int chunkSizeBytes,
                                                            SnapshotLimits limits) {
        requireNonNull(db, "db");
        requireNonNull(workDirectory, "workDirectory");
        requireNonNull(snapshotInstaller, "snapshotInstaller");
        requireNonNull(limits, "limits");
        return new RockserverRatisSnapshotSupport(RocksDBCheckpointSnapshotSupport.forRocksDB(
                db,
                workDirectory,
                snapshotInstaller,
                chunkSizeBytes,
                limits
        ));
    }

    public void takeSnapshot(RatisPartitionSnapshotMetadata metadata, Consumer<Object> snapshotChunkConsumer) {
        var snapshotMetadata = requireNonNull(metadata, "metadata");
        var chunkConsumer = requireNonNull(snapshotChunkConsumer, "snapshotChunkConsumer");
        var emittedManifest = new boolean[]{false};
        checkpointSupport.takeSnapshot(snapshotMetadata.ratisIndex(), chunk -> {
            if (!emittedManifest[0]) {
                if (!(chunk instanceof CheckpointSnapshotManifest checkpointManifest)) {
                    throw new IllegalStateException("RocksDB checkpoint snapshot did not start with a manifest");
                }
                chunkConsumer.accept(RatisPartitionSnapshotManifest.create(snapshotMetadata, checkpointManifest));
                emittedManifest[0] = true;
            }
            chunkConsumer.accept(chunk);
        });
        if (!emittedManifest[0]) {
            throw new IllegalStateException("RocksDB checkpoint snapshot emitted no manifest");
        }
    }

    public RatisPartitionSnapshotManifest installSnapshot(ShardId expectedPartitionId,
                                                          long expectedRatisIndex,
                                                          List<Object> snapshotChunks) {
        requireNonNull(expectedPartitionId, "expectedPartitionId");
        if (expectedRatisIndex < 0) {
            throw new IllegalArgumentException("expectedRatisIndex must be >= 0");
        }
        snapshotChunks = copySnapshotChunks(snapshotChunks);
        if (snapshotChunks.size() < 2) {
            throw new IllegalArgumentException("Ratis partition snapshot is missing manifest or checkpoint chunks");
        }
        if (!(snapshotChunks.getFirst() instanceof RatisPartitionSnapshotManifest manifest)) {
            throw new IllegalArgumentException("Ratis partition snapshot first chunk is not a partition manifest");
        }
        manifest.validateManifestChecksum();
        if (!manifest.partitionId().equals(expectedPartitionId)) {
            throw new IllegalArgumentException("Ratis partition snapshot partition " + manifest.partitionId()
                    + " does not match expected partition " + expectedPartitionId);
        }
        if (manifest.ratisIndex() != expectedRatisIndex) {
            throw new IllegalArgumentException("Ratis partition snapshot index " + manifest.ratisIndex()
                    + " does not match expected index " + expectedRatisIndex);
        }

        var checkpointChunks = snapshotChunks.subList(1, snapshotChunks.size());
        if (!(checkpointChunks.getFirst() instanceof CheckpointSnapshotManifest checkpointManifest)) {
            throw new IllegalArgumentException("Ratis partition snapshot is missing RocksDB checkpoint manifest");
        }
        manifest.validateCheckpointManifest(checkpointManifest);
        checkpointSupport.installSnapshot(expectedRatisIndex, checkpointChunks);
        return manifest;
    }

    private static List<Object> copySnapshotChunks(List<Object> snapshotChunks) {
        requireNonNull(snapshotChunks, "snapshotChunks");
        for (int chunkIndex = 0; chunkIndex < snapshotChunks.size(); chunkIndex++) {
            if (snapshotChunks.get(chunkIndex) == null) {
                throw new IllegalArgumentException("snapshotChunks chunk " + chunkIndex + " must not be null");
            }
        }
        return List.copyOf(snapshotChunks);
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
