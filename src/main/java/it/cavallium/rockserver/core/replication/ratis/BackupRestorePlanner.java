package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.metadata.BackupManifest;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport;
import it.cavallium.rockserver.core.replication.ratis.PartitionBackupExecutor.PartitionBackupArtifact;
import it.cavallium.rockserver.core.replication.ratis.PartitionBackupExecutor.PartitionBackupLimits;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class BackupRestorePlanner {

    private static final long MAX_CRC32C_VALUE = 0xffff_ffffL;

    private final BackupRestoreLimits limits;

    public BackupRestorePlanner() {
        this(BackupRestoreLimits.DEFAULT_LIMITS);
    }

    public BackupRestorePlanner(BackupRestoreLimits limits) {
        this.limits = requireNonNull(limits, "limits");
    }

    public BackupRestorePlan plan(BackupManifest manifest,
                                  ClusterId sourceClusterId,
                                  RestoreTarget target,
                                  List<PartitionBackupArtifact> artifacts) {
        requireNonNull(manifest, "manifest");
        requireNonNull(sourceClusterId, "sourceClusterId");
        requireNonNull(target, "target");
        requireNonNull(artifacts, "artifacts");
        requirePartitionCount(manifest.partitionCuts().size(), "backup manifest partition cuts");
        requirePartitionCount(artifacts.size(), "backup artifacts");
        var artifactsByPartition = artifactsByPartition(artifacts);
        var restoringIntoNewCluster = !sourceClusterId.equals(target.targetClusterId());
        if (!restoringIntoNewCluster && target.targetMetadataEpoch() <= manifest.metadataEpoch()) {
            throw new IllegalArgumentException("same-cluster restore must advance metadata epoch");
        }

        var partitionRestores = new ArrayList<PartitionRestore>(manifest.partitionCuts().size());
        for (BackupManifest.PartitionCut cut : manifest.partitionCuts()) {
            var artifact = artifactsByPartition.remove(cut.partitionId());
            if (artifact == null) {
                throw new IllegalArgumentException("missing backup artifact for partition " + cut.partitionId());
            }
            validateArtifact(cut, artifact);
            var targetPartitionEpoch = target.targetPartitionEpoch(cut.partitionId());
            if (!restoringIntoNewCluster && targetPartitionEpoch <= cut.partitionEpoch()) {
                throw new IllegalArgumentException("same-cluster restore for partition " + cut.partitionId()
                        + " must use a new partition epoch");
            }
            partitionRestores.add(new PartitionRestore(
                    cut.partitionId(),
                    cut.partitionId(),
                    cut.partitionEpoch(),
                    targetPartitionEpoch,
                    cut.ratisTerm(),
                    cut.ratisIndex(),
                    artifact.totalSizeBytes(),
                    artifact.snapshotChecksumCrc32c()
            ));
        }
        if (!artifactsByPartition.isEmpty()) {
            throw new IllegalArgumentException("backup artifacts contain partitions not present in manifest: "
                    + artifactsByPartition.keySet());
        }
        if (!target.targetPartitionEpochs().keySet().equals(manifestPartitionIds(manifest))) {
            throw new IllegalArgumentException("restore target partition epochs must match backup manifest partitions");
        }
        return new BackupRestorePlan(
                manifest,
                sourceClusterId,
                target.targetClusterId(),
                target.targetMetadataEpoch(),
                partitionRestores
        );
    }

    private static Map<ShardId, PartitionBackupArtifact> artifactsByPartition(List<PartitionBackupArtifact> artifacts) {
        var artifactsByPartition = new HashMap<ShardId, PartitionBackupArtifact>();
        for (PartitionBackupArtifact artifact : artifacts) {
            var resolvedArtifact = requireNonNull(artifact, "artifacts entry");
            if (artifactsByPartition.putIfAbsent(resolvedArtifact.partitionId(), resolvedArtifact) != null) {
                throw new IllegalArgumentException("duplicate backup artifact for partition "
                        + resolvedArtifact.partitionId());
            }
        }
        return artifactsByPartition;
    }

    private void validateArtifact(BackupManifest.PartitionCut cut, PartitionBackupArtifact artifact) {
        if (artifact.partitionEpoch() != cut.partitionEpoch()) {
            throw new IllegalArgumentException("backup artifact epoch " + artifact.partitionEpoch()
                    + " does not match manifest cut epoch " + cut.partitionEpoch());
        }
        if (artifact.ratisTerm() != cut.ratisTerm()) {
            throw new IllegalArgumentException("backup artifact term " + artifact.ratisTerm()
                    + " does not match manifest cut term " + cut.ratisTerm());
        }
        if (artifact.ratisIndex() != cut.ratisIndex()) {
            throw new IllegalArgumentException("backup artifact index " + artifact.ratisIndex()
                    + " does not match manifest cut index " + cut.ratisIndex());
        }
        PartitionBackupExecutor.requireSnapshotChunkCount(
                artifact.snapshotChunks().size(),
                limits.maxSnapshotChunksPerPartition(),
                "backup artifact snapshot for " + artifact.partitionId()
        );
        long actualSizeBytes = PartitionBackupExecutor.snapshotSizeBytes(
                artifact.snapshotChunks(),
                limits.maxBytesPerPartition(),
                "backup artifact snapshot for " + artifact.partitionId()
        );
        if (artifact.totalSizeBytes() != actualSizeBytes) {
            throw new IllegalArgumentException("backup artifact size " + artifact.totalSizeBytes()
                    + " does not match snapshot chunk size " + actualSizeBytes);
        }
        long actualChecksum = PartitionBackupExecutor.snapshotChecksum(artifact.snapshotChunks());
        if (artifact.snapshotChecksumCrc32c() != actualChecksum) {
            throw new IllegalArgumentException("backup artifact checksum " + artifact.snapshotChecksumCrc32c()
                    + " does not match snapshot chunk checksum " + actualChecksum);
        }
    }

    private static java.util.Set<ShardId> manifestPartitionIds(BackupManifest manifest) {
        var partitionIds = new java.util.HashSet<ShardId>();
        for (BackupManifest.PartitionCut cut : manifest.partitionCuts()) {
            partitionIds.add(cut.partitionId());
        }
        return partitionIds;
    }

    private void requirePartitionCount(int partitionCount, String description) {
        if (partitionCount > limits.maxPartitionsPerBackup()) {
            throw new IllegalArgumentException(description + " contain " + partitionCount
                    + " partitions, exceeding limit " + limits.maxPartitionsPerBackup());
        }
    }

    public record BackupRestoreLimits(long maxBytesPerPartition,
                                      int maxPartitionsPerBackup,
                                      int maxSnapshotChunksPerPartition) {

        public static final BackupRestoreLimits DEFAULT_LIMITS = new BackupRestoreLimits(
                RocksDBCheckpointSnapshotSupport.DEFAULT_LIMITS.maxTotalSizeBytes(),
                PartitionBackupLimits.DEFAULT_MAX_PARTITIONS_PER_BACKUP,
                PartitionBackupLimits.DEFAULT_MAX_SNAPSHOT_CHUNKS_PER_PARTITION
        );

        public BackupRestoreLimits {
            if (maxBytesPerPartition < 0) {
                throw new IllegalArgumentException("maxBytesPerPartition must be >= 0");
            }
            if (maxPartitionsPerBackup <= 0) {
                throw new IllegalArgumentException("maxPartitionsPerBackup must be positive");
            }
            if (maxSnapshotChunksPerPartition < 2) {
                throw new IllegalArgumentException("maxSnapshotChunksPerPartition must be at least 2");
            }
        }
    }

    public record RestoreTarget(ClusterId targetClusterId,
                                long targetMetadataEpoch,
                                Map<ShardId, Long> targetPartitionEpochs) {

        public RestoreTarget {
            requireNonNull(targetClusterId, "targetClusterId");
            requireNonNegative(targetMetadataEpoch, "targetMetadataEpoch");
            targetPartitionEpochs = copyPartitionEpochs(targetPartitionEpochs);
        }

        private long targetPartitionEpoch(ShardId partitionId) {
            var epoch = targetPartitionEpochs.get(partitionId);
            if (epoch == null) {
                throw new IllegalArgumentException("missing restore target epoch for partition " + partitionId);
            }
            return epoch;
        }
    }

    public record BackupRestorePlan(BackupManifest sourceManifest,
                                    ClusterId sourceClusterId,
                                    ClusterId targetClusterId,
                                    long targetMetadataEpoch,
                                    List<PartitionRestore> partitions) {

        public BackupRestorePlan {
            requireNonNull(sourceManifest, "sourceManifest");
            requireNonNull(sourceClusterId, "sourceClusterId");
            requireNonNull(targetClusterId, "targetClusterId");
            requireNonNegative(targetMetadataEpoch, "targetMetadataEpoch");
            partitions = copyPartitionRestores(partitions);
        }
    }

    public record PartitionRestore(ShardId sourcePartitionId,
                                   ShardId targetPartitionId,
                                   long sourcePartitionEpoch,
                                   long targetPartitionEpoch,
                                   long sourceRatisTerm,
                                   long sourceRatisIndex,
                                   long sourceSnapshotBytes,
                                   long sourceSnapshotChecksumCrc32c) {

        public PartitionRestore {
            requireNonNull(sourcePartitionId, "sourcePartitionId");
            requireNonNull(targetPartitionId, "targetPartitionId");
            requireNonNegative(sourcePartitionEpoch, "sourcePartitionEpoch");
            requireNonNegative(targetPartitionEpoch, "targetPartitionEpoch");
            requireNonNegative(sourceRatisTerm, "sourceRatisTerm");
            requireNonNegative(sourceRatisIndex, "sourceRatisIndex");
            requireNonNegative(sourceSnapshotBytes, "sourceSnapshotBytes");
            requireSourceSnapshotChecksumCrc32c(sourceSnapshotChecksumCrc32c);
        }
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    private static Map<ShardId, Long> copyPartitionEpochs(Map<ShardId, Long> targetPartitionEpochs) {
        requireNonNull(targetPartitionEpochs, "targetPartitionEpochs");
        var validatedEpochs = new HashMap<ShardId, Long>(targetPartitionEpochs.size());
        for (var entry : targetPartitionEpochs.entrySet()) {
            var resolvedEntry = requireNonNull(entry, "targetPartitionEpochs entry");
            var partitionId = requireNonNull(resolvedEntry.getKey(), "targetPartitionId");
            var partitionEpoch = requireNonNull(resolvedEntry.getValue(), "targetPartitionEpoch");
            requireNonNegative(partitionEpoch, "targetPartitionEpoch");
            validatedEpochs.put(partitionId, partitionEpoch);
        }
        return Map.copyOf(validatedEpochs);
    }

    private static List<PartitionRestore> copyPartitionRestores(List<PartitionRestore> partitions) {
        requireNonNull(partitions, "partitions");
        var validatedPartitions = new ArrayList<PartitionRestore>(partitions.size());
        for (PartitionRestore partition : partitions) {
            validatedPartitions.add(requireNonNull(partition, "partitions entry"));
        }
        validatedPartitions.sort(Comparator.comparing(PartitionRestore::targetPartitionId));
        return List.copyOf(validatedPartitions);
    }

    private static void requireSourceSnapshotChecksumCrc32c(long value) {
        if (value < 0 || value > MAX_CRC32C_VALUE) {
            throw new IllegalArgumentException("sourceSnapshotChecksumCrc32c must be an unsigned CRC32C value");
        }
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
