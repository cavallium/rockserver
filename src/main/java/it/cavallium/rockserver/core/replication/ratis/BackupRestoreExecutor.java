package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.metadata.BackupManifest;
import it.cavallium.rockserver.core.replication.ratis.BackupRestorePlanner.BackupRestorePlan;
import it.cavallium.rockserver.core.replication.ratis.BackupRestorePlanner.PartitionRestore;
import it.cavallium.rockserver.core.replication.ratis.BackupRestorePlanner.RestoreTarget;
import it.cavallium.rockserver.core.replication.ratis.PartitionBackupExecutor.PartitionBackupArtifact;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletionException;

public final class BackupRestoreExecutor {

    private final BackupRestorePlanner planner;

    public BackupRestoreExecutor() {
        this(new BackupRestorePlanner());
    }

    public BackupRestoreExecutor(BackupRestorePlanner planner) {
        this.planner = requireNonNull(planner, "planner");
    }

    public BackupRestorePlan restore(BackupManifest manifest,
                                     ClusterId sourceClusterId,
                                     RestoreTarget target,
                                     List<PartitionBackupArtifact> artifacts,
                                     PartitionRestoreInstaller installer) throws Exception {
        requireNonNull(installer, "installer");
        var plan = planner.plan(manifest, sourceClusterId, target, artifacts);
        var artifactsByPartition = artifactsByPartition(artifacts);
        for (PartitionRestore partition : plan.partitions()) {
            try {
                installer.install(partition, artifactsByPartition.get(partition.sourcePartitionId()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PartitionRestoreInstallException(partition, e);
            } catch (RuntimeException e) {
                throw installRuntimeFailure(partition, e);
            } catch (Exception e) {
                throw new PartitionRestoreInstallException(partition, e);
            }
        }
        return plan;
    }

    private static PartitionRestoreInstallException installRuntimeFailure(PartitionRestore partition,
                                                                          RuntimeException failure) {
        var cause = unwrapCompletionException(failure);
        if (cause instanceof Error error) {
            throw error;
        }
        if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        if (cause instanceof Exception exception) {
            return new PartitionRestoreInstallException(partition, exception);
        }
        return new PartitionRestoreInstallException(partition,
                new CompletionException(installFailureMessage(partition), cause));
    }

    private static Throwable unwrapCompletionException(Throwable failure) {
        if (failure instanceof CompletionException completionException && completionException.getCause() != null) {
            return unwrapCompletionException(completionException.getCause());
        }
        return failure;
    }

    private static java.util.Map<ShardId, PartitionBackupArtifact> artifactsByPartition(
            List<PartitionBackupArtifact> artifacts) {
        var artifactsByPartition = new HashMap<ShardId, PartitionBackupArtifact>();
        for (PartitionBackupArtifact artifact : artifacts) {
            var resolvedArtifact = requireNonNull(artifact, "artifacts entry");
            artifactsByPartition.put(resolvedArtifact.partitionId(), resolvedArtifact);
        }
        return artifactsByPartition;
    }

    @FunctionalInterface
    public interface PartitionRestoreInstaller {

        void install(PartitionRestore partition, PartitionBackupArtifact artifact) throws Exception;
    }

    public static final class PartitionRestoreInstallException extends Exception {

        private final PartitionRestore partition;

        public PartitionRestoreInstallException(PartitionRestore partition, Exception cause) {
            super(installFailureMessage(partition), requireNonNull(cause, "cause"));
            this.partition = requireNonNull(partition, "partition");
        }

        public PartitionRestore partition() {
            return partition;
        }
    }

    private static String installFailureMessage(PartitionRestore partition) {
        requireNonNull(partition, "partition");
        return "Backup restore installer failed for partition "
                + partition.targetPartitionId()
                + " from source index " + partition.sourceRatisIndex();
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
