package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.metadata.BackupManifest;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport.CheckpointSnapshotFileChunk;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport.CheckpointSnapshotManifest;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CheckedOutputStream;
import java.util.zip.CRC32C;

public final class PartitionBackupExecutor {

    private static final long MAX_CRC32C_VALUE = 0xffff_ffffL;
    private static final int EXPORTER_SHUTDOWN_TIMEOUT_SECONDS = 5;
    private static final String EXPORT_FAILURE_MESSAGE = "partition backup export failed";

    private final PartitionBackupLimits limits;

    public PartitionBackupExecutor(PartitionBackupLimits limits) {
        this.limits = requireNonNull(limits, "limits");
    }

    public List<PartitionBackupArtifact> export(BackupManifest manifest,
                                                PartitionSnapshotExporter exporter) {
        requireNonNull(manifest, "manifest");
        requireNonNull(exporter, "exporter");
        var partitionCuts = manifest.partitionCuts();
        if (partitionCuts.size() > limits.maxPartitionsPerBackup()) {
            throw new IllegalArgumentException("backup manifest contains " + partitionCuts.size()
                    + " partition cuts, exceeding limit " + limits.maxPartitionsPerBackup());
        }
        ExecutorService executor = Executors.newFixedThreadPool(limits.maxParallelPartitions(),
                backupThreadFactory());
        Throwable failure = null;
        try {
            var completion = new ExecutorCompletionService<PartitionBackupArtifact>(executor);
            var artifacts = new ArrayList<PartitionBackupArtifact>(partitionCuts.size());
            int submitted = 0;
            int initialSubmissions = Math.min(limits.maxParallelPartitions(), partitionCuts.size());
            for (; submitted < initialSubmissions; submitted++) {
                submit(completion, manifest, partitionCuts.get(submitted), exporter);
            }
            for (int completed = 0; completed < partitionCuts.size(); completed++) {
                artifacts.add(completion.take().get());
                if (submitted < partitionCuts.size()) {
                    submit(completion, manifest, partitionCuts.get(submitted), exporter);
                    submitted++;
                }
            }
            return artifacts.stream()
                    .sorted(Comparator.comparing(PartitionBackupArtifact::partitionId))
                    .toList();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failure = new CompletionException("partition backup export interrupted", e);
            throw (CompletionException) failure;
        } catch (ExecutionException e) {
            failure = unwrap(e.getCause());
            throw throwFailure(failure);
        } finally {
            shutdownNowAndAwait(executor, failure);
        }
    }

    private void submit(ExecutorCompletionService<PartitionBackupArtifact> completion,
                        BackupManifest manifest,
                        BackupManifest.PartitionCut cut,
                        PartitionSnapshotExporter exporter) {
        completion.submit(() -> exportOne(manifest, cut, exporter));
    }

    private PartitionBackupArtifact exportOne(BackupManifest manifest,
                                              BackupManifest.PartitionCut cut,
                                              PartitionSnapshotExporter exporter) {
        List<Object> snapshotChunks;
        var snapshotDescription = "partition backup snapshot for " + cut.partitionId();
        try {
            var exportedSnapshotChunks = exporter.export(cut);
            if (exportedSnapshotChunks == null) {
                throw new IllegalArgumentException(snapshotDescription + " chunks must not be null");
            }
            requireSnapshotChunkCount(
                    exportedSnapshotChunks.size(),
                    limits.maxSnapshotChunksPerPartition(),
                    snapshotDescription
            );
            snapshotChunks = copyAndValidateSnapshotChunks(exportedSnapshotChunks, snapshotDescription);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(exporterInterruptedMessage(cut.partitionId()), e);
        } catch (CompletionException e) {
            throw exporterCompletionFailure(cut.partitionId(), e);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new CompletionException(exporterFailureMessage(cut.partitionId()), e);
        }
        if (snapshotChunks.size() < 2) {
            throw new IllegalArgumentException("partition backup snapshot is missing required manifests");
        }
        if (!(snapshotChunks.getFirst() instanceof RatisPartitionSnapshotManifest snapshotManifest)) {
            throw new IllegalArgumentException("partition backup snapshot first chunk is not a Ratis manifest");
        }
        if (!snapshotManifest.partitionId().equals(cut.partitionId())) {
            throw new IllegalArgumentException("partition backup snapshot partition " + snapshotManifest.partitionId()
                    + " does not match backup cut partition " + cut.partitionId());
        }
        if (snapshotManifest.partitionEpoch() != cut.partitionEpoch()) {
            throw new IllegalArgumentException("partition backup snapshot epoch " + snapshotManifest.partitionEpoch()
                    + " does not match backup cut epoch " + cut.partitionEpoch());
        }
        if (snapshotManifest.ratisTerm() != cut.ratisTerm()) {
            throw new IllegalArgumentException("partition backup snapshot term " + snapshotManifest.ratisTerm()
                    + " does not match backup cut term " + cut.ratisTerm());
        }
        if (snapshotManifest.ratisIndex() != cut.ratisIndex()) {
            throw new IllegalArgumentException("partition backup snapshot index " + snapshotManifest.ratisIndex()
                    + " does not match backup cut index " + cut.ratisIndex());
        }
        if (snapshotManifest.metadataEpoch() != manifest.metadataEpoch()) {
            throw new IllegalArgumentException("partition backup snapshot metadata epoch "
                    + snapshotManifest.metadataEpoch() + " does not match backup manifest metadata epoch "
                    + manifest.metadataEpoch());
        }
        if (!(snapshotChunks.get(1) instanceof CheckpointSnapshotManifest checkpointManifest)) {
            throw new IllegalArgumentException("partition backup snapshot is missing RocksDB checkpoint manifest");
        }
        snapshotManifest.validateCheckpointManifest(checkpointManifest);

        long totalBytes = snapshotSizeBytes(
                snapshotChunks,
                limits.maxBytesPerPartition(),
                "partition backup snapshot for " + cut.partitionId()
        );
        if (totalBytes != snapshotManifest.checkpointTotalSizeBytes()) {
            throw new IllegalArgumentException("partition backup snapshot size " + totalBytes
                    + " does not match manifest size " + snapshotManifest.checkpointTotalSizeBytes());
        }
        return new PartitionBackupArtifact(
                cut.partitionId(),
                cut.partitionEpoch(),
                cut.ratisTerm(),
                cut.ratisIndex(),
                totalBytes,
                snapshotChecksum(snapshotChunks),
                snapshotChunks
        );
    }

    public static long snapshotChecksum(List<Object> snapshotChunks) {
        requireNonNull(snapshotChunks, "snapshotChunks");
        var checksum = new CRC32C();
        try (var out = new DataOutputStream(new CheckedOutputStream(OutputStream.nullOutputStream(), checksum))) {
            for (int chunkIndex = 0; chunkIndex < snapshotChunks.size(); chunkIndex++) {
                var snapshotChunk = requireSupportedSnapshotChunk(
                        snapshotChunks.get(chunkIndex),
                        chunkIndex,
                        "partition backup snapshot"
                );
                switch (snapshotChunk) {
                    case RatisPartitionSnapshotManifest manifest -> {
                        out.writeInt(1);
                        out.writeLong(manifest.manifestCrc32c());
                    }
                    case CheckpointSnapshotManifest manifest -> {
                        out.writeInt(2);
                        out.writeLong(RatisPartitionSnapshotManifest.checkpointManifestChecksum(manifest));
                    }
                    case CheckpointSnapshotFileChunk fileChunk -> {
                        long actualCrc32c = checkpointFileChunkChecksum(fileChunk);
                        if (actualCrc32c != fileChunk.crc32c()) {
                            throw new IllegalArgumentException("partition backup snapshot file "
                                    + fileChunk.relativePath() + " chunk " + fileChunk.chunkIndex()
                                    + " failed checksum validation");
                        }
                        out.writeInt(3);
                        out.writeUTF(fileChunk.relativePath());
                        out.writeInt(fileChunk.chunkIndex());
                        out.writeInt(fileChunk.chunkCount());
                        out.writeLong(actualCrc32c);
                    }
                    default -> throw new IllegalArgumentException("unsupported partition backup snapshot chunk: "
                            + snapshotChunk.getClass().getName());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to checksum partition backup snapshot chunks", e);
        }
        return checksum.getValue();
    }

    private static long checkpointFileChunkChecksum(CheckpointSnapshotFileChunk fileChunk) {
        var bytes = fileChunk.bytes();
        var checksum = new CRC32C();
        checksum.update(bytes, 0, bytes.length);
        return checksum.getValue();
    }

    static long snapshotSizeBytes(List<Object> snapshotChunks, long maxBytes, String description) {
        requireNonNull(snapshotChunks, "snapshotChunks");
        requireNonNull(description, "description");
        long totalBytes = 0;
        for (int chunkIndex = 0; chunkIndex < snapshotChunks.size(); chunkIndex++) {
            var snapshotChunk = requireSupportedSnapshotChunk(snapshotChunks.get(chunkIndex), chunkIndex, description);
            if (snapshotChunk instanceof CheckpointSnapshotFileChunk fileChunk) {
                totalBytes = addExact(totalBytes, fileChunk.sizeBytes(), description + " size");
                if (totalBytes > maxBytes) {
                    throw new IllegalArgumentException(description + " is " + totalBytes
                            + " bytes, exceeding limit " + maxBytes);
                }
            }
        }
        return totalBytes;
    }

    private static List<Object> copyAndValidateSnapshotChunks(List<Object> snapshotChunks, String description) {
        requireNonNull(snapshotChunks, "snapshotChunks");
        requireNonNull(description, "description");
        for (int chunkIndex = 0; chunkIndex < snapshotChunks.size(); chunkIndex++) {
            requireSupportedSnapshotChunk(snapshotChunks.get(chunkIndex), chunkIndex, description);
        }
        return List.copyOf(snapshotChunks);
    }

    private static Object requireSupportedSnapshotChunk(Object snapshotChunk, int chunkIndex, String description) {
        if (snapshotChunk == null) {
            throw new IllegalArgumentException(description + " chunk " + chunkIndex + " must not be null");
        }
        if (snapshotChunk instanceof RatisPartitionSnapshotManifest
                || snapshotChunk instanceof CheckpointSnapshotManifest
                || snapshotChunk instanceof CheckpointSnapshotFileChunk) {
            return snapshotChunk;
        }
        throw new IllegalArgumentException(description + " chunk " + chunkIndex + " has unsupported type: "
                + snapshotChunk.getClass().getName());
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException completionException
                && completionException.getCause() != null
                && !hasExplicitCompletionMessage(completionException)) {
            return unwrap(completionException.getCause());
        }
        if (failure instanceof RuntimeException || failure instanceof Error) {
            return failure;
        }
        return new CompletionException(EXPORT_FAILURE_MESSAGE, failure);
    }

    private static RuntimeException exporterCompletionFailure(ShardId partitionId, CompletionException failure) {
        var cause = unwrapCompletion(failure);
        if (cause instanceof Error error) {
            throw error;
        }
        if (cause instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (cause instanceof InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            return new CompletionException(exporterInterruptedMessage(partitionId), interruptedException);
        }
        if (cause instanceof Exception exception) {
            return new CompletionException(exporterFailureMessage(partitionId), exception);
        }
        return new CompletionException(exporterFailureMessage(partitionId), cause);
    }

    private static Throwable unwrapCompletion(Throwable failure) {
        if (failure instanceof CompletionException completionException && completionException.getCause() != null) {
            return unwrapCompletion(completionException.getCause());
        }
        return failure;
    }

    private static String exporterFailureMessage(ShardId partitionId) {
        return "partition backup exporter failed for partition " + partitionId;
    }

    private static String exporterInterruptedMessage(ShardId partitionId) {
        return "partition backup exporter interrupted for partition " + partitionId;
    }

    private static boolean hasExplicitCompletionMessage(CompletionException failure) {
        var cause = failure.getCause();
        var message = failure.getMessage();
        return cause != null && message != null && !message.equals(cause.toString());
    }

    private static void shutdownNowAndAwait(ExecutorService executor, Throwable failure) {
        executor.shutdownNow();
        var shutdownFailure = awaitExporterShutdown(executor);
        if (shutdownFailure == null) {
            return;
        }
        if (failure == null) {
            throw shutdownFailure;
        }
        failure.addSuppressed(shutdownFailure);
    }

    private static RuntimeException awaitExporterShutdown(ExecutorService executor) {
        var interrupted = false;
        var deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(EXPORTER_SHUTDOWN_TIMEOUT_SECONDS);
        try {
            while (true) {
                try {
                    var remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0) {
                        return exportersDidNotStopFailure();
                    }
                    if (executor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
                        return null;
                    }
                    return exportersDidNotStopFailure();
                } catch (InterruptedException _) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static IllegalStateException exportersDidNotStopFailure() {
        return new IllegalStateException("partition backup exporters did not stop within "
                + EXPORTER_SHUTDOWN_TIMEOUT_SECONDS + " seconds");
    }

    private static RuntimeException throwFailure(Throwable failure) {
        if (failure instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        return new CompletionException(EXPORT_FAILURE_MESSAGE, failure);
    }

    private static java.util.concurrent.ThreadFactory backupThreadFactory() {
        var threadId = new AtomicInteger();
        return task -> {
            var thread = new Thread(task, "rockserver-partition-backup-" + threadId.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    public record PartitionBackupLimits(int maxParallelPartitions,
                                        long maxBytesPerPartition,
                                        int maxPartitionsPerBackup,
                                        int maxSnapshotChunksPerPartition) {

        public static final int DEFAULT_MAX_PARTITIONS_PER_BACKUP = 4096;
        public static final int DEFAULT_MAX_SNAPSHOT_CHUNKS_PER_PARTITION = Math.addExact(
                RocksDBCheckpointSnapshotSupport.DEFAULT_LIMITS.maxChunkCount(),
                2
        );

        public PartitionBackupLimits(int maxParallelPartitions, long maxBytesPerPartition) {
            this(
                    maxParallelPartitions,
                    maxBytesPerPartition,
                    DEFAULT_MAX_PARTITIONS_PER_BACKUP,
                    DEFAULT_MAX_SNAPSHOT_CHUNKS_PER_PARTITION
            );
        }

        public PartitionBackupLimits(int maxParallelPartitions,
                                     long maxBytesPerPartition,
                                     int maxPartitionsPerBackup) {
            this(
                    maxParallelPartitions,
                    maxBytesPerPartition,
                    maxPartitionsPerBackup,
                    DEFAULT_MAX_SNAPSHOT_CHUNKS_PER_PARTITION
            );
        }

        public PartitionBackupLimits {
            if (maxParallelPartitions <= 0) {
                throw new IllegalArgumentException("maxParallelPartitions must be positive");
            }
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

    public record PartitionBackupArtifact(ShardId partitionId,
                                          long partitionEpoch,
                                          long ratisTerm,
                                          long ratisIndex,
                                          long totalSizeBytes,
                                          long snapshotChecksumCrc32c,
                                          List<Object> snapshotChunks) {

        public PartitionBackupArtifact {
            requireNonNull(partitionId, "partitionId");
            requireNonNegative(partitionEpoch, "partitionEpoch");
            requireNonNegative(ratisTerm, "ratisTerm");
            requireNonNegative(ratisIndex, "ratisIndex");
            requireNonNegative(totalSizeBytes, "totalSizeBytes");
            requireSnapshotChecksumCrc32c(snapshotChecksumCrc32c);
            snapshotChunks = copyAndValidateSnapshotChunks(snapshotChunks, "partition backup artifact snapshot");
        }
    }

    @FunctionalInterface
    public interface PartitionSnapshotExporter {

        List<Object> export(BackupManifest.PartitionCut cut) throws Exception;
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    private static void requireSnapshotChecksumCrc32c(long value) {
        if (value < 0 || value > MAX_CRC32C_VALUE) {
            throw new IllegalArgumentException("snapshotChecksumCrc32c must be an unsigned CRC32C value");
        }
    }

    static void requireSnapshotChunkCount(int chunkCount, int maxChunkCount, String description) {
        if (chunkCount > maxChunkCount) {
            throw new IllegalArgumentException(description + " contains " + chunkCount
                    + " chunks, exceeding limit " + maxChunkCount);
        }
    }

    private static long addExact(long first, long second, String description) {
        try {
            return Math.addExact(first, second);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(description + " exceeds supported range", e);
        }
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
