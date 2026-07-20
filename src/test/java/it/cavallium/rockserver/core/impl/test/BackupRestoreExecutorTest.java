package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.RockserverStorageKeyspace;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.ClientDedupeState;
import it.cavallium.rockserver.core.cluster.command.ClientDedupeStateCodec;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcEvent;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcEventCodec;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcToken;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationOp;
import it.cavallium.rockserver.core.cluster.metadata.BackupManifest;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import it.cavallium.rockserver.core.replication.ratis.BackupRestoreExecutor;
import it.cavallium.rockserver.core.replication.ratis.BackupRestorePlanner;
import it.cavallium.rockserver.core.replication.ratis.BackupRestorePlanner.BackupRestoreLimits;
import it.cavallium.rockserver.core.replication.ratis.BackupRestorePlanner.PartitionRestore;
import it.cavallium.rockserver.core.replication.ratis.BackupRestorePlanner.RestoreTarget;
import it.cavallium.rockserver.core.replication.ratis.PartitionBackupExecutor;
import it.cavallium.rockserver.core.replication.ratis.PartitionBackupExecutor.PartitionBackupArtifact;
import it.cavallium.rockserver.core.replication.ratis.PartitionBackupExecutor.PartitionBackupLimits;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionSnapshotManifest;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionSnapshotMetadata;
import it.cavallium.rockserver.core.replication.ratis.RockserverRatisSnapshotSupport;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

class BackupRestoreExecutorTest {

    @BeforeAll
    static void loadRocksDb() {
        RocksDBLoader.loadLibrary();
    }

    @Test
    void restoresSnapshotIntoNewClusterAndExposesSourceCutsForAudit(@TempDir Path directory) throws Exception {
        var sourceClusterId = clusterId(1);
        var targetClusterId = clusterId(2);
        var manifest = backupManifest();
        var artifact = realArtifact(directory, manifest);
        var targetDirectory = directory.resolve("restored-db");
        var targetSupport = new RockserverRatisSnapshotSupport(new RocksDBCheckpointSnapshotSupport(
                ignored -> {
                },
                directory.resolve("restore-work"),
                RocksDBCheckpointSnapshotSupport.replacingClosedDatabaseDirectory(targetDirectory),
                4
        ));
        var target = new RestoreTarget(targetClusterId, 0,
                Map.of(partitionId(), manifest.partitionCuts().getFirst().partitionEpoch()));

        var plan = new BackupRestoreExecutor().restore(manifest, sourceClusterId, target, List.of(artifact),
                (partition, backupArtifact) -> targetSupport.installSnapshot(
                        partition.targetPartitionId(),
                        partition.sourceRatisIndex(),
                        backupArtifact.snapshotChunks()
                ));

        assertEquals(manifest.partitionCuts(), plan.sourceManifest().partitionCuts());
        assertEquals(sourceClusterId, plan.sourceClusterId());
        assertEquals(targetClusterId, plan.targetClusterId());
        assertEquals(artifact.snapshotChecksumCrc32c(), plan.partitions().getFirst().sourceSnapshotChecksumCrc32c());
        try (var options = new Options().setCreateIfMissing(false);
             var restored = RocksDB.open(options, targetDirectory.toString())) {
            assertArrayEquals(bytes("value"), restored.get(userDataKey().toByteArray()));
            assertArrayEquals(bytes("schema"), restored.get(schemaKey().toByteArray()));
            assertArrayEquals(dedupeWatermarkValue(), restored.get(dedupeWatermarkKey().toByteArray()));
            assertArrayEquals(cdcPositionValue(), restored.get(cdcPositionKey().toByteArray()));
        }
    }

    @Test
    void sameClusterRestoreRequiresNewMetadataAndPartitionEpochs() throws Exception {
        var clusterId = clusterId(1);
        var manifest = backupManifest();
        var artifact = fakeArtifact(manifest);
        var installerCalls = new AtomicInteger();

        assertThrows(IllegalArgumentException.class, () -> new BackupRestoreExecutor().restore(
                manifest,
                clusterId,
                new RestoreTarget(clusterId, manifest.metadataEpoch(),
                        Map.of(partitionId(), manifest.partitionCuts().getFirst().partitionEpoch())),
                List.of(artifact),
                (ignoredPartition, ignoredArtifact) -> installerCalls.incrementAndGet()
        ));

        var plan = new BackupRestoreExecutor().restore(
                manifest,
                clusterId,
                new RestoreTarget(clusterId, manifest.metadataEpoch() + 1,
                        Map.of(partitionId(), manifest.partitionCuts().getFirst().partitionEpoch() + 1)),
                List.of(artifact),
                (ignoredPartition, ignoredArtifact) -> installerCalls.incrementAndGet()
        );

        assertEquals(1, installerCalls.get());
        assertEquals(manifest.partitionCuts().getFirst().partitionEpoch() + 1,
                plan.partitions().getFirst().targetPartitionEpoch());
    }

    @Test
    void rejectsMissingOrMismatchedArtifactsBeforeInstallerRuns() {
        var sourceClusterId = clusterId(1);
        var targetClusterId = clusterId(2);
        var manifest = backupManifest();
        var calls = new AtomicInteger();
        var target = new RestoreTarget(targetClusterId, 0,
                Map.of(partitionId(), manifest.partitionCuts().getFirst().partitionEpoch()));

        assertThrows(IllegalArgumentException.class, () -> new BackupRestoreExecutor().restore(
                manifest,
                sourceClusterId,
                target,
                List.of(),
                (ignoredPartition, ignoredArtifact) -> calls.incrementAndGet()
        ));
        var validArtifact = fakeArtifact(manifest);
        assertThrows(IllegalArgumentException.class, () -> new BackupRestoreExecutor().restore(
                manifest,
                sourceClusterId,
                target,
                List.of(new PartitionBackupArtifact(
                        partitionId(),
                        manifest.partitionCuts().getFirst().partitionEpoch(),
                        manifest.partitionCuts().getFirst().ratisTerm(),
                        manifest.partitionCuts().getFirst().ratisIndex() + 1,
                        validArtifact.totalSizeBytes(),
                        validArtifact.snapshotChecksumCrc32c(),
                        validArtifact.snapshotChunks()
                )),
                (ignoredPartition, ignoredArtifact) -> calls.incrementAndGet()
        ));
        var wrongSize = assertThrows(IllegalArgumentException.class, () -> new BackupRestoreExecutor().restore(
                manifest,
                sourceClusterId,
                target,
                List.of(new PartitionBackupArtifact(
                        validArtifact.partitionId(),
                        validArtifact.partitionEpoch(),
                        validArtifact.ratisTerm(),
                        validArtifact.ratisIndex(),
                        validArtifact.totalSizeBytes() + 1,
                        validArtifact.snapshotChecksumCrc32c(),
                        validArtifact.snapshotChunks()
                )),
                (ignoredPartition, ignoredArtifact) -> calls.incrementAndGet()
        ));
        assertTrue(wrongSize.getMessage().contains("snapshot chunk size"));
        var wrongChecksum = assertThrows(IllegalArgumentException.class, () -> new BackupRestoreExecutor().restore(
                manifest,
                sourceClusterId,
                target,
                List.of(new PartitionBackupArtifact(
                        validArtifact.partitionId(),
                        validArtifact.partitionEpoch(),
                        validArtifact.ratisTerm(),
                        validArtifact.ratisIndex(),
                        validArtifact.totalSizeBytes(),
                        validArtifact.snapshotChecksumCrc32c() + 1,
                        validArtifact.snapshotChunks()
                )),
                (ignoredPartition, ignoredArtifact) -> calls.incrementAndGet()
        ));
        assertTrue(wrongChecksum.getMessage().contains("snapshot chunk checksum"));

        var corruptFileChunk = assertThrows(IllegalArgumentException.class,
                () -> new BackupRestoreExecutor().restore(
                        manifest,
                        sourceClusterId,
                        target,
                        List.of(artifactWithMismatchedFileChecksum(validArtifact)),
                        (ignoredPartition, ignoredArtifact) -> calls.incrementAndGet()
                ));
        assertTrue(corruptFileChunk.getMessage().contains("failed checksum validation"));

        assertEquals(0, calls.get());
    }

    @Test
    void rejectsOutOfRangeRestoreChecksum() {
        assertThrows(IllegalArgumentException.class, () -> new PartitionRestore(
                partitionId(),
                partitionId(),
                1,
                2,
                3,
                4,
                5,
                0x1_0000_0000L
        ));
    }

    @Test
    void restoreTargetValidatesPartitionEpochEntriesBeforeCopying() {
        var nullPartitionId = new HashMap<ShardId, Long>();
        nullPartitionId.put(null, 1L);
        var nullPartitionIdError = assertThrows(IllegalArgumentException.class,
                () -> new RestoreTarget(clusterId(2), 0, nullPartitionId));

        var nullEpoch = new HashMap<ShardId, Long>();
        nullEpoch.put(partitionId(), null);
        var nullEpochError = assertThrows(IllegalArgumentException.class,
                () -> new RestoreTarget(clusterId(2), 0, nullEpoch));

        var negativeEpochError = assertThrows(IllegalArgumentException.class,
                () -> new RestoreTarget(clusterId(2), 0, Map.of(partitionId(), -1L)));

        assertEquals("targetPartitionId must not be null", nullPartitionIdError.getMessage());
        assertEquals("targetPartitionEpoch must not be null", nullEpochError.getMessage());
        assertTrue(negativeEpochError.getMessage().contains("targetPartitionEpoch"));
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() throws Exception {
        var sourceClusterId = clusterId(1);
        var targetClusterId = clusterId(2);
        var manifest = backupManifest();
        var artifact = fakeArtifact(manifest);
        var target = new RestoreTarget(targetClusterId, 0,
                Map.of(partitionId(), manifest.partitionCuts().getFirst().partitionEpoch()));
        var planner = new BackupRestorePlanner();
        var partition = new PartitionRestore(partitionId(), partitionId(), 1, 2, 3, 4, 5, 6);
        var nullArtifactsEntry = new ArrayList<PartitionBackupArtifact>();
        nullArtifactsEntry.add(null);
        var nullPartitionsEntry = new ArrayList<PartitionRestore>();
        nullPartitionsEntry.add(null);

        assertRejectsNull("planner must not be null", () -> new BackupRestoreExecutor(null));
        assertRejectsNull("limits must not be null", () -> new BackupRestorePlanner(null));
        assertRejectsNull("installer must not be null", () -> new BackupRestoreExecutor().restore(
                manifest,
                sourceClusterId,
                target,
                List.of(artifact),
                null
        ));

        assertRejectsNull("manifest must not be null",
                () -> planner.plan(null, sourceClusterId, target, List.of(artifact)));
        assertRejectsNull("sourceClusterId must not be null",
                () -> planner.plan(manifest, null, target, List.of(artifact)));
        assertRejectsNull("target must not be null",
                () -> planner.plan(manifest, sourceClusterId, null, List.of(artifact)));
        assertRejectsNull("artifacts must not be null",
                () -> planner.plan(manifest, sourceClusterId, target, null));
        assertRejectsNull("artifacts entry must not be null",
                () -> planner.plan(manifest, sourceClusterId, target, nullArtifactsEntry));

        assertRejectsNull("targetClusterId must not be null",
                () -> new RestoreTarget(null, 0, Map.of(partitionId(), 1L)));
        assertRejectsNull("targetPartitionEpochs must not be null",
                () -> new RestoreTarget(targetClusterId, 0, null));
        assertRejectsNull("sourceManifest must not be null",
                () -> new BackupRestorePlanner.BackupRestorePlan(
                        null,
                        sourceClusterId,
                        targetClusterId,
                        0,
                        List.of(partition)
                ));
        assertRejectsNull("sourceClusterId must not be null",
                () -> new BackupRestorePlanner.BackupRestorePlan(
                        manifest,
                        null,
                        targetClusterId,
                        0,
                        List.of(partition)
                ));
        assertRejectsNull("targetClusterId must not be null",
                () -> new BackupRestorePlanner.BackupRestorePlan(
                        manifest,
                        sourceClusterId,
                        null,
                        0,
                        List.of(partition)
                ));
        assertRejectsNull("partitions must not be null",
                () -> new BackupRestorePlanner.BackupRestorePlan(
                        manifest,
                        sourceClusterId,
                        targetClusterId,
                        0,
                        null
                ));
        assertRejectsNull("partitions entry must not be null",
                () -> new BackupRestorePlanner.BackupRestorePlan(
                        manifest,
                        sourceClusterId,
                        targetClusterId,
                        0,
                        nullPartitionsEntry
                ));
        assertRejectsNull("sourcePartitionId must not be null",
                () -> new PartitionRestore(null, partitionId(), 1, 2, 3, 4, 5, 6));
        assertRejectsNull("targetPartitionId must not be null",
                () -> new PartitionRestore(partitionId(), null, 1, 2, 3, 4, 5, 6));
        assertRejectsNull("partition must not be null",
                () -> {
                    throw new BackupRestoreExecutor.PartitionRestoreInstallException(null, new IOException("failed"));
                });
        assertRejectsNull("cause must not be null",
                () -> {
                    throw new BackupRestoreExecutor.PartitionRestoreInstallException(partition, null);
                });
    }

    @Test
    void rejectsRestoreResourceLimitsBeforeInstallerRuns() {
        var sourceClusterId = clusterId(1);
        var targetClusterId = clusterId(2);
        var manifest = backupManifest();
        var artifact = fakeArtifact(manifest);
        var calls = new AtomicInteger();
        var target = new RestoreTarget(targetClusterId, 0,
                Map.of(partitionId(), manifest.partitionCuts().getFirst().partitionEpoch()));

        var artifactCountError = assertThrows(IllegalArgumentException.class, () -> new BackupRestoreExecutor(
                new BackupRestorePlanner(new BackupRestoreLimits(Long.MAX_VALUE, 1, 3))
        ).restore(
                manifest,
                sourceClusterId,
                target,
                List.of(artifact, artifact),
                (ignoredPartition, ignoredArtifact) -> calls.incrementAndGet()
        ));
        var chunkCountError = assertThrows(IllegalArgumentException.class, () -> new BackupRestoreExecutor(
                new BackupRestorePlanner(new BackupRestoreLimits(Long.MAX_VALUE, 1, 2))
        ).restore(
                manifest,
                sourceClusterId,
                target,
                List.of(artifact),
                (ignoredPartition, ignoredArtifact) -> calls.incrementAndGet()
        ));
        var byteCountError = assertThrows(IllegalArgumentException.class, () -> new BackupRestoreExecutor(
                new BackupRestorePlanner(new BackupRestoreLimits(artifact.totalSizeBytes() - 1, 1, 3))
        ).restore(
                manifest,
                sourceClusterId,
                target,
                List.of(artifact),
                (ignoredPartition, ignoredArtifact) -> calls.incrementAndGet()
        ));

        assertTrue(artifactCountError.getMessage().contains("backup artifacts"));
        assertTrue(chunkCountError.getMessage().contains("chunks"));
        assertTrue(byteCountError.getMessage().contains("bytes"));
        assertEquals(0, calls.get());
    }

    @Test
    void restoreInstallerFailureIncludesPartitionContext() {
        var sourceClusterId = clusterId(1);
        var targetClusterId = clusterId(2);
        var manifest = backupManifest();
        var artifact = fakeArtifact(manifest);
        var target = new RestoreTarget(targetClusterId, 0,
                Map.of(partitionId(), manifest.partitionCuts().getFirst().partitionEpoch()));
        var installFailure = new IOException("install failed");

        var failure = assertThrows(BackupRestoreExecutor.PartitionRestoreInstallException.class,
                () -> new BackupRestoreExecutor().restore(
                        manifest,
                        sourceClusterId,
                        target,
                        List.of(artifact),
                        (ignoredPartition, ignoredArtifact) -> {
                            throw installFailure;
                        }
                ));

        assertSame(installFailure, failure.getCause());
        assertEquals(partitionId(), failure.partition().targetPartitionId());
        assertTrue(failure.getMessage().contains(partitionId().toString()));
        assertTrue(failure.getMessage().contains("source index 100"));
    }

    @Test
    void restoreInstallerInterruptionRestoresInterruptAndIncludesPartitionContext() {
        var sourceClusterId = clusterId(1);
        var targetClusterId = clusterId(2);
        var manifest = backupManifest();
        var artifact = fakeArtifact(manifest);
        var target = new RestoreTarget(targetClusterId, 0,
                Map.of(partitionId(), manifest.partitionCuts().getFirst().partitionEpoch()));
        var installFailure = new InterruptedException("install interrupted");
        try {
            var failure = assertThrows(BackupRestoreExecutor.PartitionRestoreInstallException.class,
                    () -> new BackupRestoreExecutor().restore(
                            manifest,
                            sourceClusterId,
                            target,
                            List.of(artifact),
                            (ignoredPartition, ignoredArtifact) -> {
                                throw installFailure;
                            }
                    ));

            assertSame(installFailure, failure.getCause());
            assertEquals(partitionId(), failure.partition().targetPartitionId());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    void restoreInstallerFatalFailureRetainsSeverity() {
        var sourceClusterId = clusterId(1);
        var targetClusterId = clusterId(2);
        var manifest = backupManifest();
        var artifact = fakeArtifact(manifest);
        var target = new RestoreTarget(targetClusterId, 0,
                Map.of(partitionId(), manifest.partitionCuts().getFirst().partitionEpoch()));
        var fatalFailure = new AssertionError("fatal install failed");

        var failure = assertThrows(AssertionError.class, () -> new BackupRestoreExecutor().restore(
                manifest,
                sourceClusterId,
                target,
                List.of(artifact),
                (ignoredPartition, ignoredArtifact) -> {
                    throw fatalFailure;
                }
        ));

        assertSame(fatalFailure, failure);
    }

    @Test
    void restoreInstallerWrappedFatalFailureRetainsSeverity() {
        var sourceClusterId = clusterId(1);
        var targetClusterId = clusterId(2);
        var manifest = backupManifest();
        var artifact = fakeArtifact(manifest);
        var target = new RestoreTarget(targetClusterId, 0,
                Map.of(partitionId(), manifest.partitionCuts().getFirst().partitionEpoch()));
        var fatalFailure = new AssertionError("wrapped fatal install failed");

        var failure = assertThrows(AssertionError.class, () -> new BackupRestoreExecutor().restore(
                manifest,
                sourceClusterId,
                target,
                List.of(artifact),
                (ignoredPartition, ignoredArtifact) -> {
                    throw new CompletionException(new CompletionException(fatalFailure));
                }
        ));

        assertSame(fatalFailure, failure);
    }

    @Test
    void restoreInstallerWrappedCheckedFailureIncludesPartitionContext() {
        var sourceClusterId = clusterId(1);
        var targetClusterId = clusterId(2);
        var manifest = backupManifest();
        var artifact = fakeArtifact(manifest);
        var target = new RestoreTarget(targetClusterId, 0,
                Map.of(partitionId(), manifest.partitionCuts().getFirst().partitionEpoch()));
        var installFailure = new IOException("wrapped install failed");

        var failure = assertThrows(BackupRestoreExecutor.PartitionRestoreInstallException.class,
                () -> new BackupRestoreExecutor().restore(
                        manifest,
                        sourceClusterId,
                        target,
                        List.of(artifact),
                        (ignoredPartition, ignoredArtifact) -> {
                            throw new CompletionException(installFailure);
                        }
                ));

        assertSame(installFailure, failure.getCause());
        assertEquals(partitionId(), failure.partition().targetPartitionId());
        assertTrue(failure.getMessage().contains("source index 100"));
    }

    @Test
    void restoreInstallerWrappedThrowableFailureIncludesPartitionContext() {
        var sourceClusterId = clusterId(1);
        var targetClusterId = clusterId(2);
        var manifest = backupManifest();
        var artifact = fakeArtifact(manifest);
        var target = new RestoreTarget(targetClusterId, 0,
                Map.of(partitionId(), manifest.partitionCuts().getFirst().partitionEpoch()));
        var installFailure = new Throwable("wrapped install throwable");

        var failure = assertThrows(BackupRestoreExecutor.PartitionRestoreInstallException.class,
                () -> new BackupRestoreExecutor().restore(
                        manifest,
                        sourceClusterId,
                        target,
                        List.of(artifact),
                        (ignoredPartition, ignoredArtifact) -> {
                            throw new CompletionException(new CompletionException(installFailure));
                        }
                ));

        var completionFailure = assertInstanceOf(CompletionException.class, failure.getCause());
        assertEquals(failure.getMessage(), completionFailure.getMessage());
        assertSame(installFailure, completionFailure.getCause());
        assertEquals(partitionId(), failure.partition().targetPartitionId());
    }

    @Test
    void restoreInstallerWrappedInterruptionRestoresInterruptAndIncludesPartitionContext() {
        var sourceClusterId = clusterId(1);
        var targetClusterId = clusterId(2);
        var manifest = backupManifest();
        var artifact = fakeArtifact(manifest);
        var target = new RestoreTarget(targetClusterId, 0,
                Map.of(partitionId(), manifest.partitionCuts().getFirst().partitionEpoch()));
        var installFailure = new InterruptedException("wrapped install interrupted");
        try {
            var failure = assertThrows(BackupRestoreExecutor.PartitionRestoreInstallException.class,
                    () -> new BackupRestoreExecutor().restore(
                            manifest,
                            sourceClusterId,
                            target,
                            List.of(artifact),
                            (ignoredPartition, ignoredArtifact) -> {
                                throw new CompletionException(installFailure);
                            }
                    ));

            assertSame(installFailure, failure.getCause());
            assertEquals(partitionId(), failure.partition().targetPartitionId());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    void rejectsInvalidRestoreLimits() {
        assertThrows(IllegalArgumentException.class, () -> new BackupRestoreLimits(-1, 1, 2));
        assertThrows(IllegalArgumentException.class, () -> new BackupRestoreLimits(1, 0, 2));
        assertThrows(IllegalArgumentException.class, () -> new BackupRestoreLimits(1, 1, 1));
    }

    private static PartitionBackupArtifact realArtifact(Path directory, BackupManifest manifest) throws Exception {
        var sourceDirectory = directory.resolve("source-db");
        var chunks = new ArrayList<>();
        var cut = manifest.partitionCuts().getFirst();
        try (var options = new Options().setCreateIfMissing(true);
             var source = RocksDB.open(options, sourceDirectory.toString())) {
            source.put(userDataKey().toByteArray(), bytes("value"));
            source.put(schemaKey().toByteArray(), bytes("schema"));
            source.put(dedupeWatermarkKey().toByteArray(), dedupeWatermarkValue());
            source.put(cdcPositionKey().toByteArray(), cdcPositionValue());
            try (var flushOptions = new FlushOptions().setWaitForFlush(true)) {
                source.flush(flushOptions);
            }
            var snapshotSupport = RockserverRatisSnapshotSupport.forRocksDB(
                    source,
                    directory.resolve("snapshot-work"),
                    ignored -> {
                    },
                    4
            );
            snapshotSupport.takeSnapshot(new RatisPartitionSnapshotMetadata(
                    cut.partitionId(),
                    cut.partitionEpoch(),
                    cut.ratisTerm(),
                    cut.ratisIndex(),
                    manifest.metadataEpoch(),
                    3,
                    0,
                    0
            ), chunks::add);
        }
        return new PartitionBackupExecutor(new PartitionBackupLimits(1, 64 * 1024))
                .export(manifest, ignored -> chunks)
                .getFirst();
    }

    private static BackupManifest backupManifest() {
        return new BackupManifest("backup-12", 12, 11,
                List.of(new BackupManifest.PartitionCut(partitionId(), 3, 5, 100, 12)));
    }

    private static void assertRejectsNull(String expectedMessage, Executable executable) {
        var error = assertThrows(IllegalArgumentException.class, executable);
        assertEquals(expectedMessage, error.getMessage());
    }

    private static PartitionBackupArtifact fakeArtifact(BackupManifest manifest) {
        var cut = manifest.partitionCuts().getFirst();
        var bytes = bytes("snapshot");
        var checkpointManifest = new RocksDBCheckpointSnapshotSupport.CheckpointSnapshotManifest(
                1,
                cut.ratisIndex(),
                1,
                bytes.length,
                1024
        );
        var snapshotManifest = RatisPartitionSnapshotManifest.create(
                new RatisPartitionSnapshotMetadata(
                        cut.partitionId(),
                        cut.partitionEpoch(),
                        cut.ratisTerm(),
                        cut.ratisIndex(),
                        manifest.metadataEpoch(),
                        3,
                        0,
                        0
                ),
                checkpointManifest
        );
        List<Object> chunks = List.of(
                snapshotManifest,
                checkpointManifest,
                new RocksDBCheckpointSnapshotSupport.CheckpointSnapshotFileChunk(
                        "CURRENT",
                        0,
                        1,
                        bytes,
                        checksum(bytes)
                )
        );
        return new PartitionBackupArtifact(
                cut.partitionId(),
                cut.partitionEpoch(),
                cut.ratisTerm(),
                cut.ratisIndex(),
                bytes.length,
                PartitionBackupExecutor.snapshotChecksum(chunks),
                chunks
        );
    }

    private static PartitionBackupArtifact artifactWithMismatchedFileChecksum(PartitionBackupArtifact artifact) {
        var chunksWithMismatchedFileChecksum = new ArrayList<>(artifact.snapshotChunks());
        var fileChunk = (RocksDBCheckpointSnapshotSupport.CheckpointSnapshotFileChunk)
                chunksWithMismatchedFileChecksum.get(2);
        chunksWithMismatchedFileChecksum.set(2, new RocksDBCheckpointSnapshotSupport.CheckpointSnapshotFileChunk(
                fileChunk.relativePath(),
                fileChunk.chunkIndex(),
                fileChunk.chunkCount(),
                fileChunk.bytes(),
                fileChunk.crc32c() ^ 1
        ));
        return new PartitionBackupArtifact(
                artifact.partitionId(),
                artifact.partitionEpoch(),
                artifact.ratisTerm(),
                artifact.ratisIndex(),
                artifact.totalSizeBytes(),
                artifact.snapshotChecksumCrc32c(),
                chunksWithMismatchedFileChecksum
        );
    }

    private static ShardId partitionId() {
        return new ShardId(7);
    }

    private static it.cavallium.buffer.Buf userDataKey() {
        return RockserverStorageKeyspace.userData(4, it.cavallium.buffer.Buf.wrap(new byte[]{0x18}));
    }

    private static it.cavallium.buffer.Buf schemaKey() {
        return RockserverStorageKeyspace.schema(4, 3);
    }

    private static it.cavallium.buffer.Buf dedupeWatermarkKey() {
        return RockserverStorageKeyspace.dedupeClientState(requestId().clientId());
    }

    private static byte[] dedupeWatermarkValue() {
        return ClientDedupeStateCodec.encode(ClientDedupeState.acknowledged(requestId(), 5, 100));
    }

    private static it.cavallium.buffer.Buf cdcPositionKey() {
        return RockserverStorageKeyspace.cdcEvent(100, 0);
    }

    private static byte[] cdcPositionValue() {
        return ReplicatedCdcEventCodec.encode(new ReplicatedCdcEvent(
                new ReplicatedCdcToken(11, partitionId(), 3, 100, 0),
                3,
                4,
                it.cavallium.buffer.Buf.wrap(new byte[]{0x18}),
                it.cavallium.buffer.Buf.wrap(bytes("value")),
                ReplicatedMutationOp.PUT
        ));
    }

    private static ClientRequestId requestId() {
        return new ClientRequestId(new ClientId(new UUID(0, 7)), 42);
    }

    private static ClusterId clusterId(long leastSignificantBits) {
        return new ClusterId(new UUID(0, leastSignificantBits));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static long checksum(byte[] bytes) {
        var checksum = new CRC32C();
        checksum.update(bytes, 0, bytes.length);
        return checksum.getValue();
    }
}
