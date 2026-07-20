package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.metadata.BackupManifest;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport.CheckpointSnapshotFileChunk;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport.CheckpointSnapshotManifest;
import it.cavallium.rockserver.core.replication.ratis.PartitionBackupExecutor;
import it.cavallium.rockserver.core.replication.ratis.PartitionBackupExecutor.PartitionBackupArtifact;
import it.cavallium.rockserver.core.replication.ratis.PartitionBackupExecutor.PartitionBackupLimits;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionSnapshotManifest;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionSnapshotMetadata;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class PartitionBackupExecutorTest {

    @Test
    void exportsPartitionSnapshotsWithConcurrencyLimitAndChecksums() throws Exception {
        var manifest = backupManifest();
        var active = new AtomicInteger();
        var maxActive = new AtomicInteger();
        var started = new AtomicInteger();
        var firstTwoStarted = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var executor = new PartitionBackupExecutor(new PartitionBackupLimits(2, 1024));

        var future = CompletableFuture.supplyAsync(() -> executor.export(manifest, cut -> {
            int currentActive = active.incrementAndGet();
            maxActive.accumulateAndGet(currentActive, Math::max);
            int currentStarted = started.incrementAndGet();
            if (currentStarted <= 2) {
                firstTwoStarted.countDown();
                assertTrue(release.await(5, TimeUnit.SECONDS));
            }
            try {
                return snapshotChunks(manifest, cut, "partition-" + cut.partitionId());
            } finally {
                active.decrementAndGet();
            }
        }));

        assertTrue(firstTwoStarted.await(5, TimeUnit.SECONDS));
        assertEquals(2, active.get());
        assertEquals(2, started.get());
        release.countDown();

        var artifacts = future.get(5, TimeUnit.SECONDS);

        assertEquals(List.of(new ShardId(1), new ShardId(2), new ShardId(3)), artifacts.stream()
                .map(PartitionBackupExecutor.PartitionBackupArtifact::partitionId)
                .toList());
        assertEquals(2, maxActive.get());
        for (var artifact : artifacts) {
            assertEquals(PartitionBackupExecutor.snapshotChecksum(artifact.snapshotChunks()),
                    artifact.snapshotChecksumCrc32c());
            assertTrue(artifact.totalSizeBytes() > 0);
        }
    }

    @Test
    void rejectsSnapshotThatDoesNotMatchBackupCut() {
        var manifest = backupManifest();
        var executor = new PartitionBackupExecutor(new PartitionBackupLimits(1, 1024));

        var error = assertThrows(IllegalArgumentException.class, () -> executor.export(manifest, cut ->
                snapshotChunks(manifest, backupCut(new ShardId(99), cut.ratisTerm(), cut.ratisIndex()),
                        "wrong-partition")));

        assertTrue(error.getMessage().contains("partition"));
    }

    @Test
    void rejectsSnapshotAbovePartitionByteLimit() {
        var manifest = backupManifest();
        var executor = new PartitionBackupExecutor(new PartitionBackupLimits(1, 4));

        var error = assertThrows(IllegalArgumentException.class, () -> executor.export(manifest, cut ->
                snapshotChunks(manifest, cut, "too-large")));

        assertTrue(error.getMessage().contains("exceeding limit"));
    }

    @Test
    void rejectsManifestAbovePartitionLimitBeforeExport() {
        var manifest = backupManifest();
        var executor = new PartitionBackupExecutor(new PartitionBackupLimits(2, 1024, 2));

        var error = assertThrows(IllegalArgumentException.class, () -> executor.export(manifest, ignored -> {
            throw new AssertionError("exporter should not run");
        }));

        assertTrue(error.getMessage().contains("partition cuts"));
    }

    @Test
    void rejectsSnapshotChunkCountBeforeCopyingExportedList() {
        var manifest = backupManifest();
        var executor = new PartitionBackupExecutor(new PartitionBackupLimits(1, 1024, 3, 2));
        var excessiveChunks = new AbstractList<>() {
            @Override
            public Object get(int index) {
                throw new AssertionError("oversized chunk list must not be copied");
            }

            @Override
            public int size() {
                return 3;
            }
        };

        var error = assertThrows(IllegalArgumentException.class,
                () -> executor.export(manifest, ignored -> excessiveChunks));

        assertTrue(error.getMessage().contains("chunks"));
        assertTrue(error.getMessage().contains("exceeding limit"));
    }

    @Test
    void rejectsNullSnapshotChunkListFromExporter() {
        var manifest = backupManifest();
        var executor = new PartitionBackupExecutor(new PartitionBackupLimits(1, 1024));

        var error = assertThrows(IllegalArgumentException.class,
                () -> executor.export(manifest, _ -> null));

        assertTrue(error.getMessage().contains("chunks must not be null"));
    }

    @Test
    void rejectsNullSnapshotChunkBeforeCopyingExportedList() {
        var manifest = backupManifest();
        var executor = new PartitionBackupExecutor(new PartitionBackupLimits(1, 1024));

        var error = assertThrows(IllegalArgumentException.class, () -> executor.export(manifest, cut -> {
            var chunks = new ArrayList<>(snapshotChunks(manifest, cut, "null-chunk"));
            chunks.set(2, null);
            return chunks;
        }));

        assertTrue(error.getMessage().contains("chunk 2 must not be null"));
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var manifest = backupManifest();
        var executor = new PartitionBackupExecutor(new PartitionBackupLimits(1, 1024));
        var nullSnapshotChunk = new ArrayList<>();
        nullSnapshotChunk.add(null);

        assertRejectsNull("limits must not be null", () -> new PartitionBackupExecutor(null));
        assertRejectsNull("manifest must not be null", () -> executor.export(null, _ -> List.of()));
        assertRejectsNull("exporter must not be null", () -> executor.export(manifest, null));
        assertRejectsNull("snapshotChunks must not be null", () -> PartitionBackupExecutor.snapshotChecksum(null));
        assertRejectsNull("partitionId must not be null", () -> new PartitionBackupArtifact(
                null,
                1,
                2,
                3,
                4,
                5,
                List.of()
        ));
        assertRejectsNull("snapshotChunks must not be null", () -> new PartitionBackupArtifact(
                new ShardId(7),
                1,
                2,
                3,
                4,
                5,
                null
        ));
        assertRejectsNull("partition backup artifact snapshot chunk 0 must not be null",
                () -> new PartitionBackupArtifact(
                        new ShardId(7),
                        1,
                        2,
                        3,
                        4,
                        5,
                        nullSnapshotChunk
                ));
    }

    @Test
    void rejectsUnsupportedSnapshotChunkBeforeChecksum() {
        var manifest = backupManifest();
        var cut = manifest.partitionCuts().getFirst();
        var chunks = new ArrayList<>(snapshotChunks(manifest, cut, "unsupported-chunk"));
        chunks.set(2, "unsupported");

        var error = assertThrows(IllegalArgumentException.class,
                () -> PartitionBackupExecutor.snapshotChecksum(chunks));

        assertTrue(error.getMessage().contains("chunk 2 has unsupported type: java.lang.String"));
    }

    @Test
    void failedExportWaitsForCanceledSiblingExporterToStop() {
        var manifest = backupManifest();
        var siblingStarted = new CountDownLatch(1);
        var siblingStopped = new AtomicInteger();
        var executor = new PartitionBackupExecutor(new PartitionBackupLimits(2, 1024));

        var error = assertThrows(IllegalStateException.class, () -> executor.export(manifest, cut -> {
            if (cut.partitionId().equals(new ShardId(1))) {
                assertTrue(siblingStarted.await(5, TimeUnit.SECONDS));
                throw new IllegalStateException("export failed");
            }
            siblingStarted.countDown();
            try {
                new CountDownLatch(1).await();
                throw new AssertionError("blocked sibling exporter should be interrupted");
            } catch (InterruptedException e) {
                siblingStopped.incrementAndGet();
                throw e;
            }
        }));

        assertEquals("export failed", error.getMessage());
        assertEquals(1, siblingStopped.get());
    }

    @Test
    void checkedExporterFailureIncludesPartitionContext() {
        var manifest = backupManifest();
        var executor = new PartitionBackupExecutor(new PartitionBackupLimits(1, 1024));
        var failure = new IOException("snapshot failed");

        var error = assertThrows(CompletionException.class, () -> executor.export(manifest, _ -> {
            throw failure;
        }));

        assertEquals("partition backup exporter failed for partition 1", error.getMessage());
        assertSame(failure, error.getCause());
    }

    @Test
    void wrappedCheckedExporterFailureIncludesPartitionContext() {
        var manifest = backupManifest();
        var executor = new PartitionBackupExecutor(new PartitionBackupLimits(1, 1024));
        var failure = new IOException("wrapped snapshot failed");

        var error = assertThrows(CompletionException.class, () -> executor.export(manifest, _ -> {
            throw new CompletionException(new CompletionException(failure));
        }));

        assertEquals("partition backup exporter failed for partition 1", error.getMessage());
        assertSame(failure, error.getCause());
    }

    @Test
    void failedExportPreservesFatalExporterFailureSeverityAndStillStopsSiblingExporter() {
        var manifest = backupManifest();
        var siblingStarted = new CountDownLatch(1);
        var siblingStopped = new AtomicInteger();
        var executor = new PartitionBackupExecutor(new PartitionBackupLimits(2, 1024));
        var fatalFailure = new AssertionError("fatal export failed");

        var error = assertThrows(AssertionError.class, () -> executor.export(manifest, cut -> {
            if (cut.partitionId().equals(new ShardId(1))) {
                assertTrue(siblingStarted.await(5, TimeUnit.SECONDS));
                throw fatalFailure;
            }
            siblingStarted.countDown();
            try {
                new CountDownLatch(1).await();
                throw new AssertionError("blocked sibling exporter should be interrupted");
            } catch (InterruptedException e) {
                siblingStopped.incrementAndGet();
                throw e;
            }
        }));

        assertSame(fatalFailure, error);
        assertEquals(1, siblingStopped.get());
    }

    @Test
    void interruptedExportWaitsForExporterShutdownAndRestoresInterrupt() throws Exception {
        var manifest = backupManifest();
        var exporterStarted = new CountDownLatch(1);
        var exporterInterrupted = new CountDownLatch(1);
        var allowExporterStop = new CountDownLatch(1);
        var exporterStopped = new AtomicInteger();
        var failure = new AtomicReference<Throwable>();
        var interruptRestored = new AtomicInteger();
        var executor = new PartitionBackupExecutor(new PartitionBackupLimits(1, 1024));
        var exportThread = new Thread(() -> {
            try {
                executor.export(manifest, _ -> {
                    exporterStarted.countDown();
                    try {
                        new CountDownLatch(1).await();
                        throw new AssertionError("blocked exporter should be interrupted");
                    } catch (InterruptedException e) {
                        exporterInterrupted.countDown();
                        assertTrue(allowExporterStop.await(5, TimeUnit.SECONDS));
                        exporterStopped.incrementAndGet();
                        throw e;
                    }
                });
            } catch (Throwable e) {
                failure.set(e);
                if (Thread.currentThread().isInterrupted()) {
                    interruptRestored.incrementAndGet();
                }
            }
        }, "partition-backup-interrupt-test");

        exportThread.start();
        assertTrue(exporterStarted.await(5, TimeUnit.SECONDS));
        exportThread.interrupt();
        assertTrue(exporterInterrupted.await(5, TimeUnit.SECONDS));
        TimeUnit.MILLISECONDS.sleep(50);
        assertTrue(exportThread.isAlive(), "export returned before interrupted exporter shutdown completed");
        allowExporterStop.countDown();
        exportThread.join(5_000);

        var completionFailure = assertInstanceOf(CompletionException.class, failure.get());
        assertEquals("partition backup export interrupted", completionFailure.getMessage());
        assertInstanceOf(InterruptedException.class, completionFailure.getCause());
        assertEquals(1, exporterStopped.get());
        assertEquals(1, interruptRestored.get());
    }

    @Test
    void rejectsInvalidLimits() {
        assertThrows(IllegalArgumentException.class, () -> new PartitionBackupLimits(0, 1));
        assertThrows(IllegalArgumentException.class, () -> new PartitionBackupLimits(1, -1));
        assertThrows(IllegalArgumentException.class, () -> new PartitionBackupLimits(1, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new PartitionBackupLimits(1, 1, 1, 1));
    }

    @Test
    void rejectsOutOfRangeArtifactChecksum() {
        assertThrows(IllegalArgumentException.class, () -> new PartitionBackupArtifact(
                new ShardId(7),
                1,
                2,
                3,
                4,
                0x1_0000_0000L,
                List.of()
        ));
    }

    @Test
    void snapshotChecksumRejectsFileChunkChecksumMismatch() {
        var manifest = backupManifest();
        var cut = manifest.partitionCuts().getFirst();
        var chunks = new java.util.ArrayList<>(snapshotChunks(manifest, cut, "corrupted"));
        var fileChunk = (CheckpointSnapshotFileChunk) chunks.get(2);
        chunks.set(2, new CheckpointSnapshotFileChunk(
                fileChunk.relativePath(),
                fileChunk.chunkIndex(),
                fileChunk.chunkCount(),
                fileChunk.bytes(),
                fileChunk.crc32c() ^ 1
        ));

        var error = assertThrows(IllegalArgumentException.class,
                () -> PartitionBackupExecutor.snapshotChecksum(chunks));

        assertTrue(error.getMessage().contains("failed checksum validation"));
    }

    private static BackupManifest backupManifest() {
        return new BackupManifest("backup-12", 12, 11, List.of(
                backupCut(new ShardId(3), 6, 300),
                backupCut(new ShardId(1), 4, 100),
                backupCut(new ShardId(2), 5, 200)
        ));
    }

    private static BackupManifest.PartitionCut backupCut(ShardId partitionId, long ratisTerm, long ratisIndex) {
        return new BackupManifest.PartitionCut(partitionId, 7, ratisTerm, ratisIndex, 12);
    }

    private static List<Object> snapshotChunks(BackupManifest manifest,
                                               BackupManifest.PartitionCut cut,
                                               String value) {
        var bytes = value.getBytes(StandardCharsets.UTF_8);
        var checkpointManifest = new CheckpointSnapshotManifest(1, cut.ratisIndex(), 1, bytes.length, 1024);
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
        return List.of(
                snapshotManifest,
                checkpointManifest,
                new CheckpointSnapshotFileChunk("CURRENT", 0, 1, bytes, checksum(bytes))
        );
    }

    private static long checksum(byte[] bytes) {
        var checksum = new java.util.zip.CRC32C();
        checksum.update(bytes, 0, bytes.length);
        return checksum.getValue();
    }

    private static void assertRejectsNull(String expectedMessage, Executable executable) {
        var error = assertThrows(IllegalArgumentException.class, executable);
        assertEquals(expectedMessage, error.getMessage());
    }
}
