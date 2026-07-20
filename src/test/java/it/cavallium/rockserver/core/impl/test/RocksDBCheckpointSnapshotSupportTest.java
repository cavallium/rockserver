package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport.CheckpointSnapshotFileChunk;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport.CheckpointSnapshotManifest;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport.SnapshotLimits;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32C;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

class RocksDBCheckpointSnapshotSupportTest {

    @BeforeAll
    static void loadRocksDb() {
        RocksDBLoader.loadLibrary();
    }

    @Test
    void snapshotsRealRocksDbCheckpointAndInstallsDirectory(@TempDir Path directory) throws Exception {
        var sourceDirectory = directory.resolve("source-db");
        var targetDirectory = directory.resolve("target-db");
        var workDirectory = directory.resolve("snapshot-work");
        List<Object> chunks = new ArrayList<>();

        try (var options = new Options().setCreateIfMissing(true);
             var source = RocksDB.open(options, sourceDirectory.toString())) {
            source.put(bytes("key"), bytes("value"));
            try (var flushOptions = new FlushOptions().setWaitForFlush(true)) {
                source.flush(flushOptions);
            }

            var support = RocksDBCheckpointSnapshotSupport.forRocksDB(
                    source,
                    workDirectory,
                    RocksDBCheckpointSnapshotSupport.replacingClosedDatabaseDirectory(targetDirectory),
                    4
            );
            support.takeSnapshot(7, chunks::add);
            assertInstanceOf(CheckpointSnapshotManifest.class, chunks.getFirst());

            Files.createDirectories(targetDirectory);
            Files.writeString(targetDirectory.resolve("stale"), "old");
            support.installSnapshot(7, chunks);
        }

        assertFalse(Files.exists(targetDirectory.resolve("stale")));
        try (var options = new Options().setCreateIfMissing(false);
             var restored = RocksDB.open(options, targetDirectory.toString())) {
            assertArrayEquals(bytes("value"), restored.get(bytes("key")));
        }
    }

    @Test
    void snapshotInstallerRejectsSymbolicLinkTarget(@TempDir Path directory) throws Exception {
        var externalDirectory = directory.resolve("external-db");
        Files.createDirectories(externalDirectory);
        Files.writeString(externalDirectory.resolve("retained"), "external");
        var targetDirectory = directory.resolve("target-db");
        Files.createSymbolicLink(targetDirectory, externalDirectory.getFileName());
        var checkpointDirectory = directory.resolve("checkpoint");
        Files.createDirectories(checkpointDirectory);
        Files.writeString(checkpointDirectory.resolve("CURRENT"), "checkpoint");

        var installer = RocksDBCheckpointSnapshotSupport.replacingClosedDatabaseDirectory(targetDirectory);
        var error = assertThrows(IOException.class, () -> installer.installSnapshot(checkpointDirectory));

        assertTrue(error.getMessage().contains("must be a directory"));
        assertTrue(Files.isSymbolicLink(targetDirectory));
        assertTrue(Files.exists(externalDirectory.resolve("retained")));
    }

    @Test
    void recoversBackupOnlyInterruptedSnapshotInstall(@TempDir Path directory) throws Exception {
        var targetDirectory = directory.resolve("target-db");
        Files.createDirectories(targetDirectory);
        Files.writeString(targetDirectory.resolve("retained"), "old");
        var backupDirectory = targetDirectory.resolveSibling(
                targetDirectory.getFileName() + RocksDBCheckpointSnapshotSupport.SNAPSHOT_BACKUP_SUFFIX);
        Files.move(targetDirectory, backupDirectory);

        RocksDBCheckpointSnapshotSupport.recoverInterruptedClosedDatabaseDirectoryInstall(targetDirectory);

        assertTrue(Files.exists(targetDirectory.resolve("retained")));
        assertFalse(Files.exists(backupDirectory));
    }

    @Test
    void removesStaleBackupAfterCompletedSnapshotInstall(@TempDir Path directory) throws Exception {
        var targetDirectory = directory.resolve("target-db");
        Files.createDirectories(targetDirectory);
        Files.writeString(targetDirectory.resolve("current"), "new");
        var backupDirectory = targetDirectory.resolveSibling(
                targetDirectory.getFileName() + RocksDBCheckpointSnapshotSupport.SNAPSHOT_BACKUP_SUFFIX);
        Files.createDirectories(backupDirectory.resolve("nested"));
        Files.writeString(backupDirectory.resolve("nested").resolve("retained"), "old");

        RocksDBCheckpointSnapshotSupport.recoverInterruptedClosedDatabaseDirectoryInstall(targetDirectory);

        assertTrue(Files.exists(targetDirectory.resolve("current")));
        assertFalse(Files.exists(backupDirectory));
    }

    @Test
    void serializesConcurrentSnapshotInstallers(@TempDir Path directory) throws Exception {
        var targetDirectory = directory.resolve("target-db");
        var installer = RocksDBCheckpointSnapshotSupport.replacingClosedDatabaseDirectory(targetDirectory);
        var start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(8)) {
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            for (var i = 0; i < 8; i++) {
                var checkpointDirectory = directory.resolve("checkpoint-" + i);
                Files.createDirectories(checkpointDirectory);
                Files.writeString(checkpointDirectory.resolve("marker-" + i), "checkpoint");
                futures.add(executor.submit(() -> {
                    start.await();
                    installer.installSnapshot(checkpointDirectory);
                    return null;
                }));
            }

            start.countDown();
            for (var future : futures) {
                future.get();
            }
        }

        try (var files = Files.list(targetDirectory)) {
            assertEquals(1, files.count());
        }
        assertFalse(Files.exists(targetDirectory.resolveSibling(
                targetDirectory.getFileName() + RocksDBCheckpointSnapshotSupport.SNAPSHOT_BACKUP_SUFFIX)));
    }

    @Test
    void rejectsCorruptFileChunk(@TempDir Path directory) {
        var restoreWorkDirectory = directory.resolve("restore-work");
        var support = new RocksDBCheckpointSnapshotSupport(
                checkpointDirectory -> {
                    Files.createDirectories(checkpointDirectory);
                    Files.writeString(checkpointDirectory.resolve("CURRENT"), "abcdef");
                },
                directory.resolve("snapshot-work"),
                ignored -> fail("corrupt snapshot must not be installed"),
                2
        );
        var chunks = new ArrayList<>();
        support.takeSnapshot(11, chunks::add);

        var corruptedChunks = new ArrayList<>(chunks);
        for (int i = 0; i < corruptedChunks.size(); i++) {
            if (corruptedChunks.get(i) instanceof CheckpointSnapshotFileChunk chunk) {
                var bytes = chunk.bytes();
                bytes[0] = (byte) (bytes[0] ^ 1);
                corruptedChunks.set(i, new CheckpointSnapshotFileChunk(
                        chunk.relativePath(),
                        chunk.chunkIndex(),
                        chunk.chunkCount(),
                        bytes,
                        chunk.crc32c()
                ));
                break;
            }
        }

        var restoringSupport = new RocksDBCheckpointSnapshotSupport(
                ignored -> {
                },
                restoreWorkDirectory,
                ignored -> fail("corrupt snapshot must not be installed"),
                2
        );
        var ex = assertThrows(IllegalArgumentException.class,
                () -> restoringSupport.installSnapshot(11, corruptedChunks));
        assertTrue(ex.getMessage().contains("checksum"));
        assertFalse(Files.exists(restoreWorkDirectory));
    }

    @Test
    void rejectsPathTraversalChunk(@TempDir Path directory) {
        var bytes = new byte[]{1};
        var support = new RocksDBCheckpointSnapshotSupport(
                ignored -> {
                },
                directory.resolve("snapshot-work"),
                ignored -> fail("path traversal snapshot must not be installed"),
                16
        );

        assertInvalidSnapshotPath(support, bytes, "../escape");
        assertInvalidSnapshotPath(support, bytes, ".");
        assertFalse(Files.exists(directory.resolve("escape")));
    }

    @Test
    void removesBrokenSymlinkLeftByFailingSnapshotInstaller(@TempDir Path directory) {
        var bytes = new byte[]{1};
        var installDirectory = new AtomicReference<Path>();
        var support = new RocksDBCheckpointSnapshotSupport(
                ignored -> {
                },
                directory.resolve("snapshot-work"),
                path -> {
                    installDirectory.set(path);
                    Files.delete(path.resolve("CURRENT"));
                    Files.delete(path);
                    Files.createSymbolicLink(path, Path.of("missing-target"));
                    throw new IOException("simulated installer failure");
                },
                16
        );
        var chunks = List.<Object>of(
                new CheckpointSnapshotManifest(1, 13, 1, 1, 16),
                new CheckpointSnapshotFileChunk("CURRENT", 0, 1, bytes, checksum(bytes))
        );

        assertThrows(RocksDBException.class, () -> support.installSnapshot(13, chunks));

        assertFalse(Files.exists(installDirectory.get(), LinkOption.NOFOLLOW_LINKS));
    }

    @Test
    void rejectsInvalidChunkSizesBeforeInstall(@TempDir Path directory) {
        var workDirectory = directory.resolve("snapshot-work");
        var support = new RocksDBCheckpointSnapshotSupport(
                ignored -> {
                },
                workDirectory,
                ignored -> fail("invalid snapshot must not be installed"),
                16
        );
        var oversizedChunk = List.<Object>of(
                new CheckpointSnapshotManifest(1, 13, 1, 2, 1),
                new CheckpointSnapshotFileChunk("CURRENT", 0, 1, new byte[]{1, 2}, checksum(new byte[]{1, 2}))
        );
        var shortIntermediateChunk = List.<Object>of(
                new CheckpointSnapshotManifest(1, 13, 1, 2, 2),
                new CheckpointSnapshotFileChunk("CURRENT", 0, 2, new byte[]{1}, checksum(new byte[]{1})),
                new CheckpointSnapshotFileChunk("CURRENT", 1, 2, new byte[]{2}, checksum(new byte[]{2}))
        );

        var oversizedError = assertThrows(IllegalArgumentException.class,
                () -> support.installSnapshot(13, oversizedChunk));
        var shortError = assertThrows(IllegalArgumentException.class,
                () -> support.installSnapshot(13, shortIntermediateChunk));

        assertTrue(oversizedError.getMessage().contains("exceeds declared chunk size"));
        assertTrue(shortError.getMessage().contains("must contain 2 bytes"));
        assertFalse(Files.exists(workDirectory));
    }

    @Test
    void rejectsInvalidCommitIndexesAndConfiguredChunkSizesBeforeSnapshotWork(@TempDir Path directory) {
        var support = new RocksDBCheckpointSnapshotSupport(
                ignored -> fail("invalid snapshot must not create a checkpoint"),
                directory.resolve("snapshot-work"),
                ignored -> fail("invalid snapshot must not be installed"),
                16
        );

        assertThrows(IllegalArgumentException.class, () -> support.takeSnapshot(-1, ignored -> {
        }));
        assertThrows(IllegalArgumentException.class, () -> support.installSnapshot(-1, List.of()));
        assertThrows(IllegalArgumentException.class, () -> new CheckpointSnapshotManifest(1, -1, 0, 0, 16));
        assertThrows(IllegalArgumentException.class, () -> new RocksDBCheckpointSnapshotSupport(
                ignored -> {
                },
                directory.resolve("snapshot-work"),
                ignored -> {
                },
                RocksDBCheckpointSnapshotSupport.MAX_CHUNK_SIZE_BYTES + 1
        ));
        assertThrows(IllegalArgumentException.class, () -> new CheckpointSnapshotManifest(
                1,
                13,
                0,
                0,
                RocksDBCheckpointSnapshotSupport.MAX_CHUNK_SIZE_BYTES + 1
        ));
        assertThrows(IllegalArgumentException.class, () -> new SnapshotLimits(-1, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new SnapshotLimits(1, -1, 1));
        assertThrows(IllegalArgumentException.class, () -> new SnapshotLimits(1, 1, -1));
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics(@TempDir Path directory) {
        var workDirectory = directory.resolve("snapshot-work");
        var installer = RocksDBCheckpointSnapshotSupport.replacingClosedDatabaseDirectory(directory.resolve("target"));
        var support = new RocksDBCheckpointSnapshotSupport(
                ignored -> fail("null input must be rejected before checkpoint creation"),
                workDirectory,
                ignored -> fail("null input must be rejected before install"),
                16
        );
        var chunksWithNull = new ArrayList<>();
        chunksWithNull.add(new CheckpointSnapshotManifest(1, 13, 1, 1, 16));
        chunksWithNull.add(null);

        assertIllegalArgument("checkpointWriter must not be null", () -> new RocksDBCheckpointSnapshotSupport(
                null,
                workDirectory,
                installer,
                16
        ));
        assertIllegalArgument("workDirectory must not be null", () -> new RocksDBCheckpointSnapshotSupport(
                ignored -> {
                },
                null,
                installer,
                16
        ));
        assertIllegalArgument("snapshotInstaller must not be null", () -> new RocksDBCheckpointSnapshotSupport(
                ignored -> {
                },
                workDirectory,
                null,
                16
        ));
        assertIllegalArgument("limits must not be null", () -> new RocksDBCheckpointSnapshotSupport(
                ignored -> {
                },
                workDirectory,
                installer,
                16,
                null
        ));
        assertIllegalArgument("db must not be null", () -> RocksDBCheckpointSnapshotSupport.forRocksDB(
                null,
                workDirectory,
                installer
        ));
        assertIllegalArgument("targetDirectory must not be null",
                () -> RocksDBCheckpointSnapshotSupport.replacingClosedDatabaseDirectory(null));
        assertIllegalArgument("targetDirectory must not be null",
                () -> RocksDBCheckpointSnapshotSupport.recoverInterruptedClosedDatabaseDirectoryInstall(null));
        assertIllegalArgument("snapshotChunkConsumer must not be null", () -> support.takeSnapshot(13, null));
        assertIllegalArgument("snapshotChunks must not be null", () -> support.installSnapshot(13, null));
        assertNullCheckpointSnapshotFileChunkInputsRejected();
        assertIllegalArgument("RocksDB checkpoint snapshot chunk 1 has unsupported type: null",
                () -> support.installSnapshot(13, chunksWithNull));
    }

    @Test
    void rejectsSnapshotResourceLimitsBeforeInstallOrEmission(@TempDir Path directory) {
        var support = new RocksDBCheckpointSnapshotSupport(
                checkpointDirectory -> {
                    Files.createDirectories(checkpointDirectory);
                    Files.write(checkpointDirectory.resolve("CURRENT"), new byte[]{1});
                    Files.write(checkpointDirectory.resolve("MANIFEST"), new byte[]{2});
                },
                directory.resolve("snapshot-work"),
                ignored -> fail("oversized snapshot must not be installed"),
                16,
                new SnapshotLimits(1, 1, 1)
        );
        var fileCountManifest = List.<Object>of(new CheckpointSnapshotManifest(1, 13, 2, 1, 16));
        var totalSizeManifest = List.<Object>of(new CheckpointSnapshotManifest(1, 13, 1, 2, 16));
        var excessiveChunkCount = List.<Object>of(
                new CheckpointSnapshotManifest(1, 13, 1, 1, 16),
                new CheckpointSnapshotFileChunk("CURRENT", 0, 1, new byte[]{1}, checksum(new byte[]{1})),
                new CheckpointSnapshotFileChunk("CURRENT", 0, 1, new byte[]{1}, checksum(new byte[]{1}))
        );

        var fileCountError = assertThrows(IllegalArgumentException.class,
                () -> support.installSnapshot(13, fileCountManifest));
        var totalSizeError = assertThrows(IllegalArgumentException.class,
                () -> support.installSnapshot(13, totalSizeManifest));
        var chunkCountError = assertThrows(IllegalArgumentException.class,
                () -> support.installSnapshot(13, excessiveChunkCount));
        var emissionError = assertThrows(IllegalArgumentException.class,
                () -> support.takeSnapshot(13, ignored -> fail("oversized snapshot must not be emitted")));

        assertTrue(fileCountError.getMessage().contains("file count"));
        assertTrue(totalSizeError.getMessage().contains("total size"));
        assertTrue(chunkCountError.getMessage().contains("chunk count"));
        assertTrue(emissionError.getMessage().contains("file count"));
    }

    @Test
    void rejectsReceivedSnapshotByteExcessBeforeInstallDirectoryCreation(@TempDir Path directory) {
        var bytes = new byte[]{1, 2};
        var chunks = List.<Object>of(
                new CheckpointSnapshotManifest(1, 13, 1, 1, 16),
                new CheckpointSnapshotFileChunk("CURRENT", 0, 1, bytes, checksum(bytes))
        );
        var limitedWorkDirectory = directory.resolve("limited-work");
        var limitedSupport = new RocksDBCheckpointSnapshotSupport(
                ignored -> {
                },
                limitedWorkDirectory,
                ignored -> fail("oversized snapshot must not be installed"),
                16,
                new SnapshotLimits(1, 1, 1)
        );
        var mismatchedWorkDirectory = directory.resolve("mismatched-work");
        var mismatchedSupport = new RocksDBCheckpointSnapshotSupport(
                ignored -> {
                },
                mismatchedWorkDirectory,
                ignored -> fail("mismatched snapshot must not be installed"),
                16,
                new SnapshotLimits(1, 2, 1)
        );

        var limitError = assertThrows(IllegalArgumentException.class,
                () -> limitedSupport.installSnapshot(13, chunks));
        var mismatchError = assertThrows(IllegalArgumentException.class,
                () -> mismatchedSupport.installSnapshot(13, chunks));

        assertTrue(limitError.getMessage().contains("total size 2 exceeds limit 1"));
        assertTrue(mismatchError.getMessage().contains("expected 1 bytes but received 2"));
        assertFalse(Files.exists(limitedWorkDirectory));
        assertFalse(Files.exists(mismatchedWorkDirectory));
    }

    @Test
    void rejectsHugeSparseCheckpointByConfiguredChunkLimitBeforeEmission(@TempDir Path directory) {
        long sparseFileSize = Math.multiplyExact(Integer.MAX_VALUE, 16L) + 1;
        var support = new RocksDBCheckpointSnapshotSupport(
                checkpointDirectory -> {
                    Files.createDirectories(checkpointDirectory);
                    try (var channel = Files.newByteChannel(
                            checkpointDirectory.resolve("CURRENT"),
                            StandardOpenOption.CREATE,
                            StandardOpenOption.WRITE
                    )) {
                        channel.position(sparseFileSize - 1);
                        channel.write(ByteBuffer.wrap(new byte[]{1}));
                    }
                },
                directory.resolve("snapshot-work"),
                ignored -> fail("oversized snapshot must not be installed"),
                16,
                new SnapshotLimits(1, Long.MAX_VALUE, Integer.MAX_VALUE)
        );

        var error = assertThrows(IllegalArgumentException.class,
                () -> support.takeSnapshot(13, ignored -> fail("oversized snapshot must not be emitted")));

        assertTrue(error.getMessage().contains("chunk count"));
    }

    @Test
    void rejectsCheckpointFileGrowthAfterAdmission(@TempDir Path directory) {
        var checkpointFile = new AtomicReference<Path>();
        var support = new RocksDBCheckpointSnapshotSupport(
                checkpointDirectory -> {
                    Files.createDirectories(checkpointDirectory);
                    checkpointFile.set(checkpointDirectory.resolve("CURRENT"));
                    Files.write(checkpointFile.get(), new byte[]{1});
                },
                directory.resolve("snapshot-work"),
                ignored -> fail("growing snapshot must not be installed"),
                16
        );

        var error = assertThrows(RocksDBException.class, () -> support.takeSnapshot(13, chunk -> {
            if (chunk instanceof CheckpointSnapshotManifest) {
                try {
                    Files.write(checkpointFile.get(), new byte[]{2}, StandardOpenOption.APPEND);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }));

        assertTrue(error.getCause().getMessage().contains("grew while reading"));
    }

    @Test
    void rejectsSymbolicLinksInCheckpoint(@TempDir Path directory) throws Exception {
        var externalFile = directory.resolve("external");
        Files.write(externalFile, new byte[]{1});
        var support = new RocksDBCheckpointSnapshotSupport(
                checkpointDirectory -> {
                    Files.createDirectories(checkpointDirectory);
                    Files.createSymbolicLink(checkpointDirectory.resolve("CURRENT"), externalFile);
                },
                directory.resolve("snapshot-work"),
                ignored -> fail("symbolic-link snapshot must not be installed"),
                16
        );

        var error = assertThrows(RocksDBException.class, () -> support.takeSnapshot(13, ignored -> {
        }));

        assertTrue(error.getCause().getMessage().contains("non-regular path"));
    }

    @Test
    void rejectsOutOfRangeFileChunkChecksum() {
        var bytes = new byte[]{1};

        assertThrows(IllegalArgumentException.class,
                () -> new CheckpointSnapshotFileChunk("CURRENT", 0, 1, bytes, -1));
        assertThrows(IllegalArgumentException.class,
                () -> new CheckpointSnapshotFileChunk("CURRENT", 0, 1, bytes, 0x1_0000_0000L));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @SuppressWarnings("DataFlowIssue")
    private static void assertNullCheckpointSnapshotFileChunkInputsRejected() {
        assertIllegalArgument("relativePath must not be null",
                () -> new CheckpointSnapshotFileChunk(null, 0, 1, new byte[]{1}, checksum(new byte[]{1})));
        assertIllegalArgument("bytes must not be null",
                () -> new CheckpointSnapshotFileChunk("CURRENT", 0, 1, null, 0));
    }

    private static void assertInvalidSnapshotPath(RocksDBCheckpointSnapshotSupport support,
                                                  byte[] bytes,
                                                  String relativePath) {
        List<Object> chunks = List.of(
                new CheckpointSnapshotManifest(1, 13, 1, bytes.length, 16),
                new CheckpointSnapshotFileChunk(relativePath, 0, 1, bytes, checksum(bytes))
        );

        var ex = assertThrows(IllegalArgumentException.class, () -> support.installSnapshot(13, chunks));
        assertTrue(ex.getMessage().contains("Invalid RocksDB checkpoint snapshot file path"));
    }

    private static void assertIllegalArgument(String message, Executable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation);
        assertEquals(message, error.getMessage());
    }

    private static long checksum(byte[] bytes) {
        var checksum = new CRC32C();
        checksum.update(bytes);
        return checksum.getValue();
    }
}
