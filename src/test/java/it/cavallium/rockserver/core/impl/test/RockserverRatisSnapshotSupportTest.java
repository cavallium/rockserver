package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.RockserverStorageKeyspace;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.PartitionStateMetadataCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandValidator;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseMode;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutation;
import it.cavallium.rockserver.core.cluster.command.RocksDBPartitionStateMachineApplier;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport.CheckpointSnapshotManifest;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionSnapshotManifest;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionSnapshotMetadata;
import it.cavallium.rockserver.core.replication.ratis.RockserverRatisStateMachine;
import it.cavallium.rockserver.core.replication.ratis.RockserverRatisSnapshotSupport;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

class RockserverRatisSnapshotSupportTest {

    @BeforeAll
    static void loadRocksDb() {
        RocksDBLoader.loadLibrary();
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics(@TempDir Path directory) throws Exception {
        var targetDirectory = directory.resolve("target-db");
        var support = snapshotInstaller(directory, targetDirectory);
        var metadata = snapshotMetadata();
        var checkpointManifest = new CheckpointSnapshotManifest(1, metadata.ratisIndex(), 1, 4, 4);
        var manifest = RatisPartitionSnapshotManifest.create(metadata, checkpointManifest);
        var nullSnapshotChunk = new ArrayList<>();
        nullSnapshotChunk.add(null);
        nullSnapshotChunk.add(checkpointManifest);

        assertRejectsNull("checkpointSupport must not be null", () -> new RockserverRatisSnapshotSupport(null));
        assertRejectsNull("metadata must not be null", () -> support.takeSnapshot(null, _ -> {
        }));
        assertRejectsNull("snapshotChunkConsumer must not be null", () -> support.takeSnapshot(metadata, null));
        assertRejectsNull("expectedPartitionId must not be null",
                () -> support.installSnapshot(null, metadata.ratisIndex(), List.of()));
        assertRejectsNull("snapshotChunks must not be null",
                () -> support.installSnapshot(metadata.partitionId(), metadata.ratisIndex(), null));
        assertRejectsNull("snapshotChunks chunk 0 must not be null",
                () -> support.installSnapshot(metadata.partitionId(), metadata.ratisIndex(), nullSnapshotChunk));

        assertRejectsNull("partitionId must not be null", () -> new RatisPartitionSnapshotMetadata(
                null,
                3,
                11,
                17,
                23,
                29,
                31,
                37
        ));
        assertRejectsNull("partitionId must not be null", () -> new RatisPartitionSnapshotManifest(
                RatisPartitionSnapshotManifest.FORMAT_VERSION,
                null,
                3,
                11,
                17,
                23,
                29,
                31,
                37,
                1,
                4,
                4,
                0,
                0
        ));
        assertRejectsNull("metadata must not be null",
                () -> RatisPartitionSnapshotManifest.create(null, checkpointManifest));
        assertRejectsNull("checkpointManifest must not be null",
                () -> RatisPartitionSnapshotManifest.create(metadata, null));
        assertRejectsNull("checkpointManifest must not be null", () -> manifest.validateCheckpointManifest(null));
        assertRejectsNull("checkpointManifest must not be null",
                () -> RatisPartitionSnapshotManifest.checkpointManifestChecksum(null));

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("source-db").toString())) {
            var installer = RocksDBCheckpointSnapshotSupport.replacingClosedDatabaseDirectory(
                    directory.resolve("factory-target-db")
            );
            assertRejectsNull("db must not be null",
                    () -> RockserverRatisSnapshotSupport.forRocksDB(null, directory.resolve("work-db"), installer));
            assertRejectsNull("workDirectory must not be null",
                    () -> RockserverRatisSnapshotSupport.forRocksDB(db, null, installer));
            assertRejectsNull("snapshotInstaller must not be null",
                    () -> RockserverRatisSnapshotSupport.forRocksDB(db, directory.resolve("work-installer"), null));
            assertRejectsNull("limits must not be null",
                    () -> RockserverRatisSnapshotSupport.forRocksDB(
                            db,
                            directory.resolve("work-limits"),
                            installer,
                            4,
                            null
                    ));
        }
    }

    @Test
    void snapshotsRocksDbCheckpointWithRatisPartitionManifestAndInstallsIt(@TempDir Path directory)
            throws Exception {
        var sourceDirectory = directory.resolve("source-db");
        var targetDirectory = directory.resolve("target-db");
        var workDirectory = directory.resolve("snapshot-work");
        var chunks = new ArrayList<>();
        var metadata = snapshotMetadata();

        try (var options = new Options().setCreateIfMissing(true);
             var source = RocksDB.open(options, sourceDirectory.toString())) {
            source.put(bytes("key"), bytes("value"));
            try (var flushOptions = new FlushOptions().setWaitForFlush(true)) {
                source.flush(flushOptions);
            }
            var snapshotSupport = RockserverRatisSnapshotSupport.forRocksDB(
                    source,
                    workDirectory,
                    RocksDBCheckpointSnapshotSupport.replacingClosedDatabaseDirectory(targetDirectory),
                    4
            );

            snapshotSupport.takeSnapshot(metadata, chunks::add);

            var manifest = assertInstanceOf(RatisPartitionSnapshotManifest.class, chunks.getFirst());
            assertEquals(RatisPartitionSnapshotManifest.FORMAT_VERSION, manifest.formatVersion());
            assertEquals(metadata.partitionId(), manifest.partitionId());
            assertEquals(metadata.partitionEpoch(), manifest.partitionEpoch());
            assertEquals(metadata.ratisTerm(), manifest.ratisTerm());
            assertEquals(metadata.ratisIndex(), manifest.ratisIndex());
            assertEquals(metadata.metadataEpoch(), manifest.metadataEpoch());
            assertEquals(metadata.schemaEpoch(), manifest.schemaEpoch());
            assertEquals(metadata.dedupeWatermark(), manifest.dedupeWatermark());
            assertEquals(metadata.cdcWatermark(), manifest.cdcWatermark());
            assertInstanceOf(CheckpointSnapshotManifest.class, chunks.get(1));

            Files.createDirectories(targetDirectory);
            Files.writeString(targetDirectory.resolve("stale"), "old");
            var installedManifest = snapshotSupport.installSnapshot(metadata.partitionId(), metadata.ratisIndex(), chunks);
            assertEquals(manifest, installedManifest);
        }

        assertFalse(Files.exists(targetDirectory.resolve("stale")));
        try (var options = new Options().setCreateIfMissing(false);
             var restored = RocksDB.open(options, targetDirectory.toString())) {
            assertArrayEquals(bytes("value"), restored.get(bytes("key")));
        }
    }

    @Test
    void rejectsCheckpointManifestThatDoesNotMatchPartitionManifest(@TempDir Path directory) throws Exception {
        var targetDirectory = directory.resolve("target-db");
        var chunks = takeSnapshot(directory, targetDirectory);
        var checkpointManifest = (CheckpointSnapshotManifest) chunks.get(1);
        chunks.set(1, new CheckpointSnapshotManifest(
                checkpointManifest.formatVersion(),
                checkpointManifest.commitIndex(),
                checkpointManifest.fileCount(),
                checkpointManifest.totalSizeBytes() + 1,
                checkpointManifest.chunkSizeBytes()
        ));
        var snapshotSupport = snapshotInstaller(directory, targetDirectory);
        var metadata = snapshotMetadata();

        var error = assertThrows(IllegalArgumentException.class,
                () -> snapshotSupport.installSnapshot(metadata.partitionId(), metadata.ratisIndex(), chunks));

        assertTrue(error.getMessage().contains("checkpoint total size"));
    }

    @Test
    void rejectsUnexpectedPartitionOrIndexBeforeInstall(@TempDir Path directory) throws Exception {
        var targetDirectory = directory.resolve("target-db");
        var chunks = takeSnapshot(directory, targetDirectory);
        var snapshotSupport = snapshotInstaller(directory, targetDirectory);
        var metadata = snapshotMetadata();

        var partitionError = assertThrows(IllegalArgumentException.class,
                () -> snapshotSupport.installSnapshot(new ShardId(8), metadata.ratisIndex(), chunks));
        var indexError = assertThrows(IllegalArgumentException.class,
                () -> snapshotSupport.installSnapshot(metadata.partitionId(), metadata.ratisIndex() + 1, chunks));

        assertTrue(partitionError.getMessage().contains("partition"));
        assertTrue(indexError.getMessage().contains("index"));
        assertFalse(Files.exists(targetDirectory));
    }

    @Test
    void rejectsInvalidManifestChecksum() {
        var checkpointManifest = new CheckpointSnapshotManifest(1, 17, 1, 4, 4);
        var manifest = RatisPartitionSnapshotManifest.create(snapshotMetadata(), checkpointManifest);

        var error = assertThrows(IllegalArgumentException.class, () -> new RatisPartitionSnapshotManifest(
                manifest.formatVersion(),
                manifest.partitionId(),
                manifest.partitionEpoch(),
                manifest.ratisTerm(),
                manifest.ratisIndex(),
                manifest.metadataEpoch(),
                manifest.schemaEpoch(),
                manifest.dedupeWatermark(),
                manifest.cdcWatermark(),
                manifest.checkpointFileCount(),
                manifest.checkpointTotalSizeBytes(),
                manifest.checkpointChunkSizeBytes(),
                manifest.checkpointManifestCrc32c(),
                manifest.manifestCrc32c() ^ 1
        ));

        assertTrue(error.getMessage().contains("checksum"));
    }

    @Test
    void rejectsOutOfRangePartitionManifestChecksums() {
        var checkpointManifest = new CheckpointSnapshotManifest(1, 17, 1, 4, 4);
        var manifest = RatisPartitionSnapshotManifest.create(snapshotMetadata(), checkpointManifest);
        long outOfRangeCrc = 0x1_0000_0000L;

        var checkpointCrcError = assertThrows(IllegalArgumentException.class,
                () -> new RatisPartitionSnapshotManifest(
                        manifest.formatVersion(),
                        manifest.partitionId(),
                        manifest.partitionEpoch(),
                        manifest.ratisTerm(),
                        manifest.ratisIndex(),
                        manifest.metadataEpoch(),
                        manifest.schemaEpoch(),
                        manifest.dedupeWatermark(),
                        manifest.cdcWatermark(),
                        manifest.checkpointFileCount(),
                        manifest.checkpointTotalSizeBytes(),
                        manifest.checkpointChunkSizeBytes(),
                        outOfRangeCrc,
                        manifest.manifestCrc32c()
                ));
        var manifestCrcError = assertThrows(IllegalArgumentException.class,
                () -> new RatisPartitionSnapshotManifest(
                        manifest.formatVersion(),
                        manifest.partitionId(),
                        manifest.partitionEpoch(),
                        manifest.ratisTerm(),
                        manifest.ratisIndex(),
                        manifest.metadataEpoch(),
                        manifest.schemaEpoch(),
                        manifest.dedupeWatermark(),
                        manifest.cdcWatermark(),
                        manifest.checkpointFileCount(),
                        manifest.checkpointTotalSizeBytes(),
                        manifest.checkpointChunkSizeBytes(),
                        manifest.checkpointManifestCrc32c(),
                        outOfRangeCrc
                ));

        assertTrue(checkpointCrcError.getMessage().contains("checkpointManifestCrc32c"));
        assertTrue(manifestCrcError.getMessage().contains("manifestCrc32c"));
    }

    @Test
    void rejectsOversizedPartitionManifestChunkSize() {
        var checkpointManifest = new CheckpointSnapshotManifest(1, 17, 1, 4, 4);
        var manifest = RatisPartitionSnapshotManifest.create(snapshotMetadata(), checkpointManifest);

        var error = assertThrows(IllegalArgumentException.class, () -> new RatisPartitionSnapshotManifest(
                manifest.formatVersion(),
                manifest.partitionId(),
                manifest.partitionEpoch(),
                manifest.ratisTerm(),
                manifest.ratisIndex(),
                manifest.metadataEpoch(),
                manifest.schemaEpoch(),
                manifest.dedupeWatermark(),
                manifest.cdcWatermark(),
                manifest.checkpointFileCount(),
                manifest.checkpointTotalSizeBytes(),
                RocksDBCheckpointSnapshotSupport.MAX_CHUNK_SIZE_BYTES + 1,
                manifest.checkpointManifestCrc32c(),
                manifest.manifestCrc32c()
        ));

        assertTrue(error.getMessage().contains("checkpointChunkSizeBytes"));
    }

    @Test
    void replacementPeerInstallsSnapshotAndReplaysLaggingLogEntries(@TempDir Path directory) throws Exception {
        var metadata = clusterMetadata();
        var partition = partition();
        var sourceDirectory = directory.resolve("source-db");
        var targetDirectory = directory.resolve("replacement-db");
        var chunks = new ArrayList<>();
        var snapshotCommand = putCommand(7, 0x18, "snapshot-value");
        var replayCommand = putCommand(8, 0x19, "replay-value");

        try (var options = new Options().setCreateIfMissing(true);
             var source = RocksDB.open(options, sourceDirectory.toString())) {
            var sourceStateMachine = persistentStateMachine(source, metadata, partition, 0);
            sourceStateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(snapshotCommand));
            try (var flushOptions = new FlushOptions().setWaitForFlush(true)) {
                source.flush(flushOptions);
            }
            var snapshotSupport = RockserverRatisSnapshotSupport.forRocksDB(
                    source,
                    directory.resolve("catch-up-snapshot-work"),
                    RocksDBCheckpointSnapshotSupport.replacingClosedDatabaseDirectory(targetDirectory),
                    4
            );
            snapshotSupport.takeSnapshot(new RatisPartitionSnapshotMetadata(
                    partition.partitionId(),
                    partition.epoch(),
                    11,
                    sourceStateMachine.lastAppliedIndex(),
                    metadata.epoch(),
                    3,
                    7,
                    0
            ), chunks::add);
            snapshotSupport.installSnapshot(partition.partitionId(), sourceStateMachine.lastAppliedIndex(), chunks);
        }

        try (var options = new Options().setCreateIfMissing(false);
             var replacement = RocksDB.open(options, targetDirectory.toString())) {
            assertArrayEquals(text("snapshot-value").toByteArray(),
                    replacement.get(userDataKey(0x18).toByteArray()));
            var restoredLastApplied = PartitionStateMetadataCodec.decodeLastAppliedIndex(
                    replacement.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray()));
            assertEquals(5, restoredLastApplied);

            var replacementStateMachine = persistentStateMachine(replacement, metadata, partition, restoredLastApplied);
            var duplicate = replacementStateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(snapshotCommand));
            var replay = replacementStateMachine.applyCommitted(6, PartitionWriteCommandCodec.encode(replayCommand));

            assertFalse(duplicate.applied());
            assertTrue(replay.applied());
            assertEquals(6, replacementStateMachine.lastAppliedIndex());
            assertArrayEquals(text("snapshot-value").toByteArray(),
                    replacement.get(userDataKey(0x18).toByteArray()));
            assertArrayEquals(text("replay-value").toByteArray(),
                    replacement.get(userDataKey(0x19).toByteArray()));
            assertEquals(6, PartitionStateMetadataCodec.decodeLastAppliedIndex(
                    replacement.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray())));
        }
    }

    private static ArrayList<Object> takeSnapshot(Path directory, Path targetDirectory) throws Exception {
        var sourceDirectory = directory.resolve("source-db");
        var chunks = new ArrayList<>();
        try (var options = new Options().setCreateIfMissing(true);
             var source = RocksDB.open(options, sourceDirectory.toString())) {
            source.put(bytes("key"), bytes("value"));
            try (var flushOptions = new FlushOptions().setWaitForFlush(true)) {
                source.flush(flushOptions);
            }
            RockserverRatisSnapshotSupport.forRocksDB(
                    source,
                    directory.resolve("snapshot-work"),
                    RocksDBCheckpointSnapshotSupport.replacingClosedDatabaseDirectory(targetDirectory)
            ).takeSnapshot(snapshotMetadata(), chunks::add);
        }
        return chunks;
    }

    private static RockserverRatisSnapshotSupport snapshotInstaller(Path directory, Path targetDirectory) {
        return new RockserverRatisSnapshotSupport(new RocksDBCheckpointSnapshotSupport(
                ignored -> {
                },
                directory.resolve("snapshot-install-work"),
                RocksDBCheckpointSnapshotSupport.replacingClosedDatabaseDirectory(targetDirectory),
                4
        ));
    }

    private static RatisPartitionSnapshotMetadata snapshotMetadata() {
        return new RatisPartitionSnapshotMetadata(
                new ShardId(7),
                3,
                11,
                17,
                23,
                29,
                31,
                37
        );
    }

    private static RockserverRatisStateMachine persistentStateMachine(RocksDB db,
                                                                      ClusterMetadata metadata,
                                                                      PartitionDescriptor partition,
                                                                      long initialLastAppliedIndex) {
        var applier = new RocksDBPartitionStateMachineApplier(metadata, db);
        return new RockserverRatisStateMachine(
                applier,
                command -> PartitionWriteCommandValidator.validate(metadata, partition, command),
                initialLastAppliedIndex
        );
    }

    private static ClusterMetadata clusterMetadata() {
        var column = new ClusterColumnMetadata(4, "accounts", schema(), 3);
        return new ClusterMetadata(new ClusterId(new UUID(0, 1)), 23, Map.of(), Map.of(4L, column));
    }

    private static ColumnSchema schema() {
        return ColumnSchema.of(IntArrayList.of(1), ObjectArrayList.of(), true);
    }

    private static PartitionDescriptor partition() {
        var leader = NodeId.of("n1");
        return new PartitionDescriptor(
                new ShardId(7),
                buf(0x10),
                buf(0x20),
                3,
                "group-7",
                List.of(new ReplicaMember(leader, URI.create("http://127.0.0.1:10001"), ReplicaRole.VOTER, 0, 0,
                        true)),
                leader,
                Map.of("rack", "rack-a"),
                PartitionSplitState.ACTIVE
        );
    }

    private static PartitionWriteCommand putCommand(long sequence, int keyByte, String value) {
        return PartitionWriteCommand.single(
                requestId(sequence),
                3,
                ReplicatedMutation.put(4, new Keys(buf(keyByte)), text(value)),
                PartitionWriteResponseMode.NONE
        );
    }

    private static ClientRequestId requestId(long sequence) {
        return new ClientRequestId(new ClientId(new UUID(0, 1)), sequence);
    }

    private static Buf userDataKey(int keyByte) {
        return RockserverStorageKeyspace.userData(4, buf(keyByte));
    }

    private static Buf buf(int... values) {
        var bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return Buf.wrap(bytes);
    }

    private static Buf text(String value) {
        return Buf.wrap(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void assertRejectsNull(String expectedMessage, Executable executable) {
        var error = assertThrows(IllegalArgumentException.class, executable);
        assertEquals(expectedMessage, error.getMessage());
    }
}
