package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcToken;
import it.cavallium.rockserver.core.cluster.metadata.BackupManifest;
import it.cavallium.rockserver.core.cluster.metadata.CdcTokenTranslation;
import it.cavallium.rockserver.core.cluster.metadata.MergeOperatorActivation;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandKind;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeMetadata;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeKind;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeStatus;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.net.URI;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class MetadataCommandTest {

    @Test
    void exposesEveryMetadataCommandKind() {
        var leader = NodeId.of("n1");
        var peer = peer(leader);
        List<MetadataCommand> commands = List.of(
                new MetadataCommand.RegisterNode(7, leader, URI.create("http://127.0.0.1:10000"),
                        URI.create("grpc://127.0.0.1:11000"), "rack-a"),
                new MetadataCommand.CreateColumn(7, new ClusterColumnMetadata(42, "accounts", schema(), 3)),
                new MetadataCommand.DeleteColumn(7, 42, 3),
                new MetadataCommand.CreatePartition(7, partition(new ShardId(1), null, buf(0x10), peer)),
                new MetadataCommand.SplitPartition(7, new ShardId(1), 3,
                        partition(new ShardId(2), null, buf(0x10), peer),
                        partition(new ShardId(3), buf(0x10), null, peer)),
                new MetadataCommand.MergePartitions(7, new ShardId(2), new ShardId(3),
                        partition(new ShardId(4), null, null, peer)),
                new MetadataCommand.ChangePartitionPeers(7, new ShardId(1), 3, List.of(peer), leader),
                new MetadataCommand.UpdateColumnSchema(7, 42, 3, 4, schema()),
                new MetadataCommand.PrepareSchemaChange(7, 100, SchemaChangeKind.CREATE_COLUMN, 42, 3,
                        new ClusterColumnMetadata(42, "accounts", schema(), 3), null),
                new MetadataCommand.ActivateSchemaChange(7, 100, 42, 3),
                new MetadataCommand.FenceSchemaChange(7, 100, 42, 3),
                new MetadataCommand.RecordBackupManifest(7, backupManifest()),
                new MetadataCommand.RecordCdcTokenTranslation(7, splitTranslation())
        );

        assertEquals(EnumSet.allOf(MetadataCommandKind.class),
                commands.stream().map(MetadataCommand::kind).collect(Collectors.toSet()));
        assertEquals(Set.of(7L), commands.stream()
                .map(MetadataCommand::expectedMetadataEpoch)
                .collect(Collectors.toSet()));
    }

    @Test
    void metadataChangesAreTypedCommandsWithEpochFences() {
        var leader = NodeId.of("n1");
        var peer = peer(leader);
        List<MetadataCommand> commands = List.of(
                new MetadataCommand.RegisterNode(11, leader, URI.create("http://127.0.0.1:10000"),
                        URI.create("grpc://127.0.0.1:11000"), "rack-a"),
                new MetadataCommand.CreateColumn(11, new ClusterColumnMetadata(42, "accounts", schema(), 3)),
                new MetadataCommand.DeleteColumn(11, 42, 3),
                new MetadataCommand.CreatePartition(11, partition(new ShardId(1), null, null, peer)),
                new MetadataCommand.SplitPartition(11, new ShardId(1), 3,
                        partition(new ShardId(2), null, buf(0x10), peer),
                        partition(new ShardId(3), buf(0x10), null, peer)),
                new MetadataCommand.MergePartitions(11, new ShardId(2), new ShardId(3),
                        partition(new ShardId(4), null, null, peer)),
                new MetadataCommand.ChangePartitionPeers(11, new ShardId(1), 3, List.of(peer), leader),
                new MetadataCommand.UpdateColumnSchema(11, 42, 3, 4, schema()),
                new MetadataCommand.PrepareSchemaChange(11, 100, SchemaChangeKind.CREATE_COLUMN, 42, 3,
                        new ClusterColumnMetadata(42, "accounts", schema(), 3), null),
                new MetadataCommand.ActivateSchemaChange(11, 100, 42, 3),
                new MetadataCommand.FenceSchemaChange(11, 100, 42, 3),
                new MetadataCommand.RecordBackupManifest(11, backupManifest(11, 12)),
                new MetadataCommand.RecordCdcTokenTranslation(11, splitTranslation(11))
        );

        assertEquals(Set.of(
                MetadataCommandKind.REGISTER_NODE,
                MetadataCommandKind.CREATE_COLUMN,
                MetadataCommandKind.DELETE_COLUMN,
                MetadataCommandKind.CREATE_PARTITION,
                MetadataCommandKind.SPLIT_PARTITION,
                MetadataCommandKind.MERGE_PARTITIONS,
                MetadataCommandKind.CHANGE_PARTITION_PEERS,
                MetadataCommandKind.UPDATE_COLUMN_SCHEMA,
                MetadataCommandKind.PREPARE_SCHEMA_CHANGE,
                MetadataCommandKind.ACTIVATE_SCHEMA_CHANGE,
                MetadataCommandKind.FENCE_SCHEMA_CHANGE,
                MetadataCommandKind.RECORD_BACKUP_MANIFEST,
                MetadataCommandKind.RECORD_CDC_TOKEN_TRANSLATION
        ), commands.stream().map(MetadataCommand::kind).collect(Collectors.toSet()));
        assertEquals(Set.of(11L), commands.stream()
                .map(MetadataCommand::expectedMetadataEpoch)
                .collect(Collectors.toSet()));
    }

    @Test
    void metadataCommandsRejectNullInputsWithExplicitDiagnostics() {
        var nodeId = NodeId.of("n1");
        var publicEndpoint = URI.create("http://127.0.0.1:10000");
        var ratisEndpoint = URI.create("grpc://127.0.0.1:11000");
        var peer = peer(nodeId);
        var partition = partition(new ShardId(1), null, null, peer);
        var column = new ClusterColumnMetadata(42, "accounts", schema(), 3);
        var activation = new MergeOperatorActivation("counter", 5, buf(0x01, 0x02));

        assertIllegalArgument("nodeId must not be null",
                () -> new MetadataCommand.RegisterNode(0, null, publicEndpoint, ratisEndpoint, "rack-a"));
        assertIllegalArgument("publicEndpoint must not be null",
                () -> new MetadataCommand.RegisterNode(0, nodeId, null, ratisEndpoint, "rack-a"));
        assertIllegalArgument("ratisEndpoint must not be null",
                () -> new MetadataCommand.RegisterNode(0, nodeId, publicEndpoint, null, "rack-a"));
        assertIllegalArgument("column must not be null",
                () -> new MetadataCommand.CreateColumn(0, null));
        assertIllegalArgument("partition must not be null",
                () -> new MetadataCommand.CreatePartition(0, null));
        assertIllegalArgument("sourcePartitionId must not be null",
                () -> new MetadataCommand.SplitPartition(0, null, 3, partition, partition));
        assertIllegalArgument("left must not be null",
                () -> new MetadataCommand.SplitPartition(0, new ShardId(1), 3, null, partition));
        assertIllegalArgument("right must not be null",
                () -> new MetadataCommand.SplitPartition(0, new ShardId(1), 3, partition, null));
        assertIllegalArgument("leftPartitionId must not be null",
                () -> new MetadataCommand.MergePartitions(0, null, new ShardId(2), partition));
        assertIllegalArgument("rightPartitionId must not be null",
                () -> new MetadataCommand.MergePartitions(0, new ShardId(1), null, partition));
        assertIllegalArgument("merged must not be null",
                () -> new MetadataCommand.MergePartitions(0, new ShardId(1), new ShardId(2), null));
        assertIllegalArgument("partitionId must not be null",
                () -> new MetadataCommand.ChangePartitionPeers(0, null, 3, List.of(peer), nodeId));
        assertIllegalArgument("peers must not be null",
                () -> new MetadataCommand.ChangePartitionPeers(0, new ShardId(1), 3, null, nodeId));
        assertIllegalArgument("peers entry must not be null",
                () -> new MetadataCommand.ChangePartitionPeers(
                        0, new ShardId(1), 3, Collections.singletonList(null), null));
        assertIllegalArgument("schema must not be null",
                () -> new MetadataCommand.UpdateColumnSchema(0, 42, 3, 4, null));
        assertIllegalArgument("changeKind must not be null",
                () -> new MetadataCommand.PrepareSchemaChange(0, 100, null, 42, 3, column, null));
        assertIllegalArgument("column must not be null",
                () -> new MetadataCommand.PrepareSchemaChange(
                        0, 100, SchemaChangeKind.CREATE_COLUMN, 42, 3, null, null));
        assertIllegalArgument("mergeOperatorActivation must not be null",
                () -> new MetadataCommand.PrepareSchemaChange(
                        0, 100, SchemaChangeKind.MERGE_OPERATOR_ACTIVATION, 42, 4, null, null));
        assertIllegalArgument("manifest must not be null",
                () -> new MetadataCommand.RecordBackupManifest(0, null));
        assertIllegalArgument("translation must not be null",
                () -> new MetadataCommand.RecordCdcTokenTranslation(0, null));
        assertEquals(activation, new MetadataCommand.PrepareSchemaChange(
                0, 100, SchemaChangeKind.MERGE_OPERATOR_ACTIVATION, 42, 4, null, activation)
                .mergeOperatorActivation());
    }

    @Test
    void schemaChangeMetadataRejectsNullInputsWithExplicitDiagnostics() {
        var column = new ClusterColumnMetadata(42, "accounts", schema(), 3);
        var activation = new MergeOperatorActivation("counter", 5, buf(0x01, 0x02));
        var metadata = new SchemaChangeMetadata(
                100,
                SchemaChangeKind.MERGE_OPERATOR_ACTIVATION,
                42,
                4,
                null,
                activation,
                SchemaChangeStatus.PREPARED
        );

        assertIllegalArgument("changeKind must not be null",
                () -> new SchemaChangeMetadata(100, null, 42, 3, column, null, SchemaChangeStatus.PREPARED));
        assertIllegalArgument("status must not be null",
                () -> new SchemaChangeMetadata(100, SchemaChangeKind.CREATE_COLUMN, 42, 3, column, null, null));
        assertIllegalArgument("column must not be null",
                () -> new SchemaChangeMetadata(
                        100, SchemaChangeKind.CREATE_COLUMN, 42, 3, null, null, SchemaChangeStatus.PREPARED));
        assertIllegalArgument("mergeOperatorActivation must not be null",
                () -> new SchemaChangeMetadata(
                        100,
                        SchemaChangeKind.MERGE_OPERATOR_ACTIVATION,
                        42,
                        4,
                        null,
                        null,
                        SchemaChangeStatus.PREPARED));
        assertIllegalArgument("status must not be null", () -> metadata.withStatus(null));
    }

    @Test
    void changePartitionPeersRequiresVotingLeaderHint() {
        var learner = new ReplicaMember(
                NodeId.of("n1"),
                URI.create("http://127.0.0.1:10001"),
                ReplicaRole.LEARNER,
                0,
                0,
                false
        );

        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.ChangePartitionPeers(
                7,
                new ShardId(1),
                3,
                List.of(learner),
                learner.nodeId()
        ));
    }

    @Test
    void registerNodeRequiresFailureDomain() {
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.RegisterNode(
                0,
                NodeId.of("n1"),
                URI.create("http://127.0.0.1:10000"),
                URI.create("http://127.0.0.1:11000"),
                " "
        ));
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.RegisterNode(
                0,
                NodeId.of("n1"),
                URI.create("http://127.0.0.1:10000"),
                URI.create("grpc://127.0.0.1:11000"),
                "rack-a\ninjected=true"
        ));
    }

    @Test
    void registerNodeRequiresSeparatePublicAndRatisEndpoints() {
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.RegisterNode(
                0,
                NodeId.of("n1"),
                URI.create("grpc://127.0.0.1:10000"),
                URI.create("grpc://127.0.0.1:10000"),
                "rack-a"
        ));
    }

    @Test
    void registerNodeRejectsInvalidEndpoints() {
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.RegisterNode(
                0,
                NodeId.of("n1"),
                URI.create("http://127.0.0.1"),
                URI.create("grpc://127.0.0.1:11000"),
                "rack-a"
        ));
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.RegisterNode(
                0,
                NodeId.of("n1"),
                URI.create("http://127.0.0.1:10000/node"),
                URI.create("grpc://127.0.0.1:11000"),
                "rack-a"
        ));
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.RegisterNode(
                0,
                NodeId.of("n1"),
                URI.create("http://127.0.0.1:10000"),
                URI.create("http://127.0.0.1:11000"),
                "rack-a"
        ));
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.RegisterNode(
                0,
                NodeId.of("n1"),
                URI.create("http://127.0.0.1:10000"),
                URI.create("grpc://127.0.0.1:11000/ratis"),
                "rack-a"
        ));
    }

    @Test
    void splitPartitionRequiresSharedBoundary() {
        var peer = peer(NodeId.of("n1"));

        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.SplitPartition(
                7,
                new ShardId(1),
                3,
                partition(new ShardId(2), null, buf(0x10), peer),
                partition(new ShardId(3), buf(0x11), null, peer)
        ));
    }

    @Test
    void peerChangeLeaderHintMustReferencePeer() {
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.ChangePartitionPeers(
                7,
                new ShardId(1),
                3,
                List.of(peer(NodeId.of("n1"))),
                NodeId.of("n2")
        ));
    }

    @Test
    void peerChangeRejectsDuplicatePeerNodeIds() {
        var peer = peer(NodeId.of("n1"));

        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.ChangePartitionPeers(
                7,
                new ShardId(1),
                3,
                List.of(peer, peer),
                peer.nodeId()
        ));
    }

    @Test
    void updateColumnSchemaMustAdvanceEpoch() {
        assertThrows(IllegalArgumentException.class,
                () -> new MetadataCommand.UpdateColumnSchema(7, 42, 3, 3, schema()));
    }

    @Test
    void replicatedMetadataRejectsLocalMergeOperatorClassShortcuts() {
        var localMergeColumn = new ClusterColumnMetadata(42, "accounts", schemaWithLocalMergeClass(), 3);

        assertThrows(IllegalArgumentException.class,
                () -> new MetadataCommand.CreateColumn(7, localMergeColumn));
        assertThrows(IllegalArgumentException.class,
                () -> new MetadataCommand.UpdateColumnSchema(7, 42, 3, 4, schemaWithLocalMergeClass()));
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.PrepareSchemaChange(
                7,
                100,
                SchemaChangeKind.CREATE_COLUMN,
                42,
                3,
                localMergeColumn,
                null
        ));
    }

    @Test
    void deleteColumnRejectsNegativeIds() {
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.DeleteColumn(7, -1, 0));
    }

    @Test
    void prepareSchemaChangeValidatesCreatePayload() {
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.PrepareSchemaChange(
                7,
                100,
                SchemaChangeKind.CREATE_COLUMN,
                42,
                3,
                new ClusterColumnMetadata(43, "accounts", schema(), 3),
                null
        ));
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.PrepareSchemaChange(
                7,
                100,
                SchemaChangeKind.CREATE_COLUMN,
                42,
                3,
                new ClusterColumnMetadata(42, "accounts", schema(), 4),
                null
        ));
    }

    @Test
    void prepareSchemaChangeValidatesDeletePayload() {
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.PrepareSchemaChange(
                7,
                100,
                SchemaChangeKind.DELETE_COLUMN,
                42,
                3,
                new ClusterColumnMetadata(42, "accounts", schema(), 3),
                null
        ));
    }

    @Test
    void prepareSchemaChangeValidatesMergeOperatorActivationPayload() {
        var activation = new MergeOperatorActivation("counter", 5, buf(0x01, 0x02));

        var command = new MetadataCommand.PrepareSchemaChange(7, 100,
                SchemaChangeKind.MERGE_OPERATOR_ACTIVATION, 42, 4, null, activation);

        assertEquals(activation, command.mergeOperatorActivation());
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.PrepareSchemaChange(
                7,
                100,
                SchemaChangeKind.MERGE_OPERATOR_ACTIVATION,
                42,
                4,
                new ClusterColumnMetadata(42, "accounts", schema(), 4),
                activation
        ));
        assertIllegalArgument("mergeOperatorActivation must not be null",
                () -> new MetadataCommand.PrepareSchemaChange(
                        7,
                        100,
                        SchemaChangeKind.MERGE_OPERATOR_ACTIVATION,
                        42,
                        4,
                        null,
                        null
                ));
    }

    @Test
    void activateAndFenceSchemaChangeRejectNegativeIds() {
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.ActivateSchemaChange(7, -1, 42, 3));
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.FenceSchemaChange(7, -1, 42, 3));
    }

    @Test
    void mergeOperatorActivationRejectsInvalidMetadata() {
        assertThrows(IllegalArgumentException.class, () -> new MergeOperatorActivation(" ", 1, buf(0x01)));
        assertThrows(IllegalArgumentException.class,
                () -> new MergeOperatorActivation("counter\ninjected=true", 1, buf(0x01)));
        assertThrows(IllegalArgumentException.class, () -> new MergeOperatorActivation("counter", -1, buf(0x01)));
        assertThrows(IllegalArgumentException.class, () -> new MergeOperatorActivation("counter", 1, buf()));
    }

    @Test
    void backupManifestUsesMetadataEpochFenceAndPartitionCuts() {
        var manifest = new BackupManifest("backup-7", 12, 11, List.of(
                backupCut(new ShardId(9), 3, 5, 100, 12),
                backupCut(new ShardId(1), 3, 4, 90, 12)
        ));

        var command = new MetadataCommand.RecordBackupManifest(11, manifest);

        assertEquals(MetadataCommandKind.RECORD_BACKUP_MANIFEST, command.kind());
        assertEquals(11, command.expectedMetadataEpoch());
        assertEquals(12, command.manifest().metadataBackupEpoch());
        assertEquals(List.of(new ShardId(1), new ShardId(9)), command.manifest().partitionCuts().stream()
                .map(BackupManifest.PartitionCut::partitionId)
                .toList());
    }

    @Test
    void backupManifestRejectsInvalidCuts() {
        assertThrows(IllegalArgumentException.class, () -> new BackupManifest(" ", 12, 11,
                List.of(backupCut(new ShardId(1), 3, 4, 90, 12))));
        assertThrows(IllegalArgumentException.class, () -> new BackupManifest("backup-7\ninjected=true", 12, 11,
                List.of(backupCut(new ShardId(1), 3, 4, 90, 12))));
        assertThrows(IllegalArgumentException.class, () -> new BackupManifest("backup-7", 12, 11, List.of()));
        assertThrows(IllegalArgumentException.class, () -> new BackupManifest("backup-7", 12, 11, List.of(
                backupCut(new ShardId(1), 3, 4, 90, 12),
                backupCut(new ShardId(1), 3, 5, 100, 12)
        )));
        assertThrows(IllegalArgumentException.class, () -> new BackupManifest("backup-7", 10, 11,
                List.of(backupCut(new ShardId(1), 3, 4, 90, 10))));
        assertThrows(IllegalArgumentException.class, () -> new BackupManifest("backup-7", 12, 11,
                List.of(backupCut(new ShardId(1), 3, 4, 90, 11))));
        assertThrows(IllegalArgumentException.class, () -> new BackupManifest.PartitionCut(
                new ShardId(1), 3, 4, -1, 12));
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.RecordBackupManifest(13,
                backupManifest(13, 12)));
        assertThrows(IllegalArgumentException.class, () -> new MetadataCommand.RecordBackupManifest(11,
                new BackupManifest("backup-7", 12, 11,
                        List.of(backupCut(new ShardId(1), 3, 4, 90, 11)))));
    }

    @Test
    void cdcTokenTranslationRecordsSplitAndMergeMetadata() {
        var split = splitTranslation(12);
        var merge = new CdcTokenTranslation.Merge(
                13,
                token(12, new ShardId(2), 4, 20, 0),
                token(12, new ShardId(3), 4, 21, 0),
                token(13, new ShardId(4), 5, 0, 0)
        );

        var splitCommand = new MetadataCommand.RecordCdcTokenTranslation(12, split);
        var mergeCommand = new MetadataCommand.RecordCdcTokenTranslation(13, merge);

        assertEquals(MetadataCommandKind.RECORD_CDC_TOKEN_TRANSLATION, splitCommand.kind());
        assertEquals(CdcTokenTranslation.TranslationKind.SPLIT, splitCommand.translation().kind());
        assertEquals(CdcTokenTranslation.TranslationKind.MERGE, mergeCommand.translation().kind());
    }

    @Test
    void cdcTokenTranslationRejectsInvalidMetadata() {
        assertThrows(IllegalArgumentException.class, () -> new CdcTokenTranslation.Split(
                12,
                token(11, new ShardId(1), 3, 10, 0),
                token(12, new ShardId(1), 4, 0, 0),
                token(12, new ShardId(3), 4, 0, 0)
        ));
        assertThrows(IllegalArgumentException.class, () -> new CdcTokenTranslation.Split(
                12,
                token(11, new ShardId(1), 3, 10, 0),
                token(11, new ShardId(2), 4, 0, 0),
                token(12, new ShardId(3), 4, 0, 0)
        ));
        assertThrows(IllegalArgumentException.class, () -> new CdcTokenTranslation.Merge(
                13,
                token(12, new ShardId(2), 4, 20, 0),
                token(12, new ShardId(2), 4, 21, 0),
                token(13, new ShardId(4), 5, 0, 0)
        ));
        assertThrows(IllegalArgumentException.class,
                () -> new MetadataCommand.RecordCdcTokenTranslation(13, splitTranslation(12)));
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }

    private static PartitionDescriptor partition(ShardId partitionId,
                                                  Buf startKeyInclusive,
                                                  Buf endKeyExclusive,
                                                 ReplicaMember peer) {
        return new PartitionDescriptor(
                partitionId,
                startKeyInclusive,
                endKeyExclusive,
                3,
                "group-" + partitionId,
                List.of(peer),
                peer.nodeId(),
                Map.of("rack", "rack-a"),
                PartitionSplitState.ACTIVE
        );
    }

    private static ReplicaMember peer(NodeId nodeId) {
        return new ReplicaMember(nodeId, URI.create("http://127.0.0.1:10001"), ReplicaRole.VOTER, 0, 0, true);
    }

    private static ColumnSchema schema() {
        return ColumnSchema.of(IntArrayList.of(4), ObjectArrayList.of(), true);
    }

    private static ColumnSchema schemaWithLocalMergeClass() {
        return ColumnSchema.of(IntArrayList.of(4), ObjectArrayList.of(), true, null, null,
                "it.cavallium.LocalMergeOperator");
    }

    private static BackupManifest backupManifest() {
        return backupManifest(7, 7);
    }

    private static BackupManifest backupManifest(long expectedMetadataEpoch, long metadataBackupEpoch) {
        return new BackupManifest("backup-" + metadataBackupEpoch, metadataBackupEpoch, expectedMetadataEpoch,
                List.of(backupCut(new ShardId(1), 3, 4, 90, metadataBackupEpoch)));
    }

    private static BackupManifest.PartitionCut backupCut(ShardId partitionId,
                                                         long partitionEpoch,
                                                         long ratisTerm,
                                                         long ratisIndex,
                                                         long observedMetadataBackupEpoch) {
        return new BackupManifest.PartitionCut(partitionId, partitionEpoch, ratisTerm, ratisIndex,
                observedMetadataBackupEpoch);
    }

    private static CdcTokenTranslation.Split splitTranslation() {
        return splitTranslation(7);
    }

    private static CdcTokenTranslation.Split splitTranslation(long metadataEpoch) {
        return new CdcTokenTranslation.Split(
                metadataEpoch,
                token(metadataEpoch - 1, new ShardId(1), 3, 10, 0),
                token(metadataEpoch, new ShardId(2), 4, 0, 0),
                token(metadataEpoch, new ShardId(3), 4, 0, 0)
        );
    }

    private static ReplicatedCdcToken token(long metadataEpoch,
                                            ShardId partitionId,
                                            long partitionEpoch,
                                            long appliedIndex,
                                            int eventOrdinal) {
        return new ReplicatedCdcToken(metadataEpoch, partitionId, partitionEpoch, appliedIndex, eventOrdinal);
    }

    private static Buf buf(int... values) {
        var bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return Buf.wrap(bytes);
    }
}
