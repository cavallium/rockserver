package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaGroupMetadata;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcToken;
import it.cavallium.rockserver.core.cluster.metadata.BackupManifest;
import it.cavallium.rockserver.core.cluster.metadata.CdcTokenTranslation;
import it.cavallium.rockserver.core.cluster.metadata.MergeOperatorActivation;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandApplier;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeKind;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeStatus;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class MetadataCommandApplierTest {

    @Test
    void appliesColumnMetadataCommandsWithEpochFences() {
        var metadata = emptyMetadata(7);
        var column = new ClusterColumnMetadata(42, "accounts", schema(), 0);

        var created = MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.CreateColumn(7, column)
        );
        assertEquals(8, created.epoch());
        assertEquals(column, created.requireColumn(42));

        var deleted = MetadataCommandApplier.apply(
                created,
                new MetadataCommand.DeleteColumn(8, 42, 0)
        );
        assertEquals(9, deleted.epoch());
        assertTrue(deleted.findColumn(42).isEmpty());

        assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.CreateColumn(6, column)
        ));
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var metadata = emptyMetadata(7);
        var command = new MetadataCommand.DeleteColumn(7, 42, 0);

        assertIllegalArgument("metadata must not be null", () -> MetadataCommandApplier.apply(null, command));
        assertIllegalArgument("command must not be null", () -> MetadataCommandApplier.apply(metadata, null));
    }

    @Test
    void directUpdateColumnSchemaApplicationIsRejected() {
        var column = new ClusterColumnMetadata(42, "accounts", schema(), 3);
        var metadata = new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(),
                Map.of(column.columnId(), column)
        );

        var failure = assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.UpdateColumnSchema(7, 42, 3, 4, schema())
        ));

        assertEquals("direct column schema updates are unsupported; use schema-change prepare/activate/fence",
                failure.getMessage());
    }

    @Test
    void appliesCreatePartitionToPartitionAndRouteMetadata() {
        var n1 = NodeId.of("n1");
        var partition = partition(new ShardId(7), n1, 1);
        var metadata = emptyMetadata(11);

        var updated = MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.CreatePartition(11, partition)
        );

        assertEquals(12, updated.epoch());
        assertEquals(partition, updated.requirePartition(partition.partitionId()));
        var group = updated.requireGroup(partition.partitionId());
        assertEquals(partition.partitionId(), group.shardId());
        assertEquals(partition.leaderHint(), group.leader());
        assertEquals(partition.peers(), group.members());
        assertEquals(partition.epoch(), group.term());
        assertEquals(partition.epoch(), group.configIndex());
    }

    @Test
    void createPartitionRejectsNonActiveInitialPartitionState() {
        var n1 = NodeId.of("n1");
        var partition = partition(new ShardId(7), 1,
                List.of(replica(n1, ReplicaRole.VOTER, 0, 0, true)),
                n1,
                PartitionSplitState.SPLIT_TARGET);
        var metadata = emptyMetadata(11);

        assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.CreatePartition(11, partition)
        ));
    }

    @Test
    void appliesSplitPartitionToPartitionAndRouteMetadata() {
        var n1 = NodeId.of("n1");
        var source = partition(new ShardId(7), n1, 3, null, buf(0x20));
        var left = partition(new ShardId(8), n1, 4, null, buf(0x10));
        var right = partition(new ShardId(9), n1, 4, buf(0x10), buf(0x20));
        var metadata = metadataWithPartitions(11, source);

        var updated = MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.SplitPartition(11, source.partitionId(), 3, left, right)
        );

        assertEquals(12, updated.epoch());
        assertTrue(updated.findPartition(source.partitionId()).isEmpty());
        assertTrue(updated.findGroup(source.partitionId()).isEmpty());
        assertEquals(left, updated.requirePartition(left.partitionId()));
        assertEquals(right, updated.requirePartition(right.partitionId()));
        assertEquals(left.peers(), updated.requireGroup(left.partitionId()).members());
        assertEquals(right.peers(), updated.requireGroup(right.partitionId()).members());
    }

    @Test
    void splitPartitionRejectsChildTopologyDrift() {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var source = partition(new ShardId(7), n1, 3, null, buf(0x20));
        var leftWithDifferentTopology = partition(new ShardId(8), n2, 4, null, buf(0x10));
        var right = partition(new ShardId(9), n1, 4, buf(0x10), buf(0x20));
        var metadata = metadataWithPartitions(11, source);

        assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.SplitPartition(11, source.partitionId(), 3, leftWithDifferentTopology, right)
        ));
    }

    @Test
    void appliesMergePartitionsToPartitionAndRouteMetadata() {
        var n1 = NodeId.of("n1");
        var left = partition(new ShardId(8), n1, 4, null, buf(0x10));
        var right = partition(new ShardId(9), n1, 4, buf(0x10), buf(0x20));
        var merged = partition(new ShardId(10), n1, 5, null, buf(0x20));
        var metadata = metadataWithPartitions(11, left, right);

        var updated = MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.MergePartitions(11, left.partitionId(), right.partitionId(), merged)
        );

        assertEquals(12, updated.epoch());
        assertTrue(updated.findPartition(left.partitionId()).isEmpty());
        assertTrue(updated.findPartition(right.partitionId()).isEmpty());
        assertTrue(updated.findGroup(left.partitionId()).isEmpty());
        assertTrue(updated.findGroup(right.partitionId()).isEmpty());
        assertEquals(merged, updated.requirePartition(merged.partitionId()));
        assertEquals(merged.peers(), updated.requireGroup(merged.partitionId()).members());
    }

    @Test
    void mergePartitionsRejectsParentOrMergedTopologyDrift() {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var left = partition(new ShardId(8), n1, 4, null, buf(0x10));
        var rightWithDifferentTopology = partition(new ShardId(9), n2, 4, buf(0x10), buf(0x20));
        var merged = partition(new ShardId(10), n1, 5, null, buf(0x20));

        assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                metadataWithPartitions(11, left, rightWithDifferentTopology),
                new MetadataCommand.MergePartitions(11, left.partitionId(), rightWithDifferentTopology.partitionId(),
                        merged)
        ));

        var right = partition(new ShardId(9), n1, 4, buf(0x10), buf(0x20));
        var mergedWithDifferentTopology = partition(new ShardId(10), n2, 5, null, buf(0x20));
        assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                metadataWithPartitions(11, left, right),
                new MetadataCommand.MergePartitions(11, left.partitionId(), right.partitionId(),
                        mergedWithDifferentTopology)
        ));
    }

    @Test
    void appliesPartitionPeerChangesWithPartitionEpochFence() {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var partition = partition(new ShardId(7), n1, 3);
        var group = new ReplicaGroupMetadata(
                partition.partitionId(),
                9,
                n1,
                partition.peers(),
                12
        );
        var metadata = new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                11,
                Map.of(partition.partitionId(), group),
                Map.of(),
                Map.of(partition.partitionId(), partition)
        );
        var addedLearner = new ReplicaMember(
                n2,
                URI.create("grpc://127.0.0.1:19002"),
                ReplicaRole.LEARNER,
                0,
                0,
                false
        );
        var proposedPeers = List.of(partition.peers().getFirst(), addedLearner);

        var updated = MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.ChangePartitionPeers(11, partition.partitionId(), 3, proposedPeers, n1)
        );

        assertEquals(12, updated.epoch());
        var updatedPartition = updated.requirePartition(partition.partitionId());
        assertEquals(4, updatedPartition.epoch());
        assertEquals(proposedPeers, updatedPartition.peers());
        assertEquals(n1, updatedPartition.leaderHint());
        var updatedGroup = updated.requireGroup(partition.partitionId());
        assertEquals(9, updatedGroup.term());
        assertEquals(13, updatedGroup.configIndex());
        assertEquals(proposedPeers, updatedGroup.members());
        assertEquals(n1, updatedGroup.leader());
    }

    @Test
    void peerChangeRequiresRouteGroupForPartition() {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var partition = partition(new ShardId(7), n1, 3);
        var metadata = new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                11,
                Map.of(),
                Map.of(),
                Map.of(partition.partitionId(), partition)
        );
        var replacementPeer = new ReplicaMember(
                n2,
                URI.create("grpc://127.0.0.1:19002"),
                ReplicaRole.VOTER,
                10,
                10,
                true
        );

        assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.ChangePartitionPeers(11, partition.partitionId(), 3, List.of(replacementPeer), n2)
        ));
    }

    @Test
    void peerChangeRejectsArbitraryReplacementOrNoopConfigBump() {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var partition = partition(new ShardId(7), n1, 3);
        var metadata = metadataWithPartitions(11, partition);
        var replacementPeer = new ReplicaMember(
                n2,
                URI.create("grpc://127.0.0.1:19002"),
                ReplicaRole.VOTER,
                10,
                10,
                true
        );

        assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.ChangePartitionPeers(11, partition.partitionId(), 3, List.of(replacementPeer), n2)
        ));
        assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.ChangePartitionPeers(11, partition.partitionId(), 3, partition.peers(), n1)
        ));
    }

    @Test
    void peerChangeRejectsUnsafePromotionOrChangingTopologyState() {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var leader = replica(n1, ReplicaRole.VOTER, 10, 10, true);
        var laggingLearner = replica(n2, ReplicaRole.LEARNER, 9, 9, false);
        var partition = partition(new ShardId(7), 3, List.of(leader, laggingLearner), n1,
                PartitionSplitState.ACTIVE);
        var promotedLaggingLearner = replica(n2, ReplicaRole.VOTER, 9, 9, false);

        assertThrows(IllegalStateException.class, () -> MetadataCommandApplier.apply(
                metadataWithPartitions(11, partition),
                new MetadataCommand.ChangePartitionPeers(11, partition.partitionId(), 3,
                        List.of(leader, promotedLaggingLearner), n1)
        ));

        var splittingPartition = partition(new ShardId(7), 3, List.of(leader), n1,
                PartitionSplitState.SPLIT_SOURCE);
        var addedLearner = replica(n2, ReplicaRole.LEARNER, 0, 0, false);
        assertThrows(IllegalStateException.class, () -> MetadataCommandApplier.apply(
                metadataWithPartitions(11, splittingPartition),
                new MetadataCommand.ChangePartitionPeers(11, splittingPartition.partitionId(), 3,
                        List.of(leader, addedLearner), n1)
        ));
    }

    @Test
    void appliesCdcTokenTranslationWithEpochFence() {
        var n1 = NodeId.of("n1");
        var sourcePartition = new ShardId(7);
        var leftPartition = new ShardId(8);
        var rightPartition = new ShardId(9);
        var left = partition(leftPartition, n1, 4, null, buf(0x10));
        var right = partition(rightPartition, n1, 4, buf(0x10), null);
        var metadata = metadataWithPartitions(12, left, right);
        var translation = new CdcTokenTranslation.Split(
                12,
                token(11, sourcePartition, 3, 20, 0),
                token(12, leftPartition, 4, 0, 0),
                token(12, rightPartition, 4, 0, 0)
        );

        var updated = MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.RecordCdcTokenTranslation(12, translation)
        );

        assertEquals(12, updated.epoch());
        assertEquals(translation, updated.requireCdcTokenTranslation(12));
        assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.RecordCdcTokenTranslation(12, new CdcTokenTranslation.Split(
                        13,
                        token(11, sourcePartition, 3, 20, 0),
                        token(13, leftPartition, 4, 0, 0),
                        token(13, rightPartition, 4, 0, 0)
                ))
        ));
    }

    @Test
    void cdcSplitTranslationRejectsUnrelatedChildrenOrNonStartTokens() {
        var n1 = NodeId.of("n1");
        var sourcePartition = new ShardId(7);
        var leftPartition = new ShardId(8);
        var rightPartition = new ShardId(9);
        var left = partition(leftPartition, n1, 4, null, buf(0x10));
        var rightWithGap = partition(rightPartition, n1, 4, buf(0x20), null);
        var gappedMetadata = metadataWithPartitions(12, left, rightWithGap);

        assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                gappedMetadata,
                new MetadataCommand.RecordCdcTokenTranslation(12, new CdcTokenTranslation.Split(
                        12,
                        token(11, sourcePartition, 3, 20, 0),
                        token(12, leftPartition, 4, 0, 0),
                        token(12, rightPartition, 4, 0, 0)
                ))
        ));

        var right = partition(rightPartition, n1, 4, buf(0x10), null);
        var adjacentMetadata = metadataWithPartitions(12, left, right);
        assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                adjacentMetadata,
                new MetadataCommand.RecordCdcTokenTranslation(12, new CdcTokenTranslation.Split(
                        12,
                        token(11, sourcePartition, 3, 20, 0),
                        token(12, leftPartition, 4, 1, 0),
                        token(12, rightPartition, 4, 0, 0)
                ))
        ));
    }

    @Test
    void cdcMergeTranslationRejectsCurrentParentsOrNonStartMergedToken() {
        var n1 = NodeId.of("n1");
        var leftPartition = new ShardId(8);
        var rightPartition = new ShardId(9);
        var mergedPartition = new ShardId(10);
        var left = partition(leftPartition, n1, 4, buf(0x30), buf(0x40));
        var merged = partition(mergedPartition, n1, 5, null, buf(0x20));
        var metadataWithLiveParent = metadataWithPartitions(12, left, merged);

        assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                metadataWithLiveParent,
                new MetadataCommand.RecordCdcTokenTranslation(12, new CdcTokenTranslation.Merge(
                        12,
                        token(11, leftPartition, 4, 20, 0),
                        token(11, rightPartition, 4, 30, 0),
                        token(12, mergedPartition, 5, 0, 0)
                ))
        ));

        var mergedOnlyMetadata = metadataWithPartitions(12, merged);
        assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                mergedOnlyMetadata,
                new MetadataCommand.RecordCdcTokenTranslation(12, new CdcTokenTranslation.Merge(
                        12,
                        token(11, leftPartition, 4, 20, 0),
                        token(11, rightPartition, 4, 30, 0),
                        token(12, mergedPartition, 5, 1, 0)
                ))
        ));
    }

    @Test
    void appliesSchemaChangePrepareActivateAndFence() {
        var metadata = emptyMetadata(7);
        var column = new ClusterColumnMetadata(42, "accounts", schema(), 3);

        var prepared = MetadataCommandApplier.apply(metadata, new MetadataCommand.PrepareSchemaChange(
                7,
                100,
                SchemaChangeKind.CREATE_COLUMN,
                42,
                3,
                column,
                null
        ));

        assertEquals(8, prepared.epoch());
        assertEquals(SchemaChangeStatus.PREPARED, prepared.requireSchemaChange(100).status());
        assertTrue(prepared.findColumn(42).isEmpty());

        var activated = MetadataCommandApplier.apply(prepared,
                new MetadataCommand.ActivateSchemaChange(8, 100, 42, 3));

        assertEquals(9, activated.epoch());
        assertEquals(column, activated.requireColumn(42));
        assertEquals(SchemaChangeStatus.ACTIVATED, activated.requireSchemaChange(100).status());

        var fenced = MetadataCommandApplier.apply(activated,
                new MetadataCommand.FenceSchemaChange(9, 100, 42, 3));

        assertEquals(10, fenced.epoch());
        assertEquals(SchemaChangeStatus.FENCED, fenced.requireSchemaChange(100).status());
    }

    @Test
    void activatesMergeOperatorSchemaChange() {
        var column = new ClusterColumnMetadata(42, "accounts", schema(), 3);
        var metadata = new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(),
                Map.of(column.columnId(), column)
        );
        var activation = new MergeOperatorActivation("counter", 5, buf(0x01, 0x02));

        var prepared = MetadataCommandApplier.apply(metadata, new MetadataCommand.PrepareSchemaChange(
                7,
                101,
                SchemaChangeKind.MERGE_OPERATOR_ACTIVATION,
                42,
                4,
                null,
                activation
        ));
        var activated = MetadataCommandApplier.apply(prepared,
                new MetadataCommand.ActivateSchemaChange(8, 101, 42, 4));

        var updatedColumn = activated.requireColumn(42);
        assertEquals(4, updatedColumn.schemaEpoch());
        assertEquals("counter", updatedColumn.schema().mergeOperatorName());
        assertEquals(5, updatedColumn.schema().mergeOperatorVersion());
        assertNull(updatedColumn.schema().mergeOperatorClass());
    }

    @Test
    void appliesBackupManifestStateWithPartitionEpochFences() {
        var n1 = NodeId.of("n1");
        var partition = partition(new ShardId(7), n1, 3);
        var metadata = metadataWithPartitions(11, partition);
        var manifest = new BackupManifest(
                "backup-12",
                12,
                11,
                List.of(new BackupManifest.PartitionCut(partition.partitionId(), 3, 5, 100, 12))
        );

        var updated = MetadataCommandApplier.apply(metadata,
                new MetadataCommand.RecordBackupManifest(11, manifest));

        assertEquals(12, updated.epoch());
        assertEquals(manifest, updated.requireBackupManifest("backup-12"));
        assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.RecordBackupManifest(11, new BackupManifest(
                        "backup-13",
                        13,
                        11,
                        List.of(new BackupManifest.PartitionCut(partition.partitionId(), 4, 5, 100, 13))
                ))
        ));
    }

    @Test
    void backupManifestApplicationRejectsFutureBackupEpochOrPartialCuts() {
        var n1 = NodeId.of("n1");
        var left = partition(new ShardId(7), n1, 3, null, buf(0x10));
        var right = partition(new ShardId(8), n1, 3, buf(0x10), null);
        var metadata = metadataWithPartitions(11, left, right);

        assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.RecordBackupManifest(11, new BackupManifest(
                        "backup-13",
                        13,
                        11,
                        List.of(
                                new BackupManifest.PartitionCut(left.partitionId(), 3, 5, 100, 13),
                                new BackupManifest.PartitionCut(right.partitionId(), 3, 5, 101, 13)
                        )
                ))
        ));
        assertThrows(IllegalArgumentException.class, () -> MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.RecordBackupManifest(11, new BackupManifest(
                        "backup-12",
                        12,
                        11,
                        List.of(new BackupManifest.PartitionCut(left.partitionId(), 3, 5, 100, 12))
                ))
        ));
    }

    @Test
    void backupManifestApplicationRejectsChangingTopologyPartitions() {
        var n1 = NodeId.of("n1");
        var partition = partition(new ShardId(7), 3, List.of(replica(n1, ReplicaRole.VOTER, 0, 0, true)), n1,
                PartitionSplitState.SPLIT_SOURCE);
        var metadata = metadataWithPartitions(11, partition);

        assertThrows(IllegalStateException.class, () -> MetadataCommandApplier.apply(
                metadata,
                new MetadataCommand.RecordBackupManifest(11, new BackupManifest(
                        "backup-12",
                        12,
                        11,
                        List.of(new BackupManifest.PartitionCut(partition.partitionId(), 3, 5, 100, 12))
                ))
        ));
    }

    private static ClusterMetadata emptyMetadata(long epoch) {
        return new ClusterMetadata(new ClusterId(new UUID(0, 1)), epoch, Map.of());
    }

    private static ClusterMetadata metadataWithPartitions(long epoch, PartitionDescriptor... partitions) {
        var groups = new java.util.LinkedHashMap<ShardId, ReplicaGroupMetadata>();
        var partitionMap = new java.util.LinkedHashMap<ShardId, PartitionDescriptor>();
        for (var partition : partitions) {
            groups.put(partition.partitionId(), new ReplicaGroupMetadata(
                    partition.partitionId(),
                    partition.epoch(),
                    partition.leaderHint(),
                    partition.peers(),
                    partition.epoch()
            ));
            partitionMap.put(partition.partitionId(), partition);
        }
        return new ClusterMetadata(new ClusterId(new UUID(0, 1)), epoch, groups, Map.of(), partitionMap);
    }

    private static PartitionDescriptor partition(ShardId partitionId, NodeId leader, long epoch) {
        return partition(partitionId, leader, epoch, null, null);
    }

    private static PartitionDescriptor partition(ShardId partitionId,
                                                 NodeId leader,
                                                 long epoch,
                                                 Buf startKeyInclusive,
                                                 Buf endKeyExclusive) {
        return partition(partitionId, epoch, List.of(replica(leader, ReplicaRole.VOTER, 0, 0, true)), leader,
                PartitionSplitState.ACTIVE, startKeyInclusive, endKeyExclusive);
    }

    private static PartitionDescriptor partition(ShardId partitionId,
                                                 long epoch,
                                                 List<ReplicaMember> peers,
                                                 NodeId leader,
                                                 PartitionSplitState splitState) {
        return partition(partitionId, epoch, peers, leader, splitState, null, null);
    }

    private static PartitionDescriptor partition(ShardId partitionId,
                                                 long epoch,
                                                 List<ReplicaMember> peers,
                                                 NodeId leader,
                                                 PartitionSplitState splitState,
                                                 Buf startKeyInclusive,
                                                 Buf endKeyExclusive) {
        return new PartitionDescriptor(
                partitionId,
                startKeyInclusive,
                endKeyExclusive,
                epoch,
                "group-" + partitionId,
                peers,
                leader,
                Map.of(),
                splitState
        );
    }

    private static ReplicaMember replica(NodeId nodeId,
                                         ReplicaRole role,
                                         long matchIndex,
                                         long appliedIndex,
                                         boolean servingReads) {
        var port = 19_000 + Integer.parseInt(nodeId.value().substring(1));
        return new ReplicaMember(
                nodeId,
                URI.create("grpc://127.0.0.1:" + port),
                role,
                matchIndex,
                appliedIndex,
                servingReads
        );
    }

    private static ColumnSchema schema() {
        return ColumnSchema.of(IntList.of(1), ObjectList.of(), true);
    }

    private static Buf buf(int value) {
        return Buf.wrap(new byte[]{(byte) value});
    }

    private static Buf buf(int... values) {
        var bytes = new byte[values.length];
        for (int index = 0; index < values.length; index++) {
            bytes[index] = (byte) values[index];
        }
        return Buf.wrap(bytes);
    }

    private static ReplicatedCdcToken token(long metadataEpoch,
                                            ShardId partitionId,
                                            long partitionEpoch,
                                            long appliedIndex,
                                            int eventOrdinal) {
        return new ReplicatedCdcToken(metadataEpoch, partitionId, partitionEpoch, appliedIndex, eventOrdinal);
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }
}
