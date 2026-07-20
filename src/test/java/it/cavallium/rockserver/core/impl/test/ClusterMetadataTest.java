package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ClusterNodeMetadata;
import it.cavallium.rockserver.core.cluster.LocalColumnFamilyMapping;
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
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeKind;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeMetadata;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeStatus;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class ClusterMetadataTest {

    @Test
    void coreMetadataModelsRejectNullInputsWithExplicitDiagnostics() {
        var clusterId = new ClusterId(new UUID(0, 1));
        var nodeId = NodeId.of("n1");
        var peer = new ReplicaMember(nodeId, URI.create("http://127.0.0.1:10001"), ReplicaRole.VOTER, 0, 0, true);
        var group = new ReplicaGroupMetadata(ShardId.DEFAULT, 1, nodeId, List.of(peer), 1);
        var partition = partition(new ShardId(7));
        var column = new ClusterColumnMetadata(42, "accounts", schema(), 3);
        var metadata = new ClusterMetadata(
                clusterId,
                7,
                Map.of(group.shardId(), group),
                Map.of(column.columnId(), column),
                Map.of(partition.partitionId(), partition)
        );

        assertIllegalArgument("value must not be null", () -> new ClusterId(null));
        assertIllegalArgument("value must not be null", () -> new ClientId(null));
        assertIllegalArgument("clientId must not be null", () -> new ClientRequestId(null, 0));
        assertIllegalArgument("nodeId must not be null",
                () -> new ClusterNodeMetadata(null, URI.create("http://127.0.0.1:10001"),
                        URI.create("grpc://127.0.0.1:10001"), "rack-a"));
        assertIllegalArgument("nodeId must not be null",
                () -> new ReplicaMember(null, URI.create("http://127.0.0.1:10001"), ReplicaRole.VOTER, 0, 0,
                        true));
        assertIllegalArgument("role must not be null",
                () -> new ReplicaMember(nodeId, URI.create("http://127.0.0.1:10001"), null, 0, 0, true));
        assertIllegalArgument("shardId must not be null",
                () -> new ReplicaGroupMetadata(null, 1, nodeId, List.of(peer), 1));
        assertIllegalArgument("members must not be null",
                () -> new ReplicaGroupMetadata(ShardId.DEFAULT, 1, nodeId, null, 1));
        assertIllegalArgument("partitionId must not be null",
                () -> new PartitionDescriptor(null, null, null, 3, "group-7", List.of(peer), nodeId, Map.of(),
                        PartitionSplitState.ACTIVE));
        assertIllegalArgument("peers must not be null",
                () -> new PartitionDescriptor(new ShardId(7), null, null, 3, "group-7", null, nodeId, Map.of(),
                        PartitionSplitState.ACTIVE));
        assertIllegalArgument("placementConstraints must not be null",
                () -> new PartitionDescriptor(new ShardId(7), null, null, 3, "group-7", List.of(peer), nodeId, null,
                        PartitionSplitState.ACTIVE));
        assertIllegalArgument("splitState must not be null",
                () -> new PartitionDescriptor(new ShardId(7), null, null, 3, "group-7", List.of(peer), nodeId,
                        Map.of(), null));
        assertIllegalArgument("calculatedKey must not be null", () -> partition.containsCalculatedKey(null));
        assertIllegalArgument("schema must not be null", () -> new ClusterColumnMetadata(42, "accounts", null, 3));
        assertIllegalArgument("columnFamilyName must not be null", () -> new LocalColumnFamilyMapping(42, null));
        assertIllegalArgument("columnFamilyName must not be null", () -> LocalColumnFamilyMapping.parseColumnId(null));

        assertIllegalArgument("clusterId must not be null",
                () -> new ClusterMetadata(null, 7, Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(),
                        Map.of()));
        assertIllegalArgument("shards must not be null",
                () -> new ClusterMetadata(clusterId, 7, null, Map.of(), Map.of(), Map.of(), Map.of(), Map.of(),
                        Map.of()));
        assertIllegalArgument("columnsById must not be null",
                () -> new ClusterMetadata(clusterId, 7, Map.of(), null, Map.of(), Map.of(), Map.of(), Map.of(),
                        Map.of()));
        assertIllegalArgument("partitionsById must not be null",
                () -> new ClusterMetadata(clusterId, 7, Map.of(), Map.of(), null, Map.of(), Map.of(), Map.of(),
                        Map.of()));
        assertIllegalArgument("nodesById must not be null",
                () -> new ClusterMetadata(clusterId, 7, Map.of(), Map.of(), Map.of(), null, Map.of(), Map.of(),
                        Map.of()));
        assertIllegalArgument("cdcTokenTranslationsByEpoch must not be null",
                () -> new ClusterMetadata(clusterId, 7, Map.of(), Map.of(), Map.of(), Map.of(), null, Map.of(),
                        Map.of()));
        assertIllegalArgument("schemaChangesById must not be null",
                () -> new ClusterMetadata(clusterId, 7, Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), null,
                        Map.of()));
        assertIllegalArgument("backupManifestsById must not be null",
                () -> new ClusterMetadata(clusterId, 7, Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(),
                        null));

        assertIllegalArgument("shardId must not be null", () -> metadata.findGroup(null));
        assertIllegalArgument("shardId must not be null", () -> metadata.requireGroup(null));
        assertIllegalArgument("name must not be null", () -> metadata.findColumn((String) null));
        assertIllegalArgument("partitionId must not be null", () -> metadata.findPartition(null));
        assertIllegalArgument("partitionId must not be null", () -> metadata.requirePartition(null));
        assertIllegalArgument("backupId must not be null", () -> metadata.findBackupManifest(null));
        assertIllegalArgument("backupId must not be null", () -> metadata.requireBackupManifest(null));
        assertIllegalArgument("nodeId must not be null", () -> metadata.findNode(null));
        assertIllegalArgument("nodeId must not be null", () -> metadata.requireNode(null));
    }

    @Test
    void clusterNodeMetadataRejectsInvalidEndpoints() {
        var nodeId = NodeId.of("n1");
        assertThrows(IllegalArgumentException.class, () -> new ClusterNodeMetadata(
                nodeId,
                URI.create("http://127.0.0.1"),
                URI.create("grpc://127.0.0.1:19001"),
                "rack-a"
        ));
        assertThrows(IllegalArgumentException.class, () -> new ClusterNodeMetadata(
                nodeId,
                URI.create("http://127.0.0.1:8080/node"),
                URI.create("grpc://127.0.0.1:19001"),
                "rack-a"
        ));
        assertThrows(IllegalArgumentException.class, () -> new ClusterNodeMetadata(
                nodeId,
                URI.create("http://127.0.0.1:8080"),
                URI.create("http://127.0.0.1:19001"),
                "rack-a"
        ));
        assertThrows(IllegalArgumentException.class, () -> new ClusterNodeMetadata(
                nodeId,
                URI.create("http://127.0.0.1:8080"),
                URI.create("grpc://127.0.0.1:19001/ratis"),
                "rack-a"
        ));
    }

    @Test
    void clusterNodeMetadataRejectsInvalidFailureDomain() {
        assertThrows(IllegalArgumentException.class, () -> node("rack-a\ninjected=true"));
    }

    @Test
    void replicaMemberRejectsInvalidEndpoint() {
        var nodeId = NodeId.of("n1");
        assertThrows(IllegalArgumentException.class, () -> new ReplicaMember(
                nodeId,
                URI.create("http://127.0.0.1"),
                ReplicaRole.VOTER,
                0,
                0,
                true
        ));
        assertThrows(IllegalArgumentException.class, () -> new ReplicaMember(
                nodeId,
                URI.create("http://127.0.0.1:10001/member"),
                ReplicaRole.VOTER,
                0,
                0,
                true
        ));
    }

    @Test
    void replicaMemberRejectsAppliedIndexBeyondMatchIndex() {
        var nodeId = NodeId.of("n1");

        assertThrows(IllegalArgumentException.class, () -> new ReplicaMember(
                nodeId,
                URI.create("http://127.0.0.1:10001"),
                ReplicaRole.VOTER,
                9,
                10,
                true
        ));
    }

    @Test
    void replicaGroupRequiresVotingLeader() {
        var node = NodeId.of("n1");
        var member = new ReplicaMember(node, URI.create("http://127.0.0.1:10000"), ReplicaRole.LEARNER, 0, 0, true);

        assertThrows(IllegalArgumentException.class,
                () -> new ReplicaGroupMetadata(ShardId.DEFAULT, 1, node, List.of(member), 1));
    }

    @Test
    void computesQuorumFromVotersOnly() {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var n3 = NodeId.of("n3");
        var learner = NodeId.of("n4");
        var group = new ReplicaGroupMetadata(ShardId.DEFAULT, 2, n1, List.of(
                new ReplicaMember(n1, URI.create("http://127.0.0.1:10001"), ReplicaRole.VOTER, 10, 10, true),
                new ReplicaMember(n2, URI.create("http://127.0.0.1:10002"), ReplicaRole.VOTER, 10, 10, true),
                new ReplicaMember(n3, URI.create("http://127.0.0.1:10003"), ReplicaRole.VOTER, 8, 8, true),
                new ReplicaMember(learner, URI.create("http://127.0.0.1:10004"), ReplicaRole.LEARNER, 7, 7, false)
        ), 5);

        assertEquals(3, group.voterCount());
        assertEquals(2, group.quorumSize());
        assertTrue(group.hasQuorum(2));
    }

    @Test
    void clusterMetadataRequiresMatchingMapKey() {
        var clusterId = new ClusterId(new UUID(0, 1));
        var leader = NodeId.of("n1");
        var group = new ReplicaGroupMetadata(new ShardId(1), 1, leader, List.of(
                new ReplicaMember(leader, URI.create("http://127.0.0.1:10001"), ReplicaRole.VOTER, 0, 0, true)
        ), 1);

        assertThrows(IllegalArgumentException.class,
                () -> new ClusterMetadata(clusterId, 1, Map.of(ShardId.DEFAULT, group)));
    }

    @Test
    void clusterMetadataIndexesClusterAssignedColumns() {
        var column = new ClusterColumnMetadata(42, "accounts", schema(), 3);
        var metadata = new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of(), Map.of(42L, column));

        assertEquals(column, metadata.requireColumn(42));
        assertEquals(column, metadata.findColumn(42).orElseThrow());
        assertEquals(column, metadata.findColumn("accounts").orElseThrow());
        assertEquals(1, metadata.columnDefinitions().size());
    }

    @Test
    void clusterMetadataRequiresMatchingColumnMapKey() {
        var column = new ClusterColumnMetadata(42, "accounts", schema(), 3);

        assertThrows(IllegalArgumentException.class,
                () -> new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of(), Map.of(43L, column)));
    }

    @Test
    void clusterMetadataRejectsDuplicateColumnNames() {
        var first = new ClusterColumnMetadata(42, "accounts", schema(), 3);
        var second = new ClusterColumnMetadata(43, "accounts", schema(), 3);

        assertThrows(IllegalArgumentException.class,
                () -> new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of(), Map.of(
                        first.columnId(), first,
                        second.columnId(), second
                )));
    }

    @Test
    void clusterColumnMetadataRejectsInvalidName() {
        assertThrows(IllegalArgumentException.class,
                () -> new ClusterColumnMetadata(42, " ", schema(), 3));
        assertThrows(IllegalArgumentException.class,
                () -> new ClusterColumnMetadata(42, "accounts\ninjected=true", schema(), 3));
    }

    @Test
    void columnSchemaRejectsInvalidMergeOperatorStrings() {
        assertThrows(IllegalArgumentException.class,
                () -> ColumnSchema.of(IntArrayList.of(4), ObjectArrayList.of(), true, " ", 1L));
        assertThrows(IllegalArgumentException.class,
                () -> ColumnSchema.of(IntArrayList.of(4), ObjectArrayList.of(), true,
                        "merge\ninjected=true", 1L));
        assertThrows(IllegalArgumentException.class,
                () -> ColumnSchema.of(IntArrayList.of(4), ObjectArrayList.of(), true, "merge", 1L, " "));
        assertThrows(IllegalArgumentException.class,
                () -> ColumnSchema.of(IntArrayList.of(4), ObjectArrayList.of(), true, "merge", 1L,
                        "it.example.Merge\ninjected=true"));
    }

    @Test
    void clusterMetadataIndexesPartitionDescriptors() {
        var partition = partition(new ShardId(7));
        var metadata = new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of(), Map.of(), Map.of(
                partition.partitionId(), partition
        ));

        assertEquals(partition, metadata.requirePartition(new ShardId(7)));
        assertEquals(partition, metadata.findPartition(new ShardId(7)).orElseThrow());
        assertEquals(1, metadata.partitionDescriptors().size());
    }

    @Test
    void clusterMetadataRequiresMatchingPartitionMapKey() {
        var partition = partition(new ShardId(7));

        assertThrows(IllegalArgumentException.class,
                () -> new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of(), Map.of(), Map.of(
                        ShardId.DEFAULT, partition
                )));
    }

    @Test
    void clusterMetadataRequiresPartitionRouteGroupToMatchDescriptor() {
        var partition = partition(new ShardId(7));
        var mismatchedMemberGroup = new ReplicaGroupMetadata(
                partition.partitionId(),
                partition.epoch(),
                partition.leaderHint(),
                List.of(new ReplicaMember(NodeId.of("n1"), URI.create("http://127.0.0.1:10002"),
                        ReplicaRole.VOTER, 0, 0, true)),
                partition.epoch()
        );
        assertThrows(IllegalArgumentException.class, () -> new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(partition.partitionId(), mismatchedMemberGroup),
                Map.of(),
                Map.of(partition.partitionId(), partition)
        ));

        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var peers = List.of(
                new ReplicaMember(n1, URI.create("http://127.0.0.1:10001"), ReplicaRole.VOTER, 0, 0, true),
                new ReplicaMember(n2, URI.create("http://127.0.0.1:10002"), ReplicaRole.VOTER, 0, 0, true)
        );
        var twoPeerPartition = new PartitionDescriptor(
                new ShardId(8),
                null,
                null,
                3,
                "group-8",
                peers,
                n1,
                Map.of("rack", "rack-a"),
                PartitionSplitState.ACTIVE
        );
        var mismatchedLeaderGroup = new ReplicaGroupMetadata(
                twoPeerPartition.partitionId(),
                twoPeerPartition.epoch(),
                n2,
                peers,
                twoPeerPartition.epoch()
        );
        assertThrows(IllegalArgumentException.class, () -> new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(twoPeerPartition.partitionId(), mismatchedLeaderGroup),
                Map.of(),
                Map.of(twoPeerPartition.partitionId(), twoPeerPartition)
        ));
    }

    @Test
    void clusterMetadataRequiresMembersToReferenceRegisteredNodes() {
        var knownNode = node("rack-a");
        var unknownNode = NodeId.of("n2");
        var unknownPeer = new ReplicaMember(
                unknownNode,
                URI.create("http://127.0.0.1:10002"),
                ReplicaRole.VOTER,
                0,
                0,
                true
        );
        var partition = new PartitionDescriptor(
                new ShardId(7),
                null,
                null,
                3,
                "group-7",
                List.of(unknownPeer),
                unknownNode,
                Map.of(),
                PartitionSplitState.ACTIVE
        );
        assertThrows(IllegalArgumentException.class, () -> new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(),
                Map.of(),
                Map.of(partition.partitionId(), partition),
                Map.of(knownNode.nodeId(), knownNode)
        ));

        var routeGroup = new ReplicaGroupMetadata(new ShardId(8), 1, unknownNode, List.of(unknownPeer), 1);
        assertThrows(IllegalArgumentException.class, () -> new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(routeGroup.shardId(), routeGroup),
                Map.of(),
                Map.of(),
                Map.of(knownNode.nodeId(), knownNode)
        ));
    }

    @Test
    void clusterMetadataRequiresRegisteredMemberEndpointMatchesNodeRatisEndpoint() {
        var knownNode = node("rack-a");
        var mismatchedPeer = new ReplicaMember(
                knownNode.nodeId(),
                URI.create("grpc://127.0.0.1:19999"),
                ReplicaRole.VOTER,
                0,
                0,
                true
        );
        var partition = new PartitionDescriptor(
                new ShardId(7),
                null,
                null,
                3,
                "group-7",
                List.of(mismatchedPeer),
                knownNode.nodeId(),
                Map.of(),
                PartitionSplitState.ACTIVE
        );

        var partitionFailure = assertThrows(IllegalArgumentException.class, () -> new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(),
                Map.of(),
                Map.of(partition.partitionId(), partition),
                Map.of(knownNode.nodeId(), knownNode)
        ));
        assertEquals("partition peer endpoint does not match registered node Ratis endpoint for n1: 7",
                partitionFailure.getMessage());

        var routeGroup = new ReplicaGroupMetadata(new ShardId(8), 1, knownNode.nodeId(), List.of(mismatchedPeer), 1);
        var routeFailure = assertThrows(IllegalArgumentException.class, () -> new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(routeGroup.shardId(), routeGroup),
                Map.of(),
                Map.of(),
                Map.of(knownNode.nodeId(), knownNode)
        ));
        assertEquals("replica group member endpoint does not match registered node Ratis endpoint for n1: 8",
                routeFailure.getMessage());
    }

    @Test
    void clusterMetadataRejectsDuplicatePartitionRatisGroupIds() {
        var n1 = NodeId.of("n1");
        var peer = new ReplicaMember(n1, URI.create("http://127.0.0.1:10001"), ReplicaRole.VOTER, 0, 0, true);
        var first = new PartitionDescriptor(
                new ShardId(7),
                null,
                null,
                3,
                "shared-group",
                List.of(peer),
                n1,
                Map.of(),
                PartitionSplitState.ACTIVE
        );
        var second = new PartitionDescriptor(
                new ShardId(8),
                null,
                null,
                3,
                "shared-group",
                List.of(peer),
                n1,
                Map.of(),
                PartitionSplitState.ACTIVE
        );

        assertThrows(IllegalArgumentException.class, () -> new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(),
                Map.of(),
                Map.of(first.partitionId(), first, second.partitionId(), second)
        ));
    }

    @Test
    void clusterMetadataRejectsOverlappingPartitionRanges() {
        var first = partition(new ShardId(7), null, buf(0x20));
        var overlapping = partition(new ShardId(8), buf(0x10), null);

        assertThrows(IllegalArgumentException.class, () -> new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(),
                Map.of(),
                Map.of(first.partitionId(), first, overlapping.partitionId(), overlapping)
        ));

        var adjacent = new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(),
                Map.of(),
                Map.of(
                        first.partitionId(), first,
                        new ShardId(9), partition(new ShardId(9), buf(0x20), null)
                )
        );
        assertEquals(2, adjacent.partitionDescriptors().size());
    }

    @Test
    void clusterMetadataIndexesNodes() {
        var node = node("rack-a");
        var metadata = new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of(), Map.of(), Map.of(), Map.of(
                node.nodeId(), node
        ));

        assertEquals(node, metadata.requireNode(NodeId.of("n1")));
        assertEquals(node, metadata.findNode(NodeId.of("n1")).orElseThrow());
        assertEquals(1, metadata.nodes().size());
    }

    @Test
    void clusterMetadataRequiresMatchingNodeMapKey() {
        var node = node("rack-a");

        assertThrows(IllegalArgumentException.class,
                () -> new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of(), Map.of(), Map.of(), Map.of(
                        NodeId.of("n2"), node
                )));
    }

    @Test
    void clusterMetadataIndexesSchemaChangesAndBackupManifests() {
        var schemaChange = new SchemaChangeMetadata(
                100,
                SchemaChangeKind.CREATE_COLUMN,
                42,
                3,
                new ClusterColumnMetadata(42, "orders", schema(), 3),
                null,
                SchemaChangeStatus.PREPARED
        );
        var manifest = new BackupManifest(
                "backup-7",
                7,
                7,
                List.of(new BackupManifest.PartitionCut(new ShardId(7), 3, 5, 100, 7))
        );
        var partition = partition(new ShardId(7));

        var metadata = new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(),
                Map.of(),
                Map.of(partition.partitionId(), partition),
                Map.of(),
                Map.of(),
                Map.of(schemaChange.schemaChangeId(), schemaChange),
                Map.of(manifest.backupId(), manifest)
        );

        assertEquals(schemaChange, metadata.requireSchemaChange(100));
        assertEquals(schemaChange, metadata.findSchemaChange(100).orElseThrow());
        assertEquals(1, metadata.schemaChanges().size());
        assertEquals(manifest, metadata.requireBackupManifest("backup-7"));
        assertEquals(manifest, metadata.findBackupManifest("backup-7").orElseThrow());
        assertEquals(1, metadata.backupManifests().size());
    }

    @Test
    void clusterMetadataRequiresMatchingSchemaChangeAndBackupKeys() {
        var schemaChange = new SchemaChangeMetadata(
                100,
                SchemaChangeKind.CREATE_COLUMN,
                42,
                3,
                new ClusterColumnMetadata(42, "orders", schema(), 3),
                null,
                SchemaChangeStatus.PREPARED
        );
        var manifest = new BackupManifest(
                "backup-7",
                7,
                7,
                List.of(new BackupManifest.PartitionCut(new ShardId(7), 3, 5, 100, 7))
        );

        assertThrows(IllegalArgumentException.class, () -> new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(101L, schemaChange),
                Map.of()
        ));
        assertThrows(IllegalArgumentException.class, () -> new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of("other", manifest)
        ));
    }

    @Test
    void clusterMetadataRequiresCurrentBackupCutsToMatchPartitions() {
        var partition = partition(new ShardId(7));
        var unknownPartitionManifest = new BackupManifest(
                "backup-7",
                7,
                7,
                List.of(new BackupManifest.PartitionCut(new ShardId(8), 3, 5, 100, 7))
        );
        assertThrows(IllegalArgumentException.class, () -> new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(),
                Map.of(),
                Map.of(partition.partitionId(), partition),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(unknownPartitionManifest.backupId(), unknownPartitionManifest)
        ));

        var mismatchedEpochManifest = new BackupManifest(
                "backup-8",
                7,
                7,
                List.of(new BackupManifest.PartitionCut(partition.partitionId(), partition.epoch() + 1, 5, 100, 7))
        );
        assertThrows(IllegalArgumentException.class, () -> new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(),
                Map.of(),
                Map.of(partition.partitionId(), partition),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(mismatchedEpochManifest.backupId(), mismatchedEpochManifest)
        ));

        var historicalManifest = new BackupManifest(
                "backup-6",
                6,
                6,
                List.of(new BackupManifest.PartitionCut(new ShardId(99), 1, 5, 100, 6))
        );
        var metadata = new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                7,
                Map.of(),
                Map.of(),
                Map.of(partition.partitionId(), partition),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(historicalManifest.backupId(), historicalManifest)
        );
        assertEquals(historicalManifest, metadata.requireBackupManifest("backup-6"));
    }

    @Test
    void clusterMetadataRequiresCurrentCdcTranslationsToMatchPostTopologyPartitions() {
        var sourcePartition = new ShardId(7);
        var leftPartition = new ShardId(8);
        var rightPartition = new ShardId(9);
        var left = partition(leftPartition, null, buf(0x10), 4);
        var right = partition(rightPartition, buf(0x10), null, 4);
        var split = new CdcTokenTranslation.Split(
                12,
                token(11, sourcePartition, 3, 20),
                token(12, leftPartition, 4, 0),
                token(12, rightPartition, 4, 0)
        );
        var splitMetadata = new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                12,
                Map.of(),
                Map.of(),
                Map.of(left.partitionId(), left, right.partitionId(), right),
                Map.of(),
                Map.of(split.metadataEpoch(), split)
        );
        assertEquals(split, splitMetadata.requireCdcTokenTranslation(12));

        assertThrows(IllegalArgumentException.class, () -> new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                12,
                Map.of(),
                Map.of(),
                Map.of(left.partitionId(), left),
                Map.of(),
                Map.of(split.metadataEpoch(), split)
        ));

        var mismatchedRight = partition(rightPartition, buf(0x10), null, 5);
        assertThrows(IllegalArgumentException.class, () -> new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                12,
                Map.of(),
                Map.of(),
                Map.of(left.partitionId(), left, mismatchedRight.partitionId(), mismatchedRight),
                Map.of(),
                Map.of(split.metadataEpoch(), split)
        ));

        var mergedPartition = new ShardId(10);
        var merged = partition(mergedPartition, 5);
        var merge = new CdcTokenTranslation.Merge(
                13,
                token(12, leftPartition, 4, 30),
                token(12, rightPartition, 4, 31),
                token(13, mergedPartition, 5, 0)
        );
        var mergeMetadata = new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                13,
                Map.of(),
                Map.of(),
                Map.of(merged.partitionId(), merged),
                Map.of(),
                Map.of(merge.metadataEpoch(), merge)
        );
        assertEquals(merge, mergeMetadata.requireCdcTokenTranslation(13));

        var mismatchedMerged = partition(mergedPartition, 6);
        assertThrows(IllegalArgumentException.class, () -> new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                13,
                Map.of(),
                Map.of(),
                Map.of(mismatchedMerged.partitionId(), mismatchedMerged),
                Map.of(),
                Map.of(merge.metadataEpoch(), merge)
        ));

        var historicalSplit = new CdcTokenTranslation.Split(
                11,
                token(10, sourcePartition, 3, 20),
                token(11, leftPartition, 4, 0),
                token(11, rightPartition, 4, 0)
        );
        var historicalMetadata = new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                12,
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(historicalSplit.metadataEpoch(), historicalSplit)
        );
        assertEquals(historicalSplit, historicalMetadata.requireCdcTokenTranslation(11));
    }

    @Test
    void localColumnFamilyMappingIsDerivedFromClusterColumnId() {
        var mapping = LocalColumnFamilyMapping.forColumn(42);

        assertEquals(42, mapping.columnId());
        assertEquals("_rk_col_42", mapping.columnFamilyName());
        assertTrue(mapping.isCanonical());
        assertEquals(42, LocalColumnFamilyMapping.parseColumnId(mapping.columnFamilyName()).orElseThrow());
        assertTrue(LocalColumnFamilyMapping.parseColumnId("accounts").isEmpty());
    }

    @Test
    void clusterAssignedColumnIdsProduceStableMappingsAcrossStores() {
        var column = new ClusterColumnMetadata(42, "accounts", schema(), 3);
        var firstStoreMetadata = new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of(),
                Map.of(column.columnId(), column));
        var reloadedStoreMetadata = new ClusterMetadata(firstStoreMetadata.clusterId(), firstStoreMetadata.epoch(),
                firstStoreMetadata.shards(), firstStoreMetadata.columnsById());

        var firstMapping = LocalColumnFamilyMapping.forColumn(firstStoreMetadata.requireColumn(42).columnId());
        var reloadedMapping = LocalColumnFamilyMapping.forColumn(reloadedStoreMetadata.requireColumn(42).columnId());

        assertEquals(42, firstStoreMetadata.findColumn("accounts").orElseThrow().columnId());
        assertEquals(firstMapping, reloadedMapping);
        assertEquals("_rk_col_42", reloadedMapping.columnFamilyName());
    }

    @Test
    void localColumnFamilyMappingRejectsNegativeIds() {
        assertThrows(IllegalArgumentException.class, () -> LocalColumnFamilyMapping.forColumn(-1));
        assertTrue(LocalColumnFamilyMapping.parseColumnId("_rk_col_-1").isEmpty());
    }

    private static ColumnSchema schema() {
        return ColumnSchema.of(IntArrayList.of(4), ObjectArrayList.of(), true);
    }

    private static PartitionDescriptor partition(ShardId partitionId) {
        return partition(partitionId, null, null);
    }

    private static PartitionDescriptor partition(ShardId partitionId, long epoch) {
        return partition(partitionId, null, null, epoch);
    }

    private static PartitionDescriptor partition(ShardId partitionId, Buf startKeyInclusive, Buf endKeyExclusive) {
        return partition(partitionId, startKeyInclusive, endKeyExclusive, 3);
    }

    private static PartitionDescriptor partition(ShardId partitionId,
                                                 Buf startKeyInclusive,
                                                 Buf endKeyExclusive,
                                                 long epoch) {
        var leader = NodeId.of("n1");
        return new PartitionDescriptor(
                partitionId,
                startKeyInclusive,
                endKeyExclusive,
                epoch,
                "group-" + partitionId,
                List.of(new ReplicaMember(leader, URI.create("http://127.0.0.1:10001"), ReplicaRole.VOTER, 0, 0,
                        true)),
                leader,
                Map.of("rack", "rack-a"),
                PartitionSplitState.ACTIVE
        );
    }

    private static ClusterNodeMetadata node(String failureDomain) {
        return new ClusterNodeMetadata(
                NodeId.of("n1"),
                URI.create("http://127.0.0.1:10001"),
                URI.create("grpc://127.0.0.1:10001"),
                failureDomain
        );
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
                                            long appliedIndex) {
        return new ReplicatedCdcToken(metadataEpoch, partitionId, partitionEpoch, appliedIndex, 0);
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }
}
