package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ClusterNodeMetadata;
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
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeKind;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeMetadata;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeStatus;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterMetadataConfig;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RatisProcessAdapterMetadataConfigTest {

    @Test
    void metadataConfigRoundTripsAdapterStateMetadata(@TempDir Path tempDir) throws Exception {
        var source = metadata();
        var config = new RatisProcessAdapterMetadataConfig(source);
        var path = tempDir.resolve("adapter-metadata.properties");

        config.write(path);

        var decoded = RatisProcessAdapterMetadataConfig.read(path).metadata();
        assertEquals(source.clusterId(), decoded.clusterId());
        assertEquals(source.epoch(), decoded.epoch());
        assertEquals(source.shards(), decoded.shards());
        assertEquals(source.nodesById(), decoded.nodesById());

        var column = decoded.requireColumn(42);
        assertEquals("accounts", column.name());
        assertEquals(7, column.schemaEpoch());
        assertEquals(2, column.schema().keysCount());
        assertEquals(4, column.schema().key(0));
        assertEquals(ColumnHashType.XXHASH8, column.schema().variableTailKey(1));
        assertTrue(column.schema().hasValue());
        assertEquals("merge", column.schema().mergeOperatorName());
        assertEquals(3, column.schema().mergeOperatorVersion());
        assertEquals("it.example.Merge", column.schema().mergeOperatorClass());

        var partition = decoded.requirePartition(new ShardId(7));
        var startKeyInclusive = partition.startKeyInclusive();
        var endKeyExclusive = partition.endKeyExclusive();
        assertNotNull(startKeyInclusive);
        assertNotNull(endKeyExclusive);
        assertArrayEquals(new byte[]{1, 2}, startKeyInclusive.toByteArray());
        assertArrayEquals(new byte[]{9}, endKeyExclusive.toByteArray());
        assertEquals("ratis-group-7", partition.ratisGroupId());
        assertEquals(NodeId.of("n1"), partition.leaderHint());
        assertEquals(Map.of("rack", "rack-a"), partition.placementConstraints());
        assertEquals(List.of(
                member("n1", ReplicaRole.VOTER, 11, 10, true),
                member("n2", ReplicaRole.LEARNER, 8, 7, false)
        ), partition.peers());
        assertEquals(source.cdcTokenTranslationsByEpoch(), decoded.cdcTokenTranslationsByEpoch());
        assertEquals(source.schemaChangesById(), decoded.schemaChangesById());
        assertEquals(source.backupManifestsById(), decoded.backupManifestsById());
    }

    @Test
    void metadataConfigRejectsNullInputsWithExplicitDiagnostics() {
        var nullMetadata = assertThrows(
                IllegalArgumentException.class,
                () -> new RatisProcessAdapterMetadataConfig(null)
        );
        var nullProperties = assertThrows(
                IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(null)
        );
        var nullPayload = assertThrows(
                IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromBytes(null)
        );
        var nullPath = assertThrows(
                IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.read(null)
        );

        assertEquals("metadata must not be null", nullMetadata.getMessage());
        assertEquals("properties must not be null", nullProperties.getMessage());
        assertEquals("payload must not be null", nullPayload.getMessage());
        assertEquals("path must not be null", nullPath.getMessage());
    }

    @Test
    void metadataConfigRejectsUnknownKeys() {
        var properties = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        properties.setProperty("unexpected", "value");

        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(properties));
    }

    @Test
    void metadataConfigRejectsNonContiguousIndexes() {
        var properties = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        properties.remove("node.0.nodeId");
        properties.setProperty("node.1.nodeId", "n3");
        properties.setProperty("node.1.publicEndpoint", "http://n3.public:10000");
        properties.setProperty("node.1.ratisEndpoint", "grpc://10.0.0.3:19003");
        properties.setProperty("node.1.failureDomain", "rack-c");

        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(properties));
    }

    @Test
    void metadataConfigRejectsNonCanonicalIndexes() {
        var topLevel = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        topLevel.setProperty("node.00.nodeId", "n0");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(topLevel));

        var nestedObject = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        nestedObject.setProperty("partition.0.peer.00.nodeId", "n0");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(nestedObject));

        var indexedValue = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        indexedValue.setProperty("column.0.schema.fixedKey.00", "8");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(indexedValue));
    }

    @Test
    void metadataConfigRejectsNonCanonicalNumericValues() {
        var epoch = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        epoch.setProperty("epoch", "012");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(epoch));

        var partitionId = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        partitionId.setProperty("partition.0.partitionId", "07");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(partitionId));

        var fixedKey = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        fixedKey.setProperty("column.0.schema.fixedKey.0", "04");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(fixedKey));

        var optionalLong = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        optionalLong.setProperty("column.0.schema.mergeOperatorVersion", "+3");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(optionalLong));
    }

    @Test
    void metadataConfigRejectsNonCanonicalBase64ByteValues() {
        var boundary = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        boundary.setProperty("partition.0.startKeyInclusive", "AQI");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(boundary));

        var artifactHash = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        artifactHash.setProperty("schemaChange.0.mergeActivation.artifactHash", "BAU");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(artifactHash));
    }

    @Test
    void metadataConfigRejectsMissingRequiredKeys() {
        var properties = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        properties.remove("clusterId");

        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(properties));
    }

    @Test
    void metadataConfigRejectsInvalidClusterIdWithKeyedDiagnostic() {
        var properties = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        properties.setProperty("clusterId", "not-a-uuid");

        var failure = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(properties));

        assertEquals(
                "Invalid UUID for Ratis process adapter metadata key clusterId: not-a-uuid",
                failure.getMessage()
        );
    }

    @Test
    void metadataConfigRejectsDuplicateIdentifiers() {
        var duplicateNode = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        duplicateNode.setProperty("node.1.nodeId", "n1");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(duplicateNode));

        var duplicatePartitionPeer = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        duplicatePartitionPeer.setProperty("partition.0.peer.1.nodeId", "n1");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(duplicatePartitionPeer));

        var duplicateGroupMember = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        duplicateGroupMember.setProperty("shard.0.member.1.nodeId", "n1");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(duplicateGroupMember));

        var duplicatePlacement = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        duplicatePlacement.setProperty("partition.0.placement.1.key", "rack");
        duplicatePlacement.setProperty("partition.0.placement.1.value", "rack-b");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(duplicatePlacement));

        var duplicatePartitionGroupId = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        duplicatePartitionGroupId.setProperty("partition.1.partitionId", "8");
        duplicatePartitionGroupId.setProperty("partition.1.epoch", "5");
        duplicatePartitionGroupId.setProperty("partition.1.ratisGroupId", "ratis-group-7");
        duplicatePartitionGroupId.setProperty("partition.1.leaderHint", "n1");
        duplicatePartitionGroupId.setProperty("partition.1.splitState", "ACTIVE");
        duplicatePartitionGroupId.setProperty("partition.1.peer.0.nodeId", "n1");
        duplicatePartitionGroupId.setProperty("partition.1.peer.0.endpoint", "http://n1.public:10000");
        duplicatePartitionGroupId.setProperty("partition.1.peer.0.role", "VOTER");
        duplicatePartitionGroupId.setProperty("partition.1.peer.0.matchIndex", "0");
        duplicatePartitionGroupId.setProperty("partition.1.peer.0.appliedIndex", "0");
        duplicatePartitionGroupId.setProperty("partition.1.peer.0.servingReads", "true");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(duplicatePartitionGroupId));
    }

    @Test
    void metadataConfigRejectsOverlappingPartitionRanges() {
        var overlappingPartition = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        overlappingPartition.setProperty("partition.1.partitionId", "8");
        overlappingPartition.setProperty("partition.1.startKeyInclusive", "BA==");
        overlappingPartition.setProperty("partition.1.epoch", "5");
        overlappingPartition.setProperty("partition.1.ratisGroupId", "ratis-group-8");
        overlappingPartition.setProperty("partition.1.leaderHint", "n1");
        overlappingPartition.setProperty("partition.1.splitState", "ACTIVE");
        overlappingPartition.setProperty("partition.1.peer.0.nodeId", "n1");
        overlappingPartition.setProperty("partition.1.peer.0.endpoint", "http://n1.public:10000");
        overlappingPartition.setProperty("partition.1.peer.0.role", "VOTER");
        overlappingPartition.setProperty("partition.1.peer.0.matchIndex", "0");
        overlappingPartition.setProperty("partition.1.peer.0.appliedIndex", "0");
        overlappingPartition.setProperty("partition.1.peer.0.servingReads", "true");

        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(overlappingPartition));
    }

    @Test
    void metadataConfigRejectsInvalidBooleans() {
        var properties = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        properties.setProperty("partition.0.peer.0.servingReads", "maybe");

        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(properties));
    }

    @Test
    void metadataConfigRejectsInvalidEnumsWithKeyedDiagnostics() {
        assertInvalidEnum("column.0.schema.variableTailKey.0");
        assertInvalidEnum("partition.0.splitState");
        assertInvalidEnum("cdcTranslation.0.kind");
        assertInvalidEnum("schemaChange.0.kind");
        assertInvalidEnum("schemaChange.0.status");
        assertInvalidEnum("partition.0.peer.0.role");
        assertInvalidEnum("shard.0.member.0.role");
    }

    @Test
    void metadataConfigRejectsInvalidColumnName() {
        var properties = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        properties.setProperty("column.0.name", "accounts\ninjected=true");

        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(properties));
    }

    @Test
    void metadataConfigRejectsInvalidMergeOperatorSchemaStrings() {
        var invalidName = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        invalidName.setProperty("column.0.schema.mergeOperatorName", "merge\ninjected=true");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(invalidName));

        var invalidClass = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        invalidClass.setProperty("column.0.schema.mergeOperatorClass", "it.example.Merge\ninjected=true");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(invalidClass));
    }

    @Test
    void metadataConfigRejectsInvalidPartitionRatisGroupId() {
        var properties = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        properties.setProperty("partition.0.ratisGroupId", "ratis-group-7\ninjected=true");

        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(properties));
    }

    @Test
    void metadataConfigRejectsCurrentBackupCutMismatches() {
        var unknownPartition = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        unknownPartition.setProperty("backupManifest.0.cut.0.partitionId", "8");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(unknownPartition));

        var mismatchedPartitionEpoch = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        mismatchedPartitionEpoch.setProperty("backupManifest.0.cut.0.partitionEpoch", "6");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(mismatchedPartitionEpoch));
    }

    @Test
    void metadataConfigRejectsCurrentCdcTranslationMismatches() {
        var unknownChildPartition = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        unknownChildPartition.setProperty("cdcTranslation.0.metadataEpoch", "12");
        unknownChildPartition.setProperty("cdcTranslation.0.leftStartToken.metadataEpoch", "12");
        unknownChildPartition.setProperty("cdcTranslation.0.rightStartToken.metadataEpoch", "12");

        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(unknownChildPartition));
    }

    @Test
    void metadataConfigRejectsPartialOptionalSchemaChangePayloadGroups() {
        var partialColumn = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        partialColumn.setProperty("schemaChange.0.column.name", "accounts");

        var partialColumnFailure = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(partialColumn));
        assertEquals("Incomplete optional Ratis process adapter metadata group: schemaChange.0.column.",
                partialColumnFailure.getMessage());

        var partialMergeActivation = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        partialMergeActivation.setProperty("schemaChange.0.kind", "DELETE_COLUMN");
        partialMergeActivation.remove("schemaChange.0.mergeActivation.name");

        var partialMergeActivationFailure = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(partialMergeActivation));
        assertEquals("Incomplete optional Ratis process adapter metadata group: schemaChange.0.mergeActivation.",
                partialMergeActivationFailure.getMessage());
    }

    @Test
    void metadataConfigRejectsInvalidPlacementConstraints() {
        var invalidKey = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        invalidKey.setProperty("partition.0.placement.0.key", "rack\ninjected=true");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(invalidKey));

        var invalidValue = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        invalidValue.setProperty("partition.0.placement.0.value", "rack-a\ninjected=true");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(invalidValue));
    }

    @Test
    void metadataConfigRejectsInvalidNodeRatisEndpoint() {
        var nonGrpc = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        nonGrpc.setProperty("node.0.ratisEndpoint", "http://10.0.0.1:19001");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(nonGrpc));

        var decorated = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        decorated.setProperty("node.0.ratisEndpoint", "grpc://10.0.0.1:19001/ratis");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(decorated));
    }

    @Test
    void metadataConfigRejectsInvalidNodePublicEndpoint() {
        var missingPort = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        missingPort.setProperty("node.0.publicEndpoint", "http://n1.public");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(missingPort));

        var decorated = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        decorated.setProperty("node.0.publicEndpoint", "http://n1.public:10000/rockserver");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(decorated));
    }

    @Test
    void metadataConfigRejectsInvalidReplicaMemberEndpoints() {
        var partitionPeer = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        partitionPeer.setProperty("partition.0.peer.0.endpoint", "http://n1.public");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(partitionPeer));

        var groupMember = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        groupMember.setProperty("shard.0.member.0.endpoint", "http://n1.public:10000/member");
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(groupMember));
    }

    private static ClusterMetadata metadata() {
        var n1 = node("n1", "grpc://10.0.0.1:19001", "rack-a");
        var n2 = node("n2", "grpc://10.0.0.2:19002", "rack-b");
        var column = new ClusterColumnMetadata(
                42,
                "accounts",
                ColumnSchema.of(
                        IntArrayList.of(4),
                        ObjectArrayList.of(ColumnHashType.XXHASH8),
                        true,
                        "merge",
                        3L,
                        "it.example.Merge"
                ),
                7
        );
        var partition = new PartitionDescriptor(
                new ShardId(7),
                Buf.wrap(new byte[]{1, 2}).freeze(),
                Buf.wrap(new byte[]{9}).freeze(),
                5,
                "ratis-group-7",
                List.of(
                        member("n1", ReplicaRole.VOTER, 11, 10, true),
                        member("n2", ReplicaRole.LEARNER, 8, 7, false)
                ),
                NodeId.of("n1"),
                Map.of("rack", "rack-a"),
                PartitionSplitState.ACTIVE
        );
        var group = new ReplicaGroupMetadata(
                partition.partitionId(),
                6,
                NodeId.of("n1"),
                partition.peers(),
                9
        );
        var translation = new CdcTokenTranslation.Split(
                11,
                token(10, new ShardId(70), 4, 99),
                token(11, new ShardId(80), 5, 0),
                token(11, new ShardId(90), 5, 0)
        );
        var schemaChange = new SchemaChangeMetadata(
                100,
                SchemaChangeKind.MERGE_OPERATOR_ACTIVATION,
                column.columnId(),
                column.schemaEpoch() + 1,
                null,
                new MergeOperatorActivation("counter", 9, Buf.wrap(new byte[]{4, 5}).freeze()),
                SchemaChangeStatus.PREPARED
        );
        var backupManifest = new BackupManifest(
                "backup-12",
                12,
                12,
                List.of(new BackupManifest.PartitionCut(partition.partitionId(), partition.epoch(), 6, 90, 12))
        );
        return new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                12,
                Map.of(group.shardId(), group),
                Map.of(column.columnId(), column),
                Map.of(partition.partitionId(), partition),
                Map.of(
                        n1.nodeId(), n1,
                        n2.nodeId(), n2
                ),
                Map.of(translation.metadataEpoch(), translation),
                Map.of(schemaChange.schemaChangeId(), schemaChange),
                Map.of(backupManifest.backupId(), backupManifest)
        );
    }

    private static ClusterNodeMetadata node(String nodeId, String ratisEndpoint, String failureDomain) {
        return new ClusterNodeMetadata(
                NodeId.of(nodeId),
                URI.create("http://" + nodeId + ".public:10000"),
                URI.create(ratisEndpoint),
                failureDomain
        );
    }

    private static ReplicaMember member(String nodeId,
                                        ReplicaRole role,
                                        long matchIndex,
                                        long appliedIndex,
                                        boolean servingReads) {
        return new ReplicaMember(
                NodeId.of(nodeId),
                ratisEndpoint(nodeId),
                role,
                matchIndex,
                appliedIndex,
                servingReads
        );
    }

    private static URI ratisEndpoint(String nodeId) {
        var nodeIndex = Integer.parseInt(nodeId.substring(1));
        return URI.create("grpc://10.0.0." + nodeIndex + ":1900" + nodeIndex);
    }

    private static void assertInvalidEnum(String key) {
        var properties = new RatisProcessAdapterMetadataConfig(metadata()).toProperties();
        properties.setProperty(key, "BROKEN");

        var failure = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterMetadataConfig.fromProperties(properties));

        assertEquals("Invalid enum for Ratis process adapter metadata key " + key + ": BROKEN", failure.getMessage());
    }

    private static ReplicatedCdcToken token(long metadataEpoch,
                                            ShardId partitionId,
                                            long partitionEpoch,
                                            long appliedIndex) {
        return new ReplicatedCdcToken(metadataEpoch, partitionId, partitionEpoch, appliedIndex, 0);
    }
}
