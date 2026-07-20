package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandKind;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class MetadataVersionCompatibilityTest {

    @Test
    void metadataCommandKindsRemainAppendOnlyCompatibilitySurface() {
        var kindNames = Arrays.stream(MetadataCommandKind.values())
                .map(Enum::name)
                .toList();

        assertEquals(List.of(
                "REGISTER_NODE",
                "CREATE_COLUMN",
                "DELETE_COLUMN",
                "CREATE_PARTITION",
                "SPLIT_PARTITION",
                "MERGE_PARTITIONS",
                "CHANGE_PARTITION_PEERS",
                "UPDATE_COLUMN_SCHEMA",
                "PREPARE_SCHEMA_CHANGE",
                "ACTIVATE_SCHEMA_CHANGE",
                "FENCE_SCHEMA_CHANGE",
                "RECORD_BACKUP_MANIFEST",
                "RECORD_CDC_TOKEN_TRANSLATION"
        ), kindNames);
    }

    @Test
    void metadataCommandPermitsRemainCoveredByKinds() {
        var commandNames = Arrays.stream(MetadataCommand.class.getPermittedSubclasses())
                .map(Class::getSimpleName)
                .sorted()
                .toList();

        assertEquals(List.of(
                "ActivateSchemaChange",
                "ChangePartitionPeers",
                "CreateColumn",
                "CreatePartition",
                "DeleteColumn",
                "FenceSchemaChange",
                "MergePartitions",
                "PrepareSchemaChange",
                "RecordBackupManifest",
                "RecordCdcTokenTranslation",
                "RegisterNode",
                "SplitPartition",
                "UpdateColumnSchema"
        ), commandNames);
    }

    @Test
    void clusterMetadataRecordSchemaSurfaceRemainsStable() {
        var componentNames = Arrays.stream(ClusterMetadata.class.getRecordComponents())
                .map(RecordComponent::getName)
                .toList();

        assertEquals(List.of(
                "clusterId",
                "epoch",
                "shards",
                "columnsById",
                "partitionsById",
                "nodesById",
                "cdcTokenTranslationsByEpoch",
                "schemaChangesById",
                "backupManifestsById"
        ), componentNames);
    }
}
