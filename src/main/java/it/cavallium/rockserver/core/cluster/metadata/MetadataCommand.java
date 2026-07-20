package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.EndpointUris;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.RockserverKeyEncoder;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.SingleLineStrings;
import it.cavallium.rockserver.core.common.ColumnSchema;

import java.net.URI;
import java.util.HashSet;
import java.util.List;

import org.jetbrains.annotations.Nullable;

public sealed interface MetadataCommand permits MetadataCommand.RegisterNode,
        MetadataCommand.CreateColumn,
        MetadataCommand.DeleteColumn,
        MetadataCommand.CreatePartition,
        MetadataCommand.SplitPartition,
        MetadataCommand.MergePartitions,
        MetadataCommand.ChangePartitionPeers,
        MetadataCommand.UpdateColumnSchema,
        MetadataCommand.PrepareSchemaChange,
        MetadataCommand.ActivateSchemaChange,
        MetadataCommand.FenceSchemaChange,
        MetadataCommand.RecordBackupManifest,
        MetadataCommand.RecordCdcTokenTranslation {

    long expectedMetadataEpoch();

    MetadataCommandKind kind();

    record RegisterNode(long expectedMetadataEpoch,
                        NodeId nodeId,
                        URI publicEndpoint,
                        URI ratisEndpoint,
                        String failureDomain) implements MetadataCommand {

        public RegisterNode {
            requireNonNegative(expectedMetadataEpoch, "expectedMetadataEpoch");
            requireArgument(nodeId, "nodeId");
            publicEndpoint = EndpointUris.requireEndpointUri(publicEndpoint, "publicEndpoint");
            ratisEndpoint = EndpointUris.requireGrpcUri(ratisEndpoint, "ratisEndpoint");
            if (publicEndpoint.equals(ratisEndpoint)) {
                throw new IllegalArgumentException("node public endpoint must be separate from Ratis endpoint");
            }
            SingleLineStrings.requireNonBlank(failureDomain, "failureDomain");
        }

        @Override
        public MetadataCommandKind kind() {
            return MetadataCommandKind.REGISTER_NODE;
        }
    }

    record CreateColumn(long expectedMetadataEpoch,
                        ClusterColumnMetadata column) implements MetadataCommand {

        public CreateColumn {
            requireNonNegative(expectedMetadataEpoch, "expectedMetadataEpoch");
            requireArgument(column, "column");
            rejectLocalMergeOperatorClass(column.schema());
        }

        @Override
        public MetadataCommandKind kind() {
            return MetadataCommandKind.CREATE_COLUMN;
        }
    }

    record DeleteColumn(long expectedMetadataEpoch,
                        long columnId,
                        long expectedSchemaEpoch) implements MetadataCommand {

        public DeleteColumn {
            requireNonNegative(expectedMetadataEpoch, "expectedMetadataEpoch");
            requireNonNegative(columnId, "columnId");
            requireNonNegative(expectedSchemaEpoch, "expectedSchemaEpoch");
        }

        @Override
        public MetadataCommandKind kind() {
            return MetadataCommandKind.DELETE_COLUMN;
        }
    }

    record CreatePartition(long expectedMetadataEpoch,
                           PartitionDescriptor partition) implements MetadataCommand {

        public CreatePartition {
            requireNonNegative(expectedMetadataEpoch, "expectedMetadataEpoch");
            requireArgument(partition, "partition");
        }

        @Override
        public MetadataCommandKind kind() {
            return MetadataCommandKind.CREATE_PARTITION;
        }
    }

    record SplitPartition(long expectedMetadataEpoch,
                          ShardId sourcePartitionId,
                          long expectedSourceEpoch,
                          PartitionDescriptor left,
                          PartitionDescriptor right) implements MetadataCommand {

        public SplitPartition {
            requireNonNegative(expectedMetadataEpoch, "expectedMetadataEpoch");
            requireArgument(sourcePartitionId, "sourcePartitionId");
            requireNonNegative(expectedSourceEpoch, "expectedSourceEpoch");
            requireArgument(left, "left");
            requireArgument(right, "right");
            validateSplitDescriptors(left, right);
        }

        @Override
        public MetadataCommandKind kind() {
            return MetadataCommandKind.SPLIT_PARTITION;
        }
    }

    record MergePartitions(long expectedMetadataEpoch,
                           ShardId leftPartitionId,
                           ShardId rightPartitionId,
                           PartitionDescriptor merged) implements MetadataCommand {

        public MergePartitions {
            requireNonNegative(expectedMetadataEpoch, "expectedMetadataEpoch");
            requireArgument(leftPartitionId, "leftPartitionId");
            requireArgument(rightPartitionId, "rightPartitionId");
            if (leftPartitionId.equals(rightPartitionId)) {
                throw new IllegalArgumentException("merge partition ids must be distinct");
            }
            requireArgument(merged, "merged");
        }

        @Override
        public MetadataCommandKind kind() {
            return MetadataCommandKind.MERGE_PARTITIONS;
        }
    }

    record ChangePartitionPeers(long expectedMetadataEpoch,
                                ShardId partitionId,
                                long expectedPartitionEpoch,
                                List<ReplicaMember> peers,
                                @Nullable NodeId leaderHint) implements MetadataCommand {

        public ChangePartitionPeers {
            requireNonNegative(expectedMetadataEpoch, "expectedMetadataEpoch");
            requireArgument(partitionId, "partitionId");
            requireNonNegative(expectedPartitionEpoch, "expectedPartitionEpoch");
            requireArgument(peers, "peers");
            if (peers.isEmpty()) {
                throw new IllegalArgumentException("partition peer change must include at least one peer");
            }
            peers = copyPeers(peers);
            validateUniquePeerNodeIds(peers);
            validateLeaderHint(leaderHint, peers);
        }

        @Override
        public MetadataCommandKind kind() {
            return MetadataCommandKind.CHANGE_PARTITION_PEERS;
        }
    }

    record UpdateColumnSchema(long expectedMetadataEpoch,
                              long columnId,
                              long expectedSchemaEpoch,
                              long newSchemaEpoch,
                              ColumnSchema schema) implements MetadataCommand {

        public UpdateColumnSchema {
            requireNonNegative(expectedMetadataEpoch, "expectedMetadataEpoch");
            requireNonNegative(columnId, "columnId");
            requireNonNegative(expectedSchemaEpoch, "expectedSchemaEpoch");
            requireNonNegative(newSchemaEpoch, "newSchemaEpoch");
            if (newSchemaEpoch <= expectedSchemaEpoch) {
                throw new IllegalArgumentException("newSchemaEpoch must be greater than expectedSchemaEpoch");
            }
            requireArgument(schema, "schema");
            rejectLocalMergeOperatorClass(schema);
        }

        @Override
        public MetadataCommandKind kind() {
            return MetadataCommandKind.UPDATE_COLUMN_SCHEMA;
        }
    }

    record PrepareSchemaChange(long expectedMetadataEpoch,
                               long schemaChangeId,
                               SchemaChangeKind changeKind,
                               long columnId,
                               long targetSchemaEpoch,
                               @Nullable ClusterColumnMetadata column,
                               @Nullable MergeOperatorActivation mergeOperatorActivation) implements MetadataCommand {

        public PrepareSchemaChange {
            requireNonNegative(expectedMetadataEpoch, "expectedMetadataEpoch");
            requireNonNegative(schemaChangeId, "schemaChangeId");
            requireArgument(changeKind, "changeKind");
            requireNonNegative(columnId, "columnId");
            requireNonNegative(targetSchemaEpoch, "targetSchemaEpoch");
            validatePreparedSchemaChange(changeKind, columnId, targetSchemaEpoch, column, mergeOperatorActivation);
        }

        @Override
        public MetadataCommandKind kind() {
            return MetadataCommandKind.PREPARE_SCHEMA_CHANGE;
        }
    }

    record ActivateSchemaChange(long expectedMetadataEpoch,
                                long schemaChangeId,
                                long columnId,
                                long targetSchemaEpoch) implements MetadataCommand {

        public ActivateSchemaChange {
            requireNonNegative(expectedMetadataEpoch, "expectedMetadataEpoch");
            requireNonNegative(schemaChangeId, "schemaChangeId");
            requireNonNegative(columnId, "columnId");
            requireNonNegative(targetSchemaEpoch, "targetSchemaEpoch");
        }

        @Override
        public MetadataCommandKind kind() {
            return MetadataCommandKind.ACTIVATE_SCHEMA_CHANGE;
        }
    }

    record FenceSchemaChange(long expectedMetadataEpoch,
                             long schemaChangeId,
                             long columnId,
                             long fencedSchemaEpoch) implements MetadataCommand {

        public FenceSchemaChange {
            requireNonNegative(expectedMetadataEpoch, "expectedMetadataEpoch");
            requireNonNegative(schemaChangeId, "schemaChangeId");
            requireNonNegative(columnId, "columnId");
            requireNonNegative(fencedSchemaEpoch, "fencedSchemaEpoch");
        }

        @Override
        public MetadataCommandKind kind() {
            return MetadataCommandKind.FENCE_SCHEMA_CHANGE;
        }
    }

    record RecordBackupManifest(long expectedMetadataEpoch,
                                BackupManifest manifest) implements MetadataCommand {

        public RecordBackupManifest {
            requireNonNegative(expectedMetadataEpoch, "expectedMetadataEpoch");
            requireArgument(manifest, "manifest");
            if (manifest.metadataBackupEpoch() < expectedMetadataEpoch) {
                throw new IllegalArgumentException("metadata backup epoch must fence at or after expected metadata epoch");
            }
            for (BackupManifest.PartitionCut cut : manifest.partitionCuts()) {
                if (cut.observedMetadataBackupEpoch() != manifest.metadataBackupEpoch()) {
                    throw new IllegalArgumentException("partition backup cut must observe metadata backup epoch "
                            + manifest.metadataBackupEpoch());
                }
            }
        }

        @Override
        public MetadataCommandKind kind() {
            return MetadataCommandKind.RECORD_BACKUP_MANIFEST;
        }
    }

    record RecordCdcTokenTranslation(long expectedMetadataEpoch,
                                     CdcTokenTranslation translation) implements MetadataCommand {

        public RecordCdcTokenTranslation {
            requireNonNegative(expectedMetadataEpoch, "expectedMetadataEpoch");
            requireArgument(translation, "translation");
            if (translation.metadataEpoch() != expectedMetadataEpoch) {
                throw new IllegalArgumentException("CDC token translation metadata epoch must match expected "
                        + "metadata epoch");
            }
        }

        @Override
        public MetadataCommandKind kind() {
            return MetadataCommandKind.RECORD_CDC_TOKEN_TRANSLATION;
        }
    }

    private static void validatePreparedSchemaChange(SchemaChangeKind changeKind,
                                                     long columnId,
                                                     long targetSchemaEpoch,
                                                     @Nullable ClusterColumnMetadata column,
                                                     @Nullable MergeOperatorActivation mergeOperatorActivation) {
        switch (changeKind) {
            case CREATE_COLUMN -> {
                requireArgument(column, "column");
                if (column.columnId() != columnId) {
                    throw new IllegalArgumentException("prepared column id must match columnId");
                }
                if (column.schemaEpoch() != targetSchemaEpoch) {
                    throw new IllegalArgumentException("prepared column schema epoch must match targetSchemaEpoch");
                }
                rejectLocalMergeOperatorClass(column.schema());
                if (mergeOperatorActivation != null) {
                    throw new IllegalArgumentException("create-column schema change must not include merge activation");
                }
            }
            case DELETE_COLUMN -> {
                if (column != null || mergeOperatorActivation != null) {
                    throw new IllegalArgumentException("delete-column schema change must not include payload metadata");
                }
            }
            case MERGE_OPERATOR_ACTIVATION -> {
                if (column != null) {
                    throw new IllegalArgumentException("merge-operator activation must not include column metadata");
                }
                requireArgument(mergeOperatorActivation, "mergeOperatorActivation");
            }
        }
    }

    private static List<ReplicaMember> copyPeers(List<ReplicaMember> peers) {
        for (ReplicaMember peer : peers) {
            requireArgument(peer, "peers entry");
        }
        return List.copyOf(peers);
    }

    private static void rejectLocalMergeOperatorClass(ColumnSchema schema) {
        if (schema.mergeOperatorClass() != null) {
            throw new IllegalArgumentException("local mergeOperatorClass is not allowed in replicated metadata; "
                    + "use metadata-activated merge artifacts");
        }
    }

    private static void validateSplitDescriptors(PartitionDescriptor left, PartitionDescriptor right) {
        if (left.partitionId().equals(right.partitionId())) {
            throw new IllegalArgumentException("split partition ids must be distinct");
        }
        if (left.endKeyExclusive() == null || right.startKeyInclusive() == null) {
            throw new IllegalArgumentException("split descriptors must share a concrete split boundary");
        }
        if (RockserverKeyEncoder.compareCalculatedKeys(left.endKeyExclusive(), right.startKeyInclusive()) != 0) {
            throw new IllegalArgumentException("split descriptors must share the same boundary key");
        }
    }

    private static void validateLeaderHint(@Nullable NodeId leaderHint, List<ReplicaMember> peers) {
        if (leaderHint == null) {
            return;
        }
        for (ReplicaMember peer : peers) {
            if (peer.nodeId().equals(leaderHint) && peer.isVoter()) {
                return;
            }
        }
        throw new IllegalArgumentException("leader hint must reference a voting partition peer: " + leaderHint);
    }

    private static void validateUniquePeerNodeIds(List<ReplicaMember> peers) {
        var nodeIds = new HashSet<NodeId>();
        for (ReplicaMember peer : peers) {
            if (!nodeIds.add(peer.nodeId())) {
                throw new IllegalArgumentException("duplicate node in partition peer change: " + peer.nodeId());
            }
        }
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
