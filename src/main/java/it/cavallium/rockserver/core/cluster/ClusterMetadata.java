package it.cavallium.rockserver.core.cluster;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcToken;
import it.cavallium.rockserver.core.cluster.metadata.BackupManifest;
import it.cavallium.rockserver.core.cluster.metadata.CdcTokenTranslation;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

public record ClusterMetadata(ClusterId clusterId,
                              long epoch,
                              Map<ShardId, ReplicaGroupMetadata> shards,
                              Map<Long, ClusterColumnMetadata> columnsById,
                              Map<ShardId, PartitionDescriptor> partitionsById,
                              Map<NodeId, ClusterNodeMetadata> nodesById,
                              Map<Long, CdcTokenTranslation> cdcTokenTranslationsByEpoch,
                              Map<Long, SchemaChangeMetadata> schemaChangesById,
                              Map<String, BackupManifest> backupManifestsById) {

    public ClusterMetadata(ClusterId clusterId,
                           long epoch,
                           Map<ShardId, ReplicaGroupMetadata> shards) {
        this(clusterId, epoch, shards, Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
    }

    public ClusterMetadata(ClusterId clusterId,
                           long epoch,
                           Map<ShardId, ReplicaGroupMetadata> shards,
                           Map<Long, ClusterColumnMetadata> columnsById) {
        this(clusterId, epoch, shards, columnsById, Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
    }

    public ClusterMetadata(ClusterId clusterId,
                           long epoch,
                           Map<ShardId, ReplicaGroupMetadata> shards,
                           Map<Long, ClusterColumnMetadata> columnsById,
                           Map<ShardId, PartitionDescriptor> partitionsById) {
        this(clusterId, epoch, shards, columnsById, partitionsById, Map.of(), Map.of(), Map.of(), Map.of());
    }

    public ClusterMetadata(ClusterId clusterId,
                           long epoch,
                           Map<ShardId, ReplicaGroupMetadata> shards,
                           Map<Long, ClusterColumnMetadata> columnsById,
                           Map<ShardId, PartitionDescriptor> partitionsById,
                           Map<NodeId, ClusterNodeMetadata> nodesById) {
        this(clusterId, epoch, shards, columnsById, partitionsById, nodesById, Map.of(), Map.of(), Map.of());
    }

    public ClusterMetadata(ClusterId clusterId,
                           long epoch,
                           Map<ShardId, ReplicaGroupMetadata> shards,
                           Map<Long, ClusterColumnMetadata> columnsById,
                           Map<ShardId, PartitionDescriptor> partitionsById,
                           Map<NodeId, ClusterNodeMetadata> nodesById,
                           Map<Long, CdcTokenTranslation> cdcTokenTranslationsByEpoch) {
        this(clusterId, epoch, shards, columnsById, partitionsById, nodesById, cdcTokenTranslationsByEpoch, Map.of(),
                Map.of());
    }

    public ClusterMetadata {
        requireArgument(clusterId, "clusterId");
        requireArgument(shards, "shards");
        requireArgument(columnsById, "columnsById");
        requireArgument(partitionsById, "partitionsById");
        requireArgument(nodesById, "nodesById");
        requireArgument(cdcTokenTranslationsByEpoch, "cdcTokenTranslationsByEpoch");
        requireArgument(schemaChangesById, "schemaChangesById");
        requireArgument(backupManifestsById, "backupManifestsById");
        if (epoch < 0) {
            throw new IllegalArgumentException("epoch must be >= 0");
        }
        shards = Map.copyOf(shards);
        columnsById = Map.copyOf(columnsById);
        partitionsById = Map.copyOf(partitionsById);
        nodesById = Map.copyOf(nodesById);
        cdcTokenTranslationsByEpoch = Map.copyOf(cdcTokenTranslationsByEpoch);
        schemaChangesById = Map.copyOf(schemaChangesById);
        backupManifestsById = Map.copyOf(backupManifestsById);
        for (var entry : nodesById.entrySet()) {
            if (!entry.getKey().equals(entry.getValue().nodeId())) {
                throw new IllegalArgumentException("node map key does not match node id: " + entry.getKey());
            }
        }
        for (var entry : shards.entrySet()) {
            if (!entry.getKey().equals(entry.getValue().shardId())) {
                throw new IllegalArgumentException("shard map key does not match group shard id: " + entry.getKey());
            }
            validateRegisteredMemberNodes(entry.getKey(), entry.getValue().members(), nodesById,
                    "replica group member");
        }
        var partitionRatisGroupIds = new HashSet<String>();
        for (var entry : partitionsById.entrySet()) {
            if (!entry.getKey().equals(entry.getValue().partitionId())) {
                throw new IllegalArgumentException("partition map key does not match partition id: "
                        + entry.getKey());
            }
            if (!partitionRatisGroupIds.add(entry.getValue().ratisGroupId())) {
                throw new IllegalArgumentException("duplicate partition Ratis group id in cluster metadata: "
                        + entry.getValue().ratisGroupId());
            }
            validateRegisteredMemberNodes(entry.getKey(), entry.getValue().peers(), nodesById,
                    "partition peer");
            var routeGroup = shards.get(entry.getKey());
            if (routeGroup != null) {
                validatePartitionRouteGroup(entry.getKey(), entry.getValue(), routeGroup);
            }
        }
        validateNonOverlappingPartitionRanges(partitionsById.values());
        var columnNames = new HashSet<String>();
        for (var entry : columnsById.entrySet()) {
            if (entry.getKey() != entry.getValue().columnId()) {
                throw new IllegalArgumentException("column map key does not match column id: " + entry.getKey());
            }
            if (!columnNames.add(entry.getValue().name())) {
                throw new IllegalArgumentException("duplicate column name in cluster metadata: "
                        + entry.getValue().name());
            }
        }
        for (var entry : cdcTokenTranslationsByEpoch.entrySet()) {
            if (entry.getKey() != entry.getValue().metadataEpoch()) {
                throw new IllegalArgumentException("CDC token translation map key does not match metadata epoch: "
                        + entry.getKey());
            }
            if (entry.getValue().metadataEpoch() > epoch) {
                throw new IllegalArgumentException("CDC token translation metadata epoch is newer than cluster "
                        + "metadata epoch: " + entry.getValue().metadataEpoch());
            }
            if (entry.getValue().metadataEpoch() == epoch) {
                validateCurrentCdcTokenTranslation(entry.getValue(), partitionsById);
            }
        }
        for (var entry : schemaChangesById.entrySet()) {
            if (entry.getKey() != entry.getValue().schemaChangeId()) {
                throw new IllegalArgumentException("schema change map key does not match schema change id: "
                        + entry.getKey());
            }
        }
        for (var entry : backupManifestsById.entrySet()) {
            if (!entry.getKey().equals(entry.getValue().backupId())) {
                throw new IllegalArgumentException("backup manifest map key does not match backup id: "
                        + entry.getKey());
            }
            if (entry.getValue().metadataEpoch() > epoch) {
                throw new IllegalArgumentException("backup manifest metadata epoch is newer than cluster metadata "
                        + "epoch: " + entry.getValue().metadataEpoch());
            }
            if (entry.getValue().metadataEpoch() == epoch) {
                validateCurrentBackupManifestCuts(entry.getValue(), partitionsById);
            }
        }
    }

    private static void validatePartitionRouteGroup(ShardId partitionId,
                                                    PartitionDescriptor partition,
                                                    ReplicaGroupMetadata routeGroup) {
        if (!partition.peers().equals(routeGroup.members())) {
            throw new IllegalArgumentException("partition route group peers do not match partition peers: "
                    + partitionId);
        }
        if (!sameValue(partition.leaderHint(), routeGroup.leader())) {
            throw new IllegalArgumentException("partition route group leader does not match partition leader hint: "
                    + partitionId);
        }
    }

    private static void validateRegisteredMemberNodes(ShardId shardId,
                                                      Collection<ReplicaMember> members,
                                                      Map<NodeId, ClusterNodeMetadata> nodesById,
                                                      String memberDescription) {
        if (nodesById.isEmpty()) {
            return;
        }
        for (ReplicaMember member : members) {
            var node = nodesById.get(member.nodeId());
            if (node == null) {
                throw new IllegalArgumentException(memberDescription + " references unknown node "
                        + member.nodeId() + ": " + shardId);
            }
            if (!member.endpoint().equals(node.ratisEndpoint())) {
                throw new IllegalArgumentException(memberDescription + " endpoint does not match registered node "
                        + "Ratis endpoint for " + member.nodeId() + ": " + shardId);
            }
        }
    }

    private static void validateNonOverlappingPartitionRanges(Collection<PartitionDescriptor> partitions) {
        var sorted = new ArrayList<>(partitions);
        sorted.sort((left, right) -> {
            var result = compareNullableLowerBound(left.startKeyInclusive(), right.startKeyInclusive());
            if (result != 0) {
                return result;
            }
            return left.partitionId().compareTo(right.partitionId());
        });
        for (int index = 1; index < sorted.size(); index++) {
            var previous = sorted.get(index - 1);
            var current = sorted.get(index);
            if (lowerLessThanUpper(current.startKeyInclusive(), previous.endKeyExclusive())) {
                throw new IllegalArgumentException("partition ranges overlap: "
                        + previous.partitionId() + " and " + current.partitionId());
            }
        }
    }

    private static int compareNullableLowerBound(Buf left, Buf right) {
        if (left == null) {
            return right == null ? 0 : -1;
        }
        if (right == null) {
            return 1;
        }
        return RockserverKeyEncoder.compareCalculatedKeys(left, right);
    }

    private static boolean lowerLessThanUpper(Buf lowerInclusive, Buf upperExclusive) {
        return lowerInclusive == null
                || upperExclusive == null
                || RockserverKeyEncoder.compareCalculatedKeys(lowerInclusive, upperExclusive) < 0;
    }

    private static void validateCurrentBackupManifestCuts(BackupManifest manifest,
                                                          Map<ShardId, PartitionDescriptor> partitionsById) {
        for (BackupManifest.PartitionCut cut : manifest.partitionCuts()) {
            var partition = partitionsById.get(cut.partitionId());
            if (partition == null) {
                throw new IllegalArgumentException("current backup manifest cut references unknown partition: "
                        + cut.partitionId());
            }
            if (partition.epoch() != cut.partitionEpoch()) {
                throw new IllegalArgumentException("current backup manifest cut partition epoch does not match "
                        + "metadata partition: " + cut.partitionId());
            }
        }
    }

    private static void validateCurrentCdcTokenTranslation(CdcTokenTranslation translation,
                                                           Map<ShardId, PartitionDescriptor> partitionsById) {
        switch (translation) {
            case CdcTokenTranslation.Split split -> {
                validateCurrentCdcToken(split.leftStartToken(), partitionsById, "leftStartToken");
                validateCurrentCdcToken(split.rightStartToken(), partitionsById, "rightStartToken");
            }
            case CdcTokenTranslation.Merge merge ->
                    validateCurrentCdcToken(merge.mergedStartToken(), partitionsById, "mergedStartToken");
        }
    }

    private static void validateCurrentCdcToken(ReplicatedCdcToken token,
                                                Map<ShardId, PartitionDescriptor> partitionsById,
                                                String tokenName) {
        var partition = partitionsById.get(token.partitionId());
        if (partition == null) {
            throw new IllegalArgumentException("current CDC token translation " + tokenName
                    + " references unknown partition: " + token.partitionId());
        }
        if (partition.epoch() != token.partitionEpoch()) {
            throw new IllegalArgumentException("current CDC token translation " + tokenName
                    + " partition epoch does not match metadata partition: " + token.partitionId());
        }
    }

    public Optional<ReplicaGroupMetadata> findGroup(ShardId shardId) {
        requireArgument(shardId, "shardId");
        return Optional.ofNullable(shards.get(shardId));
    }

    public ReplicaGroupMetadata requireGroup(ShardId shardId) {
        requireArgument(shardId, "shardId");
        var group = shards.get(shardId);
        if (group == null) {
            throw new IllegalArgumentException("unknown shard: " + shardId);
        }
        return group;
    }

    public Collection<ReplicaGroupMetadata> groups() {
        return shards.values();
    }

    public Optional<ClusterColumnMetadata> findColumn(long columnId) {
        return Optional.ofNullable(columnsById.get(columnId));
    }

    public Optional<ClusterColumnMetadata> findColumn(String name) {
        requireArgument(name, "name");
        return columnsById.values().stream()
                .filter(column -> column.name().equals(name))
                .findFirst();
    }

    public ClusterColumnMetadata requireColumn(long columnId) {
        var column = columnsById.get(columnId);
        if (column == null) {
            throw new IllegalArgumentException("unknown column: " + columnId);
        }
        return column;
    }

    public Collection<ClusterColumnMetadata> columnDefinitions() {
        return columnsById.values();
    }

    public Optional<PartitionDescriptor> findPartition(ShardId partitionId) {
        requireArgument(partitionId, "partitionId");
        return Optional.ofNullable(partitionsById.get(partitionId));
    }

    public PartitionDescriptor requirePartition(ShardId partitionId) {
        requireArgument(partitionId, "partitionId");
        var partition = partitionsById.get(partitionId);
        if (partition == null) {
            throw new IllegalArgumentException("unknown partition: " + partitionId);
        }
        return partition;
    }

    public Collection<PartitionDescriptor> partitionDescriptors() {
        return partitionsById.values();
    }

    @SuppressWarnings("unused")
    public Optional<CdcTokenTranslation> findCdcTokenTranslation(long metadataEpoch) {
        return Optional.ofNullable(cdcTokenTranslationsByEpoch.get(metadataEpoch));
    }

    public CdcTokenTranslation requireCdcTokenTranslation(long metadataEpoch) {
        var translation = cdcTokenTranslationsByEpoch.get(metadataEpoch);
        if (translation == null) {
            throw new IllegalArgumentException("unknown CDC token translation metadata epoch: " + metadataEpoch);
        }
        return translation;
    }

    public Collection<CdcTokenTranslation> cdcTokenTranslations() {
        return cdcTokenTranslationsByEpoch.values();
    }

    public Optional<SchemaChangeMetadata> findSchemaChange(long schemaChangeId) {
        return Optional.ofNullable(schemaChangesById.get(schemaChangeId));
    }

    public SchemaChangeMetadata requireSchemaChange(long schemaChangeId) {
        var schemaChange = schemaChangesById.get(schemaChangeId);
        if (schemaChange == null) {
            throw new IllegalArgumentException("unknown schema change: " + schemaChangeId);
        }
        return schemaChange;
    }

    public Collection<SchemaChangeMetadata> schemaChanges() {
        return schemaChangesById.values();
    }

    public Optional<BackupManifest> findBackupManifest(String backupId) {
        requireArgument(backupId, "backupId");
        return Optional.ofNullable(backupManifestsById.get(backupId));
    }

    public BackupManifest requireBackupManifest(String backupId) {
        requireArgument(backupId, "backupId");
        var manifest = backupManifestsById.get(backupId);
        if (manifest == null) {
            throw new IllegalArgumentException("unknown backup manifest: " + backupId);
        }
        return manifest;
    }

    public Collection<BackupManifest> backupManifests() {
        return backupManifestsById.values();
    }

    public Optional<ClusterNodeMetadata> findNode(NodeId nodeId) {
        requireArgument(nodeId, "nodeId");
        return Optional.ofNullable(nodesById.get(nodeId));
    }

    public ClusterNodeMetadata requireNode(NodeId nodeId) {
        requireArgument(nodeId, "nodeId");
        var node = nodesById.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("unknown node: " + nodeId);
        }
        return node;
    }

    public Collection<ClusterNodeMetadata> nodes() {
        return nodesById.values();
    }

    private static boolean sameValue(Object left, Object right) {
        return left == null ? right == null : left.equals(right);
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
