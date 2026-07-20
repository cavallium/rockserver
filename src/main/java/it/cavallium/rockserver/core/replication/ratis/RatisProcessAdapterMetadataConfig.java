package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ClusterNodeMetadata;
import it.cavallium.rockserver.core.cluster.EndpointUris;
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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Pattern;

public record RatisProcessAdapterMetadataConfig(ClusterMetadata metadata) {

    private static final String CLUSTER_ID_KEY = "clusterId";
    private static final String EPOCH_KEY = "epoch";
    public static final int MAX_METADATA_CONFIG_BYTES = 64 * 1024 * 1024;
    private static final String METADATA_PAYLOAD_DESCRIPTION = "Ratis process adapter metadata payload";
    private static final Pattern KNOWN_KEY = Pattern.compile(
            "clusterId|epoch"
                    + "|shard\\.\\d+\\.(shardId|term|leader|configIndex"
                    + "|member\\.\\d+\\.(nodeId|endpoint|role|matchIndex|appliedIndex|servingReads))"
                    + "|node\\.\\d+\\.(nodeId|publicEndpoint|ratisEndpoint|failureDomain)"
                    + "|column\\.\\d+\\.(columnId|name|schemaEpoch|schema\\.hasValue"
                    + "|schema\\.mergeOperatorName|schema\\.mergeOperatorVersion|schema\\.mergeOperatorClass"
                    + "|schema\\.fixedKey\\.\\d+|schema\\.variableTailKey\\.\\d+)"
                    + "|partition\\.\\d+\\.(partitionId|startKeyInclusive|endKeyExclusive|epoch|ratisGroupId"
                    + "|leaderHint|splitState|peer\\.\\d+\\.(nodeId|endpoint|role|matchIndex|appliedIndex|servingReads)"
                    + "|placement\\.\\d+\\.(key|value))"
                    + "|cdcTranslation\\.\\d+\\.(kind|metadataEpoch"
                    + "|(sourceEndToken|leftStartToken|rightStartToken|leftEndToken|rightEndToken|mergedStartToken)"
                    + "\\.(metadataEpoch|partitionId|partitionEpoch|appliedIndex|eventOrdinal))"
                    + "|schemaChange\\.\\d+\\.(schemaChangeId|kind|columnId|targetSchemaEpoch|status"
                    + "|column\\.(columnId|name|schemaEpoch|schema\\.hasValue|schema\\.mergeOperatorName"
                    + "|schema\\.mergeOperatorVersion|schema\\.mergeOperatorClass|schema\\.fixedKey\\.\\d+"
                    + "|schema\\.variableTailKey\\.\\d+)"
                    + "|mergeActivation\\.(name|version|artifactHash))"
                    + "|backupManifest\\.\\d+\\.(backupId|metadataBackupEpoch|metadataEpoch"
                    + "|cut\\.\\d+\\.(partitionId|partitionEpoch|ratisTerm|ratisIndex|observedMetadataBackupEpoch))"
    );

    public RatisProcessAdapterMetadataConfig {
        if (metadata == null) {
            throw new IllegalArgumentException("metadata must not be null");
        }
    }

    public static RatisProcessAdapterMetadataConfig read(Path path) throws IOException {
        return fromProperties(readProperties(requirePath(path)));
    }

    static Properties readProperties(Path path) throws IOException {
        return RatisProcessAdapterProperties.read(
                requirePath(path),
                MAX_METADATA_CONFIG_BYTES,
                METADATA_PAYLOAD_DESCRIPTION
        );
    }

    public static RatisProcessAdapterMetadataConfig fromProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("properties must not be null");
        }
        rejectUnknownKeys(properties);
        var metadata = new ClusterMetadata(
                requiredClusterId(properties),
                requiredLong(properties, EPOCH_KEY),
                groups(properties),
                columns(properties),
                partitions(properties),
                nodes(properties),
                cdcTokenTranslations(properties),
                schemaChanges(properties),
                backupManifests(properties)
        );
        return new RatisProcessAdapterMetadataConfig(metadata);
    }

    public Properties toProperties() {
        var properties = new Properties();
        properties.setProperty(CLUSTER_ID_KEY, metadata.clusterId().value().toString());
        properties.setProperty(EPOCH_KEY, Long.toString(metadata.epoch()));
        writeGroups(properties);
        writeNodes(properties);
        writeColumns(properties);
        writePartitions(properties);
        writeCdcTokenTranslations(properties);
        writeSchemaChanges(properties);
        writeBackupManifests(properties);
        return properties;
    }

    public byte[] toBytes() {
        return RatisProcessAdapterProperties.toBytes(
                toProperties(),
                "Rockserver Ratis process adapter metadata",
                MAX_METADATA_CONFIG_BYTES,
                METADATA_PAYLOAD_DESCRIPTION
        );
    }

    public String semanticSha256() {
        return RatisProcessAdapterProperties.semanticSha256(toProperties());
    }

    public static RatisProcessAdapterMetadataConfig fromBytes(byte[] payload) {
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        return fromProperties(RatisProcessAdapterProperties.fromBytes(
                payload,
                MAX_METADATA_CONFIG_BYTES,
                METADATA_PAYLOAD_DESCRIPTION
        ));
    }

    private static Path requirePath(Path path) {
        if (path == null) {
            throw new IllegalArgumentException("path must not be null");
        }
        return path;
    }

    public void write(Path path) throws IOException {
        RatisProcessAdapterFileWriter.writeProperties(
                path,
                toProperties(),
                "Rockserver Ratis process adapter metadata",
                MAX_METADATA_CONFIG_BYTES,
                METADATA_PAYLOAD_DESCRIPTION
        );
    }

    private void writeGroups(Properties properties) {
        var groups = metadata.groups().stream()
                .sorted(Comparator.comparing(ReplicaGroupMetadata::shardId))
                .toList();
        for (int index = 0; index < groups.size(); index++) {
            var group = groups.get(index);
            var prefix = "shard." + index + ".";
            properties.setProperty(prefix + "shardId", group.shardId().toString());
            properties.setProperty(prefix + "term", Long.toString(group.term()));
            if (group.leader() != null) {
                properties.setProperty(prefix + "leader", group.leader().value());
            }
            properties.setProperty(prefix + "configIndex", Long.toString(group.configIndex()));
            writeMembers(properties, prefix, group.members());
        }
    }

    private void writeNodes(Properties properties) {
        var nodes = metadata.nodes().stream()
                .sorted(Comparator.comparing(ClusterNodeMetadata::nodeId))
                .toList();
        for (int index = 0; index < nodes.size(); index++) {
            var node = nodes.get(index);
            var prefix = "node." + index + ".";
            properties.setProperty(prefix + "nodeId", node.nodeId().value());
            properties.setProperty(prefix + "publicEndpoint", node.publicEndpoint().toString());
            properties.setProperty(prefix + "ratisEndpoint", node.ratisEndpoint().toString());
            properties.setProperty(prefix + "failureDomain", node.failureDomain());
        }
    }

    private void writeColumns(Properties properties) {
        var columns = metadata.columnDefinitions().stream()
                .sorted(Comparator.comparingLong(ClusterColumnMetadata::columnId))
                .toList();
        for (int index = 0; index < columns.size(); index++) {
            var column = columns.get(index);
            var prefix = "column." + index + ".";
            properties.setProperty(prefix + "columnId", Long.toString(column.columnId()));
            properties.setProperty(prefix + "name", column.name());
            properties.setProperty(prefix + "schemaEpoch", Long.toString(column.schemaEpoch()));
            writeSchema(properties, prefix + "schema.", column.schema());
        }
    }

    private static void writeSchema(Properties properties, String prefix, ColumnSchema schema) {
        properties.setProperty(prefix + "hasValue", Boolean.toString(schema.hasValue()));
        for (int index = 0; index < schema.fixedLengthKeysCount(); index++) {
            properties.setProperty(prefix + "fixedKey." + index, Integer.toString(schema.key(index)));
        }
        for (int index = 0; index < schema.variableLengthKeysCount(); index++) {
            properties.setProperty(prefix + "variableTailKey." + index,
                    schema.variableTailKey(schema.fixedLengthKeysCount() + index).name());
        }
        if (schema.mergeOperatorName() != null) {
            properties.setProperty(prefix + "mergeOperatorName", schema.mergeOperatorName());
        }
        if (schema.mergeOperatorVersion() != null) {
            properties.setProperty(prefix + "mergeOperatorVersion", Long.toString(schema.mergeOperatorVersion()));
        }
        if (schema.mergeOperatorClass() != null) {
            properties.setProperty(prefix + "mergeOperatorClass", schema.mergeOperatorClass());
        }
    }

    private void writePartitions(Properties properties) {
        var partitions = metadata.partitionDescriptors().stream()
                .sorted(Comparator.comparing(PartitionDescriptor::partitionId))
                .toList();
        for (int index = 0; index < partitions.size(); index++) {
            var partition = partitions.get(index);
            var prefix = "partition." + index + ".";
            properties.setProperty(prefix + "partitionId", partition.partitionId().toString());
            if (partition.startKeyInclusive() != null) {
                properties.setProperty(prefix + "startKeyInclusive", encodeBytes(partition.startKeyInclusive()));
            }
            if (partition.endKeyExclusive() != null) {
                properties.setProperty(prefix + "endKeyExclusive", encodeBytes(partition.endKeyExclusive()));
            }
            properties.setProperty(prefix + "epoch", Long.toString(partition.epoch()));
            properties.setProperty(prefix + "ratisGroupId", partition.ratisGroupId());
            if (partition.leaderHint() != null) {
                properties.setProperty(prefix + "leaderHint", partition.leaderHint().value());
            }
            properties.setProperty(prefix + "splitState", partition.splitState().name());
            writePeers(properties, prefix, partition.peers());
            writePlacement(properties, prefix, partition.placementConstraints());
        }
    }

    private static void writePeers(Properties properties, String partitionPrefix, List<ReplicaMember> peers) {
        for (int index = 0; index < peers.size(); index++) {
            var peer = peers.get(index);
            var prefix = partitionPrefix + "peer." + index + ".";
            writeReplicaMember(properties, prefix, peer);
        }
    }

    private static void writePlacement(Properties properties, String partitionPrefix, Map<String, String> placement) {
        var sorted = new TreeMap<>(placement);
        var index = 0;
        for (var entry : sorted.entrySet()) {
            var prefix = partitionPrefix + "placement." + index + ".";
            properties.setProperty(prefix + "key", entry.getKey());
            properties.setProperty(prefix + "value", entry.getValue());
            index++;
        }
    }

    private static void writeMembers(Properties properties, String groupPrefix, List<ReplicaMember> members) {
        for (int index = 0; index < members.size(); index++) {
            var member = members.get(index);
            var prefix = groupPrefix + "member." + index + ".";
            writeReplicaMember(properties, prefix, member);
        }
    }

    private static void writeReplicaMember(Properties properties, String prefix, ReplicaMember member) {
        properties.setProperty(prefix + "nodeId", member.nodeId().value());
        properties.setProperty(prefix + "endpoint", member.endpoint().toString());
        properties.setProperty(prefix + "role", member.role().name());
        properties.setProperty(prefix + "matchIndex", Long.toString(member.matchIndex()));
        properties.setProperty(prefix + "appliedIndex", Long.toString(member.appliedIndex()));
        properties.setProperty(prefix + "servingReads", Boolean.toString(member.servingReads()));
    }

    private void writeCdcTokenTranslations(Properties properties) {
        var translations = metadata.cdcTokenTranslations().stream()
                .sorted(Comparator.comparingLong(CdcTokenTranslation::metadataEpoch))
                .toList();
        for (int index = 0; index < translations.size(); index++) {
            var translation = translations.get(index);
            var prefix = "cdcTranslation." + index + ".";
            properties.setProperty(prefix + "kind", translation.kind().name());
            properties.setProperty(prefix + "metadataEpoch", Long.toString(translation.metadataEpoch()));
            switch (translation) {
                case CdcTokenTranslation.Split split -> {
                    writeCdcToken(properties, prefix + "sourceEndToken.", split.sourceEndToken());
                    writeCdcToken(properties, prefix + "leftStartToken.", split.leftStartToken());
                    writeCdcToken(properties, prefix + "rightStartToken.", split.rightStartToken());
                }
                case CdcTokenTranslation.Merge merge -> {
                    writeCdcToken(properties, prefix + "leftEndToken.", merge.leftEndToken());
                    writeCdcToken(properties, prefix + "rightEndToken.", merge.rightEndToken());
                    writeCdcToken(properties, prefix + "mergedStartToken.", merge.mergedStartToken());
                }
            }
        }
    }

    private void writeSchemaChanges(Properties properties) {
        var schemaChanges = metadata.schemaChanges().stream()
                .sorted(Comparator.comparingLong(SchemaChangeMetadata::schemaChangeId))
                .toList();
        for (int index = 0; index < schemaChanges.size(); index++) {
            var schemaChange = schemaChanges.get(index);
            var prefix = "schemaChange." + index + ".";
            properties.setProperty(prefix + "schemaChangeId", Long.toString(schemaChange.schemaChangeId()));
            properties.setProperty(prefix + "kind", schemaChange.changeKind().name());
            properties.setProperty(prefix + "columnId", Long.toString(schemaChange.columnId()));
            properties.setProperty(prefix + "targetSchemaEpoch", Long.toString(schemaChange.targetSchemaEpoch()));
            properties.setProperty(prefix + "status", schemaChange.status().name());
            if (schemaChange.column() != null) {
                writeColumn(properties, prefix + "column.", schemaChange.column());
            }
            if (schemaChange.mergeOperatorActivation() != null) {
                writeMergeOperatorActivation(properties, prefix + "mergeActivation.",
                        schemaChange.mergeOperatorActivation());
            }
        }
    }

    private static void writeColumn(Properties properties, String prefix, ClusterColumnMetadata column) {
        properties.setProperty(prefix + "columnId", Long.toString(column.columnId()));
        properties.setProperty(prefix + "name", column.name());
        properties.setProperty(prefix + "schemaEpoch", Long.toString(column.schemaEpoch()));
        writeSchema(properties, prefix + "schema.", column.schema());
    }

    private static void writeMergeOperatorActivation(Properties properties,
                                                     String prefix,
                                                     MergeOperatorActivation activation) {
        properties.setProperty(prefix + "name", activation.name());
        properties.setProperty(prefix + "version", Long.toString(activation.version()));
        properties.setProperty(prefix + "artifactHash", encodeBytes(activation.artifactHash()));
    }

    private void writeBackupManifests(Properties properties) {
        var manifests = metadata.backupManifests().stream()
                .sorted(Comparator.comparing(BackupManifest::backupId))
                .toList();
        for (int index = 0; index < manifests.size(); index++) {
            var manifest = manifests.get(index);
            var prefix = "backupManifest." + index + ".";
            properties.setProperty(prefix + "backupId", manifest.backupId());
            properties.setProperty(prefix + "metadataBackupEpoch", Long.toString(manifest.metadataBackupEpoch()));
            properties.setProperty(prefix + "metadataEpoch", Long.toString(manifest.metadataEpoch()));
            for (int cutIndex = 0; cutIndex < manifest.partitionCuts().size(); cutIndex++) {
                var cut = manifest.partitionCuts().get(cutIndex);
                var cutPrefix = prefix + "cut." + cutIndex + ".";
                properties.setProperty(cutPrefix + "partitionId", cut.partitionId().toString());
                properties.setProperty(cutPrefix + "partitionEpoch", Long.toString(cut.partitionEpoch()));
                properties.setProperty(cutPrefix + "ratisTerm", Long.toString(cut.ratisTerm()));
                properties.setProperty(cutPrefix + "ratisIndex", Long.toString(cut.ratisIndex()));
                properties.setProperty(cutPrefix + "observedMetadataBackupEpoch",
                        Long.toString(cut.observedMetadataBackupEpoch()));
            }
        }
    }

    private static void writeCdcToken(Properties properties, String prefix, ReplicatedCdcToken token) {
        properties.setProperty(prefix + "metadataEpoch", Long.toString(token.metadataEpoch()));
        properties.setProperty(prefix + "partitionId", token.partitionId().toString());
        properties.setProperty(prefix + "partitionEpoch", Long.toString(token.partitionEpoch()));
        properties.setProperty(prefix + "appliedIndex", Long.toString(token.appliedIndex()));
        properties.setProperty(prefix + "eventOrdinal", Integer.toString(token.eventOrdinal()));
    }

    private static Map<NodeId, ClusterNodeMetadata> nodes(Properties properties) {
        var nodes = new LinkedHashMap<NodeId, ClusterNodeMetadata>();
        for (int index = 0; hasIndexedObject(properties, "node.", index, "nodeId"); index++) {
            var prefix = "node." + index + ".";
            var node = new ClusterNodeMetadata(
                    NodeId.of(required(properties, prefix + "nodeId")),
                    requiredEndpointUri(properties, prefix + "publicEndpoint"),
                    requiredGrpcUri(properties, prefix + "ratisEndpoint"),
                    required(properties, prefix + "failureDomain")
            );
            if (nodes.put(node.nodeId(), node) != null) {
                throw new IllegalArgumentException("Duplicate metadata node id: " + node.nodeId());
            }
        }
        rejectIndexedObjectGaps(properties, "node.", nodes.size());
        return Map.copyOf(nodes);
    }

    private static Map<ShardId, ReplicaGroupMetadata> groups(Properties properties) {
        var groups = new LinkedHashMap<ShardId, ReplicaGroupMetadata>();
        for (int index = 0; hasIndexedObject(properties, "shard.", index, "shardId"); index++) {
            var prefix = "shard." + index + ".";
            var group = new ReplicaGroupMetadata(
                    new ShardId(requiredUnsignedLong(properties, prefix + "shardId")),
                    requiredLong(properties, prefix + "term"),
                    optionalNodeId(properties, prefix + "leader"),
                    members(properties, prefix),
                    requiredLong(properties, prefix + "configIndex")
            );
            if (groups.put(group.shardId(), group) != null) {
                throw new IllegalArgumentException("Duplicate metadata shard id: " + group.shardId());
            }
        }
        rejectIndexedObjectGaps(properties, "shard.", groups.size());
        return Map.copyOf(groups);
    }

    private static Map<Long, ClusterColumnMetadata> columns(Properties properties) {
        var columns = new LinkedHashMap<Long, ClusterColumnMetadata>();
        for (int index = 0; hasIndexedObject(properties, "column.", index, "columnId"); index++) {
            var prefix = "column." + index + ".";
            var column = new ClusterColumnMetadata(
                    requiredLong(properties, prefix + "columnId"),
                    required(properties, prefix + "name"),
                    schema(properties, prefix + "schema."),
                    requiredLong(properties, prefix + "schemaEpoch")
            );
            if (columns.put(column.columnId(), column) != null) {
                throw new IllegalArgumentException("Duplicate metadata column id: " + column.columnId());
            }
        }
        rejectIndexedObjectGaps(properties, "column.", columns.size());
        return Map.copyOf(columns);
    }

    private static ColumnSchema schema(Properties properties, String prefix) {
        var fixedKeys = new IntArrayList();
        for (var key : indexedKeys(properties, prefix + "fixedKey.")) {
            fixedKeys.add(requiredInt(properties, key));
        }
        var variableTailKeys = new ObjectArrayList<ColumnHashType>();
        for (var key : indexedKeys(properties, prefix + "variableTailKey.")) {
            variableTailKeys.add(requiredEnum(properties, key, ColumnHashType.class));
        }
        return ColumnSchema.of(
                fixedKeys,
                variableTailKeys,
                requiredBoolean(properties, prefix + "hasValue"),
                optional(properties, prefix + "mergeOperatorName"),
                optionalLong(properties, prefix + "mergeOperatorVersion"),
                optional(properties, prefix + "mergeOperatorClass")
        );
    }

    private static Map<ShardId, PartitionDescriptor> partitions(Properties properties) {
        var partitions = new LinkedHashMap<ShardId, PartitionDescriptor>();
        for (int index = 0; hasIndexedObject(properties, "partition.", index, "partitionId"); index++) {
            var prefix = "partition." + index + ".";
            var partition = new PartitionDescriptor(
                    new ShardId(requiredUnsignedLong(properties, prefix + "partitionId")),
                    optionalBytes(properties, prefix + "startKeyInclusive"),
                    optionalBytes(properties, prefix + "endKeyExclusive"),
                    requiredLong(properties, prefix + "epoch"),
                    required(properties, prefix + "ratisGroupId"),
                    peers(properties, prefix),
                    optionalNodeId(properties, prefix + "leaderHint"),
                    placement(properties, prefix),
                    requiredEnum(properties, prefix + "splitState", PartitionSplitState.class)
            );
            if (partitions.put(partition.partitionId(), partition) != null) {
                throw new IllegalArgumentException("Duplicate metadata partition id: " + partition.partitionId());
            }
        }
        rejectIndexedObjectGaps(properties, "partition.", partitions.size());
        return Map.copyOf(partitions);
    }

    private static Map<Long, CdcTokenTranslation> cdcTokenTranslations(Properties properties) {
        var translations = new LinkedHashMap<Long, CdcTokenTranslation>();
        for (int index = 0; hasIndexedObject(properties, "cdcTranslation.", index, "kind"); index++) {
            var prefix = "cdcTranslation." + index + ".";
            var kind = requiredEnum(properties, prefix + "kind", CdcTokenTranslation.TranslationKind.class);
            var metadataEpoch = requiredLong(properties, prefix + "metadataEpoch");
            var translation = switch (kind) {
                case SPLIT -> new CdcTokenTranslation.Split(
                        metadataEpoch,
                        cdcToken(properties, prefix + "sourceEndToken."),
                        cdcToken(properties, prefix + "leftStartToken."),
                        cdcToken(properties, prefix + "rightStartToken.")
                );
                case MERGE -> new CdcTokenTranslation.Merge(
                        metadataEpoch,
                        cdcToken(properties, prefix + "leftEndToken."),
                        cdcToken(properties, prefix + "rightEndToken."),
                        cdcToken(properties, prefix + "mergedStartToken.")
                );
            };
            if (translations.put(translation.metadataEpoch(), translation) != null) {
                throw new IllegalArgumentException("Duplicate CDC token translation metadata epoch: "
                        + translation.metadataEpoch());
            }
        }
        rejectIndexedObjectGaps(properties, "cdcTranslation.", translations.size());
        return Map.copyOf(translations);
    }

    private static Map<Long, SchemaChangeMetadata> schemaChanges(Properties properties) {
        var schemaChanges = new LinkedHashMap<Long, SchemaChangeMetadata>();
        for (int index = 0; hasIndexedObject(properties, "schemaChange.", index, "schemaChangeId"); index++) {
            var prefix = "schemaChange." + index + ".";
            var schemaChange = new SchemaChangeMetadata(
                    requiredLong(properties, prefix + "schemaChangeId"),
                    requiredEnum(properties, prefix + "kind", SchemaChangeKind.class),
                    requiredLong(properties, prefix + "columnId"),
                    requiredLong(properties, prefix + "targetSchemaEpoch"),
                    optionalColumn(properties, prefix + "column."),
                    optionalMergeOperatorActivation(properties, prefix + "mergeActivation."),
                    requiredEnum(properties, prefix + "status", SchemaChangeStatus.class)
            );
            if (schemaChanges.put(schemaChange.schemaChangeId(), schemaChange) != null) {
                throw new IllegalArgumentException("Duplicate schema change id: " + schemaChange.schemaChangeId());
            }
        }
        rejectIndexedObjectGaps(properties, "schemaChange.", schemaChanges.size());
        return Map.copyOf(schemaChanges);
    }

    private static ClusterColumnMetadata optionalColumn(Properties properties, String prefix) {
        if (!properties.containsKey(prefix + "columnId")) {
            rejectPartialOptionalGroup(properties, prefix);
            return null;
        }
        return new ClusterColumnMetadata(
                requiredLong(properties, prefix + "columnId"),
                required(properties, prefix + "name"),
                schema(properties, prefix + "schema."),
                requiredLong(properties, prefix + "schemaEpoch")
        );
    }

    private static MergeOperatorActivation optionalMergeOperatorActivation(Properties properties, String prefix) {
        if (!properties.containsKey(prefix + "name")) {
            rejectPartialOptionalGroup(properties, prefix);
            return null;
        }
        return new MergeOperatorActivation(
                required(properties, prefix + "name"),
                requiredLong(properties, prefix + "version"),
                requiredBytes(properties, prefix + "artifactHash")
        );
    }

    private static Map<String, BackupManifest> backupManifests(Properties properties) {
        var manifests = new LinkedHashMap<String, BackupManifest>();
        for (int index = 0; hasIndexedObject(properties, "backupManifest.", index, "backupId"); index++) {
            var prefix = "backupManifest." + index + ".";
            var cuts = new ArrayList<BackupManifest.PartitionCut>();
            var cutPrefix = prefix + "cut.";
            for (int cutIndex = 0; hasIndexedObject(properties, cutPrefix, cutIndex, "partitionId"); cutIndex++) {
                var itemPrefix = cutPrefix + cutIndex + ".";
                cuts.add(new BackupManifest.PartitionCut(
                        new ShardId(requiredUnsignedLong(properties, itemPrefix + "partitionId")),
                        requiredLong(properties, itemPrefix + "partitionEpoch"),
                        requiredLong(properties, itemPrefix + "ratisTerm"),
                        requiredLong(properties, itemPrefix + "ratisIndex"),
                        requiredLong(properties, itemPrefix + "observedMetadataBackupEpoch")
                ));
            }
            rejectIndexedObjectGaps(properties, cutPrefix, cuts.size());
            var manifest = new BackupManifest(
                    required(properties, prefix + "backupId"),
                    requiredLong(properties, prefix + "metadataBackupEpoch"),
                    requiredLong(properties, prefix + "metadataEpoch"),
                    cuts
            );
            if (manifests.put(manifest.backupId(), manifest) != null) {
                throw new IllegalArgumentException("Duplicate backup manifest id: " + manifest.backupId());
            }
        }
        rejectIndexedObjectGaps(properties, "backupManifest.", manifests.size());
        return Map.copyOf(manifests);
    }

    private static ReplicatedCdcToken cdcToken(Properties properties, String prefix) {
        return new ReplicatedCdcToken(
                requiredLong(properties, prefix + "metadataEpoch"),
                new ShardId(requiredUnsignedLong(properties, prefix + "partitionId")),
                requiredLong(properties, prefix + "partitionEpoch"),
                requiredLong(properties, prefix + "appliedIndex"),
                requiredInt(properties, prefix + "eventOrdinal")
        );
    }

    private static List<ReplicaMember> peers(Properties properties, String partitionPrefix) {
        var peers = new ArrayList<ReplicaMember>();
        var nodeIds = new HashSet<NodeId>();
        var prefix = partitionPrefix + "peer.";
        for (int index = 0; hasIndexedObject(properties, prefix, index, "nodeId"); index++) {
            var peerPrefix = prefix + index + ".";
            var peer = new ReplicaMember(
                    NodeId.of(required(properties, peerPrefix + "nodeId")),
                    requiredEndpointUri(properties, peerPrefix + "endpoint"),
                    requiredEnum(properties, peerPrefix + "role", ReplicaRole.class),
                    requiredLong(properties, peerPrefix + "matchIndex"),
                    requiredLong(properties, peerPrefix + "appliedIndex"),
                    requiredBoolean(properties, peerPrefix + "servingReads")
            );
            if (!nodeIds.add(peer.nodeId())) {
                throw new IllegalArgumentException("Duplicate partition peer node id: " + peer.nodeId());
            }
            peers.add(peer);
        }
        rejectIndexedObjectGaps(properties, prefix, peers.size());
        return List.copyOf(peers);
    }

    private static List<ReplicaMember> members(Properties properties, String groupPrefix) {
        var members = new ArrayList<ReplicaMember>();
        var nodeIds = new HashSet<NodeId>();
        var prefix = groupPrefix + "member.";
        for (int index = 0; hasIndexedObject(properties, prefix, index, "nodeId"); index++) {
            var memberPrefix = prefix + index + ".";
            var member = new ReplicaMember(
                    NodeId.of(required(properties, memberPrefix + "nodeId")),
                    requiredEndpointUri(properties, memberPrefix + "endpoint"),
                    requiredEnum(properties, memberPrefix + "role", ReplicaRole.class),
                    requiredLong(properties, memberPrefix + "matchIndex"),
                    requiredLong(properties, memberPrefix + "appliedIndex"),
                    requiredBoolean(properties, memberPrefix + "servingReads")
            );
            if (!nodeIds.add(member.nodeId())) {
                throw new IllegalArgumentException("Duplicate metadata group member node id: " + member.nodeId());
            }
            members.add(member);
        }
        rejectIndexedObjectGaps(properties, prefix, members.size());
        return List.copyOf(members);
    }

    private static Map<String, String> placement(Properties properties, String partitionPrefix) {
        var placement = new LinkedHashMap<String, String>();
        var prefix = partitionPrefix + "placement.";
        for (int index = 0; hasIndexedObject(properties, prefix, index, "key"); index++) {
            var placementPrefix = prefix + index + ".";
            var key = required(properties, placementPrefix + "key");
            var value = required(properties, placementPrefix + "value");
            if (placement.put(key, value) != null) {
                throw new IllegalArgumentException("Duplicate partition placement key: " + key);
            }
        }
        rejectIndexedObjectGaps(properties, prefix, placement.size());
        return Map.copyOf(placement);
    }

    private static void rejectPartialOptionalGroup(Properties properties, String prefix) {
        for (var key : properties.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                throw new IllegalArgumentException(
                        "Incomplete optional Ratis process adapter metadata group: " + prefix
                );
            }
        }
    }

    private static List<String> indexedKeys(Properties properties, String prefix) {
        var keys = new TreeMap<Integer, String>();
        for (var key : properties.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                keys.put(index(prefix, key), key);
            }
        }
        var result = new ArrayList<String>();
        for (int expected = 0; expected < keys.size(); expected++) {
            if (!keys.containsKey(expected)) {
                throw new IllegalArgumentException("Non-contiguous metadata config index for prefix " + prefix);
            }
            result.add(keys.get(expected));
        }
        return List.copyOf(result);
    }

    private static boolean hasIndexedObject(Properties properties, String prefix, int index, String sentinel) {
        return properties.containsKey(prefix + index + "." + sentinel);
    }

    private static void rejectIndexedObjectGaps(Properties properties, String prefix, int parsedObjects) {
        for (var key : properties.stringPropertyNames()) {
            if (!key.startsWith(prefix)) {
                continue;
            }
            var remainder = key.substring(prefix.length());
            var separator = remainder.indexOf('.');
            if (separator < 0) {
                continue;
            }
            var index = parseIndex(key, remainder.substring(0, separator));
            if (index >= parsedObjects) {
                throw new IllegalArgumentException("Non-contiguous metadata config index: " + key);
            }
        }
    }

    private static int index(String prefix, String key) {
        var suffix = key.substring(prefix.length());
        return parseIndex(key, suffix);
    }

    private static int parseIndex(String key, String index) {
        try {
            var parsed = Integer.parseInt(index);
            if (parsed < 0) {
                throw new IllegalArgumentException("Negative metadata config index: " + key);
            }
            if (!Integer.toString(parsed).equals(index)) {
                throw new IllegalArgumentException("Non-canonical metadata config index: " + key);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid metadata config index: " + key, e);
        }
    }

    private static void rejectUnknownKeys(Properties properties) {
        for (var key : properties.stringPropertyNames()) {
            if (!KNOWN_KEY.matcher(key).matches()) {
                throw new IllegalArgumentException("Unknown Ratis process adapter metadata key: " + key);
            }
        }
    }

    private static String required(Properties properties, String key) {
        var value = properties.getProperty(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing Ratis process adapter metadata key: " + key);
        }
        if (value.isBlank()) {
            throw new IllegalArgumentException("Ratis process adapter metadata key must not be blank: " + key);
        }
        return value;
    }

    private static ClusterId requiredClusterId(Properties properties) {
        var value = required(properties, CLUSTER_ID_KEY);
        try {
            return new ClusterId(java.util.UUID.fromString(value));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid UUID for Ratis process adapter metadata key "
                    + CLUSTER_ID_KEY + ": " + value, e);
        }
    }

    private static URI requiredGrpcUri(Properties properties, String key) {
        return EndpointUris.requireGrpcUri(
                requiredUri(properties, key),
                "Ratis process adapter metadata key " + key
        );
    }

    private static URI requiredEndpointUri(Properties properties, String key) {
        return EndpointUris.requireEndpointUri(
                requiredUri(properties, key),
                "Ratis process adapter metadata key " + key
        );
    }

    private static URI requiredUri(Properties properties, String key) {
        var value = required(properties, key);
        try {
            return URI.create(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Ratis process adapter metadata key must be a URI: " + key, e);
        }
    }

    private static String optional(Properties properties, String key) {
        var value = properties.getProperty(key);
        return value == null || value.isBlank() ? null : value;
    }

    private static long requiredLong(Properties properties, String key) {
        return parseLong(required(properties, key), key);
    }

    private static int requiredInt(Properties properties, String key) {
        return parseInt(required(properties, key), key);
    }

    private static Long optionalLong(Properties properties, String key) {
        var value = optional(properties, key);
        return value == null ? null : parseLong(value, key);
    }

    private static long requiredUnsignedLong(Properties properties, String key) {
        return parseUnsignedLong(required(properties, key), key);
    }

    private static long parseLong(String value, String key) {
        try {
            var parsed = Long.parseLong(value);
            if (!Long.toString(parsed).equals(value)) {
                throw new IllegalArgumentException("Non-canonical long for Ratis process adapter metadata key "
                        + key + ": " + value);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long for Ratis process adapter metadata key "
                    + key + ": " + value, e);
        }
    }

    private static long parseUnsignedLong(String value, String key) {
        try {
            var parsed = Long.parseUnsignedLong(value);
            if (!Long.toUnsignedString(parsed).equals(value)) {
                throw new IllegalArgumentException("Non-canonical unsigned long for Ratis process adapter metadata key "
                        + key + ": " + value);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid unsigned long for Ratis process adapter metadata key "
                    + key + ": " + value, e);
        }
    }

    private static int parseInt(String value, String key) {
        try {
            var parsed = Integer.parseInt(value);
            if (!Integer.toString(parsed).equals(value)) {
                throw new IllegalArgumentException("Non-canonical integer for Ratis process adapter metadata key "
                        + key + ": " + value);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer for Ratis process adapter metadata key "
                    + key + ": " + value, e);
        }
    }

    private static boolean requiredBoolean(Properties properties, String key) {
        var value = required(properties, key);
        if ("true".equals(value)) {
            return true;
        }
        if ("false".equals(value)) {
            return false;
        }
        throw new IllegalArgumentException("Invalid boolean for Ratis process adapter metadata key "
                + key + ": " + value);
    }

    private static <E extends Enum<E>> E requiredEnum(Properties properties, String key, Class<E> enumType) {
        var value = required(properties, key);
        try {
            return Enum.valueOf(enumType, value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid enum for Ratis process adapter metadata key "
                    + key + ": " + value, e);
        }
    }

    private static NodeId optionalNodeId(Properties properties, String key) {
        var value = optional(properties, key);
        return value == null ? null : NodeId.of(value);
    }

    private static Buf optionalBytes(Properties properties, String key) {
        var value = optional(properties, key);
        return value == null ? null : decodeBytes(value, key);
    }

    private static Buf requiredBytes(Properties properties, String key) {
        return decodeBytes(required(properties, key), key);
    }

    private static Buf decodeBytes(String value, String key) {
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Base64 for Ratis process adapter metadata key "
                    + key + ": " + value, e);
        }
        if (!Base64.getEncoder().encodeToString(decoded).equals(value)) {
            throw new IllegalArgumentException("Non-canonical Base64 for Ratis process adapter metadata key "
                    + key + ": " + value);
        }
        return Buf.wrap(decoded).freeze();
    }

    private static String encodeBytes(Buf value) {
        return Base64.getEncoder().encodeToString(value.toByteArray());
    }
}
