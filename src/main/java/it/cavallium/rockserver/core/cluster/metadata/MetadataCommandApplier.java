package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ClusterNodeMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaGroupMetadata;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.RockserverKeyEncoder;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcToken;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class MetadataCommandApplier {

    private MetadataCommandApplier() {
    }

    public static ClusterMetadata apply(ClusterMetadata metadata, MetadataCommand command) {
        requireArgument(metadata, "metadata");
        requireArgument(command, "command");
        requireExpectedEpoch(metadata, command);
        return switch (command) {
            case MetadataCommand.RegisterNode registerNode -> registerNode(metadata, registerNode);
            case MetadataCommand.CreateColumn createColumn -> createColumn(metadata, createColumn);
            case MetadataCommand.DeleteColumn deleteColumn -> deleteColumn(metadata, deleteColumn);
            case MetadataCommand.CreatePartition createPartition -> createPartition(metadata, createPartition);
            case MetadataCommand.ChangePartitionPeers changePeers -> changePartitionPeers(metadata, changePeers);
            case MetadataCommand.UpdateColumnSchema _ -> unsupportedColumnSchemaUpdate();
            case MetadataCommand.SplitPartition splitPartition -> splitPartition(metadata, splitPartition);
            case MetadataCommand.MergePartitions mergePartitions -> mergePartitions(metadata, mergePartitions);
            case MetadataCommand.PrepareSchemaChange prepareSchemaChange -> prepareSchemaChange(metadata,
                    prepareSchemaChange);
            case MetadataCommand.ActivateSchemaChange activateSchemaChange -> activateSchemaChange(metadata,
                    activateSchemaChange);
            case MetadataCommand.FenceSchemaChange fenceSchemaChange -> fenceSchemaChange(metadata,
                    fenceSchemaChange);
            case MetadataCommand.RecordBackupManifest recordBackupManifest -> recordBackupManifest(metadata,
                    recordBackupManifest);
            case MetadataCommand.RecordCdcTokenTranslation recordCdcTokenTranslation ->
                    recordCdcTokenTranslation(metadata, recordCdcTokenTranslation);
        };
    }

    private static ClusterMetadata registerNode(ClusterMetadata metadata, MetadataCommand.RegisterNode command) {
        var node = new ClusterNodeMetadata(
                command.nodeId(),
                command.publicEndpoint(),
                command.ratisEndpoint(),
                command.failureDomain()
        );
        var nodes = copy(metadata.nodesById());
        requireAbsent(nodes, node.nodeId(), "node");
        nodes.put(node.nodeId(), node);
        return withNodes(metadata, nodes);
    }

    private static ClusterMetadata createColumn(ClusterMetadata metadata, MetadataCommand.CreateColumn command) {
        var column = command.column();
        var columns = copy(metadata.columnsById());
        requireAbsent(columns, column.columnId(), "column");
        columns.put(column.columnId(), column);
        return withColumns(metadata, columns);
    }

    private static ClusterMetadata deleteColumn(ClusterMetadata metadata, MetadataCommand.DeleteColumn command) {
        var existing = metadata.requireColumn(command.columnId());
        if (existing.schemaEpoch() != command.expectedSchemaEpoch()) {
            throw new IllegalArgumentException("column schema epoch does not match expectedSchemaEpoch");
        }
        var columns = copy(metadata.columnsById());
        columns.remove(command.columnId());
        return withColumns(metadata, columns);
    }

    private static ClusterMetadata createPartition(ClusterMetadata metadata,
                                                   MetadataCommand.CreatePartition command) {
        var partition = command.partition();
        if (partition.splitState() != PartitionSplitState.ACTIVE) {
            throw new IllegalArgumentException("created partition must be active");
        }
        var partitions = copy(metadata.partitionsById());
        requireAbsent(partitions, partition.partitionId(), "partition");
        var groups = copy(metadata.shards());
        requireAbsent(groups, partition.partitionId(), "partition route group");
        partitions.put(partition.partitionId(), partition);
        groups.put(partition.partitionId(), routeGroup(partition));
        return withPartitionsAndGroups(metadata, partitions, groups);
    }

    private static ClusterMetadata changePartitionPeers(ClusterMetadata metadata,
                                                        MetadataCommand.ChangePartitionPeers command) {
        var existing = metadata.requirePartition(command.partitionId());
        var existingGroup = metadata.requireGroup(command.partitionId());
        if (existing.epoch() != command.expectedPartitionEpoch()) {
            throw new IllegalArgumentException("partition epoch does not match expectedPartitionEpoch");
        }
        requireValidPeerTransition(existing, command.peers(), command.leaderHint());
        var replacement = new PartitionDescriptor(
                existing.partitionId(),
                existing.startKeyInclusive(),
                existing.endKeyExclusive(),
                existing.epoch() + 1,
                existing.ratisGroupId(),
                command.peers(),
                command.leaderHint(),
                existing.placementConstraints(),
                existing.splitState()
        );
        var partitions = copy(metadata.partitionsById());
        partitions.put(command.partitionId(), replacement);
        var groups = copy(metadata.shards());
        groups.put(command.partitionId(), new ReplicaGroupMetadata(
                existingGroup.shardId(),
                existingGroup.term(),
                command.leaderHint(),
                command.peers(),
                existingGroup.configIndex() + 1
        ));
        return withPartitionsAndGroups(metadata, partitions, groups);
    }

    private static ClusterMetadata splitPartition(ClusterMetadata metadata,
                                                  MetadataCommand.SplitPartition command) {
        var source = metadata.requirePartition(command.sourcePartitionId());
        metadata.requireGroup(command.sourcePartitionId());
        if (source.epoch() != command.expectedSourceEpoch()) {
            throw new IllegalArgumentException("source partition epoch does not match expectedSourceEpoch");
        }
        if (source.splitState() != PartitionSplitState.ACTIVE) {
            throw new IllegalStateException("source partition must be active before split");
        }
        if (command.left().partitionId().equals(command.sourcePartitionId())
                || command.right().partitionId().equals(command.sourcePartitionId())) {
            throw new IllegalArgumentException("split child partition id must not reuse source partition id");
        }
        requireSameBoundary(source.startKeyInclusive(), command.left().startKeyInclusive(), "left split start");
        requireSameBoundary(source.endKeyExclusive(), command.right().endKeyExclusive(), "right split end");
        requireAdvancingPartitionEpoch(command.left(), source.epoch(), "left split partition");
        requireAdvancingPartitionEpoch(command.right(), source.epoch(), "right split partition");
        requireSameTopology(source, command.left(), "left split partition");
        requireSameTopology(source, command.right(), "right split partition");

        var partitions = copy(metadata.partitionsById());
        partitions.remove(command.sourcePartitionId());
        requireAbsent(partitions, command.left().partitionId(), "left split partition");
        requireAbsent(partitions, command.right().partitionId(), "right split partition");
        partitions.put(command.left().partitionId(), command.left());
        partitions.put(command.right().partitionId(), command.right());

        var groups = copy(metadata.shards());
        groups.remove(command.sourcePartitionId());
        requireAbsent(groups, command.left().partitionId(), "left split route group");
        requireAbsent(groups, command.right().partitionId(), "right split route group");
        groups.put(command.left().partitionId(), routeGroup(command.left()));
        groups.put(command.right().partitionId(), routeGroup(command.right()));
        return withPartitionsAndGroups(metadata, partitions, groups);
    }

    private static ClusterMetadata mergePartitions(ClusterMetadata metadata,
                                                   MetadataCommand.MergePartitions command) {
        var left = metadata.requirePartition(command.leftPartitionId());
        var right = metadata.requirePartition(command.rightPartitionId());
        metadata.requireGroup(command.leftPartitionId());
        metadata.requireGroup(command.rightPartitionId());
        if (left.splitState() != PartitionSplitState.ACTIVE || right.splitState() != PartitionSplitState.ACTIVE) {
            throw new IllegalStateException("merge source partitions must be active");
        }
        if (command.merged().partitionId().equals(command.leftPartitionId())
                || command.merged().partitionId().equals(command.rightPartitionId())) {
            throw new IllegalArgumentException("merged partition id must not reuse source partition id");
        }
        requireAdjacent(left, right);
        requireSameBoundary(left.startKeyInclusive(), command.merged().startKeyInclusive(), "merged start");
        requireSameBoundary(right.endKeyExclusive(), command.merged().endKeyExclusive(), "merged end");
        requireAdvancingPartitionEpoch(command.merged(), Math.max(left.epoch(), right.epoch()),
                "merged partition");
        requireSameTopology(left, right, "merge parents");
        requireSameTopology(left, command.merged(), "merged partition");

        var partitions = copy(metadata.partitionsById());
        partitions.remove(command.leftPartitionId());
        partitions.remove(command.rightPartitionId());
        requireAbsent(partitions, command.merged().partitionId(), "merged partition");
        partitions.put(command.merged().partitionId(), command.merged());

        var groups = copy(metadata.shards());
        groups.remove(command.leftPartitionId());
        groups.remove(command.rightPartitionId());
        requireAbsent(groups, command.merged().partitionId(), "merged route group");
        groups.put(command.merged().partitionId(), routeGroup(command.merged()));
        return withPartitionsAndGroups(metadata, partitions, groups);
    }

    private static ClusterMetadata unsupportedColumnSchemaUpdate() {
        throw new IllegalArgumentException(
                "direct column schema updates are unsupported; use schema-change prepare/activate/fence");
    }

    private static ClusterMetadata prepareSchemaChange(ClusterMetadata metadata,
                                                       MetadataCommand.PrepareSchemaChange command) {
        validatePreparedSchemaChangeTarget(metadata, command);
        var schemaChanges = copy(metadata.schemaChangesById());
        requireAbsent(schemaChanges, command.schemaChangeId(), "schema change");
        schemaChanges.put(command.schemaChangeId(), SchemaChangeMetadata.prepared(command));
        return withSchemaChanges(metadata, schemaChanges);
    }

    private static ClusterMetadata activateSchemaChange(ClusterMetadata metadata,
                                                        MetadataCommand.ActivateSchemaChange command) {
        var prepared = requireSchemaChange(metadata, command.schemaChangeId(), command.columnId(),
                command.targetSchemaEpoch(), SchemaChangeStatus.PREPARED);
        var columns = copy(metadata.columnsById());
        switch (prepared.changeKind()) {
            case CREATE_COLUMN -> {
                var column = requireState(prepared.column(), "prepared column");
                requireAbsent(columns, column.columnId(), "column");
                columns.put(column.columnId(), column);
            }
            case DELETE_COLUMN -> {
                var existing = metadata.requireColumn(command.columnId());
                if (existing.schemaEpoch() != command.targetSchemaEpoch()) {
                    throw new IllegalArgumentException("column schema epoch does not match schema change target");
                }
                columns.remove(command.columnId());
            }
            case MERGE_OPERATOR_ACTIVATION -> {
                var existing = metadata.requireColumn(command.columnId());
                if (command.targetSchemaEpoch() <= existing.schemaEpoch()) {
                    throw new IllegalArgumentException("merge operator activation schema epoch must advance");
                }
                var activation = requireState(prepared.mergeOperatorActivation(), "mergeOperatorActivation");
                columns.put(command.columnId(), new ClusterColumnMetadata(
                        existing.columnId(),
                        existing.name(),
                        withMergeOperator(existing.schema(), activation),
                        command.targetSchemaEpoch()
                ));
            }
        }
        var schemaChanges = copy(metadata.schemaChangesById());
        schemaChanges.put(prepared.schemaChangeId(), prepared.withStatus(SchemaChangeStatus.ACTIVATED));
        return withColumnsAndSchemaChanges(metadata, columns, schemaChanges);
    }

    private static ClusterMetadata fenceSchemaChange(ClusterMetadata metadata,
                                                     MetadataCommand.FenceSchemaChange command) {
        var schemaChange = requireSchemaChange(metadata, command.schemaChangeId(), command.columnId(),
                command.fencedSchemaEpoch(), SchemaChangeStatus.ACTIVATED);
        var schemaChanges = copy(metadata.schemaChangesById());
        schemaChanges.put(schemaChange.schemaChangeId(), schemaChange.withStatus(SchemaChangeStatus.FENCED));
        return withSchemaChanges(metadata, schemaChanges);
    }

    private static ClusterMetadata recordBackupManifest(ClusterMetadata metadata,
                                                        MetadataCommand.RecordBackupManifest command) {
        var manifest = command.manifest();
        if (manifest.metadataEpoch() != metadata.epoch()) {
            throw new IllegalArgumentException("backup manifest metadata epoch must match current metadata epoch");
        }
        if (manifest.metadataBackupEpoch() != metadata.epoch() + 1) {
            throw new IllegalArgumentException("backup manifest metadata backup epoch must equal next metadata epoch");
        }
        var uncoveredPartitions = copy(metadata.partitionsById());
        for (var cut : manifest.partitionCuts()) {
            var partition = uncoveredPartitions.remove(cut.partitionId());
            if (partition == null) {
                throw new IllegalArgumentException("backup cut references unknown partition: " + cut.partitionId());
            }
            if (partition.epoch() != cut.partitionEpoch()) {
                throw new IllegalArgumentException("backup cut partition epoch does not match metadata partition");
            }
            requireActivePartition(partition, "backup partition");
        }
        if (!uncoveredPartitions.isEmpty()) {
            throw new IllegalArgumentException("backup manifest must include every current partition");
        }
        var backupManifests = copy(metadata.backupManifestsById());
        requireAbsent(backupManifests, manifest.backupId(), "backup manifest");
        backupManifests.put(manifest.backupId(), manifest);
        return withBackupManifests(metadata, backupManifests);
    }

    private static ClusterMetadata recordCdcTokenTranslation(ClusterMetadata metadata,
                                                             MetadataCommand.RecordCdcTokenTranslation command) {
        var translation = command.translation();
        if (translation.metadataEpoch() != metadata.epoch()) {
            throw new IllegalArgumentException("CDC token translation metadata epoch must match current metadata epoch");
        }
        validateCdcTokenTranslation(metadata, translation);
        var translations = copy(metadata.cdcTokenTranslationsByEpoch());
        requireAbsent(translations, translation.metadataEpoch(), "CDC token translation");
        translations.put(translation.metadataEpoch(), translation);
        return withCdcTokenTranslations(metadata, translations);
    }

    private static void requireExpectedEpoch(ClusterMetadata metadata, MetadataCommand command) {
        if (metadata.epoch() != command.expectedMetadataEpoch()) {
            throw new IllegalArgumentException("metadata epoch does not match expectedMetadataEpoch");
        }
    }

    private static void validatePreparedSchemaChangeTarget(ClusterMetadata metadata,
                                                           MetadataCommand.PrepareSchemaChange command) {
        switch (command.changeKind()) {
            case CREATE_COLUMN -> {
                var column = requireArgument(command.column(), "column");
                requireAbsent(metadata.columnsById(), column.columnId(), "column");
            }
            case DELETE_COLUMN -> {
                var existing = metadata.requireColumn(command.columnId());
                if (existing.schemaEpoch() != command.targetSchemaEpoch()) {
                    throw new IllegalArgumentException("delete schema change target must match column schema epoch");
                }
            }
            case MERGE_OPERATOR_ACTIVATION -> {
                var existing = metadata.requireColumn(command.columnId());
                if (command.targetSchemaEpoch() <= existing.schemaEpoch()) {
                    throw new IllegalArgumentException("merge operator activation schema epoch must advance");
                }
            }
        }
    }

    private static SchemaChangeMetadata requireSchemaChange(ClusterMetadata metadata,
                                                            long schemaChangeId,
                                                            long columnId,
                                                            long targetSchemaEpoch,
                                                            SchemaChangeStatus status) {
        var schemaChange = metadata.requireSchemaChange(schemaChangeId);
        if (schemaChange.columnId() != columnId) {
            throw new IllegalArgumentException("schema change column does not match command");
        }
        if (schemaChange.targetSchemaEpoch() != targetSchemaEpoch) {
            throw new IllegalArgumentException("schema change target epoch does not match command");
        }
        if (schemaChange.status() != status) {
            throw new IllegalStateException("schema change must be " + status);
        }
        return schemaChange;
    }

    private static ColumnSchema withMergeOperator(ColumnSchema schema, MergeOperatorActivation activation) {
        var fixedKeys = new IntArrayList(schema.fixedLengthKeysCount());
        for (int index = 0; index < schema.fixedLengthKeysCount(); index++) {
            fixedKeys.add(schema.key(index));
        }
        var variableTailKeys = new ObjectArrayList<ColumnHashType>();
        for (int index = 0; index < schema.variableLengthKeysCount(); index++) {
            variableTailKeys.add(schema.variableTailKey(schema.fixedLengthKeysCount() + index));
        }
        return ColumnSchema.of(
                fixedKeys,
                variableTailKeys,
                schema.hasValue(),
                activation.name(),
                activation.version()
        );
    }

    private static <K, V> LinkedHashMap<K, V> copy(Map<K, V> source) {
        return new LinkedHashMap<>(source);
    }

    private static <K, V> void requireAbsent(Map<K, V> map, K key, String kind) {
        if (map.containsKey(key)) {
            throw new IllegalArgumentException("duplicate " + kind + ": " + key);
        }
    }

    private static void requireAdvancingPartitionEpoch(PartitionDescriptor partition,
                                                       long previousEpoch,
                                                       String kind) {
        if (partition.epoch() <= previousEpoch) {
            throw new IllegalArgumentException(kind + " epoch must advance");
        }
    }

    private static void requireAdjacent(PartitionDescriptor left, PartitionDescriptor right) {
        requireAdjacent(left, right, "merge partitions");
    }

    private static void requireAdjacent(PartitionDescriptor left,
                                        PartitionDescriptor right,
                                        String description) {
        if (left.endKeyExclusive() == null || right.startKeyInclusive() == null) {
            throw new IllegalArgumentException(description + " must share a concrete boundary");
        }
        requireSameBoundary(left.endKeyExclusive(), right.startKeyInclusive(), description + " boundary");
    }

    private static void requireSameTopology(PartitionDescriptor expected,
                                            PartitionDescriptor actual,
                                            String description) {
        if (!expected.peers().equals(actual.peers())
                || !sameValue(expected.leaderHint(), actual.leaderHint())
                || !expected.placementConstraints().equals(actual.placementConstraints())) {
            throw new IllegalArgumentException(description
                    + " must preserve partition membership, leader hint, and placement");
        }
    }

    private static void validateCdcTokenTranslation(ClusterMetadata metadata,
                                                    CdcTokenTranslation translation) {
        switch (translation) {
            case CdcTokenTranslation.Split split -> validateSplitCdcTranslation(metadata, split);
            case CdcTokenTranslation.Merge merge -> validateMergeCdcTranslation(metadata, merge);
        }
    }

    private static void validateSplitCdcTranslation(ClusterMetadata metadata,
                                                    CdcTokenTranslation.Split translation) {
        requireParentTokenPrecedesTranslation(translation.sourceEndToken(), translation.metadataEpoch(),
                "sourceEndToken");
        requirePartitionAbsent(metadata, translation.sourceEndToken().partitionId(), "split source partition");
        var left = requireCurrentStartTokenPartition(metadata, translation.leftStartToken(), "leftStartToken");
        var right = requireCurrentStartTokenPartition(metadata, translation.rightStartToken(), "rightStartToken");
        requireActivePartition(left, "left CDC child partition");
        requireActivePartition(right, "right CDC child partition");
        requireAdjacent(left, right, "split CDC child partitions");
        requireSameTopology(left, right, "split CDC child partitions");
    }

    private static void validateMergeCdcTranslation(ClusterMetadata metadata,
                                                    CdcTokenTranslation.Merge translation) {
        requireParentTokenPrecedesTranslation(translation.leftEndToken(), translation.metadataEpoch(),
                "leftEndToken");
        requireParentTokenPrecedesTranslation(translation.rightEndToken(), translation.metadataEpoch(),
                "rightEndToken");
        requirePartitionAbsent(metadata, translation.leftEndToken().partitionId(), "left merge parent partition");
        requirePartitionAbsent(metadata, translation.rightEndToken().partitionId(), "right merge parent partition");
        var merged = requireCurrentStartTokenPartition(metadata, translation.mergedStartToken(), "mergedStartToken");
        requireActivePartition(merged, "merged CDC partition");
    }

    private static PartitionDescriptor requireCurrentStartTokenPartition(ClusterMetadata metadata,
                                                                         ReplicatedCdcToken token,
                                                                         String tokenName) {
        if (token.metadataEpoch() != metadata.epoch()) {
            throw new IllegalArgumentException(tokenName + " metadata epoch must match current metadata epoch");
        }
        if (token.appliedIndex() != 0 || token.eventOrdinal() != 0) {
            throw new IllegalArgumentException(tokenName + " must be a partition start token");
        }
        var partition = metadata.requirePartition(token.partitionId());
        if (partition.epoch() != token.partitionEpoch()) {
            throw new IllegalArgumentException(tokenName + " partition epoch does not match metadata partition");
        }
        return partition;
    }

    private static void requireParentTokenPrecedesTranslation(ReplicatedCdcToken token,
                                                              long translationMetadataEpoch,
                                                              String tokenName) {
        if (token.metadataEpoch() >= translationMetadataEpoch) {
            throw new IllegalArgumentException(tokenName
                    + " metadata epoch must precede CDC token translation metadata epoch");
        }
    }

    private static void requirePartitionAbsent(ClusterMetadata metadata, ShardId partitionId, String description) {
        if (metadata.findPartition(partitionId).isPresent()) {
            throw new IllegalArgumentException(description + " must not exist in current metadata: " + partitionId);
        }
    }

    private static void requireActivePartition(PartitionDescriptor partition, String description) {
        if (partition.splitState() != PartitionSplitState.ACTIVE) {
            throw new IllegalStateException(description + " must be active");
        }
    }

    private static void requireValidPeerTransition(PartitionDescriptor existing,
                                                   List<ReplicaMember> proposedPeers,
                                                   NodeId proposedLeaderHint) {
        if (existing.isSplitting()) {
            throw new IllegalStateException("partition topology is changing");
        }
        var added = new ArrayList<ReplicaMember>();
        for (var proposed : proposedPeers) {
            if (findPeer(existing.peers(), proposed.nodeId()) == null) {
                added.add(proposed);
            }
        }
        var removed = new ArrayList<ReplicaMember>();
        ReplicaMember changedCurrent = null;
        ReplicaMember changedProposed = null;
        for (var current : existing.peers()) {
            var proposed = findPeer(proposedPeers, current.nodeId());
            if (proposed == null) {
                removed.add(current);
            } else if (!current.equals(proposed)) {
                if (changedCurrent != null) {
                    throw new IllegalArgumentException("partition peer change must alter at most one existing replica");
                }
                changedCurrent = current;
                changedProposed = proposed;
            }
        }

        if (added.size() > 1 || removed.size() > 1) {
            throw new IllegalArgumentException("partition peer change must be a single-step transition");
        }
        if (!added.isEmpty() && !removed.isEmpty()) {
            throw new IllegalArgumentException("partition peer change must not add and remove replicas together");
        }
        if (changedCurrent != null && (!added.isEmpty() || !removed.isEmpty())) {
            throw new IllegalArgumentException("partition peer change must not edit a replica while changing membership");
        }

        if (changedCurrent != null) {
            requireLearnerPromotion(existing, changedCurrent, changedProposed, proposedLeaderHint);
            return;
        }
        if (!added.isEmpty()) {
            requireReplicaAddition(existing, added.getFirst(), proposedLeaderHint);
            return;
        }
        if (!removed.isEmpty()) {
            requireReplicaRemoval(existing, proposedPeers, removed.getFirst(), proposedLeaderHint);
            return;
        }
        requireLeaderTransfer(existing, proposedPeers, proposedLeaderHint);
    }

    private static void requireReplicaAddition(PartitionDescriptor existing,
                                               ReplicaMember added,
                                               NodeId proposedLeaderHint) {
        if (!sameValue(existing.leaderHint(), proposedLeaderHint)) {
            throw new IllegalArgumentException("adding replica must preserve leader hint");
        }
        if (added.isVoter()) {
            throw new IllegalArgumentException("new replicas must be added as learners before catch-up");
        }
        if (added.servingReads()) {
            throw new IllegalArgumentException("new learners must not serve reads before catch-up");
        }
    }

    private static void requireLearnerPromotion(PartitionDescriptor existing,
                                                ReplicaMember current,
                                                ReplicaMember proposed,
                                                NodeId proposedLeaderHint) {
        if (!sameValue(existing.leaderHint(), proposedLeaderHint)) {
            throw new IllegalArgumentException("promoting replica must preserve leader hint");
        }
        if (current.isVoter() || !proposed.isVoter()) {
            throw new IllegalArgumentException("partition peer role changes must promote a learner to voter");
        }
        requireSameReplicaStateExceptRole(current, proposed);
        var leader = requireLeaderPeer(existing.peers(), existing.leaderHint(), "partition leader hint");
        requireCaughtUp(proposed, leader, "replica must catch up before promotion");
    }

    private static void requireReplicaRemoval(PartitionDescriptor existing,
                                              List<ReplicaMember> proposedPeers,
                                              ReplicaMember removed,
                                              NodeId proposedLeaderHint) {
        if (removed.isVoter()) {
            requireOldQuorumPreserved(existing.peers(), proposedPeers);
        }
        if (sameValue(removed.nodeId(), existing.leaderHint())) {
            if (proposedLeaderHint == null) {
                throw new IllegalArgumentException("removing the leader requires a replacement leader hint");
            }
            var replacementLeader = requireLeaderPeer(proposedPeers, proposedLeaderHint, "replacement leader");
            requireServingReads(replacementLeader, "replacement leader");
            requireCaughtUp(replacementLeader, removed, "replacement leader must be caught up to removed leader");
            return;
        }
        if (!sameValue(existing.leaderHint(), proposedLeaderHint)) {
            var currentLeader = requireLeaderPeer(existing.peers(), existing.leaderHint(), "partition leader hint");
            var newLeader = requireLeaderPeer(proposedPeers, proposedLeaderHint, "new leader hint");
            requireServingReads(newLeader, "new leader");
            requireCaughtUp(newLeader, currentLeader, "new leader must be caught up to current leader");
        }
    }

    private static void requireLeaderTransfer(PartitionDescriptor existing,
                                              List<ReplicaMember> proposedPeers,
                                              NodeId proposedLeaderHint) {
        if (sameValue(existing.leaderHint(), proposedLeaderHint)) {
            throw new IllegalArgumentException("partition peer change must change membership or leader hint");
        }
        var currentLeader = requireLeaderPeer(existing.peers(), existing.leaderHint(), "partition leader hint");
        var targetLeader = requireLeaderPeer(proposedPeers, proposedLeaderHint, "target leader");
        requireServingReads(targetLeader, "target leader");
        requireCaughtUp(targetLeader, currentLeader, "target leader must be caught up to current leader");
    }

    private static void requireSameReplicaStateExceptRole(ReplicaMember current,
                                                          ReplicaMember proposed) {
        if (!current.nodeId().equals(proposed.nodeId())
                || !current.endpoint().equals(proposed.endpoint())
                || current.matchIndex() != proposed.matchIndex()
                || current.appliedIndex() != proposed.appliedIndex()
                || current.servingReads() != proposed.servingReads()) {
            throw new IllegalArgumentException("promoted replica must preserve endpoint, progress, and read-serving state");
        }
    }

    private static ReplicaMember requireLeaderPeer(List<ReplicaMember> peers,
                                                   NodeId leaderHint,
                                                   String description) {
        if (leaderHint == null) {
            throw new IllegalStateException(description + " is missing");
        }
        var leader = findPeer(peers, leaderHint);
        if (leader == null || !leader.isVoter()) {
            throw new IllegalArgumentException(description + " must reference a voting peer");
        }
        return leader;
    }

    private static ReplicaMember findPeer(List<ReplicaMember> peers, NodeId nodeId) {
        if (nodeId == null) {
            return null;
        }
        for (var peer : peers) {
            if (peer.nodeId().equals(nodeId)) {
                return peer;
            }
        }
        return null;
    }

    private static void requireServingReads(ReplicaMember peer, String description) {
        if (!peer.servingReads()) {
            throw new IllegalArgumentException(description + " must be serving reads");
        }
    }

    private static void requireCaughtUp(ReplicaMember candidate,
                                        ReplicaMember reference,
                                        String message) {
        if (candidate.matchIndex() < reference.matchIndex()
                || candidate.appliedIndex() < reference.appliedIndex()) {
            throw new IllegalStateException(message);
        }
    }

    private static void requireOldQuorumPreserved(List<ReplicaMember> currentPeers,
                                                  List<ReplicaMember> proposedPeers) {
        var oldVoters = voterCount(currentPeers);
        var proposedVoters = voterCount(proposedPeers);
        var oldQuorum = oldVoters / 2 + 1;
        if (proposedVoters < oldQuorum) {
            throw new IllegalStateException("removing voter would not preserve the current quorum size");
        }
    }

    private static int voterCount(List<ReplicaMember> peers) {
        var voters = 0;
        for (var peer : peers) {
            if (peer.isVoter()) {
                voters++;
            }
        }
        return voters;
    }

    private static void requireSameBoundary(Buf expected,
                                            Buf actual,
                                            String name) {
        if (expected == null || actual == null) {
            if (expected != actual) {
                throw new IllegalArgumentException(name + " boundary mismatch");
            }
            return;
        }
        if (RockserverKeyEncoder.compareCalculatedKeys(expected, actual) != 0) {
            throw new IllegalArgumentException(name + " boundary mismatch");
        }
    }

    private static ClusterMetadata withNodes(ClusterMetadata metadata,
                                             Map<NodeId, ClusterNodeMetadata> nodes) {
        return new ClusterMetadata(
                metadata.clusterId(),
                metadata.epoch() + 1,
                metadata.shards(),
                metadata.columnsById(),
                metadata.partitionsById(),
                nodes,
                metadata.cdcTokenTranslationsByEpoch(),
                metadata.schemaChangesById(),
                metadata.backupManifestsById()
        );
    }

    private static ClusterMetadata withColumns(ClusterMetadata metadata,
                                               Map<Long, ClusterColumnMetadata> columns) {
        return new ClusterMetadata(
                metadata.clusterId(),
                metadata.epoch() + 1,
                metadata.shards(),
                columns,
                metadata.partitionsById(),
                metadata.nodesById(),
                metadata.cdcTokenTranslationsByEpoch(),
                metadata.schemaChangesById(),
                metadata.backupManifestsById()
        );
    }

    private static ClusterMetadata withColumnsAndSchemaChanges(ClusterMetadata metadata,
                                                               Map<Long, ClusterColumnMetadata> columns,
                                                               Map<Long, SchemaChangeMetadata> schemaChanges) {
        return new ClusterMetadata(
                metadata.clusterId(),
                metadata.epoch() + 1,
                metadata.shards(),
                columns,
                metadata.partitionsById(),
                metadata.nodesById(),
                metadata.cdcTokenTranslationsByEpoch(),
                schemaChanges,
                metadata.backupManifestsById()
        );
    }

    private static ClusterMetadata withPartitionsAndGroups(ClusterMetadata metadata,
                                                           Map<ShardId, PartitionDescriptor> partitions,
                                                           Map<ShardId, ReplicaGroupMetadata> groups) {
        return new ClusterMetadata(
                metadata.clusterId(),
                metadata.epoch() + 1,
                groups,
                metadata.columnsById(),
                partitions,
                metadata.nodesById(),
                metadata.cdcTokenTranslationsByEpoch(),
                metadata.schemaChangesById(),
                metadata.backupManifestsById()
        );
    }

    private static ClusterMetadata withCdcTokenTranslations(ClusterMetadata metadata,
                                                            Map<Long, CdcTokenTranslation> translations) {
        return new ClusterMetadata(
                metadata.clusterId(),
                metadata.epoch(),
                metadata.shards(),
                metadata.columnsById(),
                metadata.partitionsById(),
                metadata.nodesById(),
                translations,
                metadata.schemaChangesById(),
                metadata.backupManifestsById()
        );
    }

    private static ClusterMetadata withSchemaChanges(ClusterMetadata metadata,
                                                     Map<Long, SchemaChangeMetadata> schemaChanges) {
        return new ClusterMetadata(
                metadata.clusterId(),
                metadata.epoch() + 1,
                metadata.shards(),
                metadata.columnsById(),
                metadata.partitionsById(),
                metadata.nodesById(),
                metadata.cdcTokenTranslationsByEpoch(),
                schemaChanges,
                metadata.backupManifestsById()
        );
    }

    private static ClusterMetadata withBackupManifests(ClusterMetadata metadata,
                                                       Map<String, BackupManifest> backupManifests) {
        return new ClusterMetadata(
                metadata.clusterId(),
                metadata.epoch() + 1,
                metadata.shards(),
                metadata.columnsById(),
                metadata.partitionsById(),
                metadata.nodesById(),
                metadata.cdcTokenTranslationsByEpoch(),
                metadata.schemaChangesById(),
                backupManifests
        );
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

    private static <T> T requireState(T value, String name) {
        if (value == null) {
            throw new IllegalStateException(name + " must not be null");
        }
        return value;
    }

    private static ReplicaGroupMetadata routeGroup(PartitionDescriptor partition) {
        return new ReplicaGroupMetadata(
                partition.partitionId(),
                partition.epoch(),
                partition.leaderHint(),
                partition.peers(),
                partition.epoch()
        );
    }

}
