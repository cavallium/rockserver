package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import it.cavallium.rockserver.core.cluster.metadata.MergeOperatorActivation;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandCodec;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandValidator;
import it.cavallium.rockserver.core.cluster.metadata.MetadataStateMachine;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeKind;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeStatus;
import it.cavallium.rockserver.core.cluster.workflow.PartitionTopologyWorkflowPlanner;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class MetadataStateMachineTest {

    private static final ClusterId CLUSTER_ID = new ClusterId(new UUID(0, 1));

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var metadata = new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of());
        var stateMachine = new MetadataStateMachine(metadata, 5);

        assertIllegalArgument("metadata must not be null", () -> new MetadataStateMachine(null));
        assertIllegalArgument("metadata must not be null", () -> new MetadataStateMachine(null, 5));
        assertIllegalArgument("payload must not be null", () -> stateMachine.applyCommitted(6, null));
        assertIllegalArgument("metadata must not be null", () -> new MetadataStateMachine.ApplyResult(6, true, null));
    }

    @Test
    void appliesCommittedMetadataCommandsOnceByLogIndex() {
        var initial = new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of());
        var stateMachine = new MetadataStateMachine(initial, 5);
        var column = new ClusterColumnMetadata(42, "accounts", schema(), 0);
        var command = new MetadataCommand.CreateColumn(7, column);

        var skipped = stateMachine.applyCommitted(5, new byte[]{1});
        assertFalse(skipped.applied());
        assertEquals(5, stateMachine.lastAppliedIndex());
        assertEquals(initial, stateMachine.metadata());

        var applied = stateMachine.applyCommitted(6, MetadataCommandCodec.encode(command));

        assertTrue(applied.applied());
        assertEquals(6, applied.logIndex());
        assertEquals(6, stateMachine.lastAppliedIndex());
        assertEquals(8, stateMachine.metadata().epoch());
        assertEquals(column, stateMachine.metadata().requireColumn(42));
    }

    @Test
    void committedPayloadCapRejectsOversizedPayloadWithoutAdvancingState() {
        var initial = new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of());
        var stateMachine = new MetadataStateMachine(initial, 5);
        var oversized = new byte[MetadataCommandValidator.MAX_PROPOSAL_PAYLOAD_BYTES + 1];

        var thrown = assertThrows(IllegalArgumentException.class,
                () -> stateMachine.applyCommitted(6, oversized));

        assertEquals("Metadata committed payload exceeds "
                + MetadataCommandValidator.MAX_PROPOSAL_PAYLOAD_BYTES + " bytes: " + oversized.length,
                thrown.getMessage());
        assertEquals(5, stateMachine.lastAppliedIndex());
        assertEquals(initial, stateMachine.metadata());
    }

    @Test
    void appliesPlannerTopologyCommandsThroughCodecAndStateMachine() {
        var planner = new PartitionTopologyWorkflowPlanner();
        var source = partition(new ShardId(1), buf(0x00), buf(0xff), 3, "group-1");
        var stateMachine = new MetadataStateMachine(metadata(11, source));

        var splitPlan = planner.planSplit(
                stateMachine.metadata(),
                11,
                source.partitionId(),
                source.epoch(),
                new ShardId(2),
                new ShardId(3),
                buf(0x80),
                "group-2",
                "group-3",
                token(11, source.partitionId(), source.epoch(), 40, 0)
        );

        var split = stateMachine.applyCommitted(1, MetadataCommandCodec.encode(splitPlan.splitCommand()));

        assertTrue(split.applied());
        assertEquals(12, stateMachine.metadata().epoch());
        assertTrue(stateMachine.metadata().findPartition(source.partitionId()).isEmpty());
        var left = stateMachine.metadata().requirePartition(new ShardId(2));
        var right = stateMachine.metadata().requirePartition(new ShardId(3));
        assertEquals(left.peers(), stateMachine.metadata().requireGroup(left.partitionId()).members());
        assertEquals(right.peers(), stateMachine.metadata().requireGroup(right.partitionId()).members());

        var splitTranslation = stateMachine.applyCommitted(
                2,
                MetadataCommandCodec.encode(splitPlan.cdcTokenTranslationCommand())
        );

        assertTrue(splitTranslation.applied());
        assertEquals(12, stateMachine.metadata().epoch());
        assertEquals(splitPlan.cdcTokenTranslationCommand().translation(),
                stateMachine.metadata().requireCdcTokenTranslation(12));

        var mergePlan = planner.planMerge(
                stateMachine.metadata(),
                12,
                left.partitionId(),
                right.partitionId(),
                new ShardId(4),
                "group-4",
                token(12, left.partitionId(), left.epoch(), 50, 0),
                token(12, right.partitionId(), right.epoch(), 55, 0)
        );

        var merge = stateMachine.applyCommitted(3, MetadataCommandCodec.encode(mergePlan.mergeCommand()));

        assertTrue(merge.applied());
        assertEquals(13, stateMachine.metadata().epoch());
        assertTrue(stateMachine.metadata().findPartition(left.partitionId()).isEmpty());
        assertTrue(stateMachine.metadata().findPartition(right.partitionId()).isEmpty());
        var merged = stateMachine.metadata().requirePartition(new ShardId(4));
        assertEquals(buf(0x00), merged.startKeyInclusive());
        assertEquals(buf(0xff), merged.endKeyExclusive());
        assertEquals(merged.peers(), stateMachine.metadata().requireGroup(merged.partitionId()).members());

        var mergeTranslation = stateMachine.applyCommitted(
                4,
                MetadataCommandCodec.encode(mergePlan.cdcTokenTranslationCommand())
        );

        assertTrue(mergeTranslation.applied());
        assertEquals(13, stateMachine.metadata().epoch());
        assertEquals(mergePlan.cdcTokenTranslationCommand().translation(),
                stateMachine.metadata().requireCdcTokenTranslation(13));
        assertEquals(4, stateMachine.lastAppliedIndex());
    }

    @Test
    void appliesSchemaAndBackupMetadataCommandsThroughCodecAndStateMachine() {
        var source = partition(new ShardId(1), buf(0x00), buf(0xff), 3, "group-1");
        var stateMachine = new MetadataStateMachine(metadata(20, source));
        var column = new ClusterColumnMetadata(42, "accounts", schema(), 5);

        var prepare = stateMachine.applyCommitted(1, MetadataCommandCodec.encode(
                new MetadataCommand.PrepareSchemaChange(
                        20,
                        100,
                        SchemaChangeKind.CREATE_COLUMN,
                        42,
                        5,
                        column,
                        null
                )
        ));
        var activate = stateMachine.applyCommitted(2, MetadataCommandCodec.encode(
                new MetadataCommand.ActivateSchemaChange(21, 100, 42, 5)
        ));
        var fence = stateMachine.applyCommitted(3, MetadataCommandCodec.encode(
                new MetadataCommand.FenceSchemaChange(22, 100, 42, 5)
        ));
        var mergePrepare = stateMachine.applyCommitted(4, MetadataCommandCodec.encode(
                new MetadataCommand.PrepareSchemaChange(
                        23,
                        101,
                        SchemaChangeKind.MERGE_OPERATOR_ACTIVATION,
                        42,
                        6,
                        null,
                        new MergeOperatorActivation("counter", 1, buf(0x01, 0x02))
                )
        ));
        var backupManifest = new BackupManifest(
                "backup-25",
                25,
                24,
                List.of(new BackupManifest.PartitionCut(source.partitionId(), source.epoch(), 6, 90, 25))
        );
        var backup = stateMachine.applyCommitted(5, MetadataCommandCodec.encode(
                new MetadataCommand.RecordBackupManifest(24, backupManifest)
        ));

        assertTrue(prepare.applied());
        assertTrue(activate.applied());
        assertTrue(fence.applied());
        assertTrue(mergePrepare.applied());
        assertTrue(backup.applied());
        assertEquals(column, stateMachine.metadata().requireColumn(42));
        assertEquals(SchemaChangeStatus.FENCED, stateMachine.metadata().requireSchemaChange(100).status());
        assertEquals(SchemaChangeStatus.PREPARED, stateMachine.metadata().requireSchemaChange(101).status());
        assertEquals(backupManifest, stateMachine.metadata().requireBackupManifest("backup-25"));
        assertEquals(25, stateMachine.metadata().epoch());
        assertEquals(5, stateMachine.lastAppliedIndex());
    }

    private static ColumnSchema schema() {
        return ColumnSchema.of(IntArrayList.of(1), ObjectArrayList.of(), true);
    }

    private static ClusterMetadata metadata(long epoch, PartitionDescriptor... partitions) {
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
        return new ClusterMetadata(CLUSTER_ID, epoch, groups, Map.of(), partitionMap);
    }

    private static PartitionDescriptor partition(ShardId partitionId,
                                                 Buf startKeyInclusive,
                                                 Buf endKeyExclusive,
                                                 long epoch,
                                                 String ratisGroupId) {
        var leader = NodeId.of("n1");
        return new PartitionDescriptor(
                partitionId,
                startKeyInclusive,
                endKeyExclusive,
                epoch,
                ratisGroupId,
                List.of(peer(leader)),
                leader,
                Map.of("rack", "rack-a"),
                PartitionSplitState.ACTIVE
        );
    }

    private static ReplicaMember peer(NodeId nodeId) {
        return new ReplicaMember(nodeId, URI.create("grpc://127.0.0.1:10001"), ReplicaRole.VOTER, 0, 0, true);
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

    private static void assertIllegalArgument(String message, Runnable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }
}
