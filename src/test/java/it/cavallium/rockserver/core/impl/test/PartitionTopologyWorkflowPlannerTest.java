package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
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
import it.cavallium.rockserver.core.cluster.metadata.CdcTokenTranslation;
import it.cavallium.rockserver.core.cluster.workflow.PartitionMergePlan;
import it.cavallium.rockserver.core.cluster.workflow.PartitionSplitPlan;
import it.cavallium.rockserver.core.cluster.workflow.PartitionTopologyWorkflowPlanner;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class PartitionTopologyWorkflowPlannerTest {

    private final PartitionTopologyWorkflowPlanner planner = new PartitionTopologyWorkflowPlanner();

    @Test
    void plansSplitWithContiguousChildrenAndCdcTranslation() {
        var source = partition(new ShardId(1), buf(0x00), buf(0xff), 3, "group-1");
        var metadata = metadata(11, source);
        var sourceEndToken = token(11, source.partitionId(), source.epoch(), 40);

        var plan = planner.planSplit(
                metadata,
                11,
                source.partitionId(),
                source.epoch(),
                new ShardId(2),
                new ShardId(3),
                buf(0x80),
                "group-2",
                "group-3",
                sourceEndToken
        );

        var split = plan.splitCommand();
        assertEquals(11, split.expectedMetadataEpoch());
        assertEquals(source.partitionId(), split.sourcePartitionId());
        assertEquals(4, split.left().epoch());
        assertEquals(buf(0x00), split.left().startKeyInclusive());
        assertEquals(buf(0x80), split.left().endKeyExclusive());
        assertEquals(buf(0x80), split.right().startKeyInclusive());
        assertEquals(buf(0xff), split.right().endKeyExclusive());
        assertEquals(PartitionSplitState.ACTIVE, split.left().splitState());
        assertEquals(PartitionSplitState.ACTIVE, split.right().splitState());

        var translation = (CdcTokenTranslation.Split) plan.cdcTokenTranslationCommand().translation();
        assertEquals(12, plan.cdcTokenTranslationCommand().expectedMetadataEpoch());
        assertEquals(sourceEndToken, translation.sourceEndToken());
        assertEquals(new ShardId(2), translation.leftStartToken().partitionId());
        assertEquals(new ShardId(3), translation.rightStartToken().partitionId());
        assertEquals(4, translation.leftStartToken().partitionEpoch());
        assertEquals(4, translation.rightStartToken().partitionEpoch());
    }

    @Test
    void splitRejectsBoundaryOutsideSourceRangeOrWrongEpoch() {
        var source = partition(new ShardId(1), buf(0x10), buf(0x20), 3, "group-1");
        var metadata = metadata(11, source);
        var sourceEndToken = token(11, source.partitionId(), source.epoch(), 40);

        assertThrows(IllegalArgumentException.class, () -> planner.planSplit(
                metadata, 11, source.partitionId(), 2, new ShardId(2), new ShardId(3), buf(0x18),
                "group-2", "group-3", sourceEndToken));
        assertThrows(IllegalArgumentException.class, () -> planner.planSplit(
                metadata, 11, source.partitionId(), source.epoch(), new ShardId(2), new ShardId(3), buf(0x10),
                "group-2", "group-3", sourceEndToken));
        assertThrows(IllegalArgumentException.class, () -> planner.planSplit(
                metadata, 11, source.partitionId(), source.epoch(), new ShardId(2), new ShardId(3), buf(0x20),
                "group-2", "group-3", sourceEndToken));
    }

    @Test
    void splitRejectsMissingSourceRouteGroupOrConflictingChildRouteGroup() {
        var source = partition(new ShardId(1), buf(0x00), buf(0xff), 3, "group-1");
        var sourceEndToken = token(11, source.partitionId(), source.epoch(), 40);

        assertThrows(IllegalArgumentException.class, () -> planner.planSplit(
                metadataWithoutRouteGroups(11, source),
                11,
                source.partitionId(),
                source.epoch(),
                new ShardId(2),
                new ShardId(3),
                buf(0x80),
                "group-2",
                "group-3",
                sourceEndToken
        ));

        assertThrows(IllegalArgumentException.class, () -> planner.planSplit(
                metadata(11, Map.of(new ShardId(2), routeGroup(new ShardId(2))), source),
                11,
                source.partitionId(),
                source.epoch(),
                new ShardId(2),
                new ShardId(3),
                buf(0x80),
                "group-2",
                "group-3",
                sourceEndToken
        ));

        assertThrows(IllegalArgumentException.class, () -> planner.planSplit(
                metadata(11, source),
                11,
                source.partitionId(),
                source.epoch(),
                new ShardId(2),
                new ShardId(2),
                buf(0x80),
                "group-2",
                "group-3",
                sourceEndToken
        ));
    }

    @Test
    void topologyPlansRejectInvalidRatisGroupIds() {
        var source = partition(new ShardId(1), buf(0x00), buf(0xff), 3, "group-1");
        var metadata = metadata(11, source);
        var sourceEndToken = token(11, source.partitionId(), source.epoch(), 40);

        assertThrows(IllegalArgumentException.class, () -> planner.planSplit(
                metadata,
                11,
                source.partitionId(),
                source.epoch(),
                new ShardId(2),
                new ShardId(3),
                buf(0x80),
                "group-2\ninjected=true",
                "group-3",
                sourceEndToken
        ));

        var left = partition(new ShardId(2), buf(0x00), buf(0x80), 4, "group-2");
        var right = partition(new ShardId(3), buf(0x80), buf(0xff), 4, "group-3");
        var mergeMetadata = metadata(12, left, right);

        assertThrows(IllegalArgumentException.class, () -> planner.planMerge(
                mergeMetadata,
                12,
                left.partitionId(),
                right.partitionId(),
                new ShardId(4),
                "group-4\ninjected=true",
                token(12, left.partitionId(), left.epoch(), 50),
                token(12, right.partitionId(), right.epoch(), 55)
        ));
    }

    @Test
    void plansMergeWithAdjacentParentsAndCdcTranslation() {
        var left = partition(new ShardId(2), buf(0x00), buf(0x80), 4, "group-2");
        var right = partition(new ShardId(3), buf(0x80), buf(0xff), 4, "group-3");
        var metadata = metadata(12, left, right);

        var plan = planner.planMerge(
                metadata,
                12,
                left.partitionId(),
                right.partitionId(),
                new ShardId(4),
                "group-4",
                token(12, left.partitionId(), left.epoch(), 50),
                token(12, right.partitionId(), right.epoch(), 55)
        );

        var merge = plan.mergeCommand();
        assertEquals(12, merge.expectedMetadataEpoch());
        assertEquals(new ShardId(4), merge.merged().partitionId());
        assertEquals(5, merge.merged().epoch());
        assertEquals(buf(0x00), merge.merged().startKeyInclusive());
        assertEquals(buf(0xff), merge.merged().endKeyExclusive());

        var translation = (CdcTokenTranslation.Merge) plan.cdcTokenTranslationCommand().translation();
        assertEquals(13, plan.cdcTokenTranslationCommand().expectedMetadataEpoch());
        assertEquals(new ShardId(4), translation.mergedStartToken().partitionId());
        assertEquals(5, translation.mergedStartToken().partitionEpoch());
    }

    @Test
    void mergeRejectsNonAdjacentOrMismatchedMembershipParents() {
        var left = partition(new ShardId(2), buf(0x00), buf(0x70), 4, "group-2");
        var right = partition(new ShardId(3), buf(0x80), buf(0xff), 4, "group-3");
        var metadata = metadata(12, left, right);

        assertThrows(IllegalArgumentException.class, () -> planner.planMerge(
                metadata,
                12,
                left.partitionId(),
                right.partitionId(),
                new ShardId(4),
                "group-4",
                token(12, left.partitionId(), left.epoch(), 50),
                token(12, right.partitionId(), right.epoch(), 55)
        ));

        var differentPeer = new PartitionDescriptor(
                right.partitionId(),
                right.startKeyInclusive(),
                right.endKeyExclusive(),
                right.epoch(),
                right.ratisGroupId(),
                List.of(peer(NodeId.of("n2"))),
                NodeId.of("n2"),
                right.placementConstraints(),
                right.splitState()
        );
        var membershipMismatch = metadata(12, partition(new ShardId(2), buf(0x00), buf(0x80), 4, "group-2"),
                differentPeer);

        assertThrows(IllegalArgumentException.class, () -> planner.planMerge(
                membershipMismatch,
                12,
                new ShardId(2),
                right.partitionId(),
                new ShardId(4),
                "group-4",
                token(12, new ShardId(2), 4, 50),
                token(12, right.partitionId(), right.epoch(), 55)
        ));
    }

    @Test
    void mergeRejectsMissingParentRouteGroupOrConflictingMergedRouteGroup() {
        var left = partition(new ShardId(2), buf(0x00), buf(0x80), 4, "group-2");
        var right = partition(new ShardId(3), buf(0x80), buf(0xff), 4, "group-3");

        assertThrows(IllegalArgumentException.class, () -> planner.planMerge(
                metadataWithoutRouteGroups(12, left, right),
                12,
                left.partitionId(),
                right.partitionId(),
                new ShardId(4),
                "group-4",
                token(12, left.partitionId(), left.epoch(), 50),
                token(12, right.partitionId(), right.epoch(), 55)
        ));

        assertThrows(IllegalArgumentException.class, () -> planner.planMerge(
                metadata(12, Map.of(new ShardId(4), routeGroup(new ShardId(4))), left, right),
                12,
                left.partitionId(),
                right.partitionId(),
                new ShardId(4),
                "group-4",
                token(12, left.partitionId(), left.epoch(), 50),
                token(12, right.partitionId(), right.epoch(), 55)
        ));
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var source = partition(new ShardId(1), buf(0x00), buf(0xff), 3, "group-1");
        var metadata = metadata(11, source);
        var sourceEndToken = token(11, source.partitionId(), source.epoch(), 40);

        assertIllegalArgument("metadata must not be null", () -> planner.planSplit(
                null, 11, source.partitionId(), source.epoch(), new ShardId(2), new ShardId(3), buf(0x80),
                "group-2", "group-3", sourceEndToken));
        assertIllegalArgument("sourcePartitionId must not be null", () -> planner.planSplit(
                metadata, 11, null, source.epoch(), new ShardId(2), new ShardId(3), buf(0x80),
                "group-2", "group-3", sourceEndToken));
        assertIllegalArgument("leftPartitionId must not be null", () -> planner.planSplit(
                metadata, 11, source.partitionId(), source.epoch(), null, new ShardId(3), buf(0x80),
                "group-2", "group-3", sourceEndToken));
        assertIllegalArgument("rightPartitionId must not be null", () -> planner.planSplit(
                metadata, 11, source.partitionId(), source.epoch(), new ShardId(2), null, buf(0x80),
                "group-2", "group-3", sourceEndToken));
        assertIllegalArgument("splitKey must not be null", () -> planner.planSplit(
                metadata, 11, source.partitionId(), source.epoch(), new ShardId(2), new ShardId(3), null,
                "group-2", "group-3", sourceEndToken));
        assertIllegalArgument("sourceEndToken must not be null", () -> planner.planSplit(
                metadata, 11, source.partitionId(), source.epoch(), new ShardId(2), new ShardId(3), buf(0x80),
                "group-2", "group-3", null));

        var splitPlan = planner.planSplit(metadata, 11, source.partitionId(), source.epoch(), new ShardId(2),
                new ShardId(3), buf(0x80), "group-2", "group-3", sourceEndToken);
        assertIllegalArgument("splitCommand must not be null",
                () -> new PartitionSplitPlan(null, splitPlan.cdcTokenTranslationCommand()));
        assertIllegalArgument("cdcTokenTranslationCommand must not be null",
                () -> new PartitionSplitPlan(splitPlan.splitCommand(), null));

        var left = partition(new ShardId(2), buf(0x00), buf(0x80), 4, "group-2");
        var right = partition(new ShardId(3), buf(0x80), buf(0xff), 4, "group-3");
        var mergeMetadata = metadata(12, left, right);
        var leftEndToken = token(12, left.partitionId(), left.epoch(), 50);
        var rightEndToken = token(12, right.partitionId(), right.epoch(), 55);

        assertIllegalArgument("metadata must not be null", () -> planner.planMerge(
                null, 12, left.partitionId(), right.partitionId(), new ShardId(4), "group-4", leftEndToken,
                rightEndToken));
        assertIllegalArgument("leftPartitionId must not be null", () -> planner.planMerge(
                mergeMetadata, 12, null, right.partitionId(), new ShardId(4), "group-4", leftEndToken,
                rightEndToken));
        assertIllegalArgument("rightPartitionId must not be null", () -> planner.planMerge(
                mergeMetadata, 12, left.partitionId(), null, new ShardId(4), "group-4", leftEndToken,
                rightEndToken));
        assertIllegalArgument("mergedPartitionId must not be null", () -> planner.planMerge(
                mergeMetadata, 12, left.partitionId(), right.partitionId(), null, "group-4", leftEndToken,
                rightEndToken));
        assertIllegalArgument("leftEndToken must not be null", () -> planner.planMerge(
                mergeMetadata, 12, left.partitionId(), right.partitionId(), new ShardId(4), "group-4", null,
                rightEndToken));
        assertIllegalArgument("rightEndToken must not be null", () -> planner.planMerge(
                mergeMetadata, 12, left.partitionId(), right.partitionId(), new ShardId(4), "group-4",
                leftEndToken, null));

        var mergePlan = planner.planMerge(mergeMetadata, 12, left.partitionId(), right.partitionId(),
                new ShardId(4), "group-4", leftEndToken, rightEndToken);
        assertIllegalArgument("mergeCommand must not be null",
                () -> new PartitionMergePlan(null, mergePlan.cdcTokenTranslationCommand()));
        assertIllegalArgument("cdcTokenTranslationCommand must not be null",
                () -> new PartitionMergePlan(mergePlan.mergeCommand(), null));
    }

    private static ClusterMetadata metadata(long epoch, PartitionDescriptor... partitions) {
        return metadata(epoch, Map.of(), partitions);
    }

    private static ClusterMetadata metadata(long epoch,
                                            Map<ShardId, ReplicaGroupMetadata> extraRouteGroups,
                                            PartitionDescriptor... partitions) {
        var byId = new java.util.HashMap<ShardId, PartitionDescriptor>();
        var groups = new java.util.HashMap<ShardId, ReplicaGroupMetadata>(extraRouteGroups);
        for (PartitionDescriptor partition : partitions) {
            byId.put(partition.partitionId(), partition);
            groups.putIfAbsent(partition.partitionId(), routeGroup(partition));
        }
        return new ClusterMetadata(new ClusterId(new UUID(0, 1)), epoch, Map.copyOf(groups), Map.of(),
                Map.copyOf(byId));
    }

    private static ClusterMetadata metadataWithoutRouteGroups(long epoch, PartitionDescriptor... partitions) {
        var byId = new java.util.HashMap<ShardId, PartitionDescriptor>();
        for (PartitionDescriptor partition : partitions) {
            byId.put(partition.partitionId(), partition);
        }
        return new ClusterMetadata(new ClusterId(new UUID(0, 1)), epoch, Map.of(), Map.of(), Map.copyOf(byId));
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

    private static ReplicaGroupMetadata routeGroup(ShardId shardId) {
        var peer = peer(NodeId.of("n1"));
        return new ReplicaGroupMetadata(shardId, 1, peer.nodeId(), List.of(peer), 1);
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
                                            long appliedIndex) {
        return new ReplicatedCdcToken(metadataEpoch, partitionId, partitionEpoch, appliedIndex, 0);
    }

    private static Buf buf(int... values) {
        var bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return Buf.wrap(bytes);
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var failure = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, failure.getMessage());
    }
}
