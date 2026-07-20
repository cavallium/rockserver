package it.cavallium.rockserver.core.cluster.workflow;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.RockserverKeyEncoder;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.SingleLineStrings;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.Nullable;

public final class PartitionCreationWorkflowPlanner {

    public PartitionCreationPlan planCreatePartition(ClusterMetadata metadata,
                                                     long expectedMetadataEpoch,
                                                     ShardId partitionId,
                                                     @Nullable Buf startKeyInclusive,
                                                     @Nullable Buf endKeyExclusive,
                                                     long partitionEpoch,
                                                     String ratisGroupId,
                                                     List<NodeId> voterNodeIds,
                                                     NodeId leaderHint,
                                                     Map<String, String> placementConstraints) {
        var requiredMetadata = requireArgument(metadata, "metadata");
        var requiredPartitionId = requireArgument(partitionId, "partitionId");
        var requiredVoterNodeIds = requireArgument(voterNodeIds, "voterNodeIds");
        var requiredLeaderHint = requireArgument(leaderHint, "leaderHint");
        var requiredPlacementConstraints = requireArgument(placementConstraints, "placementConstraints");
        requireMetadataEpoch(requiredMetadata, expectedMetadataEpoch);
        rejectExistingPartition(requiredMetadata, requiredPartitionId);
        rejectExistingRouteGroup(requiredMetadata, requiredPartitionId);

        var peers = votingPeers(requiredMetadata, requiredVoterNodeIds);
        requireVotingLeader(peers, requiredLeaderHint);
        var partition = new PartitionDescriptor(
                requiredPartitionId,
                startKeyInclusive,
                endKeyExclusive,
                partitionEpoch,
                SingleLineStrings.requireNonBlank(ratisGroupId, "ratisGroupId"),
                peers,
                requiredLeaderHint,
                requiredPlacementConstraints,
                PartitionSplitState.ACTIVE
        );
        rejectOverlappingRange(requiredMetadata, partition);
        return new PartitionCreationPlan(new MetadataCommand.CreatePartition(expectedMetadataEpoch, partition));
    }

    private static List<ReplicaMember> votingPeers(ClusterMetadata metadata, List<NodeId> voterNodeIds) {
        if (voterNodeIds.isEmpty()) {
            throw new IllegalArgumentException("partition must contain at least one voter");
        }
        var peers = new ArrayList<ReplicaMember>(voterNodeIds.size());
        var seen = new HashSet<NodeId>();
        for (NodeId nodeId : voterNodeIds) {
            var requiredNodeId = requireArgument(nodeId, "voterNodeIds entry");
            if (!seen.add(requiredNodeId)) {
                throw new IllegalArgumentException("duplicate voter node: " + requiredNodeId);
            }
            var node = metadata.requireNode(requiredNodeId);
            peers.add(new ReplicaMember(
                    node.nodeId(),
                    node.ratisEndpoint(),
                    ReplicaRole.VOTER,
                    0,
                    0,
                    true
            ));
        }
        return List.copyOf(peers);
    }

    private static void requireVotingLeader(List<ReplicaMember> peers, NodeId leaderHint) {
        for (ReplicaMember peer : peers) {
            if (peer.nodeId().equals(leaderHint) && peer.isVoter()) {
                return;
            }
        }
        throw new IllegalArgumentException("leader hint must reference a voting peer: " + leaderHint);
    }

    private static void requireMetadataEpoch(ClusterMetadata metadata, long expectedMetadataEpoch) {
        if (expectedMetadataEpoch < 0) {
            throw new IllegalArgumentException("expectedMetadataEpoch must be >= 0");
        }
        if (metadata.epoch() != expectedMetadataEpoch) {
            throw new IllegalArgumentException("metadata epoch does not match expectedMetadataEpoch");
        }
    }

    private static void rejectExistingPartition(ClusterMetadata metadata, ShardId partitionId) {
        if (metadata.findPartition(partitionId).isPresent()) {
            throw new IllegalArgumentException("partition already exists: " + partitionId);
        }
    }

    private static void rejectExistingRouteGroup(ClusterMetadata metadata, ShardId partitionId) {
        if (metadata.findGroup(partitionId).isPresent()) {
            throw new IllegalArgumentException("partition route group already exists: " + partitionId);
        }
    }

    private static void rejectOverlappingRange(ClusterMetadata metadata, PartitionDescriptor candidate) {
        for (PartitionDescriptor existing : metadata.partitionDescriptors()) {
            if (rangesIntersect(candidate.startKeyInclusive(),
                    candidate.endKeyExclusive(),
                    existing.startKeyInclusive(),
                    existing.endKeyExclusive())) {
                throw new IllegalArgumentException("partition range overlaps existing partition: "
                        + existing.partitionId());
            }
        }
    }

    private static boolean rangesIntersect(@Nullable Buf leftStartInclusive,
                                           @Nullable Buf leftEndExclusive,
                                           @Nullable Buf rightStartInclusive,
                                           @Nullable Buf rightEndExclusive) {
        return lowerLessThanUpper(leftStartInclusive, rightEndExclusive)
                && lowerLessThanUpper(rightStartInclusive, leftEndExclusive);
    }

    private static boolean lowerLessThanUpper(@Nullable Buf lowerInclusive, @Nullable Buf upperExclusive) {
        return lowerInclusive == null
                || upperExclusive == null
                || RockserverKeyEncoder.compareCalculatedKeys(lowerInclusive, upperExclusive) < 0;
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
