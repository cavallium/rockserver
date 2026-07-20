package it.cavallium.rockserver.core.cluster.workflow;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;

import java.util.ArrayList;
import java.util.List;

import org.jetbrains.annotations.Nullable;

public final class PartitionMembershipWorkflowPlanner {

    public PartitionMembershipChangePlan planAddReplica(ClusterMetadata metadata,
                                                        long expectedMetadataEpoch,
                                                        ShardId partitionId,
                                                        long expectedPartitionEpoch,
                                                        ReplicaMember newReplica) {
        var requiredNewReplica = requireArgument(newReplica, "newReplica");
        if (requiredNewReplica.role() != ReplicaRole.LEARNER) {
            throw new IllegalArgumentException("new replicas must be added as learners before catch-up");
        }
        if (requiredNewReplica.servingReads()) {
            throw new IllegalArgumentException("new learners must not serve reads before catch-up");
        }
        var partition = requirePartition(metadata, expectedMetadataEpoch, partitionId, expectedPartitionEpoch);
        requireRegisteredNode(metadata, requiredNewReplica);
        if (findPeer(partition, requiredNewReplica.nodeId()) != null) {
            throw new IllegalArgumentException("replica already exists in partition: " + requiredNewReplica.nodeId());
        }
        var peers = new ArrayList<>(partition.peers());
        peers.add(requiredNewReplica);
        return command(MembershipChangeKind.ADD_REPLICA, expectedMetadataEpoch, partition, peers, partition.leaderHint());
    }

    public ReplicaCatchUpPlan planCatchUp(ClusterMetadata metadata,
                                          long expectedMetadataEpoch,
                                          ShardId partitionId,
                                          long expectedPartitionEpoch,
                                          NodeId replicaId) {
        var partition = requirePartition(metadata, expectedMetadataEpoch, partitionId, expectedPartitionEpoch);
        var replica = requirePeer(partition, replicaId);
        if (replica.isVoter()) {
            throw new IllegalArgumentException("catch-up target must be a learner");
        }
        var leader = requireLeaderPeer(partition);
        return new ReplicaCatchUpPlan(
                partition.partitionId(),
                partition.epoch(),
                replica.nodeId(),
                leader.matchIndex(),
                leader.appliedIndex()
        );
    }

    public PartitionMembershipChangePlan planPromoteReplica(ClusterMetadata metadata,
                                                            long expectedMetadataEpoch,
                                                            ShardId partitionId,
                                                            long expectedPartitionEpoch,
                                                            NodeId replicaId) {
        var partition = requirePartition(metadata, expectedMetadataEpoch, partitionId, expectedPartitionEpoch);
        var replica = requirePeer(partition, replicaId);
        if (replica.isVoter()) {
            throw new IllegalArgumentException("replica is already a voter: " + replicaId);
        }
        var leader = requireLeaderPeer(partition);
        requireCaughtUp(replica, leader, "replica must catch up before promotion");
        var peers = replacePeer(partition.peers(), new ReplicaMember(
                replica.nodeId(),
                replica.endpoint(),
                ReplicaRole.VOTER,
                replica.matchIndex(),
                replica.appliedIndex(),
                replica.servingReads()
        ));
        return command(MembershipChangeKind.PROMOTE_REPLICA, expectedMetadataEpoch, partition, peers,
                partition.leaderHint());
    }

    public PartitionMembershipChangePlan planRemoveReplica(ClusterMetadata metadata,
                                                           long expectedMetadataEpoch,
                                                           ShardId partitionId,
                                                           long expectedPartitionEpoch,
                                                           NodeId replicaId,
                                                           @Nullable NodeId newLeaderHint) {
        var partition = requirePartition(metadata, expectedMetadataEpoch, partitionId, expectedPartitionEpoch);
        var removed = requirePeer(partition, replicaId);
        var peers = partition.peers().stream()
                .filter(peer -> !peer.nodeId().equals(replicaId))
                .toList();
        if (peers.isEmpty()) {
            throw new IllegalArgumentException("cannot remove the final partition replica");
        }
        if (removed.isVoter()) {
            requireOldQuorumPreserved(partition.peers(), peers);
        }

        var leaderHint = partition.leaderHint();
        if (replicaId.equals(leaderHint)) {
            if (newLeaderHint == null) {
                throw new IllegalArgumentException("removing the leader requires a replacement leader hint");
            }
            var newLeader = findPeer(peers, newLeaderHint);
            if (newLeader == null || !newLeader.isVoter()) {
                throw new IllegalArgumentException("replacement leader must be a remaining voter");
            }
            if (!newLeader.servingReads()) {
                throw new IllegalArgumentException("replacement leader must be serving reads");
            }
            requireCaughtUp(newLeader, removed, "replacement leader must be caught up to removed leader");
            leaderHint = newLeaderHint;
        } else if (newLeaderHint != null) {
            var newLeader = findPeer(peers, newLeaderHint);
            if (newLeader == null || !newLeader.isVoter()) {
                throw new IllegalArgumentException("new leader hint must reference a remaining voter");
            }
            if (!newLeader.servingReads()) {
                throw new IllegalArgumentException("new leader must be serving reads");
            }
            var currentLeader = requireLeaderPeer(partition);
            requireCaughtUp(newLeader, currentLeader, "new leader must be caught up to current leader");
            leaderHint = newLeaderHint;
        }

        return command(MembershipChangeKind.REMOVE_REPLICA, expectedMetadataEpoch, partition, peers, leaderHint);
    }

    public PartitionMembershipChangePlan planLeaderTransfer(ClusterMetadata metadata,
                                                            long expectedMetadataEpoch,
                                                            ShardId partitionId,
                                                            long expectedPartitionEpoch,
                                                            NodeId targetLeaderId) {
        var requiredTargetLeaderId = requireArgument(targetLeaderId, "targetLeaderId");
        var partition = requirePartition(metadata, expectedMetadataEpoch, partitionId, expectedPartitionEpoch);
        if (requiredTargetLeaderId.equals(partition.leaderHint())) {
            throw new IllegalArgumentException("target leader is already the leader");
        }
        var currentLeader = requireLeaderPeer(partition);
        var targetLeader = requirePeer(partition, requiredTargetLeaderId);
        if (!targetLeader.isVoter()) {
            throw new IllegalArgumentException("target leader must be a voter");
        }
        if (!targetLeader.servingReads()) {
            throw new IllegalArgumentException("target leader must be serving reads");
        }
        requireCaughtUp(targetLeader, currentLeader, "target leader must be caught up to current leader");
        return command(MembershipChangeKind.TRANSFER_LEADER, expectedMetadataEpoch, partition, partition.peers(),
                requiredTargetLeaderId);
    }

    private static PartitionDescriptor requirePartition(ClusterMetadata metadata,
                                                        long expectedMetadataEpoch,
                                                        ShardId partitionId,
                                                        long expectedPartitionEpoch) {
        var requiredMetadata = requireArgument(metadata, "metadata");
        var requiredPartitionId = requireArgument(partitionId, "partitionId");
        if (requiredMetadata.epoch() != expectedMetadataEpoch) {
            throw new IllegalArgumentException("metadata epoch does not match expectedMetadataEpoch");
        }
        var partition = requiredMetadata.requirePartition(requiredPartitionId);
        if (partition.epoch() != expectedPartitionEpoch) {
            throw new IllegalArgumentException("partition epoch does not match expectedPartitionEpoch");
        }
        if (partition.isSplitting()) {
            throw new IllegalStateException("partition topology is changing");
        }
        return partition;
    }

    private static PartitionMembershipChangePlan command(MembershipChangeKind kind,
                                                         long expectedMetadataEpoch,
                                                         PartitionDescriptor partition,
                                                         List<ReplicaMember> peers,
                                                         @Nullable NodeId leaderHint) {
        return new PartitionMembershipChangePlan(kind, new MetadataCommand.ChangePartitionPeers(
                expectedMetadataEpoch,
                partition.partitionId(),
                partition.epoch(),
                peers,
                leaderHint
        ));
    }

    private static void requireRegisteredNode(ClusterMetadata metadata, ReplicaMember replica) {
        if (metadata.nodesById().isEmpty()) {
            return;
        }
        var node = metadata.nodesById().get(replica.nodeId());
        if (node == null) {
            throw new IllegalArgumentException("replica node is not registered in cluster metadata: "
                    + replica.nodeId());
        }
        if (!replica.endpoint().equals(node.ratisEndpoint())) {
            throw new IllegalArgumentException("replica endpoint does not match registered node Ratis endpoint: "
                    + replica.nodeId());
        }
    }

    private static ReplicaMember requirePeer(PartitionDescriptor partition, NodeId nodeId) {
        var requiredNodeId = requireArgument(nodeId, "nodeId");
        var peer = findPeer(partition, requiredNodeId);
        if (peer == null) {
            throw new IllegalArgumentException("unknown partition replica: " + requiredNodeId);
        }
        return peer;
    }

    private static ReplicaMember requireLeaderPeer(PartitionDescriptor partition) {
        if (partition.leaderHint() == null) {
            throw new IllegalStateException("partition has no leader hint");
        }
        var leader = findPeer(partition, partition.leaderHint());
        if (leader == null || !leader.isVoter()) {
            throw new IllegalStateException("partition leader hint must reference a voting peer");
        }
        return leader;
    }

    private static ReplicaMember findPeer(PartitionDescriptor partition, NodeId nodeId) {
        return findPeer(partition.peers(), nodeId);
    }

    private static ReplicaMember findPeer(List<ReplicaMember> peers, NodeId nodeId) {
        for (ReplicaMember peer : peers) {
            if (peer.nodeId().equals(nodeId)) {
                return peer;
            }
        }
        return null;
    }

    private static List<ReplicaMember> replacePeer(List<ReplicaMember> peers, ReplicaMember replacement) {
        var replaced = new ArrayList<ReplicaMember>(peers.size());
        for (ReplicaMember peer : peers) {
            if (peer.nodeId().equals(replacement.nodeId())) {
                replaced.add(replacement);
            } else {
                replaced.add(peer);
            }
        }
        return List.copyOf(replaced);
    }

    private static void requireCaughtUp(ReplicaMember candidate, ReplicaMember currentLeader, String message) {
        if (candidate.matchIndex() < currentLeader.matchIndex()
                || candidate.appliedIndex() < currentLeader.appliedIndex()) {
            throw new IllegalStateException(message);
        }
    }

    private static void requireOldQuorumPreserved(List<ReplicaMember> currentPeers, List<ReplicaMember> proposedPeers) {
        var oldVoters = voterCount(currentPeers);
        var proposedVoters = voterCount(proposedPeers);
        var oldQuorum = oldVoters / 2 + 1;
        if (proposedVoters < oldQuorum) {
            throw new IllegalStateException("removing voter would not preserve the current quorum size");
        }
    }

    private static int voterCount(List<ReplicaMember> peers) {
        int count = 0;
        for (ReplicaMember peer : peers) {
            if (peer.isVoter()) {
                count++;
            }
        }
        return count;
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
