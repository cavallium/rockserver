package it.cavallium.rockserver.core.cluster;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;

public record ReplicaGroupMetadata(ShardId shardId,
                                   long term,
                                   NodeId leader,
                                   List<ReplicaMember> members,
                                   long configIndex) {

    public ReplicaGroupMetadata {
        requireArgument(shardId, "shardId");
        requireArgument(members, "members");
        if (term < 0) {
            throw new IllegalArgumentException("term must be >= 0");
        }
        if (configIndex < 0) {
            throw new IllegalArgumentException("configIndex must be >= 0");
        }
        members = List.copyOf(members);
        if (members.isEmpty()) {
            throw new IllegalArgumentException("replica group must contain at least one member");
        }

        var nodeIds = new HashSet<NodeId>();
        boolean hasVoter = false;
        boolean hasLeader = leader == null;
        for (ReplicaMember member : members) {
            if (!nodeIds.add(member.nodeId())) {
                throw new IllegalArgumentException("duplicate node in replica group: " + member.nodeId());
            }
            hasVoter |= member.isVoter();
            hasLeader |= member.nodeId().equals(leader) && member.isVoter();
        }
        if (!hasVoter) {
            throw new IllegalArgumentException("replica group must contain at least one voter");
        }
        if (!hasLeader) {
            throw new IllegalArgumentException("leader must be a voting member of the replica group");
        }
    }

    public int voterCount() {
        int voters = 0;
        for (ReplicaMember member : members) {
            if (member.isVoter()) {
                voters++;
            }
        }
        return voters;
    }

    public int quorumSize() {
        return (voterCount() / 2) + 1;
    }

    public boolean hasQuorum(int acknowledgedVoters) {
        return acknowledgedVoters >= quorumSize();
    }

    public Optional<ReplicaMember> leaderMember() {
        if (leader == null) {
            return Optional.empty();
        }
        return members.stream().filter(member -> member.nodeId().equals(leader)).findFirst();
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
