package it.cavallium.rockserver.core.replication.ratis;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public record RatisPartitionDescriptor(String partitionId,
                                       String raftGroupId,
                                       String localPeerId,
                                       List<RatisPeerDescriptor> voters,
                                       Path dataPath) {

    public RatisPartitionDescriptor {
        var requiredPartitionId = RatisAdapterBoundary.requireSingleLineNonBlank(partitionId, "partitionId");
        RatisAdapterBoundary.requireSingleLineNonBlank(raftGroupId, "raftGroupId");
        var requiredLocalPeerId = RatisAdapterBoundary.requireSingleLineNonBlank(localPeerId, "localPeerId");
        voters = copyVoters(voters);
        if (voters.isEmpty()) {
            throw new IllegalArgumentException("voters must not be empty");
        }
        var peerIds = new HashSet<String>();
        var endpoints = new HashSet<String>();
        for (var voter : voters) {
            if (!peerIds.add(voter.peerId())) {
                throw new IllegalArgumentException("duplicate voter peerId in partition " + requiredPartitionId
                        + ": " + voter.peerId());
            }
            if (!endpoints.add(voter.endpoint())) {
                throw new IllegalArgumentException("duplicate voter endpoint in partition " + requiredPartitionId
                        + ": " + voter.endpoint());
            }
        }
        if (voters.stream().noneMatch(peer -> peer.peerId().equals(requiredLocalPeerId))) {
            throw new IllegalArgumentException("localPeerId must be present in voters");
        }
        RatisAdapterBoundary.requireSingleLinePath(dataPath, "dataPath");
    }

    private static List<RatisPeerDescriptor> copyVoters(List<RatisPeerDescriptor> voters) {
        if (voters == null) {
            throw new IllegalArgumentException("voters must not be null");
        }
        var copy = new ArrayList<RatisPeerDescriptor>();
        for (var voter : voters) {
            if (voter == null) {
                throw new IllegalArgumentException("voters entry must not be null");
            }
            copy.add(voter);
        }
        return List.copyOf(copy);
    }
}
