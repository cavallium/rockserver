package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.SingleLineStrings;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public record BootstrapManifest(ClusterId clusterId,
                                String metadataGroupId,
                                List<BootstrapPeer> initialPeers,
                                long bootstrapEpoch) {

    public BootstrapManifest {
        requireArgument(clusterId, "clusterId");
        SingleLineStrings.requireNonBlank(metadataGroupId, "metadataGroupId");
        initialPeers = copyInitialPeers(initialPeers);
        if (initialPeers.isEmpty()) {
            throw new IllegalArgumentException("bootstrap manifest must contain at least one peer");
        }
        if (bootstrapEpoch < 0) {
            throw new IllegalArgumentException("bootstrapEpoch must be >= 0");
        }

        var nodeIds = new HashSet<NodeId>();
        var publicEndpoints = new HashSet<URI>();
        var ratisEndpoints = new HashSet<URI>();
        for (BootstrapPeer peer : initialPeers) {
            if (!nodeIds.add(peer.nodeId())) {
                throw new IllegalArgumentException("duplicate bootstrap node id: " + peer.nodeId());
            }
            if (!publicEndpoints.add(peer.publicEndpoint())) {
                throw new IllegalArgumentException("duplicate bootstrap public endpoint: " + peer.publicEndpoint());
            }
            if (!ratisEndpoints.add(peer.ratisEndpoint())) {
                throw new IllegalArgumentException("duplicate bootstrap Ratis endpoint: " + peer.ratisEndpoint());
            }
        }
    }

    public Optional<BootstrapPeer> findPeer(NodeId nodeId) {
        var resolvedNodeId = requireArgument(nodeId, "nodeId");
        return initialPeers.stream()
                .filter(peer -> peer.nodeId().equals(resolvedNodeId))
                .findFirst();
    }

    public Set<NodeId> peerIds() {
        var peerIds = new HashSet<NodeId>();
        for (BootstrapPeer peer : initialPeers) {
            peerIds.add(peer.nodeId());
        }
        return Set.copyOf(peerIds);
    }

    public List<ReplicaMember> metadataGroupMembers() {
        return initialPeers.stream()
                .map(peer -> new ReplicaMember(peer.nodeId(), peer.ratisEndpoint(), ReplicaRole.VOTER, 0, 0, true))
                .toList();
    }

    private static List<BootstrapPeer> copyInitialPeers(List<BootstrapPeer> initialPeers) {
        var requiredInitialPeers = requireArgument(initialPeers, "initialPeers");
        for (var peer : requiredInitialPeers) {
            requireArgument(peer, "initialPeers entry");
        }
        return List.copyOf(requiredInitialPeers);
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
