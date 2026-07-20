package it.cavallium.rockserver.core.cluster;

import it.cavallium.buffer.Buf;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.Nullable;

public record PartitionDescriptor(ShardId partitionId,
                                  @Nullable Buf startKeyInclusive,
                                  @Nullable Buf endKeyExclusive,
                                  long epoch,
                                  String ratisGroupId,
                                  List<ReplicaMember> peers,
                                  @Nullable NodeId leaderHint,
                                  Map<String, String> placementConstraints,
                                  PartitionSplitState splitState) {

    public PartitionDescriptor {
        requireArgument(partitionId, "partitionId");
        if (epoch < 0) {
            throw new IllegalArgumentException("epoch must be >= 0");
        }
        SingleLineStrings.requireNonBlank(ratisGroupId, "ratisGroupId");
        requireArgument(peers, "peers");
        if (peers.isEmpty()) {
            throw new IllegalArgumentException("partition must contain at least one peer");
        }
        peers = List.copyOf(peers);
        validateUniquePeerNodeIds(peers);
        placementConstraints = validatePlacementConstraints(placementConstraints);
        requireArgument(splitState, "splitState");
        startKeyInclusive = freezeBoundary(startKeyInclusive);
        endKeyExclusive = freezeBoundary(endKeyExclusive);
        validateRange(startKeyInclusive, endKeyExclusive);
        validateLeaderHint(leaderHint, peers);
    }

    public boolean containsCalculatedKey(Buf calculatedKey) {
        requireArgument(calculatedKey, "calculatedKey");
        if (startKeyInclusive != null
                && RockserverKeyEncoder.compareCalculatedKeys(calculatedKey, startKeyInclusive) < 0) {
            return false;
        }
        return endKeyExclusive == null
                || RockserverKeyEncoder.compareCalculatedKeys(calculatedKey, endKeyExclusive) < 0;
    }

    public boolean isSplitting() {
        return splitState != PartitionSplitState.ACTIVE;
    }

    private static @Nullable Buf freezeBoundary(@Nullable Buf boundary) {
        return boundary == null ? null : Buf.wrap(boundary.toByteArray()).freeze();
    }

    private static void validateRange(@Nullable Buf startKeyInclusive, @Nullable Buf endKeyExclusive) {
        if (startKeyInclusive != null
                && endKeyExclusive != null
                && RockserverKeyEncoder.compareCalculatedKeys(startKeyInclusive, endKeyExclusive) >= 0) {
            throw new IllegalArgumentException("partition start key must be lower than end key");
        }
    }

    private static void validateLeaderHint(@Nullable NodeId leaderHint, List<ReplicaMember> peers) {
        if (leaderHint == null) {
            return;
        }
        for (ReplicaMember peer : peers) {
            if (peer.nodeId().equals(leaderHint) && peer.isVoter()) {
                return;
            }
        }
        throw new IllegalArgumentException("leader hint must reference a voting partition peer: " + leaderHint);
    }

    private static void validateUniquePeerNodeIds(List<ReplicaMember> peers) {
        var nodeIds = new HashSet<NodeId>();
        for (ReplicaMember peer : peers) {
            if (!nodeIds.add(peer.nodeId())) {
                throw new IllegalArgumentException("duplicate node in partition peers: " + peer.nodeId());
            }
        }
    }

    private static Map<String, String> validatePlacementConstraints(Map<String, String> placementConstraints) {
        requireArgument(placementConstraints, "placementConstraints");
        var validated = new LinkedHashMap<String, String>(placementConstraints.size());
        for (var entry : placementConstraints.entrySet()) {
            var key = SingleLineStrings.requireNonBlank(entry.getKey(), "placement constraint key");
            var value = SingleLineStrings.requireNonBlank(entry.getValue(), "placement constraint value");
            validated.put(key, value);
        }
        return Map.copyOf(validated);
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
