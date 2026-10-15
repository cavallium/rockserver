package it.cavallium.rockserver.core.common.distributed;

import java.util.*;

/** Routing interval [lowerInclusive,upperExclusive); null bounds denote the respective infinities. */
public record RangeDescriptor(UUID rangeId, UUID groupId, long rangeEpoch, long configurationVersion,
                              byte[] lowerInclusive, byte[] upperExclusive, TopologyState topologyState,
                              List<ReplicaDescriptor> replicas) {
    public enum TopologyState { ACTIVE, FENCED, RETIRED }

    public RangeDescriptor {
        Objects.requireNonNull(rangeId, "rangeId");
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(topologyState, "topologyState");
        if (rangeEpoch < 0 || configurationVersion < 0) throw new IllegalArgumentException("Negative range/configuration epoch");
        if (lowerInclusive != null && upperExclusive != null && Arrays.compareUnsigned(lowerInclusive, upperExclusive) >= 0) {
            throw new IllegalArgumentException("Range bounds must increase in unsigned order");
        }
        if ((lowerInclusive != null && lowerInclusive.length > RangeDirectoryCodec.MAX_ENCODED_BYTES)
                || (upperExclusive != null && upperExclusive.length > RangeDirectoryCodec.MAX_ENCODED_BYTES)) {
            throw new IllegalArgumentException("Range bound exceeds directory byte limit");
        }
        lowerInclusive = lowerInclusive == null ? null : lowerInclusive.clone();
        upperExclusive = upperExclusive == null ? null : upperExclusive.clone();
        Objects.requireNonNull(replicas, "replicas");
        if (replicas.isEmpty() || replicas.size() > RangeDirectoryCodec.MAX_REPLICAS) throw new IllegalArgumentException("Invalid replica count");
        var nodes = new HashSet<UUID>();
        var incarnations = new HashSet<UUID>();
        var ids = new HashSet<UUID>();
        var peers = new HashSet<String>();
        var addresses = new HashSet<String>();
        int voters = 0;
        for (ReplicaDescriptor replica : replicas) {
            if (!nodes.add(replica.nodeId()) || !incarnations.add(replica.nodeIncarnation()) || !ids.add(replica.replicaId())
                    || !peers.add(replica.peerId()) || !addresses.add(replica.address())) {
                throw new IllegalArgumentException("Duplicate replica identity/endpoint in range");
            }
            if (replica.role() == ReplicaDescriptor.Role.VOTER) voters++;
        }
        if (voters < 1 || voters > 5) throw new IllegalArgumentException("Range requires 1..5 voters");
        replicas = replicas.stream().sorted(Comparator.comparing(ReplicaDescriptor::replicaId, RangeDirectoryCodec.UUID_ORDER)).toList();
    }

    /** Immutable boundary geometry for admission without cloning the keys. */
    public int boundsBytes() { return (lowerInclusive==null?0:lowerInclusive.length)+(upperExclusive==null?0:upperExclusive.length); }
    @Override public byte[] lowerInclusive() { return lowerInclusive == null ? null : lowerInclusive.clone(); }
    @Override public byte[] upperExclusive() { return upperExclusive == null ? null : upperExclusive.clone(); }
    @Override public boolean equals(Object other) {
        return other instanceof RangeDescriptor range && rangeId.equals(range.rangeId) && groupId.equals(range.groupId)
                && rangeEpoch == range.rangeEpoch && configurationVersion == range.configurationVersion
                && Arrays.equals(lowerInclusive, range.lowerInclusive) && Arrays.equals(upperExclusive, range.upperExclusive)
                && topologyState == range.topologyState && replicas.equals(range.replicas);
    }
    @Override public int hashCode() {
        return 31 * (31 * Objects.hash(rangeId, groupId, rangeEpoch, configurationVersion, topologyState, replicas)
                + Arrays.hashCode(lowerInclusive)) + Arrays.hashCode(upperExclusive);
    }
}
