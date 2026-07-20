package it.cavallium.rockserver.core.cluster.client;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ReadConsistency;
import it.cavallium.rockserver.core.cluster.ReplicaGroupMetadata;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicatedCommandKind;
import it.cavallium.rockserver.core.cluster.ShardId;

import java.util.Comparator;
import java.util.Optional;

public final class ClusterRequestRouter {

    private final ClusterMetadata metadata;

    public ClusterRequestRouter(ClusterMetadata metadata) {
        this.metadata = requireArgument(metadata, "metadata");
    }

    public ClusterMetadata metadata() {
        return metadata;
    }

    public RouteTarget route(ShardId shardId, ReplicatedCommandKind kind, RoutingPolicy policy) {
        requireArgument(shardId, "shardId");
        requireArgument(kind, "kind");
        requireArgument(policy, "policy");

        var group = metadata.requireGroup(shardId);
        return switch (kind) {
            case REPLICATED_WRITE, REPLICATED_METADATA -> routeLeader(group, kind);
            case ROUTED_READ -> routeRead(group, kind, policy);
            case LOCAL_SESSION, LOCAL_ADMIN, CDC, UNSUPPORTED ->
                    throw new IllegalArgumentException("command kind is not cluster-routable: " + kind);
        };
    }

    private RouteTarget routeRead(ReplicaGroupMetadata group, ReplicatedCommandKind kind, RoutingPolicy policy) {
        if (policy.consistency() == ReadConsistency.LINEARIZABLE) {
            return routeLeaderRead(group, kind, policy);
        }

        if (policy.preferFollowers()) {
            var preferred = selectReplica(group, policy, true);
            if (preferred.isPresent()) {
                var member = preferred.get();
                return new RouteTarget(group.shardId(), member, member.nodeId().equals(group.leader()), kind,
                        metadata.epoch());
            }
        }

        var fallback = selectReplica(group, policy, false);
        if (fallback.isPresent()) {
            var member = fallback.get();
            return new RouteTarget(group.shardId(), member, member.nodeId().equals(group.leader()), kind,
                    metadata.epoch());
        }

        throw new IllegalStateException("no replica can satisfy read policy for shard " + group.shardId());
    }

    private RouteTarget routeLeader(ReplicaGroupMetadata group, ReplicatedCommandKind kind) {
        var leader = requireLeader(group);
        return new RouteTarget(group.shardId(), leader, true, kind, metadata.epoch());
    }

    private RouteTarget routeLeaderRead(ReplicaGroupMetadata group,
                                        ReplicatedCommandKind kind,
                                        RoutingPolicy policy) {
        var leader = requireLeader(group);
        if (!leader.servingReads() || leader.appliedIndex() < policy.minAppliedIndex()) {
            throw new IllegalStateException("leader cannot satisfy read policy for shard " + group.shardId());
        }
        return new RouteTarget(group.shardId(), leader, true, kind, metadata.epoch());
    }

    private static ReplicaMember requireLeader(ReplicaGroupMetadata group) {
        return group.leaderMember()
                .orElseThrow(() -> new IllegalStateException("no known leader for shard " + group.shardId()));
    }

    private Optional<ReplicaMember> selectReplica(ReplicaGroupMetadata group,
                                                  RoutingPolicy policy,
                                                  boolean followersOnly) {
        var leader = group.leader();
        return group.members()
                .stream()
                .filter(ReplicaMember::servingReads)
                .filter(member -> member.appliedIndex() >= policy.minAppliedIndex())
                .filter(member -> !followersOnly || !member.nodeId().equals(leader))
                .max(Comparator
                        .comparingLong(ReplicaMember::appliedIndex)
                        .thenComparingLong(ReplicaMember::matchIndex)
                        .thenComparing(member -> member.nodeId().value()));
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
