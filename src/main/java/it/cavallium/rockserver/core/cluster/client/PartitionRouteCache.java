package it.cavallium.rockserver.core.cluster.client;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ReplicatedCommandKind;
import it.cavallium.rockserver.core.cluster.ShardId;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public final class PartitionRouteCache {

    private ClusterMetadata metadata;
    private ClusterRequestRouter router;
    private final Map<RouteCacheKey, RouteTarget> routes = new HashMap<>();

    public PartitionRouteCache(ClusterMetadata metadata) {
        updateMetadata(metadata);
    }

    public synchronized ClusterMetadata metadata() {
        return metadata;
    }

    public synchronized long metadataEpoch() {
        return metadata.epoch();
    }

    public synchronized void updateMetadata(ClusterMetadata newMetadata) {
        updateMetadata(newMetadata, ignored -> {
        });
    }

    public synchronized void updateMetadata(ClusterMetadata newMetadata,
                                            Consumer<ClusterMetadata> beforeCommit) {
        var requiredMetadata = requireArgument(newMetadata, "newMetadata");
        var requiredBeforeCommit = requireArgument(beforeCommit, "beforeCommit");
        if (metadata != null) {
            if (!metadata.clusterId().equals(requiredMetadata.clusterId())) {
                throw new IllegalArgumentException("route cache cannot switch cluster id from "
                        + metadata.clusterId() + " to " + requiredMetadata.clusterId());
            }
            if (requiredMetadata.epoch() < metadata.epoch()) {
                throw new IllegalArgumentException("route cache metadata epoch regression from "
                        + metadata.epoch() + " to " + requiredMetadata.epoch());
            }
            if (metadata.equals(requiredMetadata)) {
                return;
            }
        }
        var keepCachedRoutes = metadata != null && metadata.epoch() == requiredMetadata.epoch();
        if (keepCachedRoutes && !routingDataEquals(metadata, requiredMetadata)) {
            throw new IllegalArgumentException(
                    "route cache same-epoch metadata refresh cannot change partition routing data");
        }

        requiredBeforeCommit.accept(requiredMetadata);
        metadata = requiredMetadata;
        if (!keepCachedRoutes) {
            router = new ClusterRequestRouter(requiredMetadata);
            routes.clear();
        }
    }

    public synchronized RouteTarget route(ShardId shardId,
                                          ReplicatedCommandKind kind,
                                          RoutingPolicy policy) {
        return routeAtEpoch(metadata.epoch(), shardId, kind, policy);
    }

    public synchronized RouteTarget routeAtEpoch(long expectedMetadataEpoch,
                                                 ShardId shardId,
                                                 ReplicatedCommandKind kind,
                                                 RoutingPolicy policy) {
        var requestedShardId = requireArgument(shardId, "shardId");
        var requestedKind = requireArgument(kind, "kind");
        var requestedPolicy = requireArgument(policy, "policy");
        if (expectedMetadataEpoch != metadata.epoch()) {
            throw new IllegalStateException("route cache metadata epoch mismatch: expected "
                    + expectedMetadataEpoch + " but current epoch is " + metadata.epoch());
        }
        var key = new RouteCacheKey(requestedShardId, requestedKind, requestedPolicy);
        var cached = routes.get(key);
        if (cached != null && cached.metadataEpoch() == expectedMetadataEpoch) {
            return cached;
        }
        var target = router.route(requestedShardId, requestedKind, requestedPolicy);
        routes.put(key, target);
        return target;
    }

    public synchronized void invalidate(ShardId shardId) {
        var invalidatedShardId = requireArgument(shardId, "shardId");
        routes.keySet().removeIf(key -> key.shardId().equals(invalidatedShardId));
    }

    public synchronized void clear() {
        routes.clear();
    }

    private static boolean routingDataEquals(ClusterMetadata left, ClusterMetadata right) {
        return left.shards().equals(right.shards())
                && left.partitionsById().equals(right.partitionsById());
    }

    private record RouteCacheKey(ShardId shardId,
                                 ReplicatedCommandKind kind,
                                 RoutingPolicy policy) {

        private RouteCacheKey {
            requireArgument(shardId, "shardId");
            requireArgument(kind, "kind");
            requireArgument(policy, "policy");
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
