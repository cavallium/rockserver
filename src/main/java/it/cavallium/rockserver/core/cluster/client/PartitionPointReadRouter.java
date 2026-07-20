package it.cavallium.rockserver.core.cluster.client;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.PartitionBoundaryRules;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.ReplicatedCommandKind;
import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommandValidator;

public final class PartitionPointReadRouter {

    private final PartitionRouteCache routeCache;

    public PartitionPointReadRouter(PartitionRouteCache routeCache) {
        this.routeCache = requireArgument(routeCache, "routeCache");
    }

    public RouteTarget routeLinearizableRead(PartitionPointReadCommand command) {
        return routeLinearizableReadAtEpoch(routeCache.metadataEpoch(), command);
    }

    public RouteTarget routeLinearizableReadAtEpoch(long expectedMetadataEpoch,
                                                    PartitionPointReadCommand command) {
        return routeReadAtEpoch(expectedMetadataEpoch, command, RoutingPolicy.linearizable());
    }

    public RouteTarget routeReadAtEpoch(long expectedMetadataEpoch,
                                        PartitionPointReadCommand command,
                                        RoutingPolicy policy) {
        requireArgument(command, "command");
        requireArgument(policy, "policy");
        var metadata = routeCache.metadata();
        if (expectedMetadataEpoch != metadata.epoch()) {
            throw staleRoute(expectedMetadataEpoch, metadata.epoch());
        }
        var partition = owningPartition(metadata, command);
        try {
            return routeCache.routeAtEpoch(expectedMetadataEpoch, partition.partitionId(),
                    ReplicatedCommandKind.ROUTED_READ, policy);
        } catch (IllegalStateException e) {
            var currentEpoch = routeCache.metadataEpoch();
            if (expectedMetadataEpoch != currentEpoch) {
                throw staleRoute(expectedMetadataEpoch, currentEpoch, e);
            }
            throw routeFailed(e);
        }
    }

    private static PartitionReadRouteException staleRoute(long expectedMetadataEpoch, long currentMetadataEpoch) {
        return staleRoute(expectedMetadataEpoch, currentMetadataEpoch, null);
    }

    private static PartitionReadRouteException staleRoute(long expectedMetadataEpoch,
                                                          long currentMetadataEpoch,
                                                          RuntimeException cause) {
        var direction = expectedMetadataEpoch > currentMetadataEpoch ? "future" : "stale";
        return new PartitionReadRouteException(
                PartitionReadFailureKind.STALE_ROUTE,
                direction + " read route metadata epoch " + expectedMetadataEpoch + "; current epoch is "
                        + currentMetadataEpoch,
                cause
        );
    }

    private static PartitionReadRouteException routeFailed(RuntimeException cause) {
        return new PartitionReadRouteException(
                PartitionReadFailureKind.QUERY_FAILED,
                "read route failed: " + cause.getMessage(),
                cause
        );
    }

    private static PartitionDescriptor owningPartition(ClusterMetadata metadata,
                                                       PartitionPointReadCommand command) {
        try {
            var calculatedKey = PartitionPointReadCommandValidator.calculatedKey(metadata, command);
            return PartitionBoundaryRules.requirePartition(metadata.partitionDescriptors(), calculatedKey);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw routeFailed(e);
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
