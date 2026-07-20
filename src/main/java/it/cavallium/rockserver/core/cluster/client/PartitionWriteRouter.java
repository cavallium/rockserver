package it.cavallium.rockserver.core.cluster.client;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.PartitionBoundaryRules;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.ReplicatedCommandKind;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandValidator;

public final class PartitionWriteRouter {

    private final PartitionRouteCache routeCache;

    public PartitionWriteRouter(PartitionRouteCache routeCache) {
        this.routeCache = requireArgument(routeCache, "routeCache");
    }

    public RouteTarget routeWrite(PartitionWriteCommand command) {
        return routeWriteAtEpoch(routeCache.metadataEpoch(), command);
    }

    public RouteTarget routeWriteAtEpoch(long expectedMetadataEpoch, PartitionWriteCommand command) {
        requireArgument(command, "command");
        var metadata = routeCache.metadata();
        if (expectedMetadataEpoch != metadata.epoch()) {
            throw staleRoute(expectedMetadataEpoch, metadata.epoch());
        }
        var partition = owningPartition(metadata, command);
        PartitionWriteCommandValidator.validate(metadata, partition, command);
        try {
            return routeCache.routeAtEpoch(expectedMetadataEpoch, partition.partitionId(),
                    ReplicatedCommandKind.REPLICATED_WRITE, RoutingPolicy.linearizable());
        } catch (IllegalStateException e) {
            var currentEpoch = routeCache.metadataEpoch();
            if (expectedMetadataEpoch != currentEpoch) {
                throw staleRoute(expectedMetadataEpoch, currentEpoch, e);
            }
            throw routeFailed(e);
        }
    }

    private static PartitionWriteRouteException staleRoute(long expectedMetadataEpoch, long currentMetadataEpoch) {
        return staleRoute(expectedMetadataEpoch, currentMetadataEpoch, null);
    }

    private static PartitionWriteRouteException staleRoute(long expectedMetadataEpoch,
                                                           long currentMetadataEpoch,
                                                           RuntimeException cause) {
        var direction = expectedMetadataEpoch > currentMetadataEpoch ? "future" : "stale";
        return new PartitionWriteRouteException(
                PartitionWriteFailureKind.STALE_ROUTE,
                direction + " route metadata epoch " + expectedMetadataEpoch + "; current epoch is "
                        + currentMetadataEpoch,
                cause
        );
    }

    private static PartitionWriteRouteException routeFailed(RuntimeException cause) {
        return new PartitionWriteRouteException(
                PartitionWriteFailureKind.ROUTE_FAILED,
                "write route failed: " + cause.getMessage(),
                cause
        );
    }

    private static PartitionDescriptor owningPartition(ClusterMetadata metadata, PartitionWriteCommand command) {
        if (command.seal() != null) {
            try {
                return metadata.requirePartition(command.seal().partitionId());
            } catch (IllegalArgumentException | IllegalStateException e) {
                throw routeFailed(e);
            }
        }
        var firstMutation = command.mutations().getFirst();
        var column = metadata.requireColumn(firstMutation.columnId());
        var calculatedKey = PartitionBoundaryRules.routingKey(column.schema(), firstMutation.keys());
        try {
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
