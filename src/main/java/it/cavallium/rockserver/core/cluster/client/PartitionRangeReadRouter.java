package it.cavallium.rockserver.core.cluster.client;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.PartitionBoundaryRules;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.ReplicatedCommandKind;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommand;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jetbrains.annotations.Nullable;

public final class PartitionRangeReadRouter {

    private final PartitionRouteCache routeCache;

    public PartitionRangeReadRouter(PartitionRouteCache routeCache) {
        this.routeCache = requireArgument(routeCache, "routeCache");
    }

    public RouteTarget routeLinearizableScan(PartitionRangeReadCommand command) {
        return routeLinearizableScanAtEpoch(routeCache.metadataEpoch(), command);
    }

    public RouteTarget routeLinearizableScanAtEpoch(long expectedMetadataEpoch,
                                                    PartitionRangeReadCommand command) {
        requireArgument(command, "command");
        var metadata = metadataAtEpoch(expectedMetadataEpoch);
        var partition = owningPartition(metadata, command);
        return routeAtEpoch(expectedMetadataEpoch, partition.partitionId());
    }

    public List<PartitionRangeReadRoute> routeLinearizableScanSegmentsAtEpoch(long expectedMetadataEpoch,
                                                                              PartitionRangeReadCommand command) {
        requireArgument(command, "command");
        var metadata = metadataAtEpoch(expectedMetadataEpoch);
        var partitions = owningPartitions(metadata, command);
        var routes = new ArrayList<PartitionRangeReadRoute>(partitions.size());
        for (int i = 0; i < partitions.size(); i++) {
            var partition = partitions.get(i);
            var target = routeAtEpoch(expectedMetadataEpoch, partition.partitionId());
            routes.add(new PartitionRangeReadRoute(target, segmentCommand(command, i, partitions.size() - 1)));
        }
        if (command.reverse()) {
            Collections.reverse(routes);
        }
        return List.copyOf(routes);
    }

    private ClusterMetadata metadataAtEpoch(long expectedMetadataEpoch) {
        var metadata = routeCache.metadata();
        if (expectedMetadataEpoch != metadata.epoch()) {
            throw staleRoute(expectedMetadataEpoch, metadata.epoch());
        }
        return metadata;
    }

    private RouteTarget routeAtEpoch(long expectedMetadataEpoch, ShardId partitionId) {
        try {
            return routeCache.routeAtEpoch(expectedMetadataEpoch,
                    partitionId,
                    ReplicatedCommandKind.ROUTED_READ,
                    RoutingPolicy.linearizable());
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
                direction + " range read route metadata epoch " + expectedMetadataEpoch + "; current epoch is "
                        + currentMetadataEpoch,
                cause
        );
    }

    private static PartitionReadRouteException routeFailed(RuntimeException cause) {
        return new PartitionReadRouteException(
                PartitionReadFailureKind.QUERY_FAILED,
                "range read route failed: " + cause.getMessage(),
                cause
        );
    }

    private static PartitionDescriptor owningPartition(ClusterMetadata metadata,
                                                       PartitionRangeReadCommand command) {
        try {
            var column = requireMatchingColumn(metadata, command);
            var startKeyInclusive = calculatedBound(column.schema(), command.startKeysInclusive());
            var endKeyExclusive = calculatedBound(column.schema(), command.endKeysExclusive());
            return PartitionBoundaryRules.requireSinglePartitionRange(metadata.partitionDescriptors(),
                    startKeyInclusive,
                    endKeyExclusive);
        } catch (IllegalArgumentException e) {
            if (isMalformedRange(e)) {
                throw e;
            }
            throw routeFailed(e);
        } catch (IllegalStateException e) {
            throw routeFailed(e);
        }
    }

    private static List<PartitionDescriptor> owningPartitions(ClusterMetadata metadata,
                                                              PartitionRangeReadCommand command) {
        try {
            var column = requireMatchingColumn(metadata, command);
            var startKeyInclusive = calculatedBound(column.schema(), command.startKeysInclusive());
            var endKeyExclusive = calculatedBound(column.schema(), command.endKeysExclusive());
            return PartitionBoundaryRules.requirePartitionRange(metadata.partitionDescriptors(),
                    startKeyInclusive,
                    endKeyExclusive);
        } catch (IllegalArgumentException e) {
            if (isMalformedRange(e)) {
                throw e;
            }
            throw routeFailed(e);
        } catch (IllegalStateException e) {
            throw routeFailed(e);
        }
    }

    private static boolean isMalformedRange(IllegalArgumentException failure) {
        return "range start key must be lower than end key".equals(failure.getMessage());
    }

    private static ClusterColumnMetadata requireMatchingColumn(ClusterMetadata metadata,
                                                               PartitionRangeReadCommand command) {
        var column = metadata.requireColumn(command.columnId());
        if (column.schemaEpoch() != command.schemaEpoch()) {
            throw new IllegalArgumentException("range read schema epoch " + command.schemaEpoch()
                    + " does not match column schema epoch " + column.schemaEpoch());
        }
        return column;
    }

    private static PartitionRangeReadCommand segmentCommand(PartitionRangeReadCommand command,
                                                            int partitionIndex,
                                                            int lastPartitionIndex) {
        return new PartitionRangeReadCommand(
                command.schemaEpoch(),
                command.columnId(),
                partitionIndex == 0 ? command.startKeysInclusive() : null,
                partitionIndex == lastPartitionIndex ? command.endKeysExclusive() : null,
                command.reverse(),
                command.maxEntries()
        );
    }

    private static @Nullable Buf calculatedBound(ColumnSchema schema, @Nullable Keys keys) {
        if (keys == null || keys.keys().length == 0) {
            return null;
        }
        return PartitionBoundaryRules.routingKey(schema, keys);
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
