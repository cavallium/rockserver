package it.cavallium.rockserver.core.cluster;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.jetbrains.annotations.Nullable;

/**
 * Partition ranges are expressed only in calculated-key bytes. For bucketed columns, variable-tail keys are
 * represented by their configured hashes, so partition scans follow calculated-hash ordering rather than raw
 * variable-tail-key ordering.
 */
public final class PartitionBoundaryRules {

    private PartitionBoundaryRules() {
    }

    public static Buf routingKey(ColumnSchema schema, Keys keys) {
        return RockserverKeyEncoder.calculateKey(schema, keys);
    }

    public static Optional<PartitionDescriptor> findPartition(Collection<PartitionDescriptor> partitions,
                                                              Buf calculatedKey) {
        var requiredPartitions = requireArgument(partitions, "partitions");
        var requiredCalculatedKey = requireArgument(calculatedKey, "calculatedKey");
        PartitionDescriptor match = null;
        for (PartitionDescriptor partition : requiredPartitions) {
            var requiredPartition = requireArgument(partition, "partition");
            if (requiredPartition.containsCalculatedKey(requiredCalculatedKey)) {
                if (match != null) {
                    throw new IllegalStateException("calculated key matches multiple partitions");
                }
                match = requiredPartition;
            }
        }
        return Optional.ofNullable(match);
    }

    public static PartitionDescriptor requirePartition(Collection<PartitionDescriptor> partitions, Buf calculatedKey) {
        return findPartition(partitions, calculatedKey)
                .orElseThrow(() -> new IllegalArgumentException("no partition owns calculated key"));
    }

    public static PartitionDescriptor requireSinglePartitionRange(Collection<PartitionDescriptor> partitions,
                                                                  @Nullable Buf startKeyInclusive,
                                                                  @Nullable Buf endKeyExclusive) {
        var requiredPartitions = requireArgument(partitions, "partitions");
        validateRangeBounds(startKeyInclusive, endKeyExclusive);

        PartitionDescriptor match = null;
        for (PartitionDescriptor partition : requiredPartitions) {
            var requiredPartition = requireArgument(partition, "partition");
            if (rangesIntersect(startKeyInclusive,
                    endKeyExclusive,
                    requiredPartition.startKeyInclusive(),
                    requiredPartition.endKeyExclusive())) {
                requireActiveForRangeScan(requiredPartition);
                if (match != null) {
                    throw new IllegalArgumentException("calculated key range spans multiple partitions");
                }
                match = requiredPartition;
            }
        }
        if (match == null) {
            throw new IllegalArgumentException("no partition owns calculated key range");
        }
        if (!rangeContainedBy(match, startKeyInclusive, endKeyExclusive)) {
            throw new IllegalArgumentException("calculated key range is not fully contained in partition "
                    + match.partitionId());
        }
        return match;
    }

    public static List<PartitionDescriptor> requirePartitionRange(Collection<PartitionDescriptor> partitions,
                                                                  @Nullable Buf startKeyInclusive,
                                                                  @Nullable Buf endKeyExclusive) {
        var requiredPartitions = requireArgument(partitions, "partitions");
        validateRangeBounds(startKeyInclusive, endKeyExclusive);

        var matches = new ArrayList<PartitionDescriptor>();
        for (PartitionDescriptor partition : requiredPartitions) {
            var requiredPartition = requireArgument(partition, "partition");
            if (rangesIntersect(startKeyInclusive,
                    endKeyExclusive,
                    requiredPartition.startKeyInclusive(),
                    requiredPartition.endKeyExclusive())) {
                requireActiveForRangeScan(requiredPartition);
                matches.add(requiredPartition);
            }
        }
        if (matches.isEmpty()) {
            throw new IllegalArgumentException("no partition owns calculated key range");
        }
        matches.sort(PartitionBoundaryRules::comparePartitionStarts);
        validatePartitionRangeCoverage(matches, startKeyInclusive, endKeyExclusive);
        return List.copyOf(matches);
    }

    public static void validateSplitBoundary(PartitionDescriptor source, Buf splitKey) {
        var requiredSource = requireArgument(source, "source");
        var requiredSplitKey = requireArgument(splitKey, "splitKey");
        if (requiredSource.startKeyInclusive() != null
                && RockserverKeyEncoder.compareCalculatedKeys(requiredSplitKey, requiredSource.startKeyInclusive()) <= 0) {
            throw new IllegalArgumentException("split key must be greater than the source partition start key");
        }
        if (requiredSource.endKeyExclusive() != null
                && RockserverKeyEncoder.compareCalculatedKeys(requiredSplitKey, requiredSource.endKeyExclusive()) >= 0) {
            throw new IllegalArgumentException("split key must be lower than the source partition end key");
        }
    }

    private static void validateRangeBounds(@Nullable Buf startKeyInclusive, @Nullable Buf endKeyExclusive) {
        if (startKeyInclusive != null
                && endKeyExclusive != null
                && RockserverKeyEncoder.compareCalculatedKeys(startKeyInclusive, endKeyExclusive) >= 0) {
            throw new IllegalArgumentException("range start key must be lower than end key");
        }
    }

    private static void requireActiveForRangeScan(PartitionDescriptor partition) {
        if (partition.isSplitting()) {
            throw new IllegalStateException("partition " + partition.partitionId()
                    + " is not active for range scans: " + partition.splitState());
        }
    }

    private static void validatePartitionRangeCoverage(List<PartitionDescriptor> partitions,
                                                       @Nullable Buf startKeyInclusive,
                                                       @Nullable Buf endKeyExclusive) {
        var first = partitions.getFirst();
        if (!lowerBoundContainedBy(first, startKeyInclusive)) {
            throw new IllegalArgumentException("calculated key range starts before first matched partition "
                    + first.partitionId());
        }
        var last = partitions.getLast();
        if (!upperBoundContainedBy(last, endKeyExclusive)) {
            throw new IllegalArgumentException("calculated key range extends past last matched partition "
                    + last.partitionId());
        }
        for (int i = 1; i < partitions.size(); i++) {
            var previous = partitions.get(i - 1);
            var current = partitions.get(i);
            if (!partitionEndEqualsNextStart(previous, current)) {
                throw new IllegalArgumentException("calculated key range crosses non-contiguous partitions "
                        + previous.partitionId() + " and " + current.partitionId());
            }
        }
    }

    private static boolean partitionEndEqualsNextStart(PartitionDescriptor previous, PartitionDescriptor current) {
        return previous.endKeyExclusive() != null
                && current.startKeyInclusive() != null
                && RockserverKeyEncoder.compareCalculatedKeys(previous.endKeyExclusive(),
                current.startKeyInclusive()) == 0;
    }

    private static int comparePartitionStarts(PartitionDescriptor left, PartitionDescriptor right) {
        return compareNullableLowerBound(left.startKeyInclusive(), right.startKeyInclusive());
    }

    private static int compareNullableLowerBound(@Nullable Buf left, @Nullable Buf right) {
        if (left == null) {
            return right == null ? 0 : -1;
        }
        if (right == null) {
            return 1;
        }
        return RockserverKeyEncoder.compareCalculatedKeys(left, right);
    }

    private static boolean rangesIntersect(@Nullable Buf leftStartInclusive,
                                           @Nullable Buf leftEndExclusive,
                                           @Nullable Buf rightStartInclusive,
                                           @Nullable Buf rightEndExclusive) {
        return lowerLessThanUpper(leftStartInclusive, rightEndExclusive)
                && lowerLessThanUpper(rightStartInclusive, leftEndExclusive);
    }

    private static boolean rangeContainedBy(PartitionDescriptor partition,
                                            @Nullable Buf startKeyInclusive,
                                            @Nullable Buf endKeyExclusive) {
        return lowerBoundContainedBy(partition, startKeyInclusive)
                && upperBoundContainedBy(partition, endKeyExclusive);
    }

    private static boolean lowerBoundContainedBy(PartitionDescriptor partition, @Nullable Buf startKeyInclusive) {
        if (startKeyInclusive == null) {
            return partition.startKeyInclusive() == null;
        }
        return partition.startKeyInclusive() == null
                || RockserverKeyEncoder.compareCalculatedKeys(startKeyInclusive, partition.startKeyInclusive()) >= 0;
    }

    private static boolean upperBoundContainedBy(PartitionDescriptor partition, @Nullable Buf endKeyExclusive) {
        if (endKeyExclusive == null) {
            return partition.endKeyExclusive() == null;
        }
        return partition.endKeyExclusive() == null
                || RockserverKeyEncoder.compareCalculatedKeys(endKeyExclusive, partition.endKeyExclusive()) <= 0;
    }

    private static boolean lowerLessThanUpper(@Nullable Buf lowerInclusive, @Nullable Buf upperExclusive) {
        return lowerInclusive == null
                || upperExclusive == null
                || RockserverKeyEncoder.compareCalculatedKeys(lowerInclusive, upperExclusive) < 0;
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
