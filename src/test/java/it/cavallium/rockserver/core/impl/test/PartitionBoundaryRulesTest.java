package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionBoundaryRules;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.RockserverKeyEncoder;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class PartitionBoundaryRulesTest {

    @Test
    @SuppressWarnings("DataFlowIssue")
    void rejectsNullInputsWithExplicitDiagnostics() {
        var partition = partition(1, null, null);
        var partitions = List.of(partition);

        assertIllegalArgument("partitions must not be null",
                () -> PartitionBoundaryRules.findPartition(null, buf(0x10)));
        assertIllegalArgument("calculatedKey must not be null",
                () -> PartitionBoundaryRules.findPartition(partitions, null));
        assertIllegalArgument("partitions must not be null",
                () -> PartitionBoundaryRules.requirePartition(null, buf(0x10)));
        assertIllegalArgument("calculatedKey must not be null",
                () -> PartitionBoundaryRules.requirePartition(partitions, null));
        assertIllegalArgument("partitions must not be null",
                () -> PartitionBoundaryRules.requireSinglePartitionRange(null, null, null));
        assertIllegalArgument("partition must not be null",
                () -> PartitionBoundaryRules.requireSinglePartitionRange(Collections.singletonList(null), null, null));
        assertIllegalArgument("partitions must not be null",
                () -> PartitionBoundaryRules.requirePartitionRange(null, null, null));
        assertIllegalArgument("partition must not be null",
                () -> PartitionBoundaryRules.requirePartitionRange(Collections.singletonList(null), null, null));
        assertIllegalArgument("source must not be null",
                () -> PartitionBoundaryRules.validateSplitBoundary(null, buf(0x10)));
        assertIllegalArgument("splitKey must not be null",
                () -> PartitionBoundaryRules.validateSplitBoundary(partition, null));
    }

    @Test
    void looksUpPartitionsByHalfOpenCalculatedKeyRanges() {
        var first = partition(1, null, buf(0x10));
        var second = partition(2, buf(0x10), buf(0x20));
        var third = partition(3, buf(0x20), null);
        var partitions = List.of(first, second, third);

        assertEquals(first, PartitionBoundaryRules.requirePartition(partitions, buf(0x0f)));
        assertEquals(second, PartitionBoundaryRules.requirePartition(partitions, buf(0x10)));
        assertEquals(second, PartitionBoundaryRules.requirePartition(partitions, buf(0x1f)));
        assertEquals(third, PartitionBoundaryRules.requirePartition(partitions, buf(0x20)));
    }

    @Test
    void rejectsOverlappingPartitionLookup() {
        var partitions = List.of(partition(1, null, buf(0x20)), partition(2, buf(0x10), null));

        assertThrows(IllegalStateException.class, () -> PartitionBoundaryRules.findPartition(partitions, buf(0x10)));
    }

    @Test
    void requiresSinglePartitionRangeInsideHalfOpenBoundaries() {
        var first = partition(1, null, buf(0x10));
        var second = partition(2, buf(0x10), buf(0x20));
        var third = partition(3, buf(0x20), null);
        var partitions = List.of(first, second, third);

        assertEquals(first, PartitionBoundaryRules.requireSinglePartitionRange(partitions, null, buf(0x10)));
        assertEquals(second, PartitionBoundaryRules.requireSinglePartitionRange(partitions, buf(0x10), buf(0x20)));
        assertEquals(third, PartitionBoundaryRules.requireSinglePartitionRange(partitions, buf(0x20), null));
    }

    @Test
    void rejectsRangeSpanningMultiplePartitions() {
        var partitions = List.of(partition(1, null, buf(0x10)),
                partition(2, buf(0x10), buf(0x20)),
                partition(3, buf(0x20), null));

        assertThrows(IllegalArgumentException.class,
                () -> PartitionBoundaryRules.requireSinglePartitionRange(partitions, buf(0x0f), buf(0x11)));
        assertThrows(IllegalArgumentException.class,
                () -> PartitionBoundaryRules.requireSinglePartitionRange(partitions, buf(0x18), null));
        assertThrows(IllegalArgumentException.class,
                () -> PartitionBoundaryRules.requireSinglePartitionRange(partitions, buf(0x10), buf(0x10)));
    }

    @Test
    void requiresPartitionRangeCoverageAcrossContiguousPartitions() {
        var first = partition(1, null, buf(0x10));
        var second = partition(2, buf(0x10), buf(0x20));
        var third = partition(3, buf(0x20), null);
        var partitions = List.of(second, third, first);

        assertEquals(List.of(first, second),
                PartitionBoundaryRules.requirePartitionRange(partitions, buf(0x0f), buf(0x18)));
        assertEquals(List.of(second, third),
                PartitionBoundaryRules.requirePartitionRange(partitions, buf(0x18), null));
    }

    @Test
    void rejectsPartitionRangeWithCoverageGap() {
        var partitions = List.of(partition(1, null, buf(0x10)), partition(2, buf(0x20), null));

        assertThrows(IllegalArgumentException.class,
                () -> PartitionBoundaryRules.requirePartitionRange(partitions, buf(0x0f), buf(0x28)));
    }

    @Test
    void rejectsRangeScanTouchingSplittingPartition() {
        var active = partition(1, null, buf(0x10));
        var splitting = partition(2, buf(0x10), null, PartitionSplitState.SPLIT_SOURCE);
        var partitions = List.of(active, splitting);

        assertThrows(IllegalStateException.class,
                () -> PartitionBoundaryRules.requireSinglePartitionRange(partitions, buf(0x10), buf(0x18)));
        assertThrows(IllegalStateException.class,
                () -> PartitionBoundaryRules.requirePartitionRange(partitions, buf(0x08), buf(0x18)));
    }

    @Test
    void validatesSplitBoundaryInsideSourceRange() {
        var source = partition(1, buf(0x10), buf(0x20));

        PartitionBoundaryRules.validateSplitBoundary(source, buf(0x18));

        assertThrows(IllegalArgumentException.class,
                () -> PartitionBoundaryRules.validateSplitBoundary(source, buf(0x10)));
        assertThrows(IllegalArgumentException.class,
                () -> PartitionBoundaryRules.validateSplitBoundary(source, buf(0x20)));
        assertThrows(IllegalArgumentException.class,
                () -> PartitionBoundaryRules.validateSplitBoundary(source, buf(0x08)));
    }

    @Test
    void routesBucketedCollisionsByCalculatedKeyNotBucketElementKey() {
        var schema = ColumnSchema.of(IntArrayList.of(1), ObjectArrayList.of(ColumnHashType.ALLSAME8), true);
        var first = new Keys(buf(7), text("collision-a"));
        var second = new Keys(buf(7), text("collision-b"));
        var partition = partition(1, null, null);

        var firstRoutingKey = PartitionBoundaryRules.routingKey(schema, first);
        var secondRoutingKey = PartitionBoundaryRules.routingKey(schema, second);

        assertEquals(firstRoutingKey, secondRoutingKey);
        assertNotEquals(RockserverKeyEncoder.calculateBucketElementKey(schema, first),
                RockserverKeyEncoder.calculateBucketElementKey(schema, second));
        assertEquals(partition, PartitionBoundaryRules.requirePartition(List.of(partition), firstRoutingKey));
        assertEquals(partition, PartitionBoundaryRules.requirePartition(List.of(partition), secondRoutingKey));
    }

    private static void assertIllegalArgument(String expectedMessage, org.junit.jupiter.api.function.Executable action) {
        var failure = assertThrows(IllegalArgumentException.class, action);

        assertEquals(expectedMessage, failure.getMessage());
    }

    private static PartitionDescriptor partition(long id, Buf startKeyInclusive, Buf endKeyExclusive) {
        return partition(id, startKeyInclusive, endKeyExclusive, PartitionSplitState.ACTIVE);
    }

    private static PartitionDescriptor partition(long id,
                                                 Buf startKeyInclusive,
                                                 Buf endKeyExclusive,
                                                 PartitionSplitState splitState) {
        var leader = NodeId.of("n1");
        return new PartitionDescriptor(
                new ShardId(id),
                startKeyInclusive,
                endKeyExclusive,
                1,
                "group-" + id,
                List.of(new ReplicaMember(leader, URI.create("http://127.0.0.1:10001"), ReplicaRole.VOTER, 0, 0,
                        true)),
                leader,
                Map.of("rack", "rack-a"),
                splitState
        );
    }

    private static Buf buf(int... values) {
        var bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return Buf.wrap(bytes);
    }

    private static Buf text(String value) {
        return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
    }
}
