package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaGroupMetadata;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ReplicatedCommandKind;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.client.PartitionRouteCache;
import it.cavallium.rockserver.core.cluster.client.RoutingPolicy;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeKind;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeMetadata;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeStatus;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class PartitionRouteCacheTest {

    private static final ClusterId CLUSTER_ID = new ClusterId(new UUID(0, 1));
    private static final ShardId SHARD = ShardId.DEFAULT;
    private static final NodeId N1 = NodeId.of("n1");
    private static final NodeId N2 = NodeId.of("n2");
    private static final NodeId N3 = NodeId.of("n3");

    @SuppressWarnings("DataFlowIssue")
    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var cache = new PartitionRouteCache(metadata(7, N1));

        assertIllegalArgument("newMetadata must not be null", () -> new PartitionRouteCache(null));
        assertIllegalArgument("newMetadata must not be null", () -> cache.updateMetadata(null));
        assertIllegalArgument("beforeCommit must not be null", () -> cache.updateMetadata(metadata(8, N1), null));
        assertIllegalArgument("shardId must not be null",
                () -> cache.route(null, ReplicatedCommandKind.REPLICATED_WRITE, RoutingPolicy.linearizable()));
        assertIllegalArgument("kind must not be null",
                () -> cache.route(SHARD, null, RoutingPolicy.linearizable()));
        assertIllegalArgument("policy must not be null",
                () -> cache.route(SHARD, ReplicatedCommandKind.REPLICATED_WRITE, null));
        assertIllegalArgument("shardId must not be null", () -> cache.invalidate(null));
    }

    @Test
    void reusesCachedRouteWithinSameMetadataEpoch() {
        var cache = new PartitionRouteCache(metadata(7, N1));

        var first = cache.route(SHARD, ReplicatedCommandKind.REPLICATED_WRITE, RoutingPolicy.linearizable());
        var second = cache.route(SHARD, ReplicatedCommandKind.REPLICATED_WRITE, RoutingPolicy.linearizable());

        assertSame(first, second);
        assertEquals(7, first.metadataEpoch());
        assertEquals(N1, first.replica().nodeId());
    }

    @Test
    void rejectsRouteRequestsForStaleMetadataEpoch() {
        var cache = new PartitionRouteCache(metadata(7, N1));

        assertThrows(IllegalStateException.class,
                () -> cache.routeAtEpoch(6, SHARD, ReplicatedCommandKind.REPLICATED_WRITE,
                        RoutingPolicy.linearizable()));
    }

    @Test
    void metadataUpdateClearsCachedRouteAndUsesNewLeader() {
        var cache = new PartitionRouteCache(metadata(7, N1));
        cache.route(SHARD, ReplicatedCommandKind.REPLICATED_WRITE, RoutingPolicy.linearizable());

        cache.updateMetadata(metadata(8, N2));
        var target = cache.routeAtEpoch(8, SHARD, ReplicatedCommandKind.REPLICATED_WRITE,
                RoutingPolicy.linearizable());

        assertEquals(8, target.metadataEpoch());
        assertEquals(N2, target.replica().nodeId());
    }

    @Test
    void rejectsMetadataEpochRegression() {
        var cache = new PartitionRouteCache(metadata(7, N1));

        assertThrows(IllegalArgumentException.class, () -> cache.updateMetadata(metadata(6, N1)));
    }

    @Test
    void listenerFailureLeavesMetadataAndCachedRouteUnchanged() {
        var cache = new PartitionRouteCache(metadata(7, N1));
        var cached = cache.route(SHARD, ReplicatedCommandKind.REPLICATED_WRITE, RoutingPolicy.linearizable());

        assertThrows(IllegalStateException.class, () -> cache.updateMetadata(metadata(8, N2), ignored -> {
            throw new IllegalStateException("refresh failed");
        }));

        assertEquals(7, cache.metadataEpoch());
        assertEquals(N1, cache.metadata().requireGroup(SHARD).leader());
        assertSame(cached, cache.routeAtEpoch(7, SHARD, ReplicatedCommandKind.REPLICATED_WRITE,
                RoutingPolicy.linearizable()));
    }

    @Test
    void sameEpochNonRoutingMetadataRefreshKeepsCachedRoute() {
        var cache = new PartitionRouteCache(metadata(7, N1));
        var cached = cache.route(SHARD, ReplicatedCommandKind.REPLICATED_WRITE, RoutingPolicy.linearizable());
        var schemaChange = new SchemaChangeMetadata(42, SchemaChangeKind.DELETE_COLUMN, 4, 3,
                null, null, SchemaChangeStatus.FENCED);
        var refreshed = metadataWithSchemaChange(schemaChange);

        cache.updateMetadata(refreshed);

        assertEquals(refreshed, cache.metadata());
        assertEquals(schemaChange, cache.metadata().schemaChangesById().get(42L));
        assertSame(cached, cache.routeAtEpoch(7, SHARD, ReplicatedCommandKind.REPLICATED_WRITE,
                RoutingPolicy.linearizable()));
    }

    @Test
    void invalidateRemovesOnlyOnePartitionRoute() {
        var cache = new PartitionRouteCache(metadata(7, N1));
        var first = cache.route(SHARD, ReplicatedCommandKind.REPLICATED_WRITE, RoutingPolicy.linearizable());

        cache.invalidate(SHARD);
        var second = cache.route(SHARD, ReplicatedCommandKind.REPLICATED_WRITE, RoutingPolicy.linearizable());

        assertEquals(first, second);
    }

    @Test
    void rejectsSameEpochRouteGroupChange() {
        var initial = metadata(7, N1);
        var cache = new PartitionRouteCache(initial);
        var cached = cache.route(SHARD, ReplicatedCommandKind.REPLICATED_WRITE, RoutingPolicy.linearizable());

        var failure = assertThrows(IllegalArgumentException.class, () -> cache.updateMetadata(metadata(7, N2)));

        assertEquals("route cache same-epoch metadata refresh cannot change partition routing data",
                failure.getMessage());
        assertSame(initial, cache.metadata());
        assertSame(cached, cache.routeAtEpoch(7, SHARD, ReplicatedCommandKind.REPLICATED_WRITE,
                RoutingPolicy.linearizable()));
    }

    @Test
    void rejectsSameEpochPartitionRoutingChange() {
        var initial = metadataWithPartition(buf(0x40));
        var cache = new PartitionRouteCache(initial);

        var failure = assertThrows(IllegalArgumentException.class,
                () -> cache.updateMetadata(metadataWithPartition(buf(0x50))));

        assertEquals("route cache same-epoch metadata refresh cannot change partition routing data",
                failure.getMessage());
        assertSame(initial, cache.metadata());
    }

    private static ClusterMetadata metadata(long epoch, NodeId leader) {
        var group = new ReplicaGroupMetadata(SHARD, 3, leader, List.of(
                member(N1, 25),
                member(N2, 24),
                member(N3, 10)
        ), 3);
        return new ClusterMetadata(CLUSTER_ID, epoch, Map.of(SHARD, group));
    }

    private static ClusterMetadata metadataWithSchemaChange(SchemaChangeMetadata schemaChange) {
        var group = new ReplicaGroupMetadata(SHARD, 3, N1, List.of(
                member(N1, 25),
                member(N2, 24),
                member(N3, 10)
        ), 3);
        return new ClusterMetadata(
                CLUSTER_ID,
                7,
                Map.of(SHARD, group),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(schemaChange.schemaChangeId(), schemaChange),
                Map.of()
        );
    }

    private static ClusterMetadata metadataWithPartition(Buf endKeyExclusive) {
        var members = List.of(member(N1, 0));
        var group = new ReplicaGroupMetadata(SHARD, 1, N1, members, 1);
        var partition = partition(endKeyExclusive);
        return new ClusterMetadata(CLUSTER_ID, 7, Map.of(SHARD, group), Map.of(), Map.of(SHARD, partition));
    }

    private static PartitionDescriptor partition(Buf endKeyExclusive) {
        return new PartitionDescriptor(
                SHARD,
                null,
                endKeyExclusive,
                7,
                "group-" + SHARD,
                List.of(member(N1, 0)),
                N1,
                Map.of(),
                PartitionSplitState.ACTIVE
        );
    }

    private static Buf buf(int value) {
        return Buf.wrap((byte) value);
    }

    private static ReplicaMember member(NodeId nodeId, long appliedIndex) {
        return new ReplicaMember(nodeId,
                URI.create("http://127.0.0.1:10" + nodeId.value().substring(1)),
                ReplicaRole.VOTER,
                appliedIndex,
                appliedIndex,
                true);
    }

    private static void assertIllegalArgument(String expectedMessage, Runnable runnable) {
        var error = assertThrows(IllegalArgumentException.class, runnable::run);
        assertEquals(expectedMessage, error.getMessage());
    }
}
