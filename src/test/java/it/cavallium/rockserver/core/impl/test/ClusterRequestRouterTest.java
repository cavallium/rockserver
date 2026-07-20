package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.ReadConsistency;
import it.cavallium.rockserver.core.cluster.ReplicaGroupMetadata;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ReplicatedCommandKind;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.client.ClusterRequestRouter;
import it.cavallium.rockserver.core.cluster.client.RoutingPolicy;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class ClusterRequestRouterTest {

    private static final ShardId SHARD = ShardId.DEFAULT;
    private static final NodeId LEADER = NodeId.of("n1");
    private static final NodeId FOLLOWER_FAST = NodeId.of("n2");
    private static final NodeId FOLLOWER_LAGGING = NodeId.of("n3");

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("metadata must not be null", () -> new ClusterRequestRouter(null));

        var router = router();
        assertIllegalArgument("shardId must not be null",
                () -> router.route(null, ReplicatedCommandKind.REPLICATED_WRITE, RoutingPolicy.linearizable()));
        assertIllegalArgument("kind must not be null",
                () -> router.route(SHARD, null, RoutingPolicy.linearizable()));
        assertIllegalArgument("policy must not be null",
                () -> router.route(SHARD, ReplicatedCommandKind.REPLICATED_WRITE, null));
    }

    @Test
    void routesWritesToLeader() {
        var target = router().route(SHARD, ReplicatedCommandKind.REPLICATED_WRITE, RoutingPolicy.linearizable());

        assertEquals(LEADER, target.replica().nodeId());
        assertTrue(target.leader());
        assertEquals(7, target.metadataEpoch());
    }

    @Test
    void routesLinearizableReadsToLeader() {
        var target = router().route(SHARD, ReplicatedCommandKind.ROUTED_READ, RoutingPolicy.linearizable());

        assertEquals(LEADER, target.replica().nodeId());
        assertTrue(target.leader());
        assertEquals(7, target.metadataEpoch());
    }

    @Test
    void rejectsLinearizableReadsWhenLeaderIsNotServingReads() {
        var router = router(
                member(LEADER, 25, false),
                member(FOLLOWER_FAST, 24, true),
                member(FOLLOWER_LAGGING, 10, true)
        );

        var failure = assertThrows(IllegalStateException.class,
                () -> router.route(SHARD, ReplicatedCommandKind.ROUTED_READ, RoutingPolicy.linearizable()));

        assertEquals("leader cannot satisfy read policy for shard " + SHARD, failure.getMessage());
    }

    @Test
    void routesWritesToLeaderEvenWhenLeaderIsNotServingReads() {
        var router = router(
                member(LEADER, 25, false),
                member(FOLLOWER_FAST, 24, true),
                member(FOLLOWER_LAGGING, 10, true)
        );

        var target = router.route(SHARD, ReplicatedCommandKind.REPLICATED_WRITE, RoutingPolicy.linearizable());

        assertEquals(LEADER, target.replica().nodeId());
        assertTrue(target.leader());
    }

    @Test
    void routesFollowerReadsToBestFreshFollower() {
        var target = router().route(SHARD, ReplicatedCommandKind.ROUTED_READ, RoutingPolicy.follower(20));

        assertEquals(FOLLOWER_FAST, target.replica().nodeId());
        assertFalse(target.leader());
        assertEquals(7, target.metadataEpoch());
    }

    @Test
    void routesLocalReadsToBestFreshReplicaWithoutFollowerPreference() {
        var target = router().route(SHARD, ReplicatedCommandKind.ROUTED_READ, RoutingPolicy.local());

        assertEquals(LEADER, target.replica().nodeId());
        assertTrue(target.leader());
        assertEquals(7, target.metadataEpoch());
    }

    @Test
    void customStaleReadsWithoutFollowerPreferenceCanUseLeader() {
        var target = router().route(SHARD, ReplicatedCommandKind.ROUTED_READ,
                new RoutingPolicy(ReadConsistency.BOUNDED_STALENESS, 25, false));

        assertEquals(LEADER, target.replica().nodeId());
        assertTrue(target.leader());
        assertEquals(7, target.metadataEpoch());
    }

    @Test
    void rejectsReadsWhenNoReplicaSatisfiesAppliedIndex() {
        assertThrows(IllegalStateException.class,
                () -> router().route(SHARD, ReplicatedCommandKind.ROUTED_READ, RoutingPolicy.follower(100)));
    }

    @Test
    void rejectsLocalSessionCommands() {
        assertThrows(IllegalArgumentException.class,
                () -> router().route(SHARD, ReplicatedCommandKind.LOCAL_SESSION, RoutingPolicy.local()));
    }

    private static ClusterRequestRouter router() {
        return router(
                member(LEADER, 25, true),
                member(FOLLOWER_FAST, 24, true),
                member(FOLLOWER_LAGGING, 10, true)
        );
    }

    private static ClusterRequestRouter router(ReplicaMember... members) {
        var group = new ReplicaGroupMetadata(SHARD, 3, LEADER, List.of(
                members
        ), 3);
        var metadata = new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of(SHARD, group));
        return new ClusterRequestRouter(metadata);
    }

    private static ReplicaMember member(NodeId nodeId, long appliedIndex, boolean servingReads) {
        return new ReplicaMember(nodeId,
                URI.create("http://127.0.0.1:1000" + nodeId.value().substring(1)),
                ReplicaRole.VOTER,
                appliedIndex,
                appliedIndex,
                servingReads);
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }
}
