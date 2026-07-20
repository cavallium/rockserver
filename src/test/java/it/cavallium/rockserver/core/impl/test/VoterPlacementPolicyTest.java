package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.cluster.ClusterNodeMetadata;
import it.cavallium.rockserver.core.cluster.NodeHealthReport;
import it.cavallium.rockserver.core.cluster.NodeHealthStatus;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.placement.VoterPlacementConstraints;
import it.cavallium.rockserver.core.cluster.placement.VoterPlacementPolicy;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class VoterPlacementPolicyTest {

    private final VoterPlacementPolicy policy = new VoterPlacementPolicy();

    @Test
    void selectsVotersAcrossFailureDomainsByCapacityOrder() {
        var n1 = node("n1", "rack-a");
        var n2 = node("n2", "rack-a");
        var n3 = node("n3", "rack-b");
        var n4 = node("n4", "rack-c");
        var constraints = constraints(3, 3);

        var selected = policy.selectVoters(
                List.of(n1, n2, n3, n4),
                reports(
                        report(n1, NodeHealthStatus.HEALTHY, 5, 0.10d),
                        report(n2, NodeHealthStatus.HEALTHY, 1, 0.30d),
                        report(n3, NodeHealthStatus.HEALTHY, 2, 0.20d),
                        report(n4, NodeHealthStatus.HEALTHY, 4, 0.10d)
                ),
                constraints,
                1_500
        );

        assertEquals(List.of(NodeId.of("n2"), NodeId.of("n3"), NodeId.of("n4")),
                selected.stream().map(ClusterNodeMetadata::nodeId).toList());
    }

    @Test
    void fillsExtraVotersByKeepingFailureDomainCountsBalanced() {
        var n1 = node("n1", "rack-a");
        var n2 = node("n2", "rack-a");
        var n3 = node("n3", "rack-b");
        var n4 = node("n4", "rack-c");
        var constraints = constraints(4, 2);

        var selected = policy.selectVoters(
                List.of(n1, n2, n3, n4),
                reports(
                        report(n1, NodeHealthStatus.HEALTHY, 1, 0.10d),
                        report(n2, NodeHealthStatus.HEALTHY, 2, 0.10d),
                        report(n3, NodeHealthStatus.HEALTHY, 3, 0.10d),
                        report(n4, NodeHealthStatus.HEALTHY, 4, 0.10d)
                ),
                constraints,
                1_500
        );

        assertEquals(List.of(NodeId.of("n1"), NodeId.of("n3"), NodeId.of("n4"), NodeId.of("n2")),
                selected.stream().map(ClusterNodeMetadata::nodeId).toList());
    }

    @Test
    void rejectsPlacementWhenHealthyDomainsAreInsufficient() {
        var n1 = node("n1", "rack-a");
        var n2 = node("n2", "rack-a");
        var n3 = node("n3", "rack-b");
        var constraints = constraints(2, 2);

        assertThrows(IllegalStateException.class, () -> policy.selectVoters(
                List.of(n1, n2, n3),
                reports(
                        report(n1, NodeHealthStatus.HEALTHY, 1, 0.10d),
                        report(n2, NodeHealthStatus.HEALTHY, 2, 0.10d),
                        report(n3, NodeHealthStatus.UNAVAILABLE, 3, 0.10d)
                ),
                constraints,
                1_500
        ));
    }

    @Test
    void validatesExistingVoterPlacement() {
        var n1 = node("n1", "rack-a");
        var n2 = node("n2", "rack-b");
        var n3 = node("n3", "rack-c");
        var constraints = constraints(2, 2);

        policy.validateVoterPlacement(
                List.of(peer(n1, ReplicaRole.VOTER), peer(n2, ReplicaRole.VOTER), peer(n3, ReplicaRole.LEARNER)),
                Map.of(n1.nodeId(), n1, n2.nodeId(), n2, n3.nodeId(), n3),
                constraints
        );

        assertThrows(IllegalStateException.class, () -> policy.validateVoterPlacement(
                List.of(peer(n1, ReplicaRole.VOTER), peer(node("n4", "rack-a"), ReplicaRole.VOTER)),
                Map.of(n1.nodeId(), n1, NodeId.of("n4"), node("n4", "rack-a")),
                constraints
        ));
        assertThrows(IllegalStateException.class, () -> policy.validateVoterPlacement(
                List.of(peer(n1, ReplicaRole.VOTER), peer(n2, ReplicaRole.VOTER)),
                Map.of(n1.nodeId(), n1),
                constraints
        ));
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var n1 = node("n1", "rack-a");
        var constraints = constraints(1, 1);
        var nodes = List.of(n1);
        var healthReports = reports(report(n1, NodeHealthStatus.HEALTHY, 1, 0.10d));
        var peers = List.of(peer(n1, ReplicaRole.VOTER));
        var nodesById = Map.of(n1.nodeId(), n1);

        assertIllegalArgument("nodes must not be null",
                () -> policy.selectVoters(null, healthReports, constraints, 1_500));
        assertIllegalArgument("healthReports must not be null",
                () -> policy.selectVoters(nodes, null, constraints, 1_500));
        assertIllegalArgument("constraints must not be null",
                () -> policy.selectVoters(nodes, healthReports, null, 1_500));
        assertIllegalArgument("nodes entry must not be null",
                () -> policy.selectVoters(Collections.singletonList(null), healthReports, constraints, 1_500));

        var nullKeyReports = new HashMap<NodeId, NodeHealthReport>();
        nullKeyReports.put(null, report(n1, NodeHealthStatus.HEALTHY, 1, 0.10d));
        assertIllegalArgument("healthReports key must not be null",
                () -> policy.selectVoters(nodes, nullKeyReports, constraints, 1_500));

        var nullValueReports = new HashMap<NodeId, NodeHealthReport>();
        nullValueReports.put(n1.nodeId(), null);
        assertIllegalArgument("healthReports value must not be null",
                () -> policy.selectVoters(nodes, nullValueReports, constraints, 1_500));

        assertIllegalArgument("peers must not be null",
                () -> policy.validateVoterPlacement(null, nodesById, constraints));
        assertIllegalArgument("nodesById must not be null",
                () -> policy.validateVoterPlacement(peers, null, constraints));
        assertIllegalArgument("constraints must not be null",
                () -> policy.validateVoterPlacement(peers, nodesById, null));
        assertIllegalArgument("peers entry must not be null",
                () -> policy.validateVoterPlacement(Collections.singletonList(null), nodesById, constraints));

        var nullKeyNodes = new HashMap<NodeId, ClusterNodeMetadata>();
        nullKeyNodes.put(null, n1);
        assertIllegalArgument("nodesById key must not be null",
                () -> policy.validateVoterPlacement(peers, nullKeyNodes, constraints));

        var nullValueNodes = new HashMap<NodeId, ClusterNodeMetadata>();
        nullValueNodes.put(n1.nodeId(), null);
        assertIllegalArgument("nodesById value must not be null",
                () -> policy.validateVoterPlacement(peers, nullValueNodes, constraints));
    }

    private static VoterPlacementConstraints constraints(int targetVoterCount, int minFailureDomains) {
        return new VoterPlacementConstraints(targetVoterCount, minFailureDomains, 64, 0.75d, 600);
    }

    private static ClusterNodeMetadata node(String nodeId, String failureDomain) {
        var port = 10_000 + Integer.parseInt(nodeId.substring(1));
        return new ClusterNodeMetadata(
                NodeId.of(nodeId),
                URI.create("http://127.0.0.1:" + port),
                URI.create("grpc://127.0.0.1:" + port),
                failureDomain
        );
    }

    private static ReplicaMember peer(ClusterNodeMetadata node, ReplicaRole role) {
        return new ReplicaMember(node.nodeId(), node.ratisEndpoint(), role, 0, 0, true);
    }

    private static NodeHealthReport report(ClusterNodeMetadata node,
                                           NodeHealthStatus status,
                                           int partitionReplicaCount,
                                           double processCpuLoad) {
        return new NodeHealthReport(
                node.nodeId(),
                status,
                1_000,
                80,
                100,
                40,
                100,
                partitionReplicaCount,
                processCpuLoad
        );
    }

    private static Map<NodeId, NodeHealthReport> reports(NodeHealthReport... reports) {
        var byNodeId = new HashMap<NodeId, NodeHealthReport>();
        for (NodeHealthReport report : reports) {
            byNodeId.put(report.nodeId(), report);
        }
        return Map.copyOf(byNodeId);
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var failure = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, failure.getMessage());
    }
}
