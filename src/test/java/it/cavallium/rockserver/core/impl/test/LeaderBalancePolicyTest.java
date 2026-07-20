package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.cluster.NodeHealthReport;
import it.cavallium.rockserver.core.cluster.NodeHealthStatus;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.placement.LeaderBalanceConstraints;
import it.cavallium.rockserver.core.cluster.placement.LeaderBalancePolicy;
import it.cavallium.rockserver.core.cluster.placement.LeaderTransferRecommendation;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class LeaderBalancePolicyTest {

    private final LeaderBalancePolicy policy = new LeaderBalancePolicy();

    @Test
    void plansTransferFromOverloadedLeaderToLeastLoadedCaughtUpVoter() {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var n3 = NodeId.of("n3");
        var partitions = List.of(
                partition(1, n1, PartitionSplitState.ACTIVE, voter(n1, 10, 10), voter(n2, 10, 10), voter(n3, 10, 10)),
                partition(2, n1, PartitionSplitState.ACTIVE, voter(n1, 10, 10), voter(n2, 10, 10), voter(n3, 10, 10)),
                partition(3, n2, PartitionSplitState.ACTIVE, voter(n1, 10, 10), voter(n2, 10, 10), voter(n3, 10, 10))
        );

        var transfers = policy.planTransfers(partitions, reports(
                report(n1, NodeHealthStatus.HEALTHY, 4, 0.10d),
                report(n2, NodeHealthStatus.HEALTHY, 2, 0.10d),
                report(n3, NodeHealthStatus.HEALTHY, 1, 0.10d)
        ), constraints(1), 1_500);

        assertEquals(List.of(new LeaderTransferRecommendation(new ShardId(1), 1, n1, n3, 1, 1)), transfers);
    }

    @Test
    void capsTransfersAndUpdatesProjectedLeaderCounts() {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var n3 = NodeId.of("n3");
        var partitions = List.of(
                partition(1, n1, PartitionSplitState.ACTIVE, voter(n1, 10, 10), voter(n2, 10, 10), voter(n3, 10, 10)),
                partition(2, n1, PartitionSplitState.ACTIVE, voter(n1, 10, 10), voter(n2, 10, 10), voter(n3, 10, 10)),
                partition(3, n1, PartitionSplitState.ACTIVE, voter(n1, 10, 10), voter(n2, 10, 10), voter(n3, 10, 10)),
                partition(4, n1, PartitionSplitState.ACTIVE, voter(n1, 10, 10), voter(n2, 10, 10), voter(n3, 10, 10))
        );

        var transfers = policy.planTransfers(partitions, reports(
                report(n1, NodeHealthStatus.HEALTHY, 4, 0.10d),
                report(n2, NodeHealthStatus.HEALTHY, 1, 0.10d),
                report(n3, NodeHealthStatus.HEALTHY, 2, 0.10d)
        ), constraints(2), 1_500);

        assertEquals(List.of(
                new LeaderTransferRecommendation(new ShardId(1), 1, n1, n2, 3, 1),
                new LeaderTransferRecommendation(new ShardId(2), 1, n1, n3, 2, 1)
        ), transfers);
    }

    @Test
    void skipsLaggingLearnerAndStaleTargets() {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var n3 = NodeId.of("n3");
        var n4 = NodeId.of("n4");
        var partitions = List.of(
                partition(1, n1, PartitionSplitState.ACTIVE, voter(n1, 10, 10), voter(n2, 9, 9), learner(n3, 10, 10)),
                partition(2, n1, PartitionSplitState.ACTIVE, voter(n1, 10, 10), voter(n4, 10, 10))
        );

        var transfers = policy.planTransfers(partitions, reports(
                report(n1, NodeHealthStatus.HEALTHY, 4, 0.10d),
                report(n2, NodeHealthStatus.HEALTHY, 1, 0.10d),
                report(n3, NodeHealthStatus.HEALTHY, 1, 0.10d),
                staleReport(n4)
        ), constraints(2), 1_500);

        assertEquals(List.of(), transfers);
    }

    @Test
    void skipsPartitionsThatAreNotActive() {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var partitions = List.of(
                partition(1, n1, PartitionSplitState.SPLIT_SOURCE, voter(n1, 10, 10), voter(n2, 10, 10)),
                partition(2, n1, PartitionSplitState.SPLIT_SOURCE, voter(n1, 10, 10), voter(n2, 10, 10))
        );

        var transfers = policy.planTransfers(partitions, reports(
                report(n1, NodeHealthStatus.HEALTHY, 4, 0.10d),
                report(n2, NodeHealthStatus.HEALTHY, 1, 0.10d)
        ), constraints(2), 1_500);

        assertEquals(List.of(), transfers);
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var partitions = List.of(partition(1, n1, PartitionSplitState.ACTIVE, voter(n1, 10, 10), voter(n2, 10, 10)));
        var healthReports = reports(
                report(n1, NodeHealthStatus.HEALTHY, 1, 0.10d),
                report(n2, NodeHealthStatus.HEALTHY, 1, 0.10d)
        );
        var constraints = constraints(1);

        assertIllegalArgument("partitions must not be null",
                () -> policy.planTransfers(null, healthReports, constraints, 1_500));
        assertIllegalArgument("healthReports must not be null",
                () -> policy.planTransfers(partitions, null, constraints, 1_500));
        assertIllegalArgument("constraints must not be null",
                () -> policy.planTransfers(partitions, healthReports, null, 1_500));
        assertIllegalArgument("partitions entry must not be null",
                () -> policy.planTransfers(Collections.singletonList(null), healthReports, constraints, 1_500));

        var nullKeyReports = new HashMap<NodeId, NodeHealthReport>();
        nullKeyReports.put(null, report(n1, NodeHealthStatus.HEALTHY, 1, 0.10d));
        assertIllegalArgument("healthReports key must not be null",
                () -> policy.planTransfers(partitions, nullKeyReports, constraints, 1_500));

        var nullValueReports = new HashMap<NodeId, NodeHealthReport>();
        nullValueReports.put(n1, null);
        assertIllegalArgument("healthReports value must not be null",
                () -> policy.planTransfers(partitions, nullValueReports, constraints, 1_500));

        assertIllegalArgument("partitionId must not be null",
                () -> new LeaderTransferRecommendation(null, 1, n1, n2, 0, 1));
        assertIllegalArgument("fromLeader must not be null",
                () -> new LeaderTransferRecommendation(new ShardId(1), 1, null, n2, 0, 1));
        assertIllegalArgument("toLeader must not be null",
                () -> new LeaderTransferRecommendation(new ShardId(1), 1, n1, null, 0, 1));
    }

    private static LeaderBalanceConstraints constraints(int maxTransfers) {
        return new LeaderBalanceConstraints(maxTransfers, 600);
    }

    private static PartitionDescriptor partition(long partitionId,
                                                 NodeId leader,
                                                 PartitionSplitState splitState,
                                                 ReplicaMember... peers) {
        return new PartitionDescriptor(
                new ShardId(partitionId),
                null,
                null,
                1,
                "group-" + partitionId,
                List.of(peers),
                leader,
                Map.of(),
                splitState
        );
    }

    private static ReplicaMember voter(NodeId nodeId, long matchIndex, long appliedIndex) {
        return peer(nodeId, ReplicaRole.VOTER, matchIndex, appliedIndex);
    }

    private static ReplicaMember learner(NodeId nodeId, long matchIndex, long appliedIndex) {
        return peer(nodeId, ReplicaRole.LEARNER, matchIndex, appliedIndex);
    }

    private static ReplicaMember peer(NodeId nodeId, ReplicaRole role, long matchIndex, long appliedIndex) {
        var port = 10_000 + Integer.parseInt(nodeId.value().substring(1));
        return new ReplicaMember(
                nodeId,
                URI.create("grpc://127.0.0.1:" + port),
                role,
                matchIndex,
                appliedIndex,
                true
        );
    }

    private static NodeHealthReport report(NodeId nodeId,
                                           NodeHealthStatus status,
                                           int partitionReplicaCount,
                                           double processCpuLoad) {
        return new NodeHealthReport(
                nodeId,
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

    private static NodeHealthReport staleReport(NodeId nodeId) {
        return new NodeHealthReport(
                nodeId,
                NodeHealthStatus.HEALTHY,
                800,
                80,
                100,
                40,
                100,
                1,
                0.10d
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
