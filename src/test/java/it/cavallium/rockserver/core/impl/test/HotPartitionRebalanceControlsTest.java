package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.rebalance.AdminRebalanceAction;
import it.cavallium.rockserver.core.cluster.rebalance.AdminRebalanceAdmissionController;
import it.cavallium.rockserver.core.cluster.rebalance.AdminRebalanceDecision;
import it.cavallium.rockserver.core.cluster.rebalance.AdminRebalanceRequest;
import it.cavallium.rockserver.core.cluster.rebalance.HotPartition;
import it.cavallium.rockserver.core.cluster.rebalance.HotPartitionDetector;
import it.cavallium.rockserver.core.cluster.rebalance.HotPartitionReason;
import it.cavallium.rockserver.core.cluster.rebalance.HotPartitionThresholds;
import it.cavallium.rockserver.core.cluster.rebalance.PartitionLoadReport;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class HotPartitionRebalanceControlsTest {

    @Test
    void detectsFreshHotPartitionsAndIgnoresStaleReports() {
        var detector = new HotPartitionDetector();
        var thresholds = new HotPartitionThresholds(1_000, 500, 10_000, 50, 100, 600);

        var hot = detector.detect(List.of(
                report(1, 1_000, 700, 350, 5_000, 30, 5),
                report(2, 800, 2_000, 1_000, 20_000, 200, 300),
                report(3, 1_000, 100, 600, 1_000, 20, 120)
        ), thresholds, 1_500);

        assertEquals(List.of(new ShardId(1), new ShardId(3)), hot.stream()
                .map(partition -> partition.report().partitionId())
                .toList());
        assertEquals(List.of(HotPartitionReason.TOTAL_OPS), hot.get(0).reasons());
        assertEquals(List.of(HotPartitionReason.WRITE_OPS, HotPartitionReason.QUEUE_DEPTH),
                hot.get(1).reasons());
    }

    @Test
    void admitsExplicitAdminRebalanceRequests() {
        var controller = new AdminRebalanceAdmissionController();
        var metadata = metadata(partition(new ShardId(1), 5, PartitionSplitState.ACTIVE));
        var request = new AdminRebalanceRequest(
                "rebalance-1",
                AdminRebalanceAction.SPLIT_PARTITION,
                new ShardId(1),
                null,
                null,
                5,
                "manual hot partition split",
                true
        );

        var decision = controller.admit(metadata, 11, request, 0, 1);

        assertEquals(request, decision.request());
        assertEquals("rebalance request accepted for dry run", decision.message());
    }

    @Test
    void rejectsStaleOrUnsafeAdminRebalanceRequests() {
        var controller = new AdminRebalanceAdmissionController();
        var activeMetadata = metadata(partition(new ShardId(1), 5, PartitionSplitState.ACTIVE));
        var changingMetadata = metadata(partition(new ShardId(1), 5, PartitionSplitState.SPLIT_SOURCE));
        var request = new AdminRebalanceRequest(
                "rebalance-1",
                AdminRebalanceAction.SPLIT_PARTITION,
                new ShardId(1),
                null,
                null,
                5,
                "manual hot partition split",
                false
        );

        assertThrows(IllegalArgumentException.class, () -> controller.admit(activeMetadata, 10, request, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> controller.admit(activeMetadata, 11,
                new AdminRebalanceRequest("rebalance-2", AdminRebalanceAction.SPLIT_PARTITION, new ShardId(1), null,
                        null, 4, "stale epoch", false), 0, 1));
        assertThrows(IllegalStateException.class, () -> controller.admit(changingMetadata, 11, request, 0, 1));
        assertThrows(IllegalStateException.class, () -> controller.admit(activeMetadata, 11, request, 1, 1));
        assertIllegalArgument("targetNodeId must not be null", () -> new AdminRebalanceRequest(
                "rebalance-3",
                AdminRebalanceAction.TRANSFER_LEADER,
                new ShardId(1),
                null,
                null,
                5,
                "missing leader target",
                false
        ));
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var controller = new AdminRebalanceAdmissionController();
        var metadata = metadata(partition(new ShardId(1), 5, PartitionSplitState.ACTIVE));
        var request = new AdminRebalanceRequest(
                "rebalance-1",
                AdminRebalanceAction.SPLIT_PARTITION,
                new ShardId(1),
                null,
                null,
                5,
                "manual hot partition split",
                false
        );

        assertIllegalArgument("metadata must not be null", () -> controller.admit(null, 11, request, 0, 1));
        assertIllegalArgument("request must not be null", () -> controller.admit(metadata, 11, null, 0, 1));
        assertIllegalArgument("action must not be null", () -> new AdminRebalanceRequest(
                "rebalance-2", null, new ShardId(1), null, null, 5, "reason", false));
        assertIllegalArgument("partitionId must not be null", () -> new AdminRebalanceRequest(
                "rebalance-3", AdminRebalanceAction.SPLIT_PARTITION, null, null, null, 5, "reason", false));
        assertIllegalArgument("secondaryPartitionId must not be null", () -> new AdminRebalanceRequest(
                "rebalance-4", AdminRebalanceAction.MERGE_PARTITIONS, new ShardId(1), null, null, 5,
                "reason", false));
        assertIllegalArgument("targetNodeId must not be null", () -> new AdminRebalanceRequest(
                "rebalance-5", AdminRebalanceAction.MOVE_REPLICA, new ShardId(1), null, null, 5,
                "reason", false));
        assertIllegalArgument("request must not be null", () -> new AdminRebalanceDecision(null, "accepted"));

        var detector = new HotPartitionDetector();
        var thresholds = new HotPartitionThresholds(1_000, 500, 10_000, 50, 100, 600);
        var report = report(1, 1_000, 700, 350, 5_000, 30, 5);
        assertIllegalArgument("reports must not be null", () -> detector.detect(null, thresholds, 1_500));
        assertIllegalArgument("thresholds must not be null", () -> detector.detect(List.of(report), null, 1_500));
        assertIllegalArgument("reports entry must not be null",
                () -> detector.detect(Collections.singletonList(null), thresholds, 1_500));
        assertIllegalArgument("partitionId must not be null", () -> new PartitionLoadReport(
                null, 5, 1_000, 700, 350, 5_000, 30, 5));
        assertIllegalArgument("report must not be null",
                () -> new HotPartition(null, List.of(HotPartitionReason.TOTAL_OPS)));
        assertIllegalArgument("reasons must not be null", () -> new HotPartition(report, null));
        assertIllegalArgument("reasons entry must not be null",
                () -> new HotPartition(report, Collections.singletonList(null)));
    }

    private static PartitionLoadReport report(long partitionId,
                                              long observedAtMillis,
                                              double readOpsPerSecond,
                                              double writeOpsPerSecond,
                                              double bytesPerSecond,
                                              double averageLatencyMillis,
                                              int pendingRequests) {
        return new PartitionLoadReport(
                new ShardId(partitionId),
                5,
                observedAtMillis,
                readOpsPerSecond,
                writeOpsPerSecond,
                bytesPerSecond,
                averageLatencyMillis,
                pendingRequests
        );
    }

    private static ClusterMetadata metadata(PartitionDescriptor partition) {
        return new ClusterMetadata(new ClusterId(new UUID(0, 1)), 11, Map.of(), Map.of(), Map.of(
                partition.partitionId(), partition
        ));
    }

    private static PartitionDescriptor partition(ShardId partitionId,
                                                 long epoch,
                                                 PartitionSplitState splitState) {
        var leader = NodeId.of("n1");
        return new PartitionDescriptor(
                partitionId,
                null,
                null,
                epoch,
                "group-" + partitionId,
                List.of(new ReplicaMember(leader, URI.create("grpc://127.0.0.1:10001"), ReplicaRole.VOTER, 0, 0,
                        true)),
                leader,
                Map.of(),
                splitState
        );
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var failure = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, failure.getMessage());
    }
}
