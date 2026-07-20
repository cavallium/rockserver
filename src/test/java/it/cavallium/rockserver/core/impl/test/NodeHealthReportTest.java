package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.NodeHealthReport;
import it.cavallium.rockserver.core.cluster.NodeHealthStatus;
import it.cavallium.rockserver.core.cluster.NodeId;

import org.junit.jupiter.api.Test;

class NodeHealthReportTest {

    @Test
    void reportsFreshnessAndCapacityEligibility() {
        var report = new NodeHealthReport(
                NodeId.of("n1"),
                NodeHealthStatus.HEALTHY,
                1_000,
                80,
                100,
                40,
                100,
                3,
                0.25d
        );

        assertTrue(report.isFresh(1_500, 600));
        assertFalse(report.isFresh(1_700, 600));
        assertTrue(report.canAcceptReplica(64, 0.50d, 1_500, 600));
        assertFalse(report.canAcceptReplica(90, 0.50d, 1_500, 600));
        assertEquals(0.20d, report.diskUtilization(), 0.000_001d);
        assertEquals(0.40d, report.memoryUtilization(), 0.000_001d);
    }

    @Test
    void unavailableOrStaleNodesCannotAcceptReplicas() {
        var unavailable = new NodeHealthReport(
                NodeId.of("n1"),
                NodeHealthStatus.UNAVAILABLE,
                1_000,
                80,
                100,
                40,
                100,
                3,
                0.25d
        );

        assertFalse(unavailable.canAcceptReplica(64, 0.50d, 1_500, 600));
        assertFalse(unavailable.canAcceptReplica(64, 0.50d, 1_700, 600));
    }

    @Test
    void rejectsInvalidReportValues() {
        assertThrows(IllegalArgumentException.class, () -> new NodeHealthReport(
                NodeId.of("n1"), NodeHealthStatus.HEALTHY, 1, 101, 100, 1, 2, 0, 0.1d));
        assertThrows(IllegalArgumentException.class, () -> new NodeHealthReport(
                NodeId.of("n1"), NodeHealthStatus.HEALTHY, 1, 1, 100, 3, 2, 0, 0.1d));
        assertThrows(IllegalArgumentException.class, () -> new NodeHealthReport(
                NodeId.of("n1"), NodeHealthStatus.HEALTHY, 1, 1, 100, 1, 2, -1, 0.1d));
        assertThrows(IllegalArgumentException.class, () -> new NodeHealthReport(
                NodeId.of("n1"), NodeHealthStatus.HEALTHY, 1, 1, 100, 1, 2, 0, 1.1d));
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("nodeId must not be null", () -> new NodeHealthReport(
                null, NodeHealthStatus.HEALTHY, 1, 1, 100, 1, 2, 0, 0.1d));
        assertIllegalArgument("status must not be null", () -> new NodeHealthReport(
                NodeId.of("n1"), null, 1, 1, 100, 1, 2, 0, 0.1d));
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var failure = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, failure.getMessage());
    }
}
