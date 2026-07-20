package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionReplicaStorageLayout;
import it.cavallium.rockserver.core.cluster.ProcessMemoryBudget;
import it.cavallium.rockserver.core.cluster.ShardId;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class PartitionReplicaStorageLayoutTest {

    @Test
    @SuppressWarnings("DataFlowIssue")
    void storageLayoutRejectsNullInputsWithExplicitDiagnostics() {
        var root = Path.of("/tmp/rockserver");
        var nodeId = NodeId.of("n1");

        assertIllegalArgument("rootDirectory must not be null",
                () -> new PartitionReplicaStorageLayout(null, nodeId, ShardId.DEFAULT, 1));
        assertIllegalArgument("nodeId must not be null",
                () -> new PartitionReplicaStorageLayout(root, null, ShardId.DEFAULT, 1));
        assertIllegalArgument("partitionId must not be null",
                () -> new PartitionReplicaStorageLayout(root, nodeId, null, 1));
    }

    @Test
    void assignsOneRocksDbPathPerPartitionReplica() {
        var root = Path.of("/var/lib/rockserver");
        var first = new PartitionReplicaStorageLayout(root, NodeId.of("node/1"), new ShardId(1), 3);
        var second = new PartitionReplicaStorageLayout(root, NodeId.of("node/1"), new ShardId(2), 3);

        assertEquals(root.resolve("nodes/node%2F1/partitions/partition-1/epoch-3/rocksdb"),
                first.rocksDbPath());
        assertEquals(root.resolve("nodes/node%2F1/partitions/partition-1/epoch-3/ratis"),
                first.ratisStoragePath());
        assertEquals(root.resolve("nodes/node%2F1/partitions/partition-1/epoch-3/snapshots"),
                first.snapshotsPath());
        assertNotEquals(first.rocksDbPath(), second.rocksDbPath());
    }

    @Test
    void storageLayoutRejectsNegativeEpoch() {
        assertThrows(IllegalArgumentException.class,
                () -> new PartitionReplicaStorageLayout(Path.of("/tmp/rockserver"), NodeId.of("n1"),
                        ShardId.DEFAULT, -1));
    }

    @Test
    void processMemoryBudgetTracksReservedAndPerPartitionBytes() {
        var budget = new ProcessMemoryBudget(1024, 256, 512, 4);

        assertEquals(768, budget.reservedBytes());
        assertEquals(256, budget.unreservedBytes());
        assertEquals(128, budget.writeBufferBytesPerPartitionReplica());
    }

    @Test
    void processMemoryBudgetRejectsInvalidBudgets() {
        assertThrows(IllegalArgumentException.class, () -> new ProcessMemoryBudget(0, 0, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new ProcessMemoryBudget(1024, -1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new ProcessMemoryBudget(1024, 0, -1, 1));
        assertThrows(IllegalArgumentException.class, () -> new ProcessMemoryBudget(1024, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new ProcessMemoryBudget(1024, 800, 300, 1));
    }

    private static void assertIllegalArgument(String expectedMessage, org.junit.jupiter.api.function.Executable action) {
        var failure = assertThrows(IllegalArgumentException.class, action);

        assertEquals(expectedMessage, failure.getMessage());
    }
}
