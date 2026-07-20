package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaGroupMetadata;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.metadata.MetadataGroupStartupPolicy;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class MetadataGroupStartupPolicyTest {

    private static final ClusterId CLUSTER_ID = new ClusterId(new UUID(0, 1));
    private static final NodeId N1 = NodeId.of("n1");
    private static final ShardId PARTITION = new ShardId(7);

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("metadata must not be null",
                () -> MetadataGroupStartupPolicy.requiresSnapshot(null));
        assertIllegalArgument("metadata must not be null",
                () -> MetadataGroupStartupPolicy.isNoSnapshotReplaySeed(null));
    }

    @Test
    void allowsBootstrapReplaySeedWithoutSnapshot() {
        var member = member(N1);
        var metadataGroupRoute = new ReplicaGroupMetadata(new ShardId(99), 0, N1, List.of(member), 0);
        var metadata = new ClusterMetadata(
                CLUSTER_ID,
                0,
                Map.of(metadataGroupRoute.shardId(), metadataGroupRoute),
                Map.of(),
                Map.of()
        );

        assertTrue(MetadataGroupStartupPolicy.isNoSnapshotReplaySeed(metadata));
        assertFalse(MetadataGroupStartupPolicy.requiresSnapshot(metadata));
    }

    @Test
    void requiresSnapshotForGeneratedLiveMetadata() {
        assertTrue(MetadataGroupStartupPolicy.requiresSnapshot(new ClusterMetadata(
                CLUSTER_ID,
                1,
                Map.of(),
                Map.of(),
                Map.of()
        )));
        assertTrue(MetadataGroupStartupPolicy.requiresSnapshot(new ClusterMetadata(
                CLUSTER_ID,
                0,
                Map.of(),
                Map.of(44L, column()),
                Map.of()
        )));

        var partition = partition();
        assertTrue(MetadataGroupStartupPolicy.requiresSnapshot(new ClusterMetadata(
                CLUSTER_ID,
                0,
                Map.of(PARTITION, new ReplicaGroupMetadata(PARTITION, 1, N1, partition.peers(), 1)),
                Map.of(),
                Map.of(PARTITION, partition)
        )));
    }

    @Test
    void requiresSnapshotForProgressedEpochZeroReplicaGroups() {
        assertTrue(MetadataGroupStartupPolicy.requiresSnapshot(new ClusterMetadata(
                CLUSTER_ID,
                0,
                Map.of(new ShardId(99), new ReplicaGroupMetadata(new ShardId(99), 1, N1, List.of(member(N1)), 0)),
                Map.of(),
                Map.of(),
                Map.of()
        )));
        assertTrue(MetadataGroupStartupPolicy.requiresSnapshot(new ClusterMetadata(
                CLUSTER_ID,
                0,
                Map.of(new ShardId(99), new ReplicaGroupMetadata(new ShardId(99), 0, N1, List.of(member(N1)), 1)),
                Map.of(),
                Map.of(),
                Map.of()
        )));
        assertTrue(MetadataGroupStartupPolicy.requiresSnapshot(new ClusterMetadata(
                CLUSTER_ID,
                0,
                Map.of(new ShardId(99), new ReplicaGroupMetadata(new ShardId(99), 0, N1,
                        List.of(member(N1, ReplicaRole.VOTER, 1, 1, true)), 0)),
                Map.of(),
                Map.of(),
                Map.of()
        )));
        assertTrue(MetadataGroupStartupPolicy.requiresSnapshot(new ClusterMetadata(
                CLUSTER_ID,
                0,
                Map.of(new ShardId(99), new ReplicaGroupMetadata(new ShardId(99), 0, N1,
                        List.of(member(N1, ReplicaRole.VOTER, 0, 0, false)), 0)),
                Map.of(),
                Map.of(),
                Map.of()
        )));
    }

    private static ClusterColumnMetadata column() {
        return new ClusterColumnMetadata(
                44,
                "accounts",
                ColumnSchema.of(IntList.of(1), ObjectList.of(), true),
                0
        );
    }

    private static PartitionDescriptor partition() {
        return new PartitionDescriptor(
                PARTITION,
                null,
                null,
                1,
                "group-7",
                List.of(member(N1)),
                N1,
                Map.of(),
                PartitionSplitState.ACTIVE
        );
    }

    private static ReplicaMember member(NodeId nodeId) {
        return member(nodeId, ReplicaRole.VOTER, 0, 0, true);
    }

    private static ReplicaMember member(NodeId nodeId,
                                        ReplicaRole role,
                                        long matchIndex,
                                        long appliedIndex,
                                        boolean servingReads) {
        return new ReplicaMember(nodeId, URI.create("grpc://127.0.0.1:19001"), role, matchIndex, appliedIndex,
                servingReads);
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }
}
