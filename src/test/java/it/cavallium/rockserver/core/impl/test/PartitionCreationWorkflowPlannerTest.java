package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ClusterNodeMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaGroupMetadata;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandApplier;
import it.cavallium.rockserver.core.cluster.workflow.PartitionCreationPlan;
import it.cavallium.rockserver.core.cluster.workflow.PartitionCreationWorkflowPlanner;
import it.cavallium.rockserver.core.replication.ratis.RatisPeerDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterConfig;

import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PartitionCreationWorkflowPlannerTest {

    private static final ClusterId CLUSTER_ID = new ClusterId(new UUID(0, 1));
    private static final NodeId N1 = NodeId.of("n1");
    private static final NodeId N2 = NodeId.of("n2");
    private static final ShardId PARTITION = new ShardId(7);

    private final PartitionCreationWorkflowPlanner planner = new PartitionCreationWorkflowPlanner();

    @Test
    void plansCreatePartitionFromRegisteredVotersAndAdapterConsumesCommittedState(@TempDir Path tempDir) {
        var metadata = metadata(List.of(
                node(N1, "grpc://10.0.0.1:19001", "rack-a"),
                node(N2, "grpc://10.0.0.2:19002", "rack-b")
        ), List.of(), Map.of());

        var plan = planner.planCreatePartition(
                metadata,
                11,
                PARTITION,
                null,
                null,
                1,
                "group-7",
                List.of(N1, N2),
                N1,
                Map.of("placement", "initial")
        );

        var command = plan.command();
        assertEquals(11, command.expectedMetadataEpoch());
        var partition = command.partition();
        assertEquals(PARTITION, partition.partitionId());
        assertEquals("group-7", partition.ratisGroupId());
        assertEquals(N1, partition.leaderHint());
        assertEquals(PartitionSplitState.ACTIVE, partition.splitState());
        assertEquals(List.of(
                member(N1, "grpc://10.0.0.1:19001"),
                member(N2, "grpc://10.0.0.2:19002")
        ), partition.peers());

        var committed = MetadataCommandApplier.apply(metadata, command);

        assertEquals(12, committed.epoch());
        assertEquals(partition, committed.requirePartition(PARTITION));
        var routeGroup = committed.requireGroup(PARTITION);
        assertEquals(N1, routeGroup.leader());
        assertEquals(partition.peers(), routeGroup.members());
        assertEquals(partition.epoch(), routeGroup.configIndex());

        var adapterConfig = RatisProcessAdapterConfig.fromMetadata(committed, N1, tempDir.resolve("adapter-data"));
        assertEquals(1, adapterConfig.dataPartitions().size());
        var adapterPartition = adapterConfig.dataPartitions().getFirst();
        assertEquals("7", adapterPartition.partitionId());
        assertEquals("group-7", adapterPartition.raftGroupId());
        assertEquals("n1", adapterPartition.localPeerId());
        assertEquals(tempDir.resolve("adapter-data").resolve("partition-7"), adapterPartition.dataPath());
        assertEquals(List.of(
                new RatisPeerDescriptor("n1", "grpc://10.0.0.1:19001", "rack-a"),
                new RatisPeerDescriptor("n2", "grpc://10.0.0.2:19002", "rack-b")
        ), adapterPartition.voters());
    }

    @Test
    void rejectsInvalidInitialPartitionPlans() {
        var metadata = metadata(List.of(
                node(N1, "grpc://10.0.0.1:19001", "rack-a"),
                node(N2, "grpc://10.0.0.2:19002", "rack-b")
        ), List.of(), Map.of());

        assertThrows(IllegalArgumentException.class, () -> planner.planCreatePartition(
                metadata, 10, PARTITION, null, null, 1, "group-7", List.of(N1), N1, Map.of()));
        assertThrows(IllegalArgumentException.class, () -> planner.planCreatePartition(
                metadata, 11, PARTITION, null, null, 1, "group-7", List.of(N1, N1), N1, Map.of()));
        assertThrows(IllegalArgumentException.class, () -> planner.planCreatePartition(
                metadata, 11, PARTITION, null, null, 1, "group-7", List.of(N1), N2, Map.of()));
        assertThrows(IllegalArgumentException.class, () -> planner.planCreatePartition(
                metadata, 11, PARTITION, null, null, 1, "group-7", List.of(NodeId.of("missing")), N1, Map.of()));
        assertThrows(IllegalArgumentException.class, () -> planner.planCreatePartition(
                metadata, 11, PARTITION, null, null, 1, "group-7\ninjected=true", List.of(N1), N1, Map.of()));
        assertThrows(IllegalArgumentException.class, () -> planner.planCreatePartition(
                metadata, 11, PARTITION, null, null, 1, "group-7", List.of(N1), N1,
                Map.of("rack\ninjected=true", "rack-a")));
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var metadata = metadata(List.of(
                node(N1, "grpc://10.0.0.1:19001", "rack-a"),
                node(N2, "grpc://10.0.0.2:19002", "rack-b")
        ), List.of(), Map.of());

        assertIllegalArgument("metadata must not be null", () -> planner.planCreatePartition(
                null, 11, PARTITION, null, null, 1, "group-7", List.of(N1), N1, Map.of()));
        assertIllegalArgument("partitionId must not be null", () -> planner.planCreatePartition(
                metadata, 11, null, null, null, 1, "group-7", List.of(N1), N1, Map.of()));
        assertIllegalArgument("voterNodeIds must not be null", () -> planner.planCreatePartition(
                metadata, 11, PARTITION, null, null, 1, "group-7", null, N1, Map.of()));
        assertIllegalArgument("leaderHint must not be null", () -> planner.planCreatePartition(
                metadata, 11, PARTITION, null, null, 1, "group-7", List.of(N1), null, Map.of()));
        assertIllegalArgument("placementConstraints must not be null", () -> planner.planCreatePartition(
                metadata, 11, PARTITION, null, null, 1, "group-7", List.of(N1), N1, null));
        assertIllegalArgument("voterNodeIds entry must not be null", () -> planner.planCreatePartition(
                metadata, 11, PARTITION, null, null, 1, "group-7", Collections.singletonList(null), N1,
                Map.of()));
        assertIllegalArgument("command must not be null", () -> new PartitionCreationPlan(null));
    }

    @Test
    void rejectsOverlappingRangesAndExistingRouteGroups() {
        var existing = partition(new ShardId(1), buf(0x10), buf(0x20));
        var metadata = metadata(List.of(node(N1, "grpc://10.0.0.1:19001", "rack-a")),
                List.of(existing), Map.of());

        assertThrows(IllegalArgumentException.class, () -> planner.planCreatePartition(
                metadata, 11, PARTITION, buf(0x18), buf(0x30), 1, "group-7", List.of(N1), N1, Map.of()));

        var routeGroup = new ReplicaGroupMetadata(PARTITION, 1, N1, List.of(member(N1, "grpc://10.0.0.1:19001")), 1);
        var metadataWithRouteGroup = metadata(List.of(node(N1, "grpc://10.0.0.1:19001", "rack-a")),
                List.of(), Map.of(PARTITION, routeGroup));

        assertThrows(IllegalArgumentException.class, () -> planner.planCreatePartition(
                metadataWithRouteGroup, 11, PARTITION, null, null, 1, "group-7", List.of(N1), N1, Map.of()));
    }

    private static ClusterMetadata metadata(List<ClusterNodeMetadata> nodes,
                                            List<PartitionDescriptor> partitions,
                                            Map<ShardId, ReplicaGroupMetadata> routeGroups) {
        return new ClusterMetadata(
                CLUSTER_ID,
                11,
                routeGroups,
                Map.of(),
                partitions.stream().collect(Collectors.toMap(
                        PartitionDescriptor::partitionId,
                        partition -> partition
                )),
                nodes.stream().collect(Collectors.toMap(
                        ClusterNodeMetadata::nodeId,
                        node -> node
                ))
        );
    }

    private static PartitionDescriptor partition(ShardId partitionId,
                                                 Buf startKeyInclusive,
                                                 Buf endKeyExclusive) {
        return new PartitionDescriptor(
                partitionId,
                startKeyInclusive,
                endKeyExclusive,
                1,
                "group-" + partitionId,
                List.of(member(N1, "grpc://10.0.0.1:19001")),
                N1,
                Map.of(),
                PartitionSplitState.ACTIVE
        );
    }

    private static ReplicaMember member(NodeId nodeId, String endpoint) {
        return new ReplicaMember(nodeId, URI.create(endpoint), ReplicaRole.VOTER, 0, 0, true);
    }

    private static ClusterNodeMetadata node(NodeId nodeId, String ratisEndpoint, String failureDomain) {
        return new ClusterNodeMetadata(
                nodeId,
                URI.create("http://" + nodeId + ".public:10000"),
                URI.create(ratisEndpoint),
                failureDomain
        );
    }

    private static Buf buf(int... values) {
        var bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return Buf.wrap(bytes);
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var failure = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, failure.getMessage());
    }
}
