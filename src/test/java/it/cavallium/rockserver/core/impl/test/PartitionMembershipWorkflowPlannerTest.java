package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ClusterNodeMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.workflow.MembershipChangeKind;
import it.cavallium.rockserver.core.cluster.workflow.PartitionMembershipChangePlan;
import it.cavallium.rockserver.core.cluster.workflow.PartitionMembershipWorkflowPlanner;
import it.cavallium.rockserver.core.cluster.workflow.ReplicaCatchUpPlan;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class PartitionMembershipWorkflowPlannerTest {

    private static final ShardId PARTITION = new ShardId(7);

    private final PartitionMembershipWorkflowPlanner planner = new PartitionMembershipWorkflowPlanner();

    @Test
    void plansAddReplicaAsLearnerOnly() {
        var metadata = metadata(partition(voter("n1", 10, 10), voter("n2", 10, 10), voter("n3", 10, 10)));
        var learner = learner("n4", 0, 0);

        var plan = planner.planAddReplica(metadata, 11, PARTITION, 5, learner);

        assertEquals(MembershipChangeKind.ADD_REPLICA, plan.kind());
        assertEquals(11, plan.command().expectedMetadataEpoch());
        assertEquals(5, plan.command().expectedPartitionEpoch());
        assertEquals(List.of("n1", "n2", "n3", "n4"), plan.command().peers().stream()
                .map(peer -> peer.nodeId().value())
                .toList());
        assertThrows(IllegalArgumentException.class,
                () -> planner.planAddReplica(metadata, 11, PARTITION, 5, voter("n4", 0, 0)));
        assertThrows(IllegalArgumentException.class,
                () -> planner.planAddReplica(metadata, 11, PARTITION, 5,
                        peer("n4", ReplicaRole.LEARNER, 0, 0, true)));
    }

    @Test
    void addReplicaRequiresRegisteredNodeWhenNodeMapIsAvailable() {
        var partition = partition(voter("n1", 10, 10), voter("n2", 10, 10), voter("n3", 10, 10));
        var authoritativeMetadata = metadataWithNodes(partition, "n1", "n2", "n3");

        var failure = assertThrows(IllegalArgumentException.class,
                () -> planner.planAddReplica(authoritativeMetadata, 11, PARTITION, 5, learner("n4", 0, 0)));

        assertEquals("replica node is not registered in cluster metadata: n4", failure.getMessage());

        var registeredMetadata = metadataWithNodes(partition, "n1", "n2", "n3", "n4");
        var wrongEndpoint = new ReplicaMember(
                NodeId.of("n4"),
                URI.create("grpc://127.0.0.1:19999"),
                ReplicaRole.LEARNER,
                0,
                0,
                false
        );
        var endpointFailure = assertThrows(IllegalArgumentException.class,
                () -> planner.planAddReplica(registeredMetadata, 11, PARTITION, 5, wrongEndpoint));
        assertEquals("replica endpoint does not match registered node Ratis endpoint: n4",
                endpointFailure.getMessage());

        var plan = planner.planAddReplica(registeredMetadata, 11, PARTITION, 5, learner("n4", 0, 0));

        assertEquals(List.of("n1", "n2", "n3", "n4"), plan.command().peers().stream()
                .map(peer -> peer.nodeId().value())
                .toList());
    }

    @Test
    void plansCatchUpAndPromotesOnlyCaughtUpLearners() {
        var lagging = metadata(partition(voter("n1", 10, 10), voter("n2", 10, 10), learner("n4", 9, 9)));
        var caughtUp = metadata(partition(voter("n1", 10, 10), voter("n2", 10, 10), learner("n4", 10, 10)));

        var catchUp = planner.planCatchUp(lagging, 11, PARTITION, 5, NodeId.of("n4"));

        assertEquals(PARTITION, catchUp.partitionId());
        assertEquals(10, catchUp.requiredMatchIndex());
        assertEquals(10, catchUp.requiredAppliedIndex());
        assertThrows(IllegalStateException.class,
                () -> planner.planPromoteReplica(lagging, 11, PARTITION, 5, NodeId.of("n4")));

        var promote = planner.planPromoteReplica(caughtUp, 11, PARTITION, 5, NodeId.of("n4"));

        assertEquals(MembershipChangeKind.PROMOTE_REPLICA, promote.kind());
        assertEquals(ReplicaRole.VOTER, promote.command().peers().stream()
                .filter(peer -> peer.nodeId().equals(NodeId.of("n4")))
                .findFirst()
                .orElseThrow()
                .role());
    }

    @Test
    void plansRemoveReplicaWithQuorumAndLeaderConstraints() {
        var metadata = metadata(partition(voter("n1", 10, 10), voter("n2", 10, 10), voter("n3", 10, 10)));

        var removeFollower = planner.planRemoveReplica(metadata, 11, PARTITION, 5, NodeId.of("n3"), null);

        assertEquals(MembershipChangeKind.REMOVE_REPLICA, removeFollower.kind());
        assertEquals(List.of("n1", "n2"), removeFollower.command().peers().stream()
                .map(peer -> peer.nodeId().value())
                .toList());
        assertEquals(NodeId.of("n1"), removeFollower.command().leaderHint());

        var removeLeader = planner.planRemoveReplica(metadata, 11, PARTITION, 5, NodeId.of("n1"), NodeId.of("n2"));

        assertEquals(NodeId.of("n2"), removeLeader.command().leaderHint());
        assertThrows(IllegalArgumentException.class,
                () -> planner.planRemoveReplica(metadata, 11, PARTITION, 5, NodeId.of("n1"), null));
        var nonServingReplacement = metadata(partition(voter("n1", 10, 10), nonServingVoter("n2", 10, 10),
                voter("n3", 10, 10)));
        var replacementNotServing = assertThrows(IllegalArgumentException.class,
                () -> planner.planRemoveReplica(nonServingReplacement, 11, PARTITION, 5, NodeId.of("n1"),
                        NodeId.of("n2")));
        assertEquals("replacement leader must be serving reads", replacementNotServing.getMessage());
        var hintedFollowerRemoval = assertThrows(IllegalArgumentException.class,
                () -> planner.planRemoveReplica(nonServingReplacement, 11, PARTITION, 5, NodeId.of("n3"),
                        NodeId.of("n2")));
        assertEquals("new leader must be serving reads", hintedFollowerRemoval.getMessage());

        var twoVoters = metadata(partition(voter("n1", 10, 10), voter("n2", 10, 10)));
        assertThrows(IllegalStateException.class,
                () -> planner.planRemoveReplica(twoVoters, 11, PARTITION, 5, NodeId.of("n2"), null));
    }

    @Test
    void plansLeaderTransferOnlyToCaughtUpServingVoter() {
        var metadata = metadata(partition(voter("n1", 10, 10), voter("n2", 10, 10), learner("n4", 10, 10)));

        var transfer = planner.planLeaderTransfer(metadata, 11, PARTITION, 5, NodeId.of("n2"));

        assertEquals(MembershipChangeKind.TRANSFER_LEADER, transfer.kind());
        assertEquals(NodeId.of("n2"), transfer.command().leaderHint());
        assertThrows(IllegalArgumentException.class,
                () -> planner.planLeaderTransfer(metadata, 11, PARTITION, 5, NodeId.of("n4")));

        var lagging = metadata(partition(voter("n1", 10, 10), voter("n2", 9, 9)));
        assertThrows(IllegalStateException.class,
                () -> planner.planLeaderTransfer(lagging, 11, PARTITION, 5, NodeId.of("n2")));
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var metadata = metadata(partition(voter("n1", 10, 10), voter("n2", 10, 10), learner("n4", 10, 10)));

        assertIllegalArgument("newReplica must not be null",
                () -> planner.planAddReplica(metadata, 11, PARTITION, 5, null));
        assertIllegalArgument("metadata must not be null",
                () -> planner.planCatchUp(null, 11, PARTITION, 5, NodeId.of("n4")));
        assertIllegalArgument("partitionId must not be null",
                () -> planner.planCatchUp(metadata, 11, null, 5, NodeId.of("n4")));
        assertIllegalArgument("nodeId must not be null",
                () -> planner.planCatchUp(metadata, 11, PARTITION, 5, null));
        assertIllegalArgument("targetLeaderId must not be null",
                () -> planner.planLeaderTransfer(metadata, 11, PARTITION, 5, null));

        var command = planner.planAddReplica(metadata, 11, PARTITION, 5, learner("n5", 0, 0)).command();
        assertIllegalArgument("kind must not be null", () -> new PartitionMembershipChangePlan(null, command));
        assertIllegalArgument("command must not be null",
                () -> new PartitionMembershipChangePlan(MembershipChangeKind.ADD_REPLICA, null));
        assertIllegalArgument("partitionId must not be null",
                () -> new ReplicaCatchUpPlan(null, 5, NodeId.of("n4"), 10, 10));
        assertIllegalArgument("replicaId must not be null",
                () -> new ReplicaCatchUpPlan(PARTITION, 5, null, 10, 10));
    }

    private static ClusterMetadata metadata(PartitionDescriptor partition) {
        return new ClusterMetadata(new ClusterId(new UUID(0, 1)), 11, Map.of(), Map.of(), Map.of(
                partition.partitionId(), partition
        ));
    }

    private static ClusterMetadata metadataWithNodes(PartitionDescriptor partition, String... nodeIds) {
        var nodes = new LinkedHashMap<NodeId, ClusterNodeMetadata>();
        for (String nodeId : nodeIds) {
            var node = node(nodeId);
            nodes.put(node.nodeId(), node);
        }
        return new ClusterMetadata(new ClusterId(new UUID(0, 1)), 11, Map.of(), Map.of(), Map.of(
                partition.partitionId(), partition
        ), nodes);
    }

    private static ClusterNodeMetadata node(String nodeId) {
        var node = NodeId.of(nodeId);
        var port = 20_000 + Integer.parseInt(nodeId.substring(1));
        return new ClusterNodeMetadata(
                node,
                URI.create("http://127.0.0.1:" + port),
                URI.create("grpc://127.0.0.1:" + (10_000 + Integer.parseInt(nodeId.substring(1)))),
                "rack-a"
        );
    }

    private static PartitionDescriptor partition(ReplicaMember... peers) {
        return new PartitionDescriptor(
                PARTITION,
                null,
                null,
                5,
                "group-7",
                List.of(peers),
                NodeId.of("n1"),
                Map.of("rack", "rack-a"),
                PartitionSplitState.ACTIVE
        );
    }

    private static ReplicaMember voter(String nodeId, long matchIndex, long appliedIndex) {
        return peer(nodeId, ReplicaRole.VOTER, matchIndex, appliedIndex);
    }

    private static ReplicaMember nonServingVoter(String nodeId, long matchIndex, long appliedIndex) {
        return peer(nodeId, ReplicaRole.VOTER, matchIndex, appliedIndex, false);
    }

    private static ReplicaMember learner(String nodeId, long matchIndex, long appliedIndex) {
        return peer(nodeId, ReplicaRole.LEARNER, matchIndex, appliedIndex);
    }

    private static ReplicaMember peer(String nodeId, ReplicaRole role, long matchIndex, long appliedIndex) {
        return peer(nodeId, role, matchIndex, appliedIndex, role == ReplicaRole.VOTER);
    }

    private static ReplicaMember peer(String nodeId,
                                      ReplicaRole role,
                                      long matchIndex,
                                      long appliedIndex,
                                      boolean servingReads) {
        var node = NodeId.of(nodeId);
        var port = 10_000 + Integer.parseInt(nodeId.substring(1));
        return new ReplicaMember(
                node,
                URI.create("grpc://127.0.0.1:" + port),
                role,
                matchIndex,
                appliedIndex,
                servingReads
        );
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var failure = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, failure.getMessage());
    }
}
