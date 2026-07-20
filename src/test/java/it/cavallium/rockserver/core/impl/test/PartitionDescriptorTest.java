package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class PartitionDescriptorTest {

    @Test
    void containsCalculatedKeysInsideUnsignedHalfOpenRange() {
        var descriptor = descriptor(buf(0x10), buf(0x20), PartitionSplitState.ACTIVE);

        assertFalse(descriptor.containsCalculatedKey(buf(0x0f)));
        assertTrue(descriptor.containsCalculatedKey(buf(0x10)));
        assertTrue(descriptor.containsCalculatedKey(buf(0x1f)));
        assertFalse(descriptor.containsCalculatedKey(buf(0x20)));
    }

    @Test
    void acceptsOpenStartAndEndRange() {
        var descriptor = descriptor(null, null, PartitionSplitState.ACTIVE);

        assertTrue(descriptor.containsCalculatedKey(buf(0x00)));
        assertTrue(descriptor.containsCalculatedKey(buf(0xff)));
    }

    @Test
    void rejectsEmptyOrInvertedRange() {
        assertThrows(IllegalArgumentException.class,
                () -> descriptor(buf(0x10), buf(0x10), PartitionSplitState.ACTIVE));
        assertThrows(IllegalArgumentException.class,
                () -> descriptor(buf(0x20), buf(0x10), PartitionSplitState.ACTIVE));
    }

    @Test
    void rejectsInvalidRatisGroupId() {
        assertThrows(IllegalArgumentException.class,
                () -> descriptor(" ", null, null, PartitionSplitState.ACTIVE));
        assertThrows(IllegalArgumentException.class,
                () -> descriptor("group-1\ninjected=true", null, null, PartitionSplitState.ACTIVE));
    }

    @Test
    void rejectsInvalidPlacementConstraints() {
        assertInvalidPlacementConstraints(Map.of(" ", "rack-a"));
        assertInvalidPlacementConstraints(Map.of("rack", "rack-a\ninjected=true"));
    }

    @Test
    void rejectsLeaderHintOutsidePeerSet() {
        assertThrows(IllegalArgumentException.class, () -> new PartitionDescriptor(
                ShardId.DEFAULT,
                null,
                null,
                1,
                "group-1",
                List.of(peer(NodeId.of("n1"))),
                NodeId.of("n2"),
                Map.of("rack", "rack-a"),
                PartitionSplitState.ACTIVE
        ));
    }

    @Test
    void rejectsLeaderHintForNonVotingPeer() {
        var leader = NodeId.of("n1");

        assertThrows(IllegalArgumentException.class, () -> new PartitionDescriptor(
                ShardId.DEFAULT,
                null,
                null,
                1,
                "group-1",
                List.of(peer(leader, ReplicaRole.LEARNER)),
                leader,
                Map.of("rack", "rack-a"),
                PartitionSplitState.ACTIVE
        ));
    }

    @Test
    void rejectsDuplicatePeerNodeIds() {
        var leader = NodeId.of("n1");

        assertThrows(IllegalArgumentException.class, () -> new PartitionDescriptor(
                ShardId.DEFAULT,
                null,
                null,
                1,
                "group-1",
                List.of(peer(leader), peer(leader)),
                leader,
                Map.of("rack", "rack-a"),
                PartitionSplitState.ACTIVE
        ));
    }

    @Test
    void exposesSplitState() {
        assertFalse(descriptor(null, null, PartitionSplitState.ACTIVE).isSplitting());
        assertTrue(descriptor(null, null, PartitionSplitState.SPLIT_SOURCE).isSplitting());
    }

    private static PartitionDescriptor descriptor(Buf startKeyInclusive,
                                                  Buf endKeyExclusive,
                                                  PartitionSplitState splitState) {
        return descriptor("group-1", startKeyInclusive, endKeyExclusive, splitState);
    }

    private static PartitionDescriptor descriptor(String ratisGroupId,
                                                  Buf startKeyInclusive,
                                                  Buf endKeyExclusive,
                                                  PartitionSplitState splitState) {
        return descriptor(ratisGroupId, Map.of("rack", "rack-a"), startKeyInclusive, endKeyExclusive, splitState);
    }

    private static PartitionDescriptor descriptor(String ratisGroupId,
                                                  Map<String, String> placementConstraints,
                                                  Buf startKeyInclusive,
                                                  Buf endKeyExclusive,
                                                  PartitionSplitState splitState) {
        var leader = NodeId.of("n1");
        return new PartitionDescriptor(
                ShardId.DEFAULT,
                startKeyInclusive,
                endKeyExclusive,
                1,
                ratisGroupId,
                List.of(peer(leader)),
                leader,
                placementConstraints,
                splitState
        );
    }

    private static void assertInvalidPlacementConstraints(Map<String, String> placementConstraints) {
        assertThrows(IllegalArgumentException.class, () -> {
            var leader = NodeId.of("n1");
            new PartitionDescriptor(
                    ShardId.DEFAULT,
                    null,
                    null,
                    1,
                    "group-1",
                    List.of(peer(leader)),
                    leader,
                    placementConstraints,
                    PartitionSplitState.ACTIVE
            );
        });
    }

    private static ReplicaMember peer(NodeId nodeId) {
        return peer(nodeId, ReplicaRole.VOTER);
    }

    private static ReplicaMember peer(NodeId nodeId, ReplicaRole role) {
        return new ReplicaMember(nodeId, URI.create("http://127.0.0.1:10001"), role, 0, 0,
                role == ReplicaRole.VOTER);
    }

    private static Buf buf(int... values) {
        var bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return Buf.wrap(bytes);
    }
}
