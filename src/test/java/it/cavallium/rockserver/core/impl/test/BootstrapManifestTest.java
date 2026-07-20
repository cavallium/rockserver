package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapManifest;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapPeer;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class BootstrapManifestTest {

    @Test
    void recordsClusterAndMetadataGroupBootstrapInputs() {
        var clusterId = new ClusterId(new UUID(0, 1));
        var n1 = peer("n1", 10001, 11001, "rack-a");
        var n2 = peer("n2", 10002, 11002, "rack-b");
        var manifest = new BootstrapManifest(clusterId, "metadata-group", List.of(n1, n2), 9);

        assertEquals(clusterId, manifest.clusterId());
        assertEquals("metadata-group", manifest.metadataGroupId());
        assertEquals(9, manifest.bootstrapEpoch());
        assertEquals(Set.of(NodeId.of("n1"), NodeId.of("n2")), manifest.peerIds());
        assertEquals(n1, manifest.findPeer(NodeId.of("n1")).orElseThrow());
    }

    @Test
    void exposesMetadataGroupMembersAsInitialVoters() {
        var manifest = new BootstrapManifest(new ClusterId(new UUID(0, 1)), "metadata-group",
                List.of(peer("n1", 10001, 11001, "rack-a")), 0);

        var member = manifest.metadataGroupMembers().getFirst();

        assertEquals(NodeId.of("n1"), member.nodeId());
        assertEquals(URI.create("grpc://127.0.0.1:11001"), member.endpoint());
        assertEquals(ReplicaRole.VOTER, member.role());
        assertTrue(member.isVoter());
    }

    @Test
    void rejectsEmptyPeerSet() {
        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapManifest(new ClusterId(new UUID(0, 1)), "metadata-group", List.of(), 0));
    }

    @Test
    void rejectsDuplicateNodeIdsAndEndpoints() {
        var n1 = peer("n1", 10001, 11001, "rack-a");

        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapManifest(new ClusterId(new UUID(0, 1)), "metadata-group", List.of(
                        n1,
                        peer("n1", 10002, 11002, "rack-b")
                ), 0));
        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapManifest(new ClusterId(new UUID(0, 1)), "metadata-group", List.of(
                        n1,
                        peer("n2", 10001, 11002, "rack-b")
                ), 0));
        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapManifest(new ClusterId(new UUID(0, 1)), "metadata-group", List.of(
                        n1,
                        peer("n2", 10002, 11001, "rack-b")
                ), 0));
    }

    @Test
    void rejectsInvalidGroupAndFailureDomain() {
        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapManifest(new ClusterId(new UUID(0, 1)), " ", List.of(
                        peer("n1", 10001, 11001, "rack-a")
                ), 0));
        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapManifest(new ClusterId(new UUID(0, 1)), "metadata\ninjected=true", List.of(
                        peer("n1", 10001, 11001, "rack-a")
                ), 0));
        assertThrows(IllegalArgumentException.class, () -> peer("n1", 10001, 11001, " "));
        assertThrows(IllegalArgumentException.class,
                () -> peer("n1", 10001, 11001, "rack-a\ninjected=true"));
    }

    @Test
    void rejectsInvalidPeerEndpoints() {
        assertThrows(IllegalArgumentException.class, () -> new BootstrapPeer(
                NodeId.of("n1"),
                URI.create("http://127.0.0.1"),
                URI.create("grpc://127.0.0.1:11001"),
                "rack-a"
        ));
        assertThrows(IllegalArgumentException.class, () -> new BootstrapPeer(
                NodeId.of("n1"),
                URI.create("http://127.0.0.1:10001"),
                URI.create("http://127.0.0.1:11001"),
                "rack-a"
        ));
        assertThrows(IllegalArgumentException.class, () -> new BootstrapPeer(
                NodeId.of("n1"),
                URI.create("http://127.0.0.1:10001"),
                URI.create("grpc://127.0.0.1:11001/ratis"),
                "rack-a"
        ));
    }

    @Test
    void rejectsNegativeBootstrapEpoch() {
        assertThrows(IllegalArgumentException.class,
                () -> new BootstrapManifest(new ClusterId(new UUID(0, 1)), "metadata-group", List.of(
                        peer("n1", 10001, 11001, "rack-a")
                ), -1));
    }

    private static BootstrapPeer peer(String nodeId, int publicPort, int ratisPort, String failureDomain) {
        return new BootstrapPeer(
                NodeId.of(nodeId),
                URI.create("http://127.0.0.1:" + publicPort),
                URI.create("grpc://127.0.0.1:" + ratisPort),
                failureDomain
        );
    }
}
