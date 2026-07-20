package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapManifest;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapMode;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapClusterMetadata;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapPeer;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapPlan;
import it.cavallium.rockserver.core.cluster.metadata.NodeIdentityStore;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BootstrapPlanTest {

    @Test
    void definesNewClusterBootstrap() {
        var manifest = manifest(clusterId(1), peer("n1", 10001, 11001));

        var plan = BootstrapPlan.newCluster(manifest);

        assertEquals(BootstrapMode.NEW_CLUSTER, plan.mode());
        assertEquals(manifest, plan.manifest());
        assertNull(plan.sourceClusterId());
    }

    @Test
    void freshClusterBootstrapFormsMetadataGroupAndRejectsMismatchedIdentity(@TempDir Path directory)
            throws Exception {
        var clusterId = clusterId(1);
        var localPeer = peer("n1", 10001, 11001);
        var remotePeer = peer("n2", 10002, 11002);
        var manifest = new BootstrapManifest(clusterId, "metadata-group", List.of(localPeer, remotePeer), 0);
        var plan = BootstrapPlan.newCluster(manifest);

        var metadataMembers = plan.manifest().metadataGroupMembers();
        var localIdentity = NodeIdentityStore.loadOrPersist(directory, clusterId, localPeer.nodeId());

        assertEquals(clusterId, localIdentity.clusterId());
        assertEquals(localPeer.nodeId(), localIdentity.nodeId());
        assertEquals(List.of(localPeer.nodeId(), remotePeer.nodeId()),
                metadataMembers.stream().map(member -> member.nodeId()).toList());
        assertEquals(List.of(ReplicaRole.VOTER, ReplicaRole.VOTER),
                metadataMembers.stream().map(member -> member.role()).toList());
        assertThrows(IllegalStateException.class,
                () -> NodeIdentityStore.loadOrPersist(directory, clusterId(2), localPeer.nodeId()));
        assertThrows(IllegalStateException.class,
                () -> NodeIdentityStore.loadOrPersist(directory, clusterId, remotePeer.nodeId()));
    }

    @Test
    void bootstrapPlanProjectsInitialClusterMetadata() {
        var clusterId = clusterId(1);
        var localPeer = peer("n1", 10001, 11001);
        var remotePeer = peer("n2", 10002, 11002);
        var plan = BootstrapPlan.newCluster(new BootstrapManifest(
                clusterId,
                "metadata-group",
                List.of(localPeer, remotePeer),
                3
        ));

        var metadata = BootstrapClusterMetadata.fromPlan(plan);

        assertEquals(clusterId, metadata.clusterId());
        assertEquals(3, metadata.epoch());
        assertTrue(metadata.partitionDescriptors().isEmpty());
        assertEquals(localPeer.publicEndpoint(), metadata.requireNode(localPeer.nodeId()).publicEndpoint());
        assertEquals(remotePeer.ratisEndpoint(), metadata.requireNode(remotePeer.nodeId()).ratisEndpoint());
    }

    @Test
    void definesSingleNodeDevelopmentBootstrap() {
        var clusterId = clusterId(1);
        var peer = peer("dev-node", 10001, 11001);

        var plan = BootstrapPlan.singleNodeDevelopment(clusterId, "metadata-dev", peer, 0);

        assertEquals(BootstrapMode.SINGLE_NODE_DEVELOPMENT, plan.mode());
        assertEquals(clusterId, plan.manifest().clusterId());
        assertEquals(List.of(peer), plan.manifest().initialPeers());
    }

    @Test
    void singleNodeDevelopmentRejectsMultiplePeers() {
        assertThrows(IllegalArgumentException.class, () -> new BootstrapPlan(
                BootstrapMode.SINGLE_NODE_DEVELOPMENT,
                new BootstrapManifest(clusterId(1), "metadata-dev", List.of(
                        peer("n1", 10001, 11001),
                        peer("n2", 10002, 11002)
                ), 0),
                null
        ));
    }

    @Test
    void definesDisasterRestoreIntoNewClusterId() {
        var sourceClusterId = clusterId(1);
        var restoreManifest = manifest(clusterId(2), peer("n1", 10001, 11001));

        var plan = BootstrapPlan.disasterRestore(sourceClusterId, restoreManifest);

        assertEquals(BootstrapMode.DISASTER_RESTORE, plan.mode());
        assertEquals(sourceClusterId, plan.sourceClusterId());
        assertEquals(restoreManifest, plan.manifest());
    }

    @Test
    void disasterRestoreRejectsSameClusterId() {
        var clusterId = clusterId(1);

        assertThrows(IllegalArgumentException.class,
                () -> BootstrapPlan.disasterRestore(clusterId, manifest(clusterId, peer("n1", 10001, 11001))));
    }

    @Test
    void sourceClusterIsOnlyValidForDisasterRestore() {
        assertThrows(IllegalArgumentException.class, () -> new BootstrapPlan(
                BootstrapMode.NEW_CLUSTER,
                manifest(clusterId(2), peer("n1", 10001, 11001)),
                clusterId(1)
        ));
    }

    private static BootstrapManifest manifest(ClusterId clusterId, BootstrapPeer peer) {
        return new BootstrapManifest(clusterId, "metadata-group", List.of(peer), 0);
    }

    private static BootstrapPeer peer(String nodeId, int publicPort, int ratisPort) {
        return new BootstrapPeer(
                NodeId.of(nodeId),
                URI.create("http://127.0.0.1:" + publicPort),
                URI.create("grpc://127.0.0.1:" + ratisPort),
                "rack-a"
        );
    }

    private static ClusterId clusterId(long lowBits) {
        return new ClusterId(new UUID(0, lowBits));
    }
}
