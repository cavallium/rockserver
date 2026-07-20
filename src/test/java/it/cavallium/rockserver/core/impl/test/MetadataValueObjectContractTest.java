package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcToken;
import it.cavallium.rockserver.core.cluster.metadata.BackupManifest;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapClusterMetadata;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapManifest;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapMode;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapPeer;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapPlan;
import it.cavallium.rockserver.core.cluster.metadata.CdcTokenTranslation;
import it.cavallium.rockserver.core.cluster.metadata.MergeOperatorActivation;
import it.cavallium.rockserver.core.cluster.metadata.NodeIdentity;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class MetadataValueObjectContractTest {

    @Test
    void bootstrapValuesRejectNullInputsWithExplicitDiagnostics() {
        var manifest = manifest(clusterId(1), peer("n1"));
        var peer = peer("n2");

        assertIllegalArgument("clusterId must not be null",
                () -> new BootstrapManifest(null, "metadata-group", List.of(peer), 0));
        assertIllegalArgument("initialPeers must not be null",
                () -> new BootstrapManifest(clusterId(1), "metadata-group", null, 0));
        assertIllegalArgument("initialPeers entry must not be null",
                () -> new BootstrapManifest(clusterId(1), "metadata-group", Collections.singletonList(null), 0));
        assertIllegalArgument("nodeId must not be null", () -> manifest.findPeer(null));

        assertIllegalArgument("nodeId must not be null",
                () -> new BootstrapPeer(null, publicEndpoint(10001), ratisEndpoint(11001), "rack-a"));
        assertIllegalArgument("bootstrap public endpoint must not be null",
                () -> new BootstrapPeer(NodeId.of("n1"), null, ratisEndpoint(11001), "rack-a"));
        assertIllegalArgument("bootstrap Ratis endpoint must not be null",
                () -> new BootstrapPeer(NodeId.of("n1"), publicEndpoint(10001), null, "rack-a"));

        assertIllegalArgument("mode must not be null", () -> new BootstrapPlan(null, manifest, null));
        assertIllegalArgument("manifest must not be null", () -> new BootstrapPlan(BootstrapMode.NEW_CLUSTER, null, null));
        assertIllegalArgument("sourceClusterId must not be null",
                () -> new BootstrapPlan(BootstrapMode.DISASTER_RESTORE, manifest(clusterId(2), peer), null));
        assertIllegalArgument("manifest must not be null", () -> BootstrapPlan.newCluster(null));
        assertIllegalArgument("plan must not be null", () -> BootstrapClusterMetadata.fromPlan(null));
    }

    @Test
    void identityAndMergeActivationRejectNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("clusterId must not be null", () -> new NodeIdentity(null, NodeId.of("n1")));
        assertIllegalArgument("nodeId must not be null", () -> new NodeIdentity(clusterId(1), null));

        assertIllegalArgument("name must not be null", () -> new MergeOperatorActivation(null, 1, buf(1)));
        assertIllegalArgument("artifactHash must not be null",
                () -> new MergeOperatorActivation("counter", 1, null));
    }

    @Test
    void backupManifestRejectsNullInputsWithExplicitDiagnostics() {
        var cut = backupCut(new ShardId(1));

        assertIllegalArgument("backupId must not be null", () -> new BackupManifest(null, 12, 11, List.of(cut)));
        assertIllegalArgument("partitionCuts must not be null", () -> new BackupManifest("backup-12", 12, 11, null));
        assertIllegalArgument("partitionCuts entry must not be null",
                () -> new BackupManifest("backup-12", 12, 11, Collections.singletonList(null)));
        assertIllegalArgument("partitionId must not be null", () -> backupCut(null));
    }

    @Test
    void cdcTokenTranslationsRejectNullInputsWithExplicitDiagnostics() {
        var source = token(11, new ShardId(1), 3);
        var left = token(12, new ShardId(2), 4);
        var right = token(12, new ShardId(3), 4);
        var merged = token(13, new ShardId(4), 5);

        assertIllegalArgument("sourceEndToken must not be null",
                () -> new CdcTokenTranslation.Split(12, null, left, right));
        assertIllegalArgument("leftStartToken must not be null",
                () -> new CdcTokenTranslation.Split(12, source, null, right));
        assertIllegalArgument("rightStartToken must not be null",
                () -> new CdcTokenTranslation.Split(12, source, left, null));

        assertIllegalArgument("leftEndToken must not be null",
                () -> new CdcTokenTranslation.Merge(13, null, right, merged));
        assertIllegalArgument("rightEndToken must not be null",
                () -> new CdcTokenTranslation.Merge(13, left, null, merged));
        assertIllegalArgument("mergedStartToken must not be null",
                () -> new CdcTokenTranslation.Merge(13, left, right, null));
    }

    private static BootstrapManifest manifest(ClusterId clusterId, BootstrapPeer peer) {
        return new BootstrapManifest(clusterId, "metadata-group", List.of(peer), 0);
    }

    private static BootstrapPeer peer(String nodeId) {
        return new BootstrapPeer(NodeId.of(nodeId), publicEndpoint(10001), ratisEndpoint(11001), "rack-a");
    }

    private static URI publicEndpoint(int port) {
        return URI.create("http://127.0.0.1:" + port);
    }

    private static URI ratisEndpoint(int port) {
        return URI.create("grpc://127.0.0.1:" + port);
    }

    private static ClusterId clusterId(long lowBits) {
        return new ClusterId(new UUID(0, lowBits));
    }

    private static BackupManifest.PartitionCut backupCut(ShardId partitionId) {
        return new BackupManifest.PartitionCut(partitionId, 3, 5, 100, 12);
    }

    private static ReplicatedCdcToken token(long metadataEpoch, ShardId partitionId, long partitionEpoch) {
        return new ReplicatedCdcToken(metadataEpoch, partitionId, partitionEpoch, 10, 0);
    }

    private static Buf buf(int value) {
        return Buf.wrap((byte) value);
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }
}
