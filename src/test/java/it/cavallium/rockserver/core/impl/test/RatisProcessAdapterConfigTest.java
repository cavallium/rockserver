package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ClusterNodeMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapManifest;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapPeer;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisPeerDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterConfig;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RatisProcessAdapterConfigTest {

    @Test
    void metadataConfigIncludesLocalVotingPartitionsAndUsesNodeRatisEndpoints(@TempDir Path tempDir) {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var n3 = NodeId.of("n3");
        var localPartitionGroupId = UUID.randomUUID().toString();
        var remotePartitionGroupId = UUID.randomUUID().toString();
        var localPartition = partition(new ShardId(7), localPartitionGroupId, null, buf(0x80), List.of(
                member(n2, ReplicaRole.VOTER, "grpc://10.0.0.2:19002"),
                member(n3, ReplicaRole.LEARNER, "grpc://10.0.0.3:19003"),
                member(n1, ReplicaRole.VOTER, "grpc://10.0.0.1:19001")
        ));
        var remotePartition = partition(new ShardId(8), remotePartitionGroupId, buf(0x80), null, List.of(
                member(n2, ReplicaRole.VOTER, "grpc://10.0.0.2:19002")
        ));
        var adapterDataRoot = tempDir.resolve("ratis-adapter");
        var metadata = metadata(List.of(localPartition, remotePartition), List.of(
                node(n1, "grpc://10.0.0.1:19001", "rack-a"),
                node(n2, "grpc://10.0.0.2:19002", "rack-b"),
                node(n3, "grpc://10.0.0.3:19003", "rack-c")
        ));

        var config = RatisProcessAdapterConfig.fromMetadata(metadata, n1, adapterDataRoot);

        assertEquals(1, config.partitions().size());
        var partition = config.partitions().getFirst();
        assertEquals("7", partition.partitionId());
        assertEquals(localPartitionGroupId, partition.raftGroupId());
        assertEquals("n1", partition.localPeerId());
        assertEquals(adapterDataRoot.resolve("partition-7"), partition.dataPath());
        assertEquals(List.of(
                new RatisPeerDescriptor("n2", "grpc://10.0.0.2:19002", "rack-b"),
                new RatisPeerDescriptor("n1", "grpc://10.0.0.1:19001", "rack-a")
        ), partition.voters());

        var properties = config.toProperties("0".repeat(64));
        assertEquals("1", properties.getProperty("metadataEpoch"));
        assertEquals("0".repeat(64), properties.getProperty("metadataSha256"));
        assertEquals("7", properties.getProperty("partition.0.partitionId"));
        assertEquals(localPartitionGroupId, properties.getProperty("partition.0.raftGroupId"));
        assertEquals("n1", properties.getProperty("partition.0.localPeerId"));
        assertEquals(adapterDataRoot.resolve("partition-7").toString(),
                properties.getProperty("partition.0.dataPath"));
        assertEquals("n2", properties.getProperty("partition.0.peer.0.peerId"));
        assertEquals("grpc://10.0.0.2:19002", properties.getProperty("partition.0.peer.0.endpoint"));
        assertEquals("rack-b", properties.getProperty("partition.0.peer.0.failureDomain"));
        assertEquals("n1", properties.getProperty("partition.0.peer.1.peerId"));
        assertNull(properties.getProperty("partition.0.peer.2.peerId"));
        assertNull(properties.getProperty("partition.1.partitionId"));
    }

    @Test
    void metadataConfigRejectsLocalLearnerPartition(@TempDir Path tempDir) {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var metadata = metadata(List.of(partition(new ShardId(7), UUID.randomUUID().toString(), List.of(
                member(n1, ReplicaRole.LEARNER, "grpc://10.0.0.1:19001"),
                member(n2, ReplicaRole.VOTER, "grpc://10.0.0.2:19002")
        ))), List.of(
                node(n1, "grpc://10.0.0.1:19001", "rack-a"),
                node(n2, "grpc://10.0.0.2:19002", "rack-b")
        ));

        var error = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterConfig.fromMetadata(metadata, n1, tempDir));
        assertEquals("local learner partition is not supported by the process adapter: 7", error.getMessage());
    }

    @Test
    void clusterMetadataRejectsMissingPeerNodeMetadata() {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");

        assertThrows(IllegalArgumentException.class, () -> metadata(
                List.of(partition(new ShardId(7), UUID.randomUUID().toString(), List.of(
                        member(n1, ReplicaRole.VOTER, "grpc://10.0.0.1:19001"),
                        member(n2, ReplicaRole.VOTER, "grpc://10.0.0.2:19002")
                ))),
                List.of(node(n1, "grpc://10.0.0.1:19001", "rack-a"))
        ));
    }

    @Test
    void metadataConfigRejectsRatisEndpointWithoutHostAndPort() {
        var n1 = NodeId.of("n1");

        assertThrows(IllegalArgumentException.class,
                () -> node(n1, "ratis-endpoint", "rack-a"));
    }

    @Test
    void metadataConfigRejectsNonGrpcRatisEndpoint() {
        var n1 = NodeId.of("n1");

        assertThrows(IllegalArgumentException.class,
                () -> node(n1, "http://10.0.0.1:19001", "rack-a"));
    }

    @Test
    void adapterConfigRejectsDuplicateGroupIdsAndDataPaths(@TempDir Path tempDir) {
        var voters = List.of(new RatisPeerDescriptor("n1", "grpc://10.0.0.1:19001", "rack-a"));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterConfig(List.of(
                new RatisPartitionDescriptor(
                        "7",
                        "shared-group",
                        "n1",
                        voters,
                        tempDir.resolve("partition-7")
                ),
                new RatisPartitionDescriptor(
                        "8",
                        "shared-group",
                        "n1",
                        voters,
                        tempDir.resolve("partition-8")
                )
        )));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterConfig(List.of(
                new RatisPartitionDescriptor(
                        "7",
                        "group-7",
                        "n1",
                        voters,
                        tempDir.resolve("shared-partition")
                ),
                new RatisPartitionDescriptor(
                        "8",
                        "group-8",
                        "n1",
                        voters,
                        tempDir.resolve(".").resolve("shared-partition")
                )
        )));
    }

    @Test
    void adapterConfigRejectsNullInputsWithExplicitDiagnostics(@TempDir Path tempDir) {
        var n1 = NodeId.of("n1");
        var voters = List.of(new RatisPeerDescriptor("n1", "grpc://10.0.0.1:19001", "rack-a"));
        var metadata = metadata(List.of(), List.of(node(n1, "grpc://10.0.0.1:19001", "rack-a")));
        var partitions = new java.util.ArrayList<RatisPartitionDescriptor>();
        partitions.add(null);

        var nullPartitions = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterConfig(null));
        var nullPartitionEntry = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterConfig(partitions));
        var nullMetadata = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterConfig.fromMetadata(null, n1, tempDir));
        var nullLocalNode = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterConfig.fromMetadata(metadata, null, tempDir));
        var nullDataRoot = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterConfig.fromMetadata(metadata, n1, null));
        var nullBootstrap = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterConfig.fromMetadata(metadata, null, n1, tempDir));
        var nullCanonicalRoot = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterConfig.canonicalDataPath(null, "partition-a"));
        var nullCanonicalPartition = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterConfig.canonicalDataPath(tempDir, null));
        var nullTopologyPartitions = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterConfig.metadataGroupTopologySha256(null));
        var nullTopologyEntry = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterConfig.metadataGroupTopologySha256(partitions));
        var nullSha256 = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterConfig(List.of(new RatisPartitionDescriptor(
                        "7",
                        "group-7",
                        "n1",
                        voters,
                        tempDir.resolve("partition-7")
                ))).toProperties(null));

        assertEquals("partitions must not be null", nullPartitions.getMessage());
        assertEquals("partitions entry must not be null", nullPartitionEntry.getMessage());
        assertEquals("metadata must not be null", nullMetadata.getMessage());
        assertEquals("localNodeId must not be null", nullLocalNode.getMessage());
        assertEquals("adapterDataRoot must not be null", nullDataRoot.getMessage());
        assertEquals("bootstrapManifest must not be null", nullBootstrap.getMessage());
        assertEquals("adapterDataRoot must not be null", nullCanonicalRoot.getMessage());
        assertEquals("partitionId must not be null", nullCanonicalPartition.getMessage());
        assertEquals("partitions must not be null", nullTopologyPartitions.getMessage());
        assertEquals("partitions entry must not be null", nullTopologyEntry.getMessage());
        assertEquals("metadataSha256 must not be null", nullSha256.getMessage());
    }

    @Test
    void peerDescriptorRejectsNonGrpcEndpoint() {
        assertThrows(IllegalArgumentException.class,
                () -> new RatisPeerDescriptor("n1", "http://10.0.0.1:19001", "rack-a"));
    }

    @Test
    void peerDescriptorRejectsDecoratedEndpoint() {
        assertThrows(IllegalArgumentException.class,
                () -> new RatisPeerDescriptor("n1", "grpc://10.0.0.1:19001/ratis", "rack-a"));
        assertThrows(IllegalArgumentException.class,
                () -> new RatisPeerDescriptor("n1", "grpc://node:19001?transport=grpc", "rack-a"));
        assertThrows(IllegalArgumentException.class,
                () -> new RatisPeerDescriptor("n1", "grpc://node:19001#listener", "rack-a"));
        assertThrows(IllegalArgumentException.class,
                () -> new RatisPeerDescriptor("n1", "grpc://user@node:19001", "rack-a"));
    }

    @Test
    void peerDescriptorCanonicalizesEndpointAliases() {
        var peer = new RatisPeerDescriptor("n1", "GRPC://Node.Example:19001", "rack-a");
        assertEquals("grpc://node.example:19001", peer.endpoint());

        assertThrows(IllegalArgumentException.class, () -> new RatisPartitionDescriptor(
                "7",
                "group-7",
                "n1",
                List.of(
                        new RatisPeerDescriptor("n1", "grpc://node.example:19001", "rack-a"),
                        new RatisPeerDescriptor("n2", "GRPC://NODE.EXAMPLE:19001", "rack-b")
                ),
                Path.of("partition-7")
        ));
    }

    @Test
    void metadataConfigWritesLoadableProperties(@TempDir Path tempDir) throws Exception {
        var n1 = NodeId.of("n1");
        var groupId = UUID.randomUUID().toString();
        var config = RatisProcessAdapterConfig.fromMetadata(
                metadata(List.of(partition(new ShardId(7), groupId, List.of(
                        member(n1, ReplicaRole.VOTER, "grpc://10.0.0.1:19001")
                ))), List.of(node(n1, "grpc://10.0.0.1:19001", "rack-a"))),
                n1,
                tempDir.resolve("adapter-data")
        );
        var configPath = tempDir.resolve("conf").resolve("ratis-adapter.properties");

        config.write(configPath, "0".repeat(64));

        var loaded = new Properties();
        try (var reader = Files.newBufferedReader(configPath)) {
            loaded.load(reader);
        }
        assertEquals("7", loaded.getProperty("partition.0.partitionId"));
        assertEquals("1", loaded.getProperty("metadataEpoch"));
        assertEquals("0".repeat(64), loaded.getProperty("metadataSha256"));
        assertEquals(groupId, loaded.getProperty("partition.0.raftGroupId"));
        assertEquals(tempDir.resolve("adapter-data").resolve("partition-7").toString(),
                loaded.getProperty("partition.0.dataPath"));
    }

    @Test
    void metadataConfigCanIncludeBootstrapMetadataGroup(@TempDir Path tempDir) {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var bootstrapManifest = new BootstrapManifest(
                new ClusterId(new UUID(0, 1)),
                "metadata-group",
                List.of(
                        peer(n1, "grpc://10.0.0.1:19001", "rack-a"),
                        peer(n2, "grpc://10.0.0.2:19002", "rack-b")
                ),
                0
        );

        var config = RatisProcessAdapterConfig.fromMetadata(
                metadata(List.of(), List.of(
                        node(n1, "grpc://10.0.0.1:19001", "rack-a"),
                        node(n2, "grpc://10.0.0.2:19002", "rack-b")
                )),
                bootstrapManifest,
                n1,
                tempDir.resolve("adapter-data")
        );

        var metadataGroup = config.metadataGroupPartition().orElseThrow();
        assertEquals(RatisProcessAdapterConfig.METADATA_GROUP_PARTITION_ID, metadataGroup.partitionId());
        assertEquals("metadata-group", metadataGroup.raftGroupId());
        assertEquals("n1", metadataGroup.localPeerId());
        assertEquals(tempDir.resolve("adapter-data").resolve("metadata-group"), metadataGroup.dataPath());
        assertEquals(List.of(
                new RatisPeerDescriptor("n1", "grpc://10.0.0.1:19001", "rack-a"),
                new RatisPeerDescriptor("n2", "grpc://10.0.0.2:19002", "rack-b")
        ), metadataGroup.voters());
        assertEquals(List.of(), config.dataPartitions());
    }

    @Test
    void metadataGroupTopologyFingerprintTracksOnlyImmutableBootstrapTopology(@TempDir Path tempDir) {
        var voters = List.of(
                new RatisPeerDescriptor("n1", "grpc://10.0.0.1:19001", "rack-a"),
                new RatisPeerDescriptor("n2", "grpc://10.0.0.2:19002", "rack-b")
        );
        var metadataGroup = new RatisPartitionDescriptor(
                RatisProcessAdapterConfig.METADATA_GROUP_PARTITION_ID,
                "metadata-group",
                "n1",
                voters,
                tempDir.resolve("metadata-group")
        );
        var fingerprint = new RatisProcessAdapterConfig(List.of(metadataGroup)).metadataGroupTopologySha256();

        assertEquals(64, fingerprint.length());
        assertEquals(fingerprint, new RatisProcessAdapterConfig(List.of(new RatisPartitionDescriptor(
                RatisProcessAdapterConfig.METADATA_GROUP_PARTITION_ID,
                "metadata-group",
                "n2",
                voters,
                tempDir.resolve("other-metadata-group-path")
        ))).metadataGroupTopologySha256());
        assertNotEquals(fingerprint, new RatisProcessAdapterConfig(List.of()).metadataGroupTopologySha256());
        assertNotEquals(fingerprint, new RatisProcessAdapterConfig(List.of(new RatisPartitionDescriptor(
                RatisProcessAdapterConfig.METADATA_GROUP_PARTITION_ID,
                "other-metadata-group",
                "n1",
                voters,
                tempDir.resolve("metadata-group")
        ))).metadataGroupTopologySha256());
        assertNotEquals(fingerprint, new RatisProcessAdapterConfig(List.of(new RatisPartitionDescriptor(
                RatisProcessAdapterConfig.METADATA_GROUP_PARTITION_ID,
                "metadata-group",
                "n1",
                List.of(
                        voters.getFirst(),
                        new RatisPeerDescriptor("n2", "grpc://10.0.0.2:19002", "other-rack")
                ),
                tempDir.resolve("metadata-group")
        ))).metadataGroupTopologySha256());
    }

    @Test
    void canonicalDataPathRejectsPartitionPathSegments(@TempDir Path tempDir) {
        assertEquals(tempDir.resolve("partition-partition-a"),
                RatisProcessAdapterConfig.canonicalDataPath(tempDir, "partition-a"));
        assertEquals(tempDir.resolve("metadata-group"),
                RatisProcessAdapterConfig.canonicalDataPath(
                        tempDir,
                        RatisProcessAdapterConfig.METADATA_GROUP_PARTITION_ID
                ));
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterConfig.canonicalDataPath(tempDir, "partition-a/../../escape"));
        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterConfig.canonicalDataPath(tempDir, "partition-a\\..\\..\\escape"));
    }

    private static ClusterMetadata metadata(List<PartitionDescriptor> partitions, List<ClusterNodeMetadata> nodes) {
        return new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                1,
                Map.of(),
                Map.of(),
                partitions.stream().collect(java.util.stream.Collectors.toMap(
                        PartitionDescriptor::partitionId,
                        partition -> partition
                )),
                nodes.stream().collect(java.util.stream.Collectors.toMap(
                        ClusterNodeMetadata::nodeId,
                        node -> node
                ))
        );
    }

    private static PartitionDescriptor partition(ShardId partitionId,
                                                 String ratisGroupId,
                                                 List<ReplicaMember> peers) {
        return partition(partitionId, ratisGroupId, null, null, peers);
    }

    private static PartitionDescriptor partition(ShardId partitionId,
                                                 String ratisGroupId,
                                                 Buf startKeyInclusive,
                                                 Buf endKeyExclusive,
                                                 List<ReplicaMember> peers) {
        var leader = peers.stream()
                .filter(ReplicaMember::isVoter)
                .findFirst()
                .map(ReplicaMember::nodeId)
                .orElse(null);
        return new PartitionDescriptor(
                partitionId,
                startKeyInclusive,
                endKeyExclusive,
                1,
                ratisGroupId,
                peers,
                leader,
                Map.of(),
                PartitionSplitState.ACTIVE
        );
    }

    private static ReplicaMember member(NodeId nodeId, ReplicaRole role, String ratisEndpoint) {
        return new ReplicaMember(nodeId, URI.create(ratisEndpoint), role, 0, 0, true);
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
        for (int index = 0; index < values.length; index++) {
            bytes[index] = (byte) values[index];
        }
        return Buf.wrap(bytes);
    }

    private static BootstrapPeer peer(NodeId nodeId, String ratisEndpoint, String failureDomain) {
        return new BootstrapPeer(
                nodeId,
                URI.create("http://" + nodeId + ".public:10000"),
                URI.create(ratisEndpoint),
                failureDomain
        );
    }
}
