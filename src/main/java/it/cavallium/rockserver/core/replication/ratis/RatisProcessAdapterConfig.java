package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ClusterNodeMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapManifest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

import static it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterProperties.sha256;
import static it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterProperties.updateDigest;

public record RatisProcessAdapterConfig(long metadataEpoch, List<RatisPartitionDescriptor> partitions) {

    private static final String METADATA_EPOCH_KEY = "metadataEpoch";
    private static final String METADATA_SHA256_KEY = "metadataSha256";
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private static final String PARTITION_PREFIX = "partition.";
    public static final String METADATA_GROUP_PARTITION_ID = "__metadata_group__";
    public static final int MAX_ADAPTER_CONFIG_BYTES = 64 * 1024 * 1024;
    private static final String ADAPTER_CONFIG_PAYLOAD_DESCRIPTION = "Ratis process adapter config payload";
    private static final String METADATA_GROUP_TOPOLOGY_FINGERPRINT_VERSION =
            "rockserver-ratis-metadata-group-topology-v1";

    public RatisProcessAdapterConfig {
        if (metadataEpoch < 0) {
            throw new IllegalArgumentException("metadataEpoch must not be negative");
        }
        if (partitions == null) {
            throw new IllegalArgumentException("partitions must not be null");
        }
        var partitionCopy = new ArrayList<RatisPartitionDescriptor>(partitions.size());
        for (var partition : partitions) {
            if (partition == null) {
                throw new IllegalArgumentException("partitions entry must not be null");
            }
            partitionCopy.add(partition);
        }
        partitions = List.copyOf(partitionCopy);
        var partitionIds = new HashSet<String>();
        var raftGroupIds = new HashSet<String>();
        var dataPaths = new HashSet<Path>();
        for (var partition : partitions) {
            if (!partitionIds.add(partition.partitionId())) {
                throw new IllegalArgumentException("duplicate adapter partition id: " + partition.partitionId());
            }
            if (!raftGroupIds.add(partition.raftGroupId())) {
                throw new IllegalArgumentException("duplicate adapter Ratis group id: " + partition.raftGroupId());
            }
            var dataPath = partition.dataPath().toAbsolutePath().normalize();
            if (!dataPaths.add(dataPath)) {
                throw new IllegalArgumentException("duplicate adapter partition data path: " + dataPath);
            }
        }
    }

    public RatisProcessAdapterConfig(List<RatisPartitionDescriptor> partitions) {
        this(0, partitions);
    }

    public static RatisProcessAdapterConfig fromMetadata(ClusterMetadata metadata,
                                                         NodeId localNodeId,
                                                         Path adapterDataRoot) {
        if (metadata == null) {
            throw new IllegalArgumentException("metadata must not be null");
        }
        if (localNodeId == null) {
            throw new IllegalArgumentException("localNodeId must not be null");
        }
        if (adapterDataRoot == null) {
            throw new IllegalArgumentException("adapterDataRoot must not be null");
        }
        metadata.requireNode(localNodeId);

        var partitions = metadata.partitionDescriptors().stream()
                .sorted(Comparator.comparing(PartitionDescriptor::partitionId))
                .map(partition -> localPartition(metadata, partition, localNodeId, adapterDataRoot))
                .filter(Objects::nonNull)
                .toList();
        return new RatisProcessAdapterConfig(metadata.epoch(), partitions);
    }

    public static RatisProcessAdapterConfig fromMetadata(ClusterMetadata metadata,
                                                         BootstrapManifest bootstrapManifest,
                                                         NodeId localNodeId,
                                                         Path adapterDataRoot) {
        if (metadata == null) {
            throw new IllegalArgumentException("metadata must not be null");
        }
        if (bootstrapManifest == null) {
            throw new IllegalArgumentException("bootstrapManifest must not be null");
        }
        if (localNodeId == null) {
            throw new IllegalArgumentException("localNodeId must not be null");
        }
        if (adapterDataRoot == null) {
            throw new IllegalArgumentException("adapterDataRoot must not be null");
        }
        var partitions = new ArrayList<RatisPartitionDescriptor>();
        partitions.add(metadataGroupPartition(bootstrapManifest, localNodeId, adapterDataRoot));
        var dataConfig = fromMetadata(metadata, localNodeId, adapterDataRoot);
        partitions.addAll(dataConfig.partitions());
        return new RatisProcessAdapterConfig(dataConfig.metadataEpoch(), partitions);
    }

    public Optional<RatisPartitionDescriptor> metadataGroupPartition() {
        return partitions.stream()
                .filter(partition -> isMetadataGroupPartitionId(partition.partitionId()))
                .findFirst();
    }

    public List<RatisPartitionDescriptor> dataPartitions() {
        return partitions.stream()
                .filter(partition -> !isMetadataGroupPartitionId(partition.partitionId()))
                .toList();
    }

    public static boolean isMetadataGroupPartitionId(String partitionId) {
        return METADATA_GROUP_PARTITION_ID.equals(partitionId);
    }

    public static Path canonicalDataPath(Path adapterDataRoot, String partitionId) {
        if (adapterDataRoot == null) {
            throw new IllegalArgumentException("adapterDataRoot must not be null");
        }
        if (partitionId == null) {
            throw new IllegalArgumentException("partitionId must not be null");
        }
        var requiredPartitionId = RatisAdapterBoundary.requireSingleLineNonBlank(partitionId, "partitionId");
        var partitionDirectory = isMetadataGroupPartitionId(requiredPartitionId)
                ? "metadata-group"
                : "partition-" + requirePathSegment(requiredPartitionId);
        return adapterDataRoot.resolve(partitionDirectory).normalize();
    }

    public String metadataGroupTopologySha256() {
        return metadataGroupTopologySha256(partitions);
    }

    public static String metadataGroupTopologySha256(List<RatisPartitionDescriptor> partitions) {
        if (partitions == null) {
            throw new IllegalArgumentException("partitions must not be null");
        }
        var digest = sha256();
        updateDigest(digest, METADATA_GROUP_TOPOLOGY_FINGERPRINT_VERSION);
        RatisPartitionDescriptor metadataGroup = null;
        for (var partition : partitions) {
            if (partition == null) {
                throw new IllegalArgumentException("partitions entry must not be null");
            }
            if (isMetadataGroupPartitionId(partition.partitionId())) {
                if (metadataGroup != null) {
                    throw new IllegalArgumentException("multiple metadata-group partition descriptors");
                }
                metadataGroup = partition;
            }
        }
        if (metadataGroup == null) {
            updateDigest(digest, "absent");
        } else {
            updateDigest(digest, "present");
            updateDigest(digest, metadataGroup.raftGroupId());
            updateDigest(digest, Integer.toString(metadataGroup.voters().size()));
            for (var voter : metadataGroup.voters()) {
                updateDigest(digest, voter.peerId());
                updateDigest(digest, voter.endpoint());
                updateDigest(digest, voter.failureDomain());
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    public Properties toProperties(String metadataSha256) {
        var properties = new Properties();
        properties.setProperty(METADATA_EPOCH_KEY, Long.toString(metadataEpoch));
        properties.setProperty(METADATA_SHA256_KEY, requireSha256(metadataSha256));
        for (int partitionIndex = 0; partitionIndex < partitions.size(); partitionIndex++) {
            writePartition(properties, partitionIndex, partitions.get(partitionIndex));
        }
        return properties;
    }

    public void write(Path configPath, String metadataSha256) throws IOException {
        RatisProcessAdapterFileWriter.writeProperties(
                configPath,
                toProperties(metadataSha256),
                "Rockserver Ratis process adapter config",
                MAX_ADAPTER_CONFIG_BYTES,
                ADAPTER_CONFIG_PAYLOAD_DESCRIPTION
        );
    }

    public static String readMetadataSha256(Path configPath) throws IOException {
        var properties = readProperties(configPath);
        var metadataSha256 = properties.getProperty(METADATA_SHA256_KEY);
        if (metadataSha256 == null) {
            throw new IllegalArgumentException("missing required adapter config key: " + METADATA_SHA256_KEY);
        }
        return requireSha256(metadataSha256);
    }

    static Properties readProperties(Path configPath) throws IOException {
        return RatisProcessAdapterProperties.read(
                configPath,
                MAX_ADAPTER_CONFIG_BYTES,
                ADAPTER_CONFIG_PAYLOAD_DESCRIPTION
        );
    }

    private static RatisPartitionDescriptor localPartition(ClusterMetadata metadata,
                                                           PartitionDescriptor partition,
                                                           NodeId localNodeId,
                                                           Path adapterDataRoot) {
        var localPeer = partition.peers().stream()
                .filter(peer -> peer.nodeId().equals(localNodeId))
                .findFirst()
                .orElse(null);
        if (localPeer == null) {
            return null;
        }
        if (!localPeer.isVoter()) {
            throw new IllegalArgumentException("local learner partition is not supported by the process adapter: "
                    + partition.partitionId());
        }

        var voters = new ArrayList<RatisPeerDescriptor>();
        var voterIds = new HashSet<NodeId>();
        for (var peer : partition.peers()) {
            if (!peer.isVoter()) {
                continue;
            }
            if (!voterIds.add(peer.nodeId())) {
                throw new IllegalArgumentException("duplicate voter in partition " + partition.partitionId()
                        + ": " + peer.nodeId());
            }
            var node = metadata.requireNode(peer.nodeId());
            voters.add(new RatisPeerDescriptor(
                    peer.nodeId().value(),
                    ratisEndpoint(node),
                    node.failureDomain()
            ));
        }

        return new RatisPartitionDescriptor(
                partition.partitionId().toString(),
                partition.ratisGroupId(),
                localNodeId.value(),
                voters,
                canonicalDataPath(adapterDataRoot, partition.partitionId().toString())
        );
    }

    private static RatisPartitionDescriptor metadataGroupPartition(BootstrapManifest bootstrapManifest,
                                                                   NodeId localNodeId,
                                                                   Path adapterDataRoot) {
        bootstrapManifest.findPeer(localNodeId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "local node must be present in bootstrap metadata group: " + localNodeId
                ));
        var voters = bootstrapManifest.initialPeers().stream()
                .map(peer -> new RatisPeerDescriptor(
                        peer.nodeId().value(),
                        ratisEndpoint(peer.ratisEndpoint()),
                        peer.failureDomain()
                ))
                .toList();
        return new RatisPartitionDescriptor(
                METADATA_GROUP_PARTITION_ID,
                bootstrapManifest.metadataGroupId(),
                localNodeId.value(),
                voters,
                canonicalDataPath(adapterDataRoot, METADATA_GROUP_PARTITION_ID)
        );
    }

    private static String ratisEndpoint(java.net.URI endpoint) {
        if (endpoint.getHost() == null || endpoint.getPort() < 0) {
            throw new IllegalArgumentException("bootstrap Ratis endpoint must include host and port: "
                    + endpoint);
        }
        return endpoint.toString();
    }

    private static String ratisEndpoint(ClusterNodeMetadata node) {
        var endpoint = node.ratisEndpoint();
        if (endpoint.getHost() == null || endpoint.getPort() < 0) {
            throw new IllegalArgumentException("node Ratis endpoint must include host and port: "
                    + node.nodeId());
        }
        return endpoint.toString();
    }

    private static void writePartition(Properties properties,
                                       int partitionIndex,
                                       RatisPartitionDescriptor partition) {
        var prefix = PARTITION_PREFIX + partitionIndex + ".";
        properties.setProperty(prefix + "partitionId", partition.partitionId());
        properties.setProperty(prefix + "raftGroupId", partition.raftGroupId());
        properties.setProperty(prefix + "localPeerId", partition.localPeerId());
        properties.setProperty(prefix + "dataPath", partition.dataPath().toString());
        for (int peerIndex = 0; peerIndex < partition.voters().size(); peerIndex++) {
            var peer = partition.voters().get(peerIndex);
            var peerPrefix = prefix + "peer." + peerIndex + ".";
            properties.setProperty(peerPrefix + "peerId", peer.peerId());
            properties.setProperty(peerPrefix + "endpoint", peer.endpoint());
            properties.setProperty(peerPrefix + "failureDomain", peer.failureDomain());
        }
    }

    private static String requireSha256(String value) {
        if (value == null) {
            throw new IllegalArgumentException(METADATA_SHA256_KEY + " must not be null");
        }
        if (!SHA256.matcher(value).matches()) {
            throw new IllegalArgumentException(METADATA_SHA256_KEY + " must be a lowercase SHA-256: " + value);
        }
        return value;
    }

    private static String requirePathSegment(String value) {
        if (value.equals(".") || value.equals("..") || value.indexOf('/') >= 0 || value.indexOf('\\') >= 0) {
            throw new IllegalArgumentException("partitionId must be a single path segment: " + value);
        }
        return value;
    }
}
