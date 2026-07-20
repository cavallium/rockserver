package it.cavallium.rockserver.ratis;

import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisPeerDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterConfig;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterPaths;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

public record RatisClasspathAdapterConfig(long metadataEpoch,
                                          String metadataSha256,
                                          List<RatisPartitionDescriptor> partitions) {

    private static final String METADATA_EPOCH_KEY = "metadataEpoch";
    private static final String METADATA_SHA256_KEY = "metadataSha256";
    private static final Pattern KNOWN_KEY = Pattern.compile(
            "metadataEpoch|metadataSha256|partition\\.\\d+\\.(partitionId|raftGroupId|localPeerId|dataPath|peer\\.\\d+\\.(peerId|endpoint|failureDomain))"
    );
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    public static final int MAX_CLASSPATH_ADAPTER_CONFIG_BYTES = 64 * 1024 * 1024;

    public RatisClasspathAdapterConfig {
        if (metadataEpoch < 0) {
            throw new IllegalArgumentException("metadataEpoch must not be negative");
        }
        metadataSha256 = requireSha256(metadataSha256, METADATA_SHA256_KEY);
        partitions = List.copyOf(Objects.requireNonNull(partitions, "partitions"));
        var partitionIds = new HashSet<String>();
        var raftGroupIds = new HashSet<String>();
        var dataPaths = new HashSet<Path>();
        for (var partition : partitions) {
            if (!partitionIds.add(partition.partitionId())) {
                throw new IllegalArgumentException("duplicate Ratis classpath adapter partition id: "
                        + partition.partitionId());
            }
            if (!raftGroupIds.add(partition.raftGroupId())) {
                throw new IllegalArgumentException("duplicate Ratis classpath adapter group id: "
                        + partition.raftGroupId());
            }
            var dataPath = partition.dataPath().toAbsolutePath().normalize();
            if (!dataPaths.add(dataPath)) {
                throw new IllegalArgumentException("duplicate Ratis classpath adapter data path: " + dataPath);
            }
        }
    }

    public static RatisClasspathAdapterConfig read(Path path) throws IOException {
        Objects.requireNonNull(path, "path");
        return fromProperties(RatisProcessAdapterProperties.read(
                path,
                MAX_CLASSPATH_ADAPTER_CONFIG_BYTES,
                "Ratis classpath adapter config"
        ));
    }

    public static RatisClasspathAdapterConfig fromProperties(Properties properties) {
        Objects.requireNonNull(properties, "properties");
        rejectUnknownKeys(properties);
        var metadataEpoch = requiredLong(properties, METADATA_EPOCH_KEY);
        var partitions = new ArrayList<RatisPartitionDescriptor>();
        for (int partitionIndex = 0; hasPartition(properties, partitionIndex); partitionIndex++) {
            partitions.add(partition(properties, partitionIndex));
        }
        rejectPartitionGaps(properties, partitions.size());
        return new RatisClasspathAdapterConfig(metadataEpoch, required(properties, METADATA_SHA256_KEY), partitions);
    }

    public void requireMetadataEpoch(long expectedMetadataEpoch) {
        if (metadataEpoch != expectedMetadataEpoch) {
            throw new IllegalArgumentException("Ratis classpath adapter config metadata epoch " + metadataEpoch
                    + " does not match metadata epoch " + expectedMetadataEpoch);
        }
    }

    public void requireMetadataSha256(String expectedMetadataSha256) {
        expectedMetadataSha256 = requireSha256(expectedMetadataSha256, "expectedMetadataSha256");
        if (!metadataSha256.equals(expectedMetadataSha256)) {
            throw new IllegalArgumentException("Ratis classpath adapter config metadata SHA-256 " + metadataSha256
                    + " does not match metadata SHA-256 " + expectedMetadataSha256);
        }
    }

    public void requireLocalPeerId(String expectedLocalPeerId) {
        var localPeerId = NodeId.of(expectedLocalPeerId).value();
        for (var partition : partitions) {
            if (!localPeerId.equals(partition.localPeerId())) {
                throw new IllegalArgumentException("Ratis classpath adapter partition " + partition.partitionId()
                        + " local peer id " + partition.localPeerId() + " does not match process node id "
                        + localPeerId);
            }
        }
    }

    public void requireDataRoot(Path expectedDataRoot) {
        var dataRoot = Objects.requireNonNull(expectedDataRoot, "expectedDataRoot").toAbsolutePath().normalize();
        for (var partition : partitions) {
            var expectedPath = RatisProcessAdapterConfig.canonicalDataPath(dataRoot, partition.partitionId());
            var actualPath = partition.dataPath().toAbsolutePath().normalize();
            if (!expectedPath.equals(actualPath)) {
                throw new IllegalArgumentException("Ratis classpath adapter partition " + partition.partitionId()
                        + " data path " + actualPath + " does not match expected path " + expectedPath);
            }
            try {
                if (!RatisProcessAdapterPaths.resolvesInside(dataRoot, actualPath)) {
                    throw new IllegalArgumentException("Ratis classpath adapter partition " + partition.partitionId()
                            + " data path " + actualPath + " must resolve inside data root " + dataRoot);
                }
            } catch (IOException e) {
                throw new IllegalArgumentException("Ratis classpath adapter partition " + partition.partitionId()
                        + " data path " + actualPath + " could not be resolved safely inside data root " + dataRoot, e);
            }
        }
    }

    public String metadataGroupTopologySha256() {
        return RatisProcessAdapterConfig.metadataGroupTopologySha256(partitions);
    }

    public void requireMetadataGroupTopologySha256(String expectedSha256) {
        Objects.requireNonNull(expectedSha256, "expectedSha256");
        if (!SHA256.matcher(expectedSha256).matches()) {
            throw new IllegalArgumentException("Invalid expected metadata-group topology SHA-256: " + expectedSha256);
        }
        var actualSha256 = metadataGroupTopologySha256();
        if (!expectedSha256.equals(actualSha256)) {
            throw new IllegalArgumentException("Ratis classpath adapter metadata-group topology SHA-256 "
                    + actualSha256 + " does not match expected SHA-256 " + expectedSha256);
        }
    }

    private static RatisPartitionDescriptor partition(Properties properties, int partitionIndex) {
        var prefix = "partition." + partitionIndex + ".";
        var peers = new ArrayList<RatisPeerDescriptor>();
        for (int peerIndex = 0; hasPeer(properties, partitionIndex, peerIndex); peerIndex++) {
            var peerPrefix = prefix + "peer." + peerIndex + ".";
            peers.add(new RatisPeerDescriptor(
                    required(properties, peerPrefix + "peerId"),
                    required(properties, peerPrefix + "endpoint"),
                    required(properties, peerPrefix + "failureDomain")
            ));
        }
        rejectPeerGaps(properties, partitionIndex, peers.size());
        return new RatisPartitionDescriptor(
                required(properties, prefix + "partitionId"),
                required(properties, prefix + "raftGroupId"),
                required(properties, prefix + "localPeerId"),
                peers,
                Path.of(required(properties, prefix + "dataPath"))
        );
    }

    private static boolean hasPartition(Properties properties, int partitionIndex) {
        return properties.containsKey("partition." + partitionIndex + ".partitionId");
    }

    private static boolean hasPeer(Properties properties, int partitionIndex, int peerIndex) {
        return properties.containsKey("partition." + partitionIndex + ".peer." + peerIndex + ".peerId");
    }

    private static void rejectPartitionGaps(Properties properties, int parsedPartitions) {
        for (var key : properties.stringPropertyNames()) {
            if (key.startsWith("partition.")) {
                var secondDot = key.indexOf('.', "partition.".length());
                var index = parseIndex(key, key.substring("partition.".length(), secondDot));
                if (index >= parsedPartitions) {
                    throw new IllegalArgumentException("Non-contiguous partition index in adapter config: " + key);
                }
            }
        }
    }

    private static void rejectPeerGaps(Properties properties, int partitionIndex, int parsedPeers) {
        var prefix = "partition." + partitionIndex + ".peer.";
        for (var key : properties.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                var indexStart = prefix.length();
                var indexEnd = key.indexOf('.', indexStart);
                var index = parseIndex(key, key.substring(indexStart, indexEnd));
                if (index >= parsedPeers) {
                    throw new IllegalArgumentException("Non-contiguous peer index in adapter config: " + key);
                }
            }
        }
    }

    private static void rejectUnknownKeys(Properties properties) {
        for (var key : properties.stringPropertyNames()) {
            if (!KNOWN_KEY.matcher(key).matches()) {
                throw new IllegalArgumentException("Unknown Ratis classpath adapter config key: " + key);
            }
        }
    }

    private static int parseIndex(String key, String index) {
        try {
            var parsed = Integer.parseInt(index);
            if (parsed < 0) {
                throw new IllegalArgumentException("Negative Ratis classpath adapter config index: " + key);
            }
            if (!Integer.toString(parsed).equals(index)) {
                throw new IllegalArgumentException("Non-canonical Ratis classpath adapter config index: " + key);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Ratis classpath adapter config index: " + key, e);
        }
    }

    private static String required(Properties properties, String key) {
        var value = properties.getProperty(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing Ratis classpath adapter config key: " + key);
        }
        if (value.isBlank()) {
            throw new IllegalArgumentException("Ratis classpath adapter config key must not be blank: " + key);
        }
        return value;
    }

    private static long requiredLong(Properties properties, String key) {
        var value = required(properties, key);
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Ratis classpath adapter config long key: " + key, e);
        }
    }

    private static String requireSha256(String value, String name) {
        Objects.requireNonNull(value, name);
        if (!SHA256.matcher(value).matches()) {
            throw new IllegalArgumentException(name + " must be a lowercase SHA-256: " + value);
        }
        return value;
    }
}
