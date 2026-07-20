package it.cavallium.rockserver.core.cluster;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public record PartitionReplicaStorageLayout(Path rootDirectory,
                                            NodeId nodeId,
                                            ShardId partitionId,
                                            long partitionEpoch) {

    public PartitionReplicaStorageLayout {
        var requiredRootDirectory = requireArgument(rootDirectory, "rootDirectory");
        requireArgument(nodeId, "nodeId");
        requireArgument(partitionId, "partitionId");
        if (partitionEpoch < 0) {
            throw new IllegalArgumentException("partitionEpoch must be >= 0");
        }
        rootDirectory = requiredRootDirectory.normalize();
    }

    public Path nodeDirectory() {
        return rootDirectory.resolve("nodes").resolve(encodePathSegment(nodeId.value()));
    }

    public Path partitionDirectory() {
        return nodeDirectory()
                .resolve("partitions")
                .resolve("partition-" + partitionId)
                .resolve("epoch-" + partitionEpoch);
    }

    public Path rocksDbPath() {
        return partitionDirectory().resolve("rocksdb");
    }

    public Path ratisStoragePath() {
        return partitionDirectory().resolve("ratis");
    }

    public Path snapshotsPath() {
        return partitionDirectory().resolve("snapshots");
    }

    private static String encodePathSegment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
