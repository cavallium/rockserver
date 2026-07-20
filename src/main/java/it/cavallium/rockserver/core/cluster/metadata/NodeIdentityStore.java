package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.internal.BoundedInputStream;
import it.cavallium.rockserver.core.cluster.internal.BoundedOutputStream;
import it.cavallium.rockserver.core.cluster.internal.DurableAtomicFileWriter;
import it.cavallium.rockserver.core.cluster.internal.StrictProperties;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Optional;
import java.util.Properties;

public final class NodeIdentityStore {

    public static final String IDENTITY_FILE = "node-identity.properties";
    public static final int MAX_NODE_IDENTITY_BYTES = 64 * 1024;

    private static final String CLUSTER_ID_PROPERTY = "clusterId";
    private static final String NODE_ID_PROPERTY = "nodeId";
    private static final String NODE_IDENTITY_DESCRIPTION = "node identity payload";
    private static final String IDENTITY_LOCK_FILE = IDENTITY_FILE + ".lock";
    private static final Object IDENTITY_WRITE_LOCK = new Object();

    private NodeIdentityStore() {
    }

    public static Optional<NodeIdentity> read(Path directory) throws IOException {
        var requiredDirectory = requireArgument(directory, "directory");
        var file = requiredDirectory.resolve(IDENTITY_FILE);
        BasicFileAttributes attributes;
        try {
            attributes = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException _) {
            return Optional.empty();
        }
        if (!attributes.isRegularFile()) {
            throw new IOException("Node identity path must be a regular file: " + file);
        }

        requirePayloadLength(attributes.size());
        try (var input = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS);
             var reader = new InputStreamReader(
                     new BoundedInputStream(input, MAX_NODE_IDENTITY_BYTES, NODE_IDENTITY_DESCRIPTION),
                     StandardCharsets.UTF_8
             )) {
            var properties = StrictProperties.load(reader, "node identity");
            var clusterId = properties.getProperty(CLUSTER_ID_PROPERTY);
            var nodeId = properties.getProperty(NODE_ID_PROPERTY);
            if (clusterId == null || nodeId == null) {
                throw new IllegalStateException("Node identity file is missing clusterId or nodeId: " + file);
            }
            return Optional.of(new NodeIdentity(ClusterId.parse(clusterId), NodeId.of(nodeId)));
        }
    }

    public static NodeIdentity loadOrPersist(Path directory, ClusterId clusterId, NodeId nodeId) throws IOException {
        var requiredDirectory = requireArgument(directory, "directory");
        var requiredClusterId = requireArgument(clusterId, "clusterId");
        var requiredNodeId = requireArgument(nodeId, "nodeId");
        return loadOrPersist(requiredDirectory, new NodeIdentity(requiredClusterId, requiredNodeId));
    }

    public static NodeIdentity loadOrPersist(Path directory, NodeIdentity expectedIdentity) throws IOException {
        var requiredDirectory = requireArgument(directory, "directory");
        var requiredExpectedIdentity = requireArgument(expectedIdentity, "expectedIdentity");
        var existing = read(requiredDirectory);
        if (existing.isPresent()) {
            return requireExpectedIdentity(existing.get(), requiredExpectedIdentity);
        }

        synchronized (IDENTITY_WRITE_LOCK) {
            DurableAtomicFileWriter.createDirectoriesDurably(requiredDirectory);
            var lockFile = requiredDirectory.resolve(IDENTITY_LOCK_FILE);
            // The monitor prevents overlapping JVM locks; the file lock coordinates distinct server processes.
            try (var lockChannel = FileChannel.open(lockFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    LinkOption.NOFOLLOW_LINKS);
                 var _ = lockChannel.lock()) {
                existing = read(requiredDirectory);
                if (existing.isPresent()) {
                    return requireExpectedIdentity(existing.get(), requiredExpectedIdentity);
                }
                writeAtomically(requiredDirectory.resolve(IDENTITY_FILE), requiredExpectedIdentity);
                return requiredExpectedIdentity;
            }
        }
    }

    private static void writeAtomically(Path file, NodeIdentity identity) throws IOException {
        var properties = new Properties();
        properties.setProperty(CLUSTER_ID_PROPERTY, identity.clusterId().toString());
        properties.setProperty(NODE_ID_PROPERTY, identity.nodeId().toString());

        DurableAtomicFileWriter.write(file, temp -> {
            try (var output = Files.newOutputStream(temp);
                 var writer = new OutputStreamWriter(
                         new BoundedOutputStream(output, MAX_NODE_IDENTITY_BYTES, NODE_IDENTITY_DESCRIPTION),
                         StandardCharsets.UTF_8
                 )) {
                properties.store(writer, "Rockserver node identity");
            }
        });
    }

    private static NodeIdentity requireExpectedIdentity(NodeIdentity existingIdentity, NodeIdentity expectedIdentity) {
        if (!existingIdentity.equals(expectedIdentity)) {
            throw new IllegalStateException("Existing node identity does not match expected identity");
        }
        return existingIdentity;
    }

    private static void requirePayloadLength(long length) {
        if (length > MAX_NODE_IDENTITY_BYTES) {
            throw new IllegalArgumentException("Invalid " + NODE_IDENTITY_DESCRIPTION + " length: " + length);
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
