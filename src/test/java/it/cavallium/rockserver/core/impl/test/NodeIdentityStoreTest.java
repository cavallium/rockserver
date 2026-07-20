package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.metadata.NodeIdentity;
import it.cavallium.rockserver.core.cluster.metadata.NodeIdentityStore;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NodeIdentityStoreTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics(@TempDir Path directory) {
        var identity = identity();

        assertIllegalArgument("directory must not be null", () -> NodeIdentityStore.read(null));
        assertIllegalArgument("directory must not be null", () -> NodeIdentityStore.loadOrPersist(null, identity));
        assertIllegalArgument("expectedIdentity must not be null", () -> NodeIdentityStore.loadOrPersist(directory, null));
        assertIllegalArgument("clusterId must not be null",
                () -> NodeIdentityStore.loadOrPersist(directory, null, identity.nodeId()));
        assertIllegalArgument("nodeId must not be null",
                () -> NodeIdentityStore.loadOrPersist(directory, identity.clusterId(), null));
    }

    @Test
    void persistsAndReloadsIdentityBeforeJoin(@TempDir Path directory) throws Exception {
        var identity = identity();

        var persisted = NodeIdentityStore.loadOrPersist(directory, identity);
        var reloaded = NodeIdentityStore.read(directory).orElseThrow();

        assertEquals(identity, persisted);
        assertEquals(identity, reloaded);
    }

    @Test
    void returnsExistingMatchingIdentity(@TempDir Path directory) throws Exception {
        var identity = identity();

        NodeIdentityStore.loadOrPersist(directory, identity);
        var loaded = NodeIdentityStore.loadOrPersist(directory, identity);

        assertEquals(identity, loaded);
    }

    @Test
    void rejectsMismatchedClusterOrNode(@TempDir Path directory) throws Exception {
        var identity = identity();
        NodeIdentityStore.loadOrPersist(directory, identity);

        assertThrows(IllegalStateException.class,
                () -> NodeIdentityStore.loadOrPersist(directory, new ClusterId(new UUID(0, 2)), identity.nodeId()));
        assertThrows(IllegalStateException.class,
                () -> NodeIdentityStore.loadOrPersist(directory, identity.clusterId(), NodeId.of("n2")));
    }

    @Test
    void emptyDirectoryHasNoIdentity(@TempDir Path directory) throws Exception {
        assertTrue(NodeIdentityStore.read(directory).isEmpty());
    }

    @Test
    void rejectsIncompleteIdentityFile(@TempDir Path directory) throws Exception {
        Files.writeString(directory.resolve(NodeIdentityStore.IDENTITY_FILE),
                "clusterId=00000000-0000-0000-0000-000000000001\n",
                StandardCharsets.UTF_8);

        assertThrows(IllegalStateException.class, () -> NodeIdentityStore.read(directory));
    }

    @Test
    void rejectsDuplicateIdentityKeys(@TempDir Path directory) throws Exception {
        Files.writeString(directory.resolve(NodeIdentityStore.IDENTITY_FILE),
                """
                        clusterId=00000000-0000-0000-0000-000000000001
                        nodeId=n1
                        \\u006eodeId=n2
                        """,
                StandardCharsets.UTF_8);

        assertEquals("Duplicate node identity key: nodeId",
                assertThrows(IllegalArgumentException.class, () -> NodeIdentityStore.read(directory))
                        .getMessage());
    }

    @Test
    void readRejectsOversizedIdentityBeforeLoading(@TempDir Path directory) throws Exception {
        var path = directory.resolve(NodeIdentityStore.IDENTITY_FILE);
        try (var file = new RandomAccessFile(path.toFile(), "rw")) {
            file.setLength(NodeIdentityStore.MAX_NODE_IDENTITY_BYTES + 1L);
        }

        assertEquals("Invalid node identity payload length: " + (NodeIdentityStore.MAX_NODE_IDENTITY_BYTES + 1L),
                assertThrows(IllegalArgumentException.class, () -> NodeIdentityStore.read(directory))
                        .getMessage());
    }

    @Test
    void writeRejectsOversizedIdentityWithoutPublishingFile(@TempDir Path directory) throws Exception {
        var identity = new NodeIdentity(
                new ClusterId(new UUID(0, 1)),
                NodeId.of("n".repeat(NodeIdentityStore.MAX_NODE_IDENTITY_BYTES))
        );

        assertThrows(IllegalArgumentException.class, () -> NodeIdentityStore.loadOrPersist(directory, identity));
        assertTrue(NodeIdentityStore.read(directory).isEmpty());
    }

    @Test
    void rejectsLineBrokenNodeIds(@TempDir Path directory) throws Exception {
        Files.writeString(directory.resolve(NodeIdentityStore.IDENTITY_FILE),
                """
                        clusterId=00000000-0000-0000-0000-000000000001
                        nodeId=n1\\ninjected=true
                        """,
                StandardCharsets.UTF_8);

        assertThrows(IllegalArgumentException.class, () -> NodeIdentityStore.read(directory));
    }

    @Test
    void rejectsSymbolicLinkIdentityFile(@TempDir Path directory) throws Exception {
        var target = directory.resolve("external-identity.properties");
        Files.writeString(target, "clusterId=00000000-0000-0000-0000-000000000001\nnodeId=n1\n",
                StandardCharsets.UTF_8);
        Files.createSymbolicLink(directory.resolve(NodeIdentityStore.IDENTITY_FILE), target.getFileName());

        assertThrows(IOException.class, () -> NodeIdentityStore.read(directory));
    }

    @Test
    void concurrentFirstStartPublishesExactlyOneIdentity(@TempDir Path directory) throws Exception {
        var start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(8)) {
            var futures = new ArrayList<java.util.concurrent.Future<NodeIdentity>>();
            for (var i = 0; i < 8; i++) {
                var identity = new NodeIdentity(new ClusterId(new UUID(0, 1)), NodeId.of("n" + i));
                futures.add(executor.submit(() -> {
                    start.await();
                    return NodeIdentityStore.loadOrPersist(directory, identity);
                }));
            }

            start.countDown();
            var persisted = new ArrayList<NodeIdentity>();
            for (var future : futures) {
                try {
                    persisted.add(future.get());
                } catch (ExecutionException e) {
                    assertInstanceOf(IllegalStateException.class, e.getCause());
                }
            }

            assertEquals(1, persisted.size());
            assertEquals(persisted.getFirst(), NodeIdentityStore.read(directory).orElseThrow());
        }
    }

    private static NodeIdentity identity() {
        return new NodeIdentity(new ClusterId(new UUID(0, 1)), NodeId.of("n1"));
    }

    private static void assertIllegalArgument(String message, ThrowingOperation operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }

    @FunctionalInterface
    private interface ThrowingOperation {

        void run() throws IOException;
    }
}
