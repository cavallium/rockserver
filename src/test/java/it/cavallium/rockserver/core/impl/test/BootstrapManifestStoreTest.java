package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapManifest;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapManifestStore;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapPeer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BootstrapManifestStoreTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics(@TempDir Path tempDir) {
        assertIllegalArgument("path must not be null", () -> BootstrapManifestStore.read(null));
        assertIllegalArgument("path must not be null", () -> BootstrapManifestStore.write(null, manifest()));
        assertIllegalArgument("manifest must not be null",
                () -> BootstrapManifestStore.write(tempDir.resolve("manifest.properties"), null));
        assertIllegalArgument("properties must not be null", () -> BootstrapManifestStore.fromProperties(null));
        assertIllegalArgument("manifest must not be null", () -> BootstrapManifestStore.toProperties(null));
    }

    @Test
    void roundTripsBootstrapManifest(@TempDir Path tempDir) throws Exception {
        var manifest = manifest();
        var path = tempDir.resolve("bootstrap").resolve("manifest.properties");

        BootstrapManifestStore.write(path, manifest);
        var reloaded = BootstrapManifestStore.read(path);

        assertEquals(manifest, reloaded);
    }

    @Test
    void readRejectsSymbolicLinkPath(@TempDir Path tempDir) throws Exception {
        var target = tempDir.resolve("target.properties");
        BootstrapManifestStore.write(target, manifest());
        var path = tempDir.resolve("manifest.properties");
        Files.createSymbolicLink(path, target.getFileName());

        var error = assertThrows(IOException.class, () -> BootstrapManifestStore.read(path));

        assertEquals("Bootstrap manifest path must be a regular file: " + path, error.getMessage());
    }

    @Test
    void readRejectsOversizedFileBeforeLoading(@TempDir Path tempDir) throws Exception {
        var path = tempDir.resolve("manifest.properties");
        try (var file = new RandomAccessFile(path.toFile(), "rw")) {
            file.setLength(BootstrapManifestStore.MAX_BOOTSTRAP_MANIFEST_BYTES + 1L);
        }

        assertEquals("Invalid bootstrap manifest payload length: "
                        + (BootstrapManifestStore.MAX_BOOTSTRAP_MANIFEST_BYTES + 1L),
                assertThrows(IllegalArgumentException.class, () -> BootstrapManifestStore.read(path))
                        .getMessage());
    }

    @Test
    void readRejectsDuplicateKeys(@TempDir Path tempDir) throws Exception {
        var path = tempDir.resolve("manifest.properties");
        Files.writeString(path,
                """
                        clusterId=00000000-0000-0000-0000-000000000001
                        metadataGroupId=metadata-group
                        bootstrapEpoch=7
                        peer.0.nodeId=n1
                        peer.0.publicEndpoint=http://127.0.0.1:8080
                        peer.0.ratisEndpoint=grpc://127.0.0.1:19001
                        peer.0.failureDomain=rack-a
                        peer.0.\\u006eodeId=n2
                        """,
                StandardCharsets.UTF_8);

        assertEquals("Duplicate bootstrap manifest payload key: peer.0.nodeId",
                assertThrows(IllegalArgumentException.class, () -> BootstrapManifestStore.read(path))
                        .getMessage());
    }

    @Test
    void writeRejectsOversizedOutputWithoutReplacingExistingManifest(@TempDir Path tempDir) throws Exception {
        var path = tempDir.resolve("manifest.properties");
        Files.writeString(path, "original");

        assertThrows(IllegalArgumentException.class, () -> BootstrapManifestStore.write(
                path,
                manifestWithFailureDomain("x".repeat(BootstrapManifestStore.MAX_BOOTSTRAP_MANIFEST_BYTES))
        ));

        assertEquals("original", Files.readString(path));
    }

    @Test
    void rejectsUnknownKeys() {
        var properties = BootstrapManifestStore.toProperties(manifest());
        properties.setProperty("unexpected", "value");

        assertThrows(IllegalArgumentException.class, () -> BootstrapManifestStore.fromProperties(properties));
    }

    @Test
    void rejectsGappedPeerIndexes() {
        var properties = new Properties();
        properties.setProperty("clusterId", new UUID(0, 1).toString());
        properties.setProperty("metadataGroupId", "metadata-group");
        properties.setProperty("bootstrapEpoch", "0");
        properties.setProperty("peer.0.nodeId", "n1");
        properties.setProperty("peer.0.publicEndpoint", "http://127.0.0.1:8080");
        properties.setProperty("peer.0.ratisEndpoint", "grpc://127.0.0.1:19001");
        properties.setProperty("peer.0.failureDomain", "rack-a");
        properties.setProperty("peer.2.nodeId", "n3");
        properties.setProperty("peer.2.publicEndpoint", "http://127.0.0.1:8082");
        properties.setProperty("peer.2.ratisEndpoint", "grpc://127.0.0.1:19003");
        properties.setProperty("peer.2.failureDomain", "rack-c");

        assertThrows(IllegalArgumentException.class, () -> BootstrapManifestStore.fromProperties(properties));
    }

    @Test
    void rejectsNonCanonicalPeerIndexes() {
        var alias = BootstrapManifestStore.toProperties(manifest());
        alias.setProperty("peer.00.nodeId", "n0");
        assertThrows(IllegalArgumentException.class, () -> BootstrapManifestStore.fromProperties(alias));

        var overflowing = BootstrapManifestStore.toProperties(manifest());
        overflowing.setProperty("peer.999999999999999999999.nodeId", "n3");
        assertThrows(IllegalArgumentException.class, () -> BootstrapManifestStore.fromProperties(overflowing));
    }

    @Test
    void rejectsNonCanonicalBootstrapEpoch() {
        var leadingZero = BootstrapManifestStore.toProperties(manifest());
        leadingZero.setProperty("bootstrapEpoch", "07");
        assertThrows(IllegalArgumentException.class, () -> BootstrapManifestStore.fromProperties(leadingZero));

        var explicitSign = BootstrapManifestStore.toProperties(manifest());
        explicitSign.setProperty("bootstrapEpoch", "+7");
        assertThrows(IllegalArgumentException.class, () -> BootstrapManifestStore.fromProperties(explicitSign));
    }

    @Test
    void rejectsLineBrokenMetadataGroupId() {
        var properties = BootstrapManifestStore.toProperties(manifest());
        properties.setProperty("metadataGroupId", "metadata-group\ninjected=true");

        assertThrows(IllegalArgumentException.class, () -> BootstrapManifestStore.fromProperties(properties));
    }

    @Test
    void rejectsMissingRequiredPeerKey() {
        var properties = BootstrapManifestStore.toProperties(manifest());
        properties.remove("peer.0.ratisEndpoint");

        assertThrows(IllegalArgumentException.class, () -> BootstrapManifestStore.fromProperties(properties));
    }

    @Test
    void rejectsInvalidPublicEndpoint() {
        var missingPort = BootstrapManifestStore.toProperties(manifest());
        missingPort.setProperty("peer.0.publicEndpoint", "http://127.0.0.1");
        assertThrows(IllegalArgumentException.class, () -> BootstrapManifestStore.fromProperties(missingPort));

        var decorated = BootstrapManifestStore.toProperties(manifest());
        decorated.setProperty("peer.0.publicEndpoint", "http://127.0.0.1:8080/rockserver");
        assertThrows(IllegalArgumentException.class, () -> BootstrapManifestStore.fromProperties(decorated));
    }

    @Test
    void rejectsInvalidRatisEndpoint() {
        var nonGrpc = BootstrapManifestStore.toProperties(manifest());
        nonGrpc.setProperty("peer.0.ratisEndpoint", "http://127.0.0.1:19001");
        assertThrows(IllegalArgumentException.class, () -> BootstrapManifestStore.fromProperties(nonGrpc));

        var decorated = BootstrapManifestStore.toProperties(manifest());
        decorated.setProperty("peer.0.ratisEndpoint", "grpc://127.0.0.1:19001/ratis");
        assertThrows(IllegalArgumentException.class, () -> BootstrapManifestStore.fromProperties(decorated));
    }

    private static BootstrapManifest manifest() {
        return manifestWithFailureDomain("rack-a");
    }

    private static BootstrapManifest manifestWithFailureDomain(String failureDomain) {
        return new BootstrapManifest(
                new ClusterId(new UUID(0, 1)),
                "metadata-group",
                List.of(
                        new BootstrapPeer(
                                NodeId.of("n1"),
                                URI.create("http://127.0.0.1:8080"),
                                URI.create("grpc://127.0.0.1:19001"),
                                failureDomain
                        ),
                        new BootstrapPeer(
                                NodeId.of("n2"),
                                URI.create("http://127.0.0.1:8081"),
                                URI.create("grpc://127.0.0.1:19002"),
                                "rack-b"
                        )
                ),
                7
        );
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
