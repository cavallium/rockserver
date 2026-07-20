package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisPeerDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterConfig;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterDeploymentPreflight;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RatisProcessAdapterFileWriterTest {

    @Test
    void configWriteReplacesTargetSymlinkWithoutMutatingDestination(@TempDir Path tempDir) throws Exception {
        var redirectedPath = tempDir.resolve("redirected.properties");
        Files.writeString(redirectedPath, "redirected");
        var targetPath = tempDir.resolve("adapter.properties");
        Files.createSymbolicLink(targetPath, redirectedPath);
        var config = new RatisProcessAdapterConfig(List.of(new RatisPartitionDescriptor(
                "7",
                "group-7",
                "n1",
                List.of(new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19001", "rack-a")),
                tempDir.resolve("partition-7")
        )));

        config.write(targetPath, "0".repeat(64));

        assertFalse(Files.isSymbolicLink(targetPath));
        assertEquals("redirected", Files.readString(redirectedPath));
        var loaded = new Properties();
        try (var reader = Files.newBufferedReader(targetPath)) {
            loaded.load(reader);
        }
        assertEquals("7", loaded.getProperty("partition.0.partitionId"));
        assertNoTempFiles(tempDir, targetPath);
    }

    @Test
    void failedBoundedReportWritePreservesTargetAndCleansTempFile(@TempDir Path tempDir) throws Exception {
        var targetPath = tempDir.resolve("ratis-adapter-preflight.properties");
        Files.writeString(targetPath, "original");
        var report = new RatisProcessAdapterDeploymentPreflight.Report(List.of(
                "x".repeat(RatisProcessAdapterDeploymentPreflight.MAX_PREFLIGHT_REPORT_BYTES + 1)
        ));

        assertThrows(IllegalArgumentException.class, () -> report.write(targetPath));

        assertEquals("original", Files.readString(targetPath));
        assertNoTempFiles(tempDir, targetPath);
    }

    @Test
    void configWriteCreatesNestedParentDirectories(@TempDir Path tempDir) throws Exception {
        var targetPath = tempDir.resolve("generated").resolve("config").resolve("adapter.properties");
        var config = new RatisProcessAdapterConfig(List.of(new RatisPartitionDescriptor(
                "7",
                "group-7",
                "n1",
                List.of(new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19001", "rack-a")),
                tempDir.resolve("partition-7")
        )));

        config.write(targetPath, "0".repeat(64));

        var loaded = new Properties();
        try (var reader = Files.newBufferedReader(targetPath)) {
            loaded.load(reader);
        }
        assertEquals("7", loaded.getProperty("partition.0.partitionId"));
        assertNoTempFiles(targetPath.getParent(), targetPath);
    }

    @Test
    void publicationRejectsNullOutputPathsWithExplicitDiagnostics(@TempDir Path tempDir) {
        var config = new RatisProcessAdapterConfig(List.of(new RatisPartitionDescriptor(
                "7",
                "group-7",
                "n1",
                List.of(new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19001", "rack-a")),
                tempDir.resolve("partition-7")
        )));
        var report = new RatisProcessAdapterDeploymentPreflight.Report(List.of());
        var evidence = new RatisProcessAdapterDeploymentPreflight.Evidence(
                List.of("java"),
                Map.of(),
                Path.of("/adapter-work"),
                Path.of("/adapter-config.properties"),
                Path.of("/adapter-metadata.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102),
                List.of(),
                new RatisProcessAdapterDeploymentPreflight.PlannedPropertiesEvidence("plannedConfig", 0, "sha"),
                new RatisProcessAdapterDeploymentPreflight.PlannedPropertiesEvidence("plannedMetadata", 0, "sha"),
                new RatisProcessAdapterDeploymentPreflight.PlannedPropertiesEvidence("publishedConfig", 0, "sha"),
                new RatisProcessAdapterDeploymentPreflight.PlannedPropertiesEvidence("publishedMetadata", 0, "sha"),
                List.of()
        );

        var nullConfigPath = assertThrows(IllegalArgumentException.class,
                () -> config.write(null, "0".repeat(64)));
        var nullReportPath = assertThrows(IllegalArgumentException.class,
                () -> report.write(null));
        var nullEvidencePath = assertThrows(IllegalArgumentException.class,
                () -> evidence.write(null));

        assertEquals("path must not be null", nullConfigPath.getMessage());
        assertEquals("reportPath must not be null", nullReportPath.getMessage());
        assertEquals("evidencePath must not be null", nullEvidencePath.getMessage());
    }

    private static void assertNoTempFiles(Path directory, Path targetPath) throws Exception {
        var prefix = targetPath.getFileName() + ".";
        try (var files = Files.list(directory)) {
            assertFalse(files.anyMatch(path -> path.getFileName().toString().startsWith(prefix)
                    && path.getFileName().toString().endsWith(".tmp")));
        }
    }
}
