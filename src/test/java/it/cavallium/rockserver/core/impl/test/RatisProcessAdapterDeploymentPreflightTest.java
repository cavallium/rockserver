package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ClusterNodeMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterDeploymentPreflight;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterCommand;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterConfig;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterDistribution;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterLaunchPlan;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterPaths;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.CRC32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RatisProcessAdapterDeploymentPreflightTest {

    private static final String TEST_SHA256 =
            "9f86d081884c7d659a2feaa0c55ad015"
                    + "a3bf4f1b2b0b822cd15d6c15b0f00a08";

    @Test
    void preflightAcceptsExecutableDistributionAndWritableStorageLayout(@TempDir Path tempDir) throws Exception {
        var plan = plan(
                validDistribution(tempDir),
                singleNodeMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertTrue(report.accepted(), report.message());
        assertEquals(List.of(), report.problems());
        assertTrue(report.asLines().contains("accepted=true"));
        assertTrue(report.asLines().contains("problem.count=0"));
        assertTrue(report.asLines().contains("message=Ratis adapter deployment preflight accepted"));
        RatisProcessAdapterDeploymentPreflight.verify(plan);
        var reportPath = tempDir.resolve("archive").resolve("ratis-adapter-preflight.properties");
        var writtenReport = RatisProcessAdapterDeploymentPreflight.writeReport(plan, reportPath);
        assertEquals(report, writtenReport);
        assertEquals(report.asText(), Files.readString(reportPath));
        assertTrue(Files.isDirectory(tempDir.resolve("work")));
        assertTrue(Files.isDirectory(tempDir.resolve("adapter-data").resolve("partition-7")));
        assertTrue(Files.isDirectory(tempDir.resolve("metadata")));
    }

    @Test
    void preflightModelsRejectNullInputsWithExplicitDiagnostics(@TempDir Path tempDir) {
        var reportPath = tempDir.resolve("preflight.properties");
        var evidencePath = tempDir.resolve("evidence.properties");
        var nullProblemEntry = new ArrayList<String>();
        nullProblemEntry.add(null);
        var nullCommandLineEntry = new ArrayList<String>();
        nullCommandLineEntry.add(null);
        var nullEnvironmentValue = new HashMap<String, String>();
        nullEnvironmentValue.put("SAFE", null);
        var nullLaunchFileEntry = new ArrayList<RatisProcessAdapterDeploymentPreflight.FileEvidence>();
        nullLaunchFileEntry.add(null);
        var nullPartitionEntry = new ArrayList<RatisProcessAdapterDeploymentPreflight.PartitionEvidence>();
        nullPartitionEntry.add(null);
        var nullVoterEndpointEntry = new ArrayList<String>();
        nullVoterEndpointEntry.add(null);
        var plannedConfig = new RatisProcessAdapterDeploymentPreflight.PlannedPropertiesEvidence(
                "plannedConfig",
                0,
                TEST_SHA256
        );
        var plannedMetadata = new RatisProcessAdapterDeploymentPreflight.PlannedPropertiesEvidence(
                "plannedMetadata",
                0,
                TEST_SHA256
        );
        var publishedConfig = new RatisProcessAdapterDeploymentPreflight.PlannedPropertiesEvidence(
                "publishedConfig",
                0,
                TEST_SHA256
        );
        var publishedMetadata = new RatisProcessAdapterDeploymentPreflight.PlannedPropertiesEvidence(
                "publishedMetadata",
                0,
                TEST_SHA256
        );
        var launchFiles = List.of(new RatisProcessAdapterDeploymentPreflight.FileEvidence(
                "javaExecutable",
                Path.of("/usr/bin/java"),
                0,
                TEST_SHA256
        ));
        var partitions = List.of(new RatisProcessAdapterDeploymentPreflight.PartitionEvidence(
                "partition-7",
                "group-7",
                "n1",
                Path.of("/adapter-data/partition-7"),
                List.of("n1=grpc://127.0.0.1:19001")
        ));

        var nullCheckPlan = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterDeploymentPreflight.check(null));
        var nullWriteReportPlan = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterDeploymentPreflight.writeReport(null, reportPath));
        var nullWriteReportPath = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterDeploymentPreflight.writeReport(null, null));
        var nullEvidencePlan = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterDeploymentPreflight.evidence(null));
        var nullWriteEvidencePlan = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterDeploymentPreflight.writeEvidence(null, evidencePath));
        var nullWriteEvidencePath = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterDeploymentPreflight.writeEvidence(null, null));
        var nullProblems = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterDeploymentPreflight.Report(null));
        var nullProblem = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterDeploymentPreflight.Report(nullProblemEntry));
        var nullCommandLine = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterDeploymentPreflight.Evidence(
                        null,
                        Map.of(),
                        Path.of("/work"),
                        Path.of("/config"),
                        Path.of("/metadata"),
                        new InetSocketAddress("127.0.0.1", 19101),
                        new InetSocketAddress("127.0.0.1", 19102),
                        launchFiles,
                        plannedConfig,
                        plannedMetadata,
                        publishedConfig,
                        publishedMetadata,
                        partitions
                ));
        var nullCommandLineEntryError = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterDeploymentPreflight.Evidence(
                        nullCommandLineEntry,
                        Map.of(),
                        Path.of("/work"),
                        Path.of("/config"),
                        Path.of("/metadata"),
                        new InetSocketAddress("127.0.0.1", 19101),
                        new InetSocketAddress("127.0.0.1", 19102),
                        launchFiles,
                        plannedConfig,
                        plannedMetadata,
                        publishedConfig,
                        publishedMetadata,
                        partitions
                ));
        var nullEnvironment = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterDeploymentPreflight.Evidence(
                        List.of("java"),
                        null,
                        Path.of("/work"),
                        Path.of("/config"),
                        Path.of("/metadata"),
                        new InetSocketAddress("127.0.0.1", 19101),
                        new InetSocketAddress("127.0.0.1", 19102),
                        launchFiles,
                        plannedConfig,
                        plannedMetadata,
                        publishedConfig,
                        publishedMetadata,
                        partitions
                ));
        var nullEnvironmentValueError = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterDeploymentPreflight.Evidence(
                        List.of("java"),
                        nullEnvironmentValue,
                        Path.of("/work"),
                        Path.of("/config"),
                        Path.of("/metadata"),
                        new InetSocketAddress("127.0.0.1", 19101),
                        new InetSocketAddress("127.0.0.1", 19102),
                        launchFiles,
                        plannedConfig,
                        plannedMetadata,
                        publishedConfig,
                        publishedMetadata,
                        partitions
                ));
        var nullLaunchFile = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterDeploymentPreflight.Evidence(
                        List.of("java"),
                        Map.of(),
                        Path.of("/work"),
                        Path.of("/config"),
                        Path.of("/metadata"),
                        new InetSocketAddress("127.0.0.1", 19101),
                        new InetSocketAddress("127.0.0.1", 19102),
                        nullLaunchFileEntry,
                        plannedConfig,
                        plannedMetadata,
                        publishedConfig,
                        publishedMetadata,
                        partitions
                ));
        var nullPartition = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterDeploymentPreflight.Evidence(
                        List.of("java"),
                        Map.of(),
                        Path.of("/work"),
                        Path.of("/config"),
                        Path.of("/metadata"),
                        new InetSocketAddress("127.0.0.1", 19101),
                        new InetSocketAddress("127.0.0.1", 19102),
                        launchFiles,
                        plannedConfig,
                        plannedMetadata,
                        publishedConfig,
                        publishedMetadata,
                        nullPartitionEntry
                ));
        var nullFilePath = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterDeploymentPreflight.FileEvidence(
                        "javaExecutable",
                        null,
                        0,
                        TEST_SHA256
                ));
        var nullVoterEndpoints = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterDeploymentPreflight.PartitionEvidence(
                        "partition-7",
                        "group-7",
                        "n1",
                        Path.of("/adapter-data/partition-7"),
                        null
                ));
        var nullVoterEndpoint = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterDeploymentPreflight.PartitionEvidence(
                        "partition-7",
                        "group-7",
                        "n1",
                        Path.of("/adapter-data/partition-7"),
                        nullVoterEndpointEntry
                ));

        assertEquals("plan must not be null", nullCheckPlan.getMessage());
        assertEquals("plan must not be null", nullWriteReportPlan.getMessage());
        assertEquals("reportPath must not be null", nullWriteReportPath.getMessage());
        assertEquals("plan must not be null", nullEvidencePlan.getMessage());
        assertEquals("plan must not be null", nullWriteEvidencePlan.getMessage());
        assertEquals("evidencePath must not be null", nullWriteEvidencePath.getMessage());
        assertEquals("problems must not be null", nullProblems.getMessage());
        assertEquals("problems entry must not be null", nullProblem.getMessage());
        assertEquals("commandLine must not be null", nullCommandLine.getMessage());
        assertEquals("commandLine entry must not be null", nullCommandLineEntryError.getMessage());
        assertEquals("environment must not be null", nullEnvironment.getMessage());
        assertEquals("environment value must not be null", nullEnvironmentValueError.getMessage());
        assertEquals("launchFiles entry must not be null", nullLaunchFile.getMessage());
        assertEquals("partitions entry must not be null", nullPartition.getMessage());
        assertEquals("path must not be null", nullFilePath.getMessage());
        assertEquals("voterEndpoints must not be null", nullVoterEndpoints.getMessage());
        assertEquals("voterEndpoints entry must not be null", nullVoterEndpoint.getMessage());
    }

    @Test
    void preflightReportsMissingExecutableAndClasspathFiles(@TempDir Path tempDir) {
        var distribution = new RatisProcessAdapterDistribution(
                tempDir.resolve("missing-adapter-home"),
                tempDir.resolve("missing-java"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of(Path.of("lib/missing-ratis-server.jar")),
                List.of(),
                Map.of()
        );
        var plan = plan(
                distribution,
                singleNodeMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertFalse(report.accepted());
        assertProblemContains(report, "javaExecutable does not exist");
        assertProblemContains(report, "adapterHome cannot be resolved");
        assertProblemContains(report, "classpath entry does not exist");
        assertTrue(report.asLines().contains("accepted=false"));
        assertTrue(report.asLines().contains("problem.count=3"));
        var error = assertThrows(IOException.class,
                () -> RatisProcessAdapterDeploymentPreflight.verify(plan));
        assertTrue(error.getMessage().contains("Ratis adapter deployment preflight failed"));
        assertThrows(IOException.class, () -> RatisProcessAdapterDeploymentPreflight.evidence(plan));
        var evidencePath = tempDir.resolve("archive").resolve("ratis-adapter-evidence.properties");
        assertThrows(IOException.class, () -> RatisProcessAdapterDeploymentPreflight.writeEvidence(plan, evidencePath));
        assertFalse(Files.exists(evidencePath));
    }

    @Test
    void preflightRejectsAdapterHomeThatIsNotDirectory(@TempDir Path tempDir) throws Exception {
        var javaExecutable = tempDir.resolve("java-home").resolve("bin").resolve("java");
        Files.createDirectories(javaExecutable.getParent());
        Files.writeString(javaExecutable, "test");
        assertTrue(javaExecutable.toFile().setExecutable(true));
        var adapterHome = tempDir.resolve("adapter-home");
        Files.writeString(adapterHome, "not a directory");
        var distribution = new RatisProcessAdapterDistribution(
                adapterHome,
                javaExecutable,
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of(Path.of("lib/ratis-server.jar")),
                List.of(),
                Map.of()
        );
        var plan = plan(
                distribution,
                singleNodeMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertFalse(report.accepted());
        assertProblemContains(report, "adapterHome must resolve to a directory");
    }

    @Test
    void preflightRejectsClasspathSymlinkOutsideAdapterHome(@TempDir Path tempDir) throws Exception {
        var distribution = validDistribution(tempDir);
        var externalJar = tempDir.resolve("outside").resolve("injected.jar");
        createFile(externalJar);
        var classpathSymlink = distribution.adapterHome().resolve("lib").resolve("ratis-server.jar");
        Files.delete(classpathSymlink);
        Files.createSymbolicLink(classpathSymlink, externalJar);
        var plan = plan(
                distribution,
                singleNodeMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertFalse(report.accepted());
        assertProblemContains(report, "classpath entry must resolve inside adapterHome");
    }

    @Test
    void preflightRejectsClasspathSymlinkDuplicateInsideAdapterHome(@TempDir Path tempDir) throws Exception {
        var baseDistribution = validDistribution(tempDir);
        var duplicateClasspathEntry = baseDistribution.adapterHome().resolve("lib").resolve("ratis-server-copy.jar");
        Files.createSymbolicLink(duplicateClasspathEntry, Path.of("ratis-server.jar"));
        var distribution = new RatisProcessAdapterDistribution(
                baseDistribution.adapterHome(),
                baseDistribution.javaExecutable(),
                baseDistribution.mainClass(),
                List.of(
                        Path.of("adapter/rockserver-ratis-adapter.jar"),
                        Path.of("lib/ratis-server.jar"),
                        Path.of("lib/ratis-server-copy.jar")
                ),
                baseDistribution.defaultJvmArguments(),
                baseDistribution.environment()
        );
        var plan = plan(
                distribution,
                singleNodeMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertFalse(report.accepted());
        assertProblemContains(report, "classpath entry must not resolve to duplicate classpath file");
    }

    @Test
    void preflightRejectsEmptyExecutableAndClasspathFiles(@TempDir Path tempDir) throws Exception {
        var distribution = validDistribution(tempDir);
        Files.writeString(distribution.javaExecutable(), "");
        Files.writeString(distribution.adapterHome().resolve("lib").resolve("ratis-server.jar"), "");
        var plan = plan(
                distribution,
                singleNodeMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertFalse(report.accepted());
        assertProblemContains(report, "javaExecutable must not be empty");
        assertProblemContains(report, "classpath entry must not be empty");
    }

    @Test
    void preflightRejectsUnreadableClasspathJar(@TempDir Path tempDir) throws Exception {
        var distribution = validDistribution(tempDir);
        Files.writeString(distribution.adapterHome().resolve("lib").resolve("ratis-server.jar"), "not a jar");
        var plan = plan(
                distribution,
                singleNodeMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertFalse(report.accepted());
        assertProblemContains(report, "classpath entry must be a readable JAR file");
    }

    @Test
    void preflightRejectsOverlappingWorkingAndPartitionDataDirectories(@TempDir Path tempDir) throws Exception {
        var sharedDataRoot = tempDir.resolve("adapter-data");
        var plan = plan(
                validDistribution(tempDir),
                singleNodeMetadata(),
                sharedDataRoot,
                sharedDataRoot,
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertFalse(report.accepted());
        assertProblemContains(report, "workingDirectory must not overlap partition 7 dataPath");
    }

    @Test
    void preflightRejectsSymlinkedStorageDirectoryOverlap(@TempDir Path tempDir) throws Exception {
        var sharedDataRoot = tempDir.resolve("adapter-data");
        Files.createDirectories(sharedDataRoot);
        var workingDirectory = tempDir.resolve("adapter-work-link");
        Files.createSymbolicLink(workingDirectory, sharedDataRoot);
        var plan = plan(
                validDistribution(tempDir),
                singleNodeMetadata(),
                workingDirectory,
                sharedDataRoot,
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertFalse(report.accepted());
        assertProblemContains(report, "workingDirectory must not overlap partition 7 dataPath");
    }

    @Test
    void preflightRejectsPartitionDataSymlinkOutsideAdapterDataRoot(@TempDir Path tempDir) throws Exception {
        var adapterDataRoot = tempDir.resolve("adapter-data");
        var redirectedData = tempDir.resolve("redirected-data");
        Files.createDirectories(adapterDataRoot);
        Files.createDirectories(redirectedData);
        Files.createSymbolicLink(adapterDataRoot.resolve("partition-7"), redirectedData);
        var plan = plan(
                validDistribution(tempDir),
                singleNodeMetadata(),
                tempDir.resolve("work"),
                adapterDataRoot,
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertFalse(report.accepted());
        assertProblemContains(report, "partition 7 dataPath must resolve inside adapterDataRoot");
    }

    @Test
    void preflightRejectsBrokenPartitionDataSymlink(@TempDir Path tempDir) throws Exception {
        var adapterDataRoot = tempDir.resolve("adapter-data");
        Files.createDirectories(adapterDataRoot);
        Files.createSymbolicLink(
                adapterDataRoot.resolve("partition-7"),
                adapterDataRoot.resolve("missing-partition-target")
        );
        var plan = plan(
                validDistribution(tempDir),
                singleNodeMetadata(),
                tempDir.resolve("work"),
                adapterDataRoot,
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertFalse(report.accepted());
        assertProblemContains(report, "partition 7 dataPath could not be resolved safely");
    }

    @Test
    void existingAncestorResolutionRejectsBrokenSymlink(@TempDir Path tempDir) throws Exception {
        var brokenLink = tempDir.resolve("broken-link");
        Files.createSymbolicLink(brokenLink, tempDir.resolve("missing-target"));

        assertThrows(IOException.class,
                () -> RatisProcessAdapterPaths.resolveExistingAncestor(brokenLink.resolve("child")));
    }

    @Test
    void existingAncestorResolutionRejectsNullInputsWithExplicitDiagnostics(@TempDir Path tempDir) {
        var nullAncestorPath = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterPaths.resolveExistingAncestor(null));
        var nullDirectory = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterPaths.resolvesInside(null, tempDir));
        var nullPath = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterPaths.resolvesInside(tempDir, null));

        assertEquals("path must not be null", nullAncestorPath.getMessage());
        assertEquals("directory must not be null", nullDirectory.getMessage());
        assertEquals("path must not be null", nullPath.getMessage());
    }

    @Test
    void preflightAcceptsSymlinkedAdapterDataRootWithContainedChildren(@TempDir Path tempDir) throws Exception {
        var realAdapterDataRoot = tempDir.resolve("real-adapter-data");
        Files.createDirectories(realAdapterDataRoot);
        var adapterDataRoot = tempDir.resolve("adapter-data");
        Files.createSymbolicLink(adapterDataRoot, realAdapterDataRoot);
        var plan = plan(
                validDistribution(tempDir),
                singleNodeMetadata(),
                tempDir.resolve("work"),
                adapterDataRoot,
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertTrue(report.accepted(), report.message());
        assertTrue(Files.isDirectory(realAdapterDataRoot.resolve("partition-7")));
    }

    @Test
    void preflightRejectsWorkingDirectoryInsideAdapterDataRoot(@TempDir Path tempDir) throws Exception {
        var adapterDataRoot = tempDir.resolve("adapter-data");
        var plan = plan(
                validDistribution(tempDir),
                singleNodeMetadata(),
                adapterDataRoot.resolve("work"),
                adapterDataRoot,
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertFalse(report.accepted());
        assertProblemContains(report, "workingDirectory must not overlap adapterDataRoot");
    }

    @Test
    void preflightRejectsConfigInsideAdapterDataRoot(@TempDir Path tempDir) throws Exception {
        var adapterDataRoot = tempDir.resolve("adapter-data");
        var plan = plan(
                validDistribution(tempDir),
                singleNodeMetadata(),
                tempDir.resolve("work"),
                adapterDataRoot,
                adapterDataRoot.resolve("config").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertFalse(report.accepted());
        assertProblemContains(report, "configPath must not be inside adapterDataRoot");
        assertProblemContains(report, "metadataPath must not be inside adapterDataRoot");
    }

    @Test
    void preflightRejectsGeneratedConfigInsideAdapterHome(@TempDir Path tempDir) throws Exception {
        var distribution = validDistribution(tempDir);
        var plan = plan(
                distribution,
                singleNodeMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                distribution.adapterHome().resolve("generated").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertFalse(report.accepted());
        assertProblemContains(report, "configPath must not be inside adapterHome");
        assertProblemContains(report, "metadataPath must not be inside adapterHome");
    }

    @Test
    void preflightRejectsNonCanonicalPartitionDataPath(@TempDir Path tempDir) throws Exception {
        var plan = plan(
                validDistribution(tempDir),
                singleNodeMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );
        var partition = plan.adapterConfig().partitions().getFirst();
        var tamperedConfig = new RatisProcessAdapterConfig(
                plan.adapterConfig().metadataEpoch(),
                List.of(new RatisPartitionDescriptor(
                        partition.partitionId(),
                        partition.raftGroupId(),
                        partition.localPeerId(),
                        partition.voters(),
                        tempDir.resolve("redirected-data")
                ))
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(withAdapterConfig(plan, tamperedConfig));

        assertFalse(report.accepted());
        assertProblemContains(report, "dataPath must equal canonical adapterDataRoot child");
    }

    @Test
    void launchPlanRejectsDuplicatePeerEndpoints(@TempDir Path tempDir) {
        assertThrows(IllegalArgumentException.class, () -> plan(
                validDistribution(tempDir),
                duplicatePeerEndpointMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        ));
    }

    @Test
    void preflightRejectsSharedHealthAndRpcListener(@TempDir Path tempDir) throws Exception {
        var plan = plan(
                validDistribution(tempDir),
                singleNodeMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19101)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertFalse(report.accepted());
        assertProblemContains(report, "healthAddress and rpcAddress must not share the same listener port");
    }

    @Test
    void preflightRejectsAdapterListenersThatCollideWithLocalRatisEndpoint(@TempDir Path tempDir)
            throws Exception {
        var plan = plan(
                validDistribution(tempDir),
                singleNodeMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("0.0.0.0", 19001),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertFalse(report.accepted());
        assertProblemContains(report, "healthAddress must not share the local Ratis listener");
    }

    @Test
    void preflightRejectsLoopbackAliasListenerCollisions(@TempDir Path tempDir)
            throws Exception {
        var plan = plan(
                validDistribution(tempDir),
                singleNodeMetadata(URI.create("grpc://localhost:19001")),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19001),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(plan);

        assertFalse(report.accepted());
        assertProblemContains(report, "healthAddress must not share the local Ratis listener");
    }

    @Test
    void preflightRejectsMismatchedMetadataGroupTopologyFingerprint(@TempDir Path tempDir) throws Exception {
        var plan = plan(
                validDistribution(tempDir),
                singleNodeMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );
        var command = plan.command();
        var environment = new HashMap<>(command.environment());
        environment.put(RatisProcessAdapterLaunchPlan.METADATA_GROUP_TOPOLOGY_SHA256_ENV, "0".repeat(64));
        var tamperedPlan = new RatisProcessAdapterLaunchPlan(
                plan.adapterConfig(),
                plan.metadataConfig(),
                plan.adapterHome(),
                plan.adapterDataRoot(),
                plan.configPath(),
                plan.metadataPath(),
                new RatisProcessAdapterCommand(
                        command.javaExecutable(),
                        command.workingDirectory(),
                        command.classpathEntries(),
                        command.mainClass(),
                        command.jvmArguments(),
                        command.programArguments(),
                        environment
                ),
                plan.healthAddress(),
                plan.rpcAddress()
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(tamperedPlan);

        assertFalse(report.accepted());
        assertProblemContains(report, RatisProcessAdapterLaunchPlan.METADATA_GROUP_TOPOLOGY_SHA256_ENV);
    }

    @Test
    void preflightRejectsDuplicateTamperedOrExtraProgramArguments(@TempDir Path tempDir) throws Exception {
        var plan = plan(
                validDistribution(tempDir),
                singleNodeMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );
        var command = plan.command();
        var programArguments = new ArrayList<>(command.programArguments());
        programArguments.add("--health-port=19199");
        programArguments.add("--print-health");
        var tamperedPlan = new RatisProcessAdapterLaunchPlan(
                plan.adapterConfig(),
                plan.metadataConfig(),
                plan.adapterHome(),
                plan.adapterDataRoot(),
                plan.configPath(),
                plan.metadataPath(),
                new RatisProcessAdapterCommand(
                        command.javaExecutable(),
                        command.workingDirectory(),
                        command.classpathEntries(),
                        command.mainClass(),
                        command.jvmArguments(),
                        programArguments,
                        command.environment()
                ),
                plan.healthAddress(),
                plan.rpcAddress()
        );

        var report = RatisProcessAdapterDeploymentPreflight.check(tamperedPlan);

        assertFalse(report.accepted());
        assertProblemContains(report, "programArguments must match generated adapter launch arguments");
    }

    @Test
    void preflightEvidenceCapturesSupervisorLaunchMaterial(@TempDir Path tempDir) throws Exception {
        var plan = plan(
                validDistribution(tempDir),
                singleNodeMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );
        plan.writeConfig();

        var evidence = RatisProcessAdapterDeploymentPreflight.evidence(plan);

        assertEquals(plan.command().toCommandLine(), evidence.commandLine());
        assertEquals(plan.command().environment(), evidence.environment());
        assertEquals(plan.command().workingDirectory(), evidence.workingDirectory());
        assertEquals(plan.configPath(), evidence.configPath());
        assertEquals(plan.metadataPath(), evidence.metadataPath());
        assertEquals(plan.healthAddress(), evidence.healthAddress());
        assertEquals(plan.rpcAddress(), evidence.rpcAddress());
        assertEquals(3, evidence.launchFiles().size());
        var javaFile = evidence.launchFiles().getFirst();
        assertEquals("javaExecutable", javaFile.role());
        assertEquals(plan.command().javaExecutable(), javaFile.path());
        assertEquals(4, javaFile.size());
        assertEquals(TEST_SHA256, javaFile.sha256());
        var adapterJar = evidence.launchFiles().get(1);
        assertEquals("classpath", adapterJar.role());
        assertEquals(plan.command().classpathEntries().getFirst(), adapterJar.path());
        assertEquals(Files.size(adapterJar.path()), adapterJar.size());
        assertEquals(sha256(adapterJar.path()), adapterJar.sha256());
        var ratisJar = evidence.launchFiles().get(2);
        assertEquals("classpath", ratisJar.role());
        assertEquals(plan.command().classpathEntries().get(1), ratisJar.path());
        assertEquals(Files.size(ratisJar.path()), ratisJar.size());
        assertEquals(sha256(ratisJar.path()), ratisJar.sha256());
        assertEquals(9, evidence.plannedConfig().propertyCount());
        assertEquals(17, evidence.plannedMetadata().propertyCount());
        assertEquals(9, evidence.publishedConfig().propertyCount());
        assertEquals(evidence.plannedConfig().sha256(), evidence.publishedConfig().sha256());
        assertEquals(17, evidence.publishedMetadata().propertyCount());
        assertEquals(evidence.plannedMetadata().sha256(), evidence.publishedMetadata().sha256());
        assertEquals(1, evidence.partitions().size());
        var partition = evidence.partitions().getFirst();
        assertEquals("7", partition.partitionId());
        assertEquals("n1", partition.localPeerId());
        assertEquals(tempDir.resolve("adapter-data").resolve("partition-7"), partition.dataPath());
        assertEquals(List.of("n1=grpc://127.0.0.1:19001"), partition.voterEndpoints());

        var evidenceLines = evidence.asLines();
        assertTrue(evidenceLines.contains("environment.ROCKSERVER_NODE_ID=n1"));
        assertTrue(evidenceLines.contains("environment.ROCKSERVER_RATIS_DATA_ROOT="
                + tempDir.resolve("adapter-data").toAbsolutePath().normalize()));
        assertTrue(evidenceLines.contains("environment.ROCKSERVER_RATIS_METADATA_GROUP_TOPOLOGY_SHA256="
                + plan.adapterConfig().metadataGroupTopologySha256()));
        assertTrue(evidenceLines.contains("healthAddress=127.0.0.1:19101"));
        assertTrue(evidenceLines.contains("rpcAddress=127.0.0.1:19102"));
        assertTrue(evidenceLines.contains("launchFile.count=3"));
        assertTrue(evidenceLines.contains("launchFile.0.role=javaExecutable"));
        assertTrue(evidenceLines.contains("launchFile.0.size=4"));
        assertTrue(evidenceLines.contains("launchFile.0.sha256=" + TEST_SHA256));
        assertTrue(evidenceLines.contains("launchFile.1.role=classpath"));
        assertTrue(evidenceLines.contains("launchFile.1.size=" + adapterJar.size()));
        assertTrue(evidenceLines.contains("launchFile.1.sha256=" + adapterJar.sha256()));
        assertTrue(evidenceLines.contains("plannedConfig.property.count=9"));
        assertTrue(evidenceLines.contains("plannedConfig.sha256=" + evidence.plannedConfig().sha256()));
        assertTrue(evidenceLines.contains("plannedMetadata.property.count=17"));
        assertTrue(evidenceLines.contains("plannedMetadata.sha256=" + evidence.plannedMetadata().sha256()));
        assertTrue(evidenceLines.contains("publishedConfig.property.count=9"));
        assertTrue(evidenceLines.contains("publishedConfig.sha256=" + evidence.publishedConfig().sha256()));
        assertTrue(evidenceLines.contains("publishedMetadata.property.count=17"));
        assertTrue(evidenceLines.contains("publishedMetadata.sha256=" + evidence.publishedMetadata().sha256()));
        assertTrue(evidenceLines.contains("partition.0.partitionId=7"));
        assertTrue(evidenceLines.contains("partition.0.voter.0=n1=grpc://127.0.0.1:19001"));

        var evidencePath = tempDir.resolve("archive").resolve("ratis-adapter-evidence.properties");
        RatisProcessAdapterDeploymentPreflight.writeEvidence(plan, evidencePath);
        assertEquals(evidence.asText(), Files.readString(evidencePath));
    }

    @Test
    void preflightEvidenceRequiresPublishedConfigPairToMatchPlan(@TempDir Path tempDir) throws Exception {
        var plan = plan(
                validDistribution(tempDir),
                singleNodeMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var missing = assertThrows(IOException.class, () -> RatisProcessAdapterDeploymentPreflight.evidence(plan));
        assertTrue(missing.getMessage().contains("ratis-adapter.properties"));

        plan.writeConfig();
        Files.writeString(plan.metadataPath(), Files.readString(plan.metadataPath()) + "tampered=true\n");

        var mismatched = assertThrows(IOException.class, () -> RatisProcessAdapterDeploymentPreflight.evidence(plan));
        assertTrue(mismatched.getMessage().contains("publishedMetadata"));
        assertTrue(mismatched.getMessage().contains("does not match planned properties"));
    }

    @Test
    void preflightArtifactsRejectOversizedWritesWithoutReplacingExistingFiles(@TempDir Path tempDir)
            throws Exception {
        var reportPath = tempDir.resolve("ratis-adapter-preflight.properties");
        Files.writeString(reportPath, "original-report");
        var oversizedReport = new RatisProcessAdapterDeploymentPreflight.Report(List.of(
                "x".repeat(RatisProcessAdapterDeploymentPreflight.MAX_PREFLIGHT_REPORT_BYTES + 1)
        ));

        var reportError = assertThrows(IllegalArgumentException.class, () -> oversizedReport.write(reportPath));

        assertTrue(reportError.getMessage().contains("preflight report"));
        assertEquals("original-report", Files.readString(reportPath));

        var evidencePath = tempDir.resolve("ratis-adapter-evidence.properties");
        Files.writeString(evidencePath, "original-evidence");
        var oversizedEvidence = new RatisProcessAdapterDeploymentPreflight.Evidence(
                List.of("java", "x".repeat(RatisProcessAdapterDeploymentPreflight.MAX_DEPLOYMENT_EVIDENCE_BYTES + 1)),
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

        var evidenceError = assertThrows(IllegalArgumentException.class,
                () -> oversizedEvidence.write(evidencePath));

        assertTrue(evidenceError.getMessage().contains("deployment evidence"));
        assertEquals("original-evidence", Files.readString(evidencePath));
    }

    @Test
    void preflightEvidenceRejectsOversizedLaunchFileBeforeHashing(@TempDir Path tempDir) throws Exception {
        var distribution = validDistribution(tempDir);
        var oversizedClasspathEntry = distribution.adapterHome().resolve("lib").resolve("ratis-server.jar");
        createOversizedReadableJar(oversizedClasspathEntry);
        var plan = plan(
                distribution,
                singleNodeMetadata(),
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("metadata").resolve("ratis-adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19101),
                new InetSocketAddress("127.0.0.1", 19102)
        );

        var error = assertThrows(IOException.class, () -> RatisProcessAdapterDeploymentPreflight.evidence(plan));

        assertTrue(error.getMessage().contains("launch file exceeds"));
    }

    private static void assertProblemContains(RatisProcessAdapterDeploymentPreflight.Report report, String text) {
        assertTrue(report.problems().stream().anyMatch(problem -> problem.contains(text)),
                () -> "Expected problem containing '" + text + "' in " + report.problems());
    }

    private static RatisProcessAdapterLaunchPlan withAdapterConfig(RatisProcessAdapterLaunchPlan plan,
                                                                   RatisProcessAdapterConfig adapterConfig) {
        return new RatisProcessAdapterLaunchPlan(
                adapterConfig,
                plan.metadataConfig(),
                plan.adapterHome(),
                plan.adapterDataRoot(),
                plan.configPath(),
                plan.metadataPath(),
                plan.command(),
                plan.healthAddress(),
                plan.rpcAddress()
        );
    }

    private static RatisProcessAdapterLaunchPlan plan(RatisProcessAdapterDistribution distribution,
                                                      ClusterMetadata metadata,
                                                      Path workingDirectory,
                                                      Path adapterDataRoot,
                                                      Path configPath,
                                                      InetSocketAddress healthAddress,
                                                      InetSocketAddress rpcAddress) {
        return RatisProcessAdapterLaunchPlan.fromMetadata(
                distribution,
                metadata,
                NodeId.of("n1"),
                new RatisProcessAdapterLaunchPlan.Options(
                        workingDirectory,
                        adapterDataRoot,
                        configPath,
                        configPath.resolveSibling(configPath.getFileName() + ".metadata"),
                        healthAddress,
                        rpcAddress,
                        List.of(),
                        Map.of()
                )
        );
    }

    private static RatisProcessAdapterDistribution validDistribution(Path tempDir) throws IOException {
        var javaExecutable = tempDir.resolve("java-home").resolve("bin").resolve("java");
        Files.createDirectories(javaExecutable.getParent());
        Files.writeString(javaExecutable, "test");
        assertTrue(javaExecutable.toFile().setExecutable(true));

        var adapterHome = tempDir.resolve("adapter-home");
        createFile(adapterHome.resolve("adapter").resolve("rockserver-ratis-adapter.jar"));
        createFile(adapterHome.resolve("lib").resolve("ratis-server.jar"));

        return new RatisProcessAdapterDistribution(
                adapterHome,
                javaExecutable,
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of(
                        Path.of("adapter/rockserver-ratis-adapter.jar"),
                        Path.of("lib/ratis-server.jar")
                ),
                List.of("-Xmx512m"),
                Map.of()
        );
    }

    private static void createFile(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        try (var jar = new JarOutputStream(Files.newOutputStream(path))) {
            var entry = new JarEntry("rockserver-fixture.txt");
            entry.setTime(0L);
            jar.putNextEntry(entry);
            jar.write("test".getBytes(StandardCharsets.UTF_8));
            jar.closeEntry();
        }
    }

    private static void createOversizedReadableJar(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        var entryName = "rockserver-fixture.txt".getBytes(StandardCharsets.UTF_8);
        var entryBytes = "test".getBytes(StandardCharsets.UTF_8);
        var crc = new CRC32();
        crc.update(entryBytes);
        try (var file = new RandomAccessFile(path.toFile(), "rw")) {
            file.setLength(0L);
            file.seek(RatisProcessAdapterDeploymentPreflight.MAX_LAUNCH_FILE_BYTES + 1);
            var localHeaderOffset = file.getFilePointer();
            writeIntLe(file, 0x04034b50L);
            writeShortLe(file, 10);
            writeShortLe(file, 0);
            writeShortLe(file, 0);
            writeShortLe(file, 0);
            writeShortLe(file, 0);
            writeIntLe(file, crc.getValue());
            writeIntLe(file, entryBytes.length);
            writeIntLe(file, entryBytes.length);
            writeShortLe(file, entryName.length);
            writeShortLe(file, 0);
            file.write(entryName);
            file.write(entryBytes);

            var centralDirectoryOffset = file.getFilePointer();
            writeIntLe(file, 0x02014b50L);
            writeShortLe(file, 20);
            writeShortLe(file, 10);
            writeShortLe(file, 0);
            writeShortLe(file, 0);
            writeShortLe(file, 0);
            writeShortLe(file, 0);
            writeIntLe(file, crc.getValue());
            writeIntLe(file, entryBytes.length);
            writeIntLe(file, entryBytes.length);
            writeShortLe(file, entryName.length);
            writeShortLe(file, 0);
            writeShortLe(file, 0);
            writeShortLe(file, 0);
            writeShortLe(file, 0);
            writeIntLe(file, 0);
            writeIntLe(file, localHeaderOffset);
            file.write(entryName);

            var centralDirectorySize = file.getFilePointer() - centralDirectoryOffset;
            writeIntLe(file, 0x06054b50L);
            writeShortLe(file, 0);
            writeShortLe(file, 0);
            writeShortLe(file, 1);
            writeShortLe(file, 1);
            writeIntLe(file, centralDirectorySize);
            writeIntLe(file, centralDirectoryOffset);
            writeShortLe(file, 0);
        }
    }

    private static void writeShortLe(RandomAccessFile file, long value) throws IOException {
        file.write((int) (value & 0xffL));
        file.write((int) ((value >>> 8) & 0xffL));
    }

    private static void writeIntLe(RandomAccessFile file, long value) throws IOException {
        file.write((int) (value & 0xffL));
        file.write((int) ((value >>> 8) & 0xffL));
        file.write((int) ((value >>> 16) & 0xffL));
        file.write((int) ((value >>> 24) & 0xffL));
    }

    private static String sha256(Path path) throws IOException {
        var digest = sha256Digest();
        digest.update(Files.readAllBytes(path));
        return HexFormat.of().formatHex(digest.digest());
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest is unavailable", e);
        }
    }

    private static ClusterMetadata singleNodeMetadata() {
        return singleNodeMetadata(URI.create("grpc://127.0.0.1:19001"));
    }

    private static ClusterMetadata singleNodeMetadata(URI ratisEndpoint) {
        var nodeId = NodeId.of("n1");
        var node = new ClusterNodeMetadata(
                nodeId,
                URI.create("http://127.0.0.1:8080"),
                ratisEndpoint,
                "rack-a"
        );
        var partition = partition(List.of(new ReplicaMember(
                nodeId,
                node.ratisEndpoint(),
                ReplicaRole.VOTER,
                0,
                0,
                true
        )), nodeId);
        return metadata(Map.of(node.nodeId(), node), partition);
    }

    private static ClusterMetadata duplicatePeerEndpointMetadata() {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var duplicateEndpoint = URI.create("grpc://127.0.0.1:19001");
        var node1 = new ClusterNodeMetadata(
                n1,
                URI.create("http://127.0.0.1:8080"),
                duplicateEndpoint,
                "rack-a"
        );
        var node2 = new ClusterNodeMetadata(
                n2,
                URI.create("http://127.0.0.2:8080"),
                duplicateEndpoint,
                "rack-b"
        );
        var partition = partition(List.of(
                new ReplicaMember(n1, node1.ratisEndpoint(), ReplicaRole.VOTER, 0, 0, true),
                new ReplicaMember(n2, node2.ratisEndpoint(), ReplicaRole.VOTER, 0, 0, true)
        ), n1);
        return metadata(Map.of(node1.nodeId(), node1, node2.nodeId(), node2), partition);
    }

    private static PartitionDescriptor partition(List<ReplicaMember> peers, NodeId leaderHint) {
        return new PartitionDescriptor(
                new ShardId(7),
                null,
                null,
                1,
                UUID.randomUUID().toString(),
                peers,
                leaderHint,
                Map.of(),
                PartitionSplitState.ACTIVE
        );
    }

    private static ClusterMetadata metadata(Map<NodeId, ClusterNodeMetadata> nodes,
                                            PartitionDescriptor partition) {
        return new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                1,
                Map.of(),
                Map.of(),
                Map.of(partition.partitionId(), partition),
                nodes
        );
    }
}
