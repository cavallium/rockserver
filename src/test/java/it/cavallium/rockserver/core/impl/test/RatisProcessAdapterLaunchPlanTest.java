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
import it.cavallium.rockserver.core.replication.ratis.RatisAdapterBoundaryMode;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterConfig;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterCommand;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterDistribution;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterLaunchPlan;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterMetadataConfig;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessLauncher;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RatisProcessAdapterLaunchPlanTest {

    @Test
    void launchPlanBuildsCommandWritesConfigAndExposesBoundaryHelpers(@TempDir Path tempDir) throws Exception {
        var nodeId = NodeId.of("n1");
        var groupId = UUID.randomUUID().toString();
        var configPath = tempDir.resolve("conf").resolve("ratis-adapter.properties");
        var metadataPath = configPath.resolveSibling(configPath.getFileName() + ".metadata");
        var distribution = distribution();
        var plan = RatisProcessAdapterLaunchPlan.fromMetadata(
                distribution,
                metadata(nodeId, groupId),
                nodeId,
                new RatisProcessAdapterLaunchPlan.Options(
                        tempDir.resolve("work"),
                        tempDir.resolve("adapter-data"),
                        configPath,
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082),
                        List.of("-Drockserver.log.level=debug"),
                        Map.of("EXTRA_ENV", "extra")
                )
        );

        assertEquals(List.of(
                "/usr/lib/jvm/default/bin/java",
                "-Xmx512m",
                "-Drockserver.log.level=debug",
                "-cp",
                "/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter.jar"
                        + File.pathSeparator
                        + "/opt/rockserver-ratis-adapter/lib/ratis-server.jar",
                "it.cavallium.rockserver.ratis.AdapterMain",
                "--config=" + configPath,
                "--metadata=" + metadataPath,
                "--health-host=127.0.0.1",
                "--health-port=19081",
                "--rpc-host=127.0.0.1",
                "--rpc-port=19082"
        ), plan.command().toCommandLine());
        assertEquals("base", plan.command().environment().get("BASE_ENV"));
        assertEquals("extra", plan.command().environment().get("EXTRA_ENV"));
        assertEquals("n1", plan.command().environment().get(RatisProcessAdapterLaunchPlan.NODE_ID_ENV));
        assertEquals(tempDir.resolve("adapter-data").toString(),
                plan.command().environment().get(RatisProcessAdapterLaunchPlan.ADAPTER_DATA_ROOT_ENV));
        assertEquals(plan.adapterConfig().metadataGroupTopologySha256(),
                plan.command().environment().get(RatisProcessAdapterLaunchPlan.METADATA_GROUP_TOPOLOGY_SHA256_ENV));
        assertEquals(configPath.toString(),
                plan.command().environment().get(RatisProcessAdapterLaunchPlan.CONFIG_PATH_ENV));
        assertEquals(metadataPath.toString(),
                plan.command().environment().get(RatisProcessAdapterLaunchPlan.METADATA_PATH_ENV));

        plan.writeConfig();

        var properties = new Properties();
        try (var reader = Files.newBufferedReader(configPath)) {
            properties.load(reader);
        }
        assertEquals("1", properties.getProperty("metadataEpoch"));
        assertEquals(plan.metadataConfig().semanticSha256(), properties.getProperty("metadataSha256"));
        assertEquals("7", properties.getProperty("partition.0.partitionId"));
        assertEquals(groupId, properties.getProperty("partition.0.raftGroupId"));
        assertEquals("n1", properties.getProperty("partition.0.localPeerId"));
        assertEquals(tempDir.resolve("adapter-data").resolve("partition-7").toString(),
                properties.getProperty("partition.0.dataPath"));
        assertTrue(Files.exists(metadataPath));
        assertTrue(Files.isRegularFile(configPath.resolveSibling(configPath.getFileName() + ".lock")));

        try (var lifecycle = plan.lifecycle("ratis-process-test", Duration.ofSeconds(1), Duration.ofSeconds(1))) {
            assertEquals(RatisAdapterBoundaryMode.EXTERNAL_PROCESS, lifecycle.boundary().mode());
            assertTrue(lifecycle.boundary().keepsRatisOffProductionModulePath());
        }
        try (var adapterFactory = plan.adapterFactory("ratis-process-test", Duration.ofSeconds(1))) {
            assertEquals(RatisAdapterBoundaryMode.EXTERNAL_PROCESS, adapterFactory.boundary().mode());
        }
    }

    @Test
    void lifecycleWritesConfigBeforeStartingProcess(@TempDir Path tempDir) throws Exception {
        var nodeId = NodeId.of("n1");
        var groupId = UUID.randomUUID().toString();
        var configPath = tempDir.resolve("conf").resolve("ratis-adapter.properties");
        var metadataPath = configPath.resolveSibling(configPath.getFileName() + ".metadata");
        var plan = RatisProcessAdapterLaunchPlan.fromMetadata(
                distribution(tempDir),
                metadata(nodeId, groupId),
                nodeId,
                new RatisProcessAdapterLaunchPlan.Options(
                        tempDir.resolve("work"),
                        tempDir.resolve("adapter-data"),
                        configPath,
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082),
                        List.of(),
                        Map.of()
                )
        );
        var process = new FakeProcess();
        try (var lifecycle = plan.lifecycle(
                "ratis-process-test",
                Duration.ofSeconds(1),
                launchedCommand -> {
                    assertEquals(plan.command(), launchedCommand);
                    assertTrue(Files.exists(configPath));
                    return process;
                },
                _ -> it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterHealth.ready()
        )) {
            lifecycle.start();

            assertTrue(process.isAlive());
            var properties = new Properties();
            try (var reader = Files.newBufferedReader(configPath)) {
                properties.load(reader);
            }
            assertEquals("7", properties.getProperty("partition.0.partitionId"));
            assertTrue(Files.exists(metadataPath));
        }

        assertTrue(process.destroyCalled);
    }

    @Test
    void writeConfigStagesMetadataBeforeConfigActivation(@TempDir Path tempDir) throws Exception {
        var nodeId = NodeId.of("n1");
        var configPath = tempDir.resolve("config-target");
        var metadataPath = tempDir.resolve("published-metadata.properties");
        Files.createDirectory(configPath);
        var plan = RatisProcessAdapterLaunchPlan.fromMetadata(
                distribution(),
                metadata(nodeId, UUID.randomUUID().toString()),
                nodeId,
                new RatisProcessAdapterLaunchPlan.Options(
                        tempDir.resolve("work"),
                        tempDir.resolve("adapter-data"),
                        configPath,
                        metadataPath,
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082),
                        List.of(),
                        Map.of()
                )
        );

        assertThrows(IOException.class, plan::writeConfig);

        assertEquals(plan.metadataConfig().metadata(),
                RatisProcessAdapterMetadataConfig.read(metadataPath).metadata());
        assertTrue(Files.isDirectory(configPath));
    }

    @Test
    void writeConfigRejectsSymbolicLinkPairLockBeforePublishingMetadata(@TempDir Path tempDir) throws Exception {
        var nodeId = NodeId.of("n1");
        var configPath = tempDir.resolve("conf").resolve("ratis-adapter.properties");
        var metadataPath = configPath.resolveSibling(configPath.getFileName() + ".metadata");
        Files.createDirectories(configPath.getParent());
        var redirectedLock = tempDir.resolve("redirected.lock");
        Files.writeString(redirectedLock, "");
        Files.createSymbolicLink(
                configPath.resolveSibling(configPath.getFileName() + ".lock"),
                redirectedLock
        );
        var plan = RatisProcessAdapterLaunchPlan.fromMetadata(
                distribution(),
                metadata(nodeId, UUID.randomUUID().toString()),
                nodeId,
                new RatisProcessAdapterLaunchPlan.Options(
                        tempDir.resolve("work"),
                        tempDir.resolve("adapter-data"),
                        configPath,
                        metadataPath,
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082),
                        List.of(),
                        Map.of()
                )
        );

        assertThrows(IOException.class, plan::writeConfig);

        assertFalse(Files.exists(configPath));
        assertFalse(Files.exists(metadataPath));
    }

    @Test
    void launchPlanRejectsMetadataPathThatCollidesWithConfigPairLock(@TempDir Path tempDir) {
        var configPath = tempDir.resolve("ratis-adapter.properties");

        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterLaunchPlan.fromMetadata(
                distribution(),
                metadata(NodeId.of("n1"), UUID.randomUUID().toString()),
                NodeId.of("n1"),
                new RatisProcessAdapterLaunchPlan.Options(
                        tempDir.resolve("work"),
                        tempDir.resolve("adapter-data"),
                        configPath,
                        configPath.resolveSibling(configPath.getFileName() + ".lock"),
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082),
                        List.of(),
                        Map.of()
                )
        ));
    }

    @Test
    void launchPlanRejectsNullInputsWithExplicitDiagnostics(@TempDir Path tempDir) {
        var nodeId = NodeId.of("n1");
        var metadata = metadata(nodeId, UUID.randomUUID().toString());
        var options = options(tempDir);
        var adapterConfig = RatisProcessAdapterConfig.fromMetadata(
                metadata,
                nodeId,
                tempDir.resolve("adapter-data")
        );
        var metadataConfig = new RatisProcessAdapterMetadataConfig(metadata);
        var command = distribution().toCommand(
                options.workingDirectory(),
                List.of(),
                List.of("--print-health"),
                Map.of()
        );
        var plan = new RatisProcessAdapterLaunchPlan(
                adapterConfig,
                metadataConfig,
                Path.of("/opt/rockserver-ratis-adapter"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("adapter.properties"),
                tempDir.resolve("adapter.properties.metadata"),
                command,
                new InetSocketAddress("127.0.0.1", 19081),
                new InetSocketAddress("127.0.0.1", 19082)
        );
        var nullEnvironmentValue = new java.util.LinkedHashMap<String, String>();
        nullEnvironmentValue.put("SAFE", null);

        var nullAdapterConfig = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterLaunchPlan(
                        null,
                        metadataConfig,
                        Path.of("/opt/rockserver-ratis-adapter"),
                        tempDir.resolve("adapter-data"),
                        tempDir.resolve("adapter.properties"),
                        tempDir.resolve("adapter.properties.metadata"),
                        command,
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082)
                ));
        var nullMetadataConfig = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterLaunchPlan(
                        adapterConfig,
                        null,
                        Path.of("/opt/rockserver-ratis-adapter"),
                        tempDir.resolve("adapter-data"),
                        tempDir.resolve("adapter.properties"),
                        tempDir.resolve("adapter.properties.metadata"),
                        command,
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082)
                ));
        var nullCommand = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterLaunchPlan(
                        adapterConfig,
                        metadataConfig,
                        Path.of("/opt/rockserver-ratis-adapter"),
                        tempDir.resolve("adapter-data"),
                        tempDir.resolve("adapter.properties"),
                        tempDir.resolve("adapter.properties.metadata"),
                        null,
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082)
                ));
        var nullDistribution = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterLaunchPlan.fromMetadata(null, metadata, nodeId, options));
        var nullLocalNode = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterLaunchPlan.fromMetadata(distribution(), metadata, null, options));
        var nullOptions = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterLaunchPlan.fromMetadata(distribution(), metadata, nodeId, null));
        var nullJvmArguments = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterLaunchPlan.Options(
                        tempDir.resolve("work"),
                        tempDir.resolve("adapter-data"),
                        tempDir.resolve("adapter.properties"),
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082),
                        null,
                        Map.of()
                ));
        var nullJvmArgument = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterLaunchPlan.Options(
                        tempDir.resolve("work"),
                        tempDir.resolve("adapter-data"),
                        tempDir.resolve("adapter.properties"),
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082),
                        Collections.singletonList(null),
                        Map.of()
                ));
        var nullEnvironmentOverrides = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterLaunchPlan.Options(
                        tempDir.resolve("work"),
                        tempDir.resolve("adapter-data"),
                        tempDir.resolve("adapter.properties"),
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082),
                        List.of(),
                        null
                ));
        var nullEnvironmentValueError = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterLaunchPlan.Options(
                        tempDir.resolve("work"),
                        tempDir.resolve("adapter-data"),
                        tempDir.resolve("adapter.properties"),
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082),
                        List.of(),
                        nullEnvironmentValue
                ));
        var nullAdmission = assertThrows(IllegalArgumentException.class,
                () -> plan.writeConfig(null));
        var nullActivation = assertThrows(IllegalArgumentException.class,
                () -> plan.writeConfig(_ -> {
                }, null));

        assertEquals("adapterConfig must not be null", nullAdapterConfig.getMessage());
        assertEquals("metadataConfig must not be null", nullMetadataConfig.getMessage());
        assertEquals("command must not be null", nullCommand.getMessage());
        assertEquals("distribution must not be null", nullDistribution.getMessage());
        assertEquals("localNodeId must not be null", nullLocalNode.getMessage());
        assertEquals("options must not be null", nullOptions.getMessage());
        assertEquals("jvmArguments must not be null", nullJvmArguments.getMessage());
        assertEquals("jvmArguments entry must not be null", nullJvmArgument.getMessage());
        assertEquals("environmentOverrides must not be null", nullEnvironmentOverrides.getMessage());
        assertEquals("environmentOverrides value must not be null", nullEnvironmentValueError.getMessage());
        assertEquals("admission must not be null", nullAdmission.getMessage());
        assertEquals("activation must not be null", nullActivation.getMessage());
    }

    @Test
    void writeConfigRejectsLowerEpochAndDivergentSameEpochPublication(@TempDir Path tempDir) throws Exception {
        var nodeId = NodeId.of("n1");
        var configPath = tempDir.resolve("conf").resolve("ratis-adapter.properties");
        var metadataPath = configPath.resolveSibling(configPath.getFileName() + ".metadata");
        var published = launchPlan(
                tempDir,
                configPath,
                metadataPath,
                metadata(nodeId, UUID.randomUUID().toString(), 2)
        );
        var stale = launchPlan(
                tempDir,
                configPath,
                metadataPath,
                metadata(nodeId, UUID.randomUUID().toString(), 1)
        );
        var divergent = launchPlan(
                tempDir,
                configPath,
                metadataPath,
                metadata(nodeId, UUID.randomUUID().toString(), 2)
        );
        published.writeConfig();

        assertThrows(IllegalArgumentException.class, stale::writeConfig);
        assertThrows(IllegalArgumentException.class, divergent::writeConfig);

        assertEquals(published.metadataConfig().metadata(),
                RatisProcessAdapterMetadataConfig.read(metadataPath).metadata());
    }

    @Test
    void writeConfigRunsAdmissionAfterCrossProcessPairLockAcquisition(@TempDir Path tempDir) throws Exception {
        var nodeId = NodeId.of("n1");
        var configPath = tempDir.resolve("conf").resolve("ratis-adapter.properties");
        var metadataPath = configPath.resolveSibling(configPath.getFileName() + ".metadata");
        var lockPath = configPath.resolveSibling(configPath.getFileName() + ".lock");
        var plan = launchPlan(
                tempDir,
                configPath,
                metadataPath,
                metadata(nodeId, UUID.randomUUID().toString())
        );
        Files.createDirectories(lockPath.getParent());
        var lockHolder = startConfigPairLockHolder(lockPath);
        var executor = Executors.newSingleThreadExecutor();
        try (var lockHolderOutput = lockHolder.inputReader()) {
            assertEquals("locked", lockHolderOutput.readLine());
            var admissionCalled = new AtomicBoolean();
            var write = executor.submit(() -> {
                plan.writeConfig(_ -> admissionCalled.set(true));
                return null;
            });

            assertThrows(TimeoutException.class, () -> write.get(100, TimeUnit.MILLISECONDS));
            assertFalse(admissionCalled.get());
            assertFalse(Files.exists(metadataPath));

            lockHolder.getOutputStream().close();
            assertTrue(lockHolder.waitFor(5, TimeUnit.SECONDS));
            write.get(5, TimeUnit.SECONDS);

            assertTrue(admissionCalled.get());
            assertTrue(Files.isRegularFile(metadataPath));
        } finally {
            lockHolder.destroyForcibly();
            lockHolder.waitFor(5, TimeUnit.SECONDS);
            executor.shutdownNow();
        }
    }

    @Test
    void writeConfigRetainsCrossProcessPairLockThroughActivation(@TempDir Path tempDir) throws Exception {
        var nodeId = NodeId.of("n1");
        var configPath = tempDir.resolve("conf").resolve("ratis-adapter.properties");
        var metadataPath = configPath.resolveSibling(configPath.getFileName() + ".metadata");
        var lockPath = configPath.resolveSibling(configPath.getFileName() + ".lock");
        var plan = launchPlan(
                tempDir,
                configPath,
                metadataPath,
                metadata(nodeId, UUID.randomUUID().toString())
        );
        var activationStarted = new CountDownLatch(1);
        var releaseActivation = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            Process lockHolder = null;
            try {
                var write = executor.submit(() -> {
                    plan.writeConfig(_ -> {
                    }, () -> {
                        activationStarted.countDown();
                        awaitConfigActivationRelease(releaseActivation);
                    });
                    return null;
                });
                assertTrue(activationStarted.await(5, TimeUnit.SECONDS));
                assertTrue(Files.isRegularFile(configPath));
                assertTrue(Files.isRegularFile(metadataPath));

                lockHolder = startConfigPairLockHolder(lockPath);
                try (var lockHolderOutput = lockHolder.inputReader()) {
                    var lockAcquired = executor.submit(lockHolderOutput::readLine);

                    assertThrows(TimeoutException.class, () -> lockAcquired.get(100, TimeUnit.MILLISECONDS));

                    releaseActivation.countDown();
                    write.get(5, TimeUnit.SECONDS);
                    assertEquals("locked", lockAcquired.get(5, TimeUnit.SECONDS));
                    lockHolder.getOutputStream().close();
                    assertTrue(lockHolder.waitFor(5, TimeUnit.SECONDS));
                }
            } finally {
                releaseActivation.countDown();
                if (lockHolder != null) {
                    lockHolder.destroyForcibly();
                    lockHolder.waitFor(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    void writeConfigSerializesSamePairInsideJvmThroughActivation(@TempDir Path tempDir) throws Exception {
        var nodeId = NodeId.of("n1");
        var configPath = tempDir.resolve("conf").resolve("ratis-adapter.properties");
        var metadataPath = configPath.resolveSibling(configPath.getFileName() + ".metadata");
        var plan = launchPlan(
                tempDir,
                configPath,
                metadataPath,
                metadata(nodeId, UUID.randomUUID().toString())
        );
        var activationStarted = new CountDownLatch(1);
        var releaseActivation = new CountDownLatch(1);
        var secondAdmissionCalled = new AtomicBoolean();
        try (var executor = Executors.newFixedThreadPool(2)) {
            var firstWrite = executor.submit(() -> {
                plan.writeConfig(_ -> {
                }, () -> {
                    activationStarted.countDown();
                    awaitConfigActivationRelease(releaseActivation);
                });
                return null;
            });
            assertTrue(activationStarted.await(5, TimeUnit.SECONDS));

            var secondWrite = executor.submit(() -> {
                plan.writeConfig(_ -> secondAdmissionCalled.set(true));
                return null;
            });
            assertThrows(TimeoutException.class, () -> secondWrite.get(100, TimeUnit.MILLISECONDS));
            assertFalse(secondAdmissionCalled.get());

            releaseActivation.countDown();
            firstWrite.get(5, TimeUnit.SECONDS);
            secondWrite.get(5, TimeUnit.SECONDS);
            assertTrue(secondAdmissionCalled.get());
        } finally {
            releaseActivation.countDown();
        }
    }

    @Test
    void writeConfigAllowsIndependentPairsToActivateConcurrently(@TempDir Path tempDir) throws Exception {
        var nodeId = NodeId.of("n1");
        var firstConfigPath = tempDir.resolve("first").resolve("ratis-adapter.properties");
        var secondConfigPath = tempDir.resolve("second").resolve("ratis-adapter.properties");
        var firstPlan = launchPlan(
                tempDir.resolve("first-plan"),
                firstConfigPath,
                firstConfigPath.resolveSibling(firstConfigPath.getFileName() + ".metadata"),
                metadata(nodeId, UUID.randomUUID().toString())
        );
        var secondPlan = launchPlan(
                tempDir.resolve("second-plan"),
                secondConfigPath,
                secondConfigPath.resolveSibling(secondConfigPath.getFileName() + ".metadata"),
                metadata(nodeId, UUID.randomUUID().toString())
        );
        var firstActivationStarted = new CountDownLatch(1);
        var releaseFirstActivation = new CountDownLatch(1);
        var secondActivationCalled = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var firstWrite = executor.submit(() -> {
                firstPlan.writeConfig(_ -> {
                }, () -> {
                    firstActivationStarted.countDown();
                    awaitConfigActivationRelease(releaseFirstActivation);
                });
                return null;
            });
            assertTrue(firstActivationStarted.await(5, TimeUnit.SECONDS));

            var secondWrite = executor.submit(() -> {
                secondPlan.writeConfig(_ -> {
                }, secondActivationCalled::countDown);
                return null;
            });
            assertTrue(secondActivationCalled.await(5, TimeUnit.SECONDS));
            secondWrite.get(5, TimeUnit.SECONDS);

            releaseFirstActivation.countDown();
            firstWrite.get(5, TimeUnit.SECONDS);
        } finally {
            releaseFirstActivation.countDown();
        }
    }

    @Test
    void lifecycleRejectsFailedPreflightBeforeWritingConfigOrStartingProcess(@TempDir Path tempDir) throws Exception {
        var nodeId = NodeId.of("n1");
        var configPath = tempDir.resolve("conf").resolve("ratis-adapter.properties");
        var plan = RatisProcessAdapterLaunchPlan.fromMetadata(
                distribution(),
                metadata(nodeId, UUID.randomUUID().toString()),
                nodeId,
                new RatisProcessAdapterLaunchPlan.Options(
                        tempDir.resolve("work"),
                        tempDir.resolve("adapter-data"),
                        configPath,
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082),
                        List.of(),
                        Map.of()
                )
        );
        var launcherCalled = new boolean[] {false};
        try (var lifecycle = plan.lifecycle(
                "ratis-process-test",
                Duration.ofSeconds(1),
                _ -> {
                    launcherCalled[0] = true;
                    return new FakeProcess();
                },
                _ -> it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterHealth.ready()
        )) {
            var error = assertThrows(IOException.class, lifecycle::start);

            assertTrue(error.getMessage().contains("Ratis adapter deployment preflight failed"));
            assertFalse(launcherCalled[0]);
            assertFalse(Files.exists(configPath));
        }
    }

    @Test
    void launchOptionsRejectUnconnectablePorts(@TempDir Path tempDir) {
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterLaunchPlan.Options(
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("adapter.properties"),
                new InetSocketAddress("127.0.0.1", 0),
                new InetSocketAddress("127.0.0.1", 19082),
                List.of(),
                Map.of()
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterLaunchPlan.Options(
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19081),
                new InetSocketAddress("127.0.0.1", 0),
                List.of(),
                Map.of()
        ));
    }

    @Test
    void distributionRejectsNullClasspathAndArgumentInputsWithExplicitDiagnostics() {
        var nullClasspath = assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterDistribution(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                null,
                List.of("-Xmx512m"),
                Map.of()
        ));
        var nullClasspathEntry = assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterDistribution(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                Collections.singletonList(null),
                List.of("-Xmx512m"),
                Map.of()
        ));
        var nullDefaultArguments = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterDistribution(
                        Path.of("/opt/rockserver-ratis-adapter"),
                        Path.of("/usr/lib/jvm/default/bin/java"),
                        "it.cavallium.rockserver.ratis.AdapterMain",
                        List.of(Path.of("adapter/rockserver-ratis-adapter.jar")),
                        null,
                        Map.of()
                ));
        var nullDefaultArgumentEntry = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterDistribution(
                        Path.of("/opt/rockserver-ratis-adapter"),
                        Path.of("/usr/lib/jvm/default/bin/java"),
                        "it.cavallium.rockserver.ratis.AdapterMain",
                        List.of(Path.of("adapter/rockserver-ratis-adapter.jar")),
                        Collections.singletonList(null),
                        Map.of()
                ));
        var distribution = distribution();
        var nullJvmArguments = assertThrows(IllegalArgumentException.class, () -> distribution.toCommand(
                Path.of("/var/lib/rockserver/adapter-work"),
                null,
                List.of("--print-health"),
                Map.of()
        ));
        var nullJvmArgumentEntry = assertThrows(IllegalArgumentException.class, () -> distribution.toCommand(
                Path.of("/var/lib/rockserver/adapter-work"),
                Collections.singletonList(null),
                List.of("--print-health"),
                Map.of()
        ));
        var nullProgramArguments = assertThrows(IllegalArgumentException.class, () -> distribution.toCommand(
                Path.of("/var/lib/rockserver/adapter-work"),
                List.of(),
                null,
                Map.of()
        ));
        var nullProgramArgumentEntry = assertThrows(IllegalArgumentException.class, () -> distribution.toCommand(
                Path.of("/var/lib/rockserver/adapter-work"),
                List.of(),
                Collections.singletonList(null),
                Map.of()
        ));

        assertEquals("runtimeClasspathEntries must not be null", nullClasspath.getMessage());
        assertEquals("runtimeClasspathEntries entry must not be null", nullClasspathEntry.getMessage());
        assertEquals("defaultJvmArguments must not be null", nullDefaultArguments.getMessage());
        assertEquals("defaultJvmArguments entry must not be null", nullDefaultArgumentEntry.getMessage());
        assertEquals("jvmArguments must not be null", nullJvmArguments.getMessage());
        assertEquals("jvmArguments entry must not be null", nullJvmArgumentEntry.getMessage());
        assertEquals("programArguments must not be null", nullProgramArguments.getMessage());
        assertEquals("programArguments entry must not be null", nullProgramArgumentEntry.getMessage());
    }

    @Test
    void distributionRejectsDuplicateRuntimeClasspathEntries() {
        var error = assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterDistribution(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of(
                        Path.of("lib/ratis-server.jar"),
                        Path.of("lib/../lib/ratis-server.jar")
                ),
                List.of("-Xmx512m"),
                Map.of()
        ));

        assertTrue(error.getMessage().contains("runtimeClasspathEntries must not contain duplicate paths"));
    }

    @Test
    void distributionRejectsRuntimeClasspathEntriesThatDoNotNameJars() {
        for (var classpathEntry : List.of(
                Path.of("."),
                Path.of("lib/.."),
                Path.of("lib"),
                Path.of("lib/ratis-server.zip")
        )) {
            var error = assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterDistribution(
                    Path.of("/opt/rockserver-ratis-adapter"),
                    Path.of("/usr/lib/jvm/default/bin/java"),
                    "it.cavallium.rockserver.ratis.AdapterMain",
                    List.of(classpathEntry),
                    List.of("-Xmx512m"),
                    Map.of()
            ), classpathEntry.toString());

            assertTrue(error.getMessage().contains("relative JAR paths"), classpathEntry.toString());
        }
    }

    @Test
    void commandRejectsNullClasspathAndArgumentInputsWithExplicitDiagnostics() {
        var nullClasspath = assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java"),
                Path.of("/var/lib/rockserver/adapter-work"),
                null,
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of("-Xmx512m"),
                List.of("--print-health"),
                Map.of()
        ));
        var nullClasspathEntry = assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java"),
                Path.of("/var/lib/rockserver/adapter-work"),
                Collections.singletonList(null),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of("-Xmx512m"),
                List.of("--print-health"),
                Map.of()
        ));
        var nullJvmArguments = assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java"),
                Path.of("/var/lib/rockserver/adapter-work"),
                List.of(Path.of("/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter.jar")),
                "it.cavallium.rockserver.ratis.AdapterMain",
                null,
                List.of("--print-health"),
                Map.of()
        ));
        var nullJvmArgumentEntry = assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java"),
                Path.of("/var/lib/rockserver/adapter-work"),
                List.of(Path.of("/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter.jar")),
                "it.cavallium.rockserver.ratis.AdapterMain",
                Collections.singletonList(null),
                List.of("--print-health"),
                Map.of()
        ));
        var nullProgramArguments = assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java"),
                Path.of("/var/lib/rockserver/adapter-work"),
                List.of(Path.of("/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter.jar")),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of("-Xmx512m"),
                null,
                Map.of()
        ));
        var nullProgramArgumentEntry = assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java"),
                Path.of("/var/lib/rockserver/adapter-work"),
                List.of(Path.of("/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter.jar")),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of("-Xmx512m"),
                Collections.singletonList(null),
                Map.of()
        ));

        assertEquals("classpathEntries must not be null", nullClasspath.getMessage());
        assertEquals("classpathEntries entry must not be null", nullClasspathEntry.getMessage());
        assertEquals("jvmArguments must not be null", nullJvmArguments.getMessage());
        assertEquals("jvmArguments entry must not be null", nullJvmArgumentEntry.getMessage());
        assertEquals("programArguments must not be null", nullProgramArguments.getMessage());
        assertEquals("programArguments entry must not be null", nullProgramArgumentEntry.getMessage());
    }

    @Test
    void commandRejectsDuplicateClasspathEntries() {
        var error = assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java"),
                Path.of("/var/lib/rockserver/adapter-work"),
                List.of(
                        Path.of("/opt/rockserver-ratis-adapter/lib/ratis-server.jar"),
                        Path.of("/opt/rockserver-ratis-adapter/lib/../lib/ratis-server.jar")
                ),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of("-Xmx512m"),
                List.of("--print-health"),
                Map.of()
        ));

        assertTrue(error.getMessage().contains("classpathEntries must not contain duplicate paths"));
    }

    @Test
    void commandRejectsClasspathEntriesThatDoNotNameJars() {
        for (var classpathEntry : List.of(
                Path.of("/"),
                Path.of("/opt/rockserver-ratis-adapter/lib"),
                Path.of("/opt/rockserver-ratis-adapter/lib/ratis-server.zip")
        )) {
            var error = assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterCommand(
                    Path.of("/usr/lib/jvm/default/bin/java"),
                    Path.of("/var/lib/rockserver/adapter-work"),
                    List.of(classpathEntry),
                    "it.cavallium.rockserver.ratis.AdapterMain",
                    List.of("-Xmx512m"),
                    List.of("--print-health"),
                    Map.of()
            ), classpathEntry.toString());

            assertTrue(error.getMessage().contains("classpathEntries must name JAR files"), classpathEntry.toString());
        }
    }

    @Test
    void commandAndDistributionRejectLineUnsafeArguments() {
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java"),
                Path.of("/var/lib/rockserver/adapter-work"),
                List.of(Path.of("/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter.jar")),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of("-Dunsafe=bad\nvalue"),
                List.of("--print-health"),
                Map.of()
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java"),
                Path.of("/var/lib/rockserver/adapter-work"),
                List.of(Path.of("/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter.jar")),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of("-Xmx512m"),
                List.of("--config=/tmp/config\r--metadata=/tmp/metadata"),
                Map.of()
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterDistribution(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of(Path.of("adapter/rockserver-ratis-adapter.jar")),
                List.of("-Dunsafe=bad\nvalue"),
                Map.of()
        ));

        var distribution = new RatisProcessAdapterDistribution(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of(Path.of("adapter/rockserver-ratis-adapter.jar")),
                List.of("-Xmx512m"),
                Map.of()
        );
        assertThrows(IllegalArgumentException.class, () -> distribution.toCommand(
                Path.of("/var/lib/rockserver/adapter-work"),
                List.of("-Dunsafe=bad\rvalue"),
                List.of("--print-health"),
                Map.of()
        ));
        assertThrows(IllegalArgumentException.class, () -> distribution.toCommand(
                Path.of("/var/lib/rockserver/adapter-work"),
                List.of(),
                List.of("--config=/tmp/config\n--metadata=/tmp/metadata"),
                Map.of()
        ));
    }

    @Test
    void commandDistributionAndLaunchOptionsRejectLineUnsafePaths(@TempDir Path tempDir) {
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java\n"),
                Path.of("/var/lib/rockserver/adapter-work"),
                List.of(Path.of("/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter.jar")),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of("-Xmx512m"),
                List.of("--print-health"),
                Map.of()
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java"),
                Path.of("/var/lib/rockserver/adapter-work\r"),
                List.of(Path.of("/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter.jar")),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of("-Xmx512m"),
                List.of("--print-health"),
                Map.of()
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java"),
                Path.of("/var/lib/rockserver/adapter-work"),
                List.of(Path.of("/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter\n.jar")),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of("-Xmx512m"),
                List.of("--print-health"),
                Map.of()
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterDistribution(
                Path.of("/opt/rockserver-ratis-adapter\n"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of(Path.of("adapter/rockserver-ratis-adapter.jar")),
                List.of("-Xmx512m"),
                Map.of()
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterDistribution(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of(Path.of("adapter/rockserver-ratis-adapter\r.jar")),
                List.of("-Xmx512m"),
                Map.of()
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterLaunchPlan.Options(
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                Path.of("adapter\n.properties"),
                new InetSocketAddress("127.0.0.1", 19081),
                new InetSocketAddress("127.0.0.1", 19082),
                List.of(),
                Map.of()
        ));
    }

    @Test
    void launchPlanRejectsJvmArgumentsThatOverrideAdapterLaunchTarget(@TempDir Path tempDir) {
        var nodeId = NodeId.of("n1");
        var defaultArgumentError = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterDistribution(
                        Path.of("/opt/rockserver-ratis-adapter"),
                        Path.of("/usr/lib/jvm/default/bin/java"),
                        "it.cavallium.rockserver.ratis.AdapterMain",
                        List.of(Path.of("adapter/rockserver-ratis-adapter.jar")),
                        List.of("-jar", "unexpected.jar"),
                        Map.of()
                ));
        assertTrue(defaultArgumentError.getMessage().contains("launch target"));

        var overrideArgumentError = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterLaunchPlan.fromMetadata(
                        distribution(),
                        metadata(nodeId, UUID.randomUUID().toString()),
                        nodeId,
                        new RatisProcessAdapterLaunchPlan.Options(
                                tempDir.resolve("work"),
                                tempDir.resolve("adapter-data"),
                                tempDir.resolve("adapter.properties"),
                                new InetSocketAddress("127.0.0.1", 19081),
                                new InetSocketAddress("127.0.0.1", 19082),
                                List.of("-jar", "unexpected.jar"),
                                Map.of()
                        )
                ));
        assertTrue(overrideArgumentError.getMessage().contains("launch target"));
    }

    @Test
    void launchPlanRejectsJvmArgumentsThatAlterAdapterCodeLoading(@TempDir Path tempDir) {
        var nodeId = NodeId.of("n1");
        for (var jvmArgument : List.of(
                "-javaagent:/tmp/agent.jar",
                "-agentlib:jdwp=transport=dt_socket",
                "-agentpath:/tmp/agent.so",
                "-Xbootclasspath/a:/tmp/injected.jar",
                "--patch-module=java.base=/tmp/patches",
                "--upgrade-module-path=/tmp/modules"
        )) {
            var error = assertThrows(IllegalArgumentException.class,
                    () -> RatisProcessAdapterLaunchPlan.fromMetadata(
                            distribution(),
                            metadata(nodeId, UUID.randomUUID().toString()),
                            nodeId,
                            new RatisProcessAdapterLaunchPlan.Options(
                                    tempDir.resolve("work"),
                                    tempDir.resolve("adapter-data"),
                                    tempDir.resolve("adapter.properties"),
                                    new InetSocketAddress("127.0.0.1", 19081),
                                    new InetSocketAddress("127.0.0.1", 19082),
                                    List.of(jvmArgument),
                                    Map.of()
                            )
                    ), jvmArgument);
            assertTrue(error.getMessage().contains("instrumentation boundary"), jvmArgument);
        }
    }

    @Test
    void launchPlanRejectsJvmArgumentsThatExitBeforeAdapterMain(@TempDir Path tempDir) {
        var nodeId = NodeId.of("n1");
        for (var jvmArgument : List.of(
                "--dry-run",
                "--validate-modules",
                "--list-modules",
                "--describe-module=java.base",
                "--version",
                "-version",
                "--help",
                "-X"
        )) {
            var error = assertThrows(IllegalArgumentException.class,
                    () -> RatisProcessAdapterLaunchPlan.fromMetadata(
                            distribution(),
                            metadata(nodeId, UUID.randomUUID().toString()),
                            nodeId,
                            new RatisProcessAdapterLaunchPlan.Options(
                                    tempDir.resolve("work"),
                                    tempDir.resolve("adapter-data"),
                                    tempDir.resolve("adapter.properties"),
                                    new InetSocketAddress("127.0.0.1", 19081),
                                    new InetSocketAddress("127.0.0.1", 19082),
                                    List.of(jvmArgument),
                                    Map.of()
                            )
                    ), jvmArgument);
            assertTrue(error.getMessage().contains("inspection"), jvmArgument);
        }
    }

    @Test
    void launchPlanRejectsReservedEnvironmentOverrides(@TempDir Path tempDir) {
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterLaunchPlan.Options(
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19081),
                new InetSocketAddress("127.0.0.1", 19082),
                List.of(),
                Map.of(RatisProcessAdapterLaunchPlan.NODE_ID_ENV, "wrong-node")
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterLaunchPlan.Options(
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19081),
                new InetSocketAddress("127.0.0.1", 19082),
                List.of(),
                Map.of(RatisProcessAdapterLaunchPlan.ADAPTER_DATA_ROOT_ENV, "/wrong/data")
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterLaunchPlan.Options(
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19081),
                new InetSocketAddress("127.0.0.1", 19082),
                List.of(),
                Map.of(RatisProcessAdapterLaunchPlan.METADATA_GROUP_TOPOLOGY_SHA256_ENV, "0".repeat(64))
        ));

        var nodeId = NodeId.of("n1");
        var distribution = new RatisProcessAdapterDistribution(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of(Path.of("adapter/rockserver-ratis-adapter.jar")),
                List.of("-Xmx512m"),
                Map.of(RatisProcessAdapterLaunchPlan.CONFIG_PATH_ENV, "/wrong/config")
        );
        assertThrows(IllegalArgumentException.class, () -> RatisProcessAdapterLaunchPlan.fromMetadata(
                distribution,
                metadata(nodeId, UUID.randomUUID().toString()),
                nodeId,
                new RatisProcessAdapterLaunchPlan.Options(
                        tempDir.resolve("work"),
                        tempDir.resolve("adapter-data"),
                        tempDir.resolve("adapter.properties"),
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082),
                        List.of(),
                        Map.of()
                )
        ));
    }

    @Test
    void launchPlanRejectsUnsafeEnvironmentEntries(@TempDir Path tempDir) {
        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterLaunchPlan.Options(
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19081),
                new InetSocketAddress("127.0.0.1", 19082),
                List.of(),
                Map.of("BAD\nKEY", "value")
        ));

        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterLaunchPlan.Options(
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19081),
                new InetSocketAddress("127.0.0.1", 19082),
                List.of(),
                Map.of("BAD\0KEY", "value")
        ));

        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterLaunchPlan.Options(
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19081),
                new InetSocketAddress("127.0.0.1", 19082),
                List.of(),
                Map.of("BAD_VALUE", "bad\nvalue")
        ));

        var nonPortableOptionsEnvironmentKey = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterLaunchPlan.Options(
                        tempDir.resolve("work"),
                        tempDir.resolve("adapter-data"),
                        tempDir.resolve("adapter.properties"),
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082),
                        List.of(),
                        Map.of("BAD KEY", "value")
                ));
        assertTrue(nonPortableOptionsEnvironmentKey.getMessage().contains("portable environment variable"));

        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterDistribution(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of(Path.of("adapter/rockserver-ratis-adapter.jar")),
                List.of("-Xmx512m"),
                Map.of("BAD=KEY", "value")
        ));

        assertThrows(IllegalArgumentException.class, () -> new RatisProcessAdapterDistribution(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of(Path.of("adapter/rockserver-ratis-adapter.jar")),
                List.of("-Xmx512m"),
                Map.of("BAD_VALUE", "bad\0value")
        ));

        var nonPortableDistributionEnvironmentKey = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterDistribution(
                        Path.of("/opt/rockserver-ratis-adapter"),
                        Path.of("/usr/lib/jvm/default/bin/java"),
                        "it.cavallium.rockserver.ratis.AdapterMain",
                        List.of(Path.of("adapter/rockserver-ratis-adapter.jar")),
                        List.of("-Xmx512m"),
                        Map.of("BAD.KEY", "value")
                ));
        assertTrue(nonPortableDistributionEnvironmentKey.getMessage().contains("portable environment variable"));

        var optionsEnvironmentError = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterLaunchPlan.Options(
                        tempDir.resolve("work"),
                        tempDir.resolve("adapter-data"),
                        tempDir.resolve("adapter.properties"),
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082),
                        List.of(),
                        Map.of("JDK_JAVA_OPTIONS", "--dry-run")
                ));
        assertTrue(optionsEnvironmentError.getMessage().contains("Java launcher environment"));

        var commandEnvironmentError = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterCommand(
                        Path.of("/usr/lib/jvm/default/bin/java"),
                        Path.of("/var/lib/rockserver/adapter-work"),
                        List.of(Path.of("/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter.jar")),
                        "it.cavallium.rockserver.ratis.AdapterMain",
                        List.of("-Xmx512m"),
                        List.of("--print-health"),
                        Map.of("_JAVA_OPTIONS", "-javaagent:/tmp/agent.jar")
                ));
        assertTrue(commandEnvironmentError.getMessage().contains("Java launcher environment"));

        var distributionEnvironmentError = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterDistribution(
                        Path.of("/opt/rockserver-ratis-adapter"),
                        Path.of("/usr/lib/jvm/default/bin/java"),
                        "it.cavallium.rockserver.ratis.AdapterMain",
                        List.of(Path.of("adapter/rockserver-ratis-adapter.jar")),
                        List.of("-Xmx512m"),
                        Map.of("JAVA_TOOL_OPTIONS", "-javaagent:/tmp/agent.jar")
                ));
        assertTrue(distributionEnvironmentError.getMessage().contains("Java launcher environment"));

        var classpathEnvironmentError = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterCommand(
                        Path.of("/usr/lib/jvm/default/bin/java"),
                        Path.of("/var/lib/rockserver/adapter-work"),
                        List.of(Path.of("/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter.jar")),
                        "it.cavallium.rockserver.ratis.AdapterMain",
                        List.of("-Xmx512m"),
                        List.of("--print-health"),
                        Map.of("classpath", "/tmp/injected.jar")
                ));
        assertTrue(classpathEnvironmentError.getMessage().contains("Java launcher environment"));

        var nativeLoaderEnvironmentError = assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterLaunchPlan.Options(
                        tempDir.resolve("work"),
                        tempDir.resolve("adapter-data"),
                        tempDir.resolve("adapter.properties"),
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082),
                        List.of(),
                        Map.of("LD_PRELOAD", "/tmp/injected.so")
                ));
        assertTrue(nativeLoaderEnvironmentError.getMessage().contains("native loader environment"));
    }

    @Test
    void launcherRejectsNullInputsWithExplicitDiagnostics() {
        var nullCommand = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessLauncher.defaultLauncher().start(null));
        var nullProcessBuilder = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessLauncher.applyEnvironment(null, Map.of()));
        var nullCommandEnvironment = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessLauncher.applyEnvironment(new ProcessBuilder("java"), null));

        assertEquals("command must not be null", nullCommand.getMessage());
        assertEquals("processBuilder must not be null", nullProcessBuilder.getMessage());
        assertEquals("commandEnvironment must not be null", nullCommandEnvironment.getMessage());
    }

    @Test
    void processLauncherScrubsInheritedJavaLauncherEnvironment() {
        var processBuilder = new ProcessBuilder("ignored");
        var environment = processBuilder.environment();
        environment.put("JAVA_TOOL_OPTIONS", "-javaagent:/tmp/agent.jar");
        environment.put("_JAVA_OPTIONS", "-Xbootclasspath/a:/tmp/injected.jar");
        environment.put("jdk_java_options", "--dry-run");
        environment.put("CLASSPATH", "/tmp/injected.jar");
        environment.put("SAFE_INHERITED", "kept");

        RatisProcessLauncher.applyEnvironment(processBuilder, Map.of("SAFE_COMMAND", "value"));

        assertFalse(environment.containsKey("JAVA_TOOL_OPTIONS"));
        assertFalse(environment.containsKey("_JAVA_OPTIONS"));
        assertFalse(environment.containsKey("jdk_java_options"));
        assertFalse(environment.containsKey("CLASSPATH"));
        assertEquals("kept", environment.get("SAFE_INHERITED"));
        assertEquals("value", environment.get("SAFE_COMMAND"));
    }

    @Test
    void processLauncherScrubsInheritedNativeLoaderEnvironment() {
        var processBuilder = new ProcessBuilder("ignored");
        var environment = processBuilder.environment();
        environment.put("LD_PRELOAD", "/tmp/injected.so");
        environment.put("ld_library_path", "/tmp/native");
        environment.put("DYLD_INSERT_LIBRARIES", "/tmp/injected.dylib");
        environment.put("SAFE_INHERITED", "kept");

        RatisProcessLauncher.applyEnvironment(processBuilder, Map.of("SAFE_COMMAND", "value"));

        assertFalse(environment.containsKey("LD_PRELOAD"));
        assertFalse(environment.containsKey("ld_library_path"));
        assertFalse(environment.containsKey("DYLD_INSERT_LIBRARIES"));
        assertEquals("kept", environment.get("SAFE_INHERITED"));
        assertEquals("value", environment.get("SAFE_COMMAND"));
    }

    private static RatisProcessAdapterDistribution distribution() {
        return new RatisProcessAdapterDistribution(
                Path.of("/opt/rockserver-ratis-adapter"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                List.of(
                        Path.of("adapter/rockserver-ratis-adapter.jar"),
                        Path.of("lib/ratis-server.jar")
                ),
                List.of("-Xmx512m"),
                Map.of("BASE_ENV", "base")
        );
    }

    private static RatisProcessAdapterDistribution distribution(Path tempDir) throws IOException {
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
                Map.of("BASE_ENV", "base")
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

    private static ClusterMetadata metadata(NodeId nodeId, String groupId) {
        return metadata(nodeId, groupId, 1);
    }

    private static ClusterMetadata metadata(NodeId nodeId, String groupId, long metadataEpoch) {
        var partition = new PartitionDescriptor(
                new ShardId(7),
                null,
                null,
                1,
                groupId,
                List.of(new ReplicaMember(nodeId, URI.create("grpc://10.0.0.1:19001"),
                        ReplicaRole.VOTER, 0, 0, true)),
                nodeId,
                Map.of(),
                PartitionSplitState.ACTIVE
        );
        var node = new ClusterNodeMetadata(
                nodeId,
                URI.create("http://n1.public:10000"),
                URI.create("grpc://10.0.0.1:19001"),
                "rack-a"
        );
        return new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                metadataEpoch,
                Map.of(),
                Map.of(),
                Map.of(partition.partitionId(), partition),
                Map.of(node.nodeId(), node)
        );
    }

    private static RatisProcessAdapterLaunchPlan launchPlan(Path tempDir,
                                                           Path configPath,
                                                           Path metadataPath,
                                                           ClusterMetadata metadata) {
        return RatisProcessAdapterLaunchPlan.fromMetadata(
                distribution(),
                metadata,
                NodeId.of("n1"),
                new RatisProcessAdapterLaunchPlan.Options(
                        tempDir.resolve("work"),
                        tempDir.resolve("adapter-data"),
                        configPath,
                        metadataPath,
                        new InetSocketAddress("127.0.0.1", 19081),
                        new InetSocketAddress("127.0.0.1", 19082),
                        List.of(),
                        Map.of()
                )
        );
    }

    private static RatisProcessAdapterLaunchPlan.Options options(Path tempDir) {
        return new RatisProcessAdapterLaunchPlan.Options(
                tempDir.resolve("work"),
                tempDir.resolve("adapter-data"),
                tempDir.resolve("adapter.properties"),
                new InetSocketAddress("127.0.0.1", 19081),
                new InetSocketAddress("127.0.0.1", 19082),
                List.of(),
                Map.of()
        );
    }

    private static Process startConfigPairLockHolder(Path lockPath) throws IOException {
        return new ProcessBuilder(
                Path.of(System.getProperty("java.home")).resolve("bin").resolve("java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                ConfigPairLockHolder.class.getName(),
                lockPath.toString()
        ).redirectErrorStream(true).start();
    }

    private static void awaitConfigActivationRelease(CountDownLatch latch) throws IOException {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IOException("timed out waiting for config activation release");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for config activation release", e);
        }
    }

    public static final class ConfigPairLockHolder {

        private ConfigPairLockHolder() {
        }

        static void main(String[] args) throws Exception {
            var lockPath = Path.of(args[0]);
            try (var lockChannel = FileChannel.open(lockPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    LinkOption.NOFOLLOW_LINKS);
                 var _ = lockChannel.lock()) {
                System.out.println("locked");
                System.out.flush();
                var _ = System.in.read();
            }
        }
    }

    private static final class FakeProcess extends Process {

        private boolean alive = true;
        private boolean destroyCalled;

        @Override
        public OutputStream getOutputStream() {
            return OutputStream.nullOutputStream();
        }

        @Override
        public InputStream getInputStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public InputStream getErrorStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public int waitFor() {
            alive = false;
            return 0;
        }

        @Override
        public boolean waitFor(long timeout, TimeUnit unit) {
            return !alive;
        }

        @Override
        public int exitValue() {
            if (alive) {
                throw new IllegalThreadStateException("still running");
            }
            return 0;
        }

        @Override
        public void destroy() {
            destroyCalled = true;
            alive = false;
        }

        @Override
        public Process destroyForcibly() {
            destroy();
            return this;
        }

        @Override
        public boolean isAlive() {
            return alive;
        }
    }
}
