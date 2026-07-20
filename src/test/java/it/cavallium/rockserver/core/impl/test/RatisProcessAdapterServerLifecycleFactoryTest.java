package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ClusterNodeMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ReplicatedModeConfiguration;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapPeer;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapManifestStore;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapPlan;
import it.cavallium.rockserver.core.cluster.metadata.CdcTokenTranslation;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcToken;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.replication.ratis.RatisAdapterBoundaryMode;
import it.cavallium.rockserver.core.replication.ratis.RatisAdapterBoundary;
import it.cavallium.rockserver.core.replication.ratis.RatisAdapterClientResolver;
import it.cavallium.rockserver.core.replication.ratis.RatisAdapterFactory;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionClient;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterConfig;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterLaunchPlan;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterMetadataConfig;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterServerLifecycleFactory;
import it.cavallium.rockserver.core.replication.ratis.SharedRatisServerLifecycle;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RatisProcessAdapterServerLifecycleFactoryTest {

    @Test
    void launchPlanUsesReplicatedModeAdapterSettingsAndWritesGeneratedFiles(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var groupId = UUID.randomUUID().toString();
        var configuration = configuration(tempDir, nodeId);

        var plan = RatisProcessAdapterServerLifecycleFactory.launchPlan(
                configuration,
                metadata(nodeId, groupId)
        );

        assertEquals(RatisProcessAdapterServerLifecycleFactory.generatedConfigPath(configuration),
                plan.configPath());
        assertEquals(RatisProcessAdapterServerLifecycleFactory.generatedMetadataPath(configuration),
                plan.metadataPath());
        assertEquals(new InetSocketAddress("127.0.0.1", 19101), plan.healthAddress());
        assertEquals(new InetSocketAddress("127.0.0.1", 19102), plan.rpcAddress());
        assertTrue(plan.command().toCommandLine().contains(
                "--metadata=" + RatisProcessAdapterServerLifecycleFactory.generatedMetadataPath(configuration)
        ));
        assertEquals("n1", plan.command().environment().get(RatisProcessAdapterLaunchPlan.NODE_ID_ENV));

        plan.writeConfig();

        assertTrue(Files.exists(RatisProcessAdapterServerLifecycleFactory.generatedConfigPath(configuration)));
        assertTrue(Files.exists(RatisProcessAdapterServerLifecycleFactory.generatedMetadataPath(configuration)));
    }

    @Test
    void sharedLifecycleAndAdapterFactoryPreserveExternalProcessBoundary(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var metadata = metadata(nodeId, UUID.randomUUID().toString());
        var configuration = configuration(tempDir, nodeId);

        try (var lifecycle = RatisProcessAdapterServerLifecycleFactory.sharedLifecycle(
                configuration,
                metadata,
                Duration.ofSeconds(1),
                Duration.ofSeconds(1)
        ); var adapterFactory = RatisProcessAdapterServerLifecycleFactory.adapterFactory(
                configuration,
                metadata,
                Duration.ofSeconds(1)
        )) {
            assertEquals(RatisAdapterBoundaryMode.EXTERNAL_PROCESS, lifecycle.boundary().mode());
            assertEquals(RatisAdapterBoundaryMode.EXTERNAL_PROCESS, adapterFactory.boundary().mode());
        }
    }

    @Test
    void launchPlanCanUseBootstrapPlanBeforeDataPartitionsExist(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var bootstrapPlan = BootstrapPlan.singleNodeDevelopment(
                new ClusterId(new UUID(0, 1)),
                "metadata-dev",
                new BootstrapPeer(
                        nodeId,
                        URI.create("http://127.0.0.1:8080"),
                        URI.create("grpc://127.0.0.1:19001"),
                        "rack-a"
                ),
                0
        );

        var plan = RatisProcessAdapterServerLifecycleFactory.launchPlan(
                configuration(tempDir, nodeId),
                bootstrapPlan
        );

        assertEquals(RatisProcessAdapterConfig.METADATA_GROUP_PARTITION_ID,
                plan.adapterConfig().metadataGroupPartition().orElseThrow().partitionId());
        assertTrue(plan.adapterConfig().dataPartitions().isEmpty());
        plan.writeConfig();
        assertTrue(Files.exists(RatisProcessAdapterServerLifecycleFactory.generatedConfigPath(
                configuration(tempDir, nodeId)
        )));
    }

    @Test
    void bootstrapLaunchPlanReadsConfiguredManifestPath(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configuration(tempDir, nodeId);
        var bootstrapPlan = BootstrapPlan.singleNodeDevelopment(
                new ClusterId(new UUID(0, 1)),
                "metadata-dev",
                new BootstrapPeer(
                        nodeId,
                        URI.create("http://127.0.0.1:8080"),
                        URI.create("grpc://127.0.0.1:19001"),
                        "rack-a"
                ),
                0
        );
        BootstrapManifestStore.write(configuration.bootstrapManifestPath(), bootstrapPlan.manifest());

        var plan = RatisProcessAdapterServerLifecycleFactory.bootstrapLaunchPlan(configuration);
        assertEquals(RatisProcessAdapterConfig.METADATA_GROUP_PARTITION_ID,
                plan.adapterConfig().metadataGroupPartition().orElseThrow().partitionId());
        try (var lifecycle = RatisProcessAdapterServerLifecycleFactory.sharedBootstrapLifecycle(configuration);
             var adapterFactory = RatisProcessAdapterServerLifecycleFactory.bootstrapAdapterFactory(
                     configuration,
                     Duration.ofSeconds(1)
             );
             var connection = RatisProcessAdapterServerLifecycleFactory.bootstrapReplicatedConnection(
                     "bootstrap",
                     configuration,
                     Duration.ofSeconds(1)
             )) {
            assertEquals(RatisAdapterBoundaryMode.EXTERNAL_PROCESS, lifecycle.boundary().mode());
            assertEquals(RatisAdapterBoundaryMode.EXTERNAL_PROCESS, adapterFactory.boundary().mode());
            assertNotNull(connection.getSyncApi());
        }
    }

    @Test
    void configuredLaunchPlanAndConnectionPreferGeneratedMetadata(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configuration(tempDir, nodeId);
        var liveMetadata = metadataWithColumn(nodeId, UUID.randomUUID().toString());
        RatisProcessAdapterServerLifecycleFactory.launchPlan(configuration, liveMetadata).writeConfig();

        var plan = RatisProcessAdapterServerLifecycleFactory.configuredLaunchPlan(configuration);
        try (var adapterFactory = RatisProcessAdapterServerLifecycleFactory.configuredAdapterFactory(
                configuration,
                Duration.ofSeconds(1)
        ); var explicitConnection = RatisProcessAdapterServerLifecycleFactory.replicatedConnection(
                "explicit",
                configuration,
                liveMetadata,
                Duration.ofSeconds(1)
        ); var connection = RatisProcessAdapterServerLifecycleFactory.configuredReplicatedConnection(
                "live",
                configuration,
                Duration.ofSeconds(1)
        )) {
            assertEquals(RatisAdapterBoundaryMode.EXTERNAL_PROCESS, adapterFactory.boundary().mode());
            assertEquals(9, explicitConnection.getSyncApi().getColumnId("live-col"));
            assertEquals(9, connection.getSyncApi().getColumnId("live-col"));
        }

        assertEquals(5, plan.metadataConfig().metadata().epoch());
        assertEquals(1, plan.adapterConfig().dataPartitions().size());
        assertEquals(new ShardId(7).toString(), plan.adapterConfig().dataPartitions().getFirst().partitionId());
        assertEquals(RatisProcessAdapterConfig.METADATA_GROUP_PARTITION_ID,
                plan.adapterConfig().metadataGroupPartition().orElseThrow().partitionId());
    }

    @Test
    void configuredMetadataRejectsStagedMetadataWithoutActivatedConfig(@TempDir Path tempDir) throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configuration(tempDir, nodeId);
        var liveMetadata = metadataWithColumn(nodeId, UUID.randomUUID().toString());
        new RatisProcessAdapterMetadataConfig(liveMetadata).write(
                RatisProcessAdapterServerLifecycleFactory.generatedMetadataPath(configuration)
        );

        var error = assertThrows(IOException.class,
                () -> RatisProcessAdapterServerLifecycleFactory.configuredMetadata(configuration));

        assertTrue(error.getMessage().contains("generated adapter metadata exists without activated config"));
    }

    @Test
    void configuredMetadataRejectsActivatedConfigWithoutMetadataSidecar(@TempDir Path tempDir) throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configuration(tempDir, nodeId);
        var plan = RatisProcessAdapterServerLifecycleFactory.launchPlan(
                configuration,
                metadataWithColumn(nodeId, UUID.randomUUID().toString())
        );
        plan.adapterConfig().write(plan.configPath(), plan.metadataConfig().semanticSha256());

        var error = assertThrows(IOException.class,
                () -> RatisProcessAdapterServerLifecycleFactory.configuredMetadata(configuration));

        assertTrue(error.getMessage().contains("generated adapter config exists without metadata sidecar"));
    }

    @Test
    void configuredMetadataRejectsDigestDivergentMetadataSidecar(@TempDir Path tempDir) throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configuration(tempDir, nodeId);
        var liveMetadata = metadataWithColumn(nodeId, UUID.randomUUID().toString());
        RatisProcessAdapterServerLifecycleFactory.launchPlan(configuration, liveMetadata).writeConfig();
        new RatisProcessAdapterMetadataConfig(withCdcTokenTranslation(liveMetadata)).write(
                RatisProcessAdapterServerLifecycleFactory.generatedMetadataPath(configuration)
        );

        var error = assertThrows(IOException.class,
                () -> RatisProcessAdapterServerLifecycleFactory.configuredMetadata(configuration));

        assertTrue(error.getMessage().contains("does not match sidecar SHA-256"));
    }

    @Test
    void configuredMetadataRejectsGeneratedMetadataSymlinkInsteadOfFallingBackToBootstrap(@TempDir Path tempDir)
            throws Exception {
        var configuration = configuration(tempDir, NodeId.of("n1"));
        var generatedMetadataPath = RatisProcessAdapterServerLifecycleFactory.generatedMetadataPath(configuration);
        Files.createDirectories(generatedMetadataPath.getParent());
        Files.createSymbolicLink(generatedMetadataPath, Path.of("missing-metadata.properties"));

        var error = assertThrows(IOException.class,
                () -> RatisProcessAdapterServerLifecycleFactory.configuredMetadata(configuration));

        assertTrue(error.getMessage().contains("generated adapter metadata path must be a regular file"));
    }

    @Test
    void configuredDeploymentPreflightUsesConfiguredLaunchPlan(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configurationWithValidDistribution(tempDir, nodeId);

        var report = RatisProcessAdapterServerLifecycleFactory.configuredDeploymentPreflight(configuration);
        var evidence = RatisProcessAdapterServerLifecycleFactory.configuredDeploymentEvidence(configuration);
        var reportPath = RatisProcessAdapterServerLifecycleFactory.writeConfiguredDeploymentPreflightReport(configuration);
        var evidencePath = RatisProcessAdapterServerLifecycleFactory.writeConfiguredDeploymentEvidence(configuration);

        assertTrue(report.accepted(), report.message());
        var dataPath = configuration.dataPath();
        var metadataPath = configuration.metadataPath();
        assertNotNull(dataPath);
        assertNotNull(metadataPath);
        assertEquals(dataPath.resolve("adapter-work"), evidence.workingDirectory());
        assertEquals(RatisProcessAdapterServerLifecycleFactory.generatedConfigPath(configuration), evidence.configPath());
        assertEquals(RatisProcessAdapterServerLifecycleFactory.generatedMetadataPath(configuration),
                evidence.metadataPath());
        assertEquals("n1", evidence.environment().get(RatisProcessAdapterLaunchPlan.NODE_ID_ENV));
        assertEquals(RatisProcessAdapterConfig.METADATA_GROUP_PARTITION_ID,
                evidence.partitions().getFirst().partitionId());
        assertEquals(RatisProcessAdapterServerLifecycleFactory.generatedPreflightReportPath(configuration), reportPath);
        assertTrue(Files.readString(reportPath).contains("accepted=true"));
        assertEquals(RatisProcessAdapterServerLifecycleFactory.generatedEvidencePath(configuration), evidencePath);
        var evidenceText = Files.readString(evidencePath);
        assertTrue(evidenceText.contains(
                "partition.0.partitionId=" + RatisProcessAdapterConfig.METADATA_GROUP_PARTITION_ID
        ));
        assertTrue(evidenceText.contains("launchFile.0.sha256="));
        assertTrue(evidenceText.contains("plannedConfig.sha256="));
        assertTrue(evidenceText.contains("plannedMetadata.sha256="));
        assertTrue(evidenceText.contains("publishedConfig.sha256="));
        assertTrue(evidenceText.contains("publishedMetadata.sha256="));
        RatisProcessAdapterServerLifecycleFactory.verifyConfiguredDeploymentPreflight(configuration);
        assertTrue(Files.exists(RatisProcessAdapterServerLifecycleFactory.generatedConfigPath(configuration)));
        assertTrue(Files.exists(RatisProcessAdapterServerLifecycleFactory.generatedMetadataPath(configuration)));
        assertTrue(Files.isDirectory(dataPath
                .resolve("adapter-data")
                .resolve("metadata-group")));
        assertTrue(Files.isDirectory(metadataPath
                .resolve("adapter-process")));
    }

    @Test
    void writeConfiguredDeploymentEvidenceRequiresAcceptedPreflight(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configuration(tempDir, nodeId);
        var reportPath = RatisProcessAdapterServerLifecycleFactory.writeConfiguredDeploymentPreflightReport(configuration);
        var evidencePath = RatisProcessAdapterServerLifecycleFactory.generatedEvidencePath(configuration);

        var error = assertThrows(IOException.class,
                () -> RatisProcessAdapterServerLifecycleFactory.writeConfiguredDeploymentEvidence(configuration));

        assertTrue(error.getMessage().contains("Ratis adapter deployment preflight failed"));
        var reportText = Files.readString(reportPath);
        assertTrue(reportText.contains("accepted=false"));
        assertTrue(reportText.contains("classpath entry does not exist"));
        assertFalse(Files.exists(evidencePath));
        assertFalse(Files.exists(RatisProcessAdapterServerLifecycleFactory.generatedConfigPath(configuration)));
        assertFalse(Files.exists(RatisProcessAdapterServerLifecycleFactory.generatedMetadataPath(configuration)));
    }

    @Test
    void refreshAdapterMetadataPublishesFilesReloadsAdapterAndRefreshesResolver(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configurationWithValidDistribution(tempDir, nodeId);
        var refreshedMetadata = metadataWithColumn(nodeId, UUID.randomUUID().toString());
        var adapterFactory = new RecordingAdapterFactory();
        var resolver = new RatisAdapterClientResolver(adapterFactory, List.of());

        RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                configuration,
                refreshedMetadata,
                adapterFactory,
                resolver
        );

        var storedMetadata = RatisProcessAdapterMetadataConfig.read(
                RatisProcessAdapterServerLifecycleFactory.generatedMetadataPath(configuration)
        ).metadata();
        assertEquals(refreshedMetadata, storedMetadata);
        assertEquals(1, adapterFactory.reloadCount);
        assertEquals(2, adapterFactory.openedPartitions.size());
        assertEquals(RatisProcessAdapterConfig.METADATA_GROUP_PARTITION_ID,
                adapterFactory.openedPartitions.getFirst().partitionId());
        assertEquals(new ShardId(7).toString(), adapterFactory.openedPartitions.get(1).partitionId());
    }

    @Test
    void refreshAdapterMetadataRequiresAcceptedPreflightBeforePublicationOrReload(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configuration(tempDir, nodeId);
        var refreshedMetadata = metadataWithColumn(nodeId, UUID.randomUUID().toString());
        var adapterFactory = new RecordingAdapterFactory();
        var resolver = new RatisAdapterClientResolver(adapterFactory, List.of());

        var error = assertThrows(UncheckedIOException.class, () ->
                RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                        configuration,
                        refreshedMetadata,
                        adapterFactory,
                        resolver
                ));

        assertTrue(error.getMessage().contains("failed to publish Ratis adapter metadata"));
        assertEquals(0, adapterFactory.reloadCount);
        assertTrue(adapterFactory.openedPartitions.isEmpty());
        assertFalse(Files.exists(RatisProcessAdapterServerLifecycleFactory.generatedConfigPath(configuration)));
        assertFalse(Files.exists(RatisProcessAdapterServerLifecycleFactory.generatedMetadataPath(configuration)));
    }

    @Test
    void refreshAdapterMetadataRejectsStaleOrTopologyDivergentPublishedEpochBeforeReload(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configurationWithValidDistribution(tempDir, nodeId);
        var publishedMetadata = metadataWithColumn(nodeId, UUID.randomUUID().toString());
        var adapterFactory = new RecordingAdapterFactory();
        var resolver = new RatisAdapterClientResolver(adapterFactory, List.of());
        RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                configuration,
                publishedMetadata,
                adapterFactory,
                resolver
        );

        assertThrows(IllegalArgumentException.class, () ->
                RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                        configuration,
                        metadata(nodeId, UUID.randomUUID().toString()),
                        adapterFactory,
                        resolver
                ));
        assertThrows(IllegalArgumentException.class, () ->
                RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                        configuration,
                        metadataWithColumn(nodeId, UUID.randomUUID().toString()),
                        adapterFactory,
                        resolver
                ));

        assertEquals(publishedMetadata, RatisProcessAdapterMetadataConfig.read(
                RatisProcessAdapterServerLifecycleFactory.generatedMetadataPath(configuration)
        ).metadata());
        assertEquals(1, adapterFactory.reloadCount);
    }

    @Test
    void refreshAdapterMetadataAllowsSameEpochCdcTranslationPublication(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configurationWithValidDistribution(tempDir, nodeId);
        var publishedMetadata = metadataWithColumn(nodeId, UUID.randomUUID().toString());
        var sameEpochMetadata = withCdcTokenTranslation(publishedMetadata);
        var adapterFactory = new RecordingAdapterFactory();
        var resolver = new RatisAdapterClientResolver(adapterFactory, List.of());

        RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                configuration,
                publishedMetadata,
                adapterFactory,
                resolver
        );
        RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                configuration,
                sameEpochMetadata,
                adapterFactory,
                resolver
        );

        assertEquals(sameEpochMetadata, RatisProcessAdapterMetadataConfig.read(
                RatisProcessAdapterServerLifecycleFactory.generatedMetadataPath(configuration)
        ).metadata());
        assertEquals(2, adapterFactory.reloadCount);
    }

    @Test
    void refreshAdapterMetadataAllowsIdempotentPublishedEpochRetry(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configurationWithValidDistribution(tempDir, nodeId);
        var publishedMetadata = metadataWithColumn(nodeId, UUID.randomUUID().toString());
        var adapterFactory = new RecordingAdapterFactory();
        var resolver = new RatisAdapterClientResolver(adapterFactory, List.of());

        RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                configuration,
                publishedMetadata,
                adapterFactory,
                resolver
        );
        RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                configuration,
                publishedMetadata,
                adapterFactory,
                resolver
        );

        assertEquals(2, adapterFactory.reloadCount);
    }

    @Test
    void refreshAdapterMetadataRepairsStagedMetadataAfterConfigActivationFailure(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configurationWithValidDistribution(tempDir, nodeId);
        var publishedMetadata = metadataWithColumn(nodeId, UUID.randomUUID().toString());
        var configPath = RatisProcessAdapterServerLifecycleFactory.generatedConfigPath(configuration);
        var metadataPath = RatisProcessAdapterServerLifecycleFactory.generatedMetadataPath(configuration);
        var plan = RatisProcessAdapterServerLifecycleFactory.launchPlan(configuration, publishedMetadata);
        Files.createDirectories(configPath);
        var adapterFactory = new RecordingAdapterFactory();
        var resolver = new RatisAdapterClientResolver(adapterFactory, List.of());

        assertThrows(IOException.class, plan::writeConfig);

        assertEquals(publishedMetadata, RatisProcessAdapterMetadataConfig.read(metadataPath).metadata());
        assertTrue(Files.isDirectory(configPath));
        assertEquals(0, adapterFactory.reloadCount);
        assertTrue(adapterFactory.openedPartitions.isEmpty());

        Files.delete(configPath);
        RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                configuration,
                publishedMetadata,
                adapterFactory,
                resolver
        );

        assertTrue(Files.isRegularFile(configPath));
        assertEquals(1, adapterFactory.reloadCount);
        assertEquals(2, adapterFactory.openedPartitions.size());
    }

    @Test
    void refreshAdapterMetadataRetriesAfterReloadFailure(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configurationWithValidDistribution(tempDir, nodeId);
        var publishedMetadata = metadataWithColumn(nodeId, UUID.randomUUID().toString());
        var failure = new IllegalStateException("reload failed");
        var adapterFactory = new RecordingAdapterFactory();
        adapterFactory.reloadFailure = failure;
        var resolver = new RatisAdapterClientResolver(adapterFactory, List.of());

        assertEquals(failure, assertThrows(IllegalStateException.class, () ->
                RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                        configuration,
                        publishedMetadata,
                        adapterFactory,
                        resolver
                )));

        assertEquals(1, adapterFactory.reloadCount);
        assertTrue(adapterFactory.openedPartitions.isEmpty());

        adapterFactory.reloadFailure = null;
        RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                configuration,
                publishedMetadata,
                adapterFactory,
                resolver
        );

        assertEquals(2, adapterFactory.reloadCount);
        assertEquals(2, adapterFactory.openedPartitions.size());
    }

    @Test
    void connectionOwnershipRollbackClosesResolverAndSuppressesCleanupFailure() {
        var closeFailure = new AssertionError("close failed");
        var adapterFactory = new RecordingAdapterFactory();
        adapterFactory.closeFailure = closeFailure;
        var resolver = new RatisAdapterClientResolver(adapterFactory, List.of());
        var constructionFailure = new IllegalStateException("construction failed");

        assertEquals(constructionFailure, assertThrows(IllegalStateException.class, () ->
                RatisProcessAdapterServerLifecycleFactory.transferResolverOwnership(resolver, () -> {
                    throw constructionFailure;
                })));

        assertTrue(adapterFactory.closed);
        assertEquals(List.of(closeFailure), List.of(constructionFailure.getSuppressed()));
        assertThrows(IllegalStateException.class, () -> resolver.client("missing"));
    }

    @Test
    void refreshAdapterMetadataPropagatesFatalReloadFailureCause(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configurationWithValidDistribution(tempDir, nodeId);
        var refreshedMetadata = metadataWithColumn(nodeId, UUID.randomUUID().toString());
        var failure = new AssertionError("reload failed");
        var adapterFactory = new RecordingAdapterFactory();
        adapterFactory.reloadFailure = failure;
        var resolver = new RatisAdapterClientResolver(adapterFactory, List.of());

        assertEquals(failure, assertThrows(AssertionError.class, () ->
                RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                        configuration,
                        refreshedMetadata,
                        adapterFactory,
                        resolver
                )));

        assertEquals(1, adapterFactory.reloadCount);
        assertTrue(adapterFactory.openedPartitions.isEmpty());
    }

    @Test
    void refreshAdapterMetadataPropagatesNestedFatalReloadFailureCause(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configurationWithValidDistribution(tempDir, nodeId);
        var refreshedMetadata = metadataWithColumn(nodeId, UUID.randomUUID().toString());
        var failure = new AssertionError("reload failed");
        var adapterFactory = new RecordingAdapterFactory();
        adapterFactory.reloadFailure = new CompletionException(new CompletionException(failure));
        var resolver = new RatisAdapterClientResolver(adapterFactory, List.of());

        assertSame(failure, assertThrows(AssertionError.class, () ->
                RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                        configuration,
                        refreshedMetadata,
                        adapterFactory,
                        resolver
                )));

        assertEquals(1, adapterFactory.reloadCount);
        assertTrue(adapterFactory.openedPartitions.isEmpty());
    }

    @Test
    void refreshAdapterMetadataFlattensNestedCheckedReloadFailureCause(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configurationWithValidDistribution(tempDir, nodeId);
        var refreshedMetadata = metadataWithColumn(nodeId, UUID.randomUUID().toString());
        var failure = new IOException("reload failed");
        var adapterFactory = new RecordingAdapterFactory();
        adapterFactory.reloadFailure = new CompletionException(new CompletionException(failure));
        var resolver = new RatisAdapterClientResolver(adapterFactory, List.of());

        var thrown = assertThrows(CompletionException.class, () ->
                RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                        configuration,
                        refreshedMetadata,
                        adapterFactory,
                        resolver
                ));

        assertEquals("Ratis adapter metadata reload failed", thrown.getMessage());
        assertSame(failure, thrown.getCause());
        assertEquals(1, adapterFactory.reloadCount);
        assertTrue(adapterFactory.openedPartitions.isEmpty());
    }

    @Test
    void refreshAdapterMetadataInterruptionCancelsReloadAndRestoresInterruptFlag(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configurationWithValidDistribution(tempDir, nodeId);
        var refreshedMetadata = metadataWithColumn(nodeId, UUID.randomUUID().toString());
        var response = new CompletableFuture<Void>();
        var adapterFactory = new RecordingAdapterFactory();
        adapterFactory.reloadResponse = response;
        var resolver = new RatisAdapterClientResolver(adapterFactory, List.of());
        var failure = new AtomicReference<Throwable>();
        var interrupted = new AtomicBoolean();
        var refreshThread = new Thread(() -> {
            try {
                RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                        configuration,
                        refreshedMetadata,
                        adapterFactory,
                        resolver
                );
            } catch (Throwable e) {
                failure.set(e);
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        refreshThread.start();
        assertTrue(adapterFactory.reloadStarted.await(5, TimeUnit.SECONDS));

        refreshThread.interrupt();
        refreshThread.join(TimeUnit.SECONDS.toMillis(5));

        assertFalse(refreshThread.isAlive());
        var completionFailure = assertInstanceOf(CompletionException.class, failure.get());
        assertInstanceOf(InterruptedException.class, completionFailure.getCause());
        assertEquals("Ratis adapter metadata reload interrupted", completionFailure.getMessage());
        assertTrue(interrupted.get());
        assertTrue(response.isCancelled());
        assertTrue(adapterFactory.openedPartitions.isEmpty());
    }

    @Test
    void launchPlanRequiresEnabledReplication() {
        var nodeId = NodeId.of("n1");
        var disabled = ReplicatedModeConfiguration.disabled();

        assertThrows(IllegalArgumentException.class, () ->
                RatisProcessAdapterServerLifecycleFactory.launchPlan(disabled, metadata(nodeId, "group")));
    }

    @Test
    void serverLifecycleFactoryRejectsNullInputsWithExplicitDiagnostics(@TempDir Path tempDir)
            throws Exception {
        var nodeId = NodeId.of("n1");
        var configuration = configuration(tempDir, nodeId);
        var metadata = metadata(nodeId, UUID.randomUUID().toString());
        var adapterFactory = new RecordingAdapterFactory();
        var resolver = new RatisAdapterClientResolver(adapterFactory, List.of());
        var nullResultAdapterFactory = new RecordingAdapterFactory();
        var nullResultResolver = new RatisAdapterClientResolver(nullResultAdapterFactory, List.of());

        var nullSharedDelegate = assertThrows(IllegalArgumentException.class,
                () -> new SharedRatisServerLifecycle(null));
        var nullLaunchConfiguration = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterServerLifecycleFactory.launchPlan(null, metadata));
        var nullLaunchMetadata = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterServerLifecycleFactory.launchPlan(configuration, (ClusterMetadata) null));
        var nullBootstrapPlan = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterServerLifecycleFactory.launchPlan(configuration, (BootstrapPlan) null));
        var nullSharedBootstrapPlan = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterServerLifecycleFactory.sharedLifecycle(configuration, (BootstrapPlan) null));
        var nullGeneratedPathConfiguration = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterServerLifecycleFactory.generatedConfigPath(null));
        var nullOwnershipResolver = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterServerLifecycleFactory.transferResolverOwnership(null, () -> null));
        var nullOwnershipFactory = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterServerLifecycleFactory.transferResolverOwnership(resolver, null));
        var nullConnectionResult = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterServerLifecycleFactory.transferResolverOwnership(nullResultResolver, () -> null));
        var nullRefreshAdapterFactory = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                        configuration,
                        metadata,
                        null,
                        resolver
                ));
        var nullRefreshResolver = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterServerLifecycleFactory.refreshAdapterMetadata(
                        configuration,
                        metadata,
                        adapterFactory,
                        null
                ));

        assertEquals("delegate must not be null", nullSharedDelegate.getMessage());
        assertEquals("configuration must not be null", nullLaunchConfiguration.getMessage());
        assertEquals("metadata must not be null", nullLaunchMetadata.getMessage());
        assertEquals("bootstrapPlan must not be null", nullBootstrapPlan.getMessage());
        assertEquals("bootstrapPlan must not be null", nullSharedBootstrapPlan.getMessage());
        assertEquals("configuration must not be null", nullGeneratedPathConfiguration.getMessage());
        assertEquals("resolver must not be null", nullOwnershipResolver.getMessage());
        assertEquals("connectionFactory must not be null", nullOwnershipFactory.getMessage());
        assertEquals("connectionFactory result must not be null", nullConnectionResult.getMessage());
        assertEquals("adapterFactory must not be null", nullRefreshAdapterFactory.getMessage());
        assertEquals("resolver must not be null", nullRefreshResolver.getMessage());
        assertTrue(nullResultAdapterFactory.closed);
    }

    private static ReplicatedModeConfiguration configuration(Path tempDir, NodeId nodeId) throws Exception {
        return configuration(tempDir, nodeId, Path.of("/usr/lib/jvm/default/bin/java"), false);
    }

    private static ReplicatedModeConfiguration configurationWithValidDistribution(Path tempDir, NodeId nodeId)
            throws Exception {
        var javaExecutable = tempDir.resolve("java-home").resolve("bin").resolve("java");
        Files.createDirectories(javaExecutable.getParent());
        Files.writeString(javaExecutable, "test");
        assertTrue(javaExecutable.toFile().setExecutable(true));
        return configuration(tempDir, nodeId, javaExecutable, true);
    }

    private static ReplicatedModeConfiguration configuration(Path tempDir,
                                                             NodeId nodeId,
                                                             Path javaExecutable,
                                                             boolean createClasspathEntries) throws Exception {
        var adapterHome = tempDir.resolve("adapter-home");
        var manifestPath = adapterHome.resolve("ratis-adapter.properties");
        var bootstrapManifestPath = tempDir.resolve("bootstrap.properties");
        Files.createDirectories(adapterHome);
        if (createClasspathEntries) {
            createFile(adapterHome.resolve("adapter").resolve("rockserver-ratis-adapter.jar"));
            createFile(adapterHome.resolve("lib").resolve("ratis-server.jar"));
        }
        Files.writeString(manifestPath, """
                mainClass=it.cavallium.rockserver.ratis.AdapterMain
                classpath.0=adapter/rockserver-ratis-adapter.jar
                classpath.1=lib/ratis-server.jar
                """);
        BootstrapManifestStore.write(
                bootstrapManifestPath,
                BootstrapPlan.singleNodeDevelopment(
                        new ClusterId(new UUID(0, 1)),
                        "metadata-group",
                        new BootstrapPeer(
                                nodeId,
                                URI.create("http://127.0.0.1:8080"),
                                URI.create("grpc://127.0.0.1:19001"),
                                "rack-a"
                        ),
                        0
                ).manifest()
        );

        return new ReplicatedModeConfiguration(
                true,
                nodeId,
                URI.create("grpc://127.0.0.1:19001"),
                URI.create("http://127.0.0.1:8080"),
                tempDir.resolve("data"),
                tempDir.resolve("metadata"),
                bootstrapManifestPath,
                "rack-a",
                new ReplicatedModeConfiguration.AdapterProcess(
                        adapterHome,
                        manifestPath,
                        javaExecutable,
                        new InetSocketAddress("127.0.0.1", 19101),
                        new InetSocketAddress("127.0.0.1", 19102)
                ),
                List.of(new ReplicatedModeConfiguration.MetadataPeer(
                        nodeId,
                        URI.create("grpc://127.0.0.1:19001"),
                        URI.create("http://127.0.0.1:8080"),
                        "rack-a"
                ))
        );
    }

    private static void createFile(Path path) throws Exception {
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
        var partition = new PartitionDescriptor(
                new ShardId(7),
                null,
                null,
                1,
                groupId,
                List.of(new ReplicaMember(nodeId, URI.create("grpc://127.0.0.1:19001"),
                        ReplicaRole.VOTER, 0, 0, true)),
                nodeId,
                Map.of(),
                PartitionSplitState.ACTIVE
        );
        var node = new ClusterNodeMetadata(
                nodeId,
                URI.create("http://n1.public:10000"),
                URI.create("grpc://127.0.0.1:19001"),
                "rack-a"
        );
        return new ClusterMetadata(
                new ClusterId(new UUID(0, 1)),
                1,
                Map.of(),
                Map.of(),
                Map.of(partition.partitionId(), partition),
                Map.of(node.nodeId(), node)
        );
    }

    private static ClusterMetadata metadataWithColumn(NodeId nodeId, String groupId) {
        var column = new ClusterColumnMetadata(
                9,
                "live-col",
                ColumnSchema.of(IntList.of(1), ObjectList.of(), true),
                4
        );
        var base = metadata(nodeId, groupId);
        return new ClusterMetadata(
                base.clusterId(),
                5,
                base.shards(),
                Map.of(column.columnId(), column),
                base.partitionsById(),
                base.nodesById()
        );
    }

    private static ClusterMetadata withCdcTokenTranslation(ClusterMetadata metadata) {
        var mergedPartition = metadata.requirePartition(new ShardId(7));
        var translation = new CdcTokenTranslation.Merge(
                metadata.epoch(),
                new ReplicatedCdcToken(metadata.epoch() - 1, new ShardId(8), 0, 10, 0),
                new ReplicatedCdcToken(metadata.epoch() - 1, new ShardId(9), 0, 11, 0),
                new ReplicatedCdcToken(metadata.epoch(), mergedPartition.partitionId(), mergedPartition.epoch(), 0, 0)
        );
        return new ClusterMetadata(
                metadata.clusterId(),
                metadata.epoch(),
                metadata.shards(),
                metadata.columnsById(),
                metadata.partitionsById(),
                metadata.nodesById(),
                Map.of(translation.metadataEpoch(), translation),
                metadata.schemaChangesById(),
                metadata.backupManifestsById()
        );
    }

    private static final class RecordingAdapterFactory implements RatisAdapterFactory {

        private final ArrayList<RatisPartitionDescriptor> openedPartitions = new ArrayList<>();
        private int reloadCount;
        private Throwable reloadFailure;
        private CompletableFuture<Void> reloadResponse;
        private final CountDownLatch reloadStarted = new CountDownLatch(1);
        private Error closeFailure;
        private boolean closed;

        @Override
        public RatisAdapterBoundary boundary() {
            return RatisAdapterBoundary.externalProcess("recording");
        }

        @Override
        public RatisPartitionClient openPartition(RatisPartitionDescriptor descriptor) {
            openedPartitions.add(descriptor);
            return new RecordingPartitionClient(descriptor);
        }

        @Override
        public CompletableFuture<Void> reloadMetadata() {
            reloadCount++;
            reloadStarted.countDown();
            if (reloadFailure != null) {
                return CompletableFuture.failedFuture(reloadFailure);
            }
            if (reloadResponse == null) {
                return CompletableFuture.completedFuture(null);
            }
            return reloadResponse;
        }

        @Override
        public void close() {
            closed = true;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private record RecordingPartitionClient(RatisPartitionDescriptor descriptor) implements RatisPartitionClient {

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public void close() {
        }
    }
}
