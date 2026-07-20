package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ReplicatedModeConfiguration;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapClusterMetadata;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapManifest;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapManifestStore;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapPlan;
import it.cavallium.rockserver.core.client.ReplicatedConnection;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import static it.cavallium.rockserver.core.common.FutureUtils.awaitInterruptibly;
import static it.cavallium.rockserver.core.common.FutureUtils.unwrapCompletionException;

public final class RatisProcessAdapterServerLifecycleFactory {

    private static final String IMPLEMENTATION_NAME = "apache-ratis-process";
    private static final Duration DEFAULT_STOP_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration DEFAULT_HEALTH_TIMEOUT = Duration.ofSeconds(10);
    private static final String ADAPTER_WORK_DIRECTORY = "adapter-work";
    private static final String ADAPTER_DATA_DIRECTORY = "adapter-data";
    private static final String GENERATED_CONFIG_DIRECTORY = "adapter-process";
    private static final String GENERATED_CONFIG_FILE = "ratis-adapter.properties";
    private static final String GENERATED_METADATA_FILE = "ratis-adapter-metadata.properties";
    private static final String GENERATED_PREFLIGHT_REPORT_FILE = "ratis-adapter-preflight.properties";
    private static final String GENERATED_EVIDENCE_FILE = "ratis-adapter-evidence.properties";

    private RatisProcessAdapterServerLifecycleFactory() {
    }

    public static RatisProcessAdapterLaunchPlan launchPlan(ReplicatedModeConfiguration configuration,
                                                           ClusterMetadata metadata) throws IOException {
        return launchPlan(configuration, metadata, configuredBootstrapPlan(configuration).manifest());
    }

    private static RatisProcessAdapterLaunchPlan launchPlan(ReplicatedModeConfiguration configuration,
                                                            ClusterMetadata metadata,
                                                            BootstrapManifest bootstrapManifest)
            throws IOException {
        requireNonNull(configuration, "configuration");
        requireNonNull(metadata, "metadata");
        requireNonNull(bootstrapManifest, "bootstrapManifest");
        if (!configuration.enabled()) {
            throw new IllegalArgumentException("replication must be enabled before creating a Ratis adapter launch plan");
        }
        var adapter = requireNonNull(configuration.adapterProcess(), "adapterProcess");
        var distribution = RatisProcessAdapterDistributionManifest.read(
                adapter.manifestPath(),
                adapter.adapterHome(),
                adapter.javaExecutable()
        );
        var metadataPath = requireNonNull(configuration.metadataPath(), "metadataPath");
        var dataPath = requireNonNull(configuration.dataPath(), "dataPath");
        var generatedConfigRoot = metadataPath.resolve(GENERATED_CONFIG_DIRECTORY);
        return RatisProcessAdapterLaunchPlan.fromMetadata(
                distribution,
                metadata,
                bootstrapManifest,
                requireNonNull(configuration.nodeId(), "nodeId"),
                new RatisProcessAdapterLaunchPlan.Options(
                        dataPath.resolve(ADAPTER_WORK_DIRECTORY),
                        adapterDataRoot(configuration),
                        generatedConfigRoot.resolve(GENERATED_CONFIG_FILE),
                        generatedConfigRoot.resolve(GENERATED_METADATA_FILE),
                        adapter.healthAddress(),
                        adapter.rpcAddress(),
                        List.of(),
                        Map.of()
                )
        );
    }

    public static RatisProcessAdapterLaunchPlan launchPlan(ReplicatedModeConfiguration configuration,
                                                           BootstrapPlan bootstrapPlan) throws IOException {
        requireNonNull(bootstrapPlan, "bootstrapPlan");
        return launchPlan(configuration, BootstrapClusterMetadata.fromPlan(bootstrapPlan), bootstrapPlan.manifest());
    }

    public static RatisProcessAdapterLaunchPlan bootstrapLaunchPlan(ReplicatedModeConfiguration configuration)
            throws IOException {
        return launchPlan(configuration, configuredBootstrapPlan(configuration));
    }

    public static ClusterMetadata configuredMetadata(ReplicatedModeConfiguration configuration) throws IOException {
        requireEnabled(configuration);
        return RatisProcessAdapterLaunchPlan.withConfigPairLock(
                generatedConfigPath(configuration),
                () -> configuredMetadataLocked(configuration)
        );
    }

    private static ClusterMetadata configuredMetadataLocked(ReplicatedModeConfiguration configuration)
            throws IOException {
        var generatedConfigPath = generatedConfigPath(configuration);
        var generatedMetadataPath = generatedMetadataPath(configuration);
        var configExists = generatedArtifactExists(generatedConfigPath, "generated adapter config");
        var metadataExists = generatedArtifactExists(generatedMetadataPath, "generated adapter metadata");
        if (!configExists && !metadataExists) {
            return BootstrapClusterMetadata.fromPlan(configuredBootstrapPlan(configuration));
        }
        if (!configExists) {
            throw new IOException("generated adapter metadata exists without activated config: " + generatedMetadataPath);
        }
        if (!metadataExists) {
            throw new IOException("generated adapter config exists without metadata sidecar: " + generatedConfigPath);
        }
        var metadataConfig = RatisProcessAdapterMetadataConfig.read(generatedMetadataPath);
        var activeMetadataSha256 = RatisProcessAdapterConfig.readMetadataSha256(generatedConfigPath);
        var metadataSha256 = metadataConfig.semanticSha256();
        if (!activeMetadataSha256.equals(metadataSha256)) {
            throw new IOException("generated adapter config metadata SHA-256 " + activeMetadataSha256
                    + " does not match sidecar SHA-256 " + metadataSha256);
        }
        return metadataConfig.metadata();
    }

    public static RatisProcessAdapterLaunchPlan configuredLaunchPlan(ReplicatedModeConfiguration configuration)
            throws IOException {
        return launchPlan(configuration, configuredMetadata(configuration));
    }

    public static RatisProcessAdapterDeploymentPreflight.Report configuredDeploymentPreflight(
            ReplicatedModeConfiguration configuration) throws IOException {
        return RatisProcessAdapterDeploymentPreflight.check(configuredLaunchPlan(configuration));
    }

    public static RatisProcessAdapterDeploymentPreflight.Evidence configuredDeploymentEvidence(
            ReplicatedModeConfiguration configuration) throws IOException {
        var plan = configuredLaunchPlan(configuration);
        RatisProcessAdapterDeploymentPreflight.verify(plan);
        plan.writeConfig();
        return RatisProcessAdapterDeploymentPreflight.evidence(plan);
    }

    public static Path writeConfiguredDeploymentEvidence(ReplicatedModeConfiguration configuration)
            throws IOException {
        var evidencePath = generatedEvidencePath(configuration);
        configuredDeploymentEvidence(configuration).write(evidencePath);
        return evidencePath;
    }

    public static Path writeConfiguredDeploymentPreflightReport(ReplicatedModeConfiguration configuration)
            throws IOException {
        var reportPath = generatedPreflightReportPath(configuration);
        RatisProcessAdapterDeploymentPreflight.writeReport(configuredLaunchPlan(configuration), reportPath);
        return reportPath;
    }

    public static void verifyConfiguredDeploymentPreflight(ReplicatedModeConfiguration configuration)
            throws IOException {
        RatisProcessAdapterDeploymentPreflight.verify(configuredLaunchPlan(configuration));
    }

    public static RatisServerLifecycle sharedLifecycle(ReplicatedModeConfiguration configuration,
                                                       ClusterMetadata metadata) throws IOException {
        return sharedLifecycle(configuration, metadata, DEFAULT_STOP_TIMEOUT, DEFAULT_HEALTH_TIMEOUT);
    }

    public static RatisServerLifecycle sharedLifecycle(ReplicatedModeConfiguration configuration,
                                                       BootstrapPlan bootstrapPlan) throws IOException {
        requireNonNull(bootstrapPlan, "bootstrapPlan");
        var plan = launchPlan(configuration, BootstrapClusterMetadata.fromPlan(bootstrapPlan), bootstrapPlan.manifest());
        return sharedLifecycle(configuration, plan, DEFAULT_STOP_TIMEOUT, DEFAULT_HEALTH_TIMEOUT);
    }

    public static RatisServerLifecycle sharedBootstrapLifecycle(ReplicatedModeConfiguration configuration)
            throws IOException {
        return sharedLifecycle(configuration, configuredBootstrapPlan(configuration));
    }

    public static RatisServerLifecycle configuredSharedLifecycle(ReplicatedModeConfiguration configuration)
            throws IOException {
        return sharedLifecycle(configuration, configuredMetadata(configuration));
    }

    public static RatisServerLifecycle sharedLifecycle(ReplicatedModeConfiguration configuration,
                                                       ClusterMetadata metadata,
                                                       Duration stopTimeout,
                                                       Duration healthTimeout) throws IOException {
        var plan = launchPlan(configuration, metadata);
        return sharedLifecycle(configuration, plan, stopTimeout, healthTimeout);
    }

    public static RatisAdapterFactory adapterFactory(ReplicatedModeConfiguration configuration,
                                                     ClusterMetadata metadata,
                                                     Duration rpcTimeout) throws IOException {
        return launchPlan(configuration, metadata).adapterFactory(IMPLEMENTATION_NAME, rpcTimeout);
    }

    public static RatisAdapterFactory adapterFactory(ReplicatedModeConfiguration configuration,
                                                     BootstrapPlan bootstrapPlan,
                                                     Duration rpcTimeout) throws IOException {
        requireNonNull(bootstrapPlan, "bootstrapPlan");
        return launchPlan(configuration, BootstrapClusterMetadata.fromPlan(bootstrapPlan), bootstrapPlan.manifest())
                .adapterFactory(IMPLEMENTATION_NAME, rpcTimeout);
    }

    public static RatisAdapterFactory bootstrapAdapterFactory(ReplicatedModeConfiguration configuration,
                                                              Duration rpcTimeout) throws IOException {
        return adapterFactory(configuration, configuredBootstrapPlan(configuration), rpcTimeout);
    }

    public static RatisAdapterFactory configuredAdapterFactory(ReplicatedModeConfiguration configuration,
                                                               Duration rpcTimeout) throws IOException {
        return adapterFactory(configuration, configuredMetadata(configuration), rpcTimeout);
    }

    public static ReplicatedConnection replicatedConnection(String name,
                                                            ReplicatedModeConfiguration configuration,
                                                            ClusterMetadata metadata,
                                                            Duration rpcTimeout) throws IOException {
        var plan = launchPlan(configuration, metadata);
        return replicatedConnection(name, configuration, metadata, rpcTimeout, plan);
    }

    public static ReplicatedConnection bootstrapReplicatedConnection(String name,
                                                                     ReplicatedModeConfiguration configuration,
                                                                     Duration rpcTimeout) throws IOException {
        var plan = bootstrapLaunchPlan(configuration);
        return replicatedConnection(name, configuration, plan.metadataConfig().metadata(), rpcTimeout, plan);
    }

    public static ReplicatedConnection configuredReplicatedConnection(String name,
                                                                      ReplicatedModeConfiguration configuration,
                                                                      Duration rpcTimeout) throws IOException {
        var plan = configuredLaunchPlan(configuration);
        return replicatedConnection(name, configuration, plan.metadataConfig().metadata(), rpcTimeout, plan);
    }

    private static ReplicatedConnection replicatedConnection(String name,
                                                             ReplicatedModeConfiguration configuration,
                                                             ClusterMetadata metadata,
                                                             Duration rpcTimeout,
                                                             RatisProcessAdapterLaunchPlan plan) {
        var adapterFactory = plan.adapterFactory(IMPLEMENTATION_NAME, rpcTimeout);
        var resolver = RatisAdapterClientResolver.fromLaunchPlan(
                adapterFactory,
                plan
        );
        return transferResolverOwnership(resolver, () ->
                new ReplicatedConnection(name, metadata, resolver, resolver, resolver, resolver,
                        metadataRefreshListener(configuration, adapterFactory, resolver),
                        metadataSubmitter(resolver)));
    }

    @VisibleForTesting
    public static ReplicatedConnection transferResolverOwnership(RatisAdapterClientResolver resolver,
                                                                 Supplier<ReplicatedConnection> connectionFactory) {
        requireNonNull(resolver, "resolver");
        requireNonNull(connectionFactory, "connectionFactory");
        try {
            return requireNonNull(connectionFactory.get(), "connectionFactory result");
        } catch (RuntimeException | Error e) {
            try {
                resolver.close();
            } catch (RuntimeException | Error closeFailure) {
                suppress(e, closeFailure);
            }
            throw e;
        }
    }

    public static Path generatedConfigPath(ReplicatedModeConfiguration configuration) {
        return requireNonNull(requireNonNull(configuration, "configuration").metadataPath(), "metadataPath")
                .resolve(GENERATED_CONFIG_DIRECTORY)
                .resolve(GENERATED_CONFIG_FILE);
    }

    public static Path generatedMetadataPath(ReplicatedModeConfiguration configuration) {
        return requireNonNull(requireNonNull(configuration, "configuration").metadataPath(), "metadataPath")
                .resolve(GENERATED_CONFIG_DIRECTORY)
                .resolve(GENERATED_METADATA_FILE);
    }

    public static Path generatedEvidencePath(ReplicatedModeConfiguration configuration) {
        return requireNonNull(requireNonNull(configuration, "configuration").metadataPath(), "metadataPath")
                .resolve(GENERATED_CONFIG_DIRECTORY)
                .resolve(GENERATED_EVIDENCE_FILE);
    }

    public static Path generatedPreflightReportPath(ReplicatedModeConfiguration configuration) {
        return requireNonNull(requireNonNull(configuration, "configuration").metadataPath(), "metadataPath")
                .resolve(GENERATED_CONFIG_DIRECTORY)
                .resolve(GENERATED_PREFLIGHT_REPORT_FILE);
    }

    private static BootstrapPlan configuredBootstrapPlan(ReplicatedModeConfiguration configuration)
            throws IOException {
        requireEnabled(configuration);
        var manifest = BootstrapManifestStore.read(
                requireNonNull(configuration.bootstrapManifestPath(), "bootstrapManifestPath")
        );
        return BootstrapPlan.newCluster(manifest);
    }

    private static void requireEnabled(ReplicatedModeConfiguration configuration) {
        requireNonNull(configuration, "configuration");
        if (!configuration.enabled()) {
            throw new IllegalArgumentException("replication must be enabled before creating a Ratis adapter launch plan");
        }
    }

    public static synchronized void refreshAdapterMetadata(ReplicatedModeConfiguration configuration,
                                                           ClusterMetadata metadata,
                                                           RatisAdapterFactory adapterFactory,
                                                           RatisAdapterClientResolver resolver) {
        var resolvedAdapterFactory = requireNonNull(adapterFactory, "adapterFactory");
        var resolvedResolver = requireNonNull(resolver, "resolver");
        try {
            var plan = launchPlan(configuration, metadata);
            RatisProcessAdapterDeploymentPreflight.verify(plan);
            plan.writeConfig(forwardMetadataPublicationAdmission(configuration, plan), () -> {
                awaitInterruptibly(resolvedAdapterFactory.reloadMetadata(), "Ratis adapter metadata reload");
                resolvedResolver.updatePartitions(plan.adapterConfig().partitions());
            });
        } catch (IOException e) {
            throw new UncheckedIOException("failed to publish Ratis adapter metadata", e);
        } catch (CompletionException e) {
            throw unwrapCompletionException(e, "Ratis adapter metadata reload failed");
        }
    }

    private static RatisProcessAdapterLaunchPlan.ConfigWriteAdmission forwardMetadataPublicationAdmission(
            ReplicatedModeConfiguration configuration,
            RatisProcessAdapterLaunchPlan plan) {
        return publishedMetadata -> requireForwardMetadataPublication(
                configuration,
                plan.metadataConfig().metadata(),
                plan.adapterConfig(),
                publishedMetadata
        );
    }

    private static void requireForwardMetadataPublication(ReplicatedModeConfiguration configuration,
                                                          ClusterMetadata metadata,
                                                          RatisProcessAdapterConfig adapterConfig,
                                                          @Nullable ClusterMetadata publishedMetadata)
            throws IOException {
        if (publishedMetadata == null) {
            return;
        }
        if (!publishedMetadata.clusterId().equals(metadata.clusterId())) {
            throw new IllegalArgumentException("Ratis adapter metadata refresh cannot switch cluster id from "
                    + publishedMetadata.clusterId() + " to " + metadata.clusterId());
        }
        if (metadata.epoch() < publishedMetadata.epoch()) {
            throw new IllegalArgumentException("Ratis adapter metadata epoch regression from "
                    + publishedMetadata.epoch() + " to " + metadata.epoch());
        }
        if (metadata.epoch() == publishedMetadata.epoch() && !metadata.equals(publishedMetadata)) {
            var publishedAdapterConfig = RatisProcessAdapterConfig.fromMetadata(
                    publishedMetadata,
                    configuredBootstrapPlan(configuration).manifest(),
                    requireNonNull(configuration.nodeId(), "nodeId"),
                    adapterDataRoot(configuration)
            );
            if (!publishedAdapterConfig.equals(adapterConfig)) {
                throw new IllegalArgumentException("Ratis adapter partition config changed without advancing "
                        + "metadata epoch " + metadata.epoch());
            }
        }
    }

    private static RatisServerLifecycle sharedLifecycle(ReplicatedModeConfiguration configuration,
                                                        RatisProcessAdapterLaunchPlan plan,
                                                        Duration stopTimeout,
                                                        Duration healthTimeout) {
        requireNonNull(configuration, "configuration");
        requireNonNull(plan, "plan");
        return new SharedRatisServerLifecycle(plan.lifecycle(
                IMPLEMENTATION_NAME,
                stopTimeout,
                healthTimeout,
                forwardMetadataPublicationAdmission(configuration, plan)
        ));
    }

    private static void suppress(Throwable failure, Throwable addition) {
        if (failure != addition) {
            failure.addSuppressed(addition);
        }
    }

    private static boolean generatedArtifactExists(Path path, String description) throws IOException {
        try {
            var attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isRegularFile()) {
                throw new IOException(description + " path must be a regular file: " + path);
            }
            return true;
        } catch (NoSuchFileException _) {
            return false;
        }
    }

    private static Consumer<ClusterMetadata> metadataRefreshListener(ReplicatedModeConfiguration configuration,
                                                                     RatisAdapterFactory adapterFactory,
                                                                     RatisAdapterClientResolver resolver) {
        return metadata -> refreshAdapterMetadata(configuration, metadata, adapterFactory, resolver);
    }

    private static RatisMetadataCommandSubmitter metadataSubmitter(RatisAdapterClientResolver resolver) {
        return new RatisMetadataCommandSubmitter(resolver.client(RatisProcessAdapterConfig.METADATA_GROUP_PARTITION_ID));
    }

    private static Path adapterDataRoot(ReplicatedModeConfiguration configuration) {
        return requireNonNull(requireNonNull(configuration, "configuration").dataPath(), "dataPath")
                .resolve(ADAPTER_DATA_DIRECTORY);
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
