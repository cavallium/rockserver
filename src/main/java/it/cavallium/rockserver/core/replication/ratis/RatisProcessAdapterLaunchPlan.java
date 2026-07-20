package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.rockserver.core.cluster.EndpointUris;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.metadata.BootstrapManifest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jetbrains.annotations.Nullable;

public record RatisProcessAdapterLaunchPlan(RatisProcessAdapterConfig adapterConfig,
                                            RatisProcessAdapterMetadataConfig metadataConfig,
                                            Path adapterHome,
                                            Path adapterDataRoot,
                                            Path configPath,
                                            Path metadataPath,
                                            RatisProcessAdapterCommand command,
                                            InetSocketAddress healthAddress,
                                            InetSocketAddress rpcAddress) {

    public static final String NODE_ID_ENV = "ROCKSERVER_NODE_ID";
    public static final String ADAPTER_DATA_ROOT_ENV = "ROCKSERVER_RATIS_DATA_ROOT";
    public static final String METADATA_GROUP_TOPOLOGY_SHA256_ENV =
            "ROCKSERVER_RATIS_METADATA_GROUP_TOPOLOGY_SHA256";
    public static final String CONFIG_PATH_ENV = "ROCKSERVER_RATIS_CONFIG";
    public static final String METADATA_PATH_ENV = "ROCKSERVER_RATIS_METADATA";
    private static final String CONFIG_PAIR_LOCK_SUFFIX = ".lock";
    private static final ConcurrentMap<Path, JvmConfigPairLock> CONFIG_PAIR_JVM_LOCKS = new ConcurrentHashMap<>();
    private static final Set<String> RESERVED_ENVIRONMENT_KEYS = Set.of(
            NODE_ID_ENV,
            ADAPTER_DATA_ROOT_ENV,
            METADATA_GROUP_TOPOLOGY_SHA256_ENV,
            CONFIG_PATH_ENV,
            METADATA_PATH_ENV
    );

    public RatisProcessAdapterLaunchPlan {
        requireNonNull(adapterConfig, "adapterConfig");
        requireNonNull(metadataConfig, "metadataConfig");
        RatisAdapterBoundary.requireSingleLinePath(adapterHome, "adapterHome");
        RatisAdapterBoundary.requireSingleLinePath(adapterDataRoot, "adapterDataRoot");
        var requiredConfigPath = RatisAdapterBoundary.requireSingleLinePath(configPath, "configPath");
        var requiredMetadataPath = RatisAdapterBoundary.requireSingleLinePath(metadataPath, "metadataPath");
        if (configPairLockPath(requiredConfigPath).equals(requiredMetadataPath.toAbsolutePath().normalize())) {
            throw new IllegalArgumentException("metadataPath must not equal config-pair lock path: " + metadataPath);
        }
        requireNonNull(command, "command");
        EndpointUris.requireSocketAddress(healthAddress, "healthAddress");
        EndpointUris.requireSocketAddress(rpcAddress, "rpcAddress");
    }

    public static RatisProcessAdapterLaunchPlan fromMetadata(RatisProcessAdapterDistribution distribution,
                                                             ClusterMetadata metadata,
                                                             NodeId localNodeId,
                                                             Options options) {
        requireNonNull(distribution, "distribution");
        requireNonNull(localNodeId, "localNodeId");
        requireNonNull(options, "options");
        rejectReservedEnvironment(distribution.environment(), "distribution environment");
        var adapterConfig = RatisProcessAdapterConfig.fromMetadata(
                metadata,
                localNodeId,
                options.adapterDataRoot()
        );
        return fromAdapterConfig(distribution, metadata, localNodeId, options, adapterConfig);
    }

    public static RatisProcessAdapterLaunchPlan fromMetadata(RatisProcessAdapterDistribution distribution,
                                                             ClusterMetadata metadata,
                                                             BootstrapManifest bootstrapManifest,
                                                             NodeId localNodeId,
                                                             Options options) {
        requireNonNull(distribution, "distribution");
        requireNonNull(localNodeId, "localNodeId");
        requireNonNull(options, "options");
        rejectReservedEnvironment(distribution.environment(), "distribution environment");
        var adapterConfig = RatisProcessAdapterConfig.fromMetadata(
                metadata,
                bootstrapManifest,
                localNodeId,
                options.adapterDataRoot()
        );
        return fromAdapterConfig(distribution, metadata, localNodeId, options, adapterConfig);
    }

    private static RatisProcessAdapterLaunchPlan fromAdapterConfig(RatisProcessAdapterDistribution distribution,
                                                                   ClusterMetadata metadata,
                                                                   NodeId localNodeId,
                                                                   Options options,
                                                                   RatisProcessAdapterConfig adapterConfig) {
        requireNonNull(adapterConfig, "adapterConfig");
        var metadataConfig = new RatisProcessAdapterMetadataConfig(metadata);
        var command = distribution.toCommand(
                options.workingDirectory(),
                options.jvmArguments(),
                programArguments(options),
                environment(localNodeId, adapterConfig, options)
        );
        return new RatisProcessAdapterLaunchPlan(
                adapterConfig,
                metadataConfig,
                distribution.adapterHome(),
                options.adapterDataRoot(),
                options.configPath(),
                options.metadataPath(),
                command,
                options.healthAddress(),
                options.rpcAddress()
        );
    }

    public void writeConfig() throws IOException {
        writeConfig(this::requireSameEpochMetadataMatch, () -> {
        });
    }

    public void writeConfig(ConfigWriteAdmission admission) throws IOException {
        writeConfig(admission, () -> {
        });
    }

    public void writeConfig(ConfigWriteAdmission admission, ConfigWriteActivation activation) throws IOException {
        var resolvedAdmission = requireNonNull(admission, "admission");
        var resolvedActivation = requireNonNull(activation, "activation");
        withConfigPairLock(configPath, () -> {
            var publishedMetadata = publishedMetadata();
            requireNonRegressiveMetadataPublication(publishedMetadata);
            resolvedAdmission.verify(publishedMetadata);
            metadataConfig.write(metadataPath);
            adapterConfig.write(configPath, metadataConfig.semanticSha256());
            resolvedActivation.activate();
            return null;
        });
    }

    static <T> T withConfigPairLock(Path configPath, ConfigPairOperation<T> operation) throws IOException {
        requireNonNull(operation, "operation");
        var lockPath = configPairLockPath(configPath);
        var jvmLock = retainConfigPairJvmLock(lockPath);
        try {
            synchronized (jvmLock) {
                RatisProcessAdapterFileWriter.createDirectoriesDurably(lockPath.getParent());
                // The path-scoped monitor prevents overlapping JVM locks; the file lock coordinates distinct processes.
                try (var lockChannel = FileChannel.open(lockPath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        LinkOption.NOFOLLOW_LINKS);
                     var _ = lockChannel.lock()) {
                    return operation.run();
                }
            }
        } finally {
            releaseConfigPairJvmLock(lockPath, jvmLock);
        }
    }

    private static JvmConfigPairLock retainConfigPairJvmLock(Path lockPath) {
        return CONFIG_PAIR_JVM_LOCKS.compute(lockPath, (_, lock) -> lock == null ? new JvmConfigPairLock() : lock.retain());
    }

    private static void releaseConfigPairJvmLock(Path lockPath, JvmConfigPairLock lock) {
        CONFIG_PAIR_JVM_LOCKS.compute(lockPath, (_, registeredLock) -> {
            if (registeredLock != lock) {
                throw new IllegalStateException("config-pair JVM lock registration changed unexpectedly: " + lockPath);
            }
            return lock.release() ? null : lock;
        });
    }

    private static final class JvmConfigPairLock {

        private int users = 1;

        private JvmConfigPairLock retain() {
            users = Math.incrementExact(users);
            return this;
        }

        private boolean release() {
            if (users <= 0) {
                throw new IllegalStateException("config-pair JVM lock released without retained user");
            }
            return --users == 0;
        }
    }

    private @Nullable ClusterMetadata publishedMetadata() throws IOException {
        try {
            var attributes = Files.readAttributes(metadataPath, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isRegularFile()) {
                throw new IOException("generated adapter metadata path must be a regular file: " + metadataPath);
            }
            return RatisProcessAdapterMetadataConfig.read(metadataPath).metadata();
        } catch (NoSuchFileException _) {
            return null;
        }
    }

    private void requireNonRegressiveMetadataPublication(@Nullable ClusterMetadata publishedMetadata) {
        if (publishedMetadata == null) {
            return;
        }
        var planned = metadataConfig.metadata();
        if (!publishedMetadata.clusterId().equals(planned.clusterId())) {
            throw new IllegalArgumentException("Ratis adapter metadata refresh cannot switch cluster id from "
                    + publishedMetadata.clusterId() + " to " + planned.clusterId());
        }
        if (planned.epoch() < publishedMetadata.epoch()) {
            throw new IllegalArgumentException("Ratis adapter metadata epoch regression from "
                    + publishedMetadata.epoch() + " to " + planned.epoch());
        }
    }

    private void requireSameEpochMetadataMatch(@Nullable ClusterMetadata publishedMetadata) {
        if (publishedMetadata == null) {
            return;
        }
        var planned = metadataConfig.metadata();
        if (planned.epoch() == publishedMetadata.epoch() && !planned.equals(publishedMetadata)) {
            throw new IllegalArgumentException("Ratis adapter metadata changed without advancing metadata epoch "
                    + planned.epoch());
        }
    }

    private static Path configPairLockPath(Path configPath) {
        var normalizedConfigPath = requireNonNull(configPath, "configPath").toAbsolutePath().normalize();
        var fileName = normalizedConfigPath.getFileName();
        if (fileName == null) {
            throw new IllegalArgumentException("configPath must name a file: " + configPath);
        }
        return normalizedConfigPath.resolveSibling(fileName + CONFIG_PAIR_LOCK_SUFFIX);
    }

    public Properties adapterConfigProperties() {
        return adapterConfig.toProperties(metadataConfig.semanticSha256());
    }

    public RatisServerLifecycle lifecycle(String implementationName,
                                          Duration stopTimeout,
                                          Duration healthTimeout) {
        return lifecycle(implementationName, stopTimeout, healthTimeout, this::requireSameEpochMetadataMatch);
    }

    public RatisServerLifecycle lifecycle(String implementationName,
                                          Duration stopTimeout,
                                          Duration healthTimeout,
                                          ConfigWriteAdmission admission) {
        return lifecycle(
                implementationName,
                stopTimeout,
                RatisProcessLauncher.defaultLauncher(),
                new RatisProcessAdapterSocketHealthProbe(healthAddress, healthTimeout),
                admission
        );
    }

    public RatisServerLifecycle lifecycle(String implementationName,
                                          Duration stopTimeout,
                                          RatisProcessLauncher launcher,
                                          RatisProcessAdapterHealthProbe healthProbe) {
        return lifecycle(implementationName, stopTimeout, launcher, healthProbe, this::requireSameEpochMetadataMatch);
    }

    public RatisServerLifecycle lifecycle(String implementationName,
                                          Duration stopTimeout,
                                          RatisProcessLauncher launcher,
                                          RatisProcessAdapterHealthProbe healthProbe,
                                          ConfigWriteAdmission admission) {
        var processLifecycle = new RatisProcessAdapterLifecycle(
                implementationName,
                command,
                stopTimeout,
                launcher,
                healthProbe
        );
        return new ConfigWritingLifecycle(this, processLifecycle, admission);
    }

    public RatisAdapterFactory adapterFactory(String implementationName, Duration rpcTimeout) {
        return new RatisProcessAdapterClientFactory(
                implementationName,
                new RatisProcessAdapterSocketRpcTransport(rpcAddress, rpcTimeout)
        );
    }

    private static List<String> programArguments(Options options) {
        return List.of(
                "--config=" + processVisiblePath(options.configPath()),
                "--metadata=" + processVisiblePath(options.metadataPath()),
                "--health-host=" + options.healthAddress().getHostString(),
                "--health-port=" + options.healthAddress().getPort(),
                "--rpc-host=" + options.rpcAddress().getHostString(),
                "--rpc-port=" + options.rpcAddress().getPort()
        );
    }

    private static Map<String, String> environment(NodeId localNodeId,
                                                   RatisProcessAdapterConfig adapterConfig,
                                                   Options options) {
        var environment = new LinkedHashMap<>(options.environmentOverrides());
        environment.put(NODE_ID_ENV, localNodeId.value());
        environment.put(ADAPTER_DATA_ROOT_ENV, processVisiblePath(options.adapterDataRoot()).toString());
        environment.put(METADATA_GROUP_TOPOLOGY_SHA256_ENV, adapterConfig.metadataGroupTopologySha256());
        environment.put(CONFIG_PATH_ENV, processVisiblePath(options.configPath()).toString());
        environment.put(METADATA_PATH_ENV, processVisiblePath(options.metadataPath()).toString());
        return Map.copyOf(environment);
    }

    private static Path processVisiblePath(Path path) {
        return path.toAbsolutePath().normalize();
    }

    public record Options(Path workingDirectory,
                          Path adapterDataRoot,
                          Path configPath,
                          Path metadataPath,
                          InetSocketAddress healthAddress,
                          InetSocketAddress rpcAddress,
                          List<String> jvmArguments,
                          Map<String, String> environmentOverrides) {

        public Options(Path workingDirectory,
                       Path adapterDataRoot,
                       Path configPath,
                       InetSocketAddress healthAddress,
                       InetSocketAddress rpcAddress,
                       List<String> jvmArguments,
                       Map<String, String> environmentOverrides) {
            this(
                    workingDirectory,
                    adapterDataRoot,
                    configPath,
                    defaultMetadataPath(configPath),
                    healthAddress,
                    rpcAddress,
                    jvmArguments,
                    environmentOverrides
            );
        }

        public Options {
            RatisAdapterBoundary.requireSingleLinePath(workingDirectory, "workingDirectory");
            RatisAdapterBoundary.requireSingleLinePath(adapterDataRoot, "adapterDataRoot");
            RatisAdapterBoundary.requireSingleLinePath(configPath, "configPath");
            RatisAdapterBoundary.requireSingleLinePath(metadataPath, "metadataPath");
            EndpointUris.requireSocketAddress(healthAddress, "healthAddress");
            EndpointUris.requireSocketAddress(rpcAddress, "rpcAddress");
            jvmArguments = copyJvmArguments(jvmArguments);
            environmentOverrides = copyEnvironment(environmentOverrides);
            rejectReservedEnvironment(environmentOverrides, "environmentOverrides");
        }

        private static Path defaultMetadataPath(Path configPath) {
            requireNonNull(configPath, "configPath");
            var fileName = configPath.getFileName();
            if (fileName == null) {
                return Path.of("ratis-adapter-metadata.properties");
            }
            return configPath.resolveSibling(fileName + ".metadata");
        }
    }

    private static void rejectReservedEnvironment(Map<String, String> environment, String name) {
        var resolvedEnvironment = requireNonNull(environment, name);
        for (var entry : resolvedEnvironment.entrySet()) {
            RatisAdapterBoundary.requireEnvironmentKey(entry.getKey(), name + " key");
            RatisAdapterBoundary.requireEnvironmentValue(entry.getValue(), name + " value");
        }
        for (var key : RESERVED_ENVIRONMENT_KEYS) {
            if (resolvedEnvironment.containsKey(key)) {
                throw new IllegalArgumentException(name + " must not override reserved adapter environment key: " + key);
            }
        }
    }

    private static List<String> copyJvmArguments(List<String> jvmArguments) {
        var resolvedJvmArguments = requireNonNull(jvmArguments, "jvmArguments");
        for (var jvmArgument : resolvedJvmArguments) {
            if (jvmArgument == null) {
                throw new IllegalArgumentException("jvmArguments entry must not be null");
            }
        }
        return List.copyOf(resolvedJvmArguments);
    }

    private static Map<String, String> copyEnvironment(Map<String, String> environment) {
        var name = "environmentOverrides";
        var resolvedEnvironment = requireNonNull(environment, name);
        var copy = new LinkedHashMap<String, String>();
        for (var entry : resolvedEnvironment.entrySet()) {
            var key = RatisAdapterBoundary.requireEnvironmentKey(entry.getKey(), name + " key");
            var value = RatisAdapterBoundary.requireEnvironmentValue(entry.getValue(), name + " value");
            copy.put(key, value);
        }
        return Map.copyOf(copy);
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private record ConfigWritingLifecycle(RatisProcessAdapterLaunchPlan plan,
                                          RatisProcessAdapterLifecycle processLifecycle,
                                          ConfigWriteAdmission admission) implements RatisServerLifecycle {

        private ConfigWritingLifecycle {
            requireNonNull(plan, "plan");
            requireNonNull(processLifecycle, "processLifecycle");
            requireNonNull(admission, "admission");
        }

        @Override
        public RatisAdapterBoundary boundary() {
            return processLifecycle.boundary();
        }

        @Override
        public void start() throws IOException {
            RatisProcessAdapterDeploymentPreflight.verify(plan);
            plan.writeConfig(admission, processLifecycle::start);
        }

        @Override
        public void close() throws IOException {
            processLifecycle.close();
        }
    }

    @FunctionalInterface
    public interface ConfigWriteAdmission {

        void verify(@Nullable ClusterMetadata publishedMetadata) throws IOException;
    }

    @FunctionalInterface
    public interface ConfigWriteActivation {

        void activate() throws IOException;
    }

    @FunctionalInterface
    interface ConfigPairOperation<T> {

        T run() throws IOException;
    }
}
