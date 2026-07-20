package it.cavallium.rockserver.ratis;

import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterHealth;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterHealthCodec;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterLaunchPlan;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterMetadataConfig;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcHandler;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterSocketHealthServer;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterSocketRpcServer;
import it.cavallium.rockserver.core.replication.ratis.RatisFeatureUnavailableException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

public final class AdapterMain {

    static final Duration RPC_TIMEOUT = Duration.ofSeconds(10);

    private AdapterMain() {
    }

    public static void main(String[] args) throws IOException {
        var config = AdapterConfig.parse(args);
        if (config.printHealth()) {
            System.out.print(RatisProcessAdapterHealthCodec.encode(RatisProcessAdapterHealth.ready()));
            return;
        }

        try (var resources = new AdapterResourceScope()) {
            var runtime = resources.add(adapterRuntime(config));
            var partitionClients = runtime == null
                    ? partitionClients(config, resources)
                    : new LinkedHashMap<String, RatisClasspathPartitionClient>();
            if (runtime != null) {
                runtime.start();
            }
            var rpcServer = resources.add(rpcServer(config, partitionId -> {
                if (runtime != null) {
                    return runtime.borrowedClient(partitionId);
                }
                var client = partitionClients.get(partitionId);
                if (client == null) {
                    throw new IllegalArgumentException("Unknown adapter partition: " + partitionId);
                }
                return client;
            }, runtime == null ? null : () -> reloadRuntime(config, runtime)));
            if (rpcServer != null) {
                rpcServer.start();
                System.out.println("rockserver-ratis-adapter rpc-port=" + rpcServer.boundAddress().getPort());
            }

            var healthCheckedRpcServer = rpcServer;
            var healthServer = resources.add(new RatisProcessAdapterSocketHealthServer(
                     new InetSocketAddress(config.healthHost(), config.healthPort()),
                     Duration.ofSeconds(10),
                     () -> adapterHealth(runtime, healthCheckedRpcServer)
             ));
            healthServer.start();
            System.out.println("rockserver-ratis-adapter health-port=" + healthServer.boundAddress().getPort());
            healthServer.awaitClosed();
        }
    }

    private static RatisProcessAdapterHealth adapterHealth(RatisClasspathAdapterRuntime runtime,
                                                           RatisProcessAdapterSocketRpcServer rpcServer) {
        var rpcHealth = rpcServer == null ? RatisProcessAdapterHealth.ready() : rpcServer.health();
        if (!rpcHealth.isReady()) {
            return rpcHealth;
        }
        return runtime == null ? rpcHealth : runtime.health();
    }

    private static RatisClasspathAdapterRuntime adapterRuntime(AdapterConfig config) throws IOException {
        if (config.configPath() == null || config.metadataPath() == null) {
            return null;
        }
        var snapshot = runtimeSnapshot(config);
        return new RatisClasspathAdapterRuntime(snapshot.partitions(), snapshot.stateMachineFactory());
    }

    private static CompletableFuture<Void> reloadRuntime(AdapterConfig config, RatisClasspathAdapterRuntime runtime) {
        try {
            var snapshot = runtimeSnapshot(config);
            runtime.reload(snapshot.partitions(), snapshot.stateMachineFactory());
            return CompletableFuture.completedFuture(null);
        } catch (IOException | RuntimeException | Error e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static RuntimeSnapshot runtimeSnapshot(AdapterConfig config) throws IOException {
        if (config.configPath() == null || config.metadataPath() == null) {
            throw new RatisFeatureUnavailableException("adapter metadata reload requires config and metadata paths");
        }
        var adapterConfig = adapterConfig(config.configPath());
        var metadataConfig = RatisProcessAdapterMetadataConfig.read(config.metadataPath());
        var metadata = metadataConfig.metadata();
        adapterConfig.requireMetadataEpoch(metadata.epoch());
        adapterConfig.requireMetadataSha256(metadataConfig.semanticSha256());
        return new RuntimeSnapshot(
                adapterConfig.partitions(),
                new RockserverRatisClasspathStateMachineFactory(metadata)
        );
    }

    private static LinkedHashMap<String, RatisClasspathPartitionClient> partitionClients(AdapterConfig config,
                                                                                          AdapterResourceScope resources)
            throws IOException {
        var clients = new LinkedHashMap<String, RatisClasspathPartitionClient>();
        if (config.configPath() == null) {
            return clients;
        }
        for (var partition : adapterConfig(config.configPath()).partitions()) {
            var client = resources.add(new RatisClasspathPartitionClient(partition));
            if (clients.put(partition.partitionId(), client) != null) {
                throw new IllegalArgumentException("Duplicate adapter partition id: " + partition.partitionId());
            }
        }
        return clients;
    }

    private static RatisClasspathAdapterConfig adapterConfig(Path configPath) throws IOException {
        var adapterConfig = RatisClasspathAdapterConfig.read(configPath);
        adapterConfig.requireLocalPeerId(requiredEnvironment(RatisProcessAdapterLaunchPlan.NODE_ID_ENV));
        adapterConfig.requireDataRoot(Path.of(requiredEnvironment(RatisProcessAdapterLaunchPlan.ADAPTER_DATA_ROOT_ENV)));
        adapterConfig.requireMetadataGroupTopologySha256(
                requiredEnvironment(RatisProcessAdapterLaunchPlan.METADATA_GROUP_TOPOLOGY_SHA256_ENV));
        return adapterConfig;
    }

    private static String requiredEnvironment(String key) {
        var value = System.getenv(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing adapter environment variable: " + key);
        }
        return value;
    }

    private static RatisProcessAdapterSocketRpcServer rpcServer(AdapterConfig config,
                                                                RatisProcessAdapterRpcHandler.PartitionClientResolver resolver,
                                                                RatisProcessAdapterRpcHandler.MetadataReloader metadataReloader) {
        if (config.rpcPort() < 0) {
            return null;
        }
        var handler = metadataReloader == null
                ? new RatisProcessAdapterRpcHandler(resolver)
                : new RatisProcessAdapterRpcHandler(resolver, metadataReloader);
        return new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress(config.rpcHost(), config.rpcPort()),
                handler,
                RPC_TIMEOUT
        );
    }

    private record AdapterConfig(String healthHost,
                                 int healthPort,
                                 String rpcHost,
                                 int rpcPort,
                                 Path configPath,
                                 Path metadataPath,
                                 boolean printHealth) {

        private static AdapterConfig parse(String[] args) {
            var healthHost = "127.0.0.1";
            var healthPort = 0;
            var rpcHost = "127.0.0.1";
            var rpcPort = -1;
            Path configPath = null;
            Path metadataPath = null;
            var printHealth = false;
            for (var arg : args) {
                if (arg.equals("--print-health")) {
                    printHealth = true;
                } else if (arg.startsWith("--health-host=")) {
                    healthHost = value(arg, "--health-host=");
                } else if (arg.startsWith("--health-port=")) {
                    healthPort = port(arg, "--health-port=", 0, 65_535);
                } else if (arg.startsWith("--rpc-host=")) {
                    rpcHost = value(arg, "--rpc-host=");
                } else if (arg.startsWith("--rpc-port=")) {
                    rpcPort = port(arg, "--rpc-port=", -1, 65_535);
                } else if (arg.startsWith("--config=")) {
                    configPath = Path.of(value(arg, "--config="));
                } else if (arg.startsWith("--metadata=")) {
                    metadataPath = Path.of(value(arg, "--metadata="));
                } else {
                    throw new IllegalArgumentException("Unsupported adapter argument: " + arg);
                }
            }
            if (healthHost.isBlank()) {
                throw new IllegalArgumentException("health host must not be blank");
            }
            if (rpcHost.isBlank()) {
                throw new IllegalArgumentException("RPC host must not be blank");
            }
            if (healthPort < 0 || healthPort > 65535) {
                throw new IllegalArgumentException("health port out of range: " + healthPort);
            }
            if (rpcPort < -1 || rpcPort > 65535) {
                throw new IllegalArgumentException("RPC port out of range: " + rpcPort);
            }
            if (configPath != null && rpcPort < 0) {
                throw new IllegalArgumentException("RPC port must be configured when adapter config is provided");
            }
            if (metadataPath != null && configPath == null) {
                throw new IllegalArgumentException("adapter config must be provided when adapter metadata is provided");
            }
            return new AdapterConfig(
                    healthHost.toLowerCase(Locale.ROOT),
                    healthPort,
                    rpcHost.toLowerCase(Locale.ROOT),
                    rpcPort,
                    configPath,
                    metadataPath,
                    printHealth
            );
        }

        private static String value(String arg, String prefix) {
            var value = arg.substring(prefix.length());
            if (value.isBlank()) {
                throw new IllegalArgumentException("Adapter argument must not be blank: " + prefix);
            }
            return value;
        }

        private static int port(String arg, String prefix, int minInclusive, int maxInclusive) {
            var value = value(arg, prefix);
            int parsed;
            try {
                parsed = Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Adapter port argument must be an integer: " + prefix, e);
            }
            if (!Integer.toString(parsed).equals(value)) {
                throw new IllegalArgumentException("Adapter port argument must be canonical: " + prefix);
            }
            if (parsed < minInclusive || parsed > maxInclusive) {
                throw new IllegalArgumentException("Adapter port argument out of range: " + prefix + parsed);
            }
            return parsed;
        }
    }

    private record RuntimeSnapshot(List<RatisPartitionDescriptor> partitions,
                                   RatisClasspathAdapterRuntime.StateMachineFactory stateMachineFactory) {

        private RuntimeSnapshot {
            partitions = List.copyOf(partitions);
        }
    }
}
