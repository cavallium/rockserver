package it.cavallium.rockserver.core.cluster;

import it.cavallium.rockserver.core.config.DatabaseConfig;
import it.cavallium.rockserver.core.config.ReplicationConfig;
import it.cavallium.rockserver.core.config.ReplicationMetadataPeerConfig;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;

public record ReplicatedModeConfiguration(boolean enabled,
                                          @Nullable NodeId nodeId,
                                          @Nullable URI advertisedRatisEndpoint,
                                          @Nullable URI publicApiEndpoint,
                                          @Nullable Path dataPath,
                                          @Nullable Path metadataPath,
                                          @Nullable Path bootstrapManifestPath,
                                          @Nullable String failureDomain,
                                          @Nullable AdapterProcess adapterProcess,
                                          List<MetadataPeer> metadataPeers) {

    public ReplicatedModeConfiguration {
        metadataPeers = copyMetadataPeers(metadataPeers);
        if (enabled) {
            requireEnabledField(nodeId, "nodeId");
            requireEnabledField(advertisedRatisEndpoint, "advertisedRatisEndpoint");
            requireEnabledField(publicApiEndpoint, "publicApiEndpoint");
            requireEnabledField(dataPath, "dataPath");
            requireEnabledField(metadataPath, "metadataPath");
            requireEnabledField(bootstrapManifestPath, "bootstrapManifestPath");
            requireEnabledField(failureDomain, "failureDomain");
            requireEnabledField(adapterProcess, "adapterProcess");
            SingleLineStrings.requireNonBlank(failureDomain, "failureDomain");
            EndpointUris.requireGrpcUri(advertisedRatisEndpoint, "advertisedRatisEndpoint");
            EndpointUris.requireEndpointUri(publicApiEndpoint, "publicApiEndpoint");
            requireSeparateEndpoints(advertisedRatisEndpoint, publicApiEndpoint, "replication");
            if (metadataPeers.isEmpty()) {
                throw new IllegalArgumentException("metadataPeers must not be empty when replication is enabled");
            }
            requireMetadataPeerSet(metadataPeers, nodeId);
        } else if (failureDomain != null) {
            SingleLineStrings.requireNonBlank(failureDomain, "failureDomain");
        }
    }

    public static ReplicatedModeConfiguration disabled() {
        return new ReplicatedModeConfiguration(false, null, null, null, null, null, null, null, null, List.of());
    }

    public static ReplicatedModeConfiguration from(DatabaseConfig databaseConfig) throws GestaltException {
        var requiredDatabaseConfig = requireArgument(databaseConfig, "databaseConfig");
        return from(requiredDatabaseConfig.replication());
    }

    public static ReplicatedModeConfiguration from(ReplicationConfig config) throws GestaltException {
        var requiredConfig = requireArgument(config, "config");
        var metadataPeers = metadataPeers(requiredConfig);
        return new ReplicatedModeConfiguration(
                requiredConfig.enabled(),
                nodeId(requiredConfig.nodeId()),
                uri(requiredConfig.advertisedRatisEndpoint(), "advertised-ratis-endpoint"),
                uri(requiredConfig.publicApiEndpoint(), "public-api-endpoint"),
                requiredConfig.dataPath(),
                requiredConfig.metadataPath(),
                requiredConfig.bootstrapManifestPath(),
                requiredConfig.failureDomain(),
                adapterProcess(requiredConfig),
                metadataPeers
        );
    }

    private static List<MetadataPeer> metadataPeers(ReplicationConfig config) throws GestaltException {
        var peerConfigs = config.metadataPeers();
        if (peerConfigs == null) {
            throw new IllegalArgumentException("replication metadataPeers config must not be null");
        }
        var peers = new ArrayList<MetadataPeer>(peerConfigs.length);
        for (var index = 0; index < peerConfigs.length; index++) {
            var peerConfig = peerConfigs[index];
            if (peerConfig == null) {
                throw new IllegalArgumentException("replication metadataPeers[" + index + "] must not be null");
            }
            peers.add(metadataPeer(peerConfig));
        }
        return peers;
    }

    private static void requireMetadataPeerSet(List<MetadataPeer> metadataPeers, NodeId localNodeId) {
        var nodeIds = new HashSet<NodeId>();
        var ratisEndpoints = new HashSet<URI>();
        var publicApiEndpoints = new HashSet<URI>();
        var includesLocalNode = false;
        for (var peer : metadataPeers) {
            if (!nodeIds.add(peer.nodeId())) {
                throw new IllegalArgumentException("metadataPeers contains duplicate nodeId: " + peer.nodeId());
            }
            if (!ratisEndpoints.add(peer.advertisedRatisEndpoint())) {
                throw new IllegalArgumentException("metadataPeers contains duplicate advertisedRatisEndpoint: "
                        + peer.advertisedRatisEndpoint());
            }
            if (!publicApiEndpoints.add(peer.publicApiEndpoint())) {
                throw new IllegalArgumentException("metadataPeers contains duplicate publicApiEndpoint: "
                        + peer.publicApiEndpoint());
            }
            if (peer.nodeId().equals(localNodeId)) {
                includesLocalNode = true;
            }
        }
        if (!includesLocalNode) {
            throw new IllegalArgumentException("metadataPeers must include local nodeId when replication is enabled: "
                    + localNodeId);
        }
    }

    @Nullable
    private static AdapterProcess adapterProcess(ReplicationConfig config) throws GestaltException {
        var adapterHome = config.adapterHome();
        var manifestPath = config.adapterManifestPath();
        var javaExecutable = config.adapterJavaExecutable();
        var healthEndpoint = config.adapterHealthEndpoint();
        var rpcEndpoint = config.adapterRpcEndpoint();
        if (adapterHome == null
                && manifestPath == null
                && javaExecutable == null
                && healthEndpoint == null
                && rpcEndpoint == null) {
            return null;
        }
        return new AdapterProcess(
                adapterHome,
                manifestPath,
                javaExecutable,
                socketAddress(healthEndpoint, "adapter-health-endpoint"),
                socketAddress(rpcEndpoint, "adapter-rpc-endpoint")
        );
    }

    private static MetadataPeer metadataPeer(ReplicationMetadataPeerConfig peerConfig) {
        try {
            return new MetadataPeer(
                    NodeId.of(requireNonBlank(peerConfig.nodeId(), "metadata peer node-id")),
                    uri(requireNonBlank(peerConfig.advertisedRatisEndpoint(),
                                    "metadata peer advertised-ratis-endpoint"),
                            "metadata peer advertised-ratis-endpoint"),
                    uri(requireNonBlank(peerConfig.publicApiEndpoint(), "metadata peer public-api-endpoint"),
                            "metadata peer public-api-endpoint"),
                    SingleLineStrings.requireNonBlank(peerConfig.failureDomain(), "metadata peer failure-domain")
            );
        } catch (GestaltException e) {
            throw new IllegalArgumentException("Invalid metadata peer replication config", e);
        }
    }

    @Nullable
    private static NodeId nodeId(@Nullable String value) {
        return value == null ? null : NodeId.of(requireNonBlank(value, "node-id"));
    }

    @Nullable
    private static URI uri(@Nullable String value, String fieldName) {
        return value == null ? null : URI.create(requireNonBlank(value, fieldName));
    }

    private static String requireNonBlank(@Nullable String value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }

    private static void requireEnabledField(@Nullable Object value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException("replication enabled requires " + fieldName);
        }
    }

    public record MetadataPeer(NodeId nodeId,
                               URI advertisedRatisEndpoint,
                               URI publicApiEndpoint,
                               String failureDomain) {

        public MetadataPeer {
            var requiredNodeId = requireArgument(nodeId, "nodeId");
            EndpointUris.requireGrpcUri(advertisedRatisEndpoint,
                    "metadata peer " + requiredNodeId.value() + " advertisedRatisEndpoint");
            EndpointUris.requireEndpointUri(publicApiEndpoint,
                    "metadata peer " + requiredNodeId.value() + " publicApiEndpoint");
            requireSeparateEndpoints(advertisedRatisEndpoint, publicApiEndpoint,
                    "metadata peer " + requiredNodeId.value());
            SingleLineStrings.requireNonBlank(failureDomain, "failureDomain");
        }
    }

    public record AdapterProcess(Path adapterHome,
                                 Path manifestPath,
                                 Path javaExecutable,
                                 InetSocketAddress healthAddress,
                                 InetSocketAddress rpcAddress) {

        public AdapterProcess {
            requirePath(adapterHome, "adapterHome");
            requirePath(manifestPath, "manifestPath");
            requirePath(javaExecutable, "javaExecutable");
            var requiredHealthAddress = requireSocketAddress(healthAddress, "healthAddress");
            var requiredRpcAddress = requireSocketAddress(rpcAddress, "rpcAddress");
            if (requiredHealthAddress.equals(requiredRpcAddress)) {
                throw new IllegalArgumentException("adapter health and RPC endpoints must be separate");
            }
        }
    }

    private static List<MetadataPeer> copyMetadataPeers(List<MetadataPeer> metadataPeers) {
        var requiredMetadataPeers = requireArgument(metadataPeers, "metadataPeers");
        for (MetadataPeer metadataPeer : requiredMetadataPeers) {
            requireArgument(metadataPeer, "metadataPeers entry");
        }
        return List.copyOf(requiredMetadataPeers);
    }

    private static void requireSeparateEndpoints(URI advertisedRatisEndpoint,
                                                 URI publicApiEndpoint,
                                                 String owner) {
        if (advertisedRatisEndpoint.equals(publicApiEndpoint)) {
            throw new IllegalArgumentException(owner
                    + " public API endpoint must be separate from advertised Ratis endpoint");
        }
    }

    private static void requirePath(@Nullable Path path, String fieldName) {
        if (path == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        if (path.toString().isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    private static InetSocketAddress requireSocketAddress(@Nullable InetSocketAddress address, String fieldName) {
        if (address == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        EndpointUris.requireSocketAddress(address, fieldName);
        return address;
    }

    private static InetSocketAddress socketAddress(@Nullable String endpoint, String fieldName) {
        var uri = uri(endpoint, fieldName);
        if (uri == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        EndpointUris.requireSocketHostAndPort(uri.getHost(), uri.getPort(), fieldName);
        return new InetSocketAddress(uri.getHost(), uri.getPort());
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
