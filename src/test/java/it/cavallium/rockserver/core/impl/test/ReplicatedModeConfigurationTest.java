package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.ReplicatedModeConfiguration;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.config.DatabaseConfig;
import it.cavallium.rockserver.core.config.ReplicationConfig;
import it.cavallium.rockserver.core.config.ReplicationMetadataPeerConfig;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

class ReplicatedModeConfigurationTest {

    @Test
    void defaultConfigProducesDisabledReplicatedMode() throws Exception {
        var configuration = ReplicatedModeConfiguration.from(ConfigParser.parseDefault());

        assertFalse(configuration.enabled());
        assertEquals(0, configuration.metadataPeers().size());
    }

    @Test
    void replicatedModeRejectsNullMetadataPeerConfigArray() {
        var error = assertThrows(IllegalArgumentException.class,
                () -> ReplicatedModeConfiguration.from(replicationConfigWithMetadataPeers(null)));

        assertEquals("replication metadataPeers config must not be null", error.getMessage());
    }

    @Test
    void replicatedModeRejectsNullMetadataPeerConfigEntry() {
        var error = assertThrows(IllegalArgumentException.class,
                () -> ReplicatedModeConfiguration.from(replicationConfigWithMetadataPeers(
                        new ReplicationMetadataPeerConfig[]{null}
                )));

        assertEquals("replication metadataPeers[0] must not be null", error.getMessage());
    }

    @Test
    void replicatedModeRejectsNullMetadataPeerConfigFields() {
        var nullNodeId = assertThrows(IllegalArgumentException.class,
                () -> ReplicatedModeConfiguration.from(replicationConfigWithMetadataPeers(
                        new ReplicationMetadataPeerConfig[]{replicationMetadataPeerConfig(
                                null,
                                "grpc://127.0.0.1:19001",
                                "http://127.0.0.1:8080",
                                "rack-a"
                        )}
                )));
        var nullRatisEndpoint = assertThrows(IllegalArgumentException.class,
                () -> ReplicatedModeConfiguration.from(replicationConfigWithMetadataPeers(
                        new ReplicationMetadataPeerConfig[]{replicationMetadataPeerConfig(
                                "node-a",
                                null,
                                "http://127.0.0.1:8080",
                                "rack-a"
                        )}
                )));
        var nullPublicEndpoint = assertThrows(IllegalArgumentException.class,
                () -> ReplicatedModeConfiguration.from(replicationConfigWithMetadataPeers(
                        new ReplicationMetadataPeerConfig[]{replicationMetadataPeerConfig(
                                "node-a",
                                "grpc://127.0.0.1:19001",
                                null,
                                "rack-a"
                        )}
                )));
        var nullFailureDomain = assertThrows(IllegalArgumentException.class,
                () -> ReplicatedModeConfiguration.from(replicationConfigWithMetadataPeers(
                        new ReplicationMetadataPeerConfig[]{replicationMetadataPeerConfig(
                                "node-a",
                                "grpc://127.0.0.1:19001",
                                "http://127.0.0.1:8080",
                                null
                        )}
                )));

        assertEquals("metadata peer node-id must not be null", nullNodeId.getMessage());
        assertEquals("metadata peer advertised-ratis-endpoint must not be null",
                nullRatisEndpoint.getMessage());
        assertEquals("metadata peer public-api-endpoint must not be null", nullPublicEndpoint.getMessage());
        assertEquals("metadata peer failure-domain must not be null", nullFailureDomain.getMessage());
    }

    @Test
    void replicatedModeRejectsNullInputsWithExplicitDiagnostics(@TempDir Path directory) {
        assertIllegalArgument("databaseConfig must not be null",
                () -> ReplicatedModeConfiguration.from((DatabaseConfig) null));
        assertIllegalArgument("config must not be null",
                () -> ReplicatedModeConfiguration.from((ReplicationConfig) null));
        assertIllegalArgument("metadataPeers must not be null", () -> new ReplicatedModeConfiguration(
                false, null, null, null, null, null, null, null, null, null));
        assertIllegalArgument("metadataPeers entry must not be null", () -> new ReplicatedModeConfiguration(
                false, null, null, null, null, null, null, null, null, Collections.singletonList(null)));
        assertIllegalArgument("nodeId must not be null", () -> new ReplicatedModeConfiguration.MetadataPeer(
                null,
                URI.create("grpc://127.0.0.1:19001"),
                URI.create("http://127.0.0.1:8080"),
                "rack-a"
        ));
        assertIllegalArgument("javaExecutable must not be null", () -> new ReplicatedModeConfiguration.AdapterProcess(
                directory.resolve("adapter"),
                directory.resolve("adapter").resolve("ratis-adapter.properties"),
                null,
                new java.net.InetSocketAddress("127.0.0.1", 19101),
                new java.net.InetSocketAddress("127.0.0.1", 19102)
        ));
    }

    @Test
    void parsesEnabledReplicatedModeConfiguration(@TempDir Path directory) throws Exception {
        var configFile = directory.resolve("replication.conf");
        Files.writeString(configFile, """
                database: {
                  replication: {
                    enabled: true
                    node-id: "node-a"
                    advertised-ratis-endpoint: "grpc://127.0.0.1:19001"
                    public-api-endpoint: "http://127.0.0.1:8080"
                    data-path: "%s"
                    metadata-path: "%s"
                    bootstrap-manifest-path: "%s"
                    failure-domain: "rack-a"
                    adapter-home: "%s"
                    adapter-manifest-path: "%s"
                    adapter-java-executable: "%s"
                    adapter-health-endpoint: "tcp://127.0.0.1:19101"
                    adapter-rpc-endpoint: "tcp://127.0.0.1:19102"
                    metadata-peers: [
                      {
                        node-id: "node-a"
                        advertised-ratis-endpoint: "grpc://127.0.0.1:19001"
                        public-api-endpoint: "http://127.0.0.1:8080"
                        failure-domain: "rack-a"
                      }
                      {
                        node-id: "node-b"
                        advertised-ratis-endpoint: "grpc://127.0.0.1:19002"
                        public-api-endpoint: "http://127.0.0.1:8081"
                        failure-domain: "rack-b"
                      }
                    ]
                  }
                }
                """.formatted(
                directory.resolve("data"),
                directory.resolve("metadata"),
                directory.resolve("bootstrap.properties"),
                directory.resolve("adapter"),
                directory.resolve("adapter").resolve("ratis-adapter.properties"),
                Path.of("/usr/lib/jvm/default/bin/java")
        ));

        var configuration = ReplicatedModeConfiguration.from(ConfigParser.parse(configFile));

        assertTrue(configuration.enabled());
        assertEquals(NodeId.of("node-a"), configuration.nodeId());
        assertEquals(URI.create("grpc://127.0.0.1:19001"), configuration.advertisedRatisEndpoint());
        assertEquals(URI.create("http://127.0.0.1:8080"), configuration.publicApiEndpoint());
        assertEquals(directory.resolve("data"), configuration.dataPath());
        assertEquals(directory.resolve("metadata"), configuration.metadataPath());
        assertEquals(directory.resolve("bootstrap.properties"), configuration.bootstrapManifestPath());
        assertEquals("rack-a", configuration.failureDomain());
        var adapterProcess = configuration.adapterProcess();
        assertNotNull(adapterProcess);
        assertEquals(directory.resolve("adapter"), adapterProcess.adapterHome());
        assertEquals(directory.resolve("adapter").resolve("ratis-adapter.properties"),
                adapterProcess.manifestPath());
        assertEquals(Path.of("/usr/lib/jvm/default/bin/java"), adapterProcess.javaExecutable());
        assertEquals("127.0.0.1", adapterProcess.healthAddress().getHostString());
        assertEquals(19101, adapterProcess.healthAddress().getPort());
        assertEquals(19102, adapterProcess.rpcAddress().getPort());
        assertEquals(2, configuration.metadataPeers().size());
        assertEquals(NodeId.of("node-b"), configuration.metadataPeers().get(1).nodeId());
        assertEquals(URI.create("grpc://127.0.0.1:19002"),
                configuration.metadataPeers().get(1).advertisedRatisEndpoint());
    }

    @Test
    void enabledReplicatedModeRequiresNodeIdentityAndEndpoints(@TempDir Path directory) throws Exception {
        var configFile = directory.resolve("replication-invalid.conf");
        Files.writeString(configFile, """
                database: {
                  replication: {
                    enabled: true
                  }
                }
                """);

        var error = assertThrows(IllegalArgumentException.class,
                () -> ReplicatedModeConfiguration.from(ConfigParser.parse(configFile)));
        assertEquals("replication enabled requires nodeId", error.getMessage());
    }

    @Test
    void enabledReplicatedModeRequiresSeparatePublicAndRatisEndpoints(@TempDir Path directory) throws Exception {
        var configFile = directory.resolve("replication-shared-endpoint.conf");
        Files.writeString(configFile, """
                database: {
                  replication: {
                    enabled: true
                    node-id: "node-a"
                    advertised-ratis-endpoint: "grpc://127.0.0.1:19001"
                    public-api-endpoint: "grpc://127.0.0.1:19001"
                    data-path: "%s"
                    metadata-path: "%s"
                    bootstrap-manifest-path: "%s"
                    failure-domain: "rack-a"
                    adapter-home: "%s"
                    adapter-manifest-path: "%s"
                    adapter-java-executable: "%s"
                    adapter-health-endpoint: "tcp://127.0.0.1:19101"
                    adapter-rpc-endpoint: "tcp://127.0.0.1:19102"
                    metadata-peers: [
                      {
                        node-id: "node-a"
                        advertised-ratis-endpoint: "grpc://127.0.0.1:19001"
                        public-api-endpoint: "http://127.0.0.1:8080"
                        failure-domain: "rack-a"
                      }
                    ]
                  }
                }
                """.formatted(
                directory.resolve("data"),
                directory.resolve("metadata"),
                directory.resolve("bootstrap.properties"),
                directory.resolve("adapter"),
                directory.resolve("adapter").resolve("ratis-adapter.properties"),
                Path.of("/usr/lib/jvm/default/bin/java")
        ));

        assertThrows(IllegalArgumentException.class,
                () -> ReplicatedModeConfiguration.from(ConfigParser.parse(configFile)));
    }

    @Test
    void enabledReplicatedModeRequiresGrpcAdvertisedRatisEndpoint(@TempDir Path directory) {
        var error = assertThrows(IllegalArgumentException.class, () -> new ReplicatedModeConfiguration(
                true,
                NodeId.of("node-a"),
                URI.create("http://127.0.0.1:19001"),
                URI.create("http://127.0.0.1:8080"),
                directory.resolve("data"),
                directory.resolve("metadata"),
                directory.resolve("bootstrap.properties"),
                "rack-a",
                validAdapterProcess(directory),
                List.of(validMetadataPeer())
        ));

        assertEquals("advertisedRatisEndpoint must use grpc scheme", error.getMessage());
    }

    @Test
    void enabledReplicatedModeRequiresLocalMetadataPeer(@TempDir Path directory) {
        var error = assertThrows(IllegalArgumentException.class,
                () -> constructEnabledConfiguration(directory, List.of(metadataPeer(
                        "node-b",
                        "grpc://127.0.0.1:19002",
                        "http://127.0.0.1:8081",
                        "rack-b"
                ))));

        assertEquals("metadataPeers must include local nodeId when replication is enabled: node-a",
                error.getMessage());
    }

    @Test
    void enabledReplicatedModeRejectsDuplicateMetadataPeerIdentities(@TempDir Path directory) {
        var duplicateNodeId = assertThrows(IllegalArgumentException.class,
                () -> constructEnabledConfiguration(directory, List.of(
                        validMetadataPeer(),
                        metadataPeer("node-a", "grpc://127.0.0.1:19002", "http://127.0.0.1:8081", "rack-b")
                )));
        var duplicateRatisEndpoint = assertThrows(IllegalArgumentException.class,
                () -> constructEnabledConfiguration(directory, List.of(
                        validMetadataPeer(),
                        metadataPeer("node-b", "grpc://127.0.0.1:19001", "http://127.0.0.1:8081", "rack-b")
                )));
        var duplicatePublicEndpoint = assertThrows(IllegalArgumentException.class,
                () -> constructEnabledConfiguration(directory, List.of(
                        validMetadataPeer(),
                        metadataPeer("node-b", "grpc://127.0.0.1:19002", "http://127.0.0.1:8080", "rack-b")
                )));

        assertEquals("metadataPeers contains duplicate nodeId: node-a", duplicateNodeId.getMessage());
        assertEquals("metadataPeers contains duplicate advertisedRatisEndpoint: grpc://127.0.0.1:19001",
                duplicateRatisEndpoint.getMessage());
        assertEquals("metadataPeers contains duplicate publicApiEndpoint: http://127.0.0.1:8080",
                duplicatePublicEndpoint.getMessage());
    }

    @Test
    void metadataPeersRequireSeparatePublicAndRatisEndpoints() {
        assertThrows(IllegalArgumentException.class, () -> new ReplicatedModeConfiguration.MetadataPeer(
                NodeId.of("node-a"),
                URI.create("grpc://127.0.0.1:19001"),
                URI.create("grpc://127.0.0.1:19001"),
                "rack-a"
        ));
    }

    @Test
    void metadataPeersRequireUsableEndpointUris() {
        var ratisError = assertThrows(IllegalArgumentException.class,
                () -> new ReplicatedModeConfiguration.MetadataPeer(
                        NodeId.of("node-a"),
                        URI.create("http://127.0.0.1:19001"),
                        URI.create("http://127.0.0.1:8080"),
                        "rack-a"
                ));
        var publicApiError = assertThrows(IllegalArgumentException.class,
                () -> new ReplicatedModeConfiguration.MetadataPeer(
                        NodeId.of("node-a"),
                        URI.create("grpc://127.0.0.1:19001"),
                        URI.create("http://127.0.0.1"),
                        "rack-a"
                ));

        assertEquals("metadata peer node-a advertisedRatisEndpoint must use grpc scheme", ratisError.getMessage());
        assertEquals("metadata peer node-a publicApiEndpoint port must be between 1 and 65535",
                publicApiError.getMessage());
    }

    @Test
    void replicatedModeRejectsLineBrokenFailureDomains(@TempDir Path directory) {
        var metadataPeer = new ReplicatedModeConfiguration.MetadataPeer(
                NodeId.of("node-a"),
                URI.create("grpc://127.0.0.1:19001"),
                URI.create("http://127.0.0.1:8080"),
                "rack-a"
        );
        var adapterProcess = new ReplicatedModeConfiguration.AdapterProcess(
                directory.resolve("adapter"),
                directory.resolve("adapter").resolve("ratis-adapter.properties"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                new java.net.InetSocketAddress("127.0.0.1", 19101),
                new java.net.InetSocketAddress("127.0.0.1", 19102)
        );

        assertThrows(IllegalArgumentException.class, () -> new ReplicatedModeConfiguration(
                true,
                NodeId.of("node-a"),
                URI.create("grpc://127.0.0.1:19001"),
                URI.create("http://127.0.0.1:8080"),
                directory.resolve("data"),
                directory.resolve("metadata"),
                directory.resolve("bootstrap.properties"),
                "rack-a\ninjected=true",
                adapterProcess,
                List.of(metadataPeer)
        ));
        assertThrows(IllegalArgumentException.class, () -> new ReplicatedModeConfiguration(
                false,
                null,
                null,
                null,
                null,
                null,
                null,
                "rack-a\ninjected=true",
                null,
                List.of()
        ));
        assertThrows(IllegalArgumentException.class, () -> new ReplicatedModeConfiguration.MetadataPeer(
                NodeId.of("node-a"),
                URI.create("grpc://127.0.0.1:19001"),
                URI.create("http://127.0.0.1:8080"),
                "rack-a\rinjected=true"
        ));
    }

    @Test
    void enabledReplicatedModeRequiresAdapterProcessSettings(@TempDir Path directory) throws Exception {
        var configFile = directory.resolve("replication-missing-adapter.conf");
        Files.writeString(configFile, """
                database: {
                  replication: {
                    enabled: true
                    node-id: "node-a"
                    advertised-ratis-endpoint: "grpc://127.0.0.1:19001"
                    public-api-endpoint: "http://127.0.0.1:8080"
                    data-path: "%s"
                    metadata-path: "%s"
                    bootstrap-manifest-path: "%s"
                    failure-domain: "rack-a"
                    metadata-peers: [
                      {
                        node-id: "node-a"
                        advertised-ratis-endpoint: "grpc://127.0.0.1:19001"
                        public-api-endpoint: "http://127.0.0.1:8080"
                        failure-domain: "rack-a"
                      }
                    ]
                  }
                }
                """.formatted(
                directory.resolve("data"),
                directory.resolve("metadata"),
                directory.resolve("bootstrap.properties")
        ));

        var error = assertThrows(IllegalArgumentException.class,
                () -> ReplicatedModeConfiguration.from(ConfigParser.parse(configFile)));
        assertEquals("replication enabled requires adapterProcess", error.getMessage());
    }

    @Test
    void adapterProcessRequiresSeparateHealthAndRpcEndpoints(@TempDir Path directory) {
        var endpoint = new java.net.InetSocketAddress("127.0.0.1", 19101);

        assertThrows(IllegalArgumentException.class, () -> new ReplicatedModeConfiguration.AdapterProcess(
                directory.resolve("adapter"),
                directory.resolve("adapter").resolve("ratis-adapter.properties"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                endpoint,
                endpoint
        ));
    }

    @Test
    void adapterProcessRejectsNullRequiredFields(@TempDir Path directory) {
        var nullAdapterHome = assertThrows(IllegalArgumentException.class,
                () -> new ReplicatedModeConfiguration.AdapterProcess(
                        null,
                        directory.resolve("adapter").resolve("ratis-adapter.properties"),
                        Path.of("/usr/lib/jvm/default/bin/java"),
                        new java.net.InetSocketAddress("127.0.0.1", 19101),
                        new java.net.InetSocketAddress("127.0.0.1", 19102)
                ));
        var nullHealthAddress = assertThrows(IllegalArgumentException.class,
                () -> new ReplicatedModeConfiguration.AdapterProcess(
                        directory.resolve("adapter"),
                        directory.resolve("adapter").resolve("ratis-adapter.properties"),
                        Path.of("/usr/lib/jvm/default/bin/java"),
                        null,
                        new java.net.InetSocketAddress("127.0.0.1", 19102)
                ));

        assertEquals("adapterHome must not be null", nullAdapterHome.getMessage());
        assertEquals("healthAddress must not be null", nullHealthAddress.getMessage());
    }

    @Test
    void partialAdapterProcessConfigRejectsNullEndpointBeforeEnabledAdmission(@TempDir Path directory) {
        var error = assertThrows(IllegalArgumentException.class,
                () -> ReplicatedModeConfiguration.from(replicationConfigWithMissingAdapterRpcEndpoint(directory)));

        assertEquals("adapter-rpc-endpoint must not be null", error.getMessage());
    }

    private static ReplicatedModeConfiguration.MetadataPeer validMetadataPeer() {
        return metadataPeer("node-a", "grpc://127.0.0.1:19001", "http://127.0.0.1:8080", "rack-a");
    }

    private static void assertIllegalArgument(String message, Executable executable) {
        var failure = assertThrows(IllegalArgumentException.class, executable);
        assertEquals(message, failure.getMessage());
    }

    private static ReplicatedModeConfiguration.MetadataPeer metadataPeer(String nodeId,
                                                                        String advertisedRatisEndpoint,
                                                                        String publicApiEndpoint,
                                                                        String failureDomain) {
        return new ReplicatedModeConfiguration.MetadataPeer(
                NodeId.of(nodeId),
                URI.create(advertisedRatisEndpoint),
                URI.create(publicApiEndpoint),
                failureDomain
        );
    }

    private static void constructEnabledConfiguration(
            Path directory,
            List<ReplicatedModeConfiguration.MetadataPeer> metadataPeers
    ) {
        new ReplicatedModeConfiguration(
                true,
                NodeId.of("node-a"),
                URI.create("grpc://127.0.0.1:19001"),
                URI.create("http://127.0.0.1:8080"),
                directory.resolve("data"),
                directory.resolve("metadata"),
                directory.resolve("bootstrap.properties"),
                "rack-a",
                validAdapterProcess(directory),
                metadataPeers
        );
    }

    private static ReplicatedModeConfiguration.AdapterProcess validAdapterProcess(Path directory) {
        return new ReplicatedModeConfiguration.AdapterProcess(
                directory.resolve("adapter"),
                directory.resolve("adapter").resolve("ratis-adapter.properties"),
                Path.of("/usr/lib/jvm/default/bin/java"),
                new java.net.InetSocketAddress("127.0.0.1", 19101),
                new java.net.InetSocketAddress("127.0.0.1", 19102)
        );
    }

    private static ReplicationConfig replicationConfigWithMetadataPeers(ReplicationMetadataPeerConfig[] metadataPeers) {
        return new ReplicationConfig() {
            @Override
            public boolean enabled() {
                return false;
            }

            @Override
            public String nodeId() {
                return null;
            }

            @Override
            public String advertisedRatisEndpoint() {
                return null;
            }

            @Override
            public String publicApiEndpoint() {
                return null;
            }

            @Override
            public Path dataPath() {
                return null;
            }

            @Override
            public Path metadataPath() {
                return null;
            }

            @Override
            public Path bootstrapManifestPath() {
                return null;
            }

            @Override
            public String failureDomain() {
                return null;
            }

            @Override
            public Path adapterHome() {
                return null;
            }

            @Override
            public Path adapterManifestPath() {
                return null;
            }

            @Override
            public Path adapterJavaExecutable() {
                return null;
            }

            @Override
            public String adapterHealthEndpoint() {
                return null;
            }

            @Override
            public String adapterRpcEndpoint() {
                return null;
            }

            @Override
            public ReplicationMetadataPeerConfig[] metadataPeers() {
                return metadataPeers;
            }
        };
    }

    private static ReplicationConfig replicationConfigWithMissingAdapterRpcEndpoint(Path directory) {
        return new ReplicationConfig() {
            @Override
            public boolean enabled() {
                return false;
            }

            @Override
            public String nodeId() {
                return null;
            }

            @Override
            public String advertisedRatisEndpoint() {
                return null;
            }

            @Override
            public String publicApiEndpoint() {
                return null;
            }

            @Override
            public Path dataPath() {
                return null;
            }

            @Override
            public Path metadataPath() {
                return null;
            }

            @Override
            public Path bootstrapManifestPath() {
                return null;
            }

            @Override
            public String failureDomain() {
                return null;
            }

            @Override
            public Path adapterHome() {
                return directory.resolve("adapter");
            }

            @Override
            public Path adapterManifestPath() {
                return directory.resolve("adapter").resolve("ratis-adapter.properties");
            }

            @Override
            public Path adapterJavaExecutable() {
                return Path.of("/usr/lib/jvm/default/bin/java");
            }

            @Override
            public String adapterHealthEndpoint() {
                return "tcp://127.0.0.1:19101";
            }

            @Override
            public String adapterRpcEndpoint() {
                return null;
            }

            @Override
            public ReplicationMetadataPeerConfig[] metadataPeers() {
                return new ReplicationMetadataPeerConfig[0];
            }
        };
    }

    private static ReplicationMetadataPeerConfig replicationMetadataPeerConfig(String nodeId,
                                                                              String advertisedRatisEndpoint,
                                                                              String publicApiEndpoint,
                                                                              String failureDomain) {
        return new ReplicationMetadataPeerConfig() {
            @Override
            public String nodeId() {
                return nodeId;
            }

            @Override
            public String advertisedRatisEndpoint() {
                return advertisedRatisEndpoint;
            }

            @Override
            public String publicApiEndpoint() {
                return publicApiEndpoint;
            }

            @Override
            public String failureDomain() {
                return failureDomain;
            }
        };
    }
}
