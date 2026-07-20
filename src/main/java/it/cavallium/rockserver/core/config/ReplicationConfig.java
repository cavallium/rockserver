package it.cavallium.rockserver.core.config;

import java.nio.file.Path;

import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;

public interface ReplicationConfig {

    boolean enabled() throws GestaltException;

    @Nullable String nodeId() throws GestaltException;

    @Nullable String advertisedRatisEndpoint() throws GestaltException;

    @Nullable String publicApiEndpoint() throws GestaltException;

    @Nullable Path dataPath() throws GestaltException;

    @Nullable Path metadataPath() throws GestaltException;

    @Nullable Path bootstrapManifestPath() throws GestaltException;

    @Nullable String failureDomain() throws GestaltException;

    @Nullable Path adapterHome() throws GestaltException;

    @Nullable Path adapterManifestPath() throws GestaltException;

    @Nullable Path adapterJavaExecutable() throws GestaltException;

    @Nullable String adapterHealthEndpoint() throws GestaltException;

    @Nullable String adapterRpcEndpoint() throws GestaltException;

    ReplicationMetadataPeerConfig[] metadataPeers() throws GestaltException;
}
