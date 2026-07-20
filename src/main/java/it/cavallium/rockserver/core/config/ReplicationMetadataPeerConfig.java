package it.cavallium.rockserver.core.config;

import org.github.gestalt.config.exceptions.GestaltException;

public interface ReplicationMetadataPeerConfig {

    String nodeId() throws GestaltException;

    String advertisedRatisEndpoint() throws GestaltException;

    String publicApiEndpoint() throws GestaltException;

    String failureDomain() throws GestaltException;
}
