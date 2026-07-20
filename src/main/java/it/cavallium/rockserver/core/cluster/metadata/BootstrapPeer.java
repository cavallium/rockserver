package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.rockserver.core.cluster.EndpointUris;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.SingleLineStrings;

import java.net.URI;

public record BootstrapPeer(NodeId nodeId,
                            URI publicEndpoint,
                            URI ratisEndpoint,
                            String failureDomain) {

    public BootstrapPeer {
        requireNodeId(nodeId);
        publicEndpoint = EndpointUris.requireEndpointUri(publicEndpoint, "bootstrap public endpoint");
        ratisEndpoint = EndpointUris.requireGrpcUri(ratisEndpoint, "bootstrap Ratis endpoint");
        SingleLineStrings.requireNonBlank(failureDomain, "failureDomain");
    }

    private static void requireNodeId(NodeId value) {
        if (value == null) {
            throw new IllegalArgumentException("nodeId must not be null");
        }
    }
}
