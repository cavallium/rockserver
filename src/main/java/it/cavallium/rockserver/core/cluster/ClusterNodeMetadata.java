package it.cavallium.rockserver.core.cluster;

import java.net.URI;

public record ClusterNodeMetadata(NodeId nodeId,
                                  URI publicEndpoint,
                                  URI ratisEndpoint,
                                  String failureDomain) {

    public ClusterNodeMetadata {
        if (nodeId == null) {
            throw new IllegalArgumentException("nodeId must not be null");
        }
        publicEndpoint = EndpointUris.requireEndpointUri(publicEndpoint, "node public endpoint");
        ratisEndpoint = EndpointUris.requireGrpcUri(ratisEndpoint, "node Ratis endpoint");
        SingleLineStrings.requireNonBlank(failureDomain, "failureDomain");
        if (publicEndpoint.equals(ratisEndpoint)) {
            throw new IllegalArgumentException("node public endpoint must be separate from Ratis endpoint");
        }
    }
}
