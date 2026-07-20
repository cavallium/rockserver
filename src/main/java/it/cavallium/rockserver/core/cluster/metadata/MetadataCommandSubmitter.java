package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;

@FunctionalInterface
public interface MetadataCommandSubmitter {

    ClusterMetadata submit(MetadataCommand command);
}
