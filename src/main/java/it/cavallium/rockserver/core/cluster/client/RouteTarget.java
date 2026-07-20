package it.cavallium.rockserver.core.cluster.client;

import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicatedCommandKind;
import it.cavallium.rockserver.core.cluster.ShardId;

public record RouteTarget(ShardId shardId,
                          ReplicaMember replica,
                          boolean leader,
                          ReplicatedCommandKind commandKind,
                          long metadataEpoch) {

    public RouteTarget {
        requireArgument(shardId, "shardId");
        requireArgument(replica, "replica");
        requireArgument(commandKind, "commandKind");
        if (metadataEpoch < 0) {
            throw new IllegalArgumentException("metadataEpoch must be >= 0");
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
