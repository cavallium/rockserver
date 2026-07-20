package it.cavallium.rockserver.ratis;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterConfig;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisPeerDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.apache.ratis.statemachine.StateMachine;

public final class RockserverRatisClasspathStateMachineFactory
        implements RatisClasspathAdapterRuntime.StateMachineFactory {

    private final ClusterMetadata metadata;

    public RockserverRatisClasspathStateMachineFactory(ClusterMetadata metadata) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
    }

    @Override
    public StateMachine create(RatisPartitionDescriptor descriptor) throws IOException {
        Objects.requireNonNull(descriptor, "descriptor");
        if (RatisProcessAdapterConfig.isMetadataGroupPartitionId(descriptor.partitionId())) {
            return new RockserverMetadataClasspathStateMachine(metadata);
        }
        var partition = requireMatchingDataPartition(descriptor);
        return new RockserverRatisClasspathStateMachine(
                metadata,
                partition,
                descriptor.dataPath().resolve("rocksdb")
        );
    }

    @Override
    public void validateReload(List<RatisPartitionDescriptor> descriptors) {
        Objects.requireNonNull(descriptors, "descriptors");
        for (var descriptor : descriptors) {
            if (RatisProcessAdapterConfig.isMetadataGroupPartitionId(descriptor.partitionId())) {
                continue;
            }
            requireMatchingDataPartition(descriptor);
        }
    }

    @Override
    public void reload(StateMachine stateMachine, RatisPartitionDescriptor descriptor) {
        Objects.requireNonNull(stateMachine, "stateMachine");
        Objects.requireNonNull(descriptor, "descriptor");
        if (RatisProcessAdapterConfig.isMetadataGroupPartitionId(descriptor.partitionId())) {
            if (!(stateMachine instanceof RockserverMetadataClasspathStateMachine)) {
                throw new IllegalArgumentException("Rockserver metadata reload requires a metadata state machine");
            }
            return;
        }
        requireMatchingDataPartition(descriptor);
        if (!(stateMachine instanceof RockserverRatisClasspathStateMachine rockserverStateMachine)) {
            throw new IllegalArgumentException("Rockserver metadata reload requires a Rockserver state machine");
        }
        rockserverStateMachine.reloadMetadata(metadata);
    }

    private PartitionDescriptor requireMatchingDataPartition(RatisPartitionDescriptor descriptor) {
        var partition = metadata.requirePartition(partitionId(descriptor.partitionId()));
        if (!partition.ratisGroupId().equals(descriptor.raftGroupId())) {
            throw new IllegalArgumentException("Ratis adapter partition group id does not match metadata for "
                    + descriptor.partitionId());
        }
        var expectedVoters = partition.peers().stream()
                .filter(peer -> peer.isVoter())
                .map(peer -> {
                    var node = metadata.requireNode(peer.nodeId());
                    return new RatisPeerDescriptor(
                            peer.nodeId().value(),
                            node.ratisEndpoint().toString(),
                            node.failureDomain()
                    );
                })
                .toList();
        if (!expectedVoters.equals(descriptor.voters())) {
            throw new IllegalArgumentException("Ratis adapter partition voters do not match metadata for "
                    + descriptor.partitionId());
        }
        return partition;
    }

    private static ShardId partitionId(String value) {
        try {
            var parsed = Long.parseUnsignedLong(value);
            if (!Long.toUnsignedString(parsed).equals(value)) {
                throw new IllegalArgumentException(
                        "Ratis partition descriptor id must be canonical numeric shard id: " + value
                );
            }
            return new ShardId(parsed);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Ratis partition descriptor id must be a numeric shard id: " + value, e);
        }
    }
}
