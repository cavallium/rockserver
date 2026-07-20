package it.cavallium.rockserver.core.cluster;

import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBException;

public record ReplicatedCommand(ClusterId clusterId,
                                ShardId shardId,
                                long metadataEpoch,
                                long schemaEpoch,
                                ClientRequestId requestId,
                                ReplicatedCommandKind kind,
                                RocksDBAPICommand<?, ?, ?> command,
                                long deadlineEpochMillis,
                                WriteDurability durability) {

    public ReplicatedCommand {
        requireArgument(clusterId, "clusterId");
        requireArgument(shardId, "shardId");
        requireArgument(requestId, "requestId");
        requireArgument(kind, "kind");
        requireArgument(command, "command");
        requireArgument(durability, "durability");
        if (metadataEpoch < 0) {
            throw new IllegalArgumentException("metadataEpoch must be >= 0");
        }
        if (schemaEpoch < 0) {
            throw new IllegalArgumentException("schemaEpoch must be >= 0");
        }
        if (deadlineEpochMillis < 0) {
            throw new IllegalArgumentException("deadlineEpochMillis must be >= 0");
        }
    }

    public static ReplicatedCommand from(ClusterId clusterId,
                                         ShardId shardId,
                                         long metadataEpoch,
                                         long schemaEpoch,
                                         ClientRequestId requestId,
                                         RocksDBAPICommand<?, ?, ?> command,
                                         long deadlineEpochMillis,
                                         WriteDurability durability) {
        var kind = ReplicatedCommandClassifier.classify(command);
        return new ReplicatedCommand(clusterId,
                shardId,
                metadataEpoch,
                schemaEpoch,
                requestId,
                kind,
                command,
                deadlineEpochMillis,
                durability
        );
    }

    public void requireReplicatedLogEntry() {
        ReplicatedCommandClassifier.requireSafeForReplicatedLog(command);
        if (!kind.entersReplicatedLog()) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
                    "command cannot enter the replicated log: " + command.getClass().getName());
        }
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
