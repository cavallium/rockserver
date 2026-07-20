package it.cavallium.rockserver.core.cluster;

public enum ReplicationFailurePhase {
    APPEND,
    COMMIT,
    APPLY,
    RESPONSE_PERSISTENCE,
    ROCKSDB_WRITE,
    RESPONSE_SEND
}
