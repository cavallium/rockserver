package it.cavallium.rockserver.core.replication.ratis;

public enum RatisProcessAdapterRpcOperation {
    PROPOSE,
    WAIT_FOR_COMMIT,
    QUERY_LINEARIZABLE,
    QUERY_STALE,
    QUERY_LOCAL,
    RELOAD_METADATA
}
