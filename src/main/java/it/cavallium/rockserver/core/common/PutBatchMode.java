package it.cavallium.rockserver.core.common;

public enum PutBatchMode {
    WRITE_BATCH,
    WRITE_BATCH_NO_WAL,
    SST_INGESTION,
    /**
     * Ingest an SST behind, skipping duplicate keys
     * and ingesting everything in the bottommost level
     */
    SST_INGEST_BEHIND
}
