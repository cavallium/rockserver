package it.cavallium.rockserver.core.common.cdc;

public enum CdcCommitMode {
    /**
     * Commits the offset only after the entire batch fetched from the server is successfully processed.
     * This offers the best performance but may result in duplicate events being replayed if the stream
     * crashes in the middle of processing a batch.
     */
    BATCH,

    /**
     * Commits the offset after each event is successfully processed.
     * This ensures minimizing duplicates upon restart (resume from last processed event),
     * but incurs significantly higher latency due to frequent commit calls.
     */
    PER_EVENT
}
