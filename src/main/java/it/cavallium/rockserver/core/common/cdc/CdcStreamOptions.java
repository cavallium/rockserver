package it.cavallium.rockserver.core.common.cdc;

import java.time.Duration;
import org.jetbrains.annotations.Nullable;

/**
 * Options for CDC streaming.
 */
public record CdcStreamOptions(
        @Nullable Long fromSeq,
        long batchSize,
        @Nullable Duration idleDelay,
        CdcCommitMode commitMode
) {
    public static final long DEFAULT_BATCH_SIZE = 10_000L;
    public static final Duration DEFAULT_IDLE_DELAY = Duration.ofMillis(100);
    public static final CdcCommitMode DEFAULT_COMMIT_MODE = CdcCommitMode.NONE;

    public static CdcStreamOptions defaults() {
        return new CdcStreamOptions(null, DEFAULT_BATCH_SIZE, DEFAULT_IDLE_DELAY, DEFAULT_COMMIT_MODE);
    }
}
