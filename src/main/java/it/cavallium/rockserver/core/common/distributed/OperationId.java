package it.cavallium.rockserver.core.common.distributed;

import java.util.Objects;
import java.util.UUID;

/** Stable retry identity, independent of leadership and transport requests. */
public record OperationId(UUID sessionId, long epoch, long sequence) {
    public OperationId {
        Objects.requireNonNull(sessionId, "sessionId");
        if (epoch < 0 || sequence < 0) {
            throw new IllegalArgumentException("Operation epoch and sequence must be nonnegative");
        }
    }
}
