package it.cavallium.rockserver.distributed.ratis;

import java.util.Objects;
import java.util.UUID;

/** Stable tokens for common schema calls; a fresh common call always allocates a new token. */
public final class RatisSchemaRequests {
    private RatisSchemaRequests() {}
    public enum Kind { CREATE,DELETE,DELETE_IF_EXISTS,UPLOAD }
    public enum State { QUEUED,NOT_STARTED,RUNNING,PAUSED,DONE,ABORTED,CONFLICT }
    public record Result(UUID requestId,Kind kind,long value,boolean changed,boolean aborted) {
        public Result { Objects.requireNonNull(requestId); Objects.requireNonNull(kind); }
    }
    public record Status(UUID requestId,Kind kind,State state,RatisSchemaClient.Phase phase,Result result) {
        public Status { Objects.requireNonNull(requestId); Objects.requireNonNull(kind); Objects.requireNonNull(state); }
    }
}
