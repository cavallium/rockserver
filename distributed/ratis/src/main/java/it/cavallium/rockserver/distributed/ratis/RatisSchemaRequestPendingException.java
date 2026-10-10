package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RocksDBException;
import java.util.UUID;

/** The caller can inspect/resume this exact token; repeating a common call creates another action. */
public final class RatisSchemaRequestPendingException extends RocksDBException {
    private final RatisSchemaRequests.Status progress;
    public RatisSchemaRequestPendingException(RatisSchemaRequests.Status progress,Throwable cause) {
        super(RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN,"Schema request "+progress.requestId()+" is "+progress.state()+"; inspect or resume the original token",cause);
        this.progress=progress;
    }
    public UUID requestId() { return progress.requestId(); }
    public RatisSchemaRequests.Kind operationKind() { return progress.kind(); }
    public RatisSchemaRequests.Status progress() { return progress; }
}
