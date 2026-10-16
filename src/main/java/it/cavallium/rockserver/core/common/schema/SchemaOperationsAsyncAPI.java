package it.cavallium.rockserver.core.common.schema;

import it.cavallium.rockserver.core.common.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/** Existing UUIDs only: resume never retries an ordinary create/delete/upload submission. */
public interface SchemaOperationsAsyncAPI {
    CompletableFuture<List<SchemaOperations.Status>> inventory(RequestContext context);
    CompletableFuture<SchemaOperations.Status> status(UUID requestId,RequestContext context);
    CompletableFuture<SchemaOperations.Status> resume(UUID requestId,RequestContext context);
    CompletableFuture<SchemaOperations.Status> abort(UUID requestId,RequestContext context);
    static SchemaOperationsAsyncAPI unsupported(){return Unsupported.INSTANCE;}
    final class Unsupported implements SchemaOperationsAsyncAPI {
        private static final Unsupported INSTANCE=new Unsupported();private Unsupported(){}
        private static <T>CompletableFuture<T> refuse(RequestContext context){try{Objects.requireNonNull(context);return CompletableFuture.failedFuture(RocksDBException.of(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,"Retained schema operations are unavailable through this backend"));}catch(Throwable failure){return CompletableFuture.failedFuture(failure);}}
        public CompletableFuture<List<SchemaOperations.Status>> inventory(RequestContext context){return refuse(context);}
        public CompletableFuture<SchemaOperations.Status> status(UUID id,RequestContext context){return refuse(context);}
        public CompletableFuture<SchemaOperations.Status> resume(UUID id,RequestContext context){return refuse(context);}
        public CompletableFuture<SchemaOperations.Status> abort(UUID id,RequestContext context){return refuse(context);}
    }
}
