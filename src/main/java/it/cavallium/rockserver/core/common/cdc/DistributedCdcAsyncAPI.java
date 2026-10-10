package it.cavallium.rockserver.core.common.cdc;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.distributed.DistributedCdcSubscription;
import it.cavallium.rockserver.core.common.distributed.RangeDirectory;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** Backend-authoritative vector CDC lifecycle. Delivery requires an explicit consumer batch ACK. */
public interface DistributedCdcAsyncAPI {
    CompletableFuture<RangeDirectory> context(UUID domainId,RequestContext context);
    CompletableFuture<DistributedCdcSubscriptions.Status> create(DistributedCdcSubscription.Definition definition,RequestContext context);
    CompletableFuture<DistributedCdcSubscriptions.Status> recover(UUID subscriptionId,RequestContext context);
    CompletableFuture<DistributedCdcSubscriptions.Status> status(UUID subscriptionId,RequestContext context);
    /** maxBytes bounds the native source page; projected envelope/definition metadata has MAX_BATCH_BYTES bound. */
    CompletableFuture<DistributedCdcSubscriptions.Poll> poll(UUID subscriptionId,int maxRecords,int maxBytes,RequestContext context);
    CompletableFuture<DistributedCdcSubscriptions.Status> acknowledge(UUID subscriptionId,UUID batchId,RequestContext context);
    CompletableFuture<DistributedCdcSubscriptions.Status> closeSubscription(UUID subscriptionId,RequestContext context);
}
