package it.cavallium.rockserver.core.replication.ratis;

import java.util.concurrent.CompletableFuture;

public interface RatisAdapterFactory extends AutoCloseable {

    RatisAdapterBoundary boundary();

    RatisPartitionClient openPartition(RatisPartitionDescriptor descriptor);

    default CompletableFuture<Void> reloadMetadata() {
        return CompletableFuture.failedFuture(new RatisFeatureUnavailableException(
                "Ratis adapter does not support metadata reload"
        ));
    }

    @Override
    default void close() {
    }
}
