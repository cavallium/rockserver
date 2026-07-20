package it.cavallium.rockserver.core.replication.ratis;

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface RatisProcessAdapterRpcTransport extends AutoCloseable {

    CompletableFuture<RatisProcessAdapterRpcResponse> send(RatisProcessAdapterRpcRequest request);

    @Override
    default void close() {
    }
}
