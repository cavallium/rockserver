package it.cavallium.rockserver.core.common;

import java.util.concurrent.CompletableFuture;

public interface RocksDBAsyncAPIRequestHandler {

	default <R> CompletableFuture<R> requestAsync(RocksDBAPICommand<R> req) {
		return CompletableFuture.failedFuture(new UnsupportedOperationException("Unsupported request type: " + req));
	}
}
