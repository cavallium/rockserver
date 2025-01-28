package it.cavallium.rockserver.core.common;

import org.reactivestreams.Publisher;

import java.util.concurrent.CompletableFuture;

public interface RocksDBAsyncAPIRequestHandler {

	@SuppressWarnings("unchecked")
    default <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> req) {
		return (RA) switch (req) {
			case RocksDBAPICommand.RocksDBAPICommandStream<?> _ ->
					(Publisher<R>) subscriber ->
							subscriber.onError(new UnsupportedOperationException("Unsupported request type: " + req));
			case RocksDBAPICommand.RocksDBAPICommandSingle<?> _ ->
					CompletableFuture.<R>failedFuture(new UnsupportedOperationException("Unsupported request type: " + req));
        };
	}
}
