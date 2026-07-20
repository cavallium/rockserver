package it.cavallium.rockserver.core.common;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public interface RocksDBAsyncAPIRequestHandler {

	@SuppressWarnings("unchecked")
	default <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> req) {
		return (RA) switch (req) {
			case RocksDBAPICommand.RocksDBAPICommandStream<?> _ ->
					(Publisher<R>) subscriber -> {
						var cancelled = new AtomicBoolean();
						subscriber.onSubscribe(new Subscription() {
							@Override
							public void request(long n) {
							}

							@Override
							public void cancel() {
								cancelled.set(true);
							}
						});
						if (!cancelled.get()) {
							subscriber.onError(unsupportedRequestType(req));
						}
					};
			case RocksDBAPICommand.RocksDBAPICommandSingle<?> _ ->
					CompletableFuture.<R>failedFuture(unsupportedRequestType(req));
		};
	}

	private static RocksDBException unsupportedRequestType(RocksDBAPICommand<?, ?, ?> req) {
		return RocksDBException.of(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
				"Unsupported request type: " + req);
	}
}
