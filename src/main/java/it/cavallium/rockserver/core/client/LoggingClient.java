package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;

public class LoggingClient implements RocksDBConnection {

	private final RocksDBConnection client;
	private final LoggingSyncApi syncApi;
	private final LoggingAsyncApi asyncApi;
	private final Logger logger;

	public LoggingClient(RocksDBConnection client) {
		this.client = client;
		this.syncApi = new LoggingSyncApi(client.getSyncApi());
		this.asyncApi = new LoggingAsyncApi(client.getAsyncApi());
		this.logger = LoggerFactory.getLogger("db.requests");
	}

	@Override
	public URI getUrl() {
		return client.getUrl();
	}

	@Override
	public RocksDBSyncAPI getSyncApi() {
		return syncApi;
	}

	@Override
	public RocksDBAsyncAPI getAsyncApi() {
		return asyncApi;
	}

	@Override
	public void close() throws IOException {
		client.close();
	}

	private class LoggingSyncApi implements RocksDBSyncAPI {

		private final RocksDBSyncAPI syncApi;

		public LoggingSyncApi(RocksDBSyncAPI syncApi) {
			this.syncApi = syncApi;
		}

		@Override
		public <RESULT_ITEM_TYPE, SYNC_RESULT, ASYNC_RESULT> SYNC_RESULT requestSync(RocksDBAPICommand<RESULT_ITEM_TYPE, SYNC_RESULT, ASYNC_RESULT> req) {
			logger.trace("Request input (sync): {}", req);
			SYNC_RESULT result;
			try {
				result = syncApi.requestSync(req);
			} catch (Throwable e) {
				logger.trace("Request failed: {}    Error: {}", req, e.getMessage());
				throw e;
			}
			logger.trace("Request executed: {}    Result: {}", req, result);
			return result;
		}
	}

	private class LoggingAsyncApi implements RocksDBAsyncAPI {

		private final RocksDBAsyncAPI asyncApi;

		public LoggingAsyncApi(RocksDBAsyncAPI asyncApi) {
			this.asyncApi = asyncApi;
		}

		@SuppressWarnings("unchecked")
        @Override
		public <RESULT_ITEM_TYPE, SYNC_RESULT, ASYNC_RESULT> ASYNC_RESULT requestAsync(RocksDBAPICommand<RESULT_ITEM_TYPE, SYNC_RESULT, ASYNC_RESULT> req) {
			if (!logger.isEnabledForLevel(Level.TRACE)) {
				return asyncApi.requestAsync(req);
			} else {
				logger.trace("Request input (async): {}", req);
				var r = asyncApi.requestAsync(req);
				return switch (req) {
					case RocksDBAPICommand.RocksDBAPICommandSingle<?> _ ->
                            (ASYNC_RESULT) ((CompletableFuture<?>) r).whenComplete((result, e) -> {
                                if (e != null) {
                                    logger.trace("Request failed: {}    Error: {}", req, e.getMessage());
                                } else {
                                    logger.trace("Request executed: {}    Result: {}", req, result);
                                }
                            });
					case RocksDBAPICommand.RocksDBAPICommandStream<?> _ ->
                            (ASYNC_RESULT) Flux.from((Publisher<?>) r).doOnEach(signal -> {
                                if (signal.isOnNext()) {
                                    logger.trace("Request: {}    Partial result: {}", req, signal);
                                } else if (signal.isOnError()) {
                                    var e = signal.getThrowable();
                                    assert e != null;
									logger.trace("Request failed: {}    Error: {}", req, e.getMessage());
                                } else if (signal.isOnComplete()) {
                                    logger.trace("Request executed: {}    Result: terminated successfully", req);
                                }
                            });
				};
			}
		}
	}
}
