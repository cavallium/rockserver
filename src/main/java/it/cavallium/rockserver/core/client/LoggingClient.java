package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
		public <R> R requestSync(RocksDBAPICommand<R> req) {
			logger.trace("Request input (sync): {}", req);
			R result;
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

		@Override
		public <R> CompletableFuture<R> requestAsync(RocksDBAPICommand<R> req) {
			logger.trace("Request input (async): {}", req);
			return asyncApi.requestAsync(req).whenComplete((result, e) -> {
				if (e != null) {
					logger.trace("Request failed: {}    Error: {}", req, e.getMessage());
				} else {
					logger.trace("Request executed: {}    Result: {}", req, result);
				}
			});
		}
	}
}
