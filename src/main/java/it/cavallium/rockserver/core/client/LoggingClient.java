package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LoggingClient implements RocksDBConnection {

	private final RocksDBConnection client;
	private final LoggingSyncApi syncApi;
	private final LoggingAsyncApi asyncApi;
	private final Logger logger;

	public LoggingClient(RocksDBConnection client) {
		this.client = client;
		this.syncApi = new LoggingSyncApi(client.getSyncApi());
		this.asyncApi = new LoggingAsyncApi(client.getAsyncApi());
		this.logger = Logger.getLogger("db.requests");
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
			R result;
			try {
				result = syncApi.requestSync(req);
			} catch (Throwable e) {
				logger.log(Level.FINEST, "Request failed: {0}    Error: {1}", new Object[] {req, e.getMessage()});
				throw e;
			}
			logger.log(Level.FINEST, "Request executed: {0}    Result: {1}", new Object[] {req, result});
			return result;
		}
	}

	private class LoggingAsyncApi implements RocksDBAsyncAPI {

		private final RocksDBAsyncAPI asyncApi;

		public LoggingAsyncApi(RocksDBAsyncAPI asyncApi) {
			this.asyncApi = asyncApi;
		}

		@Override
		public <R> CompletionStage<R> requestAsync(RocksDBAPICommand<R> req) {
			return asyncApi.requestAsync(req).whenComplete((result, e) -> {
				if (e != null) {
					logger.log(Level.FINEST, "Request failed: {0}    Error: {1}", new Object[] {req, e.getMessage()});
				} else {
					logger.log(Level.FINEST, "Request executed: {0}    Result: {1}", new Object[] {req, result});
				}
			});
		}
	}
}
