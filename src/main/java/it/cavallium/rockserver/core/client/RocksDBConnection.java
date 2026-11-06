package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.RequestContext;
import java.io.Closeable;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

public interface RocksDBConnection extends Closeable {

	/**
	 * Get connection url
	 *
	 * @return connection url
	 */
	URI getUrl();

	/** Return the exact mandatory workload contract implemented by the ultimate backend. */
	RockserverCapabilities getCapabilities();

	/** Return a sync view permanently bound to the mandatory request context. */
	RocksDBSyncAPI getSyncApi(RequestContext context);

	/** Return an async view permanently bound to the mandatory request context. */
	RocksDBAsyncAPI getAsyncApi(RequestContext context);

	/**
	 * Opens a client session, whose writes carry a {@link it.cavallium.rockserver.core.common.ClientRequestId}: only
	 * where {@link RockserverCapabilities#clientSessions()} is true. Completes with the session id.
	 */
	default CompletableFuture<Long> openClientSessionAsync(RequestContext context) {
		return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED,
				"This server has no client sessions"));
	}
}
