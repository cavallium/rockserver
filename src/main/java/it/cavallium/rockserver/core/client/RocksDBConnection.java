package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.RequestContext;
import java.io.Closeable;
import java.net.URI;

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
}
