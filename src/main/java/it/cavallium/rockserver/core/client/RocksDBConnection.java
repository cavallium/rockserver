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

	/**
	 * Return capabilities implemented by the ultimate backend.
	 *
	 * <p>The conservative default keeps third-party wrappers source compatible
	 * without advertising optional operations that they may not delegate.</p>
	 */
	default RockserverCapabilities getCapabilities() {
		return RockserverCapabilities.COMPATIBLE_BASELINE;
	}

	/** Return a sync view permanently bound to the mandatory request context. */
	RocksDBSyncAPI getSyncApi(RequestContext context);

	/** Return an async view permanently bound to the mandatory request context. */
	RocksDBAsyncAPI getAsyncApi(RequestContext context);
}
