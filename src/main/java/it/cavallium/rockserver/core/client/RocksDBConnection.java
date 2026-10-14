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

	/** Vector CDC lifecycle; each operation requires its caller's workload and timeout policy. */
	default it.cavallium.rockserver.core.common.cdc.DistributedCdcAsyncAPI getDistributedCdcApi() {
		throw it.cavallium.rockserver.core.common.RocksDBException.of(
				it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
				"Distributed CDC subscriptions are unavailable through this connection");
	}

	/** Distributed mutation preparation, recovery and explicit result acknowledgement. */
	default it.cavallium.rockserver.core.common.distributed.RemoteMutationAsyncAPI getRemoteMutationApi() {
		throw it.cavallium.rockserver.core.common.RocksDBException.of(
				it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
				"Remote mutation ownership is unavailable through this connection");
	}

	/** Return the effective backend features and limits supported through this connection. */
	RockserverCapabilities getCapabilities();

	/** Return a sync view permanently bound to the mandatory request context. */
	RocksDBSyncAPI getSyncApi(RequestContext context);

	/** Return an async view permanently bound to the mandatory request context. */
	RocksDBAsyncAPI getAsyncApi(RequestContext context);
}
