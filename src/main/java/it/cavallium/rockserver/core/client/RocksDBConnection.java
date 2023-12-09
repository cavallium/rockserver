package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RocksDBAPI;
import java.io.Closeable;
import java.net.URI;

public interface RocksDBConnection extends Closeable, RocksDBAPI {

	/**
	 * Get connection url
	 *
	 * @return connection url
	 */
	URI getUrl();
}
