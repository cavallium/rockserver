package it.cavallium.rockserver.core.common;

public interface RocksDBSyncAPIRequestHandler {

	default <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> req) {
		throw new UnsupportedOperationException("Unsupported request type: " + req);
	}
}
