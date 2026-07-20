package it.cavallium.rockserver.core.common;

public interface RocksDBSyncAPIRequestHandler {

	default <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> req) {
		throw RocksDBException.of(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
				"Unsupported request type: " + req);
	}
}
