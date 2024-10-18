package it.cavallium.rockserver.core.common;

public interface RocksDBSyncAPIRequestHandler {

	default <R> R requestSync(RocksDBAPICommand<R> req) {
		throw new UnsupportedOperationException("Unsupported request type: " + req);
	}
}
