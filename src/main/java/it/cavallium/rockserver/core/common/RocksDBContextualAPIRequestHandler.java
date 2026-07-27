package it.cavallium.rockserver.core.common;

/** Internal dispatch boundary used by a context-bound public API view. */
public interface RocksDBContextualAPIRequestHandler {

	<R, RS, RA> RS requestSync(RequestContext context, RocksDBAPICommand<R, RS, RA> request);

	<R, RS, RA> RA requestAsync(RequestContext context, RocksDBAPICommand<R, RS, RA> request);
}
