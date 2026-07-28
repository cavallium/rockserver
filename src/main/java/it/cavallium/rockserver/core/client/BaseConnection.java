package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBContextualAPIRequestHandler;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

public abstract class BaseConnection implements RocksDBConnection, RocksDBContextualAPIRequestHandler {

	private final String name;
	private final ThreadLocal<RequestContext> dispatchContext = new ThreadLocal<>();

	public BaseConnection(String name) {
		this.name = name;
	}

	@Override
	public final RocksDBSyncAPI getSyncApi(RequestContext context) {
		return new ContextBoundRocksDBAPI(this, Objects.requireNonNull(context, "context"));
	}

	@Override
	public final RocksDBAsyncAPI getAsyncApi(RequestContext context) {
		return new ContextBoundRocksDBAPI(this, Objects.requireNonNull(context, "context"));
	}

	protected final RequestContext currentRequestContext() {
		return Objects.requireNonNull(dispatchContext.get(), "No workload context is bound to this dispatch");
	}

	/**
	 * Compatibility for the still-public raw connection methods. Context-bound API views
	 * always install their immutable context before dispatch; direct internal callers keep
	 * the historical BATCH behavior until the raw API surface is removed.
	 */
	protected final RequestContext currentRequestContextOrBatch() {
		var context = dispatchContext.get();
		return context != null ? context : RequestContext.batch();
	}

	protected final <T> T withRequestContext(RequestContext context, Supplier<T> operation) {
		Objects.requireNonNull(context, "context");
		Objects.requireNonNull(operation, "operation");
		var previous = dispatchContext.get();
		dispatchContext.set(context);
		try {
			return operation.get();
		} finally {
			if (previous == null) {
				dispatchContext.remove();
			} else {
				dispatchContext.set(previous);
			}
		}
	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public String toString() {
		return "db \"" + name + "\" (" + getUrl() + ")";
	}
}
