package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

abstract class BaseConnection implements RocksDBConnection {

	private final String name;
	private final ThreadLocal<BoundRequestContext> dispatchContext = new ThreadLocal<>();

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
		return currentBoundRequestContext().value();
	}

	protected final BoundRequestContext currentBoundRequestContext() {
		return Objects.requireNonNull(dispatchContext.get(), "No workload context is bound to this dispatch");
	}

	protected final <T> T withRequestContext(BoundRequestContext context, Supplier<T> operation) {
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

	abstract <R, RS, RA> RS requestSync(BoundRequestContext context,
			RocksDBAPICommand<R, RS, RA> request);

	abstract <R, RS, RA> RA requestAsync(BoundRequestContext context,
			RocksDBAPICommand<R, RS, RA> request);

	abstract Mono<CdcBatch> cdcPollBatchAsync(BoundRequestContext context,
			@NotNull String id,
			@Nullable Long fromSeq,
			long maxEvents);

	@Override
	public void close() throws IOException {

	}

	@Override
	public String toString() {
		return "db \"" + name + "\" (" + getUrl() + ")";
	}
}
