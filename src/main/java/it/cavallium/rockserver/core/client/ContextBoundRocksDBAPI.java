package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import it.cavallium.rockserver.core.impl.WorkloadAdmission;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Public API view with one mandatory immutable workload context. */
final class ContextBoundRocksDBAPI implements RocksDBAPI {

	private final BaseConnection dispatcher;
	private final RequestContext context;

	ContextBoundRocksDBAPI(BaseConnection dispatcher, RequestContext context) {
		this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
		this.context = Objects.requireNonNull(context, "context");
	}

	@Override
	public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> request) {
		Objects.requireNonNull(request, "request");
		WorkloadAdmission.resolve(context, request);
		return dispatcher.requestSync(BoundRequestContext.bind(context), request);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
		Objects.requireNonNull(request, "request");
		WorkloadAdmission.resolve(context, request);
		if (request instanceof RocksDBAPICommand.RocksDBAPICommandStream) {
			return (RA) Flux.defer(() -> Flux.from((Publisher<?>) dispatcher.requestAsync(
					BoundRequestContext.bind(context), request)));
		}
		return dispatcher.requestAsync(BoundRequestContext.bind(context), request);
	}

	@Override
	public Mono<CdcBatch> cdcPollBatchAsync(@NotNull String id, @Nullable Long fromSeq, long maxEvents) {
		// Exact CDC batch cursors are richer than the legacy event-stream default:
		// filtered-empty pages still advance and transports enforce a response budget.
		// Preserve the concrete connection implementation while binding the protected
		// CDC dispatch to this immutable API view.
		return Mono.defer(() -> dispatcher.cdcPollBatchAsync(
				BoundRequestContext.bind(context), id, fromSeq, maxEvents));
	}
}
