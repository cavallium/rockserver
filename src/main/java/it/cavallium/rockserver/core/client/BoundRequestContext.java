package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RequestContext;
import java.util.Objects;

/** One public operation's immutable client-local monotonic timeout binding. */
record BoundRequestContext(RequestContext value, long startedNanos) {

	private static final BoundRequestContext ANALYTICAL_NO_TIMEOUT = noTimeout(RequestContext.analytical());
	private static final BoundRequestContext INGEST_NO_TIMEOUT = noTimeout(RequestContext.ingest());
	private static final BoundRequestContext BATCH_NO_TIMEOUT = noTimeout(RequestContext.batch());

	BoundRequestContext {
		Objects.requireNonNull(value, "value");
	}

	static BoundRequestContext bind(RequestContext context) {
		return bindAt(context, System.nanoTime());
	}

	static BoundRequestContext bindAt(RequestContext context, long startedNanos) {
		Objects.requireNonNull(context, "context");
		if (!context.hasTimeout()) {
			return switch (context.profile()) {
				case ANALYTICAL -> ANALYTICAL_NO_TIMEOUT;
				case INGEST -> INGEST_NO_TIMEOUT;
				case BATCH -> BATCH_NO_TIMEOUT;
				default -> throw new IllegalArgumentException(
						"Only client-selectable non-latency profiles may omit a timeout");
			};
		}
		return new BoundRequestContext(context, startedNanos);
	}

	private static BoundRequestContext noTimeout(RequestContext context) {
		return new BoundRequestContext(context, 0L);
	}

	long remainingNanos() {
		return remainingNanosAt(System.nanoTime());
	}

	long remainingNanosAt(long nowNanos) {
		long timeoutNanos = value.timeoutNanos();
		if (timeoutNanos == RequestContext.NO_TIMEOUT) {
			return RequestContext.NO_TIMEOUT;
		}
		long elapsedNanos = nowNanos - startedNanos;
		if (elapsedNanos < 0L || elapsedNanos >= timeoutNanos) {
			return 0L;
		}
		return timeoutNanos - elapsedNanos;
	}
}
