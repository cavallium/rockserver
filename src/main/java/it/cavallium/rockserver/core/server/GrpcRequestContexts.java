package it.cavallium.rockserver.core.server;


import io.grpc.Context;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.WorkloadAdmission;
import java.util.Objects;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/** Maps the wire request context to a {@link it.cavallium.rockserver.core.common.RequestContext} and to the APIs that run under it. */
final class GrpcRequestContexts {

	private final RocksDBConnection client;
	private final RWScheduler scheduler;
	private final @Nullable EmbeddedDB embeddedDatabase;

	private static final it.cavallium.rockserver.core.common.RequestContext[] NO_TIMEOUT_CONTEXTS
			= createNoDeadlineContexts();

	private final RocksDBSyncAPI[] noDeadlineSyncApis = new RocksDBSyncAPI[NO_TIMEOUT_CONTEXTS.length];

	private final RocksDBAsyncAPI[] noDeadlineAsyncApis = new RocksDBAsyncAPI[NO_TIMEOUT_CONTEXTS.length];

	private final RocksDBSyncAPI commandCaptureApi = new RocksDBSyncAPI() {
		@Override
		public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> request) {
			throw new CapturedCommand(request);
		}
	};

	private final class CapturedCommand extends RuntimeException {

		private final RocksDBAPICommand<?, ?, ?> command;

		private CapturedCommand(RocksDBAPICommand<?, ?, ?> command) {
			super(null, null, false, false);
			this.command = command;
		}
	}

	GrpcRequestContexts(RocksDBConnection client, RWScheduler scheduler, @Nullable EmbeddedDB embeddedDatabase) {
		this.client = Objects.requireNonNull(client, "client");
		this.scheduler = scheduler;
		this.embeddedDatabase = embeddedDatabase;
		for (var profile : WorkloadProfile.values()) {
			var context = NO_TIMEOUT_CONTEXTS[profile.ordinal()];
			if (context != null) {
				noDeadlineSyncApis[profile.ordinal()] = client.getSyncApi(context);
				noDeadlineAsyncApis[profile.ordinal()] = client.getAsyncApi(context);
			}
		}
	}

	static it.cavallium.rockserver.core.common.RequestContext[] createNoDeadlineContexts() {
		var contexts = new it.cavallium.rockserver.core.common.RequestContext[WorkloadProfile.values().length];
		contexts[WorkloadProfile.ANALYTICAL.ordinal()]
				= it.cavallium.rockserver.core.common.RequestContext.analytical();
		contexts[WorkloadProfile.INGEST.ordinal()]
				= it.cavallium.rockserver.core.common.RequestContext.ingest();
		contexts[WorkloadProfile.BATCH.ordinal()]
				= it.cavallium.rockserver.core.common.RequestContext.batch();
		return contexts;
	}

	ResolvedRequestContext mapRequestContext(
			it.cavallium.rockserver.core.common.api.proto.RequestContext wireContext) {
		if (wireContext == null || wireContext.getProfileValue() == 0) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Request context and workload profile are required");
		}
		if (wireContext.getWorkloadContractVersion()
				!= RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Request context requires workload contract version "
							+ RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION);
		}
		final WorkloadProfile profile;
		try {
			profile = WorkloadProfile.fromWireValue(wireContext.getProfileValue());
		} catch (IllegalArgumentException unknown) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					unknown.getMessage(), unknown);
		}
		long timeoutNanos = wireContext.getTimeoutNanos();
		if (timeoutNanos <= 0L) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Request timeoutNanos must be positive");
		}
		var transportDeadline = Context.current().getDeadline();
		if (transportDeadline != null) {
			long transportRemainingNanos = Math.max(0L,
					transportDeadline.timeRemaining(TimeUnit.NANOSECONDS));
			timeoutNanos = Math.min(timeoutNanos, transportRemainingNanos);
		}
		if (timeoutNanos == 0L) {
			throw RocksDBException.notApplied(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
					"Request deadline already expired");
		}
		if (!profile.isClientSelectable()) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Workload profile " + profile + " is owned by Rockserver");
		}
		try {
			it.cavallium.rockserver.core.common.RequestContext context;
			if (timeoutNanos == it.cavallium.rockserver.core.common.RequestContext.NO_TIMEOUT) {
				var cached = NO_TIMEOUT_CONTEXTS[profile.ordinal()];
				if (cached != null) {
					context = cached;
				} else {
					context = new it.cavallium.rockserver.core.common.RequestContext(profile,
							timeoutNanos);
				}
			} else {
				context = new it.cavallium.rockserver.core.common.RequestContext(profile, timeoutNanos);
			}
			if (wireContext.hasRequestId()) {
				var id = wireContext.getRequestId();
				context = context.withRequestId(new it.cavallium.rockserver.core.common.ClientRequestId(
						id.getSessionId(), id.getSequence(), id.getCompletedBelow()));
			}
			return new ResolvedRequestContext(context, scheduler.bindTimeoutNanos(timeoutNanos));
		} catch (IllegalArgumentException invalid) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Invalid request context: " + invalid.getMessage(), invalid);
		}
	}

	RocksDBAsyncAPI asyncApi(
			it.cavallium.rockserver.core.common.api.proto.RequestContext context) {
		return asyncApi(mapRequestContext(context));
	}

	RocksDBSyncAPI syncApi(ResolvedRequestContext context) {
		return new RocksDBSyncAPI() {
			@Override
			public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> request) {
				var downstreamContext = remainingContext(context);
				return scheduler.withDeadlineBinding(downstreamContext,
						context.localMonotonicDeadlineNanos(),
						() -> syncApi(downstreamContext).requestSync(request));
			}
		};
	}

	RocksDBAsyncAPI asyncApi(ResolvedRequestContext context) {
		return new RocksDBAsyncAPI() {
			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandStream) {
					return (RA) Flux.defer(() -> Flux.from((Publisher<?>) dispatchAsync(context, request)));
				}
				return dispatchAsync(context, request);
			}

			private <R, RS, RA> RA dispatchAsync(ResolvedRequestContext context,
					RocksDBAPICommand<R, RS, RA> request) {
				var downstreamContext = remainingContext(context);
				return scheduler.withDeadlineBinding(downstreamContext,
						context.localMonotonicDeadlineNanos(),
						() -> asyncApi(downstreamContext).requestAsync(request));
			}
		};
	}

	it.cavallium.rockserver.core.common.RequestContext remainingContext(
			ResolvedRequestContext context) {
		long deadlineNanos = context.localMonotonicDeadlineNanos();
		if (deadlineNanos == Long.MAX_VALUE) {
			return context.value();
		}
		long remainingNanos = scheduler.remainingMonotonicDeadlineNanos(deadlineNanos);
		if (remainingNanos == 0L) {
			throw RocksDBException.notApplied(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
					"Request deadline expired before downstream dispatch");
		}
		return context.value().withTimeoutNanos(remainingNanos);
	}

	RocksDBSyncAPI syncApi(it.cavallium.rockserver.core.common.RequestContext context) {
		// The cached API is bound to a context with no request id: a numbered write must reach the connection with its
		// own context, or it is sent (or proposed) unnumbered and a resend applies twice (review of R58, 1).
		if (!context.hasTimeout() && context.requestId() == null) {
			var cached = noDeadlineSyncApis[context.profile().ordinal()];
			if (cached != null) {
				return cached;
			}
		}
		return client.getSyncApi(context);
	}

	RocksDBAsyncAPI asyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
		// The cached API is bound to a context with no request id: a numbered write must reach the connection with its
		// own context, or it is sent (or proposed) unnumbered and a resend applies twice (review of R58, 1).
		if (!context.hasTimeout() && context.requestId() == null) {
			var cached = noDeadlineAsyncApis[context.profile().ordinal()];
			if (cached != null) {
				return cached;
			}
		}
		return client.getAsyncApi(context);
	}

	RocksDBSyncAPI protectedApi() {
		return noDeadlineSyncApis[WorkloadProfile.BATCH.ordinal()];
	}

	RocksDBAsyncAPI protectedAsyncApi() {
		return noDeadlineAsyncApis[WorkloadProfile.BATCH.ordinal()];
	}

	RocksDBAPICommand<?, ?, ?> captureCommand(Function<RocksDBSyncAPI, ?> operation) {
		try {
			operation.apply(commandCaptureApi);
		} catch (CapturedCommand captured) {
			return captured.command;
		}
		throw new IllegalStateException("gRPC operation did not dispatch a concrete Rockserver command");
	}

	WorkloadProfile resolveCommand(
			it.cavallium.rockserver.core.common.RequestContext context,
			RocksDBAPICommand<?, ?, ?> command) {
		if (embeddedDatabase == null) {
			return WorkloadAdmission.resolve(context, command);
		}
		var settings = embeddedDatabase.getWorkloadSettings();
		return WorkloadAdmission.resolve(context,
				command,
				settings.latencyFanOutMaxItems(),
				settings.latencyFanOutMaxBytes(),
				settings.latencyRangeMaxItems(),
				settings.latencyRangeMaxBytes());
	}

	WorkloadProfile preAdmit(
			it.cavallium.rockserver.core.common.RequestContext context,
			OperationFamily expectedFamily,
			RocksDBAPICommand<?, ?, ?> command) {
		if (command.operationFamily() != expectedFamily) {
			throw new IllegalStateException("gRPC adapter expected " + expectedFamily
					+ " but decoded " + command.operationFamily() + " for "
					+ command.getClass().getSimpleName());
		}
		return resolveCommand(context, command);
	}

	WorkloadProfile preAdmit(ResolvedRequestContext context,
			OperationFamily expectedFamily,
			RocksDBAPICommand<?, ?, ?> command) {
		return preAdmit(context.value(), expectedFamily, command);
	}
}
