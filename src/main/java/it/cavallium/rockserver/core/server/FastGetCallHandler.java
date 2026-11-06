package it.cavallium.rockserver.core.server;


import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;

import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

/** The unary Get, answered with a response that streams the value without copying it into a protobuf message. */
final class FastGetCallHandler implements ServerCallHandler<GetRequest, FastGetResponse> {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class.getName());

	private final GrpcServer server;

	FastGetCallHandler(GrpcServer server) {
		this.server = server;
	}

	@Override
	public Listener<GetRequest> startCall(ServerCall<GetRequest, FastGetResponse> call, Metadata headers) {
		call.request(2);
		return new FastGetListener(call, Context.current());
	}

	private final class FastGetListener extends Listener<GetRequest>
			implements Runnable, RWScheduler.EstimatedWork, RWScheduler.RejectionAwareTask {

		private static final int CANCELLED = 1;
		private static final int HALF_CLOSED = 1 << 1;
		private static final int TERMINATED = 1 << 2;
		private static final VarHandle STATE;

		static {
			try {
				STATE = MethodHandles.lookup().findVarHandle(FastGetListener.class, "state", int.class);
			} catch (NoSuchFieldException | IllegalAccessException failure) {
				throw new ExceptionInInitializerError(failure);
			}
		}

		private final ServerCall<GetRequest, FastGetResponse> call;
		private final Context callContext;
		private volatile int state;
		private volatile @Nullable Disposable task;
		private @Nullable GetRequest request;
		private @Nullable ResolvedRequestContext requestContext;
		private @Nullable Keys keys;
		private long estimatedBytes;

		private FastGetListener(ServerCall<GetRequest, FastGetResponse> call, Context callContext) {
			this.call = call;
			this.callContext = callContext;
		}

		@Override
		public void onMessage(GetRequest message) {
			if (request != null) {
				cancel();
				call.close(Status.INVALID_ARGUMENT.withDescription("Unary Get received multiple requests"),
						new Metadata());
				return;
			}
			request = message;
		}

		@Override
		public void onHalfClose() {
			if (!markHalfClosed() || isCancelled()) {
				return;
			}
			GetRequest currentRequest = request;
			if (currentRequest == null) {
				cancel();
				call.close(Status.INVALID_ARGUMENT.withDescription("Unary Get received no request"),
						new Metadata());
				return;
			}

			Disposable scheduled;
			try {
				requestContext = server.contexts.mapRequestContext(currentRequest.getContext());
				keys = GrpcProtoMapping.mapKeys(currentRequest.getKeysList());
				var command = new RocksDBAPICommand.RocksDBAPICommandSingle.Get<>(
						currentRequest.getTransactionOrUpdateId(),
						currentRequest.getColumnId(),
						keys,
						RequestType.current());
				var profile = server.contexts.preAdmit(requestContext, OperationFamily.POINT_LOOKUP, command);
				estimatedBytes = command.estimatedBytes();
				scheduled = server.workloadScheduler(server.commandWorkloadScheduler(command), profile,
						command.operationFamily(),
						requestContext.localMonotonicDeadlineNanos())
						.schedule(this);
			} catch (Throwable schedulingError) {
				if (tryTerminate()) {
					closeFastGetFailure(call, currentRequest, schedulingError, this);
				}
				return;
			}
			task = scheduled;
			if (isCancelled()) {
				scheduled.dispose();
			}
		}

		@Override
		public void onCancel() {
			cancel();
			Disposable scheduled = task;
			if (scheduled != null) {
				scheduled.dispose();
			}
		}

		@Override
		public void run() {
			if (!tryTerminate()) {
				return;
			}
			Context previous = callContext.attach();
			try {
				runFastGetCall(call,
						Objects.requireNonNull(request, "request"),
						Objects.requireNonNull(requestContext, "requestContext"),
						Objects.requireNonNull(keys, "keys"),
						this);
			} finally {
				callContext.detach(previous);
			}
		}

		@Override
		public long estimatedBytes() {
			return estimatedBytes;
		}

		@Override
		public void reject(RuntimeException failure) {
			if (tryTerminate()) {
				closeFastGetFailure(call,
						Objects.requireNonNull(request, "request"),
						failure,
						this);
			}
		}

		private boolean markHalfClosed() {
			int current;
			do {
				current = state;
				if ((current & HALF_CLOSED) != 0) {
					return false;
				}
			} while (!STATE.compareAndSet(this, current, current | HALF_CLOSED));
			return true;
		}

		private void cancel() {
			STATE.getAndBitwiseOr(this, CANCELLED);
		}

		private boolean isCancelled() {
			return (state & CANCELLED) != 0;
		}

		private boolean tryTerminate() {
			int current;
			do {
				current = state;
				if ((current & (CANCELLED | TERMINATED)) != 0) {
					return false;
				}
			} while (!STATE.compareAndSet(this, current, current | TERMINATED));
			return true;
		}
	}

	private void runFastGetCall(ServerCall<GetRequest, FastGetResponse> call,
			GetRequest request,
			ResolvedRequestContext context,
			Keys keys,
			FastGetCallHandler.FastGetListener listener) {
		FastGetResponse response = null;
		try {
			if (listener.isCancelled() || call.isCancelled()) {
				return;
			}
			response = server.grpc.createFastGetResponse(request,
					context.value(),
					keys,
					server.grpcGetStrategy,
					server.embeddedDatabase);
			if (listener.isCancelled() || call.isCancelled()) {
				return;
			}
			call.sendHeaders(new Metadata());
			if (listener.isCancelled() || call.isCancelled()) {
				return;
			}
			call.sendMessage(response);
			response.close();
			response = null;
			if (!listener.isCancelled() && !call.isCancelled()) {
				call.close(Status.OK, new Metadata());
			}
		} catch (Throwable error) {
			closeFastGetFailure(call, request, error, listener);
		} finally {
			if (response != null) {
				try {
					response.close();
				} catch (Throwable closeError) {
					if (!listener.isCancelled() && !call.isCancelled()) {
						LOG.error("Failed to close a unary Get response after framing", closeError);
					}
				}
			}
		}
	}

	private void closeFastGetFailure(ServerCall<GetRequest, FastGetResponse> call,
			GetRequest request,
			Throwable error,
			FastGetCallHandler.FastGetListener listener) {
		if (listener.isCancelled() || call.isCancelled()) {
			return;
		}
		listener.cancel();
		Throwable mapped = server.grpc.mapRequestError("get", request, error);
		Status status = Status.fromThrowable(mapped);
		Metadata trailers = Status.trailersFromThrowable(mapped);
		call.close(status, trailers != null ? trailers : new Metadata());
	}
}
