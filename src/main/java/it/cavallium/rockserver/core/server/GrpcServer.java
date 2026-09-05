package it.cavallium.rockserver.core.server;

import it.cavallium.rockserver.core.common.SstMaintenanceProto;

import static it.cavallium.rockserver.core.common.Utils.toByteArray;
import static it.cavallium.rockserver.core.common.Utils.toBuf;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Drainable;
import io.grpc.Grpc;
import io.grpc.KnownLength;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.KVBatch.KVBatchRef;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.RequestType.RequestDelete;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.api.proto.Delta;
import it.cavallium.rockserver.core.common.api.proto.FirstAndLast;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.impl.InternalConnection;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.ReactorResultOwnership;
import it.cavallium.rockserver.core.impl.WorkloadAdmission;
import it.unimi.dsi.fastutil.ints.Int2IntFunction;
import it.unimi.dsi.fastutil.ints.Int2ObjectFunction;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import it.cavallium.buffer.Buf;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.SocketAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

public class GrpcServer extends Server {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class.getName());
	private static final String GRPC_LATE_ERROR_HANDLER_CONTEXT_KEY
			= GrpcServer.class.getName() + ".lateErrorHandler";
	private static final Object ITERATOR_OPERATION_LEASE_CONTEXT_KEY = new Object();
	// Reactor exposes sequence-local dropped-error handling through this documented context key, while
	// keeping Hooks.KEY_ON_ERROR_DROPPED package-private.
	private static final String REACTOR_ON_ERROR_DROPPED_CONTEXT_KEY = "reactor.onErrorDropped.local";
	private static final String LATE_PROTECTED_OPERATION_FAILURE_METRIC
			= "rockserver.grpc.protected.late.failures";
	private static final Consumer<Throwable> UNCONTEXTUALIZED_LATE_ERROR_HANDLER
			= GrpcServer::logUncontextualizedLateError;
	private static final long LATE_READ_DEADLINE_LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
	private static final ConcurrentMap<String, LateReadDeadlineLogState> LATE_READ_DEADLINE_LOG_STATES
			= new ConcurrentHashMap<>();

	private record ResolvedRequestContext(
			it.cavallium.rockserver.core.common.RequestContext value,
			long localMonotonicDeadlineNanos) {

		private WorkloadProfile profile() {
			return value.profile();
		}

	}

	private final GrpcServerImpl grpc;
	private final EventLoopGroup elg;
	private final io.grpc.Server server;
	private final RWScheduler scheduler;
	private final boolean ownsScheduler;
	private final @Nullable EmbeddedDB embeddedDatabase;
	private final MeterRegistry metricsRegistry;
	private final GrpcGetStrategy grpcGetStrategy;
	private final MustCompleteOperationTracker mustCompleteOperations = new MustCompleteOperationTracker();
	private final AtomicLong cancelledMustCompleteOperations = new AtomicLong();
	private final ConcurrentMap<String, LongAdder> lateProtectedOperationFailures = new ConcurrentHashMap<>();

	private enum ScheduledCancellationPolicy {
		CANCEL_WHILE_QUEUED,
		MUST_COMPLETE
	}

	private static final class MustCompleteOperationTracker {

		private final Object monitor = new Object();
		private int acceptedOperations;
		private boolean accepting = true;

		private boolean register() {
			synchronized (monitor) {
				if (!accepting) {
					return false;
				}
				acceptedOperations++;
				return true;
			}
		}

		private void operationTerminated() {
			synchronized (monitor) {
				if (acceptedOperations == 0) {
					throw new IllegalStateException("Must-complete operation accounting underflow");
				}
				acceptedOperations--;
				if (acceptedOperations == 0) {
					monitor.notifyAll();
				}
			}
		}

		private DrainResult stopAcceptingAndAwait(Duration timeout) {
			long timeoutNanos = saturatedTimeoutNanos(timeout);
			long remainingNanos = timeoutNanos;
			long waitStartedNanos = System.nanoTime();
			boolean interrupted = false;
			synchronized (monitor) {
				accepting = false;
				while (acceptedOperations != 0) {
					if (remainingNanos <= 0L) {
						return new DrainResult(false, interrupted);
					}
					try {
						TimeUnit.NANOSECONDS.timedWait(monitor, remainingNanos);
					} catch (InterruptedException _) {
						interrupted = true;
					}
					remainingNanos = remainingTimeoutNanos(timeoutNanos, waitStartedNanos);
				}
				return new DrainResult(true, interrupted);
			}
		}

		private int acceptedOperations() {
			synchronized (monitor) {
				return acceptedOperations;
			}
		}
	}

	private record DrainResult(boolean drained, boolean interrupted) {}

	private record AwaitTerminationResult(boolean terminated, boolean interrupted) {}

	private enum GrpcGetStrategy {
		LEGACY,
		EXACT_HEAP,
		PINNED,
		AUTOMATIC;

		private static GrpcGetStrategy configured() {
			String value = System.getProperty("rockserver.grpc.fast-get.strategy", "automatic")
					.strip()
					.toLowerCase(Locale.ROOT);
			return switch (value) {
				case "legacy" -> LEGACY;
				case "exact-heap", "heap" -> EXACT_HEAP;
				case "pinned" -> PINNED;
				case "automatic", "auto" -> AUTOMATIC;
				default -> throw new IllegalArgumentException(
						"Unknown rockserver.grpc.fast-get.strategy: " + value);
			};
		}
	}

	private static void logUncontextualizedLateError(Throwable error) {
		var rocksError = GrpcServerImpl.findRocksDBException(error);
		var status = Status.fromThrowable(error);
		if (rocksError != null) {
			var statusCode = lateErrorStatusCode(status, rocksError);
			Long suppressed = rocksError.getErrorUniqueId() == RocksDBErrorType.READ_DEADLINE_EXCEEDED
					? claimLateReadDeadlineLog("<uncontextualized>")
					: 0L;
			if (suppressed == null) {
				return;
			}
			if (suppressed == 0L) {
				LOG.warn("Late gRPC request failure after call termination without request context: "
						+ "errorType={}, grpcStatus={}, message={}",
						rocksError.getErrorUniqueId(),
						statusCode,
						GrpcServerImpl.sanitizeForLog(rocksError.getMessage()));
			} else {
				LOG.warn("Late gRPC request failure after call termination without request context: "
						+ "errorType={}, grpcStatus={}, message={}, suppressedSimilarFailures={}",
						rocksError.getErrorUniqueId(),
						statusCode,
						GrpcServerImpl.sanitizeForLog(rocksError.getMessage()),
						suppressed);
			}
			LOG.debug("Late gRPC request failure stack without request context", error);
			return;
		}
		if (status.getCode() == Code.CANCELLED
				|| error instanceof java.util.concurrent.CancellationException) {
			LOG.debug("Late gRPC cancellation after call termination without request context: description={}",
					GrpcServerImpl.sanitizeForLog(status.getDescription()));
			return;
		}
		if (status.getCode() != Code.UNKNOWN && status.getCode() != Code.INTERNAL) {
			LOG.warn("Late gRPC transport failure after call termination without request context: "
					+ "grpcStatus={}, description={}",
					status.getCode(),
					GrpcServerImpl.sanitizeForLog(status.getDescription()));
			LOG.debug("Late gRPC transport failure stack without request context", error);
			return;
		}
		LOG.error("Unexpected late gRPC request failure after call termination; request context is unavailable",
				error);
	}

	private static @Nullable Long claimLateReadDeadlineLog(String operation) {
		var state = LATE_READ_DEADLINE_LOG_STATES.computeIfAbsent(operation,
				_ -> new LateReadDeadlineLogState());
		long now = System.nanoTime();
		long previous = state.lastLogNanos.get();
		if ((previous == 0L || now - previous >= LATE_READ_DEADLINE_LOG_INTERVAL_NANOS)
				&& state.lastLogNanos.compareAndSet(previous, now)) {
			return state.suppressed.getAndSet(0L);
		}
		state.suppressed.incrementAndGet();
		return null;
	}

	private static Code lateErrorStatusCode(Status status, RocksDBException rocksError) {
		if (rocksError.getErrorUniqueId() == RocksDBErrorType.READ_DEADLINE_EXCEEDED) {
			return Code.DEADLINE_EXCEEDED;
		}
		return status.getCode() == Code.UNKNOWN ? Code.INTERNAL : status.getCode();
	}

	private void recordLateProtectedOperationFailure(String operation,
			Throwable error,
			ContextView context) {
		lateProtectedOperationFailures.computeIfAbsent(operation, _ -> new LongAdder()).increment();
		metricsRegistry.counter(LATE_PROTECTED_OPERATION_FAILURE_METRIC, "operation", operation).increment();
		GrpcServerImpl.lateErrorHandler(context).accept(error);
	}

	private static final class LateReadDeadlineLogState {

		private final AtomicLong lastLogNanos = new AtomicLong();
		private final AtomicLong suppressed = new AtomicLong();
	}

	public GrpcServer(RocksDBConnection client, SocketAddress socketAddress) throws IOException {
		super(client);
		ExpectedGrpcStreamCloseLogFilter.install();
		if (client instanceof InternalConnection internalConnection) {
			this.scheduler = internalConnection.getScheduler();
			this.ownsScheduler = false;
			this.embeddedDatabase = internalConnection.getEmbeddedDB();
		} else {
			this.scheduler = new RWScheduler(Runtime.getRuntime().availableProcessors(),
					Runtime.getRuntime().availableProcessors(),
					"grpc-db"
			);
			this.ownsScheduler = true;
			this.embeddedDatabase = null;
		}
		this.metricsRegistry = embeddedDatabase != null
				? embeddedDatabase.getMetricsRegistry()
				: Metrics.globalRegistry;
		this.grpcGetStrategy = GrpcGetStrategy.configured();
		this.grpc = new GrpcServerImpl(this.getClient());
		EventLoopGroup elg;
		Class<? extends ServerChannel> channelType;
		if (socketAddress instanceof DomainSocketAddress _) {
			elg = new EpollEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2);
			channelType = EpollServerDomainSocketChannel.class;
		} else {
			elg = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2);
			channelType = NioServerSocketChannel.class;
		}
		this.elg = elg;
		ServerServiceDefinition service = grpcGetStrategy == GrpcGetStrategy.LEGACY
				? grpc.bindService()
				: bindFastGetService();
		this.server = NettyServerBuilder
				.forAddress(socketAddress)
				.bossEventLoopGroup(elg)
				.workerEventLoopGroup(elg)
				.directExecutor()
				.channelType(channelType)
				.withChildOption(ChannelOption.SO_KEEPALIVE, false)
				.maxInboundMessageSize(512 * 1024 * 1024)
				.addService(service)
				.permitKeepAliveWithoutCalls(true)
				.permitKeepAliveTime(5, TimeUnit.SECONDS)
				.intercept(new AdaptiveCompressionInterceptor())
				.build();
		LOG.info("GRPC RocksDB server is listening at " + socketAddress);
	}

	@Override
	public void start() throws IOException {
		server.start();
	}

	public int getPort() {
		return server.getPort();
	}

	@VisibleForTesting
	public int getActiveIteratorOperationLeaseCountForTesting() {
		return grpc.iteratorOperations.size();
	}

	@VisibleForTesting
	public int getAcceptedMustCompleteOperationCountForTesting() {
		return mustCompleteOperations.acceptedOperations();
	}

	@VisibleForTesting
	public long getCancelledMustCompleteOperationCountForTesting() {
		return cancelledMustCompleteOperations.get();
	}

	@VisibleForTesting
	public long getLateProtectedOperationFailureCountForTesting(String operation) {
		var failures = lateProtectedOperationFailures.get(operation);
		return failures == null ? 0L : failures.sum();
	}

	@VisibleForTesting
	public RWScheduler getSchedulerForTesting() {
		return scheduler;
	}

	@VisibleForTesting
	public static void clearLateReadDeadlineLogStatesForTesting() {
		LATE_READ_DEADLINE_LOG_STATES.clear();
	}

	@VisibleForTesting
	public static boolean isExpectedGrpcClientCancellationForTesting(java.util.logging.LogRecord record) {
		return ExpectedGrpcStreamCloseLogFilter.isExpectedClientCancellation(record);
	}

	@VisibleForTesting
	public static void installExpectedGrpcClientCancellationLogFilterForTesting() {
		ExpectedGrpcStreamCloseLogFilter.install();
	}

	private static class AdaptiveCompressionInterceptor implements ServerInterceptor {

		@Override
		public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
				Metadata headers,
				ServerCallHandler<ReqT, RespT> next) {
			SocketAddress remoteAddress = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
			call.setCompression(shouldCompressGrpcResponse(remoteAddress) ? "gzip" : "identity");
			return next.startCall(call, headers);
		}
	}

	@VisibleForTesting
	public static boolean shouldCompressGrpcResponse(@Nullable SocketAddress remoteAddress) {
		if (remoteAddress instanceof DomainSocketAddress) {
			return false;
		}
		if (remoteAddress instanceof InetSocketAddress inetSocketAddress) {
			var address = inetSocketAddress.getAddress();
			if (address != null) {
				return !address.isLoopbackAddress();
			}
			return !inetSocketAddress.getHostString().equalsIgnoreCase("localhost");
		}
		return true;
	}

	private ServerServiceDefinition bindFastGetService() {
		ServerServiceDefinition generated = grpc.bindService();
		var builder = ServerServiceDefinition.builder(generated.getServiceDescriptor().getName());
		String getMethodName = RocksDBServiceGrpc.getGetMethod().getFullMethodName();
		for (ServerMethodDefinition<?, ?> method : generated.getMethods()) {
			if (method.getMethodDescriptor().getFullMethodName().equals(getMethodName)) {
				builder.addMethod(fastGetMethodDescriptor(), new FastGetCallHandler());
			} else {
				addUnchangedMethod(builder, method);
			}
		}
		return builder.build();
	}

	private static MethodDescriptor<GetRequest, FastGetResponse> fastGetMethodDescriptor() {
		var generated = RocksDBServiceGrpc.getGetMethod();
		return generated.toBuilder(generated.getRequestMarshaller(), FastGetResponseMarshaller.INSTANCE).build();
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private static void addUnchangedMethod(ServerServiceDefinition.Builder builder,
			ServerMethodDefinition<?, ?> method) {
		builder.addMethod((ServerMethodDefinition) method);
	}

	private final class FastGetCallHandler implements ServerCallHandler<GetRequest, FastGetResponse> {

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
					requestContext = grpc.mapRequestContext(currentRequest.getContext());
					keys = GrpcServerImpl.mapKeys(currentRequest.getKeysList());
					var command = new RocksDBAPICommand.RocksDBAPICommandSingle.Get<>(
							currentRequest.getTransactionOrUpdateId(),
							currentRequest.getColumnId(),
							keys,
							RequestType.current());
					var profile = grpc.preAdmit(requestContext, OperationFamily.POINT_LOOKUP, command);
					estimatedBytes = command.estimatedBytes();
					scheduled = scheduler.scheduler(profile,
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
			response = grpc.createFastGetResponse(request,
					context.value(),
					keys,
					grpcGetStrategy,
					embeddedDatabase);
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
		Throwable mapped = grpc.mapRequestError("get", request, error);
		Status status = Status.fromThrowable(mapped);
		Metadata trailers = Status.trailersFromThrowable(mapped);
		call.close(status, trailers != null ? trailers : new Metadata());
	}

	private static final class FastGetResponse implements AutoCloseable {

		private static final VarHandle CLOSED;

		static {
			try {
				CLOSED = MethodHandles.lookup().findVarHandle(FastGetResponse.class, "closed", boolean.class);
			} catch (NoSuchFieldException | IllegalAccessException failure) {
				throw new ExceptionInInitializerError(failure);
			}
		}

		private final boolean present;
		private final @Nullable Buf value;
		private final boolean pinned;
		private final @Nullable EmbeddedDB.FastGetResult owner;
		private volatile boolean closed;

		private FastGetResponse(boolean present,
				@Nullable Buf value,
				boolean pinned,
				@Nullable EmbeddedDB.FastGetResult owner) {
			if (present != (value != null)) {
				throw new IllegalArgumentException("present Get responses must have a value, including empty values");
			}
			this.present = present;
			this.value = value;
			this.pinned = pinned;
			this.owner = owner;
		}

		private InputStream openStream() {
			if (closed) {
				throw new IllegalStateException("Get response has already been closed");
			}
			return new FastGetInputStream(present, value, pinned);
		}

		@Override
		public void close() {
			if (CLOSED.compareAndSet(this, false, true) && owner != null) {
				owner.close();
			}
		}
	}

	private enum FastGetResponseMarshaller implements MethodDescriptor.Marshaller<FastGetResponse> {
		INSTANCE;

		@Override
		public InputStream stream(FastGetResponse value) {
			return value.openStream();
		}

		@Override
		public FastGetResponse parse(InputStream stream) {
			try {
				GetResponse parsed = GetResponse.parseFrom(stream);
				return parsed.hasValue()
						? new FastGetResponse(true, Buf.wrap(parsed.getValue().toByteArray()), false, null)
						: new FastGetResponse(false, null, false, null);
			} catch (IOException exception) {
				throw Status.INTERNAL.withDescription("Invalid Get response").withCause(exception).asRuntimeException();
			}
		}
	}

	private static final class FastGetInputStream extends InputStream implements Drainable, KnownLength {

		private static final int COPY_CHUNK_BYTES = 16 * 1024;
		private static final byte[] EMPTY_PREFIX = new byte[0];
		private static final ThreadLocal<byte[]> COPY_CHUNK = ThreadLocal.withInitial(
				() -> new byte[COPY_CHUNK_BYTES]);

		private final byte[] prefix;
		private final @Nullable Buf value;
		private final boolean pinned;
		private int prefixOffset;
		private int valueOffset;

		private FastGetInputStream(boolean present, @Nullable Buf value, boolean pinned) {
			this.value = value;
			this.pinned = pinned;
			this.prefix = present ? protobufBytesPrefix(Objects.requireNonNull(value).size()) : EMPTY_PREFIX;
			long totalLength = (long) prefix.length + (value != null ? value.size() : 0L);
			if (totalLength > Integer.MAX_VALUE) {
				throw new IllegalArgumentException("Get response exceeds the gRPC message length limit: " + totalLength);
			}
		}

		@Override
		public int read() {
			if (prefixOffset < prefix.length) {
				return prefix[prefixOffset++] & 0xff;
			}
			if (value == null || valueOffset >= value.size()) {
				return -1;
			}
			return value.getByte(valueOffset++) & 0xff;
		}

		@Override
		public int read(byte[] target, int offset, int length) {
			Objects.checkFromIndexSize(offset, length, target.length);
			if (length == 0) {
				return 0;
			}
			int copied = 0;
			if (prefixOffset < prefix.length) {
				int count = Math.min(length, prefix.length - prefixOffset);
				System.arraycopy(prefix, prefixOffset, target, offset, count);
				prefixOffset += count;
				offset += count;
				length -= count;
				copied += count;
			}
			if (length > 0 && value != null && valueOffset < value.size()) {
				int count = Math.min(length, value.size() - valueOffset);
				if (pinned) {
					MemorySegment.copy(value.asMemorySegmentStrict(),
							ValueLayout.JAVA_BYTE,
							valueOffset,
							target,
							offset,
							count);
				} else {
					System.arraycopy(value.getBackingByteArray(),
							value.getBackingByteArrayOffset() + valueOffset,
							target,
							offset,
							count);
				}
				valueOffset += count;
				copied += count;
			}
			return copied == 0 ? -1 : copied;
		}

		@Override
		public int drainTo(OutputStream target) throws IOException {
			int written = 0;
			if (prefixOffset < prefix.length) {
				int count = prefix.length - prefixOffset;
				target.write(prefix, prefixOffset, count);
				prefixOffset = prefix.length;
				written += count;
			}
			if (value == null || valueOffset >= value.size()) {
				return written;
			}
			int remaining = value.size() - valueOffset;
			if (!pinned) {
				target.write(value.getBackingByteArray(),
						value.getBackingByteArrayOffset() + valueOffset,
						remaining);
				valueOffset += remaining;
				return written + remaining;
			}

			MemorySegment segment = value.asMemorySegmentStrict();
			byte[] chunk = COPY_CHUNK.get();
			while (remaining > 0) {
				int count = Math.min(remaining, chunk.length);
				MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, valueOffset, chunk, 0, count);
				target.write(chunk, 0, count);
				valueOffset += count;
				remaining -= count;
				written += count;
			}
			return written;
		}

		@Override
		public int available() {
			return prefix.length - prefixOffset + (value != null ? value.size() - valueOffset : 0);
		}

		private static byte[] protobufBytesPrefix(int valueLength) {
			byte[] result = new byte[1 + varintSize(valueLength)];
			result[0] = 0x0a;
			int value = valueLength;
			int index = 1;
			while ((value & ~0x7f) != 0) {
				result[index++] = (byte) ((value & 0x7f) | 0x80);
				value >>>= 7;
			}
			result[index] = (byte) value;
			return result;
		}

		private static int varintSize(int value) {
			int size = 1;
			while ((value & ~0x7f) != 0) {
				size++;
				value >>>= 7;
			}
			return size;
		}
	}


	private final class GrpcServerImpl extends ReactorRocksDBServiceGrpc.RocksDBServiceImplBase {

		private static final long ITERATOR_VALUE_PAGE_SIZE = 64L;
		private static final long ITERATOR_ADVANCE_STEP_SIZE = 4_096L;
		private static final long DEFAULT_ITERATOR_QUANTUM_BYTES = 8L * 1_024L * 1_024L;
		private static final long DEFAULT_ITERATOR_QUANTUM_NANOS = TimeUnit.MILLISECONDS.toNanos(8L);
		private static final int WRITE_ELISION_MULTI_STEP_SIZE = 4_096;
		private static final it.cavallium.rockserver.core.common.RequestContext[] NO_TIMEOUT_CONTEXTS
				= createNoDeadlineContexts();

		private final RocksDBConnection client;
		private final RocksDBSyncAPI[] noDeadlineSyncApis = new RocksDBSyncAPI[NO_TIMEOUT_CONTEXTS.length];
		private final RocksDBAsyncAPI[] noDeadlineAsyncApis = new RocksDBAsyncAPI[NO_TIMEOUT_CONTEXTS.length];
		private final ConcurrentMap<Long, Object> iteratorOperations = new ConcurrentHashMap<>();
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

		public GrpcServerImpl(RocksDBConnection client) {
			this.client = Objects.requireNonNull(client, "client");
			for (var profile : WorkloadProfile.values()) {
				var context = NO_TIMEOUT_CONTEXTS[profile.ordinal()];
				if (context != null) {
					noDeadlineSyncApis[profile.ordinal()] = client.getSyncApi(context);
					noDeadlineAsyncApis[profile.ordinal()] = client.getAsyncApi(context);
				}
			}
		}

		private static it.cavallium.rockserver.core.common.RequestContext[] createNoDeadlineContexts() {
			var contexts = new it.cavallium.rockserver.core.common.RequestContext[WorkloadProfile.values().length];
			contexts[WorkloadProfile.ANALYTICAL.ordinal()]
					= it.cavallium.rockserver.core.common.RequestContext.analytical();
			contexts[WorkloadProfile.INGEST.ordinal()]
					= it.cavallium.rockserver.core.common.RequestContext.ingest();
			contexts[WorkloadProfile.BATCH.ordinal()]
					= it.cavallium.rockserver.core.common.RequestContext.batch();
			return contexts;
		}

		private ResolvedRequestContext mapRequestContext(
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
				throw RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
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
				return new ResolvedRequestContext(context, scheduler.bindTimeoutNanos(timeoutNanos));
			} catch (IllegalArgumentException invalid) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Invalid request context: " + invalid.getMessage(), invalid);
			}
		}

		private RocksDBAsyncAPI asyncApi(
				it.cavallium.rockserver.core.common.api.proto.RequestContext context) {
			return asyncApi(mapRequestContext(context));
		}

		private RocksDBSyncAPI syncApi(ResolvedRequestContext context) {
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

		private RocksDBAsyncAPI asyncApi(ResolvedRequestContext context) {
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

				@Override
				public Mono<CdcBatch> cdcPollBatchAsync(String id, Long fromSeq, long maxEvents) {
					return Mono.defer(() -> {
						var downstreamContext = remainingContext(context);
						return scheduler.withDeadlineBinding(downstreamContext,
								context.localMonotonicDeadlineNanos(),
								() -> asyncApi(downstreamContext).cdcPollBatchAsync(id, fromSeq, maxEvents));
					});
				}
			};
		}

		private it.cavallium.rockserver.core.common.RequestContext remainingContext(
				ResolvedRequestContext context) {
			long deadlineNanos = context.localMonotonicDeadlineNanos();
			if (deadlineNanos == Long.MAX_VALUE) {
				return context.value();
			}
			long remainingNanos = scheduler.remainingMonotonicDeadlineNanos(deadlineNanos);
			if (remainingNanos == 0L) {
				throw RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
						"Request deadline expired before downstream dispatch");
			}
			return new it.cavallium.rockserver.core.common.RequestContext(
					context.profile(), remainingNanos);
		}

		private reactor.core.scheduler.Scheduler contextualScheduler(
				ResolvedRequestContext context,
				OperationFamily family) {
			return scheduler.scheduler(context.profile(),
					family,
					context.localMonotonicDeadlineNanos());
		}

		private RocksDBSyncAPI syncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			if (!context.hasTimeout()) {
				var cached = noDeadlineSyncApis[context.profile().ordinal()];
				if (cached != null) {
					return cached;
				}
			}
			return client.getSyncApi(context);
		}

		private RocksDBAsyncAPI asyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			if (!context.hasTimeout()) {
				var cached = noDeadlineAsyncApis[context.profile().ordinal()];
				if (cached != null) {
					return cached;
				}
			}
			return client.getAsyncApi(context);
		}

		private RocksDBSyncAPI protectedApi() {
			return noDeadlineSyncApis[WorkloadProfile.BATCH.ordinal()];
		}

		private RocksDBAsyncAPI protectedAsyncApi() {
			return noDeadlineAsyncApis[WorkloadProfile.BATCH.ordinal()];
		}

		private RocksDBAPICommand<?, ?, ?> captureCommand(Function<RocksDBSyncAPI, ?> operation) {
			try {
				operation.apply(commandCaptureApi);
			} catch (CapturedCommand captured) {
				return captured.command;
			}
			throw new IllegalStateException("gRPC operation did not dispatch a concrete Rockserver command");
		}

		private WorkloadProfile resolveCommand(
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

		private WorkloadProfile preAdmit(
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

		private WorkloadProfile preAdmit(ResolvedRequestContext context,
				OperationFamily expectedFamily,
				RocksDBAPICommand<?, ?, ?> command) {
			return preAdmit(context.value(), expectedFamily, command);
		}

		// functions

		@Override
		public Mono<CapabilitiesResponse> getCapabilities(CapabilitiesRequest request) {
			var capabilities = client.getCapabilities();
			return Mono.just(CapabilitiesResponse.newBuilder()
					.setWorkloadContractVersion(capabilities.workloadContractVersion())
					.build());
		}

		@Override
		public Mono<OpenTransactionResponse> openTransaction(OpenTransactionRequest request) {
			return executeSync(request.getContext(), OperationFamily.METADATA, contextualApi -> {
				var txId = contextualApi.openTransaction(
						java.time.Duration.ofNanos(request.getTransactionLeaseTtlNanos()));
				return OpenTransactionResponse.newBuilder().setTransactionId(txId).build();
			}).transform(this.onErrorMapMonoWithRequestInfo("openTransaction", request));
		}

		@Override
		public Mono<CloseTransactionResponse> closeTransaction(CloseTransactionRequest request) {
			return Mono.defer(() -> {
				if (request.getCommit()) {
					return executeSync(request.getContext(), OperationFamily.MUTATION, contextualApi -> {
						var committed = contextualApi.closeTransaction(request.getTransactionId(), true);
						return CloseTransactionResponse.newBuilder().setSuccessful(committed).build();
					});
				}
				requireV3(request.getContext().getWorkloadContractVersion());
				return executeMustComplete("rollback",
						() -> {
							var rolledBack = protectedApi().closeTransaction(request.getTransactionId(), false);
							return CloseTransactionResponse.newBuilder().setSuccessful(rolledBack).build();
						},
						scheduler.scheduler(WorkloadProfile.CONTROL, OperationFamily.CONTROL,
								Long.MAX_VALUE));
			}).transform(this.onErrorMapMonoWithRequestInfo("closeTransaction", request));
		}

		@Override
		public Mono<Empty> closeFailedUpdate(CloseFailedUpdateRequest request) {
			requireV3(request.getWorkloadContractVersion());
			return executeMustComplete("closeFailedUpdate", () -> {
				protectedApi().closeFailedUpdate(request.getUpdateId());
				return Empty.getDefaultInstance();
			}, scheduler.scheduler(WorkloadProfile.CONTROL, OperationFamily.CONTROL, Long.MAX_VALUE))
					.transform(this.onErrorMapMonoWithRequestInfo("closeFailedUpdate", request));
		}

		@Override
		public Mono<CreateColumnResponse> createColumn(CreateColumnRequest request) {
			return executeWrite(request.getContext(), contextualApi -> {
				var colId = contextualApi.createColumn(request.getName(), mapColumnSchema(request.getSchema()));
				return CreateColumnResponse.newBuilder().setColumnId(colId).build();
			}).transform(this.onErrorMapMonoWithRequestInfo("createColumn", request));
		}

		@Override
		public Mono<Empty> deleteColumn(DeleteColumnRequest request) {
			return executeWrite(request.getContext(), contextualApi -> {
				contextualApi.deleteColumn(request.getColumnId());
				return Empty.getDefaultInstance();
			}).transform(this.onErrorMapMonoWithRequestInfo("deleteColumn", request));
		}

		@Override
		public Mono<DeleteColumnIfExistsResponse> deleteColumnIfExists(DeleteColumnIfExistsRequest request) {
			return executeWrite(request.getContext(), contextualApi -> DeleteColumnIfExistsResponse.newBuilder()
					.setDeleted(contextualApi.deleteColumnIfExists(request.getName()))
					.build())
					.transform(this.onErrorMapMonoWithRequestInfo("deleteColumnIfExists", request));
		}

		@Override
		public Mono<GetColumnIdResponse> getColumnId(GetColumnIdRequest request) {
			return executeSync(request.getContext(), OperationFamily.METADATA, contextualApi -> {
				var colId = contextualApi.getColumnId(request.getName());
				return GetColumnIdResponse.newBuilder().setColumnId(colId).build();
			}).transform(this.onErrorMapMonoWithRequestInfo("getColumnId", request));
		}

		@Override
		public Mono<EntriesCount> estimateNumKeys(EstimateNumKeysRequest request) {
			return executeSync(request.getContext(), OperationFamily.METADATA, contextualApi -> EntriesCount.newBuilder()
					.setCount(contextualApi.estimateNumKeys(request.getColumnId()))
					.build())
					.transform(this.onErrorMapMonoWithRequestInfo("estimateNumKeys", request));
		}

		@Override
		public Mono<Empty> put(PutRequest request) {
			return executeWrite(request.getContext(), contextualApi -> {
				contextualApi.put(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getData().getKeysList()),
						toBuf(request.getData().getValue()),
						RequestType.none()
				);
				return Empty.getDefaultInstance();
			}).transform(this.onErrorMapMonoWithRequestInfo("put", request));
		}

		@Override
		public Mono<Empty> putEnsure(PutRequest request) {
			return executeWrite(request.getContext(), contextualApi -> {
				contextualApi.put(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getData().getKeysList()),
						toBuf(request.getData().getValue()),
						RequestType.ensure());
				return Empty.getDefaultInstance();
			}).transform(this.onErrorMapMonoWithRequestInfo("putEnsure", request));
		}

		@Override
		public Mono<Empty> delete(DeleteRequest request) {
			return executeWrite(request.getContext(), contextualApi -> {
				contextualApi.delete(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getKeysList()),
						RequestType.none()
				);
				return Empty.getDefaultInstance();
			}).transform(this.onErrorMapMonoWithRequestInfo("delete", request));
		}

		@Override
		public Mono<Empty> deleteRange(DeleteRangeRequest request) {
			return executeWrite(request.getContext(), contextualApi -> {
				contextualApi.deleteRange(request.getColumnId(),
						mapKeys(request.getStartKeysInclusiveList()),
						mapKeys(request.getEndKeysExclusiveList())
				);
				return Empty.getDefaultInstance();
			}).transform(this.onErrorMapMonoWithRequestInfo("deleteRange", request));
		}

		@Override
		public Mono<ExistsMultiResponse> existsMulti(ExistsMultiRequest request) {
			return Mono.defer(() -> {
					var keys = request.getKeysMultiList().stream()
							.map(keyTuple -> mapKeys(keyTuple.getKeysList()))
							.toList();
					return fromCancellableFuture(asyncApi(request.getContext()).existsMultiAsync(
							request.getTransactionId(),
							request.getColumnId(),
							keys));
				})
					.map(present -> ExistsMultiResponse.newBuilder().addAllPresent(present).build())
					.transform(this.onErrorMapMonoWithRequestInfo("existsMulti", request));
		}

		@Override
		public Mono<Empty> merge(MergeRequest request) {
			return executeWrite(request.getContext(), contextualApi -> {
				contextualApi.merge(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getData().getKeysList()),
						toBuf(request.getData().getValue()),
						RequestType.none()
				);
				return Empty.getDefaultInstance();
			}).transform(this.onErrorMapMonoWithRequestInfo("merge", request));
		}

		@Override
		public Mono<Empty> putBatch(Flux<PutBatchRequest> request) {
			return request.switchOnFirst((firstSignal, requestFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
                    assert firstValue != null;
                    if (!firstValue.hasInitialRequest()) {
						return Mono.<Empty>error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var mode = switch (initialRequest.getMode()) {
						case WRITE_BATCH -> PutBatchMode.WRITE_BATCH;
						case WRITE_BATCH_NO_WAL -> PutBatchMode.WRITE_BATCH_NO_WAL;
						case SST_INGESTION -> PutBatchMode.SST_INGESTION;
						case SST_INGEST_BEHIND -> PutBatchMode.SST_INGEST_BEHIND;
						case UNRECOGNIZED -> throw new UnsupportedOperationException("Unrecognized request \"mode\"");
					};

					var batches = requestFlux
							.skip(1) // skip initial request
							.<KVBatch>handle((putBatchRequest, sink) -> {
								if (!putBatchRequest.hasData()) {
									sink.error( RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
											"Multiple initial requests"));
									return;
								}
								var batch = putBatchRequest.getData();
								try {
									sink.next(mapKVBatch(batch.getEntriesList()));
								} catch (Throwable ex) {
									sink.error(ex);
								}
							});

					return Mono
							.fromFuture(() -> asyncApi(initialRequest.getContext()).putBatchAsync(
									initialRequest.getColumnId(), batches, mode))
							.transform(this.onErrorMapMonoWithRequestInfo("putBatch", initialRequest));
				} else if (firstSignal.isOnComplete()) {
					return Mono.error(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				} else {
					return requestFlux;
				}
			}).then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Mono<Empty> mergeBatch(Flux<MergeBatchRequest> request) {
			return request.switchOnFirst((firstSignal, requestFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
					assert firstValue != null;
					if (!firstValue.hasInitialRequest()) {
						return Mono.<Empty>error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var mode = switch (initialRequest.getMode()) {
						case MERGE_WRITE_BATCH -> MergeBatchMode.MERGE_WRITE_BATCH;
						case MERGE_WRITE_BATCH_NO_WAL -> MergeBatchMode.MERGE_WRITE_BATCH_NO_WAL;
						case MERGE_SST_INGESTION -> MergeBatchMode.MERGE_SST_INGESTION;
						case MERGE_SST_INGEST_BEHIND -> MergeBatchMode.MERGE_SST_INGEST_BEHIND;
						case UNRECOGNIZED -> throw new UnsupportedOperationException("Unrecognized request \"mode\"");
					};

					var batches = requestFlux
							.skip(1) // skip initial request
							.<KVBatch>handle((mergeBatchRequest, sink) -> {
								if (!mergeBatchRequest.hasData()) {
									sink.error( RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
											"Multiple initial requests"));
									return;
								}
								var batch = mergeBatchRequest.getData();
								try {
									sink.next(mapKVBatch(batch.getEntriesList()));
								} catch (Throwable ex) {
									sink.error(ex);
								}
							});

					return Mono
							.fromFuture(() -> asyncApi(initialRequest.getContext()).mergeBatchAsync(
									initialRequest.getColumnId(), batches, mode))
							.transform(this.onErrorMapMonoWithRequestInfo("mergeBatch", initialRequest));
				} else if (firstSignal.isOnComplete()) {
					return Mono.error(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				} else {
					return requestFlux;
				}
			}).then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Mono<Empty> deleteMulti(Flux<DeleteMultiRequest> request) {
			return request.switchOnFirst((firstSignal, requestsFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
					assert firstValue != null;
					if (!firstValue.hasInitialRequest()) {
						return Mono.<Empty>error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var dataFlux = requestsFlux
							.skip(1)
							.map(deleteRequest -> {
								if (!deleteRequest.hasData()) {
									throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
								}
								return deleteRequest.getData();
							});
					return deleteMultiDataFlux(initialRequest, dataFlux, "deleteMulti");
				} else if (firstSignal.isOnComplete()) {
					return Mono.error(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				} else {
					return requestsFlux;
				}
			}).then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Flux<Previous> deleteMultiGetPrevious(Flux<DeleteMultiRequest> request) {
			return deleteMultiResponseFlux(request, "deleteMultiGetPrevious", RequestType.previous(), previous -> {
				var builder = Previous.newBuilder();
				if (previous != null) {
					builder.setPrevious(Utils.toByteString(previous));
				}
				return builder.build();
			});
		}

		@Override
		public Flux<PreviousPresence> deleteMultiGetPreviousPresence(Flux<DeleteMultiRequest> request) {
			return deleteMultiResponseFlux(request,
					"deleteMultiGetPreviousPresence",
					RequestType.previousPresence(),
					present -> PreviousPresence.newBuilder().setPresent(present).build());
		}

		private Mono<Empty> deleteMultiDataFlux(DeleteMultiInitialRequest initialRequest,
				Flux<DeleteRequest> dataFlux,
				String requestName) {
			var context = mapRequestContext(initialRequest.getContext());
			if (context.profile() == WorkloadProfile.LATENCY) {
				var contextualApi = asyncApi(context);
				return collectLatencyMulti(dataFlux, GrpcServerImpl::encodedInputBytes, requestName)
						.flatMap(data -> Mono.fromFuture(() -> {
							var keys = new ArrayList<Keys>(data.size());
							for (var value : data) {
								keys.add(mapKeys(value.getKeysList()));
							}
							return contextualApi.deleteMultiAsync(
									initialRequest.getTransactionOrUpdateId(),
									initialRequest.getColumnId(),
									keys,
									RequestType.none());
						}))
						.thenReturn(Empty.getDefaultInstance())
						.transform(this.onErrorMapMonoWithRequestInfo(requestName, initialRequest));
			}
			var contextualApi = syncApi(context);
			return dataFlux
					.publishOn(contextualScheduler(context, OperationFamily.MUTATION))
					.doOnNext(data -> contextualApi.delete(initialRequest.getTransactionOrUpdateId(),
							initialRequest.getColumnId(),
							mapKeys(data.getKeysList()),
								RequestType.none()))
					.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest))
					.then(Mono.just(Empty.getDefaultInstance()));
		}

		private <T, R> Flux<R> deleteMultiResponseFlux(Flux<DeleteMultiRequest> request,
				String requestName,
				RequestDelete<? super Buf, T> requestType,
				Function<T, R> mapper) {
			return request.switchOnFirst((firstSignal, requestsFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
					assert firstValue != null;
					if (!firstValue.hasInitialRequest()) {
						return Flux.error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var context = mapRequestContext(initialRequest.getContext());
					var dataFlux = requestsFlux
							.skip(1)
							.map(deleteRequest -> {
								if (!deleteRequest.hasData()) {
									throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
								}
								return deleteRequest.getData();
							});
					if (context.profile() == WorkloadProfile.LATENCY) {
						var contextualApi = asyncApi(context);
						return collectLatencyMulti(dataFlux, GrpcServerImpl::encodedInputBytes, requestName)
								.flatMapMany(data -> {
									var keys = new ArrayList<Keys>(data.size());
									for (var value : data) {
										keys.add(mapKeys(value.getKeysList()));
									}
									return Mono.fromFuture(() -> contextualApi.deleteMultiAsync(
											initialRequest.getTransactionOrUpdateId(),
											initialRequest.getColumnId(),
											keys,
											requestType))
										.flatMapMany(Flux::fromIterable)
										.map(mapper);
								})
								.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest));
					}
					var contextualApi = syncApi(context);
					return dataFlux
							.publishOn(contextualScheduler(context, OperationFamily.MUTATION))
							.map(data -> mapper.apply(contextualApi.delete(initialRequest.getTransactionOrUpdateId(),
									initialRequest.getColumnId(),
									mapKeys(data.getKeysList()),
									requestType)))
							.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest));
				} else {
					return Flux.error(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				}
			});
		}

		@Override
		public Mono<Merged> mergeGetMerged(MergeRequest request) {
			return executeWrite(request.getContext(), contextualApi -> {
				var merged = contextualApi.merge(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getData().getKeysList()),
						toBuf(request.getData().getValue()),
						RequestType.merged());
				return Merged.newBuilder()
						.setMerged(merged != null ? unmapValueHeap(merged) : ByteString.EMPTY)
						.build();
			}).transform(this.onErrorMapMonoWithRequestInfo("mergeGetMerged", request));
		}

		@Override
		public Mono<Empty> putMultiList(PutMultiListRequest request) {
			return executePutMultiList(request, RequestType.none(), "putMultiList");
		}

		@Override
		public Mono<Empty> putMultiListEnsure(PutMultiListRequest request) {
			return executePutMultiList(request, RequestType.ensure(), "putMultiListEnsure");
		}

		private Mono<Empty> executePutMultiList(PutMultiListRequest request,
				RequestType.RequestPut<? super Buf, ?> requestType,
				String requestName) {
			var initialRequest = request.getInitialRequest();
			var batch = mapKVBatch(request.getDataList());
			return executeWrite(initialRequest.getContext(), contextualApi -> {
				contextualApi.putMulti(initialRequest.getTransactionOrUpdateId(),
						initialRequest.getColumnId(),
						batch.keys(),
						batch.values(),
						requestType);
				return Empty.getDefaultInstance();
			}).transform(this.onErrorMapMonoWithRequestInfo(requestName, initialRequest));
		}

		@Override
		public Mono<Empty> mergeMulti(Flux<MergeMultiRequest> request) {
			return request.switchOnFirst((firstSignal, requestsFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
					assert firstValue != null;
					if (!firstValue.hasInitialRequest()) {
						return Mono.<Empty>error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var dataFlux = requestsFlux
							.skip(1) // skip the initial request
							.map(mergeRequest -> {
								if (!mergeRequest.hasData()) {
									throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
								}
								return mergeRequest.getData();
							});
					return mergeMultiDataFlux(initialRequest, dataFlux, "mergeMulti");
				} else if (firstSignal.isOnComplete()) {
					return Mono.error(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				} else {
					return requestsFlux;
				}
			}).then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Flux<Merged> mergeMultiGetMerged(Flux<MergeMultiRequest> request) {
			return request.switchOnFirst((firstSignal, requestsFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
					assert firstValue != null;
					if (!firstValue.hasInitialRequest()) {
						return Flux.error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var context = mapRequestContext(initialRequest.getContext());
					var dataFlux = requestsFlux
							.skip(1) // skip the initial request
							.map(mergeRequest -> {
								if (!mergeRequest.hasData()) {
									throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
								}
								return mergeRequest.getData();
							});
					if (context.profile() == WorkloadProfile.LATENCY) {
						var contextualApi = asyncApi(context);
						return collectLatencyMulti(dataFlux, GrpcServerImpl::encodedInputBytes,
								"mergeMultiGetMerged")
								.flatMapMany(data -> {
									var batch = mapKVBatch(data);
									return Mono.fromFuture(() -> contextualApi.mergeMultiAsync(
											initialRequest.getTransactionOrUpdateId(),
											initialRequest.getColumnId(),
											batch.keys(),
											batch.values(),
											RequestType.merged()))
										.flatMapMany(Flux::fromIterable)
										.map(merged -> Merged.newBuilder()
												.setMerged(merged != null ? unmapValueHeap(merged) : ByteString.EMPTY)
												.build());
								})
								.onErrorMap(ex -> this.handleError(ex).asRuntimeException());
					}
					var contextualApi = syncApi(context);
					return dataFlux
							.publishOn(contextualScheduler(context, OperationFamily.MUTATION))
							.map(data -> {
								var merged = contextualApi.merge(initialRequest.getTransactionOrUpdateId(),
										initialRequest.getColumnId(),
										mapKeys(data.getKeysList()),
										toBuf(data.getValue()),
										RequestType.merged());
								return Merged.newBuilder()
										.setMerged(merged != null ? unmapValueHeap(merged) : ByteString.EMPTY)
										.build();
							})
							.onErrorMap(ex -> this.handleError(ex).asRuntimeException());
				} else {
					return Flux.error(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				}
			});
		}

		private Mono<Empty> mergeMultiDataFlux(MergeMultiInitialRequest initialRequest,
				Flux<KV> dataFlux, String requestName) {
			var context = mapRequestContext(initialRequest.getContext());
			if (context.profile() == WorkloadProfile.LATENCY) {
				var contextualApi = asyncApi(context);
				return collectLatencyMulti(dataFlux, GrpcServerImpl::encodedInputBytes, requestName)
						.flatMap(data -> {
							var batch = mapKVBatch(data);
							return Mono.fromFuture(() -> contextualApi.mergeMultiAsync(
									initialRequest.getTransactionOrUpdateId(),
									initialRequest.getColumnId(),
									batch.keys(),
									batch.values(),
									RequestType.none()));
						})
						.thenReturn(Empty.getDefaultInstance())
						.transform(this.onErrorMapMonoWithRequestInfo(requestName, initialRequest));
			}
			var contextualApi = syncApi(context);
			return dataFlux
					.publishOn(contextualScheduler(context, OperationFamily.MUTATION))
					.doOnNext(data -> {
						contextualApi.merge(initialRequest.getTransactionOrUpdateId(),
								initialRequest.getColumnId(),
								mapKeys(data.getKeysList()),
								toBuf(data.getValue()),
								RequestType.none());
					})
					.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest))
					.then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Mono<Empty> putMulti(Flux<PutMultiRequest> request) {
			return request.switchOnFirst((firstSignal, requestsFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
					assert firstValue != null;
					if (!firstValue.hasInitialRequest()) {
						return Mono.<Empty>error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var dataFlux = requestsFlux
							.skip(1) // skip the initial request
							.map(putRequest -> {
								if (!putRequest.hasData()) {
									throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
								}
								return putRequest.getData();
							});
					return putMultiDataFlux(initialRequest, dataFlux, "putMulti");
				} else if (firstSignal.isOnComplete()) {
					return Mono.error(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				} else {
					return requestsFlux;
				}
			}).then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Mono<Empty> putMultiEnsure(Flux<PutMultiRequest> request) {
			return request.switchOnFirst((firstSignal, requestsFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
					assert firstValue != null;
					if (!firstValue.hasInitialRequest()) {
						return Mono.<Empty>error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var dataFlux = requestsFlux
							.skip(1)
							.map(putRequest -> {
								if (!putRequest.hasData()) {
									throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
								}
								return putRequest.getData();
							});
					return putMultiEnsureDataFlux(initialRequest, dataFlux, "putMultiEnsure");
				} else if (firstSignal.isOnComplete()) {
					return Mono.error(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				} else {
					return requestsFlux;
				}
			}).then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Flux<Previous> putMultiGetPrevious(Flux<PutMultiRequest> request) {
			return putMultiResponseFlux(request, "putMultiGetPrevious", RequestType.previous(), previous -> {
				var builder = Previous.newBuilder();
				if (previous != null) {
					builder.setPrevious(Utils.toByteString(previous));
				}
				return builder.build();
			});
		}

		@Override
		public Flux<Delta> putMultiGetDelta(Flux<PutMultiRequest> request) {
			return putMultiResponseFlux(request, "putMultiGetDelta", RequestType.delta(), delta -> {
				var builder = Delta.newBuilder();
				if (delta.previous() != null) {
					builder.setPrevious(Utils.toByteString(delta.previous()));
				}
				if (delta.current() != null) {
					builder.setCurrent(Utils.toByteString(delta.current()));
				}
				return builder.build();
			});
		}

		@Override
		public Flux<Changed> putMultiGetChanged(Flux<PutMultiRequest> request) {
			return putMultiResponseFlux(request,
					"putMultiGetChanged",
					RequestType.changed(),
					changed -> Changed.newBuilder().setChanged(changed).build());
		}

		@Override
		public Flux<PreviousPresence> putMultiGetPreviousPresence(Flux<PutMultiRequest> request) {
			return putMultiResponseFlux(request,
					"putMultiGetPreviousPresence",
					RequestType.previousPresence(),
					present -> PreviousPresence.newBuilder().setPresent(present).build());
		}

		private <T, R> Flux<R> putMultiResponseFlux(Flux<PutMultiRequest> request,
				String requestName,
				RequestType.RequestPut<? super Buf, T> requestType,
				Function<T, R> mapper) {
			return request.switchOnFirst((firstSignal, requestsFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
					assert firstValue != null;
					if (!firstValue.hasInitialRequest()) {
						return Flux.error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var context = mapRequestContext(initialRequest.getContext());
					var dataFlux = requestsFlux
							.skip(1)
							.map(putRequest -> {
								if (!putRequest.hasData()) {
									throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
								}
								return putRequest.getData();
							});
					if (context.profile() == WorkloadProfile.LATENCY) {
						var contextualApi = asyncApi(context);
						return collectLatencyMulti(dataFlux, GrpcServerImpl::encodedInputBytes, requestName)
								.flatMapMany(data -> {
									var batch = mapKVBatch(data);
									return Mono.fromFuture(() -> contextualApi.putMultiAsync(
											initialRequest.getTransactionOrUpdateId(),
											initialRequest.getColumnId(),
											batch.keys(),
											batch.values(),
											requestType))
										.flatMapMany(Flux::fromIterable)
										.map(mapper);
								})
								.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest));
					}
					var contextualApi = syncApi(context);
					return dataFlux
							.publishOn(contextualScheduler(context, OperationFamily.MUTATION))
							.map(data -> mapper.apply(contextualApi.put(initialRequest.getTransactionOrUpdateId(),
									initialRequest.getColumnId(),
									mapKeys(data.getKeysList()),
									toBuf(data.getValue()),
									requestType)))
							.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest));
				} else {
					return Flux.error(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				}
			});
		}

		private Mono<Empty> putMultiDataFlux(PutMultiInitialRequest initialRequest,
				Flux<KV> dataFlux, String requestName) {
			var context = mapRequestContext(initialRequest.getContext());
			if (context.profile() == WorkloadProfile.LATENCY) {
				var contextualApi = asyncApi(context);
				return collectLatencyMulti(dataFlux, GrpcServerImpl::encodedInputBytes, requestName)
						.flatMap(data -> {
							var batch = mapKVBatch(data);
							return Mono.fromFuture(() -> contextualApi.putMultiAsync(
									initialRequest.getTransactionOrUpdateId(),
									initialRequest.getColumnId(),
									batch.keys(),
									batch.values(),
									RequestType.none()));
						})
						.thenReturn(Empty.getDefaultInstance())
						.transform(this.onErrorMapMonoWithRequestInfo(requestName, initialRequest));
			}
			var contextualApi = syncApi(context);
			return dataFlux
					.publishOn(contextualScheduler(context, OperationFamily.MUTATION))
					.doOnNext(data -> {
						contextualApi.put(initialRequest.getTransactionOrUpdateId(),
								initialRequest.getColumnId(),
								mapKeys(data.getKeysList()),
								toBuf(data.getValue()),
								RequestType.none());
					})
					.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest))
					.then(Mono.just(Empty.getDefaultInstance()));
		}

		private Mono<Empty> putMultiEnsureDataFlux(PutMultiInitialRequest initialRequest,
				Flux<KV> dataFlux,
				String requestName) {
			var context = mapRequestContext(initialRequest.getContext());
			if (context.profile() == WorkloadProfile.LATENCY) {
				var contextualApi = asyncApi(context);
				return collectLatencyMulti(dataFlux, GrpcServerImpl::encodedInputBytes, requestName)
						.flatMap(data -> {
							var batch = mapKVBatch(data);
							return Mono.fromFuture(() -> contextualApi.putMultiAsync(
									initialRequest.getTransactionOrUpdateId(),
									initialRequest.getColumnId(),
									batch.keys(),
									batch.values(),
									RequestType.ensure()));
						})
						.thenReturn(Empty.getDefaultInstance())
						.transform(this.onErrorMapMonoWithRequestInfo(requestName, initialRequest));
			}
			var contextualApi = syncApi(context);
			return dataFlux
					.buffer(WRITE_ELISION_MULTI_STEP_SIZE)
					.publishOn(contextualScheduler(context, OperationFamily.MUTATION))
					.doOnNext(data -> {
						var batch = mapKVBatch(data);
						contextualApi.putMulti(initialRequest.getTransactionOrUpdateId(),
								initialRequest.getColumnId(),
								batch.keys(),
								batch.values(),
								RequestType.ensure());
					})
					.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest))
					.then(Mono.just(Empty.getDefaultInstance()));
		}

		private <T> Mono<List<T>> collectLatencyMulti(Flux<T> dataFlux,
				ToLongFunction<T> encodedInputBytes,
				String requestName) {
			int maximumItems = embeddedDatabase != null
					? embeddedDatabase.getWorkloadSettings().latencyFanOutMaxItems()
					: WorkloadAdmission.MAX_LATENCY_ITEMS;
			long maximumBytes = embeddedDatabase != null
					? embeddedDatabase.getWorkloadSettings().latencyFanOutMaxBytes()
					: WorkloadAdmission.MAX_LATENCY_ENCODED_INPUT_BYTES;
			return dataFlux.collect(
					() -> new LatencyMultiBuffer<T>(maximumItems, maximumBytes, requestName),
					(buffer, value) -> buffer.add(value, encodedInputBytes.applyAsLong(value)))
					.map(LatencyMultiBuffer::values);
		}

		private static long encodedInputBytes(KV value) {
			long bytes = value.getValue().size();
			for (var key : value.getKeysList()) {
				bytes += key.size();
			}
			return bytes;
		}

		private static long encodedInputBytes(DeleteRequest value) {
			long bytes = 0L;
			for (var key : value.getKeysList()) {
				bytes += key.size();
			}
			return bytes;
		}

		private static final class LatencyMultiBuffer<T> {

			private final int maximumItems;
			private final long maximumBytes;
			private final String requestName;
			private final ArrayList<T> values;
			private long encodedBytes;

			private LatencyMultiBuffer(int maximumItems, long maximumBytes, String requestName) {
				this.maximumItems = maximumItems;
				this.maximumBytes = maximumBytes;
				this.requestName = requestName;
				this.values = new ArrayList<>(maximumItems);
			}

			private void add(T value, long valueBytes) {
				if (values.size() >= maximumItems) {
					throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
							requestName + " item count must not exceed " + maximumItems);
				}
				if (valueBytes > maximumBytes - encodedBytes) {
					throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
							requestName + " encoded input must not exceed " + maximumBytes + " bytes");
				}
				values.add(value);
				encodedBytes += valueBytes;
			}

			private List<T> values() {
				return List.copyOf(values);
			}
		}

		@Override
		public Mono<Previous> putGetPrevious(PutRequest request) {
			return executeWrite(request.getContext(), contextualApi -> {
				var prev = contextualApi.put(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getData().getKeysList()),
						toBuf(request.getData().getValue()),
						RequestType.previous()
				);
				var prevBuilder = Previous.newBuilder();
				if (prev != null) {
					prevBuilder.setPrevious(Utils.toByteString(prev));
				}
				return prevBuilder.build();
			}).transform(this.onErrorMapMonoWithRequestInfo("putGetPrevious", request));
		}

		@Override
		public Mono<Delta> putGetDelta(PutRequest request) {
			return executeWrite(request.getContext(), contextualApi -> {
				var delta = contextualApi.put(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getData().getKeysList()),
						toBuf(request.getData().getValue()),
						RequestType.delta()
				);
				var deltaBuilder = Delta.newBuilder();
				if (delta.previous() != null) {
					deltaBuilder.setPrevious(Utils.toByteString(delta.previous()));
				}
				if (delta.current() != null) {
					deltaBuilder.setCurrent(Utils.toByteString(delta.current()));
				}
				return deltaBuilder.build();
			}).transform(this.onErrorMapMonoWithRequestInfo("putGetDelta", request));
		}

		@Override
		public Mono<Changed> putGetChanged(PutRequest request) {
			return executeWrite(request.getContext(), contextualApi -> {
				var changed = contextualApi.put(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getData().getKeysList()),
						toBuf(request.getData().getValue()),
						RequestType.changed()
				);
				return Changed.newBuilder().setChanged(changed).build();
			}).transform(this.onErrorMapMonoWithRequestInfo("putGetChanged", request));
		}

		@Override
		public Mono<PreviousPresence> putGetPreviousPresence(PutRequest request) {
			return executeWrite(request.getContext(), contextualApi -> {
				var present = contextualApi.put(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getData().getKeysList()),
						toBuf(request.getData().getValue()),
						RequestType.previousPresence()
				);
				return PreviousPresence.newBuilder().setPresent(present).build();
			}).transform(this.onErrorMapMonoWithRequestInfo("putGetPreviousPresence", request));
		}

		@Override
		public Mono<Previous> deleteGetPrevious(DeleteRequest request) {
			return executeWrite(request.getContext(), contextualApi -> {
				var prev = contextualApi.delete(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getKeysList()),
						RequestType.previous()
				);
				var prevBuilder = Previous.newBuilder();
				if (prev != null) {
					prevBuilder.setPrevious(Utils.toByteString(prev));
				}
				return prevBuilder.build();
			}).transform(this.onErrorMapMonoWithRequestInfo("deleteGetPrevious", request));
		}

		@Override
		public Mono<PreviousPresence> deleteGetPreviousPresence(DeleteRequest request) {
			return executeWrite(request.getContext(), contextualApi -> {
				var present = contextualApi.delete(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getKeysList()),
						RequestType.previousPresence()
				);
				return PreviousPresence.newBuilder().setPresent(present).build();
			}).transform(this.onErrorMapMonoWithRequestInfo("deleteGetPreviousPresence", request));
		}

		@Override
		public Mono<GetResponse> get(GetRequest request) {
			return executeSync(request.getContext(), OperationFamily.POINT_LOOKUP, contextualApi -> {
				var current = contextualApi.get(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getKeysList()),
						RequestType.current()
				);
				var responseBuilder = GetResponse.newBuilder();
				if (current != null) {
					responseBuilder.setValue(Utils.toByteString(current));
				}
				return responseBuilder.build();
			}).transform(this.onErrorMapMonoWithRequestInfo("get", request));
		}

		private FastGetResponse createFastGetResponse(GetRequest request,
				it.cavallium.rockserver.core.common.RequestContext context,
				Keys keys,
				GrpcGetStrategy strategy,
				@Nullable EmbeddedDB embeddedDatabase) {
			if (request.getTransactionOrUpdateId() == 0 && embeddedDatabase != null) {
				EmbeddedDB.FastGetOutput output = switch (strategy) {
					case EXACT_HEAP -> EmbeddedDB.FastGetOutput.EXACT_HEAP;
					case PINNED -> EmbeddedDB.FastGetOutput.PINNED;
					case AUTOMATIC -> EmbeddedDB.FastGetOutput.AUTOMATIC;
					case LEGACY -> throw new IllegalStateException("legacy Get cannot use the custom binding");
				};
				EmbeddedDB.FastGetResult result = embeddedDatabase.tryGetFast(
						request.getColumnId(), keys, output);
				if (result != null) {
					try {
						return new FastGetResponse(result.isPresent(),
								result.value(),
								result.isPinned(),
								result);
					} catch (Throwable error) {
						result.close();
						throw error;
					}
				}
			}

			Buf current = syncApi(context).get(request.getTransactionOrUpdateId(),
					request.getColumnId(),
					keys,
					RequestType.current());
			return current != null
					? new FastGetResponse(true, current, false, null)
					: new FastGetResponse(false, null, false, null);
		}

		@Override
		public Mono<UpdateBegin> getForUpdate(GetRequest request) {
			return executeSync(request.getContext(), OperationFamily.MUTATION, contextualApi -> {
				var forUpdate = contextualApi.get(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getKeysList()),
						RequestType.forUpdate()
				);
				var responseBuilder = UpdateBegin.newBuilder();
				responseBuilder.setUpdateId(forUpdate.updateId());
				if (forUpdate.previous() != null) {
					responseBuilder.setPrevious(Utils.toByteString(forUpdate.previous()));
				}
				return responseBuilder.build();
			}).transform(this.onErrorMapMonoWithRequestInfo("getForUpdate", request));
		}

		@Override
		public Mono<PreviousPresence> exists(GetRequest request) {
			return executeSync(request.getContext(), OperationFamily.POINT_LOOKUP, contextualApi -> {
				var exists = contextualApi.get(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getKeysList()),
						RequestType.exists()
				);
				return PreviousPresence.newBuilder().setPresent(exists).build();
			}).transform(this.onErrorMapMonoWithRequestInfo("exists", request));
		}

		@Override
		public Mono<OpenIteratorResponse> openIterator(OpenIteratorRequest request) {
			return executeSync(request.getContext(), OperationFamily.BOUNDARY_SEEK, contextualApi -> {
				var iteratorId = contextualApi.openIterator(request.getTransactionId(),
						request.getColumnId(),
						mapKeys(request.getStartKeysInclusiveList()),
						mapKeys(request.getEndKeysExclusiveList()),
						request.getReverse(),
						java.time.Duration.ofNanos(request.getIteratorLeaseTtlNanos())
				);
				return OpenIteratorResponse.newBuilder().setIteratorId(iteratorId).build();
			}, response -> {
				long iteratorId = response.getIteratorId();
				protectedApi().closeIterator(iteratorId);
			}, scheduler.scheduler(WorkloadProfile.CONTROL, OperationFamily.CONTROL, Long.MAX_VALUE))
					.transform(this.onErrorMapMonoWithRequestInfo("openIterator", request));
		}

		@Override
		public Mono<Empty> closeIterator(CloseIteratorRequest request) {
			requireV3(request.getWorkloadContractVersion());
			return executeMustComplete("closeIterator", () -> {
					protectedApi().closeIterator(request.getIteratorId());
					return Empty.getDefaultInstance();
				}, scheduler.scheduler(WorkloadProfile.CONTROL, OperationFamily.CONTROL, Long.MAX_VALUE))
					.transform(this.onErrorMapMonoWithRequestInfo("closeIterator", request));
		}

		@Override
		public Mono<Empty> seekTo(SeekToRequest request) {
			return withIteratorLease(request.getIterationId(), () -> executeCompositeRead(
					request.getContext(), OperationFamily.BOUNDARY_SEEK, contextualApi -> {
				contextualApi.seekTo(request.getIterationId(), mapKeys(request.getKeysList()));
				return Empty.getDefaultInstance();
			})).transform(this.onErrorMapMonoWithRequestInfo("seekTo", request));
		}

		@Override
		public Mono<Empty> subsequent(SubsequentRequest request) {
			return validateIteratorCounts(request)
					.then(validateSubsequentCommand(request, RequestType.exists()))
					.then(withIteratorLease(request.getIterationId(), () -> {
						if (requiresCooperativeIteratorContinuation(request)) {
							return fromCancellableIteratorFuture(() -> asyncApi(request.getContext()).subsequentAsync(
									request.getIterationId(),
									request.getSkipCount(),
									request.getTakeCount(),
									RequestType.none()))
									.thenReturn(Empty.getDefaultInstance());
						}
						return advanceIterator(request.getContext(), request.getIterationId(),
								request.getSkipCount(), request.getTakeCount())
								.thenReturn(Empty.getDefaultInstance());
					}))
					.transform(this.onErrorMapMonoWithRequestInfo("subsequent", request));
		}

		@Override
		public Mono<PreviousPresence> subsequentExists(SubsequentRequest request) {
			return validateIteratorCounts(request)
					.then(validateSubsequentCommand(request, RequestType.exists()))
					.then(withIteratorLease(request.getIterationId(), () -> {
						if (requiresCooperativeIteratorContinuation(request)) {
							return fromCancellableIteratorFuture(() -> asyncApi(request.getContext()).subsequentAsync(
									request.getIterationId(),
									request.getSkipCount(),
									request.getTakeCount(),
									RequestType.exists()))
									.map(found -> PreviousPresence.newBuilder().setPresent(found).build());
						}
						return advanceIterator(request.getContext(), request.getIterationId(), request.getSkipCount(), 0)
								.thenMany(iteratorChunks(request.getTakeCount(), ITERATOR_ADVANCE_STEP_SIZE))
								.concatMap(take -> executeCompositeRead(request.getContext(), OperationFamily.RANGE_PAGE,
										contextualApi -> contextualApi.subsequent(
												request.getIterationId(), 0, take, RequestType.exists())), 1)
								.takeUntil(found -> !found)
								.reduce(false, (found, pageFound) -> found || pageFound)
								.map(found -> PreviousPresence.newBuilder().setPresent(found).build());
					}))
					.transform(this.onErrorMapMonoWithRequestInfo("subsequentExists", request));
		}

		@Override
		public Flux<KV> subsequentMultiGet(SubsequentRequest request) {
			return validateIteratorCounts(request)
					.then(validateSubsequentCommand(request, RequestType.multi()))
					.thenMany(withIteratorFluxLease(request.getIterationId(), () -> {
						if (requiresCooperativeIteratorContinuation(request)) {
							return cooperativeIteratorMulti(request);
						}
						return advanceIterator(request.getContext(), request.getIterationId(), request.getSkipCount(), 0)
								.thenMany(iteratorChunks(request.getTakeCount(), ITERATOR_VALUE_PAGE_SIZE)
										.concatMap(take -> executeCompositeRead(request.getContext(), OperationFamily.RANGE_PAGE,
												contextualApi -> contextualApi.subsequent(
												request.getIterationId(), 0, take, RequestType.multi())), 1)
										.takeUntil(values -> values.size() < ITERATOR_VALUE_PAGE_SIZE)
										.concatMapIterable(Function.identity(), 1)
										.map(entry -> KV.newBuilder()
												.setValue(Utils.toByteString(entry))
												.build()));
					}))
					.transform(this.onErrorMapFluxWithRequestInfo("subsequentMultiGet", request));
		}

		@Override
		public Mono<FirstAndLast> reduceRangeFirstAndLast(GetRangeRequest request) {
			return Mono.defer(() -> fromCancellableFuture(asyncApi(request.getContext()).reduceRangeAsync(
						request.getTransactionId(),
						request.getColumnId(),
						mapKeys(request.getStartKeysInclusiveList()),
						mapKeys(request.getEndKeysExclusiveList()),
						request.getReverse(),
						RequestType.firstAndLast())))
					.map(range -> {
						if (range.first() == null || range.last() == null) {
							return FirstAndLast.getDefaultInstance();
						}
						return FirstAndLast.newBuilder()
								.setFirst(unmapKVHeap(range.first()))
								.setLast(unmapKVHeap(range.last()))
								.build();
					})
					.transform(this.onErrorMapMonoWithRequestInfo("reduceRangeFirstAndLast", request));
		}

		@Override
		public Mono<EntriesCount> reduceRangeEntriesCount(GetRangeRequest request) {
			return Mono.defer(() -> fromCancellableFuture(asyncApi(request.getContext()).reduceRangeAsync(
						request.getTransactionId(),
						request.getColumnId(),
						mapKeys(request.getStartKeysInclusiveList()),
						mapKeys(request.getEndKeysExclusiveList()),
						request.getReverse(),
						RequestType.entriesCount())))
					.map(count -> EntriesCount.newBuilder().setCount(count).build())
					.transform(this.onErrorMapMonoWithRequestInfo("reduceRangeEntriesCount", request));
		}

		@Override
		public Flux<KV> getAllInRange(GetRangeRequest request) {
			return getAllInRange(request, RequestType.allInRange(), "getAllInRange");
		}

		@Override
		public Flux<KV> getAllInRangeNoCache(GetRangeRequest request) {
			return getAllInRange(request, RequestType.allInRangeNoCache(), "getAllInRangeNoCache");
		}

		@Override
		public Mono<it.cavallium.rockserver.core.common.api.proto.RangePage> getRangePage(
				GetRangePageRequest request) {
			return Mono.defer(() -> {
				if (!request.hasBudget()) {
					return Mono.error(RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
							"Range budget is required"));
				}
				final it.cavallium.rockserver.core.common.RangeBudget budget;
				try {
					budget = new it.cavallium.rockserver.core.common.RangeBudget(
							request.getBudget().getMaxItems(), request.getBudget().getMaxBytes());
				} catch (IllegalArgumentException invalid) {
					return Mono.error(RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
							invalid.getMessage(), invalid));
				}
				var requestType = switch (request.getRequestTypeValue()) {
					case 1 -> RequestType.<it.cavallium.rockserver.core.common.KV>allInRange();
					case 2 -> RequestType.<it.cavallium.rockserver.core.common.KV>allInRangeNoCache();
					default -> throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
							"Unknown range request type: " + request.getRequestTypeValue());
				};
				var resumeAfter = request.hasResumeAfter()
						? mapKeys(request.getResumeAfter().getKeysList())
						: null;
				return fromCancellableFuture(asyncApi(request.getContext()).getRangePageAsync(
						request.getTransactionId(),
						request.getColumnId(),
						mapKeys(request.getStartKeysInclusiveList()),
						mapKeys(request.getEndKeysExclusiveList()),
						request.getReverse(),
						resumeAfter,
						requestType,
						budget));
			}).map(page -> {
				var response = it.cavallium.rockserver.core.common.api.proto.RangePage.newBuilder()
						.setHasMore(page.hasMore());
				for (var item : page.items()) {
					response.addItems(unmapKVHeap(item));
				}
				if (page.resumeAfter() != null) {
					var resumeAfter = RangeKey.newBuilder();
					for (var key : page.resumeAfter().keys()) {
						resumeAfter.addKeys(Utils.toByteString(key));
					}
					response.setResumeAfter(resumeAfter);
				}
				return response.build();
			}).transform(this.onErrorMapMonoWithRequestInfo("getRangePage", request));
		}

		private Flux<KV> getAllInRange(GetRangeRequest request,
				RequestType.RequestGetRange<? super it.cavallium.rockserver.core.common.KV,
						it.cavallium.rockserver.core.common.KV> requestType,
				String requestName) {
			return Flux.defer(() -> Flux
					.from(asyncApi(request.getContext()).getRangeAsync(request.getTransactionId(),
							request.getColumnId(),
							mapKeys(request.getStartKeysInclusiveList()),
							mapKeys(request.getEndKeysExclusiveList()),
							request.getReverse(),
							requestType)))
					.map(GrpcServerImpl::unmapKVHeap)
					.transform(this.onErrorMapFluxWithRequestInfo(requestName, request));
		}

		@Override
		public Flux<it.cavallium.rockserver.core.common.api.proto.ScanRawResponse> scanRaw(ScanRawRequest request) {
			return Flux.defer(() -> {
				var api = asyncApi(request.getContext());
				if (request.getResumable()) {
					final Set<RawSstToken> completedSsts;
					try {
						completedSsts = request.getCompletedSstTokensList().stream()
								.map(RawSstToken::new)
								.collect(Collectors.toUnmodifiableSet());
					} catch (IllegalArgumentException invalidToken) {
						return Flux.error(Status.INVALID_ARGUMENT
								.withDescription(invalidToken.getMessage())
								.withCause(invalidToken)
								.asRuntimeException());
					}
					var events = Flux.from(api.scanRawResumableAsync(request.getColumnId(),
							request.getShardIndex(), request.getShardCount(), completedSsts));
					return events.map(event -> switch (event) {
						case RawScanEvent.Batch batch -> rawScanBatchResponse(
								batch.serialized(), batch.completedSstToken());
						case RawScanEvent.SstCompleted completed -> rawScanCompletionResponse(completed.token());
					});
				}
				if (request.getCompletedSstTokensCount() != 0) {
					return Flux.error(Status.INVALID_ARGUMENT
							.withDescription("completed SST tokens require resumable raw scan mode")
							.asRuntimeException());
				}
				// Keep the plain path allocation-neutral: only resumable scans
				// need RawScanEvent wrappers.
				return Flux.from(api.scanRawAsync(
						request.getColumnId(), request.getShardIndex(), request.getShardCount()))
						.map(batch -> rawScanBatchResponse(batch.serialized()));
			})
					.limitRate(17, 1)
					.transform(this.onErrorMapFluxWithRequestInfo("scanRaw", request));
		}

		private static it.cavallium.rockserver.core.common.api.proto.ScanRawResponse rawScanBatchResponse(
				Buf serialized) {
			return rawScanBatchResponse(serialized, null);
		}

		private static it.cavallium.rockserver.core.common.api.proto.ScanRawResponse rawScanBatchResponse(
				Buf serialized,
				@Nullable RawSstToken completedSstToken) {
			var serializedBatchValue = UnsafeByteOperations.unsafeWrap(
					serialized.getBackingByteArray(),
					serialized.getBackingByteArrayOffset(),
					serialized.getBackingByteArrayLength());
			var response = it.cavallium.rockserver.core.common.api.proto.ScanRawResponse.newBuilder()
					.setSerialized(serializedBatchValue);
			if (completedSstToken != null) {
				response.setCompletedSstTokenAfterBatch(completedSstToken.value());
			}
			return response.build();
		}

		private static it.cavallium.rockserver.core.common.api.proto.ScanRawResponse rawScanCompletionResponse(
				RawSstToken completedSstToken) {
			return it.cavallium.rockserver.core.common.api.proto.ScanRawResponse.newBuilder()
					.setCompletedSstToken(completedSstToken.value())
					.build();
		}

		@Override
		public Mono<Empty> flush(FlushRequest request) {
			requireV3(request.getWorkloadContractVersion());
			return executeScheduled(() -> {
				protectedApi().flush();
				return Empty.getDefaultInstance();
			}, scheduler.scheduler(WorkloadProfile.PHYSICAL_MAINTENANCE, OperationFamily.FLUSH, Long.MAX_VALUE))
					.transform(this.onErrorMapMonoWithRequestInfo("flush", request));
		}

        @Override
        public Mono<GetSstMetadataResponse> getSstMetadata(GetSstMetadataRequest request) {
            return executeSync(request.getContext(), OperationFamily.METADATA, api ->
                    SstMaintenanceProto.encode(api.getSstMetadata(request.getColumnId(), request.getLevel())))
                    .transform(this.onErrorMapMonoWithRequestInfo("getSstMetadata", request));
        }

        @Override
        public Mono<CompactFilesResponse> compactFiles(CompactFilesRequest request) {
            requireV3(request.getWorkloadContractVersion());
            return Mono.defer(() -> {
                var decoded = SstMaintenanceProto.decode(request);
                return executeScheduled(() -> SstMaintenanceProto.encode(protectedApi().compactFiles(decoded)),
                        scheduler.scheduler(WorkloadProfile.PHYSICAL_MAINTENANCE, OperationFamily.COMPACTION, Long.MAX_VALUE));
            }).transform(this.onErrorMapMonoWithRequestInfo("compactFiles", request));
        }

		@Override
		public Mono<Empty> compact(CompactRequest request) {
			requireV3(request.getWorkloadContractVersion());
			return executeScheduled(() -> {
				protectedApi().compact();
				return Empty.getDefaultInstance();
			}, scheduler.scheduler(WorkloadProfile.PHYSICAL_MAINTENANCE, OperationFamily.COMPACTION, Long.MAX_VALUE))
					.transform(this.onErrorMapMonoWithRequestInfo("compact", request));
		}

		@Override
		public Mono<GetAllColumnDefinitionsResponse> getAllColumnDefinitions(GetAllColumnDefinitionsRequest request) {
			return executeSync(request.getContext(), OperationFamily.METADATA, contextualApi -> {
				var definitions = contextualApi.getAllColumnDefinitions();
				var builder = GetAllColumnDefinitionsResponse.newBuilder();
				for (Entry<String, ColumnSchema> e : definitions.entrySet()) {
					builder.addColumns(Column.newBuilder().setName(e.getKey()).setSchema(unmapColumnSchema(e.getValue())));
				}
 			return builder.build();
			}).transform(this.onErrorMapMonoWithRequestInfo("getAllColumnDefinitions", request));
		}

		@Override
		public Mono<UploadMergeOperatorResponse> uploadMergeOperator(UploadMergeOperatorRequest request) {
			return executeSync(request.getContext(), OperationFamily.MUTATION, contextualApi -> {
				var version = contextualApi.uploadMergeOperator(request.getOperatorName(), request.getClassName(), request.getJarPayload().toByteArray());
				return UploadMergeOperatorResponse.newBuilder().setVersion(version).build();
			}).transform(this.onErrorMapMonoWithRequestInfo("uploadMergeOperator", request));
		}

		@Override
		public Mono<CheckMergeOperatorResponse> checkMergeOperator(CheckMergeOperatorRequest request) {
			return executeSync(request.getContext(), OperationFamily.METADATA, contextualApi -> {
				var version = contextualApi.checkMergeOperator(request.getOperatorName(), request.getHash().toByteArray());
				var builder = CheckMergeOperatorResponse.newBuilder();
				if (version != null) {
					builder.setVersion(version);
				}
				return builder.build();
			}).transform(this.onErrorMapMonoWithRequestInfo("checkMergeOperator", request));
		}

            // ============ CDC RPCs ============

			@Override
			public Mono<CdcCreateResponse> cdcCreate(CdcCreateRequest request) {
				requireV3(request.getWorkloadContractVersion());
				Long fromSeq = request.hasFromSeq() ? request.getFromSeq() : null;
				OptionalLong expectedLastCommitted = switch (request.getExpectedLastCommittedCase()) {
					case EXPECTABSENT -> OptionalLong.empty();
					case EXPECTEDLASTCOMMITTEDSEQ -> OptionalLong.of(request.getExpectedLastCommittedSeq());
					case EXPECTEDLASTCOMMITTED_NOT_SET -> throw RocksDBException.of(
							RocksDBErrorType.PUT_INVALID_REQUEST,
							"CDC creation precondition is required");
				};
				return executeScheduled(() -> {
					var cols = request.getColumnIdsCount() > 0 ? request.getColumnIdsList().stream().map(Long::valueOf).toList() : null;
					Boolean resolvedValues = request.hasResolvedValues() ? request.getResolvedValues() : null;
					long startSeq = protectedApi().cdcCreate(
							request.getId(), fromSeq, cols, resolvedValues, expectedLastCommitted);
					return CdcCreateResponse.newBuilder().setStartSeq(startSeq).build();
				}, scheduler.scheduler(WorkloadProfile.CDC, OperationFamily.MUTATION,
						Long.MAX_VALUE))
						.transform(this.onErrorMapMonoWithRequestInfo("cdcCreate", request));
			}

            @Override
			public Mono<Empty> cdcDelete(CdcDeleteRequest request) {
				requireV3(request.getWorkloadContractVersion());
				return executeScheduled(() -> {
					protectedApi().cdcDelete(request.getId());
					return Empty.getDefaultInstance();
				}, scheduler.scheduler(WorkloadProfile.CDC, OperationFamily.MUTATION,
						Long.MAX_VALUE))
						.transform(this.onErrorMapMonoWithRequestInfo("cdcDelete", request));
            }

			@Override
			public Mono<CdcGetEarliestAvailableSequenceResponse> cdcGetEarliestAvailableSequence(
					CdcGetEarliestAvailableSequenceRequest request) {
				requireV3(request.getWorkloadContractVersion());
				return executeScheduled(() -> CdcGetEarliestAvailableSequenceResponse.newBuilder()
						.setSequence(protectedApi().cdcGetEarliestAvailableSequence())
						.build(), scheduler.scheduler(WorkloadProfile.CDC, OperationFamily.WAL_PAGE, Long.MAX_VALUE))
						.transform(this.onErrorMapMonoWithRequestInfo(
								"cdcGetEarliestAvailableSequence", request));
			}

            @Override
			public Mono<CdcGetLastCommittedSequenceResponse> cdcGetLastCommittedSequence(
					CdcGetLastCommittedSequenceRequest request) {
				requireV3(request.getWorkloadContractVersion());
				return executeScheduled(() -> {
					var sequence = protectedApi().cdcGetLastCommittedSequence(request.getId());
                    var response = CdcGetLastCommittedSequenceResponse.newBuilder();
                    sequence.ifPresent(response::setLastCommittedSeq);
                    return response.build();
				}, scheduler.scheduler(WorkloadProfile.CDC, OperationFamily.WAL_PAGE, Long.MAX_VALUE))
						.transform(this.onErrorMapMonoWithRequestInfo(
                        "cdcGetLastCommittedSequence", request));
            }

            @Override
			public Flux<it.cavallium.rockserver.core.common.api.proto.CDCEvent> cdcPoll(CdcPollRequest request) {
				requireV3(request.getWorkloadContractVersion());
                return Flux.defer(() -> {
                            long maxEvents = request.getMaxEvents() > 0 ? request.getMaxEvents() : 10_000L;
                            Long fromSeq = request.hasFromSeq() ? request.getFromSeq() : null;
                            int maxResponseBytes = requestedMaxResponseBytes(request);
							return Flux.from(protectedAsyncApi().cdcPollAsync(request.getId(), fromSeq, maxEvents))
                                    .map(event -> CdcResponseBudget.buildEvent(event, maxResponseBytes));
                        })
                        .transform(this.onErrorMapFluxWithRequestInfo("cdcPoll", request));
            }

            @Override
			public Mono<CdcPollResponse> cdcPollBatch(CdcPollRequest request) {
				requireV3(request.getWorkloadContractVersion());
                return Mono.defer(() -> {
                            long maxEvents = request.getMaxEvents() > 0 ? request.getMaxEvents() : 10_000L;
                            Long fromSeq = request.hasFromSeq() ? request.getFromSeq() : null;
                            int maxResponseBytes = requestedMaxResponseBytes(request);
							return protectedAsyncApi().cdcPollBatchAsync(request.getId(), fromSeq, maxEvents)
                                    .map(batch -> CdcResponseBudget.build(batch, maxResponseBytes));
                        })
                        .transform(this.onErrorMapMonoWithRequestInfo("cdcPollBatch", request));
            }

            private static int requestedMaxResponseBytes(CdcPollRequest request) {
                int requestedMaxResponseBytes = request.getMaxResponseBytes();
				if (requestedMaxResponseBytes <= 0) {
					throw Status.INVALID_ARGUMENT
							.withDescription("maxResponseBytes must be positive: "
									+ requestedMaxResponseBytes)
							.asRuntimeException();
				}
				return requestedMaxResponseBytes;
            }

			@Override
			public Mono<Empty> cdcCommit(CdcCommitRequest request) {
				requireV3(request.getWorkloadContractVersion());
				return executeMustComplete("cdcCommit", () -> {
					protectedApi().cdcCommit(request.getId(), request.getSeq());
					return Empty.getDefaultInstance();
				}, scheduler.scheduler(WorkloadProfile.CDC, OperationFamily.MUTATION,
						Long.MAX_VALUE))
						.transform(this.onErrorMapMonoWithRequestInfo("cdcCommit", request));
            }

		// utils

		private static void requireV3(int workloadContractVersion) {
			if (workloadContractVersion != RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Request requires workload contract version "
								+ RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION);
			}
		}

		private <T> Mono<T> executeSync(
				it.cavallium.rockserver.core.common.api.proto.RequestContext wireContext,
				OperationFamily family,
				Function<RocksDBSyncAPI, T> operation) {
			return Mono.defer(() -> {
				var context = mapRequestContext(wireContext);
				var command = captureCommand(operation);
					var profile = preAdmit(context, family, command);
					return executeScheduled(() -> operation.apply(syncApi(context)),
							scheduler.scheduler(profile,
									command.operationFamily(),
									context.localMonotonicDeadlineNanos()),
						command.estimatedBytes());
			});
		}

		private <T> Mono<T> executeSync(
				it.cavallium.rockserver.core.common.api.proto.RequestContext wireContext,
				OperationFamily family,
				Function<RocksDBSyncAPI, T> operation,
				Consumer<T> lateSuccessCleanup,
				reactor.core.scheduler.Scheduler lateSuccessCleanupScheduler) {
			return Mono.defer(() -> {
				var context = mapRequestContext(wireContext);
				var command = captureCommand(operation);
					var profile = preAdmit(context, family, command);
					return executeScheduled(() -> operation.apply(syncApi(context)),
							scheduler.scheduler(profile,
									command.operationFamily(),
									context.localMonotonicDeadlineNanos()),
						lateSuccessCleanup,
						lateSuccessCleanupScheduler,
						command.estimatedBytes());
			});
		}

		private <T> Mono<T> executeWrite(
				it.cavallium.rockserver.core.common.api.proto.RequestContext wireContext,
				Function<RocksDBSyncAPI, T> operation) {
			return executeSync(wireContext, OperationFamily.MUTATION, operation);
		}

		private <T> Mono<T> executeCompositeRead(
				it.cavallium.rockserver.core.common.api.proto.RequestContext wireContext,
				OperationFamily family,
				Function<RocksDBSyncAPI, T> operation) {
			return executeSync(wireContext, family, operation);
		}

		private Mono<Void> validateIteratorCounts(SubsequentRequest request) {
			if (request.getSkipCount() < 0 || request.getTakeCount() < 0) {
				return Mono.error(RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Iterator skip and take counts must be non-negative"));
			}
			return Mono.empty();
		}

		private Mono<Void> validateSubsequentCommand(SubsequentRequest request,
				RequestType.RequestIterate<? super Buf, ?> requestType) {
			return Mono.fromRunnable(() -> {
				var context = mapRequestContext(request.getContext());
				var command = new RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent<>(
						request.getIterationId(),
						request.getSkipCount(),
						request.getTakeCount(),
						requestType);
				preAdmit(context, OperationFamily.RANGE_PAGE, command);
			});
		}

		private boolean requiresCooperativeIteratorContinuation(SubsequentRequest request) {
			long skipCount = request.getSkipCount();
			long takeCount = request.getTakeCount();
			return mapRequestContext(request.getContext()).profile() != WorkloadProfile.LATENCY
					&& (skipCount > 1L
							|| takeCount > 1L
							|| (skipCount != 0L && takeCount != 0L));
		}

		private IteratorQuantumLimits iteratorQuantumLimits() {
			var database = embeddedDatabase;
			if (database == null) {
				return new IteratorQuantumLimits(
						ITERATOR_ADVANCE_STEP_SIZE,
						DEFAULT_ITERATOR_QUANTUM_BYTES,
						DEFAULT_ITERATOR_QUANTUM_NANOS);
			}
			var settings = database.getWorkloadSettings();
			return new IteratorQuantumLimits(
					settings.rangeQuantumMaxItems(),
					settings.rangeQuantumMaxBytes(),
					settings.rangeQuantumMaxDuration().toNanos());
		}

		/**
		 * Preserve one logical scheduler node for a long MULTI continuation while keeping
		 * gRPC delivery incremental. Native skip calls remain bounded to 4,096 entries and
		 * value calls to 64 entries; only heap-backed values survive a cooperative park.
		 */
		private Flux<KV> cooperativeIteratorMulti(SubsequentRequest request) {
			return Flux.deferContextual(contextView -> {
				var iteratorLease = contextView.<IteratorOperationLease>getOrDefault(
						ITERATOR_OPERATION_LEASE_CONTEXT_KEY, null);
				if (iteratorLease == null) {
					return Flux.error(new IllegalStateException(
							"Cooperative iterator stream was started without an operation lease"));
				}

				final ResolvedRequestContext requestContext;
				final RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent<List<Buf>> command;
				final WorkloadProfile profile;
				final RocksDBSyncAPI contextualApi;
				final RWScheduler.WorkloadExecutor workloadExecutor;
				final IteratorQuantumLimits quantumLimits;
				try {
					requestContext = mapRequestContext(request.getContext());
					command = new RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent<>(
							request.getIterationId(),
							request.getSkipCount(),
							request.getTakeCount(),
							RequestType.multi());
					profile = preAdmit(requestContext, OperationFamily.RANGE_PAGE, command);
					contextualApi = syncApi(requestContext);
					workloadExecutor = scheduler.executor(
							profile,
							command.operationFamily(),
							requestContext.localMonotonicDeadlineNanos());
					quantumLimits = iteratorQuantumLimits();
				} catch (Throwable failure) {
					iteratorLease.operationTerminated();
					return Flux.error(failure);
				}

				return Flux.<KV>create(sink -> {
					if (!iteratorLease.registerTask()) {
						iteratorLease.operationTerminated();
						sink.error(new java.util.concurrent.CancellationException(
								"Iterator operation terminated before its stream could start"));
						return;
					}
					final CooperativeIteratorMultiStream state;
					try {
						state = new CooperativeIteratorMultiStream(
								request.getIterationId(),
								request.getSkipCount(),
								request.getTakeCount(),
								contextualApi,
								quantumLimits,
								iteratorLease,
								sink,
								lateErrorHandler(contextView));
						sink.onRequest(state::request);
						sink.onCancel(state::cancel);
					} catch (Throwable initializationFailure) {
						iteratorLease.taskAndOperationTerminated();
						sink.error(initializationFailure);
						return;
					}
					try {
						state.attach(workloadExecutor.executeCooperatively(
								state,
								state.estimatedBytes()));
						state.start();
					} catch (Throwable admissionFailure) {
						state.admissionFailed(admissionFailure);
					}
				}, FluxSink.OverflowStrategy.ERROR);
			});
		}

		private Flux<Long> iteratorChunks(long count, long stepSize) {
			return Flux.generate(() -> count, (remaining, sink) -> {
				if (remaining <= 0) {
					sink.complete();
					return 0L;
				}
				long chunk = Math.min(remaining, stepSize);
				sink.next(chunk);
				return remaining - chunk;
			});
		}

		private Mono<Void> advanceIterator(
				it.cavallium.rockserver.core.common.api.proto.RequestContext context,
				long iteratorId,
				long skipCount,
				long takeCount) {
			return advanceIteratorPart(context, iteratorId, skipCount)
					.then(advanceIteratorPart(context, iteratorId, takeCount));
		}

		private Mono<Void> advanceIteratorPart(
				it.cavallium.rockserver.core.common.api.proto.RequestContext context,
				long iteratorId,
				long count) {
			return iteratorChunks(count, ITERATOR_ADVANCE_STEP_SIZE)
					.concatMap(step -> executeCompositeRead(context, OperationFamily.RANGE_PAGE,
							contextualApi -> contextualApi.subsequent(
									iteratorId, 0, step, RequestType.exists())), 1)
					.takeUntil(found -> !found)
					.then();
		}

		private <T> Mono<T> withIteratorLease(long iteratorId, Supplier<Mono<T>> operation) {
			return Mono.defer(() -> {
				var lease = new IteratorOperationLease(iteratorId);
				if (iteratorOperations.putIfAbsent(iteratorId, lease) != null) {
					return Mono.error(concurrentIteratorOperation(iteratorId));
				}
				try {
					return operation.get()
							.doOnTerminate(lease::operationTerminated)
							.doFinally(_ -> lease.operationTerminated())
							.contextWrite(context -> context.put(ITERATOR_OPERATION_LEASE_CONTEXT_KEY, lease));
				} catch (Throwable error) {
					lease.operationTerminated();
					return Mono.error(error);
				}
			});
		}

		private <T> Flux<T> withIteratorFluxLease(long iteratorId, Supplier<Flux<T>> operation) {
			return Flux.defer(() -> {
				var lease = new IteratorOperationLease(iteratorId);
				if (iteratorOperations.putIfAbsent(iteratorId, lease) != null) {
					return Flux.error(concurrentIteratorOperation(iteratorId));
				}
				try {
					return operation.get()
							.doOnTerminate(lease::operationTerminated)
							.doFinally(_ -> lease.operationTerminated())
							.contextWrite(context -> context.put(ITERATOR_OPERATION_LEASE_CONTEXT_KEY, lease));
				} catch (Throwable error) {
					lease.operationTerminated();
					return Flux.error(error);
				}
			});
		}

		/**
		 * A subscriber owns the iterator lease until it terminates and every native task that
		 * it started has actually returned. Cancelling an RPC can interrupt a RocksDB call,
		 * but JNI is allowed to keep running until its own deadline; releasing the lease from
		 * {@code doFinally(CANCEL)} alone would then admit a concurrent operation on the same
		 * native iterator.
		 */
		private final class IteratorOperationLease {

			private static final int OPERATION_TERMINATED = 1 << 31;
			private static final int ACTIVE_TASKS_MASK = Integer.MAX_VALUE;
			private static final VarHandle STATE;

			static {
				try {
					STATE = MethodHandles.lookup().findVarHandle(IteratorOperationLease.class, "state", int.class);
				} catch (NoSuchFieldException | IllegalAccessException failure) {
					throw new ExceptionInInitializerError(failure);
				}
			}

			private final long iteratorId;
			private volatile int state;

			private IteratorOperationLease(long iteratorId) {
				this.iteratorId = iteratorId;
			}

			private boolean registerTask() {
				while (true) {
					int current = state;
					if ((current & OPERATION_TERMINATED) != 0) {
						return false;
					}
					if ((current & ACTIVE_TASKS_MASK) == ACTIVE_TASKS_MASK) {
						throw new IllegalStateException("Too many active iterator tasks");
					}
					if (STATE.compareAndSet(this, current, current + 1)) {
						return true;
					}
				}
			}

			private void taskTerminated() {
				while (true) {
					int current = state;
					int activeTasks = current & ACTIVE_TASKS_MASK;
					if (activeTasks == 0) {
						throw new IllegalStateException("Iterator task accounting underflow");
					}
					int updated = (current & OPERATION_TERMINATED) | (activeTasks - 1);
					if (STATE.compareAndSet(this, current, updated)) {
						if (updated == OPERATION_TERMINATED) {
							iteratorOperations.remove(iteratorId, this);
						}
						return;
					}
				}
			}

			private void operationTerminated() {
				while (true) {
					int current = state;
					if ((current & OPERATION_TERMINATED) != 0) {
						return;
					}
					int updated = current | OPERATION_TERMINATED;
					if (STATE.compareAndSet(this, current, updated)) {
						if (updated == OPERATION_TERMINATED) {
							iteratorOperations.remove(iteratorId, this);
						}
						return;
					}
				}
			}

			private void taskAndOperationTerminated() {
				// Both dimensions must be released before a cooperative stream publishes
				// completion or failure; the outer doFinally remains an idempotent cancel guard.
				try {
					taskTerminated();
				} finally {
					operationTerminated();
				}
			}
		}

		/**
		 * Server-side streaming counterpart to the embedded iterator continuation. It
		 * retains only logical counters and one 64-value heap page across redispatches;
		 * every native call returns before this task can yield or park.
		 */
		private final class CooperativeIteratorMultiStream
				implements RWScheduler.CooperativeCompletionTask, Disposable {

			private static final VarHandle DEMAND;
			private static final VarHandle TERMINATED;

			static {
				try {
					var lookup = MethodHandles.lookup();
					DEMAND = lookup.findVarHandle(CooperativeIteratorMultiStream.class, "demand", long.class);
					TERMINATED = lookup.findVarHandle(
							CooperativeIteratorMultiStream.class, "terminated", boolean.class);
				} catch (NoSuchFieldException | IllegalAccessException failure) {
					throw new ExceptionInInitializerError(failure);
				}
			}

			private final long iteratorId;
			private final RocksDBSyncAPI contextualApi;
			private final IteratorQuantumLimits quantumLimits;
			private final IteratorOperationLease iteratorLease;
			private final FluxSink<KV> sink;
			private final Consumer<Throwable> lateErrors;
			private volatile long demand;
			private volatile boolean terminated;
			private volatile boolean ready;
			private volatile boolean cancellationRequested;
			private volatile @Nullable RWScheduler.CooperativeHandle handle;
			private long remainingSkip;
			private long remainingTake;
			private @Nullable List<Buf> page;
			private int pageIndex;
			private boolean sourceExhausted;
			private boolean checkpointAfterPage;
			private boolean completionPrepared;

			private CooperativeIteratorMultiStream(long iteratorId,
					long skipCount,
					long takeCount,
					RocksDBSyncAPI contextualApi,
					IteratorQuantumLimits quantumLimits,
					IteratorOperationLease iteratorLease,
					FluxSink<KV> sink,
					Consumer<Throwable> lateErrors) {
				this.iteratorId = iteratorId;
				this.remainingSkip = skipCount;
				this.remainingTake = takeCount;
				this.contextualApi = contextualApi;
				this.quantumLimits = quantumLimits;
				this.iteratorLease = iteratorLease;
				this.sink = sink;
				this.lateErrors = lateErrors;
			}

			private void attach(RWScheduler.CooperativeHandle handle) {
				boolean cancel;
				boolean resume;
				synchronized (this) {
					this.handle = Objects.requireNonNull(handle, "handle");
					cancel = cancellationRequested || terminated;
					resume = ready && (remainingTake == 0L || demand > 0L);
				}
				if (cancel) {
					handle.cancel();
				} else if (resume) {
					handle.resume();
				}
			}

			private void start() {
				RWScheduler.CooperativeHandle currentHandle;
				boolean cancel;
				boolean resume;
				synchronized (this) {
					if (terminated) {
						return;
					}
					ready = true;
					currentHandle = Objects.requireNonNull(handle, "handle");
					cancel = cancellationRequested;
					resume = remainingTake == 0L || demand > 0L;
				}
				if (cancel) {
					currentHandle.cancel();
				} else if (resume) {
					currentHandle.resume();
				}
			}

			@Override
			public RWScheduler.CooperativeResult runCooperatively(
					RWScheduler.CooperativeContext context) {
				if (terminated) {
					return RWScheduler.CooperativeResult.COMPLETE;
				}
				if (!ready) {
					return RWScheduler.CooperativeResult.PARK;
				}
				if (cancellationRequested || context.terminationRequested()) {
					return RWScheduler.CooperativeResult.COMPLETE;
				}

				try {
					while (true) {
						if (cancellationRequested || context.terminationRequested()) {
							return RWScheduler.CooperativeResult.COMPLETE;
						}
						if (page != null) {
							if (!emitPage()) {
								return RWScheduler.CooperativeResult.PARK;
							}
							boolean checkpointReached = checkpointAfterPage;
							checkpointAfterPage = false;
							if (sourceExhausted || remainingTake == 0L) {
								prepareCompletion();
								return RWScheduler.CooperativeResult.COMPLETE;
							}
							if (demand == 0L) {
								return RWScheduler.CooperativeResult.PARK;
							}
							if (checkpointReached || context.preemptionRequested()) {
								return RWScheduler.CooperativeResult.YIELD;
							}
							continue;
						}

						if (remainingSkip == 0L && remainingTake == 0L) {
							prepareCompletion();
							return RWScheduler.CooperativeResult.COMPLETE;
						}
						if (remainingTake > 0L && demand == 0L) {
							return RWScheduler.CooperativeResult.PARK;
						}

						if (remainingSkip > 0L) {
							long step = Math.min(remainingSkip, quantumLimits.maximumItems());
							boolean exhausted = false;
							boolean checkpointReached = false;
							if (embeddedDatabase != null) {
								var quantum = embeddedDatabase.advanceIteratorQuantumInternal(
										iteratorId,
										step,
										quantumLimits.maximumDurationNanos(),
										context);
								remainingSkip -= quantum.advanced();
								exhausted = quantum.exhausted();
								checkpointReached = quantum.checkpointRequested();
							} else {
								boolean advanced = contextualApi.subsequent(
										iteratorId, 0L, step, RequestType.exists());
								remainingSkip -= step;
								exhausted = !advanced;
							}
							if (exhausted) {
								sourceExhausted = true;
								remainingSkip = 0L;
								remainingTake = 0L;
								prepareCompletion();
								return RWScheduler.CooperativeResult.COMPLETE;
							}
							if (cancellationRequested || context.terminationRequested()) {
								return RWScheduler.CooperativeResult.COMPLETE;
							}
							if (checkpointReached || context.preemptionRequested()) {
								return RWScheduler.CooperativeResult.YIELD;
							}
							continue;
						}

						long step = Math.min(remainingTake,
								Math.min(ITERATOR_VALUE_PAGE_SIZE, quantumLimits.maximumItems()));
						final List<Buf> values;
						if (embeddedDatabase != null) {
							var quantum = embeddedDatabase.readIteratorQuantumInternal(
									iteratorId,
									step,
									quantumLimits.maximumBytes(),
									quantumLimits.maximumDurationNanos(),
									context);
							values = quantum.values();
							sourceExhausted = quantum.exhausted();
							checkpointAfterPage = quantum.checkpointRequested();
						} else {
							values = Objects.requireNonNull(contextualApi.subsequent(
									iteratorId, 0L, step, RequestType.<Buf>multi()),
									"Iterator MULTI page");
							sourceExhausted = values.size() < step;
							checkpointAfterPage = context.preemptionRequested();
						}
						if (values.size() > step) {
							throw new IllegalStateException("Iterator MULTI page exceeded its requested size");
						}
						remainingTake -= values.size();
						if (values.isEmpty()) {
							prepareCompletion();
							return RWScheduler.CooperativeResult.COMPLETE;
						}
						page = values;
						pageIndex = 0;
					}
				} catch (RuntimeException failure) {
					// Keep terminal attribution and first-cause arbitration scheduler-authoritative.
					context.fail(failure);
					return RWScheduler.CooperativeResult.COMPLETE;
				}
			}

			private long estimatedBytes() {
				return quantumLimits.maximumBytes();
			}

			private boolean emitPage() {
				var currentPage = Objects.requireNonNull(page, "page");
				while (pageIndex < currentPage.size()) {
					if (cancellationRequested || demand == 0L) {
						return false;
					}
					var value = currentPage.get(pageIndex++);
					producedOne();
					sink.next(KV.newBuilder()
							.setValue(Utils.toByteString(value))
							.build());
				}
				page = null;
				pageIndex = 0;
				return true;
			}

			private void producedOne() {
				while (true) {
					long current = demand;
					if (current == Long.MAX_VALUE) {
						return;
					}
					if (current <= 0L) {
						throw new IllegalStateException("Iterator value produced without downstream demand");
					}
					if (DEMAND.compareAndSet(this, current, current - 1L)) {
						return;
					}
				}
			}

			private void request(long requested) {
				if (requested <= 0L || terminated) {
					return;
				}
				while (true) {
					long current = demand;
					long updated = current + requested;
					if (updated < 0L) {
						updated = Long.MAX_VALUE;
					}
					if (DEMAND.compareAndSet(this, current, updated)) {
						break;
					}
				}
				var currentHandle = handle;
				if (currentHandle != null) {
					currentHandle.resume();
				}
			}

			private void cancel() {
				if (terminated) {
					return;
				}
				cancellationRequested = true;
				var currentHandle = handle;
				if (currentHandle != null) {
					currentHandle.cancel();
				}
			}

			private void admissionFailed(Throwable failure) {
				finish(failure);
			}

			private void prepareCompletion() {
				synchronized (this) {
					if (terminated) {
						return;
					}
					if (completionPrepared) {
						throw new IllegalStateException("Iterator stream completion was prepared twice");
					}
					completionPrepared = true;
				}
			}

			@Override
			public void completeCooperatively() {
				synchronized (this) {
					if (terminated) {
						return;
					}
					if (!completionPrepared) {
						throw new IllegalStateException(
								"Scheduler selected RUN without an iterator stream result");
					}
				}
				finish(null);
			}

			@Override
			public void reject(RuntimeException failure) {
				finish(failure);
			}

			private void finish(@Nullable Throwable failure) {
				if (!TERMINATED.compareAndSet(this, false, true)) {
					return;
				}
				page = null;
				iteratorLease.taskAndOperationTerminated();
				if (cancellationRequested || sink.isCancelled()) {
					if (failure != null
							&& !(failure instanceof java.util.concurrent.CancellationException)) {
						lateErrors.accept(failure);
					}
				} else if (failure == null) {
					sink.complete();
				} else {
					sink.error(failure);
				}
			}

			@Override
			public void dispose() {
				cancel();
			}

			@Override
			public boolean isDisposed() {
				return terminated;
			}
		}

		private record IteratorQuantumLimits(long maximumItems,
				long maximumBytes,
				long maximumDurationNanos) {
		}

		/** Tracks whether cancellation won before a scheduled callable began running. */
		private final class ScheduledTaskLifecycle {

			private static final int QUEUED = 0;
			private static final int RUNNING = 1;
			private static final int TERMINATED = 2;
			private static final VarHandle STATE;

			static {
				try {
					STATE = MethodHandles.lookup().findVarHandle(ScheduledTaskLifecycle.class, "state", int.class);
				} catch (NoSuchFieldException | IllegalAccessException failure) {
					throw new ExceptionInInitializerError(failure);
				}
			}

			private final IteratorOperationLease iteratorLease;
			private volatile int state = QUEUED;

			private ScheduledTaskLifecycle(IteratorOperationLease iteratorLease) {
				this.iteratorLease = iteratorLease;
			}

			private boolean start() {
				return STATE.compareAndSet(this, QUEUED, RUNNING);
			}

			private void cancelBeforeStart() {
				if (STATE.compareAndSet(this, QUEUED, TERMINATED)) {
					taskTerminated();
				}
			}

			private void runningTaskTerminated() {
				if (!STATE.compareAndSet(this, RUNNING, TERMINATED)) {
					throw new IllegalStateException("Scheduled task did not terminate from running state");
				}
				taskTerminated();
			}

			private void taskTerminated() {
				iteratorLease.taskTerminated();
			}
		}

		private RocksDBException concurrentIteratorOperation(long iteratorId) {
			return RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Concurrent operation on iterator " + iteratorId + " is not supported");
		}

		/**
		 * Bridge an externally-owned future without delivering its terminal signal after
		 * the RPC has been cancelled. Native calls may finish after cancellation; their
		 * late failures still go through the request-scoped diagnostic handler instead of
		 * Reactor's global onErrorDropped hook.
		 */
		private <T> Mono<T> fromCancellableFuture(CompletableFuture<T> future) {
			return bridgeCancellableFuture(future, null);
		}

		/**
		 * Start one logical iterator continuation only after its server-side lease has
		 * registered an active task. RPC cancellation releases the subscriber side of the
		 * lease immediately, but the task side remains registered until the underlying
		 * future terminates after any in-flight JNI step.
		 */
		private <T> Mono<T> fromCancellableIteratorFuture(
				Supplier<CompletableFuture<T>> futureSupplier) {
			return Mono.deferContextual(contextView -> {
				var iteratorLease = contextView.<IteratorOperationLease>getOrDefault(
						ITERATOR_OPERATION_LEASE_CONTEXT_KEY, null);
				if (iteratorLease == null) {
					return Mono.error(new IllegalStateException(
							"Iterator future was started without an operation lease"));
				}
				if (!iteratorLease.registerTask()) {
					return Mono.error(new java.util.concurrent.CancellationException(
							"Iterator operation terminated before its continuation could start"));
				}
				final CompletableFuture<T> future;
				try {
					future = Objects.requireNonNull(futureSupplier.get(), "futureSupplier result");
				} catch (Throwable failure) {
					iteratorLease.taskTerminated();
					return Mono.error(failure);
				}
				return bridgeCancellableFuture(future, iteratorLease);
			});
		}

		private <T> Mono<T> bridgeCancellableFuture(CompletableFuture<T> future,
				@Nullable IteratorOperationLease terminalLease) {
			return Mono.create(sink -> {
				var bridge = new CancellableFutureBridge<>(future, sink, terminalLease);
				sink.onCancel(bridge);
				future.whenComplete(bridge);
			});
		}

		private final class CancellableFutureBridge<T> implements Disposable, BiConsumer<T, Throwable> {

			private final CompletableFuture<T> future;
			private final reactor.core.publisher.MonoSink<T> sink;
			private final @Nullable IteratorOperationLease terminalLease;
			private boolean cancelled;

			private CancellableFutureBridge(CompletableFuture<T> future,
					reactor.core.publisher.MonoSink<T> sink,
					@Nullable IteratorOperationLease terminalLease) {
				this.future = future;
				this.sink = sink;
				this.terminalLease = terminalLease;
			}

			@Override
			public void dispose() {
				synchronized (this) {
					cancelled = true;
				}
				future.cancel(true);
			}

			@Override
			public void accept(T value, Throwable failure) {
				Throwable error = failure instanceof CompletionException completionError
						&& completionError.getCause() != null
						? completionError.getCause()
						: failure;
				if (terminalLease != null) {
					terminalLease.taskTerminated();
				}
				boolean late;
				synchronized (this) {
					late = cancelled;
					if (!late) {
						if (error != null) {
							sink.error(error);
						} else {
							sink.success(value);
						}
					}
				}
				if (late && error != null
						&& !(error instanceof java.util.concurrent.CancellationException)) {
					lateErrorHandler(sink.contextView()).accept(error);
				}
			}
		}

		private final class ScheduledCall<T> implements Disposable,
				Runnable,
				RWScheduler.EstimatedWork,
				RWScheduler.RejectionAwareTask {

			private final reactor.core.publisher.MonoSink<T> sink;
			private final Callable<T> callable;
			private final reactor.core.scheduler.Scheduler executionScheduler;
			private final @Nullable Consumer<T> lateSuccessCleanup;
			private final @Nullable reactor.core.scheduler.Scheduler lateSuccessCleanupScheduler;
			private final @Nullable ScheduledTaskLifecycle taskLifecycle;
			private final boolean mustComplete;
			private final @Nullable String protectedOperation;
			private final long estimatedBytes;
			private @Nullable Disposable task;
			private @Nullable Object pendingResult;
			private boolean cancelled;
			private boolean running;
			private boolean terminated;

			private ScheduledCall(reactor.core.publisher.MonoSink<T> sink,
					Callable<T> callable,
					reactor.core.scheduler.Scheduler executionScheduler,
					@Nullable Consumer<T> lateSuccessCleanup,
					@Nullable reactor.core.scheduler.Scheduler lateSuccessCleanupScheduler,
					@Nullable ScheduledTaskLifecycle taskLifecycle,
					boolean mustComplete,
					@Nullable String protectedOperation,
					long estimatedBytes) {
				this.sink = sink;
				this.callable = callable;
				this.executionScheduler = executionScheduler;
				this.lateSuccessCleanup = lateSuccessCleanup;
				this.lateSuccessCleanupScheduler = lateSuccessCleanupScheduler;
				this.taskLifecycle = taskLifecycle;
				this.mustComplete = mustComplete;
				this.protectedOperation = protectedOperation;
				this.estimatedBytes = estimatedBytes;
			}

			@Override
			public long estimatedBytes() {
				return estimatedBytes;
			}

			private void schedule() {
				final Disposable submitted;
				try {
					submitted = executionScheduler.schedule(this);
				} catch (Throwable schedulingError) {
					schedulingFailed(schedulingError);
					return;
				}
				boolean disposeSubmitted;
				synchronized (this) {
					task = submitted;
					disposeSubmitted = cancelled && !mustComplete;
				}
				if (disposeSubmitted) {
					submitted.dispose();
				}
			}

			@Override
			public void dispose() {
				final Disposable submitted;
				final @Nullable Object cancelledResult;
				synchronized (this) {
					if (cancelled) {
						return;
					}
					cancelled = true;
					cancelledResult = pendingResult;
					pendingResult = null;
					submitted = mustComplete ? null : task;
				}
				cleanupCancelledResult(cancelledResult);
				if (mustComplete) {
					cancelledMustCompleteOperations.incrementAndGet();
				} else {
					if (taskLifecycle != null) {
						taskLifecycle.cancelBeforeStart();
					}
					if (submitted != null) {
						submitted.dispose();
					}
				}
			}

			@Override
			public void run() {
				if (!claimRun()) {
					return;
				}
				if (taskLifecycle != null && !taskLifecycle.start()) {
					markAbortedRunTerminated();
					return;
				}
				final T result;
				try {
					result = callable.call();
				} catch (Throwable error) {
					runningTaskTerminated();
					emitOrRecordLateError(error);
					return;
				}
				runningTaskTerminated();
				publishResult(result);
			}

			private void runningTaskTerminated() {
				synchronized (this) {
					if (!running || terminated) {
						throw new IllegalStateException("Scheduled call did not terminate exactly once from running state");
					}
					running = false;
					terminated = true;
				}
				if (taskLifecycle != null) {
					taskLifecycle.runningTaskTerminated();
				}
				if (mustComplete) {
					mustCompleteOperations.operationTerminated();
				}
			}

			private void runLateSuccessCleanup(T result) {
				Runnable cleanup = () -> {
					try {
						lateSuccessCleanup.accept(result);
					} catch (Throwable cleanupError) {
						lateErrorHandler(sink.contextView()).accept(cleanupError);
					}
				};
				try {
					if (lateSuccessCleanupScheduler != null) {
						lateSuccessCleanupScheduler.schedule(cleanup);
					} else {
						cleanup.run();
					}
				} catch (Throwable schedulingError) {
					LOG.debug("Late-success cleanup scheduler rejected the task; "
							+ "running iterator cleanup inline", schedulingError);
					cleanup.run();
				}
			}

			private void publishResult(T result) {
				Object cancelledResult = null;
				boolean emit;
				synchronized (this) {
					if (pendingResult != null) {
						throw new IllegalStateException("Scheduled call already owns a pending result");
					}
					pendingResult = ReactorResultOwnership.encode(result);
					emit = !cancelled;
					if (!emit) {
						cancelledResult = pendingResult;
						pendingResult = null;
					}
				}
				if (emit) {
					sink.success(result);
				} else {
					cleanupCancelledResult(cancelledResult);
				}
			}

			private void resultDisposed() {
				Object cancelledResult = null;
				synchronized (this) {
					if (cancelled) {
						cancelledResult = pendingResult;
					}
					pendingResult = null;
				}
				cleanupCancelledResult(cancelledResult);
			}

			private void cleanupCancelledResult(@Nullable Object encodedResult) {
				if (encodedResult == null || lateSuccessCleanup == null) {
					return;
				}
				runLateSuccessCleanup(ReactorResultOwnership.decode(encodedResult));
			}

			private void schedulingFailed(Throwable schedulingError) {
				if (schedulingError instanceof RuntimeException runtimeFailure) {
					reject(runtimeFailure);
				} else {
					reject(new RejectedExecutionException("gRPC scheduling failed", schedulingError));
				}
			}

			@Override
			public void reject(RuntimeException failure) {
				if (!claimRejection()) {
					return;
				}
				if (taskLifecycle != null) {
					taskLifecycle.cancelBeforeStart();
				}
				if (mustComplete) {
					mustCompleteOperations.operationTerminated();
				}
				emitOrRecordLateError(failure);
			}

			private boolean claimRun() {
				synchronized (this) {
					if (terminated || running) {
						return false;
					}
					if (cancelled && !mustComplete) {
						terminated = true;
						return false;
					}
					running = true;
					return true;
				}
			}

			private boolean claimRejection() {
				synchronized (this) {
					if (terminated || running) {
						return false;
					}
					terminated = true;
					return true;
				}
			}

			private void markAbortedRunTerminated() {
				synchronized (this) {
					running = false;
					terminated = true;
				}
			}

			private void emitOrRecordLateError(Throwable error) {
				boolean lateError;
				synchronized (this) {
					lateError = cancelled;
					if (!lateError) {
						sink.error(error);
					}
				}
				if (lateError) {
					if (mustComplete) {
						recordLateProtectedOperationFailure(
								Objects.requireNonNull(protectedOperation),
								error,
								sink.contextView());
					} else {
						lateErrorHandler(sink.contextView()).accept(error);
					}
				}
			}
		}

		private <T> Mono<T> executeScheduled(Callable<T> callable, reactor.core.scheduler.Scheduler executionScheduler) {
			return executeScheduled(callable, executionScheduler, 0L);
		}

		private <T> Mono<T> executeScheduled(Callable<T> callable,
				reactor.core.scheduler.Scheduler executionScheduler,
				long estimatedBytes) {
			return executeScheduled(callable,
					executionScheduler,
					null,
					null,
					ScheduledCancellationPolicy.CANCEL_WHILE_QUEUED,
					null,
					estimatedBytes);
		}

		private <T> Mono<T> executeMustComplete(String operation,
				Callable<T> callable,
				reactor.core.scheduler.Scheduler executionScheduler) {
			return executeScheduled(callable,
					executionScheduler,
					null,
					null,
					ScheduledCancellationPolicy.MUST_COMPLETE,
					operation,
					0L);
		}

		private <T> Mono<T> executeScheduled(Callable<T> callable,
				reactor.core.scheduler.Scheduler executionScheduler,
				@Nullable Consumer<T> lateSuccessCleanup,
				@Nullable reactor.core.scheduler.Scheduler lateSuccessCleanupScheduler) {
			return executeScheduled(callable,
					executionScheduler,
					lateSuccessCleanup,
					lateSuccessCleanupScheduler,
					0L);
		}

		private <T> Mono<T> executeScheduled(Callable<T> callable,
				reactor.core.scheduler.Scheduler executionScheduler,
				@Nullable Consumer<T> lateSuccessCleanup,
				@Nullable reactor.core.scheduler.Scheduler lateSuccessCleanupScheduler,
				long estimatedBytes) {
			return executeScheduled(callable,
					executionScheduler,
					lateSuccessCleanup,
					lateSuccessCleanupScheduler,
					ScheduledCancellationPolicy.CANCEL_WHILE_QUEUED,
					null,
					estimatedBytes);
		}

		private <T> Mono<T> executeScheduled(Callable<T> callable,
				reactor.core.scheduler.Scheduler executionScheduler,
				@Nullable Consumer<T> lateSuccessCleanup,
				@Nullable reactor.core.scheduler.Scheduler lateSuccessCleanupScheduler,
				ScheduledCancellationPolicy cancellationPolicy,
				@Nullable String protectedOperation,
				long estimatedBytes) {
			return Mono.deferContextual(contextView -> {
				var iteratorLease = contextView.<IteratorOperationLease>getOrDefault(
						ITERATOR_OPERATION_LEASE_CONTEXT_KEY, null);
				ScheduledTaskLifecycle taskLifecycle;
				if (iteratorLease != null) {
					if (!iteratorLease.registerTask()) {
						return Mono.error(new java.util.concurrent.CancellationException(
								"Iterator operation terminated before its task could be scheduled"));
					}
					taskLifecycle = new ScheduledTaskLifecycle(iteratorLease);
				} else {
					taskLifecycle = null;
				}
				return Mono.<T>create(sink -> {
					boolean mustComplete = cancellationPolicy == ScheduledCancellationPolicy.MUST_COMPLETE;
					if (mustComplete && !mustCompleteOperations.register()) {
						if (taskLifecycle != null) {
							taskLifecycle.cancelBeforeStart();
						}
						sink.error(new RejectedExecutionException("gRPC server is shutting down"));
						return;
					}
					// RocksDB JNI calls may keep running after interruption until their native deadline. The
					// lifecycle object serializes cancellation with terminal delivery and retains the submitted
					// handle so cancellation before handle publication still removes queued work.
					var scheduledCall = new ScheduledCall<>(sink,
							callable,
							executionScheduler,
							lateSuccessCleanup,
							lateSuccessCleanupScheduler,
								taskLifecycle,
								mustComplete,
								protectedOperation,
								estimatedBytes);
					sink.onCancel(scheduledCall);
					sink.onDispose(scheduledCall::resultDisposed);
					scheduledCall.schedule();
				});
			});
		}

		// mappers

		private <T> Function<Flux<T>, Flux<T>> onErrorMapFluxWithRequestInfo(String requestName, Message request) {
			return flux -> {
				var lateErrorHandler = lateRequestErrorHandler(requestName, request);
				return flux
						.onErrorResume(throwable -> Mono.error(mapRequestError(requestName, request, throwable)))
						.contextWrite(context -> context
								.put(GRPC_LATE_ERROR_HANDLER_CONTEXT_KEY, lateErrorHandler)
								.put(REACTOR_ON_ERROR_DROPPED_CONTEXT_KEY, lateErrorHandler));
			};
		}

		private <T> Function<Mono<T>, Mono<T>> onErrorMapMonoWithRequestInfo(String requestName, Message request) {
			return mono -> {
				var lateErrorHandler = lateRequestErrorHandler(requestName, request);
				return mono
						.onErrorResume(throwable -> Mono.error(mapRequestError(requestName, request, throwable)))
						.contextWrite(context -> context
								.put(GRPC_LATE_ERROR_HANDLER_CONTEXT_KEY, lateErrorHandler)
								.put(REACTOR_ON_ERROR_DROPPED_CONTEXT_KEY, lateErrorHandler));
			};
		}

		private Throwable mapRequestError(String requestName, Message request, Throwable throwable) {
			var ex = handleError(throwable).asException();
			if (ex.getStatus().getCode() == Code.INTERNAL && findRocksDBException(throwable) == null) {
				LOG.error("Unexpected internal gRPC request failure: operation={}, requestType={}, request={}",
						requestName,
						request.getDescriptorForType().getFullName(),
						summarizeRequest(request),
						throwable);
				return RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR,
						ex.getCause() != null ? ex.getCause() : throwable);
			}
			return ex;
		}

		private static Consumer<Throwable> lateErrorHandler(ContextView context) {
			return context.getOrDefault(GRPC_LATE_ERROR_HANDLER_CONTEXT_KEY,
					UNCONTEXTUALIZED_LATE_ERROR_HANDLER);
		}

		private static Consumer<Throwable> lateRequestErrorHandler(String requestName, Message request) {
			return error -> logLateRequestError(requestName, request, error);
		}

		private static void logLateRequestError(String requestName, Message request, Throwable error) {
			var requestType = request.getDescriptorForType().getFullName();
			var requestSummary = summarizeRequest(request);
			var rocksDBException = findRocksDBException(error);
			var status = Status.fromThrowable(error);

			if (rocksDBException != null) {
				var statusCode = lateErrorStatusCode(status, rocksDBException);
				Long suppressed = rocksDBException.getErrorUniqueId() == RocksDBErrorType.READ_DEADLINE_EXCEEDED
						? claimLateReadDeadlineLog(requestName)
						: 0L;
				if (suppressed == null) {
					return;
				}
				if (suppressed == 0L) {
					LOG.warn("Late gRPC request failure after call termination: operation={}, requestType={}, "
							+ "request={}, errorType={}, grpcStatus={}, message={}",
							requestName,
							requestType,
							requestSummary,
							rocksDBException.getErrorUniqueId(),
							statusCode,
							sanitizeForLog(rocksDBException.getMessage()));
				} else {
					LOG.warn("Late gRPC request failure after call termination: operation={}, requestType={}, "
							+ "request={}, errorType={}, grpcStatus={}, message={}, suppressedSimilarFailures={}",
							requestName,
							requestType,
							requestSummary,
							rocksDBException.getErrorUniqueId(),
							statusCode,
							sanitizeForLog(rocksDBException.getMessage()),
							suppressed);
				}
				LOG.debug("Late gRPC request failure stack: operation={}, requestType={}",
						requestName,
						requestType,
						error);
				return;
			}

			if (status.getCode() == Code.CANCELLED
					|| error instanceof java.util.concurrent.CancellationException) {
				LOG.debug("Late gRPC cancellation after call termination: operation={}, requestType={}, request={}, "
						+ "description={}",
						requestName,
						requestType,
						requestSummary,
						sanitizeForLog(status.getDescription()));
				return;
			}

			if (status.getCode() != Code.UNKNOWN && status.getCode() != Code.INTERNAL) {
				LOG.warn("Late gRPC transport failure after call termination: operation={}, requestType={}, request={}, "
						+ "grpcStatus={}, description={}",
						requestName,
						requestType,
						requestSummary,
						status.getCode(),
						sanitizeForLog(status.getDescription()));
				LOG.debug("Late gRPC transport failure stack: operation={}, requestType={}",
						requestName,
						requestType,
						error);
				return;
			}

			LOG.error("Unexpected late gRPC request failure after call termination: operation={}, requestType={}, "
					+ "request={}, grpcStatus={}, description={}",
					requestName,
					requestType,
					requestSummary,
					status.getCode(),
					sanitizeForLog(status.getDescription()),
					error);
		}

		@Nullable
		private static RocksDBException findRocksDBException(Throwable error) {
			var current = error;
			for (int depth = 0; current != null && depth < 32; depth++) {
				if (current instanceof RocksDBException rocksDBException) {
					return rocksDBException;
				}
				var cause = current.getCause();
				if (cause == current) {
					break;
				}
				current = cause;
			}
			return null;
		}

		private static String summarizeRequest(Message request) {
			try {
				return summarizeMessage(request, 0);
			} catch (Throwable summaryError) {
				return "{summaryUnavailable=" + summaryError.getClass().getSimpleName() + "}";
			}
		}

		private static String summarizeMessage(Message message, int depth) {
			var fields = new StringJoiner(", ", "{", "}");
			for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
				var fieldName = field.getJsonName();
				var value = message.getField(field);
				if (field.isRepeated()) {
					fields.add(fieldName + ".count=" + ((List<?>) value).size());
					continue;
				}
				if (field.hasPresence() && !message.hasField(field)) {
					continue;
				}
				fields.add(fieldName + "=" + summarizeFieldValue(field, value, depth));
			}
			return fields.toString();
		}

		private static String summarizeFieldValue(FieldDescriptor field, Object value, int depth) {
			return switch (field.getJavaType()) {
				case BYTE_STRING -> "bytes(" + ((ByteString) value).size() + ")";
				case MESSAGE -> depth < 1
						? summarizeMessage((Message) value, depth + 1)
						: "message(" + ((Message) value).getDescriptorForType().getFullName() + ")";
				case STRING -> "\"" + sanitizeForLog(String.valueOf(value)) + "\"";
				default -> String.valueOf(value);
			};
		}

		private static String sanitizeForLog(@Nullable String value) {
			if (value == null) {
				return "<none>";
			}
			var sanitized = value
					.replace("\\", "\\\\")
					.replace("\r", "\\r")
					.replace("\n", "\\n");
			return sanitized.length() <= 256 ? sanitized : sanitized.substring(0, 253) + "...";
		}

		@Override
		protected Throwable onErrorMap(Throwable throwable) {
			var ex = handleError(throwable).asException();
			if (ex.getStatus().getCode() == Code.INTERNAL && findRocksDBException(throwable) == null) {
				LOG.error("Unexpected internal error during request", ex);
			}
			return ex;
		}

		private static KV unmapKVHeap(it.cavallium.rockserver.core.common.KV kv) {
			if (kv == null) return null;
			var result = KV.newBuilder();
			for (@NotNull Buf key : kv.keys().keys()) {
				result.addKeys(UnsafeByteOperations.unsafeWrap(toByteArray(key)));
			}
			return result
					.setValue(unmapValueHeap(kv.value()))
					.build();
		}

		private static ByteString unmapValueHeap(@Nullable Buf value) {
			if (value == null) return null;
			return Utils.toByteString(value);
		}

		private static ColumnSchema mapColumnSchema(it.cavallium.rockserver.core.common.api.proto.ColumnSchema schema) {
			return ColumnSchema.of(mapKeysLength(schema.getFixedKeysCount(), schema::getFixedKeys),
					mapVariableTailKeys(schema.getVariableTailKeysCount(), schema::getVariableTailKeys),
					schema.getHasValue(),
					schema.hasMergeOperatorName() ? schema.getMergeOperatorName() : null,
					schema.hasMergeOperatorVersion() ? schema.getMergeOperatorVersion() : null,
					schema.hasMergeOperatorClass() ? schema.getMergeOperatorClass() : null
			);
		}

		private static it.cavallium.rockserver.core.common.api.proto.ColumnSchema unmapColumnSchema(@NotNull ColumnSchema schema) {
			var builder = it.cavallium.rockserver.core.common.api.proto.ColumnSchema.newBuilder()
					.addAllFixedKeys(unmapFixedKeys(schema))
					.addAllVariableTailKeys(unmapVariableTailKeys(schema))
					.setHasValue(schema.hasValue());
			if (schema.mergeOperatorName() != null) {
				builder.setMergeOperatorName(schema.mergeOperatorName());
			}
			if (schema.mergeOperatorVersion() != null) {
				builder.setMergeOperatorVersion(schema.mergeOperatorVersion());
			}
			if (schema.mergeOperatorClass() != null) {
				builder.setMergeOperatorClass(schema.mergeOperatorClass());
			}
			return builder.build();
		}

		private static Iterable<Integer> unmapFixedKeys(@NotNull ColumnSchema schema) {
			var result = new IntArrayList(schema.fixedLengthKeysCount());
			for (int i = 0; i < schema.fixedLengthKeysCount(); i++) {
				result.add(schema.key(i));
			}
			return result;
		}

		private static Iterable<it.cavallium.rockserver.core.common.api.proto.ColumnHashType> unmapVariableTailKeys(@NotNull ColumnSchema schema) {
			var result = new ArrayList<it.cavallium.rockserver.core.common.api.proto.ColumnHashType>(schema.variableTailKeys().size());
			for (it.cavallium.rockserver.core.common.ColumnHashType variableTailKey : schema.variableTailKeys()) {
				result.add(switch (variableTailKey) {
					case XXHASH32 -> it.cavallium.rockserver.core.common.api.proto.ColumnHashType.XXHASH32;
					case XXHASH8 -> it.cavallium.rockserver.core.common.api.proto.ColumnHashType.XXHASH8;
					case ALLSAME8 -> it.cavallium.rockserver.core.common.api.proto.ColumnHashType.ALLSAME8;
					case FIXEDINTEGER32 -> it.cavallium.rockserver.core.common.api.proto.ColumnHashType.FIXEDINTEGER32;
				});
			}
			return result;
		}

		private static IntList mapKeysLength(int count, Int2IntFunction keyGetterAt) {
			var l = new IntArrayList(count);
			for (int i = 0; i < count; i++) {
				l.add((int) keyGetterAt.apply(i));
			}
			return l;
		}

		private static ObjectList<ColumnHashType> mapVariableTailKeys(int count,
				Int2ObjectFunction<it.cavallium.rockserver.core.common.api.proto.ColumnHashType> variableTailKeyGetterAt) {
			var l = new ObjectArrayList<ColumnHashType>(count);
			for (int i = 0; i < count; i++) {
				l.add(switch (variableTailKeyGetterAt.apply(i)) {
					case XXHASH32 -> ColumnHashType.XXHASH32;
					case XXHASH8 -> ColumnHashType.XXHASH8;
					case ALLSAME8 -> ColumnHashType.ALLSAME8;
					case FIXEDINTEGER32 -> ColumnHashType.FIXEDINTEGER32;
					case UNRECOGNIZED -> throw new UnsupportedOperationException();
				});
			}
			return l;
		}

		private static Keys mapKeys(List<ByteString> wireKeys) {
			var segments = new Buf[wireKeys.size()];
			for (int i = 0; i < segments.length; i++) {
				segments[i] = toBuf(wireKeys.get(i));
			}
			return new Keys(segments);
		}

		private static KVBatch mapKVBatch(List<KV> entries) {
			var keys = new ArrayList<Keys>(entries.size());
			var values = new ArrayList<Buf>(entries.size());
			for (var entry : entries) {
				keys.add(mapKeys(entry.getKeysList()));
				values.add(toBuf(entry.getValue()));
			}
			return new KVBatchRef(keys, values);
		}

		private static Status handleError(Throwable ex) {
			if (ex instanceof StatusRuntimeException e && e.getStatus().getCode().equals(Status.CANCELLED.getCode())) {
				return e.getStatus();
			}

			var rocksError = findRocksDBException(ex);
			if (rocksError != null) {
				return switch (rocksError.getErrorUniqueId()) {
						case COMPACTION_CONFLICT -> Status.ABORTED
                                .withDescription(rocksError.getLocalizedMessage()).withCause(rocksError);
                        case PUT_INVALID_REQUEST -> Status.INVALID_ARGUMENT
								.withDescription(rocksError.getLocalizedMessage()).withCause(rocksError);
						case CDC_SUBSCRIPTION_NOT_FOUND, TRANSACTION_NOT_FOUND -> Status.NOT_FOUND
								.withDescription(rocksError.getLocalizedMessage()).withCause(rocksError);
						case CDC_RESPONSE_TOO_LARGE -> Status.FAILED_PRECONDITION
								.withDescription(rocksError.getLocalizedMessage()).withCause(rocksError);
						case READ_DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
								.withDescription(rocksError.getLocalizedMessage()).withCause(rocksError);
						case SERVER_OVERLOADED -> Status.RESOURCE_EXHAUSTED
								.withDescription(rocksError.getLocalizedMessage()).withCause(rocksError);
						default -> Status.INTERNAL
								.withDescription(rocksError.getLocalizedMessage()).withCause(rocksError);
				};
			}
			return switch (ex) {
				case CompletionException exx -> handleError(exx.getCause());
				case StatusException ex2 -> ex2.getStatus();
				case StatusRuntimeException ex3 -> ex3.getStatus();
				case null, default -> Status.INTERNAL.withCause(ex);
			};
		}
	}

	@Override
	public void close() throws IOException {
		LOG.info("GRPC server is shutting down...");
		var gracefulShutdownTimeout = durationProperty("it.cavallium.rockserver.grpc.server.shutdown-graceful-timeout-ms",
				Duration.ofMinutes(1));
		var forcedShutdownTimeout = durationProperty("it.cavallium.rockserver.grpc.server.shutdown-forced-timeout-ms",
				Duration.ofMinutes(1));
		var schedulerShutdownTimeout = durationProperty("it.cavallium.rockserver.grpc.server.scheduler-shutdown-timeout-ms",
				Duration.ofMinutes(2));
		boolean interrupted = false;
		IOException closeFailure = null;
		server.shutdown();
		var gracefulTermination = awaitTerminationUninterruptibly(server, gracefulShutdownTimeout);
		interrupted |= gracefulTermination.interrupted();
		if (!gracefulTermination.terminated()) {
			server.shutdownNow();
			var forcedTermination = awaitTerminationUninterruptibly(server, forcedShutdownTimeout);
			interrupted |= forcedTermination.interrupted();
			if (!forcedTermination.terminated()) {
				LOG.error("GRPC server did not terminate after forced shutdown");
			}
		}
		var protectedDrain = mustCompleteOperations.stopAcceptingAndAwait(schedulerShutdownTimeout);
		interrupted |= protectedDrain.interrupted();
		if (!protectedDrain.drained()) {
			int remaining = mustCompleteOperations.acceptedOperations();
			LOG.error("GRPC server timed out draining {} accepted protected operations; "
					+ "its scheduler will not be terminated by this server", remaining);
			closeFailure = new IOException("Timed out draining " + remaining
					+ " accepted protected gRPC operations");
		}
		try {
			elg.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
		} catch (InterruptedException e) {
			interrupted = true;
			LOG.warn("Grpc server event loop shutdown interrupted; continuing to wait", e);
			elg.terminationFuture().syncUninterruptibly();
		}
		if (ownsScheduler && protectedDrain.drained()) {
			try {
				scheduler.disposeGracefully().timeout(schedulerShutdownTimeout).onErrorResume(ex -> {
					LOG.error("Grpc server executor shutdown timed out, terminating...", ex);
					scheduler.dispose();
					return Mono.empty();
				}).block();
			} catch (RuntimeException error) {
				closeFailure = appendCloseFailure(closeFailure,
						new IOException("Failed to terminate the gRPC scheduler", error));
			}
		}
		try {
			super.close();
		} catch (IOException error) {
			closeFailure = appendCloseFailure(closeFailure, error);
		} finally {
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
		if (closeFailure != null) {
			throw closeFailure;
		}
		LOG.info("GRPC server shut down.");
	}

	private static AwaitTerminationResult awaitTerminationUninterruptibly(io.grpc.Server server,
			Duration timeout) {
		long timeoutNanos = saturatedTimeoutNanos(timeout);
		long remainingNanos = timeoutNanos;
		long waitStartedNanos = System.nanoTime();
		boolean interrupted = false;
		do {
			try {
				return new AwaitTerminationResult(
						server.awaitTermination(Math.max(0L, remainingNanos), TimeUnit.NANOSECONDS),
						interrupted);
			} catch (InterruptedException _) {
				interrupted = true;
				remainingNanos = remainingTimeoutNanos(timeoutNanos, waitStartedNanos);
			}
		} while (remainingNanos > 0L);
		return new AwaitTerminationResult(server.isTerminated(), interrupted);
	}

	private static long saturatedTimeoutNanos(Duration timeout) {
		if (timeout.isNegative()) {
			throw new IllegalArgumentException("timeout must not be negative");
		}
		try {
			return timeout.toNanos();
		} catch (ArithmeticException overflow) {
			return Long.MAX_VALUE;
		}
	}

	private static long remainingTimeoutNanos(long timeoutNanos, long waitStartedNanos) {
		long elapsedNanos = System.nanoTime() - waitStartedNanos;
		if (elapsedNanos <= 0L) {
			return timeoutNanos;
		}
		return elapsedNanos >= timeoutNanos ? 0L : timeoutNanos - elapsedNanos;
	}

	private static IOException appendCloseFailure(@Nullable IOException current, IOException additional) {
		if (current == null) {
			return additional;
		}
		current.addSuppressed(additional);
		return current;
	}

	private static Duration durationProperty(String name, Duration defaultValue) {
		var value = System.getProperty(name);
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		try {
			long millis = Long.parseLong(value);
			if (millis < 0) {
				LOG.warn("Invalid negative duration for system property {}: {}", name, value);
				return defaultValue;
			}
			return Duration.ofNanos(TimeUnit.MILLISECONDS.toNanos(millis));
		} catch (NumberFormatException ex) {
			LOG.warn("Invalid duration in milliseconds for system property {}: {}", name, value);
			return defaultValue;
		}
	}
}
