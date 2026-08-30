package it.cavallium.rockserver.core.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.FirstAndLast;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.KVBatch.KVBatchRef;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestType.RequestChanged;
import it.cavallium.rockserver.core.common.RequestType.RequestDelete;
import it.cavallium.rockserver.core.common.RequestType.RequestDelta;
import it.cavallium.rockserver.core.common.RequestType.RequestExists;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestMulti;
import it.cavallium.rockserver.core.common.RequestType.RequestNothing;
import it.cavallium.rockserver.core.common.RequestType.RequestPrevious;
import it.cavallium.rockserver.core.common.RequestType.RequestPreviousPresence;
import it.cavallium.rockserver.core.common.RequestType.RequestMerge;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.SerializedKVBatch.SerializedKVBatchRef;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.api.proto.ColumnHashType;
import it.cavallium.rockserver.core.common.api.proto.Delta;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc.RocksDBServiceFutureStub;
import it.unimi.dsi.fastutil.ints.Int2IntFunction;
import it.unimi.dsi.fastutil.ints.Int2ObjectFunction;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.cavallium.buffer.Buf;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.time.Duration;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;

import static it.cavallium.rockserver.core.common.Utils.toBuf;

/** Public gRPC connection. Database operations are exposed only by context-bound API views. */
public final class GrpcConnection extends BaseConnection {

	public static final String MAX_INBOUND_MESSAGE_SIZE_PROPERTY =
			GrpcConnectionDelegate.MAX_INBOUND_MESSAGE_SIZE_PROPERTY;
	public static final int DEFAULT_MAX_INBOUND_MESSAGE_SIZE =
			GrpcConnectionDelegate.DEFAULT_MAX_INBOUND_MESSAGE_SIZE;
	public static final int MIN_MAX_INBOUND_MESSAGE_SIZE =
			GrpcConnectionDelegate.MIN_MAX_INBOUND_MESSAGE_SIZE;

	private final GrpcConnectionDelegate delegate;
	// Retained as test-observable lifecycle handles; ownership stays with the raw delegate.
	private final ManagedChannel channel;
	private final ExecutorService callbackExecutor;
	private final EventLoopGroup eventLoopGroup;

	private GrpcConnection(String name, SocketAddress socketAddress, URI address) {
		super(name);
		this.delegate = new GrpcConnectionDelegate(name, socketAddress, address);
		this.channel = delegate.channel;
		this.callbackExecutor = delegate.callbackExecutor;
		this.eventLoopGroup = delegate.eventLoopGroup;
	}

	public static int configuredMaxInboundMessageSize() {
		return GrpcConnectionDelegate.configuredMaxInboundMessageSize();
	}

	public static GrpcConnection forHostAndPort(String name, HostAndPort address) {
		return new GrpcConnection(name,
				new InetSocketAddress(address.host(), address.port()),
				URI.create("http://" + address.host() + ":" + address.port()));
	}

	public static GrpcConnection forPath(String name, Path unixSocketPath) {
		return new GrpcConnection(name,
				new DomainSocketAddress(unixSocketPath.toFile()),
				URI.create("unix://" + unixSocketPath));
	}

	@Override
	public URI getUrl() {
		return delegate.getUrl();
	}

	@Override
	public RockserverCapabilities getCapabilities() {
		return delegate.getCapabilities();
	}

	@Override
	<R, RS, RA> RS requestSync(BoundRequestContext context, RocksDBAPICommand<R, RS, RA> request) {
		return delegate.requestSync(context, request);
	}

	@Override
	<R, RS, RA> RA requestAsync(BoundRequestContext context, RocksDBAPICommand<R, RS, RA> request) {
		return delegate.requestAsync(context, request);
	}

	@Override
	Mono<CdcBatch> cdcPollBatchAsync(BoundRequestContext context,
			@NotNull String id,
			@Nullable Long fromSeq,
			long maxEvents) {
		return delegate.cdcPollBatchAsync(context, id, fromSeq, maxEvents);
	}

	@Override
	public void close() {
		delegate.close();
	}
}

/** Package-private raw implementation behind {@link GrpcConnection}. */
final class GrpcConnectionDelegate extends BaseConnection implements RocksDBAPI {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcConnection.class);
	private static final int EVENT_LOOP_THREADS_PER_CONNECTION = 1;
	private static final long CALLBACK_EXECUTOR_SHUTDOWN_SECONDS = 5;
	private static final long CAPABILITY_HANDSHAKE_TIMEOUT_SECONDS = 10;
	private static final String MAX_RETRY_ATTEMPTS_PROPERTY
			= "it.cavallium.rockserver.grpc.client.max-retry-attempts";
	private static final String INITIAL_RETRY_BACKOFF_PROPERTY
			= "it.cavallium.rockserver.grpc.client.initial-retry-backoff";
	private static final String MAX_RETRY_BACKOFF_PROPERTY
			= "it.cavallium.rockserver.grpc.client.max-retry-backoff";
	private static final String RETRY_BACKOFF_MULTIPLIER_PROPERTY
			= "it.cavallium.rockserver.grpc.client.retry-backoff-multiplier";
	private static final int PUT_MULTI_LIST_MAX_ITEMS = 65_536;
	private static final long PUT_MULTI_LIST_MAX_LOGICAL_BYTES =
			it.cavallium.rockserver.core.common.RangeBudget.DEFAULT_MAX_BYTES;
	private static final it.cavallium.rockserver.core.common.api.proto.RequestContext[] NO_TIMEOUT_WIRE_CONTEXTS
			= createNoDeadlineWireContexts();
	public static final String MAX_INBOUND_MESSAGE_SIZE_PROPERTY
			= "it.cavallium.rockserver.grpc.client.max-inbound-message-size-bytes";
	public static final int DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 64 * 1024 * 1024;
	public static final int MIN_MAX_INBOUND_MESSAGE_SIZE = 4 * 1024 * 1024;
	/*
	 * A retry can run after the server completed an operation but before its
	 * response reached the client. Keep this allowlist to stateless unary reads:
	 * transaction/update/iterator creation, mutations, maintenance, CDC polling
	 * and every streaming method require caller-owned recovery instead.
	 *
	 * Generated descriptors bind the service-config names to the wire schema.
	 */
	private static final List<MethodDescriptor<?, ?>> AUTOMATIC_RETRY_METHOD_DESCRIPTORS = List.of(
			RocksDBServiceGrpc.getGetCapabilitiesMethod(),
			RocksDBServiceGrpc.getGetColumnIdMethod(),
			RocksDBServiceGrpc.getEstimateNumKeysMethod(),
			RocksDBServiceGrpc.getGetMethod(),
			RocksDBServiceGrpc.getExistsMethod(),
			RocksDBServiceGrpc.getExistsMultiMethod(),
			RocksDBServiceGrpc.getReduceRangeFirstAndLastMethod(),
			RocksDBServiceGrpc.getReduceRangeEntriesCountMethod(),
			RocksDBServiceGrpc.getGetRangePageMethod(),
			RocksDBServiceGrpc.getGetAllColumnDefinitionsMethod(),
			RocksDBServiceGrpc.getCheckMergeOperatorMethod(),
			RocksDBServiceGrpc.getCdcGetEarliestAvailableSequenceMethod(),
			RocksDBServiceGrpc.getCdcGetLastCommittedSequenceMethod());
	private static final List<Map<String, String>> AUTOMATIC_RETRY_METHOD_CONFIG_NAMES =
			AUTOMATIC_RETRY_METHOD_DESCRIPTORS.stream()
					.map(GrpcConnectionDelegate::serviceConfigName)
					.toList();
	final ManagedChannel channel;
	final ExecutorService callbackExecutor;
	final EventLoopGroup eventLoopGroup;
	private final RocksDBServiceFutureStub futureStub;
	private final ReactorRocksDBServiceGrpc.ReactorRocksDBServiceStub reactiveStub;
	private final URI address;
	private final int maxInboundMessageSize;
	private final RockserverCapabilities capabilities;

	private static it.cavallium.rockserver.core.common.api.proto.RequestContext[] createNoDeadlineWireContexts() {
		var contexts = new it.cavallium.rockserver.core.common.api.proto.RequestContext[WorkloadProfile.values().length];
		for (var profile : List.of(
				WorkloadProfile.ANALYTICAL,
				WorkloadProfile.INGEST,
				WorkloadProfile.BATCH)) {
			contexts[profile.ordinal()] = it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
					.setProfileValue(profile.wireValue())
					.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
					.setTimeoutNanos(RequestContext.NO_TIMEOUT)
					.build();
		}
		return contexts;
	}

	GrpcConnectionDelegate(String name, SocketAddress socketAddress, URI address) {
		super(name);
		int maxInboundMessageSize = configuredMaxInboundMessageSize();
		NettyChannelBuilder channelBuilder;
		if (socketAddress instanceof InetSocketAddress inetSocketAddress) {
			channelBuilder = NettyChannelBuilder
					.forAddress(inetSocketAddress.getHostString(), inetSocketAddress.getPort());
		} else {
			channelBuilder = NettyChannelBuilder
					.forAddress(socketAddress);
		}

		channelBuilder
				.usePlaintext()
				.maxInboundMessageSize(maxInboundMessageSize)
				.keepAliveTime(30, TimeUnit.SECONDS)
				.keepAliveTimeout(5, TimeUnit.SECONDS)
				.keepAliveWithoutCalls(true);
		configureRetry(channelBuilder);

		// Keep reactive-gRPC's bounded adapters off the transport loop: a backpressured
		// application callback must not prevent the same channel from making progress.
		var callbackExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
				.name("rockserver-grpc-" + name + "-callback-", 0)
				.factory());
		EventLoopGroup eventLoopGroup = null;
		ManagedChannel channel;
		try {
			channelBuilder.executor(callbackExecutor);
			if (socketAddress instanceof DomainSocketAddress _) {
				eventLoopGroup = new EpollEventLoopGroup(EVENT_LOOP_THREADS_PER_CONNECTION);
				channelBuilder
						.eventLoopGroup(eventLoopGroup)
						.channelType(EpollDomainSocketChannel.class);
			} else {
				eventLoopGroup = new NioEventLoopGroup(EVENT_LOOP_THREADS_PER_CONNECTION);
				channelBuilder
						.eventLoopGroup(eventLoopGroup)
						.channelType(NioSocketChannel.class);
			}
			channelBuilder.intercept(new RetryLoggingInterceptor());
			channel = channelBuilder.build();
		} catch (RuntimeException | Error ex) {
			callbackExecutor.shutdownNow();
			if (eventLoopGroup != null) {
				eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
			}
			throw ex;
		}
		var futureStub = RocksDBServiceGrpc.newFutureStub(channel);
		RockserverCapabilities capabilities;
		try {
			var response = futureStub
					.withDeadlineAfter(CAPABILITY_HANDSHAKE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
					.getCapabilities(CapabilitiesRequest.getDefaultInstance())
					.get(CAPABILITY_HANDSHAKE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			capabilities = new RockserverCapabilities(response.getWorkloadContractVersion());
			capabilities.requireCompatible();
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			closeFailedConstruction(channel, eventLoopGroup, callbackExecutor);
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR,
					"Interrupted during the Rockserver capability handshake", interrupted);
		} catch (ExecutionException | TimeoutException failure) {
			closeFailedConstruction(channel, eventLoopGroup, callbackExecutor);
			var cause = failure instanceof ExecutionException && failure.getCause() != null
					? failure.getCause()
					: failure;
			if (Status.fromThrowable(cause).getCode() == Code.UNIMPLEMENTED) {
				throw RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED,
						"The connected Rockserver does not expose the mandatory workload capability handshake", cause);
			}
			if (cause instanceof RuntimeException runtime) {
				throw runtime;
			}
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR,
					"Rockserver capability handshake failed", cause);
		} catch (RuntimeException | Error failure) {
			closeFailedConstruction(channel, eventLoopGroup, callbackExecutor);
			throw failure;
		}
		this.channel = channel;
		this.callbackExecutor = callbackExecutor;
		this.eventLoopGroup = eventLoopGroup;
		this.futureStub = futureStub;
		this.reactiveStub = ReactorRocksDBServiceGrpc.newReactorStub(channel);
		this.address = address;
		this.maxInboundMessageSize = maxInboundMessageSize;
		this.capabilities = capabilities;
	}

	@Override
	public RockserverCapabilities getCapabilities() {
		return capabilities;
	}

	private static void closeFailedConstruction(ManagedChannel channel,
			@Nullable EventLoopGroup eventLoopGroup,
			ExecutorService callbackExecutor) {
		channel.shutdownNow();
		if (eventLoopGroup != null) {
			eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
		}
		callbackExecutor.shutdownNow();
	}

	public static int configuredMaxInboundMessageSize() {
		var value = System.getProperty(MAX_INBOUND_MESSAGE_SIZE_PROPERTY);
		if (value == null || value.isBlank()) {
			return DEFAULT_MAX_INBOUND_MESSAGE_SIZE;
		}

		final long parsed;
		try {
			parsed = Long.parseLong(value);
		} catch (NumberFormatException ex) {
			throw new IllegalArgumentException("System property " + MAX_INBOUND_MESSAGE_SIZE_PROPERTY
					+ " must be an integer byte count, but was: " + value, ex);
		}
		if (parsed < MIN_MAX_INBOUND_MESSAGE_SIZE || parsed > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("System property " + MAX_INBOUND_MESSAGE_SIZE_PROPERTY
					+ " must be between " + MIN_MAX_INBOUND_MESSAGE_SIZE + " and " + Integer.MAX_VALUE
					+ " bytes, but was: " + value);
		}
		return (int) parsed;
	}

	private static void configureRetry(NettyChannelBuilder channelBuilder) {
		int maxAttempts = intProperty(MAX_RETRY_ATTEMPTS_PROPERTY, 5, 1);
		if (maxAttempts <= 1) {
			channelBuilder.disableRetry();
			return;
		}

		channelBuilder
				.enableRetry()
				.maxRetryAttempts(maxAttempts)
				.defaultServiceConfig(Map.of(
						"methodConfig", List.of(Map.of(
								"name", AUTOMATIC_RETRY_METHOD_CONFIG_NAMES,
								"retryPolicy", Map.of(
										"maxAttempts", (double) maxAttempts,
										"initialBackoff", System.getProperty(INITIAL_RETRY_BACKOFF_PROPERTY, "0.5s"),
										"maxBackoff", System.getProperty(MAX_RETRY_BACKOFF_PROPERTY, "30s"),
										"backoffMultiplier", doubleProperty(RETRY_BACKOFF_MULTIPLIER_PROPERTY, 2.0d, 1.0d),
										// Retry only transport loss here. SERVER_OVERLOADED and
										// UPDATE_RETRY are application backpressure/conflict signals;
										// callers must observe them immediately and apply domain-level
										// retry, cooldown, or transaction reconstruction.
										"retryableStatusCodes", AUTOMATIC_RETRYABLE_STATUS_CODE_NAMES
								)
						))
				));
	}

	private static Map<String, String> serviceConfigName(MethodDescriptor<?, ?> method) {
		if (method.getType() != MethodDescriptor.MethodType.UNARY) {
			throw new IllegalArgumentException("Automatic gRPC retry requires a unary method: "
					+ method.getFullMethodName());
		}
		String fullMethodName = method.getFullMethodName();
		int separator = fullMethodName.lastIndexOf('/');
		if (separator <= 0 || separator == fullMethodName.length() - 1) {
			throw new IllegalArgumentException("Invalid generated gRPC method name: " + fullMethodName);
		}
		return Map.of(
				"service", fullMethodName.substring(0, separator),
				"method", fullMethodName.substring(separator + 1));
	}

	private static List<MethodDescriptor<?, ?>> automaticRetryMethodDescriptors() {
		return AUTOMATIC_RETRY_METHOD_DESCRIPTORS;
	}

	private static Set<String> automaticRetryMethodFullNames() {
		return AUTOMATIC_RETRY_METHOD_DESCRIPTORS.stream()
				.map(MethodDescriptor::getFullMethodName)
				.collect(Collectors.toUnmodifiableSet());
	}

	private static int intProperty(String name, int defaultValue, int minValue) {
		var value = System.getProperty(name);
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		try {
			return Math.max(minValue, Integer.parseInt(value));
		} catch (NumberFormatException ex) {
			LOG.warn("Invalid integer value for system property {}: {}", name, value);
			return defaultValue;
		}
	}

	private static double doubleProperty(String name, double defaultValue, double minValue) {
		var value = System.getProperty(name);
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		try {
			return Math.max(minValue, Double.parseDouble(value));
		} catch (NumberFormatException ex) {
			LOG.warn("Invalid double value for system property {}: {}", name, value);
			return defaultValue;
		}
	}

	@Override
	public URI getUrl() {
		return address;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <R, RS, RA> RS requestSync(BoundRequestContext context, RocksDBAPICommand<R, RS, RA> req) {
		return withRequestContext(context, () -> (RS) switch (req) {
			case RocksDBAPICommand.RocksDBAPICommandSingle<?> _ -> {
				try {
					var asyncResponse = (CompletableFuture<R>) req.handleAsync(this);
					yield asyncResponse.join();
				} catch (CompletionException ex) {
					var cause = ex.getCause();
					if (cause instanceof RuntimeException exx) {
						throw exx;
					} else {
						throw ex;
					}
				}
			}
			case RocksDBAPICommand.RocksDBAPICommandStream<?> _ -> {
				var asyncResponse = (Publisher<R>) req.handleAsync(this);
				yield Flux.from(asyncResponse).toStream();
			}
		});
	}

	@Override
	public <R, RS, RA> RA requestAsync(BoundRequestContext context, RocksDBAPICommand<R, RS, RA> req) {
		return withRequestContext(context, () -> req.handleAsync(this));
	}

	@Override
	public CompletableFuture<Long> openTransactionAsync(Duration transactionLeaseTtl) throws RocksDBException {
		var request = OpenTransactionRequest.newBuilder()
				.setTransactionLeaseTtlNanos(LeaseTtl.toNanos(transactionLeaseTtl, "transactionLeaseTtl"))
				.setContext(currentWireRequestContext())
				.build();
		return toResponse(futureStubWithRequestDeadline().openTransaction(request), OpenTransactionResponse::getTransactionId);
	}

	@Override
	public CompletableFuture<Boolean> closeTransactionAsync(long transactionId, boolean commit) throws RocksDBException {
		var request = CloseTransactionRequest.newBuilder()
				.setTransactionId(transactionId)
				.setCommit(commit)
				.setContext(currentWireRequestContext(commit))
				.build();
		var stub = commit ? futureStubWithRequestDeadline() : futureStub;
		return toResponse(stub.closeTransaction(request), CloseTransactionResponse::getSuccessful);
	}

	@Override
	public CompletableFuture<Void> closeFailedUpdateAsync(long updateId) throws RocksDBException {
		var request = CloseFailedUpdateRequest.newBuilder()
				.setUpdateId(updateId)
				.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
				.build();
		return toResponse(this.futureStub.closeFailedUpdate(request), _ -> null);
	}

	@Override
	public CompletableFuture<Long> createColumnAsync(String name,
											 @NotNull ColumnSchema schema) throws RocksDBException {
		var requestBuilder = CreateColumnRequest.newBuilder()
				.setName(name)
				.setSchema(mapColumnSchema(schema))
				.setContext(currentWireRequestContext());
		var request = requestBuilder.build();
		return toResponse(futureStubWithRequestDeadline().createColumn(request), CreateColumnResponse::getColumnId);
	}

	@Override
	public CompletableFuture<Long> uploadMergeOperatorAsync(String name, String className, byte[] jarData) {
		return toResponse(futureStubWithRequestDeadline().uploadMergeOperator(UploadMergeOperatorRequest.newBuilder()
				.setOperatorName(name)
				.setClassName(className)
				.setJarPayload(ByteString.copyFrom(jarData))
				.setContext(currentWireRequestContext())
				.build()), UploadMergeOperatorResponse::getVersion);
	}

	@Override
	public CompletableFuture<Long> checkMergeOperatorAsync(String name, byte[] hash) {
		return toResponse(futureStubWithRequestDeadline().checkMergeOperator(CheckMergeOperatorRequest.newBuilder()
				.setOperatorName(name)
				.setHash(UnsafeByteOperations.unsafeWrap(hash))
				.setContext(currentWireRequestContext())
				.build()), resp -> resp.hasVersion() ? resp.getVersion() : null);
	}

	@Override
	public CompletableFuture<Void> deleteColumnAsync(long columnId) throws RocksDBException {
		var request = DeleteColumnRequest.newBuilder()
				.setColumnId(columnId)
				.setContext(currentWireRequestContext())
				.build();
		return toResponse(futureStubWithRequestDeadline().deleteColumn(request), _ -> null);
	}

	@Override
	public CompletableFuture<Boolean> deleteColumnIfExistsAsync(@NotNull String name) throws RocksDBException {
		var request = DeleteColumnIfExistsRequest.newBuilder()
				.setName(name)
				.setContext(currentWireRequestContext())
				.build();
		return toResponse(futureStubWithRequestDeadline().deleteColumnIfExists(request), DeleteColumnIfExistsResponse::getDeleted);
	}

	@Override
	public CompletableFuture<Long> getColumnIdAsync(@NotNull String name) throws RocksDBException {
		var request = GetColumnIdRequest.newBuilder()
				.setName(name)
				.setContext(currentWireRequestContext())
				.build();
		return toResponse(futureStubWithRequestDeadline().getColumnId(request), GetColumnIdResponse::getColumnId);
	}

	@Override
	public CompletableFuture<Long> estimateNumKeysAsync(long columnId) throws RocksDBException {
		var request = EstimateNumKeysRequest.newBuilder()
				.setColumnId(columnId)
				.setContext(currentWireRequestContext())
				.build();
		return toResponse(futureStubWithRequestDeadline().estimateNumKeys(request), EntriesCount::getCount);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> CompletableFuture<T> putAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		var request = PutRequest.newBuilder()
				.setTransactionOrUpdateId(transactionOrUpdateId)
				.setColumnId(columnId)
				.setData(mapKV(keys, value))
				.setContext(currentWireRequestContext())
				.build();
		if (requestType == null) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Null request type");
		}
		return (CompletableFuture<T>) switch (requestType) {
			case RequestNothing<?> _ -> toResponse(futureStubWithRequestDeadline().put(request), _ -> null);
			case RequestType.RequestEnsure<?> _ ->
					toResponse(futureStubWithRequestDeadline().putEnsure(request), _ -> null);
			case RequestPrevious<?> _ ->
					toResponse(futureStubWithRequestDeadline().putGetPrevious(request), GrpcConnectionDelegate::mapPrevious);
			case RequestDelta<?> _ ->
					toResponse(futureStubWithRequestDeadline().putGetDelta(request), GrpcConnectionDelegate::mapDelta);
			case RequestChanged<?> _ ->
					toResponse(futureStubWithRequestDeadline().putGetChanged(request), Changed::getChanged);
			case RequestType.RequestPreviousPresence<?> _ ->
					toResponse(futureStubWithRequestDeadline().putGetPreviousPresence(request), PreviousPresence::getPresent);
		};
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> CompletableFuture<T> deleteAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		if (requestType == null) {
			throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "requestType");
		}
		var requestBuilder = DeleteRequest.newBuilder()
				.setTransactionOrUpdateId(transactionOrUpdateId)
				.setColumnId(columnId)
				.setContext(currentWireRequestContext());
		for (var key : keys.keys()) {
			requestBuilder.addKeys(mapKey(key));
		}
		var request = requestBuilder.build();
		return (CompletableFuture<T>) switch (requestType) {
			case RequestNothing<?> _ -> toResponse(futureStubWithRequestDeadline().delete(request), _ -> null);
			case RequestPrevious<?> _ ->
					toResponse(futureStubWithRequestDeadline().deleteGetPrevious(request), GrpcConnectionDelegate::mapPrevious);
			case RequestPreviousPresence<?> _ ->
					toResponse(futureStubWithRequestDeadline().deleteGetPreviousPresence(request), PreviousPresence::getPresent);
		};
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> CompletableFuture<T> mergeAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		var request = MergeRequest.newBuilder()
				.setTransactionOrUpdateId(transactionOrUpdateId)
				.setColumnId(columnId)
				.setData(mapKV(keys, value))
				.setContext(currentWireRequestContext())
				.build();
		if (requestType == null) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Null request type");
		}
		return (CompletableFuture<T>) switch (requestType) {
			case RequestNothing<?> _ -> toResponse(futureStubWithRequestDeadline().merge(request), _ -> null);
			case RequestType.RequestMerged<?> _ ->
					toResponse(futureStubWithRequestDeadline().mergeGetMerged(request), x ->
							x.hasMerged() ? mapByteString(x.getMerged()) : null);
		};
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> CompletableFuture<List<T>> putMultiAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull List<@NotNull Keys> allKeys,
			@NotNull List<@NotNull Buf> allValues,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		var count = allKeys.size();
		if (count != allValues.size()) {
			throw new IllegalArgumentException("Keys length is different than values length! "
					+ count + " != " + allValues.size());
		}

		var initialRequest = PutMultiInitialRequest.newBuilder()
				.setTransactionOrUpdateId(transactionOrUpdateId)
				.setColumnId(columnId)
				.setContext(currentWireRequestContext())
				.build();

		if ((requestType instanceof RequestNothing<?> || requestType instanceof RequestType.RequestEnsure<?>)
				&& fitsPutMultiListBudget(allKeys, allValues)) {
			var listRequest = PutMultiListRequest.newBuilder()
					.setInitialRequest(initialRequest);
			for (int index = 0; index < count; index++) {
				listRequest.addData(mapKV(allKeys.get(index), allValues.get(index)));
			}
			var request = listRequest.build();
			return (CompletableFuture<List<T>>) (switch (requestType) {
				case RequestNothing<?> _ -> toResponse(
						futureStubWithRequestDeadline().putMultiList(request), _ -> List.<T>of());
				case RequestType.RequestEnsure<?> _ -> toResponse(
						futureStubWithRequestDeadline().putMultiListEnsure(request), _ -> List.<T>of());
				default -> throw new IllegalStateException("Unexpected unary put-multi request type");
			});
		}

		var streamingInitialRequest = PutMultiRequest.newBuilder()
				.setInitialRequest(initialRequest)
				.build();

		Mono<PutMultiRequest> initialRequestMono = Mono.just(streamingInitialRequest);
		Flux<PutMultiRequest> dataRequestsFlux = Flux.fromIterable(() -> GrpcConnectionDelegate
				.map(allKeys.iterator(), allValues.iterator(), (keys, value) -> PutMultiRequest.newBuilder()
						.setData(mapKV(keys, value))
						.build()));
		var inputRequests = initialRequestMono.concatWith(dataRequestsFlux);

		return (CompletableFuture<List<T>>) (switch (requestType) {
			case RequestNothing<?> _ ->
					toResponse(reactiveStubWithRequestDeadline().putMulti(inputRequests)
						.ignoreElement()
						.toFuture())
						.thenApply(_ -> List.of());
			case RequestType.RequestEnsure<?> _ ->
					toResponse(reactiveStubWithRequestDeadline().putMultiEnsure(inputRequests)
							.ignoreElement()
							.toFuture())
							.thenApply(_ -> List.of());
			case RequestPrevious<?> _ ->
					toResponse(reactiveStubWithRequestDeadline().putMultiGetPrevious(inputRequests)
						.collect(() -> new ArrayList<@Nullable Buf>(),
								(list, value) -> list.add(GrpcConnectionDelegate.mapPrevious(value)))
						.toFuture());
			case RequestDelta<?> _ ->
					toResponse(reactiveStubWithRequestDeadline().putMultiGetDelta(inputRequests)
						.map(GrpcConnectionDelegate::mapDelta)
						.collectList()
						.toFuture());
			case RequestChanged<?> _ ->
					toResponse(reactiveStubWithRequestDeadline().putMultiGetChanged(inputRequests)
						.map(Changed::getChanged)
						.collectList()
						.toFuture());
			case RequestPreviousPresence<?> _ ->
					toResponse(reactiveStubWithRequestDeadline().putMultiGetPreviousPresence(inputRequests)
						.map(PreviousPresence::getPresent)
						.collectList()
						.toFuture());
		});
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> CompletableFuture<List<T>> deleteMultiAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> allKeys,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		if (requestType == null) {
			throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "requestType");
		}

		var initialRequest = DeleteMultiRequest.newBuilder()
				.setInitialRequest(DeleteMultiInitialRequest.newBuilder()
							.setTransactionOrUpdateId(transactionOrUpdateId)
							.setColumnId(columnId)
							.setContext(currentWireRequestContext())
						.build())
				.build();

		Mono<DeleteMultiRequest> initialRequestMono = Mono.just(initialRequest);
		Flux<DeleteMultiRequest> dataRequestsFlux = Flux.fromIterable(allKeys)
				.map(keys -> {
					var data = DeleteRequest.newBuilder();
					for (var key : keys.keys()) {
						data.addKeys(mapKey(key));
					}
					return DeleteMultiRequest.newBuilder()
							.setData(data)
							.build();
				});
		var inputRequests = initialRequestMono.concatWith(dataRequestsFlux);

		return (CompletableFuture<List<T>>) (switch (requestType) {
			case RequestNothing<?> _ ->
					toResponse(reactiveStubWithRequestDeadline().deleteMulti(inputRequests)
							.ignoreElement()
							.toFuture())
							.thenApply(_ -> List.of());
			case RequestPrevious<?> _ ->
					toResponse(reactiveStubWithRequestDeadline().deleteMultiGetPrevious(inputRequests)
							.collect(() -> new ArrayList<@Nullable Buf>(),
									(list, value) -> list.add(GrpcConnectionDelegate.mapPrevious(value)))
							.toFuture());
			case RequestPreviousPresence<?> _ ->
					toResponse(reactiveStubWithRequestDeadline().deleteMultiGetPreviousPresence(inputRequests)
							.map(PreviousPresence::getPresent)
							.collectList()
							.toFuture());
		});
	}

	@Override
	public CompletableFuture<Void> deleteRangeAsync(long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive) throws RocksDBException {
		var requestBuilder = DeleteRangeRequest.newBuilder()
				.setColumnId(columnId)
				.setContext(currentWireRequestContext());
		if (startKeysInclusive != null) {
			for (var key : startKeysInclusive.keys()) {
				requestBuilder.addStartKeysInclusive(mapKey(key));
			}
		}
		if (endKeysExclusive != null) {
			for (var key : endKeysExclusive.keys()) {
				requestBuilder.addEndKeysExclusive(mapKey(key));
			}
		}
		var request = requestBuilder.build();
		return toResponse(futureStubWithRequestDeadline().deleteRange(request), _ -> null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> CompletableFuture<List<T>> mergeMultiAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull List<@NotNull Keys> allKeys,
			@NotNull List<@NotNull Buf> allValues,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		var count = allKeys.size();
		if (count != allValues.size()) {
			throw new IllegalArgumentException("Keys length is different than values length! "
					+ count + " != " + allValues.size());
		}

		var initialRequest = MergeMultiRequest.newBuilder()
				.setInitialRequest(MergeMultiInitialRequest.newBuilder()
							.setTransactionOrUpdateId(transactionOrUpdateId)
							.setColumnId(columnId)
							.setContext(currentWireRequestContext())
						.build())
				.build();

		Mono<MergeMultiRequest> initialRequestMono = Mono.just(initialRequest);
		Flux<MergeMultiRequest> dataRequestsFlux = Flux.fromIterable(() -> GrpcConnectionDelegate
				.map(allKeys.iterator(), allValues.iterator(), (keys, value) -> MergeMultiRequest.newBuilder()
						.setData(mapKV(keys, value))
						.build()));
		var inputRequests = initialRequestMono.concatWith(dataRequestsFlux);

		return (CompletableFuture<List<T>>) (switch (requestType) {
			case RequestNothing<?> _ ->
					toResponse(reactiveStubWithRequestDeadline().mergeMulti(inputRequests)
						.ignoreElement()
						.toFuture())
						.thenApply(_ -> List.of());
			case RequestType.RequestMerged<?> _ ->
					toResponse(reactiveStubWithRequestDeadline().mergeMultiGetMerged(inputRequests)
						.map(v -> v.hasMerged() ? mapByteString(v.getMerged()) : null)
						.collectList()
						.toFuture());
		});
	}

	@Override
	public Publisher<SerializedKVBatch> scanRawAsync(long columnId, int shardIndex, int shardCount) {
		return toResponse(reactiveStubWithRequestDeadline().scanRaw(ScanRawRequest.newBuilder()
						.setColumnId(columnId)
						.setShardIndex(shardIndex)
						.setShardCount(shardCount)
						.setContext(currentWireRequestContext())
						.build()))
				.map(batch -> new SerializedKVBatchRef(Buf.wrap(batch.getSerialized().toByteArray())));
	}

	@Override
	public Publisher<RawScanEvent> scanRawResumableAsync(long columnId,
			int shardIndex,
			int shardCount,
			Set<RawSstToken> completedSsts) {
		var request = ScanRawRequest.newBuilder()
				.setColumnId(columnId)
				.setShardIndex(shardIndex)
				.setShardCount(shardCount)
				.setContext(currentWireRequestContext())
				.setResumable(true);
		for (RawSstToken completedSst : completedSsts) {
			request.addCompletedSstTokens(completedSst.value());
		}
		return toResponse(reactiveStubWithRequestDeadline().scanRaw(request.build()))
				.map(event -> switch (event.getEventCase()) {
					case SERIALIZED -> new RawScanEvent.Batch(
							Buf.wrap(event.getSerialized().toByteArray()),
							event.hasCompletedSstTokenAfterBatch()
									? new RawSstToken(event.getCompletedSstTokenAfterBatch())
									: null);
					case COMPLETEDSSTTOKEN -> {
						if (event.hasCompletedSstTokenAfterBatch()) {
							throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR,
									"Rockserver returned two completion tokens in one raw-scan event");
						}
						yield new RawScanEvent.SstCompleted(new RawSstToken(event.getCompletedSstToken()));
					}
					case EVENT_NOT_SET -> throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR,
							"Rockserver returned an empty resumable raw-scan event");
				});
	}

	@Override
	public CompletableFuture<Void> putBatchAsync(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull PutBatchMode mode) throws RocksDBException {
		var initialRequest = Mono.just(PutBatchRequest.newBuilder()
				.setInitialRequest(PutBatchInitialRequest.newBuilder()
						.setColumnId(columnId)
							.setMode(switch (mode) {
							case WRITE_BATCH -> it.cavallium.rockserver.core.common.api.proto.PutBatchMode.WRITE_BATCH;
							case WRITE_BATCH_NO_WAL -> it.cavallium.rockserver.core.common.api.proto.PutBatchMode.WRITE_BATCH_NO_WAL;
							case SST_INGESTION -> it.cavallium.rockserver.core.common.api.proto.PutBatchMode.SST_INGESTION;
							case SST_INGEST_BEHIND -> it.cavallium.rockserver.core.common.api.proto.PutBatchMode.SST_INGEST_BEHIND;
							})
							.setContext(currentWireRequestContext())
						.build())
				.build());
		var nextRequests = Flux.from(batchPublisher).map(batch -> {
			var request = PutBatchRequest.newBuilder();
			request.setData(mapKVBatch(batch));
			return request.build();
		});
		var inputFlux = initialRequest.concatWith(nextRequests);
		return toResponse(reactiveStubWithRequestDeadline().putBatch(inputFlux).then().toFuture());
	}

	@Override
	public CompletableFuture<Void> mergeBatchAsync(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull it.cavallium.rockserver.core.common.MergeBatchMode mode) throws RocksDBException {
		var initialRequest = Mono.just(MergeBatchRequest.newBuilder()
				.setInitialRequest(MergeBatchInitialRequest.newBuilder()
						.setColumnId(columnId)
							.setMode(switch (mode) {
							case MERGE_WRITE_BATCH -> it.cavallium.rockserver.core.common.api.proto.MergeBatchMode.MERGE_WRITE_BATCH;
							case MERGE_WRITE_BATCH_NO_WAL -> it.cavallium.rockserver.core.common.api.proto.MergeBatchMode.MERGE_WRITE_BATCH_NO_WAL;
							case MERGE_SST_INGESTION -> it.cavallium.rockserver.core.common.api.proto.MergeBatchMode.MERGE_SST_INGESTION;
							case MERGE_SST_INGEST_BEHIND -> it.cavallium.rockserver.core.common.api.proto.MergeBatchMode.MERGE_SST_INGEST_BEHIND;
							})
							.setContext(currentWireRequestContext())
						.build())
				.build());
		var nextRequests = Flux.from(batchPublisher).map(batch -> {
			var request = MergeBatchRequest.newBuilder();
			request.setData(mapKVBatch(batch));
			return request.build();
		});
		var inputFlux = initialRequest.concatWith(nextRequests);
		return toResponse(reactiveStubWithRequestDeadline().mergeBatch(inputFlux).then().toFuture());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> CompletableFuture<T> getAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			RequestGet<? super Buf, T> requestType) throws RocksDBException {
		if (requestType == null) {
			throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "requestType");
		}
		var requestBuilder = GetRequest.newBuilder()
				.setTransactionOrUpdateId(transactionOrUpdateId)
				.setColumnId(columnId)
				.setContext(currentWireRequestContext());
		for (var key : keys.keys()) {
			requestBuilder.addKeys(mapKey(key));
		}
		var request = requestBuilder.build();
		if (requestType instanceof RequestType.RequestForUpdate<?>) {
			return toResponse(futureStubWithRequestDeadline().getForUpdate(request), x -> (T) new UpdateContext<>(
					x.hasPrevious() ? mapByteString(x.getPrevious()) : null,
					x.getUpdateId()
			));
		} else if (requestType instanceof RequestType.RequestExists<?>) {
			return toResponse(futureStubWithRequestDeadline().exists(request), x -> (T) (Boolean) x.getPresent());
		} else if (requestType instanceof RequestNothing<?>) {
			return toResponse(futureStubWithRequestDeadline().get(request), GrpcConnectionDelegate::ignoreGetResponse);
		} else if (requestType instanceof RequestType.RequestCurrent<?>) {
			return toResponse(futureStubWithRequestDeadline().get(request), GrpcConnectionDelegate::mapCurrentGetResponse);
		} else {
			throw new IllegalStateException("Unsupported get request type " + requestType);
		}
	}

	private static <T> T ignoreGetResponse(GetResponse ignored) {
		return null;
	}

	@SuppressWarnings("unchecked")
	private static <T> T mapCurrentGetResponse(GetResponse response) {
		return response.hasValue() ? (T) mapByteString(response.getValue()) : null;
	}

	@Override
	public CompletableFuture<List<Boolean>> existsMultiAsync(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys) throws RocksDBException {
		var deadlineStub = futureStubWithRequestDeadline();
		var request = ExistsMultiRequest.newBuilder()
				.setTransactionId(transactionId)
				.setColumnId(columnId)
				.setContext(currentWireRequestContext());
		for (var logicalKeys : keys) {
			var wireKeys = KeyTuple.newBuilder();
			for (var key : logicalKeys.keys()) {
				wireKeys.addKeys(mapKey(key));
			}
			request.addKeysMulti(wireKeys);
		}
		return toResponse(deadlineStub.existsMulti(request.build()),
				response -> List.copyOf(response.getPresentList()),
				GrpcConnectionDelegate::mapReadDeadlineError);
	}

	@Override
	public CompletableFuture<Long> openIteratorAsync(long transactionId,
			long columnId,
			@NotNull Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			Duration iteratorLeaseTtl) throws RocksDBException {
		var requestBuilder = OpenIteratorRequest.newBuilder()
				.setTransactionId(transactionId)
				.setColumnId(columnId)
				.setReverse(reverse)
				.setIteratorLeaseTtlNanos(LeaseTtl.toNanos(iteratorLeaseTtl, "iteratorLeaseTtl"))
				.setContext(currentWireRequestContext());
		if (startKeysInclusive != null) {
			for (var key : startKeysInclusive.keys()) {
				requestBuilder.addStartKeysInclusive(mapKey(key));
			}
		}
		if (endKeysExclusive != null) {
			for (var key : endKeysExclusive.keys()) {
				requestBuilder.addEndKeysExclusive(mapKey(key));
			}
		}
		var request = requestBuilder.build();
		return toResponse(futureStubWithRequestDeadline().openIterator(request), OpenIteratorResponse::getIteratorId);
	}

	@Override
	public CompletableFuture<Void> closeIteratorAsync(long iteratorId) throws RocksDBException {
		var request = CloseIteratorRequest.newBuilder()
				.setIteratorId(iteratorId)
				.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
				.build();
		return toResponse(this.futureStub.closeIterator(request), _ -> null);
	}

	@Override
	public CompletableFuture<Void> seekToAsync(long iterationId, @NotNull Keys keys) throws RocksDBException {
		var requestBuilder = SeekToRequest.newBuilder()
				.setIterationId(iterationId)
				.setContext(currentWireRequestContext());
		for (var key : keys.keys()) {
			requestBuilder.addKeys(mapKey(key));
		}
		var request = requestBuilder.build();
		return toResponse(futureStubWithRequestDeadline().seekTo(request), _ -> null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> CompletableFuture<T> subsequentAsync(long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super Buf, T> requestType) throws RocksDBException {
		var request = SubsequentRequest.newBuilder()
				.setIterationId(iterationId)
				.setSkipCount(skipCount)
				.setTakeCount(takeCount)
				.setContext(currentWireRequestContext())
				.build();
		return switch (requestType) {
			case RequestNothing<?> _ -> toResponse(futureStubWithRequestDeadline().subsequent(request), _ -> null);
			case RequestExists<?> _ ->
					(CompletableFuture<T>) toResponse(futureStubWithRequestDeadline().subsequentExists(request), PreviousPresence::getPresent);
			case RequestMulti<?> _ ->
					(CompletableFuture<T>) toResponse(reactiveStubWithRequestDeadline().subsequentMultiGet(request)
							.map(kv -> mapByteString(kv.getValue()))
							.collectList()
							.toFuture());
		};
	}

	private static boolean fitsPutMultiListBudget(List<Keys> allKeys, List<Buf> allValues) {
		if (allKeys.size() > PUT_MULTI_LIST_MAX_ITEMS) {
			return false;
		}
		long bytes = 0L;
		for (int index = 0; index < allKeys.size(); index++) {
			bytes += allValues.get(index).size();
			if (bytes > PUT_MULTI_LIST_MAX_LOGICAL_BYTES) {
				return false;
			}
			for (var key : allKeys.get(index).keys()) {
				bytes += key.size();
				if (bytes > PUT_MULTI_LIST_MAX_LOGICAL_BYTES) {
					return false;
				}
			}
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> CompletableFuture<T> reduceRangeAsync(long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.
		RequestReduceRange<? super it.cavallium.rockserver.core.common.KV, T> requestType) throws RocksDBException {
		var deadlineStub = futureStubWithRequestDeadline();
		var requestBuilder = GetRangeRequest.newBuilder()
				.setTransactionId(transactionId)
				.setColumnId(columnId)
				.setReverse(reverse)
				.setContext(currentWireRequestContext());
		if (startKeysInclusive != null) {
			for (var key : startKeysInclusive.keys()) {
				requestBuilder.addStartKeysInclusive(mapKey(key));
			}
		}
		if (endKeysExclusive != null) {
			for (var key : endKeysExclusive.keys()) {
				requestBuilder.addEndKeysExclusive(mapKey(key));
			}
		}
		var request = requestBuilder.build();
		return (CompletableFuture<T>) switch (requestType) {
			case RequestType.RequestGetFirstAndLast<?> _ ->
					toResponse(deadlineStub.reduceRangeFirstAndLast(request), result -> new FirstAndLast<>(
							result.hasFirst() ? mapKV(result.getFirst()) : null,
							result.hasLast() ? mapKV(result.getLast()) : null
					), GrpcConnectionDelegate::mapReadDeadlineError);
			case RequestType.RequestEntriesCount<?> _ ->
					toResponse(deadlineStub.reduceRangeEntriesCount(request),
							EntriesCount::getCount,
							GrpcConnectionDelegate::mapReadDeadlineError);
			default -> throw new UnsupportedOperationException();
		};
	}

	private RocksDBServiceFutureStub futureStubWithRequestDeadline() {
		long remainingNanos = remainingRequestDeadlineNanos();
		return remainingNanos == RequestContext.NO_TIMEOUT
				? futureStub
				: futureStub.withDeadlineAfter(remainingNanos, TimeUnit.NANOSECONDS);
	}

	private ReactorRocksDBServiceGrpc.ReactorRocksDBServiceStub reactiveStubWithRequestDeadline() {
		long remainingNanos = remainingRequestDeadlineNanos();
		return remainingNanos == RequestContext.NO_TIMEOUT
				? reactiveStub
				: reactiveStub.withDeadlineAfter(remainingNanos, TimeUnit.NANOSECONDS);
	}

	private long remainingRequestDeadlineNanos() {
		long remainingNanos = currentBoundRequestContext().remainingNanos();
		if (remainingNanos == 0L) {
			throw RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
					"Request deadline already expired");
		}
		return remainingNanos;
	}

	@Override
	public <T> CompletableFuture<it.cavallium.rockserver.core.common.RangePage<T>> getRangePageAsync(
			long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@Nullable Keys resumeAfter,
			@NotNull RequestType.RequestGetRange<? super it.cavallium.rockserver.core.common.KV, T> requestType,
			@NotNull it.cavallium.rockserver.core.common.RangeBudget budget) throws RocksDBException {
		var requestBuilder = GetRangePageRequest.newBuilder()
				.setTransactionId(transactionId)
				.setColumnId(columnId)
				.setReverse(reverse)
				.setRequestTypeValue(switch (requestType) {
					case RequestType.RequestGetAllInRange<?> _ -> 1;
					case RequestType.RequestGetAllInRangeNoCache<?> _ -> 2;
				})
				.setBudget(it.cavallium.rockserver.core.common.api.proto.RangeBudget.newBuilder()
						.setMaxItems(budget.maxItems())
						.setMaxBytes(budget.maxBytes()))
				.setContext(currentWireRequestContext());
		if (startKeysInclusive != null) {
			for (var key : startKeysInclusive.keys()) {
				requestBuilder.addStartKeysInclusive(mapKey(key));
			}
		}
		if (endKeysExclusive != null) {
			for (var key : endKeysExclusive.keys()) {
				requestBuilder.addEndKeysExclusive(mapKey(key));
			}
		}
		if (resumeAfter != null) {
			var resumeAfterBuilder = RangeKey.newBuilder();
			for (var key : resumeAfter.keys()) {
				resumeAfterBuilder.addKeys(mapKey(key));
			}
			requestBuilder.setResumeAfter(resumeAfterBuilder);
		}
		return toResponse(futureStubWithRequestDeadline().getRangePage(requestBuilder.build()), response -> {
			@SuppressWarnings("unchecked")
			var items = (List<T>) (List<?>) response.getItemsList().stream()
					.map(GrpcConnectionDelegate::mapKV)
					.toList();
			var mappedResumeAfter = response.hasResumeAfter()
					? mapKeys(response.getResumeAfter().getKeysList())
					: null;
			return new it.cavallium.rockserver.core.common.RangePage<>(
					items, mappedResumeAfter, response.getHasMore());
		}, GrpcConnectionDelegate::mapReadDeadlineError);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Publisher<T> getRangeAsync(long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.
		RequestGetRange<? super it.cavallium.rockserver.core.common.KV, T> requestType) throws RocksDBException {
		var deadlineStub = reactiveStubWithRequestDeadline();
		var requestBuilder = GetRangeRequest.newBuilder()
				.setTransactionId(transactionId)
				.setColumnId(columnId)
				.setReverse(reverse)
				.setContext(currentWireRequestContext());
		if (startKeysInclusive != null) {
			for (var key : startKeysInclusive.keys()) {
				requestBuilder.addStartKeysInclusive(mapKey(key));
			}
		}
		if (endKeysExclusive != null) {
			for (var key : endKeysExclusive.keys()) {
				requestBuilder.addEndKeysExclusive(mapKey(key));
			}
		}
		var request = requestBuilder.build();
		return (Publisher<T>) switch (requestType) {
			case RequestType.RequestGetAllInRange<?> _ -> toReadResponse(deadlineStub.getAllInRange(request)
					.map(kv -> mapKV(kv)));
			case RequestType.RequestGetAllInRangeNoCache<?> _ -> toReadResponse(deadlineStub.getAllInRangeNoCache(request)
					.map(GrpcConnectionDelegate::mapKV));
		};
	}

    // ============ CDC Async API ============

	@Override
	public CompletableFuture<Long> cdcCreateAsync(@NotNull String id,
			@Nullable Long fromSeq,
			@Nullable List<Long> columnIds,
			@Nullable Boolean resolvedValues,
			@NotNull OptionalLong expectedLastCommitted) throws RocksDBException {
		var builder = CdcCreateRequest.newBuilder()
				.setId(id)
				.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION);
        if (fromSeq != null) builder.setFromSeq(fromSeq);
        if (columnIds != null) builder.addAllColumnIds(columnIds);
        if (resolvedValues != null) builder.setResolvedValues(resolvedValues);
		if (expectedLastCommitted.isPresent()) {
			builder.setExpectedLastCommittedSeq(expectedLastCommitted.getAsLong());
		} else {
			builder.setExpectAbsent(Empty.getDefaultInstance());
		}
        return toResponse(futureStub.cdcCreate(builder.build()), CdcCreateResponse::getStartSeq);
    }

    @Override
    public CompletableFuture<Void> cdcDeleteAsync(@NotNull String id) throws RocksDBException {
		return toResponse(futureStub.cdcDelete(CdcDeleteRequest.newBuilder()
				.setId(id)
				.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
				.build()), _ -> null);
    }

	@Override
	public CompletableFuture<Long> cdcGetEarliestAvailableSequenceAsync() throws RocksDBException {
		return toResponse(futureStub.cdcGetEarliestAvailableSequence(
				CdcGetEarliestAvailableSequenceRequest.newBuilder()
						.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
						.build()),
				CdcGetEarliestAvailableSequenceResponse::getSequence);
	}

    @Override
    public CompletableFuture<OptionalLong> cdcGetLastCommittedSequenceAsync(@NotNull String id) throws RocksDBException {
		var request = CdcGetLastCommittedSequenceRequest.newBuilder()
				.setId(id)
				.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
				.build();
        return toResponse(futureStub.cdcGetLastCommittedSequence(request), response ->
                response.hasLastCommittedSeq()
                        ? OptionalLong.of(response.getLastCommittedSeq())
                        : OptionalLong.empty());
    }

    @Override
    public CompletableFuture<Void> cdcCommitAsync(@NotNull String id, long seq) throws RocksDBException {
		return toResponse(futureStub.cdcCommit(CdcCommitRequest.newBuilder()
				.setId(id).setSeq(seq)
				.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
				.build()), _ -> null);
    }

    @Override
    public Publisher<CDCEvent> cdcPollAsync(@NotNull String id, @Nullable Long fromSeq, long maxEvents) throws RocksDBException {
		var builder = CdcPollRequest.newBuilder()
				.setId(id)
				.setMaxEvents(maxEvents)
				.setMaxResponseBytes(maxInboundMessageSize)
				.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION);
        if (fromSeq != null) builder.setFromSeq(fromSeq);
        return toResponse(reactiveStub.cdcPoll(builder.build()))
                .map(GrpcConnectionDelegate::mapCDCEvent);
    }

	@Override
	Mono<CdcBatch> cdcPollBatchAsync(BoundRequestContext context,
			@NotNull String id,
			@Nullable Long fromSeq,
			long maxEvents) {
		return withRequestContext(context, () -> cdcPollBatchAsync(id, fromSeq, maxEvents));
	}

    @Override
    public Mono<CdcBatch> cdcPollBatchAsync(@NotNull String id, @Nullable Long fromSeq, long maxEvents) {
		var builder = CdcPollRequest.newBuilder()
				.setId(id)
				.setMaxEvents(maxEvents)
				.setMaxResponseBytes(maxInboundMessageSize)
				.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION);
        if (fromSeq != null) builder.setFromSeq(fromSeq);
		return toResponse(reactiveStub.cdcPollBatch(builder.build()))
				.map(response -> new CdcBatch(
						response.getEventsList().stream().map(GrpcConnectionDelegate::mapCDCEvent).toList(),
						response.getNextSeq()));
    }

    private static CDCEvent mapCDCEvent(it.cavallium.rockserver.core.common.api.proto.CDCEvent ev) {
        var op = switch (ev.getOp()) {
            case PUT -> CDCEvent.Op.PUT;
            case DELETE -> CDCEvent.Op.DELETE;
            case MERGE -> CDCEvent.Op.MERGE;
            case UNRECOGNIZED -> CDCEvent.Op.PUT;
        };
        return new CDCEvent(ev.getSeq(), ev.getColumnId(), mapByteString(ev.getKey()), mapByteString(ev.getValue()), op);
    }

	@Override
	public CompletableFuture<Void> flushAsync() {
		var request = FlushRequest.newBuilder()
				.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
				.build();
		return toResponse(this.futureStub.flush(request), _ -> null);
	}

	@Override
	public CompletableFuture<Void> compactAsync() {
		var request = CompactRequest.newBuilder()
				.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
				.build();
		return toResponse(this.futureStub.compact(request), _ -> null);
	}

	@Override
	public CompletableFuture<Map<String, ColumnSchema>> getAllColumnDefinitionsAsync() {
		var request = GetAllColumnDefinitionsRequest.newBuilder()
				.setContext(currentWireRequestContext())
				.build();
		return toResponse(futureStubWithRequestDeadline().getAllColumnDefinitions(request),
				response -> response.getColumnsList().stream()
						.collect(Collectors.toMap(Column::getName, col -> unmapColumnSchema(col.getSchema()))));
	}

	private static it.cavallium.rockserver.core.common.Delta<Buf> mapDelta(Delta x) {
		return new it.cavallium.rockserver.core.common.Delta<>(
				x.hasPrevious() ? mapByteString(x.getPrevious()) : null,
				x.hasCurrent() ? mapByteString(x.getCurrent()) : null
		);
	}

	public static <A, B, C> Iterator<C> map(Iterator<A> a, Iterator<B> b, BiFunction<A, B, C> f) {
		return new Iterator<>() {
			public boolean hasNext() {
				return a.hasNext() && b.hasNext(); // This uses the shorter of the two `Iterator`s.
			}

			public C next() {
				return f.apply(a.next(), b.next());
			}
		};
	}

	@Nullable
	private static Buf mapPrevious(Previous x) {
		return x.hasPrevious() ? mapByteString(x.getPrevious()) : null;
	}

	private static Buf mapByteString(ByteString data) {
		return Utils.toBuf(data);
	}

	private static it.cavallium.rockserver.core.common.api.proto.KVBatch mapKVBatch(@NotNull KVBatch kvBatch) {
		var result = it.cavallium.rockserver.core.common.api.proto.KVBatch.newBuilder();
		var keys = kvBatch.keys().iterator();
		var values = kvBatch.values().iterator();
		while (keys.hasNext()) {
			result.addEntries(mapKV(keys.next(), values.next()));
		}
		return result.build();
	}

	private it.cavallium.rockserver.core.common.api.proto.RequestContext currentWireRequestContext() {
		return currentWireRequestContext(true);
	}

	private it.cavallium.rockserver.core.common.api.proto.RequestContext currentWireRequestContext(
			boolean enforceCallerDeadline) {
		var boundContext = currentBoundRequestContext();
		var context = boundContext.value();
		long remainingNanos = enforceCallerDeadline
				? boundContext.remainingNanos()
				: RequestContext.NO_TIMEOUT;
		if (remainingNanos == 0L) {
			throw RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
					"Request deadline already expired");
		}
		if (remainingNanos == RequestContext.NO_TIMEOUT) {
			var cached = NO_TIMEOUT_WIRE_CONTEXTS[context.profile().ordinal()];
			if (cached != null) {
				return cached;
			}
		}
		return it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
				.setProfileValue(context.profile().wireValue())
				.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
				.setTimeoutNanos(remainingNanos)
				.build();
	}

	private static KV mapKV(@NotNull Keys keys, @NotNull Buf value) {
		var result = KV.newBuilder();
		for (var key : keys.keys()) {
			result.addKeys(mapKey(key));
		}
		return result.setValue(mapValue(value)).build();
	}

	private static it.cavallium.rockserver.core.common.KV mapKV(@NotNull KV entry) {
		return new it.cavallium.rockserver.core.common.KV(
				mapKeys(entry.getKeysList()),
				toBuf(entry.getValue())
		);
	}

	private static Keys mapKeys(List<ByteString> wireKeys) {
		var segments = new Buf[wireKeys.size()];
		for (int i = 0; i < segments.length; i++) {
			segments[i] = toBuf(wireKeys.get(i));
		}
		return new Keys(segments);
	}

	private static ByteString mapKey(Buf key) {
		return UnsafeByteOperations.unsafeWrap(key.getBackingByteArray(),
				key.getBackingByteArrayOffset(),
				key.getBackingByteArrayLength());
	}

	private static ByteString mapValue(@NotNull Buf value) {
		if (value == null) {
			throw RocksDBException.of(RocksDBErrorType.UNEXPECTED_NULL_VALUE, "value");
		}
		return UnsafeByteOperations.unsafeWrap(value.getBackingByteArray(),
				value.getBackingByteArrayOffset(),
				value.getBackingByteArrayLength());
	}

	private static it.cavallium.rockserver.core.common.api.proto.ColumnSchema mapColumnSchema(@NotNull ColumnSchema schema) {
		var builder = it.cavallium.rockserver.core.common.api.proto.ColumnSchema.newBuilder()
				.addAllFixedKeys(mapFixedKeys(schema))
				.addAllVariableTailKeys(mapVariableTailKeys(schema))
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

	private static ColumnSchema unmapColumnSchema(it.cavallium.rockserver.core.common.api.proto.ColumnSchema schema) {
		return ColumnSchema.of(unmapKeysLength(schema.getFixedKeysCount(), schema::getFixedKeys),
				unmapVariableTailKeys(schema.getVariableTailKeysCount(), schema::getVariableTailKeys),
				schema.getHasValue(),
				schema.hasMergeOperatorName() ? schema.getMergeOperatorName() : null,
				schema.hasMergeOperatorVersion() ? schema.getMergeOperatorVersion() : null,
				schema.hasMergeOperatorClass() ? schema.getMergeOperatorClass() : null
		);
	}
	private static IntList unmapKeysLength(int count, Int2IntFunction keyGetterAt) {
		var l = new IntArrayList(count);
		for (int i = 0; i < count; i++) {
			l.add((int) keyGetterAt.apply(i));
		}
		return l;
	}

	private static ObjectList<it.cavallium.rockserver.core.common.ColumnHashType> unmapVariableTailKeys(int count,
			Int2ObjectFunction<it.cavallium.rockserver.core.common.api.proto.ColumnHashType> variableTailKeyGetterAt) {
		var l = new ObjectArrayList<it.cavallium.rockserver.core.common.ColumnHashType>(count);
		for (int i = 0; i < count; i++) {
			l.add(switch (variableTailKeyGetterAt.apply(i)) {
				case XXHASH32 -> it.cavallium.rockserver.core.common.ColumnHashType.XXHASH32;
				case XXHASH8 -> it.cavallium.rockserver.core.common.ColumnHashType.XXHASH8;
				case ALLSAME8 -> it.cavallium.rockserver.core.common.ColumnHashType.ALLSAME8;
				case FIXEDINTEGER32 -> it.cavallium.rockserver.core.common.ColumnHashType.FIXEDINTEGER32;
				case UNRECOGNIZED -> throw new UnsupportedOperationException();
			});
		}
		return l;
	}

	private static Iterable<Integer> mapFixedKeys(@NotNull ColumnSchema schema) {
		var result = new IntArrayList(schema.fixedLengthKeysCount());
		for (int i = 0; i < schema.fixedLengthKeysCount(); i++) {
			result.add(schema.key(i));
		}
		return result;
	}

	private static Iterable<ColumnHashType> mapVariableTailKeys(@NotNull ColumnSchema schema) {
		var result = new ArrayList<ColumnHashType>(schema.variableTailKeys().size());
		for (it.cavallium.rockserver.core.common.ColumnHashType variableTailKey : schema.variableTailKeys()) {
			result.add(switch (variableTailKey) {
				case XXHASH32 -> ColumnHashType.XXHASH32;
				case XXHASH8 -> ColumnHashType.XXHASH8;
				case ALLSAME8 -> ColumnHashType.ALLSAME8;
				case FIXEDINTEGER32 -> ColumnHashType.FIXEDINTEGER32;
			});
		}
		return result;
	}

	private static <T> CompletableFuture<T> toResponse(CompletableFuture<T> future) {
		return future
				.exceptionallyCompose(ex -> CompletableFuture.failedFuture(mapGrpcStatusError(ex)));
	}

	private static <T> Mono<T> toResponse(Mono<T> mono) {
		return mono.onErrorMap(GrpcConnectionDelegate::mapGrpcStatusError);
	}

	private static <T> Flux<T> toResponse(Flux<T> flux) {
		return flux.onErrorMap(GrpcConnectionDelegate::mapGrpcStatusError);
	}

	private static <T> Flux<T> toReadResponse(Flux<T> flux) {
		return flux.onErrorMap(GrpcConnectionDelegate::mapReadDeadlineError);
	}

	private <T, U> CompletableFuture<U> toResponse(ListenableFuture<T> listenableFuture, Function<T, U> mapper) {
		return toResponse(listenableFuture, mapper, GrpcConnectionDelegate::mapGrpcStatusError);
	}

	private <T, U> CompletableFuture<U> toResponse(ListenableFuture<T> listenableFuture,
			Function<T, U> mapper,
			Function<Throwable, Throwable> errorMapper) {
		return bridgeResponse(listenableFuture, mapper, errorMapper, callbackExecutor);
	}

	static <T, U> CompletableFuture<U> bridgeResponse(ListenableFuture<T> listenableFuture,
			Function<T, U> mapper,
			Function<Throwable, Throwable> errorMapper,
			Executor callbackExecutor) {
		var response = new GrpcResponseFuture<>(listenableFuture, mapper, errorMapper);
		Futures.addCallback(listenableFuture, response, callbackExecutor);
		return response;
	}

	private static final List<String> AUTOMATIC_RETRYABLE_STATUS_CODE_NAMES = List.of(Code.UNAVAILABLE.name());
	private static final Set<Code> AUTOMATIC_RETRYABLE_STATUS_CODES = AUTOMATIC_RETRYABLE_STATUS_CODE_NAMES.stream()
			.map(Code::valueOf)
			.collect(java.util.stream.Collectors.toUnmodifiableSet());

	static boolean isAutomaticallyRetryableStatus(Code code) {
		return AUTOMATIC_RETRYABLE_STATUS_CODES.contains(code);
	}

	private static final class RetryLoggingInterceptor implements ClientInterceptor {

		private static final long DEFAULT_WARNING_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
		private static final int DEFAULT_MAX_WARNING_KEYS = 256;

		private final Consumer<String> warningLogger;
		private final LongSupplier nanoTimeSource;
		private final long warningIntervalNanos;
		private final int maxWarningKeys;
		private final LinkedHashMap<WarningKey, WarningState> warningStates =
				new LinkedHashMap<>(16, 0.75f, true);

		private RetryLoggingInterceptor() {
			this(LOG::warn);
		}

		private RetryLoggingInterceptor(Consumer<String> warningLogger) {
			this(warningLogger, System::nanoTime, DEFAULT_WARNING_INTERVAL_NANOS, DEFAULT_MAX_WARNING_KEYS);
		}

		private RetryLoggingInterceptor(Consumer<String> warningLogger,
				LongSupplier nanoTimeSource,
				long warningIntervalNanos,
				int maxWarningKeys) {
			this.warningLogger = Objects.requireNonNull(warningLogger, "warningLogger");
			this.nanoTimeSource = Objects.requireNonNull(nanoTimeSource, "nanoTimeSource");
			if (warningIntervalNanos <= 0) {
				throw new IllegalArgumentException("warningIntervalNanos must be positive");
			}
			if (maxWarningKeys <= 0) {
				throw new IllegalArgumentException("maxWarningKeys must be positive");
			}
			this.warningIntervalNanos = warningIntervalNanos;
			this.maxWarningKeys = maxWarningKeys;
		}

		private static String terminalStatusWarning(MethodDescriptor<?, ?> method, Status status) {
			// A client interceptor observes the logical call's final close after any
			// transparent or configured retries, not each individual transport attempt.
			return "gRPC call to " + method.getFullMethodName()
					+ " reached terminal status " + status.getCode()
					+ ". No further automatic retry will be attempted by gRPC.";
		}

		private void logTerminalStatusWarning(MethodDescriptor<?, ?> method, Status status) {
			var key = new WarningKey(method.getFullMethodName(), status.getCode());
			var nowNanos = nanoTimeSource.getAsLong();
			long suppressedCount;
			synchronized (warningStates) {
				var state = warningStates.get(key);
				if (state == null) {
					if (warningStates.size() == maxWarningKeys) {
						var eldest = warningStates.entrySet().iterator();
						eldest.next();
						eldest.remove();
					}
					warningStates.put(key, new WarningState(nowNanos + warningIntervalNanos));
					suppressedCount = 0;
				} else if (nowNanos - state.nextWarningNanos >= 0) {
					suppressedCount = state.suppressedCount;
					state.suppressedCount = 0;
					state.nextWarningNanos = nowNanos + warningIntervalNanos;
				} else {
					state.suppressedCount++;
					return;
				}
			}

			var warning = terminalStatusWarning(method, status);
			if (suppressedCount > 0) {
				warning += " Suppressed " + suppressedCount
						+ " additional terminal warnings for this method and status since the previous warning.";
			}
			warningLogger.accept(warning);
		}

		@Override
		public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
				MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
			return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
				@Override
				public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {
					var loggingListener = new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
						@Override
						public void onClose(Status status, Metadata trailers) {
							if (!status.isOk() && isAutomaticallyRetryableStatus(status.getCode())) {
								logTerminalStatusWarning(method, status);
							}
							super.onClose(status, trailers);
						}
					};
					super.start(loggingListener, headers);
				}
			};
		}

		private record WarningKey(String methodName, Code statusCode) {}

		private static final class WarningState {

			private long nextWarningNanos;
			private long suppressedCount;

			private WarningState(long nextWarningNanos) {
				this.nextWarningNanos = nextWarningNanos;
			}
		}
	}

	private static final String grpcRocksDbErrorPrefixString = "RocksDBError: [uid:";

	private static Throwable mapGrpcStatusError(@NotNull Throwable t) {
		if (t instanceof StatusRuntimeException statusRuntimeException
				&& statusRuntimeException.getStatus().getDescription() != null
				&& statusRuntimeException.getStatus().getDescription().startsWith(grpcRocksDbErrorPrefixString)) {
			var desc = statusRuntimeException.getStatus().getDescription();
			var closeIndex = desc.indexOf(']', grpcRocksDbErrorPrefixString.length());
			if (closeIndex < 0) {
				return mapTransportDeadlineError(t);
			}
			var errorCode = desc.substring(grpcRocksDbErrorPrefixString.length(), closeIndex);
			var errorDescriptionStart = closeIndex + 1;
			if (errorDescriptionStart < desc.length() && desc.charAt(errorDescriptionStart) == ' ') {
				errorDescriptionStart++;
			}
			var errorDescription = desc.substring(errorDescriptionStart);
			final RocksDBErrorType errorType;
			try {
				errorType = RocksDBErrorType.valueOf(errorCode);
			} catch (IllegalArgumentException malformedOrFutureErrorCode) {
				return mapTransportDeadlineError(t);
			}
			if (errorType == RocksDBErrorType.UPDATE_RETRY) {
				return new RocksDBRetryException();
			} else {
				return RocksDBException.of(errorType, errorDescription);
			}
		}
		return mapTransportDeadlineError(t);
	}

	private static Throwable mapTransportDeadlineError(@NotNull Throwable error) {
		return Status.fromThrowable(error).getCode() == Code.DEADLINE_EXCEEDED
				? RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED, "Deadline exceeded")
				: error;
	}

	private static Throwable mapReadDeadlineError(@NotNull Throwable error) {
		return mapGrpcStatusError(error);
	}

	private <T> CompletableFuture<T> toResponse(ListenableFuture<T> listenableFuture) {
		return toResponse(listenableFuture, Function.identity(), Function.identity());
	}

	private static final class GrpcResponseFuture<T, U> extends CompletableFuture<U>
			implements FutureCallback<T> {

		private final ListenableFuture<T> listenableFuture;
		private final Function<T, U> mapper;
		private final Function<Throwable, Throwable> errorMapper;
		private final AtomicBoolean callbackClaimed = new AtomicBoolean();

		private GrpcResponseFuture(ListenableFuture<T> listenableFuture,
				Function<T, U> mapper,
				Function<Throwable, Throwable> errorMapper) {
			this.listenableFuture = listenableFuture;
			this.mapper = mapper;
			this.errorMapper = errorMapper;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			boolean cancelled = listenableFuture.cancel(mayInterruptIfRunning);
			if (!cancelled) {
				return false;
			}
			super.cancel(mayInterruptIfRunning);
			return cancelled;
		}

		@Override
		public void onSuccess(T result) {
			if (!tryClaimCallback()) {
				return;
			}
			try {
				complete(mapper.apply(result));
			} catch (VirtualMachineError fatal) {
				completeExceptionally(fatal);
				throw fatal;
			} catch (Throwable mapperFailure) {
				completeExceptionally(mapperFailure);
			}
		}

		@Override
		public void onFailure(@NotNull Throwable failure) {
			if (failure instanceof CancellationException && listenableFuture.isCancelled()) {
				super.cancel(false);
				return;
			}
			if (!tryClaimCallback()) {
				return;
			}
			final Throwable mappedFailure;
			try {
				mappedFailure = errorMapper.apply(failure);
			} catch (VirtualMachineError fatal) {
				preserveOriginalFailure(fatal, failure);
				completeExceptionally(fatal);
				throw fatal;
			} catch (Throwable mapperFailure) {
				preserveOriginalFailure(mapperFailure, failure);
				completeExceptionally(mapperFailure);
				return;
			}

			if (mappedFailure == null) {
				var invalidResult = new NullPointerException("The gRPC error mapper returned null");
				preserveOriginalFailure(invalidResult, failure);
				completeExceptionally(invalidResult);
			} else {
				completeExceptionally(mappedFailure);
			}
		}

		private boolean tryClaimCallback() {
			return !isDone() && callbackClaimed.compareAndSet(false, true) && !isDone();
		}

		private static void preserveOriginalFailure(Throwable mapperFailure, Throwable originalFailure) {
			if (mapperFailure != originalFailure && mapperFailure.getCause() != originalFailure) {
				mapperFailure.addSuppressed(originalFailure);
			}
		}
	}

	@Override
	public void close() {
		try {
			this.channel.shutdown();
		} catch (Exception ex) {
			LOG.error("Failed to close channel", ex);
		}
		try {
			if (!this.channel.awaitTermination(1, TimeUnit.MINUTES)) {
				this.channel.shutdownNow();
				this.channel.awaitTermination(1, TimeUnit.MINUTES);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOG.error("Failed to wait channel termination", e);
			try {
				this.channel.shutdownNow();
			} catch (Exception ex) {
				LOG.error("Failed to close channel", ex);
			}
		}
		this.callbackExecutor.shutdown();
		try {
			if (!this.callbackExecutor.awaitTermination(CALLBACK_EXECUTOR_SHUTDOWN_SECONDS, TimeUnit.SECONDS)) {
				this.callbackExecutor.shutdownNow();
				if (!this.callbackExecutor.awaitTermination(CALLBACK_EXECUTOR_SHUTDOWN_SECONDS, TimeUnit.SECONDS)) {
					LOG.warn("gRPC callback executor did not terminate");
				}
			}
		} catch (InterruptedException e) {
			this.callbackExecutor.shutdownNow();
			Thread.currentThread().interrupt();
			LOG.error("Failed to wait gRPC callback executor termination", e);
		}
		try {
			this.eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOG.error("Failed to wait channel event loop termination", e);
		} catch (Exception ex) {
			LOG.error("Failed to close channel event loop", ex);
		}
	}
}
