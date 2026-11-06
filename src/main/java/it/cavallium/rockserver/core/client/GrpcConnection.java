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
import io.grpc.ConnectivityState;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import it.cavallium.rockserver.core.common.MultiGetPage;
import it.cavallium.rockserver.core.common.RangeBudget;
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
import java.net.URISyntaxException;
import java.time.Duration;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
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

import static it.cavallium.rockserver.core.common.Utils.toBuf;

/** Public gRPC connection. Database operations are exposed only by context-bound API views. */
public final class GrpcConnection extends BaseConnection {

	public static final String MAX_INBOUND_MESSAGE_SIZE_PROPERTY =
			GrpcConnectionDelegate.MAX_INBOUND_MESSAGE_SIZE_PROPERTY;
	public static final int DEFAULT_MAX_INBOUND_MESSAGE_SIZE =
			GrpcConnectionDelegate.DEFAULT_MAX_INBOUND_MESSAGE_SIZE;
	public static final int MIN_MAX_INBOUND_MESSAGE_SIZE =
			GrpcConnectionDelegate.MIN_MAX_INBOUND_MESSAGE_SIZE;

	/**
	 * System property: {@code false} turns off {@link SessionWrites} on connections made afterwards: writes are then
	 * sent once, as to a server without client sessions.
	 */
	public static final String SESSION_WRITES_PROPERTY = "it.cavallium.rockserver.grpc.client.session-writes";

	private static final Logger SESSION_LOG = LoggerFactory.getLogger(GrpcConnection.class);
	private final GrpcConnectionDelegate delegate;
	/** Numbered writes, on a replicated server that has client sessions and a connection that retries. */
	private final @Nullable SessionWrites sessionWrites;
	// Retained as test-observable lifecycle handles; ownership stays with the raw delegate.
	private final ManagedChannel channel;
	private final ExecutorService callbackExecutor;
	private final EventLoopGroup eventLoopGroup;

	private GrpcConnection(String name, SocketAddress socketAddress, URI address) {
		this(name, socketAddress, address, true);
	}

	private GrpcConnection(String name, SocketAddress socketAddress, URI address, boolean automaticRetry) {
		this(name, new GrpcConnectionDelegate(name, socketAddress, address, automaticRetry));
	}

	private GrpcConnection(String name, SocketAddress socketAddress, URI address, boolean automaticRetry,
			@Nullable GrpcClientTransport transport) {
		this(name, new GrpcConnectionDelegate(name, socketAddress, address, automaticRetry, transport));
	}

	private GrpcConnection(String name, GrpcConnectionDelegate delegate) {
		super(name);
		this.delegate = delegate;
		var capabilities = delegate.getCapabilities();
		boolean sessionsOff = "false".equalsIgnoreCase(System.getProperty(SESSION_WRITES_PROPERTY));
		this.sessionWrites = delegate.automaticRetry && capabilities.clientSessions() && !sessionsOff
				? new SessionWrites(delegate::openClientSessionAsync,
						(context, command) -> (CompletableFuture<?>) delegate.requestAsync(context, command),
						delegate.callbackExecutor, capabilities.numberedBatches())
				: null;
		if (delegate.automaticRetry) {
			// Read once, from the node that answered the handshake: a connection made before the cluster offered sessions
			// does not start using them, and must be made again after the upgrade.
			SESSION_LOG.info("Connection {}: client sessions {} (server offers sessions: {}, numbered batches: {}, {}: {})",
					name, sessionWrites != null ? "on" : "off", capabilities.clientSessions(),
					capabilities.numberedBatches(), SESSION_WRITES_PROPERTY, sessionsOff ? "false" : "default");
		}
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

	/**
	 * Like {@link #forHostAndPort(String, HostAndPort)}, without gRPC's automatic retry of reads after a transport
	 * loss: for a caller that retries on its own, possibly elsewhere, such as a replica forwarding to its leader.
	 */
	public static GrpcConnection forHostAndPortWithoutRetry(String name, HostAndPort address) {
		return new GrpcConnection(name,
				new InetSocketAddress(address.host(), address.port()),
				URI.create("http://" + address.host() + ":" + address.port()),
				false);
	}

	/**
	 * Like {@link #forHostAndPortWithoutRetry(String, HostAndPort)}, on the event loop threads and callback executor of
	 * {@code transport}, which the connection does not close.
	 */
	public static GrpcConnection forHostAndPortWithoutRetry(String name, HostAndPort address,
			GrpcClientTransport transport) {
		return new GrpcConnection(name,
				new InetSocketAddress(address.host(), address.port()),
				URI.create("http://" + address.host() + ":" + address.port()),
				false,
				transport);
	}

	/**
	 * A connection to any of several Rockserver nodes, such as the nodes of a replicated cluster, which all serve
	 * every request. It uses one node at a time: the first in the list that accepts a connection (list the expected
	 * leader first, to save followers a forwarding hop). When the connection to that node is lost, calls in flight
	 * fail as with a single address (reads that gRPC may retry are retried, on the next node), and new calls go to
	 * the first node of the list that accepts a connection, within about one connect timeout per dead node before it.
	 * The connection does not move back to an earlier node while the current one works. One address is the same as
	 * {@link #forHostAndPort(String, HostAndPort)}.
	 *
	 * <p>This call returns once a node answered the capability handshake. No node may be ready yet, for example with a
	 * cluster that is starting: while the answer is {@code UNAVAILABLE}, which is also what a list gets when no host
	 * resolves, or no answer comes within 10 s, as with silent or frozen nodes, it tries again, for 30 s by default, and
	 * then fails. Any other answer, such as a server that lacks the handshake, fails it at once. The system property
	 * {@code it.cavallium.rockserver.grpc.client.handshake-wait-ms} sets this wait, for one address too, where it is 0
	 * by default: that one fails after the call's own retries, which take about 8 s on a refused connection. The wait
	 * ends on time, unless it is shorter than the first attempt, which always has its 10 s.</p>
	 */
	public static GrpcConnection forHostsAndPorts(String name, List<HostAndPort> addresses) {
		return forHostsAndPorts(name, addresses, true);
	}

	/**
	 * Like {@link #forHostsAndPorts(String, List)}, without gRPC's automatic retry of reads after a transport loss.
	 */
	public static GrpcConnection forHostsAndPortsWithoutRetry(String name, List<HostAndPort> addresses) {
		return forHostsAndPorts(name, addresses, false);
	}

	private static GrpcConnection forHostsAndPorts(String name, List<HostAndPort> addresses, boolean automaticRetry) {
		if (addresses.isEmpty()) {
			throw new IllegalArgumentException("At least one address is required");
		}
		if (addresses.size() == 1) {
			return automaticRetry
					? forHostAndPort(name, addresses.getFirst())
					: forHostAndPortWithoutRetry(name, addresses.getFirst());
		}
		var copy = List.copyOf(addresses);
		return new GrpcConnection(name, new GrpcConnectionDelegate(name, copy, httpUrl(copy), automaticRetry));
	}

	/** The URL of a connection to these addresses: {@code http://host1:port1,host2:port2}. */
	static URI httpUrl(List<HostAndPort> addresses) {
		try {
			return Utils.parseConnectionUrl("http://" + Utils.formatHostAndPorts(addresses));
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException("Invalid addresses: " + addresses, e);
		}
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
	public CompletableFuture<Long> openClientSessionAsync(RequestContext context) {
		return delegate.openClientSessionAsync(context);
	}

	@SuppressWarnings("unchecked")
	@Override
	<R, RS, RA> RS requestSync(BoundRequestContext context, RocksDBAPICommand<R, RS, RA> request) {
		var writes = sessionWrites;
		if (writes != null && (writes.numbers(request) || SessionWrites.observes(request))) {
			// Bounded by the deadline plus a margin: a lost callback must not block the caller forever. Past it the write
			// is cancelled, and ends unknown if a copy was sent.
			return (RS) SessionWrites.await((CompletableFuture<Object>) requestAsync(context, request),
					SessionWrites.syncWaitNanos(context));
		}
		return delegate.requestSync(context, request);
	}

	@SuppressWarnings("unchecked")
	@Override
	<R, RS, RA> RA requestAsync(BoundRequestContext context, RocksDBAPICommand<R, RS, RA> request) {
		var writes = sessionWrites;
		if (writes != null && writes.numbers(request)) {
			return (RA) writes.send(context, (RocksDBAPICommand.RocksDBAPICommandSingle<?>) request);
		}
		if (writes != null && SessionWrites.observes(request)) {
			return (RA) ((CompletableFuture<Object>) delegate.requestAsync(context, request))
					.thenApply(result -> {
						writes.observe(request, result);
						return result;
					});
		}
		return delegate.requestAsync(context, request);
	}

	@Override
	public void close() {
		var writes = sessionWrites;
		try {
			delegate.close();
		} finally {
			// After the channel: the calls it fails end as unknown, and those waiting for a resend end here.
			if (writes != null) {
				writes.close();
			}
		}
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
	static final String CONNECT_TIMEOUT_PROPERTY = "it.cavallium.rockserver.grpc.client.connect-timeout-ms";
	static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 3_000;
	static final String KEEP_ALIVE_TIME_PROPERTY = "it.cavallium.rockserver.grpc.client.keep-alive-time-ms";
	static final int DEFAULT_KEEP_ALIVE_TIME_MILLIS = 10_000;
	static final String KEEP_ALIVE_TIMEOUT_PROPERTY = "it.cavallium.rockserver.grpc.client.keep-alive-timeout-ms";
	static final int DEFAULT_KEEP_ALIVE_TIMEOUT_MILLIS = 5_000;
	static final String MAX_RECONNECT_DELAY_PROPERTY = "it.cavallium.rockserver.grpc.client.max-reconnect-delay-ms";
	static final int DEFAULT_MAX_RECONNECT_DELAY_MILLIS = 2_000;
	static final String HANDSHAKE_WAIT_PROPERTY = "it.cavallium.rockserver.grpc.client.handshake-wait-ms";
	/** Several addresses are a cluster, whose nodes can all be waiting for each other during a start. */
	static final int DEFAULT_HANDSHAKE_WAIT_SEVERAL_ADDRESSES_MILLIS = 30_000;
	/**
	 * One address has no wait of its own, as it never had: a plain server that is down is an error for its caller, after
	 * the retries of the call, which let a client that starts with its server connect.
	 */
	static final int DEFAULT_HANDSHAKE_WAIT_ONE_ADDRESS_MILLIS = 0;
	private static final long HANDSHAKE_RETRY_PAUSE_MILLIS = 500;
	/**
	 * One node at a time, the first of the list that accepts a connection: gRPC's default policy, written out so
	 * that the address order is a contract (never shuffled).
	 */
	private static final List<Map<String, ?>> PICK_FIRST_IN_ORDER =
			List.of(Map.of("pick_first", Map.of("shuffleAddressList", false)));
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
	 * transaction/update/iterator creation, mutations, maintenance and every
	 * streaming method require caller-owned recovery instead.
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
			RocksDBServiceGrpc.getGetMultiMethod(),
			RocksDBServiceGrpc.getReduceRangeFirstAndLastMethod(),
			RocksDBServiceGrpc.getReduceRangeEntriesCountMethod(),
			RocksDBServiceGrpc.getGetRangePageMethod(),
			RocksDBServiceGrpc.getGetAllColumnDefinitionsMethod(),
			RocksDBServiceGrpc.getCheckMergeOperatorMethod());
	private static final List<Map<String, String>> AUTOMATIC_RETRY_METHOD_CONFIG_NAMES =
			AUTOMATIC_RETRY_METHOD_DESCRIPTORS.stream()
					.map(GrpcConnectionDelegate::serviceConfigName)
					.toList();
	final ManagedChannel channel;
	final ExecutorService callbackExecutor;
	final EventLoopGroup eventLoopGroup;
	/** gRPC retries reads after a transport loss, and {@link SessionWrites} resends numbered writes. */
	final boolean automaticRetry;
	/** False when {@link #eventLoopGroup} and {@link #callbackExecutor} belong to a shared {@link GrpcClientTransport}. */
	private final boolean ownsTransport;
	private final RocksDBServiceFutureStub futureStub;
	private final ReactorRocksDBServiceGrpc.ReactorRocksDBServiceStub reactiveStub;
	private final URI address;
	private final int maxInboundMessageSize;
	private final RockserverCapabilities capabilities;
	private final ReconnectTimer reconnectTimer;
	/** Sends leader-bound calls straight to the leader; null for one address. */
	private final @Nullable LeaderHintRouter leaderRouter;

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
		this(name, socketAddress, address, true);
	}

	GrpcConnectionDelegate(String name, SocketAddress socketAddress, URI address, boolean automaticRetry) {
		this(name, socketAddress, address, automaticRetry, null);
	}

	/** @param transport shared threads to use instead of the connection's own; ignored for a domain socket */
	GrpcConnectionDelegate(String name, SocketAddress socketAddress, URI address, boolean automaticRetry,
			@Nullable GrpcClientTransport transport) {
		this(name,
				socketAddress instanceof InetSocketAddress inetSocketAddress
						? NettyChannelBuilder.forAddress(inetSocketAddress.getHostString(), inetSocketAddress.getPort())
						: NettyChannelBuilder.forAddress(socketAddress),
				socketAddress instanceof DomainSocketAddress,
				null,
				address,
				automaticRetry,
				socketAddress instanceof DomainSocketAddress ? null : transport);
	}

	/** Several nodes, tried in order by {@code pick_first} (see {@link MultiAddressNameResolver}). */
	GrpcConnectionDelegate(String name, List<HostAndPort> addresses, URI address, boolean automaticRetry) {
		this(name,
				NettyChannelBuilder.forTarget(MultiAddressNameResolver.target(addresses))
						.setNameResolverArg(MultiAddressNameResolver.LAST_GOOD_ADDRESSES, new HashMap<>()),
				false,
				addresses,
				address,
				automaticRetry,
				null);
	}

	/**
	 * @param addresses the nodes of a connection to several of them, null for one: see {@link LeaderHintRouter}
	 * @param transport shared threads, which this connection does not close; null to make its own
	 */
	private GrpcConnectionDelegate(String name,
			NettyChannelBuilder channelBuilder,
			boolean domainSocket,
			@Nullable List<HostAndPort> addresses,
			URI address,
			boolean automaticRetry,
			@Nullable GrpcClientTransport transport) {
		super(name);
		boolean severalAddresses = addresses != null;
		var hintMode = severalAddresses ? LeaderHintRouter.configuredMode() : LeaderHintRouter.Mode.OFF;
		// Reads that the retry policy would send again run again on the ordinary channel if the leader's fails.
		var leaderRouter = hintMode == LeaderHintRouter.Mode.OFF ? null : new LeaderHintRouter(hintMode, addresses,
				automaticRetry ? AUTOMATIC_RETRY_METHOD_DESCRIPTORS.stream()
						.map(MethodDescriptor::getBareMethodName)
						.collect(Collectors.toUnmodifiableSet()) : Set.of());
		int maxInboundMessageSize = configuredMaxInboundMessageSize();

		// A node that dies without closing its connections (power loss, partition, a frozen process) is noticed by
		// keepalive within its time plus its timeout: calls in flight then fail, and new calls find another address.
		// A shorter time would close the connection during a long pause of a node or of this client; 10 s is the
		// shortest gRPC's client allows, and the server permits a ping every 5 s.
		channelBuilder
				.usePlaintext()
				.maxInboundMessageSize(maxInboundMessageSize)
				.keepAliveTime(intProperty(KEEP_ALIVE_TIME_PROPERTY, DEFAULT_KEEP_ALIVE_TIME_MILLIS, 1),
						TimeUnit.MILLISECONDS)
				.keepAliveTimeout(intProperty(KEEP_ALIVE_TIMEOUT_PROPERTY, DEFAULT_KEEP_ALIVE_TIMEOUT_MILLIS, 1),
						TimeUnit.MILLISECONDS)
				.keepAliveWithoutCalls(true);
		if (automaticRetry) {
			configureRetry(channelBuilder, severalAddresses);
		} else {
			channelBuilder
					.disableRetry()
					.defaultServiceConfig(Map.of("loadBalancingConfig", PICK_FIRST_IN_ORDER));
		}

		// Keep reactive-gRPC's bounded adapters off the transport loop: a backpressured
		// application callback must not prevent the same channel from making progress.
		boolean ownsTransport = transport == null;
		var callbackExecutor = transport != null ? transport.callbackExecutor
				: Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
						.name("rockserver-grpc-" + name + "-callback-", 0)
						.factory());
		EventLoopGroup eventLoopGroup = null;
		ManagedChannel channel;
		try {
			channelBuilder.executor(callbackExecutor);
			if (transport != null) {
				eventLoopGroup = transport.eventLoopGroup;
				channelBuilder.eventLoopGroup(eventLoopGroup).channelType(NioSocketChannel.class);
			} else if (domainSocket) {
				eventLoopGroup = new EpollEventLoopGroup(EVENT_LOOP_THREADS_PER_CONNECTION);
				channelBuilder
						.eventLoopGroup(eventLoopGroup)
						.channelType(EpollDomainSocketChannel.class);
			} else {
				eventLoopGroup = new NioEventLoopGroup(EVENT_LOOP_THREADS_PER_CONNECTION);
				channelBuilder.eventLoopGroup(eventLoopGroup);
				if (severalAddresses) {
					// A host that is down does not refuse the connection, and a frozen process accepts it and stays
					// silent: give up on either after this long and try the next address, instead of waiting for
					// Netty's 30 s or for keepalive. One address has nowhere else to go, and keeps waiting.
					int connectTimeoutMillis = intProperty(CONNECT_TIMEOUT_PROPERTY, DEFAULT_CONNECT_TIMEOUT_MILLIS, 1);
					channelBuilder
							.withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
							.channelFactory(() -> {
								var socket = new NioSocketChannel();
								socket.pipeline().addLast(new ServerGreetingTimeoutHandler(connectTimeoutMillis));
								return socket;
							});
				} else {
					channelBuilder.channelType(NioSocketChannel.class);
				}
			}
			channelBuilder.intercept(new RetryLoggingInterceptor());
			if (leaderRouter != null) {
				channelBuilder.intercept(leaderRouter.hintReader());
			}
			channel = channelBuilder.build();
		} catch (RuntimeException | Error ex) {
			if (ownsTransport) {
				callbackExecutor.shutdownNow();
				if (eventLoopGroup != null) {
					eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
				}
			}
			throw ex;
		}
		var futureStub = RocksDBServiceGrpc.newFutureStub(channel);
		RockserverCapabilities capabilities;
		try {
			var response = handshake(channel, futureStub, severalAddresses);
			capabilities = new RockserverCapabilities(response.getWorkloadContractVersion(),
					response.getClientSessions(), response.getNumberedBatches());
			capabilities.requireCompatible();
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			closeFailedConstruction(channel, ownsTransport ? eventLoopGroup : null,
					ownsTransport ? callbackExecutor : null);
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR,
					"Interrupted during the Rockserver capability handshake", interrupted);
		} catch (ExecutionException failure) {
			closeFailedConstruction(channel, ownsTransport ? eventLoopGroup : null,
					ownsTransport ? callbackExecutor : null);
			var cause = failure.getCause() != null ? failure.getCause() : failure;
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
			closeFailedConstruction(channel, ownsTransport ? eventLoopGroup : null,
					ownsTransport ? callbackExecutor : null);
			throw failure;
		}
		this.channel = channel;
		this.callbackExecutor = callbackExecutor;
		this.eventLoopGroup = eventLoopGroup;
		this.automaticRetry = automaticRetry;
		this.ownsTransport = ownsTransport;
		// The handshake stays on the ordinary channel; later calls may go to the leader's.
		this.leaderRouter = leaderRouter;
		if (leaderRouter != null) {
			leaderRouter.start(leader -> newLeaderChannel(leader, maxInboundMessageSize,
					this.callbackExecutor, this.eventLoopGroup, leaderRouter));
		}
		var stubChannel = leaderRouter != null ? leaderRouter.route(channel) : channel;
		this.futureStub = leaderRouter != null ? RocksDBServiceGrpc.newFutureStub(stubChannel) : futureStub;
		this.reactiveStub = ReactorRocksDBServiceGrpc.newReactorStub(stubChannel);
		this.address = address;
		this.maxInboundMessageSize = maxInboundMessageSize;
		this.capabilities = capabilities;
		this.reconnectTimer = new ReconnectTimer(channel, eventLoopGroup);
	}

	/**
	 * Asks for the capabilities of the server, trying again within the wait while the server cannot be reached or gives
	 * no answer in time. Any other answer ends it at once: waiting cannot change it. Failing calls do not start a
	 * connection, and an attempt after a failure waits for the channel's growing delay: each round skips that delay.
	 *
	 * <p>The first attempt always has its whole time, and later ones end with the wait. An attempt can take seconds,
	 * with the retries of the call, so looking at the clock only between attempts would overshoot the wait by that
	 * much.</p>
	 */
	private static CapabilitiesResponse handshake(ManagedChannel channel, RocksDBServiceFutureStub futureStub,
			boolean severalAddresses) throws InterruptedException, ExecutionException {
		long waitNanos = TimeUnit.MILLISECONDS.toNanos(intProperty(HANDSHAKE_WAIT_PROPERTY, severalAddresses
				? DEFAULT_HANDSHAKE_WAIT_SEVERAL_ADDRESSES_MILLIS
				: DEFAULT_HANDSHAKE_WAIT_ONE_ADDRESS_MILLIS, 0));
		long pauseNanos = TimeUnit.MILLISECONDS.toNanos(HANDSHAKE_RETRY_PAUSE_MILLIS);
		long attemptNanos = TimeUnit.SECONDS.toNanos(CAPABILITY_HANDSHAKE_TIMEOUT_SECONDS);
		long start = System.nanoTime();
		ExecutionException reported = null;
		while (true) {
			var response = futureStub
					.withDeadlineAfter(attemptNanos, TimeUnit.NANOSECONDS)
					.getCapabilities(CapabilitiesRequest.getDefaultInstance());
			ExecutionException failure;
			try {
				return response.get(attemptNanos, TimeUnit.NANOSECONDS);
			} catch (ExecutionException callFailure) {
				failure = callFailure;
			} catch (TimeoutException timeout) {
				// The same event as the deadline of the call, which this timeout can reach first.
				response.cancel(true);
				failure = new ExecutionException(Status.DEADLINE_EXCEEDED
						.withDescription("No answer to the capability handshake")
						.withCause(timeout)
						.asRuntimeException());
			}
			var code = Status.fromThrowable(failure.getCause()).getCode();
			if (code != Code.UNAVAILABLE && code != Code.DEADLINE_EXCEEDED) {
				throw failure;
			}
			// An attempt that the end of the wait cut short says less than an UNAVAILABLE heard before it.
			if (reported == null || code == Code.UNAVAILABLE) {
				reported = failure;
			}
			long leftNanos = waitNanos - (System.nanoTime() - start);
			if (leftNanos <= 0) {
				throw reported;
			}
			TimeUnit.NANOSECONDS.sleep(Math.min(pauseNanos, leftNanos));
			channel.resetConnectBackoff();
			attemptNanos = Math.min(attemptNanos, leftNanos - pauseNanos);
		}
	}

	/**
	 * With its address or every address of its list failing, a channel retries after a delay that grows up to 2 minutes,
	 * and failing calls do not start a connection: a client could lag behind a node, or a cluster, that returned. This
	 * timer skips the rest of that delay, so a client reconnects within the configured maximum. Each client has its own
	 * period, which keeps the clients of a cluster that returned from reconnecting together.
	 *
	 * <p>The timer runs only while the channel is in {@code TRANSIENT_FAILURE}. In any other state there is only a
	 * listener for its next change: a healthy channel has nothing scheduled, and an idle one stays idle.</p>
	 */
	private static final class ReconnectTimer {

		private final ManagedChannel channel;
		private final EventLoopGroup eventLoopGroup;
		private final long periodMillis;
		private @Nullable ScheduledFuture<?> timer;
		private boolean closed;

		ReconnectTimer(ManagedChannel channel, EventLoopGroup eventLoopGroup) {
			this.channel = channel;
			this.eventLoopGroup = eventLoopGroup;
			int maxDelayMillis = intProperty(MAX_RECONNECT_DELAY_PROPERTY, DEFAULT_MAX_RECONNECT_DELAY_MILLIS, 2);
			this.periodMillis = ThreadLocalRandom.current().nextLong(maxDelayMillis / 2, maxDelayMillis + 1L);
			onStateChange();
		}

		private synchronized void onStateChange() {
			if (closed) {
				return;
			}
			var state = channel.getState(false);
			if (state != ConnectivityState.TRANSIENT_FAILURE) {
				stopTimer();
			} else if (timer == null) {
				timer = eventLoopGroup.scheduleWithFixedDelay(channel::resetConnectBackoff, periodMillis, periodMillis,
						TimeUnit.MILLISECONDS);
			}
			if (state != ConnectivityState.SHUTDOWN) {
				channel.notifyWhenStateChanged(state, this::onStateChange);
			}
		}

		private void stopTimer() {
			if (timer != null) {
				timer.cancel(false);
				timer = null;
			}
		}

		synchronized void close() {
			closed = true;
			stopTimer();
		}
	}

	@Override
	public RockserverCapabilities getCapabilities() {
		return capabilities;
	}

	private static void closeFailedConstruction(ManagedChannel channel,
			@Nullable EventLoopGroup eventLoopGroup,
			@Nullable ExecutorService callbackExecutor) {
		channel.shutdownNow();
		if (eventLoopGroup != null) {
			eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
		}
		if (callbackExecutor != null) {
			callbackExecutor.shutdownNow();
		}
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

	/**
	 * Default first pause of gRPC's automatic read retry with several addresses: the retry goes to the next node of the
	 * list (the failed one's connection is gone), so it need not wait for that node to come back.
	 */
	static final String DEFAULT_INITIAL_RETRY_BACKOFF_SEVERAL_ADDRESSES = "0.05s";
	static final String DEFAULT_INITIAL_RETRY_BACKOFF_ONE_ADDRESS = "0.5s";

	/** With one address. */
	private static void configureRetry(NettyChannelBuilder channelBuilder) {
		configureRetry(channelBuilder, false);
	}

	private static void configureRetry(NettyChannelBuilder channelBuilder, boolean severalAddresses) {
		int maxAttempts = intProperty(MAX_RETRY_ATTEMPTS_PROPERTY, 5, 1);
		if (maxAttempts <= 1) {
			channelBuilder
					.disableRetry()
					.defaultServiceConfig(Map.of("loadBalancingConfig", PICK_FIRST_IN_ORDER));
			return;
		}

		channelBuilder
				.enableRetry()
				.maxRetryAttempts(maxAttempts)
				.defaultServiceConfig(Map.of(
						"loadBalancingConfig", PICK_FIRST_IN_ORDER,
						"methodConfig", List.of(Map.of(
								"name", AUTOMATIC_RETRY_METHOD_CONFIG_NAMES,
								"retryPolicy", retryPolicy(maxAttempts, severalAddresses)
						))
				));
	}

	/** The gRPC retry policy of the automatically retried methods (read by {@code GrpcRetryPolicyTest}). */
	static Map<String, Object> retryPolicy(int maxAttempts, boolean severalAddresses) {
		return Map.of(
				"maxAttempts", (double) maxAttempts,
				"initialBackoff", System.getProperty(INITIAL_RETRY_BACKOFF_PROPERTY, severalAddresses
						? DEFAULT_INITIAL_RETRY_BACKOFF_SEVERAL_ADDRESSES : DEFAULT_INITIAL_RETRY_BACKOFF_ONE_ADDRESS),
				"maxBackoff", System.getProperty(MAX_RETRY_BACKOFF_PROPERTY, "30s"),
				"backoffMultiplier", doubleProperty(RETRY_BACKOFF_MULTIPLIER_PROPERTY, 2.0d, 1.0d),
				// Retry only transport loss here. SERVER_OVERLOADED and
				// UPDATE_RETRY are application backpressure/conflict signals;
				// callers must observe them immediately and apply domain-level
				// retry, cooldown, or transaction reconstruction.
				"retryableStatusCodes", AUTOMATIC_RETRYABLE_STATUS_CODE_NAMES
		);
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

	/** Opens a client session on a replicated server (see {@link RockserverCapabilities#clientSessions()}). */
	@Override
	public CompletableFuture<Long> openClientSessionAsync(RequestContext context) {
		return withRequestContext(BoundRequestContext.bind(context), () -> toResponse(futureStubWithRequestDeadline()
				.openClientSession(OpenClientSessionRequest.newBuilder()
						.setContext(currentWireRequestContext())
						.build()), OpenClientSessionResponse::getSessionId));
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
				.setResumable(true)
				.setAcceptsTaggedSstTokens(true);
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
	public CompletableFuture<MultiGetPage> getMultiAsync(
			long columnId, List<Keys> keys, RangeBudget budget) {
		MultiGetPage.validateRequest(keys, budget);
		int requestedKeys = keys.size();
		var responseBudget = new RangeBudget(budget.maxItems(),
				Math.min(budget.maxBytes(), maxInboundMessageSize));
		var request = GetMultiRequest.newBuilder().setColumnId(columnId)
				.setBudget(it.cavallium.rockserver.core.common.api.proto.RangeBudget.newBuilder()
						.setMaxItems(budget.maxItems())
						.setMaxBytes(responseBudget.maxBytes()))
				.setContext(currentWireRequestContext());
		for (var logicalKeys : keys) {
			var wireKeys = KeyTuple.newBuilder();
			for (var key : logicalKeys.keys()) {
				wireKeys.addKeys(mapKey(key));
			}
			request.addKeysMulti(wireKeys);
		}
		return toResponse(futureStubWithRequestDeadline().getMulti(request.build()), response -> {
			if (response.getValuesCount() > Math.min(requestedKeys, responseBudget.maxItems())) {
				throw new IllegalStateException("Invalid MultiGet response size");
			}
			var values = new ArrayList<Buf>(response.getValuesCount());
			for (var value : response.getValuesList()) {
				values.add(value.hasValue() ? mapByteString(value.getValue()) : null);
			}
			var page = new MultiGetPage(values, response.getHasMore());
			page.validateResponse(requestedKeys, responseBudget);
			return page;
		}, GrpcConnectionDelegate::mapReadDeadlineError);
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
			throw RocksDBException.notApplied(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
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

	@Override
	public CompletableFuture<Void> flushAsync() {
		return flushAsync(false);
	}

	@Override
	public CompletableFuture<Void> flushWalAsync() {
		return flushAsync(true);
	}

	private CompletableFuture<Void> flushAsync(boolean walOnly) {
		var request = FlushRequest.newBuilder()
				.setWalOnly(walOnly)
				.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
				.build();
		return toResponse(this.futureStub.flush(request), _ -> null);
	}

    @Override
    public CompletableFuture<ColumnTableProperties> getTablePropertiesAsync(long columnId) {
        var request = GetTablePropertiesRequest.newBuilder().setColumnId(columnId)
                .setContext(currentWireRequestContext()).build();
        return toResponse(futureStubWithRequestDeadline().getTableProperties(request),
                p -> new ColumnTableProperties(
                        p.getTableCount(),
                        p.getDataSize(),
                        p.getIndexSize(),
                        p.getIndexPartitions(),
                        p.getTopLevelIndexSize(),
                        p.getFilterSize(),
                        p.getRawKeySize(),
                        p.getRawValueSize(),
                        p.getNumDataBlocks(),
                        p.getNumEntries(),
                        p.getNumDeletions(),
                        p.getNumMergeOperands(),
                        p.getNumRangeDeletions(),
                        p.getSlowCompressionEstimatedDataSize(),
                        p.getFastCompressionEstimatedDataSize(),
                        p.getOldestCreationTime(),
                        p.getNewestCreationTime(),
                        p.getOldestKeyTime(),
                        p.getFormatVersionsMap(),
                        p.getFixedKeyLengthsMap(),
                        p.getIndexKeysAreUserKeysMap(),
                        p.getIndexValuesAreDeltaEncodedMap(),
                        p.getColumnFamilyIdsMap(),
                        p.getFilterPoliciesMap(),
                        p.getComparatorsMap(),
                        p.getMergeOperatorsMap(),
                        p.getPrefixExtractorsMap(),
                        p.getPropertyCollectorsMap(),
                        p.getCompressionsMap()));
    }

    @Override
    public CompletableFuture<SstMaintenance.Metadata> getSstMetadataAsync(long columnId, int level) {
        var request = GetSstMetadataRequest.newBuilder().setColumnId(columnId).setLevel(level)
                .setContext(currentWireRequestContext()).build();
        return toResponse(futureStubWithRequestDeadline().withMaxInboundMessageSize(SstMaintenance.MAX_METADATA_RESPONSE_BYTES)
                .getSstMetadata(request), m -> new SstMaintenance.Metadata(m.getSession(), m.getColumnId(),
                        m.getColumnName(), m.getNumLevels(), m.getBaseLevel(), m.getPathsList(),
                        m.getFilesList().stream().map(f -> new SstMaintenance.File(f.getName(), f.getLevel(),
                                f.getPathId(), f.getSizeBytes(), f.getSmallestKeyHex(), f.getLargestKeyHex(),
                                f.getBeingCompacted())).toList()));
    }

    @Override
    public CompletableFuture<SstMaintenance.Result> compactFilesAsync(SstMaintenance.Request request) {
        // Deliberately excluded from the automatic-retry allowlist, like compact().
        var wireRequest = CompactFilesRequest.newBuilder()
                .setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
                .setColumnId(request.columnId()).setSession(request.session()).addAllFiles(request.files())
                .setLevel(request.level()).setOutputPathId(request.outputPathId())
                .setOutputFileSizeLimit(request.outputFileSizeLimit()).setMaxInputBytes(request.maxInputBytes())
                .setMaxSubcompactions(request.maxSubcompactions()).setExecute(request.execute()).build();
        return toResponse(futureStub.compactFiles(wireRequest), r -> new SstMaintenance.Result(r.getExecuted(),
                r.getInputFilesList(), r.getOutputFilesList(), r.getValidatedInputBytes(), r.getOutputBytes(),
                r.getElapsedNanos()));
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
			throw RocksDBException.notApplied(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
					"Request deadline already expired");
		}
		var requestId = context.requestId();
		if (remainingNanos == RequestContext.NO_TIMEOUT && requestId == null) {
			var cached = NO_TIMEOUT_WIRE_CONTEXTS[context.profile().ordinal()];
			if (cached != null) {
				return cached;
			}
		}
		var wire = it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
				.setProfileValue(context.profile().wireValue())
				.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
				.setTimeoutNanos(remainingNanos);
		if (requestId != null) {
			wire.setRequestId(it.cavallium.rockserver.core.common.api.proto.ClientRequestId.newBuilder()
					.setSessionId(requestId.sessionId())
					.setSequence(requestId.sequence())
					.setCompletedBelow(requestId.completedBelow()));
		}
		return wire.build();
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
			.collect(Collectors.toUnmodifiableSet());

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

	private static final Metadata.Key<String> APPLIED_KEY = Metadata.Key.of(RocksDBException.APPLIED_METADATA_KEY,
			Metadata.ASCII_STRING_MARSHALLER);

	/**
	 * Messages by which a server older than {@link RocksDBException#APPLIED_METADATA_KEY} said that a write was not
	 * applied. Read only when a failure has no such trailer; a server that sends it keeps these messages for older
	 * clients.
	 */
	static final List<String> LEGACY_NOT_APPLIED_MESSAGES = List.of("not applied", "already expired",
			"before downstream dispatch");

	/**
	 * True if a failure of a server says that the write was not applied: from its {@link RocksDBException#APPLIED_METADATA_KEY}
	 * trailer, or, from a server without it, from the message.
	 */
	static boolean saysNotApplied(@Nullable Metadata trailers, String message) {
		var applied = trailers == null ? null : trailers.get(APPLIED_KEY);
		if (applied != null) {
			return RocksDBException.NOT_APPLIED.equals(applied);
		}
		for (var legacy : LEGACY_NOT_APPLIED_MESSAGES) {
			if (message.contains(legacy)) {
				return true;
			}
		}
		return false;
	}

	static Throwable mapGrpcStatusError(@NotNull Throwable t) {
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
			} else if (saysNotApplied(statusRuntimeException.getTrailers(), errorDescription)) {
				return RocksDBException.notApplied(errorType, errorDescription);
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

	/** A channel to the leader that {@link LeaderHintRouter} found, configured like the ordinary one. */
	private static ManagedChannel newLeaderChannel(HostAndPort leader,
			int maxInboundMessageSize,
			ExecutorService callbackExecutor,
			EventLoopGroup eventLoopGroup,
			LeaderHintRouter router) {
		var builder = NettyChannelBuilder.forAddress(leader.host(), leader.port())
				.usePlaintext()
				.maxInboundMessageSize(maxInboundMessageSize)
				.keepAliveTime(intProperty(KEEP_ALIVE_TIME_PROPERTY, DEFAULT_KEEP_ALIVE_TIME_MILLIS, 1), TimeUnit.MILLISECONDS)
				.keepAliveTimeout(intProperty(KEEP_ALIVE_TIMEOUT_PROPERTY, DEFAULT_KEEP_ALIVE_TIMEOUT_MILLIS, 1),
						TimeUnit.MILLISECONDS)
				.keepAliveWithoutCalls(true)
				.executor(callbackExecutor)
				.eventLoopGroup(eventLoopGroup)
				.channelType(NioSocketChannel.class)
				.withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS,
						intProperty(CONNECT_TIMEOUT_PROPERTY, DEFAULT_CONNECT_TIMEOUT_MILLIS, 1));
		// No retries here: a retry would go to the same leader, perhaps dead. LeaderHintRouter runs the reads that the
		// ordinary channel would retry again on that channel instead, which fails over to a live node.
		builder.disableRetry().defaultServiceConfig(Map.of("loadBalancingConfig", PICK_FIRST_IN_ORDER));
		return builder.intercept(new RetryLoggingInterceptor(), router.hintReader()).build();
	}

	@Override
	public void close() {
		if (this.leaderRouter != null) {
			// Before the event loop and the callback executor it shares go away.
			this.leaderRouter.close();
		}
		this.reconnectTimer.close();
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
		if (!ownsTransport) {
			// The shared transport's owner closes its threads.
			return;
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
