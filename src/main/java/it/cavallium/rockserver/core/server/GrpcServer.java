package it.cavallium.rockserver.core.server;


import com.google.common.annotations.VisibleForTesting;
import io.grpc.Grpc;
import io.grpc.MethodDescriptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.ServerCallStreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.impl.InternalConnection;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.config.WorkloadSettings;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.InetSocketAddress;
import java.time.Duration;

import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class GrpcServer extends Server {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class.getName());

	final GrpcServerImpl grpc;
	final GrpcRequestContexts contexts;
	private final GrpcTransport transport;
	private final EventLoopGroup elg;
	private final Class<? extends ServerChannel> channelType;
	private final ServerServiceDefinition service;
	final RWScheduler scheduler;
	private final boolean ownsScheduler;
	/**
	 * For a non-embedded connection: every request waits on that connection for a whole round trip and its admission
	 * happens there, so each runs on a virtual thread of its own, at most {@link GrpcProxyMode#PROXY_PARALLELISM} at once, without
	 * this server's workload queues. Only for single-shot {@code schedule} calls: streaming paths need a worker, and
	 * they use {@link #scheduler} instead. Null for an embedded database.
	 */
	final reactor.core.scheduler.@Nullable Scheduler proxyScheduler;
	/** Runs the calls' callbacks in proxy mode; null when they run on the event loop. */
	private final java.util.concurrent.@Nullable ExecutorService callExecutor;
	final @Nullable EmbeddedDB embeddedDatabase;
	private final MeterRegistry metricsRegistry;
	final GrpcGetStrategy grpcGetStrategy;
	final MustCompleteOperationTracker mustCompleteOperations;

	/** Names the leader in every response, for a connection to a replicated node; null otherwise. */
	private final it.cavallium.rockserver.core.common.@Nullable LeaderHint leaderHint;

	/**
	 * Largest message a client may send, in bytes. Each call holds its whole message on the heap while it is decoded,
	 * so at most the proxy parallelism times this; the default fits the largest batches of a single node.
	 */
	private static final String MAX_INBOUND_MESSAGE_BYTES_PROPERTY
			= "it.cavallium.rockserver.grpc.server.max-inbound-message-bytes";
	private static final int DEFAULT_MAX_INBOUND_MESSAGE_BYTES = 512 * 1024 * 1024;

	public GrpcServer(RocksDBConnection client, SocketAddress socketAddress) throws IOException {
		super(client);
		// The client itself: getClient() wraps it.
		this.leaderHint = client instanceof it.cavallium.rockserver.core.common.LeaderHint hint ? hint : null;
		ExpectedGrpcStreamCloseLogFilter.install();
		if (client instanceof InternalConnection internalConnection) {
			this.scheduler = internalConnection.getScheduler();
			this.ownsScheduler = false;
			this.embeddedDatabase = internalConnection.getEmbeddedDB();
			this.proxyScheduler = null;
		} else {
			// Every request waits on the other connection (a remote or replicated database) for a whole round trip:
			// with one platform thread per core, the round trip latency would cap the throughput. Single-shot
			// requests use the proxy scheduler. Streaming paths, which hold a worker for the whole stream, and
			// rollbacks and maintenance use the workload scheduler, whose bounded queues refuse excess streams.
			this.scheduler = RWScheduler.withVirtualWorkers(WorkloadSettings.proxy(GrpcProxyMode.PROXY_STREAM_PARALLELISM), "grpc-db");
			this.ownsScheduler = true;
			this.embeddedDatabase = null;
			this.proxyScheduler = new GrpcProxyMode.ProxyScheduler(GrpcProxyMode.PROXY_PARALLELISM);
		}
		this.metricsRegistry = embeddedDatabase != null
				? embeddedDatabase.getMetricsRegistry()
				: Metrics.globalRegistry;
		this.grpcGetStrategy = GrpcGetStrategy.configured();
		this.mustCompleteOperations = new MustCompleteOperationTracker(metricsRegistry);
		this.contexts = new GrpcRequestContexts(this.getClient(), scheduler, embeddedDatabase);
		this.grpc = new GrpcServerImpl(this, this.getClient());
		EventLoopGroup elg;
		Class<? extends ServerChannel> channelType;
		// Named, so that a replicated node tells the API's transport threads from those of Ratis' own (shaded) gRPC,
		// which keeps Netty's default names: a direct memory error is fatal only on the latter (DirectMemoryWatch).
		var threads = new io.netty.util.concurrent.DefaultThreadFactory("rockserver-grpc-server-loop", false);
		if (socketAddress instanceof DomainSocketAddress _) {
			elg = new EpollEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, threads);
			channelType = EpollServerDomainSocketChannel.class;
		} else {
			elg = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, threads);
			channelType = NioServerSocketChannel.class;
		}
		this.elg = elg;
		this.channelType = channelType;
		this.service = bindReadService();
		// Calls to another connection: their callbacks run off the event loop, so that one client connection
		// (one event loop thread) is not limited to what a single thread can decode, dispatch and answer.
		this.callExecutor = proxyScheduler != null
				? java.util.concurrent.Executors.newThreadPerTaskExecutor(
						Thread.ofVirtual().name("grpc-call-", 0).factory())
				: null;
		this.transport = new GrpcTransport(socketAddress, elg, channelType, service, callExecutor, leaderHint);
		LOG.info("GRPC RocksDB server is listening at " + socketAddress);
	}

	/**
	 * The largest message a client may send: the system property
	 * {@code it.cavallium.rockserver.grpc.server.max-inbound-message-bytes}, by default 512 MiB.
	 */
	@VisibleForTesting
	public static int configuredMaxInboundMessageBytes() {
		var value = System.getProperty(MAX_INBOUND_MESSAGE_BYTES_PROPERTY);
		if (value == null || value.isBlank()) {
			return DEFAULT_MAX_INBOUND_MESSAGE_BYTES;
		}
		try {
			int bytes = Integer.parseInt(value.strip());
			if (bytes > 0) {
				return bytes;
			}
		} catch (NumberFormatException ignored) {
			// Warned below.
		}
		LOG.warn("Invalid value for system property {}: {}", MAX_INBOUND_MESSAGE_BYTES_PROPERTY, value);
		return DEFAULT_MAX_INBOUND_MESSAGE_BYTES;
	}

	@Override
	public void start() throws IOException {
		transport.start();
	}

	public int getPort() {
		return transport.getPort();
	}

	/**
	 * Stops serving, for a server in front of another connection (not an embedded database): the port stops listening
	 * and every connection gets a GOAWAY, so that clients with other addresses move to them. Calls already running go
	 * on until they end. {@link #resume()} serves again. Does nothing if already paused.
	 */
	public void pause() {
		requireProxyMode();
		transport.pause();
	}

	/**
	 * Listens again on the same address after {@link #pause()}, with a new gRPC server. If that fails the server stays
	 * paused, and this can be called again. Does nothing if not paused.
	 */
	public void resume() throws IOException {
		requireProxyMode();
		transport.resume();
	}

	private void requireProxyMode() {
		if (proxyScheduler == null) {
			throw new IllegalStateException("A server over an embedded database does not pause");
		}
	}

	@VisibleForTesting
	public int getActiveIteratorOperationLeaseCountForTesting() {
		return grpc.iterators.iteratorOperations.size();
	}

	@VisibleForTesting
	public int getAcceptedMustCompleteOperationCountForTesting() {
		return mustCompleteOperations.acceptedOperations();
	}

	@VisibleForTesting
	public long getCancelledMustCompleteOperationCountForTesting() {
		return mustCompleteOperations.cancelledMustCompleteOperations.get();
	}

	@VisibleForTesting
	public long getLateProtectedOperationFailureCountForTesting(String operation) {
		var failures = mustCompleteOperations.lateProtectedOperationFailures.get(operation);
		return failures == null ? 0L : failures.sum();
	}

	@VisibleForTesting
	public RWScheduler getSchedulerForTesting() {
		return scheduler;
	}

	@VisibleForTesting
	public static void clearLateReadDeadlineLogStatesForTesting() {
		GrpcRequestErrors.LATE_READ_DEADLINE_LOG_STATES.clear();
	}

	@VisibleForTesting
	public static boolean isExpectedGrpcClientCancellationForTesting(java.util.logging.LogRecord record) {
		return ExpectedGrpcStreamCloseLogFilter.isExpectedClientCancellation(record);
	}

	@VisibleForTesting
	public static void installExpectedGrpcClientCancellationLogFilterForTesting() {
		ExpectedGrpcStreamCloseLogFilter.install();
	}

	/** The age at which a server in front of another connection ends a connection; zero, the default, means never. */
	@VisibleForTesting
	public static Duration configuredMaxConnectionAge() {
		return GrpcProxyMode.configuredMaxConnectionAge();
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

	private ServerServiceDefinition bindReadService() {
		ServerServiceDefinition generated = grpc.bindService();
		var builder = ServerServiceDefinition.builder(generated.getServiceDescriptor().getName());
		String getMethodName = RocksDBServiceGrpc.getGetMethod().getFullMethodName();
		String multiGetMethodName = RocksDBServiceGrpc.getGetMultiMethod().getFullMethodName();
		for (ServerMethodDefinition<?, ?> method : generated.getMethods()) {
			if (grpcGetStrategy != GrpcGetStrategy.LEGACY
					&& method.getMethodDescriptor().getFullMethodName().equals(getMethodName)) {
				builder.addMethod(fastGetMethodDescriptor(), new FastGetCallHandler(this));
			} else if (method.getMethodDescriptor().getFullMethodName().equals(multiGetMethodName)) {
				var descriptor = RocksDBServiceGrpc.getGetMultiMethod().toBuilder(
						RocksDBServiceGrpc.getGetMultiMethod().getRequestMarshaller(),
						MultiGetResponseMarshaller.INSTANCE).build();
				builder.addMethod(descriptor, ServerCalls.asyncUnaryCall((request, observer) -> {
					var task = new MultiGetTask(this, request, (ServerCallStreamObserver<MultiGetResponse>) observer);
					task.observer.setOnCancelHandler(task::cancel);
					task.schedule();
				}));
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

	/**
	 * The scheduler of one task of {@code profile} and {@code family}: the proxy scheduler when this server forwards
	 * every request to a non-embedded connection (see {@link #proxyScheduler}), otherwise a view of {@code rw}.
	 */
	reactor.core.scheduler.Scheduler workloadScheduler(RWScheduler rw,
			WorkloadProfile profile,
			OperationFamily family,
			long localMonotonicDeadlineNanos) {
		var proxy = proxyScheduler;
		// Rollbacks and maintenance keep the scheduler's isolated pools: they must run even when data requests fill the
		// proxy's permits. closeFailedUpdate takes no pool at all in proxy mode, see closeFailedUpdate.
		if (proxy != null && profile != WorkloadProfile.CONTROL && profile != WorkloadProfile.PHYSICAL_MAINTENANCE) {
			return proxy;
		}
		return rw.scheduler(profile, family, localMonotonicDeadlineNanos);
	}

	RWScheduler columnWorkloadScheduler(long columnId) {
		return embeddedDatabase == null ? scheduler : embeddedDatabase.getSchedulerForColumn(columnId);
	}

	RWScheduler commandWorkloadScheduler(RocksDBAPICommand<?, ?, ?> command) {
		return embeddedDatabase == null ? scheduler : embeddedDatabase.getSchedulerForCommand(command);
	}

	@Override
	public void close() throws IOException {
		LOG.info("GRPC server is shutting down...");
		var gracefulShutdownTimeout = GrpcServerSettings.durationProperty("it.cavallium.rockserver.grpc.server.shutdown-graceful-timeout-ms",
				Duration.ofMinutes(1));
		var forcedShutdownTimeout = GrpcServerSettings.durationProperty("it.cavallium.rockserver.grpc.server.shutdown-forced-timeout-ms",
				Duration.ofMinutes(1));
		var schedulerShutdownTimeout = GrpcServerSettings.durationProperty("it.cavallium.rockserver.grpc.server.scheduler-shutdown-timeout-ms",
				Duration.ofMinutes(2));
		boolean interrupted = false;
		IOException closeFailure = null;
		interrupted |= transport.close(gracefulShutdownTimeout, forcedShutdownTimeout);
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
		if (callExecutor != null) {
			callExecutor.shutdown();
		}
		if (proxyScheduler != null) {
			proxyScheduler.dispose();
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

	private static IOException appendCloseFailure(@Nullable IOException current, IOException additional) {
		if (current == null) {
			return additional;
		}
		current.addSuppressed(additional);
		return current;
	}

}
