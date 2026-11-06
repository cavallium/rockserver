package it.cavallium.rockserver.core.server;


import io.grpc.ServerServiceDefinition;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.*;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The gRPC server that listens: started, paused (stopped and replaced on resume) and closed. */
final class GrpcTransport {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class.getName());

	private final EventLoopGroup elg;
	private final Class<? extends ServerChannel> channelType;
	private final ServerServiceDefinition service;
	/** Runs the calls' callbacks in proxy mode; null when they run on the event loop. */
	private final java.util.concurrent.@Nullable ExecutorService callExecutor;
	/** Names the leader in every response, for a connection to a replicated node; null otherwise. */
	private final it.cavallium.rockserver.core.common.@Nullable LeaderHint leaderHint;

	/** Guards the replacement of {@link #server}: {@link #pause()}, {@link #resume()} and {@link #close}. */
	private final Object transportLock = new Object();

	/** Where this server listens, with the port it actually got once started. */
	private volatile SocketAddress listenAddress;

	/** Replaced by {@link #resume()}: a gRPC server cannot start again. */
	private volatile io.grpc.Server server;

	/** Stopped by {@link #pause()}, and still finishing the calls they were running. Guarded by transportLock. */
	private final List<io.grpc.Server> draining = new ArrayList<>();

	/** Guarded by transportLock. */
	private boolean paused;

	/** Guarded by transportLock. */
	private boolean closed;

	private record AwaitTerminationResult(boolean terminated, boolean interrupted) {}

	GrpcTransport(SocketAddress socketAddress,
			EventLoopGroup elg,
			Class<? extends ServerChannel> channelType,
			ServerServiceDefinition service,
			java.util.concurrent.@Nullable ExecutorService callExecutor,
			it.cavallium.rockserver.core.common.@Nullable LeaderHint leaderHint) {
		this.elg = elg;
		this.channelType = channelType;
		this.service = service;
		this.callExecutor = callExecutor;
		this.leaderHint = leaderHint;
		this.listenAddress = socketAddress;
		this.server = newTransport(socketAddress);
	}

	void start() throws IOException {
		server.start();
		// A port chosen by the system stays the same when the server resumes.
		if (listenAddress instanceof InetSocketAddress inet) {
			listenAddress = new InetSocketAddress(inet.getAddress(), server.getPort());
		}
	}

	int getPort() {
		return listenAddress instanceof InetSocketAddress inet ? inet.getPort() : server.getPort();
	}

	/** See {@link GrpcServer#pause()}. */
	void pause() {
		synchronized (transportLock) {
			if (paused || closed) {
				return;
			}
			paused = true;
			draining.removeIf(io.grpc.Server::isTerminated);
			draining.add(server);
			server.shutdown();
		}
		LOG.warn("GRPC server at {} paused: not listening until it resumes", listenAddress);
	}

	/** See {@link GrpcServer#resume()}. */
	void resume() throws IOException {
		synchronized (transportLock) {
			if (!paused || closed) {
				return;
			}
			var resumed = newTransport(listenAddress);
			resumed.start();
			server = resumed;
			paused = false;
		}
		LOG.info("GRPC server at {} resumed", listenAddress);
	}

	/** Stops every gRPC server, the current one and the paused ones still draining; true if interrupted meanwhile. */
	boolean close(Duration gracefulShutdownTimeout, Duration forcedShutdownTimeout) {
		boolean interrupted = false;
		var servers = new ArrayList<io.grpc.Server>();
		synchronized (transportLock) {
			closed = true;
			servers.addAll(draining);
			servers.add(server);
			server.shutdown();
		}
		for (var stopping : servers) {
			var gracefulTermination = awaitTerminationUninterruptibly(stopping, gracefulShutdownTimeout);
			interrupted |= gracefulTermination.interrupted();
			if (!gracefulTermination.terminated()) {
				stopping.shutdownNow();
				var forcedTermination = awaitTerminationUninterruptibly(stopping, forcedShutdownTimeout);
				interrupted |= forcedTermination.interrupted();
				if (!forcedTermination.terminated()) {
					LOG.error("GRPC server did not terminate after forced shutdown");
				}
			}
		}
		return interrupted;
	}

	private io.grpc.Server newTransport(SocketAddress socketAddress) {
		var builder = NettyServerBuilder
				.forAddress(socketAddress)
				.bossEventLoopGroup(elg)
				.workerEventLoopGroup(elg);
		if (callExecutor != null) {
			GrpcProxyMode.configureTransport(builder, callExecutor);
		} else {
			builder.directExecutor();
		}
		if (leaderHint != null) {
			builder.intercept(new LeaderHintInterceptor(leaderHint));
		}
		return builder
				.channelType(channelType)
				.withChildOption(ChannelOption.SO_KEEPALIVE, false)
				.maxInboundMessageSize(GrpcServer.configuredMaxInboundMessageBytes())
				.addService(service)
				.permitKeepAliveWithoutCalls(true)
				.permitKeepAliveTime(5, TimeUnit.SECONDS)
				.intercept(new AdaptiveCompressionInterceptor())
				.build();
	}

	private static AwaitTerminationResult awaitTerminationUninterruptibly(io.grpc.Server server,
			Duration timeout) {
		long timeoutNanos = GrpcServerSettings.saturatedTimeoutNanos(timeout);
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
				remainingNanos = GrpcServerSettings.remainingTimeoutNanos(timeoutNanos, waitStartedNanos);
			}
		} while (remainingNanos > 0L);
		return new AwaitTerminationResult(server.isTerminated(), interrupted);
	}
}
