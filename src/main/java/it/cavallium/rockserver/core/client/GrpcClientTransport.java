package it.cavallium.rockserver.core.client;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty event loop threads and a gRPC callback executor that several {@link GrpcConnection}s share, instead of each
 * making its own: for a process that keeps many connections, such as a replica forwarding to its leader over several
 * connections per peer. The connections made with it neither own nor close it: close it after them.
 */
public final class GrpcClientTransport implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcClientTransport.class);
	private static final long SHUTDOWN_SECONDS = 5;

	final EventLoopGroup eventLoopGroup;
	final ExecutorService callbackExecutor;

	private GrpcClientTransport(EventLoopGroup eventLoopGroup, ExecutorService callbackExecutor) {
		this.eventLoopGroup = eventLoopGroup;
		this.callbackExecutor = callbackExecutor;
	}

	/**
	 * @param name             names the threads
	 * @param eventLoopThreads Netty threads; Netty assigns each new connection to the next one in turn
	 */
	public static GrpcClientTransport create(String name, int eventLoopThreads) {
		var callbackExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
				.name("rockserver-grpc-" + name + "-callback-", 0)
				.factory());
		try {
			return new GrpcClientTransport(new NioEventLoopGroup(eventLoopThreads,
					Thread.ofPlatform().daemon().name("rockserver-grpc-" + name + "-loop-", 0).factory()),
					callbackExecutor);
		} catch (RuntimeException | Error e) {
			callbackExecutor.shutdownNow();
			throw e;
		}
	}

	/** Stops the threads. Close the connections that use them first: their calls would otherwise stop midway. */
	@Override
	public void close() {
		callbackExecutor.shutdown();
		try {
			if (!callbackExecutor.awaitTermination(SHUTDOWN_SECONDS, TimeUnit.SECONDS)) {
				callbackExecutor.shutdownNow();
				LOG.warn("gRPC callback executor did not terminate");
			}
		} catch (InterruptedException e) {
			callbackExecutor.shutdownNow();
			Thread.currentThread().interrupt();
		}
		try {
			eventLoopGroup.shutdownGracefully(0, SHUTDOWN_SECONDS, TimeUnit.SECONDS).sync();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (RuntimeException e) {
			LOG.error("Failed to close the shared event loop", e);
		}
	}
}
