package it.cavallium.rockserver.core.server;


import com.google.common.annotations.VisibleForTesting;
import io.grpc.netty.NettyServerBuilder;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.api.proto.*;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import java.util.concurrent.TimeUnit;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** A server in front of a non-embedded connection (a remote or replicated database). */
final class GrpcProxyMode {

	private GrpcProxyMode() {
	}

	/**
	 * Requests that may wait on a non-embedded connection at once; more are refused as overload. A count, not bytes: a
	 * replicated node bounds the bytes of its writes itself (its proposer's byte admission), and a waiting read holds
	 * only its key. At the production workload's 0.5-2 KiB values this many requests hold at most tens of MiB.
	 */
	static final int PROXY_PARALLELISM = 16384;

	/**
	 * Streaming requests (iterators and streamed putMulti, deleteMulti and mergeMulti) of each kind that may wait on a
	 * non-embedded connection at once.
	 */
	static final int PROXY_STREAM_PARALLELISM = 256;

	private static final String MAX_CONNECTION_AGE_PROPERTY = "it.cavallium.rockserver.grpc.server.max-connection-age-ms";

	private static final String MAX_CONNECTION_AGE_GRACE_PROPERTY
			= "it.cavallium.rockserver.grpc.server.max-connection-age-grace-ms";

	private static final Duration DEFAULT_MAX_CONNECTION_AGE_GRACE = Duration.ofMinutes(10);

	/** The age at which a server in front of another connection ends a connection; zero, the default, means never. */
	@VisibleForTesting
	static Duration configuredMaxConnectionAge() {
		return GrpcServerSettings.durationProperty(MAX_CONNECTION_AGE_PROPERTY, Duration.ZERO);
	}

	/** The executor of the calls' callbacks, and the connection age, of a server in front of another connection. */
	static void configureTransport(NettyServerBuilder builder, ExecutorService callExecutor) {
		builder.executor(callExecutor);
		// Off by default. Ending a connection brings a client that moved away back to the first address of its list,
		// but every client then starts again from the top of the list at every cycle, so a silent first address costs
		// it the connect timeout each time, and the grace cuts every call that outlives it, on a healthy cluster too.
		// gRPC adds a jitter of +/-10% to the age.
		var maxAge = configuredMaxConnectionAge();
		if (maxAge.isPositive()) {
			builder.maxConnectionAge(maxAge.toNanos(), TimeUnit.NANOSECONDS)
					.maxConnectionAgeGrace(
							GrpcServerSettings.durationProperty(MAX_CONNECTION_AGE_GRACE_PROPERTY, DEFAULT_MAX_CONNECTION_AGE_GRACE).toNanos(),
							TimeUnit.NANOSECONDS);
		}
	}

	/**
	 * Runs each task on a virtual thread of its own, at most {@code limit} at once. Beyond that a task is refused
	 * with a retryable {@code SERVER_OVERLOADED}: the caller maps it like any database error, instead of the generic
	 * rejection (and the stack trace on the event loop) of Reactor's executor scheduler.
	 */
	static final class ProxyScheduler implements Scheduler {

		private final Semaphore permits;
		private final ThreadFactory threads = Thread.ofVirtual().name("grpc-proxy-", 0).factory();
		/** For Reactor operators that need a worker; requests themselves go through {@link #schedule}. */
		private final Scheduler workers = Schedulers.fromExecutor(
				command -> threads.newThread(command).start());

		ProxyScheduler(int limit) {
			this.permits = new Semaphore(limit);
		}

		@Override
		public Disposable schedule(Runnable task) {
			if (!permits.tryAcquire()) {
				throw RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, "Too many requests in flight");
			}
			var scheduled = new ProxyTask(task);
			try {
				threads.newThread(scheduled).start();
			} catch (RuntimeException | Error e) {
				permits.release();
				throw e;
			}
			return scheduled;
		}

		@Override
		public Worker createWorker() {
			return workers.createWorker();
		}

		@Override
		public void dispose() {
			workers.dispose();
		}

		/** A task that does not run if it is disposed before it starts. */
		private final class ProxyTask implements Runnable, Disposable {

			private final Runnable task;
			private final AtomicBoolean claimed = new AtomicBoolean();

			ProxyTask(Runnable task) {
				this.task = task;
			}

			@Override
			public void run() {
				try {
					if (claimed.compareAndSet(false, true)) {
						task.run();
					}
				} finally {
					permits.release();
				}
			}

			@Override
			public void dispose() {
				claimed.compareAndSet(false, true);
			}

			@Override
			public boolean isDisposed() {
				return claimed.get();
			}
		}
	}
}
