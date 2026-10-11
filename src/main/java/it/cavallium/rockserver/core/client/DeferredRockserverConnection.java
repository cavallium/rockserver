package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import org.slf4j.LoggerFactory;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

/** Defers the SDK's mandatory capability handshake until remote work is requested. */
final class DeferredRockserverConnection implements RocksDBConnection {

	private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(50);
	private final URI url;
	private final Callable<RocksDBConnection> factory;
	private final LongSupplier nanoTime;
	private RocksDBConnection connection;
	private Attempt attempt;
	private boolean closed;

	DeferredRockserverConnection(URI url, Callable<RocksDBConnection> factory) {
		this(url, factory, System::nanoTime);
	}

	DeferredRockserverConnection(URI url, Callable<RocksDBConnection> factory, LongSupplier nanoTime) {
		this.url = Objects.requireNonNull(url);
		this.factory = Objects.requireNonNull(factory);
		this.nanoTime = Objects.requireNonNull(nanoTime);
	}

	@Override
	public URI getUrl() {
		return url;
	}

	@Override
	public RockserverCapabilities getCapabilities() {
		return initialized().getCapabilities();
	}

	@Override
	public RocksDBSyncAPI getSyncApi(RequestContext context) {
		return initialized().getSyncApi(context);
	}

	private RocksDBConnection initialized() {
		if (Schedulers.isInNonBlockingThread()) {
			throw new IllegalStateException("Synchronous Rockserver access requires a blocking thread");
		}
		return acquire(new Budget(RequestContext.batch())).block();
	}

	@Override
	public RocksDBAsyncAPI getAsyncApi(RequestContext context) {
		Objects.requireNonNull(context);
		return new RocksDBAsyncAPI() {
			@Override
			public CompletableFuture<Void> flushWalAsync() {
				var budget = new Budget(context);
				return acquire(budget).flatMap(client -> Mono.fromFuture(() ->
						client.getAsyncApi(budget.remainingContext()).flushWalAsync())).toFuture();
			}

			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
				return (RA) switch (request) {
					case RocksDBAPICommand.RocksDBAPICommandSingle<?> ignored -> single(request, new Budget(context));
					case RocksDBAPICommand.RocksDBAPICommandStream<?> ignored -> Flux.defer(() -> {
						var budget = new Budget(context);
						return acquire(budget).flatMapMany(client -> (Publisher<R>)
								client.getAsyncApi(budget.remainingContext()).requestAsync(request));
					});
				};
			}

			private <R, RS, RA> CompletableFuture<R> single(RocksDBAPICommand<R, RS, RA> request, Budget budget) {
				return acquire(budget)
							.flatMap(client -> Mono.fromFuture(() -> (CompletableFuture<R>)
								client.getAsyncApi(budget.remainingContext()).requestAsync(request))).toFuture();
			}

			@Override
			public Mono<CdcBatch> cdcPollBatchAsync(String id, Long fromSeq, long maxEvents) {
				return Mono.defer(() -> {
					var budget = new Budget(context);
					return acquire(budget).flatMap(client -> client.getAsyncApi(budget.remainingContext())
							.cdcPollBatchAsync(id, fromSeq, maxEvents));
				});
			}
		};
	}

	private Mono<RocksDBConnection> acquire(Budget budget) {
		return Mono.defer(() -> acquireConnection().timeout(
				Duration.ofNanos(Math.min(CONNECT_TIMEOUT.toNanos(), budget.remainingNanos())),
				Mono.error(deadlineExceeded())));
	}

	private static RuntimeException deadlineExceeded() {
		return io.grpc.Status.DEADLINE_EXCEEDED.withDescription("Rockserver request deadline expired during connection initialization")
				.asRuntimeException();
	}

	private final class Budget {
		private final RequestContext context;
		private final long started;

		Budget(RequestContext context) {
			this.context = context;
			this.started = nanoTime.getAsLong();
		}

		long remainingNanos() {
			if (!context.hasTimeout()) return RequestContext.NO_TIMEOUT;
			long elapsed = nanoTime.getAsLong() - started;
			if (elapsed < 0 || elapsed >= context.timeoutNanos()) throw deadlineExceeded();
			return context.timeoutNanos() - elapsed;
		}

		RequestContext remainingContext() {
			return new RequestContext(context.profile(), remainingNanos());
		}
	}

	private Mono<RocksDBConnection> acquireConnection() {
		return Mono.defer(() -> {
			Attempt current;
			boolean start;
			synchronized (this) {
				if (closed) return Mono.error(new IllegalStateException("Rockserver connection is closed"));
				if (connection != null) return Mono.just(connection);
				if (attempt != null && attempt.abandoned) {
					return Mono.fromFuture(attempt.finished, true).then(Mono.defer(this::acquireConnection));
				}
				start = attempt == null;
				if (start) attempt = new Attempt();
				current = attempt;
				current.waiters++;
			}
			if (start) {
				try {
					Schedulers.boundedElastic().schedule(() -> connect(current));
				} catch (RuntimeException failure) {
					failed(current, failure);
				}
			}
			return Mono.fromFuture(current.result, true)
					.doFinally(signal -> {
						synchronized (this) {
							if (--current.waiters == 0 && attempt == current) current.abandoned = true;
						}
					});
		});
	}

	private void connect(Attempt current) {
		RocksDBConnection built = null;
		try {
			built = factory.call();
			// Never expose a fabricated or unchecked backend workload contract.
			built.getCapabilities().requireCompatible();
			boolean accepted;
			synchronized (this) {
				accepted = !closed && attempt == current && !current.abandoned && current.waiters > 0;
				if (accepted) {
					connection = built;
					attempt = null;
				}
			}
			if (accepted) current.result.complete(built);
			else closeRejected(built);
		} catch (Exception failure) {
			if (built != null) closeRejected(built);
			failed(current, failure);
		} finally {
			synchronized (this) {
				if (attempt == current) attempt = null;
			}
			current.finished.complete(null);
		}
	}

	private void failed(Attempt current, Throwable failure) {
		synchronized (this) {
			if (attempt == current) attempt = null;
		}
		current.result.completeExceptionally(failure);
		current.finished.complete(null);
	}

	private static void closeRejected(RocksDBConnection client) {
		try {
			client.close();
		} catch (Exception failure) {
			LoggerFactory.getLogger(DeferredRockserverConnection.class).warn("Failed to close deferred Rockserver client", failure);
		}
	}

	@Override
	public void close() throws IOException {
		RocksDBConnection existing;
		Attempt pending;
		synchronized (this) {
			closed = true;
			existing = connection;
			connection = null;
			pending = attempt;
			attempt = null;
		}
		if (pending != null) {
			pending.result.completeExceptionally(new IllegalStateException("Rockserver connection is closed"));
			pending.finished.complete(null);
		}
		if (existing != null) existing.close();
	}

	private static final class Attempt {
		final CompletableFuture<RocksDBConnection> result = new CompletableFuture<>();
		final CompletableFuture<Void> finished = new CompletableFuture<>();
		int waiters;
		boolean abandoned;
	}
}
