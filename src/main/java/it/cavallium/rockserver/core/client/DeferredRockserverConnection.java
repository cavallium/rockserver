package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.slf4j.LoggerFactory;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

/**
 * Defers the SDK's mandatory capability handshake until remote work is requested. The future of a single call, of
 * {@code flushWalAsync} and of {@code openClientSessionAsync} is a {@link DeferredCall}: its cancel and its outcome are
 * those of the real connection's call once that call is made (R47-2). A call that fails before then was never sent,
 * and its failure says so ({@link RocksDBException#isNotApplied()}): {@code READ_DEADLINE_EXCEEDED} for a deadline spent
 * while connecting, {@code CLIENT_CLOSED} for a closed client, as the direct client's (R48-4, R61-7). A failure of the
 * factory or of the handshake keeps its own exception, a transport failure that callers classify by its gRPC status.
 */
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

	/** Delegates once connected, as every call does: the default would answer NOT_IMPLEMENTED for a server with sessions. */
	@Override
	public CompletableFuture<Long> openClientSessionAsync(RequestContext context) {
		Objects.requireNonNull(context);
		var budget = new Budget(context);
		return DeferredCall.start(acquire(budget), client -> client.openClientSessionAsync(budget.remainingContext()));
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
				return DeferredCall.start(acquire(budget),
						client -> client.getAsyncApi(budget.remainingContext()).flushWalAsync());
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
				return DeferredCall.start(acquire(budget), client -> (CompletableFuture<R>)
						client.getAsyncApi(budget.remainingContext()).requestAsync(request));
			}
		};
	}

	private Mono<RocksDBConnection> acquire(Budget budget) {
		return Mono.defer(() -> acquireConnection().timeout(
				Duration.ofNanos(Math.min(CONNECT_TIMEOUT.toNanos(), budget.remainingNanos())),
				Mono.error(deadlineExceeded())));
	}

	/**
	 * The deadline came before a connection existed: nothing was sent, so the failure says so (R48-4), as the direct
	 * client's for a deadline that expired before the call (GrpcConnection).
	 */
	private static RuntimeException deadlineExceeded() {
		return RocksDBException.notApplied(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
				"Rockserver request deadline expired during connection initialization; the request was not sent");
	}

	/**
	 * The client is closed, and no connection carries the call: nothing was sent, so the failure says so (R48-4), as
	 * SessionWrites' for a write on a closed client. {@code CLIENT_CLOSED}, not {@code SERVER_OVERLOADED} as it was: a
	 * caller that waits out an overload without bound would send for ever a call that this client will never send
	 * (R61-7).
	 */
	private static RuntimeException closedBeforeSent() {
		return RocksDBException.notApplied(RocksDBErrorType.CLIENT_CLOSED,
				"Rockserver connection is closed; the request was not sent");
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
			return context.withTimeoutNanos(remainingNanos());
		}
	}

	private Mono<RocksDBConnection> acquireConnection() {
		return Mono.defer(() -> {
			Attempt current;
			boolean start;
			synchronized (this) {
				if (closed) return Mono.error(closedBeforeSent());
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
					var task = Schedulers.boundedElastic().schedule(() -> connect(current));
					boolean closedMeanwhile;
					synchronized (this) {
						current.task = task;
						closedMeanwhile = closed;
					}
					if (closedMeanwhile) task.dispose();
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
		Disposable task;
		synchronized (this) {
			closed = true;
			existing = connection;
			connection = null;
			pending = attempt;
			task = pending != null ? pending.task : null;
			attempt = null;
		}
		if (pending != null) {
			pending.result.completeExceptionally(closedBeforeSent());
			pending.finished.complete(null);
		}
		// Interrupts the handshake: nobody needs it now, and it holds a thread and keeps connecting until it ends.
		if (task != null) task.dispose();
		if (existing != null) existing.close();
	}

	/**
	 * The future of a single call made once the connection exists (R47-2). Until then a {@link #cancel} stops the
	 * acquisition, and nothing is sent. After, it is the call's own cancel, and this future ends as the call does: a
	 * numbered write that sent a copy refuses the cancel and ends OUTCOME_UNKNOWN (SessionWrites' {@code CallResult}), a
	 * call that already ended refuses it and its outcome comes through ({@link CancelPropagatingFuture}). A future made
	 * with {@code Mono.toFuture()} accepted every cancel, ending cancelled ("not applied") whatever the call did.
	 */
	private static final class DeferredCall<T> extends CompletableFuture<T> {
		private final Object lock = new Object();
		/** Guarded by {@link #lock}. */
		private Disposable acquisition;
		/** Guarded by {@link #lock}: the call, once made. */
		private CompletableFuture<T> call;
		/** Guarded by {@link #lock}: cancelled before the call was made, which then never is. */
		private boolean cancelledBeforeCall;
		/**
		 * Guarded by {@link #lock}: the call is being made. Only the thread making it can see this (the lock is held
		 * meanwhile), when the call completes some other future synchronously and one of its dependent stages cancels
		 * this one (review of R47, 1).
		 */
		private boolean making;
		/** Guarded by {@link #lock}: a cancel came while the call was being made; it is passed to the call once made. */
		private boolean cancelWhileMaking;
		/** Guarded by {@link #lock}: that cancel's argument. */
		private boolean interruptWhileMaking;

		static <T> CompletableFuture<T> start(Mono<RocksDBConnection> connection,
				Function<RocksDBConnection, CompletableFuture<T>> call) {
			var result = new DeferredCall<T>();
			var subscription = connection.subscribe(client -> result.connected(client, call), result::completeExceptionally);
			boolean cancelled;
			synchronized (result.lock) {
				cancelled = result.cancelledBeforeCall;
				if (!cancelled) result.acquisition = subscription;
			}
			if (cancelled) subscription.dispose();
			return result;
		}

		private void connected(RocksDBConnection client, Function<RocksDBConnection, CompletableFuture<T>> function) {
			CompletableFuture<T> made;
			boolean cancelMade;
			boolean interrupt;
			synchronized (lock) {
				if (cancelledBeforeCall) return;
				// Made under the lock, so that a cancel from another thread either comes first (and nothing is sent) or
				// reaches the call. One from this thread while the call is made (a dependent stage run synchronously) is
				// remembered and passed to the call below.
				making = true;
				try {
					made = Objects.requireNonNull(function.apply(client), "call");
				} catch (Throwable failure) {
					made = CompletableFuture.failedFuture(failure);
				} finally {
					making = false;
				}
				call = made;
				cancelMade = cancelWhileMaking;
				interrupt = interruptWhileMaking;
			}
			var source = made;
			made.whenComplete((value, error) -> {
				if (error == null) {
					complete(value);
				} else if (source.isCancelled() && unwrap(error) instanceof CancellationException) {
					cancelOnly();
				} else {
					completeExceptionally(unwrap(error));
				}
			});
			// The cancel that came while the call was made: if the call takes it, this future ends cancelled through the
			// stage above; if it refuses it (it ended, or a numbered write sent a copy), its outcome comes through.
			if (cancelMade) made.cancel(interrupt);
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			if (isDone()) return isCancelled();
			CompletableFuture<T> made;
			Disposable pending = null;
			synchronized (lock) {
				made = call;
				if (made == null && making) {
					// Reentrant, from the thread making the call: the call may already have sent something, so this is not
					// a cancel before the call. It cannot be granted yet: it is passed to the call once made, and this
					// future ends as the call does.
					cancelWhileMaking = true;
					interruptWhileMaking |= mayInterruptIfRunning;
					return false;
				}
				if (made == null) {
					cancelledBeforeCall = true;
					pending = acquisition;
				}
			}
			if (made == null) {
				// Nothing was sent: the acquisition stops (its waiter leaves, as before) and this future is cancelled.
				if (pending != null) pending.dispose();
			} else if (!made.cancel(mayInterruptIfRunning) && !made.isCancelled()) {
				// The call refused the cancel (it ended, or a copy of a numbered write was sent): its outcome completes this.
				return false;
			}
			super.cancel(mayInterruptIfRunning);
			return isCancelled();
		}

		private void cancelOnly() {
			super.cancel(false);
		}

		private static Throwable unwrap(Throwable error) {
			return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
		}
	}

	private static final class Attempt {
		final CompletableFuture<RocksDBConnection> result = new CompletableFuture<>();
		final CompletableFuture<Void> finished = new CompletableFuture<>();
		int waiters;
		boolean abandoned;
		Disposable task;
	}
}
