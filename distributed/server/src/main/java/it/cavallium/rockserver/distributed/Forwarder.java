package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Delete;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Get;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Merge;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Put;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static it.cavallium.rockserver.distributed.ReplicatedConnection.unwrap;

/**
 * Forwards commands to the leader: each attempt bounded by the caller's deadline and the request timeout, sent again
 * to whichever node leads when that is safe ({@link #isReplayable}), and otherwise failed as retryable or of unknown
 * outcome ({@link #unreachableFailure}).
 */
final class Forwarder {

	/** Pause before forwarding a request again after the leader could not be reached. */
	private static final long FORWARD_RETRY_MS = 50;

	private final NodeConfig config;
	private final RockserverStateMachine stateMachine;
	private final LeaderForwarder forwarder;
	private final LocalCommands localCommands;
	/** Serves a command again once this node became the leader meanwhile. */
	private final Router router;
	private final NodeMetrics.Series forwardTime;

	Forwarder(NodeConfig config,
			RockserverStateMachine stateMachine,
			LeaderForwarder forwarder,
			LocalCommands localCommands,
			Router router,
			NodeMetrics metrics) {
		this.config = config;
		this.stateMachine = stateMachine;
		this.forwarder = forwarder;
		this.localCommands = localCommands;
		this.router = router;
		this.forwardTime = metrics.duration("forward");
	}

	private boolean isLeader() {
		return localCommands.isLeader();
	}

	@SuppressWarnings("unchecked")
	CompletableFuture<?> forward(RequestContext context, RocksDBAPICommandSingle<?> command) {
		return forward(context, command, System.nanoTime() + config.requestTimeout().toNanos(), callerDeadline(context));
	}

	/** When the caller of a request that arrives now gives up, in {@link System#nanoTime()} units. */
	static long callerDeadline(RequestContext context) {
		long now = System.nanoTime();
		return context.hasTimeout() ? now + Math.min(context.timeoutNanos(), Long.MAX_VALUE / 4) : now + Long.MAX_VALUE / 4;
	}

	/**
	 * How long a forwarded call may take, at most: the request timeout, which also bounds the leader's own waits of
	 * a read, plus the key wait for an update, which waits for its key on the leader longer than that.
	 */
	private long forwardTimeoutNanos(RocksDBAPICommandSingle<?> command) {
		long timeout = config.requestTimeout().toNanos();
		if (command instanceof Get<?> c && c.requestType() instanceof RequestType.RequestForUpdate<?>
				&& c.transactionOrUpdateId() == 0) {
			return timeout + UpdateProtocol.UPDATE_LOCK_WAIT.toNanos();
		}
		return timeout;
	}

	/**
	 * {@code call}, failed with a {@link TimeoutException} if it does not end within {@code timeoutNanos}; the gRPC
	 * call is then cancelled.
	 */
	private static <T> CompletableFuture<T> withTimeout(CompletableFuture<T> call, long timeoutNanos) {
		call.orTimeout(timeoutNanos, TimeUnit.NANOSECONDS).whenComplete((_, error) -> {
			if (error != null && unwrap(error) instanceof TimeoutException) {
				// The core client's futures cancel their gRPC call; the leader also stops at the deadline it was given.
				call.cancel(false);
			}
		});
		return call;
	}

	/**
	 * Forwards {@code command} to the leader. When the leader cannot be reached (it died or lost leadership, and this
	 * node may not know yet), a command that can safely run again ({@link #isReplayable}) is sent again, to whichever
	 * node leads then, until {@code deadlineNanos}; any other command fails with a retryable error, or with an
	 * unknown outcome if it may have taken effect. Each attempt takes at most the caller's deadline or
	 * {@link #forwardTimeoutNanos}, whichever comes first, and the leader gets that deadline too: a leader that is alive
	 * but stuck does not hold the call, nor this node's caller, indefinitely.
	 */
	@SuppressWarnings("unchecked")
	CompletableFuture<?> forward(RequestContext context, RocksDBAPICommandSingle<?> command, long deadlineNanos,
			long callerDeadlineNanos) {
		return forwarder.leader(stateMachine).thenCompose(leader -> {
			if (leader.equals(config.nodeId())) {
				if (!isLeader()) {
					// Raft still names this node as leader, but it cannot serve (its replica failed).
					return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
							"This node cannot serve as leader"));
				}
				// We became the leader meanwhile.
				return (CompletableFuture<Object>) router.single(context, command);
			}
			long start = System.nanoTime();
			return forwarder.target(leader, stateMachine).thenCompose(target -> {
				long attempt = Math.min(forwardTimeoutNanos(command), callerDeadlineNanos - System.nanoTime());
				if (attempt <= 0) {
					return CompletableFuture.failedFuture(RocksDBException.notApplied(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
							"The request deadline passed before the request was forwarded to the leader"));
				}
				var bounded = context.withTimeoutNanos(attempt);
				var call = withTimeout(
						(CompletableFuture<Object>) target.connection().getAsyncApi(bounded).requestAsync(command), attempt);
				return call.whenComplete((_, error) -> {
					forwardTime.since(start);
					// Not after a timeout: the connection may be fine, and other calls use it.
					if (error != null && !(unwrap(error) instanceof TimeoutException) && isTransportFailure(unwrap(error))) {
						forwarder.invalidate(target);
					}
				});
			}).exceptionallyCompose(error -> {
				var cause = unwrap(error);
				boolean notSent = cause instanceof LeaderForwarder.NotConnectedException;
				boolean lost = !notSent && isTransportFailure(cause);
				if (!(notSent || lost || cause instanceof RocksDBRetryException)
						|| (!isReplayable(command, lost) && !notSent)) {
					return CompletableFuture.failedFuture(lost ? unreachableFailure(command, cause) : cause);
				}
				if (System.nanoTime() - deadlineNanos > 0) {
					// A retry signal from the leader (or a request never sent) is still the right answer.
					return CompletableFuture.failedFuture(lost ? RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
							"No leader could serve the request in time: " + cause) : cause);
				}
				return CompletableFuture.supplyAsync(() -> null,
								CompletableFuture.delayedExecutor(FORWARD_RETRY_MS, TimeUnit.MILLISECONDS))
						.thenCompose(_ -> (CompletableFuture<Object>) forward(context, command, deadlineNanos,
								callerDeadlineNanos));
			});
		});
	}

	/** The call reached the leader, or may have, and its connection then failed or the call timed out. */
	/**
	 * The forwarded call was lost: no answer, the connection failed ({@code UNAVAILABLE}), or the stream was reset
	 * ({@code CANCELLED}, as a leader whose API server stops with calls still running resets them).
	 */
	private static boolean isTransportFailure(Throwable cause) {
		if (cause instanceof TimeoutException) {
			return true;
		}
		var code = io.grpc.Status.fromThrowable(cause).getCode();
		return code == io.grpc.Status.Code.UNAVAILABLE || code == io.grpc.Status.Code.CANCELLED;
	}

	/**
	 * Commands that can run again on another leader after an attempt whose outcome is unknown: reads, and the
	 * opening of transactions and updates (one opened on a leader that is gone went away with it). An update is not
	 * opened again after an attempt that was {@code lost} in transport: the leader may be alive and have opened it,
	 * and that update would hold its key until its lease ends, also against the update sent again.
	 */
	private static boolean isReplayable(RocksDBAPICommandSingle<?> command, boolean lost) {
		return switch (command) {
			case Get<?> c when c.requestType() instanceof RequestType.RequestForUpdate<?> ->
					c.transactionOrUpdateId() == 0 && !lost;
			case Get<?> c -> c.transactionOrUpdateId() == 0;
			case OpenTransaction _ -> true;
			case RocksDBAPICommandSingle.GetMulti _, RocksDBAPICommandSingle.GetColumnId _,
					RocksDBAPICommandSingle.EstimateNumKeys _, RocksDBAPICommandSingle.CheckMergeOperator _,
					RocksDBAPICommandSingle.GetAllColumnDefinitions _ -> true;
			case RocksDBAPICommandSingle.ExistsMulti c -> c.transactionId() == 0;
			case RocksDBAPICommandSingle.ReduceRange<?> c -> c.transactionId() == 0;
			case RocksDBAPICommandSingle.GetRangePage<?> c -> c.transactionId() == 0;
			default -> false;
		};
	}

	/**
	 * The failure of a command that cannot run again after the leader became unreachable: retryable if it changes
	 * nothing by itself (it refers to a transaction, update or iterator that went away with the leader), of unknown
	 * outcome if it is a commit, which may have been applied.
	 */
	private static RuntimeException unreachableFailure(RocksDBAPICommandSingle<?> command, Throwable cause) {
		boolean commits = switch (command) {
			case CloseTransaction c -> c.commit();
			case Put<?> _, PutMulti<?> _, Delete<?> _, DeleteMulti<?> _, Merge<?> _, MergeMulti<?> _ -> true;
			default -> false;
		};
		if (commits) {
			return RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN,
					"The leader became unreachable, the outcome of the commit is unknown: " + cause);
		}
		return new RocksDBRetryException();
	}

	Flux<Object> forwardStream(RequestContext context, RocksDBAPICommandStream<?> command) {
		return forwardStream(context, command, System.nanoTime() + config.requestTimeout().toNanos());
	}

	/**
	 * Forwards a stream to the leader. As for {@link #forward}, a stream that was never sent, or a non-transactional
	 * stream that failed before its first element, is sent again to whichever node leads then, until
	 * {@code deadlineNanos}. A stream inside a transaction fails with a retryable error when the leader becomes
	 * unreachable; a stream cut after some elements fails with {@code SERVER_OVERLOADED}, since the caller already
	 * received part of it.
	 */
	@SuppressWarnings("unchecked")
	Flux<Object> forwardStream(RequestContext context, RocksDBAPICommandStream<?> command, long deadlineNanos) {
		return Mono.fromFuture(forwarder.leader(stateMachine)).flatMapMany(leader -> {
			if (leader.equals(config.nodeId())) {
				if (!isLeader()) {
					return Flux.error(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
							"This node cannot serve as leader"));
				}
				return Flux.from(router.stream(context, command));
			}
			var emitted = new AtomicBoolean();
			return Mono.fromFuture(forwarder.target(leader, stateMachine))
					.flatMapMany(target -> idleTimeout(Flux.from((Publisher<Object>) target.connection()
									.getAsyncApi(context)
									.requestAsync(command)), config.requestTimeout())
							.doOnNext(_ -> emitted.set(true))
							.doOnError(error -> {
								var cause = unwrap(error);
								if (!(cause instanceof TimeoutException) && isTransportFailure(cause)) {
									forwarder.invalidate(target);
								}
							}))
					.onErrorResume(error -> {
						var cause = unwrap(error);
						boolean notSent = cause instanceof LeaderForwarder.NotConnectedException;
						boolean lost = !notSent && isTransportFailure(cause);
						if (!(notSent || lost || cause instanceof RocksDBRetryException)) {
							return Flux.error(cause);
						}
						if (emitted.get()) {
							return Flux.error(lost ? RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
									"The leader became unreachable during the stream: " + cause) : cause);
						}
						if ((!notSent && isTransactional(command)) || System.nanoTime() - deadlineNanos > 0) {
							return Flux.error(lost ? new RocksDBRetryException() : cause);
						}
						return Mono.delay(Duration.ofMillis(FORWARD_RETRY_MS))
								.thenMany(Flux.defer(() -> forwardStream(context, command, deadlineNanos)));
					});
		});
	}

	/**
	 * {@code source}, failed with a {@link TimeoutException} (and cancelled) once it delivers no element for
	 * {@code idle} while its subscriber is waiting for one. A subscriber that requests nothing stops the clock: a slow
	 * consumer is not a stuck leader.
	 */
	static <T> Flux<T> idleTimeout(Flux<T> source, Duration idle) {
		return Flux.defer(() -> {
			var demand = new AtomicLong();
			var since = new AtomicLong(System.nanoTime());
			var timedOut = new AtomicBoolean();
			long idleNanos = idle.toNanos();
			var stuck = Flux.interval(idle.dividedBy(4).isZero() ? Duration.ofMillis(1) : idle.dividedBy(4))
					.filter(_ -> demand.get() > 0 && System.nanoTime() - since.get() > idleNanos)
					.take(1)
					.doOnNext(_ -> timedOut.set(true));
			return source
					.doOnNext(_ -> {
						demand.decrementAndGet();
						since.set(System.nanoTime());
					})
					.takeUntilOther(stuck)
					.concatWith(Mono.defer(() -> timedOut.get()
							? Mono.error(new TimeoutException("No element for " + idle))
							: Mono.empty()))
					.doOnRequest(n -> {
						long before = demand.getAndAccumulate(n, (a, b) -> a + b < 0 ? Long.MAX_VALUE : a + b);
						if (before <= 0) {
							since.set(System.nanoTime());
						}
					});
		});
	}

	/** Streams that iterate inside a transaction, which exists only on the leader that opened it. */
	private static boolean isTransactional(RocksDBAPICommandStream<?> command) {
		return command instanceof RocksDBAPICommandStream.GetRange<?> range && range.transactionId() != 0;
	}
}
