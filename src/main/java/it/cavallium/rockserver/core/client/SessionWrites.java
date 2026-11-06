package it.cavallium.rockserver.core.client;

import io.grpc.Status;
import io.grpc.Status.Code;
import it.cavallium.rockserver.core.common.ClientRequestId;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.UpdateContext;
import it.unimi.dsi.fastutil.longs.LongRBTreeSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * Writes that take effect at most once on a replicated server, across lost connections and leader changes, through
 * a client session (Raft dissertation, section 6.3). Used by {@link GrpcConnection} when the server advertises
 * {@link it.cavallium.rockserver.core.common.RockserverCapabilities#clientSessions()}.
 *
 * <p>The session is opened on the first write. Each write gets the next number of the session and keeps it: when its
 * outcome is unknown (the connection was lost, or the server answered {@code OUTCOME_UNKNOWN}), the same numbered
 * write is sent again, after a short pause, to whichever node the channel then reaches, until the caller's deadline
 * ({@link #RETRY_WINDOW_PROPERTY} for a caller without one). The server applies a numbered write at most once and
 * answers every copy with the first result. The caller gets {@code OUTCOME_UNKNOWN} only when the deadline came
 * before an answer settled it.</p>
 *
 * <p>Numbered: writes outside transactions, transaction commits, and the write that ends an update opened through
 * this connection (whose id this class remembers from the {@code getForUpdate} answer). A write staged in a
 * transaction is not: sending it again could stage it twice. A write whose first copy is refused because its session
 * expired was not applied, and goes again under a new session; a copy sent again after an unknown outcome that meets
 * an expired session ends with {@code OUTCOME_UNKNOWN}.</p>
 */
final class SessionWrites {

	/** How long a write of a caller without a deadline is sent again after an unknown outcome. */
	static final String RETRY_WINDOW_PROPERTY = "it.cavallium.rockserver.grpc.client.write-retry-window-ms";
	static final long DEFAULT_RETRY_WINDOW_MILLIS = 30_000;
	private static final long FIRST_PAUSE_NANOS = TimeUnit.MILLISECONDS.toNanos(50);
	private static final long MAX_PAUSE_NANOS = TimeUnit.SECONDS.toNanos(1);
	/** How long an update id is remembered: longer than any update lease. */
	private static final long UPDATE_MEMORY_NANOS = TimeUnit.MINUTES.toNanos(10);
	private static final int MAX_REMEMBERED_UPDATES = 1 << 16;

	/** The numbering of one session. */
	private static final class Session {
		final long id;
		private long next;
		private final LongRBTreeSet inFlight = new LongRBTreeSet();

		Session(long id) {
			this.id = id;
		}

		synchronized long allocate() {
			long sequence = next++;
			inFlight.add(sequence);
			return sequence;
		}

		synchronized void finish(long sequence) {
			inFlight.remove(sequence);
		}

		synchronized long completedBelow() {
			return inFlight.isEmpty() ? next : inFlight.firstLong();
		}
	}

	/** One numbered write and its copies. */
	private static final class Call {
		final BoundRequestContext context;
		final RocksDBAPICommandSingle<?> command;
		final long deadlineNanos;
		final CompletableFuture<Object> result = new CompletableFuture<>();
		@Nullable Session session;
		long sequence;
		/** A copy was sent and its outcome is unknown: no "not applied" answer can end the call any more. */
		boolean maybeApplied;
		long pauseNanos = FIRST_PAUSE_NANOS;

		Call(BoundRequestContext context, RocksDBAPICommandSingle<?> command, long deadlineNanos) {
			this.context = context;
			this.command = command;
			this.deadlineNanos = deadlineNanos;
		}

		/** A copy of it was handed to the channel: if it ends without an answer, its outcome is unknown. */
		volatile boolean sent;

		/** Its number is final: no copy of it is sent any more. */
		synchronized void finish() {
			if (session != null) {
				session.finish(sequence);
				session = null;
			}
		}

		void succeed(@Nullable Object value) {
			finish();
			result.complete(value);
		}

		void fail(Throwable error) {
			finish();
			result.completeExceptionally(error);
		}

		void failUnknown(Throwable cause) {
			fail(RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN,
					"The write was sent, and no answer settled its outcome before its deadline: " + describe(cause)));
		}

		/** Ends it because the connection closed: unknown if a copy was sent, else not applied. */
		void failClosed() {
			if (sent) {
				fail(RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN,
						"The connection closed before an answer settled the outcome of the write"));
			} else {
				fail(closedBeforeSent());
			}
		}
	}

	private static RocksDBException closedBeforeSent() {
		return RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
				"The connection is closed; the write was not sent");
	}

	/** Set by {@link #close}: nothing is sent or scheduled any more. */
	private volatile boolean closed;
	/** Calls without their answer yet, which {@link #close} ends. */
	private final java.util.Set<Call> live = ConcurrentHashMap.newKeySet();

	/**
	 * Ends every call still waiting, as {@link Call#failClosed}: a call waiting for a resend or a session would
	 * otherwise never complete once the connection's executor and channel are gone.
	 */
	void close() {
		closed = true;
		for (var call : List.copyOf(live)) {
			call.failClosed();
		}
	}

	/**
	 * Runs {@code task} after {@code pauseNanos} on {@link #executor}, or ends {@code onClosed} if the connection closed
	 * meanwhile. The timer is the JDK's shared one, which outlives the connection: a task refused by a closed executor
	 * is not dropped in silence.
	 */
	private void later(long pauseNanos, Runnable task, Runnable onClosed) {
		CompletableFuture.delayedExecutor(pauseNanos, TimeUnit.NANOSECONDS).execute(() -> {
			if (closed) {
				onClosed.run();
				return;
			}
			try {
				executor.execute(task);
			} catch (java.util.concurrent.RejectedExecutionException stopping) {
				onClosed.run();
			}
		});
	}

	private final Function<RequestContext, CompletableFuture<Long>> opener;
	private final BiFunction<BoundRequestContext, RocksDBAPICommandSingle<?>, CompletableFuture<?>> sender;
	private final Executor executor;
	private final long retryWindowNanos;
	private final Object sessionLock = new Object();
	/** The current session, or its opening; null before the first write and after it expired. Guarded by sessionLock. */
	private @Nullable CompletableFuture<Session> session;
	/** Update ids returned by {@code getForUpdate}, with the time they are forgotten. */
	private final Map<Long, Long> updates = new ConcurrentHashMap<>();

	SessionWrites(Function<RequestContext, CompletableFuture<Long>> opener,
			BiFunction<BoundRequestContext, RocksDBAPICommandSingle<?>, CompletableFuture<?>> sender,
			Executor executor) {
		this.opener = opener;
		this.sender = sender;
		this.executor = executor;
		this.retryWindowNanos = TimeUnit.MILLISECONDS.toNanos(Long.getLong(RETRY_WINDOW_PROPERTY,
				DEFAULT_RETRY_WINDOW_MILLIS));
	}

	/** True if {@code command} is numbered (see the class description). */
	boolean numbers(RocksDBAPICommand<?, ?, ?> command) {
		return switch (command) {
			case RocksDBAPICommandSingle.Put<?> c -> isReplicatedOrUpdateEnd(c.transactionOrUpdateId());
			case RocksDBAPICommandSingle.PutMulti<?> c -> isReplicatedOrUpdateEnd(c.transactionOrUpdateId());
			case RocksDBAPICommandSingle.Delete<?> c -> isReplicatedOrUpdateEnd(c.transactionOrUpdateId());
			case RocksDBAPICommandSingle.DeleteMulti<?> c -> isReplicatedOrUpdateEnd(c.transactionOrUpdateId());
			case RocksDBAPICommandSingle.Merge<?> c -> isReplicatedOrUpdateEnd(c.transactionOrUpdateId());
			case RocksDBAPICommandSingle.MergeMulti<?> c -> isReplicatedOrUpdateEnd(c.transactionOrUpdateId());
			case RocksDBAPICommandSingle.CloseTransaction c -> c.commit();
			default -> false;
		};
	}

	private boolean isReplicatedOrUpdateEnd(long id) {
		return id == 0 || updates.containsKey(id);
	}

	/**
	 * True if the answer to {@code command} has to be seen by {@link #observe}: a {@code getForUpdate} that opens an
	 * update, not one inside a transaction, which answers with the transaction's own id (the transaction's later writes
	 * are staged, and must not be numbered).
	 */
	static boolean observes(RocksDBAPICommand<?, ?, ?> command) {
		return command instanceof RocksDBAPICommandSingle.CloseFailedUpdate
				|| (command instanceof RocksDBAPICommandSingle.Get<?> get && get.transactionOrUpdateId() == 0
						&& get.requestType() instanceof RequestType.RequestForUpdate<?>);
	}

	/** Remembers the update a {@code getForUpdate} opened, forgets one that a close ended. */
	void observe(RocksDBAPICommand<?, ?, ?> command, @Nullable Object result) {
		if (command instanceof RocksDBAPICommandSingle.CloseFailedUpdate close) {
			updates.remove(close.updateId());
		} else if (observes(command) && result instanceof UpdateContext<?> update && update.updateId() != 0) {
			long now = System.nanoTime();
			if (updates.size() >= MAX_REMEMBERED_UPDATES) {
				for (Iterator<Long> it = updates.values().iterator(); it.hasNext(); ) {
					if (it.next() - now < 0) {
						it.remove();
					}
				}
			}
			if (updates.size() < MAX_REMEMBERED_UPDATES) {
				updates.put(update.updateId(), now + UPDATE_MEMORY_NANOS);
			}
		}
	}

	/** Sends the numbered write {@code command}, again after unknown outcomes; see the class description. */
	CompletableFuture<Object> send(BoundRequestContext context, RocksDBAPICommandSingle<?> command) {
		long now = System.nanoTime();
		long remaining = context.remainingNanosAt(now);
		long deadline = remaining == RequestContext.NO_TIMEOUT ? now + retryWindowNanos : now + remaining;
		if (closed) {
			return CompletableFuture.failedFuture(closedBeforeSent());
		}
		var call = new Call(context, command, deadline);
		live.add(call);
		call.result.whenComplete((_, _) -> {
			live.remove(call);
			forgetUpdate(command);
		});
		if (closed) {
			// close() may have run before the call was added.
			call.failClosed();
			return call.result;
		}
		start(call);
		return call.result;
	}

	private void forgetUpdate(RocksDBAPICommandSingle<?> command) {
		long id = switch (command) {
			case RocksDBAPICommandSingle.Put<?> c -> c.transactionOrUpdateId();
			case RocksDBAPICommandSingle.PutMulti<?> c -> c.transactionOrUpdateId();
			case RocksDBAPICommandSingle.Delete<?> c -> c.transactionOrUpdateId();
			case RocksDBAPICommandSingle.DeleteMulti<?> c -> c.transactionOrUpdateId();
			case RocksDBAPICommandSingle.Merge<?> c -> c.transactionOrUpdateId();
			case RocksDBAPICommandSingle.MergeMulti<?> c -> c.transactionOrUpdateId();
			default -> 0;
		};
		if (id != 0) {
			updates.remove(id);
		}
	}

	/** Numbers the write in the current session, opening one if needed, and sends its first copy. */
	private void start(Call call) {
		session(call).whenComplete((session, error) -> {
			if (error != null) {
				// Not sent: whatever the opening's own outcome, this write was not applied.
				var cause = unwrap(error);
				call.fail(cause instanceof RocksDBException rocks
						&& rocks.getErrorUniqueId() != RocksDBErrorType.OUTCOME_UNKNOWN
						? rocks : RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
								"Could not open a client session; the write was not sent: " + describe(cause)));
				return;
			}
			call.session = session;
			call.sequence = session.allocate();
			attempt(call, session);
		});
	}

	/** The current session, opened if there is none. Opening is retried like a write until the call's deadline. */
	private CompletableFuture<Session> session(Call call) {
		CompletableFuture<Session> opening;
		synchronized (sessionLock) {
			if (session != null && !session.isCompletedExceptionally()) {
				return session;
			}
			opening = new CompletableFuture<>();
			session = opening;
		}
		open(call, opening, FIRST_PAUSE_NANOS);
		return opening;
	}

	private void open(Call call, CompletableFuture<Session> opening, long pauseNanos) {
		CompletableFuture<Long> id;
		try {
			long remaining = call.deadlineNanos - System.nanoTime();
			var context = call.context.value().withRequestId(null);
			id = opener.apply(context.hasTimeout() ? context.withTimeoutNanos(Math.max(1, remaining)) : context);
		} catch (RuntimeException ex) {
			id = CompletableFuture.failedFuture(ex);
		}
		id.whenComplete((sessionId, error) -> {
			if (error == null) {
				opening.complete(new Session(sessionId));
				return;
			}
			var cause = unwrap(error);
			// Opening twice only leaves an unused session, which expires.
			if (isUnknownOutcome(cause) && canPause(call, pauseNanos)) {
				later(pauseNanos, () -> open(call, opening, Math.min(MAX_PAUSE_NANOS, pauseNanos * 2)),
						() -> opening.completeExceptionally(closedBeforeSent()));
				return;
			}
			opening.completeExceptionally(cause);
		});
	}

	private void sessionExpired(Session expired) {
		synchronized (sessionLock) {
			if (session != null && session.isDone() && !session.isCompletedExceptionally()
					&& session.join() == expired) {
				session = null;
			}
		}
	}

	private void attempt(Call call, Session session) {
		var id = new ClientRequestId(session.id, call.sequence, session.completedBelow());
		var context = new BoundRequestContext(call.context.value().withRequestId(id), call.context.startedNanos());
		if (closed) {
			call.failClosed();
			return;
		}
		CompletableFuture<?> answer;
		call.sent = true;
		try {
			answer = sender.apply(context, call.command);
		} catch (RuntimeException ex) {
			answer = CompletableFuture.failedFuture(ex);
		}
		answer.whenComplete((value, error) -> {
			if (error == null) {
				call.succeed(value);
				return;
			}
			var cause = unwrap(error);
			if (cause instanceof RocksDBException rocks
					&& rocks.getErrorUniqueId() == RocksDBErrorType.SESSION_EXPIRED) {
				call.finish();
				sessionExpired(session);
				if (call.maybeApplied) {
					call.failUnknown(cause);
				} else {
					// This copy was the first, and it was not applied: send it as a new write.
					start(call);
				}
				return;
			}
			boolean unknown = isUnknownOutcome(cause);
			if (!unknown && !(call.maybeApplied && isNotApplied(cause))) {
				// The answer: a result recorded by the server, or a failure that tells this write was not applied.
				call.fail(cause);
				return;
			}
			call.maybeApplied = true;
			if (closed) {
				call.failClosed();
				return;
			}
			if (!canPause(call, call.pauseNanos)) {
				call.failUnknown(cause);
				return;
			}
			long pause = call.pauseNanos;
			call.pauseNanos = Math.min(MAX_PAUSE_NANOS, pause * 2);
			later(pause, () -> attempt(call, session), call::failClosed);
		});
	}

	private static boolean canPause(Call call, long pauseNanos) {
		// Room for the pause and a copy that can still reach the server.
		return call.deadlineNanos - System.nanoTime() > pauseNanos + FIRST_PAUSE_NANOS;
	}

	/** The copy may have been applied, and the server did not say: a lost call, or {@code OUTCOME_UNKNOWN}. */
	static boolean isUnknownOutcome(Throwable cause) {
		if (cause instanceof RocksDBException rocks) {
			// A deadline error is unknown too, unless it says the write was not sent (gRPC's own deadline is one).
			return rocks.getErrorUniqueId() == RocksDBErrorType.OUTCOME_UNKNOWN
					|| (rocks.getErrorUniqueId() == RocksDBErrorType.READ_DEADLINE_EXCEEDED && !saysNotApplied(rocks));
		}
		var code = Status.fromThrowable(cause).getCode();
		// CANCELLED: a server whose API stops resets the calls still running.
		return code == Code.UNAVAILABLE || code == Code.CANCELLED;
	}

	/**
	 * The server says that this copy was not applied, but it might be if sent again: overload, or a deadline that came
	 * before the write was sent. After an unknown outcome such an answer says nothing about the earlier copies, so the
	 * write is sent again.
	 */
	private static boolean isNotApplied(Throwable cause) {
		if (cause instanceof RocksDBRetryException) {
			return false;
		}
		return cause instanceof RocksDBException rocks
				&& (rocks.getErrorUniqueId() == RocksDBErrorType.SERVER_OVERLOADED
						|| rocks.getErrorUniqueId() == RocksDBErrorType.READ_DEADLINE_EXCEEDED);
	}

	private static boolean saysNotApplied(RocksDBException error) {
		var message = String.valueOf(error.getMessage());
		return message.contains("not applied") || message.contains("already expired")
				|| message.contains("before downstream dispatch");
	}

	private static Throwable unwrap(Throwable error) {
		while ((error instanceof CompletionException || error instanceof ExecutionException) && error.getCause() != null) {
			error = error.getCause();
		}
		return error;
	}

	private static String describe(Throwable cause) {
		return cause.getMessage() != null ? cause.getMessage() : cause.toString();
	}
}
