package it.cavallium.rockserver.core.client;

import io.grpc.Status;
import io.grpc.Status.Code;
import it.cavallium.rockserver.core.common.ClientRequestId;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.KVBatchChunks;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
 * <p>Numbered: writes outside transactions, transaction commits, the write that ends an update opened through
 * this connection (whose id this class remembers from the {@code getForUpdate} answer), schema changes (column
 * creation and deletion, merge operator upload), range deletions, and batch writes, chunk by chunk
 * ({@link #sendBatch}) when the server numbers batches. A write staged in a transaction is not: sending it again could stage it twice. A write whose first copy is refused because its
 * session expired was not applied, and goes again under a new session; a copy sent again after an unknown outcome that
 * meets an expired session ends with {@code OUTCOME_UNKNOWN}.</p>
 *
 * <p>A caller that cancels the future of a write stops it: no copy is sent any more, and the copy in flight is
 * cancelled. If a copy was sent, the future does not become cancelled but ends with {@code OUTCOME_UNKNOWN}, since
 * that copy may have been applied; otherwise it is cancelled, and the write was not applied.</p>
 */
final class SessionWrites {

	private static final Logger LOG = LoggerFactory.getLogger(SessionWrites.class);

	/** How long a write of a caller without a deadline is sent again after an unknown outcome. */
	static final String RETRY_WINDOW_PROPERTY = "it.cavallium.rockserver.grpc.client.write-retry-window-ms";
	static final long DEFAULT_RETRY_WINDOW_MILLIS = 30_000;
	private static final long FIRST_PAUSE_NANOS = TimeUnit.MILLISECONDS.toNanos(50);
	private static final long MAX_PAUSE_NANOS = TimeUnit.SECONDS.toNanos(1);
	/** How long an update id is remembered: longer than any update lease. */
	private static final long UPDATE_MEMORY_NANOS = TimeUnit.MINUTES.toNanos(10);
	static final int MAX_REMEMBERED_UPDATES = 1 << 16;
	/** Extra wait of a synchronous caller beyond the deadline, for the answer that ends its write ({@link #syncWaitNanos}). */
	static final long SYNC_WAIT_MARGIN_NANOS = TimeUnit.SECONDS.toNanos(5);
	/** {@link #syncWaitNanos} of a caller without a deadline: it waits until the write ends (still interruptible). */
	static final long NO_SYNC_WAIT_BOUND = Long.MAX_VALUE;

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

	/**
	 * The future a caller gets for a write. Cancelling it stops the write ({@code stop}): if a copy was sent
	 * ({@code sent}), the write may have been applied, so the future is not cancelled but ends with
	 * {@code OUTCOME_UNKNOWN}, and {@code cancel} returns false; otherwise it is cancelled, and the write was not applied.
	 */
	static final class CallResult extends CompletableFuture<Object> {
		private final BooleanSupplier sent;
		private final Runnable stop;
		/**
		 * Set by {@link #cancel} before it reads {@code sent}; a sender sets its {@code sent} flag before it reads this one
		 * ({@link SessionWrites#attempt}). With both volatile, either the caller sees the copy as sent (and the write ends
		 * unknown), or the sender sees the cancel and sends nothing.
		 */
		volatile boolean stopping;

		CallResult(BooleanSupplier sent, Runnable stop) {
			this.sent = sent;
			this.stop = stop;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			if (isDone()) {
				return isCancelled();
			}
			stopping = true;
			boolean cancelled;
			if (sent.getAsBoolean()) {
				completeExceptionally(RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN,
						"The caller cancelled the write after a copy of it was sent; it may or may not be applied"));
				cancelled = false;
			} else {
				cancelled = super.cancel(mayInterruptIfRunning);
			}
			stop.run();
			return cancelled;
		}
	}

	/** One numbered write and its copies. */
	private static final class Call {
		final BoundRequestContext context;
		final RocksDBAPICommandSingle<?> command;
		final long deadlineNanos;
		final CallResult result;
		/** The batch this write is a chunk of ({@link #sendBatch}), or null. */
		final @Nullable CallResult batch;
		/** The copy in flight, cancelled if the caller cancels the write. */
		volatile @Nullable CompletableFuture<?> inFlight;
		@Nullable Session session;
		long sequence;
		/** A copy was sent and its outcome is unknown: no "not applied" answer can end the call any more. */
		boolean maybeApplied;
		/** Times it was numbered again in a new session, after its first copy met an expired session. */
		int reissues;
		long pauseNanos = FIRST_PAUSE_NANOS;

		Call(BoundRequestContext context, RocksDBAPICommandSingle<?> command, long deadlineNanos,
				@Nullable CallResult batch) {
			this.context = context;
			this.command = command;
			this.deadlineNanos = deadlineNanos;
			this.batch = batch;
			this.result = new CallResult(() -> this.sent, this::stopped);
		}

		/** A copy of it was handed to the channel: if it ends without an answer, its outcome is unknown. */
		volatile boolean sent;

		/** The caller cancelled it: its number is final, and the copy in flight is cancelled. */
		private void stopped() {
			finish();
			var copy = inFlight;
			if (copy != null) {
				copy.cancel(true);
			}
		}

		/** It has its answer, or the caller cancelled it: nothing more is sent. */
		boolean ended() {
			if (result.isDone()) {
				finish();
				return true;
			}
			return false;
		}

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
	/** The server numbers batch calls ({@link it.cavallium.rockserver.core.common.RockserverCapabilities#numberedBatches()}). */
	private final boolean numberedBatches;
	private final Object sessionLock = new Object();
	/** The current session, or its opening; null before the first write and after it expired. Guarded by sessionLock. */
	private @Nullable CompletableFuture<Session> session;
	/**
	 * Update ids returned by {@code getForUpdate}, with the time they are forgotten, oldest first. At most
	 * {@link #MAX_REMEMBERED_UPDATES}: past that the oldest is forgotten ({@link #forgottenUpdates}), and the write that
	 * ends it is then sent unnumbered, once, as without sessions. Guarded by itself.
	 */
	private final java.util.LinkedHashMap<Long, Long> updates = new java.util.LinkedHashMap<>();
	/** Updates forgotten while open because {@link #updates} was full. */
	private final AtomicLong forgottenUpdates = new AtomicLong();
	private volatile long nextForgottenLogNanos;

	SessionWrites(Function<RequestContext, CompletableFuture<Long>> opener,
			BiFunction<BoundRequestContext, RocksDBAPICommandSingle<?>, CompletableFuture<?>> sender,
			Executor executor) {
		this(opener, sender, executor, true);
	}

	SessionWrites(Function<RequestContext, CompletableFuture<Long>> opener,
			BiFunction<BoundRequestContext, RocksDBAPICommandSingle<?>, CompletableFuture<?>> sender,
			Executor executor, boolean numberedBatches) {
		this.numberedBatches = numberedBatches;
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
			// Schema changes: a copy of a deletion would answer "not found" (or false) for a deletion that took effect, and
			// creation and upload, which the server already makes idempotent by name and schema or content, get resent
			// after an unknown outcome instead of failing with it.
			case RocksDBAPICommandSingle.CreateColumn _, RocksDBAPICommandSingle.DeleteColumn _,
					RocksDBAPICommandSingle.DeleteColumnIfExists _, RocksDBAPICommandSingle.UploadMergeOperator _ -> true;
			// Only on a server that numbers them: an older one ignores the id of a batch, and would apply a resent chunk
			// twice. Unnumbered, a batch is streamed as before and never resent.
			case RocksDBAPICommandSingle.PutBatch _, RocksDBAPICommandSingle.MergeBatch _ -> numberedBatches;
			// A copy of a range deletion sent after later writes into the range would delete them too.
			case RocksDBAPICommandSingle.DeleteRange _ -> true;
			default -> false;
		};
	}

	private boolean isReplicatedOrUpdateEnd(long id) {
		if (id == 0) {
			return true;
		}
		synchronized (updates) {
			return updates.containsKey(id);
		}
	}

	/** Updates forgotten while open because too many were remembered: their final write was not numbered. */
	long forgottenUpdates() {
		return forgottenUpdates.get();
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
			synchronized (updates) {
				updates.remove(close.updateId());
			}
		} else if (observes(command) && result instanceof UpdateContext<?> update && update.updateId() != 0) {
			long now = System.nanoTime();
			boolean forgotOne = false;
			synchronized (updates) {
				// Oldest first, and every id is remembered for the same time: the expired ones are at the head.
				for (Iterator<Long> it = updates.values().iterator(); it.hasNext(); ) {
					if (it.next() - now >= 0) {
						break;
					}
					it.remove();
				}
				updates.remove(update.updateId());
				if (updates.size() >= MAX_REMEMBERED_UPDATES) {
					var oldest = updates.keySet().iterator();
					oldest.next();
					oldest.remove();
					forgotOne = true;
				}
				updates.put(update.updateId(), now + UPDATE_MEMORY_NANOS);
			}
			if (forgotOne) {
				long forgotten = forgottenUpdates.incrementAndGet();
				if (now - nextForgottenLogNanos >= 0) {
					nextForgottenLogNanos = now + TimeUnit.MINUTES.toNanos(1);
					LOG.warn("More than {} updates are open on this connection: the oldest is forgotten, and the write that"
							+ " ends it is sent unnumbered, once ({} so far)", MAX_REMEMBERED_UPDATES, forgotten);
				}
			}
		}
	}

	/** Sends the numbered write {@code command}, again after unknown outcomes; see the class description. */
	CompletableFuture<Object> send(BoundRequestContext context, RocksDBAPICommandSingle<?> command) {
		if (command instanceof RocksDBAPICommandSingle.PutBatch || command instanceof RocksDBAPICommandSingle.MergeBatch) {
			return sendBatch(context, command);
		}
		return sendOne(context, command, null, null);
	}

	/**
	 * A batch write: its batches are split into the chunks a replicated server writes as one log entry each
	 * ({@link KVBatchChunks}), and each chunk is sent as a numbered write of its own, one at a time, in order, all
	 * within the call's deadline. A chunk, kept until its answer, can be sent again after an unknown outcome, which the
	 * stream of the whole call could not. Once a chunk took effect, a failure of a later one leaves the call partly
	 * applied: it ends with {@code INVALID_REQUEST}, as on the server, which callers do not resend as a whole.
	 */
	private CompletableFuture<Object> sendBatch(BoundRequestContext context, RocksDBAPICommandSingle<?> command) {
		boolean merge = command instanceof RocksDBAPICommandSingle.MergeBatch;
		long columnId;
		Publisher<KVBatch> batches;
		Function<KVBatch, RocksDBAPICommandSingle<?>> chunkCommand;
		if (command instanceof RocksDBAPICommandSingle.MergeBatch c) {
			columnId = c.columnId();
			batches = c.batchPublisher();
			chunkCommand = chunk -> new RocksDBAPICommandSingle.MergeBatch(c.columnId(), Flux.<KVBatch>just(chunk),
					c.mode());
		} else {
			var c = (RocksDBAPICommandSingle.PutBatch) command;
			columnId = c.columnId();
			batches = c.batchPublisher();
			chunkCommand = chunk -> new RocksDBAPICommandSingle.PutBatch(c.columnId(), Flux.<KVBatch>just(chunk),
					c.mode());
		}
		var appliedChunks = new AtomicLong();
		var appliedKeys = new AtomicLong();
		// A chunk was sent: cancelling the batch then ends it unknown (see CallResult). A chunk that was applied was sent;
		// the chunk in progress tells by its own flag, which it sets before it checks whether the batch is stopping.
		var current = new java.util.concurrent.atomic.AtomicReference<Call>();
		BooleanSupplier anySent = () -> {
			if (appliedChunks.get() > 0) {
				return true;
			}
			var chunk = current.get();
			return chunk != null && chunk.sent;
		};
		var subscription = new java.util.concurrent.atomic.AtomicReference<Disposable>();
		var result = new CallResult(anySent, () -> {
			var running = subscription.get();
			if (running != null) {
				running.dispose();
			}
		});
		var running = Flux.from(batches)
				.concatMapIterable(KVBatchChunks::split)
				// One chunk at a time: a later chunk must not overtake an earlier one writing the same key.
				.concatMap(chunk -> Mono.fromFuture(() -> sendOne(context, chunkCommand.apply(chunk), result, current::set))
						.then(Mono.fromRunnable(() -> {
							appliedChunks.incrementAndGet();
							appliedKeys.addAndGet(chunk.keys().size());
						})), 1)
				.then()
				.onErrorMap(error -> appliedChunks.get() == 0 ? unwrap(error) : RocksDBException.of(
						RocksDBErrorType.INVALID_REQUEST,
						(merge ? "mergeBatch" : "putBatch") + " on column " + columnId + " was partly applied: "
								+ appliedChunks.get() + " chunks (" + appliedKeys.get() + " keys) were applied before it"
								+ " failed, so it is not safe to resend as a whole: " + describe(unwrap(error))))
				.subscribe(null, result::completeExceptionally, () -> result.complete(null));
		subscription.set(running);
		if (result.isDone()) {
			// Cancelled while subscribing.
			running.dispose();
		}
		return result;
	}

	private CompletableFuture<Object> sendOne(BoundRequestContext context, RocksDBAPICommandSingle<?> command,
			@Nullable CallResult batch, java.util.function.@Nullable Consumer<Call> created) {
		long now = System.nanoTime();
		long remaining = context.remainingNanosAt(now);
		long deadline = remaining == RequestContext.NO_TIMEOUT ? now + retryWindowNanos : now + remaining;
		if (closed) {
			return CompletableFuture.failedFuture(closedBeforeSent());
		}
		var call = new Call(context, command, deadline, batch);
		if (created != null) {
			created.accept(call);
		}
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
			synchronized (updates) {
				updates.remove(id);
			}
		}
	}

	/** Numbers the write in the current session, opening one if needed, and sends its first copy. */
	private void start(Call call) {
		if (call.ended()) {
			return;
		}
		session(call).whenComplete((session, error) -> {
			if (call.ended()) {
				return;
			}
			if (error != null) {
				// Not sent: whatever the opening's own outcome, this write was not applied.
				var cause = unwrap(error);
				if ((isUnknownOutcome(cause) || isNotApplied(cause)) && !closed && canPause(call, call.pauseNanos)) {
					// The opening, maybe shared with other writes and sent within another write's deadline, ended without a
					// session but did not refuse sessions for good (an unknown outcome, overload, or the opener's deadline):
					// this one opens again, in its own time. A refusal such as NOT_IMPLEMENTED ends every waiting write.
					long pause = call.pauseNanos;
					call.pauseNanos = Math.min(MAX_PAUSE_NANOS, pause * 2);
					later(pause, () -> start(call), call::failClosed);
					return;
				}
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
		if (call.ended()) {
			return;
		}
		var id = new ClientRequestId(session.id, call.sequence, session.completedBelow());
		var context = new BoundRequestContext(call.context.value().withRequestId(id), call.context.startedNanos());
		if (closed) {
			call.failClosed();
			return;
		}
		CompletableFuture<?> answer;
		// Published before the cancel check below (see CallResult#stopping): a caller that cancels from now on sees the
		// copy as sent.
		call.sent = true;
		if (call.result.stopping || call.result.isDone()) {
			// The caller cancelled before this copy left: nothing is sent, and its cancel ends the write.
			call.ended();
			return;
		}
		if (call.batch != null && call.batch.stopping) {
			// The batch was cancelled before this chunk left: nothing is sent. The batch decided its own outcome; this
			// chunk only has to end (unknown, since its flag is set), which is never seen.
			call.result.cancel(true);
			return;
		}
		try {
			answer = sender.apply(context, call.command);
		} catch (RuntimeException ex) {
			answer = CompletableFuture.failedFuture(ex);
		}
		call.inFlight = answer;
		if (call.result.isDone()) {
			// Cancelled while this copy was being sent.
			answer.cancel(true);
		}
		answer.whenComplete((value, error) -> {
			call.inFlight = null;
			if (call.ended()) {
				return;
			}
			if (error == null) {
				call.succeed(value);
				return;
			}
			var cause = unwrap(error);
			// SESSION_EXPIRED: this copy was refused before it ran (a server sets the not-applied flag on it, and answers
			// OUTCOME_UNKNOWN instead when a copy of its own may have run). Copies this client sent earlier are the
			// client's to judge (maybeApplied), so the flag is not needed here; the final failure carries it.
			if (cause instanceof RocksDBException rocks
					&& rocks.getErrorUniqueId() == RocksDBErrorType.SESSION_EXPIRED) {
				call.finish();
				sessionExpired(session);
				if (call.maybeApplied) {
					call.failUnknown(cause);
				} else if (call.reissues++ == 0) {
					// This copy was the first, and it was not applied: send it as a new write.
					start(call);
				} else if (!closed && canPause(call, call.pauseNanos)) {
					// The new session expired too (a server whose sessions outlive no write): not in a tight loop.
					long pause = call.pauseNanos;
					call.pauseNanos = Math.min(MAX_PAUSE_NANOS, pause * 2);
					later(pause, () -> start(call), call::failClosed);
				} else {
					// Not applied, and no time left to send it again: flagged, also when a server without the trailer
					// sent it, so that the caller can tell it from an unknown outcome.
					call.fail(rocks.isNotApplied() ? rocks : RocksDBException.notApplied(rocks.getErrorUniqueId(),
							rocks.getMessage()));
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
			// A deadline error is unknown too, unless the server says the write was not applied
			// (RocksDBException#isNotApplied, carried by a trailer). gRPC's own deadline, which the client maps to
			// READ_DEADLINE_EXCEEDED, says nothing: the copy may have been applied.
			return rocks.getErrorUniqueId() == RocksDBErrorType.OUTCOME_UNKNOWN
					|| (rocks.getErrorUniqueId() == RocksDBErrorType.READ_DEADLINE_EXCEEDED && !rocks.isNotApplied());
		}
		var code = Status.fromThrowable(cause).getCode();
		// CANCELLED: a server whose API stops resets the calls still running. DEADLINE_EXCEEDED: gRPC's own deadline,
		// should it reach here unmapped.
		return code == Code.UNAVAILABLE || code == Code.CANCELLED || code == Code.DEADLINE_EXCEEDED;
	}

	/**
	 * The server says that this copy was not applied, but it might be if sent again: overload, or a failure flagged
	 * {@link RocksDBException#isNotApplied()} (a deadline that came before the write was sent). After an unknown outcome
	 * such an answer says nothing about the earlier copies, so the write is sent again.
	 */
	private static boolean isNotApplied(Throwable cause) {
		if (cause instanceof RocksDBRetryException) {
			return false;
		}
		return cause instanceof RocksDBException rocks
				&& (rocks.getErrorUniqueId() == RocksDBErrorType.SERVER_OVERLOADED || rocks.isNotApplied());
	}

	/**
	 * How long a synchronous caller waits for the answer to {@code context}'s write: its deadline plus
	 * {@link #SYNC_WAIT_MARGIN_NANOS}, saturated. The write ends by then on its own; the bound only keeps a lost callback
	 * from blocking the caller forever. A caller without a deadline is not bounded ({@link #NO_SYNC_WAIT_BOUND}): its
	 * copies have no gRPC deadline, and a batch sends its chunks one after the other with no bound on the whole, so
	 * any fixed bound would cut a long but healthy write and end it unknown.
	 */
	static long syncWaitNanos(BoundRequestContext context) {
		long remaining = context.remainingNanosAt(System.nanoTime());
		if (remaining == RequestContext.NO_TIMEOUT) {
			return NO_SYNC_WAIT_BOUND;
		}
		remaining = Math.max(0, remaining);
		// Saturated: a huge finite timeout (RequestContext clamps "forever" to Long.MAX_VALUE - 1) must not overflow.
		return remaining > NO_SYNC_WAIT_BOUND - SYNC_WAIT_MARGIN_NANOS ? NO_SYNC_WAIT_BOUND
				: remaining + SYNC_WAIT_MARGIN_NANOS;
	}

	/**
	 * Waits for {@code answer} for at most {@code waitNanos}. Past that, or if the thread is interrupted (its interrupt
	 * status is kept), the write is cancelled ({@link CallResult}): {@code OUTCOME_UNKNOWN} if a copy was sent.
	 */
	static Object await(CompletableFuture<Object> answer, long waitNanos) {
		try {
			return waitNanos == NO_SYNC_WAIT_BOUND ? answer.get() : answer.get(waitNanos, TimeUnit.NANOSECONDS);
		} catch (ExecutionException ex) {
			throw rethrow(ex.getCause());
		} catch (TimeoutException ex) {
			answer.cancel(true);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			answer.cancel(true);
		}
		if (answer.isCancelled()) {
			throw RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
					"No answer came within the deadline of the request");
		}
		try {
			return answer.join();
		} catch (CompletionException ex) {
			throw rethrow(ex.getCause());
		}
	}

	private static RuntimeException rethrow(Throwable cause) {
		cause = unwrap(cause);
		if (cause instanceof RuntimeException runtime) {
			return runtime;
		}
		return new CompletionException(cause);
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
