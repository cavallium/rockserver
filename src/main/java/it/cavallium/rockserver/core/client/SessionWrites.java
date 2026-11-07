package it.cavallium.rockserver.core.client;

import io.grpc.Status;
import it.cavallium.buffer.Buf;
import io.grpc.Status.Code;
import it.cavallium.rockserver.core.common.ClientRequestId;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.KVBatchChunks;
import it.cavallium.rockserver.core.common.Keys;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
 * before an answer settled it. No copy is sent once the deadline or the window has passed, the first one included: a
 * write whose session opens later ends {@code READ_DEADLINE_EXCEEDED} flagged
 * {@linkplain RocksDBException#isNotApplied() not applied}. A write that gets no session at all was never sent, and
 * ends flagged not applied too: with the opening's own error, or {@code SERVER_OVERLOADED} if the opening's outcome
 * was unknown, if the deadline passed before an opening was sent, if a caller without a deadline got no answer to its
 * opening within the window, or if the connection closed first.</p>
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
	/**
	 * Counted bytes up to which a streamed {@code putMulti}, {@code deleteMulti} or {@code mergeMulti} is numbered
	 * ({@link #fitsNumberedMulti}), as the Rust client's {@code sessions::NUMBERED_MULTI_MAX_BYTES}: a quarter of the
	 * server's limit for a numbered multi write ({@code GrpcServerImpl.NUMBERED_MULTI_MAX_BYTES}, 16 MiB, which it
	 * refuses past as invalid). A larger call is sent unnumbered, streamed and applied item by item as before numbering,
	 * once: its size has no limit, and a lost call leaves its outcome unknown.
	 */
	static final long NUMBERED_MULTI_MAX_BYTES = KVBatchChunks.CHUNK_BYTES;
	/** Heap cost the server counts per item of a numbered multi write ({@code GrpcServerImpl.NUMBERED_MULTI_ITEM_HEAP_BYTES}). */
	static final long MULTI_ITEM_HEAP_BYTES = 96;
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

	/** Flagged not applied (R26-3 review, as the Rust client's {@code not_sent}): the write was never sent. */
	private static RocksDBException closedBeforeSent() {
		return RocksDBException.notApplied(RocksDBErrorType.SERVER_OVERLOADED,
				"The connection is closed; the write was not sent");
	}

	/** Set by {@link #close}: nothing is sent or scheduled any more. */
	private volatile boolean closed;
	/** Calls without their answer yet, which {@link #close} ends. */
	private final Set<Call> live = ConcurrentHashMap.newKeySet();

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
			} catch (RejectedExecutionException stopping) {
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
	private final LinkedHashMap<Long, Long> updates = new LinkedHashMap<>();
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
			// A multi write goes unary (putMultiList, for a put without results within its budget) or streamed; the server
			// collects a numbered stream whole, up to a limit, so a stream past NUMBERED_MULTI_MAX_BYTES is sent unnumbered.
			case RocksDBAPICommandSingle.PutMulti<?> c -> isReplicatedOrUpdateEnd(c.transactionOrUpdateId())
					&& (GrpcConnectionDelegate.sendsPutMultiList(c.keys(), c.values(), c.requestType())
							|| fitsNumberedMulti(c.keys(), c.values()));
			case RocksDBAPICommandSingle.Delete<?> c -> isReplicatedOrUpdateEnd(c.transactionOrUpdateId());
			case RocksDBAPICommandSingle.DeleteMulti<?> c -> isReplicatedOrUpdateEnd(c.transactionOrUpdateId())
					&& fitsNumberedMulti(c.keys(), null);
			case RocksDBAPICommandSingle.Merge<?> c -> isReplicatedOrUpdateEnd(c.transactionOrUpdateId());
			case RocksDBAPICommandSingle.MergeMulti<?> c -> isReplicatedOrUpdateEnd(c.transactionOrUpdateId())
					&& fitsNumberedMulti(c.keys(), c.values());
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

	/**
	 * True if a streamed multi write of {@code keys} (and {@code values}, null for a {@code deleteMulti}) counts at most
	 * {@link #NUMBERED_MULTI_MAX_BYTES}, as the server counts it ({@code GrpcServerImpl.numberedItemBytes}): per item
	 * its key and value bytes, {@link KVBatchChunks#ITEM_OVERHEAD_BYTES} per key segment and value (at least one) and
	 * {@link #MULTI_ITEM_HEAP_BYTES}. So it also holds far fewer items than the server's item limit.
	 */
	static boolean fitsNumberedMulti(List<Keys> keys, @Nullable List<Buf> values) {
		if (values != null && values.size() != keys.size()) {
			// Refused by the connection before anything is sent.
			return true;
		}
		long bytes = 0;
		for (int index = 0; index < keys.size(); index++) {
			int segments = 0;
			for (var key : keys.get(index).keys()) {
				bytes += key.size();
				segments++;
			}
			if (values != null) {
				bytes += values.get(index).size();
				segments++;
			}
			bytes += MULTI_ITEM_HEAP_BYTES + Math.max(1, segments) * KVBatchChunks.ITEM_OVERHEAD_BYTES;
			if (bytes > NUMBERED_MULTI_MAX_BYTES) {
				return false;
			}
		}
		return true;
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
		var current = new AtomicReference<Call>();
		BooleanSupplier anySent = () -> {
			if (appliedChunks.get() > 0) {
				return true;
			}
			var chunk = current.get();
			return chunk != null && chunk.sent;
		};
		var subscription = new AtomicReference<Disposable>();
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

	/** The transaction or update id of a write that can end an update, else 0. */
	private static long updateIdOf(RocksDBAPICommand<?, ?, ?> command) {
		return switch (command) {
			case RocksDBAPICommandSingle.Put<?> c -> c.transactionOrUpdateId();
			case RocksDBAPICommandSingle.PutMulti<?> c -> c.transactionOrUpdateId();
			case RocksDBAPICommandSingle.Delete<?> c -> c.transactionOrUpdateId();
			case RocksDBAPICommandSingle.DeleteMulti<?> c -> c.transactionOrUpdateId();
			case RocksDBAPICommandSingle.Merge<?> c -> c.transactionOrUpdateId();
			case RocksDBAPICommandSingle.MergeMulti<?> c -> c.transactionOrUpdateId();
			default -> 0;
		};
	}

	/**
	 * True if {@code command}, which {@link #numbers} does not number, ends an update this connection remembers: a
	 * multi write over {@link #NUMBERED_MULTI_MAX_BYTES} counted. It is sent unnumbered, and its update has to be
	 * forgotten ({@link #forgetUpdate}) once it ended, as a numbered one is, or the id stays numbered for a later
	 * transaction that gets it, whose staged writes would then be refused.
	 */
	boolean endsUpdateUnnumbered(RocksDBAPICommand<?, ?, ?> command) {
		long id = updateIdOf(command);
		return id != 0 && isReplicatedOrUpdateEnd(id);
	}

	/** Forgets the update that {@code command} ended, if it carries one. */
	void forgetUpdate(RocksDBAPICommand<?, ?, ?> command) {
		long id = updateIdOf(command);
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
				// Flagged (R26-3, as the Rust client): the opening's own error may not say it, as gRPC's deadline on the
				// opening, mapped to READ_DEADLINE_EXCEEDED, which would otherwise read as an unknown outcome.
				call.fail(cause instanceof RocksDBException rocks
						&& rocks.getErrorUniqueId() != RocksDBErrorType.OUTCOME_UNKNOWN
						? flaggedNotApplied(rocks) : RocksDBException.notApplied(RocksDBErrorType.SERVER_OVERLOADED,
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
		if (opening.isDone()) {
			// Ended by the window (below) while this attempt waited for its pause.
			return;
		}
		long remaining = call.deadlineNanos - System.nanoTime();
		if (remaining <= 0) {
			// As the Rust client's `remaining` before every opening: nothing is opened once the deadline or the window
			// has passed. The write was not sent.
			opening.completeExceptionally(notSent("the deadline came before a session was opened"));
			return;
		}
		CompletableFuture<Long> id;
		var context = call.context.value().withRequestId(null);
		try {
			id = opener.apply(context.hasTimeout() ? context.withTimeoutNanos(remaining) : context);
		} catch (RuntimeException ex) {
			id = CompletableFuture.failedFuture(ex);
		}
		if (!context.hasTimeout() && !id.isDone()) {
			// A caller without a deadline sends the opening with no gRPC deadline: the window ends it here, as the Rust
			// client's `within_window`, so that neither this write nor one waiting for this opening is bounded only by
			// keepalive. The opening may still succeed on the server: that leaves an unused session, which expires.
			CompletableFuture.delayedExecutor(remaining, TimeUnit.NANOSECONDS).execute(() -> opening
					.completeExceptionally(notSent("no answer to the opening of a session before the retry window ended")));
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

	/** The opening of a session ended without one: the write was not sent, so it was not applied. */
	private static RocksDBException notSent(String cause) {
		return RocksDBException.notApplied(RocksDBErrorType.SERVER_OVERLOADED,
				"Could not open a client session; the write was not sent: " + cause);
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
		if (System.nanoTime() - call.deadlineNanos >= 0) {
			// Nothing is sent once the deadline or the window has passed, for either kind of caller (R26-4, as the Rust
			// client's `remaining`): a session that opened late, or a resend after a long pause. A caller without a
			// deadline sends its copies with no gRPC deadline, so nothing else would stop a copy sent now, which, lost,
			// would end OUTCOME_UNKNOWN where not sending it is known not applied.
			if (call.maybeApplied) {
				call.failUnknown(RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED, "the deadline came"));
			} else {
				call.fail(RocksDBException.notApplied(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
						"The request deadline expired before the write was sent"));
			}
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

	/** {@code error}, flagged not applied if it is not already (an {@code UPDATE_RETRY} cannot be, and stays as it is). */
	private static RocksDBException flaggedNotApplied(RocksDBException error) {
		if (error.isNotApplied() || error instanceof RocksDBRetryException) {
			return error;
		}
		var flagged = RocksDBException.notApplied(error.getErrorUniqueId(), error.getMessage());
		flagged.initCause(error);
		return flagged;
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
