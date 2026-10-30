package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.unimi.dsi.fastutil.longs.LongRBTreeSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;
import java.io.Closeable;
import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.jetbrains.annotations.Nullable;

/**
 * Submits log entries to the current leader, from any node, batching the requests that arrive together.
 *
 * <p>Every proposal is a numbered {@link Request}. Requests wait in a queue while {@link #MAX_BATCHES_IN_FLIGHT}
 * batches are in flight, and then go together as one log entry (see {@link LogEntryCodec#encodeBatch}): the Raft
 * log, its fsync and the apply then cost one entry for the whole batch. A batch is resent (leader changes,
 * unreachable nodes, timeouts) with the same bytes until it gets an answer or the request timeout passes, and
 * applying a copy of a request that already took effect returns the recorded result instead of executing it again.
 * So a write takes effect at most once even when copies reach a new leader or a restarted node. The same holds for a
 * request whose result did not fit in its batch's reply: a copy of it sent alone gets that result. When this node is
 * the leader, an attempt goes straight to its own Raft server; otherwise it goes, through the Ratis client's RPC
 * layer, to the leader that this node's own Raft server knows.</p>
 *
 * <p>Every attempt of one batch is the same Ratis invocation (client id and call id). A leader that still has an
 * earlier attempt pending, or answered it, returns that attempt's reply from its retry cache instead of appending
 * the batch again. A leader that never saw it (a new leader, or after the cache entry expired) appends it again,
 * and the apply deduplicates its requests by sequence.</p>
 *
 * <p>Each remote attempt is an independent blocking call on its own virtual thread, not a request of the client's
 * ordered async stream: once one request of that stream runs out of retries, Ratis closes the stream for good, and
 * every later proposal of this node would fail without being sent.</p>
 *
 * <p>Transaction commits ({@link #proposeOnce}) have their own queue: they are only ever sent once, to this node's
 * own Raft server, and a conflict rejects only the commits that conflict: the others are sent again in the next
 * batch.</p>
 *
 * <p>A request whose caller has less time left than the request timeout is resent only until its caller's deadline,
 * minus {@link #CALLER_MARGIN_NANOS}. If it is still queued then, or its caller cancelled it before it was sent, it
 * leaves the queue without being sent: the caller gets {@code READ_DEADLINE_EXCEEDED}, and the write was not
 * applied. A batch is resent only until the earliest caller deadline among its requests. If that deadline ends the
 * batch without an answer, its other requests go back to the queue and are sent again in a new batch, under their
 * own deadlines (see {@link Lane#endUnanswered}).</p>
 */
final class Proposer implements Closeable {

	/** The leader rejected a transaction commit because one of its keys changed after its snapshot. */
	static final class ConflictException extends RocksDBRetryException {}

	private static final long NOT_READY_RETRY_MS = 50;
	private static final Duration MIN_ATTEMPT_TIMEOUT = Duration.ofSeconds(3);
	private static final long ATTEMPT_SLEEP_MS = 100;
	/** Batches of one queue in flight at once: later requests wait for one of them to finish, and then go together. */
	static final int MAX_BATCHES_IN_FLIGHT = 32;
	private static final int MAX_BATCH_REQUESTS = 4096;
	/** Bytes of entry bodies per batch; a single larger request goes alone. */
	private static final int MAX_BATCH_BYTES = 8 * 1024 * 1024;
	/**
	 * A request with less time left than this when its batch is formed is failed without being sent: it would get at
	 * most one hurried attempt, and its outcome would then be unknown instead of a clean "not applied".
	 */
	private static final long MIN_TIME_LEFT_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
	/**
	 * Taken off the time left to a caller (at most a tenth of it): a request still queued this long before its
	 * caller's deadline fails at once, so that the caller gets the clean "not applied" answer before its own deadline
	 * ends the call with an unknown outcome.
	 */
	private static final long CALLER_MARGIN_NANOS = TimeUnit.MILLISECONDS.toNanos(20);
	/** Time left to a caller without a deadline of its own: the request gets the node's request timeout. */
	static final long NO_CALLER_DEADLINE = Long.MAX_VALUE;
	/** Batch header, plus a sequence per request. */
	private static final int BATCH_HEADER_BYTES = 1 + 2 * Long.BYTES + Integer.BYTES;
	/** Largest entry body ({@link LogEntryCodec#encodeEntryBody}) that fits in a log entry alone. */
	static final long MAX_ENTRY_BODY_BYTES = RaftSettings.MAX_ENTRY_BYTES - BATCH_HEADER_BYTES - Long.BYTES;

	private final long proposerId = new SecureRandom().nextLong();
	/** Guards {@link #nextSequence} and {@link #inFlight}. */
	private final Object numbering = new Object();
	private long nextSequence;
	/** Requests that have no final answer yet. */
	private final LongSortedSet inFlight = new LongRBTreeSet();
	private final RaftClient client;
	private final Supplier<RaftServer> localServer;
	private final RaftGroupId groupId;
	private final RaftPeerId self;
	/** Client id of every batch's Ratis invocation: separate from {@link #client}'s own, whose call ids are global. */
	private final ClientId invocationClientId = ClientId.randomId();
	private final AtomicLong callIds = new AtomicLong();
	private final ExecutorService sender = Executors.newThreadPerTaskExecutor(
			Thread.ofVirtual().name("rockserver-propose-", 0).factory());
	/** Fails queued requests at their caller's deadline, also while no batch slot frees up to look at the queue. */
	private final ScheduledThreadPoolExecutor expiries = new ScheduledThreadPoolExecutor(1,
			Thread.ofPlatform().daemon().name("rockserver-propose-expiry").factory());
	private final long requestTimeoutNanos;
	/** How long one client attempt waits for its reply. */
	private final long attemptTimeoutNanos;
	private final NodeMetrics metrics;
	private final Lane anyLeader;
	private final Lane localCommits;
	private volatile boolean closed;

	Proposer(RaftGroup group, RaftProperties properties, NodeConfig config, Supplier<RaftServer> localServer,
			NodeMetrics metrics) {
		this.metrics = metrics;
		this.anyLeader = new Lane(false);
		this.localCommits = new Lane(true);
		this.requestTimeoutNanos = config.requestTimeout().toNanos();
		this.expiries.setRemoveOnCancelPolicy(true);
		this.localServer = localServer;
		this.groupId = group.getGroupId();
		this.self = RaftPeerId.valueOf(config.nodeId());
		var attemptProperties = new RaftProperties(properties);
		// Long enough that a slow but healthy commit is not resent: a resend is safe, but re-replicates the entry.
		var attemptTimeout = config.electionTimeoutMax().multipliedBy(2);
		if (attemptTimeout.compareTo(MIN_ATTEMPT_TIMEOUT) < 0) {
			attemptTimeout = MIN_ATTEMPT_TIMEOUT;
		}
		this.attemptTimeoutNanos = attemptTimeout.toNanos();
		RaftClientConfigKeys.Rpc.setRequestTimeout(attemptProperties, RaftSettings.duration(attemptTimeout));
		// Attempts go through the client's RPC layer, one call each; the batch is resent until its deadline.
		this.client = RaftClient.newBuilder()
				.setClientId(ClientId.randomId())
				.setRaftGroup(group)
				.setProperties(attemptProperties)
				.setRetryPolicy(RetryPolicies.noRetry())
				.build();
	}

	RaftClient client() {
		return client;
	}

	/** {@link #propose(LogEntry, long)} for a request without a caller deadline. */
	CompletableFuture<Object> propose(LogEntry entry) {
		return propose(entry, NO_CALLER_DEADLINE);
	}

	/**
	 * Proposes {@code entry} and completes with its result. {@code callerTimeLeftNanos} is what is left of the
	 * caller's timeout ({@link #NO_CALLER_DEADLINE} if none). Fails with a {@link RocksDBException}: the result's own
	 * error, {@link RocksDBRetryException} if the leader refused it (stale transaction), {@code READ_DEADLINE_EXCEEDED}
	 * if the caller's deadline came before it was sent or before a leader accepted it (not applied), or
	 * {@code SERVER_OVERLOADED} when no leader answered in time; then the outcome is unknown, unless the message says
	 * it was not applied.
	 */
	CompletableFuture<Object> propose(LogEntry entry, long callerTimeLeftNanos) {
		return submit(entry, anyLeader, callerTimeLeftNanos);
	}

	/**
	 * Proposes a transaction commit to this node's own Raft server, which must be the leader that opened it, in a
	 * single attempt: a resend could reach another leader, which would refuse it although the first attempt may
	 * have been committed. Fails with {@link ConflictException} on a conflict, {@link RocksDBRetryException} if it
	 * was refused before being appended, {@code READ_DEADLINE_EXCEEDED} if the caller's deadline came before it was
	 * sent, and {@code SERVER_OVERLOADED} if its outcome is unknown.
	 */
	CompletableFuture<Object> proposeOnce(LogEntry.Guarded entry, long callerTimeLeftNanos) {
		return submit(entry, localCommits, callerTimeLeftNanos);
	}

	/** A request waiting for its answer. */
	private static final class Pending {

		private static final int QUEUED = 0;
		private static final int SENT = 1;
		private static final int ABANDONED = 2;

		final long sequence;
		final byte[] body;
		final boolean batchable;
		/** When resends stop: the node's request timeout, or the caller's deadline if earlier. */
		final long deadlineNanos;
		/** The deadline is the caller's: a request still queued then fails as not applied. */
		final boolean callerDeadline;
		/**
		 * Least time left before {@link #deadlineNanos} to be sent at all (see {@link #MIN_TIME_LEFT_NANOS}); for a
		 * caller's deadline, a quarter of the caller's time at most, so that a short deadline still gets a chance.
		 */
		final long minTimeLeftNanos;
		/** Leader epoch of a transaction commit, -1 otherwise: commits of different epochs never share an entry. */
		final long epoch;
		/** When it last entered the queue. */
		volatile long queuedNanos = System.nanoTime();
		/**
		 * Set when its batch's reply had no room for its result ({@link ApplyResult#RESEND_ALONE}): it took effect, and
		 * its recorded result is fetched instead, see {@link Lane#complete}. A data write is sent again, sharing its
		 * batch only with other such copies, at most {@link #copyBatchLimit} of them: each of them only reads back its
		 * record.
		 */
		volatile boolean resentAlone;
		/**
		 * Copies in a batch led by this one: as many results as the last reply carried, and at least one, which always
		 * fits. The copies then go as several batches in flight together, instead of one batch whose reply again
		 * carries only what fits (possibly nothing), one round trip after another.
		 */
		volatile int copyBatchLimit = 1;
		/**
		 * Set when it went back to the queue after a batch whose copy may have been appended (see
		 * {@link Lane#endUnanswered}): if it then fails without an answer, its outcome is unknown, never "not applied".
		 */
		volatile boolean maybeAppended;
		/** Completed only by the proposer; callers get a copy, so that cancelling theirs cannot complete this one. */
		final CompletableFuture<Object> result = new CompletableFuture<>();
		/** QUEUED until it is taken into a batch (SENT), or until its caller gives up on it first (ABANDONED). */
		private final AtomicInteger state = new AtomicInteger(QUEUED);

		Pending(long sequence, byte[] body, boolean batchable, long deadlineNanos, boolean callerDeadline,
				long minTimeLeftNanos, long epoch) {
			this.sequence = sequence;
			this.body = body;
			this.batchable = batchable;
			this.deadlineNanos = deadlineNanos;
			this.callerDeadline = callerDeadline;
			this.minTimeLeftNanos = minTimeLeftNanos;
			this.epoch = epoch;
		}

		boolean isQueued() {
			return state.get() == QUEUED;
		}

		/** Marks it as taken into a batch; false if it was abandoned, and must then not be sent. */
		boolean markSent() {
			return state.get() == SENT || state.compareAndSet(QUEUED, SENT);
		}

		/**
		 * Back in the queue after a batch that no leader appended: as if it was never sent, so that its caller's
		 * deadline or cancellation can again take it out of the queue.
		 */
		void unsend() {
			state.set(QUEUED);
		}

		/**
		 * Fails it with {@code error} if it was never sent, so that it never will be; does nothing once it was sent,
		 * since it may then take effect. It stays in its queue until a batch is formed, which skips it.
		 */
		void abandon(Throwable error) {
			if (state.compareAndSet(QUEUED, ABANDONED)) {
				result.completeExceptionally(error);
			}
		}

		/** True if it goes in a batch of its own. */
		boolean alone() {
			return !batchable;
		}

		/** Completes it with {@code result}, its final answer. */
		void complete(ApplyResult result) {
			switch (result) {
				case ApplyResult.Ok ok -> this.result.complete(ok.value());
				case ApplyResult.Failed failed when failed.type() == RocksDBErrorType.UPDATE_RETRY ->
						this.result.completeExceptionally(new RocksDBRetryException());
				case ApplyResult.Failed failed -> this.result.completeExceptionally(failed.toException());
				case ApplyResult.ResendAlone _ -> fail(unavailable(new IllegalStateException(
						"The reply of a request sent alone has no room for its result")));
			}
		}

		/**
		 * Fails it with {@code error}. A request resent alone already took effect, so whatever ended its resend, it
		 * fails saying that it was applied.
		 */
		void fail(RocksDBException error) {
			result.completeExceptionally(resentAlone ? RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
					"The write was applied, but its result was not received: " + error.getMessage()) : error);
		}
	}

	/** Every request submitted and not answered yet, so that close() can answer them all. */
	private final java.util.Set<Pending> live = java.util.concurrent.ConcurrentHashMap.newKeySet();

	private CompletableFuture<Object> submit(LogEntry entry, Lane lane, long callerTimeLeftNanos) {
		byte[] body;
		try {
			body = LogEntryCodec.encodeEntryBody(entry);
		} catch (IllegalArgumentException ex) {
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.INVALID_REQUEST, ex.getMessage()));
		}
		if (body.length > MAX_ENTRY_BODY_BYTES) {
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
					"Write too large for one replicated entry: " + body.length + " bytes"));
		}
		if (closed) {
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
					"The node is stopping"));
		}
		if (callerTimeLeftNanos <= 0) {
			return CompletableFuture.failedFuture(callerDeadlinePassed());
		}
		long now = System.nanoTime();
		boolean callerDeadline = callerTimeLeftNanos < requestTimeoutNanos;
		long deadlineNanos = callerDeadline
				? now + callerTimeLeftNanos - Math.min(CALLER_MARGIN_NANOS, callerTimeLeftNanos / 10)
				: now + requestTimeoutNanos;
		long sequence;
		synchronized (numbering) {
			sequence = nextSequence++;
			inFlight.add(sequence);
		}
		var pending = new Pending(sequence, body, Applier.isBatchable(entry), deadlineNanos, callerDeadline,
				callerDeadline ? Math.min(MIN_TIME_LEFT_NANOS, callerTimeLeftNanos / 4) : MIN_TIME_LEFT_NANOS,
				entry instanceof LogEntry.Guarded guarded ? guarded.leaderEpoch() : -1);
		live.add(pending);
		pending.result.whenComplete((_, _) -> {
			live.remove(pending);
			synchronized (numbering) {
				inFlight.remove(sequence);
			}
		});
		lane.enqueue(pending);
		if (closed) {
			// close() may have swept the queues before this request was added.
			pending.result.completeExceptionally(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
					"The node is stopping"));
		}
		if (callerDeadline && pending.isQueued()) {
			// It waits for a batch slot: fail it at its deadline even if no slot frees up by then.
			expireWhenQueued(pending);
		}
		var copy = pending.result.copy();
		copy.whenComplete((_, _) -> {
			if (copy.isCancelled()) {
				pending.abandon(new CancellationException("The caller cancelled the write before it was sent"));
			}
		});
		return copy;
	}

	private void expireWhenQueued(Pending pending) {
		try {
			var expiry = expiries.schedule(() -> pending.abandon(callerDeadlinePassed()),
					pending.deadlineNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
			pending.result.whenComplete((_, _) -> expiry.cancel(false));
		} catch (RejectedExecutionException stopping) {
			// close() answers every request.
		}
	}

	private static RocksDBException callerDeadlinePassed() {
		return RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
				"The request deadline came before the write was sent; it was not applied");
	}

	/** For a request that was sent, and refused before the append by every leader until its caller's deadline. */
	private static RocksDBException callerDeadlinePassedUnaccepted() {
		return RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
				"The request deadline came before a leader accepted the write; it was not applied");
	}

	private static RocksDBException notAccepted() {
		return RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
				"No leader accepted the write in time; it was not applied");
	}

	private static RocksDBException noAnswer(@Nullable Throwable cause) {
		return unavailable(cause != null ? cause
				: new java.util.concurrent.TimeoutException("No answer before the deadline"));
	}

	/** Every request numbered below this has its final answer: no copy of it will ever be sent again. */
	private long completedBelow() {
		synchronized (numbering) {
			return inFlight.isEmpty() ? nextSequence : inFlight.firstLong();
		}
	}

	/** One queue of requests, sent as batches with at most {@link #MAX_BATCHES_IN_FLIGHT} in flight. */
	private final class Lane {

		/** Sends transaction commits once to this node's Raft server, instead of to any leader until answered. */
		private final boolean commits;
		/** Guarded by this. */
		private final ArrayDeque<Pending> queue = new ArrayDeque<>();
		/** Guarded by this. */
		private int batchesInFlight;

		private final NodeMetrics.Series queueTime;
		private final NodeMetrics.Series roundTrip;
		private final NodeMetrics.Series batchSize;
		private final NodeMetrics.Series resends;
		private final NodeMetrics.Series aloneResends;

		Lane(boolean commits) {
			this.commits = commits;
			var name = commits ? "commits" : "writes";
			this.queueTime = metrics.duration(name + ".queue");
			this.roundTrip = metrics.duration(name + ".batch");
			this.batchSize = metrics.amount(name + ".batchSize");
			this.resends = metrics.amount(name + ".resends");
			this.aloneResends = metrics.amount(name + ".resentAlone");
		}

		void enqueue(Pending pending) {
			synchronized (this) {
				queue.addLast(pending);
			}
			dispatch();
		}

		/** Puts requests back at the head of the queue, in their order, to be sent in the next batch. */
		void requeue(List<Pending> pendings) {
			synchronized (this) {
				for (int i = pendings.size() - 1; i >= 0; i--) {
					queue.addFirst(pendings.get(i));
				}
			}
			dispatch();
		}

		/** Sends batches while there are queued requests and room in flight. */
		void dispatch() {
			while (true) {
				List<Pending> batch;
				synchronized (this) {
					if (queue.isEmpty() || batchesInFlight >= MAX_BATCHES_IN_FLIGHT) {
						return;
					}
					batch = take();
					if (batch.isEmpty()) {
						continue;
					}
					batchesInFlight++;
				}
				try {
					send(batch);
				} catch (RuntimeException ex) {
					fail(batch, ex instanceof RocksDBException rocks ? rocks : Proposer.unavailable(ex));
					finished();
				}
			}
		}

		/**
		 * The next batch: the first queued request and, if it is a data write, the data writes behind it, up to the
		 * batch limits. Requests past their deadline, or with less than {@link #MIN_TIME_LEFT_NANOS} left before the
		 * node's own deadline, fail as not applied, except those resent alone (see {@link Pending#fail}) and those
		 * that an earlier batch may have appended ({@link Pending#maybeAppended}). Abandoned requests are dropped.
		 */
		private List<Pending> take() {
			var batch = new ArrayList<Pending>();
			long now = System.nanoTime();
			int bytes = 0;
			// The smallest copyBatchLimit of the copies taken: each was marked by a reply that could carry that many.
			int copyLimit = Integer.MAX_VALUE;
			while (!queue.isEmpty()) {
				var next = queue.peekFirst();
				if (next.result.isDone()) {
					queue.pollFirst();
					continue;
				}
				if (closed || next.deadlineNanos - now < next.minTimeLeftNanos) {
					queue.pollFirst();
					next.fail(closed ? RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, "The node is stopping")
							: next.maybeAppended ? noAnswer(null)
							: next.callerDeadline ? callerDeadlinePassed()
							: notAccepted());
					continue;
				}
				if (!batch.isEmpty() && (next.alone() || batch.getFirst().alone()
						|| next.resentAlone != batch.getFirst().resentAlone || next.epoch != batch.getFirst().epoch
						|| (next.resentAlone && batch.size() >= Math.min(copyLimit, next.copyBatchLimit))
						|| batch.size() >= MAX_BATCH_REQUESTS || bytes + next.body.length > MAX_BATCH_BYTES)) {
					break;
				}
				queue.pollFirst();
				if (!next.markSent()) {
					// Abandoned just now.
					continue;
				}
				batch.add(next);
				bytes += next.body.length + Long.BYTES;
				if (next.resentAlone) {
					copyLimit = Math.min(copyLimit, next.copyBatchLimit);
				}
				if (next.alone()) {
					break;
				}
			}
			return batch;
		}

		private void finished() {
			synchronized (this) {
				batchesInFlight--;
			}
			dispatch();
		}

		private void send(List<Pending> batch) {
			var sequences = new long[batch.size()];
			var bodies = new ArrayList<byte[]>(batch.size());
			long earliest = batch.getFirst().deadlineNanos;
			long latest = earliest;
			long callers = Long.MAX_VALUE;
			for (int i = 0; i < sequences.length; i++) {
				var pending = batch.get(i);
				sequences[i] = pending.sequence;
				bodies.add(pending.body);
				earliest = Math.min(earliest, pending.deadlineNanos);
				latest = Math.max(latest, pending.deadlineNanos);
				if (pending.callerDeadline) {
					callers = Math.min(callers, pending.deadlineNanos);
				}
			}
			// Resent until the newest member's deadline, but at most one attempt beyond the oldest one's: its caller
			// should not wait twice its timeout. The oldest is not past its deadline (see take()). Never resent past
			// a member's caller deadline: that caller was told its write timed out, and may write it again. The
			// members with time left then go back to the queue (see endUnanswered).
			long deadline = Math.min(Math.min(latest, earliest + attemptTimeoutNanos), callers);
			var message = Message.valueOf(UnsafeByteOperations.unsafeWrap(
					LogEntryCodec.encodeBatch(proposerId, completedBelow(), sequences, bodies)));
			long now = System.nanoTime();
			for (var pending : batch) {
				queueTime.record(now - pending.queuedNanos);
			}
			batchSize.record(batch.size());
			// One Ratis invocation for every attempt of this batch; a batch formed later is another invocation.
			long callId = callIds.incrementAndGet();
			if (commits) {
				sendCommits(batch, message, callId, deadline, now);
			} else {
				attempt(batch, message, callId, deadline, now, false);
			}
		}

		/**
		 * One attempt at a batch of data writes, then resends until answered or {@code deadlineNanos}. Once an attempt
		 * ended without telling whether the entry was appended ({@code ambiguous}), a later refusal no longer means
		 * "not applied": the batch can only end with its results or an unknown outcome.
		 */
		private void attempt(List<Pending> batch, Message message, long callId, long deadlineNanos, long sentNanos,
				boolean ambiguous) {
			var reply = isLocalLeader() ? submitLocal(message, callId, deadlineNanos)
					: CompletableFuture.supplyAsync(() -> sendToLeader(message, callId), sender);
			reply.handleAsync((answer, error) -> {
				var refusal = error != null ? stateMachineException(error)
						: !answer.isSuccess() && answer.getException() instanceof StateMachineException e ? e : null;
				try {
					var cause = error != null ? ReplicatedConnection.unwrap(error)
							: !answer.isSuccess() ? answer.getException() : null;
					// No answer: no leader, unreachable, refused before the append, or a replica not ready.
					boolean unanswered = (refusal != null ? isNotReady(refusal) : error != null || !answer.isSuccess())
							&& !isReplyTooLarge(error);
					if (unanswered) {
						boolean nowAmbiguous = ambiguous || (refusal == null && !isRefusedBeforeAppend(cause));
						long delay = refusal != null || cause instanceof LeaderNotReadyException
								|| cause instanceof NotLeaderException || cause instanceof ResourceUnavailableException
								? NOT_READY_RETRY_MS : ATTEMPT_SLEEP_MS;
						// The resend itself must go out before the deadline, not only the decision to resend.
						if (closed || System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay) - deadlineNanos >= 0) {
							roundTrip.since(sentNanos);
							endUnanswered(batch, deadlineNanos, nowAmbiguous, refusal, cause);
							finished();
							return null;
						}
						// Send the same batch again. If an earlier copy took effect, applying this one returns its
						// recorded results.
						resends.record(1);
						CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS, sender).execute(() -> {
							if (closed || System.nanoTime() - deadlineNanos >= 0) {
								// Too late after all (the timer ran late): no copy goes out after the deadline.
								try {
									endUnanswered(batch, deadlineNanos, nowAmbiguous, refusal, cause);
								} catch (Throwable unexpected) {
									fail(batch, unavailable(unexpected));
								} finally {
									finished();
								}
								return;
							}
							attempt(batch, message, callId, deadlineNanos, sentNanos, nowAmbiguous);
						});
						return null;
					}
				} catch (Throwable unexpected) {
					fail(batch, unavailable(unexpected));
					finished();
					return null;
				}
				roundTrip.since(sentNanos);
				try {
					if (refusal != null) {
						for (var pending : batch) {
							// An earlier copy may have been appended, although this one was refused.
							pending.fail(ambiguous || pending.maybeAppended ? unavailable(refusal) : refusal(refusal));
						}
					} else if (isReplyTooLarge(error)) {
						fail(batch, replyTooLarge(error));
					} else {
						complete(batch, answer);
					}
				} catch (Throwable unexpected) {
					fail(batch, unavailable(unexpected));
				} finally {
					finished();
				}
				return null;
			}, sender);
		}

		/**
		 * Ends a batch of data writes whose resends stopped without an answer, at {@code deadlineNanos} or because the
		 * node is stopping. The batch's deadline is the earliest caller deadline among its members, if any (see
		 * {@link #send}), so the other members may still have time left. They go back to the head of the queue and
		 * are sent again in a new batch, a new Ratis invocation, under their own deadlines. That is safe also when a
		 * copy of this batch may have been appended ({@code ambiguous}): their sequences stay in flight, so applying a
		 * copy of one that took effect returns its recorded result. Such members are marked
		 * {@link Pending#maybeAppended}. The others were never appended, so their callers' deadlines and cancellations
		 * can again take them out of the queue. The members whose own deadline is the batch's, or too close to it
		 * (see {@link Pending#minTimeLeftNanos}), fail: as not applied if no copy was appended, with an unknown
		 * outcome otherwise.
		 */
		private void endUnanswered(List<Pending> batch, long deadlineNanos, boolean ambiguous,
				@Nullable StateMachineException refusal, @Nullable Throwable cause) {
			long now = System.nanoTime();
			var again = new ArrayList<Pending>(0);
			for (var pending : batch) {
				boolean appended = ambiguous || pending.maybeAppended;
				if (!closed && pending.deadlineNanos - deadlineNanos > 0
						&& pending.deadlineNanos - now >= pending.minTimeLeftNanos) {
					pending.maybeAppended = appended;
					pending.queuedNanos = now;
					if (!appended && !pending.resentAlone) {
						pending.unsend();
					}
					again.add(pending);
				} else if (appended) {
					pending.fail(noAnswer(cause));
				} else if (refusal != null) {
					pending.fail(refusal(refusal));
				} else {
					pending.fail(pending.callerDeadline ? callerDeadlinePassedUnaccepted() : notAccepted());
				}
			}
			if (!again.isEmpty()) {
				// Before finished(), so that the dispatch after it, which has this batch's slot, finds them queued.
				requeue(again);
				for (var pending : again) {
					if (pending.callerDeadline && pending.isQueued()) {
						expireWhenQueued(pending);
					}
				}
			}
		}

		/**
		 * The single attempt at a batch of transaction commits. A conflict rejects the whole log entry before it is
		 * appended: the commits that conflict fail, the others go again with the next batch.
		 */
		private void sendCommits(List<Pending> batch, Message message, long callId, long deadlineNanos,
				long sentNanos) {
			submitLocal(message, callId, deadlineNanos).handleAsync((answer, error) -> {
				roundTrip.since(sentNanos);
				try {
					var refusal = error != null ? stateMachineException(error)
							: !answer.isSuccess() && answer.getException() instanceof StateMachineException e ? e : null;
					if (refusal != null && isCause(refusal, RockserverStateMachine.ConflictException.class)) {
						var positions = RockserverStateMachine.ConflictException.positions(
								refusal.getCause().getMessage());
						if (positions == null) {
							fail(batch, new ConflictException());
						} else {
							var rest = new ArrayList<>(batch);
							for (int position : positions) {
								batch.get(position).result.completeExceptionally(new ConflictException());
								rest.set(position, null);
							}
							rest.removeIf(java.util.Objects::isNull);
							requeue(rest);
						}
					} else if (refusal != null) {
						fail(batch, refusal(refusal));
					} else if (isRefusedBeforeAppend(error != null ? ReplicatedConnection.unwrap(error)
							: answer.isSuccess() ? null : answer.getException())) {
						fail(batch, new RocksDBRetryException());
					} else if (error != null) {
						fail(batch, unavailable(error));
					} else if (!answer.isSuccess()) {
						// Such as NotLeaderException: a leader stepping down answers so for entries already appended,
						// which the next leader may still commit. The outcome is unknown, never "not applied".
						fail(batch, unavailable(answer.getException()));
					} else {
						complete(batch, answer);
					}
				} catch (Throwable unexpected) {
					fail(batch, unavailable(unexpected));
				} finally {
					finished();
				}
				return null;
			}, sender);
		}

		/**
		 * Completes each request of {@code batch} with its own result from {@code reply}. A request whose result did
		 * not fit in the reply ({@link ApplyResult#RESEND_ALONE}) took effect, and its sequence is still in flight, so
		 * its record is kept: a data write is sent again in a batch of such copies only, no larger than the number of
		 * results this reply carried (see {@link Pending#copyBatchLimit}), and applying a copy returns its record,
		 * which fits in a reply by itself. A transaction commit cannot be sent again (a copy would conflict with itself,
		 * or reach another leader), but this node's own Raft server applied it before answering: its record is read
		 * here.
		 */
		private void complete(List<Pending> batch, RaftClientReply reply) {
			if (!reply.isSuccess()) {
				fail(batch, unavailable(reply.getException()));
				return;
			}
			List<ApplyResult> results;
			try {
				results = ResultCodec.decodeAll(reply.getMessage().getContent().toByteArray());
			} catch (IllegalArgumentException ex) {
				fail(batch, unavailable(ex));
				return;
			}
			if (results.size() != batch.size()) {
				fail(batch, unavailable(new IllegalStateException(
						"Expected " + batch.size() + " results, got " + results.size())));
				return;
			}
			int carried = 0;
			for (var result : results) {
				if (!(result instanceof ApplyResult.ResendAlone)) {
					carried++;
				}
			}
			var resend = new ArrayList<Pending>(0);
			for (int i = 0; i < results.size(); i++) {
				var pending = batch.get(i);
				var result = results.get(i);
				if (!(result instanceof ApplyResult.ResendAlone) || batch.size() == 1) {
					pending.complete(result);
					continue;
				}
				pending.resentAlone = true;
				if (commits) {
					completeFromLocalRecord(pending);
				} else {
					pending.queuedNanos = System.nanoTime();
					pending.copyBatchLimit = Math.max(1, carried);
					resend.add(pending);
				}
			}
			if (!resend.isEmpty()) {
				aloneResends.record(resend.size());
				requeue(resend);
			}
		}

		private void completeFromLocalRecord(Pending pending) {
			ApplyResult recorded;
			try {
				recorded = localRecord(pending.sequence);
			} catch (IOException | RuntimeException ex) {
				pending.fail(unavailable(ex));
				return;
			}
			if (recorded == null) {
				pending.fail(unavailable(new IllegalStateException("No local record of transaction commit "
						+ pending.sequence)));
			} else {
				aloneResends.record(1);
				pending.complete(recorded);
			}
		}
	}

	private boolean isLocalLeader() {
		try {
			var info = localServer.get().getDivision(groupId).getInfo();
			return info.isLeader() && info.isLeaderReady();
		} catch (IOException | RuntimeException ex) {
			return false;
		}
	}

	/** The batch's Ratis invocation {@code callId}, addressed to {@code server}. */
	private RaftClientRequest invocation(Message message, long callId, RaftPeerId server) {
		return RaftClientRequest.newBuilder()
				.setClientId(invocationClientId)
				.setServerId(server)
				.setGroupId(groupId)
				.setCallId(callId)
				.setMessage(message)
				.setType(RaftClientRequest.writeRequestType())
				.build();
	}

	/** Submits straight to the local Raft server, with no client in between that could redirect it elsewhere. */
	private CompletableFuture<RaftClientReply> submitLocal(Message message, long callId, long deadlineNanos) {
		try {
			// The deadline only stops new copies (see attempt): a copy already submitted here runs to its end, so
			// wait for its answer at least as long as a remote attempt would, or a healthy commit of a batch holding
			// one short caller deadline would end as "outcome unknown" for every member.
			return localServer.get().submitClientRequestAsync(invocation(message, callId, self))
					.orTimeout(Math.max(deadlineNanos - System.nanoTime(), attemptTimeoutNanos), TimeUnit.NANOSECONDS);
		} catch (IOException ex) {
			return CompletableFuture.failedFuture(ex);
		}
	}

	/**
	 * Sends one attempt to the leader that this node's own Raft server knows, through the client's RPC layer, which
	 * waits for the reply at most the attempt timeout. Not through {@code client.io()}, which would make each attempt
	 * a new invocation. With no leader known (an election), nothing is sent: the attempt fails as refused before
	 * the append.
	 */
	private RaftClientReply sendToLeader(Message message, long callId) {
		var rpc = client.getClientRpc();
		RaftPeerId leader;
		try {
			var division = localServer.get().getDivision(groupId);
			leader = division.getInfo().getLeaderId();
			if (leader == null) {
				throw new LeaderNotReadyException(division.getMemberId());
			}
			var peer = division.getGroup().getPeer(leader);
			if (peer != null) {
				// A member that joined after the client was built; known members are kept as they are.
				rpc.addRaftPeers(List.of(peer));
			}
		} catch (IOException ex) {
			throw new CompletionException(ex);
		}
		try {
			return rpc.sendRequest(invocation(message, callId, leader));
		} catch (IOException ex) {
			// Resets the connection to that peer if the error calls for it, as the client's own calls do.
			rpc.handleException(leader, ex, false);
			throw new CompletionException(ex);
		}
	}

	/**
	 * Errors Ratis returns only before appending the entry: a leader not ready yet, or no permit for one more pending
	 * request. Not NotLeaderException, which a leader stepping down also returns for appended entries, nor the
	 * ResourceUnavailableException for "add a pending write request", raised after the append.
	 */
	private static boolean isRefusedBeforeAppend(@Nullable Throwable error) {
		return error instanceof LeaderNotReadyException
				|| (error instanceof ResourceUnavailableException
						&& String.valueOf(error.getMessage()).contains("acquire a pending write request"));
	}

	/**
	 * True if gRPC refused to deliver the reply because it is larger than the client's message limit. Only replies
	 * can be that large (requests are smaller than {@link RaftSettings#MAX_ENTRY_BYTES}), so the entry was applied:
	 * resending it would only get the same reply again until the deadline. {@link ResultCodec#encodeAll} keeps
	 * replies under the limit, so this is a last resort.
	 */
	private static boolean isReplyTooLarge(@Nullable Throwable error) {
		for (Throwable t = error; t != null; t = t.getCause()) {
			if (t instanceof StatusRuntimeException exception
					&& exception.getStatus().getCode() == Status.Code.RESOURCE_EXHAUSTED) {
				return true;
			}
			if (t.getCause() == t) {
				break;
			}
		}
		return false;
	}

	private static RocksDBException replyTooLarge(Throwable error) {
		return RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
				"The reply to the replicated write is too large to receive; the write was applied: "
						+ ReplicatedConnection.unwrap(error));
	}

	private static StateMachineException stateMachineException(Throwable error) {
		for (Throwable t = error; t != null; t = t.getCause()) {
			if (t instanceof StateMachineException exception) {
				return exception;
			}
		}
		return null;
	}

	private static boolean isNotReady(StateMachineException exception) {
		return isCause(exception, RockserverStateMachine.NotReadyException.class);
	}

	private static boolean isCause(StateMachineException exception, Class<?> type) {
		// Ratis rebuilds the cause on the client from its class name.
		return exception.getCause() != null && exception.getCause().getClass().getName().equals(type.getName());
	}

	/**
	 * The result this node's replica recorded for request {@code sequence}, or null if there is none. It is there
	 * for a request whose entry this node's own Raft server answered: the leader answers only once it applied it.
	 */
	private @Nullable ApplyResult localRecord(long sequence) throws IOException {
		return localServer.get().getDivision(groupId).getStateMachine() instanceof RockserverStateMachine stateMachine
				? stateMachine.recordedResult(proposerId, sequence) : null;
	}

	private static void fail(List<Pending> batch, RocksDBException error) {
		for (var pending : batch) {
			pending.fail(error);
		}
	}

	/**
	 * A state machine exception: the entry was refused before being appended (conflict, stale transaction, not
	 * ready), or anything else, such as the leader failing to apply it, which leaves its outcome unknown.
	 */
	private static RocksDBException refusal(StateMachineException exception) {
		if (isCause(exception, RockserverStateMachine.ConflictException.class)) {
			return new ConflictException();
		}
		if (isCause(exception, RockserverStateMachine.RetryableException.class)
				|| isCause(exception, RockserverStateMachine.NotReadyException.class)) {
			return new RocksDBRetryException();
		}
		return unavailable(exception);
	}

	static RocksDBException unavailable(Throwable error) {
		var cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
		return RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
				"Replicated write not acknowledged, its outcome is unknown: " + cause);
	}

	@Override
	public void close() throws IOException {
		closed = true;
		for (var lane : List.of(anyLeader, localCommits)) {
			List<Pending> queued;
			synchronized (lane) {
				queued = new ArrayList<>(lane.queue);
				lane.queue.clear();
			}
			fail(queued, RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, "The node is stopping"));
		}
		try (client) {
			expiries.shutdownNow();
			sender.shutdownNow();
		} finally {
			// Batches in flight lose their reply handlers with the executor: their outcome is unknown.
			fail(List.copyOf(live), RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
					"The node stopped before the write was acknowledged; its outcome is unknown"));
		}
	}
}
