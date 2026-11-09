package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.ClientRequestId;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.client.impl.RaftClientRpcWithProxy;
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
import org.apache.ratis.util.IOUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

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
 *
 * <p>Batches are taken on whichever thread dispatches, and encoded and submitted on {@link #submitter}: never on the
 * control plane, which handles the replies and the timers, nor on the proposing caller, whose {@code propose} returns
 * at once. A local submission can block inside Ratis during a disk stall (see {@link #submitter}). A data write on a
 * node that last saw it does not lead goes straight to {@link #sender} instead, which encodes it and sends it to the
 * leader (see {@link #leaderHint}).</p>
 */
final class Proposer implements Closeable {

	/** The leader rejected a transaction commit because one of its keys changed after its snapshot. */
	static final class ConflictException extends RocksDBRetryException {}

	private static final long NOT_READY_RETRY_MS = 50;
	/** Longest wait of a caller that gives up on a write while its batch is being encoded (see Pending#awaitEncoded). */
	static final long MAX_ENCODE_WAIT_NANOS = TimeUnit.SECONDS.toNanos(1);
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
	/**
	 * Largest entry body ({@link LogEntryCodec#encodeEntryBody}) that fits in a log entry alone: with the batch header,
	 * the request's sequence and client request id, and the leader's stamp.
	 */
	static final long MAX_ENTRY_BODY_BYTES = RaftSettings.MAX_ENTRY_BYTES - LogEntryCodec.STAMP_BYTES
			- LogEntryCodec.BATCH_HEADER_BYTES - LogEntryCodec.MAX_REQUEST_OVERHEAD_BYTES;
	/** System property: bytes of writes one node holds at most (see {@link #writeAdmissionBytes}). */
	static final String WRITE_ADMISSION_BYTES_PROPERTY = DistributedProperties.PREFIX + "write-admission-bytes";
	/** The name {@link #WRITE_ADMISSION_BYTES_PROPERTY} had before, still read. */
	static final String OLD_WRITE_ADMISSION_BYTES_PROPERTY = "it.cavallium.rockserver.replicated.write-admission-bytes";
	/** What a request costs on the heap besides its body: the request, its future, its queue node. */
	static final long REQUEST_OVERHEAD_BYTES = 256;
	/**
	 * Bytes of attempts one node has in flight at once. Ratis' pending-request byte limit on the leader
	 * ({@link RaftSettings}) is a multiple of this, so that the leader's own attempts never reach it, nor those of
	 * all the nodes of a cluster of up to {@link RaftSettings#NODES_UNDER_PENDING_LIMIT} nodes. At least one entry of
	 * the largest size must fit, and the first one refused while no other attempt holds the lane's reservation gets
	 * in ahead of later attempts that would leave it no room (R50-1, see {@link ByteAdmission}: one reservation, not
	 * a queue).
	 */
	static final long ATTEMPT_BYTES_IN_FLIGHT = 2L * RaftSettings.MAX_ENTRY_BYTES;

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
	/** Runs the blocking calls of remote attempts, which only wait for the network. */
	private final ExecutorService sender = Executors.newThreadPerTaskExecutor(
			Thread.ofVirtual().name("rockserver-propose-", 0).factory());
	/**
	 * The node's control plane ({@link NodeThreads#control()}), platform threads: handles the replies, schedules the
	 * resends and fails queued requests at their deadline, also while no batch slot frees up to look at the queue. It
	 * never waits behind virtual-thread carriers pinned by database calls, and never encodes nor submits a batch (see
	 * {@link #submitter}), so that a submission blocked in Ratis during a disk stall does not stop it.
	 */
	private final ScheduledExecutorService control;
	/**
	 * The node's submit threads ({@link NodeThreads#submit()}), platform threads: encode the batches and hand them to
	 * Ratis, this node's own Raft server or the sender for a remote leader. Never {@link #control} nor the proposing
	 * caller: in Ratis 3.3.1 {@code RaftServerImpl.executeSubmitClientRequestAsync} waits with
	 * {@code supplyAsync(..., clientExecutor).join()} for {@code writeAsyncImpl}, whose {@code appendTransaction} holds
	 * the server's lock around {@code ServerState.appendLog}, which ends in {@code SegmentedRaftLogWorker.addIOTask}:
	 * that waits, without giving up, for room in the log queue. A disk stall fills that queue (64 MiB, below the 160
	 * MiB pending-request limit, see {@link RaftSettings}), and every local submission then blocks for the whole stall.
	 */
	private final Executor submitter;
	/**
	 * Bytes of the requests this node holds, from {@link #submit} to their answer (see {@link #writeAdmissionBytes}).
	 * A request that does not fit is refused at once as not applied, instead of filling the heap.
	 */
	private final ByteAdmission admission = new ByteAdmission(writeAdmissionBytes());
	private final NodeMetrics.Series admissionRejects;
	private final long requestTimeoutNanos;
	/** How long one client attempt waits for its reply. */
	private final long attemptTimeoutNanos;
	private final NodeMetrics metrics;
	private final Lane anyLeader;
	private final Lane localCommits;
	private volatile boolean closed;
	/**
	 * Whether this node led, ready, the last time {@link #isLocalLeader()} looked: chooses, without waiting, where a data
	 * batch or its resend goes (R22-2). While it led, to {@link #submitter}, which may block in Ratis' submit; while it
	 * did not, to {@link #sender}, whose attempt only sends to the leader and cannot block in Ratis, so that a follower's
	 * batch is not handed over twice. {@link #isLocalLeader()} itself can wait while the Raft group starts, so it is
	 * never called on the dispatching thread. Only a hint: each attempt asks {@link #isLocalLeader()} again, and an
	 * attempt on {@link #sender} that finds this node leading hands its submission to {@link #submitter}.
	 */
	private volatile boolean leaderHint = true;

	/**
	 * @param control   platform threads for the reply handling and timers; the caller shuts them down after
	 *                  {@link #close()}
	 * @param submitter threads that encode batches and submit them, which may block inside Ratis (see
	 *                  {@link NodeThreads#submit()}); the caller shuts them down after {@link #close()}
	 */
	Proposer(RaftGroup group, RaftProperties properties, NodeConfig config, Supplier<RaftServer> localServer,
			NodeMetrics metrics, ScheduledExecutorService control, Executor submitter) {
		this.metrics = metrics;
		this.control = control;
		this.submitter = submitter;
		this.admissionRejects = metrics.amount("writes.admissionRejects");
		this.anyLeader = new Lane(false);
		this.localCommits = new Lane(true);
		this.requestTimeoutNanos = config.requestTimeout().toNanos();
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

	/** Bytes the byte admission holds now, for tests. */
	long admittedBytes() {
		return admission.used();
	}

	/** Run by a batch just before it encodes its requests, for tests that pause there. */
	volatile @Nullable Runnable beforeEncodeForTesting;

	/**
	 * Run by {@link Lane#take} with a request it drops from the queue unsent (past its deadline, or the node stopping),
	 * just after it left the queue and before it is failed, for tests that race its expiry timer there.
	 */
	volatile @Nullable Consumer<Pending> onTakeDropsForTesting;

	/** The byte admission itself, for tests that fill it to make this node refuse writes. */
	ByteAdmission admissionForTesting() {
		return admission;
	}

	/** Bytes the attempt permits of the lane of data writes reserve now (R50-1), for tests. */
	long dataLaneReservedBytes() {
		return anyLeader.permits.reservedBytes();
	}

	/** Batches in flight in both lanes now, for tests. */
	int batchesInFlight() {
		int count = 0;
		for (var lane : List.of(anyLeader, localCommits)) {
			synchronized (lane) {
				count += lane.batchesInFlight;
			}
		}
		return count;
	}

	/**
	 * Bytes of writes this node holds at most, queued or in flight: {@link #WRITE_ADMISSION_BYTES_PROPERTY} if set,
	 * otherwise an eighth of the maximum heap (a body is held about twice while in flight: in its request and in its
	 * batch). Never less than room for two entries of the largest size.
	 */
	static long writeAdmissionBytes() {
		return writeAdmissionBytes(DistributedProperties.get(WRITE_ADMISSION_BYTES_PROPERTY,
				OLD_WRITE_ADMISSION_BYTES_PROPERTY), Runtime.getRuntime().maxMemory());
	}

	static long writeAdmissionBytes(@Nullable String configured, long maxHeapBytes) {
		long bytes = configured != null && !configured.isBlank() ? Long.parseLong(configured.strip()) : maxHeapBytes / 8;
		return Math.max(bytes, 2 * (MAX_ENTRY_BODY_BYTES + REQUEST_OVERHEAD_BYTES));
	}

	private static final ThreadFactory CALLER_THREADS = Thread.ofVirtual().name("rockserver-propose-reply-", 0).factory();
	/**
	 * Completes the callers' futures, each on a virtual thread of its own, so that the callers' continuations never
	 * run on {@link #control}. Never refuses a task: it holds no resource to shut down.
	 */
	private static final Executor CALLERS = task -> CALLER_THREADS.newThread(task).start();

	/** {@link #propose(LogEntry, long)} for a request without a caller deadline. */
	CompletableFuture<Object> propose(LogEntry entry) {
		return propose(entry, NO_CALLER_DEADLINE);
	}

	/** {@link #propose(LogEntry, long)} for a request without a client session. */
	CompletableFuture<Object> propose(LogEntry entry, long callerTimeLeftNanos) {
		return propose(entry, callerTimeLeftNanos, null);
	}

	/**
	 * Proposes {@code entry} and completes with its result. {@code callerTimeLeftNanos} is what is left of the
	 * caller's timeout ({@link #NO_CALLER_DEADLINE} if none). Fails with a {@link RocksDBException}: the result's own
	 * error, {@link RocksDBRetryException} if the leader refused it (stale transaction), {@code READ_DEADLINE_EXCEEDED}
	 * if the caller's deadline came before it was sent or before a leader accepted it (not applied), or
	 * {@code SERVER_OVERLOADED} when no leader accepted it in time (not applied), or {@code OUTCOME_UNKNOWN} when it
	 * may have been appended and no answer came. {@code client} is its id in a client session, null if it has none:
	 * then the apply identifies it by that id, so a copy the client sends again through any node takes effect once.
	 */
	CompletableFuture<Object> propose(LogEntry entry, long callerTimeLeftNanos, @Nullable ClientRequestId client) {
		return submit(entry, anyLeader, callerTimeLeftNanos, client);
	}

	/**
	 * Proposes a transaction commit to this node's own Raft server, which must be the leader that opened it, in a
	 * single attempt: a resend could reach another leader, which would refuse it although the first attempt may
	 * have been committed. Fails with {@link ConflictException} on a conflict, {@link RocksDBRetryException} if it
	 * was refused before being appended, {@code READ_DEADLINE_EXCEEDED} if the caller's deadline came before it was
	 * sent, and {@code OUTCOME_UNKNOWN} if its outcome is unknown.
	 */
	CompletableFuture<Object> proposeOnce(LogEntry.Guarded entry, long callerTimeLeftNanos,
			@Nullable ClientRequestId client) {
		return submit(entry, localCommits, callerTimeLeftNanos, client);
	}

	/** A request waiting for its answer. */
	static final class Pending {

		private static final int QUEUED = 0;
		private static final int SENT = 1;
		private static final int ABANDONED = 2;

		final long sequence;
		/** Its id in a client session, which identifies it at apply instead of {@link #sequence}; null if none. */
		final @Nullable ClientRequestId client;
		/**
		 * Its entry, encoded only when its batch goes out, straight into the batch (see {@link Lane#send}). Until then
		 * it is the caller's; once encoded, it is its copy in that batch (slices of the batch's immutable bytes), so
		 * that a request sent again never reads its caller's buffers after the caller may have been answered. Null
		 * once abandoned: the queue may hold it a while longer, its entry need not (see {@link #abandon}).
		 */
		volatile @Nullable LogEntry entry;
		/** Encoded size of {@link #entry} ({@link LogEntryCodec#entrySize}), its weight in batches and admission. */
		final int bodySize;
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
		/** The future its caller got ({@link CallerCopy}), for tests. */
		volatile @Nullable CompletableFuture<Object> callerCopy;
		/** Completed only by the proposer; callers get a copy, so that cancelling theirs cannot complete this one. */
		final CompletableFuture<Object> result = new CompletableFuture<>();
		/** QUEUED until it is taken into a batch (SENT), or until its caller gives up on it first (ABANDONED). */
		private final AtomicInteger state = new AtomicInteger(QUEUED);

		Pending(long sequence, @Nullable ClientRequestId client, LogEntry entry, int bodySize, boolean batchable,
				long deadlineNanos, boolean callerDeadline, long minTimeLeftNanos, long epoch) {
			this.sequence = sequence;
			this.client = client;
			this.entry = entry;
			this.bodySize = bodySize;
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
			refusedOnce = true;
			state.set(QUEUED);
		}

		/** Back in the queue after it was taken into a batch that was never sent: as if it was never taken. */
		void untake() {
			state.set(QUEUED);
		}

		/** Set by {@link #unsend}: it was sent once, and refused before the append. */
		volatile boolean refusedOnce;

		/**
		 * Its expiry timer ({@link #expireWhenQueued}), the latest one armed; guarded by this. At most one is waiting at
		 * a time: its deadline never changes, so a requeue finds the one still waiting and arms none (R21-2).
		 */
		private @Nullable ScheduledFuture<?> expiry;

		/**
		 * Set from just before it is taken into a batch until that batch's encoding no longer reads its entry: its
		 * caller's buffers may be read while it is set. See {@link #awaitEncoded}.
		 */
		private volatile boolean encoding;

		/** Its batch is about to read its entry: set before {@link #markSent}, so a caller that sees it sent sees it. */
		void encodingStarted() {
			encoding = true;
		}

		/** Run once the encoding ends ({@link #whenEncoded}); guarded by this. */
		private @Nullable List<Runnable> afterEncoding;

		/** Its batch read its entry (or will not): a caller waiting in {@link #awaitEncoded} goes on. */
		void encodingEnded() {
			if (encoding) {
				List<Runnable> waiting;
				synchronized (this) {
					encoding = false;
					notifyAll();
					waiting = afterEncoding;
					afterEncoding = null;
				}
				if (waiting != null) {
					waiting.forEach(Runnable::run);
				}
			}
		}

		/**
		 * Runs {@code action} once no batch reads its entry: now if none does, otherwise on the thread that ends the
		 * encoding (the action should only hand work off). The non-blocking form of {@link #awaitEncoded}.
		 */
		void whenEncoded(Runnable action) {
			synchronized (this) {
				if (encoding) {
					if (afterEncoding == null) {
						afterEncoding = new ArrayList<>(1);
					}
					afterEncoding.add(action);
					return;
				}
			}
			action.run();
		}

		/** True while a batch may read its entry, for tests. */
		boolean isEncoding() {
			return encoding;
		}

		/**
		 * Waits, at most {@code maxNanos}, until no batch reads its entry any more. Called when its caller gives up on it
		 * (cancels, or times out) before its answer: a write already taken into a batch cannot be abandoned, and its
		 * caller, answered, may reuse its buffers at once, so it must not be answered while the batch still reads them.
		 * The wait is the encoding of one batch at most, and only in the short time between the batch taking the write
		 * and encoding it; afterwards the entry is the batch's own copy ({@link Lane#send}).
		 */
		void awaitEncoded(long maxNanos) {
			if (!encoding) {
				return;
			}
			long deadline = System.nanoTime() + maxNanos;
			boolean interrupted = false;
			synchronized (this) {
				while (encoding) {
					long left = deadline - System.nanoTime();
					if (left <= 0) {
						break;
					}
					try {
						TimeUnit.NANOSECONDS.timedWait(this, left);
					} catch (InterruptedException ex) {
						interrupted = true;
					}
				}
			}
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}

		/** Its failure when its caller's deadline comes while it is queued: it was not applied. */
		RocksDBException callerDeadlineFailure() {
			return refusedOnce ? callerDeadlinePassedUnaccepted() : callerDeadlinePassed();
		}

		/**
		 * Its failure when its deadline comes while it is queued, as {@link Lane#take} fails it: it was not applied. With
		 * no caller deadline, its deadline is the node's request timeout.
		 */
		RocksDBException deadlineFailure() {
			return callerDeadline ? callerDeadlineFailure() : notAccepted();
		}

		/**
		 * Fails it with {@code error} if it was never sent, so that it never will be; does nothing once it was sent,
		 * since it may then take effect. It stays in its queue until a batch is formed, which skips it. True if this
		 * call abandoned it.
		 */
		boolean abandon(Throwable error) {
			if (state.compareAndSet(QUEUED, ABANDONED)) {
				// Its body is given back to the admission now; the rest of its weight once it leaves its queue.
				entry = null;
				result.completeExceptionally(error);
				return true;
			}
			return false;
		}

		boolean isAbandoned() {
			return state.get() == ABANDONED;
		}

		/** True if it goes in a batch of its own. */
		boolean alone() {
			return !batchable;
		}

		/**
		 * Completes it with {@code result}, its final answer. {@code earlierCopy}: an earlier attempt of its batch may
		 * have been appended. A session that expired before this copy applied ({@code SESSION_EXPIRED}, not recorded)
		 * says nothing about an earlier copy, which may have taken effect: then the outcome is unknown (or, resent
		 * alone, applied with its result lost), never the final "not applied" that would make the client write again.
		 * Without an earlier copy, {@code SESSION_EXPIRED} carries the not-applied flag ({@link
		 * RocksDBException#isNotApplied()}): this copy, the only one, was refused before it ran.
		 */
		void complete(ApplyResult result, boolean earlierCopy) {
			switch (result) {
				case ApplyResult.Failed failed when failed.type() == RocksDBErrorType.SESSION_EXPIRED
						&& (earlierCopy || resentAlone) -> fail(unavailable(failed.toException()));
				// The only copy of this request, refused unexecuted: flagged, so that the client's trailer says so.
				case ApplyResult.Failed failed when failed.type() == RocksDBErrorType.SESSION_EXPIRED ->
						this.result.completeExceptionally(RocksDBException.notApplied(failed.type(), failed.message()));
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
	private final Set<Pending> live = ConcurrentHashMap.newKeySet();

	private CompletableFuture<Object> submit(LogEntry entry, Lane lane, long callerTimeLeftNanos,
			@Nullable ClientRequestId client) {
		// Its exact encoded size, checked as encoding would check it; it is encoded only when its batch goes out,
		// straight into the batch (see Lane.send).
		long bodySize;
		try {
			bodySize = LogEntryCodec.entrySize(entry);
		} catch (IllegalArgumentException ex) {
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.INVALID_REQUEST, ex.getMessage()));
		}
		if (bodySize > MAX_ENTRY_BODY_BYTES) {
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
					"Write too large for one replicated entry: " + bodySize + " bytes"));
		}
		if (closed) {
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
					"The node is stopping"));
		}
		if (callerTimeLeftNanos <= 0) {
			return CompletableFuture.failedFuture(callerDeadlinePassed());
		}
		long weight = bodySize + REQUEST_OVERHEAD_BYTES;
		if (!admission.tryAcquire(weight)) {
			admissionRejects.record(1);
			return CompletableFuture.failedFuture(RocksDBException.notApplied(RocksDBErrorType.SERVER_OVERLOADED,
					"Too many bytes of writes are waiting on this node (" + admission.used() + " of " + admission.limit()
							+ "); the write was not applied"));
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
		var pending = new Pending(sequence, client, entry, (int) bodySize, Applier.isBatchable(entry), deadlineNanos,
				callerDeadline,
				callerDeadline ? Math.min(MIN_TIME_LEFT_NANOS, callerTimeLeftNanos / 4) : MIN_TIME_LEFT_NANOS,
				entry instanceof LogEntry.Guarded guarded ? guarded.leaderEpoch() : -1);
		live.add(pending);
		// The answer comes on the control plane: the caller's continuations run on a virtual thread instead. Made
		// before the request is queued: the thread that queues it may also take it into a batch, sent at once.
		var copy = new CallerCopy(pending);
		pending.callerCopy = copy;
		// One callback, so that the caller is answered only after the request's bytes were given back (R39-2): as two,
		// the answer could come first, since a future runs its dependents in no promised order (LIFO in practice).
		pending.result.whenComplete((value, error) -> {
			try {
				live.remove(pending);
				synchronized (numbering) {
					inFlight.remove(sequence);
				}
				// An abandoned request is still in its queue: its overhead is given back when it leaves it (see take()).
				admission.release(pending.isAbandoned() ? weight - REQUEST_OVERHEAD_BYTES : weight);
			} finally {
				CALLERS.execute(() -> copy.answer(value, error));
			}
		});
		lane.enqueue(pending);
		if (closed) {
			// close() may have swept the queues before this request was added. If a batch took it meanwhile, close()
			// answers it as unknown.
			pending.abandon(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, "The node is stopping"));
		}
		// It waits for a batch slot: fail it at its deadline even if no slot frees up by then.
		expireWhenQueued(pending);
		return copy;
	}

	/**
	 * The future a caller of {@link #submit} gets: completed with its request's answer, on a virtual thread, and
	 * never completing the request itself. A caller that gives up on it before the answer, while a batch is reading
	 * the request's buffers ({@link Pending#encoding}), is answered only once the batch has read them, so that it can
	 * reuse them at once: {@link #cancel} waits for that (at most {@link #MAX_ENCODE_WAIT_NANOS}) before it returns;
	 * {@link #completeExceptionally} (which {@code orTimeout} uses, on the JDK's one delayer thread) does not wait but
	 * completes the future later, once the encoding ended. Both take a request that was not sent yet out of its queue. Only the dependents of this future see that order:
	 * {@code complete}, {@code obtrude*} and {@code completeOnTimeout} with a value are not covered.
	 */
	private final class CallerCopy extends CompletableFuture<Object> {

		private final Pending pending;

		CallerCopy(Pending pending) {
			this.pending = pending;
		}

		/** The request's answer. */
		void answer(@Nullable Object value, @Nullable Throwable error) {
			if (error != null) {
				super.completeExceptionally(error);
			} else {
				super.complete(value);
			}
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			if (isDone()) {
				return false;
			}
			if (!pending.result.isDone()) {
				// Never sent: it never will be. Taken into a batch: answered once the batch no longer reads its buffers.
				pending.abandon(new CancellationException("The caller cancelled the write before it was sent"));
				pending.awaitEncoded(MAX_ENCODE_WAIT_NANOS);
			}
			return super.cancel(mayInterruptIfRunning);
		}

		@Override
		public boolean completeExceptionally(Throwable ex) {
			if (isDone()) {
				return false;
			}
			if (!pending.result.isDone()) {
				// Never sent: it never will be (its caller was told it failed, and may write it again).
				pending.abandon(ex);
			}
			if (!pending.result.isDone() && pending.isEncoding()) {
				Runnable fail = () -> CALLERS.execute(() -> CallerCopy.super.completeExceptionally(ex));
				pending.whenEncoded(fail);
				try {
					control.schedule(fail, MAX_ENCODE_WAIT_NANOS, TimeUnit.NANOSECONDS);
				} catch (RejectedExecutionException stopping) {
					// close() answers every request, and ends every encoding.
				}
				return true;
			}
			return super.completeExceptionally(ex);
		}
	}

	/**
	 * If {@code pending} is queued (never sent, or sent and refused before the append), fails it as not applied when
	 * {@link Lane#take} would: {@link Pending#minTimeLeftNanos} before its deadline, its caller's or else the node's
	 * request timeout, even if no batch slot frees up by then to look at the queue (R21-1, R21-4, R22-F2: at the
	 * deadline itself the caller would get that answer only {@link #CALLER_MARGIN_NANOS} before its own deadline). Called after every enqueue and requeue of requests that are queued again; arms a timer
	 * only if none is still waiting, so that a request requeued again and again holds one timer, not one per requeue
	 * (R21-2). The timer only abandons a request that is queued when it fires ({@link Pending#abandon} needs QUEUED),
	 * so one that fires while the request is in a batch does nothing; once it fired, the next requeue arms another.
	 */
	private void expireWhenQueued(Pending pending) {
		if (!pending.isQueued()) {
			return;
		}
		boolean first;
		synchronized (pending) {
			var current = pending.expiry;
			if (pending.result.isDone() || current != null && current.getDelay(TimeUnit.NANOSECONDS) > 0) {
				// Answered, or a timer still waits for the same deadline.
				return;
			}
			first = current == null;
			try {
				pending.expiry = control.schedule(() -> pending.abandon(pending.deadlineFailure()),
						pending.deadlineNanos - pending.minTimeLeftNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
			} catch (RejectedExecutionException stopping) {
				// close() answers every request.
				return;
			}
		}
		if (first) {
			// One callback per request, which cancels whichever timer is the latest then.
			pending.result.whenComplete((_, _) -> {
				ScheduledFuture<?> latest;
				synchronized (pending) {
					latest = pending.expiry;
				}
				if (latest != null) {
					latest.cancel(false);
				}
			});
		}
	}

	private static RocksDBException stopping() {
		return RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, "The node is stopping");
	}

	private static RocksDBException callerDeadlinePassed() {
		return RocksDBException.notApplied(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
				"The request deadline came before the write was sent; it was not applied");
	}

	/** For a request that was sent, and refused before the append by every leader until its caller's deadline. */
	private static RocksDBException callerDeadlinePassedUnaccepted() {
		return RocksDBException.notApplied(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
				"The request deadline came before a leader accepted the write; it was not applied");
	}

	private static RocksDBException notAccepted() {
		return RocksDBException.notApplied(RocksDBErrorType.SERVER_OVERLOADED,
				"No leader accepted the write in time; it was not applied");
	}

	private static RocksDBException noAnswer(@Nullable Throwable cause) {
		return unavailable(cause != null ? cause
				: new TimeoutException("No answer before the deadline"));
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
		/**
		 * Bytes of this lane's attempts in flight, counted before they are handed to Ratis (see
		 * {@link #ATTEMPT_BYTES_IN_FLIGHT}); each lane has its own, so that data writes never take the permits of
		 * transaction commits. An attempt that does not fit is not sent: it is refused before the append by this
		 * node's own decision, and sent again after {@link #NOT_READY_RETRY_MS}, data writes as the same batch,
		 * commits back through the queue. A local attempt keeps its permit until Ratis answers it, not only until
		 * this node stops waiting: the entry stays pending in Ratis until then. A refused attempt asks again under
		 * the name of its batch (data writes: the batch; commits: the commit holding the reservation if the batch has
		 * it, else its first), and the first one refused while no reservation is held reserves its bytes until it gets
		 * in (R50-1). One reservation, not a queue: a waiter refused while another holds it can be overtaken, once the
		 * holder got in, by a later attempt refused before the waiter asks again, so the order among several large
		 * waiters is not first come first served; one large waiter gets in. In a saturated lane the bytes a finished batch frees would otherwise go to
		 * the batch dispatched right after it, and an attempt larger than one release, up to
		 * {@link RaftSettings#MAX_ENTRY_BYTES} against batches of up to {@link #MAX_BATCH_BYTES}, could be refused
		 * until its deadline. A batch that stops sending gives its reservation up; one of commits that leaves the queue
		 * otherwise lets it lapse ({@link ByteAdmission#RESERVATION_NANOS}).
		 */
		final ByteAdmission permits = new ByteAdmission(ATTEMPT_BYTES_IN_FLIGHT);
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

		/**
		 * Forms batches while there are queued requests and room in flight, and hands each to {@link #submitter}, which
		 * encodes and sends it (R22-F1), or, for a data batch while this node last saw it does not lead, to
		 * {@link #sender} (R22-2, see {@link #leaderHint}). Taking a batch is short and never waits but for this lane's lock, so it stays on
		 * the calling thread (a caller, a control thread or a submit thread), and batches form exactly as before;
		 * encoding and the submission to Ratis, which can block during a disk stall, never run on {@link #control} or
		 * on the proposing caller. No order between batches is relied on (see design.md, "Submitting off the control
		 * plane"): {@code send()} already ran outside the lane's lock, on whichever thread dispatched, and two batches
		 * taken one after the other by one thread can reach Ratis in either order.
		 */
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
				// Commits are only ever submitted to this node's own Raft server.
				boolean viaSender = !commits && !leaderHint;
				try {
					(viaSender ? sender : submitter).execute(() -> sendTaken(batch, viaSender));
				} catch (RejectedExecutionException ex) {
					// The node is stopping: nothing of this batch was encoded or sent.
					encodingEnded(batch);
					fail(batch, stopping());
					synchronized (this) {
						batchesInFlight--;
					}
				}
			}
		}

		/**
		 * Sends a batch that {@link #dispatch} took, on a submit thread or, {@code onSender}, on {@link #sender}. A batch
		 * reached once the proposer is closing is not sent (R22-4): {@code close()} answers the writes in flight only
		 * after closing the client, which can take seconds, and nothing would wait for this append. Any failure here,
		 * an {@link Error} too (an out-of-memory error encoding up to 8 MiB), ends the batch and gives back its slot
		 * (R22-3): the executor would keep it to itself, and the writes would wait unanswered until the node stops.
		 */
		private void sendTaken(List<Pending> batch, boolean onSender) {
			try {
				if (closed) {
					encodingEnded(batch);
					fail(batch, stopping());
					finished();
					return;
				}
				send(batch, onSender);
			} catch (Throwable ex) {
				encodingEnded(batch);
				fail(batch, ex instanceof RocksDBException rocks ? rocks : Proposer.unavailable(ex));
				finished();
			}
		}

		/**
		 * The next batch: the first queued request and, if it is a data write, the data writes behind it, up to the
		 * batch limits. Requests past their deadline, or with less than {@link #MIN_TIME_LEFT_NANOS} left before the
		 * node's own deadline, fail as not applied, except those resent alone (see {@link Pending#fail}). A request that
		 * an earlier batch may have appended never comes back to the queue: it fails with an unknown outcome instead
		 * (see {@link Lane#endUnanswered}). Abandoned requests are dropped.
		 */
		private List<Pending> take() {
			var batch = new ArrayList<Pending>();
			long now = System.nanoTime();
			int bytes = 0;
			// The smallest copyBatchLimit of the copies taken: each was marked by a reply that could carry that many.
			int copyLimit = Integer.MAX_VALUE;
			// The deadlines of the batch: a caller's deadline caps the resends of its whole batch (see send()), so a
			// request with one only shares a batch with requests whose deadlines are within an attempt timeout of it.
			long earliest = Long.MAX_VALUE;
			long latest = Long.MIN_VALUE;
			boolean anyCaller = false;
			while (!queue.isEmpty()) {
				var next = queue.peekFirst();
				if (next.result.isDone() || next.entry == null) {
					queue.pollFirst();
					leftQueue(next);
					continue;
				}
				if (closed || next.deadlineNanos - now < next.minTimeLeftNanos) {
					queue.pollFirst();
					var onDrop = onTakeDropsForTesting;
					if (onDrop != null) {
						onDrop.accept(next);
					}
					// Out of QUEUED before it is failed (R22-F3): its expiry timer may abandon it at this very moment, and
					// then the admission keeps its overhead until leftQueue() gives it back, which must happen once.
					var failure = closed ? stopping() : next.deadlineFailure();
					if (!next.abandon(failure)) {
						// Resent alone (still SENT): failed saying it was applied. Already abandoned: nothing to do.
						next.fail(failure);
					}
					leftQueue(next);
					continue;
				}
				if (!batch.isEmpty() && (next.alone() || batch.getFirst().alone()
						|| next.resentAlone != batch.getFirst().resentAlone || next.epoch != batch.getFirst().epoch
						|| (next.resentAlone && batch.size() >= Math.min(copyLimit, next.copyBatchLimit))
						|| batch.size() >= MAX_BATCH_REQUESTS || bytes + next.bodySize > MAX_BATCH_BYTES
						|| ((anyCaller || next.callerDeadline) && Math.max(latest, next.deadlineNanos)
								- Math.min(earliest, next.deadlineNanos) > attemptTimeoutNanos))) {
					break;
				}
				queue.pollFirst();
				next.encodingStarted();
				if (!next.markSent()) {
					// Abandoned just now.
					next.encodingEnded();
					leftQueue(next);
					continue;
				}
				batch.add(next);
				bytes += next.bodySize + Long.BYTES;
				earliest = Math.min(earliest, next.deadlineNanos);
				latest = Math.max(latest, next.deadlineNanos);
				anyCaller |= next.callerDeadline;
				if (next.resentAlone) {
					copyLimit = Math.min(copyLimit, next.copyBatchLimit);
				}
				if (next.alone()) {
					break;
				}
			}
			return batch;
		}

		private static void encodingEnded(List<Pending> batch) {
			for (var pending : batch) {
				pending.encodingEnded();
			}
		}

		/** {@code pending} left the queue without being sent: an abandoned one gives back the rest of its weight. */
		private void leftQueue(Pending pending) {
			if (pending.isAbandoned()) {
				admission.release(REQUEST_OVERHEAD_BYTES);
			}
		}

		private void finished() {
			synchronized (this) {
				batchesInFlight--;
			}
			dispatch();
		}

		private void send(List<Pending> batch, boolean onSender) {
			var sequences = new long[batch.size()];
			ClientRequestId[] clients = null;
			var entries = new ArrayList<LogEntry>(batch.size());
			long entriesSize = 0;
			long earliest = batch.getFirst().deadlineNanos;
			long latest = earliest;
			long callers = Long.MAX_VALUE;
			for (int i = 0; i < sequences.length; i++) {
				var pending = batch.get(i);
				sequences[i] = pending.sequence;
				if (pending.client != null) {
					if (clients == null) {
						clients = new ClientRequestId[sequences.length];
					}
					clients[i] = pending.client;
				}
				// Not null: a request taken into a batch is never abandoned (see Pending.markSent).
				entries.add(Objects.requireNonNull(pending.entry, "entry"));
				entriesSize += pending.bodySize;
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
			// Each entry is encoded once, straight into the batch, which also returns the batch's requests as decoding
			// it in place would read them. If this node leads, its state machine takes them instead of decoding the
			// batch (see submitLocal).
			var beforeEncode = beforeEncodeForTesting;
			if (beforeEncode != null) {
				beforeEncode.run();
			}
			LogEntryCodec.EncodedBatch encoded;
			try {
				encoded = LogEntryCodec.encodeBatch(proposerId, completedBelow(), sequences, clients, entries,
						entriesSize, true);
			} catch (RuntimeException ex) {
				refuseUnencodable(batch, ex);
				return;
			}
			var message = Message.valueOf(UnsafeByteOperations.unsafeWrap(encoded.bytes()));
			var local = encoded.requests();
			if (local != null) {
				// From now on each request is the copy in this batch, never again its caller's buffers: a request that
				// goes back to the queue still sent (a commit after another's conflict, a copy resent alone) cannot be
				// abandoned, so its caller may be answered (cancelled, timed out) and reuse its buffers while it waits.
				// A batch with a schema change has no requests; its members only go back to the queue unsent.
				for (int i = 0; i < sequences.length; i++) {
					batch.get(i).entry = local.get(i).entry();
				}
			}
			// The callers' buffers are not read any more: a caller that gave up meanwhile is answered now.
			encodingEnded(batch);
			long now = System.nanoTime();
			for (var pending : batch) {
				queueTime.record(now - pending.queuedNanos);
			}
			batchSize.record(batch.size());
			// One Ratis invocation for every attempt of this batch; a batch formed later is another invocation.
			long callId = callIds.incrementAndGet();
			if (commits) {
				sendCommits(batch, message, local, callId, deadline, now);
			} else {
				attempt(batch, message, local, callId, deadline, now, false, onSender);
			}
		}

		/**
		 * The batch did not encode, although {@link LogEntryCodec#entrySize} accepted each of its entries at submit:
		 * a caller changed the buffers of its write while it was queued. Nothing of this batch was sent. The requests
		 * that no longer encode to their size fail, as invalid unless an earlier batch applied them (resent alone); the others
		 * go back to the queue. If every request still encodes alone, the cause is elsewhere, and {@code error} fails
		 * the batch (see {@link #dispatch}).
		 */
		private void refuseUnencodable(List<Pending> batch, RuntimeException error) {
			var rest = new ArrayList<Pending>(batch.size());
			for (var pending : batch) {
				try {
					var entry = Objects.requireNonNull(pending.entry, "entry");
					if (LogEntryCodec.encodeEntryBody(entry).length != pending.bodySize) {
						throw new IllegalStateException("The write changed size after it was submitted");
					}
					rest.add(pending);
				} catch (RuntimeException invalid) {
					pending.fail(pending.resentAlone ? noAnswer(invalid)
							: RocksDBException.notApplied(RocksDBErrorType.INVALID_REQUEST,
									"The write could not be encoded, it was not applied: " + invalid.getMessage()));
				}
			}
			// Read for the last time: the rest goes back to the queue, as never taken (or failed, if every one encodes).
			encodingEnded(batch);
			if (rest.size() == batch.size()) {
				throw error;
			}
			// Nothing was sent: those that no earlier batch applied (resent alone) go back as never taken, so that their
			// caller's deadline or cancellation can again take them out of the queue (and their buffers be reused).
			for (var pending : rest) {
				if (!pending.resentAlone) {
					pending.untake();
				}
			}
			requeue(rest);
			for (var pending : rest) {
				expireWhenQueued(pending);
			}
			finished();
		}

		/**
		 * One attempt at a batch of data writes, then resends until answered or {@code deadlineNanos}. Once an attempt
		 * ended without telling whether the entry was appended ({@code ambiguous}), a later refusal no longer means
		 * "not applied": the batch can only end with its results or an unknown outcome. Runs on a submit thread or,
		 * {@code onSender}, on {@link #sender} (see {@link #leaderHint}): a local submission always runs on a submit
		 * thread, a remote attempt always on {@link #sender}, with one hand-off at most.
		 */
		private void attempt(List<Pending> batch, Message message, @Nullable List<Request> local, long callId,
				long deadlineNanos, long sentNanos, boolean ambiguous, boolean onSender) {
			CompletableFuture<RaftClientReply> reply;
			if (isLocalLeader()) {
				if (onSender) {
					// This node leads after all: its submission can block in Ratis, so not on the sender's threads.
					CompletableFuture<RaftClientReply> submitted;
					try {
						submitted = CompletableFuture.supplyAsync(
								() -> submitLocal(message, local, callId, deadlineNanos, permits, batch), submitter)
								.thenCompose(Function.identity());
					} catch (RejectedExecutionException stopping) {
						submitted = CompletableFuture.failedFuture(stopping);
					}
					reply = submitted;
				} else {
					reply = submitLocal(message, local, callId, deadlineNanos, permits, batch);
				}
			} else if (onSender) {
				// Already on the sender: no second hand-off. Failures as supplyAsync would report them.
				CompletableFuture<RaftClientReply> sent;
				try {
					sent = CompletableFuture.completedFuture(sendToLeader(message, callId, permits, batch));
				} catch (Throwable ex) {
					sent = CompletableFuture.failedFuture(ex instanceof CompletionException ? ex : new CompletionException(ex));
				}
				reply = sent;
			} else {
				reply = CompletableFuture.supplyAsync(() -> sendToLeader(message, callId, permits, batch), sender);
			}
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
								|| cause instanceof NoPermitException
								? NOT_READY_RETRY_MS : ATTEMPT_SLEEP_MS;
						// The resend itself must go out before the deadline, not only the decision to resend.
						if (closed || System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay) - deadlineNanos >= 0) {
							roundTrip.since(sentNanos);
							// No more copies: a reservation of its permit bytes (R50-1) would only hold others back.
							permits.cancel(batch);
							endUnanswered(batch, deadlineNanos, nowAmbiguous, refusal, cause);
							finished();
							return null;
						}
						// Send the same batch again. If an earlier copy took effect, applying this one returns its
						// recorded results.
						resends.record(1);
						Runnable end = () -> {
							permits.cancel(batch);
							try {
								endUnanswered(batch, deadlineNanos, nowAmbiguous, refusal, cause);
							} catch (Throwable unexpected) {
								fail(batch, unavailable(unexpected));
							} finally {
								finished();
							}
						};
						// The timer only hands the resend over, to a submit thread or, while this node does not lead, to
						// the sender (see leaderHint): a local submission can block inside Ratis.
						Consumer<Boolean> resend = viaSender -> {
							if (closed || System.nanoTime() - deadlineNanos >= 0) {
								// Too late after all (the timer ran late): no copy goes out after the deadline.
								end.run();
								return;
							}
							try {
								attempt(batch, message, local, callId, deadlineNanos, sentNanos, nowAmbiguous, viaSender);
							} catch (Throwable unexpected) {
								// The executor would keep it to itself. Earlier copies may have been appended.
								permits.cancel(batch);
								fail(batch, unavailable(unexpected));
								finished();
							}
						};
						try {
							control.schedule(() -> {
								boolean viaSender = !leaderHint;
								try {
									(viaSender ? sender : submitter).execute(() -> resend.accept(viaSender));
								} catch (RejectedExecutionException stopping) {
									end.run();
								}
							}, delay, TimeUnit.MILLISECONDS);
						} catch (RejectedExecutionException stopping) {
							end.run();
						}
						return null;
					}
				} catch (Throwable unexpected) {
					permits.cancel(batch);
					fail(batch, unavailable(unexpected));
					finished();
					return null;
				}
				roundTrip.since(sentNanos);
				// Answered: it got its permit, which ended a reservation it held; nothing of it waits any more.
				permits.cancel(batch);
				try {
					if (refusal != null) {
						for (var pending : batch) {
							// An earlier copy may have been appended, although this one was refused.
							pending.fail(ambiguous ? unavailable(refusal) : refusal(refusal));
						}
					} else if (isReplyTooLarge(error)) {
						fail(batch, replyTooLarge(error));
					} else {
						complete(batch, answer, ambiguous);
					}
				} catch (Throwable unexpected) {
					fail(batch, unavailable(unexpected));
				} finally {
					finished();
				}
				return null;
			}, control);
		}

		/**
		 * Ends a batch of data writes whose resends stopped without an answer, at {@code deadlineNanos} or because the
		 * node is stopping. The batch's deadline is the earliest caller deadline among its members, if any (see
		 * {@link #send}; take() keeps members' deadlines within an attempt timeout of each other when one is a
		 * caller's), so the other members may still have time left. If no copy of the batch was appended, they go
		 * back to the head of the queue and are sent again in a new batch, a new Ratis invocation, under their own
		 * deadlines; their callers' deadlines and cancellations can again take them out of the queue. If a copy may
		 * have been appended ({@code ambiguous}), they fail with an unknown outcome instead: a new invocation would
		 * append the whole batch a second time while the first copy may still be pending, the amplification that
		 * keeping one invocation per batch avoids. The members whose own deadline is the batch's, or too close to it
		 * (see {@link Pending#minTimeLeftNanos}), fail too: as not applied if no copy was appended.
		 */
		private void endUnanswered(List<Pending> batch, long deadlineNanos, boolean ambiguous,
				@Nullable StateMachineException refusal, @Nullable Throwable cause) {
			long now = System.nanoTime();
			var again = new ArrayList<Pending>(0);
			for (var pending : batch) {
				// Only requests that no copy of this batch may have appended go back: one that may have been appended
				// fails here with an unknown outcome, so no request in the queue was ever possibly appended (R17-3).
				if (!closed && !ambiguous && pending.deadlineNanos - deadlineNanos > 0
						&& pending.deadlineNanos - now >= pending.minTimeLeftNanos) {
					pending.queuedNanos = now;
					if (!pending.resentAlone) {
						pending.unsend();
					}
					again.add(pending);
				} else if (ambiguous) {
					pending.fail(noAnswer(cause));
				} else if (closed) {
					pending.fail(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, "The node is stopping"));
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
					expireWhenQueued(pending);
				}
			}
		}

		/**
		 * The single attempt at a batch of transaction commits. A conflict rejects the whole log entry before it is
		 * appended: the commits that conflict fail, the others go again with the next batch.
		 */
		private void sendCommits(List<Pending> batch, Message message, @Nullable List<Request> local, long callId,
				long deadlineNanos, long sentNanos) {
			// One commit names the attempts of these commits (R50-1): the one holding the reservation if it is among them,
			// else the first. A batch refused for want of a permit comes back at the head of the queue (retryLater), but
			// one requeued later goes in front of it, and take() can merge both under the later one's first commit:
			// asked under that, the reserved bytes of the holder, part of this batch, would be kept free beside it until
			// the reservation lapsed (review of R50, 2).
			var waiter = permits.waiterAmong(batch);
			submitLocal(message, local, callId, deadlineNanos, permits, waiter).handleAsync((answer, error) -> {
				roundTrip.since(sentNanos);
				boolean again = false;
				try {
					var refusal = error != null ? stateMachineException(error)
							: !answer.isSuccess() && answer.getException() instanceof StateMachineException e ? e : null;
					if (refusal != null && isCause(refusal, RockserverStateMachine.ConflictException.class)) {
						var positions = ConflictPositions.parse(refusal.getCause().getMessage(), batch.size());
						if (positions == null) {
							fail(batch, new ConflictException());
						} else {
							var rest = new ArrayList<>(batch);
							for (int position : positions) {
								batch.get(position).result.completeExceptionally(new ConflictException());
								rest.set(position, null);
							}
							rest.removeIf(Objects::isNull);
							// Rejected with the whole entry before its append: back as never sent, as after a busy
							// refusal, so that their deadlines and cancellations apply again. The finished() below
							// usually gives this batch's slot to them, but another requeue on another control thread
							// may get ahead of them, so they also get their expiry (R21-3).
							long now = System.nanoTime();
							for (var pending : rest) {
								pending.queuedNanos = now;
								if (!pending.resentAlone) {
									pending.unsend();
								}
							}
							requeue(rest);
							for (var pending : rest) {
								expireWhenQueued(pending);
							}
						}
					} else if (refusal != null) {
						fail(batch, refusal(refusal));
					} else if (isBusyBeforeAppend(error != null ? ReplicatedConnection.unwrap(error)
							: answer.isSuccess() ? null : answer.getException())) {
						// Not appended, only no room yet: back through the queue, under the commits' own deadlines.
						again = true;
						retryLater(batch);
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
						complete(batch, answer, false);
					}
				} catch (Throwable unexpected) {
					fail(batch, unavailable(unexpected));
				} finally {
					if (!again) {
						permits.cancel(waiter);
					}
					finished();
				}
				return null;
			}, control);
		}

		/**
		 * Puts the commits of a batch that was not appended back in the queue after {@link #NOT_READY_RETRY_MS}, as if
		 * never sent: their deadlines and cancellations apply again, and they fail as not applied at their deadline
		 * (their caller's, or else the request timeout), from {@link #take} or, while no slot frees up, from
		 * {@link #expireWhenQueued}.
		 */
		private void retryLater(List<Pending> batch) {
			for (var pending : batch) {
				pending.queuedNanos = System.nanoTime();
				pending.unsend();
			}
			try {
				control.schedule(() -> {
					requeue(batch);
					// A commit sent at once when submitted has no expiry timer, and every slot may be taken again by
					// now: fail it at its deadline even if no slot frees up by then (R21-1).
					for (var pending : batch) {
						expireWhenQueued(pending);
					}
				}, NOT_READY_RETRY_MS, TimeUnit.MILLISECONDS);
			} catch (RejectedExecutionException stopping) {
				fail(batch, RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, "The node is stopping"));
			}
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
		private void complete(List<Pending> batch, RaftClientReply reply, boolean ambiguous) {
			if (!reply.isSuccess()) {
				fail(batch, unavailable(reply.getException()));
				return;
			}
			List<ApplyResult> results;
			try {
				results = ResultCodec.decodeAll(reply.getMessage().getContent().asReadOnlyByteBuffer());
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
					pending.complete(result, ambiguous);
					continue;
				}
				pending.resentAlone = true;
				if (commits) {
					// A database read: off the control threads, which this reply handler runs on.
					try {
						sender.execute(() -> completeFromLocalRecord(pending));
					} catch (RejectedExecutionException stopping) {
						pending.fail(unavailable(stopping));
					}
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
				recorded = localRecord(pending);
			} catch (IOException | RuntimeException ex) {
				pending.fail(unavailable(ex));
				return;
			}
			if (recorded == null) {
				pending.fail(unavailable(new IllegalStateException("No local record of transaction commit "
						+ (pending.client != null ? pending.client : pending.sequence))));
			} else {
				aloneResends.record(1);
				pending.complete(recorded, false);
			}
		}
	}

	private boolean isLocalLeader() {
		try {
			var info = localServer.get().getDivision(groupId).getInfo();
			boolean leads = info.isLeader() && info.isLeaderReady();
			leaderHint = leads;
			return leads;
		} catch (IOException | RuntimeException ex) {
			leaderHint = false;
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

	/**
	 * Submits straight to the local Raft server, with no client in between that could redirect it elsewhere.
	 * {@code local}, if not null, is the batch's requests as {@link LogEntryCodec#encodeBatch} returned them with its
	 * bytes: they are offered to this node's state machine, which takes them instead of decoding the batch (see
	 * {@link RockserverStateMachine#offerLocal}), and withdrawn once Ratis answers, in case it answered without
	 * starting a transaction (a refusal, or the retry cache).
	 */
	private CompletableFuture<RaftClientReply> submitLocal(Message message, @Nullable List<Request> local, long callId,
			long deadlineNanos, ByteAdmission permits, Object waiter) {
		long weight = message.size();
		if (!permits.tryAcquire(weight, waiter)) {
			return CompletableFuture.failedFuture(new NoPermitException(weight));
		}
		CompletableFuture<RaftClientReply> reply;
		RockserverStateMachine.LocalProposal offered = null;
		try {
			var server = localServer.get();
			if (local != null
					&& server.getDivision(groupId).getStateMachine() instanceof RockserverStateMachine stateMachine) {
				offered = stateMachine.offerLocal(invocationClientId, callId, message.getContent(), local);
			}
			reply = server.submitClientRequestAsync(invocation(message, callId, self));
		} catch (IOException | RuntimeException ex) {
			permits.release(weight);
			if (offered != null) {
				offered.withdraw();
			}
			return CompletableFuture.failedFuture(ex);
		}
		var withdrawn = offered;
		// Given back when Ratis answers, which it does once the entry is no longer pending (applied, or failed by a
		// step down), not when this node stops waiting below: until then the entry holds room in Ratis' limits.
		reply.whenComplete((_, _) -> {
			permits.release(weight);
			if (withdrawn != null) {
				withdrawn.withdraw();
			}
		});
		// The deadline only stops new copies (see attempt): a copy already submitted here runs to its end, so
		// wait for its answer at least as long as a remote attempt would, or a healthy commit of a batch holding
		// one short caller deadline would end as "outcome unknown" for every member. A copy, so that the timeout
		// does not complete Ratis' own future.
		return reply.copy().orTimeout(Math.max(deadlineNanos - System.nanoTime(), attemptTimeoutNanos),
				TimeUnit.NANOSECONDS);
	}

	/**
	 * Sends one attempt to the leader that this node's own Raft server knows, through the client's RPC layer, which
	 * waits for the reply at most the attempt timeout. Not through {@code client.io()}, which would make each attempt
	 * a new invocation. With no leader known (an election), nothing is sent: the attempt fails as refused before
	 * the append.
	 */
	private RaftClientReply sendToLeader(Message message, long callId, ByteAdmission permits, Object waiter) {
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
		// Per attempt: the remote leader's pending requests cannot be seen from here. Ratis' own refusal when its
		// limit is full is recognized as before the append (see isRatisPendingLimit).
		long weight = message.size();
		if (!permits.tryAcquire(weight, waiter)) {
			throw new CompletionException(new NoPermitException(weight));
		}
		Object proxy = null;
		try {
			var request = invocation(message, callId, leader);
			// The proxy this attempt is sent on, read just before the call: only that one is replaced if the call fails
			// (R9-2). GrpcClientRpc.sendRequest reads it from the map once more, and Ratis has no way to send on a given
			// proxy (the sendRequest that takes one is private), so a reset by another attempt between the two reads sends
			// this attempt on the replacement while this one holds the old proxy (R10-5). If it fails there, the reset
			// below keeps the replacement, and the resend to that peer can fail on it once more; the attempt after that
			// reads it first and replaces it. So such a race costs at most one more failed attempt and its resend.
			proxy = currentProxy(rpc, leader);
			return rpc.sendRequest(request);
		} catch (IOException ex) {
			// Resets the connection to that peer if the error calls for it, as the client's own calls do. On a thread
			// of its own: closing the old channel waits up to 3 s (then 2 s more) for its calls to end
			// (GrpcUtil.shutdownManagedChannel, see RatisContractTest), and a frozen leader ends none, so done here it
			// would hold this attempt's failure, and the resend to the next leader, that long.
			resetConnection(rpc, leader, proxy, ex);
			throw new CompletionException(ex);
		} catch (RuntimeException ex) {
			// The proxy threw instead of failing the call: it is broken, and Ratis would keep it (it resets a proxy only
			// on connection errors), so every later attempt to this peer would fail the same way. Seen with a proxy
			// whose metric interceptor has no registry (R8-4, see resetBrokenProxy). Replaced like a failed connection.
			resetBrokenProxy(rpc, leader, proxy, ex);
			throw new CompletionException(ex);
		} finally {
			permits.release(weight);
		}
	}

	/** How long {@link #resetConnection} waits for Ratis to take the failed proxy out of its map. */
	static final long RESET_REMOVAL_WAIT_NANOS = TimeUnit.SECONDS.toNanos(1);

	/**
	 * {@code rpc.handleException(peer, error, false)} on a virtual thread of its own: Ratis takes the peer's proxy out
	 * of its map at once, under its lock, and then closes the old channel on the calling thread, which waits for the
	 * calls in flight up to 3 s. This thread waits only for the first part: until the map no longer holds the proxy
	 * that failed (bounded by {@link #RESET_REMOVAL_WAIT_NANOS}). So a resend to the same peer right after takes a new
	 * channel, and is not sent on the old one that the reset then closes under it; attempts to other peers never
	 * waited for the close (the map's lock is not held meanwhile).
	 *
	 * <p>{@code failed} is the proxy the attempt was sent on, read before the call (null if unknown: then the one in the
	 * map now). If the map no longer holds it, another attempt that failed on it already replaced it, and the proxy now
	 * in the map is that replacement: it is kept (R9-2). Without this, attempts in flight on one broken proxy, which all
	 * fail together, would each reset the proxy of the one before, closing healthy channels under their resends.</p>
	 */
	static void resetConnection(RaftClientRpc rpc, RaftPeerId peer, @Nullable Object failed, IOException error) {
		// Ratis' proxy map resets on these errors only (PeerProxyMap.handleException with reconnect false).
		reset(rpc, peer, failed, error, IOUtils.shouldReconnect(error));
	}

	/**
	 * R8-4: replaces the proxy to {@code peer} whose call threw {@code error} instead of failing, as
	 * {@link #resetConnection} does after a connection error. Ratis 3.3.1 can build a client proxy whose metric
	 * interceptor has no registry: {@code RatisMetrics.create} shares a registry that already exists without counting
	 * the new holder, and {@code RefCountingMap.put} counts a new one only after it is in the map, so the close of an
	 * older proxy to the same peer, in that moment, removes it and the new proxy gets {@code null}. Every call of that
	 * proxy then throws a {@link NullPointerException} when it starts (and so does its close), and no error of it is a
	 * connection error. {@link #resetConnection} closes old proxies while new ones are made, so this node can meet it
	 * after a leader loss ({@code RatisContractTest.aProxyMadeWhileTheOneBeforeItClosesCanGetNoMetricRegistry}).
	 */
	static void resetBrokenProxy(RaftClientRpc rpc, RaftPeerId peer, @Nullable Object failed, RuntimeException error) {
		reset(rpc, peer, failed, error, true);
	}

	private static void reset(RaftClientRpc rpc, RaftPeerId peer, @Nullable Object sentOn, Exception error,
			boolean reconnect) {
		Object failed = !reconnect ? null : sentOn != null ? sentOn : currentProxy(rpc, peer);
		if (failed != null && currentProxy(rpc, peer) != failed) {
			// Already replaced by another attempt that failed on it: the proxy in the map is a new one (R9-2).
			return;
		}
		Thread.ofVirtual().name("rockserver-propose-reset-" + peer).start(() -> {
			try {
				// Checked again here, just before the reset: Ratis' map has no compare-and-remove, so a reset by another
				// attempt between this check and the removal can still replace one healthy proxy (a narrow window).
				if (failed == null || currentProxy(rpc, peer) == failed) {
					rpc.handleException(peer, error, reconnect);
				}
			} catch (RuntimeException ex) {
				LoggerFactory.getLogger(Proposer.class).debug("Could not reset the connection to {}", peer, ex);
			}
		});
		if (failed == null) {
			return;
		}
		long deadline = System.nanoTime() + RESET_REMOVAL_WAIT_NANOS;
		while (currentProxy(rpc, peer) == failed && System.nanoTime() - deadline < 0) {
			LockSupport.parkNanos(50_000);
		}
	}

	/** The proxy that {@code rpc} uses for {@code peer} now, or null if it has none or does not say. */
	private static @Nullable Object currentProxy(RaftClientRpc rpc, RaftPeerId peer) {
		if (!(rpc instanceof RaftClientRpcWithProxy<?> withProxy)) {
			return null;
		}
		try {
			return withProxy.getProxies().getProxy(peer);
		} catch (IOException | RuntimeException ex) {
			// Between the removal and the new entry, or a peer that left: not the failed proxy.
			return null;
		}
	}

	/**
	 * This node did not send an attempt: its attempts in flight already hold {@link #ATTEMPT_BYTES_IN_FLIGHT}. It is
	 * refused before the append by this node's own decision, unlike Ratis' own refusals, which tell only by their
	 * message whether they came before the append.
	 */
	static final class NoPermitException extends IOException {
		NoPermitException(long bytes) {
			super("No permit for " + bytes + " more bytes of attempts in flight");
		}
	}

	/**
	 * Errors that come only before the entry is appended: a leader not ready yet (Ratis' typed refusal), no permit of
	 * this node's own ({@link NoPermitException}), or Ratis' own full pending-request queue
	 * ({@link #isRatisPendingLimit}). Not NotLeaderException, which a leader stepping down also returns for appended
	 * entries, nor any other ResourceUnavailableException, such as the one for failing to add a pending request,
	 * raised after the append: an unknown outcome.
	 */
	static boolean isRefusedBeforeAppend(@Nullable Throwable error) {
		return error instanceof LeaderNotReadyException || isBusyBeforeAppend(error);
	}

	/** Refusals before the append for lack of room, which a resend shortly after may get past. */
	private static boolean isBusyBeforeAppend(@Nullable Throwable error) {
		return error instanceof NoPermitException || isRatisPendingLimit(error);
	}

	/** The text of Ratis' refusal before the append when its pending-request limits are full. */
	static final String RATIS_PENDING_LIMIT_MESSAGE = "acquire a pending write request";

	/**
	 * Ratis' refusal when its pending-request limits are full, which it raises before appending. Ratis uses the same
	 * exception type after the append ("... add a pending write request"), so only the message tells them
	 * apart: {@code RatisContractTest} pins both against the Ratis version in use. A message that changed fails that
	 * test, and here falls back to an unknown outcome, which is safe. The leader can reach those limits although each
	 * node's own permits stay below them: a remote attempt's permit is given back when its node stops waiting, while
	 * the entry may stay pending until it commits.
	 */
	static boolean isRatisPendingLimit(@Nullable Throwable error) {
		return error instanceof ResourceUnavailableException
				&& String.valueOf(error.getMessage()).contains(RATIS_PENDING_LIMIT_MESSAGE);
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
	private @Nullable ApplyResult localRecord(Pending pending) throws IOException {
		if (!(localServer.get().getDivision(groupId).getStateMachine() instanceof RockserverStateMachine stateMachine)) {
			return null;
		}
		return pending.client != null ? stateMachine.recordedResult(pending.client)
				: stateMachine.recordedResult(proposerId, pending.sequence);
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

	/** The write may have been appended, and no answer tells whether it was applied. */
	static RocksDBException unavailable(Throwable error) {
		var cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
		return RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN,
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
		try {
			sender.shutdownNow();
		} finally {
			try {
				closeClient(client);
			} finally {
				// Batches in flight lose their remote calls with the executor, and their reply handlers once the node
				// stops its control plane: their outcome is unknown.
				fail(List.copyOf(live), RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN,
						"The node stopped before the write was acknowledged; its outcome is unknown"));
			}
		}
	}

	/**
	 * Closes the client. A runtime failure is logged, not thrown (R8-4): Ratis closes every proxy of the client even
	 * when one fails, then its event loops ({@code PeerProxyMap.close}, {@code GrpcClientRpc.close}), but it catches
	 * only an {@link IOException} of each proxy and throws a {@link CompletionException} for any other. The one seen
	 * is the close of a proxy without a metric registry ({@link #resetBrokenProxy}), never reset because it was to a
	 * peer that no attempt reached again, such as a halted leader; it failed the node's whole stop.
	 */
	static void closeClient(Closeable client) throws IOException {
		try {
			client.close();
		} catch (RuntimeException ex) {
			LoggerFactory.getLogger(Proposer.class).warn("The Raft client failed to close one of its connections; "
					+ "the others and its threads are closed", ex);
		}
	}
}
