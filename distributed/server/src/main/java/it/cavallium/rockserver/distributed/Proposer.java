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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
import org.jetbrains.annotations.Nullable;

/**
 * Submits log entries to the current leader, from any node, batching the requests that arrive together.
 *
 * <p>Every proposal is a numbered {@link Request}. Requests wait in a queue while {@link #MAX_BATCHES_IN_FLIGHT}
 * batches are in flight, and then go together as one log entry (see {@link LogEntryCodec#encodeBatch}): the Raft
 * log, its fsync and the apply then cost one entry for the whole batch. A batch is resent (leader changes,
 * unreachable nodes, timeouts) with the same bytes until it gets an answer or the request timeout passes, and
 * applying a copy of a request that already took effect returns the recorded result instead of executing it again.
 * So a write takes effect at most once even when copies reach a new leader or a restarted node. When this node is
 * the leader, an attempt goes straight to its own Raft server; otherwise it is a single Ratis client call, which
 * follows the leader hints Ratis returns.</p>
 *
 * <p>Each client attempt is an independent blocking call on its own virtual thread, not a request of the client's
 * ordered async stream: once one request of that stream runs out of retries, Ratis closes the stream for good, and
 * every later proposal of this node would fail without being sent.</p>
 *
 * <p>Transaction commits ({@link #proposeOnce}) have their own queue: they are only ever sent once, to this node's
 * own Raft server, and a conflict rejects only the commits that conflict: the others are sent again in the next
 * batch.</p>
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
	/** Batch header, plus a sequence per request. */
	private static final int BATCH_HEADER_BYTES = 1 + 2 * Long.BYTES + Integer.BYTES;

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
	private final ClientId localClientId = ClientId.randomId();
	private final AtomicLong localCallIds = new AtomicLong();
	private final ExecutorService sender = Executors.newThreadPerTaskExecutor(
			Thread.ofVirtual().name("rockserver-propose-", 0).factory());
	private final long requestTimeoutNanos;
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
		this.localServer = localServer;
		this.groupId = group.getGroupId();
		this.self = RaftPeerId.valueOf(config.nodeId());
		var attemptProperties = new RaftProperties(properties);
		// Long enough that a slow but healthy commit is not resent: a resend is safe, but re-replicates the entry.
		var attemptTimeout = config.electionTimeoutMax().multipliedBy(2);
		RaftClientConfigKeys.Rpc.setRequestTimeout(attemptProperties,
				RaftSettings.duration(attemptTimeout.compareTo(MIN_ATTEMPT_TIMEOUT) > 0 ? attemptTimeout : MIN_ATTEMPT_TIMEOUT));
		// One Ratis call per attempt; the batch is resent until its deadline.
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

	/**
	 * Proposes {@code entry} and completes with its result. Fails with a {@link RocksDBException}: the result's own
	 * error, {@link RocksDBRetryException} if the leader refused it (stale transaction), or {@code SERVER_OVERLOADED}
	 * when no leader answered in time; then the outcome is unknown.
	 */
	CompletableFuture<Object> propose(LogEntry entry) {
		return submit(entry, anyLeader);
	}

	/**
	 * Proposes a transaction commit to this node's own Raft server, which must be the leader that opened it, in a
	 * single attempt: a resend could reach another leader, which would refuse it although the first attempt may
	 * have been committed. Fails with {@link ConflictException} on a conflict, {@link RocksDBRetryException} if it
	 * was refused before being appended, and {@code SERVER_OVERLOADED} if its outcome is unknown.
	 */
	CompletableFuture<Object> proposeOnce(LogEntry.Guarded entry) {
		return submit(entry, localCommits);
	}

	/** A request waiting for its answer. */
	private static final class Pending {
		final long sequence;
		final byte[] body;
		final boolean batchable;
		final long deadlineNanos;
		/** Leader epoch of a transaction commit, -1 otherwise: commits of different epochs never share an entry. */
		final long epoch;
		final long queuedNanos = System.nanoTime();
		/** Completed only by the proposer; callers get a copy, so that cancelling theirs cannot complete this one. */
		final CompletableFuture<Object> result = new CompletableFuture<>();

		Pending(long sequence, byte[] body, boolean batchable, long deadlineNanos, long epoch) {
			this.sequence = sequence;
			this.body = body;
			this.batchable = batchable;
			this.deadlineNanos = deadlineNanos;
			this.epoch = epoch;
		}
	}

	/** Every request submitted and not answered yet, so that close() can answer them all. */
	private final java.util.Set<Pending> live = java.util.concurrent.ConcurrentHashMap.newKeySet();

	private CompletableFuture<Object> submit(LogEntry entry, Lane lane) {
		byte[] body;
		try {
			body = LogEntryCodec.encodeEntryBody(entry);
		} catch (IllegalArgumentException ex) {
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.INVALID_REQUEST, ex.getMessage()));
		}
		if (BATCH_HEADER_BYTES + Long.BYTES + (long) body.length > RaftSettings.MAX_ENTRY_BYTES) {
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
					"Write too large for one replicated entry: " + body.length + " bytes"));
		}
		if (closed) {
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
					"The node is stopping"));
		}
		long sequence;
		synchronized (numbering) {
			sequence = nextSequence++;
			inFlight.add(sequence);
		}
		var pending = new Pending(sequence, body, Applier.isBatchable(entry), System.nanoTime() + requestTimeoutNanos,
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
		return pending.result.copy();
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

		Lane(boolean commits) {
			this.commits = commits;
			var name = commits ? "commits" : "writes";
			this.queueTime = metrics.duration(name + ".queue");
			this.roundTrip = metrics.duration(name + ".batch");
			this.batchSize = metrics.amount(name + ".batchSize");
			this.resends = metrics.amount(name + ".resends");
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
		 * batch limits. Requests already past their deadline were never sent, so they fail without taking effect.
		 */
		private List<Pending> take() {
			var batch = new ArrayList<Pending>();
			long now = System.nanoTime();
			int bytes = 0;
			while (!queue.isEmpty()) {
				var next = queue.peekFirst();
				if (next.result.isDone()) {
					queue.pollFirst();
					continue;
				}
				if (closed || next.deadlineNanos - now < MIN_TIME_LEFT_NANOS) {
					queue.pollFirst();
					next.result.completeExceptionally(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
							closed ? "The node is stopping" : "No leader accepted the write in time; it was not applied"));
					continue;
				}
				if (!batch.isEmpty() && (!next.batchable || !batch.getFirst().batchable || next.epoch != batch.getFirst().epoch
						|| batch.size() >= MAX_BATCH_REQUESTS || bytes + next.body.length > MAX_BATCH_BYTES)) {
					break;
				}
				queue.pollFirst();
				batch.add(next);
				bytes += next.body.length + Long.BYTES;
				if (!next.batchable) {
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
			long deadline = batch.getFirst().deadlineNanos;
			for (int i = 0; i < sequences.length; i++) {
				sequences[i] = batch.get(i).sequence;
				bodies.add(batch.get(i).body);
				// The latest: the oldest member has at least MIN_TIME_LEFT_NANOS left (see take()), the newest a full timeout.
				deadline = Math.max(deadline, batch.get(i).deadlineNanos);
			}
			var message = Message.valueOf(UnsafeByteOperations.unsafeWrap(
					LogEntryCodec.encodeBatch(proposerId, completedBelow(), sequences, bodies)));
			long now = System.nanoTime();
			for (var pending : batch) {
				queueTime.record(now - pending.queuedNanos);
			}
			batchSize.record(batch.size());
			if (commits) {
				sendCommits(batch, message, now);
			} else {
				attempt(batch, message, deadline, now);
			}
		}

		/** One attempt at a batch of data writes, then resends until answered or {@code deadlineNanos}. */
		private void attempt(List<Pending> batch, Message message, long deadlineNanos, long sentNanos) {
			var reply = isLocalLeader() ? submitLocal(message, deadlineNanos)
					: CompletableFuture.supplyAsync(() -> sendBlocking(message), sender);
			reply.handleAsync((answer, error) -> {
				try {
					var refusal = error != null ? stateMachineException(error)
							: !answer.isSuccess() && answer.getException() instanceof StateMachineException e ? e : null;
					boolean retryable = !closed && System.nanoTime() - deadlineNanos < 0
							&& (refusal != null ? isNotReady(refusal) : error != null || !answer.isSuccess());
					if (retryable) {
						// No answer yet (no leader, unreachable, refused before append): send the same batch again. If
						// an earlier copy took effect, applying this one returns its recorded results.
						var cause = error != null ? ReplicatedConnection.unwrap(error) : answer.getException();
						long delay = refusal != null || cause instanceof LeaderNotReadyException
								|| cause instanceof NotLeaderException || cause instanceof ResourceUnavailableException
								? NOT_READY_RETRY_MS : ATTEMPT_SLEEP_MS;
						resends.record(1);
						CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS, sender)
								.execute(() -> attempt(batch, message, deadlineNanos, sentNanos));
						return null;
					}
				} catch (Throwable unexpected) {
					fail(batch, unavailable(unexpected));
					finished();
					return null;
				}
				roundTrip.since(sentNanos);
				try {
					if (error == null && !answer.isSuccess() && !(answer.getException() instanceof StateMachineException)) {
						fail(batch, unavailable(answer.getException()));
					} else {
						var refusal = error != null ? stateMachineException(error)
								: !answer.isSuccess() ? (StateMachineException) answer.getException() : null;
						if (refusal != null) {
							fail(batch, refusal(refusal));
						} else if (error != null) {
							fail(batch, unavailable(error));
						} else {
							complete(batch, answer);
						}
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
		 * The single attempt at a batch of transaction commits. A conflict rejects the whole log entry before it is
		 * appended: the commits that conflict fail, the others go again with the next batch.
		 */
		private void sendCommits(List<Pending> batch, Message message, long sentNanos) {
			submitLocal(message, System.nanoTime() + requestTimeoutNanos).handleAsync((answer, error) -> {
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
	}

	private boolean isLocalLeader() {
		try {
			var info = localServer.get().getDivision(groupId).getInfo();
			return info.isLeader() && info.isLeaderReady();
		} catch (IOException | RuntimeException ex) {
			return false;
		}
	}

	/** Submits straight to the local Raft server, with no client in between that could redirect it elsewhere. */
	private CompletableFuture<RaftClientReply> submitLocal(Message message, long deadlineNanos) {
		var request = RaftClientRequest.newBuilder()
				.setClientId(localClientId)
				.setServerId(self)
				.setGroupId(groupId)
				.setCallId(localCallIds.incrementAndGet())
				.setMessage(message)
				.setType(RaftClientRequest.writeRequestType())
				.build();
		try {
			return localServer.get().submitClientRequestAsync(request)
					.orTimeout(Math.max(1, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
		} catch (IOException ex) {
			return CompletableFuture.failedFuture(ex);
		}
	}

	private RaftClientReply sendBlocking(Message message) {
		try {
			return client.io().send(message);
		} catch (IOException ex) {
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

	/** Completes each request of {@code batch} with its own result from {@code reply}. */
	private static void complete(List<Pending> batch, RaftClientReply reply) {
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
		for (int i = 0; i < results.size(); i++) {
			var result = batch.get(i).result;
			switch (results.get(i)) {
				case ApplyResult.Ok ok -> result.complete(ok.value());
				case ApplyResult.Failed failed when failed.type() == RocksDBErrorType.UPDATE_RETRY ->
						result.completeExceptionally(new RocksDBRetryException());
				case ApplyResult.Failed failed -> result.completeExceptionally(failed.toException());
			}
		}
	}

	private static void fail(List<Pending> batch, RocksDBException error) {
		for (var pending : batch) {
			pending.result.completeExceptionally(error);
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
			sender.shutdownNow();
		} finally {
			// Batches in flight lose their reply handlers with the executor: their outcome is unknown.
			fail(List.copyOf(live), RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
					"The node stopped before the write was acknowledged; its outcome is unknown"));
		}
	}
}
