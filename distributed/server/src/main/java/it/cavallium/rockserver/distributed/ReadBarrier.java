package it.cavallium.rockserver.distributed;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.io.grpc.ConnectivityState;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Linearizable-read barrier for the leader, built on public Ratis APIs only.
 *
 * <p>Stock Ratis 3.3.1 ReadIndex can reuse a leadership confirmation obtained before the read arrived
 * ({@code ReadIndexHeartbeats.addAppendEntriesListener} returns null when the commit index did not move), so a
 * deposed leader may serve stale reads. This barrier instead:</p>
 * <ol>
 *   <li>requires this division to be the leader and ready (it committed an entry of its own term), and records
 *       the commit index {@code c} when {@link #await} is called;</li>
 *   <li>joins the next confirmation round that starts after the call: every voting peer of the current (and,
 *       during a membership change, previous) configuration is asked for its current term via
 *       {@link TermService}; the round succeeds when a majority of each configuration, counting this node,
 *       reports this leader's term; any reply with a higher term fails it;</li>
 *   <li>waits until the local applied index reaches {@code c}.</li>
 * </ol>
 * <p>Concurrent calls share rounds. A round costs one network round trip, no fsync. At most
 * {@link #ROUNDS_IN_FLIGHT} rounds run at once, so a call that arrives just after a round was sent starts the next
 * one at once instead of waiting for the first to end; calls that arrive while both run share the round after.</p>
 * <p>Each peer has at most {@link #QUERIES_PER_PEER} term queries in flight, whose deadline is the end of the
 * rounds that sent them, so a paused or dead peer does not pile up calls, nor connection attempts, while rounds keep
 * succeeding without it. A reply counts only for the rounds that sent the query, so only if the query was sent after
 * the read arrived: rounds that find every query slot to a peer taken send one query together once a slot frees, if
 * they still need that peer and have time left; meanwhile the peer simply does not contribute to them.</p>
 * <p>{@link #readIndex()} serves a follower's read: the commit index recorded when it was called, and the term, once
 * leadership was confirmed after the call. The follower then waits for its own apply to reach it.</p>
 * <p>The returned futures complete on threads of this barrier, never on gRPC, Ratis apply or JDK timer threads,
 * so callers may run blocking work, such as a database read, in their continuations.</p>
 */
final class ReadBarrier implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(ReadBarrier.class);
	/** Failures to send to one peer are logged at most this often. */
	private static final long SEND_FAILURE_LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

	/** Thrown (as the future's failure) when this node is not, or no longer, a ready leader. */
	static final class NotLeaderException extends Exception {
		NotLeaderException(String message) {
			super(message);
		}
	}

	/** Thrown (as the future's failure) when leadership was confirmed but the apply did not catch up in time. */
	static final class ApplyLagException extends Exception {
		ApplyLagException(long index, long applied) {
			super("The leader is still applying earlier entries: applied " + applied + " of " + index);
		}
	}

	/**
	 * A read index ({@link #readIndex()}): the commit index when the request arrived, and the term in which this node
	 * confirmed its leadership. A follower serves a read with it only while its database holds no entry of a later term
	 * ({@link Router}): the leader of a later term may not have turned {@link #followerReads} on yet.
	 */
	record ReadIndex(long index, long term) {}

	/** Rounds that may run at once. */
	static final int ROUNDS_IN_FLIGHT = 2;
	/** Term queries that may be in flight to one peer at once. */
	static final int QUERIES_PER_PEER = ROUNDS_IN_FLIGHT;

	/**
	 * A caller of the next round: {@code commitIndex} is the commit index when it arrived, which its future completes
	 * with; {@code applyIndex} is the applied index it waits for, -1 for none.
	 */
	private record Waiter(long term, long commitIndex, long applyIndex, CompletableFuture<Long> future) {}

	/** What a caller waits for, once leadership is confirmed. */
	private enum Wait {
		/** Every entry committed before the call is applied here. */
		COMMITTED,
		/** The entries of earlier leaders are applied here: see {@link #awaitLeadership()}. */
		LEADERSHIP,
		/** Nothing: the caller only wants the commit index. */
		NOTHING
	}

	private final RaftServer server;
	private final RaftGroupId groupId;
	private final AppliedIndex applied;
	private final Duration roundTimeout;
	/** Channels to the other peers' Raft ports, by address, so that a peer whose address changes gets a new one. */
	private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
	/** The term queries to each other voting peer. */
	private final Map<RaftPeerId, PeerLink> links = new ConcurrentHashMap<>();
	/** Every future returned by {@link #await} and not completed yet, failed by {@link #close}. */
	private final Set<CompletableFuture<Long>> outstanding = ConcurrentHashMap.newKeySet();
	/**
	 * Completes the callers' futures and starts queued rounds; see {@link #execute}. In a node, the platform threads of
	 * its control plane ({@link NodeThreads#control()}), which the node shuts down.
	 */
	private final ExecutorService executor;
	/** {@link #executor} is this barrier's own, shut down by {@link #close}. */
	private final boolean ownsExecutor;

	private final Object lock = new Object();
	/** Waiters of the next round, whose RPCs have not been sent yet. Guarded by {@link #lock}. */
	private List<Waiter> nextRound = new ArrayList<>();
	/** Rounds whose RPCs were sent and that have not ended. Guarded by {@link #lock}. */
	private int roundsRunning;
	private volatile boolean closed;
	/**
	 * Followers may serve reads: {@link #awaitLeadership()} then waits for the whole commit index, as {@link #await()}.
	 * Set by the node's configuration, and for good by the first {@link #readIndex()}.
	 */
	volatile boolean followerReads;

	ReadBarrier(RaftServer server, RaftGroupId groupId, AppliedIndex applied, Duration roundTimeout) {
		this(server, groupId, applied, roundTimeout, _ -> -1);
	}

	/**
	 * @param readyIndex for a term this node leads, the log index it must have applied before it serves as its
	 *                   leader: Ratis counts a leader ready once it handed over the entries of earlier leaders, not
	 *                   once the state machine applied them
	 */
	ReadBarrier(RaftServer server, RaftGroupId groupId, AppliedIndex applied, Duration roundTimeout,
			java.util.function.LongUnaryOperator readyIndex) {
		this(server, groupId, applied, roundTimeout, readyIndex, null);
	}

	/**
	 * @param executor runs the rounds and completes the callers' futures; null for one of the barrier's own (virtual
	 *                 threads). A given one is not shut down by {@link #close}
	 */
	ReadBarrier(RaftServer server, RaftGroupId groupId, AppliedIndex applied, Duration roundTimeout,
			java.util.function.LongUnaryOperator readyIndex, @Nullable ExecutorService executor) {
		this.ownsExecutor = executor == null;
		this.executor = executor != null ? executor : Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
				.name("rockserver-read-barrier-", 0)
				.factory());
		this.server = server;
		this.groupId = groupId;
		this.applied = applied;
		this.roundTimeout = roundTimeout;
		this.readyIndex = readyIndex;
	}

	private final java.util.function.LongUnaryOperator readyIndex;

	/**
	 * Completes when every write committed before this call is visible in the local database and this node was
	 * the leader after this call. Fails with {@link NotLeaderException} or {@link java.util.concurrent.TimeoutException}.
	 */
	CompletableFuture<Void> await() {
		return enqueue(Wait.COMMITTED).thenApply(_ -> null);
	}

	/**
	 * Completes when this node was the ready leader after this call; unlike {@link #await()}, it does not wait for
	 * the entries committed before this call to be applied, only for those of earlier leaders (see the constructor).
	 * That is enough when the state machine acknowledges a write only after this leader applied it, and the caller
	 * reads only acknowledged writes: every entry of earlier leaders is then applied, and any write acknowledged in
	 * its term was applied before its acknowledgement.
	 *
	 * <p>Not with follower reads ({@link #followerReads}): a follower applies an entry once it learns that it is
	 * committed, possibly before this leader applied it and acknowledged its write, and a client may read it there.
	 * A read that starts after that one ended must see the entry too, here as well, so it waits for the commit index,
	 * as {@link #await()} does. A follower serves only entries of terms whose leader answered it a read index, so had
	 * this flag on: a leader that never answered one keeps the shortcut, whatever the other nodes' configuration.</p>
	 */
	CompletableFuture<Void> awaitLeadership() {
		return enqueue(Wait.LEADERSHIP).thenApply(_ -> null);
	}

	/**
	 * Completes with this node's commit index at the time of this call, once this node was the ready leader after
	 * this call; it does not wait for any apply. A follower that applied up to it may serve a linearizable read: it
	 * then has every entry committed before the read arrived, including every acknowledged write. Not
	 * {@link #awaitLeadership()}'s shortcut: the follower did not apply this leader's entries in its own apply.
	 */
	CompletableFuture<ReadIndex> readIndex() {
		// Before the answer: once a follower has it, it may expose entries this leader did not apply yet.
		followerReads = true;
		var term = new long[1];
		return enqueue(Wait.NOTHING, term).thenApply(index -> new ReadIndex(index, term[0]));
	}

	private CompletableFuture<Long> enqueue(Wait wait) {
		return enqueue(wait, null);
	}

	/** @param termOut if not null, receives the term the caller's round confirms, before the future is returned */
	private CompletableFuture<Long> enqueue(Wait wait, long @Nullable [] termOut) {
		long term;
		long commitIndex;
		try {
			var division = server.getDivision(groupId);
			var info = division.getInfo();
			term = info.getCurrentTerm();
			if (!info.isLeader() || !info.isLeaderReady()) {
				return CompletableFuture.failedFuture(new NotLeaderException("Not the ready leader of term " + term));
			}
			commitIndex = division.getRaftLog().getLastCommittedIndex();
			if (info.getCurrentTerm() != term) {
				return CompletableFuture.failedFuture(new NotLeaderException("Term changed from " + term));
			}
		} catch (IOException e) {
			return CompletableFuture.failedFuture(new NotLeaderException("No division for " + groupId + ": " + e));
		}

		if (termOut != null) {
			termOut[0] = term;
		}
		var future = new CompletableFuture<Long>();
		long applyIndex = switch (wait) {
			case COMMITTED -> commitIndex;
			case LEADERSHIP -> followerReads ? commitIndex : Math.min(commitIndex, readyIndex.applyAsLong(term));
			case NOTHING -> -1;
		};
		List<Waiter> round;
		synchronized (lock) {
			if (closed) {
				return CompletableFuture.failedFuture(new NotLeaderException("Read barrier closed"));
			}
			outstanding.add(future);
			nextRound.add(new Waiter(term, commitIndex, applyIndex, future));
			round = takeNextRoundIfIdle();
		}
		future.whenComplete((_, _) -> outstanding.remove(future));
		if (round != null) {
			startRound(round);
		}
		return future;
	}

	@Override
	public void close() {
		synchronized (lock) {
			if (closed) {
				return;
			}
			closed = true;
			nextRound = new ArrayList<>();
		}
		// On the closing thread: the executor is about to stop.
		for (var future : outstanding) {
			future.completeExceptionally(new NotLeaderException("Read barrier closed"));
		}
		closeChannels();
		if (ownsExecutor) {
			executor.shutdown();
		}
	}

	/** The waiters of the next round, if fewer than {@link #ROUNDS_IN_FLIGHT} run; the caller must then run it. */
	private @Nullable List<Waiter> takeNextRoundIfIdle() {
		assert Thread.holdsLock(lock);
		if (roundsRunning >= ROUNDS_IN_FLIGHT || closed || nextRound.isEmpty()) {
			return null;
		}
		var round = nextRound;
		nextRound = new ArrayList<>();
		roundsRunning++;
		return round;
	}

	/**
	 * Sends the RPCs of a round now and, when it ends, completes its waiters and starts the round that queued up
	 * behind it. At most {@link #ROUNDS_IN_FLIGHT} rounds run at a time.
	 */
	private void startRound(List<Waiter> waiters) {
		CompletableFuture<Long> outcome;
		try {
			outcome = confirmLeadership(waiters);
		} catch (RuntimeException e) {
			// Never leave the round counted as running: that would stall every later barrier.
			outcome = CompletableFuture.failedFuture(e);
		}
		// A round ends on a gRPC thread inside Votes' monitor, on the JDK timeout thread or, for a single voter,
		// on the caller's thread: none of them should go on to run the next round.
		outcome.whenCompleteAsync((term, error) -> {
			finish(waiters, term, error);
			var next = afterRound();
			if (next != null) {
				startRound(next);
			}
		}, this::execute);
	}

	private @Nullable List<Waiter> afterRound() {
		synchronized (lock) {
			roundsRunning--;
			return takeNextRoundIfIdle();
		}
	}

	/**
	 * Completes each waiter once the applied index reaches its commit index, or fails it. Each waiter is completed
	 * by a task of its own, so that callers' continuations run in parallel and never on the Ratis apply thread.
	 */
	private void finish(List<Waiter> waiters, @Nullable Long confirmedTerm, @Nullable Throwable error) {
		for (var waiter : waiters) {
			if (error != null) {
				execute(() -> waiter.future.completeExceptionally(error));
			} else if (waiter.term != confirmedTerm) {
				var notLeader = new NotLeaderException(
						"Leadership of term " + waiter.term + " ended, current term " + confirmedTerm);
				execute(() -> waiter.future.completeExceptionally(notLeader));
			} else if (waiter.applyIndex < 0) {
				execute(() -> waiter.future.complete(waiter.commitIndex));
			} else {
				// Bounded like the round: a new leader far behind in applying earlier terms fails its reads in time.
				applied.await(waiter.applyIndex).orTimeout(roundTimeout.toNanos(), TimeUnit.NANOSECONDS)
						.whenCompleteAsync((_, applyError) -> {
							if (applyError == null) {
								waiter.future.complete(waiter.commitIndex);
							} else {
								waiter.future.completeExceptionally(new ApplyLagException(waiter.applyIndex, applied.get()));
							}
						}, this::execute);
			}
		}
	}

	/** Runs {@code task} on this barrier's executor, or right here once it is closed. */
	private void execute(Runnable task) {
		try {
			executor.execute(task);
		} catch (RejectedExecutionException e) {
			// Closed: every caller's future has already failed, so the task only finishes the bookkeeping.
			task.run();
		}
	}

	/**
	 * Starts one confirmation round. Its RPCs are sent now, after every waiter of the round arrived. Completes with
	 * the confirmed term once a majority of each configuration (counting this node) reported it.
	 */
	private CompletableFuture<Long> confirmLeadership(List<Waiter> waiters) {
		RaftServer.Division division;
		try {
			division = server.getDivision(groupId);
		} catch (IOException e) {
			return CompletableFuture.failedFuture(new NotLeaderException("No division for " + groupId + ": " + e));
		}
		// Each waiter saw this node lead in its own term. Terms only grow, so a round confirms at most the
		// latest of them; waiters of an earlier term fail in finish().
		long term = waiters.stream().mapToLong(Waiter::term).max().orElseThrow();
		if (!isLeaderInTerm(division, term)) {
			return CompletableFuture.failedFuture(new NotLeaderException("No longer the leader of term " + term));
		}
		var conf = division.getRaftConf();
		long deadline = System.nanoTime() + roundTimeout.toNanos();
		var votes = new Votes(division, term, deadline, conf.getCurrentPeers(), conf.getPreviousPeers());

		Map<RaftPeerId, RaftPeer> others = new LinkedHashMap<>();
		for (var peers : List.of(conf.getCurrentPeers(), conf.getPreviousPeers())) {
			for (var peer : peers) {
				if (!peer.getId().equals(division.getId())) {
					others.putIfAbsent(peer.getId(), peer);
				}
			}
		}
		dropUnusedChannels(others.values());
		links.keySet().retainAll(others.keySet());
		votes.check();
		for (var peer : others.values()) {
			if (votes.outcome.isDone()) {
				break;
			}
			if (peer.getAddress() == null) {
				continue;
			}
			link(peer).ask(votes);
		}
		return votes.outcome.orTimeout(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
	}

	/** The link to {@code peer}, a new one if its address changed. */
	private PeerLink link(RaftPeer peer) {
		return links.compute(peer.getId(), (id, link) -> link != null && link.address.equals(peer.getAddress())
				? link
				: new PeerLink(id, peer.getAddress()));
	}

	/** True if this division is the leader and its term is {@code term}. */
	private static boolean isLeaderInTerm(RaftServer.Division division, long term) {
		var info = division.getInfo();
		// Reading the term on both sides of isLeader() pins the leadership to that term.
		return info.getCurrentTerm() == term && info.isLeader() && info.getCurrentTerm() == term;
	}

	/** Term replies of one round. */
	private static final class Votes {

		final CompletableFuture<Long> outcome = new CompletableFuture<>();
		/** When the round times out, in {@link System#nanoTime()} units; no query of the round outlives it. */
		final long deadline;
		private final RaftServer.Division self;
		private final long term;
		private final Set<RaftPeerId> current;
		/** Empty unless a membership change is in progress. */
		private final Set<RaftPeerId> previous;
		private final Set<RaftPeerId> confirmed = new HashSet<>();

		Votes(RaftServer.Division self,
				long term,
				long deadline,
				Iterable<RaftPeer> current,
				Iterable<RaftPeer> previous) {
			this.self = self;
			this.term = term;
			this.deadline = deadline;
			this.current = ids(current);
			this.previous = ids(previous);
			confirmed.add(self.getId());
		}

		synchronized void onReply(RaftPeerId peer, long peerTerm) {
			if (peerTerm > term) {
				outcome.completeExceptionally(new NotLeaderException(
						"Peer " + peer + " is in term " + peerTerm + ", after this leader's term " + term));
			} else if (peerTerm == term) {
				confirmed.add(peer);
				check();
			}
		}

		synchronized void check() {
			if (outcome.isDone() || !hasMajority(current) || !hasMajority(previous)) {
				return;
			}
			if (isLeaderInTerm(self, term)) {
				outcome.complete(term);
			} else {
				outcome.completeExceptionally(new NotLeaderException("No longer the leader of term " + term));
			}
		}

		private boolean hasMajority(Set<RaftPeerId> voters) {
			if (voters.isEmpty()) {
				return true;
			}
			long count = voters.stream().filter(confirmed::contains).count();
			return count > voters.size() / 2;
		}

		private static Set<RaftPeerId> ids(Iterable<RaftPeer> peers) {
			var ids = new HashSet<RaftPeerId>();
			peers.forEach(peer -> ids.add(peer.getId()));
			return ids;
		}
	}

	/**
	 * The term queries to one peer, at most {@link #QUERIES_PER_PEER} in flight at a time. Without this limit every
	 * round would send its own query to a paused peer, and each would stay pending until its deadline.
	 */
	private final class PeerLink {

		final RaftPeerId id;
		final String address;
		/** Guarded by this. */
		private int inFlight;
		/**
		 * Rounds that found every slot taken and need a query of their own once one frees: they share it. Guarded by
		 * this.
		 */
		private final List<Votes> waiting = new ArrayList<>();
		/** When a failure to send to this peer was last logged, see {@link #SEND_FAILURE_LOG_INTERVAL_NANOS}. */
		private volatile long lastSendFailureLogNanos = System.nanoTime() - SEND_FAILURE_LOG_INTERVAL_NANOS;

		PeerLink(RaftPeerId id, String address) {
			this.id = id;
			this.address = address;
		}

		/** Asks the peer for its term on behalf of {@code votes}, now or once a query in flight ends. */
		void ask(Votes votes) {
			synchronized (this) {
				if (inFlight >= QUERIES_PER_PEER) {
					// The queries in flight were sent before this round started: their replies cannot count for it.
					waiting.removeIf(round -> round.outcome.isDone());
					waiting.add(votes);
					return;
				}
				inFlight++;
			}
			send(List.of(votes));
		}

		/**
		 * Sends one query on behalf of {@code rounds}, which own a slot of the link; if none of them needs it any more,
		 * passes the slot on to the rounds waiting for one, or frees it.
		 */
		private void send(@Nullable List<Votes> rounds) {
			while (rounds != null) {
				long now = System.nanoTime();
				var live = rounds.stream().filter(round -> !round.outcome.isDone() && round.deadline - now > 0).toList();
				if (!closed && !live.isEmpty()) {
					long remaining = live.stream().mapToLong(round -> round.deadline - now).max().orElseThrow();
					try {
						TermService.currentTerm(channel(address), groupId, Duration.ofNanos(remaining))
								.whenComplete((peerTerm, error) -> {
									var next = release();
									if (error == null) {
										for (var round : live) {
											round.onReply(id, peerTerm);
										}
									}
									send(next);
								});
						return;
					} catch (RuntimeException e) {
						// Such as an unusable address: this peer does not contribute to these rounds. Said in the log, or
						// a misconfigured peer would show only as rounds that time out.
						long failedAt = System.nanoTime();
						if (failedAt - lastSendFailureLogNanos >= SEND_FAILURE_LOG_INTERVAL_NANOS) {
							lastSendFailureLogNanos = failedAt;
							LOG.warn("Read barrier: cannot ask peer {} at {} for its term; it does not count toward"
									+ " leadership confirmations (logged at most once a minute per peer)", id, address, e);
						}
					}
				}
				rounds = release();
			}
		}

		/** Ends a query in flight; returns the rounds that now own its slot and must send their own, if any. */
		private synchronized @Nullable List<Votes> release() {
			if (waiting.isEmpty()) {
				inFlight--;
				return null;
			}
			var next = List.copyOf(waiting);
			waiting.clear();
			return next;
		}
	}

	private ManagedChannel channel(String address) {
		var channel = channels.computeIfAbsent(address,
				_ -> NettyChannelBuilder.forTarget(address).usePlaintext().build());
		if (closed) {
			// close() may have swept the channels before this one was added.
			closeChannels();
		}
		// While a peer is down, gRPC waits longer and longer (up to two minutes) between attempts to reach it, and
		// fails calls at once in between. A round must reach a peer as soon as it is back, so each query that finds
		// the channel failed makes a fresh attempt: at most one connection attempt per query, and a peer has at
		// most one query in flight, which lasts at most a round timeout.
		if (channel.getState(false) == ConnectivityState.TRANSIENT_FAILURE) {
			channel.resetConnectBackoff();
		}
		return channel;
	}

	private void dropUnusedChannels(Iterable<RaftPeer> peers) {
		var addresses = new HashSet<String>();
		peers.forEach(peer -> addresses.add(peer.getAddress()));
		channels.entrySet().removeIf(entry -> {
			if (addresses.contains(entry.getKey())) {
				return false;
			}
			// shutdown() lets the RPCs of an earlier round finish.
			entry.getValue().shutdown();
			return true;
		});
	}

	private void closeChannels() {
		for (var address : List.copyOf(channels.keySet())) {
			var channel = channels.remove(address);
			if (channel != null) {
				channel.shutdownNow();
			}
		}
	}
}
