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
 * <p>Concurrent calls share rounds. A round costs one network round trip, no fsync.</p>
 * <p>Each peer has at most one term query in flight, whose deadline is the end of the round that sent it, so a
 * paused or dead peer does not pile up calls, nor connection attempts, while rounds keep succeeding without it. A
 * reply counts only for the round that sent the query, so only if the query was sent after the read arrived: a
 * round that finds an earlier round's query still in flight to a peer sends its own once that one ends, if it
 * still needs that peer and has time left; meanwhile the peer simply does not contribute to it.</p>
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

	/** A caller of the next round; {@code commitIndex} is the applied index it waits for, -1 for none. */
	private record Waiter(long term, long commitIndex, CompletableFuture<Void> future) {}

	private final RaftServer server;
	private final RaftGroupId groupId;
	private final AppliedIndex applied;
	private final Duration roundTimeout;
	/** Channels to the other peers' Raft ports, by address, so that a peer whose address changes gets a new one. */
	private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
	/** The term queries to each other voting peer. */
	private final Map<RaftPeerId, PeerLink> links = new ConcurrentHashMap<>();
	/** Every future returned by {@link #await} and not completed yet, failed by {@link #close}. */
	private final Set<CompletableFuture<Void>> outstanding = ConcurrentHashMap.newKeySet();
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
	/** Guarded by {@link #lock}. */
	private boolean roundRunning;
	private volatile boolean closed;

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
		return await(true);
	}

	/**
	 * Completes when this node was the ready leader after this call; unlike {@link #await()}, it does not wait for
	 * the entries committed before this call to be applied, only for those of earlier leaders (see the constructor).
	 * That is enough when the state machine acknowledges a write only after this leader applied it, and the caller
	 * reads only acknowledged writes: every entry of earlier leaders is then applied, and any write acknowledged in
	 * its term was applied before its acknowledgement.
	 */
	CompletableFuture<Void> awaitLeadership() {
		return await(false);
	}

	private CompletableFuture<Void> await(boolean waitForApply) {
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

		var future = new CompletableFuture<Void>();
		List<Waiter> round;
		synchronized (lock) {
			if (closed) {
				return CompletableFuture.failedFuture(new NotLeaderException("Read barrier closed"));
			}
			outstanding.add(future);
			nextRound.add(new Waiter(term, waitForApply ? commitIndex : Math.min(commitIndex, readyIndex.applyAsLong(term)),
					future));
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

	/** The waiters of the next round, if no round is running; the caller must then run it. */
	private @Nullable List<Waiter> takeNextRoundIfIdle() {
		assert Thread.holdsLock(lock);
		if (roundRunning || closed || nextRound.isEmpty()) {
			return null;
		}
		var round = nextRound;
		nextRound = new ArrayList<>();
		roundRunning = true;
		return round;
	}

	/**
	 * Sends the RPCs of a round now and, when it ends, completes its waiters and starts the round that queued up
	 * behind it. At most one round runs at a time.
	 */
	private void startRound(List<Waiter> waiters) {
		CompletableFuture<Long> outcome;
		try {
			outcome = confirmLeadership(waiters);
		} catch (RuntimeException e) {
			// Never leave roundRunning set: that would stall every later barrier.
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
			roundRunning = false;
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
			} else if (waiter.commitIndex < 0) {
				execute(() -> waiter.future.complete(null));
			} else {
				// Bounded like the round: a new leader far behind in applying earlier terms fails its reads in time.
				applied.await(waiter.commitIndex).orTimeout(roundTimeout.toNanos(), TimeUnit.NANOSECONDS)
						.whenCompleteAsync((_, applyError) -> {
							if (applyError == null) {
								waiter.future.complete(null);
							} else {
								waiter.future.completeExceptionally(new ApplyLagException(waiter.commitIndex, applied.get()));
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
	 * The term queries to one peer, at most one in flight at a time. Without this limit every round would send its
	 * own query to a paused peer, and each would stay pending until its deadline.
	 */
	private final class PeerLink {

		final RaftPeerId id;
		final String address;
		/** Guarded by this. */
		private boolean inFlight;
		/** The round that found a query in flight and needs one of its own once that one ends. Guarded by this. */
		private @Nullable Votes waiting;
		/** When a failure to send to this peer was last logged, see {@link #SEND_FAILURE_LOG_INTERVAL_NANOS}. */
		private volatile long lastSendFailureLogNanos = System.nanoTime() - SEND_FAILURE_LOG_INTERVAL_NANOS;

		PeerLink(RaftPeerId id, String address) {
			this.id = id;
			this.address = address;
		}

		/** Asks the peer for its term on behalf of {@code votes}, now or once the query in flight ends. */
		void ask(Votes votes) {
			synchronized (this) {
				if (inFlight) {
					// The query in flight was sent before this round started: its reply cannot count for it.
					// Rounds run one at a time, so any round already waiting here has ended: this one replaces it.
					waiting = votes;
					return;
				}
				inFlight = true;
			}
			send(votes);
		}

		/**
		 * Sends the query of {@code votes}, which owns the link; if that round no longer needs it, passes the link
		 * on to the round waiting behind it, or frees it.
		 */
		private void send(@Nullable Votes votes) {
			while (votes != null) {
				long remaining = votes.deadline - System.nanoTime();
				if (!closed && !votes.outcome.isDone() && remaining > 0) {
					var round = votes;
					try {
						TermService.currentTerm(channel(address), groupId, Duration.ofNanos(remaining))
								.whenComplete((peerTerm, error) -> {
									var next = release();
									if (error == null) {
										round.onReply(id, peerTerm);
									}
									send(next);
								});
						return;
					} catch (RuntimeException e) {
						// Such as an unusable address: this peer does not contribute to the round. Said in the log, or
						// a misconfigured peer would show only as rounds that time out.
						long now = System.nanoTime();
						if (now - lastSendFailureLogNanos >= SEND_FAILURE_LOG_INTERVAL_NANOS) {
							lastSendFailureLogNanos = now;
							LOG.warn("Read barrier: cannot ask peer {} at {} for its term; it does not count toward"
									+ " leadership confirmations (logged at most once a minute per peer)", id, address, e);
						}
					}
				}
				votes = release();
			}
		}

		/** Ends the query in flight; returns the round that now owns the link and must send its own, if any. */
		private synchronized @Nullable Votes release() {
			var next = waiting;
			waiting = null;
			inFlight = next != null;
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
