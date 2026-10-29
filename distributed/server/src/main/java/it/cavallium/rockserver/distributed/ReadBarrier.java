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
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.jetbrains.annotations.Nullable;

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
 */
final class ReadBarrier implements Closeable {

	/** Thrown (as the future's failure) when this node is not, or no longer, a ready leader. */
	static final class NotLeaderException extends Exception {
		NotLeaderException(String message) {
			super(message);
		}
	}

	private record Waiter(long term, long commitIndex, CompletableFuture<Void> future) {}

	private final RaftServer server;
	private final RaftGroupId groupId;
	private final AppliedIndex applied;
	private final Duration roundTimeout;
	/** Channels to the other peers' Raft ports, by address, so that a peer whose address changes gets a new one. */
	private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
	/** Every future returned by {@link #await} and not completed yet, failed by {@link #close}. */
	private final Set<CompletableFuture<Void>> outstanding = ConcurrentHashMap.newKeySet();

	private final Object lock = new Object();
	/** Waiters of the next round, whose RPCs have not been sent yet. Guarded by {@link #lock}. */
	private List<Waiter> nextRound = new ArrayList<>();
	/** Guarded by {@link #lock}. */
	private boolean roundRunning;
	private volatile boolean closed;

	ReadBarrier(RaftServer server, RaftGroupId groupId, AppliedIndex applied, Duration roundTimeout) {
		this.server = server;
		this.groupId = groupId;
		this.applied = applied;
		this.roundTimeout = roundTimeout;
	}

	/**
	 * Completes when every write committed before this call is visible in the local database and this node was
	 * the leader after this call. Fails with {@link NotLeaderException} or {@link java.util.concurrent.TimeoutException}.
	 */
	CompletableFuture<Void> await() {
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
			nextRound.add(new Waiter(term, commitIndex, future));
			round = takeNextRoundIfIdle();
		}
		future.whenComplete((_, _) -> outstanding.remove(future));
		runRounds(round);
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
		for (var future : outstanding) {
			future.completeExceptionally(new NotLeaderException("Read barrier closed"));
		}
		closeChannels();
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

	/** Runs {@code round} and then every round that queued up behind it. At most one round runs at a time. */
	private void runRounds(@Nullable List<Waiter> round) {
		// A loop rather than recursion: rounds that complete immediately (a single voter, or no longer leader)
		// would otherwise nest one stack frame per round.
		while (round != null) {
			var waiters = round;
			CompletableFuture<Long> outcome;
			try {
				outcome = confirmLeadership(waiters);
			} catch (RuntimeException e) {
				// Never leave roundRunning set: that would stall every later barrier.
				outcome = CompletableFuture.failedFuture(e);
			}
			if (!outcome.isDone()) {
				outcome.whenComplete((term, error) -> {
					finish(waiters, term, error);
					runRounds(afterRound());
				});
				return;
			}
			// Already complete: whenComplete runs right here.
			outcome.whenComplete((term, error) -> finish(waiters, term, error));
			round = afterRound();
		}
	}

	private @Nullable List<Waiter> afterRound() {
		synchronized (lock) {
			roundRunning = false;
			return takeNextRoundIfIdle();
		}
	}

	/** Completes each waiter once the applied index reaches its commit index, or fails it. */
	private void finish(List<Waiter> waiters, @Nullable Long confirmedTerm, @Nullable Throwable error) {
		for (var waiter : waiters) {
			if (error != null) {
				waiter.future.completeExceptionally(error);
			} else if (waiter.term != confirmedTerm) {
				waiter.future.completeExceptionally(new NotLeaderException(
						"Leadership of term " + waiter.term + " ended, current term " + confirmedTerm));
			} else {
				applied.await(waiter.commitIndex).thenRun(() -> waiter.future.complete(null));
			}
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
		var votes = new Votes(division, term, conf.getCurrentPeers(), conf.getPreviousPeers());

		Map<RaftPeerId, RaftPeer> others = new LinkedHashMap<>();
		for (var peers : List.of(conf.getCurrentPeers(), conf.getPreviousPeers())) {
			for (var peer : peers) {
				if (!peer.getId().equals(division.getId())) {
					others.putIfAbsent(peer.getId(), peer);
				}
			}
		}
		dropUnusedChannels(others.values());
		votes.check();
		for (var peer : others.values()) {
			if (votes.outcome.isDone()) {
				break;
			}
			if (peer.getAddress() == null) {
				continue;
			}
			TermService.currentTerm(channel(peer.getAddress()), groupId, roundTimeout)
					.whenComplete((peerTerm, error) -> {
						if (error == null) {
							votes.onReply(peer.getId(), peerTerm);
						}
					});
		}
		return votes.outcome.orTimeout(roundTimeout.toNanos(), TimeUnit.NANOSECONDS);
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
		private final RaftServer.Division self;
		private final long term;
		private final Set<RaftPeerId> current;
		/** Empty unless a membership change is in progress. */
		private final Set<RaftPeerId> previous;
		private final Set<RaftPeerId> confirmed = new HashSet<>();

		Votes(RaftServer.Division self, long term, Iterable<RaftPeer> current, Iterable<RaftPeer> previous) {
			this.self = self;
			this.term = term;
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

	private ManagedChannel channel(String address) {
		var channel = channels.computeIfAbsent(address,
				_ -> NettyChannelBuilder.forTarget(address).usePlaintext().build());
		if (closed) {
			// close() may have swept the channels before this one was added.
			closeChannels();
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
