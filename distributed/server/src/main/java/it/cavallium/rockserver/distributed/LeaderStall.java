package it.cavallium.rockserver.distributed;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.ratis.proto.RaftProtos.ServerRpcProto;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.jetbrains.annotations.Nullable;

/**
 * Tells when a leader should step down because it cannot commit: a new leader whose first entry got no reply, and a
 * leader whose own Raft log stopped writing.
 *
 * <p>A leader appends an entry when its term starts, and refuses writes until that entry is committed. The Ratis leader
 * moves a follower's match index only on the reply to a request that carried entries: the reply to a heartbeat has no
 * match index. It also moves its next index past an entry when it sends the entry, and keeps it there when the stream
 * fails without telling which request was lost. So if that stream dies after the follower wrote the entry and before its
 * reply arrives, the leader never sends the entry again, and every heartbeat that follows succeeds. With no other
 * majority for the entry, it stays uncommitted and writes stay refused for as long as this leader lasts, because the writes
 * that would carry a new match index are the ones refused. Only a new appender starts over, and a new leader has one.</p>
 *
 * <p>So a leader steps down when no entry of its term is committed, it sent everything it has to a majority of members
 * that keep answering, and the commit index did not move for {@value #ELECTION_TIMEOUTS_TO_STALL} maximum election
 * timeouts, and at least {@link #MIN_STALL}. A reply lost this way never comes, while any other reply takes much less than
 * that. What the rule leaves out keeps its leader:</p>
 * <ul>
 *   <li>A leader that is behind in applying: the rule looks at what is committed, not at what is applied, so a long replay
 *   after a restart is no stall.</li>
 *   <li>A leader with a follower still catching up, from the log or from a snapshot: the leader has not sent it
 *   everything yet, and a step down would make it start over.</li>
 *   <li>A leader without a majority: the rule needs members that answer. Ratis itself steps such a leader down after one
 *   election timeout, and another step down would change nothing.</li>
 * </ul>
 *
 * <p>After its first commit a leader cannot stall like this: the next write it accepts carries entries, and the reply
 * moves the match index past the entry whose reply was lost.</p>
 *
 * <p>A slow disk looks the same as a lost reply: a reply that takes longer than the bound on every leader. A step down
 * then only starts the same wait over, and the bound would end every leader before its first commit. So the bound
 * doubles after each step down of this node, up to {@value #MAX_DOUBLINGS} times, and goes back to its first value when
 * the commit index moves. A lost reply ends at the first step down, so it never pays for this.</p>
 *
 * <p>The other rule, {@link #logStopped}, is for a leader of any age. Ratis commits on a leader only up to what the leader
 * itself flushed ({@code RaftLogBase.updateCommitIndex}), whatever its followers hold, so a leader whose log worker hangs
 * commits nothing, and its heartbeats still keep the followers from an election. That ended only when the queue of the
 * log worker filled up and Ratis, which then blocks with the log lock held, stopped sending heartbeats: after about 40 s
 * with 8 writers of 1 MiB values, and never with a few small writes. So a leader that has entries to flush and flushes
 * nothing for {@value #ELECTION_TIMEOUTS_TO_LOG_STALL} maximum election timeouts, and at least {@link #MIN_LOG_STALL},
 * steps down too. Its bound doubles in the same way, and goes back when a flush takes less than its first value: with
 * disks that are all this slow a commit proves nothing, and a shorter bound would end the next leader before its own.</p>
 */
final class LeaderStall {

	static final int ELECTION_TIMEOUTS_TO_STALL = 5;
	/** Enough for the longest sleep of the leader between two tries and one more request to fit several times. */
	static final Duration MIN_STALL = Duration.ofSeconds(10);
	static final int ELECTION_TIMEOUTS_TO_LOG_STALL = 3;
	/** Far above what an fsync takes on a working disk, and much less than what a hung one costs without this. */
	static final Duration MIN_LOG_STALL = Duration.ofSeconds(5);
	/** The bounds stop growing at 8 times their first value. */
	static final int MAX_DOUBLINGS = 3;

	/** What a leader knows about one follower. */
	record Follower(String id, long nextIndex, long lastReplyMillisAgo) {}

	private final long stallAfterNanos;
	private final long logStallAfterNanos;
	private boolean waiting;
	private long term;
	private long commitIndex;
	private long sinceNanos;
	private boolean logWaiting;
	private long flushIndex;
	private long logSinceNanos;
	/** Step downs of the rule of the first entry since the commit index last moved. */
	private int stepDowns;
	private long commitIndexAtStepDown;
	/** Step downs of the rule of the log since a flush took less than the first bound. */
	private int logStepDowns;

	LeaderStall(Duration stallAfter, Duration logStallAfter) {
		this.stallAfterNanos = stallAfter.toNanos();
		this.logStallAfterNanos = logStallAfter.toNanos();
	}

	static LeaderStall forElectionTimeout(Duration electionTimeoutMax) {
		var stallAfter = electionTimeoutMax.multipliedBy(ELECTION_TIMEOUTS_TO_STALL);
		var logStallAfter = electionTimeoutMax.multipliedBy(ELECTION_TIMEOUTS_TO_LOG_STALL);
		return new LeaderStall(stallAfter.compareTo(MIN_STALL) < 0 ? MIN_STALL : stallAfter,
				logStallAfter.compareTo(MIN_LOG_STALL) < 0 ? MIN_LOG_STALL : logStallAfter);
	}

	/** {@link #shouldStepDown(boolean, long, long, long, boolean)} for a caller that can always step down. */
	boolean shouldStepDown(boolean waiting, long term, long commitIndex, long nowNanos) {
		return shouldStepDown(waiting, term, commitIndex, nowNanos, true);
	}

	/**
	 * Takes the current observation and tells whether the leader should step down now. After a true answer the
	 * doubled bound starts, for a step down that did not happen or that did not help. Call it on every node, leader or
	 * not: the commit index it sees is what brings the bounds back.
	 *
	 * @param waiting whether this node leads, with no entry of its term committed, and {@link #sentToMajority}
	 * @param canStepDown whether the caller can start a step-down now: when not (the previous one is still being started),
	 *     the bound that passed gives no answer, and is neither doubled nor started again, so the next check that can
	 *     act steps down at once (R25 review)
	 */
	boolean shouldStepDown(boolean waiting, long term, long commitIndex, long nowNanos, boolean canStepDown) {
		if (stepDowns > 0 && commitIndex != commitIndexAtStepDown) {
			stepDowns = 0;
		}
		if (!waiting || !this.waiting || term != this.term || commitIndex != this.commitIndex) {
			this.waiting = waiting;
			this.term = term;
			this.commitIndex = commitIndex;
			sinceNanos = nowNanos;
			return false;
		}
		if (nowNanos - sinceNanos < stallAfterNanos << doublings(stepDowns) || !canStepDown) {
			return false;
		}
		sinceNanos = nowNanos;
		stepDowns++;
		commitIndexAtStepDown = commitIndex;
		return true;
	}

	/** {@link #logStopped(boolean, long, long, long, boolean)} for a caller that can always step down. */
	boolean logStopped(boolean leader, long flushIndex, long lastIndex, long nowNanos) {
		return logStopped(leader, flushIndex, lastIndex, nowNanos, true);
	}

	/**
	 * Takes the current observation of the leader's own log and tells whether it should step down now: it has entries to
	 * flush and its flush index stayed where it is for the whole bound. After a true answer the doubled bound starts.
	 *
	 * @param leader whether this node leads
	 * @param flushIndex the last entry of the log that reached the disk
	 * @param lastIndex the last entry of the log
	 * @param canStepDown whether the caller can start a step-down now: when not (the previous one is still being started),
	 *     the bound that passed gives no answer, and is neither doubled nor started again, so the next check that can
	 *     act steps down at once (R25 review)
	 */
	boolean logStopped(boolean leader, long flushIndex, long lastIndex, long nowNanos, boolean canStepDown) {
		boolean waiting = leader && flushIndex < lastIndex;
		if (!waiting || !logWaiting || flushIndex != this.flushIndex) {
			// Only a leader's flush says something about the disk: a step down ends the wait too, right after the bound.
			if (leader && logWaiting && flushIndex != this.flushIndex && nowNanos - logSinceNanos < logStallAfterNanos) {
				logStepDowns = 0;
			}
			logWaiting = waiting;
			this.flushIndex = flushIndex;
			logSinceNanos = nowNanos;
			return false;
		}
		if (nowNanos - logSinceNanos < logStallAfterNanos << doublings(logStepDowns) || !canStepDown) {
			return false;
		}
		logSinceNanos = nowNanos;
		logStepDowns++;
		return true;
	}

	private static int doublings(int stepDowns) {
		return Math.min(stepDowns, MAX_DOUBLINGS);
	}

	/**
	 * This node leads, no entry of its term is committed, and a majority of members has been sent all of its log and
	 * still answers.
	 *
	 * @param answeredWithinMillis how recent a follower's last reply must be for it to count
	 */
	static boolean waitsForReplyOfSentEntry(RaftServer.Division division, String self, long answeredWithinMillis) {
		var info = division.getInfo();
		var log = division.getRaftLog();
		if (!info.isLeader()) {
			return false;
		}
		var committed = log.getTermIndex(log.getLastCommittedIndex());
		if (committed != null && committed.getTerm() == info.getCurrentTerm()) {
			return false;
		}
		// Ratis gives the next indices without follower ids, in the order of its log appenders, the same order as
		// the follower list of the role info. Each is a separate snapshot of that list, so the indices are paired with
		// ids only when the list has the same followers in the same order before and after them.
		var before = info.getRoleInfoProto().getLeaderInfo().getFollowerInfoList();
		var nextIndices = info.getFollowerNextIndices();
		var after = info.getRoleInfoProto().getLeaderInfo().getFollowerInfoList();
		var followers = pairById(before, nextIndices, after);
		if (followers == null) {
			return false;
		}
		var conf = division.getRaftConf();
		return sentToMajority(followers, log.getNextIndex(), answeredWithinMillis, self, peerIds(conf.getCurrentPeers()),
				peerIds(conf.getPreviousPeers()));
	}

	/**
	 * The followers of {@code nextIndices}, with the latest reply times of {@code after}; null if the follower lists
	 * read around the indices differ in ids or order, or in length from the indices: they cannot then be paired.
	 */
	static @Nullable List<Follower> pairById(List<ServerRpcProto> before, long @Nullable [] nextIndices,
			List<ServerRpcProto> after) {
		if (nextIndices == null || before.size() != nextIndices.length || after.size() != nextIndices.length) {
			return null;
		}
		var followers = new ArrayList<Follower>(nextIndices.length);
		for (int i = 0; i < nextIndices.length; i++) {
			var id = before.get(i).getId().getId();
			if (!id.equals(after.get(i).getId().getId())) {
				return null;
			}
			followers.add(new Follower(RaftPeerId.valueOf(id).toString(), nextIndices[i],
					after.get(i).getLastRpcElapsedTimeMs()));
		}
		return followers;
	}

	private static List<String> peerIds(Collection<RaftPeer> peers) {
		return peers.stream().map(peer -> peer.getId().toString()).toList();
	}

	/**
	 * Whether the leader sent its whole log to a majority of members, counting itself, in the current configuration and
	 * in the previous one while both count: only followers that answered within {@code answeredWithinMillis} count. A
	 * follower that is not a member, because it is still catching up to join, does not.
	 *
	 * @param logNextIndex the index that the leader's next entry will get
	 * @param previousMembers the previous configuration, empty if there is none
	 */
	static boolean sentToMajority(List<Follower> followers, long logNextIndex, long answeredWithinMillis, String self,
			Collection<String> members, Collection<String> previousMembers) {
		var reached = new HashSet<String>();
		reached.add(self);
		for (var follower : followers) {
			if (follower.nextIndex() >= logNextIndex && follower.lastReplyMillisAgo() <= answeredWithinMillis) {
				reached.add(follower.id());
			}
		}
		return isMajority(members, reached) && (previousMembers.isEmpty() || isMajority(previousMembers, reached));
	}

	private static boolean isMajority(Collection<String> members, HashSet<String> reached) {
		return 2 * members.stream().filter(reached::contains).count() > members.size();
	}
}
