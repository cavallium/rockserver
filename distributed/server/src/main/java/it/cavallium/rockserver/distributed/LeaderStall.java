package it.cavallium.rockserver.distributed;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * Tells when a new leader needs a new election to commit the first entry of its term.
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
 */
final class LeaderStall {

	static final int ELECTION_TIMEOUTS_TO_STALL = 5;
	/** Enough for the longest sleep of the leader between two tries and one more request to fit several times. */
	static final Duration MIN_STALL = Duration.ofSeconds(10);

	/** What a leader knows about one follower. */
	record Follower(String id, long nextIndex, long lastReplyMillisAgo) {}

	private final long stallAfterNanos;
	private boolean waiting;
	private long term;
	private long commitIndex;
	private long sinceNanos;

	LeaderStall(Duration stallAfter) {
		this.stallAfterNanos = stallAfter.toNanos();
	}

	static LeaderStall forElectionTimeout(Duration electionTimeoutMax) {
		var stallAfter = electionTimeoutMax.multipliedBy(ELECTION_TIMEOUTS_TO_STALL);
		return new LeaderStall(stallAfter.compareTo(MIN_STALL) < 0 ? MIN_STALL : stallAfter);
	}

	/**
	 * Takes the current observation and tells whether the leader should step down now. After a true answer the whole
	 * bound starts again, for a step down that did not happen.
	 *
	 * @param waiting whether this node leads, with no entry of its term committed, and {@link #sentToMajority}
	 */
	boolean shouldStepDown(boolean waiting, long term, long commitIndex, long nowNanos) {
		if (!waiting || !this.waiting || term != this.term || commitIndex != this.commitIndex) {
			this.waiting = waiting;
			this.term = term;
			this.commitIndex = commitIndex;
			sinceNanos = nowNanos;
			return false;
		}
		if (nowNanos - sinceNanos < stallAfterNanos) {
			return false;
		}
		sinceNanos = nowNanos;
		return true;
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
