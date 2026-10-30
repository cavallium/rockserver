package it.cavallium.rockserver.distributed;

import java.time.Duration;

/**
 * Decides whether a node serves its API, from whether it knows a leader.
 *
 * <p>A client leaves a node only when its transport is lost, and a node cut off from the Raft majority still answers
 * HTTP/2 and keepalive while every request fails. So a node that has known a leader and has had none for
 * {@value #ELECTION_TIMEOUTS_TO_PAUSE} maximum election timeouts, and at least {@link #MIN_PAUSE_AFTER}, stops serving: its
 * clients move to their next address. Raft runs on its own port, so the node keeps taking part in elections, and it serves again by itself once it knows a
 * leader, also after a period when no node of the cluster had one.</p>
 *
 * <p>A node without a leader for that long is not in a normal election. A node loses its leader when its election timer
 * fires, and a round of voting lasts at most one election timeout, so the bound leaves room for three rounds, two of them
 * lost to a split vote. The minimum is for a node that stopped for a moment (a long GC pause, SIGSTOP): a leader that got
 * no answer from a follower for that long sleeps before it tries again, up to
 * {@link RaftSettings#APPENDER_RETRY_LONGEST_SLEEP}, and the node has no leader until then. The minimum is twice that
 * sleep, so that one more try fits in it if the first one is lost.</p>
 *
 * <p>The state of the gate is in memory, but whether a leader existed is not: a node that restarts with what a leader
 * left behind has known one, and it is judged from its start, or a restart while cut off from the majority would keep
 * its clients on a node that cannot serve them. Then a whole cluster that takes longer than the bound to elect a leader
 * pauses until it does. None of its nodes could serve before that, and elections and everything else that forms the
 * cluster use the Raft ports. What it costs is an answer to the capability handshake of a client that starts meanwhile: a
 * connection to several addresses keeps asking, for 30 s by default (see {@code GrpcConnection.forHostsAndPorts}), and one
 * to a single address fails after the retries of the call, about 8 s. Only a node that never had any of that, the member
 * of a new cluster, is not judged: a cluster that is still forming has nothing better to offer its clients.</p>
 *
 * <p>A node must keep knowing a leader for {@value #ELECTION_TIMEOUTS_TO_RESUME} maximum election timeout to serve
 * again. Its leader was then confirmed by a heartbeat: without one, its election timer would have fired and cleared it.
 * Both conditions need an unbroken observation, which keeps the node serving through an election shorter than the
 * bound, and leaves at least the sum of the two between one pause and the next.</p>
 */
final class ApiGate {

	static final int ELECTION_TIMEOUTS_TO_PAUSE = 3;
	/** The longest sleep of the leader before it tries a stopped follower again, and the same again as a margin. */
	static final Duration MIN_PAUSE_AFTER = RaftSettings.APPENDER_RETRY_LONGEST_SLEEP.multipliedBy(2);
	static final int ELECTION_TIMEOUTS_TO_RESUME = 1;

	private final long pauseAfterNanos;
	private final long resumeAfterNanos;
	private boolean serving = true;
	private boolean everKnewLeader;
	private boolean knowsLeader;
	private long sinceNanos;

	/**
	 * @param knewLeader whether a leader existed before this start
	 * @param startNanos the time of this start, from which the bound counts until the first observation of a leader
	 */
	ApiGate(Duration pauseAfter, Duration resumeAfter, boolean knewLeader, long startNanos) {
		this.pauseAfterNanos = pauseAfter.toNanos();
		this.resumeAfterNanos = resumeAfter.toNanos();
		this.everKnewLeader = knewLeader;
		this.sinceNanos = startNanos;
	}

	static ApiGate forElectionTimeout(Duration electionTimeoutMax, boolean knewLeader, long startNanos) {
		var pauseAfter = electionTimeoutMax.multipliedBy(ELECTION_TIMEOUTS_TO_PAUSE);
		return new ApiGate(pauseAfter.compareTo(MIN_PAUSE_AFTER) < 0 ? MIN_PAUSE_AFTER : pauseAfter,
				electionTimeoutMax.multipliedBy(ELECTION_TIMEOUTS_TO_RESUME), knewLeader, startNanos);
	}

	/** Takes the current observation and tells whether the node should serve now. */
	boolean shouldServe(boolean leaderKnown, long nowNanos) {
		if (leaderKnown != knowsLeader) {
			knowsLeader = leaderKnown;
			sinceNanos = nowNanos;
		}
		everKnewLeader |= leaderKnown;
		long heldNanos = nowNanos - sinceNanos;
		if (serving && everKnewLeader && !leaderKnown && heldNanos >= pauseAfterNanos) {
			serving = false;
		} else if (!serving && leaderKnown && heldNanos >= resumeAfterNanos) {
			serving = true;
		}
		return serving;
	}
}
