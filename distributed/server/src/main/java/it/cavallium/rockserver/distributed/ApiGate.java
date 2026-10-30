package it.cavallium.rockserver.distributed;

import java.time.Duration;

/**
 * Decides whether a node serves its API, from whether it knows a leader.
 *
 * <p>A client leaves a node only when its transport is lost, and a node cut off from the Raft majority still answers
 * HTTP/2 and keepalive while every request fails. So a node that once knew a leader and has had none for
 * {@value #ELECTION_TIMEOUTS_TO_PAUSE} maximum election timeouts stops serving: its clients move to their next address.
 * Raft runs on its own port, so the node keeps taking part in elections, and it serves again by itself once it knows a
 * leader, also after a period when no node of the cluster had one.</p>
 *
 * <p>A node without a leader for that long is not in a normal election. A node loses its leader when its election timer
 * fires, and a round of voting lasts at most one election timeout, so the bound leaves room for three rounds, two of them
 * lost to a split vote. A node that only just started has never known a leader, and is not judged by this: a cluster
 * that is still forming has nothing better to offer its clients.</p>
 *
 * <p>A node must keep knowing a leader for {@value #ELECTION_TIMEOUTS_TO_RESUME} maximum election timeout to serve
 * again. Its leader was then confirmed by a heartbeat: without one, its election timer would have fired and cleared it.
 * Both conditions need an unbroken observation, which keeps the node serving through an election shorter than the
 * bound, and leaves at least the sum of the two between one pause and the next.</p>
 */
final class ApiGate {

	static final int ELECTION_TIMEOUTS_TO_PAUSE = 3;
	static final int ELECTION_TIMEOUTS_TO_RESUME = 1;

	private final long pauseAfterNanos;
	private final long resumeAfterNanos;
	private boolean serving = true;
	private boolean everKnewLeader;
	private boolean knowsLeader;
	private long sinceNanos;

	ApiGate(Duration pauseAfter, Duration resumeAfter) {
		this.pauseAfterNanos = pauseAfter.toNanos();
		this.resumeAfterNanos = resumeAfter.toNanos();
	}

	static ApiGate forElectionTimeout(Duration electionTimeoutMax) {
		return new ApiGate(electionTimeoutMax.multipliedBy(ELECTION_TIMEOUTS_TO_PAUSE),
				electionTimeoutMax.multipliedBy(ELECTION_TIMEOUTS_TO_RESUME));
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
