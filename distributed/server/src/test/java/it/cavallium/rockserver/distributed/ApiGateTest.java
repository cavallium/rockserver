package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ApiGateTest {

	private static final Duration ELECTION_TIMEOUT_MAX = Duration.ofSeconds(2);
	private static final long TICK = TimeUnit.MILLISECONDS.toNanos(200);
	private static final long PAUSE_AFTER = TimeUnit.SECONDS.toNanos(6);
	private static final long RESUME_AFTER = TimeUnit.SECONDS.toNanos(2);

	private final ApiGate gate = ApiGate.forElectionTimeout(ELECTION_TIMEOUT_MAX);
	/** Not zero: the clock of {@link System#nanoTime()} starts anywhere. */
	private long now = -123_456_789L;

	/** Observes {@code leaderKnown} every tick for this long, and returns the last answer. */
	private boolean run(boolean leaderKnown, long nanos) {
		boolean serving = gate.shouldServe(leaderKnown, now);
		for (long elapsed = 0; elapsed < nanos; elapsed += TICK) {
			now += TICK;
			serving = gate.shouldServe(leaderKnown, now);
		}
		return serving;
	}

	@Test
	void aNodeThatNeverKnewALeaderKeepsServing() {
		assertTrue(run(false, 10 * PAUSE_AFTER));
	}

	@Test
	void aNodeLeaderlessForTheBoundPausesAndNotBefore() {
		assertTrue(run(true, TICK));
		assertTrue(run(false, PAUSE_AFTER - TICK));
		assertFalse(run(false, TICK));
	}

	@Test
	void aShortElectionNeverPauses() {
		assertTrue(run(true, TICK));
		for (int election = 0; election < 100; election++) {
			assertTrue(run(false, PAUSE_AFTER - TICK));
			assertTrue(run(true, TICK));
		}
	}

	@Test
	void aPausedNodeResumesOnlyAfterKnowingALeaderForTheBound() {
		run(true, TICK);
		assertFalse(run(false, PAUSE_AFTER));
		assertFalse(run(true, RESUME_AFTER - TICK));
		assertTrue(run(true, TICK));
	}

	/** A leader seen in passing, which leaves again, is no reason to serve. */
	@Test
	void aPausedNodeStaysPausedThroughAShortLeader() {
		run(true, TICK);
		assertFalse(run(false, PAUSE_AFTER));
		for (int blip = 0; blip < 100; blip++) {
			assertFalse(run(true, RESUME_AFTER - TICK));
			assertFalse(run(false, TICK));
		}
		assertTrue(run(true, RESUME_AFTER));
	}

	@Test
	void aNodeThatResumedNeedsTheWholeBoundToPauseAgain() {
		run(true, TICK);
		run(false, PAUSE_AFTER);
		assertTrue(run(true, RESUME_AFTER));
		assertTrue(run(false, PAUSE_AFTER - TICK));
		assertFalse(run(false, TICK));
	}
}
