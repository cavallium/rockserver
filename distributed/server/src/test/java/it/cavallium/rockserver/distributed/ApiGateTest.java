package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.retry.MultipleLinearRandomRetry;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.junit.jupiter.api.Test;

class ApiGateTest {

	private static final Duration ELECTION_TIMEOUT_MAX = Duration.ofSeconds(2);
	private static final long TICK = TimeUnit.MILLISECONDS.toNanos(200);
	private static final long PAUSE_AFTER = TimeUnit.SECONDS.toNanos(6);
	private static final long RESUME_AFTER = TimeUnit.SECONDS.toNanos(2);

	/** Not zero: the clock of {@link System#nanoTime()} starts anywhere. */
	private long now = -123_456_789L;
	private final ApiGate gate = ApiGate.forElectionTimeout(ELECTION_TIMEOUT_MAX, false, now);

	/** Observes {@code leaderKnown} every tick for this long, and returns the last answer. */
	private boolean run(boolean leaderKnown, long nanos) {
		return run(gate, leaderKnown, nanos);
	}

	private boolean run(ApiGate gate, boolean leaderKnown, long nanos) {
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

	/** A node restarted while cut off from the majority: its clients must leave it, with no leader seen since. */
	@Test
	void aNodeThatKnewALeaderBeforeItsStartPausesFromItsStart() {
		var restarted = ApiGate.forElectionTimeout(ELECTION_TIMEOUT_MAX, true, now);
		assertTrue(run(restarted, false, PAUSE_AFTER - TICK));
		assertFalse(run(restarted, false, TICK));
	}

	/** The first observation may come long after the start, as it does on a loaded machine. */
	@Test
	void theBoundOfARestartedNodeCountsFromItsStart() {
		var restarted = ApiGate.forElectionTimeout(ELECTION_TIMEOUT_MAX, true, now);
		now += PAUSE_AFTER;
		assertFalse(restarted.shouldServe(false, now));
	}

	@Test
	void aRestartedNodeThatFindsItsLeaderInTimeKeepsServing() {
		var restarted = ApiGate.forElectionTimeout(ELECTION_TIMEOUT_MAX, true, now);
		assertTrue(run(restarted, false, PAUSE_AFTER - TICK));
		assertTrue(run(restarted, true, 10 * PAUSE_AFTER));
	}

	@Test
	void aRestartedNodeResumesLikeAnyOther() {
		var restarted = ApiGate.forElectionTimeout(ELECTION_TIMEOUT_MAX, true, now);
		assertFalse(run(restarted, false, PAUSE_AFTER));
		assertFalse(run(restarted, true, RESUME_AFTER - TICK));
		assertTrue(run(restarted, true, TICK));
	}

	@Test
	void aNodeLeaderlessForTheBoundPausesAndNotBefore() {
		assertTrue(run(true, TICK));
		assertTrue(run(false, PAUSE_AFTER - TICK));
		assertFalse(run(false, TICK));
	}

	/** A follower that stopped for a moment waits for the leader's next try, whatever its election timeout. */
	@Test
	void aShortElectionTimeoutDoesNotShortenTheBound() {
		var shortTimeout = ApiGate.forElectionTimeout(Duration.ofMillis(800), false, 0);
		assertTrue(shortTimeout.shouldServe(true, 0));
		assertTrue(shortTimeout.shouldServe(false, TICK));
		assertTrue(shortTimeout.shouldServe(false, ApiGate.MIN_PAUSE_AFTER.toNanos()));
		assertFalse(shortTimeout.shouldServe(false, TICK + ApiGate.MIN_PAUSE_AFTER.toNanos()));
	}

	/** What the bound assumes about the retry policy, read from the policy itself and Ratis' multiplier. */
	@Test
	void theMinimumBoundOutlastsEveryRetrySleepOfTheLeader() {
		var policy = MultipleLinearRandomRetry.parseCommaSeparated(RaftSettings.APPENDER_RETRY_POLICY);
		assertEquals(Duration.ofSeconds(3), RaftSettings.APPENDER_RETRY_LONGEST_SLEEP);
		assertEquals(RaftSettings.APPENDER_RETRY_LONGEST_SLEEP.multipliedBy(2), ApiGate.MIN_PAUSE_AFTER);
		for (int failures : new int[] {1, 10, 11, 30, 31, 1_000, 1_000_000, 1_000_000_030}) {
			var sleep = policy.handleAttemptFailure(() -> failures);
			assertTrue(sleep.shouldRetry(), () -> "the policy ends at " + failures + " failures");
			for (int draw = 0; draw < 1000; draw++) {
				long millis = sleep.getSleepTime().toLong(TimeUnit.MILLISECONDS);
				assertTrue(millis <= RaftSettings.APPENDER_RETRY_LONGEST_SLEEP.toMillis(), () -> millis + " ms");
			}
		}
		assertFalse(policy.handleAttemptFailure(() -> 1_000_000_031).shouldRetry());
	}

	/** The bound assumes the policy above: it must be the one Ratis gets. */
	@Test
	void theRetryPolicyReachesRatis() {
		var config = NodeConfig.defaults("a", List.of(new PeerConfig("a", "localhost", 1, "localhost", 2)), Path.of("data"));
		assertEquals(RaftSettings.APPENDER_RETRY_POLICY,
				RaftServerConfigKeys.Log.Appender.retryPolicy(RaftSettings.server(config)));
	}

	@Test
	void aLongElectionTimeoutLengthensTheBound() {
		var longTimeout = ApiGate.forElectionTimeout(Duration.ofSeconds(4), false, 0);
		assertTrue(longTimeout.shouldServe(true, 0));
		assertTrue(longTimeout.shouldServe(false, TICK));
		assertTrue(longTimeout.shouldServe(false, TimeUnit.SECONDS.toNanos(12)));
		assertFalse(longTimeout.shouldServe(false, TICK + TimeUnit.SECONDS.toNanos(12)));
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
