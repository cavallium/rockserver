package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(90)
class SchedulerHighContentionBenchmarkTest {

	@Test
	void extremeMixedContentionExercisesEveryProfilePoolAndTerminalPath() throws Exception {
		var result = SchedulerHighContentionBenchmark.run(config(
				100_000,
				48,
				8,
				8,
				2_048,
				4_096,
				2_048,
				0x1020_3040_5060_7080L));

		assertAll(
				() -> assertEquals(100_000, result.attempts()),
				() -> assertTrue(result.attemptsPerSecond() > 0.0d),
				() -> assertTrue(result.usefulRunsPerSecond() > 0.0d),
				() -> assertTrue(result.outcomes().get(RWScheduler.TerminalOutcome.OVERLOAD) > 0L,
						"the run must actually saturate bounded admission"),
				() -> assertTrue(result.outcomes().get(RWScheduler.TerminalOutcome.DEADLINE) > 0L),
				() -> assertTrue(result.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION) > 0L),
				() -> assertTrue(result.injectedFailures() > 0L),
				() -> assertEquals(result.injectedFailures(),
						result.outcomes().get(RWScheduler.TerminalOutcome.FAILURE)),
				() -> assertTrue(result.yieldTransitions() > 0L),
				() -> assertTrue(result.parkTransitions() > 0L),
				() -> assertTrue(result.pools().get(RWScheduler.Pool.READ).batchLimitedObserved()),
				() -> assertTrue(result.pools().get(RWScheduler.Pool.WRITE).batchLimitedObserved()));

		for (var profile : WorkloadProfile.values()) {
			var profileResult = result.profiles().get(profile);
			assertTrue(profileResult.attempts() > 1_000L, "insufficient contention for " + profile);
			assertTrue(profileResult.runs() > 0L, "no useful progress for " + profile);
			assertTrue(profileResult.queueP99Nanos() > 0L, "missing queue distribution for " + profile);
		}
		for (var pool : RWScheduler.Pool.values()) {
			var poolResult = result.pools().get(pool);
			assertTrue(poolResult.peakActive() > 0, "pool never dispatched: " + pool);
			assertTrue(poolResult.peakQueued() <= poolResult.queueBound(), "queue bound exceeded: " + pool);
			assertTrue(poolResult.finalSnapshot().drainedAndConserved(), "pool did not drain: " + pool);
		}
	}

	@Test
	void repeatedIndependentSeedsPreserveConservationAndProgress() throws Exception {
		for (long seed : new long[] {1L, 0x5EEDL, -0x6A09E667F3BCC909L}) {
			var result = SchedulerHighContentionBenchmark.run(config(
					24_000,
					24,
					4,
					4,
					256,
					512,
					512,
					seed));
			assertEquals(24_000L, result.outcomes().values().stream().mapToLong(Long::longValue).sum());
			assertEquals(0L, result.duplicateExecutions());
			assertFalse(result.pools().values().stream()
					.anyMatch(pool -> !pool.finalSnapshot().drainedAndConserved()));
		}
	}

	@Test
	void reportIsStableMachineReadableAndIncludesEveryDimension() throws Exception {
		var result = SchedulerHighContentionBenchmark.run(config(
				12_000,
				16,
				4,
				4,
				128,
				256,
				256,
				42L));
		String report = result.toReport();

		assertTrue(report.startsWith("schema=rockserver-scheduler-high-contention-v1\n"));
		assertTrue(report.contains("attempts_per_second="));
		assertTrue(report.contains("useful_runs_per_second="));
		for (var profile : WorkloadProfile.values()) {
			assertTrue(report.contains("profile." + profile.name().toLowerCase(java.util.Locale.ROOT)
					+ ".queue_p99_nanos="));
			assertTrue(report.contains("profile." + profile.name().toLowerCase(java.util.Locale.ROOT)
					+ ".end_to_end_p99_nanos="));
		}
		for (var pool : RWScheduler.Pool.values()) {
			assertTrue(report.contains("pool." + pool.name().toLowerCase(java.util.Locale.ROOT)
					+ ".peak_outstanding="));
		}
	}

	@Test
	void invalidConfigurationsFailBeforeStartingThreads() {
		var invalid = config(100, 1, 1, 1, 1, 1, 1, 1L);
		assertThrows(IllegalArgumentException.class, invalid::validate);
	}

	private static SchedulerHighContentionBenchmark.Config config(int operations,
			int submitters,
			int readWorkers,
			int writeWorkers,
			int foregroundQueueCapacity,
			int batchQueueCapacity,
			int workTokens,
			long seed) {
		return new SchedulerHighContentionBenchmark.Config(
				operations,
				submitters,
				readWorkers,
				writeWorkers,
				Math.max(1, readWorkers / 4),
				foregroundQueueCapacity,
				batchQueueCapacity,
				workTokens,
				4,
				2,
				7,
				15,
				7,
				40,
				true,
				seed,
				Duration.ofSeconds(45));
	}
}
