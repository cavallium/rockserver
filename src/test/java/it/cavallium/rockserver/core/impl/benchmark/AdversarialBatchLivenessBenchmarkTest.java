package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class AdversarialBatchLivenessBenchmarkTest {

	@Test
	void candidateRemainsWorkConservingAndHandsOffWhenWriteBecomesDispatchable() throws Exception {
		var config = new AdversarialBatchLivenessBenchmark.Config(3,
				3,
				96,
				Duration.ofMillis(10),
				Duration.ofMillis(240),
				Duration.ofSeconds(5),
				Duration.ofMillis(250));

		var result = AdversarialBatchLivenessBenchmark.run(config);

		result.assertCandidateWorkConserving(Duration.ofMillis(100), 20.0d);
		assertTrue(result.writeFairTurnDelayNanos() <= config.fairTurnBound().toNanos());
	}

	@Test
	void rejectsAWindowTooShortToDistinguishPacingFromStarvation() {
		assertThrows(IllegalArgumentException.class, () -> new AdversarialBatchLivenessBenchmark.Config(
				3,
				3,
				32,
				Duration.ofMillis(20),
				Duration.ofMillis(100),
				Duration.ofSeconds(5),
				Duration.ofMillis(250)));
	}
}
