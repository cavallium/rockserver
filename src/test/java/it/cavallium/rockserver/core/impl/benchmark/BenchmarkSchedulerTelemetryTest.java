package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BenchmarkSchedulerTelemetryTest {

	@Test
	void readsWaveOneAccountingWithoutAllocatingCompatibilityRecords() {
		var snapshot = new RWScheduler.PoolSnapshot(4, 0, 1, 2, 3, 6,
				12L, 10L, 8L, 7L, 0L,
				Map.of(WorkloadProfile.BATCH, 12L), Map.of(WorkloadProfile.BATCH, 1),
				Map.of(WorkloadProfile.BATCH, 2), Map.of(WorkloadProfile.BATCH, 3),
				Map.of(WorkloadProfile.BATCH, 6),
				Map.of(RWScheduler.TerminalOutcome.RUN, 5L,
						RWScheduler.TerminalOutcome.OVERLOAD, 1L),
				false, Integer.MAX_VALUE, List.of(), false, false);

		assertTrue(BenchmarkSchedulerTelemetry.exactAccounting());
		assertTrue(BenchmarkSchedulerTelemetry.allocationFreePoolTelemetry());
		assertEquals(6, BenchmarkSchedulerTelemetry.outstandingTasks(snapshot));
		assertEquals(3, BenchmarkSchedulerTelemetry.parkedTasks(snapshot, 6));
		assertEquals(12L, BenchmarkSchedulerTelemetry.submissionAttempts(snapshot));
		assertEquals(6L, BenchmarkSchedulerTelemetry.terminalOutcomes(snapshot));
	}

	@Test
	void legacyFallbackDerivesAggregateOutstandingAndParkedCounts() {
		assertEquals(6, BenchmarkSchedulerTelemetry.legacyOutstandingTasks(10L, 4L));
		assertEquals(3, BenchmarkSchedulerTelemetry.legacyParkedTasks(6, 1, 2));
		assertEquals(0, BenchmarkSchedulerTelemetry.legacyOutstandingTasks(4L, 5L));
		assertEquals(0, BenchmarkSchedulerTelemetry.legacyParkedTasks(2, 2, 1));
	}
}
