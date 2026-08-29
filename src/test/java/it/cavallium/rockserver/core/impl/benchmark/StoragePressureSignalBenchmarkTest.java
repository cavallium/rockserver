package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class StoragePressureSignalBenchmarkTest {

	private static final int[] COLUMN_FAMILY_COUNTS = {1, 64, 256, 1_024, 4_096};

	@Test
	void fullSignalAndColumnCountMatrixIsCorrectTimedAndAllocationFree() {
		var result = StoragePressureSignalBenchmark.run(smokeConfig());

		assertDoesNotThrow(result::assertCorrectAndAllocationFree);
		assertEquals(COLUMN_FAMILY_COUNTS.length * StoragePressureSignalBenchmark.Scenario.values().length,
				result.cases().size());
		var keys = new HashSet<String>();
		for (var measurement : result.cases()) {
			assertTrue(keys.add(measurement.scenario() + "/" + measurement.columnFamilies()),
					"duplicate matrix entry");
			assertEquals(Math.multiplyExact(measurement.evaluations(), measurement.columnFamilies()),
					measurement.columnObservations());
			assertEquals(0L, measurement.allocatedBytes());
			assertTrue(measurement.evaluationsPerSecond() > 0.0d);
			assertTrue(measurement.columnObservationsPerSecond() > 0.0d);
			assertTrue(measurement.cpuNanosPerEvaluation() > 0.0d);
		}
	}

	@Test
	void transitionCaseAlternatesEveryEvaluationAndDisabledLimitsNeverPressure() {
		var result = StoragePressureSignalBenchmark.run(new StoragePressureSignalBenchmark.Config(
				new int[] {64}, 32_000L, 64_000L, 1_001, 1_001, 16, "test-build"));

		var transitions = caseFor(result, StoragePressureSignalBenchmark.Scenario.FREQUENT_TRANSITIONS);
		assertEquals(500L, transitions.pressuredEvaluations());
		assertEquals(transitions.expectedPressuredEvaluations(), transitions.pressuredEvaluations());
		var disabled = caseFor(result, StoragePressureSignalBenchmark.Scenario.DISABLED_LIMITS);
		assertEquals(0L, disabled.pressuredEvaluations());
	}

	@Test
	void reportIsStableAndContainsEveryRequiredPerformanceDimension() {
		var result = StoragePressureSignalBenchmark.run(new StoragePressureSignalBenchmark.Config(
				new int[] {1}, 1_000L, 2_000L, 1_000, 2_000, 64, "candidate-sha"));
		String report = result.toReport();

		assertTrue(report.startsWith("schema=rockserver-storage-pressure-signal-v1\n"));
		assertTrue(report.contains("build_id=candidate-sha\n"));
		assertTrue(report.contains("case.no_pressure.cf_1.evaluations_per_second="));
		assertTrue(report.contains("case.last_column_trigger.cf_1.cpu_nanos_per_column_observation="));
		assertTrue(report.contains("case.actual_delayed_write.cf_1.allocated_bytes_per_evaluation=0.000\n"));
		assertTrue(report.contains("case.frequent_transitions.cf_1.latency_p99_nanos="));
	}

	@Test
	void malformedContractsFailBeforeMeasurement() {
		assertThrows(IllegalArgumentException.class, () -> new StoragePressureSignalBenchmark.Config(
				new int[0], 1L, 1L, 1, 1, 1, "build").validate());
		assertThrows(IllegalArgumentException.class, () -> new StoragePressureSignalBenchmark.Config(
				new int[] {0}, 1L, 1L, 1, 1, 1, "build").validate());
		assertThrows(IllegalArgumentException.class, () -> new StoragePressureSignalBenchmark.Config(
				new int[] {1}, 1L, 1L, 2, 1, 1, "build").validate());
	}

	private static StoragePressureSignalBenchmark.Config smokeConfig() {
		return new StoragePressureSignalBenchmark.Config(COLUMN_FAMILY_COUNTS,
				1_000_000L,
				128_000L,
				100,
				1_000_000,
				64,
				"test-build");
	}

	private static StoragePressureSignalBenchmark.CaseResult caseFor(
			StoragePressureSignalBenchmark.Result result,
			StoragePressureSignalBenchmark.Scenario scenario) {
		return result.cases().stream()
				.filter(measurement -> measurement.scenario() == scenario)
				.findFirst()
				.orElseThrow();
	}
}
