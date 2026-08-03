package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.benchmark.GrpcRawScanBenchmark;
import org.junit.jupiter.api.Test;

class GrpcRawScanBenchmarkTest {

	@Test
	void pairedConfidenceGateAcceptsAConsistentCandidateImprovement() {
		var result = GrpcRawScanBenchmark.evaluateForTesting(
				new double[] {100, 102, 98, 101, 99},
				new double[] {110, 112, 108, 111, 109},
				new double[] {100, 102, 98, 101, 99},
				new double[] {88, 90, 86, 89, 87},
				new double[] {1_000, 1_020, 980, 1_010, 990},
				new double[] {900, 918, 882, 909, 891},
				1.0d, 1.0d, 1.0d);

		assertTrue(result.passed());
		assertTrue(result.throughput().lower95() > 1.0d);
		assertTrue(result.queueP99().upper95() < 1.0d);
		assertTrue(result.scanP99().upper95() < 1.0d);
	}

	@Test
	void pairedConfidenceGateRejectsRawThroughputRegression() {
		var result = GrpcRawScanBenchmark.evaluateForTesting(
				new double[] {100, 100, 100, 100, 100},
				new double[] {90, 91, 89, 90, 90},
				new double[] {100, 100, 100, 100, 100},
				new double[] {90, 90, 90, 90, 90},
				new double[] {1_000, 1_000, 1_000, 1_000, 1_000},
				new double[] {900, 900, 900, 900, 900},
				1.0d, 1.0d, 1.0d);

		assertFalse(result.passed());
		assertTrue(result.failures().stream().anyMatch(failure -> failure.contains("throughput")));
	}

	@Test
	void pairedConfidenceGateRejectsSchedulerOrWholePathP99Regression() {
		var result = GrpcRawScanBenchmark.evaluateForTesting(
				new double[] {100, 100, 100, 100, 100},
				new double[] {110, 110, 110, 110, 110},
				new double[] {100, 100, 100, 100, 100},
				new double[] {120, 121, 119, 120, 120},
				new double[] {1_000, 1_000, 1_000, 1_000, 1_000},
				new double[] {1_200, 1_210, 1_190, 1_200, 1_200},
				1.0d, 1.0d, 1.0d);

		assertFalse(result.passed());
		assertTrue(result.failures().stream().anyMatch(failure -> failure.contains("queue-p99")));
		assertTrue(result.failures().stream().anyMatch(failure -> failure.contains("scan-p99")));
	}

	@Test
	void confidenceIntervalCrossingEqualityIsNotReportedAsARegression() {
		var result = GrpcRawScanBenchmark.evaluateForTesting(
				new double[] {100, 100, 100, 100, 100},
				new double[] {90, 110, 92, 108, 100},
				new double[] {100, 100, 100, 100, 100},
				new double[] {80, 120, 85, 115, 100},
				new double[] {1_000, 1_000, 1_000, 1_000, 1_000},
				new double[] {800, 1_200, 850, 1_150, 1_000},
				1.0d, 1.0d, 1.0d);

		assertTrue(result.passed(), "an interval containing equality is inconclusive, not a demonstrated loss");
		assertTrue(result.throughput().lower95() < 1.0d && result.throughput().upper95() > 1.0d);
		assertTrue(result.queueP99().lower95() < 1.0d && result.queueP99().upper95() > 1.0d);
	}

	@Test
	void comparisonRequiresCompletePairedVectors() {
		assertThrows(IllegalArgumentException.class, () -> GrpcRawScanBenchmark.evaluateForTesting(
				new double[] {100, 100}, new double[] {100},
				new double[] {100, 100}, new double[] {100, 100},
				new double[] {100, 100}, new double[] {100, 100},
				1.0d, 1.0d, 1.0d));
	}
}
