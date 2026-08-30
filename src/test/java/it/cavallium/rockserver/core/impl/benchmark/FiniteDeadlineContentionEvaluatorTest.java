package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FiniteDeadlineContentionEvaluatorTest {

	@TempDir
	Path tempDir;

	@Test
	void fixedControllerRejectsTolerancedThroughputAndAllocationRegressions() throws Exception {
		Files.writeString(tempDir.resolve("metadata.properties"), """
				contract-version=v2
				scheduler-operations=1000000
				scheduler-submitters=64
				scheduler-read-workers=8
				scheduler-write-workers=8
				scheduler-foreground-capacity=65536
				scheduler-batch-capacity=65536
				scheduler-seed=104372305701837
				baseline-sha=0000000000000000000000000000000000000000
				candidate-sha=1111111111111111111111111111111111111111
				""");
		Path workers = Files.createDirectories(tempDir.resolve("raw-scheduler"));
		for (int round = 1; round <= PairedPerformanceContractV2.REQUIRED_PAIRS; round++) {
			Files.writeString(workers.resolve("round-%02d-baseline.properties".formatted(round)),
					worker(100.0d, 100.0d));
			Files.writeString(workers.resolve("round-%02d-candidate.properties".formatted(round)),
					worker(90.0d, 110.0d));
		}

		var evaluation = FiniteDeadlineContentionEvaluator.evaluate(tempDir).evaluation();

		assertEquals(PairedPerformanceContractV2.Decision.FAIL, evaluation.decision());
		assertEquals(true, evaluation.metrics().get("attempts-throughput").regressionDemonstrated());
		assertEquals(true, evaluation.metrics().get("allocation-per-attempt").regressionDemonstrated());
	}

	private static String worker(double throughput, double allocation) {
		return """
				schema=rockserver-scheduler-high-contention-v2
				operations=1000000
				seed=104372305701837
				submitters=64
				latency_finite_deadlines=true
				elapsed_nanos=1000000000
				attempts_per_second=%s
				useful_runs_per_second=%s
				process.cpu_nanos_per_attempt=100
				process.allocated_bytes_per_attempt=%s
				profile.latency.runs=100
				profile.latency.queue_p99_nanos=100
				profile.latency.end_to_end_p99_nanos=100
				profile.latency.maximum_progress_gap_nanos=100
				outcome.run=1000000
				outcome.failure=0
				outcome.deadline=0
				outcome.cancellation=0
				outcome.overload=0
				outcome.shutdown=0
				""".formatted(throughput, throughput, allocation);
	}
}
