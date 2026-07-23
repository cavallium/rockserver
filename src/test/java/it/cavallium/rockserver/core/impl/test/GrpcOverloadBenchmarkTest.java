package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.benchmark.GrpcOverloadBenchmark;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class GrpcOverloadBenchmarkTest {

	@Test
	void acceptancePassesAtEveryBoundary() {
		var report = GrpcOverloadBenchmark.evaluateAcceptance(new GrpcOverloadBenchmark.GateInput(
				0,
				0,
				100,
				200,
				1,
				1,
				36,
				1,
				36,
				1,
				true,
				0,
				0,
				0,
				true));

		assertTrue(report.passed());
		assertEquals("none", report.failedSummary());
	}

	@Test
	void acceptanceReportsEveryIndependentFailure() {
		var report = GrpcOverloadBenchmark.evaluateAcceptance(new GrpcOverloadBenchmark.GateInput(
				1,
				2,
				100,
				201,
				0,
				0,
				37,
				2,
				36,
				1,
				false,
				3,
				4,
				5,
				false));

		assertFalse(report.passed());
		Set<String> failed = report.checks().stream()
				.filter(check -> !check.passed())
				.map(GrpcOverloadBenchmark.GateCheck::name)
				.collect(Collectors.toSet());
		assertEquals(Set.of(
				"foreground_deadlines",
				"first_last_deadlines",
				"foreground_p99_ratio",
				"maintenance_progress",
				"cancellation_progress",
				"combined_write_limit",
				"maintenance_write_limit",
				"queues_and_resources_drained",
				"unexpected_errors",
				"foreground_rejections",
				"native_handle_leaks",
				"clean_shutdown"), failed);
	}

	@Test
	void acceptanceRejectsMissingLatencySamples() {
		var report = GrpcOverloadBenchmark.evaluateAcceptance(new GrpcOverloadBenchmark.GateInput(
				0, 0, 0, 0, 1, 1, 1, 1, 1, 1, true, 0, 0, 0, true));

		assertFalse(report.passed());
		assertTrue(report.failedSummary().contains("foreground_p99_ratio"));
	}

	@Test
	void percentileUsesNearestRankAndHandlesEmptySamples() {
		assertEquals(0, GrpcOverloadBenchmark.percentile(new long[0], 0.99));
		assertEquals(10, GrpcOverloadBenchmark.percentile(new long[] {10, 20, 30, 40}, 0.01));
		assertEquals(20, GrpcOverloadBenchmark.percentile(new long[] {10, 20, 30, 40}, 0.50));
		assertEquals(40, GrpcOverloadBenchmark.percentile(new long[] {10, 20, 30, 40}, 0.99));
	}

	@Test
	void booleanOptionsRejectTypos() {
		assertThrows(IllegalArgumentException.class,
				() -> GrpcOverloadBenchmark.main(new String[] {"--smoke=truthy"}));
	}
}
