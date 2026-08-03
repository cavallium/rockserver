package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.benchmark.GrpcRawScanBenchmark;
import it.cavallium.rockserver.core.impl.benchmark.PairedPerformanceContract;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GrpcRawScanBenchmarkTest {

	@Test
	void removedPermissiveThresholdOptionsCannotReactivateTheOldGate() {
		assertThrows(IllegalArgumentException.class, () -> GrpcRawScanBenchmark.main(
				new String[] {"--strict-non-inferiority=true"}));
		assertThrows(IllegalArgumentException.class, () -> GrpcRawScanBenchmark.main(
				new String[] {"--minimum-throughput-ratio=0.99"}));
	}

	@Test
	void paretoGateRejectsSmallRegressionsAndReportsOnlyEligibleExceptions() {
		Map<String, PairedPerformanceContract.MetricSamples> samples = completeParetoSamples();
		samples.put("mib-per-second", paired(100.0d, 99.5d));
		samples.put("scan-p99", paired(100.0d, 97.0d));
		samples.put("allocated-bytes-per-entry", paired(100.0d, 100.1d));

		var result = GrpcRawScanBenchmark.evaluateParetoForTesting(samples, List.of(), true);

		assertFalse(result.automaticAcceptancePassed());
		assertTrue(result.exceptionCandidates().contains("mib-per-second"));
		assertFalse(result.exceptionCandidates().contains("allocated-bytes-per-entry"));
		assertTrue(result.materialImprovements().contains("scan-p99"));
	}

	@Test
	void paretoGateTreatsParkedAndOutstandingPeaksAsExactNoIncreaseMetrics() {
		Map<String, PairedPerformanceContract.MetricSamples> samples = completeParetoSamples();
		samples.put("mib-per-second", paired(100.0d, 103.0d));
		samples.put("peak-parked", paired(2.0d, 3.0d));

		var result = GrpcRawScanBenchmark.evaluateParetoForTesting(samples, List.of(), true);

		assertFalse(result.automaticAcceptancePassed());
		assertFalse(result.exceptionCandidates().contains("peak-parked"));
	}

	@Test
	void paretoGateRequiresTheFixedTenPairs() {
		Map<String, PairedPerformanceContract.MetricSamples> samples = completeParetoSamples();
		samples.put("mib-per-second", new PairedPerformanceContract.MetricSamples(
				new double[] {100.0d}, new double[] {103.0d}));

		var result = GrpcRawScanBenchmark.evaluateParetoForTesting(samples, List.of(), true);

		assertFalse(result.automaticAcceptancePassed());
		assertTrue(result.failures().stream().anyMatch(failure -> failure.contains("requires exactly 10 pairs")));
	}

	private static Map<String, PairedPerformanceContract.MetricSamples> completeParetoSamples() {
		Map<String, PairedPerformanceContract.MetricSamples> samples = new LinkedHashMap<>();
		var missing = GrpcRawScanBenchmark.evaluateParetoForTesting(Map.of(), List.of(), false);
		for (String failure : missing.failures()) {
			if (failure.startsWith("missing metric ")) {
				samples.put(failure.substring("missing metric ".length()), paired(100.0d, 100.0d));
			}
		}
		return samples;
	}

	private static PairedPerformanceContract.MetricSamples paired(double baseline, double candidate) {
		return new PairedPerformanceContract.MetricSamples(constant(baseline), constant(candidate));
	}

	private static double[] constant(double value) {
		double[] values = new double[PairedPerformanceContract.REQUIRED_PAIRS];
		java.util.Arrays.fill(values, value);
		return values;
	}
}
