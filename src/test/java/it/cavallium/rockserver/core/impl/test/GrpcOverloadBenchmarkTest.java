package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Status;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.benchmark.GrpcOverloadBenchmark;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GrpcOverloadBenchmarkTest {

	@TempDir
	Path tempDir;

	@Test
	void correctnessAcceptanceDoesNotTreatOldPerformanceCeilingsAsCrossBuildGates() {
		var report = GrpcOverloadBenchmark.evaluateAcceptance(passingInput(100, 200));

		assertTrue(report.passed());
		assertEquals("none", report.failedSummary());
	}

	@Test
	void acceptanceReportsEveryIndependentFailure() {
		var report = GrpcOverloadBenchmark.evaluateAcceptance(new GrpcOverloadBenchmark.GateInput(
				1,
				2,
				3,
				new GrpcOverloadBenchmark.RatioConfidenceInterval(3, 2.01d, 2.001d, 2.02d),
				new GrpcOverloadBenchmark.RatioConfidenceInterval(3, 0.799d, 0.79d, 0.81d),
				0,
				false,
				3,
				4,
				new GrpcOverloadBenchmark.RequestAccounting(2, 1, 1, 1, 2, Map.of(Status.Code.OK, 1L)),
				new GrpcOverloadBenchmark.RequestAccounting(3, 2, 1, 0, 3, Map.of(Status.Code.OK, 2L)),
				new GrpcOverloadBenchmark.IntegrityResult(2, 1, 2, 0, 1, 1,
						List.of("failure"),
						new GrpcOverloadBenchmark.RequestAccounting(4, 3, 1, 0, 4,
								Map.of(Status.Code.OK, 3L))),
				Set.of(WorkloadProfile.LATENCY),
				new GrpcOverloadBenchmark.PoolUtilization(
						4, 10, 0, 0, 0, 0, 0, 1, 5, 0.0d, true, 250_000L),
				new GrpcOverloadBenchmark.PoolUtilization(
						4, 10, 10, 0, 10, 1, 30, 2, 50, 0.25d, true, 250_000L),
				new GrpcOverloadBenchmark.SchedulerConservation(10, 9, 8, 9, 1,
						List.of("read imbalance")),
				new GrpcOverloadBenchmark.SchedulerConservation(10, 10, 9, 9, 0,
						List.of("write imbalance")),
				new GrpcOverloadBenchmark.PriorityEvidence(0, 0, 0, 0, 0, 0, 0, 0, 0),
				false,
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
				"all_operation_deadlines",
				"cancellation_progress",
				"transport_request_conservation",
				"round_trip_integrity",
				"all_profiles_progress",
				"read_pool_work_conserving",
				"write_pool_work_conserving",
				"scheduler_counter_conservation",
				"priority_and_quantum_bound",
				"queues_and_resources_drained",
				"runtime_telemetry_available",
				"unexpected_errors",
				"foreground_rejections",
				"native_handle_leaks",
				"clean_shutdown"), failed);
	}

	@Test
	void acceptanceRejectsMissingLatencySamples() {
		var report = GrpcOverloadBenchmark.evaluateAcceptance(passingInput(0, 0));

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
	void runtimeCountersExcludeOnlyTheMeasuredObserverThread() {
		assertEquals(700L, GrpcOverloadBenchmark.subtractObserverForTesting(1_000L, 300L));
		assertEquals(0L, GrpcOverloadBenchmark.subtractObserverForTesting(100L, 300L));
		assertEquals(-1L, GrpcOverloadBenchmark.subtractObserverForTesting(-1L, 0L));
		assertEquals(-1L, GrpcOverloadBenchmark.subtractObserverForTesting(1L, -1L));
	}

	@Test
	void pairedConfidenceIntervalIsExactForStableRoundsAndWidensForNoise() {
		var stable = GrpcOverloadBenchmark.ratioConfidenceInterval(new double[] {1.0d, 1.0d, 1.0d});
		assertEquals(1.0d, stable.mean());
		assertEquals(1.0d, stable.lower95());
		assertEquals(1.0d, stable.upper95());

		var noisy = GrpcOverloadBenchmark.ratioConfidenceInterval(new double[] {0.8d, 1.0d, 1.2d});
		assertTrue(noisy.lower95() < noisy.mean());
		assertTrue(noisy.upper95() > noisy.mean());
	}

	@Test
	void booleanOptionsRejectTypos() {
		assertThrows(IllegalArgumentException.class,
				() -> GrpcOverloadBenchmark.main(new String[] {"--smoke=truthy"}));
	}

	@Test
	void enforcedReleaseOptionsRequireExactCleanColdHardwareProvenance() {
		String sha = "a".repeat(40);
		assertThrows(IllegalArgumentException.class, () -> GrpcOverloadBenchmark.validateOptionsForTesting(
				"--build-id=" + sha,
				"--build-state=dirty",
				"--storage-label=hdd-btrfs",
				"--host-state=dedicated",
				"--cache-state=unknown",
				"--prepare-only=true",
				"--enforce=true"));
		assertThrows(IllegalArgumentException.class, () -> GrpcOverloadBenchmark.validateOptionsForTesting(
				"--build-id=" + sha,
				"--build-state=clean",
				"--storage-label=hdd-btrfs",
				"--host-state=dedicated",
				"--cache-state=warm",
				"--reuse-preloaded=true",
				"--enforce=true"));
		assertThrows(IllegalArgumentException.class, () -> GrpcOverloadBenchmark.validateOptionsForTesting(
				"--build-id=" + sha,
				"--build-state=clean",
				"--storage-label=ci-structural",
				"--host-state=dedicated",
				"--cache-state=cold",
				"--reuse-preloaded=true",
				"--enforce=true"));
		assertThrows(IllegalArgumentException.class, () -> GrpcOverloadBenchmark.validateOptionsForTesting(
				"--build-id=" + sha,
				"--build-state=clean",
				"--storage-label=hdd-btrfs",
				"--host-state=dedicated",
				"--cache-state=cold",
				"--instrumentation-mode=portable",
				"--reuse-preloaded=true",
				"--enforce=true"));

		GrpcOverloadBenchmark.validateOptionsForTesting(
				"--build-id=" + sha,
				"--build-state=clean",
				"--storage-label=hdd-btrfs",
				"--host-state=dedicated",
				"--cache-state=unknown",
				"--prepare-only=true",
				"--enforce=true");
		GrpcOverloadBenchmark.validateOptionsForTesting(
				"--build-id=" + sha,
				"--build-state=clean",
				"--storage-label=hdd-btrfs",
				"--host-state=dedicated",
				"--cache-state=cold",
				"--reuse-preloaded=true",
				"--enforce=true");
	}

	@Test
	void comparisonFingerprintIsStableAndCoversWorkloadAndCacheIdentity() {
		String[] base = {"--smoke=true", "--enforce=false", "--build-id=diagnostic-dirty",
				"--build-state=dirty", "--storage-label=nvme", "--cache-state=warm"};
		String fingerprint = GrpcOverloadBenchmark.comparisonFingerprintForTesting(base);

		assertEquals(fingerprint, GrpcOverloadBenchmark.comparisonFingerprintForTesting(base));
		assertEquals(fingerprint, GrpcOverloadBenchmark.comparisonFingerprintForTesting(
				"--smoke=true", "--enforce=false", "--build-id=another-build",
				"--build-state=clean", "--storage-label=nvme", "--cache-state=warm"),
				"build provenance must not prevent baseline/candidate workload identity matching");
		assertNotEquals(fingerprint, GrpcOverloadBenchmark.comparisonFingerprintForTesting(
				"--smoke=true", "--enforce=false", "--build-id=diagnostic-dirty",
				"--build-state=dirty", "--storage-label=nvme", "--cache-state=warm",
				"--foreground-write-rate=101"));
		assertNotEquals(fingerprint, GrpcOverloadBenchmark.comparisonFingerprintForTesting(
				"--smoke=true", "--enforce=false", "--build-id=diagnostic-dirty",
				"--build-state=dirty", "--storage-label=nvme", "--cache-state=unknown"));
	}

	@Test
	void preparedRootCanBeClaimedExactlyOnce() throws Exception {
		Path root = tempDir.resolve("one-shot");

		GrpcOverloadBenchmark.claimRunAttemptForTesting(root);

		assertTrue(Files.isRegularFile(root.resolve("run-attempt.properties")));
		assertThrows(IOException.class, () -> GrpcOverloadBenchmark.claimRunAttemptForTesting(root));
	}

	@Test
	void hostMemoryParserPreservesByteAccuratePreflightEvidence() {
		var memory = GrpcOverloadBenchmark.parseHostMemoryForTesting("""
				MemTotal:       65536 kB
				MemAvailable:   8192 kB
				SwapTotal:      4096 kB
				SwapFree:       1024 kB
				""");

		assertTrue(memory.known());
		assertEquals(65_536L * 1_024L, memory.totalBytes());
		assertEquals(8_192L * 1_024L, memory.availableBytes());
		assertEquals(4_096L * 1_024L, memory.swapTotalBytes());
		assertEquals(1_024L * 1_024L, memory.swapFreeBytes());
	}

	@Test
	void storageLabelsAreCheckedAgainstResolvedFilesystemAndRotationalEvidence() {
		var hdd = new GrpcOverloadBenchmark.StorageEnvironment(
				"/media/dati", "/dev/sda1", "btrfs", 1, "rotational-test-device");
		var nvme = new GrpcOverloadBenchmark.StorageEnvironment(
				"/media/fast", "/dev/nvme1n1p1", "xfs", 0, "nvme-test-device");
		var tmpfs = new GrpcOverloadBenchmark.StorageEnvironment(
				"/tmp", "tmpfs", "tmpfs", -1, "unknown");

		assertTrue(GrpcOverloadBenchmark.storageMatchesLabelForTesting(hdd, "hdd-btrfs"));
		assertFalse(GrpcOverloadBenchmark.storageMatchesLabelForTesting(hdd, "nvme"));
		assertTrue(GrpcOverloadBenchmark.storageMatchesLabelForTesting(nvme, "nvme"));
		assertFalse(GrpcOverloadBenchmark.storageMatchesLabelForTesting(tmpfs, "hdd-btrfs"));
		assertTrue(GrpcOverloadBenchmark.storageMatchesLabelForTesting(tmpfs, "ci-structural"));
	}

	@Test
	void competingJvmBenchmarkClassifierRejectsTimingContaminationWithoutLeakingCommands() {
		assertTrue(GrpcOverloadBenchmark.isCompetingBenchmarkCommandForTesting(
				"/usr/bin/java", "java -cp workload.jar org.openjdk.jmh.runner.ForkedMain"));
		assertTrue(GrpcOverloadBenchmark.isCompetingBenchmarkCommandForTesting(
				"java", "java -jar scheduler-benchmark.jar"));
		assertFalse(GrpcOverloadBenchmark.isCompetingBenchmarkCommandForTesting(
				"/usr/bin/java", "java -jar production-server.jar"));
		assertFalse(GrpcOverloadBenchmark.isCompetingBenchmarkCommandForTesting(
				"/usr/bin/python3", "python3 benchmark_driver.py"));
	}

	@Test
	void workConservationUsesTheCooperativeTimeBoundRatherThanAPlatformDependentSampleCount() {
		var atBound = new GrpcOverloadBenchmark.PoolUtilization(
				4, 100, 100, 0, 100, 67, 33, 4, 33, 0.90d, true, 250_000L);
		var pastBound = new GrpcOverloadBenchmark.PoolUtilization(
				4, 100, 100, 0, 100, 66, 34, 4, 34, 0.90d, true, 250_000L);
		var portable = new GrpcOverloadBenchmark.PoolUtilization(
				4, 100, 100, 0, 100, 67, 33, 4, 0, 0.90d, false, 250_000L);
		var fastTurnover = new GrpcOverloadBenchmark.PoolUtilization(
				4, 100, 100, 0, 10, 9, 1, 3, 4, 0.90d, true, 250_000L);
		var policyLimited = new GrpcOverloadBenchmark.PoolUtilization(
				36, 100, 0, 100, 0, 0, 0, 4, 0, 0.0d, true, 250_000L);

		assertEquals(8_250_000L, atBound.maximumAvoidableIdleNanos());
		assertTrue(atBound.saturatedAndWorkConserving());
		assertFalse(pastBound.saturatedAndWorkConserving());
		assertFalse(portable.saturatedAndWorkConserving());
		assertTrue(fastTurnover.saturatedAndWorkConserving(),
				"exact bounded idle time is authoritative for fast tasks between sampling instants");
		assertTrue(policyLimited.saturatedAndWorkConserving(),
				"intentionally limited BATCH backlog is justified idle capacity, not lost work");
	}

	@Test
	void priorityEvidenceAllowsOneCorrelatedOutlierButNotTwo() {
		var oneOutlier = new GrpcOverloadBenchmark.PriorityEvidence(
				1_000_000L,
				8_000_000L,
				40_000L,
				100_000L,
				25_000L,
				45_000L,
				5,
				4,
				4);
		var twoOutliers = new GrpcOverloadBenchmark.PriorityEvidence(
				1_000_000L,
				8_000_000L,
				40_000L,
				100_000L,
				25_000L,
				45_000L,
				5,
				3,
				3);

		assertTrue(oneOutlier.passed());
		assertFalse(twoOutliers.passed());
	}

	private static GrpcOverloadBenchmark.GateInput passingInput(long baselineP99, long mixedP99) {
		var requests = new GrpcOverloadBenchmark.RequestAccounting(
				100, 100, 0, 0, 8, Map.of(Status.Code.OK, 100L));
		var integrityRequests = new GrpcOverloadBenchmark.RequestAccounting(
				20, 20, 0, 0, 1, Map.of(Status.Code.OK, 20L));
		var integrity = new GrpcOverloadBenchmark.IntegrityResult(
				10, 10, 10, 10, 0, 0, List.of(), integrityRequests);
		var utilization = new GrpcOverloadBenchmark.PoolUtilization(
				4, 100, 100, 0, 100, 100, 0, 4, 0, 1.0d, true, 250_000L);
		var scheduler = new GrpcOverloadBenchmark.SchedulerConservation(
				100, 90, 90, 100, 0, List.of());
		return new GrpcOverloadBenchmark.GateInput(
				0,
				0,
				0,
				GrpcOverloadBenchmark.ratioConfidenceInterval(new double[] {
						baselineP99 > 0 ? mixedP99 / (double) baselineP99 : Double.POSITIVE_INFINITY}),
				new GrpcOverloadBenchmark.RatioConfidenceInterval(1, 0.80d, 0.80d, 0.80d),
				1,
				true,
				0,
				0,
				requests,
				requests,
				integrity,
				EnumSet.allOf(WorkloadProfile.class),
				utilization,
				utilization,
				scheduler,
				scheduler,
				new GrpcOverloadBenchmark.PriorityEvidence(
						1_000_000L,
						8_000_000L,
						8_000_000L,
						9_000_000L,
						5_000_000L,
						6_000_000L,
						5,
						4,
						4),
				true,
				0,
				true);
	}
}
