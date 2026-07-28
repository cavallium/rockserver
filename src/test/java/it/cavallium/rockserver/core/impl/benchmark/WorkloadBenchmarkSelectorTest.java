package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.WorkloadProfile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkloadBenchmarkSelectorTest {

	@Test
	void generatesInclusivePowersOfTwo() {
		assertEquals(List.of(4, 8, 16, 32), WorkloadBenchmarkSelector.powersOfTwo(3, 33));
		assertEquals(List.of(4, 8), WorkloadBenchmarkSelector.powersOfTwo(1, 8));
		assertThrows(IllegalArgumentException.class,
				() -> WorkloadBenchmarkSelector.powersOfTwo(17, 31));
		assertThrows(IllegalArgumentException.class,
				() -> WorkloadBenchmarkSelector.powersOfTwo(1, 2));
	}

	@Test
	void choosesSmallestCandidateInsideBothPerformanceWindows() {
		var selection = WorkloadBenchmarkSelector.select(List.of(
				candidate(4, 90.0d, 9_900_000L, true, 0L),
				candidate(8, 96.0d, 10_500_000L, true, 0L),
				candidate(16, 100.0d, 10_000_000L, true, 0L),
				candidate(32, 101.0d, 13_000_000L, true, 0L)));

		assertEquals(8, selection.winner());
		assertEquals(List.of(4, 16), selection.adjacentCandidates());
		assertEquals(101.0d * 7.0d, selection.maximumThroughput());
		assertEquals(9_900_000L, selection.minimumRelevantP99Nanos());
		assertTrue(selection.evaluations().get(1).eligible());
	}

	@Test
	void rejectsCandidateWhenAnyOtherProfileViolatesItsSlo() {
		var selection = WorkloadBenchmarkSelector.select(List.of(
				candidate(4, 97.0d, 10_000_000L, false, 0L),
				candidate(8, 100.0d, 10_000_000L, true, 0L)));

		assertEquals(8, selection.winner());
		assertTrue(!selection.evaluations().getFirst().slosPassed());
	}

	@Test
	void rejectsLeaksAndIncomparableRuns() {
		var selection = WorkloadBenchmarkSelector.select(List.of(
				candidate(4, 99.0d, 10_000_000L, true, 1L),
				candidate(8, 100.0d, 10_000_000L, true, 0L)));
		assertEquals(8, selection.winner());

		var differentHost = new WorkloadBenchmarkSelector.CandidateMeasurement(
				16, "dataset", "comparison", "build", "nvme", 42L, true, true,
				candidateProfiles(100.0d, 10_000_000L, true), 0L, 0L, 0L, 0L);
		assertThrows(IllegalArgumentException.class,
				() -> WorkloadBenchmarkSelector.select(List.of(candidate(8, 100, 10_000_000L, true, 0L), differentHost)));

		var differentShape = new WorkloadBenchmarkSelector.CandidateMeasurement(
				16, "dataset", "different-comparison", "build", "hdd-zfs", 42L, true, true,
				candidateProfiles(100.0d, 10_000_000L, true), 0L, 0L, 0L, 0L);
		assertThrows(IllegalArgumentException.class,
				() -> WorkloadBenchmarkSelector.select(List.of(
						candidate(8, 100, 10_000_000L, true, 0L), differentShape)));
		var differentBuild = new WorkloadBenchmarkSelector.CandidateMeasurement(
				16, "dataset", "comparison", "other-build", "hdd-zfs", 42L, true, true,
				candidateProfiles(100.0d, 10_000_000L, true), 0L, 0L, 0L, 0L);
		assertThrows(IllegalArgumentException.class,
				() -> WorkloadBenchmarkSelector.select(List.of(
						candidate(8, 100, 10_000_000L, true, 0L), differentBuild)));
		assertThrows(IllegalArgumentException.class,
				() -> WorkloadBenchmarkSelector.select(List.of(
						candidate(4, 100, 10_000_000L, true, 0L),
						candidate(16, 100, 10_000_000L, true, 0L))));
		assertThrows(IllegalArgumentException.class,
				() -> WorkloadBenchmarkSelector.select(List.of(candidate(
						8, 100, 10_000_000L, true, 0L, false, true))));
		var checksFailed = WorkloadBenchmarkSelector.select(List.of(
				candidate(4, 99, 10_000_000L, true, 0L, true, false),
				candidate(8, 100, 10_000_000L, true, 0L)));
		assertEquals(8, checksFailed.winner());
	}

	@Test
	void selectionInputRoundTripsAndJsonNamesAdjacentVerification(@TempDir Path temporary) throws Exception {
		var measurement = candidate(8, 100.123456789d, 10_000_000L, true, 0L);
		Path input = temporary.resolve("selection-input.properties");
		WorkloadBenchmarkSelection.writeSelectionInput(input, measurement);
		assertEquals(measurement, WorkloadBenchmarkSelection.readSelectionInput(input));

		var json = WorkloadBenchmarkSelection.toJson(WorkloadBenchmarkSelector.select(List.of(
				candidate(4, 96.0d, 10_000_000L, true, 0L),
				measurement,
				candidate(16, 101.0d, 10_100_000L, true, 0L))));
		assertTrue(json.contains("\"winner\": 4"));
		assertTrue(json.contains("\"adjacent_verification_candidates\": [8]"));
		assertTrue(json.contains("\"comparison_fingerprint\": \"comparison\""));
		assertTrue(json.contains("\"build_id\": \"build\""));

		String malformed = Files.readString(input)
				.replace("enforced-hardware-run=true", "enforced-hardware-run=tru");
		Files.writeString(input, malformed);
		assertThrows(IllegalArgumentException.class, () -> WorkloadBenchmarkSelection.readSelectionInput(input));
	}

	@Test
	void rejectsUnknownHarnessOptionsBeforeTouchingRoot(@TempDir Path temporary) {
		Path root = temporary.resolve("must-not-be-created");
		assertThrows(IllegalArgumentException.class, () -> SevenProfileWorkloadBenchmark.main(new String[] {
				"--root=" + root,
				"--mesure-seconds=1"
		}));
		assertTrue(Files.notExists(root));
	}

	@Test
	void rejectsInvalidBaselineBeforeOpeningPreparedRoot(@TempDir Path temporary) throws Exception {
		Path root = temporary.resolve("must-remain-unopened");
		Path baseline = temporary.resolve("ingest-baseline.properties");
		Files.writeString(baseline, "schema=rockserver-ingest-isolated-baseline-v3\n");
		assertThrows(IllegalArgumentException.class, () -> SevenProfileWorkloadBenchmark.main(new String[] {
				"--root=" + root,
				"--candidate=4",
				"--build-id=" + "d".repeat(40),
				"--storage-label=hdd-zfs",
				"--cache-state=cold",
				"--reuse-prepared=true",
				"--measure-seconds=3",
				"--pressure-seconds=1",
				"--enforce=true",
				"--ingest-isolated-baseline-file=" + baseline
		}));
		assertTrue(Files.notExists(root));
	}

	private static WorkloadBenchmarkSelector.CandidateMeasurement candidate(int candidate,
			double perProfileThroughput,
			long p99Nanos,
			boolean sloPassed,
			long leaks) {
		return candidate(candidate, perProfileThroughput, p99Nanos, sloPassed, leaks, true, true);
	}

	private static WorkloadBenchmarkSelector.CandidateMeasurement candidate(int candidate,
			double perProfileThroughput,
			long p99Nanos,
			boolean sloPassed,
			long leaks,
			boolean enforcedHardwareRun,
			boolean runChecksPassed) {
		return new WorkloadBenchmarkSelector.CandidateMeasurement(
				candidate,
				"dataset",
				"comparison",
				"build",
				"hdd-zfs",
				42L,
				enforcedHardwareRun,
				runChecksPassed,
				candidateProfiles(perProfileThroughput, p99Nanos, sloPassed),
				0L,
				0L,
				0L,
				leaks);
	}

	private static EnumMap<WorkloadProfile, WorkloadBenchmarkSelector.ProfileMeasurement> candidateProfiles(
			double throughput,
			long p99Nanos,
			boolean sloPassed) {
		var profiles = new EnumMap<WorkloadProfile, WorkloadBenchmarkSelector.ProfileMeasurement>(WorkloadProfile.class);
		for (var profile : WorkloadBenchmarkSelector.ALL_PROFILES) {
			profiles.put(profile, new WorkloadBenchmarkSelector.ProfileMeasurement(
					throughput,
					p99Nanos / 3,
					p99Nanos / 2,
					p99Nanos,
					0L,
					0L,
					1L,
					profile == WorkloadProfile.LATENCY || profile == WorkloadProfile.INGEST,
					sloPassed));
		}
		return profiles;
	}
}
