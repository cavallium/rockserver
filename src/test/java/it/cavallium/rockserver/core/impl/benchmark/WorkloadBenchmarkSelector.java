package it.cavallium.rockserver.core.impl.benchmark;

import it.cavallium.rockserver.core.common.WorkloadProfile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Pure candidate generation and selection rules for the seven-profile hardware benchmark. */
public final class WorkloadBenchmarkSelector {

	public static final double THROUGHPUT_TOLERANCE = 0.05d;
	public static final double P99_TOLERANCE = 0.10d;
	public static final int MINIMUM_CANDIDATE = 4;
	public static final List<WorkloadProfile> ALL_PROFILES = List.of(
			WorkloadProfile.CONTROL,
			WorkloadProfile.LATENCY,
			WorkloadProfile.ANALYTICAL,
			WorkloadProfile.INGEST,
			WorkloadProfile.CDC,
			WorkloadProfile.BATCH,
			WorkloadProfile.PHYSICAL_MAINTENANCE);

	private WorkloadBenchmarkSelector() {
	}

	/** Generate every power of two in the inclusive range. */
	public static List<Integer> powersOfTwo(int minimum, int maximum) {
		if (minimum < 1 || maximum < minimum) {
			throw new IllegalArgumentException("Candidate bounds must be positive and ordered");
		}
		var candidates = new ArrayList<Integer>();
		int effectiveMinimum = Math.max(minimum, MINIMUM_CANDIDATE);
		int candidate = Integer.highestOneBit(effectiveMinimum);
		if (candidate < effectiveMinimum) {
			if (candidate >= (1 << 30)) {
				throw new IllegalArgumentException("Candidate range overflows int powers of two");
			}
			candidate <<= 1;
		}
		while (candidate <= maximum) {
			candidates.add(candidate);
			if (candidate > (1 << 29)) {
				break;
			}
			candidate <<= 1;
		}
		if (candidates.isEmpty()) {
			throw new IllegalArgumentException("Candidate range contains no power of two");
		}
		return List.copyOf(candidates);
	}

	/**
	 * Choose the smallest setting that is within 5% of maximum throughput, within 10% of
	 * the minimum relevant p99, and passes every per-profile SLO, run-check, and leak gate.
	 */
	public static Selection select(List<CandidateMeasurement> input) {
		if (input.isEmpty()) {
			throw new IllegalArgumentException("At least one candidate measurement is required");
		}
		var measurements = input.stream()
				.sorted(Comparator.comparingInt(CandidateMeasurement::candidate))
				.toList();
		var seen = new HashSet<Integer>();
		String datasetFingerprint = measurements.getFirst().datasetFingerprint();
		String comparisonFingerprint = measurements.getFirst().comparisonFingerprint();
		String buildId = measurements.getFirst().buildId();
		String storageLabel = measurements.getFirst().storageLabel();
		long seed = measurements.getFirst().seed();
		int previousCandidate = 0;
		for (var measurement : measurements) {
			if (!measurement.enforcedHardwareRun()) {
				throw new IllegalArgumentException(
						"Selection requires enforced cold-cache hardware results");
			}
			if (measurement.candidate() < MINIMUM_CANDIDATE
					|| !isPowerOfTwo(measurement.candidate()) || !seen.add(measurement.candidate())) {
				throw new IllegalArgumentException("Candidates must be unique powers of two of at least four: "
						+ measurement.candidate());
			}
			if (previousCandidate != 0 && measurement.candidate() != previousCandidate * 2) {
				throw new IllegalArgumentException("Candidates must form a contiguous powers-of-two sequence");
			}
			previousCandidate = measurement.candidate();
			if (!datasetFingerprint.equals(measurement.datasetFingerprint())
					|| !comparisonFingerprint.equals(measurement.comparisonFingerprint())
					|| !buildId.equals(measurement.buildId())
					|| !storageLabel.equals(measurement.storageLabel())
					|| seed != measurement.seed()) {
				throw new IllegalArgumentException(
						"Candidates must share dataset, comparison shape, build ID, storage label, and seed");
			}
		}

		var referenceMeasurements = measurements.stream()
				.filter(CandidateMeasurement::selectionReferenceEligible)
				.toList();
		if (referenceMeasurements.isEmpty()) {
			throw new IllegalStateException("No valid benchmark candidate can define performance windows");
		}
		double maximumThroughput = referenceMeasurements.stream()
				.mapToDouble(CandidateMeasurement::totalThroughput)
				.max()
				.orElseThrow();
		long minimumRelevantP99 = referenceMeasurements.stream()
				.mapToLong(CandidateMeasurement::relevantP99Nanos)
				.filter(value -> value > 0L)
				.min()
				.orElseThrow(() -> new IllegalArgumentException("No candidate has a relevant p99 sample"));
		double minimumThroughput = maximumThroughput * (1.0d - THROUGHPUT_TOLERANCE);
		double maximumP99 = minimumRelevantP99 * (1.0d + P99_TOLERANCE);

		var evaluations = new ArrayList<CandidateEvaluation>(measurements.size());
		CandidateMeasurement winner = null;
		for (var measurement : measurements) {
			boolean throughputPassed = measurement.totalThroughput() >= minimumThroughput;
			boolean p99Passed = measurement.relevantP99Nanos() > 0L
					&& measurement.relevantP99Nanos() <= maximumP99;
			boolean slosPassed = measurement.allSlosPassed();
			boolean leaksPassed = measurement.leakedResources() == 0L;
			var evaluation = new CandidateEvaluation(
					measurement.candidate(),
					measurement.totalThroughput(),
					measurement.relevantP99Nanos(),
					throughputPassed,
					p99Passed,
					slosPassed,
					leaksPassed,
					measurement.runChecksPassed());
			evaluations.add(evaluation);
			if (winner == null && evaluation.eligible()) {
				winner = measurement;
			}
		}
		if (winner == null) {
			throw new IllegalStateException(
					"No benchmark candidate satisfies throughput, p99, SLO, run-check, and leak gates");
		}

		int winnerIndex = measurements.indexOf(winner);
		var adjacent = new ArrayList<Integer>(2);
		if (winnerIndex > 0) {
			adjacent.add(measurements.get(winnerIndex - 1).candidate());
		}
		if (winnerIndex + 1 < measurements.size()) {
			adjacent.add(measurements.get(winnerIndex + 1).candidate());
		}
		return new Selection(
				datasetFingerprint,
				comparisonFingerprint,
				buildId,
				storageLabel,
				seed,
				winner.candidate(),
				maximumThroughput,
				minimumRelevantP99,
				List.copyOf(adjacent),
				List.copyOf(evaluations));
	}

	private static boolean isPowerOfTwo(int value) {
		return value > 0 && (value & (value - 1)) == 0;
	}

	public record ProfileMeasurement(double throughput,
			long queueP99Nanos,
			long executionP99Nanos,
			long endToEndP99Nanos,
			long rejections,
			long cancellations,
			long quantumCount,
			boolean relevantP99,
			boolean sloPassed) {

		public ProfileMeasurement {
			if (!Double.isFinite(throughput) || throughput < 0.0d
					|| queueP99Nanos < 0L || executionP99Nanos < 0L || endToEndP99Nanos < 0L
					|| rejections < 0L || cancellations < 0L || quantumCount < 0L) {
				throw new IllegalArgumentException("Profile measurements must be finite and non-negative");
			}
		}
	}

	public record CandidateMeasurement(int candidate,
			String datasetFingerprint,
			String comparisonFingerprint,
			String buildId,
			String storageLabel,
			long seed,
			boolean enforcedHardwareRun,
			boolean runChecksPassed,
			Map<WorkloadProfile, ProfileMeasurement> profiles,
			long maximumCdcLag,
			long maximumRetainedSnapshots,
			long maximumStoragePressure,
			long leakedResources) {

		public CandidateMeasurement {
			Objects.requireNonNull(datasetFingerprint, "datasetFingerprint");
			Objects.requireNonNull(comparisonFingerprint, "comparisonFingerprint");
			Objects.requireNonNull(buildId, "buildId");
			Objects.requireNonNull(storageLabel, "storageLabel");
			Objects.requireNonNull(profiles, "profiles");
			if (datasetFingerprint.isBlank() || comparisonFingerprint.isBlank()
					|| buildId.isBlank() || storageLabel.isBlank()) {
				throw new IllegalArgumentException(
						"Dataset fingerprint, comparison fingerprint, build ID, and storage label are required");
			}
			for (String value : List.of(datasetFingerprint, comparisonFingerprint, buildId, storageLabel)) {
				if (!value.equals(value.strip()) || value.indexOf('\n') >= 0
						|| value.indexOf('\r') >= 0 || value.indexOf('\\') >= 0) {
					throw new IllegalArgumentException(
							"Benchmark identity values must be single-line canonical property values");
				}
			}
			if (maximumCdcLag < 0L || maximumRetainedSnapshots < 0L
					|| maximumStoragePressure < 0L || leakedResources < 0L) {
				throw new IllegalArgumentException("Candidate counters must be non-negative");
			}
			var copy = new EnumMap<WorkloadProfile, ProfileMeasurement>(WorkloadProfile.class);
			copy.putAll(profiles);
			if (copy.size() != ALL_PROFILES.size()) {
				throw new IllegalArgumentException("Every one of the seven profiles must be measured");
			}
			for (var profile : ALL_PROFILES) {
				Objects.requireNonNull(copy.get(profile), "Missing profile measurement: " + profile);
			}
			if (!Double.isFinite(copy.values().stream().mapToDouble(ProfileMeasurement::throughput).sum())) {
				throw new IllegalArgumentException("Aggregate candidate throughput must be finite");
			}
			profiles = Map.copyOf(copy);
		}

		public double totalThroughput() {
			return profiles.values().stream().mapToDouble(ProfileMeasurement::throughput).sum();
		}

		public long relevantP99Nanos() {
			return profiles.values().stream()
					.filter(ProfileMeasurement::relevantP99)
					.mapToLong(ProfileMeasurement::endToEndP99Nanos)
					.max()
					.orElse(0L);
		}

		public boolean allSlosPassed() {
			return profiles.values().stream().allMatch(ProfileMeasurement::sloPassed);
		}

		public boolean hardwareAcceptancePassed() {
			return enforcedHardwareRun && runChecksPassed;
		}

		private boolean selectionReferenceEligible() {
			return hardwareAcceptancePassed() && allSlosPassed() && leakedResources == 0L;
		}
	}

	public record CandidateEvaluation(int candidate,
			double throughput,
			long relevantP99Nanos,
			boolean throughputPassed,
			boolean p99Passed,
			boolean slosPassed,
			boolean leaksPassed,
			boolean runChecksPassed) {

		public boolean eligible() {
			return throughputPassed && p99Passed && slosPassed && leaksPassed && runChecksPassed;
		}
	}

	public record Selection(String datasetFingerprint,
			String comparisonFingerprint,
			String buildId,
			String storageLabel,
			long seed,
			int winner,
			double maximumThroughput,
			long minimumRelevantP99Nanos,
			List<Integer> adjacentCandidates,
			List<CandidateEvaluation> evaluations) {

		public Selection {
			adjacentCandidates = List.copyOf(adjacentCandidates);
			evaluations = List.copyOf(evaluations);
		}
	}
}
