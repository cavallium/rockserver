package it.cavallium.rockserver.core.impl.benchmark;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared v1.3.11 Pareto contract for counterbalanced paired benchmark results.
 *
 * <p>Automatic acceptance is deliberately stricter than the exception ceilings. A metric's
 * geometric-mean estimate must be no worse than equality and its confidence interval must not
 * demonstrate a regression. Exception ceilings are reported only as review candidates: they
 * never turn a failed automatic gate into a pass.</p>
 */
public final class PairedPerformanceContract {

	public static final int REQUIRED_PAIRS = 10;
	public static final double MATERIAL_THROUGHPUT_RATIO = 1.02d;
	public static final double MATERIAL_COST_RATIO = 0.98d;
	public static final double THROUGHPUT_EXCEPTION_CEILING = 0.99d;
	public static final double COST_EXCEPTION_CEILING = 1.02d;

	private PairedPerformanceContract() {
	}

	public enum Direction {
		HIGHER_IS_BETTER,
		LOWER_IS_BETTER,
		NO_INCREASE
	}

	public record MetricSpec(String name,
	                         Direction direction,
	                         boolean primary,
	                         double exceptionCeiling) {

		public MetricSpec {
			if (name == null || name.isBlank()) {
				throw new IllegalArgumentException("Metric name is required");
			}
			if (direction == null) {
				throw new IllegalArgumentException("Metric direction is required");
			}
			if (direction == Direction.NO_INCREASE && primary) {
				throw new IllegalArgumentException("Exact no-increase metrics cannot be primary improvements");
			}
			if (Double.isFinite(exceptionCeiling) && exceptionCeiling <= 0.0d) {
				throw new IllegalArgumentException("Exception ceilings must be positive or unavailable");
			}
		}

		public static MetricSpec throughput(String name, boolean primary) {
			return new MetricSpec(name, Direction.HIGHER_IS_BETTER, primary,
					THROUGHPUT_EXCEPTION_CEILING);
		}

		public static MetricSpec cost(String name, boolean primary) {
			return new MetricSpec(name, Direction.LOWER_IS_BETTER, primary,
					COST_EXCEPTION_CEILING);
		}

		public static MetricSpec allocation(String name, boolean primary) {
			return new MetricSpec(name, Direction.LOWER_IS_BETTER, primary, Double.NaN);
		}

		public static MetricSpec noIncrease(String name) {
			return new MetricSpec(name, Direction.NO_INCREASE, false, Double.NaN);
		}
	}

	public record MetricSamples(double[] baseline, double[] candidate) {

		public MetricSamples {
			baseline = baseline.clone();
			candidate = candidate.clone();
		}

		@Override
		public double[] baseline() {
			return baseline.clone();
		}

		@Override
		public double[] candidate() {
			return candidate.clone();
		}
	}

	public record MetricEvaluation(MetricSpec specification,
	                               PairedBenchmarkStatistics.RatioConfidenceInterval interval,
	                               boolean pointEstimatePassed,
	                               boolean confidenceNonRegressionPassed,
	                               boolean materialImprovement,
	                               boolean exceptionCeilingPassed) {

		public boolean automaticNonRegressionPassed() {
			return pointEstimatePassed && confidenceNonRegressionPassed;
		}

		public boolean exceptionCandidate() {
			return !automaticNonRegressionPassed()
					&& Double.isFinite(specification.exceptionCeiling())
					&& exceptionCeilingPassed;
		}
	}

	public record Evaluation(Map<String, MetricEvaluation> metrics,
	                         List<String> failures,
	                         List<String> materialImprovements,
	                         List<String> exceptionCandidates,
	                         boolean materialImprovementRequired) {

		public Evaluation {
			metrics = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(metrics));
			failures = List.copyOf(failures);
			materialImprovements = List.copyOf(materialImprovements);
			exceptionCandidates = List.copyOf(exceptionCandidates);
		}

		public boolean nonRegressionPassed() {
			return failures.stream().noneMatch(failure -> !failure.equals(
					"no predeclared primary metric demonstrates a material improvement"));
		}

		public boolean materialImprovementProven() {
			return !materialImprovements.isEmpty();
		}

		public boolean automaticAcceptancePassed() {
			return failures.isEmpty();
		}
	}

	public static Evaluation evaluate(List<MetricSpec> specifications,
	                                  Map<String, MetricSamples> samples,
	                                  List<String> structuralFailures,
	                                  boolean requireMaterialImprovement) {
		Map<String, MetricEvaluation> evaluations = new LinkedHashMap<>();
		List<String> failures = new ArrayList<>(structuralFailures);
		List<String> materialImprovements = new ArrayList<>();
		List<String> exceptionCandidates = new ArrayList<>();
		for (MetricSpec specification : specifications) {
			MetricSamples metricSamples = samples.get(specification.name());
			if (metricSamples == null) {
				failures.add("missing metric " + specification.name());
				continue;
			}
			double[] baseline = metricSamples.baseline();
			double[] candidate = metricSamples.candidate();
			if (baseline.length != REQUIRED_PAIRS || candidate.length != REQUIRED_PAIRS) {
				failures.add(specification.name() + " requires exactly " + REQUIRED_PAIRS + " pairs");
				continue;
			}
			MetricEvaluation evaluation;
			try {
				evaluation = specification.direction() == Direction.NO_INCREASE
						? evaluateNoIncrease(specification, baseline, candidate)
						: evaluateRatio(specification, baseline, candidate);
			} catch (IllegalArgumentException malformed) {
				failures.add(specification.name() + " has invalid samples: " + malformed.getMessage());
				continue;
			}
			evaluations.put(specification.name(), evaluation);
			if (!evaluation.pointEstimatePassed()) {
				failures.add(specification.name() + " geometric-mean ratio "
						+ format(evaluation.interval().mean()) + " is worse than 1.000000");
			}
			if (!evaluation.confidenceNonRegressionPassed()) {
				failures.add(specification.name() + " confidence interval demonstrates a regression: ["
						+ format(evaluation.interval().lower95()) + ", "
						+ format(evaluation.interval().upper95()) + ']');
			}
			if (evaluation.materialImprovement()) {
				materialImprovements.add(specification.name());
			}
			if (evaluation.exceptionCandidate()) {
				exceptionCandidates.add(specification.name());
			}
		}
		if (requireMaterialImprovement && materialImprovements.isEmpty()) {
			failures.add("no predeclared primary metric demonstrates a material improvement");
		}
		return new Evaluation(evaluations, failures, materialImprovements, exceptionCandidates,
				requireMaterialImprovement);
	}

	private static MetricEvaluation evaluateRatio(MetricSpec specification,
	                                             double[] baseline,
	                                             double[] candidate) {
		var interval = PairedBenchmarkStatistics.pairedLogRatio(baseline, candidate);
		if (!interval.available()) {
			throw new IllegalArgumentException("confidence interval is unavailable");
		}
		boolean higher = specification.direction() == Direction.HIGHER_IS_BETTER;
		boolean pointPassed = higher ? interval.mean() >= 1.0d : interval.mean() <= 1.0d;
		boolean confidencePassed = higher ? interval.upper95() >= 1.0d : interval.lower95() <= 1.0d;
		boolean material = specification.primary() && (higher
				? interval.lower95() >= MATERIAL_THROUGHPUT_RATIO
				: interval.upper95() <= MATERIAL_COST_RATIO);
		boolean exceptionPassed = !Double.isFinite(specification.exceptionCeiling())
				|| (higher ? interval.mean() >= specification.exceptionCeiling()
				: interval.mean() <= specification.exceptionCeiling());
		return new MetricEvaluation(specification, interval, pointPassed, confidencePassed,
				material, exceptionPassed);
	}

	private static MetricEvaluation evaluateNoIncrease(MetricSpec specification,
	                                                  double[] baseline,
	                                                  double[] candidate) {
		boolean passed = true;
		boolean positive = true;
		for (int index = 0; index < baseline.length; index++) {
			if (!Double.isFinite(baseline[index]) || baseline[index] < 0.0d
					|| !Double.isFinite(candidate[index]) || candidate[index] < 0.0d) {
				throw new IllegalArgumentException("samples must be finite and non-negative at index " + index);
			}
			passed &= candidate[index] <= baseline[index];
			positive &= baseline[index] > 0.0d && candidate[index] > 0.0d;
		}
		var interval = positive
				? PairedBenchmarkStatistics.pairedLogRatio(baseline, candidate)
				: new PairedBenchmarkStatistics.RatioConfidenceInterval(
						0, Double.NaN, Double.NaN, Double.NaN);
		return new MetricEvaluation(specification, interval, passed, passed, false, false);
	}

	private static String format(double value) {
		return String.format(java.util.Locale.ROOT, "%.6f", value);
	}
}
