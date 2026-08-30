package it.cavallium.rockserver.core.impl.benchmark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Multiplicity-controlled v2 contract for paired performance evidence.
 *
 * <p>FAIL means a deterministic ceiling was breached or a toleranced regression was demonstrated
 * with Holm family-wise error at {@link #FAMILY_WISE_ALPHA}. A statistically imprecise result is
 * INCONCLUSIVE, never a false FAIL. PASS is an intersection-union claim: every stochastic metric
 * independently proves non-inferiority at alpha 0.05, so the probability of falsely claiming that
 * all metrics are non-inferior is at most 0.05 without assuming independence.</p>
 */
public final class PairedPerformanceContractV2 {

	public static final int REQUIRED_PAIRS = 10;
	public static final double FAMILY_WISE_ALPHA = 0.05d;
	public static final double THROUGHPUT_MINIMUM_RATIO = 0.99d;
	public static final double COST_MAXIMUM_RATIO = 1.02d;
	public static final double MATERIAL_THROUGHPUT_RATIO = 1.02d;
	public static final double MATERIAL_COST_RATIO = 0.98d;

	private PairedPerformanceContractV2() {
	}

	public enum Direction {
		HIGHER_IS_BETTER,
		LOWER_IS_BETTER,
		DETERMINISTIC_NO_INCREASE
	}

	public enum Decision {
		PASS,
		FAIL,
		INCONCLUSIVE
	}

	public record MetricSpec(String name,
	                         Direction direction,
	                         boolean primaryImprovement,
	                         double ratioOffset) {

		public MetricSpec {
			if (name == null || name.isBlank()) throw new IllegalArgumentException("Metric name is required");
			if (direction == null) throw new IllegalArgumentException("Metric direction is required");
			if (!Double.isFinite(ratioOffset) || ratioOffset < 0.0d) {
				throw new IllegalArgumentException("Metric ratio offset must be finite and non-negative");
			}
			if (direction == Direction.DETERMINISTIC_NO_INCREASE && primaryImprovement) {
				throw new IllegalArgumentException("Deterministic ceilings cannot be improvement primaries");
			}
			if (direction == Direction.DETERMINISTIC_NO_INCREASE && ratioOffset != 0.0d) {
				throw new IllegalArgumentException("Deterministic ceilings do not use ratio offsets");
			}
		}

		public static MetricSpec throughput(String name, boolean primaryImprovement) {
			return new MetricSpec(name, Direction.HIGHER_IS_BETTER, primaryImprovement, 0.0d);
		}

		public static MetricSpec cost(String name, boolean primaryImprovement) {
			return new MetricSpec(name, Direction.LOWER_IS_BETTER, primaryImprovement, 0.0d);
		}

		/** Non-negative discrete/sampled cost using a fixed predeclared pseudocount of one. */
		public static MetricSpec countCost(String name) {
			return new MetricSpec(name, Direction.LOWER_IS_BETTER, false, 1.0d);
		}

		public static MetricSpec allocation(String name, boolean primaryImprovement) {
			return cost(name, primaryImprovement);
		}

		public static MetricSpec noIncrease(String name) {
			return new MetricSpec(name, Direction.DETERMINISTIC_NO_INCREASE, false, 0.0d);
		}
	}

	public record MetricSamples(double[] baseline, double[] candidate) {

		public MetricSamples {
			baseline = baseline.clone();
			candidate = candidate.clone();
		}

		@Override public double[] baseline() { return baseline.clone(); }
		@Override public double[] candidate() { return candidate.clone(); }
	}

	public record MetricEvaluation(MetricSpec specification,
	                               PairedBenchmarkStatistics.RatioConfidenceInterval interval,
	                               double nonInferiorityMargin,
	                               double regressionPValue,
	                               double regressionHolmAdjustedPValue,
	                               boolean regressionDemonstrated,
	                               double nonInferiorityPValue,
	                               boolean nonInferiorityProven,
	                               boolean equivalenceProven,
	                               double materialImprovementPValue,
	                               double materialHolmAdjustedPValue,
	                               boolean materialImprovementProven,
	                               boolean deterministicCeilingPassed) {

		MetricEvaluation withAdjusted(double regressionAdjusted,
				double materialAdjusted) {
			return new MetricEvaluation(specification,
					interval,
					nonInferiorityMargin,
					regressionPValue,
					regressionAdjusted,
					regressionAdjusted <= FAMILY_WISE_ALPHA,
					nonInferiorityPValue,
					nonInferiorityProven,
					equivalenceProven,
					materialImprovementPValue,
					materialAdjusted,
					specification.primaryImprovement() && materialAdjusted <= FAMILY_WISE_ALPHA,
					deterministicCeilingPassed);
		}
	}

	public record Evaluation(Map<String, MetricEvaluation> metrics,
	                         Decision decision,
	                         List<String> failures,
	                         List<String> inconclusiveMetrics,
	                         List<String> materialImprovements,
	                         int stochasticHypotheses) {

		public Evaluation {
			metrics = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(metrics));
			failures = List.copyOf(failures);
			inconclusiveMetrics = List.copyOf(inconclusiveMetrics);
			materialImprovements = List.copyOf(materialImprovements);
		}

		public boolean automaticAcceptancePassed() {
			return decision == Decision.PASS;
		}
	}

	public static Evaluation evaluate(List<MetricSpec> specifications,
			Map<String, MetricSamples> samples,
			List<String> structuralFailures) {
		var failures = new ArrayList<>(structuralFailures);
		var preliminary = new LinkedHashMap<String, MetricEvaluation>();
		for (var specification : specifications) {
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
			try {
				var evaluation = specification.direction() == Direction.DETERMINISTIC_NO_INCREASE
						? evaluateDeterministic(specification, baseline, candidate)
						: evaluateStochastic(specification, baseline, candidate);
				preliminary.put(specification.name(), evaluation);
				if (!evaluation.deterministicCeilingPassed()) {
					failures.add(specification.name() + " breached its deterministic no-increase ceiling");
				}
			} catch (IllegalArgumentException malformed) {
				failures.add(specification.name() + " has invalid samples: " + malformed.getMessage());
			}
		}

		var stochastic = preliminary.values().stream()
				.filter(metric -> metric.specification().direction() != Direction.DETERMINISTIC_NO_INCREASE)
				.toList();
		Map<String, Double> regressionAdjusted = holmAdjusted(stochastic, MetricEvaluation::regressionPValue);
		var primary = stochastic.stream().filter(metric -> metric.specification().primaryImprovement()).toList();
		Map<String, Double> materialAdjusted = holmAdjusted(primary, MetricEvaluation::materialImprovementPValue);
		var metrics = new LinkedHashMap<String, MetricEvaluation>();
		var inconclusive = new ArrayList<String>();
		var material = new ArrayList<String>();
		for (var entry : preliminary.entrySet()) {
			var metric = entry.getValue();
			if (metric.specification().direction() == Direction.DETERMINISTIC_NO_INCREASE) {
				metrics.put(entry.getKey(), metric);
				continue;
			}
			var adjusted = metric.withAdjusted(
					regressionAdjusted.get(entry.getKey()),
					materialAdjusted.getOrDefault(entry.getKey(), 1.0d));
			metrics.put(entry.getKey(), adjusted);
			if (adjusted.regressionDemonstrated()) {
				failures.add(entry.getKey() + " demonstrates a toleranced regression after Holm correction");
			} else if (!adjusted.nonInferiorityProven()) {
				inconclusive.add(entry.getKey());
			}
			if (adjusted.materialImprovementProven()) material.add(entry.getKey());
		}
		Decision decision = !failures.isEmpty()
				? Decision.FAIL
				: !inconclusive.isEmpty() ? Decision.INCONCLUSIVE : Decision.PASS;
		return new Evaluation(metrics, decision, failures, inconclusive, material, stochastic.size());
	}

	private static MetricEvaluation evaluateStochastic(MetricSpec specification,
			double[] baseline,
			double[] candidate) {
		var statistics = LogStatistics.of(baseline, candidate, specification.ratioOffset());
		boolean higher = specification.direction() == Direction.HIGHER_IS_BETTER;
		double marginRatio = higher ? 1.0d / THROUGHPUT_MINIMUM_RATIO : COST_MAXIMUM_RATIO;
		double harmMean = higher ? -statistics.meanLog() : statistics.meanLog();
		double harmMargin = Math.log(marginRatio);
		double boundaryT = standardized(harmMean - harmMargin, statistics.standardError());
		double regressionP = upperTail(boundaryT, statistics.degreesOfFreedom());
		double nonInferiorityP = lowerTail(boundaryT, statistics.degreesOfFreedom());

		double lowerEquivalence = higher ? THROUGHPUT_MINIMUM_RATIO : 1.0d / COST_MAXIMUM_RATIO;
		double upperEquivalence = higher ? 1.0d / THROUGHPUT_MINIMUM_RATIO : COST_MAXIMUM_RATIO;
		double lowerP = upperTail(standardized(
				statistics.meanLog() - Math.log(lowerEquivalence), statistics.standardError()),
				statistics.degreesOfFreedom());
		double upperP = lowerTail(standardized(
				statistics.meanLog() - Math.log(upperEquivalence), statistics.standardError()),
				statistics.degreesOfFreedom());

		double benefitMean = higher ? statistics.meanLog() : -statistics.meanLog();
		double materialBoundary = higher
				? Math.log(MATERIAL_THROUGHPUT_RATIO)
				: -Math.log(MATERIAL_COST_RATIO);
		double materialP = upperTail(standardized(
				benefitMean - materialBoundary, statistics.standardError()), statistics.degreesOfFreedom());
		return new MetricEvaluation(specification,
				statistics.interval95(),
				higher ? THROUGHPUT_MINIMUM_RATIO : COST_MAXIMUM_RATIO,
				regressionP,
				1.0d,
				false,
				nonInferiorityP,
				nonInferiorityP <= FAMILY_WISE_ALPHA,
				lowerP <= FAMILY_WISE_ALPHA && upperP <= FAMILY_WISE_ALPHA,
				materialP,
				1.0d,
				false,
				true);
	}

	private static MetricEvaluation evaluateDeterministic(MetricSpec specification,
			double[] baseline,
			double[] candidate) {
		boolean passed = true;
		for (int pair = 0; pair < baseline.length; pair++) {
			if (!Double.isFinite(baseline[pair]) || baseline[pair] < 0.0d
					|| !Double.isFinite(candidate[pair]) || candidate[pair] < 0.0d) {
				throw new IllegalArgumentException("samples must be finite and non-negative at index " + pair);
			}
			passed &= candidate[pair] <= baseline[pair];
		}
		return new MetricEvaluation(specification,
				new PairedBenchmarkStatistics.RatioConfidenceInterval(0, Double.NaN, Double.NaN, Double.NaN),
				1.0d,
				Double.NaN,
				Double.NaN,
				false,
				Double.NaN,
				passed,
				passed,
				Double.NaN,
				Double.NaN,
				false,
				passed);
	}

	private static Map<String, Double> holmAdjusted(List<MetricEvaluation> metrics,
			java.util.function.ToDoubleFunction<MetricEvaluation> pValue) {
		var ordered = new ArrayList<>(metrics);
		ordered.sort(Comparator.comparingDouble(pValue));
		var adjusted = new LinkedHashMap<String, Double>();
		double previous = 0.0d;
		for (int rank = 0; rank < ordered.size(); rank++) {
			var metric = ordered.get(rank);
			double value = Math.min(1.0d, (ordered.size() - rank) * pValue.applyAsDouble(metric));
			previous = Math.max(previous, value);
			adjusted.put(metric.specification().name(), previous);
		}
		return Map.copyOf(adjusted);
	}

	static double studentTCdf(double value, int degreesOfFreedom) {
		if (degreesOfFreedom < 1) throw new IllegalArgumentException("degrees of freedom must be positive");
		if (Double.isNaN(value)) return Double.NaN;
		if (value == Double.NEGATIVE_INFINITY) return 0.0d;
		if (value == Double.POSITIVE_INFINITY) return 1.0d;
		if (value == 0.0d) return 0.5d;
		double x = degreesOfFreedom / (degreesOfFreedom + value * value);
		double halfBeta = 0.5d * regularizedBeta(x, degreesOfFreedom / 2.0d, 0.5d);
		return value > 0.0d ? 1.0d - halfBeta : halfBeta;
	}

	private static double lowerTail(double t, int degreesOfFreedom) {
		if (t == Double.NEGATIVE_INFINITY) return 0.0d;
		if (t == Double.POSITIVE_INFINITY) return 1.0d;
		return studentTCdf(t, degreesOfFreedom);
	}

	private static double upperTail(double t, int degreesOfFreedom) {
		if (t == Double.NEGATIVE_INFINITY) return 1.0d;
		if (t == Double.POSITIVE_INFINITY) return 0.0d;
		return 1.0d - studentTCdf(t, degreesOfFreedom);
	}

	private static double standardized(double difference, double standardError) {
		if (standardError != 0.0d) return difference / standardError;
		if (Math.abs(difference) <= 1.0e-14d) return 0.0d;
		return difference < 0.0d ? Double.NEGATIVE_INFINITY
				: difference > 0.0d ? Double.POSITIVE_INFINITY : 0.0d;
	}

	private static double regularizedBeta(double x, double a, double b) {
		if (x <= 0.0d) return 0.0d;
		if (x >= 1.0d) return 1.0d;
		double front = Math.exp(logGamma(a + b) - logGamma(a) - logGamma(b)
				+ a * Math.log(x) + b * Math.log1p(-x));
		return x < (a + 1.0d) / (a + b + 2.0d)
				? front * betaFraction(x, a, b) / a
				: 1.0d - front * betaFraction(1.0d - x, b, a) / b;
	}

	private static double betaFraction(double x, double a, double b) {
		final int maximumIterations = 10_000;
		final double epsilon = 3.0e-14d;
		final double minimum = 1.0e-300d;
		double qab = a + b;
		double qap = a + 1.0d;
		double qam = a - 1.0d;
		double c = 1.0d;
		double d = 1.0d - qab * x / qap;
		if (Math.abs(d) < minimum) d = minimum;
		d = 1.0d / d;
		double result = d;
		for (int iteration = 1; iteration <= maximumIterations; iteration++) {
			int twice = 2 * iteration;
			double aa = iteration * (b - iteration) * x / ((qam + twice) * (a + twice));
			d = 1.0d + aa * d;
			if (Math.abs(d) < minimum) d = minimum;
			c = 1.0d + aa / c;
			if (Math.abs(c) < minimum) c = minimum;
			d = 1.0d / d;
			result *= d * c;
			aa = -(a + iteration) * (qab + iteration) * x / ((a + twice) * (qap + twice));
			d = 1.0d + aa * d;
			if (Math.abs(d) < minimum) d = minimum;
			c = 1.0d + aa / c;
			if (Math.abs(c) < minimum) c = minimum;
			d = 1.0d / d;
			double delta = d * c;
			result *= delta;
			if (Math.abs(delta - 1.0d) <= epsilon) return result;
		}
		throw new IllegalStateException("Incomplete beta fraction did not converge");
	}

	private static double logGamma(double value) {
		double[] coefficients = {
				676.5203681218851d, -1259.1392167224028d, 771.32342877765313d,
				-176.61502916214059d, 12.507343278686905d, -0.13857109526572012d,
				9.9843695780195716e-6d, 1.5056327351493116e-7d
		};
		if (value < 0.5d) {
			return Math.log(Math.PI) - Math.log(Math.sin(Math.PI * value)) - logGamma(1.0d - value);
		}
		double shifted = value - 1.0d;
		double sum = 0.99999999999980993d;
		for (int index = 0; index < coefficients.length; index++) {
			sum += coefficients[index] / (shifted + index + 1.0d);
		}
		double t = shifted + coefficients.length - 0.5d;
		return 0.5d * Math.log(2.0d * Math.PI) + (shifted + 0.5d) * Math.log(t) - t + Math.log(sum);
	}

	private record LogStatistics(double meanLog,
				double standardError,
				int degreesOfFreedom,
				PairedBenchmarkStatistics.RatioConfidenceInterval interval95) {

		private static LogStatistics of(double[] baseline, double[] candidate, double ratioOffset) {
			if (baseline.length != candidate.length || baseline.length < 2) {
				throw new IllegalArgumentException("paired samples require at least two equal-length vectors");
			}
			double[] logs = new double[baseline.length];
			double sum = 0.0d;
			double minimum = Double.POSITIVE_INFINITY;
			double maximum = Double.NEGATIVE_INFINITY;
			for (int pair = 0; pair < baseline.length; pair++) {
				if (!Double.isFinite(baseline[pair]) || baseline[pair] < 0.0d
						|| !Double.isFinite(candidate[pair]) || candidate[pair] < 0.0d
						|| baseline[pair] + ratioOffset <= 0.0d
						|| candidate[pair] + ratioOffset <= 0.0d) {
					throw new IllegalArgumentException(
							"stochastic samples plus their predeclared offset must be finite and positive at index "
									+ pair);
				}
				logs[pair] = Math.log((candidate[pair] + ratioOffset) / (baseline[pair] + ratioOffset));
				sum += logs[pair];
				minimum = Math.min(minimum, logs[pair]);
				maximum = Math.max(maximum, logs[pair]);
			}
			double mean = sum / logs.length;
			double squared = 0.0d;
			for (double log : logs) {
				double deviation = log - mean;
				squared += deviation * deviation;
			}
			double standardError = minimum == maximum
					? 0.0d
					: Math.sqrt(squared / (logs.length - 1.0d)) / Math.sqrt(logs.length);
			return new LogStatistics(mean,
					standardError,
					logs.length - 1,
					PairedBenchmarkStatistics.logRatioConfidenceInterval(
							Arrays.stream(logs).map(Math::exp).toArray()));
		}
	}
}
