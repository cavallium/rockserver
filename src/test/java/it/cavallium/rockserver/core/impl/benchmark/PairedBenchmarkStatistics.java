package it.cavallium.rockserver.core.impl.benchmark;

/**
 * Statistical helpers shared by opt-in paired subprocess benchmarks.
 */
public final class PairedBenchmarkStatistics {

	private PairedBenchmarkStatistics() {
	}

	/**
	 * Computes the geometric mean candidate/baseline ratio and its two-sided 95% Student-t
	 * confidence interval. The interval is formed in log space and exponentiated only at the end.
	 */
	public static RatioConfidenceInterval pairedLogRatio(double[] baseline, double[] candidate) {
		if (baseline.length != candidate.length) {
			throw new IllegalArgumentException("Paired samples must have the same length");
		}
		double[] ratios = new double[baseline.length];
		for (int index = 0; index < ratios.length; index++) {
			double base = baseline[index];
			double next = candidate[index];
			if (!Double.isFinite(base) || base <= 0.0d || !Double.isFinite(next) || next <= 0.0d) {
				throw new IllegalArgumentException("Paired samples must be finite and positive at index " + index);
			}
			ratios[index] = next / base;
		}
		return logRatioConfidenceInterval(ratios);
	}

	/**
	 * Computes a geometric-mean Student-t interval from precomputed positive ratios.
	 */
	public static RatioConfidenceInterval logRatioConfidenceInterval(double[] ratios) {
		if (ratios.length == 0) {
			return RatioConfidenceInterval.unavailable();
		}
		double[] logs = new double[ratios.length];
		double sum = 0.0d;
		for (int index = 0; index < ratios.length; index++) {
			double ratio = ratios[index];
			if (!Double.isFinite(ratio) || ratio <= 0.0d) {
				throw new IllegalArgumentException("Ratios must be finite and positive at index " + index);
			}
			double log = Math.log(ratio);
			logs[index] = log;
			sum += log;
		}
		double meanLog = sum / logs.length;
		if (logs.length == 1) {
			double ratio = Math.exp(meanLog);
			return new RatioConfidenceInterval(1, ratio, ratio, ratio);
		}
		double squaredDeviations = 0.0d;
		for (double log : logs) {
			double deviation = log - meanLog;
			squaredDeviations += deviation * deviation;
		}
		double standardError = Math.sqrt(squaredDeviations / (logs.length - 1L)) / Math.sqrt(logs.length);
		double margin = studentTCritical95(logs.length - 1) * standardError;
		return new RatioConfidenceInterval(logs.length,
				Math.exp(meanLog),
				Math.exp(meanLog - margin),
				Math.exp(meanLog + margin));
	}

	private static double studentTCritical95(int degreesOfFreedom) {
		return switch (degreesOfFreedom) {
			case 1 -> 12.706d;
			case 2 -> 4.303d;
			case 3 -> 3.182d;
			case 4 -> 2.776d;
			case 5 -> 2.571d;
			case 6 -> 2.447d;
			case 7 -> 2.365d;
			case 8 -> 2.306d;
			case 9 -> 2.262d;
			case 10 -> 2.228d;
			case 11 -> 2.201d;
			case 12 -> 2.179d;
			case 13 -> 2.160d;
			case 14 -> 2.145d;
			case 15 -> 2.131d;
			case 16 -> 2.120d;
			case 17 -> 2.110d;
			case 18 -> 2.101d;
			case 19 -> 2.093d;
			case 20 -> 2.086d;
			case 21 -> 2.080d;
			case 22 -> 2.074d;
			case 23 -> 2.069d;
			case 24 -> 2.064d;
			case 25 -> 2.060d;
			case 26 -> 2.056d;
			case 27 -> 2.052d;
			case 28 -> 2.048d;
			case 29 -> 2.045d;
			default -> degreesOfFreedom < 40 ? 2.021d : degreesOfFreedom < 60 ? 2.000d : 1.960d;
		};
	}

	/**
	 * Geometric candidate/baseline ratio with a two-sided 95% confidence interval.
	 */
	public record RatioConfidenceInterval(int samples, double mean, double lower95, double upper95) {

		private static RatioConfidenceInterval unavailable() {
			return new RatioConfidenceInterval(0, Double.NaN, Double.NaN, Double.NaN);
		}

		public boolean available() {
			return samples > 0
					&& Double.isFinite(mean)
					&& Double.isFinite(lower95)
					&& Double.isFinite(upper95)
					&& mean > 0.0d
					&& lower95 > 0.0d
					&& upper95 >= lower95;
		}
	}
}
