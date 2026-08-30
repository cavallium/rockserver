package it.cavallium.rockserver.core.impl.benchmark;

import java.io.IOException;
import java.nio.file.Path;

/** Compatibility entry point; the immutable controller now owns evaluation. */
public final class FiniteDeadlineContentionEvaluator {

	private FiniteDeadlineContentionEvaluator() {}

	public static void main(String[] args) throws Exception {
		if (args.length != 1) throw new IllegalArgumentException("Expected one prepared benchmark root");
		FiniteDeadlineContentionBenchmark.evaluate(Path.of(args[0]).toAbsolutePath().normalize());
	}

	static FiniteDeadlineContentionBenchmark.Result evaluate(Path root) throws IOException {
		return FiniteDeadlineContentionBenchmark.evaluate(root);
	}
}
