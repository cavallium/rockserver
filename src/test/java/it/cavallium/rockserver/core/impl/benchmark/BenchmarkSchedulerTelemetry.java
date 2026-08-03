package it.cavallium.rockserver.core.impl.benchmark;

import it.cavallium.rockserver.core.impl.RWScheduler;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/** Binary-compatible access to scheduler accounting added after the immutable performance baseline. */
final class BenchmarkSchedulerTelemetry {

	private static final MethodHandle PARKED_TASKS = optionalIntAccessor("parkedTasks");
	private static final MethodHandle OUTSTANDING_TASKS = optionalIntAccessor("outstandingTasks");
	private static final MethodHandle SUBMISSION_ATTEMPTS = optionalLongAccessor("submissionAttempts");
	private static final RWScheduler.TerminalOutcome[] TERMINAL_OUTCOME_VALUES =
			RWScheduler.TerminalOutcome.values();
	private static final boolean EXACT_ACCOUNTING = PARKED_TASKS != null
			&& OUTSTANDING_TASKS != null && SUBMISSION_ATTEMPTS != null;

	private BenchmarkSchedulerTelemetry() {
	}

	/** Standalone binary-compatibility probe for the immutable baseline and the Wave 1 candidate. */
	public static void main(String[] args) {
		String expectedValue = System.getProperty("rockserver.benchmark.expected-exact-accounting");
		if (!"true".equals(expectedValue) && !"false".equals(expectedValue)) {
			throw new IllegalArgumentException(
					"Set -Drockserver.benchmark.expected-exact-accounting=true|false");
		}
		boolean expected = Boolean.parseBoolean(expectedValue);
		if (EXACT_ACCOUNTING != expected) {
			throw new IllegalStateException("Scheduler accounting API mismatch: expected=" + expected
					+ " actual=" + EXACT_ACCOUNTING);
		}
		System.out.println("Scheduler accounting ABI probe passed; exact=" + EXACT_ACCOUNTING);
	}

	static int outstandingTasks(RWScheduler.PoolSnapshot snapshot) {
		if (OUTSTANDING_TASKS != null) return invokeInt(OUTSTANDING_TASKS, snapshot);
		return legacyOutstandingTasks(snapshot.acceptedTasks(), terminalOutcomes(snapshot));
	}

	static int parkedTasks(RWScheduler.PoolSnapshot snapshot, int outstandingTasks) {
		if (PARKED_TASKS != null) return invokeInt(PARKED_TASKS, snapshot);
		return legacyParkedTasks(outstandingTasks, snapshot.queuedTasks(), snapshot.activeTasks());
	}

	static long submissionAttempts(RWScheduler.PoolSnapshot snapshot) {
		return SUBMISSION_ATTEMPTS != null ? invokeLong(SUBMISSION_ATTEMPTS, snapshot) : snapshot.acceptedTasks();
	}

	static long terminalOutcomes(RWScheduler.PoolSnapshot snapshot) {
		long total = 0L;
		for (RWScheduler.TerminalOutcome outcome : TERMINAL_OUTCOME_VALUES) {
			Long count = snapshot.outcomes().get(outcome);
			if (count != null) total += count;
		}
		return total;
	}

	static boolean exactAccounting() {
		return EXACT_ACCOUNTING;
	}

	static int legacyOutstandingTasks(long acceptedTasks, long terminalOutcomes) {
		long outstanding = Math.max(0L, acceptedTasks - terminalOutcomes);
		return Math.toIntExact(Math.min(Integer.MAX_VALUE, outstanding));
	}

	static int legacyParkedTasks(int outstandingTasks, int queuedTasks, int activeTasks) {
		return Math.max(0, outstandingTasks - queuedTasks - activeTasks);
	}

	private static MethodHandle optionalIntAccessor(String name) {
		return optionalAccessor(name, int.class);
	}

	private static MethodHandle optionalLongAccessor(String name) {
		return optionalAccessor(name, long.class);
	}

	private static MethodHandle optionalAccessor(String name, Class<?> result) {
		try {
			return MethodHandles.lookup().findVirtual(RWScheduler.PoolSnapshot.class, name,
					MethodType.methodType(result));
		} catch (NoSuchMethodException missingOnImmutableBaseline) {
			return null;
		} catch (IllegalAccessException inaccessible) {
			throw new ExceptionInInitializerError(inaccessible);
		}
	}

	private static int invokeInt(MethodHandle accessor, RWScheduler.PoolSnapshot snapshot) {
		try {
			return (int) accessor.invokeExact(snapshot);
		} catch (Throwable failure) {
			throw rethrow(failure);
		}
	}

	private static long invokeLong(MethodHandle accessor, RWScheduler.PoolSnapshot snapshot) {
		try {
			return (long) accessor.invokeExact(snapshot);
		} catch (Throwable failure) {
			throw rethrow(failure);
		}
	}

	private static RuntimeException rethrow(Throwable failure) {
		if (failure instanceof RuntimeException runtime) return runtime;
		if (failure instanceof Error error) throw error;
		return new IllegalStateException("Unable to read scheduler benchmark telemetry", failure);
	}
}
