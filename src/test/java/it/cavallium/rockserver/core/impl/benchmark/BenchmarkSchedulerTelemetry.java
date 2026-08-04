package it.cavallium.rockserver.core.impl.benchmark;

import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/** Binary-compatible access to scheduler accounting added after the immutable performance baseline. */
final class BenchmarkSchedulerTelemetry {

	private static final MethodHandle PARKED_TASKS = optionalIntAccessor("parkedTasks");
	private static final MethodHandle OUTSTANDING_TASKS = optionalIntAccessor("outstandingTasks");
	private static final MethodHandle SUBMISSION_ATTEMPTS = optionalLongAccessor("submissionAttempts");
	private static final MethodHandle COPY_POOL_TELEMETRY = optionalPoolTelemetryCopier();
	private static final RWScheduler.TerminalOutcome[] TERMINAL_OUTCOME_VALUES =
			RWScheduler.TerminalOutcome.values();
	private static final WorkloadProfile[] WORKLOAD_PROFILES = WorkloadProfile.values();
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

	static boolean allocationFreePoolTelemetry() {
		return COPY_POOL_TELEMETRY != null;
	}

	/** Copy a common primitive view, using the immutable baseline snapshot only as an ABI fallback. */
	static void copyPoolTelemetry(RWScheduler scheduler, RWScheduler.Pool pool, long[] target) {
		if (target.length < RWScheduler.POOL_TELEMETRY_LENGTH) {
			throw new IllegalArgumentException("Pool telemetry buffer is too small");
		}
		var copier = COPY_POOL_TELEMETRY;
		if (copier != null) {
			try {
				copier.invokeExact(scheduler, pool, target);
				return;
			} catch (Throwable failure) {
				throw rethrow(failure);
			}
		}
		var snapshot = scheduler.poolSnapshot(pool);
		int outstanding = outstandingTasks(snapshot);
		target[RWScheduler.POOL_TELEMETRY_WORKER_COUNT] = snapshot.workerCount();
		target[RWScheduler.POOL_TELEMETRY_WAITING_WORKERS] = snapshot.waitingWorkers();
		target[RWScheduler.POOL_TELEMETRY_QUEUED_TASKS] = snapshot.queuedTasks();
		target[RWScheduler.POOL_TELEMETRY_ACTIVE_TASKS] = snapshot.activeTasks();
		target[RWScheduler.POOL_TELEMETRY_PARKED_TASKS] = parkedTasks(snapshot, outstanding);
		target[RWScheduler.POOL_TELEMETRY_OUTSTANDING_TASKS] = outstanding;
		target[RWScheduler.POOL_TELEMETRY_SUBMISSION_ATTEMPTS] = submissionAttempts(snapshot);
		target[RWScheduler.POOL_TELEMETRY_ACCEPTED_TASKS] = snapshot.acceptedTasks();
		target[RWScheduler.POOL_TELEMETRY_STARTED_TASKS] = snapshot.startedTasks();
		target[RWScheduler.POOL_TELEMETRY_COMPLETED_TASKS] = snapshot.completedTasks();
		target[RWScheduler.POOL_TELEMETRY_FAILED_TASKS] = snapshot.failedTasks();
		target[RWScheduler.POOL_TELEMETRY_TERMINAL_OUTCOMES] = terminalOutcomes(snapshot);
		target[RWScheduler.POOL_TELEMETRY_BATCH_LIMITED] = snapshot.batchDispatchLimited() ? 1L : 0L;
		target[RWScheduler.POOL_TELEMETRY_BATCH_ALLOWANCE] = snapshot.batchStartAllowance();
		for (var profile : WORKLOAD_PROFILES) {
			int index = profile.ordinal();
			target[RWScheduler.POOL_TELEMETRY_SCALARS + index] =
					snapshot.queuedByProfile().getOrDefault(profile, 0);
			target[RWScheduler.POOL_TELEMETRY_SCALARS + WORKLOAD_PROFILES.length + index] =
					snapshot.activeByProfile().getOrDefault(profile, 0);
		}
	}

	static int queued(long[] telemetry, WorkloadProfile profile) {
		return Math.toIntExact(telemetry[RWScheduler.POOL_TELEMETRY_SCALARS + profile.ordinal()]);
	}

	static int active(long[] telemetry, WorkloadProfile profile) {
		return Math.toIntExact(telemetry[RWScheduler.POOL_TELEMETRY_SCALARS
				+ WORKLOAD_PROFILES.length + profile.ordinal()]);
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

	private static MethodHandle optionalPoolTelemetryCopier() {
		try {
			return MethodHandles.lookup().findVirtual(RWScheduler.class, "copyPoolTelemetry",
					MethodType.methodType(void.class, RWScheduler.Pool.class, long[].class));
		} catch (NoSuchMethodException missingOnImmutableBaseline) {
			return null;
		} catch (IllegalAccessException inaccessible) {
			throw new ExceptionInInitializerError(inaccessible);
		}
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
