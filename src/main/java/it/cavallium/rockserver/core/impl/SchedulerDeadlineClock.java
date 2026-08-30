package it.cavallium.rockserver.core.impl;

import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Scheduler deadline time source.
 *
 * <p>The production instance calls the JVM clocks directly. Tests may supply deterministic
 * wall and monotonic clocks without changing the scheduler's synchronization or worker model.</p>
 */
final class SchedulerDeadlineClock {

	private final LongSupplier nanoTimeSource;
	private final long nanoOrigin;

	private SchedulerDeadlineClock(LongSupplier nanoTimeSource) {
		this.nanoTimeSource = Objects.requireNonNull(nanoTimeSource, "nanoTimeSource");
		this.nanoOrigin = nanoTimeSource.getAsLong();
	}

	static SchedulerDeadlineClock system() {
		return new SchedulerDeadlineClock(System::nanoTime);
	}

	static SchedulerDeadlineClock testing(LongSupplier nanoTimeSource) {
		return new SchedulerDeadlineClock(nanoTimeSource);
	}

	/** Monotonic elapsed nanoseconds since this scheduler clock was created. */
	long monotonicNanos() {
		long elapsed = nanoTimeSource.getAsLong() - nanoOrigin;
		// nanoTime subtraction is valid across signed wrap for every representable interval below
		// 2^63 ns. A regressing/beyond-lifetime source fails closed instead of granting runtime.
		return elapsed < 0L ? Long.MAX_VALUE : elapsed;
	}

	long monotonicDeadlineAfterNanos(long remainingNanos) {
		return deadlineAfterNanos(monotonicNanos(), Math.max(0L, remainingNanos));
	}

	private static long deadlineAfterNanos(long nowNanos, long remainingNanos) {
		return remainingNanos >= Long.MAX_VALUE - nowNanos
				? Long.MAX_VALUE
				: nowNanos + remainingNanos;
	}

	long remainingNanos(long monotonicDeadlineNanos) {
		long nowNanos = monotonicNanos();
		return monotonicDeadlineNanos <= nowNanos ? 0L : monotonicDeadlineNanos - nowNanos;
	}
}
