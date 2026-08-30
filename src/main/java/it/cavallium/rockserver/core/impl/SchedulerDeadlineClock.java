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

	private final LongSupplier epochMillisSource;
	private final LongSupplier nanoTimeSource;
	private final long nanoOrigin;
	private volatile EpochAnchor epochAnchor;

	private SchedulerDeadlineClock(LongSupplier epochMillisSource, LongSupplier nanoTimeSource) {
		this.epochMillisSource = Objects.requireNonNull(epochMillisSource, "epochMillisSource");
		this.nanoTimeSource = Objects.requireNonNull(nanoTimeSource, "nanoTimeSource");
		this.nanoOrigin = nanoTimeSource.getAsLong();
		this.epochAnchor = new EpochAnchor(epochMillisSource.getAsLong(), 0L);
	}

	static SchedulerDeadlineClock system() {
		return new SchedulerDeadlineClock(System::currentTimeMillis, System::nanoTime);
	}

	static SchedulerDeadlineClock testing(LongSupplier epochMillisSource, LongSupplier nanoTimeSource) {
		return new SchedulerDeadlineClock(epochMillisSource, nanoTimeSource);
	}

	long epochMillis() {
		return epochMillisSource.getAsLong();
	}

	/** Monotonic elapsed nanoseconds since this scheduler clock was created. */
	long monotonicNanos() {
		long elapsed = nanoTimeSource.getAsLong() - nanoOrigin;
		// nanoTime subtraction is valid across signed wrap for every representable interval below
		// 2^63 ns. A regressing/beyond-lifetime source fails closed instead of granting runtime.
		return elapsed < 0L ? Long.MAX_VALUE : elapsed;
	}

	/**
	 * Bind one external absolute deadline to the monotonic budget visible at admission.
	 * The returned value is immutable and safe to compare with {@link #monotonicNanos()}.
	 */
	long monotonicDeadlineNanos(long deadlineEpochMillis) {
		long nowNanos = monotonicNanos();
		long nowEpochMillis = epochMillis();
		if (deadlineEpochMillis <= nowEpochMillis) {
			return nowNanos;
		}
		EpochAnchor anchor = epochAnchor(nowEpochMillis, nowNanos);
		long remainingMillis = deadlineEpochMillis - anchor.epochMillis;
		if (remainingMillis < 0L) {
			remainingMillis = Long.MAX_VALUE;
		}
		long remainingNanos = remainingMillis >= Long.MAX_VALUE / 1_000_000L
				? Long.MAX_VALUE
				: remainingMillis * 1_000_000L;
		long mapped = deadlineAfterNanos(anchor.monotonicNanos, remainingNanos);
		return Math.max(nowNanos, mapped);
	}

	private EpochAnchor epochAnchor(long nowEpochMillis, long nowNanos) {
		EpochAnchor observed = epochAnchor;
		if (!wallOffsetChanged(observed, nowEpochMillis, nowNanos)) {
			return observed;
		}
		synchronized (this) {
			observed = epochAnchor;
			if (wallOffsetChanged(observed, nowEpochMillis, nowNanos)) {
				observed = new EpochAnchor(nowEpochMillis, nowNanos);
				epochAnchor = observed;
			}
			return observed;
		}
	}

	private static boolean wallOffsetChanged(EpochAnchor anchor, long nowEpochMillis, long nowNanos) {
		long elapsedNanos = nowNanos - anchor.monotonicNanos;
		if (elapsedNanos < 0L) return true;
		long elapsedMillis = elapsedNanos / 1_000_000L;
		long projectedEpochMillis = anchor.epochMillis > Long.MAX_VALUE - elapsedMillis
				? Long.MAX_VALUE
				: anchor.epochMillis + elapsedMillis;
		long difference = nowEpochMillis - projectedEpochMillis;
		if (((nowEpochMillis ^ projectedEpochMillis) & (nowEpochMillis ^ difference)) < 0L) {
			return true;
		}
		return difference < -1L || difference > 1L;
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

	private record EpochAnchor(long epochMillis, long monotonicNanos) {
	}
}
