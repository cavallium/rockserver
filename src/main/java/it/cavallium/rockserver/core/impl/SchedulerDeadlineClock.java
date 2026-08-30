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

	private SchedulerDeadlineClock(LongSupplier epochMillisSource, LongSupplier nanoTimeSource) {
		this.epochMillisSource = Objects.requireNonNull(epochMillisSource, "epochMillisSource");
		this.nanoTimeSource = Objects.requireNonNull(nanoTimeSource, "nanoTimeSource");
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

	long nanoTime() {
		return nanoTimeSource.getAsLong();
	}
}
