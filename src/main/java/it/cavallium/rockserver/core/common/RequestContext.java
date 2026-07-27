package it.cavallium.rockserver.core.common;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Mandatory caller-supplied workload contract for generic Rockserver operations.
 *
 * <p>The profile selects a service guarantee. Rockserver separately derives the
 * operation family and rejects incompatible combinations. Latency work must carry a
 * finite absolute deadline so the admission queue can use earliest-deadline-first
 * ordering; other client profiles may use {@link #NO_DEADLINE}.</p>
 *
 * @param profile client-selectable service profile
 * @param deadlineEpochMillis absolute Unix-epoch deadline in milliseconds, or
 *                            {@link #NO_DEADLINE}
 */
public record RequestContext(WorkloadProfile profile, long deadlineEpochMillis) {

	public static final long NO_DEADLINE = Long.MAX_VALUE;

	public RequestContext {
		Objects.requireNonNull(profile, "profile");
		if (!profile.isClientSelectable()) {
			throw new IllegalArgumentException("Profile " + profile + " is owned by Rockserver");
		}
		if (deadlineEpochMillis <= 0L) {
			throw new IllegalArgumentException("deadlineEpochMillis must be positive");
		}
		if (profile == WorkloadProfile.LATENCY && deadlineEpochMillis == NO_DEADLINE) {
			throw new IllegalArgumentException("LATENCY requires a finite deadline");
		}
	}

	public static RequestContext latency(Instant deadline) {
		Objects.requireNonNull(deadline, "deadline");
		return new RequestContext(WorkloadProfile.LATENCY, deadline.toEpochMilli());
	}

	public static RequestContext latency(Duration timeout) {
		return latency(timeout, Clock.systemUTC());
	}

	static RequestContext latency(Duration timeout, Clock clock) {
		Objects.requireNonNull(timeout, "timeout");
		Objects.requireNonNull(clock, "clock");
		if (timeout.isNegative() || timeout.isZero()) {
			throw new IllegalArgumentException("LATENCY timeout must be positive");
		}
		long now = clock.millis();
		long timeoutMillis;
		try {
			timeoutMillis = timeout.toMillis();
		} catch (ArithmeticException overflow) {
			timeoutMillis = Long.MAX_VALUE;
		}
		if (timeoutMillis <= 0L) {
			timeoutMillis = 1L;
		}
		long deadline = timeoutMillis >= Long.MAX_VALUE - now
				? Long.MAX_VALUE - 1L
				: now + timeoutMillis;
		return new RequestContext(WorkloadProfile.LATENCY, deadline);
	}

	public static RequestContext analytical() {
		return new RequestContext(WorkloadProfile.ANALYTICAL, NO_DEADLINE);
	}

	public static RequestContext analytical(Instant deadline) {
		return withDeadline(WorkloadProfile.ANALYTICAL, deadline);
	}

	public static RequestContext ingest() {
		return new RequestContext(WorkloadProfile.INGEST, NO_DEADLINE);
	}

	public static RequestContext ingest(Instant deadline) {
		return withDeadline(WorkloadProfile.INGEST, deadline);
	}

	public static RequestContext batch() {
		return new RequestContext(WorkloadProfile.BATCH, NO_DEADLINE);
	}

	public static RequestContext batch(Instant deadline) {
		return withDeadline(WorkloadProfile.BATCH, deadline);
	}

	private static RequestContext withDeadline(WorkloadProfile profile, Instant deadline) {
		Objects.requireNonNull(deadline, "deadline");
		return new RequestContext(profile, deadline.toEpochMilli());
	}

	/** True when the absolute request deadline has been reached. */
	public boolean isExpired(Clock clock) {
		Objects.requireNonNull(clock, "clock");
		return deadlineEpochMillis != NO_DEADLINE && clock.millis() >= deadlineEpochMillis;
	}
}
