package it.cavallium.rockserver.core.common;

import java.time.Duration;
import java.util.Objects;

/**
 * Mandatory caller-supplied workload contract for generic Rockserver operations.
 *
 * <p>The profile selects a service guarantee. Rockserver separately derives the
 * operation family and rejects incompatible combinations. The timeout is a reusable
 * policy: every public operation binds it once to a local monotonic deadline before
 * client-side queueing. Latency work must carry a finite timeout; other client profiles
 * may use {@link #NO_TIMEOUT}.</p>
 *
 * @param profile client-selectable service profile
 * @param timeoutNanos positive relative timeout in nanoseconds, or {@link #NO_TIMEOUT}
 */
public record RequestContext(WorkloadProfile profile, long timeoutNanos) {

	public static final long NO_TIMEOUT = Long.MAX_VALUE;
	private static final long MAX_FINITE_TIMEOUT_NANOS = Long.MAX_VALUE - 1L;
	private static final RequestContext ANALYTICAL_NO_TIMEOUT =
			new RequestContext(WorkloadProfile.ANALYTICAL, NO_TIMEOUT);
	private static final RequestContext INGEST_NO_TIMEOUT =
			new RequestContext(WorkloadProfile.INGEST, NO_TIMEOUT);
	private static final RequestContext BATCH_NO_TIMEOUT =
			new RequestContext(WorkloadProfile.BATCH, NO_TIMEOUT);

	public RequestContext {
		Objects.requireNonNull(profile, "profile");
		if (!profile.isClientSelectable()) {
			throw new IllegalArgumentException("Profile " + profile + " is owned by Rockserver");
		}
		if (timeoutNanos <= 0L) {
			throw new IllegalArgumentException("timeoutNanos must be positive");
		}
		if (profile == WorkloadProfile.LATENCY && timeoutNanos == NO_TIMEOUT) {
			throw new IllegalArgumentException("LATENCY requires a finite timeout");
		}
	}

	public static RequestContext latency(Duration timeout) {
		return withTimeout(WorkloadProfile.LATENCY, timeout);
	}

	public static RequestContext analytical() {
		return ANALYTICAL_NO_TIMEOUT;
	}

	public static RequestContext analytical(Duration timeout) {
		return withTimeout(WorkloadProfile.ANALYTICAL, timeout);
	}

	public static RequestContext ingest() {
		return INGEST_NO_TIMEOUT;
	}

	public static RequestContext ingest(Duration timeout) {
		return withTimeout(WorkloadProfile.INGEST, timeout);
	}

	public static RequestContext batch() {
		return BATCH_NO_TIMEOUT;
	}

	public static RequestContext batch(Duration timeout) {
		return withTimeout(WorkloadProfile.BATCH, timeout);
	}

	private static RequestContext withTimeout(WorkloadProfile profile, Duration timeout) {
		Objects.requireNonNull(timeout, "timeout");
		if (timeout.isNegative() || timeout.isZero()) {
			throw new IllegalArgumentException(profile + " timeout must be positive");
		}
		long timeoutNanos;
		try {
			timeoutNanos = timeout.toNanos();
		} catch (ArithmeticException overflow) {
			timeoutNanos = MAX_FINITE_TIMEOUT_NANOS;
		}
		return new RequestContext(profile, Math.min(timeoutNanos, MAX_FINITE_TIMEOUT_NANOS));
	}

	public boolean hasTimeout() {
		return timeoutNanos != NO_TIMEOUT;
	}
}
