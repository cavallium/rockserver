package it.cavallium.rockserver.core.common;

import java.time.Duration;
import java.util.Objects;

/** Exact conversion for transaction and iterator lease lifetimes. */
public final class LeaseTtl {
	private LeaseTtl() {}

	public static long toNanos(Duration leaseTtl, String name) {
		Objects.requireNonNull(leaseTtl, name);
		if (leaseTtl.isZero() || leaseTtl.isNegative()) {
			throw new IllegalArgumentException(name + " must be positive");
		}
		try {
			return leaseTtl.toNanos();
		} catch (ArithmeticException overflow) {
			return Long.MAX_VALUE;
		}
	}

	public static long toMillisCeil(Duration leaseTtl, String name) {
		long nanos = toNanos(leaseTtl, name);
		if (nanos == Long.MAX_VALUE) return Long.MAX_VALUE;
		long millis = nanos / 1_000_000L;
		return nanos % 1_000_000L == 0L ? millis : millis + 1L;
	}
}
