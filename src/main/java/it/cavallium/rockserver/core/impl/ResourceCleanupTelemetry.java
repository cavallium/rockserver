package it.cavallium.rockserver.core.impl;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** Cached observations of the existing deadline cleaner, never scans resources on a metrics scrape. */
final class ResourceCleanupTelemetry {

	private final AtomicLong failures = new AtomicLong();
	private final LongSupplier nanoClock;
	private volatile Sample sample;

	ResourceCleanupTelemetry() {
		this(System::nanoTime);
	}

	ResourceCleanupTelemetry(LongSupplier nanoClock) {
		this.nanoClock = nanoClock;
	}

	void register(MeterRegistry registry, List<Meter> meters, String database, String resource) {
		meters.add(Gauge.builder("rockserver.resource.overdue.count", this, ResourceCleanupTelemetry::overdueCount)
				.description("Registered expired resources still retained after deadline cleanup; not all native resources")
				.tags("db", database, "resource", resource).register(registry));
		meters.add(Gauge.builder("rockserver.resource.oldest.overdue.seconds", this, ResourceCleanupTelemetry::oldestOverdueSeconds)
				.description("Largest deadline overrun of entries remaining after cleanup, measured at that scan")
				.baseUnit("seconds").tags("db", database, "resource", resource).register(registry));
		meters.add(Gauge.builder("rockserver.resource.cleanup.scan.age.seconds", this, ResourceCleanupTelemetry::scanAgeSeconds)
				.description("Monotonic age of the last completed deadline cleanup scan; NaN before its first scan")
				.baseUnit("seconds").tags("db", database, "resource", resource).register(registry));
		meters.add(FunctionCounter.builder("rockserver.resource.cleanup.failures", failures, AtomicLong::get)
				.description("Resource cleanup failures: expired transaction/iterator closes and physical range cleanup, excluding range observers/timers")
				.tags("db", database, "resource", resource).register(registry));
	}

	void closeFailed() {
		failures.incrementAndGet();
	}

	void record(Scan scan) {
		sample = new Sample(scan.count, scan.oldestOverdueMillis, nanoClock.getAsLong());
	}

	double overdueCount() {
		var current = sample;
		return current == null ? Double.NaN : current.count;
	}

	double oldestOverdueSeconds() {
		var current = sample;
		return current == null ? Double.NaN : current.oldestOverdueMillis / 1_000.0;
	}

	double scanAgeSeconds() {
		var current = sample;
		return current == null ? Double.NaN : Math.max(0L, nanoClock.getAsLong() - current.sampledNanos) / 1_000_000_000.0;
	}

	/** One bounded accumulator per maintenance scan; no extra state per admitted resource. */
	static final class Scan {
		private final long nowMillis;
		private long count;
		private long oldestOverdueMillis;

		Scan(long nowMillis) {
			this.nowMillis = nowMillis;
		}

		void deadline(long deadlineMillis) {
			if (deadlineMillis <= nowMillis) {
				long overdue = nowMillis - deadlineMillis;
				overdueMillis(overdue < 0L ? Long.MAX_VALUE : overdue);
			}
		}

		void overdueNanos(long overdueNanos) {
			if (overdueNanos >= 0L) {
				overdueMillis(overdueNanos / 1_000_000L);
			}
		}

		private void overdueMillis(long overdueMillis) {
			count++;
			oldestOverdueMillis = Math.max(oldestOverdueMillis, overdueMillis);
		}
	}

	private record Sample(long count, long oldestOverdueMillis, long sampledNanos) {
	}
}
