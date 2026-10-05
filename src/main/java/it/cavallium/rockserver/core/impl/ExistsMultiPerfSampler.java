package it.cavallium.rockserver.core.impl;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.rocksdb.PerfContext;
import org.rocksdb.PerfLevel;
import org.rocksdb.RocksDB;

/** Internal, bounded LATENCY status-only diagnostics. Caller-thread counters may omit work performed off thread;
 * block-read timing is partial and bytes are not physical disk bytes. Time-cadence sampling is not a census. */
public final class ExistsMultiPerfSampler {
	static final String PROPERTY = "rockserver.exists-perf.";
	private static final ThreadLocal<Boolean> ACTIVE = new ThreadLocal<>();
	private final long intervalNanos;
	private final long windowNanos;
	private final long started;
	private final AtomicLong next = new AtomicLong();
	private final AtomicLong remaining;
	private final LongSupplier clock;
	private final Consumer<Observation> observer;
	private final Runnable failure;

	public interface Access {
		PerfLevel level();
		void level(PerfLevel level);
		Counters counters();
	}

	public record Counters(long reads, long bytes, long readNanos, long hits, long checksumNanos, long decompressNanos) {
		Counters minus(Counters before) {
			return new Counters(reads - before.reads, bytes - before.bytes, readNanos - before.readNanos,
					hits - before.hits, checksumNanos - before.checksumNanos, decompressNanos - before.decompressNanos);
		}
	}
	public record Observation(Counters counters, long nativeNanos) {}
	public record Sample(Access access, PerfLevel prior, Counters before, long nativeStarted) {}

	public ExistsMultiPerfSampler(long intervalNanos, long windowNanos, long budget, LongSupplier clock,
			Consumer<Observation> observer, Runnable failure) {
		this(intervalNanos, windowNanos, budget, clock, clock.getAsLong(), observer, failure);
	}

	public ExistsMultiPerfSampler(long intervalNanos, long windowNanos, long budget, LongSupplier clock,
			long startNanos, Consumer<Observation> observer, Runnable failure) {
		this.intervalNanos = intervalNanos;
		this.windowNanos = windowNanos;
		this.remaining = new AtomicLong(budget);
		this.clock = clock;
		this.started = startNanos;
		this.observer = observer;
		this.failure = failure;
	}

	static ExistsMultiPerfSampler configured(MeterRegistry registry, String db) {
		// Explicitly opt in; hard caps prevent a forgotten setting becoming continuous profiling.
		try {
			long interval = Long.getLong(PROPERTY + "interval-ms", 0L);
			long window = Long.getLong(PROPERTY + "window-ms", 0L);
			long budget = Long.getLong(PROPERTY + "budget", 0L);
			if (interval < 10 || interval > 300_000 || window <= 0 || window > 300_000 || budget <= 0 || budget > 1000) {
				return disabled();
			}
			long startNanos = System.nanoTime();
			String notBefore = System.getProperty(PROPERTY + "not-before-epoch-millis");
			if (notBefore != null) {
				long offsetMillis = Math.subtractExact(Long.parseLong(notBefore), System.currentTimeMillis());
				if (offsetMillis <= -window) return disabled();
				startNanos = Math.addExact(startNanos, Math.multiplyExact(offsetMillis, 1_000_000L));
			}
			String[] names = {"samples", "block.read.count", "block.read.bytes", "block.read.nanoseconds",
					"block.cache.hits", "block.checksum.nanoseconds", "block.decompress.nanoseconds", "native.nanoseconds"};
			Counter[] counters = new Counter[names.length];
			for (int i = 0; i < names.length; i++) {
				counters[i] = registry.counter("rockserver.exists.perf.caller." + names[i], "db", db, "path", "status_only");
			}
			Counter failures = registry.counter("rockserver.exists.perf.caller.failures", "db", db, "path", "status_only");
			return new ExistsMultiPerfSampler(interval * 1_000_000L, window * 1_000_000L, budget,
					System::nanoTime, startNanos, value -> {
						var c = value.counters();
						counters[0].increment();
						counters[1].increment(c.reads()); counters[2].increment(c.bytes());
						counters[3].increment(c.readNanos()); counters[4].increment(c.hits());
						counters[5].increment(c.checksumNanos()); counters[6].increment(c.decompressNanos());
						counters[7].increment(value.nativeNanos());
					}, failures::increment);
		} catch (Throwable ignored) {
			return disabled();
		}
	}

	static ExistsMultiPerfSampler disabled() {
		return new ExistsMultiPerfSampler(0, 0, 0, System::nanoTime, ignored -> {}, () -> {});
	}

	public Sample begin(RocksDB db) {
		try {
			if (!reserve()) return null;
			return capture(new Access() {
				private PerfContext context;
				public PerfLevel level() { return db.getPerfLevel(); }
				public void level(PerfLevel level) { db.setPerfLevel(level); }
				public Counters counters() {
					if (context == null) context = db.getPerfContext(); // Borrowed TLS; never reset or transfer.
					return new Counters(context.getBlockReadCount(), context.getBlockReadByte(), context.getBlockReadTime(),
							context.getBlockCacheHitCount(), context.getBlockChecksumTime(), context.getBlockDecompressTime());
				}
			});
		} catch (Throwable ignored) { failed(); return null; }
	}

	public Sample begin(Access access) {
		try { return reserve() ? capture(access) : null; }
		catch (Throwable ignored) { failed(); return null; }
	}

	private boolean reserve() {
		if (remaining.get() <= 0 || ACTIVE.get() != null) return false;
		long elapsed = clock.getAsLong() - started;
		if (elapsed < 0 || elapsed >= windowNanos) return false;
		long due = next.get();
		if (elapsed < due || !next.compareAndSet(due, elapsed + intervalNanos)) return false;
		return remaining.getAndUpdate(value -> value > 0 ? value - 1 : 0) > 0;
	}

	private Sample capture(Access access) {
		PerfLevel prior = null;
		try {
			ACTIVE.set(Boolean.TRUE);
			prior = access.level();
			if (prior == PerfLevel.UNINITIALIZED || prior == PerfLevel.OUT_OF_BOUNDS) {
				ACTIVE.remove();
				return null;
			}
			if (prior.getValue() < PerfLevel.ENABLE_TIME_EXCEPT_FOR_MUTEX.getValue()) {
				access.level(PerfLevel.ENABLE_TIME_EXCEPT_FOR_MUTEX);
			}
			return new Sample(access, prior, access.counters(), clock.getAsLong());
		} catch (Throwable ignored) {
			restore(access, prior);
			clearActive();
			failed();
			return null;
		}
	}

	/** Finish on the native caller thread, before publishing any diagnostic observation. */
	public void finish(Sample sample) {
		Observation observation = null;
		boolean diagnosticFailed = false;
		try {
			long elapsed = clock.getAsLong() - sample.nativeStarted();
			var delta = sample.access().counters().minus(sample.before());
			if (elapsed < 0 || delta.reads() < 0 || delta.bytes() < 0 || delta.readNanos() < 0
					|| delta.hits() < 0 || delta.checksumNanos() < 0 || delta.decompressNanos() < 0) {
				diagnosticFailed = true; // Another TLS user reset counters; this sample is invalid.
			} else {
				observation = new Observation(delta, elapsed);
			}
		} catch (Throwable ignored) { diagnosticFailed = true; }
		finally {
			if (!restore(sample.access(), sample.prior())) diagnosticFailed = true;
			clearActive();
		}
		if (diagnosticFailed) failed();
		if (observation != null) {
			try { observer.accept(observation); } catch (Throwable ignored) { failed(); }
		}
	}

	private boolean restore(Access access, PerfLevel prior) {
		if (prior != null && prior.getValue() > PerfLevel.UNINITIALIZED.getValue()
				&& prior.getValue() < PerfLevel.ENABLE_TIME_EXCEPT_FOR_MUTEX.getValue()) {
			try { access.level(prior); } catch (Throwable ignored) { return false; }
		}
		return true;
	}
	private static void clearActive() { try { ACTIVE.remove(); } catch (Throwable ignored) {} }
	private void failed() { try { failure.run(); } catch (Throwable ignored) {} }
}
