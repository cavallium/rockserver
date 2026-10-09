package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.distributed.SnapshotRetentionExpiredException;
import java.time.Duration;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Pool-wide native pin admission. Bytes are conservative estimates, not retained SST measurements. */
public final class PoolSnapshotAdmission {
    public record Limits(int maximumPins, long maximumEstimatedBytes, Duration maximumAge, long minimumFreeBytes) {
        public Limits {
            Objects.requireNonNull(maximumAge);
            if (maximumPins < 1 || maximumEstimatedBytes < 1 || maximumAge.isZero()
                    || maximumAge.isNegative() || minimumFreeBytes < 0) throw new IllegalArgumentException("Invalid snapshot limits");
            maximumAge.toNanos();
        }
        public static Limits defaults() { return new Limits(64, 8L << 30, Duration.ofSeconds(60), 1L << 30); }
    }
    /** Estimated bytes sum each pin's whole-pool SST and memtable estimate, including shared files repeatedly. */
    public record Stats(int pins, long estimatedBytes, Duration oldestAge) {}
    record Sample(long estimatedBytes, long minimumFreeBytes, long sampledAtNanos) {}
    @FunctionalInterface interface Source { Sample sample() throws Exception; }
    private static final long MAX_SAMPLE_AGE = Duration.ofSeconds(5).toNanos();
    private final Limits limits;
    private final Source source;
    private final LongSupplier clock;
    private final IdentityHashMap<Lease, Boolean> pins = new IdentityHashMap<>();
    private long estimatedBytes;
    private Throwable releaseFailure;

    PoolSnapshotAdmission(Limits limits, Source source, LongSupplier clock) {
        this.limits = Objects.requireNonNull(limits);
        this.source = Objects.requireNonNull(source);
        this.clock = Objects.requireNonNull(clock);
    }
    Lease reserve() {
        synchronized (this) { requireHealthy(); }
        Sample sample = sample(); // Native/property and filesystem work never holds the accounting monitor.
        synchronized (this) {
            requireHealthy();
            long now = clock.getAsLong();
            requireFresh(sample, now);
            requireAge(now);
            if (pins.size() >= limits.maximumPins() || sample.estimatedBytes() > limits.maximumEstimatedBytes() - estimatedBytes) {
                throw rejected("Pool snapshot pin or estimated byte limit exceeded", null);
            }
            requirePressure(sample, pins.size() + 1);
            var lease = new Lease(now, sample.estimatedBytes());
            pins.put(lease, Boolean.TRUE);
            estimatedBytes += lease.bytes;
            return lease;
        }
    }
    /** Process-only: an ambiguous release fences this pool without refunding its retained charge. */
    public synchronized Throwable releaseFailure() { return releaseFailure; }
    synchronized void failRelease(Throwable failure) { if (releaseFailure == null) releaseFailure = failure; }
    private void requireHealthy() {
        if (releaseFailure != null) throw rejected("Native snapshot release outcome unknown; pool retained", releaseFailure);
    }
    public synchronized Stats stats() {
        long now = clock.getAsLong(), oldest = 0;
        for (Lease lease : pins.keySet()) oldest = Math.max(oldest, elapsed(now, lease.started));
        return new Stats(pins.size(), estimatedBytes, Duration.ofNanos(oldest));
    }
    private Sample sample() {
        try {
            Sample sample = Objects.requireNonNull(source.sample());
            requireFresh(sample, clock.getAsLong());
            return sample;
        } catch (Exception failure) { throw rejected("Pool snapshot pressure metrics unavailable", failure); }
    }
    private void requireFresh(Sample sample, long now) {
        long age = now - sample.sampledAtNanos();
        if (sample.estimatedBytes() < 0 || sample.minimumFreeBytes() < 0 || age < 0 || age > MAX_SAMPLE_AGE) {
            throw rejected("Pool snapshot pressure metrics invalid or stale", null);
        }
    }
    private void requirePressure(Sample sample, int count) {
        if (sample.minimumFreeBytes() < limits.minimumFreeBytes()
                || sample.estimatedBytes() > limits.maximumEstimatedBytes() / count) {
            throw rejected("Pool snapshot estimated byte or free-space pressure exceeded", null);
        }
    }
    private void requireAge(long now) {
        for (Lease lease : pins.keySet()) if (elapsed(now, lease.started) >= limits.maximumAge().toNanos()) {
            throw rejected("Pool contains an expired native snapshot pin", null);
        }
    }
    private static long elapsed(long now, long started) { return Math.max(0, now - started); }
    private static RocksDBException rejected(String message, Throwable cause) {
        return RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED, message, cause);
    }
    final class Lease implements AutoCloseable {
        private final long started, bytes;
        private Lease(long started, long bytes) { this.started = started; this.bytes = bytes; }
        void checkAge() {
            if (elapsed(clock.getAsLong(), started) >= limits.maximumAge().toNanos()) throw new SnapshotRetentionExpiredException();
        }
        void checkUse() {
            synchronized (PoolSnapshotAdmission.this) { requireHealthy(); }
            checkAge();
            Sample sample = sample();
            synchronized (PoolSnapshotAdmission.this) {
                requireHealthy();
                checkAge();
                requireFresh(sample, clock.getAsLong());
                requirePressure(sample, Math.max(1, pins.size()));
            }
        }
        @Override public void close() {
            synchronized (PoolSnapshotAdmission.this) {
                if (pins.remove(this) != null) estimatedBytes -= bytes;
            }
        }
    }
}
