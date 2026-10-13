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
    /** One node-owned ledger shared by its physical pools; it never invokes native/property callbacks. */
    public static final class Shared {
        private final Limits limits;private final LongSupplier clock;private final IdentityHashMap<Reservation,Boolean> retained=new IdentityHashMap<>();private long bytes;private Throwable failure;
        public Shared(Limits limits,LongSupplier clock){this.limits=Objects.requireNonNull(limits);this.clock=Objects.requireNonNull(clock);}
        synchronized Reservation reserve(Sample sample,Object pool){healthy();refresh(pool,sample.estimatedBytes());long now=clock.getAsLong();for(var value:retained.keySet())if(elapsed(now,value.started)>=limits.maximumAge().toNanos())throw rejected("Node contains expired native snapshot pin",null);if(retained.size()>=limits.maximumPins() || sample.estimatedBytes()>limits.maximumEstimatedBytes()-bytes || sample.minimumFreeBytes()<limits.minimumFreeBytes())throw rejected("Node snapshot pin/byte/free-space limit exceeded",null);var value=new Reservation(pool,now,sample.estimatedBytes());retained.put(value,Boolean.TRUE);bytes+=value.bytes;return value;}
        private void refresh(Object pool,long estimate){for(var value:retained.keySet())if(value.pool==pool && estimate>value.bytes){bytes=Math.addExact(bytes,estimate-value.bytes);value.bytes=estimate;}}
        synchronized void fail(Throwable cause){if(failure==null)failure=cause;}
        private void healthy(){if(failure!=null)throw rejected("Node native snapshot release outcome unknown",failure);}
        public synchronized Stats stats(){long oldest=0,now=clock.getAsLong();for(var value:retained.keySet())oldest=Math.max(oldest,elapsed(now,value.started));return new Stats(retained.size(),bytes,Duration.ofNanos(oldest));}
        final class Reservation implements AutoCloseable {final Object pool;final long started;long bytes;Reservation(Object pool,long started,long bytes){this.pool=pool;this.started=started;this.bytes=bytes;}void check(Sample sample){synchronized(Shared.this){healthy();refresh(pool,sample.estimatedBytes());if(elapsed(clock.getAsLong(),started)>=limits.maximumAge().toNanos())throw new SnapshotRetentionExpiredException();if(sample.minimumFreeBytes()<limits.minimumFreeBytes() || Shared.this.bytes>limits.maximumEstimatedBytes())throw rejected("Node snapshot pressure exceeded",null);}}@Override public void close(){synchronized(Shared.this){if(retained.remove(this)!=null)Shared.this.bytes-=bytes;}}}
    }
    private final Limits limits;
    private final Source source;
    private final Shared shared;
    private final LongSupplier clock;
    private final IdentityHashMap<Lease, Boolean> pins = new IdentityHashMap<>();
    private long estimatedBytes;
    private Throwable releaseFailure;

    PoolSnapshotAdmission(Limits limits, Source source, LongSupplier clock) {this(limits,source,clock,null);}
    PoolSnapshotAdmission(Limits limits, Source source, LongSupplier clock,Shared shared) {
        this.shared=shared;
        this.limits = Objects.requireNonNull(limits);
        this.source = Objects.requireNonNull(source);
        this.clock = Objects.requireNonNull(clock);
    }
    Lease reserve() {
        synchronized (this) { requireHealthy(); }
        Sample sample = sample(); // Native/property and filesystem work never holds the accounting monitor.
        var global=shared==null?null:shared.reserve(sample,this);boolean accepted=false;try{synchronized (this) {
            requireHealthy();
            long now = clock.getAsLong();
            requireFresh(sample, now);
            requireAge(now);
            if (pins.size() >= limits.maximumPins() || sample.estimatedBytes() > limits.maximumEstimatedBytes() - estimatedBytes) {
                throw rejected("Pool snapshot pin or estimated byte limit exceeded", null);
            }
            requirePressure(sample, pins.size() + 1);
            var lease = new Lease(now, sample.estimatedBytes(),global);
            pins.put(lease, Boolean.TRUE);
            estimatedBytes += lease.bytes;
            accepted=true;return lease;
        }}finally{if(!accepted && global!=null)global.close();}
    }
    /** Process-only: an ambiguous release fences this pool without refunding its retained charge. */
    public synchronized Throwable releaseFailure() { return releaseFailure; }
    synchronized void failRelease(Throwable failure) { if (releaseFailure == null) releaseFailure = failure;if(shared!=null)shared.fail(failure); }
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
        private final long started, bytes;private final Shared.Reservation global;
        private Lease(long started, long bytes,Shared.Reservation global) { this.started = started; this.bytes = bytes;this.global=global; }
        void checkAge() {
            if (elapsed(clock.getAsLong(), started) >= limits.maximumAge().toNanos()) throw new SnapshotRetentionExpiredException();
        }
        void checkUse() {
            synchronized (PoolSnapshotAdmission.this) { requireHealthy(); }
            checkAge();
            Sample sample = sample();if(global!=null)global.check(sample);
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
            }if(global!=null)global.close();
        }
    }
}
