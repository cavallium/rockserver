package it.cavallium.rockserver.core.impl.rocksdb;

import org.rocksdb.Cache;

/** RocksJava-compatible shared_ptr holder with explicit background-job shutdown. */
public final class SnapshotCache extends Cache {
    private boolean stopped;
    private boolean destroyed;
    private int activeCaptures;

    public SnapshotCache(NativeStartupBridge.CacheConfig config) {
        super(NativeStartupBridge.createCache(config));
    }

    /** Must run before closing the DB; cancels and joins native snapshot jobs. */
    public synchronized void stop() {
        if (!stopped && !destroyed) {
            stopped = true;
            NativeStartupBridge.stopCache(nativeHandle_);
        }
    }

    public NativeStartupBridge.SnapshotResult snapshot() {
        synchronized (this) {
            requireRunning();
            activeCaptures++;
        }
        try {
            return NativeStartupBridge.capture(nativeHandle_);
        } finally {
            synchronized (this) {
                activeCaptures--;
                notifyAll();
            }
        }
    }

    public synchronized void feedback(int mask) {
        requireRunning();
        NativeStartupBridge.cacheFeedback(nativeHandle_, mask);
    }

    public synchronized String stats() {
        requireAlive();
        return NativeStartupBridge.cacheStats(nativeHandle_);
    }

    /** Parse only during telemetry polling, never on a cache lookup. */
    public synchronized NativeStartupBridge.CacheStats statsSnapshot() {
        requireAlive();
        return NativeStartupBridge.parseCacheStats(NativeStartupBridge.cacheStats(nativeHandle_));
    }

    public String compatibilityId() { return NativeStartupBridge.cacheCompatibilityId(); }

    @Override public synchronized long getUsage() {
        requireAlive();
        return super.getUsage();
    }

    @Override public synchronized long getPinnedUsage() {
        requireAlive();
        return super.getPinnedUsage();
    }

    private void requireAlive() {
        if (destroyed) throw new IllegalStateException("Snapshot cache is closed");
    }

    private void requireRunning() {
        if (stopped || destroyed) throw new IllegalStateException("Snapshot cache is stopped");
    }

    @Override public synchronized void close() { super.close(); }

    @Override protected synchronized void disposeInternal(long handle) {
        if (!destroyed) {
            stop();
            // Native stop joins the job; retain the holder until the FFM calls return too.
            boolean interrupted = false;
            while (activeCaptures != 0) {
                try { wait(); }
                catch (InterruptedException e) { interrupted = true; }
            }
            try {
                NativeStartupBridge.destroyCache(handle);
                destroyed = true;
            } finally {
                if (interrupted) Thread.currentThread().interrupt();
            }
        }
    }
}
