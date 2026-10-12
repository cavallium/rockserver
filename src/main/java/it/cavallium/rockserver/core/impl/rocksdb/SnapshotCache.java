package it.cavallium.rockserver.core.impl.rocksdb;

import org.rocksdb.Cache;

/** RocksJava-compatible shared_ptr holder with explicit background-job shutdown. */
public final class SnapshotCache extends Cache {
    private boolean stopped;
    private boolean destroyed;

    public SnapshotCache(NativeStartupBridge.CacheConfig config) {
        super(NativeStartupBridge.createCache(config));
    }

    /** Must run before closing the DB; cancels and joins native snapshot jobs. */
    public synchronized void stop() {
        if (!stopped && !destroyed) {
            NativeStartupBridge.stopCache(nativeHandle_);
            stopped = true;
        }
    }

    public synchronized NativeStartupBridge.SnapshotResult snapshot() {
        requireRunning();
        return NativeStartupBridge.capture(nativeHandle_);
    }

    public synchronized void feedback(int mask) {
        requireRunning();
        NativeStartupBridge.cacheFeedback(nativeHandle_, mask);
    }

    public synchronized String stats() {
        requireAlive();
        return NativeStartupBridge.cacheStats(nativeHandle_);
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
            NativeStartupBridge.destroyCache(handle);
            destroyed = true;
        }
    }
}
