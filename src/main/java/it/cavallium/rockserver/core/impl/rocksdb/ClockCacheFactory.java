package it.cavallium.rockserver.core.impl.rocksdb;

import org.rocksdb.Cache;
import org.rocksdb.ClockCache;
import org.rocksdb.HyperClockCache;

public class ClockCacheFactory implements CacheFactory {

	@Override
	public Cache newCache(long size, double highPriorityPoolRatio) {
		if (highPriorityPoolRatio != 0.0d) {
			throw new IllegalArgumentException("HyperClockCache does not support a high-priority pool ratio");
		}
		return new HyperClockCache(size, 0, -1, false) {
			{
				RocksLeakDetector.register(this, "clock-cache", owningHandle_);
			}
		};
	}

	@Override
	public boolean supportsHighPriorityPool() {
		return false;
	}
}
