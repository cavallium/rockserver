package it.cavallium.rockserver.core.impl.rocksdb;

import org.rocksdb.Cache;
import org.rocksdb.ClockCache;
import org.rocksdb.HyperClockCache;

public class ClockCacheFactory implements CacheFactory {

	@Override
	public Cache newCache(long size) {
		return new HyperClockCache(size, 0, -1, false) {
			{
				RocksLeakDetector.register(this, "clock-cache", owningHandle_);
			}
		};
	}
}
