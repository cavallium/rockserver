package it.cavallium.rockserver.core.impl.rocksdb;

import org.rocksdb.Cache;
import org.rocksdb.ClockCache;

public class ClockCacheFactory implements CacheFactory {

	@Override
	public Cache newCache(long size) {
		return new ClockCache(size) {
			{
				RocksLeakDetector.register(this, owningHandle_);
			}
		};
	}
}
