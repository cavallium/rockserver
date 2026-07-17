package it.cavallium.rockserver.core.impl.rocksdb;

import org.rocksdb.Cache;
import org.rocksdb.LRUCache;

public class LRUCacheFactory implements CacheFactory {

	@Override
	public Cache newCache(long size, double highPriorityPoolRatio) {
		// RocksDB only gives index/filter/compression-dictionary blocks protected
		// capacity when this ratio is non-zero. strictCapacityLimit stays false:
		// rejecting cache insertions can make reads fail while iterators pin blocks.
		return new LRUCache(size, -1, false, highPriorityPoolRatio) {
			{
				RocksLeakDetector.register(this, "lru-cache", owningHandle_);
			}
		};
	}
}
