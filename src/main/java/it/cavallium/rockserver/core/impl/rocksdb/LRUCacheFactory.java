package it.cavallium.rockserver.core.impl.rocksdb;

import org.rocksdb.Cache;
import org.rocksdb.LRUCache;

public class LRUCacheFactory implements CacheFactory {

	@Override
	public Cache newCache(long size) {
		return new LRUCache(size);
	}
}
