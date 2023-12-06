package it.cavallium.rockserver.core.impl.rocksdb;

import org.rocksdb.Cache;

public interface CacheFactory {

	Cache newCache(long size);
}
