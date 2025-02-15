package it.cavallium.rockserver.core.impl.rocksdb;

import org.rocksdb.ReadOptions;

public class LeakSafeReadOptions extends ReadOptions {

	public LeakSafeReadOptions(String label) {
		super();
		RocksLeakDetector.register(this, label, owningHandle_);
	}
}
