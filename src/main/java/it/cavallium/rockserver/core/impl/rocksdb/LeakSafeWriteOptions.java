package it.cavallium.rockserver.core.impl.rocksdb;

import org.rocksdb.WriteOptions;

public class LeakSafeWriteOptions extends WriteOptions {

	public LeakSafeWriteOptions(String label) {
		RocksLeakDetector.register(this, label, owningHandle_);
	}
}
