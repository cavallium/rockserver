package it.cavallium.rockserver.core.impl.rocksdb;

import org.rocksdb.WriteBatch;

public class LeakSafeWriteBatch extends WriteBatch {

	public LeakSafeWriteBatch() {
		super();
		RocksLeakDetector.register(this, "write-batch", owningHandle_);
	}
}
