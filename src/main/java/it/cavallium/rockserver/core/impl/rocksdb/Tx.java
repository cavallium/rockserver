package it.cavallium.rockserver.core.impl.rocksdb;

import java.io.Closeable;
import org.rocksdb.AbstractNativeReference;
import org.rocksdb.Transaction;

public record Tx(Transaction val, boolean isFromGetForUpdate, RocksDBObjects objs)
		implements Closeable {

	@Override
	public void close() {
		val.close();
		objs.close();
	}
}
