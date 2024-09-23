package it.cavallium.rockserver.core.impl.rocksdb;

import java.io.Closeable;

import org.rocksdb.Transaction;

public record Tx(Transaction val, boolean isFromGetForUpdate, RocksDBObjects objs)
		implements Closeable, DBWriter {

	@Override
	public void close() {
		val.close();
		objs.close();
	}
}
