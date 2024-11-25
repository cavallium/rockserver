package it.cavallium.rockserver.core.impl.rocksdb;

import java.io.Closeable;

import it.cavallium.rockserver.core.common.RocksDBException;
import org.rocksdb.Transaction;

public record Tx(Transaction val, boolean isFromGetForUpdate, long expirationTimestamp, RocksDBObjects objs)
		implements Closeable, DBWriter {

	@Override
	public void close() {
		val.close();
		objs.close();
	}

	@Override
	public void writePending() throws RocksDBException {

	}
}
