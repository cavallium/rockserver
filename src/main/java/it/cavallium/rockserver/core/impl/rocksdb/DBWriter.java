package it.cavallium.rockserver.core.impl.rocksdb;

public sealed interface DBWriter extends AutoCloseable permits SSTWriter, Tx, WB {

	@Override
	void close();

	/**
	 * Writes any pending kv pair to the db
     */
    void writePending() throws it.cavallium.rockserver.core.common.RocksDBException;
}
