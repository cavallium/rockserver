package it.cavallium.rockserver.core.impl.rocksdb;

public sealed interface DBWriter permits SSTWriter, Tx, WB {
    /**
     * Writes any pending kv pair to the db
     */
    void writePending() throws it.cavallium.rockserver.core.common.RocksDBException;
}
