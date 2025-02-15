package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.RocksDBException;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.Closeable;

public record WB(RocksDB rocksDB, @NotNull WriteBatch wb, boolean disableWal) implements Closeable, DBWriter {
    private static final boolean MIGRATE = Boolean.parseBoolean(System.getProperty("rocksdb.migrate", "false"));
    @Override
    public void close() {
        wb.close();
    }

    public void writePending() throws RocksDBException {
        try (var w = new LeakSafeWriteOptions(null)) {
            if (disableWal || MIGRATE) {
                w.setDisableWAL(true);
            }
            rocksDB.write(w, wb);
        } catch (org.rocksdb.RocksDBException e) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.WRITE_BATCH_1, e);
        }
    }
}
