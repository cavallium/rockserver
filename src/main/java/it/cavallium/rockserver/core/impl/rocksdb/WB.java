package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.RocksDBException;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.Closeable;

public final class WB implements Closeable, DBWriter {
    private static final boolean MIGRATE = Boolean.parseBoolean(System.getProperty("rocksdb.migrate", "false"));
    
    private final RocksDB rocksDB;
    private @NotNull WriteBatch wb;
    private final boolean disableWal;

    public WB(RocksDB rocksDB, @NotNull WriteBatch wb, boolean disableWal) {
        this.rocksDB = rocksDB;
        this.wb = wb;
        this.disableWal = disableWal;
    }

    public RocksDB rocksDB() {
        return rocksDB;
    }

    public @NotNull WriteBatch wb() {
        return wb;
    }

    public boolean disableWal() {
        return disableWal;
    }

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

    public void flushAndReset() throws RocksDBException {
        writePending();
        wb.close();
        wb = new WriteBatch();
    }
}
