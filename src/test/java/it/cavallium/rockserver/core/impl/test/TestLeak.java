package it.cavallium.rockserver.core.impl.test;

import org.rocksdb.Options;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.RocksDB;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

public class TestLeak {
    private static final int WRITES = Integer.getInteger("rockserver.testLeak.writes", 50_000);

    static {
        RocksDB.loadLibrary();
    }

    @Test
    public void test(@TempDir Path tempDir) throws Exception {
        var dbPath = tempDir.resolve("test_db").toString();
        try (Options opt = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opt, dbPath);
             WriteOptions wopt = new WriteOptions()) {

            byte[] key = new byte[10];
            byte[] val = new byte[10];
            for (int i = 0; i < WRITES; i++) {
                try (WriteBatch wb = new WriteBatch()) {
                    wb.put(key, val);
                    db.write(wopt, wb);
                }
            }
        }
    }
}
