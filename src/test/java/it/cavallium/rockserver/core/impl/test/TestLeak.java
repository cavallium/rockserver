package it.cavallium.rockserver.core.impl.test;

import org.rocksdb.Options;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.RocksDB;
import org.junit.jupiter.api.Test;
import java.io.File;

public class TestLeak {
    static {
        RocksDB.loadLibrary();
    }
    @Test
    public void test() throws Exception {
        File f = new File("test_db");
        if (!f.exists()) f.mkdirs();
        try (Options opt = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(opt, "test_db");
             WriteOptions wopt = new WriteOptions()) {
            
            byte[] key = new byte[10];
            byte[] val = new byte[10];
            for (int i = 0; i < 2000000; i++) {
                try (WriteBatch wb = new WriteBatch()) {
                    wb.put(key, val);
                    db.write(wopt, wb);
                }
            }
        }
    }
}
