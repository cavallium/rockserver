package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.WriteBatchIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WriteBatchFormatTest {

    @TempDir
    Path tempDir;

    @Test
    public void testFormat() throws Exception {
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, tempDir.toString())) {
             
            ColumnFamilyHandle cf = db.createColumnFamily(new ColumnFamilyDescriptor("new_cf".getBytes()));
            int cfId = cf.getID();
            
            try (WriteBatch wb = new WriteBatch()) {
                wb.put("key1".getBytes(), "val1".getBytes());
                wb.put(cf, "key2".getBytes(), "val2".getBytes());
                wb.putLogData("log".getBytes());

                byte[] data = wb.data();
                
                List<String> events = new ArrayList<>();
                WriteBatch.Handler handler = new WriteBatch.Handler() {
                    @Override
                    public void put(int columnFamilyId, byte[] key, byte[] value) {
                        events.add("PUT CF" + columnFamilyId + " " + new String(key) + "=" + new String(value));
                    }

                    @Override
                    public void put(byte[] key, byte[] value) {
                        events.add("PUT DEF " + new String(key) + "=" + new String(value));
                    }

                    @Override
                    public void merge(int columnFamilyId, byte[] key, byte[] value) {}

                    @Override
                    public void merge(byte[] key, byte[] value) {}

                    @Override
                    public void delete(int columnFamilyId, byte[] key) {}

                    @Override
                    public void delete(byte[] key) {}

                    @Override
                    public void singleDelete(int columnFamilyId, byte[] key) {}

                    @Override
                    public void singleDelete(byte[] key) {}

                    @Override
                    public void deleteRange(int columnFamilyId, byte[] beginKey, byte[] endKey) {}

                    @Override
                    public void deleteRange(byte[] beginKey, byte[] endKey) {}

                    @Override
                    public void logData(byte[] blob) {
                         events.add("LOG " + new String(blob));
                    }

                    @Override
                    public void putBlobIndex(int columnFamilyId, byte[] key, byte[] value) {}

                    @Override
                    public void markBeginPrepare() {}

                    @Override
                    public void markEndPrepare(byte[] xid) {}

                    @Override
                    public void markCommit(byte[] xid) {}

                    @Override
                    public void markRollback(byte[] xid) {}

                    @Override
                    public void markNoop(boolean emptyBatch) {}

                    @Override
                    public void markCommitWithTimestamp(byte[] xid, byte[] ts) {}
                };
                
                WriteBatchIterator.iterate(data, handler);
                
                assertEquals(3, events.size());
                assertEquals("PUT DEF key1=val1", events.get(0));
                assertEquals("PUT CF" + cfId + " key2=val2", events.get(1));
                assertEquals("LOG log", events.get(2));
            }
        }
    }
}
