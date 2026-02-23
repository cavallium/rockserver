package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType.RequestEntriesCount;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class BucketedColumnEntriesCountTest {

    @TempDir
    Path tempDir;

    private EmbeddedDB db;
    private long columnId;

    @BeforeEach
    void setUp() throws IOException, RocksDBException {
        db = new EmbeddedDB(tempDir, "test-db", null);

        // Schema: 1 Fixed Key (4 bytes int), 1 Variable Key (hashed with XXHASH32)
        // This forces buckets because of variable length keys.
        var schema = ColumnSchema.of(
            IntArrayList.of(4),
            ObjectArrayList.of(ColumnHashType.XXHASH32),
            true
        );
        columnId = db.createColumn("bucketed_col", schema);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (db != null) {
            db.closeTesting();
        }
    }

    @Test
    void testEntriesCountWithBuckets() throws RocksDBException {
        // 1. Insert data into the same bucket (same fixed key, different variable keys)
        Buf fixedKey = Buf.wrap(new byte[]{0, 0, 0, 1}); // Fixed key 1

        int entriesCount = 5;
        for (int i = 0; i < entriesCount; i++) {
            Buf varKey = Buf.wrap(("var-" + i).getBytes(StandardCharsets.UTF_8));
            Buf value = Buf.wrap(("val-" + i).getBytes(StandardCharsets.UTF_8));
            
            db.put(0, columnId, new Keys(new Buf[]{fixedKey, varKey}), value, new RequestPut.RequestNothing<>());
        }

        // 2. Insert data into another bucket (different fixed key)
        Buf fixedKey2 = Buf.wrap(new byte[]{0, 0, 0, 2}); // Fixed key 2
        for (int i = 0; i < 3; i++) {
             Buf varKey = Buf.wrap(("var-" + i).getBytes(StandardCharsets.UTF_8));
             Buf value = Buf.wrap(("val-" + i).getBytes(StandardCharsets.UTF_8));
             db.put(0, columnId, new Keys(new Buf[]{fixedKey2, varKey}), value, new RequestPut.RequestNothing<>());
        }
        
        // Total expected entries: 5 + 3 = 8

        // 3. Call reduceRange with RequestEntriesCount
        try {
            Long count = db.reduceRange(0, columnId, null, null, false, new RequestEntriesCount<>(), 1000);
            assertEquals(8L, count, "Should count total entries across buckets");
        } catch (RocksDBException e) {
            if (e.getErrorUniqueId() == RocksDBException.RocksDBErrorType.UNSUPPORTED_COLUMN_TYPE) {
                fail("RequestEntriesCount should be supported for bucketed columns");
            } else {
                throw e;
            }
        }
    }
}
