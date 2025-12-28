package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import static org.junit.jupiter.api.Assertions.*;

class BucketedColumnIntegrationTest {

    @TempDir
    Path tempDir;

    private EmbeddedDB db;
    private long columnId;

    @BeforeEach
    void setUp() throws IOException, RocksDBException {
        db = new EmbeddedDB(tempDir, "test-db", null);
        
        // Bucketed Schema: Fixed(4 bytes) + Variable(Hash)
        var schema = ColumnSchema.of(
                IntArrayList.of(4),
                ObjectArrayList.of(ColumnHashType.XXHASH32),
                true
        );
        columnId = db.createColumn("bucketed", schema);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (db != null) db.close();
    }

    @Test
    void testGetRangeWithBuckets() throws RocksDBException {
        // 1. Insert data
        // Fixed Key: 1. Variable Keys: A, B, C (Should end up in same bucket)
        // Fixed Key: 2. Variable Keys: D, E (Same bucket)
        
        Buf fixedKey1 = Buf.wrap(new byte[]{0,0,0,1});
        Buf fixedKey2 = Buf.wrap(new byte[]{0,0,0,2});
        
        Buf varKeyA = Buf.wrap("A".getBytes());
        Buf varKeyB = Buf.wrap("B".getBytes());
        Buf varKeyC = Buf.wrap("C".getBytes());
        Buf varKeyD = Buf.wrap("D".getBytes());
        Buf varKeyE = Buf.wrap("E".getBytes());
        
        Buf val = Buf.wrap("val".getBytes());
        
        db.put(0, columnId, new Keys(new Buf[]{fixedKey1, varKeyA}), val, new RequestType.RequestNothing<>());
        db.put(0, columnId, new Keys(new Buf[]{fixedKey1, varKeyB}), val, new RequestType.RequestNothing<>());
        db.put(0, columnId, new Keys(new Buf[]{fixedKey1, varKeyC}), val, new RequestType.RequestNothing<>());
        
        db.put(0, columnId, new Keys(new Buf[]{fixedKey2, varKeyD}), val, new RequestType.RequestNothing<>());
        db.put(0, columnId, new Keys(new Buf[]{fixedKey2, varKeyE}), val, new RequestType.RequestNothing<>());

        // 2. GetRange (Forward)
        List<KV> results = db.getRange(0, columnId, null, null, false, new RequestType.RequestGetAllInRange<>(), 1000)
                .collect(Collectors.toList());
                
        // Total 5 entries
        assertEquals(5, results.size());
        
        // Verify keys (assuming insertion order or hash order - checking contents)
        // Since we verify correctness, we just check if we got 5 valid KVs.
        for (KV kv : results) {
            assertEquals(2, kv.keys().keys().length);
            assertEquals(4, kv.keys().keys()[0].size());
            // Check variable key size (should be decoded)
            // Variable key was "A" (1 byte).
            // Even though schema says XXHASH32, the bucket stores ORIGINAL key.
            // So decoded variable key size should be 1.
            assertEquals(1, kv.keys().keys()[1].size());
        }
        
        // 3. GetRange (Reverse)
        List<KV> reverseResults = db.getRange(0, columnId, null, null, true, new RequestType.RequestGetAllInRange<>(), 1000)
                .collect(Collectors.toList());
        assertEquals(5, reverseResults.size());
    }

    @Test
    void testFirstAndLast() throws RocksDBException {
        Buf fixedKey1 = Buf.wrap(new byte[]{0,0,0,1});
        Buf varKeyA = Buf.wrap("A".getBytes());
        Buf varKeyZ = Buf.wrap("Z".getBytes());
        Buf val = Buf.wrap("val".getBytes());
        
        db.put(0, columnId, new Keys(new Buf[]{fixedKey1, varKeyA}), val, new RequestType.RequestNothing<>());
        db.put(0, columnId, new Keys(new Buf[]{fixedKey1, varKeyZ}), val, new RequestType.RequestNothing<>());
        
        var result = db.reduceRange(0, columnId, null, null, false, new RequestType.RequestGetFirstAndLast<>(), 1000);
        
        assertNotNull(result.first());
        assertNotNull(result.last());
        assertNotEquals(result.first().keys(), result.last().keys());
    }

    @Test
    void testEntriesCount() throws RocksDBException {
        Buf fixedKey1 = Buf.wrap(new byte[]{0,0,0,1});
        Buf val = Buf.wrap("val".getBytes());
        
        // 5 entries in one bucket
        for (int i = 0; i < 5; i++) {
            Buf varKey = Buf.wrap(("A" + i).getBytes());
            db.put(0, columnId, new Keys(new Buf[]{fixedKey1, varKey}), val, new RequestType.RequestNothing<>());
        }
        
        // 3 entries in another bucket
        Buf fixedKey2 = Buf.wrap(new byte[]{0,0,0,2});
        for (int i = 0; i < 3; i++) {
             Buf varKey = Buf.wrap(("B" + i).getBytes());
             db.put(0, columnId, new Keys(new Buf[]{fixedKey2, varKey}), val, new RequestType.RequestNothing<>());
        }
        
        Long count = db.reduceRange(0, columnId, null, null, false, new RequestType.RequestEntriesCount<>(), 1000);
        assertEquals(8L, count);
    }
}
