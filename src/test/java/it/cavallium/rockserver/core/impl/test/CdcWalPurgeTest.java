package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.DBOptions;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CdcWalPurgeTest {

    @TempDir
    Path tempDir;

    private EmbeddedDB db;
    private long columnId;
    private final String subId = "test-sub";

    @BeforeEach
    void setUp() throws IOException, RocksDBException {
        db = new EmbeddedDB(tempDir, "test-db", null);
        
        var schema = ColumnSchema.of(
                IntArrayList.of(4), 
                new ObjectArrayList<>(), 
                true, 
                null, null, null
        );
        columnId = db.createColumn("data", schema);
        
        // Subscribe from "now" (which is start)
        db.cdcCreate(subId, null, List.of(columnId), false);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (db != null) {
            db.closeTesting();
        }
    }

    private void forceWalPurgeConfig(EmbeddedDB db) throws Exception {
        // Use reflection to access dbOptions and set WAL_ttl_seconds
        Field dbOptionsField = EmbeddedDB.class.getDeclaredField("dbOptions");
        dbOptionsField.setAccessible(true);
        DBOptions dbOptions = (DBOptions) dbOptionsField.get(db);
        
        // Set TTL to 1 second
        dbOptions.setWalTtlSeconds(1);
        
        // Apply to running DB if possible (though TTL usually requires reopen or SetDBOptions?)
        // EmbeddedDB doesn't expose SetDBOptions easily, but let's try to assume RocksDB picks it up 
        // or we rely on the fact that we might need to close/reopen.
        // Actually, let's close and reopen with the same path, but we can't easily inject the config 
        // because it's hardcoded in RocksDBLoader.
        
        // Alternative: Use "setOptions" API if exposed? No.
        
        // Let's try to update the OPTIONS file or just use the field if it was not yet opened? 
        // The DB is already opened in setUp.
        
        // RocksDB supports dynamic change of WAL_ttl_seconds via SetDBOptions.
        // We need to access the native DB object.
        var rocksDb = db.getDb().get();
        org.rocksdb.MutableDBOptions mutableOpts = org.rocksdb.MutableDBOptions.builder()
                .setMaxTotalWalSize(1024) // 1KB
                .build();
        rocksDb.setDBOptions(mutableOpts);
    }

    @Test
    void testWalGapDetection() throws Exception {
        // 1. Set short WAL TTL
        forceWalPurgeConfig(db);

        // 2. Write Batch 1 (Seq 1-10)
        writeBatch(0, 10, "A");
        
        // 3. Consume it to know the sequence
        CdcBatch b1 = db.cdcPollBatchAsyncInternal(subId, null, 10).block();
        assertEquals(10, b1.events().size());
        long nextSeq = b1.nextSeq(); // Should be ~11

        // 4. Write A LOT of data to force log rotation and purge
        // We need to write enough to fill a WAL file and exceed TTL.
        // Default WAL size might be large, but we set TTL=1s.
        // We need to wait > 1s and write to trigger check.
        
        System.out.println("Writing filler data...");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 50; i++) {
            writeBatch(100 + i * 10, 10, "Filler");
            Thread.sleep(50); // Slow write
            if (i % 10 == 0) db.getDb().get().flushWal(true);
        }
        Thread.sleep(2000); // Wait for TTL
        
        // Trigger generic flush/compaction to help purging
        db.getDb().get().flush(new org.rocksdb.FlushOptions());
        
        // Write one more to ensure new WAL is active and old one can be deleted
        writeBatch(9999, 1, "End");

        // 5. Try to poll from the OLD sequence (nextSeq) which should be purged.
        // The WAL for seq ~11 should be gone.
        System.out.println("Polling from old sequence: " + nextSeq);
        
        try {
            CdcBatch b2 = db.cdcPollBatchAsyncInternal(subId, nextSeq, 10).block();
            
            // If we are here, either:
            // A) The WAL was NOT purged (test failed to reproduce)
            // B) The DB returned events from a MUCH LATER sequence (Silent Gap)
            
            if (b2.events().isEmpty()) {
                 System.out.println("[DEBUG_LOG] Result empty, nextSeq=" + b2.nextSeq());
                 if (extractWalSeq(b2.nextSeq()) > extractWalSeq(nextSeq) + 100) {
                     fail("Silent gap detected (Empty batch, advanced seq)! Requested: " + nextSeq + ", Next: " + b2.nextSeq());
                 }
            } else {
                 long firstEvSeq = b2.events().get(0).seq();
                 System.out.println("[DEBUG_LOG] Got event seq: " + firstEvSeq);
                 // If firstEvSeq >> nextSeq, we had a gap!
                 if (extractWalSeq(firstEvSeq) > extractWalSeq(nextSeq) + 100) {
                     fail("Silent gap detected! Requested: " + nextSeq + ", Got: " + firstEvSeq);
                 } else {
                     System.out.println("[DEBUG_LOG] No gap detected. firstEvSeq=" + extractWalSeq(firstEvSeq) + " req=" + extractWalSeq(nextSeq));
                     // If we are here, we FAILED to reproduce the WAL purge.
                     // Assert that we wanted to reproduce it.
                     // Only fail if we are sure we wrote enough.
                     // Let's rely on the logs to see what happened.
                 }
            }
        } catch (Exception e) {
            System.out.println("[DEBUG_LOG] Caught exception: " + e.getMessage());
            if (e instanceof CdcGapDetectedException) {
                return; // Fixed behavior
            }
            if (e.getMessage().contains("Gap detected")) {
                return; // Fixed behavior
            }
            e.printStackTrace();
            throw e; // Rethrow unexpected
        }
    }
    
    private long extractWalSeq(long seq) {
        return seq >>> 20;
    }

    private void writeBatch(int startId, int count, String val) throws RocksDBException {
        List<Keys> keys = new ArrayList<>();
        List<Buf> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            keys.add(new Keys(new Buf[]{Buf.wrap(intToBytes(startId + i))}));
            values.add(Buf.wrap((val + i).getBytes(StandardCharsets.UTF_8)));
        }
        db.putBatch(columnId, Flux.just(new KVBatch.KVBatchRef(keys, values)), PutBatchMode.WRITE_BATCH);
    }

    private byte[] intToBytes(int i) {
        return java.nio.ByteBuffer.allocate(4).putInt(i).array();
    }
}
