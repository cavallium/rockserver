package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CdcConsistencyTest {

    @TempDir
    Path tempDir;

    private EmbeddedDB db;
    private long defaultColId;
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
        defaultColId = db.createColumn("data", schema);
        db.cdcCreate(subId, null, List.of(defaultColId), false);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (db != null) {
            db.closeTesting();
        }
    }

    private byte[] intToBytes(int i) {
        return java.nio.ByteBuffer.allocate(4).putInt(i).array();
    }
    
    private int bytesToInt(byte[] b) {
        return java.nio.ByteBuffer.wrap(b).getInt();
    }

    @Test
    void testTransactionVisibility() throws Exception {
        // 1. Transaction Commit
        long txId1 = db.openTransaction(5000);
        db.put(txId1, defaultColId, new Keys(new Buf[]{Buf.wrap(intToBytes(1))}), Buf.wrap("Committed".getBytes()), RequestType.none());
        
        // Should not see it yet
        CdcBatch batch = db.cdcPollBatchAsyncInternal(subId, null, 100).block();
        assertTrue(batch.events().isEmpty(), "Uncommitted transaction should not be visible");
        
        db.closeTransaction(txId1, true); // Commit
        
        // Should see it now
        batch = db.cdcPollBatchAsyncInternal(subId, null, 100).block();
        assertEquals(1, batch.events().size());
        assertEquals("Committed", new String(batch.events().get(0).value().toByteArray()));
        long nextSeq = batch.nextSeq();

        // 2. Transaction Rollback
        long txId2 = db.openTransaction(5000);
        db.put(txId2, defaultColId, new Keys(new Buf[]{Buf.wrap(intToBytes(2))}), Buf.wrap("RolledBack".getBytes()), RequestType.none());
        
        // Should not see it
        batch = db.cdcPollBatchAsyncInternal(subId, nextSeq, 100).block();
        assertTrue(batch.events().isEmpty(), "Uncommitted transaction should not be visible");
        
        db.closeFailedUpdate(txId2); // Rollback
        
        // Should NEVER see it
        batch = db.cdcPollBatchAsyncInternal(subId, nextSeq, 100).block();
        assertTrue(batch.events().isEmpty(), "Rolled back transaction should never be visible");
    }

    @Test
    void testAtomicBatchPagination() throws Exception {
        int batchSize = 50;
        List<Keys> keys = new ArrayList<>();
        List<Buf> values = new ArrayList<>();

        for (int i = 0; i < batchSize; i++) {
            keys.add(new Keys(new Buf[]{Buf.wrap(intToBytes(i))}));
            values.add(Buf.wrap(("v"+i).getBytes()));
        }

        // Atomic write
        db.putBatch(defaultColId, Flux.just(new KVBatch.KVBatchRef(keys, values)), PutBatchMode.WRITE_BATCH);

        int pageSize = 7;
        int received = 0;
        long nextSeq = 0;
        
        // Expected total iterations: ceil(50/7) = 8
        // Loop a bit more to be safe
        for (int i = 0; i < 15; i++) {
            CdcBatch batch = db.cdcPollBatchAsyncInternal(subId, nextSeq == 0 ? null : nextSeq, pageSize).block();
            if (batch.events().isEmpty()) break;
            
            for (CDCEvent ev : batch.events()) {
                assertEquals(received, bytesToInt(ev.key().toByteArray()), "Events should be strictly ordered");
                received++;
            }
            nextSeq = batch.nextSeq();
        }
        
        assertEquals(batchSize, received, "Should receive all events from atomic batch via pagination");
    }

    @Test
    void testMultiColumnFamilyAtomicity() throws Exception {
        // Create 2nd column
        long col2 = db.createColumn("data2", ColumnSchema.of(IntArrayList.of(4), new ObjectArrayList<>(), true, null, null, null));
        String sub2 = "multi-sub";
        // Subscribe to BOTH
        db.cdcCreate(sub2, null, List.of(defaultColId, col2), false);
        
        // Manually create a WriteBatch with mixed CFs using Transaction
        long txId = db.openTransaction(5000);
        db.put(txId, defaultColId, new Keys(new Buf[]{Buf.wrap(intToBytes(1))}), Buf.wrap("A".getBytes()), RequestType.none());
        db.put(txId, col2, new Keys(new Buf[]{Buf.wrap(intToBytes(2))}), Buf.wrap("B".getBytes()), RequestType.none());
        db.put(txId, defaultColId, new Keys(new Buf[]{Buf.wrap(intToBytes(3))}), Buf.wrap("C".getBytes()), RequestType.none());
        db.closeTransaction(txId, true);
        
        CdcBatch batch = db.cdcPollBatchAsyncInternal(sub2, null, 10).block();
        assertEquals(3, batch.events().size());
        
        assertEquals(defaultColId, batch.events().get(0).columnId());
        assertEquals(col2, batch.events().get(1).columnId());
        assertEquals(defaultColId, batch.events().get(2).columnId());
        
        assertEquals(1, bytesToInt(batch.events().get(0).key().toByteArray()));
        assertEquals(2, bytesToInt(batch.events().get(1).key().toByteArray()));
        assertEquals(3, bytesToInt(batch.events().get(2).key().toByteArray()));
    }


    @Test
    void testEmptyBatchHandling() throws Exception {
        // 1. Write LogData only batch
        try (org.rocksdb.WriteBatch wb = new org.rocksdb.WriteBatch()) {
            wb.putLogData("Metadata".getBytes());
            db.getDb().get().write(new org.rocksdb.WriteOptions(), wb);
        }

        // 2. Put C
        db.put(0, defaultColId, new Keys(new Buf[]{Buf.wrap(intToBytes(99))}), Buf.wrap("C".getBytes()), RequestType.none());

        // 3. Poll
        CdcBatch b = db.cdcPollBatchAsyncInternal(subId, null, 100).block();

        // Should find C. The empty batch is skipped.
        assertNotNull(b);
        assertFalse(b.events().isEmpty());
        assertEquals(1, b.events().size());
        assertEquals(99, bytesToInt(b.events().get(0).key().toByteArray()));
    }
}
