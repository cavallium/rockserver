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
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CdcGhostAndSkipTest {

    @TempDir
    Path tempDir;

    private EmbeddedDB db;
    private long colA;
    private long colB;

    @BeforeEach
    void setUp() throws IOException, RocksDBException {
        db = new EmbeddedDB(tempDir, "test-db", null);
        
        colA = db.createColumn("col-A", ColumnSchema.of(IntArrayList.of(4), new ObjectArrayList<>(), true, null, null, null));
        colB = db.createColumn("col-B", ColumnSchema.of(IntArrayList.of(4), new ObjectArrayList<>(), true, null, null, null));
    }

    @AfterEach
    void tearDown() throws IOException {
        if (db != null) {
            db.closeTesting();
        }
    }

    private byte[] intToBytes(int x) {
        return java.nio.ByteBuffer.allocate(4).putInt(x).array();
    }
    
    private int bytesToInt(byte[] b) {
        return java.nio.ByteBuffer.wrap(b).getInt();
    }

    @Test
    void testGhostUpdatesAndCrossCfIsolation() throws Exception {
        // Create subscriptions
        String subA = "sub-A";
        String subB = "sub-B";
        db.cdcCreate(subA, null, List.of(colA), false);
        db.cdcCreate(subB, null, List.of(colB), false);

        // Write a mixed batch: [Put A1, Put B1, Put A2]
        // We need to use raw RocksDB to ensure they are in the same WriteBatch
        // EmbeddedDB.putBatch usually separates by column or puts all in one if supported.
        // Let's use putBatch with multiple entries.
        
        List<Keys> keys = new ArrayList<>();
        List<Buf> values = new ArrayList<>();
        // But EmbeddedDB.putBatch takes a single columnId. 
        // We need to use a Transaction or access raw DB to mix columns in one atomic batch, 
        // OR rely on the fact that EmbeddedDB doesn't easily expose mixed-column atomic writes in one call 
        // EXCEPT via Transactions.
        
        long txId = db.openTransaction(10000);
        db.put(txId, colA, new Keys(new Buf[]{Buf.wrap(intToBytes(1))}), Buf.wrap("A1".getBytes()), RequestType.none());
        db.put(txId, colB, new Keys(new Buf[]{Buf.wrap(intToBytes(1))}), Buf.wrap("B1".getBytes()), RequestType.none());
        db.put(txId, colA, new Keys(new Buf[]{Buf.wrap(intToBytes(2))}), Buf.wrap("A2".getBytes()), RequestType.none());
        db.closeTransaction(txId, true); // Commit

        // Poll Sub A
        CdcBatch batchA = db.cdcPollBatchAsyncInternal(subA, null, 10).block();
        assertEquals(2, batchA.events().size(), "Sub A should see 2 events");
        assertEquals("A1", new String(batchA.events().get(0).value().toByteArray()));
        assertEquals("A2", new String(batchA.events().get(1).value().toByteArray()));

        // Poll Sub B
        CdcBatch batchB = db.cdcPollBatchAsyncInternal(subB, null, 10).block();
        assertEquals(1, batchB.events().size(), "Sub B should see 1 event");
        assertEquals("B1", new String(batchB.events().get(0).value().toByteArray()));
    }

    @Test
    void testBatchInterleavingResumption() throws Exception {
        String subA = "sub-A-interleaved";
        db.cdcCreate(subA, null, List.of(colA), false);

        // Write mixed batch: [Put A1, Put B1, Put A2]
        long txId = db.openTransaction(10000);
        db.put(txId, colA, new Keys(new Buf[]{Buf.wrap(intToBytes(1))}), Buf.wrap("A1".getBytes()), RequestType.none());
        db.put(txId, colB, new Keys(new Buf[]{Buf.wrap(intToBytes(1))}), Buf.wrap("B1".getBytes()), RequestType.none());
        db.put(txId, colA, new Keys(new Buf[]{Buf.wrap(intToBytes(2))}), Buf.wrap("A2".getBytes()), RequestType.none());
        db.closeTransaction(txId, true);

        // Poll A with limit 1
        CdcBatch batch1 = db.cdcPollBatchAsyncInternal(subA, null, 1).block();
        assertEquals(1, batch1.events().size());
        assertEquals("A1", new String(batch1.events().get(0).value().toByteArray()));
        
        long nextSeq = batch1.nextSeq();

        // Poll A remaining
        // Internally this must skip A1 (index 0), skip B1 (index 1 - filtered), and find A2 (index 2)
        CdcBatch batch2 = db.cdcPollBatchAsyncInternal(subA, nextSeq, 10).block();
        assertEquals(1, batch2.events().size());
        assertEquals("A2", new String(batch2.events().get(0).value().toByteArray()));
    }

    @Test
    void testLogDataStuckIssue() throws Exception {
        String subA = "sub-logdata";
        db.cdcCreate(subA, null, List.of(colA), false);

        // 1. Write Data (Using Transaction for consistency)
        long tx1 = db.openTransaction(10000);
        db.put(tx1, colA, new Keys(new Buf[]{Buf.wrap(intToBytes(1))}), Buf.wrap("Data1".getBytes()), RequestType.none());
        db.closeTransaction(tx1, true);

        // Poll
        CdcBatch b1 = db.cdcPollBatchAsyncInternal(subA, null, 10).block();
        assertEquals(1, b1.events().size());
        long seqAfterA1 = b1.nextSeq();

        // 2. Write Put B1 (Batch 2) - This batch is filtered out for subA
        long tx2 = db.openTransaction(10000);
        db.put(tx2, colB, new Keys(new Buf[]{Buf.wrap(intToBytes(1))}), Buf.wrap("B1".getBytes()), RequestType.none());
        db.closeTransaction(tx2, true);

        // 3. Write Put A2 (Batch 3)
        long tx3 = db.openTransaction(10000);
        db.put(tx3, colA, new Keys(new Buf[]{Buf.wrap(intToBytes(2))}), Buf.wrap("A2".getBytes()), RequestType.none());
        db.closeTransaction(tx3, true);

        // 4. Poll from seqAfterA1.
        // It should skip Batch 2 (all filtered) and find Batch 3.
        CdcBatch b2 = db.cdcPollBatchAsyncInternal(subA, seqAfterA1, 10).block();
        
        assertFalse(b2.events().isEmpty(), "Should not be stuck on filtered batch");
        assertEquals("A2", new String(b2.events().get(0).value().toByteArray()));
    }

    @Test
    void testIdempotency() throws Exception {
        String sub = "sub-idempotent";
        db.cdcCreate(sub, null, List.of(colA), false);

        // Write 3 items
        long tx = db.openTransaction(10000);
        for(int i=0; i<3; i++) {
            db.put(tx, colA, new Keys(new Buf[]{Buf.wrap(intToBytes(i))}), Buf.wrap(("V"+i).getBytes()), RequestType.none());
        }
        db.closeTransaction(tx, true);

        // Poll 1: Get all 3
        CdcBatch b1 = db.cdcPollBatchAsyncInternal(sub, null, 10).block();
        assertEquals(3, b1.events().size());
        long seqAfter = b1.nextSeq();

        // Poll 2: Same request (from null) -> Should get same result
        CdcBatch b2 = db.cdcPollBatchAsyncInternal(sub, null, 10).block();
        assertEquals(3, b2.events().size());
        assertEquals(b1.events().get(0).seq(), b2.events().get(0).seq());
        assertEquals(b1.nextSeq(), b2.nextSeq());

        // Poll 3: From seqAfter -> Should get empty
        CdcBatch b3 = db.cdcPollBatchAsyncInternal(sub, seqAfter, 10).block();
        assertTrue(b3.events().isEmpty());
    }
}
