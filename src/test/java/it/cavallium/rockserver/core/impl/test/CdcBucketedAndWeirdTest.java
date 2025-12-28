package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CdcBucketedAndWeirdTest {

    @TempDir
    Path tempDir;

    private EmbeddedDB db;

    @BeforeEach
    void setUp() throws IOException, RocksDBException {
        db = new EmbeddedDB(tempDir, "test-db-buckets", null);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (db != null) {
            db.close();
        }
    }

    private byte[] intToBytes(int x) {
        return java.nio.ByteBuffer.allocate(4).putInt(x).array();
    }

    @Test
    void testBucketedColumnResolved() throws Exception {
        // Create bucketed column: 1 fixed int, 1 variable string (hashed)
        long colId = db.createColumn("bucket-col", ColumnSchema.of(
                IntList.of(4),
                ObjectList.of(ColumnHashType.XXHASH32),
                true
        ));

        String subId = "sub-bucket-resolved";
        // Create CDC subscription WITH resolvedValues = true
        long startSeq = db.cdcCreate(subId, null, List.of(colId), true);

        // 1. Put (1, "A") -> Val "V1"
        // Fixed key: 1. Variable: "A".
        db.put(0, colId, 
               new Keys(new Buf[]{Buf.wrap(intToBytes(1)), Buf.wrap("A".getBytes())}), 
               Buf.wrap("V1".getBytes()), 
               RequestType.none());

        // 2. Put (1, "A") -> Val "V2"
        // Same fixed key 1. Same variable "A".
        // This updates the SAME bucket.
        db.put(0, colId, 
               new Keys(new Buf[]{Buf.wrap(intToBytes(1)), Buf.wrap("A".getBytes())}), 
               Buf.wrap("V2".getBytes()), 
               RequestType.none());

        // Poll CDC
        // We expect 2 events (one for each Put).
        // Since resolved=true, BOTH events should contain the LATEST bucket content (containing "V2").
        
        List<CDCEvent> events = db.cdcPollBatchAsyncInternal(subId, null, 10).block().events();
        assertEquals(2, events.size());

        CDCEvent ev1 = events.get(0);
        CDCEvent ev2 = events.get(1);

        assertEquals(CDCEvent.Op.PUT, ev1.op());
        assertEquals(CDCEvent.Op.PUT, ev2.op());
        
        // Keys should be the real keys (Fixed + Variable)
        // Fixed: 4 bytes. Variable: "A" (1 byte). Total > 4.
        assertTrue(ev1.key().size() > 4, "Key should contain variable part");
        assertEquals(1, java.nio.ByteBuffer.wrap(ev1.key().toByteArray()).getInt());
        // Verify key ends with "A"
        byte[] k1 = ev1.key().toByteArray();
        assertEquals((byte)'A', k1[k1.length-1]);
        
        assertTrue(ev2.key().size() > 4);

        // Values should be the resolved element value ("V2"), not the bucket blob.
        // Since resolved=true, and we overwrote V1 with V2, both events point to latest value V2.
        assertArrayEquals("V2".getBytes(), ev1.value().toByteArray(), "Value should be resolved V2");
        assertArrayEquals("V2".getBytes(), ev2.value().toByteArray(), "Value should be resolved V2");
    }

    @Test
    void testBucketedColumnRaw() throws Exception {
        // Control test: resolved=false
        long colId = db.createColumn("bucket-col-raw", ColumnSchema.of(
                IntList.of(4),
                ObjectList.of(ColumnHashType.XXHASH32),
                true
        ));

        String subId = "sub-bucket-raw";
        db.cdcCreate(subId, null, List.of(colId), false);

        db.put(0, colId, new Keys(new Buf[]{Buf.wrap(intToBytes(1)), Buf.wrap("A".getBytes())}), Buf.wrap("V1".getBytes()), RequestType.none());
        db.put(0, colId, new Keys(new Buf[]{Buf.wrap(intToBytes(1)), Buf.wrap("B".getBytes())}), Buf.wrap("V2".getBytes()), RequestType.none());

        List<CDCEvent> events = db.cdcPollBatchAsyncInternal(subId, null, 10).block().events();
        assertEquals(2, events.size());
        
        // Ev1: Bucket has only V1
        // Ev2: Bucket has V1 and V2
        // They should differ.
        assertFalse(java.util.Arrays.equals(events.get(0).value().toByteArray(), events.get(1).value().toByteArray()),
                "With resolved=false, events should reflect historical state (different)");
    }

    @Test
    void testNoValueColumn() throws Exception {
        // Column with hasValue=false (Set behavior).
        // Using bucketed keys (Fixed + Var) to ensure it works with buckets.
        long colId = db.createColumn("set-col", ColumnSchema.of(
                IntList.of(4),
                ObjectList.of(ColumnHashType.XXHASH32),
                false // hasValue = false
        ));

        String subId = "sub-set";
        db.cdcCreate(subId, null, List.of(colId), true); // resolved=true

        // Put (1, "A")
        db.put(0, colId, 
               new Keys(new Buf[]{Buf.wrap(intToBytes(1)), Buf.wrap("A".getBytes())}), 
               Buf.wrap(new byte[0]), // Value must be empty/ignored
               RequestType.none());

        // Poll
        List<CDCEvent> events = db.cdcPollBatchAsyncInternal(subId, null, 10).block().events();
        assertEquals(1, events.size());
        assertEquals(CDCEvent.Op.PUT, events.get(0).op());
        
        // Value should be empty (since hasValue=false), NOT the bucket blob.
        // The bucket blob contains the keys, but we extracted them into the event key.
        assertEquals(0, events.get(0).value().size(), "Value should be empty for hasValue=false column");
        
        // Key should contain "A"
        assertTrue(events.get(0).key().size() > 4);
    }
}
