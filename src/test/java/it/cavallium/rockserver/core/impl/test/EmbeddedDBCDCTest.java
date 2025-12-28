package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

public class EmbeddedDBCDCTest {

    @TempDir
    Path tempDir;

    private EmbeddedDB db;
    private long colId;

    @BeforeEach
    public void setUp() throws IOException {
        db = new EmbeddedDB(tempDir, "test_cdc", null);
        // Standard column
        colId = db.createColumn("data", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (db != null) {
            db.close();
        }
    }

    @Test
    public void testCDCStandardBehavior() throws Exception {
        String subId = "sub1";
        long startSeq = db.cdcCreate(subId, 0L, List.of(colId), false);

        Keys key = new Keys(Buf.wrap("key_0001".getBytes(StandardCharsets.UTF_8)));
        Buf val1 = Buf.wrap("val1".getBytes(StandardCharsets.UTF_8));
        Buf val2 = Buf.wrap("val2".getBytes(StandardCharsets.UTF_8));

        db.put(0, colId, key, val1, RequestType.none());
        db.put(0, colId, key, val2, RequestType.none());

        List<CDCEvent> events = db.cdcPoll(subId, startSeq, 100).toList();
        
        Assertions.assertEquals(2, events.size());
        
        CDCEvent ev1 = events.get(0);
        Assertions.assertEquals(CDCEvent.Op.PUT, ev1.op());
        Assertions.assertEquals("val1", new String(ev1.value().asArray(), StandardCharsets.UTF_8));
        
        CDCEvent ev2 = events.get(1);
        Assertions.assertEquals(CDCEvent.Op.PUT, ev2.op());
        Assertions.assertEquals("val2", new String(ev2.value().asArray(), StandardCharsets.UTF_8));
    }

    @Test
    public void testCDCEmitLatestValuesBehavior() throws Exception {
        String subId = "sub2";
        long startSeq = db.cdcCreate(subId, 0L, List.of(colId), true);

        Keys key = new Keys(Buf.wrap("keyLatst".getBytes(StandardCharsets.UTF_8)));
        Buf val1 = Buf.wrap("v1".getBytes(StandardCharsets.UTF_8));
        Buf val2 = Buf.wrap("v2".getBytes(StandardCharsets.UTF_8));

        db.put(0, colId, key, val1, RequestType.none());
        db.put(0, colId, key, val2, RequestType.none());

        List<CDCEvent> events = db.cdcPoll(subId, startSeq, 100).toList();
        
        Assertions.assertEquals(2, events.size());
        
        // Both events should point to the latest value "v2"
        CDCEvent ev1 = events.get(0);
        Assertions.assertEquals(CDCEvent.Op.PUT, ev1.op());
        Assertions.assertEquals("v2", new String(ev1.value().asArray(), StandardCharsets.UTF_8));
        
        CDCEvent ev2 = events.get(1);
        Assertions.assertEquals(CDCEvent.Op.PUT, ev2.op());
        Assertions.assertEquals("v2", new String(ev2.value().asArray(), StandardCharsets.UTF_8));
    }

    @Test
    public void testCDCEmitLatestValuesMixedKeys() throws Exception {
        String subId = "sub3";
        long startSeq = db.cdcCreate(subId, 0L, List.of(colId), true);

        Keys k1 = new Keys(Buf.wrap("key_0001".getBytes(StandardCharsets.UTF_8)));
        Keys k2 = new Keys(Buf.wrap("key_0002".getBytes(StandardCharsets.UTF_8)));
        Buf v1 = Buf.wrap("v1".getBytes(StandardCharsets.UTF_8));
        Buf v2 = Buf.wrap("v2".getBytes(StandardCharsets.UTF_8));
        Buf v3 = Buf.wrap("v3".getBytes(StandardCharsets.UTF_8));

        db.put(0, colId, k1, v1, RequestType.none());
        db.put(0, colId, k2, v2, RequestType.none());
        db.put(0, colId, k1, v3, RequestType.none());

        List<CDCEvent> events = db.cdcPoll(subId, startSeq, 100).toList();

        Assertions.assertEquals(3, events.size());

        Assertions.assertEquals("v3", new String(events.get(0).value().asArray(), StandardCharsets.UTF_8));
        Assertions.assertEquals("v2", new String(events.get(1).value().asArray(), StandardCharsets.UTF_8));
        Assertions.assertEquals("v3", new String(events.get(2).value().asArray(), StandardCharsets.UTF_8));
    }

    @Test
    public void testCDCEmitLatestValuesBucketedSupported() throws Exception {
        long bucketColId = db.createColumn("bucketed", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(ColumnHashType.XXHASH32), true));
        
        String subId = "sub4";
        // emitLatestValues = true
        long startSeq = db.cdcCreate(subId, 0L, List.of(bucketColId), true);

        // Bucketed column key: 1 fixed (Long.BYTES=8) + 1 variable (hashed XXHASH32)
        Keys key = new Keys(Buf.wrap("fixedkey".getBytes(StandardCharsets.UTF_8)), Buf.wrap("variable".getBytes(StandardCharsets.UTF_8)));
        Buf val1 = Buf.wrap("b1".getBytes(StandardCharsets.UTF_8));
        Buf val2 = Buf.wrap("b2".getBytes(StandardCharsets.UTF_8));

        db.put(0, bucketColId, key, val1, RequestType.none());
        db.put(0, bucketColId, key, val2, RequestType.none());

        List<CDCEvent> events = db.cdcPoll(subId, startSeq, 100).toList();

        Assertions.assertEquals(2, events.size());
        
        // Since we now support resolution for bucketed columns, both events should
        // resolve to the LATEST state of the bucket.
        // Also, we expect "Real Keys" (Fixed + Variable concatenated), not Raw/Hashed keys.
        
        CDCEvent ev1 = events.get(0);
        CDCEvent ev2 = events.get(1);
        
        // Value should be "b2" (the latest value), not the bucket blob
        Assertions.assertEquals("b2", new String(ev1.value().asArray(), StandardCharsets.UTF_8));
        Assertions.assertEquals("b2", new String(ev2.value().asArray(), StandardCharsets.UTF_8));
        
        // Key should be "fixedkey" + "variable"
        String expectedKey = "fixedkey" + "variable";
        Assertions.assertEquals(expectedKey, new String(ev1.key().asArray(), StandardCharsets.UTF_8));
        Assertions.assertEquals(expectedKey, new String(ev2.key().asArray(), StandardCharsets.UTF_8));
    }
}
