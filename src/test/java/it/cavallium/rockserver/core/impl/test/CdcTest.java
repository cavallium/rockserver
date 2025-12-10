package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class CdcTest {

    private Path dbDir;
    private Path configFile;

    @BeforeEach
    void setUp() throws IOException {
        dbDir = Files.createTempDirectory("rockserver-cdc-test");
        configFile = Files.createTempFile("rockserver-config", ".conf");
        Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (dbDir != null) Utils.deleteDirectory(dbDir.toString());
        if (configFile != null) Files.deleteIfExists(configFile);
    }

    @Test
    void testBasicCdcFlow() throws Exception {
        try (var db = new EmbeddedConnection(dbDir, "cdc-test", configFile)) {
            var colId = db.getSyncApi().createColumn("test-col", ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));

            // Create CDC subscription
            var startSeq = db.getSyncApi().cdcCreate("sub1", null, null);
            assertTrue(startSeq >= 0);

            // Put some data
            var key1 = new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 1})});
            db.getSyncApi().put(0, colId, key1, Buf.wrap("val1".getBytes(StandardCharsets.UTF_8)), RequestType.none());
            
            var key2 = new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 2})});
            db.getSyncApi().put(0, colId, key2, Buf.wrap("val2".getBytes(StandardCharsets.UTF_8)), RequestType.none());

            // Verify events
            List<CDCEvent> events = db.getSyncApi().cdcPoll("sub1", null, 100).collect(Collectors.toList());
            assertEquals(2, events.size());
            assertEquals(CDCEvent.Op.PUT, events.get(0).op());
            assertEquals(CDCEvent.Op.PUT, events.get(1).op());
            assertEquals("val1", new String(events.get(0).value().toByteArray(), StandardCharsets.UTF_8));
            assertEquals("val2", new String(events.get(1).value().toByteArray(), StandardCharsets.UTF_8));
            
            // Commit
            db.getSyncApi().cdcCommit("sub1", events.get(1).seq());
        }
    }
    
    @Test
    void testResumeAfterRestart() throws Exception {
        var colName = "test-col-resume";
        long lastSeq;
        long colId;
        
        try (var db = new EmbeddedConnection(dbDir, "cdc-resume", configFile)) {
            colId = db.getSyncApi().createColumn(colName, ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
            db.getSyncApi().cdcCreate("sub1", null, null);
            
            var key1 = new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 1})});
            db.getSyncApi().put(0, colId, key1, Buf.wrap("val1".getBytes(StandardCharsets.UTF_8)), RequestType.none());
            
            List<CDCEvent> events = db.getSyncApi().cdcPoll("sub1", null, 100).collect(Collectors.toList());
            assertEquals(1, events.size());
            lastSeq = events.get(0).seq();
            db.getSyncApi().cdcCommit("sub1", lastSeq);
        }
        
        // Reopen
        try (var db = new EmbeddedConnection(dbDir, "cdc-resume", configFile)) {
            colId = db.getSyncApi().getColumnId(colName);
            
            // New event
            var key2 = new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 2})});
            db.getSyncApi().put(0, colId, key2, Buf.wrap("val2".getBytes(StandardCharsets.UTF_8)), RequestType.none());
            
            // Poll - should get only the new event
            List<CDCEvent> events = db.getSyncApi().cdcPoll("sub1", null, 100).collect(Collectors.toList());
            assertEquals(1, events.size());
            assertTrue(events.get(0).seq() > lastSeq);
            assertEquals("val2", new String(events.get(0).value().toByteArray(), StandardCharsets.UTF_8));
        }
    }
    
    @Test
    void testBucketedColumn() throws Exception {
        try (var db = new EmbeddedConnection(dbDir, "cdc-bucket", configFile)) {
            // Bucketed column: 1 fixed key (int), 1 variable key (int)
            var colId = db.getSyncApi().createColumn("bucket-col", ColumnSchema.of(
                    IntList.of(Integer.BYTES), 
                    ObjectList.of(ColumnHashType.XXHASH32),
                    true
            ));
            db.getSyncApi().cdcCreate("sub-bucket", null, null);
            
            var key = new Keys(new Buf[]{
                Buf.wrap(new byte[]{0, 0, 0, 1}), // fixed
                Buf.wrap(new byte[]{0, 0, 0, 2})  // variable
            });
            db.getSyncApi().put(0, colId, key, Buf.wrap("bucket-val".getBytes(StandardCharsets.UTF_8)), RequestType.none());
            
            List<CDCEvent> events = db.getSyncApi().cdcPoll("sub-bucket", null, 100).collect(Collectors.toList());
            assertEquals(1, events.size());
            // The key in CDC event should be the fixed key (4 bytes)
            assertEquals(4, events.get(0).key().size());
            // The value should be the bucket content (not just "bucket-val")
            assertTrue(events.get(0).value().size() > "bucket-val".length());
        }
    }

    @Test
    void testStressAndBackpressure() throws Exception {
        int totalEvents = 50_000;
        int batchSize = 1000;
        
        try (var db = new EmbeddedConnection(dbDir, "cdc-stress", configFile)) {
            var colId = db.getSyncApi().createColumn("stress-col", ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
            var startSeq = db.getSyncApi().cdcCreate("stress-sub", null, null);
            
            // 1. Generate load
            for (int i = 0; i < totalEvents; i++) {
                var key = new Keys(new Buf[]{Buf.wrap(new byte[]{
                        (byte) (i >> 24), (byte) (i >> 16), (byte) (i >> 8), (byte) i
                })});
                db.getSyncApi().put(0, colId, key, Buf.wrap(("val-" + i).getBytes(StandardCharsets.UTF_8)), RequestType.none());
                if (i % 10000 == 0) db.getSyncApi().flush(); // Force some WAL flushes/rotations
            }
            
            // 2. Poll with backpressure
            long consumed = 0;
            long lastSeq = startSeq - 1;
            
            while (consumed < totalEvents) {
                // Poll small batch
                List<CDCEvent> events = db.getSyncApi().cdcPoll("stress-sub", null, batchSize).collect(Collectors.toList());
                
                if (events.isEmpty()) {
                    // Should not happen if we expect events, but maybe wait a bit? 
                    // In embedded DB, put is synchronous so WAL should be ready.
                    break;
                }
                
                // Verify order and sequence
                for (CDCEvent ev : events) {
                    assertTrue(ev.seq() > lastSeq, "Sequence must be strictly increasing");
                    lastSeq = ev.seq();
                    consumed++;
                }
                
                // Commit periodically
                db.getSyncApi().cdcCommit("stress-sub", lastSeq);
            }
            
            assertEquals(totalEvents, consumed);
        }
    }
}
