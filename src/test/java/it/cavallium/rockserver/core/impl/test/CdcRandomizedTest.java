package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class CdcRandomizedTest {

    @TempDir
    Path tempDir;

    private EmbeddedDB db;
    private long columnId;
    private final String subId = "rand-sub";

    @BeforeEach
    void setUp() throws IOException, RocksDBException {
        db = new EmbeddedDB(tempDir, "test-db-rand", null);
        var schema = ColumnSchema.of(
                IntArrayList.of(4), // Int key
                new ObjectArrayList<>(),
                true,
                null, null, null
        );
        columnId = db.createColumn("data", schema);
        db.cdcCreate(subId, null, List.of(columnId), false);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (db != null) {
            db.close();
        }
    }

    private byte[] intToBytes(int i) {
        return ByteBuffer.allocate(4).putInt(i).array();
    }

    private int bytesToInt(byte[] b) {
        return ByteBuffer.wrap(b).getInt();
    }

    @Test
    void testRandomizedOpsVerification() throws RocksDBException {
        runRandomizedTest(5000, 100, false);
    }

    @Test
    void testRandomizedOpsWithCommitVerification() throws RocksDBException {
        runRandomizedTest(5000, 100, true);
    }

    private void runRandomizedTest(int iterations, int keyRange, boolean commitPeriodically) throws RocksDBException {
        Random random = new Random(42); // Deterministic seed
        Map<Integer, String> expectedState = new HashMap<>();

        // 1. Perform Randomized Operations
        for (int i = 0; i < iterations; i++) {
            int keyInt = random.nextInt(keyRange);
            Keys key = new Keys(new Buf[]{Buf.wrap(intToBytes(keyInt))});

            // 80% PUT, 20% DELETE
            if (random.nextDouble() < 0.8) {
                String valueStr = "val-" + i;
                Buf value = Buf.wrap(valueStr.getBytes(StandardCharsets.UTF_8));
                db.put(0, columnId, key, value, RequestType.none());
                expectedState.put(keyInt, valueStr);
            } else {
                db.delete(0, columnId, key, RequestType.none());
                expectedState.remove(keyInt);
            }
        }

        // 2. Replay CDC Stream
        Map<Integer, String> reconstructedState = new HashMap<>();
        Long startSeq = null; // Start from beginning

        // Consume until we reach the end
        long processedEvents = 0;
        
        // We expect at least 'iterations' events if we consume everything, 
        // BUT some ops might be filtered or compacted if we restart? 
        // No, CDC WAL guarantees all ops are present unless deleted from WAL (which shouldn't happen this fast).
        
        while (true) {
            CdcBatch batch = db.cdcPollBatchAsyncInternal(subId, startSeq, 100).block();
            assertNotNull(batch);
            
            if (batch.events().isEmpty()) {
                // Check if we are truly at the end (compare with latest seq)
                // Or simply break if we've processed enough?
                // The issue is cdcPoll returns empty if no MORE events.
                // We should be able to read all written events.
                
                // Let's rely on the fact that we wrote synchronously, so they should be available.
                // If the loop finishes too early, the size check below will fail.
                break;
            }

            for (CDCEvent event : batch.events()) {
                int key = bytesToInt(event.key().toByteArray());
                if (event.op() == CDCEvent.Op.PUT || event.op() == CDCEvent.Op.MERGE) {
                    // Note: Basic CDC emits raw ops. If we used MERGE in test, we'd need to handle it.
                    // Here we only use PUT and DELETE.
                    reconstructedState.put(key, new String(event.value().toByteArray(), StandardCharsets.UTF_8));
                } else if (event.op() == CDCEvent.Op.DELETE) {
                    reconstructedState.remove(key);
                }
                processedEvents++;
            }
            
            long lastSeq = batch.events().get(batch.events().size() - 1).seq();
            startSeq = batch.nextSeq();

            if (commitPeriodically && processedEvents % 500 == 0) {
                db.cdcCommit(subId, lastSeq);
                // Reset startSeq to null to test resumption from commit
                startSeq = null;
            }
        }

        // 3. Validation
        assertEquals(iterations, processedEvents, "Should have processed exactly one event per operation");
        assertEquals(expectedState.size(), reconstructedState.size(), "State size mismatch");

        for (Map.Entry<Integer, String> entry : expectedState.entrySet()) {
            assertEquals(entry.getValue(), reconstructedState.get(entry.getKey()), 
                    "Mismatch for key " + entry.getKey());
        }
        
        // 4. Verify against DB
        for (Map.Entry<Integer, String> entry : expectedState.entrySet()) {
            Keys key = new Keys(new Buf[]{Buf.wrap(intToBytes(entry.getKey()))});
            Buf dbVal = db.get(0, columnId, key, RequestType.current());
            assertNotNull(dbVal, "DB missing key " + entry.getKey());
            assertEquals(entry.getValue(), new String(dbVal.toByteArray(), StandardCharsets.UTF_8));
        }
        
        // Verify deleted keys are gone from DB
        for (int k = 0; k < keyRange; k++) {
            if (!expectedState.containsKey(k)) {
                Keys key = new Keys(new Buf[]{Buf.wrap(intToBytes(k))});
                Buf dbVal = db.get(0, columnId, key, RequestType.current());
                // Depending on implementation, get might return null or empty or throw.
                // In embedded DB, usually returns null if not found.
                if (dbVal != null) {
                    // Check if it's really empty or just a tombstone?
                    // RocksDB get returns null if not found.
                    // EmbeddedDB.get returns null.
                     assertEquals(null, dbVal, "Key " + k + " should be deleted");
                }
            }
        }
    }
}
