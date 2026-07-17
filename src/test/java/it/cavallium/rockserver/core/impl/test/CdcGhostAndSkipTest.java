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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CdcGhostAndSkipTest {

    private static final int FILTERED_WAL_BATCH_SIZE = 512;
    private static final int FILTERED_WAL_BATCHES_PER_PHYSICAL_PAGE = 8;
    private static final int FILTERED_MUTATIONS_PER_PHYSICAL_PAGE =
            FILTERED_WAL_BATCH_SIZE * FILTERED_WAL_BATCHES_PER_PHYSICAL_PAGE;
    private static final Duration CDC_POLL_TIMEOUT = Duration.ofSeconds(30);

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

    private int writeFilteredWalPage() throws RocksDBException {
        int written = 0;
        for (int batch = 0; batch < FILTERED_WAL_BATCHES_PER_PHYSICAL_PAGE; batch++) {
            long txId = db.openTransaction(10_000);
            for (int i = 0; i < FILTERED_WAL_BATCH_SIZE; i++) {
                int key = batch * FILTERED_WAL_BATCH_SIZE + i;
                db.put(txId,
                        colB,
                        new Keys(new Buf[]{Buf.wrap(intToBytes(key))}),
                        Buf.wrap(intToBytes(key)),
                        RequestType.none());
                written++;
            }
            db.closeTransaction(txId, true);
        }
        return written;
    }

    private int writeFilteredWalPagesThenMatchingEvent() throws RocksDBException {
        int written = writeFilteredWalPage();
        long matchingTx = db.openTransaction(10_000);
        db.put(matchingTx,
                colA,
                new Keys(new Buf[]{Buf.wrap(intToBytes(FILTERED_MUTATIONS_PER_PHYSICAL_PAGE))}),
                Buf.wrap("match".getBytes(StandardCharsets.UTF_8)),
                RequestType.none());
        db.closeTransaction(matchingTx, true);
        return written + 1;
    }

    @Test
    void eventOnlyAsyncPollTraversesFullyFilteredPhysicalPage() throws Exception {
        String subscriptionId = "sub-event-only-filtered-pages";
        long startSeq = db.cdcCreate(subscriptionId, null, List.of(colA), false);
        int written = writeFilteredWalPagesThenMatchingEvent();

        assertEquals(FILTERED_MUTATIONS_PER_PHYSICAL_PAGE + 1, written,
                "the fixture must contain a full physical page in multiple WAL batches plus one matching batch");

        List<CDCEvent> events = Flux.from(db.cdcPollAsyncInternal(subscriptionId, startSeq, 1))
                .collectList()
                .block(CDC_POLL_TIMEOUT);

        assertNotNull(events);
        assertEquals(1, events.size(), "Event-only polling must continue past an empty physical CDC page");
        assertEquals(colA, events.getFirst().columnId());
        assertEquals("match", new String(events.getFirst().value().toByteArray(), StandardCharsets.UTF_8));
    }

    @Test
    void eventOnlyAsyncPollRetainsOneWalIteratorAcrossFilteredPhysicalSlices() throws Exception {
        String subscriptionId = "sub-event-only-retained-wal-iterator";
        long startSeq = db.cdcCreate(subscriptionId, null, List.of(colA), false);
        final int filteredPages = 3;
        int filteredMutations = 0;
        for (int page = 0; page < filteredPages; page++) {
            filteredMutations += writeFilteredWalPage();
        }
        long matchingTx = db.openTransaction(10_000);
        db.put(matchingTx,
                colA,
                new Keys(new Buf[]{Buf.wrap(intToBytes(filteredMutations))}),
                Buf.wrap("match".getBytes(StandardCharsets.UTF_8)),
                RequestType.none());
        db.closeTransaction(matchingTx, true);

        assertEquals((long) filteredPages * FILTERED_MUTATIONS_PER_PHYSICAL_PAGE,
                filteredMutations,
                "the fixture must force several 4096-mutation scheduler slices");
        var iteratorOpens = new AtomicInteger();
        db.setCdcWalIteratorOpenObserverForTesting(iteratorOpens::incrementAndGet);
        List<CDCEvent> events;
        try {
            events = Flux.from(db.cdcPollAsyncInternal(subscriptionId, startSeq, 1))
                    .collectList()
                    .block(CDC_POLL_TIMEOUT);
        } finally {
            db.setCdcWalIteratorOpenObserverForTesting(null);
        }

        assertNotNull(events);
        assertEquals(1, events.size());
        assertEquals("match", new String(events.getFirst().value().toByteArray(), StandardCharsets.UTF_8));
        assertEquals(1, iteratorOpens.get(),
                "fair scheduler slices must retain one WAL iterator for the logical poll");
    }

    @Test
    void batchPollContinuesAcrossFilteredSchedulerSlices() throws Exception {
        String subscriptionId = "sub-batch-filtered-page";
        long startSeq = db.cdcCreate(subscriptionId, null, List.of(colA), false);
        // Scheduler slices are an internal fairness boundary. A logical batch poll
        // retains its WAL/parser cursor and must not expose an artificial empty batch
        // merely because one slice contained only filtered mutations.
        int written = writeFilteredWalPagesThenMatchingEvent();

        assertEquals(FILTERED_MUTATIONS_PER_PHYSICAL_PAGE + 1, written);

        CdcBatch batch = db.cdcPollBatchAsyncInternal(subscriptionId, startSeq, 1)
                .block(CDC_POLL_TIMEOUT);

        assertNotNull(batch);
        assertEquals(1, batch.events().size());
        assertEquals("match", new String(batch.events().getFirst().value().toByteArray(), StandardCharsets.UTF_8));
        assertTrue(batch.nextSeq() > startSeq);
    }

    @Test
    void eventOnlyAsyncPollDoesNotChaseWritesPastItsCapturedTail() throws Exception {
        String subscriptionId = "sub-event-only-stable-tail";
        long startSeq = db.cdcCreate(subscriptionId, null, List.of(colA), false);
        assertEquals(FILTERED_MUTATIONS_PER_PHYSICAL_PAGE, writeFilteredWalPage());

        var appended = new AtomicBoolean();
        db.setCdcPollTailCapturedObserverForTesting(() -> {
            if (appended.compareAndSet(false, true)) {
                long txId = db.openTransaction(10_000);
                db.put(txId,
                        colA,
                        new Keys(new Buf[]{Buf.wrap(intToBytes(FILTERED_MUTATIONS_PER_PHYSICAL_PAGE))}),
                        Buf.wrap("after-tail".getBytes(StandardCharsets.UTF_8)),
                        RequestType.none());
                db.closeTransaction(txId, true);
            }
        });

        List<CDCEvent> firstPoll;
        try {
            firstPoll = Flux.from(db.cdcPollAsyncInternal(subscriptionId, startSeq, 1))
                    .collectList()
                    .block(CDC_POLL_TIMEOUT);
        } finally {
            db.setCdcPollTailCapturedObserverForTesting(null);
        }

        assertTrue(appended.get(), "the fixture must append after the logical poll captured its tail");
        assertNotNull(firstPoll);
        assertTrue(firstPoll.isEmpty(), "a logical poll must not chase a concurrently growing WAL");

        List<CDCEvent> secondPoll = Flux.from(db.cdcPollAsyncInternal(subscriptionId, startSeq, 1))
                .collectList()
                .block(CDC_POLL_TIMEOUT);
        assertNotNull(secondPoll);
        assertEquals(1, secondPoll.size(), "the next logical poll must see the post-tail append");
        assertEquals("after-tail",
                new String(secondPoll.getFirst().value().toByteArray(), StandardCharsets.UTF_8));
    }

    @Test
    void synchronousPollHonorsRequestsAboveTheFormerPhysicalPageSize() throws Exception {
        String subscriptionId = "sub-sync-large";
        long startSeq = db.cdcCreate(subscriptionId, null, List.of(colA), false);
        final int events = 5_000;
        final int transactionSize = 512;
        int written = 0;
        for (int first = 0; first < events; first += transactionSize) {
            long txId = db.openTransaction(10_000);
            int end = Math.min(events, first + transactionSize);
            for (int i = first; i < end; i++) {
                db.put(txId,
                        colA,
                        new Keys(new Buf[]{Buf.wrap(intToBytes(i))}),
                        Buf.wrap(intToBytes(i)),
                        RequestType.none());
                written++;
            }
            db.closeTransaction(txId, true);
        }

        assertEquals(events, written, "the fixture must not overfill its final WAL batch");
        var iteratorOpens = new AtomicInteger();
        db.setCdcWalIteratorOpenObserverForTesting(iteratorOpens::incrementAndGet);
        List<CDCEvent> polled;
        try {
            try (var poll = db.cdcPoll(subscriptionId, startSeq, events)) {
                polled = poll.toList();
            }
        } finally {
            db.setCdcWalIteratorOpenObserverForTesting(null);
        }

        assertEquals(events, polled.size(),
                "the public synchronous API must not be silently capped to an internal page size");
        assertEquals(1, iteratorOpens.get(),
                "a synchronous poll must scan its fixed WAL tail with one iterator");
        var uniqueSequences = new HashSet<Long>(events);
        long previousSequence = -1L;
        for (int i = 0; i < polled.size(); i++) {
            var event = polled.get(i);
            assertTrue(event.seq() > previousSequence, "CDC events must remain strictly ordered across pages");
            assertTrue(uniqueSequences.add(event.seq()), "CDC pages must not replay an event");
            assertEquals(i, bytesToInt(event.key().toByteArray()), "unexpected key order at index " + i);
            assertEquals(i, bytesToInt(event.value().toByteArray()), "unexpected value order at index " + i);
            previousSequence = event.seq();
        }
        assertEquals(events, uniqueSequences.size());
    }

    @Test
    void eventOnlyAsyncPollKeepsOneAggregateByteBudgetAcrossPhysicalPages() throws Exception {
        String subscriptionId = "sub-async-byte-budget";
        long startSeq = db.cdcCreate(subscriptionId, null, List.of(colA), false);
        final int events = 10_000;
        final int eventsPerTransaction = 1_000;
        final int valueBytes = 1_800;
        var value = Buf.wrap(new byte[valueBytes]);
        int written = 0;
        for (int first = 0; first < events; first += eventsPerTransaction) {
            long txId = db.openTransaction(10_000);
            int end = Math.min(events, first + eventsPerTransaction);
            for (int i = first; i < end; i++) {
                db.put(txId,
                        colA,
                        new Keys(new Buf[]{Buf.wrap(intToBytes(i))}),
                        value,
                        RequestType.none());
                written++;
            }
            db.closeTransaction(txId, true);
        }

        assertEquals(events, written);
        List<CDCEvent> emitted = Flux.from(db.cdcPollAsyncInternal(subscriptionId, startSeq, events))
                .collectList()
                .block(CDC_POLL_TIMEOUT);
        assertNotNull(emitted);
        long eventBytes = Integer.BYTES + valueBytes;
        long byteBudget = 16L * 1024L * 1024L;
        long expectedEvents = byteBudget / eventBytes;
        assertEquals(expectedEvents, emitted.size(),
                "all continuation pages must share one exact logical 16 MiB byte budget");
        assertTrue(emitted.size() > 4_096, "the logical poll should continue beyond one physical page");
        assertTrue((long) emitted.size() * eventBytes <= byteBudget);
        for (int i = 0; i < emitted.size(); i++) {
            assertEquals(i, bytesToInt(emitted.get(i).key().toByteArray()),
                    "byte-limited continuation must remain gap-free and ordered");
        }
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
