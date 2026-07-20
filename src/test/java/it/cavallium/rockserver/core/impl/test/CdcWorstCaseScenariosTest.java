package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.MyStringAppendOperator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.function.Executable;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(60)
class CdcWorstCaseScenariosTest {

    @TempDir
    Path tempDir;

    private EmbeddedDB db;
    private long targetCol;
    private long noiseCol;

    @BeforeEach
    void setUp() throws IOException, RocksDBException {
        db = new EmbeddedDB(tempDir, "cdc-worst-case", null);
        targetCol = db.createColumn("target", fixedSchema());
        noiseCol = db.createColumn("noise", fixedSchema());
    }

    @AfterEach
    void tearDown() throws IOException {
        if (db != null) {
            db.closeTesting();
        }
    }

    @Test
    void tinyPagesSurviveInterleavedFilteredDeletesAndFilteredEmptyBatches() throws RocksDBException {
        String subId = "tiny-interleaved";
        db.cdcCreate(subId, null, List.of(targetCol), false);

        long tx = db.openTransaction(10_000);
        db.put(tx, targetCol, key(1), value("target-1"), RequestType.none());
        db.put(tx, noiseCol, key(100), value("noise-100"), RequestType.none());
        db.put(tx, targetCol, key(2), value("target-2"), RequestType.none());
        db.delete(tx, targetCol, key(1), RequestType.none());
        db.closeTransaction(tx, true);

        List<CDCEvent> events = new ArrayList<>();
        Long cursor = null;
        for (int i = 0; i < 3; i++) {
            CdcBatch page = poll(subId, cursor, 1);
            assertEquals(1, page.events().size(), "tiny page " + i + " must contain exactly one target event");
            events.add(page.events().getFirst());
            cursor = page.nextSeq();
        }

        assertEvent(events.get(0), CDCEvent.Op.PUT, 1, "target-1");
        assertEvent(events.get(1), CDCEvent.Op.PUT, 2, "target-2");
        assertEquals(CDCEvent.Op.DELETE, events.get(2).op());
        assertEquals(1, intValue(events.get(2).key()));
        assertStrictlyIncreasing(events);

        long beforeFilteredOnly = cursor;
        long noiseOnlyTx = db.openTransaction(10_000);
        db.put(noiseOnlyTx, noiseCol, key(200), value("noise-200"), RequestType.none());
        db.put(noiseOnlyTx, noiseCol, key(201), value("noise-201"), RequestType.none());
        db.closeTransaction(noiseOnlyTx, true);

        CdcBatch empty = poll(subId, cursor, 10);
        assertTrue(empty.events().isEmpty(), "filtered-only WAL batch must emit no target events");
        assertTrue(empty.nextSeq() > beforeFilteredOnly, "filtered-only WAL batch must still advance the CDC cursor");

        db.put(0, targetCol, key(3), value("target-3"), RequestType.none());
        CdcBatch tail = poll(subId, empty.nextSeq(), 10);
        assertEquals(1, tail.events().size());
        assertEvent(tail.events().getFirst(), CDCEvent.Op.PUT, 3, "target-3");
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void cdcLifecycleRejectsNullIdsWithExplicitDiagnostics() {
        assertNullCdcIdRejected(() -> db.cdcCreate(null, null, null, false));
        assertNullCdcIdRejected(() -> db.cdcDelete(null));
        assertNullCdcIdRejected(() -> db.cdcCommit(null, 0));
    }

    @Test
    void hardMaxEventsCapsHugePollAndResumeCompletesWithoutDuplicates() throws RocksDBException {
        String subId = "hard-cap";
        db.cdcCreate(subId, null, List.of(targetCol), false);

        int total = 10_050;
        List<Keys> keys = new ArrayList<>(total);
        List<Buf> values = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            keys.add(key(i));
            values.add(value("v" + i));
        }
        db.putBatch(targetCol, Flux.just(new KVBatch.KVBatchRef(keys, values)), PutBatchMode.WRITE_BATCH);

        CdcBatch first = poll(subId, null, 50_000);
        assertEquals(10_000, first.events().size(), "poll must honor the hard max event cap");
        assertEquals(0, intValue(first.events().getFirst().key()));
        assertEquals(9_999, intValue(first.events().getLast().key()));
        assertStrictlyIncreasing(first.events());

        CdcBatch second = poll(subId, first.nextSeq(), 50_000);
        assertEquals(50, second.events().size(), "resume after the hard cap must return the remaining events");
        assertEquals(10_000, intValue(second.events().getFirst().key()));
        assertEquals(10_049, intValue(second.events().getLast().key()));
        assertTrue(second.events().getFirst().seq() > first.events().getLast().seq());

        CdcBatch exhausted = poll(subId, second.nextSeq(), 10);
        assertTrue(exhausted.events().isEmpty(), "all events should be consumed after the capped resume");
    }

    @Test
    void oversizedFirstEventStillMakesProgressAndNextPollReadsFollowingEvent() throws RocksDBException {
        String subId = "oversized-first";
        db.cdcCreate(subId, null, List.of(targetCol), false);

        byte[] oversized = new byte[(16 * 1024 * 1024) + 1024];
        Arrays.fill(oversized, (byte) 7);
        db.putBatch(targetCol, Flux.just(new KVBatch.KVBatchRef(
                List.of(key(1), key(2)),
                List.of(Buf.wrap(oversized), value("tail"))
        )), PutBatchMode.WRITE_BATCH);

        CdcBatch first = poll(subId, null, 10);
        assertEquals(1, first.events().size(), "oversized first event must be emitted so the stream can progress");
        assertEquals(1, intValue(first.events().getFirst().key()));
        assertEquals(oversized.length, first.events().getFirst().value().size());

        CdcBatch second = poll(subId, first.nextSeq(), 10);
        assertEquals(1, second.events().size(), "next poll must resume after the oversized event");
        assertEvent(second.events().getFirst(), CDCEvent.Op.PUT, 2, "tail");
    }

    @Test
    void resolvedValuesSurviveRapidPutMergeDeletePutChurn() throws RocksDBException {
        long resolvedCol = db.createColumn("resolved-churn", mergeSchema());
        String subId = "resolved-churn";
        db.cdcCreate(subId, null, List.of(resolvedCol), true);

        db.put(0, resolvedCol, key(1), value("one"), RequestType.none());
        db.merge(0, resolvedCol, key(1), value("two"), RequestType.none());
        db.delete(0, resolvedCol, key(1), RequestType.none());
        db.put(0, resolvedCol, key(1), value("final"), RequestType.none());

        CdcBatch batch = poll(subId, null, 10);
        assertEquals(4, batch.events().size(), "all churn operations should be represented");
        assertStrictlyIncreasing(batch.events());
        for (CDCEvent event : batch.events()) {
            assertEvent(event, CDCEvent.Op.PUT, 1, "final");
        }

        CdcBatch exhausted = poll(subId, batch.nextSeq(), 10);
        assertTrue(exhausted.events().isEmpty(), "resolved churn should not replay after nextSeq");
    }

    @Test
    void commitsAreMonotonicEvenIfOlderAckArrivesAfterNewerAck() throws RocksDBException {
        String subId = "commit-monotonic";
        db.cdcCreate(subId, null, List.of(targetCol), false);

        db.put(0, targetCol, key(1), value("one"), RequestType.none());
        db.put(0, targetCol, key(2), value("two"), RequestType.none());
        CdcBatch first = poll(subId, null, 10);
        assertEquals(2, first.events().size());

        long olderSeq = first.events().getFirst().seq();
        long newerSeq = first.events().getLast().seq();
        db.cdcCommit(subId, newerSeq);
        db.cdcCommit(subId, olderSeq);

        db.put(0, targetCol, key(3), value("three"), RequestType.none());
        CdcBatch resumed = poll(subId, null, 10);
        assertEquals(1, resumed.events().size(), "older late commit must not rewind the subscription");
        assertEvent(resumed.events().getFirst(), CDCEvent.Op.PUT, 3, "three");
    }

    @Test
    void deletedColumnBacklogIsSkippedWithoutBlockingLiveColumns() throws RocksDBException {
        long obsoleteCol = db.createColumn("obsolete", fixedSchema());
        String subId = "deleted-column-backlog";
        db.cdcCreate(subId, null, List.of(obsoleteCol, targetCol), false);

        db.put(0, obsoleteCol, key(1), value("obsolete"), RequestType.none());
        db.deleteColumn(obsoleteCol);
        db.put(0, targetCol, key(2), value("live"), RequestType.none());

        CdcBatch batch = poll(subId, null, 10);
        assertEquals(1, batch.events().size(), "dropped column WAL entries must not block later live-column data");
        assertEvent(batch.events().getFirst(), CDCEvent.Op.PUT, 2, "live");
    }

    @Test
    void updatingSubscriptionFilterKeepsCommittedCursor() throws RocksDBException {
        String subId = "filter-update";
        db.cdcCreate(subId, null, List.of(targetCol), false);

        db.put(0, targetCol, key(1), value("target-before"), RequestType.none());
        CdcBatch first = poll(subId, null, 10);
        assertEquals(1, first.events().size());
        db.cdcCommit(subId, first.events().getFirst().seq());

        long restartSeq = db.cdcCreate(subId, null, List.of(noiseCol), false);
        assertTrue(restartSeq > first.events().getFirst().seq(), "filter update must not rewind committed CDC offset");

        db.put(0, targetCol, key(2), value("target-after"), RequestType.none());
        db.put(0, noiseCol, key(3), value("noise-after"), RequestType.none());

        CdcBatch afterFilterChange = poll(subId, null, 10);
        assertEquals(1, afterFilterChange.events().size());
        assertEquals((long) noiseCol, afterFilterChange.events().getFirst().columnId());
        assertEvent(afterFilterChange.events().getFirst(), CDCEvent.Op.PUT, 3, "noise-after");
    }

    @Test
    void zeroAndNegativeMaxEventsUseDefaultLimitAndStillMakeProgress() throws RocksDBException {
        String zeroSub = "zero-max-events";
        db.cdcCreate(zeroSub, null, List.of(targetCol), false);
        db.put(0, targetCol, key(1), value("one"), RequestType.none());
        db.put(0, targetCol, key(2), value("two"), RequestType.none());

        CdcBatch zeroMax = poll(zeroSub, null, 0);
        assertEquals(2, zeroMax.events().size());
        assertEvent(zeroMax.events().getFirst(), CDCEvent.Op.PUT, 1, "one");
        assertEvent(zeroMax.events().getLast(), CDCEvent.Op.PUT, 2, "two");

        String negativeSub = "negative-max-events";
        db.cdcCreate(negativeSub, null, List.of(targetCol), false);
        db.put(0, targetCol, key(3), value("three"), RequestType.none());
        db.put(0, targetCol, key(4), value("four"), RequestType.none());

        CdcBatch negativeMax = poll(negativeSub, null, -100);
        assertEquals(2, negativeMax.events().size());
        assertEvent(negativeMax.events().getFirst(), CDCEvent.Op.PUT, 3, "three");
        assertEvent(negativeMax.events().getLast(), CDCEvent.Op.PUT, 4, "four");
    }

    @Test
    void bucketedResolvedValuesAfterElementChurnExposeCurrentRealKeys() throws RocksDBException {
        long bucketCol = db.createColumn("bucketed-resolved-churn", bucketedSchema());
        String subId = "bucketed-resolved-churn";
        db.cdcCreate(subId, null, List.of(bucketCol), true);

        db.put(0, bucketCol, bucketKey(1, "a"), value("A1"), RequestType.none());
        db.put(0, bucketCol, bucketKey(1, "b"), value("B1"), RequestType.none());
        db.delete(0, bucketCol, bucketKey(1, "a"), RequestType.none());

        CdcBatch batch = poll(subId, null, 10);
        assertEquals(3, batch.events().size(), "each raw bucket update should resolve to the current bucket contents");
        assertStrictlyIncreasing(batch.events());
        for (CDCEvent event : batch.events()) {
            assertBucketEvent(event, 1, "b", "B1");
        }
    }

    @Test
    void concurrentOutOfOrderCommitsCannotRewindSubscription() throws Exception {
        String subId = "concurrent-commit";
        db.cdcCreate(subId, null, List.of(targetCol), false);

        int total = 200;
        List<Keys> keys = new ArrayList<>(total);
        List<Buf> values = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            keys.add(key(i));
            values.add(value("v" + i));
        }
        db.putBatch(targetCol, Flux.just(new KVBatch.KVBatchRef(keys, values)), PutBatchMode.WRITE_BATCH);

        CdcBatch batch = poll(subId, null, total);
        assertEquals(total, batch.events().size());
        List<Long> shuffledSeqs = new ArrayList<>(batch.events().stream().map(CDCEvent::seq).toList());
        Collections.shuffle(shuffledSeqs, new Random(13));

        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>(shuffledSeqs.size());
        try {
            for (Long seq : shuffledSeqs) {
                futures.add(pool.submit(() -> {
                    start.await();
                    db.cdcCommit(subId, seq);
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }

        db.put(0, targetCol, key(total), value("after-commits"), RequestType.none());
        CdcBatch resumed = poll(subId, null, 10);
        assertEquals(1, resumed.events().size(), "concurrent older commits must not overwrite the highest committed seq");
        assertEvent(resumed.events().getFirst(), CDCEvent.Op.PUT, total, "after-commits");
    }

    private CdcBatch poll(String subId, Long fromSeq, long maxEvents) {
        CdcBatch batch = db.cdcPollBatchAsyncInternal(subId, fromSeq, maxEvents).block(Duration.ofSeconds(10));
        assertNotNull(batch);
        return batch;
    }

    private static ColumnSchema fixedSchema() {
        return ColumnSchema.of(IntArrayList.of(4), new ObjectArrayList<>(), true, null, null, null);
    }

    private static ColumnSchema mergeSchema() {
        return ColumnSchema.of(IntArrayList.of(4),
                new ObjectArrayList<>(),
                true,
                null,
                null,
                MyStringAppendOperator.class.getName());
    }

    private static ColumnSchema bucketedSchema() {
        return ColumnSchema.of(IntArrayList.of(4),
                new ObjectArrayList<>(List.of(ColumnHashType.ALLSAME8)),
                true,
                null,
                null,
                null);
    }

    private static Keys key(int value) {
        return new Keys(new Buf[]{Buf.wrap(intBytes(value))});
    }

    private static Keys bucketKey(int fixed, String variable) {
        return new Keys(new Buf[]{Buf.wrap(intBytes(fixed)), value(variable)});
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    private static int intValue(Buf value) {
        return ByteBuffer.wrap(value.toByteArray()).getInt();
    }

    private static Buf value(String value) {
        return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String string(Buf value) {
        return new String(value.toByteArray(), StandardCharsets.UTF_8);
    }

    private static void assertEvent(CDCEvent event, CDCEvent.Op op, int key, String value) {
        assertEquals(op, event.op());
        assertEquals(key, intValue(event.key()));
        assertEquals(value, string(event.value()));
    }

    private static void assertBucketEvent(CDCEvent event, int fixedKey, String variableKey, String value) {
        assertEquals(CDCEvent.Op.PUT, event.op());
        byte[] key = event.key().toByteArray();
        assertTrue(key.length >= Integer.BYTES);
        assertEquals(fixedKey, ByteBuffer.wrap(key, 0, Integer.BYTES).getInt());
        assertEquals(variableKey, new String(key, Integer.BYTES, key.length - Integer.BYTES, StandardCharsets.UTF_8));
        assertEquals(value, string(event.value()));
    }

    private static void assertStrictlyIncreasing(List<CDCEvent> events) {
        for (int i = 1; i < events.size(); i++) {
            assertTrue(events.get(i).seq() > events.get(i - 1).seq(),
                    "CDC sequence must strictly increase at index " + i);
        }
    }

    private static void assertNullCdcIdRejected(Executable executable) {
        var error = assertThrows(IllegalArgumentException.class, executable);
        assertEquals("id must not be null", error.getMessage());
    }
}
