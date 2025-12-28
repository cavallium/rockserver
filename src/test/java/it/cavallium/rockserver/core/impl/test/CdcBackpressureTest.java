package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.cdc.CdcStreamOptions;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class CdcBackpressureTest {

    @TempDir
    Path tempDir;

    private void setupAndRun(String testName, TestRunnable runnable) throws Exception {
        try (EmbeddedConnection db = new EmbeddedConnection(tempDir.resolve(testName), testName, null)) {
             runnable.run(db);
        }
    }

    interface TestRunnable {
        void run(EmbeddedConnection db) throws Exception;
    }

    // Helper to create column and subscription
    private long setupCdc(EmbeddedConnection db, String subId) {
        var schema = ColumnSchema.of(IntList.of(4), new ObjectArrayList<>(), true, null, null, "it.cavallium.rockserver.examples.MessagePatchMergeOperator");
        long colId = db.createColumn("messages", schema);
        try {
            db.getSyncApi().cdcCreate(subId, null, List.of(colId));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return colId;
    }

  @Timeout(10)
    @Test
    public void testDbFastCdcSlow() throws Exception {
        setupAndRun("testDbFastCdcSlow", db -> {
            String subId = "sub1";
            long colId = setupCdc(db, subId);
            var api = db.getAsyncApi();

            int itemCount = 500;
            Flux<KVBatch> source = Flux.range(0, itemCount)
                    .map(this::createBatch);

            // Fast producer
            Mono.fromFuture(api.mergeBatchAsync(colId, source, MergeBatchMode.MERGE_WRITE_BATCH)).subscribe();

            AtomicInteger received = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(itemCount);

            // Slow consumer: limit rate
            api.cdcStream(subId, new CdcStreamOptions(null, 10, Duration.ofMillis(100), null), null)
                    .delayElements(Duration.ofMillis(10)) // Slow consumption
                    .doOnNext(e -> {
                        received.incrementAndGet();
                        latch.countDown();
                    })
                    .take(itemCount)
                    .blockLast(Duration.ofSeconds(60));

            Assertions.assertEquals(itemCount, received.get());
        });
    }

  @Timeout(10)
    @Test
    public void testDbSlowCdcFast() throws Exception {
        setupAndRun("testDbSlowCdcFast", db -> {
            String subId = "sub2";
            long colId = setupCdc(db, subId);
            var api = db.getAsyncApi();

            int itemCount = 10;
            // Slow producer
            Flux<KVBatch> source = Flux.range(0, itemCount)
                    .delayElements(Duration.ofMillis(200))
                    .map(this::createBatch);

            Mono.fromFuture(api.mergeBatchAsync(colId, source, MergeBatchMode.MERGE_WRITE_BATCH)).subscribe();

            AtomicInteger received = new AtomicInteger(0);
            
            // Fast consumer (normal stream)
            List<Long> seqs = api.cdcStream(subId, new CdcStreamOptions(null, 10, Duration.ofMillis(50), null), null)
                    .doOnNext(e -> received.incrementAndGet())
                    .take(itemCount)
                    .map(e -> e.seq())
                    .collectList()
                    .block(Duration.ofSeconds(30));

            Assertions.assertEquals(itemCount, received.get());
        });
    }

  @Timeout(10)
    @Test
    public void testDbFastCdcLagging() throws Exception {
         setupAndRun("testDbFastCdcLagging", db -> {
            String subId = "sub3";
            long colId = setupCdc(db, subId);
            var api = db.getAsyncApi();

            int itemCount = 1000;
            // Burst producer
            Flux<KVBatch> source = Flux.range(0, itemCount).map(this::createBatch);
            Mono.fromFuture(api.mergeBatchAsync(colId, source, MergeBatchMode.MERGE_WRITE_BATCH)).block(); // Wait for completion

            // CDC starts LATER
            AtomicInteger received = new AtomicInteger(0);
            
            Long count = api.cdcStream(subId, new CdcStreamOptions(null, 100, Duration.ofMillis(10), null), null)
                    .take(itemCount)
                    .count()
                    .block(Duration.ofSeconds(10));

            Assertions.assertEquals(itemCount, count);
        });
    }

  @Timeout(10)
    @Test
    public void testThroughput() throws Exception {
         setupAndRun("testThroughput", db -> {
            String subId = "sub4";
            long colId = setupCdc(db, subId);
            var api = db.getAsyncApi();

            int itemCount = 5000;
            Flux<KVBatch> source = Flux.range(0, itemCount).map(this::createBatch);
            
            Mono.fromFuture(api.mergeBatchAsync(colId, source, MergeBatchMode.MERGE_WRITE_BATCH)).subscribe();

            Long count = api.cdcStream(subId, new CdcStreamOptions(null, 1000, Duration.ofMillis(10), null), null)
                    .take(itemCount)
                    .count()
                    .block(Duration.ofSeconds(30));
             
            Assertions.assertEquals(itemCount, count);
        });
    }

    private KVBatch createBatch(int i) {
        Buf key = Buf.wrap(ByteBuffer.allocate(4).putInt(i).array());
        Buf val = Buf.wrap("val".getBytes(StandardCharsets.UTF_8));
        return new KVBatch.KVBatchRef(Collections.singletonList(new Keys(new Buf[]{key})), Collections.singletonList(val));
    }
}
