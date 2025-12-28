package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.cdc.CdcStreamOptions;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

public class ReproduceCdcMergeBugTest {

    @TempDir
    Path tempDir;

    @Timeout(10)
    @Test
    public void testCdcReceiveFromMergeBatch() throws Exception {
        // Setup DB
        try (EmbeddedConnection db = new EmbeddedConnection(tempDir, "test-db", null)) {
            var api = db.getAsyncApi();
            
            // Create column
            var schema = ColumnSchema.of(IntList.of(4), new ObjectArrayList<>(), true, null, null, "it.cavallium.rockserver.examples.MessagePatchMergeOperator");
            long colId = db.createColumn("messages", schema);

            // Create CDC subscription
            String subId = "test-sub";
            db.cdcCreate(subId, null, List.of(colId));

            // Prepare stream of batches
            Sinks.Many<KVBatch> sink = Sinks.many().multicast().onBackpressureBuffer(2000);
            
            // Start mergeBatchAsync
            api.mergeBatchAsync(colId, sink.asFlux(), MergeBatchMode.MERGE_WRITE_BATCH)
                    .exceptionally(e -> {
                        e.printStackTrace();
                        return null;
                    });

            // Start CDC listener
            int totalItems = 1000;
            AtomicInteger receivedCount = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(totalItems);

            var cdcDisposable = api.cdcStream(subId, new CdcStreamOptions(null, 100, Duration.ofMillis(10), null), null)
                    .doOnNext(event -> {
                        receivedCount.incrementAndGet();
                        latch.countDown();
                    })
                    .subscribe();

            // Emit batches
            for (int i = 0; i < totalItems; i++) {
                int id = i;
                Buf key = Buf.wrap(ByteBuffer.allocate(4).putInt(id).array());
                Buf val = Buf.wrap("dummy".getBytes(StandardCharsets.UTF_8));
                
                List<Keys> keys = List.of(new Keys(new Buf[]{key}));
                List<Buf> values = List.of(val);
                
                var res = sink.tryEmitNext(new KVBatch.KVBatchRef(keys, values));
                if (res.isFailure()) {
                     throw new RuntimeException("Emit failed: " + res);
                }
            }

            // Wait for CDC
            boolean received = latch.await(30, TimeUnit.SECONDS);
            
            cdcDisposable.dispose();
            sink.tryEmitComplete();

            Assertions.assertTrue(received, "CDC should receive " + totalItems + " events, but got " + receivedCount.get());
        }
    }
}
