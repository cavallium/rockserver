package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.cdc.CdcCommitMode;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.buffer.Buf;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CdcAutocommitTest {
    @Test
    void testCrashAndResume() throws Exception {
        Path dbDir = Files.createTempDirectory("cdc-autocommit");
        Path configFile = Files.createTempFile("rockserver", ".conf");
        Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");

        try (var db = new EmbeddedConnection(dbDir, "cdc-autocommit", configFile)) {
            var colId = db.getSyncApi().createColumn("test", ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
            var subId = "sub1";
            long startSeq = db.getSyncApi().cdcCreate(subId, null, null);

            // Put 10 items
            for (int i = 0; i < 10; i++) {
                db.getSyncApi().put(0, colId, 
                    new Keys(new Buf[]{Buf.wrap(new byte[]{0,0,0,(byte)i})}), 
                    Buf.wrap("val".getBytes()), 
                    RequestType.none());
            }

            // Consume and crash
            AtomicInteger processed = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(1);
            java.util.concurrent.atomic.AtomicReference<Long> firstEventSeq = new java.util.concurrent.atomic.AtomicReference<>();
            java.util.concurrent.atomic.AtomicReference<Long> lastProcessedSeq = new java.util.concurrent.atomic.AtomicReference<>();

            db.getAsyncApi().cdcStream(subId, startSeq, 10, Duration.ofMillis(10), CdcCommitMode.PER_EVENT, ev -> {
                int p = processed.incrementAndGet();
                if (p == 1) firstEventSeq.set(ev.seq());
                if (p == 5) {
                    lastProcessedSeq.set(ev.seq());
                    latch.countDown();
                    return Mono.error(new RuntimeException("Crash"));
                }
                return Mono.empty();
            }).subscribe(
                    _ -> {},
                    err -> { /* Expected error */ }
            );

            latch.await(5, TimeUnit.SECONDS);
            Thread.sleep(100); // Wait for potential async commits

            // Resume
            List<CDCEvent> resumed = db.getAsyncApi().cdcStream(subId, null, 10, Duration.ofMillis(10))
                .take(1)
                .collectList()
                .block(Duration.ofSeconds(5));

            Assertions.assertNotNull(resumed);
            Assertions.assertFalse(resumed.isEmpty());
            long resumedSeq = resumed.get(0).seq();
            
            System.out.println("[DEBUG_LOG] StartSeq: " + startSeq + " FirstEvent: " + firstEventSeq.get() + " LastProcessed: " + lastProcessedSeq.get() + " ResumedSeq: " + resumedSeq);
            
            // To ensure "perfect" autocommit with no duplicates after crash,
            // we should have committed at least the ones we processed successfully?
            // If we processed 1..4 successfully, and 5 crashed.
            // We expect to resume at 5 (or 6 if 5 is considered "failed but consumed" - no, it failed).
            // Actually, if 5 failed, we probably want to retry 5.
            // But we DEFINITELY don't want to retry 1..4.
            // So resumedSeq should be > firstEventSeq.get().
            // Ideally resumedSeq should be close to lastProcessedSeq.
            
            // With current batch implementation, resumedSeq == firstEventSeq.get().
            // This test asserts the DESIRED behavior (No Duplicates).
            Assertions.assertTrue(resumedSeq > firstEventSeq.get(), 
                "Received duplicate events! Resumed at " + resumedSeq + " but already processed starting from " + firstEventSeq.get());
        } finally {
            // cleanup
        }
    }
}
