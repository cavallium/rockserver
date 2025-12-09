package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

/**
 * High‑coverage leak tests exercising complex/edge scenarios to catch subtle leaks.
 */
public class ComplexLeakScenariosTest {

    private EmbeddedConnection db;
    private long colId;
    private Path configFile;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("it.cavallium.rockserver.leakdetection", "true");
        System.setProperty("rockserver.core.print-config", "false");

        configFile = Files.createTempFile("leak-complex", ".conf");
        Files.writeString(configFile, """
database: {
  global: {
    ingest-behind: true
    fallback-column-options: {
      merge-operator-class: "it.cavallium.rockserver.core.impl.MyStringAppendOperator"
    }
  }
}
""");
        db = new EmbeddedConnection(null, "complex-leaks", configFile);
        colId = db.getSyncApi().createColumn("col", ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
        if (configFile != null) Files.deleteIfExists(configFile);
    }

    @Test
    void forUpdate_changed_and_unchanged_paths_have_no_leaks() {
        var internal = db.getInternalDB();
        Random rnd = new Random(7);
        for (int i = 0; i < 200; i++) {
            var key = new Keys(new Buf[]{Buf.wrap(intKey(i))});
            // Start update flow
            var ctx = db.getSyncApi().get(0, colId, key, RequestType.forUpdate());
            long updateId = ctx.updateId();
            assertTrue(updateId != 0, "Expected non-zero updateId");
            boolean change = rnd.nextBoolean();
            if (change) {
                // Perform a real change
                db.getSyncApi().put(updateId, colId, key, Buf.wrap(new byte[]{(byte) i}), RequestType.none());
            } else {
                // No change -> ensure we close the failed update
                db.getSyncApi().closeFailedUpdate(updateId);
            }
        }
        // Give async bits a moment
        sleep(50);
        assertEquals(0, internal.getPendingOpsCount(), "No pending ops after forUpdate flows");
        assertEquals(0, internal.getOpenTransactionsCount(), "No open tx after forUpdate flows");
        assertEquals(0, internal.getOpenIteratorsCount(), "No open iterators after forUpdate flows");
    }

    @Test
    void many_concurrent_ranges_with_random_cancellation_have_no_leaks() {
        List<Disposable> subs = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            var pub = db.getRangeAsync(0, colId, null, null, (i % 2) == 0, RequestType.allInRange(), Duration.ofSeconds(5).toMillis());
            var sub = Flux.from(pub).take(1 + (i % 4)).subscribe();
            subs.add(sub);
        }
        // Randomly cancel
        ThreadLocalRandom.current().ints(0, subs.size()).distinct().limit(16).forEach(idx -> subs.get(idx).dispose());
        // Let others complete or be disposed
        sleep(100);
        var internal = db.getInternalDB();
        assertEquals(0, internal.getOpenIteratorsCount(), "No iterators left after concurrent ranges");
        assertEquals(0, internal.getPendingOpsCount(), "No pending ops after concurrent ranges");
        assertEquals(0, internal.getOpenTransactionsCount(), "No open tx after concurrent ranges");
    }

    @Test
    void expired_tx_and_iterators_are_cleaned_up_without_waiting_scheduler() {
        var internal = db.getInternalDB();
        // Open a tx with tiny timeout so it expires immediately
        long txId = db.getSyncApi().openTransaction(1);
        // Open an iterator with tiny timeout
        var key = new Keys(new Buf[]{Buf.wrap(intKey(1))});
        long itId = db.getSyncApi().openIterator(txId, colId, key, null, false, 1);
        // Do not close them. Let them expire, then run immediate cleanup.
        sleep(5);
        invokeCleanup(internal);
        // Verify no open resources remain
        assertEquals(0, internal.getOpenTransactionsCount(), "Expired tx must be cleaned");
        assertEquals(0, internal.getOpenIteratorsCount(), "Expired iterators must be cleaned");
        assertEquals(0, internal.getPendingOpsCount(), "No pending ops after cleanup");
    }

    @Test
    void batch_publishers_that_error_do_not_leak() {
        // Prepare a failing publisher that errors after emitting a few items
        java.util.ArrayList<Keys> goodKeys = new java.util.ArrayList<>();
        goodKeys.add(new Keys(new Buf[]{Buf.wrap(intKey(1))}));
        var goodVals = java.util.List.of(Buf.wrap(new byte[]{1}));
        var good = new KVBatch.KVBatchRef(goodKeys, goodVals);

        // Use Reactor to avoid re-entrant recursive onNext/request loops
        var failing = (org.reactivestreams.Publisher<KVBatch>)
                Flux.<KVBatch>just(good)
                        .concatWith(Flux.error(new RuntimeException("boom")));

        try {
            db.getSyncApi().putBatch(colId, failing, PutBatchMode.WRITE_BATCH);
            fail("Expected exception from failing publisher");
        } catch (Exception expected) {
            // ok
        }

        sleep(50);
        var internal = db.getInternalDB();
        assertEquals(0, internal.getPendingOpsCount(), "No pending ops after failing putBatch");
        assertEquals(0, internal.getOpenTransactionsCount(), "No open tx after failing putBatch");
        assertEquals(0, internal.getOpenIteratorsCount(), "No open iterators after failing putBatch");
    }

    private static byte[] intKey(int v) {
        return new byte[]{(byte) (v & 0xFF), (byte) ((v >> 8) & 0xFF), (byte) ((v >> 16) & 0xFF), (byte) ((v >> 24) & 0xFF)};
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { }
    }

    private static void invokeCleanup(EmbeddedDB internal) {
        try {
            // Call test helpers directly (package access via same module)
            java.lang.reflect.Method m1 = EmbeddedDB.class.getDeclaredMethod("cleanupExpiredTransactionsNow");
            java.lang.reflect.Method m2 = EmbeddedDB.class.getDeclaredMethod("cleanupExpiredIteratorsNow");
            m1.setAccessible(true);
            m2.setAccessible(true);
            m1.invoke(internal);
            m2.invoke(internal);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
