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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Randomized stress test performing a very wide variety of operations to detect native leaks.
 * Runs quickly by default; extend duration by setting -Drockserver.test.stress.seconds.
 *
 * Coverage highlights:
 * - plain put/get/merge
 * - explicit transactions (commit/rollback) and tiny-timeout transactions that expire
 * - forUpdate flows with both changed and unchanged paths (closeFailedUpdate)
 * - getRange async streams with bounds, reverse order, and random cancellations
 * - explicit openIterator/closeIterator flows and occasionally intentional no-close + forced cleanup
 * - batch put/merge with successful publisher and failing publisher
 * - reduceRange entriesCount and firstAndLast
 * - periodic flush/compact
 */
public class RandomizedLeakStressTest {

    private EmbeddedConnection db;
    private long colId;
    private Path configFile;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("it.cavallium.rockserver.leakdetection", "true");
        System.setProperty("rockserver.core.print-config", "false");

        configFile = Files.createTempFile("leak-rand", ".conf");
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
        db = new EmbeddedConnection(null, "rnd-leaks", configFile);
        colId = db.getSyncApi().createColumn("rnd-col", ColumnSchema.of(
                IntList.of(Integer.BYTES), ObjectList.of(), true));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.closeTesting();
        if (configFile != null) Files.deleteIfExists(configFile);
    }

    @Test
    void randomizedWorkloadHasNoLeaks() {
        int seconds = Integer.getInteger("rockserver.test.stress.seconds", 6);
        long endAt = System.currentTimeMillis() + seconds * 1000L;
        Random rnd = new Random(12345);

        int keySpace = 256;

        // Keep a few range subscriptions to cancel later
        java.util.List<reactor.core.Disposable> liveRanges = new java.util.ArrayList<>();
        // Keep a few explicitly opened iterators (ids) to close later
        java.util.List<Long> openIterators = new java.util.ArrayList<>();

        while (System.currentTimeMillis() < endAt) {
            int op = rnd.nextInt(14);
            switch (op) {
                case 0 -> doPut(rnd, keySpace);
                case 1 -> doMerge(rnd, keySpace);
                case 2 -> doGet(rnd, keySpace);
                case 3 -> doTxCommit(rnd, keySpace);
                case 4 -> doTxRollback(rnd, keySpace);
                case 5 -> doRangeCancel(rnd);
                case 6 -> doForUpdateFlow(rnd, keySpace);
                case 7 -> startConcurrentRange(rnd, liveRanges);
                case 8 -> maybeCancelARange(rnd, liveRanges);
                case 9 -> openIteratorShortLived(rnd, openIterators);
                case 10 -> maybeCloseAnIterator(rnd, openIterators);
                case 11 -> doReduceRange(rnd);
                case 12 -> maybeFlush();
                case 13 -> maybeCompact();
            }

            // Occasionally simulate expiring tx + forced cleanup
            if ((rnd.nextInt(100)) == 0) {
                openExpiringTxAndIteratorThenCleanup();
            }
        }

        // Allow async cleanup to finish
        sleep(100);

        // Cancel leftover ranges and close outstanding iterators, then force cleanup
        for (var d : liveRanges) {
            try { d.dispose(); } catch (Throwable ignored) {}
        }
        for (Long itId : openIterators) {
            try { db.getSyncApi().closeIterator(itId); } catch (Throwable ignored) {}
        }
        // Deterministic cleanup of any expiring items
        invokeCleanup(getInternal(db));

        var internal = getInternal(db);
        assertEquals(0, internal.getPendingOpsCount(), "Pending ops must be zero after randomized run");
        assertEquals(0, internal.getOpenTransactionsCount(), "Open transactions must be zero after randomized run");
        assertEquals(0, internal.getOpenIteratorsCount(), "Open iterators must be zero after randomized run");

        // Explicitly close DB
        try { db.closeTesting(); db = null; } catch (Exception e) { e.printStackTrace(); }

        // Wait for background threads to die
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            boolean clean = true;
            for (Thread t : Thread.getAllStackTraces().keySet()) {
                String name = t.getName();
                if (name.contains("db-") || name.contains("rocksdb") || name.contains("influx")) {
                    clean = false;
                    break;
                }
            }
            if (clean) break;
            try { Thread.sleep(100); } catch (InterruptedException e) { break; }
        }

        // Final verification
        StringBuilder err = new StringBuilder();
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            String name = t.getName();
            if (t.isAlive() && (name.contains("db-") || name.contains("rocksdb") || name.contains("influx"))) {
                err.append("Leaked thread: ").append(name).append(" (Daemon=").append(t.isDaemon()).append(")\n");
            }
            if (!t.isDaemon() && !name.equals("main") && !name.startsWith("Test worker") && !name.startsWith("junit")) {
                 // Note: In IDE/Maven, test runner threads might be non-daemon. Be careful failing on them.
                 // But we should ensure *our* threads are gone.
            }
        }
        if (err.length() > 0) {
            throw new RuntimeException("Threads leaked:\n" + err);
        }
    }

    private void doPut(Random rnd, int keySpace) {
        var key = new Keys(new Buf[]{Buf.wrap(intKey(rnd.nextInt(keySpace))) });
        var val = Buf.wrap(new byte[]{(byte) rnd.nextInt(256)});
        db.getSyncApi().put(0, colId, key, val, RequestType.none());
    }

    private void doMerge(Random rnd, int keySpace) {
        var key = new Keys(new Buf[]{Buf.wrap(intKey(rnd.nextInt(keySpace))) });
        var val = Buf.wrap(("+" + (char) ('a' + rnd.nextInt(26))).getBytes());
        db.getSyncApi().merge(0, colId, key, val, RequestType.none());
    }

    private void doGet(Random rnd, int keySpace) {
        var key = new Keys(new Buf[]{Buf.wrap(intKey(rnd.nextInt(keySpace))) });
        db.getSyncApi().get(0, colId, key, RequestType.current());
    }

    private void doTxCommit(Random rnd, int keySpace) {
        var key = new Keys(new Buf[]{Buf.wrap(intKey(rnd.nextInt(keySpace))) });
        long tx = db.getSyncApi().openTransaction(5_000);
        db.getSyncApi().put(tx, colId, key, Buf.wrap(new byte[]{(byte) rnd.nextInt(256)}), RequestType.none());
        assertTrue(db.getSyncApi().closeTransaction(tx, true));
    }

    private void doTxRollback(Random rnd, int keySpace) {
        var key = new Keys(new Buf[]{Buf.wrap(intKey(rnd.nextInt(keySpace))) });
        long tx = db.getSyncApi().openTransaction(5_000);
        db.getSyncApi().put(tx, colId, key, Buf.wrap(new byte[]{(byte) rnd.nextInt(256)}), RequestType.none());
        assertTrue(db.getSyncApi().closeTransaction(tx, false));
    }

    private void doRangeCancel(Random rnd) {
        var pub = db.getRangeAsync(0, colId, null, null, rnd.nextBoolean(), RequestType.allInRange(), Duration.ofSeconds(5).toMillis());
        var d = reactor.core.publisher.Flux.from(pub).take(1 + rnd.nextInt(5)).subscribe();
        d.dispose();
    }

    private void doForUpdateFlow(Random rnd, int keySpace) {
        var key = new Keys(new Buf[]{Buf.wrap(intKey(rnd.nextInt(keySpace))) });
        var ctx = db.getSyncApi().get(0, colId, key, RequestType.forUpdate());
        long updateId = ctx.updateId();
        if (rnd.nextBoolean()) {
            // change path
            db.getSyncApi().put(updateId, colId, key, Buf.wrap(new byte[]{(byte) rnd.nextInt(256)}), RequestType.none());
        } else {
            // unchanged path
            db.getSyncApi().closeFailedUpdate(updateId);
        }
    }

    private void startConcurrentRange(Random rnd, java.util.List<reactor.core.Disposable> subs) {
        // Random lower/upper bounds
        Integer from = rnd.nextBoolean() ? rnd.nextInt(256) : null;
        Integer to = rnd.nextBoolean() ? rnd.nextInt(256) : null;
        Buf fromBuf = from != null ? Buf.wrap(intKey(from)) : null;
        Buf toBuf = to != null ? Buf.wrap(intKey(to)) : null;
        Keys start = (fromBuf != null) ? new Keys(new Buf[]{fromBuf}) : null;
        Keys end = (toBuf != null) ? new Keys(new Buf[]{toBuf}) : null;
        boolean reverse = rnd.nextBoolean();
        var pub = db.getRangeAsync(0, colId, start, end, reverse, RequestType.allInRange(), Duration.ofSeconds(10).toMillis());
        var sub = reactor.core.publisher.Flux.from(pub).take(1 + rnd.nextInt(8)).subscribe();
        subs.add(sub);
    }

    private void maybeCancelARange(Random rnd, java.util.List<reactor.core.Disposable> subs) {
        if (subs.isEmpty()) return;
        int idx = rnd.nextInt(subs.size());
        try { subs.get(idx).dispose(); } catch (Throwable ignored) {}
    }

    private void openIteratorShortLived(Random rnd, java.util.List<Long> openIterators) {
        try {
            var start = new Keys(new Buf[]{Buf.wrap(intKey(rnd.nextInt(256))) });
            long it = db.getSyncApi().openIterator(0, colId, start, null, rnd.nextBoolean(), 5); // 5ms timeout
            // sometimes keep it open to expire
            if (rnd.nextBoolean()) {
                openIterators.add(it);
            } else {
                db.getSyncApi().closeIterator(it);
            }
        } catch (Exception ignored) {
            // ignore expected errors if iterator expired immediately
        }
    }

    private void maybeCloseAnIterator(Random rnd, java.util.List<Long> openIterators) {
        if (openIterators.isEmpty()) return;
        int idx = rnd.nextInt(openIterators.size());
        Long id = openIterators.remove(idx);
        try { db.getSyncApi().closeIterator(id); } catch (Throwable ignored) {}
    }

    private void doReduceRange(Random rnd) {
        boolean reverse = rnd.nextBoolean();
        long timeout = Duration.ofSeconds(5).toMillis();
        if (rnd.nextBoolean()) {
            db.getSyncApi().reduceRange(0, colId, null, null, reverse, RequestType.entriesCount(), timeout);
        } else {
            db.getSyncApi().reduceRange(0, colId, null, null, reverse, RequestType.firstAndLast(), timeout);
        }
    }

    private void maybeFlush() {
        // Best-effort flush; should not leak resources
        try { db.getSyncApi().flush(); } catch (Throwable ignored) {}
    }

    private void maybeCompact() {
        // Best-effort compact; should not leak resources
        try { db.getSyncApi().compact(); } catch (Throwable ignored) {}
    }

    private void openExpiringTxAndIteratorThenCleanup() {
        try {
            long tx = db.getSyncApi().openTransaction(1);
            var key = new Keys(new Buf[]{Buf.wrap(intKey(1))});
            try { db.getSyncApi().openIterator(tx, colId, key, null, false, 1); } catch (Throwable ignored) {}
        } catch (Throwable ignored) {}
        // Sleep a moment, then run deterministic cleanup
        sleep(5);
        invokeCleanup(getInternal(db));
    }

    private static byte[] intKey(int v) {
        return new byte[] { (byte) (v & 0xFF), (byte) ((v >> 8) & 0xFF), (byte) ((v >> 16) & 0xFF), (byte) ((v >> 24) & 0xFF)};
    }

    private static EmbeddedDB getInternal(EmbeddedConnection c) { return c.getInternalDB(); }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { }
    }

    private static void invokeCleanup(EmbeddedDB internal) {
        try {
            java.lang.reflect.Method m1 = EmbeddedDB.class.getDeclaredMethod("cleanupExpiredTransactionsNow");
            java.lang.reflect.Method m2 = EmbeddedDB.class.getDeclaredMethod("cleanupExpiredIteratorsNow");
            m1.setAccessible(true);
            m2.setAccessible(true);
            m1.invoke(internal);
            m2.invoke(internal);
        } catch (Exception e) {
            // Testing helper; ignore if not present
        }
    }
}
