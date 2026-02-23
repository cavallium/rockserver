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
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deterministic leak tests to ensure operations do not leave pending ops,
 * open transactions, or iterators behind. Uses in-memory DB.
 */
public class LeakDetectionTest {

    private EmbeddedConnection db;
    private long colId;
    private Path configFile;

    @BeforeEach
    void setUp() throws Exception {
        // Reduce noise and enable leak detection logs in tests
        System.setProperty("it.cavallium.rockserver.leakdetection", "true");
        System.setProperty("rockserver.core.print-config", "false");

        configFile = Files.createTempFile("leak-test", ".conf");
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
        db = new EmbeddedConnection(null, "leak-tests", configFile);
        colId = db.getSyncApi().createColumn("leak-col", ColumnSchema.of(
                IntList.of(Integer.BYTES),
                ObjectList.of(),
                true
        ));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.closeTesting();
        if (configFile != null) Files.deleteIfExists(configFile);
    }

    @Test
    void testNoPendingResourcesAfterMixedOperations() {
        // Basic put/get
        var k = new Keys(new Buf[]{Buf.wrap(new byte[]{1, 2, 3, 4})});
        db.getSyncApi().put(0, colId, k, Buf.wrap("a".getBytes()), RequestType.none());
        var v = db.getSyncApi().get(0, colId, k, RequestType.current());
        assertNotNull(v);

        // Explicit transaction with commit
        long txId = db.getSyncApi().openTransaction(5_000);
        db.getSyncApi().put(txId, colId, k, Buf.wrap("b".getBytes()), RequestType.none());
        assertTrue(db.getSyncApi().closeTransaction(txId, true));

        // Explicit transaction with rollback
        long txId2 = db.getSyncApi().openTransaction(5_000);
        db.getSyncApi().put(txId2, colId, k, Buf.wrap("c".getBytes()), RequestType.none());
        assertTrue(db.getSyncApi().closeTransaction(txId2, false));

        // Merge and read back
        db.getSyncApi().merge(0, colId, k, Buf.wrap("+d".getBytes()), RequestType.none());
        var merged = db.getSyncApi().get(0, colId, k, RequestType.current());
        assertNotNull(merged);

        // Range scan with cancellation (iterator should be closed)
        var pub = db.getRangeAsync(0, colId, null, null, false, RequestType.allInRange(), Duration.ofSeconds(5).toMillis());
        var sub = reactor.core.publisher.Flux.from(pub).take(1).subscribe();
        sub.dispose();

        // Access internal counts
        var internal = db.getInternalDB();
        // Allow async cleanup to run
        sleep(50);
        assertEquals(0, internal.getPendingOpsCount(), "Pending ops should be zero after operations");
        assertEquals(0, internal.getOpenTransactionsCount(), "No open transactions expected");
        assertEquals(0, internal.getOpenIteratorsCount(), "No open iterators expected");
    }

    @Test
    void testRangeCancellationAndIteratorClose() {
        // Populate some rows
        for (int i = 0; i < 100; i++) {
            var key = new Keys(new Buf[]{Buf.wrap(new byte[]{(byte) i, 0, 0, 0})});
            db.getSyncApi().put(0, colId, key, Buf.wrap(new byte[]{(byte) i}), RequestType.none());
        }
        // Start range and cancel after a few elements
        var pub = db.getRangeAsync(0, colId, null, null, false, RequestType.allInRange(), Duration.ofSeconds(10).toMillis());
        var disposable = reactor.core.publisher.Flux.from(pub).take(3).subscribe();
        disposable.dispose();
        var internal = db.getInternalDB();
        sleep(50);
        assertEquals(0, internal.getOpenIteratorsCount(), "Iterator should be closed after cancellation");
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { }
    }
}
