package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.*;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.impl.ColumnInstance;
import it.cavallium.rockserver.core.impl.StartupWriteCompletions;
import it.cavallium.rockserver.core.impl.rocksdb.LeakSafeWriteBatch;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import it.cavallium.rockserver.core.impl.rocksdb.SSTWriter;
import it.cavallium.rockserver.core.impl.rocksdb.WB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

class StartupWriteFailureTest {
    @TempDir Path directory;
    @Test void actualReadOnlyBatchWriteAndMissingFileIngestAttemptsPublishFailureAndReleaseActivity() throws Exception {
        RocksDBLoader.loadLibrary();
        Path path = directory.resolve("db");
        try (var options = new Options().setCreateIfMissing(true)) {
            try (var created = RocksDB.open(options, path.toString())) { created.put(new byte[]{1}, new byte[]{2}); }
            try (var readOnly = RocksDB.openReadOnly(options, path.toString())) {
                var writes = new StartupWriteCompletions();
                try (var batch = new WB(readOnly, new LeakSafeWriteBatch(), false, writes)) {
                    batch.wb().put(new byte[]{1}, new byte[]{3});
                    assertEquals(0, writes.snapshot(1).completed(), "batch serialization is not storage completion");
                    assertThrows(it.cavallium.rockserver.core.common.RocksDBException.class, batch::writePending);
                }
                var batchFailure = writes.snapshot(2);
                assertEquals(1, batchFailure.calls().count()); assertEquals(1, batchFailure.calls().failures());
                assertEquals(0, batchFailure.active());
                // Invoke the exact real native ingestion boundary with an absent source, independently of SST serialization.
                var column = new ColumnInstance(readOnly.getDefaultColumnFamily(), ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
                var invocation = new SSTWriter(readOnly, column, directory.resolve("absent.sst"), null, false, null, writes);
                var ingest = SSTWriter.class.getDeclaredMethod("ingest", boolean.class); ingest.setAccessible(true);
                var failure = assertThrows(InvocationTargetException.class, () -> ingest.invoke(invocation, false));
                assertInstanceOf(org.rocksdb.RocksDBException.class, failure.getCause());
                var ingestFailure = writes.snapshot(3);
                assertEquals(1, ingestFailure.calls().count()); assertEquals(1, ingestFailure.calls().failures());
                assertEquals(0, ingestFailure.active());
            }
        }
    }
}
