package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

public class EmbeddedDBRobustColumnTest {

    @TempDir
    Path tempDir;

    private EmbeddedDB db;

    @BeforeEach
    public void setUp() throws IOException {
        // Ensure we start with a fresh DB for each test unless specified otherwise
        db = new EmbeddedDB(tempDir, "robust_test", null);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (db != null) {
            db.closeTesting();
        }
    }

    @Test
    public void testSchemaValidation() {
        String colName = "SchemaTest";
        ColumnSchema schema1 = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
        ColumnSchema schema2 = ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true);

        long id1 = db.createColumn(colName, schema1);
        
        // Same schema -> should return same ID (idempotent)
        long id2 = db.createColumn(colName, schema1);
        assertEquals(id1, id2);

        // Different schema -> should fail
        RocksDBException ex = assertThrows(RocksDBException.class, () -> db.createColumn(colName, schema2));
        assertEquals(RocksDBErrorType.COLUMN_EXISTS, ex.getErrorUniqueId());
    }

    @Test
    public void testDeletionAndRecreation() {
        String colName = "DeleteRecreate";
        ColumnSchema schema = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);

        long id1 = db.createColumn(colName, schema);
        long fetchedId1 = db.getColumnId(colName);
        assertEquals(id1, fetchedId1);

        db.deleteColumn(id1);

        // Should not be found
        RocksDBException ex = assertThrows(RocksDBException.class, () -> db.getColumnId(colName));
        assertEquals(RocksDBErrorType.COLUMN_NOT_FOUND, ex.getErrorUniqueId());

        // Recreate
        long id2 = db.createColumn(colName, schema);
        long fetchedId2 = db.getColumnId(colName);
        assertEquals(id2, fetchedId2);
        
        // We ensure the new ID is valid.
        assertTrue(id2 > 0);
    }

    @Test
    public void testConcurrentCreationSameColumn() throws InterruptedException {
        int threads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        String colName = "ConcurrentCol";
        ColumnSchema schema = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);

        List<Callable<Long>> tasks = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            tasks.add(() -> db.createColumn(colName, schema));
        }

        List<Future<Long>> results = executor.invokeAll(tasks);
        
        Long firstId = null;
        for (Future<Long> result : results) {
            try {
                long id = result.get();
                if (firstId == null) {
                    firstId = id;
                } else {
                    assertEquals(firstId, id);
                }
            } catch (Exception e) {
                fail("Thread failed: " + e.getMessage());
            }
        }
        executor.shutdown();
    }
    
    @Test
    public void testConcurrentCreationDifferentColumns() throws InterruptedException {
        int threads = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        ColumnSchema schema = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);

        List<Callable<Long>> tasks = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int idx = i;
            tasks.add(() -> db.createColumn("Col_" + idx, schema));
        }

        List<Future<Long>> results = executor.invokeAll(tasks);
        
        for (int i = 0; i < threads; i++) {
            try {
                long id = results.get(i).get();
                assertTrue(id > 0);
                assertEquals(id, db.getColumnId("Col_" + i));
            } catch (Exception e) {
                fail("Thread failed: " + e.getMessage());
            }
        }
        executor.shutdown();
    }

    @Test
    public void testPersistenceAcrossRestarts() throws IOException {
        String colName = "PersistentCol";
        ColumnSchema schema = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
        
        long idOrig = db.createColumn(colName, schema);
        db.closeTesting();
        db = null;

        // Reopen
        db = new EmbeddedDB(tempDir, "robust_test", null);
        long idNew = db.getColumnId(colName);
        
        assertEquals(idOrig, idNew, "Column ID should be stable across restarts (backed by RocksDB CF ID)");
    }
    
    @Test
    public void testManualColumnDetection() throws Exception {
        db.closeTesting();
        db = null;
        
        // Open raw and add a CF
        try (org.rocksdb.Options options = new org.rocksdb.Options().setCreateIfMissing(true)) {
             List<byte[]> families = RocksDB.listColumnFamilies(options, tempDir.toAbsolutePath().toString());
             List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
             for (byte[] family : families) {
                 descriptors.add(new ColumnFamilyDescriptor(family));
             }
             List<ColumnFamilyHandle> handles = new ArrayList<>();
             try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
                  org.rocksdb.RocksDB rawDb = org.rocksdb.RocksDB.open(dbOptions, tempDir.toAbsolutePath().toString(), descriptors, handles)) {
                 
                 ColumnFamilyDescriptor newCf = new ColumnFamilyDescriptor("ManualCol".getBytes(StandardCharsets.UTF_8));
                 rawDb.createColumnFamily(newCf).close();
                 
                 for(ColumnFamilyHandle handle : handles) {
                     handle.close();
                 }
             }
        }
        
        // Reopen EmbeddedDB
        db = new EmbeddedDB(tempDir, "robust_test", null);
        
        // ManualCol should NOT be accessible via getColumnId (unconfigured, no schema)
        RocksDBException ex = assertThrows(RocksDBException.class, () -> db.getColumnId("ManualCol"));
        assertEquals(RocksDBErrorType.COLUMN_NOT_FOUND, ex.getErrorUniqueId());
        
        // Calling createColumn should configure the unconfigured column
        ColumnSchema schema = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
        long id = db.createColumn("ManualCol", schema);
        assertTrue(id > 0);
        
        // Now it should be accessible
        long fetchedId = db.getColumnId("ManualCol");
        assertEquals(id, fetchedId);
    }
}
