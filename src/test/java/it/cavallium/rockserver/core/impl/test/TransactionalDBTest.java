package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.impl.rocksdb.DatabaseTasks;
import it.cavallium.rockserver.core.impl.rocksdb.TransactionalDB;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

class TransactionalDBTest {

    @TempDir
    Path tempDir;

    @Test
    void factoryRejectsPlainRocksDbWithArgumentError() throws Exception {
        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, tempDir.toString())) {
            var descriptors = List.<ColumnFamilyDescriptor>of();
            var handles = new ArrayList<ColumnFamilyHandle>();

            var failure = assertThrows(IllegalArgumentException.class,
                    () -> TransactionalDB.create(tempDir.toString(), db, descriptors, handles, null));

            assertEquals("This database is not transactional", failure.getMessage());
        }
    }

    @Test
    void factoryRejectsNullDatabaseWithArgumentError() {
        var failure = assertThrows(IllegalArgumentException.class,
                () -> TransactionalDB.create(tempDir.toString(), null, List.of(), new ArrayList<>(), null));

        assertEquals("db must not be null", failure.getMessage());
    }

    @Test
    void constructorRejectsMismatchedStartupColumnsWithArgumentError() throws Exception {
        try (var options = new Options().setCreateIfMissing(true);
             var db = OptimisticTransactionDB.open(options, tempDir.resolve("mismatched-columns").toString())) {
            var descriptors = List.of(new ColumnFamilyDescriptor("extra".getBytes(StandardCharsets.US_ASCII)));
            var handles = new ArrayList<ColumnFamilyHandle>();
            var databaseTasks = new DatabaseTasks(db, true, Duration.ZERO);

            var failure = assertThrows(IllegalArgumentException.class,
                    () -> new TransactionalDB.OptimisticTransactionalDB(
                            tempDir.toString(), db, descriptors, handles, databaseTasks));

            assertEquals("Startup column descriptors and handles size mismatch: descriptors=1, handles=0",
                    failure.getMessage());
        }
    }
}
