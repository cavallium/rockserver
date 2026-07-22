package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import it.cavallium.rockserver.core.impl.rocksdb.RocksLeakDetector;
import it.cavallium.rockserver.core.impl.rocksdb.TransactionalDB;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.BackgroundErrorReason;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ChecksumType;
import org.rocksdb.HyperClockCache;
import org.rocksdb.IndexType;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksObject;
import org.rocksdb.Status;
import org.rocksdb.StatsLevel;
import org.rocksdb.WALRecoveryMode;
import org.slf4j.LoggerFactory;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class RocksDBLoaderComplexConfigTest {

    private static final List<String> LOADER_PROPERTIES = List.of(
            "it.cavallium.dbengine.write.delayedrate",
            "it.cavallium.dbengine.jobs.background.num"
    );

    @AfterEach
    void clearLoaderProperties() {
        LOADER_PROPERTIES.forEach(System::clearProperty);
    }

    @Test
    void extractsBundledJniLibraryOnce(@TempDir Path tempDir) throws Exception {
        Method extract = RocksDBLoader.class.getDeclaredMethod("loadLibraryFromJarToTemp", Path.class);
        extract.setAccessible(true);

        Path first = (Path) extract.invoke(null, tempDir);
        assertTrue(Files.isRegularFile(first));
        long extractedSize = Files.size(first);
        assertTrue(extractedSize > 0L);

        Path second = (Path) extract.invoke(null, tempDir);
        assertEquals(first, second);
        assertEquals(extractedSize, Files.size(second));
    }

    @Test
    void appliesPersistentPessimisticAndSpinningConfiguration(@TempDir Path tempDir) throws Exception {
        System.setProperty("it.cavallium.dbengine.write.delayedrate", "1234567");

        var config = parse(tempDir, "persistent-modes", """
                database: {
                  global: {
                    follow-rocksdb-optimizations: false
                    paranoid-checks: false
                    spinning: true
                    absolute-consistency: false
                    use-direct-io: false
                    allow-rocksdb-memory-mapping: false
                    allow-rocksdb-mmap-writes: true
                    maximum-open-files: 37
                    max-file-opening-threads: 5
                    optimistic: false
                    block-cache: "8MiB"
                    write-buffer-manager: "4MiB"
                    database-write-buffer-size: "1MiB"
                    max-subcompactions: 3
                    log-path: "./custom-log"
                    wal-path: "./custom-wal"
                    delay-wal-flush-duration: PT1S
                    max-background-jobs: 2
                    fallback-column-options: {
                      cache-index-and-filter-blocks: false
                      partition-filters: true
                      block-size: "32KiB"
                    }
                    column-options: [
                      {
                        name: "default"
                        cache-index-and-filter-blocks: false
                        partition-filters: true
                        block-size: "32KiB"
                      }
                    ]
                  }
                }
                """);
        Path dbPath = tempDir.resolve("persistent-db");

        var loaded = RocksDBLoader.load(dbPath, config, LoggerFactory.getLogger(getClass()));
        try {
            assertInstanceOf(TransactionalDB.PessimisticTransactionalDB.class, loaded.db());
            assertInstanceOf(LRUCache.class, loaded.cache(),
                    "metadata reservation requires the LRU priority-pool implementation");
            assertNotNull(loaded.dbOptions().statistics(), "runtime RocksDB statistics must stay enabled");
            assertEquals(StatsLevel.EXCEPT_TIME_FOR_MUTEX, loaded.dbOptions().statistics().statsLevel());
            assertFalse(loaded.dbOptions().skipStatsUpdateOnDbOpen(),
                    "startup must retain per-SST statistics used for compaction decisions");
            assertFalse(loaded.dbOptions().openFilesAsync());
            assertFalse(loaded.dbOptions().paranoidChecks());
            assertTrue(loaded.dbOptions().isOwningHandle(),
                    "successful load must transfer live native-reference ownership to LoadedDb");
            assertEquals(3, loaded.dbOptions().maxSubcompactions());
            assertEquals(1_234_567L, loaded.dbOptions().delayedWriteRate());
            assertEquals(1_048_576L, loaded.dbOptions().dbWriteBufferSize());
            assertEquals(37, loaded.dbOptions().maxOpenFiles());
            assertEquals(5, loaded.dbOptions().maxFileOpeningThreads());
            assertEquals(2, loaded.dbOptions().maxBackgroundJobs());
            assertTrue(loaded.dbOptions().manualWalFlush());
            assertEquals(WALRecoveryMode.PointInTimeRecovery, loaded.dbOptions().walRecoveryMode());
            assertFalse(loaded.dbOptions().allowMmapReads());
            assertTrue(loaded.dbOptions().allowMmapWrites());
            assertFalse(loaded.dbOptions().useDirectReads());
            assertFalse(loaded.dbOptions().useDirectIoForFlushAndCompaction());
            assertEquals(dbPath.resolve("custom-wal").toAbsolutePath().normalize(),
                    Path.of(loaded.dbOptions().walDir()).normalize());
            assertEquals(dbPath.resolve("custom-log").toAbsolutePath().normalize(),
                    Path.of(loaded.dbOptions().dbLogDir()).normalize());
            assertTrue(Files.isDirectory(dbPath.resolve("custom-wal")));
            assertTrue(Files.isDirectory(dbPath.resolve("custom-log")));
            try (var optionFiles = Files.list(dbPath)) {
                Path optionsFile = optionFiles
                        .filter(file -> file.getFileName().toString().startsWith("OPTIONS-"))
                        .max(Path::compareTo)
                        .orElseThrow();
                String persistedOptions = Files.readString(optionsFile);
                assertTrue(persistedOptions.contains("enforce_write_buffer_manager_during_recovery=false"),
                        "the RocksDB 11 upgrade must retain the 10.10 WAL-recovery memory and flush policy");
                assertTrue(persistedOptions.contains("format_version=6"),
                        "the compatibility table format must reach the native options");
                // RocksDB omits the negative sentinel when serializing OPTIONS. The live table
                // configuration below is the authoritative compatibility assertion.
                assertFalse(persistedOptions.contains("uniform_cv_threshold=0.200000"),
                        "the RocksJava 11 uniformity default must remain disabled in persisted options");
            }

            var columnOptions = loaded.definitiveColumnFamilyOptionsMap().get("default");
            assertNotNull(columnOptions);
            assertEquals(2, columnOptions.level0FileNumCompactionTrigger());
            assertEquals(3, columnOptions.minWriteBufferNumberToMerge());
            assertEquals(4, columnOptions.maxWriteBufferNumber());
            var table = assertInstanceOf(BlockBasedTableConfig.class, columnOptions.tableFormatConfig());
            assertEquals(IndexType.kTwoLevelIndexSearch, table.indexType());
            assertEquals(ChecksumType.kXXH3, table.checksumType());
            assertEquals(6, table.formatVersion(),
                    "the RocksDB 11 upgrade must not silently change production SST output format");
            assertEquals(-1.0d, table.uniformCvThreshold(),
                    "new SSTs must remain readable by RocksDB 10.10 and avoid new index-build work");
            assertEquals(32L * 1024L, table.blockSize());
            assertTrue(table.partitionFilters());
            assertTrue(table.cacheIndexAndFilterBlocksWithHighPriority());
            assertFalse(table.cacheIndexAndFilterBlocks());
        } finally {
            loaded.db().close();
            loaded.refs().close();
        }
    }

    @Test
    void enablesAsynchronousFileOpeningWithoutChangingTheEventualOpenFileMode(@TempDir Path tempDir)
            throws Exception {
        var config = parse(tempDir, "async-file-open", """
                database.global.maximum-open-files = -1
                database.global.open-files-async = true
                database.global.max-file-opening-threads = 3
                """);

        var logger = mock(org.slf4j.Logger.class);
        var loaded = RocksDBLoader.load(tempDir.resolve("async-file-open-db"), config,
                logger);
        AbstractEventListener listener = null;
        try {
            assertEquals(-1, loaded.dbOptions().maxOpenFiles());
            assertEquals(3, loaded.dbOptions().maxFileOpeningThreads());
            assertTrue(loaded.dbOptions().openFilesAsync());
            assertTrue(loaded.dbOptions().skipStatsUpdateOnDbOpen(),
                    "RocksDB requires the synchronous table-statistics pass to be skipped for async opening");
            assertEquals(1, loaded.dbOptions().listeners().size(),
                    "background async-open failures must remain observable after DB::Open returns");
            listener = assertInstanceOf(AbstractEventListener.class, loaded.dbOptions().listeners().getFirst());
            var status = new Status(Status.Code.IOError, Status.SubCode.NoSpace, "failed to open SST");
            listener.onBackgroundError(BackgroundErrorReason.ASYNC_FILE_OPEN, status);
            verify(logger).error(
                    eq("RocksDB background error: reason={}, code={}, subcode={}, state={}"),
                    eq(BackgroundErrorReason.ASYNC_FILE_OPEN),
                    eq(Status.Code.IOError),
                    eq(Status.SubCode.NoSpace),
                    eq("failed to open SST"));
        } finally {
            loaded.db().close();
            loaded.refs().close();
        }
        assertNotNull(listener);
        assertFalse(listener.isOwningHandle(), "the listener's native callback handle must be released at shutdown");
    }

    @Test
    void servesExistingSstsAndCanCloseWhileAsynchronousOpeningIsActive(@TempDir Path tempDir)
            throws Exception {
        Path dbPath = tempDir.resolve("async-existing-ssts-db");
        var creationConfig = parse(tempDir, "async-existing-ssts-create", """
                database.global.open-files-async = false
                database.global.maximum-open-files = -1
                database.global.disable-auto-compactions = true
                """);
        var keys = new ArrayList<byte[]>();
        var values = new ArrayList<byte[]>();
        var initial = RocksDBLoader.load(dbPath, creationConfig, LoggerFactory.getLogger(getClass()));
        try (var flushOptions = new org.rocksdb.FlushOptions().setWaitForFlush(true)) {
            for (int i = 0; i < 32; i++) {
                byte[] key = ("key-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                byte[] value = ("value-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                keys.add(key);
                values.add(value);
                initial.db().get().put(key, value);
                initial.db().get().flush(flushOptions);
            }
        } finally {
            initial.db().close();
            initial.refs().close();
        }

        var asyncConfig = parse(tempDir, "async-existing-ssts-open", """
                database.global.open-files-async = true
                database.global.maximum-open-files = -1
                database.global.max-file-opening-threads = 1
                database.global.disable-auto-compactions = true
                """);
        var asynchronouslyOpened = RocksDBLoader.load(dbPath, asyncConfig, LoggerFactory.getLogger(getClass()));
        try (var executor = Executors.newFixedThreadPool(4)) {
            var reads = new ArrayList<Callable<byte[]>>(keys.size());
            for (byte[] key : keys) {
                reads.add(() -> asynchronouslyOpened.db().get().get(key));
            }
            var results = executor.invokeAll(reads);
            for (int i = 0; i < results.size(); i++) {
                assertArrayEquals(values.get(i), results.get(i).get());
            }
        } finally {
            asynchronouslyOpened.db().close();
            asynchronouslyOpened.refs().close();
        }

        var closeDuringWarmup = RocksDBLoader.load(dbPath, asyncConfig, LoggerFactory.getLogger(getClass()));
        closeDuringWarmup.db().close();
        closeDuringWarmup.refs().close();

        var pessimisticAsyncConfig = parse(tempDir, "async-existing-ssts-pessimistic", """
                database.global.optimistic = false
                database.global.open-files-async = true
                database.global.maximum-open-files = -1
                database.global.max-file-opening-threads = 1
                database.global.disable-auto-compactions = true
                """);
        var pessimisticCloseDuringWarmup = RocksDBLoader.load(dbPath, pessimisticAsyncConfig,
                LoggerFactory.getLogger(getClass()));
        assertInstanceOf(org.rocksdb.TransactionDB.class, pessimisticCloseDuringWarmup.db().get());
        pessimisticCloseDuringWarmup.db().close();
        pessimisticCloseDuringWarmup.refs().close();
    }

    @Test
    void rejectsAsyncFileOpeningWithFiniteOpenFileLimit(@TempDir Path tempDir) throws Exception {
        var config = parse(tempDir, "invalid-async-file-open", """
                database.global.maximum-open-files = 1024
                database.global.open-files-async = true
                """);

        var failure = assertThrows(RocksDBException.class,
                () -> RocksDBLoader.load(tempDir.resolve("invalid-async-file-open-db"), config,
                        LoggerFactory.getLogger(getClass())));

        assertEquals(RocksDBErrorType.CONFIG_ERROR, failure.getErrorUniqueId());
        assertTrue(failure.getMessage().contains("maximum-open-files=-1"));
    }

    @Test
    void rejectsNonPositiveMaxFileOpeningThreads(@TempDir Path tempDir) throws Exception {
        for (int invalidValue : List.of(0, -3)) {
            var config = parse(tempDir, "invalid-file-opening-threads-" + invalidValue, """
                    database.global.max-file-opening-threads = %d
                    """.formatted(invalidValue));

            var failure = assertThrows(RocksDBException.class,
                    () -> RocksDBLoader.load(tempDir.resolve("invalid-file-opening-threads-db-" + invalidValue),
                            config, LoggerFactory.getLogger(getClass())));

            assertEquals(RocksDBErrorType.CONFIG_ERROR, failure.getErrorUniqueId());
            assertTrue(failure.getMessage().contains("max-file-opening-threads"));
        }
    }

    @Test
    void rejectsNonPositiveSchedulerParallelismBeforeOpeningDatabase(@TempDir Path tempDir) throws Exception {
        var invalidSettings = List.of(
                "database.parallelism.read = 0",
                "database.parallelism.write = -3"
        );
        for (int i = 0; i < invalidSettings.size(); i++) {
            Path configPath = tempDir.resolve("invalid-scheduler-parallelism-" + i + ".conf");
            Files.writeString(configPath, invalidSettings.get(i));
            Path dbPath = tempDir.resolve("invalid-scheduler-parallelism-db-" + i);
            String databaseName = "invalid-scheduler-parallelism-" + i;

            var failure = assertThrows(RocksDBException.class,
                    () -> new EmbeddedDB(dbPath, databaseName, configPath));

            assertEquals(RocksDBErrorType.CONFIG_ERROR, failure.getErrorUniqueId());
            assertTrue(failure.getMessage().contains("parallelism"));
            assertFalse(Files.exists(dbPath), "invalid scheduler config must fail before DB::Open");
        }
    }

    @Test
    void useClockCacheSelectsHyperClockWhenPriorityPoolIsDisabled(@TempDir Path tempDir) throws Exception {
        var config = parse(tempDir, "clock-cache", """
                database.global.use-clock-cache = true
                database.global.block-cache-high-priority-ratio = 0
                database.global.block-cache = 8MiB
                database.global.write-buffer-manager = 4MiB
                """);

        var loaded = RocksDBLoader.load(tempDir.resolve("clock-cache-db"), config,
                LoggerFactory.getLogger(getClass()));
        try {
            assertInstanceOf(HyperClockCache.class, loaded.cache());
            assertEquals(Runtime.getRuntime().availableProcessors(), loaded.dbOptions().maxFileOpeningThreads());
        } finally {
            loaded.db().close();
            loaded.refs().close();
        }
    }

    @Test
    void rejectsInvalidBlockCacheHighPriorityRatioBeforeOpeningDatabase(@TempDir Path tempDir) throws Exception {
        var config = parse(tempDir, "invalid-cache-ratio", """
                database.global.block-cache-high-priority-ratio = 1.01
                """);

        var failure = assertThrows(RocksDBException.class,
                () -> RocksDBLoader.load(tempDir.resolve("invalid-cache-ratio-db"), config,
                        LoggerFactory.getLogger(getClass())));

        assertEquals(RocksDBErrorType.CONFIG_ERROR, failure.getErrorUniqueId());
        assertTrue(failure.getMessage().contains("block-cache-high-priority-ratio"));
    }

    @Test
    void rejectsNonPositiveMaxSubcompactionsBeforeOpeningDatabase(@TempDir Path tempDir) throws Exception {
        var config = parse(tempDir, "invalid-max-subcompactions", """
                database.global.max-subcompactions = 0
                """);

        var failure = assertThrows(RocksDBException.class,
                () -> RocksDBLoader.load(tempDir.resolve("invalid-max-subcompactions-db"), config,
                        LoggerFactory.getLogger(getClass())));

        assertEquals(RocksDBErrorType.CONFIG_ERROR, failure.getErrorUniqueId());
        assertTrue(failure.getMessage().contains("max-subcompactions"));
    }

    @Test
    void disableSlowdownKeepsAllWriteStallThresholdsDisabled(@TempDir Path tempDir) throws Exception {
        var config = parse(tempDir, "no-compactions", """
                database.global.disable-auto-compactions = true
                database.global.disable-write-slowdown = true
                """);
        var loaded = RocksDBLoader.load(tempDir.resolve("no-compactions"), config,
                LoggerFactory.getLogger(getClass()));
        try {
            var columnOptions = loaded.definitiveColumnFamilyOptionsMap().get("default");
            assertNotNull(columnOptions);
            assertTrue(columnOptions.disableAutoCompactions());
            assertEquals(-1, columnOptions.level0FileNumCompactionTrigger());
            assertEquals(-1, columnOptions.level0SlowdownWritesTrigger());
            assertEquals(Integer.MAX_VALUE, columnOptions.level0StopWritesTrigger());
            assertEquals(Long.MAX_VALUE, columnOptions.hardPendingCompactionBytesLimit());
            assertEquals(Long.MAX_VALUE, columnOptions.softPendingCompactionBytesLimit());
            assertEquals(0, loaded.dbOptions().maxBackgroundJobs());
        } finally {
            loaded.db().close();
            loaded.refs().close();
        }
    }

    @Test
    void failedLoadClosesEveryPartiallyConstructedNativeReference(@TempDir Path tempDir) throws Exception {
        var config = parse(tempDir, "invalid-merge", """
                database: {
                  global: {
                    fallback-column-options: {
                      merge-operator-class: "java.lang.String"
                    }
                    column-options: [
                      {
                        name: "default"
                        merge-operator-class: "java.lang.String"
                      }
                    ]
                  }
                }
                """);

        var nativeReferences = new ArrayList<RocksObject>();
        try (var registrations = mockStatic(RocksLeakDetector.class)) {
            registrations.when(() -> RocksLeakDetector.register(any(), anyString(), any()))
                    .thenAnswer(invocation -> {
                        nativeReferences.add(invocation.getArgument(0));
                        return null;
                    });
            var failure = assertThrows(RocksDBException.class,
                    () -> RocksDBLoader.load(tempDir.resolve("invalid-merge-db"), config,
                            LoggerFactory.getLogger(getClass())));
            assertEquals(RocksDBErrorType.CONFIG_ERROR, failure.getErrorUniqueId());
        }
        assertTrue(nativeReferences.size() >= 5, "the failure must occur after native loader setup starts");
        nativeReferences.forEach(reference -> assertFalse(reference.isOwningHandle(),
                () -> "partially constructed reference was not closed: " + reference.getClass().getName()));
    }

    private static it.cavallium.rockserver.core.config.DatabaseConfig parse(
            Path tempDir, String name, String configText) throws Exception {
        Path configFile = tempDir.resolve(name + ".conf");
        Files.writeString(configFile, configText);
        return ConfigParser.parse(configFile);
    }
}
