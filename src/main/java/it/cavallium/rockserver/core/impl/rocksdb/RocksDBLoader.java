package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.*;
import java.io.InputStream;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.*;

import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.*;
import org.rocksdb.util.Environment;
import org.rocksdb.util.SizeUnit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;

import static it.cavallium.rockserver.core.common.Utils.mapList;
import static java.lang.Boolean.parseBoolean;
import static java.util.Objects.requireNonNull;
import static org.rocksdb.ColumnFamilyOptionsInterface.DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET;

public class RocksDBLoader {

    private static final boolean FOLLOW_ROCKSDB_OPTIMIZATIONS = true;

    private static final boolean PARANOID_CHECKS
            = Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.checks.paranoid", "true"));
    private static final boolean USE_CLOCK_CACHE
            = Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.clockcache.enable", "false"));
    private static final CacheFactory CACHE_FACTORY = USE_CLOCK_CACHE ? new ClockCacheFactory() : new LRUCacheFactory();
    private static final String bugJniLibraryFileName = Environment.getJniLibraryFileName("rocksdbjni");
    private static final String jniLibraryFileName = Environment.getJniLibraryFileName("rocksdb");
    @Nullable
    private static final String fallbackJniLibraryFileName = Environment.getFallbackJniLibraryFileName("rocksdb");
    @Nullable
    private static final String bugFallbackJniLibraryFileName = Environment.getFallbackJniLibraryFileName("rocksdbjni");

    public static void loadLibrary() {
        for (final CompressionType compressionType : CompressionType.values()) {
            try {
                if (compressionType.getLibraryName() != null) {
                    System.loadLibrary(compressionType.getLibraryName());
                }
            } catch (final UnsatisfiedLinkError e) {
                if (compressionType == CompressionType.LZ4_COMPRESSION) {
                    throw new IllegalStateException("Can't load LZ4", e);
                }
                if (compressionType == CompressionType.ZSTD_COMPRESSION) {
                    throw new IllegalStateException("Can't load ZSTD", e);
                }
                // since it may be optional, we ignore its loading failure here.
            }
        }
        try {
            String currentUsersHomeDir = System.getProperty("user.home");
            var jniPath = Path.of(currentUsersHomeDir).resolve(".jni").resolve("rocksdb").resolve(RocksDBMetadata.getRocksDBVersionHash());
            if (Files.notExists(jniPath)) {
                Files.createDirectories(jniPath);
            }
            loadLibraryFromJarToTemp(jniPath);

            RocksDB.loadLibrary(List.of(jniPath.toAbsolutePath().toString()));
        } catch (IOException e) {
            RocksDB.loadLibrary();
        }
    }

    private static Path loadLibraryFromJarToTemp(final Path tmpDir) throws IOException {
        var temp1 = tmpDir.resolve(bugJniLibraryFileName);
        if (Files.exists(temp1)) {
            return temp1;
        }
        try (InputStream is = RocksDB.class.getClassLoader().getResourceAsStream(jniLibraryFileName)) {
            if (is != null) {
                Files.copy(is, temp1, StandardCopyOption.REPLACE_EXISTING);
                return temp1;
            }
        }

        if (bugFallbackJniLibraryFileName == null) {
            throw new RuntimeException("rocksdb was not found inside JAR.");
        }

        var temp2 = tmpDir.resolve(bugFallbackJniLibraryFileName);
        try (InputStream is = RocksDB.class.getClassLoader().getResourceAsStream(fallbackJniLibraryFileName)) {
            if (is != null) {
                Files.copy(is, temp2, StandardCopyOption.REPLACE_EXISTING);
                return temp2;
            }
        }

        throw new RuntimeException("rocksdb was not found inside JAR.");
    }

    public record LoadedDb(TransactionalDB db, DBOptions dbOptions,
                           Map<String, ColumnFamilyOptions> definitiveColumnFamilyOptionsMap, RocksDBObjects refs,
                           @Nullable Cache cache) {}

    public static ColumnFamilyOptions getColumnOptions(String name,
        GlobalDatabaseConfig globalDatabaseConfig,
        Logger logger,
        RocksDBObjects refs,
        boolean inMemory,
        @Nullable Cache cache) {
        try {
            var columnFamilyOptions = new ColumnFamilyOptions();
            refs.add(columnFamilyOptions);

            FallbackColumnConfig columnOptions = null;
            for (NamedColumnConfig namedColumnConfig : globalDatabaseConfig.columnOptions()) {
                if (namedColumnConfig.name().equals(name)) {
                    columnOptions = namedColumnConfig;
                    break;
                }
            }
            if (columnOptions == null) {
                columnOptions = globalDatabaseConfig.fallbackColumnOptions();
            }

            //noinspection ConstantConditions
            if (columnOptions.memtableMemoryBudgetBytes() != null) {
                // about 512MB of ram will be used for level style compaction
                columnFamilyOptions.optimizeLevelStyleCompaction(Optional.ofNullable(columnOptions.memtableMemoryBudgetBytes())
                        .map(DataSize::longValue)
                        .orElse(DEFAULT_COMPACTION_MEMTABLE_MEMORY_BUDGET));
            }

            if (isDisableAutoCompactions()) {
                columnFamilyOptions.setDisableAutoCompactions(true);
            }
            try {
                columnFamilyOptions.setPrepopulateBlobCache(PrepopulateBlobCache.PREPOPULATE_BLOB_FLUSH_ONLY);
            } catch (Throwable ex) {
                logger.error("Failed to set prepopulate blob cache", ex);
            }

            // This option is not supported with multiple db paths
            // https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
            // https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-on-Spinning-Disks
            boolean dynamicLevelBytes = (globalDatabaseConfig.volumes() == null || globalDatabaseConfig.volumes().length <= 1)
                    && !globalDatabaseConfig.ingestBehind();
            if (dynamicLevelBytes) {
                columnFamilyOptions.setLevelCompactionDynamicLevelBytes(true);
            }

            // https://nightlies.apache.org/flink/flink-docs-release-1.3/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
            columnFamilyOptions
                    .setTargetFileSizeBase(256 * SizeUnit.MB)
                    .setMaxBytesForLevelBase(SizeUnit.GB);

            if (isDisableAutoCompactions()) {
                columnFamilyOptions.setLevel0FileNumCompactionTrigger(-1);
            } else if (!FOLLOW_ROCKSDB_OPTIMIZATIONS) {
                // ArangoDB uses a value of 2: https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
                // Higher values speed up writes, but slow down reads
                columnFamilyOptions.setLevel0FileNumCompactionTrigger(2);
            }
            if (isDisableSlowdown()) {
                columnFamilyOptions.setLevel0SlowdownWritesTrigger(-1);
                columnFamilyOptions.setLevel0StopWritesTrigger(Integer.MAX_VALUE);
                columnFamilyOptions.setHardPendingCompactionBytesLimit(Long.MAX_VALUE);
                columnFamilyOptions.setSoftPendingCompactionBytesLimit(Long.MAX_VALUE);
            }
            {
                // https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
                columnFamilyOptions.setLevel0SlowdownWritesTrigger(20);
                // https://www.arangodb.com/docs/stable/programs-arangod-rocksdb.html
                columnFamilyOptions.setLevel0StopWritesTrigger(36);
            }

            if (columnOptions.levels().length > 0) {
                columnFamilyOptions.setNumLevels(columnOptions.levels().length);

                List<CompressionType> compressionPerLevel = new ArrayList<>();
                for (ColumnLevelConfig columnLevelConfig : columnOptions.levels()) {
                    CompressionType compression = columnLevelConfig.compression();
                    compressionPerLevel.add(compression);
                }
                if (compressionPerLevel.size() != columnOptions.levels().length) {
                    throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, "Database column levels and compression per level count is different! %s != %s".formatted(compressionPerLevel.size(), columnOptions.levels().length));
                }
                columnFamilyOptions.setCompressionPerLevel(compressionPerLevel);

                var firstLevelOptions = getRocksLevelOptions(columnOptions.levels()[0], refs);
                columnFamilyOptions.setCompressionType(firstLevelOptions.compressionType);
                columnFamilyOptions.setCompressionOptions(firstLevelOptions.compressionOptions);

                var lastLevelOptions = getRocksLevelOptions(columnOptions
                        .levels()[columnOptions.levels().length - 1], refs);
                columnFamilyOptions.setBottommostCompressionType(lastLevelOptions.compressionType);
                columnFamilyOptions.setBottommostCompressionOptions(lastLevelOptions.compressionOptions);
            } else {
                columnFamilyOptions.setNumLevels(7);
                List<CompressionType> compressionTypes = new ArrayList<>(7);
                for (int i = 0; i < 7; i++) {
                    if (i < 2) {
                        compressionTypes.add(CompressionType.NO_COMPRESSION);
                    } else {
                        compressionTypes.add(CompressionType.LZ4_COMPRESSION);
                    }
                }
                columnFamilyOptions.setBottommostCompressionType(CompressionType.LZ4HC_COMPRESSION);
                var compressionOptions = new CompressionOptions()
                        .setEnabled(true)
                        .setMaxDictBytes(Math.toIntExact(32 * SizeUnit.KB));
                refs.add(compressionOptions);
                setZstdCompressionOptions(compressionOptions);
                columnFamilyOptions.setBottommostCompressionOptions(compressionOptions);
                columnFamilyOptions.setCompressionPerLevel(compressionTypes);
            }

            final BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();

            if (!FOLLOW_ROCKSDB_OPTIMIZATIONS) {
                columnFamilyOptions.setWriteBufferSize(256 * SizeUnit.MB);
            }
            Optional.ofNullable(columnOptions.writeBufferSize())
                    .map(DataSize::longValue)
                    .ifPresent(columnFamilyOptions::setWriteBufferSize);

            columnFamilyOptions.setMaxWriteBufferNumberToMaintain(1);
            if (tableOptions instanceof BlockBasedTableConfig blockBasedTableConfig) {
                blockBasedTableConfig.setVerifyCompression(false);
            }
            // If OptimizeFiltersForHits == true: memory size = bitsPerKey * (totalKeys * 0.1)
            // If OptimizeFiltersForHits == false: memory size = bitsPerKey * totalKeys
            BloomFilterConfig filter = null;
            BloomFilterConfig bloomFilterConfig = columnOptions.bloomFilter();
            if (bloomFilterConfig != null) filter = bloomFilterConfig;
            if (filter == null) {
                if (inMemory) {
                    throw it.cavallium.rockserver.core.common.RocksDBException.of(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.CONFIG_ERROR, "Please set a bloom filter. It's required for in-memory databases");
                }
                if (tableOptions instanceof BlockBasedTableConfig blockBasedTableConfig) {
                    blockBasedTableConfig.setFilterPolicy(null);
                }
            } else {
                final BloomFilter bloomFilter = new BloomFilter(filter.bitsPerKey());
                refs.add(bloomFilter);
                if (tableOptions instanceof BlockBasedTableConfig blockBasedTableConfig) {
                    blockBasedTableConfig.setFilterPolicy(bloomFilter);
                }
            }
            boolean cacheIndexAndFilterBlocks = !inMemory && Optional.ofNullable(columnOptions.cacheIndexAndFilterBlocks())
                    // https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
                    .orElse(true);
            if (globalDatabaseConfig.spinning()) {
                // https://nightlies.apache.org/flink/flink-docs-release-1.3/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
                columnFamilyOptions.setMinWriteBufferNumberToMerge(3);
                // https://nightlies.apache.org/flink/flink-docs-release-1.3/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
                columnFamilyOptions.setMaxWriteBufferNumber(4);
            }
            if (tableOptions instanceof BlockBasedTableConfig blockBasedTableConfig) {
                blockBasedTableConfig
                        // http://rocksdb.org/blog/2018/08/23/data-block-hash-index.html
                        .setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash)
                        // http://rocksdb.org/blog/2018/08/23/data-block-hash-index.html
                        .setDataBlockHashTableUtilRatio(0.75)
                        // https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
                        .setPinTopLevelIndexAndFilter(true)
                        // https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
                        .setPinL0FilterAndIndexBlocksInCache(!inMemory)
                        // https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
                        .setCacheIndexAndFilterBlocksWithHighPriority(true)
                        .setCacheIndexAndFilterBlocks(cacheIndexAndFilterBlocks)
                        // https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
                        // Enabling partition filters increase the reads by 2x
                        .setPartitionFilters(Optional.ofNullable(columnOptions.partitionFilters()).orElse(false))
                        // https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
                        .setIndexType(inMemory ? IndexType.kHashSearch : Optional.ofNullable(columnOptions.partitionFilters()).orElse(false) ? IndexType.kTwoLevelIndexSearch : IndexType.kBinarySearch)
                        .setChecksumType(inMemory ? ChecksumType.kNoChecksum : ChecksumType.kXXH3)
                        // Spinning disks: 64KiB to 256KiB (also 512KiB). SSDs: 16KiB
                        // https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-on-Spinning-Disks
                        // https://nightlies.apache.org/flink/flink-docs-release-1.3/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
                        .setBlockSize(inMemory ? 4 * SizeUnit.KB : Optional.ofNullable(columnOptions.blockSize())
                                .map(DataSize::longValue)
                                .orElse((globalDatabaseConfig.spinning() ? 128 : 16) * SizeUnit.KB))
                        .setBlockCache(cache)
                        .setNoBlockCache(cache == null);
            }
            if (inMemory) {
                columnFamilyOptions.useCappedPrefixExtractor(4);
                tableOptions.setBlockRestartInterval(4);
            }

            columnFamilyOptions.setTableFormatConfig(tableOptions);
            columnFamilyOptions.setCompactionPriority(CompactionPriority.MinOverlappingRatio);
            // https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-on-Spinning-Disks
            // https://github.com/EighteenZi/rocksdb_wiki/blob/master/RocksDB-Tuning-Guide.md#throughput-gap-between-random-read-vs-sequential-read-is-much-higher-in-spinning-disks-suggestions=
            BloomFilterConfig bloomFilterOptions = columnOptions.bloomFilter();
            if (bloomFilterOptions != null) {
                // https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-on-Spinning-Disks
                // https://github.com/EighteenZi/rocksdb_wiki/blob/master/RocksDB-Tuning-Guide.md#throughput-gap-between-random-read-vs-sequential-read-is-much-higher-in-spinning-disks-suggestions=
                boolean optimizeForHits = globalDatabaseConfig.spinning();
                Boolean value = bloomFilterOptions.optimizeForHits();
                if (value != null) optimizeForHits = value;
                columnFamilyOptions.setOptimizeFiltersForHits(optimizeForHits);
            }
            return columnFamilyOptions;
        } catch (GestaltException ex) {
            throw it.cavallium.rockserver.core.common.RocksDBException.of(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.ROCKSDB_CONFIG_ERROR, ex);
        }
    }

    private static void setZstdCompressionOptions(CompressionOptions compressionOptions) {
        // https://rocksdb.org/blog/2021/05/31/dictionary-compression.html#:~:text=(zstd%20only,it%20to%20100x
        compressionOptions
                .setZStdMaxTrainBytes(compressionOptions.maxDictBytes() * 100);
    }

    public static LoadedDb load(@Nullable Path path, DatabaseConfig config, Logger logger) {
        var refs = new RocksDBObjects();
        // Get databases directory path
        Path definitiveDbPath;
        if (path != null) {
            definitiveDbPath = path.toAbsolutePath();
        } else {
            try {
                definitiveDbPath = Files.createTempDirectory("temp-rocksdb");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        var optionsWithCache = makeRocksDBOptions(path, definitiveDbPath, config, refs, logger);
        return loadDb(path, definitiveDbPath, config, optionsWithCache, refs, logger);
    }

    record OptionsWithCache(DBOptions options, @Nullable Cache standardCache) {}

    private static OptionsWithCache makeRocksDBOptions(@Nullable Path path,
        Path definitiveDbPath,
        DatabaseConfig databaseOptions,
        RocksDBObjects refs,
        Logger logger) {
        try {
            // Get databases directory path
            Path parentPath;
            if (path != null) {
                parentPath = path.toAbsolutePath().getParent();
            } else {
                parentPath = null;
            }

            List<DbPathRecord> volumeConfigs = getVolumeConfigs(definitiveDbPath, databaseOptions);

            // the Options class contains a set of configurable DB options
            // that determines the behaviour of the database.
            var options = new DBOptions();
            refs.add(options);
            options.setParanoidChecks(PARANOID_CHECKS);
            options.setSkipCheckingSstFileSizesOnDbOpen(true);
            if (!databaseOptions.global().unorderedWrite()) {
                options.setEnablePipelinedWrite(true);
            }
            var maxSubCompactions = Integer.parseInt(System.getProperty("it.cavallium.dbengine.compactions.max.sub", "-1"));
            if (maxSubCompactions > 0) {
                options.setMaxSubcompactions(maxSubCompactions);
            }
            var customWriteRate = Long.parseLong(System.getProperty("it.cavallium.dbengine.write.delayedrate", "-1"));
            if (customWriteRate >= 0) {
                options.setDelayedWriteRate(customWriteRate);
            }

            getLogPath(definitiveDbPath, databaseOptions).map(Path::toString).ifPresent(options::setDbLogDir);

            if (path != null) {
                getWalDir(definitiveDbPath, databaseOptions).map(Path::toString).ifPresent(options::setWalDir);
            }

            options.setCreateIfMissing(true);
            options.setSkipStatsUpdateOnDbOpen(true);
            options.setCreateMissingColumnFamilies(true);
            options.setInfoLogLevel(InfoLogLevel.WARN_LEVEL);

            var delayWalFlushConfig = getWalFlushDelayConfig(databaseOptions);
            if (delayWalFlushConfig.isPositive()) {
                options.setManualWalFlush(true);
            }

            options.setAvoidFlushDuringShutdown(false); // Flush all WALs during shutdown
            options.setAvoidFlushDuringRecovery(true); // Flush all WALs during startup
            options.setWalRecoveryMode(databaseOptions.global().absoluteConsistency()
                    ? WALRecoveryMode.AbsoluteConsistency
                    : WALRecoveryMode.PointInTimeRecovery); // Crash if the WALs are corrupted.Default: TolerateCorruptedTailRecords
            options.setDeleteObsoleteFilesPeriodMicros(20 * 1000000); // 20 seconds
            options.setKeepLogFileNum(10);

            if (parentPath != null) {
                requireNonNull(parentPath);
                requireNonNull(path.getFileName());
                List<DbPath> paths = mapList(volumeConfigs, p -> new DbPath(p.path, p.targetSize));
                options.setDbPaths(paths);
            } else if (!volumeConfigs.isEmpty() && (volumeConfigs.size() > 1 || definitiveDbPath.relativize(volumeConfigs.getFirst().path).isAbsolute())) {
                throw it.cavallium.rockserver.core.common.RocksDBException.of(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.CONFIG_ERROR, "in-memory databases should not have any volume configured");
            }
            options.setMaxOpenFiles(Optional.ofNullable(databaseOptions.global().maximumOpenFiles()).orElse(-1));
            options.setMaxFileOpeningThreads(Runtime.getRuntime().availableProcessors());
            if (databaseOptions.global().spinning()) {
                // https://nightlies.apache.org/flink/flink-docs-release-1.3/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html
                options.setUseFsync(false);
            }

            long writeBufferManagerSize = Optional.ofNullable(databaseOptions.global().writeBufferManager())
                    .map(DataSize::longValue)
                    .orElse(0L);

            if (isDisableAutoCompactions()) {
                options.setMaxBackgroundJobs(0);
            } else {
                var backgroundJobs = Integer.parseInt(System.getProperty("it.cavallium.dbengine.jobs.background.num", "-1"));
                if (backgroundJobs >= 0) {
                    options.setMaxBackgroundJobs(backgroundJobs);
                }
            }

            Cache blockCache;
            final boolean useDirectIO = path != null && databaseOptions.global().useDirectIo();
            final boolean allowMmapReads = (path == null) || (!useDirectIO && databaseOptions.global().allowRocksdbMemoryMapping());
            final boolean allowMmapWrites = (path != null) && (!useDirectIO && (databaseOptions.global().allowRocksdbMemoryMapping()
                    || parseBoolean(System.getProperty("it.cavallium.dbengine.mmapwrites.enable", "false"))));

            // todo: replace with a real option called database-write-buffer-size
            // 0 = default = disabled
            long dbWriteBufferSize = Long.parseLong(System.getProperty("it.cavallium.dbengine.dbwritebuffer.size", "0"));

            options
                    .setDbWriteBufferSize(dbWriteBufferSize)
                    .setBytesPerSync(64 * SizeUnit.MB)
                    .setWalBytesPerSync(64 * SizeUnit.MB)

                    .setWalTtlSeconds(80) // Auto
                    .setWalSizeLimitMB(0) // Auto
                    .setMaxTotalWalSize(0) // AUto
            ;
            if (path != null) {
                blockCache = CACHE_FACTORY.newCache(writeBufferManagerSize + Optional.ofNullable(databaseOptions.global().blockCache()).map(DataSize::longValue).orElse( 512 * SizeUnit.MB));
                refs.add(blockCache);
            } else {
                blockCache = null;
            }

            if (useDirectIO) {
                options
                        // Option to enable readahead in compaction
                        // If not set, it will be set to 2MB internally
                        .setCompactionReadaheadSize(4 * SizeUnit.MB) // recommend at least 2MB
                        // Option to tune write buffer for direct writes
                        .setWritableFileMaxBufferSize(2 * SizeUnit.MB)
                ;
            }
            if (databaseOptions.global().spinning()) {
                options
                        // method documentation
                        .setCompactionReadaheadSize(16 * SizeUnit.MB)
                        // guessed
                        .setWritableFileMaxBufferSize(8 * SizeUnit.MB);
            }
            options.setIncreaseParallelism(Runtime.getRuntime().availableProcessors());

            if (path != null && writeBufferManagerSize > 0L) {
                var writeBufferManager = new WriteBufferManager(writeBufferManagerSize, blockCache, false);
                refs.add(writeBufferManager);
                options.setWriteBufferManager(writeBufferManager);
            }

            if (useDirectIO) {
                options
                        .setAllowMmapReads(false)
                        .setAllowMmapWrites(false)
                        .setUseDirectReads(true)
                ;
            } else {
                options
                        .setAllowMmapReads(allowMmapReads)
                        .setAllowMmapWrites(allowMmapWrites);
            }

            if (useDirectIO || !allowMmapWrites) {
                options.setUseDirectIoForFlushAndCompaction(true);
            }

            options
                    .setAllowIngestBehind(databaseOptions.global().ingestBehind())
                    .setUnorderedWrite(databaseOptions.global().unorderedWrite());

            return new OptionsWithCache(options, blockCache);
        } catch (GestaltException e) {
            throw it.cavallium.rockserver.core.common.RocksDBException.of(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.ROCKSDB_CONFIG_ERROR, e);
        }
    }

    private static Duration getWalFlushDelayConfig(DatabaseConfig databaseOptions) throws GestaltException {
        return Objects.requireNonNullElse(databaseOptions.global().delayWalFlushDuration(), Duration.ZERO);
    }

    private static Optional<Path> getWalDir(Path definitiveDbPath, DatabaseConfig databaseOptions)
        throws GestaltException {
        return Optional.ofNullable(databaseOptions.global().walPath())
            .map(definitiveDbPath::resolve);
    }

    private static Optional<Path> getLogPath(Path definitiveDbPath, DatabaseConfig databaseOptions)
        throws GestaltException {
        return Optional.ofNullable(databaseOptions.global().logPath())
            .map(definitiveDbPath::resolve);
    }

    public static List<DbPathRecord> getVolumeConfigs(@NotNull Path definitiveDbPath, DatabaseConfig databaseOptions)
        throws GestaltException {
        return ConfigPrinter
            .getVolumeConfigs(databaseOptions.global())
            .stream()
            .map(volumeConfig -> {
                try {
                    var volumePath = volumeConfig.volumePath();
                    Objects.requireNonNull(volumePath, "volumePath is null");
                    return new DbPathRecord(definitiveDbPath.resolve(volumePath), volumeConfig.targetSize().longValue());
                } catch (NullPointerException | GestaltException e) {
                    throw it.cavallium.rockserver.core.common.RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, "Failed to load volume configurations", e);
                }
            })
            .toList();
    }

    private static LoadedDb loadDb(@Nullable Path path,
        @NotNull Path definitiveDbPath,
        DatabaseConfig databaseOptions, OptionsWithCache optionsWithCache, RocksDBObjects refs, Logger logger) {
        var inMemory = path == null;
        var rocksdbOptions = optionsWithCache.options();
        Map<String, ColumnFamilyOptions> definitiveColumnFamilyOptionsMap = new HashMap<>();
        try {
            List<DbPathRecord> volumeConfigs = getVolumeConfigs(definitiveDbPath, databaseOptions);
            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            var walPath = getWalDir(definitiveDbPath, databaseOptions);
            var logPath = getLogPath(definitiveDbPath, databaseOptions);

            // Create base directories
            if (Files.notExists(definitiveDbPath)) {
                Files.createDirectories(definitiveDbPath);
            }
            for (DbPathRecord volumeConfig : volumeConfigs) {
                if (Files.notExists(volumeConfig.path)) {
                    Files.createDirectories(volumeConfig.path);
                }
            }
            if (walPath.isPresent() && Files.notExists(walPath.get())) {
                Files.createDirectories(walPath.get());
            }
            if (logPath.isPresent() && Files.notExists(logPath.get())) {
                Files.createDirectories(logPath.get());
            }

            var defaultColumnOptions = new ColumnFamilyOptions();
            refs.add(defaultColumnOptions);
            descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultColumnOptions));

            var rocksLogger = new RocksLogger(rocksdbOptions, logger);

            var columnConfigs = databaseOptions.global().columnOptions();

            SequencedMap<String, FallbackColumnConfig> columnConfigMap = new LinkedHashMap<>();

            for (NamedColumnConfig columnConfig : columnConfigs) {
                columnConfigMap.put(columnConfig.name(), columnConfig);
            }
            if (path != null) {
                List<String> existingColumnFamilies;
                try (var options = new Options()) {
                    options.setCreateIfMissing(true);
                    existingColumnFamilies = mapList(RocksDB.listColumnFamilies(options, path.toString()), b -> new String(b, StandardCharsets.UTF_8));
                }
                for (String existingColumnFamily : existingColumnFamilies) {
                    columnConfigMap.putIfAbsent(existingColumnFamily, databaseOptions.global().fallbackColumnOptions());
                }
            }

            for (Map.Entry<String, FallbackColumnConfig> entry : columnConfigMap.entrySet()) {
                String name = entry.getKey();
                var columnFamilyOptions = getColumnOptions(name, databaseOptions.global(),
                        logger, refs, path == null, optionsWithCache.standardCache());
                refs.add(columnFamilyOptions);

                descriptors.add(new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.US_ASCII), columnFamilyOptions));
                definitiveColumnFamilyOptionsMap.put(name, columnFamilyOptions);
            }

            var handles = new ArrayList<ColumnFamilyHandle>();
            RocksDB db;
            // a factory method that returns a RocksDB instance
            if (databaseOptions.global().optimistic()) {
                db = OptimisticTransactionDB.open(rocksdbOptions, definitiveDbPath.toString(), descriptors, handles);
            } else {
                var transactionOptions = new TransactionDBOptions()
                    .setWritePolicy(TxnDBWritePolicy.WRITE_COMMITTED)
                    .setTransactionLockTimeout(5000)
                    .setDefaultLockTimeout(5000);
                refs.add(transactionOptions);
                db = TransactionDB.open(rocksdbOptions,
                    transactionOptions,
                    definitiveDbPath.toString(),
                    descriptors,
                    handles
                );
            }

            handles.forEach(refs::add);

            try {
                for (ColumnFamilyHandle cfh : handles) {
                    var props = db.getProperty(cfh, "rocksdb.stats");
                    logger.trace("Stats for database column {}: {}", new String(cfh.getName(), StandardCharsets.UTF_8),
                        props);
                }
            } catch (RocksDBException ex) {
                logger.debug("Failed to obtain stats", ex);
            }

            var delayWalFlushConfig = getWalFlushDelayConfig(databaseOptions);
            var dbTasks = new DatabaseTasks(db, inMemory, delayWalFlushConfig);
            return new LoadedDb(TransactionalDB.create(definitiveDbPath.toString(), db, descriptors, handles, dbTasks), rocksdbOptions, definitiveColumnFamilyOptionsMap, refs, optionsWithCache.standardCache());
        } catch (IOException | RocksDBException ex) {
            throw it.cavallium.rockserver.core.common.RocksDBException.of(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.ROCKSDB_LOAD_ERROR, "Failed to load rocksdb", ex);
        } catch (GestaltException e) {
            throw it.cavallium.rockserver.core.common.RocksDBException.of(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.CONFIG_ERROR, "Failed to load rocksdb", e);
        }
    }

    public record DbPathRecord(Path path, long targetSize) {}

    public static boolean isDisableAutoCompactions() {
        return parseBoolean(System.getProperty("it.cavallium.dbengine.compactions.auto.disable", "false"));
    }

    public static boolean isDisableSlowdown() {
        return isDisableAutoCompactions()
                || parseBoolean(System.getProperty("it.cavallium.dbengine.disableslowdown", "false"));
    }

    private record RocksLevelOptions(CompressionType compressionType, CompressionOptions compressionOptions) {}
    private static RocksLevelOptions getRocksLevelOptions(ColumnLevelConfig levelOptions, RocksDBObjects refs) throws GestaltException {
        var compressionType = levelOptions.compression();
        var compressionOptions = new CompressionOptions();
        refs.add(compressionOptions);
        if (compressionType != CompressionType.NO_COMPRESSION) {
            compressionOptions.setEnabled(true)
                    .setMaxDictBytes(Math.toIntExact(levelOptions.maxDictBytes().longValue()));
            setZstdCompressionOptions(compressionOptions);
        } else {
            compressionOptions.setEnabled(false);
        }
        return new RocksLevelOptions(compressionType, compressionOptions);
    }
}
