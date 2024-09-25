package it.cavallium.rockserver.core.impl.test;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class TestSSTWriter {

    private static final Logger LOG = LoggerFactory.getLogger(TestSSTWriter.class);

    private EmbeddedDB db;
    private long colId;

    @BeforeEach
    public void setUp() throws IOException {
        db = new EmbeddedDB(null, "test", null);
        this.colId = db.createColumn("test", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
    }

    @Test
    public void test() throws IOException {
        LOG.info("Obtaining sst writer");
        var globalDatabaseConfigOverride = new GlobalDatabaseConfig() {
            @Override
            public boolean spinning() {
                return false;
            }

            @Override
            public boolean checksum() {
                return false;
            }

            @Override
            public boolean useDirectIo() {
                return false;
            }

            @Override
            public boolean allowRocksdbMemoryMapping() {
                return true;
            }

            @Override
            public @Nullable Integer maximumOpenFiles() {
                return -1;
            }

            @Override
            public boolean optimistic() {
                return true;
            }

            @Override
            public @Nullable DataSize blockCache() {
                return new DataSize("10MiB");
            }

            @Override
            public @Nullable DataSize writeBufferManager() {
                return new DataSize("1MiB");
            }

            @Override
            public @Nullable Path logPath() {
                return null;
            }

            @Override
            public @Nullable Path walPath() {
                return null;
            }

            @Override
            public @Nullable Duration delayWalFlushDuration() {
                return null;
            }

            @Override
            public boolean absoluteConsistency() {
                return false;
            }

            @Override
            public boolean ingestBehind() {
                return true;
            }

            @Override
            public boolean unorderedWrite() {
                return false;
            }

            @Override
            public VolumeConfig[] volumes() {
                return new VolumeConfig[0];
            }

            @Override
            public FallbackColumnConfig fallbackColumnOptions() {
                return null;
            }

            @Override
            public NamedColumnConfig[] columnOptions() {
                return new NamedColumnConfig[0];
            }
        };
        var fallbackColumnConfig = new FallbackColumnConfig() {

            @Override
            public ColumnLevelConfig[] levels() {
                return new ColumnLevelConfig[] {
                    new ColumnLevelConfig() {
                        @Override
                        public CompressionType compression() {
                            return CompressionType.NO_COMPRESSION;
                        }

                        @Override
                        public DataSize maxDictBytes() {
                            return DataSize.ZERO;
                        }
                    }
                };
            }

            @Override
            public @Nullable DataSize memtableMemoryBudgetBytes() {
                return new DataSize("1MiB");
            }

            @Override
            public @Nullable Boolean cacheIndexAndFilterBlocks() {
                return true;
            }

            @Override
            public @Nullable Boolean partitionFilters() {
                return false;
            }

            @Override
            public @Nullable BloomFilterConfig bloomFilter() {
                return new BloomFilterConfig() {
                    @Override
                    public int bitsPerKey() {
                        return 10;
                    }

                    @Override
                    public @Nullable Boolean optimizeForHits() {
                        return true;
                    }
                };
            }

            @Override
            public @Nullable DataSize blockSize() {
                return new DataSize("128KiB");
            }

            @Override
            public @Nullable DataSize writeBufferSize() {
                return new DataSize("1MiB");
            }
        };
        try (var sstWriter = db.getSSTWriter(colId, globalDatabaseConfigOverride, fallbackColumnConfig, false, true)) {
            LOG.info("Creating sst");
            var tl = ThreadLocalRandom.current();
            var bytes = new byte[1024];
            long i = 0;
            while (i < 10_000) {
                var ib = Longs.toByteArray(i++);
                tl.nextBytes(bytes);
                sstWriter.put(ib, bytes);
            }
            LOG.info("Writing pending sst data");
            sstWriter.writePending();
            LOG.info("Done, closing");
        }
        LOG.info("Done");
    }

    @AfterEach
    public void tearDown() throws IOException {
        db.close();
    }
}
