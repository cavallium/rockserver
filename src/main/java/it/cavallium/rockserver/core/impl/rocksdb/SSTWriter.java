package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.ColumnInstance;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

public record SSTWriter(RocksDB db, it.cavallium.rockserver.core.impl.ColumnInstance col, Path path, SstFileWriter sstFileWriter, boolean ingestBehind,
                        RocksDBObjects refs) implements Closeable, DBWriter {

    private static final Logger LOG = LoggerFactory.getLogger(SSTWriter.class);

    public static SSTWriter open(Path tempSSTsPath, TransactionalDB db, ColumnInstance col, ColumnFamilyOptions columnConifg, boolean forceNoOptions, boolean ingestBehind, RocksDBObjects refs) throws IOException, org.rocksdb.RocksDBException {
        if (refs == null) {
            refs = new RocksDBObjects();
        }
        var envOptions = new EnvOptions();
        if (!forceNoOptions) {
            envOptions
                    .setAllowFallocate(true)
                    .setWritableFileMaxBufferSize(10 * SizeUnit.MB)
                    .setRandomAccessMaxBufferSize(10 * SizeUnit.MB)
                    .setCompactionReadaheadSize(2 * SizeUnit.MB)
                    .setBytesPerSync(10 * SizeUnit.MB);
        }
        refs.add(envOptions);

        var options = new Options();
        refs.add(options);
        if (!forceNoOptions) {
            options
                    .setDisableAutoCompactions(true)
                    .setManualWalFlush(true)
                    .setUseDirectIoForFlushAndCompaction(true)
                    .setBytesPerSync(5 * SizeUnit.MB)
                    .setParanoidChecks(false)
                    .setSkipCheckingSstFileSizesOnDbOpen(true)
                    .setForceConsistencyChecks(false)
                    .setParanoidFileChecks(false);
            if (columnConifg != null) {
                options
                        .setNumLevels(columnConifg.numLevels())
                        .setTableFormatConfig(columnConifg.tableFormatConfig())
                        .setTargetFileSizeBase(columnConifg.targetFileSizeBase())
                        .setTargetFileSizeMultiplier(columnConifg.targetFileSizeMultiplier())
                        .setMaxOpenFiles(-1)
                        .setCompressionPerLevel(columnConifg.compressionPerLevel())
                        .setCompressionType(columnConifg.compressionType())
                        .setCompressionOptions(cloneCompressionOptions(columnConifg.compressionOptions()))
                        .setBottommostCompressionType(columnConifg.bottommostCompressionType())
                        .setBottommostCompressionOptions(cloneCompressionOptions(columnConifg.bottommostCompressionOptions()));
                if (columnConifg.memTableConfig() != null) {
                        options.setMemTableConfig(columnConifg.memTableConfig());
                }
            }
        }
        Path tempFile;
        try {
            var tempDir = tempSSTsPath;
            if (Files.notExists(tempDir)) {
                Files.createDirectories(tempDir);
            }
            tempFile = tempDir.resolve(UUID.randomUUID() + ".sst");
        } catch (IOException ex) {
            refs.close();
            throw ex;
        }
        var sstFileWriter = new SstFileWriter(envOptions, options);
        var sstWriter = new SSTWriter(db.get(), col, tempFile, sstFileWriter, ingestBehind, refs);
        sstFileWriter.open(tempFile.toString());
        return sstWriter;
    }

    private static CompressionOptions cloneCompressionOptions(CompressionOptions compressionOptions) {
        return new CompressionOptions()
                .setEnabled(compressionOptions.enabled())
                .setMaxDictBytes(compressionOptions.maxDictBytes())
                .setLevel(compressionOptions.level())
                .setStrategy(compressionOptions.strategy())
                .setZStdMaxTrainBytes(compressionOptions.zstdMaxTrainBytes())
                .setWindowBits(compressionOptions.windowBits());
    }

    public void put(byte[] key, byte[] value) throws RocksDBException {
        try {
            sstFileWriter.put(key, value);
        } catch (org.rocksdb.RocksDBException e) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_UNKNOWN_ERROR, e);
        }
    }

    public void put(ByteBuffer key, ByteBuffer value) throws RocksDBException {
        try {
            sstFileWriter.put(key, value);
        } catch (org.rocksdb.RocksDBException e) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_UNKNOWN_ERROR, e);
        }
    }

    @Override
    public void writePending() throws it.cavallium.rockserver.core.common.RocksDBException {
        try {
            checkOwningHandle();
            try (this) {
                sstFileWriter.finish();
                try (var ingestOptions = new IngestExternalFileOptions()) {
                    ingestOptions
                            .setIngestBehind(ingestBehind)
                            .setAllowBlockingFlush(true)
                            .setMoveFiles(true)
                            .setAllowGlobalSeqNo(true)
                            .setWriteGlobalSeqno(false)
                            .setSnapshotConsistency(false);
                    db.ingestExternalFile(col.cfh(), List.of(path.toString()), ingestOptions);
                }
            }
        } catch (org.rocksdb.RocksDBException e) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.SST_WRITE_1, e);
        }
    }

    private void checkOwningHandle() {
        if (!sstFileWriter.isOwningHandle()) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.SST_WRITE_4, "SST writer is closed");
        }
    }

    @Override
    public void close() {
        if (sstFileWriter.isOwningHandle()) {
            sstFileWriter.close();
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                LOG.error("Failed to delete a file: {}", path, e);
            }
        }
        refs.close();
    }

    public long fileSize() {
        if (!sstFileWriter.isOwningHandle()) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.SST_GET_SIZE_FAILED, "The SSTWriter is closed");
        }
        try {
            return sstFileWriter.fileSize();
        } catch (org.rocksdb.RocksDBException e) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.SST_GET_SIZE_FAILED, e);
        }
    }
}
