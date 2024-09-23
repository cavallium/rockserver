package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.RocksDBException;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.SstFileWriter;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.List;

public record SSTWriter(RocksDB db, it.cavallium.rockserver.core.impl.ColumnInstance col, Path path, SstFileWriter sstFileWriter, boolean ingestBehind) implements Closeable, DBWriter {

    @Override
    public void writePending() throws it.cavallium.rockserver.core.common.RocksDBException {
        try {
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
        } catch (org.rocksdb.RocksDBException e) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.SST_WRITE_1, e);
        }
    }

    @Override
    public void close() {
        sstFileWriter.close();
    }
}
