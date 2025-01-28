package it.cavallium.rockserver.core.impl.test;

import com.google.common.primitives.Longs;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.github.gestalt.config.exceptions.GestaltException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ThreadLocalRandom;

public class TestSSTWriter {

    private static final Logger LOG = LoggerFactory.getLogger(TestSSTWriter.class);

    private EmbeddedDB db;
    private long colId;
    private Path tempSstPath;

    @BeforeEach
    public void setUp() throws IOException {
        db = new EmbeddedDB(null, "test", null);
        this.colId = db.createColumn("test", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
        this.tempSstPath = Files.createTempDirectory("tempssts");
    }

    @Test
    public void test() throws IOException {
        LOG.info("Obtaining sst writer");
        try (var sstWriter = db.getSSTWriter(colId, null, true, false)) {
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

    @Test
    public void testCompression() throws IOException, RocksDBException, GestaltException {
        LOG.info("Obtaining sst writer");
        try (var sstWriter = db.getSSTWriter(colId, null, false, false)) {
            LOG.info("Creating sst");
            var tl = ThreadLocalRandom.current();
            var bytes = new byte[1024];
            long i = 0;
            while (i < 1_000) {
                var ib = Longs.toByteArray(i++);
                tl.nextBytes(bytes);
                sstWriter.put(ib, bytes);
            }
            LOG.info("Writing pending sst data");
            sstWriter.writePending();
            LOG.info("Done, closing");
        }
        var transactionalDB = db.getDb();
        var rocksDB = transactionalDB.get();
        var metadata = rocksDB.getLiveFilesMetaData();
        Assertions.assertEquals(1, metadata.size(), "There are more than one sst files");
        var sstMetadata = metadata.getFirst();
        var sstPath = Path.of(sstMetadata.path(), sstMetadata.fileName());
        Assertions.assertTrue(Files.exists(sstPath), "SST file does not exists");
        try (var options = new Options(); var sstReader = new SstFileReader(options)) {
            sstReader.open(sstPath.toString());
            var p = sstReader.getTableProperties();
            Assertions.assertNotEquals("snappy", p.getCompressionName().toLowerCase());
        }
        LOG.info("Done");
    }

    @AfterEach
    public void tearDown() throws IOException {
        db.close();
        Files.walkFileTree(tempSstPath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }
        });
        Files.deleteIfExists(tempSstPath);
    }
}
