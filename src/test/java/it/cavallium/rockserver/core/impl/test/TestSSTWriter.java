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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
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
        try (var sstWriter = db.getSSTWriter(colId, null, null, true, false)) {
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
