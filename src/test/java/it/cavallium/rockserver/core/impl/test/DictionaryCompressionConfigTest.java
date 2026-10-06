package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.*;

import it.cavallium.rockserver.core.config.ColumnLevelConfig;
import it.cavallium.rockserver.core.config.DatabaseConfig;
import it.cavallium.rockserver.core.config.FallbackColumnConfig;
import it.cavallium.rockserver.core.config.GlobalDatabaseConfig;
import it.cavallium.rockserver.core.config.NamedColumnConfig;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.config.ConfigPrinter;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.slf4j.LoggerFactory;

class DictionaryCompressionConfigTest {

    @Test
    void persistsDefaultAndEmptyLevelFallback(@TempDir Path dir) throws Exception {
        for (boolean emptyLevels : new boolean[]{false, true}) {
            Path configPath = dir.resolve("config-" + emptyLevels + ".conf");
            Files.writeString(configPath, "database.global.column-options=[]\n" + (emptyLevels
                    ? "database.global.fallback-column-options.levels=[]" : ""));
            var parsed = ConfigParser.parse(configPath);
            var config = parsed;
            if (emptyLevels) {
                var fallback = mock(FallbackColumnConfig.class, delegatesTo(parsed.global().fallbackColumnOptions()));
                doReturn(new ColumnLevelConfig[0]).when(fallback).levels();
                var global = mock(GlobalDatabaseConfig.class, delegatesTo(parsed.global()));
                doReturn(new NamedColumnConfig[0]).when(global).columnOptions();
                doReturn(fallback).when(global).fallbackColumnOptions();
                config = mock(DatabaseConfig.class, delegatesTo(parsed));
                doReturn(global).when(config).global();
            }
            var loaded = RocksDBLoader.load(dir.resolve("db-" + emptyLevels), config,
                    LoggerFactory.getLogger(getClass()));
            try {
                String options = persistedOptions(loaded.definitiveDbPath());
                assertCompression(options, "compression_opts", 64L << 20, 0, 0);
                assertCompression(options, "bottommost_compression_opts", 64L << 20, 32768, 3276800);
            } finally {
                loaded.db().close();
                loaded.refs().close();
            }
        }
    }

    @Test
    void copiesExplicitLimitsIntoExternalWriterAndRetainsDataOnReopen(@TempDir Path dir) throws Exception {
        Path configPath = dir.resolve("config.conf");
        Files.writeString(configPath, """
                database.global.column-options=[]
                database.global.fallback-column-options.volumes=[{volume-path: ".", target-size: "1GiB"}]
                database.global.fallback-column-options.levels=[
                  { compression: ZSTD, max-dict-bytes: 4KiB, max-dict-buffer-bytes: 8MiB }
                  { compression: ZSTD, max-dict-bytes: 0 }
                  { compression: ZSTD, max-dict-bytes: 0 }
                  { compression: ZSTD, max-dict-bytes: 0 }
                  { compression: ZSTD, max-dict-bytes: 0 }
                  { compression: ZSTD, max-dict-bytes: 0 }
                  { compression: ZSTD, max-dict-bytes: 32KiB, max-dict-buffer-bytes: 16MiB }
                ]
                database.global.column-options=[${database.global.fallback-column-options} {name: "default"}]
                """);
        Path printedConfig = dir.resolve("printed.conf");
        Files.writeString(printedConfig, "database: " + ConfigPrinter.stringify(ConfigParser.parse(configPath)));
        assertEquals(16L << 20, ConfigParser.parse(printedConfig).global().fallbackColumnOptions()
                .levels()[6].maxDictBufferBytes().longValue());
        Path dbPath = dir.resolve("db");
        try (var db = new EmbeddedDB(dbPath, "dictionary-test", configPath)) {
            long column = db.createColumn("test", ColumnSchema.of(IntList.of(4), ObjectList.of(), true));
            for (int round = 0; round < 2; round++) {
                try (var writer = db.getSSTWriter(column, null, false, false)) {
                    var writerOptions = writer.refs().asList().stream().filter(Options.class::isInstance)
                            .map(Options.class::cast).findFirst().orElseThrow();
                    // Emit native OPTIONS from the actual writer options, not Java cached getters.
                    try (var copied = new Options(writerOptions).setCfPaths(java.util.List.of()).setCreateIfMissing(true);
                         var probe = RocksDB.open(copied, dir.resolve("writer-options-" + round).toString())) {
                        String options = persistedOptions(dir.resolve("writer-options-" + round));
                        assertCompression(options, "compression_opts", 8L << 20, 4096, 409600);
                        assertCompression(options, "bottommost_compression_opts", 16L << 20, 32768, 3276800);
                        assertTrue(options.contains("format_version=6"));
                    }
                    for (int i = round * 100; i < (round + 1) * 100; i++) {
                        writer.put(key(i), value(i));
                    }
                    writer.writePending();
                    assertFalse(writer.sstFileWriter().isOwningHandle());
                    assertTrue(writer.refs().asList().stream().filter(org.rocksdb.RocksObject.class::isInstance)
                            .map(org.rocksdb.RocksObject.class::cast).noneMatch(org.rocksdb.RocksObject::isOwningHandle));
                    for (int i = 0; i < (round + 1) * 100; i++) {
                        assertArrayEquals(value(i), db.getDb().get().get(writer.col().cfh(), key(i)));
                    }
                }
            }
            String options = persistedOptions(dbPath);
            assertTrue(options.contains("max_dict_buffer_bytes=16777216;"));
        }
        try (var db = new EmbeddedDB(dbPath, "dictionary-test", configPath);
             var writer = db.getSSTWriter(db.getColumnId("test"), null, false, false)) {
            for (int i = 0; i < 200; i++) {
                assertArrayEquals(value(i), db.getDb().get().get(writer.col().cfh(), key(i)));
            }
        }
    }

    @Test
    void closesWriterResourcesWhenNativeOpenFails(@TempDir Path dir) throws Exception {
        try (var db = new EmbeddedDB(dir.resolve("db"), "open-failure", null)) {
            long column = db.createColumn("test", ColumnSchema.of(IntList.of(4), ObjectList.of(), true));
            try (var writer = db.getSSTWriter(column, null, false, false)) {
                Path notDirectory = dir.resolve("regular-file");
                Files.writeString(notDirectory, "");
                var refs = new it.cavallium.rockserver.core.impl.rocksdb.RocksDBObjects();
                var columnOptions = RocksDBLoader.getColumnOptions("test", dir.resolve("db"), dir.resolve("db"),
                        ConfigParser.parseDefault().global(), LoggerFactory.getLogger(getClass()), refs,
                        false, java.util.Map.of());
                refs.add(() -> { throw new IllegalStateException("cleanup-failed"); });
                var openError = assertThrows(org.rocksdb.RocksDBException.class,
                        () -> it.cavallium.rockserver.core.impl.rocksdb.SSTWriter.open(notDirectory, db.getDb(),
                                writer.col(), columnOptions.options(), false, false, refs));
                assertTrue(refs.asList().stream().filter(org.rocksdb.RocksObject.class::isInstance)
                        .map(org.rocksdb.RocksObject.class::cast).noneMatch(org.rocksdb.RocksObject::isOwningHandle));
                assertEquals(1, openError.getSuppressed().length);
                assertEquals("cleanup-failed", openError.getSuppressed()[0].getCause().getMessage());
                assertThrows(RuntimeException.class, refs::close);
            }
        }
    }

    @Test
    void supportsExplicitUnboundedAndRejectsNegativeLimit(@TempDir Path dir) throws Exception {
        Path configPath = dir.resolve("config.conf");
        Files.writeString(configPath, """
                database.global.column-options=[]
                database.global.fallback-column-options.volumes=[{volume-path: ".", target-size: "1GiB"}]
                database.global.fallback-column-options.levels=[
                  { compression: ZSTD, max-dict-bytes: 0 }
                  { compression: ZSTD, max-dict-bytes: 0 }
                  { compression: ZSTD, max-dict-bytes: 0 }
                  { compression: ZSTD, max-dict-bytes: 0 }
                  { compression: ZSTD, max-dict-bytes: 0 }
                  { compression: ZSTD, max-dict-bytes: 0 }
                  { compression: ZSTD, max-dict-bytes: 32KiB, max-dict-buffer-bytes: 0 }
                ]
                database.global.column-options=[${database.global.fallback-column-options} {name: "default"}]
                """);
        var loaded = RocksDBLoader.load(dir.resolve("db"), ConfigParser.parse(configPath),
                LoggerFactory.getLogger(getClass()));
        try {
            assertCompression(persistedOptions(loaded.definitiveDbPath()), "bottommost_compression_opts",
                    0, 32768, 3276800);
        } finally {
            loaded.db().close();
            loaded.refs().close();
        }
        Files.writeString(configPath, Files.readString(configPath).replace("max-dict-buffer-bytes: 0", "max-dict-buffer-bytes: -1"));
        var invalid = ConfigParser.parse(configPath);
        var error = assertThrows(it.cavallium.rockserver.core.common.RocksDBException.class,
                () -> RocksDBLoader.load(dir.resolve("invalid"), invalid, LoggerFactory.getLogger(getClass())));
        assertTrue(error.getMessage().contains("max-dict-buffer-bytes"));
    }

    private static byte[] key(int i) {
        return ByteBuffer.allocate(4).putInt(i).array();
    }

    private static byte[] value(int i) {
        return ("dictionary compression test value " + i).repeat(30).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static String persistedOptions(Path db) throws Exception {
        try (var files = Files.list(db)) {
            return Files.readString(files.filter(p -> p.getFileName().toString().startsWith("OPTIONS-"))
                    .max(Path::compareTo).orElseThrow());
        }
    }

    private static void assertCompression(String options, String name, long buffer, int dictionary, int training) {
        String line = options.lines().map(String::strip).filter(s -> s.startsWith(name + "={"))
                .findFirst().orElseThrow();
        assertTrue(line.contains("max_dict_buffer_bytes=" + buffer + ";"), line);
        assertTrue(line.contains("max_dict_bytes=" + dictionary + ";"), line);
        assertTrue(line.contains("zstd_max_train_bytes=" + training + ";"), line);
        assertTrue(line.contains("window_bits=-14;"), line);
        assertTrue(line.contains("level=32767;"), line);
        assertTrue(line.contains("strategy=0;"), line);
    }
}
