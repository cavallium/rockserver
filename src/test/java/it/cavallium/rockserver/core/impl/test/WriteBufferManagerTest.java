package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.WriteBufferManager;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that a {@link WriteBufferManager} is always created for on-disk databases,
 * even when not explicitly configured. Without it, memtable memory grows unbounded
 * and is not tracked by the block cache.
 */
class WriteBufferManagerTest {

	@Test
	void writeBufferManagerCreatedWithoutExplicitConfig(@TempDir Path tempDir) throws Exception {
		var configFile = Files.createTempFile(tempDir, "wbm-test", ".conf");
		// Minimal config: no writeBufferManager setting
		Files.writeString(configFile, """
database: {
  global: {
    fallback-column-options: {
      levels: []
    }
  }
}
""");

		var dbPath = tempDir.resolve("testdb");
		var logger = LoggerFactory.getLogger(WriteBufferManagerTest.class);

		var config = ConfigParser.parse(configFile);
		var loadedDb = RocksDBLoader.load(dbPath, config, logger);
		try {
			// Verify a WriteBufferManager is present in refs
			boolean hasWriteBufferManager = loadedDb.refs().asList().stream()
					.anyMatch(ref -> ref instanceof WriteBufferManager);
			assertTrue(hasWriteBufferManager,
					"WriteBufferManager must be created for on-disk databases even without explicit configuration");

			// Verify cache is also present (WriteBufferManager requires it)
			assertNotNull(loadedDb.cache(),
					"Block cache must be created for on-disk databases");
		} finally {
			loadedDb.db().close();
			loadedDb.refs().close();
		}
	}

	@Test
	void maxLastLevelSstSizeComputesCorrectMultiplier(@TempDir Path tempDir) throws Exception {
		var configFile = Files.createTempFile(tempDir, "max-sst-test", ".conf");
		// first-level-sst-size=64MiB, max-last-level-sst-size=4GiB, 7 levels
		// ratio = 4GiB / 64MiB = 64, multiplier = floor(64 ^ (1/6)) = floor(2.0) = 2
		Files.writeString(configFile, """
database: {
  global: {
    fallback-column-options: {
      first-level-sst-size: "64MiB"
      max-last-level-sst-size: "4GiB"
      levels: [
        { compression: PLAIN, max-dict-bytes: 0 }
        { compression: PLAIN, max-dict-bytes: 0 }
        { compression: LZ4, max-dict-bytes: 0 }
        { compression: LZ4, max-dict-bytes: 0 }
        { compression: ZSTD, max-dict-bytes: 0 }
        { compression: ZSTD, max-dict-bytes: 0 }
        { compression: ZSTD, max-dict-bytes: 0 }
      ]
    }
    column-options: [
      {
        name: "testcol"
        first-level-sst-size: "64MiB"
        max-last-level-sst-size: "4GiB"
        levels: [
          { compression: PLAIN, max-dict-bytes: 0 }
          { compression: PLAIN, max-dict-bytes: 0 }
          { compression: LZ4, max-dict-bytes: 0 }
          { compression: LZ4, max-dict-bytes: 0 }
          { compression: ZSTD, max-dict-bytes: 0 }
          { compression: ZSTD, max-dict-bytes: 0 }
          { compression: ZSTD, max-dict-bytes: 0 }
        ]
      }
    ]
  }
}
""");

		var dbPath = tempDir.resolve("testdb-maxsst");
		var logger = LoggerFactory.getLogger(WriteBufferManagerTest.class);

		var config = ConfigParser.parse(configFile);
		var loadedDb = RocksDBLoader.load(dbPath, config, logger);
		try {
			var cfOptions = loadedDb.definitiveColumnFamilyOptionsMap().get("testcol");
			assertNotNull(cfOptions, "testcol column options must exist");
			// ratio=64, numLevels=7, multiplier = round(64^(1/6)) = 2
			assertEquals(2, cfOptions.targetFileSizeMultiplier(),
					"targetFileSizeMultiplier should be computed from maxLastLevelSstSize");
		} finally {
			loadedDb.db().close();
			loadedDb.refs().close();
		}
	}

	@Test
	void maxLastLevelSstSizeSmallRatioGivesMultiplierOne(@TempDir Path tempDir) throws Exception {
		var configFile = Files.createTempFile(tempDir, "max-sst-small-test", ".conf");
		// max-last-level-sst-size <= first-level-sst-size → multiplier = 1
		Files.writeString(configFile, """
database: {
  global: {
    fallback-column-options: {
      first-level-sst-size: "256MiB"
      max-last-level-sst-size: "256MiB"
      levels: [
        { compression: PLAIN, max-dict-bytes: 0 }
        { compression: PLAIN, max-dict-bytes: 0 }
        { compression: LZ4, max-dict-bytes: 0 }
        { compression: LZ4, max-dict-bytes: 0 }
        { compression: ZSTD, max-dict-bytes: 0 }
        { compression: ZSTD, max-dict-bytes: 0 }
        { compression: ZSTD, max-dict-bytes: 0 }
      ]
    }
    column-options: [
      {
        name: "testcol"
        first-level-sst-size: "256MiB"
        max-last-level-sst-size: "256MiB"
        levels: [
          { compression: PLAIN, max-dict-bytes: 0 }
          { compression: PLAIN, max-dict-bytes: 0 }
          { compression: LZ4, max-dict-bytes: 0 }
          { compression: LZ4, max-dict-bytes: 0 }
          { compression: ZSTD, max-dict-bytes: 0 }
          { compression: ZSTD, max-dict-bytes: 0 }
          { compression: ZSTD, max-dict-bytes: 0 }
        ]
      }
    ]
  }
}
""");

		var dbPath = tempDir.resolve("testdb-maxsst-small");
		var logger = LoggerFactory.getLogger(WriteBufferManagerTest.class);

		var config = ConfigParser.parse(configFile);
		var loadedDb = RocksDBLoader.load(dbPath, config, logger);
		try {
			var cfOptions = loadedDb.definitiveColumnFamilyOptionsMap().get("testcol");
			assertNotNull(cfOptions, "testcol column options must exist");
			assertEquals(1, cfOptions.targetFileSizeMultiplier(),
					"targetFileSizeMultiplier should be 1 when maxLastLevelSstSize <= firstLevelSstSize");
		} finally {
			loadedDb.db().close();
			loadedDb.refs().close();
		}
	}

	@Test
	void writeBufferManagerNotCreatedForInMemoryDb() throws Exception {
		var configFile = Files.createTempFile("wbm-inmem-test", ".conf");
		try {
			Files.writeString(configFile, """
database: {
  global: {
    fallback-column-options: {
      levels: []
      bloom-filter: {
        bits-per-key: 10
      }
    }
  }
}
""");

			var logger = LoggerFactory.getLogger(WriteBufferManagerTest.class);
			var config = ConfigParser.parse(configFile);
			// null path = in-memory
			var loadedDb = RocksDBLoader.load(null, config, logger);
			try {
				// In-memory databases should NOT have a WriteBufferManager (no block cache)
				boolean hasWriteBufferManager = loadedDb.refs().asList().stream()
						.anyMatch(ref -> ref instanceof WriteBufferManager);
				assertFalse(hasWriteBufferManager,
						"In-memory databases should not have a WriteBufferManager");
				assertNull(loadedDb.cache(),
						"In-memory databases should not have a block cache");
			} finally {
				loadedDb.db().close();
				loadedDb.refs().close();
			}
		} finally {
			Files.deleteIfExists(configFile);
		}
	}
}
