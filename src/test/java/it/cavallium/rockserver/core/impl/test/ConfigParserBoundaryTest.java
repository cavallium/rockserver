package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.config.ConfigPrinter;
import it.cavallium.rockserver.core.config.DataSize;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigParserBoundaryTest {

	@TempDir
	Path tempDir;

	@Test
	void defaultConfigurationExposesOperationalDefaults() {
		var config = ConfigParser.parse(null);

		assertAll(
				() -> assertEquals(30, config.parallelism().read()),
				() -> assertEquals(10, config.parallelism().write()),
				() -> assertEquals("default", config.metrics().databaseName()),
				() -> assertFalse(config.metrics().influx().enabled()),
				() -> assertTrue(config.metrics().influx().allowInsecureCertificates()),
				() -> assertTrue(config.metrics().jmx().enabled()),
				() -> assertTrue(config.global().followRocksdbOptimizations()),
				() -> assertTrue(config.global().paranoidChecks()),
				() -> assertFalse(config.global().useClockCache()),
				() -> assertTrue(config.global().enableFastGet()),
				() -> assertTrue(config.global().checksum()),
				() -> assertTrue(config.global().absoluteConsistency()),
				() -> assertFalse(config.global().unorderedWrite()),
				() -> assertFalse(config.global().allowRocksdbMmapWrites()),
				() -> assertEquals(-1, config.global().maximumOpenFiles()),
				() -> assertFalse(config.global().openFilesAsync()),
				() -> assertNull(config.global().maxFileOpeningThreads()),
				() -> assertFalse(config.global().disableAutoCompactions()),
				() -> assertFalse(config.global().disableWriteSlowdown()),
				() -> assertEquals(new DataSize("512MiB"), config.global().blockCache()),
				() -> assertEquals(0.5d, config.global().blockCacheHighPriorityRatio()),
				() -> assertNull(config.global().databaseWriteBufferSize()),
				() -> assertEquals(Duration.ofSeconds(5), config.global().delayWalFlushDuration()),
				() -> assertNull(config.global().maxSubcompactions()),
				() -> assertNull(config.global().maxBackgroundJobs()),
				() -> assertEquals("default", config.global().columnOptions()[0].name())
		);
	}

	@Test
	void laterSourcesOverrideEarlierSourcesWithoutDiscardingDefaults() throws IOException {
		Path first = write("first.conf", """
				database.parallelism.read = 7
				database.global.block-cache = 1GiB
				database.global.max-background-jobs = 2
				""");
		Path second = write("second.conf", """
				database.parallelism.read = 9
				database.global.max-background-jobs = 3
				database.global.unordered-write = true
				""");
		var parser = new ConfigParser();
		parser.addSource(null);
		parser.addSource(first);
		parser.addSource(second);

		var config = parser.parse();

		assertAll(
				() -> assertEquals(9, config.parallelism().read()),
				() -> assertEquals(10, config.parallelism().write()),
				() -> assertEquals(new DataSize("1GiB"), config.global().blockCache()),
				() -> assertEquals(3, config.global().maxBackgroundJobs()),
				() -> assertTrue(config.global().unorderedWrite()),
				() -> assertTrue(config.global().checksum())
		);
	}

	@Test
	void invalidDataSizeIsReportedAsConfigError() throws IOException {
		Path invalid = write("invalid-size.conf", "database.global.block-cache = 1KiX");

		var exception = assertThrows(RocksDBException.class, () -> ConfigParser.parse(invalid));

		assertEquals(RocksDBErrorType.CONFIG_ERROR, exception.getErrorUniqueId());
	}

	@Test
	void invalidScalarIsReportedAsConfigError() throws IOException {
		Path invalid = write("invalid-scalar.conf", "database.parallelism.read = definitely-not-an-integer");

		var exception = assertThrows(RocksDBException.class, () -> ConfigParser.parse(invalid));

		assertEquals(RocksDBErrorType.CONFIG_ERROR, exception.getErrorUniqueId());
	}

	@Test
	void missingSourceIsReportedAsConfigError() {
		Path missing = tempDir.resolve("missing.conf");

		var exception = assertThrows(RocksDBException.class, () -> ConfigParser.parse(missing));

		assertEquals(RocksDBErrorType.CONFIG_ERROR, exception.getErrorUniqueId());
	}

	@Test
	void printedDefaultConfigurationCanBeParsedWithoutChangingValues() throws IOException {
		var original = ConfigParser.parseDefault();
		Path printed = write("printed-default.conf", "database: " + ConfigPrinter.stringify(original));

		var reparsed = ConfigParser.parse(printed);

		assertAll(
				() -> assertEquals(original.parallelism().read(), reparsed.parallelism().read()),
				() -> assertEquals(original.parallelism().write(), reparsed.parallelism().write()),
				() -> assertEquals(original.metrics().databaseName(), reparsed.metrics().databaseName()),
				() -> assertEquals(original.global().followRocksdbOptimizations(),
						reparsed.global().followRocksdbOptimizations()),
				() -> assertEquals(original.global().paranoidChecks(), reparsed.global().paranoidChecks()),
				() -> assertEquals(original.global().useClockCache(), reparsed.global().useClockCache()),
				() -> assertEquals(original.global().allowRocksdbMmapWrites(),
						reparsed.global().allowRocksdbMmapWrites()),
				() -> assertEquals(original.global().openFilesAsync(), reparsed.global().openFilesAsync()),
				() -> assertEquals(original.global().maxFileOpeningThreads(),
						reparsed.global().maxFileOpeningThreads()),
				() -> assertEquals(original.global().disableAutoCompactions(),
						reparsed.global().disableAutoCompactions()),
				() -> assertEquals(original.global().disableWriteSlowdown(),
						reparsed.global().disableWriteSlowdown()),
				() -> assertEquals(original.global().blockCache(), reparsed.global().blockCache()),
				() -> assertEquals(original.global().blockCacheHighPriorityRatio(),
						reparsed.global().blockCacheHighPriorityRatio()),
				() -> assertEquals(original.global().databaseWriteBufferSize(),
						reparsed.global().databaseWriteBufferSize()),
				() -> assertEquals(original.global().maxSubcompactions(), reparsed.global().maxSubcompactions()),
				() -> assertEquals(original.global().fallbackColumnOptions().levels().length,
						reparsed.global().fallbackColumnOptions().levels().length),
				() -> assertEquals(original.global().fallbackColumnOptions().levels()[0].compression(),
						reparsed.global().fallbackColumnOptions().levels()[0].compression()),
				() -> assertEquals(original.global().fallbackColumnOptions().volumes()[0].volumePath(),
						reparsed.global().fallbackColumnOptions().volumes()[0].volumePath()),
				() -> assertEquals(original.global().fallbackColumnOptions().volumes()[0].targetSize(),
						reparsed.global().fallbackColumnOptions().volumes()[0].targetSize()),
				() -> assertEquals(original.global().fallbackColumnOptions().memtableMaxRangeDeletions(),
						reparsed.global().fallbackColumnOptions().memtableMaxRangeDeletions()),
				() -> assertEquals(original.global().columnOptions()[0].name(),
						reparsed.global().columnOptions()[0].name()),
				() -> assertEquals(original.global().columnOptions()[0].mergeOperatorClass(),
						reparsed.global().columnOptions()[0].mergeOperatorClass())
		);
	}

	@Test
	void printedConfigurationPreservesNonDefaultGlobalValues() throws IOException {
		Path custom = write("custom.conf", """
				database.global.follow-rocksdb-optimizations = false
				database.global.paranoid-checks = false
				database.global.use-clock-cache = true
				database.global.allow-rocksdb-mmap-writes = true
				database.global.open-files-async = true
				database.global.max-file-opening-threads = 7
				database.global.disable-auto-compactions = true
				database.global.disable-write-slowdown = true
				database.global.temp-sst-path = ./custom-temp
				database.global.delay-wal-flush-duration = PT0.125S
				database.global.unordered-write = true
				database.global.max-background-jobs = 3
				database.global.max-subcompactions = 2
				database.global.database-write-buffer-size = 2GiB
				database.global.block-cache-high-priority-ratio = 0.25
				""");
		var original = ConfigParser.parse(custom);
		Path printed = write("printed-custom.conf", "database: " + ConfigPrinter.stringify(original));

		var reparsed = ConfigParser.parse(printed);

		assertAll(
				() -> assertFalse(reparsed.global().followRocksdbOptimizations()),
				() -> assertFalse(reparsed.global().paranoidChecks()),
				() -> assertTrue(reparsed.global().useClockCache()),
				() -> assertTrue(reparsed.global().allowRocksdbMmapWrites()),
				() -> assertTrue(reparsed.global().openFilesAsync()),
				() -> assertEquals(7, reparsed.global().maxFileOpeningThreads()),
				() -> assertTrue(reparsed.global().disableAutoCompactions()),
				() -> assertTrue(reparsed.global().disableWriteSlowdown()),
				() -> assertEquals(original.global().tempSstPath(), reparsed.global().tempSstPath()),
				() -> assertEquals(original.global().delayWalFlushDuration(), reparsed.global().delayWalFlushDuration()),
				() -> assertEquals(original.global().unorderedWrite(), reparsed.global().unorderedWrite()),
				() -> assertEquals(2, reparsed.global().maxSubcompactions()),
				() -> assertEquals(new DataSize("2GiB"), reparsed.global().databaseWriteBufferSize()),
				() -> assertEquals(original.global().maxBackgroundJobs(), reparsed.global().maxBackgroundJobs()),
				() -> assertEquals(0.25d, reparsed.global().blockCacheHighPriorityRatio())
		);
	}

	private Path write(String name, String content) throws IOException {
		return Files.writeString(tempDir.resolve(name), content);
	}
}
