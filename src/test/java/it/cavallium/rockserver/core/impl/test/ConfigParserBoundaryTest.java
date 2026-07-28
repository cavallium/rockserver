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
import it.cavallium.rockserver.core.config.WorkloadSettings;
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
				() -> assertEquals(20, config.parallelism().read()),
				() -> assertEquals(36, config.parallelism().write()),
				() -> assertEquals(4_096, config.parallelism().workload().latencyQueueCapacity()),
				() -> assertEquals(1, config.parallelism().workload().readLatencyReservation()),
				() -> assertEquals(Duration.ofSeconds(60),
						config.parallelism().workload().retainedSnapshotMaximumAge()),
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
				() -> assertEquals(new DataSize("4GiB"), config.global().maxTotalWalSize()),
				() -> assertEquals(86_400L, config.global().walTtlSeconds()),
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
				() -> assertEquals(36, config.parallelism().write()),
				() -> assertEquals(4_096, config.parallelism().workload().latencyQueueCapacity()),
				() -> assertEquals(512, config.parallelism().workload().batchQueueCapacity()),
				() -> assertEquals(new DataSize("1GiB"), config.global().blockCache()),
				() -> assertEquals(3, config.global().maxBackgroundJobs()),
				() -> assertTrue(config.global().unorderedWrite()),
				() -> assertTrue(config.global().checksum())
		);
	}

	@Test
	void defaultWorkloadConfigurationIsCompleteAndPrintedWithoutLegacyLanes() throws Exception {
		var config = ConfigParser.parseDefault();
		var settings = WorkloadSettings.resolve(config);
		var printed = ConfigPrinter.stringify(config);

		assertEquals(WorkloadSettings.defaults(20, 36), settings);
		assertFalse(printed.contains("maintenance-write"));
		assertFalse(printed.contains("foreground-write-queue-capacity"));
		assertFalse(printed.contains("maintenance-write-queue-capacity"));
		for (String key : new String[] {
				"latency-queue-capacity",
				"ingest-queue-capacity",
				"cdc-queue-capacity",
				"analytical-queue-capacity",
				"batch-queue-capacity",
				"control-queue-capacity",
				"physical-maintenance-queue-capacity",
				"read-latency-reservation",
				"read-ingest-reservation",
				"read-cdc-reservation",
				"write-latency-reservation",
				"write-ingest-reservation",
				"write-cdc-reservation",
				"control-threads",
				"physical-concurrency",
				"analytical-active-limit",
				"retained-analytical-snapshots",
				"retained-snapshot-maximum-age",
				"latency-burst",
				"ingest-drr-weight",
				"cdc-drr-weight",
				"analytical-drr-weight",
				"batch-drr-weight",
				"pressured-batch-maximum-active",
				"pressured-batch-interval",
				"range-quantum-max-items",
				"range-quantum-max-bytes",
				"range-quantum-max-duration",
				"cdc-quantum-max-mutations",
				"cdc-quantum-max-bytes",
				"cdc-quantum-max-duration",
				"latency-range-max-items",
				"latency-range-max-bytes",
				"latency-fan-out-max-items",
				"latency-fan-out-max-bytes"
		}) {
			assertTrue(printed.contains('"' + key + '"'), "missing effective key " + key);
		}
	}

	@Test
	void invalidWorkloadCapacitiesReservationsAndDurationsAreConfigErrors() throws IOException {
		String[] invalidOverrides = {
				"database.parallelism.read = 2",
				"database.parallelism.workload.latency-queue-capacity = 0",
				"database.parallelism.workload.read-latency-reservation = -1",
				"database.parallelism.read = 3\n"
						+ "database.parallelism.workload.read-latency-reservation = 2",
				"database.parallelism.workload.analytical-active-limit = 21",
				"database.parallelism.workload.control-threads = 0",
				"database.parallelism.workload.retained-snapshot-maximum-age = PT0S",
				"database.parallelism.workload.pressured-batch-interval = PT-1S",
				"database.parallelism.workload.range-quantum-max-duration = PT0S",
				"database.parallelism.workload.cdc-quantum-max-bytes = 0B"
		};
		for (int i = 0; i < invalidOverrides.length; i++) {
			Path invalid = write("invalid-workload-" + i + ".conf", invalidOverrides[i]);
			var exception = assertThrows(RocksDBException.class,
					() -> ConfigParser.parse(invalid), invalidOverrides[i]);
			assertEquals(RocksDBErrorType.CONFIG_ERROR, exception.getErrorUniqueId());
		}
	}

	@Test
	void removedForegroundAndMaintenanceKeysFailInsteadOfFallingBack() throws IOException {
		for (String key : new String[] {
				"maintenance-write",
				"foreground-write-queue-capacity",
				"maintenance-write-queue-capacity"
		}) {
			Path invalid = write("removed-" + key + ".conf", "database.parallelism." + key + " = 1");
			var exception = assertThrows(RocksDBException.class, () -> ConfigParser.parse(invalid));
			assertEquals(RocksDBErrorType.CONFIG_ERROR, exception.getErrorUniqueId());
			assertTrue(exception.getMessage().contains("Removed workload configuration key"));
		}
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
				() -> assertEquals(ConfigPrinter.stringifyWorkload(original.parallelism().workload()),
						ConfigPrinter.stringifyWorkload(reparsed.parallelism().workload())),
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
				() -> assertEquals(original.global().maxTotalWalSize(),
						reparsed.global().maxTotalWalSize()),
				() -> assertEquals(original.global().walTtlSeconds(), reparsed.global().walTtlSeconds()),
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
				database.parallelism.read = 7
				database.parallelism.write = 12
				database.parallelism.workload.latency-queue-capacity = 123
				database.parallelism.workload.ingest-queue-capacity = 124
				database.parallelism.workload.batch-queue-capacity = 45
				database.parallelism.workload.control-threads = 3
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
				database.global.max-total-wal-size = 6GiB
				database.global.wal-ttl-seconds = 43200
				database.global.block-cache-high-priority-ratio = 0.25
				""");
		var original = ConfigParser.parse(custom);
		Path printed = write("printed-custom.conf", "database: " + ConfigPrinter.stringify(original));

		var reparsed = ConfigParser.parse(printed);

		assertAll(
				() -> assertEquals(7, reparsed.parallelism().read()),
				() -> assertEquals(12, reparsed.parallelism().write()),
				() -> assertEquals(123, reparsed.parallelism().workload().latencyQueueCapacity()),
				() -> assertEquals(124, reparsed.parallelism().workload().ingestQueueCapacity()),
				() -> assertEquals(45, reparsed.parallelism().workload().batchQueueCapacity()),
				() -> assertEquals(3, reparsed.parallelism().workload().controlThreads()),
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
				() -> assertEquals(new DataSize("6GiB"), reparsed.global().maxTotalWalSize()),
				() -> assertEquals(43_200L, reparsed.global().walTtlSeconds()),
				() -> assertEquals(original.global().maxBackgroundJobs(), reparsed.global().maxBackgroundJobs()),
				() -> assertEquals(0.25d, reparsed.global().blockCacheHighPriorityRatio())
		);
	}

	private Path write(String name, String content) throws IOException {
		return Files.writeString(tempDir.resolve(name), content);
	}
}
