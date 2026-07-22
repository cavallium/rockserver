package it.cavallium.rockserver.core.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.rocksdb.WalFileType.kAliveLogFile;
import static org.rocksdb.WalFileType.kArchivedLogFile;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.rockserver.core.impl.RocksDBWalMetrics.WalFileMetadata;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RocksDBWalMetricsTest {

	private static final long LIVE_WAL_LIMIT = 4L * 1024L * 1024L * 1024L;

	@Test
	void rocksDbMetadataTypeClassifiesLiveAndArchivedWalFiles(@TempDir Path walDirectory) {
		var livePath = walDirectory.resolve("archive-looking/000001.log");
		var olderLivePath = walDirectory.resolve("archive-looking/000000.log");
		var archivedPath = walDirectory.resolve("not-an-archive/000002.log");
		var newerArchivedPath = walDirectory.resolve("not-an-archive/000003.log");
		var timestamps = Map.of(
				livePath, 10_000L,
				olderLivePath, 5_000L,
				archivedPath, 20_000L,
				newerArchivedPath, 30_000L);

		var snapshot = RocksDBWalMetrics.classify(List.of(
				new WalFileMetadata("/archive-looking/000001.log", kAliveLogFile, 100L),
				new WalFileMetadata("/archive-looking/000000.log", kAliveLogFile, 150L),
				new WalFileMetadata("/not-an-archive/000002.log", kArchivedLogFile, 200L),
				new WalFileMetadata("/not-an-archive/000003.log", kArchivedLogFile, 250L)
		), walDirectory, timestamps::get);

		assertEquals(new RocksDBWalMetrics.WalGroup(250L, 2L, 5_000L), snapshot.live());
		assertEquals(new RocksDBWalMetrics.WalGroup(450L, 2L, 20_000L), snapshot.archived());
	}

	@Test
	void exportsWalVisibilityWithoutChangingCdcReadableArchives(@TempDir Path walDirectory)
			throws Exception {
		var now = Instant.parse("2026-07-22T12:00:00Z");
		var livePath = walDirectory.resolve("000003.log");
		var archivedPath = walDirectory.resolve("archive/000001.log");
		Files.createDirectories(archivedPath.getParent());
		Files.write(livePath, new byte[] {1, 2, 3});
		var archivedContents = new byte[] {4, 5, 6, 7};
		Files.write(archivedPath, archivedContents);
		Files.setLastModifiedTime(livePath, FileTime.from(now.minusSeconds(90)));
		Files.setLastModifiedTime(archivedPath, FileTime.from(now.minusSeconds(3_600)));

		try (var registry = new SimpleMeterRegistry();
				var walMetrics = new RocksDBWalMetrics(
						"visibility-test",
						registry,
						walDirectory,
						() -> List.of(
								new WalFileMetadata("/000003.log", kAliveLogFile, 1_024L),
								new WalFileMetadata("/archive/000001.log", kArchivedLogFile, 2_048L)),
						() -> 123L,
						path -> Files.getLastModifiedTime(path).toMillis(),
						Clock.fixed(now, ZoneOffset.UTC),
						LIVE_WAL_LIMIT)) {
			walMetrics.refresh();

			assertEquals(1_024.0d, gauge(registry, RocksDBWalMetrics.LIVE_BYTES_METRIC));
			assertEquals(1.0d, gauge(registry, RocksDBWalMetrics.LIVE_FILES_METRIC));
			assertEquals(90.0d, gauge(registry, RocksDBWalMetrics.LIVE_OLDEST_AGE_METRIC));
			assertEquals(2_048.0d, gauge(registry, RocksDBWalMetrics.ARCHIVED_BYTES_METRIC));
			assertEquals(1.0d, gauge(registry, RocksDBWalMetrics.ARCHIVED_FILES_METRIC));
			assertEquals(3_600.0d, gauge(registry, RocksDBWalMetrics.ARCHIVED_OLDEST_AGE_METRIC));
			assertEquals(123.0d, gauge(registry, RocksDBWalMetrics.MIN_LOG_NUMBER_TO_KEEP_METRIC));
			assertEquals((double) LIVE_WAL_LIMIT,
					gauge(registry, RocksDBWalMetrics.CONFIGURED_LIVE_LIMIT_METRIC));
			assertArrayEquals(archivedContents, Files.readAllBytes(archivedPath),
					"metrics collection must not purge or rewrite archived WALs used by CDC");
		}
	}

	private static double gauge(MeterRegistry registry, String name) {
		return registry.get(name).tag("database", "visibility-test").gauge().value();
	}
}
