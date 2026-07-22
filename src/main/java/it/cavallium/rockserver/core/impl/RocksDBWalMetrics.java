package it.cavallium.rockserver.core.impl;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.rocksdb.LogFile;
import org.rocksdb.RocksDB;
import org.rocksdb.WalFileType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RocksDBWalMetrics implements AutoCloseable {

	static final String LIVE_BYTES_METRIC = "rocksdb.wal.live.bytes";
	static final String LIVE_FILES_METRIC = "rocksdb.wal.live.files";
	static final String LIVE_OLDEST_AGE_METRIC = "rocksdb.wal.live.oldest.age.seconds";
	static final String ARCHIVED_BYTES_METRIC = "rocksdb.wal.archived.bytes";
	static final String ARCHIVED_FILES_METRIC = "rocksdb.wal.archived.files";
	static final String ARCHIVED_OLDEST_AGE_METRIC = "rocksdb.wal.archived.oldest.age.seconds";
	static final String MIN_LOG_NUMBER_TO_KEEP_METRIC = "rocksdb.wal.min.log.number.to.keep";
	static final String CONFIGURED_LIVE_LIMIT_METRIC = "rocksdb.wal.configured.live.limit.bytes";

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBWalMetrics.class);
	private static final long NO_TIMESTAMP = -1L;
	private static final WalGroup EMPTY_GROUP = new WalGroup(0L, 0L, NO_TIMESTAMP);
	private static final WalSnapshot EMPTY_SNAPSHOT = new WalSnapshot(EMPTY_GROUP, EMPTY_GROUP, 0L);

	private final Path walDirectory;
	private final WalMetadataSource walMetadataSource;
	private final MinLogNumberSource minLogNumberSource;
	private final FileTimestampSource fileTimestampSource;
	private final Clock clock;
	private final AtomicReference<WalSnapshot> snapshot = new AtomicReference<>(EMPTY_SNAPSHOT);
	private final List<Meter> meters = new ArrayList<>(8);
	private final AtomicBoolean closed = new AtomicBoolean();

	RocksDBWalMetrics(String databaseName,
			MeterRegistry registry,
			RocksDB rocksDB,
			Path walDirectory,
			long configuredLiveWalLimitBytes) {
		this(databaseName,
				registry,
				walDirectory,
				() -> rocksDB.getSortedWalFiles().stream().map(WalFileMetadata::from).toList(),
				() -> rocksDB.getLongProperty(RocksDBLongProperty.MIN_LOG_NUMBER_TO_KEEP.getName()),
				path -> Files.getLastModifiedTime(path).toMillis(),
				Clock.systemUTC(),
				configuredLiveWalLimitBytes);
	}

	RocksDBWalMetrics(String databaseName,
			MeterRegistry registry,
			Path walDirectory,
			WalMetadataSource walMetadataSource,
			MinLogNumberSource minLogNumberSource,
			FileTimestampSource fileTimestampSource,
			Clock clock,
			long configuredLiveWalLimitBytes) {
		this.walDirectory = Objects.requireNonNull(walDirectory, "walDirectory");
		this.walMetadataSource = Objects.requireNonNull(walMetadataSource, "walMetadataSource");
		this.minLogNumberSource = Objects.requireNonNull(minLogNumberSource, "minLogNumberSource");
		this.fileTimestampSource = Objects.requireNonNull(fileTimestampSource, "fileTimestampSource");
		this.clock = Objects.requireNonNull(clock, "clock");

		meters.add(Gauge.builder(LIVE_BYTES_METRIC, snapshot, value -> value.get().live().bytes())
				.description("Bytes in live RocksDB WAL files")
				.baseUnit("bytes")
				.tag("database", databaseName)
				.register(registry));
		meters.add(Gauge.builder(LIVE_FILES_METRIC, snapshot, value -> value.get().live().fileCount())
				.description("Number of live RocksDB WAL files")
				.tag("database", databaseName)
				.register(registry));
		meters.add(Gauge.builder(LIVE_OLDEST_AGE_METRIC, this, value -> value.oldestAgeSeconds(value.snapshot.get().live()))
				.description("Age of the oldest live RocksDB WAL file")
				.baseUnit("seconds")
				.tag("database", databaseName)
				.register(registry));
		meters.add(Gauge.builder(ARCHIVED_BYTES_METRIC, snapshot, value -> value.get().archived().bytes())
				.description("Bytes in archived RocksDB WAL files retained for CDC")
				.baseUnit("bytes")
				.tag("database", databaseName)
				.register(registry));
		meters.add(Gauge.builder(ARCHIVED_FILES_METRIC, snapshot, value -> value.get().archived().fileCount())
				.description("Number of archived RocksDB WAL files retained for CDC")
				.tag("database", databaseName)
				.register(registry));
		meters.add(Gauge.builder(ARCHIVED_OLDEST_AGE_METRIC, this,
					value -> value.oldestAgeSeconds(value.snapshot.get().archived()))
				.description("Age of the oldest archived RocksDB WAL file retained for CDC")
				.baseUnit("seconds")
				.tag("database", databaseName)
				.register(registry));
		meters.add(Gauge.builder(MIN_LOG_NUMBER_TO_KEEP_METRIC, snapshot,
					value -> value.get().minLogNumberToKeep())
				.description("RocksDB minimum WAL log number still required by live state")
				.tag("database", databaseName)
				.register(registry));
		meters.add(Gauge.builder(CONFIGURED_LIVE_LIMIT_METRIC, () -> configuredLiveWalLimitBytes)
				.description("Configured RocksDB max-total-wal-size limit")
				.baseUnit("bytes")
				.tag("database", databaseName)
				.register(registry));
	}

	void refresh() {
		var previous = snapshot.get();
		var updated = previous;
		try {
			var classified = classify(walMetadataSource.load(), walDirectory, fileTimestampSource);
			updated = new WalSnapshot(classified.live(), classified.archived(), previous.minLogNumberToKeep());
		} catch (Exception ex) {
			LOG.warn("Failed to refresh RocksDB WAL file metrics; retaining the previous snapshot", ex);
		}
		try {
			updated = new WalSnapshot(updated.live(), updated.archived(), minLogNumberSource.load());
		} catch (Exception ex) {
			LOG.warn("Failed to refresh rocksdb.min-log-number-to-keep; retaining the previous value", ex);
		}
		snapshot.set(updated);
	}

	static WalSnapshot classify(List<WalFileMetadata> files,
			Path walDirectory,
			FileTimestampSource fileTimestampSource) {
		var live = new WalGroupAccumulator();
		var archived = new WalGroupAccumulator();
		for (var file : files) {
			var accumulator = switch (file.type()) {
				case kAliveLogFile -> live;
				case kArchivedLogFile -> archived;
			};
			accumulator.addFile(file.sizeFileBytes());
			try {
				accumulator.addTimestamp(fileTimestampSource.lastModifiedMillis(
						resolveMetadataPath(walDirectory, file.pathName())));
			} catch (Exception ignored) {
				// A live WAL can move to the archive between RocksDB's metadata snapshot and this stat.
				// Keep its RocksDB-reported type/size and retry the timestamp on the next collection.
			}
		}
		return new WalSnapshot(live.snapshot(), archived.snapshot(), 0L);
	}

	private static Path resolveMetadataPath(Path walDirectory, String metadataPath) {
		var relativePath = metadataPath.startsWith("/") ? metadataPath.substring(1) : metadataPath;
		return walDirectory.resolve(relativePath);
	}

	private double oldestAgeSeconds(WalGroup group) {
		if (group.oldestModifiedMillis() == NO_TIMESTAMP) {
			return 0.0d;
		}
		return Math.max(0.0d, (clock.millis() - group.oldestModifiedMillis()) / 1000.0d);
	}

	@Override
	public void close() {
		if (closed.compareAndSet(false, true)) {
			meters.forEach(Meter::close);
		}
	}

	@FunctionalInterface
	interface WalMetadataSource {
		List<WalFileMetadata> load() throws Exception;
	}

	@FunctionalInterface
	interface MinLogNumberSource {
		long load() throws Exception;
	}

	@FunctionalInterface
	interface FileTimestampSource {
		long lastModifiedMillis(Path path) throws Exception;
	}

	record WalFileMetadata(String pathName, WalFileType type, long sizeFileBytes) {

		WalFileMetadata {
			Objects.requireNonNull(pathName, "pathName");
			Objects.requireNonNull(type, "type");
		}

		static WalFileMetadata from(LogFile file) {
			return new WalFileMetadata(file.pathName(), file.type(), file.sizeFileBytes());
		}
	}

	record WalSnapshot(WalGroup live, WalGroup archived, long minLogNumberToKeep) {}

	record WalGroup(long bytes, long fileCount, long oldestModifiedMillis) {}

	private static final class WalGroupAccumulator {

		private long bytes;
		private long fileCount;
		private long oldestModifiedMillis = NO_TIMESTAMP;

		void addFile(long fileBytes) {
			bytes = saturatedAdd(bytes, Math.max(0L, fileBytes));
			fileCount = saturatedAdd(fileCount, 1L);
		}

		void addTimestamp(long modifiedMillis) {
			if (oldestModifiedMillis == NO_TIMESTAMP || modifiedMillis < oldestModifiedMillis) {
				oldestModifiedMillis = modifiedMillis;
			}
		}

		WalGroup snapshot() {
			return new WalGroup(bytes, fileCount, oldestModifiedMillis);
		}

		private static long saturatedAdd(long current, long increment) {
			return current > Long.MAX_VALUE - increment ? Long.MAX_VALUE : current + increment;
		}
	}
}
