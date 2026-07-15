package it.cavallium.rockserver.core.config;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import org.github.gestalt.config.exceptions.GestaltException;

import java.util.*;

public class ConfigPrinter {

	public static String stringifyBloomFilter(BloomFilterConfig o) throws GestaltException {
		return """
				{
				        "bits-per-key": %d,
				        "optimize-for-hits": %s
				      }\
				""".formatted(o.bitsPerKey(), o.optimizeForHits());
	}

	public static String stringify(DatabaseConfig config) {
		try {
			return stringifyDatabase(config);
		} catch (GestaltException e) {
			throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, "Can't stringify config", e);
		}
	}

	public static String stringifyDatabase(DatabaseConfig o) throws GestaltException {
		return """
				{
				  "parallelism": %s,
				  "metrics": %s,
				  "global": %s
				}""".formatted(stringifyParallelism(o.parallelism()),
				stringifyMetrics(o.metrics()),
				stringifyGlobalDatabase(o.global()));
	}

	public static String stringifyLevel(ColumnLevelConfig o) throws GestaltException {
		return """
				{
				        "compression": "%s",
				        "max-dict-bytes": "%s"
				      }\
				""".formatted(stringifyCompression(o.compression()), o.maxDictBytes());
	}

	public static List<VolumeConfig> getVolumeConfigs(FallbackColumnConfig g) throws GestaltException {
		return List.of(g.volumes());
	}

	public static String stringifyGlobalDatabase(GlobalDatabaseConfig o) throws GestaltException {
		StringJoiner joiner = new StringJoiner(",", "[", "]");
		for (NamedColumnConfig namedColumnConfig : Objects.requireNonNullElse(o.columnOptions(), new NamedColumnConfig[0])) {
			String s = stringifyNamedColumn(namedColumnConfig);
			joiner.add(s);
		}
		return """
				{
				    "enable-fast-get": %b,
				    "spinning": %b,
				    "checksum": %b,
				    "use-direct-io": %b,
				    "allow-rocksdb-memory-mapping": %b,
				    "maximum-open-files": %d,
				    "optimistic": %b,
				    "block-cache": %s,
				    "write-buffer-manager": %s,
				    "log-path": %s,
				    "wal-path": %s,
				    "wal-ttl-seconds": %s,
				    "temp-sst-path": %s,
				    "delay-wal-flush-duration": %s,
				    "absolute-consistency": %b,
				    "ingest-behind": %b,
				    "unordered-write": %b,
				    "max-background-jobs": %s,
				    "fallback-column-options": %s,
				    "column-options": %s
				  }\
				""".formatted(o.enableFastGet(),
				o.spinning(),
				o.checksum(),
				o.useDirectIo(),
				o.allowRocksdbMemoryMapping(),
				o.maximumOpenFiles(),
				o.optimistic(),
				quote(o.blockCache()),
				quote(o.writeBufferManager()),
				quote(o.logPath()),
				quote(o.walPath()),
				o.walTtlSeconds(),
				quote(o.tempSstPath()),
				quote(o.delayWalFlushDuration()),
				o.absoluteConsistency(),
				o.ingestBehind(),
				o.unorderedWrite(),
				o.maxBackgroundJobs(),
				stringifyFallbackColumn(o.fallbackColumnOptions()),
				joiner.toString()
		);
	}

	public static String stringifyParallelism(ParallelismConfig o) throws GestaltException {
		return """
				{
				    "read": %d,
				    "write": %d
				  }\
				""".formatted(o.read(),
				o.write()
		);
	}

	public static String stringifyMetrics(MetricsConfig o) throws GestaltException {
		return """
				{
				    "database-name": %s,
				    "influx": %s,
				    "jmx": %s
				  }\
				""".formatted(
				quote(o.databaseName()),
				stringifyInflux(o.influx()),
				stringifyJmx(o.jmx())
		);
	}

	public static String stringifyInflux(InfluxMetricsConfig o) throws GestaltException {
		return """
				{
				      "enabled": %b,
				      "url": %s,
				      "bucket": %s,
				      "user": %s,
				      "token": %s,
				      "org": %s,
				      "allow-insecure-certificates": %s
				    }\
				""".formatted(o.enabled(),
				quote(o.url()),
				quote(o.bucket()),
				quote(o.user()),
				quote(o.token()),
				quote(o.org()),
				o.allowInsecureCertificates()
		);
	}

	public static String stringifyJmx(JmxMetricsConfig o) throws GestaltException {
		return """
				{
				      "enabled": %b
				    }\
				""".formatted(o.enabled());
	}

	private static String stringifyVolume(VolumeConfig o) throws GestaltException {
		return """
				{
				        "volume-path": %s,
				        "target-size": %s
				      }\
				""".formatted(quote(o.volumePath()),
				quote(o.targetSize())
		);
	}

	public static String stringifyFallbackColumn(FallbackColumnConfig o) throws GestaltException {
		StringJoiner joiner = new StringJoiner(",", "[", "]");
		for (ColumnLevelConfig columnLevelConfig : Objects.requireNonNullElse(o.levels(), new ColumnLevelConfig[0])) {
			String s = stringifyLevel(columnLevelConfig);
			joiner.add(s);
		}
		String bloom = null;
		BloomFilterConfig value = o.bloomFilter();
		if (value != null) {
			String s = stringifyBloomFilter(value);
			if (s != null) bloom = s;
		}
		StringJoiner volumesStr = new StringJoiner(",", "[", "]");
		for (VolumeConfig volumeConfig : getVolumeConfigs(o)) {
			String s = stringifyVolume(volumeConfig);
			volumesStr.add(s);
		}
		return """
				{
				      "merge-operator-class": %s,
				      "first-level-sst-size": %s,
				      "max-last-level-sst-size": %s,
				      "volumes": %s,
				      "levels": %s,
				      "memtable-memory-budget-bytes": %s,
				      "memtable-max-range-deletions": %s,
				      "cache-index-and-filter-blocks": %s,
				      "partition-filters": %s,
				      "bloom-filter": %s,
				      "block-size": %s,
				      "write-buffer-size": %s
			    }\
			""".formatted(
				quote(o.mergeOperatorClass()),
				quote(o.firstLevelSstSize()),
				quote(o.maxLastLevelSstSize()),
				volumesStr.toString(),
				joiner.toString(),
				quote(o.memtableMemoryBudgetBytes()),
				o.memtableMaxRangeDeletions(),
				o.cacheIndexAndFilterBlocks(),
				o.partitionFilters(),
				bloom,
				quote(o.blockSize()),
				quote(o.writeBufferSize())
		);
	}

	public static String stringifyNamedColumn(NamedColumnConfig o) throws GestaltException {
		StringJoiner joiner = new StringJoiner(",", "[", "]");
		for (ColumnLevelConfig columnLevelConfig : (o.levels() != null ? List.of(o.levels()) : List.<ColumnLevelConfig>of())) {
			String s = stringifyLevel(columnLevelConfig);
			joiner.add(s);
		}
		String bloom = null;
		BloomFilterConfig value = o.bloomFilter();
		if (value != null) {
			String s = stringifyBloomFilter(value);
			if (s != null) bloom = s;
		}
		StringJoiner volumesStr = new StringJoiner(",", "[", "]");
		for (VolumeConfig volumeConfig : getVolumeConfigs(o)) {
			String s = stringifyVolume(volumeConfig);
			volumesStr.add(s);
		}
		return """
				{
				      "merge-operator-class": %s,
				      "name": %s,
				      "first-level-sst-size": %s,
				      "max-last-level-sst-size": %s,
				      "volumes": %s,
				      "levels": %s,
				      "memtable-memory-budget-bytes": %s,
				      "memtable-max-range-deletions": %s,
				      "cache-index-and-filter-blocks": %s,
				      "partition-filters": %s,
				      "bloom-filter": %s,
				      "block-size": %s,
				      "write-buffer-size": %s
			    }\
			""".formatted(quote(o.mergeOperatorClass()),
				quote(o.name()),
				quote(o.firstLevelSstSize()),
				quote(o.maxLastLevelSstSize()),
				volumesStr.toString(),
				joiner.toString(),
				quote(o.memtableMemoryBudgetBytes()),
				o.memtableMaxRangeDeletions(),
				o.cacheIndexAndFilterBlocks(),
				o.partitionFilters(),
				bloom,
				quote(o.blockSize()),
				quote(o.writeBufferSize())
		);
	}

	private static String stringifyCompression(org.rocksdb.CompressionType compressionType) {
		return switch (compressionType.name()) {
			case "NO_COMPRESSION" -> "PLAIN";
			case "SNAPPY_COMPRESSION" -> "SNAPPY";
			case "LZ4_COMPRESSION" -> "LZ4";
			case "LZ4HC_COMPRESSION" -> "LZ4_HC";
			case "ZSTD_COMPRESSION" -> "ZSTD";
			case "ZLIB_COMPRESSION" -> "ZLIB";
			case "BZLIB2_COMPRESSION" -> "BZLIB2";
			default -> throw new IllegalArgumentException("Unsupported RocksDB compression type: " + compressionType);
		};
	}

	private static String quote(Object value) {
		if (value == null) {
			return "null";
		}
		return '"' + value.toString()
				.replace("\\", "\\\\")
				.replace("\"", "\\\"")
				.replace("\n", "\\n")
				.replace("\r", "\\r") + '"';
	}
}
