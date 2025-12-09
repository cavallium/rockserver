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
				        "optimize-for-hits": %b
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
				""".formatted(o.compression(), o.maxDictBytes());
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
				    "block-cache": "%s",
				    "write-buffer-manager": "%s",
				    "log-path": "%s",
				    "wal-path": "%s",
				    "absolute-consistency": %b,
				    "ingest-behind": %b,
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
				o.blockCache(),
				o.writeBufferManager(),
				o.logPath(),
				o.walPath(),
				o.absoluteConsistency(),
				o.ingestBehind(),
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
				o.databaseName(),
				stringifyInflux(o.influx()),
				stringifyJmx(o.jmx())
		);
	}

	public static String stringifyInflux(InfluxMetricsConfig o) throws GestaltException {
		return """
				{
				      "enabled": %b,
				      "url": "%s",
				      "bucket": "%s",
				      "user": "%s",
				      "token": "%s",
				      "org": "%s",
				      "allow-insecure-certificates": %b
				    }\
				""".formatted(o.enabled(),
				o.url(),
				o.bucket(),
				o.user(),
				o.token(),
				o.org(),
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
				        "volume-path": "%s",
				        "target-size-bytes": "%s"
				      }\
				""".formatted(o.volumePath(),
				o.targetSize()
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
				      "memtable-memory-budget-bytes": "%s",
				      "cache-index-and-filter-blocks": %b,
				      "partition-filters": %s,
				      "bloom-filter": %s,
				      "block-size": "%s",
				      "write-buffer-size": "%s"
			    }\
			""".formatted(
				o.mergeOperatorClass(),
				o.firstLevelSstSize(),
				o.maxLastLevelSstSize(),
				volumesStr.toString(),
				joiner.toString(),
				o.memtableMemoryBudgetBytes(),
				o.cacheIndexAndFilterBlocks(),
				o.partitionFilters(),
				bloom,
				o.blockSize(),
				o.writeBufferSize()
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
				      "name": "%s",
				      "first-level-sst-size": %s,
				      "max-last-level-sst-size": %s,
				      "volumes": %s,
				      "levels": %s,
				      "memtable-memory-budget-bytes": "%s",
				      "cache-index-and-filter-blocks": %b,
				      "partition-filters": %s,
				      "bloom-filter": %s,
				      "block-size": "%s",
				      "write-buffer-size": "%s"
			    }\
			""".formatted(o.name(),
				o.mergeOperatorClass(),
				o.firstLevelSstSize(),
				o.maxLastLevelSstSize(),
				volumesStr.toString(),
				joiner.toString(),
				o.memtableMemoryBudgetBytes(),
				o.cacheIndexAndFilterBlocks(),
				o.partitionFilters(),
				bloom,
				o.blockSize(),
				o.writeBufferSize()
		);
	}
}
