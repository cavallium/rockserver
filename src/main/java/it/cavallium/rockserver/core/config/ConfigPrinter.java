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
			throw new RocksDBException(RocksDBErrorType.CONFIG_ERROR, "Can't stringify config", e);
		}
	}

	public static String stringifyDatabase(DatabaseConfig o) throws GestaltException {
		return """
				{
				  "global": %s
				}""".formatted(stringifyGlobalDatabase(o.global()));
	}

	public static String stringifyLevel(ColumnLevelConfig o) throws GestaltException {
		return """
				{
				        "compression": "%s",
				        "max-dict-bytes": "%s"
				      }\
				""".formatted(o.compression(), o.maxDictBytes());
	}

	public static List<VolumeConfig> getVolumeConfigs(GlobalDatabaseConfig g) throws GestaltException {
		try {
			return List.of(g.volumes());
		} catch (GestaltException ex) {
			if (ex.getMessage().startsWith("Failed to get cached object from proxy config while calling method:")) {
				return List.of();
			} else {
				throw ex;
			}
		}
	}

	public static String stringifyGlobalDatabase(GlobalDatabaseConfig o) throws GestaltException {
		StringJoiner joiner = new StringJoiner(",", "[", "]");
		for (NamedColumnConfig namedColumnConfig : Objects.requireNonNullElse(o.columnOptions(), new NamedColumnConfig[0])) {
			String s = stringifyNamedColumn(namedColumnConfig);
			joiner.add(s);
		}
		StringJoiner result = new StringJoiner(",", "[", "]");
		for (VolumeConfig volumeConfig : getVolumeConfigs(o)) {
			String s = stringifyVolume(volumeConfig);
			result.add(s);
		}
		return """
				{
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
				    "volumes": %s,
				    "fallback-column-options": %s,
				    "column-options": %s
				  }\
				""".formatted(o.spinning(),
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
				result.toString(),
				stringifyFallbackColumn(o.fallbackColumnOptions()),
				joiner.toString()
		);
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
		return """
				{
				      "levels": %s,
				      "memtable-memory-budget-bytes": "%s",
				      "cache-index-and-filter-blocks": %b,
				      "partition-filters": %s,
				      "bloom-filter": %s,
				      "block-size": "%s",
				      "write-buffer-size": "%s"
				    }\
				""".formatted(joiner.toString(),
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
		return """
				{
				      "name": "%s",
				      "levels": %s,
				      "memtable-memory-budget-bytes": "%s",
				      "cache-index-and-filter-blocks": %b,
				      "partition-filters": %s,
				      "bloom-filter": %s,
				      "block-size": "%s",
				      "write-buffer-size": "%s"
				    }\
				""".formatted(o.name(),
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
