package it.cavallium.rockserver.core.config;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ConfigPrinter {

	public static String stringifyBloomFilter(BloomFilterConfig o) {
		return """
				{
				        "bits-per-key": %d,
				        "optimize-for-hits": %b
				      }\
				""".formatted(o.bitsPerKey(), o.optimizeForHits());
	}

	public static String stringifyDatabase(DatabaseConfig o) {
		return """
				{
				  "global": %s
				}""".formatted(stringifyGlobalDatabase(o.global()));
	}

	public static String stringifyLevel(DatabaseLevel o) {
		return """
				{
				        "compression": "%s",
				        "max-dict-bytes": "%s"
				      }\
				""".formatted(o.compression(), o.maxDictBytes());
	}

	public static String stringifyFallbackColumn(FallbackColumnOptions o) {
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
				""".formatted(Arrays.stream(Objects.requireNonNullElse(o.levels(), new DatabaseLevel[0]))
				.map(ConfigPrinter::stringifyLevel).collect(Collectors.joining(",", "[", "]")),
				o.memtableMemoryBudgetBytes(),
				o.cacheIndexAndFilterBlocks(),
				o.partitionFilters(),
				stringifyBloomFilter(o.bloomFilter()),
				o.blockSize(),
				o.writeBufferSize()
		);
	}

	public static String stringifyGlobalDatabase(GlobalDatabaseConfig o) {
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
				stringifyFallbackColumn(o.fallbackColumnOptions()),
				Arrays.stream(Objects.requireNonNullElse(o.columnOptions(), new NamedColumnOptions[0]))
						.map(ConfigPrinter::stringifyNamedColumn)
						.collect(Collectors.joining(",", "[", "]"))
		);
	}

	public static String stringifyNamedColumn(NamedColumnOptions o) {
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
				(o.levels() != null ? List.of(o.levels()) : List.<DatabaseLevel>of()).stream()
						.map(ConfigPrinter::stringifyLevel).collect(Collectors.joining(",", "[", "]")),
				o.memtableMemoryBudgetBytes(),
				o.cacheIndexAndFilterBlocks(),
				o.partitionFilters(),
				stringifyBloomFilter(o.bloomFilter()),
				o.blockSize(),
				o.writeBufferSize()
		);
	}
}
