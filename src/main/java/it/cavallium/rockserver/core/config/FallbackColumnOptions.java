package it.cavallium.rockserver.core.config;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public interface FallbackColumnOptions {

	DatabaseLevel[] levels();

	DataSize memtableMemoryBudgetBytes();

	boolean cacheIndexAndFilterBlocks();

	boolean partitionFilters();

	BloomFilterConfig bloomFilter();

	DataSize blockSize();

	DataSize writeBufferSize();

	static String stringify(FallbackColumnOptions o) {
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
				.map(DatabaseLevel::stringify).collect(Collectors.joining(",", "[", "]")),
				o.memtableMemoryBudgetBytes(),
				o.cacheIndexAndFilterBlocks(),
				o.partitionFilters(),
				BloomFilterConfig.stringify(o.bloomFilter()),
				o.blockSize(),
				o.writeBufferSize()
		);
	}
}
