package it.cavallium.rockserver.core.config;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public interface NamedColumnOptions extends FallbackColumnOptions {

	String name();

	static String stringify(NamedColumnOptions o) {
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
